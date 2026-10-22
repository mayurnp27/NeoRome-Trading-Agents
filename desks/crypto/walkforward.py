"""Deterministic walk-forward reports for the two crypto research lanes.

The module is deliberately a research boundary.  It accepts immutable,
caller-supplied observations, replays the frozen CR-001 spot rule using
next-available execution, and evaluates the prospective memecoin cohort using
the existing Jupiter route outcome recorder.  It never fetches data, changes a
ledger, submits a route, or enables a scheduler.

The output is versioned and carries a content hash so each run is an immutable
trial.  Walk-forward windows are chronological; the token lane also applies a
240-minute label embargo because the longest preregistered outcome horizon is
240 minutes.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

from .trend_replay import CostAssumptions, InsufficientHistory, ReplayError, replay, validate_bars

try:
    from neorome.token_outcomes import OutcomeError, evaluate as evaluate_token_outcomes
except ImportError:  # pragma: no cover - useful when the file is inspected alone
    OutcomeError = ValueError
    evaluate_token_outcomes = None


UTC = timezone.utc
SCHEMA_VERSION = 1
ENGINE_VERSION = "crypto-walkforward-v1"
LIQUID_TRIAL_VERSION = "cr-001-walkforward-v1"
TOKEN_TRIAL_VERSION = "solana-memecoin-walkforward-v1"
TOKEN_EMBARGO_MINUTES = 240
HORIZONS_MINUTES = (15, 60, 240)


class WalkForwardError(ValueError):
    """Input cannot be admitted to a deterministic walk-forward trial."""


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise WalkForwardError("input contains non-JSON data") from exc


def input_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise WalkForwardError(f"{field} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise WalkForwardError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise WalkForwardError(f"{field} must include a timezone")
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _decimal(value: Any, field: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise WalkForwardError(f"{field} must be a finite number") from exc
    if not parsed.is_finite():
        raise WalkForwardError(f"{field} must be a finite number")
    return parsed


def _phase_ranges(count: int, *, minimum_history: int, embargo: int) -> dict[str, tuple[int, int]]:
    """Return chronological development/validation/holdout evaluation ranges."""
    if count < minimum_history + 3:
        raise WalkForwardError("insufficient rows for chronological three-way split")
    available = count - minimum_history
    development = max(1, available // 2)
    validation = max(1, (available - development) // 2)
    first = minimum_history
    second = first + development
    third = second + validation
    # The embargo is intentionally a gap in evaluation rows.  It is not used
    # to invent labels or make a small sample look statistically independent.
    validation_start = second + embargo
    holdout_start = third + embargo
    if validation_start >= holdout_start or holdout_start >= count:
        raise WalkForwardError("chronological split leaves no validation or holdout rows")
    return {
        "development": (first, second),
        "validation": (validation_start, third),
        "holdout": (holdout_start, count),
    }


def _trade_metrics(result: Mapping[str, Any], rows: Sequence[Mapping[str, Any]],
                   start: int, end: int) -> dict[str, Any]:
    start_at = rows[start]["timestamp"]
    end_at = rows[end - 1]["timestamp"]
    closed = [trade for trade in result.get("trades", [])
              if trade.get("entry_execution_day", "") >= start_at
              and trade.get("exit_execution_day", "") <= end_at]
    signals = [signal for signal in result.get("signals", [])
               if start_at <= signal.get("signal_day", "") <= end_at]
    boundary = [trade for trade in result.get("trades", [])
                if trade not in closed and
                trade.get("entry_execution_day", "") <= end_at and
                trade.get("exit_execution_day", "") >= start_at]
    pnl = sum(int(trade["net_pnl_cents"]) for trade in closed)
    benchmark_open = Decimal(rows[start]["open"])
    benchmark_close = Decimal(rows[end - 1]["close"])
    benchmark_bps = ((benchmark_close / benchmark_open) - Decimal(1)) * Decimal(10000)
    return {
        "start": start_at, "end": end_at, "signal_count": len(signals),
        "closed_trade_count": len(closed), "boundary_trade_count": len(boundary),
        "net_pnl_cents": pnl, "benchmark_return_bps": str(benchmark_bps.quantize(Decimal("0.01"))),
        "insufficient_outcome_data": len(closed) == 0,
    }


def _liquid_blockers(metrics: Mapping[str, Mapping[str, Any]], row_count: int,
                     *, minimum_observation_days: int = 60) -> list[str]:
    blockers: list[str] = []
    if row_count < minimum_observation_days:
        blockers.append("observation_floor_below_60_calendar_days")
    for phase in ("validation", "holdout"):
        row = metrics.get(phase, {})
        if row.get("closed_trade_count", 0) == 0:
            blockers.append(f"{phase}_has_no_completed_trades")
    if any(metrics.get(phase, {}).get("boundary_trade_count", 0) for phase in metrics):
        blockers.append("trade_crosses_walk_forward_boundary")
    # The current bar contract has no venue, bid/ask, outage mask or fee
    # schedule version.  CR-001 replay is still useful, but admission remains
    # blocked until those execution facts are archived.
    blockers.extend(("venue_execution_evidence_missing", "fee_schedule_version_missing",
                     "historical_depth_and_outage_mask_missing"))
    return sorted(set(blockers))


def walk_forward_liquid(bars: Sequence[Mapping[str, Any]], *,
                        embargo_bars: int = 1,
                        minimum_observation_days: int = 60) -> dict[str, Any]:
    """Replay CR-001 in chronological cost-stressed windows.

    No parameters are fitted or optimized.  The first history is warm-up;
    validation and holdout are scored only on trades whose entry and exit are
    both inside that phase.
    """
    if type(embargo_bars) is not int or embargo_bars < 0:
        raise WalkForwardError("embargo_bars must be a nonnegative integer")
    rows = validate_bars(bars)
    ranges = _phase_ranges(len(rows), minimum_history=101, embargo=embargo_bars)
    scenarios = {
        "round_trip_10bps": CostAssumptions(fee_bps="5", slippage_bps="0"),
        "round_trip_25bps": CostAssumptions(fee_bps="12.5", slippage_bps="0"),
        "round_trip_50bps": CostAssumptions(fee_bps="25", slippage_bps="0"),
    }
    phase_results: dict[str, Any] = {}
    for phase, (start, end) in ranges.items():
        scenario_rows: dict[str, Any] = {}
        for name, costs in scenarios.items():
            result = replay(rows[:end], costs=costs)
            scenario_rows[name] = {**_trade_metrics(result, rows, start, end),
                                   "cost_assumptions": costs.as_dict()}
        phase_results[phase] = {"range_indices": [start, end], "scenarios": scenario_rows}
    summary_metrics = {
        phase: phase_results[phase]["scenarios"]["round_trip_25bps"]
        for phase in phase_results
    }
    payload = {
        "schema_version": SCHEMA_VERSION, "engine_version": ENGINE_VERSION,
        "trial_version": LIQUID_TRIAL_VERSION, "lane": "liquid_btc_trend",
        "policy_id": "CR-001", "research_only": True, "execution_enabled": False,
        "input_sha256": input_sha256(list(bars)),
        "bars": {"count": len(rows), "first": rows[0]["timestamp"], "last": rows[-1]["timestamp"]},
        "split": {"embargo_bars": embargo_bars, "phases": phase_results},
        "metrics": summary_metrics,
        "cost_stress_bps_round_trip": [10, 25, 50],
        "acceptance": {"status": "blocked", "blockers": _liquid_blockers(summary_metrics, len(rows),
                                                                           minimum_observation_days=minimum_observation_days)},
        "cadence": {"monitoring": "daily", "walk_forward_revalidation": "weekly",
                     "promotion_review": "monthly"},
        "parameter_policy": "frozen_model_free_no_optimization",
    }
    payload["trial_id"] = hashlib.sha256(_canonical(payload)).hexdigest()
    return payload


def _x_signals(candidate: Mapping[str, Any]) -> Mapping[str, Any] | None:
    for key in ("x_features", "social_features"):
        if isinstance(candidate.get(key), Mapping):
            return candidate[key]
    social = candidate.get("social")
    if isinstance(social, Mapping) and isinstance(social.get("signals"), Mapping):
        return social["signals"]
    return None


def _x_eligible(candidate: Mapping[str, Any]) -> bool | None:
    value = candidate.get("x_eligible", candidate.get("social_eligible"))
    if isinstance(value, bool):
        return value
    features = _x_signals(candidate)
    if features is None:
        return None
    # Reuse the declared X policy only when its raw features are complete.  A
    # missing field stays unknown; it is never interpreted as a pass.
    required = ("mentions", "unique_credible_authors", "bot_post_fraction")
    if any(key not in features for key in required):
        return None
    try:
        return (int(features["mentions"]) >= 3 and
                int(features["unique_credible_authors"]) >= 2 and
                features.get("rug_warning_post_ids", []) == [] and
                Decimal(str(features["bot_post_fraction"])) <= Decimal("0.50"))
    except (TypeError, ValueError, InvalidOperation):
        return None


def _token_rows(result: Mapping[str, Any]) -> dict[str, dict[int, dict[str, Any]]]:
    rows: dict[str, dict[int, dict[str, Any]]] = {}
    for row in result.get("cohort", []):
        if not isinstance(row, Mapping):
            continue
        key = f"{row.get('cohort_index')}:{row.get('chain')}:{row.get('mint')}"
        rows[key] = {int(item.get("horizon_minutes")): item for item in row.get("horizons", [])
                     if isinstance(item, Mapping) and item.get("horizon_minutes") in HORIZONS_MINUTES}
    return rows


def _cohort_metrics(rows: Sequence[Mapping[str, Any]], outcomes: Mapping[str, dict[int, dict[str, Any]]],
                    *, x_group: str) -> dict[str, Any]:
    selected = [row for row in rows if row.get("x_group") == x_group]
    horizon_metrics: dict[str, Any] = {}
    for horizon in HORIZONS_MINUTES:
        statuses: dict[str, int] = {}
        net = 0
        modeled = 0
        for row in selected:
            item = outcomes.get(row["key"], {}).get(horizon)
            status = item.get("status", "missing") if item else "missing"
            statuses[status] = statuses.get(status, 0) + 1
            if status == "modeled_outcome":
                modeled += 1
                net += int(item.get("net_pnl_usdc_raw", 0))
        horizon_metrics[str(horizon)] = {"candidate_count": len(selected),
                                         "modeled_outcome_count": modeled,
                                         "status_counts": statuses,
                                         "net_pnl_usdc_raw": str(net)}
    return {"candidate_count": len(selected), "horizons": horizon_metrics}


def _token_blockers(rows: Sequence[Mapping[str, Any]], *, x_unknown: int,
                   minimum_candidates: int = 30) -> list[str]:
    blockers: list[str] = []
    if len(rows) < minimum_candidates:
        blockers.append("prospective_eligible_cohort_below_30_candidates")
    if x_unknown:
        blockers.append("x_features_or_eligibility_missing")
    if not any(row.get("x_group") == "x_selected" for row in rows):
        blockers.append("x_selected_cohort_empty")
    if not any(row.get("x_group") == "no_x_baseline" for row in rows):
        blockers.append("no_x_baseline_cohort_empty")
    for horizon in HORIZONS_MINUTES:
        if not any(row.get("outcomes", {}).get(horizon, {}).get("status") == "modeled_outcome" for row in rows):
            blockers.append(f"no_modeled_{horizon}m_route_outcomes")
    blockers.extend(("route_landing_and_network_fee_evidence_missing",
                     "jupiter_quote_history_is_not_realized_execution"))
    return sorted(set(blockers))


def walk_forward_tokens(discovery: Mapping[str, Any], sell_observations: Sequence[Mapping[str, Any]] | None = None,
                        *, embargo_minutes: int = TOKEN_EMBARGO_MINUTES,
                        minimum_candidates: int = 30) -> dict[str, Any]:
    """Evaluate a prospective token cohort with X ablation and 15/60/240m routes."""
    if evaluate_token_outcomes is None:  # pragma: no cover
        raise WalkForwardError("token outcome evaluator unavailable")
    if type(embargo_minutes) is not int or embargo_minutes < max(HORIZONS_MINUTES):
        raise WalkForwardError("embargo_minutes must cover the 240-minute outcome horizon")
    if not isinstance(discovery, Mapping):
        raise WalkForwardError("discovery must be an object")
    raw_candidates = discovery.get("candidates")
    if not isinstance(raw_candidates, Sequence) or isinstance(raw_candidates, (str, bytes)):
        raise WalkForwardError("discovery.candidates must be a list")
    ordered = sorted((deepcopy(dict(row)) for row in raw_candidates if isinstance(row, Mapping)),
                     key=lambda row: row.get("received_at", row.get("observed_at", "")))
    # The evaluator owns route identity, exact acquired quantity and failure
    # classes.  Its output is immutable evidence for this trial.
    normalized_discovery = {**dict(discovery), "candidates": ordered}
    outcome = evaluate_token_outcomes(normalized_discovery, sell_observations or [])
    outcome_by_key = _token_rows(outcome)
    cohort: list[dict[str, Any]] = []
    x_unknown = 0
    for row in outcome.get("cohort", []):
        original = ordered[int(row["cohort_index"])] if int(row.get("cohort_index", -1)) < len(ordered) else {}
        # Only entries with a complete prospective route and no screen
        # rejection enter the eligible evaluation cohort.  Rejections and
        # disappeared candidates stay in the immutable outcome, but are not
        # silently counted as successful opportunities.
        eligible = row.get("status") == "observed_pending" and isinstance(row.get("entry"), Mapping)
        if not eligible:
            continue
        x_state = _x_eligible(original)
        if x_state is None:
            # The no-X comparator is the same prospective eligible cohort with
            # X ignored.  Keep the missing-feature count as a blocker so this
            # fallback cannot be mistaken for a completed X ablation.
            x_unknown += 1
            group = "no_x_baseline"
        else:
            group = "x_selected" if x_state else "no_x_baseline"
        key = f"{row.get('cohort_index')}:{row.get('chain')}:{row.get('mint')}"
        outcomes = outcome_by_key.get(key, {})
        received = _timestamp(row["received_at"], "candidate.received_at")
        cohort.append({"key": key, "received_at": _iso(received), "x_group": group,
                       "outcomes": outcomes})
    # Build disjoint causal windows by candidate receipt time.  The first
    # candidate after a phase boundary must also be beyond the longest label
    # horizon.  No feature or threshold is fitted to one window and applied to
    # another; the split is a report boundary for monitoring the predeclared
    # policy.
    cohort.sort(key=lambda row: row["received_at"])
    n = len(cohort)
    split: dict[str, tuple[int, int]] = {"development": (0, 0), "validation": (0, 0), "holdout": (0, 0)}
    if n:
        development_end = max(1, n // 2)
        validation_target_end = max(development_end + 1, (3 * n) // 4)
        development_boundary = _timestamp(cohort[development_end - 1]["received_at"], "cohort.received_at")
        validation_start = next((index for index in range(development_end, n)
                                 if _timestamp(cohort[index]["received_at"], "cohort.received_at")
                                 >= development_boundary + timedelta(minutes=embargo_minutes)), n)
        validation_end = min(n, max(validation_start + 1, validation_target_end))
        if validation_start >= n:
            validation_start = validation_end = n
        validation_boundary = (_timestamp(cohort[validation_end - 1]["received_at"], "cohort.received_at")
                              if validation_end > validation_start else None)
        holdout_start = n
        if validation_boundary is not None:
            holdout_start = next((index for index in range(validation_end, n)
                                  if _timestamp(cohort[index]["received_at"], "cohort.received_at")
                                  >= validation_boundary + timedelta(minutes=embargo_minutes)), n)
        split = {"development": (0, development_end),
                 "validation": (validation_start, validation_end),
                 "holdout": (holdout_start, n)}
    phase_metrics: dict[str, Any] = {}
    for name, (start, end) in split.items():
        rows = cohort[start:end]
        phase_metrics[name] = {"candidate_count": len(rows),
                               "x_selected": _cohort_metrics(rows, outcome_by_key, x_group="x_selected"),
                               "no_x_baseline": _cohort_metrics(rows, outcome_by_key, x_group="no_x_baseline"),
                               "range": [rows[0]["received_at"], rows[-1]["received_at"]] if rows else None}
    payload = {
        "schema_version": SCHEMA_VERSION, "engine_version": ENGINE_VERSION,
        "trial_version": TOKEN_TRIAL_VERSION, "lane": "solana_memecoin",
        "research_only": True, "execution_enabled": False,
        "discovery_input_sha256": input_sha256(discovery),
        "sell_observations_input_sha256": input_sha256(list(sell_observations or [])),
        "policy": {"horizons_minutes": list(HORIZONS_MINUTES), "primary_horizon_minutes": 60,
                   "embargo_minutes": embargo_minutes, "selection": "prospective eligible cohort",
                   "x_ablation": "predeclared X eligibility versus same eligible cohort without X"},
        "retained_outcomes": outcome.get("cohort", []),
        "cohort": cohort, "split": phase_metrics,
        "acceptance": {"status": "blocked", "blockers": _token_blockers(cohort, x_unknown=x_unknown,
                                                                            minimum_candidates=minimum_candidates)},
        "cadence": {"monitoring": "continuous event-driven", "walk_forward_revalidation": "daily",
                     "cohort_review": "weekly", "promotion_review": "monthly"},
        "parameter_policy": "frozen_model_free_no_optimization",
    }
    payload["trial_id"] = hashlib.sha256(_canonical(payload)).hexdigest()
    return payload


def build_report(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Build a report from ``{bars, discovery, sell_observations}`` input."""
    if not isinstance(payload, Mapping):
        raise WalkForwardError("input must be an object")
    result: dict[str, Any] = {"schema_version": SCHEMA_VERSION, "engine_version": ENGINE_VERSION}
    if "bars" in payload:
        result["liquid"] = walk_forward_liquid(payload["bars"])
    if "discovery" in payload:
        result["tokens"] = walk_forward_tokens(payload["discovery"], payload.get("sell_observations", []))
    if len(result) == 2:
        raise WalkForwardError("input must include bars and/or discovery")
    result["input_sha256"] = input_sha256(payload)
    result["research_only"] = True
    result["execution_enabled"] = False
    return result


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    try:
        raw = Path(args.input).read_bytes()
        if len(raw) > 5_000_000:
            raise WalkForwardError("input exceeds 5,000,000 bytes")
        payload = json.loads(raw)
        result = build_report(payload)
        output = Path(args.output)
        if os.path.lexists(output):
            raise WalkForwardError("output already exists")
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(_canonical(result) + b"\n")
    except (WalkForwardError, ReplayError, OutcomeError, OSError, ValueError) as exc:
        print(str(exc))
        return 1
    print(json.dumps({"status": "ok", "trial_ids": [value["trial_id"] for key, value in result.items()
                     if key in ("liquid", "tokens")]}, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = ["build_report", "input_sha256", "main", "walk_forward_liquid", "walk_forward_tokens"]
