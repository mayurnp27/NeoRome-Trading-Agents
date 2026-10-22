"""Read-only reporting for the isolated Solana memecoin paper program."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import secrets
import sqlite3
from typing import Any, Mapping

UTC = timezone.utc
REPORT_VERSION = "memecoin-program-report-v1"
PROGRAM_TARGET_NAV_CENTS = 70_000
MARK_FRESHNESS_SECONDS = 90


def _iso(now: datetime | None = None) -> str:
    return (now or datetime.now(UTC)).astimezone(UTC).isoformat().replace("+00:00", "Z")


def _read_candidates(root: Path) -> list[Mapping[str, Any]]:
    return _read_json_records(root / "candidates")[0]


def _read_json_records(directory: Path, *, require_horizons: bool = False,
                       require_cycle_identity: bool = False) -> tuple[list[Mapping[str, Any]], int, int]:
    """Read bounded evidence files and retain valid/invalid file counts."""
    if not directory.is_dir() or directory.is_symlink():
        return [], 0, 0
    valid: list[Mapping[str, Any]] = []
    valid_count = invalid_count = 0
    for path in sorted(directory.glob("*.json")):
        try:
            if path.is_symlink() or not path.is_file() or path.stat().st_size > 2_000_000:
                invalid_count += 1
                continue
            value = json.loads(path.read_text())
            if (not isinstance(value, Mapping)
                    or (require_horizons and not isinstance(value.get("horizons"), Mapping))
                    or (require_cycle_identity and (not isinstance(value.get("cycle_id"), str)
                        or not isinstance(value.get("schema_version"), str)))):
                invalid_count += 1
                continue
        except (OSError, UnicodeError, ValueError):
            invalid_count += 1
            continue
        valid.append(value)
        valid_count += 1
    return valid, valid_count, invalid_count


def _outcome_statuses(root: Path) -> dict[str, int]:
    result: Counter[str] = Counter()
    records, _, _ = _read_json_records(root / "outcomes", require_horizons=True)
    for raw in records:
        horizons = raw.get("horizons") if isinstance(raw, Mapping) else None
        if not isinstance(horizons, Mapping):
            continue
        for item in horizons.values():
            if isinstance(item, Mapping) and isinstance(item.get("status"), str):
                result[item["status"]] += 1
    return dict(sorted(result.items()))


def _rate(numerator: int, denominator: int) -> float | None:
    return None if denominator == 0 else numerator / denominator


def _m0_eligible(candidate: Mapping[str, Any]) -> bool:
    experiments = candidate.get("experiments")
    return isinstance(experiments, list) and any(
        isinstance(item, Mapping) and item.get("cohort_id") == "M0" and item.get("status") == "eligible"
        for item in experiments
    )


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp requires timezone")
    return parsed.astimezone(UTC)


def _ledger(ledger_path: Path, *, now: datetime | None = None) -> dict[str, Any]:
    if not ledger_path.is_file() or ledger_path.is_symlink():
        return {"status": "ledger_missing"}
    connection = sqlite3.connect(f"file:{ledger_path}?mode=ro", uri=True)
    try:
        connection.row_factory = sqlite3.Row
        account = connection.execute("SELECT * FROM accounts WHERE desk='crypto'").fetchone()
        if account is None:
            return {"status": "crypto_account_missing"}
        fills = connection.execute("SELECT * FROM fills WHERE desk='crypto'").fetchall()
        closes = connection.execute("""SELECT c.* FROM closes c JOIN positions p USING(position_id)
                                     WHERE p.desk='crypto'""").fetchall()
        positions = connection.execute("SELECT * FROM positions WHERE desk='crypto' AND state='open'").fetchall()
        open_ids = {row["position_id"] for row in positions}
        latest = connection.execute(
            "SELECT * FROM mark_batches ORDER BY julianday(as_of) DESC,rowid DESC LIMIT 1"
        ).fetchone()
        mark_rows = []
        if latest is not None:
            mark_rows = connection.execute(
                "SELECT * FROM market_marks WHERE batch_id=?", (latest["batch_id"],)
            ).fetchall()
        marked_ids = {row["position_id"] for row in mark_rows}
        report_now = (now or datetime.now(UTC)).astimezone(UTC)
        coverage_status = "not_applicable"
        coverage_reason = None
        marks_fresh: bool | None = None
        marked = 0
        if open_ids:
            if latest is None:
                coverage_status = "missing"
                coverage_reason = "no_mark_batch"
                marks_fresh = False
            elif marked_ids != open_ids:
                coverage_status = "incomplete"
                coverage_reason = "open_position_coverage_mismatch"
                marks_fresh = False
            else:
                try:
                    source_times = [_parse_timestamp(row["source_timestamp"]) for row in mark_rows]
                    if any(source > report_now for source in source_times):
                        coverage_status = "future"
                        coverage_reason = "mark_source_from_future"
                        marks_fresh = False
                    elif any((report_now - source).total_seconds() > MARK_FRESHNESS_SECONDS
                             for source in source_times):
                        coverage_status = "stale"
                        coverage_reason = "mark_source_stale"
                        marks_fresh = False
                    else:
                        coverage_status = "complete"
                        marks_fresh = True
                except (TypeError, ValueError):
                    coverage_status = "invalid"
                    coverage_reason = "mark_source_timestamp_invalid"
                    marks_fresh = False
            if coverage_status == "complete":
                marked = sum(row["liquidation_value_cents"] for row in mark_rows)
        else:
            # With no open position there is no liquidation value to estimate;
            # account cash is the complete sleeve NAV.
            marks_fresh = True
        realized = sum(row["realized_pnl_cents"] for row in closes)
        entry_fees = sum(row["fee_cents"] for row in fills)
        close_fees = sum(row["fee_cents"] for row in closes)
        gross_close_proceeds = sum(row["proceeds_cents"] for row in closes)
        fill_links = []
        for row in fills:
            candidate_sha256 = None
            cohort_id = None
            try:
                proposal = connection.execute(
                    "SELECT payload FROM proposals WHERE proposal_id=?", (row["proposal_id"],)
                ).fetchone()
                payload = json.loads(proposal["payload"]) if proposal is not None else None
                metadata = payload.get("metadata") if isinstance(payload, Mapping) else None
                token_meta = metadata.get("token_memecoin_v1") if isinstance(metadata, Mapping) else None
                value = token_meta.get("candidate_content_sha256") if isinstance(token_meta, Mapping) else None
                if isinstance(value, str):
                    candidate_sha256 = value
                if isinstance(token_meta, Mapping) and isinstance(token_meta.get("cohort_id"), str):
                    cohort_id = token_meta["cohort_id"]
            except (TypeError, ValueError, UnicodeError, sqlite3.Error):
                candidate_sha256 = None
            fill_links.append({"fill_id": row["fill_id"], "candidate_content_sha256": candidate_sha256,
                               "cohort_id": cohort_id})
        sleeve_nav = (account["available_cash_cents"] + account["reserved_cash_cents"] + marked
                      if coverage_status in {"complete", "not_applicable"} else None)
        net_pnl = None if sleeve_nav is None else sleeve_nav - account["initial_cash_cents"]
        target_gap = None if sleeve_nav is None else max(0, PROGRAM_TARGET_NAV_CENTS - sleeve_nav)
        return {
            "status": "ok",
            "initial_cash_cents": account["initial_cash_cents"],
            "available_cash_cents": account["available_cash_cents"],
            "reserved_cash_cents": account["reserved_cash_cents"],
            "open_positions": len(positions),
            "modeled_fills": len(fills),
            "closed_positions": len(closes),
            "entry_cost_cents": sum(row["total_cost_cents"] for row in fills),
            "entry_fees_cents": entry_fees,
            "close_fees_cents": close_fees,
            "gross_close_proceeds_cents": gross_close_proceeds,
            "total_modeled_fees_cents": entry_fees + close_fees,
            "realized_pnl_cents": realized,
            "marked_open_value_cents": marked if sleeve_nav is not None else None,
            "sleeve_nav_cents": sleeve_nav,
            "net_pnl_cents": net_pnl,
            "target_nav_cents": PROGRAM_TARGET_NAV_CENTS,
            "target_net_gain_cents": PROGRAM_TARGET_NAV_CENTS - account["initial_cash_cents"],
            "target_gap_cents": target_gap,
            "target_reached": None if sleeve_nav is None else sleeve_nav >= PROGRAM_TARGET_NAV_CENTS,
            "mark_coverage": {
                "status": coverage_status,
                "reason": coverage_reason,
                "fresh": marks_fresh,
                "open_positions": len(open_ids),
                "marked_positions": len(marked_ids & open_ids),
                "batch_id": latest["batch_id"] if latest is not None else None,
                "as_of": latest["as_of"] if latest is not None else None,
            },
            "mark_coverage_status": coverage_status,
            "marks_fresh": marks_fresh,
            "live_orders": 0,
            "fill_links": fill_links,
        }
    finally:
        connection.close()


def build_report(state_dir: str | Path, ledger_path: str | Path, *, now: datetime | None = None) -> dict[str, Any]:
    state_root = Path(state_dir)
    candidates, valid_candidate_files, invalid_candidate_files = _read_json_records(state_root / "candidates")
    outcomes, valid_outcome_files, invalid_outcome_files = _read_json_records(state_root / "outcomes", require_horizons=True)
    cycles, valid_cycle_files, invalid_cycle_files = _read_json_records(
        state_root / "cycles", require_cycle_identity=True
    )
    cohort_status: Counter[str] = Counter()
    cohort_observations: Counter[str] = Counter()
    reasons: Counter[str] = Counter()
    social_unavailable = 0
    entry_routes = 0
    sell_preflights = 0
    baseline_eligible = 0
    m0_eligible = 0
    for candidate in candidates:
        if candidate.get("entry_quote") is not None:
            entry_routes += 1
        if candidate.get("sell_preflight") is not None:
            sell_preflights += 1
        if candidate.get("eligible_for_internal_paper") is True:
            baseline_eligible += 1
        if _m0_eligible(candidate):
            m0_eligible += 1
        social = candidate.get("social")
        if isinstance(social, Mapping) and social.get("channel_status") == "unavailable":
            social_unavailable += 1
        for reason in candidate.get("rejection_reasons", []):
            if isinstance(reason, str):
                reasons[reason] += 1
        for item in candidate.get("experiments", []):
            if not isinstance(item, Mapping):
                continue
            cohort = item.get("cohort_id")
            status = item.get("status")
            if isinstance(cohort, str):
                cohort_observations[cohort] += 1
                if isinstance(status, str):
                    cohort_status[f"{cohort}:{status}"] += 1
    paper = _ledger(Path(ledger_path), now=now)
    cycle_totals = {
        "discovery_cycles": len(cycles),
        "provider_profiles": 0,
        "valid_unique_mints": 0,
        "selected_slots": 0,
        "interval_skips": 0,
        "processed_observations": 0,
        "retained_observations": 0,
        "processed_retained_observations": 0,
        "x_queried": 0,
        "x_unavailable": 0,
        "entry_routes": 0,
        "sell_preflights": 0,
        "eligible_observations": 0,
        "modeled_fills": 0,
        "modeled_closes": 0,
    }
    for cycle in cycles:
        for field in ("provider_profile_count", "valid_unique_solana_mint_count", "selected_count",
                      "interval_skips", "processed_count", "retained_count", "x_queries",
                      "x_unavailable", "entry_route_count", "sell_preflight_count", "m0_eligible_count"):
            value = cycle.get(field)
            if type(value) is int and value >= 0:
                destination = {
                    "provider_profile_count": "provider_profiles",
                    "valid_unique_solana_mint_count": "valid_unique_mints",
                    "selected_count": "selected_slots",
                    "processed_count": "processed_observations",
                    "retained_count": "retained_observations",
                    "x_queries": "x_queried",
                    "x_unavailable": "x_unavailable",
                    "entry_route_count": "entry_routes",
                    "sell_preflight_count": "sell_preflights",
                    "m0_eligible_count": "eligible_observations",
                }.get(field)
                if destination is not None:
                    cycle_totals[destination] += value
    # The candidate ledger is the authoritative quote/fill denominator.  Cycle
    # aggregates are still published for discovery coverage, but are not used
    # to infer a fill attribution across retries.
    modeled_fills = int(paper.get("modeled_fills", 0) or 0)
    modeled_closes = int(paper.get("closed_positions", 0) or 0)
    cycle_totals["modeled_fills"] = modeled_fills
    cycle_totals["modeled_closes"] = modeled_closes
    cycle_totals["processed_retained_observations"] = cycle_totals["retained_observations"]
    cycle_totals.update({
        "provider_profile_count": cycle_totals["provider_profiles"],
        "valid_unique_solana_mint_count": cycle_totals["valid_unique_mints"],
        "selected_count": cycle_totals["selected_slots"],
        "interval_skip_count": cycle_totals["interval_skips"],
        "processed_count": cycle_totals["processed_observations"],
        "retained_count": cycle_totals["retained_observations"],
        "x_query_count": cycle_totals["x_queried"],
        "x_unavailable_count": cycle_totals["x_unavailable"],
        "eligible_observation_count": cycle_totals["eligible_observations"],
    })
    candidate_by_hash = {
        candidate.get("content_sha256"): candidate for candidate in candidates
        if isinstance(candidate.get("content_sha256"), str)
    }
    fill_attributions = []
    for link in paper.get("fill_links", []):
        if not isinstance(link, Mapping):
            continue
        content_hash = link.get("candidate_content_sha256")
        candidate = candidate_by_hash.get(content_hash)
        if not isinstance(candidate, Mapping):
            # No attribution is better than guessing from mint, proposal id,
            # or chronological proximity.
            fill_attributions.append({"fill_id": link.get("fill_id"), "attributed": False,
                                      "candidate_content_sha256": None})
            continue
        fill_attributions.append({"fill_id": link.get("fill_id"), "attributed": True,
                                  "candidate_content_sha256": content_hash,
                                  "cohort_id": link.get("cohort_id"),
                                  "m0_eligible": (_m0_eligible(candidate)
                                                  if link.get("cohort_id") == "M0" else None)})
    quote_funnel = {
        "entry_quotes": entry_routes,
        "sell_preflights": sell_preflights,
        "m0_eligible_observations": m0_eligible,
        "modeled_fills": modeled_fills,
        "attributed_modeled_fills": sum(1 for item in fill_attributions if item.get("attributed")),
        "attributed_m0_modeled_fills": sum(
            1 for item in fill_attributions
            if item.get("attributed") and item.get("cohort_id") == "M0" and item.get("m0_eligible") is True
        ),
        "rates": {
            "entry_to_sell_preflight": _rate(sell_preflights, entry_routes),
            "sell_preflight_to_m0_eligible": _rate(m0_eligible, sell_preflights),
            "m0_eligible_to_modeled_fill": _rate(
                sum(1 for item in fill_attributions
                    if item.get("attributed") and item.get("cohort_id") == "M0"
                    and item.get("m0_eligible") is True), m0_eligible
            ),
            "entry_to_modeled_fill": _rate(
                sum(1 for item in fill_attributions if item.get("attributed")), entry_routes
            ),
        },
    }
    file_counts = {
        "candidate": {"valid": valid_candidate_files, "invalid": invalid_candidate_files},
        "outcome": {"valid": valid_outcome_files, "invalid": invalid_outcome_files},
        "cycle": {"valid": valid_cycle_files, "invalid": invalid_cycle_files},
    }
    file_counts["candidates"] = dict(file_counts["candidate"])
    file_counts["outcomes"] = dict(file_counts["outcome"])
    file_counts["cycles"] = dict(file_counts["cycle"])
    execution = {key: value for key, value in paper.items() if key != "fill_links"}
    execution["modeled_fill_attributions"] = fill_attributions
    return {
        "schema_version": REPORT_VERSION,
        "generated_at": _iso(now),
        "mode": "internal_paper_only",
        "discovery": {
            "retained_candidates": len(candidates),
            "valid_candidate_files": valid_candidate_files,
            "invalid_candidate_files": invalid_candidate_files,
            "valid_candidates": valid_candidate_files,
            "invalid_candidates": invalid_candidate_files,
            "valid_outcome_files": valid_outcome_files,
            "invalid_outcome_files": invalid_outcome_files,
            "valid_outcomes": valid_outcome_files,
            "invalid_outcomes": invalid_outcome_files,
            "valid_cycle_files": valid_cycle_files,
            "invalid_cycle_files": invalid_cycle_files,
            "valid_cycles": valid_cycle_files,
            "invalid_cycles": invalid_cycle_files,
            "file_counts": file_counts,
            **cycle_totals,
            "baseline_eligible": baseline_eligible,
            "m0_eligible_observations": m0_eligible,
            "entry_route_observations": entry_routes,
            "sell_preflight_observations": sell_preflights,
            "x_unavailable_observations": social_unavailable,
            "cohort_observations": dict(sorted(cohort_observations.items())),
            "cohort_status": dict(sorted(cohort_status.items())),
            "top_gate_failures": dict(reasons.most_common(20)),
            "prospective_sell_outcomes": _outcome_statuses(state_root),
            "quote_to_modeled_fill": quote_funnel,
            "funnel": quote_funnel,
        },
        "execution": execution,
    }


def _write(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            descriptor = -1
            json.dump(value, stream, sort_keys=True, separators=(",", ":"), allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", required=True)
    parser.add_argument("--ledger", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    report = build_report(args.state, args.ledger)
    _write(Path(args.output), report)
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
