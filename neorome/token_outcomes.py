"""Prospective, offline memecoin outcome and sellability evaluation.

This module consumes a frozen discovery record and later Jupiter quote
observations.  It does not discover, request, sign, submit, admit, or schedule
anything.  Every discovered candidate is retained in the returned cohort,
including screen rejections and disappeared pools.

Sell observations are selected prospectively at the preregistered 15, 60 and
240 minute horizons.  The first observation received in the bounded window is
the only observation used for that horizon.  A missing observation is kept
unresolved; an unusable observation is a route failure.  Neither is converted
to a last-price or invented fill.

Candidate discovery vintages must be supplied explicitly as ``known_at`` and
``received_at``.  Local persistence timestamps such as ``recorded_at`` and
legacy ``observed_at`` fields are never promoted into causal evidence.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any, Mapping, Sequence

from .jupiter_quotes import QuoteError, USDC, raw_amount, save_new, validate_mint


HORIZONS_MINUTES = (15, 60, 240)
# Public spelling used by cohort callers; the tuple itself is immutable.
HORIZONS = HORIZONS_MINUTES
DEFAULT_HORIZON_TOLERANCE_SECONDS = 30
SCHEMA_VERSION = 1
POLICY_VERSION = "token-outcomes-v1"
_HASH = re.compile(r"[0-9a-fA-F]{64}\Z")


class OutcomeError(ValueError):
    """The supplied cohort or quote evidence is malformed."""


def _timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise OutcomeError(f"{field} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise OutcomeError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise OutcomeError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise OutcomeError("input contains non-JSON data") from exc


def input_sha256(value: Any) -> str:
    """Hash a JSON value deterministically for provenance of offline inputs."""
    return hashlib.sha256(_canonical(value)).hexdigest()


def _source_hash(record: Mapping[str, Any], field: str) -> str:
    value = record.get("response_sha256", record.get("source_sha256"))
    if not isinstance(value, str) or _HASH.fullmatch(value) is None:
        raise OutcomeError(f"{field} source hash is required")
    return value.lower()


def _positive_raw(value: Any, field: str) -> int:
    try:
        text = raw_amount(value)
    except QuoteError as exc:
        raise OutcomeError(f"{field} must be a positive integer in base units") from exc
    return int(text)


def _nonnegative_raw(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise OutcomeError(f"{field} must be a nonnegative integer in base units")
    if isinstance(value, str) and (not value or not value.isdecimal()):
        raise OutcomeError(f"{field} must be a nonnegative integer in base units")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise OutcomeError(f"{field} must be a nonnegative integer in base units") from exc
    if result < 0 or result > 2**64 - 1:
        raise OutcomeError(f"{field} must be a nonnegative integer in base units")
    return result


def _ceil_usdc(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise OutcomeError(f"{field} must be an exact nonnegative decimal")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise OutcomeError(f"{field} must be an exact nonnegative decimal") from exc
    if not number.is_finite() or number < 0:
        raise OutcomeError(f"{field} must be an exact nonnegative decimal")
    return int((number * Decimal(1_000_000)).to_integral_value(rounding=ROUND_CEILING))


def _identity(candidate: Mapping[str, Any]) -> tuple[str, str]:
    chain = candidate.get("chain")
    mint = candidate.get("mint")
    if not isinstance(chain, str) or not chain.strip():
        raise OutcomeError("candidate chain is required")
    try:
        mint = validate_mint(mint)
    except (QuoteError, TypeError) as exc:
        raise OutcomeError("candidate mint is invalid") from exc
    return chain, mint


def _candidate_times(candidate: Mapping[str, Any], index: int) -> tuple[datetime, datetime]:
    # Discovery vintages are causal evidence.  Do not use ``observed_at`` or
    # a local ``recorded_at`` as a substitute: those fields do not establish
    # when the provider made the candidate observable.
    known_raw = candidate.get("known_at")
    received_raw = candidate.get("received_at")
    if known_raw is None:
        raise OutcomeError(f"candidate[{index}].known_at is required")
    if received_raw is None:
        raise OutcomeError(f"candidate[{index}].received_at is required")
    known = _timestamp(known_raw, f"candidate[{index}].known_at")
    received = _timestamp(received_raw, f"candidate[{index}].received_at")
    if received < known:
        raise OutcomeError(f"candidate[{index}] received_at precedes known_at")
    return known, received


def _entry_observation(candidate: Mapping[str, Any]) -> Mapping[str, Any] | None:
    for key in ("entry_observation", "entry", "entry_quote"):
        value = candidate.get(key)
        if isinstance(value, Mapping):
            return value
    quotes = candidate.get("quotes")
    if isinstance(quotes, Mapping):
        observations = quotes.get("observations")
        if isinstance(observations, Sequence) and not isinstance(observations, (str, bytes)):
            if observations and isinstance(observations[0], Mapping):
                return observations[0]
    return None


def _quote(record: Mapping[str, Any], label: str) -> Mapping[str, Any]:
    quote = record.get("quote")
    if not isinstance(quote, Mapping):
        raise OutcomeError(f"{label} quote object is required")
    return quote


def _impact(quote: Mapping[str, Any], label: str) -> str:
    if quote.get("priceImpactUnit") != "percentage_points":
        raise OutcomeError(f"{label} current priceImpact units are required")
    value = quote.get("priceImpact")
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise OutcomeError(f"{label} priceImpact is invalid")
    try:
        impact = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise OutcomeError(f"{label} priceImpact is invalid") from exc
    if not impact.is_finite() or abs(impact) > Decimal("2"):
        raise OutcomeError(f"{label} priceImpact is outside the replay bound")
    return str(impact)


def _minimum_output(quote: Mapping[str, Any], label: str) -> tuple[int, int]:
    output = _positive_raw(quote.get("outAmount"), f"{label}.outAmount")
    minimum = _positive_raw(quote.get("otherAmountThreshold"),
                            f"{label}.otherAmountThreshold")
    if minimum > output:
        raise OutcomeError(f"{label} minimum output exceeds quote output")
    return output, minimum


def _validate_route_times(record: Mapping[str, Any], label: str) -> tuple[datetime, datetime]:
    requested = _timestamp(record.get("requested_at"), f"{label}.requested_at")
    received = _timestamp(record.get("received_at"), f"{label}.received_at")
    if received < requested:
        raise OutcomeError(f"{label}.received_at precedes requested_at")
    return requested, received


def _entry(candidate: Mapping[str, Any], chain: str, mint: str, candidate_received: datetime) -> dict[str, Any]:
    record = _entry_observation(candidate)
    if record is None:
        raise OutcomeError("entry evidence is missing")
    requested, received = _validate_route_times(record, "entry")
    if requested < candidate_received:
        raise OutcomeError("entry was requested before candidate receipt")
    if record.get("chain", chain) != chain:
        raise OutcomeError("entry chain identity mismatch")
    quote = _quote(record, "entry")
    if (quote.get("inputMint"), quote.get("outputMint")) != (USDC, mint):
        raise OutcomeError("entry route identity mismatch")
    input_raw = _positive_raw(quote.get("inAmount"), "entry.inAmount")
    output, minimum = _minimum_output(quote, "entry")
    impact = _impact(quote, "entry")
    source_hash = _source_hash(record, "entry")
    return {"requested_at": _iso(requested), "received_at": _iso(received),
            "source_hash": source_hash, "input_usdc_raw": str(input_raw),
            "quoted_output_raw": str(output), "acquired_raw": str(minimum),
            "price_impact": impact, "chain": chain, "mint": mint}


def _route_pair(record: Mapping[str, Any]) -> tuple[Any, Any]:
    quote = record.get("quote")
    if not isinstance(quote, Mapping):
        return None, None
    return quote.get("inputMint"), quote.get("outputMint")


def _route_in_window(record: Mapping[str, Any], chain: str, mint: str, start: datetime,
                     end: datetime, entry_received: datetime) -> bool:
    try:
        requested, received = _validate_route_times(record, "sell")
    except OutcomeError:
        return False
    quote = record.get("quote")
    if not isinstance(quote, Mapping):
        return False
    if record.get("chain", chain) != chain:
        return False
    # A route for another token is unrelated evidence.  Same-pair wrong-sized
    # routes are deliberately retained so they become route_failure below.
    if quote.get("inputMint") != mint or quote.get("outputMint") != USDC:
        return False
    return (requested >= entry_received and received >= start and received <= end)


def _route_failure_reason(record: Mapping[str, Any], chain: str, mint: str, quantity: int,
                          entry_received: datetime) -> tuple[str, dict[str, Any] | None]:
    try:
        requested, received = _validate_route_times(record, "sell")
        quote = _quote(record, "sell")
        if record.get("chain", chain) != chain:
            raise OutcomeError("sell route chain identity mismatch")
        if (quote.get("inputMint"), quote.get("outputMint")) != (mint, USDC):
            raise OutcomeError("sell route identity mismatch")
        amount = _positive_raw(quote.get("inAmount"), "sell.inAmount")
        if amount != quantity:
            raise OutcomeError("sell quantity does not match conservative acquired quantity")
        output, minimum = _minimum_output(quote, "sell")
        impact = _impact(quote, "sell")
        source_hash = _source_hash(record, "sell")
        if requested < entry_received:
            raise OutcomeError("sell route was requested before entry receipt")
        return "", {"requested_at": _iso(requested), "received_at": _iso(received),
                     "source_hash": source_hash, "input_token_raw": str(amount),
                     "quoted_output_usdc_raw": str(output),
                     "minimum_output_usdc_raw": str(minimum),
                     "price_impact": impact}
    except (OutcomeError, KeyError, TypeError) as exc:
        return str(exc), None


def _costs(value: Mapping[str, Any] | None) -> dict[str, Any]:
    if value is None:
        value = {"version": "token-outcomes-costs-v1",
                 "excludes_embedded_route_fees": True,
                 "entry_extra_usdc_raw": "0", "exit_extra_usdc_raw": "0"}
    if not isinstance(value, Mapping):
        raise OutcomeError("versioned extra costs are required")
    version = value.get("version")
    if not isinstance(version, str) or not version.strip():
        raise OutcomeError("costs.version is required")
    if value.get("excludes_embedded_route_fees") is not True:
        raise OutcomeError("costs must exclude embedded route/platform fees")
    # Route/platform fee fields are already represented by Jupiter's quoted
    # amounts.  Refuse ambiguous additions instead of counting them twice.
    for key in ("route_fee_usdc_raw", "platform_fee_usdc_raw", "route_fee", "platform_fee"):
        if key in value:
            raise OutcomeError("route/platform fees cannot be added to embedded quote fees")

    def side(name: str) -> int:
        aliases = (f"{name}_extra_usdc_raw", f"additional_{name}_usdc_raw",
                   f"{name}_cost_usdc_raw", f"additional_{name}_usd")
        for key in aliases:
            if key in value and key.endswith("_usd"):
                return _ceil_usdc(value[key], f"costs.{key}")
            if key in value:
                return _nonnegative_raw(value[key], f"costs.{key}")
        # Permit the existing replay convention of explicit decimal USD costs.
        nested = value.get(f"{name}_cost")
        if isinstance(nested, Mapping) and "additional_usd" in nested:
            if nested.get("excludes_embedded_route_fees") is not True:
                raise OutcomeError(f"costs.{name}_cost must exclude embedded route fees")
            return _ceil_usdc(nested["additional_usd"], f"costs.{name}_cost.additional_usd")
        return 0

    entry_extra, exit_extra = side("entry"), side("exit")
    return {"version": version, "excludes_embedded_route_fees": True,
            "entry_extra_usdc_raw": str(entry_extra), "exit_extra_usdc_raw": str(exit_extra),
            "route_platform_fees_readded": False}


def _horizon_outcome(entry: Mapping[str, Any], routes: Sequence[Mapping[str, Any]],
                     chain: str, mint: str, horizon: int, tolerance: int,
                     costs: Mapping[str, Any]) -> dict[str, Any]:
    entry_received = _timestamp(entry["received_at"], "entry.received_at")
    target = entry_received + timedelta(minutes=horizon)
    until = target + timedelta(seconds=tolerance)
    candidates = [(index, route) for index, route in enumerate(routes)
                  if isinstance(route, Mapping)
                  and _route_in_window(route, chain, mint, target, until, entry_received)]
    result: dict[str, Any] = {"horizon_minutes": horizon, "target_at": _iso(target),
                              "window_end_at": _iso(until), "status": "unresolved_missing_route",
                              "route_observation": None, "minimum_output_usdc_raw": None,
                              "net_pnl_usdc_raw": None, "full_loss_stress_usdc_raw": None,
                              "full_loss_stress_is_scenario": True, "reasons": []}
    if not candidates:
        result["reasons"].append("missing sell route within bounded horizon window")
        return result
    # Observation order is temporal, then source position for tied receipts.
    candidates.sort(key=lambda item: (_timestamp(item[1]["received_at"], "sell.received_at"), item[0]))
    reason, route = _route_failure_reason(candidates[0][1], chain, mint,
                                          int(entry["acquired_raw"]), entry_received)
    if route is None:
        result["status"] = "route_failure"
        result["reasons"].append(reason)
        return result
    entry_extra = int(costs["entry_extra_usdc_raw"])
    exit_extra = int(costs["exit_extra_usdc_raw"])
    entry_input = int(entry["input_usdc_raw"])
    minimum = int(route["minimum_output_usdc_raw"])
    all_in = entry_input + entry_extra + exit_extra
    result.update(status="modeled_outcome", route_observation=route,
                  minimum_output_usdc_raw=str(minimum),
                  net_proceeds_usdc_raw=str(minimum - exit_extra),
                  entry_debit_usdc_raw=str(entry_input + entry_extra),
                  net_pnl_usdc_raw=str(minimum - all_in),
                  full_loss_stress_usdc_raw=str(-all_in),
                  costs_version=costs["version"],
                  reasons=[])
    return result


def _not_evaluated(status: str) -> list[dict[str, Any]]:
    return [{"horizon_minutes": horizon, "status": status,
             "route_observation": None, "minimum_output_usdc_raw": None,
             "net_pnl_usdc_raw": None, "full_loss_stress_usdc_raw": None,
             "full_loss_stress_is_scenario": True, "reasons": []}
            for horizon in HORIZONS_MINUTES]


def evaluate(discovery: Mapping[str, Any], sell_observations: Sequence[Mapping[str, Any]] | None = None,
             *, costs: Mapping[str, Any] | None = None,
             horizon_tolerance_seconds: int = DEFAULT_HORIZON_TOLERANCE_SECONDS) -> dict[str, Any]:
    """Evaluate one immutable discovery record against later sell quotes.

    The result is research evidence only.  ``horizon_tolerance_seconds`` is
    deliberately explicit in the returned policy so a later evaluator cannot
    silently widen the label window.
    """
    if not isinstance(discovery, Mapping):
        raise OutcomeError("discovery record must be an object")
    if type(horizon_tolerance_seconds) is not int or not 0 < horizon_tolerance_seconds <= 3600:
        raise OutcomeError("horizon tolerance must be an integer from 1 to 3600 seconds")
    candidates = discovery.get("candidates")
    if not isinstance(candidates, Sequence) or isinstance(candidates, (str, bytes)):
        raise OutcomeError("discovery.candidates must be a list")
    routes = [] if sell_observations is None else sell_observations
    if not isinstance(routes, Sequence) or isinstance(routes, (str, bytes)):
        raise OutcomeError("sell observations must be a list")
    cost_model = _costs(costs)
    discovery_hash = input_sha256(discovery)
    rows: list[dict[str, Any]] = []
    for index, raw_candidate in enumerate(candidates):
        if not isinstance(raw_candidate, Mapping):
            raise OutcomeError(f"candidate[{index}] must be an object")
        candidate = deepcopy(dict(raw_candidate))
        chain, mint = _identity(candidate)
        known, received = _candidate_times(candidate, index)
        rejections = candidate.get("screen_rejections", [])
        if not isinstance(rejections, Sequence) or isinstance(rejections, (str, bytes)):
            raise OutcomeError(f"candidate[{index}].screen_rejections must be a list")
        research_status = candidate.get("research_status", candidate.get("status", ""))
        disappeared = research_status in ("disappeared", "pool_disappeared") or candidate.get("disappeared") is True
        rejected = bool(rejections) or research_status in ("screen_rejected", "rejected")
        row: dict[str, Any] = {"cohort_index": index, "chain": chain, "mint": mint,
                               "known_at": _iso(known), "received_at": _iso(received),
                               "discovery_input_sha256": discovery_hash,
                               "research_status": research_status,
                               "screen_rejections": list(rejections),
                               "disappeared": disappeared, "trade_eligible": False,
                               "execution_enabled": False, "paper_admitted": False}
        if disappeared:
            row["status"] = "disappeared"
            row["horizons"] = _not_evaluated("not_evaluated_disappeared")
            rows.append(row)
            continue
        if rejected:
            row["status"] = "screen_rejected"
            row["horizons"] = _not_evaluated("not_evaluated_screen_rejected")
            rows.append(row)
            continue
        try:
            entry = _entry(candidate, chain, mint, received)
        except OutcomeError as exc:
            row["status"] = "entry_unresolved"
            row["entry"] = None
            row["horizons"] = _not_evaluated("not_evaluated_entry_unresolved")
            row["reasons"] = [str(exc)]
            rows.append(row)
            continue
        row["entry"] = entry
        row["status"] = "observed_pending"
        row["horizons"] = [_horizon_outcome(entry, routes, chain, mint, horizon,
                                             horizon_tolerance_seconds, cost_model)
                            for horizon in HORIZONS_MINUTES]
        rows.append(row)
    return {"schema_version": SCHEMA_VERSION, "policy_version": POLICY_VERSION,
            "policy": {"horizons_minutes": list(HORIZONS_MINUTES),
                       "horizon_tolerance_seconds": horizon_tolerance_seconds,
                       "selection": "first received same-pair route in [horizon,horizon+tolerance]",
                       "minimum_output_field": "otherAmountThreshold",
                       "price_impact_unit": "percentage_points"},
            "costs": cost_model, "discovery_input_sha256": discovery_hash,
            "sell_observations_input_sha256": input_sha256(list(routes)),
            "cohort": rows, "execution_enabled": False, "paper_admitted": False,
            "trade_rows": [], "not_a_trade_or_ledger": True}


def evaluate_input(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Evaluate the CLI envelope ``{discovery, sell_observations, ...}``."""
    if not isinstance(payload, Mapping):
        raise OutcomeError("input must be an object")
    discovery = payload.get("discovery", payload)
    routes = payload.get("sell_observations", payload.get("sell_routes", []))
    return evaluate(discovery, routes,
                    costs=payload.get("costs"),
                    horizon_tolerance_seconds=payload.get(
                        "horizon_tolerance_seconds", DEFAULT_HORIZON_TOLERANCE_SECONDS))


# Descriptive alias for callers that use the task language.
track_outcomes = evaluate


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    try:
        output = Path(args.output)
        if os.path.lexists(output):
            raise OutcomeError("output already exists")
        raw = Path(args.input).read_bytes()
        if len(raw) > 2_000_000:
            raise OutcomeError("input exceeds 2,000,000 bytes")
        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise OutcomeError("input is not valid UTF-8 JSON") from exc
        result = evaluate_input(payload)
        result["input_sha256"] = hashlib.sha256(raw).hexdigest()
        save_new(output, result)
    except (OutcomeError, OSError, ValueError) as exc:
        print(str(exc), file=__import__("sys").stderr)
        return 1
    print(json.dumps({"cohort": len(result["cohort"]), "execution_enabled": False,
                      "output": str(output)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
