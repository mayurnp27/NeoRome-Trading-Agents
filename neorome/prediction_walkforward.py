"""Timestamp-correct, model-free walk-forward replay for binary contracts.

The replay consumes already archived observations.  It never fetches data,
fits a model, or mutates a ledger.  A caller supplies a conservative,
past-only probability bound for each event; the fixed policy buys one whole
contract when the lower-bound edge clears the registered threshold and holds
through explicit finalized settlement.  The output separates development,
validation, and untouched holdout results and includes all modeled fees.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
import hashlib
import json
from typing import Any, Mapping, Sequence


UTC = timezone.utc
VERSION = "prediction-walkforward-v1"
_ONE = Decimal("1")


class WalkForwardError(ValueError):
    """Replay input is malformed or violates causal ordering."""


def _time(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise WalkForwardError(f"{field} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise WalkForwardError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise WalkForwardError(f"{field} must include a timezone")
    return parsed.astimezone(UTC)


def _decimal(value: Any, field: str, *, low: Decimal | None = None, high: Decimal | None = None) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise WalkForwardError(f"{field} must be numeric")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise WalkForwardError(f"{field} must be numeric") from exc
    if not result.is_finite() or (low is not None and result < low) or (high is not None and result > high):
        raise WalkForwardError(f"{field} is outside its allowed range")
    return result


def _cents(value: Any, field: str, *, upper: int = 100) -> int:
    result = _decimal(value, field, low=Decimal("0"), high=Decimal(upper))
    if result != result.to_integral_value():
        raise WalkForwardError(f"{field} must be an integer number of cents")
    return int(result)


def _ceil(value: Decimal) -> int:
    return int(value.to_integral_value(rounding=ROUND_CEILING))


def _floor(value: Decimal) -> int:
    return int(value.to_integral_value(rounding=ROUND_FLOOR))


def _hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _fee(fees: Mapping[str, Any], action: str) -> int:
    if not isinstance(fees, Mapping) or not isinstance(fees.get("version"), str) or not fees["version"].strip():
        raise WalkForwardError("versioned fee schedule is required")
    return _ceil(_decimal(fees.get(f"{action}_fee_cents_per_contract"), f"fees.{action}", low=Decimal("0")))


def _event(event: Mapping[str, Any], index: int, *, fees: Mapping[str, Any], threshold: int) -> dict[str, Any]:
    required = ("event_id", "decision_at", "probability_lower_bound", "probability_known_at", "ask_cents", "outcome", "settlement_at")
    missing = [field for field in required if field not in event]
    if missing:
        raise WalkForwardError(f"event {index} missing " + ", ".join(missing))
    decision = _time(event["decision_at"], f"event[{index}].decision_at")
    known = _time(event["probability_known_at"], f"event[{index}].probability_known_at")
    settlement = _time(event["settlement_at"], f"event[{index}].settlement_at")
    if known > decision:
        raise WalkForwardError(f"event {index} probability is known after decision")
    if settlement <= decision:
        raise WalkForwardError(f"event {index} settlement is not after decision")
    event_id = event["event_id"]
    if not isinstance(event_id, str) or not event_id.strip():
        raise WalkForwardError(f"event {index} event_id is required")
    lower = _decimal(event["probability_lower_bound"], f"event[{index}].probability_lower_bound", low=Decimal("0"), high=_ONE)
    ask = _cents(event["ask_cents"], f"event[{index}].ask_cents")
    outcome = event["outcome"]
    if outcome not in ("YES", "NO", "yes", "no", 0, 1, True, False):
        raise WalkForwardError(f"event {index} outcome must be binary")
    outcome_yes = outcome in ("YES", "yes", 1, True)
    entry_fee, settlement_fee = _fee(fees, "entry"), _fee(fees, "settlement")
    conservative_edge = _floor(lower * Decimal(100)) - ask - settlement_fee - entry_fee
    trade = conservative_edge >= threshold
    net = (100 if outcome_yes else 0) - ask - entry_fee - settlement_fee if trade else 0
    probability = _decimal(event.get("probability", lower), f"event[{index}].probability", low=Decimal("0"), high=_ONE)
    brier = (probability - (Decimal(1) if outcome_yes else Decimal(0))) ** 2
    return {"event_id": event_id, "decision_at": decision, "settlement_at": settlement, "probability": probability,
            "outcome_yes": outcome_yes, "ask_cents": ask, "edge_cents": conservative_edge, "traded": trade,
            "net_pnl_cents": net, "brier": brier, "fee_version": fees["version"]}


def _summary(rows: Sequence[Mapping[str, Any]], *, name: str, start: int, end: int) -> dict[str, Any]:
    subset = list(rows[start:end])
    pnl = sum(int(row["net_pnl_cents"]) for row in subset)
    trades = sum(bool(row["traded"]) for row in subset)
    wins = sum(bool(row["traded"]) and bool(row["outcome_yes"]) for row in subset)
    brier = sum((row["brier"] for row in subset), Decimal("0")) / Decimal(len(subset)) if subset else None
    running = 0
    peak = 0
    drawdown = 0
    for row in subset:
        running += int(row["net_pnl_cents"])
        peak = max(peak, running)
        drawdown = max(drawdown, peak - running)
    return {"name": name, "events": len(subset), "trades": trades, "wins": wins, "net_pnl_cents": pnl,
            "max_drawdown_cents": drawdown, "brier": str(brier) if brier is not None else None,
            "first_decision_at": subset[0]["decision_at"].isoformat().replace("+00:00", "Z") if subset else None,
            "last_settlement_at": subset[-1]["settlement_at"].isoformat().replace("+00:00", "Z") if subset else None}


def replay(
    events: Sequence[Mapping[str, Any]],
    *,
    fee_schedule: Mapping[str, Any],
    edge_floor_cents: int = 3,
    development_events: int = 30,
    validation_events: int = 10,
    holdout_events: int = 20,
) -> dict[str, Any]:
    """Replay chronological events and return immutable, split results."""
    if not isinstance(events, Sequence) or isinstance(events, (str, bytes)):
        raise WalkForwardError("events must be a sequence")
    if isinstance(edge_floor_cents, bool) or not isinstance(edge_floor_cents, int) or edge_floor_cents < 0:
        raise WalkForwardError("edge_floor_cents must be a nonnegative integer")
    if any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in (development_events, validation_events, holdout_events)):
        raise WalkForwardError("split sizes must be positive integers")
    rows = [_event(event, index, fees=fee_schedule, threshold=edge_floor_cents) for index, event in enumerate(events)]
    if len({row["event_id"] for row in rows}) != len(rows):
        raise WalkForwardError("event IDs must be unique")
    if any(rows[index]["decision_at"] >= rows[index + 1]["decision_at"] for index in range(len(rows) - 1)):
        raise WalkForwardError("events must be strictly chronological by decision time")
    required = development_events + validation_events + holdout_events
    if len(rows) < required:
        raise WalkForwardError(f"at least {required} chronological events are required")
    splits = {"development": (0, development_events), "validation": (development_events, development_events + validation_events),
              "holdout": (development_events + validation_events, required)}
    result = {"schema_version": 1, "version": VERSION, "strategy": "fixed-lower-bound-binary-taker-v1",
              "edge_floor_cents": edge_floor_cents, "fee_schedule": dict(fee_schedule),
              "input_sha256": _hash(events), "events": required,
              "splits": {name: _summary(rows, name=name, start=start, end=end) for name, (start, end) in splits.items()},
              "chronological": True, "profitability_validated": False,
              "research_notice": "Replay evidence is limited to supplied events; no live or venue fill claim."}
    return result


replay_walk_forward = replay


__all__ = ["WalkForwardError", "replay", "replay_walk_forward"]
