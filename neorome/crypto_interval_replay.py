"""Deterministic, research-only replay for one binary crypto interval contract.

The input is a JSON object (or an equivalent mapping) containing one contract,
timestamped book observations, an explicit decision, and, optionally, a
finalized settlement.  This module deliberately has no network, broker,
ledger, clock, or order side effect.  A replay is evidence about the supplied
snapshots only and is never an execution-readiness claim.

The small public surface is :func:`replay` (also available as
``replay_json``).  Prices are dollars per contract and cash results are
integer cents.  Entry and scheduled-exit fills walk displayed depth.  A
settlement is accepted only when it is explicitly finalized and matches the
contract identity; an exchange candle cannot settle a contract here.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
import json
from typing import Any, Mapping, Sequence


UTC = timezone.utc
DEFAULT_IDEA_BUDGET_CENTS = 500
DEFAULT_PAPER_BUDGET_CENTS = 10_000
DEFAULT_MAX_QUOTE_AGE = timedelta(seconds=5)
DEFAULT_ORDER_LIFETIME = timedelta(seconds=5)
_CENT = Decimal("0.01")
_ONE = Decimal("1")


class ReplayError(ValueError):
    """The supplied replay input is malformed or cannot be replayed."""


def _timestamp(value: Any, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ReplayError(f"{field} must be an ISO-8601 timestamp") from exc
    else:
        raise ReplayError(f"{field} must be an ISO-8601 timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ReplayError(f"{field} must include a timezone")
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _decimal(value: Any, field: str, *, minimum: Decimal | None = None, maximum: Decimal | None = None) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise ReplayError(f"{field} must be a finite number")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ReplayError(f"{field} must be a finite number") from exc
    if not parsed.is_finite() or (minimum is not None and parsed < minimum) or (maximum is not None and parsed > maximum):
        raise ReplayError(f"{field} is outside its allowed range")
    return parsed


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ReplayError(f"{field} must be a positive integer")
    return value


def _ceil_cents(dollars: Decimal) -> int:
    return int((dollars * 100).to_integral_value(rounding=ROUND_CEILING))


def _floor_cents(dollars: Decimal) -> int:
    return int((dollars * 100).to_integral_value(rounding=ROUND_FLOOR))


def _canonical(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except (TypeError, ValueError) as exc:
        raise ReplayError("contract identity contains non-JSON data") from exc


def _load_input(value: Mapping[str, Any] | str | bytes) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ReplayError("JSON input is not UTF-8") from exc
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ReplayError("JSON input is invalid") from exc
        if isinstance(parsed, Mapping):
            return dict(parsed)
    raise ReplayError("input must be a JSON object or mapping")


def _required_mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ReplayError(f"{field} must be an object")
    return value


def _identity(contract: Mapping[str, Any]) -> dict[str, Any]:
    """Return the fields that every event must match exactly."""
    required = ("market_id", "outcome_id", "outcome", "terms", "source_identity", "open_at", "cutoff_at", "expiry_at")
    missing = [field for field in required if field not in contract]
    if missing:
        raise ReplayError("contract missing identity field(s): " + ", ".join(missing))
    outcome = contract["outcome"]
    if outcome not in ("YES", "NO", "yes", "no"):
        raise ReplayError("contract outcome must be YES or NO")
    if not str(contract["market_id"]).strip() or not str(contract["outcome_id"]).strip():
        raise ReplayError("contract identifiers must be non-empty")
    if contract["terms"] in (None, "", {}, []):
        raise ReplayError("contract terms must be explicit")
    if contract["source_identity"] in (None, "", {}, []):
        raise ReplayError("contract source_identity must be explicit")
    opened = _timestamp(contract["open_at"], "contract.open_at")
    cutoff = _timestamp(contract["cutoff_at"], "contract.cutoff_at")
    expiry = _timestamp(contract["expiry_at"], "contract.expiry_at")
    if not opened < cutoff <= expiry:
        raise ReplayError("contract lifecycle must satisfy open_at < cutoff_at <= expiry_at")
    # Hashing the exact JSON representation prevents an apparently equivalent
    # but differently worded settlement rule from silently entering replay.
    return {field: contract[field] for field in required}


def _identity_reasons(expected: Mapping[str, Any], event: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    for field, value in expected.items():
        if field not in event:
            reasons.append(f"identity_missing_{field}")
        elif _canonical(event[field]) != _canonical(value):
            reasons.append(f"identity_mismatch_{field}")
    return reasons


def _event_times(event: Mapping[str, Any], label: str) -> tuple[datetime, datetime]:
    event_at = _timestamp(event.get("event_at"), f"{label}.event_at")
    received_at = _timestamp(event.get("received_at"), f"{label}.received_at")
    if event_at > received_at:
        raise ReplayError(f"{label}.event_at cannot be after received_at")
    return event_at, received_at


def _levels(value: Any, field: str, *, ascending: bool) -> list[tuple[Decimal, int]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ReplayError(f"{field} must be a list")
    levels: list[tuple[Decimal, int]] = []
    seen_prices = set()
    for index, raw in enumerate(value):
        if not isinstance(raw, Mapping):
            raise ReplayError(f"{field}[{index}] must be an object")
        price = _decimal(raw.get("price"), f"{field}[{index}].price", minimum=Decimal("0"), maximum=_ONE)
        size = _positive_int(raw.get("size"), f"{field}[{index}].size")
        if price in seen_prices:
            raise ReplayError('duplicate depth price level')
        seen_prices.add(price)
        levels.append((price, size))
    levels.sort(key=lambda item: item[0], reverse=not ascending)
    return levels


def _book(event: Mapping[str, Any], label: str) -> dict[str, Any]:
    """Normalize either a level-list book or a one-level ask/bid snapshot."""
    asks_raw = event.get("asks")
    bids_raw = event.get("bids")
    if asks_raw is None and "ask" in event:
        asks_raw = [{"price": event["ask"], "size": event.get("ask_size", event.get("depth", 0))}]
    if bids_raw is None and "bid" in event:
        bids_raw = [{"price": event["bid"], "size": event.get("bid_size", event.get("depth", 0))}]
    # An entry snapshot may contain only executable asks and an exit snapshot
    # only executable bids. Missing sides are empty books; the caller then
    # reports a side-specific depth/gap rejection.
    if asks_raw is None:
        asks_raw = []
    if bids_raw is None:
        bids_raw = []
    asks = _levels(asks_raw, f"{label}.asks", ascending=True)
    bids = _levels(bids_raw, f"{label}.bids", ascending=False)
    if asks and bids and bids[0][0] >= asks[0][0]:
        raise ReplayError(f"{label} is crossed")
    return {"asks": asks, "bids": bids}


def _walk(levels: Sequence[tuple[Decimal, int]], contracts: int, *, buy: bool) -> tuple[int, int, list[dict[str, Any]]]:
    remaining = contracts
    gross_cents = 0
    fills: list[dict[str, Any]] = []
    for price, size in levels:
        quantity = min(remaining, size)
        if quantity <= 0:
            continue
        # Round each displayed level conservatively. A buyer pays up; a seller
        # receives down. This avoids an optimistic rounding gain across levels.
        level_cents = _ceil_cents(price * quantity) if buy else _floor_cents(price * quantity)
        gross_cents += level_cents
        fills.append({"price": str(price), "contracts": quantity, "cash_cents": level_cents})
        remaining -= quantity
        if remaining == 0:
            break
    return gross_cents, remaining, fills


def _fee_value(fee_schedule: Mapping[str, Any] | None, action: str, contracts: int) -> int:
    if fee_schedule is None:
        raise ReplayError("fee_schedule is required")
    if not isinstance(fee_schedule, Mapping):
        raise ReplayError("fee_schedule must be an object")
    version = fee_schedule.get("version")
    if not isinstance(version, str) or not version.strip():
        raise ReplayError("fee_schedule.version is required")
    key = f"{action}_fee_cents_per_contract"
    if key not in fee_schedule:
        raise ReplayError(f"fee_schedule missing {key}")
    per_contract = _decimal(fee_schedule[key], key, minimum=Decimal("0"))
    return int((per_contract * contracts).to_integral_value(rounding=ROUND_CEILING))


def _validate_fee_schedule(fee_schedule: Mapping[str, Any] | None) -> None:
    if fee_schedule is None:
        raise ReplayError("fee_schedule is required")
    if not isinstance(fee_schedule, Mapping):
        raise ReplayError("fee_schedule must be an object")
    version = fee_schedule.get("version")
    if not isinstance(version, str) or not version.strip():
        raise ReplayError("fee_schedule.version is required")
    missing = [
        f"{action}_fee_cents_per_contract"
        for action in ("entry", "exit", "settlement")
        if f"{action}_fee_cents_per_contract" not in fee_schedule
    ]
    if missing:
        raise ReplayError("fee_schedule missing " + ", ".join(missing))


def _select_quote(
    events: Sequence[Mapping[str, Any]],
    expected: Mapping[str, Any],
    *,
    execution_at: datetime,
    max_quote_age: timedelta,
    order_lifetime: timedelta,
    latest_at: datetime,
    purpose: str,
) -> tuple[dict[str, Any] | None, list[str]]:
    candidates: list[tuple[datetime, Mapping[str, Any]]] = []
    reasons: list[str] = []
    for index, event in enumerate(events):
        if not isinstance(event, Mapping):
            reasons.append(f"{purpose}_event_{index}_malformed")
            continue
        kind = event.get("event_type", event.get("kind", "book"))
        if kind not in ("book", "quote"):
            continue
        mismatch = _identity_reasons(expected, event)
        if mismatch:
            reasons.extend(mismatch)
            continue
        try:
            event_at, received_at = _event_times(event, f"{purpose}_event_{index}")
            if received_at < execution_at:
                continue
            if received_at >= latest_at:
                reasons.append(f"{purpose}_quote_at_or_after_cutoff")
                continue
            if received_at > execution_at + order_lifetime:
                continue
            if received_at - event_at > max_quote_age:
                reasons.append(f"{purpose}_quote_stale")
                continue
            normalized = _book(event, f"{purpose}_event_{index}")
        except ReplayError as exc:
            reasons.append(str(exc))
            continue
        candidates.append((received_at, {**dict(event), **normalized, "event_at": event_at, "received_at": received_at}))
    if not candidates:
        if not reasons:
            reasons.append(f"missing_post_latency_{purpose}_quote")
        return None, reasons
    candidates.sort(key=lambda item: item[0])
    return candidates[0][1], reasons


def _settlement(
    raw: Any,
    events: Sequence[Mapping[str, Any]],
    expected: Mapping[str, Any],
    *,
    expiry_at: datetime,
    minimum_received_at: datetime,
) -> tuple[Mapping[str, Any] | None, list[str]]:
    candidates: list[Mapping[str, Any]] = []
    if raw is not None:
        if not isinstance(raw, Mapping):
            return None, ["settlement_malformed"]
        candidates.append(raw)
    for event in events:
        if isinstance(event, Mapping) and event.get("event_type", event.get("kind")) in ("settlement", "finalized_settlement"):
            candidates.append(event)
    if not candidates:
        return None, ["settlement_not_finalized"]
    for item in candidates:
        mismatch = _identity_reasons(expected, item)
        if mismatch:
            continue
        if item.get("status") != "finalized" or item.get("finalized") is not True:
            continue
        if "evidence" not in item or item.get("evidence") in (None, "", {}, []):
            continue
        value = item.get("value", item.get("settlement_value"))
        if isinstance(value, bool) or value not in (0, 1, "0", "1"):
            continue
        try:
            event_at, received_at = _event_times(item, "settlement")
        except ReplayError:
            continue
        if event_at < expiry_at or received_at < minimum_received_at:
            continue
        return {**dict(item), "value": int(value), "event_at": event_at, "received_at": received_at}, []
    return None, ["settlement_not_finalized_or_identity_mismatch"]


def replay(
    data: Mapping[str, Any] | str | bytes,
    *,
    fee_schedule: Mapping[str, Any] | None = None,
    max_quote_age: timedelta = DEFAULT_MAX_QUOTE_AGE,
) -> dict[str, Any]:
    """Replay one binary contract from deterministic offline JSON input.

    ``decision`` must contain ``received_at`` and ``entry_requested``.  Its
    optional ``contracts`` is a whole-contract request (one by default), and
    ``latency_seconds`` controls the first eligible post-decision quote.  A
    scheduled exit uses ``exit_at`` plus the same latency; otherwise the
    position is held for explicit finalized settlement.

    Fee mappings require ``version`` and all three fixed per-contract fields:
    ``entry_fee_cents_per_contract``, ``exit_fee_cents_per_contract`` and
    ``settlement_fee_cents_per_contract``. This is a fixed stress model and
    does not claim to reproduce a venue's price-dependent fee formula.
    """
    payload = _load_input(data)
    _validate_fee_schedule(fee_schedule)
    contract = _required_mapping(payload.get("contract"), "contract")
    expected = _identity(contract)
    events_raw = payload.get("events", payload.get("observations", []))
    if not isinstance(events_raw, Sequence) or isinstance(events_raw, (str, bytes)):
        raise ReplayError("events must be a list")
    events = [event for event in events_raw if isinstance(event, Mapping)]
    decision = _required_mapping(payload.get("decision"), "decision")
    decision_at = _timestamp(decision.get("received_at", decision.get("decision_received_at")), "decision.received_at")
    if decision.get("entry_requested") is not True:
        return _base_result(expected, "rejected", ["entry_not_requested"], decision_at=decision_at)
    if decision.get("side", "buy") not in ("buy", "long"):
        return _base_result(expected, "rejected", ["shorts_not_supported"], decision_at=decision_at)
    latency = _decimal(decision.get("latency_seconds", 0), "decision.latency_seconds", minimum=Decimal("0"))
    latency_delta = timedelta(
        microseconds=int((latency * Decimal("1000000")).to_integral_value(rounding=ROUND_CEILING))
    )
    execution_at = decision_at + latency_delta
    requested = _positive_int(decision.get("contracts", 1), "decision.contracts")
    # A caller may tighten the caps but can never enlarge the firm defaults.
    paper_budget_raw = decision.get("paper_budget_cents", DEFAULT_PAPER_BUDGET_CENTS)
    idea_budget_raw = decision.get("idea_budget_cents", DEFAULT_IDEA_BUDGET_CENTS)
    if isinstance(paper_budget_raw, bool) or not isinstance(paper_budget_raw, int) or paper_budget_raw <= 0:
        raise ReplayError("decision.paper_budget_cents must be a positive integer")
    if isinstance(idea_budget_raw, bool) or not isinstance(idea_budget_raw, int) or idea_budget_raw <= 0:
        raise ReplayError("decision.idea_budget_cents must be a positive integer")
    paper_budget = min(DEFAULT_PAPER_BUDGET_CENTS, paper_budget_raw)
    idea_budget = min(DEFAULT_IDEA_BUDGET_CENTS, idea_budget_raw)
    order_lifetime_seconds = _decimal(decision.get("order_lifetime_seconds", str(DEFAULT_ORDER_LIFETIME.total_seconds())), "decision.order_lifetime_seconds", minimum=Decimal("0"), maximum=Decimal(str(DEFAULT_ORDER_LIFETIME.total_seconds())))
    order_lifetime = timedelta(microseconds=int((order_lifetime_seconds * Decimal("1000000")).to_integral_value(rounding=ROUND_CEILING)))
    opened_at = _timestamp(contract["open_at"], "contract.open_at")
    cutoff_at = _timestamp(contract["cutoff_at"], "contract.cutoff_at")
    expiry_at = _timestamp(contract["expiry_at"], "contract.expiry_at")
    lifecycle_reasons: list[str] = []
    if execution_at < opened_at:
        lifecycle_reasons.append("entry_before_contract_open")
    if execution_at > cutoff_at:
        lifecycle_reasons.append("entry_after_contract_cutoff")
    if lifecycle_reasons:
        return _base_result(expected, "rejected", lifecycle_reasons, decision_at=decision_at, execution_at=execution_at)
    entry_quote, entry_reasons = _select_quote(events, expected, execution_at=execution_at, max_quote_age=max_quote_age, order_lifetime=order_lifetime, latest_at=cutoff_at, purpose="entry")
    if entry_quote is None:
        return _base_result(expected, "rejected", entry_reasons, decision_at=decision_at, execution_at=execution_at)
    gross_entry, remaining, entry_fills = _walk(entry_quote["asks"], requested, buy=True)
    entry_fee = _fee_value(fee_schedule, "entry", requested)
    exit_reserve_fee = _fee_value(fee_schedule, "exit", requested)
    settlement_reserve_fee = _fee_value(fee_schedule, "settlement", requested)
    reserve_extra_fees = max(exit_reserve_fee, settlement_reserve_fee)
    entry_debit = gross_entry + entry_fee
    cash_reserved = entry_debit + reserve_extra_fees
    reasons: list[str] = []
    if remaining:
        reasons.append("insufficient_entry_depth")
    if cash_reserved > idea_budget:
        reasons.append("idea_budget_exceeded")
    if cash_reserved > paper_budget:
        reasons.append("insufficient_cash")
    if reasons:
        return _base_result(expected, "rejected", reasons, decision_at=decision_at, execution_at=execution_at, entry_quote=entry_quote, entry_fills=[], costs={"entry_gross_cents": gross_entry, "entry_fee_cents": entry_fee, "entry_debit_cents": entry_debit, "cash_reserved_cents": cash_reserved, "reserved_exit_or_settlement_fee_cents": reserve_extra_fees, "fee_schedule_version": _fee_version(fee_schedule), "fee_model": "fixed_per_contract_stress"})

    settlement, settlement_reasons = _settlement(payload.get("settlement"), events, expected, expiry_at=expiry_at, minimum_received_at=entry_quote["received_at"])
    scheduled_exit_at = decision.get("exit_at", decision.get("scheduled_exit_at"))
    exit_quote = None
    exit_reasons: list[str] = []
    gross_exit = 0
    exit_fee = 0
    settlement_fee = 0
    exit_fills: list[dict[str, Any]] = []
    realized = False
    resolution: str | None = None
    if scheduled_exit_at is not None:
        scheduled_raw = _timestamp(scheduled_exit_at, "decision.exit_at")
        if scheduled_raw < entry_quote["received_at"]:
            exit_reasons.append("exit_before_entry")
        if scheduled_raw > cutoff_at:
            exit_reasons.append("exit_after_contract_cutoff")
        scheduled = scheduled_raw + latency_delta
        if not exit_reasons:
            exit_quote, quote_reasons = _select_quote(events, expected, execution_at=scheduled, max_quote_age=max_quote_age, order_lifetime=order_lifetime, latest_at=cutoff_at, purpose="exit")
            exit_reasons.extend(quote_reasons)
        if exit_quote is not None:
            gross_exit, remaining_exit, exit_fills = _walk(exit_quote["bids"], requested, buy=False)
            if remaining_exit:
                exit_reasons.append("insufficient_exit_depth")
                exit_fills = []
                gross_exit = 0
            else:
                exit_fee = _fee_value(fee_schedule, "exit", requested)
                realized = True
                resolution = "scheduled_exit"
        else:
            exit_reasons.append("scheduled_exit_unfilled")
    elif settlement is not None:
        outcome = str(expected["outcome"]).upper()
        payout = settlement["value"] if outcome == "YES" else 1 - settlement["value"]
        gross_exit = payout * 100 * requested
        settlement_fee = _fee_value(fee_schedule, "settlement", requested)
        realized = True
        resolution = "finalized_settlement"
    if realized:
        proceeds = gross_exit - exit_fee - settlement_fee
        net_pnl = proceeds - entry_debit
        status = "realized"
    else:
        proceeds = 0
        net_pnl = None
        status = "unresolved"
    return _base_result(
        expected,
        status,
        entry_reasons + exit_reasons + ([] if settlement is not None or scheduled_exit_at is not None else settlement_reasons),
        decision_at=decision_at,
        execution_at=execution_at,
        entry_quote=entry_quote,
        entry_fills=entry_fills,
        exit_quote=exit_quote,
        exit_fills=exit_fills,
        settlement=settlement,
        contracts=requested,
        realized=realized,
        unresolved=not realized,
        resolution=resolution,
        net_pnl_cents=net_pnl,
        costs={
            "entry_gross_cents": gross_entry,
            "entry_fee_cents": entry_fee,
            "exit_gross_cents": gross_exit,
            "exit_fee_cents": exit_fee,
            "settlement_fee_cents": settlement_fee,
            "total_fees_cents": entry_fee + exit_fee + settlement_fee,
            "cash_outflow_cents": entry_debit,
            "cash_proceeds_cents": proceeds,
            "entry_debit_cents": entry_debit,
            "cash_reserved_cents": cash_reserved,
            "reserved_exit_or_settlement_fee_cents": reserve_extra_fees,
            "fee_schedule_version": _fee_version(fee_schedule),
            "fee_model": "fixed_per_contract_stress",
        },
    )


def _fee_version(fee_schedule: Mapping[str, Any] | None) -> str | None:
    if fee_schedule is None:
        return None
    if isinstance(fee_schedule, Mapping):
        return fee_schedule.get("version") if isinstance(fee_schedule.get("version"), str) else None
    return None


def _base_result(expected: Mapping[str, Any], status: str, reasons: Sequence[str], **extra: Any) -> dict[str, Any]:
    result: dict[str, Any] = {
        "scope": "offline_research_only",
        "ready_for_execution": False,
        "readiness_claim": False,
        "contract_identity": dict(expected),
        "status": status,
        "entry_rejection_reasons": list(dict.fromkeys(reasons)),
        "realized": False,
        "unresolved": status == "unresolved",
    }
    result.update(extra)
    return result


replay_json = replay


__all__ = [
    "DEFAULT_IDEA_BUDGET_CENTS",
    "DEFAULT_PAPER_BUDGET_CENTS",
    "DEFAULT_MAX_QUOTE_AGE",
    "ReplayError",
    "replay",
    "replay_json",
]


def main():
    import argparse
    import hashlib
    from pathlib import Path
    from .jupiter_quotes import save_new
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    raw = Path(args.input).read_bytes()
    if len(raw) > 2000000:
        parser.error('input exceeds limit')
    data = json.loads(raw)
    result = replay(data['replay'], fee_schedule=data['fee_schedule'])
    result['input_sha256'] = hashlib.sha256(raw).hexdigest()
    def encode(value):
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, Decimal):
            return str(value)
        raise TypeError('unsupported output type')
    save_new(args.output, json.loads(json.dumps(result, default=encode, allow_nan=False)))
    print(json.dumps({'status':result['status'], 'ready_for_execution':False}))

if __name__ == '__main__': main()
