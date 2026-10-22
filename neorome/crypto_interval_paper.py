"""Offline paper rehearsal for an admitted crypto-interval replay.

This module is deliberately a small boundary around the existing interval
adapter and replay.  It accepts their output only; it does not collect venue
data, infer timestamps, calculate fees, or create a trading runtime.  The
ledger used by :func:`rehearse` is temporary and the resulting report is
always synthetic research evidence, never profitability or execution
evidence.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import tempfile
from typing import Any, Mapping, Sequence

from .paper_ledger import MarketMark, PaperLedger, Proposal


UTC = timezone.utc
PREDICTION_SLEEVE_CENTS = 10_000
IDEA_CAP_CENTS = 500
FIRM_CAP_NOTICE = "prediction sleeve $100; per-idea cap $5"
PROVENANCE = "synthetic"


class IntervalPaperError(ValueError):
    """The supplied adapter/replay evidence cannot enter the rehearsal."""


def _time(value: Any, field: str) -> datetime:
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str):
        try:
            result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise IntervalPaperError(f"{field} must be an ISO timestamp") from exc
    else:
        raise IntervalPaperError(f"{field} must be an ISO timestamp")
    if result.tzinfo is None or result.utcoffset() is None:
        raise IntervalPaperError(f"{field} must include a timezone")
    return result.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _sha(value: Any) -> str:
    def jsonable(item: Any) -> Any:
        if isinstance(item, datetime):
            return _iso(item)
        if isinstance(item, Decimal):
            return str(item)
        if isinstance(item, Mapping):
            return {str(key): jsonable(child) for key, child in item.items()}
        if isinstance(item, (list, tuple)):
            return [jsonable(child) for child in item]
        return item
    try:
        raw = json.dumps(jsonable(value), sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    except (TypeError, ValueError) as exc:
        raise IntervalPaperError("evidence is not canonical JSON") from exc
    return hashlib.sha256(raw).hexdigest()


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise IntervalPaperError(f"{field} must be a positive integer")
    return value


def _fee_schedule(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise IntervalPaperError("a caller-supplied fee schedule is required")
    version = value.get("version")
    if not isinstance(version, str) or not version.strip():
        raise IntervalPaperError("fee_schedule.version is required")
    clean = dict(value)
    for action in ("entry", "exit", "settlement"):
        key = f"{action}_fee_cents_per_contract"
        if key not in value or isinstance(value[key], bool):
            raise IntervalPaperError(f"fee_schedule missing {key}")
        try:
            amount = Decimal(str(value[key]))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise IntervalPaperError(f"fee_schedule.{key} must be nonnegative") from exc
        if not amount.is_finite() or amount < 0:
            raise IntervalPaperError(f"fee_schedule.{key} must be nonnegative")
    return clean


def _event_timestamps(events: Sequence[Any], supplied: Any) -> list[datetime]:
    """Require an explicit timestamp for every adapter event.

    A mapping may use integer indexes or their string forms.  This prevents a
    caller from silently relying on an adapter receipt timestamp.
    """
    if isinstance(supplied, Mapping):
        values = []
        for index, event in enumerate(events):
            if index in supplied:
                values.append(supplied[index])
            elif str(index) in supplied:
                values.append(supplied[str(index)])
            else:
                raise IntervalPaperError(f"explicit event timestamp missing for event {index}")
    elif isinstance(supplied, Sequence) and not isinstance(supplied, (str, bytes)):
        if len(supplied) != len(events):
            raise IntervalPaperError("explicit event timestamps are required for every event")
        values = list(supplied)
    else:
        raise IntervalPaperError("explicit event timestamps are required")
    parsed = [_time(value, f"event_timestamps[{index}]") for index, value in enumerate(values)]
    return parsed


def _validate_adapter(adapter: Mapping[str, Any], fee: Mapping[str, Any], timestamps: Any) -> tuple[dict[str, Any], list[datetime]]:
    if not isinstance(adapter, Mapping):
        raise IntervalPaperError("adapter output must be an object")
    if adapter.get("scope") != "offline_research_only":
        raise IntervalPaperError("only offline adapter output is accepted")
    if adapter.get("execution_enabled") is not False or adapter.get("paper_admitted") is not False:
        raise IntervalPaperError("execution-enabled adapter output is refused")
    reasons = adapter.get("admission_reasons")
    if not isinstance(reasons, list):
        raise IntervalPaperError("adapter admission reasons are required")
    if reasons or adapter.get("admissible_for_replay") is not True:
        joined = ", ".join(str(reason) for reason in reasons) or "adapter did not admit replay"
        raise IntervalPaperError(f"adapter admission failed: {joined}")
    contract = adapter.get("contract")
    events = adapter.get("events")
    if not isinstance(contract, Mapping) or not isinstance(events, Sequence) or isinstance(events, (str, bytes)) or not events:
        raise IntervalPaperError("admitted adapter contract and events are required")
    parsed_times = _event_timestamps(events, timestamps)
    for index, (event, explicit) in enumerate(zip(events, parsed_times)):
        if not isinstance(event, Mapping):
            raise IntervalPaperError(f"adapter event {index} is malformed")
        event_at = event.get("event_at")
        if event_at is None or _time(event_at, f"events[{index}].event_at") != explicit:
            raise IntervalPaperError(f"explicit event timestamp does not match adapter event {index}")
    adapter_fee = adapter.get("fee")
    if not isinstance(adapter_fee, Mapping) or adapter_fee.get("status") != "caller_supplied_fixed_stress":
        raise IntervalPaperError("adapter fee mapping is unresolved")
    if adapter_fee.get("version") != fee.get("version"):
        raise IntervalPaperError("fee schedule version differs from adapter admission")
    for action in ("entry", "exit", "settlement"):
        key = f"{action}_fee_cents_per_contract"
        try:
            if Decimal(str(adapter_fee[key])) != Decimal(str(fee[key])):
                raise IntervalPaperError(f"fee schedule differs for {key}")
        except (KeyError, InvalidOperation, TypeError, ValueError) as exc:
            raise IntervalPaperError(f"adapter fee mapping is incomplete for {key}") from exc
    return dict(contract), parsed_times


def _validate_replay(replay: Mapping[str, Any], contract: Mapping[str, Any], fee: Mapping[str, Any], contract_size: int) -> None:
    if not isinstance(replay, Mapping):
        raise IntervalPaperError("replay output must be an object")
    if replay.get("scope") != "offline_research_only" or replay.get("ready_for_execution") is not False:
        raise IntervalPaperError("only research-only replay output is accepted")
    if replay.get("contract_identity") != dict(contract):
        raise IntervalPaperError("replay contract identity differs from adapter contract")
    if replay.get("contracts") != contract_size:
        raise IntervalPaperError("replay contract count differs from explicit contract size")
    costs = replay.get("costs")
    if not isinstance(costs, Mapping) or costs.get("fee_schedule_version") != fee.get("version"):
        raise IntervalPaperError("replay fee mapping is missing or version-mismatched")
    if replay.get("status") not in {"realized", "unresolved"}:
        raise IntervalPaperError("replay must be realized or unresolved before paper rehearsal")
    if replay.get("status") == "realized" and replay.get("realized") is not True:
        raise IntervalPaperError("realized replay status is inconsistent")
    if replay.get("status") == "unresolved" and replay.get("unresolved") is not True:
        raise IntervalPaperError("unresolved replay status is inconsistent")
    fills = replay.get("entry_fills")
    if not isinstance(fills, list) or sum(int(item.get("contracts", 0)) for item in fills if isinstance(item, Mapping)) != contract_size:
        raise IntervalPaperError("replay does not show a complete entry fill")
    reserved = costs.get("cash_reserved_cents")
    if isinstance(reserved, bool) or not isinstance(reserved, int) or reserved <= 0 or reserved > IDEA_CAP_CENTS:
        raise IntervalPaperError("replay exceeds the $5 prediction idea cap")


def _balance_report(book: PaperLedger) -> dict[str, Any]:
    snapshot = book.snapshot()
    balances = [
        {
            "desk": item.desk,
            "initial_cash_cents": item.initial_cash_cents,
            "available_cash_cents": item.available_cash_cents,
            "reserved_cash_cents": item.reserved_cash_cents,
            "open_position_cost_cents": item.open_position_cost_cents,
        }
        for item in book.balances()
    ]
    prediction = next(item for item in balances if item["desk"] == "prediction")
    if prediction["initial_cash_cents"] != PREDICTION_SLEEVE_CENTS:
        raise IntervalPaperError("prediction sleeve is not $100")
    return {"snapshot": snapshot.__dict__, "balances": balances}


def rehearse(
    adapter_output: Mapping[str, Any],
    replay_output: Mapping[str, Any],
    *,
    event_timestamps: Mapping[Any, Any] | Sequence[Any],
    contract_size: int,
    fee_schedule: Mapping[str, Any],
) -> dict[str, Any]:
    """Exercise one admitted replay in a temporary prediction sleeve.

    The function deliberately accepts replay *output*, rather than raw venue
    data.  A caller must first run the existing adapter and replay and pass
    their records, along with explicit event times and contract units.
    """
    fee = _fee_schedule(fee_schedule)
    size = _positive_int(contract_size, "contract_size")
    contract, event_times = _validate_adapter(adapter_output, fee, event_timestamps)
    _validate_replay(replay_output, contract, fee, size)
    costs = replay_output["costs"]
    decision_at = _time(replay_output.get("decision_at"), "replay.decision_at")
    entry_quote = replay_output.get("entry_quote")
    if not isinstance(entry_quote, Mapping):
        raise IntervalPaperError("replay entry quote is required")
    entry_received = _time(entry_quote.get("received_at"), "replay.entry_quote.received_at")
    if entry_received < decision_at:
        raise IntervalPaperError("entry quote precedes replay decision")
    expiry = _time(contract.get("expiry_at"), "contract.expiry_at")
    if expiry <= entry_received:
        raise IntervalPaperError("contract expires before paper reservation")

    with tempfile.TemporaryDirectory(prefix="neorome-crypto-interval-paper-") as temporary:
        ledger_path = Path(temporary) / "paper.sqlite3"
        book = PaperLedger(ledger_path, enforce_market_risk=True, clock=lambda: entry_received)
        book.record_marks("interval-pre-entry", [], as_of=entry_received)
        market_id = str(contract["market_id"])
        proposal = Proposal(
            proposal_id=f"crypto-interval-paper:{market_id}",
            desk="prediction",
            strategy="crypto-interval-replay",
            strategy_version="crypto-interval-paper-v1",
            source_hash=_sha(adapter_output),
            source_timestamp=decision_at,
            instrument=str(contract["outcome_id"]),
            exposure_group=f"crypto-interval:{market_id}",
            idea_id=f"crypto-interval:{market_id}",
            expiry_at=expiry,
            quantity=1,
            unit_cost_cents=int(costs["entry_gross_cents"]),
            fee_cents=int(costs["entry_fee_cents"]) + int(costs["reserved_exit_or_settlement_fee_cents"]),
            provenance=PROVENANCE,
            metadata={"synthetic": True, "research_only": True, "contract_size": size, "fee_schedule_version": fee["version"]},
        )
        reservation = book.reserve(proposal, now=entry_received)
        restarted = PaperLedger(ledger_path, enforce_market_risk=True, clock=lambda: entry_received)
        replayed_reservation = restarted.reserve(proposal, now=entry_received)
        if replayed_reservation != reservation:
            raise IntervalPaperError("reservation restart was not idempotent")
        fill_at = entry_received
        # Continue on the reopened ledger so the rehearsal proves lifecycle
        # continuity across process restart, not only reservation replay.
        fill = restarted.simulate_fill(
            reservation.reservation_id,
            fill_id=f"fill-{market_id}",
            quantity=1,
            unit_cost_cents=int(costs["entry_gross_cents"]),
            fee_cents=int(costs["entry_fee_cents"]),
            provenance=PROVENANCE,
            filled_at=fill_at,
        )
        restarted_after_fill = PaperLedger(ledger_path, enforce_market_risk=True, clock=lambda: fill_at)
        replayed_fill = restarted_after_fill.simulate_fill(
            reservation.reservation_id,
            fill_id=f"fill-{market_id}",
            quantity=1,
            unit_cost_cents=int(costs["entry_gross_cents"]),
            fee_cents=int(costs["entry_fee_cents"]),
            provenance=PROVENANCE,
            filled_at=fill_at,
        )
        if replayed_fill != fill:
            raise IntervalPaperError("simulated fill restart was not idempotent")
        actions: list[dict[str, Any]] = [{"type": "reserve", "reservation_id": reservation.reservation_id}, {"type": "simulated_fill", "fill_id": fill.fill_id}]
        resolution = replay_output.get("resolution")
        if replay_output["status"] == "realized":
            if resolution not in {"scheduled_exit", "finalized_settlement"}:
                raise IntervalPaperError("realized replay has unsupported resolution")
            evidence = replay_output.get("settlement") if resolution == "finalized_settlement" else replay_output.get("exit_quote")
            if not isinstance(evidence, Mapping):
                raise IntervalPaperError("realized replay evidence is missing")
            close_at = _time(evidence.get("received_at"), "replay close evidence.received_at")
            if close_at < fill_at:
                raise IntervalPaperError("close evidence precedes simulated fill")
            gross_proceeds = int(costs["cash_proceeds_cents"]) + int(costs["exit_fee_cents"]) + int(costs["settlement_fee_cents"])
            close = restarted.settle(
                fill.position_id,
                close_id=f"close-{market_id}",
                proceeds_cents=gross_proceeds,
                fee_cents=int(costs["exit_fee_cents"]) + int(costs["settlement_fee_cents"]),
                provenance=PROVENANCE,
                closed_at=close_at,
            )
            restarted_after_close = PaperLedger(ledger_path, enforce_market_risk=True, clock=lambda: close_at)
            replayed_close = restarted_after_close.settle(
                fill.position_id,
                close_id=f"close-{market_id}",
                proceeds_cents=gross_proceeds,
                fee_cents=int(costs["exit_fee_cents"]) + int(costs["settlement_fee_cents"]),
                provenance=PROVENANCE,
                closed_at=close_at,
            )
            if replayed_close != close:
                raise IntervalPaperError("close restart was not idempotent")
            actions.append({"type": "settlement" if resolution == "finalized_settlement" else "simulated_close", "close_id": close.close_id})
        else:
            mark_at = max(fill_at, event_times[-1])
            mark = MarketMark(fill.position_id, int(costs["entry_debit_cents"]), mark_at, _sha(replay_output), PROVENANCE)
            restarted.record_marks("interval-unresolved-mark", [mark], as_of=mark_at)
            actions.append({"type": "mark", "position_id": fill.position_id})
        final = _balance_report(restarted)
        return {
            "mode": "synthetic_crypto_interval_paper_rehearsal",
            "scope": "offline_research_only",
            "synthetic": True,
            "execution_enabled": False,
            "broker_orders_sent": 0,
            "network_calls": 0,
            "observed_shared_ledger_written": False,
            "profitability_validated": False,
            "research_notice": "Synthetic ledger behavior only; this is not profitability, fill quality, settlement, or venue evidence.",
            "risk_policy": FIRM_CAP_NOTICE,
            "contract_id": market_id,
            "contract_size": size,
            "fee_schedule_version": fee["version"],
            "adapter_admission_reasons": [],
            "replay_status": replay_output["status"],
            "replay_resolution": resolution,
            "actions": actions,
            "final": final,
        }


run = rehearse


__all__ = ["IntervalPaperError", "rehearse", "run"]
