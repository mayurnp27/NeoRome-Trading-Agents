"""Prepare one EQ-INTRADAY paper-entry reservation without mutating the book.

The module sits between a prospective intraday collector and the shared paper
ledger.  It never opens a ledger, fetches a quote, or submits an order.  Its
only positive outcome is a fully funded *unfilled* reservation payload for a
later reviewed adapter.  Exit fill evidence is intentionally unavailable at
entry time and remains a separate lifecycle obligation.
"""

from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
import re
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from desks.paper_candidates import screen_equity
from .intraday_quote_replay import DEFAULT_LATENCY, ReplayValidationError, replay_trade


NY = ZoneInfo("America/New_York")
UTC = timezone.utc
REQUIRED_TRUE = (
    "universe_eligible",
    "prefix_complete_through_signal",
    "bar_quality_clear",
    "size_units_verified",
)
REQUIRED_HASHES = ("universe_evidence_sha256", "bar_prefix_sha256", "quote_evidence_sha256")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def _timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed.astimezone(UTC)


def _validate_frozen_timing(candidate: Mapping[str, Any], strategy_id: str) -> tuple[datetime, datetime, datetime]:
    signal = _timestamp(candidate.get("signal_timestamp"), "signal_timestamp")
    planned_exit = _timestamp(candidate.get("exit_decision_timestamp"), "exit_decision_timestamp")
    session_close = _timestamp(candidate.get("session_close"), "session_close")
    signal_local = signal.astimezone(NY)
    close_local = session_close.astimezone(NY)
    if signal_local.date() != close_local.date():
        raise ValueError("signal_timestamp and session_close must share a New York session date")
    minimum = time(9, 45) if strategy_id == "orb15" else time(10, 0)
    if signal_local.time() < minimum:
        raise ValueError(f"{strategy_id} signal is before its frozen New York window")
    if signal_local.time() >= time(11, 0):
        raise ValueError("signal_timestamp must be before 11:00 New York")
    if planned_exit != session_close - timedelta(minutes=5):
        raise ValueError("exit_decision_timestamp must equal session_close minus five minutes")
    return signal, planned_exit, session_close


def prepare_entry(
    candidate: Mapping[str, Any],
    quotes: Iterable[Mapping[str, Any]],
    *,
    order_lifetime: timedelta,
) -> dict[str, Any]:
    """Return an unfilled shared-paper reservation payload or explicit refusal.

    ``candidate`` needs a stable ``candidate_id``, symbol, frozen strategy id,
    signal, session-close and scheduled-exit timestamps, explicit prospectively
    collected eligibility controls, and immutable input hashes. The
    `prefix_complete_through_signal` assertion applies only through the signal;
    it never claims the later session was known at entry. The first admissible
    ask is used only for full-funded sizing; it is not a broker or paper fill.
    """

    if not isinstance(candidate, Mapping):
        raise ValueError("candidate must be an object")
    reasons = [name + " must be explicitly true" for name in REQUIRED_TRUE if candidate.get(name) is not True]
    candidate_id = candidate.get("candidate_id")
    strategy_id = candidate.get("strategy_id")
    symbol = candidate.get("symbol")
    if not isinstance(candidate_id, str) or not candidate_id.strip():
        reasons.append("candidate_id is required")
    if strategy_id not in {"orb15", "vwap_reclaim"}:
        reasons.append("strategy_id must be a frozen EQ-INTRADAY candidate")
    if not isinstance(symbol, str) or not symbol.strip():
        reasons.append("symbol is required")
    if not isinstance(candidate.get("fractional_supported"), bool):
        reasons.append("fractional_supported must be an explicit boolean")
    if "fee_cents_per_unit" not in candidate:
        reasons.append("fee_cents_per_unit is required")
    for field in REQUIRED_HASHES:
        value = candidate.get(field)
        if not isinstance(value, str) or not _SHA256.fullmatch(value):
            reasons.append(f"{field} must be a lowercase SHA-256")
    try:
        signal_at, planned_exit_at, session_close_at = _validate_frozen_timing(candidate, strategy_id)
        for field in ('universe_known_at', 'bar_prefix_known_at', 'calendar_known_at'):
            if _timestamp(candidate.get(field), field) > signal_at:
                reasons.append(f'{field} is after the signal')
    except ValueError as exc:
        reasons.append(str(exc))
    if reasons:
        return {"accepted": False, "status": "rejected", "reasons": reasons, "scope": "unfilled_paper_preparation"}
    try:
        replay = replay_trade(
            candidate,
            quotes,
            admission=True,
            order_lifetime=order_lifetime,
            size_units_verified=True,
        )
    except ReplayValidationError as exc:
        return {"accepted": False, "status": "rejected", "reasons": [str(exc)], "scope": "unfilled_paper_preparation"}
    if replay.entry is None:
        return {
            "accepted": False,
            "status": "rejected",
            "reasons": ["entry quote replay is unresolved", *replay.reasons],
            "replay_rejections": replay.rejection_counts,
            "scope": "unfilled_paper_preparation",
        }
    # Admission mode requires a receipt.  Keep this defensive refusal local so
    # a future replay implementation cannot silently weaken the contract.
    if replay.entry.received_at is None:
        return {"accepted": False, "status": "rejected", "reasons": ["entry quote receipt is missing"], "scope": "unfilled_paper_preparation"}
    if replay.entry.received_at.astimezone(NY).time() >= time(11, 0):
        return {"accepted": False, "status": "rejected", "reasons": ["modeled entry must be before 11:00 New York"], "scope": "unfilled_paper_preparation"}
    sizing = screen_equity(
        {
            "ask": replay.entry.price,
            "quote_timestamp": replay.entry.timestamp.isoformat(),
            "signal_known_at": replay.signal_timestamp.isoformat(),
            "side": "buy",
            "fractional_supported": candidate["fractional_supported"],
            "fee_cents_per_unit": candidate["fee_cents_per_unit"],
            **({"units": candidate["units"]} if "units" in candidate else {}),
        },
        as_of=replay.entry.received_at.isoformat(),
        max_quote_age=timedelta(seconds=1),
    )
    if not sizing["accepted"]:
        return {"accepted": False, "status": "rejected", "reasons": sizing["reasons"], "sizing": sizing, "scope": "unfilled_paper_preparation"}
    if float(sizing["units"]) > replay.entry.size:
        return {
            "accepted": False,
            "status": "rejected",
            "reasons": ["entry displayed depth does not cover sized quantity"],
            "sizing": sizing,
            "scope": "unfilled_paper_preparation",
        }
    order_expires_at = min(
        signal_at + DEFAULT_LATENCY + order_lifetime,
        datetime.combine(signal_at.astimezone(NY).date(), time(11, 0), tzinfo=NY).astimezone(UTC),
    )
    if replay.entry.received_at > order_expires_at:
        return {"accepted": False, "status": "rejected", "reasons": ["quote received after order expiry"], "scope": "unfilled_paper_preparation"}
    return {
        "accepted": True,
        "status": "prepared_unfilled_reservation",
        "scope": "unfilled_paper_preparation",
        "eligible_for_execution": False,
        "candidate_id": candidate_id,
        "strategy_id": strategy_id,
        "instrument": symbol.strip(),
        "exposure_group": "us-broad-equity",
        "units": sizing["units"],
        "funded_cost_cents": sizing["cost_cents"],
        "order_expires_at": order_expires_at.isoformat(),
        "planned_exit_at": planned_exit_at.isoformat(),
        "session_close": session_close_at.isoformat(),
        "entry_quote": {
            "timestamp": replay.entry.timestamp.isoformat(),
            "received_at": replay.entry.received_at.isoformat(),
            "ask": replay.entry.price,
            "displayed_size": replay.entry.size,
            "quote_index": replay.entry.quote_index,
        },
        "replay_rejections": replay.rejection_counts,
        "provenance": {field: candidate[field] for field in REQUIRED_HASHES},
        "prefix_complete_through_signal": True,
        "exit_status": "not_yet_observable_requires_separate_quote_replay",
        "sizing": sizing,
    }


__all__ = ["prepare_entry"]
