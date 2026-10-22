"""Rehearsal-only adapter for the EQ-INTRADAY candidate contract.

This module is intentionally a small desk-owned boundary around
``neorome.paper_ledger``.  It consumes already captured candidate/quote
evidence and never fetches data, opens a broker client, or submits an order.
The ledger remains the durable source of lifecycle, cash and risk truth.

The adapter uses deterministic candidate ids as proposal ids, so replaying a
candidate after a process restart returns the original reservation.  Pending
entry expiry is independent of the planned exit: ``expire``/``cancel`` must
be called by the rehearsal driver and releases the ledger reservation without
ever creating a fill.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from typing import Any, Iterable, Mapping

from neorome.paper_ledger import (
    CloseResult,
    Fill,
    InvalidState,
    MarketMark,
    PaperLedger,
    Proposal,
    Reservation,
    RiskStopActive,
)

from .intraday_paper_candidate import prepare_entry


UTC = timezone.utc
DESK = "equities"
EXPOSURE_GROUP = "us-broad-equity"
STRATEGY = "EQ-INTRADAY"
STRATEGY_VERSION = "EQ-INTRADAY-v1"
MAX_IDEA_CENTS = 1_500
MAX_GROUP_CENTS = 3_000
MAX_AGGREGATE_CENTS = 8_000
DEFAULT_ORDER_LIFETIME = timedelta(seconds=60)


class IntradayAdapterError(ValueError):
    """Candidate or quote evidence cannot be admitted to the rehearsal."""


class EntryGateBlocked(IntradayAdapterError):
    """A local STOP/recovery gate blocks a new reservation or entry."""


def _utc(value: datetime | str, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise IntradayAdapterError(f"{field} must be an ISO timestamp") from exc
    else:
        raise IntradayAdapterError(f"{field} must be an ISO timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise IntradayAdapterError(f"{field} must include a timezone")
    return parsed.astimezone(UTC)


def _decimal(value: Any, field: str, *, nonnegative: bool = False) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise IntradayAdapterError(f"{field} must be finite numeric data")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise IntradayAdapterError(f"{field} must be finite numeric data") from exc
    if not parsed.is_finite() or (nonnegative and parsed < 0):
        raise IntradayAdapterError(f"{field} must be finite numeric data")
    return parsed


def _candidate_id(prepared: Mapping[str, Any]) -> str:
    value = prepared.get("candidate_id")
    if not isinstance(value, str) or not value.strip():
        raise IntradayAdapterError("prepared candidate_id is required")
    return value.strip()


def _integer_units(value: Any) -> int:
    parsed = _decimal(value, "units", nonnegative=False)
    if parsed <= 0 or parsed != parsed.to_integral_value():
        raise IntradayAdapterError(
            "shared paper ledger supports whole-unit intraday quantities only"
        )
    return int(parsed)


class IntradayPaperAdapter:
    """Desk-local, rehearsal-only lifecycle adapter.

    ``ledger`` must be a persistent :class:`PaperLedger` supplied by the
    caller.  No default path is created, which prevents an accidental
    production location from becoming the adapter's state store.
    """

    def __init__(
        self,
        ledger: PaperLedger,
        *,
        max_pending_lifetime: timedelta = DEFAULT_ORDER_LIFETIME,
        provenance: str = "synthetic",
    ) -> None:
        if not isinstance(ledger, PaperLedger):
            raise IntradayAdapterError("ledger must be a PaperLedger")
        if not isinstance(max_pending_lifetime, timedelta) or max_pending_lifetime <= timedelta(0):
            raise IntradayAdapterError("max_pending_lifetime must be positive")
        limits = ledger.limits
        if (
            limits.aggregate_cap_cents > MAX_AGGREGATE_CENTS
            or limits.group_cap_cents > MAX_GROUP_CENTS
            or limits.per_idea_cents[DESK] > MAX_IDEA_CENTS
        ):
            raise IntradayAdapterError("shared ledger limits exceed EQ-INTRADAY rehearsal caps")
        self.ledger = ledger
        self.max_pending_lifetime = max_pending_lifetime
        if provenance not in {"synthetic", "observed"}:
            raise IntradayAdapterError("provenance must be synthetic or observed")
        self.provenance = provenance

    def _gate(self, *, recovery_ok: bool, allow_entry: bool) -> None:
        if not isinstance(recovery_ok, bool) or not recovery_ok:
            raise EntryGateBlocked("recovery gate blocks intraday entry")
        if not isinstance(allow_entry, bool) or not allow_entry:
            raise EntryGateBlocked("entry gate blocks intraday entry")
        if self.ledger.risk_stop_active():
            raise RiskStopActive("risk STOP blocks intraday entry")

    def reserve(
        self,
        candidate: Mapping[str, Any],
        quotes: Iterable[Mapping[str, Any]],
        *,
        order_lifetime: timedelta = DEFAULT_ORDER_LIFETIME,
        now: datetime | str | None = None,
        recovery_ok: bool = True,
        allow_entry: bool = True,
    ) -> Reservation:
        """Prepare and durably reserve one candidate, without creating a fill."""

        self._gate(recovery_ok=recovery_ok, allow_entry=allow_entry)
        if not isinstance(order_lifetime, timedelta) or order_lifetime <= timedelta(0):
            raise IntradayAdapterError("order_lifetime must be positive")
        if order_lifetime > self.max_pending_lifetime:
            raise IntradayAdapterError("order_lifetime exceeds bounded pending lifetime")
        prepared = prepare_entry(candidate, quotes, order_lifetime=order_lifetime)
        if not prepared.get("accepted"):
            reasons = prepared.get("reasons") or ["candidate was not admitted"]
            raise IntradayAdapterError("; ".join(str(reason) for reason in reasons))
        candidate_id = _candidate_id(prepared)
        units = _integer_units(prepared.get("units"))
        entry_quote = prepared.get("entry_quote")
        if not isinstance(entry_quote, Mapping):
            raise IntradayAdapterError("prepared entry quote is required")
        ask = _decimal(entry_quote.get("ask"), "entry ask")
        # Reserve a conservative integer-cent gross cost, plus the declared
        # per-unit fee.  The shared ledger intentionally stores integer units
        # and integer cents, so a fractional-unit candidate is rejected above.
        gross_unit_cents = int((ask * 100).to_integral_value(rounding=ROUND_CEILING))
        fee_each = _decimal(candidate.get("fee_cents_per_unit", 0), "fee_cents_per_unit", nonnegative=True)
        if fee_each != fee_each.to_integral_value():
            raise IntradayAdapterError("fee_cents_per_unit must be whole cents for shared ledger")
        fee_cents = int(fee_each) * units
        total = gross_unit_cents * units + fee_cents
        if total > MAX_IDEA_CENTS:
            raise IntradayAdapterError("funded entry exceeds the $15 per-idea rehearsal cap")
        expiry = _utc(prepared.get("order_expires_at"), "order_expires_at")
        source_timestamp = _utc(entry_quote.get("timestamp"), "entry quote timestamp")
        reservation_now = _utc(now, "now") if now is not None else _utc(entry_quote.get("received_at"), "entry quote received_at")
        if expiry - reservation_now > self.max_pending_lifetime:
            raise IntradayAdapterError("entry expiry exceeds bounded pending lifetime")
        if expiry <= reservation_now:
            raise IntradayAdapterError("entry reservation is already expired")
        if source_timestamp > reservation_now:
            raise IntradayAdapterError("entry quote timestamp is after reservation time")
        source_hash = str(prepared["provenance"]["quote_evidence_sha256"])
        proposal = Proposal(
            proposal_id=f"eq-intraday:{candidate_id}",
            desk=DESK,
            strategy=STRATEGY,
            strategy_version=STRATEGY_VERSION,
            source_hash=source_hash,
            source_timestamp=source_timestamp,
            instrument=str(prepared["instrument"]),
            exposure_group=EXPOSURE_GROUP,
            idea_id=f"eq-intraday:{candidate_id}",
            expiry_at=expiry,
            quantity=units,
            unit_cost_cents=gross_unit_cents,
            fee_cents=fee_cents,
            provenance=self.provenance,
            metadata={
                "adapter": "desks.equities.intraday_paper_adapter",
                "candidate_id": candidate_id,
                "planned_exit_at": prepared["planned_exit_at"],
                "quote": dict(entry_quote),
                "candidate_provenance": dict(prepared["provenance"]),
                "execution_mode": "internal_paper_only",
            },
        )
        return self.ledger.reserve(proposal, now=reservation_now)

    def cancel(
        self,
        reservation_id: str,
        *,
        reason: str = "operator_cancelled",
        cancelled_at: datetime | str | None = None,
    ) -> Reservation:
        """Idempotently cancel a pending entry and release its reservation."""

        return self.ledger.cancel(
            reservation_id,
            cancel_id=f"eq-intraday-cancel:{reservation_id}",
            reason=reason,
            cancelled_at=cancelled_at,
        )

    def expire(
        self,
        reservation_id: str,
        *,
        now: datetime | str,
    ) -> Reservation:
        """Cancel only after the independent entry expiry has elapsed."""

        current = _utc(now, "now")
        reservation = self.ledger.get_reservation(reservation_id)
        expiry = _utc(reservation.expiry_at, "reservation expiry")
        if current < expiry:
            raise InvalidState("reservation has not expired")
        return self.cancel(reservation_id, reason="entry_expired", cancelled_at=current)

    def entry(
        self,
        reservation_id: str,
        *,
        quote_timestamp: datetime | str,
        filled_at: datetime | str,
        unit_cost_cents: int | None = None,
        fee_cents: int | None = None,
        fill_id: str | None = None,
        recovery_ok: bool = True,
        allow_entry: bool = True,
    ) -> Fill:
        """Record one simulated entry from already captured post-reservation evidence."""

        self._gate(recovery_ok=recovery_ok, allow_entry=allow_entry)
        reservation = self.ledger.get_reservation(reservation_id)
        quote_at = _utc(quote_timestamp, "quote_timestamp")
        filled = _utc(filled_at, "filled_at")
        if quote_at <= _utc(reservation.created_at, "reservation created_at"):
            raise IntradayAdapterError("entry quote must be captured after reservation")
        if quote_at > filled:
            raise IntradayAdapterError("entry quote cannot follow fill time")
        return self.ledger.simulate_fill(
            reservation_id,
            fill_id=fill_id or f"eq-intraday-fill:{reservation_id}",
            unit_cost_cents=unit_cost_cents,
            fee_cents=fee_cents,
            provenance=self.provenance,
            filled_at=filled,
        )

    # ``fill`` is a readable alias for callers using the ledger vocabulary.
    fill = entry

    def entry_from_quote(
        self,
        reservation_id: str,
        quote: Mapping[str, Any],
        *,
        filled_at: datetime | str,
        recovery_ok: bool = True,
        allow_entry: bool = True,
    ) -> Fill:
        """Fill at a separately observed post-reservation ask.

        The caller supplies a retained quote record rather than a broker
        acknowledgement.  This is still an internal simulated fill, but it
        prevents a reservation quote or a last trade from being relabelled as
        the entry price.
        """

        if not isinstance(quote, Mapping):
            raise IntradayAdapterError("entry fill quote must be an object")
        reservation = self.ledger.get_reservation(reservation_id)
        if str(quote.get("symbol", "")).strip() != reservation.instrument:
            raise IntradayAdapterError("entry fill quote symbol does not match reservation")
        quote_at = _utc(quote.get("timestamp"), "entry fill quote timestamp")
        received_at = _utc(quote.get("received_at"), "entry fill quote received_at")
        filled = _utc(filled_at, "filled_at")
        if received_at < quote_at or (received_at - quote_at) > timedelta(seconds=1):
            raise IntradayAdapterError("entry fill quote receipt is stale or precedes event")
        if quote_at <= _utc(reservation.created_at, "reservation created_at"):
            raise IntradayAdapterError("entry fill quote must be captured after reservation")
        if received_at > filled or quote_at > filled:
            raise IntradayAdapterError("entry fill quote cannot follow fill time")
        ask = _decimal(quote.get("ask"), "entry fill ask", nonnegative=True)
        depth = _decimal(quote.get("ask_size", quote.get("displayed_size")), "entry fill ask_size", nonnegative=True)
        bid = _decimal(quote.get("bid"), "entry fill bid", nonnegative=True)
        if ask <= 0 or bid <= 0 or bid > ask or depth < reservation.quantity:
            raise IntradayAdapterError("entry fill quote is crossed, nonpositive, or lacks displayed depth")
        return self.entry(
            reservation_id,
            quote_timestamp=quote_at,
            filled_at=filled,
            unit_cost_cents=int((ask * 100).to_integral_value(rounding=ROUND_CEILING)),
            recovery_ok=recovery_ok,
            allow_entry=allow_entry,
        )

    def mark(
        self,
        batch_id: str,
        marks: Iterable[MarketMark],
        *,
        as_of: datetime | str,
    ):
        """Persist a complete ledger mark batch supplied by the rehearsal."""

        normalized = tuple(marks)
        if not normalized:
            raise IntradayAdapterError("at least one mark is required for intraday mark")
        return self.ledger.record_marks(batch_id, normalized, as_of)

    def exit(
        self,
        position_id: str,
        *,
        proceeds_cents: int,
        fee_cents: int = 0,
        close_id: str | None = None,
        closed_at: datetime | str | None = None,
    ) -> CloseResult:
        """Idempotently close a simulated position; STOP never blocks exits."""

        return self.ledger.close(
            position_id,
            close_id=close_id or f"eq-intraday-exit:{position_id}",
            proceeds_cents=proceeds_cents,
            fee_cents=fee_cents,
            provenance=self.provenance,
            closed_at=closed_at,
        )

    def exit_from_quote(
        self,
        position_id: str,
        quote: Mapping[str, Any],
        *,
        exit_decision_at: datetime | str,
        closed_at: datetime | str,
        fee_cents_per_unit: int = 0,
    ) -> CloseResult:
        """Close at a separately observed executable bid with conservative cents."""

        if not isinstance(quote, Mapping):
            raise IntradayAdapterError("exit quote must be an object")
        position = self.ledger.get_position(position_id)
        if str(quote.get("symbol", "")).strip() != position.instrument:
            raise IntradayAdapterError("exit quote symbol does not match position")
        quote_at = _utc(quote.get("timestamp"), "exit quote timestamp")
        received_at = _utc(quote.get("received_at"), "exit quote received_at")
        decision_at = _utc(exit_decision_at, "exit_decision_at")
        closed = _utc(closed_at, "closed_at")
        if quote_at < decision_at or received_at < quote_at or (received_at - quote_at) > timedelta(seconds=1):
            raise IntradayAdapterError("exit quote is before decision or stale")
        if received_at > closed or quote_at > closed:
            raise IntradayAdapterError("exit quote cannot follow close time")
        bid = _decimal(quote.get("bid"), "exit bid", nonnegative=True)
        ask = _decimal(quote.get("ask"), "exit ask", nonnegative=True)
        depth = _decimal(quote.get("bid_size", quote.get("displayed_size")), "exit bid_size", nonnegative=True)
        if bid <= 0 or ask <= 0 or bid > ask or depth < position.quantity:
            raise IntradayAdapterError("exit quote is crossed, nonpositive, or lacks displayed depth")
        if isinstance(fee_cents_per_unit, bool) or not isinstance(fee_cents_per_unit, int) or fee_cents_per_unit < 0:
            raise IntradayAdapterError("fee_cents_per_unit must be a non-negative integer")
        proceeds = int((bid * 100 * position.quantity).to_integral_value(rounding=ROUND_FLOOR))
        return self.exit(
            position_id,
            proceeds_cents=proceeds,
            fee_cents=fee_cents_per_unit * position.quantity,
            closed_at=closed,
        )


__all__ = ["EntryGateBlocked", "IntradayAdapterError", "IntradayPaperAdapter"]
