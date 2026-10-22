"""Shared internal paper-proposal admission boundary.

The gateway accepts JSON-shaped desk proposals and routes accepted reservations
through :class:`neorome.paper_ledger.PaperLedger`.  It has no market-data,
broker, credential, or network dependencies.  A proposal only reserves cash;
an independent observed modeled-fill event is required before the gateway will
call the ledger's all-or-none simulated-fill operation.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping

from .paper_ledger import (
    Fill,
    IdempotencyConflict,
    InvalidState,
    PaperLedger,
    Proposal,
    Reservation,
    ValidationError,
)

UTC = timezone.utc
_SOURCE_KINDS = {"synthetic", "observed"}
_DESKS = {"prediction", "equities", "options", "crypto"}
_INSTRUMENT_TYPES = {"cash_asset", "prediction", "debit_spread"}
_GATEWAY_METADATA_KEY = "paper_gateway_v1"


class GatewayValidationError(ValidationError):
    """The JSON gateway contract is malformed or fails closed."""


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise GatewayValidationError(f"{field} must be a non-empty string")
    return value.strip()


def _int(value: Any, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise GatewayValidationError(f"{field} must be an integer >= {minimum}")
    return value


def _timestamp(value: Any, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise GatewayValidationError(f"{field} must be an ISO-8601 timestamp") from exc
    else:
        raise GatewayValidationError(f"{field} must be an aware timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise GatewayValidationError(f"{field} must include a timezone")
    return parsed.astimezone(UTC)


def _hash(value: Any, field: str) -> str:
    result = _text(value, field).lower()
    if len(result) != 64 or any(char not in "0123456789abcdef" for char in result):
        raise GatewayValidationError(f"{field} must be a 64-character hexadecimal digest")
    return result


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise GatewayValidationError(f"{field} must be an object")
    return value


def _canonical(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise GatewayValidationError("gateway payload must contain JSON values only") from exc


def _fresh(value: Any, field: str, *, now: datetime, max_age: int) -> datetime:
    parsed = _timestamp(value, field)
    if parsed > now:
        raise GatewayValidationError(f"{field} is in the future")
    if (now - parsed).total_seconds() > max_age:
        raise GatewayValidationError(f"{field} is stale")
    return parsed


def _as_result(value: Reservation | Fill) -> dict[str, Any]:
    return asdict(value)


class PaperProposalGateway:
    """Admit common four-desk proposals into one existing paper ledger.

    ``source_kind`` is fixed per gateway instance.  This prevents a caller
    configured for observed paper from accidentally mixing synthetic demo
    payloads into the same state.  The default is ``observed``.
    """

    def __init__(
        self,
        ledger: PaperLedger,
        *,
        source_kind: str = "observed",
        max_source_age_seconds: int | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not isinstance(ledger, PaperLedger):
            raise GatewayValidationError("ledger must be a PaperLedger")
        self.ledger = ledger
        self.source_kind = _text(source_kind, "source_kind")
        if self.source_kind not in _SOURCE_KINDS:
            raise GatewayValidationError("source_kind must be synthetic or observed")
        if max_source_age_seconds is None:
            max_source_age_seconds = ledger.max_source_age_seconds
        self.max_source_age_seconds = _int(max_source_age_seconds, "max_source_age_seconds", minimum=1)
        self.max_source_age_seconds = min(self.max_source_age_seconds, ledger.max_source_age_seconds, 60)
        self._clock = clock or (lambda: datetime.now(UTC))

    def _now(self, value: datetime | str | None) -> datetime:
        return _timestamp(value if value is not None else self._clock(), "now")

    def _assert_source_isolation(self) -> None:
        """Refuse to add this mode to a ledger containing the other mode."""
        for event in self.ledger.audit():
            if event.event_type != "reserve":
                continue
            payload = event.payload.get("payload", {})
            provenance = payload.get("provenance") if isinstance(payload, Mapping) else None
            if provenance != self.source_kind:
                raise GatewayValidationError("ledger contains mixed synthetic and observed paper state")

    def _source(
        self,
        proposal: Mapping[str, Any],
        *,
        now: datetime,
        validate_fresh: bool = True,
    ) -> tuple[dict[str, Any], datetime, datetime]:
        source = dict(_mapping(proposal.get("source"), "source"))
        kind = _text(proposal.get("source_kind"), "source_kind")
        source_kind = _text(source.get("kind"), "source.kind")
        if kind != source_kind or kind != self.source_kind:
            raise GatewayValidationError("source provenance does not match this gateway")
        source_hash = _hash(source.get("hash"), "source.hash")
        observed = (_fresh(source.get("observed_at"), "source.observed_at", now=now, max_age=self.max_source_age_seconds)
                    if validate_fresh else _timestamp(source.get("observed_at"), "source.observed_at"))
        received = (_fresh(source.get("received_at"), "source.received_at", now=now, max_age=self.max_source_age_seconds)
                    if validate_fresh else _timestamp(source.get("received_at"), "source.received_at"))
        if observed > received:
            raise GatewayValidationError("source.observed_at cannot follow source.received_at")
        valid_until = source.get("valid_until")
        if valid_until is not None and validate_fresh and _timestamp(valid_until, "source.valid_until") <= now:
            raise GatewayValidationError("source.valid_until has expired")
        return {"kind": kind, "hash": source_hash, "observed_at": observed.isoformat().replace("+00:00", "Z"),
                "received_at": received.isoformat().replace("+00:00", "Z"),
                **({"valid_until": _timestamp(valid_until, "source.valid_until").isoformat().replace("+00:00", "Z")} if valid_until is not None else {})}, observed, received

    @staticmethod
    def _quote(value: Any, *, field: str = "quote") -> dict[str, Any]:
        quote = _mapping(value, field)
        timestamp = _timestamp(quote.get("timestamp"), f"{field}.timestamp")
        bid = _int(quote.get("bid_cents"), f"{field}.bid_cents")
        ask = _int(quote.get("ask_cents"), f"{field}.ask_cents", minimum=1)
        depth = _int(quote.get("depth"), f"{field}.depth", minimum=1)
        if bid > ask:
            raise GatewayValidationError(f"{field} is crossed")
        return {"timestamp": timestamp.isoformat().replace("+00:00", "Z"), "bid_cents": bid,
                "ask_cents": ask, "depth": depth}

    @staticmethod
    def _kind_checks(proposal: Mapping[str, Any], desk: str) -> tuple[str, str, list[dict[str, Any]]]:
        instrument_type = _text(proposal.get("instrument_type"), "instrument_type")
        side = _text(proposal.get("side"), "side").lower()
        if instrument_type not in _INSTRUMENT_TYPES:
            raise GatewayValidationError("unsupported instrument_type")
        if side != "long":
            raise GatewayValidationError("only long positions are admitted")
        expected = {
            "equities": "cash_asset", "crypto": "cash_asset",
            "prediction": "prediction", "options": "debit_spread",
        }[desk]
        if instrument_type != expected:
            raise GatewayValidationError(f"{desk} requires {expected}")
        legs: list[dict[str, Any]] = []
        if instrument_type == "debit_spread":
            raw_legs = proposal.get("legs")
            if not isinstance(raw_legs, list) or len(raw_legs) != 2:
                raise GatewayValidationError("debit_spread requires exactly two legs")
            for index, raw in enumerate(raw_legs):
                leg = dict(_mapping(raw, f"legs[{index}]"))
                symbol = _text(leg.get("symbol"), f"legs[{index}].symbol")
                leg_side = _text(leg.get("side"), f"legs[{index}].side").lower()
                if leg_side not in {"buy", "sell"} or _int(leg.get("ratio"), f"legs[{index}].ratio", minimum=1) != 1:
                    raise GatewayValidationError("debit spread legs must be one buy and one sell at ratio 1")
                legs.append({"symbol": symbol, "side": leg_side, "ratio": 1})
            if {leg["side"] for leg in legs} != {"buy", "sell"} or len({leg["symbol"] for leg in legs}) != 2:
                raise GatewayValidationError("debit spread requires distinct buy and sell legs")
        return instrument_type, side, legs

    def _normalize_proposal(
        self, raw: Mapping[str, Any], *, now: datetime, replay: bool = False
    ) -> tuple[Proposal, dict[str, Any]]:
        proposal = _mapping(raw, "proposal")
        desk = _text(proposal.get("desk"), "desk")
        if desk not in _DESKS:
            raise GatewayValidationError("desk must be one of prediction, equities, options, crypto")
        source, observed, received = self._source(proposal, now=now, validate_fresh=not replay)
        quote = self._quote(proposal.get("quote"))
        if _timestamp(quote["timestamp"], "quote.timestamp") != observed:
            raise GatewayValidationError("quote.timestamp must equal source.observed_at")
        instrument_type, side, legs = self._kind_checks(proposal, desk)
        instrument = _text(proposal.get("instrument"), "instrument")
        exposure_group = _text(proposal.get("exposure_group"), "exposure_group")
        idea_id = _text(proposal.get("idea_id"), "idea_id")
        strategy = _text(proposal.get("strategy"), "strategy")
        strategy_version = _text(proposal.get("strategy_version"), "strategy_version")
        proposal_id = _text(proposal.get("proposal_id"), "proposal_id")
        quantity = _int(proposal.get("quantity"), "quantity", minimum=1)
        if instrument_type != "debit_spread" and quantity != 1:
            raise GatewayValidationError("cash assets and predictions require one whole proposal lot")
        cost = dict(_mapping(proposal.get("cost"), "cost"))
        unit_cost = _int(cost.get("unit_cost_cents"), "cost.unit_cost_cents", minimum=1)
        fee = _int(cost.get("fee_cents", 0), "cost.fee_cents")
        if unit_cost != quote["ask_cents"]:
            raise GatewayValidationError("cost.unit_cost_cents must equal the observed ask")
        if quote["depth"] < quantity:
            raise GatewayValidationError("observed quote depth is insufficient for all-or-none quantity")
        expiry = _timestamp(proposal.get("expiry_at"), "expiry_at")
        if not replay and expiry <= now:
            raise GatewayValidationError("expiry_at must be in the future")
        execution = dict(_mapping(proposal.get("execution"), "execution"))
        delay = _int(execution.get("delay_seconds"), "execution.delay_seconds")
        if not replay and (_timestamp(quote["timestamp"], "quote.timestamp") + timedelta(seconds=delay) >= expiry
                           or now + timedelta(seconds=delay) >= expiry):
            raise GatewayValidationError("execution delay cannot reach or pass expiry")
        metadata = proposal.get("metadata", {})
        if not isinstance(metadata, Mapping):
            raise GatewayValidationError("metadata must be an object")
        contract = {
            "source_kind": self.source_kind, "source": source, "quote": quote,
            "instrument_type": instrument_type, "side": side, "legs": legs,
            "execution": {"delay_seconds": delay}, "quantity": quantity,
            "instrument": instrument, "proposal_id": proposal_id,
        }
        ledger_metadata = dict(metadata)
        ledger_metadata[_GATEWAY_METADATA_KEY] = contract
        normalized = Proposal(
            proposal_id, desk, strategy, strategy_version, source["hash"], observed,
            instrument, exposure_group, idea_id, expiry, quantity, unit_cost, fee,
            self.source_kind, ledger_metadata,
        )
        return normalized, contract

    @staticmethod
    def _ledger_proposal_payload(proposal: Proposal) -> dict[str, Any]:
        """Match the ledger's durable proposal representation for replay checks."""
        return {
            "proposal_id": proposal.proposal_id,
            "desk": proposal.desk,
            "strategy": proposal.strategy,
            "strategy_version": proposal.strategy_version,
            "source_hash": proposal.source_hash.lower(),
            "source_timestamp": _timestamp(proposal.source_timestamp, "source_timestamp").isoformat().replace("+00:00", "Z"),
            "instrument": proposal.instrument,
            "exposure_group": proposal.exposure_group,
            "idea_id": proposal.idea_id,
            "expiry_at": _timestamp(proposal.expiry_at, "expiry_at").isoformat().replace("+00:00", "Z"),
            "quantity": proposal.quantity,
            "unit_cost_cents": proposal.unit_cost_cents,
            "fee_cents": proposal.fee_cents,
            "provenance": proposal.provenance,
            "metadata": json.loads(_canonical(dict(proposal.metadata))),
        }

    def _existing_reservation(self, proposal_id: str) -> tuple[Reservation, Mapping[str, Any]] | None:
        for event in self.ledger.audit():
            if event.event_type != "reserve":
                continue
            payload = event.payload.get("payload", {})
            if not isinstance(payload, Mapping) or payload.get("proposal_id") != proposal_id:
                continue
            reservation = self.ledger.get_reservation(event.entity_id)
            return reservation, payload
        return None

    def reserve(self, proposal: Mapping[str, Any], *, now: datetime | str | None = None) -> dict[str, Any]:
        """Validate and atomically reserve a proposal's full funded cost.

        This method never creates a fill.  Replaying the same JSON proposal is
        delegated to the ledger's idempotent reservation mechanism.
        """
        current = self._now(now)
        self._assert_source_isolation()
        raw = _mapping(proposal, "proposal")
        proposal_id = _text(raw.get("proposal_id"), "proposal_id")
        existing = self._existing_reservation(proposal_id)
        normalized, contract = self._normalize_proposal(raw, now=current, replay=existing is not None)
        if existing is not None:
            reservation, payload = existing
            stored = payload if isinstance(payload, Mapping) else None
            if not isinstance(stored, Mapping) or dict(stored) != self._ledger_proposal_payload(normalized):
                raise IdempotencyConflict(f"proposal_id {proposal_id!r} has different data")
            metadata = stored.get("metadata", {})
            stored_contract = metadata.get(_GATEWAY_METADATA_KEY) if isinstance(metadata, Mapping) else None
            if not isinstance(stored_contract, Mapping):
                raise GatewayValidationError("reservation was not admitted by this gateway")
            return {"status": reservation.state, "source_kind": self.source_kind,
                    "reservation": _as_result(reservation), "contract": dict(stored_contract)}
        reservation = self.ledger.reserve(normalized, now=current)
        return {"status": reservation.state, "source_kind": self.source_kind,
                "reservation": _as_result(reservation), "contract": contract}

    admit = reserve

    def _reservation_contract(self, reservation_id: str) -> tuple[Reservation, dict[str, Any]]:
        reservation = self.ledger.get_reservation(reservation_id)
        rows = [event for event in self.ledger.audit()
                if event.event_type == "reserve" and event.entity_id == reservation.reservation_id]
        if len(rows) != 1:
            raise GatewayValidationError("reservation provenance is unresolved")
        payload = rows[0].payload.get("payload", {})
        metadata = payload.get("metadata", {})
        contract = metadata.get(_GATEWAY_METADATA_KEY) if isinstance(metadata, Mapping) else None
        if not isinstance(contract, Mapping):
            raise GatewayValidationError("reservation was not admitted by this gateway")
        return reservation, dict(contract)

    def _fill_exists(self, fill_id: str) -> bool:
        return any(event.event_type == "fill" and event.entity_id == fill_id for event in self.ledger.audit())

    def record_fill(self, fill_event: Mapping[str, Any], *, now: datetime | str | None = None) -> dict[str, Any]:
        """Record one explicit observed, full-size modeled fill event.

        The event must carry its own provenance, quote, depth, modeled cost,
        quantity, and execution timestamp.  A proposal alone can never produce
        a fill.  The ledger still enforces idempotency and all existing caps.
        """
        current = self._now(now)
        event = _mapping(fill_event, "fill_event")
        reservation_id = _text(event.get("reservation_id"), "reservation_id")
        reservation, contract = self._reservation_contract(reservation_id)
        kind = _text(event.get("source_kind"), "source_kind")
        if kind != self.source_kind or kind != contract.get("source_kind"):
            raise GatewayValidationError("fill provenance does not match reservation")
        source, observed, received = self._source(event, now=current, validate_fresh=False)
        quote = self._quote(event.get("quote"), field="fill_event.quote")
        quote_time = _timestamp(quote["timestamp"], "fill_event.quote.timestamp")
        if quote_time != observed:
            raise GatewayValidationError("fill quote timestamp must equal fill source.observed_at")
        original_time = _timestamp(contract["quote"]["timestamp"], "proposal quote timestamp")
        if quote_time < original_time:
            raise GatewayValidationError("fill quote cannot precede proposal quote")
        quantity = _int(event.get("quantity"), "quantity", minimum=1)
        if quantity != reservation.quantity or quantity != contract["quantity"]:
            raise GatewayValidationError("partial or oversized fills are not supported")
        if quote["depth"] < quantity:
            raise GatewayValidationError("fill quote depth is insufficient for all-or-none quantity")
        cost = dict(_mapping(event.get("cost"), "fill_event.cost"))
        unit_cost = _int(cost.get("unit_cost_cents"), "fill_event.cost.unit_cost_cents", minimum=1)
        fee = _int(cost.get("fee_cents", 0), "fill_event.cost.fee_cents")
        if unit_cost != quote["ask_cents"]:
            raise GatewayValidationError("fill cost must equal its observed ask")
        original_unit = _int(contract["quote"]["ask_cents"], "proposal ask")
        reservation_cost = _mapping(event.get("reservation_cost", {"fee_cents": reservation.fee_cents}), "reservation_cost")
        original_fee = _int(reservation_cost.get("fee_cents", reservation.fee_cents), "reservation_cost.fee_cents")
        if original_fee != reservation.fee_cents:
            raise GatewayValidationError("reservation_cost does not match the durable reservation")
        if unit_cost > original_unit or fee > reservation.fee_cents:
            raise GatewayValidationError("modeled fill cost exceeds reserved cost")
        filled_at = _timestamp(event.get("filled_at"), "filled_at")
        if filled_at > current:
            raise GatewayValidationError("filled_at is in the future")
        if filled_at < received:
            raise GatewayValidationError("filled_at precedes source receipt")
        if filled_at < quote_time:
            raise GatewayValidationError("filled_at precedes fill quote")
        delay = _int(_mapping(contract.get("execution"), "proposal execution").get("delay_seconds"), "execution.delay_seconds")
        reservation_created = _timestamp(reservation.created_at, "reservation.created_at")
        if quote_time < reservation_created + timedelta(seconds=delay):
            raise GatewayValidationError("fill quote arrived before the declared post-reservation delay")
        if filled_at < reservation_created:
            raise GatewayValidationError("filled_at precedes reservation")
        event_id = _text(event.get("fill_id"), "fill_id")
        event_payload = {
            "reservation_id": reservation_id, "fill_id": event_id,
            "source_kind": kind, "source": source, "quote": quote,
            "quantity": quantity, "cost": {"unit_cost_cents": unit_cost, "fee_cents": fee},
            "filled_at": filled_at.isoformat().replace("+00:00", "Z"),
        }
        digest = hashlib.sha256(_canonical(event_payload).encode("utf-8")).hexdigest()[:24]
        ledger_fill_id = f"gateway-{event_id}-{digest}"
        replay = self._fill_exists(ledger_fill_id)
        if not replay:
            if _timestamp(reservation.expiry_at, "reservation.expiry_at") <= current:
                raise GatewayValidationError("reservation expired before fill submission")
            source, observed, received = self._source(event, now=current, validate_fresh=True)
            event_payload["source"] = source
        try:
            fill = self.ledger.simulate_fill(
                reservation_id, fill_id=ledger_fill_id, quantity=quantity,
                unit_cost_cents=unit_cost, fee_cents=fee, provenance=self.source_kind,
                filled_at=filled_at,
            )
        except InvalidState as exc:
            raise GatewayValidationError(f"reservation lifecycle is not fillable: {exc}") from exc
        return {"status": "filled", "source_kind": self.source_kind,
                "fill_event": event_payload, "fill": _as_result(fill)}

    fill = record_fill

    def cancel(self, reservation_id: str, *, reason: str, now: datetime | str | None = None) -> dict[str, Any]:
        """Cancel a still-reserved proposal through the ledger lifecycle path."""
        current = self._now(now)
        _reservation, _contract = self._reservation_contract(reservation_id)
        try:
            result = self.ledger.cancel(reservation_id, reason=_text(reason, "reason"), cancelled_at=current)
        except InvalidState as exc:
            raise GatewayValidationError(f"reservation lifecycle is not cancellable: {exc}") from exc
        return {"status": result.state, "source_kind": self.source_kind,
                "reservation": _as_result(result)}


__all__ = ["GatewayValidationError", "PaperProposalGateway"]
