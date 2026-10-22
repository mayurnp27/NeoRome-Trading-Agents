"""Persistent, network-free lifecycle for filled options debit verticals.

This module is deliberately a close and reconciliation boundary.  It does not
submit orders, discover broker holdings, or settle assignment/expiry.  A
caller supplies an immutable entry record, execution reports, a complete
holdings snapshot, and independently received leg quotes.  The manager uses
the existing options review/reconciliation functions and the shared
``PaperLedger`` close path only after all of those inputs have been checked.
"""

from __future__ import annotations

import contextlib
import copy
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
import fcntl
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
from typing import Any, Callable, Mapping, Sequence

from desks.options.position_review import review_position
from desks.options.reconciliation import reconcile
from desks.options.verticals import OptionLeg, ValidatedVertical, validate_vertical

from .paper_ledger import MarketMark, PaperLedger, ValidationError

UTC = timezone.utc
STATE_VERSION = 1
QUOTE_MAX_AGE = timedelta(seconds=60)
MIN_LATER_QUOTE_DELAY = timedelta(seconds=10)
INTENT_TTL = timedelta(seconds=60)


class OptionsLifecycleError(ValidationError):
    """A lifecycle contract is malformed or cannot be admitted safely."""


def _stamp(value: Any, field: str = "timestamp") -> str:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise OptionsLifecycleError(f"{field} must be an ISO-8601 timestamp") from exc
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise OptionsLifecycleError(f"{field} must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _dt(value: Any, field: str = "timestamp") -> datetime:
    return datetime.fromisoformat(_stamp(value, field).replace("Z", "+00:00"))


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise OptionsLifecycleError(f"{field} must be a non-empty string")
    return value.strip()


def _int(value: Any, field: str, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise OptionsLifecycleError(f"{field} must be an integer")
    if minimum is not None and value < minimum:
        raise OptionsLifecycleError(f"{field} must be >= {minimum}")
    return value


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise OptionsLifecycleError(f"{field} must be an object")
    return value


def _canonical(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise OptionsLifecycleError("lifecycle evidence must contain JSON values only") from exc


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _hash(value: Any, field: str) -> str:
    result = _text(value, field).lower()
    if len(result) != 64 or any(c not in "0123456789abcdef" for c in result):
        raise OptionsLifecycleError(f"{field} must be a 64-character hexadecimal digest")
    return result


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return _stamp(value)
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return value


def _ceil_cents(value: Any) -> int:
    return int((Decimal(str(value)) * Decimal("100")).to_integral_value(rounding=ROUND_CEILING))


def _floor_cents(value: Any) -> int:
    return int((Decimal(str(value)) * Decimal("100")).to_integral_value(rounding=ROUND_FLOOR))


def _entry_digest(value: Mapping[str, Any]) -> str:
    clean = dict(value)
    clean.pop("entry_digest", None)
    return _digest(clean)


class OptionsLifecycle:
    """Own one or more durable options lifecycle records.

    ``state_dir`` contains ``state.json`` and ``options.lock``.  If
    ``runtime_lock`` is supplied, it is a caller-owned context manager and is
    entered around the complete read/review/evidence/ledger-close sequence;
    the lifecycle never closes or unlocks a foreign file descriptor itself.
    """

    def __init__(
        self,
        ledger: PaperLedger,
        state_dir: str | os.PathLike[str],
        *,
        clock: Callable[[], datetime] | None = None,
        runtime_lock: Any = None,
    ) -> None:
        if not isinstance(ledger, PaperLedger):
            raise OptionsLifecycleError("ledger must be a PaperLedger")
        self.ledger = ledger
        self.state_dir = Path(state_dir)
        self.state_path = self.state_dir / "state.json"
        self.lock_path = self.state_dir / "options.lock"
        self._clock = clock or (lambda: datetime.now(UTC))
        self._runtime_lock = runtime_lock
        self._state_lock = threading.RLock()

    def _now(self, value: datetime | str | None) -> datetime:
        return _dt(value if value is not None else self._clock(), "now")

    @contextlib.contextmanager
    def _locked(self):
        self.state_dir.mkdir(parents=True, exist_ok=True)
        with self._state_lock:
            with self.lock_path.open("a+") as handle:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                try:
                    if self._runtime_lock is None:
                        yield
                    else:
                        with self._runtime_lock:
                            yield
                finally:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _load(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {"schema_version": STATE_VERSION, "status": "empty", "history": []}
        try:
            value = json.loads(self.state_path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise OptionsLifecycleError("options lifecycle state is unreadable") from exc
        if not isinstance(value, dict) or value.get("schema_version") != STATE_VERSION:
            raise OptionsLifecycleError("unsupported options lifecycle state")
        return value

    def _save(self, state: Mapping[str, Any]) -> None:
        payload = _canonical(_jsonable(state))
        self.state_dir.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(prefix=".options-state-", dir=self.state_dir)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                handle.write(payload + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_name, self.state_path)
            directory = os.open(self.state_dir, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except Exception:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass
            raise

    @staticmethod
    def _blocked(state: dict[str, Any], status: str, blockers: Sequence[str], *, now: datetime) -> dict[str, Any]:
        state.update({"status": status, "blockers": list(dict.fromkeys(str(item) for item in blockers)),
                      "risk_retained": True, "updated_at": _stamp(now)})
        return state

    @staticmethod
    def _accounting_latch(state: dict[str, Any], blockers: Sequence[str], *, now: datetime) -> dict[str, Any]:
        state["accounting_reconciliation_latched"] = True
        return OptionsLifecycle._blocked(state, "accounting_reconciliation_required", blockers, now=now)

    @staticmethod
    def _hard_accounting_block(blockers: Sequence[str], reconciliation_state: Any = None) -> bool:
        if reconciliation_state == "partial":
            return True
        text = " ".join(str(item).lower() for item in blockers)
        return any(phrase in text for phrase in (
            "assignment", "assigned", "underlying exposure", "holdings differ",
            "unexpected holdings", "invalid snapshot leg holdings", "missing activity", "incomplete spread",
            "expiration-day", "expired position",
        ))

    def _reserve_payload(self, proposal_id: str) -> Mapping[str, Any]:
        matches = []
        for event in self.ledger.audit():
            if event.event_type != "reserve":
                continue
            payload = event.payload.get("payload")
            if isinstance(payload, Mapping) and payload.get("proposal_id") == proposal_id:
                matches.append(payload)
        if len(matches) != 1:
            raise OptionsLifecycleError("recognized options position lacks one durable gateway reservation")
        return matches[0]

    @staticmethod
    def _leg_key(leg: OptionLeg) -> str:
        return leg.symbol

    @staticmethod
    def _leg_projection(leg: OptionLeg) -> dict[str, Any]:
        return {"symbol": leg.symbol, "underlying": leg.underlying, "right": leg.right,
                "expiry": str(leg.expiry), "strike": str(leg.strike), "side": leg.side,
                "quantity": leg.quantity, "bid": str(leg.bid), "ask": str(leg.ask),
                "timestamp": _stamp(leg.timestamp), "multiplier": leg.multiplier, "adjusted": leg.adjusted}

    @staticmethod
    def _leg_identity(leg: OptionLeg) -> dict[str, Any]:
        return {"symbol": leg.symbol, "underlying": leg.underlying, "right": leg.right,
                "expiry": str(leg.expiry), "strike": str(leg.strike), "side": leg.side,
                "quantity": leg.quantity, "multiplier": leg.multiplier, "adjusted": leg.adjusted}

    def _entry(self, record: Mapping[str, Any], position_id: str) -> tuple[dict[str, Any], ValidatedVertical, Mapping[str, Any]]:
        entry = dict(_mapping(record, "entry_record"))
        if entry.get("position_id") != position_id:
            raise OptionsLifecycleError("entry record position_id does not match selected position")
        raw_input = _mapping(entry.get("raw_input"), "entry_record.raw_input")
        supplied_raw_digest = _hash(entry.get("raw_input_digest"), "entry_record.raw_input_digest")
        if supplied_raw_digest != _digest(raw_input):
            raise OptionsLifecycleError("entry raw_input_digest does not match raw_input")
        raw_source = _mapping(raw_input.get("source"), "entry_record.raw_input.source")
        raw_source_hash = _hash(raw_source.get("hash"), "entry_record.raw_input.source.hash")
        legs_raw = entry.get("legs")
        if not isinstance(legs_raw, list) or len(legs_raw) != 2:
            raise OptionsLifecycleError("entry_record requires exactly two immutable legs")
        position = self.ledger.get_position(position_id)
        if position.state != "open":
            raise OptionsLifecycleError(f"position {position_id!r} is not open")
        opened_at = _dt(position.opened_at, "position.opened_at")
        entry_as_of = _dt(entry.get("entry_as_of"), "entry_record.entry_as_of")
        if entry_as_of != opened_at:
            raise OptionsLifecycleError("entry_as_of must equal the durable ledger fill time")
        entry_fee = Decimal(str(entry.get("entry_fee_per_spread", Decimal(position.entry_fee_cents) / Decimal("100") / position.quantity)))
        try:
            legs = tuple(OptionLeg.from_dict(item) for item in legs_raw)
            vertical = validate_vertical(legs, as_of=entry_as_of, max_quote_age=QUOTE_MAX_AGE,
                                        fee_per_contract=entry_fee,
                                        slippage_per_contract=Decimal(str(entry.get("entry_slippage_per_spread", "0"))))
        except Exception as exc:
            raise OptionsLifecycleError(f"entry vertical is not valid: {exc}") from exc
        raw_legs = raw_input.get("legs")
        if not isinstance(raw_legs, list) or len(raw_legs) != 2:
            raise OptionsLifecycleError("raw entry input must contain both original legs")
        try:
            raw_vertical_legs = tuple(OptionLeg.from_dict(item) for item in raw_legs)
        except Exception as exc:
            raise OptionsLifecycleError(f"raw entry legs are not valid: {exc}") from exc
        if tuple(self._leg_identity(leg) for leg in raw_vertical_legs) != tuple(self._leg_identity(leg) for leg in legs):
            raise OptionsLifecycleError("raw proposal trace is not bound to filled leg identity")
        for field in ("proposal_id", "strategy", "strategy_version", "instrument", "exposure_group", "idea_id"):
            if field not in entry:
                raise OptionsLifecycleError(f"entry_record missing {field}")
        if entry["proposal_id"] != position.proposal_id or _int(entry["quantity"], "entry_record.quantity", 1) != position.quantity:
            raise OptionsLifecycleError("entry record does not match ledger quantity or proposal")
        if _int(entry.get("ledger_cost_cents"), "entry_record.ledger_cost_cents", 1) != position.basis_cents:
            raise OptionsLifecycleError("entry record cost does not match ledger basis")
        if _int(entry.get("ledger_fee_cents", 0), "entry_record.ledger_fee_cents", 0) != position.entry_fee_cents:
            raise OptionsLifecycleError("entry record fee does not match ledger entry fee")
        if vertical.quantity != position.quantity:
            raise OptionsLifecycleError("validated vertical quantity does not match ledger position")
        modeled_entry_fees = _ceil_cents(vertical.fees_and_slippage)
        premium_cents = _ceil_cents(vertical.net_debit * Decimal("100")) * vertical.quantity
        if modeled_entry_fees != position.entry_fee_cents:
            raise OptionsLifecycleError("modeled entry fees do not match ledger entry fees")
        if premium_cents + modeled_entry_fees != position.basis_cents:
            raise OptionsLifecycleError("validated entry premium and modeled fees do not match ledger basis")
        payload = self._reserve_payload(position.proposal_id)
        if payload.get("desk") != "options" or payload.get("provenance") != position.provenance:
            raise OptionsLifecycleError("position is not an options gateway position")
        for field in ("strategy", "strategy_version", "instrument", "exposure_group", "idea_id", "quantity"):
            if payload.get(field) != entry.get(field):
                raise OptionsLifecycleError(f"entry record {field} differs from durable reservation")
        metadata = payload.get("metadata")
        contract = metadata.get("paper_gateway_v1") if isinstance(metadata, Mapping) else None
        if not isinstance(contract, Mapping) or contract.get("instrument_type") != "debit_spread":
            raise OptionsLifecycleError("position is not a recognized debit-spread gateway record")
        contract_legs = {(str(x.get("symbol")), str(x.get("side"))) for x in contract.get("legs", []) if isinstance(x, Mapping)}
        expected_contract_legs = {(leg.symbol.replace(" ", ""), leg.side.lower()) for leg in legs}
        if contract_legs != expected_contract_legs:
            raise OptionsLifecycleError("immutable legs do not match gateway leg identity")
        if contract.get("quantity") != position.quantity:
            raise OptionsLifecycleError("gateway quantity does not match ledger position")
        source = contract.get("source")
        if not isinstance(source, Mapping) or source.get("kind") != position.provenance:
            raise OptionsLifecycleError("entry source provenance is unresolved")
        gateway_source_hash = _hash(source.get("hash"), "gateway source.hash")
        if raw_source_hash != gateway_source_hash or raw_source.get("kind") != source.get("kind"):
            raise OptionsLifecycleError("raw entry input is not bound to the durable gateway source")
        fill_input = _mapping(entry.get("fill_input"), "entry_record.fill_input")
        if set(fill_input) != {"reservation_id", "fill_id", "source_kind", "source", "quote", "quantity", "cost", "filled_at"}:
            raise OptionsLifecycleError("fill_input must be the exact normalized gateway fill_event")
        fill_input_digest = _hash(entry.get("fill_input_digest"), "entry_record.fill_input_digest")
        if fill_input_digest != _digest(fill_input):
            raise OptionsLifecycleError("fill_input_digest does not match fill_input")
        expected_ledger_fill_id = f"gateway-{_text(fill_input.get('fill_id'), 'fill_input.fill_id')}-{_digest(fill_input)[:24]}"
        if expected_ledger_fill_id != position.fill_id:
            raise OptionsLifecycleError("normalized gateway fill_event does not identify the ledger fill")
        fill_audits = [event for event in self.ledger.audit()
                       if event.event_type == "fill" and event.entity_id == position.fill_id]
        if len(fill_audits) != 1:
            raise OptionsLifecycleError("ledger fill lacks one exact audit record")
        fill_audit = fill_audits[0].payload
        if (fill_audit.get("position_id") != position.position_id
                or fill_audit.get("reservation_id") != fill_input.get("reservation_id")):
            raise OptionsLifecycleError("gateway fill identity differs from the ledger audit")
        fill_source = _mapping(fill_input.get("source"), "fill_input.source")
        if fill_source.get("kind") != position.provenance:
            raise OptionsLifecycleError("fill source provenance differs from ledger position")
        _hash(fill_source.get("hash"), "fill_input.source.hash")
        fill_received = _dt(fill_source.get("received_at"), "fill_input.source.received_at")
        fill_quote = _mapping(fill_input.get("quote"), "fill_input.quote")
        fill_quote_stamp = _dt(fill_quote.get("timestamp"), "fill_input.quote.timestamp")
        if _dt(fill_source.get("observed_at"), "fill_input.source.observed_at") != fill_quote_stamp:
            raise OptionsLifecycleError("fill source and quote timestamps differ")
        filled_at = _dt(fill_input.get("filled_at"), "fill_input.filled_at")
        if (filled_at != _dt(position.opened_at, "position.opened_at") or filled_at < fill_quote_stamp or
                fill_received < fill_quote_stamp or fill_received > filled_at):
            raise OptionsLifecycleError("fill evidence time does not match the ledger fill")
        fill_quantity = _int(fill_input.get("quantity"), "fill_input.quantity", 1)
        fill_cost = _mapping(fill_input.get("cost"), "fill_input.cost")
        fill_unit = _int(fill_cost.get("unit_cost_cents"), "fill_input.cost.unit_cost_cents", 1)
        fill_fee = _int(fill_cost.get("fee_cents", 0), "fill_input.cost.fee_cents", 0)
        if (_int(fill_quote.get("ask_cents"), "fill_input.quote.ask_cents", 1) != fill_unit or
                _int(fill_quote.get("depth"), "fill_input.quote.depth", fill_quantity) < fill_quantity or
                fill_quantity != position.quantity or fill_fee != position.entry_fee_cents or
                fill_unit * fill_quantity != premium_cents or fill_unit * fill_quantity + fill_fee != position.basis_cents or
                fill_audit.get("quantity") != fill_quantity or fill_audit.get("unit_cost_cents") != fill_unit or
                fill_audit.get("fee_cents") != fill_fee or fill_audit.get("provenance") != position.provenance):
            raise OptionsLifecycleError("fill evidence does not prove the ledger quantity and basis")
        leg_times = [_dt(leg.timestamp, "filled leg timestamp") for leg in legs]
        if min(leg_times) != fill_quote_stamp or any(stamp > opened_at for stamp in leg_times):
            raise OptionsLifecycleError("filled leg quotes do not match the gateway fill chronology")
        entry["entry_as_of"] = _stamp(entry_as_of)
        entry["legs"] = [_jsonable(dict(item)) for item in legs_raw]
        entry["raw_input"] = _jsonable(raw_input)
        entry["raw_input_digest"] = supplied_raw_digest
        entry["fill_input"] = _jsonable(fill_input)
        entry["fill_input_digest"] = fill_input_digest
        entry["entry_digest"] = _entry_digest(entry)
        return entry, vertical, contract

    def _quote_bundle(self, quotes: Mapping[str, Any], sources: Mapping[str, Any], entry: ValidatedVertical,
                      now: datetime, *, provenance: str,
                      prior_sources: Mapping[str, str] | None = None) -> tuple[dict[str, Any], dict[str, Any], str]:
        if not isinstance(quotes, Mapping) or not isinstance(sources, Mapping):
            raise OptionsLifecycleError("complete current quotes and per-leg sources are required")
        symbols = {leg.symbol for leg in entry.legs}
        if set(quotes) != symbols or set(sources) != symbols:
            raise OptionsLifecycleError("quotes and sources must exactly cover both original legs")
        clean_quotes: dict[str, Any] = {}
        clean_sources: dict[str, Any] = {}
        for leg in entry.legs:
            quote = dict(_mapping(quotes[leg.symbol], f"quotes[{leg.symbol}]"))
            stamp = _dt(quote.get("timestamp"), f"quotes[{leg.symbol}].timestamp")
            if stamp > now or stamp < entry.as_of or now - stamp > QUOTE_MAX_AGE:
                raise OptionsLifecycleError("option quote is future-dated, pre-entry, or stale")
            bid = Decimal(str(quote.get("bid"))); ask = Decimal(str(quote.get("ask")))
            if bid < 0 or ask <= 0 or bid > ask:
                raise OptionsLifecycleError("option quote is crossed or non-positive")
            size_field = "bid_size" if leg.side.lower() == "buy" else "ask_size"
            _int(quote.get(size_field), f"quotes[{leg.symbol}].{size_field}", entry.quantity)
            clean_quotes[leg.symbol] = {"bid": str(bid), "ask": str(ask), "timestamp": _stamp(stamp),
                                        "bid_size": quote["bid_size"], "ask_size": quote["ask_size"]}
            source = dict(_mapping(sources[leg.symbol], f"sources[{leg.symbol}]"))
            if source.get("symbol") != leg.symbol or source.get("provenance") != provenance:
                raise OptionsLifecycleError("quote source symbol or provenance does not match the position")
            source_hash = _hash(source.get("hash"), f"sources[{leg.symbol}].hash")
            observed = _dt(source.get("observed_at"), f"sources[{leg.symbol}].observed_at")
            received = _dt(source.get("received_at"), f"sources[{leg.symbol}].received_at")
            if observed != stamp or observed > received or received > now or received - observed > QUOTE_MAX_AGE:
                raise OptionsLifecycleError("per-leg provider/receipt chronology is invalid")
            source_quote = _mapping(source.get("quote"), f"sources[{leg.symbol}].quote")
            try:
                source_quote_normalized = {"bid": str(Decimal(str(source_quote.get("bid")))),
                                         "ask": str(Decimal(str(source_quote.get("ask")))),
                                         "timestamp": _stamp(source_quote.get("timestamp")),
                                         "bid_size": _int(source_quote.get("bid_size"), "source quote bid_size", 0),
                                         "ask_size": _int(source_quote.get("ask_size"), "source quote ask_size", 0)}
            except Exception as exc:
                raise OptionsLifecycleError("source quote payload is malformed") from exc
            if source_quote_normalized != clean_quotes[leg.symbol]:
                raise OptionsLifecycleError("quote source payload does not match normalized quote")
            expected_quote_digest = _digest({"symbol": leg.symbol, "quote": clean_quotes[leg.symbol]})
            if _hash(source.get("quote_digest"), f"sources[{leg.symbol}].quote_digest") != expected_quote_digest:
                raise OptionsLifecycleError("quote source digest does not match normalized quote")
            source_material = {"symbol": leg.symbol, "provenance": provenance,
                               "provider": _text(source.get("provider"), f"sources[{leg.symbol}].provider"),
                               "quote": clean_quotes[leg.symbol], "observed_at": _stamp(observed),
                               "received_at": _stamp(received)}
            if source_hash != _digest(source_material):
                raise OptionsLifecycleError("quote source hash does not match its normalized source payload")
            if prior_sources and source_hash == prior_sources.get(leg.symbol):
                raise OptionsLifecycleError("later provider quote reused a prior source hash")
            clean_sources[leg.symbol] = {**source_material, "hash": source_hash,
                                         "quote_digest": expected_quote_digest}
        return clean_quotes, clean_sources, _digest({"quotes": clean_quotes, "sources": clean_sources})

    def _recover_pending(self, state: dict[str, Any], now: datetime) -> dict[str, Any]:
        pending = state.get("pending_close")
        if not isinstance(pending, Mapping):
            return state
        position_id = pending.get("position_id")
        try:
            position = self.ledger.get_position(position_id)
        except Exception:
            return self._blocked(state, "reconciliation_required", ["pending close references an unknown position"], now=now)
        if position.state == "closed":
            audits = [event for event in self.ledger.audit()
                      if event.event_type == "close" and event.entity_id == pending.get("close_id")]
            if len(audits) != 1:
                return self._blocked(state, "reconciliation_required", ["closed position lacks its exact close audit"], now=now)
            payload = audits[0].payload
            expected_realized = int(pending.get("proceeds_cents")) - int(pending.get("fee_cents")) - position.basis_cents
            if (payload.get("position_id") != position_id or payload.get("proceeds_cents") != pending.get("proceeds_cents") or
                    payload.get("fee_cents") != pending.get("fee_cents") or payload.get("provenance") != position.provenance or
                    payload.get("realized_pnl_cents") != expected_realized):
                return self._blocked(state, "reconciliation_required", ["close audit does not match pending close evidence"], now=now)
            state.setdefault("history", []).append({"event": "close_recovered", "pending_close": dict(pending), "at": _stamp(now)})
            state["close_evidence"] = dict(pending)
            state["pending_close"] = None
            state["intent"] = None
            state["status"] = "closed"
            state["risk_retained"] = False
            state["updated_at"] = _stamp(now)
        return state

    def position_mark(
        self,
        entry_record: Mapping[str, Any],
        *,
        reports: Sequence[Mapping[str, Any]],
        holdings_snapshot: Mapping[str, Any],
        quotes: Mapping[str, Any],
        quote_sources: Mapping[str, Any],
        now: datetime | str | None = None,
        exit_fee_per_spread: Any = 0,
        exit_slippage_per_spread: Any = 0,
    ) -> MarketMark:
        """Return one validated own-position mark without mutating the ledger.

        The caller still owns complete shared-mark coverage. This method only
        validates this options position's authoritative holdings and quote
        evidence; it refuses a negative liability and never clips it.
        """
        current = self._now(now)
        with self._locked():
            selected = entry_record.get("position_id") if isinstance(entry_record, Mapping) else None
            if not isinstance(selected, str) or not selected:
                raise OptionsLifecycleError("position_id is required for an options mark")
            entry, validated, _contract = self._entry(entry_record, selected)
            reconciliation = reconcile(validated, list(reports), dict(holdings_snapshot), as_of=current)
            if reconciliation.get("state") != "intact" or reconciliation.get("spread_valuation_allowed") is not True:
                raise OptionsLifecycleError("authoritative holdings are not intact for marking")
            provenance = self.ledger.get_position(selected).provenance
            clean_quotes, clean_sources, quote_digest = self._quote_bundle(
                quotes, quote_sources, validated, current, provenance=provenance)
            review = review_position(validated, clean_quotes, reconciliation["expected_holdings"], as_of=current,
                                     exit_fee_per_spread=exit_fee_per_spread,
                                     exit_slippage_per_spread=exit_slippage_per_spread)
            if review.get("status") != "reviewed":
                raise OptionsLifecycleError("position review blocked the options mark: " + "; ".join(review.get("blockers", [])))
            if review.get("shared_mark_eligible") is not True:
                raise OptionsLifecycleError("negative liquidation liability cannot be marked")
            source_timestamp = min(_dt(source["observed_at"]) for source in clean_sources.values())
            return MarketMark(selected, int(review["liquidation_value_cents"]), source_timestamp, quote_digest, provenance)

    def run(
        self,
        entry_record: Mapping[str, Any] | None = None,
        *,
        position_id: str | None = None,
        reports: Sequence[Mapping[str, Any]] | None = None,
        holdings_snapshot: Mapping[str, Any] | None = None,
        quotes: Mapping[str, Any] | None = None,
        quote_sources: Mapping[str, Any] | None = None,
        signal_reversed: bool = False,
        exit_fee_per_spread: Any = 0,
        exit_slippage_per_spread: Any = 0,
        now: datetime | str | None = None,
    ) -> dict[str, Any]:
        """Review and, only after a later independent quote, close one position.

        The first qualifying review creates a 60-second intent.  A second
        provider bundle at least ten seconds later and with new per-leg source
        hashes is required before the ledger close.  Reports and snapshots are
        the authoritative holdings contract; omitted or incomplete holdings
        always fail closed.
        """
        current = self._now(now)
        with self._locked():
            state = self._recover_pending(self._load(), current)
            if not isinstance(signal_reversed, bool):
                raise OptionsLifecycleError("signal_reversed must be an explicit boolean")
            if state.get("status") == "closed" and entry_record is None:
                self._save(state)
                return copy.deepcopy(state)
            if entry_record is None:
                saved = state.get("entry_record")
                if not isinstance(saved, Mapping):
                    result = self._blocked(state, "reconciliation_required", ["missing authoritative entry record"], now=current)
                    self._save(result)
                    return copy.deepcopy(result)
                entry_record = saved
            selected = position_id or entry_record.get("position_id")
            if not isinstance(selected, str) or not selected:
                result = self._blocked(state, "reconciliation_required", ["position_id is required"], now=current)
                self._save(result)
                return copy.deepcopy(result)
            if state.get("status") == "closed" and state.get("position_id") == selected:
                state["updated_at"] = _stamp(current)
                self._save(state)
                return copy.deepcopy(state)
            if state.get("accounting_reconciliation_latched") is True:
                state.update({"status": "accounting_reconciliation_required", "risk_retained": True,
                              "updated_at": _stamp(current)})
                self._save(state)
                return copy.deepcopy(state)
            prior_entry = state.get("entry_record")
            if prior_entry is not None and prior_entry.get("position_id") != selected:
                if state.get("status") != "closed" or state.get("intent") is not None:
                    result = self._blocked(state, "reconciliation_required", ["cross-position stale lifecycle intent"], now=current)
                    self._save(result)
                    return copy.deepcopy(result)
                state = {"schema_version": STATE_VERSION, "status": "empty", "history": state.get("history", [])}
            try:
                entry, validated, contract = self._entry(entry_record, selected)
            except Exception as exc:
                result = self._blocked(state, "reconciliation_required", [str(exc)], now=current)
                self._save(result)
                return copy.deepcopy(result)
            if state.get("entry_digest") and state.get("entry_digest") != entry["entry_digest"]:
                result = self._blocked(state, "reconciliation_required", ["immutable entry record changed"], now=current)
                self._save(result)
                return copy.deepcopy(result)
            state.update({"schema_version": STATE_VERSION, "entry_record": entry, "entry_digest": entry["entry_digest"],
                          "position_id": selected, "provenance": contract.get("source_kind"), "risk_retained": True})
            existing_intent = state.get("intent")
            if isinstance(existing_intent, Mapping) and existing_intent.get("position_id") == selected and current >= _dt(existing_intent["expires_at"]):
                state["intent"] = None
                state.update({"status": "intent_expired", "blockers": ["exit intent expired before independent fill quote"],
                              "updated_at": _stamp(current)})
                self._save(state)
                return copy.deepcopy(state)
            if isinstance(existing_intent, Mapping) and existing_intent.get("position_id") not in (None, selected):
                result = self._blocked(state, "reconciliation_required", ["cross-position or changed-entry intent"], now=current)
                self._save(result)
                return copy.deepcopy(result)
            if reports is None or holdings_snapshot is None:
                result = self._blocked(state, "reconciliation_required", ["missing authoritative holdings reports or complete snapshot"], now=current)
                self._save(result)
                return copy.deepcopy(result)
            try:
                reconciliation = reconcile(validated, list(reports), dict(holdings_snapshot), as_of=current)
            except Exception as exc:
                reconciliation = {"state": "reconciliation_required", "blockers": [str(exc)],
                                  "spread_valuation_allowed": False, "risk_release_allowed": False}
            state["reconciliation"] = _jsonable(reconciliation)
            state["holdings_digest"] = _digest({"reports": _jsonable(list(reports)), "snapshot": _jsonable(holdings_snapshot)})
            if reconciliation.get("state") != "intact" or reconciliation.get("spread_valuation_allowed") is not True:
                blockers = reconciliation.get("blockers", ["holdings are not intact"])
                result = (self._accounting_latch(state, blockers, now=current)
                          if self._hard_accounting_block(blockers, reconciliation.get("state"))
                          else self._blocked(state, "reconciliation_required", blockers, now=current))
                self._save(result)
                return copy.deepcopy(result)
            try:
                clean_quotes, clean_sources, quote_digest = self._quote_bundle(
                    quotes or {}, quote_sources or {}, validated, current, provenance=state["provenance"])
                review = review_position(validated, clean_quotes, reconciliation["expected_holdings"], as_of=current,
                                         exit_fee_per_spread=exit_fee_per_spread,
                                         exit_slippage_per_spread=exit_slippage_per_spread,
                                         signal_reversed=signal_reversed)
            except Exception as exc:
                clean_quotes, clean_sources, quote_digest = {}, {}, None
                review = {"status": "blocked", "blockers": [str(exc)], "exit_reasons": []}
            state["last_review"] = {"review": _jsonable(review), "quotes": clean_quotes, "sources": clean_sources,
                                    "quote_digest": quote_digest, "at": _stamp(current)}
            if review.get("status") != "reviewed":
                blockers = review.get("blockers", ["position review blocked"])
                result = (self._accounting_latch(state, blockers, now=current)
                          if self._hard_accounting_block(blockers)
                          else self._blocked(state, "reconciliation_required", blockers, now=current))
                self._save(result)
                return copy.deepcopy(result)
            if review.get("shared_mark_eligible") is not True:
                result = self._blocked(state, "reconciliation_required", ["negative liquidation liability retains risk"], now=current)
                self._save(result)
                return copy.deepcopy(result)
            intent = state.get("intent")
            if intent is None:
                if not review.get("exit_reasons"):
                    state.update({"status": "holding", "blockers": [], "risk_retained": True, "updated_at": _stamp(current)})
                    self._save(state)
                    return copy.deepcopy(state)
                state["intent"] = {"intent_id": f"intent-{selected}-{quote_digest[:24]}", "position_id": selected,
                                    "entry_digest": entry["entry_digest"], "created_at": _stamp(current),
                                    "eligible_at": _stamp(current + MIN_LATER_QUOTE_DELAY),
                                    "expires_at": _stamp(current + INTENT_TTL), "trigger_reasons": list(review["exit_reasons"]),
                                    "trigger_quote_digest": quote_digest,
                                    "trigger_quote_timestamps": {k: v["timestamp"] for k, v in clean_quotes.items()},
                                    "trigger_source_hashes": {k: v["hash"] for k, v in clean_sources.items()}}
                state.update({"status": "exit_intent", "blockers": ["independent later quote required"], "updated_at": _stamp(current)})
                self._save(state)
                return copy.deepcopy(state)
            if intent.get("position_id") != selected or intent.get("entry_digest") != entry["entry_digest"]:
                result = self._blocked(state, "reconciliation_required", ["cross-position or changed-entry intent"], now=current)
                self._save(result)
                return copy.deepcopy(result)
            if current >= _dt(intent["expires_at"]):
                state["intent"] = None
                state.update({"status": "intent_expired", "blockers": ["exit intent expired before independent fill quote"], "updated_at": _stamp(current)})
                self._save(state)
                return copy.deepcopy(state)
            if current < _dt(intent["eligible_at"]):
                state.update({"status": "exit_intent", "blockers": ["later quote delay has not elapsed"], "updated_at": _stamp(current)})
                self._save(state)
                return copy.deepcopy(state)
            if quote_digest == intent.get("trigger_quote_digest"):
                state.update({"status": "exit_intent", "blockers": ["same quote bundle cannot fill"], "updated_at": _stamp(current)})
                self._save(state)
                return copy.deepcopy(state)
            try:
                prior_sources = intent.get("trigger_source_hashes", {})
                if any(clean_sources[s]["hash"] == prior_sources.get(s) for s in clean_sources):
                    raise OptionsLifecycleError("each later leg quote requires a distinct provider hash")
                prior_times = intent.get("trigger_quote_timestamps", {})
                if any(_dt(clean_quotes[s]["timestamp"]) <= _dt(prior_times[s]) for s in clean_quotes):
                    raise OptionsLifecycleError("each later quote must be observed after the trigger quote")
                minimum_later = _dt(intent["created_at"]) + MIN_LATER_QUOTE_DELAY
                if any(_dt(clean_sources[s]["observed_at"]) < minimum_later or
                       _dt(clean_sources[s]["received_at"]) < minimum_later for s in clean_sources):
                    raise OptionsLifecycleError("provider quote timestamps must be at least ten seconds after intent creation")
            except Exception as exc:
                state.update({"status": "exit_intent", "blockers": [str(exc)], "updated_at": _stamp(current)})
                self._save(state)
                return copy.deepcopy(state)
            proceeds = _floor_cents(review.get("gross_close_dollars", "0"))
            fee = _ceil_cents(review.get("modeled_exit_cost_dollars", "0"))
            if proceeds < 0 or fee > proceeds:
                result = self._blocked(state, "reconciliation_required", ["close fee exceeds nonnegative gross proceeds"], now=current)
                self._save(result)
                return copy.deepcopy(result)
            close_id = f"close-{selected}-{intent['intent_id']}"
            evidence = {"position_id": selected, "close_id": close_id, "entry_digest": entry["entry_digest"],
                        "intent": dict(intent), "quotes": clean_quotes, "sources": clean_sources,
                        "quote_digest": quote_digest, "review": _jsonable(review), "proceeds_cents": proceeds,
                        "fee_cents": fee, "recorded_at": _stamp(current)}
            state["pending_close"] = evidence
            state.update({"status": "close_pending", "blockers": [], "updated_at": _stamp(current)})
            self._save(state)
            try:
                closed = self.ledger.close(selected, close_id=close_id, proceeds_cents=proceeds, fee_cents=fee,
                                           provenance=position_provenance(self.ledger, selected), closed_at=current)
            except Exception as exc:
                result = self._blocked(state, "reconciliation_required", [f"ledger close failed: {exc}"], now=current)
                self._save(result)
                return copy.deepcopy(result)
            evidence["ledger_close"] = _jsonable(closed.__dict__)
            state["close_evidence"] = evidence
            state["pending_close"] = None
            state["intent"] = None
            state.update({"status": "closed", "blockers": [], "risk_retained": False, "updated_at": _stamp(current)})
            self._save(state)
            return copy.deepcopy(state)


def position_provenance(ledger: PaperLedger, position_id: str) -> str:
    """Return the immutable ledger provenance for a close call."""
    return ledger.get_position(position_id).provenance


def run_options_lifecycle(ledger: PaperLedger, state_dir: str | os.PathLike[str], *args: Any, **kwargs: Any) -> dict[str, Any]:
    """Functional wrapper around :class:`OptionsLifecycle` for runtime use."""
    return OptionsLifecycle(ledger, state_dir, clock=kwargs.pop("clock", None), runtime_lock=kwargs.pop("runtime_lock", None)).run(*args, **kwargs)


__all__ = ["OptionsLifecycle", "OptionsLifecycleError", "run_options_lifecycle"]
