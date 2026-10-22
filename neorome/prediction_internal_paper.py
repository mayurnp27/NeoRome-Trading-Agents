"""Finite, zero-venue-order lifecycle for an admitted prediction candidate.

This is deliberately a caller-supplied-evidence adapter.  It does not fetch,
authenticate, load credentials, schedule itself, or submit venue orders.  A
valid observed-shadow candidate may reserve and simulate a paper position only
when it also clears conservative economic, timing, depth, and shared-book
controls.  Current captured weather and crypto evidence does not clear those
gates and therefore produces an auditable abstention.
"""
from __future__ import annotations

import contextlib
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, InvalidOperation
import fcntl
import hashlib
import json
import os
from pathlib import Path
import secrets
from typing import Any, Callable, Mapping

from .paper_ledger import LedgerError, MarketMark, PaperLedger, Proposal
from .prediction_full_port import FULL_PORT_CENTS, PredictionFullPortError, assert_full_port_book

UTC = timezone.utc
VERSION = "prediction-internal-paper-v1"
MARKER = "prediction_internal_paper"
MAX_AGE_SECONDS = 60
INTENT_SECONDS = 60
FILL_DELAY_SECONDS = 10
EDGE_FLOOR_CENTS_PER_CONTRACT = 3


class PredictionInternalPaperError(ValueError):
    pass


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def _time(value: Any, field: str) -> datetime:
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise PredictionInternalPaperError(f"{field} must be an ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PredictionInternalPaperError(f"{field} requires a timezone")
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _hash(value: Any, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdefABCDEF" for c in value):
        raise PredictionInternalPaperError(f"{field} must be a SHA-256 digest")
    return value.lower()


def _decimal(value: Any, field: str, low: Decimal = Decimal("0"), high: Decimal = Decimal("1")) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise PredictionInternalPaperError(f"{field} must be numeric") from exc
    if not result.is_finite() or result < low or result > high:
        raise PredictionInternalPaperError(f"{field} is outside its allowed range")
    return result


def _integer(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise PredictionInternalPaperError(f"{field} must be a positive integer")
    return value


def _ceil(value: Decimal) -> int:
    return int(value.to_integral_value(rounding=ROUND_CEILING))


def _floor(value: Decimal) -> int:
    return int(value.to_integral_value(rounding=ROUND_FLOOR))


def _walk(levels: Any, units: int, field: str) -> tuple[int, int]:
    if not isinstance(levels, list) or not levels:
        raise PredictionInternalPaperError(f"{field} is required")
    remaining, cents = units, Decimal("0")
    for index, level in enumerate(levels):
        if not isinstance(level, Mapping):
            raise PredictionInternalPaperError(f"{field}[{index}] is malformed")
        size = _integer(level.get("size"), f"{field}[{index}].size")
        price = _decimal(level.get("price"), f"{field}[{index}].price")
        used = min(remaining, size)
        cents += price * 100 * used
        remaining -= used
        if remaining == 0:
            return _ceil(cents), _floor(cents)
    raise PredictionInternalPaperError(f"{field} depth is insufficient")


def prepare(candidate: Mapping[str, Any], *, now: datetime | str) -> dict[str, Any]:
    """Validate an observed-shadow candidate and calculate a conservative lot."""
    if not isinstance(candidate, Mapping):
        raise PredictionInternalPaperError("candidate is required")
    body = {k: v for k, v in candidate.items() if k != "candidate_sha256"}
    if candidate.get("candidate_sha256") != _digest(body):
        raise PredictionInternalPaperError("candidate hash mismatch")
    if body.get("shadow_version") != "prediction-observed-shadow-v1" or body.get("source_kind") != "observed":
        raise PredictionInternalPaperError("only observed-shadow candidates are accepted")
    current = _time(now, "now")
    timing, market, contract, size, probability, fees = (body.get(k) for k in ("timing", "market_identity", "contract", "size", "probability", "fee_schema"))
    if not all(isinstance(x, Mapping) for x in (timing, market, contract, size, probability, fees)):
        raise PredictionInternalPaperError("candidate fields are incomplete")
    event_at, received_at, decision_at = (_time(timing.get(k), f"timing.{k}") for k in ("event_at", "received_at", "decision_at"))
    cutoff = _time(contract.get("cutoff_at"), "contract.cutoff_at")
    if not event_at <= received_at <= decision_at < cutoff or decision_at > current or (current - decision_at).total_seconds() > MAX_AGE_SECONDS:
        raise PredictionInternalPaperError("candidate timing is stale or causally invalid")
    for field in ("market_id", "outcome_id", "outcome", "terms_sha256"):
        if not isinstance(market.get(field), str) or not market[field]:
            raise PredictionInternalPaperError(f"market_identity.{field} is required")
    if market["market_id"] != contract.get("market_id") or market["outcome"] != contract.get("outcome"):
        raise PredictionInternalPaperError("candidate market and contract identities differ")
    units = _integer(size.get("contract_size"), "size.contract_size")
    if size.get("size_units") != "contracts":
        raise PredictionInternalPaperError("contract units must be explicit")
    ask_cents, _ = _walk(size.get("asks"), units, "size.asks")
    bid_cents, _ = _walk(size.get("bids"), units, "size.bids")
    side = str(market["outcome"]).lower()
    if side not in {"yes", "no"}:
        raise PredictionInternalPaperError("only YES or NO contracts are supported")
    if "lower_bound" not in probability or "upper_bound" not in probability:
        raise PredictionInternalPaperError("conservative probability bounds are required")
    low = _decimal(probability["lower_bound"], "probability.lower_bound")
    high = _decimal(probability["upper_bound"], "probability.upper_bound")
    point = _decimal(probability.get("value"), "probability.value")
    if not low <= point <= high:
        raise PredictionInternalPaperError("probability bounds do not contain point estimate")
    favorable = low if side == "yes" else Decimal("1") - high
    fee_values = []
    for name in ("entry", "exit", "settlement"):
        fee_values.append(_ceil(_decimal(fees.get(f"{name}_fee_cents_per_contract"), f"fee_schema.{name}", Decimal("0"), Decimal("10000")) * units))
    entry_fee, exit_fee, settlement_fee = fee_values
    conservative_cost = ask_cents + entry_fee + max(exit_fee, settlement_fee)
    edge = _floor(favorable * 100 * units) - conservative_cost
    if edge < EDGE_FLOOR_CENTS_PER_CONTRACT * units:
        raise PredictionInternalPaperError("conservative net edge is below the 3-cent floor")
    if conservative_cost > FULL_PORT_CENTS:
        raise PredictionInternalPaperError("prediction idea exceeds the isolated $100 sleeve")
    candidate_hash = _hash(candidate["candidate_sha256"], "candidate_sha256")
    return {"candidate_sha256": candidate_hash, "decision_at": _iso(decision_at), "cutoff_at": _iso(cutoff),
            "market_id": market["market_id"], "outcome_id": market["outcome_id"], "outcome": side,
            "terms_sha256": market["terms_sha256"], "units": units, "ask_cents": ask_cents,
            "bid_cents": bid_cents, "entry_fee_cents": entry_fee, "exit_fee_cents": exit_fee,
            "settlement_fee_cents": settlement_fee, "conservative_edge_cents": edge,
            "conservative_worst_case_cents": conservative_cost,
            "fee_version": fees.get("version"), "contract": dict(contract), "candidate": dict(candidate)}


def _root(value: str | os.PathLike[str]) -> Path:
    root = Path(value)
    if root.exists() and root.is_symlink():
        raise PredictionInternalPaperError("state directory symlink refused")
    root.mkdir(parents=True, exist_ok=True, mode=0o700); os.chmod(root, 0o700)
    return root


def _load(path: Path) -> dict[str, Any]:
    if not path.exists(): return {"schema_version": 1, "version": VERSION, "history": []}
    if path.is_symlink(): raise PredictionInternalPaperError("state file symlink refused")
    try: value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc: raise PredictionInternalPaperError("state is unreadable") from exc
    if not isinstance(value, dict) or value.get("version") != VERSION: raise PredictionInternalPaperError("state schema is unsupported")
    return value


def _write(path: Path, value: Mapping[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(fd, "w") as stream:
            fd = -1; stream.write(_canonical(value) + "\n"); stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if fd >= 0: os.close(fd)
        with contextlib.suppress(FileNotFoundError): temporary.unlink()


@contextlib.contextmanager
def _lock(root: Path):
    fd = os.open(root / "prediction-internal-paper.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    stream = os.fdopen(fd, "a+")
    try:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB); yield
    except BlockingIOError as exc:
        raise PredictionInternalPaperError("already_running") from exc
    finally: stream.close()


def _owned(book: PaperLedger) -> tuple[list[Any], list[Any]]:
    reservations, positions = [], []
    for audit in book.audit():
        payload = audit.payload.get("payload", {}) if audit.event_type == "reserve" else {}
        if not isinstance(payload, Mapping) or payload.get("metadata", {}).get(MARKER, {}).get("version") != VERSION: continue
        reservation = book.get_reservation(audit.entity_id)
        if reservation.state == "reserved": reservations.append(reservation)
        for filled in book.audit():
            if filled.event_type == "fill" and filled.payload.get("reservation_id") == reservation.reservation_id:
                position = book.get_position(filled.payload["position_id"])
                if position.state == "open": positions.append(position)
    return reservations, positions


def _proposal_payload(book: PaperLedger, proposal_id: str) -> Mapping[str, Any]:
    for audit in book.audit():
        payload = audit.payload.get("payload", {})
        if audit.event_type == "reserve" and isinstance(payload, Mapping) and payload.get("proposal_id") == proposal_id: return payload
    raise PredictionInternalPaperError("owned proposal is absent from audit")


def _assert_book(book: PaperLedger) -> None:
    try:
        assert_full_port_book(book)
    except PredictionFullPortError as exc:
        raise PredictionInternalPaperError(str(exc)) from exc


def _marks(book: PaperLedger, now: datetime, provider: Callable[[PaperLedger, datetime], Any] | None, own: MarketMark | None = None) -> None:
    if provider is None: raise PredictionInternalPaperError("complete global marks are required")
    try: marks = list(provider(book, now))
    except Exception as exc: raise PredictionInternalPaperError("global marks are unavailable") from exc
    if own is not None: marks.append(own)
    try: book.record_marks("prediction-contract-marks-" + _digest({"at": _iso(now), "marks": [repr(x) for x in marks]})[:32], marks, as_of=now)
    except LedgerError as exc: raise PredictionInternalPaperError("complete global marks are required") from exc


def _quote(value: Mapping[str, Any], marker: Mapping[str, Any], now: datetime, role: str) -> dict[str, Any]:
    if not isinstance(value, Mapping): raise PredictionInternalPaperError(f"{role} quote is required")
    for field in ("market_id", "outcome_id", "outcome", "fee_version"):
        if value.get(field) != marker.get(field): raise PredictionInternalPaperError(f"{role} quote identity differs from candidate")
    event_at, received_at = _time(value.get("event_at"), f"{role}.event_at"), _time(value.get("received_at"), f"{role}.received_at")
    if event_at > received_at or received_at > now or (now - received_at).total_seconds() > MAX_AGE_SECONDS: raise PredictionInternalPaperError(f"{role} quote is stale or causally invalid")
    source_hash = _hash(value.get("source_hash"), f"{role}.source_hash")
    ask, bid = _walk(value.get("asks"), marker["units"], f"{role}.asks")[0], _walk(value.get("bids"), marker["units"], f"{role}.bids")[1]
    return {"event_at": _iso(event_at), "received_at": _iso(received_at), "source_hash": source_hash, "ask_cents": ask, "bid_cents": bid}


def run(book: PaperLedger, state_dir: str | os.PathLike[str], *, now: datetime | str, candidate: Mapping[str, Any] | None = None,
        later_quote: Mapping[str, Any] | None = None, settlement: Mapping[str, Any] | None = None,
        request_close: bool = False, mark_provider: Callable[[PaperLedger, datetime], Any] | None = None,
        allow_entry: bool = True, recovery_ok: bool = True) -> dict[str, Any]:
    """Advance one internal-paper mutation; every external count remains zero."""
    current = _time(now, "now"); root = _root(state_dir); path = root / "prediction_internal_state.json"
    with _lock(root):
        state = _load(path); reservations, positions = _owned(book)
        if reservations and (not allow_entry or not recovery_ok or book.risk_stop_active()):
            result = book.cancel(reservations[0].reservation_id, reason="STOP or recovery entry pause", cancelled_at=current)
            state["history"].append({"type": "cancel", "result": asdict(result)}); _write(path, state)
            return {"actions": [{"type": "cancel", "result": asdict(result)}], "blocker": "entries_disabled", "broker_orders_sent": 0}
        if reservations:
            reservation = reservations[0]
            if _time(reservation.expiry_at, "reservation expiry") <= current:
                result = book.cancel(reservation.reservation_id, reason="prediction intent expired", cancelled_at=current)
                state["history"].append({"type": "cancel", "result": asdict(result)}); _write(path, state)
                return {"actions": [{"type": "cancel", "result": asdict(result)}], "blocker": "intent_expired", "broker_orders_sent": 0}
            if later_quote is None: return {"actions": [], "blocker": "awaiting_independent_fill_quote", "broker_orders_sent": 0}
            payload = _proposal_payload(book, reservation.proposal_id); marker = payload["metadata"][MARKER]
            quote = _quote(later_quote, marker, current, "fill")
            if quote["source_hash"] == marker["entry_quote_hash"] or _time(quote["received_at"], "fill receipt") < _time(reservation.created_at, "reservation created") + timedelta(seconds=FILL_DELAY_SECONDS):
                return {"actions": [], "blocker": "awaiting_independent_fill_quote", "broker_orders_sent": 0}
            actual = quote["ask_cents"] + marker["entry_fee_cents"]
            if actual > reservation.reserved_cents: raise PredictionInternalPaperError("fill cost exceeds reservation")
            _marks(book, current, mark_provider)
            fill = book.simulate_fill(reservation.reservation_id, fill_id="prediction-contract-fill-" + reservation.reservation_id,
                                      quantity=1, unit_cost_cents=quote["ask_cents"], fee_cents=marker["entry_fee_cents"], provenance="observed", filled_at=current)
            state["history"].append({"type": "fill", "result": asdict(fill), "quote": quote}); _write(path, state)
            return {"actions": [{"type": "simulated_fill", "result": asdict(fill)}], "blocker": None, "broker_orders_sent": 0}
        if positions:
            position = positions[0]; payload = _proposal_payload(book, position.proposal_id); marker = payload["metadata"][MARKER]
            if settlement is not None:
                if not isinstance(settlement, Mapping) or settlement.get("market_id") != marker["market_id"] or settlement.get("terms_sha256") != marker["terms_sha256"] or settlement.get("finalized") is not True:
                    raise PredictionInternalPaperError("final settlement identity is invalid")
                receipt, settled = _time(settlement.get("received_at"), "settlement.received_at"), _time(settlement.get("settlement_at"), "settlement.settlement_at")
                if settled < _time(marker["cutoff_at"], "cutoff") or settled > receipt or receipt > current: raise PredictionInternalPaperError("settlement timing is invalid")
                _hash(settlement.get("source_hash"), "settlement.source_hash")
                result = str(settlement.get("result", "")).lower()
                if result not in {"yes", "no"} or settlement.get("yes_payout_cents") not in {0, 100}: raise PredictionInternalPaperError("final binary settlement is invalid")
                if (result == "yes") != (settlement["yes_payout_cents"] == 100): raise PredictionInternalPaperError("settlement payout conflicts with result")
                proceeds = marker["units"] * (100 if result == marker["outcome"] else 0)
                if marker["settlement_fee_cents"] > proceeds:
                    raise PredictionInternalPaperError("settlement fee liability requires reconciliation")
                close = book.settle(position.position_id, close_id="prediction-contract-settle-" + position.position_id, proceeds_cents=proceeds, fee_cents=marker["settlement_fee_cents"], provenance="observed", closed_at=current)
                state["history"].append({"type": "settlement", "result": asdict(close)}); _write(path, state)
                return {"actions": [{"type": "settlement", "result": asdict(close)}], "blocker": None, "broker_orders_sent": 0}
            if later_quote is None: return {"actions": [], "blocker": "open_position_requires_mark_or_settlement", "broker_orders_sent": 0}
            quote = _quote(later_quote, marker, current, "exit")
            value = quote["bid_cents"] - marker["exit_fee_cents"]
            if value < 0: raise PredictionInternalPaperError("exit liability requires reconciliation")
            own = MarketMark(position.position_id, value, quote["received_at"], quote["source_hash"], "observed")
            _marks(book, current, mark_provider, own)
            intent = state.get("close_intent")
            if request_close and not isinstance(intent, Mapping):
                state["close_intent"] = {"position_id": position.position_id, "source_hash": quote["source_hash"], "created_at": _iso(current)}; _write(path, state)
                return {"actions": [{"type": "pending_close"}], "blocker": "awaiting_independent_exit_quote", "broker_orders_sent": 0}
            if isinstance(intent, Mapping):
                if intent.get("position_id") != position.position_id or quote["source_hash"] == intent.get("source_hash") or _time(quote["received_at"], "exit receipt") < _time(intent["created_at"], "close intent") + timedelta(seconds=FILL_DELAY_SECONDS):
                    return {"actions": [{"type": "mark"}], "blocker": "awaiting_independent_exit_quote", "broker_orders_sent": 0}
                close = book.close(position.position_id, close_id="prediction-contract-close-" + position.position_id, proceeds_cents=quote["bid_cents"], fee_cents=marker["exit_fee_cents"], provenance="observed", closed_at=current)
                state.pop("close_intent", None); state["history"].append({"type": "close", "result": asdict(close)}); _write(path, state)
                return {"actions": [{"type": "close", "result": asdict(close)}], "blocker": None, "broker_orders_sent": 0}
            return {"actions": [{"type": "mark"}], "blocker": None, "broker_orders_sent": 0}
        if not allow_entry or not recovery_ok or book.risk_stop_active(): return {"actions": [], "blocker": "entries_disabled", "broker_orders_sent": 0}
        if candidate is None: return {"actions": [], "blocker": "candidate_missing", "broker_orders_sent": 0}
        clean = prepare(candidate, now=current); _assert_book(book)
        proposal_id = "prediction-contract:" + clean["candidate_sha256"][:32]
        entry_evidence_hash = clean["candidate"].get("evidence_hashes", {}).get("adapter")
        marker = {"version": VERSION, "proposal_id": proposal_id,
                  "entry_quote_hash": _hash(entry_evidence_hash, "candidate entry adapter hash"),
                  **{k: clean[k] for k in ("market_id", "outcome_id", "outcome", "terms_sha256", "units", "entry_fee_cents", "exit_fee_cents", "settlement_fee_cents", "fee_version", "cutoff_at")}}
        proposal = Proposal(proposal_id, "prediction", "observed-prediction-contract", VERSION, clean["candidate_sha256"], clean["decision_at"], clean["outcome_id"], "prediction:" + clean["market_id"], "prediction:" + clean["market_id"], min(_time(clean["cutoff_at"], "cutoff"), current + timedelta(seconds=INTENT_SECONDS)), 1, clean["ask_cents"], clean["entry_fee_cents"], "observed", {MARKER: marker})
        _marks(book, current, mark_provider)
        reservation = book.reserve(proposal, now=current)
        state["history"].append({"type": "reserve", "result": asdict(reservation), "candidate_sha256": clean["candidate_sha256"]}); _write(path, state)
        return {"actions": [{"type": "reserve", "result": asdict(reservation)}], "blocker": "awaiting_independent_fill_quote", "broker_orders_sent": 0}


__all__ = ["PredictionInternalPaperError", "prepare", "run"]
