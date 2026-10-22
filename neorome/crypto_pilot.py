"""Finite observed BTC/USD internal-paper pilot.

The pilot consumes a completed CR-001 daily evidence file and the latest
observed BTC quote already recorded by the recorder.  It uses the one shared
paper ledger and never has a broker, network, credential, or order-submission
path.  A proposal reserves a fully funded fractional lot first; an independent
later quote is required before the lot can be simulated as filled.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
import fcntl
import hashlib
import json
import os
from pathlib import Path
import secrets
from typing import Any, Mapping

from desks.crypto.trend_policy import CostAssumptions, POLICY_ID, POLICY_VERSION, evaluate, validate_bars

from .crypto_daily import _quote_from_observation
from .paper_ledger import (
    CloseResult,
    Fill,
    IdempotencyConflict,
    InvalidState,
    LedgerError,
    MarketMark,
    PaperLedger,
    Position,
    Proposal,
    Reservation,
    RiskLimitExceeded,
)


UTC = timezone.utc
SYMBOL = "BTC/USD"
DEFAULT_STATE = "/var/lib/neorome-crypto-pilot"
DEFAULT_DAILY_STATE = "/var/lib/neorome-crypto-daily"
DEFAULT_OBSERVATION_STATE = "/var/lib/neorome-crypto-recorder"
DEFAULT_LEDGER = "/var/lib/neorome-paper/paper.sqlite3"
PILOT_METADATA = "crypto_pilot_v1"
DAILY_IDEA = "CR-001:BTC/USD"
PENDING_EXPIRY_SECONDS = 60
FILL_DELAY_SECONDS = 10
IDEA_CAP_CENTS = 500
FIRM_AGGREGATE_CAP_CENTS = 8_000
FIRM_CRYPTO_CAP_CENTS = 500
FIRM_GROUP_CAP_CENTS = 3_000
COST_PROFILE = {
    "id": "cr-001-scenario-v1",
    "fee_bps_per_side": 25,
    "slippage_bps_per_side": 5,
    "verified_actual_fees": False,
}


class PilotError(ValueError):
    """A fail-closed pilot validation or lifecycle error."""


class UnsupportedPosition(PilotError):
    """An open shared-ledger position cannot be valued by this pilot."""


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _time(value: Any, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise PilotError(f"{field} must be an ISO-8601 timestamp") from exc
    else:
        raise PilotError(f"{field} must be an aware timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PilotError(f"{field} must include a timezone")
    return parsed.astimezone(UTC)


def _decimal(value: Any, field: str, *, positive: bool = False) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise PilotError(f"{field} must be numeric")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise PilotError(f"{field} must be numeric") from exc
    if not result.is_finite() or (positive and result <= 0):
        raise PilotError(f"{field} must be finite and positive")
    return result


def _sha(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    return hashlib.sha256(encoded).hexdigest()


def _digest(value: Any, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdefABCDEF" for c in value):
        raise PilotError(f"{field} must be a 64-character hexadecimal digest")
    return value.lower()


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    if path.exists() and path.is_symlink():
        raise PilotError("pilot state symlink refused")
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(fd, "wb") as handle:
            fd = -1
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if fd >= 0:
            os.close(fd)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _load_json(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise PilotError("pilot input must be a regular file")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PilotError("pilot input JSON is unreadable") from exc
    if not isinstance(value, dict):
        raise PilotError("pilot input JSON must be an object")
    return value


def _ensure_state(path: str | os.PathLike[str]) -> Path:
    root = Path(path)
    if root.exists() and root.is_symlink():
        raise PilotError("pilot state directory symlink refused")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if root.is_symlink() or not root.is_dir():
        raise PilotError("pilot state directory is not a directory")
    os.chmod(root, 0o700)
    return root


def _entry_cost(units: Decimal, ask: Decimal) -> tuple[int, int, int]:
    adjusted = ask * (Decimal(1) + Decimal("0.0005"))
    gross = int((adjusted * units * 100).to_integral_value(rounding=ROUND_CEILING))
    fee = int((Decimal(gross) * Decimal("0.0025")).to_integral_value(rounding=ROUND_CEILING))
    return gross, fee, gross + fee


def _exit_value(units: Decimal, bid: Decimal) -> tuple[int, int, int]:
    adjusted = bid * (Decimal(1) - Decimal("0.0005"))
    gross = int((adjusted * units * 100).to_integral_value(rounding=ROUND_FLOOR))
    fee = int((Decimal(gross) * Decimal("0.0025")).to_integral_value(rounding=ROUND_CEILING))
    return max(0, gross), fee, max(0, gross - fee)


def _read_evidence(value: Any, now: datetime) -> tuple[dict[str, Any], str]:
    if isinstance(value, Mapping):
        evidence = dict(value)
        digest = _sha(evidence)
    else:
        path = Path(value) if value is not None else Path(DEFAULT_DAILY_STATE) / f"decision-{now.date().isoformat()}.json"
        if path.is_symlink():
            raise PilotError("daily evidence file is unsafe")
        if not path.exists():
            raise PilotError("daily evidence file is missing")
        if not path.is_file():
            raise PilotError("daily evidence file is unsafe")
        raw = path.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        try:
            evidence = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PilotError("daily evidence JSON is unreadable") from exc
        if not isinstance(evidence, dict):
            raise PilotError("daily evidence must be an object")
    if evidence.get("source_kind") != "observed" or evidence.get("execution_enabled") is not False:
        raise PilotError("only observed, execution-disabled daily evidence is accepted")
    if evidence.get("policy_id") != POLICY_ID or evidence.get("policy_version") != POLICY_VERSION:
        raise PilotError("daily evidence policy identity is unsupported")
    completed_at = evidence.get("completed_at", evidence.get("evaluation_at"))
    completed_dt = _time(completed_at, "daily evidence completed_at") if completed_at is not None else None
    if completed_dt is None or completed_dt.date() != now.date() or completed_dt > now:
        raise PilotError("daily evidence is not from the current UTC day")
    if evidence.get("ledger_mutations") != 0 or evidence.get("orders_sent") != 0 or evidence.get("profit_claim") is not False:
        raise PilotError("daily evidence indicates a mutation or unsupported claim")
    bars = evidence.get("bars")
    if not isinstance(bars, list):
        raise PilotError("daily evidence bars are missing")
    try:
        validated = validate_bars(bars, now=now, minimum_days=101)
    except Exception as exc:
        raise PilotError("daily evidence history is stale or malformed") from exc
    bars_hash = _sha(list(validated))
    bars_source = evidence.get("bars_source")
    if not isinstance(bars_source, Mapping) or bars_source.get("source_kind") != "observed":
        raise PilotError("daily bars provenance is not observed")
    declared_bars_hash = bars_source.get("sha256", evidence.get("bars_hash"))
    if declared_bars_hash is not None and _digest(declared_bars_hash, "bars hash") != bars_hash:
        raise PilotError("daily bars hash does not match evidence")
    return {**evidence, "bars": list(validated), "bars_hash": bars_hash}, digest


def _quote(
    value: Mapping[str, Any] | None,
    source: Mapping[str, Any] | None,
    *,
    now: datetime,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not isinstance(value, Mapping) or not isinstance(source, Mapping):
        raise PilotError("an observed quote and source hash are required")
    if source.get("source_kind") != "observed":
        raise PilotError("synthetic quote provenance is refused")
    source_hash = _digest(source.get("sha256", source.get("hash")), "quote source hash")
    timestamp = _time(value.get("timestamp"), "quote.timestamp")
    age = (now - timestamp).total_seconds()
    if age < 0 or age > 60:
        raise PilotError("quote is future-dated or older than 60 seconds")
    bid = _decimal(value.get("bid"), "quote.bid", positive=True)
    ask = _decimal(value.get("ask"), "quote.ask", positive=True)
    depth = _decimal(value.get("depth_units"), "quote.depth_units", positive=True)
    if value.get("symbol") != SYMBOL or bid > ask:
        raise PilotError("quote symbol or spread is invalid")
    return {
        "symbol": SYMBOL,
        "timestamp": _iso(timestamp),
        "bid": str(bid),
        "ask": str(ask),
        "depth_units": str(depth),
    }, {"source_kind": "observed", "sha256": source_hash, "age_seconds": age}


def _latest_quote(
    observation_state: str,
    now: datetime,
    injected_quote: Mapping[str, Any] | None,
    injected_source: Mapping[str, Any] | None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str | None]:
    if injected_quote is not None or injected_source is not None:
        try:
            quote, source = _quote(injected_quote, injected_source, now=now)
        except PilotError as exc:
            if "future-dated" in str(exc) or "older than 60 seconds" in str(exc):
                return None, None, "stale"
            raise
        return quote, source, None
    quote, status, source = _quote_from_observation(observation_state, now)
    if quote is None:
        return None, None, status
    source_map = dict(source)
    source_hash = source_map.get("sha256")
    if source_hash is None:
        raise PilotError("latest quote has no observed checksum")
    clean, clean_source = _quote(quote, source_map, now=now)
    return clean, clean_source, None


def _ledger_records(book: PaperLedger) -> tuple[list[tuple[Reservation, Mapping[str, Any]]], list[tuple[Position, Mapping[str, Any]]], list[Position]]:
    events = book.audit()
    proposals: dict[str, Mapping[str, Any]] = {}
    for event in events:
        if event.event_type != "reserve":
            continue
        payload = event.payload.get("payload", {})
        if isinstance(payload, Mapping):
            proposals[payload.get("proposal_id")] = payload
    pending: list[tuple[Reservation, Mapping[str, Any]]] = []
    positions: list[tuple[Position, Mapping[str, Any]]] = []
    unsupported: list[Position] = []
    seen_positions: set[str] = set()
    for event in events:
        if event.event_type == "reserve":
            payload = event.payload.get("payload", {})
            if not isinstance(payload, Mapping):
                continue
            metadata = payload.get("metadata", {})
            marker = metadata.get(PILOT_METADATA) if isinstance(metadata, Mapping) else None
            if not isinstance(marker, Mapping):
                continue
            reservation = book.get_reservation(event.entity_id)
            if reservation.state == "reserved":
                pending.append((reservation, payload))
        elif event.event_type == "fill":
            position_id = event.payload.get("position_id")
            if not isinstance(position_id, str):
                continue
            seen_positions.add(position_id)
            position = book.get_position(position_id)
            if position.state != "open":
                continue
            proposal = proposals.get(position.proposal_id)
            marker = None
            if isinstance(proposal, Mapping):
                metadata = proposal.get("metadata", {})
                marker = metadata.get(PILOT_METADATA) if isinstance(metadata, Mapping) else None
            if isinstance(marker, Mapping) and position.desk == "crypto" and position.instrument == SYMBOL:
                positions.append((position, proposal))
            else:
                unsupported.append(position)
    # A position not represented by a fill event is an integrity failure; the
    # ledger itself normally prevents this, but the pilot must fail closed.
    for event in events:
        if event.event_type == "fill" and isinstance(event.payload.get("position_id"), str) and event.payload["position_id"] not in seen_positions:
            raise PilotError("open position provenance is unresolved")
    return pending, positions, unsupported


def _check_firm_caps(book: PaperLedger) -> None:
    if not book.enforce_market_risk:
        raise PilotError("shared paper ledger must enforce market risk")
    limits = book.limits
    if limits.aggregate_cap_cents != FIRM_AGGREGATE_CAP_CENTS or limits.group_cap_cents != FIRM_GROUP_CAP_CENTS:
        raise PilotError("shared paper aggregate or group cap differs from firm cap")
    if limits.per_idea_cents.get("crypto") != FIRM_CRYPTO_CAP_CENTS:
        raise PilotError("shared paper crypto idea cap differs from firm cap")
    balances = {balance.desk: balance.initial_cash_cents for balance in book.balances()}
    if balances != {"prediction": 10_000, "equities": 30_000, "options": 30_000, "crypto": 10_000}:
        raise PilotError("shared paper balances differ from the $800 firm allocation")


def _reconcile(book: PaperLedger) -> dict[str, Any]:
    balances = [
        {
            "desk": balance.desk,
            "initial_cash_cents": balance.initial_cash_cents,
            "available_cash_cents": balance.available_cash_cents,
            "reserved_cash_cents": balance.reserved_cash_cents,
            "open_position_cost_cents": balance.open_position_cost_cents,
        }
        for balance in book.balances()
    ]
    if any(min(row["available_cash_cents"], row["reserved_cash_cents"], row["open_position_cost_cents"]) < 0 for row in balances):
        raise PilotError("negative shared-ledger cash balance")
    snapshot = book.snapshot()
    return {"reconciled": True, "paper_nav_cents": snapshot.paper_nav_cents, "balances": balances}


def _marker(position: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = position.get("metadata", {}) if isinstance(position, Mapping) else {}
    value = metadata.get(PILOT_METADATA) if isinstance(metadata, Mapping) else None
    if not isinstance(value, Mapping):
        raise UnsupportedPosition("BTC position metadata is missing")
    if value.get("symbol") != SYMBOL or value.get("daily_idea") != DAILY_IDEA:
        raise UnsupportedPosition("position is not owned by CR-001 BTC pilot")
    _decimal(value.get("units"), "position units", positive=True)
    return value


def _position_units(proposal: Mapping[str, Any]) -> Decimal:
    return _decimal(_marker(proposal).get("units"), "position units", positive=True)


def _batch_id(prefix: str, now: datetime, source_hash: str | None = None) -> str:
    stamp = now.strftime("%Y%m%dT%H%M%S%fZ")
    return f"{prefix}-{stamp}-{(source_hash or 'empty')[:20]}"


def _quote_evidence(
    quote: Mapping[str, Any], source: Mapping[str, Any], *, role: str, units: Decimal, gross: int, fee: int
) -> dict[str, Any]:
    return {
        "role": role,
        "source_kind": "observed",
        "source_hash": source["sha256"],
        "timestamp": quote["timestamp"],
        "bid": quote["bid"],
        "ask": quote["ask"],
        "depth_units": quote["depth_units"],
        "units": str(units),
        "gross_cents": gross,
        "fee_cents": fee,
    }


def _report(
    *,
    now: datetime,
    actions: list[dict[str, Any]],
    blocker: str | None,
    decision: Mapping[str, Any] | None,
    reconciliation: Mapping[str, Any] | None,
    market_snapshot: Mapping[str, Any] | None,
    state_path: Path,
    ledger_path: str,
) -> dict[str, Any]:
    return {
        "status": "blocked" if blocker else "ok",
        "mode": "observed_internal_crypto_pilot",
        "source_kind": "observed",
        "evaluated_at": _iso(now),
        "policy_id": POLICY_ID,
        "policy_version": POLICY_VERSION,
        "cost_profile": COST_PROFILE,
        "actions": actions,
        "decision": dict(decision) if isinstance(decision, Mapping) else None,
        "blocker": blocker,
        "cash_reconciliation": dict(reconciliation) if reconciliation is not None else None,
        "market_snapshot": dict(market_snapshot) if market_snapshot is not None else None,
        "ledger_path": ledger_path,
        "state_path": str(state_path),
        "broker_orders_sent": 0,
        "broker_execution_enabled": False,
        "profitability_validated": False,
    }


def run(
    daily_evidence_path: Any = None,
    state_dir: str | os.PathLike[str] = DEFAULT_STATE,
    now: datetime | None = None,
    *,
    ledger_path: str | os.PathLike[str] = DEFAULT_LEDGER,
    observation_state: str = DEFAULT_OBSERVATION_STATE,
    daily_evidence: Mapping[str, Any] | None = None,
    quote: Mapping[str, Any] | None = None,
    quote_source: Mapping[str, Any] | None = None,
    allow_entry: bool = True,
    mark_provider: Any = None,
) -> dict[str, Any]:
    """Advance one resumable pilot cycle using explicit injectable inputs."""
    current = _time(now or datetime.now(UTC), "now")
    if type(allow_entry) is not bool:
        raise PilotError("allow_entry must be boolean")
    evidence_value = daily_evidence if daily_evidence is not None else daily_evidence_path
    root = _ensure_state(state_dir)
    lock_path = root / "pilot.lock"
    if lock_path.exists() and lock_path.is_symlink():
        raise PilotError("pilot lock symlink refused")
    with lock_path.open("a+") as lock:
        os.chmod(lock_path, 0o600)
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return _report(now=current, actions=[], blocker="already_running", decision=None, reconciliation=None, market_snapshot=None, state_path=root / "state.json", ledger_path=str(ledger_path))
        state_path = root / "state.json"
        state = _load_json(state_path) if state_path.exists() else {}
        book = PaperLedger(ledger_path, enforce_market_risk=True, clock=lambda: current)
        _check_firm_caps(book)
        reconciliation = _reconcile(book)
        pending, own_positions, unsupported = _ledger_records(book)
        pending_close_evidence = state.get("pending_close_evidence")
        if isinstance(pending_close_evidence, Mapping) and not own_positions:
            close_id = pending_close_evidence.get("close_id")
            if any(event.event_type == "close" and event.entity_id == close_id for event in book.audit()):
                state.setdefault("close_evidence", []).append(dict(pending_close_evidence))
                state.pop("pending_close_evidence", None)
        if len(pending) > 1 or len(own_positions) > 1:
            raise PilotError("multiple active CR-001 BTC lifecycle records")
        actions: list[dict[str, Any]] = []
        blocker: str | None = None
        decision: Mapping[str, Any] | None = None
        evidence: dict[str, Any] | None = None
        evidence_hash: str | None = None
        global_mark_error: str | None = None
        q: dict[str, Any] | None = None
        qsource: dict[str, Any] | None = None
        qstatus: str | None = None

        def load_evidence() -> tuple[dict[str, Any], str]:
            nonlocal evidence, evidence_hash
            if evidence is None or evidence_hash is None:
                evidence, evidence_hash = _read_evidence(evidence_value, current)
            return evidence, evidence_hash

        def record_global_marks(prefix, fallback):
            marks = mark_provider(book, current) if mark_provider is not None else fallback
            if unsupported and mark_provider is None:
                raise UnsupportedPosition("unsupported open position blocks complete global marks")
            return book.record_marks(_batch_id(prefix, current, (qsource or {}).get("sha256")), marks, as_of=current)

        try:
            if pending and (not allow_entry or book.risk_stop_active()):
                reservation, _payload = pending[0]
                cancelled = book.cancel(reservation.reservation_id, reason="crypto pilot risk stop", cancelled_at=current)
                actions.append({"type": "cancel_risk_stop", "result": {"reservation_id": cancelled.reservation_id, "state": cancelled.state}})
                state["entry_attempt_date"] = current.date().isoformat()
                pending = []
            # Expiry is a durable ledger fact and must be resolved before
            # today's evidence is read.  A missing or malformed new-day file
            # must not strand yesterday's pending reservation.
            if pending and _time(pending[0][0].expiry_at, "reservation.expiry_at") <= current:
                reservation, _payload = pending[0]
                cancelled = book.cancel(reservation.reservation_id, reason="crypto pilot pending fill expired", cancelled_at=current)
                actions.append({"type": "cancel_expired_reservation", "result": {"reservation_id": cancelled.reservation_id, "state": cancelled.state}})
                state["entry_attempt_date"] = current.date().isoformat()
                pending = []
            q, qsource, qstatus = _latest_quote(observation_state, current, quote, quote_source)
            if pending:
                reservation, payload = pending[0]
                if q is not None and qsource is not None:
                    entry_marker = payload.get("metadata", {}).get(PILOT_METADATA, {})
                    entry_quote_time = _time(entry_marker.get("entry_quote_timestamp"), "entry quote timestamp")
                    quote_time = _time(q["timestamp"], "quote.timestamp")
                    units = _decimal(entry_marker.get("units"), "reserved units", positive=True)
                    gross, fee, total = _entry_cost(units, _decimal(q["ask"], "quote.ask", positive=True))
                    if quote_time <= entry_quote_time or quote_time < _time(reservation.created_at, "reservation.created_at") + timedelta(seconds=FILL_DELAY_SECONDS):
                        blocker = "awaiting_independent_post_reservation_quote"
                    elif _decimal(q["depth_units"], "quote.depth_units", positive=True) < units:
                        blocker = "insufficient_fill_depth"
                    elif total > reservation.reserved_cents:
                        blocker = "post_reservation_cost_exceeds_reserved_cash"
                    else:
                        # Revalue the entire shared book before taking new exposure.
                        if mark_provider is not None or unsupported:
                            snapshot = record_global_marks("crypto-pilot-prefill", [])
                            if not snapshot.entry_allowed:
                                raise PilotError("global market risk blocks fill")
                        state["pending_fill_quote"] = {"timestamp": q["timestamp"], "hash": qsource["sha256"]}
                        fill_id = f"crypto-pilot-fill-{reservation.reservation_id}"
                        fill_evidence = _quote_evidence(q, qsource, role="arrival_fill", units=units, gross=gross, fee=fee)
                        fill_evidence["fill_id"] = fill_id
                        fill_evidence["reservation_id"] = reservation.reservation_id
                        state["pending_fill_quote"] = fill_evidence
                        _atomic_json(state_path, state)
                        fill = book.simulate_fill(
                            reservation.reservation_id,
                            fill_id=fill_id,
                            quantity=1,
                            unit_cost_cents=gross,
                            fee_cents=fee,
                            provenance="observed",
                            filled_at=current,
                        )
                        state.setdefault("fill_evidence", []).append(fill_evidence)
                        actions.append({"type": "simulated_fill", "result": _asdict(fill), "arrival_quote": fill_evidence})
                        state.pop("pending_fill_quote", None)
                        state.update({"entry_attempt_date": _time(reservation.created_at, "reservation.created_at").date().isoformat(), "last_quote_timestamp": q["timestamp"], "last_quote_hash": qsource["sha256"], "last_action": "fill"})
                else:
                    blocker = f"quote_{qstatus or 'unavailable'}"
            elif own_positions:
                position, proposal = own_positions[0]
                recovered_fill = state.get("pending_fill_quote")
                if isinstance(recovered_fill, Mapping) and recovered_fill.get("fill_id") == position.fill_id:
                    existing_fill_evidence = state.setdefault("fill_evidence", [])
                    if not any(isinstance(item, Mapping) and item.get("fill_id") == position.fill_id for item in existing_fill_evidence):
                        existing_fill_evidence.append(dict(recovered_fill))
                    state.pop("pending_fill_quote", None)
                if q is None or qsource is None:
                    blocker = f"quote_{qstatus or 'unavailable'}"
                else:
                    units = _position_units(proposal)
                    if _decimal(q["depth_units"], "quote.depth_units", positive=True) < units:
                        blocker = "insufficient_mark_depth"
                    elif _time(q["timestamp"], "quote.timestamp") < _time(position.opened_at, "position.opened_at"):
                        blocker = "quote_precedes_position"
                    else:
                        gross, fee, net = _exit_value(units, _decimal(q["bid"], "quote.bid", positive=True))
                        marks = [MarketMark(position.position_id, net, q["timestamp"], qsource["sha256"], "observed")]
                        try:
                            record_global_marks("crypto-pilot-mark", marks)
                        except (LedgerError, ValueError) as exc:
                            # Foreign stale/unavailable marks block entries, not
                            # a validated reduction of this position's exposure.
                            blocker = "global_marks_unavailable"
                            global_mark_error = f"{type(exc).__name__}: {exc}"
                        
                        current_evidence, _ = load_evidence()
                        decision = evaluate(current_evidence["bars"], q, now=current, symbol=SYMBOL, position_units=units, costs=CostAssumptions(fee_bps=25, slippage_bps=5, idea_cap_cents=IDEA_CAP_CENTS))
                        if decision.get("status") == "exit_candidate":
                            pending_close = state.get("pending_close")
                            if not isinstance(pending_close, Mapping) or pending_close.get("position_id") != position.position_id:
                                state["pending_close"] = {
                                    "position_id": position.position_id,
                                    "target": "exit",
                                    "requested_at": _iso(current),
                                    "target_quote_timestamp": q["timestamp"],
                                }
                                blocker = "exit_target_pending"
                            else:
                                requested_at = _time(pending_close.get("requested_at"), "pending close requested_at")
                                target_quote = _time(pending_close.get("target_quote_timestamp"), "pending close quote")
                                quote_time = _time(q["timestamp"], "quote.timestamp")
                                if current < requested_at + timedelta(seconds=FILL_DELAY_SECONDS) or quote_time < requested_at + timedelta(seconds=FILL_DELAY_SECONDS) or quote_time <= target_quote:
                                    blocker = "awaiting_independent_exit_quote"
                                else:
                                    close_id = f"crypto-pilot-close-{position.position_id}"
                                    close_evidence = _quote_evidence(q, qsource, role="exit_close", units=units, gross=gross, fee=fee)
                                    close_evidence["close_id"] = close_id
                                    close_evidence["position_id"] = position.position_id
                                    state["pending_close_evidence"] = close_evidence
                                    _atomic_json(state_path, state)
                                    closed = book.close(position.position_id, close_id=close_id, proceeds_cents=gross, fee_cents=fee, provenance="observed", closed_at=current)
                                    state.setdefault("close_evidence", []).append(close_evidence)
                                    actions.append({"type": "simulated_close", "result": _asdict(closed), "exit_quote": close_evidence})
                                    state.pop("pending_close_evidence", None)
                                    state.pop("pending_close", None)
                                    state.update({"last_quote_timestamp": q["timestamp"], "last_quote_hash": qsource["sha256"], "last_action": "close"})
                                    record_global_marks("crypto-pilot-flat", [])
                        elif decision.get("status") == "abstain" and decision.get("reason") == "insufficient_exit_depth":
                            blocker = "insufficient_exit_depth"
            else:
                record_global_marks("crypto-pilot-empty", [])
                if q is None or qsource is None:
                    blocker = f"quote_{qstatus or 'unavailable'}"
                elif not allow_entry:
                    blocker = "entries_disabled"
                elif state.get("entry_attempt_date") == current.date().isoformat() or any(
                    event.event_type == "reserve" and event.payload.get("proposal_id") == f"crypto-pilot-{current.date().isoformat()}"
                    for event in book.audit()
                ):
                    blocker = "daily_entry_already_attempted"
                else:
                    current_evidence, current_evidence_hash = load_evidence()
                    decision = evaluate(current_evidence["bars"], q, now=current, symbol=SYMBOL, position_units=0, costs=CostAssumptions(fee_bps=25, slippage_bps=5, idea_cap_cents=IDEA_CAP_CENTS))
                    if decision.get("status") == "entry_candidate":
                        valuation = decision.get("valuation", {})
                        units = _decimal(valuation.get("units"), "entry units", positive=True)
                        gross, fee, total = _entry_cost(units, _decimal(q["ask"], "quote.ask", positive=True))
                        if total > IDEA_CAP_CENTS or units > _decimal(q["depth_units"], "quote.depth_units", positive=True):
                            blocker = "entry_size_exceeds_firm_cap_or_depth"
                        else:
                            proposal_id = f"crypto-pilot-{current.date().isoformat()}"
                            proposal = Proposal(
                                proposal_id=proposal_id,
                                desk="crypto",
                                strategy=POLICY_ID,
                                strategy_version=POLICY_VERSION,
                                source_hash=qsource["sha256"],
                                source_timestamp=q["timestamp"],
                                instrument=SYMBOL,
                                exposure_group="crypto:BTC/USD",
                                idea_id=DAILY_IDEA,
                                expiry_at=current + timedelta(seconds=PENDING_EXPIRY_SECONDS),
                                quantity=1,
                                unit_cost_cents=gross,
                                fee_cents=fee,
                                provenance="observed",
                                metadata={
                                    PILOT_METADATA: {
                                        "symbol": SYMBOL,
                                        "units": str(units),
                                        "daily_idea": DAILY_IDEA,
                                        "entry_quote_timestamp": q["timestamp"],
                                        "entry_quote_hash": qsource["sha256"],
                                        "bars_hash": current_evidence["bars_hash"],
                                        "evidence_hash": current_evidence_hash,
                                        "cost_profile": COST_PROFILE,
                                    }
                                },
                            )
                            reservation = book.reserve(proposal, now=current)
                            actions.append({"type": "reserve", "result": _asdict(reservation), "units": str(units), "total_reserved_cents": total})
                            state.update({"reservation_id": reservation.reservation_id, "entry_attempt_date": current.date().isoformat(), "last_action": "reserve"})
                    elif decision.get("status") == "abstain":
                        blocker = decision.get("reason")
            reconciliation = _reconcile(book)
        except (LedgerError, PilotError, ValueError) as exc:
            blocker = f"{type(exc).__name__}: {exc}"
        state.update({"schema_version": 1, "policy_id": POLICY_ID, "policy_version": POLICY_VERSION, "source_kind": "observed", "updated_at": _iso(current)})
        _atomic_json(state_path, state)
        market = None
        try:
            market = _asdict(book.market_snapshot(as_of=current))
        except LedgerError:
            pass
        report = _report(now=current, actions=actions, blocker=blocker, decision=decision, reconciliation=reconciliation, market_snapshot=market, state_path=state_path, ledger_path=str(ledger_path))
        report["global_mark_error"] = global_mark_error
        return report


def _asdict(value: Any) -> dict[str, Any]:
    from dataclasses import asdict

    return asdict(value)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--daily-evidence")
    parser.add_argument("--state-dir", default=DEFAULT_STATE)
    parser.add_argument("--ledger", default=DEFAULT_LEDGER)
    parser.add_argument("--observation-state", default=DEFAULT_OBSERVATION_STATE)
    args = parser.parse_args(argv)
    try:
        result = run(args.daily_evidence, args.state_dir, ledger_path=args.ledger, observation_state=args.observation_state)
    except Exception as exc:
        result = {"mode": "observed_internal_crypto_pilot", "status": "blocked", "blocker": f"{type(exc).__name__}: {exc}", "broker_orders_sent": 0, "broker_execution_enabled": False}
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0 if result.get("blocker") in (None, "already_running") else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["DEFAULT_DAILY_STATE", "DEFAULT_LEDGER", "DEFAULT_OBSERVATION_STATE", "DEFAULT_STATE", "run", "main"]
