"""Finite, observed internal-paper lifecycle for the EQ-INTRADAY experiment.

This module has no provider client, credentials, broker API, service loop, or
default ledger path.  It consumes an immutable decision record produced by the
desk's observed-shadow collector, reserves only in the shared ``PaperLedger``,
and records simulated fills from later retained bid/ask evidence.  Every result
explicitly reports ``broker_orders_sent: 0``.

It is intentionally a *finite* CLI.  A scheduler may invoke it only after a
separate admission and deployment review; this file does not install or enable
one.
"""

from __future__ import annotations

import argparse
import contextlib
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import secrets
import sqlite3
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from desks.equities.intraday_paper_adapter import (
    EntryGateBlocked,
    IntradayAdapterError,
    IntradayPaperAdapter,
)
from desks.equities.intraday_quote_replay import DEFAULT_LATENCY
from neorome.paper_ledger import MarketMark, PaperLedger, RiskStopActive


UTC = timezone.utc
NY = ZoneInfo("America/New_York")
STATE_SCHEMA = "eq-intraday-observed-paper-v1"
ORDER_LIFETIME = timedelta(seconds=60)
LOCK_NAME = "runtime.lock"
EXPECTED_BALANCES = {"prediction": 10_000, "equities": 30_000, "options": 30_000, "crypto": 10_000}
EXPECTED_LIMITS = {
    "aggregate_cap_cents": 8_000,
    "group_cap_cents": 3_000,
    "per_idea_cents": {"prediction": 500, "equities": 1_500, "options": 1_500, "crypto": 500},
}


class IntradayRuntimeError(ValueError):
    """Evidence, lifecycle state, or a local runtime path is unsafe."""


def _now(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise IntradayRuntimeError("now must be an ISO timestamp") from exc
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise IntradayRuntimeError("now must be timezone-aware")
    return value.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise IntradayRuntimeError(f"{label} must be an object")
    return value


def _hash(value: Any) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise IntradayRuntimeError("evidence digest must be lowercase SHA-256")
    return value


def _number(value: Any, label: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise IntradayRuntimeError(f"{label} must be finite numeric data") from exc
    if not parsed.is_finite():
        raise IntradayRuntimeError(f"{label} must be finite numeric data")
    return parsed


def _read_json(path: str | os.PathLike[str]) -> dict[str, Any]:
    target = Path(path)
    if target.is_symlink() or not target.is_file() or target.stat().st_size > 8_000_000:
        raise IntradayRuntimeError("input JSON must be a bounded regular file")
    try:
        value = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise IntradayRuntimeError("input JSON is unreadable") from exc
    return dict(_mapping(value, "input JSON"))


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    if path.exists() and path.is_symlink():
        raise IntradayRuntimeError("runtime state symlink refused")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path.parent, 0o700)
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _load_state(root: Path) -> dict[str, Any]:
    if root.exists() and root.is_symlink():
        raise IntradayRuntimeError("runtime state symlink refused")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if root.is_symlink() or not root.is_dir():
        raise IntradayRuntimeError("runtime state is not a directory")
    os.chmod(root, 0o700)
    path = root / "state.json"
    if not path.exists():
        return {"schema": STATE_SCHEMA, "candidates": {}}
    value = _read_json(path)
    if value.get("schema") != STATE_SCHEMA or not isinstance(value.get("candidates"), Mapping):
        raise IntradayRuntimeError("intraday runtime state schema is invalid")
    return value


def _require_shared_ledger(path: str | os.PathLike[str]) -> Path:
    """Refuse a missing or substituted path before ``PaperLedger`` can create it."""

    target = Path(path)
    if target.is_symlink() or not target.is_file():
        raise IntradayRuntimeError("shared paper ledger must be an existing regular file")
    try:
        with sqlite3.connect(target.resolve().as_uri() + "?mode=ro", uri=True) as connection:
            marker = connection.execute("SELECT value FROM settings WHERE key='ledger_marker'").fetchone()
            config = connection.execute("SELECT value FROM settings WHERE key='ledger_config'").fetchone()
    except (OSError, sqlite3.Error) as exc:
        raise IntradayRuntimeError("shared paper ledger is unreadable or invalid") from exc
    if marker is None or marker[0] != "neorome-paper-ledger-v1" or config is None:
        raise IntradayRuntimeError("shared paper ledger marker is invalid")
    try:
        parsed = json.loads(config[0])
    except (TypeError, json.JSONDecodeError) as exc:
        raise IntradayRuntimeError("shared paper ledger configuration is invalid") from exc
    if (parsed.get("balances") != EXPECTED_BALANCES or parsed.get("limits") != EXPECTED_LIMITS
            or parsed.get("enforce_market_risk") is not True):
        raise IntradayRuntimeError("shared paper ledger does not have required $300/$15/$30/$80 market-risk configuration")
    return target


@contextlib.contextmanager
def _runtime_lock(root: Path):
    """Acquire a nonblocking no-follow lock covering state and ledger advancement."""

    import fcntl

    lock_path = root / LOCK_NAME
    if lock_path.exists() and lock_path.is_symlink():
        raise IntradayRuntimeError("runtime lock symlink refused")
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as exc:
        raise IntradayRuntimeError("runtime lock cannot be opened safely") from exc
    try:
        os.chmod(lock_path, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        yield True
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _verify_archive(meta: Mapping[str, Any], label: str) -> None:
    digest = _hash(meta.get("digest"))
    path = meta.get("path")
    if not isinstance(path, str):
        raise IntradayRuntimeError(f"{label} archive path is missing")
    target = Path(path)
    if target.is_symlink() or not target.is_file():
        raise IntradayRuntimeError(f"{label} archive is unavailable")
    actual = hashlib.sha256(target.read_bytes()).hexdigest()
    if actual != digest:
        raise IntradayRuntimeError(f"{label} archive digest mismatch")


def validate_decision_record(record: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a retained prospective decision before it may reach the ledger."""

    value = _mapping(record, "decision record")
    if value.get("scope") != "observed_shadow_decision_only":
        raise IntradayRuntimeError("decision is not an observed-shadow record")
    if value.get("status") != "eligible_unfilled_candidate" or value.get("eligible_for_execution") is not False:
        raise IntradayRuntimeError("decision is not an eligible unfilled candidate")
    if value.get("broker_order_count") != 0:
        raise IntradayRuntimeError("decision broker order count must be zero")
    candidate = dict(_mapping(value.get("candidate"), "decision candidate"))
    prepared = _mapping(value.get("prepared"), "prepared reservation")
    collected = _mapping(value.get("collected"), "collected evidence")
    evidence = _mapping(collected.get("evidence"), "collected evidence hashes")
    archived = _mapping(collected.get("archived"), "collected archives")
    if prepared.get("accepted") is not True or prepared.get("candidate_id") != candidate.get("candidate_id"):
        raise IntradayRuntimeError("prepared reservation does not bind candidate")
    if candidate.get("strategy_id") not in {"orb15", "vwap_reclaim"}:
        raise IntradayRuntimeError("candidate strategy is not frozen EQ-INTRADAY")
    symbol = candidate.get("symbol")
    if not isinstance(symbol, str) or not symbol.strip() or prepared.get("instrument") != symbol.strip():
        raise IntradayRuntimeError("candidate instrument identity mismatch")
    for key in ("universe_evidence_sha256", "bar_prefix_sha256", "quote_evidence_sha256"):
        digest = _hash(candidate.get(key))
        if evidence.get(key) != digest:
            raise IntradayRuntimeError(f"candidate {key} differs from collected evidence")
    decoded: dict[str, Any] = {}
    for kind in ("universe", "calendar", "bar_prefix", "quotes"):
        meta = _mapping(archived.get(kind), f"{kind} archive")
        _verify_archive(meta, kind)
        decoded[kind] = _read_json(meta["path"])
    quotes = decoded["quotes"].get("payload")
    if not isinstance(quotes, list):
        raise IntradayRuntimeError("retained entry quote artifact has no quote list")
    try:
        signal = _now(candidate["signal_timestamp"])
        expiry = _now(prepared["order_expires_at"])
    except (KeyError, IntradayRuntimeError) as exc:
        raise IntradayRuntimeError("prepared reservation timing is invalid") from exc
    lifetime = expiry - signal - DEFAULT_LATENCY
    if lifetime <= timedelta(0) or lifetime > ORDER_LIFETIME:
        raise IntradayRuntimeError("prepared reservation lifetime is invalid or exceeds runtime bound")
    return {"candidate": candidate, "prepared": dict(prepared), "collected": dict(collected),
            "entry_quotes": quotes, "order_lifetime": lifetime}


def _ledger_candidate(book: PaperLedger, candidate_id: str) -> dict[str, str]:
    """Reconstruct lifecycle ids from the durable audit, never state.json alone."""

    proposal_id = f"eq-intraday:{candidate_id}"
    found: dict[str, str] = {}
    for event in book.audit():
        payload = event.payload
        if event.event_type == "reserve" and payload.get("proposal_id") == proposal_id:
            found["reservation_id"] = event.entity_id
        elif event.event_type == "fill" and event.entity_id.startswith("eq-intraday-fill:"):
            reservation = payload.get("reservation_id")
            if reservation == found.get("reservation_id"):
                found["position_id"] = str(payload.get("position_id"))
                found["fill_id"] = event.entity_id
        elif event.event_type == "close" and payload.get("position_id") == found.get("position_id"):
            found["close_id"] = event.entity_id
    return found


def _open_positions(book: PaperLedger) -> list[Any]:
    positions = []
    for event in book.audit():
        if event.event_type == "fill":
            position_id = event.payload.get("position_id")
            if isinstance(position_id, str):
                position = book.get_position(position_id)
                if position.state == "open":
                    positions.append(position)
    return positions


def _ensure_marks(book: PaperLedger, current: datetime, marks: Iterable[MarketMark] | None) -> str | None:
    """Require a complete mark set when another position is already open."""

    open_positions = _open_positions(book)
    provided = tuple(marks or ())
    if not open_positions and not provided:
        book.record_marks(f"eq-intraday-empty-{int(current.timestamp())}", [], current)
        return None
    ids = {mark.position_id for mark in provided}
    if ids != {position.position_id for position in open_positions}:
        return "global_marks_missing_or_incomplete"
    book.record_marks(f"eq-intraday-marks-{int(current.timestamp())}", list(provided), current)
    return None


def _quote_for_symbol(value: Mapping[str, Any] | None, symbol: str, label: str) -> Mapping[str, Any] | None:
    if value is None:
        return None
    quote = _mapping(value, label)
    if quote.get("symbol") != symbol:
        raise IntradayRuntimeError(f"{label} symbol does not match candidate")
    _hash(quote.get("source_sha256"))
    return quote


def run_cycle(
    state_dir: str | os.PathLike[str],
    ledger_path: str | os.PathLike[str],
    decision_record: Mapping[str, Any] | None,
    *,
    entry_fill_quote: Mapping[str, Any] | None = None,
    exit_quote: Mapping[str, Any] | None = None,
    marks: Iterable[MarketMark] | None = None,
    now: datetime | str | None = None,
    allow_entry: bool = True,
    recovery_ok: bool = True,
    exit_fee_cents_per_unit: int = 0,
) -> dict[str, Any]:
    """Advance at most one candidate through reserve, observed fill, or close.

    ``decision_record`` may be ``None`` for an honest finite abstention when
    provider evidence is unavailable.  No path in this function sends a broker
    order; all ledger mutations are ``provenance='observed'`` internal paper.
    """

    current = _now(now)
    if decision_record is None:
        return {"scope": "eq_intraday_observed_internal_paper", "status": "abstained",
                "blocker": "prospective_evidence_unavailable", "actions": [],
                "broker_orders_sent": 0, "broker_execution_enabled": False}
    validated = validate_decision_record(decision_record)
    candidate = validated["candidate"]
    candidate_id = str(candidate["candidate_id"])
    root = Path(state_dir)
    # Validate before constructing PaperLedger: its normal constructor creates
    # a new SQLite book for a missing path, which is never acceptable here.
    shared_ledger = _require_shared_ledger(ledger_path)
    _load_state(root)  # validates the root before opening its no-follow lock
    with _runtime_lock(root) as acquired:
        if not acquired:
            return {"scope": "eq_intraday_observed_internal_paper", "status": "already_running",
                    "candidate_id": candidate_id, "actions": [], "blocker": "runtime_lock_held",
                    "broker_orders_sent": 0, "broker_execution_enabled": False}
        return _run_cycle_locked(
            root, shared_ledger, decision_record, validated, current=current,
            entry_fill_quote=entry_fill_quote, exit_quote=exit_quote, marks=marks,
            allow_entry=allow_entry, recovery_ok=recovery_ok,
            exit_fee_cents_per_unit=exit_fee_cents_per_unit,
        )


def _run_cycle_locked(
    root: Path,
    ledger_path: Path,
    decision_record: Mapping[str, Any],
    validated: Mapping[str, Any],
    *,
    current: datetime,
    entry_fill_quote: Mapping[str, Any] | None,
    exit_quote: Mapping[str, Any] | None,
    marks: Iterable[MarketMark] | None,
    allow_entry: bool,
    recovery_ok: bool,
    exit_fee_cents_per_unit: int,
) -> dict[str, Any]:
    """Locked implementation; callers must hold ``_runtime_lock``."""

    candidate = validated["candidate"]
    candidate_id = str(candidate["candidate_id"])
    state = _load_state(root)
    book = PaperLedger(ledger_path, enforce_market_risk=True, clock=lambda: current)
    adapter = IntradayPaperAdapter(book, provenance="observed")
    lifecycle = _ledger_candidate(book, candidate_id)
    actions: list[dict[str, Any]] = []
    blocker: str | None = None
    reservation_id = lifecycle.get("reservation_id")
    position_id = lifecycle.get("position_id")
    close_id = lifecycle.get("close_id")

    if reservation_id and not position_id:
        reservation = book.get_reservation(reservation_id)
        if reservation.state != "reserved":
            blocker = "entry_cancelled"
        else:
            expiry = _now(reservation.expiry_at)
            cancellation_reason = (
                "risk_stop_active" if book.risk_stop_active()
                else "entries_disabled" if not allow_entry
                else "recovery_unhealthy" if not recovery_ok
                else None
            )
            if cancellation_reason is not None:
                adapter.cancel(reservation_id, reason=cancellation_reason, cancelled_at=current)
                actions.append({"type": "cancel_pending_entry", "reservation_id": reservation_id,
                                "reason": cancellation_reason})
                blocker = cancellation_reason
            elif current >= expiry:
                adapter.expire(reservation_id, now=current)
                actions.append({"type": "cancel_expired_entry", "reservation_id": reservation_id})
                blocker = "entry_expired"
            else:
                quote = _quote_for_symbol(entry_fill_quote, candidate["symbol"], "entry_fill_quote")
                if quote is None:
                    blocker = "awaiting_independent_post_reservation_quote"
                else:
                    try:
                        fill = adapter.entry_from_quote(reservation_id, quote, filled_at=current,
                                                        recovery_ok=recovery_ok, allow_entry=allow_entry)
                        position_id = fill.position_id
                        actions.append({"type": "simulated_observed_entry", "position_id": position_id})
                    except (EntryGateBlocked, RiskStopActive, IntradayAdapterError) as exc:
                        blocker = f"entry_blocked:{exc}"
    elif position_id and not close_id:
        quote = _quote_for_symbol(exit_quote, candidate["symbol"], "exit_quote")
        if quote is None:
            blocker = "awaiting_observed_exit_quote"
        else:
            try:
                closed = adapter.exit_from_quote(
                    position_id, quote,
                    exit_decision_at=candidate["exit_decision_timestamp"],
                    closed_at=current, fee_cents_per_unit=exit_fee_cents_per_unit,
                )
                close_id = closed.close_id
                actions.append({"type": "simulated_observed_exit", "position_id": position_id,
                                "realized_pnl_cents": closed.realized_pnl_cents})
            except IntradayAdapterError as exc:
                blocker = f"exit_blocked:{exc}"
    elif close_id:
        blocker = "already_closed"
    else:
        if not allow_entry or not recovery_ok:
            blocker = "entries_disabled" if not allow_entry else "recovery_unhealthy"
        else:
            mark_blocker = _ensure_marks(book, current, marks)
            if mark_blocker:
                blocker = mark_blocker
            else:
                try:
                    reservation = adapter.reserve(candidate, validated["entry_quotes"],
                                                  order_lifetime=validated["order_lifetime"], now=current,
                                                  recovery_ok=recovery_ok, allow_entry=allow_entry)
                    reservation_id = reservation.reservation_id
                    actions.append({"type": "reserve_observed_internal_paper", "reservation_id": reservation_id})
                except (EntryGateBlocked, RiskStopActive, IntradayAdapterError) as exc:
                    blocker = f"entry_blocked:{exc}"
                except Exception as exc:
                    blocker = f"reservation_rejected:{type(exc).__name__}"

    state["candidates"][candidate_id] = {
        "candidate_id": candidate_id, "updated_at": _iso(current), "reservation_id": reservation_id,
        "position_id": position_id, "close_id": close_id,
        "decision_sha256": hashlib.sha256(json.dumps(decision_record, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
    }
    _atomic_json(root / "state.json", state)
    return {
        "scope": "eq_intraday_observed_internal_paper", "status": "advanced" if actions else "abstained",
        "candidate_id": candidate_id, "actions": actions, "blocker": blocker,
        "reconciled": True, "state_path": str(root / "state.json"),
        "broker_orders_sent": 0, "broker_execution_enabled": False,
        "provenance": "observed_internal_simulation",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--ledger", required=True)
    parser.add_argument("--decision", help="retained observed-shadow decision JSON")
    parser.add_argument("--entry-fill-quote")
    parser.add_argument("--exit-quote")
    parser.add_argument("--now")
    parser.add_argument("--disable-entry", action="store_true")
    parser.add_argument("--recovery-unhealthy", action="store_true")
    parser.add_argument("--exit-fee-cents-per-unit", type=int, default=0)
    args = parser.parse_args(argv)
    try:
        record = _read_json(args.decision) if args.decision else None
        entry = _read_json(args.entry_fill_quote) if args.entry_fill_quote else None
        exit_quote = _read_json(args.exit_quote) if args.exit_quote else None
        result = run_cycle(args.state_dir, args.ledger, record, entry_fill_quote=entry,
                           exit_quote=exit_quote, now=args.now, allow_entry=not args.disable_entry,
                           recovery_ok=not args.recovery_unhealthy,
                           exit_fee_cents_per_unit=args.exit_fee_cents_per_unit)
    except Exception as exc:
        result = {"scope": "eq_intraday_observed_internal_paper", "status": "operational_failure",
                  "error_type": type(exc).__name__, "broker_orders_sent": 0,
                  "broker_execution_enabled": False}
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0 if result["status"] in {"advanced", "abstained"} else 1


__all__ = ["IntradayRuntimeError", "ORDER_LIFETIME", "run_cycle", "validate_decision_record", "main"]


if __name__ == "__main__":
    raise SystemExit(main())
