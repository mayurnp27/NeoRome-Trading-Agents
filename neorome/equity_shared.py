"""Shared internal-paper lifecycle for the bounded SPY equity policy.

The module is deliberately an adapter around :mod:`neorome.paper_ledger`.
It accepts an already captured observed daily bundle and a later observed
quote, never fetches data, and never submits broker orders.  A reservation is
created from the daily quote and can only fill from a distinct quote at least
ten seconds later.  The shared ledger remains the source of durable lifecycle
and risk truth; this module's private state keeps evidence and crash recovery
metadata.
"""

from __future__ import annotations

import contextlib
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
import fcntl
import hashlib
import inspect
import json
import os
from pathlib import Path
import secrets
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo

from desks.equities.paper_policy import STRATEGY_VERSION, evaluate, value_position

from .paper_ledger import (
    InvalidState,
    LedgerError,
    MarketMark,
    PaperLedger,
    Proposal,
)
from .equity_actions import action_clearance, validate_evidence


UTC = timezone.utc
NY = ZoneInfo("America/New_York")
SYMBOL = "SPY"
DESK = "equities"
POLICY_ID = "EQ-ETF-TREND-COMPARATOR"
POLICY_VERSION = STRATEGY_VERSION
LIFECYCLE_VERSION = "eq-shared-no-corporate-actions-v1"
PENDING_EXPIRY_SECONDS = 60
FILL_DELAY_SECONDS = 10
QUOTE_MAX_AGE_SECONDS = 60
STATE_SCHEMA_VERSION = 1
MARKER = "equity_shared"


class EquitySharedError(ValueError):
    """A malformed or unavailable input blocks this paper cycle."""


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _time(value: Any, name: str) -> datetime:
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str):
        try:
            result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise EquitySharedError(f"{name} is not an ISO timestamp") from exc
    else:
        raise EquitySharedError(f"{name} is not an ISO timestamp")
    if result.tzinfo is None or result.utcoffset() is None:
        raise EquitySharedError(f"{name} requires a timezone")
    return result.astimezone(UTC)


def _decimal(value: Any, name: str, *, positive: bool = False, nonnegative: bool = False) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise EquitySharedError(f"{name} is not finite numeric data")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise EquitySharedError(f"{name} is not finite numeric data") from exc
    if not result.is_finite() or (positive and result <= 0) or (nonnegative and result < 0):
        raise EquitySharedError(f"{name} is outside its allowed range")
    return result


def _ensure_root(path: str | os.PathLike[str]) -> Path:
    root = Path(path)
    if root.exists() and root.is_symlink():
        raise EquitySharedError("equity shared state symlink refused")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if root.is_symlink() or not root.is_dir():
        raise EquitySharedError("equity shared state is not a directory")
    try:
        os.chmod(root, 0o700)
    except OSError as exc:
        raise EquitySharedError("equity shared state permissions unavailable") from exc
    return root


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    if path.exists() and path.is_symlink():
        raise EquitySharedError("equity shared state file symlink refused")
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        encoded = (_canonical(value) + "\n").encode("utf-8")
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "schema_version": STATE_SCHEMA_VERSION,
            "policy_id": POLICY_ID,
            "policy_version": POLICY_VERSION,
            "lifecycle_version": LIFECYCLE_VERSION,
            "source_kind": "observed",
            "fill_evidence": [],
            "close_evidence": [],
        }
    if path.is_symlink() or not path.is_file():
        raise EquitySharedError("equity shared state is not a regular file")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise EquitySharedError("equity shared state is unreadable") from exc
    if not isinstance(value, dict) or value.get("schema_version") != STATE_SCHEMA_VERSION:
        raise EquitySharedError("equity shared state schema is unsupported")
    if value.get("policy_id") != POLICY_ID or value.get("policy_version") != POLICY_VERSION:
        raise EquitySharedError("equity shared state policy identity is unsupported")
    if value.get("lifecycle_version") != LIFECYCLE_VERSION:
        raise EquitySharedError("equity shared state lifecycle identity is unsupported")
    return value


@contextlib.contextmanager
def _state_lock(root: Path):
    try:
        descriptor = os.open(root / "equity.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        stream = os.fdopen(descriptor, "a+")
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        raise EquitySharedError("already_running") from exc
    except OSError as exc:
        raise EquitySharedError("unable to acquire equity shared state lock") from exc
    try:
        yield stream
    finally:
        stream.close()


@contextlib.contextmanager
def _caller_lock(lock: Any):
    """Hold the caller's runtime lock when one is supplied.

    The runtime owns this lock.  A context manager is preferred; a filesystem
    path is accepted for small command-line adapters.  Direct library calls
    may omit it, but runtime integration should always pass its lock.
    """
    if lock is None:
        yield
        return
    if hasattr(lock, "__enter__") and hasattr(lock, "__exit__"):
        with lock:
            yield
        return
    if isinstance(lock, (str, os.PathLike)):
        path = Path(lock)
        if path.exists() and path.is_symlink():
            raise EquitySharedError("caller runtime lock symlink refused")
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        stream = os.fdopen(descriptor, "a+")
        try:
            fcntl.flock(stream, fcntl.LOCK_EX)
            yield
        finally:
            stream.close()
        return
    raise EquitySharedError("caller runtime lock must be a context manager or path")


def _load_bundle(value: Mapping[str, Any] | str | os.PathLike[str]) -> tuple[dict[str, Any], str]:
    if isinstance(value, (str, os.PathLike)):
        path = Path(value)
        if path.is_symlink() or not path.is_file():
            raise EquitySharedError("daily bundle is not a regular file")
        try:
            raw = path.read_bytes()
            bundle = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise EquitySharedError("daily bundle is unreadable") from exc
        digest = hashlib.sha256(raw).hexdigest()
    elif isinstance(value, Mapping):
        bundle = dict(value)
        digest = _hash(bundle)
    else:
        raise EquitySharedError("daily bundle must be a mapping or JSON path")
    if not isinstance(bundle, dict):
        raise EquitySharedError("daily bundle must be an object")
    nested = bundle.get("bundle")
    if isinstance(nested, Mapping) and isinstance(nested.get("context"), Mapping) and isinstance(nested.get("observation"), Mapping):
        merged = dict(nested)
        for key in ("corporate_actions", "corporate_action_clearance", "action_evidence"):
            if key not in merged and key in bundle:
                merged[key] = bundle[key]
        bundle = merged
    return bundle, digest


def _validate_bundle(bundle: Mapping[str, Any], now: datetime) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    if bundle.get("source_kind") != "observed":
        raise EquitySharedError("daily bundle source_kind must be observed")
    if bundle.get("order_mutations", 0) != 0 or bundle.get("broker_orders_sent", 0) != 0:
        raise EquitySharedError("daily bundle contains order mutations")
    if bundle.get("execution_enabled") is not False:
        raise EquitySharedError("daily bundle is execution enabled")
    context = bundle.get("context")
    observation = bundle.get("observation")
    if not isinstance(context, Mapping) or not isinstance(observation, Mapping):
        raise EquitySharedError("daily bundle context and observation are required")
    if context.get("source_kind") != "observed" or observation.get("source_kind") != "observed":
        raise EquitySharedError("daily context and observation must be observed")
    if observation.get("symbol") != SYMBOL or observation.get("feed") != "iex":
        raise EquitySharedError("daily observation must be the observed SPY IEX quote")
    decision = evaluate(dict(observation), dict(context), now)
    return dict(context), dict(observation), decision


def _source_hash(value: Mapping[str, Any] | None, *, label: str) -> str:
    if not isinstance(value, Mapping) or value.get("source_kind") != "observed":
        raise EquitySharedError(f"{label} observed source evidence is required")
    candidate = value.get("sha256", value.get("hash", value.get("response_sha256")))
    if not isinstance(candidate, str) or len(candidate) != 64:
        raise EquitySharedError(f"{label} source hash is invalid")
    try:
        int(candidate, 16)
    except ValueError as exc:
        raise EquitySharedError(f"{label} source hash is invalid") from exc
    return candidate.lower()


def _quote_source(observation: Mapping[str, Any], bundle: Mapping[str, Any]) -> dict[str, Any]:
    direct = observation.get("quote_source") or bundle.get("quote_source")
    if isinstance(direct, Mapping):
        return {"source_kind": "observed", "sha256": _source_hash(direct, label="daily quote")}
    evidence = observation.get("evidence", [])
    candidates = evidence if isinstance(evidence, list) else []
    for item in reversed(candidates):
        if isinstance(item, Mapping):
            candidate = item.get("sha256", item.get("hash", item.get("response_sha256")))
            if isinstance(candidate, str) and len(candidate) == 64:
                try:
                    int(candidate, 16)
                except ValueError:
                    continue
                return {"source_kind": "observed", "sha256": candidate.lower()}
    raise EquitySharedError("daily quote observed source evidence is missing")


def _normalize_quote(value: Mapping[str, Any], source: Mapping[str, Any], now: datetime, *, label: str) -> tuple[dict[str, Any], dict[str, Any]]:
    if not isinstance(value, Mapping):
        raise EquitySharedError(f"{label} quote is missing")
    quote_time = _time(value.get("timestamp"), f"{label} quote.timestamp")
    age = (now - quote_time).total_seconds()
    if age < 0:
        raise EquitySharedError(f"{label} quote is future-dated")
    if age > QUOTE_MAX_AGE_SECONDS:
        raise EquitySharedError(f"{label} quote is stale")
    if value.get("symbol", SYMBOL) != SYMBOL:
        raise EquitySharedError(f"{label} quote symbol is unsupported")
    bid = _decimal(value.get("bid"), f"{label} quote.bid", positive=True)
    ask = _decimal(value.get("ask"), f"{label} quote.ask", positive=True)
    if ask < bid:
        raise EquitySharedError(f"{label} quote is crossed")
    if "depth_units" in value:
        depth = _decimal(value.get("depth_units"), f"{label} quote.depth_units", positive=True)
    else:
        bid_size = _decimal(value.get("bid_size"), f"{label} quote.bid_size", positive=True)
        ask_size = _decimal(value.get("ask_size"), f"{label} quote.ask_size", positive=True)
        depth = min(bid_size, ask_size)
    source_hash = _source_hash(source, label=label + " quote")
    clean = {
        "symbol": SYMBOL,
        "timestamp": _iso(quote_time),
        "bid": str(bid),
        "ask": str(ask),
        "bid_size": str(depth),
        "ask_size": str(depth),
        "depth_units": str(depth),
    }
    return clean, {"source_kind": "observed", "sha256": source_hash}


def _later_quote(value: Any, now: datetime) -> tuple[dict[str, Any], dict[str, Any]]:
    if isinstance(value, (tuple, list)) and len(value) == 2:
        quote, source = value
    elif isinstance(value, Mapping) and isinstance(value.get("quote"), Mapping):
        quote, source = value.get("quote"), value.get("source") or value.get("quote_source")
    elif isinstance(value, Mapping):
        quote = value
        source = value.get("source") or value.get("quote_source")
    else:
        raise EquitySharedError("later SPY quote must include quote and source evidence")
    return _normalize_quote(quote, source, now, label="later")


def _entry_cost(units: Decimal, ask: Decimal) -> tuple[int, int, int]:
    slipped = ask * (Decimal("1") + Decimal("0.0005"))
    gross = int((slipped * units * Decimal("100")).to_integral_value(rounding=ROUND_CEILING))
    fee = max(1, int((slipped * units * Decimal("100") * Decimal("0.001")).to_integral_value(rounding=ROUND_CEILING)))
    return gross, fee, gross + fee


def _supported_payload(payload: Mapping[str, Any]) -> bool:
    metadata = payload.get("metadata", {})
    marker = metadata.get(MARKER) if isinstance(metadata, Mapping) else None
    return bool(
        isinstance(marker, Mapping)
        and payload.get("desk") == DESK
        and payload.get("strategy") == POLICY_ID
        and payload.get("strategy_version") == POLICY_VERSION
        and payload.get("instrument") == SYMBOL
        and payload.get("provenance") == "observed"
        and marker.get("symbol") == SYMBOL
        and marker.get("lifecycle_version") == LIFECYCLE_VERSION
    )


def _entries(book: PaperLedger) -> tuple[list[tuple[Any, Mapping[str, Any]]], list[tuple[Any, Mapping[str, Any]]], list[Any]]:
    proposals: dict[str, Mapping[str, Any]] = {}
    reservations: dict[str, Mapping[str, Any]] = {}
    for event in book.audit():
        if event.event_type == "reserve":
            payload = event.payload.get("payload", {})
            if isinstance(payload, Mapping):
                proposals[str(payload.get("proposal_id"))] = payload
                reservations[event.entity_id] = payload
    pending: list[tuple[Any, Mapping[str, Any]]] = []
    own: list[tuple[Any, Mapping[str, Any]]] = []
    unsupported: list[Any] = []
    for event in book.audit():
        if event.event_type == "reserve":
            payload = proposals.get(str(event.payload.get("proposal_id")), {})
            metadata = payload.get("metadata", {}) if isinstance(payload, Mapping) else {}
            marker = metadata.get(MARKER) if isinstance(metadata, Mapping) else None
            if not isinstance(marker, Mapping):
                continue
            reservation = book.get_reservation(event.entity_id)
            if reservation.state == "reserved" and _supported_payload(payload):
                pending.append((reservation, payload))
            elif reservation.state == "reserved":
                unsupported.append(reservation)
        elif event.event_type == "fill":
            payload = reservations.get(str(event.payload.get("reservation_id")), {})
            payload = proposals.get(str(payload.get("proposal_id")), payload)
            metadata = payload.get("metadata", {}) if isinstance(payload, Mapping) else {}
            marker = metadata.get(MARKER) if isinstance(metadata, Mapping) else None
            position_id = event.payload.get("position_id")
            if not isinstance(position_id, str):
                continue
            position = book.get_position(position_id)
            if position.state != "open":
                continue
            is_equity_policy = (
                payload.get("desk") == DESK
                and payload.get("strategy") == POLICY_ID
                and payload.get("instrument") == SYMBOL
            )
            if isinstance(marker, Mapping) and _supported_payload(payload):
                own.append((position, payload))
            elif isinstance(marker, Mapping) or is_equity_policy:
                unsupported.append(position)
    return pending, own, unsupported


def _all_positions(book: PaperLedger) -> list[Any]:
    result = []
    for event in book.audit():
        if event.event_type != "fill":
            continue
        position_id = event.payload.get("position_id")
        if isinstance(position_id, str):
            position = book.get_position(position_id)
            if position.state == "open":
                result.append(position)
    return result


def _corporate_clearance(value: Any, *, now: datetime, positions: list[Any]) -> dict[str, Any]:
    """Validate the shared raw corporate-action evidence contract.

    ``equity_actions`` owns wire decoding, provenance, normalized action
    equality, and economic date semantics.  This adapter only applies its
    result to the shared lifecycle and never trusts caller-supplied flags.
    """
    if not isinstance(value, Mapping):
        raise EquitySharedError("corporate_action_evidence_missing")
    validation = validate_evidence(value, now=now)
    if validation.get("status") != "clear":
        reasons = "; ".join(str(item) for item in validation.get("reasons", []))
        raise EquitySharedError("corporate_action_evidence_invalid" + ((": " + reasons) if reasons else ""))
    holding_dates = [
        _time(position.opened_at, "position.opened_at").astimezone(NY).date().isoformat()
        for position in positions
    ] or [now.astimezone(NY).date().isoformat()]
    entry = action_clearance(value, open_date=now.astimezone(NY).date(), now=now, lookahead_days=7)
    valuation = [
        action_clearance(value, open_date=opened, now=now, lookahead_days=0)
        for opened in holding_dates
    ]
    entry_intersections = entry.get("intersections", [])
    entry_allowed = entry.get("status") == "clear"
    valuation_allowed = all(result.get("status") == "clear" for result in valuation)
    # A seven-day lookahead intersection is an entry/early-exit guard.  A
    # current-day intersection blocks valuation and closing because this
    # adapter cannot establish post-action units or receivables.
    return {
        "sha256": _hash(value),
        "entry_allowed": entry_allowed,
        "valuation_allowed": valuation_allowed,
        "upcoming_action_within_7_days": bool(entry_intersections),
        "requires_later_exit_quote": bool(entry_intersections and positions),
        "entry_clearance": entry,
        "valuation_clearance": valuation,
        "validation": validation,
    }


def _mark_positions(book: PaperLedger, provider: Callable[..., Any] | None, now: datetime) -> Any:
    if provider is None:
        raise EquitySharedError("global_marks_missing")
    try:
        try:
            arity = len(inspect.signature(provider).parameters)
        except (TypeError, ValueError):
            arity = 2
        if arity >= 3:
            marks = [provider(book, position, now) for position in _all_positions(book)]
        else:
            marks = provider(book, now)
    except Exception as exc:
        raise EquitySharedError("global_marks_unavailable") from exc
    if not isinstance(marks, (list, tuple)):
        raise EquitySharedError("global_marks_incomplete")
    try:
        signature = _hash({"as_of": _iso(now), "marks": [repr(mark) for mark in marks]})
        return book.record_marks("equity-shared-mark-" + signature[:32], list(marks), as_of=now)
    except LedgerError as exc:
        raise EquitySharedError("global_marks_incomplete") from exc


def _position_payload(book: PaperLedger, position: Any) -> Mapping[str, Any]:
    for event in book.audit():
        if event.event_type != "reserve":
            continue
        payload = event.payload.get("payload", {})
        if isinstance(payload, Mapping) and payload.get("proposal_id") == position.proposal_id:
            return payload
    raise EquitySharedError("position proposal evidence is missing")


def position_mark(
    book: PaperLedger,
    position: Any,
    now: datetime,
    quote: Mapping[str, Any],
    quote_source: Mapping[str, Any],
    corporate_actions: Mapping[str, Any],
) -> MarketMark:
    """Build one strict own-position mark for a shared mark callback.

    The caller supplies the quote and the normalized raw corporate-action
    evidence.  This helper verifies the durable proposal identity before
    producing a mark; it cannot mark an arbitrary foreign or malformed row.
    """
    if not hasattr(position, "position_id") or position.state != "open":
        raise EquitySharedError("position is not open")
    stored = book.get_position(position.position_id)
    if stored != position:
        raise EquitySharedError("position does not match shared ledger")
    payload = _position_payload(book, position)
    if not _supported_payload(payload) or payload.get("provenance") != position.provenance:
        raise EquitySharedError("position proposal identity is unsupported")
    metadata = payload.get("metadata", {})
    marker = metadata.get(MARKER) if isinstance(metadata, Mapping) else None
    if not isinstance(marker, Mapping):
        raise EquitySharedError("position marker is missing")
    units = _decimal(marker.get("units"), "position units", positive=True)
    clean_quote, source = _normalize_quote(quote, quote_source, now, label="mark")
    if _decimal(clean_quote["depth_units"], "mark quote depth", positive=True) < units:
        raise EquitySharedError("insufficient_mark_depth")
    clearance = _corporate_clearance(corporate_actions, now=now, positions=[position])
    if not clearance["valuation_allowed"]:
        raise EquitySharedError("accounting_blocked: corporate_action_valuation_blocked")
    values = value_position(str(units), clean_quote)
    return MarketMark(
        position.position_id,
        values["liquidation_value_cents"],
        clean_quote["timestamp"],
        source["sha256"],
        position.provenance,
    )


def _quote_evidence(quote: Mapping[str, Any], source: Mapping[str, Any], *, role: str, units: Decimal, gross: int, fee: int) -> dict[str, Any]:
    return {
        "role": role,
        "symbol": SYMBOL,
        "timestamp": quote["timestamp"],
        "source_kind": "observed",
        "source_hash": source["sha256"],
        "bid": quote["bid"],
        "ask": quote["ask"],
        "depth_units": quote["depth_units"],
        "units": str(units),
        "gross_cents": gross,
        "fee_cents": fee,
    }


def _asdict(value: Any) -> dict[str, Any]:
    return asdict(value) if hasattr(value, "__dataclass_fields__") else dict(value)


def run(
    daily_bundle: Mapping[str, Any] | str | os.PathLike[str],
    state_dir: str | os.PathLike[str],
    *,
    ledger_path: str | os.PathLike[str],
    later_quote: Any = None,
    quote: Mapping[str, Any] | None = None,
    quote_source: Mapping[str, Any] | None = None,
    allow_entry: bool = True,
    now: datetime | None = None,
    mark_provider: Callable[..., Any] | None = None,
    mark_all: Callable[..., Any] | None = None,
    corporate_actions: Mapping[str, Any] | None = None,
    runtime_lock: Any = None,
    caller_runtime_lock: Any = None,
    stop_path: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
    """Run one serialized observed equity-paper lifecycle cycle."""
    supplied_now = now
    if supplied_now is not None and (supplied_now.tzinfo is None or supplied_now.utcoffset() is None):
        raise EquitySharedError("now requires a timezone")
    current = (supplied_now or datetime.now(UTC)).astimezone(UTC)
    if type(allow_entry) is not bool:
        raise EquitySharedError("allow_entry must be a bool")
    if mark_provider is not None and mark_all is not None and mark_provider is not mark_all:
        raise EquitySharedError("mark_provider and mark_all disagree")
    provider = mark_provider or mark_all
    if later_quote is not None and (quote is not None or quote_source is not None):
        raise EquitySharedError("later_quote and quote/quote_source disagree")
    if later_quote is None and (quote is not None or quote_source is not None):
        later_quote = (quote, quote_source)
    caller = runtime_lock if runtime_lock is not None else caller_runtime_lock
    root = _ensure_root(state_dir)
    stop = Path(stop_path) if stop_path is not None else root / "STOP"
    actions: list[dict[str, Any]] = []
    blocker: str | None = None
    decision: dict[str, Any] = {}
    mark_snapshot: Any = None
    state_path = root / "state.json"

    try:
        with _caller_lock(caller):
            with _state_lock(root):
                state = _load_state(state_path)
                state.setdefault("fill_evidence", [])
                state.setdefault("close_evidence", [])
                book = PaperLedger(ledger_path, enforce_market_risk=True, clock=lambda: current)
                pending, own, unsupported = _entries(book)
                all_positions = _all_positions(book)
                bundle: dict[str, Any] = {}
                bundle_hash: str | None = None
                observation: dict[str, Any] = {}
                decision: dict[str, Any] = {}
                bundle_error: str | None = None
                try:
                    bundle, bundle_hash = _load_bundle(daily_bundle)
                    _context, observation, decision = _validate_bundle(bundle, current)
                except EquitySharedError as exc:
                    bundle_error = str(exc)
                    blocker = "daily_bundle_" + str(exc)
                state.update({"updated_at": _iso(current)})
                if bundle_hash is not None:
                    state["bundle_sha256"] = bundle_hash

                action_value = corporate_actions
                if action_value is None:
                    action_value = bundle.get("corporate_action_clearance") or bundle.get("corporate_actions") or bundle.get("action_evidence")
                if bundle:
                    state["daily_bundle"] = bundle
                state["corporate_action_clearance"] = action_value
                state["cycle_input"] = {
                    "evaluated_at": _iso(current),
                    "bundle_sha256": bundle_hash,
                    "bundle_error": bundle_error,
                    "later_quote_supplied": later_quote is not None,
                }
                # Persist the complete available input before any ledger
                # mutation, including STOP/recovery cancellation.
                _atomic_json(state_path, state)

                # Complete any evidence write that preceded a successful ledger
                # commit before processing a restart.
                pending_fill = state.get("pending_fill_evidence")
                if isinstance(pending_fill, Mapping) and any(
                    event.event_type == "fill" and event.entity_id == pending_fill.get("fill_id") for event in book.audit()
                ):
                    if not any(item.get("fill_id") == pending_fill.get("fill_id") for item in state["fill_evidence"] if isinstance(item, Mapping)):
                        state["fill_evidence"].append(dict(pending_fill))
                    state.pop("pending_fill_evidence", None)
                pending_close = state.get("pending_close_evidence")
                if isinstance(pending_close, Mapping) and any(
                    event.event_type == "close" and event.entity_id == pending_close.get("close_id") for event in book.audit()
                ):
                    if not any(item.get("close_id") == pending_close.get("close_id") for item in state["close_evidence"] if isinstance(item, Mapping)):
                        state["close_evidence"].append(dict(pending_close))
                    state.pop("pending_close_evidence", None)
                    state.pop("pending_close_intent", None)

                # A STOP is a local maintenance control: it releases this
                # adapter's pending liabilities while leaving shared positions
                # and the shared manual risk stop untouched.
                stop_active = stop.exists() or stop.is_symlink()
                if stop_active:
                    for reservation, _payload in pending:
                        cancelled = book.cancel(reservation.reservation_id, reason="equity shared STOP", cancelled_at=current)
                        actions.append({"type": "cancel_stop", "result": _asdict(cancelled)})
                    pending = []
                    close_intent = state.pop("pending_close_intent", None)
                    if isinstance(close_intent, Mapping):
                        actions.append({"type": "cancel_stop_close_intent", "intent": dict(close_intent)})
                    blocker = "stop_active"

                if not allow_entry:
                    if pending:
                        for reservation, _payload in list(pending):
                            cancelled = book.cancel(reservation.reservation_id, reason="equity shared entries disabled", cancelled_at=current)
                            actions.append({"type": "cancel_entries_disabled", "result": _asdict(cancelled)})
                        pending = []
                    close_intent = state.pop("pending_close_intent", None)
                    if isinstance(close_intent, Mapping):
                        actions.append({"type": "cancel_entries_disabled_close_intent", "intent": dict(close_intent)})
                    if not stop_active:
                        blocker = "entries_disabled"

                # Expiry is checked before any later quote can be used.
                for reservation, _payload in list(pending):
                    if _time(reservation.expiry_at, "reservation.expiry_at") <= current:
                        cancelled = book.cancel(reservation.reservation_id, reason="equity shared pending fill expired", cancelled_at=current)
                        actions.append({"type": "cancel_expired_reservation", "result": _asdict(cancelled)})
                close_intent = state.get("pending_close_intent")
                if (not stop_active and allow_entry and isinstance(close_intent, Mapping)
                        and _time(close_intent.get("expires_at"), "close intent expiry") <= current):
                    state.pop("pending_close_intent", None)
                    actions.append({"type": "cancel_expired_close_intent", "intent": dict(close_intent)})
                    blocker = "close_intent_expired"
                pending, own, unsupported = _entries(book)
                all_positions = _all_positions(book)

                clearance: dict[str, Any] | None = None
                clearance_error: str | None = None
                try:
                    # Corporate-action entitlement is instrument-specific:
                    # foreign desks' holdings must not widen the SPY holding
                    # interval used for action clearance.
                    clearance = _corporate_clearance(action_value, now=current, positions=[position for position, _payload in own])
                except EquitySharedError as exc:
                    clearance_error = str(exc)
                    if blocker not in {"stop_active", "entries_disabled"}:
                        blocker = "accounting_blocked: " + str(exc)
                # Persist normalized clearance alongside the original evidence.
                if clearance is not None:
                    state["normalized_corporate_action_clearance"] = clearance
                _atomic_json(state_path, state)

                # Pending reservations are filled only after the complete shared
                # portfolio has been marked and a distinct quote has arrived.
                if pending and blocker not in {"stop_active"}:
                    reservation, payload = pending[0]
                    if clearance is None:
                        blocker = blocker or "accounting_blocked: " + (clearance_error or "corporate_action_evidence_missing")
                    elif not clearance["entry_allowed"]:
                        cancelled = book.cancel(reservation.reservation_id, reason="corporate action entry guard", cancelled_at=current)
                        actions.append({"type": "cancel_corporate_action", "result": _asdict(cancelled)})
                        blocker = "corporate_action_entry_blocked"
                    elif bundle_error is not None or not decision.get("market_inputs_valid", False):
                        blocker = "daily_bundle_not_current"
                    else:
                        try:
                            mark_snapshot = _mark_positions(book, provider, current)
                        except EquitySharedError as exc:
                            blocker = str(exc)
                        if mark_snapshot is not None:
                            if later_quote is None:
                                blocker = "awaiting_independent_post_reservation_quote"
                            else:
                                try:
                                    arrival, arrival_source = _later_quote(later_quote, current)
                                    marker = payload.get("metadata", {}).get(MARKER, {})
                                    units = _decimal(marker.get("units"), "reserved units", positive=True)
                                    entry_time = _time(marker.get("entry_quote_timestamp"), "entry quote timestamp")
                                    arrival_time = _time(arrival["timestamp"], "later quote timestamp")
                                    if arrival_time <= entry_time or arrival_time < _time(reservation.created_at, "reservation.created_at") + timedelta(seconds=FILL_DELAY_SECONDS) or arrival_source["sha256"] == marker.get("entry_quote_hash"):
                                        blocker = "awaiting_independent_post_reservation_quote"
                                    elif _decimal(arrival["depth_units"], "later quote depth", positive=True) < units:
                                        blocker = "insufficient_fill_depth"
                                    else:
                                        gross, fee, total = _entry_cost(units, _decimal(arrival["ask"], "later quote ask", positive=True))
                                        if total > reservation.reserved_cents:
                                            blocker = "post_reservation_cost_exceeds_reserved_cash"
                                        else:
                                            fill_id = f"equity-shared-fill-{reservation.reservation_id}"
                                            evidence = _quote_evidence(arrival, arrival_source, role="arrival_fill", units=units, gross=gross, fee=fee)
                                            evidence.update({"fill_id": fill_id, "reservation_id": reservation.reservation_id, "corporate_action_sha256": clearance["sha256"]})
                                            state["pending_fill_evidence"] = evidence
                                            _atomic_json(state_path, state)
                                            filled = book.simulate_fill(reservation.reservation_id, fill_id=fill_id, quantity=1, unit_cost_cents=gross, fee_cents=fee, provenance="observed", filled_at=current)
                                            state["fill_evidence"].append(evidence)
                                            state.pop("pending_fill_evidence", None)
                                            actions.append({"type": "simulated_fill", "result": _asdict(filled), "arrival_quote": evidence})
                                            state["last_action"] = "fill"
                                except EquitySharedError as exc:
                                    blocker = str(exc)

                pending, own, unsupported = _entries(book)
                all_positions = _all_positions(book)

                # An exit signal creates a durable intent.  A close requires a
                # separate observed quote at least ten seconds later, matching
                # the entry lifecycle's independent-arrival rule.
                if not actions and own and blocker not in {"stop_active", "entries_disabled"}:
                    if clearance is None:
                        blocker = blocker or "accounting_blocked: " + (clearance_error or "corporate_action_evidence_missing")
                    elif not clearance["valuation_allowed"]:
                        # Once an action crosses the holding interval, neither
                        # ordinary valuation nor a close using pre-action units
                        # is safe without entitlement/receivable accounting.
                        blocker = "accounting_blocked: corporate_action_valuation_blocked"
                    else:
                        marks_error: str | None = None
                        try:
                            mark_snapshot = _mark_positions(book, provider, current)
                        except EquitySharedError as exc:
                            marks_error = str(exc)
                        requires_exit_quote = bool(clearance["requires_later_exit_quote"])
                        signal_exit = bool(
                            bundle_error is None
                            and decision.get("market_inputs_valid")
                            and decision.get("rebalance_due")
                            and decision.get("trend_positive") is False
                        )
                        should_close = requires_exit_quote or signal_exit
                        position, payload = own[0]
                        intent = state.get("pending_close_intent")
                        if isinstance(intent, Mapping) and intent.get("position_id") != position.position_id:
                            blocker = "pending_close_intent_position_mismatch"
                        elif not isinstance(intent, Mapping) and should_close:
                            trigger_quote_timestamp = None
                            trigger_source_hash = None
                            if signal_exit:
                                try:
                                    daily_source = _quote_source(observation, bundle)
                                    trigger_quote, daily_source = _normalize_quote(
                                        observation.get("quote"), daily_source, current, label="daily"
                                    )
                                    trigger_quote_timestamp = trigger_quote["timestamp"]
                                    trigger_source_hash = daily_source["sha256"]
                                except EquitySharedError as exc:
                                    blocker = str(exc)
                            if blocker is None or requires_exit_quote:
                                intent = {
                                    "intent_id": f"equity-shared-close-intent-{position.position_id}",
                                    "position_id": position.position_id,
                                    "created_at": _iso(current),
                                    "expires_at": _iso(current + timedelta(seconds=PENDING_EXPIRY_SECONDS)),
                                    "trigger": "corporate_action" if requires_exit_quote else "signal_exit",
                                    "trigger_quote_timestamp": trigger_quote_timestamp,
                                    "trigger_source_hash": trigger_source_hash,
                                    "corporate_action_sha256": clearance["sha256"],
                                    "bundle_sha256": bundle_hash,
                                }
                                state["pending_close_intent"] = intent
                                _atomic_json(state_path, state)
                                actions.append({"type": "pending_close", "intent": dict(intent), "global_marks_blocker": marks_error})
                                blocker = "awaiting_independent_exit_quote"
                        elif isinstance(intent, Mapping):
                            if later_quote is None:
                                blocker = "awaiting_independent_exit_quote"
                            else:
                                fresh_quote: tuple[dict[str, Any], dict[str, Any]] | None = None
                                try:
                                    fresh_quote = _later_quote(later_quote, current)
                                    arrival_time = _time(fresh_quote[0]["timestamp"], "later quote timestamp")
                                    earliest = _time(intent.get("created_at"), "close intent created_at") + timedelta(seconds=FILL_DELAY_SECONDS)
                                    trigger_time = intent.get("trigger_quote_timestamp")
                                    same_source = (
                                        isinstance(intent.get("trigger_source_hash"), str)
                                        and fresh_quote[1]["sha256"] == intent.get("trigger_source_hash")
                                    )
                                    if arrival_time < earliest or same_source or (
                                        trigger_time is not None
                                        and arrival_time <= _time(trigger_time, "close trigger quote timestamp")
                                    ):
                                        blocker = "awaiting_independent_exit_quote"
                                        fresh_quote = None
                                except EquitySharedError as exc:
                                    blocker = str(exc)
                                if fresh_quote is not None:
                                    exit_quote, exit_source = fresh_quote
                                    units = _decimal(payload.get("metadata", {}).get(MARKER, {}).get("units"), "position units", positive=True)
                                    if _decimal(exit_quote["depth_units"], "exit quote depth", positive=True) < units:
                                        blocker = "insufficient_exit_depth"
                                    else:
                                        values = value_position(str(units), exit_quote)
                                        close_id = f"equity-shared-close-{position.position_id}"
                                        evidence = _quote_evidence(exit_quote, exit_source, role="exit_close", units=units, gross=values["gross_exit_cents"], fee=values["exit_fee_cents"])
                                        evidence.update({
                                            "close_id": close_id,
                                            "position_id": position.position_id,
                                            "close_intent": dict(intent),
                                            "corporate_action_sha256": clearance["sha256"],
                                        })
                                        state["pending_close_evidence"] = evidence
                                        _atomic_json(state_path, state)
                                        closed = book.close(position.position_id, close_id=close_id, proceeds_cents=values["gross_exit_cents"], fee_cents=values["exit_fee_cents"], provenance="observed", closed_at=current)
                                        state["close_evidence"].append(evidence)
                                        state.pop("pending_close_evidence", None)
                                        state.pop("pending_close_intent", None)
                                        actions.append({"type": "simulated_close", "result": _asdict(closed), "exit_quote": evidence, "global_marks_blocker": marks_error})
                                        state["last_action"] = "close"
                                        if marks_error is None:
                                            try:
                                                mark_snapshot = _mark_positions(book, provider, current)
                                            except EquitySharedError:
                                                pass
                        elif marks_error:
                            blocker = blocker or marks_error

                pending, own, unsupported = _entries(book)
                if (allow_entry and not actions and not pending and not own
                        and blocker not in {"stop_active", "entries_disabled"}
                        and decision.get("eligible_for_internal_paper")):
                    if clearance is None:
                        blocker = blocker or "accounting_blocked: " + (clearance_error or "corporate_action_evidence_missing")
                    elif not clearance["entry_allowed"]:
                        blocker = "corporate_action_entry_blocked"
                    elif unsupported:
                        blocker = "unsupported_open_position_blocks_entry"
                    else:
                        try:
                            mark_snapshot = _mark_positions(book, provider, current)
                        except EquitySharedError as exc:
                            blocker = str(exc)
                        if mark_snapshot is not None:
                            daily_source = _quote_source(observation, bundle)
                            daily_quote, daily_quote_source = _normalize_quote(observation.get("quote"), daily_source, current, label="daily")
                            month = current.strftime("%Y-%m")
                            proposal_id = f"equity-shared-{month}"
                            attempted = any(
                                event.event_type == "reserve" and event.payload.get("proposal_id") == proposal_id
                                for event in book.audit()
                            )
                            if attempted:
                                blocker = "entry_already_attempted"
                            else:
                                units = _decimal(decision.get("units"), "entry units", positive=True)
                                gross, fee, total = _entry_cost(units, _decimal(daily_quote["ask"], "daily quote ask", positive=True))
                                if total > 1_500:
                                    blocker = "entry_size_exceeds_firm_cap"
                                elif _decimal(daily_quote["depth_units"], "daily quote depth", positive=True) < units:
                                    blocker = "insufficient_entry_depth"
                                else:
                                    proposal = Proposal(
                                        proposal_id=proposal_id,
                                        desk=DESK,
                                        strategy=POLICY_ID,
                                        strategy_version=POLICY_VERSION,
                                        source_hash=daily_quote_source["sha256"],
                                        source_timestamp=daily_quote["timestamp"],
                                        instrument=SYMBOL,
                                        exposure_group="us-broad-equity",
                                        idea_id=proposal_id,
                                        expiry_at=current + timedelta(seconds=PENDING_EXPIRY_SECONDS),
                                        quantity=1,
                                        unit_cost_cents=gross,
                                        fee_cents=fee,
                                        provenance="observed",
                                        metadata={MARKER: {
                                            "symbol": SYMBOL,
                                            "units": str(units),
                                            "entry_quote_timestamp": daily_quote["timestamp"],
                                            "entry_quote_hash": daily_quote_source["sha256"],
                                            "bundle_sha256": bundle_hash,
                                            "corporate_action_sha256": clearance["sha256"],
                                            "lifecycle_version": LIFECYCLE_VERSION,
                                        }},
                                    )
                                    reservation = book.reserve(proposal, now=current)
                                    actions.append({"type": "reserve", "result": _asdict(reservation), "units": str(units), "total_reserved_cents": total})
                                    state.update({"reservation_id": reservation.reservation_id, "last_action": "reserve"})

                try:
                    mark_snapshot = asdict(book.market_snapshot(as_of=current))
                except LedgerError:
                    mark_snapshot = None
                if not actions and blocker is None and not own and not pending and not decision.get("eligible_for_internal_paper"):
                    blocker = "no_entry"
                state["updated_at"] = _iso(current)
                _atomic_json(state_path, state)
    except EquitySharedError as exc:
        blocker = str(exc)
    except (LedgerError, ValueError) as exc:
        blocker = f"{type(exc).__name__}: {exc}"

    return {
        "mode": "observed_internal_shared_equity",
        "status": "ok" if blocker is None else "blocked",
        "source_kind": "observed",
        "policy_version": POLICY_VERSION,
        "lifecycle_version": LIFECYCLE_VERSION,
        "evaluated_at": _iso(current),
        "actions": actions,
        "blocker": blocker,
        "decision": decision,
        "market_snapshot": mark_snapshot,
        "state_path": str(state_path),
        "ledger_path": str(ledger_path),
        "broker_orders_sent": 0,
        "broker_execution_enabled": False,
        "fill_model": "reserved daily quote with delayed independent observed SPY quote; not a broker fill",
        "profitability_validated": False,
    }


__all__ = ["EquitySharedError", "FILL_DELAY_SECONDS", "LIFECYCLE_VERSION", "PENDING_EXPIRY_SECONDS", "position_mark", "run"]
