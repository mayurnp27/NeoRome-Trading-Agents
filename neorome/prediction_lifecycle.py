"""Gated, network-free prediction paper lifecycle on the shared ledger.

This module consumes caller-supplied evidence.  It does not fetch venue data,
approve a strategy, place an order, or turn the synthetic bridge into an
observed-data path.
"""

from __future__ import annotations

import base64
import contextlib
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import fcntl
import hashlib
import json
import os
from pathlib import Path
import secrets
from typing import Any, Callable, Mapping

from .paper_ledger import LedgerError, MarketMark, PaperLedger, Proposal

UTC = timezone.utc
SCHEMA_VERSION = 1
LIFECYCLE_VERSION = "prediction-single-taker-v1"
REVIEW_VERSION = "prediction-admission-review-v1"
SETTLEMENT_REVIEW_VERSION = "prediction-settlement-adapter-v1"
QUOTE_SCHEMA_VERSION = "prediction-quote-v1"
TERMS_SCHEMA_VERSION = "kalshi-weather-terms-v1"
TERM_FIELDS = ("ticker", "event_ticker", "market_type", "rules_primary", "rules_secondary",
               "strike_type", "floor_strike", "cap_strike", "notional_value_dollars",
               "early_close_condition", "open_time", "close_time", "expiration_time",
               "expected_expiration_time", "latest_expiration_time")
QUOTE_FIELDS = {"schema_version", "source_kind", "venue", "provider", "provider_quote_id",
                "ticker", "event_id", "outcome", "units", "timestamp", "received_at",
                "bid_cents", "ask_cents", "bid_depth", "ask_depth",
                "total_entry_fee_cents", "total_exit_fee_cents"}
MARKER = "prediction_lifecycle_v1"
MAX_AGE_SECONDS = 60
INTENT_SECONDS = 60
DELAY_SECONDS = 10


class PredictionLifecycleError(ValueError):
    pass


def _canonical(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise PredictionLifecycleError("payload must contain JSON values only") from exc


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def _hash(value: Any, name: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdefABCDEF" for c in value):
        raise PredictionLifecycleError(f"{name} must be a SHA-256 digest")
    return value.lower()


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PredictionLifecycleError(f"{name} must be a non-empty string")
    return value.strip()


def _integer(value: Any, name: str, minimum: int = 0, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum or (maximum is not None and value > maximum):
        raise PredictionLifecycleError(f"{name} is outside its allowed integer range")
    return value


def _time(value: Any, name: str) -> datetime:
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str):
        try:
            result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise PredictionLifecycleError(f"{name} must be an ISO timestamp") from exc
    else:
        raise PredictionLifecycleError(f"{name} must be an ISO timestamp")
    if result.tzinfo is None or result.utcoffset() is None:
        raise PredictionLifecycleError(f"{name} requires a timezone")
    return result.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _raw(value: Any, name: str) -> tuple[Mapping[str, Any], str]:
    if not isinstance(value, Mapping):
        raise PredictionLifecycleError(f"{name} must be an object")
    payload = value.get("payload")
    if not isinstance(payload, Mapping):
        raise PredictionLifecycleError(f"{name}.payload must be an object")
    supplied = _hash(value.get("sha256"), f"{name}.sha256")
    actual = _digest(payload)
    if supplied != actual:
        raise PredictionLifecycleError(f"{name} hash mismatch")
    return payload, actual


def _review(value: Any, *, settlement: bool = False) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise PredictionLifecycleError("versioned adapter review is required")
    expected = SETTLEMENT_REVIEW_VERSION if settlement else REVIEW_VERSION
    if value.get("version") != expected or value.get("decision") != "approved":
        raise PredictionLifecycleError("adapter review is not approved")
    reviewer = _text(value.get("reviewer"), "review.reviewer")
    manifest = value.get("manifest")
    if not isinstance(manifest, Mapping):
        raise PredictionLifecycleError("review manifest is required")
    review_hash = _hash(value.get("review_hash"), "review.review_hash")
    if _digest(manifest) != review_hash:
        raise PredictionLifecycleError("review manifest hash mismatch")
    return {"version": expected, "decision": "approved", "reviewer": reviewer,
            "review_hash": review_hash, "manifest": dict(manifest)}


def _quote(
    value: Any, now: datetime, name: str, *, identity: Mapping[str, Any], role: str
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise PredictionLifecycleError(f"{name} is required")
    raw_payload, raw_hash = _raw(value.get("raw"), f"{name}.raw")
    required_identity = {key: identity[key] for key in ("source_kind", "venue", "ticker", "event_id", "outcome", "units")}
    if raw_payload.get("schema_version") != QUOTE_SCHEMA_VERSION or set(raw_payload) != QUOTE_FIELDS:
        raise PredictionLifecycleError(f"{name} quote schema is unsupported")
    if any(raw_payload.get(key) != expected for key, expected in required_identity.items()):
        raise PredictionLifecycleError(f"{name} raw quote identity mismatch")
    provider = _text(raw_payload.get("provider"), f"{name}.provider")
    provider_quote_id = _text(raw_payload.get("provider_quote_id"), f"{name}.provider_quote_id")
    timestamp = _time(raw_payload.get("timestamp"), f"{name}.timestamp")
    received = _time(raw_payload.get("received_at"), f"{name}.received_at")
    if timestamp > received or received > now or timestamp > now:
        raise PredictionLifecycleError(f"{name} timing is invalid")
    if (now - timestamp).total_seconds() > MAX_AGE_SECONDS or (now - received).total_seconds() > MAX_AGE_SECONDS:
        raise PredictionLifecycleError(f"{name} is stale")
    bid = _integer(raw_payload.get("bid_cents"), f"{name}.bid_cents", 0, 100)
    ask = _integer(raw_payload.get("ask_cents"), f"{name}.ask_cents", 1, 100)
    bid_depth = _integer(raw_payload.get("bid_depth"), f"{name}.bid_depth", 0)
    ask_depth = _integer(raw_payload.get("ask_depth"), f"{name}.ask_depth", 0)
    entry_fee = _integer(raw_payload.get("total_entry_fee_cents"), f"{name}.total_entry_fee_cents", 0)
    exit_fee = _integer(raw_payload.get("total_exit_fee_cents"), f"{name}.total_exit_fee_cents", 0)
    if bid > ask:
        raise PredictionLifecycleError(f"{name} is crossed")
    projection = {"timestamp": _iso(timestamp), "received_at": _iso(received), "bid_cents": bid,
                  "ask_cents": ask, "bid_depth": bid_depth, "ask_depth": ask_depth,
                  "total_entry_fee_cents": entry_fee, "total_exit_fee_cents": exit_fee}
    for key, expected in projection.items():
        if value.get(key) != expected:
            raise PredictionLifecycleError(f"{name} projection does not match raw payload")
    if role in {"entry", "fill"} and ask_depth < identity["units"]:
        raise PredictionLifecycleError(f"{name} ask depth is insufficient")
    if role in {"mark", "exit"} and bid_depth < identity["units"]:
        raise PredictionLifecycleError(f"{name} bid depth is insufficient")
    return {"timestamp": _iso(timestamp), "received_at": _iso(received), "bid_cents": bid,
            "ask_cents": ask, "bid_depth": bid_depth, "ask_depth": ask_depth,
            "entry_fee_cents": entry_fee, "exit_fee_cents": exit_fee,
            "provider": provider, "provider_quote_id": provider_quote_id,
            "raw_sha256": raw_hash, "raw_payload": dict(raw_payload)}


def _admission(value: Any, now: datetime) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise PredictionLifecycleError("admission is required")
    if value.get("schema_version") != 1 or value.get("lifecycle_version") != LIFECYCLE_VERSION:
        raise PredictionLifecycleError("admission version is unsupported")
    source_kind = value.get("source_kind")
    if source_kind not in {"observed", "synthetic"}:
        raise PredictionLifecycleError("source_kind must be observed or synthetic")
    if source_kind == "observed":
        raise PredictionLifecycleError("observed_adapter_not_activated")
    # These flags are necessary negative gates, never sufficient approval.
    # In particular, the current prospective weather validation/dataset output
    # deliberately carries False and must remain in research-only status.
    if value.get("paper_eligible") is not True or value.get("calibration_eligible") is not True:
        raise PredictionLifecycleError("research evidence is not paper/calibration eligible")
    ticker = _text(value.get("ticker"), "ticker")
    event_id = _text(value.get("event_id"), "event_id")
    outcome = _text(value.get("outcome"), "outcome").lower()
    if outcome not in {"yes", "no"}:
        raise PredictionLifecycleError("outcome must be yes or no")
    units = _integer(value.get("units"), "units", 1)
    venue = _text(value.get("venue"), "venue").lower()
    if venue != "kalshi":
        raise PredictionLifecycleError("only the reviewed Kalshi adapter is supported")
    decision_at = _time(value.get("decision_at"), "decision_at")
    if decision_at > now or (now - decision_at).total_seconds() > MAX_AGE_SECONDS:
        raise PredictionLifecycleError("decision timestamp is invalid or stale")
    review = _review(value.get("adapter_review"))
    evidence = value.get("evidence")
    if not isinstance(evidence, Mapping):
        raise PredictionLifecycleError("reviewed forecast, calibration, contract and cutoff evidence is required")
    evidence_rows: dict[str, Mapping[str, Any]] = {}
    hashes: dict[str, str] = {}
    for key in ("forecast", "calibration", "contract", "cutoff"):
        evidence_rows[key], hashes[key + "_hash"] = _raw(evidence.get(key), f"evidence.{key}")
        known = _time(evidence_rows[key].get("known_at"), f"evidence.{key}.known_at")
        if known > decision_at:
            raise PredictionLifecycleError(f"evidence.{key} was not known at decision time")
    contract = evidence_rows["contract"]
    if (contract.get("ticker"), contract.get("event_id"), contract.get("outcome")) != (ticker, event_id, outcome):
        raise PredictionLifecycleError("reviewed contract identity does not match proposal")
    terms = contract.get("terms")
    if (not isinstance(terms, Mapping) or terms.get("schema_version") != TERMS_SCHEMA_VERSION
            or set(terms) != {"schema_version", *TERM_FIELDS}
            or not _text(terms.get("rules_primary"), "contract rules_primary")):
        raise PredictionLifecycleError("reviewed contract terms are required")
    if (terms.get("ticker"), terms.get("event_ticker"), terms.get("market_type"), str(terms.get("notional_value_dollars"))) != (ticker, event_id, "binary", "1.0000"):
        raise PredictionLifecycleError("reviewed contract economic terms are unsupported")
    if contract.get("family") != "weather_daily_temperature":
        raise PredictionLifecycleError("initial lifecycle is restricted to reviewed weather contracts")
    station = _text(contract.get("station_identity"), "contract station_identity")
    observation_date = _text(contract.get("observation_date"), "contract observation_date")
    try: date.fromisoformat(observation_date)
    except ValueError as exc: raise PredictionLifecycleError("contract observation_date is invalid") from exc
    cutoff_row = evidence_rows["cutoff"]
    if (cutoff_row.get("ticker"), cutoff_row.get("event_id")) != (ticker, event_id):
        raise PredictionLifecycleError("reviewed cutoff identity does not match proposal")
    cutoff = _time(cutoff_row.get("cutoff_at"), "cutoff_at")
    if _time(terms.get("close_time"), "contract close_time") != cutoff:
        raise PredictionLifecycleError("reviewed cutoff differs from frozen contract close_time")
    if decision_at >= cutoff or now >= cutoff:
        raise PredictionLifecycleError("decision is at or after contract cutoff")
    protocol = evidence_rows["calibration"].get("calibration_protocol")
    if not isinstance(protocol, Mapping) or protocol.get("version") != "prediction-calibration-30dev-20untouched-v1":
        raise PredictionLifecycleError("reviewed calibration protocol is required")
    development = _integer(protocol.get("development_events"), "calibration development events", 30)
    untouched = _integer(protocol.get("untouched_events"), "calibration untouched events", 20)
    expected_manifest = {"ticker": ticker, "event_id": event_id, "outcome": outcome,
                         "decision_at": _iso(decision_at), "cutoff_at": _iso(cutoff), "evidence_hashes": hashes}
    if review["manifest"] != expected_manifest:
        raise PredictionLifecycleError("review manifest does not bind admission evidence")
    identity = {"source_kind": source_kind, "venue": venue, "ticker": ticker, "event_id": event_id,
                "outcome": outcome, "units": units}
    quote = _quote(value.get("quote"), now, "quote", identity=identity, role="entry")
    if _time(quote["received_at"], "quote.received_at") > decision_at:
        raise PredictionLifecycleError("entry quote arrived after decision")
    total = quote["ask_cents"] * units + quote["entry_fee_cents"]
    if total > 500:
        raise PredictionLifecycleError("prediction idea exceeds $5 cap")
    proposal_id = _text(value.get("proposal_id"), "proposal_id")
    expiry = now + timedelta(seconds=INTENT_SECONDS)
    return {"proposal_id": proposal_id, "strategy": _text(value.get("strategy"), "strategy"),
            "strategy_version": _text(value.get("strategy_version"), "strategy_version"),
            "ticker": ticker, "event_id": event_id, "venue": venue, "decision_at": _iso(decision_at),
            "cutoff_at": _iso(cutoff), "terms_hash": _digest(terms), "terms": dict(terms),
            "exposure_group": f"weather:{station}:{observation_date}",
            "outcome": outcome, "units": units, "source_kind": source_kind, "review": review,
            "evidence": {**hashes, "calibration_protocol": {"version": protocol["version"],
                         "development_events": development, "untouched_events": untouched}},
            "quote": quote, "total_cents": total, "expiry_at": _iso(min(expiry, cutoff))}


def _root(value: str | os.PathLike[str]) -> Path:
    path = Path(value)
    if path.exists() and path.is_symlink():
        raise PredictionLifecycleError("state directory symlink refused")
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path, 0o700)
    return path


def _write(path: Path, state: Mapping[str, Any]) -> None:
    if path.exists() and path.is_symlink():
        raise PredictionLifecycleError("state file symlink refused")
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            descriptor = -1
            stream.write(_canonical(state) + "\n")
            stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try: os.fsync(directory)
        finally: os.close(directory)
    finally:
        if descriptor >= 0: os.close(descriptor)
        with contextlib.suppress(FileNotFoundError): temporary.unlink()


def _state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"schema_version": SCHEMA_VERSION, "lifecycle_version": LIFECYCLE_VERSION, "history": []}
    if path.is_symlink() or not path.is_file():
        raise PredictionLifecycleError("state file is not regular")
    try: value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc: raise PredictionLifecycleError("state is unreadable") from exc
    if not isinstance(value, dict) or value.get("schema_version") != SCHEMA_VERSION or value.get("lifecycle_version") != LIFECYCLE_VERSION:
        raise PredictionLifecycleError("state schema is unsupported")
    return value


@contextlib.contextmanager
def _lock(root: Path):
    descriptor = os.open(root / "prediction.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    stream = os.fdopen(descriptor, "a+")
    try:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB); yield
    except BlockingIOError as exc: raise PredictionLifecycleError("already_running") from exc
    finally: stream.close()


def _proposal_payload(book: PaperLedger, proposal_id: str) -> Mapping[str, Any] | None:
    for event in book.audit():
        payload = event.payload.get("payload", {})
        if event.event_type == "reserve" and isinstance(payload, Mapping) and payload.get("proposal_id") == proposal_id:
            return payload
    return None


def _reconcile_pending(state: dict[str, Any], book: PaperLedger) -> bool:
    """Resolve a ledger mutation that committed before its final state write."""
    changed = False
    audits = tuple(book.audit())
    pending_reserve = state.get("pending_reserve")
    if isinstance(pending_reserve, Mapping):
        match = next((e for e in audits if e.event_type == "reserve"
                      and isinstance(e.payload.get("payload"), Mapping)
                      and e.payload["payload"].get("proposal_id") == pending_reserve.get("proposal_id")), None)
        if match is not None:
            state["reservation_id"] = match.entity_id
            state["history"].append({"type": "reserve_recovered", "reservation_id": match.entity_id,
                                     "evidence": dict(pending_reserve)})
            state.pop("pending_reserve", None); changed = True
    for key, event_type, identity_key, history_type in (
        ("pending_fill", "fill", "fill_id", "fill_recovered"),
        ("pending_close", "close", "close_id", "close_recovered"),
        ("pending_settlement", "close", "close_id", "settlement_recovered"),
    ):
        pending = state.get(key)
        if not isinstance(pending, Mapping):
            continue
        identity = pending.get(identity_key)
        if any(e.event_type == event_type and e.entity_id == identity for e in audits):
            state["history"].append({"type": history_type, "evidence": dict(pending)})
            state.pop(key, None)
            if event_type == "close": state.pop("close_intent", None)
            changed = True
    pending_cancel = state.get("pending_cancel")
    if isinstance(pending_cancel, Mapping):
        try: reservation = book.get_reservation(pending_cancel.get("reservation_id"))
        except LedgerError: reservation = None
        if reservation is not None and reservation.state == "cancelled":
            state["history"].append({"type": "cancel_recovered", "evidence": dict(pending_cancel)})
            state.pop("pending_cancel", None); changed = True
    return changed


def _own(book: PaperLedger) -> tuple[list[Any], list[Any]]:
    reservations, positions = [], []
    for event in book.audit():
        if event.event_type != "reserve": continue
        payload = event.payload.get("payload", {})
        marker = payload.get("metadata", {}).get(MARKER, {}) if isinstance(payload, Mapping) else {}
        if not isinstance(marker, Mapping) or marker.get("lifecycle_version") != LIFECYCLE_VERSION: continue
        reservation = book.get_reservation(event.entity_id)
        if reservation.state == "reserved": reservations.append(reservation)
        for audit in book.audit():
            if audit.event_type == "fill" and audit.payload.get("reservation_id") == reservation.reservation_id:
                position = book.get_position(audit.payload["position_id"])
                if position.state == "open": positions.append(position)
    return reservations, positions


def _identity(marker: Mapping[str, Any]) -> dict[str, Any]:
    return {key: marker[key] for key in ("source_kind", "venue", "ticker", "event_id", "outcome", "units")}


def _marks(book: PaperLedger, provider: Callable[[PaperLedger, datetime], Any] | None, now: datetime) -> Any:
    if provider is None: raise PredictionLifecycleError("global_marks_missing")
    try: marks = provider(book, now)
    except Exception as exc: raise PredictionLifecycleError("global_marks_unavailable") from exc
    if not isinstance(marks, (list, tuple)): raise PredictionLifecycleError("global_marks_incomplete")
    try: return book.record_marks("prediction-marks-" + _digest({"at": _iso(now), "marks": [repr(x) for x in marks]})[:32], list(marks), as_of=now)
    except LedgerError as exc: raise PredictionLifecycleError("global_marks_incomplete") from exc


def _assert_firm_book(book: PaperLedger) -> None:
    if not book.enforce_market_risk:
        raise PredictionLifecycleError("shared market risk enforcement is required")
    limits = book.limits
    if (limits.per_idea_cents.get("prediction", 0) > 500 or limits.group_cap_cents > 3_000
            or limits.aggregate_cap_cents > 8_000):
        raise PredictionLifecycleError("shared ledger risk ceilings are too loose")
    balances = book.balances()
    prediction = next((row for row in balances if row.desk == "prediction"), None)
    if prediction is None or prediction.initial_cash_cents > 10_000 or sum(row.initial_cash_cents for row in balances) > 80_000:
        raise PredictionLifecycleError("shared ledger capital allocation is too loose")


def position_mark(book: PaperLedger, position: Any, now: datetime, quote: Mapping[str, Any]) -> MarketMark:
    stored = book.get_position(position.position_id)
    if stored != position or position.state != "open": raise PredictionLifecycleError("position is not open")
    payload = _proposal_payload(book, position.proposal_id)
    marker = payload.get("metadata", {}).get(MARKER, {}) if isinstance(payload, Mapping) else {}
    if (not isinstance(marker, Mapping) or marker.get("lifecycle_version") != LIFECYCLE_VERSION
            or payload.get("desk") != "prediction" or payload.get("proposal_id") != marker.get("proposal_id")
            or payload.get("strategy") != marker.get("strategy")
            or payload.get("strategy_version") != marker.get("strategy_version")
            or payload.get("instrument") != marker.get("ticker")
            or payload.get("provenance") != marker.get("source_kind")):
        raise PredictionLifecycleError("position identity is unsupported")
    clean = _quote(quote, now, "mark_quote", identity=_identity(marker), role="mark")
    if _time(clean["timestamp"], "mark timestamp") < _time(position.opened_at, "position opened"):
        raise PredictionLifecycleError("mark quote precedes position opening")
    units = _integer(marker.get("units"), "stored units", 1)
    gross = clean["bid_cents"] * units
    if clean["exit_fee_cents"] > gross:
        raise PredictionLifecycleError("liability_reconciliation_required")
    value = gross - clean["exit_fee_cents"]
    return MarketMark(position.position_id, value, clean["timestamp"], clean["raw_sha256"], payload["provenance"])


def _settlement(value: Any, marker: Mapping[str, Any], now: datetime) -> dict[str, Any]:
    if not isinstance(value, Mapping): raise PredictionLifecycleError("settlement evidence is required")
    review = _review(value.get("adapter_review"), settlement=True)
    wrapper = value.get("wrapper")
    if not isinstance(wrapper, Mapping): raise PredictionLifecycleError("settlement wrapper is required")
    received = _time(wrapper.get("received_at"), "settlement.received_at")
    if received > now: raise PredictionLifecycleError("settlement receipt is in the future")
    supplied = _hash(wrapper.get("source_hash"), "settlement.source_hash")
    try: raw = base64.b64decode(_text(wrapper.get("raw_response_b64"), "raw_response_b64"), validate=True)
    except Exception as exc: raise PredictionLifecycleError("settlement raw response is invalid base64") from exc
    if hashlib.sha256(raw).hexdigest() != supplied: raise PredictionLifecycleError("settlement raw hash mismatch")
    try: decoded = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc: raise PredictionLifecycleError("settlement raw response is invalid JSON") from exc
    markets = decoded.get("markets") if isinstance(decoded, Mapping) else None
    if not isinstance(markets, list): raise PredictionLifecycleError("settlement markets are missing")
    matches = [m for m in markets if isinstance(m, Mapping) and m.get("ticker") == marker.get("ticker")]
    if len(matches) != 1: raise PredictionLifecycleError("settlement must identify exactly one held ticker")
    market = matches[0]
    if market.get("event_ticker") != marker.get("event_id"):
        raise PredictionLifecycleError("settlement event identity mismatch")
    settled_terms = {"schema_version": TERMS_SCHEMA_VERSION, **{key: market.get(key) for key in TERM_FIELDS}}
    if _digest(settled_terms) != marker.get("terms_hash"):
        raise PredictionLifecycleError("settlement terms differ from frozen admission terms")
    expected_manifest = {"ticker": marker.get("ticker"), "event_id": marker.get("event_id"),
                         "outcome": marker.get("outcome"), "terms_hash": marker.get("terms_hash"),
                         "source_hash": supplied, "received_at": _iso(received)}
    if review["manifest"] != expected_manifest:
        raise PredictionLifecycleError("settlement review manifest does not bind final evidence")
    if market.get("status") != "finalized" or market.get("is_provisional") is True:
        raise PredictionLifecycleError("settlement is not exact final venue evidence")
    if any(market.get(key) not in (None, False, "") for key in ("disputed", "held", "corrected")):
        raise PredictionLifecycleError("settlement is disputed, held, or corrected")
    result = market.get("result")
    if result not in {"yes", "no"}: raise PredictionLifecycleError("settlement result is not binary")
    try: payout = Decimal(str(market.get("settlement_value_dollars")))
    except (InvalidOperation, ValueError) as exc: raise PredictionLifecycleError("settlement payout is invalid") from exc
    if not payout.is_finite() or payout not in {Decimal(0), Decimal(1)}:
        raise PredictionLifecycleError("only reconciled ordinary binary settlement is supported")
    venue_yes_payout = Decimal(1) if result == "yes" else Decimal(0)
    if payout != venue_yes_payout: raise PredictionLifecycleError("settlement payout conflicts with venue result")
    held_payout = Decimal(1) if result == marker.get("outcome") else Decimal(0)
    settled = _time(market.get("settlement_ts"), "settlement_ts")
    if settled > received: raise PredictionLifecycleError("settlement timestamp follows receipt")
    if settled < _time(marker.get("cutoff_at"), "stored cutoff"):
        raise PredictionLifecycleError("settlement precedes contract cutoff")
    return {"source_hash": supplied, "received_at": _iso(received), "settlement_ts": _iso(settled),
            "ticker": market["ticker"], "result": result, "venue_yes_payout": str(payout),
            "held_payout": str(held_payout),
            "expiration_value": market.get("expiration_value"), "raw_response_b64": wrapper["raw_response_b64"],
            "review": review}


def run(
    book: PaperLedger, state_dir: str | os.PathLike[str], *, now: datetime | str,
    admission: Mapping[str, Any] | None = None, later_quote: Mapping[str, Any] | None = None,
    request_close: bool = False, settlement: Mapping[str, Any] | None = None,
    mark_provider: Callable[[PaperLedger, datetime], Any] | None = None,
    allow_entry: bool = True, recovery_ok: bool = True,
) -> dict[str, Any]:
    """Advance at most one mutation in the persistent prediction lifecycle."""
    if not isinstance(book, PaperLedger): raise PredictionLifecycleError("book must be a PaperLedger")
    current = _time(now, "now"); root = _root(state_dir); path = root / "prediction_state.json"
    with _lock(root):
        state = _state(path); actions: list[dict[str, Any]] = []; blocker = None
        reservations, positions = _own(book)

        if _reconcile_pending(state, book):
            _write(path, state)

        reservations, positions = _own(book)
        if (not allow_entry or not recovery_ok or book.risk_stop_active()) and reservations:
            for reservation in reservations:
                state["pending_cancel"] = {"reservation_id": reservation.reservation_id, "at": _iso(current)}; _write(path, state)
                result = book.cancel(reservation.reservation_id, reason="STOP or recovery entry pause", cancelled_at=current)
                state["history"].append({"type": "cancel", "result": asdict(result)}); state.pop("pending_cancel", None)
                actions.append({"type": "cancel", "result": asdict(result)})
            _write(path, state); return {"actions": actions, "blocker": "entries_disabled", "state": state}
        for reservation in reservations:
            if _time(reservation.expiry_at, "expiry") <= current:
                state["pending_cancel"] = {"reservation_id": reservation.reservation_id, "at": _iso(current)}; _write(path, state)
                result = book.cancel(reservation.reservation_id, reason="prediction intent expired", cancelled_at=current)
                state["history"].append({"type": "cancel", "result": asdict(result)}); state.pop("pending_cancel", None)
                actions.append({"type": "cancel", "result": asdict(result)})
                _write(path, state); return {"actions": actions, "blocker": "intent_expired", "state": state}

        reservations, positions = _own(book)
        if positions:
            position = positions[0]; payload = _proposal_payload(book, position.proposal_id); marker = payload["metadata"][MARKER]
            intent = state.get("close_intent")
            if isinstance(intent, Mapping) and intent.get("position_id") != position.position_id:
                raise PredictionLifecycleError("close intent position mismatch")
            if settlement is not None:
                proof = _settlement(settlement, marker, current)
                if _time(proof["settlement_ts"], "settlement timestamp") < _time(position.opened_at, "position opened_at"):
                    raise PredictionLifecycleError("settlement precedes position opening")
                close_id = "prediction-settle-" + position.position_id
                units = _integer(marker["units"], "stored units", 1)
                evidence = {"close_id": close_id, "position_id": position.position_id, "proof": proof}
                state["pending_settlement"] = evidence; _write(path, state)
                result = book.settle(position.position_id, close_id=close_id, proceeds_cents=units * int(Decimal(proof["held_payout"]) * 100), fee_cents=0, provenance=payload["provenance"], closed_at=current)
                state["history"].append({"type": "settlement", "result": asdict(result), "evidence": evidence}); state.pop("pending_settlement", None)
                _write(path, state); return {"actions": [{"type": "settlement", "result": asdict(result)}], "blocker": None, "state": state}
            marks_error = None
            try: _marks(book, mark_provider, current)
            except PredictionLifecycleError as exc: marks_error = str(exc)
            if request_close and not isinstance(intent, Mapping):
                if later_quote is None:
                    raise PredictionLifecycleError("close trigger quote is required")
                trigger = _quote(later_quote, current, "exit_trigger_quote", identity=_identity(marker), role="mark")
                trigger_hash = trigger["raw_sha256"]; trigger_time = trigger["timestamp"]
                intent = {"position_id": position.position_id, "created_at": _iso(current),
                          "expires_at": _iso(current + timedelta(seconds=INTENT_SECONDS)),
                          "trigger_hash": trigger_hash, "trigger_time": trigger_time}
                state["close_intent"] = intent; _write(path, state)
                return {"actions": [{"type": "pending_close", "intent": intent}], "blocker": "awaiting_independent_exit_quote", "global_marks_blocker": marks_error, "state": state}
            if isinstance(intent, Mapping):
                if _time(intent["expires_at"], "close expiry") <= current:
                    state.pop("close_intent", None); _write(path, state)
                    return {"actions": [], "blocker": "close_intent_expired", "state": state}
                if later_quote is None: return {"actions": [], "blocker": "awaiting_independent_exit_quote", "state": state}
                quote = _quote(later_quote, current, "exit_quote", identity=_identity(marker), role="exit")
                if (_time(quote["timestamp"], "exit timestamp") < _time(intent["created_at"], "intent created") + timedelta(seconds=DELAY_SECONDS)
                        or quote["raw_sha256"] == intent.get("trigger_hash")):
                    return {"actions": [], "blocker": "awaiting_independent_exit_quote", "state": state}
                units = _integer(marker["units"], "stored units", 1)
                if quote["exit_fee_cents"] > quote["bid_cents"] * units:
                    raise PredictionLifecycleError("liability_reconciliation_required")
                close_id = "prediction-close-" + position.position_id
                evidence = {"position_id": position.position_id, "close_id": close_id, "quote": quote, "intent": dict(intent)}
                state["pending_close"] = evidence; _write(path, state)
                result = book.close(position.position_id, close_id=close_id, proceeds_cents=quote["bid_cents"] * units,
                                    fee_cents=quote["exit_fee_cents"], provenance=payload["provenance"], closed_at=current)
                state["history"].append({"type": "close", "result": asdict(result), "evidence": evidence})
                state.pop("pending_close", None); state.pop("close_intent", None); _write(path, state)
                return {"actions": [{"type": "close", "result": asdict(result)}], "blocker": marks_error, "state": state}
            return {"actions": [], "blocker": marks_error, "state": state}

        if reservations:
            reservation = reservations[0]
            if later_quote is None: return {"actions": [], "blocker": "awaiting_independent_fill_quote", "state": state}
            payload = _proposal_payload(book, reservation.proposal_id); marker = payload["metadata"][MARKER]
            quote = _quote(later_quote, current, "fill_quote", identity=_identity(marker), role="fill")
            if (_time(quote["timestamp"], "fill timestamp") < _time(reservation.created_at, "created") + timedelta(seconds=DELAY_SECONDS)
                    or quote["raw_sha256"] == marker["entry_quote_hash"]):
                return {"actions": [], "blocker": "awaiting_independent_fill_quote", "state": state}
            units = _integer(marker["units"], "stored units", 1)
            if current >= _time(marker["cutoff_at"], "stored cutoff"):
                raise PredictionLifecycleError("fill is at or after contract cutoff")
            total = quote["ask_cents"] * units + quote["entry_fee_cents"]
            if total > reservation.reserved_cents: raise PredictionLifecycleError("fill cost exceeds reservation")
            _marks(book, mark_provider, current)
            fill_id = "prediction-fill-" + reservation.reservation_id
            evidence = {"fill_id": fill_id, "reservation_id": reservation.reservation_id, "quote": quote}
            state["pending_fill"] = evidence; _write(path, state)
            result = book.simulate_fill(reservation.reservation_id, fill_id=fill_id, quantity=1,
                                        unit_cost_cents=quote["ask_cents"] * units, fee_cents=quote["entry_fee_cents"],
                                        provenance=payload["provenance"], filled_at=current)
            state["history"].append({"type": "fill", "result": asdict(result), "evidence": evidence}); state.pop("pending_fill", None)
            _write(path, state); return {"actions": [{"type": "fill", "result": asdict(result)}], "blocker": None, "state": state}

        if not allow_entry or not recovery_ok or book.risk_stop_active():
            return {"actions": [], "blocker": "entries_disabled", "state": state}
        if admission is None: return {"actions": [], "blocker": "admission_gate_missing", "state": state}
        _assert_firm_book(book)
        clean = _admission(admission, current)
        if _proposal_payload(book, clean["proposal_id"]) is not None:
            return {"actions": [], "blocker": "proposal_already_attempted", "state": state}
        _marks(book, mark_provider, current)
        marker = {"lifecycle_version": LIFECYCLE_VERSION, "proposal_id": clean["proposal_id"],
                  "strategy": clean["strategy"], "strategy_version": clean["strategy_version"],
                  "source_kind": clean["source_kind"], "ticker": clean["ticker"], "event_id": clean["event_id"],
                  "outcome": clean["outcome"], "units": clean["units"], "entry_quote_hash": clean["quote"]["raw_sha256"],
                  "venue": clean["venue"], "cutoff_at": clean["cutoff_at"], "terms_hash": clean["terms_hash"],
                  "terms": clean["terms"],
                  "review": clean["review"], "evidence": clean["evidence"], "synthetic": clean["source_kind"] == "synthetic"}
        proposal = Proposal(clean["proposal_id"], "prediction", clean["strategy"], clean["strategy_version"],
                            clean["quote"]["raw_sha256"], clean["quote"]["timestamp"], clean["ticker"],
                            clean["exposure_group"], "prediction-contract:" + clean["ticker"],
                            clean["expiry_at"], 1, clean["quote"]["ask_cents"] * clean["units"],
                            clean["quote"]["entry_fee_cents"], clean["source_kind"], {MARKER: marker})
        state["pending_reserve"] = {"proposal_id": clean["proposal_id"], "admission": clean}; _write(path, state)
        result = book.reserve(proposal, now=current)
        state["reservation_id"] = result.reservation_id; state["history"].append({"type": "reserve", "result": asdict(result), "admission": clean})
        state.pop("pending_reserve", None); _write(path, state)
        return {"actions": [{"type": "reserve", "result": asdict(result)}], "blocker": "awaiting_independent_fill_quote", "state": state}


__all__ = ["PredictionLifecycleError", "LIFECYCLE_VERSION", "position_mark", "run"]
