"""Bounded CR-001 daily crypto research decision recorder.

This module makes one read-only Alpaca crypto-bars request per UTC day and
combines those completed bars with the latest fresh BTC quote already stored
by the crypto recorder.  It evaluates the copied CR-001 policy for an
explicitly flat, research-only portfolio.  It never talks to a broker, sends
an order, mutates the paper ledger, or claims a profit.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
import fcntl
import hashlib
import json
import os
from pathlib import Path
import secrets
import sqlite3
import urllib.parse
import urllib.request
from typing import Any, Callable, Mapping

from .alpaca_paper import (
    KEYCHAIN_KEY_ID_SERVICE,
    KEYCHAIN_SECRET_SERVICE,
    _NoRedirectHandler,
)
from .recorder import credentials
from desks.crypto.trend_policy import POLICY_ID, POLICY_VERSION, evaluate


UTC = timezone.utc
DEFAULT_STATE = "/var/lib/neorome-crypto-daily"
DEFAULT_OBSERVATION_STATE = "/var/lib/neorome-crypto-recorder"
ENDPOINT_PATH = "/v1beta3/crypto/us/bars"
ENDPOINT_HOST = "data.alpaca.markets"
SYMBOL = "BTC/USD"
TIMEFRAME = "1Day"
REQUEST_LIMIT = 200
REQUIRED_DAYS = 102
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
MAX_STATE_BYTES = 256 * 1024 * 1024
QUOTE_MAX_AGE_SECONDS = 60


class DailyRecorderError(ValueError):
    """A safe, non-sensitive error suitable for a status result."""


class PaginationRejected(DailyRecorderError):
    """The provider response requires pagination, which is not permitted."""


class StateError(DailyRecorderError):
    """The private state layout is missing, unsafe, or over its bound."""


class BarsResponseError(DailyRecorderError):
    """A parsed bars response failed validation; payload is safe diagnostics."""

    def __init__(self, message: str, payload: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.payload = payload


def _now(value: datetime | None) -> datetime:
    current = value or datetime.now(UTC)
    if current.tzinfo is None or current.utcoffset() is None:
        raise DailyRecorderError("now must be timezone-aware")
    return current.astimezone(UTC)


def _timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise DailyRecorderError(f"{field} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DailyRecorderError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise DailyRecorderError(f"{field} must include a timezone")
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _number(value: Any, field: str) -> str:
    if isinstance(value, bool) or value is None:
        raise DailyRecorderError(f"{field} must be numeric")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise DailyRecorderError(f"{field} must be numeric") from exc
    if not number.is_finite() or number <= 0:
        raise DailyRecorderError(f"{field} must be finite and positive")
    return str(number)


def _diagnostic_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Retain only bar fields; provider metadata must not enter evidence."""
    grouped = payload.get("bars")
    safe_bars: list[dict[str, Any] | None] = []
    if isinstance(grouped, Mapping) and isinstance(grouped.get(SYMBOL), list):
        for raw in grouped[SYMBOL]:
            if isinstance(raw, Mapping):
                safe_bars.append({key: raw.get(key) for key in ("t", "o", "h", "l", "c")})
            else:
                safe_bars.append(None)
    return {
        "bars": {SYMBOL: safe_bars},
        "next_page_token_present": payload.get("next_page_token") not in (None, ""),
    }


def _ensure_private_root(path: str | os.PathLike[str]) -> Path:
    root = Path(path)
    if root.exists() and root.is_symlink():
        raise StateError("symlink state directory refused")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if root.is_symlink() or not root.is_dir():
        raise StateError("state directory is not a private directory")
    try:
        os.chmod(root, 0o700)
    except OSError as exc:
        raise StateError("state directory permissions unavailable") from exc
    return root


def _state_size(root: Path) -> int:
    total = 0
    for path in root.rglob("*"):
        if path.is_symlink():
            raise StateError("symlink in state directory refused")
        if path.is_file():
            total += path.stat().st_size
    return total


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise StateError("symlink or non-file state entry refused")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise StateError("state JSON is unreadable") from exc
    if not isinstance(value, dict):
        raise StateError("state JSON must be an object")
    return value


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    if path.exists() and path.is_symlink():
        raise StateError("symlink output refused")
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(temporary, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
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
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _request_bars(
    *,
    now: datetime,
    opener: Any | None = None,
    credential_loader: Callable[[str], str] = credentials,
) -> tuple[list[dict[str, Any]], dict[str, Any], Mapping[str, Any]]:
    """Fetch and normalize one fixed, non-paginated BTC/USD response."""
    key_id = credential_loader(KEYCHAIN_KEY_ID_SERVICE)
    secret_key = credential_loader(KEYCHAIN_SECRET_SERVICE)
    if not isinstance(key_id, str) or not key_id or not isinstance(secret_key, str) or not secret_key:
        raise DailyRecorderError("credentials unavailable")
    query = urllib.parse.urlencode(
        {
            "symbols": SYMBOL,
            "timeframe": TIMEFRAME,
            "start": f"{(now.date() - timedelta(days=REQUIRED_DAYS)).isoformat()}T00:00:00Z",
            # The end boundary is the current UTC midnight.  A provider may
            # still return an in-progress current-day bar, which is dropped
            # below rather than treated as a completed observation.
            "end": f"{now.date().isoformat()}T00:00:00Z",
            "limit": str(REQUEST_LIMIT),
            "sort": "asc",
        }
    )
    request = urllib.request.Request(
        f"https://{ENDPOINT_HOST}{ENDPOINT_PATH}?{query}",
        headers={"APCA-API-KEY-ID": key_id, "APCA-API-SECRET-KEY": secret_key},
        method="GET",
    )
    client = opener or urllib.request.build_opener(_NoRedirectHandler())
    try:
        response = client.open(request, timeout=30)
        status = getattr(response, "status", None)
        if status is None:
            status = response.getcode()
        body = response.read(MAX_RESPONSE_BYTES + 1)
        close = getattr(response, "close", None)
        if close:
            close()
    except Exception as exc:
        # Do not retain exception text: urllib errors may contain URL details.
        raise DailyRecorderError("bars request failed") from exc
    if status != 200:
        raise DailyRecorderError("bars request returned a non-success status")
    if len(body) > MAX_RESPONSE_BYTES:
        raise DailyRecorderError("bars response exceeds 2 MiB")
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DailyRecorderError("bars response is not valid JSON") from exc
    if not isinstance(payload, Mapping):
        raise DailyRecorderError("bars response must be an object")
    try:
        return _normalize_payload(payload, now)
    except Exception as exc:
        setattr(exc, "payload", _diagnostic_payload(payload))
        raise


def _normalize_payload(payload: Mapping[str, Any], now: datetime) -> tuple[list[dict[str, Any]], dict[str, Any], Mapping[str, Any]]:
    """Validate a parsed provider payload; payload contains no credentials."""
    token = payload.get("next_page_token")
    if token not in (None, ""):
        raise PaginationRejected("pagination token present")
    grouped = payload.get("bars")
    if not isinstance(grouped, Mapping) or set(grouped) != {SYMBOL}:
        raise DailyRecorderError("response must contain only BTC/USD bars")
    source_bars = grouped.get(SYMBOL)
    if not isinstance(source_bars, list):
        raise DailyRecorderError("BTC/USD bars must be a list")
    normalized: list[dict[str, Any]] = []
    for index, raw in enumerate(source_bars):
        if not isinstance(raw, Mapping):
            raise DailyRecorderError(f"bar {index} is malformed")
        timestamp = _timestamp(raw.get("t"), f"bars[{index}].t")
        if timestamp.time() != time.min:
            raise DailyRecorderError("daily bars must be UTC midnight")
        normalized.append(
            {
                "timestamp": _iso(timestamp),
                "complete": True,
                "open": _number(raw.get("o"), f"bars[{index}].o"),
                "high": _number(raw.get("h"), f"bars[{index}].h"),
                "low": _number(raw.get("l"), f"bars[{index}].l"),
                "close": _number(raw.get("c"), f"bars[{index}].c"),
            }
        )
    current_midnight = datetime.combine(now.date(), time.min, tzinfo=UTC)
    if any(_timestamp(row["timestamp"], "bar.timestamp") > current_midnight for row in normalized):
        raise DailyRecorderError("bars cannot include a future UTC day")
    completed = [row for row in normalized if _timestamp(row["timestamp"], "bar.timestamp") < current_midnight]
    if len(completed) < REQUIRED_DAYS:
        raise DailyRecorderError("response has fewer than 102 completed bars")
    # Policy validation rejects gaps, duplicates, current-day bars, and stale
    # history.  Validate the provider sequence before narrowing to the 102-day
    # research window so malformed older data cannot be silently skipped.
    from desks.crypto.trend_policy import validate_bars

    return list(validate_bars(completed, now=now, minimum_days=REQUIRED_DAYS))[-REQUIRED_DAYS:], {
        "source_kind": "observed",
        "provider": "alpaca_crypto_bars",
        "path": ENDPOINT_PATH,
        "symbol": SYMBOL,
        "timeframe": TIMEFRAME,
        "requested_limit": REQUEST_LIMIT,
        "selected_completed_days": REQUIRED_DAYS,
    }, _diagnostic_payload(payload)


def _fetch_bars(
    now: datetime, *, opener: Any | None = None, credential_loader: Callable[[str], str] = credentials
) -> tuple[list[dict[str, Any]], dict[str, Any], Mapping[str, Any]]:
    return _request_bars(now=now, opener=opener, credential_loader=credential_loader)


def _quote_from_observation(
    observation_state: str | os.PathLike[str], now: datetime
) -> tuple[dict[str, str] | None, str, dict[str, Any]]:
    root = Path(observation_state)
    if root.exists() and root.is_symlink():
        raise StateError("observation state symlink refused")
    database = root / "recorder.sqlite3"
    if not database.exists():
        return None, "missing", {"source_kind": "observed", "state": str(root), "status": "missing"}
    if database.is_symlink() or not database.is_file():
        raise StateError("observation database symlink or non-file refused")
    try:
        with sqlite3.connect(f"file:{database.resolve()}?mode=ro", uri=True) as connection:
            row = connection.execute(
                "SELECT id, observation, sha256 FROM attempts "
                "WHERE status = 'fresh' ORDER BY id DESC LIMIT 1"
            ).fetchone()
    except sqlite3.Error as exc:
        raise StateError("observation database unreadable") from exc
    if row is None or not row[1]:
        return None, "missing", {"source_kind": "observed", "state": str(root), "status": "missing"}
    attempt_id, encoded, digest = row
    if not isinstance(encoded, str):
        raise StateError("observation is malformed")
    actual_digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    if digest and digest != actual_digest:
        raise StateError("observation checksum mismatch")
    try:
        observation = json.loads(encoded)
    except json.JSONDecodeError as exc:
        raise StateError("observation is not valid JSON") from exc
    if not isinstance(observation, Mapping):
        raise StateError("observation must be an object")
    raw_response = observation.get("raw_response")
    quotes = raw_response.get("quotes") if isinstance(raw_response, Mapping) else None
    raw = quotes.get(SYMBOL) if isinstance(quotes, Mapping) else None
    if not isinstance(raw, Mapping):
        raise StateError("fresh observation has no BTC/USD quote")
    timestamp = _timestamp(raw.get("t", raw.get("timestamp")), "quote.timestamp")
    age = (now - timestamp).total_seconds()
    if age < 0:
        return None, "future", {"source_kind": "observed", "state": str(root), "attempt_id": attempt_id, "status": "future"}
    if age > QUOTE_MAX_AGE_SECONDS:
        return None, "stale", {"source_kind": "observed", "state": str(root), "attempt_id": attempt_id, "status": "stale", "age_seconds": age}
    bid = _number(raw.get("bp", raw.get("bid")), "quote.bid")
    ask = _number(raw.get("ap", raw.get("ask")), "quote.ask")
    bid_depth = _number(raw.get("bs", raw.get("bid_size", raw.get("depth_units"))), "quote.bid_size")
    ask_depth = _number(raw.get("as", raw.get("ask_size", raw.get("depth_units"))), "quote.ask_size")
    if Decimal(bid) > Decimal(ask):
        raise StateError("fresh BTC/USD quote is crossed")
    depth = min(Decimal(bid_depth), Decimal(ask_depth))
    if depth <= 0:
        raise StateError("fresh BTC/USD quote has no depth")
    quote = {"symbol": SYMBOL, "timestamp": _iso(timestamp), "bid": bid, "ask": ask, "depth_units": str(depth)}
    source = {
        "source_kind": "observed",
        "state": str(root),
        "attempt_id": attempt_id,
        "sha256": actual_digest,
        "status": "fresh",
        "age_seconds": age,
    }
    return quote, "fresh", source


def _result_for_replay(marker: Mapping[str, Any], today: date) -> dict[str, Any]:
    marker_date = marker.get("date")
    if marker_date == today.isoformat():
        status = "already_attempted" if marker.get("status") == "completed" else "incomplete_replay"
        return {"status": status, "execution_enabled": False, "orders_sent": 0}
    if isinstance(marker_date, str) and marker_date > today.isoformat():
        return {"status": "clock_rollback", "execution_enabled": False, "orders_sent": 0}
    return {}


def run_once(
    state: str = DEFAULT_STATE,
    observation_state: str = DEFAULT_OBSERVATION_STATE,
    *,
    now: datetime | None = None,
    opener: Any | None = None,
    credential_loader: Callable[[str], str] = credentials,
) -> dict[str, Any]:
    """Run one bounded daily research decision and return a JSON-shaped result."""
    current = _now(now)
    root = _ensure_private_root(state)
    lock_path = root / "daily.lock"
    if lock_path.exists() and lock_path.is_symlink():
        raise StateError("daily lock symlink refused")
    with lock_path.open("a+") as lock:
        os.chmod(lock_path, 0o600)
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {"status": "already_running", "execution_enabled": False, "orders_sent": 0}
        marker_path = root / "attempt.json"
        marker = _read_json(marker_path)
        if marker:
            replay = _result_for_replay(marker, current.date())
            if replay:
                return replay
        if _state_size(root) >= MAX_STATE_BYTES:
            return {"status": "storage_limit", "execution_enabled": False, "orders_sent": 0}
        attempt_id = secrets.token_hex(16)
        started_at = _iso(current)
        _atomic_json(
            marker_path,
            {"schema_version": 1, "attempt_id": attempt_id, "date": current.date().isoformat(), "started_at": started_at, "status": "incomplete"},
        )
        outcome: dict[str, Any]
        evidence: dict[str, Any] = {
            "schema_version": 1,
            "attempt_id": attempt_id,
            "attempted_at": started_at,
            "mode": "flat_research_only",
            "policy_id": POLICY_ID,
            "policy_version": POLICY_VERSION,
            "assumed_position_units": "0",
            "source_kind": "observed",
            "execution_enabled": False,
            "orders_sent": 0,
            "ledger_mutations": 0,
            "profit_claim": False,
        }
        try:
            bars, bars_source, bars_payload = _fetch_bars(current, opener=opener, credential_loader=credential_loader)
            evaluation_now = current if now is not None else _now(None)
            quote, quote_status, quote_source = _quote_from_observation(observation_state, evaluation_now)
            decision = evaluate(bars, quote, now=evaluation_now, symbol=SYMBOL, position_units=0)
            evidence.update({"evaluation_at": _iso(evaluation_now), "bars_source": bars_source, "bars_response": bars_payload, "bars": bars, "quote_source": quote_source, "quote": quote, "decision": decision})
            outcome = {"status": decision.get("status", "abstain"), "decision": decision, "execution_enabled": False, "orders_sent": 0}
            evidence["quote_status"] = quote_status
            final_status = "completed"
        except Exception as exc:
            # Persist only a class name.  It is deliberately unsafe to expose
            # urllib or credential-bearing exception strings in status output.
            error_type = type(exc).__name__
            evidence["error_type"] = error_type
            payload = getattr(exc, "payload", None)
            if isinstance(payload, Mapping):
                evidence["bars_response"] = payload
            outcome = {"status": "error", "error_type": error_type, "execution_enabled": False, "orders_sent": 0}
            final_status = "failed"
        evidence["completed_at"] = _iso(_now(None))
        evidence_path = root / f"decision-{current.date().isoformat()}.json"
        _atomic_json(evidence_path, evidence)
        _atomic_json(
            marker_path,
            {"schema_version": 1, "attempt_id": attempt_id, "date": current.date().isoformat(), "started_at": started_at, "completed_at": evidence["completed_at"], "status": final_status, "decision_path": evidence_path.name},
        )
        outcome["attempt_id"] = attempt_id
        outcome["decision_path"] = evidence_path.name
        return outcome


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--observation-state", default=DEFAULT_OBSERVATION_STATE)
    args = parser.parse_args(argv)
    try:
        result = run_once(args.state, args.observation_state)
    except Exception as exc:
        result = {"status": "error", "error_type": type(exc).__name__, "execution_enabled": False, "orders_sent": 0}
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0 if result["status"] in {"entry_candidate", "exit_candidate", "abstain", "already_attempted", "incomplete_replay", "already_running"} else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["DEFAULT_OBSERVATION_STATE", "DEFAULT_STATE", "MAX_RESPONSE_BYTES", "MAX_STATE_BYTES", "run_once", "main"]
