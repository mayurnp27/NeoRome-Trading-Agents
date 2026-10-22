"""Bounded public Kalshi KXHIGHNY market evidence.

This module performs two unauthenticated GETs per claimed UTC hour: the open
KXHIGHNY markets and settled KXHIGHNY markets closed in the prior fourteen
days.  It archives venue responses and labels only.  It does not derive
probabilities, calibrate forecasts, settle a ledger, or place orders.
"""

from __future__ import annotations

import argparse
import base64
from datetime import datetime, timedelta, timezone
import fcntl
import hashlib
import gzip
import json
import os
from pathlib import Path
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable, Mapping

from .equity_data import NoRedirect


UTC = timezone.utc
ORIGIN = "https://api.elections.kalshi.com"
PATH = "/trade-api/v2/markets"
SERIES_TICKER = "KXHIGHNY"
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
MAX_STATE_BYTES = 256 * 1024 * 1024
MAX_MARKETS = 100
MAX_REQUESTS = 2
LOOKBACK_SECONDS = 14 * 24 * 60 * 60


class PredictionEvidenceError(Exception):
    """Safe public-collector error; no response body is persisted in errors."""


class StorageLimitError(PredictionEvidenceError):
    pass


def _aware(value: datetime | None, label: str = "time") -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise PredictionEvidenceError(f"{label} must be timezone aware")
    return value.astimezone(UTC)


def _parse_time(value: Any, label: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{label} is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return _aware(parsed, label)
    except (TypeError, ValueError, PredictionEvidenceError):
        raise ValueError(f"{label} is invalid") from None


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError):
        raise PredictionEvidenceError("Response is not valid JSON") from None


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _hour_start(value: datetime) -> datetime:
    current = _aware(value)
    return current.replace(minute=0, second=0, microsecond=0)


def _validate_market_rows(payload: Mapping[str, Any], status: str) -> list[dict[str, Any]]:
    markets = payload.get("markets")
    cursor = payload.get("cursor")
    if not isinstance(markets, list) or len(markets) > MAX_MARKETS:
        raise PredictionEvidenceError("Invalid or oversized markets list")
    if not isinstance(cursor, str):
        raise PredictionEvidenceError("Invalid markets cursor")
    result: list[dict[str, Any]] = []
    for market in markets:
        if not isinstance(market, Mapping):
            raise PredictionEvidenceError("Invalid market row")
        row = dict(market)
        event_ticker = row.get("event_ticker")
        if not isinstance(event_ticker, str) or not event_ticker.startswith(SERIES_TICKER + "-"):
            raise PredictionEvidenceError("Market belongs to the wrong series")
        row_status = row.get("status")
        accepted_statuses = {"open": {"active"}, "settled": {"settled", "finalized"}}
        if row_status not in accepted_statuses[status]:
            raise PredictionEvidenceError("Market status does not match query")
        result.append(row)
    return result


class KalshiPublicClient:
    """Two-request, unauthenticated Kalshi market-data client."""

    def __init__(self, opener: Any = None, clock: Callable[[], datetime] | None = None) -> None:
        self.opener = opener or urllib.request.build_opener(NoRedirect)
        self.clock = clock or (lambda: datetime.now(UTC))
        self.requests = 0

    def fetch_status(self, status: str, *, now: datetime | None = None) -> dict[str, Any]:
        if status not in ("open", "settled"):
            raise PredictionEvidenceError("Only open and settled status are supported")
        if self.requests >= MAX_REQUESTS:
            raise PredictionEvidenceError("Hourly request budget exhausted")
        current = _aware(now or self.clock(), "query time")
        params: dict[str, Any] = {
            "series_ticker": SERIES_TICKER,
            "status": status,
            "limit": 100,
        }
        if status == "settled":
            params["min_settled_ts"] = int((current - timedelta(seconds=LOOKBACK_SECONDS)).timestamp())
        url = ORIGIN + PATH + "?" + urllib.parse.urlencode(params)
        requested_at = _aware(self.clock(), "request time")
        request = urllib.request.Request(url, headers={"Accept": "application/json"}, method="GET")
        self.requests += 1
        try:
            with self.opener.open(request, timeout=30) as response:
                if response.status != 200:
                    raise PredictionEvidenceError("Kalshi public API status not successful")
                raw = response.read(MAX_RESPONSE_BYTES + 1)
        except urllib.error.HTTPError as exc:
            raise PredictionEvidenceError(f"Kalshi public API HTTP {exc.code}") from None
        except PredictionEvidenceError:
            raise
        except Exception:
            raise PredictionEvidenceError("Kalshi public API transport failure") from None
        if len(raw) > MAX_RESPONSE_BYTES:
            raise PredictionEvidenceError("Kalshi public API response too large")
        try:
            payload = json.loads(raw)
        except (ValueError, UnicodeError):
            raise PredictionEvidenceError("Invalid Kalshi public API JSON") from None
        if not isinstance(payload, Mapping):
            raise PredictionEvidenceError("Invalid Kalshi public API object")
        received_at = _aware(self.clock(), "receipt time")
        markets = _validate_market_rows(payload, status)
        cursor = payload["cursor"]
        return {
            "status": status,
            "query": params,
            "requested_at": requested_at.isoformat(),
            "received_at": received_at.isoformat(),
            "source_hash": _sha256(raw),
            "raw_response": payload,
            "raw_response_b64": base64.b64encode(raw).decode("ascii"),
            "markets": markets,
            "cursor": cursor,
            "complete_response": cursor == "",
        }

    def fetch_hour(self, *, now: datetime | None = None) -> list[dict[str, Any]]:
        current = _aware(now or self.clock(), "query time")
        return [self.fetch_status(status, now=current) for status in ("open", "settled")]


def normalize_evidence(results: list[Mapping[str, Any]], *, collected_at: datetime | None = None) -> dict[str, Any]:
    if not isinstance(results, list) or len(results) != 2:
        raise PredictionEvidenceError("Evidence must contain open and settled responses")
    responses: list[dict[str, Any]] = []
    statuses: set[str] = set()
    receipts: list[datetime] = []
    for result in results:
        if not isinstance(result, Mapping):
            raise PredictionEvidenceError("Invalid response evidence")
        status = result.get("status")
        if status not in ("open", "settled") or status in statuses:
            raise PredictionEvidenceError("Evidence statuses are invalid or duplicated")
        statuses.add(status)
        received = _parse_time(result.get("received_at"), "receipt time")
        receipts.append(received)
        responses.append(dict(result))
    if statuses != {"open", "settled"}:
        raise PredictionEvidenceError("Evidence must contain open and settled responses")
    collection = _aware(collected_at or max(receipts), "collection time")
    return {
        "schema_version": 1,
        "source_kind": "observed",
        "venue": "kalshi_public",
        "series_ticker": SERIES_TICKER,
        "collected_at": collection.isoformat(),
        "calibration_eligible": False,
        "complete_response": all(bool(result.get("complete_response")) for result in responses),
        "responses": responses,
        "labels_are_venue_reported": True,
        "probabilities": None,
        "orders_sent": 0,
    }


def _state_size(root: Path) -> int:
    total = 0
    for directory, directories, files in os.walk(root, followlinks=False):
        for name in directories + files:
            path = Path(directory) / name
            if path.is_symlink():
                raise PredictionEvidenceError("Symlink in state directory refused")
            try:
                total += path.stat().st_size
            except OSError:
                raise PredictionEvidenceError("Unable to inspect state directory") from None
    return total


def _ensure_private_directory(state: str | os.PathLike[str]) -> Path:
    root = Path(state)
    if root.exists() and root.is_symlink():
        raise PredictionEvidenceError("State directory symlink refused")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if root.is_symlink() or not root.is_dir():
        raise PredictionEvidenceError("State directory is not private")
    try:
        os.chmod(root, 0o700)
    except OSError:
        raise PredictionEvidenceError("State directory permissions unavailable") from None
    return root


def _atomic_json(path: Path, value: Any, root: Path, *, replace: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.exists() or path.is_symlink():
        if path.is_symlink():
            raise PredictionEvidenceError("State file symlink refused")
        if not replace:
            raise PredictionEvidenceError("State file already exists")
    payload = _canonical(value) + b"\n"
    if path.suffix == ".gz":
        payload = gzip.compress(payload, compresslevel=6, mtime=0)
    existing = path.stat().st_size if path.exists() else 0
    if len(payload) > MAX_STATE_BYTES or _state_size(root) - existing + len(payload) > MAX_STATE_BYTES:
        raise StorageLimitError("State storage limit")
    temporary: str | None = None
    try:
        descriptor, temporary = tempfile.mkstemp(prefix=".prediction-evidence-", dir=str(path.parent))
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        temporary = None
    except OSError:
        raise PredictionEvidenceError("Unable to persist private state") from None
    finally:
        if temporary is not None:
            try:
                os.unlink(temporary)
            except OSError:
                pass


def read_evidence(path: str | os.PathLike[str]) -> Any:
    """Read a bounded compressed archive; exact provider bytes remain inside."""
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise PredictionEvidenceError("Invalid evidence path")
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rb') as stream:
        payload = stream.read(20_000_001)
    if len(payload)>20_000_000:
        raise PredictionEvidenceError("Evidence decompression limit")
    return json.loads(payload)


def _read_json(path: Path) -> Any:
    if path.is_symlink():
        raise PredictionEvidenceError("State file symlink refused")
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as stream:
            return json.load(stream)
    except (OSError, ValueError, UnicodeError):
        raise PredictionEvidenceError("Invalid private state") from None


def _lock(root: Path):
    try:
        descriptor = os.open(root / "prediction-evidence.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        stream = os.fdopen(descriptor, "a+")
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return stream
    except BlockingIOError:
        raise PredictionEvidenceError("Prediction evidence collection already running") from None
    except OSError:
        raise PredictionEvidenceError("Unable to acquire collection lock") from None


def _compact(status: str, run_hour: str, *, evidence_path: Path | None = None, detail: str | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {
        "status": status,
        "run_hour": run_hour,
        "source_kind": "observed" if evidence_path is not None else None,
        "venue": "kalshi_public",
        "series_ticker": SERIES_TICKER,
        "calibration_eligible": False,
        "orders_sent": 0,
    }
    if evidence_path is not None:
        result["evidence_path"] = str(evidence_path)
    if detail is not None:
        result["detail"] = detail
    return result


def collect_once(
    state: str | os.PathLike[str],
    *,
    client: KalshiPublicClient | None = None,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    """Claim one UTC hour and perform at most two unauthenticated GETs."""
    clock = clock or (lambda: datetime.now(UTC))
    before = _aware(clock(), "clock")
    hour = _hour_start(before)
    run_hour = hour.isoformat()
    root = _ensure_private_directory(state)
    lock = _lock(root)
    try:
        if _state_size(root) >= MAX_STATE_BYTES:
            return _compact("storage_limit", run_hour)
        claim_path = root / "claim.json"
        claim = _read_json(claim_path)
        if claim is not None:
            if not isinstance(claim, Mapping):
                return _compact("invalid_claim", run_hour)
            try:
                claimed_at = _parse_time(claim.get("claimed_at"), "claim time")
            except ValueError:
                return _compact("invalid_claim", run_hour)
            if before < claimed_at:
                return _compact("clock_rollback", run_hour)
            if claim.get("run_hour") == run_hour:
                evidence_path = root / "evidence" / (hour.strftime("%Y%m%dT%H") + ".json.gz")
                return _compact("same_hour_replay", run_hour, evidence_path=evidence_path if evidence_path.exists() else None)
        try:
            _atomic_json(claim_path, {"schema_version": 1, "run_hour": run_hour, "claimed_at": before.isoformat(), "research_only": True}, root, replace=True)
        except StorageLimitError:
            return _compact("storage_limit", run_hour)
        evidence_path = root / "evidence" / (hour.strftime("%Y%m%dT%H") + ".json.gz")
        status_path = root / "status.json"
        try:
            results = (client or KalshiPublicClient(clock=clock)).fetch_hour(now=before)
            evidence = normalize_evidence(results)
            _atomic_json(evidence_path, evidence, root)
            status = "observed" if evidence["complete_response"] else "incomplete_pagination"
            status_value = _compact(status, run_hour, evidence_path=evidence_path)
            status_value.update({"complete_response": evidence["complete_response"], "response_count": len(results)})
            _atomic_json(status_path, status_value, root, replace=True)
            return status_value
        except StorageLimitError:
            status_value = _compact("storage_limit", run_hour)
            try:
                _atomic_json(status_path, status_value, root, replace=True)
            except PredictionEvidenceError:
                pass
            return status_value
        except Exception as exc:
            status_value = _compact("fetch_or_validation_failed", run_hour, detail=type(exc).__name__)
            try:
                _atomic_json(status_path, status_value, root, replace=True)
            except PredictionEvidenceError:
                pass
            return status_value
    finally:
        lock.close()


collect = collect_once
fetch_hour = KalshiPublicClient.fetch_hour


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True)
    args = parser.parse_args()
    result = collect_once(args.state)
    print(json.dumps(result, sort_keys=True))
    return 1 if result["status"] in {"fetch_or_validation_failed", "storage_limit", "incomplete_pagination"} else 0


if __name__ == "__main__":
    raise SystemExit(main())
