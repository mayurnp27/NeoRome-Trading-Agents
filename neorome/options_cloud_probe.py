"""Bounded read-only Robinhood cloud options data probe.

The probe is deliberately narrow: one fixed SPY 2026-10-30 call configuration,
one New York weekday morning window, five data calls, and no orders, models or
ledger writes.  It archives the existing cloud collector's snapshot and keeps
authentication, data access, quote freshness and strategy admission separate.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import date, datetime, time, timedelta, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo


UTC = timezone.utc
NY = ZoneInfo("America/New_York")
DEFAULT_STATE = "/var/lib/neorome-options-data"
DEFAULT_TOKEN_PATH = "/var/lib/neorome-options-auth/tokens.json"
SYMBOL = "SPY"
EXPIRY = "2026-10-30"
RIGHT = "call"
STRIKES = ("765", "770")
MAX_DATA_CALLS = 5
MAX_STATE_BYTES = 64 * 1024 * 1024
WINDOW_START = time(9, 45)
WINDOW_END = time(10, 0)
EXPECTED_STALE_ERROR = "Provider quote is stale or in the future"


class OptionsCloudProbeError(Exception):
    """Safe operator-facing probe error; no token or provider body is stored."""


class StorageLimitError(OptionsCloudProbeError):
    pass


def _aware(value: datetime | None, label: str = "time") -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise OptionsCloudProbeError(f"{label} must be timezone aware")
    return value.astimezone(UTC)


def _parse_time(value: Any, label: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{label} is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return _aware(parsed, label)
    except (TypeError, ValueError, OptionsCloudProbeError):
        raise ValueError(f"{label} is invalid") from None


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError):
        raise OptionsCloudProbeError("Snapshot is not valid JSON") from None


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _expected_stale(snapshot: Mapping[str, Any], receipt: datetime, error: BaseException) -> bool:
    """Accept only normalize's known old-quote error with valid past quotes."""
    if str(error) != EXPECTED_STALE_ERROR:
        return False
    timestamps: list[datetime] = []
    responses = snapshot.get("responses")
    if not isinstance(responses, list):
        return False
    for response in responses:
        if not isinstance(response, Mapping) or response.get("tool") != "get_option_quotes":
            continue
        data = response.get("data")
        results = data.get("results") if isinstance(data, Mapping) else None
        if not isinstance(results, list):
            return False
        for item in results:
            quote = item.get("quote") if isinstance(item, Mapping) else None
            if not isinstance(quote, Mapping):
                return False
            try:
                timestamp = _parse_time(quote.get("updated_at"), "provider quote timestamp")
            except ValueError:
                return False
            if timestamp > receipt:
                return False
            timestamps.append(timestamp)
    if not timestamps:
        return False
    return any(receipt - timestamp > timedelta(seconds=60) for timestamp in timestamps)


def _in_window(now: datetime) -> bool:
    local = _aware(now).astimezone(NY)
    return local.weekday() < 5 and WINDOW_START <= local.time() < WINDOW_END


def _default_session_factory(token_path: str):
    from .robinhood_cloud_data import CloudDataSession

    return CloudDataSession(token_path)


def _default_capture(session: Any, *, clock: datetime) -> Mapping[str, Any]:
    from .robinhood_options import collect

    return collect(session.call, symbol=SYMBOL, expiry=EXPIRY, right=RIGHT, strikes=list(STRIKES))


def _default_normalize(snapshot: Mapping[str, Any], *, as_of: datetime) -> Any:
    from .robinhood_options import normalize

    return normalize(snapshot, as_of=as_of)


def _ensure_private_directory(state: str | os.PathLike[str]) -> Path:
    root = Path(state)
    if root.exists() and root.is_symlink():
        raise OptionsCloudProbeError("State directory symlink refused")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if root.is_symlink() or not root.is_dir():
        raise OptionsCloudProbeError("State directory is not private")
    try:
        os.chmod(root, 0o700)
    except OSError:
        raise OptionsCloudProbeError("State directory permissions unavailable") from None
    return root


def _state_size(root: Path) -> int:
    total = 0
    for directory, directories, files in os.walk(root, followlinks=False):
        for name in directories + files:
            path = Path(directory) / name
            if path.is_symlink():
                raise OptionsCloudProbeError("Symlink in state directory refused")
            try:
                total += path.stat().st_size
            except OSError:
                raise OptionsCloudProbeError("Unable to inspect state directory") from None
    return total


def _atomic_json(path: Path, value: Any, root: Path, *, replace: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.exists() or path.is_symlink():
        if path.is_symlink():
            raise OptionsCloudProbeError("State file symlink refused")
        if not replace:
            raise OptionsCloudProbeError("State file already exists")
    payload = _canonical(value) + b"\n"
    existing = path.stat().st_size if path.exists() else 0
    if len(payload) > MAX_STATE_BYTES or _state_size(root) - existing + len(payload) > MAX_STATE_BYTES:
        raise StorageLimitError("State storage limit")
    temporary: str | None = None
    try:
        descriptor, temporary = tempfile.mkstemp(prefix=".options-cloud-probe-", dir=str(path.parent))
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        temporary = None
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except OSError:
        raise OptionsCloudProbeError("Unable to persist private state") from None
    finally:
        if temporary is not None:
            try:
                os.unlink(temporary)
            except OSError:
                pass


def _read_json(path: Path) -> Any:
    if path.is_symlink():
        raise OptionsCloudProbeError("State file symlink refused")
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as stream:
            return json.load(stream)
    except (OSError, ValueError, UnicodeError):
        raise OptionsCloudProbeError("Invalid private state") from None


def _lock(root: Path):
    try:
        descriptor = os.open(root / "probe.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        stream = os.fdopen(descriptor, "a+")
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return stream
    except BlockingIOError:
        raise OptionsCloudProbeError("Options cloud probe already running") from None
    except OSError:
        raise OptionsCloudProbeError("Unable to acquire probe lock") from None


def _compact(status: str, run_date: str, *, snapshot_path: Path | None = None, detail: str | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {
        "status": status,
        "run_date_ny": run_date,
        "source_kind": "observed" if snapshot_path is not None else None,
        "symbol": SYMBOL,
        "expiry": EXPIRY,
        "right": RIGHT,
        "strikes": list(STRIKES),
        "broker_orders_sent": 0,
        "execution_enabled": False,
    }
    if snapshot_path is not None:
        result["snapshot_path"] = str(snapshot_path)
    if detail is not None:
        result["detail"] = detail
    return result


def _admission_defaults() -> dict[str, Any]:
    return {
        "operational_health": "not_run",
        "cloud_auth": {"status": "not_run"},
        "data_access": {"status": "not_run", "data_calls": 0},
        "freshness": {"status": "not_run"},
        "strategy_admission": {"status": "blocked", "reason": "Probe collects data only; no strategy admission is performed"},
    }


def _blocked_result(status: str, run_date: str, *, detail: str | None = None) -> dict[str, Any]:
    result = _compact(status, run_date, detail=detail)
    result.update(_admission_defaults())
    result["operational_health"] = "blocked"
    return result


def collect_once(
    state: str | os.PathLike[str] = DEFAULT_STATE,
    *,
    token_path: str | os.PathLike[str] = DEFAULT_TOKEN_PATH,
    clock: Callable[[], datetime] | None = None,
    session_factory: Callable[[str], Any] | None = None,
    capture: Callable[..., Mapping[str, Any]] | None = None,
    normalizer: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    """Run at most one fixed five-call data probe per New York weekday."""
    clock = clock or (lambda: datetime.now(UTC))
    before = _aware(clock(), "clock")
    local = before.astimezone(NY)
    run_date = local.date().isoformat()
    if not _in_window(before):
        result = _compact("closed_window", run_date)
        result.update(_admission_defaults())
        result["operational_health"] = "healthy"
        return result
    if local.date() > date.fromisoformat(EXPIRY):
        result = _compact("expired_configuration", run_date)
        result.update(_admission_defaults())
        result["operational_health"] = "healthy"
        return result
    root = _ensure_private_directory(state)
    lock = _lock(root)
    try:
        defaults = _admission_defaults()
        if _state_size(root) >= MAX_STATE_BYTES:
            return _blocked_result("storage_limit", run_date)
        claim_path = root / "claim.json"
        claim = _read_json(claim_path)
        if claim is not None:
            if not isinstance(claim, Mapping):
                return _blocked_result("invalid_claim", run_date)
            try:
                claimed_at = _parse_time(claim.get("claimed_at"), "claim time")
            except ValueError:
                return _blocked_result("invalid_claim", run_date)
            if before < claimed_at:
                return _blocked_result("clock_rollback", run_date)
            if claim.get("run_date_ny") == run_date:
                snapshot_path = root / "snapshots" / (run_date + ".json")
                status_path = root / "status.json"
                try:
                    saved = _read_json(status_path)
                except OptionsCloudProbeError:
                    return _blocked_result("claim_incomplete_failure", run_date)
                if not isinstance(saved, Mapping) or saved.get("run_date_ny") != run_date:
                    return _blocked_result("claim_incomplete_failure", run_date)
                if isinstance(saved, Mapping) and saved.get("status") in {"fetch_or_validation_failed", "storage_limit", "invalid_claim", "clock_rollback"}:
                    replay = dict(saved)
                    replay["replay"] = True
                    return replay
                if not snapshot_path.exists():
                    return _blocked_result("claim_incomplete_failure", run_date)
                result = _compact("same_day_replay", run_date, snapshot_path=snapshot_path if snapshot_path.exists() else None)
                result.update(defaults)
                result["operational_health"] = "healthy"
                return result
        try:
            _atomic_json(claim_path, {"schema_version": 1, "run_date_ny": run_date, "claimed_at": before.isoformat(), "research_only": True}, root, replace=True)
        except StorageLimitError:
            return _blocked_result("storage_limit", run_date)
        snapshot_path = root / "snapshots" / (run_date + ".json")
        status_path = root / "status.json"
        outcome = _compact("fetch_or_validation_failed", run_date)
        outcome.update(defaults)
        try:
            factory = session_factory or _default_session_factory
            fetcher = capture or _default_capture
            normalize = normalizer or _default_normalize
            with factory(str(token_path)) as session:
                outcome["cloud_auth"] = {"status": "available"}
                calls_before = getattr(session, "calls", 0)
                snapshot = dict(fetcher(session, clock=before))
                calls_after = getattr(session, "calls", calls_before)
                response_count = len(snapshot.get("responses", [])) if isinstance(snapshot.get("responses"), list) else 0
                data_calls = calls_after - calls_before if isinstance(calls_after, int) and isinstance(calls_before, int) else 0
                data_calls = max(data_calls, response_count)
                if data_calls > MAX_DATA_CALLS:
                    raise OptionsCloudProbeError("Data call budget exceeded")
                outcome["data_access"] = {"status": "available", "data_calls": data_calls}
                outcome["operational_health"] = "healthy"
                receipt_text = snapshot.get("received_at")
                receipt: datetime | None = None
                try:
                    receipt = _parse_time(receipt_text, "provider receipt")
                    after_capture = _aware(clock(), "post-capture clock")
                    if receipt < before or receipt > after_capture:
                        raise ValueError("Provider receipt chronology is invalid")
                    outcome["freshness"] = {"status": "freshness_checking", "received_at": receipt.isoformat()}
                    try:
                        # A validator must not be able to rewrite the observed
                        # receipt and thereby manufacture a fresh-source claim.
                        normalize(deepcopy(snapshot), as_of=receipt)
                        outcome["freshness"] = {"status": "fresh", "received_at": receipt.isoformat()}
                    except Exception as exc:
                        if _expected_stale(snapshot, receipt, exc):
                            outcome["freshness"] = {"status": "stale_or_unusable", "received_at": receipt.isoformat(), "error_type": type(exc).__name__}
                        else:
                            outcome["freshness"] = {"status": "validation_failed", "received_at": receipt.isoformat(), "error_type": type(exc).__name__}
                except Exception as exc:
                    outcome["freshness"] = {"status": "validation_failed", "error_type": type(exc).__name__}
                    outcome["operational_health"] = "blocked"
                snapshot["probe_metadata"] = {
                    "schema_version": 1,
                    "source_kind": "observed",
                    "symbol": SYMBOL,
                    "expiry": EXPIRY,
                    "right": RIGHT,
                    "strikes": list(STRIKES),
                    "collected_at": outcome["freshness"].get("received_at"),
                    "calibration_eligible": False,
                    "strategy_admission": outcome["strategy_admission"],
                    "data_calls": outcome["data_access"]["data_calls"],
                }
                snapshot["snapshot_sha256"] = _sha256(_canonical(snapshot))
                _atomic_json(snapshot_path, snapshot, root)
                if outcome["freshness"]["status"] == "fresh":
                    outcome["status"] = "observed"
                elif outcome["freshness"]["status"] == "stale_or_unusable":
                    outcome["status"] = "stale_or_unusable"
                else:
                    outcome["status"] = "fetch_or_validation_failed"
                    outcome["operational_health"] = "blocked"
                outcome["snapshot_sha256"] = snapshot["snapshot_sha256"]
                _atomic_json(status_path, outcome, root, replace=True)
                return outcome
        except OptionsCloudProbeError as exc:
            if str(exc) == "Data call budget exceeded":
                outcome["data_access"] = {"status": "blocked", "data_calls": MAX_DATA_CALLS + 1, "error_type": type(exc).__name__}
            outcome["operational_health"] = "blocked"
            outcome["status"] = "storage_limit" if isinstance(exc, StorageLimitError) else "fetch_or_validation_failed"
            outcome["detail"] = type(exc).__name__
        except Exception as exc:
            # CloudDataSession deliberately exposes safe exception messages;
            # only the type is persisted here as an additional redaction gate.
            if outcome["cloud_auth"]["status"] == "not_run":
                outcome["cloud_auth"] = {"status": "blocked", "error_type": type(exc).__name__}
                outcome["operational_health"] = "blocked"
            elif outcome["data_access"]["status"] == "not_run":
                outcome["data_access"] = {"status": "blocked", "data_calls": 0, "error_type": type(exc).__name__}
                outcome["operational_health"] = "blocked"
            outcome["status"] = "fetch_or_validation_failed"
            outcome["detail"] = type(exc).__name__
        try:
            _atomic_json(status_path, outcome, root, replace=True)
        except OptionsCloudProbeError:
            pass
        return outcome
    finally:
        lock.close()


collect = collect_once


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--tokens", default=DEFAULT_TOKEN_PATH)
    args = parser.parse_args()
    result = collect_once(args.state, token_path=args.tokens)
    print(json.dumps(result, sort_keys=True))
    return 1 if result["status"] in {
        "fetch_or_validation_failed",
        "storage_limit",
        "invalid_claim",
        "clock_rollback",
        "claim_incomplete_failure",
    } else 0


if __name__ == "__main__":
    raise SystemExit(main())
