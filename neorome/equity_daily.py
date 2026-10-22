"""Bounded daily SPY research capture and paper-policy evaluation.

This module performs one observed, read-only five-request capture per UTC day.
It has no order, ledger, fill, retry, or activation path.  A daily claim is
written before provider I/O so a failed day is consumed and a clock rollback
cannot silently repeat a request.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import tempfile

from desks.equities.paper_policy import evaluate

from . import recorder
from .equity_context import EquityContextClient
from .equity_data import EquityDataClient


UTC = timezone.utc
DEFAULT_STATE = "/var/lib/neorome-equity-daily"
MAX_STATE_BYTES = 256 * 1024 * 1024
MAX_CAPTURE_REQUESTS = 5


class DailyError(Exception):
    """Safe operational error whose message never contains provider data."""


class StorageLimitError(DailyError):
    """The next private JSON write would exceed the total state cap."""


def _utc_now(clock):
    value = clock() if callable(clock) else clock
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise DailyError("Clock must be timezone aware")
    return value.astimezone(UTC)


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _sha256(value):
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _ensure_private_directory(path):
    path = Path(path)
    if path.exists() and path.is_symlink():
        raise DailyError("State directory symlink refused")
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.is_symlink() or not path.is_dir():
        raise DailyError("State directory is not private")
    try:
        os.chmod(path, 0o700)
    except OSError:
        raise DailyError("State directory permissions unavailable") from None
    return path


def _state_size(root):
    total = 0
    for directory, dirs, files in os.walk(root, followlinks=False):
        for name in dirs + files:
            path = Path(directory) / name
            if path.is_symlink():
                raise DailyError("Symlink in state directory refused")
            try:
                total += path.stat().st_size
            except OSError:
                raise DailyError("Unable to inspect state directory") from None
    return total


def _atomic_json(path, value, root, *, replace=False):
    path = Path(path)
    if path.exists() or path.is_symlink():
        if path.is_symlink():
            raise DailyError("State file symlink refused")
        if not replace:
            raise DailyError("State file already exists")
    payload = (_canonical(value) + "\n").encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if len(payload) > MAX_STATE_BYTES or _state_size(root) + len(payload) > MAX_STATE_BYTES:
        raise StorageLimitError("State storage limit")
    temporary = None
    try:
        descriptor, temporary = tempfile.mkstemp(prefix=".equity-daily-", dir=str(path.parent))
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        temporary = None
    except OSError:
        raise DailyError("Unable to persist private state") from None
    finally:
        if temporary is not None:
            try:
                os.unlink(temporary)
            except OSError:
                pass


def _read_json(path):
    path = Path(path)
    if path.is_symlink():
        raise DailyError("State file symlink refused")
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as stream:
            return json.load(stream)
    except (OSError, ValueError, UnicodeError):
        raise DailyError("Invalid private state") from None


def _lock(root):
    try:
        descriptor = os.open(root / "daily.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        stream = os.fdopen(descriptor, "a+")
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return stream
    except BlockingIOError:
        raise DailyError("Daily collection already running") from None
    except OSError:
        raise DailyError("Unable to acquire daily collection lock") from None


def collect_observed(clock):
    """Collect exactly one context observation and one market observation."""
    context = EquityContextClient(credentials=recorder.credentials, clock=clock).observe("SPY")
    observation = EquityDataClient(credentials=recorder.credentials, clock=clock).observe("SPY")
    return context, observation


def _validate_observed(context, observation):
    if not isinstance(context, dict) or not isinstance(observation, dict):
        raise DailyError("Observed bundle must contain objects")
    if context.get("source_kind") != "observed" or observation.get("source_kind") != "observed":
        raise DailyError("Only observed source data is accepted")
    if context.get("source_kind") != observation.get("source_kind"):
        raise DailyError("Conflicting source kinds")
    if observation.get("symbol") != "SPY" or observation.get("feed") != "iex":
        raise DailyError("Only the fixed SPY IEX observation is accepted")
    if context.get("requests") + observation.get("request_count") != MAX_CAPTURE_REQUESTS:
        raise DailyError("Daily request budget was not exactly five GETs")
    if context.get("order_mutations") != 0 or observation.get("broker_orders") != 0:
        raise DailyError("Observed bundle contains order mutations")
    if observation.get("execution_enabled") is not False:
        raise DailyError("Observed bundle is execution enabled")
    return context, observation


def _compact(status, run_date, path=None, decision=None, error=None):
    result = {
        "status": status,
        "run_date_utc": run_date,
        "research_only": True,
        "execution_enabled": False,
        "broker_orders_sent": 0,
    }
    if path is not None:
        result["result_path"] = str(path)
    if decision is not None:
        result["eligible_for_internal_paper"] = bool(decision.get("eligible_for_internal_paper"))
        result["reasons"] = decision.get("reasons", [])
    if error is not None:
        result["error"] = error
    return result


def run_once(state=DEFAULT_STATE, collector=None, clock=None):
    """Claim and process the current UTC day without retrying provider I/O."""
    if clock is None:
        clock = lambda: datetime.now(UTC)
    before = _utc_now(clock)
    run_date = before.date().isoformat()
    root = _ensure_private_directory(state)
    lock = _lock(root)
    try:
        if _state_size(root) >= MAX_STATE_BYTES:
            return _compact("storage_limit", run_date)

        claim_path = root / "claim.json"
        claim = _read_json(claim_path)
        if claim is not None:
            claimed_at = claim.get("claimed_at") if isinstance(claim, dict) else None
            claimed_date = claim.get("run_date_utc") if isinstance(claim, dict) else None
            try:
                claimed_time = datetime.fromisoformat(str(claimed_at).replace("Z", "+00:00"))
                if claimed_time.tzinfo is None or claimed_time.utcoffset() is None:
                    return _compact("invalid_claim", run_date)
                claimed_time = claimed_time.astimezone(UTC)
            except (AttributeError, TypeError, ValueError):
                return _compact("invalid_claim", run_date)
            if before < claimed_time:
                return _compact("clock_rollback", run_date)
            if claimed_date == run_date:
                result_path = root / "runs" / (run_date + ".json")
                if result_path.exists() and not result_path.is_symlink():
                    saved = _read_json(result_path)
                    return _compact("same_day_replay", run_date, result_path,
                                    saved.get("decision") if isinstance(saved, dict) else None)
                return _compact("same_day_replay", run_date)

        claim = {"schema_version": 1, "run_date_utc": run_date,
                 "claimed_at": before.isoformat(), "research_only": True}
        try:
            _atomic_json(claim_path, claim, root, replace=True)
        except StorageLimitError:
            return _compact("storage_limit", run_date)

        result_path = root / "runs" / (run_date + ".json")
        if result_path.exists() or result_path.is_symlink():
            return _compact("same_day_replay", run_date, result_path)

        fetcher = collector or collect_observed
        try:
            collected = fetcher(clock)
            if not isinstance(collected, (tuple, list)) or len(collected) != 2:
                raise DailyError("Collector returned an invalid bundle")
            context, observation = _validate_observed(collected[0], collected[1])
            postfetch = _utc_now(clock)
            decision = evaluate(observation, context, postfetch)
            bundle = {
                "schema_version": 1,
                "source_kind": "observed",
                "symbol": "SPY",
                "feed": "iex",
                "context": context,
                "observation": observation,
                "order_mutations": 0,
                "broker_orders_sent": 0,
                "execution_enabled": False,
                "research_only": True,
            }
            record = {
                "schema_version": 1,
                "status": "completed" if decision.get("eligible_for_internal_paper") else "abstained",
                "run_date_utc": run_date,
                "collected_at": postfetch.isoformat(),
                "source_kind": "observed",
                "research_only": True,
                "execution_enabled": False,
                "broker_orders_sent": 0,
                "bundle": bundle,
                "decision": decision,
                "hashes": {
                    "context_sha256": _sha256(context),
                    "observation_sha256": _sha256(observation),
                    "decision_sha256": _sha256(decision),
                    "bundle_sha256": _sha256(bundle),
                },
            }
            _atomic_json(result_path, record, root)
            return _compact(record["status"], run_date, result_path, decision)
        except StorageLimitError:
            return _compact("storage_limit", run_date, error="StorageLimitError")
        except Exception as exc:
            record = {
                "schema_version": 1,
                "status": "fetch_or_validation_failed",
                "run_date_utc": run_date,
                "source_kind": "observed",
                "research_only": True,
                "execution_enabled": False,
                "broker_orders_sent": 0,
                "error": type(exc).__name__,
                "hashes": {},
            }
            try:
                _atomic_json(result_path, record, root)
            except DailyError:
                return _compact("storage_failure", run_date, error="DailyError")
            return _compact("fetch_or_validation_failed", run_date, result_path,
                            error=type(exc).__name__)
    finally:
        lock.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default=DEFAULT_STATE)
    args = parser.parse_args(argv)
    try:
        result = run_once(args.state)
    except DailyError as exc:
        result = _compact("operational_failure", datetime.now(UTC).date().isoformat(),
                          error=type(exc).__name__)
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0 if result["status"] in {"completed", "abstained", "same_day_replay"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
