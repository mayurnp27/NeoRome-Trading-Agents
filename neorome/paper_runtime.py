"""Unattended, bounded runner for the observed internal-paper crypto pilot.

The runtime owns scheduling and health state only.  It reuses the existing
shared paper ledger and ``crypto_pilot.run``; it never creates a ledger,
fetches data, loads credentials, or submits broker orders.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import secrets
import sqlite3
from typing import Any, Callable, Mapping

from .crypto_daily import _quote_from_observation
from .crypto_pilot import DEFAULT_LEDGER, _ledger_records, run as pilot_run
from .paper_ledger import PaperLedger


UTC = timezone.utc
DEFAULT_STATE = "/var/lib/neorome-paper-runtime"
DEFAULT_PILOT_STATE = "/var/lib/neorome-crypto-pilot"
DEFAULT_RECOVERY_STATUS = "/var/lib/neorome-paper-recovery/status.json"
STOP_NAME = "STOP"
STATUS_NAME = "status.json"
LOCK_NAME = "runtime.lock"
MAX_STORAGE_BYTES = 128 * 1024 * 1024
RECOVERY_MAX_AGE_SECONDS = 7200
DESK_STATUS = {
    "crypto": "active",
    "equities": "research_only",
    "options": "blocked_freshness_and_lifecycle",
    "prediction": "blocked_calibration",
}


class RuntimeError(ValueError):
    """A local runtime boundary failure."""


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _now(value: datetime | None) -> datetime:
    current = value or datetime.now(UTC)
    if current.tzinfo is None or current.utcoffset() is None:
        raise RuntimeError("now must be timezone-aware")
    return current.astimezone(UTC)


def _hash(value: Any) -> str | None:
    if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdefABCDEF" for c in value):
        return None
    return value.lower()


def _ensure_root(path: str | os.PathLike[str]) -> Path:
    root = Path(path)
    if root.exists() and root.is_symlink():
        raise RuntimeError("runtime state symlink refused")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if root.is_symlink() or not root.is_dir():
        raise RuntimeError("runtime state is not a directory")
    os.chmod(root, 0o700)
    return root


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    if path.exists() and path.is_symlink():
        raise RuntimeError("runtime status symlink refused")
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
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


def _load_status(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "schema_version": 1,
            "last_success": None,
            "last_cycle": None,
            "price_blocked": False,
            "operational_failure": None,
            "last_claim": None,
        }
    if path.is_symlink() or not path.is_file():
        raise RuntimeError("runtime status is not a regular file")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError("runtime status is unreadable") from exc
    if not isinstance(value, dict):
        raise RuntimeError("runtime status must be an object")
    return value


def _storage_bytes(root: Path, ledger_path: Path, pilot_root: Path) -> int:
    paths = [ledger_path, Path(str(ledger_path) + "-wal"), Path(str(ledger_path) + "-shm")]
    total = 0
    seen: set[Path] = set()
    roots = [root]
    if pilot_root.exists():
        roots.append(pilot_root)
    for scan_root in roots:
        if scan_root.is_symlink():
            raise RuntimeError("pilot/runtime storage symlink refused")
    for path in paths + [item for scan_root in roots for item in scan_root.rglob("*")]:
        if path in seen or not path.exists():
            continue
        seen.add(path)
        if path.is_symlink():
            raise RuntimeError("runtime storage symlink refused")
        if path.is_file():
            total += path.stat().st_size
    return total


def _ledger_exists(path: Path) -> None:
    if path.is_symlink() or not path.is_file():
        raise RuntimeError("shared paper ledger must already exist as a regular file")
    try:
        uri = f"file:{path.absolute()}?mode=ro"
        with sqlite3.connect(uri, uri=True) as connection:
            marker = connection.execute("SELECT value FROM settings WHERE key='ledger_marker'").fetchone()
    except sqlite3.Error as exc:
        raise RuntimeError("shared paper ledger is not a readable SQLite paper ledger") from exc
    if marker is None or marker[0] != "neorome-paper-ledger-v1":
        raise RuntimeError("shared paper ledger marker is invalid")


def _recovery_allows_entry(path: str | os.PathLike[str], current: datetime) -> tuple[bool, str | None]:
    """Gate new entries on a recent healthy recovery bundle."""
    status_path = Path(path)
    if status_path.is_symlink() or not status_path.is_file():
        return False, "recovery_unhealthy"
    try:
        value = json.loads(status_path.read_text(encoding="utf-8"))
        updated = datetime.fromisoformat(str(value["updated_at"]).replace("Z", "+00:00"))
        if updated.tzinfo is None or updated.utcoffset() is None:
            return False, "recovery_unhealthy"
        age = (current - updated.astimezone(UTC)).total_seconds()
        if age < 0 or age > RECOVERY_MAX_AGE_SECONDS or value.get("operational_healthy") is not True:
            return False, "recovery_unhealthy"
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        return False, "recovery_unhealthy"
    return True, None


def _pending_or_open(ledger_path: Path, current: datetime) -> tuple[bool, bool, bool]:
    book = PaperLedger(ledger_path, enforce_market_risk=True, clock=lambda: current)
    pending, positions, _unsupported = _ledger_records(book)
    expired = any(datetime.fromisoformat(item[0].expiry_at.replace("Z", "+00:00")) <= current for item in pending)
    return bool(pending), bool(positions), expired


def _quote_hash(
    observation_state: str,
    current: datetime,
    injected_quote: Mapping[str, Any] | None,
    injected_source: Mapping[str, Any] | None,
) -> tuple[str | None, dict[str, Any] | None, dict[str, Any] | None, str | None]:
    if injected_quote is not None or injected_source is not None:
        source_hash = _hash((injected_source or {}).get("sha256", (injected_source or {}).get("hash")))
        if source_hash is None:
            raise RuntimeError("injected quote source hash is invalid")
        return source_hash, dict(injected_quote) if injected_quote is not None else None, dict(injected_source or {}), None
    quote, status, source = _quote_from_observation(observation_state, current)
    source_hash = _hash(source.get("sha256")) if isinstance(source, Mapping) else None
    return source_hash, quote, dict(source) if isinstance(source, Mapping) else None, status


def _pilot_call(
    runner: Callable[..., Mapping[str, Any]],
    *,
    daily_evidence_path: Any,
    state_dir: Path,
    ledger_path: Path,
    observation_state: str,
    current: datetime,
    quote: Mapping[str, Any] | None,
    quote_source: Mapping[str, Any] | None,
    allow_entry: bool,
    mark_provider: Any = None,
) -> Mapping[str, Any]:
    kwargs = {
        "daily_evidence_path": daily_evidence_path,
        "state_dir": state_dir,
        "now": current,
        "ledger_path": ledger_path,
        "observation_state": observation_state,
        "quote": quote,
        "quote_source": quote_source,
    }
    kwargs["allow_entry"] = allow_entry
    if mark_provider is not None:
        kwargs["mark_provider"] = mark_provider
    return runner(**kwargs)


def _expected_result(report: Mapping[str, Any], now: datetime | None = None) -> tuple[str, bool, bool]:
    blocker = report.get("blocker")
    if not blocker:
        return "success", False, True
    text = str(blocker).lower()
    # Daily history is scheduled for 00:05 UTC. Allow its short rollover
    # window without hiding a missing archive for the remainder of the day.
    if (text == "piloterror: daily evidence file is missing" and now is not None
            and now.astimezone(UTC).hour == 0 and now.astimezone(UTC).minute < 15):
        return "abstain", False, True
    if text.startswith("markunavailable:"):
        return "price_blocked", True, True
    expected = {
        "already_running", "entries_disabled", "daily_entry_already_attempted", "global_marks_unavailable",
        "awaiting_independent_post_reservation_quote", "awaiting_independent_exit_quote",
        "insufficient_fill_depth", "insufficient_mark_depth", "insufficient_exit_depth",
        "post_reservation_cost_exceeds_reserved_cash", "entry_size_exceeds_firm_cap_or_depth",
        "exit_target_pending", "quote_missing", "quote_stale", "quote_future",
        "quote_unavailable", "recovery_unhealthy", "daily_evidence_missing",
        "daily_evidence_stale", "history_missing", "history_stale", "no_entry", "hold",
    }
    if text in expected:
        if text == "already_running":
            return "already_running", False, True
        price = any(token in text for token in ("depth", "cost", "quote", "price"))
        return ("price_blocked" if price else "abstain"), price, True
    return "operational_failure", False, False


def _status(
    *,
    current: datetime,
    status: str,
    healthy: bool,
    state: Mapping[str, Any],
    quote_hash: str | None,
    blocker: str | None = None,
    actions: list[str] | None = None,
    price_blocked: bool | None = None,
    operational_failure: str | None = None,
    market_snapshot: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    equity = state.get('equity_cycle')
    if isinstance(equity, Mapping) and equity.get('operational_healthy') is False:
        healthy = False
        status = 'operational_failure'
        operational_failure = 'equity_cycle_failed'
    result = dict(state)
    result.update(
        {
            "schema_version": 1,
            "updated_at": _iso(current),
            "operational_healthy": healthy,
            "status": status,
            "desk_status": dict(DESK_STATUS),
            "last_cycle": {
                "at": _iso(current),
                "quote_hash": quote_hash,
                "status": status,
                "blocker": blocker,
                "actions": actions or [],
            },
            "price_blocked": state.get("price_blocked", False) if price_blocked is None else price_blocked,
            "operational_failure": operational_failure,
            "market_snapshot": dict(market_snapshot) if isinstance(market_snapshot, Mapping) else state.get("market_snapshot"),
            "broker_orders_sent": 0,
            "broker_execution_enabled": False,
        }
    )
    if isinstance(equity, Mapping):
        result['desk_status']['equities'] = equity.get('desk_status', 'research_only')
    return result


def run_once(
    state_dir: str | os.PathLike[str] = DEFAULT_STATE,
    *,
    ledger_path: str | os.PathLike[str] = DEFAULT_LEDGER,
    daily_evidence_path: Any = None,
    observation_state: str = "/var/lib/neorome-crypto-recorder",
    pilot_state: str | os.PathLike[str] = DEFAULT_PILOT_STATE,
    recovery_status_path: str | os.PathLike[str] = DEFAULT_RECOVERY_STATUS,
    require_recovery: bool = True,
    now: datetime | None = None,
    quote: Mapping[str, Any] | None = None,
    quote_source: Mapping[str, Any] | None = None,
    pilot_runner: Callable[..., Mapping[str, Any]] = pilot_run,
    max_storage_bytes: int = MAX_STORAGE_BYTES,
    mark_provider: Any = None,
    equity_cycle: Any = None,
    allow_crypto_entry: bool | None = None,
) -> dict[str, Any]:
    """Run at most one pilot decision for a newly observed quote hash."""
    if allow_crypto_entry is not None and type(allow_crypto_entry) is not bool:
        raise RuntimeError("allow_crypto_entry must be boolean or None")
    current = _now(now)
    root = _ensure_root(state_dir)
    ledger = Path(ledger_path)
    _ledger_exists(ledger)
    if max_storage_bytes <= 0:
        raise RuntimeError("max_storage_bytes must be positive")
    pilot_root = Path(pilot_state)
    status_path = root / STATUS_NAME
    lock_path = root / LOCK_NAME
    if lock_path.exists() and lock_path.is_symlink():
        raise RuntimeError("runtime lock symlink refused")
    with lock_path.open("a+") as lock:
        os.chmod(lock_path, 0o600)
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {"schema_version": 1, "status": "already_running", "operational_healthy": True, "updated_at": _iso(current), "broker_orders_sent": 0, "broker_execution_enabled": False}
        state = _load_status(status_path)
        if _storage_bytes(root, ledger, pilot_root) > max_storage_bytes:
            result = _status(current=current, status="storage_limit", healthy=False, state=state, quote_hash=None, operational_failure="storage_limit")
            _atomic_json(status_path, result)
            return result
        stop_path = root / STOP_NAME
        if stop_path.is_symlink():
            result = _status(current=current, status="operational_failure", healthy=False, state=state, quote_hash=None, operational_failure="stop_file_symlink")
            _atomic_json(status_path, result)
            return result
        stop = stop_path.exists()
        try:
            pending, open_position, expired = _pending_or_open(ledger, current)
            actionable = pending or open_position
            recovery_ok, recovery_blocker = (True, None) if not require_recovery else _recovery_allows_entry(recovery_status_path, current)
            if equity_cycle is not None:
                equity_report = equity_cycle(current, not stop and recovery_ok)
                state['equity_cycle'] = equity_report
                if isinstance(equity_report.get('market_snapshot'), Mapping):
                    state['market_snapshot'] = equity_report['market_snapshot']
            # The equity cycle can perform slow shared-book integrity work.
            # Refresh wall time before evaluating recorder freshness, while
            # keeping explicit ``now`` deterministic for replay and tests.
            if now is None:
                current = _now(None)
                recovery_ok, recovery_blocker = (True, None) if not require_recovery else _recovery_allows_entry(recovery_status_path, current)
            maintenance = expired or (pending and (stop or not recovery_ok))
            quote_error: Exception | None = None
            try:
                quote_hash, current_quote, current_source, quote_status = _quote_hash(observation_state, current, quote, quote_source)
            except Exception as exc:
                if not maintenance:
                    raise
                quote_error = exc
                quote_hash, current_quote, current_source, quote_status = None, None, None, None
            # Claim persistence and other preflight work can also consume a
            # material part of the quote window. Re-read against the latest
            # production clock immediately before the pilot decision.
            if now is None:
                current = _now(None)
                recovery_ok, recovery_blocker = (True, None) if not require_recovery else _recovery_allows_entry(recovery_status_path, current)
                maintenance = expired or (pending and (stop or not recovery_ok))
                try:
                    quote_hash, current_quote, current_source, quote_status = _quote_hash(
                        observation_state, current, quote, quote_source
                    )
                    quote_error = None
                except Exception as exc:
                    if not maintenance:
                        raise
                    quote_error = exc
                    quote_hash, current_quote, current_source, quote_status = None, None, None, None
            prior_hash = _hash(state.get("last_claim", {}).get("quote_hash")) if isinstance(state.get("last_claim"), Mapping) else None
            prior_claim_status = state.get("last_claim", {}).get("status") if isinstance(state.get("last_claim"), Mapping) else None
            if stop and not actionable:
                result = _status(current=current, status="stopped", healthy=True, state=state, quote_hash=quote_hash)
                _atomic_json(status_path, result)
                return result
            if quote_hash is not None and quote_hash == prior_hash and prior_claim_status == "processed" and not maintenance:
                result = _status(current=current, status="already_processed", healthy=True, state=state, quote_hash=quote_hash)
                _atomic_json(status_path, result)
                return result
            if quote_hash is None and not maintenance and not actionable:
                result = _status(current=current, status="abstain", healthy=True, state=state, quote_hash=None, blocker=f"quote_{quote_status or 'unavailable'}")
                result["last_success"] = _iso(current)
                _atomic_json(status_path, result)
                return result
            claim = {"quote_hash": quote_hash, "claimed_at": _iso(current), "status": "claimed"}
            state["last_claim"] = claim
            _atomic_json(status_path, _status(current=current, status="claimed", healthy=True, state=state, quote_hash=quote_hash))
            report = _pilot_call(
                pilot_runner,
                daily_evidence_path=daily_evidence_path,
                state_dir=pilot_root,
                ledger_path=ledger,
                observation_state=observation_state,
                current=current,
                quote=current_quote,
                quote_source=current_source if current_quote is not None else None,
                allow_entry=(not stop and recovery_ok and allow_crypto_entry is not False),
                mark_provider=mark_provider,
            )
            actions = [str(action.get("type")) for action in report.get("actions", []) if isinstance(action, Mapping)]
            report_blocker = report.get("blocker")
            global_error = report.get("global_mark_error")
            if global_error and not str(global_error).startswith("MarkUnavailable:"):
                report_blocker = global_error
            if quote_error is not None:
                report_blocker = f"quote_read_failed:{type(quote_error).__name__}"
            if recovery_blocker and not stop and (not report_blocker or str(report_blocker).lower() == "entries_disabled"):
                report_blocker = recovery_blocker
            status, price_blocked, healthy = _expected_result({**report, "blocker": report_blocker}, current)
            retryable = str(report_blocker).lower() == "already_running"
            state["last_claim"] = {"quote_hash": quote_hash, "claimed_at": claim["claimed_at"], "status": "failed" if retryable or not healthy else "processed"}
            result = _status(current=current, status=status, healthy=healthy, state=state, quote_hash=quote_hash, blocker=report_blocker, actions=actions, price_blocked=price_blocked, operational_failure=None if healthy else str(report_blocker), market_snapshot=report.get("market_snapshot"))
            if result.get("operational_healthy") and not retryable:
                result["last_success"] = _iso(current)
        except Exception as exc:
            if isinstance(state.get("last_claim"), dict):
                state["last_claim"]["status"] = "failed"
            result = _status(current=current, status="operational_failure", healthy=False, state=state, quote_hash=None, operational_failure=type(exc).__name__)
        if _storage_bytes(root, ledger, pilot_root) > max_storage_bytes:
            result = _status(current=current, status="storage_limit", healthy=False, state=result, quote_hash=result.get("last_cycle", {}).get("quote_hash"), operational_failure="storage_limit")
        _atomic_json(status_path, result)
        return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", default=DEFAULT_STATE)
    parser.add_argument("--ledger", default=DEFAULT_LEDGER)
    parser.add_argument("--daily-evidence")
    parser.add_argument("--observation-state", default="/var/lib/neorome-crypto-recorder")
    parser.add_argument("--pilot-state", default=DEFAULT_PILOT_STATE)
    parser.add_argument("--recovery-status", default=DEFAULT_RECOVERY_STATUS)
    parser.add_argument("--no-require-recovery", action="store_true")
    args = parser.parse_args(argv)
    try:
        result = run_once(args.state_dir, ledger_path=args.ledger, daily_evidence_path=args.daily_evidence, observation_state=args.observation_state, pilot_state=args.pilot_state, recovery_status_path=args.recovery_status, require_recovery=not args.no_require_recovery)
    except Exception as exc:
        result = {"status": "operational_failure", "operational_healthy": False, "operational_failure": type(exc).__name__, "broker_orders_sent": 0}
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0 if result.get("status") in {"success", "abstain", "price_blocked", "already_processed", "stopped", "already_running"} else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["DEFAULT_PILOT_STATE", "DEFAULT_RECOVERY_STATUS", "DEFAULT_STATE", "MAX_STORAGE_BYTES", "run_once", "main"]
