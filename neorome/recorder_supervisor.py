"""Local supervisor for two recorder states and bounded immutable backups.

This module is intentionally local and credential-free.  It polls the two
recorder databases through :mod:`neorome.recorder_ops`, creates at most one
verified backup per UTC day for each recorder, and writes one atomic status
JSON document for an external service manager to inspect.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
from typing import Any, Callable, Mapping

from .recorder_ops import RecorderOpsError, backup_recorder, health_summary, verify_backup

UTC = timezone.utc
DEFAULT_SPY_STATE = "/var/lib/neorome-recorder"
DEFAULT_BTC_STATE = "/var/lib/neorome-crypto-recorder"
DEFAULT_OPERATIONS_STATE = "/var/lib/neorome-ops"
DEFAULT_STORAGE_CAP_BYTES = 2 * 1024 * 1024 * 1024
DEFAULT_RETENTION = 7
DATABASE_NAME = "recorder.sqlite3"
STATUS_NAME = "status.json"
LOCK_NAME = "supervisor.lock"
BACKUP_ROOT_NAME = "backups"
_DATED = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
_BACKUP_OVERHEAD_BYTES = 64 * 1024


class SupervisorError(Exception):
    """A safe, operator-facing supervisor error."""


def _now(value: datetime | None, clock: Callable[[], datetime]) -> datetime:
    current = value if value is not None else clock()
    if not isinstance(current, datetime) or current.tzinfo is None or current.utcoffset() is None:
        raise SupervisorError("now must be an aware timestamp")
    return current.astimezone(UTC)


def _path(value: str | os.PathLike[str], field: str) -> Path:
    result = Path(value)
    if not str(result):
        raise SupervisorError(f"{field} is required")
    return result


def _ensure_root(path: Path, field: str, *, create: bool) -> Path:
    if os.path.lexists(path) and path.is_symlink():
        raise SupervisorError(f"{field} is a symlink")
    if path.exists() and not path.is_dir():
        raise SupervisorError(f"{field} is not a directory")
    if create:
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if os.path.lexists(path) and path.is_symlink():
        raise SupervisorError(f"{field} is a symlink")
    if not path.is_dir():
        raise SupervisorError(f"{field} is not a directory")
    return path


def _ensure_regular_file(path: Path, field: str) -> None:
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise SupervisorError(f"{field} is missing") from exc
    if path.is_symlink() or not path.is_file() or info.st_size < 0:
        raise SupervisorError(f"{field} is not a regular file")


def _assert_disjoint(operations: Path, states: Mapping[str, Path]) -> None:
    ops = operations.resolve(strict=False)
    for name, state in states.items():
        resolved = state.resolve(strict=False)
        if ops == resolved or ops in resolved.parents or resolved in ops.parents:
            raise SupervisorError(f"operations state overlaps live {name} state")


def _open_lock(root: Path) -> tuple[int, Any] | None:
    lock_path = root / LOCK_NAME
    if os.path.lexists(lock_path) and lock_path.is_symlink():
        raise SupervisorError("supervisor lock is a symlink")
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    except OSError as exc:
        raise SupervisorError("unable to open supervisor lock") from exc
    try:
        os.fchmod(fd, 0o600)
        lock = os.fdopen(fd, "a+")
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            lock.close()
            return None
        return fd, lock
    except Exception:
        os.close(fd)
        raise


def _tree_size(root: Path) -> int:
    total = 0
    for current, directories, files in os.walk(root, followlinks=False):
        current_path = Path(current)
        for name in directories:
            child = current_path / name
            if child.is_symlink():
                raise SupervisorError(f"operations tree contains symlink: {child}")
        for name in files:
            child = current_path / name
            if child.is_symlink() or not child.is_file():
                raise SupervisorError(f"operations tree contains non-regular file: {child}")
            total += child.stat().st_size
    return total


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    if os.path.lexists(path) and path.is_symlink():
        raise SupervisorError("status path is a symlink")
    if path.exists() and not path.is_file():
        raise SupervisorError("status path is not a regular file")
    encoded = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    if os.path.lexists(temporary):
        if temporary.is_symlink() or not temporary.is_file():
            raise SupervisorError("status temporary path is unsafe")
        temporary.unlink()
    fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except Exception:
        try:
            if os.path.lexists(temporary) and not temporary.is_symlink():
                temporary.unlink()
        except OSError:
            pass
        raise


def _health(name: str, state: Path, current: datetime, heartbeat_seconds: int, data_stale_seconds: int) -> dict[str, Any]:
    if os.path.lexists(state) and state.is_symlink():
        return {"status": "refused_symlink", "execution_enabled": False, "state": str(state)}
    try:
        result = health_summary(
            state,
            current,
            heartbeat_seconds=heartbeat_seconds,
            data_stale_seconds=data_stale_seconds,
        )
    except Exception as exc:
        return {"status": "health_error", "execution_enabled": False, "state": str(state), "error": type(exc).__name__}
    result = dict(result)
    result["state"] = str(state)
    return result


def _health_operational(name: str, summary: Mapping[str, Any]) -> bool:
    """Reduce recorder health for a local /healthz-style readiness signal."""
    if summary.get("status") == "refused_symlink":
        return False
    process = summary.get("process", {})
    if not isinstance(process, Mapping) or process.get("status") != "healthy":
        return False
    latest_status = process.get("last_attempt_status")
    if latest_status in {"fetch_or_validation_failed", "storage_limit", "incomplete"}:
        return False
    # SPY data can be stale outside its market session. BTC is a continuous
    # market and therefore requires a fresh observation for operational health.
    if name == "btc":
        data = summary.get("data", {})
        if not isinstance(data, Mapping) or data.get("status") != "fresh":
            return False
    return True


def _backup_root(root: Path, name: str) -> Path:
    backups = root / BACKUP_ROOT_NAME
    _ensure_root(backups, "backup root", create=True)
    destination = backups / name
    _ensure_root(destination, f"{name} backup root", create=True)
    return destination


def _verified_dated(root: Path) -> tuple[list[Path], list[str]]:
    valid: list[Path] = []
    invalid: list[str] = []
    for child in root.iterdir():
        if not _DATED.fullmatch(child.name):
            continue
        if child.is_symlink() or not child.is_dir():
            invalid.append(child.name)
            continue
        result = verify_backup(child)
        if result.get("status") == "valid":
            valid.append(child)
        else:
            invalid.append(child.name)
    valid.sort(key=lambda item: item.name)
    return valid, invalid


def _delete_verified(path: Path) -> None:
    if path.is_symlink() or not path.is_dir() or not _DATED.fullmatch(path.name):
        raise SupervisorError("retention target is not an owned dated directory")
    verified = verify_backup(path)
    if verified.get("status") != "valid":
        raise SupervisorError("retention target checksum verification failed")
    children = list(path.iterdir())
    if any(child.is_symlink() or not child.is_file() for child in children):
        raise SupervisorError("retention target contains unsafe entries")
    os.chmod(path, 0o700)
    for child in children:
        child.unlink()
    path.rmdir()


def _retain(root: Path) -> dict[str, Any]:
    valid, invalid = _verified_dated(root)
    removed: list[str] = []
    for child in valid[:-DEFAULT_RETENTION]:
        _delete_verified(child)
        removed.append(child.name)
    return {
        "verified_count": len(valid) - len(removed),
        "removed": removed,
        "invalid_dated": invalid,
    }


def _source_size(state: Path, filename: str = DATABASE_NAME) -> int:
    database = state / filename
    _ensure_regular_file(database, "recorder database")
    return database.stat().st_size


def _backup_one(
    operations: Path,
    name: str,
    state: Path,
    current: datetime,
    storage_cap_bytes: int,
    database: str = DATABASE_NAME,
) -> dict[str, Any]:
    root = _backup_root(operations, name)
    dated = root / current.date().isoformat()
    source_bytes = _source_size(state, database)
    if os.path.lexists(dated):
        if dated.is_symlink() or not dated.is_dir():
            raise SupervisorError("today's backup path is unsafe")
        verification = verify_backup(dated)
        if verification.get("status") != "valid":
            raise SupervisorError("today's existing backup is invalid")
        retention = _retain(root)
        return {"status": "already_verified", "backup": str(dated), "verification": verification, "retention": retention}
    current_bytes = _tree_size(operations)
    if current_bytes + source_bytes + _BACKUP_OVERHEAD_BYTES > storage_cap_bytes:
        return {
            "status": "storage_cap",
            "storage_bytes": current_bytes,
            "source_database_bytes": source_bytes,
            "storage_cap_bytes": storage_cap_bytes,
        }
    created = backup_recorder(state, dated, now=current, database=database)
    verification = verify_backup(dated)
    if verification.get("status") != "valid":
        raise SupervisorError("new backup verification failed")
    retention = _retain(root)
    return {"status": "backup_created", "backup": str(dated), "created": created,
            "verification": verification, "retention": retention}


def _status(
    *, current: datetime, health: Mapping[str, Any], backups: Mapping[str, Any],
    storage: Mapping[str, Any], status: str, operations: Path,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "updated_at": current.isoformat().replace("+00:00", "Z"),
        "as_of": current.isoformat().replace("+00:00", "Z"),
        "status": status,
        "operational_healthy": status == "healthy",
        "operations_state": str(operations),
        "health": dict(health),
        "backups": dict(backups),
        "storage": dict(storage),
        "execution_enabled": False,
        "network_requests": 0,
        "credentials_loaded": False,
    }


def _fleet_policy(path: str | os.PathLike[str]) -> dict[str, Any]:
    source = Path(path)
    _ensure_regular_file(source, 'fleet_policy')
    if source.stat().st_size > 131072:
        raise SupervisorError('fleet policy over size limit')
    value = json.loads(source.read_text())
    from .unit_health import validated_units
    if not isinstance(value, dict) or value.get('schema') != 'neorome-operational-fleet-v1':
        raise SupervisorError('invalid fleet policy')
    required = validated_units(value.get('required_units'))
    archived = value.get('archived_recorders')
    if value.get('active_recorders') != ['btc'] or not isinstance(archived, dict) or set(archived) != {'spy'}:
        raise SupervisorError('invalid recorder retirement')
    spy = archived['spy']
    if not isinstance(spy, dict) or not isinstance(spy.get('reason'), str) or not 1 <= len(spy['reason']) <= 512:
        raise SupervisorError('recorder retirement reason required')
    replacements = validated_units(spy.get('replacement_units'))
    if not set(replacements).issubset(required) or 'neorome-crypto-recorder.timer' not in required:
        raise SupervisorError('recorder replacements not monitored')
    return value


def run_once(
    spy_state: str | os.PathLike[str] = DEFAULT_SPY_STATE,
    btc_state: str | os.PathLike[str] = DEFAULT_BTC_STATE,
    operations_state: str | os.PathLike[str] = DEFAULT_OPERATIONS_STATE,
    *,
    now: datetime | None = None,
    clock: Callable[[], datetime] | None = None,
    heartbeat_seconds: int = 180,
    data_stale_seconds: int = 180,
    storage_cap_bytes: int = DEFAULT_STORAGE_CAP_BYTES,
    paper_state: str | os.PathLike[str] | None = None,
    check_service_units: bool = False,
    fleet_policy: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
    """Poll both recorders and perform today's verified backups under one lock."""
    if isinstance(heartbeat_seconds, bool) or not isinstance(heartbeat_seconds, int) or heartbeat_seconds <= 0:
        raise SupervisorError("heartbeat_seconds must be positive")
    if isinstance(data_stale_seconds, bool) or not isinstance(data_stale_seconds, int) or data_stale_seconds <= 0:
        raise SupervisorError("data_stale_seconds must be positive")
    if isinstance(storage_cap_bytes, bool) or not isinstance(storage_cap_bytes, int) or storage_cap_bytes <= 0:
        raise SupervisorError("storage_cap_bytes must be positive")
    clock = clock or (lambda: datetime.now(UTC))
    current = _now(now, clock)
    fleet = _fleet_policy(fleet_policy) if fleet_policy is not None else None
    if fleet is not None and not check_service_units:
        raise SupervisorError('fleet policy requires unit verification')
    states = {"spy": _path(spy_state, "spy_state"), "btc": _path(btc_state, "btc_state")}
    operations = _path(operations_state, "operations_state")
    backup_states = dict(states)
    if paper_state is not None:
        backup_states['paper'] = _path(paper_state, 'paper_state')
    _assert_disjoint(operations, backup_states)
    _ensure_root(operations, "operations_state", create=True)
    health = {name: _health(name, state, current, heartbeat_seconds, data_stale_seconds)
              for name, state in states.items()}
    for name, summary in health.items():
        summary["operational_healthy"] = _health_operational(name, summary)
    units = None
    if check_service_units:
        from .unit_health import check_units
        units = check_units(required_units=fleet['required_units']) if fleet else check_units()
    required_health = dict(health)
    if fleet:
        # Historical recorder health and backups remain visible and verified.
        # Retirement affects operational readiness only while replacements pass.
        replacement_health = check_units(
            required_units=fleet['archived_recorders']['spy']['replacement_units'])
        replacements_ok = replacement_health['operational_healthy']
        health['spy']['operational_role'] = 'archived'
        health['spy']['retirement_reason'] = fleet['archived_recorders']['spy']['reason']
        health['spy']['replacement_units'] = fleet['archived_recorders']['spy']['replacement_units']
        health['spy']['replacement_verified'] = replacements_ok
        health['spy']['replacement_health'] = replacement_health
        if replacements_ok:
            required_health = {'btc': health['btc']}
    lock = _open_lock(operations)
    if lock is None:
        storage = {"status": "unmeasured", "cap_bytes": storage_cap_bytes}
        result = _status(current=current, health=health,
                         backups={name: {"status": "already_running"} for name in states},
                         storage=storage, status="already_running", operations=operations)
        return result
    _fd, lock_file = lock
    try:
        backups: dict[str, Any] = {}
        for name, state in backup_states.items():
            try:
                backups[name] = _backup_one(operations, name, state, current, storage_cap_bytes,
                                            database='paper.sqlite3' if name == 'paper' else DATABASE_NAME)
            except (OSError, RecorderOpsError, SupervisorError, ValueError, sqlite3.Error) as exc:
                backups[name] = {"status": "error", "error": type(exc).__name__}
        try:
            storage = {"status": "measured", "bytes": _tree_size(operations), "cap_bytes": storage_cap_bytes}
        except SupervisorError as exc:
            storage = {"status": "error", "error": type(exc).__name__, "cap_bytes": storage_cap_bytes}
        health_ok = all(bool(item.get("operational_healthy")) for item in required_health.values())
        backup_ok = all(item.get("status") in {"backup_created", "already_verified"} for item in backups.values())
        storage_ok = storage.get("status") == "measured" and storage.get("bytes", 0) <= storage_cap_bytes
        overall = "healthy" if health_ok and backup_ok and storage_ok else "degraded"
        result = _status(current=current, health=health, backups=backups,
                         storage=storage, status=overall, operations=operations)
        result["operational_healthy"] = health_ok and backup_ok and storage_ok
        if fleet:
            result['fleet_policy_schema'] = fleet['schema']
        if check_service_units:
            result['units'] = units
            result['operational_healthy'] = result['operational_healthy'] and result['units']['operational_healthy']
            if not result['operational_healthy']:
                result['status'] = 'degraded'
        _atomic_json(operations / STATUS_NAME, result)
        return result
    finally:
        lock_file.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spy-state", default=DEFAULT_SPY_STATE)
    parser.add_argument("--btc-state", default=DEFAULT_BTC_STATE)
    parser.add_argument("--operations-state", default=DEFAULT_OPERATIONS_STATE)
    parser.add_argument("--paper-state", default='/var/lib/neorome-paper')
    parser.add_argument("--check-units", action='store_true')
    parser.add_argument("--fleet-policy")
    parser.add_argument("--heartbeat-seconds", type=int, default=180)
    parser.add_argument("--data-stale-seconds", type=int, default=180)
    parser.add_argument("--storage-cap-bytes", type=int, default=DEFAULT_STORAGE_CAP_BYTES)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run_once(
            args.spy_state, args.btc_state, args.operations_state,
            heartbeat_seconds=args.heartbeat_seconds,
            data_stale_seconds=args.data_stale_seconds,
            storage_cap_bytes=args.storage_cap_bytes,
            paper_state=args.paper_state,
            check_service_units=args.check_units,
            fleet_policy=args.fleet_policy,
        )
        # A completed local poll/retention pass is operationally successful
        # even when recorder data is stale or one configured state is absent.
        # External monitoring reads the degraded status JSON separately.
        code = 0 if result["status"] in {"healthy", "degraded", "already_running"} else 2
    except (OSError, SupervisorError, RecorderOpsError, ValueError) as exc:
        result = {"schema_version": 1, "status": "error", "error": type(exc).__name__,
                  "execution_enabled": False, "network_requests": 0, "credentials_loaded": False}
        code = 2
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
