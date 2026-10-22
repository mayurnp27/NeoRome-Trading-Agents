"""Bounded, local recovery bundles for the internal paper runtime.

The bundle job has no network, credential, broker, or order path.  It takes
SQLite online backups while holding the shared runtime lock and the crypto
pilot lock, then writes an immutable, checksummed bundle in a new directory.
Restore always targets a new directory and never overwrites live state.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
from typing import Any, Mapping
import uuid


UTC = timezone.utc
DEFAULT_RECOVERY_ROOT = "/var/lib/neorome-paper-recovery"
DEFAULT_RUNTIME_ROOT = "/var/lib/neorome-paper-runtime"
DEFAULT_PAPER_LEDGER = "/var/lib/neorome-paper/paper.sqlite3"
DEFAULT_PILOT_STATE = "/var/lib/neorome-crypto-pilot/state.json"
DEFAULT_DAILY_ROOT = "/var/lib/neorome-crypto-daily"
DEFAULT_RECORDER_DATABASE = "/var/lib/neorome-crypto-recorder/recorder.sqlite3"
DEFAULT_CONTROL_DATABASE = "/var/lib/neorome-control/recovery-source.sqlite3"
MAX_COMBINED_BYTES = 2 * 1024 * 1024 * 1024
RETENTION_BUNDLES = 7
HEALTH_MAX_AGE_SECONDS = 7200
MANIFEST_NAME = "manifest.json"
MANIFEST_SHA_NAME = "manifest.sha256"
STATUS_NAME = "status.json"
CLAIM_NAME = "recovery-state.json"
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
RUNTIME_FILES = ("status.json", "latest-status.json", "state.json", "claim.json", "STOP")


class RecoveryError(Exception):
    """Safe recovery error without provider or credential details."""


class AlreadyRunning(RecoveryError):
    """A shared nonblocking lock is held by another process."""


def _now(value: datetime | None) -> datetime:
    result = value or datetime.now(UTC)
    if result.tzinfo is None or result.utcoffset() is None:
        raise RecoveryError("now must be timezone-aware")
    return result.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")


def _sha_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _ensure_dir(path: Path, *, create: bool = True) -> Path:
    if path.is_symlink():
        raise RecoveryError("symlink directory refused")
    if create:
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.is_symlink() or not path.is_dir():
        raise RecoveryError("state path is not a directory")
    os.chmod(path, 0o700)
    return path


def _regular(path: Path, label: str) -> Path:
    try:
        info = path.lstat()
    except FileNotFoundError:
        raise RecoveryError(f"missing {label}") from None
    if not path.is_file() or os.path.islink(path) or not info.st_size >= 0:
        raise RecoveryError(f"unsafe {label}")
    return path


def _safe_relative(value: Any) -> str:
    if not isinstance(value, str) or not value or Path(value).is_absolute():
        raise RecoveryError("manifest path is unsafe")
    path = Path(value)
    if not path.parts or any(part in ("", ".", "..") for part in path.parts):
        raise RecoveryError("manifest path is unsafe")
    return path.as_posix()


def _state_size(root: Path) -> int:
    total = 0
    for directory, dirs, files in os.walk(root, followlinks=False):
        for name in dirs + files:
            path = Path(directory) / name
            if path.is_symlink():
                raise RecoveryError("symlink in recovery state refused")
            total += path.stat().st_size
    return total


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    if path.is_symlink():
        raise RecoveryError("state JSON symlink refused")
    _ensure_dir(path.parent)
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    payload = _canonical(value)
    try:
        descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except OSError:
        raise RecoveryError("unable to persist recovery state") from None
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _read_json(path: Path) -> dict[str, Any] | None:
    if path.is_symlink():
        raise RecoveryError("state JSON symlink refused")
    if not path.exists():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise RecoveryError("recovery JSON is unreadable") from None
    if not isinstance(value, dict):
        raise RecoveryError("recovery JSON must be an object")
    return value


def _lock(path: Path):
    if path.is_symlink():
        raise RecoveryError("lock symlink refused")
    _ensure_dir(path.parent)
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        stream = os.fdopen(descriptor, "a+")
        os.chmod(path, 0o600)
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return stream
    except BlockingIOError:
        stream.close()
        raise AlreadyRunning("recovery lock is held") from None
    except OSError:
        stream.close()
        raise RecoveryError("unable to acquire recovery lock") from None


def _sources(now: datetime, values: Mapping[str, Any] | None) -> dict[str, Any]:
    values = values or {}
    daily = values.get("daily_evidence")
    result = {
        "paper_ledger": Path(values.get("paper_ledger", DEFAULT_PAPER_LEDGER)),
        "pilot_state": Path(values.get("pilot_state", DEFAULT_PILOT_STATE)),
        "daily_evidence": Path(daily) if daily is not None else Path(DEFAULT_DAILY_ROOT) / f"decision-{now.date().isoformat()}.json",
        "daily_root": Path(values.get("daily_root", DEFAULT_DAILY_ROOT)),
        "recorder_database": Path(values.get("recorder_database", DEFAULT_RECORDER_DATABASE)),
        "control_database": Path(values.get("control_database", DEFAULT_CONTROL_DATABASE)),
        "runtime_root": Path(values.get("runtime_root", DEFAULT_RUNTIME_ROOT)),
        "_daily_explicit": daily is not None,
    }
    return result


def _copy_file(source: Path, destination: Path) -> None:
    _regular(source, "source file")
    _ensure_dir(destination.parent)
    if destination.exists() or destination.is_symlink():
        raise RecoveryError("staging destination already exists")
    shutil.copyfile(source, destination)
    os.chmod(destination, 0o400)
    with destination.open("rb") as stream:
        os.fsync(stream.fileno())


def _sqlite_backup(source_path: Path, destination: Path) -> None:
    _regular(source_path, "SQLite source")
    _ensure_dir(destination.parent)
    source = target = None
    try:
        # Recovery only reads the live source. Opening it in SQLite's default
        # read-write mode can fail inside the hardened service when the source
        # tree is mounted read-only, even though the backup itself is safe.
        source = sqlite3.connect(f"{source_path.resolve().as_uri()}?mode=ro", uri=True, timeout=30.0)
        source.execute("PRAGMA query_only=ON")
        target = sqlite3.connect(destination)
        source.backup(target, pages=128, sleep=0.05)
        target.commit()
        # SQLite's online backup copies the source database header, including
        # WAL mode. Recovery snapshots are standalone immutable files; keep
        # them in DELETE mode so ordinary read-only consumers do not create
        # unmanifested -wal/-shm sidecars beside the snapshot.
        journal_mode = target.execute("PRAGMA journal_mode=DELETE").fetchone()
        if not journal_mode or str(journal_mode[0]).lower() != "delete":
            raise RecoveryError("SQLite snapshot journal mode could not be normalized")
    except sqlite3.Error:
        raise RecoveryError("SQLite online backup failed") from None
    finally:
        if target is not None:
            target.close()
        if source is not None:
            source.close()
    wal_sidecar = Path(str(destination) + "-wal")
    shm_sidecar = Path(str(destination) + "-shm")
    if wal_sidecar.exists():
        # A WAL may contain committed pages not yet checkpointed into the
        # database. Never discard it from a recovery snapshot.
        raise RecoveryError("SQLite snapshot retained a WAL sidecar")
    if shm_sidecar.exists():
        # Some SQLite builds leave a transient SHM file after backup even
        # after switching the snapshot to DELETE mode. With the connection
        # closed and no WAL present, SHM contains no database pages and is
        # safe to remove before the immutable snapshot is hashed.
        _regular(shm_sidecar, "SQLite shared-memory sidecar")
        shm_sidecar.unlink()
    os.chmod(destination, 0o400)
    with destination.open("rb") as stream:
        os.fsync(stream.fileno())
    _integrity(destination)


def _integrity(path: Path) -> None:
    db = None
    try:
        # Immutable mode prevents SQLite from creating WAL/SHM sidecars while
        # checking an already-materialized backup.
        db = sqlite3.connect(f"file:{path.resolve()}?immutable=1", uri=True, timeout=30.0)
        result = db.execute("PRAGMA integrity_check").fetchone()
    except sqlite3.Error:
        raise RecoveryError("SQLite integrity check failed") from None
    finally:
        if db is not None:
            db.close()
    if not result or result[0] != "ok":
        raise RecoveryError("SQLite integrity check failed")


def _manifest_files(root: Path) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for directory, dirs, files in os.walk(root, followlinks=False):
        for name in dirs:
            if (Path(directory) / name).is_symlink():
                raise RecoveryError("symlink in bundle refused")
        for name in files:
            path = Path(directory) / name
            relative = path.relative_to(root).as_posix()
            if relative in (MANIFEST_NAME, MANIFEST_SHA_NAME):
                continue
            _regular(path, "bundle file")
            entries.append({"path": relative, "size_bytes": path.stat().st_size, "sha256": _sha_file(path)})
    return sorted(entries, key=lambda item: item["path"])


def _write_manifest(root: Path, manifest: Mapping[str, Any]) -> None:
    manifest_path = root / MANIFEST_NAME
    payload = _canonical(manifest)
    with manifest_path.open("xb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    os.chmod(manifest_path, 0o400)
    with (root / MANIFEST_SHA_NAME).open("xb") as stream:
        stream.write((_sha_bytes(payload) + "\n").encode("ascii"))
        stream.flush()
        os.fsync(stream.fileno())
    os.chmod(root / MANIFEST_SHA_NAME, 0o400)


def _daily_paths(source: Mapping[str, Any]) -> list[Path]:
    if source.get("_daily_explicit"):
        path = source["daily_evidence"]
        return [path] if path.exists() else []
    root = source["daily_root"]
    if not root.is_dir():
        return []
    result: list[Path] = []
    for candidate in sorted(root.iterdir()):
        if candidate.name.startswith("decision-") and candidate.name.endswith(".json"):
            if not DATE_RE.fullmatch(candidate.name[len("decision-"):-len(".json")]):
                raise RecoveryError("daily evidence filename is unsafe")
            result.append(candidate)
    return result


def _bundle_size_estimate(source: Mapping[str, Any], runtime_files: tuple[str, ...]) -> int:
    total = 4 * 1024 * 1024
    for key in ("paper_ledger", "pilot_state", "recorder_database"):
        path = _regular(source[key], key)
        total += path.stat().st_size
        if key.endswith("ledger") or key == "recorder_database":
            wal = Path(str(path) + "-wal")
            if wal.exists():
                total += _regular(wal, f"{key} WAL").stat().st_size
    control = source.get("control_database")
    if control is not None and Path(control).exists():
        control = _regular(Path(control), "control_database")
        total += control.stat().st_size
        wal = Path(str(control) + "-wal")
        if wal.exists():
            total += _regular(wal, "control_database WAL").stat().st_size
    for path in _daily_paths(source):
        total += _regular(path, "daily evidence").stat().st_size
    runtime_root = source["runtime_root"]
    for name in runtime_files:
        path = runtime_root / name
        if path.exists():
            total += _regular(path, f"runtime {name}").stat().st_size
    return total


def _verified_bundle_dirs(root: Path) -> list[Path]:
    result = []
    for child in root.iterdir():
        if child.is_dir() and DATE_RE.fullmatch(child.name):
            if verify_bundle(child).get("status") == "valid":
                result.append(child)
    return sorted(result, key=lambda path: path.name)


def _prune(root: Path) -> None:
    verified = _verified_bundle_dirs(root)
    for old in verified[:-RETENTION_BUNDLES]:
        shutil.rmtree(old)


def verify_bundle(bundle: str | os.PathLike[str]) -> dict[str, Any]:
    root = Path(bundle)
    errors: list[str] = []
    if root.is_symlink() or not root.is_dir():
        return {"status": "invalid", "bundle": str(root), "errors": ["bundle is not a directory"]}
    try:
        manifest_path = _regular(root / MANIFEST_NAME, "manifest")
        manifest_sha_path = _regular(root / MANIFEST_SHA_NAME, "manifest checksum")
        manifest_bytes = manifest_path.read_bytes()
        manifest = json.loads(manifest_bytes.decode("utf-8"))
        if not isinstance(manifest, dict) or manifest.get("schema_version") != 1 or manifest.get("owner") != "neorome.paper_recovery":
            errors.append("unsupported manifest")
        declared_manifest_sha = manifest_sha_path.read_text(encoding="ascii").strip().split()
        if not declared_manifest_sha or declared_manifest_sha[0] != _sha_bytes(manifest_bytes):
            errors.append("manifest checksum mismatch")
        entries = manifest.get("files") if isinstance(manifest, dict) else None
        if not isinstance(entries, list):
            errors.append("manifest files are missing")
            entries = []
        expected: dict[str, dict[str, Any]] = {}
        for entry in entries:
            if not isinstance(entry, dict):
                errors.append("manifest file entry is invalid")
                continue
            try:
                name = _safe_relative(entry.get("path"))
            except RecoveryError:
                errors.append("manifest file path is unsafe")
                continue
            if name in expected:
                errors.append(f"duplicate manifest path: {name}")
                continue
            expected[name] = entry
            path = root / name
            try:
                _regular(path, "bundle payload")
                if entry.get("size_bytes") != path.stat().st_size or entry.get("sha256") != _sha_file(path):
                    errors.append(f"payload checksum mismatch: {name}")
            except RecoveryError as exc:
                errors.append(str(exc))
        actual: set[str] = set()
        for directory, dirs, files in os.walk(root, followlinks=False):
            for name in dirs:
                if (Path(directory) / name).is_symlink():
                    errors.append("symlink directory in bundle")
            for name in files:
                path = Path(directory) / name
                if path.name not in (MANIFEST_NAME, MANIFEST_SHA_NAME):
                    actual.add(path.relative_to(root).as_posix())
        if actual != set(expected):
            errors.append("bundle payload set mismatch")
        components = manifest.get("components") if isinstance(manifest, dict) else None
        if not isinstance(components, dict):
            errors.append("manifest components are missing")
            components = {}
        required_paths = {"paper/paper.sqlite3", "pilot/state.json", "recorder/recorder.sqlite3"}
        if not required_paths.issubset(expected):
            errors.append("required payload paths are missing")
        sqlite_declared = manifest.get("sqlite_files") if isinstance(manifest, dict) else None
        permitted_sqlite = [
            ["paper/paper.sqlite3", "recorder/recorder.sqlite3"],
            ["paper/paper.sqlite3", "recorder/recorder.sqlite3", "control/control-plane.sqlite3"],
        ]
        if sqlite_declared not in permitted_sqlite:
            errors.append("sqlite payload declaration is invalid")
        for name in ("paper", "pilot", "daily", "recorder"):
            component = components.get(name)
            if not isinstance(component, dict):
                errors.append(f"component missing: {name}")
            elif component.get("required") and not component.get("present"):
                errors.append(f"required component absent: {name}")
        for name, expected_path in (("paper", "paper/paper.sqlite3"), ("pilot", "pilot/state.json"), ("recorder", "recorder/recorder.sqlite3")):
            component = components.get(name)
            if isinstance(component, dict) and (component.get("path") != expected_path or not component.get("present")):
                errors.append(f"component payload mismatch: {name}")
        daily = components.get("daily")
        if isinstance(daily, dict):
            daily_paths = daily.get("paths")
            if not isinstance(daily_paths, list) or not all(isinstance(path, str) for path in daily_paths) or len(daily_paths) != len(set(daily_paths)):
                errors.append("daily component paths are invalid")
            else:
                for path in daily_paths:
                    if not isinstance(path, str) or not path.startswith("daily/") or path not in expected:
                        errors.append("daily component payload mismatch")
                if bool(daily_paths) != bool(daily.get("present")):
                    errors.append("daily component presence mismatch")
        for sqlite_name in sqlite_declared if isinstance(sqlite_declared, list) else []:
            try:
                _integrity(root / _safe_relative(sqlite_name))
            except RecoveryError as exc:
                errors.append(str(exc))
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError, RecoveryError):
        errors.append("manifest or payload verification failed")
    if errors:
        return {"status": "invalid", "bundle": str(root), "errors": list(dict.fromkeys(errors))}
    return {"status": "valid", "bundle": str(root), "bundle_date": manifest.get("bundle_date"), "file_count": len(manifest["files"]), "bytes": sum(item["size_bytes"] for item in manifest["files"])}


def _status(root: Path, *, now: datetime, healthy: bool, status: str, **extra: Any) -> dict[str, Any]:
    value = {"schema_version": 1, "updated_at": _iso(now), "operational_healthy": bool(healthy), "status": status, **extra}
    try:
        _atomic_json(root / STATUS_NAME, value)
    except RecoveryError:
        pass
    return value


def health_check(state: str | os.PathLike[str] = DEFAULT_RECOVERY_ROOT, *, now: datetime | None = None) -> dict[str, Any]:
    current = _now(now)
    root = Path(state)
    if not root.exists() or root.is_symlink() or not root.is_dir():
        return {"status": "unhealthy", "operational_healthy": False, "reason": "missing_status"}
    try:
        _ensure_dir(root, create=False)
    except RecoveryError:
        return {"status": "unhealthy", "operational_healthy": False, "reason": "unsafe_state"}
    saved = _read_json(root / STATUS_NAME)
    if saved is None:
        return {"status": "unhealthy", "operational_healthy": False, "reason": "missing_status"}
    try:
        updated = datetime.fromisoformat(str(saved.get("updated_at")).replace("Z", "+00:00"))
        if updated.tzinfo is None or updated.utcoffset() is None:
            raise ValueError
        age = (current - updated.astimezone(UTC)).total_seconds()
    except (TypeError, ValueError):
        return {"status": "unhealthy", "operational_healthy": False, "reason": "invalid_status"}
    bundle = root / current.date().isoformat()
    verification = verify_bundle(bundle)
    healthy = bool(saved.get("operational_healthy")) and 0 <= age <= HEALTH_MAX_AGE_SECONDS and verification.get("status") == "valid"
    return {"status": "healthy" if healthy else "unhealthy", "operational_healthy": healthy, "age_seconds": age, "bundle": str(bundle), "verification": verification}


def run_once(
    state: str | os.PathLike[str] = DEFAULT_RECOVERY_ROOT,
    *,
    now: datetime | None = None,
    sources: Mapping[str, Any] | None = None,
    runtime_files: tuple[str, ...] = RUNTIME_FILES,
) -> dict[str, Any]:
    """Create or verify today's bundle exactly once without waiting on locks."""
    current = _now(now)
    root = _ensure_dir(Path(state))
    source = _sources(current, sources)
    runtime_lock = Path((sources or {}).get("runtime_lock", source["runtime_root"] / "runtime.lock"))
    pilot_lock = Path((sources or {}).get("pilot_lock", source["pilot_state"].parent / "pilot.lock"))
    # Verifying an already materialized immutable bundle never touches live
    # runtime state. Do it before contending with the 30-second runtime lock so
    # an unlucky hourly collision cannot let recovery readiness go stale.
    claim_path = root / CLAIM_NAME
    claim = _read_json(claim_path)
    if claim is not None:
        try:
            previous = datetime.fromisoformat(str(claim.get("started_at")).replace("Z", "+00:00"))
            if previous.tzinfo is None or previous.utcoffset() is None:
                raise ValueError
            if current < previous.astimezone(UTC):
                return _status(root, now=current, healthy=False, status="clock_rollback")
        except (TypeError, ValueError):
            return _status(root, now=current, healthy=False, status="invalid_claim")
    existing = root / current.date().isoformat()
    if existing.exists() or existing.is_symlink():
        verification = verify_bundle(existing)
        if verification.get("status") == "valid":
            return _status(root, now=current, healthy=True, status="already_verified",
                           bundle=str(existing), verification=verification)
    runtime_stream = pilot_stream = None
    try:
        try:
            runtime_stream = _lock(runtime_lock)
            pilot_stream = _lock(pilot_lock)
        except AlreadyRunning:
            return {"status": "already_running", "operational_healthy": False, "bundle_date": current.date().isoformat()}

        claim = _read_json(claim_path)
        if claim is not None:
            try:
                previous = datetime.fromisoformat(str(claim.get("started_at")).replace("Z", "+00:00"))
                if previous.tzinfo is None or previous.utcoffset() is None:
                    raise ValueError
                if current < previous.astimezone(UTC):
                    return _status(root, now=current, healthy=False, status="clock_rollback")
            except (TypeError, ValueError):
                return _status(root, now=current, healthy=False, status="invalid_claim")
            if claim.get("bundle_date") == current.date().isoformat():
                existing = root / current.date().isoformat()
                if existing.exists() or existing.is_symlink():
                    verification = verify_bundle(existing)
                    if verification.get("status") == "valid":
                        return _status(root, now=current, healthy=True, status="already_verified", bundle=str(existing), verification=verification)
                    return _status(root, now=current, healthy=False, status="already_attempted", bundle=str(existing), verification=verification)

        existing = root / current.date().isoformat()
        if existing.exists() or existing.is_symlink():
            verification = verify_bundle(existing)
            if verification.get("status") == "valid":
                return _status(root, now=current, healthy=True, status="already_verified", bundle=str(existing), verification=verification)
            return _status(root, now=current, healthy=False, status="existing_invalid", bundle=str(existing), verification=verification)

        try:
            _prune(root)
            estimate = _bundle_size_estimate(source, runtime_files)
        except Exception as exc:
            return _status(root, now=current, healthy=False, status="failed", error=type(exc).__name__)
        try:
            current_size = _state_size(root)
        except Exception as exc:
            return _status(root, now=current, healthy=False, status="failed", error=type(exc).__name__)
        if current_size + estimate > MAX_COMBINED_BYTES:
            return _status(root, now=current, healthy=False, status="storage_limit")
        _atomic_json(claim_path, {"schema_version": 1, "bundle_date": current.date().isoformat(), "started_at": _iso(current), "status": "incomplete"})
        staging = root / f".staging-{current.date().isoformat()}-{uuid.uuid4().hex}"
        _ensure_dir(staging)
        try:
            components: dict[str, Any] = {}
            sqlite_files: list[str] = []
            _sqlite_backup(source["paper_ledger"], staging / "paper" / "paper.sqlite3")
            components["paper"] = {"path": "paper/paper.sqlite3", "required": True, "present": True}
            _copy_file(source["pilot_state"], staging / "pilot" / "state.json")
            components["pilot"] = {"path": "pilot/state.json", "required": True, "present": True}
            daily_files = _daily_paths(source)
            for daily_file in daily_files:
                _copy_file(daily_file, staging / "daily" / daily_file.name)
            daily_names = [f"daily/{path.name}" for path in daily_files]
            latest_daily = max((path.name[len("decision-"):-len(".json")] for path in daily_files), default=None)
            components["daily"] = {"paths": daily_names, "required": False, "present": bool(daily_names), "latest_date": latest_daily, "current_date_present": current.date().isoformat() in [path.name[len("decision-"):-len(".json")] for path in daily_files]}
            _sqlite_backup(source["recorder_database"], staging / "recorder" / "recorder.sqlite3")
            components["recorder"] = {"path": "recorder/recorder.sqlite3", "required": True, "present": True}
            control_database = source.get("control_database")
            control_present = control_database is not None and Path(control_database).exists()
            if control_present:
                _sqlite_backup(Path(control_database), staging / "control" / "control-plane.sqlite3")
            components["control"] = {"path": "control/control-plane.sqlite3", "required": False,
                                     "present": control_present}
            runtime_present: list[str] = []
            for name in runtime_files:
                if not isinstance(name, str) or Path(name).name != name:
                    raise RecoveryError("runtime filename is unsafe")
                candidate = source["runtime_root"] / name
                if candidate.exists():
                    _copy_file(candidate, staging / "runtime" / name)
                    runtime_present.append(name)
            components["runtime"] = {"paths": runtime_present, "required": False, "present": bool(runtime_present)}
            pointers = {key: str(path) for key, path in source.items() if not key.startswith("_")}
            pointers.update({"runtime_files": runtime_files, "restore_layout": {"paper_ledger": "paper/paper.sqlite3", "pilot_state": "pilot/state.json", "daily_evidence": "daily/", "recorder_database": "recorder/recorder.sqlite3", "control_database": "control/control-plane.sqlite3" if control_present else None, "runtime": "runtime/"}})
            _atomic_json(staging / "config" / "pointers.json", pointers)
            files = _manifest_files(staging)
            sqlite_files = ["paper/paper.sqlite3", "recorder/recorder.sqlite3"]
            if control_present:
                sqlite_files.append("control/control-plane.sqlite3")
            manifest = {"schema_version": 1, "owner": "neorome.paper_recovery", "bundle_date": current.date().isoformat(), "created_at": _iso(current), "components": components, "sqlite_files": sqlite_files, "pointers": pointers, "files": files, "immutable": True}
            _write_manifest(staging, manifest)
            verification = verify_bundle(staging)
            if verification.get("status") != "valid":
                raise RecoveryError("staged bundle verification failed")
            if _state_size(root) > MAX_COMBINED_BYTES:
                raise RecoveryError("recovery storage limit")
            os.replace(staging, existing)
            verification = verify_bundle(existing)
            _atomic_json(claim_path, {"schema_version": 1, "bundle_date": current.date().isoformat(), "started_at": _iso(current), "completed_at": _iso(current), "status": "completed", "bundle": str(existing)})
            _prune(root)
            return _status(root, now=current, healthy=True, status="backup_created", bundle=str(existing), verification=verification, daily_latest_date=latest_daily, daily_current_date_present=components["daily"]["current_date_present"])
        except Exception as exc:
            if staging.exists():
                shutil.rmtree(staging, ignore_errors=True)
            _status(root, now=current, healthy=False, status="failed", error=type(exc).__name__)
            return {"status": "failed", "operational_healthy": False, "bundle_date": current.date().isoformat(), "error": type(exc).__name__}
    finally:
        if pilot_stream is not None:
            pilot_stream.close()
        if runtime_stream is not None:
            runtime_stream.close()


def restore_bundle(bundle: str | os.PathLike[str], destination: str | os.PathLike[str]) -> dict[str, Any]:
    verification = verify_bundle(bundle)
    if verification.get("status") != "valid":
        raise RecoveryError("bundle verification failed")
    source_root = Path(bundle).resolve()
    target = Path(destination)
    if target.exists() or os.path.lexists(target):
        raise RecoveryError("restore destination already exists")
    target.mkdir(parents=True, mode=0o700)
    try:
        manifest = _read_json(source_root / MANIFEST_NAME)
        files = {entry["path"]: entry for entry in manifest["files"]}
        for relative in files:
            source = source_root / relative
            restored_path = target / relative
            # Verified bundles already contain standalone SQLite snapshots.
            # Opening their sources with SQLite can create WAL/SHM sidecars.
            _copy_file(source, restored_path)
            os.chmod(restored_path, 0o600)
        for relative in manifest.get("sqlite_files", []):
            _integrity(target / relative)
        # A restored tree is deliberately a runtime layout, not a new bundle;
        # validate the known payloads directly after copying from the manifest.
        if not (target / "paper/paper.sqlite3").is_file() or not (target / "pilot/state.json").is_file() or not (target / "recorder/recorder.sqlite3").is_file():
            raise RecoveryError("restored required layout is incomplete")
        return {"status": "restore_verified", "destination": str(target), "source": str(source_root), "files": len(files), "source_verification": verification}
    except Exception:
        shutil.rmtree(target, ignore_errors=True)
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    run_parser = sub.add_parser("run")
    run_parser.add_argument("--state", default=DEFAULT_RECOVERY_ROOT)
    verify_parser = sub.add_parser("verify")
    verify_parser.add_argument("--bundle", required=True)
    restore_parser = sub.add_parser("restore")
    restore_parser.add_argument("--bundle", required=True)
    restore_parser.add_argument("--destination", required=True)
    health_parser = sub.add_parser("health")
    health_parser.add_argument("--state", default=DEFAULT_RECOVERY_ROOT)
    args = parser.parse_args(argv)
    try:
        if args.command == "run":
            result = run_once(args.state)
        elif args.command == "verify":
            result = verify_bundle(args.bundle)
        elif args.command == "restore":
            result = restore_bundle(args.bundle, args.destination)
        else:
            result = health_check(args.state)
    except AlreadyRunning:
        result = {"status": "already_running", "operational_healthy": False}
    except Exception as exc:
        result = {"status": "error", "operational_healthy": False, "error": type(exc).__name__}
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0 if result.get("status") in {"backup_created", "already_verified", "valid", "restore_verified", "healthy", "already_running"} else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["DEFAULT_RECOVERY_ROOT", "MAX_COMBINED_BYTES", "RecoveryError", "health_check", "main", "restore_bundle", "run_once", "verify_bundle"]
