"""Offline backup, restore rehearsal, and health tools for the recorder.

This module intentionally has no credential, network, cloud, or alerting
dependencies.  It operates on a recorder state directory and emits data that
an external supervisor can poll itself.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
from typing import Any


UTC = timezone.utc
DATABASE_NAME = "recorder.sqlite3"
MANIFEST_NAME = "manifest.json"
MANIFEST_SHA_NAME = "manifest.sha256"
DEFAULT_HEARTBEAT_SECONDS = 180
DEFAULT_DATA_STALE_SECONDS = 180


class RecorderOpsError(Exception):
    """A safe, operator-facing recorder operations error."""


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp is not a string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp has no timezone")
    return parsed.astimezone(UTC)


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_file(path: Path) -> None:
    with path.open("rb") as stream:
        os.fsync(stream.fileno())


def _ensure_regular(path: Path, description: str) -> None:
    try:
        info = path.lstat()
    except FileNotFoundError:
        raise RecorderOpsError(f"Missing {description}") from None
    if not path.is_file() or os.path.islink(path):
        raise RecorderOpsError(f"{description} is not a regular file")
    if not info.st_size >= 0:  # pragma: no cover - defensive filesystem check
        raise RecorderOpsError(f"Invalid {description} size")


def _valid_database_name(database: str) -> str:
    if not database or Path(database).name != database or database in (".", ".."):
        raise RecorderOpsError("Database name must be a single filename")
    return database


def _database_path(state: str | os.PathLike[str], database: str = DATABASE_NAME) -> Path:
    root = Path(state)
    if root.is_symlink() or not root.is_dir():
        raise RecorderOpsError("Recorder state is not a directory")
    path = root / _valid_database_name(database)
    _ensure_regular(path, "recorder database")
    return path


def _open_readonly(path: Path) -> sqlite3.Connection:
    try:
        return sqlite3.connect(
            f"file:{path.resolve()}?mode=ro", uri=True, timeout=30.0
        )
    except sqlite3.Error as exc:
        raise RecorderOpsError("Unable to open recorder database") from exc


def _integrity_check(path: Path) -> str:
    db = None
    try:
        db = _open_readonly(path)
        row = db.execute("PRAGMA integrity_check").fetchone()
    except (sqlite3.Error, RecorderOpsError) as exc:
        if isinstance(exc, RecorderOpsError):
            raise
        raise RecorderOpsError("Recorder database integrity check failed") from exc
    finally:
        if db is not None:
            db.close()
    if not row or row[0] != "ok":
        raise RecorderOpsError("Recorder database integrity check failed")
    return "ok"


def _write_new(path: Path, payload: bytes, mode: int = 0o444) -> None:
    try:
        with path.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(path, mode)
    except FileExistsError:
        raise RecorderOpsError(f"Refusing to overwrite {path.name}") from None


def backup_recorder(
    state: str | os.PathLike[str],
    output: str | os.PathLike[str],
    *,
    now: datetime | None = None,
    database: str = DATABASE_NAME,
) -> dict[str, Any]:
    """Create one immutable backup directory using SQLite's backup API.

    ``output`` must not exist.  A fresh directory is reserved with O_EXCL-like
    mkdir semantics before the snapshot is written, so a later run cannot
    replace an earlier backup.
    """
    database = _valid_database_name(database)
    source_path = _database_path(state, database)
    destination = Path(output)
    if destination.exists() or os.path.lexists(destination):
        raise RecorderOpsError("Backup output already exists")
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        destination.mkdir(mode=0o700)
    except FileExistsError:
        raise RecorderOpsError("Backup output already exists") from None

    backup_path = destination / database
    try:
        source = _open_readonly(source_path)
        try:
            target = sqlite3.connect(backup_path)
            try:
                source.backup(target, pages=128, sleep=0.05)
                target.commit()
            finally:
                target.close()
        finally:
            source.close()
        os.chmod(backup_path, 0o444)
        _fsync_file(backup_path)
        integrity = _integrity_check(backup_path)
        database = {
            "filename": database,
            "size_bytes": backup_path.stat().st_size,
            "sha256": _sha256(backup_path),
            "integrity_check": integrity,
        }
        manifest = {
            "schema_version": 1,
            "created_at": (now or _utc_now()).astimezone(UTC).isoformat(),
            "database": database,
            "immutable": True,
        }
        manifest_bytes = _json_bytes(manifest)
        _write_new(destination / MANIFEST_NAME, manifest_bytes)
        _write_new(
            destination / MANIFEST_SHA_NAME,
            f"{hashlib.sha256(manifest_bytes).hexdigest()}  {MANIFEST_NAME}\n".encode("ascii"),
        )
        _fsync_file(destination / MANIFEST_NAME)
        _fsync_file(destination / MANIFEST_SHA_NAME)
        os.chmod(destination, 0o500)
        return {
            "status": "backup_created",
            "backup": str(destination),
            "database": database,
            "manifest": MANIFEST_NAME,
        }
    except Exception:
        # The directory was created by this call and is not a user backup yet.
        shutil.rmtree(destination, ignore_errors=True)
        raise


def _invalid(errors: list[str], backup: Path) -> dict[str, Any]:
    return {"status": "invalid", "backup": str(backup), "errors": errors}


def verify_backup(backup: str | os.PathLike[str]) -> dict[str, Any]:
    """Verify manifest checksums and SQLite integrity without modifying input."""
    root = Path(backup)
    errors: list[str] = []
    if root.is_symlink() or not root.is_dir():
        return _invalid(["backup is not a directory"], root)
    manifest_path = root / MANIFEST_NAME
    manifest_sha_path = root / MANIFEST_SHA_NAME
    for path, description in (
        (manifest_path, "manifest"),
        (manifest_sha_path, "manifest checksum"),
    ):
        try:
            _ensure_regular(path, description)
        except RecorderOpsError as exc:
            errors.append(str(exc))
    if errors:
        return _invalid(errors, root)
    try:
        manifest_bytes = manifest_path.read_bytes()
        manifest = json.loads(manifest_bytes.decode("utf-8"))
        if not isinstance(manifest, dict) or manifest.get("schema_version") != 1:
            errors.append("unsupported manifest")
        checksum_text = manifest_sha_path.read_text(encoding="ascii").strip().split()
        expected_manifest_sha = checksum_text[0] if checksum_text else ""
        if expected_manifest_sha != hashlib.sha256(manifest_bytes).hexdigest():
            errors.append("manifest checksum mismatch")
        database = manifest.get("database") if isinstance(manifest, dict) else None
        database_name = database.get("filename") if isinstance(database, dict) else None
        if not isinstance(database, dict) or not isinstance(database_name, str):
            errors.append("manifest database entry is invalid")
        else:
            try:
                database_name = _valid_database_name(database_name)
            except RecorderOpsError:
                errors.append("manifest database filename is invalid")
                database_name = None
        db_path = root / database_name if database_name is not None else None
        if db_path is not None:
            try:
                _ensure_regular(db_path, "database")
            except RecorderOpsError as exc:
                errors.append(str(exc))
                db_path = None
        if db_path is not None:
            if database.get("size_bytes") != db_path.stat().st_size:
                errors.append("database size mismatch")
            if database.get("sha256") != _sha256(db_path):
                errors.append("database checksum mismatch")
        if db_path is not None and not errors:
            _integrity_check(db_path)
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError, sqlite3.Error, RecorderOpsError):
        errors.append("manifest or database verification failed")
    if errors:
        # Preserve order while avoiding duplicate diagnostics.
        return _invalid(list(dict.fromkeys(errors)), root)
    return {
        "status": "valid",
        "backup": str(root),
        "database": manifest["database"],
    }


def restore_backup(
    backup: str | os.PathLike[str], destination: str | os.PathLike[str]
) -> dict[str, Any]:
    """Rehearse restore into a brand-new state directory.

    Existing paths, including an existing live recorder state, are refused.
    The input backup is opened read-only and is never changed.
    """
    verification = verify_backup(backup)
    if verification["status"] != "valid":
        raise RecorderOpsError("Backup verification failed")
    backup_root = Path(backup).resolve()
    target = Path(destination)
    if target.exists() or os.path.lexists(target):
        raise RecorderOpsError("Restore destination already exists")
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        target.mkdir(mode=0o700)
    except FileExistsError:
        raise RecorderOpsError("Restore destination already exists") from None
    database_name = verification["database"]["filename"]
    restored_db = target / database_name
    try:
        source = _open_readonly(backup_root / database_name)
        try:
            destination_db = sqlite3.connect(restored_db)
            try:
                source.backup(destination_db, pages=128, sleep=0.05)
                destination_db.commit()
            finally:
                destination_db.close()
        finally:
            source.close()
        os.chmod(restored_db, 0o600)
        integrity = _integrity_check(restored_db)
        return {
            "status": "restore_verified",
            "restore": str(target),
            "database": {"filename": database_name, "integrity_check": integrity},
        }
    except Exception:
        shutil.rmtree(target, ignore_errors=True)
        raise


def rehearse_restore(
    backup: str | os.PathLike[str], destination: str | os.PathLike[str]
) -> dict[str, Any]:
    """Compatibility name for the operator-facing restore rehearsal."""
    return restore_backup(backup, destination)


def _storage_counts(root: Path, database: str = DATABASE_NAME) -> dict[str, int]:
    total = 0
    files = 0
    for path in root.iterdir():
        if path.is_file() and not path.is_symlink():
            files += 1
            try:
                total += path.stat().st_size
            except OSError:
                pass
    database_path = root / database
    return {
        "state_bytes": total,
        "file_count": files,
        "database_bytes": database_path.stat().st_size if database_path.is_file() else 0,
    }


def _find_source_timestamp(value: Any) -> datetime | None:
    """Find a provider observation timestamp without naming an instrument."""
    if isinstance(value, dict):
        # Both stock and crypto provider payloads put the quote timestamp at
        # `quotes.<instrument>.t`; recurse through any instrument key.
        for key in ("quotes", "quote"):
            if key in value:
                found = _find_source_timestamp(value[key])
                if found is not None:
                    return found
        for key in ("t", "timestamp", "observed_at"):
            if key in value:
                try:
                    return _timestamp(value[key])
                except ValueError:
                    pass
        for child in value.values():
            found = _find_source_timestamp(child)
            if found is not None:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _find_source_timestamp(child)
            if found is not None:
                return found
    return None


def _extract_quote_timestamp(observation: str | None) -> datetime | None:
    if not observation:
        return None
    try:
        return _find_source_timestamp(json.loads(observation))
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None


def health_summary(
    state: str | os.PathLike[str],
    now: datetime | None = None,
    *,
    heartbeat_seconds: int = DEFAULT_HEARTBEAT_SECONDS,
    data_stale_seconds: int = DEFAULT_DATA_STALE_SECONDS,
    database: str = DATABASE_NAME,
) -> dict[str, Any]:
    """Return credential-independent health suitable for an external poller."""
    root = Path(state)
    current = (now or _utc_now()).astimezone(UTC)
    base: dict[str, Any] = {
        "status": "not_started",
        "execution_enabled": False,
        "process": {"status": "not_started"},
        "data": {"status": "unknown"},
        "last_failure": None,
        "last_incomplete": None,
        "storage": {"state_bytes": 0, "file_count": 0, "database_bytes": 0},
    }
    if root.is_symlink() or not root.is_dir():
        return base
    try:
        database = _valid_database_name(database)
    except RecorderOpsError:
        base["status"] = "unhealthy"
        base["process"] = {"status": "invalid_database_name"}
        return base
    base["storage"] = _storage_counts(root, database)
    database_path = root / database
    if not database_path.is_file() or database_path.is_symlink():
        return base
    try:
        with _open_readonly(database_path) as db:
            rows = db.execute(
                "SELECT id,started_at,status,detail,observation FROM attempts ORDER BY id DESC"
            ).fetchall()
    except (RecorderOpsError, sqlite3.Error):
        base["status"] = "unhealthy"
        base["process"] = {"status": "database_error"}
        return base
    if not rows:
        return base
    latest = rows[0]
    _, heartbeat_text, heartbeat_status, _, _ = latest
    process: dict[str, Any] = {
        "status": "healthy",
        "heartbeat_at": heartbeat_text,
        "last_attempt_status": heartbeat_status,
    }
    try:
        age = (current - _timestamp(heartbeat_text)).total_seconds()
        process["heartbeat_age_seconds"] = age
        if age < 0 or age > heartbeat_seconds:
            process["status"] = "heartbeat_stale"
    except ValueError:
        process["status"] = "invalid_heartbeat"
    base["process"] = process

    for row in rows:
        identifier, started_at, status, detail, observation = row
        entry = {"attempt_id": identifier, "at": started_at, "status": status}
        if status == "incomplete" and base["last_incomplete"] is None:
            base["last_incomplete"] = entry
        if status in ("fetch_or_validation_failed", "storage_limit") and base["last_failure"] is None:
            base["last_failure"] = {**entry, "detail": detail}

    successful = next((row for row in rows if row[2] == "fresh" and row[4]), None)
    data: dict[str, Any] = {"status": "unknown"}
    if successful:
        identifier, started_at, _, _, observation = successful
        source_at = _extract_quote_timestamp(observation)
        data.update({"attempt_id": identifier, "attempted_at": started_at})
        if source_at is None:
            data["status"] = "stale"
        else:
            data["source_timestamp"] = source_at.isoformat()
            age = (current - source_at).total_seconds()
            data["age_seconds"] = age
            data["status"] = "fresh" if 0 <= age <= data_stale_seconds else "stale"
    elif any(row[2] == "stale_or_unusable" for row in rows):
        data["status"] = "stale"
    base["data"] = data
    base["attempts"] = {
        "total": len(rows),
        "observations": sum(row[4] is not None for row in rows),
        "failures": sum(row[2] in ("fetch_or_validation_failed", "storage_limit") for row in rows),
        "incomplete": sum(row[2] == "incomplete" for row in rows),
    }
    if process["status"] != "healthy":
        base["status"] = "unhealthy"
    elif data["status"] == "stale":
        base["status"] = "stale_data"
    elif data["status"] == "unknown":
        base["status"] = "no_data"
    else:
        base["status"] = "healthy"
    return base


def _health_exit(summary: dict[str, Any]) -> int:
    status = summary.get("status")
    if status == "healthy":
        return 0
    if status == "stale_data":
        return 3
    if status == "no_data":
        return 4
    return 1


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    health = sub.add_parser("health", aliases=["healthcheck"], help="print credential-independent health JSON")
    health.add_argument("--state", required=True)
    health.add_argument("--heartbeat-seconds", type=int, default=DEFAULT_HEARTBEAT_SECONDS)
    health.add_argument("--data-stale-seconds", type=int, default=DEFAULT_DATA_STALE_SECONDS)
    health.add_argument("--database", default=DATABASE_NAME)
    backup = sub.add_parser("backup", help="create a new immutable SQLite backup")
    backup.add_argument("--state", required=True)
    backup.add_argument("--output", required=True)
    backup.add_argument("--database", default=DATABASE_NAME)
    verify = sub.add_parser("verify", help="verify a backup")
    verify.add_argument("--backup", required=True)
    restore = sub.add_parser("restore", help="restore into a new state directory")
    restore.add_argument("--backup", required=True)
    restore.add_argument("--state", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command in ("health", "healthcheck"):
            result = health_summary(
                args.state,
                heartbeat_seconds=args.heartbeat_seconds,
                data_stale_seconds=args.data_stale_seconds,
                database=args.database,
            )
            code = _health_exit(result)
        elif args.command == "backup":
            result = backup_recorder(args.state, args.output, database=args.database)
            code = 0
        elif args.command == "verify":
            result = verify_backup(args.backup)
            code = 0 if result["status"] == "valid" else 2
        else:
            result = restore_backup(args.backup, args.state)
            code = 0
    except (OSError, RecorderOpsError, sqlite3.Error, ValueError) as exc:
        result = {"status": "error", "error": type(exc).__name__}
        code = 2
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
