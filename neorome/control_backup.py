"""Publish a verified standalone control-plane snapshot for recovery bundles."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import uuid
from typing import Any


DEFAULT_DATABASE = "/var/lib/neorome-control/control-plane.sqlite3"
DEFAULT_DESTINATION = "/var/lib/neorome-control/recovery-source.sqlite3"


class ControlBackupError(Exception):
    """The control-plane recovery snapshot could not be published safely."""


def _regular(path: Path, label: str) -> Path:
    try:
        info = path.lstat()
    except FileNotFoundError:
        raise ControlBackupError(f"missing {label}") from None
    if path.is_symlink() or not path.is_file() or info.st_size < 0:
        raise ControlBackupError(f"unsafe {label}")
    return path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def publish_snapshot(
    database: str | os.PathLike[str] = DEFAULT_DATABASE,
    destination: str | os.PathLike[str] = DEFAULT_DESTINATION,
) -> dict[str, Any]:
    source_path = _regular(Path(database), "control database")
    output = Path(destination)
    if output.is_symlink() or source_path.resolve() == output.resolve(strict=False):
        raise ControlBackupError("unsafe snapshot destination")
    parent = output.parent
    if parent.is_symlink():
        raise ControlBackupError("unsafe snapshot directory")
    parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if not parent.is_dir() or parent.is_symlink():
        raise ControlBackupError("unsafe snapshot directory")
    os.chmod(parent, 0o700)
    temporary = parent / f".{output.name}.{uuid.uuid4().hex}.tmp"
    source = target = None
    try:
        source = sqlite3.connect(str(source_path), timeout=30.0)
        source.execute("PRAGMA query_only=ON")
        target = sqlite3.connect(str(temporary), timeout=30.0)
        source.backup(target, pages=128, sleep=0.05)
        target.commit()
        mode = target.execute("PRAGMA journal_mode=DELETE").fetchone()
        integrity = target.execute("PRAGMA integrity_check").fetchone()
        if not mode or str(mode[0]).lower() != "delete" or not integrity or integrity[0] != "ok":
            raise ControlBackupError("snapshot verification failed")
        target.close()
        target = None
        source.close()
        source = None
        os.chmod(temporary, 0o400)
        with temporary.open("rb") as stream:
            os.fsync(stream.fileno())
        size = temporary.stat().st_size
        checksum = _sha256(temporary)
        os.replace(temporary, output)
        directory = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return {"schema_version": 1, "status": "published", "size_bytes": size,
                "sha256": checksum, "journal_mode": "delete"}
    except (OSError, sqlite3.Error, ControlBackupError):
        raise ControlBackupError("unable to publish control snapshot") from None
    finally:
        if target is not None:
            target.close()
        if source is not None:
            source.close()
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", default=DEFAULT_DATABASE)
    parser.add_argument("--destination", default=DEFAULT_DESTINATION)
    args = parser.parse_args(argv)
    try:
        result = publish_snapshot(args.database, args.destination)
    except ControlBackupError as exc:
        result = {"schema_version": 1, "status": "failed", "error": type(exc).__name__}
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0 if result["status"] == "published" else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["ControlBackupError", "publish_snapshot"]
