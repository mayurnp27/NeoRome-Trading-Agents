"""Private, date-addressed artifacts for the finite options paper route.

The pointer is a regular JSON file, never a symlink.  Each stage first writes
an immutable dated artifact and then atomically advances/extends that pointer.
That makes retries idempotent without treating a stale fixed filename as a
valid result.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile
from typing import Any, Mapping


class DatedArtifactError(ValueError):
    pass


def canonical(value: Any) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise DatedArtifactError("artifact is not canonical JSON") from exc


def sha256(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def _regular(path: Path, *, missing_ok: bool = False) -> bool:
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        if missing_ok:
            return False
        raise DatedArtifactError("artifact path is missing") from None
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise DatedArtifactError("symlink or non-regular artifact path refused")
    return True


def _root(path: str | os.PathLike[str]) -> Path:
    root = Path(path)
    if root.exists():
        if root.is_symlink() or not root.is_dir():
            raise DatedArtifactError("artifact root is not a directory")
    else:
        root.mkdir(parents=True, mode=0o700)
    try:
        os.chmod(root, 0o700)
    except OSError as exc:
        raise DatedArtifactError("cannot secure artifact root") from exc
    return root


def _date(value: str) -> str:
    if len(value) != 10 or value[4] != "-" or value[7] != "-" or not value.replace("-", "").isdigit():
        raise DatedArtifactError("artifact date is malformed")
    return value


def _name(value: str) -> str:
    if not value.endswith(".json") or "/" in value or value.startswith("."):
        raise DatedArtifactError("artifact name is malformed")
    return value


def _read_json(path: Path) -> dict[str, Any]:
    _regular(path)
    try:
        result = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise DatedArtifactError("artifact JSON is unreadable") from exc
    if not isinstance(result, dict):
        raise DatedArtifactError("artifact JSON must be an object")
    return result


def read_current(root: str | os.PathLike[str]) -> dict[str, Any]:
    path = _root(root) / "current.json"
    return _read_json(path)


def resolve_current(root: str | os.PathLike[str], *names: str) -> dict[str, Path]:
    state = read_current(root)
    date = _date(state.get("date"))
    artifacts = state.get("artifacts")
    if not isinstance(artifacts, Mapping):
        raise DatedArtifactError("current pointer has no artifacts")
    result: dict[str, Path] = {}
    base = _root(root) / date
    for name in names:
        item = artifacts.get(name)
        if not isinstance(item, Mapping) or not isinstance(item.get("sha256"), str):
            raise DatedArtifactError(f"current pointer is missing {name}")
        path = base / _name(item.get("file", name))
        data = _read_json(path)
        if sha256(data) != item["sha256"]:
            raise DatedArtifactError(f"current pointer hash mismatch for {name}")
        result[name] = path
    return result


def _replace_json(path: Path, value: Mapping[str, Any]) -> None:
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise DatedArtifactError("current pointer is not a regular file")
    payload = canonical(value) + b"\n"
    temporary = None
    try:
        descriptor, temporary = tempfile.mkstemp(prefix=".options-current-", dir=str(path.parent))
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        temporary = None
    except OSError as exc:
        raise DatedArtifactError("cannot atomically update current pointer") from exc
    finally:
        if temporary is not None:
            try:
                os.unlink(temporary)
            except OSError:
                pass


def save_stage(root: str | os.PathLike[str], date: str, name: str, value: Mapping[str, Any], *, current_name: str | None = None) -> Path:
    """Persist one immutable stage and atomically point the date at it."""
    root_path, date, name = _root(root), _date(date), _name(name)
    directory = root_path / date
    if directory.exists() and (directory.is_symlink() or not directory.is_dir()):
        raise DatedArtifactError("dated artifact directory is unsafe")
    directory.mkdir(mode=0o700, exist_ok=True)
    os.chmod(directory, 0o700)
    path, payload = directory / name, canonical(value) + b"\n"
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    except FileExistsError:
        existing = _read_json(path)
        if canonical(existing) != canonical(value):
            raise DatedArtifactError("dated artifact collision has different content")
    except OSError as exc:
        raise DatedArtifactError("cannot create dated artifact") from exc
    else:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    old: dict[str, Any] = {}
    pointer = root_path / "current.json"
    if pointer.is_symlink():
        raise DatedArtifactError("current pointer is not a regular file")
    if pointer.exists():
        old = _read_json(pointer)
        old_date = _date(old.get("date"))
        if old_date > date:
            raise DatedArtifactError("refusing to roll current pointer backward")
        if old_date == date and isinstance(old.get("artifacts"), Mapping):
            old = dict(old["artifacts"])
        else:
            old = {}
    artifacts = dict(old)
    pointer_name = _name(current_name) if current_name is not None else name
    artifacts[pointer_name] = {"sha256": sha256(value), "file": name}
    _replace_json(pointer, {"schema_version": 1, "date": date, "artifacts": artifacts})
    return path
