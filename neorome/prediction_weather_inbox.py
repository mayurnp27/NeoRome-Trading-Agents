"""Sanitized, immutable handoff for weather candidates and later snapshots.

The writer never handles credentials or provider raw payloads.  The reader is
used by the sole ``neorome`` service that can open the shared paper ledger.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile
from typing import Any, Mapping

FORBIDDEN = frozenset({"token", "tokens", "authorization", "password", "secret", "raw", "response", "responses", "raw_response_b64"})


class PredictionWeatherInboxError(ValueError):
    pass


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _safe(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str) or key.lower() in FORBIDDEN: raise PredictionWeatherInboxError("inbox artifact includes unsanitized provider material")
            _safe(item)
    elif isinstance(value, list):
        for item in value: _safe(item)
    elif value is not None and not isinstance(value, (str, int, float, bool)):
        raise PredictionWeatherInboxError("inbox artifact is not JSON")


def _root(value: str | os.PathLike[str]) -> Path:
    root = Path(value)
    if root.exists():
        info = os.lstat(root)
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode) or info.st_mode & 0o007: raise PredictionWeatherInboxError("inbox root mode is unsafe")
    else: root.mkdir(parents=True, mode=0o750)
    os.chmod(root, 0o750); return root


def _write_once(path: Path, value: Mapping[str, Any]) -> None:
    payload = _canonical(value) + b"\n"
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0), 0o640)
    except FileExistsError:
        if path.is_symlink() or not path.is_file(): raise PredictionWeatherInboxError("inbox artifact is unsafe")
        try: existing = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as exc: raise PredictionWeatherInboxError("inbox artifact is unreadable") from exc
        if _canonical(existing) != _canonical(value): raise PredictionWeatherInboxError("inbox replay conflicts")
    except OSError as exc: raise PredictionWeatherInboxError("inbox artifact cannot be created") from exc
    else:
        with os.fdopen(descriptor, "wb") as stream: stream.write(payload); stream.flush(); os.fsync(stream.fileno())


def _pointer(path: Path, value: Mapping[str, Any]) -> None:
    if path.is_symlink() or (path.exists() and not path.is_file()): raise PredictionWeatherInboxError("inbox pointer is unsafe")
    descriptor, temporary = tempfile.mkstemp(prefix=".prediction-weather-inbox-", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o640)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1; stream.write(_canonical(value) + b"\n"); stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if descriptor >= 0: os.close(descriptor)
        try: os.unlink(temporary)
        except FileNotFoundError: pass


def _candidate(value: Mapping[str, Any]) -> tuple[dict[str, Any], str]:
    supplied = value.get("candidate_sha256")
    body = dict(value); body.pop("candidate_sha256", None)
    if not isinstance(supplied, str) or _digest(body) != supplied: raise PredictionWeatherInboxError("candidate hash is invalid")
    if value.get("source_kind") != "observed" or value.get("shadow_version") != "prediction-observed-shadow-v1": raise PredictionWeatherInboxError("candidate identity is invalid")
    _safe(value); return dict(value), supplied


def publish_candidate(candidate: Mapping[str, Any], root: str | os.PathLike[str], *, execution_snapshot_sha256: str | None = None) -> Path:
    clean, candidate_hash = _candidate(candidate); path = _root(root) / "candidates"; path.mkdir(mode=0o750, exist_ok=True)
    if execution_snapshot_sha256 is not None and (not isinstance(execution_snapshot_sha256, str) or len(execution_snapshot_sha256) != 64): raise PredictionWeatherInboxError("entry snapshot hash is invalid")
    envelope = {"schema_version": 1, "artifact_kind": "prediction-weather-candidate-inbox-v1", "candidate_sha256": candidate_hash, "candidate": clean, "execution_snapshot_sha256": execution_snapshot_sha256}
    envelope["artifact_sha256"] = _digest(envelope); output = path / (candidate_hash + ".json"); _write_once(output, envelope)
    _pointer(_root(root) / "current-candidate.json", {"schema_version": 1, "artifact": output.relative_to(_root(root)).as_posix(), "sha256": envelope["artifact_sha256"]})
    return output


def publish_snapshot(snapshot: Mapping[str, Any], root: str | os.PathLike[str]) -> Path:
    supplied = snapshot.get("snapshot_sha256"); body = dict(snapshot); body.pop("snapshot_sha256", None)
    if not isinstance(supplied, str) or _digest(body) != supplied: raise PredictionWeatherInboxError("snapshot hash is invalid")
    if snapshot.get("artifact_kind") != "prediction-weather-execution-snapshot-v1": raise PredictionWeatherInboxError("snapshot identity is invalid")
    clean = dict(snapshot); _safe(clean); path = _root(root) / "snapshots"; path.mkdir(mode=0o750, exist_ok=True)
    envelope = {"schema_version": 1, "artifact_kind": "prediction-weather-snapshot-inbox-v1", "snapshot_sha256": supplied, "snapshot": clean}
    envelope["artifact_sha256"] = _digest(envelope); output = path / (supplied + ".json"); _write_once(output, envelope)
    _pointer(_root(root) / "current-snapshot.json", {"schema_version": 1, "artifact": output.relative_to(_root(root)).as_posix(), "sha256": envelope["artifact_sha256"]})
    return output


def _read(root: str | os.PathLike[str], pointer_name: str, kind: str) -> dict[str, Any] | None:
    base = Path(root); pointer = base / pointer_name
    if not pointer.exists(): return None
    if pointer.is_symlink() or not pointer.is_file(): raise PredictionWeatherInboxError("inbox pointer is unsafe")
    try:
        address = json.loads(pointer.read_text()); relative = Path(address["artifact"])
        if relative.is_absolute() or not relative.parts or any(part in {"", ".", ".."} for part in relative.parts): raise ValueError("unsafe artifact path")
        source = base / relative
        if source.is_symlink() or not source.is_file(): raise ValueError("unsafe artifact")
        envelope = json.loads(source.read_text())
    except (OSError, KeyError, TypeError, json.JSONDecodeError) as exc: raise PredictionWeatherInboxError("inbox pointer is unreadable") from exc
    if source.is_symlink() or not source.is_file() or envelope.get("artifact_kind") != kind: raise PredictionWeatherInboxError("inbox envelope is invalid")
    body = dict(envelope); supplied = body.pop("artifact_sha256", None)
    if _digest(body) != supplied or address.get("sha256") != supplied: raise PredictionWeatherInboxError("inbox envelope hash is invalid")
    return dict(envelope)


def read_current_candidate(root: str | os.PathLike[str]) -> dict[str, Any] | None:
    envelope = _read(root, "current-candidate.json", "prediction-weather-candidate-inbox-v1")
    if envelope is None: return None
    candidate, digest = _candidate(envelope.get("candidate") or {})
    if digest != envelope.get("candidate_sha256"): raise PredictionWeatherInboxError("candidate source hash is invalid")
    return candidate


def read_current_candidate_record(root: str | os.PathLike[str]) -> dict[str, Any] | None:
    envelope = _read(root, "current-candidate.json", "prediction-weather-candidate-inbox-v1")
    if envelope is None: return None
    candidate, digest = _candidate(envelope.get("candidate") or {})
    if digest != envelope.get("candidate_sha256"): raise PredictionWeatherInboxError("candidate source hash is invalid")
    return {"candidate": candidate, "execution_snapshot_sha256": envelope.get("execution_snapshot_sha256")}


def read_current_snapshot(root: str | os.PathLike[str]) -> dict[str, Any] | None:
    envelope = _read(root, "current-snapshot.json", "prediction-weather-snapshot-inbox-v1")
    if envelope is None: return None
    snapshot = envelope.get("snapshot")
    if not isinstance(snapshot, Mapping): raise PredictionWeatherInboxError("snapshot is invalid")
    body = dict(snapshot); supplied = body.pop("snapshot_sha256", None)
    if _digest(body) != supplied or supplied != envelope.get("snapshot_sha256"): raise PredictionWeatherInboxError("snapshot source hash is invalid")
    return dict(snapshot)
