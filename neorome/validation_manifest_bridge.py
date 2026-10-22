"""Publish a strict validation manifest from native desk input inboxes.

The bridge is deliberately a local file boundary.  Desk collectors place a
native ``input.csv`` or ``input.json`` beside ``current.json``; this process
checks the declared hash and validator contract, retains the exact bytes under
the validation StateDirectory, and publishes a scheduler-compatible manifest.
It never fetches, transforms, or submits anything.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import tempfile
from typing import Any, Mapping, Sequence

from .validation_scheduler import (
    DESK_ORDER,
    MANIFEST_SCHEMA,
    MAX_INPUT_BYTES,
    ManifestError,
    VALIDATORS,
    _validate_config,
)


UTC = timezone.utc
CURRENT_MAX_BYTES = 128 * 1024
STATUS_SCHEMA = "NEOROME-VALIDATION-PUBLISHER-STATUS-v1"
_HEX64 = re.compile(r"^[0-9a-fA-F]{64}$")
_CURRENT_KEYS = {"input", "input_sha256", "boundary_at", "validator", "schedule", "config"}
_SUFFIXES = {"equities": ".csv", "options": ".json", "crypto": ".json", "prediction": ".json"}


class BridgeError(ValueError):
    """A desk inbox or controlled publication path is outside the contract."""

    def __init__(self, blocker: str, detail: str):
        super().__init__(detail)
        self.blocker = blocker


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise BridgeError("current_invalid", "value is not canonical JSON") from exc


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _timestamp(value: Any) -> str:
    if not isinstance(value, str):
        raise BridgeError("boundary_invalid", "boundary_at must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BridgeError("boundary_invalid", "boundary_at must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise BridgeError("boundary_invalid", "boundary_at must include a timezone")
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _no_duplicate_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _json_object(raw: bytes, *, label: str) -> Mapping[str, Any]:
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_no_duplicate_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise BridgeError("current_invalid_json", f"{label} is not valid JSON") from exc
    if not isinstance(value, Mapping):
        raise BridgeError("current_invalid", f"{label} must be a JSON object")
    return value


def _reject_symlink(path: Path, *, blocker: str = "symlink") -> None:
    try:
        info = os.lstat(path)
    except FileNotFoundError as exc:
        raise BridgeError("missing", f"path does not exist: {path}") from exc
    except OSError as exc:
        raise BridgeError("path_unreadable", f"cannot inspect path: {path}") from exc
    if stat.S_ISLNK(info.st_mode):
        raise BridgeError(blocker, f"symlinks are not allowed: {path}")


def _regular_bytes(path: Path, *, max_bytes: int, missing_blocker: str, symlink_blocker: str, label: str) -> bytes:
    try:
        info = os.lstat(path)
    except FileNotFoundError as exc:
        raise BridgeError(missing_blocker, f"{label} is missing") from exc
    except OSError as exc:
        raise BridgeError("path_unreadable", f"cannot inspect {label}") from exc
    if stat.S_ISLNK(info.st_mode):
        raise BridgeError(symlink_blocker, f"symlinks are not allowed: {label}")
    if not stat.S_ISREG(info.st_mode):
        raise BridgeError("not_regular", f"{label} must be a regular file")
    if info.st_size > max_bytes:
        raise BridgeError("oversize", f"{label} exceeds {max_bytes} bytes")
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise BridgeError("path_unreadable", f"cannot read {label}") from exc
    if len(raw) > max_bytes:
        raise BridgeError("oversize", f"{label} exceeds {max_bytes} bytes")
    return raw


def _ensure_dir(path: Path, *, label: str) -> None:
    if path.is_symlink():
        raise BridgeError("symlink", f"symlinks are not allowed: {label}")
    if path.exists():
        _reject_symlink(path, blocker="symlink")
        if not path.is_dir():
            raise BridgeError("not_directory", f"{label} must be a directory")
        return
    try:
        path.mkdir(parents=True, mode=0o700, exist_ok=True)
    except OSError as exc:
        raise BridgeError("path_unwritable", f"cannot create {label}") from exc
    _reject_symlink(path, blocker="symlink")


def _atomic_write(path: Path, raw: bytes, *, max_bytes: int, label: str) -> None:
    if len(raw) > max_bytes:
        raise BridgeError("oversize", f"{label} exceeds {max_bytes} bytes")
    _ensure_dir(path.parent, label=f"{label} parent")
    if path.exists() or path.is_symlink():
        _reject_symlink(path, blocker="symlink")
        if not path.is_file():
            raise BridgeError("not_regular", f"{label} must be a regular file")
        try:
            if path.read_bytes() == raw:
                return
        except OSError as exc:
            raise BridgeError("path_unreadable", f"cannot read existing {label}") from exc
    temporary: str | None = None
    try:
        descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        if path.is_symlink():
            raise BridgeError("symlink", f"symlinks are not allowed: {label}")
        os.replace(temporary, path)
        temporary = None
        try:
            directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            # The file replacement is already atomic; directory fsync is best
            # effort on filesystems that do not expose directory descriptors.
            pass
    except BridgeError:
        raise
    except OSError as exc:
        raise BridgeError("path_unwritable", f"cannot publish {label}") from exc
    finally:
        if temporary is not None:
            try:
                os.unlink(temporary)
            except OSError:
                pass


def _validate_current(desk: str, value: Mapping[str, Any]) -> dict[str, Any]:
    if set(value) != _CURRENT_KEYS:
        raise BridgeError("unknown_keys", f"{desk}.current.json keys must be exactly {sorted(_CURRENT_KEYS)}")
    expected_input = f"input{_SUFFIXES[desk]}"
    if value.get("input") != expected_input:
        raise BridgeError("input_name_invalid", f"{desk}.input must name sibling {expected_input}")
    digest = value.get("input_sha256")
    if not isinstance(digest, str) or not _HEX64.fullmatch(digest):
        raise BridgeError("input_hash_invalid", f"{desk}.input_sha256 must be a SHA-256 digest")
    if value.get("validator") != VALIDATORS[desk]:
        raise BridgeError("validator_unregistered", f"{desk}.validator is not the registered validator")
    if value.get("schedule") not in {"daily", "weekly"}:
        raise BridgeError("schedule_invalid", f"{desk}.schedule must be daily or weekly")
    boundary = _timestamp(value.get("boundary_at"))
    config = value.get("config")
    try:
        checked_config = _validate_config(desk, config)
    except ManifestError as exc:
        raise BridgeError("config_invalid", str(exc)) from exc
    return {
        "input": expected_input,
        "input_sha256": digest.lower(),
        "boundary_at": boundary,
        "validator": VALIDATORS[desk],
        "schedule": value["schedule"],
        "config": checked_config,
    }


class ValidationManifestBridge:
    """Admit native desk inputs and publish one deterministic manifest."""

    def __init__(self, state_dir: str | os.PathLike[str]):
        self.state_dir = Path(state_dir)
        self.inbox_root = self.state_dir / "inbox"
        self.input_root = self.state_dir / "inputs"
        self.manifest_path = self.state_dir / "manifest.json"
        self.status_path = self.state_dir / "publisher-status.json"

    def _desk(self, desk: str) -> tuple[dict[str, Any], dict[str, Any]]:
        inbox = self.inbox_root / desk
        if not inbox.exists():
            raise BridgeError("desk_inbox_missing", f"{desk} inbox is missing")
        _reject_symlink(inbox, blocker="desk_inbox_symlink")
        if not inbox.is_dir():
            raise BridgeError("desk_inbox_not_directory", f"{desk} inbox is not a directory")
        current = inbox / "current.json"
        current_raw = _regular_bytes(current, max_bytes=CURRENT_MAX_BYTES, missing_blocker="current_missing", symlink_blocker="current_symlink", label=f"{desk} current.json")
        entry = _validate_current(desk, _json_object(current_raw, label=f"{desk} current.json"))
        source = inbox / entry["input"]
        source_raw = _regular_bytes(source, max_bytes=MAX_INPUT_BYTES, missing_blocker="input_missing", symlink_blocker="input_symlink", label=f"{desk} native input")
        actual_hash = _sha256(source_raw)
        if actual_hash != entry["input_sha256"]:
            raise BridgeError("input_hash_mismatch", f"{desk} native input hash does not match current.json")
        retained_name = f"{actual_hash}{_SUFFIXES[desk]}"
        retained = self.input_root / desk / retained_name
        try:
            _atomic_write(retained, source_raw, max_bytes=MAX_INPUT_BYTES, label=f"{desk} retained input")
        except BridgeError as exc:
            raise BridgeError(f"retained_{exc.blocker}", str(exc)) from exc
        manifest_entry = {
            **entry,
            "input": f"{desk}/{retained_name}",
        }
        status = {"status": "ready", "blockers": [], "input_sha256": actual_hash, "boundary_at": entry["boundary_at"]}
        return manifest_entry, status

    def _status(self, *, status: str, run_id: str | None, desks: Mapping[str, Mapping[str, Any]], manifest_sha256: str | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": STATUS_SCHEMA,
            "status": status,
            "run_id": run_id,
            "desks": {desk: dict(desks[desk]) for desk in DESK_ORDER},
        }
        if manifest_sha256 is not None:
            payload["manifest_sha256"] = manifest_sha256
        return payload

    def publish(self) -> dict[str, Any]:
        _ensure_dir(self.state_dir, label="validation state directory")
        _ensure_dir(self.inbox_root, label="validation inbox")
        _ensure_dir(self.input_root, label="validation input root")
        entries: dict[str, dict[str, Any]] = {}
        statuses: dict[str, dict[str, Any]] = {}
        for desk in DESK_ORDER:
            try:
                entry, desk_status = self._desk(desk)
            except BridgeError as exc:
                statuses[desk] = {"status": "blocked", "blockers": [exc.blocker], "error": str(exc)}
            except OSError as exc:
                statuses[desk] = {"status": "blocked", "blockers": ["path_unreadable"], "error": str(exc)}
            else:
                entries[desk] = entry
                statuses[desk] = desk_status
        if not entries:
            result = self._status(status="data_not_ready", run_id=None, desks=statuses)
            _atomic_write(self.status_path, _canonical(result) + b"\n", max_bytes=CURRENT_MAX_BYTES, label="publisher status")
            return result
        created_at = max(entry["boundary_at"] for entry in entries.values())
        manifest_without_run = {
            "schema_version": MANIFEST_SCHEMA,
            "created_at": created_at,
            "desks": entries,
        }
        run_id = "native-" + _sha256(_canonical(manifest_without_run))
        manifest = {**manifest_without_run, "run_id": run_id}
        manifest_raw = _canonical(manifest) + b"\n"
        manifest_sha256 = _sha256(_canonical(manifest))
        _atomic_write(self.manifest_path, manifest_raw, max_bytes=CURRENT_MAX_BYTES, label="validation manifest")
        result = self._status(status="published", run_id=run_id, desks=statuses, manifest_sha256=manifest_sha256)
        _atomic_write(self.status_path, _canonical(result) + b"\n", max_bytes=CURRENT_MAX_BYTES, label="publisher status")
        return result


def _main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", "--state-dir", dest="state", required=True, help="validation StateDirectory")
    args = parser.parse_args(argv)
    try:
        result = ValidationManifestBridge(args.state).publish()
    except (BridgeError, OSError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, sort_keys=True))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_main())


main = _main

__all__ = ["BridgeError", "STATUS_SCHEMA", "ValidationManifestBridge", "main"]
