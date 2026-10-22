"""Bounded, sequential orchestration for deterministic desk validation.

The scheduler is deliberately a small control boundary.  It consumes an
explicit, hash-bound manifest prepared by the collectors, invokes only the
registered local validators, and writes content-addressed reports.  It never
fetches data, fits a model, submits an order, or touches the paper ledger.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import resource
import subprocess
import sys
import tempfile
from typing import Any, Callable, Iterator, Mapping, Sequence


UTC = timezone.utc
MANIFEST_SCHEMA = "NEOROME-VALIDATION-MANIFEST-v1"
REPORT_SCHEMA = "NEOROME-VALIDATION-REPORT-v1"
MAX_MANIFEST_BYTES = 128 * 1024
MAX_INPUT_BYTES = 8 * 1024 * 1024
MAX_REPORT_BYTES = 2 * 1024 * 1024
MAX_STDOUT_BYTES = 64 * 1024
MAX_STDERR_BYTES = 64 * 1024
WORKER_TIMEOUT_SECONDS = 90
WORKER_CPU_SECONDS = 60
WORKER_ADDRESS_SPACE_BYTES = 256 * 1024 * 1024
_HEX64 = re.compile(r"^[0-9a-fA-F]{64}$")
_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")

DESK_ORDER = ("equities", "options", "crypto", "prediction")
VALIDATORS = {
    "equities": "desks.equities.walkforward",
    "options": "desks.options.walk_forward",
    "crypto": "desks.crypto.walkforward",
    "prediction": "neorome.prediction_walkforward",
}
_DESK_KEYS = {"input", "input_sha256", "boundary_at", "validator", "schedule", "config"}
_CONFIG_KEYS = {
    "equities": {"strategy", "development", "validation", "holdout", "purge_days", "cadence", "cost_bps"},
    "options": {"allow_synthetic", "train_sessions", "validation_sessions", "holdout_sessions", "step_sessions", "cost_model"},
    "crypto": set(),
    "prediction": {"fee_schedule", "edge_floor_cents", "development_events", "validation_events", "holdout_events", "baseline"},
}
_SUFFIXES = {"equities": ".csv", "options": ".json", "crypto": ".json", "prediction": ".json"}


class ManifestError(ValueError):
    """The manifest or its referenced input is outside the strict contract."""


class SchedulerError(RuntimeError):
    """The scheduler could not safely produce a run result."""


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ManifestError("value is not canonical JSON") from exc


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _timestamp(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise ManifestError(f"{field} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ManifestError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ManifestError(f"{field} must include a timezone")
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _reject_symlink(path: Path, *, allow_missing: bool = False) -> None:
    """Reject a symlink at the controlled path itself.

    System locations such as macOS ``/var`` may themselves be compatibility
    symlinks.  The input-root traversal below checks every component beneath a
    resolved trusted root, while this helper protects manifests, locks and
    report destinations without rejecting those normal system paths.
    """
    try:
        exists = path.exists()
    except OSError as exc:
        raise ManifestError(f"cannot inspect path: {path}") from exc
    if not exists:
        if allow_missing:
            return
        raise ManifestError(f"path does not exist: {path}")
    if path.is_symlink():
        raise ManifestError(f"symlinks are not allowed: {path}")


def _relative_input(root: Path, value: Any, desk: str) -> Path:
    if not isinstance(value, str) or not value or "\\" in value:
        raise ManifestError(f"{desk}.input must be a relative POSIX path")
    relative = PurePosixPath(value)
    if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
        raise ManifestError(f"{desk}.input must stay below the input root")
    if relative.suffix.lower() != _SUFFIXES[desk]:
        raise ManifestError(f"{desk}.input must end in {_SUFFIXES[desk]}")
    try:
        trusted_root = root.resolve(strict=True)
        path = root.joinpath(*relative.parts)
        resolved = path.resolve(strict=True)
        resolved.relative_to(trusted_root)
    except (OSError, ValueError) as exc:
        raise ManifestError(f"{desk}.input must stay below the input root") from exc
    for index in range(1, len(relative.parts) + 1):
        component = trusted_root.joinpath(*relative.parts[:index])
        if component.is_symlink():
            raise ManifestError(f"symlinks are not allowed: {path}")
    if not resolved.is_file():
        raise ManifestError(f"{desk}.input is not a regular file")
    size = resolved.stat().st_size
    if size > MAX_INPUT_BYTES:
        raise ManifestError(f"{desk}.input exceeds {MAX_INPUT_BYTES} bytes")
    return resolved


def _validate_config(desk: str, config: Any) -> dict[str, Any]:
    if config is None:
        config = {}
    if not isinstance(config, Mapping):
        raise ManifestError(f"{desk}.config must be an object")
    unknown = set(config) - _CONFIG_KEYS[desk]
    if unknown:
        raise ManifestError(f"{desk}.config has unsupported keys: {', '.join(sorted(unknown))}")
    result = dict(config)
    if desk == "equities":
        required = {"strategy", "development", "validation", "holdout"}
        if not required.issubset(result):
            raise ManifestError("equities.config requires strategy, development, validation, and holdout")
        if not isinstance(result["strategy"], str):
            raise ManifestError("equities.config.strategy must be a string")
        for name in ("development", "validation", "holdout"):
            value = result[name]
            if not isinstance(value, list) or len(value) != 2 or any(not isinstance(item, str) for item in value):
                raise ManifestError(f"equities.config.{name} must contain two ISO dates")
            try:
                datetime.fromisoformat(value[0] + "T00:00:00+00:00")
                datetime.fromisoformat(value[1] + "T00:00:00+00:00")
            except ValueError as exc:
                raise ManifestError(f"equities.config.{name} must contain ISO dates") from exc
    if desk == "options" and result.get("allow_synthetic", False) is True:
        raise ManifestError("options synthetic replay is not permitted by the cloud scheduler")
    if desk == "prediction":
        required = {"fee_schedule", "development_events", "validation_events", "holdout_events"}
        if not required.issubset(result):
            raise ManifestError("prediction.config requires fee_schedule and chronological split sizes")
        if not isinstance(result["fee_schedule"], Mapping):
            raise ManifestError("prediction.config.fee_schedule must be an object")
        for name in ("development_events", "validation_events", "holdout_events"):
            value = result[name]
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ManifestError(f"prediction.config.{name} must be a positive integer")
    return result


@dataclass(frozen=True)
class DeskManifest:
    desk: str
    input_path: Path
    input_sha256: str
    boundary_at: str
    validator: str
    schedule: str
    config: dict[str, Any]


@dataclass(frozen=True)
class LoadedManifest:
    run_id: str
    created_at: str
    manifest_sha256: str
    desks: dict[str, DeskManifest]


def load_manifest(path: str | os.PathLike[str], input_root: str | os.PathLike[str]) -> LoadedManifest:
    """Load and validate a strict manifest, including hashes and path safety."""
    manifest_path = Path(path)
    _reject_symlink(manifest_path)
    raw = manifest_path.read_bytes()
    if len(raw) > MAX_MANIFEST_BYTES:
        raise ManifestError(f"manifest exceeds {MAX_MANIFEST_BYTES} bytes")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ManifestError("manifest is not valid JSON") from exc
    if not isinstance(payload, Mapping):
        raise ManifestError("manifest must be an object")
    if set(payload) != {"schema_version", "run_id", "created_at", "desks"}:
        raise ManifestError("manifest keys must be schema_version, run_id, created_at, and desks")
    if payload.get("schema_version") != MANIFEST_SCHEMA:
        raise ManifestError("unsupported manifest schema_version")
    run_id = payload.get("run_id")
    if not isinstance(run_id, str) or not _RUN_ID.fullmatch(run_id):
        raise ManifestError("run_id is not a safe identifier")
    created_at = _timestamp(payload.get("created_at"), "created_at")
    desk_payload = payload.get("desks")
    if not isinstance(desk_payload, Mapping) or not desk_payload:
        raise ManifestError("desks must be a non-empty object")
    root = Path(input_root)
    _reject_symlink(root)
    desks: dict[str, DeskManifest] = {}
    for desk, value in desk_payload.items():
        if desk not in DESK_ORDER:
            raise ManifestError(f"unsupported desk: {desk}")
        if not isinstance(value, Mapping) or set(value) - _DESK_KEYS:
            raise ManifestError(f"{desk} manifest entry has unsupported keys")
        if set(value) != _DESK_KEYS:
            raise ManifestError(f"{desk} manifest entry must contain all required keys")
        validator = value.get("validator")
        if validator != VALIDATORS[desk]:
            raise ManifestError(f"{desk} validator is not the registered local validator")
        schedule = value.get("schedule", "daily")
        if schedule not in {"daily", "weekly"}:
            raise ManifestError(f"{desk}.schedule must be daily or weekly")
        expected_hash = value.get("input_sha256")
        if not isinstance(expected_hash, str) or not _HEX64.fullmatch(expected_hash):
            raise ManifestError(f"{desk}.input_sha256 must be a SHA-256 digest")
        input_path = _relative_input(root, value.get("input"), desk)
        actual_hash = _sha256(input_path.read_bytes())
        if actual_hash.lower() != expected_hash.lower():
            raise ManifestError(f"{desk}.input_sha256 does not match the retained input")
        desks[desk] = DeskManifest(desk, input_path, expected_hash.lower(), _timestamp(value.get("boundary_at"), f"{desk}.boundary_at"), validator, schedule, _validate_config(desk, value.get("config")))
    manifest_hash = _sha256(_canonical(payload))
    return LoadedManifest(run_id, created_at, manifest_hash, desks)


@contextmanager
def global_lock(path: str | os.PathLike[str]) -> Iterator[bool]:
    """Acquire the one scheduler lock without waiting for another run."""
    lock_path = Path(path)
    _reject_symlink(lock_path.parent, allow_missing=True)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    _reject_symlink(lock_path.parent)
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        yield True
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _worker_limits() -> None:
    resource.setrlimit(resource.RLIMIT_CPU, (WORKER_CPU_SECONDS, WORKER_CPU_SECONDS))
    # macOS refuses to lower an unlimited address-space soft limit for a
    # process that already has mapped runtime pages.  The systemd unit still
    # enforces MemoryMax on the cloud host; apply RLIMIT_AS where the kernel
    # accepts it without making the child fail before import.
    try:
        current_soft, current_hard = resource.getrlimit(resource.RLIMIT_AS)
        if current_soft != resource.RLIM_INFINITY:
            resource.setrlimit(resource.RLIMIT_AS, (min(current_soft, WORKER_ADDRESS_SPACE_BYTES), current_hard))
    except (OSError, ValueError):
        pass
    resource.setrlimit(resource.RLIMIT_FSIZE, (MAX_REPORT_BYTES, MAX_REPORT_BYTES))
    resource.setrlimit(resource.RLIMIT_NPROC, (16, 16))


def invoke_worker(desk: str, input_path: Path, config: Mapping[str, Any], output_path: Path) -> tuple[int, str, str]:
    """Invoke a registered validator in a resource-capped child process."""
    command = [sys.executable, "-m", "neorome.validation_worker", "--desk", desk, "--input", str(input_path), "--output", str(output_path), "--config", _canonical(dict(config)).decode("utf-8")]
    try:
        completed = subprocess.run(command, cwd=str(Path(__file__).resolve().parent.parent), stdin=subprocess.DEVNULL,
                                   capture_output=True, text=True, timeout=WORKER_TIMEOUT_SECONDS,
                                   preexec_fn=_worker_limits, env={"PATH": os.environ.get("PATH", ""), "PYTHONDONTWRITEBYTECODE": "1"})
        return completed.returncode, completed.stdout[-MAX_STDOUT_BYTES:], completed.stderr[-MAX_STDERR_BYTES:]
    except subprocess.TimeoutExpired as exc:
        return 124, str(exc.stdout or "")[-MAX_STDOUT_BYTES:], "worker timed out"
    except (OSError, subprocess.SubprocessError) as exc:
        return 125, "", str(exc)


@dataclass(frozen=True)
class DeskResult:
    desk: str
    status: str
    report_path: str | None = None
    blockers: tuple[str, ...] = ()
    error: str | None = None


@dataclass(frozen=True)
class RunResult:
    status: str
    run_id: str | None
    manifest_sha256: str | None
    desks: tuple[DeskResult, ...]
    error: str | None = None


def _write_immutable(path: Path, body: Mapping[str, Any]) -> None:
    raw = _canonical(body) + b"\n"
    if len(raw) > MAX_REPORT_BYTES:
        raise SchedulerError(f"report exceeds {MAX_REPORT_BYTES} bytes")
    _reject_symlink(path.parent, allow_missing=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    _reject_symlink(path.parent)
    if path.exists() or path.is_symlink():
        if path.is_symlink() or path.read_bytes() != raw:
            raise SchedulerError(f"immutable report collision: {path}")
        return
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def _read_worker_output(path: Path) -> dict[str, Any]:
    _reject_symlink(path)
    raw = path.read_bytes()
    if len(raw) > MAX_REPORT_BYTES:
        raise SchedulerError("validator output exceeds report size cap")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SchedulerError("validator output is not valid JSON") from exc
    if not isinstance(value, Mapping):
        raise SchedulerError("validator output must be an object")
    return dict(value)


class ValidationScheduler:
    """Run a validated manifest sequentially under one nonblocking lock."""

    def __init__(self, state_dir: str | os.PathLike[str], *, invoke: Callable[[str, Path, Mapping[str, Any], Path], tuple[int, str, str]] | None = None):
        self.state_dir = Path(state_dir)
        self.invoke = invoke or invoke_worker

    def run_once(self, manifest_path: str | os.PathLike[str], input_root: str | os.PathLike[str]) -> RunResult:
        try:
            manifest = load_manifest(manifest_path, input_root)
        except (ManifestError, OSError) as exc:
            return RunResult("invalid_manifest", None, None, (), str(exc))
        lock_path = self.state_dir / "validation.lock"
        with global_lock(lock_path) as acquired:
            if not acquired:
                return RunResult("locked", manifest.run_id, manifest.manifest_sha256, ())
            return self._run_locked(manifest)

    def _run_locked(self, manifest: LoadedManifest) -> RunResult:
        report_dir = self.state_dir / "reports" / manifest.run_id
        results: list[DeskResult] = []
        for desk in DESK_ORDER:
            item = manifest.desks.get(desk)
            if item is None:
                results.append(DeskResult(desk, "manifest_missing", blockers=("desk_manifest_missing",)))
                continue
            temp_path = report_dir / f".{desk}.worker.json"
            try:
                if temp_path.exists() or temp_path.is_symlink():
                    temp_path.unlink()
                code, stdout, stderr = self.invoke(desk, item.input_path, item.config, temp_path)
                if code != 0:
                    result = DeskResult(desk, "validator_failed", error=(stderr or stdout or f"exit {code}")[-MAX_STDERR_BYTES:])
                    results.append(result)
                    continue
                worker = _read_worker_output(temp_path)
                status = str(worker.get("status", "completed"))
                blockers = tuple(str(value) for value in worker.get("blockers", []) if isinstance(value, str))
                envelope = {"schema_version": REPORT_SCHEMA, "run_id": manifest.run_id, "desk": desk,
                            "manifest_sha256": manifest.manifest_sha256, "input_sha256": item.input_sha256,
                            "boundary_at": item.boundary_at, "validator": item.validator, "status": status,
                            "research_only": True, "execution_enabled": False, "report": worker.get("report"),
                            "blockers": list(blockers)}
                destination = report_dir / f"{desk}.json"
                _write_immutable(destination, envelope)
                results.append(DeskResult(desk, status, str(destination), blockers))
            except (OSError, SchedulerError, ValueError) as exc:
                results.append(DeskResult(desk, "validator_failed", error=str(exc)))
            finally:
                try:
                    if temp_path.exists() or temp_path.is_symlink():
                        temp_path.unlink()
                except OSError:
                    pass
        summary = {"schema_version": REPORT_SCHEMA, "run_id": manifest.run_id, "manifest_sha256": manifest.manifest_sha256,
                   "status": "completed_with_failures" if any(item.status == "validator_failed" for item in results) else "completed",
                   "desks": [{"desk": item.desk, "status": item.status, "report_path": item.report_path,
                              "blockers": list(item.blockers), "error": item.error} for item in results]}
        try:
            _write_immutable(report_dir / "summary.json", summary)
        except (OSError, SchedulerError) as exc:
            return RunResult("report_failed", manifest.run_id, manifest.manifest_sha256, tuple(results), str(exc))
        return RunResult(summary["status"], manifest.run_id, manifest.manifest_sha256, tuple(results))


def _main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--input-root", required=True)
    args = parser.parse_args(argv)
    result = ValidationScheduler(args.state).run_once(args.manifest, args.input_root)
    print(json.dumps({"status": result.status, "run_id": result.run_id, "manifest_sha256": result.manifest_sha256,
                      "desks": [{"desk": item.desk, "status": item.status, "blockers": list(item.blockers), "error": item.error} for item in result.desks],
                      "error": result.error}, sort_keys=True))
    return 0 if result.status in {"completed", "completed_with_failures", "locked", "invalid_manifest"} else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_main())


__all__ = ["DESK_ORDER", "LoadedManifest", "ManifestError", "REPORT_SCHEMA", "RunResult", "VALIDATORS", "ValidationScheduler", "DeskResult", "global_lock", "load_manifest", "main"]


main = _main
