"""Publish retained, executable options evidence to the validation bridge.

This process is a local file boundary between the options evidence recorder and
the shared validation manifest bridge.  It deliberately has no provider
client, socket access, broker adapter, or synthetic-data path.  A retained
validation archive must already contain a complete chronological walk-forward
dataset with historical executable bid/ask/depth evidence.  A current
Robinhood observation, an indicative quote, or a synthetic fixture is rejected
and cannot create a scheduler input.

The source root uses the same dated-artifact pointer convention as the options
desk.  The current pointer must advertise ``options-validation.json`` (or the
name passed with ``--source-name``), with a canonical SHA-256 of the retained
archive.  The output is the native ``desks.options.walk_forward`` dataset at
``<inbox>/input.json`` and the hash-bound bridge pointer at
``<inbox>/current.json``.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import tempfile
from typing import Any, Mapping, Sequence

from desks.options.walk_forward import DataAdmissionError, Quote, ReplayError, _normalise_rows
from .validation_scheduler import MAX_INPUT_BYTES


UTC = timezone.utc
ARTIFACT_KIND = "options-validation-evidence-v1"
INPUT_KIND = "options-validation-input-v1"
STATUS_SCHEMA = "NEOROME-OPTIONS-VALIDATION-PUBLISHER-v1"
VALIDATOR = "desks.options.walk_forward"
DEFAULT_SOURCE_NAME = "options-validation.json"
DEFAULT_SOURCE_ROOT = "/var/lib/neorome-options-validation"
DEFAULT_INBOX_ROOT = "/var/lib/neorome-validation/inbox/options"
DEFAULT_STATUS_NAME = "publisher-status.json"
MIN_ROWS = 260  # 200 train + 30 validation + 30 untouched holdout
MAX_STATUS_BYTES = 128 * 1024
_HEX64 = re.compile(r"^[0-9a-fA-F]{64}$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SOURCE_KEYS = {
    "schema_version", "artifact_kind", "status", "provider", "boundary_at",
    "data_admission", "rows", "cost_model", "source_evidence", "artifact_sha256",
}
_ADMISSION_KEYS = {
    "source_kind", "provider", "entitlement_confirmed", "quotes_complete",
    "provider_timestamps",
}
_FORBIDDEN_KEYS = {"token", "tokens", "authorization", "password", "secret"}


class PublisherError(ValueError):
    """A retained archive cannot cross the options validation boundary."""

    def __init__(self, blocker: str, detail: str):
        super().__init__(detail)
        self.blocker = blocker


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise PublisherError("archive_invalid_json", "archive is not canonical JSON") from exc


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha_json(value: Any) -> str:
    return _sha_bytes(_canonical(value))


def _timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise PublisherError("boundary_invalid", f"{field} must be a timezone-aware ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PublisherError("boundary_invalid", f"{field} must be ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PublisherError("boundary_invalid", f"{field} must include a timezone")
    return parsed.astimezone(UTC)


def _safe_regular(path: Path, *, label: str, missing_blocker: str) -> None:
    try:
        info = os.lstat(path)
    except FileNotFoundError as exc:
        raise PublisherError(missing_blocker, f"{label} is missing") from exc
    except OSError as exc:
        raise PublisherError("path_unreadable", f"cannot inspect {label}") from exc
    if stat.S_ISLNK(info.st_mode):
        raise PublisherError("symlink", f"symlinks are not allowed for {label}")
    if not stat.S_ISREG(info.st_mode):
        raise PublisherError("not_regular", f"{label} must be a regular file")


def _safe_directory(path: Path, *, label: str, missing_blocker: str, create: bool = False) -> None:
    try:
        info = os.lstat(path)
    except FileNotFoundError as exc:
        if not create:
            raise PublisherError(missing_blocker, f"{label} is missing") from exc
        try:
            path.mkdir(parents=True, mode=0o700)
            info = os.lstat(path)
        except OSError as mkdir_exc:
            raise PublisherError("path_unwritable", f"cannot create {label}") from mkdir_exc
    except OSError as exc:
        raise PublisherError("path_unreadable", f"cannot inspect {label}") from exc
    if stat.S_ISLNK(info.st_mode):
        raise PublisherError("symlink", f"symlinks are not allowed for {label}")
    if not stat.S_ISDIR(info.st_mode):
        raise PublisherError("not_directory", f"{label} must be a directory")


def _read_json(path: Path, *, label: str, max_bytes: int, missing_blocker: str) -> dict[str, Any]:
    _safe_regular(path, label=label, missing_blocker=missing_blocker)
    try:
        info = os.lstat(path)
        if info.st_size > max_bytes:
            raise PublisherError("oversize", f"{label} exceeds {max_bytes} bytes")
        raw = path.read_bytes()
    except PublisherError:
        raise
    except OSError as exc:
        raise PublisherError("path_unreadable", f"cannot read {label}") from exc
    if len(raw) > max_bytes:
        raise PublisherError("oversize", f"{label} exceeds {max_bytes} bytes")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PublisherError("archive_invalid_json", f"{label} is not valid JSON") from exc
    if not isinstance(value, dict):
        raise PublisherError("archive_invalid", f"{label} must be a JSON object")
    return value


def _check_no_secret_keys(value: Any, *, path: str = "archive") -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if isinstance(key, str) and key.lower() in _FORBIDDEN_KEYS:
                raise PublisherError("private_material", f"{path} contains a forbidden credential field")
            _check_no_secret_keys(nested, path=f"{path}.{key}")
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            _check_no_secret_keys(nested, path=f"{path}[{index}]")


def _source_pointer(root: Path, source_name: str) -> tuple[Path, str]:
    _safe_directory(root, label="options validation evidence root", missing_blocker="source_pointer_missing")
    pointer = _read_json(root / "current.json", label="options validation current pointer", max_bytes=MAX_STATUS_BYTES, missing_blocker="source_pointer_missing")
    if set(pointer) != {"schema_version", "date", "artifacts"} or pointer.get("schema_version") != 1:
        raise PublisherError("source_pointer_invalid", "options validation current pointer schema is invalid")
    date = pointer.get("date")
    if not isinstance(date, str) or not _DATE.fullmatch(date):
        raise PublisherError("source_pointer_invalid", "options validation current pointer date is invalid")
    artifacts = pointer.get("artifacts")
    if not isinstance(artifacts, Mapping):
        raise PublisherError("source_pointer_invalid", "options validation current pointer has no artifacts")
    entry = artifacts.get(source_name)
    if not isinstance(entry, Mapping) or set(entry) != {"file", "sha256"}:
        raise PublisherError("source_archive_missing", f"current pointer does not advertise {source_name}")
    filename = entry.get("file")
    expected = entry.get("sha256")
    if not isinstance(filename, str) or not filename.endswith(".json") or Path(filename).name != filename or filename.startswith("."):
        raise PublisherError("source_pointer_invalid", "retained source filename is unsafe")
    if not isinstance(expected, str) or not _HEX64.fullmatch(expected):
        raise PublisherError("source_pointer_invalid", "retained source hash is not SHA-256")
    path = root / date / filename
    _safe_regular(path, label="retained options validation archive", missing_blocker="source_archive_missing")
    value = _read_json(path, label="retained options validation archive", max_bytes=MAX_INPUT_BYTES, missing_blocker="source_archive_missing")
    if _sha_json(value) != expected.lower():
        raise PublisherError("source_hash_mismatch", "retained options validation archive hash does not match its current pointer")
    return path, expected.lower()


def _positive_number(value: Any, field: str) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise PublisherError("quote_depth_invalid", f"{field} must be a finite nonnegative number")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise PublisherError("quote_depth_invalid", f"{field} must be a finite nonnegative number") from exc
    if not parsed.is_finite() or parsed < 0:
        raise PublisherError("quote_depth_invalid", f"{field} must be a finite nonnegative number")
    return parsed


def _depth(value: Any, field: str) -> Decimal:
    parsed = _positive_number(value, field)
    if parsed <= 0:
        raise PublisherError("quote_depth_invalid", f"{field} must be positive displayed depth")
    return parsed


def _validate_quote(value: Any, *, row: int, phase: str, leg: str, boundary: datetime) -> None:
    if not isinstance(value, Mapping):
        raise PublisherError("quote_missing", f"rows[{row}].{phase}.{leg} is missing")
    if value.get("source_kind") != "historical_executable":
        raise PublisherError("quote_not_historical_executable", f"rows[{row}].{phase}.{leg} is not historical executable evidence")
    if value.get("executable") is not True:
        raise PublisherError("quote_not_executable", f"rows[{row}].{phase}.{leg} is not explicitly executable")
    bid = _positive_number(value.get("bid"), f"rows[{row}].{phase}.{leg}.bid")
    ask = _positive_number(value.get("ask"), f"rows[{row}].{phase}.{leg}.ask")
    if ask <= 0 or bid > ask:
        raise PublisherError("quote_crossed", f"rows[{row}].{phase}.{leg} has an invalid bid/ask")
    _depth(value.get("bid_size"), f"rows[{row}].{phase}.{leg}.bid_size")
    _depth(value.get("ask_size"), f"rows[{row}].{phase}.{leg}.ask_size")
    timestamp = _timestamp(value.get("timestamp"), f"rows[{row}].{phase}.{leg}.timestamp")
    if timestamp > boundary:
        raise PublisherError("quote_future", f"rows[{row}].{phase}.{leg} is later than the archive boundary")
    try:
        Quote.from_mapping(value, leg=f"rows[{row}].{phase}.{leg}", synthetic=False)
    except (ReplayError, DataAdmissionError) as exc:
        raise PublisherError("quote_identity_invalid", str(exc)) from exc


def _validate_source(source: Mapping[str, Any], *, pointer_hash: str, now: datetime) -> tuple[dict[str, Any], datetime, str]:
    _check_no_secret_keys(source)
    unknown = set(source) - _SOURCE_KEYS
    if unknown:
        raise PublisherError("archive_unknown_keys", f"source archive has unsupported keys: {', '.join(sorted(unknown))}")
    if source.get("schema_version") != 1 or source.get("artifact_kind") != ARTIFACT_KIND or source.get("status") != "captured":
        raise PublisherError("source_archive_invalid", "source archive is not a captured options validation archive")
    stored_hash = source.get("artifact_sha256")
    if not isinstance(stored_hash, str) or not _HEX64.fullmatch(stored_hash):
        raise PublisherError("source_hash_invalid", "source archive is missing artifact_sha256")
    body = {key: value for key, value in source.items() if key != "artifact_sha256"}
    if _sha_json(body) != stored_hash.lower():
        raise PublisherError("source_hash_mismatch", "source archive content hash is invalid")
    boundary = _timestamp(source.get("boundary_at"), "source.boundary_at")
    if boundary > now:
        raise PublisherError("boundary_future", "source archive boundary is in the future")
    provider = source.get("provider")
    if not isinstance(provider, str) or not provider.strip():
        raise PublisherError("admission_invalid", "source provider is required")
    admission = source.get("data_admission")
    if not isinstance(admission, Mapping) or set(admission) != _ADMISSION_KEYS:
        raise PublisherError("admission_invalid", "source data_admission schema is incomplete")
    if admission.get("source_kind") != "historical_executable":
        raise PublisherError("historical_entitlement_missing", "source data is not declared historical executable evidence")
    if admission.get("provider") != provider or admission.get("entitlement_confirmed") is not True or admission.get("quotes_complete") is not True or admission.get("provider_timestamps") is not True:
        raise PublisherError("historical_entitlement_missing", "historical executable bid/ask entitlement or timestamp coverage is unconfirmed")
    source_evidence = source.get("source_evidence")
    if not isinstance(source_evidence, list) or not source_evidence:
        raise PublisherError("source_provenance_missing", "source archive has no retained evidence hashes")
    for index, item in enumerate(source_evidence):
        if not isinstance(item, Mapping) or not isinstance(item.get("sha256"), str) or not _HEX64.fullmatch(item["sha256"]):
            raise PublisherError("source_provenance_invalid", f"source_evidence[{index}] is missing a SHA-256 hash")
    rows = source.get("rows")
    if not isinstance(rows, list):
        raise PublisherError("historical_rows_missing", "source archive has no chronological walk-forward rows")
    if len(rows) < MIN_ROWS:
        raise PublisherError("historical_rows_insufficient", f"source archive has {len(rows)} rows; {MIN_ROWS} are required for the registered split")
    try:
        normalized = _normalise_rows({"rows": rows})
    except ReplayError as exc:
        raise PublisherError("historical_rows_invalid", str(exc)) from exc
    quote_count = 0
    for index, row in enumerate(normalized):
        for phase in ("entry_quotes", "exit_quotes"):
            pair = row.get(phase)
            if pair is None:
                continue
            if not isinstance(pair, Mapping):
                raise PublisherError("quote_pair_invalid", f"rows[{index}].{phase} must be an object")
            for leg in ("long", "short"):
                _validate_quote(pair.get(leg), row=index, phase=phase, leg=leg, boundary=boundary)
                quote_count += 1
    if quote_count == 0:
        raise PublisherError("executable_quotes_missing", "source archive contains no executable bid/ask/depth quote pairs")
    cost_model = source.get("cost_model", {})
    if not isinstance(cost_model, Mapping):
        raise PublisherError("cost_model_invalid", "source cost_model must be an object")
    allowed_costs = {"fee_per_contract", "slippage_bps", "share_fee_per_share", "benchmark_capital"}
    if set(cost_model) - allowed_costs:
        raise PublisherError("cost_model_invalid", "source cost_model has unsupported keys")
    for key in allowed_costs:
        if key in cost_model:
            _positive_number(cost_model[key], f"cost_model.{key}")
    dataset = {
        "schema_version": 1,
        "artifact_kind": INPUT_KIND,
        "strategy_version": "OPT-001-v1",
        "research_only": True,
        "execution_enabled": False,
        "data_admission": {
            "source_kind": "historical_executable",
            "provider": provider,
            "entitlement_confirmed": True,
            "quotes_complete": True,
            "provider_timestamps": True,
            "source_artifact_sha256": stored_hash.lower(),
            "source_pointer_sha256": pointer_hash,
            "source_evidence": list(source_evidence),
        },
        "rows": list(rows),
        "cost_model": dict(cost_model),
    }
    return dataset, boundary, stored_hash.lower()


def _atomic_write(path: Path, raw: bytes, *, max_bytes: int, label: str) -> None:
    if len(raw) > max_bytes:
        raise PublisherError("oversize", f"{label} exceeds {max_bytes} bytes")
    _safe_directory(path.parent, label=f"{label} parent", missing_blocker="path_unwritable", create=True)
    try:
        info = os.lstat(path) if path.exists() or path.is_symlink() else None
    except OSError as exc:
        raise PublisherError("path_unreadable", f"cannot inspect {label}") from exc
    if info is not None:
        if stat.S_ISLNK(info.st_mode):
            raise PublisherError("symlink", f"symlinks are not allowed for {label}")
        if not stat.S_ISREG(info.st_mode):
            raise PublisherError("not_regular", f"{label} must be a regular file")
    temporary: str | None = None
    try:
        descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        if path.is_symlink():
            raise PublisherError("symlink", f"symlinks are not allowed for {label}")
        os.replace(temporary, path)
        temporary = None
    except PublisherError:
        raise
    except OSError as exc:
        raise PublisherError("path_unwritable", f"cannot publish {label}") from exc
    finally:
        if temporary is not None:
            try:
                os.unlink(temporary)
            except OSError:
                pass


def _write_status(path: Path, result: Mapping[str, Any]) -> None:
    _atomic_write(path, _canonical(result) + b"\n", max_bytes=MAX_STATUS_BYTES, label="publisher status")


def publish(*, evidence_root: str | os.PathLike[str], inbox_root: str | os.PathLike[str], status_path: str | os.PathLike[str] | None = None, source_name: str = DEFAULT_SOURCE_NAME, now: datetime | None = None) -> dict[str, Any]:
    """Attempt one local, immutable options validation publication."""
    supplied_now = now or datetime.now(UTC)
    if supplied_now.tzinfo is None or supplied_now.utcoffset() is None:
        supplied_now = datetime.now(UTC)
        clock_error = True
    else:
        clock_error = False
    evaluation_now = supplied_now.astimezone(UTC)
    root = Path(evidence_root)
    inbox = Path(inbox_root)
    status_destination = Path(status_path) if status_path is not None else root / DEFAULT_STATUS_NAME
    result: dict[str, Any]
    try:
        if clock_error:
            raise PublisherError("clock_invalid", "publisher clock must be timezone-aware")
        if not source_name.endswith(".json") or Path(source_name).name != source_name:
            raise PublisherError("source_pointer_invalid", "source name is unsafe")
        source_path, source_hash = _source_pointer(root, source_name)
        source = _read_json(source_path, label="retained options validation archive", max_bytes=MAX_INPUT_BYTES, missing_blocker="source_archive_missing")
        dataset, boundary, artifact_hash = _validate_source(source, pointer_hash=source_hash, now=evaluation_now)
        input_raw = _canonical(dataset) + b"\n"
        if len(input_raw) > MAX_INPUT_BYTES:
            raise PublisherError("oversize", "options validation input exceeds the shared input limit")
        input_hash = _sha_bytes(input_raw)
        current = {
            "input": "input.json",
            "input_sha256": input_hash,
            "boundary_at": boundary.isoformat().replace("+00:00", "Z"),
            "validator": VALIDATOR,
            "schedule": "weekly",
            "config": {
                "allow_synthetic": False,
                "train_sessions": 200,
                "validation_sessions": 30,
                "holdout_sessions": 30,
                "step_sessions": 30,
                "cost_model": dict(dataset.get("cost_model", {})),
            },
        }
        _safe_directory(inbox, label="options validation inbox", missing_blocker="path_unwritable", create=True)
        current_path = inbox / "current.json"
        input_path = inbox / "input.json"
        if current_path.exists() or current_path.is_symlink():
            previous = _read_json(current_path, label="existing options validation current pointer", max_bytes=MAX_STATUS_BYTES, missing_blocker="current_invalid")
            previous_boundary = _timestamp(previous.get("boundary_at"), "existing current.boundary_at")
            if previous_boundary > boundary:
                raise PublisherError("boundary_rollback", "new options validation archive is older than the published input")
            if input_path.exists() or input_path.is_symlink():
                _safe_regular(input_path, label="existing options validation input", missing_blocker="input_current_mismatch")
                if _sha_bytes(input_path.read_bytes()) != previous.get("input_sha256"):
                    raise PublisherError("input_current_mismatch", "existing options input does not match its current pointer")
            elif previous.get("input") == "input.json":
                raise PublisherError("input_current_mismatch", "existing current pointer references a missing input")
            if previous_boundary == boundary and previous.get("input_sha256") != input_hash:
                raise PublisherError("publication_collision", "same-boundary options validation input differs from the published bytes")
        _atomic_write(input_path, input_raw, max_bytes=MAX_INPUT_BYTES, label="options validation input")
        _atomic_write(current_path, _canonical(current) + b"\n", max_bytes=MAX_STATUS_BYTES, label="options validation current pointer")
        result = {"schema_version": STATUS_SCHEMA, "status": "published", "source_path": str(source_path), "source_artifact_sha256": artifact_hash, "source_pointer_sha256": source_hash, "input_sha256": input_hash, "boundary_at": current["boundary_at"], "rows": len(dataset["rows"]), "orders_sent": 0, "network_calls": 0}
    except PublisherError as exc:
        result = {"schema_version": STATUS_SCHEMA, "status": "data_not_ready", "blockers": [exc.blocker], "detail": str(exc), "orders_sent": 0, "network_calls": 0}
    except (OSError, ValueError, TypeError) as exc:
        result = {"schema_version": STATUS_SCHEMA, "status": "error", "blockers": ["publisher_failure"], "detail": str(exc)[:500], "orders_sent": 0, "network_calls": 0}
    try:
        _write_status(status_destination, result)
    except PublisherError as exc:
        return {**result, "status": "error", "blockers": ["status_write_failed", exc.blocker], "detail": str(exc)[:500]}
    return result


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-root", default=DEFAULT_SOURCE_ROOT)
    parser.add_argument("--inbox-root", default=DEFAULT_INBOX_ROOT)
    parser.add_argument("--status-path")
    parser.add_argument("--source-name", default=DEFAULT_SOURCE_NAME)
    args = parser.parse_args(argv)
    result = publish(evidence_root=args.evidence_root, inbox_root=args.inbox_root, status_path=args.status_path, source_name=args.source_name)
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get("status") in {"published", "data_not_ready"} else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = ["ARTIFACT_KIND", "DEFAULT_SOURCE_NAME", "INPUT_KIND", "PublisherError", "publish", "main"]
