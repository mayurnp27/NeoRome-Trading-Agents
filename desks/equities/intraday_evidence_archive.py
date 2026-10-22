"""Immutable local evidence intake for the prospective EQ-INTRADAY shadow lane.

This is intentionally an *archive intake*, not a feed client.  Callers bring
already received JSON-compatible provider responses and receipt timestamps.
The archive hashes exactly those bytes, refuses records that would create
look-ahead, and never invents a receipt, entitlement, or quote-size unit.
It has no credentials, network, broker, scheduler, or ledger dependency.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence


UTC = timezone.utc
_KINDS = {"universe", "bar_prefix", "calendar", "quotes"}


class EvidenceValidationError(ValueError):
    """Raised when a source artifact cannot support a prospective decision."""


def _timestamp(value: Any, name: str) -> datetime:
    if not isinstance(value, str):
        raise EvidenceValidationError(f"{name} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EvidenceValidationError(f"{name} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise EvidenceValidationError(f"{name} must include a timezone")
    return parsed.astimezone(UTC)


def canonical_bytes(value: Any) -> bytes:
    """Return deterministic JSON bytes; non-JSON values are deliberately refused."""

    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
                          allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise EvidenceValidationError("artifact must be JSON-compatible") from exc


def sha256(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def validate_artifact(artifact: Mapping[str, Any], *, signal_at: Any | None = None) -> dict[str, Any]:
    """Validate one raw provider artifact and return a canonical copy plus digest.

    ``known_at`` is the local time the complete artifact became available.
    It may be before a signal but never after it. Quote records also retain an
    individual local receipt timestamp and an explicit provider size-unit
    declaration/evidence reference; those fields are not inferred from values.
    """

    if not isinstance(artifact, Mapping):
        raise EvidenceValidationError("artifact must be an object")
    kind = artifact.get("kind")
    if kind not in _KINDS:
        raise EvidenceValidationError("artifact kind must be universe, bar_prefix, calendar, or quotes")
    for field in ("source", "source_version", "known_at", "payload"):
        if field not in artifact:
            raise EvidenceValidationError(f"artifact {field} is required")
    if not isinstance(artifact["source"], str) or not artifact["source"].strip():
        raise EvidenceValidationError("artifact source is required")
    if not isinstance(artifact["source_version"], str) or not artifact["source_version"].strip():
        raise EvidenceValidationError("artifact source_version is required")
    known_at = _timestamp(artifact["known_at"], "artifact known_at")
    if signal_at is not None and known_at > _timestamp(signal_at, "signal_at"):
        raise EvidenceValidationError("artifact known_at is after signal_at")
    if kind == "quotes":
        declaration = artifact.get("size_units")
        evidence = artifact.get("size_units_evidence")
        if declaration not in {"shares", "round_lots"} or not isinstance(evidence, str) or not evidence.strip():
            raise EvidenceValidationError("quote size units require an explicit supported declaration and evidence reference")
        rows = artifact["payload"]
        if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)) or not rows:
            raise EvidenceValidationError("quote payload must be a non-empty sequence")
        latest_receipt: datetime | None = None
        for row in rows:
            if not isinstance(row, Mapping) or "received_at" not in row or "timestamp" not in row:
                raise EvidenceValidationError("each quote requires timestamp and locally recorded received_at")
            event = _timestamp(row["timestamp"], "quote timestamp")
            receipt = _timestamp(row["received_at"], "quote received_at")
            if receipt < event or (receipt - event).total_seconds() > 1:
                raise EvidenceValidationError("quote receipt lag must be between zero and one second")
            latest_receipt = receipt if latest_receipt is None else max(latest_receipt, receipt)
        if latest_receipt is not None and known_at < latest_receipt:
            raise EvidenceValidationError("quote artifact known_at precedes a contained receipt")
    if kind == "universe":
        if "selection_at" not in artifact:
            raise EvidenceValidationError("universe requires selection_at")
        selection_at = _timestamp(artifact["selection_at"], "universe selection_at")
        if known_at < selection_at:
            raise EvidenceValidationError("universe known_at precedes selection_at")
        if signal_at is not None and selection_at > _timestamp(signal_at, "signal_at"):
            raise EvidenceValidationError("universe selection_at is after signal_at")
        rows = artifact["payload"]
        if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
            raise EvidenceValidationError("universe payload must be a sequence")
        if any(not isinstance(row, Mapping) or not row.get("permanent_id") or not row.get("symbol") for row in rows):
            raise EvidenceValidationError("each universe member requires permanent_id and symbol")
    if kind == "bar_prefix":
        if artifact.get("timeframe") != "1Min" or "adjustment" not in artifact:
            raise EvidenceValidationError("bar prefix requires timeframe=1Min and explicit adjustment setting")
        rows = artifact["payload"]
        if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
            raise EvidenceValidationError("bar prefix payload must be a sequence")
        if signal_at is not None:
            signal = _timestamp(signal_at, "signal_at")
            for row in rows:
                if not isinstance(row, Mapping) or "timestamp" not in row:
                    raise EvidenceValidationError("each bar prefix row requires timestamp")
                if _timestamp(row["timestamp"], "bar timestamp") > signal:
                    raise EvidenceValidationError("bar prefix contains a row after signal_at")
    if kind == "calendar":
        payload = artifact["payload"]
        if not isinstance(payload, Mapping) or "session_close" not in payload:
            raise EvidenceValidationError("calendar payload requires session_close")
        _timestamp(payload["session_close"], "calendar session_close")
    normalized = json.loads(canonical_bytes(dict(artifact)))
    return {"digest": sha256(normalized), "known_at": known_at.isoformat(), "artifact": normalized}


def archive_artifact(root: str | Path, artifact: Mapping[str, Any], *, signal_at: Any | None = None) -> dict[str, Any]:
    """Persist a content-addressed artifact once, without overwriting evidence.

    Each object is created with exclusive mode. Existing identical content is
    reused; a path collision with unequal bytes is an integrity error.
    """

    checked = validate_artifact(artifact, signal_at=signal_at)
    payload = canonical_bytes(checked["artifact"])
    digest = checked["digest"]
    directory = Path(root) / "raw" / checked["artifact"]["kind"]
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{digest}.json"
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        if path.read_bytes() != payload:
            raise EvidenceValidationError("content-addressed artifact path has inconsistent bytes")
    else:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno()
            )
    return {"digest": digest, "known_at": checked["known_at"], "path": str(path), "kind": checked["artifact"]["kind"]}


def shadow_evidence(universe: Mapping[str, Any], bar_prefix: Mapping[str, Any], calendar: Mapping[str, Any], quotes: Mapping[str, Any], *, signal_at: Any) -> dict[str, Any]:
    """Return candidate-facing evidence only when all prospective controls exist."""

    pieces = {
        "universe": validate_artifact(universe, signal_at=signal_at),
        "bar_prefix": validate_artifact(bar_prefix, signal_at=signal_at),
        "calendar": validate_artifact(calendar, signal_at=signal_at),
        # Entry quotes are necessarily observed after the signal.  Their own
        # receipt timestamps and the later candidate order deadline govern
        # admissibility; applying the signal cutoff here would make every
        # prospective entry impossible.
        "quotes": validate_artifact(quotes),
    }
    return {
        "universe_evidence_sha256": pieces["universe"]["digest"],
        "bar_prefix_sha256": pieces["bar_prefix"]["digest"],
        "quote_evidence_sha256": pieces["quotes"]["digest"],
        "universe_known_at": pieces["universe"]["known_at"],
        "bar_prefix_known_at": pieces["bar_prefix"]["known_at"],
        "calendar_known_at": pieces["calendar"]["known_at"],
        "quote_known_at": pieces["quotes"]["known_at"],
        "size_units_verified": True,
        "evidence_scope": "prospective_receipt_timed_archive",
    }


__all__ = ["EvidenceValidationError", "archive_artifact", "canonical_bytes", "sha256", "shadow_evidence", "validate_artifact"]
