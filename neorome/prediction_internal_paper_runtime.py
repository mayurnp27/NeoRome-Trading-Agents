"""Finite status driver for prediction internal-paper admission.

The driver is intentionally evidence-only.  It reads one archived weather
evidence file, records an explicit decision, and never opens a ledger or a
venue connection.  A future reviewed caller can hand an eligible shadow
candidate to :mod:`prediction_internal_paper`; this utility will not promote
venue labels into probabilities or fills.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import secrets
import stat
from typing import Any, Mapping


MAX_BYTES = 20 * 1024 * 1024
MAX_FILES = 32
MAX_SCAN_BYTES = 256 * 1024 * 1024
DEFAULT_STATE = "/var/lib/neorome-prediction-internal-paper-status"


class RuntimeEvidenceError(ValueError):
    pass


def _load(path: str | Path) -> tuple[dict[str, Any], str]:
    source = Path(path)
    try:
        info = os.stat(source, follow_symlinks=False)
    except OSError as exc:
        raise RuntimeEvidenceError("weather evidence is not a regular file") from exc
    if source.is_symlink() or not stat.S_ISREG(info.st_mode):
        raise RuntimeEvidenceError("weather evidence is not a regular file")
    try:
        descriptor = os.open(source, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        with os.fdopen(descriptor, "rb") as stream: raw = stream.read(MAX_BYTES + 1)
    except OSError as exc:
        raise RuntimeEvidenceError("weather evidence cannot be read safely") from exc
    if len(raw) > MAX_BYTES:
        raise RuntimeEvidenceError("weather evidence exceeds the read limit")
    try:
        decoded = gzip.GzipFile(fileobj=io.BytesIO(raw)).read(MAX_BYTES + 1) if source.suffix == ".gz" else raw
    except OSError as exc:
        raise RuntimeEvidenceError("weather evidence cannot be decompressed") from exc
    if len(decoded) > MAX_BYTES:
        raise RuntimeEvidenceError("weather evidence exceeds the read limit")
    try:
        value = json.loads(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeEvidenceError("weather evidence is not JSON") from exc
    if not isinstance(value, dict):
        raise RuntimeEvidenceError("weather evidence is not an object")
    return value, hashlib.sha256(raw).hexdigest()


def _time(value: Any) -> Any:
    from datetime import datetime
    try: parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc: raise RuntimeEvidenceError("weather evidence collection time is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None: raise RuntimeEvidenceError("weather evidence collection time lacks timezone")
    return parsed


def newest_complete_evidence(directory: str | Path) -> Path:
    """Select one immutable, complete archive without following links."""
    root = Path(directory)
    if root.is_symlink() or not root.is_dir(): raise RuntimeEvidenceError("weather evidence directory is unsafe")
    candidates = []
    total = 0
    # The archive is append-only. Bound work to its newest filename-ordered
    # window rather than failing permanently once it grows beyond MAX_FILES.
    # Evidence files are timestamp-named, so this keeps the recent searchable
    # horizon while retaining the existing per-file and total-byte limits.
    entries = sorted(root.iterdir(), key=lambda path: path.name)[-MAX_FILES:]
    for path in entries:
        if path.is_symlink() or path.suffix not in {".json", ".gz"}: continue
        try: info = os.stat(path, follow_symlinks=False)
        except OSError as exc: raise RuntimeEvidenceError("weather evidence inspection failed") from exc
        if not stat.S_ISREG(info.st_mode): continue
        total += info.st_size
        if total > MAX_SCAN_BYTES: raise RuntimeEvidenceError("weather evidence scan exceeds size limit")
        try: value, _ = _load(path)
        except RuntimeEvidenceError: continue
        if value.get("complete_response") is not True: continue
        if (value.get("source_kind"), value.get("venue"), value.get("series_ticker")) != ("observed", "kalshi_public", "KXHIGHNY"): continue
        candidates.append((_time(value.get("collected_at")), path))
    if not candidates: raise RuntimeEvidenceError("no complete immutable weather evidence found")
    return max(candidates, key=lambda item: (item[0], item[1].name))[1]


def _state_root(value: str | Path) -> Path:
    root = Path(value)
    if root.exists() and root.is_symlink(): raise RuntimeEvidenceError("status state directory is unsafe")
    root.mkdir(parents=True, exist_ok=True, mode=0o700); os.chmod(root, 0o700)
    return root


def _write_status(root: Path, value: Mapping[str, Any]) -> None:
    target = root / "status.json"; temporary = root / f".status.{secrets.token_hex(8)}.tmp"
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "w") as stream:
            descriptor = -1; stream.write(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"); stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary, target)
    finally:
        if descriptor >= 0: os.close(descriptor)
        try: temporary.unlink()
        except FileNotFoundError: pass


def run_status(evidence_dir: str | Path, state: str | Path, *, expected_sha256: str | None = None) -> dict[str, Any]:
    root = _state_root(state)
    try:
        selected = newest_complete_evidence(evidence_dir)
        result = evaluate_weather(selected, expected_sha256=expected_sha256)
        result.update(selected_evidence=str(selected), operational_healthy=True)
    except RuntimeEvidenceError as exc:
        result = {"mode": "prediction_internal_paper_admission", "status": "failed_closed", "error": str(exc),
                  "ledger_mutations": 0, "broker_orders_sent": 0, "venue_requests": 0, "operational_healthy": False}
    _write_status(root, result)
    return result


def evaluate_weather(path: str | Path, *, expected_sha256: str | None = None) -> dict[str, Any]:
    """Return a zero-mutation admission decision for one archived KXHIGHNY file."""
    evidence, digest = _load(path)
    if expected_sha256 is not None and digest != expected_sha256.lower():
        raise RuntimeEvidenceError("weather evidence SHA-256 differs from the expected artifact")
    required = {"source_kind": "observed", "venue": "kalshi_public", "series_ticker": "KXHIGHNY"}
    if any(evidence.get(key) != value for key, value in required.items()):
        raise RuntimeEvidenceError("weather evidence identity is unsupported")
    reasons: list[str] = []
    if evidence.get("complete_response") is not True:
        reasons.append("weather_market_pagination_incomplete")
    if evidence.get("calibration_eligible") is not True:
        reasons.append("weather_calibration_eligible_false")
    if evidence.get("probabilities") is not None:
        reasons.append("weather_probability_payload_not_accepted_by_runtime")
    if evidence.get("orders_sent") != 0:
        reasons.append("weather_evidence_order_count_invalid")
    responses = evidence.get("responses")
    if not isinstance(responses, list) or len(responses) != 2:
        reasons.append("weather_evidence_response_shape_invalid")
    status = "abstained" if reasons else "requires_reviewed_shadow_candidate"
    return {"mode": "prediction_internal_paper_admission", "status": status,
            "evidence_path": str(Path(path)), "evidence_sha256": digest,
            "calibration_eligible": evidence.get("calibration_eligible"),
            "abstention_reasons": sorted(reasons), "ledger_mutations": 0,
            "broker_orders_sent": 0, "venue_requests": 0,
            "profitability_validated": False,
            "next_boundary": "A reviewed, past-only calibrated shadow candidate with explicit bounds, terms, fees, and executable book evidence is required."}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--weather-evidence")
    group.add_argument("--weather-evidence-dir")
    parser.add_argument("--expected-sha256")
    parser.add_argument("--state", default=DEFAULT_STATE)
    args = parser.parse_args(argv)
    try:
        if args.weather_evidence:
            result = evaluate_weather(args.weather_evidence, expected_sha256=args.expected_sha256)
        else:
            result = run_status(args.weather_evidence_dir, args.state, expected_sha256=args.expected_sha256)
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0 if result.get("operational_healthy", True) else 1
    except RuntimeEvidenceError as exc:
        print(json.dumps({"mode": "prediction_internal_paper_admission", "status": "failed_closed", "error": str(exc),
                          "ledger_mutations": 0, "broker_orders_sent": 0, "venue_requests": 0}, sort_keys=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
