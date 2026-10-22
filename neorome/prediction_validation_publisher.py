"""Publish only causally complete prediction events for validation.

The continuous public observer records market snapshots, but a snapshot is not
by itself a backtest observation.  This adapter accepts an event only when the
archive explicitly supplies an executable venue quote timestamp, an
out-of-sample calibrated probability, exact terms and a finalized settlement.
It never derives an outcome, probability, venue event time or fill from a
market midpoint.  A missing or mixed archive leaves ``current.json`` absent so
the validation bridge cannot promote stale or partial data.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import tempfile
from typing import Any, Iterator, Mapping, Sequence


UTC = timezone.utc
DEFAULT_STATE = "/var/lib/neorome-prediction-24x7"
DEFAULT_VALIDATION_STATE = "/var/lib/neorome-validation"
MINIMUM_EVENTS = 60
MAX_SOURCE_BYTES = 8 * 1024 * 1024
MAX_TOTAL_SOURCE_BYTES = 64 * 1024 * 1024
MAX_INPUT_BYTES = 8 * 1024 * 1024
VERSION = "prediction-validation-publisher-v1"
SCHEMA_VERSION = "NEOROME-PREDICTION-VALIDATION-PUBLISHER-v1"
VALIDATOR = "neorome.prediction_walkforward"
_HEX64 = re.compile(r"^[0-9a-fA-F]{64}$")
_TICKER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")


class PublisherError(ValueError):
    """A source is unavailable or violates the validation input contract."""

    def __init__(self, blocker: str, detail: str):
        super().__init__(detail)
        self.blocker = blocker


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise PublisherError("source_invalid", "value is not canonical JSON") from exc


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise PublisherError("timestamp_invalid", f"{field} must be ISO-8601")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PublisherError("timestamp_invalid", f"{field} must be ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PublisherError("timestamp_invalid", f"{field} must include a timezone")
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _regular_bytes(path: Path, *, max_bytes: int, label: str) -> bytes:
    try:
        info = os.lstat(path)
    except FileNotFoundError as exc:
        raise PublisherError("source_missing", f"{label} is missing") from exc
    except OSError as exc:
        raise PublisherError("source_unreadable", f"cannot inspect {label}") from exc
    if stat.S_ISLNK(info.st_mode):
        raise PublisherError("symlink", f"{label} is a symlink")
    if not stat.S_ISREG(info.st_mode):
        raise PublisherError("not_regular", f"{label} is not a regular file")
    if info.st_size > max_bytes:
        raise PublisherError("oversize", f"{label} exceeds {max_bytes} bytes")
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise PublisherError("source_unreadable", f"cannot read {label}") from exc
    if len(raw) > max_bytes:
        raise PublisherError("oversize", f"{label} exceeds {max_bytes} bytes")
    return raw


def _ensure_dir(path: Path, *, label: str) -> None:
    if path.exists() and path.is_symlink():
        raise PublisherError("symlink", f"{label} is a symlink")
    try:
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
    except OSError as exc:
        raise PublisherError("path_unwritable", f"cannot create {label}") from exc
    if path.is_symlink() or not path.is_dir():
        raise PublisherError("not_directory", f"{label} is not a directory")
    os.chmod(path, 0o700)


def _decode(path: Path) -> tuple[Mapping[str, Any], str]:
    raw = _regular_bytes(path, max_bytes=MAX_SOURCE_BYTES, label=str(path))
    if path.suffix == ".gz":
        try:
            with gzip.GzipFile(fileobj=__import__("io").BytesIO(raw)) as stream:
                decoded = stream.read(MAX_SOURCE_BYTES + 1)
        except (OSError, EOFError) as exc:
            raise PublisherError("source_invalid_gzip", f"{path} is not readable gzip") from exc
        if len(decoded) > MAX_SOURCE_BYTES:
            raise PublisherError("oversize", f"{path} expands beyond {MAX_SOURCE_BYTES} bytes")
    else:
        decoded = raw
    try:
        value = json.loads(decoded.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PublisherError("source_invalid_json", f"{path} is not valid JSON") from exc
    if not isinstance(value, Mapping):
        raise PublisherError("source_invalid", f"{path} must contain a JSON object")
    return value, _sha256(raw)


def _source_files(root: Path) -> list[Path]:
    if root.exists() and root.is_symlink():
        raise PublisherError("symlink", "prediction source root is a symlink")
    if not root.exists() or not root.is_dir():
        raise PublisherError("source_missing", "prediction source root is missing")
    paths: list[Path] = []
    try:
        for path in root.rglob("*"):
            if path.is_symlink():
                raise PublisherError("symlink", f"source tree contains symlink: {path}")
            if path.is_file() and path.suffix in {".json", ".gz"}:
                paths.append(path)
    except OSError as exc:
        raise PublisherError("source_unreadable", "cannot enumerate prediction source root") from exc
    return sorted(paths)


def _event_rows(value: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    """Read explicit event records; never infer them from active market rows."""
    for field in ("events", "records"):
        rows = value.get(field)
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, Mapping)]
    markets = value.get("markets")
    if isinstance(markets, list):
        rows = []
        for market in markets:
            if not isinstance(market, Mapping):
                continue
            explicit = market.get("validation_event")
            if isinstance(explicit, Mapping):
                rows.append(explicit)
        return rows
    return []


def _digest_field(value: Any, field: str) -> str:
    if not isinstance(value, str) or not _HEX64.fullmatch(value):
        raise PublisherError("event_provenance_missing", f"{field} must be a SHA-256 digest")
    return value.lower()


def _decimal(value: Any, field: str) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise PublisherError("probability_invalid", f"{field} must be numeric")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise PublisherError("probability_invalid", f"{field} must be numeric") from exc
    if not parsed.is_finite() or parsed < 0 or parsed > 1:
        raise PublisherError("probability_invalid", f"{field} must be in [0,1]")
    return parsed


def _cents(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 100:
        raise PublisherError("quote_invalid", f"{field} must be integer cents in [0,100]")
    return value


def _binary(value: Any, field: str) -> str:
    if value in ("YES", "yes", 1, True):
        return "YES"
    if value in ("NO", "no", 0, False):
        return "NO"
    raise PublisherError("settlement_invalid", f"{field} must be a finalized binary outcome")


def _normalize_event(raw: Mapping[str, Any], index: int, *, source_hash: str) -> dict[str, Any]:
    required = ("event_id", "family", "market_id", "terms_sha256", "decision_at", "known_at", "quote_known_at",
                "probability_known_at", "probability", "probability_lower_bound", "ask_cents", "yes_bid_cents",
                "yes_ask_cents", "settlement_at", "settlement_finalized", "result", "source_hash",
                "settlement_source_hash", "probability_provenance", "quote_source_kind", "fee_schedule")
    missing = [field for field in required if field not in raw]
    if missing:
        raise PublisherError("event_incomplete", f"event {index} missing {', '.join(missing)}")
    event_id = raw["event_id"]
    family = raw["family"]
    market_id = raw["market_id"]
    if not isinstance(event_id, str) or not event_id.strip() or not _TICKER.fullmatch(event_id):
        raise PublisherError("event_identity_invalid", f"event {index} event_id is invalid")
    if not isinstance(family, str) or not family.strip() or not _TICKER.fullmatch(family):
        raise PublisherError("event_identity_invalid", f"event {index} family is invalid")
    if not isinstance(market_id, str) or not market_id.strip() or not _TICKER.fullmatch(market_id):
        raise PublisherError("event_identity_invalid", f"event {index} market_id is invalid")
    terms = _digest_field(raw["terms_sha256"], f"event {index}.terms_sha256")
    quote_source = raw["quote_source_kind"]
    if quote_source != "venue_executable":
        raise PublisherError("venue_quote_unverified", f"event {index} quote is not an executable venue quote")
    decision = _timestamp(raw["decision_at"], f"event {index}.decision_at")
    known = _timestamp(raw["known_at"], f"event {index}.known_at")
    quote_known = _timestamp(raw["quote_known_at"], f"event {index}.quote_known_at")
    probability_known = _timestamp(raw["probability_known_at"], f"event {index}.probability_known_at")
    settled = _timestamp(raw["settlement_at"], f"event {index}.settlement_at")
    if known > decision or quote_known > decision or probability_known > decision:
        raise PublisherError("causal_timestamp_invalid", f"event {index} contains data known after decision")
    if settled <= decision:
        raise PublisherError("settlement_invalid", f"event {index} settlement is not after decision")
    if raw["settlement_finalized"] is not True:
        raise PublisherError("settlement_not_finalized", f"event {index} settlement is not finalized")
    result = _binary(raw["result"], f"event {index}.result")
    probability = _decimal(raw["probability"], f"event {index}.probability")
    lower = _decimal(raw["probability_lower_bound"], f"event {index}.probability_lower_bound")
    if lower > probability:
        raise PublisherError("probability_invalid", f"event {index} lower bound exceeds probability")
    provenance = raw["probability_provenance"]
    if not isinstance(provenance, Mapping):
        raise PublisherError("calibration_missing", f"event {index} calibrated probability provenance is missing")
    if provenance.get("source_kind") != "calibrated_forecast" or provenance.get("calibration_scope") != "out_of_sample":
        raise PublisherError("calibration_missing", f"event {index} probability is not marked out-of-sample calibrated")
    if not isinstance(provenance.get("model_version"), str) or not provenance["model_version"].strip():
        raise PublisherError("calibration_missing", f"event {index} model version is missing")
    _digest_field(provenance.get("source_hash"), f"event {index}.probability_provenance.source_hash")
    method = str(provenance.get("method", ""))
    if "midpoint" in method.lower() or provenance.get("source_kind") == "venue_midpoint":
        raise PublisherError("midpoint_not_alpha", f"event {index} probability uses venue midpoint")
    bid = _cents(raw["yes_bid_cents"], f"event {index}.yes_bid_cents")
    ask = _cents(raw["yes_ask_cents"], f"event {index}.yes_ask_cents")
    if bid > ask:
        raise PublisherError("quote_invalid", f"event {index} quote is crossed")
    ask_cents = _cents(raw["ask_cents"], f"event {index}.ask_cents")
    fee_schedule = raw["fee_schedule"]
    if not isinstance(fee_schedule, Mapping):
        raise PublisherError("fee_schedule_missing", f"event {index} fee schedule is missing")
    # Bind the original archive hash in the normalized row; retain the
    # event-provided source hash for the quote itself.
    source = _digest_field(raw["source_hash"], f"event {index}.source_hash")
    settlement_source = _digest_field(raw["settlement_source_hash"], f"event {index}.settlement_source_hash")
    return {
        "event_id": event_id,
        "family": family,
        "market_id": market_id,
        "terms_sha256": terms,
        "decision_at": _iso(decision),
        "known_at": _iso(known),
        "quote_known_at": _iso(quote_known),
        "probability_known_at": _iso(probability_known),
        "probability": str(probability),
        "probability_lower_bound": str(lower),
        "probability_provenance": dict(provenance),
        "quote_source_kind": quote_source,
        "yes_bid_cents": bid,
        "yes_ask_cents": ask,
        "ask_cents": ask_cents,
        "fee_schedule": dict(fee_schedule),
        "settlement_at": _iso(settled),
        "settlement_finalized": True,
        "outcome": result,
        "result": result,
        "source_hash": source,
        "settlement_source_hash": settlement_source,
        "archive_sha256": source_hash,
    }


def _fee_schedule(values: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    schedules = []
    for row in values:
        value = row.get("fee_schedule")
        if isinstance(value, Mapping):
            schedules.append(dict(value))
    if not schedules:
        raise PublisherError("fee_schedule_missing", "no versioned prediction fee schedule was supplied")
    first = schedules[0]
    if any(schedule != first for schedule in schedules[1:]):
        raise PublisherError("fee_schedule_mixed", "prediction events use different fee schedules")
    if not isinstance(first.get("version"), str) or not first["version"].strip():
        raise PublisherError("fee_schedule_missing", "fee schedule version is missing")
    for field in ("entry_fee_cents_per_contract", "settlement_fee_cents_per_contract"):
        if isinstance(first.get(field), bool) or not isinstance(first.get(field), (int, float, str)):
            raise PublisherError("fee_schedule_invalid", f"fee schedule {field} is missing")
    return first


def _atomic_write(path: Path, raw: bytes) -> None:
    _ensure_dir(path.parent, label=f"{path} parent")
    if path.exists() and path.is_symlink():
        raise PublisherError("symlink", f"publication path is a symlink: {path}")
    temporary: str | None = None
    try:
        fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        if path.is_symlink():
            raise PublisherError("symlink", f"publication path is a symlink: {path}")
        os.replace(temporary, path)
        temporary = None
    except PublisherError:
        raise
    except OSError as exc:
        raise PublisherError("path_unwritable", f"cannot publish {path.name}") from exc
    finally:
        if temporary is not None:
            try:
                os.unlink(temporary)
            except OSError:
                pass


def _remove_current(path: Path) -> None:
    if not path.exists() and not path.is_symlink():
        return
    if path.is_symlink():
        raise PublisherError("symlink", "prediction current.json is a symlink")
    try:
        path.unlink()
    except OSError as exc:
        raise PublisherError("path_unwritable", "cannot clear stale prediction current.json") from exc


@contextmanager
def _lock(path: Path) -> Iterator[None]:
    _ensure_dir(path.parent, label="validation state")
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        stream = os.fdopen(descriptor, "a+")
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        raise PublisherError("already_running", "prediction validation publisher is already running") from exc
    except OSError as exc:
        raise PublisherError("lock_unavailable", "cannot acquire prediction validation publisher lock") from exc
    try:
        yield
    finally:
        stream.close()


def _load_events(state: Path, archives: Sequence[Path]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    roots = [state, *archives]
    paths: list[Path] = []
    for root in roots:
        paths.extend(_source_files(root))
    # Keep each archive once when an explicitly supplied root overlaps state.
    unique = sorted({str(path): path for path in paths}.values(), key=lambda path: str(path))
    if not unique:
        raise PublisherError("source_missing", "no prediction evidence archives found")
    total = 0
    normalized: list[dict[str, Any]] = []
    boundaries: list[datetime] = []
    for path in unique:
        raw_value, archive_hash = _decode(path)
        total += path.stat().st_size
        if total > MAX_TOTAL_SOURCE_BYTES:
            raise PublisherError("source_budget_exceeded", "prediction archive budget exceeded")
        explicit = _event_rows(raw_value)
        if not explicit:
            # Active 24x7 market snapshots and historical weather files are
            # retained for audit, but are not validation observations.
            continue
        source_boundary = raw_value.get("collected_at", raw_value.get("captured_at", raw_value.get("boundary_at")))
        if source_boundary is not None:
            boundaries.append(_timestamp(source_boundary, f"{path}.boundary_at"))
        for index, event in enumerate(explicit):
            normalized.append(_normalize_event(event, index, source_hash=archive_hash))
    if not normalized:
        raise PublisherError("causal_dataset_missing", "archives contain no explicit complete validation events")
    return normalized, {"files": [str(path) for path in unique], "boundaries": boundaries}


def publish(*, state: str | os.PathLike[str] = DEFAULT_STATE, validation_state: str | os.PathLike[str] = DEFAULT_VALIDATION_STATE, archives: Sequence[str | os.PathLike[str]] = ()) -> dict[str, Any]:
    """Publish a complete, immutable prediction walk-forward input."""
    validation_root = Path(validation_state)
    current = validation_root / "inbox" / "prediction" / "current.json"
    input_path = validation_root / "inbox" / "prediction" / "input.json"
    try:
        with _lock(validation_root / "prediction-validation-publisher.lock"):
            events, metadata = _load_events(Path(state), [Path(item) for item in archives])
            if len(events) < MINIMUM_EVENTS:
                raise PublisherError("insufficient_events", f"need at least {MINIMUM_EVENTS} complete chronological events")
            events.sort(key=lambda row: (row["decision_at"], row["event_id"]))
            if len({row["event_id"] for row in events}) != len(events):
                raise PublisherError("duplicate_event", "event IDs must be unique")
            if any(events[index]["decision_at"] >= events[index + 1]["decision_at"] for index in range(len(events) - 1)):
                raise PublisherError("events_unordered", "events must be strictly chronological")
            families = {row["family"] for row in events}
            if len(families) != 1:
                raise PublisherError("mixed_families", "one exact prediction family is required per validation input")
            terms = {row["terms_sha256"] for row in events}
            if len(terms) != 1:
                raise PublisherError("mixed_terms", "one exact settlement terms identity is required")
            fee_schedule = _fee_schedule(events)
            family = next(iter(families))
            source_identity = {"venue": "Kalshi", "family": family, "source_kind": "observed", "terms_sha256": next(iter(terms))}
            baseline_records = []
            for row in events:
                baseline_records.append({
                    "event_id": row["event_id"], "family": family, "market_id": row["market_id"],
                    "terms_sha256": row["terms_sha256"], "decision_at": row["decision_at"],
                    "known_at": row["quote_known_at"], "settlement_at": row["settlement_at"],
                    "settlement_finalized": True, "settlement_source_hash": row["settlement_source_hash"],
                    "yes_bid_cents": row["yes_bid_cents"], "yes_ask_cents": row["yes_ask_cents"],
                    "result": row["result"], "source_hash": row["source_hash"],
                })
            payload = {
                "schema_version": 1,
                "publisher_version": VERSION,
                "source_kind": "observed",
                "family": family,
                "events": events,
                "baseline_records": baseline_records,
                "source_files": metadata["files"],
                "source_identity": source_identity,
                "execution_enabled": False,
                "orders_sent": 0,
                "ledger_mutations": 0,
                "paper_eligible": False,
                "alpha_validated": False,
                "research_notice": "Chronological validation input only; no midpoint alpha or execution claim.",
            }
            input_raw = _canonical(payload) + b"\n"
            if len(input_raw) > MAX_INPUT_BYTES:
                raise PublisherError("oversize", "published prediction input exceeds size cap")
            digest = _sha256(input_raw)
            boundary = max(metadata["boundaries"], default=max(_timestamp(row["settlement_at"], "settlement_at") for row in events))
            config = {
                "fee_schedule": fee_schedule,
                "edge_floor_cents": 3,
                "development_events": 30,
                "validation_events": 10,
                "holdout_events": 20,
                "baseline": {"family": family, "source_identity": source_identity, "development_events": 30, "holdout_events": 20},
            }
            current_payload = {
                "input": "input.json", "input_sha256": digest, "boundary_at": _iso(boundary),
                "validator": VALIDATOR, "schedule": "daily", "config": config,
            }
            _atomic_write(input_path, input_raw)
            _atomic_write(current, _canonical(current_payload) + b"\n")
            return {
                "schema_version": SCHEMA_VERSION, "status": "published", "events": len(events), "family": family,
                "input_sha256": digest, "boundary_at": current_payload["boundary_at"], "current": str(current),
                "input": str(input_path), "broker_orders_sent": 0, "execution_enabled": False,
            }
    except PublisherError as exc:
        try:
            _remove_current(current)
        except PublisherError:
            pass
        return {"schema_version": SCHEMA_VERSION, "status": "data_not_ready", "blockers": [exc.blocker],
                "error": str(exc), "broker_orders_sent": 0, "execution_enabled": False}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--archive-root", action="append", default=[])
    parser.add_argument("--validation-state", default=DEFAULT_VALIDATION_STATE)
    args = parser.parse_args(argv)
    result = publish(state=args.state, validation_state=args.validation_state, archives=args.archive_root)
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["MINIMUM_EVENTS", "PublisherError", "SCHEMA_VERSION", "publish", "main"]
