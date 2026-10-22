"""Publish retained observed equity bars into the validation inbox.

This module is intentionally a local file adapter.  It reads the most recent
successful ``neorome.equity_daily`` record, validates the observed SPY daily
bars again, and atomically publishes a CSV plus the manifest-bridge metadata
required by the shared validation scheduler.  It never fetches data, fills
missing sessions, or creates synthetic rows.
"""

from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any, Mapping, Sequence


UTC = timezone.utc
DEFAULT_DAILY_STATE = "/var/lib/neorome-equity-daily"
DEFAULT_VALIDATION_STATE = "/var/lib/neorome-validation"
MINIMUM_SESSIONS = 300
MAX_SOURCE_BYTES = 256 * 1024 * 1024
MAX_INPUT_BYTES = 8 * 1024 * 1024
STRATEGY = "overnight_monthly_v1"
VALIDATOR = "desks.equities.walkforward"
SCHEMA_VERSION = "NEOROME-EQUITIES-VALIDATION-PUBLISHER-v1"
_RUN_NAME = re.compile(r"^\d{4}-\d{2}-\d{2}\.json$")
_CSV_FIELDS = (
    "symbol",
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "session_close",
    "vwap",
    "eligible",
    "dollar_volume",
)


class PublisherError(ValueError):
    """Retained source data is unavailable or outside the publisher contract."""

    def __init__(self, blocker: str, detail: str):
        super().__init__(detail)
        self.blocker = blocker


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _ensure_directory(path: Path, *, label: str) -> None:
    if path.exists() and path.is_symlink():
        raise PublisherError("symlink", f"{label} is a symlink")
    try:
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
    except OSError as exc:
        raise PublisherError("path_unwritable", f"cannot create {label}") from exc
    if path.is_symlink() or not path.is_dir():
        raise PublisherError("not_directory", f"{label} is not a directory")


def _read_regular(path: Path, *, max_bytes: int, label: str) -> bytes:
    try:
        info = os.lstat(path)
    except FileNotFoundError as exc:
        raise PublisherError("source_missing", f"{label} is missing") from exc
    except OSError as exc:
        raise PublisherError("source_unreadable", f"cannot inspect {label}") from exc
    if path.is_symlink():
        raise PublisherError("symlink", f"{label} is a symlink")
    if not os.path.isfile(path):
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


def _read_json(path: Path, *, label: str, max_bytes: int = MAX_SOURCE_BYTES) -> Mapping[str, Any]:
    raw = _read_regular(path, max_bytes=max_bytes, label=label)
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PublisherError("source_invalid_json", f"{label} is not valid JSON") from exc
    if not isinstance(value, Mapping):
        raise PublisherError("source_invalid", f"{label} must be a JSON object")
    return value


def _timestamp(value: Any, *, field: str) -> datetime:
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


def _decimal(value: Any, *, field: str, positive: bool = False, nonnegative: bool = False) -> str:
    if isinstance(value, bool) or value is None:
        raise PublisherError("bar_invalid", f"{field} must be numeric")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise PublisherError("bar_invalid", f"{field} must be numeric") from exc
    if not parsed.is_finite() or (positive and parsed <= 0) or (nonnegative and parsed < 0):
        raise PublisherError("bar_invalid", f"{field} is outside its allowed range")
    return format(parsed, "f")


def _bars_from_record(record: Mapping[str, Any]) -> tuple[list[dict[str, str]], datetime]:
    if record.get("status") not in {"completed", "abstained"}:
        raise PublisherError("source_not_ready", "latest equity capture did not complete")
    if record.get("source_kind") != "observed" or record.get("execution_enabled") is not False:
        raise PublisherError("source_not_observed", "equity capture is not observed-only")
    if record.get("broker_orders_sent") != 0:
        raise PublisherError("source_has_orders", "equity capture contains broker orders")
    bundle = record.get("bundle")
    if not isinstance(bundle, Mapping) or bundle.get("source_kind") != "observed":
        raise PublisherError("source_missing_bundle", "equity capture has no observed bundle")
    observation = bundle.get("observation")
    if not isinstance(observation, Mapping):
        raise PublisherError("source_missing_observation", "equity capture has no observed bars")
    if observation.get("source_kind") != "observed" or observation.get("feed") != "iex":
        raise PublisherError("source_not_observed", "equity bars are not observed IEX data")
    if observation.get("broker_orders") != 0 or observation.get("execution_enabled") is not False:
        raise PublisherError("source_has_orders", "equity observation is execution enabled")
    symbol = observation.get("symbol")
    if symbol != "SPY":
        raise PublisherError("symbol_unsupported", "publisher accepts only the fixed observed SPY series")
    raw_bars = observation.get("bars")
    if not isinstance(raw_bars, list):
        raise PublisherError("bars_missing", "equity observation has no daily bars")
    if len(raw_bars) < MINIMUM_SESSIONS:
        raise PublisherError("insufficient_history", f"need at least {MINIMUM_SESSIONS} observed sessions")

    rows: list[dict[str, str]] = []
    seen: set[datetime] = set()
    previous: datetime | None = None
    for number, raw in enumerate(raw_bars, start=1):
        if not isinstance(raw, Mapping):
            raise PublisherError("bar_invalid", f"bar {number} is not an object")
        timestamp = _timestamp(raw.get("timestamp"), field=f"bar {number} timestamp")
        if timestamp in seen or (previous is not None and timestamp <= previous):
            raise PublisherError("bars_unordered", "observed bars must be unique and strictly ordered")
        seen.add(timestamp)
        previous = timestamp
        prices = {
            name: _decimal(raw.get(name), field=f"bar {number} {name}", positive=True)
            for name in ("o", "h", "l", "c")
        }
        low, high = Decimal(prices["l"]), Decimal(prices["h"])
        open_value, close_value = Decimal(prices["o"]), Decimal(prices["c"])
        if low > min(open_value, close_value) or high < max(open_value, close_value) or low > high:
            raise PublisherError("bar_invalid", f"bar {number} has inconsistent OHLC bounds")
        volume = _decimal(raw.get("volume"), field=f"bar {number} volume", positive=True)
        rows.append({
            "symbol": "SPY",
            "timestamp": _iso(timestamp),
            "open": prices["o"],
            "high": prices["h"],
            "low": prices["l"],
            "close": prices["c"],
            "volume": volume,
            "session_close": "",
            "vwap": "",
            "eligible": "true",
            "dollar_volume": "",
        })

    boundary = _timestamp(record.get("collected_at"), field="collected_at")
    return rows, boundary


def _latest_run(root: Path) -> Mapping[str, Any]:
    runs = root / "runs"
    if runs.exists() and runs.is_symlink():
        raise PublisherError("symlink", "equity runs directory is a symlink")
    if not runs.exists() or not runs.is_dir():
        raise PublisherError("source_missing", "equity runs directory is missing")
    candidates = [path for path in runs.iterdir() if _RUN_NAME.fullmatch(path.name)]
    if not candidates:
        raise PublisherError("source_missing", "no dated equity capture exists")
    selected = max(candidates, key=lambda path: path.name)
    return _read_json(selected, label=f"latest equity capture {selected.name}")


def _split_dates(rows: Sequence[Mapping[str, str]]) -> dict[str, list[str]]:
    dates = sorted({date.fromisoformat(row["timestamp"][:10]) for row in rows})
    if len(dates) < MINIMUM_SESSIONS:
        raise PublisherError("insufficient_history", f"need at least {MINIMUM_SESSIONS} observed sessions")
    # Freeze chronological splits from the retained input itself.  The source
    # has no future rows at publication time, and all boundaries are explicit
    # in current.json for the scheduler and later audit.
    first_validation = max(1, int(len(dates) * 0.60))
    first_holdout = max(first_validation + 1, int(len(dates) * 0.80))
    if first_holdout >= len(dates):
        first_holdout = len(dates) - 1
    if first_validation >= first_holdout:
        raise PublisherError("splits_invalid", "retained dates cannot form three chronological splits")
    return {
        "development": [dates[0].isoformat(), dates[first_validation - 1].isoformat()],
        "validation": [dates[first_validation].isoformat(), dates[first_holdout - 1].isoformat()],
        "holdout": [dates[first_holdout].isoformat(), dates[-1].isoformat()],
    }


def _csv_bytes(rows: Sequence[Mapping[str, str]]) -> bytes:
    from io import StringIO

    output = StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=_CSV_FIELDS, lineterminator="\n", extrasaction="raise")
    writer.writeheader()
    writer.writerows(rows)
    raw = output.getvalue().encode("utf-8")
    if len(raw) > MAX_INPUT_BYTES:
        raise PublisherError("oversize", f"published equity input exceeds {MAX_INPUT_BYTES} bytes")
    return raw


def _atomic_write(path: Path, raw: bytes, *, mode: int = 0o600) -> None:
    _ensure_directory(path.parent, label=f"{path} parent")
    if path.exists() and path.is_symlink():
        raise PublisherError("symlink", f"publication path is a symlink: {path}")
    temporary: str | None = None
    try:
        descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb") as stream:
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
        raise PublisherError("symlink", "equity current.json is a symlink")
    try:
        path.unlink()
    except OSError as exc:
        raise PublisherError("path_unwritable", "cannot clear stale equity current.json") from exc


def publish(*, daily_state: str | os.PathLike[str] = DEFAULT_DAILY_STATE, validation_state: str | os.PathLike[str] = DEFAULT_VALIDATION_STATE) -> dict[str, Any]:
    """Publish the latest retained observation or a clean data-not-ready status."""
    inbox = Path(validation_state) / "inbox" / "equities"
    current = inbox / "current.json"
    input_path = inbox / "input.csv"
    try:
        record = _latest_run(Path(daily_state))
        rows, boundary = _bars_from_record(record)
        splits = _split_dates(rows)
        input_raw = _csv_bytes(rows)
        digest = _sha256(input_raw)
        config = {
            "strategy": STRATEGY,
            **splits,
            "purge_days": 1,
            "cadence": "monthly",
            "cost_bps": [0.0, 5.0, 15.0, 30.0],
        }
        metadata = {
            "input": "input.csv",
            "input_sha256": digest,
            "boundary_at": _iso(boundary),
            "validator": VALIDATOR,
            "schedule": "daily",
            "config": config,
        }
        _atomic_write(input_path, input_raw)
        _atomic_write(current, _canonical(metadata) + b"\n")
        return {
            "schema_version": SCHEMA_VERSION,
            "status": "published",
            "rows": len(rows),
            "sessions": len({row["timestamp"][:10] for row in rows}),
            "input_sha256": digest,
            "boundary_at": metadata["boundary_at"],
            "current": str(current),
            "input": str(input_path),
            "broker_orders_sent": 0,
            "execution_enabled": False,
        }
    except PublisherError as exc:
        # A previous current.json must never make a failed capture look fresh
        # to the bridge.  The CSV is retained for audit, but the publication
        # signal is cleared until a complete observed capture succeeds.
        try:
            _remove_current(current)
        except PublisherError:
            pass
        return {
            "schema_version": SCHEMA_VERSION,
            "status": "data_not_ready",
            "blockers": [exc.blocker],
            "error": str(exc),
            "broker_orders_sent": 0,
            "execution_enabled": False,
        }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--daily-state", default=DEFAULT_DAILY_STATE)
    parser.add_argument("--validation-state", default=DEFAULT_VALIDATION_STATE)
    args = parser.parse_args(argv)
    result = publish(daily_state=args.daily_state, validation_state=args.validation_state)
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    # Missing or blocked retained data is an expected research state.  Keep
    # the timer healthy while the JSON summary and bridge status expose the
    # blocker; a failed oneshot would turn an honest abstention into an ops
    # incident and would not make data appear.
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = ["MINIMUM_SESSIONS", "PublisherError", "SCHEMA_VERSION", "main", "publish"]
