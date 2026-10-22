"""Publish retained crypto observations to the validation manifest inbox.

This publisher is a local, paper-only boundary.  It reads already archived
BTC decision records and explicitly timestamped token cohort records; it does
not call a provider, derive a route, or turn a last price into an outcome.  A
new input is published only when at least one source is complete enough to be
replayed by ``desks.crypto.walkforward``.  Missing or stale observations are
reported as data-not-ready and leave the last hash-bound input untouched.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import secrets
import stat
import tempfile
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Sequence


UTC = timezone.utc
SCHEMA_VERSION = "NEOROME-CRYPTO-VALIDATION-INPUT-v1"
VALIDATOR = "desks.crypto.walkforward"
MAX_SOURCE_BYTES = 2 * 1024 * 1024
MAX_INPUT_BYTES = 8 * 1024 * 1024
# With the registered one-bar embargo, 101 warm-up rows still require five
# later rows to leave non-empty development, validation, and holdout phases.
MIN_BARS = 106
MAX_TOKEN_CANDIDATES = 2_000
MAX_SELL_OBSERVATIONS = 10_000
# A short but otherwise valid BTC history is useful evidence for the
# validation report.  It must remain blocked from a three-way walk-forward
# until the native minimum is met, but dropping it makes the source appear to
# contain no bars at all.
NON_FATAL_BTC_BLOCKERS = frozenset({"btc_bars_below_walkforward_minimum"})


class PublisherError(ValueError):
    """A retained source cannot be safely admitted to the native input."""


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise PublisherError("input contains non-canonical JSON") from exc


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise PublisherError(f"{field} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PublisherError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PublisherError(f"{field} must include a timezone")
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _regular_json(path: Path, *, max_bytes: int, label: str) -> tuple[bytes, Mapping[str, Any]]:
    try:
        info = os.lstat(path)
    except FileNotFoundError as exc:
        raise PublisherError(f"{label}_missing") from exc
    except OSError as exc:
        raise PublisherError(f"{label}_unreadable") from exc
    if stat.S_ISLNK(info.st_mode):
        raise PublisherError(f"{label}_symlink")
    if not stat.S_ISREG(info.st_mode):
        raise PublisherError(f"{label}_not_regular")
    if info.st_size > max_bytes:
        raise PublisherError(f"{label}_oversize")
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PublisherError(f"{label}_invalid_json") from exc
    if len(raw) > max_bytes or not isinstance(value, Mapping):
        raise PublisherError(f"{label}_invalid_json")
    return raw, value


def _ensure_dir(path: Path, *, label: str) -> None:
    if path.exists() or path.is_symlink():
        if path.is_symlink():
            raise PublisherError(f"{label}_symlink")
        if not path.is_dir():
            raise PublisherError(f"{label}_not_directory")
        return
    try:
        path.mkdir(parents=True, mode=0o700, exist_ok=True)
    except OSError as exc:
        raise PublisherError(f"{label}_unwritable") from exc
    if path.is_symlink() or not path.is_dir():
        raise PublisherError(f"{label}_invalid")


def _atomic_write(path: Path, raw: bytes, *, label: str) -> None:
    if len(raw) > MAX_INPUT_BYTES:
        raise PublisherError(f"{label}_oversize")
    _ensure_dir(path.parent, label=f"{label}_parent")
    if path.exists() or path.is_symlink():
        if path.is_symlink():
            raise PublisherError(f"{label}_symlink")
        if not path.is_file():
            raise PublisherError(f"{label}_not_regular")
    temporary: Path | None = None
    descriptor = -1
    try:
        descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
        temporary = Path(name)
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        if path.is_symlink():
            raise PublisherError(f"{label}_symlink")
        os.replace(temporary, path)
        temporary = None
    except PublisherError:
        raise
    except OSError as exc:
        raise PublisherError(f"{label}_unwritable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary is not None:
            try:
                temporary.unlink()
            except OSError:
                pass


def _bar(raw: Mapping[str, Any], *, source: str, index: int) -> tuple[str, dict[str, Any]]:
    timestamp = _timestamp(raw.get("timestamp"), f"{source}.bars[{index}].timestamp")
    if timestamp.time() != datetime.min.time():
        raise PublisherError("btc_bar_not_utc_midnight")
    if raw.get("complete") is not True:
        raise PublisherError("btc_bar_incomplete")
    result: dict[str, Any] = {"timestamp": _iso(timestamp), "complete": True}
    for name in ("open", "high", "low", "close"):
        value = raw.get(name)
        if isinstance(value, bool) or value is None:
            raise PublisherError("btc_bar_value_missing")
        try:
            number = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            raise PublisherError("btc_bar_value_invalid") from None
        if not number.is_finite() or number <= 0:
            raise PublisherError("btc_bar_value_invalid")
        result[name] = str(value)
    if Decimal(result["low"]) > min(Decimal(result["open"]), Decimal(result["close"])):
        raise PublisherError("btc_bar_geometry_invalid")
    if Decimal(result["high"]) < max(Decimal(result["open"]), Decimal(result["close"])):
        raise PublisherError("btc_bar_geometry_invalid")
    return result["timestamp"], result


def _read_btc_bars(state: Path, *, now: datetime) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    if not state.exists():
        return [], [], ["btc_state_missing"]
    if state.is_symlink() or not state.is_dir():
        return [], [], ["btc_state_invalid"]
    bars_by_timestamp: dict[str, dict[str, Any]] = {}
    provenance: list[dict[str, Any]] = []
    blockers: list[str] = []
    # Daily recorder files use ``decision-*``.  ``snapshot-*`` and
    # ``rolling-*`` are accepted for retained cloud snapshots produced by the
    # same observed recorder contract.  Never sweep arbitrary JSON here: the
    # state directory also contains attempt/lock metadata.
    paths = sorted({
        path
        for pattern in ("decision-*.json", "snapshot-*.json", "rolling-*.json")
        for path in state.glob(pattern)
    })
    for path in paths:
        try:
            raw, record = _regular_json(path, max_bytes=MAX_SOURCE_BYTES, label="btc_decision")
            if record.get("source_kind") != "observed" or record.get("execution_enabled") is not False:
                continue
            bars_source = record.get("bars_source")
            if isinstance(bars_source, Mapping) and bars_source.get("source_kind") not in (None, "observed"):
                continue
            source_bars = record.get("bars")
            if not isinstance(source_bars, list):
                continue
            boundary_raw = record.get("completed_at", record.get("evaluation_at"))
            boundary = _timestamp(boundary_raw, "btc_decision.boundary")
            file_meta = {"name": path.name, "sha256": _sha256(raw), "boundary_at": _iso(boundary)}
            for index, item in enumerate(source_bars):
                if not isinstance(item, Mapping):
                    raise PublisherError("btc_bar_invalid")
                key, clean = _bar(item, source=path.name, index=index)
                previous = bars_by_timestamp.get(key)
                if previous is not None and previous != clean:
                    raise PublisherError("btc_bar_conflict")
                bars_by_timestamp[key] = clean
            provenance.append(file_meta)
        except PublisherError as exc:
            blockers.append(str(exc))
    if not bars_by_timestamp:
        blockers.append("btc_bars_missing")
        return [], provenance, sorted(set(blockers))
    ordered = sorted(bars_by_timestamp.values(), key=lambda row: row["timestamp"])
    latest = _timestamp(ordered[-1]["timestamp"], "btc.latest")
    if latest.date() != (now.date() - timedelta(days=1)):
        blockers.append("btc_bars_not_through_previous_utc_day")
    if len(ordered) < MIN_BARS:
        blockers.append("btc_bars_below_walkforward_minimum")
    # Do not publish a broken sequence; the validator must never receive a
    # silently gap-filled series.
    previous: datetime | None = None
    for row in ordered:
        timestamp = _timestamp(row["timestamp"], "btc.timestamp")
        if previous is not None and timestamp != previous + timedelta(days=1):
            blockers.append("btc_bars_have_gap")
            break
        previous = timestamp
    unique_blockers = sorted(set(blockers))
    # A short, contiguous and otherwise valid series is retained as observed
    # evidence.  The downstream walk-forward validator still rejects it, and
    # the blocker is carried in the publisher status/input.  Structural
    # defects remain fail-closed so no gap-filled or conflicting series can be
    # admitted.
    if unique_blockers and any(blocker not in NON_FATAL_BTC_BLOCKERS for blocker in unique_blockers):
        return [], provenance, unique_blockers
    return ordered, provenance, unique_blockers


def _token_candidate(record: Mapping[str, Any]) -> dict[str, Any] | None:
    # Candidate records from the runtime are retained only if their discovery
    # timestamps are explicit.  ``recorded_at`` alone is not a discovery
    # vintage and must not be promoted into one.
    if record.get("source_kind") not in (None, "observed") or record.get("execution_enabled") is True:
        return None
    known = record.get("known_at")
    received = record.get("received_at")
    if not isinstance(known, str) or not isinstance(received, str):
        return None
    known_at = _timestamp(known, "token.known_at")
    received_at = _timestamp(received, "token.received_at")
    if received_at < known_at:
        return None
    candidate = dict(record)
    candidate["known_at"] = _iso(known_at)
    candidate["received_at"] = _iso(received_at)
    return candidate


def _read_tokens(state: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    if not state.exists():
        return [], [], [], ["token_state_missing"]
    if state.is_symlink() or not state.is_dir():
        return [], [], [], ["token_state_invalid"]
    candidates_dir = state / "candidates"
    if not candidates_dir.exists() or candidates_dir.is_symlink() or not candidates_dir.is_dir():
        return [], [], [], ["token_candidates_missing"]
    candidates: list[dict[str, Any]] = []
    provenance: list[dict[str, Any]] = []
    sell_observations: list[dict[str, Any]] = []
    blockers: list[str] = []
    for path in sorted(candidates_dir.glob("*.json"))[-MAX_TOKEN_CANDIDATES:]:
        try:
            raw, record = _regular_json(path, max_bytes=MAX_SOURCE_BYTES, label="token_candidate")
            candidate = _token_candidate(record)
            if candidate is None:
                blockers.append("token_discovery_timestamp_missing")
                continue
            candidates.append(candidate)
            provenance.append({"name": path.name, "sha256": _sha256(raw), "received_at": candidate["received_at"]})
            routes = record.get("sell_observations")
            if isinstance(routes, list):
                sell_observations.extend(item for item in routes if isinstance(item, Mapping))
        except PublisherError as exc:
            blockers.append(str(exc))
    if len(sell_observations) > MAX_SELL_OBSERVATIONS:
        sell_observations = sell_observations[-MAX_SELL_OBSERVATIONS:]
    if not candidates:
        blockers.append("token_cohort_missing")
    elif not sell_observations:
        # Discovery without a later, independently retained sell route is
        # valid cohort evidence but cannot produce a modeled outcome.
        blockers.append("token_sell_outcomes_missing")
    return candidates, provenance, sell_observations, sorted(set(blockers))


def _source_boundary(provenance: Sequence[Mapping[str, Any]]) -> datetime:
    values = [_timestamp(item.get("boundary_at", item.get("received_at")), "source.boundary")
              for item in provenance]
    return max(values)


class CryptoValidationPublisher:
    """Compose observed crypto inputs and publish a hash-bound current pointer."""

    def __init__(self, inbox: str | os.PathLike[str], *, daily_state: str | os.PathLike[str], token_state: str | os.PathLike[str]):
        self.inbox = Path(inbox)
        self.daily_state = Path(daily_state)
        self.token_state = Path(token_state)
        self.status_path = self.inbox / "publisher-status.json"

    def publish(self, *, now: datetime | None = None) -> dict[str, Any]:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        _ensure_dir(self.inbox, label="crypto_inbox")
        bars, btc_sources, btc_blockers = _read_btc_bars(self.daily_state, now=current)
        candidates, token_sources, sell_observations, token_blockers = _read_tokens(self.token_state)
        blockers = sorted(set(btc_blockers + token_blockers))
        sections: dict[str, Any] = {}
        sources: dict[str, Any] = {"btc_decisions": btc_sources, "token_candidates": token_sources}
        boundaries: list[datetime] = []
        if bars:
            sections["bars"] = bars
            if btc_sources:
                boundaries.extend(_timestamp(row["boundary_at"], "btc_source.boundary")
                                  for row in btc_sources)
            else:
                boundaries.append(max(_timestamp(row["timestamp"], "bar.timestamp") for row in bars))
        if candidates:
            discovery = {"schema_version": 1, "source_kind": "observed",
                         "candidates": candidates,
                         "source_provenance": token_sources}
            sections["discovery"] = discovery
            sections["sell_observations"] = sell_observations
            boundaries.extend(_timestamp(row["received_at"], "candidate.received_at") for row in candidates)
        if not sections:
            result = {"schema_version": SCHEMA_VERSION, "status": "data_not_ready",
                      "generated_at": _iso(current), "blockers": blockers,
                      "observed_bars": len(bars),
                      "token_candidates": len(candidates),
                      "sell_observations": len(sell_observations),
                      "execution_enabled": False, "orders_sent": 0, "sources": sources}
            _atomic_write(self.status_path, _canonical(result) + b"\n", label="crypto_publisher_status")
            return result
        payload = {"schema_version": SCHEMA_VERSION, "generated_at": _iso(current),
                   "source_kind": "observed", "research_only": True,
                   "execution_enabled": False, "orders_sent": 0,
                   # Keep publisher-level insufficiency visible beside the
                   # native observations.  The validator may add its own
                   # phase blockers, but it must not lose this source fact.
                   "publisher_blockers": blockers,
                   "source_provenance": sources, **sections}
        raw = _canonical(payload) + b"\n"
        if len(raw) > MAX_INPUT_BYTES:
            result = {"schema_version": SCHEMA_VERSION, "status": "data_not_ready",
                      "generated_at": _iso(current), "blockers": ["crypto_input_oversize"],
                      "execution_enabled": False, "orders_sent": 0, "sources": sources}
            _atomic_write(self.status_path, _canonical(result) + b"\n", label="crypto_publisher_status")
            return result
        boundary = max(boundaries) if boundaries else current
        input_hash = _sha256(raw)
        pointer = {"input": "input.json", "input_sha256": input_hash,
                   "boundary_at": _iso(boundary), "validator": VALIDATOR,
                   "schedule": "daily", "config": {}}
        _atomic_write(self.inbox / "input.json", raw, label="crypto_input")
        _atomic_write(self.inbox / "current.json", _canonical(pointer) + b"\n", label="crypto_current")
        result = {"schema_version": SCHEMA_VERSION, "status": "published",
                  "generated_at": _iso(current), "input_sha256": input_hash,
                  "boundary_at": _iso(boundary), "bars": len(bars),
                  "token_candidates": len(candidates), "sell_observations": len(sell_observations),
                  "blockers": blockers, "execution_enabled": False, "orders_sent": 0,
                  "sources": sources}
        _atomic_write(self.status_path, _canonical(result) + b"\n", label="crypto_publisher_status")
        return result


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inbox", default="/var/lib/neorome-validation/inbox/crypto")
    parser.add_argument("--daily-state", default="/var/lib/neorome-crypto-daily")
    parser.add_argument("--token-state", default="/var/lib/neorome-token-paper")
    args = parser.parse_args(argv)
    try:
        result = CryptoValidationPublisher(args.inbox, daily_state=args.daily_state,
                                           token_state=args.token_state).publish()
    except (OSError, PublisherError) as exc:
        result = {"schema_version": SCHEMA_VERSION, "status": "error",
                  "error_type": type(exc).__name__, "execution_enabled": False,
                  "orders_sent": 0}
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0 if result.get("status") in {"published", "data_not_ready"} else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = ["CryptoValidationPublisher", "PublisherError", "SCHEMA_VERSION", "main"]
