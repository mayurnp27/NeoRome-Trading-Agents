"""Build frozen options trend inputs from retained daily-equity archives.

This module is deliberately read-only with respect to market providers.  It
accepts only two independently retained, hash-checked daily archives (SPY and
QQQ), then emits SPY's prior-completed-session 200-day selector inputs.  A
missing, stale, malformed, or incomplete archive produces an abstention; it
never substitutes a live value or a partial history.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from desks.options.selection import payload_sha256
from .options_dated_artifacts import DatedArtifactError, resolve_current, save_stage, sha256

UTC = timezone.utc
NY = ZoneInfo("America/New_York")


class TrendInputError(ValueError):
    pass


def _stamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise TrendInputError(f"{field} must be ISO-8601")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise TrendInputError(f"{field} must be ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise TrendInputError(f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TrendInputError(f"{field} must be an object")
    return value


def _digest(value: Any) -> str:
    try:
        return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    except (TypeError, ValueError) as exc:
        raise TrendInputError("daily archive is not canonical JSON") from exc


def _hash(value: Any, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(char not in "0123456789abcdef" for char in value.lower()):
        raise TrendInputError(f"{field} must be a SHA-256 digest")
    return value.lower()


def _evidence(rows: Any, field: str, latest: datetime) -> None:
    if not isinstance(rows, list) or not rows:
        raise TrendInputError(f"{field} evidence is missing")
    for index, row in enumerate(rows):
        entry = _mapping(row, f"{field} evidence {index}")
        _hash(entry.get("response_sha256"), f"{field} evidence {index} raw hash")
        requested, received = _stamp(entry.get("requested_at"), f"{field} evidence {index} requested"), _stamp(entry.get("received_at"), f"{field} evidence {index} received")
        if requested > received or received > latest:
            raise TrendInputError(f"{field} evidence chronology is invalid")


def _archive(archive: Mapping[str, Any], symbol: str) -> dict[str, Any]:
    archive = _mapping(archive, "daily archive")
    if archive.get("status") not in {"completed", "abstained"} or archive.get("source_kind") != "observed":
        raise TrendInputError("daily archive is not a completed observed capture")
    if archive.get("broker_orders_sent") != 0 or archive.get("execution_enabled") is not False or archive.get("research_only") is not True:
        raise TrendInputError("daily archive violates its read-only attestation")
    bundle = _mapping(archive.get("bundle"), "daily bundle")
    if bundle.get("symbol") != symbol or bundle.get("source_kind") != "observed":
        raise TrendInputError(f"daily archive does not contain {symbol}")
    hashes = _mapping(archive.get("hashes"), "daily hashes")
    context, observation = _mapping(bundle.get("context"), "daily context"), _mapping(bundle.get("observation"), "daily observation")
    for field, value in (("context_sha256", context), ("observation_sha256", observation), ("bundle_sha256", bundle)):
        if _hash(hashes.get(field), field) != _digest(value):
            raise TrendInputError(f"{field} does not match retained raw archive")
    collected = _stamp(archive.get("collected_at"), "daily archive collected_at")
    received_context, received_observation = _stamp(context.get("received_at"), "daily context receipt"), _stamp(observation.get("observed_at"), "daily observation receipt")
    if received_context > collected or received_observation > collected:
        raise TrendInputError("daily archive receipt chronology is invalid")
    _evidence(context.get("evidence"), "daily context", collected)
    _evidence(observation.get("evidence"), "daily observation", collected)
    sessions = context.get("sessions")
    if not isinstance(sessions, list) or len(sessions) < 201:
        raise TrendInputError("daily archive has fewer than 200 prior sessions")
    clock = _mapping(context.get("clock"), "daily context clock")
    now = _stamp(clock.get("timestamp"), "daily context clock timestamp")
    today = now.astimezone(NY).date().isoformat()
    if archive.get("run_date_utc") != now.date().isoformat() or not isinstance(clock.get("is_open"), bool):
        raise TrendInputError("daily archive clock is stale or malformed")
    parsed_sessions: list[dict[str, Any]] = []
    prior: list[str] = []
    for index, row in enumerate(sessions):
        item = _mapping(row, f"session {index}")
        date = item.get("date")
        opened, closed = _stamp(item.get("open_at"), f"session {index} open"), _stamp(item.get("close_at"), f"session {index} close")
        if not isinstance(date, str) or opened.date().isoformat() != date or closed.date().isoformat() != date or not opened < closed:
            raise TrendInputError("daily archive session chronology is invalid")
        parsed_sessions.append({"date": date, "open_at": opened.isoformat().replace("+00:00", "Z"), "close_at": closed.isoformat().replace("+00:00", "Z")})
        if date < today:
            prior.append(date)
    if len(prior) < 200 or today not in {item["date"] for item in parsed_sessions}:
        raise TrendInputError("daily archive lacks a current session and 200 completed sessions")
    bars = observation.get("bars")
    if not isinstance(bars, list) or len(bars) < 200:
        raise TrendInputError("daily archive has fewer than 200 bars")
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, row in enumerate(bars):
        item = _mapping(row, f"daily bar {index}")
        timestamp = _stamp(item.get("timestamp"), f"daily bar {index} timestamp")
        session = timestamp.astimezone(NY).date().isoformat()
        if timestamp > received_observation or session >= today or session in seen:
            raise TrendInputError("daily bars include a current, future, duplicate, or unpublished session")
        seen.add(session)
        for field in ("o", "h", "l", "c"):
            if not isinstance(item.get(field), (str, int, float)) or isinstance(item.get(field), bool):
                raise TrendInputError(f"daily bar {index} {field} is missing")
        normalized.append({"timestamp": timestamp.isoformat().replace("+00:00", "Z"), "session_date": session,
                           "o": str(item["o"]), "h": str(item["h"]), "l": str(item["l"]), "c": str(item["c"]),
                           "complete": True, "adjusted": False, "published_at": received_observation.isoformat().replace("+00:00", "Z")})
    normalized.sort(key=lambda item: item["session_date"])
    expected = prior[-200:]
    if [item["session_date"] for item in normalized[-200:]] != expected:
        raise TrendInputError("daily bars do not exactly cover the last 200 prior sessions")
    return {"archive_sha256": _digest(archive), "context_sha256": _digest(context), "observation_sha256": _digest(observation),
            "now": now, "today": today, "sessions": parsed_sessions, "bars": normalized[-200:],
            "observed_at": received_observation, "is_open": clock["is_open"]}


def build_trend_inputs(spy_archive: Mapping[str, Any], qqq_archive: Mapping[str, Any], *, now: datetime | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return the exact SPY 200-day observation/context after both archives validate."""
    spy, qqq = _archive(spy_archive, "SPY"), _archive(qqq_archive, "QQQ")
    evaluation_now = now or datetime.now(UTC)
    if evaluation_now.tzinfo is None or evaluation_now.utcoffset() is None:
        raise TrendInputError("evaluation time must be timezone-aware")
    evaluation_now = evaluation_now.astimezone(UTC)
    # A daily archive belongs to one UTC day.  Reusing a prior day's archive
    # would silently make the selector's "current session" assertion false.
    if spy["now"] > evaluation_now or qqq["now"] > evaluation_now or spy["now"].date() != evaluation_now.date() or qqq["now"].date() != evaluation_now.date():
        raise TrendInputError("daily archive is stale or future-dated")
    if spy["today"] != qqq["today"] or spy["now"].date() != qqq["now"].date():
        raise TrendInputError("SPY and QQQ daily archives are not from the same completed-session day")
    observed = spy["observed_at"].isoformat().replace("+00:00", "Z")
    observation = {"schema_version": 1, "source_kind": "observed", "symbol": "SPY", "bars_complete": True,
                   "observed_at": observed, "bars": spy["bars"],
                   "evidence": [{"evidence_id": f"equity-bars:{spy['observation_sha256']}", "sha256": spy["observation_sha256"], "source": "alpaca-iex"},
                                {"evidence_id": f"equity-qqq:{qqq['observation_sha256']}", "sha256": qqq["observation_sha256"], "source": "alpaca-iex"}]}
    observation["normalized_payload_sha256"] = payload_sha256(observation, hash_field="normalized_payload_sha256")
    closes = [Decimal(row["c"]) for row in spy["bars"]]
    difference = closes[-1] - sum(closes, Decimal("0")) / Decimal(200)
    if difference == 0:
        raise TrendInputError("frozen trend is equal to its 200-session mean")
    hint = {"direction": "up" if difference > 0 else "down", "right": "call" if difference > 0 else "put",
            "bars_sha256": payload_sha256(spy["bars"]), "last_close": str(closes[-1]),
            "mean_200": str(sum(closes, Decimal("0")) / Decimal(200))}
    hint["sha256"] = payload_sha256(hint, hash_field="sha256")
    context = {"schema_version": 1, "symbol": "SPY", "now": spy["now"].isoformat().replace("+00:00", "Z"),
               "sessions": spy["sessions"], "session_verified": True,
               "current_session": {"date": spy["today"], "is_open": spy["is_open"]},
               "session_calendar_evidence": {"evidence_id": f"equity-calendar:{spy['context_sha256']}", "sha256": spy["context_sha256"], "source": "alpaca-calendar"},
               "trend_archive": {"spy_sha256": spy["archive_sha256"], "qqq_sha256": qqq["archive_sha256"], "prior_completed_sessions": 200},
               "direction_hint": hint}
    context["normalized_payload_sha256"] = payload_sha256(context, hash_field="normalized_payload_sha256")
    return observation, context


def current_direction_hint(state_dir: str) -> dict[str, Any]:
    """Load the current frozen hint only after verifying both trend artifacts."""
    try:
        paths = resolve_current(state_dir, "trend-observation.json", "trend-context.json")
        observation, context = _read(str(paths["trend-observation.json"])), _read(str(paths["trend-context.json"]))
    except (DatedArtifactError, TrendInputError) as exc:
        raise TrendInputError("current frozen trend artifacts are unavailable") from exc
    if observation.get("normalized_payload_sha256") != payload_sha256(observation, hash_field="normalized_payload_sha256") or context.get("normalized_payload_sha256") != payload_sha256(context, hash_field="normalized_payload_sha256"):
        raise TrendInputError("current frozen trend hash is invalid")
    hint = context.get("direction_hint")
    if not isinstance(hint, Mapping) or hint.get("sha256") != payload_sha256(hint, hash_field="sha256"):
        raise TrendInputError("current direction hint is invalid")
    if hint.get("bars_sha256") != payload_sha256(observation.get("bars")) or hint.get("right") not in {"call", "put"}:
        raise TrendInputError("current direction hint is not bound to frozen bars")
    return dict(hint)


def _read(path: str) -> dict[str, Any]:
    source = Path(path)
    if source.is_symlink() or not source.is_file():
        raise TrendInputError("daily archive path must be a regular file")
    try:
        value = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise TrendInputError("daily archive is unreadable") from exc
    return dict(_mapping(value, "daily archive"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spy-archive")
    parser.add_argument("--qqq-archive")
    parser.add_argument("--paired-state-dir", help="read paired-daily.json through its current manifest")
    parser.add_argument("--state-dir", required=True)
    args = parser.parse_args(argv)
    try:
        if args.paired_state_dir:
            if args.spy_archive or args.qqq_archive:
                parser.error("--paired-state-dir cannot be combined with individual archives")
            from .options_trend_capture import current_pair
            spy_archive, qqq_archive = current_pair(args.paired_state_dir)
        else:
            if not args.spy_archive or not args.qqq_archive:
                parser.error("provide --paired-state-dir or both individual archives")
            spy_archive, qqq_archive = _read(args.spy_archive), _read(args.qqq_archive)
        observation, context = build_trend_inputs(spy_archive, qqq_archive)
        date = context["current_session"]["date"]
        observation_path = save_stage(args.state_dir, date, "trend-observation.json", observation)
        context_path = save_stage(args.state_dir, date, "trend-context.json", context)
        result = {"status": "ready", "date": date, "observation_path": str(observation_path), "context_path": str(context_path), "orders_sent": 0}
    except (TrendInputError, DatedArtifactError) as exc:
        result = {"status": "abstain", "reason_code": type(exc).__name__, "orders_sent": 0}
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
