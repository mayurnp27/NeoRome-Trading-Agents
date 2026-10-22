#!/usr/bin/env python3
"""Publish a curated, read-only view of the stochastic paper engine."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import stat
import time
from typing import Any

UTC = timezone.utc
POLL_SECONDS = 15
MAX_FILE_BYTES = 1024 * 1024
MAX_LEDGER_BYTES = 16 * 1024 * 1024
MAX_SOURCE_AGE_SECONDS = 180
MAX_POSITIONS = 100
MAX_CLOSED_TRADES = 20
DEFAULT_ROOT = Path("/var/lib/neorome-stochastic-paper/v2")
DEFAULT_CONFIG_ROOT = Path("/opt/neorome-stochastic-paper/v2")
DEFAULT_FEEDBACK = Path("/var/lib/neorome-maintenance-ai/inbox/paper-feedback.json")


class SourceError(Exception):
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(UTC) if parsed.tzinfo else None
    except ValueError:
        return None


def _check_no_symlink(path: Path) -> None:
    absolute = path.absolute()
    current = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        current = current / part
        try:
            info = current.lstat()
        except FileNotFoundError:
            raise SourceError("missing")
        if stat.S_ISLNK(info.st_mode):
            raise SourceError("symlink")


def _read_source(path: Path, *, max_bytes: int = MAX_FILE_BYTES) -> tuple[dict[str, Any], os.stat_result]:
    _check_no_symlink(path)
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except FileNotFoundError as exc:
        raise SourceError("missing") from exc
    except OSError as exc:
        raise SourceError("unreadable") from exc
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            raise SourceError("not_regular_file")
        if before.st_size > max_bytes:
            raise SourceError("oversized")
        with os.fdopen(fd, "rb") as stream:
            raw = stream.read(max_bytes + 1)
            after = os.fstat(stream.fileno())
        if len(raw) > max_bytes:
            raise SourceError("oversized")
        if before.st_ino != after.st_ino or before.st_mtime_ns != after.st_mtime_ns:
            raise SourceError("changed_during_read")
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        raise
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SourceError("invalid_json") from exc
    if not isinstance(value, dict):
        raise SourceError("invalid_shape")
    return value, after


def _finite(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _source_meta(path: Path, observed_at: Any, now: datetime, *, require_content_time: bool,
                 max_bytes: int = MAX_FILE_BYTES) -> tuple[dict[str, Any], dict[str, Any] | None]:
    try:
        value, info = _read_source(path, max_bytes=max_bytes)
    except SourceError as exc:
        return {"status": exc.reason, "available": False, "observed_at": None, "age_seconds": None}, None
    observed = _parse_time(observed_at) if require_content_time else datetime.fromtimestamp(info.st_mtime, UTC)
    if observed is None:
        return {"status": "timestamp_invalid", "available": True, "observed_at": None, "age_seconds": None}, value
    age = (now - observed).total_seconds()
    if age < -5:
        status = "timestamp_future"
    elif age > MAX_SOURCE_AGE_SECONDS and require_content_time:
        status = "stale"
    else:
        status = "fresh" if require_content_time else "observed"
    return {"status": status, "available": True, "observed_at": _iso(observed),
            "age_seconds": max(0, int(age)) if age >= -5 else None}, value


def _bounded_text(value: Any, maximum: int = 160) -> str | None:
    if not isinstance(value, str) or not value or len(value) > maximum:
        return None
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in value):
        return None
    return value


def _position_projection(raw: Any, *, current_fresh: bool, now: datetime) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    output: dict[str, Any] = {}
    for key in ("symbol", "expiry", "type", "entry_time"):
        text = _bounded_text(raw.get(key))
        if text:
            output[key] = text
    for key in ("strike", "quantity", "entry_ask", "entry_debit"):
        number = _finite(raw.get(key))
        if number is not None:
            output[key] = int(number) if key == "quantity" else number
    mark_time = _parse_time(raw.get("last_quote_time"))
    mark_age = (now - mark_time).total_seconds() if mark_time else None
    mark_fresh = (current_fresh and raw.get("mark_status") == "fresh" and mark_age is not None
                  and 0 <= mark_age <= 15 and _finite(raw.get("last_bid")) is not None)
    output["mark_status"] = "fresh_observation" if mark_fresh else "not_current"
    if mark_fresh:
        output["last_observed_bid"] = _finite(raw.get("last_bid"))
        output["last_quote_time"] = _iso(mark_time)
    return output or None


def _trade_projection(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    output: dict[str, Any] = {}
    for key in ("symbol", "expiry", "type", "entry_time", "exit_time", "reason", "execution_method"):
        text = _bounded_text(raw.get(key))
        if text:
            output[key] = text
    for key in ("strike", "quantity", "entry_ask", "exit_bid", "net_pnl"):
        number = _finite(raw.get(key))
        if number is not None:
            output[key] = int(number) if key == "quantity" else number
    if isinstance(raw.get("late_exit"), bool):
        output["late_exit"] = raw["late_exit"]
    return output or None


def _arm_projection(arm: str, config: dict[str, Any] | None, state: dict[str, Any] | None,
                    current: dict[str, Any] | None, current_fresh: bool, state_fresh: bool,
                    now: datetime) -> dict[str, Any]:
    result: dict[str, Any] = {"initial_cash": 1000, "status": "unknown", "position_count": None,
                              "positions": [], "closed_trades": []}
    if not config or config.get("initial_cash") != 1000:
        result["config_status"] = "unavailable_or_invalid"
    else:
        strategy = _bounded_text(config.get("strategy_id"))
        if strategy:
            result["strategy_id"] = strategy
            result["config_status"] = "valid"
        else:
            result["config_status"] = "unavailable_or_invalid"
    if not state:
        result["state_status"] = "unavailable"
        return result
    result["state_status"] = "available"
    trades = state.get("trades")
    if isinstance(trades, list):
        result["closed_trades"] = [row for item in trades[-MAX_CLOSED_TRADES:]
                                    if (row := _trade_projection(item)) is not None]
    current_row = current.get("arms", {}).get(arm) if isinstance(current, dict) else None
    if not isinstance(current_row, dict):
        current_row = {}
    arm_current_fresh = current_fresh and bool(current_row)
    for name in ("cash", "realized_pnl"):
        number = _finite(current_row.get(name))
        if number is None:
            number = _finite(state.get(name))
        if number is not None:
            result[name] = number
    arm_state_positions = state.get("positions", {})
    if isinstance(arm_state_positions, dict):
        source_positions = list(arm_state_positions.values())
    elif isinstance(arm_state_positions, list):
        source_positions = arm_state_positions
    else:
        source_positions = []
    result["position_count"] = len(source_positions)
    result["positions_truncated"] = len(source_positions) > MAX_POSITIONS
    result["positions"] = [item for raw in source_positions[:MAX_POSITIONS]
                            if (item := _position_projection(raw, current_fresh=arm_current_fresh and state_fresh, now=now)) is not None]
    current_positions = current_row.get("positions", [])
    all_marks_fresh = (isinstance(current_positions, list) and len(current_positions) == len(source_positions)
        and len(result["positions"]) == len(source_positions) and all(
        isinstance(row, dict) and row.get("mark_status") == "fresh" and
        (_parse_time(row.get("last_quote_time")) is not None) and
        0 <= (now - _parse_time(row.get("last_quote_time"))).total_seconds() <= 15
        for row in current_positions
    ) and all(item.get("mark_status") == "fresh_observation" for item in result["positions"]))
    equity = _finite(current_row.get("equity"))
    if arm_current_fresh and state_fresh and all_marks_fresh and equity is not None:
        result["equity"] = equity
        result["equity_status"] = "observed"
    else:
        result["equity_status"] = "not_observed"
    status = _bounded_text(current_row.get("status"), 64)
    if status:
        result["status"] = status
    return result


def build_feedback(*, current_path: Path, state_paths: dict[str, Path], config_paths: dict[str, Path],
                   now: datetime | None = None) -> dict[str, Any]:
    current_time = (now or datetime.now(UTC)).astimezone(UTC)
    current_meta, current = _source_meta(current_path, None, current_time, require_content_time=False)
    if current is not None:
        # Runtime status time is content-authoritative; do not use file mtime for freshness.
        current_meta, current = _source_meta(current_path, current.get("asof"), current_time,
                                             require_content_time=True)
    states: dict[str, dict[str, Any] | None] = {}
    configs: dict[str, dict[str, Any] | None] = {}
    sources: dict[str, Any] = {"current": current_meta}
    for arm in ("A", "B"):
        value, state = _source_meta(state_paths[arm], None, current_time, require_content_time=False,
                                    max_bytes=MAX_LEDGER_BYTES)
        if state is not None:
            value, state = _source_meta(state_paths[arm], state.get("last_observed_at"), current_time,
                                        require_content_time=True, max_bytes=MAX_LEDGER_BYTES)
        sources[f"arm_{arm}"] = value
        states[arm] = state
        config_meta, config = _source_meta(config_paths[arm], None, current_time, require_content_time=False)
        sources[f"config_{arm}"] = config_meta
        configs[arm] = config
    current_fresh = current_meta.get("status") == "fresh"
    return {
        "schema_version": 1,
        "as_of": _iso(current_time),
        "mode": "paper_only",
        "readiness": "unknown",
        "readiness_reason": "observation_only",
        "sources": sources,
        "arms": {arm: _arm_projection(arm, configs[arm], states[arm], current, current_fresh,
                                       sources[f"arm_{arm}"].get("status") == "fresh", current_time)
                 for arm in ("A", "B")},
    }


def _atomic_publish(path: Path, value: dict[str, Any]) -> None:
    _check_no_symlink(path.parent)
    if not path.parent.is_dir():
        raise SourceError("output_directory_missing")
    if path.exists() and path.is_symlink():
        raise SourceError("output_symlink")
    temp = path.with_name(path.name + ".tmp")
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0), 0o644)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, sort_keys=True, separators=(",", ":"))
            stream.write("\n")
            stream.flush()
            os.fchmod(stream.fileno(), 0o644)
            os.fsync(stream.fileno())
        os.replace(temp, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def _paths(root: Path, config_root: Path, output: Path) -> tuple[Path, dict[str, Path], dict[str, Path]]:
    return (root / "current.json",
            {arm: root / "arms" / f"{arm}.json" for arm in ("A", "B")},
            {arm: config_root / f"config-{arm}.json" for arm in ("A", "B")})


def run_once(*, root: Path = DEFAULT_ROOT, config_root: Path = DEFAULT_CONFIG_ROOT,
             output: Path = DEFAULT_FEEDBACK, now: datetime | None = None) -> dict[str, Any]:
    current, states, configs = _paths(root, config_root, output)
    value = build_feedback(current_path=current, state_paths=states, config_paths=configs, now=now)
    _atomic_publish(output, value)
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--config-root", type=Path, default=DEFAULT_CONFIG_ROOT)
    parser.add_argument("--output", type=Path, default=DEFAULT_FEEDBACK)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    previous_status = None
    last_emit = 0.0
    while True:
        try:
            value = run_once(root=args.root, config_root=args.config_root, output=args.output)
            status = "published"
            result = 0
        except (OSError, SourceError):
            status = "publish_failed"
            result = 1
        monotonic_now = time.monotonic()
        if args.once or status != previous_status or monotonic_now - last_emit >= 60:
            print(json.dumps({"status": status}, separators=(",", ":")), flush=True)
            previous_status, last_emit = status, monotonic_now
        if args.once:
            return result
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    raise SystemExit(main())
