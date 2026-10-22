#!/usr/bin/env python3
"""Publish a bounded, read-only operational readiness view for paper desks.

This reports worker scheduling and structural evidence only. It never observes
quotes, signals, fills, or order eligibility, and does not write desk state.
"""
from __future__ import annotations

import argparse
import hashlib
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import time
from typing import Any, Callable, Mapping


UTC = timezone.utc
MAX_READ_BYTES = 1_000_000
MAX_SYSTEMCTL_BYTES = 32_768
MAX_STATUS_AGE_SECONDS = 180
SYSTEMD_UNITS = (
    "neorome-equity-intraday-iex-fullfunded.service",
    "neorome-equity-intraday-iex-fullfunded.timer",
    "neorome-equity-intraday-watchlist.timer",
    "neorome-stochastic-paper-v2.service",
    "neorome-stochastic-paper-v2.timer",
    "neorome-btc-stochastic-paper.service",
    "neorome-btc-stochastic-paper.timer",
    "neorome-paper-runtime.service",
    "neorome-paper-runtime.timer",
    "neorome-crypto-recorder.service",
    "neorome-crypto-recorder.timer",
)


def _stamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def _read_json(path: str | os.PathLike[str]) -> tuple[Mapping[str, Any] | None, str]:
    source = Path(path)
    try:
        if source.is_symlink() or not source.is_file():
            return None, "missing"
        fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            raw = stream.read(MAX_READ_BYTES + 1)
        if len(raw) > MAX_READ_BYTES:
            return None, "oversize"
        value = json.loads(raw)
        return (value, "ok") if isinstance(value, Mapping) else (None, "invalid")
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None, "unreadable"


def _freshness(value: Mapping[str, Any] | None, keys: tuple[str, ...], now: datetime,
               max_age: int = MAX_STATUS_AGE_SECONDS) -> tuple[bool, str | None]:
    for key in keys:
        parsed = _parse_time(None if value is None else value.get(key))
        if parsed is None:
            continue
        age = (now.astimezone(UTC) - parsed).total_seconds()
        return (0 <= age <= max_age, _stamp(parsed))
    return False, None


def _systemd_rows(run: Callable[..., Any], units: tuple[str, ...] = SYSTEMD_UNITS
                  ) -> tuple[dict[str, dict[str, str]], str | None]:
    try:
        response = run(
            ["systemctl", "show", "--no-pager",
             "--property=Id,LoadState,ActiveState,Result,MainPID,NextElapseUSecRealtime,NextElapseUSecMonotonic",
             *units], capture_output=True, text=True, timeout=5, check=False)
        output = response.stdout
        if response.returncode or not isinstance(output, str) or len(output) > MAX_SYSTEMCTL_BYTES:
            return {}, "systemd_query_failed"
        rows: dict[str, dict[str, str]] = {}
        for block in output.strip().split("\n\n"):
            fields = dict(line.split("=", 1) for line in block.splitlines() if "=" in line)
            unit = fields.get("Id")
            if unit:
                if unit in rows:
                    return {}, "systemd_duplicate_unit"
                rows[unit] = fields
        return rows, None
    except (OSError, subprocess.SubprocessError, TypeError):
        return {}, "systemd_query_failed"


def _unit_loaded(rows: Mapping[str, Mapping[str, str]], unit: str) -> bool:
    return rows.get(unit, {}).get("LoadState") == "loaded"


def _timer_armed(rows: Mapping[str, Mapping[str, str]], unit: str) -> bool:
    row = rows.get(unit, {})
    invalid = {"", "n/a", "-", "0", "infinity"}
    scheduled = any(row.get(key, "").strip().lower() not in invalid
                    for key in ("NextElapseUSecRealtime", "NextElapseUSecMonotonic"))
    service = rows.get(unit.removesuffix(".timer") + ".service", {})
    running = service.get("ActiveState") in {"active", "activating"} and service.get("MainPID", "0") not in {"", "0"}
    return _unit_loaded(rows, unit) and row.get("ActiveState") == "active" and (scheduled or running)


def _recorder_evidence(path, now):
    """Inspect only the latest collector record, with a bounded read-only query."""
    try:
        from neorome.recorder_ops import _extract_quote_timestamp
        database = Path(path)
        if database.is_symlink() or not database.is_file():
            raise ValueError('missing recorder')
        db = sqlite3.connect(f"file:{database}?mode=ro", uri=True, timeout=0.5)
        try:
            started = time.monotonic()
            db.set_progress_handler(lambda: int(time.monotonic() - started > 0.5), 1000)
            row = db.execute("SELECT started_at,status,substr(observation,1,262145) FROM attempts ORDER BY id DESC LIMIT 1").fetchone()
        finally:
            db.close()
        if not row or row[1] != 'fresh' or not isinstance(row[2], str) or len(row[2]) > 262144:
            raise ValueError('latest observation unavailable')
        heartbeat = _parse_time(row[0]); quote_at = _extract_quote_timestamp(row[2])
        healthy = all(t is not None and 0 <= (now-t).total_seconds() <= 180 for t in (heartbeat, quote_at))
        return {'healthy': healthy, 'read_status': 'ok', 'heartbeat_at': row[0],
                'source_timestamp': _stamp(quote_at) if quote_at else None}
    except (OSError, ValueError, sqlite3.Error, ImportError):
        return {'healthy': False, 'read_status': 'unavailable'}


def _json_status(path: str | os.PathLike[str], *, keys: tuple[str, ...], now: datetime,
                 require_healthy: bool = False, required: Mapping[str, Any] | None = None
                 ) -> dict[str, Any]:
    value, read_status = _read_json(path)
    fresh, timestamp = _freshness(value, keys, now)
    checks = read_status == "ok" and fresh
    if require_healthy:
        checks = checks and value is not None and value.get("operational_healthy") is True
    for key, expected in (required or {}).items():
        checks = checks and value is not None and value.get(key) == expected
    return {"read_status": read_status, "fresh": fresh, "timestamp": timestamp,
            "healthy": bool(checks)}


def _profile(rows: Mapping[str, Mapping[str, str]], *, service: str, timer: str,
             structural: bool, evidence: Mapping[str, Any] | None = None,
             systemd_error: str | None = None) -> dict[str, Any]:
    timer_ok = _timer_armed(rows, timer)
    service_ok = _unit_loaded(rows, service)
    service_row = rows.get(service, {})
    if service_row.get("ActiveState") == "failed" or service_row.get("Result") not in (None, "", "success", "n/a"):
        service_ok = False
    armed = systemd_error is None and timer_ok and service_ok and structural
    return {
        "operational_state": "armed_for_session" if armed else "unknown",
        "entry_eligibility": "unknown",
        "checks": {"service_loaded": service_ok, "timer_active_with_next_trigger": timer_ok,
                   "structural_checks_passed": structural},
        "evidence": dict(evidence or {}),
        **({"systemd_error": systemd_error} if systemd_error else {}),
    }


def build_projection(*, paths: Mapping[str, str | os.PathLike[str]] | None = None,
                     now: datetime | None = None, run: Callable[..., Any] = subprocess.run,
                     units: tuple[str, ...] | None = None) -> dict[str, Any]:
    """Return readiness evidence; paths are injectable for isolated tests."""
    current = (now or datetime.now(UTC)).astimezone(UTC)
    source = {
        "equity_package": "/opt/neorome-equity-iex-fullfunded-r2/package",
        "equity_profile": "/var/lib/neorome-equity-iex-fullfunded-v1/profile.json",
        "equity_database": "/var/lib/neorome-equity-iex-fullfunded-v1/paper.sqlite3",
        "equity_watchlist": "/var/lib/neorome-equity-intraday/frozen-universe-current.json",
        "option_config_a": "/opt/neorome-stochastic-paper/v2/config-A.json",
        "option_config_b": "/opt/neorome-stochastic-paper/v2/config-B.json",
        "option_arm_a": "/var/lib/neorome-stochastic-paper/v2/arms/A.json",
        "option_arm_b": "/var/lib/neorome-stochastic-paper/v2/arms/B.json",
        "prediction_feed": "/var/lib/neorome-btc-cross30-paper/feed-status.json",
        "crypto_status": "/var/lib/neorome-paper-runtime/status.json",
        "btc_recorder_database": "/var/lib/neorome-crypto-recorder/recorder.sqlite3",
    }
    source.update(paths or {})
    unit_set = set(units or SYSTEMD_UNITS)
    # Tests can restrict the batch, while production remains a fixed allow-list.
    selected = tuple(unit for unit in SYSTEMD_UNITS if unit in unit_set)
    rows, systemd_error = _systemd_rows(run, selected)

    package = Path(source["equity_package"])
    profile, profile_read = _read_json(source["equity_profile"])
    watchlist, watchlist_read = _read_json(source["equity_watchlist"])
    db_check = "unavailable"
    try:
        database = Path(source["equity_database"])
        if not database.is_symlink() and database.is_file():
            connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True, timeout=0.5)
            try:
                started = time.monotonic()
                connection.set_progress_handler(lambda: int(time.monotonic() - started > 0.5), 1000)
                result = connection.execute("PRAGMA quick_check(1)").fetchone()
                tables = connection.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1").fetchone()
                db_check = "ok" if result == ("ok",) and tables is not None else "invalid"
            finally:
                connection.close()
    except (OSError, sqlite3.Error, ValueError):
        db_check = "invalid"
    watchlist_ok = watchlist_read == "ok" and (
        isinstance(watchlist, list) and bool(watchlist) or
        isinstance(watchlist, Mapping) and bool(watchlist)
    )
    equity_structure = (package.is_dir() and not package.is_symlink() and (package / "neorome/equity_intraday_iex_fullfunded.py").is_file()
                        and profile_read == "ok" and profile.get("profile_id") == "EQ-INTRADAY-IEX-FULLFUNDED-v1"
                        and profile.get("broker_orders_enabled") is False
                        and _timer_armed(rows, "neorome-equity-intraday-watchlist.timer")
                        and watchlist_ok and db_check == "ok")
    equity = _profile(rows,
        service="neorome-equity-intraday-iex-fullfunded.service",
        timer="neorome-equity-intraday-iex-fullfunded.timer",
        structural=equity_structure, systemd_error=systemd_error,
        evidence={"package_structure": "ok" if package.is_dir() and (package / "neorome/equity_intraday_iex_fullfunded.py").is_file() else "missing_or_invalid",
                  "profile": profile_read, "paper_database": db_check,
                  "frozen_watchlist": watchlist_read if watchlist_ok else "missing_or_invalid"})
    watcher = {"operational_state": "active" if _timer_armed(rows, "neorome-equity-intraday-watchlist.timer") else "unknown",
               "entry_eligibility": "unknown",
               "checks": {"timer_active_with_next_trigger": _timer_armed(rows, "neorome-equity-intraday-watchlist.timer")}}

    option_profiles = {}
    for arm in ("A", "B"):
        config, config_read = _read_json(source[f"option_config_{arm.lower()}"])
        state, state_read = _read_json(source[f"option_arm_{arm.lower()}"])
        structural = (config_read == "ok" and state_read == "ok" and config is not None
                      and state is not None and isinstance(config.get("strategy_id"), str)
                      and state.get("schema_version") == 2
                      and state.get("strategy_id") == config.get("strategy_id")
                      and isinstance(state.get("positions"), Mapping)
                      and isinstance(state.get("trades"), list)
                      and isinstance(state.get("config_fingerprint"), str)
                      and state.get("config_fingerprint") == hashlib.sha256(json.dumps(config, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
                      and all(isinstance(state.get(key), (int, float))
                              and not isinstance(state.get(key), bool)
                              and math.isfinite(state.get(key))
                              for key in ("cash", "initial_cash", "realized_pnl")))
        option_profiles[arm] = _profile(rows, service="neorome-stochastic-paper-v2.service",
            timer="neorome-stochastic-paper-v2.timer", structural=structural, systemd_error=systemd_error,
            evidence={"config": config_read, "arm_state": state_read,
                      "current_session_file": "not_required_for_armed_for_session"})


    prediction = _json_status(source["prediction_feed"], keys=("finished_at", "as_of"), now=current,
                              required={"mode": "internal-paper"})
    prediction_profile = _profile(rows, service="neorome-btc-stochastic-paper.service",
        timer="neorome-btc-stochastic-paper.timer", structural=prediction["healthy"],
        evidence={"feed_status": prediction}, systemd_error=systemd_error)

    crypto = _json_status(source["crypto_status"], keys=("updated_at",), now=current,
                          require_healthy=True)
    recorder = _recorder_evidence(source["btc_recorder_database"], current)
    crypto_profile = _profile(rows, service="neorome-paper-runtime.service",
        timer="neorome-paper-runtime.timer",
        structural=(crypto["healthy"] and recorder["healthy"]
                    and _unit_loaded(rows, "neorome-crypto-recorder.service")
                    and _timer_armed(rows, "neorome-crypto-recorder.timer")),
        evidence={"paper_runtime_status": crypto, "btc_recorder_status": recorder,
                  "btc_recorder_timer_active_with_next_trigger": _timer_armed(rows, "neorome-crypto-recorder.timer")},
        systemd_error=systemd_error)

    desks = {
        "equities": {"status": "active" if equity["operational_state"] == "armed_for_session" else "unknown",
                     "entry_eligibility": "unknown", "profiles": {"iex_funded": equity}},
        "options": {"status": "active" if all(p["operational_state"] == "armed_for_session" for p in option_profiles.values()) else "unknown",
                    "entry_eligibility": "unknown", "profiles": option_profiles},
        "prediction": {"status": "active" if prediction_profile["operational_state"] == "armed_for_session" else "unknown",
                       "entry_eligibility": "unknown", "profiles": {"btc_stochastic": prediction_profile}},
        "crypto": {"status": "active" if crypto_profile["operational_state"] == "armed_for_session" else "unknown",
                   "entry_eligibility": "unknown", "profiles": {"paper_runtime": crypto_profile}},
    }
    return {"schema_version": 1, "as_of": _stamp(current), "max_age_seconds": MAX_STATUS_AGE_SECONDS,
            "desks": desks}


def publish(output: str | os.PathLike[str], **kwargs: Any) -> dict[str, Any]:
    projection = build_projection(**kwargs)
    destination = Path(output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".desk-readiness-", dir=destination.parent)
    try:
        os.fchmod(fd, 0o644)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(projection, stream, sort_keys=True, separators=(",", ":"), allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, destination)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
    return projection


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="/var/lib/neorome-maintenance-ai/inbox/desk-readiness.json")
    args = parser.parse_args()
    print(json.dumps(publish(args.output), sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
