"""Collect bounded, prospective Jupiter sell-route observations for tokens.

The collector is read-only. It never reserves paper capital or sends a swap.
It records one exact-size sell quote in each predeclared horizon window, or an
explicit missing-window outcome when the window has passed.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import secrets
from typing import Any, Mapping

from .jupiter_quotes import QuoteClient, QuoteError, USDC

UTC = timezone.utc
HORIZONS_MINUTES = (15, 60, 240)
WINDOW_SECONDS = 90
MAX_QUOTES_PER_CYCLE = 3
SCHEMA_VERSION = "token-outcome-collector-v1"


def _time(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must include timezone")
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return _time(value).isoformat().replace("+00:00", "Z")


def _atomic(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            descriptor = -1
            json.dump(value, stream, sort_keys=True, separators=(",", ":"), allow_nan=False)
            stream.write("\n"); stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _load(path: Path) -> dict[str, Any] | None:
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 2_000_000:
            return None
        value = json.loads(path.read_text())
    except (OSError, UnicodeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _entry(candidate: Mapping[str, Any]) -> tuple[str, str, datetime] | None:
    mint, acquired, entry = candidate.get("mint"), candidate.get("acquired_token_raw"), candidate.get("entry_quote")
    if not isinstance(mint, str) or not isinstance(acquired, str) or not acquired.isdecimal() or int(acquired) <= 0:
        return None
    if not isinstance(entry, Mapping) or not isinstance(entry.get("received_at"), str):
        return None
    try:
        return mint, acquired, _time(entry["received_at"])
    except (TypeError, ValueError):
        return None


def run_once(state_dir: str | Path, *, now: datetime | None = None, quote_client=None) -> dict[str, Any]:
    root = Path(state_dir)
    current = _time(now or datetime.now(UTC))
    candidates = root / "candidates"
    outcomes = root / "outcomes"
    requests = 0
    updated = 0
    for candidate_path in sorted(candidates.glob("*.json")) if candidates.is_dir() else []:
        candidate = _load(candidate_path)
        if candidate is None or not isinstance(candidate.get("content_sha256"), str):
            continue
        identity = _entry(candidate)
        if identity is None:
            continue
        mint, amount, entry_received = identity
        output = outcomes / f"{candidate['content_sha256']}.json"
        existing = _load(output) or {"schema_version": SCHEMA_VERSION, "candidate_content_sha256": candidate["content_sha256"],
                                    "mint": mint, "entry_received_at": _iso(entry_received), "horizons": {}}
        horizon_state = existing.setdefault("horizons", {})
        if not isinstance(horizon_state, dict):
            horizon_state = existing["horizons"] = {}
        changed = False
        for horizon in HORIZONS_MINUTES:
            key = str(horizon)
            if key in horizon_state:
                continue
            target = entry_received + timedelta(minutes=horizon)
            deadline = target + timedelta(seconds=WINDOW_SECONDS)
            if current > deadline:
                horizon_state[key] = {"status": "unresolved_missing_route_window", "target_at": _iso(target),
                                      "deadline_at": _iso(deadline), "recorded_at": _iso(current)}
                changed = True
            elif current >= target and requests < MAX_QUOTES_PER_CYCLE:
                try:
                    route = (quote_client or QuoteClient()).quote(mint, USDC, amount)
                    horizon_state[key] = {"status": "observed_route", "target_at": _iso(target),
                                          "deadline_at": _iso(deadline), "route": route, "recorded_at": _iso(current)}
                except QuoteError as exc:
                    horizon_state[key] = {"status": "route_request_failed", "target_at": _iso(target),
                                          "deadline_at": _iso(deadline), "reason": str(exc), "recorded_at": _iso(current)}
                requests += 1
                changed = True
        if changed:
            existing["updated_at"] = _iso(current)
            _atomic(output, existing)
            updated += 1
    return {"status": "ok", "requests": requests, "candidates_updated": updated, "orders_sent": 0}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(); parser.add_argument("--state", required=True)
    args = parser.parse_args(argv)
    try:
        result = run_once(args.state)
    except Exception as exc:
        result = {"status": "operational_failure", "error_type": type(exc).__name__, "orders_sent": 0}
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 1 if result["status"] == "operational_failure" else 0


if __name__ == "__main__":
    raise SystemExit(main())
