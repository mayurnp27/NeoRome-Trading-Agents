"""Sole-``neorome`` shared-ledger worker for weather prediction paper trades.

It consumes only the sanitized immutable inbox.  This module has no network,
credentials, broker client, or venue order path.  A later snapshot must differ
from the entry snapshot before it can fill an existing reservation.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import stat
import tempfile
from typing import Any, Mapping

from .paper_ledger import PaperLedger
from .prediction_full_port import DEFAULT_LEDGER, PredictionFullPortError, open_existing
from .prediction_internal_paper import PredictionInternalPaperError, run as paper_run
from .prediction_weather_inbox import PredictionWeatherInboxError, read_current_candidate_record, read_current_snapshot

UTC = timezone.utc
DEFAULT_STATE = "/var/lib/neorome-prediction-weather-paper"
DEFAULT_INBOX = "/var/lib/neorome-prediction-weather-inbox"
DEFAULT_RECOVERY = "/var/lib/neorome-paper-recovery/status.json"


class PredictionWeatherLifecycleError(ValueError):
    pass


def _time(value: Any) -> datetime:
    try: parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc: raise PredictionWeatherLifecycleError("timestamp is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None: raise PredictionWeatherLifecycleError("timestamp lacks timezone")
    return parsed.astimezone(UTC)


def _recovery_ok(path: str | os.PathLike[str] | None, now: datetime) -> bool:
    if path is None: return True
    source = Path(path)
    if source.is_symlink() or not source.is_file(): return False
    try:
        value = json.loads(source.read_text()); updated = _time(value.get("updated_at"))
    except (OSError, json.JSONDecodeError, PredictionWeatherLifecycleError): return False
    return value.get("operational_healthy") is True and timedelta(0) <= now - updated <= timedelta(hours=2)


def _ledger(path: str | os.PathLike[str], now: datetime) -> PaperLedger:
    try:
        return open_existing(path, now=now)
    except PredictionFullPortError as exc:
        raise PredictionWeatherLifecycleError(str(exc)) from exc


def _lock(root: Path):
    root.mkdir(parents=True, exist_ok=True, mode=0o700); os.chmod(root, 0o700)
    if root.is_symlink() or not root.is_dir(): raise PredictionWeatherLifecycleError("lifecycle state directory is unsafe")
    descriptor = os.open(root / "runtime.lock", os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode): raise PredictionWeatherLifecycleError("lifecycle lock is unsafe")
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB); return descriptor
    except Exception:
        os.close(descriptor); raise


def _quote(snapshot: Mapping[str, Any], candidate: Mapping[str, Any] | None, entry_snapshot: Any) -> tuple[dict[str, Any] | None, Mapping[str, Any] | None]:
    if not isinstance(candidate, Mapping) or snapshot.get("status") != "eligible_execution_snapshot" or snapshot.get("snapshot_sha256") == entry_snapshot:
        return None, None
    market_id = candidate.get("market_identity", {}).get("market_id")
    for execution in snapshot.get("executions", []):
        if not isinstance(execution, Mapping) or execution.get("market_identity", {}).get("market_id") != market_id: continue
        timing, quote, market, fee = execution.get("snapshot_timing"), execution.get("quote"), execution.get("market_identity"), execution.get("fee_schema")
        if not all(isinstance(item, Mapping) for item in (timing, quote, market, fee)): continue
        source_hash = quote.get("size_provenance", {}).get("sha256") if isinstance(quote.get("size_provenance"), Mapping) else None
        if not isinstance(source_hash, str): continue
        outcome = market.get("outcome")
        return {"market_id": market_id, "outcome_id": market.get("outcome_id"), "outcome": outcome.lower() if isinstance(outcome, str) else outcome, "fee_version": fee.get("version"),
                "event_at": timing.get("event_at"), "received_at": timing.get("received_at"), "source_hash": source_hash,
                "asks": quote.get("asks"), "bids": quote.get("bids")}, execution
    return None, None


def _completed(root: Path) -> set[str]:
    path = root / "completed-candidates.json"
    if not path.exists(): return set()
    if path.is_symlink() or not path.is_file(): raise PredictionWeatherLifecycleError("completed candidate state is unsafe")
    try: value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc: raise PredictionWeatherLifecycleError("completed candidate state is unreadable") from exc
    if not isinstance(value, dict) or not isinstance(value.get("candidate_sha256"), list) or any(not isinstance(item, str) for item in value["candidate_sha256"]):
        raise PredictionWeatherLifecycleError("completed candidate state is invalid")
    return set(value["candidate_sha256"])


def _complete(root: Path, values: set[str]) -> None:
    path = root / "completed-candidates.json"; descriptor, temporary = tempfile.mkstemp(prefix=".prediction-weather-completed-", dir=root)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w") as stream:
            descriptor = -1; json.dump({"schema_version": 1, "candidate_sha256": sorted(values)}, stream, sort_keys=True, separators=(",", ":")); stream.write("\n"); stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if descriptor >= 0: os.close(descriptor)
        try: os.unlink(temporary)
        except FileNotFoundError: pass


def _terminal_in_ledger(book: PaperLedger, candidate: Mapping[str, Any] | None) -> bool:
    """Recover a completed candidate even if a crash preceded state-file write."""
    if not isinstance(candidate, Mapping) or not isinstance(candidate.get("candidate_sha256"), str): return False
    proposal_id = "prediction-contract:" + candidate["candidate_sha256"][:32]
    reservations, positions = set(), set()
    for event in book.audit():
        payload = event.payload.get("payload", {}) if isinstance(event.payload, Mapping) else {}
        if event.event_type == "reserve" and isinstance(payload, Mapping) and payload.get("proposal_id") == proposal_id: reservations.add(event.entity_id)
        elif event.event_type == "fill" and isinstance(event.payload, Mapping) and event.payload.get("reservation_id") in reservations: positions.add(event.payload.get("position_id"))
        elif event.event_type == "close" and isinstance(event.payload, Mapping) and event.payload.get("position_id") in positions: return True
    return False


def _settlement(snapshot: Mapping[str, Any], candidate: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(candidate, Mapping): return None
    market = candidate.get("market_identity", {})
    for row in snapshot.get("settlements", []):
        if isinstance(row, Mapping) and row.get("market_id") == market.get("market_id") and row.get("terms_sha256") == market.get("terms_sha256"):
            return dict(row)
    return None


def run_once(*, inbox_root: str | os.PathLike[str] = DEFAULT_INBOX, ledger_path: str | os.PathLike[str] = DEFAULT_LEDGER,
             state_dir: str | os.PathLike[str] = DEFAULT_STATE, recovery_status: str | os.PathLike[str] | None = DEFAULT_RECOVERY,
             now: Any = None, request_close: bool = False) -> dict[str, Any]:
    current = _time(now) if now is not None else datetime.now(UTC)
    try:
        descriptor = _lock(Path(state_dir))
        try:
            record = read_current_candidate_record(inbox_root)
            candidate = record.get("candidate") if record else None
            completed = _completed(Path(state_dir))
            original_candidate = candidate
            snapshot = read_current_snapshot(inbox_root)
            settlement = _settlement(snapshot or {}, candidate)
            stopped = (Path(state_dir) / "STOP").exists()
            book = _ledger(ledger_path, current)
            if isinstance(candidate, Mapping) and (candidate.get("candidate_sha256") in completed or _terminal_in_ledger(book, candidate)): candidate = None
            later_quote, _execution = _quote(snapshot or {}, candidate, record.get("execution_snapshot_sha256") if record else None)
            result = paper_run(book, state_dir, now=current, candidate=candidate, later_quote=later_quote, settlement=settlement,
                               mark_provider=lambda *_: [], allow_entry=not stopped, recovery_ok=_recovery_ok(recovery_status, current), request_close=request_close)
            if isinstance(original_candidate, Mapping) and any(item.get("type") in {"settlement", "close"} for item in result.get("actions", []) if isinstance(item, Mapping)):
                completed.add(str(original_candidate["candidate_sha256"])); _complete(Path(state_dir), completed)
            return {"mode": "prediction_weather_full_port_paper", "status": "paper_lifecycle", "lifecycle": result,
                    "ledger_mutations": len(result.get("actions", [])), "orders_sent": 0, "venue_requests": 0}
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN); os.close(descriptor)
    except (PredictionWeatherLifecycleError, PredictionWeatherInboxError, PredictionInternalPaperError, OSError, ValueError, sqlite3.Error) as exc:
        return {"mode": "prediction_weather_shared_paper", "status": "abstained", "abstention_reasons": [str(exc)], "ledger_mutations": 0, "orders_sent": 0, "venue_requests": 0}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inbox-root", default=DEFAULT_INBOX); parser.add_argument("--ledger", default=DEFAULT_LEDGER); parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--recovery-status", default=DEFAULT_RECOVERY); parser.add_argument("--now"); parser.add_argument("--request-close", action="store_true")
    args = parser.parse_args(argv); result = run_once(inbox_root=args.inbox_root, ledger_path=args.ledger, state_dir=args.state, recovery_status=args.recovery_status, now=args.now, request_close=args.request_close)
    print(json.dumps(result, sort_keys=True, separators=(",", ":"))); return 0 if result["status"] in {"abstained", "paper_lifecycle"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
