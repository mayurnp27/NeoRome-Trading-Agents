"""Finite forward-only EQ-INTRADAY internal-paper opportunity scanner.

The scanner is deliberately usable only during the registered 09:46--11:00
New York entry interval.  Within that interval it composes the read-only SIP
decision capture, existing shared-ledger lifecycle, and a later independent
entry quote.  It never backdates a signal, never falls back to another feed,
and every path reports zero broker orders.
"""
from __future__ import annotations

import argparse
from datetime import datetime, time, timezone
import json
import os
from pathlib import Path
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo

from desks.equities.intraday_evidence_archive import canonical_bytes, sha256
from neorome.equity_intraday_paper import run_cycle
from neorome.equity_intraday_sip import SIPClient, capture_decision, capture_later_quote


UTC = timezone.utc
NY = ZoneInfo("America/New_York")
ENTRY_OPEN = time(9, 46)
ENTRY_CLOSE = time(11, 0)


class OpportunityError(ValueError):
    pass


def _now(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise OpportunityError("now must be an ISO timestamp") from exc
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise OpportunityError("now must be timezone-aware")
    return value.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _entry_window(current: datetime) -> bool:
    local = current.astimezone(NY)
    return local.weekday() < 5 and ENTRY_OPEN <= local.time() < ENTRY_CLOSE


def _save_once(path: Path, value: Mapping[str, Any]) -> None:
    raw = canonical_bytes(dict(value))
    if path.exists() or path.is_symlink():
        if path.is_symlink() or not path.is_file() or path.read_bytes() != raw:
            raise OpportunityError("opportunity record collision or unsafe path")
        return
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError as exc:
        raise OpportunityError("opportunity record cannot be written safely") from exc
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _record(root: str | Path, value: Mapping[str, Any]) -> dict[str, Any]:
    result = {**value, "broker_orders_sent": 0, "broker_execution_enabled": False}
    digest = sha256(result)
    _save_once(Path(root) / "opportunities" / f"{digest}.json", result)
    return {**result, "opportunity_sha256": digest}


def run_once(
    archive_root: str | Path,
    universe_path: str | Path,
    symbol: str,
    *,
    state_dir: str | Path,
    ledger_path: str | Path,
    now: datetime | str | None = None,
    client: SIPClient | None = None,
    decision_capture: Callable[..., dict[str, Any]] = capture_decision,
    quote_capture: Callable[..., dict[str, Any]] = capture_later_quote,
    lifecycle: Callable[..., dict[str, Any]] = run_cycle,
) -> dict[str, Any]:
    """Run one current-session opportunity, or retain a no-backdating abstention.

    A positive path can only reserve after a retained current SIP candidate and
    can only fill after a separate later SIP quote. The shared lifecycle owns
    the existing $300/$15/$30/$80 ledger checks and still sends no broker order.
    """

    current = _now(now)
    base = {
        "scope": "eq_intraday_forward_only_opportunity",
        "symbol": symbol,
        "observed_at": _iso(current),
        "ledger_mutations": 0,
    }
    if not _entry_window(current):
        return _record(archive_root, {
            **base, "status": "abstained", "blocker": "entry_window_closed_no_backdating",
            "entry_window": "09:46-11:00 America/New_York", "actions": [],
        })

    active_client = client or SIPClient()
    decision = decision_capture(archive_root, universe_path, symbol, client=active_client, signal_at=current)
    if decision.get("status") != "eligible_unfilled_candidate":
        return _record(archive_root, {
            **base, "status": "abstained", "blocker": "current_sip_candidate_unavailable",
            "decision_status": decision.get("status"), "decision_reason": decision.get("reason"), "actions": [],
        })
    reserved = lifecycle(state_dir, ledger_path, decision, now=current)
    actions = list(reserved.get("actions", ()))
    if not any(action.get("type") == "reserve_observed_internal_paper" for action in actions if isinstance(action, Mapping)):
        return _record(archive_root, {
            **base, "status": "abstained", "blocker": reserved.get("blocker", "reservation_not_advanced"),
            "decision_sha256": decision.get("decision_sha256"), "actions": actions,
            "ledger_mutations": len(actions),
        })
    entry = quote_capture(archive_root, symbol, "entry", client=active_client)
    if entry.get("status") != "captured":
        return _record(archive_root, {
            **base, "status": "advanced", "blocker": "awaiting_independent_entry_quote",
            "decision_sha256": decision.get("decision_sha256"), "actions": actions,
            "ledger_mutations": len(actions),
        })
    filled = lifecycle(state_dir, ledger_path, decision, entry_fill_quote=entry["quote"], now=_now(None))
    actions.extend(filled.get("actions", ()))
    return _record(archive_root, {
        **base, "status": filled.get("status", "abstained"), "blocker": filled.get("blocker"),
        "decision_sha256": decision.get("decision_sha256"), "entry_capture_sha256": entry.get("capture_sha256"),
        "actions": actions, "ledger_mutations": len(actions),
    })


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-root", required=True)
    parser.add_argument("--universe", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--ledger", required=True)
    parser.add_argument("--now")
    args = parser.parse_args(argv)
    try:
        result = run_once(args.archive_root, args.universe, args.symbol, state_dir=args.state_dir,
                          ledger_path=args.ledger, now=args.now)
    except Exception as exc:
        result = {"scope": "eq_intraday_forward_only_opportunity", "status": "operational_failure",
                  "error_type": type(exc).__name__, "broker_orders_sent": 0, "broker_execution_enabled": False}
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0 if result.get("status") in {"advanced", "abstained"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
