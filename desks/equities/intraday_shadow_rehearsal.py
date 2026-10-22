"""Deterministic, synthetic end-to-end rehearsal for EQ-INTRADAY.

The function here is deliberately a fixture driver, not a collector or a
runtime.  It writes only beneath caller-supplied paths, creates an isolated
internal SQLite ledger, and labels every artifact/fill ``synthetic``.  A
successful run demonstrates integration behavior only; it says nothing about
provider entitlement, executable fills, or strategy merit.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from neorome.paper_ledger import MarketMark, PaperLedger, RiskStopActive

from .intraday_evidence_archive import archive_artifact, shadow_evidence
from .intraday_paper_adapter import EntryGateBlocked, IntradayPaperAdapter


UTC = timezone.utc
BASE = datetime(2026, 9, 21, 13, 45, tzinfo=UTC)
ORDER_LIFETIME = timedelta(seconds=1)


def _stamp(offset_seconds: float = 0) -> str:
    return (BASE + timedelta(seconds=offset_seconds)).isoformat().replace("+00:00", "Z")


def _artifacts() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Return frozen fixture artifacts with explicit synthetic provenance."""

    prefix_start = BASE.replace(hour=13, minute=30, second=0, microsecond=0)
    bars = [
        {"symbol": "SYNTH", "timestamp": (prefix_start + timedelta(minutes=index)).isoformat().replace("+00:00", "Z"),
         "open": 10.0, "high": 10.1, "low": 9.9, "close": 10.0, "volume": 1000}
        # The 09:45 New York signal may use only the completed 09:30–09:44
        # opening-range bars.  A 09:45 bar would still be forming.
        for index in range(15)
    ]
    universe = {
        "kind": "universe", "source": "synthetic-fixture", "source_version": "v1",
        "selection_at": _stamp(-1), "known_at": _stamp(-1),
        "payload": [{"permanent_id": "SYNTH-001", "symbol": "SYNTH"}],
    }
    bar_prefix = {
        "kind": "bar_prefix", "source": "synthetic-fixture", "source_version": "v1",
        "known_at": _stamp(0), "timeframe": "1Min", "adjustment": "raw", "payload": bars,
    }
    calendar = {
        "kind": "calendar", "source": "synthetic-fixture", "source_version": "v1",
        "known_at": _stamp(-1), "payload": {"session_close": _stamp(6 * 60 * 60 + 15 * 60)},
    }
    quotes = {
        "kind": "quotes", "source": "synthetic-fixture", "source_version": "v1",
        "known_at": _stamp(0.5), "size_units": "shares", "size_units_evidence": "synthetic-contract-v1",
        "payload": [{"symbol": "SYNTH", "timestamp": _stamp(0.25), "received_at": _stamp(0.5),
                     "bid": 9.9, "ask": 10.0, "bid_size": 10, "ask_size": 10}],
    }
    return universe, bar_prefix, calendar, quotes


def run_synthetic_rehearsal(
    evidence_root: str | Path,
    ledger_path: str | Path,
    *,
    outcome: str = "exit",
) -> dict[str, Any]:
    """Exercise reserve/restart/fill/mark/exit or bounded cancellation.

    ``outcome`` is either ``exit`` or ``cancel``.  The returned result is
    expressly non-executable and is safe to retain as engineering evidence.
    """

    if outcome not in {"exit", "cancel"}:
        raise ValueError("outcome must be exit or cancel")
    evidence_root = Path(evidence_root)
    ledger_path = Path(ledger_path)
    if ledger_path.parent.resolve() != evidence_root.resolve():
        raise ValueError("ledger_path must be isolated beneath the evidence root")
    universe, bars, calendar, quotes_artifact = _artifacts()
    archived = {
        "universe": archive_artifact(evidence_root, universe, signal_at=_stamp()),
        "bar_prefix": archive_artifact(evidence_root, bars, signal_at=_stamp()),
        "calendar": archive_artifact(evidence_root, calendar, signal_at=_stamp()),
        "quotes": archive_artifact(evidence_root, quotes_artifact),
    }
    evidence = shadow_evidence(universe, bars, calendar, quotes_artifact, signal_at=_stamp())
    candidate_id = f"synthetic-orb15-{outcome}"
    candidate = {
        "candidate_id": candidate_id, "strategy_id": "orb15", "symbol": "SYNTH",
        "signal_timestamp": _stamp(), "exit_decision_timestamp": _stamp(6 * 60 * 60 + 10 * 60),
        "session_close": _stamp(6 * 60 * 60 + 15 * 60), "universe_eligible": True,
        "prefix_complete_through_signal": True, "bar_quality_clear": True,
        "fractional_supported": False, "fee_cents_per_unit": 2, "units": 1, **evidence,
    }
    quotes = quotes_artifact["payload"]
    ledger = PaperLedger(ledger_path, clock=lambda: BASE)
    adapter = IntradayPaperAdapter(ledger)
    reservation = adapter.reserve(candidate, quotes, order_lifetime=ORDER_LIFETIME, now=_stamp(0.5))
    restarted = IntradayPaperAdapter(PaperLedger(ledger_path, clock=lambda: BASE))
    replay = restarted.reserve(candidate, quotes, order_lifetime=ORDER_LIFETIME, now=_stamp(0.5))
    if reservation != replay:
        raise AssertionError("restarted reservation replay was not idempotent")

    recovery_refused = False
    try:
        restarted.reserve({**candidate, "candidate_id": candidate_id + "-recovery"}, quotes,
                          order_lifetime=ORDER_LIFETIME, now=_stamp(0.5), recovery_ok=False)
    except EntryGateBlocked:
        recovery_refused = True
    ledger.set_risk_stop(True, reason="synthetic rehearsal STOP", at=_stamp(0.6))
    stop_refused = False
    try:
        restarted.reserve({**candidate, "candidate_id": candidate_id + "-stop"}, quotes,
                          order_lifetime=ORDER_LIFETIME, now=_stamp(0.6))
    except RiskStopActive:
        stop_refused = True
    ledger.set_risk_stop(False, reason="synthetic rehearsal cleanup", at=_stamp(0.61))

    result: dict[str, Any] = {
        "scope": "synthetic_engineering_rehearsal_only", "eligible_for_execution": False,
        "provenance": "synthetic", "candidate_id": candidate_id,
        "archived": archived, "reservation_id": reservation.reservation_id,
        "restart_reservation_id": replay.reservation_id, "recovery_refused": recovery_refused,
        "stop_refused": stop_refused,
    }
    if outcome == "cancel":
        cancelled = restarted.expire(reservation.reservation_id, now=_stamp(2))
        result.update({"status": "cancelled_before_fill", "cancelled_state": cancelled.state})
        return result

    fill = restarted.entry(reservation.reservation_id, quote_timestamp=_stamp(0.55), filled_at=_stamp(0.6))
    mark = MarketMark(fill.position_id, 990, _stamp(1), "d" * 64, "synthetic")
    restarted.mark(f"synthetic-mark-{candidate_id}", [mark], as_of=_stamp(1))
    close = restarted.exit(fill.position_id, proceeds_cents=990, fee_cents=2,
                           closed_at=candidate["exit_decision_timestamp"])
    result.update({"status": "closed_synthetic", "fill_id": fill.fill_id,
                   "position_id": fill.position_id, "close_id": close.close_id,
                   "realized_pnl_cents": close.realized_pnl_cents,
                   "planned_exit_at": candidate["exit_decision_timestamp"]})
    return result


__all__ = ["run_synthetic_rehearsal"]
