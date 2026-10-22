import json
import base64
import hashlib
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
import tempfile
import unittest

from neorome.equity_shared import EquitySharedError, LIFECYCLE_VERSION, position_mark, run
from neorome.equity_actions import _extract_rows, normalize_evidence
from neorome.paper_ledger import MarketMark, PaperLedger, Proposal


UTC = timezone.utc
NOW = datetime(2026, 9, 1, 14, 0, tzinfo=UTC)


def iso(value):
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def bundle(at=NOW, *, trend=True, source_hash="a" * 64, quote_age=5):
    start = date(2025, 4, 1)
    end = date(2026, 9, 18)
    sessions = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            opened = datetime.combine(current, time(13, 30), UTC)
            closed = datetime.combine(current, time(17 if current == date(2026, 9, 1) else 20), UTC)
            sessions.append({"date": current.isoformat(), "open_at": iso(opened), "close_at": iso(closed)})
        current += timedelta(days=1)
    prior = [row["date"] for row in sessions if row["date"] < at.date().isoformat()][-200:]
    closes = ["90"] * 200
    if trend:
        closes[-1] = "100"
    bars = [{"timestamp": f"{day}T04:00:00Z", "o": close, "h": close, "l": close, "c": close, "volume": "1000000"}
            for day, close in zip(prior, closes)]
    quote_time = at - timedelta(seconds=quote_age)
    context = {
        "source_kind": "observed", "received_at": iso(quote_time),
        "asset": {"symbol": "SPY", "asset_class": "us_equity", "status": "active", "tradable": True, "fractionable": True, "exchange": "ARCA"},
        "clock": {"timestamp": iso(quote_time), "is_open": True}, "sessions": sessions,
        "calendar_start": start.isoformat(), "calendar_end": end.isoformat(), "requests": 3, "order_mutations": 0,
    }
    observation = {
        "source_kind": "observed", "symbol": "SPY", "feed": "iex", "observed_at": iso(quote_time), "bars": bars,
        "quote": {"timestamp": iso(quote_time), "bid": "9", "ask": "10", "bid_size": "100", "ask_size": "100"},
        "evidence": [{"source_kind": "observed", "response_sha256": source_hash}], "request_count": 2,
    }
    return {
        "schema_version": 1, "source_kind": "observed", "context": context, "observation": observation,
        "order_mutations": 0, "broker_orders_sent": 0, "execution_enabled": False,
        "corporate_actions": corporate_evidence(at),
    }


def later_quote(at, source_hash="b" * 64, price="10"):
    return (
        {"symbol": "SPY", "timestamp": iso(at), "bid": price, "ask": price, "depth_units": "100"},
        {"source_kind": "observed", "sha256": source_hash},
    )


def corporate_evidence(at=NOW, *, ex_date="2025-01-01"):
    payload = {"corporate_actions": {"cash_dividends": [{
        "id": "action-1", "corporate_action_type": "cash_dividend", "symbol": "SPY",
        "status": "executed", "ex_date": ex_date, "process_date": "2025-01-15", "rate": "1.00",
    }]}}
    raw = json.dumps(payload, separators=(",", ":")).encode()
    result = {
        "payload": payload, "raw_response_b64": base64.b64encode(raw).decode(),
        "actions": _extract_rows(payload), "next_page_token": None,
        "query_start": (at.date() - timedelta(days=365)).isoformat(),
        "query_end": (at.date() + timedelta(days=365)).isoformat(),
        "requested_at": iso(at), "received_at": iso(at),
        "source_hash": hashlib.sha256(raw).hexdigest(),
    }
    return normalize_evidence(result, collected_at=at)


def mark_all(book, _now):
    marks = []
    for event in book.audit():
        if event.event_type != "fill":
            continue
        position = book.get_position(event.payload["position_id"])
        if position.state == "open":
            marks.append(MarketMark(position.position_id, 1000, _now, "f" * 64, position.provenance))
    return marks


class EquitySharedTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.ledger = self.root / "paper.sqlite3"
        self.state = self.root / "equity"

    def tearDown(self):
        self.temp.cleanup()

    def call(self, at, *, quote=None, daily=None, provider=mark_all):
        return run(daily or bundle(at, source_hash=(hex(int(at.timestamp()))[2:] + "0" * 64)[:64]), self.state,
                   ledger_path=self.ledger, later_quote=quote, now=at, mark_provider=provider)

    def test_delayed_fill_restart_and_mixed_foreign_marks(self):
        # Seed a BTC position so an entry cannot fabricate a foreign mark.
        book = PaperLedger(self.ledger, enforce_market_risk=True, clock=lambda: NOW)
        book.record_marks("empty", [], NOW)
        proposal = Proposal("btc-seed", "crypto", "seed", "v1", "1" * 64, NOW - timedelta(seconds=1), "BTC/USD", "crypto:BTC/USD", "btc-seed", NOW + timedelta(minutes=5), 1, 400, 0, "synthetic")
        reservation = book.reserve(proposal, now=NOW)
        book.simulate_fill(reservation.reservation_id, provenance="synthetic", filled_at=NOW)

        first = self.call(NOW)
        self.assertEqual([item["type"] for item in first["actions"]], ["reserve"])
        self.assertEqual(first["lifecycle_version"], LIFECYCLE_VERSION)
        self.assertTrue((self.state / "state.json").exists())
        waiting = self.call(NOW + timedelta(seconds=5), quote=later_quote(NOW + timedelta(seconds=5), "d" * 64))
        self.assertEqual(waiting["blocker"], "awaiting_independent_post_reservation_quote")
        filled = self.call(NOW + timedelta(seconds=11), quote=later_quote(NOW + timedelta(seconds=10), "e" * 64))
        self.assertEqual([item["type"] for item in filled["actions"]], ["simulated_fill"])
        restarted = self.call(NOW + timedelta(seconds=20), daily=bundle(NOW + timedelta(seconds=20), source_hash="1" * 64))
        self.assertEqual(restarted["actions"], [])
        state = json.loads((self.state / "state.json").read_text())
        self.assertEqual(state["fill_evidence"][0]["source_hash"], "e" * 64)
        self.assertIn("daily_bundle", state)
        self.assertEqual(state["lifecycle_version"], LIFECYCLE_VERSION)

    def test_position_mark_requires_own_identity_units_provenance_and_depth(self):
        self.call(NOW)
        self.call(NOW + timedelta(seconds=11), quote=later_quote(NOW + timedelta(seconds=10), "0" * 64))
        book = PaperLedger(self.ledger, enforce_market_risk=True, clock=lambda: NOW + timedelta(seconds=12))
        fill = next(event for event in book.audit() if event.event_type == "fill")
        position = book.get_position(fill.payload["position_id"])
        mark = position_mark(
            book, position, NOW + timedelta(seconds=12),
            {"symbol": "SPY", "timestamp": iso(NOW + timedelta(seconds=12)),
             "bid": "10", "ask": "10", "depth_units": "100"},
            {"source_kind": "observed", "sha256": "1" * 64},
            corporate_evidence(NOW + timedelta(seconds=12)),
        )
        self.assertEqual(mark.position_id, position.position_id)
        with self.assertRaisesRegex(EquitySharedError, "insufficient_mark_depth"):
            position_mark(
                book, position, NOW + timedelta(seconds=12),
                {"symbol": "SPY", "timestamp": iso(NOW + timedelta(seconds=12)),
                 "bid": "10", "ask": "10", "depth_units": "0.01"},
                {"source_kind": "observed", "sha256": "2" * 64},
                corporate_evidence(NOW + timedelta(seconds=12)),
            )

    def test_caps_stop_expiry_and_missing_or_stale_evidence(self):
        no_marks = run(bundle(NOW), self.root / "no-marks", ledger_path=self.root / "no-marks.sqlite3", now=NOW)
        self.assertEqual(no_marks["blocker"], "global_marks_missing")
        self.assertEqual(no_marks["actions"], [])

        missing = bundle(NOW)
        missing.pop("corporate_actions")
        result = self.call(NOW, daily=missing)
        self.assertIn("accounting_blocked", result["blocker"] or "")

        stale = bundle(NOW, quote_age=61, source_hash="2" * 64)
        stale_result = self.call(NOW, daily=stale)
        self.assertEqual(stale_result["blocker"], "no_entry")

        reserved = self.call(NOW)
        self.assertEqual(reserved["actions"][0]["type"], "reserve")
        (self.state / "STOP").write_text("stop")
        stopped = self.call(NOW + timedelta(seconds=1), daily=bundle(NOW + timedelta(seconds=1), source_hash="3" * 64))
        self.assertEqual(stopped["actions"][0]["type"], "cancel_stop")
        self.assertEqual(stopped["blocker"], "stop_active")

        # A second independent reservation near the 30-dollar group cap is
        # rejected by the shared ledger, preserving the fixed caps.
        (self.state / "STOP").unlink()
        cap_ledger = self.root / "cap.sqlite3"
        cap_state = self.root / "cap-state"
        book = PaperLedger(cap_ledger, enforce_market_risk=True, clock=lambda: NOW)
        book.record_marks("cap-empty", [], NOW)
        for index in (1, 2):
            proposal = Proposal(f"other-{index}", "equities", "other", "v1", f"{index}" * 64, NOW - timedelta(seconds=1), "OTHER", "us-broad-equity", f"other-{index}", NOW + timedelta(minutes=5), 1, 1000, 0, "synthetic")
            book.reserve(proposal, now=NOW)
        capped = run(bundle(NOW + timedelta(seconds=2), source_hash="4" * 64), cap_state, ledger_path=cap_ledger,
                     now=NOW + timedelta(seconds=2), mark_provider=mark_all)
        self.assertIn("exposure-group", capped["blocker"] or "")

    def test_expired_pending_is_cancelled_and_quote_age_blocks(self):
        first = self.call(NOW)
        self.assertEqual(first["actions"][0]["type"], "reserve")
        expired = self.call(NOW + timedelta(seconds=61), daily=bundle(NOW + timedelta(seconds=61), source_hash="5" * 64))
        self.assertEqual(expired["actions"][0]["type"], "cancel_expired_reservation")
        stale = self.call(NOW + timedelta(seconds=62), quote=later_quote(NOW - timedelta(seconds=1), "6" * 64), daily=bundle(NOW + timedelta(seconds=62), source_hash="7" * 64))
        self.assertIn(stale["blocker"], {"no_entry", "entry_already_attempted"})

    def test_signal_close_is_delayed_independent_restart_safe_and_risk_reducing(self):
        first = self.call(NOW)
        self.assertEqual(first["actions"][0]["type"], "reserve")
        filled = self.call(NOW + timedelta(seconds=11), quote=later_quote(NOW + timedelta(seconds=10), "8" * 64))
        self.assertEqual(filled["actions"][0]["type"], "simulated_fill")
        exit_bundle = bundle(NOW + timedelta(seconds=20), trend=False, source_hash="9" * 64)
        intent = run(exit_bundle, self.state, ledger_path=self.ledger,
                     later_quote=later_quote(NOW + timedelta(seconds=20), "a" * 64),
                     now=NOW + timedelta(seconds=20), mark_provider=None)
        self.assertEqual(intent["actions"][0]["type"], "pending_close")
        self.assertEqual(intent["blocker"], "awaiting_independent_exit_quote")

        # A restart sees the durable intent but cannot use a quote less than
        # ten seconds after it, even when its source is distinct.
        waiting = run(bundle(NOW + timedelta(seconds=25), trend=False, source_hash="b" * 64),
                      self.state, ledger_path=self.ledger,
                      later_quote=later_quote(NOW + timedelta(seconds=25), "c" * 64),
                      now=NOW + timedelta(seconds=25), mark_provider=None)
        self.assertEqual(waiting["actions"], [])
        self.assertEqual(waiting["blocker"], "awaiting_independent_exit_quote")

        exited = run(bundle(NOW + timedelta(seconds=31), trend=False, source_hash="d" * 64),
                     self.state, ledger_path=self.ledger,
                     later_quote=later_quote(NOW + timedelta(seconds=30), "e" * 64),
                     now=NOW + timedelta(seconds=31), mark_provider=None)
        self.assertEqual(exited["actions"][0]["type"], "simulated_close")
        self.assertEqual(exited["actions"][0]["global_marks_blocker"], "global_marks_missing")
        replay = run(bundle(NOW + timedelta(seconds=32), trend=False, source_hash="f" * 64),
                     self.state, ledger_path=self.ledger, now=NOW + timedelta(seconds=32), mark_provider=None)
        self.assertEqual(replay["actions"], [])

    def test_missing_daily_bundle_can_start_upcoming_action_exit_but_not_fill_it_immediately(self):
        self.call(NOW)
        self.call(NOW + timedelta(seconds=11), quote=later_quote(NOW + timedelta(seconds=10), "1" * 64))
        action_time = NOW + timedelta(seconds=20)
        evidence = corporate_evidence(action_time, ex_date="2026-09-03")
        intent = run({}, self.state, ledger_path=self.ledger,
                     corporate_actions=evidence,
                     later_quote=later_quote(action_time, "2" * 64),
                     now=action_time, mark_provider=None)
        self.assertEqual(intent["actions"][0]["type"], "pending_close")
        self.assertEqual(intent["actions"][0]["intent"]["trigger"], "corporate_action")
        closed = run({}, self.state, ledger_path=self.ledger,
                     corporate_actions=corporate_evidence(NOW + timedelta(seconds=31), ex_date="2026-09-03"),
                     later_quote=later_quote(NOW + timedelta(seconds=30), "3" * 64),
                     now=NOW + timedelta(seconds=31), mark_provider=None)
        self.assertEqual(closed["actions"][0]["type"], "simulated_close")

    def test_crossed_action_blocks_valuation_and_close_with_old_units(self):
        self.call(NOW)
        self.call(NOW + timedelta(seconds=11), quote=later_quote(NOW + timedelta(seconds=10), "4" * 64))
        crossed = datetime(2026, 9, 3, 14, 0, tzinfo=UTC)
        result = run(bundle(crossed, trend=False, source_hash="5" * 64), self.state,
                     ledger_path=self.ledger,
                     corporate_actions=corporate_evidence(crossed, ex_date="2026-09-03"),
                     later_quote=later_quote(crossed, "6" * 64), now=crossed, mark_provider=None)
        self.assertEqual(result["actions"], [])
        self.assertEqual(result["blocker"], "accounting_blocked: corporate_action_valuation_blocked")
        state = json.loads((self.state / "state.json").read_text())
        self.assertNotIn("pending_close_intent", state)

    def test_missing_bundle_still_expires_entry_and_stop_cancels_close_intent(self):
        self.call(NOW)
        expired = run({}, self.state, ledger_path=self.ledger,
                      corporate_actions=corporate_evidence(NOW + timedelta(seconds=61)),
                      now=NOW + timedelta(seconds=61), mark_provider=mark_all)
        self.assertEqual(expired["actions"][0]["type"], "cancel_expired_reservation")

        # A separate lifecycle demonstrates cancellation of a durable close
        # intent by the local STOP control.
        other_ledger = self.root / "stop.sqlite3"
        other_state = self.root / "stop-state"
        run(bundle(NOW), other_state, ledger_path=other_ledger, now=NOW, mark_provider=mark_all)
        run(bundle(NOW + timedelta(seconds=11)), other_state, ledger_path=other_ledger,
            later_quote=later_quote(NOW + timedelta(seconds=10), "7" * 64),
            now=NOW + timedelta(seconds=11), mark_provider=mark_all)
        run(bundle(NOW + timedelta(seconds=20), trend=False, source_hash="8" * 64), other_state,
            ledger_path=other_ledger, now=NOW + timedelta(seconds=20), mark_provider=mark_all)
        (other_state / "STOP").write_text("stop")
        stopped = run({}, other_state, ledger_path=other_ledger,
                      corporate_actions=corporate_evidence(NOW + timedelta(seconds=21)),
                      now=NOW + timedelta(seconds=21), mark_provider=None)
        self.assertEqual(stopped["actions"][0]["type"], "cancel_stop_close_intent")
        self.assertEqual(stopped["blocker"], "stop_active")

    def test_close_intent_expires_and_recovery_pause_cancels_it(self):
        for suffix, mode in (("expiry", "expiry"), ("recovery", "recovery")):
            ledger = self.root / f"{suffix}.sqlite3"
            state = self.root / f"{suffix}-state"
            run(bundle(NOW), state, ledger_path=ledger, now=NOW, mark_provider=mark_all)
            run(bundle(NOW + timedelta(seconds=11)), state, ledger_path=ledger,
                later_quote=later_quote(NOW + timedelta(seconds=10), ("9" if mode == "expiry" else "b") * 64),
                now=NOW + timedelta(seconds=11), mark_provider=mark_all)
            created = run(bundle(NOW + timedelta(seconds=20), trend=False, source_hash=("c" if mode == "expiry" else "d") * 64),
                          state, ledger_path=ledger, now=NOW + timedelta(seconds=20), mark_provider=mark_all)
            self.assertEqual(created["actions"][0]["type"], "pending_close")
            if mode == "expiry":
                result = run(bundle(NOW + timedelta(seconds=81), trend=False, source_hash="e" * 64),
                             state, ledger_path=ledger, now=NOW + timedelta(seconds=81), mark_provider=mark_all)
                self.assertEqual(result["actions"][0]["type"], "cancel_expired_close_intent")
                self.assertEqual(result["blocker"], "close_intent_expired")
            else:
                result = run({}, state, ledger_path=ledger,
                             corporate_actions=corporate_evidence(NOW + timedelta(seconds=21)),
                             now=NOW + timedelta(seconds=21), mark_provider=None, allow_entry=False)
                self.assertEqual(result["actions"][0]["type"], "cancel_entries_disabled_close_intent")
                self.assertEqual(result["blocker"], "entries_disabled")

    def test_known_action_within_seven_days_blocks_entry(self):
        daily = bundle(NOW, source_hash="a" * 64)
        daily["corporate_actions"] = corporate_evidence(NOW, ex_date="2026-09-03")
        result = self.call(NOW, daily=daily)
        self.assertEqual(result["blocker"], "corporate_action_entry_blocked")


if __name__ == "__main__":
    unittest.main()
