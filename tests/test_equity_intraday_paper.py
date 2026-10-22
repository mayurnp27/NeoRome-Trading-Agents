from datetime import datetime, timedelta, timezone
import fcntl
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from desks.equities.intraday_shadow_driver import run_once as shadow_once
from neorome.equity_intraday_paper import IntradayRuntimeError, LOCK_NAME, run_cycle
from neorome.paper_ledger import PaperLedger


UTC = timezone.utc
SIGNAL = datetime(2026, 9, 21, 13, 45, tzinfo=UTC)


def iso(seconds=0):
    return (SIGNAL + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


def provider(kind, _request):
    common = {"source": "documented-sip-provider", "source_version": "2026-09-schema"}
    return {
        "universe": {**common, "selection_at": iso(-2), "payload": [{"permanent_id": "id-1", "symbol": "SYNTH"}]},
        "calendar": {**common, "payload": {"session_close": iso(6 * 60 * 60 + 15 * 60)}},
        "bar_prefix": {**common, "timeframe": "1Min", "adjustment": "raw", "payload": [{"timestamp": iso(-60), "close": 10.0}]},
        "quotes": {**common, "size_units": "shares", "size_units_documentation": "provider-spec-v2026.09#quote-size",
                   "payload": [{"symbol": "SYNTH", "timestamp": iso(.25), "bid": 9.9, "ask": 10.0, "bid_size": 10, "ask_size": 10}]},
    }[kind]


def decision(_inputs):
    return {"action": "candidate", "candidate": {
        "candidate_id": "observed-orb15-synth", "strategy_id": "orb15", "symbol": "SYNTH",
        "signal_timestamp": iso(), "exit_decision_timestamp": iso(6 * 60 * 60 + 10 * 60),
        "session_close": iso(6 * 60 * 60 + 15 * 60), "universe_eligible": True,
        "prefix_complete_through_signal": True, "bar_quality_clear": True,
        "fractional_supported": False, "fee_cents_per_unit": 2, "units": 1,
    }}


class EquityIntradayPaperTests(unittest.TestCase):
    def _record(self, root):
        receipts = iter([SIGNAL - timedelta(seconds=1), SIGNAL - timedelta(seconds=.5), SIGNAL, SIGNAL + timedelta(seconds=.5)])
        result = shadow_once(root, provider, decision, signal_at=SIGNAL, symbol="SYNTH",
                             order_lifetime=timedelta(seconds=1), clock=lambda: next(receipts))
        self.assertEqual(result["status"], "eligible_unfilled_candidate")
        return result

    def quote(self, seconds, *, bid=9.9, ask=10.0):
        return {"symbol": "SYNTH", "timestamp": iso(seconds), "received_at": iso(seconds),
                "bid": bid, "ask": ask, "bid_size": 10, "ask_size": 10, "source_sha256": "f" * 64}

    def paths(self, root):
        state, ledger = Path(root) / "state", Path(root) / "paper.sqlite3"
        PaperLedger(ledger, enforce_market_risk=True, clock=lambda: SIGNAL)
        return state, ledger

    def test_no_evidence_is_an_honest_no_mutation_abstention(self):
        result = run_cycle("/tmp/unused-intraday-state", "/tmp/unused-intraday-ledger", None, now=SIGNAL)
        self.assertEqual(result["blocker"], "prospective_evidence_unavailable")
        self.assertEqual(result["broker_orders_sent"], 0)

    def test_observed_reserve_fill_exit_is_restart_safe_and_never_sends_broker_order(self):
        with TemporaryDirectory() as root:
            record = self._record(root)
            state, ledger = self.paths(root)
            reserved = run_cycle(state, ledger, record, now=SIGNAL + timedelta(seconds=.5))
            self.assertEqual(reserved["actions"][0]["type"], "reserve_observed_internal_paper")
            restarted = run_cycle(state, ledger, record, entry_fill_quote=self.quote(.75), now=SIGNAL + timedelta(seconds=.8))
            self.assertEqual(restarted["actions"][0]["type"], "simulated_observed_entry")
            exited = run_cycle(state, ledger, record, exit_quote=self.quote(6 * 60 * 60 + 10 * 60 + .25, bid=9.8, ask=9.9),
                               now=SIGNAL + timedelta(seconds=6 * 60 * 60 + 10 * 60 + .5), exit_fee_cents_per_unit=2)
            self.assertEqual(exited["actions"][0]["type"], "simulated_observed_exit")
            replay = run_cycle(state, ledger, record, now=SIGNAL + timedelta(seconds=6 * 60 * 60 + 10 * 60 + 1))
            self.assertEqual(replay["blocker"], "already_closed")
            self.assertEqual(exited["broker_orders_sent"], 0)

    def test_expiry_and_missing_post_reservation_evidence_are_abstentions(self):
        with TemporaryDirectory() as root:
            record = self._record(root)
            state, ledger = self.paths(root)
            run_cycle(state, ledger, record, now=SIGNAL + timedelta(seconds=.5))
            waiting = run_cycle(state, ledger, record, now=SIGNAL + timedelta(seconds=.75))
            self.assertEqual(waiting["blocker"], "awaiting_independent_post_reservation_quote")
            expired = run_cycle(state, ledger, record, now=SIGNAL + timedelta(seconds=1.3))
            self.assertEqual(expired["actions"][0]["type"], "cancel_expired_entry")

    def test_missing_ledger_is_refused_without_creating_a_fresh_book(self):
        with TemporaryDirectory() as root:
            record = self._record(root)
            ledger = Path(root) / "typo.sqlite3"
            with self.assertRaisesRegex(IntradayRuntimeError, "existing regular file"):
                run_cycle(Path(root) / "state", ledger, record, now=SIGNAL)
            self.assertFalse(ledger.exists())

    def test_nonledger_and_symlink_ledger_paths_are_refused(self):
        with TemporaryDirectory() as root:
            record = self._record(root)
            state = Path(root) / "state"
            nonledger = Path(root) / "not-a-ledger.sqlite3"
            nonledger.write_text("not sqlite")
            with self.assertRaisesRegex(IntradayRuntimeError, "unreadable|marker"):
                run_cycle(state, nonledger, record, now=SIGNAL)
            link = Path(root) / "ledger-link.sqlite3"
            link.symlink_to(nonledger)
            with self.assertRaisesRegex(IntradayRuntimeError, "existing regular file"):
                run_cycle(state, link, record, now=SIGNAL)

    def test_pending_entry_cancels_on_entry_gate_and_restart_is_idempotent(self):
        with TemporaryDirectory() as root:
            record = self._record(root)
            state, ledger = self.paths(root)
            run_cycle(state, ledger, record, now=SIGNAL + timedelta(seconds=.5))
            stopped = run_cycle(state, ledger, record, now=SIGNAL + timedelta(seconds=.75), allow_entry=False)
            self.assertEqual(stopped["actions"][0]["type"], "cancel_pending_entry")
            self.assertEqual(stopped["actions"][0]["reason"], "entries_disabled")
            replay = run_cycle(state, ledger, record, now=SIGNAL + timedelta(seconds=.8), allow_entry=False)
            self.assertEqual(replay["actions"], [])
            self.assertEqual(replay["blocker"], "entry_cancelled")

    def test_runtime_lock_refuses_concurrent_lifecycle_advance(self):
        with TemporaryDirectory() as root:
            record = self._record(root)
            state, ledger = self.paths(root)
            state.mkdir()
            lock = state / LOCK_NAME
            descriptor = os.open(lock, os.O_RDWR | os.O_CREAT, 0o600)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                result = run_cycle(state, ledger, record, now=SIGNAL + timedelta(seconds=.5))
            finally:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
                os.close(descriptor)
            self.assertEqual(result["status"], "already_running")
            self.assertEqual(result["broker_orders_sent"], 0)

    def test_decision_hash_or_archive_tampering_is_refused_before_ledger_open(self):
        with TemporaryDirectory() as root:
            record = self._record(root)
            record["candidate"]["quote_evidence_sha256"] = "0" * 64
            with self.assertRaisesRegex(Exception, "differs from collected"):
                run_cycle(Path(root) / "state", Path(root) / "paper.sqlite3", record, now=SIGNAL)


if __name__ == "__main__":
    unittest.main()
