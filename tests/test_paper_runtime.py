from datetime import datetime, timedelta, timezone
import fcntl
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from neorome import paper_runtime
from neorome.paper_ledger import PaperLedger, Proposal
from neorome.paper_runtime import run_once


UTC = timezone.utc
NOW = datetime(2026, 9, 21, 12, tzinfo=UTC)


class RuntimeTests(unittest.TestCase):
    def test_daily_rollover_grace_is_narrow(self):
        missing = {"blocker": "PilotError: daily evidence file is missing"}
        self.assertTrue(paper_runtime._expected_result(missing, NOW.replace(hour=0, minute=4))[2])
        self.assertFalse(paper_runtime._expected_result(missing, NOW.replace(hour=0, minute=15))[2])
        self.assertFalse(paper_runtime._expected_result({"blocker": "PilotError: daily evidence file is unsafe"}, NOW.replace(hour=0, minute=4))[2])

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.runtime = root / "runtime"
        self.pilot = root / "pilot"
        self.ledger_path = root / "paper.sqlite3"
        self.recovery = root / "recovery.json"
        PaperLedger(self.ledger_path, enforce_market_risk=True, clock=lambda: NOW)
        self.calls = []
        self.quote = {"symbol": "BTC/USD", "bid": 100, "ask": 101, "bid_depth": 100, "ask_depth": 100,
                      "quote_time": NOW.isoformat().replace("+00:00", "Z")}
        self.source = {"sha256": "a" * 64, "source_kind": "observed"}

    def tearDown(self):
        self.tmp.cleanup()

    def runner(self, **kwargs):
        self.calls.append(kwargs)
        return {"actions": [], "blocker": None, "market_snapshot": {"as_of": kwargs["now"].isoformat(), "market_nav_cents": 80000}}

    def invoke(self, **kwargs):
        kwargs.setdefault("pilot_runner", self.runner)
        kwargs.setdefault("pilot_state", self.pilot)
        kwargs.setdefault("require_recovery", False)
        return run_once(self.runtime, ledger_path=self.ledger_path, now=NOW, quote=self.quote,
                        quote_source=self.source, **kwargs)

    def test_equity_cycle_runs_under_shared_lock_and_before_crypto(self):
        events=[]
        def cycle(now,allow_entry):
            with (self.runtime/'runtime.lock').open('a+') as other:
                with self.assertRaises(BlockingIOError):
                    fcntl.flock(other,fcntl.LOCK_EX|fcntl.LOCK_NB)
            events.append(('equity',allow_entry))
            return {'operational_healthy':True,'desk_status':'active'}
        def runner(**kwargs):
            events.append(('crypto',True))
            self.assertIs(kwargs['mark_provider'], provider)
            return {'actions':[], 'blocker':None}
        provider=lambda book,now:[]
        result=self.invoke(equity_cycle=cycle,mark_provider=provider,pilot_runner=runner)
        self.assertEqual(events,[('equity',True),('crypto',True)])
        self.assertEqual(result['desk_status']['equities'],'active')

    def test_live_clock_refreshes_quote_validation_after_slow_equity_cycle(self):
        times = [NOW, NOW + timedelta(seconds=70), NOW + timedelta(seconds=120)]
        validated_at = []
        def quote_at(_state, current, _quote, _source):
            validated_at.append(current)
            return self.source['sha256'], self.quote, self.source, None
        with patch.object(paper_runtime, '_now', autospec=True, side_effect=times) as clock, \
             patch.object(paper_runtime, '_quote_hash', side_effect=quote_at):
            result = run_once(
                self.runtime, ledger_path=self.ledger_path, pilot_state=self.pilot,
                now=None, require_recovery=False,
                equity_cycle=lambda current, allowed: {'operational_healthy': True},
                pilot_runner=self.runner,
            )
        self.assertEqual(validated_at, times[1:])
        self.assertEqual(clock.call_args_list, [unittest.mock.call(None)] * 3)
        self.assertEqual(self.calls[0]['now'], times[2])
        self.assertEqual(result['updated_at'], times[2].isoformat().replace('+00:00', 'Z'))

    def test_equity_failure_remains_unhealthy_even_when_crypto_succeeds(self):
        result=self.invoke(equity_cycle=lambda now,allowed:{'operational_healthy':False})
        self.assertFalse(result['operational_healthy'])
        self.assertEqual(result['operational_failure'],'equity_cycle_failed')
        self.assertIsNone(result.get('last_success'))

    def test_foreign_integrity_error_is_not_hidden_by_crypto_exit_target(self):
        result=self.invoke(pilot_runner=lambda **kwargs:{'actions':[], 'blocker':'exit_target_pending',
                            'global_mark_error':'MarkIntegrityError: checksum mismatch'})
        self.assertFalse(result['operational_healthy'])

    def test_missing_ledger_is_never_created(self):
        missing = Path(self.tmp.name) / "missing.sqlite3"
        result = None
        with self.assertRaises(ValueError):
            run_once(self.runtime, ledger_path=missing, now=NOW, require_recovery=False,
                     pilot_runner=self.runner)
        self.assertFalse(missing.exists())
        empty = Path(self.tmp.name) / "empty.sqlite3"
        empty.touch()
        with self.assertRaises(ValueError):
            run_once(self.runtime, ledger_path=empty, now=NOW, require_recovery=False,
                     pilot_runner=self.runner)
        self.assertEqual(empty.stat().st_size, 0)

    def test_one_decision_per_quote_hash_and_snapshot_persisted(self):
        first = self.invoke()
        second = self.invoke()
        self.assertEqual(first["status"], "success")
        self.assertEqual(second["status"], "already_processed")
        self.assertEqual(len(self.calls), 1)
        status = json.loads((self.runtime / "status.json").read_text())
        self.assertTrue(status["operational_healthy"])
        self.assertEqual(status["market_snapshot"]["market_nav_cents"], 80000)
        self.assertEqual(status["desk_status"]["options"], "blocked_freshness_and_lifecycle")

        newer = dict(self.source, sha256="b" * 64)
        changed = run_once(self.runtime, ledger_path=self.ledger_path, now=NOW + timedelta(seconds=1),
                           quote=self.quote, quote_source=newer, pilot_runner=self.runner,
                           pilot_state=self.pilot, require_recovery=False)
        self.assertEqual(changed["status"], "success")
        self.assertEqual(len(self.calls), 2)

    def test_expired_pending_is_processed_without_quote(self):
        book = PaperLedger(self.ledger_path, enforce_market_risk=True, clock=lambda: NOW)
        book.record_marks("seed", [], NOW)
        proposal = Proposal("pending", "crypto", "fixture", "1", "c" * 64, NOW,
                            "BTC/USD", "CR-001:BTC/USD", "CR-001:BTC/USD",
                            NOW + timedelta(seconds=5), 1, 100, provenance="observed",
                            metadata={"crypto_pilot_v1": {"idea_id": "CR-001:BTC/USD"}})
        book.reserve(proposal, now=NOW)
        result = run_once(self.runtime, ledger_path=self.ledger_path, now=NOW + timedelta(seconds=10),
                          pilot_runner=self.runner, pilot_state=self.pilot, require_recovery=False)
        self.assertEqual(result["status"], "success")
        self.assertEqual(len(self.calls), 1)
        self.assertIsNone(self.calls[0]["quote"])

    def test_quote_reader_failure_still_cancels_expired_real_reservation(self):
        book = PaperLedger(self.ledger_path, enforce_market_risk=True, clock=lambda: NOW)
        book.record_marks("seed", [], NOW)
        proposal = Proposal("pending-real", "crypto", "fixture", "1", "d" * 64, NOW,
                            "BTC/USD", "CR-001:BTC/USD", "CR-001:BTC/USD",
                            NOW + timedelta(seconds=5), 1, 100, provenance="observed",
                            metadata={"crypto_pilot_v1": {"idea_id": "CR-001:BTC/USD"}})
        reservation = book.reserve(proposal, now=NOW)
        with patch("neorome.paper_runtime._quote_hash", side_effect=RuntimeError("recorder read failed")):
            result = run_once(self.runtime, ledger_path=self.ledger_path, now=NOW + timedelta(seconds=10),
                              pilot_runner=paper_runtime.pilot_run, pilot_state=self.pilot,
                              require_recovery=False)
        self.assertEqual(result["status"], "operational_failure")
        self.assertFalse(result["operational_healthy"])
        refreshed = PaperLedger(self.ledger_path, enforce_market_risk=True, clock=lambda: NOW)
        self.assertEqual(refreshed.get_reservation(reservation.reservation_id).state, "cancelled")

    def test_stop_blocks_entries_but_allows_maintenance(self):
        self.runtime.mkdir(parents=True)
        (self.runtime / "STOP").write_text("maintenance\n")
        result = self.invoke()
        self.assertEqual(result["status"], "stopped")
        self.assertEqual(self.calls, [])

    def test_dangling_stop_symlink_fails_closed(self):
        self.runtime.mkdir(parents=True)
        (self.runtime / "STOP").symlink_to("missing-stop")
        result = self.invoke()
        self.assertEqual(result["status"], "operational_failure")
        self.assertFalse(result["operational_healthy"])

    def test_pilot_already_running_claim_is_retried(self):
        responses = iter([
            {"actions": [], "blocker": "already_running"},
            {"actions": [], "blocker": None},
        ])
        def busy_then_success(**kwargs):
            self.calls.append(kwargs)
            return next(responses)
        first = self.invoke(pilot_runner=busy_then_success)
        self.assertEqual(first["status"], "already_running")
        self.assertIsNone(first["last_success"])
        second = self.invoke(pilot_runner=busy_then_success)
        self.assertEqual(second["status"], "success")
        self.assertEqual(len(self.calls), 2)

    def test_recovery_gate_does_not_hide_pilot_error(self):
        self.recovery.write_text(json.dumps({"updated_at": (NOW - timedelta(hours=3)).isoformat(), "operational_healthy": True}))
        def broken(**kwargs):
            return {"actions": [], "blocker": "PilotError: entry payload malformed"}
        result = self.invoke(pilot_runner=broken, recovery_status_path=self.recovery, require_recovery=True)
        self.assertEqual(result["status"], "operational_failure")
        self.assertFalse(result["operational_healthy"])

    def test_recovery_health_gates_entries_without_operational_failure(self):
        self.recovery.write_text(json.dumps({"updated_at": (NOW - timedelta(hours=3)).isoformat(), "operational_healthy": True}))
        result = self.invoke(recovery_status_path=self.recovery, require_recovery=True)
        self.assertEqual(result["status"], "abstain")
        self.assertTrue(result["operational_healthy"])
        self.assertEqual(result["last_cycle"]["blocker"], "recovery_unhealthy")
        self.assertFalse(self.calls[0]["allow_entry"])

    def test_runner_failure_is_operational_failure(self):
        def broken(**kwargs):
            raise RuntimeError("boom")
        result = self.invoke(pilot_runner=broken)
        self.assertEqual(result["status"], "operational_failure")
        self.assertFalse(result["operational_healthy"])
        retry = self.invoke(pilot_runner=broken)
        self.assertEqual(retry["status"], "operational_failure")
        self.assertEqual(len(self.calls), 0)
        status = json.loads((self.runtime / "status.json").read_text())
        self.assertEqual(status["last_claim"]["status"], "failed")

    def test_missing_or_stale_quote_is_expected_healthy_abstain(self):
        missing = run_once(self.runtime, ledger_path=self.ledger_path, now=NOW,
                           pilot_runner=self.runner, pilot_state=self.pilot,
                           require_recovery=False)
        self.assertEqual(missing["status"], "abstain")
        self.assertTrue(missing["operational_healthy"])

        def stale(**kwargs):
            return {"actions": [], "blocker": "quote_stale"}
        stale_result = run_once(self.runtime, ledger_path=self.ledger_path, now=NOW + timedelta(seconds=1),
                                quote=self.quote, quote_source=self.source, pilot_runner=stale,
                                pilot_state=self.pilot, require_recovery=False)
        self.assertEqual(stale_result["status"], "price_blocked")
        self.assertTrue(stale_result["operational_healthy"])

    def test_storage_cap_fails_closed(self):
        result = self.invoke(max_storage_bytes=1)
        self.assertEqual(result["status"], "storage_limit")
        self.assertFalse(result["operational_healthy"])
        self.assertEqual(self.calls, [])

    def test_lock_contention_does_not_overwrite_status(self):
        self.runtime.mkdir(parents=True)
        lock_path = self.runtime / "runtime.lock"
        with lock_path.open("a+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = self.invoke()
        self.assertEqual(result["status"], "already_running")
        self.assertFalse((self.runtime / "status.json").exists())


if __name__ == "__main__":
    unittest.main()
