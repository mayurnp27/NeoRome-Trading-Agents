from __future__ import annotations

from copy import deepcopy
from datetime import timedelta
import fcntl
import tempfile
import unittest
from pathlib import Path

from neorome.options_internal_paper import (
    FULL_PORT_BALANCES, FULL_PORT_LIMITS, SHARED_BALANCES, OptionsInternalPaperError,
    _digest, _ensure_initial_market_mark, _worker_exit_code, proposal_from_candidate, run_once,
)
from neorome.options_observed_shadow import run_observed_shadow
from neorome.paper_ledger import PaperLedger
from tests.test_options_observed_shadow import NOW, _evidence


def cloud_for(decision, at):
    selected = decision["shadow_candidate"]
    rows = []
    for leg in (selected["long"], selected["short"]):
        row = deepcopy(leg)
        row["timestamp"] = at.isoformat()
        rows.append(row)
    receipt = at + timedelta(seconds=1)
    body = {
        "schema_version": 1, "artifact_kind": "options-cloud-evidence-v1", "status": "captured",
        "provider": "robinhood-trading", "chain_complete": True, "broker_write_called": False,
        "local_receipt_at": receipt.isoformat(), "normalized_quotes": rows,
        "holdings": {"complete": True, "open_orders_reconciled": True, "pending_reconciliation": False,
                     "option_holdings": {}, "underlying_quantity": 0, "local_receipt_at": receipt.isoformat()},
        "orders_sent": 0,
    }
    return {**body, "artifact_sha256": _digest(body)}


def initialized_ledger(path):
    return PaperLedger(path, balances=SHARED_BALANCES, enforce_market_risk=True,
                       clock=lambda: NOW)


class OptionsInternalPaperTests(unittest.TestCase):
    def test_blocked_valuation_triggers_service_failure_hook(self):
        self.assertEqual(_worker_exit_code({"status": "holding_internal_paper", "mark_status": "blocked_evidence"}), 1)
        self.assertEqual(_worker_exit_code({"status": "filled_internal_paper", "mark_status": "shared_mark_coverage_required"}), 1)
        self.assertEqual(_worker_exit_code({"status": "holding_internal_paper", "mark_status": "persisted"}), 0)

    def test_invalid_liquidation_mark_does_not_hide_valid_exit_evidence(self):
        decision = run_observed_shadow(_evidence(), as_of=NOW)
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);state=root/'state';book=root/'paper.sqlite3'
            initialized_ledger(book)
            run_once(candidate=decision,state_dir=state,ledger_path=book,now=NOW)
            run_once(candidate=None,entry_cloud=cloud_for(decision,NOW+timedelta(seconds=12)),
                     state_dir=state,ledger_path=book,now=NOW+timedelta(seconds=14))
            bad=cloud_for(decision,NOW+timedelta(seconds=30))
            long_symbol=decision['shadow_candidate']['long']['symbol']
            for row in bad['normalized_quotes']:
                row.update(bid='0.01' if row['symbol']==long_symbol else '4.00',
                           ask='0.02' if row['symbol']==long_symbol else '4.01')
            bad['artifact_sha256']=_digest({k:v for k,v in bad.items()if k!='artifact_sha256'})
            result=run_once(candidate=None,mark_cloud=bad,exit_cloud=cloud_for(decision,NOW+timedelta(seconds=50)),
                            state_dir=state,ledger_path=book,now=NOW+timedelta(seconds=54),review_only=True)
            self.assertIn('position review blocked',result['evidence_blockers']['mark'])
            self.assertEqual(result['mark_status'],'persisted')
            self.assertIn('lifecycle',result)

    def test_review_only_does_not_enter_or_fill_pending(self):
        decision = run_observed_shadow(_evidence(), as_of=NOW)
        for pending in (False, True):
            with self.subTest(pending=pending), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                ledger = initialized_ledger(root / "paper.sqlite3")
                if pending:
                    run_once(candidate=decision, state_dir=root / "state", ledger_path=ledger.path, now=NOW)
                before = len(ledger.audit())
                result = run_once(candidate=decision, entry_cloud=cloud_for(decision, NOW + timedelta(seconds=12)),
                                  state_dir=root / "state", ledger_path=root / "paper.sqlite3",
                                  now=NOW + timedelta(seconds=14), review_only=True)
                self.assertEqual(result["reason"], "review_only_no_position")
                self.assertEqual(len(ledger.audit()), before)

    def test_prior_day_flat_empty_mark_is_refreshed_without_erasing_history(self):
        with tempfile.TemporaryDirectory() as directory:
            book = initialized_ledger(Path(directory) / "paper.sqlite3")
            old = NOW - timedelta(days=1)
            book.record_marks("prior-day-cash", [], old)
            high_water = book.market_snapshot(NOW).high_water_cents

            _ensure_initial_market_mark(book, NOW)

            marks = [event for event in book.audit() if event.event_type == "market_marks"]
            self.assertEqual([event.entity_id for event in marks][0], "prior-day-cash")
            self.assertEqual(len(marks), 2)
            self.assertTrue(book.market_snapshot(NOW).coverage_current)
            self.assertEqual(book.market_snapshot(NOW).high_water_cents, high_water)

    def test_stale_same_day_flat_empty_mark_is_refreshed(self):
        with tempfile.TemporaryDirectory() as directory:
            book = initialized_ledger(Path(directory) / "paper.sqlite3")
            book.record_marks("stale-cash", [], NOW - timedelta(seconds=301))

            _ensure_initial_market_mark(book, NOW)

            marks = [event for event in book.audit() if event.event_type == "market_marks"]
            self.assertEqual(len(marks), 2)
            self.assertTrue(book.market_snapshot(NOW).coverage_current)

    def test_stale_empty_mark_is_not_fabricated_while_pending_or_open(self):
        decision = run_observed_shadow(_evidence(), as_of=NOW)
        for open_position in (False, True):
            with self.subTest(open_position=open_position), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                ledger_path = root / "paper.sqlite3"
                initialized_ledger(ledger_path)
                reserved = run_once(candidate=decision, state_dir=root / "state", ledger_path=ledger_path, now=NOW)
                self.assertEqual(reserved["status"], "reserved")
                if open_position:
                    filled = run_once(candidate=None, entry_cloud=cloud_for(decision, NOW + timedelta(seconds=12)),
                                      state_dir=root / "state", ledger_path=ledger_path,
                                      now=NOW + timedelta(seconds=14))
                    self.assertEqual(filled["status"], "filled_internal_paper")
                book = PaperLedger(ledger_path, balances=SHARED_BALANCES, enforce_market_risk=True,
                                   clock=lambda: NOW + timedelta(seconds=400))
                before = len([event for event in book.audit() if event.event_type == "market_marks"])

                _ensure_initial_market_mark(book, NOW + timedelta(seconds=400))

                after = len([event for event in book.audit() if event.event_type == "market_marks"])
                self.assertEqual(after, before)
                self.assertGreater(book.active_exposure_count("options"), 0)

    def test_candidate_chronology_is_still_rejected_after_flat_mark_refresh(self):
        decision = run_observed_shadow(_evidence(), as_of=NOW)
        decision["shadow_candidate"]["long"]["timestamp"] = (NOW + timedelta(seconds=1)).isoformat()
        decision["artifact_sha256"] = _digest({key: value for key, value in decision.items() if key != "artifact_sha256"})
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            book = initialized_ledger(root / "paper.sqlite3")
            book.record_marks("prior-day-cash", [], NOW - timedelta(days=1))

            with self.assertRaisesRegex(OptionsInternalPaperError, "chronology is invalid"):
                _ensure_initial_market_mark(book, NOW)
                proposal_from_candidate(decision, now=NOW)

            self.assertTrue(book.market_snapshot(NOW).coverage_current)

    def test_worker_treats_intentional_cancellation_and_abstention_as_healthy(self):
        for status in ("abstain", "cancelled", "reserved", "filled_internal_paper", "holding_internal_paper"):
            self.assertEqual(_worker_exit_code({"status": status}), 0)
        self.assertEqual(_worker_exit_code({"status": "error"}), 1)
        self.assertEqual(_worker_exit_code({}), 1)

    def test_later_complete_observed_evidence_creates_only_a_modeled_fill_and_persists_mark(self):
        decision = run_observed_shadow(_evidence(), as_of=NOW)
        entry = cloud_for(decision, NOW + timedelta(seconds=12))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            initialized_ledger(root / "paper.sqlite3")
            reserved = run_once(candidate=decision, state_dir=root / "state",
                                ledger_path=root / "paper.sqlite3", now=NOW)
            self.assertEqual(reserved["status"], "reserved")
            first = run_once(candidate=None, entry_cloud=entry, state_dir=root / "state",
                             ledger_path=root / "paper.sqlite3", now=NOW + timedelta(seconds=14))
            self.assertEqual(first["status"], "filled_internal_paper")
            self.assertTrue(first["modeled_fill"])
            self.assertEqual(first["orders_sent"], 0)
            self.assertFalse(first["broker_order_sent"])
            self.assertTrue((root / "state" / "state.json").exists())

            mark = cloud_for(decision, NOW + timedelta(seconds=30))
            second = run_once(candidate=None, mark_cloud=mark, state_dir=root / "state",
                              ledger_path=root / "paper.sqlite3", now=NOW + timedelta(seconds=32))
            self.assertEqual(second["mark_status"], "persisted")
            book = PaperLedger(root / "paper.sqlite3", enforce_market_risk=True, clock=lambda: NOW + timedelta(seconds=32))
            self.assertTrue(book.market_snapshot().coverage_current)

    def test_late_paper_review_uses_current_ledger_holdings_not_entry_timestamp(self):
        decision = run_observed_shadow(_evidence(), as_of=NOW)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); state = root / "state"; path = root / "paper.sqlite3"
            initialized_ledger(path)
            run_once(candidate=decision, state_dir=state, ledger_path=path, now=NOW)
            run_once(candidate=None, entry_cloud=cloud_for(decision, NOW + timedelta(seconds=12)),
                     state_dir=state, ledger_path=path, now=NOW + timedelta(seconds=14))
            later = NOW + timedelta(minutes=10)
            cloud = cloud_for(decision, later)
            original_cloud = deepcopy(cloud)
            result = run_once(candidate=None, mark_cloud=cloud, state_dir=state, ledger_path=path,
                              now=later + timedelta(seconds=2))
            self.assertEqual(result["mark_status"], "persisted")
            self.assertEqual(cloud, original_cloud)
            self.assertEqual(result["orders_sent"], 0)

    def test_stale_mark_does_not_hide_fresh_exit_review(self):
        decision = run_observed_shadow(_evidence(), as_of=NOW)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); state = root / "state"; path = root / "paper.sqlite3"
            initialized_ledger(path)
            run_once(candidate=decision, state_dir=state, ledger_path=path, now=NOW)
            run_once(candidate=None, entry_cloud=cloud_for(decision, NOW + timedelta(seconds=12)),
                     state_dir=state, ledger_path=path, now=NOW + timedelta(seconds=14))
            later = NOW + timedelta(minutes=10)
            result = run_once(candidate=None, mark_cloud=cloud_for(decision, NOW + timedelta(seconds=30)),
                              exit_cloud=cloud_for(decision, later), state_dir=state,
                              ledger_path=path, now=later + timedelta(seconds=2))
            self.assertEqual(result["mark_status"], "persisted")
            self.assertIn("mark", result["evidence_blockers"])
            self.assertEqual(result["lifecycle"]["last_review"]["review"]["status"], "reviewed")
            self.assertEqual(result["lifecycle"]["status"], "exit_intent")
            self.assertNotIn("mark_blocker", result)

    def test_late_review_still_rejects_stale_provider_quotes(self):
        decision = run_observed_shadow(_evidence(), as_of=NOW)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); state = root / "state"; path = root / "paper.sqlite3"
            initialized_ledger(path)
            run_once(candidate=decision, state_dir=state, ledger_path=path, now=NOW)
            run_once(candidate=None, entry_cloud=cloud_for(decision, NOW + timedelta(seconds=12)),
                     state_dir=state, ledger_path=path, now=NOW + timedelta(seconds=14))
            later = NOW + timedelta(minutes=10)
            cloud = cloud_for(decision, later)
            for row in cloud["normalized_quotes"]:
                row["timestamp"] = (later - timedelta(minutes=2)).isoformat()
            cloud["artifact_sha256"] = _digest({k: v for k, v in cloud.items() if k != "artifact_sha256"})
            result = run_once(candidate=None, exit_cloud=cloud, state_dir=state, ledger_path=path,
                              now=later + timedelta(seconds=2))
            self.assertEqual(result["mark_status"], "blocked_evidence")
            self.assertIn("quote is stale", result["mark_blocker"])
            self.assertNotIn("lifecycle", result)

    def test_full_port_profile_uses_an_isolated_300_dollar_ledger_and_one_active_position(self):
        decision = run_observed_shadow(_evidence(), as_of=NOW, max_entry_cost="300",
                                       paper_profile="options_full_port_v1")
        self.assertEqual(decision["status"], "eligible_shadow_candidate")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); ledger_path = root / "full-port.sqlite3"
            PaperLedger(ledger_path, balances=FULL_PORT_BALANCES, limits=FULL_PORT_LIMITS,
                        enforce_market_risk=True, clock=lambda: NOW)
            reserved = run_once(candidate=decision, state_dir=root / "state", ledger_path=ledger_path,
                                now=NOW, full_port=True)
            self.assertEqual(reserved["status"], "reserved")
            self.assertLessEqual(reserved["reservation"]["reserved_cents"], 30_000)
            other = run_once(candidate=decision, state_dir=root / "other-state", ledger_path=ledger_path,
                             now=NOW, full_port=True)
            self.assertEqual(other["status"], "abstain")
            self.assertIn("one active", other["reason"])
            book = PaperLedger(ledger_path, balances=FULL_PORT_BALANCES, limits=FULL_PORT_LIMITS,
                               enforce_market_risk=True, clock=lambda: NOW)
            self.assertEqual({row.desk: row.initial_cash_cents for row in book.balances()}, FULL_PORT_BALANCES)
            self.assertEqual(sum(row.initial_cash_cents for row in book.balances()), 30_000)
            self.assertEqual(book.snapshot().reserved_cash_cents, reserved["reservation"]["reserved_cents"])

    def test_unfilled_exit_event_cancels_full_port_reservation_and_restart_is_inert(self):
        decision = run_observed_shadow(_evidence(), as_of=NOW, max_entry_cost="300",
                                       paper_profile="options_full_port_v1")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); ledger_path = root / "full-port.sqlite3"; state = root / "state"
            PaperLedger(ledger_path, balances=FULL_PORT_BALANCES, limits=FULL_PORT_LIMITS,
                        enforce_market_risk=True, clock=lambda: NOW)
            reserved = run_once(candidate=decision, state_dir=state, ledger_path=ledger_path, now=NOW, full_port=True)
            self.assertEqual(reserved["status"], "reserved")
            cancelled = run_once(candidate=decision, exit_cloud=cloud_for(decision, NOW + timedelta(seconds=50)),
                                 state_dir=state, ledger_path=ledger_path, now=NOW + timedelta(seconds=52), full_port=True)
            self.assertEqual(cancelled["status"], "cancelled")
            self.assertEqual(cancelled["reason"], "end_of_session_unfilled")
            book = PaperLedger(ledger_path, balances=FULL_PORT_BALANCES, limits=FULL_PORT_LIMITS,
                               enforce_market_risk=True, clock=lambda: NOW + timedelta(seconds=52))
            self.assertEqual(book.snapshot().reserved_cash_cents, 0)
            self.assertFalse(any(event.event_type == "fill" for event in book.audit()))
            restarted = run_once(candidate=decision, exit_cloud=cloud_for(decision, NOW + timedelta(seconds=55)),
                                 state_dir=state, ledger_path=ledger_path, now=NOW + timedelta(seconds=57), full_port=True)
            self.assertEqual(restarted["status"], "abstain")
            self.assertEqual(restarted["reason"], "end_of_session_unfilled")
            self.assertEqual(book.snapshot().reserved_cash_cents, 0)

    def test_partial_or_unresolved_provider_holdings_latch_risk_and_do_not_create_a_broker_fill(self):
        decision = run_observed_shadow(_evidence(), as_of=NOW)
        entry = cloud_for(decision, NOW + timedelta(seconds=12))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            initialized_ledger(root / "paper.sqlite3")
            run_once(candidate=decision, state_dir=root / "state", ledger_path=root / "paper.sqlite3", now=NOW)
            run_once(candidate=None, entry_cloud=entry, state_dir=root / "state",
                     ledger_path=root / "paper.sqlite3", now=NOW + timedelta(seconds=14))
            bad = cloud_for(decision, NOW + timedelta(seconds=30))
            bad["holdings"]["underlying_quantity"] = 100
            body = {key: value for key, value in bad.items() if key != "artifact_sha256"}
            bad["artifact_sha256"] = _digest(body)
            result = run_once(candidate=None, mark_cloud=bad, state_dir=root / "state",
                              ledger_path=root / "paper.sqlite3", now=NOW + timedelta(seconds=32))
            self.assertEqual(result["lifecycle"]["status"], "accounting_reconciliation_required")
            self.assertEqual(result["orders_sent"], 0)

    def test_later_lifecycle_allows_unrelated_positions_but_latches_selected_leg(self):
        decision = run_observed_shadow(_evidence(), as_of=NOW)
        entry = cloud_for(decision, NOW + timedelta(seconds=12))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); state = root / "state"; ledger_path = root / "paper.sqlite3"
            initialized_ledger(ledger_path)
            run_once(candidate=decision, state_dir=state, ledger_path=ledger_path, now=NOW)
            run_once(candidate=None, entry_cloud=entry, state_dir=state, ledger_path=ledger_path, now=NOW + timedelta(seconds=14))
            unrelated = cloud_for(decision, NOW + timedelta(seconds=30))
            unrelated["holdings"]["option_holdings"] = {"PERSONAL-UNRELATED-OPTION": 3}
            unrelated["artifact_sha256"] = _digest({key: value for key, value in unrelated.items() if key != "artifact_sha256"})
            self.assertEqual(run_once(candidate=None, mark_cloud=unrelated, state_dir=state, ledger_path=ledger_path,
                                      now=NOW + timedelta(seconds=32))["mark_status"], "persisted")
            conflicted = cloud_for(decision, NOW + timedelta(seconds=45))
            conflicted["holdings"]["option_holdings"] = {decision["shadow_candidate"]["long"]["symbol"]: 1}
            conflicted["artifact_sha256"] = _digest({key: value for key, value in conflicted.items() if key != "artifact_sha256"})
            result = run_once(candidate=None, mark_cloud=conflicted, state_dir=state, ledger_path=ledger_path,
                              now=NOW + timedelta(seconds=47))
            self.assertEqual(result["lifecycle"]["status"], "accounting_reconciliation_required")

    def test_stop_blocks_new_entry_but_existing_internal_position_can_still_be_marked(self):
        decision = run_observed_shadow(_evidence(), as_of=NOW)
        entry = cloud_for(decision, NOW + timedelta(seconds=12))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / "state"
            initialized_ledger(root / "paper.sqlite3")
            run_once(candidate=decision, state_dir=state, ledger_path=root / "paper.sqlite3", now=NOW)
            run_once(candidate=None, entry_cloud=entry, state_dir=state,
                     ledger_path=root / "paper.sqlite3", now=NOW + timedelta(seconds=14))
            (state / "STOP").write_text("")
            mark = cloud_for(decision, NOW + timedelta(seconds=30))
            result = run_once(candidate=None, mark_cloud=mark, state_dir=state,
                              ledger_path=root / "paper.sqlite3", now=NOW + timedelta(seconds=32))
            self.assertEqual(result["mark_status"], "persisted")

    def test_missing_or_wrong_shared_ledger_never_creates_a_new_book(self):
        decision = run_observed_shadow(_evidence(), as_of=NOW)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            missing = root / "missing.sqlite3"
            result = run_once(candidate=decision, state_dir=root / "state", ledger_path=missing, now=NOW)
            self.assertEqual(result["status"], "abstain")
            self.assertFalse(missing.exists())
            wrong = PaperLedger(root / "wrong.sqlite3", balances={**SHARED_BALANCES, "options": 20_000},
                                enforce_market_risk=True, clock=lambda: NOW)
            self.assertIsNotNone(wrong)
            result = run_once(candidate=decision, state_dir=root / "wrong-state", ledger_path=root / "wrong.sqlite3", now=NOW)
            self.assertEqual(result["status"], "abstain")
            self.assertIn("required $800", result["reason"])

    def test_stop_cancels_pending_reservation_and_releases_cash(self):
        decision = run_observed_shadow(_evidence(), as_of=NOW)
        entry = cloud_for(decision, NOW + timedelta(seconds=12))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); state = root / "state"; ledger_path = root / "paper.sqlite3"
            initialized_ledger(ledger_path)
            self.assertEqual(run_once(candidate=decision, state_dir=state, ledger_path=ledger_path, now=NOW)["status"], "reserved")
            (state / "STOP").write_text("")
            cancelled = run_once(candidate=None, entry_cloud=entry, state_dir=state, ledger_path=ledger_path,
                                 now=NOW + timedelta(seconds=14))
            self.assertEqual(cancelled["status"], "cancelled")
            book = PaperLedger(ledger_path, balances=SHARED_BALANCES, enforce_market_risk=True,
                               clock=lambda: NOW + timedelta(seconds=14))
            self.assertEqual(book.snapshot().reserved_cash_cents, 0)
            self.assertFalse(any(event.event_type == "fill" for event in book.audit()))

    def test_missing_or_stale_holdings_evidence_blocks_without_assignment_latch(self):
        decision = run_observed_shadow(_evidence(), as_of=NOW)
        entry = cloud_for(decision, NOW + timedelta(seconds=12))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); state = root / "state"; ledger_path = root / "paper.sqlite3"
            initialized_ledger(ledger_path)
            run_once(candidate=decision, state_dir=state, ledger_path=ledger_path, now=NOW)
            run_once(candidate=None, entry_cloud=entry, state_dir=state, ledger_path=ledger_path, now=NOW + timedelta(seconds=14))
            malformed = cloud_for(decision, NOW + timedelta(seconds=30))
            malformed["holdings"]["option_holdings"] = None
            body = {key: value for key, value in malformed.items() if key != "artifact_sha256"}
            malformed["artifact_sha256"] = _digest(body)
            result = run_once(candidate=None, mark_cloud=malformed, state_dir=state, ledger_path=ledger_path,
                              now=NOW + timedelta(seconds=32))
            self.assertEqual(result["mark_status"], "blocked_evidence")
            lifecycle = (state / "lifecycle" / "state.json")
            self.assertFalse(lifecycle.exists())

    def test_locked_or_symlinked_runtime_is_refused(self):
        decision = run_observed_shadow(_evidence(), as_of=NOW)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); state = root / "state"; state.mkdir(); initialized_ledger(root / "paper.sqlite3")
            lock = state / "runtime.lock"
            with lock.open("a+") as handle:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                result = run_once(candidate=decision, state_dir=state, ledger_path=root / "paper.sqlite3", now=NOW)
            self.assertEqual(result["reason"], "already_running")
