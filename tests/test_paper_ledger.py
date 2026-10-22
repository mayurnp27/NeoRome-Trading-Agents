from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
import tempfile
import unittest
from neorome.paper_ledger import (PaperLedger, Proposal, RiskLimits, RiskLimitExceeded,
    IdempotencyConflict, InvalidState, RiskStopActive, ValidationError, InsufficientCash)

NOW = datetime(2026, 9, 17, 12, tzinfo=timezone.utc)

def proposal(key='one', desk='crypto', cost=400, group='risk', idea=None):
    return Proposal(key,desk,'fixture','1','a'*64,NOW,'SYNTH',group,idea or key,
                    NOW+timedelta(minutes=5),1,cost,provenance='synthetic')

class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name)/'ledger.db'
        self.ledger = PaperLedger(self.path,clock=lambda:NOW)
    def tearDown(self):
        self.tmp.cleanup()
    def test_default_allocation(self):
        self.assertEqual(self.ledger.snapshot().paper_nav_cents,80000)
        self.assertEqual({b.desk:b.available_cash_cents for b in self.ledger.balances()},
                         {'prediction':10000,'equities':30000,'options':30000,'crypto':10000})
    def test_duplicate_payload_conflict_and_expired_replay(self):
        p=proposal();r=self.ledger.reserve(p)
        self.assertEqual(self.ledger.reserve(p,now=NOW+timedelta(days=1)),r)
        with self.assertRaises(IdempotencyConflict):self.ledger.reserve(replace(p,unit_cost_cents=401))
        self.assertEqual(self.ledger.snapshot().reserved_cash_cents,400)
    def test_concurrent_idea_cap(self):
        def attempt(i):
            ledger=PaperLedger(self.path,clock=lambda:NOW)
            try:ledger.reserve(proposal(str(i),idea='same'));return True
            except RiskLimitExceeded:return False
        with ThreadPoolExecutor(max_workers=4) as pool:
            results=list(pool.map(attempt,range(8)))
        self.assertEqual(sum(results),1)
        self.assertEqual(self.ledger.snapshot().reserved_cash_cents,400)
    def test_group_and_aggregate_caps_include_positions(self):
        for i in range(2):
            r=self.ledger.reserve(proposal(str(i),'equities',1500,idea=str(i)))
            self.ledger.simulate_fill(r.reservation_id)
        with self.assertRaises(RiskLimitExceeded):self.ledger.reserve(proposal('group'))
        for i in range(3):self.ledger.reserve(proposal('other'+str(i),'options',1500,group='other'+str(i)))
        self.ledger.reserve(proposal('last','crypto',500,group='last'))
        with self.assertRaises(RiskLimitExceeded):self.ledger.reserve(proposal('over','crypto',1,group='new'))
    def test_same_idea_split_ids_blocked_after_fill(self):
        r=self.ledger.reserve(proposal('a',idea='same'));self.ledger.simulate_fill(r.reservation_id)
        with self.assertRaises(RiskLimitExceeded):self.ledger.reserve(proposal('b',idea='same'))
    def test_same_idea_across_desks_uses_strictest_cap(self):
        self.ledger.reserve(proposal('a','prediction',400,idea='same'))
        with self.assertRaises(RiskLimitExceeded):self.ledger.reserve(proposal('b','equities',500,idea='same'))
    def test_exit_fee_liability_and_close_chronology(self):
        r=self.ledger.reserve(proposal());f=self.ledger.simulate_fill(r.reservation_id)
        with self.assertRaises(ValidationError):self.ledger.close(f.position_id,proceeds_cents=0,fee_cents=1)
        with self.assertRaises(ValidationError):self.ledger.close(f.position_id,proceeds_cents=10,closed_at=NOW-timedelta(seconds=1))
    def test_missing_audit_blocks_replay(self):
        p=proposal();self.ledger.reserve(p)
        with sqlite3.connect(str(self.path)) as conn:conn.execute('DELETE FROM audit_events')
        with self.assertRaises(InvalidState):self.ledger.reserve(p)
    def test_cancel_replay_conflict_and_no_fill(self):
        r=self.ledger.reserve(proposal());self.ledger.cancel(r.reservation_id,cancel_id='cancel',reason='test')
        self.ledger.cancel(r.reservation_id,cancel_id='cancel',reason='test')
        with self.assertRaises(IdempotencyConflict):self.ledger.cancel(r.reservation_id,cancel_id='cancel',reason='changed')
        with self.assertRaises(InvalidState):self.ledger.simulate_fill(r.reservation_id)
        self.assertEqual(self.ledger.snapshot().available_cash_cents,80000)
    def test_fill_price_improvement_fees_restart_and_close(self):
        p=replace(proposal(cost=400),fee_cents=10)
        r=self.ledger.reserve(p)
        f=self.ledger.simulate_fill(r.reservation_id,unit_cost_cents=380,fee_cents=10)
        ledger=PaperLedger(self.path,clock=lambda:NOW)
        self.assertEqual(ledger.snapshot().open_position_cost_cents,390)
        self.assertEqual(ledger.simulate_fill(r.reservation_id,unit_cost_cents=380,fee_cents=10),f)
        with self.assertRaises(IdempotencyConflict):ledger.simulate_fill(r.reservation_id)
        c=ledger.close(f.position_id,proceeds_cents=450,fee_cents=5)
        self.assertEqual(c.realized_pnl_cents,55)
        self.assertEqual(ledger.close(f.position_id,proceeds_cents=450,fee_cents=5),c)
        self.assertEqual(ledger.snapshot().paper_nav_cents,80055)
        with self.assertRaises(IdempotencyConflict):ledger.close(f.position_id,proceeds_cents=451,fee_cents=5)
    def test_fill_bounds_expiry_and_time(self):
        r=self.ledger.reserve(proposal())
        with self.assertRaises(InsufficientCash):self.ledger.simulate_fill(r.reservation_id,unit_cost_cents=401)
        with self.assertRaises(ValidationError):self.ledger.simulate_fill(r.reservation_id,quantity=2)
        with self.assertRaises(InvalidState):self.ledger.simulate_fill(r.reservation_id,filled_at=NOW+timedelta(minutes=6))
        with self.assertRaises(ValidationError):self.ledger.simulate_fill(r.reservation_id,filled_at=NOW-timedelta(seconds=1))
        with self.assertRaises(ValidationError):self.ledger.simulate_fill(r.reservation_id,provenance='observed')
    def test_bad_inputs_and_stale_sources(self):
        for change in ({'unit_cost_cents':True},{'unit_cost_cents':1.5},{'source_hash':'bad'},
                       {'source_timestamp':NOW+timedelta(seconds=1)},
                       {'source_timestamp':NOW-timedelta(minutes=6)}, {'expiry_at':NOW}):
            with self.assertRaises(ValidationError):self.ledger.reserve(replace(proposal(),**change))
    def test_stop_persists_but_allows_close(self):
        r=self.ledger.reserve(proposal());f=self.ledger.simulate_fill(r.reservation_id)
        self.ledger.set_risk_stop(True,reason='test')
        ledger=PaperLedger(self.path,clock=lambda:NOW)
        with self.assertRaises(RiskStopActive):ledger.reserve(proposal('new'))
        ledger.close(f.position_id,proceeds_cents=0)
        self.assertEqual(ledger.snapshot().paper_nav_cents,79600)
    def test_tampered_cash_blocks_read_retry_and_restart(self):
        p=proposal();self.ledger.reserve(p)
        with sqlite3.connect(str(self.path)) as conn:conn.execute("UPDATE accounts SET available_cash_cents=available_cash_cents+1 WHERE desk='crypto'")
        with self.assertRaises(InvalidState):self.ledger.reserve(p)
        with self.assertRaises(InvalidState):self.ledger.snapshot()
        with self.assertRaises(InvalidState):PaperLedger(self.path,clock=lambda:NOW)
    def test_missing_account_or_changed_configuration_not_reseeded(self):
        with sqlite3.connect(str(self.path)) as conn:conn.execute("DELETE FROM accounts WHERE desk='crypto'")
        with self.assertRaises(InvalidState):PaperLedger(self.path,clock=lambda:NOW)
        with self.assertRaises(ValidationError):PaperLedger(self.path,limits=RiskLimits(aggregate_cap_cents=9000),clock=lambda:NOW)
    def test_failure_rolls_back_all_state(self):
        original=self.ledger._audit
        def fail(*args):raise RuntimeError('injected failure')
        self.ledger._audit=fail
        with self.assertRaises(RuntimeError):self.ledger.reserve(proposal())
        self.ledger._audit=original
        self.assertEqual(self.ledger.snapshot().available_cash_cents,80000)
        self.assertEqual(len(self.ledger.audit()),0)
    def test_foreign_database_refused(self):
        path=Path(self.tmp.name)/'foreign.db'
        with sqlite3.connect(str(path)) as conn:conn.execute('CREATE TABLE unrelated (id INT)')
        with self.assertRaises(ValidationError):PaperLedger(path)
    def test_memory_database_supported(self):
        ledger=PaperLedger(':memory:',clock=lambda:NOW)
        ledger.reserve(proposal())
        self.assertEqual(ledger.snapshot().reserved_cash_cents,400)
        ledger.dispose()

    def test_zero_balance_desks_allow_an_exactly_funded_isolated_account(self):
        ledger=PaperLedger(Path(self.tmp.name)/'isolated.db', balances={'prediction':10000,'equities':0,'options':0,'crypto':0}, clock=lambda:NOW)
        self.assertEqual(ledger.snapshot().paper_nav_cents,10000)
        with self.assertRaises(InsufficientCash):ledger.reserve(proposal('no-funds','crypto',1))

if __name__=='__main__':unittest.main()
