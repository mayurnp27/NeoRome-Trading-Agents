"""Prospective crossover/risk regressions; historical tuples are training examples."""
from copy import deepcopy
from datetime import timedelta
from pathlib import Path
import tempfile
import unittest
from neorome.paper_ledger import Proposal
from neorome import btc_stochastic_paper as slow, btc_fast_stochastic_paper as fast
from neorome.paper_ledger import PaperLedger, MarketRiskPolicy, RiskLimits
from tests.test_btc_stochastic_paper import make_input, make_book, at, OPEN, TICKER, digest

class CrossoverTests(unittest.TestCase):
    def fixture(self, module, direction='bullish'):
        value=make_input(direction=direction)
        if module is fast:
            value['signal'].update(timeframe_minutes=5, stochastic_settings=[5,3,3], explicit_source_bar_timestamp=True)
        value['book1']=make_book(2,no_quantity='10000',yes_quantity='10000',source='a')
        value['book2']=make_book(4,no_quantity='10000',yes_quantity='10000',source='b')
        return value

    def test_five_observed_training_signals_require_new_cross(self):
        rows=[(fast,'bearish',97.9268006034,81.5576301876,93.235275348,89.7713411113,False),
              (slow,'bullish',10.502893,15.100961,38.433398,21.147592,True),
              (fast,'bearish',84.5667180489,81.4565603562,83.4300406975,83.0486962276,False),
              (slow,'bearish',94.030993,84.656321,90.393743,91.997008,True),
              (slow,'bearish',90.393743,91.997008,84.807657,89.744131,False)]
        for module,direction,pk,pd,k,d,eligible in rows:
            value=self.fixture(module,direction)
            value['signal'].update(previous_k=pk,previous_d=pd,k=k,d=d)
            with self.subTest(row=(module.__name__,pk)),tempfile.TemporaryDirectory() as root:
                if eligible:self.assertEqual(module.run_once(value,root,at(10))['status'],'filled')
                else:
                    with self.assertRaisesRegex(module.BTCStochasticPaperError,'fresh K/D crossover'):
                        module.run_once(value,root,at(10))
                    self.assertEqual(module.initialize(root).active_exposure_count('prediction'),0)

    def test_each_bankroll_twenty_percent_cash_fee_inclusive_and_recalculated_after_resolution(self):
        for module in (slow,fast):
            for outcome in ('yes','no'):
                with self.subTest(module=module.__name__,outcome=outcome),tempfile.TemporaryDirectory() as root:
                    value=self.fixture(module)
                    fill=module.run_once(value,root,at(10))['fill']
                    self.assertLessEqual(fill['total_cost_cents'],10000)
                    self.assertGreater(fill['total_cost_cents'],9900)
                    module.run_once({'settlement':{'ticker':TICKER,'status':'settled','result':outcome,
                        'observed_at':at(902),'source_hash':digest(outcome)}},root,at(905))
                    book=module.initialize(root)
                    available=next(x.available_cash_cents for x in book.balances() if x.desk=='prediction')
                    self.assertEqual(available,50000-fill['total_cost_cents']+(fill['quantity']*100 if outcome=='yes' else 0))
                    newer=deepcopy(value)
                    newer['signal'].update(signal_id='next-cross',bar_close_timestamp=int(OPEN.timestamp())+1800,observed_receipt_at=at(1801))
                    newer['market'].update(ticker='KXBTC15M-26OCT141230',open_time=at(1800),close_time=at(2700))
                    newer['book1']=make_book(1802,no_quantity='10000',source='next1')
                    newer['book2']=make_book(1804,no_quantity='10000',source='next2')
                    again=module.run_once(newer,root,at(1810))['fill']
                    budget=available*module.ALLOCATION_BPS//10000
                    self.assertLessEqual(again['total_cost_cents'],budget)
                    self.assertGreater(again['total_cost_cents'],budget-100)
                    self.assertEqual(len([a for a in book.audit() if a.event_type=='fill']),2)

    def test_legacy_half_cash_pending_reservations_are_cancelled_before_fill(self):
        for module in (slow, fast):
            with self.subTest(module=module.__name__), tempfile.TemporaryDirectory() as root:
                pending_input = self.fixture(module)
                pending_input['book2'] = None
                pending = module.run_once(pending_input, root, at(3))
                book = module.initialize(root)
                old_reservation = book.get_reservation(pending['reservation_id'])
                old_payload = module._reservation_payload(book, old_reservation.proposal_id)
                marker = deepcopy(old_payload['metadata'][module._MARKER])
                module._cancel(book, Path(root), old_reservation, module._time(at(4), 'now'), 'replace with legacy fixture')

                # Recreate the valid 50% reservation shape emitted by the prior release.
                marker.pop('allocation_bps', None)
                marker.pop('allocation_policy_version', None)
                marker.pop('available_cash_before_cents', None)
                marker.update(budget_cents=25_000, reserved_cents=25_000, quantity=500)
                proposal_id = 'legacy-half-cash-' + module.__name__.rsplit('_', 1)[-1]
                proposal = Proposal(
                    proposal_id=proposal_id, desk='prediction', strategy=old_payload['strategy'],
                    strategy_version=old_payload['strategy_version'], source_hash=old_payload['source_hash'],
                    source_timestamp=old_payload['source_timestamp'], instrument=old_payload['instrument'],
                    exposure_group=old_payload['exposure_group'], idea_id=proposal_id,
                    expiry_at=old_payload['expiry_at'], quantity=500, unit_cost_cents=50, fee_cents=0,
                    provenance='observed', metadata={module._MARKER: marker})
                reservation = book.reserve(proposal, now=at(2))
                module._write_event(Path(root), 'intent-' + proposal_id, 'intent',
                                    {'proposal_id': proposal_id, 'book1': marker['book1']},
                                    module._time(at(2), 'now'))

                result = module.run_once({}, root, at(10))
                self.assertEqual(result['status'], 'cancelled')
                self.assertIn('20% cash allocation', result['reason'])
                self.assertEqual(book.get_reservation(reservation.reservation_id).state, 'cancelled')
                self.assertEqual(next(row.available_cash_cents for row in book.balances()
                                      if row.desk == 'prediction'), 50_000)

    def test_legacy_half_cash_open_position_survives_restart_mark_settlement_then_sizes_new_entry_at_twenty_percent(self):
        for module in (slow, fast):
            with self.subTest(module=module.__name__), tempfile.TemporaryDirectory() as root:
                first_input = self.fixture(module)
                mark_book = first_input['book2']
                first_input['book2'] = None
                pending = module.run_once(first_input, root, at(3))
                book = module.initialize(root)
                reservation = book.get_reservation(pending['reservation_id'])
                original = module._reservation_payload(book, reservation.proposal_id)
                marker = deepcopy(original['metadata'][module._MARKER])
                module._cancel(book, Path(root), reservation, module._time(at(4), 'now'), 'replace with legacy fixture')

                # Seed a valid prior-release fill that used half of the $500 cash balance.
                marker.pop('allocation_bps', None)
                marker.pop('allocation_policy_version', None)
                marker.pop('available_cash_before_cents', None)
                marker.update(budget_cents=25_000, reserved_cents=25_000, quantity=500)
                proposal_id = 'legacy-open-' + module.__name__.rsplit('_', 1)[-1]
                proposal = Proposal(
                    proposal_id=proposal_id, desk='prediction', strategy=original['strategy'],
                    strategy_version=original['strategy_version'], source_hash=original['source_hash'],
                    source_timestamp=original['source_timestamp'], instrument=original['instrument'],
                    exposure_group=original['exposure_group'], idea_id=proposal_id,
                    expiry_at=original['expiry_at'], quantity=500, unit_cost_cents=50, fee_cents=0,
                    provenance='observed', metadata={module._MARKER: marker})
                legacy_reservation = book.reserve(proposal, now=at(2))
                book.simulate_fill(legacy_reservation.reservation_id, fill_id=proposal_id + '-fill',
                                   quantity=500, unit_cost_cents=50, fee_cents=0,
                                   provenance='observed', filled_at=at(3))

                resumed = module.run_once({'mark_book': {**mark_book, 'ticker': marker['market']['ticker']}}, root, at(10))
                self.assertEqual(resumed['status'], 'open')
                self.assertEqual(resumed['mark']['status'], 'valued')
                state = module.snapshot(root, at(11))
                self.assertEqual(state['active_position']['quantity'], 500)
                self.assertIsNone(state['available_cash_before_cents'],
                                  'old position metadata has no safely recorded pre-entry cash value')
                settled = module.run_once({'settlement': {
                    'ticker': marker['market']['ticker'], 'status': 'settled', 'result': 'yes',
                    'observed_at': at(902), 'source_hash': digest('legacy-settlement-' + proposal_id)}}, root, at(905))
                self.assertEqual(settled['status'], 'settled')

                next_input = deepcopy(self.fixture(module))
                next_input['signal'].update(signal_id='after-' + proposal_id,
                    bar_close_timestamp=int(OPEN.timestamp()) + 1800, observed_receipt_at=at(1801))
                next_input['market'].update(ticker='KXBTC15M-26OCT141230',
                    open_time=at(1800), close_time=at(2700))
                next_input['book1'] = make_book(1802, no_quantity='10000', yes_quantity='10000', source='after-first')
                next_input['book2'] = make_book(1804, no_quantity='10000', yes_quantity='10000', source='after-second')
                next_fill = module.run_once(next_input, root, at(1810))['fill']
                allocation_base = 75_000
                self.assertLessEqual(next_fill['total_cost_cents'], allocation_base * module.ALLOCATION_BPS // 10_000)
                after = module.snapshot(root, at(1810))
                self.assertEqual(after['available_cash_before_cents'], allocation_base)

    def test_rejected_D_old_method_and_continuing_state_cannot_reserve(self):
        for module in (slow,fast):
            for change in ({'d':None},{'d':True},{'previous_d':float('nan')},{'method':'turn'},
                           {'previous_d':18,'d':20},{'previous_d':22,'d':21}):
                with self.subTest(module=module.__name__,change=change),tempfile.TemporaryDirectory() as root:
                    value=self.fixture(module);value['signal'].update(change)
                    with self.assertRaises(module.BTCStochasticPaperError): module.run_once(value,root,at(10))
                    self.assertEqual(module.initialize(root).active_exposure_count('prediction'),0)

    def test_canceled_cross_cannot_retry_and_prior_equality_is_allowed(self):
        for module in (slow,fast):
            with self.subTest(module=module.__name__),tempfile.TemporaryDirectory() as root:
                value=self.fixture(module);value['signal']['previous_d']=19.5
                value['book2']=deepcopy(value['book1'])
                self.assertEqual(module.run_once(value,root,at(10))['status'],'cancelled')
                value['book2']=make_book(12,no_quantity='10000',source='retry')
                self.assertEqual(module.run_once(value,root,at(13))['status'],'cancelled_replay')
                self.assertEqual(next(x.available_cash_cents for x in module.initialize(root).balances() if x.desk=='prediction'),50000)

    def test_separate_bankrolls_and_refuse_old_funding_shape(self):
        with tempfile.TemporaryDirectory() as root:
            for module in (slow,fast):
                path=Path(root)/module.STRATEGY_ID;book=module.initialize(path)
                self.assertEqual(next(x.available_cash_cents for x in book.balances() if x.desk=='prediction'),50000)
                old=Path(root)/(module.STRATEGY_ID+'-old');old.mkdir()
                db='btc_fast_stochastic_paper.sqlite3' if module is fast else 'btc_stochastic_paper.sqlite3'
                legacy=PaperLedger(old/db,balances={'prediction':10000,'equities':0,'options':0,'crypto':0},
                    limits=book.limits,market_risk_policy=MarketRiskPolicy(daily_loss_bps=10000,max_drawdown_bps=10000),enforce_market_risk=False)
                with self.assertRaises(Exception):module.initialize(old)
                self.assertEqual(next(x.available_cash_cents for x in legacy.balances() if x.desk=='prediction'),10000)
