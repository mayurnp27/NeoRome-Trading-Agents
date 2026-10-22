from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest
from neorome.prediction_bridge import prepare, reserve
from neorome.paper_ledger import PaperLedger, RiskStopActive, RiskLimitExceeded, RiskLimits

NOW = datetime(2026,9,18,14,0,tzinfo=timezone.utc)
T = NOW.timestamp()

def fixture():
    p={'proposal_id':'synthetic-example','strategy_id':'forecasting','event_id':'fixture-weather',
       'created_at':T,'expires_at':T+30,'input_times':[T-1],'evidence_ids':['fixture-evidence'],'synthetic':True,
       'legs':[{'venue':'kalshi','market_id':'FIXTURE','outcome':'yes','quantity':10,'limit_price':'0.40',
                'execution':'taker','quote':{'received_at':T,'source_time':T,'bid':'0.39','ask':'0.40','depth':10}}]}
    c={'mode':'paper_only','strategies':{'forecasting':{'enabled':True,'paper_budget':'100'}},
       'risk':{'max_age_seconds':60,'max_input_age_seconds':300,'max_contracts':100,'max_order_cost':'5'},
       'instruments':[{'venue':'kalshi','market_id':'FIXTURE','event_id':'fixture-weather','reviewed':True,
          'review_valid_until':T+300,'fee':{'coefficient':'0.07','round_up':'0.01'}}],
       'evidence':{'fixture-evidence':{'event_id':'fixture-weather','synthetic':True,'observed_at':T-1,'valid_until':T+300}}}
    return {'proposal':p,'reason':'ok','diagnostics':{'fixture_only':True}},c

GROUPS={'fixture-weather':'weather:fixture-station-date'}

class PredictionBridgeTests(unittest.TestCase):
    def test_fee_rounding_and_aggregate_lot(self):
        r,c=fixture(); p=prepare(r,c,now=NOW,exposure_groups=GROUPS,risk_policy='firm_shared_v1')
        self.assertEqual((p.quantity,p.unit_cost_cents,p.fee_cents),(1,400,20))
        self.assertEqual(p.metadata['instrument_units'],10)
        self.assertEqual(p.provenance,'synthetic')
        self.assertEqual(p.idea_id,'prediction-event:fixture-weather')

    def test_abstention_is_not_a_proposal(self):
        self.assertIsNone(prepare({'proposal':None,'reason':'no edge'}, {},now=NOW,exposure_groups={},risk_policy='firm_shared_v1'))

    def test_unsupported_and_invalid_inputs(self):
        changes=[lambda r,c:r['proposal'].update(synthetic=False),
                 lambda r,c:r['proposal']['legs'].append(deepcopy(r['proposal']['legs'][0])),
                 lambda r,c:r['proposal']['legs'][0].update(execution='maker'),
                 lambda r,c:r['proposal']['legs'][0]['quote'].update(depth=9),
                 lambda r,c:r['proposal']['legs'][0]['quote'].update(bid='.41'),
                 lambda r,c:r['proposal']['legs'][0]['quote'].update(source_time=T-61),
                 lambda r,c:r['proposal']['legs'][0]['quote'].update(received_at=T+1),
                 lambda r,c:r['proposal']['legs'][0].update(quantity=True),
                 lambda r,c:r['proposal']['legs'][0].update(limit_price='NaN'),
                 lambda r,c:r['proposal']['legs'][0].update(quantity=12),
                 lambda r,c:c['evidence']['fixture-evidence'].update(event_id='wrong'),
                 lambda r,c:c['evidence']['fixture-evidence'].update(synthetic=False),
                 lambda r,c:c['instruments'][0].update(review_valid_until=T),
                 lambda r,c:c['strategies']['forecasting'].update(enabled=False),
                 lambda r,c:r['proposal'].update(expires_at=T)]
        for change in changes:
            with self.subTest(change=change):
                r,c=fixture();change(r,c)
                with self.assertRaises(ValueError):prepare(r,c,now=NOW,exposure_groups=GROUPS,risk_policy='firm_shared_v1')
        r,c=fixture()
        with self.assertRaises(ValueError):prepare(r,c,now=NOW,exposure_groups={},risk_policy='firm_shared_v1')

    def test_shared_book_reservation_restart_retry_and_release_without_fill(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'paper.db'; book=PaperLedger(path,enforce_market_risk=True,clock=lambda:NOW)
            r,c=fixture()
            with self.assertRaises(RiskStopActive):reserve(r,c,book,now=NOW,exposure_groups=GROUPS,risk_policy='firm_shared_v1')
            book.record_marks('empty',[],as_of=NOW)
            reservation=reserve(r,c,book,now=NOW,exposure_groups=GROUPS,risk_policy='firm_shared_v1')
            book=PaperLedger(path,enforce_market_risk=True,clock=lambda:NOW)
            self.assertEqual(reserve(r,c,book,now=NOW,exposure_groups=GROUPS,risk_policy='firm_shared_v1'),reservation)
            self.assertEqual(book.snapshot().reserved_cash_cents,420)
            self.assertEqual(book.snapshot().open_position_cost_cents,0)
            self.assertEqual(book.market_snapshot().market_nav_cents,80000)
            r['proposal']['proposal_id']='synthetic-another-same-event'
            with self.assertRaises(RiskLimitExceeded):reserve(r,c,book,now=NOW,exposure_groups=GROUPS,risk_policy='firm_shared_v1')
            book.cancel(reservation.reservation_id,reason='fixture complete')
            self.assertEqual(book.snapshot().available_cash_cents,80000)
            self.assertFalse(any(e.event_type=='fill' for e in book.audit()))

    def test_explicit_risk_ownership_required(self):
        r,c=fixture()
        with self.assertRaises(ValueError):
            prepare(r,c,now=NOW,exposure_groups=GROUPS,risk_policy='source_demo')
        c['strategies']['forecasting']['paper_budget']='1'
        p=prepare(r,c,now=NOW,exposure_groups=GROUPS,risk_policy='firm_shared_v1')
        self.assertEqual(p.metadata['risk_policy'],'firm_shared_v1')
        self.assertEqual(p.metadata['source_strategy_config']['paper_budget'],'1')

    def test_looser_firm_limits_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            r,c=fixture()
            book=PaperLedger(Path(tmp)/'paper.db',enforce_market_risk=True,limits=RiskLimits(aggregate_cap_cents=9000),clock=lambda:NOW)
            book.record_marks('empty',[],as_of=NOW)
            with self.assertRaises(ValueError):
                reserve(r,c,book,now=NOW,exposure_groups=GROUPS,risk_policy='firm_shared_v1')
            self.assertEqual(book.snapshot().reserved_cash_cents,0)

    def test_legacy_mode_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            r,c=fixture();book=PaperLedger(Path(tmp)/'paper.db',clock=lambda:NOW)
            with self.assertRaises(ValueError):reserve(r,c,book,now=NOW,exposure_groups=GROUPS,risk_policy='firm_shared_v1')

if __name__=='__main__': unittest.main()
