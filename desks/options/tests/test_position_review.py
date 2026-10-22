from datetime import datetime,timedelta,timezone
from decimal import Decimal
import json
from pathlib import Path
import unittest
from desks.options.verticals import validate_vertical
from desks.options.position_review import review_position

class PositionReviewTests(unittest.TestCase):
    def setUp(self):
        fixture=json.loads((Path(__file__).parents[1]/'fixtures/synthetic_verticals.json').read_text())
        self.now=datetime(2026,1,5,15,tzinfo=timezone.utc)
        self.entry=validate_vertical(fixture['cases'][0]['legs'],as_of=self.now)
        self.quotes={leg.symbol:{'bid':str(leg.bid),'ask':str(leg.ask),'timestamp':self.now+timedelta(seconds=1),'bid_size':1,'ask_size':1} for leg in self.entry.legs}
        self.holdings={leg.symbol:1 if leg.side=='buy' else -1 for leg in self.entry.legs}
    def run_review(self,**kwargs):
        return review_position(self.entry,self.quotes,self.holdings,as_of=self.now+timedelta(seconds=2),exit_fee_per_spread='1',exit_slippage_per_spread='0.50',**kwargs)
    def test_exit_sides_costs_and_time_exit(self):
        r=self.run_review()
        self.assertEqual(r['status'],'reviewed');self.assertEqual(r['liquidation_value_cents'],10850)
        self.assertEqual(r['net_pnl_cents'],-4150)
        self.assertIn('21_or_fewer_calendar_days_to_expiry',r['exit_reasons'])
    def test_partial_holdings_block(self):
        self.holdings[self.entry.legs[1].symbol]=0
        self.assertEqual(self.run_review()['status'],'blocked')
    def test_insufficient_depth_and_old_quote(self):
        for key,value in [('bid_size',0),('timestamp',self.now-timedelta(seconds=1))]:
            saved=self.quotes[self.entry.legs[0].symbol][key]
            self.quotes[self.entry.legs[0].symbol][key]=value
            self.assertEqual(self.run_review()['status'],'blocked')
            self.quotes[self.entry.legs[0].symbol][key]=saved
    def test_negative_liability_preserved(self):
        self.quotes[self.entry.legs[0].symbol].update(bid='0',ask='1')
        self.quotes[self.entry.legs[1].symbol].update(bid='0.1',ask='0.2')
        r=self.run_review();self.assertEqual(r['liquidation_value_cents'],-2150)
        self.assertFalse(r['shared_mark_eligible']);self.assertIn('half_entry_cost_lost',r['exit_reasons'])
    def test_reversal_and_profit(self):
        self.quotes[self.entry.legs[0].symbol].update(bid='5',ask='5.1')
        self.quotes[self.entry.legs[1].symbol].update(bid='1',ask='1.1')
        r=self.run_review(signal_reversed=True)
        self.assertIn('half_maximum_profit',r['exit_reasons']);self.assertIn('signal_reversed',r['exit_reasons'])
    def test_expiration_day_blocked(self):
        r=review_position(self.entry,self.quotes,self.holdings,as_of=datetime(2026,1,16,12,tzinfo=timezone.utc),exit_fee_per_spread=0,exit_slippage_per_spread=0)
        self.assertEqual(r['status'],'blocked');self.assertIn('Expiration-day',r['blockers'][0])
if __name__=='__main__':unittest.main()
