import unittest
from decimal import Decimal as D
from datetime import datetime,timezone
import replay
class Tests(unittest.TestCase):
 def test_variable_timestamp_precision(self):
  self.assertEqual(replay.stamp('2026-10-17T23:45:06.82941Z'),replay.stamp('2026-10-17T23:45:06.829410+00:00'))
  self.assertEqual(replay.stamp('2026-10-17T23:45:06.1Z'),replay.stamp('2026-10-17T23:45:06.100000+00:00'))
 def test_direct_fee_and_non_direct_rounding(self):
  self.assertEqual(replay.fee(1,D('.5')),D('.0175'))
  self.assertEqual(replay.fee(1,D('.5'),D('.01')),D('.02'))
  self.assertEqual(replay.fee(100,D('.5')),D('1.75'))
 def test_subpenny_balance_rounding(self):
  self.assertEqual(replay.fee(1,D('.055'),D('.01')),D('.005'))
  self.assertEqual(replay.fee(1,D('.055')),D('.0037'))
 def test_fee_inclusive_sizing(self):
  q,b=replay.size(D(500),D('.5'));self.assertEqual(q,241);self.assertEqual(b,D(125));self.assertLessEqual(q*D('.5')+replay.fee(q,D('.5')),b);self.assertGreater((q+1)*D('.5')+replay.fee(q+1,D('.5')),b)
 def test_no_ask_complement(self):
  c={'yes_bid':{'close_dollars':'.7','low_dollars':'.65'},'yes_ask':{'close_dollars':'.72','high_dollars':'.8'}}
  self.assertEqual(replay.quote_price(c,-1,'delayed_close')[0],D('.3'));self.assertEqual(replay.quote_price(c,-1,'adverse_minute')[0],D('.35'));self.assertEqual(replay.quote_price(c,1,'adverse_minute')[0],D('.8'))
 def test_missing_crossed_quotes_rejected(self):
  self.assertIsNone(replay.quote_price(None,1,'delayed_close')[0]);self.assertIsNone(replay.quote_price({'yes_bid':{'close_dollars':'.8'},'yes_ask':{'close_dollars':'.7'}},1,'delayed_close')[0])
 def test_delayed_settlement_blocks_cash_reuse(self):
  base=1790000100;inputs=[];quotes={}
  for i in range(2):
   t=base+900*i;key=str(i)
   r={'market_start':replay.iso(t),'5min_lead0':'1'};m={'ticker':key,'close_time':replay.iso(t+900),'settlement_ts':replay.iso(base+2000 if i==0 else t+1000),'result':'yes','settlement_value_dollars':'1'};inputs.append((r,m));quotes[key]={t+360:{'end_period_ts':t+360,'yes_bid':{'close_dollars':'.49'},'yes_ask':{'close_dollars':'.5'}}}
  r=replay.run_account('A_5min','delayed_close',inputs,quotes,False);self.assertEqual(r['trades'],1);self.assertEqual(r['skips']['prior_position_unsettled'],1);self.assertEqual(r['ending_cash'],616.2825)
 def test_no_synthetic_closest_quote(self):
  base=1790000100;r={'market_start':replay.iso(base),'5min_lead0':'1'};m={'ticker':'x'}
  out=replay.run_account('A_5min','delayed_close',[(r,m)],{'x':{base+359:{}}},False);self.assertEqual(out['trades'],0);self.assertEqual(out['ending_cash'],500)
if __name__=='__main__':unittest.main()
