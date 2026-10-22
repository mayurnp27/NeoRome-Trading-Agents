import unittest
from neorome.btc_interval_expansion_signal import evaluate,stochastic
from neorome import tradingview_stoch15_source as source

class SignalTests(unittest.TestCase):
    def bars(self,last=30,down=False):
        vals=[30]*14+[30,10,10,10,10,last]
        if down:vals=[100-x for x in vals]
        return [{'timestamp':i*900,'bar_close_timestamp':(i+1)*900,'open':50,'high':100,'low':1,'close':v} for i,v in enumerate(vals)]
    def evaluate(self,b):return evaluate(b,observed_at=18005,target_close=18000)
    def test_fourteen_period_cross_and_gap_in_both_directions(self):
        for down,side in [(False,'yes'),(True,'no')]:
            r=self.evaluate(self.bars(down=down));self.assertTrue(r['qualifies']);self.assertEqual(r['direction'],side);self.assertTrue(r['gap_qualifies']);self.assertEqual(r['stochastic_settings'],[14,3,3])
    def test_large_gap_preserves_original_but_excludes_gap_account(self):
        r=self.evaluate(self.bars(last=40));self.assertTrue(r['qualifies']);self.assertFalse(r['gap_qualifies'])
    def test_no_cross_when_direction_is_already_established(self):
        b=self.bars();b[-4]['close']=30;b[-3]['close']=40;b[-2]['close']=50;b[-1]['close']=60
        self.assertFalse(self.evaluate(b)['qualifies'])
    def test_gaps_nonfinite_stale_and_flat_are_rejected(self):
        b=self.bars();b[3]['timestamp']+=1;self.assertFalse(self.evaluate(b)['qualifies'])
        b=self.bars();b[-1]['close']=float('nan');self.assertFalse(self.evaluate(b)['qualifies'])
        self.assertFalse(evaluate(self.bars(),observed_at=18091,target_close=18000)['qualifies'])
        b=self.bars()
        for row in b:row.update(open=50,high=50,low=50,close=50)
        self.assertFalse(self.evaluate(b)['qualifies'])
    def test_known_fourteen_bar_range_gives_87_point_5_not_five_bar_value(self):
        from decimal import Decimal
        b=[{'low':Decimal(i+1),'high':Decimal(i+4),'close':Decimal(i+2)} for i in range(20)]
        k,d=stochastic(b)
        self.assertEqual(k[-1],Decimal('87.5'));self.assertEqual(d[-1],Decimal('87.5'))

    def test_source_resolution_and_forming_filter_are_fifteen_minutes(self):
        self.assertEqual(source.BAR_SECONDS,900);self.assertEqual(source.TIMEFRAME,'15m');b=self.bars();rows=[[x[k] for k in ['timestamp','open','high','low','close']] for x in b]
        rows.append([18000,50,100,1,50]);closed=source.validate_bars(rows,18005);self.assertEqual(closed[-1]['bar_close_timestamp'],18000)

if __name__=='__main__':unittest.main()
