import json,tempfile,unittest
from pathlib import Path
from decimal import Decimal
from neorome.btc_distance_engine import Engine,execution,levels,dec,ARMS

class EngineTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)/'ledger.sqlite3';self.e=Engine(self.path,0);self.h=3600
        self.m={'hour_start':self.h,'close':7200,'ticker':'KXBTCD-TEST-T100','strike':'100'}
        self.e.selection(self.h,self.h+3,self.m)
        self.s={'decision_time':4500,'qualifies':True,'signal':1}
        self.b={'yes_asks':[{'price':'.80','quantity':'1000'}],'yes_bids':[{'price':'.79','quantity':'1000'}],
                'no_asks':[{'price':'.21','quantity':'1000'}],'no_bids':[{'price':'.20','quantity':'1000'}]}
    def tearDown(self):self.e.db.close();self.tmp.cleanup()
    def receipt(self,t=4560.5):return {'request_started':t-.2,'received':t,'data':{'orderbook':'test'}}
    def enter(self):self.e.decision(self.h,4505,self.s);self.e.fill(self.h,self.receipt(),self.b,4560.6)
    def test_allocations_fees_maximum_size_and_idempotence(self):
        self.enter();self.e.fill(self.h,self.receipt(4561),self.b,4561.1)
        for a in self.e.status(4562)['accounts']:
            p=a['position'];self.assertIsNotNone(p);self.assertEqual(a['entries'],1)
            self.assertLessEqual(dec(p['amount']),500*dec(ARMS[a['arm']]))
            self.assertGreater(dec(execution(levels(self.b['yes_asks']),p['quantity']+1)['amount']),500*dec(ARMS[a['arm']]))
            self.assertEqual(dec(a['cash'])+dec(p['amount']),Decimal(500))
    def test_restarts_keep_positions_and_cash(self):
        self.enter();before=self.e.status(4562);self.e.db.close();self.e=Engine(self.path,0)
        self.assertEqual(before,self.e.status(4562))
        with self.assertRaises(ValueError):Engine(self.path,1)
    def test_only_future_correct_entry_window(self):
        self.e.decision(self.h,4505,self.s)
        for t in (4559,4570,4560.1):
            with self.assertRaises(ValueError):self.e.fill(self.h,self.receipt(t),self.b,t+.1)
        self.assertEqual(sum(a['entries'] for a in self.e.status(4600)['accounts']),0)
    def test_official_settlement_loss_and_drawdown(self):
        self.enter();self.e.settle(self.m['ticker'],0,self.receipt(7205));self.e.settle(self.m['ticker'],0,self.receipt(7206))
        for a in self.e.status(7210)['accounts']:
            self.assertEqual(a['losses'],1);self.assertIsNone(a['position'])
            self.assertEqual(dec(a['realized']),dec(a['cash'])-500)
            self.assertEqual(dec(a['max_dd']),(500-dec(a['cash']))/500)
        with self.assertRaises(ValueError):self.e.settle(self.m['ticker'],1,self.receipt(7207))
    def test_early_settlement_rejected_and_hold_blocks_next_trade(self):
        self.enter()
        with self.assertRaises(ValueError):self.e.settle(self.m['ticker'],1,self.receipt(7100))
        h=7200;m=dict(self.m,hour_start=h,close=10800)
        self.e.selection(h,h+3,m);self.e.decision(h,h+905,dict(self.s,decision_time=h+900))
        self.e.fill(h,self.receipt(h+960.5),self.b,h+960.6)
        self.assertTrue(all(a['entries']==1 for a in self.e.status(h+970)['accounts']))
    def test_no_midpoint_or_insufficient_depth_fill(self):
        self.b['yes_asks']=[];self.enter()
        self.assertTrue(all(a['entries']==0 for a in self.e.status(4570)['accounts']))
    def test_stale_mark_is_missing_not_cash_nav(self):
        self.enter();books={self.m['ticker']:{'received':4561,'book':self.b}}
        s=self.e.status(4562,books);self.assertTrue(all(a['nav'] is not None for a in s['accounts']))
        self.assertTrue(all(a['nav'] is None for a in self.e.status(4580,books)['accounts']))
        self.assertTrue(all(dec(a['mark_dd'])>0 for a in s['accounts']))
    def test_unqualified_or_blocked_does_not_enter(self):
        self.e.decision(self.h,4505,self.s,'global_STOP');self.e.fill(self.h,self.receipt(),self.b,4560.6)
        self.assertTrue(all(a['entries']==0 for a in self.e.status(4570)['accounts']))
    def test_no_backfilled_hour_selection(self):
        with self.assertRaises(ValueError):self.e.selection(0,61,dict(self.m,hour_start=0))
    def test_depth_vwap_per_level_fees(self):
        e=execution([(Decimal('.20'),Decimal(2)),(Decimal('.80'),Decimal(3))],5)
        self.assertEqual(dec(e['premium']),Decimal('2.80'));self.assertEqual(dec(e['fee']),Decimal('.0560'))
        self.assertEqual(dec(e['vwap']),Decimal('.56'));self.assertIsNone(execution([(Decimal('.5'),Decimal(2))],3))
    def test_published_rounding_example(self):
        e=execution([(Decimal('.055'),Decimal(1))],1,precision=Decimal('.01'))
        self.assertEqual(dec(e['amount']),Decimal('.06'))
    def test_cash_conservation_win(self):
        self.enter();positions={a['arm']:a['position'] for a in self.e.status(4561)['accounts']}
        self.e.settle(self.m['ticker'],1,self.receipt(7205))
        for a in self.e.status(7210)['accounts']:
            p=positions[a['arm']];self.assertEqual(dec(a['cash']),500-dec(p['amount'])+p['quantity']);self.assertEqual(a['wins'],1)

if __name__=='__main__':unittest.main()
