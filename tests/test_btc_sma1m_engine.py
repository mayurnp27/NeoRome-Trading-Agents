import tempfile,unittest,json
from pathlib import Path
from neorome.btc_sma1m_engine import SMAEngine
from neorome.btc_bracket_engine import BracketError
from tests.test_btc_trailing_engine import quote,digest

class SMAEngineTests(unittest.TestCase):
    def setUp(self):self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)/'sma.sqlite3';self.e=SMAEngine(self.path,started_at=0)
    def tearDown(self):self.e.close();self.tmp.cleanup()
    def cohort(self,tf,*,at=1,side='yes',cid=None):
        return {'cohort_id':cid or tf+':0','timeframe':tf,'ticker':'BTC','side':side,'observed_at':at,'signal_time':0 if at==1 else at,'close_time':100,'fee_multiplier':'1','source_hash':digest(tf+str(at))}
    def enter(self,tf):
        self.e.register_cohort(self.cohort(tf));self.e.on_quote('BTC',quote(2,'first'));self.e.on_quote('BTC',quote(4,'second'))
        return next(a for a in self.e.status(4)['arms'] if a['timeframe']==tf)
    def test_four_independent_twenty_percent_accounts_and_no_hold_exit(self):
        self.assertEqual(len(self.e.status(0)['arms']),4)
        for tf,_ in self.e.ARMS:self.e.register_cohort(self.cohort(tf))
        self.e.on_quote('BTC',quote(2,'first'));self.e.on_quote('BTC',quote(4,'second'))
        self.e.on_reversal('no',8,digest('down'),8)
        self.e.on_quote('BTC',quote(9,'exit',bid=30))
        for a in self.e.status(9)['arms']:
            self.assertEqual(a['initial_cash_cents'],50000)
            if a['timeframe'].endswith('reversal'):self.assertIsNone(a['position']);self.assertEqual(a['trades'],1)
            else:self.assertIsNotNone(a['position']);self.assertEqual(a['position']['allocation_bps'],2000);self.assertLessEqual(a['open_cost_cents'],10000)
    def test_reversal_cannot_fill_early_then_reenter_same_quote(self):
        tf='1m-sma-reversal';a=self.enter(tf);q=a['position']['quantity']
        self.e.on_reversal('no',8,digest('down'),8)
        self.e.register_cohort(self.cohort(tf,at=8,side='no',cid='next'))
        self.e.on_quote('BTC',quote(8.5,'early',bid=30));a=next(a for a in self.e.status(8.5)['arms'] if a['timeframe']==tf);self.assertEqual(a['position']['quantity'],q)
        self.e.on_quote('BTC',quote(10.5,'sell',bid=30));a=next(a for a in self.e.status(10.5)['arms'] if a['timeframe']==tf);self.assertIsNone(a['position']);cash=a['available_cash_cents']
        self.e.on_quote('BTC',quote(11.5,'buy',bid=30));a=next(a for a in self.e.status(11.5)['arms'] if a['timeframe']==tf);self.assertEqual(a['position']['side'],'no');self.assertLessEqual(a['position']['cost_cents'],cash*2000//10000)
    def test_partial_exit_and_restart_keep_trigger_and_hash_then_official_residual(self):
        tf='1m-sma-reversal';a=self.enter(tf);q=a['position']['quantity'];cost=a['position']['cost_cents']
        self.e.on_reversal('no',8,digest('down'),8);self.e.on_quote('BTC',quote(9,'partial',bid=30,bid_qty=3));self.e.close();self.e=SMAEngine(self.path,started_at=0)
        self.e.on_quote('BTC',quote(10,'partial',bid=30,bid_qty=10000));a=next(a for a in self.e.status(10)['arms'] if a['timeframe']==tf);self.assertEqual(a['position']['quantity'],q-3)
        accrued=a['position']['accrued_net_proceeds_cents'];self.e.on_settlement('BTC','yes',100,digest('settled'));a=next(a for a in self.e.status(100)['arms'] if a['timeframe']==tf);self.assertEqual(a['realized_pnl_cents'],accrued+(q-3)*100-cost)
    def test_thirty_second_entry_deadline_and_invalid_cohort(self):
        self.e.register_cohort(self.cohort('1m-sma'));self.e.on_quote('BTC',quote(2,'first'));self.e.on_quote('BTC',quote(30,'late'))
        self.assertTrue(all(a['position'] is None for a in self.e.status(30)['arms']))
        with self.assertRaises(BracketError):self.e.register_cohort(self.cohort('1m-sma',at=31)|{'signal_time':0})
    def test_same_side_signal_does_not_duplicate_position(self):
        tf='1m-sma';self.enter(tf);self.e.register_cohort(self.cohort(tf,at=8,cid='same-side'))
        kinds=[(x['kind'],json.loads(x['payload'])) for x in self.e.db.execute('select kind,payload from events')]
        self.assertTrue(any(k=='skip' and p['reason']=='position_already_open' for k,p in kinds))

if __name__=='__main__':unittest.main()
