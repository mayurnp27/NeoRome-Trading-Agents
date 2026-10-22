import json,tempfile,unittest
from pathlib import Path
from neorome.btc_hourly98_engine import Engine,dec,fee

START=1800000000 # UTC hour boundary
class EngineTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)/'hourly.sqlite3';self.e=Engine(self.path,START-1)
    def tearDown(self):self.e.close();self.tmp.cleanup()
    def market(self,venue='kalshi'):
        return {'venue':venue,'ticker':'test','hour_start':START,'close':START+3600,'fee_coefficient':'.07' if venue=='kalshi' else '.0695'}
    def book(self,p='.40',quantity='1000'):
        return {'yes_bids':[{'price':'.39','quantity':quantity}],'yes_asks':[{'price':p,'quantity':quantity}],
                'no_bids':[{'price':'.59','quantity':quantity}],'no_asks':[{'price':'.61','quantity':quantity}]}
    def enter(self,arm='kalshi-15m',side='yes',signal=START,market=None):
        market=market or self.market(arm.rsplit('-',1)[0]);self.e.admit(arm,market,side,signal,signal+1,'a'*64)
        for second in (2,4):self.e.quote(market['venue'],market['ticker'],signal+second,str(second)*64,self.book(),signal+second+.1)
    def test_four_accounts_20_percent_and_two_books(self):
        self.e.admit('kalshi-15m',self.market(),'yes',START,START+1,'a'*64)
        self.e.quote('kalshi','test',START+2,'b'*64,self.book(),START+2)
        self.assertEqual(self.e.status(START+2)['event_counts'].get('entry',0),0)
        self.e.quote('kalshi','test',START+4,'c'*64,self.book(),START+4)
        arms={a['arm']:a for a in self.e.status(START+4)['accounts']};p=arms['kalshi-15m']['position']
        self.assertLessEqual(dec(p['basis']),100);self.assertEqual(len(arms),4)
        self.assertEqual(arms['polymarket_us-15m']['cash'],'500.00')
    def test_loss_then_cash_based_sizing_and_restart(self):
        self.enter();basis=dec(self.e.status(START+4)['accounts'][0]['position']['basis'])
        self.e.settle('kalshi','test',0,START+3601,'d'*64)
        self.e.close();self.e=Engine(self.path,START-1)
        cash=dec(self.e.status(START+3602)['accounts'][0]['cash']);self.assertEqual(cash,500-basis)
        m=self.market();m.update(ticker='next',hour_start=START+3600,close=START+7200)
        self.enter(signal=START+3600,market=m)
        p=self.e.status(START+3605)['accounts'][0]['position'];self.assertLessEqual(dec(p['basis']),cash*dec('.20'))
    def test_settlement_pending_not_credited_and_idempotent(self):
        self.enter();p=self.e.status(START+4)['accounts'][0]['position']
        with self.assertRaises(ValueError):self.e.settle('kalshi','test',1,START+3599,'d'*64)
        self.assertIsNotNone(self.e.status(START+3601)['accounts'][0]['position'])
        self.e.settle('kalshi','test',1,START+3601,'d'*64);cash=self.e.status(START+3601)['accounts'][0]['cash']
        self.e.settle('kalshi','test',1,START+3602,'e'*64);self.assertEqual(self.e.status(START+3602)['accounts'][0]['cash'],cash)
        with self.assertRaises(ValueError):self.e.settle('kalshi','test',0,START+3603,'d'*64)
    def test_decimal_us_price_retained(self):
        m=self.market('polymarket_us');self.e.admit('polymarket_us-30m',m,'yes',START,START+1,'a'*64)
        b=self.book('.4355')
        for t in (2,4):self.e.quote('polymarket_us','test',START+t,str(t)*64,b,START+t)
        a=[a for a in self.e.status(START+5)['accounts'] if a['arm']=='polymarket_us-30m'][0]
        self.assertEqual(a['position']['fill_price_usd'],'0.4455');self.assertLessEqual(dec(a['position']['basis']),100)
    def test_invalid_old_signal_stale_quotes_and_price_cap(self):
        with self.assertRaises(ValueError):self.e.admit('kalshi-15m',self.market(),'yes',START-900,START+1,'a'*64)
        with self.assertRaises(ValueError):self.e.quote('kalshi','test',START,'a'*64,self.book(),START+2)
        self.e.admit('kalshi-15m',self.market(),'yes',START,START+1,'a'*64)
        for t in (2,4):self.e.quote('kalshi','test',START+t,str(t)*64,self.book('.65'),START+t)
        self.assertEqual(self.e.status(START+5)['event_counts'].get('entry',0),0)
    def test_no_entry_without_depth_or_expired(self):
        self.e.admit('kalshi-15m',self.market(),'yes',START,START+1,'a'*64)
        b=self.book(quantity='.5')
        for t in (2,4):self.e.quote('kalshi','test',START+t,str(t)*64,b,START+t)
        self.assertEqual(self.e.status(START+5)['event_counts'].get('entry',0),0)
        self.e.admit('kalshi-30m',self.market(),'yes',START,START+1,'a'*64);self.e.expire(START+90)
        self.assertEqual(self.e.status(START+91)['pending_intents'],0)

if __name__=='__main__':unittest.main()
