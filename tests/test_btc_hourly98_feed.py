import asyncio,json,tempfile,time,unittest
from pathlib import Path
from unittest.mock import patch
from neorome import btc_hourly98_feed as f

START=1800000000
class FeedTests(unittest.TestCase):
    def setUp(self):self.tmp=tempfile.TemporaryDirectory(dir=str(Path(tempfile.gettempdir()).resolve()));self.root=Path(self.tmp.name)/'new'
    def tearDown(self):self.tmp.cleanup()
    def market(self,v):return {'venue':v,'ticker':'test-'+v,'hour_start':START,'close':START+3600,'fee_coefficient':'.07'}
    def runner(self,data_only=False):
        with patch.object(f.time,'time',return_value=START-1):return f.Runner(self.root,data_only)
    def decision(self):return {'kind':'chart','tf':15,'target':START,'received':START+3,'signal':{'direction':'yes'},'markets':{},'blocker':None}
    def seed(self,r,p):
        key='15:'+str(START);r.engine.db.execute('insert into decisions values(?,?,?,0)',(key,START+3,f.canonical(p)));return key
    def test_fixed_strike_selection_and_listing(self):
        def m(strike,ticker='KXBTCD-TEST'):
            return {'ticker':ticker,'exchange_index':2,'strike_type':'greater','floor_strike':strike,'open_time':f.iso(START-1000),'close_time':f.iso(START+3600),'status':'active','rules_primary':'BRTI above strike'}
        choices=[m(101),m(99,'KXBTCD-LOWER')];self.assertEqual(f.choose_kalshi(choices,START,f.dec(100))['floor_strike'],99)
        choices[1]['open_time']=f.iso(START+1);self.assertEqual(f.choose_kalshi(choices,START,f.dec(100))['floor_strike'],101)
    def test_us_decimal_and_synthetic_no(self):
        md={'bids':[{'px':{'value':'.4355','currency':'USD'},'qty':'3.25'}],'offers':[{'px':{'value':'.4401','currency':'USD'},'qty':'2.5'}]}
        b=f.us_book(md);self.assertEqual(b['no_asks'][0]['price'],'0.5645');self.assertEqual(b['yes_bids'][0]['quantity'],'3.25')
        md['offers'][0]['px']['currency']='EUR'
        with self.assertRaises(ValueError):f.us_book(md)
    def test_kalshi_decimal_book(self):
        b=f.kalshi_book({'data':{'orderbook_fp':{'yes_dollars':[['.40','2.00']],'no_dollars':[['.59','3']]}}})
        self.assertEqual(b['yes_asks'][0]['price'],'0.41')
    def test_auth_freshness(self):
        p=Path(self.tmp.name)/'headers';p.write_text(json.dumps({'X-PM-Access-Key':'test','X-PM-Timestamp':str(int(time.time()*1000)),'X-PM-Signature':'test'}));self.assertIn('X-PM-Signature',f.auth_headers(p))
        p.write_text(json.dumps({'X-PM-Access-Key':'test','X-PM-Timestamp':'1','X-PM-Signature':'test'}))
        with self.assertRaises(ValueError):f.auth_headers(p)
    def test_pending_venues_admitted_separately_and_idempotent(self):
        r=self.runner();p=self.decision();key=self.seed(r,p)
        r.markets['kalshi']=self.market('kalshi');r.metadata_at['kalshi']=START+3
        with patch.object(f.time,'time',return_value=START+5),patch.object(f,'blocker',return_value=None):r.apply_decision(key,p)
        self.assertEqual(r.engine.db.execute('select count(*) from intents').fetchone()[0],1)
        self.assertEqual(r.engine.db.execute('select applied from decisions').fetchone()[0],0)
        r.markets['polymarket_us']=self.market('polymarket_us');r.metadata_at['polymarket_us']=START+6
        with patch.object(f.time,'time',return_value=START+7),patch.object(f,'blocker',return_value=None):
            r.apply_decision(key,p);r.apply_decision(key,p)
        self.assertEqual(r.engine.db.execute('select count(*) from intents').fetchone()[0],2)
        self.assertEqual(r.engine.db.execute('select applied from decisions').fetchone()[0],1);r.engine.close()
    def test_data_only_never_admits_and_old_signal_never_backfills(self):
        r=self.runner(data_only=True);p=self.decision();key=self.seed(r,p)
        with patch.object(f.time,'time',return_value=START+5):r.apply_decision(key,p)
        self.assertEqual(r.engine.db.execute('select count(*) from intents').fetchone()[0],0);r.engine.close()
    def test_expired_pending_classifies_missing_venues_without_entry(self):
        r=self.runner();p=self.decision();key=self.seed(r,p)
        with patch.object(f.time,'time',return_value=START+91):r.apply_decision(key,p)
        self.assertEqual(r.engine.db.execute('select applied from decisions').fetchone()[0],1)
        self.assertEqual(r.engine.db.execute('select count(*) from intents').fetchone()[0],0);r.engine.close()
    def test_recovery_guard_and_protected_roots(self):
        recovery=Path(self.tmp.name)/'recovery';stop=Path(self.tmp.name)/'STOP'
        recovery.write_text(json.dumps({'operational_healthy':True,'updated_at':f.iso(time.time()-1)}))
        with patch.object(f,'RECOVERY',recovery),patch.object(f,'STOP',stop):
            self.assertIsNone(f.blocker());stop.touch();self.assertEqual(f.blocker(),'global_STOP')
        with self.assertRaises(ValueError):f.validate_root('/var/lib/neorome-btc-brackets-v1/child')
    def test_budget_survives_restart(self):
        p=Path(self.tmp.name)/'budget';b=f.Budget(p);b.reserve();self.assertEqual(f.Budget(p).state['requests'],1)
    def test_cached_historical_selector_must_postdate_bar_close(self):
        market={'ticker':'KXBTCD-TEST','exchange_index':2,'strike_type':'greater','floor_strike':100,'open_time':f.iso(START-1000),'close_time':f.iso(START+3600),'status':'active','rules_primary':'BRTI above strike'}
        candle={'data':[[START-300,99,101,100,100,5]],'http_date':f.dt.datetime.fromtimestamp(START+100,f.UTC).strftime('%a, %d %b %Y %H:%M:%S GMT'),'cache_age':'50'}
        def request(url,*args):
            if '/candles?' in url:
                self.assertEqual(args,(60,3600));return candle
            if '/series/' in url:return {'data':{'series':{'ticker':'KXBTCD','exchange_index':2,'fee_type':'quadratic','fee_multiplier':1}}}
            ticker='KXBTCD-'+f.dt.datetime.fromtimestamp(START+3600,f.ZoneInfo('America/New_York')).strftime('%y%b%d%H').upper()
            return {'data':{'markets':[market],'event':{'event_ticker':ticker,'series_ticker':'KXBTCD','exchange_index':2,'strike_date':f.iso(START+3600)}}}
        with patch.object(f,'get',side_effect=request):
            result=f.discover_kalshi(START);self.assertEqual(result['market']['reference_response_time'],START+50)
            candle['cache_age']='150'
            with self.assertRaises(ValueError):f.discover_kalshi(START)
    def test_idle_book_does_not_write_ledger_but_future_entry_books_do(self):
        r=self.runner();row={'venue':'kalshi','ticker':'test-kalshi','received':START+4}
        self.assertFalse(r.needs_quote_accounting(row))
        r.engine.admit('kalshi-15m',self.market('kalshi'),'yes',START,START+1,'a'*64)
        self.assertTrue(r.needs_quote_accounting(row))
        book={side:[] for side in ('yes_bids','yes_asks','no_bids','no_asks')}
        r.engine.quote('kalshi','test-kalshi',START+4,'b'*64,book,START+4)
        self.assertFalse(r.needs_quote_accounting(dict(row,received=START+5.99)))
        self.assertTrue(r.needs_quote_accounting(dict(row,received=START+6)));r.engine.close()
    def test_public_http_reuses_connection_and_never_follows_redirect(self):
        class Response:
            status=200
            @property
            def headers(self):return {'Date':f.dt.datetime.now(f.UTC).strftime('%a, %d %b %Y %H:%M:%S GMT'),'Age':'0'}
            def read(self,n):return b'{"data":1}'
        class Connection:
            def __init__(self,*args,**kwargs):self.requests=[];self.status=200
            def request(self,method,path,headers):self.requests.append((method,path));self.response=Response();self.response.status=self.status
            def getresponse(self):return self.response
            def close(self):pass
        f._HTTP_LOCAL.connections={}
        with patch.object(f.http.client,'HTTPSConnection',side_effect=Connection) as factory:
            url=f.KAPI+'/series/KXBTCD';f.get(url);f.get(url);self.assertEqual(factory.call_count,1)
            connection=f._HTTP_LOCAL.connections['external-api.kalshi.com'];connection.status=302
            with self.assertRaises(ValueError):f.get(url)
            self.assertNotIn('external-api.kalshi.com',f._HTTP_LOCAL.connections)

if __name__=='__main__':unittest.main()
