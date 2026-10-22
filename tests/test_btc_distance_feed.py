import datetime as dt,json,tempfile,unittest
from unittest.mock import patch
from pathlib import Path
from neorome.btc_distance_feed import Runner,validate_market,official_result,book,iso,candles,get,ROOT,discover

class FeedTests(unittest.TestCase):
    def setUp(self):
        self.h=3600;self.m={'ticker':'KXBTCD-TEST-T100','event_ticker':'KXBTCD-TEST','exchange_index':2,'strike_type':'greater',
            'floor_strike':100,'open_time':iso(0),'created_time':iso(0),'close_time':iso(7200),'rules_primary':'BRTI preceding sixty seconds','status':'active'}
    def test_market_identity_and_created_before_start(self):
        validate_market(self.m,self.h)
        for updates in ({'created_time':iso(3601)},{'close_time':iso(10800)},{'exchange_index':0},{'strike_type':'between'},{'rules_primary':'spot close'}):
            with self.assertRaises(ValueError):validate_market(dict(self.m,**updates),self.h)
    def test_official_result_only_after_final_settlement(self):
        market={'hour_start':3600,'ticker':self.m['ticker'],'strike':'100','close':7200}
        self.assertIsNone(official_result(self.m,market,7300))
        m=dict(self.m,status='finalized',result='no',settlement_ts=iso(7205),expiration_value='100')
        self.assertEqual(official_result(m,market,7300),0)
        with self.assertRaises(ValueError):official_result(dict(m,result='yes'),market,7300)
        with self.assertRaises(ValueError):official_result(m,market,7204)
    def test_complement_and_decimal_depth(self):
        b=book({'data':{'orderbook_fp':{'yes_dollars':[['.41','10.25']],'no_dollars':[['.58','2.00']]}}})
        self.assertEqual(b['yes_asks'],[{'price':'0.42','quantity':'2.00'}])
    def test_no_order_or_private_routes(self):
        for url in ('https://external-api.kalshi.com/trade-api/v2/portfolio/orders','https://evil.example/data','https://external-api.kalshi.com/trade-api/v2/markets/KXBTC15M-ABC'):
            with self.assertRaises(ValueError):get(url)
    def test_candles_exclude_incomplete_and_reject_revisions(self):
        d=180000;r=[d-300,90,110,100,101,1];future=[d,90,110,100,102,1]
        with patch('neorome.btc_distance_feed.candle_chunk',side_effect=[{'data':[r]},{'data':[r,future]}]):
            rows,_=candles(d);self.assertEqual(rows,[r])
        with patch('neorome.btc_distance_feed.candle_chunk',side_effect=[{'data':[r]},{'data':[[d-300,90,110,100,102,1]]}]):
            with self.assertRaises(ValueError):candles(d)
    def test_startup_next_full_hour_no_catchup(self):
        with tempfile.TemporaryDirectory(prefix='neorome-btc-distance-') as folder:
            with patch('neorome.btc_distance_feed.time.time',return_value=4000):r=Runner(Path(folder))
            self.assertEqual(r.next_hour,7200)
            with patch('neorome.btc_distance_feed.time.time',return_value=4504),patch('neorome.btc_distance_feed.discover') as discover:
                r.tick();discover.assert_not_called()
            self.assertEqual(r.engine.db.execute('select count(*) from decisions').fetchone()[0],0);r.engine.db.close()
    def test_preflight_cannot_target_production(self):
        with self.assertRaises(ValueError):Runner(ROOT,True)
    def test_empty_top_level_market_list_uses_nested_event(self):
        import datetime as dt
        from zoneinfo import ZoneInfo
        event='KXBTCD-'+dt.datetime.fromtimestamp(self.h+3600,ZoneInfo('America/New_York')).strftime('%y%b%d%H').upper()
        m=dict(self.m,event_ticker=event,ticker=event+'-T100')
        payload={'event':{'event_ticker':event,'series_ticker':'KXBTCD','exchange_index':2,'strike_date':iso(7200),'markets':[m]},'markets':[]}
        with patch('neorome.btc_distance_feed.get',return_value={'data':payload}),patch('neorome.btc_distance_feed.candle_chunk',return_value={'data':[[3300,90,110,100,100,1]]}),patch('neorome.btc_distance_feed.fees',return_value=[]):
            selected,_=discover(self.h);self.assertEqual(selected['ticker'],m['ticker'])
    def test_iso_signal_integration(self):
        from neorome.btc_distance_engine import Engine
        with tempfile.TemporaryDirectory() as d:
            e=Engine(Path(d)/'a.db',0);e.selection(3600,3603,{'hour_start':3600})
            e.decision(3600,4504,{'decision_time':iso(4500),'qualifies':False,'signal':0});e.db.close()

if __name__=='__main__':unittest.main()
