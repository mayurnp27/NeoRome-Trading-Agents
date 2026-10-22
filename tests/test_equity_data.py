from datetime import datetime,timedelta,timezone
import io
import json
import unittest
import urllib.error
from neorome.equity_data import EquityDataClient,DataError,normalize_bars,shadow_decision,stamp

NOW=datetime(2026,9,18,0,15,tzinfo=timezone.utc)
class Response:
    status=200
    def __init__(self,data):self.data=json.dumps(data).encode()
    def read(self,n):return self.data[:n]
    def __enter__(self):return self
    def __exit__(self,*args):pass
class Opener:
    def __init__(self,payloads):self.payloads=list(payloads);self.requests=[]
    def open(self,request,timeout):
        self.requests.append(request)
        item=self.payloads.pop(0)
        if isinstance(item,Exception):raise item
        return Response(item)
def bars():
    end=NOW.replace(hour=0,minute=0)
    return {'symbol':'SPY','next_page_token':None,'bars':[
        {'t':(end-timedelta(days=220-i)).isoformat(),'o':100+i,'h':102+i,'l':99+i,'c':101+i,'v':1000}
        for i in range(220)]}
def quote():
    return {'quotes':{'SPY':{'t':(NOW-timedelta(hours=4)).isoformat(),'bp':320,'ap':321,'bs':10,'as':10}}}
class EquityDataTests(unittest.TestCase):
    def test_nanosecond_timestamp_on_system_python(self):
        self.assertEqual(stamp('2026-09-17T20:00:00.123456789Z').microsecond,123456)
    def test_capture_read_only_and_stale_quote_no_order(self):
        opener=Opener([bars(),quote()])
        client=EquityDataClient(opener,lambda _: 'fake-secret',lambda:NOW)
        result=client.observe()
        self.assertEqual(result['request_count'],2)
        self.assertEqual(result['broker_orders'],0)
        self.assertTrue(result['shadow_decision']['trend_above_200_observation_mean'])
        self.assertTrue(any('stale' in r for r in result['shadow_decision']['reasons']))
        self.assertNotIn('fake-secret',json.dumps(result))
        for req in opener.requests:
            self.assertEqual(req.get_method(),'GET')
            self.assertTrue(req.full_url.startswith('https://data.alpaca.markets/v2/stocks/'))
            self.assertIn('feed=iex',req.full_url)
        with self.assertRaises(DataError):client.observe()
    def test_mismatched_and_paginated_fail(self):
        for change in ({'symbol':'MU'},{'next_page_token':'more'}):
            payload=bars();payload.update(change)
            with self.assertRaises(DataError):EquityDataClient(Opener([payload]),lambda _:'k',lambda:NOW).observe()
    def test_duplicate_bad_ohlc_and_nonfinite_bars_fail(self):
        for field,value in [('t',bars()['bars'][0]['t']),('h',1),('o','NaN')]:
            payload=bars();payload['bars'][1][field]=value
            with self.assertRaises(DataError):EquityDataClient(Opener([payload]),lambda _:'k',lambda:NOW).observe()
    def test_http_error_body_not_exposed(self):
        error=urllib.error.HTTPError('https://bad/secret',403,'secret',{},io.BytesIO(b'secret'))
        with self.assertRaisesRegex(DataError,'^Market data HTTP 403$'):
            EquityDataClient(Opener([error]),lambda _:'k',lambda:NOW).observe()
    def test_unknown_symbols_and_credentials_rejected_before_network(self):
        opener=Opener([])
        with self.assertRaises(DataError):EquityDataClient(opener,lambda _:'k',lambda:NOW).observe('../orders')
        with self.assertRaises(DataError):EquityDataClient(opener,lambda _:'bad\nkey',lambda:NOW).observe()
        self.assertEqual(opener.requests,[])
    def test_future_and_crossed_quote_preserved_but_ineligible(self):
        q=quote();q['quotes']['SPY'].update(t=(NOW+timedelta(seconds=1)).isoformat(),bp=322)
        result=EquityDataClient(Opener([bars(),q]),lambda _:'k',lambda:NOW).observe()
        self.assertFalse(result['shadow_decision']['eligible_for_execution'])
        self.assertTrue(any('future' in r for r in result['shadow_decision']['reasons']))
        self.assertTrue(any('crossed' in r for r in result['shadow_decision']['reasons']))
    def test_empty_history_fails(self):
        payload=bars();payload['bars']=[]
        with self.assertRaises(DataError):EquityDataClient(Opener([payload]),lambda _:'k',lambda:NOW).observe()

if __name__=='__main__':unittest.main()
