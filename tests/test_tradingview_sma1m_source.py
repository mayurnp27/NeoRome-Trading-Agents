import asyncio,json,time,unittest
from datetime import datetime,timezone
from email.utils import format_datetime
from types import SimpleNamespace
from unittest.mock import patch
from neorome import tradingview_sma1m_source as s

class SourceTests(unittest.TestCase):
    def test_native_resolution_and_no_stochastic_dependency(self):
        now=int(time.time());target=now//60*60
        rows=[[target-60*i,100,100,100,100,1] for i in range(20,-1,-1)]
        frames=[{'session_id':'x','timestamp':now},{'m':'symbol_resolved','p':['x','symbol_1',{'full_name':s.SYMBOL,'source_id':'COINBASE','provider_id':'coinbase','name':'BTCUSD','type':'spot'}],'t':now},{'m':'timescale_update','p':['x',{'sds_1':{'s':[{'v':r} for r in rows]}}],'t':now},{'m':'series_completed','p':['x','sds_1'],'t':now}]
        class WS:
            response=SimpleNamespace(status_code=101,headers={'Date':format_datetime(datetime.now(timezone.utc),usegmt=True)})
            request=SimpleNamespace(path='/socket.io/websocket?from=chart%2F&date=',headers={'Host':'data.tradingview.com'})
            sent=[]
            async def __aenter__(self):return self
            async def __aexit__(self,*a):pass
            async def send(self,v):self.sent.append(v)
            async def recv(self):
                v=json.dumps(frames.pop(0),separators=(',',':'));return '~m~'+str(len(v))+'~m~'+v
        ws=WS()
        def factory(uri,**kwargs):
            from urllib.parse import urlsplit
            ws.request.path=urlsplit(uri).path+'?'+urlsplit(uri).query;return ws
        with patch.object(s,'_NoRedirectConnect',side_effect=factory),patch.object(s,'calculate_stochastic',side_effect=AssertionError('must not use stochastic')):
            result=asyncio.run(s.fetch_bars())
        self.assertEqual(result['timeframe'],'1m');self.assertEqual(result['bars'][-1]['bar_close_timestamp'],target)
        create=[s._decode_tv_frames(x)[0] for x in ws.sent if 'create_series' in x][0]
        self.assertEqual(create['p'][4],'1');self.assertEqual(len(result['bars']),20)
    def test_missing_minute_and_future_rows_refused(self):
        for rows in [[[60,1,1,1,1],[180,1,1,1,1]],[[1800,1,1,1,1]]]:
            with self.assertRaises(s.TradingViewSourceError):s.validate_bars(rows,1200)
    def test_forming_bar_not_used(self):
        rows=[[60*i,10,12,8,11] for i in range(1,13)]
        b=s.validate_bars(rows,12*60+10)
        self.assertEqual(b[-1]['bar_close_timestamp'],720);self.assertEqual(len(b),11)
