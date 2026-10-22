from datetime import date,datetime,timezone
import json
import unittest
from neorome.equity_context import EquityContextClient,normalize_calendar,DataError

class Response:
    status=200
    def __init__(self,value):self.raw=json.dumps(value).encode()
    def read(self,n):return self.raw[:n]
    def __enter__(self):return self
    def __exit__(self,*args):pass
class Opener:
    def __init__(self,values):self.values=iter(values);self.requests=[]
    def open(self,req,timeout):self.requests.append(req);return Response(next(self.values))

class ContextTests(unittest.TestCase):
    def test_dst_and_early_close_are_calendar_driven(self):
        rows=[{'date':'2026-03-06','open':'09:30','close':'16:00'},
              {'date':'2026-03-09','open':'09:30','close':'13:00'}]
        result=normalize_calendar(rows,date(2026,3,1),date(2026,3,10))
        self.assertIn('14:30:00+00:00',result[0]['open_at'])
        self.assertIn('13:30:00+00:00',result[1]['open_at'])
        self.assertIn('17:00:00+00:00',result[1]['close_at'])
    def test_duplicate_or_invalid_calendar_refused(self):
        row={'date':'2026-03-06','open':'09:30','close':'16:00'}
        for rows in ([row,row],[dict(row,close='08:00')],[dict(row,date='bad')]):
            with self.assertRaises(DataError):normalize_calendar(rows,date(2026,3,1),date(2026,3,10))
    def test_readonly_context_and_redaction(self):
        op=Opener([{'symbol':'SPY','class':'us_equity','status':'active','tradable':True,'fractionable':True,'exchange':'ARCA'},
            [{'date':'2026-09-18','open':'09:30','close':'16:00'}],
            {'timestamp':'2026-09-18T14:40:00Z','is_open':True}])
        client=EquityContextClient(op,lambda _:'fake-secret',lambda:datetime(2026,9,18,14,40,tzinfo=timezone.utc))
        result=client.observe()
        self.assertNotIn('fake-secret',json.dumps(result))
        self.assertEqual(result['order_mutations'],0)
        self.assertTrue(result['asset']['fractionable'])
        self.assertEqual(len(op.requests),3)
        self.assertTrue(all(r.get_method()=='GET' and r.full_url.startswith('https://paper-api.alpaca.markets/v2/') for r in op.requests))
        with self.assertRaises(DataError):client.observe()
    def test_no_order_path(self):
        client=EquityContextClient(Opener([]),lambda _:'k')
        with self.assertRaises(DataError):client._get('/v2/orders')

if __name__=='__main__':unittest.main()
