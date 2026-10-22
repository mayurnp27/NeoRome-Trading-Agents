import io,json,unittest,urllib.error
from datetime import datetime,timezone
from neorome.options_data_probe import probe
class Response(io.BytesIO):
    status=200
class Fake:
    def __init__(self,malformed=False):self.requests=[];self.malformed=malformed
    def open(self,request,timeout):
        self.requests.append(request)
        if 'feed=opra' in request.full_url:raise urllib.error.HTTPError(request.full_url,403,'Forbidden',{},None)
        return Response(b'bad' if self.malformed else json.dumps({'snapshots':{'EXAMPLE':{}},'next_page_token':'more'}).encode())
class ProbeTests(unittest.TestCase):
    def test_bounded_filtered_readonly_and_redacted(self):
        f=Fake();r=probe(f,lambda _: 'fixture-secret',datetime(2026,9,20,tzinfo=timezone.utc))
        self.assertEqual(len(f.requests),2)
        self.assertTrue(all(x.get_method()=='GET' and 'limit=2' in x.full_url and '/snapshots/SPY?' in x.full_url for x in f.requests))
        self.assertEqual(r['results'][0]['http_status'],403)
        self.assertEqual(r['results'][1]['contract_count'],1)
        self.assertTrue(r['results'][1]['pagination_pending'])
        self.assertNotIn('fixture-secret',json.dumps(r));self.assertFalse(r['paper_eligible'])
    def test_malformed_success_not_access_verified(self):
        r=probe(Fake(True),lambda _:'fixture')
        self.assertEqual(r['results'][1]['access_result'],'failed')
    def test_bad_credentials_never_send(self):
        f=Fake()
        with self.assertRaises(ValueError):probe(f,lambda _:'bad\nsecret')
        self.assertEqual(f.requests,[])
if __name__=='__main__':unittest.main()
