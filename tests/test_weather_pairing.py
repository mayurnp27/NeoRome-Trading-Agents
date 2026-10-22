import hashlib,json,unittest
from neorome.weather_pairing import evaluate

def inputs():
    a={'source_kind':'observed','error':None,'normalized':{'known_at':'2026-09-18T23:00:00Z','periods':[
      {'start_at':'2026-09-19T00:00:00-04:00','end_at':'2026-09-20T00:00:00-04:00'}]}}
    d={'cursor':'','markets':[{'event_ticker':'EVENT','ticker':'BIN','status':'finalized','result':'yes','settlement_value_dollars':'1','expiration_value':'69'}]}
    raw=json.dumps(d)
    return a,{'raw':raw,'sha256':hashlib.sha256(raw.encode()).hexdigest(),'received_at':'2026-09-20T12:00:00Z'}
class PairTests(unittest.TestCase):
    def check(self,a,o):return evaluate(a,o,target_date='2026-09-19',event_ticker='EVENT')
    def test_good_timing_still_requires_mapping(self):
        r=self.check(*inputs());self.assertTrue(r['temporal_and_label_checks_pass']);self.assertFalse(r['calibration_eligible'])
    def test_late_forecast_rejected(self):
        a,o=inputs();a['normalized']['known_at']='2026-09-20T03:00:00Z'
        self.assertFalse(self.check(a,o)['temporal_and_label_checks_pass'])
    def test_coverage_gap(self):
        a,o=inputs();a['normalized']['periods'][0]['start_at']='2026-09-19T01:00:00-04:00'
        self.assertIn('Forecast does not cover the entire target local day',self.check(a,o)['rejections'])
    def test_hash_tamper(self):
        a,o=inputs();o['raw']+=' '
        with self.assertRaises(ValueError):self.check(a,o)
    def test_nonbinary_fallback(self):
        a,o=inputs();d=json.loads(o['raw']);d['markets'][0]['settlement_value_dollars']='0.4'
        o['raw']=json.dumps(d);o['sha256']=hashlib.sha256(o['raw'].encode()).hexdigest()
        self.assertFalse(self.check(a,o)['temporal_and_label_checks_pass'])
if __name__=='__main__':unittest.main()
