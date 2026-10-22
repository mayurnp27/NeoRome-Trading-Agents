import base64
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from neorome.weather_vintage import collect, normalize, save_new, NoRedirect

NOW = datetime(2026,9,19,12,tzinfo=timezone.utc)
URL = 'https://api.weather.gov/gridpoints/OKX/33,37/forecast/hourly'
def forecast():
    return {'properties':{'updateTime':'2026-09-19T10:00:00Z','generatedAt':'2026-09-19T11:00:00Z',
        'periods':[{'startTime':'2026-09-19T12:00:00Z','endTime':'2026-09-19T13:00:00Z',
                    'temperature':70,'temperatureUnit':'F'}]}}
class WeatherArchiveTests(unittest.TestCase):
    def fetch(self,url):
        return 200,{'Date':'example'},json.dumps({'properties':{'forecastHourly':URL}} if '/points/' in url else forecast()).encode()
    def test_receipt_not_issue_hash_and_raw(self):
        r=collect(self.fetch,lambda:NOW)
        self.assertIsNone(r['error']);self.assertEqual(r['request_responses'],2)
        self.assertEqual(r['normalized']['known_at'],NOW.isoformat())
        self.assertNotEqual(r['normalized']['known_at'],r['normalized']['provider_timestamps']['updateTime'])
        for c in r['captures']:
            self.assertEqual(hashlib.sha256(base64.b64decode(c['raw_base64'])).hexdigest(),c['sha256'])
        self.assertFalse(r['paper_eligible'])
    def test_hostile_discovery_is_not_fetched(self):
        for url in ['https://evil.test/forecast/hourly',URL+'?x=1',URL+'#x',URL.replace('api.weather.gov','api.weather.gov:443')]:
            calls=[]
            def fetch(u):
                calls.append(u);return 200,{},json.dumps({'properties':{'forecastHourly':url}}).encode()
            r=collect(fetch,lambda:NOW)
            self.assertEqual(len(calls),1);self.assertIsNotNone(r['error'])
    def test_invalid_forecasts(self):
        for kind in ['future','unit','nan','overlap','naive']:
            f=forecast();p=f['properties']
            if kind=='future':p['updateTime']='2026-09-20T00:00:00Z'
            if kind=='unit':p['periods'][0]['temperatureUnit']='C'
            if kind=='nan':p['periods'][0]['temperature']=float('nan')
            if kind=='overlap':p['periods']*=2
            if kind=='naive':p['updateTime']='2026-09-19T10:00:00'
            with self.subTest(kind=kind),self.assertRaises(ValueError):normalize(f,NOW.isoformat())
    def test_bad_forecast_preserves_raw_evidence(self):
        def fetch(url):
            if '/points/' in url:return self.fetch(url)
            return 200,{},b'not json'
        r=collect(fetch,lambda:NOW)
        self.assertEqual(len(r['captures']),2);self.assertIsNotNone(r['error'])
    def test_exclusive_output_and_redirect(self):
        r=collect(self.fetch,lambda:NOW)
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'capture.json';save_new(p,r)
            self.assertEqual(p.stat().st_mode & 0o777,0o600)
            with self.assertRaises(FileExistsError):save_new(p,r)
        self.assertIsNone(NoRedirect().redirect_request(None,None,None,None,None,None))
if __name__=='__main__':unittest.main()
