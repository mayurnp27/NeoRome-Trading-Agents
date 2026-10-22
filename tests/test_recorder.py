import json
import sqlite3
import tempfile
import unittest
from datetime import datetime,timezone,timedelta
from pathlib import Path
from neorome.recorder import run_once,health

NOW=datetime(2026,9,21,14,0,tzinfo=timezone.utc)
def observation(t=NOW):
    return {'raw_response':{'quotes':{'SPY':{'t':t.isoformat(),'bp':100,'ap':101,'bs':10,'as':10}}},'evidence':{'feed':'iex'}}

class RecorderTests(unittest.TestCase):
    def setUp(self):self.temp=tempfile.TemporaryDirectory();self.state=self.temp.name
    def tearDown(self):self.temp.cleanup()
    def test_persists_and_restarts_without_duplicate_request(self):
        calls=[]
        def fetch():calls.append(1);return observation()
        self.assertEqual(run_once(self.state,fetch,lambda:NOW)['status'],'fresh')
        self.assertEqual(run_once(self.state,fetch,lambda:NOW)['status'],'interval_limited')
        self.assertEqual(len(calls),1)
        with sqlite3.connect(Path(self.state)/'recorder.sqlite3') as db:
            row=db.execute('SELECT observation,sha256 FROM attempts').fetchone()
        self.assertEqual(json.loads(row[0])['raw_response']['quotes']['SPY']['t'],NOW.isoformat())
        self.assertEqual(len(row[1]),64)
    def test_stale_future_and_crossed(self):
        for i,t in enumerate((NOW-timedelta(days=2),NOW+timedelta(days=2))):
            result=run_once(self.state,lambda:observation(t),lambda:NOW+timedelta(minutes=i))
            self.assertEqual(result['status'],'stale_or_unusable')
        data=observation();data['raw_response']['quotes']['SPY']['bp']=102
        self.assertEqual(run_once(self.state,lambda:data,lambda:NOW+timedelta(minutes=2))['status'],'stale_or_unusable')
    def test_failed_request_recorded_and_message_redacted(self):
        def fail():raise RuntimeError('SECRET_TOKEN')
        self.assertEqual(run_once(self.state,fail,lambda:NOW)['status'],'fetch_or_validation_failed')
        self.assertNotIn('SECRET_TOKEN',str(health(self.state,NOW)))
        self.assertEqual(run_once(self.state,fail,lambda:NOW)['status'],'interval_limited')
    def test_stale_heartbeat(self):
        run_once(self.state,observation,lambda:NOW)
        self.assertEqual(health(self.state,NOW+timedelta(minutes=4))['status'],'heartbeat_stale')
    def test_hourly_budget_survives_restart(self):
        run_once(self.state,observation,lambda:NOW)
        with sqlite3.connect(Path(self.state)/'recorder.sqlite3') as db:
            db.executemany('INSERT INTO attempts(started_at,status) VALUES (?,?)',[(NOW.isoformat(),'incomplete')]*59)
        self.assertEqual(run_once(self.state,lambda:self.fail('Should not fetch'),lambda:NOW+timedelta(minutes=2))['status'],'request_budget_exhausted')

if __name__=='__main__':unittest.main()
