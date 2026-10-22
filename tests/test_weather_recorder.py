import tempfile,unittest
from datetime import datetime,timezone,timedelta
from neorome.weather_recorder import run
class WeatherRecorderTests(unittest.TestCase):
    def test_hourly_attempts_persist_including_failures(self):
        now=datetime.now(timezone.utc)
        with tempfile.TemporaryDirectory() as p:
            r=run(p,lambda:{'normalized':{'periods':[1]},'error':None},now)
            self.assertEqual(r['status'],'recorded')
            self.assertEqual(run(p,lambda:self.fail('duplicate fetch'),now)['status'],'already_attempted')
            def fail():raise RuntimeError('secret')
            self.assertEqual(run(p,fail,now+timedelta(hours=1))['status'],'source_failed')
            self.assertEqual(run(p,lambda:self.fail('retry'),now+timedelta(hours=1))['status'],'already_attempted')
if __name__=='__main__':unittest.main()
