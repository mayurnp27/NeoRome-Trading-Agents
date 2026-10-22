import gzip,json,tempfile,unittest
from pathlib import Path
from datetime import datetime,timezone
from neorome.weather_dataset import run
from tests.test_weather_validation import fixture,raw,alter

class WeatherDatasetTests(unittest.TestCase):
 def test_register_before_event_pair_later_and_replay(self):
  with tempfile.TemporaryDirectory() as tmp:
   r=Path(tmp);fr=r/'forecasts';mr=r/'markets';fr.mkdir();mr.mkdir();f,o,s=fixture()
   # Actual collector files contain both response types, even when one is empty.
   empty=raw({'cursor':'','markets':[]},o['responses'][0]['requested_at'],o['collected_at'],status='settled',query={'series_ticker':'KXHIGHNY','status':'settled'},markets=[])
   opened=dict(o,responses=o['responses']+[empty])
   (fr/'20260923T00.forecast.json').write_text(json.dumps(f))
   (mr/'20260922T23.json').write_text(json.dumps(opened))
   first=run(r/'state',fr,mr,now=datetime(2026,9,23,0,10,tzinfo=timezone.utc))
   self.assertEqual(first['registrations'],1)
   later=dict(s,responses=[o['responses'][0]]+s['responses'])
   (mr/'20260924T12.json').write_text(json.dumps(later))
   second=run(r/'state',fr,mr,now=datetime(2026,9,24,12,25,tzinfo=timezone.utc))
   self.assertEqual(second['events'],1);self.assertFalse(second['paper_eligible'])
   self.assertEqual(run(r/'state',fr,mr,now=datetime(2026,9,24,12,26,tzinfo=timezone.utc))['events'],1)
   # A later receipt alone is harmless; actual held-event settlement metadata
   # revision is latched even when its payout and reported temperature match.
   later['collected_at']='2026-09-24T13:20:01Z'
   later['responses'][1]['requested_at']='2026-09-24T13:20:00Z'
   later['responses'][1]['received_at']='2026-09-24T13:20:01Z'
   (mr/'20260924T13.json').write_text(json.dumps(later))
   again=run(r/'state',fr,mr,now=datetime(2026,9,24,13,25,tzinfo=timezone.utc))
   self.assertTrue(again['operational_healthy'])
   alter(s,lambda rows:rows[0].update(settlement_ts='2026-09-24T12:01:00Z'))
   later['responses'][1]=s['responses'][0]
   later['responses'][1]['received_at']='2026-09-24T14:20:01Z'
   later['collected_at']='2026-09-24T14:20:01Z'
   (mr/'20260924T14.json').write_text(json.dumps(later))
   revised=run(r/'state',fr,mr,now=datetime(2026,9,24,14,25,tzinfo=timezone.utc))
   self.assertFalse(revised['operational_healthy']);self.assertEqual(len(revised['revision_alerts']),1)
   (mr/'20260924T14.json').unlink()
   self.assertFalse(run(r/'state',fr,mr,now=datetime(2026,9,24,15,25,tzinfo=timezone.utc))['operational_healthy'])

 def test_outside_window_does_not_register_retrospectively(self):
  with tempfile.TemporaryDirectory() as tmp:
   r=Path(tmp);(r/'f').mkdir();(r/'m').mkdir()
   result=run(r/'state',r/'f',r/'m',now=datetime(2026,9,23,4,10,tzinfo=timezone.utc))
   self.assertEqual(result['registrations'],0);self.assertEqual(result['events'],0)

 def test_incomplete_claim_is_unhealthy_without_retrospective_retry(self):
  with tempfile.TemporaryDirectory() as tmp:
   r=Path(tmp);(r/'f').mkdir();(r/'m').mkdir();a=r/'state'/'attempts';a.mkdir(parents=True)
   (a/'2026-09-23.json').write_text(json.dumps({'status':'claimed'}))
   result=run(r/'state',r/'f',r/'m',now=datetime(2026,9,23,4,10,tzinfo=timezone.utc))
   self.assertFalse(result['operational_healthy']);self.assertEqual(result['events'],0)
 def test_filename_cannot_promote_old_capture(self):
  with tempfile.TemporaryDirectory() as tmp:
   r=Path(tmp);(r/'f').mkdir();(r/'m').mkdir();f,o,s=fixture()
   (r/'m'/'99999999T99.json').write_text(json.dumps(o))
   with self.assertRaisesRegex(ValueError,'filename'):
    run(r/'state',r/'f',r/'m',now=datetime(2026,9,24,12,25,tzinfo=timezone.utc))
