import json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from neorome.wallet_research_runtime import run
W='11111111111111111111111111111111'
class MigrationTests(unittest.TestCase):
 def base(self,d):
  p=Path(d);(p/'pages').mkdir()
  s={'wallets':{W:{'completed':True,'exclusions':[{'reason':'other_fee_payer_or_sponsored_trade'}]}},'rows':{},'seed_day':'2026-09-30','daily':{'2026-09-30':20},'poll_index':0,'ranking':{'qualified':[{'wallet':W}]}}
  (p/'state.json').write_text(json.dumps(s));return p
 def test_offline_rebuild_preserves_baseline_and_receipts(self):
  with tempfile.TemporaryDirectory() as d:
   p=self.base(d);page={'address':W,'source_sha256':'a'*64,'received_at':'2026-09-30T17:00:00+00:00'}
   (p/'pages'/('a'*64+'.json')).write_text(json.dumps(page))
   row={'wallet':W,'signature':'sig','mint':W,'side':'buy','observed_at':page['received_at'],'source_sha256':'a'*64}
   with patch('neorome.wallet_research_runtime.normalize_page',return_value={'rows':[row],'exclusions':[]}):
    result=run(d,[],reprocess_only=True)
   self.assertEqual(result['requests'],0);self.assertEqual(result['status'],'normalization_rebuild_complete')
   state=json.loads((p/'state.json').read_text());self.assertIsNone(state['ranking']);self.assertEqual(state['rows'][W+':sig:'+W+':buy']['observed_at'],page['received_at'])
   self.assertTrue((p/'state-before-wallet-swap-v2.json').exists());self.assertFalse(state['wallets'][W]['normalization_review_required'])
 def test_corrupt_archive_blocks_qualification(self):
  with tempfile.TemporaryDirectory() as d:
   p=self.base(d);(p/'pages'/('a'*64+'.json')).write_text('{')
   result=run(d,[],reprocess_only=True);self.assertEqual(result['requests'],0)
   s=json.loads((p/'state.json').read_text());self.assertTrue(s['wallets'][W]['normalization_review_required']);self.assertIsNone(s['ranking'])
 def test_incremental_rebuild_makes_no_provider_calls(self):
  class Never:
   def history(self,*args):raise AssertionError('unexpected network')
  with tempfile.TemporaryDirectory() as d:
   p=self.base(d)
   for n in range(21): (p/'pages'/(format(n,'064x')+'.json')).write_text(json.dumps({'address':'unknown'}))
   a=run(d,[],client=Never(),reprocess_only=True);self.assertEqual(a['status'],'normalization_rebuild_pending');self.assertEqual(a['requests'],0)
   b=run(d,[],client=Never(),reprocess_only=True);self.assertEqual(b['status'],'normalization_rebuild_complete');self.assertEqual(b['requests'],0)
