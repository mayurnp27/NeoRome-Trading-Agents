from datetime import datetime,timezone
import tempfile,unittest
from pathlib import Path
from neorome.wallet_follow_paper import run,WalletSources
from tests.test_wallet_research_runtime import frozen_snapshot
from tests.test_wallet_follow import WALLET
import json
class PaperBridgeTests(unittest.TestCase):
 def test_empty_signal_still_manages_existing_positions(self):
  calls=[]
  def executor(**kwargs):calls.append(kwargs);return {'status':'abstain','orders_sent':0}
  with tempfile.TemporaryDirectory() as d:
   r=run(d,executor=executor,now=datetime(2026,9,30,tzinfo=timezone.utc))
   self.assertEqual(r['strategy'],'W1-wallet-follow-v3');self.assertEqual(calls[0]['entry_cap_cents'],1500)
   self.assertEqual(calls[0]['sources'].profiles()[0],[])
   self.assertEqual(calls[0]['ledger_path'],Path(d)/'paper.sqlite3')
 def test_selection_only_wallet_mints_and_provenance(self):
  s=WalletSources({'signals':[{'mint':'11111111111111111111111111111111'}],'ranking_cutoff':'prior'})
  p,e=s.profiles();self.assertEqual(len(p),1);self.assertEqual(e['wallet_follow_evidence']['ranking_cutoff'],'prior')

 def test_vetted_fresh_buy_reaches_paper_executor(self):
  import json
  from tests.test_wallet_follow import qualifying_rows,fill,MINTS,CUTOFF
  from neorome.wallet_follow import score_wallets
  historical=qualifying_rows();rank=score_wallets(historical,CUTOFF)
  self.assertEqual(len(rank['qualified']),1)
  fresh=fill('fresh-vetted-buy',MINTS[12],'buy','2026-09-30T12:00:30Z')
  calls=[]
  def executor(**kwargs):
   calls.append(kwargs)
   profiles,witness=kwargs['sources'].profiles()
   self.assertEqual([p['tokenAddress'] for p in profiles],[MINTS[12]])
   self.assertEqual(witness['wallet_follow_evidence']['signals'][0]['signature'],'fresh-vetted-buy')
   return {'status':'paper_fixture_execution_invoked','orders_sent':0}
  with tempfile.TemporaryDirectory() as d:
   Path(d,'state.json').write_text(json.dumps({'ranking':rank,'rows':{'fresh':fresh},'normalization_version':'wallet-swap-v2'}))
   Path(d,'fomo-state.json').write_text(json.dumps({'leaderboard_snapshot':frozen_snapshot([WALLET])}))
   report=run(d,executor=executor,now=datetime(2026,9,30,12,1,tzinfo=timezone.utc))
   self.assertEqual(len(calls),1)
   self.assertEqual(len(report['wallet_signals']['signals']),1)
   self.assertEqual(calls[0]['crypto_sleeve_cents'],30000)
   self.assertEqual(calls[0]['cohort'],'E1')
 def test_provisional_frozen_cohort_can_select_signal_for_paper(self):
  import json
  from tests.test_wallet_follow import WALLET,MINTS,fill,CUTOFF
  calls=[]
  def executor(**kwargs):
   calls.append(kwargs)
   profiles,witness=kwargs['sources'].profiles()
   self.assertEqual([p['tokenAddress'] for p in profiles],[MINTS[12]])
   self.assertEqual(witness['wallet_follow_evidence']['eligibility'],'provider_top10')
   return {'status':'paper_fixture_execution_invoked','orders_sent':0}
  ranking={'as_of':CUTOFF,'qualified':[],'paper_qualified':[{'wallet':WALLET,'quote_mint':'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v','paper_warnings':['history_or_excluded_activity_review_required']}],'paper_evidence_label':'provisional_wallet_history'}
  fresh=fill('provisional-fresh-buy',MINTS[12],'buy','2026-09-30T12:00:30Z',wallet=WALLET)
  with tempfile.TemporaryDirectory() as d:
   Path(d,'state.json').write_text(json.dumps({'ranking':ranking,'rows':{'fresh':fresh},'normalization_version':'wallet-swap-v2'}))
   Path(d,'fomo-state.json').write_text(json.dumps({'leaderboard_snapshot':frozen_snapshot([WALLET])}))
   report=run(d,executor=executor,now=datetime(2026,9,30,12,1,tzinfo=timezone.utc))
   self.assertEqual(len(report['wallet_signals']['signals']),1)
   self.assertEqual(report['wallet_eligibility'],'provider_top10')

class WalletSourceBoundaryTests(unittest.TestCase):
 def test_general_discovery_never_calls_network_even_with_no_signals(self):
  def forbidden(*args, **kwargs):
   raise AssertionError('W1 must not discover general market tokens')
  source=WalletSources({'signals': []}, opener=forbidden)
  self.assertEqual(source.profiles()[0], [])
  for method in (source.trending_pools, source.new_pools):
   payload,receipt=method()
   self.assertEqual(payload, {'data': []})
   self.assertEqual(receipt['network_requests'],0)
   self.assertEqual(receipt['reason'],'wallet_only_source_boundary')
 def test_legacy_mixed_source_learning_is_not_used(self):
  calls=[]
  def executor(**kwargs):
   calls.append(kwargs);return {'status':'abstain','orders_sent':0}
  with tempfile.TemporaryDirectory() as d:
   report=run(d,executor=executor,now=datetime(2026,9,30,tzinfo=timezone.utc))
   self.assertIsNone(calls[0]['learning_report_path'])
   self.assertEqual(report['learning_status'],'quarantined_legacy_mixed_source_outcomes')
