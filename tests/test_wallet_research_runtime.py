from datetime import datetime,timezone
import contextlib
import fcntl
import io
import tempfile,unittest,json
from pathlib import Path
from unittest.mock import patch
from neorome import wallet_research_runtime as runtime
from neorome.wallet_research_runtime import run, provider_paper_ranking

def frozen_snapshot(wallets, frozen_at='2026-09-30T12:00:00Z', day='2026-09-30'):
 return {'version':'provider-frozen-top10-v1','day':day,'source':'https://api.fomoapi.io/v2/leaderboard/7d','limit':10,'frozen_at':frozen_at,'response_sha256':'a'*64,'entries':[{'user_id':f'user-{i}','wallet':wallet,'rank':i+1,'pnl':10.0,'trades':4.0,'provider_claimed':True} for i,wallet in enumerate(wallets)]}
class ResearchTests(unittest.TestCase):
 def test_cli_busy_summary_is_structured_and_does_not_create_provider_client(self):
  with tempfile.TemporaryDirectory() as d:
   lock_path=Path(d)/'research.lock'
   with lock_path.open('a') as lock:
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    output=io.StringIO()
    with patch.object(runtime,'WalletHistoryClient') as client, contextlib.redirect_stdout(output):
     code=runtime.main(['--state-dir',d])
    self.assertEqual(code,0)
    self.assertEqual(json.loads(output.getvalue()),{'status':'busy','requests':0,'orders_sent':0})
    client.assert_not_called()

 def test_cli_preserves_runtime_fault_propagation(self):
  from neorome.wallet_history import WalletHistoryError
  with patch.object(runtime,'run',side_effect=WalletHistoryError('typed runtime fault')):
   with self.assertRaises(WalletHistoryError):
    runtime.main(['--state-dir','/unused'])

 def test_zero_budget_persists_no_calls(self):
  class Client:
   def history(self,*args):raise AssertionError('no request allowed')
  with tempfile.TemporaryDirectory() as d:
   r=run(d,['11111111111111111111111111111111'],client=Client(),now=datetime(2026,9,30,tzinfo=timezone.utc),daily_credits=0)
   self.assertEqual(r['requests'],0);self.assertEqual(r['qualified_wallets'],0)
 def test_failure_reservation_survives_restart(self):
  from neorome.wallet_history import WalletHistoryError
  class Client:
   def history(self,*args):raise WalletHistoryError('fixture provider unavailable')
  with tempfile.TemporaryDirectory() as d:
   a=run(d,['11111111111111111111111111111111'],client=Client(),now=datetime(2026,9,30,tzinfo=timezone.utc),daily_credits=10)
   b=run(d,['11111111111111111111111111111111'],client=Client(),now=datetime(2026,9,30,tzinfo=timezone.utc),daily_credits=10)
   self.assertEqual(a['requests'],1);self.assertEqual(b['requests'],0);self.assertEqual(b['daily_credits_reserved'],10)
 def test_fomo_discovery_does_not_qualify_provider_wallet(self):
  w='11111111111111111111111111111111'
  with tempfile.TemporaryDirectory() as d:
   p=Path(d);p.joinpath('state.json').write_text(json.dumps({'normalization_version':'wallet-swap-v2','wallets':{},'rows':{},'seed_day':None,'daily':{},'poll_index':0}))
   p.joinpath('fomo-state.json').write_text(json.dumps({'candidates':{'user':{'wallet':w}}}))
   r=run(d,[],client=object(),now=datetime(2026,9,30,tzinfo=timezone.utc),daily_credits=0)
   self.assertEqual(r['candidate_wallets'],1);self.assertEqual(r['qualified_wallets'],0);self.assertEqual(r['requests'],0)
 def test_event_only_no_pending_alert_does_not_spend(self):
  with tempfile.TemporaryDirectory() as d:
   p=Path(d);p.joinpath('state.json').write_text(json.dumps({'normalization_version':'wallet-swap-v2','wallets':{},'rows':{},'seed_day':None,'daily':{},'poll_index':0}))
   r=run(d,[],client=object(),now=datetime(2026,9,30,tzinfo=timezone.utc),event_only=True)
   self.assertEqual(r['requests'],0)
 def test_vetted_alert_prioritizes_chain_verification_once(self):
  from tests.test_wallet_follow import WALLET,MINTS,fill,address
  from unittest.mock import patch
  class Client:
   def __init__(self):self.calls=[]
   def history(self,address,cursor=None):
    self.calls.append((address,cursor));return {'address':address,'data':[],'source_sha256':'a'*64,'received_at':'2026-09-30T12:01:00+00:00','pagination_token':None}
  with tempfile.TemporaryDirectory() as d:
   p=Path(d);now=datetime(2026,9,30,12,1,tzinfo=timezone.utc)
   state={'normalization_version':'wallet-swap-v2','wallets':{},'rows':{},'seed_day':None,'daily':{},'poll_index':0,'ranking':{'as_of':'2026-09-30T12:00:00Z','qualified':[],'paper_qualified':[]}}
   p.joinpath('state.json').write_text(json.dumps(state));p.joinpath('fomo-state.json').write_text(json.dumps({'leaderboard_snapshot':frozen_snapshot([WALLET])}));p.joinpath('fomo-alerts.json').write_text(json.dumps({'alerts':[{'wallet':WALLET,'event_id':'event','mint':MINTS[0],'side':'buy','provider_at':'2026-09-30T12:00:30Z'}]}))
   state['wallets']={address(i):{'completed':False,'pages':1,'cursor':'historical-cursor','exclusions':[]} for i in range(400,422)}
   state['wallets'][WALLET]={'completed':False,'pages':1,'cursor':'historical-cursor','exclusions':[]}
   p.joinpath('state.json').write_text(json.dumps(state))
   c=Client()
   with patch('neorome.wallet_research_runtime.normalize_page',return_value={'rows':[fill('fresh',MINTS[0],'buy','2026-09-30T12:00:30Z')],'exclusions':[]}):
    a=run(d,[],client=c,now=now,event_only=True);b=run(d,[],client=c,now=now,event_only=True)
   self.assertEqual(c.calls,[(WALLET,None)]);self.assertEqual(a['requests'],1);self.assertEqual(b['requests'],0)

 def test_provider_ranking_rejects_missing_future_and_previous_day_snapshot(self):
  from tests.test_wallet_follow import WALLET
  with tempfile.TemporaryDirectory() as d:
   p=Path(d);now=datetime(2026,9,30,12,tzinfo=timezone.utc)
   self.assertIsNone(provider_paper_ranking(p,now))
   p.joinpath('fomo-state.json').write_text(json.dumps({'leaderboard_snapshot':frozen_snapshot([WALLET],day='2026-09-29',frozen_at='2026-09-29T12:00:00Z')}))
   self.assertIsNone(provider_paper_ranking(p,now))
   p.joinpath('fomo-state.json').write_text(json.dumps({'leaderboard_snapshot':frozen_snapshot([WALLET],frozen_at='2026-09-30T12:00:01Z')}))
   self.assertIsNone(provider_paper_ranking(p,now))

 def test_provider_rank_filters_malformed_duplicate_wallets_and_keeps_no_local_qualification(self):
  from tests.test_wallet_follow import WALLET
  with tempfile.TemporaryDirectory() as d:
   now=datetime(2026,9,30,12,tzinfo=timezone.utc)
   snap=frozen_snapshot([WALLET,WALLET,'not-a-wallet'])
   snap['entries'][1]['user_id']='duplicate-wallet'
   snap['entries'][2]['wallet']='not-a-wallet'
   Path(d,'fomo-state.json').write_text(json.dumps({'leaderboard_snapshot':snap}))
   ranking=provider_paper_ranking(d,now)
   self.assertEqual(ranking['qualified'],[])
   self.assertEqual([x['wallet'] for x in ranking['provider_leaderboard']['entries']],[WALLET])
   self.assertEqual(ranking['as_of'],'2026-09-30T12:00:00Z')

 def test_stale_buy_after_freeze_does_not_forwardfill_provider_signal(self):
  from tests.test_wallet_follow import WALLET,MINTS,fill,address
  from neorome.wallet_follow import following_signals
  with tempfile.TemporaryDirectory() as d:
   now=datetime(2026,9,30,12,3,tzinfo=timezone.utc)
   Path(d,'fomo-state.json').write_text(json.dumps({'leaderboard_snapshot':frozen_snapshot([WALLET])}))
   ranking=provider_paper_ranking(d,now)
   stale=fill('old-provider-buy',MINTS[0],'buy','2026-09-30T12:00:30Z',wallet=WALLET)
   self.assertEqual(following_signals([stale],ranking,now.isoformat(),eligibility='paper')['signals'],[])
