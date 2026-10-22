import json,tempfile,unittest
from pathlib import Path
from datetime import timedelta
from unittest.mock import patch
from neorome.firm_paper import build_adapters,run_once
from neorome.paper_ledger import PaperLedger
from tests.test_equity_shared import NOW,bundle,corporate_evidence,later_quote

class FirmIntegrationTests(unittest.TestCase):
 def test_crypto_entries_disabled_by_default_but_equity_cycle_remains_enabled(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);ledger=root/'paper.sqlite3';PaperLedger(ledger,enforce_market_risk=True,clock=lambda:NOW)
   events=[]
   def equity_cycle(now,allow_entry):
    events.append(('equity',allow_entry))
    return {'operational_healthy':True,'desk_status':'waiting_for_evidence'}
   def pilot_runner(**kwargs):
    events.append(('crypto',kwargs['allow_entry']))
    return {'actions':[],'blocker':'entries_disabled'}
   with patch('neorome.firm_paper.build_adapters',return_value=(lambda book,now:[],equity_cycle)):
    result=run_once(state=root/'runtime',ledger=ledger,btc_state=root/'btc',
                    pilot_state=root/'pilot',now=NOW,
                    quote={'symbol':'BTC/USD','bid':100,'ask':101,'bid_depth':1,'ask_depth':1,
                           'quote_time':NOW.isoformat().replace('+00:00','Z')},
                    quote_source={'sha256':'a'*64},require_recovery=False,
                    pilot_runner=pilot_runner)
   self.assertEqual(events,[('equity',True),('crypto',False)])
   self.assertTrue(result['operational_healthy'],result)

 def test_real_adapter_reserves_fills_and_marks_same_book(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);state=root/'runtime';ledger=root/'paper.sqlite3'
   (root/'daily/runs').mkdir(parents=True);(root/'actions/evidence').mkdir(parents=True)
   (root/'daily/runs'/f'{NOW.date()}.json').write_text(json.dumps(bundle()))
   (root/'actions/evidence'/f'{NOW.date()}.json').write_text(json.dumps(corporate_evidence()))
   provider,cycle=build_adapters(state=state,ledger=ledger,daily_root=root/'daily',actions_root=root/'actions',spy_state=root/'spy',btc_state=root/'btc')
   with patch('neorome.firm_paper.equity_quote',return_value=later_quote(NOW)):
    first=cycle(NOW,True)
   self.assertTrue(first['operational_healthy'],first)
   self.assertEqual(first['actions'][0]['type'],'reserve')
   later=NOW+timedelta(seconds=11)
   with patch('neorome.firm_paper.equity_quote',return_value=later_quote(later)):
    second=cycle(later,True)
    book=PaperLedger(ledger,enforce_market_risk=True,clock=lambda:later)
    marks=provider(book,later)
   self.assertTrue(second['operational_healthy'],second)
   self.assertEqual(second['actions'][0]['type'],'simulated_fill')
   self.assertEqual(len(marks),1)
 def test_empty_inputs_wait_without_fabricated_entry(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d)
   _,cycle=build_adapters(state=root/'state',ledger=root/'paper.sqlite3',daily_root=root/'daily',actions_root=root/'actions',spy_state=root/'spy',btc_state=root/'btc')
   result=cycle(NOW,True)
   self.assertTrue(result['operational_healthy'],result)
   self.assertEqual(result['desk_status'],'waiting_for_evidence')
   self.assertEqual(result['actions'],[])
