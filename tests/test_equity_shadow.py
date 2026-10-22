from datetime import datetime,timedelta,timezone
import json
from pathlib import Path
import tempfile
import unittest
from neorome.equity_shadow import run
from neorome.paper_ledger import PaperLedger

NOW=datetime(2026,9,18,0,30,tzinfo=timezone.utc)

def fixture():
    return {'source_kind':'observed','feed':'iex','symbol':'SPY','broker_orders':0,
        'execution_enabled':False,'observed_at':(NOW-timedelta(minutes=10)).isoformat(),
        'bars':[{'timestamp':(NOW-timedelta(days=201-i)).isoformat(),'c':str(100+i)} for i in range(200)],
        'quote':{'timestamp':(NOW-timedelta(minutes=11)).isoformat(),'bid':'300','ask':'301','bid_size':'1','ask_size':'1'},
        'shadow_decision':{'eligible_for_execution':True}}

class ShadowTests(unittest.TestCase):
    def test_recomputes_staleness_no_fills_cash_marked_and_persisted(self):
        with tempfile.TemporaryDirectory() as tmp:
            source=Path(tmp)/'fixture-observation.json';source.write_text(json.dumps(fixture()))
            result=run(source,Path(tmp)/'run',NOW)
            self.assertFalse(result['decision']['eligible_for_execution'])
            self.assertTrue(any('stale' in r for r in result['decision']['reasons']))
            self.assertEqual(result['simulated_fills'],0)
            self.assertEqual(result['paper_market_snapshot']['market_nav_cents'],80000)
            book=PaperLedger(Path(tmp)/'run/paper.sqlite3',enforce_market_risk=True,clock=lambda:NOW)
            self.assertEqual(book.market_snapshot().market_nav_cents,80000)
            self.assertEqual(sum(x.initial_cash_cents for x in book.balances()),80000)
            with self.assertRaises(FileExistsError):run(source,Path(tmp)/'run',NOW)
    def test_wrong_provenance_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            data=fixture();data['source_kind']='synthetic'
            source=Path(tmp)/'source.json';source.write_text(json.dumps(data))
            with self.assertRaises(ValueError):run(source,Path(tmp)/'run',NOW)

if __name__=='__main__':unittest.main()
