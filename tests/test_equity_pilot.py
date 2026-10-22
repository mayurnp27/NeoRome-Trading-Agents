from datetime import datetime,timedelta,timezone
import json
from pathlib import Path
import tempfile
import unittest
from neorome.equity_pilot import run
from neorome.paper_ledger import PaperLedger

UTC=timezone.utc
NOW=datetime(2026,10,1,14,tzinfo=UTC)

def bundle(now,trend=True,depth='1'):
    # Constructed calendar/quotes for software tests, not market research.
    start=now.date()-timedelta(days=500);days=[];day=start
    while day<=now.date():
        if day.weekday()<5:days.append(day)
        day+=timedelta(days=1)
    sessions=[{'date':d.isoformat(),'open_at':datetime(d.year,d.month,d.day,13,30,tzinfo=UTC).isoformat(),
               'close_at':datetime(d.year,d.month,d.day,20,tzinfo=UTC).isoformat()} for d in days]
    bars=[]
    for i,d in enumerate(days[:-1]):
        price=90+i*.01 if trend else 100-i*.01
        bars.append({'timestamp':datetime(d.year,d.month,d.day,4,tzinfo=UTC).isoformat(),
                     'o':str(price),'h':str(price+1),'l':str(price-1),'c':str(price),'volume':'100000'})
    return {'source_kind':'observed','order_mutations':0,
        'context':{'source_kind':'observed','received_at':now.isoformat(),'clock':{'timestamp':now.isoformat(),'is_open':True},
            'asset':{'symbol':'SPY','asset_class':'us_equity','status':'active','tradable':True,'fractionable':True,'exchange':'ARCA'},
            'calendar_start':start.isoformat(),'calendar_end':now.date().isoformat(),'sessions':sessions,'evidence':[]},
        'observation':{'source_kind':'observed','feed':'iex','symbol':'SPY','observed_at':now.isoformat(),'bars':bars,
            'quote':{'timestamp':(now-timedelta(seconds=1)).isoformat(),'bid':'94','ask':'94.01','bid_size':depth,'ask_size':'1'}}}

class PilotTests(unittest.TestCase):
    def test_entry_fresh_mark_repeat_and_exit_persist(self):
        with tempfile.TemporaryDirectory() as tmp:
            source=Path(tmp)/'bundle.json';state=Path(tmp)/'state'
            source.write_text(json.dumps(bundle(NOW)))
            first=run(source,state,NOW)
            self.assertIsNone(first['blocker'])
            self.assertEqual([a['type'] for a in first['actions']],['simulated_fill'])
            self.assertIsNone(first['market_snapshot']['market_nav_cents'])
            next_time=NOW+timedelta(minutes=2);source.write_text(json.dumps(bundle(next_time)))
            second=run(source,state,next_time)
            self.assertEqual(second['actions'],[])
            self.assertTrue(second['market_snapshot']['coverage_current'])
            self.assertEqual(second['broker_orders_sent'],0)
            later=NOW+timedelta(minutes=4);source.write_text(json.dumps(bundle(later,False)))
            third=run(source,state,later)
            self.assertEqual([a['type'] for a in third['actions']],['simulated_close'])
            self.assertEqual(third['cost_snapshot']['open_position_cost_cents'],0)
            self.assertEqual(third['market_snapshot']['market_nav_cents'],third['cost_snapshot']['paper_nav_cents'])
            book=PaperLedger(state/'paper.sqlite3',enforce_market_risk=True,clock=lambda:later)
            self.assertEqual(sum(e.event_type=='fill' for e in book.audit()),1)
    def test_non_rebalance_observation_does_not_fill(self):
        with tempfile.TemporaryDirectory() as tmp:
            now=NOW+timedelta(days=1);source=Path(tmp)/'bundle.json';source.write_text(json.dumps(bundle(now)))
            result=run(source,Path(tmp)/'state',now)
            self.assertFalse(result['decision']['rebalance_due'])
            self.assertEqual(result['actions'],[])
            self.assertEqual(result['cost_snapshot']['paper_nav_cents'],80000)
    def test_invalid_exit_depth_stops_open_book(self):
        with tempfile.TemporaryDirectory() as tmp:
            source=Path(tmp)/'bundle.json';state=Path(tmp)/'state';source.write_text(json.dumps(bundle(NOW)))
            run(source,state,NOW)
            now=NOW+timedelta(minutes=2);source.write_text(json.dumps(bundle(now,depth='0.001')))
            result=run(source,state,now)
            self.assertIsNotNone(result['blocker'])
            self.assertIsNone(result['market_snapshot'])
            book=PaperLedger(state/'paper.sqlite3',enforce_market_risk=True,clock=lambda:now)
            self.assertTrue(book.risk_stop_active())
            self.assertEqual(result['actions'],[])
    def test_mixed_provenance_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            data=bundle(NOW);data['observation']['source_kind']='synthetic'
            source=Path(tmp)/'bundle.json';source.write_text(json.dumps(data))
            with self.assertRaises(ValueError):run(source,Path(tmp)/'state',NOW)

if __name__=='__main__':unittest.main()
