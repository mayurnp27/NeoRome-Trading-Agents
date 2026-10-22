from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest
from neorome import tradingview_fast_signal as signal

UTC=timezone.utc

def make_record(now, previous=18, current=22):
    closed=int(now.timestamp())//300*300
    meta={'request_started_at':(now-timedelta(seconds=.2)).isoformat(), 'received_at':now.isoformat(),
          'http_date':now.isoformat(),'cache_age':0,'request_count':1,'cache_disabled':True,'stale_disabled':True,
          'explicit_source_bar_timestamp':True,'source_response_sha256':'a'*64,'endpoint':signal.ENDPOINT}
    return {'data':{'symbol':signal.SYMBOL,'timeframe':'5m','stochastic_settings':[5,3,3],
            'bars':[], 'readings':[{'bar_close_timestamp':closed-300,'k':previous,'d':80 if previous>=80 else 20},
                                  {'bar_close_timestamp':closed,'k':current,'d':80 if previous>=80 else 21}], 'source_metadata':meta},
            'received_at':now.isoformat(),'source_hash':'b'*64}

class FastSignalTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.now=datetime(2026,10,15,4,5,10,tzinfo=UTC)
    def tearDown(self):self.temp.cleanup()
    def test_both_turns_and_neutral_are_deterministic(self):
        for prior,current,side in ((18,22,'yes'),(82,78,'no'),(50,60,None)):
            result=signal.evaluate(make_record(self.now,prior,current),self.root,self.now)
            selected=result['selected_signal']
            self.assertEqual(selected['contract_side'] if selected else None,side)
            if selected:
                self.assertEqual(selected['stochastic_settings'],[5,3,3])
                self.assertTrue(selected['explicit_source_bar_timestamp'])
    def test_forming_stale_gapped_wrong_profile_and_stale_transport_fail(self):
        mutations=[lambda r:r['data'].update(stochastic_settings=[14,3,3]),
                   lambda r:r['data']['readings'][-1].update(bar_close_timestamp=int(self.now.timestamp())//300*300+300),
                   lambda r:r['data']['readings'][0].update(bar_close_timestamp=int(self.now.timestamp())//300*300-600),
                   lambda r:r['data']['source_metadata'].update(received_at=(self.now-timedelta(seconds=20)).isoformat()),
                   lambda r:r['data']['source_metadata'].update(explicit_source_bar_timestamp=False),
                   lambda r:r['data']['readings'][-1].update(k=float('nan'))]
        for mutate in mutations:
            value=make_record(self.now);mutate(value)
            with self.assertRaises(signal.TradingViewMCPError):signal.evaluate(value,self.root,self.now)
    def test_expired_closed_turn_cannot_select(self):
        now=self.now+timedelta(seconds=90)
        result=signal.evaluate(make_record(now),self.root,now)
        self.assertIsNone(result['selected_signal'])
    def test_request_crossing_boundary_fails(self):
        record=make_record(self.now)
        record['data']['source_metadata']['request_started_at']=(self.now-timedelta(seconds=12)).isoformat()
        with self.assertRaises(signal.TradingViewMCPError):signal.evaluate(record,self.root,self.now)
