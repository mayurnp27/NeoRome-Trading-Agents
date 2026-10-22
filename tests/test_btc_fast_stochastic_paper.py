from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
from pathlib import Path
import tempfile
import unittest
UTC = timezone.utc
OPEN = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)
CLOSE = OPEN + timedelta(minutes=15)
TICKER = "KXBTC15M-26OCT141200"
RULES = ("YES if the average of CF Benchmarks Bitcoin Real Time Index (BRTI) values observed "
         "at the start of each of the sixty seconds before expiration is at least the floor strike.")


def digest(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def at(seconds):
    return (OPEN + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


def make_book(receipt, *, no_bid="0.41", no_quantity="10", yes_bid="0.58", yes_quantity="10", source="a"):
    return {
        "rest_date": at(receipt - 1),
        "received_at": at(receipt),
        "source_hash": digest(source + str(receipt) + str(no_bid) + str(yes_bid)),
        "orderbook_fp": {
            "yes_dollars": [[yes_bid, yes_quantity]] if yes_bid is not None else [],
            "no_dollars": [[no_bid, no_quantity]] if no_bid is not None else [],
        },
    }


def make_input(*, method="fresh_cross", timeframe=30, direction="bullish", ticker=TICKER, book2=True, book1=None,
               second=None, signal_id="btc-stoch-30m-turn-1791979200", status="active"):
    bullish = direction == "bullish"
    return {
        "signal": {
            "signal_id": signal_id,
            "instrument": "BTC-USD",
            "timeframe_minutes": timeframe,
            "method": method,
            "bar_close_timestamp": int(OPEN.timestamp()),
            "observed_receipt_at": at(1),
            "source_hash": digest("coinbase-closed-bar"),
            "valid": True,
            "previous_k": 19.5 if bullish else 80.5,
            "previous_d": 22.0 if bullish else 80.0,
            "k": 21.0 if bullish else 79.0,
            "d": 20.5 if bullish else 80.0,
            "direction": direction,
            "contract_side": "yes" if bullish else "no",
            ("oversold" if bullish else "overbought"): 20 if bullish else 80,
        },
        "market": {
            "ticker": ticker,
            "exchange_index": 2,
            "status": status,
            "open_time": at(0),
            "close_time": CLOSE.isoformat().replace("+00:00", "Z"),
            "floor_strike": "65000.00",
            "rules_primary": RULES,
            "source_hash": digest("market-metadata"),
            "venue": "kalshi",
        },
        "fee": {"fee_type": "quadratic", "fee_multiplier": "1", "source_hash": digest("fee-metadata")},
        "book1": book1 or make_book(2, source="book1"),
        "book2": second if second is not None else (make_book(4, source="book2") if book2 else None),
    }

from neorome import btc_fast_stochastic_paper as paper


def evidence(offset=0, direction='bullish', pending=False):
    value=make_input(direction=direction, book2=not pending)
    value['signal'].update(timeframe_minutes=5, stochastic_settings=[5,3,3], explicit_source_bar_timestamp=True,
                           bar_close_timestamp=int(OPEN.timestamp())+offset, observed_receipt_at=at(offset+1))
    value['book1']=make_book(offset+2, no_quantity='1000', yes_quantity='1000', source='first')
    value['book2']=None if pending else make_book(offset+4, no_quantity='1000',yes_quantity='1000',source='second')
    return value

class FastPaperTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.root=Path(self.temp.name)
    def tearDown(self):
        self.temp.cleanup()
    def test_each_5m_close_enters_current_15m_event_with_twenty_percent_cash(self):
        for offset in (0,300,600):
            with self.subTest(offset=offset):
                root=self.root/str(offset)
                result=paper.run_once(evidence(offset),root,at(offset+10))
                self.assertEqual(result['status'],'filled')
                snapshot=paper.snapshot(root,at(offset+10))
                pred=next(x for x in snapshot['balances'] if x['desk']=='prediction')
                self.assertGreaterEqual(pred['available_cash_cents'],40000)
                self.assertLess(pred['available_cash_cents'],40100)
                self.assertEqual(snapshot['allocation_bps'], 2000)
                self.assertEqual(snapshot['allocation_policy_version'], 'btc-cash20-v1')
                self.assertEqual(snapshot['available_cash_before_cents'], 50000)
                intent=next(event for event in snapshot['audit_events'] if event['event_type']=='intent')
                self.assertEqual(intent['payload']['allocation_bps'], 2000)
                self.assertEqual(intent['payload']['allocation_policy_version'], 'btc-cash20-v1')
                self.assertEqual(intent['payload']['available_cash_before_cents'], 50000)
                self.assertEqual(result['venue_orders_sent'],0)
                self.assertTrue((root/'btc_fast_stochastic_paper.sqlite3').exists())
                self.assertFalse((root/'btc_stochastic_paper.sqlite3').exists())
    def test_yes_and_no_settle_against_correct_official_result(self):
        for direction,result in (('bullish','yes'),('bearish','no')):
            with self.subTest(direction=direction):
                root=self.root/direction
                fill=paper.run_once(evidence(300,direction),root,at(310))
                settled=paper.run_once({'settlement':{'ticker':TICKER,'status':'settled','result':result,
                    'observed_at':at(902),'source_hash':digest(direction)}},root,at(905))
                self.assertEqual(settled['status'],'settled')
                self.assertEqual(settled['close']['proceeds_cents'],fill['fill']['quantity']*100)
    def test_pending_deadline_is_signal_close_plus_90_not_market_open(self):
        result=paper.run_once(evidence(600,pending=True),self.root,at(610))
        self.assertEqual(result['status'],'pending')
        self.assertEqual(paper.snapshot(self.root,at(610))['pending_entry_deadline'],at(690))
        second=make_book(620,no_quantity='1000',yes_quantity='1000',source='later')
        result=paper.run_once({'book2':second,'market':{'ticker':TICKER}},self.root,at(621))
        self.assertEqual(result['status'],'filled')
    def test_expired_pending_releases_cash_without_external_data(self):
        paper.run_once(evidence(300,pending=True),self.root,at(310))
        result=paper.run_once({},self.root,at(391))
        self.assertEqual(result['status'],'cancelled')
        pred=next(x for x in paper.snapshot(self.root,at(391))['balances'] if x['desk']=='prediction')
        self.assertEqual(pred['available_cash_cents'],50000)
    def test_rejects_wrong_settings_missing_timestamp_forming_or_expired_bar(self):
        changes=[{'stochastic_settings':[14,3,3]}, {'timeframe_minutes':15},
                 {'explicit_source_bar_timestamp':False}, {'bar_close_timestamp':int(OPEN.timestamp())+900},
                 {'bar_close_timestamp':int(OPEN.timestamp())+301}, {'d':float('nan')}]
        for i,change in enumerate(changes):
            with self.subTest(change=change):
                value=evidence(300); value['signal'].update(change)
                with self.assertRaises(paper.BTCStochasticPaperError):
                    paper.run_once(value,self.root/str(i),at(310))
        with self.assertRaises(paper.BTCStochasticPaperError):
            paper.run_once(evidence(300),self.root/'expired',at(391))
    def test_duplicate_cycle_cannot_add_second_position(self):
        value=evidence(300)
        first=paper.run_once(value,self.root,at(310))
        again=paper.run_once(value,self.root,at(311))
        self.assertEqual(first['status'],'filled'); self.assertEqual(again['status'],'open')
        self.assertEqual(sum(x.event_type=='fill' for x in paper.initialize(self.root).audit()),1)
    def test_independent_books_and_fee_buffer_still_enforced(self):
        value=evidence(300); value['book2']=deepcopy(value['book1'])
        result=paper.run_once(value,self.root,at(310))
        self.assertEqual(result['status'],'cancelled')
        self.assertEqual(result['venue_orders_sent'],0)
