import hashlib
import json
from decimal import Decimal, ROUND_CEILING
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from neorome.btc_bracket_engine import ARMS, BracketError, Engine


def digest(label):
    return hashlib.sha256(label.encode()).hexdigest()


def quote(at, label, *, bid=40, ask=40, bid_qty=1000, ask_qty=1000):
    return {"received_at": at, "source_hash": digest(label),
            "yes_bids": [{"price_cents": bid, "quantity": bid_qty}],
            "yes_asks": [{"price_cents": ask, "quantity": ask_qty}],
            "no_bids": [{"price_cents": bid, "quantity": bid_qty}],
            "no_asks": [{"price_cents": ask, "quantity": ask_qty}]}


def cohort(**overrides):
    value = {"cohort_id": "cohort-1", "timeframe": "5m", "ticker": "BTC-YES",
             "side": "yes", "observed_at": 5.0, "signal_time": 5.0,
             "close_time": 100.0, "fee_multiplier": "1", "source_hash": digest("signal")}
    return {**value, **overrides}


class BTCBracketEngineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "brackets.sqlite3"
        self.engine = Engine(self.path, started_at=0.0)

    def tearDown(self):
        self.engine.close()
        self.temp.cleanup()

    def enter(self, *, bid=40, ask=40, qty=1000):
        self.engine.register_cohort(cohort())
        self.engine.on_quote("BTC-YES", quote(6, "first", bid=bid, ask=ask, bid_qty=qty, ask_qty=qty))
        self.engine.on_quote("BTC-YES", quote(8, "second", bid=bid, ask=ask, bid_qty=qty, ask_qty=qty))

    def arm(self, arm_id="5m-tp10-sl10"):
        return next(a for a in self.engine.status(10)["arms"] if a["arm_id"] == arm_id)

    def test_sizing_matches_existing_adapter_on_mixed_ladders_and_both_price_paths(self):
        from neorome import btc_fast_stochastic_paper as adapter
        cases=[([(60,10000)],[(20,10000)]), ([(20,10000)],[(60,10000)]),
               ([(20,7),(41,777),(60,1000)],[(18,9),(39,600),(59,2000)]),
               ([(1,999999)],[(1,999999)])]
        for index,(first,second) in enumerate(cases):
            def levels(items):return [{'price_cents':p,'quantity':q} for p,q in items]
            cid=f'mixed-{index}';ticker=f'MIXED-{index}'
            self.engine.register_cohort(cohort(cohort_id=cid,ticker=ticker,observed_at=5+index*200,signal_time=5+index*200,close_time=100+index*200))
            one=quote(6+index*200,'first-'+cid,bid=1);one['yes_asks']=levels(first)
            two=quote(8+index*200,'second-'+cid,bid=1);two['yes_asks']=levels(second)
            self.engine.on_quote(ticker,one);self.engine.on_quote(ticker,two)
            arm=self.arm('5m-hold');pos=arm['position'];budget=pos['available_cash_before_cents']*adapter.ALLOCATION_BPS//10000
            expected=adapter._size(levels(first),budget,'1',levels(second))
            self.assertEqual(pos['quantity'],expected['quantity'])
            unit,fee,total=adapter._unit_and_fee(adapter._walk(levels(second),pos['quantity']),'1')
            self.assertEqual((pos['entry_premium_cents'],pos['entry_fee_cents'],pos['cost_cents']),(unit,fee,total))
            self.engine.on_settlement(ticker,'yes',100+index*200,digest('settle-'+cid))

    def test_zero_price_asks_cannot_create_entries(self):
        self.enter(ask=0)
        self.assertIsNone(self.arm('5m-hold')['position'])

    def test_cross_timeframe_cohort_cannot_change_shared_ticker_close(self):
        self.engine.register_cohort(cohort())
        with self.assertRaises(BracketError):self.engine.register_cohort(cohort(cohort_id='other-tf',timeframe='30m',close_time=101))
        self.assertEqual(self.engine.status(10)['counts']['waiting'],1)

    def test_frozen_configuration_has_22_independent_500_dollar_accounts(self):
        status = self.engine.status(0)
        self.assertEqual(len(status["arms"]), 22)
        self.assertEqual(status["allocation_bps"], 2000)
        self.assertEqual(status["allocation_policy_version"], "btc-cash20-v1")
        self.assertEqual({a["initial_cash_cents"] for a in status["arms"]}, {50000})
        self.assertEqual(len(ARMS), 11)

    def test_database_symlink_is_refused_even_when_dangling(self):
        link = Path(self.temp.name) / "linked.sqlite3"
        link.symlink_to(Path(self.temp.name) / "missing.sqlite3")
        with self.assertRaises(BracketError): Engine(link, started_at=0)

    def test_entry_waits_for_two_future_quotes_and_sizes_to_budget_and_both_depth_snapshots(self):
        self.engine.register_cohort(cohort())
        self.engine.on_quote("BTC-YES", quote(5, "at-observation"))
        self.assertIsNone(self.arm()["position"])
        self.engine.on_quote("BTC-YES", quote(6, "first", ask_qty=700))
        self.engine.on_quote("BTC-YES", quote(7.99, "too-soon", ask_qty=1000))
        self.assertIsNone(self.arm()["position"])
        self.engine.on_quote("BTC-YES", quote(8, "second", ask_qty=1000))
        pos = self.arm()["position"]
        self.assertIsNotNone(pos)
        self.assertLessEqual(pos["quantity"], 700)
        self.assertLessEqual(pos["cost_cents"], 10000)
        self.assertEqual(pos["allocation_bps"], 2000)
        self.assertEqual(pos["allocation_budget_cents"], 10000)
        self.assertEqual(pos["available_cash_before_cents"], 50000)
        entry = self.engine.db.execute("SELECT payload FROM events WHERE kind='entry' LIMIT 1").fetchone()
        self.assertEqual(json.loads(entry[0])["available_cash_before_cents"], 50000)
        self.assertEqual(self.arm()["reserved_cash_cents"], 0)
        self.assertEqual(self.arm()["available_cash_cents"], 50000 - pos["cost_cents"])

    def test_entry_quantity_fits_the_more_expensive_first_snapshot_and_adapter_fee_rounding(self):
        self.engine.register_cohort(cohort())
        self.engine.on_quote("BTC-YES", quote(6, "expensive-first", ask=60))
        self.engine.on_quote("BTC-YES", quote(8, "cheap-second", ask=20))
        pos = self.arm()["position"]
        self.assertIsNotNone(pos)
        first_fee = int((Decimal("0.07") * pos["quantity"] * Decimal("0.61")
                         * Decimal("0.39") * 100).to_integral_value(rounding=ROUND_CEILING))
        self.assertLessEqual(pos["quantity"] * 61 + first_fee, 10000)
        actual_fee = int((Decimal("0.07") * pos["quantity"] * Decimal("0.21")
                          * Decimal("0.79") * 100).to_integral_value(rounding=ROUND_CEILING))
        self.assertEqual(pos["entry_fee_cents"], actual_fee)
        self.assertLessEqual(pos["cost_cents"], 10000)

    def test_legacy_half_cash_positions_survive_reopen_and_next_entries_use_twenty_percent(self):
        path = Path(self.temp.name) / 'legacy-half.sqlite3'
        with patch('neorome.btc_bracket_engine.ALLOCATION_BPS', 5000):
            legacy = Engine(path, started_at=0)
            legacy.register_cohort(cohort())
            legacy.on_quote('BTC-YES', quote(6, 'legacy-first'))
            legacy.on_quote('BTC-YES', quote(8, 'legacy-second'))
            old_positions = {a['arm_id']: a['position'] for a in legacy.status(10)['arms']
                             if a['timeframe'] == '5m' and a['position'] is not None}
            self.assertEqual(len(old_positions), len(ARMS))
            self.assertTrue(all(24_900 < position['cost_cents'] <= 25_000
                                for position in old_positions.values()))
            legacy.close()

        resumed = Engine(path, started_at=0)
        status = resumed.status(10)
        self.assertEqual({a['arm_id']: a['position'] for a in status['arms'] if a['position'] is not None},
                         old_positions)
        self.assertEqual(status['config']['version'], 1)
        self.assertEqual(status['allocation_bps'], 2000)
        resumed.on_settlement('BTC-YES', 'yes', 100, digest('legacy-settlement'))
        settled_status = resumed.status(100)
        self.assertTrue(all(a['position'] is None for a in settled_status['arms'] if a['timeframe'] == '5m'))

        new_cohort = cohort(cohort_id='after-legacy', ticker='BTC-NEXT', observed_at=101,
                            signal_time=101, close_time=201)
        available_before = next(a['available_cash_cents'] for a in settled_status['arms']
                                if a['arm_id'] == '5m-hold')
        resumed.register_cohort(new_cohort)
        resumed.on_quote('BTC-NEXT', quote(102, 'next-first'))
        resumed.on_quote('BTC-NEXT', quote(104, 'next-second'))
        new_arm = next(a for a in resumed.status(105)['arms'] if a['arm_id'] == '5m-hold')
        self.assertLessEqual(new_arm['position']['cost_cents'], available_before * 2000 // 10_000)
        self.assertEqual(new_arm['position']['available_cash_before_cents'], available_before)
        self.assertEqual(new_arm['position']['allocation_policy_version'], 'btc-cash20-v1')

    def test_validation_identity_conflict_and_quote_order(self):
        with self.assertRaises(BracketError): self.engine.register_cohort(cohort(side="BUY"))
        with self.assertRaises(BracketError): self.engine.register_cohort(cohort(observed_at=float("nan")))
        with self.assertRaises(BracketError): self.engine.register_cohort(cohort(observed_at=4, signal_time=5))
        with self.assertRaises(BracketError): self.engine.register_cohort(cohort(fee_multiplier="0"))
        with self.assertRaises(BracketError): self.engine.register_cohort(cohort(fee_multiplier="1000001"))
        self.engine.register_cohort(cohort())
        self.engine.register_cohort(cohort())
        with self.assertRaises(BracketError): self.engine.register_cohort(cohort(ticker="other"))
        self.engine.on_quote("BTC-YES", quote(6, "first"))
        with self.assertRaises(BracketError): self.engine.on_quote("BTC-YES", quote(6, "changed-source"))
        with self.assertRaises(BracketError): self.engine.on_quote("BTC-YES", quote(5, "old"))

    def test_entry_ask_cap_and_one_open_position_per_arm(self):
        self.enter(ask=65)
        self.assertIsNone(self.arm()["position"])
        # A later independent cohort cannot replace the arm's still-open exposure.
        self.engine.register_cohort(cohort(cohort_id="cohort-2", observed_at=9, signal_time=9,
                                           close_time=100, source_hash=digest("signal2")))
        self.engine.on_quote("BTC-YES", quote(10, "first2", bid=40, ask=40))
        self.engine.on_quote("BTC-YES", quote(12, "second2", bid=40, ask=40))
        pos = self.arm()["position"]
        self.assertIsNotNone(pos)
        self.engine.register_cohort(cohort(cohort_id="cohort-3", observed_at=13, signal_time=13,
                                           close_time=100, source_hash=digest("signal3")))
        self.engine.on_quote("BTC-YES", quote(14, "first3"))
        self.engine.on_quote("BTC-YES", quote(16, "second3"))
        self.assertEqual(self.arm()["position"]["cohort_id"], pos["cohort_id"])

    def test_stop_direction_sticky_trigger_then_later_distinct_quote_exits(self):
        self.enter()
        initial_cost = self.arm()["open_cost_cents"]
        self.engine.on_quote("BTC-YES", quote(9, "cross-sl", bid=30))
        trigger = self.arm()["position"]["trigger"]
        self.assertEqual(trigger["reason"], "stop_loss")
        self.assertEqual(self.arm()["open_cost_cents"], initial_cost)
        self.engine.on_quote("BTC-YES", quote(100, "at-close", bid=30))
        self.assertIsNotNone(self.arm()["position"], "bracket fills must stop at contract close")
        self.engine.on_settlement("BTC-YES", "no", 100, digest("stop-settlement"))
        a = self.arm()
        self.assertIsNone(a["position"])
        self.assertEqual(a["reserved_cash_cents"], 0)
        self.assertEqual(a["trades"], 1)
        self.assertEqual(a["losses"], 1)

    def test_take_profit_settlement_of_partial_exit_and_replay_are_exactly_once(self):
        self.enter()
        self.engine.on_quote("BTC-YES", quote(9, "cross-tp", bid=50))
        self.assertEqual(self.arm()["position"]["trigger"]["reason"], "take_profit")
        qty = self.arm()["position"]["quantity"]
        self.engine.on_quote("BTC-YES", quote(10, "partial", bid=50, bid_qty=1))
        self.assertEqual(self.arm()["position"]["quantity"], qty - 1)
        self.assertEqual(self.arm()["position"]["accrued_net_proceeds_cents"], 47)
        self.assertIsNone(self.arm()["latest_mark_cents"], "a partial fill invalidates the prior quote's full-position mark")
        self.engine.on_quote("BTC-YES", quote(11, "partial", bid=50, bid_qty=1))
        self.assertEqual(self.arm()["position"]["quantity"], qty - 1, "same source hash cannot reuse depth")
        self.engine.on_quote("BTC-YES", quote(12, "partial-distinct", bid=50, bid_qty=1))
        self.engine.on_quote("BTC-YES", quote(13, "settle-mark", bid=0, bid_qty=0))
        self.engine.on_settlement("BTC-YES", "yes", 100, digest("settlement"))
        after = self.arm()
        self.assertIsNone(after["position"])
        self.assertEqual(after["trades"], 1)
        cash = after["available_cash_cents"]
        self.engine.on_settlement("BTC-YES", "yes", 101, digest("settlement-reobserved"))
        self.assertEqual(self.arm()["available_cash_cents"], cash)
        with self.assertRaises(BracketError): self.engine.on_settlement("BTC-YES", "no", 100, digest("other"))

    def test_hold_ignores_thresholds_restart_preserves_open_partial_and_no_end_magic(self):
        self.enter(bid=1, ask=40)
        # Hold never creates a bracket trigger despite a zero-valued bid mark.
        self.engine.on_quote("BTC-YES", quote(9, "hold-mark", bid=1))
        self.assertIsNone(self.arm("5m-hold")["position"]["trigger"])
        hold = self.arm("5m-hold")
        self.assertEqual(hold["reserved_cash_cents"], 0)
        self.assertEqual(hold["available_cash_cents"] + hold["open_cost_cents"],
                         hold["initial_cash_cents"] + hold["realized_pnl_cents"])
        self.engine.close()
        self.engine = Engine(self.path, started_at=0.0)
        self.assertEqual(self.arm("5m-hold")["available_cash_cents"], hold["available_cash_cents"])
        self.assertIsNotNone(self.arm("5m-hold")["position"])
        self.assertEqual(self.engine.tickers(), ["BTC-YES"])
        # An unresolved exposure remains open after close until official settlement.
        self.engine.on_quote("BTC-YES", quote(100, "post-close", bid=1))
        self.assertIsNotNone(self.arm("5m-hold")["position"])

    def test_expire_skips_at_entry_deadline_without_a_quote(self):
        self.engine.register_cohort(cohort())
        self.engine.expire(95)
        self.assertEqual(self.engine.status(95)["counts"]["skipped"], 1)
        self.assertEqual(self.engine.tickers(), [])

    def test_restart_preserves_partial_exit_cash_and_config_identity(self):
        self.enter()
        self.engine.on_quote("BTC-YES", quote(9, "restart-trigger", bid=50))
        self.engine.on_quote("BTC-YES", quote(10, "restart-partial", bid=50, bid_qty=1))
        before = self.arm()
        position = before["position"]
        available = before["available_cash_cents"]
        self.engine.close()
        self.engine = Engine(self.path, started_at=0.0)
        after = self.arm()
        self.assertEqual(after["available_cash_cents"], available)
        self.assertEqual(after["position"], position)
        self.assertEqual(after["open_cost_cents"], before["open_cost_cents"])
        self.assertEqual(after["reserved_cash_cents"], before["reserved_cash_cents"])
        self.assertEqual(after["available_cash_cents"] + after["open_cost_cents"],
                         after["initial_cash_cents"] + after["realized_pnl_cents"])
        self.engine.on_quote("BTC-YES", quote(11, "restart-partial", bid=50, bid_qty=1))
        self.assertEqual(self.arm()["position"]["quantity"], position["quantity"], "restart must not reuse a source hash")
        with self.assertRaises(BracketError): Engine(self.path, started_at=1.0)

    def test_frozen_configuration_mismatch_is_rejected_without_reset(self):
        self.engine.close()
        db = sqlite3.connect(self.path)
        db.execute("UPDATE meta SET value=? WHERE key='config'", ('{"version":99}',))
        db.commit()
        db.close()
        with self.assertRaises(BracketError): Engine(self.path, started_at=0.0)

    def test_status_mark_is_net_liquidation_value_freshness_labeled_and_cleared_on_close(self):
        self.enter(bid=40, ask=40)
        before = self.arm()
        mark = before["latest_mark_cents"]
        self.assertIsNotNone(mark)
        self.assertLess(mark, before["open_cost_cents"])
        self.assertEqual(before["latest_mark_basis"], "net_liquidation_cents_including_accrued_proceeds_after_buffer_and_exit_fee")
        self.assertEqual(before["mark_age_seconds"], 2)
        self.assertFalse(before["mark_fresh"])
        fresh = next(a for a in self.engine.status(8.5)["arms"] if a["arm_id"] == "5m-tp10-sl10")
        self.assertTrue(fresh["mark_fresh"])
        self.engine.on_settlement("BTC-YES", "yes", 100, digest("mark-settlement"))
        after = self.arm()
        self.assertIsNone(after["latest_mark_cents"])
        self.assertIsNone(after["mark_received_at"])
        self.assertIsNone(after["latest_mark_basis"])
        self.assertGreater(after["available_cash_cents"], before["available_cash_cents"])
        self.assertEqual(self.engine.tickers(), [])

    def test_late_entry_is_recorded_as_skip_and_settlement_requires_official_close_time(self):
        self.engine.register_cohort(cohort())
        self.engine.on_quote("BTC-YES", quote(6, "first"))
        self.engine.on_quote("BTC-YES", quote(95.1, "late"))
        self.assertIsNone(self.arm()["position"])
        self.assertEqual(self.engine.status(95)["counts"]["skipped"], 1)
        with self.assertRaises(BracketError):
            self.engine.on_settlement("BTC-YES", "yes", 99, digest("early"))

    def test_entry_blocker_cancels_waiting_entries_but_does_not_block_settlement_path(self):
        self.engine.register_cohort(cohort())
        self.engine.set_entry_blocker("feed unhealthy", 5)
        self.assertEqual(self.engine.status(5)["counts"]["skipped"], 1)
        self.engine.set_entry_blocker(None, 6)
        self.engine.register_cohort(cohort(cohort_id="cohort-2", observed_at=6, signal_time=6,
                                           close_time=100, source_hash=digest("signal2")))
        self.engine.on_quote("BTC-YES", quote(7, "first2"))
        self.engine.on_quote("BTC-YES", quote(9, "second2"))
        self.assertIsNotNone(self.arm()["position"])


if __name__ == "__main__":
    unittest.main()
