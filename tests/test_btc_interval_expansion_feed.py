import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from neorome import btc_interval_expansion_feed as feed
from neorome.btc_interval_expansion_engine import ExpansionEngine
from neorome.btc_sma1m_engine import SMAEngine


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def quote(at, label, *, bid=20, ask=20, bid_qty=10000, ask_qty=10000):
    return {
        "received_at": at,
        "source_hash": digest(label),
        "yes_bids": [{"price_cents": bid, "quantity": bid_qty}],
        "yes_asks": [{"price_cents": ask, "quantity": ask_qty}],
        "no_bids": [{"price_cents": bid, "quantity": bid_qty}],
        "no_asks": [{"price_cents": ask, "quantity": ask_qty}],
    }


class CompletedFuture:
    def __init__(self, value):
        self.value = value

    def done(self):
        return True

    def result(self):
        return self.value


class BtcIntervalExpansionFeedTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.root = self.base / "expansion"
        self.source_path = self.base / "source-sma.sqlite3"
        self.engine = None
        self.driver = None
        self.source_engine = None

    def tearDown(self):
        self.close_driver_engine()
        if self.source_engine is not None:
            self.source_engine.close()
        self.tmp.cleanup()

    def close_driver_engine(self):
        if self.driver is not None:
            self.driver.close()
            self.driver = None
        if self.engine is not None:
            self.engine.close()
            self.engine = None

    def make_driver(self, *, source_path=None, data_only=False, root=None):
        self.root = root or self.root
        self.root.mkdir(parents=True, exist_ok=True)
        config = feed.initialize(self.root, data_only=data_only)
        config["started_at"] = 0
        feed.shared.atomic(self.root / "config.json", config)
        engine_path = self.root / "expansion.sqlite3"
        self.engine = ExpansionEngine(engine_path, started_at=0, include_reversal_brackets=True)
        self.driver = feed.ExpansionDriver(
            self.root,
            self.engine,
            archive_root=self.base / "shared-quotes",
            source_path=source_path or self.source_path,
            data_only=data_only,
        )
        return self.driver, self.engine

    def make_source_ledger(self, *, signal_time=0, observed_at=None, ticker="KXBTC15M-SOURCE"):
        observed_at = signal_time + 1 if observed_at is None else observed_at
        self.source_engine = SMAEngine(self.source_path, started_at=0)
        for timeframe, arm_id in self.source_engine.ARMS:
            self.source_engine.register_cohort({
                "cohort_id": "source:" + timeframe,
                "timeframe": timeframe,
                "ticker": ticker,
                "side": "yes",
                "observed_at": observed_at,
                "signal_time": signal_time,
                "close_time": 1000,
                "fee_multiplier": "1",
                "source_hash": digest("cohort:" + timeframe),
            })
        self.source_engine.on_quote(ticker, quote(observed_at + 1, "source-first"))
        self.source_engine.on_quote(ticker, quote(observed_at + 3, "source-entry"))
        self.source_engine.close()
        self.source_engine = None
        with sqlite3.connect(self.source_path) as con:
            con.execute("CREATE TABLE signal_decisions(target INTEGER PRIMARY KEY, observed REAL, payload TEXT, applied INTEGER NOT NULL DEFAULT 0)")
        return ticker

    def seed_expansion_reversal_position(self, engine, ticker, *, target=100):
        engine.register_cohort({
            "cohort_id": "expansion-reversal-position",
            "timeframe": "1m-sma-reversal-brackets",
            "ticker": ticker,
            "side": "yes",
            "observed_at": target + 1,
            "signal_time": target,
            "close_time": target + 900,
            "fee_multiplier": "1",
            "source_hash": digest("expansion-reversal-cohort"),
        })
        engine.on_quote(ticker, quote(target + 2, "expansion-first"))
        engine.on_quote(ticker, quote(target + 4, "expansion-entry"))

    def add_source_decision(self, target, *, reversals=()):
        with sqlite3.connect(self.source_path) as con:
            con.execute(
                "INSERT INTO signal_decisions(target,observed,payload,applied) VALUES(?,?,?,1)",
                (target, target + 10, json.dumps({"target_close": target, "reversals": list(reversals)})),
            )

    @staticmethod
    def chart_result(now, target):
        return {
            "chart": {
                "source_metadata": {
                    "received_at": feed.shared.iso(now - 2),
                    "source_response_sha256": digest("chart-response"),
                },
                "symbol": "COINBASE:BTCUSD",
                "timeframe": "15m",
                "bars": [{"bar_close_timestamp": target}],
            },
            "fee": {"fee_multiplier": "1"},
        }

    def prepare_15m_future(self, driver, engine, *, now, target, gap=True,
                           current_open=None, current_close=None):
        with engine._tx():
            engine.db.execute("INSERT INTO signal_decisions(target) VALUES(?)", (target,))
        driver.chart_target = target
        driver.chart_future = CompletedFuture(self.chart_result(now, target))
        driver.current_market = {
            "ticker": "KXBTC15M-TEST",
            "open_time": feed.shared.iso(target if current_open is None else current_open),
            "close_time": feed.shared.iso(target + 900 if current_close is None else current_close),
        }
        driver.current_market_received = now

    def test_existing_unapplied_source_intent_retries_without_replacing_known_time(self):
        self.make_source_ledger()
        driver,engine=self.make_driver(source_path=self.source_path)
        cohort={'cohort_id':'retry-intent','timeframe':'1m-sma-brackets','ticker':'KXBTC15M-TEST','side':'yes','observed_at':10,'signal_time':0,'close_time':1000,'fee_multiplier':'1','source_hash':digest('retry')}
        original=engine.register_cohort;attempts=[]
        def transient(c):
            attempts.append(c)
            if len(attempts)==1:raise ValueError('injected transient registration failure')
            return original(c)
        with patch.object(engine,'register_cohort',side_effect=transient),patch.object(feed.time,'time',return_value=12):
            with self.assertRaisesRegex(ValueError,'injected transient'):
                driver._save_source('entry:retry',10,{'cohort':cohort})
            self.assertEqual(driver.coverage_status()['pending_source_intents'],1)
            self.assertFalse(driver._save_source('entry:retry',12,{'cohort':dict(cohort,observed_at=12)}))
        self.assertEqual(driver.coverage_status()['pending_source_intents'],0)
        self.assertEqual(engine.db.execute("select observed from cohorts where cohort_id='retry-intent'").fetchone()[0],10)

    def test_source_rows_reads_actual_entries_readonly_and_validates_frozen_identity(self):
        self.make_source_ledger()
        before = hashlib.sha256(self.source_path.read_bytes()).hexdigest()
        entries, decisions = feed.source_rows(self.source_path, 0, 0)
        after = hashlib.sha256(self.source_path.read_bytes()).hexdigest()
        self.assertEqual(before, after)
        self.assertEqual(len(entries), 4)
        self.assertEqual({entry["arm_id"] for entry in entries},
                         {arm_id for _, arm_id in SMAEngine.ARMS})
        self.assertTrue(all(json.loads(entry["cohort_payload"])["side"] == "yes"
                            for entry in entries))
        self.assertEqual(decisions, [])
        with sqlite3.connect(self.source_path) as con:
            con.execute("UPDATE meta SET value=? WHERE key='config'", ('{"version":99}',))
        with self.assertRaisesRegex(ValueError, "frozen engine identity"):
            feed.source_rows(self.source_path, 0, 0)

    def test_source_entry_mapping_is_causal_and_never_uses_pre_read_quotes(self):
        ticker = self.make_source_ledger()
        driver, engine = self.make_driver(source_path=self.source_path)
        # This stale shallow quote predates the point when the child feed reads the fill.
        engine.on_quote(ticker, quote(8, "before-source-read", ask=20, ask_qty=1))
        clock_values = iter((10.0, 12.0))
        with patch.object(feed.time, "time", side_effect=lambda: next(clock_values, 12.0)):
            driver.source_cohorts(10, blocker=None)
        cohorts = engine.db.execute("SELECT * FROM cohorts ORDER BY timeframe").fetchall()
        self.assertEqual({row["timeframe"] for row in cohorts}, set(feed.SOURCE_ARMS.values()))
        self.assertEqual({row["observed"] for row in cohorts}, {12.0},
                         "the child signal clock is first known after the source query")
        for timeframe in feed.SOURCE_ARMS.values():
            self.assertEqual(sum(a["timeframe"] == timeframe for a in engine.status(12)["arms"]), 10)
        engine.on_quote(ticker, quote(12, "at-known-time", ask=50, ask_qty=1000))
        engine.on_quote(ticker, quote(13, "first-future", ask=50, ask_qty=1000))
        engine.on_quote(ticker, quote(14.9, "too-soon", ask=50, ask_qty=1000))
        self.assertEqual(engine.db.execute("SELECT count(*) FROM events WHERE kind='entry'").fetchone()[0], 0)
        engine.on_quote(ticker, quote(15, "second-future", ask=50, ask_qty=1000))
        entries = engine.db.execute("SELECT arm_id,payload FROM events WHERE kind='entry'").fetchall()
        self.assertEqual(len(entries), 40)
        self.assertTrue(all(json.loads(row[1])["quantity"] > 1 for row in entries))
        self.assertTrue(all(json.loads(row[1])["premium_cents"] == 51 for row in entries))

    def test_expired_source_entry_is_classified_as_blocked_without_cohort(self):
        self.make_source_ledger()
        driver, engine = self.make_driver(source_path=self.source_path)
        with patch.object(feed.time, "time", return_value=31):
            driver.source_cohorts(31, blocker=None)
        coverage = driver.coverage_status()
        self.assertEqual(coverage["sma_source_entries_classified"], 4)
        self.assertEqual(coverage["sma_entry_cohorts_admitted"], 0)
        self.assertEqual(len(coverage["sma_entries_blocked"]), 4)
        self.assertEqual({row["reason"] for row in coverage["sma_entries_blocked"]},
                         {"original_signal_window_expired"})
        self.assertEqual(engine.db.execute(
            "SELECT count(*) FROM cohorts WHERE cohort_id LIKE '%:source-entry-%'"
        ).fetchone()[0], 0)

    def test_reversal_source_decision_is_fresh_only_and_replay_is_restart_idempotent(self):
        ticker = self.make_source_ledger(signal_time=100, observed_at=101)
        self.add_source_decision(50, reversals=[{"side": "yes", "signal_time": 50}])
        self.add_source_decision(100, reversals=[{"side": "no", "signal_time": 100}])
        driver, engine = self.make_driver(source_path=self.source_path)
        self.seed_expansion_reversal_position(engine, ticker)
        with patch.object(feed.time, "time", return_value=110):
            driver.source_cohorts(110, blocker=None)
        position = next(a["position"] for a in engine.status(110)["arms"]
                        if a["timeframe"] == "1m-sma-reversal-brackets")
        self.assertEqual(position["trigger"]["reason"], "opposite_sma_cross")
        self.assertEqual(position["trigger"]["at"], 110)
        trigger_count = engine.db.execute(
            "SELECT count(*) FROM events WHERE kind='reversal_trigger'").fetchone()[0]
        self.assertEqual(trigger_count, 10)
        self.assertEqual(engine.db.execute(
            "SELECT count(*) FROM source_decisions WHERE source_key='reversal:100'").fetchone()[0], 1)
        self.close_driver_engine()
        driver, engine = self.make_driver(source_path=self.source_path)
        with patch.object(feed.time, "time", return_value=111):
            driver.source_cohorts(111, blocker=None)
        self.assertEqual(engine.db.execute(
            "SELECT count(*) FROM events WHERE kind='reversal_trigger'").fetchone()[0], trigger_count)
        self.assertEqual(engine.db.execute(
            "SELECT count(*) FROM source_decisions WHERE source_key='reversal:100'").fetchone()[0], 1)

    def test_data_only_reads_source_decisions_but_suppresses_reversals_and_entries(self):
        ticker = self.make_source_ledger(signal_time=100, observed_at=101)
        self.add_source_decision(100, reversals=[{"side": "no", "signal_time": 100}])
        driver, engine = self.make_driver(source_path=self.source_path, data_only=True)
        self.seed_expansion_reversal_position(engine, ticker)
        with patch.object(feed.time, "time", return_value=110):
            driver.source_cohorts(110, blocker=None)
        position = next(a["position"] for a in engine.status(110)["arms"]
                        if a["timeframe"] == "1m-sma-reversal-brackets")
        self.assertIsNone(position.get("trigger"))
        self.assertEqual(engine.db.execute(
            "SELECT count(*) FROM events WHERE kind='reversal_trigger'").fetchone()[0], 0)
        self.assertEqual(engine.db.execute(
            "SELECT count(*) FROM cohorts WHERE cohort_id LIKE '%:source-entry-%'"
        ).fetchone()[0], 0)
        coverage = driver.coverage_status()
        self.assertEqual(coverage["reversal_decisions"], 1)
        self.assertEqual({row["reason"] for row in coverage["sma_entries_blocked"]},
                         {"data_only_preflight"})

    def test_15m_raw_signal_creates_original_and_gap_only_then_original_entry_creates_children(self):
        self.make_source_ledger()
        driver, engine = self.make_driver(source_path=self.source_path)
        target, now = 900, 930
        self.prepare_15m_future(driver, engine, now=now, target=target, gap=True)
        signal = {"qualifies": True, "gap_qualifies": True, "direction": "yes", "reasons": []}
        with patch.object(feed, "evaluate", return_value=signal), patch.object(feed.time, "time", return_value=now):
            driver.cohorts(now, blocker=None)
        raw = engine.db.execute("SELECT timeframe,signal,close_time FROM cohorts ORDER BY timeframe").fetchall()
        self.assertEqual({row["timeframe"] for row in raw}, {"15m-original", "15m-gap5"})
        self.assertEqual({row["signal"] for row in raw}, {float(target)})
        self.assertEqual({row["close_time"] for row in raw}, {target + 900.0})
        self.assertEqual(driver.coverage_status()["original_15m_comparison_cohorts_admitted"], 0)
        ticker = "KXBTC15M-TEST"
        engine.on_quote(ticker, quote(target + 31, "15m-first"))
        engine.on_quote(ticker, quote(target + 33, "15m-original-entry"))
        original = next(a for a in engine.status(target + 33)["arms"]
                        if a["arm_id"] == "15m-original-hold")
        self.assertIsNotNone(original["position"])
        with patch.object(feed.time, "time", return_value=target + 35):
            driver.local_comparisons(target + 35, blocker=None)
        children = engine.db.execute(
            "SELECT timeframe,observed FROM cohorts WHERE timeframe IN ('15m-comparison','15m-trailing')"
        ).fetchall()
        self.assertEqual({row["timeframe"] for row in children}, {"15m-comparison", "15m-trailing"})
        self.assertEqual({row["observed"] for row in children}, {target + 35.0})
        engine.on_quote(ticker, quote(target + 36, "child-first"))
        engine.on_quote(ticker, quote(target + 38, "child-second"))
        child_entries = engine.db.execute(
            "SELECT arm_id FROM events WHERE kind='entry' AND cohort_id LIKE '%original-entry%'"
        ).fetchall()
        self.assertEqual(len(child_entries), 12)
        self.assertEqual(sum(row["arm_id"] == "15m-trailing-a50-d50" for row in child_entries), 1)

    def test_15m_gap_is_signal_specific_and_market_open_must_match_target(self):
        self.make_source_ledger()
        driver, engine = self.make_driver(source_path=self.source_path)
        target, now = 900, 930
        self.prepare_15m_future(driver, engine, now=now, target=target, gap=False)
        signal = {"qualifies": True, "gap_qualifies": False, "direction": "yes", "reasons": []}
        with patch.object(feed, "evaluate", return_value=signal), patch.object(feed.time, "time", return_value=now):
            driver.cohorts(now, blocker=None)
        self.assertEqual([row[0] for row in engine.db.execute("SELECT timeframe FROM cohorts")],
                         ["15m-original"])
        self.close_driver_engine()
        driver, engine = self.make_driver(source_path=self.source_path,
                                          root=self.base / "misaligned-market")
        self.prepare_15m_future(driver, engine, now=now, target=target, gap=True,
                                current_open=target - 900)
        with patch.object(feed, "evaluate", return_value=signal), patch.object(feed.time, "time", return_value=now):
            driver.cohorts(now, blocker=None)
        self.assertEqual(engine.db.execute("SELECT count(*) FROM cohorts").fetchone()[0], 0)
        decision = json.loads(engine.db.execute(
            "SELECT payload FROM signal_decisions WHERE target=?", (target,)).fetchone()[0])
        self.assertEqual(decision["admission_blocker"], "15m_signal_not_at_contract_open")

    def test_15m_stale_market_decision_persists_bounded_receipt_and_boundary_diagnostics(self):
        self.make_source_ledger()
        driver, engine = self.make_driver(source_path=self.source_path)
        target, now = 900, 930
        self.prepare_15m_future(driver, engine, now=now, target=target)
        driver.current_market_received = now - 31
        signal = {"qualifies": True, "gap_qualifies": True, "direction": "yes", "reasons": []}
        with patch.object(feed, "evaluate", return_value=signal), patch.object(feed.time, "time", return_value=now):
            driver.cohorts(now, blocker=None)

        decision = json.loads(engine.db.execute(
            "SELECT payload FROM signal_decisions WHERE target=?", (target,)).fetchone()[0])
        diagnostic = decision["market_diagnostic"]
        self.assertEqual(decision["admission_blocker"], "current_market_not_fresh")
        self.assertEqual(diagnostic["selected_market"]["ticker"], "KXBTC15M-TEST")
        self.assertEqual(diagnostic["selected_market"]["open_time"], feed.shared.iso(target))
        self.assertEqual(diagnostic["selected_market"]["close_time"], feed.shared.iso(target + 900))
        self.assertEqual(diagnostic["selected_market"]["receipt_age_seconds"], 31)
        self.assertEqual(diagnostic["chart_receipt_age_seconds"], 2)
        self.assertEqual(diagnostic["open_minus_target_seconds"], 0)
        self.assertEqual(diagnostic["close_minus_target_plus_900_seconds"], 0)
        self.assertEqual(engine.db.execute("SELECT count(*) FROM cohorts").fetchone()[0], 0)
        self.assertEqual(engine.db.execute("SELECT count(*) FROM events WHERE kind='entry'").fetchone()[0], 0)

    def test_waits_for_fresh_aligned_metadata_without_consuming_decision_or_budget(self):
        self.make_source_ledger()
        driver, engine = self.make_driver(source_path=self.source_path)
        target = 900
        driver.current_market = {"ticker": "KXBTC15M-OLD", "open_time": feed.shared.iso(0),
                                 "close_time": feed.shared.iso(target)}
        driver.current_market_received = 899
        with patch.object(driver, "source_cohorts"), patch.object(driver, "local_comparisons"), \
             patch.object(driver, "reserve_requests") as reserve, \
             patch.object(driver.pool, "submit", return_value=CompletedFuture(self.chart_result(920, target))) as submit:
            with patch.object(feed.time, "time", return_value=908):
                driver.cohorts(908, blocker=None)
            self.assertEqual(engine.db.execute("SELECT count(*) FROM signal_decisions").fetchone()[0], 0)
            reserve.assert_not_called(); submit.assert_not_called()
            self.assertEqual(driver.last_contract_wait["reason"], "15m_signal_not_at_contract_open")
            driver.current_market = {"ticker": "KXBTC15M-NEW", "open_time": feed.shared.iso(target),
                                     "close_time": feed.shared.iso(target + 900)}
            driver.current_market_received = 919
            with patch.object(feed.time, "time", return_value=920):
                driver.cohorts(920, blocker=None)
            reserve.assert_called_once_with(2); submit.assert_called_once_with(feed.chart_and_fee)
            self.assertEqual(engine.db.execute("SELECT count(*) FROM signal_decisions").fetchone()[0], 1)
            self.assertIsNone(driver.last_contract_wait)
            signal = {"qualifies": True, "gap_qualifies": True, "direction": "yes", "reasons": []}
            with patch.object(feed.time, "time", return_value=921), patch.object(feed, "evaluate", return_value=signal):
                driver.cohorts(921, blocker=None)
            self.assertGreater(engine.db.execute("SELECT count(*) FROM cohorts").fetchone()[0], 0)
            self.assertEqual(engine.db.execute("SELECT count(*) FROM events WHERE kind='entry'").fetchone()[0], 0)

    def test_metadata_wait_never_widens_request_window_or_accepts_stale_contract(self):
        self.make_source_ledger()
        driver, engine = self.make_driver(source_path=self.source_path)
        driver.current_market = {"ticker": "KXBTC15M-NEW", "open_time": feed.shared.iso(900),
                                 "close_time": feed.shared.iso(1800)}
        driver.current_market_received = 880
        with patch.object(driver, "source_cohorts"), patch.object(driver, "local_comparisons"), \
             patch.object(driver, "reserve_requests") as reserve, patch.object(driver.pool, "submit") as submit:
            with patch.object(feed.time, "time", return_value=920): driver.cohorts(920, blocker=None)
            self.assertEqual(driver.last_contract_wait["reason"], "current_market_not_fresh")
            driver.current_market_received = 945
            with patch.object(feed.time, "time", return_value=946): driver.cohorts(946, blocker=None)
            reserve.assert_not_called(); submit.assert_not_called()
            self.assertEqual(engine.db.execute("SELECT count(*) FROM signal_decisions").fetchone()[0], 0)

    def test_data_only_schedules_native_15m_chart_outside_entry_window_without_admission(self):
        self.make_source_ledger()
        driver, engine = self.make_driver(source_path=self.source_path, data_only=True)
        target, now = 900, 1025
        result = self.chart_result(now, target)
        with patch.object(driver, "reserve_requests"), patch.object(
                driver.pool, "submit", return_value=CompletedFuture(result)) as submit:
            with patch.object(feed.time, "time", return_value=now):
                driver.cohorts(now, blocker=None)
                self.assertIsNotNone(driver.chart_future)
                submit.assert_called_once_with(feed.chart_and_fee)
                signal = {"qualifies": True, "gap_qualifies": True,
                          "direction": "yes", "reasons": []}
                with patch.object(feed, "evaluate", return_value=signal):
                    driver.cohorts(now, blocker=None)
        self.assertEqual(engine.db.execute("SELECT count(*) FROM cohorts").fetchone()[0], 0)
        decision = json.loads(engine.db.execute(
            "SELECT payload FROM signal_decisions WHERE target=?", (target,)).fetchone()[0])
        self.assertEqual(decision["admission_blocker"], "data_only_preflight")
        self.assertEqual(decision["cohorts"], [])

    def test_initialize_freezes_54_account_mode_and_protects_source_roots(self):
        config = feed.initialize(self.root)
        self.assertEqual(feed.initialize(self.root), config)
        self.assertEqual(config["new_accounts"], 54)
        self.assertTrue(config["include_reversal_brackets"])
        feed.shared.atomic(self.root / "config.json", {**config, "allocation_bps": 5000})
        with self.assertRaisesRegex(ValueError, "frozen expansion configuration"):
            feed.initialize(self.root)
        with self.assertRaisesRegex(ValueError, "existing SMA state root"):
            feed.validate_root(feed.SOURCE_ROOT)


if __name__ == "__main__":
    unittest.main()
