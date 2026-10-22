import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from neorome import btc_sma1m_feed as feed
from neorome.btc_sma1m_engine import SMAEngine


def digest(text):
    return hashlib.sha256(text.encode()).hexdigest()


def quote(at, label, *, yes_bid=10, yes_ask=20, no_bid=10, no_ask=20):
    return {
        "received_at": at,
        "source_hash": digest(label),
        "yes_bids": [{"price_cents": yes_bid, "quantity": 10000}],
        "yes_asks": [{"price_cents": yes_ask, "quantity": 10000}],
        "no_bids": [{"price_cents": no_bid, "quantity": 10000}],
        "no_asks": [{"price_cents": no_ask, "quantity": 10000}],
    }


class CompletedFuture:
    def __init__(self, value):
        self.value = value

    def done(self):
        return True

    def result(self):
        return self.value


class BtcSma1mFeedTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "sma"
        self.engine = None
        self.driver = None

    def tearDown(self):
        if self.driver is not None:
            self.driver.close()
        if self.engine is not None:
            self.engine.close()
        self.tmp.cleanup()

    def make_driver(self, *, data_only=False):
        self.root.mkdir(parents=True, exist_ok=True)
        config = feed.initialize(self.root, data_only=data_only)
        # Keep the offline fixtures' chart epochs prospective for the engine.
        config["started_at"] = 0
        feed.shared.atomic(self.root / "config.json", config)
        self.engine = SMAEngine(self.root / "sma.sqlite3", started_at=0)
        self.driver = feed.SMADriver(
            self.root,
            self.engine,
            archive_root=Path(self.tmp.name) / "shared-quotes",
            data_only=data_only,
        )
        return self.driver, self.engine

    def set_current_market(self, driver, *, ticker="KXBTC15M-TEST", close=1000):
        driver.current_market = {
            "ticker": ticker,
            "open_time": feed.shared.iso(0),
            "close_time": feed.shared.iso(close),
        }
        driver.current_market_received = 84

    def chart_result(self, now=85, *, series_error=None):
        result = {
            "chart": {
                "source_metadata": {
                    "received_at": feed.shared.iso(now - 2),
                    "source_response_sha256": digest("chart-source"),
                },
                "symbol": "COINBASE:BTCUSD",
                "timeframe": "1m",
                "bars": [],
            },
        }
        if series_error is None:
            result["fee"] = {"fee_multiplier": "1"}
        else:
            result["series_error"] = series_error
        return result

    def set_future(self, driver, engine, result, *, target=60):
        with engine._tx():
            engine.db.execute(
                "insert into signal_decisions(target) values(?)",
                (target,),
            )
        driver.chart_target = target
        driver.chart_future = CompletedFuture(result)

    @staticmethod
    def signal(*, pure=True, direction="yes", confirmed=True, zone_direction="no"):
        return {
            "target_close": 60,
            "pure_qualifies": pure,
            "direction": direction if pure else None,
            "confirmed_qualifies": confirmed,
            "zone_exit_direction": zone_direction if confirmed else None,
            "sma_bias_direction": direction,
            "reasons": [],
        }

    def enter_reversal_position(self, engine, *, ticker="KXBTC15M-TEST"):
        engine.register_cohort({
            "cohort_id": "initial-reversal-position",
            "timeframe": "1m-sma-reversal",
            "ticker": ticker,
            "side": "yes",
            "observed_at": 1,
            "signal_time": 0,
            "close_time": 1000,
            "fee_multiplier": "1",
            "source_hash": digest("initial-cohort"),
        })
        engine.on_quote(ticker, quote(2, "first"))
        engine.on_quote(ticker, quote(4, "entry"))
        arm = next(a for a in engine.status(4)["arms"] if a["timeframe"] == "1m-sma-reversal")
        self.assertIsNotNone(arm["position"])

    def test_data_only_tick_still_processes_chart_source_without_admission(self):
        driver, engine = self.make_driver(data_only=True)
        with patch.object(feed.trailing.TrailingDriver, 'tick'), patch.object(driver, 'cohorts') as cohorts, patch.object(feed.time, 'time', return_value=85):
            driver.tick()
        cohorts.assert_called_once_with(85, 'data_only_preflight')

    def test_initialize_freezes_config_and_refuses_protected_roots(self):
        root = Path(self.tmp.name) / "config-root"
        config = feed.initialize(root)
        self.assertEqual(feed.initialize(root), config)
        self.assertEqual(config["initial_cash_cents_each"], 50000)
        self.assertEqual(config["allocation_bps"], 2000)
        self.assertEqual(len(config["exit_policies"]), 2)
        feed.shared.atomic(root / "config.json", dict(config, allocation_bps=5000))
        with self.assertRaisesRegex(ValueError, "SMA config mismatch"):
            feed.initialize(root)
        with self.assertRaisesRegex(ValueError, "existing trailing root refused"):
            feed.initialize(feed.trailing.ROOT)

    def test_saved_decision_recovery_is_idempotent_and_expired_cohort_never_enters(self):
        driver, engine = self.make_driver()
        ticker = "KXBTC15M-TEST"
        self.enter_reversal_position(engine, ticker=ticker)
        decision = {
            "target_close": 60,
            "observed_at": 80,
            "reversals": [{
                "side": "no",
                "observed_at": 80,
                "signal_time": 60,
                "source_hash": digest("saved-reversal"),
            }],
            "cohorts": [{
                "cohort_id": "1m-sma-reversal:60",
                "timeframe": "1m-sma-reversal",
                "ticker": ticker,
                "side": "no",
                "observed_at": 80,
                "signal_time": 60,
                "close_time": 1000,
                "fee_multiplier": "1",
                "source_hash": digest("saved-cohort"),
            }],
        }
        with engine._tx():
            engine.db.execute(
                "insert into signal_decisions(target,observed,payload,applied) values(?,?,?,0)",
                (60, 80, feed.shared.canonical(decision)),
            )
        with patch.object(feed.time, "time", return_value=91):
            driver._recover_decisions()
            driver._recover_decisions()

        self.assertEqual(engine.db.execute("select applied from signal_decisions where target=60").fetchone()[0], 1)
        self.assertEqual(engine.db.execute("select count(*) from events where kind='reversal_trigger'").fetchone()[0], 1)
        self.assertEqual(engine.db.execute("select state from cohorts where cohort_id='1m-sma-reversal:60'").fetchone()[0], "skipped")
        self.assertEqual(engine.db.execute("select count(*) from events where kind='entry' and cohort_id='1m-sma-reversal:60'").fetchone()[0], 0)

    def test_series_error_blocks_cohorts_but_applies_pure_sma_reversal(self):
        driver, engine = self.make_driver()
        self.enter_reversal_position(engine)
        self.set_future(driver, engine, self.chart_result(series_error="fee-series unavailable"))
        signal = self.signal(pure=True, direction="no", confirmed=False)
        with patch.object(feed, "evaluate", return_value=signal), patch.object(feed.time, "time", return_value=85):
            driver.cohorts(85, blocker=None)

        position = next(a["position"] for a in engine.status(85)["arms"] if a["timeframe"] == "1m-sma-reversal")
        self.assertEqual(position["reversal_trigger"]["new_side"], "no")
        self.assertEqual(engine.db.execute("select count(*) from cohorts where cohort_id like '%:60'").fetchone()[0], 0)
        payload = json.loads(engine.db.execute("select payload from signal_decisions where target=60").fetchone()[0])
        self.assertEqual(payload["admission_blocker"], "fee-series unavailable")
        self.assertEqual(payload["cohorts"], [])
        self.assertEqual(len(payload["reversals"]), 1)

    def test_data_only_suppresses_reversal_and_cohort_registration(self):
        driver, engine = self.make_driver(data_only=True)
        self.enter_reversal_position(engine)
        self.set_future(driver, engine, self.chart_result())
        signal = self.signal(pure=True, direction="no", confirmed=True)
        with patch.object(feed, "evaluate", return_value=signal), patch.object(feed.time, "time", return_value=85):
            driver.cohorts(85, blocker=None)

        position = next(a["position"] for a in engine.status(85)["arms"] if a["timeframe"] == "1m-sma-reversal")
        self.assertIsNone(position.get("reversal_trigger"))
        self.assertEqual(engine.db.execute("select count(*) from cohorts where cohort_id like '%:60'").fetchone()[0], 0)
        payload = json.loads(engine.db.execute("select payload from signal_decisions where target=60").fetchone()[0])
        self.assertEqual(payload["admission_blocker"], "data_only_preflight")
        self.assertEqual(payload.get("reversals", []), [])
        self.assertEqual(payload["cohorts"], [])

    def test_successful_signal_maps_pure_and_confirmed_sides_to_each_pair(self):
        driver, engine = self.make_driver()
        self.set_current_market(driver)
        self.set_future(driver, engine, self.chart_result())
        signal = self.signal(pure=True, direction="yes", confirmed=True, zone_direction="yes")
        with patch.object(feed, "evaluate", return_value=signal), patch.object(feed.time, "time", return_value=85):
            driver.cohorts(85, blocker=None)

        rows = engine.db.execute(
            "select timeframe,side from cohorts where cohort_id like '%:60' order by timeframe"
        ).fetchall()
        self.assertEqual(
            [(row["timeframe"], row["side"]) for row in rows],
            [
                ("1m-sma", "yes"),
                ("1m-sma-reversal", "yes"),
                ("1m-sma-stoch", "yes"),
                ("1m-sma-stoch-reversal", "yes"),
            ],
        )
        payload = json.loads(engine.db.execute("select payload from signal_decisions where target=60").fetchone()[0])
        self.assertEqual(len(payload["cohorts"]), 4)
        self.assertEqual(len(payload["reversals"]), 1)
        self.assertEqual(engine.db.execute("select applied from signal_decisions where target=60").fetchone()[0], 1)

    def test_coverage_counts_update_incrementally_for_pending_changes_and_pruning(self):
        driver, engine = self.make_driver()
        payload_a = json.dumps({"error": "source error", "admission_blocker": "blocked",
                                "signal": {"pure_qualifies": True, "confirmed_qualifies": False}})
        payload_b = json.dumps({"signal": {"pure_qualifies": True, "confirmed_qualifies": True}})
        old_payload = json.dumps({"signal": {"pure_qualifies": True}})
        with engine._tx():
            engine.db.execute("INSERT INTO signal_decisions(target) VALUES(60)")
            engine.db.execute("INSERT INTO signal_decisions(target,payload,applied) VALUES(120,?,0)", (payload_a,))
            engine.db.execute("INSERT INTO signal_decisions(target,payload,applied) VALUES(1,?,1)", (old_payload,))
            engine.db.execute("INSERT INTO signal_decisions(target,payload,applied) VALUES(180,?,1)", (payload_b,))

        database = engine.db
        class GuardCursor:
            def __init__(self, cursor, sql): self.cursor, self.sql = cursor, sql.lower()
            def fetchmany(self, size):
                self.assert_bounded(size)
                return self.cursor.fetchmany(size)
            def fetchall(self):
                if "signal_decisions" in self.sql and "limit 64" not in self.sql:
                    raise AssertionError("unbounded signal decision fetchall")
                return self.cursor.fetchall()
            @staticmethod
            def assert_bounded(size):
                if size > 64: raise AssertionError("decision payload batch exceeds 64")
            def __iter__(self): return iter(self.cursor)
            def __getattr__(self, name): return getattr(self.cursor, name)
        class GuardDatabase:
            def execute(self, sql, *args): return GuardCursor(database.execute(sql, *args), sql)
            def __getattr__(self, name): return getattr(database, name)
        engine.db = GuardDatabase()
        try:
            with patch.object(feed.json, "loads", wraps=json.loads) as loads:
                initial = driver.coverage_status()
                decoded = loads.call_count
                unchanged = driver.coverage_status()
                self.assertEqual(loads.call_count, decoded)
        finally:
            engine.db = database
        for summary in (initial, unchanged):
            self.assertEqual(summary["completed_minute_decisions"], 3)
            self.assertEqual(summary["pending"], 1)
            self.assertEqual(summary["pure_sma_signals"], 3)
            self.assertEqual(summary["confirmed_zone_exit_signals"], 1)
            self.assertEqual(summary["source_errors"], 1)
            self.assertEqual(summary["admission_blocked"], 1)

        driver._recover_decisions()
        after_recovery = driver.coverage_status()
        self.assertEqual(after_recovery["completed_minute_decisions"], 4)
        self.assertEqual(after_recovery["pending"], 0)
        self.assertEqual(after_recovery["source_errors"], 2)

        driver._save_decision(120, 130, {"signal": {"pure_qualifies": False,
                                                       "confirmed_qualifies": False}})
        after_change = driver.coverage_status()
        self.assertEqual(after_change["pure_sma_signals"], 2)
        self.assertEqual(after_change["source_errors"], 1)
        self.assertEqual(after_change["admission_blocked"], 0)

        self.assertEqual(driver._prune_decision_evidence(10), 1)
        after_prune = driver.coverage_status()
        self.assertEqual(after_prune["completed_minute_decisions"], 3)
        self.assertEqual(after_prune["pure_sma_signals"], 1)

    def test_restored_close_compares_utc_instants_and_still_rejects_real_conflict(self):
        driver, _ = self.make_driver()
        active = {"ticker": "KXBTC15M-TEST", "exchange_index": 2,
                  "open_time": feed.shared.iso(0), "close_time": "1970-01-01T00:15:00Z"}
        driver.markets[active["ticker"]] = {"close_time": feed.shared.iso(900)}
        record = {"kind": "markets", "capture_wall_ns": 50_000_000_000,
                  "source_hash": digest("market"), "valid": True}
        with patch.object(feed.shared, "decode", return_value={"markets": [active]}), \
             patch.object(feed.shared.fast, "_market", return_value=active):
            driver.consume(record, now=50.1)
        self.assertEqual(driver.current_market["close_time"], active["close_time"])
        driver.markets[active["ticker"]] = {"close_time": feed.shared.iso(901)}
        with patch.object(feed.shared, "decode", return_value={"markets": [active]}), \
             patch.object(feed.shared.fast, "_market", return_value=active):
            with self.assertRaisesRegex(ValueError, "market close conflict"):
                driver.consume(record, now=50.1)

    def test_market_discovery_diagnostics_count_bounded_rejections_and_acceptance(self):
        driver, _ = self.make_driver()
        active = {"ticker": "KXBTC15M-TEST", "exchange_index": 2,
                  "open_time": feed.shared.iso(0), "close_time": feed.shared.iso(900)}
        def record(at, *, valid=True):
            return {"kind": "markets", "capture_wall_ns": int(at * 1e9),
                    "source_hash": digest(str(at)), "valid": valid}

        driver.started_at = 15
        driver.consume(record(10), now=10.1)
        driver.consume(record(20, valid=False), now=20.1)
        driver.started_at = 0
        with patch.object(feed.shared, "decode", return_value={"markets": []}):
            driver.consume(record(30), now=30.1)
        with patch.object(feed.shared, "decode", return_value={"markets": [active, active]}):
            driver.consume(record(40), now=40.1)
        with patch.object(feed.shared, "decode", side_effect=ValueError("bad payload")):
            with self.assertRaisesRegex(ValueError, "bad payload"):
                driver.consume(record(45), now=45.1)
        def mutate_hash(discovery, now):
            discovery["source_hash"] = digest("mutated")
            return {"markets": []}
        with patch.object(feed.shared, "decode", side_effect=mutate_hash):
            with self.assertRaisesRegex(ValueError, "market discovery hash changed"):
                driver.consume(record(47), now=47.1)
        with patch.object(feed.shared, "decode", return_value={"markets": [active]}), \
             patch.object(feed.shared.fast, "_market", return_value={**active, "source_hash": digest("50")}):
            driver.consume(record(50), now=50.1)
        driver.consume(record(60), now=60.6)

        status = driver.market_discovery_status()
        self.assertEqual(status["reason_counts"], {
            "before_start": 1, "invalid_upstream": 1, "no_active_market": 1,
            "ambiguous_active_market": 1, "validation_failure": 1, "hash_failure": 1,
            "accepted": 1, "consumer_lag": 1,
        })
        self.assertEqual(status["latest"]["reason"], "consumer_lag")
        self.assertEqual(status["latest"]["capture_time"], feed.shared.iso(60))
        self.assertEqual(status["latest"]["source_hash"], digest("60"))
        self.assertAlmostEqual(status["latest"]["processing_lag_seconds"], .6)
        self.assertEqual(status["last_reject_reason"], "consumer_lag")


if __name__ == "__main__":
    unittest.main()
