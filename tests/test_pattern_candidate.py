import unittest
from datetime import datetime, timedelta, timezone

from desks.crypto.pattern_candidate import entry_screen, exit_signal, feature_match

NOW = datetime(2026, 10, 20, tzinfo=timezone.utc)


class PatternCandidateTests(unittest.TestCase):
    def snapshot(self):
        return {"mint": "M", "pool": "P", "observed_at": NOW.isoformat(),
                "available_at": NOW.isoformat(), "source_sha256": "a" * 64,
                "age_hours_at_observation": 3, "liquidity_usd": 50000, "volume_h1_usd": 100000}

    def screen(self, **changes):
        data = self.snapshot()
        data.update(changes)
        return entry_screen(data, mint="M", pool="P", now=NOW)

    def signal(self, net, seconds=60, quote_age=0):
        return exit_signal(basis_cents=500, liquidation_net_cents=net,
                           opened_at=NOW-timedelta(seconds=seconds),
                           quote_at=NOW-timedelta(seconds=quote_age), now=NOW)

    def test_matching_candidate_is_always_nonexecuting(self):
        result = self.screen(execution_enabled=True)
        self.assertTrue(result["research_candidate"])
        self.assertFalse(result["execution_enabled"])
        self.assertEqual(result["action"], "observe_only")

    def test_feature_boundaries(self):
        for liquidity in (25000, 250000):
            self.assertTrue(feature_match(liquidity_usd=liquidity, volume_h1_usd=liquidity, age_hours=0))
        for changes in ({"liquidity_usd": 24999}, {"liquidity_usd": 250001},
                        {"volume_h1_usd": 49999}, {"age_hours_at_observation": 24},
                        {"age_hours_at_observation": -1}):
            self.assertFalse(self.screen(**changes)["research_candidate"])

    def test_malformed_and_nonfinite_features_fail_closed(self):
        for field in ("liquidity_usd", "volume_h1_usd", "age_hours_at_observation"):
            for value in (None, True, "NaN", float("inf"), {}, "bad"):
                self.assertFalse(self.screen(**{field: value})["research_candidate"])

    def test_mismatched_pool_mint_and_missing_hash_fail_closed(self):
        for changes in ({"mint": "OTHER"}, {"pool": "OTHER"}, {"source_sha256": ""}):
            self.assertFalse(self.screen(**changes)["research_candidate"])

    def test_stale_future_and_not_yet_received_fail_closed(self):
        for changes in ({"observed_at": (NOW-timedelta(seconds=61)).isoformat()},
                        {"observed_at": (NOW+timedelta(seconds=1)).isoformat()},
                        {"available_at": (NOW+timedelta(seconds=1)).isoformat()},
                        {"observed_at": "2026-10-20T00:00:00"}):
            self.assertFalse(self.screen(**changes)["research_candidate"])

    def test_pool_age_is_advanced_to_decision(self):
        self.assertFalse(self.screen(age_hours_at_observation="23.999",
            observed_at=(NOW-timedelta(seconds=30)).isoformat())["research_candidate"])

    def test_fee_net_target_stop_and_gap(self):
        self.assertEqual(self.signal(749)["exit_signal"], "hold")
        self.assertEqual(self.signal(750)["exit_signal"], "take_profit_50_net")
        self.assertEqual(self.signal(375)["exit_signal"], "stop_25_net")
        self.assertEqual(self.signal(0)["exit_signal"], "stop_25_net")
        self.assertFalse(self.signal(750)["execution_enabled"])

    def test_time_limit_and_unavailable_quote(self):
        self.assertEqual(self.signal(500, 7199)["exit_signal"], "hold")
        self.assertEqual(self.signal(500, 7200)["exit_signal"], "time_limit_120_minutes")
        result = self.signal(750, 7300, 16)
        self.assertEqual(result["exit_signal"], "quote_unavailable")
        self.assertTrue(result["time_limit_due"])

    def test_invalid_exit_accounting_and_timestamps_rejected(self):
        for net in (-1, 750.0, True):
            with self.assertRaises(ValueError):
                self.signal(net)
        with self.assertRaises(ValueError):
            self.signal(750, seconds=-1)
        with self.assertRaises(ValueError):
            self.signal(750, quote_age=-1)


if __name__ == "__main__":
    unittest.main()
