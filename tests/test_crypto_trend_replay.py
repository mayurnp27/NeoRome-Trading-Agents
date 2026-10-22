import unittest
from datetime import date, timedelta
from decimal import Decimal

from desks.crypto.trend_replay import CostAssumptions, ReplayError, cost_stress, replay


def make_bars(count=110):
    start = date(2026, 1, 1)
    bars = []
    for index in range(count):
        close = Decimal("100")
        if index == 100:
            close = Decimal("130")
        elif index == 101:
            close = Decimal("131")
        elif index == 102:
            close = Decimal("90")
        bars.append(
            {
                "timestamp": f"{(start + timedelta(days=index)).isoformat()}T00:00:00Z",
                "complete": True,
                "open": str(close - 1),
                "high": str(close + 1),
                "low": str(close - 2),
                "close": str(close),
            }
        )
    return bars


class CryptoTrendReplayTests(unittest.TestCase):
    def test_entry_and_exit_use_next_day_open(self):
        bars = make_bars()
        # The entry signal is on day 100; its fill must use day 101 open.
        bars[101]["open"] = "77"
        bars[101]["low"] = "76"
        # Exit signal is on day 102; there is no same-close 90 fill.
        bars[103]["open"] = "88"
        bars[103]["low"] = "87"
        result = replay(bars, costs=CostAssumptions(fee_bps=0, slippage_bps=0))
        self.assertEqual(result["closed_trade_count"], 1)
        trade = result["trades"][0]
        self.assertEqual(trade["entry_signal_day"], bars[100]["timestamp"])
        self.assertEqual(trade["entry_execution_day"], bars[101]["timestamp"])
        self.assertEqual(trade["entry_reference_open"], "77")
        self.assertEqual(trade["exit_signal_day"], bars[102]["timestamp"])
        self.assertEqual(trade["exit_execution_day"], bars[103]["timestamp"])
        self.assertEqual(trade["exit_reference_open"], "88")
        self.assertTrue(result["execution_enabled"] is False)

    def test_final_signal_is_not_filled_at_same_close_and_terminal_is_explicit(self):
        bars = make_bars(103)
        # Entry executes on day 101. Exit signal on final day 102 has no next open.
        bars[101]["open"] = "120"
        bars[101]["low"] = "119"
        result = replay(bars, costs=CostAssumptions(fee_bps=0, slippage_bps=0))
        self.assertEqual(result["closed_trade_count"], 0)
        self.assertEqual(result["terminal_position"]["status"], "open_terminal")
        self.assertEqual(result["terminal_position"]["exit_execution"], "unavailable_next_utc_day")
        self.assertTrue(result["signals"][-1]["exit_signal"])
        self.assertFalse(result["signals"][-1]["execution_available"])

    def test_lookahead_does_not_create_early_entry(self):
        bars = make_bars()
        # A future spike must not change the first 100-day signal calculations.
        bars[100].update({"open": "99", "high": "101", "low": "98", "close": "100"})
        bars[101].update({"open": "499", "high": "501", "low": "498", "close": "500"})
        result = replay(bars, costs=CostAssumptions(fee_bps=0, slippage_bps=0))
        self.assertEqual(result["trades"][0]["entry_signal_day"], bars[101]["timestamp"])

    def test_gaps_and_bad_ohlc_are_rejected(self):
        bars = make_bars()
        bars[50]["timestamp"] = "2026-03-01T00:00:00Z"
        with self.assertRaises(ReplayError):
            replay(bars)
        bars = make_bars()
        bars[2]["low"] = "999"
        with self.assertRaises(ReplayError):
            replay(bars)

    def test_cost_stress_reduces_result_and_is_deterministic(self):
        bars = make_bars()
        low = CostAssumptions(fee_bps=0, slippage_bps=0)
        high = CostAssumptions(fee_bps=25, slippage_bps=25)
        first = cost_stress(bars, {"zero": low, "high": high})
        second = cost_stress(bars, {"zero": low, "high": high})
        self.assertEqual(first, second)
        self.assertLessEqual(
            first["scenarios"]["high"]["net_pnl_cents"],
            first["scenarios"]["zero"]["net_pnl_cents"],
        )
        self.assertEqual(first["scenarios"]["high"]["cost_assumptions"]["round_trip_bps"], "100")


if __name__ == "__main__":
    unittest.main()
