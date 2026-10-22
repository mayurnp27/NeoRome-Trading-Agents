from __future__ import annotations

import copy
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from desks.crypto.trend_policy import (
    CostAssumptions,
    TrendPolicyError,
    evaluate,
    trend_signal,
)


UTC = timezone.utc
NOW = datetime(2026, 4, 12, 0, 1, tzinfo=UTC)


def make_bars(*, signal_close: str = "102", count: int = 110) -> list[dict[str, object]]:
    first = NOW.date() - timedelta(days=count)
    bars: list[dict[str, object]] = []
    for index in range(count):
        day = first + timedelta(days=index)
        bars.append(
            {
                "timestamp": f"{day.isoformat()}T00:00:00Z",
                "complete": True,
                "open": "100",
                "high": "101",
                "low": "99",
                "close": "100",
            }
        )
    bars[-1].update({"high": signal_close, "close": signal_close})
    return bars


def make_quote(signal_day: str, *, timestamp: str | None = None, **overrides: str) -> dict[str, str]:
    if timestamp is None:
        day = datetime.fromisoformat(signal_day.replace("Z", "+00:00")) + timedelta(days=1)
        timestamp = day.replace(hour=0, minute=0, second=30).isoformat().replace("+00:00", "Z")
    quote = {
        "symbol": "BTC/USD",
        "timestamp": timestamp,
        "bid": "99",
        "ask": "100",
        "depth_units": "1",
    }
    quote.update(overrides)
    return quote


class CryptoTrendPolicyTests(unittest.TestCase):
    def test_entry_uses_prior_windows_and_fractional_cap(self) -> None:
        bars = make_bars()
        signal = trend_signal(bars, now=NOW)
        quote = make_quote(signal["signal_day"], ask="1000", bid="999", depth_units="1")

        result = evaluate(bars, quote, now=NOW)

        self.assertEqual(result["status"], "entry_candidate")
        self.assertFalse(result["execution_enabled"])
        valuation = result["valuation"]
        self.assertGreater(Decimal(valuation["units"]), 0)
        self.assertLess(Decimal(valuation["units"]), 1)
        self.assertLessEqual(valuation["all_in_cost_cents"], 500)
        self.assertEqual(result["cost_assumptions"]["fractional_units"], True)

    def test_exit_signal_and_exit_valuation_are_point_in_time(self) -> None:
        bars = make_bars(signal_close="98")
        bars[-1]["high"] = "101"
        bars[-1]["low"] = "97"
        signal = trend_signal(bars, now=NOW)
        quote = make_quote(signal["signal_day"], bid="100", ask="101", depth_units="0.5")

        result = evaluate(bars, quote, now=NOW, position_units="0.25")

        self.assertEqual(result["status"], "exit_candidate")
        self.assertTrue(result["signal"]["exit_signal"])
        self.assertEqual(result["valuation"]["side"], "sell")
        self.assertEqual(result["valuation"]["units"], "0.25")
        self.assertGreaterEqual(result["valuation"]["net_proceeds_cents"], 0)

    def test_signal_thresholds_do_not_use_signal_bar_high_or_low(self) -> None:
        baseline = make_bars(signal_close="102")
        changed = copy.deepcopy(baseline)
        changed[-1]["high"] = "100000"
        changed[-1]["low"] = "1"

        first = trend_signal(baseline, now=NOW)
        second = trend_signal(changed, now=NOW)

        self.assertEqual(first["prior_100_day_mean"], second["prior_100_day_mean"])
        self.assertEqual(first["prior_20_day_high"], second["prior_20_day_high"])
        self.assertEqual(first["prior_10_day_low"], second["prior_10_day_low"])
        self.assertEqual(first["entry_signal"], second["entry_signal"])

    def test_current_day_and_gaps_are_rejected(self) -> None:
        current_day = make_bars()
        current_day[-1]["timestamp"] = f"{NOW.date().isoformat()}T00:00:00Z"
        with self.assertRaises(TrendPolicyError):
            trend_signal(current_day, now=NOW)

        gap = make_bars()
        gap[40]["timestamp"] = (
            datetime.fromisoformat(gap[39]["timestamp"].replace("Z", "+00:00"))
            + timedelta(days=2)
        ).isoformat().replace("+00:00", "Z")
        with self.assertRaises(TrendPolicyError):
            trend_signal(gap, now=NOW)

    def test_malformed_bars_are_rejected(self) -> None:
        cases = []
        geometry = make_bars()
        geometry[10]["low"] = "100.5"
        cases.append(geometry)
        incomplete = make_bars()
        incomplete[10]["complete"] = False
        cases.append(incomplete)
        nonfinite = make_bars()
        nonfinite[10]["close"] = "NaN"
        cases.append(nonfinite)
        for bars in cases:
            with self.subTest(bars=bars[10]):
                with self.assertRaises(TrendPolicyError):
                    trend_signal(bars, now=NOW)

    def test_insufficient_history_and_unusable_quotes_abstain(self) -> None:
        short = make_bars(count=100)
        self.assertEqual(evaluate(short, None, now=NOW)["reason"], "insufficient_history")

        bars = make_bars()
        signal = trend_signal(bars, now=NOW)
        same_close = make_quote(signal["signal_day"], timestamp=signal["signal_day"])
        stale = make_quote(signal["signal_day"], timestamp="2026-04-11T00:00:00Z")
        for quote in (same_close, stale):
            with self.subTest(quote=quote):
                result = evaluate(bars, quote, now=NOW)
                self.assertEqual(result["status"], "abstain")
                self.assertIn("quote", result["reason"])

    def test_stale_history_abstains_even_with_fresh_current_quote(self) -> None:
        bars = make_bars()
        for bar in bars:
            day = datetime.fromisoformat(bar["timestamp"].replace("Z", "+00:00")) - timedelta(days=1)
            bar["timestamp"] = day.isoformat().replace("+00:00", "Z")
        old_signal_day = bars[-1]["timestamp"]
        fresh_quote = make_quote(old_signal_day, timestamp="2026-04-12T00:00:30Z")

        result = evaluate(bars, fresh_quote, now=NOW)

        self.assertEqual(result["status"], "abstain")
        self.assertEqual(result["reason"], "stale_history")

    def test_quote_older_than_sixty_seconds_abstains(self) -> None:
        bars = make_bars()
        signal = trend_signal(bars, now=NOW)
        evaluation_now = NOW.replace(minute=2, second=1)
        quote = make_quote(signal["signal_day"], timestamp="2026-04-12T00:01:00Z")

        result = evaluate(bars, quote, now=evaluation_now)

        self.assertEqual(result["status"], "abstain")
        self.assertEqual(result["reason"], "quote is stale")

    def test_cost_cap_and_depth_are_explicit(self) -> None:
        bars = make_bars()
        signal = trend_signal(bars, now=NOW)
        costs = CostAssumptions(fee_bps=Decimal("25"), slippage_bps=Decimal("25"))
        shallow = make_quote(signal["signal_day"], ask="100", depth_units="0.0000001")
        result = evaluate(bars, shallow, now=NOW, costs=costs)
        self.assertEqual(result["status"], "abstain")
        self.assertEqual(result["reason"], "insufficient_depth_or_cap_for_fractional_units")
        self.assertEqual(result["cost_assumptions"]["fee_bps_per_side"], "25")
        self.assertEqual(result["cost_assumptions"]["slippage_bps_per_side"], "25")

        expensive = make_quote(signal["signal_day"], ask="10000000", depth_units="1")
        result = evaluate(bars, expensive, now=NOW, costs=costs)
        self.assertEqual(result["status"], "abstain")


if __name__ == "__main__":
    unittest.main()
