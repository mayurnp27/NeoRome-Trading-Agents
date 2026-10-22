import copy
import unittest
from datetime import date, datetime, time, timedelta, timezone

from desks.equities.paper_policy import evaluate, value_position


UTC = timezone.utc


def _iso(moment):
    return moment.isoformat().replace("+00:00", "Z")


def _calendar(start, end, *, early_close=None, open_at="13:30:00Z", close_at="20:00:00Z"):
    holidays = {date(2026, 9, 7)}
    rows = []
    current = start
    while current <= end:
        if current.weekday() < 5 and current not in holidays:
            opened = datetime.combine(current, time.fromisoformat(open_at[:-1]), UTC)
            close_text = close_at if early_close != current else "17:00:00Z"
            closed = datetime.combine(current, time.fromisoformat(close_text[:-1]), UTC)
            rows.append({"date": current.isoformat(), "open_at": _iso(opened), "close_at": _iso(closed)})
        current += timedelta(days=1)
    return rows


def fixture(*, current=date(2026, 9, 1), current_time=time(14, 0), trend=True, source_kind="synthetic"):
    start = date(2025, 4, 1)
    end = max(date(2026, 9, 18), current + timedelta(days=20))
    sessions = _calendar(start, end, early_close=date(2026, 9, 1))
    today_row = next(row for row in sessions if row["date"] == current.isoformat())
    now = datetime.combine(current, current_time, UTC)
    prior_dates = [row["date"] for row in sessions if row["date"] < current.isoformat()]
    closes = ["90" for _ in range(200)]
    if trend:
        closes[-1] = "100"
    bars = [
        {
            "timestamp": date_text + "T04:00:00Z",
            "o": close,
            "h": close,
            "l": close,
            "c": close,
            "volume": "1000000",
        }
        for date_text, close in zip(prior_dates[-200:], closes)
    ]
    observation = {
        "symbol": "SPY",
        "source_kind": source_kind,
        "feed": "iex",
        "bars": bars,
        "quote": {
            "timestamp": _iso(now - timedelta(seconds=5)),
            "bid": "9",
            "ask": "10",
            "bid_size": "100",
            "ask_size": "100",
        },
        "observed_at": _iso(now - timedelta(seconds=5)),
    }
    context = {
        "received_at": _iso(now - timedelta(seconds=5)),
        "asset": {
            "symbol": "SPY",
            "asset_class": "us_equity",
            "status": "active",
            "tradable": True,
            "fractionable": True,
            "exchange": "ARCA",
        },
        "clock": {"timestamp": _iso(now - timedelta(seconds=5)), "is_open": True},
        "sessions": sessions,
        "calendar_start": start.isoformat(),
        "calendar_end": end.isoformat(),
    }
    # Keep the local variable visible in failures; this also documents that
    # the early close is represented by the explicit UTC session row.
    assert today_row["date"] == current.isoformat()
    return observation, context, now


class EquityPaperPolicyTests(unittest.TestCase):
    def test_eligible_lot_has_exact_all_in_fee_and_exit_value(self):
        observation, context, now = fixture()
        observation["quote"]["bid_size"] = "1"
        observation["quote"]["ask_size"] = "1"
        result = evaluate(observation, context, now)
        self.assertTrue(result["eligible_for_internal_paper"])
        self.assertEqual(result["units"], "1.000000")
        self.assertEqual(result["entry_cost_cents"], 1003)  # $10.005 ceil + $0.02 fee
        self.assertEqual(result["entry_fee_cents"], 2)
        self.assertEqual(result["gross_exit_cents"], 899)  # $9 * 0.9995, floored
        self.assertEqual(result["exit_fee_cents"], 1)
        self.assertEqual(result["liquidation_value_cents"], 898)
        self.assertEqual(result["broker_execution_enabled"], False)
        self.assertTrue(result["market_inputs_valid"])
        self.assertTrue(result["rebalance_due"])
        self.assertTrue(result["trend_positive"])

    def test_value_position_clips_minimum_exit_fee_to_gross(self):
        quote = {"bid": "0.00001", "ask": "0.00002", "bid_size": "1", "ask_size": "1"}
        self.assertEqual(value_position("1", quote), {
            "gross_exit_cents": 0,
            "exit_fee_cents": 0,
            "liquidation_value_cents": 0,
        })

    def test_value_position_rejects_exit_depth_shortfall(self):
        quote = {"bid": "10", "ask": "11", "bid_size": "0.5", "ask_size": "1"}
        with self.assertRaises(ValueError):
            value_position("0.500001", quote)

    def test_fractional_units_floor_to_six_places_and_stay_under_fifteen_dollars(self):
        observation, context, now = fixture()
        observation["quote"].update({"bid": "99", "ask": "100", "bid_size": "100", "ask_size": "100"})
        result = evaluate(observation, context, now)
        self.assertEqual(result["units"], "0.149725")
        self.assertLessEqual(result["entry_cost_cents"], 1500)
        self.assertEqual(len(result["units"].split(".")[1]), 6)

    def test_extremely_cheap_quote_uses_bounded_microshare_search(self):
        observation, context, now = fixture()
        observation["quote"].update({"bid": "0.00001", "ask": "0.00001", "bid_size": "100000000", "ask_size": "100000000"})
        result = evaluate(observation, context, now)
        self.assertTrue(result["eligible_for_internal_paper"])
        self.assertLessEqual(result["entry_cost_cents"], 1500)

    def test_holiday_and_early_close_are_calendar_data_not_weekday_guesses(self):
        observation, context, now = fixture()
        current = next(row for row in context["sessions"] if row["date"] == "2026-09-01")
        self.assertEqual(current["close_at"], "2026-09-01T17:00:00Z")
        self.assertNotIn("2026-09-07", {row["date"] for row in context["sessions"]})
        self.assertTrue(evaluate(observation, context, now)["eligible_for_internal_paper"])

    def test_explicit_utc_session_handles_dst_open(self):
        observation, context, now = fixture(current=date(2026, 11, 2), current_time=time(15, 0))
        for row in context["sessions"]:
            if row["date"] == "2026-11-02":
                row["open_at"] = "2026-11-02T14:30:00Z"
                row["close_at"] = "2026-11-02T21:00:00Z"
                break
        result = evaluate(observation, context, now)
        self.assertTrue(result["eligible_for_internal_paper"])

    def test_missing_calendar_day_and_bars_fail_closed_without_bootstrap(self):
        observation, context, now = fixture()
        context["sessions"] = [row for row in context["sessions"] if row["date"] != "2026-08-31"]
        result = evaluate(observation, context, now)
        self.assertFalse(result["eligible_for_internal_paper"])
        self.assertFalse(result["market_inputs_valid"])
        self.assertIn("session dates", " ".join(result["reasons"]).lower())

        observation, context, now = fixture()
        observation["bars"] = observation["bars"][:-1]
        result = evaluate(observation, context, now)
        self.assertFalse(result["eligible_for_internal_paper"])
        self.assertIn("200", " ".join(result["reasons"]))

    def test_stale_and_future_timestamps_fail_closed(self):
        observation, context, now = fixture()
        context["received_at"] = _iso(now - timedelta(seconds=61))
        result = evaluate(observation, context, now)
        self.assertFalse(result["eligible_for_internal_paper"])
        self.assertIn("received_at", " ".join(result["reasons"]))

        observation, context, now = fixture()
        observation["quote"]["timestamp"] = _iso(now + timedelta(seconds=1))
        result = evaluate(observation, context, now)
        self.assertFalse(result["eligible_for_internal_paper"])
        self.assertIn("future", " ".join(result["reasons"]).lower())

        observation, context, now = fixture()
        observation["observed_at"] = _iso(now - timedelta(seconds=61))
        result = evaluate(observation, context, now)
        self.assertFalse(result["eligible_for_internal_paper"])
        self.assertIn("observed_at", " ".join(result["reasons"]))

    def test_quote_must_be_inside_current_regular_session_and_before_receipt(self):
        observation, context, now = fixture()
        observation["quote"]["timestamp"] = "2026-09-01T13:00:00Z"
        context["received_at"] = "2026-09-01T13:00:05Z"
        context["clock"]["timestamp"] = "2026-09-01T13:00:05Z"
        result = evaluate(observation, context, now)
        self.assertFalse(result["eligible_for_internal_paper"])
        self.assertIn("outside", " ".join(result["reasons"]).lower())

        observation, context, now = fixture()
        context["received_at"] = _iso(now - timedelta(seconds=10))
        observation["quote"]["timestamp"] = _iso(now - timedelta(seconds=3))
        observation["observed_at"] = _iso(now - timedelta(seconds=1))
        context["clock"]["timestamp"] = _iso(now - timedelta(seconds=2))
        result = evaluate(observation, context, now)
        self.assertTrue(result["eligible_for_internal_paper"])

    def test_missing_asset_flags_and_exchange_fail_closed(self):
        observation, context, now = fixture()
        context["asset"]["fractionable"] = False
        context["asset"]["exchange"] = "OTC"
        result = evaluate(observation, context, now)
        self.assertFalse(result["eligible_for_internal_paper"])
        reasons = " ".join(result["reasons"]).lower()
        self.assertIn("fractionable", reasons)
        self.assertIn("exchange", reasons)

    def test_monthly_gate_and_trend_signal_are_separate_from_market_validity(self):
        observation, context, now = fixture(current=date(2026, 9, 2))
        result = evaluate(observation, context, now)
        self.assertFalse(result["eligible_for_internal_paper"])
        self.assertTrue(result["market_inputs_valid"])
        self.assertFalse(result["rebalance_due"])
        self.assertTrue(result["trend_positive"])

        observation, context, now = fixture(trend=False)
        result = evaluate(observation, context, now)
        self.assertFalse(result["eligible_for_internal_paper"])
        self.assertTrue(result["market_inputs_valid"])
        self.assertTrue(result["rebalance_due"])
        self.assertFalse(result["trend_positive"])

    def test_ohlc_and_quote_validation(self):
        observation, context, now = fixture()
        observation["bars"][0]["h"] = "1"
        result = evaluate(observation, context, now)
        self.assertFalse(result["eligible_for_internal_paper"])
        self.assertIn("inconsistent", " ".join(result["reasons"]).lower())

        observation, context, now = fixture()
        observation["quote"]["ask"] = "8"
        result = evaluate(observation, context, now)
        self.assertFalse(result["eligible_for_internal_paper"])
        self.assertIn("crossed", " ".join(result["reasons"]).lower())


if __name__ == "__main__":
    unittest.main()
