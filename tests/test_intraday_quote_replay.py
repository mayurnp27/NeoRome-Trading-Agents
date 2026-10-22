from datetime import datetime, timedelta, timezone
import unittest

from desks.equities.intraday_quote_replay import (
    DEFAULT_LATENCY,
    ReplayValidationError,
    replay_trade,
)


UTC = timezone.utc
BASE = datetime(2026, 9, 21, 13, 30, tzinfo=UTC)


def iso(seconds: float) -> str:
    return (BASE + timedelta(seconds=seconds)).isoformat()


def quote(seconds: float, *, bid=99.0, ask=100.0, bid_size=10.0, ask_size=10.0, **extra):
    row = {
        "symbol": "MU",
        "timestamp": iso(seconds),
        "bid": bid,
        "ask": ask,
        "bid_size": bid_size,
        "ask_size": ask_size,
    }
    row.update(extra)
    return row


def candidate(*, signal=0.0, exit_decision=10.0):
    return {
        "symbol": "MU",
        "signal_timestamp": iso(signal),
        "exit_decision_timestamp": iso(exit_decision),
    }


class IntradayQuoteReplayTests(unittest.TestCase):
    def test_default_250ms_latency_selects_first_eligible_ask_and_bid(self):
        result = replay_trade(
            candidate(),
            [quote(0.24, ask=101), quote(0.25, ask=102), quote(10.24, bid=98), quote(10.25, bid=97)],
        )
        self.assertEqual(result.entry.timestamp, BASE + timedelta(seconds=0.25))
        self.assertEqual(result.entry.price, 102)
        self.assertEqual(result.exit.timestamp, BASE + timedelta(seconds=10.25))
        self.assertEqual(result.exit.price, 97)
        self.assertTrue(result.complete)
        self.assertEqual(result.reasons, ())
        self.assertEqual(DEFAULT_LATENCY, timedelta(milliseconds=250))

    def test_one_second_and_five_second_latency(self):
        rows = [
            quote(1.0, ask=101),
            quote(1.01, ask=102),
            quote(5.0, ask=103),
            quote(5.01, ask=104),
            quote(15.0, bid=98),
            quote(15.01, bid=97),
            quote(19.0, bid=96),
            quote(19.01, bid=95),
        ]
        one = replay_trade(candidate(exit_decision=14), rows, latency=timedelta(seconds=1))
        five = replay_trade(candidate(exit_decision=14), rows, latency=timedelta(seconds=5))
        self.assertEqual(one.entry.price, 101)
        self.assertEqual(one.exit.price, 98)
        self.assertEqual(five.entry.price, 103)
        self.assertEqual(five.exit.price, 96)

    def test_stale_quote_is_rejected_and_counted(self):
        result = replay_trade(candidate(), [quote(-2), quote(0.25, ask=101), quote(10.25, bid=98)])
        self.assertEqual(result.entry.price, 101)
        self.assertGreaterEqual(result.rejection_counts["entry"].get("stale", 0), 1)

    def test_crossed_quote_is_rejected_and_counted(self):
        result = replay_trade(
            candidate(),
            [quote(0.25, bid=101, ask=100), quote(0.26, ask=102), quote(10.25, bid=98)],
        )
        self.assertEqual(result.entry.price, 102)
        self.assertEqual(result.rejection_counts["entry"]["crossed"], 1)

    def test_zero_size_quote_is_rejected_and_counted(self):
        result = replay_trade(
            candidate(),
            [quote(0.25, ask_size=0), quote(0.26, ask=102), quote(10.25, bid=98)],
        )
        self.assertEqual(result.entry.price, 102)
        self.assertEqual(result.rejection_counts["entry"]["zero_or_negative_size"], 1)

    def test_missing_exit_is_an_unresolved_failure(self):
        result = replay_trade(candidate(), [quote(0.25, ask=101)])
        self.assertTrue(result.entry)
        self.assertIsNone(result.exit)
        self.assertTrue(result.unresolved)
        self.assertIn("exit_unresolved_no_valid_quote", result.reasons)

    def test_selection_is_chronological_and_has_no_lookahead(self):
        # Input is intentionally reverse ordered.  The earliest valid quote
        # after the cutoff must win, regardless of later prices.
        result = replay_trade(
            candidate(),
            [quote(1.0, ask=110), quote(0.251, ask=101), quote(10.3, bid=90), quote(10.25, bid=98)],
        )
        self.assertEqual(result.entry.price, 101)
        self.assertEqual(result.exit.price, 98)

    def test_equal_timestamp_order_is_deterministic(self):
        result = replay_trade(
            candidate(),
            [quote(0.25, ask=102), quote(0.25, ask=101), quote(10.25, bid=98)],
        )
        self.assertEqual(result.entry.price, 102)
        self.assertEqual(result.entry.quote_index, 0)

    def test_admission_requires_receipts_size_units_and_order_lifetime(self):
        rows = [quote(0.25, ask=101), quote(10.25, bid=98)]
        with self.assertRaises(ReplayValidationError):
            replay_trade(candidate(), rows, admission=True)
        with self.assertRaises(ReplayValidationError):
            replay_trade(candidate(), rows, admission=True, order_lifetime=timedelta(seconds=1))
        result = replay_trade(
            candidate(),
            rows,
            admission=True,
            order_lifetime=timedelta(seconds=1),
            size_units_verified=True,
        )
        self.assertTrue(result.unresolved)
        self.assertEqual(result.rejection_counts["entry"]["missing_received_at"], 1)

    def test_order_lifetime_prevents_scanning_forward_for_a_convenient_quote(self):
        result = replay_trade(
            candidate(),
            [quote(3, ask=101, received_at=iso(3)), quote(13, bid=98, received_at=iso(13))],
            admission=True,
            order_lifetime=timedelta(seconds=1),
            size_units_verified=True,
        )
        self.assertTrue(result.unresolved)
        self.assertEqual(result.rejection_counts["entry"]["order_lifetime_expired"], 1)
        self.assertEqual(result.rejection_counts["exit"]["order_lifetime_expired"], 1)


if __name__ == "__main__":
    unittest.main()
