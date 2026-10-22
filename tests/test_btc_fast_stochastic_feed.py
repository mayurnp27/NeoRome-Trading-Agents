"""Bounded public driver and isolated-lifecycle regression checks."""
from datetime import datetime, timezone
from email.utils import format_datetime
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
import urllib.parse
from unittest.mock import patch

from neorome import btc_fast_stochastic_feed as feed

UTC = timezone.utc


class FeedTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def test_unknown_endpoint_and_non_native_ticker_fail_before_request(self):
        client = feed.PublicData(self.root)
        with patch.object(client.opener, "open", side_effect=AssertionError("network forbidden")):
            for kind, ticker in (("orders", None), ("market", "../../portfolio/orders")):
                with self.assertRaises(feed.FeedError):
                    client.capture(kind, ticker)
        self.assertEqual(client.calls, 0)

    def test_redirects_are_refused(self):
        with self.assertRaises(feed.FeedError):
            feed.NoRedirect().redirect_request(None, None, 302, "moved", {}, "https://other.example/")

    def test_chart_uses_actual_mcp_and_never_a_direct_http_fallback(self):
        from tests.test_tradingview_fast_signal import make_record
        now = datetime.now(UTC)
        record = make_record(now)
        reply = {"data": record["data"], "mcp_server": {"name": "TradingView BTCUSD Fast Paper Feed"},
                 "tool": "fast_stochastic", "mcp_protocol_version": "2025-06-18",
                 "mcp_transcript_sha256": "b" * 64}
        client = feed.PublicData(self.root)
        with patch.object(feed.tv_signal, "capture", return_value=reply) as call, \
             patch.object(client.opener, "open", side_effect=AssertionError("direct HTTP forbidden")):
            captured = client.capture("chart")
        call.assert_called_once_with()
        self.assertEqual(captured["provider"], "tradingview_mcp")
        self.assertEqual(client.calls, 1)
        with self.assertRaises(feed.FeedError):
            client.capture("candles")

    def test_exhausted_daily_budget_makes_no_request(self):
        (self.root / "public-data-budget.json").write_text(json.dumps({
            "date": datetime.now(UTC).date().isoformat(), "reserved_requests": 2500}))
        client = feed.PublicData(self.root)
        with patch.object(client.opener, "open", side_effect=AssertionError("network forbidden")):
            with self.assertRaises(feed.FeedError):
                client.capture("series")
        self.assertEqual(client.calls, 0)

    def test_failed_attempt_keeps_its_request_reservation(self):
        client = feed.PublicData(self.root)
        with patch.object(client.opener, "open", side_effect=OSError("offline")):
            with self.assertRaises(OSError):
                client.capture("series")
        saved = json.loads((self.root / "public-data-budget.json").read_text())
        self.assertEqual(saved["reserved_requests"], 1)

    def test_flat_between_boundaries_is_zero_requests_and_zero_fills(self):
        class FixedTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return cls(2026, 10, 14, 5, 3, tzinfo=UTC)

        (self.root / feed.tv_signal.STATE_NAME).write_text("{}")
        with patch.object(feed, "datetime", FixedTime), patch.object(
                feed.PublicData, "capture", side_effect=AssertionError("network forbidden")):
            result = feed.cycle(self.root)
        self.assertEqual(result["reason"], "waiting_next_5m_boundary")
        self.assertEqual(result["provider_requests_this_cycle"], 0)
        self.assertEqual(result["venue_orders_sent"], 0)
        self.assertIsNone(result["snapshot"]["active_position"])
        self.assertEqual(result["snapshot"]["ledger_snapshot"]["paper_nav_cents"], 50000)
        archived = list((self.root / "cycles").glob("*.json"))
        self.assertEqual(len(archived), 1)
        self.assertEqual(json.loads(archived[0].read_text())["reason"], result["reason"])

    def test_quote_book_keeps_exact_market_for_marks(self):
        record = {"ticker": "KXBTC15M-26OCT140115-15", "rest_date": "date", "received_at": "date",
                  "source_hash": "a" * 64, "data": {"orderbook_fp": {"yes_dollars": [], "no_dollars": []}}}
        self.assertEqual(feed.book(record)["ticker"], record["ticker"])

    def test_pending_wrapper_supplies_matching_market_identity(self):
        ticker = "KXBTC15M-26OCT140115-15"
        before = {"active_position": None, "pending_reservation": {
            "instrument": ticker, "expiry_at": "2099-10-14T05:15:00+00:00"}, "pending_entry_deadline":"2099-10-14T05:01:30+00:00"}
        captures = [{"data": {"market": {"ticker": ticker, "status": "active"}}},
                    {"ticker": ticker, "rest_date": "date", "received_at": "date", "source_hash": "a" * 64,
                     "data": {"orderbook_fp": {"yes_dollars": [], "no_dollars": []}}}]
        with patch.object(feed.paper, "initialize"), patch.object(feed.paper, "snapshot", return_value=before), \
             patch.object(feed.PublicData, "capture", side_effect=captures), \
             patch.object(feed, "entry_health", return_value=None), \
             patch.object(feed.paper, "run_once", return_value={"status": "pending"}) as advance:
            feed.cycle(self.root)
        self.assertEqual(advance.call_args.args[0]["market"]["ticker"], ticker)
        self.assertEqual(advance.call_args.args[0]["book2"]["ticker"], ticker)

    def test_expired_pending_cancels_without_external_data(self):
        before = {"active_position": None, "pending_reservation": {
            "instrument": "KXBTC15M-26OCT140115-15", "expiry_at": "2026-10-14T05:15:00+00:00"}, "pending_entry_deadline":"2026-10-14T05:01:30+00:00"}
        with patch.object(feed.paper, "initialize"), patch.object(feed.paper, "snapshot", return_value=before), \
             patch.object(feed.PublicData, "capture", side_effect=AssertionError("network forbidden")), \
             patch.object(feed, "entry_health", return_value=None), \
             patch.object(feed.paper, "run_once", return_value={"status": "cancelled"}) as advance:
            result = feed.cycle(self.root)
        self.assertEqual(result["status"], "cancelled")
        self.assertEqual(result["provider_requests_this_cycle"], 0)
        self.assertEqual(advance.call_args.args[0], {"entry_blocker_reason": None})

    def test_current_extreme_turn_routes_bullish_yes_and_bearish_no(self):
        class FixedTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return cls(2026, 10, 14, 12, 5, 10, tzinfo=UTC)

        for direction, side, k, prior in (("bullish", "yes", 22, 18), ("bearish", "no", 78, 82)):
            with self.subTest(direction=direction):
                ticker = "KXBTC15M-26OCT141215-15"
                signal = {"bar_close_timestamp": int(FixedTime.now().timestamp()) - 10,
                          "k": k, "previous_k": prior, "direction": direction, "contract_side": side}
                def candidates(*args, **kwargs):
                    return [signal] if kwargs["direction"] == direction else []
                core = SimpleNamespace(scan_signals=lambda *args: {"evaluations": []},
                                       signals_for_timeframe=candidates)
                def capture(kind, ticker=None):
                    record = {"source_hash": "a" * 64, "received_at": FixedTime.now().isoformat(),
                              "rest_date": FixedTime.now().isoformat(), "ticker": ticker}
                    data = {"candles": [], "chart": {"data": [{"s": "COINBASE:BTCUSD", "d": [k, k, k, k, 80000]}]},
                            "series": {"series": {"ticker": "KXBTC15M", "exchange_index": 2,
                                                   "fee_type": "quadratic", "fee_multiplier": 1}},
                            "markets": {"markets": [{"ticker": "KXBTC15M-26OCT141215-15",
                                                       "open_time": "2026-10-14T12:00:00+00:00"}]},
                            "book": {"orderbook_fp": {"yes_dollars": [], "no_dollars": []}}}
                    return record | {"data": data[kind]}
                before = {"active_position": None, "pending_reservation": None}
                with patch.object(feed, "datetime", FixedTime), patch.object(feed.tv_signal, "evaluate", return_value={"selected_signal": signal, "reason": "diagnostic", "chart_context": {}}), \
                     patch.object(feed.paper, "initialize"), patch.object(feed.paper, "snapshot", return_value=before.copy()), \
                     patch.object(feed, "entry_health", return_value=None), patch.object(feed.time, "sleep"), \
                     patch.object(feed.PublicData, "capture", side_effect=capture), \
                     patch.object(feed.paper, "run_once", return_value={"status": "filled"}) as advance:
                    result = feed.cycle(self.root)
                self.assertEqual(result["status"], "filled")
                self.assertEqual(result["selected_signal"]["contract_side"], side)
                self.assertEqual(advance.call_args.args[0]["signal"]["direction"], direction)


if __name__ == "__main__":
    unittest.main()
