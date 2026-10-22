import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
import unittest

from neorome.prediction_24x7 import Prediction24x7Error, PublicClient, capture_once, evaluate_market, _fixedpoint_book, _markets, _official_settlement


UTC = timezone.utc
NOW = datetime(2026, 9, 23, 3, 5, tzinfo=UTC)


class FakeClient(PublicClient):
    def __init__(self):
        super().__init__(clock=lambda: NOW)
        self.calls = []

    def get(self, path, params, *, now=None):
        if self.requests >= self.max_requests:
            raise Prediction24x7Error("request_budget_exhausted")
        self.calls.append((path, dict(params)))
        self.requests += 1
        if path.endswith("/markets"):
            series = params["series_ticker"]
            return {"path": path, "params": dict(params), "requested_at": NOW.isoformat(), "received_at": NOW.isoformat(),
                    "sha256": "a" * 64, "raw_response_b64": "", "payload": {"markets": [
                        {"ticker": series + "-TEST", "event_ticker": series + "-EVENT", "status": "active", "close_time": "2026-09-23T03:15:00Z",
                         "rules_primary": "official rule", "rules_secondary": "secondary"}
                    ], "cursor": ""}}
        if "/series/" in path:
            return {"path": path, "params": dict(params), "requested_at": NOW.isoformat(), "received_at": NOW.isoformat(),
                    "sha256": "c" * 64, "raw_response_b64": "", "payload": {"series": {"fee_type": "quadratic", "fee_multiplier": 1}}}
        if "/events/" in path:
            return {"path": path, "params": dict(params), "requested_at": NOW.isoformat(), "received_at": NOW.isoformat(),
                    "sha256": "d" * 64, "raw_response_b64": "", "payload": {"event": {"event_ticker": "KXBTC15M-EVENT", "series_ticker": "KXBTC15M",
                                                                                              "settlement_sources": [{"name": "CF Benchmarks", "url": "https://www.cfbenchmarks.com/"}]}}}
        return {"path": path, "params": dict(params), "requested_at": NOW.isoformat(), "received_at": NOW.isoformat(),
                "sha256": "b" * 64, "raw_response_b64": "", "payload": {"orderbook": {"yes": [[42, 3]], "no": [[58, 3]]}}}


class Prediction24x7Tests(unittest.TestCase):
    def test_real_public_shape_abstains_with_exact_gates(self):
        result = evaluate_market({"market": {"ticker": "KXBTC15M-TEST"}, "orderbook": {"payload": {"orderbook": {"yes": [[42, 1]]}}}})
        self.assertEqual(result["status"], "abstained")
        self.assertIn("venue_event_time_missing", result["abstention_reasons"])
        self.assertIn("calibrated_probability_missing", result["abstention_reasons"])
        self.assertIn("fee_schedule_unverified", result["abstention_reasons"])

    def test_incomplete_market_listing_is_a_blocker(self):
        result = evaluate_market({"market": {"ticker": "KXBTC15M-TEST"}, "orderbook": {"payload": {"orderbook": {"yes": [[42, 1]]}}}, "listing_complete": False})
        self.assertIn("incomplete_market_pagination", result["abstention_reasons"])

    def test_official_event_terms_are_bound_without_inventing_a_fee(self):
        market = {"ticker": "KXBTC15M-TEST", "close_time": "2026-09-23T03:15:00Z", "rules_primary": "official rule", "rules_secondary": "secondary"}
        event = {"payload": {"event": {"event_ticker": "KXBTC15M-EVENT", "series_ticker": "KXBTC15M",
                                         "settlement_sources": [{"name": "CF Benchmarks", "url": "https://www.cfbenchmarks.com/"}]}},
                 "received_at": NOW.isoformat(), "sha256": "d" * 64}
        definition = _official_settlement(market, event)
        self.assertEqual(definition["version"], "kalshi-public-terms-v1")
        self.assertEqual(definition["source_identity"]["sources"][0]["name"], "CF Benchmarks")

    def test_fixed_point_book_conservatively_discards_fractional_remainder(self):
        book = _fixedpoint_book({"orderbook_fp": {"yes_dollars": [["0.40", "2.50"]], "no_dollars": [["0.60", "1.25"]]}})
        self.assertEqual(book["yes_bids"][0]["size"], 2)
        self.assertEqual(book["no_bids"][0]["size"], 1)
        self.assertEqual(book["yes_asks"][0]["price"], "0.40")

    def test_market_selection_prefers_published_whole_contract_depth(self):
        rows = _markets({"markets": [
            {"ticker": "KXHIGHNY-THIN", "status": "active", "yes_bid_size_fp": "0.00", "yes_ask_size_fp": "12.00", "close_time": "2026-09-23T03:00:00Z"},
            {"ticker": "KXHIGHNY-DEEP", "status": "active", "yes_bid_size_fp": "2.50", "yes_ask_size_fp": "3.00", "close_time": "2026-09-23T04:00:00Z"},
        ], "cursor": ""}, "KXHIGHNY")
        self.assertEqual(rows[0]["ticker"], "KXHIGHNY-DEEP")

    def test_capture_is_bounded_immutable_and_replay_safe(self):
        with tempfile.TemporaryDirectory() as root:
            client = FakeClient()
            first = capture_once(root, series=("KXBTC15M", "KXHIGHNY"), client=client, clock=lambda: NOW)
            self.assertEqual(first["status"], "observed")
            self.assertEqual(first["requests"], 8)
            self.assertEqual(set(first["blockers"]), {"calibrated_probability_missing", "venue_event_time_missing"})
            second = capture_once(root, series=("KXBTC15M", "KXHIGHNY"), client=client, clock=lambda: NOW + timedelta(seconds=1))
            self.assertEqual(second["status"], "same_slot_replay")
            self.assertEqual(client.requests, 8)
            evidence = next(Path(root, "evidence").glob("*.json.gz"))
            self.assertTrue(evidence.exists())
            self.assertEqual(json.loads(__import__("gzip").open(evidence, "rt").read())["orders_sent"], 0)
            self.assertEqual(first["ledger_mutations"], 0)

    def test_request_ceiling_stops_market_enumeration(self):
        with tempfile.TemporaryDirectory() as root:
            client = FakeClient()
            client.max_requests = 2
            result = capture_once(root, series=("KXBTC15M", "KXHIGHNY"), client=client, clock=lambda: NOW)
            self.assertEqual(result["status"], "failed_closed")
            self.assertLessEqual(client.requests, 2)
            self.assertEqual(result["orders_sent"], 0)


if __name__ == "__main__":
    unittest.main()
