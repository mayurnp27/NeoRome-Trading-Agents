import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from neorome.fomo_wallet_feed import FomoWalletFeed


NOW = datetime(2026, 9, 30, 15, 0, tzinfo=timezone.utc)
WALLET = "A" * 32
MINT = "B" * 32
SECRET = "secret-never-persist-or-print"


class Clock:
    def __init__(self, value=NOW):
        self.value = value

    def __call__(self):
        return self.value


class Response:
    def __init__(self, body):
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self, size=-1):
        return self.body[:size]


class Opener:
    def __init__(self, payload):
        self.payload = payload
        self.requests = []

    def open(self, request, timeout):
        self.requests.append((request, timeout))
        return Response(json.dumps(self.payload).encode())


class FomoWalletFeedTests(unittest.TestCase):
    def test_captured_leaderboard_rest_fixture_schema_is_accepted(self):
        fixture = Path(__file__).resolve().parents[1] / "coordination/verification/fomo-free-20260930/rest-check.json"
        capture = json.loads(fixture.read_text())
        record = next(item for item in capture["requests"] if item["path"].startswith("/v2/leaderboard/7d"))
        candidates, mappings = FomoWalletFeed._parse_leaderboard(record["data"])
        self.assertEqual(set(candidates), {"d6a85eb5-d3fb-5b8a-8445-019af88ba512", "02c943b0-ddbe-5e77-8f68-3710c3ff0a64"})
        self.assertTrue(all(candidate["provider_claimed"] for candidate in candidates.values()))
        self.assertEqual(mappings["d6a85eb5-d3fb-5b8a-8445-019af88ba512"]["wallet"], "AGdtsMmphhymH5wYW3dWKSkAxpLYRrNDCTFAfuq8ii9v")

    def test_positive_sol_leaderboard_candidates_are_mapped_as_claims(self):
        with tempfile.TemporaryDirectory() as directory:
            opener = Opener({"data": [
                {"userId": "u-1", "wallets": {"solana": WALLET}, "pnlUsd": "10", "trades": 4, "handle": "alpha"},
                {"userId": "u-2", "wallets": {"solana": "invalid"}, "pnlUsd": 30, "trades": 2},
                {"userId": "u-3", "wallets": {"solana": "C" * 32}, "pnlUsd": -1, "trades": 2},
                {"userId": "u-4", "wallets": {"solana": "D" * 32}, "pnlUsd": 1, "trades": 0},
            ]})
            feed = FomoWalletFeed(directory, clock=Clock(), opener=opener)
            self.assertEqual(feed.refresh_leaderboard(SECRET)["status"], "ok")
            state = json.loads((Path(directory) / "fomo-state.json").read_text())
            self.assertEqual(state["mappings"]["u-1"]["wallet"], WALLET)
            self.assertEqual(state["mappings"]["u-1"]["rank"], 1)
            self.assertTrue(state["candidates"]["u-1"]["provider_claimed"])
            self.assertEqual(state["reserved_credits"], 250)
            self.assertEqual(state["leaderboard_snapshot"]["day"], "2026-09-30")
            self.assertEqual(state["leaderboard_snapshot"]["frozen_at"], NOW.isoformat().replace("+00:00", "Z"))
            self.assertEqual(state["leaderboard_snapshot"]["entries"][0]["rank"], 1)
            self.assertEqual(opener.requests[0][1], 15)
            self.assertIn("limit=10", opener.requests[0][0].full_url)
            self.assertEqual(feed.refresh_leaderboard(SECRET)["status"], "already_attempted_today")
            self.assertEqual(len(opener.requests), 1)

    def test_legacy_limit8_current_day_upgrades_once_and_reserves_each_request(self):
        class Broken:
            def __init__(self): self.requests = []
            def open(self, request, timeout):
                self.requests.append(request)
                raise RuntimeError("offline")
        with tempfile.TemporaryDirectory() as directory:
            opener = Broken()
            feed = FomoWalletFeed(directory, clock=Clock(), opener=opener)
            old = feed._state()
            old.update({"budget_day": "2026-09-30", "leaderboard_day": "2026-09-30",
                        "leaderboard_limit": 8, "leaderboard_reserved": True,
                        "reserved_credits": 250})
            (Path(directory) / "fomo-state.json").write_text(json.dumps(old))
            self.assertEqual(feed.refresh_leaderboard(SECRET)["status"], "request_failed")
            state = json.loads((Path(directory) / "fomo-state.json").read_text())
            self.assertEqual(state["reserved_credits"], 500)
            self.assertEqual(state["leaderboard_attempt"], {"day": "2026-09-30", "limit": 10, "version": "top10-v1"})
            self.assertEqual(feed.refresh_leaderboard(SECRET)["status"], "already_attempted_today")
            self.assertEqual(len(opener.requests), 1)

    def test_top10_parser_drops_duplicate_malformed_and_nonfinite_claims(self):
        rows = []
        alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
        for index in range(12):
            rows.append({"userId": f"u-{index}", "wallets": {"solana": alphabet[index] * 32},
                         "pnlUsd": 10, "trades": 2})
        rows[1]["wallets"]["solana"] = rows[0]["wallets"]["solana"]
        rows[2]["pnlUsd"] = "NaN"
        candidates, mappings = FomoWalletFeed._parse_leaderboard({"data": rows})
        self.assertEqual(len(candidates), 8)
        self.assertNotIn("u-1", candidates)
        self.assertNotIn("u-2", candidates)
        self.assertEqual(sorted(item["rank"] for item in candidates.values()), [1, 4, 5, 6, 7, 8, 9, 10])

    def test_alert_requires_valid_mapping_sol_chain_and_fresh_timestamp_then_deduplicates(self):
        with tempfile.TemporaryDirectory() as directory:
            feed = FomoWalletFeed(directory, clock=Clock())
            feed._parse_leaderboard  # public parser remains independently testable
            state = feed._state()
            state["mappings"] = {"known": {"wallet": WALLET, "provider_claimed": True}}
            (Path(directory) / "fomo-state.json").write_text(json.dumps(state))
            event = {"type": "alert", "replay": False, "chain": "solana", "eventId": "evt-1", "userId": "known",
                     "alertType": "buy", "tokenAddress": MINT, "ts": int(NOW.timestamp() * 1000), "txHash": "C" * 64}
            record = feed.accept_event(event)
            self.assertEqual(record["event_id"], "evt-1")
            self.assertEqual(record["wallet"], WALLET)
            self.assertEqual(record["provider_at"], NOW.isoformat().replace("+00:00", "Z"))
            self.assertEqual(record["signature"], "C" * 64)
            self.assertIsNone(feed.accept_event(event))
            replay = dict(event, eventId="evt-2")
            self.assertIsNone(feed.accept_event(replay, observed_at=NOW + timedelta(seconds=121)))
            future = dict(event, eventId="evt-3", ts=int((NOW + timedelta(seconds=10)).timestamp() * 1000))
            self.assertIsNone(feed.accept_event(future))
            wrong_chain = dict(event, eventId="evt-4", chain="ethereum")
            self.assertIsNone(feed.accept_event(wrong_chain))
            replay = dict(event, eventId="evt-replayed", replay=True)
            self.assertIsNone(feed.accept_event(replay))
            unknown = dict(event, eventId="evt-5", userId="unknown")
            self.assertIsNone(feed.accept_event(unknown))
            nonalert = dict(event, eventId="evt-6", type="heartbeat")
            self.assertIsNone(feed.accept_event(nonalert))
            queue = json.loads((Path(directory) / "fomo-alerts.json").read_text())["alerts"]
            self.assertEqual([a["event_id"] for a in queue], ["evt-1"])
            diagnostic = feed._state()["alert_diagnostics"]
            self.assertEqual(diagnostic["counts"], {"accepted": 1, "duplicate_or_invalid_event_cache": 1,
                "invalid_or_stale_timestamp": 2, "non_solana": 1, "replay": 1, "unmapped_user": 1, "non_alert": 1})
            self.assertEqual(diagnostic["last_accepted_provider_at"], record["provider_at"])
            self.assertNotIn("known", json.dumps(diagnostic))
            self.assertNotIn(MINT, json.dumps(diagnostic))
            feed.clock.value = NOW + timedelta(days=1)
            self.assertIsNone(feed.accept_event(nonalert))
            self.assertEqual(feed._state()["alert_diagnostics"]["counts"], {"non_alert": 1})


    def test_key_is_not_written_to_state_or_exposed_by_failed_http_request(self):
        class Broken:
            def open(self, request, timeout):
                raise RuntimeError(f"failed request {SECRET}")

        with tempfile.TemporaryDirectory() as directory:
            feed = FomoWalletFeed(directory, clock=Clock(), opener=Broken())
            result = feed.refresh_leaderboard(SECRET)
            self.assertEqual(result, {"status": "request_failed", "requests": 1})
            persisted = (Path(directory) / "fomo-state.json").read_text()
            self.assertNotIn(SECRET, persisted)
            self.assertNotIn(SECRET, json.dumps(result))
            self.assertEqual(feed.refresh_leaderboard(SECRET)["status"], "already_attempted_today")

    def test_fake_websocket_welcome_replay_and_fresh_alert_are_bounded_and_persisted_safely(self):
        class FeedSocket:
            def __init__(self):
                self.frames = [json.dumps({"type": "welcome", "stream": "alerts", "filter": "solana", "buffered": 4,
                                           "note": SECRET}),
                               json.dumps({"type": "alert", "replay": True, "eventId": "old", "alertType": "buy",
                                           "chain": "solana", "userId": "known", "tokenAddress": MINT,
                                           "ts": int(NOW.timestamp() * 1000)}),
                               json.dumps({"type": "alert", "eventId": "fresh", "alertType": "sell",
                                           "chain": "solana", "userId": "known", "tokenAddress": MINT,
                                           "ts": int(NOW.timestamp() * 1000)}), ""]

            def recv(self):
                return self.frames.pop(0)

            def close(self):
                pass

        class WSModule:
            def create_connection(self, url, timeout):
                self.url = url
                self.timeout = timeout
                return FeedSocket()

        with tempfile.TemporaryDirectory() as directory:
            feed = FomoWalletFeed(directory, clock=Clock(), opener=Opener({"data": {"traders": [
                {"userId": "known", "wallets": {"solana": WALLET}, "pnlUsd": 1, "trades": 1}
            ]}}), websocket_module=WSModule())
            state = feed._state()
            state["mappings"] = {"known": {"wallet": WALLET, "provider_claimed": True}}
            (Path(directory) / "fomo-state.json").write_text(json.dumps(state))
            previous = os.environ.get("FOMO_API_KEY")
            os.environ["FOMO_API_KEY"] = SECRET
            try:
                feed.run(max_connections=1, sleep=lambda _: None)
            finally:
                if previous is None:
                    os.environ.pop("FOMO_API_KEY", None)
                else:
                    os.environ["FOMO_API_KEY"] = previous
            persisted = "".join(p.read_text() for p in Path(directory).glob("*.json"))
            self.assertNotIn(SECRET, persisted)
            self.assertEqual(json.loads((Path(directory) / "fomo-alerts.json").read_text())["alerts"][0]["event_id"], "fresh")
            final_state = json.loads((Path(directory) / "fomo-state.json").read_text())
            self.assertEqual(final_state["connection"]["welcome"]["stream"], "alerts")
            self.assertEqual(final_state["connection"]["message_count"], 3)


if __name__ == "__main__":
    unittest.main()
