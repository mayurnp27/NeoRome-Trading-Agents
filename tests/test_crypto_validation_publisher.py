from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest

from neorome.crypto_validation_publisher import CryptoValidationPublisher


UTC = timezone.utc
NOW = datetime(2026, 9, 23, 21, 0, tzinfo=UTC)


def write_json(path: Path, value: object) -> bytes:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode() + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return raw


def bars(count: int = 107) -> list[dict[str, object]]:
    first = NOW.date() - timedelta(days=count)
    return [
        {
            "timestamp": datetime.combine(first + timedelta(days=index), datetime.min.time(), UTC).isoformat().replace("+00:00", "Z"),
            "complete": True,
            "open": "100",
            "high": "101",
            "low": "99",
            "close": "100",
        }
        for index in range(count)
    ]


class CryptoValidationPublisherTests(unittest.TestCase):
    def test_missing_sources_is_clean_data_not_ready_and_does_not_publish_pointer(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = CryptoValidationPublisher(root / "inbox", daily_state=root / "daily", token_state=root / "tokens").publish(now=NOW)
            self.assertEqual(result["status"], "data_not_ready")
            self.assertFalse((root / "inbox" / "input.json").exists())
            self.assertFalse((root / "inbox" / "current.json").exists())

    def test_retains_btc_bars_and_hash_binds_current_pointer(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            decision = {
                "source_kind": "observed",
                "execution_enabled": False,
                "completed_at": "2026-09-23T20:00:00Z",
                "bars_source": {"source_kind": "observed", "provider": "alpaca_crypto_bars"},
                "bars": bars(),
            }
            source = write_json(root / "daily" / "decision-2026-09-23.json", decision)
            result = CryptoValidationPublisher(root / "inbox", daily_state=root / "daily", token_state=root / "tokens").publish(now=NOW)
            self.assertEqual(result["status"], "published")
            self.assertEqual(result["bars"], 107)
            self.assertEqual(result["token_candidates"], 0)
            input_raw = (root / "inbox" / "input.json").read_bytes()
            pointer = json.loads((root / "inbox" / "current.json").read_text())
            self.assertEqual(pointer["input"], "input.json")
            self.assertEqual(pointer["input_sha256"], hashlib.sha256(input_raw).hexdigest())
            self.assertEqual(pointer["boundary_at"], "2026-09-23T20:00:00Z")
            payload = json.loads(input_raw)
            self.assertEqual(len(payload["bars"]), 107)
            self.assertEqual(payload["source_provenance"]["btc_decisions"][0]["sha256"], hashlib.sha256(source).hexdigest())
            self.assertFalse(payload["execution_enabled"])

    def test_invalid_or_stale_btc_source_is_not_published(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stale = bars()
            stale[-1]["timestamp"] = "2026-09-21T00:00:00Z"
            write_json(root / "daily" / "decision-2026-09-22.json", {
                "source_kind": "observed", "execution_enabled": False,
                "completed_at": "2026-09-23T20:00:00Z", "bars": stale,
            })
            result = CryptoValidationPublisher(root / "inbox", daily_state=root / "daily", token_state=root / "tokens").publish(now=NOW)
            self.assertEqual(result["status"], "data_not_ready")
            self.assertIn("btc_bars_not_through_previous_utc_day", result["blockers"])

    def test_embargo_requires_106_bars_before_publication(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_json(root / "daily" / "decision-2026-09-23.json", {
                "source_kind": "observed", "execution_enabled": False,
                "completed_at": "2026-09-23T20:00:00Z", "bars": bars(105),
            })
            result = CryptoValidationPublisher(
                root / "inbox", daily_state=root / "daily", token_state=root / "tokens"
            ).publish(now=NOW)
            # The retained history is valid evidence even though the native
            # validator still needs 106 rows for its three-way split.
            self.assertEqual(result["status"], "published")
            self.assertEqual(result["bars"], 105)
            self.assertIn("btc_bars_below_walkforward_minimum", result["blockers"])

    def test_short_rolling_snapshot_is_retained_as_observed_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_json(root / "daily" / "rolling-2026-09-23.json", {
                "source_kind": "observed", "execution_enabled": False,
                "completed_at": "2026-09-23T20:00:00Z", "bars": bars(102),
            })
            result = CryptoValidationPublisher(
                root / "inbox", daily_state=root / "daily", token_state=root / "tokens"
            ).publish(now=NOW)
            self.assertEqual(result["status"], "published")
            self.assertEqual(result["bars"], 102)
            self.assertIn("btc_bars_below_walkforward_minimum", result["blockers"])
            payload = json.loads((root / "inbox" / "input.json").read_text())
            self.assertEqual(len(payload["bars"]), 102)
            self.assertIn("btc_bars_below_walkforward_minimum", payload["publisher_blockers"])

    def test_explicit_token_cohort_is_retained_without_creating_sell_routes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = {
                "source_kind": "observed", "execution_enabled": False,
                "known_at": "2026-09-23T18:00:00Z", "received_at": "2026-09-23T18:00:01Z",
                "chain": "solana", "mint": "So11111111111111111111111111111111111111112",
                "screen_rejections": ["route_history_missing"],
                "entry_quote": {"requested_at": "2026-09-23T18:00:02Z"},
            }
            write_json(root / "tokens" / "candidates" / "candidate.json", candidate)
            result = CryptoValidationPublisher(root / "inbox", daily_state=root / "daily", token_state=root / "tokens").publish(now=NOW)
            self.assertEqual(result["status"], "published")
            payload = json.loads((root / "inbox" / "input.json").read_text())
            self.assertEqual(payload["discovery"]["candidates"][0]["mint"], candidate["mint"])
            self.assertEqual(payload["sell_observations"], [])
            self.assertFalse(payload["execution_enabled"])

    def test_token_recorded_at_alone_is_not_promoted_to_discovery_time(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_json(root / "tokens" / "candidates" / "candidate.json", {
                "source_kind": "observed", "execution_enabled": False,
                "recorded_at": "2026-09-23T18:00:00Z", "chain": "solana",
                "mint": "So11111111111111111111111111111111111111112",
            })
            result = CryptoValidationPublisher(root / "inbox", daily_state=root / "daily", token_state=root / "tokens").publish(now=NOW)
            self.assertEqual(result["status"], "data_not_ready")
            self.assertIn("token_discovery_timestamp_missing", result["blockers"])

    def test_explicit_token_vintages_survive_incomplete_cohort(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = {
                "source_kind": "observed", "execution_enabled": False,
                "known_at": "2026-09-23T18:00:00Z", "received_at": "2026-09-23T18:00:01Z",
                "chain": "solana", "mint": "So11111111111111111111111111111111111111112",
            }
            write_json(root / "tokens" / "candidates" / "valid.json", candidate)
            write_json(root / "tokens" / "candidates" / "missing.json", {
                "source_kind": "observed", "execution_enabled": False,
                "recorded_at": "2026-09-23T18:00:00Z", "chain": "solana",
                "mint": "So11111111111111111111111111111111111111112",
            })
            result = CryptoValidationPublisher(
                root / "inbox", daily_state=root / "daily", token_state=root / "tokens"
            ).publish(now=NOW)
            self.assertEqual(result["status"], "published")
            self.assertEqual(result["token_candidates"], 1)
            self.assertEqual(result["sell_observations"], 0)
            self.assertIn("token_discovery_timestamp_missing", result["blockers"])
            self.assertIn("token_sell_outcomes_missing", result["blockers"])
            payload = json.loads((root / "inbox" / "input.json").read_text())
            self.assertIn("token_sell_outcomes_missing", payload["publisher_blockers"])

    def test_service_is_local_read_only_and_runs_before_manifest_bridge(self):
        unit = Path("deploy/recorder/neorome-crypto-validation-publisher.service").read_text()
        timer = Path("deploy/recorder/neorome-crypto-validation-publisher.timer").read_text()
        self.assertIn("RestrictAddressFamilies=AF_UNIX", unit)
        self.assertIn("ReadOnlyPaths=/opt/neorome /var/lib/neorome-crypto-daily /var/lib/neorome-token-paper", unit)
        self.assertIn("Before=neorome-validation-manifest.service", unit)
        self.assertIn("OnCalendar=Mon..Fri *-*-* 21:00:00 UTC", timer)


if __name__ == "__main__":
    unittest.main()
