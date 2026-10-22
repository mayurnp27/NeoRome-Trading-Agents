"""Public API tests for the deployed crypto validation publisher revision."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from neorome.crypto_validation_publisher import CryptoValidationPublisher


UTC = timezone.utc
NOW = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def decision(*, now: datetime = NOW, count: int = 106, end_days_ago: int = 1,
             complete: bool = True, source_kind: str = "observed") -> dict:
    end = now.date() - timedelta(days=end_days_ago)
    first = end - timedelta(days=count - 1)
    bars = []
    for index in range(count):
        day = first + timedelta(days=index)
        bars.append({
            "timestamp": datetime.combine(day, datetime.min.time(), UTC).isoformat().replace("+00:00", "Z"),
            "complete": complete,
            "open": "100.00",
            "high": "102.00",
            "low": "99.00",
            "close": "101.00",
        })
    boundary = datetime.combine(end, datetime.max.time(), UTC).isoformat().replace("+00:00", "Z")
    return {
        "source_kind": source_kind,
        "execution_enabled": False,
        "completed_at": boundary,
        "bars": bars,
    }


class CryptoPublisherDeployedRevisionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.daily = self.root / "daily"
        self.tokens = self.root / "tokens"
        self.inbox = self.root / "inbox"
        (self.tokens / "candidates").mkdir(parents=True)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def publisher(self) -> CryptoValidationPublisher:
        return CryptoValidationPublisher(self.inbox, daily_state=self.daily, token_state=self.tokens)

    def write_decision(self, value: dict, name: str = "decision-sample.json") -> None:
        write_json(self.daily / name, value)

    def test_publishes_minimum_complete_observed_btc_history_and_is_idempotent(self) -> None:
        self.write_decision(decision())

        first = self.publisher().publish(now=NOW)
        pointer_first = (self.inbox / "current.json").read_bytes()
        payload_first = (self.inbox / "input.json").read_bytes()
        second = self.publisher().publish(now=NOW)

        self.assertEqual(first["status"], "published")
        self.assertEqual(first["bars"], 106)
        self.assertEqual(first["execution_enabled"], False)
        self.assertEqual(first["orders_sent"], 0)
        self.assertEqual(first["input_sha256"], hashlib.sha256(payload_first).hexdigest())
        self.assertEqual(second["input_sha256"], first["input_sha256"])
        self.assertEqual((self.inbox / "current.json").read_bytes(), pointer_first)
        self.assertEqual((self.inbox / "input.json").read_bytes(), payload_first)

    def test_missing_btc_source_reports_not_ready_without_publishing_pointer(self) -> None:
        result = self.publisher().publish(now=NOW)

        self.assertEqual(result["status"], "data_not_ready")
        self.assertIn("btc_state_missing", result["blockers"])
        self.assertFalse((self.inbox / "current.json").exists())
        self.assertFalse((self.inbox / "input.json").exists())

    def test_stale_and_below_minimum_btc_history_fail_closed(self) -> None:
        self.write_decision(decision(end_days_ago=2))
        stale = self.publisher().publish(now=NOW)
        self.assertEqual(stale["status"], "data_not_ready")
        self.assertIn("btc_bars_not_through_previous_utc_day", stale["blockers"])
        self.assertFalse((self.inbox / "current.json").exists())

        for path in self.daily.glob("decision-*.json"):
            path.unlink()
        self.write_decision(decision(count=105))
        short = self.publisher().publish(now=NOW)
        self.assertEqual(short["status"], "data_not_ready")
        self.assertIn("btc_bars_below_walkforward_minimum", short["blockers"])
        self.assertFalse((self.inbox / "current.json").exists())

    def test_synthetic_or_incomplete_btc_bars_are_not_admitted(self) -> None:
        self.write_decision(decision(source_kind="synthetic"))
        synthetic = self.publisher().publish(now=NOW)
        self.assertEqual(synthetic["status"], "data_not_ready")
        self.assertIn("btc_bars_missing", synthetic["blockers"])
        self.assertFalse((self.inbox / "current.json").exists())

        for path in self.daily.glob("decision-*.json"):
            path.unlink()
        self.write_decision(decision(complete=False))
        incomplete = self.publisher().publish(now=NOW)
        self.assertEqual(incomplete["status"], "data_not_ready")
        self.assertIn("btc_bar_incomplete", incomplete["blockers"])
        self.assertFalse((self.inbox / "current.json").exists())

    def test_token_recorded_at_without_discovery_timestamps_is_rejected(self) -> None:
        self.write_decision(decision())
        write_json(self.tokens / "candidates" / "candidate.json", {
            "source_kind": "observed",
            "execution_enabled": False,
            "recorded_at": "2026-10-19T20:00:00Z",
            "symbol": "TEST",
        })

        result = self.publisher().publish(now=NOW)
        payload = json.loads((self.inbox / "input.json").read_text(encoding="utf-8"))

        self.assertEqual(result["status"], "published")
        self.assertEqual(result["token_candidates"], 0)
        self.assertIn("token_discovery_timestamp_missing", result["blockers"])
        self.assertNotIn("discovery", payload)
        self.assertFalse(payload["execution_enabled"])
        self.assertEqual(payload["orders_sent"], 0)


if __name__ == "__main__":
    unittest.main()
