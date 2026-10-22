import hashlib
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from neorome.crypto_daily import run_once


UTC = timezone.utc
NOW = datetime(2026, 9, 21, 14, 0, tzinfo=UTC)


def bars_payload(now=NOW, count=102):
    first = now.date() - timedelta(days=count)
    bars = []
    for index in range(count):
        day = first + timedelta(days=index)
        close = 100 + index
        bars.append(
            {
                "t": f"{day.isoformat()}T00:00:00Z",
                "o": close,
                "h": close,
                "l": close - 1,
                "c": close,
            }
        )
    return {"bars": {"BTC/USD": bars}, "next_page_token": None}


def quote_observation(timestamp=NOW - timedelta(seconds=5)):
    return {
        "raw_response": {
            "quotes": {
                "BTC/USD": {
                    "t": timestamp.isoformat().replace("+00:00", "Z"),
                    "bp": "120",
                    "ap": "121",
                    "bs": "2",
                    "as": "2",
                }
            }
        },
        "evidence": {"feed": "crypto"},
    }


class FakeResponse:
    status = 200

    def __init__(self, payload):
        self.body = json.dumps(payload).encode()
        self.closed = False

    def read(self, limit):
        return self.body

    def close(self):
        self.closed = True


class FakeOpener:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def open(self, request, timeout):
        self.calls.append((request, timeout))
        return FakeResponse(self.payload)


def write_observation(state, observation, *, status="fresh"):
    root = Path(state)
    root.mkdir(parents=True)
    encoded = json.dumps(observation, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(encoded.encode()).hexdigest()
    with sqlite3.connect(root / "recorder.sqlite3") as db:
        db.execute(
            "CREATE TABLE attempts (id INTEGER PRIMARY KEY, started_at TEXT, status TEXT, observation TEXT, sha256 TEXT)"
        )
        db.execute(
            "INSERT INTO attempts(started_at,status,observation,sha256) VALUES (?,?,?,?)",
            (NOW.isoformat(), status, encoded, digest),
        )
        db.commit()


def credentials(_service):
    return "test-credential"


class CryptoDailyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.state = Path(self.temp.name) / "daily"
        self.observations = Path(self.temp.name) / "crypto-recorder"

    def tearDown(self):
        self.temp.cleanup()

    def run_daily(self, payload=None, *, observation=None, now=NOW):
        if observation is not None:
            write_observation(self.observations, observation)
        opener = FakeOpener(payload or bars_payload())
        result = run_once(
            str(self.state),
            str(self.observations),
            now=now,
            opener=opener,
            credential_loader=credentials,
        )
        return result, opener

    def test_flat_research_decision_is_atomic_and_replay_does_not_fetch(self):
        result, opener = self.run_daily(observation=quote_observation())
        self.assertEqual(result["status"], "entry_candidate")
        self.assertEqual(result["decision"]["execution_enabled"], False)
        self.assertEqual(result["decision"]["valuation"]["all_in_cost_cents"] <= 500, True)
        self.assertEqual(len(opener.calls), 1)
        evidence = json.loads((self.state / result["decision_path"]).read_text())
        self.assertEqual(evidence["assumed_position_units"], "0")
        self.assertEqual(evidence["ledger_mutations"], 0)
        self.assertEqual(evidence["source_kind"], "observed")

        replay, _ = self.run_daily()
        self.assertEqual(replay["status"], "already_attempted")
        self.assertEqual(len(opener.calls), 1)

    def test_missing_and_stale_quotes_abstain_without_quote_get(self):
        missing, opener = self.run_daily()
        self.assertEqual(missing["status"], "abstain")
        self.assertEqual(missing["decision"]["reason"], "missing_quote")
        self.assertEqual(len(opener.calls), 1)

        self.temp.cleanup()
        self.temp = tempfile.TemporaryDirectory()
        self.state = Path(self.temp.name) / "daily"
        self.observations = Path(self.temp.name) / "crypto-recorder"
        stale, opener = self.run_daily(observation=quote_observation(NOW - timedelta(seconds=61)))
        self.assertEqual(stale["status"], "abstain")
        self.assertEqual(stale["decision"]["reason"], "missing_quote")
        self.assertEqual(len(opener.calls), 1)

    def test_unfinished_attempt_blocks_network_replay(self):
        self.state.mkdir(parents=True)
        (self.state / "attempt.json").write_text(
            json.dumps({"date": NOW.date().isoformat(), "status": "incomplete"})
        )
        result, opener = self.run_daily(observation=quote_observation())
        self.assertEqual(result["status"], "incomplete_replay")
        self.assertEqual(opener.calls, [])

    def test_pagination_and_malformed_history_fail_closed(self):
        paged = bars_payload()
        paged["next_page_token"] = "must-not-follow"
        result, opener = self.run_daily(paged)
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["error_type"], "PaginationRejected")
        self.assertEqual(len(opener.calls), 1)

        self.temp.cleanup()
        self.temp = tempfile.TemporaryDirectory()
        self.state = Path(self.temp.name) / "daily"
        self.observations = Path(self.temp.name) / "crypto-recorder"
        malformed = bars_payload()
        del malformed["bars"]["BTC/USD"][3]["h"]
        result, _ = self.run_daily(malformed)
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["error_type"], "DailyRecorderError")

    def test_future_or_gapped_history_fails_policy_validation(self):
        payload = bars_payload()
        payload["bars"]["BTC/USD"][50]["t"] = "2026-08-01T00:00:00Z"
        result, _ = self.run_daily(payload)
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["error_type"], "TrendPolicyError")

    def test_state_cap_blocks_before_request(self):
        self.state.mkdir(parents=True)
        with (self.state / "bounded.bin").open("wb") as handle:
            handle.seek(256 * 1024 * 1024 - 1)
            handle.write(b"x")
        result, opener = self.run_daily(observation=quote_observation())
        self.assertEqual(result["status"], "storage_limit")
        self.assertEqual(opener.calls, [])


if __name__ == "__main__":
    unittest.main()
