import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import neorome.options_cloud_probe as module
from neorome.options_cloud_probe import collect_once


NOW = datetime(2026, 9, 22, 13, 50, tzinfo=timezone.utc)  # 09:50 New York


def snapshot(received_at=None):
    return {
        "source": "robinhood-trading",
        "received_at": (received_at or NOW).isoformat(),
        "responses": [
            {"tool": "get_option_quotes", "requested_at": NOW.isoformat(), "received_at": (received_at or NOW).isoformat(), "data": {"results": []}, "data_sha256": "abc"}
        ],
        "orders_sent": 0,
    }


def stale_snapshot():
    value = snapshot()
    value["responses"][0]["data"] = {
        "results": [{"quote": {"updated_at": "2026-09-22T13:47:00+00:00"}}]
    }
    return value


class Session:
    def __init__(self, calls=5):
        self.calls = 0
        self.entered = False

    def __enter__(self):
        self.entered = True
        return self

    def __exit__(self, *args):
        return False


class OptionsCloudProbeTests(unittest.TestCase):
    def test_closed_window_does_not_open_cloud_session(self):
        calls = []

        def factory(path):
            calls.append(path)
            raise AssertionError("cloud auth must not run outside window")

        result = collect_once(tempfile.mkdtemp(), clock=lambda: NOW.replace(hour=13, minute=40), session_factory=factory)
        self.assertEqual(result["status"], "closed_window")
        self.assertEqual(calls, [])
        self.assertEqual(result["operational_health"], "healthy")
        self.assertEqual(result["cloud_auth"]["status"], "not_run")

    def test_success_is_fixed_and_once_per_ny_day(self):
        sessions = []
        captures = []

        def factory(path):
            session = Session()
            sessions.append(session)
            return session

        def capture(session, *, clock):
            captures.append(clock)
            session.calls = 5
            return snapshot()

        def normalize(value, *, as_of):
            self.assertEqual(as_of, NOW)
            return [{"symbol": "SPY", "expiry": "2026-10-30", "right": "C", "strike": "765"}]

        with tempfile.TemporaryDirectory() as temporary:
            first = collect_once(temporary, token_path="/private/token", clock=lambda: NOW, session_factory=factory, capture=capture, normalizer=normalize)
            second = collect_once(temporary, token_path="/private/token", clock=lambda: NOW, session_factory=factory, capture=capture, normalizer=normalize)
            self.assertEqual(first["status"], "observed")
            self.assertEqual(first["symbol"], "SPY")
            self.assertEqual(first["expiry"], "2026-10-30")
            self.assertEqual(first["strikes"], ["765", "770"])
            self.assertEqual(first["freshness"]["status"], "fresh")
            self.assertEqual(first["operational_health"], "healthy")
            self.assertEqual(first["data_access"], {"status": "available", "data_calls": 5})
            self.assertEqual(second["status"], "same_day_replay")
            self.assertEqual(len(sessions), 1)
            self.assertEqual(len(captures), 1)
            saved = json.loads((Path(temporary) / "snapshots" / "2026-09-22.json").read_text())
            self.assertFalse(saved["probe_metadata"]["calibration_eligible"])
            self.assertEqual(saved["orders_sent"], 0)

    def test_stale_quote_is_archived_as_blocked_freshness(self):
        def factory(path):
            return Session()

        def normalize(value, *, as_of):
            raise ValueError("Provider quote is stale or in the future")

        with tempfile.TemporaryDirectory() as temporary:
            result = collect_once(temporary, clock=lambda: NOW, session_factory=factory, capture=lambda session, *, clock: stale_snapshot(), normalizer=normalize)
            self.assertEqual(result["status"], "stale_or_unusable")
            self.assertEqual(result["cloud_auth"]["status"], "available")
            self.assertEqual(result["data_access"]["status"], "available")
            self.assertEqual(result["freshness"]["status"], "stale_or_unusable")
            self.assertEqual(result["operational_health"], "healthy")
            self.assertTrue((Path(temporary) / "snapshots" / "2026-09-22.json").exists())

    def test_malformed_normalizer_is_operational_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            result = collect_once(
                temporary,
                clock=lambda: NOW,
                session_factory=lambda _: Session(),
                capture=lambda session, *, clock: snapshot(),
                normalizer=lambda value, *, as_of: (_ for _ in ()).throw(ValueError("Malformed option identity")),
            )
            self.assertEqual(result["status"], "fetch_or_validation_failed")
            self.assertEqual(result["freshness"]["status"], "validation_failed")
            self.assertEqual(result["operational_health"], "blocked")

    def test_normalizer_cannot_rewrite_observed_receipt(self):
        def normalize(value, *, as_of):
            value["received_at"] = "2026-09-23T13:50:00+00:00"
            return []

        with tempfile.TemporaryDirectory() as temporary:
            result = collect_once(
                temporary,
                clock=lambda: NOW,
                session_factory=lambda _: Session(),
                capture=lambda session, *, clock: snapshot(),
                normalizer=normalize,
            )
            self.assertEqual(result["status"], "observed")
            saved = json.loads((Path(temporary) / "snapshots" / "2026-09-22.json").read_text())
            self.assertEqual(saved["received_at"], NOW.isoformat())

    def test_failed_attempt_consumes_day_and_redacts_error(self):
        calls = []

        def factory(path):
            calls.append(path)
            raise RuntimeError("secret cloud token")

        with tempfile.TemporaryDirectory() as temporary:
            first = collect_once(temporary, clock=lambda: NOW, session_factory=factory)
            second = collect_once(temporary, clock=lambda: NOW, session_factory=factory)
            self.assertEqual(first["status"], "fetch_or_validation_failed")
            self.assertEqual(first["detail"], "RuntimeError")
            self.assertEqual(first["cloud_auth"]["status"], "blocked")
            self.assertEqual(first["operational_health"], "blocked")
            self.assertEqual(second["status"], "fetch_or_validation_failed")
            self.assertTrue(second["replay"])
            self.assertEqual(len(calls), 1)
            self.assertNotIn("secret cloud token", (Path(temporary) / "status.json").read_text())

    def test_failed_replay_preserves_operational_failure_and_cli_exit(self):
        def factory(path):
            raise RuntimeError("cloud unavailable")

        with tempfile.TemporaryDirectory() as temporary:
            first = collect_once(temporary, clock=lambda: NOW, session_factory=factory)
            replay = collect_once(temporary, clock=lambda: NOW, session_factory=factory)
            self.assertEqual(first["status"], "fetch_or_validation_failed")
            self.assertEqual(replay["status"], "fetch_or_validation_failed")
            self.assertTrue(replay["replay"])
        with patch("sys.argv", ["options_cloud_probe", "--state", "/tmp/unused"]), patch.object(module, "collect_once", return_value={"status": "fetch_or_validation_failed"}):
            self.assertEqual(module.main(), 1)
        with patch("sys.argv", ["options_cloud_probe", "--state", "/tmp/unused"]), patch.object(module, "collect_once", return_value={"status": "stale_or_unusable"}):
            self.assertEqual(module.main(), 0)

        for status in ("invalid_claim", "clock_rollback", "claim_incomplete_failure", "storage_limit"):
            with patch("sys.argv", ["options_cloud_probe", "--state", "/tmp/unused"]), patch.object(module, "collect_once", return_value={"status": status}):
                self.assertEqual(module.main(), 1)

    def test_invalid_claim_is_blocked_and_receipt_chronology_is_checked(self):
        with tempfile.TemporaryDirectory() as temporary:
            Path(temporary, "claim.json").write_text(json.dumps({"run_date_ny": "2026-09-22", "claimed_at": "bad"}))
            result = collect_once(temporary, clock=lambda: NOW, session_factory=lambda _: Session())
            self.assertEqual(result["status"], "invalid_claim")
            self.assertEqual(result["operational_health"], "blocked")
        with tempfile.TemporaryDirectory() as temporary:
            future_snapshot = snapshot(received_at=NOW.replace(hour=14))
            result = collect_once(
                temporary,
                clock=lambda: NOW,
                session_factory=lambda _: Session(),
                capture=lambda session, *, clock: future_snapshot,
                normalizer=lambda value, *, as_of: [],
            )
            self.assertEqual(result["status"], "fetch_or_validation_failed")
            self.assertEqual(result["freshness"]["status"], "validation_failed")

    def test_replay_requires_current_status_and_snapshot(self):
        with tempfile.TemporaryDirectory() as temporary:
            Path(temporary, "claim.json").write_text(json.dumps({"run_date_ny": "2026-09-22", "claimed_at": NOW.isoformat()}))
            Path(temporary, "status.json").write_text(json.dumps({"run_date_ny": "2026-09-21", "status": "observed"}))
            result = collect_once(temporary, clock=lambda: NOW, session_factory=lambda _: Session())
            self.assertEqual(result["status"], "claim_incomplete_failure")
            self.assertEqual(result["operational_health"], "blocked")
        with tempfile.TemporaryDirectory() as temporary:
            Path(temporary, "claim.json").write_text(json.dumps({"run_date_ny": "2026-09-22", "claimed_at": NOW.isoformat()}))
            Path(temporary, "status.json").write_text(json.dumps({"run_date_ny": "2026-09-22", "status": "observed"}))
            result = collect_once(temporary, clock=lambda: NOW, session_factory=lambda _: Session())
            self.assertEqual(result["status"], "claim_incomplete_failure")

    def test_symlink_and_storage_controls(self):
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary) / "state"
            state.mkdir()
            target = Path(temporary) / "target"
            target.mkdir()
            (state / "claim.json").symlink_to(target / "claim.json")
            with self.assertRaises(module.OptionsCloudProbeError):
                collect_once(state, clock=lambda: NOW, session_factory=lambda _: Session())
        with tempfile.TemporaryDirectory() as temporary:
            Path(temporary, "large.bin").write_bytes(b"x" * 100)
            old = module.MAX_STATE_BYTES
            module.MAX_STATE_BYTES = 100
            try:
                result = collect_once(temporary, clock=lambda: NOW, session_factory=lambda _: Session())
            finally:
                module.MAX_STATE_BYTES = old
            self.assertEqual(result["status"], "storage_limit")


if __name__ == "__main__":
    unittest.main()
