from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest

from neorome.equity_daily import MAX_STATE_BYTES, run_once


UTC = timezone.utc
NOW = datetime(2026, 9, 18, 15, 0, tzinfo=UTC)


def sessions(today):
    rows = []
    day = today - timedelta(days=320)
    while day <= today:
        if day.weekday() < 5:
            opened = datetime.combine(day, datetime.min.time(), UTC).replace(hour=13, minute=30)
            closed = opened + timedelta(hours=6, minutes=30)
            rows.append({"date": day.isoformat(), "open_at": opened.isoformat(), "close_at": closed.isoformat()})
        day += timedelta(days=1)
    return rows


def observed_bundle(now=NOW, *, quote_age=10, is_open=True):
    rows = sessions(now.date())
    bars = []
    for index, row in enumerate(rows[-220:]):
        close = 100 + index
        bars.append({"timestamp": row["close_at"], "o": str(close), "h": str(close + 1),
                     "l": str(close - 1), "c": str(close), "volume": "1000"})
    quote_at = now - timedelta(seconds=quote_age)
    context = {
        "source_kind": "observed", "received_at": now.isoformat(),
        "asset": {"symbol": "SPY", "asset_class": "us_equity", "status": "active",
                  "tradable": True, "fractionable": True, "exchange": "ARCA"},
        "clock": {"timestamp": now.isoformat(), "is_open": is_open},
        "calendar_start": (now.date() - timedelta(days=320)).isoformat(),
        "calendar_end": now.date().isoformat(), "sessions": rows,
        "evidence": [{"response_sha256": "context"}], "requests": 3,
        "order_mutations": 0,
    }
    observation = {
        "source_kind": "observed", "symbol": "SPY", "feed": "iex",
        "observed_at": now.isoformat(), "bars": bars,
        "quote": {"timestamp": quote_at.isoformat(), "bid": "320", "ask": "321",
                   "bid_size": "10", "ask_size": "10"},
        "evidence": [{"response_sha256": "bars"}, {"response_sha256": "quote"}],
        "request_count": 2, "broker_orders": 0, "execution_enabled": False,
    }
    return context, observation


class DailyTests(unittest.TestCase):
    def test_once_per_day_and_failed_capture_consumes_day(self):
        with tempfile.TemporaryDirectory() as directory:
            calls = []

            def collector(clock):
                calls.append("fetch")
                raise RuntimeError("provider body must not persist")

            first = run_once(directory, collector=collector, clock=lambda: NOW)
            second = run_once(directory, collector=collector, clock=lambda: NOW)
            self.assertEqual(first["status"], "fetch_or_validation_failed")
            self.assertEqual(second["status"], "same_day_replay")
            self.assertEqual(calls, ["fetch"])
            saved = json.loads((Path(directory) / "runs" / "2026-09-18.json").read_text())
            self.assertEqual(saved["error"], "RuntimeError")
            self.assertNotIn("provider body", json.dumps(saved))

    def test_success_preserves_bundle_decision_hashes_and_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            result = run_once(directory, collector=lambda clock: observed_bundle(), clock=lambda: NOW)
            self.assertIn(result["status"], {"completed", "abstained"})
            saved = json.loads((Path(directory) / "runs" / "2026-09-18.json").read_text())
            self.assertEqual(saved["source_kind"], "observed")
            self.assertEqual(saved["bundle"]["observation"]["symbol"], "SPY")
            self.assertEqual(set(saved["hashes"]), {"context_sha256", "observation_sha256", "decision_sha256", "bundle_sha256"})
            replay = run_once(directory, collector=lambda clock: self.fail("replay fetched"), clock=lambda: NOW)
            self.assertEqual(replay["status"], "same_day_replay")

    def test_stale_quote_and_closed_market_abstain(self):
        for age, is_open in ((61, True), (10, False)):
            with self.subTest(age=age, is_open=is_open), tempfile.TemporaryDirectory() as directory:
                result = run_once(directory, collector=lambda clock, age=age, is_open=is_open: observed_bundle(quote_age=age, is_open=is_open), clock=lambda: NOW)
                self.assertEqual(result["status"], "abstained")
                self.assertFalse(result["eligible_for_internal_paper"])
                self.assertTrue(result["reasons"])

    def test_nonobserved_or_conflicting_sources_are_rejected_without_retry(self):
        for mutate in (lambda c, o: (dict(c, source_kind="synthetic"), o),
                       lambda c, o: (c, dict(o, source_kind="synthetic")),
                       lambda c, o: (c, dict(o, execution_enabled=True))):
            with self.subTest(mutate=mutate), tempfile.TemporaryDirectory() as directory:
                result = run_once(directory, collector=lambda clock, mutate=mutate: mutate(*observed_bundle()), clock=lambda: NOW)
                self.assertEqual(result["status"], "fetch_or_validation_failed")
                replay = run_once(directory, collector=lambda clock: self.fail("invalid bundle retried"), clock=lambda: NOW)
                self.assertEqual(replay["status"], "same_day_replay")

    def test_clock_rollback_does_not_fetch(self):
        with tempfile.TemporaryDirectory() as directory:
            calls = []
            collector = lambda clock: (calls.append(1), observed_bundle())[1]
            self.assertIn(run_once(directory, collector=collector, clock=lambda: NOW)["status"], {"completed", "abstained"})
            rollback = run_once(directory, collector=lambda clock: self.fail("rollback fetched"),
                                clock=lambda: NOW - timedelta(seconds=1))
            self.assertEqual(rollback["status"], "clock_rollback")
            self.assertEqual(calls, [1])

    def test_storage_cap_preflights_claim_without_fetch(self):
        with tempfile.TemporaryDirectory() as directory:
            filler = Path(directory) / "filler"
            with filler.open("wb") as stream:
                stream.truncate(MAX_STATE_BYTES - 1)
            calls = []
            result = run_once(directory, collector=lambda clock: calls.append(1), clock=lambda: NOW)
            self.assertEqual(result["status"], "storage_limit")
            self.assertEqual(calls, [])
            self.assertFalse((Path(directory) / "claim.json").exists())

    def test_naive_claim_timestamp_is_invalid_without_fetch(self):
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "claim.json").write_text(json.dumps({
                "schema_version": 1,
                "run_date_utc": "2026-09-18",
                "claimed_at": "2026-09-18T15:00:00",
            }))
            result = run_once(directory, collector=lambda clock: self.fail("naive claim fetched"), clock=lambda: NOW)
            self.assertEqual(result["status"], "invalid_claim")


if __name__ == "__main__":
    unittest.main()
