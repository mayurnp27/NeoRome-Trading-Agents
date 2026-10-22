import base64
import hashlib
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from unittest.mock import patch

import neorome.prediction_evidence as module
from neorome.prediction_evidence import (
    KalshiPublicClient,
    PredictionEvidenceError,
    collect_once,
    normalize_evidence,
)


NOW = datetime(2026, 9, 22, 15, 34, 12, tzinfo=timezone.utc)


def market(status="open", event_ticker="KXHIGHNY-26SEP22"):
    return {
        "ticker": event_ticker + "-T70",
        "event_ticker": event_ticker,
        "status": "active" if status == "open" else "finalized",
        "result": "yes" if status == "settled" else "",
        "settlement_value_dollars": "1.0000" if status == "settled" else "",
        "settlement_ts": "2026-09-23T19:00:00Z" if status == "settled" else "",
        "rules_primary": "venue supplied rule",
        "yes_bid_dollars": "0.20",
        "yes_ask_dollars": "0.30",
    }


def response(status="open", cursor=""):
    return {"markets": [market(status)], "cursor": cursor}


class Response:
    status = 200

    def __init__(self, raw):
        self.raw = raw

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self, limit):
        return self.raw


class Opener:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.requests = []

    def open(self, request, timeout):
        self.requests.append((request, timeout))
        return Response(self.payloads.pop(0))


def client_for(open_payload=response("open"), settled_payload=response("settled")):
    open_raw = json.dumps(open_payload, separators=(",", ":")).encode()
    settled_raw = json.dumps(settled_payload, separators=(",", ":")).encode()
    opener = Opener([open_raw, settled_raw])
    client = KalshiPublicClient(opener=opener, clock=lambda: NOW)
    return client, opener, [open_raw, settled_raw]


class PredictionEvidenceTests(unittest.TestCase):
    def test_two_public_gets_and_fixed_queries_and_hashes(self):
        client, opener, raws = client_for()
        results = client.fetch_hour(now=NOW)
        self.assertEqual(client.requests, 2)
        self.assertEqual(len(opener.requests), 2)
        open_query = parse_qs(urlparse(opener.requests[0][0].full_url).query)
        settled_query = parse_qs(urlparse(opener.requests[1][0].full_url).query)
        self.assertEqual(open_query, {"series_ticker": ["KXHIGHNY"], "status": ["open"], "limit": ["100"]})
        self.assertEqual(settled_query["series_ticker"], ["KXHIGHNY"])
        self.assertEqual(settled_query["status"], ["settled"])
        self.assertEqual(settled_query["limit"], ["100"])
        self.assertEqual(int(settled_query["min_settled_ts"][0]), int(NOW.timestamp()) - 14 * 24 * 60 * 60)
        self.assertEqual(results[0]["source_hash"], hashlib.sha256(raws[0]).hexdigest())
        self.assertEqual(base64.b64decode(results[1]["raw_response_b64"]), raws[1])
        evidence = normalize_evidence(results)
        self.assertFalse(evidence["calibration_eligible"])
        self.assertIsNone(evidence["probabilities"])
        self.assertEqual(evidence["responses"][1]["markets"][0]["result"], "yes")

    def test_cursor_is_retained_and_marks_incomplete_without_pagination(self):
        client, opener, _ = client_for(response("open", cursor="next"), response("settled"))
        results = client.fetch_hour(now=NOW)
        self.assertEqual(client.requests, 2)
        self.assertEqual(len(opener.requests), 2)
        self.assertFalse(results[0]["complete_response"])
        evidence = normalize_evidence(results)
        self.assertFalse(evidence["complete_response"])

    def test_request_budget_never_exceeds_two(self):
        client, _, _ = client_for()
        client.fetch_hour(now=NOW)
        with self.assertRaises(PredictionEvidenceError):
            client.fetch_status("open", now=NOW)

    def test_schema_wrong_series_status_and_malformed_response_fail(self):
        for bad_open in (
            {"markets": [market(event_ticker="OTHER-26SEP22")], "cursor": ""},
            {"markets": [market(status="settled")], "cursor": ""},
            {"markets": [], "cursor": 1},
        ):
            raw = json.dumps(bad_open).encode()
            bad_client = KalshiPublicClient(opener=Opener([raw]), clock=lambda: NOW)
            with self.assertRaises(PredictionEvidenceError):
                bad_client.fetch_status("open", now=NOW)
        bad_client = KalshiPublicClient(opener=Opener([b"not-json"]), clock=lambda: NOW)
        with self.assertRaises(PredictionEvidenceError):
            bad_client.fetch_status("open", now=NOW)

    def test_hour_claim_failure_restart_and_clock_rollback(self):
        class FailingClient:
            def fetch_hour(self, **kwargs):
                raise RuntimeError("network detail")

        with tempfile.TemporaryDirectory() as temporary:
            first = collect_once(temporary, client=FailingClient(), clock=lambda: NOW)
            self.assertEqual(first["status"], "fetch_or_validation_failed")
            replay = collect_once(temporary, client=FailingClient(), clock=lambda: NOW)
            self.assertEqual(replay["status"], "same_hour_replay")
            rollback = collect_once(temporary, client=FailingClient(), clock=lambda: NOW.replace(hour=14))
            self.assertEqual(rollback["status"], "clock_rollback")

    def test_success_is_atomic_and_hour_idempotent(self):
        client, _, _ = client_for()
        with tempfile.TemporaryDirectory() as temporary:
            first = collect_once(temporary, client=client, clock=lambda: NOW)
            second = collect_once(temporary, client=client, clock=lambda: NOW)
            self.assertEqual(first["status"], "observed")
            self.assertEqual(second["status"], "same_hour_replay")
            evidence_path = Path(temporary) / "evidence" / "20260922T15.json.gz"
            self.assertTrue(evidence_path.exists())
            from neorome.prediction_evidence import read_evidence
            evidence = read_evidence(evidence_path)
            self.assertEqual(evidence_path.suffix, ".gz")
            self.assertEqual(evidence["source_kind"], "observed")
            self.assertEqual(evidence["series_ticker"], "KXHIGHNY")
            self.assertEqual(evidence["orders_sent"], 0)

    def test_symlink_and_storage_controls(self):
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary) / "state"
            state.mkdir()
            target = Path(temporary) / "target"
            target.mkdir()
            (state / "claim.json").symlink_to(target / "claim.json")
            with self.assertRaises(PredictionEvidenceError):
                collect_once(state, client=object(), clock=lambda: NOW)
        with tempfile.TemporaryDirectory() as temporary:
            Path(temporary, "large.bin").write_bytes(b"x" * 100)
            with patch.object(module, "MAX_STATE_BYTES", 100):
                result = collect_once(temporary, client=object(), clock=lambda: NOW)
            self.assertEqual(result["status"], "storage_limit")


if __name__ == "__main__":
    unittest.main()
