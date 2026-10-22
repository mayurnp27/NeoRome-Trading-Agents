import hashlib
import json
import tempfile
import unittest
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from neorome.equity_actions import (
    CorporateActionError,
    CorporateActionsClient,
    action_clearance,
    collect_once,
    normalize_evidence,
    validate_evidence,
)


NOW = datetime(2026, 9, 21, 15, 0, tzinfo=timezone.utc)


def payload(*rows, next_page_token=None, group="cash_dividends"):
    body = {"corporate_actions": {group: list(rows)}}
    if next_page_token is not None:
        body["next_page_token"] = next_page_token
    return body


def row(**overrides):
    value = {
        "id": "action-1",
        "corporate_action_type": "cash_dividend",
        "symbol": "SPY",
        "status": "executed",
        "ex_date": "2026-09-18",
        "process_date": "2026-10-30",
        "rate": "1.888834",
    }
    value.update(overrides)
    return value


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
    def __init__(self, value):
        self.value = value
        self.requests = []

    def open(self, request, timeout):
        self.requests.append((request, timeout))
        return Response(self.value)


def client_for(value):
    raw = json.dumps(value, separators=(",", ":")).encode()
    opener = Opener(raw)
    client = CorporateActionsClient(
        opener=opener,
        credentials=lambda service: "secret-value",
        clock=lambda: NOW,
    )
    return client, opener, raw


class CorporateActionsTests(unittest.TestCase):
    def test_single_get_uses_process_date_filter_and_raw_hash(self):
        client, opener, raw = client_for(payload(row()))
        result = client.fetch(now=NOW)
        query = parse_qs(urlparse(opener.requests[0][0].full_url).query)
        self.assertEqual(client.requests, 1)
        self.assertEqual(query["symbols"], ["SPY"])
        self.assertNotIn("date_type", query)
        self.assertEqual(query["data_quality"], ["all"])
        self.assertEqual(query["limit"], ["1000"])
        self.assertEqual(query["start"], ["2025-09-21"])
        self.assertEqual(query["end"], ["2027-09-21"])
        self.assertEqual(result["source_hash"], hashlib.sha256(raw).hexdigest())
        self.assertEqual(result["actions"][0]["process_date"], "2026-10-30")
        self.assertEqual(result["actions"][0]["action_type"], "cash_dividends")

    def test_pending_pagination_is_incomplete_without_second_request(self):
        client, opener, _ = client_for(payload(row(), next_page_token="opaque"))
        evidence = normalize_evidence(client.fetch(now=NOW), collected_at=NOW)
        self.assertEqual(client.requests, 1)
        self.assertEqual(len(opener.requests), 1)
        self.assertFalse(evidence["complete_response"])
        self.assertEqual(validate_evidence(evidence, now=NOW)["status"], "blocked")

    def test_malformed_response_and_oversized_response_fail(self):
        opener = Opener(b"[]")
        client = CorporateActionsClient(opener=opener, credentials=lambda _: "x", clock=lambda: NOW)
        with self.assertRaises(CorporateActionError):
            client.fetch(now=NOW)
        opener = Opener(b"x" * 2_000_001)
        client = CorporateActionsClient(opener=opener, credentials=lambda _: "x", clock=lambda: NOW)
        with self.assertRaises(CorporateActionError):
            client.fetch(now=NOW)

    def test_fresh_evidence_and_concrete_date_intersection(self):
        client, _, _ = client_for(payload(row(ex_date="2026-09-22")))
        evidence = normalize_evidence(client.fetch(now=NOW), collected_at=NOW)
        self.assertEqual(validate_evidence(evidence, now=NOW)["status"], "clear")
        blocked = action_clearance(evidence, open_date="2026-09-21", now=NOW)
        self.assertEqual(blocked["status"], "blocked")
        self.assertEqual(blocked["intersections"][0]["action_type"], "cash_dividends")
        clear_client, _, _ = client_for(payload(row(ex_date="2026-01-01")))
        clear_evidence = normalize_evidence(clear_client.fetch(now=NOW), collected_at=NOW)
        self.assertEqual(action_clearance(clear_evidence, open_date="2026-09-21", now=NOW)["status"], "clear")

    def test_unknown_event_dates_block_even_with_empty_response(self):
        client, _, _ = client_for(payload(row(ex_date=None, process_date=None)))
        evidence = normalize_evidence(client.fetch(now=NOW), collected_at=NOW)
        result = action_clearance(evidence, open_date=date(2026, 9, 21), now=NOW)
        self.assertEqual(result["status"], "blocked")
        self.assertIn("unknown type or dates", " ".join(result["reasons"]))
        empty_client, _, _ = client_for(payload())
        empty = normalize_evidence(empty_client.fetch(now=NOW), collected_at=NOW)
        self.assertEqual(empty["known_empty_only"], True)
        self.assertEqual(action_clearance(empty, open_date="2026-09-21", now=NOW)["status"], "clear")

    def test_stale_evidence_blocks_and_daily_claim_is_idempotent(self):
        client, _, _ = client_for(payload(row()))
        with tempfile.TemporaryDirectory() as temporary:
            calls = []

            class CountingClient:
                def fetch(self, **kwargs):
                    calls.append(1)
                    return client.fetch(**kwargs)

            first = collect_once(temporary, client=CountingClient(), clock=lambda: NOW)
            second = collect_once(temporary, client=CountingClient(), clock=lambda: NOW)
            self.assertEqual(first["status"], "observed")
            self.assertEqual(second["status"], "same_day_replay")
            self.assertEqual(len(calls), 1)
            evidence = json.loads((Path(temporary) / "evidence" / "2026-09-21.json").read_text())
            stale = dict(evidence, collected_at=(NOW - timedelta(days=1)).isoformat())
            self.assertEqual(validate_evidence(stale, now=NOW)["status"], "blocked")

    def test_live_group_shape_and_normalized_tampering_are_blocked(self):
        client, _, _ = client_for(payload(row()))
        evidence = normalize_evidence(client.fetch(now=NOW), collected_at=NOW)
        self.assertEqual(validate_evidence(evidence, now=NOW)["status"], "clear")
        tampered = dict(evidence)
        tampered["actions"] = [dict(evidence["actions"][0], ex_date="2026-09-22")]
        result = validate_evidence(tampered, now=NOW)
        self.assertEqual(result["status"], "blocked")
        self.assertIn("normalized actions", " ".join(result["reasons"]))

    def test_provenance_range_and_symbol_mutations_are_blocked(self):
        client, _, _ = client_for(payload(row()))
        evidence = normalize_evidence(client.fetch(now=NOW), collected_at=NOW)
        mutated = dict(evidence, query_start="2025-09-20")
        self.assertEqual(validate_evidence(mutated, now=NOW)["status"], "blocked")
        mutated = dict(evidence, provenance=dict(evidence["provenance"], endpoint="https://evil.example"))
        self.assertEqual(validate_evidence(mutated, now=NOW)["status"], "blocked")
        mutated = dict(evidence, provenance=dict(evidence["provenance"], data_quality="traded"))
        self.assertEqual(validate_evidence(mutated, now=NOW)["status"], "blocked")
        mutated = dict(evidence, provenance=dict(evidence["provenance"], received_at="2026-09-21T14:00:00+00:00"))
        self.assertEqual(validate_evidence(mutated, now=NOW)["status"], "blocked")
        wrong_client, _, _ = client_for(payload(row(symbol="QQQ")))
        wrong = normalize_evidence(wrong_client.fetch(now=NOW), collected_at=NOW)
        self.assertEqual(validate_evidence(wrong, now=NOW)["status"], "blocked")

    def test_bounded_lookahead_and_future_open_date(self):
        client, _, _ = client_for(payload(row(ex_date="2026-09-28")))
        evidence = normalize_evidence(client.fetch(now=NOW), collected_at=NOW)
        self.assertEqual(action_clearance(evidence, open_date="2026-09-21", now=NOW, lookahead_days=0)["status"], "clear")
        self.assertEqual(action_clearance(evidence, open_date="2026-09-21", now=NOW, lookahead_days=7)["status"], "blocked")
        self.assertEqual(action_clearance(evidence, open_date="2026-09-22", now=NOW, lookahead_days=1)["status"], "blocked")
        self.assertEqual(action_clearance(evidence, open_date="2026-09-21", now=NOW, lookahead_days=8)["status"], "blocked")

    def test_unknown_group_and_incomplete_economic_date_block_clearance(self):
        unknown_client, _, _ = client_for(payload(row(), group="future_unknown_actions"))
        unknown = normalize_evidence(unknown_client.fetch(now=NOW), collected_at=NOW)
        self.assertEqual(action_clearance(unknown, open_date="2026-09-21", now=NOW)["status"], "blocked")
        incomplete_client, _, _ = client_for(payload(row(ex_date=None)))
        incomplete = normalize_evidence(incomplete_client.fetch(now=NOW), collected_at=NOW)
        self.assertEqual(action_clearance(incomplete, open_date="2026-09-21", now=NOW)["status"], "blocked")

    def test_fetch_failure_consumes_claim_and_redacts_detail(self):
        class FailingClient:
            def fetch(self, **kwargs):
                raise RuntimeError("SECRET_TOKEN")

        with tempfile.TemporaryDirectory() as temporary:
            result = collect_once(temporary, client=FailingClient(), clock=lambda: NOW)
            self.assertEqual(result["status"], "fetch_or_validation_failed")
            self.assertEqual(result["detail"], "RuntimeError")
            status = json.loads((Path(temporary) / "status.json").read_text())
            self.assertNotIn("SECRET_TOKEN", json.dumps(status))
            replay = collect_once(temporary, client=FailingClient(), clock=lambda: NOW)
            self.assertEqual(replay["status"], "same_day_replay")


if __name__ == "__main__":
    unittest.main()
