from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import urllib.error
import unittest

from neorome.equity_intraday_sip import SIPClient, capture_decision, capture_later_quote
from neorome.equity_intraday_watchlist import generate


UTC = timezone.utc
BASE = datetime(2026, 9, 21, 13, 46, tzinfo=UTC)


class Response:
    status = 200
    def __init__(self, value): self.value = json.dumps(value).encode()
    def read(self, _limit): return self.value
    def __enter__(self): return self
    def __exit__(self, *_args): pass


class Opener:
    def __init__(self, payloads): self.payloads, self.requests = iter(payloads), []
    def open(self, request, timeout):
        self.requests.append(request)
        return Response(next(self.payloads))


class ForbiddenOpener:
    def __init__(self): self.requests = []
    def open(self, request, timeout):
        self.requests.append(request)
        if len(self.requests) == 1:
            return Response([{"date": "2026-09-21", "open": "09:30", "close": "16:00"}])
        raise urllib.error.HTTPError(request.full_url, 403, "Forbidden", {}, io.BytesIO(b"secret provider body"))


def bars():
    result = []
    start = BASE.replace(hour=13, minute=30)
    for minute in range(16):
        result.append({"t": (start + timedelta(minutes=minute)).isoformat().replace("+00:00", "Z"),
                       "o": 10, "h": 10.1, "l": 9.9, "c": 10.3 if minute == 15 else 10,
                       "v": 100, "vw": 10})
    return result


def frozen_universe(path):
    path.write_text(json.dumps({"source": "pit-vendor", "source_version": "v1", "selection_at": "2026-09-21T13:45:00Z",
                                "historical_membership": {"includes_delisted": True},
                                "members": [{"permanent_id": "p-1", "symbol": "SYNTH", "eligible": True}]}))


class SIPCaptureTests(unittest.TestCase):
    def client(self, opener):
        times = iter([BASE, BASE, BASE, BASE, BASE + timedelta(seconds=.1), BASE + timedelta(seconds=.1),
                      BASE + timedelta(seconds=.2), BASE + timedelta(seconds=.5), BASE + timedelta(seconds=.6)])
        return SIPClient(opener=opener, credential_loader=lambda _service: "test-secret", clock=lambda: next(times))

    def test_bounded_sip_capture_retains_unfilled_candidate_without_orders(self):
        payloads = [
            [{"date": "2026-09-21", "open": "09:30", "close": "16:00"}],
            {"bars": {"SYNTH": bars()}, "next_page_token": None},
            {"quotes": {"SYNTH": [{"t": "2026-09-21T13:46:00.400000Z", "bp": 10, "ap": 10.1, "bs": 10, "as": 10}]}},
        ]
        with TemporaryDirectory() as root:
            universe = Path(root) / "universe.json"; frozen_universe(universe)
            opener = Opener(payloads)
            result = capture_decision(Path(root) / "archive", universe, "SYNTH", client=self.client(opener), fee_cents_per_unit=1)
            self.assertEqual(result["status"], "eligible_unfilled_candidate")
            self.assertEqual(result["broker_orders_sent"], 0)
            self.assertEqual(len(opener.requests), 3)
            self.assertTrue(all(request.get_method() == "GET" and "/orders" not in request.full_url for request in opener.requests))

    def test_generated_prospective_watchlist_runs_full_capture_without_historical_claim(self):
        payloads = [
            [{"date": "2026-09-21", "open": "09:30", "close": "16:00"}],
            {"bars": {"MU": bars()}, "next_page_token": None},
            {"quotes": {"MU": [{"t": "2026-09-21T13:46:00.400000Z", "bp": 10, "ap": 10.1, "bs": 10, "as": 10}]}},
        ]
        with TemporaryDirectory() as root:
            universe = Path(root) / "prospective-watchlist.json"
            generate(universe, session_date="2026-09-21", selection_at="2026-09-21T12:45:00Z")
            result = capture_decision(Path(root) / "archive", universe, "MU", client=self.client(Opener(payloads)), fee_cents_per_unit=1)
            self.assertEqual(result["status"], "eligible_unfilled_candidate")
            self.assertEqual(result["candidate"]["research_label"], "prospective_fixed_watchlist_evidence_only")
            self.assertEqual(result["broker_orders_sent"], 0)

    def test_missing_pit_universe_is_retained_as_no_order_abstention(self):
        with TemporaryDirectory() as root:
            universe = Path(root) / "universe.json"
            universe.write_text(json.dumps({"source": "bad", "source_version": "v1", "selection_at": "2026-09-21T13:45:00Z", "members": []}))
            opener = Opener([])
            result = capture_decision(Path(root) / "archive", universe, "SYNTH", client=self.client(opener))
            self.assertEqual(result["status"], "abstained")
            self.assertEqual(result["gate"], "sip_or_pit_unavailable")
            self.assertEqual(result["broker_orders_sent"], 0)
            self.assertEqual(opener.requests, [])

    def test_independent_later_quote_is_archived_for_runtime_without_ledger(self):
        opener = Opener([{"quotes": {"SYNTH": [{"t": "2026-09-21T13:46:00.050000Z", "bp": 10, "ap": 10.1, "bs": 10, "as": 10}]}}])
        times = iter([BASE, BASE + timedelta(milliseconds=100)])
        client = SIPClient(opener=opener, credential_loader=lambda _service: "test-secret", clock=lambda: next(times))
        with TemporaryDirectory() as root:
            result = capture_later_quote(root, "SYNTH", "entry", client=client)
            self.assertEqual(result["status"], "captured")
            self.assertEqual(len(result["quote"]["source_sha256"]), 64)
            self.assertTrue(Path(result["archived"]["path"]).exists())
            self.assertEqual(result["broker_orders_sent"], 0)
            self.assertEqual(len(opener.requests), 1)

    def test_sip_403_is_retained_as_safe_access_diagnostic_and_abstention(self):
        opener = ForbiddenOpener()
        client = self.client(opener)
        with TemporaryDirectory() as root:
            universe = Path(root) / "universe.json"
            frozen_universe(universe)
            result = capture_decision(Path(root) / "archive", universe, "SYNTH", client=client)

        self.assertEqual(result["status"], "abstained")
        self.assertEqual(result["reason"], "provider_http_403")
        self.assertEqual(result["broker_orders_sent"], 0)
        self.assertFalse(result["eligible_for_execution"])
        self.assertEqual(result["diagnostic"]["provider"], "alpaca")
        self.assertEqual(result["diagnostic"]["endpoint"], "/v2/stocks/bars")
        self.assertEqual(result["diagnostic"]["feed"], "sip")
        self.assertEqual(result["diagnostic"]["http_status"], 403)
        self.assertEqual(result["diagnostic"]["failure_class"], "provider_access_denied")
        self.assertFalse(result["diagnostic"]["fallback_used"])
        self.assertNotIn("secret", json.dumps(result))
        self.assertEqual(len(opener.requests), 2)


if __name__ == "__main__": unittest.main()
