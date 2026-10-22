from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

import neorome

# In the assigned staging workspace, reuse the read-only production client
# snapshot while loading this implementation from the staging tree.
_stage_package = Path(__file__).resolve().parents[1] / "neorome"
if str(_stage_package) not in neorome.__path__:
    neorome.__path__.insert(0, str(_stage_package))

from neorome.token_social import SocialError
from neorome.token_social_cache import BudgetedXClient


UTC = timezone.utc
MINT = "11111111111111111111111111111111"
MINT_2 = "So11111111111111111111111111111111111111112"


class Clock:
    def __init__(self, value):
        self.value = value

    def __call__(self):
        return self.value


def evidence(mint, now):
    return {
        "schema_version": 1, "provider": "x", "source_kind": "observed",
        "query_kind": "contract_address", "mint": mint,
        "requested_at": (now - timedelta(seconds=2)).isoformat(),
        "received_at": now.isoformat(), "response_sha256": "a" * 64,
        "posts": [], "signals": {"mentions": 0}, "complete": True,
    }


class Provider:
    def __init__(self, clock, failure=None, mutate=None):
        self.clock = clock
        self.failure = failure
        self.mutate = mutate
        self.calls = []

    def search_contract(self, mint, max_results=20):
        self.calls.append((mint, max_results))
        if self.failure:
            raise self.failure
        value = evidence(mint, self.clock())
        if self.mutate:
            return self.mutate(value)
        return value


class CrossProcessProvider:
    def __init__(self, counter, now):
        self.counter = counter
        self.now = now

    def search_contract(self, mint, max_results=20):
        import time
        with self.counter.get_lock():
            self.counter.value += 1
        time.sleep(0.15)
        return evidence(mint, self.now)


def _cross_process_fetch(cache_dir, counter, result_queue, now):
    from neorome.token_social_cache import BudgetedXClient
    client = BudgetedXClient(cache_dir, 30, client=CrossProcessProvider(counter, now), clock=lambda: now)
    try:
        result_queue.put(("ok", client.search_contract(MINT)["mint"]))
    except Exception as exc:
        result_queue.put(("error", type(exc).__name__))


class TokenSocialCacheTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir="/private/tmp")
        self.directory = Path(self.temp.name) / "shared" / "x-social"
        self.clock = Clock(datetime(2026, 9, 30, 14, tzinfo=UTC))

    def tearDown(self):
        self.temp.cleanup()

    def wrapper(self, provider, normal=0, extra=0):
        return BudgetedXClient(self.directory, normal, extra, provider, self.clock)

    def test_shared_cache_reuses_source_and_preserves_original_provenance(self):
        provider = Provider(self.clock)
        wrapper = self.wrapper(provider, normal=30)
        first = wrapper.search_contract(MINT)
        original = (first["requested_at"], first["received_at"], first["response_sha256"], first["normalized_sha256"])
        self.clock.value += timedelta(seconds=20)
        second = wrapper.search_contract(MINT)
        self.assertEqual(len(provider.calls), 1)
        self.assertTrue(second["x_collection"]["cache_hit"])
        self.assertEqual((second["requested_at"], second["received_at"], second["response_sha256"], second["normalized_sha256"]), original)
        self.assertEqual(wrapper.last_request_metadata["api_requests"], 0)
        with sqlite3.connect(self.directory / "evidence.sqlite3") as db:
            self.assertEqual(db.execute("SELECT normal_cents,exception_cents FROM daily_spend").fetchone(), (30, 0))
            self.assertEqual(db.execute("SELECT COUNT(*) FROM request_audit").fetchone()[0], 1)

    def test_concurrent_same_mint_has_one_provider_call(self):
        import threading

        class SlowProvider(Provider):
            def search_contract(inner, mint, max_results=20):
                inner.calls.append((mint, max_results))
                import time
                time.sleep(0.1)
                inner.clock.value += timedelta(seconds=10)
                return evidence(mint, inner.clock())

        provider = SlowProvider(self.clock)
        wrappers = [self.wrapper(provider, normal=30) for _ in range(2)]
        results, failures = [], []

        def fetch(wrapper):
            try:
                results.append(wrapper.search_contract(MINT))
            except Exception as exc:
                failures.append(exc)

        threads = [threading.Thread(target=fetch, args=(wrapper,)) for wrapper in wrappers]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
        self.assertEqual(failures, [])
        self.assertEqual(len(provider.calls), 1)
        self.assertEqual(sum(bool(item["x_collection"]["cache_hit"]) for item in results), 1)

    def test_separate_processes_share_one_mint_request(self):
        import multiprocessing

        context = multiprocessing.get_context("spawn")
        counter = context.Value("i", 0)
        result_queue = context.Queue()
        args = (str(self.directory), counter, result_queue, self.clock.value)
        processes = [context.Process(target=_cross_process_fetch, args=args) for _ in range(2)]
        for process in processes:
            process.start()
        for process in processes:
            process.join(timeout=10)
        for process in processes:
            self.assertEqual(process.exitcode, 0)
        results = [result_queue.get(timeout=2) for _ in processes]
        self.assertEqual(results, [("ok", MINT), ("ok", MINT)])
        self.assertEqual(counter.value, 1)

    def test_default_zero_budget_never_calls_provider_or_invents_evidence(self):
        provider = Provider(self.clock)
        wrapper = self.wrapper(provider)
        with self.assertRaisesRegex(SocialError, "budget exhausted"):
            wrapper.search_contract(MINT)
        self.assertEqual(provider.calls, [])
        self.assertEqual(wrapper.last_request_metadata["budget_status"], "budget_exhausted")

    def test_worst_case_posts_and_authors_are_reserved(self):
        provider = Provider(self.clock)
        wrapper = self.wrapper(provider, normal=29)
        with self.assertRaisesRegex(SocialError, "budget exhausted"):
            wrapper.search_contract(MINT, max_results=20)
        self.assertEqual(provider.calls, [])
        self.assertEqual(wrapper.last_request_metadata["estimated_cost_cents"], 30)

    def test_utc_day_rollover_resets_spend_after_old_evidence_expires(self):
        provider = Provider(self.clock)
        wrapper = self.wrapper(provider, normal=30)
        wrapper.search_contract(MINT)
        self.clock.value = datetime(2026, 10, 1, 0, 0, 1, tzinfo=UTC)
        wrapper.search_contract(MINT)
        self.assertEqual(len(provider.calls), 2)
        with sqlite3.connect(self.directory / "evidence.sqlite3") as db:
            rows = db.execute("SELECT day,normal_cents FROM daily_spend ORDER BY day").fetchall()
        self.assertEqual(rows, [("2026-09-30", 30), ("2026-10-01", 30)])

    def test_exception_budget_only_covers_normal_budget_shortfall(self):
        provider = Provider(self.clock)
        wrapper = self.wrapper(provider, normal=5, extra=25)
        result = wrapper.search_contract(MINT, exception_reason="strong momentum and flow")
        self.assertEqual(result["x_collection"]["normal_reserved_cost_cents"], 5)
        self.assertEqual(result["x_collection"]["exception_reserved_cost_cents"], 25)
        self.assertEqual(result["x_collection"]["exception_reason"], "strong momentum and flow")
        self.assertEqual(len(provider.calls), 1)

    def test_provider_429_spends_reservation_and_starts_global_cooldown(self):
        provider = Provider(self.clock, failure=SocialError("X search HTTP 429"))
        wrapper = self.wrapper(provider, normal=60)
        with self.assertRaisesRegex(SocialError, "cooldown started"):
            wrapper.search_contract(MINT)
        self.assertEqual(wrapper.last_request_metadata["http_status"], 429)
        self.assertEqual(wrapper.last_request_metadata["reserved_cost_cents"], 30)
        self.assertNotIn("429", wrapper.last_request_metadata["sanitized_reason"])
        other = self.wrapper(Provider(self.clock), normal=60)
        with self.assertRaisesRegex(SocialError, "cooldown active"):
            other.search_contract(MINT_2)
        self.assertEqual(other.last_request_metadata["api_requests"], 0)
        with sqlite3.connect(self.directory / "evidence.sqlite3") as db:
            self.assertEqual(db.execute("SELECT status,http_status FROM request_audit").fetchone(), ("provider_limit", 429))
            self.assertEqual(db.execute("SELECT normal_cents FROM daily_spend").fetchone()[0], 30)

    def test_provider_failure_is_sanitized_and_reserved_cost_is_not_refunded(self):
        provider = Provider(self.clock, failure=RuntimeError("secret bearer token"))
        wrapper = self.wrapper(provider, normal=30)
        with self.assertRaisesRegex(SocialError, "evidence unavailable") as caught:
            wrapper.search_contract(MINT)
        self.assertNotIn("secret", str(caught.exception))
        self.assertEqual(wrapper.last_request_metadata["budget_status"], "provider_unavailable")
        with sqlite3.connect(self.directory / "evidence.sqlite3") as db:
            self.assertEqual(db.execute("SELECT normal_cents FROM daily_spend").fetchone()[0], 30)
            self.assertEqual(db.execute("SELECT status FROM request_audit").fetchone()[0], "provider_unavailable")

    def test_stale_and_future_timestamps_are_rejected_after_charging(self):
        for label, stamp in (("stale", self.clock.value - timedelta(seconds=61)),
                             ("future", self.clock.value + timedelta(seconds=1))):
            with self.subTest(label=label):
                directory = self.directory / label
                provider = Provider(self.clock, mutate=lambda value, stamp=stamp: {
                    **value, "requested_at": (stamp - timedelta(seconds=1)).isoformat(),
                    "received_at": stamp.isoformat(),
                })
                wrapper = BudgetedXClient(directory, 30, client=provider, clock=self.clock)
                with self.assertRaises(SocialError):
                    wrapper.search_contract(MINT)
                self.assertEqual(wrapper.last_request_metadata["budget_status"], "provider_unavailable")
                with sqlite3.connect(directory / "evidence.sqlite3") as db:
                    self.assertEqual(db.execute("SELECT normal_cents FROM daily_spend").fetchone()[0], 30)
                    self.assertEqual(db.execute("SELECT COUNT(*) FROM evidence").fetchone()[0], 0)

    def test_cached_mint_schema_mismatch_is_discarded_and_requeried(self):
        provider = Provider(self.clock)
        wrapper = self.wrapper(provider, normal=60)
        wrapper.search_contract(MINT)
        with sqlite3.connect(self.directory / "evidence.sqlite3") as db:
            body = json.loads(db.execute("SELECT body FROM evidence WHERE mint=?", (MINT,)).fetchone()[0])
            body["mint"] = MINT_2
            db.execute("UPDATE evidence SET body=? WHERE mint=?", (json.dumps(body), MINT))
        wrapper.search_contract(MINT)
        self.assertEqual(len(provider.calls), 2)
        self.assertEqual(wrapper.last_request_metadata["cache_status"], "expired_or_invalid")


if __name__ == "__main__":
    unittest.main()
