from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from neorome.crypto_research import TOKEN_PROGRAM
from neorome.jupiter_quotes import USDC
from neorome.token_observed_shadow import run_once

UTC = timezone.utc
T0 = datetime(2026, 9, 23, tzinfo=UTC)
MINT = "11111111111111111111111111111111"


def iso(value):
    return value.isoformat().replace("+00:00", "Z")


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class Clock:
    def __init__(self, start):
        self.value = start

    def __call__(self):
        value = self.value
        self.value += timedelta(seconds=1)
        return value


def envelope(provider, requested, received, payload):
    return {"schema_version": 1, "source_kind": "observed", "provider": provider,
            "requested_at": iso(requested), "received_at": iso(received),
            "payload": payload, "payload_sha256": digest(payload)}


def quote(incoming, outgoing, amount, output, minimum):
    return {"chain": "solana", "quote": {"inputMint": incoming, "outputMint": outgoing,
            "inAmount": str(amount), "outAmount": str(output),
            "otherAmountThreshold": str(minimum), "priceImpact": "0.4",
            "priceImpactUnit": "percentage_points"}}


def fixtures():
    discovery_payload = {"candidates": [{"chain": "solana", "mint": MINT,
        "screen_rejections": [], "pool": {"pair_address": MINT, "liquidity_usd": "75000",
                                                   "age_hours_at_observation": "25"},
        "mint_checks": {"program": TOKEN_PROGRAM, "mintAuthority": None,
                        "freezeAuthority": None, "supply_raw": "1000000", "decimals": 6}}]}
    entry_payload = quote(USDC, MINT, 4_800_000, 1_000, 900)
    preflight_payload = quote(MINT, USDC, 900, 4_700_000, 4_600_000)
    hashes = {"discovery": digest(discovery_payload), "entry": digest(entry_payload),
              "preflight": digest(preflight_payload)}
    control_payload = {"version": "token-observed-control-review-v1", "chain": "solana",
        "mint": MINT, "token_program": TOKEN_PROGRAM, "mint_authority": None,
        "freeze_authority": None, "transfer_restrictions": "none_reviewed",
        "owner_concentration_reviewed": True, "liquidity_reviewed": True,
        "discovery_payload_sha256": hashes["discovery"],
        "entry_payload_sha256": hashes["entry"],
        "preflight_payload_sha256": hashes["preflight"],
        "evidence_hashes": {"dexscreener_pairs": "a" * 64, "helius_mint": "b" * 64,
                            "helius_largest_accounts": "c" * 64,
                            "transfer_review": "d" * 64}}
    control_payload["review_manifest_sha256"] = digest(control_payload)
    records = {
        "discovery": envelope("dexscreener+helius", T0, T0, discovery_payload),
        "entry": envelope("jupiter", T0 + timedelta(seconds=1),
                          T0 + timedelta(seconds=1), entry_payload),
        "preflight": envelope("jupiter", T0 + timedelta(seconds=2),
                              T0 + timedelta(seconds=2), preflight_payload),
        "controls": envelope("helius+review", T0 + timedelta(seconds=3),
                             T0 + timedelta(seconds=3), control_payload)}
    costs = {"version": "token-paper-costs-v1", "excludes_embedded_route_fees": True,
             "entry_extra_usdc_raw": "100000", "exit_extra_usdc_raw": "50000"}
    return records, costs


class TokenObservedShadowTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def admit(self, **overrides):
        records, costs = fixtures()
        args = {"discovery": records["discovery"], "entry_quote": records["entry"],
                "sell_preflight": records["preflight"], "control_review": records["controls"],
                "costs": costs, "clock": Clock(T0 + timedelta(seconds=1))}
        args.update(overrides)
        return run_once(self.root, **args)

    def test_callbacks_are_stamped_in_sequence_and_publish_content_addressed_candidate(self):
        records, costs = fixtures()
        order = []

        def callback(name):
            return lambda: (order.append(name), records[name])[1]

        result = run_once(self.root, discovery=callback("discovery"), entry_quote=callback("entry"),
                          sell_preflight=callback("preflight"), control_review=callback("controls"),
                          costs=costs, clock=Clock(T0 + timedelta(seconds=1)))
        self.assertEqual(order, ["discovery", "entry", "preflight", "controls"])
        self.assertEqual(result["status"], "eligible")
        self.assertTrue(result["shadow_eligible"])
        self.assertFalse(result["execution_enabled"])
        self.assertFalse(result["profitability_evidence"])
        self.assertEqual(result["orders_sent"], 0)
        self.assertEqual(result["cash_reserved_cents"], 0)
        self.assertEqual(result["known_at"], iso(T0))
        self.assertEqual(result["received_at"], iso(T0 + timedelta(seconds=1)))
        self.assertIn(result["content_sha256"], Path(result["output"]).name)
        self.assertFalse((self.root / "token_paper.sqlite3").exists())

    def test_future_horizon_callback_is_not_called_during_decision(self):
        called = False

        def future():
            nonlocal called
            called = True
            raise AssertionError("look-ahead callback was called")

        result = self.admit(outcome=future, horizon_minutes=15)
        self.assertEqual(result["status"], "eligible")
        self.assertFalse(called)

    def test_later_received_route_can_mark_or_shadow_exit(self):
        decision = self.admit()
        target = T0 + timedelta(minutes=15, seconds=1)
        outcome_payload = quote(MINT, USDC, 900, 5_000_000, 4_900_000)
        outcome = envelope("jupiter", target - timedelta(seconds=1), target, outcome_payload)
        result = run_once(self.root, outcome=outcome, horizon_minutes=15, shadow_action="exit",
                          clock=Clock(target + timedelta(seconds=1)))
        self.assertEqual(result["status"], "eligible")
        self.assertEqual(result["shadow_action"], "exit")
        self.assertEqual(result["minimum_exit_value_usdc_raw"], "4850000")
        self.assertEqual(result["candidate_content_sha256"], decision["content_sha256"])
        self.assertEqual(result["orders_sent"], 0)

    def test_missing_or_future_outcome_abstains_and_never_invents_exit(self):
        self.admit()
        missing = run_once(self.root, clock=Clock(T0 + timedelta(minutes=15)))
        self.assertEqual(missing["status"], "abstain")
        self.assertIn("outcome route is required", missing["reason"])
        target = T0 + timedelta(minutes=15, seconds=1)
        payload = quote(MINT, USDC, 900, 5_000_000, 4_900_000)
        future = envelope("jupiter", target, target + timedelta(seconds=5), payload)
        result = run_once(self.root, outcome=future, horizon_minutes=15,
                          clock=Clock(target + timedelta(seconds=1)))
        self.assertEqual(result["status"], "abstain")
        self.assertIn("future-dated", result["reason"])

    def test_stale_ambiguous_or_unverifiable_inputs_abstain_precisely(self):
        records, costs = fixtures()
        cases = []
        stale = deepcopy(records["entry"])
        stale["received_at"] = iso(T0 - timedelta(minutes=2))
        stale["requested_at"] = iso(T0 - timedelta(minutes=2, seconds=1))
        cases.append(("entry_quote", stale, "stale"))
        ambiguous = deepcopy(records["preflight"])
        ambiguous["payload"]["quote"].pop("priceImpactUnit")
        ambiguous["payload_sha256"] = digest(ambiguous["payload"])
        cases.append(("sell_preflight", ambiguous, "ambiguous"))
        controls = deepcopy(records["controls"])
        controls["payload"]["mint_authority"] = MINT
        manifest = dict(controls["payload"]); manifest.pop("review_manifest_sha256")
        controls["payload"]["review_manifest_sha256"] = digest(manifest)
        controls["payload_sha256"] = digest(controls["payload"])
        cases.append(("control_review", controls, "unresolved"))
        for field, value, reason in cases:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as root:
                args = {"discovery": records["discovery"], "entry_quote": records["entry"],
                        "sell_preflight": records["preflight"], "control_review": records["controls"],
                        "costs": costs, "clock": Clock(T0 + timedelta(seconds=1))}
                args[field] = value
                result = run_once(root, **args)
                self.assertEqual(result["status"], "abstain")
                self.assertIn(reason, result["reason"])

    def test_wrong_mint_size_cost_or_hash_fails_closed(self):
        records, costs = fixtures()
        cases = []
        wrong_size = deepcopy(records["preflight"])
        wrong_size["payload"]["quote"]["inAmount"] = "901"
        wrong_size["payload_sha256"] = digest(wrong_size["payload"])
        cases.append((wrong_size, costs, "size-matched"))
        expensive = dict(costs); expensive["entry_extra_usdc_raw"] = "200000"
        cases.append((records["preflight"], expensive, "$5"))
        bad_hash = deepcopy(records["preflight"]); bad_hash["payload_sha256"] = "f" * 64
        cases.append((bad_hash, costs, "hash mismatch"))
        for preflight, cost, reason in cases:
            with self.subTest(reason=reason), tempfile.TemporaryDirectory() as root:
                result = run_once(root, discovery=records["discovery"], entry_quote=records["entry"],
                                  sell_preflight=preflight, control_review=records["controls"], costs=cost,
                                  clock=Clock(T0 + timedelta(seconds=1)))
                self.assertEqual(result["status"], "abstain")
                self.assertIn(reason, result["reason"])


if __name__ == "__main__":
    unittest.main()
