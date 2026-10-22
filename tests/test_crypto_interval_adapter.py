import copy
import json
from pathlib import Path
import unittest

from neorome.crypto_interval_adapter import adapt_interval_observed


ARCHIVE = Path("coordination/verification/crypto-cross-lane/interval-observed.json")


def capture():
    return json.loads(ARCHIVE.read_text(encoding="utf-8"))


def explicit_capture():
    data = capture()
    data = copy.deepcopy(data)
    book = data["books"][0]
    book["size_unit"] = "contracts"
    book["event_at"] = "2026-09-23T02:21:03.050000Z"
    book["payload"]["orderbook_fp"] = {
        "yes_dollars": [["0.40", "2"], ["0.30", "1"]],
        "no_dollars": [["0.50", "3"], ["0.60", "1"]],
    }
    return data


FEES = {
    "version": "test-fixed-stress-v1",
    "entry_fee_cents_per_contract": "1",
    "exit_fee_cents_per_contract": "1",
    "settlement_fee_cents_per_contract": "1",
}


class CryptoIntervalAdapterTests(unittest.TestCase):
    def test_complements_opposite_binary_bids_with_decimal_prices_and_provenance(self):
        result = adapt_interval_observed(explicit_capture(), fee_schedule=FEES)

        self.assertEqual(result["events"][0]["asks"][0]["price"], "0.40")
        self.assertEqual(result["events"][0]["asks"][0]["raw_side"], "no")
        self.assertTrue(result["events"][0]["asks"][0]["complemented"])
        self.assertEqual(result["events"][0]["asks"][0]["raw_price"], "0.60")
        self.assertEqual(result["events"][0]["asks"][0]["size"], 1)
        self.assertEqual(result["events"][0]["bids"][0]["price"], "0.40")
        self.assertEqual(result["events"][0]["bids"][0]["raw_side"], "yes")

    def test_exact_terms_source_and_engine_identity_are_bound(self):
        result = adapt_interval_observed(explicit_capture(), fee_schedule=FEES)

        contract = result["contract"]
        self.assertEqual(contract["market_id"], "KXBTC15M-26SEP222230-30")
        self.assertEqual(contract["source_identity"]["settlement_source"], "CF Benchmarks")
        self.assertEqual(contract["source_identity"]["benchmark"], "BRTI")
        self.assertEqual(contract["source_identity"]["engine_id"], 2)
        self.assertEqual(result["provenance"]["engine_id"], 2)
        self.assertEqual(len(contract["terms"]["terms_sha256"]), 64)
        self.assertEqual(result["fee"]["version"], "test-fixed-stress-v1")

    def test_missing_exchange_event_timestamp_is_never_invented(self):
        result = adapt_interval_observed(capture())

        event = result["events"][0]
        self.assertIsNone(event["event_at"])
        self.assertEqual(event["received_at"], "2026-09-23T02:21:03.112997+00:00")
        self.assertIn("missing_exchange_event_timestamp", result["admission_reasons"])
        self.assertFalse(result["admissible_for_replay"])
        self.assertFalse(result["execution_enabled"])
        self.assertFalse(result["paper_admitted"])

    def test_real_capture_rejects_ambiguous_units_and_unverified_quadratic_fee(self):
        result = adapt_interval_observed(capture())

        self.assertIn("ambiguous_orderbook_size_units", result["admission_reasons"])
        self.assertIn("non_integer_contract_depth", result["admission_reasons"])
        self.assertEqual(result["fee"]["status"], "unresolved")
        self.assertIn("quadratic_fee_formula_unverified", result["fee"]["reason"])
        self.assertFalse(result["admissible_for_replay"])

    def test_engine_mismatch_and_invalid_fee_mapping_cannot_admit(self):
        data = explicit_capture()
        data["markets"]["payload"]["markets"][0]["exchange_index"] = 7
        result = adapt_interval_observed(data, fee_schedule={"version": "missing-fields-v1"})

        self.assertIn("engine_identity_mismatch", result["admission_reasons"])
        self.assertIn("fee_schedule_fields_required", result["admission_reasons"])
        self.assertFalse(result["admissible_for_replay"])

    def test_fixed_fee_stress_is_not_mistaken_for_a_probability(self):
        fees = dict(FEES)
        fees["entry_fee_cents_per_contract"] = "2.5"
        result = adapt_interval_observed(explicit_capture(), fee_schedule=fees)

        self.assertEqual(result["fee"]["status"], "caller_supplied_fixed_stress")
        self.assertNotIn("fee_schedule_values_invalid", result["admission_reasons"])


if __name__ == "__main__":
    unittest.main()
