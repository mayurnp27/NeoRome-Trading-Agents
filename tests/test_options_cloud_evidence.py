from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest

from neorome.options_cloud_evidence import (
    CloudEvidenceArtifactConflict,
    ReadOnlyCloudSession,
    collect_cloud_evidence,
)


UTC = timezone.utc
NOW = datetime(2026, 9, 22, 14, 0, tzinfo=UTC)
EXPIRY = "2026-11-06"


def _instrument(identifier: str, strike: str, right: str = "call") -> dict:
    return {"id": identifier, "chain_id": "chain-1", "chain_symbol": "SPY",
            "underlying_type": "equity", "expiration_date": EXPIRY,
            "strike_price": strike, "type": right, "state": "active",
            "tradability": "tradable", "trade_value_multiplier": "100.0000"}


def _quote(identifier: str, delta: str) -> dict:
    return {"instrument_id": identifier, "bid_price": "0.40", "ask_price": "0.45",
            "bid_size": 3, "ask_size": 4, "delta": delta, "implied_volatility": "0.20",
            "updated_at": (NOW - timedelta(seconds=5)).isoformat().replace("+00:00", "Z")}


class FakeProvider:
    def __init__(self, *, open_order: bool = False, chain_next: bool = False, stale: bool = False,
                 blank_underlying_symbol: bool = False, mismatched_underlying_id: bool = False, ambiguous_search: bool = False):
        self.open_order = open_order
        self.chain_next = chain_next
        self.stale = stale
        self.blank_underlying_symbol = blank_underlying_symbol
        self.mismatched_underlying_id = mismatched_underlying_id
        self.ambiguous_search = ambiguous_search
        self.calls: list[tuple[str, dict]] = []
        self.instruments = [_instrument("opt-1", "500.0000"), _instrument("opt-2", "505.0000")]
        if stale:
            self.instruments[0]["id"] = "stale-1"

    def __call__(self, tool: str, args: dict):
        self.calls.append((tool, dict(args)))
        if tool == "search":
            rows = [{"symbol": "SPY", "instrument_id": "underlying-1"}]
            if self.ambiguous_search:
                rows.append({"symbol": "SPY", "instrument_id": "underlying-2"})
            return {"results": rows}
        if tool == "get_option_chains":
            underlying_id = "underlying-2" if self.mismatched_underlying_id else "underlying-1"
            result = {"chains": [{"id": "chain-1", "symbol": "SPY", "expiration_dates": [EXPIRY],
                                   "can_open_position": True, "settle_on_open": False,
                                   "trade_value_multiplier": "100", "cash_component": None,
                                   "underlying_instruments": [{"symbol": "" if self.blank_underlying_symbol else "SPY", "instrument": f"https://rh/v1/instruments/{underlying_id}/"}]}]}
            if self.chain_next:
                result["next"] = "https://rh/chains?cursor=more"
            return result
        if tool == "get_option_instruments":
            if args.get("cursor") == "page-2":
                return {"instruments": [self.instruments[1]]}
            return {"instruments": [self.instruments[0]], "next": "https://rh/instruments?cursor=page-2"}
        if tool == "get_option_quotes":
            results = []
            for identifier in args["instrument_ids"]:
                delta = "0.50" if identifier.endswith("1") else "0.25"
                quote = _quote(identifier, delta)
                if self.stale and identifier.endswith("1"):
                    quote["updated_at"] = (NOW - timedelta(minutes=2)).isoformat()
                results.append({"quote": quote})
            return {"results": results}
        if tool == "get_accounts":
            return {"accounts": [{"account_number": "SECRET-ACCOUNT-12345678", "is_default": True}]}
        if tool == "get_option_positions":
            return {"positions": [], "next": ""}
        if tool == "get_option_orders":
            state = "queued" if self.open_order else "filled"
            return {"orders": [{"id": "order-1", "chain_symbol": "SPY", "state": state,
                                 "quantity": "1", "processed_quantity": "0" if self.open_order else "1",
                                 "pending_quantity": "1" if self.open_order else "0", "updated_at": NOW.isoformat()}], "next": ""}
        raise AssertionError(tool)


class LargeCatalogProvider(FakeProvider):
    def __init__(self):
        super().__init__()
        self.instruments = [_instrument(f"opt-{index}", f"{500 + index}.0000") for index in range(101)]

    def __call__(self, tool: str, args: dict):
        if tool == "get_option_instruments":
            self.calls.append((tool, dict(args)))
            if args.get("cursor") == "page-2":
                return {"instruments": self.instruments[100:]}
            return {"instruments": self.instruments[:100], "next": "https://rh/instruments?cursor=page-2"}
        if tool == "get_option_quotes":
            self.calls.append((tool, dict(args)))
            return {"results": [{"quote": _quote(identifier, "0.50" if identifier.endswith("0") else "0.25")}
                                for identifier in args["instrument_ids"]]}
        return super().__call__(tool, args)


class UnavailableFarStrikeProvider(FakeProvider):
    def __init__(self, *, missing_quote=False):
        super().__init__()
        self.missing_quote = missing_quote
        self.instruments = [_instrument("opt-1", "500.0000"), _instrument("opt-2", "505.0000"), _instrument("opt-3", "900.0000")]

    def __call__(self, tool, args):
        if tool == "get_option_instruments":
            self.calls.append((tool, dict(args)))
            return {"instruments": self.instruments}
        if tool == "get_option_quotes":
            self.calls.append((tool, dict(args)))
            rows = []
            for identifier in args["instrument_ids"]:
                if self.missing_quote and identifier == "opt-3":
                    continue
                quote = _quote(identifier, "0.50" if identifier == "opt-1" else "0.25")
                if identifier == "opt-3":
                    quote.update(bid_price=None, ask_price=None, bid_size=0, ask_size=0)
                rows.append({"quote": quote})
            return {"results": rows}
        return super().__call__(tool, args)


class ActualSchemaPositionProvider(FakeProvider):
    def __init__(self, *, duplicate_active=False):
        super().__init__()
        self.duplicate_active = duplicate_active

    def __call__(self, tool, args):
        if tool == "get_option_positions":
            self.calls.append((tool, dict(args)))
            rows = [{"option_id": "opt-1", "quantity": "0"}, {"option_id": "opt-1", "quantity": "0"},
                    {"option_id": "opt-2", "quantity": "1", "pending_sell_quantity": "0"}]
            if self.duplicate_active:
                rows.append({"option_id": "opt-2", "quantity": "0", "pending_buy_quantity": "1"})
            return {"positions": rows, "next": ""}
        return super().__call__(tool, args)


class CursorPaginationProvider(FakeProvider):
    def __init__(self, *, missing_cursor=False, duplicate_cursor=False):
        super().__init__()
        self.missing_cursor = missing_cursor
        self.duplicate_cursor = duplicate_cursor

    def __call__(self, tool, args):
        if tool == "get_option_instruments":
            self.calls.append((tool, dict(args)))
            if args.get("cursor") == "page-2":
                result = {"instruments": [self.instruments[1]]}
                if self.duplicate_cursor:
                    result["next"] = "https://rh/instruments?cursor=page-2"
                return result
            if self.missing_cursor:
                return {"instruments": [self.instruments[0]], "next": "https://rh/instruments?page=2"}
            return {"instruments": [self.instruments[0]], "next": "https://rh/instruments?cursor=page-2"}
        return super().__call__(tool, args)


class AccountDuplicateCursorProvider(FakeProvider):
    def __call__(self, tool, args):
        if tool == "get_option_positions":
            self.calls.append((tool, dict(args)))
            return {"positions": [], "next": "https://rh/positions?cursor=account-page"}
        return super().__call__(tool, args)


class OptionsCloudEvidenceTests(unittest.TestCase):
    def test_complete_paginated_slice_and_flat_snapshot_is_captured(self):
        provider = FakeProvider()
        result = collect_cloud_evidence(provider, symbol="SPY", right="call", as_of=NOW, clock=lambda: NOW)
        self.assertEqual(result["status"], "captured")
        self.assertTrue(result["shadow_ready"])
        self.assertEqual(result["chain"]["pages"], 2)
        self.assertEqual(len(result["normalized_quotes"]), 2)
        self.assertEqual(result["size_units"], "contracts")
        self.assertFalse(result["broker_write_called"])
        self.assertEqual(result["holdings"]["option_holdings"], {})
        self.assertNotIn("SECRET-ACCOUNT", json.dumps(result))
        account_args = [args for tool, args in provider.calls if tool == "get_option_positions"][0]
        self.assertEqual(account_args["account_number"], "SECRET-ACCOUNT-12345678")
        stored_args = [item["arguments"] for item in result["responses"] if item["tool"] == "get_option_positions"][0]
        self.assertEqual(stored_args["account_number"], "[redacted]")

    def test_capture_as_of_encompasses_holdings_clock_after_final_provider_record(self):
        tick = 0

        def advancing_clock():
            nonlocal tick
            tick += 1
            return NOW + timedelta(milliseconds=tick)

        result = collect_cloud_evidence(FakeProvider(), symbol="SPY", right="call", as_of=NOW,
                                        clock=advancing_clock)
        capture_as_of = datetime.fromisoformat(result["as_of"].replace("Z", "+00:00"))
        holdings_receipt = datetime.fromisoformat(result["holdings"]["local_receipt_at"].replace("Z", "+00:00"))
        final_record_receipt = datetime.fromisoformat(result["responses"][-1]["received_at"].replace("Z", "+00:00"))
        self.assertGreaterEqual(capture_as_of, holdings_receipt)
        self.assertGreaterEqual(capture_as_of, final_record_receipt)
        self.assertLess(result["local_receipt_at"], result["as_of"])
        from neorome.options_observed_pipeline import produce_observed_candidate
        from tests.test_options_shadow_rehearsal import _evidence_fixture
        observation, context, _rows = _evidence_fixture()
        self.assertEqual(produce_observed_candidate(result, observation, context)["status"],
                         "eligible_shadow_candidate")

    def test_stale_quote_and_chain_pagination_abstain_with_provider_limitation(self):
        stale = collect_cloud_evidence(FakeProvider(stale=True), symbol="SPY", right="call", as_of=NOW, clock=lambda: NOW)
        self.assertEqual(stale["status"], "abstain")
        self.assertIn("stale", stale["reason"])
        self.assertEqual(stale["reason_code"], "no_viable_pair")
        self.assertFalse(stale["operational_failure"])
        incomplete = collect_cloud_evidence(FakeProvider(chain_next=True), symbol="SPY", right="call", as_of=NOW, clock=lambda: NOW)
        self.assertEqual(incomplete["status"], "abstain")
        self.assertIn("pagination", incomplete["reason"])
        self.assertTrue(incomplete["operational_failure"])

    def test_missing_pagination_cursor_is_explicit_incomplete_safe_abstention(self):
        result = collect_cloud_evidence(CursorPaginationProvider(missing_cursor=True), symbol="SPY", right="call",
                                        as_of=NOW, clock=lambda: NOW)
        self.assertEqual(result["status"], "abstain")
        self.assertEqual(result["reason_code"], "incomplete_pagination")
        self.assertIn("no cursor", result["reason"])
        self.assertTrue(result["incomplete"])
        self.assertFalse(result["chain_complete"])
        self.assertFalse(result["operational_failure"])
        self.assertEqual(result["orders_sent"], 0)
        self.assertFalse(result["broker_write_called"])

    def test_duplicate_pagination_cursor_is_explicit_incomplete_safe_abstention(self):
        result = collect_cloud_evidence(CursorPaginationProvider(duplicate_cursor=True), symbol="SPY", right="call",
                                        as_of=NOW, clock=lambda: NOW)
        self.assertEqual(result["status"], "abstain")
        self.assertEqual(result["reason_code"], "incomplete_pagination")
        self.assertIn("cursor repeated", result["reason"])
        self.assertTrue(result["incomplete"])
        self.assertFalse(result["operational_failure"])
        self.assertEqual(result["orders_sent"], 0)
        self.assertFalse(result["broker_write_called"])

    def test_duplicate_account_pagination_cursor_is_also_incomplete(self):
        result = collect_cloud_evidence(AccountDuplicateCursorProvider(), symbol="SPY", right="call",
                                        as_of=NOW, clock=lambda: NOW)
        self.assertEqual(result["status"], "abstain")
        self.assertEqual(result["reason_code"], "incomplete_pagination")
        self.assertIn("get_option_positions", result["reason"])
        self.assertTrue(result["incomplete"])
        self.assertFalse(result["operational_failure"])
        self.assertEqual(result["orders_sent"], 0)

    def test_open_order_requires_holdings_reconciliation(self):
        result = collect_cloud_evidence(FakeProvider(open_order=True), symbol="SPY", right="call", as_of=NOW, clock=lambda: NOW)
        self.assertEqual(result["status"], "abstain")
        self.assertEqual(result["reason_code"], "holdings_reconciliation_required")
        self.assertFalse(result["shadow_ready"])

    def test_blank_chain_symbol_joins_exact_search_uuid_but_mismatch_or_ambiguity_refuses(self):
        accepted = collect_cloud_evidence(FakeProvider(blank_underlying_symbol=True), symbol="SPY", right="call", as_of=NOW, clock=lambda: NOW)
        self.assertEqual(accepted["status"], "captured")
        mismatch = collect_cloud_evidence(FakeProvider(blank_underlying_symbol=True, mismatched_underlying_id=True), symbol="SPY", right="call", as_of=NOW, clock=lambda: NOW)
        self.assertEqual(mismatch["status"], "abstain")
        ambiguous = collect_cloud_evidence(FakeProvider(ambiguous_search=True), symbol="SPY", right="call", as_of=NOW, clock=lambda: NOW)
        self.assertEqual(ambiguous["status"], "abstain")

    def test_complete_catalog_can_be_reduced_to_selector_bounded_slice(self):
        result = collect_cloud_evidence(LargeCatalogProvider(), symbol="SPY", right="call", as_of=NOW, clock=lambda: NOW)
        self.assertEqual(result["status"], "captured")
        self.assertEqual(result["chain"]["catalog_contract_count"], 101)
        self.assertEqual(result["chain"]["bounded_slice_count"], 100)
        self.assertEqual(len(result["normalized_quotes"]), 100)
        self.assertEqual(result["catalog_quote_count"], 101)

    def test_unavailable_far_strikes_are_hashed_exclusions_but_missing_quote_refuses(self):
        accepted = collect_cloud_evidence(UnavailableFarStrikeProvider(), symbol="SPY", right="call", as_of=NOW, clock=lambda: NOW)
        self.assertEqual(accepted["status"], "captured")
        self.assertEqual(accepted["chain"]["catalog_contract_count"], 3)
        self.assertEqual(accepted["catalog_executable_quote_count"], 2)
        self.assertEqual(accepted["quote_exclusion_counts"], {"null_or_nonfinite_quote": 1})
        self.assertEqual(len(accepted["quote_exclusions_sha256"]), 64)
        missing = collect_cloud_evidence(UnavailableFarStrikeProvider(missing_quote=True), symbol="SPY", right="call", as_of=NOW, clock=lambda: NOW)
        self.assertEqual(missing["status"], "abstain")
        self.assertIn("incomplete", missing["reason"])

    def test_actual_position_schema_allows_duplicate_zero_rows_but_refuses_duplicate_active_identity(self):
        active = collect_cloud_evidence(ActualSchemaPositionProvider(), symbol="SPY", right="call", as_of=NOW, clock=lambda: NOW)
        self.assertEqual(active["status"], "captured")
        self.assertTrue(active["shadow_ready"])
        self.assertEqual(active["holdings"]["option_holdings"], {"SPY261106C00505000": 1})
        duplicate = collect_cloud_evidence(ActualSchemaPositionProvider(duplicate_active=True), symbol="SPY", right="call", as_of=NOW, clock=lambda: NOW)
        self.assertEqual(duplicate["status"], "abstain")
        self.assertIn("duplicate active", duplicate["reason"])

    def test_output_path_replay_is_idempotent_and_conflicts_fail_closed(self):
        evidence = FakeProvider()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "capture.json"
            first = collect_cloud_evidence(evidence, symbol="SPY", right="call", as_of=NOW, output_path=path, clock=lambda: NOW)
            replay = collect_cloud_evidence(FakeProvider(), symbol="SPY", right="call", as_of=NOW, output_path=path, clock=lambda: NOW)
            self.assertEqual(first, replay)
            changed = FakeProvider(open_order=True)
            with self.assertRaises(CloudEvidenceArtifactConflict):
                collect_cloud_evidence(changed, symbol="SPY", right="call", as_of=NOW, output_path=path, clock=lambda: NOW)

    def test_transport_rejects_every_write_tool(self):
        with self.assertRaises(ValueError):
            ReadOnlyCloudSession.call(object(), "place_option_order", {})


if __name__ == "__main__":
    unittest.main()
