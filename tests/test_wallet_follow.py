import importlib.util
from pathlib import Path
import unittest

MODULE = Path(__file__).resolve().parents[1] / "neorome" / "wallet_follow.py"
spec = importlib.util.spec_from_file_location("wallet_follow", MODULE)
wf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wf)


def address(number):
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    raw = number.to_bytes(32, "big")
    encoded = ""
    value = int.from_bytes(raw, "big")
    while value:
        value, rem = divmod(value, 58)
        encoded = alphabet[rem] + encoded
    return "1" * (len(raw) - len(raw.lstrip(b"\0"))) + encoded


WALLET = address(101)
MINTS = [address(i) for i in range(201, 220)]
CUTOFF = "2026-09-30T12:00:00Z"
HASH = "a" * 64


def fill(signature, mint, side, when, quantity="1", quote="10", fee="0",
         currency=None, wallet=WALLET, observed=None):
    return {
        "wallet": wallet, "mint": mint, "signature": signature,
        "block_time": when, "observed_at": observed or when, "side": side,
        "quantity": quantity, "quote_mint": currency or wf.USDC,
        "quote_quantity": quote, "fee_quote": fee, "source_sha256": HASH,
    }


def qualifying_rows(wallet=WALLET):
    rows = []
    for i in range(10):
        mint = MINTS[i % 5]
        hour = i // 5
        rows.append(fill(f"buy-{i}", mint, "buy", f"2026-09-30T0{hour}:00:00Z", quote="10", fee="1", wallet=wallet))
        rows.append(fill(f"sell-{i}", mint, "sell", f"2026-09-30T0{hour}:10:00Z", quote="14", fee="1", wallet=wallet))
    return rows


class WalletFollowTests(unittest.TestCase):
    def test_closed_fifo_metrics_include_entry_and_exit_fees(self):
        result = wf.score_wallets(qualifying_rows(), CUTOFF)
        cohort = next(item for item in result["rankings"] if item["quote_mint"] == wf.USDC)
        self.assertTrue(cohort["qualified"])
        self.assertEqual(cohort["closed_buy_lots"], 10)
        self.assertEqual(cohort["net_pnl"], "20")
        self.assertEqual(cohort["gross_positive_pnl"], "20")
        self.assertEqual(cohort["closed_lots"][0]["pnl"], "2")

    def test_partial_sells_allocate_basis_and_close_buy_lot_once(self):
        rows = [
            fill("b", MINTS[0], "buy", "2026-09-30T10:00:00Z", quantity="4", quote="20", fee="4"),
            fill("s1", MINTS[0], "sell", "2026-09-30T10:01:00Z", quantity="1", quote="8", fee="1"),
            fill("s2", MINTS[0], "sell", "2026-09-30T10:02:00Z", quantity="3", quote="24", fee="3"),
        ]
        result = wf.score_wallets(rows, CUTOFF)
        self.assertEqual(result["coverage"]["closed_buy_lots"], 1)
        lot = result["rankings"][0]["closed_lots"][0]
        self.assertEqual(lot["cost"], "24")
        self.assertEqual(lot["proceeds"], "28")
        self.assertEqual(lot["pnl"], "4")

    def test_unknown_or_excess_inventory_sells_are_excluded_without_inventing_lots(self):
        rows = [
            fill("opening-sell", MINTS[0], "sell", "2026-09-30T09:00:00Z"),
            fill("buy", MINTS[0], "buy", "2026-09-30T09:01:00Z", quantity="1"),
            fill("oversell", MINTS[0], "sell", "2026-09-30T09:02:00Z", quantity="2"),
        ]
        result = wf.score_wallets(rows, CUTOFF)
        self.assertEqual(result["coverage"]["closed_buy_lots"], 0)
        self.assertEqual([issue["reason"] for issue in result["issues"]],
                         ["sell_exceeds_known_inventory", "sell_exceeds_known_inventory"])
        self.assertTrue(all(issue["wallet"] == WALLET for issue in result["issues"]))

    def test_unknown_inventory_issue_disqualifies_wallet_currency(self):
        rows = qualifying_rows()
        rows.append(fill("unexplained-opening-sell", MINTS[15], "sell", "2026-09-30T11:00:00Z"))
        result = wf.score_wallets(rows, CUTOFF)
        cohort = next(item for item in result["rankings"] if item["wallet"] == WALLET)
        self.assertFalse(cohort["qualified"])
        self.assertIn("sell_exceeds_known_inventory", cohort["ineligibility_reasons"])
        self.assertEqual(result["issues"][-1]["wallet"], WALLET)

    def test_quote_currencies_are_scored_in_separate_cohorts(self):
        rows = qualifying_rows()
        rows += [
            fill("sol-b", MINTS[10], "buy", "2026-09-30T10:00:00Z", quote="2", fee="0.1", currency=wf.SOL),
            fill("sol-s", MINTS[10], "sell", "2026-09-30T10:01:00Z", quote="3", fee="0.1", currency=wf.SOL),
        ]
        result = wf.score_wallets(rows, CUTOFF)
        by_currency = {item["quote_mint"]: item for item in result["rankings"]}
        self.assertEqual(by_currency[wf.USDC]["net_pnl"], "20")
        self.assertEqual(by_currency[wf.SOL]["net_pnl"], "0.8")
        self.assertEqual(by_currency[wf.SOL]["closed_buy_lots"], 1)
        self.assertFalse(by_currency[wf.SOL]["qualified"])

    def test_explicit_unpriced_native_cost_rejects_currency_cohort(self):
        rows = qualifying_rows()
        rows[0]["costs_complete"] = False
        rows[0]["normalization_version"] = "wallet-swap-v2"
        result = wf.score_wallets(rows, CUTOFF)
        cohort = next(item for item in result["rankings"] if item["quote_mint"] == wf.USDC)
        self.assertFalse(cohort["qualified"])
        self.assertIn("unpriced_native_cost", cohort["ineligibility_reasons"])
        self.assertEqual(result["coverage"]["costs_incomplete"], 1)
        self.assertIn("unpriced_native_cost", [issue["reason"] for issue in result["issues"]])

    def test_one_unpriced_row_disqualifies_otherwise_qualified_wallet(self):
        rows = qualifying_rows()
        extra = fill("unpriced-extra", MINTS[16], "buy", "2026-09-30T11:00:00Z")
        extra["costs_complete"] = False
        extra["normalization_version"] = "wallet-swap-v2"
        rows.append(extra)
        cohort = next(item for item in wf.score_wallets(rows, CUTOFF)["rankings"]
                      if item["quote_mint"] == wf.USDC)
        self.assertFalse(cohort["qualified"])
        self.assertEqual(cohort["closed_buy_lots"], 10)
        self.assertIn("unpriced_native_cost", cohort["ineligibility_reasons"])

    def test_accounting_warnings_allow_provisional_paper_only_cohort(self):
        rows = qualifying_rows()
        unpriced = fill("unpriced-extra", MINTS[16], "buy", "2026-09-30T11:00:00Z")
        unpriced["costs_complete"] = False
        unpriced["normalization_version"] = "wallet-swap-v2"
        rows.append(unpriced)
        rows.append(fill("unexplained-opening-sell", MINTS[17], "sell", "2026-09-30T11:01:00Z"))
        rows.extend((
            fill("currency-buy", MINTS[18], "buy", "2026-09-30T11:02:00Z", currency=wf.USDC),
            fill("currency-mismatch-sell", MINTS[18], "sell", "2026-09-30T11:03:00Z", currency=wf.SOL),
        ))
        cohort = wf.score_wallets(rows, CUTOFF)["rankings"][0]
        self.assertFalse(cohort["qualified"])
        self.assertTrue(cohort["paper_qualified"])
        self.assertEqual(set(cohort["paper_warnings"]), {
            "unpriced_native_cost", "sell_exceeds_known_inventory", "sell_currency_mismatch"})
        # The incomplete row and unresolved disposals never enter priced results.
        self.assertEqual(cohort["closed_buy_lots"], 10)
        self.assertEqual(cohort["net_pnl"], "20")

    def test_negative_performance_stays_blocked_from_paper_cohort(self):
        rows = qualifying_rows()
        for row in rows:
            if row["side"] == "sell":
                row["quote_quantity"] = "1"
        cohort = wf.score_wallets(rows, CUTOFF)["rankings"][0]
        self.assertFalse(cohort["qualified"])
        self.assertFalse(cohort["paper_qualified"])
        self.assertIn("nonpositive_net_pnl", cohort["ineligibility_reasons"])

    def test_paper_signal_allows_unpriced_fresh_buy_but_strict_does_not(self):
        rows = qualifying_rows()
        historical_unpriced = fill("historical-unpriced", MINTS[16], "buy", "2026-09-30T11:00:00Z")
        historical_unpriced["costs_complete"] = False
        historical_unpriced["normalization_version"] = "wallet-swap-v2"
        rows.append(historical_unpriced)
        ranked = wf.score_wallets(rows, CUTOFF)
        fresh = fill("fresh-unpriced", MINTS[12], "buy", "2026-09-30T12:00:30Z")
        fresh["costs_complete"] = False
        fresh["normalization_version"] = "wallet-swap-v2"
        strict = wf.following_signals([fresh], ranked, "2026-09-30T12:00:40Z")
        paper = wf.following_signals([fresh], ranked, "2026-09-30T12:00:40Z", eligibility="paper")
        self.assertEqual(strict["signals"], [])
        self.assertEqual(paper["signals"][0]["signature"], "fresh-unpriced")
        self.assertEqual(paper["signals"][0]["paper_warnings"], ["unpriced_native_cost"])

    def test_paper_eligibility_helper_preserves_frozen_cutoff_and_fails_closed(self):
        ranked = wf.score_wallets(qualifying_rows(), CUTOFF)
        frozen_as_of = ranked["as_of"]
        ranked["rankings"][0]["qualified"] = False
        ranked["rankings"][0]["ineligibility_reasons"] = ["sell_exceeds_known_inventory"]
        ranked["qualified"] = []
        wf.apply_paper_eligibility(ranked)
        self.assertEqual(ranked["as_of"], frozen_as_of)
        self.assertEqual(ranked["qualified"], [])
        self.assertEqual(len(ranked["paper_qualified"]), 1)
        ranked["rankings"][0]["ineligibility_reasons"] = None
        wf.apply_paper_eligibility(ranked)
        self.assertEqual(ranked["paper_qualified"], [])

    def test_unpriced_buy_cannot_be_emitted_as_follow_signal(self):
        ranked = wf.score_wallets(qualifying_rows(), CUTOFF)
        row = fill("unpriced-signal", MINTS[12], "buy", "2026-09-30T12:00:30Z")
        row["costs_complete"] = False
        row["normalization_version"] = "wallet-swap-v2"
        result = wf.following_signals([row], ranked, "2026-09-30T12:00:40Z")
        self.assertEqual(result["signals"], [])
        self.assertEqual(result["issues"][0]["reason"], "unpriced_native_cost")

    def test_legacy_rows_without_cost_completeness_remain_supported(self):
        self.assertTrue(wf.score_wallets(qualifying_rows(), CUTOFF)["rankings"][0]["qualified"])

    def test_invalid_cost_completeness_type_is_rejected(self):
        row = fill("bad-cost-flag", MINTS[0], "buy", "2026-09-30T11:00:00Z")
        row["costs_complete"] = 1
        result = wf.score_wallets([row], CUTOFF)
        self.assertEqual(result["coverage"]["rejected"], 1)
        self.assertEqual(result["issues"][0]["reason"], "costs_complete_invalid")

    def test_signature_mint_side_deduplication_is_reported(self):
        rows = qualifying_rows()
        rows.insert(1, dict(rows[0]))
        other_wallet_fill = dict(rows[0])
        other_wallet_fill["wallet"] = address(103)
        rows.append(other_wallet_fill)
        result = wf.score_wallets(rows, CUTOFF)
        self.assertEqual(result["coverage"]["duplicate"], 1)
        self.assertEqual(result["coverage"]["closed_buy_lots"], 10)
        self.assertEqual(result["coverage"]["accepted"], 21)

    def test_malformed_future_and_negative_values_are_rejected_or_filtered(self):
        rows = qualifying_rows()
        negative = fill("negative", MINTS[6], "buy", "2026-09-30T11:00:00Z", quantity="-1")
        future = fill("future", MINTS[7], "buy", "2026-09-30T12:01:00Z")
        malformed = fill("bad-time", MINTS[8], "buy", "2026-09-30T11:00:00Z")
        malformed["block_time"] = "not-a-date"
        observed_before_block = fill("early-observation", MINTS[9], "buy", "2026-09-30T11:00:00Z",
                                     observed="2026-09-30T10:59:59Z")
        rows.extend((negative, future, malformed, observed_before_block))
        result = wf.score_wallets(rows, CUTOFF)
        self.assertEqual(result["coverage"]["future"], 1)
        self.assertEqual(result["coverage"]["rejected"], 3)
        self.assertEqual(result["coverage"]["closed_buy_lots"], 10)

    def test_follow_signals_enforce_frozen_cutoff_freshness_and_herding(self):
        first = wf.score_wallets(qualifying_rows(WALLET), CUTOFF)
        second_wallet = address(102)
        second = wf.score_wallets(qualifying_rows(second_wallet), CUTOFF)
        frozen = {"as_of": CUTOFF, "qualified": first["qualified"] + second["qualified"]}
        rows = [
            fill("new-1", MINTS[12], "buy", "2026-09-30T12:00:30Z", observed="2026-09-30T12:00:30Z"),
            fill("new-2", MINTS[12], "buy", "2026-09-30T12:00:40Z", observed="2026-09-30T12:00:40Z", wallet=second_wallet),
            fill("stale", MINTS[13], "buy", "2026-09-30T12:00:00Z", observed="2026-09-30T12:00:00Z"),
            fill("at-cutoff", MINTS[14], "buy", "2026-09-30T12:00:00Z", wallet=WALLET),
        ]
        result = wf.following_signals(rows, frozen, "2026-09-30T12:02:30Z", max_age_seconds=120)
        self.assertEqual([s["signature"] for s in result["signals"]], ["new-1", "new-2"])
        self.assertEqual(result["herd_alerts"][0]["mint"], MINTS[12])
        self.assertTrue(all(not s["execution_price_guaranteed"] for s in result["signals"]))

    def test_pending_sell_vetoes_wallet_mint_signal(self):
        ranked = wf.score_wallets(qualifying_rows(), CUTOFF)
        rows = [
            fill("new-buy", MINTS[12], "buy", "2026-09-30T12:00:30Z"),
            fill("later-sell", MINTS[12], "sell", "2026-09-30T12:00:45Z"),
        ]
        result = wf.following_signals(rows, ranked, "2026-09-30T12:01:00Z")
        self.assertEqual(result["signals"], [])
        self.assertEqual(result["issues"][0]["reason"], "vetoed_by_pending_sell")

    def test_historical_page_refetch_cannot_create_fresh_signal(self):
        ranked = wf.score_wallets(qualifying_rows(), CUTOFF)
        rows = [
            fill("pre-cutoff-refetched", MINTS[12], "buy", "2026-09-30T11:59:30Z",
                 observed="2026-09-30T12:00:30Z"),
        ]
        result = wf.following_signals(rows, ranked, "2026-09-30T12:01:00Z")
        self.assertEqual(result["signals"], [])
        future_rank = dict(ranked, as_of="2026-09-30T12:02:00Z")
        with self.assertRaisesRegex(ValueError, "ranking cutoff cannot be after as_of"):
            wf.following_signals([], future_rank, "2026-09-30T12:01:00Z")

    def test_same_page_sell_veto_uses_block_time_order(self):
        ranked = wf.score_wallets(qualifying_rows(), CUTOFF)
        shared_observed_at = "2026-09-30T12:00:50Z"
        rows = [
            fill("sell-before-buy", MINTS[12], "sell", "2026-09-30T12:00:20Z",
                 observed=shared_observed_at),
            fill("buy-after-sell", MINTS[12], "buy", "2026-09-30T12:00:40Z",
                 observed=shared_observed_at),
        ]
        result = wf.following_signals(rows, ranked, "2026-09-30T12:01:00Z")
        self.assertEqual([signal["signature"] for signal in result["signals"]], ["buy-after-sell"])


if __name__ == "__main__":
    unittest.main()
