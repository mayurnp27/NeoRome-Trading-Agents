import tempfile
from datetime import datetime, timedelta, timezone
import unittest

from neorome.paper_ledger import PaperLedger
from neorome.prediction_full_port import FULL_PORT_CENTS, initialize
from neorome.prediction_internal_paper import PredictionInternalPaperError, prepare, run
from neorome.prediction_observed_shadow import run_once
from tests.test_prediction_observed_shadow import valid_envelope


UTC = timezone.utc
T0 = datetime(2026, 9, 23, 2, 21, tzinfo=UTC)


def candidate(lower="0.58", settlement_fee="1", units=2):
    envelope = valid_envelope()
    envelope["quote"]["contract_size"] = units
    envelope["adapter"]["events"][0]["asks"][0]["size"] = units
    envelope["adapter"]["events"][0]["bids"][0]["size"] = units
    envelope["fee_schema"]["settlement_fee_cents_per_contract"] = settlement_fee
    envelope["adapter"]["fee"]["settlement_fee_cents_per_contract"] = settlement_fee
    envelope["probability"].update(lower_bound=lower, upper_bound="0.64", confidence_level="0.95")
    result = run_once(envelope, now=T0)
    assert result["status"] == "eligible_shadow_candidate"
    return result["candidate"]


def quote(at, tag, *, ask="0.41", bid="0.40", units=2):
    return {
        "market_id": "KXBTC15M-TEST-30", "outcome_id": "KXBTC15M-TEST-30:YES", "outcome": "yes",
        "fee_version": "fixed-stress-v1", "event_at": (at - timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
        "received_at": at.isoformat().replace("+00:00", "Z"), "source_hash": (tag * 64)[:64],
        "asks": [{"price": ask, "size": units}], "bids": [{"price": bid, "size": units}],
    }


class PredictionInternalPaperTests(unittest.TestCase):
    def book(self, root):
        return initialize(root + "/paper.sqlite3", now=T0)

    def test_admitted_candidate_reserves_fills_marks_closes_and_restarts(self):
        with tempfile.TemporaryDirectory() as root:
            book = self.book(root); state = root + "/state"; value = candidate()
            self.assertGreaterEqual(prepare(value, now=T0)["conservative_edge_cents"], 6)
            first = run(book, state, now=T0, candidate=value, mark_provider=lambda *_: [])
            self.assertEqual(first["actions"][0]["type"], "reserve")
            restarted = self.book(root)
            filled = run(restarted, state, now=T0 + timedelta(seconds=12), later_quote=quote(T0 + timedelta(seconds=12), "b"), mark_provider=lambda *_: [])
            self.assertEqual(filled["actions"][0]["type"], "simulated_fill")
            reopened = self.book(root)
            intent = run(reopened, state, now=T0 + timedelta(seconds=20), later_quote=quote(T0 + timedelta(seconds=20), "c"), request_close=True, mark_provider=lambda *_: [])
            self.assertEqual(intent["blocker"], "awaiting_independent_exit_quote")
            closed = run(self.book(root), state, now=T0 + timedelta(seconds=31), later_quote=quote(T0 + timedelta(seconds=31), "d"), mark_provider=lambda *_: [])
            self.assertEqual(closed["actions"][0]["type"], "close")
            self.assertEqual(closed["broker_orders_sent"], 0)
            result = closed["actions"][0]["result"]
            self.assertEqual(result["proceeds_cents"], 80)
            self.assertEqual(result["fee_cents"], 2)
            self.assertEqual(result["realized_pnl_cents"], -6)
            prediction = next(item for item in self.book(root).balances() if item.desk == "prediction")
            self.assertEqual(prediction.available_cash_cents, 9_994)

    def test_stop_cancels_pending_reservation(self):
        with tempfile.TemporaryDirectory() as root:
            book = self.book(root); state = root + "/state"; value = candidate()
            run(book, state, now=T0, candidate=value, mark_provider=lambda *_: [])
            stopped = run(book, state, now=T0 + timedelta(seconds=1), allow_entry=False, mark_provider=lambda *_: [])
            self.assertEqual(stopped["actions"][0]["type"], "cancel")

    def test_winning_settlement_credits_exact_payout_less_fee(self):
        with tempfile.TemporaryDirectory() as root:
            book = self.book(root); state = root + "/state"; value = candidate()
            run(book, state, now=T0, candidate=value, mark_provider=lambda *_: [])
            run(book, state, now=T0 + timedelta(seconds=14), later_quote=quote(T0 + timedelta(seconds=14), "e"), mark_provider=lambda *_: [])
            settled_at = datetime(2026, 9, 23, 2, 30, tzinfo=UTC)
            settlement = {"market_id": "KXBTC15M-TEST-30", "terms_sha256": value["market_identity"]["terms_sha256"], "finalized": True,
                          "settlement_at": settled_at.isoformat().replace("+00:00", "Z"), "received_at": (settled_at + timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
                          "source_hash": "f" * 64, "result": "yes", "yes_payout_cents": 100}
            report = run(book, state, now=settled_at + timedelta(seconds=2), settlement=settlement, mark_provider=lambda *_: [])
            self.assertEqual(report["actions"][0]["type"], "settlement")
            self.assertEqual(book.snapshot().open_position_cost_cents, 0)
            result = report["actions"][0]["result"]
            self.assertEqual(result["proceeds_cents"], 200)
            self.assertEqual(result["fee_cents"], 2)
            self.assertEqual(result["realized_pnl_cents"], 114)
            prediction = next(item for item in book.balances() if item.desk == "prediction")
            self.assertEqual(prediction.available_cash_cents, 10_114)

    def test_losing_settlement_with_fee_requires_reconciliation_but_zero_fee_is_exact(self):
        with tempfile.TemporaryDirectory() as root:
            book = self.book(root); state = root + "/state"; value = candidate()
            run(book, state, now=T0, candidate=value, mark_provider=lambda *_: [])
            run(book, state, now=T0 + timedelta(seconds=14), later_quote=quote(T0 + timedelta(seconds=14), "e"), mark_provider=lambda *_: [])
            settled_at = datetime(2026, 9, 23, 2, 30, tzinfo=UTC)
            settlement = {"market_id": "KXBTC15M-TEST-30", "terms_sha256": value["market_identity"]["terms_sha256"], "finalized": True,
                          "settlement_at": settled_at.isoformat().replace("+00:00", "Z"), "received_at": (settled_at + timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
                          "source_hash": "f" * 64, "result": "no", "yes_payout_cents": 0}
            with self.assertRaisesRegex(PredictionInternalPaperError, "settlement fee liability"):
                run(book, state, now=settled_at + timedelta(seconds=2), settlement=settlement, mark_provider=lambda *_: [])
            self.assertEqual(book.snapshot().open_position_cost_cents, 84)

        with tempfile.TemporaryDirectory() as root:
            book = self.book(root); state = root + "/state"; value = candidate(settlement_fee="0")
            run(book, state, now=T0, candidate=value, mark_provider=lambda *_: [])
            run(book, state, now=T0 + timedelta(seconds=14), later_quote=quote(T0 + timedelta(seconds=14), "e"), mark_provider=lambda *_: [])
            settlement["terms_sha256"] = value["market_identity"]["terms_sha256"]
            report = run(book, state, now=settled_at + timedelta(seconds=2), settlement=settlement, mark_provider=lambda *_: [])
            result = report["actions"][0]["result"]
            self.assertEqual(result["proceeds_cents"], 0)
            self.assertEqual(result["fee_cents"], 0)
            self.assertEqual(result["realized_pnl_cents"], -84)
            prediction = next(item for item in book.balances() if item.desk == "prediction")
            self.assertEqual(prediction.available_cash_cents, 9_916)

    def test_missing_bounds_refuse_reservation(self):
        shadow = run_once(valid_envelope(), now=T0)["candidate"]
        with self.assertRaisesRegex(ValueError, "conservative probability bounds"):
            prepare(shadow, now=T0)

    def test_full_port_can_reserve_a_single_idea_up_to_the_exact_100_dollar_sleeve(self):
        with tempfile.TemporaryDirectory() as root:
            # 227 contracts at 42c plus 1c entry/exit fees is a conservative
            # 9,988c worst case, while preserving integer depth and fee terms.
            book = self.book(root); state = root + "/state"; value = candidate(units=227)
            prepared = prepare(value, now=T0)
            self.assertEqual(prepared["conservative_worst_case_cents"], 9_988)
            reserved = run(book, state, now=T0, candidate=value, mark_provider=lambda *_: [])
            reservation = reserved["actions"][0]["result"]
            self.assertEqual(reservation["reserved_cents"], 9_761)
            self.assertLessEqual(prepared["conservative_worst_case_cents"], FULL_PORT_CENTS)
            balance = next(item for item in book.balances() if item.desk == "prediction")
            self.assertEqual(balance.initial_cash_cents, FULL_PORT_CENTS)
            self.assertEqual(balance.available_cash_cents + balance.reserved_cash_cents, FULL_PORT_CENTS)

    def test_shared_ledger_is_not_a_full_port_prediction_account(self):
        with tempfile.TemporaryDirectory() as root:
            shared = PaperLedger(root + "/shared.sqlite3", enforce_market_risk=True, clock=lambda: T0)
            with self.assertRaisesRegex(PredictionInternalPaperError, "full-port"):
                run(shared, root + "/state", now=T0, candidate=candidate(), mark_provider=lambda *_: [])


if __name__ == "__main__":
    unittest.main()
