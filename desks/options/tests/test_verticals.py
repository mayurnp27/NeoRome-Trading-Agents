"""Unit tests for the offline vertical validator and expiration harness."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
import subprocess
import sys
import unittest

from desks.options.evaluate import evaluate_case, evaluate_expiration, load_fixture
from desks.options.verticals import OptionLeg, VerticalValidationError, validate_vertical


AS_OF = datetime(2026, 1, 5, 15, tzinfo=timezone.utc)
EXPIRY = "2026-01-16"
QUOTE_TIME = "2026-01-05T14:59:00Z"


def occ(strike: int, right: str = "C", root: str = "AAPL", date_code: str = "260116") -> str:
    return f"{root:<6}{date_code}{right}{strike * 1000:08d}"


def leg(strike: int, right: str, side: str, bid: str, ask: str, **overrides: object) -> OptionLeg:
    values: dict[str, object] = dict(
        symbol=occ(strike, right), underlying="AAPL", right=right, expiry=EXPIRY,
        strike=str(strike), side=side, quantity=2, bid=bid, ask=ask,
        timestamp=QUOTE_TIME,
    )
    values.update(overrides)
    return OptionLeg(**values)  # type: ignore[arg-type]


class VerticalValidatorTests(unittest.TestCase):
    def test_four_vertical_orientations_and_risk(self) -> None:
        cases = (
            ([leg(150, "C", "buy", "3.00", "3.20"), leg(155, "C", "sell", "0.00", "0.20")], "bull_call_debit", Decimal("640")),
            ([leg(155, "C", "buy", "0.00", "0.20"), leg(150, "C", "sell", "3.00", "3.20")], "bear_call_credit", Decimal("440")),
            ([leg(150, "P", "buy", "0.00", "0.20"), leg(155, "P", "sell", "3.00", "3.20")], "bull_put_credit", Decimal("440")),
            ([leg(155, "P", "buy", "3.00", "3.20"), leg(150, "P", "sell", "0.00", "0.20")], "bear_put_debit", Decimal("640")),
        )
        for legs, expected_kind, expected_max_loss in cases:
            with self.subTest(expected_kind=expected_kind):
                result = validate_vertical(legs, as_of=AS_OF)
                self.assertEqual(result.kind, expected_kind)
                self.assertEqual(result.quantity, 2)
                self.assertEqual(result.max_loss, expected_max_loss)

    def test_conservative_signed_net_premium(self) -> None:
        result = validate_vertical(
            [leg(150, "C", "buy", "3.00", "3.20"), leg(155, "C", "sell", "0.00", "0.20")],
            as_of=AS_OF,
        )
        self.assertEqual(result.signed_net_debit, Decimal("3.20"))
        self.assertEqual(result.max_loss_per_share, Decimal("3.20"))

        result = validate_vertical(
            [leg(150, "P", "buy", "0.00", "0.20"), leg(155, "P", "sell", "3.00", "3.20")],
            as_of=AS_OF,
        )
        self.assertEqual(result.signed_net_debit, Decimal("-2.80"))
        self.assertEqual(result.net_credit, Decimal("2.80"))
        self.assertEqual(result.max_loss_per_share, Decimal("2.20"))

    def test_invalid_legs_and_metadata_are_rejected(self) -> None:
        valid = [leg(150, "C", "buy", "3.00", "3.20"), leg(155, "C", "sell", "0.00", "0.20")]
        invalid_sets = [
            valid[:1],
            [leg(150, "C", "buy", "3.00", "3.20"), leg(155, "P", "sell", "0.00", "0.20")],
            [leg(150, "C", "buy", "3.00", "3.20"), leg(150, "C", "sell", "0.00", "0.20")],
            [leg(150, "C", "buy", "3.00", "3.20", quantity=1), leg(155, "C", "sell", "0.00", "0.20")],
            [leg(150, "C", "buy", "3.00", "3.20", multiplier=10), leg(155, "C", "sell", "0.00", "0.20")],
            [leg(150, "C", "buy", "3.00", "3.20", adjusted=True), leg(155, "C", "sell", "0.00", "0.20")],
            [leg(150, "C", "buy", "3.00", "3.20", symbol=occ(151)), leg(155, "C", "sell", "0.00", "0.20")],
        ]
        for legs in invalid_sets:
            with self.subTest(legs=legs):
                with self.assertRaises(VerticalValidationError):
                    validate_vertical(legs, as_of=AS_OF)

    def test_mapping_must_declare_contract_adjustment_metadata(self) -> None:
        payload = vars(leg(150, "C", "buy", "3.00", "3.20"))
        for key in ("multiplier", "adjusted"):
            missing = dict(payload)
            del missing[key]
            with self.subTest(missing=key), self.assertRaises(VerticalValidationError):
                validate_vertical([missing, vars(leg(155, "C", "sell", "0.00", "0.20"))], as_of=AS_OF)
        malformed = dict(payload, adjusted="false")
        with self.assertRaises(VerticalValidationError):
            validate_vertical([malformed, vars(leg(155, "C", "sell", "0.00", "0.20"))], as_of=AS_OF)

    def test_missing_stale_future_and_crossed_quotes_are_rejected(self) -> None:
        base = [leg(150, "C", "buy", "3.00", "3.20"), leg(155, "C", "sell", "0.00", "0.20")]
        for changes in (
            {"bid": None},
            {"timestamp": None},
            {"timestamp": "2026-01-05T14:00:00Z"},
            {"timestamp": "2026-01-05T15:00:01Z"},
            {"bid": "3.30", "ask": "3.20"},
            {"bid": "NaN"},
        ):
            legs = [OptionLeg(**vars(item)) for item in base]
            legs[0] = OptionLeg(**{**vars(legs[0]), **changes})
            with self.subTest(changes=changes):
                with self.assertRaises(VerticalValidationError):
                    validate_vertical(legs, as_of=AS_OF, max_quote_age=timedelta(minutes=5))

        for buy_ask, sell_bid in (("3.20", "3.20"), ("5.20", "0.20")):
            with self.subTest(buy_ask=buy_ask, sell_bid=sell_bid), self.assertRaises(VerticalValidationError):
                validate_vertical(
                    [leg(150, "C", "buy", "3.00", buy_ask), leg(155, "C", "sell", sell_bid, "0.40")],
                    as_of=AS_OF,
                )

    def test_budget_costs_reserved_risk_and_quantity_cap(self) -> None:
        legs = [leg(150, "C", "buy", "3.00", "3.20"), leg(155, "C", "sell", "0.00", "0.20")]
        with self.assertRaises(VerticalValidationError):
            validate_vertical(legs, as_of=AS_OF, max_loss_budget="500", existing_reserved_risk="130", fee_per_contract="10", slippage_per_contract="5")
        with self.assertRaises(VerticalValidationError):
            validate_vertical(legs, as_of=AS_OF, max_quantity=1)
        result = validate_vertical(legs, as_of=AS_OF, max_loss_budget="800", existing_reserved_risk="100", fee_per_contract="10", slippage_per_contract="5")
        self.assertEqual(result.risk_including_costs, Decimal("670"))
        self.assertEqual(result.portfolio_risk, Decimal("770"))

    def test_individually_valid_legs_must_share_underlying_and_expiry(self) -> None:
        first = leg(150, "C", "buy", "3.00", "3.20")
        different_underlying = leg(155, "C", "sell", "0.00", "0.20", symbol=occ(155, "C", "MSFT"), underlying="MSFT")
        different_expiry = leg(155, "C", "sell", "0.00", "0.20", symbol=occ(155, "C", "AAPL", "260123"), expiry="2026-01-23")
        for other in (different_underlying, different_expiry):
            with self.subTest(other=other):
                with self.assertRaises(VerticalValidationError):
                    validate_vertical([first, other], as_of=AS_OF)

    def test_expiration_harness_and_synthetic_fixture(self) -> None:
        fixture = load_fixture()
        self.assertEqual(fixture["fixture_type"], "synthetic")
        for case in fixture["cases"]:
            result = evaluate_case(case, as_of=fixture["as_of"], max_quote_age=timedelta(minutes=5))
            self.assertEqual(result.max_loss, result.risk_including_costs)
        with self.assertRaises(VerticalValidationError):
            evaluate_case(fixture["cases"][0], as_of=fixture["as_of"], max_quote_age=timedelta(minutes=5), max_loss_budget="100")
        vertical = validate_vertical(
            [leg(150, "C", "buy", "3.00", "3.20"), leg(155, "C", "sell", "0.00", "0.20")], as_of=AS_OF
        )
        self.assertEqual(evaluate_expiration(vertical, "150").expiration_pnl, Decimal("-640.00"))
        self.assertEqual(evaluate_expiration(vertical, "160").expiration_pnl, Decimal("360.00"))

    def test_extreme_expiration_payoffs_reach_declared_max_loss_and_reversal_is_invariant(self) -> None:
        fixture = load_fixture()
        for case in fixture["cases"]:
            vertical = validate_vertical(case["legs"], as_of=fixture["as_of"])
            extreme_spot = "0" if vertical.kind in {"bull_call_debit", "bull_put_credit"} else "1000"
            result = evaluate_expiration(vertical, extreme_spot)
            self.assertEqual(result.expiration_pnl, -vertical.max_loss)
            reversed_vertical = validate_vertical(list(reversed(case["legs"])), as_of=fixture["as_of"])
            self.assertEqual(reversed_vertical.kind, vertical.kind)
            self.assertEqual(reversed_vertical.signed_net_debit, vertical.signed_net_debit)
            self.assertEqual(reversed_vertical.max_loss, vertical.max_loss)

    def test_cli_is_runnable(self) -> None:
        completed = subprocess.run([sys.executable, "-m", "desks.options.evaluate", "--case", "bear_put_debit"], capture_output=True, text=True, check=True)
        self.assertIn("bear_put_debit", completed.stdout)

    def test_quote_and_contract_boundaries_fail_closed(self) -> None:
        base = [leg(150, "C", "buy", "3", "3.2"), leg(155, "C", "sell", "1", "1.2")]
        for changes in (
            {"right": None}, {"quantity": True}, {"quantity": 1.5},
            {"bid": "0", "ask": "0"}, {"ask": None},
            {"symbol": "AAPL1 260116C00150000"},
            {"expiry": "2026-01-16junk"},
            {"timestamp": "2026-01-05T15:00:00"},
        ):
            with self.subTest(changes=changes), self.assertRaises(VerticalValidationError):
                validate_vertical([OptionLeg(**{**vars(base[0]), **changes}), base[1]], as_of=AS_OF)
        for controls in ({"max_quote_age": 300}, {"max_quote_age": timedelta(seconds=-1)},
                         {"max_loss_budget": "NaN"}, {"fee_per_contract": "-1"}):
            with self.subTest(controls=controls), self.assertRaises(VerticalValidationError):
                validate_vertical(base, as_of=AS_OF, **controls)
        with self.assertRaises(VerticalValidationError):
            validate_vertical(None, as_of=AS_OF)
        for extra in ({"position_intent": "buy_to_close"}, {"ratio_qty": "2"}):
            with self.subTest(extra=extra), self.assertRaises(VerticalValidationError):
                validate_vertical([{**vars(base[0]), **extra}, vars(base[1])], as_of=AS_OF)
        expiry_day = datetime(2026, 1, 16, 15, tzinfo=timezone.utc)
        fresh = [OptionLeg(**{**vars(item), "timestamp": expiry_day}) for item in base]
        with self.assertRaisesRegex(VerticalValidationError, "expiration"):
            validate_vertical(fresh, as_of=expiry_day)

    def test_cli_honors_budget_override(self) -> None:
        completed = subprocess.run(
            [sys.executable, "-m", "desks.options.evaluate", "--max-loss-budget", "1"],
            capture_output=True, text=True,
        )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("max_loss_budget", completed.stderr)


if __name__ == "__main__":
    unittest.main()
