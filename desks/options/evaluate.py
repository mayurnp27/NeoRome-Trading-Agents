"""Deterministic expiration payoff harness for validated options verticals.

The harness is an offline scenario calculator, not a fill simulator or a
backtest. It uses the validator's conservative bid/ask entries and intrinsic
value at expiration. It does not model path dependent events, early
exercise/assignment, dividends, borrow, settlement, or execution likelihood.
Run the included synthetic fixture with ``python -m desks.options.evaluate``.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
from typing import Any, Mapping

from .verticals import (
    OptionLeg,
    ValidatedVertical,
    VerticalValidationError,
    validate_vertical,
)


DEFAULT_FIXTURE = Path(__file__).with_name("fixtures") / "synthetic_verticals.json"


def _decimal(value: Any, field: str = "value") -> Decimal:
    if isinstance(value, bool) or value is None:
        raise VerticalValidationError(f"{field} must be a finite Decimal number")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise VerticalValidationError(f"{field} must be a finite Decimal number") from exc
    if not result.is_finite():
        raise VerticalValidationError(f"{field} must be a finite Decimal number")
    return result


def _intrinsic(right: str, strike: Decimal, spot: Decimal) -> Decimal:
    if right.upper().startswith("C"):
        return max(Decimal("0"), spot - strike)
    return max(Decimal("0"), strike - spot)


@dataclass(frozen=True)
class ExpirationResult:
    """Payoff at one expiration spot for a validated opening vertical."""

    kind: str
    spot: Decimal
    expiration_pnl: Decimal
    expiration_pnl_per_spread: Decimal
    max_loss: Decimal
    risk_including_costs: Decimal


def evaluate_expiration(vertical: ValidatedVertical, spot: Decimal | str | int | float) -> ExpirationResult:
    """Calculate expiration P&L from intrinsic values and conservative entries."""
    spot_decimal = _decimal(spot, "spot")
    if spot_decimal < 0:
        raise VerticalValidationError("spot must be non-negative")
    pnl_per_share = Decimal("0")
    for leg, entry in zip(vertical.legs, vertical.entry_prices):
        strike = _decimal(leg.strike, "strike")
        intrinsic = _intrinsic(leg.right, strike, spot_decimal)
        signed_position = Decimal("1") if leg.side.lower() == "buy" else Decimal("-1")
        pnl_per_share += signed_position * (intrinsic - entry)
    pnl = pnl_per_share * 100 * vertical.quantity - vertical.fees_and_slippage
    return ExpirationResult(
        kind=vertical.kind,
        spot=spot_decimal,
        expiration_pnl=pnl,
        expiration_pnl_per_spread=pnl_per_share * 100 - vertical.fees_and_slippage / vertical.quantity,
        max_loss=vertical.max_loss,
        risk_including_costs=vertical.risk_including_costs,
    )


def load_fixture(path: str | Path = DEFAULT_FIXTURE) -> Mapping[str, Any]:
    """Load a labeled synthetic JSON fixture from disk."""
    fixture_path = Path(path)
    with fixture_path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, Mapping) or payload.get("fixture_type") != "synthetic":
        raise ValueError("evaluation fixtures must be explicitly labeled synthetic")
    return payload


def evaluate_case(
    case: Mapping[str, Any],
    *,
    as_of: Any,
    max_quote_age: timedelta = timedelta(minutes=5),
    **controls: Any,
) -> ExpirationResult:
    """Validate and evaluate one JSON-shaped scenario case."""
    vertical = validate_vertical(
        [OptionLeg.from_dict(leg) for leg in case["legs"]],
        as_of=as_of,
        max_quote_age=max_quote_age,
        **controls,
    )
    return evaluate_expiration(vertical, case["spot"])


def _main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate synthetic options vertical scenarios at expiration")
    parser.add_argument("fixture", nargs="?", default=str(DEFAULT_FIXTURE), help="synthetic JSON fixture")
    parser.add_argument("--case", dest="case_name", help="case name (default: all cases)")
    parser.add_argument("--spot", type=Decimal, help="override the selected case expiration spot")
    parser.add_argument("--max-loss-budget", type=Decimal, help="override the fixture risk budget")
    parser.add_argument("--max-quantity", type=int, help="override the fixture quantity cap")
    args = parser.parse_args()
    fixture = load_fixture(args.fixture)
    cases = fixture.get("cases", [])
    if args.case_name:
        cases = [case for case in cases if case.get("name") == args.case_name]
        if not cases:
            parser.error(f"unknown case {args.case_name!r}")
    max_age = timedelta(seconds=int(fixture.get("max_quote_age_seconds", 300)))
    controls = dict(fixture.get("controls", {}))
    if args.max_loss_budget is not None:
        controls["max_loss_budget"] = str(args.max_loss_budget)
    if args.max_quantity is not None:
        controls["max_quantity"] = args.max_quantity
    print("SYNTHETIC SCENARIO ONLY (offline expiration payoff; no orders or fills)")
    for case in cases:
        scenario = dict(case)
        if args.spot is not None:
            scenario["spot"] = str(args.spot)
        result = evaluate_case(scenario, as_of=fixture["as_of"], max_quote_age=max_age, **controls)
        print(
            f"{case.get('name', result.kind)}: kind={result.kind} "
            f"spot={result.spot} expiration_pnl={result.expiration_pnl} "
            f"max_loss={result.max_loss}"
        )
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised by the CLI smoke check.
    raise SystemExit(_main())
