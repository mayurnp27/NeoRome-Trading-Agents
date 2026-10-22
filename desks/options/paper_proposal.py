"""Connect quote/risk validation to an offline Alpaca vertical preview.

This module never constructs a client or submits an order. Operational callers
must revalidate current quotes and reserve shared risk before any later send.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Sequence

from neorome.alpaca_paper import MultiLegOrderRequest, OptionLeg as WireLeg
from .verticals import ValidatedVertical, validate_vertical, VerticalValidationError


def prepare_paper_vertical(
    legs: Sequence[Any], *, as_of: datetime, client_order_id: str,
    max_loss_budget: Any, max_quantity: int,
    max_quote_age: timedelta = timedelta(minutes=5),
    existing_reserved_risk: Any = "0", fee_per_contract: Any = "0",
    slippage_per_contract: Any = "0",
) -> tuple[ValidatedVertical, MultiLegOrderRequest]:
    """Return reviewed risk figures and a typed request without any I/O.

Budget and quantity cap are required here even though the low-level arithmetic
validator can be used without them. Costs are dollars per spread (both legs),
not quote-price points. The caller supplies the complete intended cost reserve.
"""
    if max_loss_budget is None or max_quantity is None:
        raise VerticalValidationError("paper proposals require a loss budget and quantity cap")
    result = validate_vertical(
        legs, as_of=as_of, max_loss_budget=max_loss_budget,
        max_quantity=max_quantity, max_quote_age=max_quote_age,
        existing_reserved_risk=existing_reserved_risk,
        fee_per_contract=fee_per_contract,
        slippage_per_contract=slippage_per_contract,
    )
    request = MultiLegOrderRequest(
        legs=tuple(WireLeg(
            symbol=leg.symbol.replace(" ", ""), ratio_qty="1",
            side=leg.side.lower(), position_intent=leg.side.lower() + "_to_open",
        ) for leg in result.legs),
        qty=result.quantity, limit_price=result.signed_net_debit,
        client_order_id=client_order_id,
    )
    request.validate()
    return result, request


def main() -> int:
    """Preview a synthetic fixture case; intentionally has no submit flag."""
    import argparse
    import json
    from .evaluate import load_fixture, DEFAULT_FIXTURE

    parser = argparse.ArgumentParser(description="Offline synthetic vertical payload preview")
    parser.add_argument("--fixture", default=str(DEFAULT_FIXTURE))
    parser.add_argument("--case", default="bull_call_debit")
    parser.add_argument("--max-loss-budget", required=True)
    parser.add_argument("--max-quantity", required=True, type=int)
    parser.add_argument("--existing-reserved-risk", default="0")
    parser.add_argument("--fee-per-contract", default="0")
    parser.add_argument("--slippage-per-contract", default="0")
    args = parser.parse_args()
    fixture = load_fixture(args.fixture)
    cases = [case for case in fixture["cases"] if case["name"] == args.case]
    if len(cases) != 1:
        parser.error("case must identify exactly one synthetic case")
    risk, request = prepare_paper_vertical(
        cases[0]["legs"], as_of=fixture["as_of"],
        client_order_id="options-synthetic-preview-001",
        max_loss_budget=args.max_loss_budget, max_quantity=args.max_quantity,
        max_quote_age=timedelta(seconds=fixture.get("max_quote_age_seconds", 300)),
        existing_reserved_risk=args.existing_reserved_risk,
        fee_per_contract=args.fee_per_contract,
        slippage_per_contract=args.slippage_per_contract,
    )
    print(json.dumps({
        "fixture_type": "synthetic", "dry_run": True,
        "notice": "Offline example only; no fill or performance claim. ID is for preview only.",
        "risk_including_costs": str(risk.risk_including_costs),
        "portfolio_risk": str(risk.portfolio_risk), "payload": request.payload(),
    }, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
