"""Offline candidate screening and sizing for the first small paper account.

The functions in this module consume caller supplied observations only.  They
do not fetch quotes, create clients, infer a fill, or claim a backtest.  Each
function returns a JSON shaped dictionary so a desk demo can display the same
decision that a later adapter would receive::

    screen_candidate("prediction", observation, as_of="2026-01-01T12:00:00Z")

The observation schemas are deliberately small and documented in the
per-desk functions below.  Missing or unknown controls reject a candidate.
Dollar inputs are Decimal-friendly numbers expressed in dollars; returned
cash fields are integer cents.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from typing import Any, Mapping


PAPER_BUDGET_CENTS = {
    "prediction": 10_000,
    "equity": 30_000,
    "equities": 30_000,
    "options": 30_000,
    "crypto": 10_000,
}
LOSS_LIMIT_CENTS = {
    "prediction": 500,
    "equity": 1_500,
    "equities": 1_500,
    "options": 1_500,
    "crypto": 500,
}
DEFAULT_MAX_QUOTE_AGE = timedelta(minutes=5)
_CENT = Decimal("0.01")


def _result(
    desk: str,
    accepted: bool,
    reasons: list[str],
    *,
    units: Any = 0,
    cost_cents: int = 0,
    loss_cents: int = 0,
    **extra: Any,
) -> dict[str, Any]:
    """Build the stable public result shape used by all screeners."""
    result: dict[str, Any] = {
        "accepted": bool(accepted),
        "scope": "offline_sizing_only",
        "eligible_for_execution": False,
        "status": "accepted" if accepted else "rejected",
        "desk": desk,
        "units": units,
        "cost_cents": int(cost_cents),
        "loss_cents": int(loss_cents),
        "reasons": list(reasons),
    }
    result.update(extra)
    return result


def _decimal(value: Any, field: str, *, minimum: Decimal | None = None) -> Decimal:
    if value is None or isinstance(value, bool):
        raise ValueError(f"{field} must be a finite number")
    try:
        number = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a finite number") from exc
    if not number.is_finite():
        raise ValueError(f"{field} must be a finite number")
    if minimum is not None and number < minimum:
        raise ValueError(f"{field} must be at least {minimum}")
    return number


def _timestamp(value: Any, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{field} must be an ISO timestamp") from exc
    else:
        raise ValueError(f"{field} is required")
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _as_of(value: Any) -> datetime:
    if value is None:
        raise ValueError("as_of is required for deterministic screening")
    return _timestamp(value, "as_of")


def _time_checks(
    observation: Mapping[str, Any],
    fields: tuple[str, ...],
    *,
    as_of: Any,
    max_quote_age: timedelta,
) -> list[str]:
    """Return timestamp failures without raising, so the API always rejects."""
    failures: list[str] = []
    try:
        now = _as_of(as_of)
    except ValueError as exc:
        return [str(exc)]
    for field in fields:
        if field not in observation:
            failures.append(f"missing {field}")
            continue
        try:
            stamped = _timestamp(observation[field], field)
        except ValueError as exc:
            failures.append(str(exc))
            continue
        if stamped > now:
            failures.append(f"{field} is in the future")
        elif now - stamped > max_quote_age:
            failures.append(f"{field} is stale")
    return failures


def _required(observation: Mapping[str, Any], fields: tuple[str, ...]) -> list[str]:
    return [f"missing {field}" for field in fields if field not in observation]


def _cash_cents(dollars: Decimal) -> int:
    """Round cash up so a proposal cannot under-reserve by a fraction of a cent."""
    return int((dollars * 100).to_integral_value(rounding=ROUND_CEILING))


def _proceeds_cents(dollars: Decimal) -> int:
    """Round estimated proceeds down so a quote is never an overstatement."""
    return int((dollars * 100).to_integral_value(rounding=ROUND_FLOOR))


def _floor_units(cash_cents: int, unit_cost_cents: Decimal, *, fractional: bool) -> Decimal:
    if unit_cost_cents <= 0:
        return Decimal("0")
    if fractional:
        return (Decimal(cash_cents) / unit_cost_cents).quantize(Decimal("0.00000001"), rounding=ROUND_FLOOR)
    return (Decimal(cash_cents) / unit_cost_cents).to_integral_value(rounding=ROUND_FLOOR)


def _requested_units(
    observation: Mapping[str, Any],
    maximum: Decimal,
    *,
    fractional: bool,
    field: str = "units",
) -> tuple[Decimal, list[str]]:
    if field not in observation:
        return maximum, []
    try:
        requested = _decimal(observation[field], field, minimum=Decimal("0"))
    except ValueError as exc:
        return Decimal("0"), [str(exc)]
    if requested <= 0:
        return Decimal("0"), [f"{field} must be positive"]
    if not fractional and requested != requested.to_integral_value():
        return Decimal("0"), [f"{field} must be whole units"]
    if requested > maximum:
        return Decimal("0"), [f"{field} exceeds affordability or loss limit"]
    return requested, []


def _limits(desk: str, paper_budget_cents: int | None, loss_limit_cents: int | None) -> tuple[int, int]:
    key = desk.lower()
    if key not in PAPER_BUDGET_CENTS:
        raise ValueError(f"unknown desk {desk!r}")
    budget = PAPER_BUDGET_CENTS[key] if paper_budget_cents is None else paper_budget_cents
    limit = LOSS_LIMIT_CENTS[key] if loss_limit_cents is None else loss_limit_cents
    if isinstance(budget, bool) or not isinstance(budget, int) or isinstance(limit, bool) or not isinstance(limit, int):
        raise ValueError("paper and loss budgets must be integer cents")
    if budget <= 0 or limit <= 0:
        raise ValueError("paper and loss budgets must be positive")
    return budget, limit


def _reject(desk: str, reasons: list[str], **extra: Any) -> dict[str, Any]:
    return _result(desk, False, reasons, **extra)


def screen_equity(
    observation: Mapping[str, Any],
    *,
    as_of: Any,
    max_quote_age: timedelta = DEFAULT_MAX_QUOTE_AGE,
    paper_budget_cents: int | None = None,
    loss_limit_cents: int | None = None,
) -> dict[str, Any]:
    """Screen a long-only equity observation and size it in shares.

    Required fields are ``ask``, ``quote_timestamp``, ``signal_known_at``,
    ``side`` and ``fractional_supported``.  A signal must be known strictly
    before the quote.  The loss budget is the fully funded purchase amount;
    fractional shares are used only when the explicit support flag is true.
    """
    desk = "equity"
    reasons = _required(observation, ("ask", "quote_timestamp", "signal_known_at", "side", "fractional_supported", "fee_cents_per_unit"))
    reasons += _time_checks(observation, ("quote_timestamp",), as_of=as_of, max_quote_age=max_quote_age)
    try:
        now = _as_of(as_of)
    except ValueError:
        now = None
    if "signal_known_at" in observation and now is not None:
        try:
            signal_at = _timestamp(observation["signal_known_at"], "signal_known_at")
            quote_at = _timestamp(observation.get("quote_timestamp"), "quote_timestamp")
            if signal_at >= quote_at:
                reasons.append("signal_known_at must be before quote_timestamp")
            if signal_at > now:
                reasons.append("signal_known_at is in the future")
        except ValueError as exc:
            reasons.append(str(exc))
    if observation.get("side") not in {"buy", "long"}:
        reasons.append("only long equity candidates are supported")
    if not isinstance(observation.get("fractional_supported"), bool):
        reasons.append("fractional_supported must be an explicit boolean")
    try:
        ask = _decimal(observation.get("ask"), "ask", minimum=Decimal("0.00000001"))
        fee_cents = _decimal(observation.get("fee_cents_per_unit", 0), "fee_cents_per_unit", minimum=Decimal("0"))
        budget, loss_limit = _limits(desk, paper_budget_cents, loss_limit_cents)
    except (ValueError, TypeError) as exc:
        reasons.append(str(exc))
        return _reject(desk, reasons)
    if reasons:
        return _reject(desk, reasons)
    fractional = observation["fractional_supported"]
    per_unit_dollars = ask + fee_cents / 100
    per_unit_cents = per_unit_dollars * 100
    maximum = _floor_units(min(budget, loss_limit), per_unit_cents, fractional=fractional)
    units, unit_reasons = _requested_units(observation, maximum, fractional=fractional)
    if unit_reasons:
        return _reject(desk, unit_reasons, max_units=maximum)
    cost = _cash_cents(units * per_unit_dollars)
    if cost <= 0 or cost > budget or cost > loss_limit:
        return _reject(desk, ["candidate is unaffordable within paper or loss budget"], max_units=maximum)
    return _result(desk, True, ["validated long equity quote and prior signal"], units=units, cost_cents=cost, loss_cents=cost, fractional=bool(fractional))


def screen_options(
    observation: Mapping[str, Any],
    *,
    as_of: Any,
    max_quote_age: timedelta = DEFAULT_MAX_QUOTE_AGE,
    paper_budget_cents: int | None = None,
    loss_limit_cents: int | None = None,
) -> dict[str, Any]:
    """Size a caller-labelled debit vertical using whole 100-share contracts.

    Required fields are ``long_ask``, ``short_bid``, ``quote_timestamp`` and
    ``multiplier``.  Fees are ``fees_cents_per_contract`` for the complete
    two-leg spread.  This small arithmetic API does not infer contract
    identity, expiry, option right, orientation, or fill availability; those
    facts must be present in the supplied observation and validated upstream.
    A one-contract debit above the $15 loss budget is rejected, which is
    expected with a $300 account.
    """
    desk = "options"
    reasons = _required(observation, ("long_ask", "short_bid", "quote_timestamp", "multiplier", "fees_cents_per_contract"))
    reasons += _time_checks(observation, ("quote_timestamp",), as_of=as_of, max_quote_age=max_quote_age)
    try:
        long_ask = _decimal(observation.get("long_ask"), "long_ask", minimum=Decimal("0"))
        short_bid = _decimal(observation.get("short_bid"), "short_bid", minimum=Decimal("0"))
        multiplier = _decimal(observation.get("multiplier"), "multiplier", minimum=Decimal("0"))
        fees = _decimal(observation.get("fees_cents_per_contract", 0), "fees_cents_per_contract", minimum=Decimal("0"))
        budget, loss_limit = _limits(desk, paper_budget_cents, loss_limit_cents)
    except (ValueError, TypeError) as exc:
        reasons.append(str(exc))
        return _reject(desk, reasons)
    if multiplier != 100:
        reasons.append("options multiplier must be exactly 100")
    debit = long_ask - short_bid
    if debit <= 0:
        reasons.append("only positive debit verticals are supported")
    if reasons:
        return _reject(desk, reasons)
    per_contract_dollars = debit * multiplier + fees / 100
    per_contract_cents = per_contract_dollars * 100
    maximum = _floor_units(min(budget, loss_limit), per_contract_cents, fractional=False)
    units, unit_reasons = _requested_units(observation, maximum, fractional=False, field="contracts")
    if unit_reasons or units < 1:
        return _reject(desk, unit_reasons or ["one whole option contract exceeds paper or loss budget"], max_contracts=maximum)
    cost = _cash_cents(units * per_contract_dollars)
    if cost > budget or cost > loss_limit:
        return _reject(desk, ["option debit and fees exceed paper or loss budget"], max_contracts=maximum)
    return _result(desk, True, ["affordable supplied debit and fees"], units=units, contracts=units, cost_cents=cost, loss_cents=cost, multiplier=100)


def screen_prediction(
    observation: Mapping[str, Any],
    *,
    as_of: Any,
    max_quote_age: timedelta = DEFAULT_MAX_QUOTE_AGE,
    paper_budget_cents: int | None = None,
    loss_limit_cents: int | None = None,
) -> dict[str, Any]:
    """Screen a prediction-market ask using conservative expected edge.

    Required fields are ``price``, ``probability``, ``payout``,
    ``quote_timestamp`` and non-empty ``settlement_terms``.  ``category``
    should be ``weather`` or ``crypto_event`` (``event`` is also accepted for
    a supplied non-spot event).  A BTC/ETH spot observation is rejected even
    when it is labeled as a prediction candidate.
    """
    desk = "prediction"
    reasons = _required(observation, ("price", "probability", "payout", "quote_timestamp", "settlement_terms", "category", "fee_cents_per_contract"))
    reasons += _time_checks(observation, ("quote_timestamp",), as_of=as_of, max_quote_age=max_quote_age)
    if not isinstance(observation.get("settlement_terms"), str) or not observation.get("settlement_terms", "").strip():
        reasons.append("settlement_terms must be non-empty")
    category = observation.get("category")
    if category not in {"weather", "crypto_event", "event"}:
        reasons.append("unsupported prediction category")
    underlying = str(observation.get("underlying", "")).upper()
    if underlying in {"BTC", "ETH"} and str(observation.get("instrument_type", "")).lower() == "spot":
        reasons.append("BTC/ETH spot is not a prediction event")
    try:
        price = _decimal(observation.get("price"), "price")
        probability = _decimal(observation.get("probability"), "probability")
        payout = _decimal(observation.get("payout"), "payout", minimum=Decimal("0"))
        fee = _decimal(observation.get("fee_cents_per_contract", 0), "fee_cents_per_contract", minimum=Decimal("0")) / 100
        budget, loss_limit = _limits(desk, paper_budget_cents, loss_limit_cents)
    except (ValueError, TypeError) as exc:
        reasons.append(str(exc))
        return _reject(desk, reasons)
    if not (Decimal("0") < price < Decimal("1")):
        reasons.append("prediction price must satisfy 0 < price < 1")
    if not (Decimal("0") <= probability <= Decimal("1")):
        reasons.append("probability must be between 0 and 1")
    if payout != 1:
        reasons.append("v1 prediction payout must be exactly one dollar")
    edge = probability * payout - price - fee
    if edge < Decimal("0.03"):
        reasons.append("net edge after fees is below 0.03")
    if reasons:
        return _reject(desk, reasons, net_edge=edge)
    per_contract_dollars = price + fee
    per_contract_cents = per_contract_dollars * 100
    maximum = _floor_units(min(budget, loss_limit), per_contract_cents, fractional=False)
    units, unit_reasons = _requested_units(observation, maximum, fractional=False, field="contracts")
    if unit_reasons or units < 1:
        return _reject(desk, unit_reasons or ["one whole prediction contract exceeds paper or loss budget"], max_contracts=maximum, net_edge=edge)
    cost = _cash_cents(units * per_contract_dollars)
    if cost > budget or cost > loss_limit:
        return _reject(desk, ["prediction cost exceeds paper or loss budget"], max_contracts=maximum, net_edge=edge)
    return _result(desk, True, ["validated ask, settlement terms, and net edge"], units=units, contracts=units, cost_cents=cost, loss_cents=cost, net_edge=edge)


def _check_bool(observation: Mapping[str, Any], field: str, reasons: list[str]) -> None:
    value = observation.get(field)
    if value is not True:
        reasons.append(f"{field} must be explicitly true")


def screen_crypto(
    observation: Mapping[str, Any],
    *,
    as_of: Any,
    max_quote_age: timedelta = DEFAULT_MAX_QUOTE_AGE,
    paper_budget_cents: int | None = None,
    loss_limit_cents: int | None = None,
) -> dict[str, Any]:
    """Screen a spot token with explicit controls and size-matched exit quote.

    Required controls are boolean ``authority_passed``, ``sellability_passed``,
    ``owner_concentration_passed`` and ``liquidity_passed``.  ``buy_price`` /
    ``buy_size`` and ``exit_price`` / ``exit_size`` are separate quote
    observations; both timestamps must be current and the exit size must cover
    the proposed quantity.  The result is an unfilled paper quote proposal.
    """
    desk = "crypto"
    required = (
        "buy_price", "buy_size", "buy_quote_timestamp", "exit_price", "exit_size", "exit_quote_timestamp",
        "authority_passed", "sellability_passed", "owner_concentration_passed", "liquidity_passed", "fee_bps",
    )
    reasons = _required(observation, required)
    reasons += _time_checks(observation, ("buy_quote_timestamp", "exit_quote_timestamp"), as_of=as_of, max_quote_age=max_quote_age)
    for check in ("authority_passed", "sellability_passed", "owner_concentration_passed", "liquidity_passed"):
        _check_bool(observation, check, reasons)
    try:
        buy_price = _decimal(observation.get("buy_price"), "buy_price", minimum=Decimal("0.0000000001"))
        buy_size = _decimal(observation.get("buy_size"), "buy_size", minimum=Decimal("0"))
        exit_price = _decimal(observation.get("exit_price"), "exit_price", minimum=Decimal("0"))
        exit_size = _decimal(observation.get("exit_size"), "exit_size", minimum=Decimal("0"))
        fee_bps = _decimal(observation.get("fee_bps", 0), "fee_bps", minimum=Decimal("0"))
        budget, loss_limit = _limits(desk, paper_budget_cents, loss_limit_cents)
    except (ValueError, TypeError) as exc:
        reasons.append(str(exc))
        return _reject(desk, reasons)
    if buy_size <= 0 or exit_size <= 0:
        reasons.append("buy and exit quote sizes must be positive")
    if fee_bps > 10_000:
        reasons.append("fee_bps must be at most 10000")
    if exit_price <= 0:
        reasons.append("exit quote must be positive")
    if reasons:
        return _reject(desk, reasons)
    fractional = observation.get("fractional_supported", False)
    if not isinstance(fractional, bool):
        return _reject(desk, ["fractional_supported must be an explicit boolean when supplied"])
    fee_rate = fee_bps / 10_000
    per_unit_dollars = buy_price * (1 + fee_rate)
    per_unit_cents = per_unit_dollars * 100
    maximum = _floor_units(min(budget, loss_limit), per_unit_cents, fractional=fractional)
    units, unit_reasons = _requested_units(observation, maximum, fractional=fractional)
    if unit_reasons:
        return _reject(desk, unit_reasons, max_units=maximum)
    if units <= 0:
        return _reject(desk, ["one token unit exceeds paper or loss budget"], max_units=maximum)
    if units > buy_size or units > exit_size:
        return _reject(desk, ["quote size does not cover the proposed quantity"], max_units=maximum)
    cost = _cash_cents(units * per_unit_dollars)
    if cost > budget or cost > loss_limit:
        return _reject(desk, ["crypto cost including fees exceeds paper or loss budget"], max_units=maximum)
    exit_net_cents = _proceeds_cents(units * exit_price * (1 - fee_rate))
    return _result(
        desk, True, ["all token controls and size-matched quotes passed"], units=units,
        cost_cents=cost, loss_cents=cost, quoted_exit_net_cents=exit_net_cents,
        fill_status="unfilled_quote", fractional=bool(fractional), quoted_only=True,
    )


def screen_candidate(
    desk: str,
    observation: Mapping[str, Any],
    *,
    as_of: Any,
    max_quote_age: timedelta = DEFAULT_MAX_QUOTE_AGE,
    paper_budget_cents: int | None = None,
    loss_limit_cents: int | None = None,
) -> dict[str, Any]:
    """Dispatch one supplied observation to a desk screener."""
    key = desk.lower()
    functions = {"equity": screen_equity, "equities": screen_equity, "options": screen_options, "prediction": screen_prediction, "crypto": screen_crypto}
    if key not in functions:
        return _reject(key, [f"unknown desk {desk!r}"])
    return functions[key](observation, as_of=as_of, max_quote_age=max_quote_age, paper_budget_cents=paper_budget_cents, loss_limit_cents=loss_limit_cents)


__all__ = [
    "LOSS_LIMIT_CENTS", "PAPER_BUDGET_CENTS", "screen_candidate", "screen_crypto",
    "screen_equity", "screen_options", "screen_prediction",
]
