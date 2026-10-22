"""Offline validation for defined-risk, two-leg options verticals.

This module deliberately models an opening order proposal only.  It does not
query a broker, account for assignment/exercise, model early exercise,
corporate actions, settlement conventions, taxes, or transmit orders.  Quotes
are supplied by the caller and are used conservatively: buys use the ask and
sells use the bid.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import re
from typing import Any, Mapping, Sequence


UTC = timezone.utc
ZERO = Decimal("0")
STANDARD_MULTIPLIER = 100
_OCC_RE = re.compile(r"^(?P<root>[A-Z]{1,6})(?P<date>\d{6})(?P<right>[CP])(?P<strike>\d{8})$")
_OCC_PADDED_RE = re.compile(r"^(?P<root>[A-Z]{1,6}) {0,5}(?P<date>\d{6})(?P<right>[CP])(?P<strike>\d{8})$")


class VerticalValidationError(ValueError):
    """Raised when an opening vertical cannot be safely validated."""


ValidationError = VerticalValidationError


def _decimal(value: Any, field: str) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise VerticalValidationError(f"{field} must be a finite Decimal number")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise VerticalValidationError(f"{field} must be a finite Decimal number") from exc
    if not result.is_finite():
        raise VerticalValidationError(f"{field} must be a finite Decimal number")
    return result


def _date(value: Any, field: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise VerticalValidationError(f"{field} must be an ISO date") from exc
    raise VerticalValidationError(f"{field} must be an ISO date")


def _aware_datetime(value: Any, field: str) -> datetime:
    if isinstance(value, str):
        candidate = value.replace("Z", "+00:00")
        try:
            value = datetime.fromisoformat(candidate)
        except ValueError as exc:
            raise VerticalValidationError(f"{field} must be timezone-aware") from exc
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise VerticalValidationError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def parse_occ_symbol(symbol: str) -> tuple[str, date, str, Decimal]:
    """Parse a canonical OCC symbol or compact broker representation."""

    if not isinstance(symbol, str):
        raise VerticalValidationError("symbol must be a canonical OCC symbol")
    match = _OCC_PADDED_RE.fullmatch(symbol) or _OCC_RE.fullmatch(symbol)
    if not match:
        raise VerticalValidationError(f"symbol {symbol!r} is not a canonical OCC symbol")
    # A padded OCC root is exactly six characters (letters followed by
    # trailing spaces); a compact broker symbol has no spaces at all.
    root_field = symbol[: symbol.find(match.group("date"))]
    if " " in root_field and len(root_field) != 6:
        raise VerticalValidationError(f"symbol {symbol!r} has malformed root padding")
    try:
        encoded = match.group("date")
        expiry = date(2000 + int(encoded[:2]), int(encoded[2:4]), int(encoded[4:6]))
    except ValueError as exc:
        raise VerticalValidationError(f"symbol {symbol!r} has an invalid expiration") from exc
    strike = _decimal(Decimal(match.group("strike")) / Decimal(1000), "symbol strike")
    return match.group("root").strip().upper(), expiry, match.group("right"), strike


@dataclass(frozen=True)
class Quote:
    """A two-sided quote with the timestamp at which both sides were observed."""

    bid: Decimal | str | int | float | None
    ask: Decimal | str | int | float | None
    timestamp: datetime | str | None


@dataclass(frozen=True)
class OptionLeg:
    """One opening option leg and its caller-supplied quote metadata."""

    symbol: str
    underlying: str
    right: str
    expiry: date | datetime | str
    strike: Decimal | str | int | float
    side: str
    quantity: int
    bid: Decimal | str | int | float | None = None
    ask: Decimal | str | int | float | None = None
    timestamp: datetime | str | None = None
    multiplier: int = STANDARD_MULTIPLIER
    adjusted: bool = False
    quote_timestamp: datetime | str | None = None

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "OptionLeg":
        """Build a leg from fixture/API-shaped JSON, including nested quotes."""

        if not isinstance(value, Mapping):
            raise VerticalValidationError("each leg must be an object")
        quote = value.get("quote")
        quote = quote if isinstance(quote, Mapping) else {}
        if "multiplier" not in value:
            raise VerticalValidationError("mapping leg must declare multiplier")
        if "adjusted" not in value and "is_adjusted" not in value:
            raise VerticalValidationError("mapping leg must declare adjusted metadata")
        if "ratio_qty" in value and _decimal(value["ratio_qty"], "ratio_qty") != 1:
            raise VerticalValidationError("vertical leg ratio_qty must be 1")
        if "position_intent" in value:
            side = value.get("side", value.get("action"))
            if not isinstance(side, str) or value["position_intent"] != side.lower() + "_to_open":
                raise VerticalValidationError("vertical proposals require matching opening intent")
        def get(name: str, *aliases: str, default: Any = None) -> Any:
            for key in (name, *aliases):
                if key in value:
                    return value[key]
            return default
        return cls(
            symbol=get("symbol", "occ_symbol"),
            underlying=get("underlying", "underlying_symbol"),
            right=get("right", "option_type"),
            expiry=get("expiry", "expiration"),
            strike=get("strike"),
            side=get("side", "action"),
            quantity=get("quantity", "qty"),
            bid=get("bid", default=quote.get("bid")),
            ask=get("ask", default=quote.get("ask")),
            timestamp=get("timestamp", "quote_timestamp", default=quote.get("timestamp")),
            multiplier=get("multiplier", default=STANDARD_MULTIPLIER),
            adjusted=get("adjusted", "is_adjusted", default=False),
        )


@dataclass(frozen=True)
class ValidationConfig:
    """Risk and quote controls for :func:`validate_vertical`.

    Fees and slippage are currency amounts per spread contract.  Existing
    reserved risk is a currency amount for the surrounding portfolio and is
    included only in the budget comparison.
    """

    as_of: datetime
    max_quote_age: timedelta = timedelta(minutes=5)
    max_loss_budget: Decimal | str | int | float | None = None
    existing_reserved_risk: Decimal | str | int | float = ZERO
    fee_per_contract: Decimal | str | int | float = ZERO
    slippage_per_contract: Decimal | str | int | float = ZERO
    max_quantity: int | None = None


@dataclass(frozen=True)
class ValidatedVertical:
    """Validated terms and conservative risk figures for one opening vertical."""

    legs: tuple[OptionLeg, OptionLeg]
    kind: str
    quantity: int
    width: Decimal
    signed_net_debit: Decimal
    net_debit: Decimal
    net_credit: Decimal
    max_loss_per_share: Decimal
    max_loss: Decimal
    fees_and_slippage: Decimal
    risk_including_costs: Decimal
    existing_reserved_risk: Decimal
    portfolio_risk: Decimal
    as_of: datetime

    @property
    def entry_prices(self) -> tuple[Decimal, Decimal]:
        """Conservative opening prices, in the same order as ``legs``."""
        return tuple(
            _decimal(leg.ask if leg.side.lower() == "buy" else leg.bid, "entry price")
            for leg in self.legs
        )  # type: ignore[return-value]


def _validate_controls(config: ValidationConfig) -> tuple[datetime, Decimal | None, Decimal, Decimal, Decimal, int | None]:
    as_of = _aware_datetime(config.as_of, "as_of")
    if not isinstance(config.max_quote_age, timedelta):
        raise VerticalValidationError("max_quote_age must be a timedelta")
    if config.max_quote_age.total_seconds() < 0:
        raise VerticalValidationError("max_quote_age must be non-negative")
    budget = None if config.max_loss_budget is None else _decimal(config.max_loss_budget, "max_loss_budget")
    reserved = _decimal(config.existing_reserved_risk, "existing_reserved_risk")
    fees = _decimal(config.fee_per_contract, "fee_per_contract")
    slippage = _decimal(config.slippage_per_contract, "slippage_per_contract")
    for name, value in (("max_loss_budget", budget), ("existing_reserved_risk", reserved), ("fee_per_contract", fees), ("slippage_per_contract", slippage)):
        if value is not None and value < ZERO:
            raise VerticalValidationError(f"{name} must be non-negative")
    cap = config.max_quantity
    if cap is not None and (isinstance(cap, bool) or not isinstance(cap, int) or cap <= 0):
        raise VerticalValidationError("max_quantity must be a positive integer")
    return as_of, budget, reserved, fees, slippage, cap


def validate_vertical(
    legs: Sequence[OptionLeg | Mapping[str, Any]],
    *,
    as_of: datetime,
    max_quote_age: timedelta = timedelta(minutes=5),
    max_loss_budget: Decimal | str | int | float | None = None,
    existing_reserved_risk: Decimal | str | int | float = ZERO,
    fee_per_contract: Decimal | str | int | float = ZERO,
    slippage_per_contract: Decimal | str | int | float = ZERO,
    max_quantity: int | None = None,
) -> ValidatedVertical:
    """Validate a same-expiry, 1:1 defined-risk opening vertical.

    The returned risk is conservative and deterministic, but represents only
    expiration payoff and the supplied quote snapshot.  It cannot predict
    fills, assignment, liquidity, dividends, early exercise, or settlement.
    """

    config = ValidationConfig(
        as_of=as_of,
        max_quote_age=max_quote_age,
        max_loss_budget=max_loss_budget,
        existing_reserved_risk=existing_reserved_risk,
        fee_per_contract=fee_per_contract,
        slippage_per_contract=slippage_per_contract,
        max_quantity=max_quantity,
    )
    as_of_utc, budget, reserved, fees, slippage, cap = _validate_controls(config)
    if not isinstance(legs, (list, tuple)) or len(legs) != 2:
        raise VerticalValidationError("a vertical requires exactly two legs")
    normalized = tuple(leg if isinstance(leg, OptionLeg) else OptionLeg.from_dict(leg) for leg in legs)
    for index, leg in enumerate(normalized, 1):
        if not isinstance(leg.symbol, str) or not leg.symbol:
            raise VerticalValidationError(f"leg {index} has no symbol")
        if isinstance(leg.quantity, bool) or not isinstance(leg.quantity, int) or leg.quantity <= 0:
            raise VerticalValidationError(f"leg {index} quantity must be a positive integer")
        if isinstance(leg.multiplier, bool) or not isinstance(leg.multiplier, int) or leg.multiplier != STANDARD_MULTIPLIER:
            raise VerticalValidationError("only unadjusted standard 100-share contracts are supported")
        if not isinstance(leg.adjusted, bool) or leg.adjusted:
            raise VerticalValidationError("only unadjusted standard 100-share contracts are supported")
        if not isinstance(leg.side, str) or leg.side.lower() not in {"buy", "sell"}:
            raise VerticalValidationError("leg side must be buy or sell")
        if not isinstance(leg.right, str) or leg.right.upper() not in {"C", "P", "CALL", "PUT"}:
            raise VerticalValidationError("leg right must be call or put")
        if not isinstance(leg.underlying, str) or not leg.underlying.strip():
            raise VerticalValidationError("underlying is required")
        strike = _decimal(leg.strike, f"leg {index} strike")
        if strike <= ZERO:
            raise VerticalValidationError("strike must be positive")
        bid = _decimal(leg.bid, f"leg {index} bid")
        ask = _decimal(leg.ask, f"leg {index} ask")
        if bid < ZERO or ask <= ZERO or bid > ask:
            raise VerticalValidationError("quotes require non-negative bid, positive ask, and must not be crossed")
        quote_timestamp = leg.quote_timestamp if leg.quote_timestamp is not None else leg.timestamp
        if quote_timestamp is None:
            raise VerticalValidationError("quote timestamp is required")
        timestamp = _aware_datetime(quote_timestamp, f"leg {index} quote timestamp")
        if timestamp > as_of_utc:
            raise VerticalValidationError("quote timestamp cannot be in the future")
        if as_of_utc - timestamp > config.max_quote_age:
            raise VerticalValidationError("quote is stale")
        parsed_underlying, parsed_expiry, parsed_right, parsed_strike = parse_occ_symbol(leg.symbol)
        if (
            parsed_underlying != leg.underlying.strip().upper()
            or parsed_expiry != _date(leg.expiry, f"leg {index} expiry")
            or parsed_right != leg.right.upper()[0]
            or parsed_strike != strike
        ):
            raise VerticalValidationError(f"leg {index} OCC metadata does not match its fields")
    first, second = normalized
    if _date(first.expiry, "expiry") <= as_of_utc.date():
        raise VerticalValidationError("expiration must be at least the next calendar day")
    if first.underlying.strip().upper() != second.underlying.strip().upper():
        raise VerticalValidationError("legs must have the same underlying")
    if first.right.upper()[0] != second.right.upper()[0]:
        raise VerticalValidationError("legs must have the same option right")
    if _date(first.expiry, "expiry") != _date(second.expiry, "expiry"):
        raise VerticalValidationError("legs must have the same expiration")
    first_strike = _decimal(first.strike, "first strike")
    second_strike = _decimal(second.strike, "second strike")
    if first_strike == second_strike:
        raise VerticalValidationError("legs must have distinct strikes")
    if first.side.lower() == second.side.lower():
        raise VerticalValidationError("a vertical requires one buy and one sell leg")
    if first.quantity != second.quantity:
        raise VerticalValidationError("vertical legs must have equal 1:1 quantities")
    quantity = first.quantity
    if cap is not None and quantity > cap:
        raise VerticalValidationError("quantity exceeds max_quantity")

    buy = first if first.side.lower() == "buy" else second
    sell = second if first.side.lower() == "buy" else first
    buy_strike = _decimal(buy.strike, "buy strike")
    sell_strike = _decimal(sell.strike, "sell strike")
    right = first.right.upper()[0]
    if right == "C":
        kind = "bull_call_debit" if buy_strike < sell_strike else "bear_call_credit"
    else:
        kind = "bear_put_debit" if buy_strike > sell_strike else "bull_put_credit"
    signed = _decimal(buy.ask, "buy ask") - _decimal(sell.bid, "sell bid")
    width = abs(buy_strike - sell_strike)
    is_debit = kind.endswith("debit")
    if (is_debit and signed < ZERO) or (not is_debit and signed > ZERO):
        raise VerticalValidationError("quote premium sign does not match vertical geometry")
    premium = signed if is_debit else -signed
    if premium <= ZERO or premium >= width:
        raise VerticalValidationError("conservative premium must be greater than zero and less than spread width")
    # A debit spread can lose the debit paid; a credit spread can lose its
    # width less the credit received.
    max_loss_per_share = signed if is_debit else width + signed
    max_loss = max_loss_per_share * STANDARD_MULTIPLIER * quantity
    costs = (fees + slippage) * quantity
    risk = max_loss + costs
    portfolio_risk = reserved + risk
    if budget is not None and portfolio_risk > budget:
        raise VerticalValidationError("vertical exceeds max_loss_budget including reserved risk and costs")
    return ValidatedVertical(
        legs=(first, second), kind=kind, quantity=quantity, width=width,
        signed_net_debit=signed, net_debit=signed if signed > ZERO else ZERO,
        net_credit=-signed if signed < ZERO else ZERO, max_loss_per_share=max_loss_per_share,
        max_loss=max_loss, fees_and_slippage=costs, risk_including_costs=risk,
        existing_reserved_risk=reserved, portfolio_risk=portfolio_risk, as_of=as_of_utc,
    )


def validate_opening_vertical(*args: Any, **kwargs: Any) -> ValidatedVertical:
    """Compatibility alias for :func:`validate_vertical`."""
    return validate_vertical(*args, **kwargs)


def vertical_from_dict(value: Mapping[str, Any], **kwargs: Any) -> ValidatedVertical:
    """Validate a JSON-shaped object containing a ``legs`` array."""
    if not isinstance(value, Mapping) or "legs" not in value:
        raise VerticalValidationError("vertical object must contain legs")
    return validate_vertical(value["legs"], **kwargs)
