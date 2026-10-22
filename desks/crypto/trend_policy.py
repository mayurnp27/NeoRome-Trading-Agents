"""Deterministic CR-001 BTC/ETH spot trend comparator.

This module is an offline research and internal-paper candidate policy.  It
does not fetch data, create broker orders, reserve ledger cash, or approve
memecoin allocation.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_DOWN, ROUND_FLOOR
from typing import Any, Mapping, Sequence


UTC = timezone.utc
POLICY_ID = "CR-001"
POLICY_VERSION = "cr-001-v1"
SUPPORTED_SYMBOLS = {"BTC/USD", "ETH/USD"}
MIN_HISTORY_DAYS = 101
QUOTE_MAX_AGE_SECONDS = 60
IDEA_CAP_CENTS = 500
UNIT_QUANTUM = Decimal("0.000001")
DEFAULT_FEE_BPS = Decimal("10")
DEFAULT_SLIPPAGE_BPS = Decimal("5")


class TrendPolicyError(ValueError):
    """Malformed or contradictory policy input."""


class InsufficientData(TrendPolicyError):
    """Input cannot support a decision yet."""


class StaleHistory(InsufficientData):
    """The latest completed bar is not the immediately preceding UTC day."""


class QuoteUnavailable(InsufficientData):
    """The quote is absent, stale, future-dated, or cannot support the size."""


def _decimal(value: Any, field: str, *, positive: bool = False) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise TrendPolicyError(f"{field} must be numeric")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise TrendPolicyError(f"{field} must be numeric") from exc
    if not number.is_finite() or (positive and number <= 0):
        raise TrendPolicyError(f"{field} must be finite and positive")
    return number


def _timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise TrendPolicyError(f"{field} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise TrendPolicyError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise TrendPolicyError(f"{field} must include a timezone")
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _ceil_cents(dollars: Decimal) -> int:
    return int((dollars * 100).to_integral_value(rounding=ROUND_CEILING))


def _floor_cents(dollars: Decimal) -> int:
    return int((dollars * 100).to_integral_value(rounding=ROUND_FLOOR))


@dataclass(frozen=True)
class CostAssumptions:
    """Frozen v1 cost model, stated per side in basis points."""

    fee_bps: Decimal = DEFAULT_FEE_BPS
    slippage_bps: Decimal = DEFAULT_SLIPPAGE_BPS
    idea_cap_cents: int = IDEA_CAP_CENTS
    unit_precision: int = 6

    def __post_init__(self) -> None:
        fee = _decimal(self.fee_bps, "fee_bps")
        slippage = _decimal(self.slippage_bps, "slippage_bps")
        if fee < 0 or slippage < 0 or fee > 10_000 or slippage > 10_000:
            raise TrendPolicyError("fee_bps and slippage_bps must be between 0 and 10000")
        if type(self.idea_cap_cents) is not int or self.idea_cap_cents <= 0:
            raise TrendPolicyError("idea_cap_cents must be positive")
        if type(self.unit_precision) is not int or not 1 <= self.unit_precision <= 12:
            raise TrendPolicyError("unit_precision must be between 1 and 12")

    @property
    def fee_rate(self) -> Decimal:
        return _decimal(self.fee_bps, "fee_bps") / Decimal(10_000)

    @property
    def slippage_rate(self) -> Decimal:
        return _decimal(self.slippage_bps, "slippage_bps") / Decimal(10_000)

    @property
    def quantum(self) -> Decimal:
        return Decimal(1).scaleb(-self.unit_precision)

    def as_dict(self) -> dict[str, Any]:
        return {
            "fee_bps_per_side": str(_decimal(self.fee_bps, "fee_bps")),
            "slippage_bps_per_side": str(_decimal(self.slippage_bps, "slippage_bps")),
            "idea_cap_cents": self.idea_cap_cents,
            "unit_precision": self.unit_precision,
            "fractional_units": True,
        }


def _bar_timestamp(bar: Mapping[str, Any], index: int) -> datetime:
    timestamp = _timestamp(bar.get("timestamp"), f"bars[{index}].timestamp")
    if timestamp.time() != datetime.min.time():
        raise TrendPolicyError("daily bar timestamps must be UTC midnight")
    if bar.get("complete") is not True:
        raise TrendPolicyError("all daily bars must be marked complete")
    return timestamp


def validate_bars(
    bars: Sequence[Mapping[str, Any]], *, now: datetime, minimum_days: int = MIN_HISTORY_DAYS
) -> tuple[dict[str, Any], ...]:
    """Validate completed, consecutive UTC daily OHLC bars without filling gaps."""
    if not isinstance(bars, (list, tuple)):
        raise TrendPolicyError("bars must be a list or tuple")
    current = now.astimezone(UTC)
    clean: list[dict[str, Any]] = []
    previous: datetime | None = None
    for index, raw in enumerate(bars):
        if not isinstance(raw, Mapping):
            raise TrendPolicyError(f"bars[{index}] must be an object")
        timestamp = _bar_timestamp(raw, index)
        if timestamp.date() >= current.date():
            raise TrendPolicyError("bars cannot include the current or a future UTC day")
        if previous is not None and timestamp != previous + timedelta(days=1):
            raise TrendPolicyError("daily bars must be unique and sequential UTC days")
        values = {name: _decimal(raw.get(name), f"bars[{index}].{name}", positive=True)
                  for name in ("open", "high", "low", "close")}
        if values["low"] > min(values["open"], values["close"]) or values["high"] < max(values["open"], values["close"]):
            raise TrendPolicyError("daily bar has inconsistent OHLC geometry")
        clean.append({"timestamp": _iso(timestamp), "complete": True, **{k: str(v) for k, v in values.items()}})
        previous = timestamp
    if len(clean) < minimum_days:
        raise InsufficientData(f"need at least {minimum_days} completed daily bars")
    latest_day = datetime.fromisoformat(clean[-1]["timestamp"].replace("Z", "+00:00")).date()
    if latest_day != current.date() - timedelta(days=1):
        raise StaleHistory("latest completed bar must be the immediately preceding UTC day")
    return tuple(clean)


def trend_signal(bars: Sequence[Mapping[str, Any]], *, now: datetime) -> dict[str, Any]:
    """Calculate CR-001 signals using only bars before the signal bar."""
    clean = validate_bars(bars, now=now)
    closes = [Decimal(row["close"]) for row in clean]
    highs = [Decimal(row["high"]) for row in clean]
    lows = [Decimal(row["low"]) for row in clean]
    signal_bar = clean[-1]
    prior_100_mean = sum(closes[-101:-1], Decimal(0)) / Decimal(100)
    prior_20_high = max(highs[-21:-1])
    prior_10_low = min(lows[-11:-1])
    signal_close = closes[-1]
    return {
        "policy_id": POLICY_ID,
        "policy_version": POLICY_VERSION,
        "signal_day": signal_bar["timestamp"],
        "signal_close": str(signal_close),
        "prior_100_day_mean": str(prior_100_mean),
        "prior_20_day_high": str(prior_20_high),
        "prior_10_day_low": str(prior_10_low),
        "entry_signal": signal_close > prior_100_mean and signal_close > prior_20_high,
        "exit_signal": signal_close < prior_10_low or signal_close < prior_100_mean,
        "history_days": len(clean),
    }


def validate_quote(
    quote: Mapping[str, Any], *, symbol: str, now: datetime, signal_day: str
) -> dict[str, Any]:
    if not isinstance(quote, Mapping):
        raise TrendPolicyError("quote must be an object")
    if quote.get("symbol") != symbol:
        raise TrendPolicyError("quote symbol does not match policy symbol")
    timestamp = _timestamp(quote.get("timestamp"), "quote.timestamp")
    current = now.astimezone(UTC)
    signal_end = _timestamp(signal_day, "signal_day") + timedelta(days=1)
    if timestamp < signal_end:
        raise QuoteUnavailable("quote predates the next execution day; same-close fills are forbidden")
    age = (current - timestamp).total_seconds()
    if age < 0:
        raise QuoteUnavailable("quote is in the future")
    if age > QUOTE_MAX_AGE_SECONDS:
        raise QuoteUnavailable("quote is stale")
    bid = _decimal(quote.get("bid"), "quote.bid", positive=True)
    ask = _decimal(quote.get("ask"), "quote.ask", positive=True)
    depth = _decimal(quote.get("depth_units"), "quote.depth_units", positive=True)
    if bid > ask:
        raise TrendPolicyError("quote is crossed")
    return {"symbol": symbol, "timestamp": _iso(timestamp), "bid": str(bid), "ask": str(ask), "depth_units": str(depth), "age_seconds": age}


def _abstain(reason: str, *, signal: Mapping[str, Any] | None = None, quote: Mapping[str, Any] | None = None, costs: CostAssumptions | None = None) -> dict[str, Any]:
    return {
        "status": "abstain",
        "reason": reason,
        "policy_id": POLICY_ID,
        "policy_version": POLICY_VERSION,
        "execution_enabled": False,
        "signal": dict(signal) if signal is not None else None,
        "quote": dict(quote) if quote is not None else None,
        "cost_assumptions": (costs or CostAssumptions()).as_dict(),
    }


def _entry_valuation(quote: Mapping[str, Any], costs: CostAssumptions) -> dict[str, Any] | None:
    ask = Decimal(quote["ask"])
    depth = Decimal(quote["depth_units"])
    adjusted = ask * (Decimal(1) + costs.slippage_rate)
    per_unit_all_in = adjusted * (Decimal(1) + costs.fee_rate)
    cap_dollars = Decimal(costs.idea_cap_cents) / Decimal(100)
    maximum = min(depth, cap_dollars / per_unit_all_in)
    maximum_index = int((maximum / costs.quantum).to_integral_value(rounding=ROUND_FLOOR))
    if maximum_index <= 0:
        return None

    # Search integer quantum counts so cent rounding cannot push an apparently
    # capped order one cent over the all-in limit.
    def valuation(index: int) -> tuple[Decimal, int, int, int]:
        units = costs.quantum * index
        gross = adjusted * units
        fee = gross * costs.fee_rate
        gross_cents = _ceil_cents(gross)
        fee_cents = _ceil_cents(fee)
        return units, gross_cents, fee_cents, gross_cents + fee_cents

    low, high = 0, maximum_index
    while low < high:
        midpoint = (low + high + 1) // 2
        if valuation(midpoint)[3] <= costs.idea_cap_cents:
            low = midpoint
        else:
            high = midpoint - 1
    if low == 0:
        return None
    units, gross_cents, fee_cents, total = valuation(low)
    return {
        "side": "buy",
        "units": str(units),
        "modeled_price": str(adjusted),
        "quoted_price": str(ask),
        "gross_cost_cents": gross_cents,
        "fee_cents": fee_cents,
        "slippage_cents": max(0, gross_cents - _ceil_cents(ask * units)),
        "all_in_cost_cents": total,
    }


def _exit_valuation(quote: Mapping[str, Any], units: Decimal, costs: CostAssumptions) -> dict[str, Any]:
    bid = Decimal(quote["bid"])
    adjusted = bid * (Decimal(1) - costs.slippage_rate)
    gross = adjusted * units
    fee = gross * costs.fee_rate
    gross_cents = _floor_cents(gross)
    fee_cents = _ceil_cents(fee)
    return {
        "side": "sell", "units": str(units), "modeled_price": str(adjusted),
        "quoted_price": str(bid), "gross_proceeds_cents": gross_cents,
        "fee_cents": fee_cents, "slippage_cents": max(0, _floor_cents(bid * units) - gross_cents),
        "net_proceeds_cents": max(0, gross_cents - fee_cents),
    }


def evaluate(
    bars: Sequence[Mapping[str, Any]],
    quote: Mapping[str, Any] | None,
    *,
    now: datetime,
    symbol: str = "BTC/USD",
    position_units: Any = 0,
    costs: CostAssumptions | None = None,
) -> dict[str, Any]:
    """Return an entry/exit candidate or an explicit abstention."""
    if symbol not in SUPPORTED_SYMBOLS:
        raise TrendPolicyError("symbol must be BTC/USD or ETH/USD")
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise TrendPolicyError("now must be timezone-aware")
    costs = costs or CostAssumptions()
    try:
        signal = trend_signal(bars, now=now)
    except StaleHistory:
        return _abstain("stale_history", costs=costs)
    except InsufficientData:
        return _abstain("insufficient_history", costs=costs)
    if quote is None:
        return _abstain("missing_quote", signal=signal, costs=costs)
    try:
        clean_quote = validate_quote(quote, symbol=symbol, now=now, signal_day=signal["signal_day"])
    except QuoteUnavailable as exc:
        return _abstain(str(exc), signal=signal, costs=costs)
    try:
        units_held = _decimal(position_units, "position_units")
    except TrendPolicyError:
        raise
    if units_held < 0:
        raise TrendPolicyError("position_units cannot be negative")
    if units_held > 0 and signal["exit_signal"]:
        if Decimal(clean_quote["depth_units"]) < units_held:
            return _abstain("insufficient_exit_depth", signal=signal, quote=clean_quote, costs=costs)
        return {"status": "exit_candidate", "policy_id": POLICY_ID, "policy_version": POLICY_VERSION,
                "execution_enabled": False, "signal": signal, "quote": clean_quote,
                "valuation": _exit_valuation(clean_quote, units_held, costs), "cost_assumptions": costs.as_dict()}
    if units_held > 0:
        return _abstain("hold", signal=signal, quote=clean_quote, costs=costs)
    if not signal["entry_signal"]:
        return _abstain("no_entry_signal", signal=signal, quote=clean_quote, costs=costs)
    valuation = _entry_valuation(clean_quote, costs)
    if valuation is None:
        return _abstain("insufficient_depth_or_cap_for_fractional_units", signal=signal, quote=clean_quote, costs=costs)
    return {"status": "entry_candidate", "policy_id": POLICY_ID, "policy_version": POLICY_VERSION,
            "execution_enabled": False, "signal": signal, "quote": clean_quote,
            "valuation": valuation, "cost_assumptions": costs.as_dict()}


__all__ = [
    "CostAssumptions", "IDEA_CAP_CENTS", "InsufficientData", "POLICY_ID", "POLICY_VERSION",
    "QuoteUnavailable", "StaleHistory", "TrendPolicyError", "evaluate", "trend_signal", "validate_bars", "validate_quote",
]
