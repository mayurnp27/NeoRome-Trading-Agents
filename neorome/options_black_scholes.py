"""Deterministic Black--Scholes IV and delta enrichment for option archives."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, getcontext
import math
from typing import Any, Mapping


getcontext().prec = 28


class EnrichmentError(ValueError):
    """Inputs are missing, stale, or outside the model's valid domain."""


@dataclass(frozen=True)
class UnderlyingInputs:
    """Point-in-time inputs used to calculate a quote's delta."""

    spot: Decimal
    timestamp: datetime
    rate: Decimal
    dividend_yield: Decimal

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "UnderlyingInputs":
        required = {"spot", "timestamp", "rate", "dividend_yield"}
        if not isinstance(value, Mapping) or not required.issubset(value):
            raise EnrichmentError("underlying input needs spot, timestamp, rate and dividend_yield")
        timestamp = value["timestamp"]
        if isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            except ValueError as exc:
                raise EnrichmentError("underlying timestamp is not ISO-8601") from exc
        if not isinstance(timestamp, datetime) or timestamp.tzinfo is None:
            raise EnrichmentError("underlying timestamp must include a timezone")
        try:
            spot = Decimal(str(value["spot"]))
            rate = Decimal(str(value["rate"]))
            dividend = Decimal(str(value["dividend_yield"]))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise EnrichmentError("underlying numeric inputs are invalid") from exc
        if not all(item.is_finite() for item in (spot, rate, dividend)) or spot <= 0 or rate < -1 or dividend < 0:
            raise EnrichmentError("underlying numeric inputs are outside the supported range")
        return cls(spot=spot, timestamp=timestamp.astimezone(timezone.utc), rate=rate, dividend_yield=dividend)


def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _normal_pdf(value: float) -> float:
    return math.exp(-0.5 * value * value) / math.sqrt(2.0 * math.pi)


def _year_fraction(expiry: date, as_of: datetime) -> float:
    if as_of.tzinfo is None:
        raise EnrichmentError("as_of must be timezone-aware")
    seconds = (datetime.combine(expiry, datetime.min.time(), tzinfo=timezone.utc) - as_of.astimezone(timezone.utc)).total_seconds()
    if seconds <= 0:
        raise EnrichmentError("option has expired at quote timestamp")
    return seconds / (365.0 * 24.0 * 3600.0)


def _bounds(spot: float, strike: float, time: float, rate: float, dividend: float, right: str) -> tuple[float, float]:
    discounted_strike = strike * math.exp(-rate * time)
    discounted_spot = spot * math.exp(-dividend * time)
    if right == "C":
        return max(0.0, discounted_spot - discounted_strike), discounted_spot
    return max(0.0, discounted_strike - discounted_spot), discounted_strike


def _price(spot: float, strike: float, time: float, rate: float, dividend: float, volatility: float, right: str) -> float:
    if volatility <= 0:
        raise EnrichmentError("volatility must be positive")
    root = volatility * math.sqrt(time)
    d1 = (math.log(spot / strike) + (rate - dividend + 0.5 * volatility * volatility) * time) / root
    d2 = d1 - root
    if right == "C":
        return spot * math.exp(-dividend * time) * _normal_cdf(d1) - strike * math.exp(-rate * time) * _normal_cdf(d2)
    if right == "P":
        return strike * math.exp(-rate * time) * _normal_cdf(-d2) - spot * math.exp(-dividend * time) * _normal_cdf(-d1)
    raise EnrichmentError("option right must be C or P")


def implied_volatility(
    *,
    price: Decimal | str | float,
    spot: Decimal | str | float,
    strike: Decimal | str | float,
    expiry: date,
    as_of: datetime,
    rate: Decimal | str | float,
    dividend_yield: Decimal | str | float,
    right: str,
) -> Decimal:
    """Solve IV by a fixed bisection schedule after no-arbitrage checks."""
    try:
        values = [Decimal(str(item)) for item in (price, spot, strike, rate, dividend_yield)]
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise EnrichmentError("option IV inputs are invalid") from exc
    premium, spot_d, strike_d, rate_d, dividend_d = values
    right = str(right).upper()
    if not all(item.is_finite() for item in values) or premium <= 0 or spot_d <= 0 or strike_d <= 0:
        raise EnrichmentError("option IV inputs are outside the supported range")
    time = _year_fraction(expiry, as_of)
    lower, upper = _bounds(float(spot_d), float(strike_d), time, float(rate_d), float(dividend_d), right)
    price_f = float(premium)
    if price_f < lower - 1e-8 or price_f > upper + 1e-8:
        raise EnrichmentError("option midpoint violates no-arbitrage bounds")
    low, high = 1e-8, 8.0
    if _price(float(spot_d), float(strike_d), time, float(rate_d), float(dividend_d), high, right) < price_f:
        raise EnrichmentError("option midpoint is outside the supported volatility range")
    for _ in range(80):
        mid = (low + high) / 2.0
        if _price(float(spot_d), float(strike_d), time, float(rate_d), float(dividend_d), mid, right) > price_f:
            high = mid
        else:
            low = mid
    return Decimal(str((low + high) / 2.0))


def delta_from_iv(*, spot: Decimal | str | float, strike: Decimal | str | float, expiry: date, as_of: datetime, rate: Decimal | str | float, dividend_yield: Decimal | str | float, volatility: Decimal | str | float, right: str) -> Decimal:
    """Calculate delta from the frozen IV and archived market inputs."""
    values = [Decimal(str(item)) for item in (spot, strike, rate, dividend_yield, volatility)]
    spot_d, strike_d, rate_d, dividend_d, vol_d = values
    if not all(item.is_finite() for item in values) or spot_d <= 0 or strike_d <= 0 or vol_d <= 0:
        raise EnrichmentError("delta inputs are outside the supported range")
    right = str(right).upper()
    time = _year_fraction(expiry, as_of)
    d1 = (math.log(float(spot_d / strike_d)) + (float(rate_d - dividend_d) + 0.5 * float(vol_d) ** 2) * time) / (float(vol_d) * math.sqrt(time))
    factor = math.exp(-float(dividend_d) * time)
    if right == "C":
        return Decimal(str(factor * _normal_cdf(d1)))
    if right == "P":
        return Decimal(str(factor * (_normal_cdf(d1) - 1.0)))
    raise EnrichmentError("option right must be C or P")


def enrich_quote(*, bid: Any, ask: Any, contract: Mapping[str, Any], quote_timestamp: datetime, inputs: UnderlyingInputs) -> dict[str, Any]:
    """Enrich one validated BBO with IV and delta, preserving provenance."""
    if inputs.timestamp > quote_timestamp.astimezone(timezone.utc):
        raise EnrichmentError("underlying input follows quote timestamp")
    expiry = contract.get("expiry")
    if isinstance(expiry, str):
        expiry = date.fromisoformat(expiry)
    if not isinstance(expiry, date):
        raise EnrichmentError("contract expiry is required")
    midpoint = (Decimal(str(bid)) + Decimal(str(ask))) / Decimal(2)
    iv = implied_volatility(price=midpoint, spot=inputs.spot, strike=contract["strike"], expiry=expiry, as_of=quote_timestamp, rate=inputs.rate, dividend_yield=inputs.dividend_yield, right=contract["right"])
    delta = delta_from_iv(spot=inputs.spot, strike=contract["strike"], expiry=expiry, as_of=quote_timestamp, rate=inputs.rate, dividend_yield=inputs.dividend_yield, volatility=iv, right=contract["right"])
    return {"implied_volatility": str(iv), "delta": str(delta), "delta_source": "black_scholes_midpoint_v1", "underlying_inputs": {"spot": str(inputs.spot), "timestamp": inputs.timestamp.isoformat().replace("+00:00", "Z"), "rate": str(inputs.rate), "dividend_yield": str(inputs.dividend_yield)}}
