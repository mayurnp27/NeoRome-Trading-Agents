"""Pure completed-bar BTC 1-minute SMA crossover signals.

The module has no clock, feed, or execution dependencies. Callers supply the
observation time and the bars they received from their market-data source.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping


_MINUTE = 60
_MAX_BARS = 300


def _decimal(value: Any, field: str) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"invalid_{field}")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError(f"invalid_{field}") from None
    if not result.is_finite():
        raise ValueError(f"invalid_{field}")
    return result


def _number(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def _mean3(values: list[Decimal | None], end: int) -> Decimal | None:
    window = values[end - 2 : end + 1]
    if len(window) != 3 or any(value is None for value in window):
        return None
    return sum(window, Decimal(0)) / Decimal(3)  # type: ignore[arg-type]


def _stochastic_series(
    bars: list[Mapping[str, Decimal]],
) -> tuple[list[Decimal | None], list[Decimal | None]]:
    lows = [bar["low"] for bar in bars]
    highs = [bar["high"] for bar in bars]
    closes = [bar["close"] for bar in bars]
    raw_k: list[Decimal | None] = []
    for i in range(len(bars)):
        if i < 4:
            raw_k.append(None)
            continue
        low, high = min(lows[i - 4 : i + 1]), max(highs[i - 4 : i + 1])
        raw_k.append(None if high == low else Decimal(100) * (closes[i] - low) / (high - low))
    slow_k = [_mean3(raw_k, i) if i >= 2 else None for i in range(len(raw_k))]
    slow_d = [_mean3(slow_k, i) if i >= 2 else None for i in range(len(slow_k))]
    return slow_k, slow_d


def _zone_exit_direction(
    slow_k: list[Decimal | None], slow_d: list[Decimal | None]
) -> str | None:
    """Confirm only on the first completed bar where both lines exit an armed zone."""
    bull_armed = bear_armed = False
    bull_crossed = bear_crossed = False
    latest_signal: str | None = None
    for i, (k, d) in enumerate(zip(slow_k, slow_d)):
        latest_signal = None
        if k is None or d is None:
            bull_armed = bear_armed = False
            bull_crossed = bear_crossed = False
            continue
        if i and slow_k[i - 1] is not None and slow_d[i - 1] is not None:
            prev_k, prev_d = slow_k[i - 1], slow_d[i - 1]
            crossed_up = prev_k <= prev_d and k > d
            crossed_down = prev_k >= prev_d and k < d
        else:
            crossed_up = crossed_down = False

        if k <= 20 and d <= 20:
            bull_armed, bull_crossed = True, bull_crossed or crossed_up
            bear_armed, bear_crossed = False, False
        elif k >= 80 and d >= 80:
            bear_armed, bear_crossed = True, bear_crossed or crossed_down
            bull_armed, bull_crossed = False, False
        else:
            if bull_armed and crossed_up:
                bull_crossed = True
            if bear_armed and crossed_down:
                bear_crossed = True
            # Once both lines leave the armed zone, the setup is consumed on
            # this bar whether or not the cross and direction agree.
            if bull_armed and k > 20 and d > 20:
                if bull_crossed and k > d:
                    latest_signal = "yes"
                bull_armed, bull_crossed = False, False
            elif bear_armed and k < 80 and d < 80:
                if bear_crossed and k < d:
                    latest_signal = "no"
                bear_armed, bear_crossed = False, False
    return latest_signal


def _parse_bars(bars: Iterable[Mapping[str, Any]], observed_at: Decimal) -> list[dict[str, Decimal]]:
    parsed: list[dict[str, Decimal]] = []
    current_open = int(observed_at // _MINUTE) * _MINUTE
    prior_timestamp: int | None = None
    for bar in bars:
        if not isinstance(bar, Mapping):
            raise ValueError("invalid_bar")
        timestamp = _decimal(bar.get("timestamp"), "timestamp")
        close_timestamp = _decimal(bar.get("bar_close_timestamp"), "bar_close_timestamp")
        if timestamp != timestamp.to_integral_value() or close_timestamp != close_timestamp.to_integral_value():
            raise ValueError("unaligned_timestamp")
        ts, close_ts = int(timestamp), int(close_timestamp)
        if ts % _MINUTE or close_ts != ts + _MINUTE:
            raise ValueError("unaligned_timestamp")
        if prior_timestamp is not None:
            if ts == prior_timestamp:
                raise ValueError("duplicate_timestamp")
            if ts < prior_timestamp:
                raise ValueError("non_chronological_bars")
        prior_timestamp = ts
        if ts > current_open:
            raise ValueError("future_timestamp")
        if len(parsed) >= _MAX_BARS:
            raise ValueError("too_many_bars")

        prices = {name: _decimal(bar.get(name), name) for name in ("open", "high", "low", "close")}
        if any(value <= 0 for value in prices.values()):
            raise ValueError("non_positive_ohlc")
        if prices["low"] > prices["high"] or not (
            prices["low"] <= prices["open"] <= prices["high"]
            and prices["low"] <= prices["close"] <= prices["high"]
        ):
            raise ValueError("invalid_ohlc_range")
        parsed.append({"timestamp": Decimal(ts), "bar_close_timestamp": Decimal(close_ts), **prices})
    return parsed


def stochastic_5_3_3(bars: Iterable[Mapping[str, Any]]) -> tuple[float | None, float | None]:
    """Return the latest slow %K and %D, or None where the series is undefined."""
    parsed = _parse_bars(bars, Decimal("1e20"))
    slow_k, slow_d = _stochastic_series(parsed)
    return _number(slow_k[-1]) if slow_k else None, _number(slow_d[-1]) if slow_d else None


def evaluate(
    bars: Iterable[Mapping[str, Any]],
    *,
    observed_at: int | float | str,
    target_close: int | float | str | None = None,
    fresh_sma_confirmation: bool = False,
) -> dict[str, Any]:
    """Evaluate fresh SMA crosses and slow-stochastic zone-exit signals.

    ``target_close`` is an epoch-second bar-close boundary. If omitted, it is
    the start of the observation's current minute. A signal is valid for at
    most 30 seconds after that boundary.
    """
    result: dict[str, Any] = {
        "target_close": None,
        "previous_sma2": None,
        "previous_sma5": None,
        "current_sma2": None,
        "current_sma5": None,
        "k": None,
        "d": None,
        "direction": None,
        "sma_bias_direction": None,
        "zone_exit_direction": None,
        "pure_qualifies": False,
        "confirmed_qualifies": False,
        "reasons": [],
    }
    reasons: list[str] = result["reasons"]
    try:
        observed = _decimal(observed_at, "observed_at")
        if target_close is None:
            target = Decimal(int(observed // _MINUTE) * _MINUTE)
        else:
            target = _decimal(target_close, "target_close")
        if target != target.to_integral_value() or int(target) % _MINUTE:
            raise ValueError("unaligned_target_close")
        result["target_close"] = int(target)
        age = observed - target
        if age < 0:
            raise ValueError("target_close_in_future")
        if age > 30:
            raise ValueError("stale_observation")

        parsed = _parse_bars(bars, observed)
        completed = [bar for bar in parsed if bar["bar_close_timestamp"] <= target]
        target_open = int(target) - _MINUTE
        if not completed or int(completed[-1]["timestamp"]) != target_open:
            raise ValueError("latest_completed_bar_missing")
        if len(completed) > 1 and any(
            int(completed[i]["timestamp"]) - int(completed[i - 1]["timestamp"]) != _MINUTE
            for i in range(1, len(completed))
        ):
            raise ValueError("bar_gap")
        closes = [bar["close"] for bar in completed]
        if len(closes) < 6:
            raise ValueError("insufficient_bars")
        prev_i, curr_i = len(closes) - 2, len(closes) - 1
        previous_sma2 = sum(closes[prev_i - 1 : prev_i + 1], Decimal(0)) / Decimal(2)
        previous_sma5 = sum(closes[prev_i - 4 : prev_i + 1], Decimal(0)) / Decimal(5)
        current_sma2 = sum(closes[curr_i - 1 : curr_i + 1], Decimal(0)) / Decimal(2)
        current_sma5 = sum(closes[curr_i - 4 : curr_i + 1], Decimal(0)) / Decimal(5)
        result.update(
            previous_sma2=_number(previous_sma2),
            previous_sma5=_number(previous_sma5),
            current_sma2=_number(current_sma2),
            current_sma5=_number(current_sma5),
        )
        if current_sma2 > current_sma5 and previous_sma2 <= previous_sma5:
            result["direction"] = "yes"
        elif current_sma2 < current_sma5 and previous_sma2 >= previous_sma5:
            result["direction"] = "no"
        result["pure_qualifies"] = result["direction"] is not None
        if current_sma2 > current_sma5:
            result["sma_bias_direction"] = "yes"
        elif current_sma2 < current_sma5:
            result["sma_bias_direction"] = "no"

        slow_k, slow_d = _stochastic_series(completed)
        k_now, d_now = (slow_k[-1], slow_d[-1])
        result["k"], result["d"] = _number(k_now), _number(d_now)
        zone_exit = _zone_exit_direction(slow_k, slow_d)
        result["zone_exit_direction"] = zone_exit
        if zone_exit is not None:
            bias_agrees = zone_exit == result["sma_bias_direction"]
            fresh_agrees = not fresh_sma_confirmation or zone_exit == result["direction"]
            result["confirmed_qualifies"] = bias_agrees and fresh_agrees
            if not result["confirmed_qualifies"]:
                reasons.append("zone_exit_sma_confirmation_mismatch")
        elif k_now is None or d_now is None:
            reasons.append("stochastic_undefined")
        else:
            reasons.append("no_zone_exit")
        if result["pure_qualifies"]:
            reasons.append("sma_crossover")
        else:
            reasons.append("no_sma_crossover")
    except ValueError as exc:
        reasons.append(str(exc))
    except (TypeError, OverflowError):
        reasons.append("invalid_input")
    return result

