"""Pure completed-bar BTC Stoch(98,3,3) zone-exit signals.

All times are epoch seconds. Callers supply source bars and observation time;
this module has no clock, network, or execution dependencies.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping


BAR_LIMIT = 500
BAR_SECONDS_BY_TIMEFRAME = {15: 900, 30: 1800}
MIN_BARS = 103
K_PERIOD = 98
SMOOTH_K = 3
SMOOTH_D = 3
MAX_SIGNAL_AGE_SECONDS = 90


def _decimal(value: Any, field: str) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"invalid_{field}")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError(f"invalid_{field}") from None
    if not parsed.is_finite():
        raise ValueError(f"invalid_{field}")
    return parsed


def _float(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def _mean3(values: list[Decimal | None], end: int) -> Decimal | None:
    window = values[end - 2 : end + 1]
    if len(window) != 3 or any(value is None for value in window):
        return None
    return sum(window, Decimal(0)) / Decimal(3)  # type: ignore[arg-type]


def _stochastic_series(
    bars: list[Mapping[str, Decimal]],
) -> tuple[list[Decimal | None], list[Decimal | None]]:
    """Return slow %K/%D, with flat lookback windows explicitly undefined."""
    raw_k: list[Decimal | None] = []
    lows = [bar["low"] for bar in bars]
    highs = [bar["high"] for bar in bars]
    closes = [bar["close"] for bar in bars]
    for index, close in enumerate(closes):
        if index < K_PERIOD - 1:
            raw_k.append(None)
            continue
        low = min(lows[index - K_PERIOD + 1 : index + 1])
        high = max(highs[index - K_PERIOD + 1 : index + 1])
        raw_k.append(None if high == low else Decimal(100) * (close - low) / (high - low))
    slow_k = [_mean3(raw_k, i) if i >= 2 else None for i in range(len(raw_k))]
    slow_d = [_mean3(slow_k, i) if i >= 2 else None for i in range(len(slow_k))]
    return slow_k, slow_d


def stochastic_98_3_3(
    bars: Iterable[Mapping[str, Any]], *, timeframe_minutes: int = 15
) -> tuple[float | None, float | None]:
    """Calculate latest slow %K/%D from OHLC bars, or None if undefined."""
    seconds = _bar_seconds(timeframe_minutes)
    parsed = _parse_bars(bars, Decimal("1e20"), seconds)
    k, d = _stochastic_series(parsed)
    return (_float(k[-1]) if k else None, _float(d[-1]) if d else None)


def _zone_exit_direction(
    slow_k: list[Decimal | None], slow_d: list[Decimal | None]
) -> tuple[str | None, str]:
    """Apply the fresh-cross, first-zone-exit rule to a chronological series."""
    bull_armed = bear_armed = False
    bull_crossed = bear_crossed = False
    latest_direction: str | None = None
    latest_reason = "stochastic_undefined"
    for index, (k, d) in enumerate(zip(slow_k, slow_d)):
        latest_direction = None
        latest_reason = "stochastic_undefined" if k is None or d is None else "no_zone_exit"
        if k is None or d is None:
            bull_armed = bear_armed = False
            bull_crossed = bear_crossed = False
            continue

        prev_k = slow_k[index - 1] if index else None
        prev_d = slow_d[index - 1] if index else None
        crossed_up = prev_k is not None and prev_d is not None and prev_k <= prev_d and k > d
        crossed_down = prev_k is not None and prev_d is not None and prev_k >= prev_d and k < d

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
            if bull_armed and k > 20 and d > 20:
                latest_reason = "bullish_zone_exit"
                if bull_crossed and k > d:
                    latest_direction = "yes"
                    latest_reason = "bullish_zone_exit_confirmed"
                bull_armed = bull_crossed = False
            elif bear_armed and k < 80 and d < 80:
                latest_reason = "bearish_zone_exit"
                if bear_crossed and k < d:
                    latest_direction = "no"
                    latest_reason = "bearish_zone_exit_confirmed"
                bear_armed = bear_crossed = False
    return latest_direction, latest_reason


def _bar_seconds(timeframe_minutes: int) -> int:
    if isinstance(timeframe_minutes, bool) or timeframe_minutes not in BAR_SECONDS_BY_TIMEFRAME:
        raise ValueError("unsupported_timeframe")
    return BAR_SECONDS_BY_TIMEFRAME[timeframe_minutes]


def _parse_bars(
    bars: Iterable[Mapping[str, Any]], observed_at: Decimal, bar_seconds: int
) -> list[dict[str, Decimal]]:
    parsed: list[dict[str, Decimal]] = []
    current_open = int(observed_at // bar_seconds) * bar_seconds
    prior_timestamp: int | None = None
    for bar in bars:
        if not isinstance(bar, Mapping):
            raise ValueError("invalid_bar")
        timestamp = _decimal(bar.get("timestamp"), "timestamp")
        close_timestamp = _decimal(bar.get("bar_close_timestamp"), "bar_close_timestamp")
        if timestamp != timestamp.to_integral_value() or close_timestamp != close_timestamp.to_integral_value():
            raise ValueError("unaligned_timestamp")
        ts, close_ts = int(timestamp), int(close_timestamp)
        if ts % bar_seconds or close_ts != ts + bar_seconds:
            raise ValueError("unaligned_timestamp")
        if prior_timestamp is not None:
            if ts == prior_timestamp:
                raise ValueError("duplicate_timestamp")
            if ts < prior_timestamp:
                raise ValueError("non_chronological_bars")
        prior_timestamp = ts
        if ts > current_open:
            raise ValueError("future_timestamp")
        if len(parsed) >= BAR_LIMIT:
            raise ValueError("too_many_bars")
        prices = {key: _decimal(bar.get(key), key) for key in ("open", "high", "low", "close")}
        if any(value <= 0 for value in prices.values()):
            raise ValueError("non_positive_ohlc")
        if prices["low"] > prices["high"] or not (
            prices["low"] <= prices["open"] <= prices["high"]
            and prices["low"] <= prices["close"] <= prices["high"]
        ):
            raise ValueError("invalid_ohlc_range")
        parsed.append({"timestamp": Decimal(ts), "bar_close_timestamp": Decimal(close_ts), **prices})
    return parsed


def evaluate(
    bars: Iterable[Mapping[str, Any]],
    *,
    observed_at: int | float | str,
    timeframe_minutes: int = 15,
    target_close: int | float | str | None = None,
) -> dict[str, Any]:
    """Evaluate only the latest fresh completed bar for a 15m or 30m series."""
    result: dict[str, Any] = {
        "timeframe_minutes": timeframe_minutes,
        "target_close": None,
        "source_bar_timestamp": None,
        "source_bar_close_timestamp": None,
        "parameters": {"k_period": K_PERIOD, "smooth_k": SMOOTH_K, "smooth_d": SMOOTH_D},
        "k": None,
        "d": None,
        "direction": None,
        "zone_exit_reason": "stochastic_undefined",
        "reasons": [],
    }
    reasons: list[str] = result["reasons"]
    try:
        seconds = _bar_seconds(timeframe_minutes)
        observed = _decimal(observed_at, "observed_at")
        target = Decimal(int(observed // seconds) * seconds) if target_close is None else _decimal(target_close, "target_close")
        if target != target.to_integral_value() or int(target) % seconds:
            raise ValueError("unaligned_target_close")
        result["target_close"] = int(target)
        age = observed - target
        if age < 0:
            raise ValueError("target_close_in_future")
        if age > MAX_SIGNAL_AGE_SECONDS:
            raise ValueError("stale_observation")
        parsed = _parse_bars(bars, observed, seconds)
        completed = [bar for bar in parsed if bar["bar_close_timestamp"] <= target]
        expected_open = int(target) - seconds
        if not completed or int(completed[-1]["timestamp"]) != expected_open:
            raise ValueError("latest_completed_bar_missing")
        if len(completed) > 1 and any(
            int(completed[i]["timestamp"] - completed[i - 1]["timestamp"]) != seconds
            for i in range(1, len(completed))
        ):
            raise ValueError("bar_gap")
        if len(completed) < MIN_BARS:
            raise ValueError("insufficient_bars")
        slow_k, slow_d = _stochastic_series(completed)
        k, d = slow_k[-1], slow_d[-1]
        direction, reason = _zone_exit_direction(slow_k, slow_d)
        latest = completed[-1]
        result.update(
            source_bar_timestamp=int(latest["timestamp"]),
            source_bar_close_timestamp=int(latest["bar_close_timestamp"]),
            k=_float(k),
            d=_float(d),
            direction=direction,
            zone_exit_reason=reason,
        )
        if direction is None:
            reasons.append(reason)
    except ValueError as exc:
        reasons.append(str(exc))
    except (TypeError, OverflowError):
        reasons.append("invalid_input")
    return result
