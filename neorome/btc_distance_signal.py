"""Frozen BTC hourly-distance signal, implemented with the Python standard library."""

from __future__ import annotations

from datetime import datetime, timezone
import math
from typing import Any, Iterable, Mapping, Sequence


_FIVE_MINUTES = 300
_FIFTEEN_MINUTES = 900
_FORTY_FIVE_MINUTES = 2700
_SIGMA_RETURNS = 288
_DISTANCE_HORIZON = math.sqrt(9.0)


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _parse_rows(rows: Iterable[Sequence[Any] | Mapping[str, Any]], decision_epoch: int) -> dict[int, tuple[float, float, float, float, float]]:
    """Return validated candles before the decision; future candles are never read."""
    candles: dict[int, tuple[float, float, float, float, float]] = {}
    for row in rows:
        try:
            if isinstance(row, Mapping):
                epoch = int(row["open_epoch"])
                values = (row["low"], row["high"], row["open"], row["close"], row["volume"])
            else:
                epoch = int(row[0])
                values = (row[1], row[2], row[3], row[4], row[5])
        except (KeyError, IndexError, TypeError, ValueError, OverflowError) as exc:
            raise ValueError("malformed_candle") from exc
        if epoch >= decision_epoch:
            continue
        if epoch % _FIVE_MINUTES:
            raise ValueError("unaligned_candle")
        if epoch in candles:
            raise ValueError("duplicate_candle")
        parsed = tuple(_number(v) for v in values)
        if any(value is None for value in parsed):
            raise ValueError("invalid_candle_value")
        low, high, open_, close, volume = parsed  # type: ignore[misc]
        if low > min(open_, close) or high < max(open_, close) or low > high or volume < 0:
            raise ValueError("invalid_ohlcv")
        candles[epoch] = (low, high, open_, close, volume)
    return candles


def _bar(candles: dict[int, tuple[float, float, float, float, float]], end_epoch: int, minutes: int) -> tuple[float, float, float, float] | None:
    """Aggregate one complete epoch-aligned bar whose right label is end_epoch."""
    count = minutes // 5
    starts = range(end_epoch - minutes * 60, end_epoch, _FIVE_MINUTES)
    rows = [candles.get(start) for start in starts]
    if len(rows) != count or any(row is None for row in rows):
        return None
    complete = rows  # preserve chronological order
    return (complete[0][2], max(row[1] for row in complete), min(row[0] for row in complete), complete[-1][3])  # type: ignore[index]


def _stochastic(candles: dict[int, tuple[float, float, float, float, float]], decision_epoch: int) -> int | None:
    # D slope for (67, 1, 1) is the sign of the change in the 67-bar
    # stochastic level. Two adjacent valid levels require 68 closed bars.
    latest_end = decision_epoch - decision_epoch % _FIFTEEN_MINUTES
    # A missing source bar invalidates every rolling window that includes it.
    bars: list[tuple[float, float, float, float] | None] = []
    for end in range(latest_end - 67 * _FIFTEEN_MINUTES, latest_end + 1, _FIFTEEN_MINUTES):
        bars.append(_bar(candles, end, 15))
    values: list[float | None] = []
    for index in range(66, len(bars)):
        window = bars[index - 66:index + 1]
        if any(bar is None for bar in window):
            values.append(None)
            continue
        high = max(bar[1] for bar in window if bar is not None)
        low = min(bar[2] for bar in window if bar is not None)
        close = window[-1][3]  # type: ignore[index]
        values.append(100.0 * (close - low) / (high - low) if high > low else None)
    if len(values) < 2 or values[-1] is None or values[-2] is None:
        return None
    delta = values[-1] - values[-2]
    return (delta > 0) - (delta < 0)


def _sma(candles: dict[int, tuple[float, float, float, float, float]], decision_epoch: int) -> int | None:
    # A valid non-crossing (or non-close decision) contributes zero. At a
    # 45-minute close, missing current/previous bars make the component invalid.
    if decision_epoch % _FORTY_FIVE_MINUTES:
        return 0
    # Work in integer cents, as in the frozen signal's exact cent sums.
    from decimal import Decimal, InvalidOperation

    try:
        closes: list[int] = []
        for end in (decision_epoch - 3 * _FORTY_FIVE_MINUTES,
                    decision_epoch - 2 * _FORTY_FIVE_MINUTES,
                    decision_epoch - _FORTY_FIVE_MINUTES,
                    decision_epoch):
            bar = _bar(candles, end, 45)
            if bar is None:
                return None
            cents = Decimal(str(bar[3])) * 100
            if cents != cents.to_integral_value():
                return None
            closes.append(int(cents))
        older, two_ago, prior, current = closes
        # gap = sum(last 2)*3 - sum(last 3)*2
        sums2 = prior + current
        sums3 = two_ago + prior + current
        gap = sums2 * 3 - sums3 * 2
        prior_sums2 = two_ago + prior
        prior_sums3 = older + two_ago + prior
        previous_gap = prior_sums2 * 3 - prior_sums3 * 2
    except (InvalidOperation, ValueError):
        return None
    if gap > 0 and previous_gap <= 0:
        return 1
    if gap < 0 and previous_gap >= 0:
        return -1
    return 0


def _sigma(candles: dict[int, tuple[float, float, float, float, float]], decision_epoch: int) -> tuple[float | None, float | None]:
    closes: list[float] = []
    for end in range(decision_epoch - _SIGMA_RETURNS * _FIVE_MINUTES, decision_epoch + 1, _FIVE_MINUTES):
        candle = candles.get(end - _FIVE_MINUTES)
        if candle is None:
            return None, None
        closes.append(candle[3])
    if len(closes) != _SIGMA_RETURNS + 1 or any(price <= 0 for price in closes):
        return None, None
    returns = [closes[index] / closes[index - 1] - 1.0 for index in range(1, len(closes))]
    mean = math.fsum(returns) / len(returns)
    variance = math.fsum((value - mean) ** 2 for value in returns) / (len(returns) - 1)
    sigma = math.sqrt(variance)
    return closes[-1], sigma


def evaluate(rows: Iterable[Sequence[Any] | Mapping[str, Any]], decision_epoch: int, strike: Any) -> dict[str, Any]:
    """Evaluate the frozen signal from completed 5-minute Coinbase candles.

    ``rows`` contains ``[open_epoch, low, high, open, close, volume]`` records.
    The result is JSON-serializable and contains the intermediate features used
    to qualify the directional signal.
    """
    try:
        decision_epoch = int(decision_epoch)
    except (TypeError, ValueError, OverflowError):
        return _invalid("invalid_decision_epoch", None, None, None, None, None, None)
    decision_time = datetime.fromtimestamp(decision_epoch, timezone.utc).isoformat().replace("+00:00", "Z")
    if decision_epoch % _FIVE_MINUTES:
        return _invalid("unaligned_decision", decision_time, None, None, None, None, None)
    strike_value = _number(strike)
    if strike_value is None or strike_value <= 0:
        return _invalid("invalid_strike", decision_time, None, None, None, None, None)
    try:
        candles = _parse_rows(rows, decision_epoch)
    except ValueError as exc:
        return _invalid(str(exc), decision_time, None, None, None, None, None)
    spot, sigma = _sigma(candles, decision_epoch)
    stochastic = _stochastic(candles, decision_epoch)
    sma = _sma(candles, decision_epoch)
    distance = None
    if spot is not None and sigma is not None and sigma > 0:
        distance = abs(spot - strike_value) / (spot * sigma * _DISTANCE_HORIZON)
    if spot is None or sigma is None or sigma <= 0:
        return _invalid("insufficient_or_invalid_sigma_history", decision_time, stochastic, sma, spot, sigma, distance)
    if stochastic is None or sma is None:
        return _invalid("invalid_indicator_history", decision_time, stochastic, sma, spot, sigma, distance)
    raw_signal = (stochastic + sma > 0) - (stochastic + sma < 0)
    qualifies = bool(raw_signal and distance is not None and distance >= 1.0)
    return {
        "valid": True,
        "reason": None if qualifies else ("distance_below_threshold" if raw_signal else "zero_raw_signal"),
        "stochastic": stochastic,
        "sma": sma,
        "raw_signal": raw_signal,
        "signal": raw_signal if qualifies else 0,
        "qualifies": qualifies,
        "spot": spot,
        "sigma5m": sigma,
        "distance": distance,
        "decision_time": decision_time,
    }


def _invalid(reason: str, decision_time: str | None, stochastic: int | None, sma: int | None,
             spot: float | None, sigma: float | None, distance: float | None) -> dict[str, Any]:
    return {
        "valid": False,
        "reason": reason,
        "stochastic": stochastic,
        "sma": sma,
        "raw_signal": None,
        "signal": 0,
        "qualifies": False,
        "spot": spot,
        "sigma5m": sigma,
        "distance": distance,
        "decision_time": decision_time,
    }
