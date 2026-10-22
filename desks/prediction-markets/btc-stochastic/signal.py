"""Pure, deterministic BTC spot stochastic signals from closed Coinbase candles.

Input candles are dictionaries with start/open/high/low/close/volume, where
start is a UTC epoch timestamp. This module never fetches data or models fills.
"""
from __future__ import annotations

from collections import defaultdict
import math
from typing import Any, Iterable

BASE_INTERVAL_SECONDS = 900
TIMEFRAMES = (15, 30, 60)
OVERSOLD = 20.0
OVERBOUGHT = 80.0


class CandleValidationError(ValueError):
    """Input candles violate the chronological OHLCV contract."""


def validate_candles(
    candles: Iterable[dict[str, Any]], now: float, interval_seconds: int = BASE_INTERVAL_SECONDS
) -> list[dict[str, float]]:
    """Validate candles and return only closed, continuous bars.

    A candle is closed at ``start + interval_seconds <= now``. Timestamps must
    be strictly increasing, UTC interval-aligned, and continuous across every
    supplied row, including an unclosed final row.
    """
    if not math.isfinite(float(now)):
        raise CandleValidationError("now must be a finite epoch timestamp")
    if interval_seconds <= 0:
        raise CandleValidationError("interval_seconds must be positive")
    rows: list[dict[str, float]] = []
    previous_start: float | None = None
    required = ("start", "open", "high", "low", "close", "volume")
    for index, raw in enumerate(candles):
        if not isinstance(raw, dict) or any(key not in raw for key in required):
            raise CandleValidationError(f"row {index} must contain {', '.join(required)}")
        try:
            row = {key: float(raw[key]) for key in required}
        except (TypeError, ValueError, OverflowError) as exc:
            raise CandleValidationError(f"row {index} contains a non-numeric value") from exc
        if not all(math.isfinite(value) for value in row.values()):
            raise CandleValidationError(f"row {index} contains a non-finite value")
        start = row["start"]
        if not start.is_integer():
            raise CandleValidationError(f"row {index} start must be an integer epoch second")
        if int(start) % interval_seconds:
            raise CandleValidationError(f"row {index} start is not UTC interval-aligned")
        if previous_start is not None:
            if start <= previous_start:
                raise CandleValidationError("timestamps must be sorted and unique")
            if start - previous_start != interval_seconds:
                raise CandleValidationError("candles must be continuous")
        previous_start = start
        o, h, low, close, volume = (row[k] for k in ("open", "high", "low", "close", "volume"))
        if min(o, h, low, close) <= 0:
            raise CandleValidationError(f"row {index} OHLC values must be positive")
        if volume < 0:
            raise CandleValidationError(f"row {index} volume must be non-negative")
        if low > min(o, close) or h < max(o, close) or low > h:
            raise CandleValidationError(f"row {index} has an invalid OHLC range")
        rows.append(row)
    return [row for row in rows if row["start"] + interval_seconds <= now]


def aggregate_candles(
    candles: Iterable[dict[str, float]], timeframe_minutes: int
) -> list[dict[str, float]]:
    """Aggregate continuous 15-minute bars into complete UTC 15/30/60m bars."""
    if timeframe_minutes not in TIMEFRAMES:
        raise ValueError(f"timeframe_minutes must be one of {TIMEFRAMES}")
    source = list(candles)
    step = timeframe_minutes * 60
    expected = step // BASE_INTERVAL_SECONDS
    buckets: dict[int, list[dict[str, float]]] = defaultdict(list)
    for row in source:
        start = int(row["start"])
        bucket_start = (start // step) * step
        buckets[bucket_start].append(row)
    result: list[dict[str, float]] = []
    for bucket_start in sorted(buckets):
        parts = buckets[bucket_start]
        expected_starts = [bucket_start + i * BASE_INTERVAL_SECONDS for i in range(expected)]
        if [int(row["start"]) for row in parts] != expected_starts:
            continue
        result.append({
            "start": float(bucket_start),
            "open": parts[0]["open"],
            "high": max(row["high"] for row in parts),
            "low": min(row["low"] for row in parts),
            "close": parts[-1]["close"],
            "volume": sum(row["volume"] for row in parts),
        })
    return result


def stochastic(
    candles: Iterable[dict[str, float]], interval_seconds: int = BASE_INTERVAL_SECONDS
) -> list[dict[str, float]]:
    """Compute closed-bar STOCH(14,3,3): fast %K, smoothed %K, and %D.

    Points start once the 14-bar range and both three-bar smoothers exist.
    Flat 14-bar ranges are undefined and are skipped until a valid point returns.
    """
    bars = list(candles)
    fast_values: list[float | None] = []
    for i, bar in enumerate(bars):
        if i < 13:
            fast_values.append(None)
            continue
        window = bars[i - 13 : i + 1]
        highest = max(row["high"] for row in window)
        lowest = min(row["low"] for row in window)
        denominator = highest - lowest
        fast_values.append(
            None if denominator <= 0 else 100.0 * (bar["close"] - lowest) / denominator
        )

    smoothed: list[float | None] = []
    for i, fast in enumerate(fast_values):
        window = fast_values[i - 2 : i + 1] if i >= 2 else []
        smoothed.append(
            sum(window) / 3.0 if len(window) == 3 and all(v is not None for v in window) else None
        )
    result = []
    for i, k in enumerate(smoothed):
        window = smoothed[i - 2 : i + 1] if i >= 2 else []
        if k is None or len(window) != 3 or any(v is None for v in window):
            continue
        start = int(bars[i]["start"])
        result.append({
            "bar_start": start,
            "close_timestamp": start + interval_seconds,
            "fast_k": float(fast_values[i]),
            "k": k,
            "d": sum(window) / 3.0,
        })
    return result


def signals_for_timeframe(
    candles_15m: Iterable[dict[str, Any]],
    now: float,
    timeframe_minutes: int = 30,
    method: str = "turn",
    oversold: float = OVERSOLD,
    direction: str = "bullish",
    overbought: float = OVERBOUGHT,
) -> list[dict[str, Any]]:
    """Return signal events for one timeframe/method from 15m source candles.

    Bullish ``turn`` requires previous K <= oversold and current K > previous K.
    Bearish ``turn`` requires previous K >= overbought and current K < previous K.
    ``cross`` adds the matching mirrored K/D crossover.
    Signal identifiers are deterministic across repeated evaluation.
    """
    if method not in ("turn", "cross"):
        raise ValueError("method must be 'turn' or 'cross'")
    if direction not in ("bullish", "bearish"):
        raise ValueError("direction must be 'bullish' or 'bearish'")
    if timeframe_minutes not in TIMEFRAMES:
        raise ValueError(f"timeframe_minutes must be one of {TIMEFRAMES}")
    if not math.isfinite(float(oversold)):
        raise ValueError("oversold must be finite")
    if not math.isfinite(float(overbought)):
        raise ValueError("overbought must be finite")
    closed = validate_candles(candles_15m, now)
    bars = aggregate_candles(closed, timeframe_minutes)
    points = stochastic(bars, timeframe_minutes * 60)
    result = []
    for previous, current in zip(points, points[1:]):
        # Undefined oscillator windows are omitted. Do not let a later point
        # bridge that gap and masquerade as the immediately previous bar.
        if current["close_timestamp"] - previous["close_timestamp"] != timeframe_minutes * 60:
            continue
        if direction == "bullish":
            turn = previous["k"] <= oversold and current["k"] > previous["k"]
            cross = current["k"] > current["d"] and previous["k"] <= previous["d"]
            threshold = float(oversold)
            side = "yes"
        else:
            turn = previous["k"] >= overbought and current["k"] < previous["k"]
            cross = current["k"] < current["d"] and previous["k"] >= previous["d"]
            threshold = float(overbought)
            side = "no"
        if not turn or (method == "cross" and not cross):
            continue
        close_ts = int(current["close_timestamp"])
        result.append({
            "signal_id": f"btc-stoch-{timeframe_minutes}m-{direction}-{method}-{close_ts}" if direction == "bearish" else f"btc-stoch-{timeframe_minutes}m-{method}-{close_ts}",
            "instrument": "BTC-USD",
            "timeframe_minutes": timeframe_minutes,
            "method": method,
            "direction": direction,
            "contract_side": side,
            "bar_close_timestamp": close_ts,
            "previous_k": previous["k"],
            "previous_d": previous["d"],
            "k": current["k"],
            "d": current["d"],
            "oversold": float(oversold) if direction == "bullish" else None,
            "overbought": float(overbought) if direction == "bearish" else None,
            "threshold": threshold,
            "valid": True,
        })
    return result


def ensure_unique_signal_ids(signals: Iterable[dict[str, Any]]) -> None:
    """Raise when a caller attempts to evaluate the same signal ID twice."""
    seen: set[str] = set()
    for signal in signals:
        signal_id = signal.get("signal_id")
        if not isinstance(signal_id, str) or not signal_id:
            raise ValueError("every signal must have a non-empty signal_id")
        if signal_id in seen:
            raise ValueError(f"duplicate signal_id: {signal_id}")
        seen.add(signal_id)


def scan_signals(candles_15m: Iterable[dict[str, Any]], now: float) -> dict[str, Any]:
    """Evaluate the fixed 15/30/60m x turn/cross experiment set."""
    source = validate_candles(candles_15m, now)
    evaluations = []
    for timeframe in TIMEFRAMES:
        for method in ("turn", "cross"):
            found = signals_for_timeframe(source, now, timeframe, method)
            evaluations.append({
                "timeframe_minutes": timeframe,
                "method": method,
                "signals": found,
                "abstain_reason": "no qualifying closed-bar event" if not found else None,
            })
    return {
        "instrument": "BTC-USD",
        "source_interval_minutes": 15,
        "evaluated_through_timestamp": int(source[-1]["start"] + BASE_INTERVAL_SECONDS) if source else None,
        "primary": {"timeframe_minutes": 30, "method": "turn", "oversold": OVERSOLD},
        "evaluations": evaluations,
    }
