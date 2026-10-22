"""Chronological BTC spot-direction diagnostics for stochastic signal variants.

The target is BTC close-to-close return over the next 15-minute candle. It is
not a prediction-market settlement, simulated trade, or profit estimate.
"""
from __future__ import annotations

import importlib.util
import math
from pathlib import Path
from typing import Any, Iterable

try:  # Works as part of a package when an integration gives it one.
    from .signal import BASE_INTERVAL_SECONDS, TIMEFRAMES, signals_for_timeframe, validate_candles
except ImportError:  # The desk path contains a hyphen and can be loaded standalone.
    _spec = importlib.util.spec_from_file_location("btc_stochastic_signal", Path(__file__).with_name("signal.py"))
    if _spec is None or _spec.loader is None:
        raise ImportError("could not load sibling signal.py")
    _signal = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_signal)
    BASE_INTERVAL_SECONDS = _signal.BASE_INTERVAL_SECONDS
    TIMEFRAMES = _signal.TIMEFRAMES
    signals_for_timeframe = _signal.signals_for_timeframe
    validate_candles = _signal.validate_candles

METHODS = ("turn", "cross")
VARIANTS = tuple((timeframe, method) for timeframe in TIMEFRAMES for method in METHODS)
MINIMUM_HOLDOUT_SIGNALS = 10


def wilson_interval(successes: int, count: int, z: float = 1.96) -> dict[str, float] | None:
    """Return the two-sided Wilson score interval, or None for no observations."""
    if count == 0:
        return None
    if count < 0 or successes < 0 or successes > count or z <= 0:
        raise ValueError("invalid Wilson interval inputs")
    p = successes / count
    denominator = 1.0 + z * z / count
    center = (p + z * z / (2.0 * count)) / denominator
    margin = z * math.sqrt(p * (1.0 - p) / count + z * z / (4.0 * count * count)) / denominator
    return {"lower": max(0.0, center - margin), "upper": min(1.0, center + margin)}


def _summary(rows: list[dict[str, Any]], minimum_signals: int) -> dict[str, Any]:
    count = len(rows)
    correct = sum(row["direction_correct"] for row in rows)
    flats = sum(row["flat_as_loss"] for row in rows)
    return {
        "signals": count,
        "correct_up_directions": correct,
        "flat_targets_counted_as_losses": flats,
        "accuracy": correct / count if count else None,
        "wilson_95": wilson_interval(correct, count),
        "status": "insufficient_holdout" if count < minimum_signals else "diagnostic_only",
    }


def run_research(
    candles_15m: Iterable[dict[str, Any]],
    now: float,
    minimum_holdout_signals: int = MINIMUM_HOLDOUT_SIGNALS,
) -> dict[str, Any]:
    """Evaluate fixed signal variants on a chronological 70/30 BTC spot split.

    Each signal is labeled once by the immediately following 15m candle. Flat
    close-to-close moves are losses for this bullish directional diagnostic and
    remain in its accuracy denominator. Signal IDs must be unique.
    """
    if minimum_holdout_signals < 1:
        raise ValueError("minimum_holdout_signals must be positive")
    candles = validate_candles(candles_15m, now)
    if len(candles) < 2:
        raise ValueError("at least two closed 15-minute candles are required")
    # Split by chronology of closed source bars, leaving the later 30% untouched
    # by the training summary. The split itself does not select a winner.
    split_index = max(1, min(len(candles) - 1, math.floor(len(candles) * 0.70)))
    split_timestamp = int(candles[split_index]["start"])
    by_start = {int(row["start"]): row for row in candles}
    results: dict[str, Any] = {}
    all_ids: set[str] = set()

    for timeframe, method in VARIANTS:
        signals = signals_for_timeframe(candles, now, timeframe, method)
        labeled = []
        for signal in signals:
            signal_id = signal["signal_id"]
            if signal_id in all_ids:
                raise ValueError(f"duplicate signal_id: {signal_id}")
            all_ids.add(signal_id)
            target_start = signal["bar_close_timestamp"]
            prior_start = target_start - BASE_INTERVAL_SECONDS
            prior = by_start.get(prior_start)
            target = by_start.get(target_start)
            # An aggregated signal closes exactly where its next spot candle
            # opens. Missing target rows are not relabeled to a later candle.
            if prior is None or target is None:
                continue
            change = target["close"] / prior["close"] - 1.0
            flat = change == 0.0
            labeled.append({
                **signal,
                "target_start_timestamp": target_start,
                "target_close_timestamp": target_start + BASE_INTERVAL_SECONDS,
                "target_return": change,
                "target_direction": "up" if change > 0 else ("flat" if flat else "down"),
                "direction_correct": change > 0,
                "flat_as_loss": flat,
            })
        train = [
            row for row in labeled
            if row["bar_close_timestamp"] < split_timestamp
            and row["target_close_timestamp"] <= split_timestamp
        ]
        holdout = [row for row in labeled if row["bar_close_timestamp"] >= split_timestamp]
        results[f"{timeframe}m_{method}"] = {
            "timeframe_minutes": timeframe,
            "method": method,
            "labeled_signals": len(labeled),
            "train": _summary(train, 1),
            "holdout": _summary(holdout, minimum_holdout_signals),
            "purged_at_split": len(labeled) - len(train) - len(holdout),
            "signals": labeled,
        }

    return {
        "instrument": "BTC-USD",
        "source": "15-minute spot candles",
        "target": "close-to-close return from signal bar close to next 15-minute candle close",
        "target_is_venue_pnl": False,
        "split": {"train_fraction": 0.70, "holdout_fraction": 0.30, "first_holdout_start_timestamp": split_timestamp},
        "primary_variant": "30m_turn",
        "winner_selected_from_holdout": False,
        "minimum_holdout_signals": minimum_holdout_signals,
        "variants": results,
    }
