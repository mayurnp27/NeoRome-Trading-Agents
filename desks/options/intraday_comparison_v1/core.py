#!/usr/bin/env python3
"""Local-only paper signal and book core. No network or order functions.

CLI:
  python core.py signals --bars bars.json --asof ISO --config arm.json
  python core.py quote --snapshot observation.json --state arm-state.json --config arm.json
  python core.py status --state arm-state.json --config arm.json

Bars: {"results": [{"symbol": "SPY", "bars": [{"begins_at": ISO,
"open_price": 1, "high_price": 2, "low_price": 1, "close_price": 2,
"volume": 10, "interpolated": false, "session": "reg"}]}]}.
Input bars MUST be genuine five-minute intervals. Plain open/high/low/close
field names are also accepted. Timestamps require an explicit UTC offset.

Observation: {"asof": ISO, "signals": [{"id": "...", "strategy_id": "A",
"symbol": "SPY", "direction": "call", "signal_time": ISO,
"expiry": "2026-10-16"}], "catalog": [...], "underlying_quotes": [...], "quotes": [{"option_id": "...", "symbol":
"SPY", "expiry": "2026-10-16", "type": "call", "strike": 650,
"multiplier": 100, "bid": 1.00, "ask": 1.02, "bid_size": 1,
"ask_size": 1, "delta": 0.50, "updated_at": ISO}]}.
The catalog must contain all listed contracts for the signal's symbol and
eligible expiry. Each book processes only its own strategy_id from signals.
The SMA book uses completed five-minute closes; stochastic books retain their
configured stochastic crossover and optional directional veto rules.
Entries use the exact nearest OTM contract, an observed ask and whole-contract
quantity; exits use fresh observed bids and displayed depth. State and audit
files are independent per book, with atomic writes, locking and idempotency.
"""

from __future__ import annotations

import argparse
import copy
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo


DEFAULTS = {
    "strategy_id": "A",
    "book_key": "A20",
    "signal_type": "stochastic_cross",
    "symbols": ["SPY", "IWM", "QQQ", "AAPL", "MSFT", "AMZN", "GOOGL", "META", "NVDA", "TSLA"],
    "timezone": "America/New_York",
    "session_dates": ["2026-10-15", "2026-10-16", "2026-10-19", "2026-10-20", "2026-10-21", "2026-10-22", "2026-10-23", "2026-10-26", "2026-10-27", "2026-10-28", "2026-10-29", "2026-10-30", "2026-11-02", "2026-11-03", "2026-11-04", "2026-11-05", "2026-11-06", "2026-11-09", "2026-11-10", "2026-11-11"],
    "initial_cash": 1000.0,
    "fee_per_side": 0.10,
    "entry_start": "09:35",
    "entry_end": "10:50",
    "flat_time": "11:00",
    "take_profit_fraction": 0.20,
    "stop_loss_fraction": 0.20,
    "max_quote_age_seconds": 15,
    "signal_max_age_seconds": 90,
    "max_spread_absolute": 0.10,
    "max_spread_mid_fraction": 0.05,
    "max_position_equity_fraction": 0.40,
    "sizing_policy": "equity_fraction",
    "directional_filter_timeframes": [],
}


def parse_time(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("Timestamp must have an explicit timezone: " + value)
    return result


def stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def money(value: float) -> float:
    return round(value, 6)


def load_config(path: str | Path | dict) -> dict:
    raw = path if isinstance(path, dict) else json.loads(Path(path).read_text())
    config = copy.deepcopy(DEFAULTS)
    config.update({key: value for key, value in raw.items() if key in DEFAULTS or key == "session_dates"})
    if config["signal_type"] not in ("stochastic_cross", "sma_cross"):
        raise ValueError("Invalid signal_type")
    if config["signal_type"] == "stochastic_cross":
        stochastic_defaults = {"length": 5, "smooth_k": 3, "smooth_d": 3} if config["book_key"] == "B20" else {"length": 14, "smooth_k": 3, "smooth_d": 3}
        config["stochastic"] = {**stochastic_defaults, **raw.get("stochastic", {})}
        for key in ("length", "smooth_k", "smooth_d"):
            value = config["stochastic"][key]
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ValueError("Stochastic settings must be positive integers")
    else:
        if "stochastic" in raw or config["directional_filter_timeframes"]:
            raise ValueError("SMA strategy cannot configure stochastic filters")
        config["directional_filter_timeframes"] = []
        config["sma_fast"] = raw.get("sma_fast", 2)
        config["sma_slow"] = raw.get("sma_slow", 5)
        config["timeframe_minutes"] = raw.get("timeframe_minutes", 5)
        if any(isinstance(value, bool) or not isinstance(value, int) or value < 1 for value in (config["sma_fast"], config["sma_slow"])) or config["sma_fast"] >= config["sma_slow"]:
            raise ValueError("SMA periods must be positive integers with fast < slow")
        if config["timeframe_minutes"] != 5:
            raise ValueError("SMA timeframe_minutes must be 5")
        config.pop("stochastic", None)
    if config["book_key"] not in ("A20", "B20", "SMA"):
        raise ValueError("Invalid book_key")
    filter_frames = config["directional_filter_timeframes"]
    if (not isinstance(filter_frames, list) or any(isinstance(value, bool) or not isinstance(value, int) or value not in (5, 15, 30) for value in filter_frames) or len(set(filter_frames)) != len(filter_frames)):
        raise ValueError("directional_filter_timeframes must be a list of unique integer timeframes from 5, 15, 30")
    ZoneInfo(config["timezone"])
    for key in ("entry_start", "entry_end", "flat_time"):
        time.fromisoformat(config[key])
    if not (config["entry_start"] <= config["entry_end"] < config["flat_time"]):
        raise ValueError("Entry window must end before forced flat time")
    for key in DEFAULTS:
        if isinstance(DEFAULTS[key], (int, float)):
            value = config[key]
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                raise ValueError("Invalid nonnegative numeric config: " + key)
    if not 0 < config["stop_loss_fraction"] < 1 or config["take_profit_fraction"] <= 0:
        raise ValueError("Invalid stop/target fractions")
    if not 0 < config["max_position_equity_fraction"] <= 1:
        raise ValueError("Invalid position equity fraction")
    if config["sizing_policy"] not in ("equity_fraction", "equal_cash_executable"):
        raise ValueError("Invalid sizing policy")
    if "session_dates" in config:
        config["session_dates"] = sorted(set(config["session_dates"]))
        for value in config["session_dates"]:
            date.fromisoformat(value)
    return config


def allowed_day(day: date, config: dict) -> bool:
    if "session_dates" in config:
        return day.isoformat() in config["session_dates"]
    return day.weekday() < 5 and date(2026, 10, 15) <= day <= date(2026, 11, 11)


def local_deadline(day: date, clock: str, config: dict) -> datetime:
    return datetime.combine(day, time.fromisoformat(clock), ZoneInfo(config["timezone"]))


def entry_window(asof: datetime, config: dict) -> bool:
    local = asof.astimezone(ZoneInfo(config["timezone"]))
    return allowed_day(local.date(), config) and local_deadline(local.date(), config["entry_start"], config) <= asof <= local_deadline(local.date(), config["entry_end"], config)


def normalized_bars(rows: list, asof: datetime, config: dict) -> tuple[list, list]:
    """Filter to available completed genuine RTH five-minute bars, deduplicate."""
    tz = ZoneInfo(config["timezone"])
    bars = {}
    issues = []
    for row in rows:
        try:
            start = parse_time(row["begins_at"]).astimezone(tz)
            if start + timedelta(minutes=5) > asof:
                continue
            if row.get("interpolated", False):
                issues.append({"begins_at": stamp(start), "reason": "interpolated_bar"})
                continue
            session = str(row.get("session", "reg")).lower()
            if session not in ("reg", "regular", "rth") or start.weekday() > 4:
                continue
            if not (time(9, 30) <= start.time().replace(tzinfo=None) < time(16)):
                continue
            if start.second or start.microsecond or start.minute % 5:
                raise ValueError("Bar is not aligned to a five-minute interval")
            values = {}
            for key in ("open", "high", "low", "close"):
                value = float(row[key] if key in row else row[key + "_price"])
                if not math.isfinite(value) or value <= 0:
                    raise ValueError("OHLC must be positive and finite")
                values[key] = value
            if values["high"] < max(values["open"], values["close"], values["low"]) or values["low"] > min(values["open"], values["close"], values["high"]):
                raise ValueError("OHLC bounds are inconsistent")
            volume = float(row.get("volume", 0))
            if not math.isfinite(volume) or volume < 0:
                raise ValueError("Invalid volume")
            bar = {"start": start, "end": start + timedelta(minutes=5), **values, "volume": volume}
            if start in bars and bars[start] != bar:
                raise ValueError("Conflicting duplicate bar")
            bars[start] = bar
        except (KeyError, ValueError, TypeError) as exc:
            issues.append({"begins_at": str(row.get("begins_at")), "reason": str(exc)})
    return sorted(bars.values(), key=lambda bar: bar["start"]), issues


def aggregate_bars(bars: list, minutes: int, asof: datetime) -> list:
    buckets = {}
    for bar in bars:
        local = bar["start"]
        elapsed = local.hour * 60 + local.minute - 570
        beginning = local.replace(hour=9, minute=30, second=0, microsecond=0) + timedelta(minutes=(elapsed // minutes) * minutes)
        buckets.setdefault(beginning, []).append(bar)
    result = []
    for beginning, group in sorted(buckets.items()):
        group.sort(key=lambda bar: bar["start"])
        expected = min(minutes // 5, int((asof - beginning).total_seconds() // 300))
        if expected < 1 or len(group) != expected:
            continue
        if any(bar["start"] != beginning + timedelta(minutes=5 * i) for i, bar in enumerate(group)):
            continue
        result.append({"start": beginning, "end": group[-1]["end"], "open": group[0]["open"], "high": max(bar["high"] for bar in group), "low": min(bar["low"] for bar in group), "close": group[-1]["close"], "volume": sum(bar["volume"] for bar in group), "developing": expected < minutes // 5})
    return result


def stochastic_series(bars: list, settings: dict) -> list:
    length, smooth_k, smooth_d = (settings[key] for key in ("length", "smooth_k", "smooth_d"))
    raw = []
    slow_k = []
    output = []
    for i, bar in enumerate(bars):
        value = None
        if i >= length - 1:
            lookback = bars[i - length + 1:i + 1]
            low = min(item["low"] for item in lookback)
            high = max(item["high"] for item in lookback)
            # Flat high-low ranges have no directional evidence; never synthesize.
            if high > low:
                value = 100 * (bar["close"] - low) / (high - low)
        raw.append(value)
        k_window = raw[max(0, i - smooth_k + 1):i + 1]
        k = sum(k_window) / smooth_k if len(k_window) == smooth_k and all(item is not None for item in k_window) else None
        slow_k.append(k)
        d_window = slow_k[max(0, i - smooth_d + 1):i + 1]
        d = sum(d_window) / smooth_d if len(d_window) == smooth_d and all(item is not None for item in d_window) else None
        output.append({"k": k, "d": d})
    return output


def indicator_snapshot(bars: list, settings: dict) -> dict | None:
    series = stochastic_series(bars, settings)
    if len(series) < 2 or any(value is None for item in series[-2:] for value in item.values()):
        return None
    return {"k": series[-1]["k"], "d": series[-1]["d"], "prior_k": series[-2]["k"], "prior_d": series[-2]["d"], "begins_at": stamp(bars[-1]["start"]), "available_through": stamp(bars[-1]["end"]), "developing": bars[-1].get("developing", False)}


def continuous_indicator_window(bars: list, minutes: int, required: int) -> bool:
    """Missing observations cannot silently shorten the stochastic lookback."""
    window = bars[-required:]
    for previous, following in zip(window, window[1:]):
        if previous["start"].date() == following["start"].date():
            if previous["start"] + timedelta(minutes=minutes) != following["start"]:
                return False
        elif previous["start"].time().replace(tzinfo=None) != (datetime.combine(date(2000, 1, 1), time(16)) - timedelta(minutes=minutes)).time() or following["start"].time().replace(tzinfo=None) != time(9, 30):
            return False
    return True


def directional_filter_decision(direction: str, indicators: dict, config: dict) -> dict:
    """Return a fail-closed directional veto decision and auditable evidence."""
    frames = config["directional_filter_timeframes"]
    evidence = []
    for minutes in frames:
        key = str(minutes)
        indicator = indicators.get(key) if isinstance(indicators, dict) else None
        current_k = indicator.get("k") if isinstance(indicator, dict) else None
        if isinstance(current_k, bool) or not isinstance(current_k, (int, float)) or not math.isfinite(current_k) or not 0 <= current_k <= 100:
            return {"accepted": False, "reason": "invalid_directional_filter_indicator", "evidence": [{"timeframe_minutes": minutes, "current_k": current_k, "valid": False}]}
        threshold = 80 if direction == "call" else 20
        veto = current_k >= threshold if direction == "call" else current_k <= threshold
        evidence.append({"timeframe_minutes": minutes, "current_k": current_k, "comparison": ">=" if direction == "call" else "<=", "threshold": threshold, "veto": veto})
        if veto:
            return {"accepted": False, "reason": "directional_stochastic_veto", "evidence": evidence}
    return {"accepted": True, "reason": None, "evidence": evidence}


def signals(document: dict, asof: datetime, config: dict) -> dict:
    output = {"asof": stamp(asof), "strategy_id": config["strategy_id"], "signal_type": config["signal_type"], "entry_window_open": entry_window(asof, config), "candidates": [], "evaluations": []}
    if config["signal_type"] == "sma_cross":
        return _sma_signals(document, asof, config, output)
    output["stochastic"] = config["stochastic"]
    records = {item["symbol"].upper(): item for item in document.get("results", [])}
    required = sum(config["stochastic"].values()) - 1  # current plus prior %D
    for symbol in config["symbols"]:
        record = records.get(symbol, {})
        if record.get("interval", "5minute") not in ("5minute", "5minutes", "5m"):
            output["evaluations"].append({"symbol": symbol, "reason": "unsupported_source_interval", "issues": [], "required_bars_per_timeframe": required})
            continue
        bars, issues = normalized_bars(record.get("bars", []), asof, config)
        evaluation = {"symbol": symbol, "issues": issues, "required_bars_per_timeframe": required}
        if not bars:
            evaluation["reason"] = "no_completed_rth_bars"
            output["evaluations"].append(evaluation)
            continue
        frames = {5: bars, 15: aggregate_bars(bars, 15, asof), 30: aggregate_bars(bars, 30, asof)}
        snapshots = {str(minutes): indicator_snapshot(frame, config["stochastic"]) for minutes, frame in frames.items()}
        evaluation["indicators"] = snapshots
        if any(value is None for value in snapshots.values()):
            evaluation["reason"] = "insufficient_indicator_warmup"
        elif any(frame[-1]["end"] != bars[-1]["end"] for frame in frames.values()):
            evaluation["reason"] = "missing_current_higher_timeframe_components"
        elif any(not continuous_indicator_window(frame, minutes, required) for minutes, frame in frames.items()):
            evaluation["reason"] = "missing_bar_in_indicator_window"
        elif (asof - bars[-1]["end"]).total_seconds() > config["signal_max_age_seconds"]:
            evaluation["reason"] = "stale_five_minute_bar"
        else:
            five = snapshots["5"]
            bullish = five["prior_k"] <= five["prior_d"] and five["k"] > five["d"]
            bearish = five["prior_k"] >= five["prior_d"] and five["k"] < five["d"]
            call_bias = all(snapshots[str(minutes)]["k"] > snapshots[str(minutes)]["d"] and snapshots[str(minutes)]["k"] >= snapshots[str(minutes)]["prior_k"] for minutes in (15, 30))
            put_bias = all(snapshots[str(minutes)]["k"] < snapshots[str(minutes)]["d"] and snapshots[str(minutes)]["k"] <= snapshots[str(minutes)]["prior_k"] for minutes in (15, 30))
            direction = "call" if bullish and call_bias else "put" if bearish and put_bias else None
            evaluation.update({"bullish_cross": bullish, "bearish_cross": bearish, "call_bias": call_bias, "put_bias": put_bias})
            filter_decision = directional_filter_decision(direction, snapshots, config) if direction else {"accepted": True, "reason": None, "evidence": []}
            if direction:
                evaluation["directional_filter"] = filter_decision
            if not direction:
                evaluation["reason"] = "no_aligned_crossover"
            elif not filter_decision["accepted"]:
                evaluation["reason"] = filter_decision["reason"]
            elif not entry_window(asof, config):
                evaluation["reason"] = "outside_entry_window_or_pilot"
            else:
                signal_time = bars[-1]["end"]
                valid_until = min(signal_time + timedelta(seconds=config["signal_max_age_seconds"]), local_deadline(asof.astimezone(ZoneInfo(config["timezone"])).date(), config["entry_end"], config))
                signal_id = config["strategy_id"] + ":" + digest([config["strategy_id"], config["stochastic"], symbol, stamp(signal_time), direction])[:24]
                zone = "oversold" if five["prior_k"] <= 20 else "overbought" if five["prior_k"] >= 80 else "middle"
                candidate = {"id": signal_id, "strategy_id": config["strategy_id"], "symbol": symbol, "direction": direction, "signal_time": stamp(signal_time), "observed_at": stamp(asof), "valid_until": stamp(valid_until), "prior_five_minute_zone": zone, "indicators": snapshots, "directional_filter_evidence": filter_decision["evidence"]}
                output["candidates"].append(candidate)
                evaluation["reason"] = "candidate"
                evaluation["signal_id"] = signal_id
        output["evaluations"].append(evaluation)
    return output


def _sma_signals(document: dict, asof: datetime, config: dict, output: dict) -> dict:
    """Generate SMA crosses from exactly six continuous completed RTH bars."""
    records = {item["symbol"].upper(): item for item in document.get("results", [])}
    required = config["sma_slow"] + 1
    fast, slow = config["sma_fast"], config["sma_slow"]
    for symbol in config["symbols"]:
        record = records.get(symbol, {})
        if record.get("interval", "5minute") not in ("5minute", "5minutes", "5m"):
            output["evaluations"].append({"symbol": symbol, "reason": "unsupported_source_interval", "issues": [], "required_bars": required})
            continue
        bars, issues = normalized_bars(record.get("bars", []), asof, config)
        evaluation = {"symbol": symbol, "issues": issues, "required_bars": required}
        if len(bars) < required:
            evaluation["reason"] = "insufficient_sma_warmup"
            output["evaluations"].append(evaluation)
            continue
        window = bars[-required:]
        if not continuous_indicator_window(window, 5, required):
            evaluation["reason"] = "missing_bar_in_sma_window"
            output["evaluations"].append(evaluation)
            continue
        if (asof - window[-1]["end"]).total_seconds() > config["signal_max_age_seconds"]:
            evaluation["reason"] = "stale_five_minute_bar"
            output["evaluations"].append(evaluation)
            continue
        closes = [bar["close"] for bar in window]
        prior_fast = sum(closes[-fast-1:-1]) / fast
        prior_slow = sum(closes[:-1]) / slow
        current_fast = sum(closes[-fast:]) / fast
        current_slow = sum(closes[-slow:]) / slow
        bullish = prior_fast <= prior_slow and current_fast > current_slow
        bearish = prior_fast >= prior_slow and current_fast < current_slow
        direction = "call" if bullish else "put" if bearish else None
        evidence = {"timeframe_minutes": 5, "sma_fast_period": fast, "sma_slow_period": slow,
                    "prior_fast": prior_fast, "prior_slow": prior_slow,
                    "current_fast": current_fast, "current_slow": current_slow,
                    "cross": "above" if bullish else "below" if bearish else None,
                    "bar_start": stamp(window[-1]["start"]), "bar_end": stamp(window[-1]["end"])}
        evaluation.update({"sma": evidence, "bullish_cross": bullish, "bearish_cross": bearish})
        if not direction:
            evaluation["reason"] = "no_sma_crossover"
        elif not entry_window(asof, config):
            evaluation["reason"] = "outside_entry_window_or_pilot"
        else:
            signal_time = window[-1]["end"]
            valid_until = min(signal_time + timedelta(seconds=config["signal_max_age_seconds"]), local_deadline(asof.astimezone(ZoneInfo(config["timezone"])).date(), config["entry_end"], config))
            signal_id = config["strategy_id"] + ":" + digest([config["strategy_id"], symbol, stamp(signal_time), direction])[:24]
            candidate = {"id": signal_id, "strategy_id": config["strategy_id"], "symbol": symbol,
                         "direction": direction, "signal_time": stamp(signal_time), "observed_at": stamp(asof),
                         "valid_until": stamp(valid_until), "signal_type": "sma_cross", "sma_evidence": evidence}
            output["candidates"].append(candidate)
            evaluation["reason"] = "candidate"
            evaluation["signal_id"] = signal_id
        output["evaluations"].append(evaluation)
    return output


def new_state(config: dict) -> dict:
    return {"schema_version": 2, "strategy_id": config["strategy_id"], "config_fingerprint": digest(config), "initial_cash": config["initial_cash"], "cash": config["initial_cash"], "realized_pnl": 0.0, "positions": {}, "trades": [], "depth_consumed": {}, "signals_seen": {}, "processed_snapshots": {}, "pending_audit": []}


def local_day(asof: datetime, config: dict) -> date:
    return asof.astimezone(ZoneInfo(config["timezone"])).date()


def valid_entry_quote(quote: dict, asof: datetime, config: dict, signal_time: datetime) -> tuple[bool, str]:
    try:
        updated = parse_time(quote["updated_at"])
        age = (asof - updated).total_seconds()
        if age < 0 or age > config["max_quote_age_seconds"]:
            return False, "stale_or_future_quote"
        if updated <= signal_time:
            return False, "quote_not_after_signal"
        bid, ask = float(quote["bid"]), float(quote["ask"])
        if not all(math.isfinite(x) for x in (bid, ask)) or bid <= 0 or ask <= 0 or bid > ask:
            return False, "invalid_or_crossed_market"
        spread = ask - bid
        if spread > min(config["max_spread_absolute"], config["max_spread_mid_fraction"] * ((bid + ask) / 2)) + 1e-10:
            return False, "spread_too_wide"
        ask_size = int(quote["ask_size"])
        if ask_size < 1:
            return False, "insufficient_quote_size"
        if not quote.get("option_id") or int(quote["multiplier"]) != 100 or float(quote["multiplier"]) != 100:
            return False, "unsupported_contract_or_multiplier"
        return True, "valid"
    except (KeyError, ValueError, TypeError, OverflowError):
        return False, "malformed_quote"


def fresh_underlying(snapshot: dict, symbol: str, asof: datetime, config: dict) -> float | None:
    candidates = [q for q in snapshot.get("underlying_quotes", []) if q.get("symbol", "").upper() == symbol.upper()]
    candidates.sort(key=lambda q: q.get("updated_at", ""), reverse=True)
    for quote in candidates:
        try:
            updated = parse_time(quote["updated_at"])
            price = float(quote["price"])
            age = (asof - updated).total_seconds()
            if 0 <= age <= config["max_quote_age_seconds"] and math.isfinite(price) and price > 0:
                return price
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
    return None


def resolve_expiry(signal: dict, snapshot: dict, asof: datetime, config: dict) -> str | None:
    local = local_day(asof, config)
    monday = local - timedelta(days=local.weekday())
    friday = monday + timedelta(days=4)
    candidates = sorted({date.fromisoformat(str(row["expiry"])) for row in snapshot.get("catalog", []) if row.get("symbol", "").upper() == str(signal.get("symbol", "")).upper() and local <= date.fromisoformat(str(row["expiry"])) <= friday})
    if local.weekday() == 4:
        selected = local if local in candidates else None
    else:
        selected = candidates[1] if len(candidates) > 1 else candidates[0] if candidates else None
    explicit = signal.get("expiry")
    if explicit and selected and date.fromisoformat(str(explicit)) != selected:
        return None
    return selected.isoformat() if selected else None


def _fresh_position_quote(position: dict, snapshot: dict, asof: datetime, config: dict) -> tuple[dict | None, str]:
    matches = [q for q in snapshot.get("quotes", []) if q.get("option_id") == position["option_id"]]
    matches.sort(key=lambda q: q.get("updated_at", ""), reverse=True)
    for q in matches:
        try:
            updated = parse_time(q["updated_at"])
            age = (asof - updated).total_seconds()
            bid = float(q["bid"])
            if not 0 <= age <= config["max_quote_age_seconds"] or updated <= parse_time(position["entry_quote_time"]):
                continue
            if q.get("symbol") != position["symbol"] or q.get("expiry") != position["expiry"] or q.get("type") != position["type"] or float(q.get("strike")) != position["strike"] or int(q.get("multiplier")) != 100:
                continue
            if not math.isfinite(bid) or bid <= 0:
                continue
            size = int(q.get("bid_size", 0))
            if size < 0:
                continue
            return q, "valid"
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
    return None, "missing_stale_or_invalid_bid"


def available_quote_depth(state: dict, side: str, quote: dict) -> int:
    """Return displayed depth minus prior fills against this exact quote timestamp."""
    option_id = str(quote["option_id"])
    updated_at = stamp(parse_time(quote["updated_at"]))
    key = digest([side, option_id, updated_at])
    consumed = int(state.setdefault("depth_consumed", {}).get(key, 0))
    size_key = "ask_size" if side == "ask" else "bid_size"
    displayed = int(float(quote.get(size_key, 0)))
    return max(0, displayed - consumed)


def consume_quote_depth(state: dict, side: str, quote: dict, quantity: int) -> None:
    option_id = str(quote["option_id"])
    updated_at = stamp(parse_time(quote["updated_at"]))
    key = digest([side, option_id, updated_at])
    records = state.setdefault("depth_consumed", {})
    records[key] = int(records.get(key, 0)) + quantity


def _portfolio_equity(state: dict, marks: dict) -> float | None:
    total = float(state["cash"])
    for position_id, position in state["positions"].items():
        bid = marks.get(position_id)
        if bid is None:
            return None
        total += bid * position["quantity"] * 100
    return money(total)


def apply_snapshot(snapshot: dict, state: dict, config: dict) -> dict:
    """Apply one complete observation: manage all existing risk, then consider entries."""
    asof = parse_time(snapshot["asof"])
    local = asof.astimezone(ZoneInfo(config["timezone"]))
    events = []
    def event(kind: str, **values):
        events.append({"kind": kind, "asof": stamp(asof), "strategy_id": config["strategy_id"], **values})

    state.setdefault("depth_consumed", {})
    marks = {}
    # First mark and manage every held position from this same snapshot.
    for position_id, position in list(state["positions"].items()):
        quote, why = _fresh_position_quote(position, snapshot, asof, config)
        if quote is None:
            position["mark_status"] = why
            event("unresolved_mark", position_id=position_id, option_id=position["option_id"], reason=why)
            continue
        bid = float(quote["bid"])
        position.update({"last_bid": bid, "last_quote_time": quote["updated_at"], "last_observed_at": stamp(asof), "mark_status": "fresh"})
        marks[position_id] = bid
        change = bid / position["entry_ask"] - 1
        deadline = local_deadline(date.fromisoformat(position["entry_date"]), config["flat_time"], config)
        triggered_reason = "take_profit" if change >= config["take_profit_fraction"] - 1e-12 else "stop_loss" if change <= -config["stop_loss_fraction"] + 1e-12 else None
        if asof >= deadline:
            exit_reason = "time_exit"
        else:
            exit_reason = position.get("exit_latched_reason") or triggered_reason
        if exit_reason:
            position["exit_latched_reason"] = exit_reason
            available = available_quote_depth(state, "bid", quote)
            quantity = min(position["quantity"], available)
            if quantity > 0:
                consume_quote_depth(state, "bid", quote, quantity)
            if quantity <= 0:
                event("unresolved_exit_depth", position_id=position_id, option_id=position["option_id"], requested=position["quantity"], bid_size=available, reason=exit_reason)
                continue
            fee = money(config["fee_per_side"] * quantity)
            credit = money(bid * 100 * quantity - fee)
            entry_debit = money(position["unit_entry_debit"] * quantity)
            pnl = money(credit - entry_debit)
            trade = {**position, "quantity": quantity, "exit_time": stamp(asof), "exit_quote_time": quote["updated_at"], "exit_bid": bid, "exit_fee": fee, "exit_credit": credit, "net_pnl": pnl, "reason": exit_reason, "late_exit": asof > deadline, "execution_method": "observed_bid", "entry_debit": entry_debit, "entry_fee": money(config["fee_per_side"] * quantity)}
            state["cash"] = money(state["cash"] + credit)
            state["realized_pnl"] = money(state["realized_pnl"] + pnl)
            state["trades"].append(trade)
            remaining = position["quantity"] - quantity
            if remaining:
                position["quantity"] = remaining
                position["entry_debit"] = money(position["unit_entry_debit"] * remaining)
                position["entry_fee"] = money(config["fee_per_side"] * remaining)
                position["last_bid"] = bid
                marks[position_id] = bid
            else:
                del state["positions"][position_id]
                marks.pop(position_id, None)
            event("paper_exit", position_id=position_id, trade=trade, remaining_quantity=remaining, cash=state["cash"])
        else:
            event("paper_mark", position_id=position_id, option_id=position["option_id"], bid=bid, quantity=position["quantity"], unrealized_pnl=money((bid * 100 - position["unit_entry_debit"]) * position["quantity"]))

    equity = _portfolio_equity(state, marks)
    signals = snapshot.get("signals", [])
    if not isinstance(signals, list):
        raise ValueError("signals must be a list")
    deferred_entries = []
    for signal in signals:
        sid = str(signal.get("id", ""))
        prior = state["signals_seen"].get(sid)
        if prior is None:
            event("candidate", signal=signal)
        rejection = None
        filter_evidence = []
        try:
            signal_time = parse_time(signal["signal_time"])
            age = (asof - signal_time).total_seconds()
            if not sid or signal.get("strategy_id") != config["strategy_id"]:
                rejection = "invalid_signal_identity"
            elif signal.get("symbol", "").upper() not in [s.upper() for s in config["symbols"]] or signal.get("direction") not in ("call", "put"):
                rejection = "unsupported_signal"
            elif config.get("directional_filter_timeframes", []):
                filter_decision = directional_filter_decision(signal["direction"], signal.get("indicators"), config)
                filter_evidence = filter_decision["evidence"]
                if not filter_decision["accepted"]:
                    rejection = filter_decision["reason"]
            if not rejection and (signal_time.astimezone(ZoneInfo(config["timezone"])).date() != local.date() or age < 0 or age > config["signal_max_age_seconds"]):
                rejection = "stale_or_future_signal"
            if not rejection and (not entry_window(asof, config) or not entry_window(signal_time, config)):
                rejection = "outside_entry_window"
            if not rejection and prior and prior.get("status") == "entered":
                rejection = "signal_already_entered"
            expiry = resolve_expiry(signal, snapshot, asof, config)
            if not rejection and not expiry:
                rejection = "missing_or_invalid_weekly_expiry"
        except (KeyError, TypeError, ValueError):
            rejection = "malformed_signal"
        underlying = fresh_underlying(snapshot, signal.get("symbol", ""), asof, config) if not rejection else None
        if not rejection and underlying is None:
            rejection = "missing_or_stale_underlying_quote"
        selected_catalog = None
        if not rejection:
            catalog = [row for row in snapshot.get("catalog", []) if row.get("symbol", "").upper() == signal["symbol"].upper() and row.get("expiry") == expiry and row.get("type") == signal["direction"] and float(row.get("multiplier", 0)) == 100]
            try:
                strikes = [(float(row["strike"]), row) for row in catalog if math.isfinite(float(row["strike"]))]
                if signal["direction"] == "call":
                    otm = [(strike, row) for strike, row in strikes if strike > underlying]
                    selected_catalog = min(otm, key=lambda item: (item[0], str(item[1].get("option_id"))))[1] if otm else None
                else:
                    otm = [(strike, row) for strike, row in strikes if strike < underlying]
                    selected_catalog = max(otm, key=lambda item: (item[0], str(item[1].get("option_id"))))[1] if otm else None
            except (ValueError, TypeError, KeyError):
                selected_catalog = None
            if selected_catalog is None:
                rejection = "no_exact_one_strike_otm_contract"
        quote_rejections = []
        selected_quote = None
        if not rejection:
            matching = [q for q in snapshot.get("quotes", []) if q.get("option_id") == selected_catalog.get("option_id")]
            matching.sort(key=lambda q: q.get("updated_at", ""), reverse=True)
            for q in matching:
                valid, reason = valid_entry_quote(q, asof, config, signal_time)
                if valid and (q.get("symbol") != signal["symbol"] or q.get("expiry") != expiry or q.get("type") != signal["direction"] or float(q.get("strike")) != float(selected_catalog["strike"]) or float(q.get("multiplier", 0)) != 100):
                    valid, reason = False, "contract_metadata_mismatch"
                if valid:
                    selected_quote = q
                    break
                quote_rejections.append({"option_id": q.get("option_id"), "reason": reason})
            if selected_quote is None:
                rejection = "no_executable_exact_otm_quote"
        if not rejection and equity is None:
            rejection = "unknown_or_stale_position_mark"
        if not rejection and config["sizing_policy"] == "equal_cash_executable" and available_quote_depth(state, "ask", selected_quote) < 1:
            rejection = "insufficient_cash_or_ask_depth"
        if not rejection and config["sizing_policy"] == "equal_cash_executable":
            deferred_entries.append({"signal": signal, "signal_id": sid, "selected_quote": selected_quote,
                                     "expiry": expiry, "ask": float(selected_quote["ask"]),
                                     "underlying": underlying, "quote_rejections": quote_rejections,
                                     "relative_spread": (float(selected_quote["ask"]) - float(selected_quote["bid"])) / ((float(selected_quote["ask"]) + float(selected_quote["bid"])) / 2)})
            continue
        if not rejection:
            ask = float(selected_quote["ask"])
            budget = min(config["max_position_equity_fraction"] * equity, state["cash"])
            quantity_by_cash = int(math.floor((budget + 1e-9) / (ask * 100 + config["fee_per_side"])))
            quantity = min(quantity_by_cash, available_quote_depth(state, "ask", selected_quote))
            if quantity < 1:
                rejection = "insufficient_cash_or_ask_depth"
        if rejection:
            if not prior or prior.get("status") != "entered":
                state["signals_seen"][sid] = {"status": "rejected", "last_reason": rejection, "last_observed_at": stamp(asof)}
            event("entry_rejected", signal_id=sid, reason=rejection, directional_filter_evidence=filter_evidence, quote_rejections=quote_rejections, equity=equity)
            continue
        debit = money(quantity * (ask * 100 + config["fee_per_side"]))
        position_id = sid
        position = {"position_id": position_id, "signal_id": sid, "symbol": signal["symbol"], "option_id": selected_quote["option_id"], "expiry": expiry, "type": signal["direction"], "strike": float(selected_quote["strike"]), "multiplier": 100, "quantity": quantity, "entry_time": stamp(asof), "entry_date": local.date().isoformat(), "entry_quote_time": selected_quote["updated_at"], "entry_ask": ask, "entry_bid": float(selected_quote["bid"]), "entry_fee": money(config["fee_per_side"] * quantity), "entry_debit": debit, "unit_entry_debit": money(ask * 100 + config["fee_per_side"]), "underlying_at_entry": underlying, "cash_budget": money(budget), "ask_depth": int(float(selected_quote["ask_size"])), "sizing_policy": config["sizing_policy"], "signal": signal}
        position.update({"last_bid": float(selected_quote["bid"]), "last_quote_time": selected_quote["updated_at"], "last_observed_at": stamp(asof), "mark_status": "fresh"})
        state["positions"][position_id] = position
        marks[position_id] = float(selected_quote["bid"])
        consume_quote_depth(state, "ask", selected_quote, quantity)
        state["cash"] = money(state["cash"] - debit)
        state["signals_seen"][sid] = {"status": "entered", "last_observed_at": stamp(asof), "option_id": selected_quote["option_id"], "position_id": position_id}
        event("paper_entry", position=copy.deepcopy(position), cash=state["cash"], equity_before=equity)
        equity = _portfolio_equity(state, marks)

    if deferred_entries:
        unique_entries = []
        seen_ids = set()
        for entry in deferred_entries:
            if entry["signal_id"] not in seen_ids:
                seen_ids.add(entry["signal_id"])
                unique_entries.append(entry)
        allocation_cash = state["cash"]
        allocation_equity = equity
        allocatable = list(unique_entries)
        while allocatable:
            share = allocation_cash / len(allocatable)
            affordable = [entry for entry in allocatable
                          if entry["ask"] * 100 + config["fee_per_side"] <= min(share, config["max_position_equity_fraction"] * allocation_equity) + 1e-9]
            if len(affordable) == len(allocatable):
                break
            allocatable = affordable
        allocation_reason = "equal_cash_share"
        if not allocatable and unique_entries:
            full_budget = min(allocation_cash, config["max_position_equity_fraction"] * allocation_equity)
            individually_affordable = [entry for entry in unique_entries
                                       if entry["ask"] * 100 + config["fee_per_side"] <= full_budget + 1e-9]
            if individually_affordable:
                allocatable = [min(individually_affordable, key=lambda entry: (entry["relative_spread"], entry["signal_id"]))]
                allocation_reason = "single_candidate_fallback_tightest_relative_spread"
        allocatable_ids = {entry["signal_id"] for entry in allocatable}
        for entry in unique_entries:
            signal, sid = entry["signal"], entry["signal_id"]
            quote, ask, underlying, expiry = entry["selected_quote"], entry["ask"], entry["underlying"], entry["expiry"]
            if sid not in allocatable_ids:
                reason = "insufficient_cash_or_ask_depth"
                state["signals_seen"][sid] = {"status": "rejected", "last_reason": reason, "last_observed_at": stamp(asof)}
                event("entry_rejected", signal_id=sid, reason=reason, quote_rejections=entry["quote_rejections"], equity=equity,
                      sizing_policy=config["sizing_policy"], cash_budget=0.0)
                continue
            budget = min(allocation_cash / len(allocatable), config["max_position_equity_fraction"] * allocation_equity, state["cash"])
            quantity_by_cash = int(math.floor((budget + 1e-9) / (ask * 100 + config["fee_per_side"])))
            quantity = min(quantity_by_cash, available_quote_depth(state, "ask", quote))
            if quantity < 1:
                reason = "insufficient_cash_or_ask_depth"
                state["signals_seen"][sid] = {"status": "rejected", "last_reason": reason, "last_observed_at": stamp(asof)}
                event("entry_rejected", signal_id=sid, reason=reason, quote_rejections=entry["quote_rejections"], equity=equity,
                      sizing_policy=config["sizing_policy"], cash_budget=money(budget))
                continue
            debit = money(quantity * (ask * 100 + config["fee_per_side"]))
            position_id = sid
            position = {"position_id": position_id, "signal_id": sid, "symbol": signal["symbol"], "option_id": quote["option_id"], "expiry": expiry, "type": signal["direction"], "strike": float(quote["strike"]), "multiplier": 100, "quantity": quantity, "entry_time": stamp(asof), "entry_date": local.date().isoformat(), "entry_quote_time": quote["updated_at"], "entry_ask": ask, "entry_bid": float(quote["bid"]), "entry_fee": money(config["fee_per_side"] * quantity), "entry_debit": debit, "unit_entry_debit": money(ask * 100 + config["fee_per_side"]), "underlying_at_entry": underlying, "cash_budget": money(budget), "ask_depth": int(float(quote["ask_size"])), "sizing_policy": config["sizing_policy"], "allocation_reason": allocation_reason, "signal": signal}
            position.update({"last_bid": float(quote["bid"]), "last_quote_time": quote["updated_at"], "last_observed_at": stamp(asof), "mark_status": "fresh"})
            state["positions"][position_id] = position
            marks[position_id] = float(quote["bid"])
            consume_quote_depth(state, "ask", quote, quantity)
            state["cash"] = money(state["cash"] - debit)
            state["signals_seen"][sid] = {"status": "entered", "last_observed_at": stamp(asof), "option_id": quote["option_id"], "position_id": position_id}
            event("paper_entry", position=copy.deepcopy(position), cash=state["cash"], equity_before=equity,
                  allocation_reason=allocation_reason, cash_budget=money(budget))
            equity = _portfolio_equity(state, marks)

    positions = list(state["positions"].values())
    status_value = "entered" if any(e["kind"] == "paper_entry" for e in events) else "exited" if any(e["kind"] == "paper_exit" for e in events) else "marked" if any(e["kind"] == "paper_mark" for e in events) else "unresolved" if any(e["kind"].startswith("unresolved") for e in events) else "entry_rejected" if any(e["kind"] == "entry_rejected" for e in events) else "observed"
    return {"status": status_value, "asof": stamp(asof), "strategy_id": config["strategy_id"], "cash": state["cash"], "realized_pnl": state["realized_pnl"], "positions": copy.deepcopy(positions), "equity": _portfolio_equity(state, marks), "events": events}

def atomic_write(path: Path, value: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as handle:
            json.dump(value, handle, sort_keys=True, indent=2, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def flush_audit(path: Path, state: dict):
    audit_path = path.with_name(path.stem + ".audit.jsonl")
    pending = state.get("pending_audit", [])
    if not pending:
        return
    known = set()
    offset = state.get('audit_offset', 0)
    if not isinstance(offset, int) or offset < 0:
        raise ValueError('Invalid committed audit offset')
    if audit_path.exists():
        if offset > audit_path.stat().st_size:
            raise ValueError('Committed audit has been truncated')
        with audit_path.open('rb') as handle:
            handle.seek(offset)
            content = handle.read(2_000_001)
        if len(content) > 2_000_000:
            raise ValueError('Uncommitted audit tail exceeds recovery bound')
        complete_bytes = offset
        for line in content.splitlines(keepends=True):
            if line.strip():
                try:
                    known.add(json.loads(line)["event_id"])
                except (ValueError, KeyError):
                    if complete_bytes + len(line) == offset + len(content) and not line.endswith(b"\n"):
                        # Repair only a torn trailing append; pending state retains
                        # its complete original event for the recovery below.
                        with audit_path.open("r+b") as handle:
                            handle.truncate(complete_bytes)
                            handle.flush()
                            os.fsync(handle.fileno())
                        break
                    raise ValueError("Audit has a corrupt complete record; inspect " + str(audit_path))
                if not line.endswith(b"\n"):
                    # A complete final JSON record may have lost only its newline.
                    # Preserve it and restore the boundary before appending events.
                    with audit_path.open("ab") as handle:
                        handle.write(b"\n")
                        handle.flush()
                        os.fsync(handle.fileno())
            complete_bytes += len(line)
    elif offset:
        raise ValueError('Committed audit is missing')
    with audit_path.open("a") as handle:
        for event in pending:
            if event["event_id"] not in known:
                handle.write(json.dumps(event, sort_keys=True, allow_nan=False) + "\n")
                known.add(event["event_id"])
        handle.flush()
        os.fsync(handle.fileno())
    state['audit_offset'] = audit_path.stat().st_size
    state["pending_audit"] = []
    atomic_write(path, state)


def process_snapshot(snapshot: dict, state_path: str | Path, config: dict) -> dict:
    path = Path(state_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Kernel locks release on a killed process; no stale PID file can strand exits.
    lock_path = path.with_name(path.name + ".lock")
    try:
        descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        os.close(descriptor)
        raise RuntimeError('State has an active writer') from exc
    try:
        os.ftruncate(descriptor, 0)
        os.write(descriptor, str(os.getpid()).encode())
        state = json.loads(path.read_text()) if path.exists() else new_state(config)
        if state.get("config_fingerprint") != digest(config):
            raise ValueError("Config differs from the frozen state; use a separate state for a new arm/version")
        flush_audit(path, state)
        snapshot_id = str(snapshot.get("observation_id") or digest(snapshot))
        if snapshot_id in state["processed_snapshots"]:
            return {**state["processed_snapshots"][snapshot_id], "duplicate": True}
        if state.get("last_observed_at") and parse_time(snapshot["asof"]) < parse_time(state["last_observed_at"]):
            raise ValueError("Observation precedes the last processed timestamp; replay requires a separate state")
        result = apply_snapshot(snapshot, state, config)
        for index, event in enumerate(result["events"]):
            event["event_id"] = digest([config["strategy_id"], snapshot_id, index])
            event["observation_id"] = snapshot_id
        result["observation_id"] = snapshot_id
        state["processed_snapshots"][snapshot_id] = copy.deepcopy(result)
        # Older observations cannot execute again because the monotonic clock
        # guard remains durable. Financial events stay in the append-only audit.
        while len(state['processed_snapshots']) > 256:
            oldest = min(state['processed_snapshots'], key=lambda key: parse_time(state['processed_snapshots'][key]['asof']))
            del state['processed_snapshots'][oldest]
        state["pending_audit"] = copy.deepcopy(result["events"])
        state["last_observed_at"] = result["asof"]
        atomic_write(path, state)
        flush_audit(path, state)
        return result
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def status(state_path: str | Path, config: dict) -> dict:
    path = Path(state_path)
    state = json.loads(path.read_text()) if path.exists() else new_state(config)
    if state.get("config_fingerprint") != digest(config):
        raise ValueError("State/config identity mismatch")
    trades = state["trades"]
    groups = {}
    for trade in trades:
        key = trade['position_id']
        groups[key] = money(groups.get(key, 0) + trade['net_pnl'])
    closed_groups = {key: pnl for key, pnl in groups.items() if key not in state['positions']}
    wins = [pnl for pnl in closed_groups.values() if pnl > 0]
    losses = [pnl for pnl in closed_groups.values() if pnl < 0]
    positions = list(state["positions"].values())
    all_marked = all(position.get("mark_status") == "fresh" and "last_bid" in position for position in positions)
    equity = money(state["cash"] + sum(position["last_bid"] * position["quantity"] * 100 for position in positions)) if all_marked else None
    return {"strategy_id": config["strategy_id"], "cash": state["cash"], "initial_cash": state["initial_cash"], "realized_pnl": state["realized_pnl"], "closed_contracts": sum(t["quantity"] for t in trades), "closed_trades": len(closed_groups), "realized_exit_fills": len(trades), "win_rate": len(wins) / len(closed_groups) if closed_groups else None, "mean_win": sum(wins) / len(wins) if wins else None, "mean_loss": sum(losses) / len(losses) if losses else None, "net_expectancy_per_trade": sum(closed_groups.values()) / len(closed_groups) if closed_groups else None, "realized_pnl_from_open_positions": money(sum(pnl for key, pnl in groups.items() if key in state['positions'])), "equity": equity, "positions": positions, "mark_asof": state.get('last_observed_at'), "audit_pending": len(state.get("pending_audit", [])), "execution_note": "Local paper fills use observed asks and bids; sampled exits may overshoot thresholds. Trade statistics aggregate partial exits by completed position."}

def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    subcommands = parser.add_subparsers(dest="command", required=True)
    signal_command = subcommands.add_parser("signals")
    signal_command.add_argument("--bars", required=True)
    signal_command.add_argument("--asof", required=True)
    signal_command.add_argument("--config", required=True)
    quote_command = subcommands.add_parser("quote")
    quote_command.add_argument("--snapshot", required=True)
    quote_command.add_argument("--state", required=True)
    quote_command.add_argument("--config", required=True)
    status_command = subcommands.add_parser("status")
    status_command.add_argument("--state", required=True)
    status_command.add_argument("--config", required=True)
    arguments = parser.parse_args()
    try:
        config = load_config(arguments.config)
        if arguments.command == "signals":
            result = signals(json.loads(Path(arguments.bars).read_text()), parse_time(arguments.asof), config)
        elif arguments.command == "quote":
            result = process_snapshot(json.loads(Path(arguments.snapshot).read_text()), arguments.state, config)
        else:
            result = status(arguments.state, config)
        print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))
    except (ValueError, KeyError, TypeError, OSError, RuntimeError) as exc:
        parser.exit(2, "paperdesk: " + str(exc) + "\n")


if __name__ == "__main__":
    main()
