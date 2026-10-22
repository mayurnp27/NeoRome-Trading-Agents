"""Deterministic, timestamp-correct walk-forward evaluation for equities.

This module only consumes retained bars supplied by the caller.  It has no
provider, broker, scheduler, or model-fitting path.  The registered strategies
are deliberately frozen snapshots of the desk experiments:

* ``overnight_monthly_v1`` selects positive overnight-minus-intraday scores at
  a monthly boundary and evaluates the following overnight return;
* ``intraday_orb15_v1`` and ``intraday_vwap_reclaim_v1`` implement the two
  registered minute-bar candidates.

The evaluator computes every trial from the same frozen input/config hash,
keeps development, validation, and purged holdout metrics separate, and writes
content-addressed records.  A retained record is never overwritten.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
from calendar import monthrange
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo


SCHEMA_VERSION = "EQ-WALKFORWARD-v1"
NY = ZoneInfo("America/New_York")
UTC = timezone.utc
REGISTERED_STRATEGIES: dict[str, dict[str, Any]] = {
    "overnight_monthly_v1": {
        "kind": "daily_overnight",
        "version": "2026-09-23",
        "lookback_sessions": 252,
        "top_n": 10,
        "description": "Monthly top positive overnight-minus-intraday score; one overnight at the next month boundary.",
    },
    "intraday_orb15_v1": {
        "kind": "minute_intraday",
        "version": "2026-09-23",
        "opening_minutes": 15,
        "last_entry_local": "11:00",
        "exit_minutes_before_close": 5,
        "description": "First completed close above the 09:30-09:44 opening range high.",
    },
    "intraday_vwap_reclaim_v1": {
        "kind": "minute_intraday",
        "version": "2026-09-23",
        "start_local": "10:00",
        "last_entry_local": "11:00",
        "exit_minutes_before_close": 5,
        "description": "First completed close reclaiming provider cumulative VWAP.",
    },
}


class WalkForwardError(ValueError):
    """Raised when retained data cannot support a deterministic evaluation."""


def _as_float(value: Any, field: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise WalkForwardError(f"{field} must be numeric") from exc
    if not math.isfinite(result):
        raise WalkForwardError(f"{field} must be finite")
    return result


def _as_timestamp(value: Any, field: str) -> datetime:
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            result = datetime.fromisoformat(text)
        except ValueError as exc:
            raise WalkForwardError(f"{field} must be ISO-8601") from exc
    else:
        raise WalkForwardError(f"{field} must be an ISO-8601 timestamp")
    if result.tzinfo is None or result.utcoffset() is None:
        raise WalkForwardError(f"{field} must include a timezone offset")
    return result.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class Bar:
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    session_close: datetime | None = None
    vwap: float | None = None
    eligible: bool = True
    dollar_volume: float | None = None

    @property
    def local_date(self) -> date:
        return self.timestamp.astimezone(NY).date()

    @property
    def local_time(self):
        return self.timestamp.astimezone(NY).time()


def parse_bars(rows: Iterable[Mapping[str, Any]]) -> list[Bar]:
    """Validate retained bars and return strict timestamp/symbol order.

    The caller must provide timezone-aware timestamps.  Missing rows are kept
    as a data limitation by the caller; this parser never synthesizes bars.
    """

    result: list[Bar] = []
    seen: set[tuple[str, datetime]] = set()
    for number, raw in enumerate(rows, start=1):
        if not isinstance(raw, Mapping):
            raise WalkForwardError(f"row {number} must be an object")
        symbol = str(raw.get("symbol", "")).strip().upper()
        if not symbol:
            raise WalkForwardError(f"row {number} has an empty symbol")
        timestamp = _as_timestamp(raw.get("timestamp"), f"row {number} timestamp")
        key = (symbol, timestamp)
        if key in seen:
            raise WalkForwardError(f"duplicate symbol/timestamp: {symbol} {_iso(timestamp)}")
        seen.add(key)
        prices = {name: _as_float(raw.get(name), f"row {number} {name}") for name in ("open", "high", "low", "close", "volume")}
        if min(prices["open"], prices["high"], prices["low"], prices["close"]) <= 0 or prices["volume"] < 0:
            raise WalkForwardError(f"row {number} has invalid price or volume")
        if prices["low"] > min(prices["open"], prices["close"]) or prices["high"] < max(prices["open"], prices["close"]):
            raise WalkForwardError(f"row {number} has invalid OHLC bounds")
        session_close = raw.get("session_close")
        close_value = _as_timestamp(session_close, f"row {number} session_close") if session_close not in (None, "") else None
        vwap = None if raw.get("vwap") in (None, "") else _as_float(raw.get("vwap"), f"row {number} vwap")
        dollar_volume = None if raw.get("dollar_volume") in (None, "") else _as_float(raw.get("dollar_volume"), f"row {number} dollar_volume")
        eligible = raw.get("eligible", True)
        if isinstance(eligible, str):
            eligible = eligible.strip().lower() in {"1", "true", "yes", "y"}
        if not isinstance(eligible, bool):
            raise WalkForwardError(f"row {number} eligible must be boolean")
        result.append(Bar(symbol, timestamp, prices["open"], prices["high"], prices["low"], prices["close"], prices["volume"], close_value, vwap, eligible, dollar_volume))
    result.sort(key=lambda bar: (bar.timestamp, bar.symbol))
    return result


@dataclass(frozen=True)
class Trade:
    strategy_id: str
    symbol: str
    session_date: str
    entry_timestamp: str
    exit_timestamp: str
    entry_price: float
    exit_price: float
    benchmark_return: float
    source_rows: tuple[str, ...]

    @property
    def gross_return(self) -> float:
        return self.exit_price / self.entry_price - 1.0


@dataclass(frozen=True)
class WalkForwardSpec:
    development_start: str
    development_end: str
    validation_start: str
    validation_end: str
    holdout_start: str
    holdout_end: str
    purge_days: int = 1
    cadence: str = "monthly"

    def __post_init__(self) -> None:
        starts_ends = [
            (self.development_start, self.development_end),
            (self.validation_start, self.validation_end),
            (self.holdout_start, self.holdout_end),
        ]
        parsed = [tuple(date.fromisoformat(item) for item in pair) for pair in starts_ends]
        if any(start > end for start, end in parsed):
            raise WalkForwardError("walk-forward period start must be before its end")
        if parsed[1][0] <= parsed[0][1] or parsed[2][0] <= parsed[1][1]:
            raise WalkForwardError("walk-forward periods must be chronological and non-overlapping")
        if self.purge_days < 0:
            raise WalkForwardError("purge_days cannot be negative")
        if self.cadence not in {"daily", "weekly", "monthly"}:
            raise WalkForwardError("cadence must be daily, weekly, or monthly")

    def split_for(self, session_date: date) -> str | None:
        development = (date.fromisoformat(self.development_start), date.fromisoformat(self.development_end))
        validation = (date.fromisoformat(self.validation_start), date.fromisoformat(self.validation_end))
        holdout = (date.fromisoformat(self.holdout_start), date.fromisoformat(self.holdout_end))
        if development[0] <= session_date <= development[1]:
            return "development"
        if validation[0] <= session_date <= validation[1] and session_date >= development[1] + timedelta(days=self.purge_days + 1):
            return "validation"
        if holdout[0] <= session_date <= holdout[1] and session_date >= validation[1] + timedelta(days=self.purge_days + 1):
            return "holdout"
        return None


def _score_rows(history: Sequence[Bar], lookback: int) -> dict[str, float]:
    by_symbol: dict[str, list[Bar]] = {}
    for bar in history:
        if bar.eligible:
            by_symbol.setdefault(bar.symbol, []).append(bar)
    scores: dict[str, float] = {}
    for symbol, rows in by_symbol.items():
        rows = sorted(rows, key=lambda item: item.timestamp)
        if len(rows) < lookback + 1:
            continue
        overnight: list[float] = []
        intraday: list[float] = []
        for previous, current in zip(rows[-lookback - 1 : -1], rows[-lookback:]):
            if current.open <= 0 or previous.close <= 0:
                continue
            overnight.append(math.log(current.open / previous.close))
            intraday.append(math.log(current.close / current.open))
        if len(overnight) == lookback:
            scores[symbol] = statistics.fmean(overnight) - statistics.fmean(intraday)
    return scores


def _overnight_trades(bars: Sequence[Bar], params: Mapping[str, Any]) -> list[Trade]:
    daily = sorted(bars, key=lambda item: (item.symbol, item.timestamp))
    by_symbol: dict[str, list[Bar]] = {}
    for bar in daily:
        by_symbol.setdefault(bar.symbol, []).append(bar)
    all_dates = sorted({bar.local_date for bar in daily})
    month_firsts: list[date] = []
    seen_months: set[tuple[int, int]] = set()
    for item in all_dates:
        key = (item.year, item.month)
        if key not in seen_months:
            seen_months.add(key)
            month_firsts.append(item)
    lookback = int(params.get("lookback_sessions", 252))
    top_n = int(params.get("top_n", 10))
    trades: list[Trade] = []
    for first_date in month_firsts:
        first_by_symbol = {bar.symbol: bar for bar in daily if bar.local_date == first_date}
        previous_history = [bar for bar in daily if bar.local_date < first_date]
        scores = _score_rows(previous_history, lookback)
        ranked = sorted((item for item in scores.items() if item[1] > 0), key=lambda item: (-item[1], item[0]))[:top_n]
        for symbol, _score in ranked:
            prior = [bar for bar in by_symbol[symbol] if bar.local_date < first_date]
            current = first_by_symbol.get(symbol)
            if not prior or current is None:
                continue
            previous = prior[-1]
            benchmark = current.open / previous.close - 1.0
            trades.append(Trade("overnight_monthly_v1", symbol, first_date.isoformat(), _iso(previous.timestamp), _iso(current.timestamp), previous.close, current.open, benchmark, (_iso(previous.timestamp), _iso(current.timestamp))))
    return trades


def _intraday_trades(bars: Sequence[Bar], strategy_id: str) -> list[Trade]:
    by_session: dict[tuple[str, date], list[Bar]] = {}
    for bar in bars:
        by_session.setdefault((bar.symbol, bar.local_date), []).append(bar)
    trades: list[Trade] = []
    for (symbol, session_date), session in sorted(by_session.items(), key=lambda item: (item[0][1], item[0][0])):
        session = sorted(session, key=lambda item: item.timestamp)
        if not all(bar.session_close is not None for bar in session):
            continue
        session_close = session[0].session_close
        assert session_close is not None
        exit_timestamp = session_close - timedelta(minutes=5)
        expected = [session[0].timestamp + timedelta(minutes=index) for index in range(int((exit_timestamp - session[0].timestamp).total_seconds() // 60) + 1)]
        if session[0].local_time.hour != 9 or session[0].local_time.minute != 30 or [bar.timestamp for bar in session] != expected:
            # Gaps and unexpected rows are data limitations, never a free
            # interpolation or an implicit successful no-trade.
            continue
        exit_bar = next((bar for bar in session if bar.timestamp == exit_timestamp), None)
        if exit_bar is None:
            continue
        orb_rows = [bar for bar in session if bar.local_time.hour == 9 and bar.local_time.minute >= 30 and bar.local_time.minute < 45]
        signal_index: int | None = None
        if strategy_id == "intraday_orb15_v1":
            if not orb_rows:
                continue
            high = max(bar.high for bar in orb_rows)
            signal_index = next((index for index, bar in enumerate(session) if bar.timestamp > orb_rows[-1].timestamp and bar.local_time.hour < 11 and bar.close > high), None)
        elif strategy_id == "intraday_vwap_reclaim_v1":
            previous: Bar | None = None
            for index, bar in enumerate(session):
                if bar.local_time.hour < 10 or bar.vwap is None:
                    previous = bar
                    continue
                if previous is not None and previous.close < (previous.vwap if previous.vwap is not None else previous.close) and bar.close > bar.vwap:
                    signal_index = index
                    break
                previous = bar
        else:
            raise WalkForwardError(f"unsupported strategy: {strategy_id}")
        if signal_index is None or signal_index + 1 >= len(session):
            continue
        entry = session[signal_index + 1]
        if entry.local_time.hour >= 11:
            continue
        if entry.timestamp >= exit_bar.timestamp:
            continue
        benchmark = exit_bar.open / entry.open - 1.0
        trades.append(Trade(strategy_id, symbol, session_date.isoformat(), _iso(entry.timestamp), _iso(exit_bar.timestamp), entry.open, exit_bar.open, benchmark, (_iso(session[signal_index].timestamp), _iso(entry.timestamp), _iso(exit_bar.timestamp))))
    return trades


def generate_trades(strategy_id: str, bars: Sequence[Bar]) -> list[Trade]:
    if strategy_id not in REGISTERED_STRATEGIES:
        raise WalkForwardError(f"strategy is not registered: {strategy_id}")
    config = REGISTERED_STRATEGIES[strategy_id]
    if config["kind"] == "daily_overnight":
        return _overnight_trades(bars, config)
    return _intraday_trades(bars, strategy_id)


def _metrics(trades: Sequence[Trade], cost_bps_per_side: float) -> dict[str, Any]:
    factor_entry = 1.0 + cost_bps_per_side / 10000.0
    factor_exit = 1.0 - cost_bps_per_side / 10000.0
    gross = [trade.gross_return for trade in trades]
    net = [(trade.exit_price * factor_exit) / (trade.entry_price * factor_entry) - 1.0 for trade in trades]
    benchmark = [trade.benchmark_return for trade in trades]
    equity = 1.0
    peak = 1.0
    max_drawdown = 0.0
    for value in net:
        equity *= 1.0 + value
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity / peak - 1.0)
    gross_total = math.prod(1.0 + value for value in gross) - 1.0 if gross else 0.0
    net_total = math.prod(1.0 + value for value in net) - 1.0 if net else 0.0
    benchmark_total = math.prod(1.0 + value for value in benchmark) - 1.0 if benchmark else 0.0
    return {
        "trade_count": len(trades),
        "gross_total_return": gross_total,
        "net_total_return": net_total,
        "net_mean_trade_return": statistics.fmean(net) if net else 0.0,
        "benchmark_total_return": benchmark_total,
        "excess_total_return": net_total - benchmark_total,
        "max_drawdown": max_drawdown,
        "cost_bps_per_side": cost_bps_per_side,
        "break_even_cost_bps_per_side_approx": max(0.0, statistics.fmean(gross) * 5000.0) if gross else 0.0,
    }


def _config_hash(strategy_id: str, spec: WalkForwardSpec, costs: Sequence[float]) -> str:
    raw = json.dumps({"schema": SCHEMA_VERSION, "strategy": strategy_id, "registry": REGISTERED_STRATEGIES[strategy_id], "spec": asdict(spec), "costs": list(costs)}, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()


def evaluate(strategy_id: str, rows: Iterable[Mapping[str, Any]], spec: WalkForwardSpec, *, costs_bps_per_side: Sequence[float] = (0.0, 5.0, 15.0, 30.0)) -> dict[str, Any]:
    if not costs_bps_per_side:
        raise WalkForwardError("at least one cost trial is required")
    costs = tuple(float(value) for value in costs_bps_per_side)
    if any(value < 0 or not math.isfinite(value) for value in costs):
        raise WalkForwardError("cost trials must be finite and non-negative")
    bars = parse_bars(rows)
    if not bars:
        raise WalkForwardError("retained input is empty")
    trades = generate_trades(strategy_id, bars)
    session_groups: dict[tuple[str, date], list[Bar]] = {}
    for bar in bars:
        session_groups.setdefault((bar.symbol, bar.local_date), []).append(bar)
    incomplete_sessions = 0
    if REGISTERED_STRATEGIES[strategy_id]["kind"] == "minute_intraday":
        for session in session_groups.values():
            closes = {bar.session_close for bar in session}
            if len(closes) != 1 or next(iter(closes)) is None:
                incomplete_sessions += 1
                continue
            session = sorted(session, key=lambda item: item.timestamp)
            end = next(iter(closes)) - timedelta(minutes=5)
            expected = [session[0].timestamp + timedelta(minutes=index) for index in range(int((end - session[0].timestamp).total_seconds() // 60) + 1)]
            if session[0].local_time.hour != 9 or session[0].local_time.minute != 30 or [bar.timestamp for bar in session] != expected:
                incomplete_sessions += 1
    split_metrics: dict[str, dict[str, dict[str, Any]]] = {}
    for cost in costs:
        split_metrics[str(cost)] = {}
        for split in ("development", "validation", "holdout"):
            selected = [trade for trade in trades if spec.split_for(date.fromisoformat(trade.session_date)) == split]
            split_metrics[str(cost)][split] = _metrics(selected, cost)
    input_payload = [
        {"symbol": bar.symbol, "timestamp": _iso(bar.timestamp), "open": bar.open, "high": bar.high, "low": bar.low, "close": bar.close, "volume": bar.volume, "session_close": _iso(bar.session_close) if bar.session_close else None, "vwap": bar.vwap, "eligible": bar.eligible, "dollar_volume": bar.dollar_volume}
        for bar in bars
    ]
    input_hash = hashlib.sha256(json.dumps(input_payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return {
        "schema_version": SCHEMA_VERSION,
        "strategy_id": strategy_id,
        "strategy_registry": REGISTERED_STRATEGIES[strategy_id],
        "config_hash": _config_hash(strategy_id, spec, costs),
        "input_sha256": input_hash,
        "spec": asdict(spec),
        "data_limits": {
            "rows": len(bars),
            "symbols": sorted({bar.symbol for bar in bars}),
            "trade_count_before_split": len(trades),
            "incomplete_sessions": incomplete_sessions,
            # A boolean eligibility flag is not proof of historical membership;
            # promotion requires a separately retained PIT universe artifact.
            "point_in_time_universe_verified": False,
            "provider_quote_replay": False,
            "fixture_or_retained_data_label": "caller_supplied_retained_rows",
        },
        "trials": split_metrics,
        "acceptance": {
            "status": "research_only",
            "eligible_for_promotion": False,
            "reason": "point_in_time_universe_and_executable_quote_evidence_are_not certified by this offline evaluator",
            "required_checks": ["point_in_time_universe", "timestamped_quote_replay", "held_out_sample", "prospective_shadow"],
        },
    }


def retain_record(root: str | Path, record: Mapping[str, Any]) -> dict[str, str]:
    """Write one immutable content-addressed evaluation record."""
    raw = json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    digest = hashlib.sha256(raw).hexdigest()
    target = Path(root) / str(record["strategy_id"]) / f"{digest}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() and target.read_bytes() != raw:
        raise WalkForwardError("content-addressed record collision")
    if not target.exists():
        target.write_bytes(raw)
    return {"sha256": digest, "path": str(target)}


def revalidation_due(last_completed: datetime | None, now: datetime, cadence: str) -> bool:
    """Return whether a periodic run is due; no scheduler or side effect."""
    if cadence not in {"daily", "weekly", "monthly"}:
        raise WalkForwardError("cadence must be daily, weekly, or monthly")
    now = _as_timestamp(now, "now")
    if last_completed is None:
        return True
    previous = _as_timestamp(last_completed, "last_completed")
    if now < previous:
        raise WalkForwardError("now cannot precede last_completed")
    if cadence == "daily":
        return now.date() > previous.date()
    if cadence == "weekly":
        return now.date() >= previous.date() + timedelta(days=7)
    year, month = previous.year + (previous.month // 12), (previous.month % 12) + 1
    return (now.year, now.month) >= (year, month)


def load_csv(path: str | Path) -> list[dict[str, str]]:
    with Path(path).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _cli() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_csv")
    parser.add_argument("output_root")
    parser.add_argument("--strategy", choices=sorted(REGISTERED_STRATEGIES), required=True)
    parser.add_argument("--cost-bps", nargs="+", type=float, default=[0.0, 5.0, 15.0, 30.0])
    parser.add_argument("--development", nargs=2, metavar=("START", "END"), required=True)
    parser.add_argument("--validation", nargs=2, metavar=("START", "END"), required=True)
    parser.add_argument("--holdout", nargs=2, metavar=("START", "END"), required=True)
    parser.add_argument("--purge-days", type=int, default=1)
    parser.add_argument("--cadence", choices=("daily", "weekly", "monthly"), default="monthly")
    args = parser.parse_args()
    spec = WalkForwardSpec(args.development[0], args.development[1], args.validation[0], args.validation[1], args.holdout[0], args.holdout[1], args.purge_days, args.cadence)
    record = evaluate(args.strategy, load_csv(args.input_csv), spec, costs_bps_per_side=args.cost_bps)
    retained = retain_record(args.output_root, record)
    print(json.dumps({"record": retained, "config_hash": record["config_hash"], "input_sha256": record["input_sha256"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())


__all__ = [
    "Bar",
    "REGISTERED_STRATEGIES",
    "SCHEMA_VERSION",
    "Trade",
    "WalkForwardError",
    "WalkForwardSpec",
    "evaluate",
    "generate_trades",
    "load_csv",
    "parse_bars",
    "retain_record",
    "revalidation_due",
]
