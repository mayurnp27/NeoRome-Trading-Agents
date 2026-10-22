"""Research-only intraday screening harness for the frozen EQ-INTRADAY rules.

The harness consumes an already materialized CSV.  It does not download data,
connect to a broker, or submit orders.  Signals are evaluated on completed bars
and fills use the next bar's open.  A supplied ``session_close`` timestamp is
required so that an exit is never invented when the scheduled bar is absent.

The engineering screen deliberately uses one share per symbol and independent
cash ledgers.  Its return metrics are therefore pipeline diagnostics, not a
$300 portfolio simulation and not evidence that a fill was executable.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from datetime import time
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import vectorbt as vbt


NY = ZoneInfo("America/New_York")
UTC = "UTC"
OPENING_START = time(9, 30)
OPENING_END = time(9, 45)
VWAP_START = time(10, 0)
LAST_ENTRY = time(11, 0)
DEFAULT_COSTS_BPS = (0, 5, 15, 30)
SCHEMA_VERSION = "EQ-INTRADAY-SCREEN-v1"


class ScreenValidationError(ValueError):
    """Raised when the input cannot support a timestamp-correct screen."""

    def __init__(self, message: str, errors: Sequence[Mapping[str, Any]] | None = None):
        super().__init__(message)
        self.errors = [dict(item) for item in (errors or ())]


@dataclass(frozen=True)
class Trade:
    symbol: str
    session_date: str
    signal_timestamp: pd.Timestamp
    entry_timestamp: pd.Timestamp
    exit_timestamp: pd.Timestamp
    entry_open: float
    exit_open: float
    candidate: str


def _error(code: str, **fields: Any) -> dict[str, Any]:
    return {"code": code, **fields}


def _parse_timestamp(value: Any, label: str) -> pd.Timestamp:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        raise ScreenValidationError(f"{label} is missing")
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise ScreenValidationError(f"{label} is not a timestamp") from exc
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ScreenValidationError(f"{label} must include a timezone")
    return timestamp.tz_convert(UTC)


def _parse_bool(value: Any, label: str) -> bool:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, np.integer)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "t", "yes", "y"}:
            return True
        if normalized in {"0", "false", "f", "no", "n", ""}:
            return False
    raise ScreenValidationError(f"{label} must be boolean")


def _number_series(frame: pd.DataFrame, column: str, *, positive: bool = False) -> None:
    values = pd.to_numeric(frame[column], errors="coerce")
    if values.isna().any() or not np.isfinite(values.to_numpy(dtype=float)).all():
        raise ScreenValidationError(f"{column} contains non-finite values")
    if positive and (values <= 0).any():
        raise ScreenValidationError(f"{column} must be positive")
    frame[column] = values.astype(float)


def _timestamp_column(frame: pd.DataFrame, column: str) -> pd.Series:
    parsed: list[pd.Timestamp] = []
    for row_number, value in enumerate(frame[column], start=2):
        try:
            parsed.append(_parse_timestamp(value, f"{column} row {row_number}"))
        except ScreenValidationError as exc:
            raise ScreenValidationError(str(exc), [_error("invalid_timestamp", column=column, row=row_number)]) from exc
    return pd.Series(pd.DatetimeIndex(parsed), index=frame.index)


def _session_key(frame: pd.DataFrame) -> pd.Series:
    return frame["timestamp"].dt.tz_convert(NY).dt.strftime("%Y-%m-%d")


def _coerce_session_closes(
    frame: pd.DataFrame,
    session_closes: Mapping[str, Any] | None,
) -> pd.Series:
    if "session_close" in frame.columns:
        return _timestamp_column(frame, "session_close")
    if not session_closes:
        raise ScreenValidationError("session_close column or calendar/session close input is required")
    closes: list[pd.Timestamp] = []
    for session_date in frame["session_date"]:
        if session_date not in session_closes:
            raise ScreenValidationError(f"session close is missing for {session_date}")
        closes.append(_parse_timestamp(session_closes[session_date], f"session close {session_date}"))
    return pd.Series(pd.DatetimeIndex(closes), index=frame.index)


def validate_bars(
    frame: pd.DataFrame,
    *,
    session_closes: Mapping[str, Any] | None = None,
) -> pd.DataFrame:
    """Validate and normalize a bars/signals frame.

    Timestamps are required to be timezone-aware and are normalized to UTC.
    Session dates are derived in New York and cannot be supplied by a caller;
    this prevents a UTC date from silently becoming a trading-session label.
    """

    if not isinstance(frame, pd.DataFrame) or frame.empty:
        raise ScreenValidationError("input CSV is empty")
    required = {"symbol", "timestamp", "open", "high", "low", "close", "volume"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ScreenValidationError(f"input CSV is missing required columns: {', '.join(missing)}")
    normalized = frame.copy()
    normalized["symbol"] = normalized["symbol"].astype(str).str.strip()
    if (normalized["symbol"] == "").any() or normalized["symbol"].str.lower().eq("nan").any():
        raise ScreenValidationError("symbol cannot be empty")
    normalized["timestamp"] = _timestamp_column(normalized, "timestamp")
    normalized["session_date"] = _session_key(normalized)
    normalized["session_close"] = _coerce_session_closes(normalized, session_closes)
    for column in ("open", "high", "low", "close", "volume"):
        _number_series(normalized, column, positive=True)
    if "vwap" not in normalized.columns:
        raise ScreenValidationError("vwap column is required for the provider cumulative VWAP rule")
    _number_series(normalized, "vwap", positive=True)
    for column in ("open", "high", "low", "close"):
        if normalized[column].isna().any():
            raise ScreenValidationError(f"{column} contains missing values")
    if (normalized["low"] > normalized[["open", "close"]].min(axis=1)).any():
        raise ScreenValidationError("low is above open or close")
    if (normalized["high"] < normalized[["open", "close"]].max(axis=1)).any():
        raise ScreenValidationError("high is below open or close")
    if normalized.duplicated(["symbol", "timestamp"]).any():
        raise ScreenValidationError("duplicate symbol/timestamp rows are not allowed")
    normalized["session_close"] = normalized["session_close"].astype("datetime64[ns, UTC]")
    close_local = normalized["session_close"].dt.tz_convert(NY)
    row_local = normalized["timestamp"].dt.tz_convert(NY)
    if (close_local.dt.strftime("%Y-%m-%d") != normalized["session_date"]).any():
        raise ScreenValidationError("session_close must be on the row's New York session date")
    if (close_local.dt.time <= OPENING_START).any():
        raise ScreenValidationError("session_close must be after the regular-session open")
    if (row_local >= close_local).any():
        raise ScreenValidationError("bars at or after session_close are not valid completed regular-session bars")
    normalized = normalized.sort_values(["symbol", "timestamp"], kind="mergesort").reset_index(drop=True)
    close_counts = normalized.groupby("session_date", sort=False)["session_close"].nunique()
    if (close_counts > 1).any():
        bad = close_counts[close_counts > 1].index.tolist()
        raise ScreenValidationError(f"multiple session closes supplied for {bad}")

    gap_errors: list[dict[str, Any]] = []
    for (symbol, session_date), group in normalized.groupby(["symbol", "session_date"], sort=False):
        close_value = group["session_close"].iloc[0]
        close_local_value = close_value.tz_convert(NY)
        if close_local_value.second or close_local_value.microsecond:
            gap_errors.append(_error("session_close_not_minute_aligned", symbol=symbol, session_date=session_date))
            continue
        start = pd.Timestamp(f"{session_date} 09:30", tz=NY).tz_convert(UTC)
        end = close_value - pd.Timedelta(minutes=1)
        expected = pd.date_range(start, end, freq="min", tz=UTC)
        actual = pd.DatetimeIndex(group["timestamp"])
        missing = expected.difference(actual)
        extra = actual.difference(expected)
        if len(missing) or len(extra) or len(actual) != len(expected):
            scheduled_exit = close_value - pd.Timedelta(minutes=5)
            code = "missing_scheduled_exit" if scheduled_exit in missing else "missing_or_gapped_session"
            gap_errors.append(_error(
                code,
                symbol=symbol,
                session_date=session_date,
                expected_bars=int(len(expected)),
                actual_bars=int(len(actual)),
                missing_bars=int(len(missing)),
                unexpected_bars=int(len(extra)),
                scheduled_exit=scheduled_exit.isoformat() if code == "missing_scheduled_exit" else None,
            ))
    if gap_errors:
        raise ScreenValidationError("sessions must contain complete consecutive one-minute regular-session bars", gap_errors)

    # A supplied signal column must reproduce the frozen rule. This keeps the
    # CSV useful as an audit trail without allowing tuned signals to change the
    # registered experiment.
    for column in ("orb_signal", "vwap_reclaim_signal"):
        if column in normalized.columns:
            normalized[column] = [
                _parse_bool(value, f"{column} row {index + 2}")
                for index, value in enumerate(normalized[column])
            ]
    return normalized


def _scheduled_exit(group: pd.DataFrame) -> pd.Timestamp:
    close = group["session_close"].iloc[0]
    target = close - pd.Timedelta(minutes=5)
    matching = group[group["timestamp"] == target]
    if matching.empty:
        raise ScreenValidationError(
            f"missing scheduled exit bar {target.isoformat()} for {group['symbol'].iloc[0]} {group['session_date'].iloc[0]}",
            [_error("missing_scheduled_exit", symbol=group["symbol"].iloc[0], session_date=group["session_date"].iloc[0], scheduled_exit=target.isoformat())],
        )
    return target


def _provider_cumulative_vwap(group: pd.DataFrame) -> pd.Series:
    volume = group["volume"].astype(float)
    return (group["vwap"].astype(float) * volume).cumsum() / volume.cumsum()


def _computed_signal(group: pd.DataFrame, candidate: str) -> pd.Series:
    local = group["timestamp"].dt.tz_convert(NY)
    local_time = local.dt.time
    if candidate == "orb15":
        opening = group[(local_time >= OPENING_START) & (local_time < OPENING_END)]
        if opening.empty:
            raise ScreenValidationError(
                f"missing 09:30-09:44 opening range for {group['symbol'].iloc[0]} {group['session_date'].iloc[0]}",
                [_error("missing_opening_range", symbol=group["symbol"].iloc[0], session_date=group["session_date"].iloc[0])],
            )
        opening_high = float(opening["high"].max())
        raw = (local_time >= OPENING_END) & (local_time < LAST_ENTRY) & (group["close"] > opening_high)
        return raw & ~raw.shift(1, fill_value=False)
    if candidate == "vwap_reclaim":
        cumulative = _provider_cumulative_vwap(group)
        return (
            (local_time >= VWAP_START)
            & (local_time < LAST_ENTRY)
            & (group["close"].shift(1) < cumulative.shift(1))
            & (group["close"] > cumulative)
        ).fillna(False)
    raise ValueError(f"unknown candidate: {candidate}")


def _signals_for(group: pd.DataFrame, candidate: str) -> pd.Series:
    column = "orb_signal" if candidate == "orb15" else "vwap_reclaim_signal"
    computed = _computed_signal(group, candidate)
    if column in group.columns:
        supplied = group[column].astype(bool)
        if not supplied.reset_index(drop=True).equals(computed.reset_index(drop=True)):
            raise ScreenValidationError(
                f"{column} does not match the frozen {candidate} rule",
                [_error("signal_rule_mismatch", candidate=candidate, symbol=group["symbol"].iloc[0], session_date=group["session_date"].iloc[0])],
            )
    signals = computed
    local_time = group["timestamp"].dt.tz_convert(NY).dt.time
    minimum = OPENING_END if candidate == "orb15" else VWAP_START
    invalid = signals & ((local_time < minimum) | (local_time >= LAST_ENTRY))
    if invalid.any():
        raise ScreenValidationError(
            f"{candidate} signal outside its permitted New York time window",
            [_error("invalid_signal_timing", candidate=candidate, symbol=group["symbol"].iloc[0], session_date=group["session_date"].iloc[0])],
        )
    return signals


def build_trades(frame: pd.DataFrame, candidate: str) -> list[Trade]:
    """Create timestamp-correct next-open trades for one frozen candidate."""

    trades: list[Trade] = []
    for (_symbol, _session_date), group in frame.groupby(["symbol", "session_date"], sort=False):
        group = group.sort_values("timestamp").reset_index(drop=True)
        exit_timestamp = _scheduled_exit(group)
        signals = _signals_for(group, candidate)
        signal_positions = np.flatnonzero(signals.to_numpy(dtype=bool))
        if signal_positions.size == 0:
            continue
        signal_position = int(signal_positions[0])
        entry_position = signal_position + 1
        if entry_position >= len(group) or group.loc[entry_position, "session_date"] != _session_date:
            raise ScreenValidationError(
                f"missing next-bar open after {group.loc[signal_position, 'timestamp'].isoformat()}",
                [_error("missing_next_open", candidate=candidate, symbol=_symbol, session_date=_session_date, signal_timestamp=group.loc[signal_position, "timestamp"].isoformat())],
            )
        entry_timestamp = group.loc[entry_position, "timestamp"]
        if entry_timestamp.tz_convert(NY).time() >= LAST_ENTRY:
            # A signal can be observable in the 10:59 completed bar while its
            # next-open fill is 11:00. That fill is outside the frozen window.
            continue
        if entry_timestamp != group.loc[signal_position, "timestamp"] + pd.Timedelta(minutes=1):
            raise ScreenValidationError(
                "next-bar open is not exactly one minute after the signal",
                [_error("nonconsecutive_next_open", candidate=candidate, symbol=_symbol, session_date=_session_date, signal_timestamp=group.loc[signal_position, "timestamp"].isoformat())],
            )
        if entry_timestamp >= exit_timestamp:
            raise ScreenValidationError(
                f"next-bar open for {candidate} is not before the scheduled exit",
                [_error("entry_after_exit", candidate=candidate, symbol=_symbol, session_date=_session_date, entry_timestamp=entry_timestamp.isoformat(), scheduled_exit=exit_timestamp.isoformat())],
            )
        entry_open = float(group.loc[entry_position, "open"])
        exit_open = float(group.loc[group["timestamp"].eq(exit_timestamp), "open"].iloc[0])
        trades.append(Trade(_symbol, _session_date, group.loc[signal_position, "timestamp"], entry_timestamp, exit_timestamp, entry_open, exit_open, candidate))
    return trades


def _wide_prices(frame: pd.DataFrame, column: str) -> pd.DataFrame:
    return frame.pivot(index="timestamp", columns="symbol", values=column).sort_index()


def _signal_frames(frame: pd.DataFrame, trades: Iterable[Trade]) -> tuple[pd.DataFrame, pd.DataFrame]:
    index = pd.DatetimeIndex(sorted(frame["timestamp"].unique()))
    symbols = sorted(frame["symbol"].unique())
    entries = pd.DataFrame(False, index=index, columns=symbols)
    exits = pd.DataFrame(False, index=index, columns=symbols)
    for trade in trades:
        entries.loc[trade.entry_timestamp, trade.symbol] = True
        exits.loc[trade.exit_timestamp, trade.symbol] = True
    return entries, exits


def _portfolio_metrics(
    open_prices: pd.DataFrame,
    close_prices: pd.DataFrame,
    entries: pd.DataFrame,
    exits: pd.DataFrame,
    *,
    cost_bps: int,
    init_cash: float,
) -> dict[str, Any]:
    price = open_prices.reindex(index=entries.index, columns=entries.columns)
    close = close_prices.reindex(index=entries.index, columns=entries.columns)
    portfolio = vbt.Portfolio.from_signals(
        close,
        entries,
        exits,
        price=price,
        size=1,
        init_cash=init_cash,
        cash_sharing=False,
        fees=cost_bps / 10000.0,
        slippage=0.0,
        allow_partial=False,
        raise_reject=False,
        direction="longonly",
        upon_opposite_entry="ignore",
    )
    profits = np.asarray(portfolio.total_profit(), dtype=float)
    values = np.asarray(portfolio.value(), dtype=float)
    if values.ndim == 1:
        values = values[:, None]
    aggregate = np.nansum(values, axis=1)
    initial = init_cash * len(entries.columns)
    curve = np.concatenate(([initial], aggregate))
    drawdown = curve / np.maximum.accumulate(curve) - 1.0
    count = int(np.nansum(np.asarray(portfolio.trades.count(), dtype=float)))
    return {
        "initial_cash_per_symbol": init_cash,
        "symbols": int(len(entries.columns)),
        "final_value": float(aggregate[-1]) if aggregate.size else initial,
        "total_profit": float(np.nansum(profits)),
        "total_return": float(np.nansum(profits) / initial) if initial else 0.0,
        "max_drawdown": float(np.nanmin(drawdown)) if drawdown.size else 0.0,
        "trade_count": count,
        "cost_bps_per_side": cost_bps,
    }


def _trade_json(trade: Trade, cost_bps: int) -> dict[str, Any]:
    rate = cost_bps / 10000.0
    gross = trade.exit_open - trade.entry_open
    net = gross - (trade.entry_open + trade.exit_open) * rate
    return {
        "symbol": trade.symbol,
        "session_date": trade.session_date,
        "candidate": trade.candidate,
        "signal_timestamp": trade.signal_timestamp.isoformat(),
        "entry_timestamp": trade.entry_timestamp.isoformat(),
        "exit_timestamp": trade.exit_timestamp.isoformat(),
        "entry_open": trade.entry_open,
        "exit_open": trade.exit_open,
        "gross_pnl_one_share": gross,
        "net_pnl_one_share": net,
        "cost_bps_per_side": cost_bps,
    }


def _control_trades(frame: pd.DataFrame) -> list[Trade]:
    controls: list[Trade] = []
    for (symbol, session_date), group in frame.groupby(["symbol", "session_date"], sort=False):
        group = group.sort_values("timestamp").reset_index(drop=True)
        exit_timestamp = _scheduled_exit(group)
        first = group.iloc[0]
        exit_row = group[group["timestamp"] == exit_timestamp].iloc[0]
        controls.append(Trade(symbol, session_date, first["timestamp"], first["timestamp"], exit_timestamp, float(first["open"]), float(exit_row["open"]), "same_symbol_open_to_exit"))
    return controls


def screen_frame(
    frame: pd.DataFrame,
    *,
    costs_bps: Sequence[int] = DEFAULT_COSTS_BPS,
    init_cash: float = 100_000.0,
    session_closes: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Run both candidates and every requested per-side cost scenario."""

    normalized = validate_bars(frame, session_closes=session_closes)
    costs = tuple(int(cost) for cost in costs_bps)
    if not costs or any(cost < 0 for cost in costs):
        raise ValueError("costs_bps must contain nonnegative basis-point values")
    open_prices = _wide_prices(normalized, "open")
    close_prices = _wide_prices(normalized, "close")
    candidates = {name: build_trades(normalized, name) for name in ("orb15", "vwap_reclaim")}
    controls = _control_trades(normalized)
    control_entries, control_exits = _signal_frames(normalized, controls)
    trials: list[dict[str, Any]] = []
    for candidate, trades in candidates.items():
        entries, exits = _signal_frames(normalized, trades)
        for cost_bps in costs:
            metrics = _portfolio_metrics(open_prices, close_prices, entries, exits, cost_bps=cost_bps, init_cash=init_cash)
            if metrics["trade_count"] != len(trades):
                raise ScreenValidationError(
                    "VectorBT did not execute every modeled candidate trade; cash assumptions are insufficient",
                    [_error("portfolio_trade_mismatch", candidate=candidate, cost_bps_per_side=cost_bps, modeled_trades=len(trades), executed_trades=metrics["trade_count"])],
                )
            control_metrics = _portfolio_metrics(
                open_prices,
                close_prices,
                control_entries,
                control_exits,
                cost_bps=cost_bps,
                init_cash=init_cash,
            )
            if control_metrics["trade_count"] != len(controls):
                raise ScreenValidationError(
                    "VectorBT did not execute every modeled control trade; cash assumptions are insufficient",
                    [_error("control_trade_mismatch", cost_bps_per_side=cost_bps, modeled_trades=len(controls), executed_trades=control_metrics["trade_count"])],
                )
            trials.append({
                "candidate": candidate,
                "cost_bps_per_side": cost_bps,
                "gross_cost_baseline": cost_bps == 0,
                "trades": [_trade_json(trade, cost_bps) for trade in trades],
                "portfolio": metrics,
                "controls": {
                    "cash": {"total_return": 0.0, "total_profit": 0.0, "trade_count": 0},
                    "same_symbol_open_to_exit": {
                        **control_metrics,
                        "trades": [_trade_json(trade, cost_bps) for trade in controls],
                    },
                },
            })
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "ok",
        "research_only": True,
        "labels": {
            "universe": "fixed supplied universe; selection-biased engineering screen",
            "costs": "bar cost scenario only; not calibrated executable fills",
            "execution": "next-bar open model; no quote replay, depth, latency, or fill proof",
            "portfolio": "one share per symbol with independent cash ledgers; not the $300 equity sleeve",
            "filters_omitted": "point-in-time eligibility, 60-session history, dollar-volume and opening-volume filters",
        },
        "assumptions": {
            "timezone": "America/New_York",
            "opening_range": "09:30-09:44 completed one-minute bars",
            "last_entry": "strictly before 11:00 New York",
            "exit": "open of the bar five minutes before supplied scheduled session close",
            "max_trades": "one per symbol/session/candidate",
            "overnight": False,
            "provider_vwap": "cumulative sum(provider_bar_vwap * volume) / cumulative volume",
        },
        "input_summary": {
            "symbols": sorted(normalized["symbol"].unique().tolist()),
            "sessions": sorted(normalized["session_date"].unique().tolist()),
            "bar_count": int(len(normalized)),
        },
        "trials": trials,
    }


def _input_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_csv(
    input_csv: str | Path,
    output_json: str | Path | None = None,
    *,
    costs_bps: Sequence[int] = DEFAULT_COSTS_BPS,
    init_cash: float = 100_000.0,
    session_closes: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Read one CSV, screen it, and optionally create an exclusive JSON report."""

    input_path = Path(input_csv)
    input_hash = _input_sha256(input_path)
    try:
        result = screen_frame(
            pd.read_csv(input_path),
            costs_bps=costs_bps,
            init_cash=init_cash,
            session_closes=session_closes,
        )
    except ScreenValidationError as exc:
        result = {
            "schema_version": SCHEMA_VERSION,
            "status": "rejected",
            "research_only": True,
            "input_sha256": input_hash,
            "validation_errors": exc.errors or [{"code": "validation_error", "message": str(exc)}],
            "labels": {"missing_or_gapped_sessions": "rejected and retained as validation evidence"},
        }
        if output_json is not None:
            _write_json(Path(output_json), result)
        raise
    result["input_sha256"] = input_hash
    result["input_path"] = str(input_path)
    if output_json is not None:
        _write_json(Path(output_json), result)
    return result


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_csv", type=Path)
    parser.add_argument("output_json", type=Path)
    parser.add_argument("--cost-bps", nargs="+", type=int, default=list(DEFAULT_COSTS_BPS))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    run_csv(args.input_csv, args.output_json, costs_bps=args.cost_bps)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
