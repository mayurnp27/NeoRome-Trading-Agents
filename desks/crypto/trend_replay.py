"""Offline chronological replay for the CR-001 liquid-crypto comparator.

The replay deliberately accepts only caller-supplied completed daily bars.  It
does not fetch data, read a ledger, create orders, or enable a runtime.  A
signal is evaluated at a completed UTC daily close and, when a following bar
exists, is executed at that following bar's open.  This keeps the simulator
from using a same-close fill or future information.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from typing import Any, Mapping, Sequence


UTC = timezone.utc
POLICY_ID = "CR-001"
POLICY_VERSION = "cr-001-v1"
MIN_HISTORY_DAYS = 101
DEFAULT_CAP_CENTS = 500
DEFAULT_UNIT_PRECISION = 6


class ReplayError(ValueError):
    """The supplied history or replay configuration is not admissible."""


class InsufficientHistory(ReplayError):
    """The supplied history cannot form a CR-001 lookback."""


def _decimal(value: Any, field: str, *, positive: bool = False, minimum: Decimal | None = None) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise ReplayError(f"{field} must be a finite number")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ReplayError(f"{field} must be a finite number") from exc
    if not parsed.is_finite() or (positive and parsed <= 0) or (minimum is not None and parsed < minimum):
        raise ReplayError(f"{field} is outside its allowed range")
    return parsed


def _timestamp(value: Any, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ReplayError(f"{field} must be an ISO-8601 timestamp") from exc
    else:
        raise ReplayError(f"{field} must be an ISO-8601 timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ReplayError(f"{field} must include a timezone")
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _field(row: Mapping[str, Any], name: str, short: str, index: int) -> Any:
    value = row.get(name, row.get(short))
    if value is None:
        raise ReplayError(f"bars[{index}].{name} is required")
    return value


def _ceil_cents(dollars: Decimal) -> int:
    return int((dollars * 100).to_integral_value(rounding=ROUND_CEILING))


def _floor_cents(dollars: Decimal) -> int:
    return int((dollars * 100).to_integral_value(rounding=ROUND_FLOOR))


def _quantize_units(units: Decimal, precision: int) -> Decimal:
    return units.quantize(Decimal(1).scaleb(-precision), rounding=ROUND_FLOOR)


class CostAssumptions:
    """Deterministic per-side cost assumptions expressed in basis points."""

    __slots__ = ("fee_bps", "slippage_bps", "idea_cap_cents", "unit_precision")

    def __init__(
        self,
        *,
        fee_bps: Decimal | int | str = Decimal("10"),
        slippage_bps: Decimal | int | str = Decimal("5"),
        idea_cap_cents: int = DEFAULT_CAP_CENTS,
        unit_precision: int = DEFAULT_UNIT_PRECISION,
    ) -> None:
        self.fee_bps = _decimal(fee_bps, "fee_bps", minimum=Decimal(0))
        self.slippage_bps = _decimal(slippage_bps, "slippage_bps", minimum=Decimal(0))
        if self.fee_bps > Decimal("10000") or self.slippage_bps > Decimal("10000"):
            raise ReplayError("fee_bps and slippage_bps must be at most 10000")
        if type(idea_cap_cents) is not int or idea_cap_cents <= 0:
            raise ReplayError("idea_cap_cents must be a positive integer")
        if type(unit_precision) is not int or not 1 <= unit_precision <= 12:
            raise ReplayError("unit_precision must be between 1 and 12")
        self.idea_cap_cents = idea_cap_cents
        self.unit_precision = unit_precision

    @property
    def fee_rate(self) -> Decimal:
        return self.fee_bps / Decimal(10000)

    @property
    def slippage_rate(self) -> Decimal:
        return self.slippage_bps / Decimal(10000)

    @property
    def quantum(self) -> Decimal:
        return Decimal(1).scaleb(-self.unit_precision)

    @property
    def round_trip_bps(self) -> Decimal:
        return Decimal(2) * (self.fee_bps + self.slippage_bps)

    def as_dict(self) -> dict[str, Any]:
        return {
            "fee_bps_per_side": str(self.fee_bps),
            "slippage_bps_per_side": str(self.slippage_bps),
            "round_trip_bps": str(self.round_trip_bps),
            "idea_cap_cents": self.idea_cap_cents,
            "unit_precision": self.unit_precision,
        }


def validate_bars(bars: Sequence[Mapping[str, Any]], *, minimum_days: int = MIN_HISTORY_DAYS) -> tuple[dict[str, Any], ...]:
    """Normalize complete UTC daily OHLC rows and reject gaps or bad geometry."""
    if not isinstance(bars, (list, tuple)):
        raise ReplayError("bars must be a list or tuple")
    if type(minimum_days) is not int or minimum_days < 1:
        raise ReplayError("minimum_days must be a positive integer")
    clean: list[dict[str, Any]] = []
    previous: datetime | None = None
    for index, raw in enumerate(bars):
        if not isinstance(raw, Mapping):
            raise ReplayError(f"bars[{index}] must be an object")
        timestamp = _timestamp(_field(raw, "timestamp", "t", index), f"bars[{index}].timestamp")
        if timestamp.time() != datetime.min.time():
            raise ReplayError("daily bar timestamps must be UTC midnight")
        if raw.get("complete", True) is not True:
            raise ReplayError("all daily bars must be marked complete")
        if previous is not None and timestamp != previous + timedelta(days=1):
            raise ReplayError("daily bars must be unique and consecutive UTC days")
        values = {
            name: _decimal(_field(raw, name, short, index), f"bars[{index}].{name}", positive=True)
            for name, short in (("open", "o"), ("high", "h"), ("low", "l"), ("close", "c"))
        }
        if values["low"] > min(values["open"], values["close"]) or values["high"] < max(values["open"], values["close"]):
            raise ReplayError("daily bar has inconsistent OHLC geometry")
        clean.append({"timestamp": _iso(timestamp), "complete": True, **{key: str(value) for key, value in values.items()}})
        previous = timestamp
    if len(clean) < minimum_days:
        raise InsufficientHistory(f"need at least {minimum_days} complete daily bars")
    return tuple(clean)


def _signal(rows: Sequence[Mapping[str, Any]], index: int) -> dict[str, Any]:
    closes = [Decimal(row["close"]) for row in rows]
    highs = [Decimal(row["high"]) for row in rows]
    lows = [Decimal(row["low"]) for row in rows]
    mean = sum(closes[index - 100:index], Decimal(0)) / Decimal(100)
    prior_high = max(highs[index - 20:index])
    prior_low = min(lows[index - 10:index])
    close = closes[index]
    return {
        "signal_day": rows[index]["timestamp"],
        "signal_close": str(close),
        "prior_100_day_mean": str(mean),
        "prior_20_day_high": str(prior_high),
        "prior_10_day_low": str(prior_low),
        "entry_signal": close > mean and close > prior_high,
        "exit_signal": close < prior_low or close < mean,
    }


def _buy(open_price: Decimal, costs: CostAssumptions) -> tuple[Decimal, int, int, Decimal]:
    modeled = open_price * (Decimal(1) + costs.slippage_rate)
    # Search integer quanta so conservative cent rounding cannot exceed the cap.
    maximum = _quantize_units(Decimal(costs.idea_cap_cents) / 100 / (modeled * (Decimal(1) + costs.fee_rate)), costs.unit_precision)
    units = maximum
    while units > 0:
        gross = modeled * units
        gross_cents = _ceil_cents(gross)
        fee_cents = _ceil_cents(gross * costs.fee_rate)
        if gross_cents + fee_cents <= costs.idea_cap_cents:
            return units, gross_cents, fee_cents, modeled
        units = _quantize_units(units - costs.quantum, costs.unit_precision)
    return Decimal(0), 0, 0, modeled


def _sell(close_price: Decimal, units: Decimal, costs: CostAssumptions) -> tuple[int, int, Decimal]:
    modeled = close_price * (Decimal(1) - costs.slippage_rate)
    gross_cents = _floor_cents(modeled * units)
    fee_cents = _ceil_cents(modeled * units * costs.fee_rate)
    return gross_cents, fee_cents, modeled


def replay(bars: Sequence[Mapping[str, Any]], *, costs: CostAssumptions | None = None) -> dict[str, Any]:
    """Replay CR-001 chronologically and return a research-only result."""
    assumptions = costs or CostAssumptions()
    rows = validate_bars(bars)
    trades: list[dict[str, Any]] = []
    open_trade: dict[str, Any] | None = None
    signals: list[dict[str, Any]] = []

    # The final row has no next-day open.  It is still evaluated below as a
    # terminal signal, but never receives a fabricated same-close execution.
    for index in range(100, len(rows)):
        signal = _signal(rows, index)
        signal["index"] = index
        signal["execution_available"] = index + 1 < len(rows)
        signals.append(signal)
        if index + 1 >= len(rows):
            continue
        execution = rows[index + 1]
        if open_trade is None and signal["entry_signal"]:
            units, gross_cents, fee_cents, modeled = _buy(Decimal(execution["open"]), assumptions)
            if units <= 0:
                continue
            open_trade = {
                "entry_signal_day": signal["signal_day"],
                "entry_execution_day": execution["timestamp"],
                "entry_price": str(modeled),
                "entry_reference_open": execution["open"],
                "units": str(units),
                "entry_gross_cents": gross_cents,
                "entry_fee_cents": fee_cents,
            }
        elif open_trade is not None and signal["exit_signal"]:
            gross_cents, fee_cents, modeled = _sell(Decimal(execution["open"]), Decimal(open_trade["units"]), assumptions)
            entry_total = open_trade["entry_gross_cents"] + open_trade["entry_fee_cents"]
            exit_total = gross_cents - fee_cents
            trades.append({
                **open_trade,
                "exit_signal_day": signal["signal_day"],
                "exit_execution_day": execution["timestamp"],
                "exit_price": str(modeled),
                "exit_reference_open": execution["open"],
                "exit_gross_cents": gross_cents,
                "exit_fee_cents": fee_cents,
                "status": "closed",
                "net_pnl_cents": exit_total - entry_total,
            })
            open_trade = None

    terminal_mark: dict[str, Any] | None = None
    if open_trade is not None:
        final = rows[-1]
        gross_cents, fee_cents, modeled = _sell(Decimal(final["close"]), Decimal(open_trade["units"]), assumptions)
        entry_total = open_trade["entry_gross_cents"] + open_trade["entry_fee_cents"]
        terminal_mark = {
            **open_trade,
            "mark_day": final["timestamp"],
            "mark_reference_close": final["close"],
            "mark_price": str(modeled),
            "mark_gross_cents": gross_cents,
            "mark_exit_fee_cents": fee_cents,
            "status": "open_terminal",
            "terminal_mark_pnl_cents": gross_cents - fee_cents - entry_total,
            "exit_execution": "unavailable_next_utc_day",
        }

    realized = sum(trade["net_pnl_cents"] for trade in trades)
    unrealized = terminal_mark["terminal_mark_pnl_cents"] if terminal_mark else 0
    # A simple equal-cap buy-and-hold comparator begins at the first supplied
    # open and is marked at the last close using the same cost model.
    benchmark_units, benchmark_entry, benchmark_fee, _ = _buy(Decimal(rows[0]["open"]), assumptions)
    benchmark_gross, benchmark_exit_fee, _ = _sell(Decimal(rows[-1]["close"]), benchmark_units, assumptions)
    benchmark_pnl = benchmark_gross - benchmark_exit_fee - benchmark_entry - benchmark_fee if benchmark_units else 0
    return {
        "status": "research_only",
        "research_only": True,
        "execution_enabled": False,
        "admissible_for_paper": False,
        "policy_id": POLICY_ID,
        "policy_version": POLICY_VERSION,
        "bars": {"count": len(rows), "first_day": rows[0]["timestamp"], "last_day": rows[-1]["timestamp"]},
        "cost_assumptions": assumptions.as_dict(),
        "trades": trades,
        "terminal_position": terminal_mark,
        "signals": signals,
        "closed_trade_count": len(trades),
        "realized_pnl_cents": realized,
        "unrealized_pnl_cents": unrealized,
        "net_pnl_cents": realized + unrealized,
        "benchmark_pnl_cents": benchmark_pnl,
        "benchmark_units": str(benchmark_units),
    }


def cost_stress(
    bars: Sequence[Mapping[str, Any]],
    scenarios: Mapping[str, CostAssumptions] | None = None,
) -> dict[str, Any]:
    """Run predeclared cost scenarios over the same supplied bar history."""
    selected = scenarios or {
        "round_trip_10bps": CostAssumptions(fee_bps=Decimal("5"), slippage_bps=Decimal("0")),
        "round_trip_25bps": CostAssumptions(fee_bps=Decimal("12.5"), slippage_bps=Decimal("0")),
        "round_trip_50bps": CostAssumptions(fee_bps=Decimal("25"), slippage_bps=Decimal("0")),
    }
    if not isinstance(selected, Mapping) or not selected:
        raise ReplayError("scenarios must be a non-empty mapping")
    results = {}
    for name, assumption in selected.items():
        if not isinstance(name, str) or not name:
            raise ReplayError("scenario names must be non-empty strings")
        if not isinstance(assumption, CostAssumptions):
            raise ReplayError("scenarios must contain CostAssumptions")
        results[name] = replay(bars, costs=assumption)
    return {
        "status": "research_only",
        "research_only": True,
        "execution_enabled": False,
        "admissible_for_paper": False,
        "policy_id": POLICY_ID,
        "policy_version": POLICY_VERSION,
        "scenarios": results,
    }


__all__ = ["CostAssumptions", "InsufficientHistory", "ReplayError", "cost_stress", "replay", "validate_bars"]
