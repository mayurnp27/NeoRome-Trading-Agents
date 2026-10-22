"""Offline eligibility and cost model for the SPY internal paper comparator.

The policy consumes already captured observations and an explicit session
calendar.  It never fetches data, touches credentials, submits orders, or
writes a ledger.  Prices and fees are deliberately modeled assumptions; they
are not a broker fee schedule and do not guarantee a fill.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
import re


UTC = timezone.utc
STRATEGY_VERSION = "EQ-ETF-TREND-COMPARATOR-v1"
MAX_ENTRY_CENTS = 1500
ENTRY_FEE_BPS = Decimal("10")
EXIT_FEE_BPS = Decimal("10")
ENTRY_SLIPPAGE_BPS = Decimal("5")
EXIT_SLIPPAGE_BPS = Decimal("5")
FEE_MINIMUM_CENTS = 1
UNITS_QUANTUM = Decimal("0.000001")
ALLOWED_EXCHANGES = frozenset(("ARCA", "NYSE", "NASDAQ", "AMEX", "BATS"))


def _zero_result():
    return {
        "eligible_for_internal_paper": False,
        "reasons": [],
        "units": "0.000000",
        # entry_cost_cents is all-in: gross entry plus entry_fee_cents.
        "entry_cost_cents": 0,
        "entry_fee_cents": 0,
        "liquidation_value_cents": 0,
        "strategy_version": STRATEGY_VERSION,
        "broker_execution_enabled": False,
        "signal": False,
        "position_assumption": "No existing position assumed",
        "market_inputs_valid": False,
        "rebalance_due": False,
        "trend_positive": False,
        "cost_model": {
            "entry_fee_bps": "10",
            "exit_fee_bps": "10",
            "entry_slippage_bps": "5",
            "exit_slippage_bps": "5",
            "fee_minimum_cents": 1,
            "maximum_all_in_entry_cents": MAX_ENTRY_CENTS,
            "fills_guaranteed": False,
        },
    }


def _decimal(value, label, *, positive=False, nonnegative=False):
    if isinstance(value, bool) or value is None:
        raise ValueError("{} is not finite numeric data".format(label))
    if not isinstance(value, (str, int, float, Decimal)):
        raise ValueError("{} is not finite numeric data".format(label))
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError("{} is not finite numeric data".format(label)) from None
    if not number.is_finite():
        raise ValueError("{} is not finite numeric data".format(label))
    if positive and number <= 0:
        raise ValueError("{} must be positive".format(label))
    if nonnegative and number < 0:
        raise ValueError("{} must be nonnegative".format(label))
    return number


def _utc_datetime(value, label):
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str):
        text = value[:-1] + "+00:00" if value.endswith("Z") else value
        try:
            result = datetime.fromisoformat(text)
        except (TypeError, ValueError):
            raise ValueError("{} is not an ISO timestamp".format(label)) from None
    else:
        raise ValueError("{} is not an ISO timestamp".format(label))
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("{} requires timezone".format(label))
    return result.astimezone(UTC)


def _utc_date(value, label):
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("{} is not an ISO date".format(label))
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise ValueError("{} is not an ISO date".format(label)) from None


def _ceil_cents(amount):
    return int((amount * Decimal("100")).to_integral_value(rounding=ROUND_CEILING))


def _floor_cents(amount):
    return int((amount * Decimal("100")).to_integral_value(rounding=ROUND_FLOOR))


def _floor_units(value):
    return value.quantize(UNITS_QUANTUM, rounding=ROUND_FLOOR)


def _format_units(value):
    return format(value.quantize(UNITS_QUANTUM, rounding=ROUND_FLOOR), "f")


def _fee_cents(gross_cents, gross_dollars, bps):
    if gross_cents <= 0:
        return 0
    modeled = _ceil_cents(gross_dollars * bps / Decimal("10000"))
    return max(FEE_MINIMUM_CENTS, modeled)


def _quote_prices(quote):
    if not isinstance(quote, dict):
        raise ValueError("Quote is missing")
    bid = _decimal(quote.get("bid"), "quote bid", positive=True)
    ask = _decimal(quote.get("ask"), "quote ask", positive=True)
    bid_size = _decimal(quote.get("bid_size"), "quote bid size", positive=True)
    ask_size = _decimal(quote.get("ask_size"), "quote ask size", positive=True)
    if ask < bid:
        raise ValueError("Quote is crossed")
    return bid, ask, bid_size, ask_size


def _entry_values(units, ask):
    slipped = ask * (Decimal("1") + ENTRY_SLIPPAGE_BPS / Decimal("10000"))
    gross_dollars = units * slipped
    gross_cents = _ceil_cents(gross_dollars)
    fee = _fee_cents(gross_cents, gross_dollars, ENTRY_FEE_BPS)
    return gross_dollars, gross_cents, fee, gross_cents + fee, slipped


def value_position(units, quote):
    """Return conservative modeled exit cents for an open fractional lot.

    The returned mapping contains the gross bid-side proceeds after 5 bps
    adverse exit slippage, the modeled 10 bps fee (minimum one cent and
    clipped at gross proceeds), and the resulting net liquidation value.
    """
    quantity = _decimal(units, "units", positive=True)
    quantity = _floor_units(quantity)
    if quantity <= 0:
        raise ValueError("units must contain at least one micro-share")
    bid, _ask, bid_size, _ask_size = _quote_prices(quote)
    if quantity > bid_size:
        raise ValueError("Units exceed executable bid depth")
    slipped = bid * (Decimal("1") - EXIT_SLIPPAGE_BPS / Decimal("10000"))
    gross_dollars = quantity * slipped
    gross_cents = _floor_cents(gross_dollars)
    fee = min(gross_cents, _fee_cents(gross_cents, gross_dollars, EXIT_FEE_BPS))
    return {
        "gross_exit_cents": gross_cents,
        "exit_fee_cents": fee,
        "liquidation_value_cents": gross_cents - fee,
    }


def _validate_bars(observation, sessions_by_date, today):
    rows = observation.get("bars") if isinstance(observation, dict) else None
    if not isinstance(rows, list) or len(rows) < 200:
        raise ValueError("Fewer than 200 completed daily bars")
    parsed = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError("Daily bar {} is invalid".format(index))
        timestamp = _utc_datetime(row.get("timestamp"), "bar timestamp")
        prices = {
            key: _decimal(row.get(key), "bar {} {}".format(index, key), positive=True)
            for key in ("o", "h", "l", "c")
        }
        if prices["l"] > min(prices["o"], prices["c"]):
            raise ValueError("Daily bar low is inconsistent")
        if prices["h"] < max(prices["o"], prices["c"]):
            raise ValueError("Daily bar high is inconsistent")
        if prices["l"] > prices["h"]:
            raise ValueError("Daily bar range is inconsistent")
        _decimal(row.get("volume"), "bar {} volume".format(index), positive=True)
        if parsed and timestamp <= parsed[-1][0]:
            raise ValueError("Daily bars are unordered or duplicated")
        parsed.append((timestamp, prices["c"]))

    dates = [timestamp.date() for timestamp, _close in parsed]
    expected = [item for item in sessions_by_date if item < today]
    if len(expected) < 200 or dates[-200:] != expected[-200:]:
        raise ValueError("Previous 200 completed session dates do not match last 200 bars")
    if dates[-1] != expected[-1]:
        raise ValueError("Last bar is not the immediately prior completed session")
    return parsed


def _validate_sessions(context, now):
    sessions = context.get("sessions") if isinstance(context, dict) else None
    if not isinstance(sessions, list) or len(sessions) < 200:
        raise ValueError("Session calendar is missing")
    start = _utc_date(context.get("calendar_start"), "calendar_start")
    end = _utc_date(context.get("calendar_end"), "calendar_end")
    if start > end or (end - start).days + 1 < 500:
        raise ValueError("Session calendar does not cover 500 calendar days")
    today = now.date()
    if today < start or today > end:
        raise ValueError("Session calendar does not cover today")

    records = []
    seen = set()
    for row in sessions:
        if not isinstance(row, dict):
            raise ValueError("Session calendar row is invalid")
        session_date = _utc_date(row.get("date"), "session date")
        if session_date in seen or session_date < start or session_date > end:
            raise ValueError("Session calendar dates are invalid or duplicated")
        opened = _utc_datetime(row.get("open_at"), "session open_at")
        closed = _utc_datetime(row.get("close_at"), "session close_at")
        if opened.date() != session_date or closed.date() != session_date or not opened < closed:
            raise ValueError("Session hours are invalid")
        seen.add(session_date)
        records.append((session_date, opened, closed))
    records.sort(key=lambda row: row[0])
    dates = [row[0] for row in records]
    if today not in seen:
        raise ValueError("Today is missing from the session calendar")
    current = next(row for row in records if row[0] == today)
    if not current[1] <= now < current[2]:
        raise ValueError("Current time is outside today's regular session")
    return records, dates, current


def evaluate(observation, context, now):
    """Evaluate one deterministic SPY internal-paper entry opportunity."""
    result = _zero_result()
    reasons = []
    try:
        evaluation_time = _utc_datetime(now, "now")
    except ValueError as exc:
        reasons.append(str(exc))
        result["reasons"] = reasons
        return result

    if not isinstance(observation, dict):
        reasons.append("Observation is missing")
        result["reasons"] = reasons
        return result
    if observation.get("symbol") != "SPY":
        reasons.append("Only the predeclared SPY comparator is eligible")
    if observation.get("source_kind") not in ("observed", "synthetic"):
        reasons.append("Observation source_kind must be observed or synthetic")
    if observation.get("feed") != "iex":
        reasons.append("Observation feed must be iex")

    received = clock_timestamp = quote_timestamp = observed_timestamp = None
    try:
        received = _utc_datetime(context.get("received_at"), "Context received_at")
        clock_timestamp = _utc_datetime(context.get("clock", {}).get("timestamp"), "Clock timestamp")
        quote_timestamp = _utc_datetime(observation.get("quote", {}).get("timestamp"), "Quote timestamp")
        observed_timestamp = _utc_datetime(observation.get("observed_at"), "Observation observed_at")
        for label, timestamp in (("Context received_at", received), ("Clock timestamp", clock_timestamp), ("Quote timestamp", quote_timestamp), ("Observation observed_at", observed_timestamp)):
            age = (evaluation_time - timestamp).total_seconds()
            if age < 0 or age > 60:
                reasons.append(label + " is stale or in the future")
        if quote_timestamp > observed_timestamp:
            reasons.append("Quote timestamp follows observation receipt")
    except (AttributeError, TypeError, ValueError) as exc:
        reasons.append(str(exc))

    clock = context.get("clock") if isinstance(context, dict) else None
    if not isinstance(clock, dict) or clock.get("is_open") is not True:
        reasons.append("Clock is not open")

    asset = context.get("asset") if isinstance(context, dict) else None
    if not isinstance(asset, dict):
        reasons.append("Asset metadata is missing")
    else:
        if asset.get("symbol") != observation.get("symbol"):
            reasons.append("Asset symbol does not match observation")
        if asset.get("asset_class") != "us_equity":
            reasons.append("Asset class is not us_equity")
        if asset.get("status") != "active":
            reasons.append("Asset is not active")
        if asset.get("tradable") is not True:
            reasons.append("Asset is not tradable")
        if asset.get("fractionable") is not True:
            reasons.append("Asset is not fractionable")
        if asset.get("exchange") not in ALLOWED_EXCHANGES:
            reasons.append("Asset exchange is not an allowed non-OTC venue")

    try:
        records, session_dates, current = _validate_sessions(context, evaluation_time)
        sessions_by_date = [row[0] for row in records]
        parsed_bars = _validate_bars(observation, sessions_by_date, evaluation_time.date())
    except (AttributeError, TypeError, ValueError) as exc:
        records = []
        parsed_bars = []
        reasons.append(str(exc))

    if quote_timestamp is not None and records:
        if not current[1] <= quote_timestamp < current[2]:
            reasons.append("Quote timestamp is outside today's regular session")

    try:
        bid, ask, bid_size, ask_size = _quote_prices(observation.get("quote"))
    except (AttributeError, TypeError, ValueError) as exc:
        bid = ask = bid_size = ask_size = None
        reasons.append(str(exc))

    if parsed_bars:
        closes = [close for _timestamp, close in parsed_bars[-200:]]
        trend_positive = closes[-1] > sum(closes, Decimal("0")) / Decimal("200")
        result["trend_positive"] = trend_positive
        result["signal"] = trend_positive
        if not trend_positive:
            reasons.append("Prior close is not above the 200-session mean")

    rebalance_due = False
    if records:
        today = evaluation_time.date()
        month_sessions = [row[0] for row in records if row[0].year == today.year and row[0].month == today.month]
        rebalance_due = bool(month_sessions and today == month_sessions[0])
        result["rebalance_due"] = rebalance_due
        if not rebalance_due:
            reasons.append("Today is not the first trading session of the UTC month")

    market_inputs_valid = not reasons or all(
        reason in (
            "Prior close is not above the 200-session mean",
            "Today is not the first trading session of the UTC month",
        )
        for reason in reasons
    )
    result["market_inputs_valid"] = market_inputs_valid

    if market_inputs_valid and rebalance_due and result["trend_positive"] and bid is not None:
        slipped_ask = ask * (Decimal("1") + ENTRY_SLIPPAGE_BPS / Decimal("10000"))
        depth_ticks = int((min(bid_size, ask_size) / UNITS_QUANTUM).to_integral_value(rounding=ROUND_FLOOR))
        price_ticks = int((Decimal(MAX_ENTRY_CENTS) / Decimal("100") / slipped_ask / UNITS_QUANTUM).to_integral_value(rounding=ROUND_FLOOR))
        # Search integer micro-share ticks.  This stays bounded even for a
        # very cheap quote and accounts for the one-cent minimum fee exactly.
        low, high = 0, min(depth_ticks, price_ticks)
        while low < high:
            midpoint = (low + high + 1) // 2
            candidate = Decimal(midpoint) * UNITS_QUANTUM
            _gross, _gross_cents, _fee, total, _slipped = _entry_values(candidate, ask)
            if total <= MAX_ENTRY_CENTS:
                low = midpoint
            else:
                high = midpoint - 1
        units = Decimal(low) * UNITS_QUANTUM
        if units <= 0:
            reasons.append("Quote depth or price cannot fund one micro-share")
        else:
            _gross, gross_cents, fee, total, entry_price = _entry_values(units, ask)
            exit_values = value_position(units, observation["quote"])
            result.update({
                "eligible_for_internal_paper": True,
                "units": _format_units(units),
                "entry_cost_cents": total,
                "entry_fee_cents": fee,
                "liquidation_value_cents": exit_values["liquidation_value_cents"],
                "gross_entry_cents": gross_cents,
                "gross_exit_cents": exit_values["gross_exit_cents"],
                "exit_fee_cents": exit_values["exit_fee_cents"],
                "entry_price": format(entry_price, "f"),
                "exit_price": format(bid * (Decimal("1") - EXIT_SLIPPAGE_BPS / Decimal("10000")), "f"),
            })
    result["reasons"] = reasons
    return result


__all__ = ["STRATEGY_VERSION", "evaluate", "value_position"]
