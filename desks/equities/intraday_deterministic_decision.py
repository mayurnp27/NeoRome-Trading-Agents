"""Point-in-time ORB15/VWAP decision callback for observed-shadow intake.

The callback receives only archived pre-signal inputs from
``intraday_shadow_driver``.  It never sees entry quotes, contacts a provider,
or opens a ledger.  Missing universe eligibility, bar quality, or a complete
minute prefix produces an explicit abstention.
"""

from __future__ import annotations

from datetime import datetime, timedelta, time, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping
from zoneinfo import ZoneInfo


UTC = timezone.utc
NY = ZoneInfo("America/New_York")


def _time(value: Any, label: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be an ISO timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"{label} must include a timezone")
    return parsed.astimezone(UTC)


def _number(value: Any, label: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be numeric") from exc
    if not result.is_finite() or result <= 0:
        raise ValueError(f"{label} must be positive finite")
    return result


def _field(row: Mapping[str, Any], long: str, short: str) -> Any:
    return row[long] if long in row else row.get(short)


def _bar_rows(raw: Any, symbol: str, signal: datetime) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        raise ValueError("bar prefix payload must be a list")
    output: list[dict[str, Any]] = []
    for row in raw:
        if not isinstance(row, Mapping) or str(row.get("symbol", symbol)).strip() != symbol:
            continue
        stamp = _time(_field(row, "timestamp", "t"), "bar timestamp")
        if stamp + timedelta(minutes=1) > signal:
            continue
        output.append({"timestamp": stamp, "high": _number(_field(row, "high", "h"), "bar high"),
                       "close": _number(_field(row, "close", "c"), "bar close"),
                       "volume": _number(_field(row, "volume", "v"), "bar volume"),
                       "vwap": _number(_field(row, "vwap", "vw"), "bar vwap")})
    output.sort(key=lambda item: item["timestamp"])
    return output


def _complete_prefix(rows: list[dict[str, Any]], signal: datetime) -> tuple[bool, str]:
    if not rows:
        return False, "no completed regular-session bars"
    session = signal.astimezone(NY).date()
    start = datetime.combine(session, time(9, 30), tzinfo=NY).astimezone(UTC)
    latest = rows[-1]["timestamp"]
    expected = [start + timedelta(minutes=index) for index in range(int((latest - start).total_seconds() // 60) + 1)]
    # The local decision receipt may lag the completed bar.  That lag is
    # retained as the candidate signal time; requiring equality would either
    # invent impossible pre-close receipt timing or silently discard a valid
    # conservative decision.
    if latest + timedelta(minutes=1) > signal or [row["timestamp"] for row in rows] != expected:
        return False, "bar prefix is incomplete through the signal"
    return True, ""


def _eligible_universe(rows: Any, symbol: str) -> bool:
    if not isinstance(rows, list):
        return False
    return any(isinstance(row, Mapping) and row.get("symbol") == symbol
               and row.get("permanent_id") and (row.get("eligible") is True or row.get("universe_eligible") is True
                                                  or row.get("watchlist_member") is True)
               for row in rows)


def deterministic_decision(inputs: Mapping[str, Any], *, fee_cents_per_unit: int = 0, units: int = 1) -> dict[str, Any]:
    """Return a frozen candidate or an explicit, retained no-trade reason."""

    try:
        if not isinstance(inputs, Mapping):
            raise ValueError("decision input must be an object")
        if isinstance(fee_cents_per_unit, bool) or not isinstance(fee_cents_per_unit, int) or fee_cents_per_unit < 0:
            raise ValueError("fee_cents_per_unit must be a non-negative integer")
        if isinstance(units, bool) or not isinstance(units, int) or units <= 0:
            raise ValueError("units must be a positive whole number")
        signal = _time(inputs.get("signal_at"), "signal_at")
        symbol = str(inputs.get("symbol", "")).strip()
        raw = inputs.get("raw")
        evidence = inputs.get("evidence")
        if not symbol or not isinstance(raw, Mapping) or not isinstance(evidence, Mapping):
            raise ValueError("symbol, raw evidence, and evidence hashes are required")
        if not _eligible_universe(raw.get("universe"), symbol):
            return {"action": "abstain", "reason": "frozen universe does not explicitly admit symbol"}
        if raw.get("bar_prefix_quality_clear") is not True:
            return {"action": "abstain", "reason": "bar prefix quality is not explicitly clear"}
        rows = _bar_rows(raw.get("bar_prefix"), symbol, signal)
        complete, reason = _complete_prefix(rows, signal)
        if not complete:
            return {"action": "abstain", "reason": reason}
        calendar = raw.get("calendar")
        if not isinstance(calendar, Mapping):
            return {"action": "abstain", "reason": "calendar close is unavailable"}
        close = _time(calendar.get("session_close"), "session_close")
        local_signal = signal.astimezone(NY)
        if close.astimezone(NY).date() != local_signal.date():
            return {"action": "abstain", "reason": "calendar close is not in signal session"}
        opening = [row for row in rows if time(9, 30) <= row["timestamp"].astimezone(NY).time() < time(9, 45)]
        if len(opening) != 15:
            return {"action": "abstain", "reason": "opening range is incomplete"}
        latest = rows[-1]
        strategy: str | None = None
        opening_high = max(row["high"] for row in opening)
        later = [row for row in rows if row["timestamp"].astimezone(NY).time() >= time(9, 45)]
        if local_signal.time() >= time(9, 46) and latest["close"] > opening_high and all(row["close"] <= opening_high for row in later[:-1]):
            strategy = "orb15"
        if strategy is None and local_signal.time() >= time(10, 1) and len(rows) >= 2:
            cumulative_volume = Decimal(0)
            cumulative_notional = Decimal(0)
            vwaps: list[Decimal] = []
            for row in rows:
                cumulative_volume += row["volume"]
                cumulative_notional += row["vwap"] * row["volume"]
                vwaps.append(cumulative_notional / cumulative_volume)
            if rows[-2]["close"] < vwaps[-2] and latest["close"] > vwaps[-1]:
                strategy = "vwap_reclaim"
        if strategy is None:
            return {"action": "abstain", "reason": "no frozen ORB15 or VWAP-reclaim signal"}
        candidate_id = f"eq-intraday-{strategy}-{symbol}-{signal.strftime('%Y%m%dT%H%M%SZ')}"
        candidate = {
            "candidate_id": candidate_id, "strategy_id": strategy, "symbol": symbol,
            "signal_timestamp": signal.isoformat().replace("+00:00", "Z"),
            "exit_decision_timestamp": (close - timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
            "session_close": close.isoformat().replace("+00:00", "Z"),
            "universe_eligible": True, "prefix_complete_through_signal": True,
            "bar_quality_clear": True, "size_units_verified": True, "fractional_supported": False,
            "fee_cents_per_unit": fee_cents_per_unit, "units": units,
            "universe_evidence_sha256": evidence["universe_evidence_sha256"],
            "bar_prefix_sha256": evidence["bar_prefix_sha256"],
            "quote_evidence_sha256": evidence["quote_evidence_sha256"],
            "universe_known_at": evidence["universe_known_at"],
            "bar_prefix_known_at": evidence["bar_prefix_known_at"],
            "calendar_known_at": evidence["calendar_known_at"],
        }
        if raw.get("universe_method") == "prospective_fixed_watchlist":
            candidate["universe_method"] = "prospective_fixed_watchlist"
            candidate["research_label"] = "prospective_fixed_watchlist_evidence_only"
        return {"action": "candidate", "candidate": candidate}
    except (KeyError, ValueError) as exc:
        return {"action": "abstain", "reason": f"deterministic input refused: {exc}"}


__all__ = ["deterministic_decision"]
