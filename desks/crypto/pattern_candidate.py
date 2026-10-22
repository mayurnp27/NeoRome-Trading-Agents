"""MC-PATTERN-01: offline hypothesis evaluation, with no execution adapter.

Feature matches and exit signals are research observations, never orders or
fills. Snapshot ingestion, shared risk, sizing quotes and a prospective ledger
remain prerequisites for any separately authorized paper activation.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

VERSION = "mc-pattern-01-v1"
MAX_IDEA_CENTS = 500


def _number(value: Any) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError("missing or invalid number")
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("invalid number") from exc
    if not result.is_finite():
        raise ValueError("nonfinite number")
    return result


def _time(value: str | datetime) -> datetime:
    result = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("timezone required")
    return result.astimezone(timezone.utc)


def feature_match(*, liquidity_usd: Any, volume_h1_usd: Any, age_hours: Any) -> bool:
    """Descriptive numerical screen only; does not verify provenance or safety."""
    try:
        liquidity, volume, age = map(_number, (liquidity_usd, volume_h1_usd, age_hours))
    except ValueError:
        return False
    return (Decimal(25_000) <= liquidity <= Decimal(250_000)
            and volume >= liquidity and Decimal(0) <= age < Decimal(24))


def entry_screen(snapshot: Mapping[str, Any], *, mint: str, pool: str,
                 now: datetime) -> dict[str, Any]:
    """Require a single identified pool, known-at-decision data and a source hash.

    `observed_at` is the market snapshot timestamp; `available_at` is when we
    received it. Age is advanced from observation to decision time. Both clocks
    must be timezone aware. Hash presence is provenance metadata, not a verified
    cryptographic attestation; the eventual ingestion adapter must verify it.
    """
    result = {"version": VERSION, "feature_match": False, "research_candidate": False,
              "execution_enabled": False, "action": "observe_only", "reasons": []}
    reasons = result["reasons"]
    try:
        current = _time(now)
        observed, available = _time(snapshot["observed_at"]), _time(snapshot["available_at"])
        if not mint or not pool or snapshot.get("mint") != mint or snapshot.get("pool") != pool:
            reasons.append("identity_mismatch")
        if not observed <= available <= current or not 0 <= (current - observed).total_seconds() <= 60:
            reasons.append("stale_or_noncausal_snapshot")
        source_hash = snapshot.get("source_sha256", "")
        if not isinstance(source_hash, str) or len(source_hash) != 64 or any(c not in "0123456789abcdef" for c in source_hash):
            reasons.append("missing_source_hash")
        age = _number(snapshot["age_hours_at_observation"]) + Decimal(str((current - observed).total_seconds())) / 3600
        if _number(snapshot["age_hours_at_observation"]) < 0:
            reasons.append("negative_pool_age")
        result["feature_match"] = feature_match(liquidity_usd=snapshot["liquidity_usd"],
            volume_h1_usd=snapshot["volume_h1_usd"], age_hours=age)
        if not result["feature_match"]:
            reasons.append("outside_feature_rules")
    except (KeyError, ValueError, TypeError, AttributeError, OverflowError):
        reasons.append("invalid_snapshot")
    result["research_candidate"] = not reasons
    return result


def exit_signal(*, basis_cents: int, liquidation_net_cents: int,
                opened_at: datetime, quote_at: datetime, now: datetime) -> dict[str, Any]:
    """Signal from full-position net liquidation, including modeled exit costs.

    Basis includes all entry costs. Values must come from a quote sized for the
    entire position; this arithmetic cannot verify quote routing or realizability.
    An exit signal is never a claim of execution at the threshold. Stale quotes
    remain unknown, including after the time limit; never invent proceeds.
    """
    if type(basis_cents) is not int or basis_cents <= 0 or type(liquidation_net_cents) is not int or liquidation_net_cents < 0:
        raise ValueError("require positive basis and nonnegative net proceeds in integer cents")
    opened, quoted, current = map(_time, (opened_at, quote_at, now))
    if not opened <= quoted <= current:
        raise ValueError("noncausal position/quote timestamps")
    elapsed = (current - opened).total_seconds()
    result = {"version": VERSION, "execution_enabled": False, "action": "observe_only",
              "exit_signal": None, "time_limit_due": elapsed >= 7200}
    if (current - quoted).total_seconds() > 15:
        result["exit_signal"] = "quote_unavailable"
    elif liquidation_net_cents * 2 >= basis_cents * 3:
        result["exit_signal"] = "take_profit_50_net"
    elif liquidation_net_cents * 4 <= basis_cents * 3:
        result["exit_signal"] = "stop_25_net"
    elif elapsed >= 7200:
        result["exit_signal"] = "time_limit_120_minutes"
    else:
        result["exit_signal"] = "hold"
    return result
