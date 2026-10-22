"""Provider-neutral, read-only admission for equity quote evidence.

The intraday strategy may screen with many sources, but a paper fill still
requires a timestamped bid/ask observation with an auditable source.  This
module is intentionally an offline boundary: it accepts already received
evidence and never calls a provider, broker, ledger, or scheduler.

Robinhood and TradingView are described explicitly because their currently
installed integrations do not meet this boundary.  A future provider can be
added only after it supplies all fields required by :func:`admit_quote`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import re
from typing import Any, Mapping


UTC = timezone.utc
DEFAULT_MAX_AGE = timedelta(seconds=1)
_DIGEST = re.compile(r"^[0-9a-fA-F]{64}$")
_SYMBOL = re.compile(r"^[A-Z][A-Z0-9.\-]{0,14}$")


class ProviderAdmissionError(ValueError):
    """A malformed quote evidence record cannot support paper admission."""


@dataclass(frozen=True, slots=True)
class ProviderCapability:
    """Evidence capabilities observed in the installed provider integration."""

    provider: str
    callable_scope: str
    current_bid_ask: bool
    provider_event_timestamp: bool
    stable_provenance: bool
    executable_evidence: bool
    blocker: str | None = None


@dataclass(frozen=True, slots=True)
class QuoteEvidence:
    """Canonical quote evidence accepted by the paper-only boundary."""

    provider: str
    source_version: str
    symbol: str
    bid: Decimal
    ask: Decimal
    quote_at: datetime
    received_at: datetime
    provenance_sha256: str
    feed: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "source_version": self.source_version,
            "symbol": self.symbol,
            "bid": format(self.bid, "f"),
            "ask": format(self.ask, "f"),
            "quote_at": self.quote_at.isoformat(),
            "received_at": self.received_at.isoformat(),
            "provenance_sha256": self.provenance_sha256,
            "feed": self.feed,
        }


@dataclass(frozen=True, slots=True)
class AdmissionResult:
    """Deterministic result suitable for a retained abstention record."""

    admitted: bool
    provider: str
    symbol: str
    reason_code: str | None = None
    reason: str | None = None
    quote: QuoteEvidence | None = None

    def as_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "admitted": self.admitted,
            "provider": self.provider,
            "symbol": self.symbol,
            "reason_code": self.reason_code,
            "reason": self.reason,
        }
        if self.quote is not None:
            result["quote"] = self.quote.as_dict()
        return result


# These are capabilities of the checked-in integrations, not assumptions
# about products in general.  The Robinhood cloud transport's allowlist is
# options/account oriented. TradingView returns screening/chart/last-price
# observations; neither integration currently returns executable US equity
# bid/ask evidence with a provider event timestamp.
PROVIDER_CAPABILITIES: dict[str, ProviderCapability] = {
    "robinhood": ProviderCapability(
        provider="robinhood",
        callable_scope="options instruments/quotes and account read methods",
        current_bid_ask=False,
        provider_event_timestamp=False,
        stable_provenance=False,
        executable_evidence=False,
        blocker="Robinhood integration exposes no callable US equity quote or bar method",
    ),
    "tradingview": ProviderCapability(
        provider="tradingview",
        callable_scope="stock screener/chart indicators and Yahoo-backed price fields",
        current_bid_ask=False,
        provider_event_timestamp=False,
        stable_provenance=False,
        executable_evidence=False,
        blocker="TradingView integration exposes no current bid/ask pair with a provider event timestamp",
    ),
    # This is an evidence contract, not an entitlement claim. Current SIP
    # entitlement is still checked by equity_intraday_sip.SIPClient.
    "alpaca_sip": ProviderCapability(
        provider="alpaca_sip",
        callable_scope="SIP bars and bid/ask quotes through the existing bounded adapter",
        current_bid_ask=True,
        provider_event_timestamp=True,
        stable_provenance=True,
        executable_evidence=True,
    ),
}


def provider_capability(provider: str) -> ProviderCapability | None:
    """Return the installed integration's declared capability, if known."""

    return PROVIDER_CAPABILITIES.get(str(provider).strip().lower())


def _timestamp(value: Any, label: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ProviderAdmissionError(f"{label} must be an ISO timestamp") from exc
    else:
        raise ProviderAdmissionError(f"{label} must be an ISO timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ProviderAdmissionError(f"{label} must include a timezone")
    return parsed.astimezone(UTC)


def _number(value: Any, label: str) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise ProviderAdmissionError(f"{label} must be a positive finite number")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ProviderAdmissionError(f"{label} must be a positive finite number") from exc
    if not number.is_finite() or number <= 0:
        raise ProviderAdmissionError(f"{label} must be a positive finite number")
    return number


def _symbol(value: Any, label: str = "symbol") -> str:
    if not isinstance(value, str):
        raise ProviderAdmissionError(f"{label} must be an uppercase equity symbol")
    symbol = value.strip()
    if not _SYMBOL.fullmatch(symbol):
        raise ProviderAdmissionError(f"{label} must be an uppercase equity symbol")
    return symbol


def _digest(value: Any) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise ProviderAdmissionError("provenance_sha256 must be a 64-character digest")
    return value.lower()


def _canonical_quote(
    evidence: Mapping[str, Any], *, expected_symbol: str, as_of: datetime,
    max_age: timedelta,
) -> QuoteEvidence:
    if not isinstance(evidence, Mapping):
        raise ProviderAdmissionError("quote evidence must be an object")
    provider = evidence.get("provider", evidence.get("source"))
    if not isinstance(provider, str) or not provider.strip():
        raise ProviderAdmissionError("provider is required")
    provider = provider.strip().lower()
    source_version = evidence.get("source_version", evidence.get("provider_version"))
    if not isinstance(source_version, str) or not source_version.strip():
        raise ProviderAdmissionError("source_version is required")
    symbol = _symbol(evidence.get("symbol", evidence.get("ticker")))
    if symbol != expected_symbol:
        raise ProviderAdmissionError("quote symbol does not match requested symbol")
    bid = _number(evidence.get("bid", evidence.get("bid_price", evidence.get("bp"))), "bid")
    ask = _number(evidence.get("ask", evidence.get("ask_price", evidence.get("ap"))), "ask")
    if bid > ask:
        raise ProviderAdmissionError("quote is crossed")
    quote_at = _timestamp(evidence.get("quote_at", evidence.get("timestamp", evidence.get("t"))), "quote_at")
    received_at = _timestamp(evidence.get("received_at", evidence.get("receipt_at")), "received_at")
    if quote_at > received_at:
        raise ProviderAdmissionError("received_at precedes quote_at")
    if received_at > as_of:
        raise ProviderAdmissionError("received_at is in the future")
    if as_of - quote_at > max_age:
        raise ProviderAdmissionError("quote is stale")
    if as_of - received_at > max_age:
        raise ProviderAdmissionError("quote receipt is stale")
    provenance = _digest(evidence.get("provenance_sha256", evidence.get("source_sha256")))
    feed = evidence.get("feed")
    if not isinstance(feed, str) or not feed.strip():
        raise ProviderAdmissionError("feed is required for provenance")
    return QuoteEvidence(provider, source_version.strip(), symbol, bid, ask, quote_at, received_at, provenance, feed.strip())


def admit_quote(
    provider: str, evidence: Mapping[str, Any], *, expected_symbol: str,
    as_of: datetime, max_age: timedelta = DEFAULT_MAX_AGE,
) -> AdmissionResult:
    """Evaluate one already-received quote without making external calls.

    Known Robinhood and TradingView integrations fail closed with a clear
    external-data blocker even if a caller supplies a last-price/chart record.
    A valid ``alpaca_sip`` record is accepted only as evidence; this function
    never enables a broker order or changes the existing SIP entitlement gate.
    """

    requested = _symbol(expected_symbol, "expected_symbol")
    if not isinstance(as_of, datetime) or as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ProviderAdmissionError("as_of must be timezone-aware")
    now = as_of.astimezone(UTC)
    if not isinstance(max_age, timedelta) or max_age <= timedelta(0):
        raise ProviderAdmissionError("max_age must be positive")
    name = str(provider).strip().lower()
    capability = provider_capability(name)
    if capability is None:
        return AdmissionResult(False, name, requested, "unknown_provider", "provider capability is not registered")
    if not (capability.current_bid_ask and capability.provider_event_timestamp and capability.stable_provenance):
        return AdmissionResult(False, name, requested, "provider_capability_insufficient", capability.blocker)
    try:
        quote = _canonical_quote(evidence, expected_symbol=requested, as_of=now, max_age=max_age)
    except ProviderAdmissionError as exc:
        return AdmissionResult(False, name, requested, "invalid_quote_evidence", str(exc))
    if quote.provider != name:
        return AdmissionResult(False, name, requested, "provider_identity_mismatch", "quote provider does not match adapter")
    if name == "alpaca_sip" and quote.feed.lower() != "sip":
        return AdmissionResult(False, name, requested, "wrong_feed", "paper admission requires explicit SIP feed evidence")
    return AdmissionResult(True, name, requested, quote=quote)


__all__ = [
    "AdmissionResult", "DEFAULT_MAX_AGE", "PROVIDER_CAPABILITIES", "ProviderAdmissionError",
    "ProviderCapability", "QuoteEvidence", "admit_quote", "provider_capability",
]
