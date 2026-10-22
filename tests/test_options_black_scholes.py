from datetime import datetime, timezone
from decimal import Decimal

import pytest

from neorome.options_black_scholes import EnrichmentError, UnderlyingInputs, delta_from_iv, enrich_quote, implied_volatility


AS_OF = datetime(2026, 1, 2, 15, 0, tzinfo=timezone.utc)


def test_black_scholes_round_trip_and_delta():
    price = Decimal("7.9656")
    iv = implied_volatility(price=price, spot="100", strike="100", expiry=AS_OF.date().replace(year=2027), as_of=AS_OF, rate="0", dividend_yield="0", right="C")
    assert abs(iv - Decimal("0.20")) < Decimal("0.001")
    delta = delta_from_iv(spot="100", strike="100", expiry=AS_OF.date().replace(year=2027), as_of=AS_OF, rate="0", dividend_yield="0", volatility=iv, right="C")
    assert Decimal("0.50") < delta < Decimal("0.55")


def test_enrichment_requires_point_in_time_inputs():
    inputs = UnderlyingInputs.from_mapping({"spot": "100", "timestamp": "2026-01-02T14:59:00Z", "rate": "0.04", "dividend_yield": "0.01"})
    quote = enrich_quote(bid="9.9", ask="10.1", contract={"expiry": "2027-01-02", "strike": "100", "right": "C"}, quote_timestamp=AS_OF, inputs=inputs)
    assert quote["delta_source"] == "black_scholes_midpoint_v1"
    assert "underlying_inputs" in quote


def test_enrichment_rejects_future_underlying_input():
    inputs = UnderlyingInputs.from_mapping({"spot": "100", "timestamp": "2026-01-02T16:00:00Z", "rate": "0.04", "dividend_yield": "0.01"})
    with pytest.raises(EnrichmentError, match="follows quote"):
        enrich_quote(bid="9.9", ask="10.1", contract={"expiry": "2027-01-02", "strike": "100", "right": "C"}, quote_timestamp=AS_OF, inputs=inputs)


def test_missing_inputs_fail_closed():
    with pytest.raises(EnrichmentError):
        UnderlyingInputs.from_mapping({"spot": "100"})
