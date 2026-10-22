"""Small retained executable-shaped fixture for deterministic replay tests.

The quotes are synthetic and deliberately carry ``synthetic_executable_fixture``
provenance.  They exercise a filled vertical, an exit on signal reversal, and
an exposure-matched share benchmark without asserting market performance.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal


def _symbol(root: str, expiry: date, right: str, strike: str) -> str:
    encoded = f"{expiry:%y%m%d}{right}{int(float(strike) * 1000):08d}"
    return f"{root}{encoded}"


def _quote(*, symbol: str, underlying: str, expiry: date, right: str, strike: str, bid: str, ask: str, delta: str, timestamp: str, bid_size: str = "2", ask_size: str = "2") -> dict[str, object]:
    return {
        "symbol": symbol,
        "underlying": underlying,
        "expiry": expiry.isoformat(),
        "right": right,
        "strike": strike,
        "bid": bid,
        "ask": ask,
        "bid_size": bid_size,
        "ask_size": ask_size,
        "delta": delta,
        "timestamp": timestamp,
        "source_kind": "synthetic_executable_fixture",
        "executable": True,
    }


def dataset() -> dict[str, object]:
    start = date(2026, 1, 1)
    rows: list[dict[str, object]] = []
    closes: list[Decimal] = []
    for index in range(260):
        session = start + timedelta(days=index)
        close = Decimal("100") if index < 200 else (Decimal("102") if index in {200, 201} else Decimal("90") if index == 202 else sum(closes[-200:], Decimal(0)) / Decimal(200))
        row: dict[str, object] = {
            "session_date": session.isoformat(),
            "signal_timestamp": f"{session.isoformat()}T20:00:00Z",
            "close": str(close),
            "spot": "100",
            "shares": {"bid": "99.90", "ask": "100.10", "bid_size": "10", "ask_size": "10"},
        }
        if index == 200:
            expiry = session + timedelta(days=45)
            long_symbol = _symbol("SPY", expiry, "C", "100")
            short_symbol = _symbol("SPY", expiry, "C", "101")
            row.update({
                "close": "102",
                "entry_quotes": {
                    "long": _quote(symbol=long_symbol, underlying="SPY", expiry=expiry, right="C", strike="100", bid="0.10", ask="0.14", delta="0.50", timestamp=f"{session.isoformat()}T20:00:01Z"),
                    "short": _quote(symbol=short_symbol, underlying="SPY", expiry=expiry, right="C", strike="101", bid="0.04", ask="0.08", delta="0.25", timestamp=f"{session.isoformat()}T20:00:01Z"),
                },
                "chain_metadata": {"complete": True, "available_expirations": [expiry.isoformat()]},
            })
        if index == 202:
            expiry = start + timedelta(days=200) + timedelta(days=45)
            long_symbol = _symbol("SPY", expiry, "C", "100")
            short_symbol = _symbol("SPY", expiry, "C", "101")
            row.update({
                "close": "90",
                "exit_quotes": {
                    "long": _quote(symbol=long_symbol, underlying="SPY", expiry=expiry, right="C", strike="100", bid="0.20", ask="0.24", delta="0.50", timestamp=f"{session.isoformat()}T20:00:01Z"),
                    "short": _quote(symbol=short_symbol, underlying="SPY", expiry=expiry, right="C", strike="101", bid="0.05", ask="0.09", delta="0.25", timestamp=f"{session.isoformat()}T20:00:01Z"),
                },
            })
        rows.append(row)
        closes.append(Decimal(str(row["close"])))
    return {
        "fixture_type": "synthetic",
        "data_admission": {
            "source_kind": "synthetic_executable_fixture",
            "provider": "retained-fixture",
            "entitlement_confirmed": False,
            "quotes_complete": False,
            "provider_timestamps": True,
        },
        "cost_model": {"fee_per_contract": "0", "slippage_bps": "0", "share_fee_per_share": "0", "benchmark_capital": "300"},
        "rows": rows,
    }


__all__ = ["dataset"]
