from __future__ import annotations

import asyncio
import json
import math
import time
import unittest
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from urllib.parse import urlsplit

from neorome import tradingview_fast_mcp as mcp_module
from neorome import tradingview_fast_source as source


def bars_fixture(count: int = 12, *, include_forming: bool = False, server_time: int | None = None) -> list[list[float]]:
    now = int(time.time()) if server_time is None else server_time
    current_open = now // source.BAR_SECONDS * source.BAR_SECONDS
    first = current_open - (count - 1) * source.BAR_SECONDS
    rows = []
    for index in range(count):
        low = 100.0 + index
        rows.append([first + index * source.BAR_SECONDS, low + 3, low + 10, low, low + 6, 1.0])
    if not include_forming:
        rows.pop()
    return rows


def packet(value: dict) -> str:
    body = json.dumps(value, separators=(",", ":"))
    return f"~m~{len(body.encode())}~m~{body}"


class FakeResponse:
    def __init__(self, date: str):
        self.status_code = 101
        self.headers = {"Date": date}


class FakeRequest:
    def __init__(self, uri: str):
        self.path = urlsplit(uri).path + "?" + urlsplit(uri).query
        self.headers = {"Host": "data.tradingview.com"}


class FakeWebSocket:
    def __init__(self, uri: str, responses: list[str], date: str):
        self.request = FakeRequest(uri)
        self.response = FakeResponse(date)
        self.responses = responses
        self.sent: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    async def send(self, value: str):
        self.sent.append(value)

    async def recv(self):
        if not self.responses:
            raise AssertionError("source read beyond the prepared offline response")
        return self.responses.pop(0)


def fake_responses(*, now: int, rows: list[list[float]], identity: dict | None = None) -> list[str]:
    identity = identity or {
        "full_name": source.SYMBOL,
        "source_id": "COINBASE",
        "provider_id": "coinbase",
        "name": "BTCUSD",
        "type": "spot",
    }
    return [
        packet({"session_id": "x.charts-free", "timestamp": now}),
        packet({"m": "symbol_resolved", "p": ["cs_test", "symbol_1", identity], "t": now}),
        packet(
            {
                "m": "timescale_update",
                "p": [
                    "cs_test",
                    {"sds_1": {"s": [{"i": i, "v": row} for i, row in enumerate(rows)]}},
                ],
                "t": now,
            }
        ),
        packet({"m": "series_completed", "p": ["cs_test", "sds_1", "streaming", "s1"], "t": now}),
    ]


def install_fake_connection(monkeypatch, *, now: int | None = None, rows=None, identity=None, date=None):
    server_now = int(time.time()) if now is None else now
    ws = None

    def factory(uri: str, **kwargs):
        nonlocal ws
        assert uri.startswith(source.ENDPOINT + "?")
        assert kwargs["origin"] == source._ORIGIN
        assert kwargs["proxy"] is None
        assert kwargs["max_size"] == 2 * 1024 * 1024
        http_date = date or format_datetime(datetime.now(timezone.utc), usegmt=True)
        ws = FakeWebSocket(uri, fake_responses(now=server_now, rows=rows or bars_fixture(server_time=server_now), identity=identity), http_date)
        return ws

    monkeypatch.setattr(source, "_NoRedirectConnect", factory)
    return lambda: ws


class TradingViewFastSourceTests(unittest.TestCase):
  def test_stoch_5_3_3_matches_independent_known_vector(self):
    lows = [1, 2, 1, 3, 2, 4, 3, 2, 4, 5]
    highs = [5, 6, 7, 7, 8, 8, 9, 8, 10, 11]
    closes = [4, 3, 5, 6, 4, 7, 8, 4, 9, 8]
    bars = [
        {
            "timestamp": 1_700_000_000 + i * source.BAR_SECONDS,
            "bar_close_timestamp": 1_700_000_000 + (i + 1) * source.BAR_SECONDS,
            "low": lows[i],
            "high": highs[i],
            "open": (lows[i] + closes[i]) / 2,
            "close": closes[i],
        }
        for i in range(10)
    ]
    readings = source.calculate_stochastic(bars)
    self.assertEqual(len(readings), 2)
    self.assertAlmostEqual(readings[-1]["k"], 60.91269841269841)
    self.assertAlmostEqual(readings[-1]["d"], 65.34391534391533)
    self.assertEqual(len(readings), len(bars) - 8)
    self.assertEqual(readings[0]["bar_close_timestamp"], bars[8]["bar_close_timestamp"])
    self.assertEqual(readings[-1]["bar_close_timestamp"], bars[-1]["bar_close_timestamp"])
    self.assertTrue(all(math.isfinite(float(item[field])) for item in readings for field in ("k", "d")))


  def test_forming_bar_excluded_using_its_explicit_timestamp(self):
    now = int(time.time())
    rows = bars_fixture(count=12, include_forming=True, server_time=now)
    closed = source.validate_bars(rows, now)
    self.assertEqual(len(closed), 11)
    self.assertTrue(all(int(row["bar_close_timestamp"]) <= now for row in closed))
    self.assertEqual(closed[-1]["timestamp"], rows[-2][0])


  def test_bar_gap_duplicate_order_and_ohlc_errors_fail_closed(self):
    cases = [
        [[3000, 10, 12, 9, 11], [3600, 11, 13, 10, 12]],  # gap
        [[3000, 10, 12, 9, 11], [3000, 11, 13, 10, 12]],  # duplicate
        [[3600, 10, 12, 9, 11], [3300, 11, 13, 10, 12]],  # out of order
        [[3000, 10, 9, 11, 11]],  # inconsistent OHLC
    ]
    for rows in cases:
      with self.subTest(rows=rows), self.assertRaises(source.TradingViewSourceError):
        source.validate_bars(rows, 100_000)


  def test_nonfinite_ohlc_refused(self):
    rows = bars_fixture(count=12)
    rows[0][2] = float("nan")
    with self.assertRaisesRegex(source.TradingViewSourceError, "non-finite"):
        source.validate_bars(rows, int(time.time()))

  def test_future_open_bar_refused(self):
    now = int(time.time())
    rows = bars_fixture(count=12, include_forming=True, server_time=now)
    future = now // source.BAR_SECONDS * source.BAR_SECONDS + source.BAR_SECONDS
    rows.append([future, 120, 125, 118, 122])
    with self.assertRaisesRegex(source.TradingViewSourceError, "future"):
      source.validate_bars(rows, now)

  def test_framed_tradingview_heartbeat_is_decoded(self):
    self.assertEqual(source._decode_tv_frames("~m~4~m~~h~1"), ["~h~1"])
    self.assertEqual(source._decode_tv_frames("~h~123"), ["~h~123"])

  def test_common_provider_delay_flags_are_refused(self):
    for info in (
        {"seconds_delay": 1},
        {"data_status": "delayed_streaming"},
        {"delay": 0},
    ):
      with self.subTest(info=info):
        if info == {"delay": 0}:
          source._reject_delayed_flags(info)
        else:
          with self.assertRaisesRegex(source.TradingViewSourceError, "delayed"):
            source._reject_delayed_flags(info)


  def test_zero_range_window_refused(self):
    bars = [
        {"timestamp": 300 * i, "bar_close_timestamp": 300 * (i + 1), "open": 1, "high": 1, "low": 1, "close": 1}
        for i in range(10)
    ]
    with self.assertRaisesRegex(source.TradingViewSourceError, "zero-range"):
        source.calculate_stochastic(bars)


  def test_public_guest_source_returns_verified_closed_bars_and_metadata(self):
    now = int(time.time())
    from unittest.mock import patch
    with patch.object(source, "_NoRedirectConnect") as connect_mock:
      server_now = now
      socket = FakeWebSocket(
          source._URI_PREFIX + datetime.now(timezone.utc).strftime("%Y_%m_%d-%H_%M"),
          fake_responses(now=server_now, rows=bars_fixture(count=12, include_forming=True, server_time=now)),
          format_datetime(datetime.now(timezone.utc), usegmt=True),
      )
      connect_mock.return_value = socket
      connect_mock.side_effect = lambda *args, **kwargs: socket
      result = asyncio.run(source.fetch_fast_stochastic())
    self.assertEqual(result["symbol"], "COINBASE:BTCUSD")
    self.assertEqual(result["timeframe"], "5m")
    self.assertEqual(result["stochastic_settings"], [5, 3, 3])
    self.assertEqual(len(result["bars"]), 11)
    self.assertGreaterEqual(len(result["readings"]), 2)
    metadata = result["source_metadata"]
    self.assertEqual(metadata["request_count"], 1)
    self.assertIs(metadata["explicit_source_bar_timestamp"], True)
    self.assertIs(metadata["cache_disabled"], True)
    self.assertIs(metadata["stale_disabled"], True)
    self.assertEqual(len(metadata["source_response_sha256"]), 64)
    sent = [json.loads(value.split("~m~", 2)[2])["m"] for value in socket.sent]
    self.assertEqual(sent, ["set_auth_token", "chart_create_session", "resolve_symbol", "create_series"])


  def test_wrong_resolved_identity_and_delay_flags_are_refused(self):
    from unittest.mock import patch
    now = int(time.time())
    wrong = {"full_name": "KRAKEN:BTCUSD", "source_id": "KRAKEN", "provider_id": "kraken", "name": "BTCUSD", "type": "spot"}
    with patch.object(source, "_NoRedirectConnect", lambda uri, **kwargs: FakeWebSocket(uri, fake_responses(now=now, rows=bars_fixture(server_time=now), identity=wrong), format_datetime(datetime.now(timezone.utc), usegmt=True))):
      with self.assertRaisesRegex(source.TradingViewSourceError, "different market"):
        asyncio.run(source.fetch_fast_stochastic())
    delayed = {"full_name": source.SYMBOL, "source_id": "COINBASE", "provider_id": "coinbase", "name": "BTCUSD", "type": "spot", "is_delayed": True}
    with patch.object(source, "_NoRedirectConnect", lambda uri, **kwargs: FakeWebSocket(uri, fake_responses(now=now, rows=bars_fixture(server_time=now), identity=delayed), format_datetime(datetime.now(timezone.utc), usegmt=True))):
      with self.assertRaisesRegex(source.TradingViewSourceError, "delayed"):
        asyncio.run(source.fetch_fast_stochastic())


  def test_stale_handshake_date_refused(self):
    from unittest.mock import patch
    now = int(time.time())
    stale = format_datetime(datetime.now(timezone.utc) - timedelta(seconds=45), usegmt=True)
    with patch.object(source, "_NoRedirectConnect", lambda uri, **kwargs: FakeWebSocket(uri, fake_responses(now=now, rows=bars_fixture(server_time=now)), stale)):
      with self.assertRaisesRegex(source.TradingViewSourceError, "Date is stale"):
        asyncio.run(source.fetch_fast_stochastic())

  def test_stale_chart_server_time_refused(self):
    from unittest.mock import patch
    stale_server = int(time.time()) - 30
    fresh_date = format_datetime(datetime.now(timezone.utc), usegmt=True)
    with patch.object(source, "_NoRedirectConnect", lambda uri, **kwargs: FakeWebSocket(uri, fake_responses(now=stale_server, rows=bars_fixture(server_time=stale_server)), fresh_date)):
      with self.assertRaisesRegex(source.TradingViewSourceError, "chart server time is stale"):
        asyncio.run(source.fetch_fast_stochastic())

  def test_upstream_symbol_error_is_refused(self):
    from unittest.mock import patch
    now = int(time.time())
    responses = [
        packet({"session_id": "x.charts-free", "timestamp": now}),
        packet({"m": "symbol_error", "p": ["cs_test", "symbol_1", "invalid symbol"], "t": now}),
    ]
    with patch.object(source, "_NoRedirectConnect", lambda uri, **kwargs: FakeWebSocket(uri, responses, format_datetime(datetime.now(timezone.utc), usegmt=True))):
      with self.assertRaisesRegex(source.TradingViewSourceError, "rejected the fixed symbol"):
        asyncio.run(source.fetch_fast_stochastic())


  def test_mcp_tool_is_offline_fixed_and_has_one_public_tool(self):
    from unittest.mock import patch
    called = []

    async def fake_fetch():
        called.append(True)
        return {"symbol": source.SYMBOL, "timeframe": source.TIMEFRAME}

    patcher = patch.object(mcp_module, "fetch_fast_stochastic", fake_fetch)
    patcher.start()
    self.addCleanup(patcher.stop)
    result = asyncio.run(mcp_module.fast_stochastic())
    self.assertEqual(result, {"symbol": source.SYMBOL, "timeframe": source.TIMEFRAME})
    self.assertEqual(called, [True])
    for args in (("ETHUSD", "COINBASE", "5m"), ("BTCUSD", "KRAKEN", "5m"), ("BTCUSD", "COINBASE", "15m")):
        with self.assertRaisesRegex(ValueError, "fixed"):
            asyncio.run(mcp_module.fast_stochastic(*args))
    self.assertEqual(list(mcp_module.mcp._tool_manager._tools), ["fast_stochastic"])


if __name__ == "__main__":
    unittest.main()
