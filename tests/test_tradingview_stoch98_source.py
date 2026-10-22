import asyncio
import hashlib
import json
import time
import unittest
from datetime import datetime, timezone
from email.utils import format_datetime
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import urlsplit

from neorome import tradingview_stoch98_source as source


def chart_rows(timeframe_minutes, server_time, count=260):
    seconds = timeframe_minutes * 60
    current_open = server_time // seconds * seconds
    # Includes the current forming candle; it must be removed by validation.
    return [[current_open - seconds * i, 100, 103, 97, 101, 2] for i in range(count - 1, -1, -1)]


class TradingViewStoch98SourceTests(unittest.TestCase):
    def test_native_validation_for_both_timeframes_and_forming_bar_filter(self):
        now = int(time.time())
        for timeframe in (15, 30):
            rows = chart_rows(timeframe, now)
            closed = source.validate_bars(rows, now, timeframe_minutes=timeframe)
            self.assertEqual(len(closed), len(rows) - 1)
            self.assertEqual(closed[-1]["bar_close_timestamp"], now // (timeframe * 60) * (timeframe * 60))
            self.assertEqual(closed[0]["timestamp"] % (timeframe * 60), 0)
            self.assertLessEqual(len(closed), 300)
        with self.assertRaises(source.TradingViewSourceError):
            source.validate_bars(chart_rows(15, now)[:100], now, timeframe_minutes=15)
        with self.assertRaises(source.TradingViewSourceError):
            source.validate_bars(chart_rows(15, now), now, timeframe_minutes=60)

    def test_gap_and_future_rows_are_rejected(self):
        now = int(time.time())
        target = now // 900 * 900
        rows = chart_rows(15, now)
        with self.assertRaisesRegex(source.TradingViewSourceError, "gap"):
            source.validate_bars(rows[:50] + rows[51:], now, timeframe_minutes=15)
        with self.assertRaisesRegex(source.TradingViewSourceError, "future"):
            source.validate_bars(rows + [[target + 900, 100, 101, 99, 100]], now, timeframe_minutes=15)

    def test_fetch_requests_native_resolution_and_preserves_source_evidence(self):
        for timeframe in (15, 30):
            now = int(time.time())
            seconds = timeframe * 60
            current_open = now // seconds * seconds
            rows = [[current_open - seconds * i, 100, 103, 97, 101, 2] for i in range(260, -1, -1)]
            frames = [
                {"session_id": "x", "timestamp": now},
                {"m": "symbol_resolved", "p": ["x", "symbol_1", {
                    "full_name": source.SYMBOL,
                    "source_id": "COINBASE",
                    "provider_id": "coinbase",
                    "name": "BTCUSD",
                    "type": "spot",
                }], "t": now},
                {"m": "timescale_update", "p": ["x", {"sds_1": {"s": [{"v": row} for row in rows]}}], "t": now},
                {"m": "series_completed", "p": ["x", "sds_1"], "t": now},
            ]

            class FakeSocket:
                response = SimpleNamespace(
                    status_code=101,
                    headers={"Date": format_datetime(datetime.now(timezone.utc), usegmt=True)},
                )
                request = SimpleNamespace(path="", headers={"Host": "data.tradingview.com"})

                def __init__(self):
                    self.sent = []

                async def __aenter__(self):
                    return self

                async def __aexit__(self, *args):
                    return None

                async def send(self, value):
                    self.sent.append(value)

                async def recv(self):
                    value = json.dumps(frames.pop(0), separators=(",", ":"))
                    return f"~m~{len(value)}~m~{value}"

            ws = FakeSocket()

            def factory(uri, **kwargs):
                ws.request.path = urlsplit(uri).path + "?" + urlsplit(uri).query
                return ws

            with patch.object(source, "_NoRedirectConnect", side_effect=factory):
                result = asyncio.run(source.fetch_bars(timeframe_minutes=timeframe))

            self.assertEqual(result["timeframe"], f"{timeframe}m")
            self.assertEqual(len(result["bars"]), 260)
            self.assertEqual(result["source_metadata"]["requested_bars"], 300)
            self.assertEqual(result["source_metadata"]["returned_bars"], 260)
            self.assertEqual(result["source_metadata"]["source_response_sha256"], hashlib.sha256(result["raw_frames"].encode()).hexdigest())
            create_frame = next(source._decode_tv_frames(packet)[0] for packet in ws.sent if "create_series" in packet)
            self.assertEqual(create_frame["p"][4], str(timeframe))
            self.assertEqual(create_frame["p"][5], 300)

    def test_source_rejects_unsupported_resolution_before_connect(self):
        with patch.object(source, "_NoRedirectConnect", side_effect=AssertionError("must not connect")):
            with self.assertRaisesRegex(source.TradingViewSourceError, "unsupported"):
                asyncio.run(source.fetch_bars(timeframe_minutes=60))


if __name__ == "__main__":
    unittest.main()
