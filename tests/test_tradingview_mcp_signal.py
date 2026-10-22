"""Causal TradingView closed-bar state and actual stdio protocol checks."""
from datetime import datetime, timedelta, timezone
import json
import os
import socket
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from neorome import tradingview_mcp_signal as tv

UTC = timezone.utc
BASE = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)


def make_record(now, k=10, d=12, forming=90):
    iso = now.isoformat()
    return {"source_hash": str(k).encode().hex().ljust(64, "0"), "received_at": iso,
            "data": {"symbol": tv.SYMBOL, "timeframe": "30m",
                     "stochastic": {"k": forming, "d": forming, "previous_k": k, "previous_d": d},
                     "source_metadata": {"request_started_at": iso, "received_at": iso,
                                         "http_date": iso, "cache_age": 0, "request_count": 1,
                                         "endpoint": "https://scanner.tradingview.com/crypto/scan",
                                         "source_response_sha256": "c" * 64,
                                         "cache_disabled": True, "stale_disabled": True}}}


class TradingViewStateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def evaluate(self, now, **kwargs):
        return tv.evaluate(make_record(now, **kwargs), self.root, now)

    def test_warmup_then_closed_bullish_turn_ignores_forming_values(self):
        first = self.evaluate(BASE + timedelta(seconds=5), k=10, forming=99)
        self.assertIsNone(first["selected_signal"])
        self.assertEqual(first["reason"], "awaiting_adjacent_closed_tradingview_bar")
        later = BASE + timedelta(minutes=30, seconds=5)
        signal = self.evaluate(later, k=15, forming=1)["selected_signal"]
        self.assertEqual((signal["direction"], signal["contract_side"]), ("bullish", "yes"))
        self.assertEqual((signal["previous_k"], signal["k"]), (10, 15))
        self.assertEqual(signal["bar_close_timestamp"], int(later.timestamp()) - 5)
        self.assertEqual(signal["signal_provider"], "tradingview_mcp")
        self.assertFalse(signal["explicit_source_bar_timestamp"])
        self.assertEqual(self.evaluate(later, k=15)["selected_signal"]["signal_id"], signal["signal_id"])

    def test_bearish_uses_closed_80_downward_turn(self):
        self.evaluate(BASE, k=85, d=80)
        signal = self.evaluate(BASE + timedelta(minutes=30), k=78, d=80)["selected_signal"]
        self.assertEqual((signal["direction"], signal["contract_side"]), ("bearish", "no"))

    def test_missing_adjacent_bar_and_late_entry_are_abstentions(self):
        self.evaluate(BASE, k=10)
        self.assertIsNone(self.evaluate(BASE + timedelta(hours=1), k=15)["selected_signal"])
        self.assertIsNone(self.evaluate(BASE + timedelta(hours=1, minutes=30, seconds=91), k=18)["selected_signal"])

    def test_same_closed_bar_cannot_create_a_new_turn(self):
        self.evaluate(BASE, k=10)
        self.assertIsNone(self.evaluate(BASE + timedelta(minutes=15), k=15)["selected_signal"])

    def test_stale_future_missing_values_and_cached_reads_fail_without_state(self):
        for changes in ({"http_date": (BASE - timedelta(seconds=16)).isoformat()},
                        {"received_at": (BASE + timedelta(seconds=1)).isoformat()},
                        {"cache_age": 16}, {"cache_disabled": False}, {"request_count": 2}):
            record = make_record(BASE)
            record["data"]["source_metadata"].update(changes)
            with self.subTest(changes=changes), self.assertRaises(tv.TradingViewMCPError):
                tv.evaluate(record, self.root, BASE)
            self.assertFalse((self.root / tv.STATE_NAME).exists())
        record = make_record(BASE)
        record["data"]["stochastic"]["previous_k"] = None
        with self.assertRaises(tv.TradingViewMCPError):
            tv.evaluate(record, self.root, BASE)

    def test_cross_boundary_request_is_not_assigned_an_invented_close(self):
        record = make_record(BASE + timedelta(seconds=1))
        record["data"]["source_metadata"]["request_started_at"] = (BASE - timedelta(seconds=1)).isoformat()
        with self.assertRaisesRegex(tv.TradingViewMCPError, "crossed"):
            tv.evaluate(record, self.root, BASE + timedelta(seconds=2))


class TradingViewProtocolTests(unittest.TestCase):
    def test_private_warm_transport_requests_exact_mcp_tool(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "feed.sock"
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                server.bind(str(path)); path.chmod(0o600); server.listen(1)
            except BaseException:
                server.close();raise
            errors = []
            def respond():
                try:
                    with server.accept()[0] as connection:
                        request = json.loads(connection.makefile("rb").readline())
                        if request != {"method": "coin_analysis", "symbol": "BTCUSD", "exchange": "COINBASE", "timeframe": "30m"}:
                            raise AssertionError("unexpected warm request")
                        response = {"ok": True, "data": make_record(BASE)["data"],
                                    "mcp_server": {"name": "TradingView BTCUSD Paper Feed"},
                                    "mcp_protocol_version": "2025-11-25", "mcp_transcript_sha256": "a" * 64,
                                    "tool": "coin_analysis"}
                        connection.sendall(json.dumps(response).encode() + b"\n")
                except Exception as exc:
                    errors.append(exc)
            thread = threading.Thread(target=respond, daemon=True);thread.start()
            try:
                with patch.object(tv, "BROKER_SOCKET", path), patch.object(tv, "BROKER_UID", os.getuid()):
                    result = tv.capture()
                self.assertEqual(result["data"]["symbol"], tv.SYMBOL)
                self.assertEqual(result["tool"], "coin_analysis")
            finally:
                thread.join(timeout=2);server.close()
            self.assertFalse(errors)

    def test_warm_socket_rejects_public_permissions_and_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "feed.sock"
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
                server.bind(str(path));path.chmod(0o660)
                with patch.object(tv, "BROKER_SOCKET", path), patch.object(tv, "BROKER_UID", os.getuid()):
                    with self.assertRaisesRegex(tv.TradingViewMCPError, "permissions"):
                        tv.capture()
                link = Path(directory) / "link.sock";link.symlink_to(path)
                with patch.object(tv, "BROKER_SOCKET", link), patch.object(tv, "BROKER_UID", os.getuid()):
                    with self.assertRaisesRegex(tv.TradingViewMCPError, "identity"):
                        tv.capture()

    def test_stdio_initializes_lists_and_calls_the_exact_tool(self):
        code = '''
import sys,json
for line in sys.stdin:
 value=json.loads(line); method=value['method']; identity=value.get('id')
 if method=='initialize':result={'protocolVersion':'2025-06-18','serverInfo':{'name':'TradingView BTCUSD Paper Feed','version':'test'}}
 elif method=='notifications/initialized':continue
 elif method=='tools/list':result={'tools':[{'name':'coin_analysis'}]}
 else:
  assert value['params']=={'name':'coin_analysis','arguments':{'symbol':'BTCUSD','exchange':'COINBASE','timeframe':'30m'}}
  result={'isError':False,'content':[{'type':'text','text':json.dumps({'symbol':'COINBASE:BTCUSD'})}]}
 print(json.dumps({'jsonrpc':'2.0','id':identity,'result':result}),flush=True)
'''
        result = tv.exchange([sys.executable, "-u", "-c", code], dict(os.environ))
        self.assertEqual(result["data"]["symbol"], "COINBASE:BTCUSD")
        self.assertEqual(result["tool"], "coin_analysis")
        self.assertEqual(len(result["mcp_transcript_sha256"]), 64)

    def test_partial_protocol_line_is_bounded_by_deadline(self):
        code = "import sys,time;sys.stdout.write('{');sys.stdout.flush();time.sleep(5)"
        with self.assertRaisesRegex(tv.TradingViewMCPError, "deadline"):
            tv.exchange([sys.executable, "-u", "-c", code], dict(os.environ), timeout=0.1)

    def test_eof_and_protocol_errors_are_not_signals(self):
        with self.assertRaises(tv.TradingViewMCPError):
            tv.exchange([sys.executable, "-c", "pass"], dict(os.environ), timeout=1)


if __name__ == "__main__":
    unittest.main()
