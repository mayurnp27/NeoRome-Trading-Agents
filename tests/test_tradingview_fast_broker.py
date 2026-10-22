from __future__ import annotations

import asyncio
import json
import os
import stat
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from neorome import tradingview_fast_broker as broker


def valid_analysis():
    return {
        "symbol": "COINBASE:BTCUSD", "timeframe": "5m", "stochastic_settings": [5,3,3],
        "bars": [{}]*10, "readings": [{"k":18,"d":19},{"k":22,"d":20}],
        "stochastic": {"k": 44.123456, "d": 45.234567, "previous_k": 46.345678, "previous_d": 47.456789},
        "source_metadata": {
            "request_started_at": "2026-10-14T12:00:00.000Z",
            "received_at": "2026-10-14T12:00:00.100Z",
            "http_date": "2026-10-14T12:00:00.000Z",
            "cache_age": 0.1,
            "request_count": 1, "explicit_source_bar_timestamp": True,
            "cache_disabled": True,
            "stale_disabled": True,
            "source_response_sha256": "a" * 64,
            "endpoint": "wss://data.tradingview.com/socket.io/websocket",
        },
    }


class FakeSession:
    def __init__(self, response=None, delay=0):
        self.response = response or SimpleNamespace(
            content=[SimpleNamespace(type="text", text=json.dumps(valid_analysis()))],
            isError=False,
        )
        self.delay = delay
        self.calls = []
        self.active = 0
        self.max_active = 0
        self.initialize_count = 0
        self.list_count = 0

    async def initialize(self):
        self.initialize_count += 1
        return {"serverInfo": {"name": broker.MCP_SERVER_NAME, "version": "1"}, "protocolVersion": "2025-06-18"}

    async def list_tools(self):
        self.list_count += 1
        return {"tools": [{"name": "fast_stochastic", "inputSchema": {"type": "object"}}]}

    async def call_tool(self, name, arguments):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.calls.append((name, arguments))
        try:
            if self.delay:
                await asyncio.sleep(self.delay)
            return self.response
        finally:
            self.active -= 1


class FakeWriter:
    def __init__(self):
        self.parts = []
        self.closed = False

    def write(self, data):
        self.parts.append(data)

    async def drain(self):
        return None

    def close(self):
        self.closed = True

    async def wait_closed(self):
        return None


class BrokerProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.session = FakeSession(delay=0.01)
        self.info, self.version, self.catalog, self.initialize_message = await broker.initialize_session(self.session)
        self.lock = asyncio.Lock()

    def test_accepts_only_fixed_request(self):
        line = json.dumps({
            "method": "fast_stochastic", "symbol": "BTCUSD", "exchange": "COINBASE", "timeframe": "5m"
        }).encode() + b"\n"
        self.assertEqual(broker.validate_request(line)["symbol"], "BTCUSD")
        for raw in (
            b"{}\n",
            b'{"method":"fast_stochastic","symbol":"ETHUSD","exchange":"COINBASE","timeframe":"5m"}\n',
            b'{"method":"fast_stochastic","symbol":"BTCUSD","exchange":"COINBASE","timeframe":"5m","extra":1}\n',
            b'{"method":"fast_stochastic","method":"fast_stochastic","symbol":"BTCUSD","exchange":"COINBASE","timeframe":"5m"}\n',
            b"x" * (broker.REQUEST_LIMIT_BYTES + 1),
        ):
            with self.subTest(raw=raw[:80]), self.assertRaises(broker.BrokerError):
                broker.validate_request(raw)

    async def test_fixed_tool_name_arguments_response_and_transcript(self):
        response = await broker.call_analysis(
            self.session, self.info, self.version, self.catalog, self.initialize_message, self.lock
        )
        self.assertEqual(self.session.calls, [(
            "fast_stochastic", {"symbol": "BTCUSD", "exchange": "COINBASE", "timeframe": "5m"}
        )])
        self.assertTrue(response["ok"])
        self.assertEqual(response["tool"], "fast_stochastic")
        self.assertEqual(response["mcp_server"]["name"], broker.MCP_SERVER_NAME)
        self.assertEqual(len(response["mcp_transcript_sha256"]), 64)

    async def test_fake_socket_writer_handles_one_line_request(self):
        instance = broker.TradingViewBroker()
        instance.session = self.session
        instance.server_info = self.info
        instance.protocol_version = self.version
        instance.catalog = self.catalog
        instance.initialize_message = self.initialize_message
        reader = asyncio.StreamReader()
        reader.feed_data(json.dumps({
            "method": "fast_stochastic", "symbol": "BTCUSD", "exchange": "COINBASE", "timeframe": "5m"
        }).encode() + b"\n")
        reader.feed_eof()
        writer = FakeWriter()
        await instance.handle_client(reader, writer)
        self.assertTrue(writer.closed)
        reply = json.loads(b"".join(writer.parts))
        self.assertTrue(reply["ok"])
        self.assertEqual(len(b"".join(writer.parts)), len(broker.encode_response(reply)))

    async def test_fake_socket_writer_returns_bounded_refusal_for_invalid_request(self):
        instance = broker.TradingViewBroker()
        instance.session = self.session
        reader = asyncio.StreamReader()
        reader.feed_data(b'{"method":"fast_stochastic","symbol":"ETHUSD"}\n')
        reader.feed_eof()
        writer = FakeWriter()
        await instance.handle_client(reader, writer)
        reply = json.loads(b"".join(writer.parts))
        self.assertFalse(reply["ok"])
        self.assertEqual(self.session.calls, [])
        self.assertLessEqual(len(b"".join(writer.parts)), broker.RESPONSE_LIMIT_BYTES)

    async def test_session_initializes_and_lists_once_calls_are_serialized(self):
        await asyncio.gather(*(
            broker.call_analysis(
                self.session, self.info, self.version, self.catalog, self.initialize_message, self.lock
            )
            for _ in range(4)
        ))
        self.assertEqual(self.session.initialize_count, 1)
        self.assertEqual(self.session.list_count, 1)
        self.assertEqual(len(self.session.calls), 4)
        self.assertEqual(self.session.max_active, 1)

    async def test_mcp_tool_error_is_a_refusal(self):
        session = FakeSession(response=SimpleNamespace(content=[], isError=True))
        with self.assertRaisesRegex(broker.BrokerError, "reported an error"):
            await broker.call_analysis(
                session, self.info, self.version, self.catalog, self.initialize_message, self.lock
            )

    async def test_error_envelope_and_missing_metadata_cannot_be_success(self):
        for data in ({"error": {"code": "UPSTREAM_ERROR"}}, {"stochastic": valid_analysis()["stochastic"]}):
            session = FakeSession(response=SimpleNamespace(
                content=[SimpleNamespace(type="text", text=json.dumps(data))], isError=False
            ))
            with self.subTest(data=data), self.assertRaises(broker.BrokerError):
                await broker.call_analysis(
                    session, self.info, self.version, self.catalog, self.initialize_message, self.lock
                )

    async def test_malformed_mcp_payload_fails_closed(self):
        session = FakeSession(response=SimpleNamespace(
            content=[SimpleNamespace(type="text", text="not-json")], isError=False
        ))
        with self.assertRaises(broker.BrokerFatalError):
            await broker.call_analysis(
                session, self.info, self.version, self.catalog, self.initialize_message, self.lock
            )

    async def test_refuses_unexpected_catalog_during_single_initialization(self):
        class UnexpectedToolSession(FakeSession):
            async def list_tools(self):
                self.list_count += 1
                return {"tools": [{"name": "fast_stochastic"}, {"name": "top_gainers"}]}

        session = UnexpectedToolSession()
        with self.assertRaisesRegex(broker.BrokerFatalError, "unexpected tool catalog"):
            await broker.initialize_session(session)
        self.assertEqual(session.initialize_count, 1)
        self.assertEqual(session.list_count, 1)

    async def test_timeout_runs_in_the_current_task(self):
        caller = asyncio.current_task()
        owner = []

        async def wait_forever():
            owner.append(asyncio.current_task())
            await asyncio.sleep(10)

        with self.assertRaises(asyncio.TimeoutError):
            await broker.await_with_timeout(wait_forever(), 0.01)
        self.assertEqual(owner, [caller])

    def test_response_encoding_is_bounded(self):
        encoded = broker.encode_response({"ok": True, "data": {"large": "x" * broker.RESPONSE_LIMIT_BYTES}})
        self.assertLessEqual(len(encoded), broker.RESPONSE_LIMIT_BYTES)
        self.assertEqual(json.loads(encoded), {"ok": False, "error": "response too large"})


class BrokerFilesystemTests(unittest.TestCase):
    def test_atomic_status_and_private_modes(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            os.chmod(directory, 0o700)
            status = directory / "status.json"
            broker.verify_runtime_directory(directory, os.geteuid(), os.geteuid())
            broker.write_status(status, "ready", server_info={"name": broker.MCP_SERVER_NAME})
            self.assertEqual(stat.S_IMODE(status.stat().st_mode), 0o600)
            self.assertEqual(json.loads(status.read_text())["state"], "ready")
            self.assertEqual(sorted(path.name for path in directory.iterdir()), ["status.json"])

    def test_refuses_symlink_or_foreign_feed_socket_and_removes_stale_owned_one(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            target = directory / "target"
            target.write_text("leave me")
            socket_path = directory / "feed.sock"
            socket_path.symlink_to(target)
            with self.assertRaisesRegex(broker.BrokerFatalError, "not a Unix socket"):
                broker.remove_stale_owned_socket(socket_path, os.geteuid())
            socket_path.unlink()

            socket_path.touch()

            class RefusedProbe:
                def settimeout(self, _timeout):
                    pass

                def connect(self, _path):
                    raise OSError(broker.errno.ECONNREFUSED, "no listener")

                def close(self):
                    pass

            with mock.patch.object(broker.stat, "S_ISSOCK", return_value=True), mock.patch.object(
                broker.socket, "socket", return_value=RefusedProbe()
            ):
                broker.remove_stale_owned_socket(socket_path, os.geteuid())
            self.assertFalse(socket_path.exists())

    def test_refuses_foreign_owned_socket_without_connecting(self):
        with tempfile.TemporaryDirectory() as temporary:
            socket_path = Path(temporary) / "feed.sock"
            socket_path.touch()
            original_lstat = Path.lstat

            def foreign_lstat(path):
                if path == socket_path:
                    return SimpleNamespace(
                        st_mode=stat.S_IFSOCK | 0o600,
                        st_uid=os.geteuid() + 1,
                        st_dev=1,
                        st_ino=2,
                    )
                return original_lstat(path)

            with mock.patch.object(Path, "lstat", foreign_lstat):
                with self.assertRaisesRegex(broker.BrokerFatalError, "foreign owner"):
                    broker.remove_stale_owned_socket(socket_path, os.geteuid())

    def test_refuses_active_socket_and_wrong_runtime_permissions(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            os.chmod(directory, 0o750)
            socket_path = directory / "feed.sock"
            socket_path.touch()

            class ActiveProbe:
                def settimeout(self, _timeout):
                    pass

                def connect(self, _path):
                    return None

                def close(self):
                    pass

            with mock.patch.object(broker.stat, "S_ISSOCK", return_value=True), mock.patch.object(
                broker.socket, "socket", return_value=ActiveProbe()
            ):
                with self.assertRaisesRegex(broker.BrokerFatalError, "active"):
                    broker.remove_stale_owned_socket(socket_path, os.geteuid())
            with self.assertRaisesRegex(broker.BrokerFatalError, "mode"):
                broker.verify_runtime_directory(directory, os.geteuid(), os.geteuid())


if __name__ == "__main__":
    unittest.main()
