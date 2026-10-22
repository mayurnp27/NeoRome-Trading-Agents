"""Bounded stdio MCP client and causal closed-bar TradingView signal state.

No Coinbase candles or locally computed oscillator values are used here.
TradingView's [1] field identifies the previous completed bar. Its close time
is inferred from the requested UTC resolution; the provider exposes no explicit
bar timestamp, and that limitation is retained in every decision record.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import selectors
import socket
import stat
import subprocess
import time

UTC = timezone.utc
RUNTIME = Path("/opt/neorome-tradingview-mcp-r1/venv/bin/python")
VENDOR = Path(__file__).resolve().parents[1] / "vendor"
BROKER_SOCKET = Path("/run/neorome-tradingview-mcp/feed.sock")
BROKER_UID = 997
STATE_NAME = "tradingview-closed-bars.json"
SYMBOL = "COINBASE:BTCUSD"
MAX_BYTES = 2_000_000


class TradingViewMCPError(ValueError):
    pass


def stamp(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise TradingViewMCPError("MCP timestamp lacks timezone")
    return parsed.astimezone(UTC)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def exchange(command, env, timeout=35):
    """Perform initialize, tools/list, and one actual coin_analysis MCP call."""
    proc = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL, env=env, bufsize=0)
    selector = selectors.DefaultSelector()
    selector.register(proc.stdout, selectors.EVENT_READ)
    os.set_blocking(proc.stdout.fileno(), False)
    deadline = time.monotonic() + timeout
    buffer = b""
    transcript = []

    def send(value):
        proc.stdin.write(json.dumps(value, allow_nan=False).encode() + b"\n")
        proc.stdin.flush()

    def response(identifier):
        nonlocal buffer
        for _ in range(100):
            while b"\n" not in buffer:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not selector.select(remaining):
                    raise TradingViewMCPError("MCP response deadline exceeded")
                part = os.read(proc.stdout.fileno(), 65536)
                if not part:
                    raise TradingViewMCPError("MCP server exited before response")
                buffer += part
                if len(buffer) > MAX_BYTES:
                    raise TradingViewMCPError("MCP response size exceeded")
            line, buffer = buffer.split(b"\n", 1)
            value = json.loads(line)
            transcript.append(value)
            if value.get("id") == identifier:
                if value.get("error") or "result" not in value:
                    raise TradingViewMCPError("MCP returned a protocol/tool error")
                return value["result"]
        raise TradingViewMCPError("MCP notification budget exceeded")

    try:
        send({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2025-06-18", "capabilities": {},
            "clientInfo": {"name": "NeoRome-BTC-Paper", "version": "3"}}})
        initialized = response(1)
        if (initialized.get("protocolVersion") != "2025-06-18"
                or initialized.get("serverInfo", {}).get("name") != "TradingView BTCUSD Paper Feed"):
            raise TradingViewMCPError("MCP initialization identity missing")
        send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        # Cold imports must not consume the subsequent protocol/API budgets.
        deadline = time.monotonic() + min(timeout, 5)
        send({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
        tools = response(2).get("tools", [])
        if [item.get("name") for item in tools] != ["coin_analysis"]:
            raise TradingViewMCPError("restricted TradingView MCP tool catalog differs")
        deadline = time.monotonic() + min(timeout, 15)
        send({"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {
            "name": "coin_analysis", "arguments": {
                "symbol": "BTCUSD", "exchange": "COINBASE", "timeframe": "30m"}}})
        result = response(3)
        if result.get("isError"):
            raise TradingViewMCPError("TradingView MCP call refused")
        content = result.get("content", [])
        if len(content) != 1 or content[0].get("type") != "text":
            raise TradingViewMCPError("TradingView MCP content shape invalid")
        data = json.loads(content[0]["text"])
        if not isinstance(data, dict) or data.get("error"):
            raise TradingViewMCPError("TradingView MCP analysis error")
        return {"data": data, "mcp_server": initialized["serverInfo"],
                "mcp_protocol_version": initialized["protocolVersion"],
                "mcp_transcript_sha256": digest(transcript), "tool": "coin_analysis"}
    finally:
        selector.close()
        proc.stdin.close()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=2)
        proc.stdout.close()


def capture():
    """Ask the private warm broker to make one actual stdio MCP tool call."""
    info = BROKER_SOCKET.lstat()
    if (not stat.S_ISSOCK(info.st_mode) or info.st_uid != BROKER_UID
            or stat.S_IMODE(info.st_mode) & 0o077):
        raise TradingViewMCPError("private MCP socket identity/permissions differ")
    deadline = time.monotonic() + 25
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(25)
        connection.connect(str(BROKER_SOCKET))
        connection.sendall(json.dumps({"method": "coin_analysis", "symbol": "BTCUSD",
                                       "exchange": "COINBASE", "timeframe": "30m"}).encode() + b"\n")
        body = b""
        while b"\n" not in body:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TradingViewMCPError("warm MCP response deadline exceeded")
            connection.settimeout(remaining)
            part = connection.recv(65536)
            if not part:
                raise TradingViewMCPError("warm MCP broker closed before response")
            body += part
            if len(body) > MAX_BYTES:
                raise TradingViewMCPError("warm MCP response size exceeded")
    result = json.loads(body.split(b"\n", 1)[0])
    if result.get("ok") is not True:
        raise TradingViewMCPError("warm MCP call refused: " + str(result.get("error", "unknown"))[:120])
    if result.get("mcp_server", {}).get("name") != "TradingView BTCUSD Paper Feed":
        raise TradingViewMCPError("warm TradingView MCP server identity differs")
    return {key: result[key] for key in ("data", "mcp_server", "mcp_protocol_version", "mcp_transcript_sha256", "tool")}


def _number(value):
    if type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value <= 100:
        raise TradingViewMCPError("TradingView MCP oscillator must be finite and within [0,100]")
    return value


def evaluate(record, root, now):
    """Compare two adjacent closed TradingView bars, never forming-bar K."""
    data = record["data"]
    if data.get("symbol") != SYMBOL or data.get("timeframe") != "30m":
        raise TradingViewMCPError("TradingView MCP symbol/timeframe differs")
    metadata = data.get("source_metadata", {})
    if metadata.get("endpoint") != "https://scanner.tradingview.com/crypto/scan":
        raise TradingViewMCPError("TradingView MCP upstream endpoint differs")
    source_hash = metadata.get("source_response_sha256")
    if not isinstance(source_hash, str) or len(source_hash) != 64 or any(c not in "0123456789abcdef" for c in source_hash):
        raise TradingViewMCPError("TradingView MCP upstream digest missing")
    received = stamp(metadata["received_at"])
    started = stamp(metadata["request_started_at"])
    rest_date = stamp(metadata["http_date"])
    if not 0 <= (now - received).total_seconds() <= 15:
        raise TradingViewMCPError("TradingView MCP reading is stale or future dated")
    if not -2 <= (received - rest_date).total_seconds() <= 15:
        raise TradingViewMCPError("TradingView MCP upstream Date is stale or future dated")
    if metadata.get("cache_age") is not None and not 0 <= float(metadata["cache_age"]) <= 15:
        raise TradingViewMCPError("TradingView MCP upstream cache is stale")
    if (type(metadata.get("request_count")) is not int or metadata.get("request_count") != 1 or metadata.get("cache_disabled") is not True
            or metadata.get("stale_disabled") is not True):
        raise TradingViewMCPError("TradingView MCP strict source profile missing")
    closed_at = int(received.timestamp()) // 1800 * 1800
    if started > received or int(started.timestamp()) // 1800 * 1800 != closed_at:
        raise TradingViewMCPError("TradingView MCP request crossed a 30-minute boundary")
    values = data.get("stochastic", {})
    k, d = _number(values.get("previous_k")), _number(values.get("previous_d"))
    context = {"current_forming_k": _number(values.get("k")),
               "current_forming_d": _number(values.get("d")),
               "closed_k": k, "closed_d": d,
               "closed_at_inferred": closed_at, "explicit_source_bar_timestamp": False}
    root = Path(root)
    path = root / STATE_NAME
    if path.is_symlink():
        raise TradingViewMCPError("TradingView signal state symlink refused")
    state = json.loads(path.read_text()) if path.exists() else {
        "schema": "tradingview-closed-bars-v1", "symbol": SYMBOL, "readings": []}
    if state.get("schema") != "tradingview-closed-bars-v1" or state.get("symbol") != SYMBOL:
        raise TradingViewMCPError("TradingView signal state identity differs")
    readings = state["readings"]
    if not isinstance(readings, list) or len(readings) > 2:
        raise TradingViewMCPError("TradingView signal state history shape invalid")
    for item in readings:
        _number(item["k"]); _number(item["d"])
        if type(item["bar_close_timestamp"]) is not int or item["bar_close_timestamp"] % 1800:
            raise TradingViewMCPError("TradingView stored bar is not aligned")
        observed = stamp(item["observed_at"])
        if observed > now:
            raise TradingViewMCPError("TradingView stored observation is future dated")
        if not 0 <= observed.timestamp() - item["bar_close_timestamp"] < 1800:
            raise TradingViewMCPError("TradingView stored observation crosses its inferred bar")
    prior = next((r for r in readings if r["bar_close_timestamp"] == closed_at - 1800), None)
    current = {"bar_close_timestamp": closed_at, "k": k, "d": d,
               "observed_at": received.isoformat(), "source_hash": record["source_hash"]}
    retained = [r for r in readings if r["bar_close_timestamp"] < closed_at][-1:] + [current]
    temporary = path.with_suffix(".tmp")
    if temporary.is_symlink():
        raise TradingViewMCPError("TradingView temporary signal state symlink refused")
    temporary.write_text(json.dumps(state | {"readings": retained}, allow_nan=False) + "\n")
    temporary.chmod(0o600); temporary.replace(path)
    selected = None
    if prior and 0 <= now.timestamp() - closed_at <= 90:
        direction = ("bullish" if prior["k"] <= 20 and k > prior["k"] and prior["k"] <= prior["d"] and k > d else
                     "bearish" if prior["k"] >= 80 and k < prior["k"] and prior["k"] >= prior["d"] and k < d else None)
        if direction:
            selected = {"signal_id": f"btc-tv-stoch-30m-{direction}-cross-{closed_at}",
                        "instrument": "BTC-USD", "timeframe_minutes": 30, "method": "fresh_cross",
                        "direction": direction, "contract_side": "yes" if direction == "bullish" else "no",
                        "bar_close_timestamp": closed_at, "previous_k": prior["k"], "previous_d": prior["d"],
                        "k": k, "d": d, "oversold": 20, "overbought": 80, "valid": True,
                        "observed_receipt_at": record["received_at"], "source_hash": record["source_hash"],
                        "signal_provider": "tradingview_mcp", "prior_source_hash": prior["source_hash"],
                        "explicit_source_bar_timestamp": False}
    return {"selected_signal": selected, "chart_context": context,
            "signal_provider": "tradingview_mcp", "chart_context_hash": record["source_hash"],
            "reason": "awaiting_adjacent_closed_tradingview_bar" if prior is None else
                      "no_current_closed_30m_extreme_fresh_cross"}
