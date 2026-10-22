"""Fetch completed COINBASE:BTCUSD one-minute bars for isolated SMA paper tests."""
from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlsplit

from websockets.asyncio.client import connect


SYMBOL = "COINBASE:BTCUSD"
TIMEFRAME = "1m"
BAR_SECONDS = 60
STOCHASTIC_SETTINGS = [5, 3, 3]
ENDPOINT = "wss://data.tradingview.com/socket.io/websocket"
_URI_PREFIX = ENDPOINT + "?from=chart%2F&date="
_ALLOWED_HOST = "data.tradingview.com"
_ORIGIN = "https://www.tradingview.com"
_SERVER_NAME = "TradingView BTCUSD SMA1m Paper Feed"
_GUEST_MARKER = "unauthorized_user_token"
_MAX_WALL_SECONDS = 15.0
_MAX_RESPONSE_BYTES = 2 * 1024 * 1024
_REQUESTED_BARS = 200
_FRESHNESS_SECONDS = 15.0


class TradingViewSourceError(RuntimeError):
    """Fail-closed error raised when the public TradingView response is unusable."""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _finite_number(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TradingViewSourceError(f"TradingView returned invalid {label}")
    result = float(value)
    if not math.isfinite(result):
        raise TradingViewSourceError(f"TradingView returned non-finite {label}")
    return result


def _tv_message(method: str, params: list[Any]) -> str:
    encoded = json.dumps({"m": method, "p": params}, separators=(",", ":"), ensure_ascii=True)
    return f"~m~{len(encoded.encode('utf-8'))}~m~{encoded}"


def _decode_tv_frames(message: str) -> list[dict[str, Any] | str]:
    """Decode TradingView's length-prefixed frames, preserving heartbeat frames."""
    raw = message.encode("utf-8")
    result: list[dict[str, Any] | str] = []
    offset = 0
    while offset < len(raw):
        if raw.startswith(b"~h~", offset):
            next_packet = raw.find(b"~m~", offset + 3)
            end = len(raw) if next_packet < 0 else next_packet
            heartbeat = raw[offset:end].decode("ascii", "strict")
            if not re.fullmatch(r"~h~[0-9]+", heartbeat):
                raise TradingViewSourceError("TradingView sent a malformed heartbeat")
            result.append(heartbeat)
            offset = end
            continue
        if not raw.startswith(b"~m~", offset):
            raise TradingViewSourceError("TradingView sent an unknown chart frame")
        length_end = raw.find(b"~m~", offset + 3)
        if length_end < 0:
            raise TradingViewSourceError("TradingView sent a truncated chart frame")
        length_text = raw[offset + 3 : length_end]
        if not length_text or not length_text.isdigit() or len(length_text) > 9:
            raise TradingViewSourceError("TradingView sent an invalid chart frame length")
        payload_start = length_end + 3
        payload_length = int(length_text)
        payload_end = payload_start + payload_length
        if payload_end > len(raw):
            raise TradingViewSourceError("TradingView sent a truncated chart payload")
        try:
            payload = raw[payload_start:payload_end]
            if payload.startswith(b"~h~"):
                heartbeat = payload.decode("ascii", "strict")
                if not re.fullmatch(r"~h~[0-9]+", heartbeat):
                    raise TradingViewSourceError("TradingView sent a malformed heartbeat")
                result.append(heartbeat)
                offset = payload_end
                continue
            decoded = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise TradingViewSourceError("TradingView sent invalid chart JSON") from None
        if isinstance(decoded, dict):
            result.append(decoded)
        else:
            raise TradingViewSourceError("TradingView chart frame was not an object")
        offset = payload_end
    return result


def _reject_delayed_flags(value: Any) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = str(key).lower().replace("_", "")
            if normalized in {"delay", "secondsdelay", "delayed", "isdelayed", "delayeddata", "isdelayeddata"}:
                if item is True or (isinstance(item, (int, float)) and not isinstance(item, bool) and item > 0):
                    raise TradingViewSourceError("TradingView flagged the symbol data as delayed")
                if isinstance(item, str) and item.strip().lower() in {"true", "delayed", "yes"}:
                    raise TradingViewSourceError("TradingView flagged the symbol data as delayed")
            if normalized == "datastatus" and isinstance(item, str) and "delayed" in item.lower():
                raise TradingViewSourceError("TradingView flagged the symbol data as delayed")
            _reject_delayed_flags(item)
    elif isinstance(value, list):
        for item in value:
            _reject_delayed_flags(item)


def _extract_rows(params: Any) -> list[list[Any]]:
    if not isinstance(params, list) or len(params) < 2 or not isinstance(params[1], dict):
        raise TradingViewSourceError("TradingView timescale response is malformed")
    series = params[1].get("sds_1")
    if not isinstance(series, dict) or not isinstance(series.get("s"), list):
        raise TradingViewSourceError("TradingView omitted the requested OHLC series")
    rows: list[list[Any]] = []
    seen_in_update: set[int] = set()
    previous: int | None = None
    for item in series["s"]:
        if not isinstance(item, dict) or not isinstance(item.get("v"), list) or len(item["v"]) < 5:
            raise TradingViewSourceError("TradingView returned an incomplete OHLC row")
        values = item["v"]
        timestamp = _finite_number(values[0], "bar timestamp")
        if not timestamp.is_integer():
            raise TradingViewSourceError("TradingView returned a fractional bar timestamp")
        stamp = int(timestamp)
        if stamp in seen_in_update or (previous is not None and stamp <= previous):
            raise TradingViewSourceError("TradingView bars were duplicate or out of order")
        seen_in_update.add(stamp)
        previous = stamp
        rows.append(values)
    return rows


def validate_bars(rows: list[list[Any]], server_time: int) -> list[dict[str, int | float]]:
    """Validate identity-independent OHLC rows and remove the forming 1-minute bar."""
    if not isinstance(server_time, int) or server_time <= 0:
        raise TradingViewSourceError("TradingView server time is missing")
    by_timestamp: dict[int, list[Any]] = {}
    for row in rows:
        if not isinstance(row, list) or len(row) < 5:
            raise TradingViewSourceError("TradingView returned an incomplete OHLC row")
        ts_value = _finite_number(row[0], "bar timestamp")
        if not ts_value.is_integer():
            raise TradingViewSourceError("TradingView returned a fractional bar timestamp")
        timestamp = int(ts_value)
        if timestamp % BAR_SECONDS != 0:
            raise TradingViewSourceError("TradingView returned a misaligned 1-minute bar")
        if timestamp in by_timestamp:
            raise TradingViewSourceError("TradingView returned duplicate bars")
        by_timestamp[timestamp] = row

    all_timestamps = list(by_timestamp)
    if any(current <= previous for previous, current in zip(all_timestamps, all_timestamps[1:])):
        raise TradingViewSourceError("TradingView 1-minute bars were out of order")
    current_open = server_time // BAR_SECONDS * BAR_SECONDS
    if any(timestamp > current_open for timestamp in all_timestamps):
        raise TradingViewSourceError("TradingView returned a future 1-minute bar")
    for previous, current in zip(all_timestamps, all_timestamps[1:]):
        if current - previous != BAR_SECONDS:
            raise TradingViewSourceError("TradingView 1-minute bar history has a gap")

    closed: list[dict[str, int | float]] = []
    for timestamp in all_timestamps:
        if timestamp + BAR_SECONDS > server_time:
            continue
        row = by_timestamp[timestamp]
        open_price, high, low, close = (
            _finite_number(row[index], field)
            for index, field in ((1, "open"), (2, "high"), (3, "low"), (4, "close"))
        )
        if min(open_price, high, low, close) <= 0:
            raise TradingViewSourceError("TradingView returned non-positive BTC OHLC prices")
        if low > min(open_price, close) or high < max(open_price, close) or low > high:
            raise TradingViewSourceError("TradingView OHLC values are inconsistent")
        bar: dict[str, int | float] = {
            "timestamp": timestamp,
            "bar_close_timestamp": timestamp + BAR_SECONDS,
            "open": open_price,
            "high": high,
            "low": low,
            "close": close,
        }
        if len(row) >= 6:
            volume = _finite_number(row[5], "volume")
            if volume < 0:
                raise TradingViewSourceError("TradingView returned negative volume")
            bar["volume"] = volume
        closed.append(bar)
    if len(closed) < 10:
        raise TradingViewSourceError("TradingView returned fewer than 10 closed bars")
    return closed[-200:]


def calculate_stochastic(bars: list[dict[str, Any]]) -> list[dict[str, int | float]]:
    """Calculate slow Stoch(5,3,3) as SMA3(fast %K) and SMA3(slow %K)."""
    if len(bars) < 10:
        raise TradingViewSourceError("Stoch(5,3,3) requires at least 10 closed bars")
    fast_k: list[float] = []
    for index in range(len(bars)):
        if index < 4:
            continue
        window = bars[index - 4 : index + 1]
        highest = max(float(bar["high"]) for bar in window)
        lowest = min(float(bar["low"]) for bar in window)
        denominator = highest - lowest
        if not math.isfinite(denominator) or denominator <= 0:
            raise TradingViewSourceError("Stoch(5,3,3) encountered a zero-range bar window")
        value = 100.0 * (float(bars[index]["close"]) - lowest) / denominator
        if not math.isfinite(value):
            raise TradingViewSourceError("Stoch(5,3,3) produced a non-finite %K")
        fast_k.append(value)

    smoothed_k: list[float] = []
    first_index = 6
    for index in range(2, len(fast_k)):
        value = sum(fast_k[index - 2 : index + 1]) / 3.0
        smoothed_k.append(value)
    readings: list[dict[str, int | float]] = []
    for index in range(2, len(smoothed_k)):
        k = smoothed_k[index]
        d = sum(smoothed_k[index - 2 : index + 1]) / 3.0
        bar_index = first_index + index
        if not math.isfinite(k) or not math.isfinite(d):
            raise TradingViewSourceError("Stoch(5,3,3) produced a non-finite reading")
        readings.append({"bar_close_timestamp": int(bars[bar_index]["bar_close_timestamp"]), "k": k, "d": d})
    if len(readings) < 2:
        raise TradingViewSourceError("TradingView history cannot produce two stochastic readings")
    return readings


class _NoRedirectConnect(connect):
    """Refuse every HTTP redirect instead of following it to another route."""

    def process_redirect(self, exc: Exception) -> Exception:
        return exc


def _http_date(value: str | None) -> datetime:
    if not isinstance(value, str) or not value:
        raise TradingViewSourceError("TradingView handshake omitted its HTTP Date")
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        raise TradingViewSourceError("TradingView handshake Date was invalid") from None
    if parsed.tzinfo is None:
        raise TradingViewSourceError("TradingView handshake Date lacked a timezone")
    return parsed.astimezone(timezone.utc)


async def fetch_bars() -> dict[str, Any]:
    """Make one bounded uncached chart request; never require stochastic for SMA-only."""
    if ENDPOINT.split("/", 3)[2] != _ALLOWED_HOST:
        raise TradingViewSourceError("TradingView endpoint host is not allowlisted")
    request_started = _utc_now()
    uri = _URI_PREFIX + request_started.strftime("%Y_%m_%d-%H_%M")
    aggregate = bytearray()
    bars_by_timestamp: dict[int, list[Any]] = {}
    server_time: int | None = None
    resolved = False
    series_done = False
    sid = "cs_" + hashlib.sha256(request_started.isoformat().encode()).hexdigest()[:16]
    try:
        async with asyncio.timeout(_MAX_WALL_SECONDS):
            async with _NoRedirectConnect(
                uri,
                origin=_ORIGIN,
                additional_headers={"User-Agent": "Mozilla/5.0"},
                proxy=None,
                open_timeout=5.0,
                close_timeout=1.0,
                ping_interval=10.0,
                ping_timeout=5.0,
                max_size=_MAX_RESPONSE_BYTES,
                max_queue=16,
                compression=None,
            ) as ws:
                expected_path = urlsplit(uri).path + "?" + urlsplit(uri).query
                if ws.request.path != expected_path or ws.response.status_code != 101:
                    raise TradingViewSourceError("TradingView WebSocket handshake was redirected or invalid")
                parsed_host = ws.request.headers.get("Host", "").split(":", 1)[0].lower()
                if parsed_host != _ALLOWED_HOST:
                    raise TradingViewSourceError("TradingView WebSocket host was not allowlisted")
                date_value = ws.response.headers.get("Date")
                http_date = _http_date(date_value)
                await ws.send(_tv_message("set_auth_token", [_GUEST_MARKER]))
                await ws.send(_tv_message("chart_create_session", [sid, ""]))
                symbol_query = "=" + json.dumps(
                    {"symbol": SYMBOL, "adjustment": "splits", "session": "regular"},
                    separators=(",", ":"),
                )
                await ws.send(_tv_message("resolve_symbol", [sid, "symbol_1", symbol_query]))

                while not series_done:
                    raw_message = await ws.recv()
                    encoded = raw_message.encode("utf-8") if isinstance(raw_message, str) else bytes(raw_message)
                    if len(aggregate) + len(encoded) > _MAX_RESPONSE_BYTES:
                        raise TradingViewSourceError("TradingView response exceeded 2 MiB")
                    aggregate.extend(encoded)
                    for frame in _decode_tv_frames(raw_message.decode("utf-8") if isinstance(raw_message, bytes) else raw_message):
                        if isinstance(frame, str):
                            heartbeat = f"~m~{len(frame.encode('utf-8'))}~m~{frame}"
                            await ws.send(heartbeat)
                            continue
                        if frame.get("session_id"):
                            timestamp = frame.get("timestamp")
                            if isinstance(timestamp, (int, float)) and not isinstance(timestamp, bool) and math.isfinite(float(timestamp)):
                                server_time = int(timestamp)
                            continue
                        method = frame.get("m")
                        params = frame.get("p", [])
                        if isinstance(frame.get("t"), (int, float)) and not isinstance(frame.get("t"), bool):
                            server_time = int(frame["t"])
                        if method in {"critical_error", "symbol_error", "series_error"}:
                            raise TradingViewSourceError("TradingView rejected the fixed symbol or chart request")
                        if method == "symbol_resolved":
                            if not isinstance(params, list) or len(params) < 3 or not isinstance(params[2], dict):
                                raise TradingViewSourceError("TradingView symbol resolution was malformed")
                            info = params[2]
                            _reject_delayed_flags(info)
                            if (
                                info.get("full_name") != SYMBOL
                                or info.get("source_id") != "COINBASE"
                                or info.get("provider_id") != "coinbase"
                                or info.get("name") != "BTCUSD"
                            ):
                                raise TradingViewSourceError("TradingView resolved a different market")
                            if info.get("type") != "spot":
                                raise TradingViewSourceError("TradingView returned a non-spot instrument")
                            resolved = True
                            await ws.send(_tv_message("create_series", [sid, "sds_1", "s1", "symbol_1", "1", _REQUESTED_BARS]))
                        elif method == "timescale_update":
                            if not resolved:
                                raise TradingViewSourceError("TradingView sent bars before symbol identity was verified")
                            rows = _extract_rows(params)
                            for row in rows:
                                timestamp = int(row[0])
                                bars_by_timestamp[timestamp] = row
                        elif method == "series_completed":
                            series_done = True
    except TradingViewSourceError:
        raise
    except TimeoutError:
        raise TradingViewSourceError("TradingView source request exceeded 15 seconds") from None
    except Exception as exc:
        raise TradingViewSourceError(f"TradingView source request failed: {type(exc).__name__}") from None

    received = _utc_now()
    if len(aggregate) > _MAX_RESPONSE_BYTES:
        raise TradingViewSourceError("TradingView response exceeded 2 MiB")
    if not resolved or not series_done or not bars_by_timestamp:
        raise TradingViewSourceError("TradingView response did not complete the requested chart series")
    if server_time is None or server_time <= 0:
        raise TradingViewSourceError("TradingView chart response omitted server time")
    server_age = (received.timestamp() - server_time)
    if not -1.0 <= server_age <= _FRESHNESS_SECONDS:
        raise TradingViewSourceError("TradingView chart server time is stale or in the future")
    http_age = (received - http_date).total_seconds()
    if not -1.0 <= http_age <= _FRESHNESS_SECONDS:
        raise TradingViewSourceError("TradingView WebSocket handshake Date is stale or in the future")

    bars = validate_bars([bars_by_timestamp[ts] for ts in sorted(bars_by_timestamp)], server_time)
    return {
        "symbol": SYMBOL,
        "timeframe": TIMEFRAME,
        "bars": bars,
        "raw_frames": bytes(aggregate).decode("utf-8"),
        "source_metadata": {
            "request_started_at": _utc_iso(request_started),
            "received_at": _utc_iso(received),
            "endpoint": ENDPOINT,
            "request_count": 1,
            "source_response_sha256": hashlib.sha256(bytes(aggregate)).hexdigest(),
            "explicit_source_bar_timestamp": True,
            "http_date": _utc_iso(http_date),
            "http_date_age_seconds": max(0.0, http_age),
            "server_timestamp": server_time,
            "server_time_age_seconds": max(0.0, server_age),
            "cache_age": 0,
            "cache_disabled": True,
            "stale_disabled": True,
            "freshness_evidence": "TradingView WebSocket handshake Date and chart server timestamp checked; bars filtered by explicit source open timestamp",
            "symbol_identity": {"full_name": SYMBOL, "source_id": "COINBASE", "provider_id": "coinbase"},
        },
    }
