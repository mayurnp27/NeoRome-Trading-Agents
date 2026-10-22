"""Long-lived, restricted Unix-socket broker for the TradingView paper MCP.

The broker owns one upstream stdio subprocess and one MCP session. It only
relays the fixed BTCUSD/COINBASE/5m Stoch(5,3,3) paper-analysis request; it does not
calculate signals, place orders, or cache market data.
"""
from __future__ import annotations

import asyncio
import errno
import hashlib
import json
import math
import os
import socket
import stat
import sys
import tempfile
from contextlib import AsyncExitStack
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RUNTIME_DIR = Path("/run/neorome-tradingview-fast-mcp")
SOCKET_PATH = RUNTIME_DIR / "feed.sock"
STATUS_PATH = RUNTIME_DIR / "status.json"
UPSTREAM_PACKAGE = Path(__file__).resolve().parents[1]
UPSTREAM_PYTHON = "/opt/neorome-tradingview-mcp-r1/venv/bin/python"
MCP_SERVER_NAME = "TradingView BTCUSD Fast Paper Feed"
SERVICE_UID = 997

REQUEST_LIMIT_BYTES = 4096
RESPONSE_LIMIT_BYTES = 2 * 1024 * 1024
REQUEST_TIMEOUT_SECONDS = 5.0
TOOL_TIMEOUT_SECONDS = 20.0
STARTUP_TIMEOUT_SECONDS = 300.0
SOCKET_MODE = 0o600
DIRECTORY_MODE = 0o700

_REQUEST = {
    "method": "fast_stochastic",
    "symbol": "BTCUSD",
    "exchange": "COINBASE",
    "timeframe": "5m",
}


class BrokerError(Exception):
    """A safe, caller-facing broker refusal."""


class BrokerFatalError(Exception):
    """An upstream session failure that requires a process restart."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _model_value(value: Any) -> Any:
    """Convert MCP SDK models and nested values to canonical JSON types."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): _model_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_model_value(item) for item in value]
    dump = getattr(value, "model_dump", None)
    if callable(dump):
        return _model_value(dump(mode="json", exclude_none=True))
    dump = getattr(value, "dict", None)
    if callable(dump):
        return _model_value(dump(exclude_none=True))
    raise BrokerFatalError("MCP returned an unsupported protocol value")


def canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(
            _model_value(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise BrokerFatalError("MCP response could not be canonicalized") from exc


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def validate_request(raw: bytes) -> dict[str, str]:
    """Accept only the exact fixed line protocol; never echo request values."""
    if not isinstance(raw, bytes) or len(raw) > REQUEST_LIMIT_BYTES:
        raise BrokerError("request too large")
    payload = raw[:-1] if raw.endswith(b"\n") else raw
    try:
        request = json.loads(payload.decode("utf-8"), object_pairs_hook=_reject_duplicate_keys)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        raise BrokerError("invalid JSON request") from None
    if not isinstance(request, dict) or set(request) != set(_REQUEST):
        raise BrokerError("request does not match the fixed paper-analysis schema")
    if request != _REQUEST:
        raise BrokerError("request is fixed to BTCUSD on COINBASE at 5m")
    return _REQUEST.copy()


def validate_analysis(data: Any) -> dict[str, Any]:
    """Refuse MCP errors and incomplete results instead of marking them ok."""
    if not isinstance(data, dict) or data.get("error") or data.get("ok") is False:
        raise BrokerError("upstream analysis returned an error")
    metadata = data.get("source_metadata")
    if not isinstance(metadata, dict):
        raise BrokerError("upstream analysis omitted source metadata")
    required_metadata = {
        "request_started_at", "received_at", "http_date", "cache_age",
        "request_count", "cache_disabled", "stale_disabled",
        "source_response_sha256", "endpoint",
    }
    if not required_metadata.issubset(metadata):
        raise BrokerError("upstream source metadata is incomplete")
    if metadata.get("request_count") != 1 or metadata.get("cache_disabled") is not True or metadata.get("stale_disabled") is not True:
        raise BrokerError("upstream source metadata failed freshness checks")
    cache_age = metadata.get("cache_age")
    if isinstance(cache_age, bool) or not isinstance(cache_age, (int, float)) or not math.isfinite(cache_age) or not 0 <= cache_age <= 15:
        raise BrokerError("upstream source metadata is stale or invalid")
    digest = metadata.get("source_response_sha256")
    if not isinstance(digest, str) or len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest.lower()):
        raise BrokerError("upstream response digest is missing or invalid")
    if metadata.get("endpoint") != "wss://data.tradingview.com/socket.io/websocket":
        raise BrokerError("upstream source endpoint is invalid")
    if (data.get("symbol") != "COINBASE:BTCUSD" or data.get("timeframe") != "5m"
            or data.get("stochastic_settings") != [5, 3, 3]
            or metadata.get("explicit_source_bar_timestamp") is not True):
        raise BrokerError("upstream exact 5m Stoch(5,3,3) profile differs")
    if not isinstance(data.get("bars"), list) or not isinstance(data.get("readings"), list) or len(data["readings"]) < 2:
        raise BrokerError("upstream omitted closed bars or adjacent stochastic readings")
    return data


def _catalog_value(catalog: Any) -> list[dict[str, Any]]:
    tools = getattr(catalog, "tools", None)
    if tools is None and isinstance(catalog, dict):
        tools = catalog.get("tools")
    if not isinstance(tools, (list, tuple)):
        raise BrokerFatalError("MCP tool catalog is malformed")
    try:
        result = [_model_value(tool) for tool in tools]
    except BrokerFatalError:
        raise
    if len(result) != 1 or not isinstance(result[0], dict) or result[0].get("name") != "fast_stochastic":
        raise BrokerFatalError("MCP server exposed an unexpected tool catalog")
    return result


def _server_info(init_result: Any) -> dict[str, Any]:
    info = getattr(init_result, "serverInfo", None)
    if info is None and isinstance(init_result, dict):
        info = init_result.get("serverInfo")
    try:
        result = _model_value(info)
    except BrokerFatalError:
        raise BrokerFatalError("MCP initialization response is malformed") from None
    if not isinstance(result, dict) or result.get("name") != MCP_SERVER_NAME:
        raise BrokerFatalError("MCP server identity did not match the paper profile")
    return result


def _protocol_version(init_result: Any) -> str:
    version = getattr(init_result, "protocolVersion", None)
    if version is None and isinstance(init_result, dict):
        version = init_result.get("protocolVersion")
    if not isinstance(version, str) or not version:
        raise BrokerFatalError("MCP initialization omitted its protocol version")
    return version


async def initialize_session(session: Any) -> tuple[dict[str, Any], str, list[dict[str, Any]], dict[str, Any]]:
    """Initialize exactly once and validate server identity plus sole tool."""
    try:
        initialized = await session.initialize()
        initialize_message = _model_value(initialized)
        info = _server_info(initialized)
        version = _protocol_version(initialized)
        catalog = _catalog_value(await session.list_tools())
    except BrokerFatalError:
        raise
    except Exception as exc:
        raise BrokerFatalError("MCP initialization or tool discovery failed") from exc
    return info, version, catalog, initialize_message


async def await_with_timeout(awaitable: Any, timeout_seconds: float) -> Any:
    """Bound an await without moving async context managers into child tasks.

    The MCP stdio transport owns AnyIO cancel scopes that must be exited in
    the same task where they were entered. asyncio.wait_for() creates a child
    task, so use a timer that cancels the current task instead.
    """
    task = asyncio.current_task()
    if task is None:
        raise RuntimeError("timeout helper requires an asyncio task")
    timed_out = False

    def cancel_for_timeout() -> None:
        nonlocal timed_out
        timed_out = True
        task.cancel()

    handle = asyncio.get_running_loop().call_later(timeout_seconds, cancel_for_timeout)
    try:
        return await awaitable
    except asyncio.CancelledError:
        if timed_out:
            raise asyncio.TimeoutError from None
        raise
    finally:
        handle.cancel()


def _content_text(result: Any) -> str:
    content = getattr(result, "content", None)
    if not isinstance(content, (list, tuple)) or len(content) != 1:
        raise BrokerFatalError("MCP tool response did not contain one text item")
    item = content[0]
    item_type = getattr(item, "type", None)
    text = getattr(item, "text", None)
    if item_type != "text" or not isinstance(text, str):
        raise BrokerFatalError("MCP tool response was not a text result")
    return text


def response_from_analysis(
    analysis: dict[str, Any],
    server_info: dict[str, Any],
    protocol_version: str,
    catalog: list[dict[str, Any]],
    initialize_message: dict[str, Any],
) -> dict[str, Any]:
    data = validate_analysis(analysis)
    transcript = {
        "initialize": initialize_message,
        "catalog": catalog,
        "response": data,
    }
    transcript_hash = hashlib.sha256(canonical_json(transcript)).hexdigest()
    return {
        "ok": True,
        "data": data,
        "mcp_server": server_info,
        "mcp_protocol_version": protocol_version,
        "mcp_transcript_sha256": transcript_hash,
        "tool": "fast_stochastic",
    }


def encode_response(response: dict[str, Any]) -> bytes:
    """Encode one bounded newline-delimited reply, replacing oversize data."""
    try:
        encoded = json.dumps(response, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8") + b"\n"
    except (TypeError, ValueError, UnicodeEncodeError):
        encoded = b'{"ok":false,"error":"response encoding failed"}\n'
    if len(encoded) > RESPONSE_LIMIT_BYTES:
        encoded = b'{"ok":false,"error":"response too large"}\n'
    return encoded


def verify_runtime_directory(path: Path, expected_uid: int, actual_uid: int) -> None:
    if actual_uid != expected_uid:
        raise BrokerFatalError("broker process has the wrong service identity")
    try:
        info = path.lstat()
    except OSError as exc:
        raise BrokerFatalError("systemd runtime directory is unavailable") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise BrokerFatalError("runtime path is not a private directory")
    if info.st_uid != expected_uid or stat.S_IMODE(info.st_mode) != DIRECTORY_MODE:
        raise BrokerFatalError("runtime directory ownership or mode is unsafe")


def remove_stale_owned_socket(path: Path, expected_uid: int) -> None:
    """Remove only an owned stale socket after proving no listener accepts."""
    try:
        info = path.lstat()
    except FileNotFoundError:
        return
    except OSError as exc:
        raise BrokerFatalError("cannot inspect existing feed socket") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISSOCK(info.st_mode):
        raise BrokerFatalError("existing feed path is not a Unix socket")
    if info.st_uid != expected_uid:
        raise BrokerFatalError("existing feed socket has a foreign owner")

    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        probe.settimeout(0.5)
        probe.connect(str(path))
    except OSError as exc:
        if exc.errno not in (errno.ECONNREFUSED, errno.ENOENT):
            raise BrokerFatalError("cannot safely check existing feed socket") from exc
    else:
        raise BrokerFatalError("an active feed broker already owns the socket")
    finally:
        probe.close()

    try:
        current = path.lstat()
    except FileNotFoundError:
        return
    if (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino):
        raise BrokerFatalError("feed socket changed during stale-socket check")
    if stat.S_ISSOCK(current.st_mode) and current.st_uid == expected_uid:
        path.unlink()


def unlink_own_socket(path: Path, expected_uid: int) -> None:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return
    except OSError:
        return
    if stat.S_ISSOCK(info.st_mode) and info.st_uid == expected_uid:
        try:
            path.unlink()
        except OSError:
            pass


def write_status(
    path: Path,
    state: str,
    *,
    server_info: dict[str, Any] | None = None,
    error: str | None = None,
    pid: int | None = None,
) -> None:
    if state not in {"initializing", "ready", "error"}:
        raise ValueError("invalid broker status state")
    record: dict[str, Any] = {"state": state, "pid": pid or os.getpid(), "updated_at": _utc_now()}
    if server_info is not None:
        record["mcp_server"] = server_info
    if error is not None:
        record["error"] = error[:120]
    payload = json.dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8")
    fd, temporary = tempfile.mkstemp(prefix=".status-", dir=str(path.parent))
    try:
        os.fchmod(fd, SOCKET_MODE)
        with os.fdopen(fd, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


async def call_analysis(
    session: Any,
    server_info: dict[str, Any],
    protocol_version: str,
    catalog: list[dict[str, Any]],
    initialize_message: dict[str, Any],
    call_lock: asyncio.Lock,
) -> dict[str, Any]:
    async with call_lock:
        try:
            result = await await_with_timeout(
                session.call_tool(
                    "fast_stochastic",
                    arguments={"symbol": "BTCUSD", "exchange": "COINBASE", "timeframe": "5m"},
                ),
                TOOL_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError as exc:
            raise BrokerFatalError("MCP tool call timed out") from exc
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise BrokerFatalError("MCP session call failed") from exc
        if getattr(result, "isError", False):
            raise BrokerError("upstream MCP tool reported an error")
        text = _content_text(result)
        try:
            parsed = json.loads(text)
        except (json.JSONDecodeError, TypeError):
            raise BrokerFatalError("MCP tool result was not valid JSON") from None
        return response_from_analysis(parsed, server_info, protocol_version, catalog, initialize_message)


class TradingViewBroker:
    def __init__(self) -> None:
        self.session: Any = None
        self.server_info: dict[str, Any] | None = None
        self.protocol_version: str | None = None
        self.catalog: list[dict[str, Any]] | None = None
        self.initialize_message: dict[str, Any] | None = None
        self.call_lock = asyncio.Lock()
        self.stack: AsyncExitStack | None = None
        self.server: asyncio.AbstractServer | None = None
        self.socket_created = False
        self.fatal_event = asyncio.Event()

    async def connect_backend(self) -> None:
        # Imported only in production startup; offline unit tests need stdlib.
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client

        package = UPSTREAM_PACKAGE.resolve()
        if not package.is_dir() or not (package / "neorome" / "tradingview_fast_mcp.py").is_file():
            raise BrokerFatalError("restricted upstream MCP package is unavailable")
        env = {
            "PATH": "/usr/bin:/bin",
            "PYTHONPATH": str(package) + os.pathsep + str(package / "vendor"),
            "PYTHONDONTWRITEBYTECODE": "1",
            "OPENBLAS_NUM_THREADS": "1",
        }
        params = StdioServerParameters(
            command=UPSTREAM_PYTHON,
            args=["-B", "-m", "neorome.tradingview_fast_mcp"],
            env=env,
            cwd=str(package),
        )
        self.stack = AsyncExitStack()
        await self.stack.__aenter__()
        try:
            read_stream, write_stream = await self.stack.enter_async_context(stdio_client(params))
            self.session = await self.stack.enter_async_context(ClientSession(read_stream, write_stream))
            (
                self.server_info,
                self.protocol_version,
                self.catalog,
                self.initialize_message,
            ) = await initialize_session(self.session)
        except BaseException:
            await self.stack.aclose()
            self.stack = None
            raise

    async def warmup(self) -> None:
        if (
            self.session is None or self.server_info is None or self.protocol_version is None
            or self.catalog is None or self.initialize_message is None
        ):
            raise BrokerFatalError("MCP backend is not initialized")
        # Deployment starts this service before the paper trade window. Warm the
        # persistent subprocess/session once, then discard this observation.
        await call_analysis(
            self.session, self.server_info, self.protocol_version, self.catalog,
            self.initialize_message, self.call_lock,
        )

    async def bootstrap(self) -> None:
        async def startup() -> None:
            await self.connect_backend()
            await self.warmup()

        await await_with_timeout(startup(), STARTUP_TIMEOUT_SECONDS)

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        fatal: BrokerFatalError | None = None
        try:
            try:
                raw = await asyncio.wait_for(reader.readuntil(b"\n"), timeout=REQUEST_TIMEOUT_SECONDS)
            except asyncio.TimeoutError:
                raise BrokerError("request timed out") from None
            except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, ValueError):
                raise BrokerError("invalid or oversized request") from None
            request = validate_request(raw)
            if request != _REQUEST:
                raise BrokerError("invalid request")
            if (
                self.session is None or self.server_info is None or self.protocol_version is None
                or self.catalog is None or self.initialize_message is None
            ):
                raise BrokerError("broker is not ready")
            response = await call_analysis(
                self.session, self.server_info, self.protocol_version, self.catalog,
                self.initialize_message, self.call_lock,
            )
        except BrokerFatalError as exc:
            fatal = exc
            response = {"ok": False, "error": str(exc)}
        except BrokerError as exc:
            response = {"ok": False, "error": str(exc)}
        except Exception:
            response = {"ok": False, "error": "broker request failed"}
        try:
            writer.write(encode_response(response))
            await asyncio.wait_for(writer.drain(), timeout=REQUEST_TIMEOUT_SECONDS)
        except Exception:
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
        if fatal is not None:
            self.fatal_event.set()

    async def listen(self) -> None:
        self.server = await asyncio.start_unix_server(
            self.handle_client, path=str(SOCKET_PATH), limit=REQUEST_LIMIT_BYTES + 1
        )
        self.socket_created = True
        os.chmod(SOCKET_PATH, SOCKET_MODE)
        socket_info = SOCKET_PATH.lstat()
        if not stat.S_ISSOCK(socket_info.st_mode) or socket_info.st_uid != SERVICE_UID:
            self.server.close()
            await self.server.wait_closed()
            raise BrokerFatalError("bound feed socket ownership is unsafe")
        write_status(STATUS_PATH, "ready", server_info=self.server_info, pid=os.getpid())

    async def serve(self) -> None:
        if self.server is None:
            raise BrokerFatalError("feed socket is not bound")
        serving = asyncio.create_task(self.server.serve_forever())
        fatal = asyncio.create_task(self.fatal_event.wait())
        done, pending = await asyncio.wait((serving, fatal), return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        if fatal in done and self.fatal_event.is_set():
            raise BrokerFatalError("upstream MCP session requires restart")
        await serving

    async def close(self, status_error: str | None = None) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
        if self.socket_created:
            unlink_own_socket(SOCKET_PATH, SERVICE_UID)
            self.socket_created = False
        if self.stack is not None:
            await self.stack.aclose()
            self.stack = None
        try:
            write_status(
                STATUS_PATH,
                "error",
                server_info=self.server_info,
                error=status_error or "broker stopped",
                pid=os.getpid(),
            )
        except OSError:
            pass


async def run_broker() -> None:
    verify_runtime_directory(RUNTIME_DIR, SERVICE_UID, os.geteuid())
    write_status(STATUS_PATH, "initializing", pid=os.getpid())
    broker = TradingViewBroker()
    failure: str | None = None
    try:
        remove_stale_owned_socket(SOCKET_PATH, SERVICE_UID)
        await broker.bootstrap()
        await broker.listen()
        await broker.serve()
    except BrokerFatalError as exc:
        failure = str(exc)
        raise
    except Exception as exc:
        failure = "broker startup or serving failed"
        raise BrokerFatalError(failure) from exc
    finally:
        await broker.close(failure)


def main() -> int:
    try:
        asyncio.run(run_broker())
    except BrokerFatalError as exc:
        print(f"TradingView broker failed: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
