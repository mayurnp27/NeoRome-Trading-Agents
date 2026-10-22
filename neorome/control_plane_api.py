"""Bounded, read-only HTTP view of NeoRome's control-plane store.

The service is loopback-only by default.  Binding to any non-loopback address
requires a bearer token at construction time; providing a token on loopback also
enables authentication.  Responses are projections rather than raw database
rows so internal tokens, credentials, and idempotency material never cross the
HTTP boundary.
"""

from __future__ import annotations

import argparse
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ipaddress
import json
import os
from pathlib import Path
import re
import sqlite3
import socket
from typing import Any, Iterable, Mapping
from urllib.parse import parse_qs, unquote, urlsplit

from .control_plane_store import ControlPlaneStore, ControlPlaneStoreError


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
DEFAULT_LIMIT = 50
MAX_LIMIT = 200
MAX_BODY_BYTES = 65_536
MAX_PATH_BYTES = 4_096
MAX_HEADER_BYTES = 16_384
MAX_HEADER_COUNT = 50
MAX_RESPONSE_BYTES = 1_048_576
MAX_QUERY_VALUE = 256

_TASK_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_SENSITIVE_KEY = re.compile(
    r"(?:authorization|cookie|credential|password|passwd|secret|token|api[_-]?key|private[_-]?key)",
    re.IGNORECASE,
)
_BEARER_VALUE = re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+")
_INLINE_SECRET = re.compile(
    r"(?i)\b(authorization|password|secret|token|api[_-]?key|private[_-]?key)\s*[:=]\s*([^\s,;]+)"
)

_EVENT_FIELDS = (
    "sequence", "event_id", "timestamp", "ingested_at", "desk", "agent_id",
    "event_type", "severity", "summary", "data", "correlation_id", "source",
    "schema_version",
)
_DESK_FIELDS = (
    "desk", "status", "created_at", "updated_at", "source", "schema_version",
    "last_event_id", "details",
)
_HEALTH_FIELDS = (
    "component", "status", "created_at", "updated_at", "source", "schema_version",
    "last_event_id", "details",
)
_REPORT_FIELDS = (
    "report_id", "report_type", "period_start", "period_end", "generated_at",
    "created_at", "updated_at", "source", "status", "schema_version",
    "structured", "human_readable", "last_event_id",
)
_TASK_FIELDS = (
    "task_id", "title", "description", "acceptance_criteria", "priority", "status",
    "created_at", "updated_at", "source", "schema_version", "created_by",
    "approved_by", "assigned_to", "dependencies", "blocked_by",
    "execution_environment", "results", "artifacts", "git_commit", "test_results",
    "lease_expires_at", "last_heartbeat_at", "last_event_id",
)
_POSITION_FIELDS = ("position_id", "proposal_id", "desk", "instrument", "exposure_group",
                    "idea_id", "quantity", "basis_cents", "state", "opened_at")
_ORDER_FIELDS = ("reservation_id", "proposal_id", "desk", "instrument", "exposure_group",
                 "idea_id", "quantity", "reserved_cents", "fee_cents", "state", "created_at", "expiry_at")
_TRADE_FIELDS = ("fill_id", "reservation_id", "position_id", "proposal_id", "desk", "instrument",
                 "exposure_group", "idea_id", "quantity", "total_cost_cents", "fee_cents",
                 "filled_at", "provenance", "execution_mode")


class APIError(Exception):
    """Expected request failure with an HTTP status and safe public message."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


def _is_loopback(host: str) -> bool:
    """Return true only for explicit loopback names or addresses."""
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _clean(value: Any) -> Any:
    """Recursively redact secret-shaped fields and bearer material."""
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for raw_key, item in value.items():
            key = str(raw_key)
            if _SENSITIVE_KEY.search(key):
                result[key] = "[REDACTED]"
            else:
                result[key] = _clean(item)
        return result
    if isinstance(value, (list, tuple)):
        return [_clean(item) for item in value]
    if isinstance(value, str):
        without_bearer = _BEARER_VALUE.sub("Bearer [REDACTED]", value)
        return _INLINE_SECRET.sub(lambda match: f"{match.group(1)}=[REDACTED]", without_bearer)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return str(value)


def _project(row: Mapping[str, Any], fields: Iterable[str]) -> dict[str, Any]:
    return _clean({field: row[field] for field in fields if field in row})


def _decode_row(row: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in dict(row).items():
        if key.endswith("_json"):
            public_key = key[:-5]
            try:
                result[public_key] = json.loads(value) if value is not None else None
            except (TypeError, json.JSONDecodeError):
                result[public_key] = None
        else:
            result[key] = value
    return result


def _query_value(query: Mapping[str, list[str]], name: str) -> str | None:
    values = query.get(name)
    if not values:
        return None
    if len(values) != 1 or len(values[0]) > MAX_QUERY_VALUE:
        raise APIError(400, f"invalid {name}")
    return values[0]


def _limit(query: Mapping[str, list[str]]) -> int:
    raw = _query_value(query, "limit")
    if raw is None:
        return DEFAULT_LIMIT
    try:
        value = int(raw)
    except ValueError as exc:
        raise APIError(400, "limit must be an integer") from exc
    if not 1 <= value <= MAX_LIMIT:
        raise APIError(400, f"limit must be between 1 and {MAX_LIMIT}")
    return value


class ControlPlaneView:
    """Safe response adapter over current and future ``ControlPlaneStore`` readers."""

    def __init__(self, store: ControlPlaneStore, paper_ledger: str | os.PathLike[str] | None = None) -> None:
        self.store = store
        self.paper_ledger = None if paper_ledger is None else Path(paper_ledger)

    def _paper_rows(self, query: str, values: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        if self.paper_ledger is None:
            return []
        if self.paper_ledger.is_symlink() or not self.paper_ledger.is_file():
            raise APIError(503, "paper ledger unavailable")
        try:
            uri = f"file:{self.paper_ledger.resolve()}?mode=ro"
            with sqlite3.connect(uri, uri=True, timeout=5) as connection:
                connection.execute("PRAGMA query_only=ON")
                connection.row_factory = sqlite3.Row
                marker = connection.execute("SELECT value FROM settings WHERE key='ledger_marker'").fetchone()
                if marker is None or marker[0] != "neorome-paper-ledger-v1":
                    raise APIError(503, "paper ledger unavailable")
                return [dict(row) for row in connection.execute(query, values).fetchall()]
        except sqlite3.Error as exc:
            raise APIError(503, "paper ledger unavailable") from exc

    def _rows(
        self,
        table: str,
        *,
        order: str,
        limit: int | None = None,
        where: str = "",
        values: tuple[Any, ...] = (),
    ) -> list[dict[str, Any]]:
        allowed = {"desks", "system_health", "incidents", "reports", "tasks"}
        if table not in allowed:
            raise ValueError("unsupported control-plane projection")
        suffix = f" WHERE {where}" if where else ""
        limit_sql = " LIMIT ?" if limit is not None else ""
        parameters = (*values, limit) if limit is not None else values
        with self.store._read() as conn:  # compatibility until all public readers land
            rows = conn.execute(
                f"SELECT * FROM {table}{suffix} ORDER BY {order}{limit_sql}",  # noqa: S608
                parameters,
            ).fetchall()
        return [_decode_row(row) for row in rows]

    def desks(self) -> list[dict[str, Any]]:
        method = getattr(self.store, "list_desks", None)
        rows = method() if callable(method) else self._rows("desks", order="desk")
        return [_project(row, _DESK_FIELDS) for row in rows]

    def activity(self, query: Mapping[str, list[str]]) -> list[dict[str, Any]]:
        limit = _limit(query)
        kwargs = {
            "desk": _query_value(query, "desk"),
            "event_type": _query_value(query, "event_type"),
            "severity": _query_value(query, "severity"),
            "correlation_id": _query_value(query, "correlation_id"),
            "since": _query_value(query, "since"),
            "until": _query_value(query, "until"),
            "limit": limit,
        }
        rows = self.store.list_events(**{key: value for key, value in kwargs.items() if value is not None})
        return [_project(row, _EVENT_FIELDS) for row in rows]

    def positions(self, query: Mapping[str, list[str]]) -> list[dict[str, Any]]:
        limit = _limit(query)
        clauses, values = [], []
        for column, name in (("desk", "desk"), ("instrument", "symbol"), ("state", "status")):
            value = _query_value(query, name)
            if value is not None:
                clauses.append(f"{column}=?")
                values.append(value)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        rows = self._paper_rows(
            "SELECT position_id,proposal_id,desk,instrument,exposure_group,idea_id,quantity,basis_cents,state,opened_at "
            f"FROM positions{where} ORDER BY opened_at DESC,position_id LIMIT ?", tuple(values + [limit]))
        return [_project(row, _POSITION_FIELDS) for row in rows]

    def position(self, position_id: str) -> dict[str, Any] | None:
        if not _TASK_ID.fullmatch(position_id):
            raise APIError(400, "invalid position id")
        rows = self._paper_rows(
            "SELECT position_id,proposal_id,desk,instrument,exposure_group,idea_id,quantity,basis_cents,state,opened_at "
            "FROM positions WHERE position_id=? LIMIT 1", (position_id,))
        return None if not rows else _project(rows[0], _POSITION_FIELDS)

    def orders(self, query: Mapping[str, list[str]]) -> list[dict[str, Any]]:
        limit = _limit(query)
        clauses, values = [], []
        for column, name in (("desk", "desk"), ("instrument", "symbol"), ("state", "status")):
            value = _query_value(query, name)
            if value is not None:
                clauses.append(f"{column}=?")
                values.append(value)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        rows = self._paper_rows(
            "SELECT reservation_id,proposal_id,desk,instrument,exposure_group,idea_id,quantity,reserved_cents,fee_cents,state,created_at,expiry_at "
            f"FROM reservations{where} ORDER BY created_at DESC,reservation_id LIMIT ?", tuple(values + [limit]))
        return [_project(row, _ORDER_FIELDS) for row in rows]

    def trades(self, query: Mapping[str, list[str]]) -> list[dict[str, Any]]:
        limit = _limit(query)
        clauses, values = [], []
        for column, name in (("desk", "desk"), ("instrument", "symbol")):
            value = _query_value(query, name)
            if value is not None:
                clauses.append(f"{column}=?")
                values.append(value)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        rows = self._paper_rows(
            "SELECT fill_id,reservation_id,position_id,proposal_id,desk,instrument,exposure_group,idea_id,quantity,total_cost_cents,fee_cents,filled_at,provenance "
            f"FROM fills{where} ORDER BY filled_at DESC,fill_id LIMIT ?", tuple(values + [limit]))
        for row in rows:
            row["execution_mode"] = "internal_simulation"
        return [_project(row, _TRADE_FIELDS) for row in rows]

    def event_group(self, query: Mapping[str, list[str]], event_types: tuple[str, ...]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        remaining = _limit(query)
        for event_type in event_types:
            for row in self.store.list_events(desk=_query_value(query, "desk"), event_type=event_type,
                                              since=_query_value(query, "since"),
                                              until=_query_value(query, "until"), limit=remaining):
                rows.append(_project(row, _EVENT_FIELDS))
            remaining = max(1, remaining - len(rows))
            if len(rows) >= _limit(query):
                break
        rows.sort(key=lambda item: (item.get("timestamp", ""), item.get("sequence", 0)), reverse=True)
        return rows[:_limit(query)]

    def health(self) -> dict[str, Any]:
        method = getattr(self.store, "list_system_health", None)
        rows = method() if callable(method) else self._rows("system_health", order="component")
        components = [_project(row, _HEALTH_FIELDS) for row in rows]
        degraded = [
            row["component"] for row in components
            if row.get("status") not in {"healthy", "ready", "active", "ok"}
        ]
        integrity_ok = bool(self.store.integrity_check())
        return {
            "ok": integrity_ok and not degraded,
            "integrity_ok": integrity_ok,
            "degraded_components": degraded,
            "components": components,
        }

    def reports(self, query: Mapping[str, list[str]]) -> list[dict[str, Any]]:
        limit = _limit(query)
        report_type = _query_value(query, "report_type")
        method = getattr(self.store, "list_reports", None)
        if callable(method):
            kwargs: dict[str, Any] = {"limit": limit}
            if report_type is not None:
                kwargs["report_type"] = report_type
            rows = method(**kwargs)
        else:
            rows = self._rows(
                "reports", order="generated_at DESC, report_id", limit=limit,
                where="report_type=?" if report_type else "", values=(report_type,) if report_type else (),
            )
        return [_project(row, _REPORT_FIELDS) for row in rows]

    def latest_report(self, query: Mapping[str, list[str]]) -> dict[str, Any] | None:
        report_type = _query_value(query, "report_type")
        method = getattr(self.store, "get_latest_report", None) or getattr(self.store, "latest_report", None)
        if callable(method):
            row = method(report_type=report_type) if report_type else method()
            return None if row is None else _project(row, _REPORT_FIELDS)
        rows = self._rows(
            "reports", order="generated_at DESC, report_id", limit=1,
            where="report_type=?" if report_type else "", values=(report_type,) if report_type else (),
        )
        return None if not rows else _project(rows[0], _REPORT_FIELDS)

    def tasks(self, query: Mapping[str, list[str]]) -> list[dict[str, Any]]:
        limit = _limit(query)
        status = _query_value(query, "status")
        method = getattr(self.store, "list_tasks", None)
        if callable(method):
            kwargs: dict[str, Any] = {"limit": limit}
            if status is not None:
                kwargs["status"] = status
            rows = method(**kwargs)
        else:
            rows = self._rows(
                "tasks", order="updated_at DESC, task_id", limit=limit,
                where="status=?" if status else "", values=(status,) if status else (),
            )
        return [_project(row, _TASK_FIELDS) for row in rows]

    def task(self, task_id: str) -> dict[str, Any] | None:
        if not _TASK_ID.fullmatch(task_id):
            raise APIError(400, "invalid task id")
        method = getattr(self.store, "get_task", None)
        if callable(method):
            row = method(task_id)
        else:
            rows = self._rows("tasks", order="task_id", limit=1, where="task_id=?", values=(task_id,))
            row = rows[0] if rows else None
        return None if row is None else _project(row, _TASK_FIELDS)

    def firm_status(self) -> dict[str, Any]:
        desks = self.desks()
        health = self.health()
        open_incidents = self._rows(
            "incidents", order="updated_at DESC, incident_id", where="status<>?", values=("resolved",),
        )
        critical = sum(1 for row in open_incidents if row.get("severity") == "critical")
        degraded_desks = [row["desk"] for row in desks if row.get("status") not in {"healthy", "ready", "active"}]
        return _clean({
            "ok": health["ok"] and critical == 0,
            "desk_count": len(desks),
            "degraded_desks": degraded_desks,
            "health_component_count": len(health["components"]),
            "open_incident_count": len(open_incidents),
            "critical_incident_count": critical,
        })


def make_handler(store: ControlPlaneStore, bearer_token: str | None = None,
                 paper_ledger: str | os.PathLike[str] | None = None) -> type[BaseHTTPRequestHandler]:
    """Create a bounded handler class without placing secrets on the class object."""
    view = ControlPlaneView(store, paper_ledger)
    expected_token = bearer_token

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def setup(self) -> None:
            self.request.settimeout(5)
            super().setup()

        def log_message(self, *_: Any) -> None:
            pass  # Never persist attacker-controlled paths, headers, or credentials.

        def _authorized(self) -> bool:
            if expected_token is None:
                return True
            header = self.headers.get("Authorization", "")
            prefix = "Bearer "
            if not header.startswith(prefix):
                return False
            return hmac.compare_digest(header[len(prefix):], expected_token)

        def _send(self, status: int, payload: Mapping[str, Any]) -> None:
            body = (json.dumps(_clean(payload), sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()
            if len(body) > MAX_RESPONSE_BYTES:
                status = 507
                body = b'{"error":"response exceeds size limit"}\n'
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def _content_length(self) -> int:
            raw = self.headers.get("Content-Length")
            if raw is None:
                return 0
            try:
                value = int(raw)
            except ValueError as exc:
                raise APIError(400, "invalid content length") from exc
            if value < 0:
                raise APIError(400, "invalid content length")
            if value > MAX_BODY_BYTES:
                raise APIError(413, "request body exceeds size limit")
            return value

        def _validate_headers(self) -> None:
            if len(self.headers) > MAX_HEADER_COUNT:
                raise APIError(431, "too many request headers")
            total = sum(len(key.encode()) + len(value.encode()) + 4 for key, value in self.headers.items())
            if total > MAX_HEADER_BYTES:
                raise APIError(431, "request headers exceed size limit")
            if self.headers.get("Transfer-Encoding") is not None:
                raise APIError(400, "transfer encoding is not supported")

        def do_GET(self) -> None:
            try:
                if len(self.path.encode("utf-8", errors="replace")) > MAX_PATH_BYTES:
                    raise APIError(414, "request target exceeds size limit")
                self._validate_headers()
                self._content_length()
                if not self._authorized():
                    raise APIError(401, "authentication required")
                parsed = urlsplit(self.path)
                try:
                    query = (parse_qs(parsed.query, keep_blank_values=True, strict_parsing=True,
                                      max_num_fields=20) if parsed.query else {})
                except ValueError as exc:
                    raise APIError(400, "invalid query") from exc
                path = unquote(parsed.path)
                if path == "/firm/status":
                    data: Any = view.firm_status()
                elif path == "/desks":
                    data = view.desks()
                elif path == "/activity":
                    data = view.activity(query)
                elif path == "/positions":
                    data = view.positions(query)
                elif path.startswith("/positions/") and path.count("/") == 2:
                    data = view.position(path[len("/positions/"):])
                    if data is None:
                        raise APIError(404, "position not found")
                elif path == "/trades":
                    data = view.trades(query)
                elif path == "/orders":
                    data = view.orders(query)
                elif path == "/signals":
                    data = view.event_group(query, ("SIGNAL_GENERATED", "SIGNAL_REJECTED"))
                elif path == "/research":
                    data = view.event_group(query, ("RESEARCH_COMPLETED",))
                elif path == "/health":
                    data = view.health()
                elif path == "/reports":
                    data = view.reports(query)
                elif path == "/reports/latest":
                    data = view.latest_report(query)
                elif path == "/tasks":
                    data = view.tasks(query)
                elif path.startswith("/tasks/") and path.count("/") == 2:
                    data = view.task(path[len("/tasks/"):])
                    if data is None:
                        raise APIError(404, "task not found")
                else:
                    raise APIError(404, "not found")
                self._send(200, {"data": data})
            except APIError as exc:
                headers = getattr(self, "_headers_buffer", None)
                if not headers:
                    self._send(exc.status, {"error": exc.message})
            except (ControlPlaneStoreError, ValueError):
                self._send(400, {"error": "invalid request"})
            except Exception:
                self._send(500, {"error": "control plane unavailable"})

        def do_POST(self) -> None:
            try:
                self._validate_headers()
                self._content_length()
                if not self._authorized():
                    raise APIError(401, "authentication required")
                raise APIError(405, "read-only API")
            except APIError as exc:
                self._send(exc.status, {"error": exc.message})

        do_PUT = do_POST
        do_PATCH = do_POST
        do_DELETE = do_POST

    return Handler


class BoundedControlPlaneServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = 16

    def get_request(self) -> tuple[socket.socket, Any]:
        connection, address = super().get_request()
        connection.settimeout(5)
        return connection, address


def create_server(
    store: ControlPlaneStore,
    *,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    bearer_token: str | None = None,
    paper_ledger: str | os.PathLike[str] | None = None,
) -> BoundedControlPlaneServer:
    """Create the API server, refusing unauthenticated non-loopback binds."""
    if not isinstance(port, int) or isinstance(port, bool) or not 0 <= port <= 65_535:
        raise ValueError("port must be between 0 and 65535")
    if bearer_token is not None and (not isinstance(bearer_token, str) or not 32 <= len(bearer_token) <= 4096):
        raise ValueError("bearer token must contain at least 32 characters")
    if not _is_loopback(host) and bearer_token is None:
        raise ValueError("non-loopback binding requires bearer authentication")
    return BoundedControlPlaneServer((host, port), make_handler(store, bearer_token, paper_ledger))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--bearer-token-env", default="NEOROME_CONTROL_API_TOKEN")
    parser.add_argument("--paper-ledger")
    args = parser.parse_args()
    token = os.environ.get(args.bearer_token_env)
    with ControlPlaneStore(args.database) as store:
        with create_server(store, host=args.host, port=args.port, bearer_token=token,
                           paper_ledger=args.paper_ledger) as server:
            server.serve_forever()


if __name__ == "__main__":
    main()
