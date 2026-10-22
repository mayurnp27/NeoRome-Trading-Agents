"""Read-only MCP stdio interface over NeoRome's bounded control-plane view."""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Mapping

from .control_plane_api import ControlPlaneView
from .control_plane_store import ControlPlaneStore


TOOLS = [
    ("neorome_firm_status", "Current firm readiness and incident summary", {}),
    ("neorome_desks", "Current desk status", {}),
    ("neorome_positions", "Current or historical internal-paper positions", {
        "desk": {"type": "string"}, "symbol": {"type": "string"},
        "status": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 200},
    }),
    ("neorome_trades", "Internal-paper fills", {
        "desk": {"type": "string"}, "symbol": {"type": "string"},
        "limit": {"type": "integer", "minimum": 1, "maximum": 200},
    }),
    ("neorome_orders", "Internal-paper reservations and order state", {
        "desk": {"type": "string"}, "symbol": {"type": "string"},
        "status": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 200},
    }),
    ("neorome_signals", "Generated and rejected signals", {
        "desk": {"type": "string"}, "since": {"type": "string"}, "until": {"type": "string"},
        "limit": {"type": "integer", "minimum": 1, "maximum": 200},
    }),
    ("neorome_research", "Completed research records", {
        "desk": {"type": "string"}, "since": {"type": "string"}, "until": {"type": "string"},
        "limit": {"type": "integer", "minimum": 1, "maximum": 200},
    }),
    ("neorome_activity", "Filtered canonical activity", {
        "desk": {"type": "string"}, "event_type": {"type": "string"},
        "severity": {"type": "string"}, "correlation_id": {"type": "string"},
        "limit": {"type": "integer", "minimum": 1, "maximum": 200},
    }),
    ("neorome_health", "Control-plane and dependency health", {}),
    ("neorome_reports", "Stored reports", {
        "report_type": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 200},
    }),
    ("neorome_latest_report", "Latest stored report", {"report_type": {"type": "string"}}),
    ("neorome_tasks", "Persistent work queue", {
        "status": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 200},
    }),
    ("neorome_task_status", "One task by ID", {"task_id": {"type": "string"}}),
]


def tool_definitions() -> list[dict[str, Any]]:
    return [{"name": name, "description": description,
             "inputSchema": {"type": "object", "properties": properties,
                             "required": ["task_id"] if name == "neorome_task_status" else [],
                             "additionalProperties": False}}
            for name, description, properties in TOOLS]


def _query(arguments: Mapping[str, Any]) -> dict[str, list[str]]:
    return {key: [str(value)] for key, value in arguments.items() if value is not None}


def call_tool(view: ControlPlaneView, name: str, arguments: Mapping[str, Any]) -> Any:
    if name == "neorome_firm_status":
        return view.firm_status()
    if name == "neorome_desks":
        return view.desks()
    if name == "neorome_positions":
        return view.positions(_query(arguments))
    if name == "neorome_trades":
        return view.trades(_query(arguments))
    if name == "neorome_orders":
        return view.orders(_query(arguments))
    if name == "neorome_signals":
        return view.event_group(_query(arguments), ("SIGNAL_GENERATED", "SIGNAL_REJECTED"))
    if name == "neorome_research":
        return view.event_group(_query(arguments), ("RESEARCH_COMPLETED",))
    if name == "neorome_activity":
        return view.activity(_query(arguments))
    if name == "neorome_health":
        return view.health()
    if name == "neorome_reports":
        return view.reports(_query(arguments))
    if name == "neorome_latest_report":
        return view.latest_report(_query(arguments))
    if name == "neorome_tasks":
        return view.tasks(_query(arguments))
    if name == "neorome_task_status":
        task_id = arguments.get("task_id")
        if not isinstance(task_id, str):
            raise ValueError("task_id is required")
        return view.task(task_id)
    raise ValueError("unknown tool")


def dispatch(view: ControlPlaneView, message: Mapping[str, Any]) -> dict[str, Any] | None:
    request_id = message.get("id")
    method = message.get("method")
    if method == "notifications/initialized":
        return None
    try:
        if method == "initialize":
            result = {"protocolVersion": "2025-06-18", "capabilities": {"tools": {}},
                      "serverInfo": {"name": "neorome-control", "version": "1.0.0"}}
        elif method == "tools/list":
            result = {"tools": tool_definitions()}
        elif method == "tools/call":
            params = message.get("params", {})
            value = call_tool(view, params.get("name"), params.get("arguments", {}))
            result = {"content": [{"type": "text", "text": json.dumps(value, sort_keys=True,
                                                                         separators=(",", ":"))}],
                      "structuredContent": value}
        else:
            raise ValueError("method not found")
        return {"jsonrpc": "2.0", "id": request_id, "result": result}
    except (TypeError, ValueError) as exc:
        return {"jsonrpc": "2.0", "id": request_id,
                "error": {"code": -32602, "message": str(exc)}}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    args = parser.parse_args()
    with ControlPlaneStore(args.database) as store:
        view = ControlPlaneView(store)
        for raw in sys.stdin:
            try:
                message = json.loads(raw)
                response = dispatch(view, message)
            except (json.JSONDecodeError, TypeError):
                response = {"jsonrpc": "2.0", "id": None,
                            "error": {"code": -32700, "message": "parse error"}}
            if response is not None:
                sys.stdout.write(json.dumps(response, sort_keys=True, separators=(",", ":")) + "\n")
                sys.stdout.flush()


if __name__ == "__main__":
    main()
