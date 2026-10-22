from __future__ import annotations

from datetime import datetime, timedelta, timezone
from http.client import HTTPConnection
import json
from pathlib import Path
import tempfile
import threading
import unittest

from neorome.control_plane_api import MAX_BODY_BYTES, MAX_HEADER_BYTES, MAX_LIMIT, create_server
from neorome.control_plane_store import ControlPlaneStore, NeoRomeEvent
from neorome.paper_ledger import PaperLedger, Proposal


UTC = timezone.utc


class RunningAPI:
    def __init__(self, store, token=None, paper_ledger=None):
        self.server = create_server(store, port=0, bearer_token=token, paper_ledger=paper_ledger)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def request(self, method, path, *, headers=None):
        connection = HTTPConnection("127.0.0.1", self.server.server_port, timeout=2)
        connection.request(method, path, headers=headers or {})
        response = connection.getresponse()
        body = response.read()
        connection.close()
        return response.status, json.loads(body)


class ControlPlaneAPITests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = ControlPlaneStore(Path(self.temp.name) / "control.db")
        now = datetime.now(UTC)
        self.store.record_desk_state(NeoRomeEvent.create(
            desk="equities", event_type="DESK_HEARTBEAT", severity="info",
            summary="Equities ready", source="test", timestamp=now,
            data={"status": "active", "details": {
                "mode": "paper", "api_key": "must-not-leak",
                "message": "Bearer also-must-not-leak",
            }},
        ))
        self.store.record_system_health(NeoRomeEvent.create(
            desk="main", event_type="DESK_HEARTBEAT", severity="info",
            summary="Database healthy", source="test", timestamp=now,
            data={"component": "database", "status": "healthy", "details": {"latency_ms": 2}},
        ))
        self.store.create_task(
            task_id="task-1", title="Review feed", description="Review paper feed only",
            acceptance_criteria=["No live orders"], priority="normal", created_by="operator",
            source="test", idempotency_key="create-task-1", now=now,
        )
        self.store.store_report(
            report_id="report-1", report_type="daily", period_start=now - timedelta(days=1),
            period_end=now, structured={"paper_pnl": 1.25, "secret": "must-not-leak"},
            human_readable="Paper report", source="test", idempotency_key="report-1",
            generated_at=now,
        )

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def test_default_bind_is_loopback_and_non_loopback_requires_auth(self):
        server = create_server(self.store, port=0)
        try:
            self.assertEqual(server.server_address[0], "127.0.0.1")
        finally:
            server.server_close()
        with self.assertRaisesRegex(ValueError, "requires bearer"):
            create_server(self.store, host="0.0.0.0", port=0)

    def test_all_read_endpoints_and_secret_redaction(self):
        with RunningAPI(self.store) as api:
            expected = {
                "/firm/status": dict,
                "/desks": list,
                "/activity": list,
                "/positions": list,
                "/trades": list,
                "/orders": list,
                "/signals": list,
                "/research": list,
                "/health": dict,
                "/reports": list,
                "/reports/latest": dict,
                "/tasks": list,
                "/tasks/task-1": dict,
            }
            bodies = []
            for path, kind in expected.items():
                status, body = api.request("GET", path)
                self.assertEqual(status, 200, path)
                self.assertIsInstance(body["data"], kind, path)
                bodies.append(body)
        rendered = json.dumps(bodies)
        self.assertNotIn("must-not-leak", rendered)
        self.assertNotIn("also-must-not-leak", rendered)
        self.assertNotIn("lease_token", rendered)
        self.assertIn("[REDACTED]", rendered)

    def test_authentication_is_constant_boundary_for_authenticated_server(self):
        token = "x" * 32
        with RunningAPI(self.store, token=token) as api:
            status, _ = api.request("GET", "/health")
            self.assertEqual(status, 401)
            status, _ = api.request("GET", "/health", headers={"Authorization": "Bearer wrong"})
            self.assertEqual(status, 401)
            status, body = api.request(
                "GET", "/health", headers={"Authorization": f"Bearer {token}"},
            )
            self.assertEqual(status, 200)
            self.assertTrue(body["data"]["ok"])

    def test_limits_bodies_unknown_tasks_and_writes_are_bounded(self):
        with RunningAPI(self.store) as api:
            status, _ = api.request("GET", f"/activity?limit={MAX_LIMIT + 1}")
            self.assertEqual(status, 400)
            status, _ = api.request(
                "GET", "/health", headers={"Content-Length": str(MAX_BODY_BYTES + 1)},
            )
            self.assertEqual(status, 413)
            status, _ = api.request("GET", "/health", headers={"X-Large": "x" * MAX_HEADER_BYTES})
            self.assertEqual(status, 431)
            status, _ = api.request("GET", "/tasks/missing")
            self.assertEqual(status, 404)
            status, body = api.request("POST", "/tasks")
            self.assertEqual(status, 405)
            self.assertEqual(body["error"], "read-only API")

    def test_paper_positions_orders_and_trades_are_read_only_projections(self):
        path = Path(self.temp.name) / "paper.sqlite3"
        now = datetime.now(UTC)
        ledger = PaperLedger(path, enforce_market_risk=False, clock=lambda: now)
        reservation = ledger.reserve(Proposal(
            proposal_id="api-proposal", desk="crypto", strategy="test", strategy_version="v1",
            source_hash="a" * 64, source_timestamp=now, instrument="BTC/USD",
            exposure_group="crypto:btc", idea_id="api", expiry_at=now + timedelta(minutes=5),
            quantity=1, unit_cost_cents=100, provenance="observed",
        ), now=now)
        fill = ledger.simulate_fill(reservation.reservation_id, fill_id="api-fill",
                                    provenance="observed", filled_at=now)
        ledger.dispose()
        with RunningAPI(self.store, paper_ledger=path) as api:
            for endpoint in ("/positions", "/orders", "/trades"):
                status, body = api.request("GET", endpoint)
                self.assertEqual(status, 200)
                self.assertEqual(len(body["data"]), 1)
            status, body = api.request("GET", f"/positions/{fill.position_id}")
            self.assertEqual(status, 200)
            self.assertEqual(body["data"]["instrument"], "BTC/USD")


if __name__ == "__main__":
    unittest.main()
