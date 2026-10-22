from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest
import uuid
import json
import sqlite3

from neorome.control_plane_reporting import generate_report, report_slot
from neorome.control_plane_reporting import _store_timestamp
from neorome.control_plane_store import ControlPlaneStore, MAX_JSON_BYTES, NeoRomeEvent


class ControlPlaneReportingTests(unittest.TestCase):
    def test_store_timestamp_preflight_uses_microsecond_precision(self):
        self.assertEqual(
            _store_timestamp("2026-09-22T14:01:00Z"),
            "2026-09-22T14:01:00.000000Z",
        )
        self.assertEqual(
            _store_timestamp("2026-09-22T14:01:00.123456+00:00"),
            "2026-09-22T14:01:00.123456Z",
        )

    def test_dst_safe_slots(self):
        kind, start, end = report_slot(datetime(2026, 7, 1, 14, 5, tzinfo=timezone.utc))
        self.assertEqual(kind, "morning")
        self.assertEqual(end.hour, 14)  # 10:00 EDT
        kind, _, end = report_slot(datetime(2026, 1, 2, 21, 20, tzinfo=timezone.utc))
        self.assertEqual(kind, "close")
        self.assertEqual(end.hour, 21)  # 16:15 EST

    def test_early_manual_snapshot_does_not_suppress_scheduled_report(self):
        with tempfile.TemporaryDirectory() as root:
            with ControlPlaneStore(Path(root) / "control.sqlite3") as store:
                early = datetime(2026, 10, 18, 4, 52, tzinfo=timezone.utc)
                first = generate_report(store, now=early)
                scheduled = datetime(2026, 10, 18, 14, 0, 5, tzinfo=timezone.utc)
                second = generate_report(store, now=scheduled)
                self.assertEqual(first["report_id"], "2026-10-18-morning")
                self.assertEqual(second["report_id"], "2026-10-18-morning-scheduled")
                self.assertEqual(store.get_report(first["report_id"]), first)
                self.assertEqual(generate_report(store, now=scheduled), second)
                self.assertEqual(len(store.list_reports()), 2)

    def test_report_persists_and_repeat_returns_same_record(self):
        with tempfile.TemporaryDirectory() as root:
            with ControlPlaneStore(Path(root) / "control.sqlite3") as store:
                now = datetime(2026, 9, 22, 14, 1, tzinfo=timezone.utc)
                first = generate_report(store, now=now, paper_ledger=str(Path(root) / "missing.sqlite3"))
                again = generate_report(store, now=now, paper_ledger=None)
                self.assertEqual(first["report_id"], "2026-09-22-morning")
                self.assertEqual(first, again)
                self.assertEqual(len(store.list_reports()), 1)
                self.assertIn("Paper ledger unavailable", first["human_readable"])

    def test_large_unicode_sources_are_compacted_with_exact_coverage_and_references(self):
        with tempfile.TemporaryDirectory() as root:
            with ControlPlaneStore(Path(root) / "control.sqlite3") as store:
                now = datetime(2026, 9, 22, 14, 1, tzinfo=timezone.utc)
                long_text = "風" * 1200
                for index in range(800):
                    store.emit_event(NeoRomeEvent.create(
                        event_id=str(uuid.uuid4()), timestamp=now, desk="equities",
                        event_type="MARKET_OBSERVATION", severity="info",
                        summary=f"observation {index} {long_text}", source="test",
                        data={"detail": long_text},
                    ))
                for index in range(12):
                    store.record_incident(NeoRomeEvent.create(
                        event_id=str(uuid.uuid4()), timestamp=now, desk="system",
                        event_type="SYSTEM_ERROR", severity="warning",
                        summary=f"incident {index} {long_text}", source="test",
                        data={"incident_id": f"INC-{index}", "status": "open",
                              "details": {"evidence": long_text}},
                    ))
                for index in range(20):
                    store.create_task(
                        task_id=f"NR-{index}", title=f"Task {index}",
                        description=long_text * 10, acceptance_criteria=[long_text],
                        priority="normal", created_by="test", source="test",
                        idempotency_key=f"task-{index}", now=now,
                    )

                result = generate_report(store, now=now)
                structured = result["structured"]
                serialized = json.dumps(
                    {"report_id": result["report_id"], "report_type": result["report_type"],
                     "period_start": result["period_start"], "period_end": result["period_end"],
                     "structured": structured, "human_readable": result["human_readable"]},
                    sort_keys=True, separators=(",", ":"), allow_nan=False,
                ).encode("utf-8")
                self.assertLessEqual(len(serialized), MAX_JSON_BYTES)
                self.assertEqual(structured["coverage"]["activity"]["total"], 832)
                self.assertEqual(structured["coverage"]["incidents"]["total"], 12)
                self.assertEqual(structured["coverage"]["outstanding_tasks"]["total"], 20)
                self.assertEqual(structured["firm_status"], "degraded")
                self.assertFalse(structured["coverage"]["activity"]["complete"])
                self.assertIn("report_byte_budget", structured["coverage"]["activity"]["reasons"])
                self.assertTrue(all("reference" in item for item in structured["activity"]))
                self.assertTrue(all("details_available" in item for item in structured["incidents"]))
                self.assertTrue(all("reference" in item for item in structured["outstanding_tasks"]))
                self.assertIn("Coverage is partial", result["human_readable"])
                self.assertTrue(all(len(item["summary"]) <= 240 for item in structured["activity"]))

    def test_metadata_snapshot_caps_rows_without_loading_source_json(self):
        with tempfile.TemporaryDirectory() as root:
            with ControlPlaneStore(Path(root) / "control.sqlite3") as store:
                now = datetime(2026, 9, 22, 14, 1, tzinfo=timezone.utc)
                store.emit_event(NeoRomeEvent.create(
                    event_id=str(uuid.uuid4()), timestamp=now, desk="system",
                    event_type="MARKET_OBSERVATION", severity="info", summary="large payload",
                    source="test", data={"payload": "x" * 800_000},
                ))
                for index in range(1001):
                    store.create_task(
                        task_id=f"NR-{index:04d}", title=f"Task {index}",
                        description=("task detail " * 3000 if index == 0 else "brief detail"),
                        acceptance_criteria=["bounded projection"], priority="normal",
                        created_by="test", source="test", idempotency_key=f"task-{index}", now=now,
                    )

                def forbidden_listing(*_args, **_kwargs):
                    raise AssertionError("full-row list API must not be used by report generation")

                store.list_events = forbidden_listing
                store.list_incidents = forbidden_listing
                store.list_tasks = forbidden_listing
                result = generate_report(store, now=now)
                coverage = result["structured"]["coverage"]
                self.assertEqual(coverage["activity"]["total"], 1002)
                self.assertEqual(coverage["activity"]["shown"], 1000)
                self.assertIn("source_listing_limit", coverage["activity"]["reasons"])
                self.assertEqual(coverage["outstanding_tasks"]["total"], 1001)
                self.assertEqual(coverage["outstanding_tasks"]["shown"], 1000)
                self.assertIn("source_listing_limit", coverage["outstanding_tasks"]["reasons"])

    def test_paper_positions_are_capped_with_exact_count_and_financial_aggregates(self):
        with tempfile.TemporaryDirectory() as root:
            paper_path = Path(root) / "paper.sqlite3"
            with sqlite3.connect(paper_path) as connection:
                connection.executescript("""
                    CREATE TABLE settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                    CREATE TABLE accounts(desk TEXT, initial_cash_cents INTEGER,
                        available_cash_cents INTEGER, reserved_cash_cents INTEGER);
                    CREATE TABLE positions(position_id TEXT, desk TEXT, instrument TEXT,
                        exposure_group TEXT, idea_id TEXT, quantity TEXT, basis_cents INTEGER,
                        state TEXT, opened_at TEXT);
                    CREATE TABLE closes(realized_pnl_cents INTEGER);
                    CREATE TABLE audit_events(event_id TEXT);
                    INSERT INTO settings VALUES ('ledger_marker','neorome-paper-ledger-v1');
                    INSERT INTO accounts VALUES ('equities',30000,27500,2500);
                    INSERT INTO closes VALUES (1234);
                """)
                connection.executemany(
                    "INSERT INTO positions VALUES (?,?,?,?,?,?,?,?,?)",
                    ((f"POS-{index:04d}", "equities", "SPY", "index", "idea-1",
                      "1", 1000, "open", f"2026-09-22T12:{index % 60:02d}:00Z")
                     for index in range(1001)),
                )

            with ControlPlaneStore(Path(root) / "control.sqlite3") as store:
                now = datetime(2026, 9, 22, 14, 1, tzinfo=timezone.utc)
                result = generate_report(store, now=now, paper_ledger=str(paper_path))
                paper = result["structured"]["paper"]
                self.assertEqual(paper["position_coverage"]["total"], 1001)
                self.assertEqual(paper["position_coverage"]["shown"], 1000)
                self.assertIn("source_listing_limit", paper["position_coverage"]["reasons"])
                self.assertEqual(paper["balances"][0]["available_cash_cents"], 27500)
                self.assertEqual(paper["realized_pnl_cents"], 1234)


if __name__ == "__main__":
    unittest.main()
