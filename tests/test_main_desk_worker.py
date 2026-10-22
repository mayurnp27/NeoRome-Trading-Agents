from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest

from neorome.control_plane_store import ControlPlaneStore
from neorome.main_desk_worker import poll_once


NOW = datetime(2026, 9, 22, 20, 0, tzinfo=timezone.utc)


class MainDeskWorkerTests(unittest.TestCase):
    def test_only_approved_task_is_claimed_and_handoff_does_not_leak_lease(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            with ControlPlaneStore(root / "control.sqlite3") as store:
                store.create_task(task_id="NR-1", title="draft", description="wait",
                                  acceptance_criteria=["none"], priority="low", created_by="user",
                                  source="test", idempotency_key="create-1")
                store.create_task(task_id="NR-2", title="approved", description="inspect",
                                  acceptance_criteria=["reviewed"], priority="high", created_by="user",
                                  source="test", idempotency_key="create-2",
                                  execution_environment="mac_main_desk")
                store.approve_task("NR-2", approved_by="user", source="test", idempotency_key="approve-2")
                result = poll_once(store, worker_id="MAC_MAIN_DESK", environment="mac_main_desk",
                                   capabilities=["engineering"], spool=root / "spool", hostname="mac", now=NOW)
                self.assertEqual(result, {"status": "claimed", "task_id": "NR-2"})
                task = store.get_task("NR-2")
                self.assertEqual(task["status"], "CLAIMED")
                handoff = json.loads((root / "spool" / "NR-2.json").read_text())
                self.assertNotIn("lease_token", handoff)
                self.assertEqual(store.get_task("NR-1")["status"], "DRAFT")

    def test_reconnect_does_not_claim_second_task_while_lease_is_active(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            with ControlPlaneStore(root / "control.sqlite3") as store:
                for index in (1, 2):
                    store.create_task(task_id=f"NR-{index}", title="task", description="inspect",
                                      acceptance_criteria=["reviewed"], priority="high", created_by="user",
                                      source="test", idempotency_key=f"create-{index}")
                    store.approve_task(f"NR-{index}", approved_by="user", source="test",
                                       idempotency_key=f"approve-{index}")
                first = poll_once(store, worker_id="MAC_MAIN_DESK", environment="mac_main_desk",
                                  capabilities=["engineering"], spool=root / "spool", hostname="mac", now=NOW)
                second = poll_once(store, worker_id="MAC_MAIN_DESK", environment="mac_main_desk",
                                   capabilities=["engineering"], spool=root / "spool", hostname="mac",
                                   now=NOW + timedelta(seconds=30))
                self.assertEqual(first["status"], "claimed")
                self.assertEqual(second, {"status": "busy", "task_id": first["task_id"]})
                self.assertEqual(sum(task["status"] in {"CLAIMED", "IN_PROGRESS"}
                                     for task in store.list_tasks()), 1)


if __name__ == "__main__":
    unittest.main()
