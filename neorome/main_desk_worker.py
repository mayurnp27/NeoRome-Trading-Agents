"""Safe task-queue bootstrap for a local NeoRome Main Desk workspace.

This worker claims approved specifications and writes a durable handoff file. It
never executes commands or code from a task.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import socket
import uuid
from typing import Any, Mapping, Sequence

from .control_plane_store import ControlPlaneStore


UTC = timezone.utc
PRIORITY = {"critical": 0, "high": 1, "medium": 2, "normal": 2, "low": 3}


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.parent.is_symlink() or path.is_symlink():
        raise ValueError("worker spool symlink refused")
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _eligible(task: Mapping[str, Any], completed: set[str], environment: str) -> bool:
    if task.get("status") not in {"APPROVED", "QUEUED"}:
        return False
    target = task.get("execution_environment")
    if target not in (None, environment):
        return False
    if task.get("blocked_by"):
        return False
    return all(item in completed for item in task.get("dependencies", []))


def poll_once(
    store: ControlPlaneStore,
    *,
    worker_id: str,
    environment: str,
    capabilities: Sequence[str],
    spool: str | os.PathLike[str],
    hostname: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    current = (now or datetime.now(UTC)).astimezone(UTC)
    stamp = current.isoformat(timespec="microseconds").replace("+00:00", "Z")
    host = hostname or socket.gethostname()
    worker = store.get_worker(worker_id)
    active_id = None if worker is None else worker.get("current_task")
    if active_id:
        task = store.get_task(active_id)
        if task and task.get("status") in {"CLAIMED", "IN_PROGRESS"}:
            store.heartbeat_task(
                active_id, lease_token=task["lease_token"], worker_id=worker_id,
                source="neorome.main_desk_worker",
                idempotency_key=f"task-heartbeat:{worker_id}:{active_id}:{stamp}", now=current,
            )
            store.heartbeat_worker(
                worker_id=worker_id, hostname=host, environment=environment, status="busy",
                capabilities=capabilities, current_task=active_id,
                source="neorome.main_desk_worker", idempotency_key=f"worker:{worker_id}:{stamp}", now=current,
            )
            return {"status": "busy", "task_id": active_id}
    tasks = store.list_tasks(limit=1000)
    completed = {item["task_id"] for item in tasks if item["status"] == "COMPLETED"}
    candidates = [item for item in tasks if _eligible(item, completed, environment)]
    candidates.sort(key=lambda item: (PRIORITY.get(str(item.get("priority", "normal")).lower(), 9),
                                      item["created_at"], item["task_id"]))
    if not candidates:
        store.heartbeat_worker(
            worker_id=worker_id, hostname=host, environment=environment, status="online",
            capabilities=capabilities, current_task=None,
            source="neorome.main_desk_worker", idempotency_key=f"worker:{worker_id}:{stamp}", now=current,
        )
        return {"status": "idle", "task_id": None}
    selected = candidates[0]
    claimed = store.claim_task(
        selected["task_id"], worker_id=worker_id, source="neorome.main_desk_worker",
        idempotency_key=f"claim:{worker_id}:{selected['task_id']}:{stamp}", now=current,
    )
    store.heartbeat_worker(
        worker_id=worker_id, hostname=host, environment=environment, status="busy",
        capabilities=capabilities, current_task=claimed["task_id"],
        source="neorome.main_desk_worker", idempotency_key=f"worker:{worker_id}:{stamp}", now=current,
    )
    handoff = {key: value for key, value in claimed.items() if key not in {"lease_token"}}
    handoff["claim_metadata"] = {"worker_id": worker_id, "claimed_at": stamp}
    _atomic_json(Path(spool) / f"{claimed['task_id']}.json", handoff)
    return {"status": "claimed", "task_id": claimed["task_id"]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True, help="Local/private control database path")
    parser.add_argument("--spool", required=True)
    parser.add_argument("--worker-id", default="MAC_MAIN_DESK")
    parser.add_argument("--environment", default="mac_main_desk")
    parser.add_argument("--capability", action="append", default=["research", "engineering"])
    args = parser.parse_args()
    with ControlPlaneStore(args.database) as store:
        result = poll_once(store, worker_id=args.worker_id, environment=args.environment,
                           capabilities=args.capability, spool=args.spool)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
