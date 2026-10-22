from __future__ import annotations

import hashlib
import json
from pathlib import Path

from neorome.validation_scheduler import (
    DESK_ORDER,
    MANIFEST_SCHEMA,
    ValidationScheduler,
    global_lock,
)


def _manifest(root: Path, *, desks: tuple[str, ...] = DESK_ORDER, run_id: str = "run-1") -> Path:
    entries = {}
    for desk in desks:
        suffix = ".csv" if desk == "equities" else ".json"
        relative = f"{desk}/input{suffix}"
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if desk == "equities":
            path.write_text("symbol,timestamp,open,high,low,close,volume\n", encoding="utf-8")
        else:
            path.write_text("{}", encoding="utf-8")
        config = {}
        if desk == "equities":
            config = {"strategy": "intraday_orb15_v1", "development": ["2026-01-01", "2026-01-02"], "validation": ["2026-01-04", "2026-01-05"], "holdout": ["2026-01-07", "2026-01-08"]}
        elif desk == "prediction":
            config = {"fee_schedule": {"version": "test", "entry_fee_cents_per_contract": 1, "settlement_fee_cents_per_contract": 1}, "development_events": 1, "validation_events": 1, "holdout_events": 1}
        entries[desk] = {"input": relative, "input_sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "boundary_at": "2026-09-23T20:00:00Z", "validator": {"equities": "desks.equities.walkforward", "options": "desks.options.walk_forward", "crypto": "desks.crypto.walkforward", "prediction": "neorome.prediction_walkforward"}[desk], "schedule": "daily", "config": config}
    manifest = {"schema_version": MANIFEST_SCHEMA, "run_id": run_id, "created_at": "2026-09-23T20:01:00Z", "desks": entries}
    path = root / "manifest.json"
    path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    return path


def _fake_invoke(order: list[str], *, fail: str | None = None):
    def invoke(desk: str, _input: Path, _config, output: Path):
        order.append(desk)
        if desk == fail:
            return 17, "", "deliberate validator failure"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps({"status": "completed", "blockers": [], "report": {"desk": desk}}, sort_keys=True), encoding="utf-8")
        return 0, "", ""
    return invoke


def test_dispatch_is_sequential_and_missing_desks_abstain(tmp_path: Path):
    root = tmp_path / "inputs"
    root.mkdir()
    manifest = _manifest(root, desks=("equities", "crypto"))
    order: list[str] = []
    result = ValidationScheduler(tmp_path / "state", invoke=_fake_invoke(order)).run_once(manifest, root)
    assert order == ["equities", "crypto"]
    assert [item.status for item in result.desks] == ["completed", "manifest_missing", "completed", "manifest_missing"]
    assert (tmp_path / "state/reports/run-1/summary.json").exists()


def test_nonblocking_global_lock_does_not_wait_or_dispatch(tmp_path: Path):
    root = tmp_path / "inputs"
    root.mkdir()
    manifest = _manifest(root, desks=("equities",))
    lock_path = tmp_path / "state/validation.lock"
    with global_lock(lock_path) as acquired:
        assert acquired
        result = ValidationScheduler(tmp_path / "state", invoke=_fake_invoke([])).run_once(manifest, root)
    assert result.status == "locked"


def test_invalid_manifest_and_symlink_are_clean_abstentions(tmp_path: Path):
    root = tmp_path / "inputs"
    root.mkdir()
    manifest = _manifest(root, desks=("equities",))
    bad = json.loads(manifest.read_text())
    bad["desks"]["equities"]["input_sha256"] = "0" * 64
    manifest.write_text(json.dumps(bad), encoding="utf-8")
    result = ValidationScheduler(tmp_path / "state", invoke=_fake_invoke([])).run_once(manifest, root)
    assert result.status == "invalid_manifest"
    assert not (tmp_path / "state/reports").exists()


def test_reports_are_immutable_and_idempotent(tmp_path: Path):
    root = tmp_path / "inputs"
    root.mkdir()
    manifest = _manifest(root, desks=("equities",))
    scheduler = ValidationScheduler(tmp_path / "state", invoke=_fake_invoke([]))
    first = scheduler.run_once(manifest, root)
    destination = tmp_path / "state/reports/run-1/equities.json"
    original = destination.read_bytes()
    second = scheduler.run_once(manifest, root)
    assert first.manifest_sha256 == second.manifest_sha256
    assert destination.read_bytes() == original


def test_validator_failure_isolated_and_later_desks_continue(tmp_path: Path):
    root = tmp_path / "inputs"
    root.mkdir()
    manifest = _manifest(root, desks=("equities", "options", "crypto"))
    order: list[str] = []
    result = ValidationScheduler(tmp_path / "state", invoke=_fake_invoke(order, fail="options")).run_once(manifest, root)
    assert order == ["equities", "options", "crypto"]
    assert result.status == "completed_with_failures"
    assert [item.status for item in result.desks] == ["completed", "validator_failed", "completed", "manifest_missing"]
    assert (tmp_path / "state/reports/run-1/crypto.json").exists()
