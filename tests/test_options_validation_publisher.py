from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path

from neorome.options_validation_publisher import ARTIFACT_KIND, publish


UTC = timezone.utc
NOW = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(value: object) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def quote(*, day: datetime, strike: str, right: str, delta: str, bid: str, ask: str) -> dict[str, object]:
    expiry = "2026-12-18"
    encoded = f"{int(float(strike) * 1000):08d}"
    return {
        "symbol": f"SPY261218{right}{encoded}",
        "underlying": "SPY",
        "expiry": expiry,
        "right": right,
        "strike": strike,
        "bid": bid,
        "ask": ask,
        "bid_size": 5,
        "ask_size": 5,
        "delta": delta,
        "timestamp": (day + timedelta(minutes=2)).isoformat().replace("+00:00", "Z"),
        "source_kind": "historical_executable",
        "executable": True,
    }


def rows(count: int = 260) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    start = datetime(2025, 1, 1, 0, 0, tzinfo=UTC)
    for index in range(count):
        day = start + timedelta(days=index)
        long = quote(day=day, strike="500", right="C", delta="0.50", bid="10", ask="11")
        short = quote(day=day, strike="505", right="C", delta="0.25", bid="5", ask="6")
        result.append({
            "session_date": day.date().isoformat(),
            "signal_timestamp": (day + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
            "close": "500",
            "direction": "up",
            "chain_metadata": {"complete": True, "available_expirations": ["2026-12-18"]},
            "entry_quotes": {"long": long, "short": short},
            "exit_quotes": {"long": long, "short": short},
        })
    return result


def source_archive(*, count: int = 260, source_kind: str = "historical_executable") -> dict[str, object]:
    body: dict[str, object] = {
        "schema_version": 1,
        "artifact_kind": ARTIFACT_KIND,
        "status": "captured",
        "provider": "historical-options-entitlement",
        "boundary_at": "2025-09-18T00:00:00Z",
        "data_admission": {
            "source_kind": source_kind,
            "provider": "historical-options-entitlement",
            "entitlement_confirmed": True,
            "quotes_complete": True,
            "provider_timestamps": True,
        },
        "rows": rows(count),
        "cost_model": {"fee_per_contract": "0", "slippage_bps": "25", "share_fee_per_share": "0", "benchmark_capital": "300"},
        "source_evidence": [{"kind": "retained-provider-archive", "sha256": "a" * 64}],
    }
    body["artifact_sha256"] = digest(body)
    return body


def stage_source(root: Path, source: dict[str, object]) -> None:
    date = "2025-09-18"
    filename = "options-validation-20250918.json"
    directory = root / date
    directory.mkdir(parents=True, exist_ok=True)
    (directory / filename).write_bytes(canonical(source) + b"\n")
    (root / "current.json").write_bytes(canonical({
        "schema_version": 1,
        "date": date,
        "artifacts": {"options-validation.json": {"file": filename, "sha256": digest(source)}},
    }) + b"\n")


def test_publishes_only_hash_bound_historical_executable_input(tmp_path: Path) -> None:
    evidence = tmp_path / "evidence"
    stage_source(evidence, source_archive())
    inbox = tmp_path / "inbox"
    result = publish(evidence_root=evidence, inbox_root=inbox, now=NOW)

    assert result["status"] == "published"
    assert result["network_calls"] == 0
    payload = json.loads((inbox / "input.json").read_text())
    pointer = json.loads((inbox / "current.json").read_text())
    assert payload["data_admission"]["source_kind"] == "historical_executable"
    assert payload["data_admission"]["source_artifact_sha256"] == result["source_artifact_sha256"]
    assert pointer["input"] == "input.json"
    assert pointer["input_sha256"] == hashlib.sha256((inbox / "input.json").read_bytes()).hexdigest()
    assert pointer["validator"] == "desks.options.walk_forward"
    assert pointer["config"]["allow_synthetic"] is False


def test_missing_source_is_an_honest_abstention(tmp_path: Path) -> None:
    result = publish(evidence_root=tmp_path / "evidence", inbox_root=tmp_path / "inbox", now=NOW)
    assert result["status"] == "data_not_ready"
    assert result["blockers"] == ["source_pointer_missing"]
    assert not (tmp_path / "inbox" / "input.json").exists()


def test_synthetic_archive_is_rejected(tmp_path: Path) -> None:
    evidence = tmp_path / "evidence"
    stage_source(evidence, source_archive(source_kind="retained_executable_fixture"))
    result = publish(evidence_root=evidence, inbox_root=tmp_path / "inbox", now=NOW)
    assert result["status"] == "data_not_ready"
    assert result["blockers"] == ["historical_entitlement_missing"]


def test_short_history_is_not_published(tmp_path: Path) -> None:
    evidence = tmp_path / "evidence"
    stage_source(evidence, source_archive(count=259))
    result = publish(evidence_root=evidence, inbox_root=tmp_path / "inbox", now=NOW)
    assert result["status"] == "data_not_ready"
    assert result["blockers"] == ["historical_rows_insufficient"]


def test_zero_depth_is_rejected_before_publication(tmp_path: Path) -> None:
    evidence = tmp_path / "evidence"
    source = source_archive()
    source["rows"][0]["entry_quotes"]["long"]["ask_size"] = 0  # type: ignore[index]
    source["artifact_sha256"] = digest({key: value for key, value in source.items() if key != "artifact_sha256"})
    stage_source(evidence, source)
    result = publish(evidence_root=evidence, inbox_root=tmp_path / "inbox", now=NOW)
    assert result["status"] == "data_not_ready"
    assert result["blockers"] == ["quote_depth_invalid"]


def test_pointer_hash_mismatch_is_rejected(tmp_path: Path) -> None:
    evidence = tmp_path / "evidence"
    source = source_archive()
    stage_source(evidence, source)
    pointer = json.loads((evidence / "current.json").read_text())
    pointer["artifacts"]["options-validation.json"]["sha256"] = "b" * 64
    (evidence / "current.json").write_bytes(canonical(pointer) + b"\n")
    result = publish(evidence_root=evidence, inbox_root=tmp_path / "inbox", now=NOW)
    assert result["status"] == "data_not_ready"
    assert result["blockers"] == ["source_hash_mismatch"]


def test_same_archive_is_idempotent_and_older_archive_cannot_roll_back(tmp_path: Path) -> None:
    evidence = tmp_path / "evidence"
    stage_source(evidence, source_archive())
    inbox = tmp_path / "inbox"
    first = publish(evidence_root=evidence, inbox_root=inbox, now=NOW)
    second = publish(evidence_root=evidence, inbox_root=inbox, now=NOW)
    assert first == second
    (inbox / "input.json").write_bytes((inbox / "input.json").read_bytes())

    newer = source_archive()
    newer["boundary_at"] = "2025-09-19T00:00:00Z"
    newer["artifact_sha256"] = digest({key: value for key, value in newer.items() if key != "artifact_sha256"})
    stage_source(evidence, newer)
    result = publish(evidence_root=evidence, inbox_root=inbox, now=NOW)
    assert result["status"] == "published"
    stage_source(evidence, source_archive())
    rollback = publish(evidence_root=evidence, inbox_root=inbox, now=NOW)
    assert rollback["status"] == "data_not_ready"
    assert rollback["blockers"] == ["boundary_rollback"]
