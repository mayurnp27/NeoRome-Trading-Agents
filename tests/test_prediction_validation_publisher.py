from __future__ import annotations

from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import json
from pathlib import Path

from neorome.prediction_validation_publisher import MINIMUM_EVENTS, publish


UTC = timezone.utc


def _events(count: int = MINIMUM_EVENTS) -> list[dict]:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = []
    for index in range(count):
        decision = start + timedelta(days=index)
        digest = f"{index + 1:064x}"
        rows.append({
            "event_id": f"KXTEST-{index:03d}", "family": "KXTEST", "market_id": f"KXTEST-{index:03d}-YES",
            "terms_sha256": "a" * 64, "decision_at": decision.isoformat().replace("+00:00", "Z"),
            "known_at": (decision - timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
            "quote_known_at": (decision - timedelta(minutes=4)).isoformat().replace("+00:00", "Z"),
            "probability_known_at": (decision - timedelta(minutes=3)).isoformat().replace("+00:00", "Z"),
            "probability": "0.70", "probability_lower_bound": "0.65", "ask_cents": 40,
            "yes_bid_cents": 38, "yes_ask_cents": 40, "settlement_at": (decision + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
            "settlement_finalized": True, "result": "YES" if index % 2 == 0 else "NO",
            "source_hash": digest, "settlement_source_hash": f"{index + 100:064x}",
            "quote_source_kind": "venue_executable",
            "probability_provenance": {"source_kind": "calibrated_forecast", "calibration_scope": "out_of_sample",
                                         "model_version": "weather-cal-v1", "method": "isotonic_forecast",
                                         "source_hash": digest},
            "fee_schedule": {"version": "kalshi-fee-v1", "entry_fee_cents_per_contract": 1,
                              "settlement_fee_cents_per_contract": 0},
        })
    return rows


def _write_archive(root: Path, payload: dict, *, compressed: bool = False) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    path = root / ("events.json.gz" if compressed else "events.json")
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    if compressed:
        path.write_bytes(gzip.compress(raw, mtime=0))
    else:
        path.write_bytes(raw)
    return path


def test_publishes_only_complete_hash_bound_events(tmp_path: Path):
    state = tmp_path / "prediction"
    validation = tmp_path / "validation"
    _write_archive(state / "evidence", {"schema_version": 1, "collected_at": "2026-09-23T20:00:00Z", "events": _events()}, compressed=True)

    result = publish(state=state, validation_state=validation)

    assert result["status"] == "published"
    input_path = validation / "inbox" / "prediction" / "input.json"
    current_path = validation / "inbox" / "prediction" / "current.json"
    raw = input_path.read_bytes()
    current = json.loads(current_path.read_text())
    assert current["input"] == "input.json"
    assert current["input_sha256"] == hashlib.sha256(raw).hexdigest()
    payload = json.loads(raw)
    assert len(payload["events"]) == MINIMUM_EVENTS
    assert len(payload["baseline_records"]) == MINIMUM_EVENTS
    assert current["validator"] == "neorome.prediction_walkforward"
    assert current["config"]["development_events"] == 30
    assert payload["execution_enabled"] is False


def test_public_snapshot_and_weather_archive_do_not_become_validation_events(tmp_path: Path):
    state = tmp_path / "prediction"
    validation = tmp_path / "validation"
    _write_archive(state / "evidence", {"schema_version": 1, "markets": [{"market": {"ticker": "KXHIGHNY-1"}}]})
    weather = tmp_path / "weather"
    _write_archive(weather, {"target_date": "2026-09-20", "labels": [{"status": "finalized", "result": "no"}]})

    result = publish(state=state, archives=[weather], validation_state=validation)

    assert result["status"] == "data_not_ready"
    assert result["blockers"] == ["causal_dataset_missing"]
    assert not (validation / "inbox" / "prediction" / "current.json").exists()


def test_failed_capture_clears_stale_current_and_rejects_midpoint_probability(tmp_path: Path):
    state = tmp_path / "prediction"
    validation = tmp_path / "validation"
    _write_archive(state, {"events": _events()})
    assert publish(state=state, validation_state=validation)["status"] == "published"
    events = _events()
    events[0]["probability_provenance"]["method"] = "venue_midpoint_reference"
    _write_archive(state, {"events": events})

    result = publish(state=state, validation_state=validation)

    assert result["status"] == "data_not_ready"
    assert result["blockers"] == ["midpoint_not_alpha"]
    assert not (validation / "inbox" / "prediction" / "current.json").exists()


def test_rejects_unfinalized_or_insufficient_events(tmp_path: Path):
    state = tmp_path / "prediction"
    validation = tmp_path / "validation"
    events = _events(MINIMUM_EVENTS - 1)
    _write_archive(state, {"events": events})
    assert publish(state=state, validation_state=validation)["blockers"] == ["insufficient_events"]
    events = _events()
    events[0]["settlement_finalized"] = False
    _write_archive(state, {"events": events})
    assert publish(state=state, validation_state=validation)["blockers"] == ["settlement_not_finalized"]

