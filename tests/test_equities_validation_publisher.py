from __future__ import annotations

from datetime import datetime, timedelta, timezone
import csv
import hashlib
import json
from pathlib import Path

from neorome.equities_validation_publisher import MINIMUM_SESSIONS, publish


UTC = timezone.utc


def _record(root: Path, *, status: str = "abstained", symbol: str = "SPY", bars=None) -> Path:
    run_dir = root / "runs"
    run_dir.mkdir(parents=True)
    if bars is None:
        start = datetime(2025, 1, 2, 14, 30, tzinfo=UTC)
        bars = []
        for number in range(MINIMUM_SESSIONS):
            timestamp = start + timedelta(days=number)
            price = 100 + number / 10
            bars.append({"timestamp": timestamp.isoformat().replace("+00:00", "Z"), "o": str(price), "h": str(price + 1), "l": str(price - 1), "c": str(price + 0.5), "volume": "1000000"})
    payload = {
        "status": status,
        "collected_at": "2026-09-23T20:00:00Z",
        "source_kind": "observed",
        "execution_enabled": False,
        "broker_orders_sent": 0,
        "bundle": {"source_kind": "observed", "observation": {"source_kind": "observed", "symbol": symbol, "feed": "iex", "broker_orders": 0, "execution_enabled": False, "bars": bars}},
    }
    target = run_dir / "2026-09-23.json"
    target.write_text(json.dumps(payload), encoding="utf-8")
    return target


def test_publishes_exact_observed_rows_and_hash_bound_metadata(tmp_path: Path):
    daily = tmp_path / "daily"
    validation = tmp_path / "validation"
    _record(daily)

    result = publish(daily_state=daily, validation_state=validation)

    assert result["status"] == "published"
    input_path = validation / "inbox" / "equities" / "input.csv"
    current_path = validation / "inbox" / "equities" / "current.json"
    input_raw = input_path.read_bytes()
    current = json.loads(current_path.read_text(encoding="utf-8"))
    assert current["input"] == "input.csv"
    assert current["input_sha256"] == hashlib.sha256(input_raw).hexdigest()
    rows = list(csv.DictReader(input_raw.decode("utf-8").splitlines()))
    assert len(rows) == MINIMUM_SESSIONS
    assert rows[0]["symbol"] == "SPY"
    assert rows[-1]["close"] == "130.4"
    assert current["validator"] == "desks.equities.walkforward"
    assert current["config"]["strategy"] == "overnight_monthly_v1"
    assert current["config"]["development"][0] < current["config"]["validation"][0] < current["config"]["holdout"][0]


def test_failed_capture_clears_stale_publication_without_creating_rows(tmp_path: Path):
    daily = tmp_path / "daily"
    validation = tmp_path / "validation"
    _record(daily)
    assert publish(daily_state=daily, validation_state=validation)["status"] == "published"
    current = validation / "inbox" / "equities" / "current.json"
    current.write_text('{"stale":true}\n', encoding="utf-8")
    (daily / "runs" / "2026-09-23.json").write_text(json.dumps({"status": "fetch_or_validation_failed"}), encoding="utf-8")

    result = publish(daily_state=daily, validation_state=validation)

    assert result["status"] == "data_not_ready"
    assert "source_not_ready" in result["blockers"]
    assert not current.exists()
    assert (validation / "inbox" / "equities" / "input.csv").exists()


def test_rejects_synthetic_or_incomplete_source(tmp_path: Path):
    daily = tmp_path / "daily"
    validation = tmp_path / "validation"
    _record(daily, status="completed")
    record = json.loads((daily / "runs" / "2026-09-23.json").read_text(encoding="utf-8"))
    record["source_kind"] = "synthetic"
    (daily / "runs" / "2026-09-23.json").write_text(json.dumps(record), encoding="utf-8")
    result = publish(daily_state=daily, validation_state=validation)
    assert result["status"] == "data_not_ready"
    assert "source_not_observed" in result["blockers"]
    assert not (validation / "inbox" / "equities" / "input.csv").exists()


def test_rejects_short_history_and_bad_ohlc(tmp_path: Path):
    daily = tmp_path / "daily"
    validation = tmp_path / "validation"
    _record(daily, bars=[{"timestamp": "2026-01-01T14:30:00Z", "o": "100", "h": "99", "l": "98", "c": "100", "volume": "1"}])
    result = publish(daily_state=daily, validation_state=validation)
    assert result["status"] == "data_not_ready"
    assert "insufficient_history" in result["blockers"]

