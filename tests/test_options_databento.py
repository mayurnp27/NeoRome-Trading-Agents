from __future__ import annotations

from datetime import datetime, timezone
import os

import pytest

from neorome.options_databento import (
    DatabentoOptionsConfig,
    DatabentoOptionsError,
    fetch_quotes,
    estimate_historical_cost,
    probe_entitlement,
    to_walk_forward_quote,
    validate_quote_record,
)


class Metadata:
    def list_datasets(self):
        return ["OPRA.PILLAR"]

    def list_schemas(self, *, dataset):
        assert dataset == "OPRA.PILLAR"
        return ["cbbo-1m", "cmbp-1"]

    def list_unit_prices(self, *, dataset):
        return [{"mode": "historical", "unit_prices": {"cbbo-1m": 0.04}}]

    def get_dataset_range(self, *, dataset):
        return {"start": "2013-01-01T00:00:00Z", "end": "2026-09-24T00:00:00Z"}

    def get_billable_size(self, **kwargs):
        assert kwargs["schema"] == "cbbo-1m"
        return 1024

    def get_cost(self, **kwargs):
        assert kwargs["schema"] == "cbbo-1m"
        return 0.01


class Timeseries:
    def get_range(self, **kwargs):
        if kwargs:
            assert kwargs["dataset"] == "OPRA.PILLAR"
            assert kwargs["schema"] == "cbbo-1m"
        return [
            {
                "symbol": "SPY   261030C00765000",
                "ts_event": "2026-09-18T19:59:00Z",
                "ts_recv": "2026-09-18T19:59:00.010Z",
                "bid_px_00": 13.85,
                "ask_px_00": 13.94,
                "bid_sz_00": 100,
                "ask_sz_00": 99,
            }
        ]


class Client:
    metadata = Metadata()
    timeseries = Timeseries()


class IndexedFrame:
    def __init__(self):
        self.reset = False

    def reset_index(self):
        self.reset = True
        return self

    def to_dict(self, *, orient):
        assert orient == "records"
        assert self.reset is True
        return Timeseries().get_range()


class IndexedResult:
    def to_df(self):
        return IndexedFrame()


class IndexedTimeseries:
    def get_range(self, **kwargs):
        return IndexedResult()


class IndexedClient:
    metadata = Metadata()
    timeseries = IndexedTimeseries()


def test_probe_uses_metadata_only_and_marks_missing_greeks():
    result = probe_entitlement(client=Client())
    assert result["status"] == "entitled"
    assert result["metadata_only"] is True
    assert result["orders_sent"] == 0
    assert result["greeks_in_feed"] is False
    assert result["replay_ready"] is False


def test_probe_without_key_is_blocked(monkeypatch):
    monkeypatch.delenv("DATABENTO_API_KEY", raising=False)
    result = probe_entitlement(client_factory=lambda _: Client())
    assert result["status"] == "blocked"
    assert "DATABENTO_API_KEY is not set" in result["blockers"]


def test_fetch_is_bounded_and_read_only():
    rows = fetch_quotes(
        ["SPY   261030C00765000"],
        "2026-09-18T19:59:00Z",
        "2026-09-18T20:00:00Z",
        client=Client(),
    )
    assert len(rows) == 1
    assert rows[0]["bid_sz_00"] == 100


def test_fetch_preserves_dataframe_index_timestamp():
    rows = fetch_quotes(
        ["SPY   261030C00765000"],
        "2026-09-18T19:59:00Z",
        "2026-09-18T20:00:00Z",
        client=IndexedClient(),
    )
    assert rows[0]["ts_recv"] == "2026-09-18T19:59:00.010Z"


def test_cost_estimate_is_metadata_only():
    result = estimate_historical_cost("SPY   261030C00765000", "2026-09-18", "2026-09-19", client=Client())
    assert result["billable_bytes"] == 1024
    assert result["estimated_cost_usd"] == 0.01
    assert result["downloaded"] is False


def test_quote_mapping_requires_explicit_delta_and_preserves_receive_timestamp():
    record = Timeseries().get_range()[0]
    quote = to_walk_forward_quote(
        record,
        delta="0.5085",
        contract={
            "symbol": "SPY261030C00765000",
            "underlying": "SPY",
            "expiry": "2026-10-30",
            "right": "C",
            "strike": "765",
        },
    )
    assert quote["source_kind"] == "historical_executable"
    assert quote["executable"] is True
    assert quote["timestamp"] == "2026-09-18T19:59:00.010Z"
    assert quote["provider_event_timestamp"] == "2026-09-18T19:59:00Z"
    assert quote["bid_size"] == 100


def test_quote_validation_rejects_crossed_or_missing_size():
    record = Timeseries().get_range()[0].copy()
    record.pop("ts_event")
    validate_quote_record(record)
    record["ask_px_00"] = 13.80
    with pytest.raises(DatabentoOptionsError, match="crossed"):
        validate_quote_record(record)
    del record["bid_sz_00"]
    with pytest.raises(DatabentoOptionsError, match="missing"):
        validate_quote_record(record)


def test_config_restricts_dataset_and_schema():
    with pytest.raises(ValueError):
        DatabentoOptionsConfig(dataset="NASDAQ.ITCH")
    with pytest.raises(ValueError):
        DatabentoOptionsConfig(schema="trades")
