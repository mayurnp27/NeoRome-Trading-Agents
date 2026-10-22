"""Read-only Databento OPRA historical options data adapter.

This module deliberately separates entitlement probing from data downloads.
The probe uses metadata endpoints only.  A quote request is opt-in and never
places an order.  The default ``cbbo-1m`` schema supplies timestamped,
consolidated NBBO prices and displayed top-of-book sizes; the denser
``cmbp-1`` schema is available when event-level replay is required.

Databento does not publish option Greeks in its OPRA quote records.  Callers
must provide a deterministic, separately archived delta enrichment before a
row can be adapted to ``desks.options.walk_forward``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import argparse
import importlib
import json
import os
from typing import Any, Callable, Iterable, Mapping, Sequence


DATASET = "OPRA.PILLAR"
DEFAULT_SCHEMA = "cbbo-1m"
EVENT_SCHEMA = "cmbp-1"
REQUIRED_FIELDS = frozenset(
    {"ts_recv", "symbol", "bid_px_00", "ask_px_00", "bid_sz_00", "ask_sz_00"}
)


class DatabentoOptionsError(RuntimeError):
    """The provider is not installed, entitled, or returned unsafe data."""


@dataclass(frozen=True)
class DatabentoOptionsConfig:
    """Bounded configuration for one historical request."""

    api_key_env: str = "DATABENTO_API_KEY"
    dataset: str = DATASET
    schema: str = DEFAULT_SCHEMA
    stype_in: str = "raw_symbol"
    max_records: int = 250_000

    def __post_init__(self) -> None:
        if self.dataset != DATASET:
            raise ValueError("options adapter is restricted to OPRA.PILLAR")
        if self.schema not in {DEFAULT_SCHEMA, EVENT_SCHEMA}:
            raise ValueError("options schema must be cbbo-1m or cmbp-1")
        if self.stype_in not in {"raw_symbol", "parent"}:
            raise ValueError("stype_in must be raw_symbol or parent")
        if isinstance(self.max_records, bool) or not 1 <= self.max_records <= 2_000_000:
            raise ValueError("max_records is outside the bounded request limit")


def _api_key(config: DatabentoOptionsConfig) -> str:
    key = os.environ.get(config.api_key_env, "")
    if not key:
        raise DatabentoOptionsError(f"{config.api_key_env} is not set")
    if len(key) > 256 or any(ord(char) < 33 or ord(char) > 126 for char in key):
        raise DatabentoOptionsError("Databento API key contains invalid characters")
    return key


def _client(config: DatabentoOptionsConfig, factory: Callable[[str], Any] | None = None) -> Any:
    try:
        module = importlib.import_module("databento")
    except ImportError as exc:
        raise DatabentoOptionsError("install databento before using the historical adapter") from exc
    constructor = factory or module.Historical
    return constructor(_api_key(config))


def _rows(value: Any) -> list[Mapping[str, Any]]:
    """Normalize SDK DataFrame/DBN/JSON-like results without importing pandas."""
    if isinstance(value, Mapping):
        return [value]
    if hasattr(value, "to_df"):
        frame = value.to_df()
        # Databento places ``ts_recv`` in the DataFrame index. Preserve it as
        # a normal field before converting records or the replay loses its
        # quote timestamp.
        if hasattr(frame, "reset_index"):
            frame = frame.reset_index()
        if hasattr(frame, "to_dict"):
            return list(frame.to_dict(orient="records"))
    if hasattr(value, "to_dict"):
        try:
            converted = value.to_dict(orient="records")
        except TypeError:
            converted = value.to_dict()
        if isinstance(converted, list):
            return converted
    try:
        return [dict(item) if not isinstance(item, Mapping) else item for item in value]
    except TypeError as exc:
        raise DatabentoOptionsError("Databento result is not iterable records") from exc


def _metadata_list(client: Any, method: str, **kwargs: Any) -> Any:
    metadata = getattr(client, "metadata", None)
    if metadata is None or not hasattr(metadata, method):
        raise DatabentoOptionsError(f"Databento client lacks metadata.{method}")
    try:
        return getattr(metadata, method)(**kwargs)
    except Exception as exc:  # provider-specific exception types are not stable
        raise DatabentoOptionsError(f"metadata.{method} failed") from exc


def _metadata_call(client: Any, method: str, **kwargs: Any) -> Any:
    metadata = getattr(client, "metadata", None)
    if metadata is None or not hasattr(metadata, method):
        raise DatabentoOptionsError(f"Databento client lacks metadata.{method}")
    try:
        return getattr(metadata, method)(**kwargs)
    except Exception as exc:  # provider-specific exception types are not stable
        raise DatabentoOptionsError(f"metadata.{method} failed") from exc


def _identifier_set(value: Any, *, field: str) -> set[str]:
    """Accept both current string lists and older object-shaped responses."""
    if not isinstance(value, (list, tuple)):
        raise DatabentoOptionsError(f"metadata.{field} returned an unexpected shape")
    result: set[str] = set()
    for item in value:
        if isinstance(item, str):
            result.add(item)
        elif isinstance(item, Mapping):
            candidate = item.get(field[:-1]) or item.get(field) or item.get("id") or item.get("name")
            if isinstance(candidate, str):
                result.add(candidate)
    return result


def probe_entitlement(
    config: DatabentoOptionsConfig | None = None,
    *,
    client: Any | None = None,
    client_factory: Callable[[str], Any] | None = None,
) -> dict[str, Any]:
    """Check installation, key presence and OPRA metadata without downloading data."""
    config = config or DatabentoOptionsConfig()
    result: dict[str, Any] = {
        "provider": "databento",
        "dataset": config.dataset,
        "schema": config.schema,
        "metadata_only": True,
        "orders_sent": 0,
    }
    try:
        if client is None:
            client = _client(config, client_factory)
        datasets = _metadata_list(client, "list_datasets")
        dataset_ids = _identifier_set(datasets, field="datasets")
        if config.dataset not in dataset_ids:
            raise DatabentoOptionsError("OPRA.PILLAR is not present in the account metadata")
        schemas = _metadata_list(client, "list_schemas", dataset=config.dataset)
        schema_ids = _identifier_set(schemas, field="schemas")
        if config.schema not in schema_ids:
            raise DatabentoOptionsError(f"{config.schema} is not available for OPRA.PILLAR")
        prices = _metadata_list(client, "list_unit_prices", dataset=config.dataset)
        available_range = _metadata_list(client, "get_dataset_range", dataset=config.dataset)
        result.update(
            {
                "status": "entitled",
                "schema_available": True,
                "unit_prices": prices,
                "dataset_range": available_range,
                "greeks_in_feed": False,
                "replay_ready": False,
                "blockers": [
                    "provider does not publish option Greeks in OPRA quote records",
                    "historical contract definitions and deterministic delta enrichment must be archived",
                ],
            }
        )
    except DatabentoOptionsError as exc:
        result.update({"status": "blocked", "replay_ready": False, "blockers": [str(exc)]})
    return result


def estimate_historical_cost(
    symbols: Sequence[str] | str,
    start: str | datetime | date,
    end: str | datetime | date,
    *,
    config: DatabentoOptionsConfig | None = None,
    client: Any | None = None,
    client_factory: Callable[[str], Any] | None = None,
) -> dict[str, Any]:
    """Estimate bytes and cost through provider metadata before downloading."""
    config = config or DatabentoOptionsConfig()
    if isinstance(symbols, str):
        symbols = (symbols,)
    symbols = tuple(symbols)
    if not symbols or len(symbols) > 500:
        raise ValueError("one to 500 option symbols are supported per request")
    if client is None:
        client = _client(config, client_factory)
    params = {
        "dataset": config.dataset,
        "symbols": list(symbols),
        "schema": config.schema,
        "stype_in": config.stype_in,
        "start": start,
        "end": end,
    }
    billable_bytes = _metadata_call(client, "get_billable_size", **params)
    cost = _metadata_call(client, "get_cost", **params)
    if isinstance(billable_bytes, bool) or not isinstance(billable_bytes, (int, float)) or billable_bytes < 0:
        raise DatabentoOptionsError("provider returned an invalid billable byte estimate")
    if isinstance(cost, bool) or not isinstance(cost, (int, float)) or cost < 0:
        raise DatabentoOptionsError("provider returned an invalid cost estimate")
    return {"provider": "databento", "dataset": config.dataset, "schema": config.schema,
            "symbols": list(symbols), "billable_bytes": billable_bytes,
            "estimated_cost_usd": cost, "downloaded": False, "orders_sent": 0}


def fetch_quotes(
    symbols: Sequence[str] | str,
    start: str | datetime | date,
    end: str | datetime | date,
    *,
    config: DatabentoOptionsConfig | None = None,
    client: Any | None = None,
    client_factory: Callable[[str], Any] | None = None,
) -> list[Mapping[str, Any]]:
    """Fetch a bounded, read-only OPRA historical quote slice."""
    config = config or DatabentoOptionsConfig()
    if isinstance(symbols, str):
        symbols = (symbols,)
    symbols = tuple(symbols)
    if not symbols or len(symbols) > 500:
        raise ValueError("one to 500 option symbols are supported per request")
    if client is None:
        client = _client(config, client_factory)
    timeseries = getattr(client, "timeseries", None)
    if timeseries is None or not hasattr(timeseries, "get_range"):
        raise DatabentoOptionsError("Databento client lacks timeseries.get_range")
    try:
        result = timeseries.get_range(
            dataset=config.dataset,
            schema=config.schema,
            stype_in=config.stype_in,
            symbols=list(symbols),
            start=start,
            end=end,
            limit=config.max_records,
        )
    except Exception as exc:
        raise DatabentoOptionsError("historical OPRA request failed") from exc
    rows = _rows(result)
    if len(rows) > config.max_records:
        raise DatabentoOptionsError("provider returned more records than the request limit")
    return rows


def validate_quote_record(record: Mapping[str, Any]) -> None:
    """Fail closed on records that cannot support executable quote replay."""
    missing = sorted(REQUIRED_FIELDS - set(record))
    if missing:
        raise DatabentoOptionsError("quote record missing " + ", ".join(missing))
    for field in ("bid_px_00", "ask_px_00", "bid_sz_00", "ask_sz_00"):
        value = record[field]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
            raise DatabentoOptionsError(f"quote field {field} is not positive")
    if record["bid_px_00"] > record["ask_px_00"]:
        raise DatabentoOptionsError("quote is crossed")
    for field in ("ts_recv",):
        if isinstance(record[field], bool) or not isinstance(record[field], (int, float, str, datetime)):
            raise DatabentoOptionsError(f"quote field {field} is not timestamped")


def to_walk_forward_quote(record: Mapping[str, Any], *, delta: str | float, contract: Mapping[str, Any]) -> dict[str, Any]:
    """Map one validated BBO record to the walk-forward quote contract.

    ``contract`` must come from the same dated OPRA definition archive.  Delta
    is intentionally required as an explicit argument; this adapter never
    invents Greeks from a midpoint or last trade.
    """
    validate_quote_record(record)
    required_contract = {"symbol", "underlying", "expiry", "right", "strike"}
    if not required_contract.issubset(contract):
        raise DatabentoOptionsError("dated OPRA contract definition is incomplete")
    # For CBBO interval snapshots, ts_recv is the capture time of the quote
    # observation. ts_event can be undefined or describe another event type,
    # so it is not suitable as the executable quote timestamp.
    timestamp = record["ts_recv"]
    return {
        "symbol": contract["symbol"],
        "underlying": contract["underlying"],
        "expiry": contract["expiry"],
        "right": contract["right"],
        "strike": str(contract["strike"]),
        "bid": str(record["bid_px_00"]),
        "ask": str(record["ask_px_00"]),
        "bid_size": record["bid_sz_00"],
        "ask_size": record["ask_sz_00"],
        "delta": str(delta),
        "timestamp": timestamp,
        "provider_event_timestamp": record.get("ts_event"),
        "source_kind": "historical_executable",
        "executable": True,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe", action="store_true", help="run the metadata-only entitlement probe")
    args = parser.parse_args(argv)
    if not args.probe:
        parser.error("only --probe is available; quote downloads require an explicit library call")
    result = probe_entitlement()
    print(json.dumps(result, sort_keys=True, default=str))
    return 0 if result.get("status") == "entitled" else 2


if __name__ == "__main__":  # pragma: no cover - exercised by the operator
    raise SystemExit(main())
