from datetime import date, datetime, timedelta
from decimal import Decimal
import json
import math

import pytest

from neorome.options_black_scholes import delta_from_iv
from neorome.options_history_archive import ArchiveAssembler, ArchiveBudget, ArchiveConfig, ArchiveError, DatabentoArchiveProvider, candidate_contracts, choose_vertical, select_vertical_from_quotes
from desks.options.walk_forward import _candidate, run_walk_forward


def definitions():
    return [
        {"raw_symbol": "SPY   260930C00500000", "instrument_class": "C", "expiration": "2026-09-30", "strike_price": 500, "ts_event": "2026-09-01T20:00:00Z"},
        {"raw_symbol": "SPY   260930C00510000", "instrument_class": "C", "expiration": "2026-09-30", "strike_price": 510, "ts_event": "2026-09-01T20:00:00Z"},
        {"raw_symbol": "SPY   260930P00500000", "instrument_class": "P", "expiration": "2026-09-30", "strike_price": 500, "ts_event": "2026-09-01T20:00:00Z"},
        {"raw_symbol": "SPY   260930P00490000", "instrument_class": "P", "expiration": "2026-09-30", "strike_price": 490, "ts_event": "2026-09-01T20:00:00Z"},
    ]


def test_vertical_uses_exact_occ_contracts():
    long, short, expiries = choose_vertical(definitions(), session=date(2026, 8, 15), underlying="SPY", spot=Decimal("500"), direction="up")
    assert long["symbol"] == "SPY260930C00500000"
    assert short["symbol"] == "SPY260930C00510000"
    assert long["provider_symbol"] == "SPY   260930C00500000"
    assert short["provider_symbol"] == "SPY   260930C00510000"
    assert expiries == ["2026-09-30"]


def test_stratified_chain_reaches_walk_forward_delta_targets():
    session = date(2026, 8, 15)
    expiry = date(2026, 9, 30)
    spot = Decimal("500")
    definitions_for_chain = [
        {"raw_symbol": f"SPY   260930C{strike * 1000:08d}", "instrument_class": "C", "expiration": expiry.isoformat(), "strike_price": strike, "ts_event": "2026-08-15T20:00:00Z"}
        for strike in range(425, 576)
    ]
    candidates, expiries = candidate_contracts(definitions_for_chain, session=session, underlying="SPY", spot=spot, direction="up")
    assert 20 <= len(candidates) <= 24
    assert any(Decimal(item["strike"]) == spot for item in candidates)
    assert all(Decimal("0.85") <= Decimal(item["strike"]) / spot <= Decimal("1.15") for item in candidates)

    quote_time = "2026-08-15T19:59:00Z"
    as_of = datetime.fromisoformat(quote_time.replace("Z", "+00:00"))
    rate, dividend, volatility = Decimal("0.04"), Decimal("0.01"), Decimal("0.25")

    def normal_cdf(value):
        return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))

    def call_price(strike):
        years = (expiry - as_of.date()).days / 365.0
        d1 = (math.log(float(spot) / strike) + (float(rate - dividend) + 0.5 * float(volatility) ** 2) * years) / (float(volatility) * math.sqrt(years))
        d2 = d1 - float(volatility) * math.sqrt(years)
        return float(spot) * math.exp(-float(dividend) * years) * normal_cdf(d1) - strike * math.exp(-float(rate) * years) * normal_cdf(d2)

    enriched = {}
    for contract in candidates:
        strike = Decimal(contract["strike"])
        theoretical = Decimal(str(call_price(float(strike))))
        enriched[contract["symbol"]] = {
            "symbol": contract["symbol"], "underlying": "SPY", "expiry": contract["expiry"], "right": "C", "strike": contract["strike"],
            "bid": str(max(theoretical - Decimal("0.02"), Decimal("0.01"))), "ask": str(theoretical + Decimal("0.02")),
            "bid_size": 100, "ask_size": 100, "delta": str(delta_from_iv(spot=spot, strike=strike, expiry=expiry, as_of=as_of, rate=rate, dividend_yield=dividend, volatility=volatility, right="C")),
            "timestamp": quote_time, "source_kind": "historical_executable", "executable": True,
        }
    long, short, long_quote, short_quote = select_vertical_from_quotes(candidates, enriched, direction="up")
    assert abs(abs(Decimal(long_quote["delta"])) - Decimal("0.50")) <= Decimal("0.10")
    assert abs(abs(Decimal(short_quote["delta"])) - Decimal("0.25")) <= Decimal("0.10")
    row = {"session_date": session, "signal_timestamp": as_of, "chain_metadata": {"complete": True, "available_expirations": expiries}, "entry_quotes": {"long": long_quote, "short": short_quote}}
    assert isinstance(_candidate(row, direction="up", synthetic=False), tuple)


class Metadata:
    def get_cost(self, **kwargs):
        return 0.01

    def get_billable_size(self, **kwargs):
        return 100


class Timeseries:
    def get_range(self, *, schema, symbols, **kwargs):
        if schema == "definition":
            return definitions()
        symbol_list = symbols if isinstance(symbols, list) else [symbols]
        start = str(kwargs["start"])
        return [{"symbol": symbol, "ts_event": f"{start}T19:59:00Z", "ts_recv": f"{start}T19:59:00.001Z", "bid_px_00": 9.9, "ask_px_00": 10.1, "bid_sz_00": 10, "ask_sz_00": 10} for symbol in symbol_list]


class Client:
    metadata = Metadata()
    timeseries = Timeseries()


def inputs(sessions):
    return {item: {"close": 500 + index, "spot": 500, "timestamp": f"{item.isoformat()}T19:59:00Z", "rate": "0.04", "dividend_yield": "0.01"} for index, item in enumerate(sessions)}


def test_archive_requires_budget_estimate_before_fetch(tmp_path):
    assembler = ArchiveAssembler(DatabentoArchiveProvider(Client()), ArchiveConfig(tmp_path, budget=ArchiveBudget(max_request_cost="0.005", max_total_cost="1")))
    sessions = [date(2026, 1, 25) + timedelta(days=index) for index in range(260)]
    with pytest.raises(ArchiveError, match="per-request"):
        assembler.run(sessions, inputs(sessions))
    state = json.loads((tmp_path / "state.json").read_text())
    assert state["total_cost"] == "0"
    assert len((tmp_path / "manifest.jsonl").read_text().splitlines()) == 200
    assert not (tmp_path / "options-validation.json").exists()


def test_archive_is_resumable_for_context_rows(tmp_path):
    assembler = ArchiveAssembler(DatabentoArchiveProvider(Client()), ArchiveConfig(tmp_path, budget=ArchiveBudget(max_request_cost="0.05", max_total_cost="0.03")))
    sessions = [date(2026, 1, 25) + timedelta(days=index) for index in range(260)]
    archive_inputs = inputs(sessions)
    archive_inputs[sessions[201]]["close"] = 400
    for index in range(202, len(sessions)):
        prior = sum(Decimal(str(archive_inputs[sessions[item]]["close"])) for item in range(index - 200, index)) / Decimal(200)
        archive_inputs[sessions[index]]["close"] = str(prior)
    result = assembler.run(sessions, archive_inputs)
    assert result["status"] == "complete"
    assert result["sessions"] == 260
    assert result["total_cost"] == "0.03"
    assert (tmp_path / "options-validation.json").exists()
    state = json.loads((tmp_path / "state.json").read_text())
    assert len(state["sessions"]) == 260
    manifest_lines = (tmp_path / "manifest.jsonl").read_text().splitlines()
    assert len(manifest_lines) == 260
    assert len(list((tmp_path / "raw").glob("*.json"))) == 3
    # A second run is a no-op and does not append duplicate manifest entries.
    again = assembler.run(sessions, archive_inputs)
    assert again["status"] == "complete"
    assert len((tmp_path / "manifest.jsonl").read_text().splitlines()) == 260


def test_archive_rejects_stale_underlying_inputs(tmp_path):
    assembler = ArchiveAssembler(DatabentoArchiveProvider(Client()), ArchiveConfig(tmp_path, budget=ArchiveBudget(max_request_cost="0.05", max_total_cost="0.03")))
    sessions = [date(2026, 1, 25) + timedelta(days=index) for index in range(260)]
    archive_inputs = inputs(sessions)
    archive_inputs[sessions[201]]["timestamp"] = f"{sessions[201].isoformat()}T18:00:00Z"
    with pytest.raises(ArchiveError, match="timestamp exceed"):
        assembler.run(sessions, archive_inputs)


def test_archive_is_admitted_by_walk_forward(tmp_path):
    assembler = ArchiveAssembler(DatabentoArchiveProvider(Client()), ArchiveConfig(tmp_path, budget=ArchiveBudget(max_request_cost="0.05", max_total_cost="0.03")))
    sessions = [date(2026, 1, 25) + timedelta(days=index) for index in range(260)]
    archive_inputs = inputs(sessions)
    archive_inputs[sessions[201]]["close"] = 400
    for index in range(202, len(sessions)):
        prior = sum(Decimal(str(archive_inputs[sessions[item]]["close"])) for item in range(index - 200, index)) / Decimal(200)
        archive_inputs[sessions[index]]["close"] = str(prior)
    assembler.run(sessions, archive_inputs)
    archive = json.loads((tmp_path / "options-validation.json").read_text())
    report = run_walk_forward(archive)
    assert report["admissible_for_paper"] is True
    assert report["data_admission"]["blockers"] == []
