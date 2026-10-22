from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import unittest

from desks.options.selection import payload_sha256
from neorome.options_shadow_rehearsal import OptionsShadowRehearsal

UTC = timezone.utc
NOW = datetime(2026, 9, 22, 14, 0, tzinfo=UTC)
EXPIRY = "2026-10-30"


def _evidence(identifier: str) -> dict[str, str]:
    return {"evidence_id": identifier, "sha256": "a" * 64, "source": "offline-fixture"}


def _row(symbol: str, right: str, strike: str, delta: str, ask: str, bid: str) -> dict[str, object]:
    return {
        "instrument_id": symbol + "-id", "symbol": symbol, "underlying": "SPY", "expiry": EXPIRY,
        "right": right, "strike": strike, "multiplier": 100, "adjusted": False,
        "bid": bid, "ask": ask, "bid_size": 10, "ask_size": 10, "delta": delta,
        "implied_volatility": "0.20", "timestamp": "2026-09-22T13:59:00+00:00",
    }


def _evidence_fixture() -> tuple[dict, dict, list[dict]]:
    sessions = []
    cursor = NOW.date() - timedelta(days=1)
    while len(sessions) < 250:
        if cursor.weekday() < 5:
            sessions.append({"date": cursor.isoformat(), "open_at": f"{cursor.isoformat()}T13:30:00+00:00",
                             "close_at": f"{cursor.isoformat()}T20:00:00+00:00"})
        cursor -= timedelta(days=1)
    sessions.append({"date": NOW.date().isoformat(), "open_at": f"{NOW.date().isoformat()}T13:30:00+00:00",
                     "close_at": f"{NOW.date().isoformat()}T20:00:00+00:00"})
    sessions.sort(key=lambda item: item["date"])
    prior = [date.fromisoformat(item["date"]) for item in sessions if item["date"] < NOW.date().isoformat()][-200:]
    bars = [{"session_date": item.isoformat(), "timestamp": f"{item.isoformat()}T20:00:00+00:00",
             "o": "100", "h": "101" if index == 199 else "100", "l": "100", "c": "101" if index == 199 else "100",
             "complete": True} for index, item in enumerate(prior)]
    observation = {"source_kind": "observed", "symbol": "SPY", "observed_at": NOW.isoformat(),
                   "bars_complete": True, "bars": bars, "evidence": [_evidence("bars")]}
    context = {
        "symbol": "SPY", "now": NOW.isoformat(), "option_received_at": NOW.isoformat(),
        "sessions": sessions, "session_verified": True,
        "current_session": {"date": NOW.date().isoformat(), "is_open": True},
        "session_calendar_evidence": _evidence("calendar"), "option_evidence": _evidence("options"),
        "chain_metadata": {"symbol": "SPY", "complete": True,
                            "available_expirations": {"C": [EXPIRY], "P": [EXPIRY]}},
        "candidate_universe": {"complete": True, "limit": 100, "next_page_token": "",
                               "right": "C", "expiry": EXPIRY},
    }
    rows = [_row("SPY261030C00765000", "C", "765", "0.50", "0.10", "0.08"),
            _row("SPY261030C00770000", "C", "770", "0.25", "0.05", "0.04")]
    observation["normalized_payload_sha256"] = payload_sha256(observation, hash_field="normalized_payload_sha256")
    context["candidate_rows_sha256"] = payload_sha256(rows)
    context["normalized_payload_sha256"] = payload_sha256(context, hash_field="normalized_payload_sha256")
    return observation, context, rows


def _quote_sources(quotes: dict[str, dict[str, object]], at: datetime) -> dict[str, dict[str, object]]:
    result = {}
    stamp = at.isoformat().replace("+00:00", "Z")
    for symbol, quote in quotes.items():
        clean = {"bid": str(quote["bid"]), "ask": str(quote["ask"]), "timestamp": stamp,
                 "bid_size": quote["bid_size"], "ask_size": quote["ask_size"]}
        material = {"symbol": symbol, "provenance": "synthetic", "provider": "offline-rehearsal",
                    "quote": clean, "observed_at": stamp, "received_at": stamp}
        encoded = json.dumps(material, sort_keys=True, separators=(",", ":"))
        result[symbol] = {**material, "hash": hashlib.sha256(encoded.encode()).hexdigest(),
                          "quote_digest": hashlib.sha256(json.dumps({"symbol": symbol, "quote": clean},
                                                                       sort_keys=True, separators=(",", ":")).encode()).hexdigest()}
    return result


def _run_inputs():
    observation, context, candidates = _evidence_fixture()
    symbols = [row["symbol"] for row in candidates]
    fill = NOW + timedelta(seconds=10)
    reports = [{"report_id": "r-long", "order_id": "o-long", "symbol": symbols[0], "intent": "open",
                "cumulative_quantity": 1, "timestamp": (fill + timedelta(seconds=1)).isoformat()},
               {"report_id": "r-short", "order_id": "o-short", "symbol": symbols[1], "intent": "open",
                "cumulative_quantity": 1, "timestamp": (fill + timedelta(seconds=2)).isoformat()}]
    holdings = {"complete": True, "timestamp": (fill + timedelta(seconds=3)).isoformat(),
                "option_holdings": {symbols[0]: 1, symbols[1]: -1}, "underlying_quantity": 0,
                "open_orders_reconciled": True}
    first = fill + timedelta(seconds=10)
    second = fill + timedelta(seconds=25)
    def bundle(at: datetime):
        quotes = {symbols[0]: {"bid": "4.00", "ask": "4.10", "timestamp": at.isoformat(), "bid_size": 10, "ask_size": 10},
                  symbols[1]: {"bid": "0.01", "ask": "0.02", "timestamp": at.isoformat(), "bid_size": 10, "ask_size": 10}}
        return {"now": at, "quotes": quotes, "quote_sources": _quote_sources(quotes, at)}
    return observation, context, candidates, reports, holdings, [bundle(first), bundle(second)]


class OptionsShadowRehearsalTests(unittest.TestCase):
    def test_complete_lifecycle_restart_and_idempotency(self):
        args = _run_inputs()
        result = OptionsShadowRehearsal().run(observation=args[0], context=args[1], candidates=args[2],
                                              reports=args[3], holdings_snapshot=args[4], exit_marks=args[5])
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["lifecycle_status"], "simulated_closed")
        self.assertEqual(result["profitability_status"], "not_evaluated")
        self.assertFalse(result["profitability_claim"])
        self.assertTrue(result["idempotency"]["reservation_replayed"])
        self.assertTrue(result["idempotency"]["fill_replayed"])
        self.assertEqual(result["restart"]["status"], "closed")

    def test_per_leg_costs_reach_the_reserved_and_filled_spread(self):
        args = _run_inputs()
        result = OptionsShadowRehearsal().run(
            observation=args[0], context=args[1], candidates=args[2],
            reports=args[3], holdings_snapshot=args[4], exit_marks=args[5],
            fee_per_leg="0.25", slippage_per_leg="0.25",
        )
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["selection"]["cost_model"]["total_entry_cost"], "7.00")
        self.assertEqual(result["reservation"]["reserved_cents"], 700)
        self.assertEqual(result["filled"]["fee_cents"], 100)

    def test_incomplete_chain_and_stale_quote_refuse_before_reservation(self):
        observation, context, candidates, reports, holdings, marks = _run_inputs()
        context = deepcopy(context)
        context["chain_metadata"]["complete"] = False
        context["normalized_payload_sha256"] = payload_sha256(context, hash_field="normalized_payload_sha256")
        refused = OptionsShadowRehearsal().run(observation=observation, context=context, candidates=candidates,
                                                reports=reports, holdings_snapshot=holdings, exit_marks=marks)
        self.assertEqual((refused["status"], refused["reason_code"]), ("refused", "invalid_evidence"))

        observation, context, candidates, reports, holdings, marks = _run_inputs()
        candidates = deepcopy(candidates)
        candidates[0]["timestamp"] = "2026-09-22T13:00:00+00:00"
        refused = OptionsShadowRehearsal().run(observation=observation, context=context, candidates=candidates,
                                                reports=reports, holdings_snapshot=holdings, exit_marks=marks)
        self.assertEqual((refused["status"], refused["reason_code"]), ("refused", "invalid_evidence"))

    def test_partial_or_async_holdings_refuse_lifecycle(self):
        observation, context, candidates, reports, holdings, marks = _run_inputs()
        partial = deepcopy(holdings)
        partial["option_holdings"][candidates[1]["symbol"]] = 0
        result = OptionsShadowRehearsal().run(observation=observation, context=context, candidates=candidates,
                                               reports=reports, holdings_snapshot=partial, exit_marks=marks)
        self.assertEqual(result["status"], "refused")
        self.assertEqual(result["reason_code"], "rehearsal_blocked")

    def test_missing_or_stale_exit_quote_refuses_after_entry(self):
        observation, context, candidates, reports, holdings, marks = _run_inputs()
        missing = deepcopy(marks)
        missing[0]["quote_sources"] = {}
        result = OptionsShadowRehearsal().run(observation=observation, context=context, candidates=candidates,
                                               reports=reports, holdings_snapshot=holdings, exit_marks=missing)
        self.assertEqual((result["status"], result["reason_code"]), ("refused", "rehearsal_blocked"))

        observation, context, candidates, reports, holdings, marks = _run_inputs()
        stale = deepcopy(marks)
        stale[0]["now"] = NOW + timedelta(seconds=200)
        result = OptionsShadowRehearsal().run(observation=observation, context=context, candidates=candidates,
                                               reports=reports, holdings_snapshot=holdings, exit_marks=stale)
        self.assertEqual((result["status"], result["reason_code"]), ("refused", "rehearsal_blocked"))

        observation, context, candidates, reports, holdings, marks = _run_inputs()
        async_reports = deepcopy(reports)
        async_reports[0]["timestamp"] = (NOW + timedelta(seconds=9)).isoformat()
        result = OptionsShadowRehearsal().run(observation=observation, context=context, candidates=candidates,
                                               reports=async_reports, holdings_snapshot=holdings, exit_marks=marks)
        self.assertEqual(result["status"], "refused")
        self.assertEqual(result["reason_code"], "rehearsal_blocked")


if __name__ == "__main__":
    unittest.main()
