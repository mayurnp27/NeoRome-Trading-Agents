import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from desks.options.selection import STRATEGY_VERSION, payload_sha256, select


UTC = timezone.utc
NOW = datetime(2026, 9, 22, 14, 0, tzinfo=UTC)


def _evidence(identifier):
    return {"evidence_id": identifier, "sha256": "a" * 64, "source": "fixture"}


def _fixture(*, equal=False, direction="up", now=NOW, expiry="2026-10-30"):
    sessions = []
    cursor = now.date() - timedelta(days=1)
    while len(sessions) < 250:
        if cursor.weekday() < 5:
            sessions.append({"date": cursor.isoformat(), "open_at": f"{cursor.isoformat()}T13:30:00+00:00", "close_at": f"{cursor.isoformat()}T20:00:00+00:00"})
        cursor -= timedelta(days=1)
    sessions.append({"date": now.astimezone(timezone(timedelta(hours=-4))).date().isoformat(), "open_at": f"{now.date().isoformat()}T13:30:00+00:00", "close_at": f"{now.date().isoformat()}T20:00:00+00:00"})
    sessions.sort(key=lambda row: row["date"])
    bars = []
    prior = [date.fromisoformat(row["date"]) for row in sessions if row["date"] < now.date().isoformat()][-200:]
    for index, item in enumerate(prior):
        close = "100" if equal or index < 199 else ("99" if direction == "down" else "101")
        bars.append({"session_date": item.isoformat(), "timestamp": f"{item.isoformat()}T20:00:00+00:00", "o": close, "h": close, "l": close, "c": close, "complete": True})
    observation = {"source_kind": "observed", "symbol": "SPY", "observed_at": now.isoformat(), "bars_complete": True, "bars": bars, "evidence": [_evidence("bars") ]}
    context = {
        "symbol": "SPY", "now": now.isoformat(), "option_received_at": now.isoformat(), "sessions": sessions, "session_verified": True,
        "current_session": {"date": now.astimezone(timezone(timedelta(hours=-4))).date().isoformat(), "is_open": True}, "session_calendar_evidence": _evidence("calendar"), "option_evidence": _evidence("options"),
        "chain_metadata": {"symbol": "SPY", "complete": True, "available_expirations": {"C": [expiry], "P": [expiry]}},
        "candidate_universe": {"complete": True, "limit": 100, "next_page_token": "", "right": "C" if direction == "up" else "P", "expiry": expiry},
    }
    rows = []
    if direction == "up":
        rows.extend([
            _row("SPY261030C00765000", "C", "765", "0.50", "0.10", "0.08", expiry),
            _row("SPY261030C00770000", "C", "770", "0.25", "0.05", "0.04", expiry),
        ])
    else:
        rows.extend([
            _row("SPY261030P00770000", "P", "770", "-0.50", "0.12", "0.10", expiry),
            _row("SPY261030P00765000", "P", "765", "-0.25", "0.05", "0.04", expiry),
        ])
    observation["normalized_payload_sha256"] = payload_sha256(observation, hash_field="normalized_payload_sha256")
    context["candidate_rows_sha256"] = payload_sha256(rows)
    context["normalized_payload_sha256"] = payload_sha256(context, hash_field="normalized_payload_sha256")
    return observation, context, rows


def _row(symbol, right, strike, delta, ask, bid, expiry):
    return {
        "instrument_id": symbol + "-id", "symbol": symbol, "underlying": "SPY", "expiry": expiry,
        "right": right, "strike": strike, "multiplier": 100, "adjusted": False,
        "bid": bid, "ask": ask, "bid_size": 10, "ask_size": 10, "delta": delta,
        "implied_volatility": "0.20", "timestamp": "2026-09-22T13:59:00+00:00",
    }


def _rehash(observation, context, rows):
    context["candidate_rows_sha256"] = payload_sha256(rows)
    observation["normalized_payload_sha256"] = payload_sha256(observation, hash_field="normalized_payload_sha256")
    context["normalized_payload_sha256"] = payload_sha256(context, hash_field="normalized_payload_sha256")


class SelectionTests(unittest.TestCase):
    def test_uptrend_selects_call_debit_and_is_not_fill(self):
        observation, context, rows = _fixture()
        result = select(observation, context, rows, fee_per_leg="0.10", slippage_per_leg="0.05")
        self.assertEqual(result["status"], "candidate")
        self.assertEqual(result["strategy_version"], STRATEGY_VERSION)
        self.assertEqual(result["direction"], "up")
        self.assertEqual(result["long"]["right"], "C")
        self.assertEqual(result["short"]["strike"], "770")
        self.assertEqual(result["fill_status"], "not_a_fill")
        self.assertFalse(result["profit_approved"])
        self.assertEqual(result["evidence_ids"], ["bars", "calendar", "options"])

    def test_downtrend_selects_put_debit(self):
        observation, context, rows = _fixture(direction="down")
        result = select(observation, context, rows, fee_per_leg="0.10", slippage_per_leg="0.05")
        self.assertEqual(result["status"], "candidate")
        self.assertEqual(result["direction"], "down")
        self.assertEqual(result["long"]["right"], "P")
        self.assertGreater(Decimal(result["long"]["strike"]), Decimal(result["short"]["strike"]))

    def test_equal_trend_abstains(self):
        observation, context, rows = _fixture(equal=True)
        result = select(observation, context, rows, fee_per_leg="0.10", slippage_per_leg="0.05")
        self.assertEqual((result["status"], result["reason_code"]), ("no_trade", "trend_equal"))

    def test_session_cutoff_and_dte_cutoff_abstain(self):
        observation, context, rows = _fixture(now=NOW.replace(hour=13, minute=29))
        result = select(observation, context, rows, fee_per_leg="0.10", slippage_per_leg="0.05")
        self.assertEqual(result["reason_code"], "invalid_evidence")
        observation, context, rows = _fixture()
        for row in rows:
            row["symbol"] = row["symbol"].replace("261030", "261021")
            row["instrument_id"] = row["symbol"] + "-id"
            row["expiry"] = "2026-10-21"
        context["chain_metadata"]["available_expirations"]["C"] = ["2026-10-21"]
        context["chain_metadata"]["available_expirations"]["P"] = ["2026-10-21"]
        context["candidate_universe"]["expiry"] = "2026-10-21"
        _rehash(observation, context, rows)
        result = select(observation, context, rows, fee_per_leg="0.10", slippage_per_leg="0.05")
        self.assertEqual(result["reason_code"], "no_expiry_in_window")

    def test_nearest_expiry_and_deterministic_delta_tie(self):
        observation, context, rows = _fixture()
        rows.extend([
            _row("SPY261106C00764000", "C", "764", "0.50", "0.10", "0.08", "2026-11-06"),
            _row("SPY261106C00769000", "C", "769", "0.25", "0.05", "0.04", "2026-11-06"),
            _row("SPY261106C00763000", "C", "763", "0.50", "0.10", "0.08", "2026-11-06"),
        ])
        rows = rows[2:]
        context["chain_metadata"]["available_expirations"]["C"].append("2026-11-06")
        context["chain_metadata"]["available_expirations"]["P"].append("2026-11-06")
        context["candidate_universe"]["expiry"] = "2026-11-06"
        _rehash(observation, context, rows)
        result = select(observation, context, rows, fee_per_leg="0.10", slippage_per_leg="0.05")
        self.assertEqual(result["expiry"], "2026-11-06")
        self.assertEqual(result["long"]["strike"], "763")

    def test_invalid_identity_stale_depth_and_affordability_abstain(self):
        observation, context, rows = _fixture()
        rows[0]["adjusted"] = True
        result = select(observation, context, rows, fee_per_leg="0.10", slippage_per_leg="0.05")
        self.assertEqual(result["reason_code"], "invalid_evidence")
        observation, context, rows = _fixture()
        rows[0]["timestamp"] = "2026-09-22T13:00:00+00:00"
        result = select(observation, context, rows, fee_per_leg="0.10", slippage_per_leg="0.05")
        self.assertEqual(result["reason_code"], "invalid_evidence")
        observation, context, rows = _fixture()
        rows[1]["ask_size"] = 0
        result = select(observation, context, rows, fee_per_leg="0.10", slippage_per_leg="0.05")
        self.assertEqual(result["reason_code"], "invalid_evidence")
        observation, context, rows = _fixture()
        rows[0]["ask"] = "0.20"
        _rehash(observation, context, rows)
        result = select(observation, context, rows, fee_per_leg="0.10", slippage_per_leg="0.05")
        self.assertEqual(result["reason_code"], "entry_cost_cap")

    def test_payload_bindings_receipts_catalog_and_delta_tolerance_block_bypass(self):
        observation, context, rows = _fixture()
        rows[0]["ask"] = "0.11"
        result = select(observation, context, rows, fee_per_leg="0.10", slippage_per_leg="0.05")
        self.assertEqual(result["reason_code"], "invalid_evidence")

        observation, context, rows = _fixture()
        rows[1] = dict(rows[0], instrument_id="different-id")
        _rehash(observation, context, rows)
        result = select(observation, context, rows, fee_per_leg="0.10", slippage_per_leg="0.05")
        self.assertEqual(result["reason_code"], "invalid_evidence")

        observation, context, rows = _fixture()
        observation["bars"][-1]["session_date"] = "2026-09-18"
        observation["normalized_payload_sha256"] = payload_sha256(observation, hash_field="normalized_payload_sha256")
        result = select(observation, context, rows, fee_per_leg="0.10", slippage_per_leg="0.05")
        self.assertEqual(result["reason_code"], "invalid_evidence")

        observation, context, rows = _fixture()
        context["chain_metadata"]["available_expirations"]["C"] = ["2026-11-06"]
        _rehash(observation, context, rows)
        result = select(observation, context, rows, fee_per_leg="0.10", slippage_per_leg="0.05")
        self.assertEqual(result["reason_code"], "missing_chosen_expiry")

        observation, context, rows = _fixture()
        context["candidate_universe"]["complete"] = False
        _rehash(observation, context, rows)
        result = select(observation, context, rows, fee_per_leg="0.10", slippage_per_leg="0.05")
        self.assertEqual(result["reason_code"], "invalid_evidence")

        observation, context, rows = _fixture()
        rows[0]["delta"] = "0.99"
        rows[1]["delta"] = "0.98"
        _rehash(observation, context, rows)
        result = select(observation, context, rows, fee_per_leg="0.10", slippage_per_leg="0.05")
        self.assertEqual(result["reason_code"], "no_directional_pair")

        observation, context, rows = _fixture()
        rows[0]["ask"] = "0.187"
        _rehash(observation, context, rows)
        result = select(observation, context, rows, fee_per_leg="0.10", slippage_per_leg="0.05")
        self.assertEqual(result["status"], "candidate")
        self.assertEqual(Decimal(result["cost_model"]["total_entry_cost"]), Decimal("15"))


if __name__ == "__main__":
    unittest.main()
