import unittest

from neorome.crypto_interval_paper import IntervalPaperError, rehearse
from neorome.crypto_interval_replay import replay


IDENTITY = {
    "market_id": "btc-15m-2026-01-01T12:00Z",
    "outcome_id": "yes-token-1",
    "outcome": "YES",
    "terms": {"reference": "oracle-btc-usd", "comparison": ">=", "cutoff": "2026-01-01T12:15:00Z"},
    "source_identity": {"venue": "research-book", "oracle": "oracle-btc-usd-v1"},
    "open_at": "2026-01-01T11:45:00Z",
    "cutoff_at": "2026-01-01T12:15:00Z",
    "expiry_at": "2026-01-01T12:15:00Z",
}
FEES = {
    "version": "fixed-stress-v1",
    "entry_fee_cents_per_contract": "1",
    "exit_fee_cents_per_contract": "1",
    "settlement_fee_cents_per_contract": "1",
}


def adapter(*, reasons=None):
    event = {
        **IDENTITY,
        "event_type": "book",
        "event_at": "2026-01-01T12:00:00Z",
        "received_at": "2026-01-01T12:00:02Z",
        "asks": [{"price": "0.40", "size": 2}],
        "bids": [{"price": "0.39", "size": 2}],
    }
    return {
        "scope": "offline_research_only",
        "execution_enabled": False,
        "paper_admitted": False,
        "admissible_for_replay": not reasons,
        "admission_reasons": list(reasons or []),
        "contract": IDENTITY,
        "events": [event],
        "fee": {"status": "caller_supplied_fixed_stress", **FEES},
    }


def replay_input(*, settlement=False):
    event = {
        **IDENTITY,
        "event_type": "book",
        "event_at": "2026-01-01T12:00:00Z",
        "received_at": "2026-01-01T12:00:02Z",
        "asks": [{"price": "0.40", "size": 2}],
        "bids": [{"price": "0.39", "size": 2}],
    }
    value = {
        "contract": IDENTITY,
        "decision": {"received_at": "2026-01-01T11:59:58Z", "latency_seconds": 1, "entry_requested": True, "contracts": 2},
        "events": [event],
    }
    if settlement:
        value["settlement"] = {
            **IDENTITY,
            "event_type": "settlement",
            "status": "finalized",
            "finalized": True,
            "value": 1,
            "evidence": {"record_id": "final-1"},
            "event_at": "2026-01-01T12:15:00Z",
            "received_at": "2026-01-01T12:15:02Z",
        }
    return value


class CryptoIntervalPaperTests(unittest.TestCase):
    def test_finalized_settlement_runs_isolated_prediction_sleeve(self):
        replayed = replay(replay_input(settlement=True), fee_schedule=FEES)
        report = rehearse(
            adapter(), replayed, event_timestamps=["2026-01-01T12:00:00Z"], contract_size=2, fee_schedule=FEES
        )
        self.assertEqual(report["mode"], "synthetic_crypto_interval_paper_rehearsal")
        self.assertEqual([action["type"] for action in report["actions"]], ["reserve", "simulated_fill", "settlement"])
        self.assertFalse(report["execution_enabled"])
        self.assertFalse(report["observed_shared_ledger_written"])
        self.assertFalse(report["profitability_validated"])
        prediction = next(item for item in report["final"]["balances"] if item["desk"] == "prediction")
        self.assertEqual(prediction["available_cash_cents"], 10_000 + replayed["net_pnl_cents"])
        self.assertEqual(prediction["reserved_cash_cents"], 0)

    def test_unresolved_settlement_marks_open_position_and_restarts_reservation(self):
        replayed = replay(replay_input(), fee_schedule=FEES)
        self.assertEqual(replayed["status"], "unresolved")
        report = rehearse(
            adapter(), replayed, event_timestamps={0: "2026-01-01T12:00:00Z"}, contract_size=2, fee_schedule=FEES
        )
        self.assertEqual([action["type"] for action in report["actions"]], ["reserve", "simulated_fill", "mark"])
        self.assertEqual(report["final"]["snapshot"]["open_position_cost_cents"], replayed["costs"]["entry_debit_cents"])

    def test_any_adapter_admission_reason_fails_closed(self):
        replayed = replay(replay_input(), fee_schedule=FEES)
        with self.assertRaisesRegex(IntervalPaperError, "missing_exchange_event_timestamp"):
            rehearse(
                adapter(reasons=["missing_exchange_event_timestamp"]),
                replayed,
                event_timestamps=["2026-01-01T12:00:00Z"],
                contract_size=2,
                fee_schedule=FEES,
            )

    def test_timestamps_units_and_fee_version_are_explicit(self):
        replayed = replay(replay_input(), fee_schedule=FEES)
        with self.assertRaisesRegex(IntervalPaperError, "explicit event timestamps"):
            rehearse(adapter(), replayed, event_timestamps=[], contract_size=2, fee_schedule=FEES)
        with self.assertRaisesRegex(IntervalPaperError, "contract count"):
            rehearse(adapter(), replayed, event_timestamps=["2026-01-01T12:00:00Z"], contract_size=1, fee_schedule=FEES)
        wrong = dict(FEES, version="unreviewed-v2")
        with self.assertRaisesRegex(IntervalPaperError, "version"):
            rehearse(adapter(), replayed, event_timestamps=["2026-01-01T12:00:00Z"], contract_size=2, fee_schedule=wrong)


if __name__ == "__main__":
    unittest.main()
