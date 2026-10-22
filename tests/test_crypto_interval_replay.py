import json
import unittest

from neorome.crypto_interval_replay import ReplayError, replay


IDENTITY = {
    "market_id": "btc-15m-2026-01-01T12:00Z",
    "outcome_id": "yes-token-1",
    "outcome": "YES",
    "terms": {
        "reference": "oracle-btc-usd",
        "comparison": ">=",
        "cutoff": "2026-01-01T12:15:00Z",
        "payout_cents": 100,
    },
    "source_identity": {"venue": "research-book", "oracle": "oracle-btc-usd-v1"},
    "open_at": "2026-01-01T11:45:00Z",
    "cutoff_at": "2026-01-01T12:15:00Z",
    "expiry_at": "2026-01-01T12:15:00Z",
}

FEE = {
    "version": "test-fees-v1",
    "entry_fee_cents_per_contract": "1.5",
    "exit_fee_cents_per_contract": "1.5",
    "settlement_fee_cents_per_contract": "0.5",
}


def book(event_at, received_at, *, asks=None, bids=None, **extra):
    value = {
        **IDENTITY,
        "event_type": "book",
        "event_at": event_at,
        "received_at": received_at,
    }
    if asks is not None:
        value["asks"] = asks
    if bids is not None:
        value["bids"] = bids
    value.update(extra)
    return value


def settlement(*, value=1, received_at="2026-01-01T12:15:02Z", **extra):
    return {
        **IDENTITY,
        "event_type": "settlement",
        "status": "finalized",
        "finalized": True,
        "value": value,
        "evidence": {"record_id": "oracle-final-1"},
        "event_at": "2026-01-01T12:15:00Z",
        "received_at": received_at,
        **extra,
    }


def payload(events, *, decision=None, settlement_value=None):
    value = {
        "contract": IDENTITY,
        "decision": {
            "received_at": "2026-01-01T11:59:58Z",
            "latency_seconds": 1,
            "entry_requested": True,
            "contracts": 2,
            **(decision or {}),
        },
        "events": events,
    }
    if settlement_value is not None:
        value["settlement"] = settlement(value=settlement_value)
    return value


def run(data, **kwargs):
    kwargs.setdefault("fee_schedule", FEE)
    return replay(data, **kwargs)


class CryptoIntervalReplayTests(unittest.TestCase):
    def test_json_replay_walks_adverse_depth_and_rounds_fees_conservatively(self):
        data = payload(
            [
                book(
                    "2026-01-01T12:00:00Z",
                    "2026-01-01T12:00:02Z",
                    asks=[{"price": "0.40", "size": 1}, {"price": "0.60", "size": 1}],
                ),
            ],
            settlement_value=1,
        )
        result = run(json.dumps(data))

        self.assertEqual(result["status"], "realized")
        self.assertTrue(result["realized"])
        self.assertEqual(result["resolution"], "finalized_settlement")
        self.assertEqual(result["costs"]["entry_gross_cents"], 100)
        self.assertEqual(result["costs"]["entry_fee_cents"], 3)
        self.assertEqual(result["costs"]["settlement_fee_cents"], 1)
        self.assertEqual(result["costs"]["cash_proceeds_cents"], 199)
        self.assertEqual(result["costs"]["cash_outflow_cents"], 103)
        self.assertEqual(result["costs"]["cash_reserved_cents"], 106)
        self.assertEqual(result["net_pnl_cents"], 96)
        self.assertEqual(result["costs"]["fee_schedule_version"], "test-fees-v1")
        self.assertFalse(result["ready_for_execution"])

    def test_decision_uses_received_time_and_does_not_look_ahead(self):
        events = [
            book(
                "2026-01-01T11:59:40Z",
                "2026-01-01T11:59:45Z",
                asks=[{"price": "0.10", "size": 2}],
            ),
            book(
                "2026-01-01T12:00:00Z",
                "2026-01-01T12:00:03Z",
                asks=[{"price": "0.55", "size": 2}],
            ),
        ]
        result = run(payload(events))

        self.assertEqual(result["status"], "unresolved")
        self.assertEqual(result["entry_fills"][0]["price"], "0.55")
        self.assertEqual(result["costs"]["entry_gross_cents"], 110)
        self.assertIn("settlement_not_finalized", result["entry_rejection_reasons"])

    def test_identity_mismatch_and_stale_observation_block_entry(self):
        wrong = book(
            "2026-01-01T12:00:00Z",
            "2026-01-01T12:00:02Z",
            asks=[{"price": "0.40", "size": 2}],
            terms={"comparison": "<"},
        )
        stale = book(
            "2026-01-01T11:00:00Z",
            "2026-01-01T12:00:02Z",
            asks=[{"price": "0.40", "size": 2}],
        )
        result = run(payload([wrong, stale]))

        self.assertEqual(result["status"], "rejected")
        self.assertIn("identity_mismatch_terms", result["entry_rejection_reasons"])
        self.assertIn("entry_quote_stale", result["entry_rejection_reasons"])

    def test_insufficient_depth_and_cash_are_distinct_rejections(self):
        shallow = run(
            payload(
                [
                    book(
                        "2026-01-01T12:00:00Z",
                        "2026-01-01T12:00:02Z",
                        asks=[{"price": "0.40", "size": 1}],
                    )
                ],
            )
        )
        self.assertEqual(shallow["status"], "rejected")
        self.assertIn("insufficient_entry_depth", shallow["entry_rejection_reasons"])

        expensive = run(
            payload(
                [
                    book(
                        "2026-01-01T12:00:00Z",
                        "2026-01-01T12:00:02Z",
                        asks=[{"price": "1.00", "size": 2}],
                    )
                ],
                decision={"paper_budget_cents": 100, "idea_budget_cents": 500},
            )
        )
        self.assertEqual(expensive["status"], "rejected")
        self.assertIn("insufficient_cash", expensive["entry_rejection_reasons"])

    def test_scheduled_exit_requires_post_latency_bid_and_realizes_adverse_fill(self):
        result = run(
            payload(
                [
                    book(
                        "2026-01-01T12:00:00Z",
                        "2026-01-01T12:00:02Z",
                        asks=[{"price": "0.40", "size": 2}],
                    ),
                    book(
                        "2026-01-01T12:10:00Z",
                        "2026-01-01T12:10:02Z",
                        bids=[{"price": "0.20", "size": 2}],
                    ),
                ],
                decision={"exit_at": "2026-01-01T12:10:00Z"},
            ),
        )
        self.assertEqual(result["status"], "realized")
        self.assertEqual(result["resolution"], "scheduled_exit")
        self.assertEqual(result["costs"]["exit_gross_cents"], 40)
        self.assertEqual(result["costs"]["exit_fee_cents"], 3)
        self.assertEqual(result["net_pnl_cents"], -46)

    def test_missing_finalized_settlement_is_unresolved(self):
        result = run(
            payload(
                [
                    book(
                        "2026-01-01T12:00:00Z",
                        "2026-01-01T12:00:02Z",
                        asks=[{"price": "0.40", "size": 2}],
                    )
                ]
            )
        )
        self.assertEqual(result["status"], "unresolved")
        self.assertTrue(result["unresolved"])
        self.assertIsNone(result["net_pnl_cents"])
        self.assertIn("settlement_not_finalized", result["entry_rejection_reasons"])

    def test_order_lifetime_and_all_or_none_exit_depth(self):
        result = run(
            payload(
                [
                    book(
                        "2026-01-01T12:00:00Z",
                        "2026-01-01T12:00:02Z",
                        asks=[{"price": "0.40", "size": 2}],
                    ),
                    book(
                        "2026-01-01T12:10:00Z",
                        "2026-01-01T12:10:02Z",
                        bids=[{"price": "0.20", "size": 1}],
                    ),
                ],
                decision={"exit_at": "2026-01-01T12:10:00Z"},
            )
        )
        self.assertEqual(result["status"], "unresolved")
        self.assertIn("insufficient_exit_depth", result["entry_rejection_reasons"])
        self.assertEqual(result["exit_fills"], [])

        late = run(
            payload(
                [
                    book(
                        "2026-01-01T12:00:00Z",
                        "2026-01-01T12:00:02Z",
                        asks=[{"price": "0.40", "size": 2}],
                    ),
                    book(
                        "2026-01-01T12:10:10Z",
                        "2026-01-01T12:10:10Z",
                        bids=[{"price": "0.20", "size": 2}],
                    ),
                ],
                decision={"exit_at": "2026-01-01T12:10:00Z"},
            )
        )
        self.assertEqual(late["status"], "unresolved")
        self.assertIn("missing_post_latency_exit_quote", late["entry_rejection_reasons"])

        before_entry = run(
            payload(
                [
                    book(
                        "2026-01-01T12:00:00Z",
                        "2026-01-01T12:00:02Z",
                        asks=[{"price": "0.40", "size": 2}],
                    ),
                    book(
                        "2026-01-01T11:59:00Z",
                        "2026-01-01T11:59:01Z",
                        bids=[{"price": "0.20", "size": 2}],
                    ),
                ],
                decision={"exit_at": "2026-01-01T11:59:00Z"},
            )
        )
        self.assertEqual(before_entry["status"], "unresolved")
        self.assertIn("exit_before_entry", before_entry["entry_rejection_reasons"])
        self.assertFalse(before_entry["realized"])

    def test_lifecycle_and_settlement_type_are_explicit(self):
        after_cutoff = payload(
            [
                book(
                    "2026-01-01T12:15:01Z",
                    "2026-01-01T12:15:01Z",
                    asks=[{"price": "0.40", "size": 2}],
                )
            ],
            decision={"received_at": "2026-01-01T12:15:00Z"},
        )
        result = run(after_cutoff)
        self.assertEqual(result["status"], "rejected")
        self.assertIn("entry_after_contract_cutoff", result["entry_rejection_reasons"])

        late_quote = payload(
            [
                book(
                    "2026-01-01T12:15:00Z",
                    "2026-01-01T12:15:00Z",
                    asks=[{"price": "0.40", "size": 2}],
                )
            ], decision={"received_at": "2026-01-01T12:14:58Z"}
        )
        result = run(late_quote)
        self.assertEqual(result["status"], "rejected")
        self.assertIn("entry_quote_at_or_after_cutoff", result["entry_rejection_reasons"])

        invalid_settlement = payload(
            [
                book(
                    "2026-01-01T12:00:00Z",
                    "2026-01-01T12:00:02Z",
                    asks=[{"price": "0.40", "size": 2}],
                )
            ],
            settlement_value=None,
        )
        invalid_settlement["settlement"] = settlement(value=True)
        result = run(invalid_settlement)
        self.assertEqual(result["status"], "unresolved")
        self.assertIn("settlement_not_finalized_or_identity_mismatch", result["entry_rejection_reasons"])

    def test_fee_schedule_is_required_and_caps_cannot_be_enlarged(self):
        data = payload(
            [
                book(
                    "2026-01-01T12:00:00Z",
                    "2026-01-01T12:00:02Z",
                    asks=[{"price": "1.00", "size": 10}],
                )
            ],
            decision={"paper_budget_cents": 100000, "idea_budget_cents": 100000, "contracts": 10},
        )
        with self.assertRaisesRegex(ReplayError, "fee_schedule is required"):
            replay(data)
        result = run(data)
        self.assertEqual(result["status"], "rejected")
        self.assertIn("idea_budget_exceeded", result["entry_rejection_reasons"])
        self.assertEqual(result["costs"]["cash_reserved_cents"], 1030)


if __name__ == "__main__":
    unittest.main()
