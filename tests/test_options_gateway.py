from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest

from neorome.options_gateway import (
    OptionsGatewayValidationError,
    convert_options_proposal,
    reserve_options_proposal,
)
from neorome.paper_gateway import PaperProposalGateway
from neorome.paper_ledger import PaperLedger, RiskLimitExceeded, RiskLimits


UTC = timezone.utc
NOW = datetime(2026, 9, 21, 14, 0, tzinfo=UTC)
HASH = "a" * 64
FILL_HASH = "b" * 64


def occ(strike: int, right: str = "C", root: str = "SPY", date_code: str = "261218") -> str:
    return f"{root:<6}{date_code}{right}{strike * 1000:08d}"


def leg(strike: int, side: str, *, bid: str, ask: str, timestamp: datetime = NOW - timedelta(seconds=60),
        depth: int = 4, quantity: int = 2, root: str = "SPY", expiry: str = "2026-12-18",
        right: str = "C") -> dict[str, object]:
    return {
        "symbol": occ(strike, right=right, root=root), "underlying": root,
        "right": right, "expiry": expiry, "strike": str(strike), "side": side,
        "quantity": quantity, "bid": bid, "ask": ask,
        "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
        "multiplier": 100, "adjusted": False, "depth": depth,
        "ask_size": depth, "bid_size": depth,
    }


def payload(*, quantity: int = 2, ask: str = "0.03", sell_bid: str = "0.01") -> dict[str, object]:
    return {
        "proposal_id": "opt-001", "strategy": "trend-vertical", "strategy_version": "v1",
        "source_kind": "observed",
        "source": {"kind": "observed", "hash": HASH,
                    "received_at": (NOW - timedelta(seconds=30)).isoformat().replace("+00:00", "Z")},
        "idea_id": "spy-idea-001", "expiry_at": (NOW + timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
        "execution": {"delay_seconds": 10},
        "legs": [
            leg(650, "buy", bid="0.02", ask=ask, quantity=quantity),
            leg(655, "sell", bid=sell_bid, ask="0.02", quantity=quantity),
        ],
        "metadata": {"caller": "fixture"},
    }


class OptionsGatewayTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.ledger = PaperLedger(Path(self.temp.name) / "paper.sqlite3", clock=lambda: NOW, enforce_market_risk=True)
        self.ledger.record_marks('initial-cash', [], NOW-timedelta(seconds=60))
        self.gateway = PaperProposalGateway(self.ledger, clock=lambda: NOW)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_refuses_relaxed_firm_controls(self):
        for name, enforce, limits in [('off', False, RiskLimits()), ('raised', True, RiskLimits(aggregate_cap_cents=9000))]:
            ledger = PaperLedger(Path(self.temp.name)/name, enforce_market_risk=enforce, limits=limits, clock=lambda: NOW)
            gateway = PaperProposalGateway(ledger, clock=lambda: NOW)
            with self.assertRaises(OptionsGatewayValidationError):
                reserve_options_proposal(payload(), gateway, as_of=NOW)

    def test_both_leg_costs_enforced_by_low_level_budget(self):
        with self.assertRaises(OptionsGatewayValidationError):
            convert_options_proposal(payload(), as_of=NOW,
                fee_cents_per_contract=2, slippage_cents_per_contract=1,
                max_loss_budget="4.10")

    def test_valid_debit_conversion_has_complete_spread_cents_multiplier_and_depth(self) -> None:
        converted = convert_options_proposal(
            payload(), as_of=NOW, fee_cents_per_contract=2, slippage_cents_per_contract=1
        )
        self.assertEqual(converted["desk"], "options")
        self.assertEqual(converted["instrument_type"], "debit_spread")
        self.assertEqual(converted["quantity"], 2)
        self.assertEqual(converted["quote"], {
            "timestamp": "2026-09-21T13:59:00Z", "bid_cents": 0,
            "ask_cents": 200, "depth": 4,
        })
        self.assertEqual(converted["cost"], {"unit_cost_cents": 200, "fee_cents": 12})
        self.assertEqual(converted["metadata"]["options_gateway_v1"]["multiplier"], 100)
        self.assertEqual(converted["metadata"]["options_gateway_v1"]["complete_spread_depth"], 4)
        self.assertEqual(converted["metadata"]["options_gateway_v1"]["leg_quote_timestamps"], [
            {"symbol": occ(650).replace(" ", ""), "timestamp": "2026-09-21T13:59:00Z"},
            {"symbol": occ(655).replace(" ", ""), "timestamp": "2026-09-21T13:59:00Z"},
        ])
        self.assertEqual(converted["exposure_group"], "us-broad-equity")
        self.assertTrue(converted["instrument"].startswith("vertical:"))

    def test_microsecond_asynchronous_legs_use_oldest_common_timestamp(self) -> None:
        data = payload()
        newer = NOW - timedelta(seconds=59, microseconds=999999)
        data["legs"][1]["timestamp"] = newer.isoformat().replace("+00:00", "Z")
        data["source"]["observed_at"] = (NOW - timedelta(seconds=60)).isoformat().replace("+00:00", "Z")
        converted = convert_options_proposal(data, as_of=NOW)
        self.assertEqual(converted["quote"]["timestamp"], "2026-09-21T13:59:00Z")
        self.assertEqual(converted["source"]["observed_at"], "2026-09-21T13:59:00Z")
        self.assertEqual(
            [item["timestamp"] for item in converted["metadata"]["options_gateway_v1"]["leg_quote_timestamps"]],
            ["2026-09-21T13:59:00Z", "2026-09-21T13:59:00.000001Z"],
        )

    def test_negative_liquidation_is_not_clipped_to_zero(self):
        data = payload()
        data['legs'][1]['ask'] = '0.04'
        with self.assertRaises(OptionsGatewayValidationError):
            convert_options_proposal(data, as_of=NOW)

    def test_sixty_one_second_quote_rejected_by_default(self):
        data = payload()
        for item in data['legs']:
            item['timestamp'] = (NOW-timedelta(seconds=61)).isoformat()
        with self.assertRaises(OptionsGatewayValidationError):
            convert_options_proposal(data, as_of=NOW)

    def test_age_override_source_conflict_and_wrong_side_size_rejected(self):
        with self.assertRaises(OptionsGatewayValidationError):
            convert_options_proposal(payload(), as_of=NOW, max_quote_age=timedelta(seconds=61))
        data = payload()
        data['source']['observed_at'] = NOW.isoformat()
        with self.assertRaises(OptionsGatewayValidationError):
            convert_options_proposal(data, as_of=NOW)
        data = payload()
        data['legs'][0]['ask_size'] = 0
        with self.assertRaises(OptionsGatewayValidationError):
            convert_options_proposal(data, as_of=NOW)

    def test_convert_and_reserve_uses_shared_fifteen_dollar_idea_cap(self) -> None:
        result = reserve_options_proposal(
            payload(), self.gateway, as_of=NOW, now=NOW,
            fee_cents_per_contract=2, slippage_cents_per_contract=1,
        )
        self.assertEqual(result["status"], "reserved")
        self.assertEqual(self.ledger.snapshot().reserved_cash_cents, 412)
        replay = reserve_options_proposal(
            deepcopy(payload()), self.gateway, as_of=NOW, now=NOW + timedelta(hours=1),
            fee_cents_per_contract=2, slippage_cents_per_contract=1,
        )
        self.assertEqual(replay["reservation"], result["reservation"])
        expensive = payload(ask="0.10", sell_bid="0.01")
        expensive["proposal_id"] = "opt-expensive"
        with self.assertRaises(RiskLimitExceeded):
            reserve_options_proposal(expensive, self.gateway, as_of=NOW, now=NOW)

    def test_later_independent_complete_spread_event_is_required(self) -> None:
        reservation = reserve_options_proposal(
            payload(), self.gateway, as_of=NOW - timedelta(seconds=30), now=NOW - timedelta(seconds=30)
        )["reservation"]
        too_early = {
            "reservation_id": reservation["reservation_id"], "fill_id": "opt-fill-early",
            "source_kind": "observed",
            "source": {"kind": "observed", "hash": FILL_HASH,
                        "observed_at": (NOW - timedelta(seconds=21)).isoformat().replace("+00:00", "Z"),
                        "received_at": (NOW - timedelta(seconds=21)).isoformat().replace("+00:00", "Z")},
            "quote": {"timestamp": (NOW - timedelta(seconds=21)).isoformat().replace("+00:00", "Z"),
                      "bid_cents": 0, "ask_cents": 200, "depth": 2},
            "quantity": 2, "cost": {"unit_cost_cents": 200, "fee_cents": 0},
            "filled_at": NOW.isoformat().replace("+00:00", "Z"),
        }
        with self.assertRaisesRegex(ValueError, "post-reservation delay"):
            self.gateway.record_fill(too_early)
        event = {
            "reservation_id": reservation["reservation_id"], "fill_id": "opt-fill-001",
            "source_kind": "observed",
            "source": {"kind": "observed", "hash": FILL_HASH,
                        "observed_at": (NOW - timedelta(seconds=15)).isoformat().replace("+00:00", "Z"),
                        "received_at": (NOW - timedelta(seconds=15)).isoformat().replace("+00:00", "Z")},
            "quote": {"timestamp": (NOW - timedelta(seconds=15)).isoformat().replace("+00:00", "Z"),
                      "bid_cents": 0, "ask_cents": 200, "depth": 2},
            "quantity": 2, "cost": {"unit_cost_cents": 200, "fee_cents": 0},
            "filled_at": NOW.isoformat().replace("+00:00", "Z"),
        }
        self.assertEqual(self.gateway.record_fill(event)["status"], "filled")

    def test_geometry_depth_and_labels_fail_closed(self) -> None:
        cases = []
        credit = payload()
        credit["legs"] = [
            leg(655, "buy", bid="0.01", ask="0.02"),
            leg(650, "sell", bid="0.04", ask="0.05"),
        ]
        cases.append(credit)
        mismatched = payload()
        mismatched["legs"] = [
            leg(650, "buy", bid="0.02", ask="0.03"),
            leg(655, "sell", bid="0.01", ask="0.02", root="QQQ"),
        ]
        cases.append(mismatched)
        labelled = payload()
        labelled["legs"][0] = {**labelled["legs"][0], "label": "long"}
        cases.append(labelled)
        shallow = payload()
        shallow["legs"][0] = {**shallow["legs"][0], "depth": 1}
        cases.append(shallow)
        for case in cases:
            with self.subTest(case=case):
                with self.assertRaises(OptionsGatewayValidationError):
                    convert_options_proposal(case, as_of=NOW)

    def test_each_leg_stale_future_and_receipt_conflicts_fail_closed(self) -> None:
        for index in (0, 1):
            stale = payload()
            stale["legs"][index] = {**stale["legs"][index],
                                     "timestamp": (NOW - timedelta(seconds=61)).isoformat().replace("+00:00", "Z")}
            with self.subTest(case="stale", index=index), self.assertRaises(OptionsGatewayValidationError):
                convert_options_proposal(stale, as_of=NOW)
            future = payload()
            future["legs"][index] = {**future["legs"][index],
                                      "timestamp": (NOW + timedelta(microseconds=1)).isoformat().replace("+00:00", "Z")}
            with self.subTest(case="future", index=index), self.assertRaises(OptionsGatewayValidationError):
                convert_options_proposal(future, as_of=NOW)

        # The common spread timestamp is the older leg, but the newer leg must
        # still not postdate source receipt.
        after_receipt = payload()
        after_receipt["legs"][1] = {**after_receipt["legs"][1],
                                     "timestamp": (NOW - timedelta(seconds=20)).isoformat().replace("+00:00", "Z")}
        with self.assertRaisesRegex(OptionsGatewayValidationError, "either leg"):
            convert_options_proposal(after_receipt, as_of=NOW)

    def test_source_and_aggregate_provenance_fail_closed(self) -> None:
        future = payload()
        future["source"] = {**future["source"],
                             "received_at": (NOW + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")}
        with self.assertRaises(OptionsGatewayValidationError):
            convert_options_proposal(future, as_of=NOW)
        mismatched = payload()
        mismatched["source"]["observed_at"] = (NOW - timedelta(seconds=59)).isoformat().replace("+00:00", "Z")
        with self.assertRaises(OptionsGatewayValidationError):
            convert_options_proposal(mismatched, as_of=NOW)

    def test_missing_or_mixed_provenance_and_explicit_group_fail_closed(self) -> None:
        missing = payload()
        del missing["source"]["hash"]
        with self.assertRaises(OptionsGatewayValidationError):
            convert_options_proposal(missing, as_of=NOW)
        wrong_group = payload()
        wrong_group["exposure_group"] = "options:other"
        with self.assertRaises(OptionsGatewayValidationError):
            convert_options_proposal(wrong_group, as_of=NOW)
        wrong_kind = payload()
        wrong_kind["source_kind"] = "synthetic"
        wrong_kind["source"] = {**wrong_kind["source"], "kind": "observed"}
        with self.assertRaises(OptionsGatewayValidationError):
            convert_options_proposal(wrong_kind, as_of=NOW)


if __name__ == "__main__":
    unittest.main()
