import unittest
from datetime import datetime, timezone
from decimal import Decimal

from desks.options.paper_proposal import prepare_paper_vertical
from desks.options.verticals import VerticalValidationError
from neorome.alpaca_paper import PaperClient


class PaperProposalTests(unittest.TestCase):
    def legs(self, right="C", reverse=False):
        legs = [dict(
            symbol="SPY   261218" + right + strike,
            underlying="SPY", expiry="2026-12-18", right=right,
            strike=price, side=side, quantity=2, multiplier=100,
            adjusted=False, bid=bid, ask=ask,
            timestamp="2026-09-14T14:00:00+00:00",
        ) for strike, price, side, bid, ask in [
            ("00500000", "500", "buy", "6", "6.2"),
            ("00505000", "505", "sell", "3", "3.2"),
        ]]
        if right == "P":
            legs[0].update(bid="3", ask="3.2", side="sell")
            legs[1].update(bid="6", ask="6.2", side="buy")
        if reverse:
            for leg in legs:
                leg["side"] = "sell" if leg["side"] == "buy" else "buy"
        return legs

    def prepare(self, legs=None, **changes):
        controls = dict(
            as_of=datetime(2026, 9, 14, 14, 0, 1, tzinfo=timezone.utc),
            client_order_id="options-synthetic-preview-001",
            max_loss_budget="1000", max_quantity=2,
            fee_per_contract="2", slippage_per_contract="1",
        )
        controls.update(changes)
        return prepare_paper_vertical(self.legs() if legs is None else legs, **controls)

    def test_four_orientations_risk_and_payload_agree(self):
        for right in ("C", "P"):
            for reverse in (False, True):
                with self.subTest(right=right, reverse=reverse):
                    risk, request = self.prepare(self.legs(right, reverse))
                    self.assertEqual(risk.max_loss, Decimal("440" if reverse else "640"))
                    self.assertEqual(risk.risk_including_costs, risk.max_loss + 6)
                    payload = request.payload()
                    self.assertEqual(payload["limit_price"], "-2.8" if reverse else "3.2")
                    self.assertEqual(payload["qty"], "2")
                    self.assertEqual(payload["order_class"], "mleg")
                    self.assertTrue(all(" " not in leg["symbol"] for leg in payload["legs"]))
                    self.assertTrue(all(leg["ratio_qty"] == "1" for leg in payload["legs"]))

    def test_preview_has_no_credentials_or_network(self):
        def forbidden(*args, **kwargs):
            self.fail("offline proposal attempted I/O")
        client = PaperClient(credential_provider=forbidden, opener=forbidden)
        _, request = self.prepare()
        self.assertEqual(client.submit_order(request), request.payload())

    def test_budget_includes_costs_and_reserved_risk(self):
        with self.assertRaises(VerticalValidationError):
            self.prepare(max_loss_budget="645")
        with self.assertRaises(VerticalValidationError):
            self.prepare(max_loss_budget="700", existing_reserved_risk="55")
        self.prepare(max_loss_budget="700", existing_reserved_risk="54")

    def test_mandatory_controls_and_stale_quotes(self):
        for changes in ({"max_loss_budget": None}, {"max_quantity": None}, {"max_quantity": 1}):
            with self.subTest(changes=changes), self.assertRaises(VerticalValidationError):
                self.prepare(**changes)
        with self.assertRaises(VerticalValidationError):
            self.prepare(as_of=datetime(2026, 9, 14, 14, 6, tzinfo=timezone.utc))


if __name__ == "__main__":
    unittest.main()
