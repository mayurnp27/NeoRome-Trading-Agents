"""Synthetic wire-format fixtures and fake transports; no market data or orders."""

import json
import socket
import unittest

from neorome.alpaca_paper import (
    AmbiguousOrderError,
    MultiLegOrderRequest,
    PaperClient,
    PAPER_API_URL,
    OptionLeg,
    ValidationError,
)


class FakeResponse:
    def __init__(self, value, status=200):
        self.status = status
        self._body = value if isinstance(value, bytes) else json.dumps(value).encode("utf-8")
        self.closed = False

    def close(self):
        self.closed = True

    def read(self):
        return self._body


class RecordingOpener:
    def __init__(self, response=None, error=None):
        self.requests = []
        self.response = response or FakeResponse({"id": "order-1"})
        self.error = error

    def open(self, request, timeout=None):
        self.requests.append((request, timeout))
        if self.error:
            raise self.error
        return self.response


def credentials(service):
    return {
        "neorome.alpaca.paper.key-id": "key-for-tests",
        "neorome.alpaca.paper.secret-key": "secret-for-tests",
    }[service]


class MultiLegOrderValidationTests(unittest.TestCase):
    call_low = "AAPL250117C00190000"
    call_high = "AAPL250117C00210000"
    put_low = "AAPL250117P00190000"
    put_high = "AAPL250117P00210000"

    def request(self, first=None, second=None, **changes):
        value = {
            "legs": (
                first or OptionLeg(self.call_low, "1", "buy", "buy_to_open"),
                second or OptionLeg(self.call_high, "1", "sell", "sell_to_open"),
            ),
            "qty": "2",
            "limit_price": "1.25",
            "client_order_id": "mleg-001",
        }
        value.update(changes)
        return MultiLegOrderRequest(**value)

    def test_call_debit_payload_is_signed_and_exact(self):
        self.assertEqual(
            self.request().payload(),
            {
                "order_class": "mleg",
                "qty": "2",
                "type": "limit",
                "limit_price": "1.25",
                "time_in_force": "day",
                "client_order_id": "mleg-001",
                "legs": [
                    {"symbol": self.call_low, "ratio_qty": "1", "side": "buy", "position_intent": "buy_to_open"},
                    {"symbol": self.call_high, "ratio_qty": "1", "side": "sell", "position_intent": "sell_to_open"},
                ],
            },
        )

    def test_put_credit_payload_is_signed(self):
        request = self.request(
            OptionLeg(self.put_high, "1", "sell", "sell_to_open"),
            OptionLeg(self.put_low, "1", "buy", "buy_to_open"),
            limit_price="-1.25",
        )
        self.assertEqual(
            request.payload(),
            {
                "order_class": "mleg",
                "qty": "2",
                "type": "limit",
                "limit_price": "-1.25",
                "time_in_force": "day",
                "client_order_id": "mleg-001",
                "legs": [
                    {"symbol": self.put_high, "ratio_qty": "1", "side": "sell", "position_intent": "sell_to_open"},
                    {"symbol": self.put_low, "ratio_qty": "1", "side": "buy", "position_intent": "buy_to_open"},
                ],
            },
        )

    def test_vertical_shape_and_numeric_edges_are_rejected(self):
        bad_requests = (
            self.request(first=OptionLeg("AAPL250117C0019000", "1", "buy", "buy_to_open")),
            self.request(second=OptionLeg("MSFT250117C00210000", "1", "sell", "sell_to_open")),
            self.request(second=OptionLeg(self.call_low, "1", "sell", "sell_to_open")),
            self.request(second=OptionLeg(self.call_high, "2", "sell", "sell_to_open")),
            self.request(second=OptionLeg(self.call_high, "1", "buy", "buy_to_open")),
            self.request(second=OptionLeg(self.call_high, "1", "sell", "sell_to_close")),
            self.request(qty="1.5"),
            self.request(limit_price="NaN"),
            self.request(limit_price="Infinity"),
            self.request(limit_price="0"),
            self.request(limit_price="20"),
            self.request(limit_price="-1.25"),
            self.request(type="market"),
            self.request(time_in_force="gtc"),
        )
        for request in bad_requests:
            with self.subTest(request=request):
                with self.assertRaises(ValidationError):
                    request.payload()

    def test_put_debit_requires_high_strike_buy(self):
        request = self.request(
            OptionLeg(self.put_low, "1", "buy", "buy_to_open"),
            OptionLeg(self.put_high, "1", "sell", "sell_to_open"),
        )
        with self.assertRaises(ValidationError):
            request.payload()


class MultiLegPaperClientTests(unittest.TestCase):
    def order(self, **changes):
        value = {
            "legs": (
                OptionLeg("AAPL250117C00190000", "1", "buy", "buy_to_open"),
                OptionLeg("AAPL250117C00210000", "1", "sell", "sell_to_open"),
            ),
            "qty": "1",
            "limit_price": "1.00",
            "client_order_id": "typed-mleg",
        }
        value.update(changes)
        return MultiLegOrderRequest(**value)

    def test_typed_mleg_preview_is_offline_and_mapping_remains_rejected(self):
        def fail(_service):
            raise AssertionError("credential provider must not be called")

        client = PaperClient(credential_provider=fail, opener=lambda *_args, **_kwargs: self.fail("network"))
        order = self.order()
        self.assertEqual(client.preview_order(order)["order_class"], "mleg")
        with self.assertRaises(ValidationError):
            client.preview_order(order.payload())

    def test_typed_mleg_submit_serializes_and_uses_timeout_once(self):
        opener = RecordingOpener(FakeResponse({"id": "mleg-order-1"}))
        client = PaperClient(credential_provider=credentials, timeout=3.5, opener=opener)
        order = self.order()
        result = client.submit_order(order, dry_run=False)
        self.assertEqual(result["id"], "mleg-order-1")
        self.assertEqual(len(opener.requests), 1)
        request, timeout = opener.requests[0]
        self.assertEqual(timeout, 3.5)
        self.assertEqual(request.full_url, PAPER_API_URL + "/v2/orders")
        self.assertEqual(json.loads(request.data.decode("utf-8")), order.payload())

    def test_typed_mleg_timeout_reconciles_with_same_client_id_without_retry(self):
        timeout_opener = RecordingOpener(error=socket.timeout("timed out"))
        client = PaperClient(credential_provider=credentials, opener=timeout_opener)
        order = self.order(client_order_id="same-mleg-id")
        with self.assertRaises(AmbiguousOrderError):
            client.submit_order(order, dry_run=False)
        self.assertEqual(len(timeout_opener.requests), 1)

        reconcile_opener = RecordingOpener(FakeResponse({"id": "mleg-order-1"}))
        client = PaperClient(credential_provider=credentials, opener=reconcile_opener)
        result = client.get_order_by_client_id(order.client_order_id)
        self.assertEqual(result["id"], "mleg-order-1")
        self.assertEqual(
            reconcile_opener.requests[0][0].full_url,
            PAPER_API_URL + "/v2/orders:by_client_order_id?client_order_id=same-mleg-id",
        )


if __name__ == "__main__":
    unittest.main()
