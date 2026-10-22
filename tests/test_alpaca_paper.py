import json
import socket
import unittest
import urllib.error

from neorome.alpaca_paper import (
    AmbiguousOrderError,
    ApiError,
    CredentialError,
    PAPER_API_URL,
    PaperAdapterError,
    RedirectBlockedError,
    ValidationError,
    OrderRequest,
    PaperClient,
    _NoRedirectHandler,
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


class OrderValidationTests(unittest.TestCase):
    def base(self, **changes):
        value = {
            "symbol": "AAPL",
            "qty": "1",
            "side": "buy",
            "asset_class": "us_equity",
            "client_order_id": "desk-001",
        }
        value.update(changes)
        return OrderRequest(**value)

    def test_exact_market_and_limit_payload(self):
        self.assertEqual(
            self.base().payload(),
            {
                "symbol": "AAPL", "qty": "1", "side": "buy", "type": "market",
                "time_in_force": "day", "client_order_id": "desk-001",
            },
        )
        self.assertEqual(
            self.base(type="limit", limit_price="123.45").payload()["limit_price"],
            "123.45",
        )

    def test_quantity_and_client_id_edges(self):
        for qty in ("0", "-1", "NaN", "Infinity", "abc"):
            with self.subTest(qty=qty):
                with self.assertRaises(ValidationError):
                    self.base(qty=qty).payload()
        with self.assertRaises(ValidationError):
            self.base(client_order_id="contains space").payload()
        with self.assertRaises(ValidationError):
            self.base(client_order_id="x" * 49).payload()

    def test_asset_specific_rules(self):
        with self.assertRaises(ValidationError):
            self.base(asset_class="us_option", symbol="AAPL240119C00100000").payload()
        option = self.base(
            asset_class="us_option", symbol="AAPL240119C00100000", position_intent="buy_to_open"
        )
        self.assertEqual(option.payload()["position_intent"], "buy_to_open")
        with self.assertRaises(ValidationError):
            self.base(
                asset_class="us_option", qty="1.5", position_intent="buy_to_open"
            ).payload()
        with self.assertRaises(ValidationError):
            self.base(
                asset_class="us_option", position_intent="sell_to_open"
            ).payload()
        with self.assertRaises(ValidationError):
            self.base(asset_class="crypto", time_in_force="day").payload()
        self.assertEqual(
            self.base(asset_class="crypto", symbol="BTC/USD", time_in_force="ioc").payload()["qty"],
            "1",
        )
        with self.assertRaises(ValidationError):
            self.base(qty="0.5", time_in_force="gtc").payload()

    def test_notional_and_multileg_rejected(self):
        for changes in ({"notional": "10"}, {"legs": []}, {"order_class": "mleg"}, {"multileg": True}):
            with self.subTest(changes=changes):
                with self.assertRaises(ValidationError):
                    self.base(**changes).payload()


class PaperClientTests(unittest.TestCase):
    def test_preview_never_loads_credentials_or_network(self):
        def fail(_service):
            raise AssertionError("credential provider must not be called")

        client = PaperClient(credential_provider=fail, opener=lambda *_args, **_kwargs: self.fail("network"))
        order = OrderRequest("AAPL", "1", "buy", "us_equity", "preview-1")
        self.assertEqual(client.submit_order(order), order.payload())
        self.assertEqual(client.preview_order(order), order.payload())
        self.assertEqual(client.cancel_order("order-1"), {"order_id": "order-1", "dry_run": True})

    def test_request_endpoint_headers_and_serialization(self):
        opener = RecordingOpener(FakeResponse({"id": "order-1"}))
        client = PaperClient(credential_provider=credentials, opener=opener)
        order = OrderRequest(
            "BTC/USD", "0.0001", "buy", "crypto", "crypto-1", "limit", "gtc", "1.00"
        )
        result = client.submit_order(order, dry_run=False)
        self.assertEqual(result["id"], "order-1")
        request, timeout = opener.requests[0]
        self.assertEqual(request.full_url, PAPER_API_URL + "/v2/orders")
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(request.headers["Apca-api-key-id"], "key-for-tests")
        self.assertEqual(json.loads(request.data.decode("utf-8")), order.payload())
        self.assertGreater(timeout, 0)

    def test_order_lookup_uses_fixed_endpoint_and_encoded_id(self):
        opener = RecordingOpener(FakeResponse({"id": "order-1"}))
        client = PaperClient(credential_provider=credentials, opener=opener)
        client.get_order_by_client_id("desk_1-2")
        self.assertEqual(
            opener.requests[0][0].full_url,
            PAPER_API_URL + "/v2/orders:by_client_order_id?client_order_id=desk_1-2",
        )

    def test_redirects_are_rejected(self):
        handler = _NoRedirectHandler()
        with self.assertRaises(RedirectBlockedError):
            handler.redirect_request(None, None, 302, "Found", {}, "https://example.test")

    def test_credential_error_is_redacted_and_unchained(self):
        secret = "super-secret-value"

        def fail(_service):
            raise RuntimeError("provider exploded with " + secret)

        client = PaperClient(credential_provider=fail, opener=RecordingOpener())
        with self.assertRaises(CredentialError) as caught:
            client.get_account()
        self.assertNotIn(secret, str(caught.exception))
        self.assertIsNone(caught.exception.__cause__)
        self.assertFalse(opener_was_called(client))

    def test_malformed_credential_is_rejected_before_transport(self):
        opener = RecordingOpener()

        def bad_credentials(service):
            return "key\nwith-control" if service.endswith("key-id") else "secret"

        client = PaperClient(credential_provider=bad_credentials, opener=opener)
        with self.assertRaises(CredentialError):
            client.get_account()
        self.assertFalse(opener.requests)

    def test_timeout_and_server_error_are_ambiguous(self):
        order = OrderRequest("AAPL", "1", "buy", "us_equity", "same-id")
        for error, response in (
            (socket.timeout("secret timeout"), None),
            (urllib.error.URLError("secret network error"), None),
            (None, FakeResponse({"error": "server detail"}, status=503)),
            (None, FakeResponse(b"not-json", status=200)),
        ):
            with self.subTest(error=error, response=response):
                client = PaperClient(
                    credential_provider=credentials,
                    opener=RecordingOpener(response=response, error=error),
                )
                with self.assertRaises(AmbiguousOrderError) as caught:
                    client.submit_order(order, dry_run=False)
                self.assertIn("SAME client_order_id", str(caught.exception))

    def test_mutation_does_not_retry_and_delete_accepts_204(self):
        opener = RecordingOpener(error=socket.timeout("timed out"))
        client = PaperClient(credential_provider=credentials, opener=opener)
        with self.assertRaises(AmbiguousOrderError):
            client.submit_order(OrderRequest("AAPL", "1", "buy", "us_equity", "same-id"), dry_run=False)
        self.assertEqual(len(opener.requests), 1)

        response = FakeResponse(b"", status=204)
        opener = RecordingOpener(response=response)
        client = PaperClient(credential_provider=credentials, opener=opener)
        self.assertIsNone(client.cancel_order("order/1", dry_run=False))
        self.assertTrue(response.closed)
        self.assertEqual(opener.requests[0][0].full_url, PAPER_API_URL + "/v2/orders/order%2F1")

    def test_non_boolean_dry_run_cannot_submit(self):
        client = PaperClient(credential_provider=credentials, opener=RecordingOpener())
        order = OrderRequest("AAPL", "1", "buy", "us_equity", "same-id")
        with self.assertRaises(ValidationError):
            client.submit_order(order, dry_run=0)
        self.assertFalse(client._opener.requests)

    def test_client_error_is_not_ambiguous(self):
        client = PaperClient(
            credential_provider=credentials,
            opener=RecordingOpener(FakeResponse({"error": "bad request"}, status=422)),
        )
        with self.assertRaises(ApiError):
            client.submit_order(OrderRequest("AAPL", "1", "buy", "us_equity", "same-id"), dry_run=False)

    def test_success_without_order_id_is_ambiguous(self):
        client = PaperClient(
            credential_provider=credentials,
            opener=RecordingOpener(FakeResponse({"status": "accepted"}, status=201)),
        )
        with self.assertRaises(AmbiguousOrderError):
            client.submit_order(OrderRequest("AAPL", "1", "buy", "us_equity", "same-id"), dry_run=False)

    def test_read_network_error_is_safe(self):
        secret = "response-secret"
        client = PaperClient(
            credential_provider=credentials,
            opener=RecordingOpener(error=OSError("socket leaked " + secret)),
        )
        with self.assertRaises(PaperAdapterError) as caught:
            client.get_account()
        self.assertNotIn(secret, str(caught.exception))


def opener_was_called(client):
    return bool(getattr(client._opener, "requests", []))


if __name__ == "__main__":
    unittest.main()
