import json
from pathlib import Path
import tempfile
import unittest
import urllib.error
import urllib.parse
from unittest.mock import Mock
from neorome.jupiter_quotes import QuoteClient, QuoteError, NoRedirect, SOL, USDC, ENDPOINT, collect, save_new


class Response:
    status = 200
    def __init__(self, data): self.body = data if isinstance(data, bytes) else json.dumps(data).encode()
    def read(self, limit): return self.body[:limit]
    def __enter__(self): return self
    def __exit__(self, *args): pass


def payload(**changes):
    value = {'inputMint': SOL, 'outputMint': USDC, 'inAmount': '10000000', 'outAmount': '990000',
             'router': 'metis', 'transaction': None, 'slippageBps': 50}
    value.update(changes)
    return value


class JupiterQuotesTests(unittest.TestCase):
    def client(self, data=None):
        opener = Mock()
        opener.open.return_value = Response(payload() if data is None else data)
        loader = Mock(return_value='test-secret-only')
        return QuoteClient(loader, opener), opener, loader

    def test_request_is_fixed_quote_only_and_record_has_no_secret(self):
        client, opener, _ = self.client()
        record = client.quote(SOL, USDC, '10000000')
        request = opener.open.call_args[0][0]
        url = urllib.parse.urlsplit(request.full_url)
        self.assertEqual(request.get_method(), 'GET')
        self.assertEqual(url.scheme + '://' + url.netloc + url.path, ENDPOINT)
        self.assertEqual(set(urllib.parse.parse_qs(url.query)), {'inputMint','outputMint','amount','swapMode','slippageBps'})
        self.assertEqual(request.get_header('X-api-key'), 'test-secret-only')
        self.assertIsNone(request.data)
        self.assertNotIn('test-secret-only', json.dumps(record))
        self.assertNotIn('transaction', record['quote'])
        self.assertFalse(record['execution_enabled'])
        self.assertFalse(record['eligible_for_trading'])
        self.assertEqual(len(record['response_sha256']), 64)

    def test_bad_inputs_never_touch_credentials_or_network(self):
        for args in [(SOL,USDC,True),(SOL,USDC,'1.2'),(SOL,USDC,0),(SOL,USDC,2**64),
                     ('https://evil.test',USDC,1),(SOL,SOL,1),('1'*33,USDC,1),(SOL,USDC,1,True)]:
            with self.subTest(args=args):
                client, opener, loader = self.client()
                with self.assertRaises(QuoteError): client.quote(*args)
                loader.assert_not_called(); opener.open.assert_not_called()

    def test_current_impact_schema_preserves_units(self):
        client, _, _ = self.client(payload(priceImpact=0.25))
        quote = client.quote(SOL, USDC, '10000000')['quote']
        self.assertEqual(quote['priceImpact'], '0.25')
        self.assertEqual(quote['priceImpactUnit'], 'percentage_points')
        for bad in (True, 'NaN', 'Infinity', 101, {}):
            client, _, _ = self.client(payload(priceImpact=bad))
            with self.assertRaises(QuoteError): client.quote(SOL, USDC, '10000000')

    def test_unusable_response_is_rejected(self):
        for data in [payload(inputMint=USDC), payload(inAmount='999'), payload(outAmount='0'),
                     payload(outAmount=True), payload(error='no route'), payload(errorCode=1),
                     payload(transaction='unsigned-material'), payload(taker=SOL),
                     payload(router='echo-test-secret-only'), payload(feeBps=-1), [], b'bad json', b'x'*1_000_001]:
            with self.subTest(kind=type(data).__name__):
                client, _, _ = self.client(data)
                with self.assertRaises(QuoteError): client.quote(SOL,USDC,10000000)

    def test_http_error_is_sanitized_and_never_retried(self):
        client, opener, _ = self.client()
        opener.open.side_effect = urllib.error.HTTPError(ENDPOINT,403,'test-secret-only',{},None)
        with self.assertRaisesRegex(QuoteError,'HTTP 403') as caught: client.quote(SOL,USDC,10000000)
        self.assertNotIn('test-secret-only',str(caught.exception))
        self.assertEqual(opener.open.call_count,1)

    def test_transport_error_is_sanitized(self):
        client, opener, _ = self.client()
        opener.open.side_effect = RuntimeError('test-secret-only')
        with self.assertRaisesRegex(QuoteError,'transport failed') as caught: client.quote(SOL,USDC,10000000)
        self.assertNotIn('test-secret-only',str(caught.exception))

    def test_redirect_refused(self):
        with self.assertRaises(QuoteError): NoRedirect().redirect_request(None,None,302,'',{},'https://evil.test')

    def test_round_trip_is_two_observations_not_fills(self):
        client, opener, _ = self.client()
        opener.open.side_effect = [Response(payload()),Response(payload(inputMint=USDC,outputMint=SOL,inAmount='990000',outAmount='9800000'))]
        record=collect(client,SOL,USDC,10000000,True)
        self.assertEqual(opener.open.call_count,2)
        self.assertEqual(record['observations'][1]['request']['amount'],'990000')
        self.assertTrue(record['not_realized_pnl'])
        self.assertFalse(record['execution_enabled'])

    def test_evidence_is_exclusive_and_private(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'quote.json'
            save_new(path, {'quote':1})
            self.assertEqual(path.stat().st_mode & 0o777,0o600)
            with self.assertRaises(FileExistsError):save_new(path,{'quote':2})
            self.assertEqual(json.loads(path.read_text()),{'quote':1})
            link=Path(tmp)/'link.json';link.symlink_to(path)
            with self.assertRaises(FileExistsError):save_new(link,{'quote':3})
            self.assertEqual(list(Path(tmp).glob('.quote-*')),[])


if __name__ == '__main__': unittest.main()
