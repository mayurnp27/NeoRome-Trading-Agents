import json
import unittest
from unittest.mock import Mock
from neorome.crypto_interval_evidence import PublicCapture, capture


class Response:
    status = 200
    def __init__(self, data): self.data = json.dumps(data).encode()
    def read(self, limit): return self.data[:limit]
    def __enter__(self): return self
    def __exit__(self,*args): pass


class CaptureTests(unittest.TestCase):
    def test_bounded_public_capture_preserves_raw_and_engine(self):
        opener = Mock()
        rows = [{'ticker':'KXBTC15M-26SEP230000', 'status':'active', 'exchange_index':2}]
        opener.open.side_effect = [Response({'series':{'exchange_index':2}}),
                                     Response({'markets':rows}), Response({'orderbook_fp':{}})]
        result = capture(PublicCapture(opener))
        self.assertEqual(result['requests'],3)
        self.assertIn('exchange_index=2',opener.open.call_args[0][0].full_url)
        self.assertEqual(len(result['books'][0]['sha256']),64)
        self.assertFalse(result['paper_admitted'])

    def test_failures_are_saved_without_retry(self):
        opener = Mock()
        opener.open.side_effect = RuntimeError('sensitive transport detail')
        result = capture(PublicCapture(opener))
        self.assertEqual(result['requests'],2)
        self.assertEqual(result['markets']['status'],'failed')
        self.assertNotIn('sensitive',json.dumps(result))

    def test_endpoint_and_budget(self):
        opener = Mock()
        opener.open.return_value = Response({})
        client = PublicCapture(opener)
        with self.assertRaises(ValueError): client.fetch('/portfolio/orders')
        for _ in range(4): client.fetch('/markets')
        with self.assertRaises(ValueError): client.fetch('/markets')
        self.assertEqual(opener.open.call_count,4)

if __name__ == '__main__': unittest.main()
