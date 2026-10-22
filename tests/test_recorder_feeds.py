import tempfile
import unittest
from datetime import datetime,timezone
from neorome.recorder import run_once

class FeedTests(unittest.TestCase):
    def test_crypto_identity_and_mix_refusal(self):
        now=datetime.now(timezone.utc)
        raw={'raw_response':{'quotes':{'BTC/USD':{'t':now.isoformat(),'bp':100000,'ap':100001,'bs':1,'as':1}}}}
        with tempfile.TemporaryDirectory() as p:
            self.assertEqual(run_once(p,lambda:raw,lambda:now,feed='btc_usd')['status'],'fresh')
            with self.assertRaises(ValueError):run_once(p,lambda:raw,lambda:now,feed='spy_iex')
    def test_crypto_does_not_accept_wrong_symbol(self):
        now=datetime.now(timezone.utc)
        with tempfile.TemporaryDirectory() as p:
            result=run_once(p,lambda:{'raw_response':{'quotes':{'SPY':{}}}},lambda:now,feed='btc_usd')
            self.assertEqual(result['status'],'fetch_or_validation_failed')

if __name__=='__main__':unittest.main()
