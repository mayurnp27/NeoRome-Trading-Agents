import json
import os
import tempfile
import unittest
from unittest.mock import Mock, patch
import urllib.error

from neorome.crypto_research import Sources
from neorome.jupiter_quotes import QuoteError
from neorome.discovery_cache import GeckoDiscoveryCache


GECKO_TRENDING = 'https://api.geckoterminal.com/api/v2/networks/solana/trending_pools?include=base_token,quote_token&page=1'


class Clock:
    def __init__(self, value=100.0):
        self.value = value

    def __call__(self):
        return self.value


class Response:
    def __init__(self, body, status=200):
        self.body = body
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self, limit):
        return self.body


class DiscoveryCacheTests(unittest.TestCase):
    def test_independent_sources_share_fresh_response_and_provenance(self):
        body = json.dumps({'data': [{'id': 'solana_pool'}]}).encode()
        opener = Mock()
        opener.open.return_value = Response(body)
        clock = Clock()
        with tempfile.TemporaryDirectory() as directory:
            first = Sources(opener, discovery_cache=GeckoDiscoveryCache(directory, clock=clock))
            second = Sources(opener, discovery_cache=GeckoDiscoveryCache(directory, clock=clock))

            first_result = first.trending_pools()
            second_result = second.trending_pools()

        self.assertEqual(opener.open.call_count, 1)
        self.assertEqual(second_result, first_result)
        self.assertEqual(first_result[1]['requested_at'], second_result[1]['requested_at'])
        self.assertEqual(first_result[1]['received_at'], second_result[1]['received_at'])
        self.assertEqual(first_result[1]['response_sha256'], second_result[1]['response_sha256'])

    def test_expired_response_is_not_substituted_during_shared_429_cooldown(self):
        body = json.dumps({'data': []}).encode()
        opener = Mock()
        opener.open.side_effect = [
            Response(body),
            urllib.error.HTTPError(GECKO_TRENDING, 429, 'too many requests', {}, None),
            Response(json.dumps({'data': [{'id': 'solana_new'}]}).encode()),
        ]
        clock = Clock()
        with tempfile.TemporaryDirectory() as directory:
            first = Sources(opener, discovery_cache=GeckoDiscoveryCache(directory, clock=clock))
            second = Sources(opener, discovery_cache=GeckoDiscoveryCache(directory, clock=clock))
            first.trending_pools()

            clock.value += 31
            with self.assertRaisesRegex(QuoteError, 'HTTP 429'):
                first.trending_pools()

            clock.value += 30
            with self.assertRaisesRegex(QuoteError, 'HTTP 429'):
                second.trending_pools()
            self.assertEqual(opener.open.call_count, 2)

            clock.value += 30
            result = second.trending_pools()

        self.assertEqual(opener.open.call_count, 3)
        self.assertEqual(result[0]['data'][0]['id'], 'solana_new')

    def test_cache_does_not_cover_dexscreener_or_rpc_reads(self):
        opener = Mock()
        opener.open.side_effect = [Response(b'[]'), Response(b'[]')]
        with tempfile.TemporaryDirectory() as directory:
            source = Sources(opener, discovery_cache=GeckoDiscoveryCache(directory))
            source.profiles()
            source.profiles()
        self.assertEqual(opener.open.call_count, 2)

    def test_environment_variable_enables_cache(self):
        opener = Mock()
        opener.open.return_value = Response(b'{"data": []}')
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
                'NEOROME_DISCOVERY_CACHE_DIR': directory}):
            source = Sources(opener)
            source.trending_pools()
            source.trending_pools()
        self.assertEqual(opener.open.call_count, 1)


if __name__ == '__main__':
    unittest.main()
