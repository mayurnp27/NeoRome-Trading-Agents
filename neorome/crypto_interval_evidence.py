"""Finite public BTC-interval metadata/book capture. No trading or scheduler."""
import argparse
import base64
from datetime import datetime, timezone
import hashlib
import json
import re
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from .jupiter_quotes import NoRedirect, save_new

ORIGIN = 'https://external-api.kalshi.com/trade-api/v2'
SERIES = 'KXBTC15M'


class PublicCapture:
    def __init__(self, opener=None, clock=None):
        self.opener = opener or urllib.request.build_opener(NoRedirect)
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.attempts = 0

    def fetch(self, path, params=None):
        if not (path == '/markets' or path == '/series/'+SERIES or
                re.fullmatch(r'/markets/KXBTC15M-[A-Z0-9-]+/orderbook', path)):
            raise ValueError('endpoint not permitted')
        if self.attempts >= 4:
            raise ValueError('capture request budget exhausted')
        self.attempts += 1
        url = ORIGIN + path + ('?' + urllib.parse.urlencode(params) if params else '')
        record = {'path': path, 'params': params or {}, 'requested_at': self.clock().isoformat()}
        try:
            req = urllib.request.Request(url, headers={'Accept':'application/json'}, method='GET')
            with self.opener.open(req, timeout=15) as response:
                if response.status != 200:
                    raise ValueError('non-success status')
                raw = response.read(2000001)
            if len(raw) > 2000000:
                raise ValueError('response too large')
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                raise ValueError('object response required')
            record.update(received_at=self.clock().isoformat(), payload=payload,
                          sha256=hashlib.sha256(raw).hexdigest(),
                          raw_b64=base64.b64encode(raw).decode('ascii'), status='captured')
        except urllib.error.HTTPError as exc:
            record.update(status='failed', error='HTTP '+str(exc.code))
            exc.close()
        except Exception:
            record.update(status='failed', error='transport or schema failure')
        return record


def capture(client):
    series = client.fetch('/series/'+SERIES)
    params = {'series_ticker': SERIES, 'status':'open', 'limit':20}
    # Obtain engine identity from the series response rather than assuming it.
    meta = series.get('payload', {}).get('series', {})
    engine = meta.get('exchange_index') if isinstance(meta, dict) else None
    if type(engine) is int and engine >= 0:
        params['exchange_index'] = engine
    markets = client.fetch('/markets', params)
    rows = markets.get('payload', {}).get('markets', [])
    selected = []
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            ticker = row.get('ticker', '')
            if (isinstance(ticker, str) and re.fullmatch(r'KXBTC15M-[A-Z0-9-]+', ticker)
                    and row.get('status') == 'active'):
                selected.append(row)
    selected.sort(key=lambda row: (str(row.get('close_time','')), row['ticker']))
    books = []
    for row in selected[:2]:
        query = {'depth':10}
        if type(row.get('exchange_index')) is int:
            query['exchange_index'] = row['exchange_index']
        books.append(client.fetch('/markets/'+row['ticker']+'/orderbook', query))
    return {'schema_version':1, 'kind':'crypto_interval_evidence', 'series':series,
            'markets':markets, 'books':books, 'requests':client.attempts,
            'execution_enabled':False, 'paper_admitted':False,
            'limitations':['bounded selection, not full market coverage',
                'HTTP receipt is not an exchange event timestamp',
                'no synchronized oracle stream or contract settlement verification',
                'no historical replay, calibration, or fill claim']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    if Path(args.output).exists():
        parser.error('output already exists')
    result = capture(PublicCapture())
    save_new(args.output, result)
    print(json.dumps({'requests':result['requests'], 'books':len(result['books']),
                      'market_status':result['markets']['status'], 'paper_admitted':False}))

if __name__ == '__main__': main()
