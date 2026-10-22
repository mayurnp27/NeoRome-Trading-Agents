"""Stage-one SPY IEX recorder. One read-only request per run; no trading path."""
import argparse
from datetime import datetime, timezone
from decimal import Decimal
import fcntl
import hashlib
import json
import os
from pathlib import Path
import urllib.request
import urllib.parse
import sqlite3

from .equity_data import EquityDataClient, normalize_quote, stamp, NoRedirect
from .alpaca_paper import _keychain_value, KEYCHAIN_KEY_ID_SERVICE, KEYCHAIN_SECRET_SERVICE

MAX_DAILY_ATTEMPTS = 1440
MAX_HOURLY_ATTEMPTS = 60
MIN_INTERVAL_SECONDS = 60
MAX_STATE_BYTES = 256 * 1024 * 1024


def credentials(service):
    names = {KEYCHAIN_KEY_ID_SERVICE:'NEOROME_ALPACA_PAPER_KEY_ID',
             KEYCHAIN_SECRET_SERVICE:'NEOROME_ALPACA_PAPER_SECRET_KEY'}
    if os.environ.get('NEOROME_CREDENTIAL_SOURCE') == 'environment':
        value = os.environ.get(names[service])
        if not value: raise ValueError('Missing recorder credential')
        return value
    if os.environ.get('NEOROME_CREDENTIAL_SOURCE', 'keychain') != 'keychain':
        raise ValueError('Unknown credential source')
    return _keychain_value(service)


def fetch_quote():
    client = EquityDataClient(credentials=credentials)
    raw, evidence = client._get('/v2/stocks/quotes/latest', {'symbols':'SPY','feed':'iex'})
    return {'raw_response':raw, 'evidence':evidence}


def fetch_crypto_quote():
    """One fixed Alpaca US BTC/USD quote request; no exchange/order endpoint."""
    url = 'https://data.alpaca.markets/v1beta3/crypto/us/latest/quotes?symbols=BTC%2FUSD'
    key = credentials(KEYCHAIN_KEY_ID_SERVICE)
    secret = credentials(KEYCHAIN_SECRET_SERVICE)
    requested = datetime.now(timezone.utc).isoformat()
    req = urllib.request.Request(url, headers={'APCA-API-KEY-ID':key,
        'APCA-API-SECRET-KEY':secret, 'Accept':'application/json'}, method='GET')
    with urllib.request.build_opener(NoRedirect).open(req, timeout=30) as response:
        raw = response.read(2_000_001)
        if response.status != 200 or len(raw) > 2_000_000:
            raise ValueError('Invalid crypto response')
    return {'raw_response':json.loads(raw), 'evidence':{'provider':'alpaca','feed':'us',
        'symbol':'BTC/USD', 'requested_at':requested,
        'received_at':datetime.now(timezone.utc).isoformat(),
        'response_sha256':hashlib.sha256(raw).hexdigest()}}


def health(state, now=None):
    now = now or datetime.now(timezone.utc)
    path = Path(state)/'recorder.sqlite3'
    if not path.exists(): return {'status':'not_started','execution_enabled':False}
    with sqlite3.connect('file:'+str(path.resolve())+'?mode=ro', uri=True) as db:
        row = db.execute('SELECT started_at,status,detail FROM attempts ORDER BY id DESC LIMIT 1').fetchone()
    if not row: return {'status':'not_started','execution_enabled':False}
    age = (now-stamp(row[0])).total_seconds()
    return {'status': 'heartbeat_stale' if age > 180 or age < 0 else row[1],
            'last_attempt_at':row[0], 'age_seconds':age, 'detail':row[2],
            'execution_enabled':False}


def run_once(state, fetch=fetch_quote, clock=lambda:datetime.now(timezone.utc), *, feed='spy_iex'):
    """Persist attempt BEFORE I/O: crashes/restarts cannot reset request budgets."""
    if feed not in ('spy_iex', 'btc_usd'): raise ValueError('Unsupported recorder feed')
    ticker, source = ('SPY','alpaca_iex') if feed=='spy_iex' else ('BTC/USD','alpaca_us')
    os.umask(0o077)
    root = Path(state)
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if root.is_symlink(): raise ValueError('Symlink state directory refused')
    with (root/'recorder.lock').open('a') as lock:
        try: fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError: return {'status':'already_running','execution_enabled':False}
        size = sum(p.stat().st_size for p in root.iterdir() if p.is_file())
        if size >= MAX_STATE_BYTES: return {'status':'storage_limit','execution_enabled':False}
        db = sqlite3.connect(root/'recorder.sqlite3')
        try:
            db.execute('PRAGMA synchronous=FULL')
            db.execute('CREATE TABLE IF NOT EXISTS attempts (id INTEGER PRIMARY KEY, started_at TEXT NOT NULL, status TEXT NOT NULL, detail TEXT, observation TEXT, sha256 TEXT)')
            db.execute('CREATE TABLE IF NOT EXISTS recorder_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
            stored = db.execute("SELECT value FROM recorder_metadata WHERE key='feed'").fetchone()
            if stored and stored[0] != feed: raise ValueError('Cannot mix feeds in one state directory')
            if not stored and feed != 'spy_iex' and db.execute('SELECT count(*) FROM attempts').fetchone()[0]:
                raise ValueError('Legacy recorder state belongs to SPY')
            db.execute("INSERT OR IGNORE INTO recorder_metadata VALUES ('feed',?)",(feed,))
            db.commit()
            now = clock(); started = now.astimezone(timezone.utc).isoformat()
            previous = db.execute('SELECT started_at FROM attempts ORDER BY id DESC LIMIT 1').fetchone()
            if previous and (now-stamp(previous[0])).total_seconds() < MIN_INTERVAL_SECONDS:
                return {'status':'interval_limited','execution_enabled':False}
            daily = db.execute('SELECT count(*) FROM attempts WHERE substr(started_at,1,10)=?',(started[:10],)).fetchone()[0]
            hourly = db.execute('SELECT count(*) FROM attempts WHERE substr(started_at,1,13)=?',(started[:13],)).fetchone()[0]
            if daily >= MAX_DAILY_ATTEMPTS or hourly >= MAX_HOURLY_ATTEMPTS:
                return {'status':'request_budget_exhausted','execution_enabled':False}
            cursor = db.execute('INSERT INTO attempts(started_at,status) VALUES (?,?)',(started,'incomplete'))
            identifier = cursor.lastrowid
            db.commit()
            observation = None; digest = None
            try:
                data = fetch()
                encoded = json.dumps(data, sort_keys=True, separators=(',',':'), allow_nan=False)
                if len(encoded.encode()) > 2_100_000: raise ValueError('Oversized observation')
                observation = encoded; digest = hashlib.sha256(encoded.encode()).hexdigest()
                quote = normalize_quote(data['raw_response'],ticker)
                receipt = clock()
                age = (receipt-stamp(quote['timestamp'])).total_seconds()
                bid, ask = Decimal(quote['bid']), Decimal(quote['ask'])
                valid = 0 < bid <= ask and Decimal(quote['bid_size'])>0 and Decimal(quote['ask_size'])>0
                status = 'fresh' if valid and 0 <= age <= 60 else 'stale_or_unusable'
                detail = json.dumps({'source':source,'symbol':ticker,'quote_age_seconds':age,
                                     'received_at':receipt.isoformat(),'execution_enabled':False})
            except Exception as exc:
                status = 'fetch_or_validation_failed'
                # Exceptions can contain URLs/credentials: never persist their messages.
                detail = type(exc).__name__
            db.execute('UPDATE attempts SET status=?,detail=?,observation=?,sha256=? WHERE id=?',
                       (status,detail,observation,digest,identifier))
            db.commit()
            return {'status':status,'attempt_id':identifier,'execution_enabled':False,'orders_sent':0}
        finally: db.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state',required=True)
    parser.add_argument('--health',action='store_true')
    parser.add_argument('--feed',choices=['spy_iex','btc_usd'],default='spy_iex')
    args=parser.parse_args()
    result=health(args.state) if args.health else run_once(args.state, fetch=fetch_quote if args.feed=='spy_iex' else fetch_crypto_quote, feed=args.feed)
    print(json.dumps(result))
    return 1 if result['status'] in ('fetch_or_validation_failed','storage_limit','heartbeat_stale') else 0

if __name__=='__main__': raise SystemExit(main())
