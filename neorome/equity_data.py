"""Bounded, read-only Alpaca IEX observations for equity shadow research."""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import re
import urllib.error
import urllib.parse
import urllib.request
from .alpaca_paper import _keychain_value, KEYCHAIN_KEY_ID_SERVICE, KEYCHAIN_SECRET_SERVICE
from .jupiter_quotes import save_new

UTC = timezone.utc
ORIGIN = 'https://data.alpaca.markets'

class DataError(Exception):
    pass

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise DataError('Market data redirect refused')


def stamp(value):
    try:
        # Provider timestamps use nanoseconds; Python 3.8 accepts microseconds.
        # Truncate (never round into the future) sub-microsecond precision.
        if not isinstance(value, str):
            raise ValueError()
        value = re.sub(r'(\.\d{6})\d+(?=Z|[+-]\d\d:\d\d$)', r'\1', value)
        result = datetime.fromisoformat(value.replace('Z','+00:00'))
    except (AttributeError, ValueError, TypeError):
        raise DataError('Invalid source timestamp') from None
    if result.tzinfo is None:
        raise DataError('Source timestamp requires timezone')
    return result.astimezone(UTC)


def number(value, *, positive=False):
    if type(value) not in (int,float,str):
        raise DataError('Invalid numeric source field')
    try:
        result = Decimal(str(value))
    except InvalidOperation:
        raise DataError('Invalid numeric source field') from None
    if not result.is_finite() or result < 0 or (positive and result == 0):
        raise DataError('Invalid numeric source field')
    return result


def symbol(value):
    if not isinstance(value,str) or not re.fullmatch('[A-Z]{1,5}',value):
        raise DataError('Only simple uppercase equity symbols supported')
    return value


class EquityDataClient:
    """Two requests maximum. No retries, redirects, broker calls or feed fallback."""
    def __init__(self, opener=None, credentials=None, clock=None):
        self.opener = opener or urllib.request.build_opener(NoRedirect)
        self.credentials = credentials or _keychain_value
        self.clock = clock or (lambda:datetime.now(UTC))
        self.requests = 0
        self._keys = None

    def _get(self, path, params):
        if self.requests >= 2:
            raise DataError('Two-request observation budget exhausted')
        if not (re.fullmatch('/v2/stocks/[A-Z]{1,5}/bars',path) or path == '/v2/stocks/quotes/latest'):
            raise DataError('Unsupported read-only data path')
        if params.get('feed') != 'iex':
            raise DataError('Only explicit IEX data allowed in this adapter')
        if self._keys is None:
            try:
                keys = [self.credentials(KEYCHAIN_KEY_ID_SERVICE), self.credentials(KEYCHAIN_SECRET_SERVICE)]
                if any(not isinstance(k,str) or not re.fullmatch(r'[\x21-\x7e]+',k) for k in keys):
                    raise ValueError()
                self._keys = keys
            except Exception:
                raise DataError('Market data credentials unavailable') from None
        request = urllib.request.Request(ORIGIN+path+'?'+urllib.parse.urlencode(params),
            headers={'APCA-API-KEY-ID':self._keys[0], 'APCA-API-SECRET-KEY':self._keys[1], 'Accept':'application/json'},method='GET')
        requested = self.clock()
        self.requests += 1
        try:
            with self.opener.open(request, timeout=30) as response:
                if response.status != 200:
                    raise DataError('Market data returned non-success status')
                raw = response.read(2_000_001)
        except urllib.error.HTTPError as exc:
            raise DataError('Market data HTTP '+str(exc.code)) from None
        except DataError:
            raise
        except Exception:
            raise DataError('Market data transport failure') from None
        if len(raw)>2_000_000:
            raise DataError('Market data response too large')
        try:
            payload=json.loads(raw)
        except (ValueError,UnicodeError):
            raise DataError('Invalid market data JSON') from None
        if not isinstance(payload,dict):
            raise DataError('Invalid market data object')
        return payload, {'provider':'alpaca','feed':'iex','path':path,'parameters':params,
            'requested_at':requested.isoformat(),'received_at':self.clock().isoformat(),
            'response_sha256':hashlib.sha256(raw).hexdigest()}

    def observe(self, ticker='SPY'):
        ticker=symbol(ticker)
        now=self.clock()
        if now.tzinfo is None:
            raise DataError('Clock must be timezone aware')
        # Exclude the current UTC day, avoiding an in-progress daily bar.
        end=now.astimezone(UTC).replace(hour=0,minute=0,second=0,microsecond=0)
        start=end-timedelta(days=500)
        raw_bars, bars_evidence=self._get('/v2/stocks/'+ticker+'/bars',{
            'timeframe':'1Day','start':start.isoformat(),'end':end.isoformat(),
            'adjustment':'all','feed':'iex','limit':1000,'sort':'asc'})
        bars=normalize_bars(raw_bars,ticker,start,end)
        raw_quote, quote_evidence=self._get('/v2/stocks/quotes/latest',{'symbols':ticker,'feed':'iex'})
        quote=normalize_quote(raw_quote,ticker)
        received=self.clock()
        result={'schema_version':1,'source_kind':'observed','symbol':ticker,'feed':'iex',
            'observed_at':received.isoformat(),'bars':bars,'quote':quote,
            'evidence':[bars_evidence,quote_evidence], 'request_count':self.requests,
            'broker_orders':0,'execution_enabled':False}
        result['shadow_decision']=shadow_decision(result,received)
        return result


def normalize_bars(payload,ticker,start,end):
    if payload.get('symbol') != ticker or payload.get('next_page_token'):
        raise DataError('Mismatched symbol or incomplete bar pagination')
    rows=payload.get('bars')
    if not isinstance(rows,list) or not rows or len(rows)>1000:
        raise DataError('Missing or oversized daily-bar series')
    result=[];previous=None
    for row in rows:
        if not isinstance(row,dict):raise DataError('Invalid daily bar')
        t=stamp(row.get('t'))
        if t<start or t>=end or (previous is not None and t<=previous):
            raise DataError('Unordered, duplicate or out-of-range daily bars')
        prices={k:number(row.get(k),positive=True) for k in ('o','h','l','c')}
        if prices['l']>min(prices['o'],prices['c']) or prices['h']<max(prices['o'],prices['c']) or prices['l']>prices['h']:
            raise DataError('Inconsistent OHLC bar')
        volume=number(row.get('v'),positive=True)
        result.append({'timestamp':t.isoformat(),**{k:str(v) for k,v in prices.items()},'volume':str(volume)})
        previous=t
    return result


def normalize_quote(payload,ticker):
    quotes=payload.get('quotes')
    if not isinstance(quotes,dict) or ticker not in quotes or not isinstance(quotes[ticker],dict):
        raise DataError('Missing requested quote')
    row=quotes[ticker]
    # Zero/one-sided/crossed quotes are retained as non-executable evidence.
    return {'timestamp':stamp(row.get('t')).isoformat(),
            'bid':str(number(row.get('bp'))),'ask':str(number(row.get('ap'))),
            'bid_size':str(number(row.get('bs'))),'ask_size':str(number(row.get('as')))}


def shadow_decision(observation,now):
    """Diagnostic for the predeclared SPY 200-session comparator, never an order."""
    reasons=[];bars=observation['bars'];quote=observation['quote']
    if observation['symbol'] != 'SPY':
        reasons.append('Only the predeclared SPY comparator is evaluated by this path')
    if len(bars)<200:
        reasons.append('Fewer than 200 daily observations')
        trend=None;mean=None
    else:
        mean=sum(Decimal(b['c']) for b in bars[-200:])/200
        trend=Decimal(bars[-1]['c'])>mean
    age=(now-stamp(quote['timestamp'])).total_seconds()
    if age<0:reasons.append('Quote timestamp is in the future')
    elif age>300:reasons.append('Quote is stale for a prospective paper entry')
    bid,ask=Decimal(quote['bid']),Decimal(quote['ask'])
    if bid<=0 or ask<=0 or bid>ask:reasons.append('Quote is one-sided or crossed')
    if Decimal(quote['bid_size'])<=0 or Decimal(quote['ask_size'])<=0:reasons.append('Quote depth is unavailable')
    if bars and (now-stamp(bars[-1]['timestamp'])).total_seconds()>7*86400:
        reasons.append('Daily history is stale')
    if bars and stamp(quote['timestamp'])<=stamp(bars[-1]['timestamp'])+timedelta(days=1):
        reasons.append('Quote does not follow the completed signal bar')
    # No synthetic assertion of asset eligibility, fractional execution, fees or session schedule.
    reasons.extend(['Monthly rebalance calendar and asset eligibility not integrated',
                    'Fractional execution and complete fee model not verified',
                    'IEX quotes are single-venue observations, not guaranteed fills'])
    return {'strategy':'EQ-ETF-TREND-COMPARATOR-v1','status':'shadow_only_no_order',
        'eligible_for_execution':False,'trend_above_200_observation_mean':trend,
        'mean_adjusted_close':str(mean) if mean is not None else None,
        'signal_last_bar':bars[-1]['timestamp'] if bars else None,
        'quote_age_seconds':age,'reasons':reasons,
        'bar_count':len(bars),'paper_sleeve_cents':30000,
        'market_session_calendar_verified':False,'profitability_validated':False}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--symbol',default='SPY',choices=['SPY','MU'])
    parser.add_argument('--output',required=True)
    args=parser.parse_args()
    try:
        if Path(args.output).exists() or Path(args.output).is_symlink():
            raise DataError("Output already exists; choose a new observation file")
        result=EquityDataClient().observe(args.symbol)
        save_new(Path(args.output),result)
    except DataError as exc:
        parser.exit(1,str(exc)+'\n')
    print(json.dumps({'output':args.output,'bars':len(result['bars']),
        'status':result['shadow_decision']['status'],'requests':result['request_count'],'broker_orders':0}))

if __name__=='__main__':main()
