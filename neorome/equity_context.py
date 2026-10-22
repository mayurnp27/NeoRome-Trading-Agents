"""Read-only Alpaca paper asset, clock and exchange-calendar context (Python 3.9+)."""
from __future__ import annotations
import argparse
from datetime import datetime,timedelta,timezone
import hashlib
import json
from pathlib import Path
import re
import urllib.error
import urllib.parse
import urllib.request
from zoneinfo import ZoneInfo
from .alpaca_paper import _keychain_value, KEYCHAIN_KEY_ID_SERVICE, KEYCHAIN_SECRET_SERVICE
from .equity_data import DataError,NoRedirect,stamp,symbol,EquityDataClient
from .jupiter_quotes import save_new

NY=ZoneInfo('America/New_York')
UTC=timezone.utc
PAPER_ORIGIN='https://paper-api.alpaca.markets'


def normalize_calendar(rows,start,end):
    if not isinstance(rows,list) or not rows or len(rows)>600:
        raise DataError('Missing or excessive calendar rows')
    result=[];prev=None
    for row in rows:
        if not isinstance(row,dict):raise DataError('Invalid calendar row')
        try:
            day=datetime.strptime(row['date'],'%Y-%m-%d').date()
            def session_time(value):
                if not isinstance(value,str) or not re.fullmatch(r'\d{2}:\d{2}',value):
                    raise ValueError()
                return datetime.combine(day,datetime.strptime(value,'%H:%M').time(),NY).astimezone(UTC)
            opened=session_time(row['open']);closed=session_time(row['close'])
        except (ValueError,TypeError,KeyError):
            raise DataError('Invalid calendar date or local session time') from None
        if not start<=day<=end or (prev is not None and day<=prev) or opened>=closed:
            raise DataError('Out-of-range or unordered calendar')
        result.append({'date':day.isoformat(),'open_at':opened.isoformat(),'close_at':closed.isoformat()})
        prev=day
    return result


class EquityContextClient:
    def __init__(self,opener=None,credentials=None,clock=None):
        self.opener=opener or urllib.request.build_opener(NoRedirect)
        self.credentials=credentials or _keychain_value
        self.clock=clock or (lambda:datetime.now(UTC))
        self.requests=0;self.keys=None
    def _get(self,path,params=None):
        if path not in ('/v2/calendar','/v2/clock') and not re.fullmatch('/v2/assets/[A-Z]{1,5}',path):
            raise DataError('Unsupported context path')
        if self.requests>=3:raise DataError('Context request budget exhausted')
        if self.keys is None:
            try:
                self.keys=[self.credentials(KEYCHAIN_KEY_ID_SERVICE),self.credentials(KEYCHAIN_SECRET_SERVICE)]
                if any(not isinstance(k,str) or not re.fullmatch(r'[\x21-\x7e]+',k) for k in self.keys):raise ValueError()
            except Exception:raise DataError('Context credentials unavailable') from None
        query='?'+urllib.parse.urlencode(params) if params else ''
        request=urllib.request.Request(PAPER_ORIGIN+path+query,headers={
            'APCA-API-KEY-ID':self.keys[0],'APCA-API-SECRET-KEY':self.keys[1],'Accept':'application/json'},method='GET')
        requested=self.clock();self.requests+=1
        try:
            with self.opener.open(request,timeout=20) as response:
                if response.status!=200:raise DataError('Context HTTP status not successful')
                raw=response.read(1_000_001)
        except urllib.error.HTTPError as exc:raise DataError('Context HTTP '+str(exc.code)) from None
        except DataError:raise
        except Exception:raise DataError('Context transport failure') from None
        if len(raw)>1_000_000:raise DataError('Context response too large')
        try:payload=json.loads(raw)
        except (ValueError,UnicodeError):raise DataError('Invalid context JSON') from None
        return payload,{'path':path,'parameters':params or {},'requested_at':requested.isoformat(),
            'received_at':self.clock().isoformat(),'response_sha256':hashlib.sha256(raw).hexdigest()}
    def observe(self,ticker='SPY'):
        ticker=symbol(ticker);now=self.clock();today=now.astimezone(NY).date();start=today-timedelta(days=500)
        raw_asset,asset_evidence=self._get('/v2/assets/'+ticker)
        if not isinstance(raw_asset,dict) or raw_asset.get('symbol')!=ticker:raise DataError('Asset symbol mismatch')
        asset={'symbol':ticker,'asset_class':raw_asset.get('class'),'status':raw_asset.get('status'),
            'tradable':raw_asset.get('tradable'),'fractionable':raw_asset.get('fractionable'),'exchange':raw_asset.get('exchange')}
        raw_calendar,cal_evidence=self._get('/v2/calendar',{'start':start.isoformat(),'end':today.isoformat()})
        sessions=normalize_calendar(raw_calendar,start,today)
        raw_clock,clock_evidence=self._get('/v2/clock')
        if not isinstance(raw_clock,dict) or type(raw_clock.get('is_open')) is not bool:raise DataError('Invalid market clock')
        clock={'timestamp':stamp(raw_clock.get('timestamp')).isoformat(),'is_open':raw_clock['is_open']}
        return {'source_kind':'observed','received_at':self.clock().isoformat(),'asset':asset,'clock':clock,
            'calendar_start':start.isoformat(),'calendar_end':today.isoformat(),'sessions':sessions,
            'evidence':[asset_evidence,cal_evidence,clock_evidence],'requests':self.requests,'order_mutations':0}


def collect_bundle(ticker='SPY'):
    context=EquityContextClient().observe(ticker)
    observation=EquityDataClient().observe(ticker)
    return {'schema_version':1,'source_kind':'observed','context':context,'observation':observation,
        'received_at':datetime.now(UTC).isoformat(),'order_mutations':0,'maximum_requests':5}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',required=True)
    args=parser.parse_args()
    try:
        path=Path(args.output)
        if path.exists() or path.is_symlink():raise DataError('Output exists')
        bundle=collect_bundle();save_new(path,bundle)
    except DataError as exc:parser.exit(1,str(exc)+'\n')
    print(json.dumps({'output':args.output,'sessions':len(bundle['context']['sessions']),
        'asset':bundle['context']['asset'],'clock':bundle['context']['clock'],'order_mutations':0}))

if __name__=='__main__':main()
