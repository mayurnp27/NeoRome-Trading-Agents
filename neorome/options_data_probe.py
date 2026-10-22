"""Two-request read-only options feed check. Does not establish executable fills."""
import argparse
from datetime import datetime, timezone, timedelta
import hashlib
import json
import urllib.request
import urllib.parse
import urllib.error
from .alpaca_paper import _keychain_value, KEYCHAIN_KEY_ID_SERVICE, KEYCHAIN_SECRET_SERVICE
from .equity_data import NoRedirect
from .jupiter_quotes import save_new


def probe(opener=None, credentials=_keychain_value, now=None):
    now=now or datetime.now(timezone.utc)
    opener=opener or urllib.request.build_opener(NoRedirect)
    keys=[credentials(KEYCHAIN_KEY_ID_SERVICE),credentials(KEYCHAIN_SECRET_SERVICE)]
    if any(not isinstance(k,str) or not k or any(ord(c)<33 or ord(c)>126 for c in k) for k in keys):
        raise ValueError('Credentials unavailable or invalid')
    rows=[]
    for feed in ('opra','indicative'):
        params={'feed':feed,'limit':2,'type':'call',
                'expiration_date_gte':(now.date()+timedelta(days=30)).isoformat(),
                'expiration_date_lte':(now.date()+timedelta(days=60)).isoformat()}
        url='https://data.alpaca.markets/v1beta1/options/snapshots/SPY?'+urllib.parse.urlencode(params)
        row={'requested_feed':feed,'url':url,'requested_at':datetime.now(timezone.utc).isoformat()}
        request=urllib.request.Request(url,headers={'APCA-API-KEY-ID':keys[0],'APCA-API-SECRET-KEY':keys[1]},method='GET')
        try:
            with opener.open(request,timeout=15) as response:
                raw=response.read(1_000_001);row['http_status']=response.status
            if len(raw)>1_000_000:raise ValueError('Response too large')
            if row['http_status']!=200:raise ValueError('Unexpected status')
            body=json.loads(raw);snapshots=body.get('snapshots')
            if not isinstance(snapshots,dict):raise ValueError('Invalid snapshots')
            row.update(response_sha256=hashlib.sha256(raw).hexdigest(),raw_response=body,
                       contract_count=len(snapshots),pagination_pending=bool(body.get('next_page_token')),
                       access_result='response_received' if snapshots else 'empty_response',
                       executable_quote_quality_verified=False)
        except urllib.error.HTTPError as exc:
            row.update(http_status=exc.code,access_result='http_rejected')
        except Exception as exc:
            row.update(access_result='failed',error_type=type(exc).__name__)
        row['received_at']=datetime.now(timezone.utc).isoformat();rows.append(row)
    return {'mode':'read_only_feed_probe','requests':2,'results':rows,
            'orders_sent':0,'paper_eligible':False,'historical_access_verified':False,
            'note':'A successful response proves endpoint access only. Indicative quotes are modified; no executable spread or fill is established.'}

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',required=True)
    args=parser.parse_args()
    from pathlib import Path
    if Path(args.output).exists():parser.error('Output exists')
    r=probe();save_new(args.output,r)
    print(json.dumps({'output':args.output,'results':[{k:v for k,v in x.items() if k in ('requested_feed','http_status','contract_count','access_result')} for x in r['results']],'orders_sent':0}))
