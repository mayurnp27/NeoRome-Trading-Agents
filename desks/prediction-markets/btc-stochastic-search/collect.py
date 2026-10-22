"""Bounded public GET collector. No credentials, orders, or runtime changes."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parent / 'data'
END = int(datetime(2026,10,18,tzinfo=timezone.utc).timestamp())
START = END - 180*86400

def get(name, base, params):
    path = ROOT / (name+'.json')
    if path.exists():
        return json.loads(path.read_text())['data']
    url = base+'?'+urllib.parse.urlencode(params)
    req = urllib.request.Request(url,headers={'User-Agent':'NeoRome-Research/1.0','Accept':'application/json'})
    with urllib.request.urlopen(req,timeout=30) as r:
        raw = r.read(15_000_001)
        assert len(raw)<=15_000_000
        record = {'url':url,'retrieved_at':datetime.now(timezone.utc).isoformat(),
                  'status':r.status,'date':r.headers.get('Date'),
                  'sha256':hashlib.sha256(raw).hexdigest(),'data':json.loads(raw)}
    path.write_text(json.dumps(record)+'\n')
    time.sleep(.35)
    return record['data']

def main():
    ROOT.mkdir(exist_ok=True)
    markets = {}
    for route in ('markets','historical/markets'):
        cursor = ''
        for page in range(25):
            params = {'series_ticker':'KXBTC15M','limit':1000}
            if route=='markets': params.update(status='settled',exchange_index=2)
            if cursor: params['cursor']=cursor
            data = get(route.replace('/','-')+f'-{page:02d}',
                       'https://external-api.kalshi.com/trade-api/v2/'+route,params)
            batch=data['markets']
            assert all(m['ticker'].startswith('KXBTC15M-') for m in batch)
            for m in batch: markets.setdefault(m['ticker'],m)
            print(route,page,len(batch),flush=True)
            cursor=data.get('cursor','')
            if not cursor or (batch and max(datetime.fromisoformat(m['close_time'].replace('Z','+00:00')).timestamp() for m in batch)<START): break
    (ROOT/'markets.json').write_text(json.dumps(list(markets.values())))
    candles={}
    for i,lo in enumerate(range(START-2*86400,END,250*300)):
        hi=min(lo+250*300,END)
        data=get(f'coinbase-{i:03d}','https://api.exchange.coinbase.com/products/BTC-USD/candles',
                 {'granularity':300,'start':datetime.fromtimestamp(lo,timezone.utc).isoformat(),
                  'end':datetime.fromtimestamp(hi,timezone.utc).isoformat()})
        assert isinstance(data,list)
        for row in data:
            if lo<=row[0]<hi: candles[row[0]]=row
        if i%10==0: print('coinbase',i,len(candles),flush=True)
    (ROOT/'candles.json').write_text(json.dumps([candles[t] for t in sorted(candles)]))
    print('complete',len(markets),len(candles),flush=True)

if __name__=='__main__': main()
