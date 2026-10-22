"""Bounded public quote collection; archived and current tiers, no account APIs."""
from pathlib import Path
from datetime import datetime,timezone
from concurrent.futures import ThreadPoolExecutor
import csv,json,time,threading,hashlib,urllib.request,urllib.parse,urllib.error

R=Path(__file__).resolve().parent; DATA=R/'data'; BASE='https://external-api.kalshi.com/trade-api/v2'
lock=threading.Lock(); next_request=0.; request_count=0

def ts(s):
    s=s.replace(' ','T').replace('Z','+00:00')
    return datetime.strptime(s,'%Y-%m-%dT%H:%M:%S.%f%z' if '.' in s else '%Y-%m-%dT%H:%M:%S%z').timestamp()

def get(name,route,params):
    global next_request,request_count
    path=DATA/(name+'.json')
    if path.exists():return json.loads(path.read_text())['data']
    url=BASE+route+'?'+urllib.parse.urlencode(params)
    for attempt in range(3):
        with lock:
            delay=max(0,next_request-time.monotonic());next_request=max(next_request,time.monotonic())+.4
            request_count+=1;assert request_count<=5100
        if delay:time.sleep(delay)
        try:
            with urllib.request.urlopen(urllib.request.Request(url,headers={'User-Agent':'NeoRome-Research/1.0','Accept':'application/json'}),timeout=30) as response:
                raw=response.read(5_000_001);assert len(raw)<=5_000_000
                envelope={'url':url,'status':response.status,'retrieved_at':datetime.now(timezone.utc).isoformat(),'date':response.headers.get('Date'),'raw_sha256':hashlib.sha256(raw).hexdigest(),'data':json.loads(raw)}
            path.write_text(json.dumps(envelope));return envelope['data']
        except urllib.error.HTTPError as e:
            error={'url':url,'status':e.code,'body':e.read(2000).decode(errors='replace')}
            (DATA/(name+'-error.json')).write_text(json.dumps(error))
            if e.code not in (429,500,502,503,504) or attempt==2:raise
            time.sleep(2**(attempt+1))

def one(m):
    key=m['ticker'];data=get('history-'+key,'/historical/markets/'+key+'/candlesticks',{'start_ts':int(ts(m['open_time'])),'end_ts':int(ts(m['close_time'])),'period_interval':1})
    assert data['ticker']==key
    return key,data['candlesticks']

def normalized(rows,tier):
    output=[]
    for r in rows:
        v={'end_period_ts':r['end_period_ts'],'source_tier':tier}
        for name in ('yes_bid','yes_ask'):
            v[name]={}
            for field in ('open','high','low','close'):
                value=r.get(name,{}).get(field+'_dollars',r.get(name,{}).get(field))
                if value is not None:assert 0<=float(value)<=1
                v[name][field+'_dollars']=value
        output.append(v)
    return output

def main():
    cutoff=ts(json.loads((DATA/'cutoff.json').read_text())['data']['market_settled_ts'])
    matches={r['ticker'] for r in csv.DictReader((R.parent/'btc-stochastic-search/matched-outcomes.csv').open()) if '2026-08-18'<=r['start']<'2026-09-12'}
    markets=sorted([m for m in json.loads((R.parent/'btc-stochastic-search/data/markets.json').read_text()) if m['ticker'] in matches],key=lambda m:ts(m['open_time']))
    historical=[m for m in markets if ts(m['settlement_ts'])<cutoff]; current=[m for m in markets if ts(m['settlement_ts'])>=cutoff]
    print('requested',len(markets),'historical',len(historical),'current',len(current),flush=True)
    quotes={}
    assert not historical, 'v2 only uses the complete current tier before final evaluation'
    for i in range(0,len(current),20):
        batch=current[i:i+20];start=int(ts(batch[0]['open_time']));end=int(ts(batch[-1]['close_time']))
        assert (end-start)//60*len(batch)<=10000
        data=get(f'current-{i//20:03d}','/markets/candlesticks',{'market_tickers':','.join(m['ticker'] for m in batch),'start_ts':start,'end_ts':end,'period_interval':1,'exchange_index':2})
        for m in data['markets']:
            key=m['market_ticker'];assert key not in quotes;quotes[key]=normalized(m['candlesticks'],'current')
        if i%200==0:print('current completed',i+len(batch),'of',len(current),flush=True)
    assert set(quotes)==matches
    assert request_count<=150
    (DATA/'quotes.json').write_text(json.dumps(quotes))
    print('complete',len(quotes),'empty',sum(not v for v in quotes.values()),'network requests this run',request_count,flush=True)

if __name__=='__main__':main()
