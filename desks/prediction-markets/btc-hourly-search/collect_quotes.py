"""Fetch all evaluation-hour quote histories independently of indicator selection."""
from pathlib import Path
from datetime import datetime,timezone
import json,hashlib,time,urllib.parse,urllib.request,urllib.error,gzip
R=Path(__file__).resolve().parent;BASE='https://external-api.kalshi.com/trade-api/v2/'
def stamp(x):return datetime.strptime(x.replace('Z','+00:00'),'%Y-%m-%dT%H:%M:%S.%f%z' if '.' in x else '%Y-%m-%dT%H:%M:%S%z').timestamp()
def get(name,route,params):
 p=R/'data'/(name+'.json')
 if p.exists():return json.loads(p.read_text())['data']
 u=BASE+route+'?'+urllib.parse.urlencode(params)
 for attempt in range(4):
  try:
   req=urllib.request.Request(u,headers={'User-Agent':'NeoRome-Research/1.0','Accept-Encoding':'gzip'})
   with urllib.request.urlopen(req,timeout=45) as r:
    b=r.read()
    if r.headers.get('Content-Encoding')=='gzip':b=gzip.decompress(b)
   d=json.loads(b);p.write_text(json.dumps({'url':u,'retrieved_at':datetime.now(timezone.utc).isoformat(),'sha256':hashlib.sha256(b).hexdigest(),'data':d}));time.sleep(.6);return d
  except urllib.error.HTTPError as e:
   if e.code not in (429,500,502,503,504) or attempt==3:raise
   time.sleep([10,30,60][attempt])
prices={r[0]+300:r[4] for r in json.loads((R/'data/candles.json').read_text())};best={}
for p in sorted((R/'data').glob('markets-*.json')):
 d=json.loads(p.read_text())
 for m in d.get('data',{}).get('markets',[]):
  if not '2026-09-13T01:00:00Z'<=m['close_time']<='2026-10-18T00:00:00Z':continue
  end=stamp(m['close_time']);start=end-3600
  if end%3600 or m.get('result') not in ('yes','no') or m.get('strike_type')!='greater':continue
  if stamp(m['created_time'])>start or stamp(m['open_time'])>start:continue
  if start not in prices or m.get('floor_strike') is None:continue
  s=float(m['floor_strike']);v=float(m['expiration_value'])
  if (v>s)!=(m['result']=='yes') or stamp(m['settlement_ts'])<end:continue
  key=(abs(s-prices[start]),s,m['ticker'])
  if start not in best or key<best[start][0]:best[start]=(key,m)
markets=[best[t][1] for t in sorted(best)];assert len(markets)>750,len(markets)
(R/'data/quote-market-universe.json').write_text(json.dumps(markets))
print('quote universe',len(markets),flush=True)
series=get('series','series/KXBTCD',{'exchange_index':2})
fees=get('fee-history','series/fee_changes',{'series_ticker':'KXBTCD','show_historical':'true'})
events=get('event-fee-history','events/fee_changes',{'limit':1000});assert not events.get('cursor')
quotes={};groups=[];current=[]
for m in markets:
 candidate=current+[m];start=int(stamp(candidate[0]['close_time']))-3600;end=int(stamp(candidate[-1]['close_time']))-14*60
 if current and ((end-start)//60+1)*len(candidate)>9000:groups.append(current);current=[m]
 else:current=candidate
if current:groups.append(current)
assert len(groups)<=150,len(groups)
for i,group in enumerate(groups):
 start=int(stamp(group[0]['close_time']))-3600;end=int(stamp(group[-1]['close_time']))-14*60
 d=get(f'quotes-{i:03d}','markets/candlesticks',{'market_tickers':','.join(m['ticker'] for m in group),'start_ts':start,'end_ts':end,'period_interval':1,'exchange_index':2})
 for item in d['markets']:quotes[item['market_ticker']]=item['candlesticks']
 for m in group:quotes.setdefault(m['ticker'],[])
 if i%5==0:print('quotes',i,len(quotes),flush=True)
(R/'data/quotes.json').write_text(json.dumps(quotes));print('complete quotes',len(quotes),'groups',len(groups),flush=True)
