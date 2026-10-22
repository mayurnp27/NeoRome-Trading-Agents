"""Bounded read-only public collection, immutable raw caches, no account API."""
from pathlib import Path
from datetime import datetime,timezone
from concurrent.futures import ThreadPoolExecutor
import json,hashlib,time,gzip,urllib.request,urllib.parse,urllib.error
R=Path(__file__).resolve().parent;DATA=R/'data';BASE='https://external-api.kalshi.com/trade-api/v2/'
def stamp(s):return int(datetime.fromisoformat(s.replace('Z','+00:00')).timestamp())
def get(name,route,params,base=BASE):
 p=DATA/(name+'.json');url=base+route+'?'+urllib.parse.urlencode(params)
 if p.exists():
  cache=json.loads(p.read_text());assert cache['url']==url;return cache['data']
 for attempt in range(4):
  try:
   req=urllib.request.Request(url,headers={'User-Agent':'NeoRome-Public-Research/1.0','Accept-Encoding':'gzip','Accept':'application/json'})
   with urllib.request.urlopen(req,timeout=40) as res:
    b=res.read(20000001);assert len(b)<=20000000
    if res.headers.get('Content-Encoding')=='gzip':b=gzip.decompress(b)
   d=json.loads(b);p.write_text(json.dumps({'url':url,'retrieved_at':datetime.now(timezone.utc).isoformat(),'sha256':hashlib.sha256(b).hexdigest(),'data':d}));time.sleep(.25);return d
  except urllib.error.HTTPError as ex:
   if ex.code not in (429,500,502,503,504) or attempt==3:raise
   time.sleep([5,15,30][attempt])

def metadata():
 # Resume the same series traversal after the existing immutable Apr20 boundary.
 old=R.parent/'btc-hourly-search/data/historical-markets-516.json';cached=json.loads(old.read_text());cursor=cached['data']['cursor'];assert cursor
 pages=[];lo=stamp('2026-02-05T00:00:00Z');hi=stamp('2026-04-01T00:00:00Z');records={};ranges=[]
 for page in range(600):
  d=get(f'older-markets-{page:03d}','historical/markets',{'series_ticker':'KXBTCD','limit':1000,'cursor':cursor});batch=d['markets'];assert all(m['ticker'].startswith('KXBTCD-') for m in batch)
  times=[stamp(m['close_time']) for m in batch]
  for m,t in zip(batch,times):
   if lo<t<=hi:records[m['ticker']]=m
  ranges.append([min(times) if times else None,max(times) if times else None]);cursor=d.get('cursor','')
  if page%20==0:print('older metadata pages',page+1,'in-window records',len(records),'oldest',min((m['close_time'] for m in batch),default=''),flush=True)
  if not cursor or (times and max(times)<=lo):break
 else:raise RuntimeError('metadata request budget exhausted')
 (DATA/'older-markets.json').write_text(json.dumps(list(records.values())))
 cursor='';new={};newlo=stamp('2026-10-18T00:00:00Z');newhi=stamp('2026-10-18T22:00:00Z')
 for page in range(12):
  params={'series_ticker':'KXBTCD','limit':1000,'status':'settled','exchange_index':2,'min_close_ts':newlo,'max_close_ts':newhi}
  if cursor:params['cursor']=cursor
  d=get(f'new-markets-{page:02d}','markets',params)
  for m in d['markets']:
   if newlo<stamp(m['close_time'])<=newhi:new[m['ticker']]=m
  cursor=d.get('cursor','')
  if not cursor:break
 else:raise RuntimeError('new-day metadata budget exhausted')
 (DATA/'new-markets.json').write_text(json.dumps(list(new.values())))
 for name,route,params in [('series','series/KXBTCD',{'exchange_index':2}),('fee-history','series/fee_changes',{'series_ticker':'KXBTCD','show_historical':'true'}),('event-fee-history','events/fee_changes',{'limit':1000})]:get(name,route,params)
 (DATA/'metadata-complete.json').write_text(json.dumps({'older_records':len(records),'new_records':len(new),'resume_source':str(old),'resume_source_sha256':hashlib.sha256(old.read_bytes()).hexdigest(),'page_ranges':ranges,'older_exhausted_below_start':not cursor or ranges[-1][1]<=lo},indent=2))
 print('metadata complete',len(records),len(new),flush=True)

def candles():
 for label,first,last in [('older','2026-02-02T00:00:00Z','2026-04-01T00:00:00Z'),('new','2026-10-15T00:00:00Z','2026-10-18T22:00:00Z')]:
  lo=stamp(first);hi=stamp(last);allrows={}
  for i,start in enumerate(range(lo,hi,250*300)):
   end=min(start+250*300,hi);params={'granularity':300,'start':datetime.fromtimestamp(start,timezone.utc).isoformat(),'end':datetime.fromtimestamp(end,timezone.utc).isoformat()}
   d=get(f'{label}-candles-{i:03d}','products/BTC-USD/candles',params,'https://api.exchange.coinbase.com/')
   for row in d:
    if start<=row[0]<end:allrows[row[0]]=row
   if i%20==0:print(label,'candle pages',i+1,flush=True)
  (DATA/(label+'-candles.json')).write_text(json.dumps([allrows[t] for t in sorted(allrows)]));print(label,'candles complete',len(allrows),flush=True)
if __name__=='__main__':
 with ThreadPoolExecutor(max_workers=2) as pool:
  jobs=[pool.submit(metadata),pool.submit(candles)]
  for job in jobs:job.result()
