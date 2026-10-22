"""Public Polymarket hourly outcomes and resolution-source Binance OHLC."""
from pathlib import Path
from datetime import datetime,timezone
import urllib.request,urllib.parse,urllib.error,json,hashlib,time,gzip
R=Path(__file__).resolve().parent/'data'
def get(name,base,params):
 p=R/(name+'.json')
 if p.exists():return json.loads(p.read_text())['data']
 u=base+'?'+urllib.parse.urlencode(params)
 for attempt in range(4):
  try:
   with urllib.request.urlopen(urllib.request.Request(u,headers={'User-Agent':'NeoRome-Research/1.0','Accept-Encoding':'gzip'}),timeout=40) as r:
    b=r.read()
    if r.headers.get('Content-Encoding')=='gzip':b=gzip.decompress(b)
   d=json.loads(b);p.write_text(json.dumps({'url':u,'retrieved_at':datetime.now(timezone.utc).isoformat(),'sha256':hashlib.sha256(b).hexdigest(),'data':d}));time.sleep(.4);return d
  except urllib.error.HTTPError as e:
   if e.code not in (429,500,502,503,504) or attempt==3:raise
   time.sleep([10,30,60][attempt])
events={};date_min='2026-04-21T00:00:00Z'
for page in range(60):
 d=get(f'poly-events-{page:03d}','https://gamma-api.polymarket.com/events',{'series_id':'10114','closed':'true','end_date_min':date_min,'end_date_max':'2026-10-18T00:00:00Z','order':'endDate','ascending':'true','limit':100,'offset':page*100 if page<=20 else 0})
 assert isinstance(d,list) and all(any(s['id']=='10114' for s in e.get('series',[])) for e in d)
 for e in d:events[e['id']]=e
 if page>=20 and d:
  from datetime import timedelta
  date_min=(datetime.fromisoformat(max(e['endDate'] for e in d).replace('Z','+00:00'))+timedelta(seconds=1)).isoformat()
 if page%5==0:print('events',page,len(events),flush=True)
 if len(d)<100:break
else:raise RuntimeError('Event budget exhausted')
(R/'polymarket-events.json').write_text(json.dumps(list(events.values())))
start=int(datetime(2026,4,10,tzinfo=timezone.utc).timestamp()*1000);end=int(datetime(2026,10,18,tzinfo=timezone.utc).timestamp()*1000);candles={}
for page,lo in enumerate(range(start,end,1000*300000)):
 hi=min(end,lo+1000*300000)
 d=get(f'binance-{page:03d}','https://data-api.binance.vision/api/v3/klines',{'symbol':'BTCUSDT','interval':'5m','startTime':lo,'endTime':hi-1,'limit':1000})
 assert isinstance(d,list)
 for row in d:
  if lo<=row[0]<hi:candles[row[0]//1000]=[row[0]//1000,float(row[3]),float(row[2]),float(row[1]),float(row[4]),float(row[5])]
 if page%10==0:print('binance',page,len(candles),flush=True)
(R/'binance-candles.json').write_text(json.dumps([candles[t] for t in sorted(candles)]));print('complete',len(events),len(candles),flush=True)
