"""Public cached Kalshi metadata; bounded sequential calls, no credentials/orders."""
from pathlib import Path
from datetime import datetime,timezone
import json,hashlib,time,gzip,urllib.request,urllib.parse,urllib.error
R=Path(__file__).resolve().parent/'data'; BASE='https://external-api.kalshi.com/trade-api/v2/'
START=int(datetime(2026,4,21,tzinfo=timezone.utc).timestamp()); END=int(datetime(2026,10,18,tzinfo=timezone.utc).timestamp())
def get(name,route,params):
 p=R/(name+'.json')
 if p.exists():return json.loads(p.read_text())['data']
 u=BASE+route+'?'+urllib.parse.urlencode(params)
 for attempt in range(4):
  try:
   with urllib.request.urlopen(urllib.request.Request(u,headers={'User-Agent':'NeoRome-Public-Research/1.0','Accept-Encoding':'gzip'}),timeout=45) as r:
    b=r.read(20000001);assert len(b)<=20000000
    if r.headers.get('Content-Encoding')=='gzip':b=gzip.decompress(b)
    d=json.loads(b);p.write_text(json.dumps({'url':u,'retrieved_at':datetime.now(timezone.utc).isoformat(),'sha256':hashlib.sha256(b).hexdigest(),'data':d}));time.sleep(.6);return d
  except urllib.error.HTTPError as e:
   if e.code not in (429,500,502,503,504):raise
   (R/(name+'-error.json')).write_text(json.dumps({'status':e.code,'at':datetime.now(timezone.utc).isoformat(),'attempt':attempt}))
   if attempt==3:raise
   time.sleep([10,30,60][attempt])
total=0; routes={}
for route in ('markets','historical/markets'):
 cursor='';done=False
 for page in range(450 if route=='markets' else 1000):
  params={'series_ticker':'KXBTCD','limit':1000}
  if route=='markets':params.update(status='settled',exchange_index=2,min_close_ts=START,max_close_ts=END)
  if cursor:params['cursor']=cursor
  d=get(route.replace('/','-')+f'-{page:03d}',route,params);batch=d['markets']
  assert all(m['ticker'].startswith('KXBTCD-') for m in batch)
  total+=len(batch)
  cursor=d.get('cursor','')
  lo=min((m['close_time'] for m in batch),default='');hi=max((m['close_time'] for m in batch),default='')
  if page%5==0:print(route,page,total,lo,hi,flush=True)
  if not cursor or (batch and hi<'2026-04-21T00:00:00Z'):done=True;break
 assert done,'Public request budget exhausted; dataset not complete'
 routes[route]={'pages':page+1,'complete':done,'last_page_close_range':[lo,hi]}
(R/'collection-complete.json').write_text(json.dumps({'records_with_possible_duplicates':total,'routes':routes,'completed_at':datetime.now(timezone.utc).isoformat()},indent=2))
print('complete',total,flush=True)
