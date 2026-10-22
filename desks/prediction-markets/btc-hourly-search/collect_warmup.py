from pathlib import Path
from datetime import datetime,timezone
import json,hashlib,time,urllib.request,urllib.parse
R=Path(__file__).resolve().parent; p=R.parent/'btc-stochastic-search/data/candles.json'
c={r[0]:r for r in json.loads(p.read_text())}; lo=int(datetime(2026,4,10,tzinfo=timezone.utc).timestamp());hi=min(c)
for i,start in enumerate(range(lo,hi,250*300)):
 end=min(start+250*300,hi);fn=R/'data'/f'warmup-{i:02d}.json'
 if fn.exists():data=json.loads(fn.read_text())['data']
 else:
  u='https://api.exchange.coinbase.com/products/BTC-USD/candles?'+urllib.parse.urlencode({'granularity':300,'start':datetime.fromtimestamp(start,timezone.utc).isoformat(),'end':datetime.fromtimestamp(end,timezone.utc).isoformat()})
  b=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"NeoRome-Research/1.0","Accept":"application/json"}),timeout=40).read();data=json.loads(b);fn.write_text(json.dumps({'url':u,'retrieved_at':datetime.now(timezone.utc).isoformat(),'sha256':hashlib.sha256(b).hexdigest(),'data':data}));time.sleep(.5)
 for row in data:
  if start<=row[0]<end:c.setdefault(row[0],row)
(R/'data/candles.json').write_text(json.dumps([c[t] for t in sorted(c)]));print('candles',len(c))
