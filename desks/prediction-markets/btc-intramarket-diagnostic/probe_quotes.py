import csv,json,hashlib,urllib.request,urllib.parse
from datetime import datetime,timezone
from pathlib import Path
R=Path(__file__).resolve().parent
m=list(csv.DictReader((R.parent/'btc-stochastic-search/matched-outcomes.csv').open()))[-1]
a=int(datetime.fromisoformat(m['start']).timestamp());b=a+900
url='https://external-api.kalshi.com/trade-api/v2/series/KXBTC15M/markets/'+urllib.parse.quote(m['ticker'])+'/candlesticks?'+urllib.parse.urlencode({'start_ts':a,'end_ts':b,'period_interval':1,'exchange_index':2})
with urllib.request.urlopen(urllib.request.Request(url,headers={'User-Agent':'NeoRome-Research/1.0'}),timeout=25) as r:
 raw=r.read(2000000);capture={'url':url,'status':r.status,'retrieved_at':datetime.now(timezone.utc).isoformat(),'sha256':hashlib.sha256(raw).hexdigest(),'data':json.loads(raw)}
(R/'quote-access-probe.json').write_text(json.dumps(capture,indent=2))
print(json.dumps({'status':capture['status'],'ticker':m['ticker'],'candles':len(capture['data'].get('candlesticks',[])),'quote_field_keys':list(capture['data'].get('candlesticks',[{}])[0].keys())}))
