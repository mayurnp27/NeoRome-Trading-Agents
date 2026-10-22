from pathlib import Path
import csv,json,hashlib,time,urllib.parse,urllib.request,urllib.error
from datetime import datetime,timezone
R=Path(__file__).resolve().parent;D=R/'data';BASE='https://external-api.kalshi.com/trade-api/v2'
def get(name,route,params):
 path=D/(name+'.json')
 if path.exists():return json.loads(path.read_text())['data']
 url=BASE+route+('?'+urllib.parse.urlencode(params) if params else '')
 with urllib.request.urlopen(urllib.request.Request(url,headers={'User-Agent':'NeoRome-Research/1.0','Accept':'application/json'}),timeout=40) as r:
  raw=r.read(20_000_001);assert len(raw)<=20_000_000
  v={'url':url,'retrieved_at':datetime.now(timezone.utc).isoformat(),'status':r.status,'date':r.headers.get('Date'),'raw_sha256':hashlib.sha256(raw).hexdigest(),'data':json.loads(raw)}
 path.write_text(json.dumps(v));time.sleep(.4);return v['data']
def main():
 D.mkdir(exist_ok=True)
 get('series','/series/KXBTC15M',{'exchange_index':2})
 get('fee-history','/series/fee_changes',{'series_ticker':'KXBTC15M','show_historical':'true'})
 events=get('event-fee-history-00','/events/fee_changes',{'limit':1000})
 assert not events.get('cursor'), 'Fee history grew beyond the archived single page; retrieve remaining pages before replay.'
 rows=[r for r in csv.DictReader((R.parent/'btc-stochastic-search/matched-outcomes.csv').open()) if r['start']>='2026-09-13']
 quotes={}
 for i in range(0,len(rows),20):
  batch=rows[i:i+20];a=int(datetime.fromisoformat(batch[0]['start']).timestamp());b=int(datetime.fromisoformat(batch[-1]['end']).timestamp())
  data=get(f'quotes-{i//20:03d}','/markets/candlesticks',{'market_tickers':','.join(r['ticker'] for r in batch),'start_ts':a,'end_ts':b,'period_interval':1,'exchange_index':2})
  requested={r['ticker'] for r in batch}
  for m in data['markets']:
   key=m['market_ticker'];assert key in requested;assert key not in quotes
   quotes[key]=m['candlesticks']
  print('batch',i//20,'markets',len(data['markets']),'candles',sum(len(m['candlesticks']) for m in data['markets']),flush=True)
 (D/'quotes.json').write_text(json.dumps(quotes))
 print('complete',len(rows),len(quotes),flush=True)
if __name__=='__main__':
 try:main()
 except urllib.error.HTTPError as e:
  body=e.read().decode()
  (D/'last-http-error.json').write_text(json.dumps({'status':e.code,'url':e.url,'body':body}))
  print(e.code,body)
  raise SystemExit(1)
