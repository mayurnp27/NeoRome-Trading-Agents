from collect import R,DATA,get,stamp
import json,hashlib,urllib.error
lock=json.loads((R/'prediction-lock.json').read_text())
for name in ('older_transfer','new_day'):
 root=R/name;pred=json.loads((root/'predictions.json').read_text());assert hashlib.sha256((root/'predictions.json').read_bytes()).hexdigest()==lock['periods'][name]['predictions_sha256'];wanted=[p for p in pred if p['qualifies']];quotes={};errors=[]
 assert len(wanted)<=200,'quote call budget exceeded'
 for i,p in enumerate(wanted):
  entry=stamp(p['entry_time']);params={'start_ts':entry-60,'end_ts':entry,'period_interval':1}
  if name=='older_transfer':route='historical/markets/'+p['ticker']+'/candlesticks'
  else:route='series/KXBTCD/markets/'+p['ticker']+'/candlesticks';params['exchange_index']=2
  try:d=get(name+'-quotes-'+str(i).zfill(3),route,params)
  except urllib.error.HTTPError as ex:
   if ex.code in (404,410):errors.append({'ticker':p['ticker'],'status':ex.code});quotes[p['ticker']]=[];continue
   raise
  assert not d.get('ticker') or d['ticker']==p['ticker'];quotes[p['ticker']]=d['candlesticks']
  if i%20==0:print(name,'quotes',i+1,'of',len(wanted),flush=True)
 (root/'quotes.json').write_text(json.dumps(quotes));(root/'quote-errors.json').write_text(json.dumps(errors));print(name,'quotes complete',len(quotes),flush=True)
