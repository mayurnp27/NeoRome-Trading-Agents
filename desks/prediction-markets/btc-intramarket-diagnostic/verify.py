import csv,json,math,statistics
from datetime import datetime
from pathlib import Path
R=Path(__file__).resolve().parent;S=R.parent/'btc-stochastic-search';j=json.loads((R/'results.json').read_text());raw={int(r[0]):r for r in json.loads((S/'data/candles.json').read_text())};markets=list(csv.DictReader((S/'matched-outcomes.csv').open()));rows=list(csv.DictReader((R/'predictions.csv').open()));checks=[]
for case in j['cases']:
 active=correct=0
 for m,stored in zip(markets,rows):
  if not m['floor_strike']:continue
  t=int(datetime.fromisoformat(m['start']).timestamp());elapsed=case['minutes_elapsed'];decision=t+elapsed*60
  # Every referenced candle ends at or before decision time.
  times=[decision-300-i*300 for i in range(13)];assert all(s+300<=decision for s in times)
  if not all(s in raw for s in times):continue
  closes=[raw[s][4] for s in reversed(times)];returns=[math.log(b/a) for a,b in zip(closes,closes[1:])];sigma=statistics.stdev(returns)
  if sigma<=0:continue
  price=raw[decision-300][4];gap=math.log(price/float(m['floor_strike']));z=gap/(sigma*math.sqrt((15-elapsed)/5))
  if 'lead_threshold' in case:score=(1 if gap>0 else -1 if gap<0 else 0) if abs(z)>=case['lead_threshold'] else 0
  else:
   if t not in raw:continue
   d=price-raw[t][3];score=1 if d>0 else -1 if d<0 else 0
  assert score==float(stored[case['id'] if 'lead_threshold' in case else f'spot_reference_{elapsed}']),(case['id'],m['ticker'])
  if m['start']>='2026-09-13' and score:
   active+=1;correct+=score==(1 if m['result']=='yes' else -1)
 assert active==case['final_reused']['active'];assert abs(correct/active-case['final_reused']['accuracy'])<1e-12
 checks.append({'id':case['id'],'active':active,'correct':correct})
proof={'independent_scalar_checks':checks,'past_candle_timestamp_checks':True,'new_model_fits':0,'data_publication_and_fills_verified':False};(R/'verification.json').write_text(json.dumps(proof,indent=2));print(proof)
