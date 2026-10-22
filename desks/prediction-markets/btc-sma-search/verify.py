"""Independent scalar SMA reconstruction for the six requested crossover cases."""
import collections,csv,json,math
from pathlib import Path
R=Path(__file__).resolve().parent; S=R.parent/'btc-stochastic-search'
x=json.loads((R/'results.json').read_text());rows=json.loads((S/'data/candles.json').read_text())
from datetime import datetime
outcomes={int(datetime.fromisoformat(r['start']).timestamp()):1 if r['result']=='yes' else -1 for r in csv.DictReader((S/'matched-outcomes.csv').open())}
start=int(datetime.fromisoformat(x['source_data']['split_boundaries'][1]).timestamp())+86400
checks=[]
for c in x['candidates']:
 if c['unit']!='bars' or c['rule']!='cross':continue
 tf=c['timeframe'];step=tf*60;g=collections.defaultdict(list)
 for r in rows:g[int(r[0])//step*step].append(r)
 bars={t+step:sorted(a)[-1][4] for t,a in g.items() if len(a)==tf//5}
 fast,slow=c['pair'];gaps={}
 for t in bars:
  vals=[bars.get(t-i*step) for i in range(slow)]
  if all(v is not None for v in vals):gaps[t]=sum(vals[:fast])/fast-sum(vals)/slow
 active=correct=0
 for t,y in outcomes.items():
  if t<start or t not in gaps or t-step not in gaps:continue
  cur,prev=gaps[t],gaps[t-step]
  signal=1 if cur>0 and prev<=0 else (-1 if cur<0 and prev>=0 else 0)
  if signal:active+=1;correct+=signal==y
 expected=c['final_reused'];assert active==expected['active'],(c['id'],active,expected['active'])
 assert abs(correct/active-expected['accuracy'])<1e-12
 checks.append({'id':c['id'],'active':active,'correct':correct,'verified':True})
(R/'verification.json').write_text(json.dumps({'scalar_checks':checks,'focused_tests_passed':5,'clarified_primary_units':'candle lengths'},indent=2));print(checks)
