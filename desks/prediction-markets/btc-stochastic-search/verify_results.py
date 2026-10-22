"""Independent scalar indicator reconstruction for selected holdout candidates."""
import collections,csv,hashlib,json,math
from pathlib import Path
R=Path(__file__).resolve().parent
j=json.loads((R/'results.json').read_text())
assert hashlib.sha256((R/'search.py').read_bytes()).hexdigest()==j['code_sha256']
rows=json.loads((R/'data/candles.json').read_text()); groups={}
for tf in (5,15,30):
 g=collections.defaultdict(list)
 for r in rows:g[int(r[0])//(tf*60)*(tf*60)].append(r)
 groups[tf]={t+tf*60:(max(r[2] for r in a),min(r[1] for r in a),sorted(a)[-1][4]) for t,a in g.items() if len(a)==tf//5}
from datetime import datetime
outcomes={int(datetime.fromisoformat(r['start']).timestamp()):1 if r['result']=='yes' else -1 for r in csv.DictReader((R/'matched-outcomes.csv').open())}
start=int(datetime.fromisoformat(j['data']['split_boundaries'][1]).timestamp())+86400
checks=[]
def avg(a):return sum(a)/len(a) if all(math.isfinite(v) for v in a) else float('nan')
for name in ('5','15','30'):
 c=j['selected'][name]; m=c['meta']; tf=m['tf']; step=tf*60; bars=groups[tf]; raw={}; K={}; D={}
 for t in range(min(bars),max(bars)+step,step):
  window=[bars.get(t-i*step) for i in range(m['n'])]
  value=float('nan')
  if all(window):
   high=max(b[0] for b in window); low=min(b[1] for b in window)
   if high>low:value=100*(bars[t][2]-low)/(high-low)
  raw[t]=value
  K[t]=avg([raw.get(t-i*step,float('nan')) for i in range(m['k'])])
  D[t]=avg([K.get(t-i*step,float('nan')) for i in range(m['d'])])
 active=correct=0
 for t,y in outcomes.items():
  if t<start or t%step:continue
  k=K.get(t,float('nan')); pk=K.get(t-step,float('nan')); d=D.get(t,float('nan')); pd=D.get(t-step,float('nan'))
  if not all(map(math.isfinite,(k,pk,d,pd))):continue
  signal=0
  if m['rule']=='cross':signal=1 if k>d and pk<=pd else (-1 if k<d and pk>=pd else 0)
  elif m['rule']=='turn20':signal=1 if pk<=20 and k>pk else (-1 if pk>=80 and k<pk else 0)
  else:raise AssertionError('unexpected selected rule')
  if signal:active+=1;correct+=signal*m['orientation']==y
 expected=c['holdout']; assert active==expected['active'],(name,active,expected)
 assert abs(correct/active-expected['accuracy'])<1e-12
 checks.append({'timeframe':name,'active':active,'correct':correct,'accuracy':correct/active,'matched':True})
trials=list(csv.DictReader((R/'trials.csv').open())); assert len(trials)==j['trials']==69800
eligible=[r for r in trials if int(r['train_active'])>=200 and int(r['validation_active'])>=100 and float(r['train_correlation'])>0]
best=max(eligible,key=lambda r:float(r['validation_correlation']));assert best['id']==j['selected']['overall']['meta']['id']
combo=max((r for r in eligible if '+' in r['tf']),key=lambda r:float(r['validation_correlation']))
result={'independent_scalar_checks':checks,'trial_count_verified':len(trials),'selection_verified':True,'best_combination_validation_only':combo}
(R/'verification.json').write_text(json.dumps(result,indent=2));print(json.dumps(result,indent=2))
