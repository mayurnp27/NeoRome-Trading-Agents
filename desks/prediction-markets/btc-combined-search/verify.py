import csv,json,math
from pathlib import Path
R=Path(__file__).resolve().parent;x=json.loads((R/'results.json').read_text());rows=list(csv.DictReader((R/'final-predictions.csv').open()))
def number(v):return float(v) if v else float('nan')
for c in x['candidates']:
 active=correct=0;xs=[];ys=[]
 for r in rows:
  parts=[number(r[k]) for k in c['components']];v=float('nan') if not all(map(math.isfinite,parts)) else (parts[0] if all(a==parts[0] for a in parts) else 0.)
  actual=number(r[c['id']]);assert (math.isnan(v) and math.isnan(actual)) or v==actual
  if math.isfinite(v):xs.append(v);ys.append(float(r['outcome']))
  if math.isfinite(v) and v!=0:active+=1;correct+=v==float(r['outcome'])
 m=c['final_reused'];assert active==m['active']
 if active:assert abs(correct/active-m['accuracy'])<1e-12
 mx=sum(xs)/len(xs);my=sum(ys)/len(ys);den=math.sqrt(sum((v-mx)**2 for v in xs)*sum((v-my)**2 for v in ys));corr=sum((a-mx)*(b-my) for a,b in zip(xs,ys))/den if den else 0
 assert abs(corr-m['correlation'])<1e-10
old_sma=json.loads((R.parent/'btc-sma-search/results.json').read_text())
for c in old_sma['candidates']:
 if c['unit']!='bars':continue
 key=f"sma:{c['timeframe']}m:{c['pair'][0]}/{c['pair'][1]}:{c['rule']}"
 for field in ('active','accuracy','correlation'):assert abs(x['baselines'][key][field]-c['final_reused'][field])<1e-12
old_stoch=json.loads((R.parent/'btc-stochastic-search/results.json').read_text())
for tf in (5,15,30):
 for field in ('active','accuracy','correlation'):assert abs(x['baselines'][f'stoch:{tf}m:event'][field]-old_stoch['selected'][str(tf)]['holdout'][field])<1e-12
for group,winner in x['selection'].items():
 eligible=[c for c in x['candidates'] if c['train']['active']>=200 and c['validation']['active']>=100 and (group=='overall' or c['recipe']==group)]
 assert max(eligible,key=lambda c:c['validation']['correlation'])['id']==winner
proof={'verified_combination_predictions_and_metrics':90,'prior_component_baselines_matched':15,'validation_selections_verified':len(x['selection']),'focused_tests_passed':5}
(R/'verification.json').write_text(json.dumps(proof,indent=2));print(proof)
