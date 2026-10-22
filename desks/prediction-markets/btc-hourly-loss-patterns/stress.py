"""Verification-only cost stresses of the positive distance diagnostic; no new filters."""
from analyze import R,SRC,e,dump,replay,D
import json,csv
rows=[json.loads(s) for s in (R/'observations.jsonl').read_text().splitlines() if json.loads(s)['filters']['distance_ge_1']]
full={m['ticker']:m for m in json.loads((SRC/'data/quote-market-universe.json').read_text())}
quotes={t:{c['end_period_ts']:c for c in cs} for t,cs in json.loads((SRC/'data/quotes.json').read_text()).items()}
e.R=R;e.STRATEGIES={'distance_verification':(15,'signal')}
inputs=[({'market_start':r['start'],'signal':r['signal']},full[r['ticker']]) for r in rows]
a=[e.run_account('distance_verification',s,inputs,quotes) for s in ('delayed_close','plus_one_cent','cent_precision')]
primary=json.loads((R/'results.json').read_text())['filters']['distance_ge_1']['account_25pct']
assert a[0]['ending_cash']==primary['ending_cash'] and a[0]['trades']==65 and a[0]['wins']==57
# Independent rational audit of all stress fee/debit/quantity/account arithmetic.
from fractions import Fraction as F
import math
for scenario in ('delayed_close','plus_one_cent','cent_precision'):
 cash=F(500);precision=100 if scenario=='cent_precision' else 10000
 ts=[json.loads(s) for s in (R/'accounts/distance_verification'/scenario/'trades.jsonl').read_text().splitlines()]
 for t in ts:
  p=F(t['price']);q=t['quantity'];budget=cash/4
  def cost(n):
   raw=F(math.ceil(F(7,100)*n*p*(1-p)*10**6),10**6)
   return F(math.ceil((n*p+raw)*precision),precision)
  debit=cost(q);assert debit==F(t['debit']) and debit<=budget<cost(q+1)
  assert F(t['cash_before'])==cash and F(t['fee'])==debit-q*p
  cash+=F(t['payout'])-debit;assert cash==F(t['cash_after_settlement'])
 dump(R/f'{scenario}-audit.json',{'trades':len(ts),'ending_cash':float(cash),'exact_rational_audit':True})
dump(R/'stress-results.json',{'purpose':'Checks requested after observing positive distance>=1 result; not an expanded filter search.','accounts':a,'separate_verified_engine_primary_matches':True})
for x in a:print(x['scenario'],x['ending_cash'],x['trades'],x['max_settled_balance_drawdown_pct'],x['one_contract_mean_net_all_eligible'])
