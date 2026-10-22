"""Four isolated $500 cash accounts per fixed unused period; no rule fitting."""
from pathlib import Path
from decimal import Decimal as D,ROUND_CEILING,ROUND_FLOOR
from fractions import Fraction as F
import json,hashlib,importlib.util,math,collections
import numpy as np
R=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('accepted_engine',R.parent/'btc-priced-paper/replay.py');e=importlib.util.module_from_spec(spec);spec.loader.exec_module(e)
modern_fee=e.fee

def historical_fee(q,p,precision=D('.01')):return (D('.07')*q*p*(1-p)).quantize(D('.01'),rounding=ROUND_CEILING) if q else D(0)
def dump(p,v):p.write_text(json.dumps(v,indent=2,allow_nan=False))
def audit(folder,allocation,legacy):
 ts=[json.loads(x) for x in (folder/'trades.jsonl').read_text().splitlines()];events=[json.loads(x) for x in (folder/'events.jsonl').read_text().splitlines()];balance=F(500);prior_settle=None
 for t in ts:
  p=F(t['price']);q=t['quantity'];budget=balance*F(str(allocation))
  precision=100 if folder.name=='cent_precision' else 10000
  def cost(qty):
   raw=F(7,100)*qty*p*(1-p)
   if legacy:return qty*p+F(math.ceil(raw*100),100)
   return F(math.ceil((qty*p+F(math.ceil(raw*10**6),10**6))*precision),precision)
  debit=cost(q);assert F(t['debit'])==debit and debit<=budget<cost(q+1)
  assert F(t['cash_before'])==balance and F(t['fee'])==debit-q*p
  assert e.stamp(t['signal_time'])+60==e.stamp(t['entry_time']) and e.stamp(t['close_time'])-e.stamp(t['entry_time'])==2640
  if prior_settle is not None:assert prior_settle<=e.stamp(t['entry_time'])
  prior_settle=e.stamp(t['settlement_time']);assert prior_settle>=e.stamp(t['close_time'])
  balance+=F(t['payout'])-debit;assert F(t['cash_after_settlement'])==balance
 cash=F(500)
 for event in events:
  cash+=F(event['delta']);assert cash==F(event['cash_after']) and cash>=0
 assert cash==balance
 return {'trades':len(ts),'events':len(events),'ending_cash':float(cash),'rational_audit':True}

def main():
 lock=json.loads((R/'prediction-lock.json').read_text());assert hashlib.sha256((R/'protocol.json').read_bytes()).hexdigest()==lock['protocol_sha256']
 series=json.loads((R/'data/series.json').read_text())['data']['series'];changes=json.loads((R/'data/fee-history.json').read_text())['data'];events=json.loads((R/'data/event-fee-history.json').read_text())['data'];assert series['fee_type']=='quadratic' and series['fee_multiplier']==1;assert not changes['series_fee_change_arr'] and not events.get('cursor') and not [v for v in events['event_fee_changes'] if v['series_ticker']=='KXBTCD']
 result={'periods':{},'fee_evidence':{'standard_multiplier':1,'series_changes':0,'event_overrides':0,'older_rounding':'ceil_cent(.07*q*p*(1-p)); indexed official Feb5 version; original historical PDF has since been replaced by July version. Primary older fee version has this provenance limitation.','new_rounding':'current July7 schedule, ceil6dp fee then total debit aligned to .0001; cent-account sensitivity.','fee_sources':['https://demo.kalshi.co/docs/kalshi-fee-schedule.pdf','https://kalshi.com/docs/kalshi-fee-schedule.pdf','https://docs.kalshi.com/getting_started/fee_rounding']},'fill_limit':'Actual historical entry bid/ask candles; hypothetical full fills without verified available size, queue position or publication latency.'}
 for name in ('older_transfer','new_day'):
  root=R/name;e.R=root;pred=json.loads((root/'predictions.json').read_text());assert hashlib.sha256((root/'predictions.json').read_bytes()).hexdigest()==lock['periods'][name]['predictions_sha256'];markets={m['ticker']:m for m in json.loads((root/'selected-markets.json').read_text())};rawquotes=json.loads((root/'quotes.json').read_text());quotes={};quote_formats=collections.Counter()
  for ticker,cs in rawquotes.items():
   quotes[ticker]={}
   for c in cs:
    # Historical and current API schemas are retained raw; only explicit dollar fields accepted.
    for field in ('yes_bid','yes_ask'):
     if 'close_dollars' in c[field]:quote_formats['close_dollars']+=1
     elif isinstance(c[field].get('close'),str) and '.' in c[field]['close']:
      c[field]['close_dollars']=c[field]['close'];quote_formats['documented_decimal_close_string']+=1
     else:raise ValueError('Unrecognized historical quote units; do not infer cents/dollars')
    assert c['end_period_ts'] not in quotes[ticker];quotes[ticker][c['end_period_ts']]=c
  inputs=[];invalid=[]
  for p in pred:
   m=markets[p['ticker']];end=e.stamp(m['close_time'])
   try:
    valid=m.get('result') in ('yes','no') and ((D(str(m['expiration_value']))>D(str(m['floor_strike'])))==(m['result']=='yes')) and not m.get('is_provisional') and e.stamp(m['settlement_ts'])>=end and D(m['settlement_value_dollars'])==(1 if m['result']=='yes' else 0)
   except (TypeError,KeyError,ValueError,ArithmeticError):valid=False
   if not valid:invalid.append({'ticker':p['ticker'],'qualified':p['qualifies']});continue
   inputs.append((p,m))
  accounts=[];audits=[]
  for allocation in (.01,.02,.05,.25):
   key='size_'+str(int(allocation*100))+'pct';e.STRATEGIES={key:(15,'signal')}
   def size(cash,price,precision=D('.0001')):
    budget=cash*D(str(allocation));q=int((budget/price).to_integral_value(rounding=ROUND_FLOOR))
    while q and q*price+e.fee(q,price,precision)>budget:q-=1
    return q,budget
   e.size=size
   scenarios=('delayed_close','plus_one_cent','cent_precision')
   for scenario in scenarios:
    legacy=name=='older_transfer' and scenario!='cent_precision'
    # In older period 'cent_precision' is modern fine-rounding comparison, named explicitly below.
    e.fee=historical_fee if legacy else modern_fee
    actual_scenario='delayed_close' if name=='older_transfer' and scenario=='cent_precision' else scenario
    if name=='older_transfer' and scenario=='cent_precision':
     alt=key+'_modern_rounding';e.STRATEGIES={alt:(15,'signal')};out=e.run_account(alt,actual_scenario,inputs,quotes);folder=root/'accounts'/alt/actual_scenario
    else:out=e.run_account(key,actual_scenario,inputs,quotes);folder=root/'accounts'/key/actual_scenario
    out['allocation']=allocation;out['comparison']='modern_rounding' if name=='older_transfer' and scenario=='cent_precision' else scenario;out['fee_rule']='historical_cent' if legacy else 'modern';out['ledger']=str(folder.relative_to(R));accounts.append(out);audits.append(audit(folder,allocation,legacy));e.STRATEGIES={key:(15,'signal')}
  e.fee=historical_fee if name=='older_transfer' else modern_fee
  reference=[];missing=[]
  for p,m in inputs:
   if not p['qualifies']:continue
   price,reason=e.quote_price(quotes.get(p['ticker'],{}).get(int(e.stamp(p['entry_time']))),p['signal'],'delayed_close')
   if reason:missing.append({'ticker':p['ticker'],'reason':reason});continue
   won=(p['signal']==1)==(m['result']=='yes');fee=e.fee(1,price);reference.append({'ticker':p['ticker'],'start':p['market_start'],'won':won,'price':str(price),'fee':str(fee),'net':str(D(int(won))-price-fee),'distance':p['distance']})
  days=sorted(set(v['start'][:10] for v in reference));summary={'signals':len(reference),'wins':sum(v['won'] for v in reference)}
  if reference:
   summary.update(win_rate=summary['wins']/len(reference),mean_price=sum(float(v['price']) for v in reference)/len(reference),mean_fee=sum(float(v['fee']) for v in reference)/len(reference),mean_net=sum(float(v['net']) for v in reference)/len(reference),unit_net_total=float(sum((D(v['net']) for v in reference),D(0))))
   if len(days)>=10:
    totals=np.array([[sum(float(v['net']) for v in reference if v['start'][:10]==day),sum(v['start'][:10]==day for v in reference)] for day in days]);rng=np.random.default_rng(20261018);bs=totals[rng.integers(0,len(days),(2000,len(days)))].sum(1);summary['daily_block_mean_net95']=np.quantile(bs[:,0]/bs[:,1],[.025,.975]).tolist()
   else:summary['daily_block_mean_net95']=None
  write_result={'coverage':lock['periods'][name],'invalid_settlement_metadata':invalid,'missing_quotes':missing,'quote_formats':dict(quote_formats),'reference':summary,'accounts':accounts,'audits':audits};result['periods'][name]=write_result;dump(root/'unit-reference.json',reference)
  print(name,json.dumps(summary),flush=True)
  for a in accounts:print(a['allocation'],a['comparison'],a['trades'],a['ending_cash'],a['max_settled_balance_drawdown_pct'],flush=True)
 dump(R/'results.json',result)
if __name__=='__main__':main()
