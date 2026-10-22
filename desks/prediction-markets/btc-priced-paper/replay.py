"""Finite quote-priced internal-paper accounts. No API credentials or orders."""
from pathlib import Path
from decimal import Decimal as D, ROUND_CEILING, ROUND_FLOOR
from datetime import datetime,timezone
import csv,json,hashlib,math,collections
R=Path(__file__).resolve().parent
STRATEGIES={'A_5min':(5,'5min_lead0'),'B_10min':(10,'10min_lead0'),'C_10min_lead1':(10,'10min_lead1')}
SCENARIOS=('delayed_close','same_boundary','adverse_minute','plus_one_cent','cent_precision')

def stamp(s):
 value=s.replace(' ', 'T').replace('Z','+00:00')
 # Python 3.8 fromisoformat rejects otherwise-valid 1/2/4/5-digit fractions.
 fmt='%Y-%m-%dT%H:%M:%S.%f%z' if '.' in value else '%Y-%m-%dT%H:%M:%S%z'
 return datetime.strptime(value,fmt).timestamp()
def iso(t):return datetime.fromtimestamp(t,timezone.utc).isoformat()
def ceil(value,precision):return value.quantize(precision,rounding=ROUND_CEILING)
def fee(q,price,precision=D('.0001')):
 if not q:return D(0)
 raw=ceil(D('.07')*q*price*(1-price),D('.000001'))
 return ceil(q*price+raw,precision)-q*price

def size(cash,price,precision=D('.0001')):
 budget=cash*D('.25');q=int((budget/price).to_integral_value(rounding=ROUND_FLOOR))
 # All fees are nonnegative; q above is an upper bound.
 while q>0 and q*price+fee(q,price,precision)>budget:q-=1
 return q,budget

def quote_price(candle,side,scenario):
 if not candle:return None,'missing_exact_quote'
 try:
  bid=D(candle['yes_bid']['close_dollars']);ask=D(candle['yes_ask']['close_dollars'])
  if not all(x.is_finite() for x in (bid,ask)) or not D(0)<=bid<=ask<=D(1):return None,'invalid_crossed_quote'
  if scenario=='adverse_minute':
   price=D(candle['yes_ask']['high_dollars']) if side==1 else 1-D(candle['yes_bid']['low_dollars'])
  else:price=ask if side==1 else 1-bid
  if not price.is_finite() or not 0<price<1:return None,'nontradable_price'
  if scenario=='plus_one_cent':price=min(D(1),price+D('.01'))
  return price,None
 except (KeyError,TypeError,ValueError,ArithmeticError):return None,'missing_invalid_quote_field'

def run_account(strategy,scenario,inputs,quotes,save=True):
 minute,col=STRATEGIES[strategy];precision=D('.01') if scenario=='cent_precision' else D('.0001')
 cash=D(500);pending=None;trades=[];events=[];decisions=[];reasons=collections.Counter();reference=[]
 peak=cash;maxdd=D(0);longest=losestreak=0
 def settle():
  nonlocal cash,pending,peak,maxdd,losestreak,longest
  if pending is None:return
  t=pending;cash+=D(t['payout']);t['cash_after_settlement']=str(cash)
  events.append({'timestamp':t['settlement_time'],'kind':'settlement','ticker':t['ticker'],'delta':t['payout'],'cash_after':str(cash)})
  peak=max(peak,cash);maxdd=max(maxdd,(peak-cash)/peak)
  losestreak=losestreak+1 if D(t['net_pnl'])<0 else 0;longest=max(longest,losestreak);pending=None
 for r,m in inputs:
  start=stamp(r['market_start']);decision=start+minute*60;entry=decision+(0 if scenario=='same_boundary' else 60)
  if pending is not None and stamp(pending['settlement_time'])<=entry:settle()
  row={'ticker':m['ticker'],'signal_time':iso(decision),'entry_time':iso(entry),'signal':r[col]}
  try:side=float(r[col])
  except ValueError:side=float('nan')
  if not math.isfinite(side) or side==0:reason='no_signal'
  elif pending is not None:reason='prior_position_unsettled'
  else:reason=None
  if reason:reasons[reason]+=1;row['status']=reason;decisions.append(row);continue
  side=int(side);book=quotes.get(m['ticker'],{}).get(int(entry));price,reason=quote_price(book,side,scenario)
  if reason:reasons[reason]+=1;row['status']=reason;decisions.append(row);continue
  try:settlement=stamp(m['settlement_ts']);close=stamp(m['close_time']);payoutyes=D(m['settlement_value_dollars'])
  except (KeyError,TypeError,ValueError):reason='invalid_settlement_metadata'
  else:
   if not entry<close<=settlement or m.get('is_provisional') or m.get('result') not in ('yes','no') or payoutyes != (1 if m['result']=='yes' else 0):reason='invalid_settlement_metadata'
  if reason:reasons[reason]+=1;row['status']=reason;decisions.append(row);continue
  won=(side==1)==(m['result']=='yes');unitfee=fee(1,price,precision)
  reference.append({'ticker':m['ticker'],'signal_time':iso(decision),'won':won,'price':str(price),'one_contract_fee':str(unitfee),'one_contract_net':str(D(int(won))-price-unitfee)})
  q,budget=size(cash,price,precision)
  if not q:reason='insufficient_quarter_budget';reasons[reason]+=1;row['status']=reason;decisions.append(row);continue
  f=fee(q,price,precision);cost=q*price;total=cost+f;before=cash;cash-=total;pay=D(q) if won else D(0)
  assert total<=budget and cash>=0
  t={'ticker':m['ticker'],'strategy':strategy,'scenario':scenario,'signal_time':iso(decision),'entry_time':iso(entry),'quote_end_period_ts':book['end_period_ts'],'settlement_time':iso(settlement),'side':'yes' if side==1 else 'no','price':str(price),'quantity':q,'cash_before':str(before),'budget':str(budget),'premium':str(cost),'fee':str(f),'debit':str(total),'cash_after_entry':str(cash),'won':won,'payout':str(pay),'net_pnl':str(pay-total),'close_time':m['close_time']}
  trades.append(t);pending=t;events.append({'timestamp':iso(entry),'kind':'entry','ticker':m['ticker'],'delta':str(-total),'cash_after':str(cash)});row['status']='simulated_fill';decisions.append(row)
 settle()
 premium=sum((D(t['premium']) for t in trades),D(0));fees=sum((D(t['fee']) for t in trades),D(0));payout=sum((D(t['payout']) for t in trades),D(0));quantity=sum(t['quantity'] for t in trades);wins=sum(t['won'] for t in trades)
 assert cash==D(500)+payout-premium-fees
 out={'strategy':strategy,'scenario':scenario,'starting_cash':500,'ending_cash':float(cash),'net_pnl':float(cash-500),'return_pct':float((cash/500-1)*100),'trades':len(trades),'wins':wins,'win_rate':wins/len(trades) if trades else None,'fees':float(fees),'gross_pnl_same_fills':float(payout-premium),'premium_turnover':float(premium),'contracts':quantity,'average_price_per_contract':float(premium/quantity) if quantity else None,'average_fee_per_contract':float(fees/quantity) if quantity else None,'contract_weighted_win_rate':float(payout/quantity) if quantity else None,'max_settled_balance_drawdown_pct':float(maxdd*100),'longest_net_losing_streak':longest,'skips':dict(reasons),'eligible_quote_reference_count':len(reference),'one_contract_mean_net_all_eligible':sum(float(t['one_contract_net']) for t in reference)/len(reference) if reference else None,'max_trade_quantity':max((t['quantity'] for t in trades),default=0),'open_positions_end':0}
 if save:
  folder=R/'accounts'/strategy/scenario;folder.mkdir(parents=True,exist_ok=True)
  for name,data in [('trades',trades),('events',events),('decisions',decisions),('one-contract-reference',reference)]:
   with (folder/(name+'.jsonl')).open('w') as f:
    for item in data:f.write(json.dumps(item)+'\n')
  (folder/'account.json').write_text(json.dumps(out,indent=2))
 return out

def main():
 series=json.loads((R/'data/series.json').read_text())['data']['series'];history=json.loads((R/'data/fee-history.json').read_text())['data']['series_fee_change_arr'];eventhistory=json.loads((R/'data/event-fee-history-00.json').read_text())['data']
 assert series['fee_type']=='quadratic' and series['fee_multiplier']==1 and not history
 assert not eventhistory.get('cursor') and not [e for e in eventhistory['event_fee_changes'] if e['series_ticker']=='KXBTC15M']
 markets={m['ticker']:m for m in json.loads((R.parent/'btc-stochastic-search/data/markets.json').read_text())};by_start={stamp(m['open_time']):m for m in markets.values() if '2026-09-13'<=m['open_time']<'2026-10-18'}
 predictions=[r for r in csv.DictReader((R.parent/'btc-intramarket-diagnostic/predictions.csv').open()) if r['market_start']>='2026-09-13']
 inputs=[(r,by_start[stamp(r['market_start'])]) for r in predictions];assert len(inputs)==3320
 rawquotes=json.loads((R/'data/quotes.json').read_text());quotes={}
 for key,rows in rawquotes.items():
  quotes[key]={r['end_period_ts']:r for r in rows};assert len(quotes[key])==len(rows)
 results=[run_account(strategy,scenario,inputs,quotes) for strategy in STRATEGIES for scenario in SCENARIOS]
 source_paths=[R/'data/quotes.json',R/'replay.py',R.parent/'btc-intramarket-diagnostic/predictions.csv',R.parent/'btc-stochastic-search/data/markets.json',R/'data/series.json',R/'data/fee-history.json',R/'data/event-fee-history-00.json']
 result={'period':['2026-09-13T00:00:00Z','2026-10-18T00:00:00Z'],'markets':len(inputs),'quote_markets':len(quotes),'fee_model':'0.07*q*p*(1-p), ceil6dp then ceil total debit to account precision; no settlement fee','fee_evidence':'July7 2026 schedule, current series multiplier1, empty historical series changes, no BTC15M event overrides in all19 returned event changes','primary_scenario':'delayed_close','fill_provenance':'hypothetical full fill at historical top-of-book candle quote, no depth verification','source_hashes':{str(p.relative_to(R.parent)):hashlib.sha256(p.read_bytes()).hexdigest() for p in source_paths},'accounts':results}
 (R/'results.json').write_text(json.dumps(result,indent=2,allow_nan=False))
 for r in results:print(r['strategy'],r['scenario'],'cash',r['ending_cash'],'pnl',r['net_pnl'],'fees',r['fees'],'trades',r['trades'],'win',r['win_rate'],'dd',r['max_settled_balance_drawdown_pct'],r['skips'])
if __name__=='__main__':main()
