"""Verify every account decision maps to a locked signal and obeys the cutoff."""
from pathlib import Path
import json,hashlib
import numpy as np
import pandas as pd
R=Path(__file__).resolve().parent

def main():
 lock=json.loads((R/'selection-lock.json').read_text());z=np.load(R/'locked-predictions.npz');ms=pd.read_csv(R/'selected-markets.csv');h=z['evaluation'];keys=list(lock['selected']);summary=json.loads((R/'paper-results.json').read_text());account_meta={(a['strategy'],a['scenario']):a for a in summary['accounts']};rows={row.ticker:i for i,row in ms.iterrows()};count=trades=0;mins=[];times=set()
 for path in sorted((R/'accounts').glob('*/*/account.json')):
  a=json.loads(path.read_text());c=lock['selected']['overall/'+a['strategy']];x=z['s'+str(keys.index('overall/'+a['strategy']))];off=c['meta']['offset'];assert off in (0,15,29) and account_meta[(a['strategy'],a['scenario'])]['selected_id']==c['meta']['id'];decisions=[json.loads(s) for s in (path.parent/'decisions.jsonl').read_text().splitlines()]
  assert len(decisions)==int(h.sum());seen=set();by_ticker={v['ticker']:v for v in decisions}
  for d in decisions:
   i=rows[d['ticker']];assert h[i] and i not in seen;seen.add(i);expected=float(x[i]);actual=float(d['signal']);assert actual==expected or (np.isnan(expected) and np.isnan(actual));start=pd.Timestamp(ms.iloc[i].start);signal=pd.Timestamp(d['signal_time']);entry=pd.Timestamp(d['entry_time']);end=pd.Timestamp(ms.iloc[i].end)
   assert signal==start+pd.Timedelta(minutes=off);assert entry==signal+pd.Timedelta(seconds=60);remaining=(end-entry).total_seconds()/60;assert remaining>=30;mins.append(remaining);times.add(off+1);count+=1
  for line in (path.parent/'trades.jsonl').read_text().splitlines():
   t=json.loads(line);decision=by_ticker[t['ticker']];assert decision['status']=='simulated_fill';assert t['signal_time']==decision['signal_time'] and t['entry_time']==decision['entry_time'];assert (1 if t['side']=='yes' else -1)==x[rows[t['ticker']]];assert t['quote_end_period_ts']==int(pd.Timestamp(t['entry_time']).timestamp());trades+=1
 # Existing0/15 selections remain identical: changed cutoff does not retune them.
 old=R.parent/'btc-hourly-search';same=0
 for base,prior in [(R,old),(R/'polymarket',old/'polymarket')]:
  assert hashlib.sha256((base/'selected-markets.csv').read_bytes()).digest()==hashlib.sha256((prior/'selected-markets.csv').read_bytes()).digest(), 'Selected contract benchmarks/labels changed'
  a=json.loads((base/'selection-lock.json').read_text());b=json.loads((prior/'selection-lock.json').read_text());za=np.load(base/'locked-predictions.npz');zb=np.load(prior/'locked-predictions.npz');oldkeys=list(b['selected'])
  for j,(key,c) in enumerate(a['selected'].items()):
   if key.startswith('overall/') or c['meta']['offset'] not in (0,15):continue
   assert c['meta']['id']==b['selected'][key]['meta']['id'];np.testing.assert_array_equal(za['s'+str(j)],zb['s'+str(oldkeys.index(key))]);same+=1
 out={'decisions_checked':count,'trades_checked':trades,'minimum_minutes_to_close':min(mins),'entry_minutes':sorted(times),'all_decisions_map_to_locked_signal':True,'quote_end_timestamp_equals_entry':True,'trades_join_matching_decision_schedule':True,'selected_market_benchmarks_and_labels_unchanged':True,'unchanged_offset0_15_selections_checked':same,'execution_limit':'Historical quote snapshots support a hypothetical full touch fill; actual publication latency/queue/depth not established; no delayed replacement fill modeled.'}
 (R/'timing-verification.json').write_text(json.dumps(out,indent=2));print(json.dumps(out))
if __name__=='__main__':main()
