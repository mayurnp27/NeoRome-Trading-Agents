"""Render the predeclared early-entry settlement study from frozen outputs."""
from pathlib import Path
import json,html,importlib.util
from reportlab.graphics.shapes import Drawing,String
from reportlab.graphics.charts.lineplots import LinePlot
from reportlab.graphics import renderSVG
from reportlab.lib import colors
R=Path(__file__).resolve().parent
E=html.escape
pct=lambda x:'—' if x is None else f'{100*x:.2f}%'
num=lambda x:'—' if x is None else f'{x:.3f}'
def settings(m):
 if 'components' in m:return ('Agreement: ' if m['rule']=='agree' else 'Vote: ')+' + '.join(settings(c) for c in m['components'])
 suffix=' inverted' if m.get('orientation')==-1 else ''
 return (f"{m['tf']}m Stoch({m['n']},{m['k']},{m['d']}) {m['rule']}" if m['id'].startswith('ST:') else f"{m['tf']}m SMA({m['fast']},{m['slow']}) {m['rule']}")+suffix

def table(head,rows):return '<div class="scroll"><table><thead><tr>'+''.join('<th>'+E(str(x))+'</th>' for x in head)+'</tr></thead><tbody>'+''.join('<tr>'+''.join('<td>'+E(str(x))+'</td>' for x in row)+'</tr>' for row in rows)+'</tbody></table></div>'
def chart(data,filename):
 d=Drawing(760,300);p=LinePlot();p.x=55;p.y=45;p.width=650;p.height=205;p.joinedLines=1;p.xValueAxis.valueMin=0;p.xValueAxis.valueMax=30;p.xValueAxis.valueSteps=[1,16,30];p.yValueAxis.valueMin=40;p.yValueAxis.valueMax=90;p.yValueAxis.valueSteps=[40,50,60,70,80,90]
 p.data=[]
 for i,(family,color) in enumerate([('stochastic','#4f46e5'),('sma','#0891b2'),('mixed_combination','#c2410c')]):
  p.data.append([(off+1,data['selected'][f'{family}/offset{off}']['evaluation']['accuracy']*100) for off in (0,15,29)])
  p.lines[i].strokeColor=colors.HexColor(color);p.lines[i].strokeWidth=2
  d.add(String(65+i*220,275,family.replace('_combination',''),fontSize=11,fillColor=colors.HexColor(color)))
 d.add(p);d.add(String(220,12,'Modeled entry minute after hourly window starts',fontSize=10));d.add(String(5,255,'Accuracy (%)',fontSize=9));renderSVG.drawToFile(d,str(R/filename))

def main():
 targets={v:json.loads((R/p).read_text()) for v,p in [('Kalshi','results.json'),('Polymarket','polymarket/results.json')]};paper=json.loads((R/'paper-results.json').read_text());audit=json.loads((R/'verification.json').read_text())
 out=['''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>BTC hourly settlement — entries at least 30 minutes before close</title><style>body{font:16px/1.55 system-ui,sans-serif;color:#17263d;background:#f8fafc;max-width:1160px;margin:40px auto;padding:0 24px}h1{font-size:36px;line-height:1.2}h2{margin-top:40px;border-bottom:1px solid #cbd5e1;padding-bottom:8px}table{border-collapse:collapse;width:100%;background:white;font-size:13px;margin:18px 0}th,td{padding:10px 12px;text-align:left;vertical-align:top;border-bottom:1px solid #e2e8f0}th{background:#e2e8f0}a{color:#4338ca}.scroll{overflow:auto}.callout{padding:18px;background:#eef2ff;border-left:4px solid #4f46e5}details{background:white;padding:14px;margin:16px 0}summary{cursor:pointer;font-weight:600}img{width:100%;height:auto}.muted{font-size:14px;color:#64748b}</style><p class="muted">NeoRome · 18 October 2026 · Historical research</p><h1>Predict the hourly market result, enter at least 30 minutes before close</h1><p>The target is the contract’s displayed price-to-beat or strike. The BTC price at our entry is not the settlement benchmark, and the contract’s purchase price in cents is a separate cost.</p><div class="callout">Decisions at minutes 0, 15 and 29. Modeled execution exactly one minute later, at minutes 1, 16 and 30: respectively 59, 44 and 30 minutes remain. Late replacement fills are excluded. Only completed chart candles available at decision time are used.</div>''']
 out.append('<h2>The benchmark used</h2>'+table(['Market','Settlement benchmark','Result being predicted'],[['Kalshi KXBTCD Above/Below','Listed dollar strike of the selected contract','YES if final 60-second BRTI average is strictly above the strike; NO otherwise.'],['Polymarket hourly Up/Down','Opening trade of that hour’s Binance BTC/USDT candle','UP if hourly close is greater than or equal to hourly open; DOWN otherwise.']]))
 out.append('<p>Example: a $100,000 benchmark and a $100,100 settlement is an Above/Up outcome even if BTC was $100,400 when the contract was purchased. That intervening fall does not change the benchmark.</p>')
 rows=[]
 for venue,d in targets.items():
  for fam in ('stochastic','sma','mixed'):
   c=d['selected']['overall/'+fam];m=c['evaluation'];off=c['meta']['offset'];rows.append([venue,fam,off,off+1,60-off-1,settings(c['meta']),f"{m['wins']}/{m['active']}",pct(m['accuracy']),pct(m['coverage']),num(m['correlation'])])
 out.append('<h2>Validation-selected strategies under the entry deadline</h2>'+table(['Target','Family','Decision minute','Entry minute','Minutes left','Settings','Correct / signals','Accuracy','Coverage','Correlation'],rows))
 out.append('<p>Parameters and decision time are chosen using training then validation only. Accuracy and correlation are descriptive evaluation measures, not calibrated probabilities. Same-family strategies can combine several chart timeframes. Mixed means stochastic plus SMA.</p>')
 for venue,d in targets.items():
  name=venue.lower()+'-accuracy.svg';chart(d,name);out.append(f'<h2>{venue}: every allowed decision time</h2><img src="{name}" alt="{venue} accuracy for three early-entry schedules"><p class="muted">Each point uses a separately validation-selected strategy. The horizontal axis is modeled execution time, one minute after the signal.</p>')
  rows=[]
  for off in (0,15,29):
   for fam in ('stochastic','sma','mixed_combination'):
    c=d['selected'][f'{fam}/offset{off}'];m=c['evaluation'];b=c['spot_on_same_trades'];ci=c['bootstrap']['paired_accuracy_advantage95']
    rows.append([off,off+1,fam.replace('_combination',''),settings(c['meta']),pct(c['train']['accuracy']),pct(c['validation']['accuracy']),pct(m['accuracy']),m['active'],pct(b['accuracy']) if b else '—',pct(c['paired_accuracy_advantage_vs_spot']),('—' if ci[0] is None else f'{100*ci[0]:+.2f} to {100*ci[1]:+.2f} pp')])
  out.append(table(['Decision','Entry','Family','Settings','Train accuracy','Validation accuracy','Evaluation accuracy','Signals','Price baseline, matched hours','Paired difference','95% daily-block interval'],rows))
  out.append('<p>The simple comparison baseline predicts the side of the market benchmark on which the latest observed price sits. It uses the completed five-minute close available at decision time: minute 25 for decision 29. It is a prediction comparator, not the contract’s settlement benchmark. Polymarket’s minute-0 comparison is unavailable because the opening trade is not yet known.</p>')
  rows=[]
  for off in (0,15,29):
   for tf in (15,30,45,60):
    for fam in ('stochastic','sma'):
     c=d['selected'].get(f'{fam}/chart{tf}/offset{off}')
     if c:
      m=c['evaluation'];rows.append([off,tf,fam,settings(c['meta']),pct(m['accuracy']),m['active'],pct(m['coverage']),num(m['correlation'])])
  out.append('<details><summary>All four chart timeframes: selected single-chart rules</summary>'+table(['Decision','Chart minutes','Family','Settings','Accuracy','Signals','Coverage','Correlation'],rows)+'</details>')
  rows=[]
  for tf in (15,30,45,60):
   for pair in ('2-5','5-10'):
    for rule in ('state','cross'):
     for off in (0,15,29):
      c=d['selected'][f'control/{tf}/{pair}/{rule}/{off}'];m=c['evaluation'];rows.append([tf,pair,rule,off,pct(m['accuracy']),m['active']])
  out.append('<details><summary>Fixed SMA 2/5 and 5/10 controls</summary>'+table(['Chart minutes','Lengths','Rule','Decision minute','Accuracy','Signals'],rows)+'</details>')
 out.append('<h2>Kalshi quote-based paper replay</h2><p>Each of three separate accounts starts with $500 and allocates at most 25% of current settled cash including fees. Integer contracts, one unsettled position maximum, official settlement timestamps. Historical YES ask or 1−YES bid at the modeled entry timestamp. These are hypothetical full touch fills; available depth and queue position are not established.</p>')
 rows=[]
 for a in paper['accounts']:rows.append([a['strategy'],a['scenario'],a['entry_offset_minutes'],a['trades'],pct(a['win_rate']),f"${a['ending_cash']:.2f}",f"${a['fees']:.2f}"])
 out.append(table(['Family','Scenario','Entry minute','Trades','Account win rate','Ending cash','Total fees'],rows))
 rows=[]
 for fam,s in paper['unit_shadows'].items():rows.append([fam,s['signals'],pct(s['win_rate']),f"{100*s['mean_price']:.3f}¢",f"{100*s['mean_fee']:.3f}¢",f"{100*s['mean_net']:+.3f}¢",f"{100*s['mean_net_daily_block95'][0]:+.3f} to {100*s['mean_net_daily_block95'][1]:+.3f}¢"])
 out.append('<h3>One contract on every eligible quote</h3><p>This reference includes every active signal with an eligible quote, ignoring cash depletion and the one-position constraint. Its win rate can differ from full-signal accuracy and finite-account accuracy because the subsets differ.</p>'+table(['Family','Quoted signals','Win rate','Mean price','Mean fee','Mean net','Net 95% daily-block interval'],rows))
 out.append('<p>Polymarket executable historical asks and depth were not established, so no actual-entry Polymarket profit estimate is supplied. None of the prior minute-45 account results are transferred to this experiment.</p>')
 out.append('''<h2>What the deadline changes</h2><p>A signal based on the candle finishing at minute 30 cannot be assumed available for a delayed fill by minute 30. The latest decision here is minute 29. It therefore cannot use the new 30-minute candle, or the 15-minute candle closing at minute 30. With completed candles only, its indicator states equal those available at minute 15; fresh crossover/zone events do not fire at minute 29. Any accuracy increase from using an unfinished candle must be tested separately with point-in-time partial-bar construction.</p><h2>Method and limits</h2><p>Same 180-day data and original official outcome labels. Training April 21–August 6; August 7 embargo; validation August 8–September 11; September 12 embargo; evaluation September 13–October 17. Training/validation labels must settle before their cutoffs. This evaluation period was already inspected repeatedly: exploratory, not an untouched confirmation.</p><p>Stochastic lookback 5–100, K/D smoothing 1–5, eight rules and both orientations. SMA every pair 2≤fast&lt;slow≤200, state and fresh cross, both orientations. Four chart timeframes and three decisions: 1,406,448 single-chart variants per target, 2,812,896 total. This includes repeated minute-0/15 variants from the original study. Training-only top ten distinct sequences per family/chart/time; top-five pair combinations and top-one three/four-chart agreements. Minimum 200 training and 50 validation signals. Validation signed correlation ranks candidates and entry times. Combinations are exhaustive only within these declared shortlists.</p>''')
 out.append('<p>Combination counts: '+', '.join(f'{venue}: {d["combination_trials"]:,}' for venue,d in targets.items())+'. Confidence intervals use 2,000 whole-day resamples over only 35 evaluation days and are unadjusted for the large search. Original final metadata can contain historical revisions not reconstructed. Coinbase is a proxy for Kalshi’s BRTI settlement source; Binance matches the Polymarket rule.</p>')
 out.append('<h2>Verification and reproducibility</h2><p>Nine focused tests passed. Independent calculations reconstructed '+f"{audit['kalshi']['independently_reconstructed_cells']+audit['polymarket']['independently_reconstructed_cells']:,}"+' selected signal cells and checked source precision, arithmetic bounds, chronological masks, price baselines and declared trial counts. Candidate scores for every trial were not independently recomputed. Exact-rational audit checked '+f"{audit['cash']['trades']:,}"+' trades and '+f"{audit['cash']['cash_events']:,}"+' cash events. All saved account decisions and trades meet the at-least-30-minute deadline in modeled timestamps.</p>')
 out.append('''<p><a href="EXPERIMENT.md">Frozen protocol</a> · <a href="results.json">Kalshi results</a> · <a href="polymarket/results.json">Polymarket results</a> · <a href="paper-results.json">Paper results</a> · <a href="verification.json">Audit</a> · <a href="manifest.json">Hashes</a> · <a href="review.json">Independent review</a></p><p>Inputs are reused by a relative read-only-use symlink to the original research cache. The original report, locks and data remain intact. No live orders, deployment, permission changes or ongoing paper process.</p><p>Sources: <a href="https://assets.kalshi.com/contract_terms/BTC.pdf">Kalshi contract terms</a>; <a href="https://polymarket.com/event/bitcoin-up-or-down-october-1-2026-7am-et">Polymarket hourly rules</a>; <a href="https://docs.kalshi.com/api-reference/market/batch-get-market-candlesticks">Kalshi quote candles</a>; <a href="https://kalshi.com/docs/kalshi-fee-schedule.pdf">fee schedule</a> and <a href="https://docs.kalshi.com/getting_started/fee_rounding">rounding</a>.</p></html>''')
 (R/'report.html').write_text('\n'.join(out));print('report saved',R/'report.html')
if __name__=='__main__':main()
