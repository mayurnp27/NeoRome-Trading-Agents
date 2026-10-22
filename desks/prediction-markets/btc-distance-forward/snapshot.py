"""One bounded read-only cloud snapshot and local report refresh."""
from pathlib import Path
import sys,json,datetime as dt,html
ROOT=Path(__file__).resolve().parents[3];HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'coordination/verification/btc-crossover-20261016'));from remote import run
r=run(r'''
from pathlib import Path
import json,sqlite3,time,subprocess,hashlib
root=Path('/var/lib/neorome-btc-distance-v1');c=sqlite3.connect('file:'+str(root/'ledger.sqlite3')+'?mode=ro',uri=True);c.row_factory=sqlite3.Row;c.execute('PRAGMA query_only=ON')
status=json.loads((root/'status.json').read_text())
units={u:subprocess.run(['systemctl','show',u,'-p','ActiveState','-p','SubState','-p','NRestarts','-p','UnitFileState','-p','ExecStart'],capture_output=True,text=True,timeout=5).stdout for u in ['neorome-btc-distance.service','neorome-btc-hourly98.service','neorome-paper-runtime.timer']}
rows=c.execute("select id,at,kind,arm,payload from events where kind in ('entry','settlement','skip','error') order by id desc limit 80").fetchall()
events=[dict(v,payload=json.loads(v['payload'])) for v in rows]
counts={t:c.execute('select count(*) from '+t).fetchone()[0] for t in ['selections','decisions','sources','settlements','marks']}
print(json.dumps({'captured_at':time.time(),'status':status,'units':units,'table_counts':counts,'recent_events':events,'release_manifest_sha256':hashlib.sha256(Path('/opt/neorome-btc-distance-r2/MANIFEST.json').read_bytes()).hexdigest(),'ledger_bytes':sum(p.stat().st_size for p in root.glob('ledger.sqlite3*'))}))
''')
(HERE/'latest.json').write_text(json.dumps(r,indent=2)+'\n')
s=r['status'];ts=s['updated_at'];rows=''
for a in sorted(s['accounts'],key=lambda a:float(a['allocation'])):
    money=lambda x:'unknown' if x is None else '$'+format(float(x),',.2f')
    rows+='<tr>'+''.join('<td>'+html.escape(str(v))+'</td>' for v in [str(int(float(a['allocation'])*100))+'%',money(a['cash']),money(a['nav']),money(a['realized']),money(a['fees']),a['entries'],str(a['wins'])+'/'+str(a['losses']),f"{float(a['max_dd']):.2%}",f"{float(a['mark_dd']):.2%}",'Open' if a['position'] else 'Flat'])+'</tr>'
trade_rows=''
for e in r['recent_events']:
    if e['kind']=='entry':
        p=e['payload'];trade_rows+='<tr>'+''.join('<td>'+html.escape(str(v))+'</td>' for v in [dt.datetime.fromtimestamp(e['at'],dt.timezone.utc).isoformat(),e['arm'],p['ticker'],p['side'],p['quantity'],p['vwap'],p['fee'],p['amount']])+'</tr>'
if not trade_rows:trade_rows='<tr><td colspan="8">No prospective entries recorded yet.</td></tr>'
page='''<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>BTC distance filter — paper tracker</title><style>body{max-width:1300px;margin:40px auto;padding:0 24px;background:#f5f8fb;color:#193345;font:16px/1.6 system-ui}h1{font-size:32px}table{width:100%;border-collapse:collapse;background:white;margin:22px 0}th,td{text-align:left;padding:11px;border-bottom:1px solid #dae4ed}th{font-size:13px;background:#e2edf5}.status{padding:18px;background:#e5f2e9;border-radius:10px}.note{color:#536575}a{color:#07598e}code{font-size:13px}</style><h1>Frozen BTC distance filter · forward paper tracker</h1>'''
page+=f'<p class="status">Snapshot: {html.escape(ts)} · Service: {"running" if "ActiveState=active" in r["units"]["neorome-btc-distance.service"] else "check required"} · Entry hold: {html.escape(str(s["entry_blocker"] or "none"))}</p>'
page+='<p>Four independent $500 internal-paper accounts using actual observed Kalshi books and simulated fills and fees. The filter remains unchanged. Each percentage applies to current available cash, including fees.</p><table><tr><th>Allocation</th><th>Available cash</th><th>Marked equity</th><th>Realized profit</th><th>Fees</th><th>Entries</th><th>Wins/losses</th><th>Settled max DD</th><th>Marked max DD</th><th>Position</th></tr>'+rows+'</table>'
page+='<p>One hourly fixed-strike BTC market; stochastic15m(67,1,1) D-slope + SMA45m(2,3) fresh-cross vote; distance at least one volatility unit. Decision at minute15; first fresh quote at minute16, within10seconds. Hold until official settlement.</p>'
page+='<h2>Recent simulated entries at observed prices</h2><table><tr><th>UTC</th><th>Account</th><th>Contract</th><th>Side</th><th>Quantity</th><th>VWAP $</th><th>Fee $</th><th>Debit $</th></tr>'+trade_rows+'</table>'
page+=f'<p class="note">Selected hours: {r["table_counts"]["selections"]}; recorded decisions: {r["table_counts"]["decisions"]}; resolved contracts: {r["table_counts"]["settlements"]}. This page is a dated snapshot; the cloud collector continues independently. Profitability has not been established. Marked drawdown is sampled about every10seconds and becomes unavailable when fresh depth is missing. Direct-member rounding is modeled; there are no broker fills or charged fees.</p><p><a href="SPEC.md">Frozen specification</a> · <a href="latest.json">Detailed snapshot</a> · <a href="https://docs.kalshi.com/getting_started/fee_rounding">Official fee rounding</a></p>'
(HERE/'report.html').write_text(page)
print(json.dumps({'updated_at':ts,'unit':r['units']['neorome-btc-distance.service'],'entry_blocker':s['entry_blocker'],'errors':s['errors'],'accounts':[{k:a[k] for k in ('arm','cash','realized','fees','entries','wins','losses','max_dd','mark_dd')} for a in s['accounts']],'next_full_hour':s['next_full_hour'],'table_counts':r['table_counts']}))
