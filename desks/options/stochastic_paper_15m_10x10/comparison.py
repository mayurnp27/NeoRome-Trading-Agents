#!/usr/bin/env python3
"""Read-only comparison publisher for the original and Monday paper cohorts."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import subprocess
import time
from zoneinfo import ZoneInfo

import feedback

UTC=timezone.utc
ROOT=Path('/var/lib/neorome-stochastic-paper')
CODE=Path('/opt/neorome-stochastic-paper')
OUTPUT=Path('/var/lib/neorome-maintenance-ai/inbox')

def fingerprint(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()

def comparison_row(projection, reference):
    """Flat cash is ledger equity; open positions require observed fresh equity."""
    cash=projection.get('cash');realized=projection.get('realized_pnl')
    valid=lambda x:isinstance(x,(int,float)) and not isinstance(x,bool) and math.isfinite(x)
    if projection.get('state_status')!='available' or not valid(cash) or not valid(realized):
        return {'status':'unavailable','return_pct':None,'equity':None}
    count=projection.get('position_count')
    if count==0:
        equity=cash;kind='ledger_cash_flat'
    elif projection.get('equity_status')=='observed' and valid(projection.get('equity')):
        equity=projection['equity'];kind='fresh_observed_bid_equity'
    else:
        equity=None;kind='open_positions_without_current_equity'
    pnl=round(equity-reference['equity'],6) if equity is not None else None
    return {'strategy_id':projection.get('strategy_id'),'status':'observed' if equity is not None else 'equity_unavailable',
            'launch_reference_equity':reference['equity'],'cash':cash,'equity':equity,'equity_basis':kind,
            'net_pnl_since_launch':pnl,'return_pct':round(pnl/reference['equity']*100,6) if pnl is not None else None,
            'realized_pnl_since_launch':round(realized-reference['realized_pnl'],6),'open_positions':count,
            'recent_closed_trades':projection.get('closed_trades',[])}

def build(*, baseline, roots, configs, now=None, run=subprocess.run):
    now=(now or datetime.now(UTC)).astimezone(UTC)
    values={};checks={}
    for cohort in ('original','filtered'):
        state_paths={a:roots[cohort]/'arms'/(a+'.json') for a in ('A','B')}
        config_paths={a:configs[cohort]/('config-'+a+'.json') for a in ('A','B')}
        values[cohort]=feedback.build_feedback(current_path=roots[cohort]/'current.json',state_paths=state_paths,config_paths=config_paths,now=now)
        for arm in ('A','B'):
            cfg,_=feedback._read_source(config_paths[arm]);state,_=feedback._read_source(state_paths[arm],max_bytes=feedback.MAX_LEDGER_BYTES)
            # Financial cash/realized values come from the ledger, including when
            # the last runtime projection is older than the latest state write.
            for key in ('cash','realized_pnl'):
                values[cohort]['arms'][arm][key]=state.get(key)
            checks[cohort+'_'+arm]=(state.get('strategy_id')==cfg.get('strategy_id') and state.get('config_fingerprint')==fingerprint(cfg)
                and cfg.get('initial_cash')==1000 and state.get('initial_cash')==1000
                and fingerprint(cfg)==baseline[cohort][arm]['config_fingerprint'])
    unit='neorome-stochastic-paper-f15-10x10.timer'
    result=run(['systemctl','show',unit,'--property=ActiveState,UnitFileState,NextElapseUSecRealtime','--no-pager'],capture_output=True,text=True,timeout=5,check=False)
    fields=dict(line.split('=',1) for line in result.stdout.splitlines() if '=' in line) if result.returncode==0 else {}
    checks['filtered_timer_armed']=(fields.get('ActiveState')=='active' and fields.get('UnitFileState')=='enabled' and bool(fields.get('NextElapseUSecRealtime')))
    report={'schema_version':1,'as_of':feedback._iso(now),'mode':'paper_comparison_only','launch_date':'2026-10-19',
            'checks':checks,'structural_healthy':all(checks.values()),'timer':fields,
            'cohorts':{c:{a:comparison_row(values[c]['arms'][a],baseline[c][a]) for a in ('A','B')} for c in ('original','filtered')},
            'source_observations':{c:values[c]['sources'] for c in values},'broker_orders_enabled':False}
    report['phase']='scheduled_before_start' if now.astimezone(ZoneInfo('America/New_York')).date().isoformat()<'2026-10-19' else 'comparison_active'
    return report,values['filtered']

def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline',type=Path,default=Path(__file__).with_name('comparison-baseline.json'))
    parser.add_argument('--once',action='store_true')
    args=parser.parse_args(argv)
    baseline,_=feedback._read_source(args.baseline)
    roots={'original':ROOT/'v2','filtered':ROOT/'f15-10x10'}
    configs={'original':CODE/'v2','filtered':Path(__file__).parent}
    last=None;failures=0;last_emit=0.0
    while True:
        try:
            report,projection=build(baseline=baseline,roots=roots,configs=configs)
            feedback._atomic_publish(OUTPUT/'paper-feedback-f15-10x10.json',projection)
            feedback._atomic_publish(OUTPUT/'options-comparison.json',report)
            status='published' if report['structural_healthy'] else 'checks_failed'
            error=None
        except (OSError,ValueError,feedback.SourceError,subprocess.SubprocessError) as exc:
            status='publish_failed'
            error={'class':type(exc).__name__,'reason':exc.reason if isinstance(exc,feedback.SourceError) else 'publish_exception'}
        failures=failures+1 if status!='published' else 0
        now=time.monotonic()
        if args.once or status!=last or now-last_emit>=60 or failures>=3:
            print(json.dumps({'status':status,'consecutive_failures':failures,'error':error}),flush=True);last=status;last_emit=now
        if args.once:return 0 if status=='published' else 1
        # A live process must not mask a dead publisher from systemd recovery.
        if failures>=3:return 1
        time.sleep(15)

if __name__=='__main__':raise SystemExit(main())
