"""Persist one observed no-order equity decision against a guarded paper book."""
from __future__ import annotations
import argparse
from dataclasses import asdict
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
from .equity_data import stamp,shadow_decision
from .paper_ledger import PaperLedger
from .jupiter_quotes import save_new


def run(observation_path, output_dir, now=None):
    now=now or datetime.now(timezone.utc)
    raw=Path(observation_path).read_bytes()
    observation=json.loads(raw)
    if observation.get('source_kind')!='observed' or observation.get('feed')!='iex':
        raise ValueError('Expected a recorded observed IEX dataset')
    if observation.get('broker_orders') != 0 or observation.get('execution_enabled') is not False:
        raise ValueError('Unexpected observation execution state')
    if stamp(observation['observed_at'])>now:
        raise ValueError('Observation receipt is in the future')
    # Re-evaluate quote age at consumption; never trust the stored decision alone.
    decision=shadow_decision(observation,now)
    out=Path(output_dir)
    out.mkdir(parents=True,exist_ok=False,mode=0o700)
    book=PaperLedger(out/'paper.sqlite3',enforce_market_risk=True,clock=lambda:now)
    marked=book.record_marks('initial-empty-book',[],as_of=now)
    # This stage deliberately has no reserve/fill path: asset, fee and session
    # eligibility is still missing. An empty book is honestly marked at cash.
    if decision['eligible_for_execution']:
        raise ValueError('Execution promotion requires a separately reviewed adapter')
    result={'mode':'observed_equity_shadow_no_order','source_kind':'observed',
        'observation_sha256':hashlib.sha256(raw).hexdigest(),
        'observation_received_at':observation['observed_at'],'evaluated_at':now.isoformat(),
        'decision':decision,'paper_market_snapshot':asdict(marked),
        'paper_balances':[asdict(x) for x in book.balances()],
        'execution_enabled':False,'orders_sent':0,'simulated_fills':0,
        'market_risk_enforced':True,'strategy_profitability_validated':False,
        'note':'Fresh empty evaluation book at requested $800 split; no positions or invented returns.'}
    save_new(out/'report.json',result)
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--observation',required=True)
    parser.add_argument('--output-dir',required=True)
    args=parser.parse_args()
    result=run(args.observation,args.output_dir)
    print(json.dumps({'report':str(Path(args.output_dir)/'report.json'),'status':result['decision']['status'],
        'market_nav_cents':result['paper_market_snapshot']['market_nav_cents'],'orders_sent':0}))

if __name__=='__main__':main()
