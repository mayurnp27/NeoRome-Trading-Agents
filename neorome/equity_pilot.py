"""Finite, persistent SPY internal-paper pilot. No broker submission path."""
from __future__ import annotations
import argparse
from dataclasses import asdict
from datetime import datetime,timedelta,timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import uuid
from desks.equities.paper_policy import evaluate,value_position
from .equity_data import stamp
from .paper_ledger import PaperLedger,Proposal,MarketMark,LedgerError
from .jupiter_quotes import save_new

UTC=timezone.utc

def positions(book):
    events=book.audit()
    proposals={e.payload['proposal_id']:e.payload['payload'] for e in events if e.event_type=='reserve'}
    ids={e.payload['position_id'] for e in events if e.event_type=='fill'}
    return [(p,proposals[p.proposal_id]) for p in (book.get_position(i) for i in sorted(ids)) if p.state=='open']


def run(bundle_path,state_dir,now=None):
    now=now or datetime.now(UTC)
    raw=Path(bundle_path).read_bytes();bundle=json.loads(raw);digest=hashlib.sha256(raw).hexdigest()
    if bundle.get('source_kind')!='observed' or bundle.get('order_mutations')!=0:
        raise ValueError('Only observed read-only bundles accepted')
    observation=bundle['observation'];context=bundle['context']
    if observation.get('source_kind')!='observed' or context.get('source_kind')!='observed':
        raise ValueError('Mixed or synthetic source provenance refused')
    decision=evaluate(observation,context,now)
    root=Path(state_dir)
    if root.is_symlink():raise ValueError('State directory symlink refused')
    root.mkdir(parents=True,exist_ok=True,mode=0o700)
    descriptor=os.open(root/'pilot.lock',os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        book=PaperLedger(root/'paper.sqlite3',enforce_market_risk=True,clock=lambda:now)
        batch=uuid.uuid4().hex
        current=positions(book)
        for p,proposal in current:
            if p.desk!='equities' or p.instrument!='SPY' or proposal['strategy']!='EQ-ETF-TREND-COMPARATOR':
                raise ValueError('Pilot cannot value a position owned by another strategy')
        actions=[];blocker=None
        # Empty-book cash can always be reconciled; open positions need valid data.
        try:
            if not current:
                book.record_marks('cash-'+batch,[],as_of=now)
            elif decision['market_inputs_valid']:
                marks=[]
                for p,proposal in current:
                    valued=value_position(proposal['metadata']['instrument_units'],observation['quote'])
                    marks.append(MarketMark(p.position_id,valued['liquidation_value_cents'],
                        observation['quote']['timestamp'],digest,'observed'))
                book.record_marks('quotes-'+batch,marks,as_of=now)
            if current and decision['market_inputs_valid'] and decision['rebalance_due'] and decision['trend_positive'] is False:
                for p,proposal in current:
                    valued=value_position(proposal['metadata']['instrument_units'],observation['quote'])
                    closed=book.close(p.position_id,proceeds_cents=valued['gross_exit_cents'],
                        fee_cents=valued['exit_fee_cents'],provenance='observed')
                    actions.append({'type':'simulated_close','result':asdict(closed)})
                book.record_marks('flat-'+batch,[],as_of=now)
            elif not current and decision['eligible_for_internal_paper']:
                month=now.strftime('%Y-%m')
                # An economic idea has one fixed monthly ID; restart/repeated
                # observations cannot open another lot under a fresh random ID.
                idea='spy-monthly-'+month
                proposal=Proposal(idea,'equities','EQ-ETF-TREND-COMPARATOR',decision['strategy_version'],
                    digest,observation['quote']['timestamp'],'SPY','us-broad-equity',idea,
                    now+timedelta(seconds=60),1,decision['entry_cost_cents']-decision['entry_fee_cents'],
                    decision['entry_fee_cents'],'observed',
                    {'instrument_units':decision['units'],'lot_kind':'aggregate_funded_proposal',
                     'cost_policy':decision.get('cost_model'),'calendar_evidence':context['evidence']})
                reservation=book.reserve(proposal)
                filled=book.simulate_fill(reservation.reservation_id,provenance='observed')
                actions.append({'type':'simulated_fill','result':asdict(filled)})
                # The pre-fill quote cannot be recycled as a post-fill mark.
                # A subsequent fresh observation must value the new position.
            snapshot=asdict(book.market_snapshot(as_of=now))
        except (LedgerError,ValueError) as exc:
            blocker=type(exc).__name__+': '+str(exc)
            if isinstance(exc,ValueError) and current:
                book.set_risk_stop(True,reason='Open-position valuation unavailable')
                snapshot=None
            else:
                try:snapshot=asdict(book.market_snapshot(as_of=now))
                except LedgerError:snapshot=None
        report={'mode':'observed_internal_paper_pilot','source_kind':'observed','evaluated_at':now.isoformat(),
            'bundle_sha256':digest,'decision':decision,'actions':actions,'blocker':blocker,
            'market_snapshot':snapshot,'cost_snapshot':asdict(book.snapshot()),
            'balances':[asdict(x) for x in book.balances()],
            'broker_orders_sent':0,'broker_execution_enabled':False,
            'fill_model':'size-bounded IEX quote with adverse slippage and modeled costs; not a guaranteed broker fill',
            'profitability_validated':False}
        target=root/'reports'/('decision-'+batch+'.json');save_new(target,report)
        report['report_path']=str(target)
        return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle',required=True)
    parser.add_argument('--state-dir',required=True)
    args=parser.parse_args();report=run(args.bundle,args.state_dir)
    print(json.dumps({'report':report['report_path'],'simulated_actions':len(report['actions']),
        'blocker':report['blocker'],'reasons':report['decision']['reasons'],'broker_orders_sent':0}))

if __name__=='__main__':main()
