"""Offline loss-control regression scenario; constructed prices, no broker."""
from dataclasses import asdict
from datetime import datetime,timedelta,timezone
import argparse
import json
from pathlib import Path
from .paper_ledger import PaperLedger,Proposal,MarketMark,RiskStopActive
from .jupiter_quotes import save_new

def run(output_dir):
    out=Path(output_dir);out.mkdir(parents=True,exist_ok=False,mode=0o700)
    now=datetime(2026,9,18,12,tzinfo=timezone.utc)
    book=PaperLedger(out/'paper.sqlite3',enforce_market_risk=True,clock=lambda:now)
    initial=book.record_marks('cash',[],as_of=now)
    proposal=Proposal('fixture-entry','equities','loss-control-fixture','1','a'*64,now,
        'SYNTH-EQUITY','synthetic-equity','fixture-idea',now+timedelta(minutes=5),1,1500)
    res=book.reserve(proposal)
    fill=book.simulate_fill(res.reservation_id)
    now+=timedelta(seconds=1)
    dropped=book.record_marks('fixture-drop',[
        MarketMark(fill.position_id,500,now,'b'*64,'synthetic')],as_of=now)
    assert dropped.market_nav_cents==79000
    assert dropped.automatic_risk_latched and not dropped.entry_allowed
    book.set_risk_stop(False,reason='fixture attempt to clear only manual stop')
    book=PaperLedger(out/'paper.sqlite3',enforce_market_risk=True,clock=lambda:now)
    persisted=book.market_snapshot()
    assert persisted.automatic_risk_latched and not persisted.entry_allowed
    next_proposal=Proposal('blocked-entry','crypto','loss-control-fixture','1','c'*64,now,
        'SYNTH-TOKEN','synthetic-crypto','other-idea',now+timedelta(minutes=5),1,100)
    blocked=False
    try:book.reserve(next_proposal)
    except RiskStopActive:blocked=True
    assert blocked
    record={'mode':'synthetic_loss_control_regression','orders_sent':0,
        'initial':asdict(initial),'after_constructed_loss':asdict(dropped),
        'after_restart_and_manual_clear':asdict(persisted),'new_entry_blocked':blocked,
        'note':'A constructed $10 mark loss checks the 1% daily-loss gate. Not investment performance.'}
    save_new(out/'report.json',record)
    return record

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',required=True)
    args=parser.parse_args();record=run(args.output_dir)
    print(json.dumps({'report':str(Path(args.output_dir)/'report.json'),'new_entry_blocked':record['new_entry_blocked']}))

if __name__=='__main__':main()
