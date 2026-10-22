"""Finite offline compatibility rehearsal of an existing synthetic bot export."""
import argparse
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from .paper_ledger import PaperLedger
from .prediction_bridge import reserve


def run(export_path, config_path, output_dir):
    raw=Path(export_path).read_bytes(); config_raw=Path(config_path).read_bytes()
    exported=json.loads(raw); config=json.loads(config_raw)
    if exported.get('synthetic') is not True:
        raise ValueError('Synthetic demo export required')
    candidates=exported['strategy_results']
    times={x['proposal']['created_at'] for x in candidates if x.get('proposal')}
    if len(times)!=1:
        raise ValueError('Rehearsal requires one common synthetic decision time')
    now=datetime.fromtimestamp(times.pop(),timezone.utc)
    out=Path(output_dir);out.mkdir(parents=True,exist_ok=False,mode=0o700)
    # An isolated software-test book, not the operational paper portfolio.
    book=PaperLedger(out/'fixture.sqlite3',enforce_market_risk=True,clock=lambda:now)
    book.record_marks('fixture-cash',[],as_of=now)
    records=[]
    for candidate in candidates:
        p=candidate.get('proposal')
        mapping={} if not p else {p['event_id']:'synthetic-event:'+p['event_id']}
        record={'strategy':p['strategy_id'] if p else None,'synthetic':True}
        try:
            reserved=reserve(candidate,config,book,now=now,exposure_groups=mapping,risk_policy='firm_shared_v1')
            if reserved:
                assert reserve(candidate,config,book,now=now,exposure_groups=mapping,risk_policy='firm_shared_v1')==reserved
                record['reservation']=asdict(reserved)
                record['reserved_snapshot']=asdict(book.snapshot())
                book.cancel(reserved.reservation_id,reason='Offline compatibility check complete; no simulated fill')
            else:record['abstention']=candidate.get('reason')
        except ValueError as exc:
            record['rejection']=str(exc)
        records.append(record)
    report={'mode':'synthetic_export_compatibility_only','source_export_sha256':hashlib.sha256(raw).hexdigest(),
       'source_config_sha256':hashlib.sha256(config_raw).hexdigest(),'fixture_as_of':now.isoformat(),
       'risk_policy':'firm_shared_v1','source_cash_and_risk_policy_replaced':True,
       'decisions':records,'final':asdict(book.snapshot()),'network_calls':0,'broker_orders_sent':0,
       'simulated_fills':0,'profitability_validated':False,
       'notice':'Isolated test book. Original demo prices, probabilities, forecasts and outcomes are invented. No live portfolio balance changed.'}
    (out/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    return report

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--export',required=True);parser.add_argument('--config',required=True);parser.add_argument('--output-dir',required=True)
    args=parser.parse_args(); report=run(args.export,args.config,args.output_dir)
    print(json.dumps({'report':str(Path(args.output_dir)/'report.json'),'simulated_fills':0,'decisions':report['decisions']}))
