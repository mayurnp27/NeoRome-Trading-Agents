"""Capture and publish one exact-leg, read-only options lifecycle event."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import re

from .options_candidate_inbox import read_current_candidate
from .options_cloud_evidence import collect_with_cloud_data
from .options_lifecycle_inbox import LifecycleInboxError, PHASES, publish

UTC=timezone.utc

def _event_id(phase:str, receipt:str)->str:
    return phase+"-"+re.sub(r"[^0-9a-z]+","-",receipt.lower()).strip("-")

def main(argv=None)->int:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase",choices=sorted(PHASES),required=True)
    parser.add_argument("--candidate-inbox-root",required=True)
    parser.add_argument("--lifecycle-inbox-root",required=True)
    parser.add_argument("--tokens",default="/var/lib/neorome-options-auth/tokens.json")
    args=parser.parse_args(argv)
    try:
        candidate=read_current_candidate(args.candidate_inbox_root)
        selected=candidate["shadow_candidate"]; long,short=selected["long"],selected["short"]
        symbol=str(selected["symbol"]); right="call" if long["right"]=="C" else "put"
        expected={str(long["symbol"]),str(short["symbol"])}
        capture=collect_with_cloud_data(token_path=args.tokens,symbol=symbol,right=right)
        receipt=str(capture["local_receipt_at"]); date=datetime.fromisoformat(receipt.replace("Z","+00:00")).astimezone(UTC).date().isoformat()
        path=publish(phase=args.phase,event_id=_event_id(args.phase,receipt),date=date,capture=capture,expected_symbols=expected,root=args.lifecycle_inbox_root)
        result={"status":"captured","phase":args.phase,"artifact_path":str(path),"orders_sent":0}
    except Exception as exc:
        result={"status":"abstain","reason_code":type(exc).__name__,"orders_sent":0}
    import json;print(json.dumps(result,sort_keys=True));return 0
if __name__=="__main__": raise SystemExit(main())
