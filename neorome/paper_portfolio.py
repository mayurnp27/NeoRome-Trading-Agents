"""Initialize and inspect the durable internal paper portfolio; never sends orders."""
import argparse
from dataclasses import asdict
import json
import os
from pathlib import Path

from .paper_ledger import PaperLedger, InvalidState


def inspect(state):
    root = Path(state)
    if root.is_symlink():
        raise ValueError('State symlinks are refused')
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    ledger = PaperLedger(root / 'paper.sqlite3', enforce_market_risk=True)
    # Reject accidentally reusing a synthetic rehearsal as the observed book.
    for event in ledger.audit():
        if event.event_type == 'reserve' and event.payload.get('payload', {}).get('provenance') != 'observed':
            raise ValueError('Observed portfolio contains non-observed reservation')
    try:
        market = asdict(ledger.market_snapshot())
    except InvalidState:
        market = {'status': 'complete_current_marks_required'}
    return {
        'mode': 'internal_paper', 'broker_execution_enabled': False,
        'strategy_scheduler_enabled': None,
        'strategy_scheduler_status_source': 'paper runtime status and service manager; not inferred from a ledger file',
        'market_risk_enforced': True,
        'snapshot': asdict(ledger.snapshot()),
        'market_snapshot': market,
        'desk_budgets_cents': {'prediction': 10000, 'equities': 30000, 'options': 30000, 'crypto': 10000},
        'activation_requirement': 'Reviewed strategy adapters, observed arrival-price fills and exit/mark lifecycle per desk',
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state', required=True)
    args = parser.parse_args()
    os.umask(0o077)
    print(json.dumps(inspect(args.state), sort_keys=True))


if __name__ == '__main__':
    main()
