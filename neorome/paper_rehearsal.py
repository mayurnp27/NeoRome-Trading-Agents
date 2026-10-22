"""One finite, offline, synthetic four-desk rehearsal. Never sends orders."""
from __future__ import annotations
import argparse
from dataclasses import asdict, replace
from datetime import datetime, timedelta
from decimal import Decimal
import hashlib
import json
from pathlib import Path

from desks.paper_candidates import screen_candidate
from neorome.paper_ledger import PaperLedger, Proposal, RiskStopActive

DEFAULT_FIXTURE = Path(__file__).resolve().parents[1] / 'examples/paper/four_desks.json'


def json_default(value):
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(type(value).__name__)


def run(output_dir, fixture_path=DEFAULT_FIXTURE):
    """Require a new directory; keep every decision and a durable SQLite ledger."""
    raw = Path(fixture_path).read_bytes()
    fixture = json.loads(raw)
    if fixture.get('source_kind') != 'synthetic':
        raise ValueError('This rehearsal accepts synthetic fixtures only')
    now = datetime.fromisoformat(fixture['as_of'].replace('Z', '+00:00'))
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=False, mode=0o700)
    ledger_path = out / 'paper.sqlite3'
    ledger = PaperLedger(ledger_path, clock=lambda: now)
    initial = asdict(ledger.snapshot())
    decisions, filled, proposals = [], [], []
    source_hash = hashlib.sha256(raw).hexdigest()
    for case in fixture['cases']:
        decision = screen_candidate(case['desk'], case['observation'], as_of=now)
        record = {'case_id': case['id'], 'instrument': case['instrument'],
                  'source_kind': 'synthetic', 'decision': decision}
        decisions.append(record)
        if not decision['accepted']:
            continue
        # One ledger lot represents the entire fully funded screened proposal.
        # Instrument units are retained separately; this is not a broker quantity.
        proposal = Proposal(
            proposal_id=case['id'], desk=case['desk'], strategy='synthetic-rehearsal',
            strategy_version='1', source_hash=source_hash, source_timestamp=now,
            instrument=case['instrument'], exposure_group=case['group'], idea_id=case['id'],
            expiry_at=now + timedelta(minutes=5), quantity=1,
            unit_cost_cents=decision['cost_cents'], provenance='synthetic',
            metadata={'lot_kind': 'aggregate_funded_proposal',
                      'instrument_units': str(decision['units']), 'all_in_cost_includes_entry_fees': True,
                      'observation': case['observation']})
        reservation = ledger.reserve(proposal)
        # Retry must return the same reservation, not reserve cash again.
        assert ledger.reserve(proposal) == reservation
        fill = ledger.simulate_fill(reservation.reservation_id)
        assert ledger.simulate_fill(reservation.reservation_id) == fill
        record.update(reservation=asdict(reservation), fill=asdict(fill))
        proposals.append(proposal)
        filled.append((case, fill, record))
    opened = asdict(ledger.snapshot())
    # Restart with every position open, then settle the constructed outcomes.
    ledger = PaperLedger(ledger_path, clock=lambda: now)
    assert asdict(ledger.snapshot()) == opened
    for case, fill, record in filled:
        kwargs = {'proceeds_cents': case['gross_exit_cents'], 'fee_cents': case['exit_fee_cents']}
        result = ledger.close(fill.position_id, **kwargs)
        assert ledger.close(fill.position_id, **kwargs) == result
        record['close'] = asdict(result)
    # A cancelled reservation releases funds exactly once.
    cancelled = ledger.reserve(replace(proposals[0], proposal_id='cancel-demo', idea_id='cancel-demo'))
    ledger.cancel(cancelled.reservation_id, reason='synthetic cancellation')
    ledger.cancel(cancelled.reservation_id, reason='synthetic cancellation')
    ledger.set_risk_stop(True, reason='rehearsal finished; new entries stopped')
    stopped = False
    try:
        ledger.reserve(replace(proposals[0], proposal_id='stop-demo', idea_id='stop-demo'))
    except RiskStopActive:
        stopped = True
    assert stopped
    ledger = PaperLedger(ledger_path, clock=lambda: now)
    final = asdict(ledger.snapshot())
    assert ledger.risk_stop_active()
    assert final['reserved_cash_cents'] == 0 and final['open_position_cost_cents'] == 0
    assert final['paper_nav_cents'] == initial['paper_nav_cents'] + final['realized_pnl_cents']
    result = {
        'mode': 'synthetic_internal_rehearsal', 'execution_enabled': False,
        'broker_requests': 0, 'market_strategy_validated': False,
        'fixture_sha256': source_hash, 'initial': initial, 'after_fills': opened,
        'final': final, 'desk_balances': [asdict(b) for b in ledger.balances()],
        'decisions': decisions, 'audit_event_count': len(ledger.audit()),
        'checks': {'restart_preserved_open_positions': True, 'retries_idempotent': True,
                   'cancel_releases_once': True, 'risk_stop_persisted': stopped,
                   'cash_conservation': True},
        'limitations': ['Constructed prices and exits; not historical or forward performance.',
                        'Only whole proposal-lot fills/closes; no partial fills or option lifecycle.',
                        'Cost-basis NAV; no market marks or automatic drawdown detection.',
                        'Screening is sizing validation, not evidence of a profitable signal.']}
    with (out / 'report.json').open('x') as stream:
        json.dump(result, stream, indent=2, default=json_default)
        stream.write('\n')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', required=True, help='New output directory; never overwrites a run')
    args = parser.parse_args()
    report = run(args.output_dir)
    print(json.dumps({'mode': report['mode'], 'report': str(Path(args.output_dir) / 'report.json'),
                      'initial_cents': report['initial']['paper_nav_cents'],
                      'final_cents': report['final']['paper_nav_cents'], 'broker_requests': 0}))


if __name__ == '__main__':
    main()
