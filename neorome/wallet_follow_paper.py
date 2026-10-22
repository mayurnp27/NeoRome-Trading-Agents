"""W1 wallet selection with the existing E1 paper execution/lifecycle engine.

W1 has its own $300 ledger. It is not the existing E1 sleeve: only the reviewed
E1 execution mechanics are reused. No observed wallet entry price is a fill.
"""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

from .crypto_research import Sources, BudgetOpener
from .wallet_follow import following_signals
from .wallet_research_runtime import write, run as collect_wallet_history, provider_paper_ranking


class WalletSources(Sources):
    def __init__(self, evidence, opener=None):
        super().__init__(opener or BudgetOpener(16))
        self.evidence = evidence

    def profiles(self):
        now = datetime.now(timezone.utc).isoformat()
        signals = self.evidence['signals']
        digest = hashlib.sha256(json.dumps(self.evidence, sort_keys=True).encode()).hexdigest()
        mints = list(dict.fromkeys(s['mint'] for s in signals))[:3]
        return ([{'chainId': 'solana', 'tokenAddress': m} for m in mints],
                {'source': 'W1-provider-top10-frozen-leaderboard', 'requested_at': now,
                 'received_at': now, 'response_sha256': digest,
                 'wallet_follow_evidence': self.evidence})


    def _disabled_general_discovery(self, kind):
        now = datetime.now(timezone.utc).isoformat()
        payload = {'data': []}
        return payload, {'source': 'W1-disabled-' + kind,
            'requested_at': now, 'received_at': now,
            'response_sha256': hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest(),
            'reason': 'wallet_only_source_boundary', 'network_requests': 0}

    def trending_pools(self):
        return self._disabled_general_discovery('trending-pools')

    def new_pools(self):
        return self._disabled_general_discovery('new-pools')


def run(root, executor=None, now=None):
    root = Path(root)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    live_clock = now is None
    now = now or datetime.now(timezone.utc)
    # Pushed vetted-wallet alerts trigger bounded chain verification before selection.
    collection = collect_wallet_history(root, [], now=now, event_only=True) if (root / 'fomo-alerts.json').exists() else None
    if live_clock:
        now = datetime.now(timezone.utc)
    state_path = root / 'state.json'
    state = json.loads(state_path.read_text()) if state_path.exists() else {}
    ranking = provider_paper_ranking(root, now)
    evidence = (following_signals(list(state.get('rows', {}).values()), ranking, now.isoformat(),
                                  eligibility='paper') if ranking else {
        'as_of': now.isoformat(), 'ranking_cutoff': None, 'paper_only': True,
        'eligibility': 'provider_top10', 'evidence_label': 'provisional_provider_top10',
        'signals': [], 'herd_alerts': [], 'issues': [],
        'reason': 'no_current_frozen_provider_top10',
    })
    if executor is None:
        from .token_memecoin_runtime import run_once
        executor = run_once
    # Empty selection still calls the lifecycle engine so existing positions
    # receive marks/exits. All entry routes/controls are recollected at our size.
    result = executor(state_dir=root / 'paper-state', ledger_path=root / 'paper.sqlite3',
        sources=WalletSources(evidence), cohort='E1', crypto_sleeve_cents=30000,
        entry_cap_cents=1500, entry_fraction_bps=500,
        learning_report_path=None,
        learning_cohort_id="W1")
    report = {'strategy': 'W1-wallet-follow-v3', 'selection_version': 'provider-frozen-top10-wallet-only-v2',
        'execution_engine': 'E1-sized-v2',
        'learning_status': 'quarantined_legacy_mixed_source_outcomes',
        'at': now.isoformat(), 'separate_simulated_starting_cash_cents': 30000,
        'wallet_signals': evidence, 'fomo_event_verification': collection, 'paper_result': result, 'orders_sent': 0,
        'wallet_normalization_version': state.get('normalization_version', 'wallet-swap-v1'),
        'wallet_eligibility': 'provider_top10',
        'wallet_evidence_label': 'provisional_provider_top10',
        'provider_leaderboard': (ranking.get('provider_leaderboard') if ranking else None),
        'caveat': 'wallet-only discovery; legacy W1 general-feed fills preserved but excluded from active learning; E1 execution mechanics reused'}
    write(root / 'paper-latest.json', report)
    return report


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--state-dir', required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.state_dir), sort_keys=True))
