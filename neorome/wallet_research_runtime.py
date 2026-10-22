"""Persistent wallet discovery/backfill and prospective paper signal producer."""
import argparse
from datetime import datetime, timezone, timedelta
import fcntl
import hashlib
import json
import math
import re
from pathlib import Path

from .wallet_history import WalletHistoryClient, normalize_page, candidate_wallets, WalletHistoryError
from .wallet_follow import score_wallets, following_signals, apply_paper_eligibility, CURRENCIES
from .jupiter_quotes import validate_mint, QuoteError


def write(path, value):
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    tmp.chmod(0o600)
    tmp.replace(path)


NORMALIZATION_VERSION = 'wallet-swap-v2'
PROVIDER_PAPER_VERSION = 'provider-frozen-top10-v1'
PROVIDER_LEADERBOARD_SOURCE = 'https://api.fomoapi.io/v2/leaderboard/7d'
MAX_WALLET_METADATA = 512


def provider_paper_ranking(root, now=None):
    """Return today's frozen provider top-10 as a separate provisional cohort.

    This view never mutates or relaxes local research rankings. Previous-day,
    future-dated, stale, incomplete or malformed snapshots produce no cohort.
    """
    root = Path(root)
    now = now or datetime.now(timezone.utc)
    path = root / 'fomo-state.json'
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 2_000_000:
            return None
        state = json.loads(path.read_text())
        snapshot = state.get('leaderboard_snapshot')
        if not isinstance(snapshot, dict):
            return None
        if (snapshot.get('version') != PROVIDER_PAPER_VERSION
                or snapshot.get('source') != PROVIDER_LEADERBOARD_SOURCE
                or snapshot.get('limit') != 10
                or snapshot.get('day') != now.astimezone(timezone.utc).date().isoformat()):
            return None
        frozen_raw = snapshot.get('frozen_at')
        if not isinstance(frozen_raw, str):
            return None
        frozen = datetime.fromisoformat(frozen_raw.replace('Z', '+00:00'))
        if frozen.tzinfo is None or frozen.utcoffset() is None:
            return None
        frozen = frozen.astimezone(timezone.utc)
        now_utc = now.astimezone(timezone.utc)
        if frozen > now_utc or now_utc - frozen > timedelta(hours=24):
            return None
        source_entries = snapshot.get('entries')
        if not isinstance(source_entries, list):
            return None
        entries, seen_users, seen_wallets = [], set(), set()
        for item in source_entries:
            if not isinstance(item, dict) or len(entries) >= 10:
                continue
            user_id, wallet, rank = item.get('user_id'), item.get('wallet'), item.get('rank')
            pnl, trades = item.get('pnl'), item.get('trades')
            if (not isinstance(user_id, str) or not user_id or type(rank) is not int
                    or not 1 <= rank <= 10 or not isinstance(wallet, str)):
                continue
            try:
                wallet = validate_mint(wallet)
                pnl_value, trade_value = float(pnl), float(trades)
            except (ValueError, TypeError, OverflowError, QuoteError):
                continue
            if (not math.isfinite(pnl_value) or not math.isfinite(trade_value)
                    or pnl_value <= 0 or trade_value <= 0
                    or user_id in seen_users or wallet in seen_wallets):
                continue
            seen_users.add(user_id)
            seen_wallets.add(wallet)
            entries.append({'user_id': user_id, 'wallet': wallet, 'rank': rank,
                            'provider_claimed': True})
        if not entries:
            return None
        cohorts = []
        for entry in entries:
            for currency in sorted(CURRENCIES):
                cohorts.append({
                    'wallet': entry['wallet'], 'quote_mint': currency,
                    'provider_user_id': entry['user_id'],
                    'provider_rank': entry['rank'],
                    'provider_claimed': True,
                    'paper_warnings': ['provider_leaderboard_claim_unverified'],
                })
        frozen_at = frozen.isoformat().replace('+00:00', 'Z')
        return {
            'as_of': frozen_at, 'qualified': [], 'rankings': [],
            'paper_qualified': cohorts,
            'paper_evidence_label': 'provisional_provider_top10',
            'selection_basis': 'daily_frozen_provider_leaderboard',
            'provider_leaderboard': {
                'version': PROVIDER_PAPER_VERSION,
                'day': snapshot['day'], 'source': snapshot['source'], 'limit': 10,
                'frozen_at': frozen_at,
                'response_sha256': snapshot.get('response_sha256'),
                'entries': entries,
            },
        }
    except (OSError, ValueError, TypeError, AttributeError):
        return None


def rebuild_archive(root, state, page_limit=20):
    """Bounded, offline migration; original receipts and baseline remain intact."""
    if state.get('normalization_version') == NORMALIZATION_VERSION:
        return None
    baseline = root / 'state-before-wallet-swap-v2.json'
    migration = state.get('normalization_migration')
    if migration is None:
        if not baseline.exists():
            write(baseline, state)
        files = sorted(p.name for p in (root / 'pages').glob('*.json'))
        if len(files) > 10000:
            raise WalletHistoryError('normalization archive page ceiling reached')
        migration = {'version': NORMALIZATION_VERSION, 'files': files, 'index': 0,
                     'rows': {}, 'exclusions': {}, 'errors': [], 'sources': []}
        state['normalization_migration'] = migration
        # A frozen ranking based on old costs must not survive the migration.
        state['ranking'] = None
    rows = migration['rows']
    for name in migration['files'][migration['index']:migration['index'] + page_limit]:
        path = root / 'pages' / name
        try:
            if not re.fullmatch(r'[0-9a-f]{64}\.json', name) or path.is_symlink() or not path.is_file() or path.stat().st_size > 4000000:
                raise ValueError('unsafe_or_missing_archive_page')
            page = json.loads(path.read_text())
            address = page.get('address')
            if address in state['wallets'] and page.get('source_sha256') != path.stem:
                raise ValueError('archive_source_hash_mismatch')
            if address in state['wallets']:
                normalized = normalize_page(page, address)
                migration['sources'].append(page['source_sha256'])
                for row in normalized['rows']:
                    key = address + ':' + row['signature'] + ':' + row['mint'] + ':' + row['side']
                    prior = rows.get(key)
                    if prior is None or row['observed_at'] < prior['observed_at']:
                        rows[key] = row
                if len(rows) > 20000:
                    raise ValueError('normalization row ceiling reached')
                prior_exclusions = migration['exclusions'].setdefault(address, [])
                prior_exclusions.extend(normalized['exclusions'])
                migration['exclusions'][address] = prior_exclusions[-100:]
        except (ValueError, TypeError, KeyError, OSError) as exc:
            migration['errors'].append({'file': name, 'reason': type(exc).__name__})
            migration['errors'] = migration['errors'][-100:]
        migration['index'] += 1
    complete = migration['index'] == len(migration['files'])
    result = {'version': NORMALIZATION_VERSION, 'processed_pages': migration['index'],
              'total_pages': len(migration['files']), 'rebuilt_rows': len(rows),
              'errors': migration['errors'], 'complete': complete}
    if complete:
        sources = set(migration['sources'])
        uncovered = {r['wallet'] for r in state['rows'].values()
                     if r.get('source_sha256') not in sources}
        for address, meta in state['wallets'].items():
            meta['exclusions'] = migration['exclusions'].get(address, [])
            meta['normalization_review_required'] = bool(migration['errors']) or address in uncovered
        result['unrecoverable_wallets'] = sorted(uncovered)
        state['rows'] = rows
        state['normalization_version'] = NORMALIZATION_VERSION
        state['normalization_rebuild'] = result
        state['seed_day'] = None  # next discovery uses actor-aware seeds
        state.pop('normalization_migration', None)
    return result


def run(root, seed_mints, client=None, now=None, daily_credits=2000, reprocess_only=False, event_only=False):
    """At most two 20-transaction pages per run and 2,000 credits/day.

    No orders here: the frozen wallet evidence feeds separately reviewed paper
    execution. Ambiguous actors/transfers and unresolved currency costs are retained
    as exclusions or unpriced rows. Archived pages are rebuilt without requests
    before the new normalizer can qualify wallets. Seed selection is explicitly prospective, not an unbiased
    historical universe or proof that the largest holder is a successful trader.
    """
    if type(daily_credits) is not int or not 0 <= daily_credits <= 2000:
        raise ValueError('invalid daily wallet research ceiling')
    root = Path(root)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    now = now or datetime.now(timezone.utc)
    with (root / 'research.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {'status': 'busy', 'orders_sent': 0}
        state_path = root / 'state.json'
        state = json.loads(state_path.read_text()) if state_path.exists() else {
            'wallets': {}, 'rows': {}, 'seed_day': None, 'daily': {}, 'poll_index': 0}
        if isinstance(state.get('ranking'), dict) and isinstance(state['ranking'].get('rankings'), list):
            apply_paper_eligibility(state['ranking'])
        provider_ranking = provider_paper_ranking(root, now)
        migration = rebuild_archive(root, state)
        if migration is not None and (not migration['complete'] or reprocess_only):
            result = {'status': 'normalization_rebuild_complete' if migration['complete'] else 'normalization_rebuild_pending',
                      'orders_sent': 0, 'requests': 0, 'candidate_wallets': len(state['wallets']),
                      'qualified_wallets': 0, 'migration': migration}
            write(root / 'latest.json', result)
            write(root / 'signals.json', {'signals': [], 'herd_alerts': [], 'reason': 'normalization_rebuild'})
            write(root / 'ranking.json', {'qualified': [], 'rankings': [], 'reason': 'normalization_rebuild'})
            write(state_path, state)
            return result
        if reprocess_only:
            return {'status': 'normalization_current', 'orders_sent': 0, 'requests': 0,
                    'candidate_wallets': len(state['wallets']), 'qualified_wallets': 0}
        # Admit today's ten provider wallets for bounded chain-history collection.
        # Existing wallet records and receipts remain intact under a bounded cap.
        feed_path = root / 'fomo-state.json'
        if provider_ranking:
            provider_entries = provider_ranking['provider_leaderboard']['entries']
            for entry in provider_entries:
                address = entry['wallet']
                if address not in state['wallets'] and len(state['wallets']) >= MAX_WALLET_METADATA:
                    continue
                state['wallets'].setdefault(address, {'cursor': None, 'pages': 0,
                    'completed': False, 'seed_mint': None, 'exclusions': [],
                    'discovered_by': 'fomo-provider-claimed', 'fomo_user_id': entry['user_id'],
                    'provider_rank': entry['rank'],
                    'provider_snapshot_day': provider_ranking['provider_leaderboard']['day'],
                    'provider_snapshot_frozen_at': provider_ranking['as_of']})
        # Keep older feed candidates available to historical research discovery,
        # but never use them in the provider paper cohort without today's snapshot.
        if not event_only and feed_path.exists() and feed_path.stat().st_size <= 2000000:
            try:
                candidates = json.loads(feed_path.read_text()).get('candidates', {})
                for user_id, candidate in list(candidates.items())[:10]:
                    address = validate_mint(candidate['wallet'])
                    if address not in state['wallets'] and len(state['wallets']) >= MAX_WALLET_METADATA:
                        break
                    state['wallets'].setdefault(address, {'cursor': None, 'pages': 0,
                        'completed': False, 'seed_mint': None, 'exclusions': [],
                        'discovered_by': 'fomo-provider-claimed', 'fomo_user_id': user_id})
            except (ValueError, TypeError, KeyError, OSError, AttributeError, QuoteError):
                pass
        provider_wallet_set = ({entry['wallet'] for entry in provider_ranking['provider_leaderboard']['entries']}
                               if provider_ranking else set())
        qualified_set = provider_wallet_set.intersection(state['wallets'])
        pending = []
        capacity_blocked_alerts = 0
        alerts_path = root / 'fomo-alerts.json'
        if alerts_path.exists() and alerts_path.stat().st_size <= 2000000:
            try:
                for alert in json.loads(alerts_path.read_text()).get('alerts', [])[-50:]:
                    age = (now - datetime.fromisoformat(alert['provider_at'].replace('Z','+00:00'))).total_seconds()
                    if (alert['wallet'] in provider_wallet_set and 0 <= age <= 120
                            and alert['event_id'] not in state.get('fomo_verified_events', [])):
                        if alert['wallet'] in qualified_set:
                            pending.append(alert)
                        else:
                            capacity_blocked_alerts += 1
            except (ValueError, TypeError, KeyError, OSError, AttributeError, QuoteError):
                pass
        if event_only and not pending:
            return {'status': 'no_pending_vetted_fomo_event', 'requests': 0, 'orders_sent': 0,
                    'tracking_capacity_blocked_alerts': capacity_blocked_alerts}
        day = now.date().isoformat()
        used = state['daily'].get(day, 0)
        client = client or WalletHistoryClient(max_calls=2)
        calls = 0
        errors = []

        def fetch(address, cursor=None):
            nonlocal used, calls
            if calls >= 2 or used + 10 > daily_credits:
                raise WalletHistoryError('daily research credit ceiling reached')
            # Durable reservation before any attempted request, no assumed refund.
            raw = root / 'pages'
            if raw.exists() and sum(p.stat().st_size for p in raw.glob('*.json')) > 256 * 1024 * 1024:
                raise WalletHistoryError('wallet archive storage ceiling reached')
            if len(state['rows']) >= 20000:
                raise WalletHistoryError('wallet normalized row ceiling reached')
            calls += 1
            used += 10
            state['daily'][day] = used
            write(state_path, state)
            page = client.history(address, cursor)
            raw = root / 'pages'
            raw.mkdir(mode=0o700, exist_ok=True)
            write(raw / (page['source_sha256'] + '.json'), page)
            return page

        if not provider_ranking and not event_only and not pending and state['seed_day'] != day and seed_mints:
            try:
                p = fetch(seed_mints[0])
                for w in candidate_wallets(p, 3):
                    if w not in state['wallets'] and len(state['wallets']) >= MAX_WALLET_METADATA:
                        break
                    state['wallets'].setdefault(w, {'cursor': None, 'pages': 0,
                        'completed': False, 'seed_mint': seed_mints[0], 'exclusions': []})
                state['seed_day'] = day
            except WalletHistoryError as exc:
                errors.append(str(exc))

        # Qualified wallets are polled first. Others backfill in round-robin,
        # up to 20 pages each; bounded history remains explicitly incomplete.
        prior = state.get('ranking')
        prior_cohort = (prior or {}).get('paper_qualified', (prior or {}).get('qualified', []))
        local_qualified = [x['wallet'] for x in prior_cohort if isinstance(x, dict) and x.get('wallet')]
        provider_wallets = ([entry['wallet'] for entry in provider_ranking['provider_leaderboard']['entries']]
                            if provider_ranking else [])
        qualified = list(dict.fromkeys(provider_wallets + local_qualified))
        addresses = list(dict.fromkeys(a['wallet'] for a in pending)) or list(dict.fromkeys(qualified)) or sorted(state['wallets'])
        if addresses and calls < 2 and (not provider_ranking or bool(pending)):
            index = 0 if pending else state['poll_index'] % len(addresses)
            address = addresses[index]
            state['poll_index'] += 1
            meta = state['wallets'][address]
            try:
                backfill = not pending and not provider_ranking and address not in local_qualified and not meta['completed'] and meta['pages'] < 20
                p = fetch(address, meta['cursor'] if backfill else None)
                n = normalize_page(p, address)
                for row in n['rows']:
                    key = address + ':' + row['signature'] + ':' + row['mint'] + ':' + row['side']
                    # Keep first observation time; refetch never makes an old trade fresh.
                    state['rows'].setdefault(key, row)
                meta['exclusions'] = (meta['exclusions'] + n['exclusions'])[-100:]
                meta['last_received_at'] = p['received_at']
                if pending:
                    seen = state.get('fomo_verified_events', [])
                    matched_events = []
                    for alert in pending:
                        if alert['wallet'] != address:
                            continue
                        for row in n['rows']:
                            if row['mint'] != alert.get('mint') or row['side'] != alert.get('side'):
                                continue
                            if alert.get('signature') and row['signature'] != alert['signature']:
                                continue
                            delta = abs((datetime.fromisoformat(row['block_time'].replace('Z','+00:00')) - datetime.fromisoformat(alert['provider_at'].replace('Z','+00:00'))).total_seconds())
                            if delta <= 45:
                                matched_events.append(alert['event_id'])
                                break
                    # Retry within freshness window if provider indexing lags the alert.
                    state['fomo_verified_events'] = (seen + matched_events)[-128:]
                if backfill:
                    meta['pages'] += 1
                    meta['cursor'] = p['pagination_token']
                    meta['completed'] = p['pagination_token'] is None
            except WalletHistoryError as exc:
                errors.append(str(exc))

        # Decisions use the previous ranking, never a ranking fitted on the buy
        # being followed. Freeze fresh research rankings at most every six hours.
        decision_at = datetime.now(timezone.utc) if client.__class__ is WalletHistoryClient else now
        rows = list(state['rows'].values())
        signals = following_signals(rows, provider_ranking, decision_at.isoformat(), eligibility='paper') if provider_ranking else {
            'as_of': decision_at.isoformat(), 'signals': [], 'herd_alerts': [],
            'reason': 'no_current_frozen_provider_top10',
            'evidence_label': 'provisional_provider_top10'}
        expired = prior is None or decision_at - datetime.fromisoformat(prior['as_of'].replace('Z', '+00:00')) >= timedelta(hours=6)
        if expired:
            ranking = score_wallets(rows, decision_at.isoformat())
            # Unparsed successful activity can hide disposals/costs. Flag affected
            # wallets for explicit review rather than silently cherry-pick swaps.
            for item in ranking['rankings']:
                w = state['wallets'][item['wallet']]
                risky = any(e['reason'] not in {'failed_or_unparsed_transaction', 'stablecoin_not_memecoin_candidate'} for e in w['exclusions'])
                if risky or not w['completed'] or w.get('normalization_review_required'):
                    item['qualified'] = False
                    item['ineligibility_reasons'].append('history_or_excluded_activity_review_required')
            ranking['qualified'] = [r for r in ranking['rankings'] if r['qualified']]
            apply_paper_eligibility(ranking)
            state['ranking'] = ranking
            write(root / 'ranking.json', ranking)
        result = {'schema_version': 'wallet-follow-research-v1', 'at': decision_at.isoformat(),
            'status': 'research_active', 'orders_sent': 0, 'requests': calls,
            'daily_credits_reserved': used, 'daily_credit_ceiling': daily_credits,
            'candidate_wallets': len(state['wallets']), 'normalized_swaps': len(rows),
            'qualified_wallets': len(state['ranking']['qualified']),
            'paper_qualified_wallets': len(state['ranking'].get('paper_qualified', state['ranking']['qualified'])),
            'provider_paper_wallets': (len(provider_ranking['provider_leaderboard']['entries'])
                                       if provider_ranking else 0),
            'fomo_candidates': sum(w.get('discovered_by') == 'fomo-provider-claimed' for w in state['wallets'].values()),
            'fomo_pending_vetted_alerts': len(pending),
            'history_backfill_status': ('deferred_without_fresh_top10_alert'
                                        if provider_ranking and not pending else 'available'),
            'signals': signals, 'errors': errors,
            'execution_status': ('paper_bridge_configured' if (root / 'paper-latest.json').exists() else 'paper_bridge_pending_startup'),
            'universe_caveat': 'current candidate mint traders; selection-biased research sample'}
        write(root / 'latest.json', result)
        write(state_path, state)
        return result


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument('--state-dir', required=True)
    p.add_argument('--seed-mint', action='append', default=[])
    p.add_argument('--candidate-dir')
    p.add_argument('--reprocess-only', action='store_true')
    args = p.parse_args(argv)
    seeds = args.seed_mint
    if args.candidate_dir:
        files = sorted(Path(args.candidate_dir).glob('*.json'), key=lambda p: p.stat().st_mtime, reverse=True)
        for path in files[:3]:
            try:
                mint = json.loads(path.read_text()).get('mint')
                if mint:
                    seeds = [mint] + seeds
                    break
            except (ValueError, OSError):
                continue
    result = run(args.state_dir, seeds, reprocess_only=args.reprocess_only)
    if result.get('status') == 'busy':
        summary = {'status': 'busy', 'requests': 0, 'orders_sent': 0}
    else:
        summary = {k: result[k] for k in ('status', 'requests', 'candidate_wallets', 'qualified_wallets', 'orders_sent')}
    print(json.dumps(summary))
    return 0


if __name__ == '__main__':
    main()
