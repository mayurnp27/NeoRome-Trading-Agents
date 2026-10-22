"""Small prospective Solana research batches; all candidates remain non-tradable."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from .jupiter_quotes import QuoteClient, QuoteError, NoRedirect, SOL, USDC, collect, save_new, validate_mint

DEX = 'https://api.dexscreener.com'
HELIUS = 'https://mainnet.helius-rpc.com/'
TOKEN_PROGRAM = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
TOKEN_2022_PROGRAM = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'
POLICY = {'version': 'crypto-discovery-v3', 'min_pool_liquidity_usd': '15000',
          'min_pool_age_hours': Decimal('0.05'), 'quote_input_usdc_base_units': '4800000',
          'max_candidates': 3, 'max_rpc_slot_skew': 150}
UNRESOLVED = ['beneficial_owner_concentration', 'pool_vault_and_liquidity_control',
              'wallet_sell_simulation', 'realistic_fills_and_total_fees', 'shared_portfolio_risk_and_ledger']


def utc_now():
    return datetime.now(timezone.utc)


def decimal_value(value):
    if type(value) not in (str, int, float):
        raise QuoteError('Missing numeric research field')
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        raise QuoteError('Invalid numeric research field') from None
    if not number.is_finite() or number < 0:
        raise QuoteError('Invalid numeric research field')
    return number


def integer(value, minimum=0):
    if type(value) is not int or value < minimum:
        raise QuoteError('Invalid integer research field')
    return value


def raw_integer(value):
    if not isinstance(value, str) or not re.fullmatch(r'[0-9]{1,20}', value):
        raise QuoteError('Invalid raw token amount')
    result = int(value)
    if result > 2**64 - 1:
        raise QuoteError('Raw token amount exceeds u64')
    return result


def helius_key():
    environment = os.environ.get('NEOROME_HELIUS_API_KEY')
    if environment:
        if not re.fullmatch(r'[\x21-\x7e]+', environment):
            raise QuoteError('Invalid Helius credential format')
        return environment
    try:
        result = subprocess.run(['/usr/bin/security', 'find-generic-password', '-s', 'neorome.helius.api-key', '-w'],
                                capture_output=True, timeout=30)
        if result.returncode:
            raise QuoteError('Helius credential unavailable')
        key = result.stdout.decode().rstrip('\r\n')
    except (OSError, subprocess.SubprocessError, UnicodeError):
        raise QuoteError('Helius credential unavailable') from None
    if not key or not re.fullmatch(r'[\x21-\x7e]+', key):
        raise QuoteError('Invalid Helius credential format')
    return key


class BudgetOpener:
    """Count attempts (including failures) and restrict all outbound origins."""
    def __init__(self, limit, opener=None, clock=time.monotonic, sleep=time.sleep):
        if type(limit) is not int or not 1 <= limit <= 16:
            raise QuoteError('Invalid request budget')
        self.limit, self.used = limit, 0
        self.by_host, self.last = {}, {}
        self.opener = opener or urllib.request.build_opener(NoRedirect)
        self.clock, self.sleep = clock, sleep

    def open(self, request, timeout=20):
        url = urllib.parse.urlsplit(request.full_url)
        if url.scheme != 'https' or url.netloc not in {'api.dexscreener.com', 'mainnet.helius-rpc.com', 'api.jup.ag'}:
            raise QuoteError('Research endpoint refused')
        method = request.get_method()
        if url.netloc == 'api.jup.ag':
            allowed = method == 'GET' and url.path == '/swap/v2/order'
        elif url.netloc == 'api.dexscreener.com':
            allowed = method == 'GET' and (url.path == '/token-profiles/latest/v1' or
                re.fullmatch(r'/token-pairs/v1/solana/[1-9A-HJ-NP-Za-km-z]{32,44}', url.path) is not None)
        else:
            try:
                rpc = json.loads(request.data or b'{}')
                allowed = method == 'POST' and url.path == '/' and isinstance(rpc, dict) and rpc.get('method') in ('getAccountInfo', 'getTokenLargestAccounts')
            except (ValueError, UnicodeError): allowed = False
        if not allowed or self.used >= self.limit:
            raise QuoteError('Research request budget exhausted or method refused')
        delay = 1.05 if url.netloc == 'api.dexscreener.com' else 0.55
        wait = delay - (self.clock() - self.last.get(url.netloc, float('-inf')))
        if wait > 0: self.sleep(wait)
        self.used += 1
        self.by_host[url.netloc] = self.by_host.get(url.netloc, 0) + 1
        self.last[url.netloc] = self.clock()
        return self.opener.open(request, timeout=timeout)


class Sources:
    def __init__(self, opener, credential_loader=helius_key):
        self.opener, self.credential_loader = opener, credential_loader

    def _read(self, url, source, payload=None):
        started = utc_now().isoformat()
        req = urllib.request.Request(url, data=None if payload is None else json.dumps(payload).encode(),
              headers={'Accept': 'application/json', 'Content-Type': 'application/json', 'User-Agent': 'NeoRome-Research/1.0'},
              method='GET' if payload is None else 'POST')
        try:
            with self.opener.open(req, timeout=20) as response:
                if response.status != 200: raise QuoteError('Research provider rejected request')
                body = response.read(1_000_001)
        except urllib.error.HTTPError as error:
            status = error.code
            try: error.close()
            except Exception: pass
            raise QuoteError(f'Research provider HTTP {status}') from None
        except QuoteError:
            raise
        except Exception:
            raise QuoteError('Research provider transport failed') from None
        if len(body) > 1_000_000: raise QuoteError('Research response too large')
        try: data = json.loads(body)
        except (ValueError, UnicodeError): raise QuoteError('Invalid research JSON') from None
        return data, {'source': source, 'requested_at': started, 'received_at': utc_now().isoformat(),
                      'response_sha256': hashlib.sha256(body).hexdigest()}

    def profiles(self):
        return self._read(DEX + '/token-profiles/latest/v1', 'dexscreener:latest-profiles')

    def pairs(self, mint):
        validate_mint(mint)
        return self._read(DEX + '/token-pairs/v1/solana/' + mint, 'dexscreener:token-pairs')

    def rpc(self, method, mint):
        if method not in ('getAccountInfo', 'getTokenLargestAccounts'):
            raise QuoteError('RPC method is not a permitted read')
        validate_mint(mint)
        config = {'commitment': 'finalized'}
        if method == 'getAccountInfo': config['encoding'] = 'jsonParsed'
        data, evidence = self._read(HELIUS + '?' + urllib.parse.urlencode({'api-key': self.credential_loader()}),
               'helius:' + method, {'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': [mint, config]})
        if not isinstance(data, dict) or type(data.get('id')) is not int or data.get('id') != 1 or data.get('jsonrpc') != '2.0' or 'error' in data or not isinstance(data.get('result'), dict):
            raise QuoteError('Invalid or failed Helius RPC response')
        return data['result'], evidence


def discover(profiles, limit):
    if isinstance(profiles, dict) and profiles.get('chainId'): profiles = [profiles]
    if not isinstance(profiles, list): raise QuoteError('Invalid discovery response')
    chosen, seen = [], set()
    for profile in profiles:
        if not isinstance(profile, dict) or profile.get('chainId') != 'solana': continue
        mint = profile.get('tokenAddress')
        try: validate_mint(mint)
        except QuoteError: continue
        if mint in seen or mint in (SOL, USDC): continue
        seen.add(mint); chosen.append(mint)
        if len(chosen) >= limit: break
    return chosen


def discovery_coverage(profiles, selected=None, evidence=None):
    """Return bounded, provenance-safe discovery denominator metadata.

    The research result intentionally keeps the provider's raw profiles out of
    the durable coverage record.  Counts are calculated before applying the
    caller's candidate limit, so a limited batch still has an honest provider
    denominator.
    """
    if isinstance(profiles, dict) and profiles.get('chainId'):
        rows = [profiles]
    elif isinstance(profiles, list):
        rows = profiles
    else:
        raise QuoteError('Invalid discovery response')
    valid, seen = 0, set()
    for profile in rows:
        if not isinstance(profile, dict) or profile.get('chainId') != 'solana':
            continue
        mint = profile.get('tokenAddress')
        try:
            validate_mint(mint)
        except QuoteError:
            continue
        if mint in (SOL, USDC) or mint in seen:
            continue
        seen.add(mint)
        valid += 1
    selected = list(selected if selected is not None else discover(rows, POLICY['max_candidates']))
    # Evidence is deliberately restricted to fields emitted by Sources._read;
    # this prevents a provider URL or credential accidentally entering a cycle
    # record when a test/provider supplies additional metadata.
    provenance = []
    if isinstance(evidence, dict):
        item = {}
        source = evidence.get('source')
        if isinstance(source, str) and re.fullmatch(r'[A-Za-z0-9:_-]{1,80}', source):
            item['source'] = source
        for key in ('requested_at', 'received_at'):
            value = evidence.get(key)
            if isinstance(value, str) and len(value) <= 64:
                item[key] = value
        response_hash = evidence.get('response_sha256')
        if isinstance(response_hash, str) and re.fullmatch(r'[0-9a-fA-F]{64}', response_hash):
            item['response_sha256'] = response_hash
        if item:
            provenance.append(item)
    return {
        'provider_profile_count': len(rows),
        'valid_unique_solana_mint_count': valid,
        'selected_count': len(selected),
        'provider_provenance': provenance,
    }


def select_pool(pairs, mint, now):
    if not isinstance(pairs, list): raise QuoteError('Invalid pool response')
    choices = []
    for pair in pairs:
        try:
            if pair['chainId'] != 'solana' or pair['baseToken']['address'] != mint: continue
            address = validate_mint(pair['pairAddress'])
            liquidity = decimal_value(pair['liquidity']['usd'])
            created = integer(pair['pairCreatedAt'], 1)
            age_hours = (Decimal(str(now.timestamp())) * 1000 - created) / Decimal(3_600_000)
            if age_hours < 0: continue
            txns = pair.get('txns', {}).get('h1', {})
            volume = decimal_value(pair.get('volume', {}).get('h1', 0))
            buys = integer(txns.get('buys', 0))
            sells = integer(txns.get('sells', 0))
            choices.append({'pair_address': address, 'liquidity_usd': str(liquidity),
                            'pair_created_at_ms': created, 'age_hours_at_observation': str(age_hours),
                            'volume_h1_usd': str(volume), 'buys_h1': buys, 'sells_h1': sells,
                            'fdv_usd': str(decimal_value(pair.get('fdv', 0)))})
        except (KeyError, TypeError, QuoteError): continue
    if not choices: raise QuoteError('No complete matching pool metadata')
    return max(choices, key=lambda p: (Decimal(p['liquidity_usd']), p['pair_address']))


def mint_checks(result):
    try:
        slot = integer(result['context']['slot'], 1)
        value = result['value']
        if not isinstance(value, dict) or value.get('executable') is not False: raise QuoteError('Mint account unavailable')
        owner = validate_mint(value['owner'])
        if owner not in (TOKEN_PROGRAM, TOKEN_2022_PROGRAM):
            return {'slot': slot, 'program': owner}, ['unsupported_token_program']
        parsed = value['data']['parsed']
        if parsed['type'] != 'mint': raise QuoteError('Account is not a parsed mint')
        info = parsed['info']
        if info['isInitialized'] is not True: raise QuoteError('Mint is not initialized')
        supply = raw_integer(info['supply'])
        if supply == 0: raise QuoteError('Mint supply is zero')
        decimals = integer(info['decimals'])
        if decimals > 255: raise QuoteError('Invalid token decimals')
        findings = []
        output = {'slot': slot, 'program': owner, 'supply_raw': str(supply), 'decimals': decimals}
        for field in ('mintAuthority', 'freezeAuthority'):
            authority = info[field]  # Missing is not equivalent to revoked.
            if authority is not None:
                validate_mint(authority); findings.append(field + '_present')
            output[field] = authority
        extensions = info.get('extensions', [])
        if owner == TOKEN_2022_PROGRAM:
            if not isinstance(extensions, list) or not extensions:
                findings.append('token_2022_extensions_missing')
            else:
                names = []
                for extension in extensions:
                    if not isinstance(extension, dict) or extension.get('extension') not in ('metadataPointer', 'tokenMetadata'):
                        findings.append('unreviewed_token_2022_extension'); continue
                    names.append(extension['extension'])
                    state = extension.get('state')
                    if not isinstance(state, dict): findings.append('invalid_token_2022_extension'); continue
                    if extension['extension'] == 'metadataPointer' and state.get('authority') is not None:
                        findings.append('metadata_pointer_authority_present')
                    if extension['extension'] == 'tokenMetadata' and (state.get('updateAuthority') is not None
                            or state.get('additionalMetadata') not in ([], None)):
                        findings.append('mutable_or_extended_token_metadata')
                if sorted(names) != ['metadataPointer', 'tokenMetadata']:
                    findings.append('token_2022_extension_set_not_allowlisted')
                output['reviewed_extensions'] = sorted(names)
        elif extensions:
            findings.append('unreviewed_extensions')
        return output, findings
    except (KeyError, TypeError): raise QuoteError('Incomplete mint risk data') from None


def account_concentration(result, mint):
    try:
        slot = integer(result['context']['slot'], 1)
        rows = result['value']
        if not isinstance(rows, list) or not 1 <= len(rows) <= 20: raise QuoteError('Missing largest-account data')
        accounts, seen = [], set()
        for row in rows:
            address = validate_mint(row['address'])
            amount = raw_integer(row['amount'])
            if address in seen or type(row['decimals']) is not int or row['decimals'] != mint['decimals']: raise QuoteError('Inconsistent largest-account data')
            seen.add(address); accounts.append({'address': address, 'amount_raw': str(amount)})
        total = sum(int(x['amount_raw']) for x in accounts)
        supply = int(mint['supply_raw'])
        if total > supply or abs(slot - mint['slot']) > POLICY['max_rpc_slot_skew']:
            raise QuoteError('Supply and account snapshots are inconsistent')
        return {'slot': slot, 'accounts': accounts, 'largest_account_supply_fraction': str(Decimal(max(int(x['amount_raw']) for x in accounts)) / supply),
                'listed_accounts_supply_fraction': str(Decimal(total) / supply),
                'beneficial_owners_identified': False, 'includes_possible_pool_vaults': True}
    except (KeyError, TypeError): raise QuoteError('Incomplete largest-account data') from None


def research(sources, quotes, limit=1, now=None):
    if type(limit) is not int or not 1 <= limit <= POLICY['max_candidates']: raise QuoteError('Candidate limit must be 1–3')
    now = now or utc_now()
    profiles, discovery_evidence = sources.profiles()
    selected = discover(profiles, limit)
    coverage = discovery_coverage(profiles, selected, discovery_evidence)
    candidates = []
    for mint in selected:
        row = {'chain': 'solana', 'mint': mint, 'observed_at': now.isoformat(), 'screen_rejections': [],
               'unresolved_checks': list(UNRESOLVED), 'eligible_for_trading': False, 'execution_enabled': False,
               'evidence': [discovery_evidence], 'quotes': None}
        candidates.append(row)
        try:
            pairs, evidence = sources.pairs(mint); row['evidence'].append(evidence)
            pool = select_pool(pairs, mint, now); row['pool'] = pool
            if Decimal(pool['liquidity_usd']) < Decimal(POLICY['min_pool_liquidity_usd']): row['screen_rejections'].append('pool_liquidity_below_research_floor')
            if Decimal(pool['age_hours_at_observation']) < POLICY['min_pool_age_hours']: row['screen_rejections'].append('pool_younger_than_research_floor')
            # Still collect mint/account evidence for the bounded candidate set;
            # never loosen the liquidity screen to manufacture passing samples.
            result, evidence = sources.rpc('getAccountInfo', mint); row['evidence'].append(evidence)
            row['mint_checks'], findings = mint_checks(result); row['screen_rejections'].extend(findings)
            if row['mint_checks'].get('supply_raw'):
                result, evidence = sources.rpc('getTokenLargestAccounts', mint); row['evidence'].append(evidence)
                row['account_concentration'] = account_concentration(result, row['mint_checks'])
            if not row['screen_rejections']:
                entry = quotes.quote(USDC, mint, POLICY['quote_input_usdc_base_units'])
                conservative_amount = entry['quote'].get('otherAmountThreshold', entry['quote']['outAmount'])
                preflight = quotes.quote(mint, USDC, conservative_amount)
                row['quotes'] = {'schema_version': 1, 'kind': 'quote_observations',
                                 'execution_enabled': False, 'observations': [entry, preflight],
                                 'not_realized_pnl': True,
                                 'preflight_uses_conservative_entry_minimum': True}
        except QuoteError as error:
            row['screen_rejections'].append(str(error))
        row['research_status'] = 'screen_rejected' if row['screen_rejections'] else 'observed_pending_risk_review'
    return {'schema_version': 1, 'policy': dict(POLICY), 'observed_at': now.isoformat(),
            'source_selection': 'latest profiles in provider order, deduplicated; not an exhaustive launch feed',
            'candidates': candidates, 'execution_enabled': False, 'memecoin_allocation_usd': 0,
            'discovery_coverage': coverage, 'coverage': dict(coverage)}


def main(argv=None):
    parser = argparse.ArgumentParser(description='One bounded Solana discovery/risk/quote research batch; no execution')
    parser.add_argument('--limit', type=int, choices=(1, 2, 3), default=1)
    parser.add_argument('--output', required=True)
    args = parser.parse_args(argv)
    try:
        if os.path.lexists(args.output): raise QuoteError('Output already exists')
        budget = BudgetOpener(1 + 5 * args.limit)
        record = research(Sources(budget), QuoteClient(opener=budget), args.limit)
        record['request_budget'] = {'maximum': budget.limit, 'attempted': budget.used, 'by_host': budget.by_host}
        save_new(args.output, record)
    except (QuoteError, OSError) as error:
        print(str(error) if isinstance(error, QuoteError) else 'Unable to save research evidence', file=sys.stderr)
        return 1
    print(json.dumps({'candidates': len(record['candidates']), 'requests': budget.used,
                      'statuses': [x['research_status'] for x in record['candidates']], 'execution_enabled': False}))
    return 0


if __name__ == '__main__': raise SystemExit(main())
