"""Read-only, bounded Helius Parsed Events collection for wallet research."""
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import urllib.parse
import urllib.request
import urllib.error

from .crypto_research import helius_key
from .jupiter_quotes import validate_mint, SOL, USDC, NoRedirect


class WalletHistoryError(ValueError):
    pass


class WalletHistoryClient:
    """No signing methods, redirects, credential logging, or implicit pagination."""
    def __init__(self, max_calls=4, opener=None, credential_loader=helius_key):
        if type(max_calls) is not int or not 1 <= max_calls <= 20:
            raise WalletHistoryError('invalid request ceiling')
        self.max_calls, self.used = max_calls, 0
        self.opener = opener or urllib.request.build_opener(NoRedirect)
        self.credential_loader = credential_loader

    def history(self, address, cursor=None):
        validate_mint(address)
        if cursor is not None and (not isinstance(cursor, str) or len(cursor) > 2048):
            raise WalletHistoryError('invalid cursor')
        if self.used >= self.max_calls:
            raise WalletHistoryError('wallet research request ceiling reached')
        payload = {'address': address, 'limit': 20, 'sortOrder': 'desc',
                   'commitment': 'confirmed', 'includeRawTransaction': False}
        if cursor:
            payload['paginationToken'] = cursor
        key = self.credential_loader()
        url = 'https://mainnet.helius-rpc.com/v1/parsed-events/transaction-history?api-key=' + urllib.parse.quote(key, safe='')
        request = urllib.request.Request(url, data=json.dumps(payload).encode(),
            headers={'Content-Type': 'application/json', 'Accept': 'application/json'}, method='POST')
        started = datetime.now(timezone.utc).isoformat()
        self.used += 1
        try:
            with self.opener.open(request, timeout=20) as response:
                if response.status != 200:
                    raise WalletHistoryError('wallet provider rejected request')
                body = response.read(4_000_001)
        except urllib.error.HTTPError as error:
            status = error.code
            error.close()
            raise WalletHistoryError('wallet provider HTTP ' + str(status)) from None
        except Exception:
            raise WalletHistoryError('wallet provider unavailable') from None
        if len(body) > 4_000_000:
            raise WalletHistoryError('wallet response too large')
        try:
            data = json.loads(body)
        except (ValueError, UnicodeError):
            raise WalletHistoryError('wallet response invalid') from None
        if not isinstance(data, dict) or not isinstance(data.get('data'), list) or len(data['data']) > 20:
            raise WalletHistoryError('wallet page schema invalid')
        return {'schema_version': 'wallet-history-v1', 'address': address,
                'requested_at': started, 'received_at': datetime.now(timezone.utc).isoformat(),
                'source_sha256': hashlib.sha256(body).hexdigest(), 'data': data['data'],
                'pagination_token': data.get('paginationToken'), 'credits_reserved': 10,
                'provider': 'helius-parsed-events', 'commitment': 'confirmed'}


def _raw_amount(value):
    """Parsed Events amounts are integer base units; bools are never amounts."""
    if type(value) is int:
        amount = value
    elif isinstance(value, str) and value.isascii() and value.isdecimal():
        amount = int(value)
    else:
        raise WalletHistoryError('invalid_swap_amounts')
    if amount < 0 or amount > 2**64 - 1:
        raise WalletHistoryError('invalid_swap_amounts')
    return amount


def _format_decimal(value):
    rendered = format(value, 'f')
    if '.' in rendered:
        rendered = rendered.rstrip('0').rstrip('.')
    return rendered or '0'


def _deltas(transfers, mint, wallet):
    if not isinstance(transfers, list) or any(not isinstance(t, dict) for t in transfers):
        raise WalletHistoryError('malformed_transaction')
    selected = [t for t in transfers if t.get('mint') == mint]
    if not selected:
        raise WalletHistoryError('wallet_quote_transfer_missing')
    decimals = set()
    net = 0
    for transfer in selected:
        decimals.add(transfer.get('decimals'))
        amount = _raw_amount(transfer.get('rawTokenAmount'))
        if transfer.get('toUserAccount') == wallet:
            net += amount
        if transfer.get('fromUserAccount') == wallet:
            net -= amount
    if len(decimals) != 1:
        raise WalletHistoryError('token_decimals_unverified')
    decimal_places = next(iter(decimals))
    if type(decimal_places) is not int or not 0 <= decimal_places <= 18:
        raise WalletHistoryError('token_decimals_unverified')
    return net, decimal_places


def _native_costs(parsed, wallet):
    payer = parsed.get('feePayer')
    fee = parsed.get('fee')
    if type(fee) is not int or not 0 <= fee <= 2**64 - 1:
        raise WalletHistoryError('invalid_network_fee')
    network = fee if payer == wallet else 0
    sponsor = fee if payer != wallet else 0
    outgoing = 0
    native_transfers = parsed.get('nativeTransfers', [])
    if not isinstance(native_transfers, list):
        raise WalletHistoryError('malformed_transaction')
    for transfer in native_transfers:
        if not isinstance(transfer, dict):
            raise WalletHistoryError('malformed_transaction')
        amount = _raw_amount(transfer.get('amount'))
        if transfer.get('fromUserAccount') == wallet:
            outgoing += amount
    return network, sponsor, outgoing


def _transaction_sol_usdc_rate(parsed_data):
    """Return a SOL->USDC execution rate from direct inner route hops only."""
    hops = parsed_data.get('inner_swaps', [])
    if not isinstance(hops, list):
        raise WalletHistoryError('native_cost_conversion_unverified')
    directions = set()
    total_sol = total_usdc = 0
    for hop in hops:
        if not isinstance(hop, dict):
            raise WalletHistoryError('native_cost_conversion_unverified')
        input_mint, output_mint = hop.get('input_mint'), hop.get('output_mint')
        if (input_mint, output_mint) == (SOL, USDC):
            direction = 'sol_to_usdc'
            sol_raw = _raw_amount(hop.get('input_amount'))
            usdc_raw = _raw_amount(hop.get('output_amount'))
        elif (input_mint, output_mint) == (USDC, SOL):
            direction = 'usdc_to_sol'
            usdc_raw = _raw_amount(hop.get('input_amount'))
            sol_raw = _raw_amount(hop.get('output_amount'))
        else:
            continue
        if sol_raw <= 0 or usdc_raw <= 0:
            raise WalletHistoryError('native_cost_conversion_unverified')
        directions.add(direction)
        total_sol += sol_raw
        total_usdc += usdc_raw
    if len(directions) != 1 or total_sol <= 0 or total_usdc <= 0:
        return None
    # Both directions reduce to raw USDC / raw SOL * 10^(9-6).
    return Decimal(total_usdc) / Decimal(total_sol) * Decimal(1000)


def _normalize_item(item, page, wallet):
    p = item['parsed']
    if not isinstance(p, dict):
        raise WalletHistoryError('failed_or_unparsed_transaction')
    if item.get('parserStatus') != 'OK' or p.get('transactionStatus') != 'OK':
        raise WalletHistoryError('failed_or_unparsed_transaction')
    payer = p.get('feePayer')
    if not isinstance(payer, str):
        raise WalletHistoryError('fee_payer_unverified')
    validate_mint(payer)
    summary = p['summary']
    if not isinstance(summary, dict) or not isinstance(summary.get('parsedData'), dict):
        raise WalletHistoryError('malformed_transaction')
    s = summary['parsedData']
    if summary.get('type') != 'swap' or s.get('type') != 'swap':
        raise WalletHistoryError('non_swap_or_transfer')
    input_mint, output_mint = s['input_mint'], s['output_mint']
    if input_mint == output_mint or input_mint not in (SOL, USDC) and output_mint not in (SOL, USDC):
        raise WalletHistoryError('unsupported_quote_or_multitoken_swap')
    # A supported quote must be one endpoint, and the other endpoint is the traded mint.
    quote_mint = input_mint if input_mint in (SOL, USDC) else output_mint
    mint = output_mint if input_mint == quote_mint else input_mint
    side = 'buy' if input_mint == quote_mint else 'sell'
    validate_mint(mint)
    if mint in (SOL, USDC):
        raise WalletHistoryError('stablecoin_or_native_not_memecoin_candidate')

    gross_quote = _raw_amount(s['in_amount'] if side == 'buy' else s['actual_out_amount'])
    gross_token = _raw_amount(s['actual_out_amount'] if side == 'buy' else s['in_amount'])
    if gross_quote <= 0 or gross_token <= 0:
        raise WalletHistoryError('invalid_swap_amounts')
    transfers = p['tokenTransfers']
    if not isinstance(transfers, list):
        raise WalletHistoryError('malformed_transaction')

    token_net, token_decimals = _deltas(transfers, mint, wallet)
    expected_token_sign = 1 if side == 'buy' else -1
    if token_net * expected_token_sign <= 0:
        raise WalletHistoryError('wallet_transfer_swap_mismatch')
    quote_net, quote_decimals = _deltas(transfers, quote_mint, wallet)
    canonical_quote_decimals = 9 if quote_mint == SOL else 6
    if quote_decimals != canonical_quote_decimals:
        raise WalletHistoryError('quote_decimals_unverified')
    if ((side == 'buy' and quote_net >= 0) or (side == 'sell' and quote_net <= 0)
            or (side == 'buy' and -quote_net < gross_quote)
            or (side == 'sell' and quote_net > gross_quote)):
        raise WalletHistoryError('wallet_quote_transfer_swap_mismatch')
    quote_fee_raw = (-quote_net - gross_quote if side == 'buy' else gross_quote - quote_net)
    if quote_fee_raw < 0:
        raise WalletHistoryError('wallet_quote_transfer_swap_mismatch')

    trader_network_fee, sponsor_network_fee, native_net_out = _native_costs(p, wallet)
    # For a SOL-quoted swap, the quote transfer is WSOL. Native activity is a
    # separate cost only when it exceeds the quote leg's direct native flow.
    if quote_mint == SOL:
        native_trade_raw = gross_quote if side == 'buy' else 0
        # If the WSOL quote was wrapped into native SOL as well, remove the
        # reconciled principal and quote-token platform fee once. Incoming
        # transfers never offset trader outflows (including rent refunds).
        native_extra = max(0, native_net_out - native_trade_raw - quote_fee_raw)
    else:
        native_extra = native_net_out
    native_cost_raw = trader_network_fee + native_extra
    quote_scale = Decimal(10) ** quote_decimals
    fee_quote = Decimal(quote_fee_raw) / quote_scale
    # SOL quote accounting includes SOL-denominated network/direct costs only.
    if quote_mint == SOL:
        fee_quote += Decimal(native_cost_raw) / Decimal(10**9)
    conversion = None
    costs_complete = native_cost_raw == 0 or quote_mint == SOL
    if quote_mint == USDC and native_cost_raw > 0:
        rate = _transaction_sol_usdc_rate(s)
        if rate is not None:
            native_cost_usdc = Decimal(native_cost_raw) / Decimal(10**9) * rate
            fee_quote += native_cost_usdc
            costs_complete = True
            conversion = {
                'method': 'same_transaction_direct_inner_swap_sol_usdc',
                'signature': item['signature'],
                'block_time': datetime.fromtimestamp(p['blockTime'], timezone.utc).isoformat(),
                'source_sha256': page['source_sha256'],
                'native_cost_lamports': native_cost_raw,
                'usdc_per_sol': _format_decimal(rate),
                'usdc_cost': _format_decimal(native_cost_usdc),
            }
    block_time = p['blockTime']
    if type(block_time) not in (int, float) or isinstance(block_time, bool):
        raise WalletHistoryError('malformed_transaction')
    if type(item.get('signature')) is not str or not item['signature']:
        raise WalletHistoryError('malformed_transaction')
    row = {
        'wallet': wallet, 'mint': mint, 'signature': item['signature'],
        'attributed_actor': wallet, 'fee_payer': payer,
        'block_time': datetime.fromtimestamp(block_time, timezone.utc).isoformat(),
        'observed_at': page['received_at'], 'side': side,
        # Trader-owned transfer delta is the observed inventory change. The
        # parser's swap amount can omit a separately transferred platform fee.
        'quantity': format(Decimal(abs(token_net)) / Decimal(10) ** token_decimals, 'f'),
        'parsed_token_quantity_raw': gross_token,
        'token_transfer_delta_raw': token_net,
        'quote_mint': quote_mint,
        'quote_quantity': format(Decimal(gross_quote) / quote_scale, 'f'),
        'fee_quote': _format_decimal(fee_quote),
        'source_sha256': page['source_sha256'],
        'normalization_version': 'wallet-swap-v2',
        'cost_basis': 'actor_balance_transfer_reconciliation_and_quote_leg_fees',
        'trader_network_fee_lamports': trader_network_fee,
        'sponsor_network_fee_lamports': sponsor_network_fee,
        'trader_native_direct_outflow_lamports': native_extra,
        'native_cost_lamports': native_cost_raw,
        'costs_complete': costs_complete,
        'native_cost_conversion': conversion,
    }
    return row


def normalize_page(page, wallet):
    """Normalize swaps only when both trader-owned token legs reconcile.

    ``feePayer`` identifies the network-fee payer, not necessarily the trader.
    Sponsored attribution therefore requires observed token-transfer deltas for
    the candidate actor on both the traded token and quote-token legs.
    """
    validate_mint(wallet)
    rows, exclusions = [], []
    for item in page['data']:
        try:
            rows.append(_normalize_item(item, page, wallet))
        except (KeyError, TypeError, ValueError, OverflowError, AttributeError) as exc:
            reason = str(exc) if isinstance(exc, WalletHistoryError) else 'malformed_transaction'
            exclusions.append({'signature': item.get('signature') if isinstance(item, dict) else None,
                               'reason': reason})
    return {'rows': rows, 'exclusions': exclusions,
            'complete_history': page.get('pagination_token') is None,
            'partial_history_warning': 'unknown inventory and excluded activity require separate review'}


def candidate_wallets(page, limit=10):
    """Find bounded candidate actors whose token and quote legs both reconcile."""
    if type(limit) is not int or not 1 <= limit <= 100:
        raise WalletHistoryError('invalid candidate ceiling')
    found = []
    for item in page.get('data', []):
        if not isinstance(item, dict):
            continue
        p = item.get('parsed') or {}
        if (not isinstance(p, dict) or item.get('parserStatus') != 'OK' or p.get('transactionStatus') != 'OK'
                or not isinstance(p.get('summary'), dict) or p['summary'].get('type') != 'swap'):
            continue
        candidates = set()
        transfers = p.get('tokenTransfers', [])
        if not isinstance(transfers, list):
            continue
        for transfer in transfers:
            if not isinstance(transfer, dict):
                continue
            for field in ('fromUserAccount', 'toUserAccount'):
                address = transfer.get(field)
                if isinstance(address, str):
                    candidates.add(address)
        reconciled = []
        for address in sorted(candidates):
            try:
                validate_mint(address)
                _normalize_item(item, page, address)
                reconciled.append(address)
            except (ValueError, KeyError, TypeError, OverflowError):
                continue
        # An ambiguous transfer graph cannot nominate a trader.
        if len(reconciled) == 1 and reconciled[0] not in found:
            found.append(reconciled[0])
        if len(found) >= limit:
            break
    return found
