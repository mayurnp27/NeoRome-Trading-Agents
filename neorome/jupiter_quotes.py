"""Bounded Jupiter quote collection. No wallet, signing or execution interface."""
from __future__ import annotations

import argparse
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request

ENDPOINT = 'https://api.jup.ag/swap/v2/order'
KEYCHAIN_SERVICE = 'neorome.jupiter.api-key'
SOL = 'So11111111111111111111111111111111111111112'
USDC = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
_BASE58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
MAX_BODY = 1_000_000


class QuoteError(Exception):
    """Safe diagnostic; never includes credentials or provider response bodies."""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise QuoteError('Jupiter redirect refused')


def validate_mint(value):
    if not isinstance(value, str) or not 32 <= len(value) <= 44 or any(c not in _BASE58 for c in value):
        raise QuoteError('Mint must be a Solana public address')
    number = 0
    for character in value:
        number = number * 58 + _BASE58.index(character)
    size = (number.bit_length() + 7) // 8 + len(value) - len(value.lstrip('1'))
    if size != 32:
        raise QuoteError('Mint must decode to 32 bytes')
    return value


def raw_amount(value):
    if type(value) is int:
        amount = value
    elif isinstance(value, str) and re.fullmatch(r'[1-9][0-9]{0,19}', value):
        amount = int(value)
    else:
        raise QuoteError('Amount must be a positive integer in token base units')
    if not 0 < amount <= 2**64 - 1:
        raise QuoteError('Amount is outside the unsigned 64-bit range')
    return str(amount)


def load_key():
    environment = os.environ.get('NEOROME_JUPITER_API_KEY')
    if environment:
        if not re.fullmatch(r'[\x21-\x7e]+', environment):
            raise QuoteError('Jupiter credential format is invalid')
        return environment
    try:
        result = subprocess.run(['/usr/bin/security', 'find-generic-password', '-s', KEYCHAIN_SERVICE, '-w'],
                                capture_output=True, timeout=30, check=False)
    except (OSError, subprocess.SubprocessError):
        raise QuoteError('Jupiter Keychain lookup failed') from None
    if result.returncode:
        raise QuoteError('Jupiter credential is unavailable')
    try:
        key = result.stdout.decode('utf-8').rstrip('\r\n')
    except UnicodeError:
        raise QuoteError('Jupiter credential encoding is invalid') from None
    if not key or not re.fullmatch(r'[\x21-\x7e]+', key):
        raise QuoteError('Jupiter credential format is invalid')
    return key


class QuoteClient:
    def __init__(self, credential_loader=load_key, opener=None):
        self._credential_loader = credential_loader
        self._opener = opener or urllib.request.build_opener(NoRedirect)

    def quote(self, input_mint, output_mint, amount, slippage_bps=50):
        input_mint, output_mint = validate_mint(input_mint), validate_mint(output_mint)
        if input_mint == output_mint:
            raise QuoteError('Quote requires two distinct mints')
        amount = raw_amount(amount)
        if type(slippage_bps) is not int or not 0 <= slippage_bps <= 10000:
            raise QuoteError('Slippage must be an integer from 0 to 10000 basis points')
        params = dict(inputMint=input_mint, outputMint=output_mint, amount=amount,
                      swapMode='ExactIn', slippageBps=str(slippage_bps))
        # Fixed official endpoint; no taker, receiver, referral or caller-supplied URL.
        key = self._credential_loader()
        request = urllib.request.Request(ENDPOINT + '?' + urllib.parse.urlencode(params),
                  headers={'x-api-key': key, 'Accept': 'application/json', 'User-Agent': 'NeoRome-Quote-Research/1.0'}, method='GET')
        started = datetime.now(timezone.utc)
        try:
            with self._opener.open(request, timeout=20) as response:
                if response.status != 200:
                    raise QuoteError('Jupiter did not return a successful quote')
                body = response.read(MAX_BODY + 1)
        except urllib.error.HTTPError as error:
            status = error.code
            try:
                error.close()
            except Exception:
                pass
            raise QuoteError(f'Jupiter quote rejected (HTTP {status}); no retry attempted') from None
        except QuoteError:
            raise
        except Exception:
            raise QuoteError('Jupiter quote transport failed; no retry attempted') from None
        if len(body) > MAX_BODY:
            raise QuoteError('Jupiter response exceeded the size limit')
        try:
            data = json.loads(body)
        except (ValueError, UnicodeError):
            raise QuoteError('Jupiter returned malformed JSON') from None
        if not isinstance(data, dict) or any(data.get(k) not in (None, '', 0) for k in ('error', 'errorCode', 'errorMessage')):
            raise QuoteError('Jupiter returned an unusable quote')
        if data.get('transaction') not in (None, '') or data.get('taker') not in (None, ''):
            raise QuoteError('Unexpected transaction/wallet material in quote-only response')
        if (data.get('inputMint'), data.get('outputMint'), data.get('inAmount')) != (input_mint, output_mint, amount):
            raise QuoteError('Jupiter quote does not match the requested pair and amount')
        output = raw_amount(data.get('outAmount'))
        # Store a strictly bounded public-data subset, not arbitrary provider fields.
        quote = {'inputMint': input_mint, 'outputMint': output_mint, 'inAmount': amount, 'outAmount': output}
        for field in ('router', 'mode', 'requestId', 'expireAt', 'priceImpactPct', 'otherAmountThreshold', 'feeMint'):
            value = data.get(field)
            if value is not None:
                if not isinstance(value, str) or len(value) > 160 or key in value:
                    raise QuoteError('Invalid public quote metadata')
                quote[field] = value
        for field in ('slippageBps', 'feeBps', 'signatureFeeLamports', 'prioritizationFeeLamports', 'rentFeeLamports'):
            value = data.get(field)
            if value is not None:
                if type(value) is not int or value < 0:
                    raise QuoteError('Invalid quote fee metadata')
                quote[field] = value
        # Current API expresses priceImpact in percentage points; retain the
        # exact decimal and its units instead of treating it as the old ratio.
        if data.get('priceImpact') is not None:
            value = data['priceImpact']
            try:
                if type(value) not in (str, int, float):
                    raise ValueError
                impact = Decimal(str(value))
                if not impact.is_finite() or abs(impact) > 100:
                    raise ValueError
            except (InvalidOperation, ValueError):
                raise QuoteError('Invalid quote impact metadata') from None
            quote['priceImpact'] = str(impact)
            quote['priceImpactUnit'] = 'percentage_points'
        return {'schema_version': 1, 'provider': 'jupiter', 'endpoint': ENDPOINT,
                'requested_at': started.isoformat(), 'received_at': datetime.now(timezone.utc).isoformat(),
                'request': params, 'quote': quote, 'response_sha256': hashlib.sha256(body).hexdigest(),
                'execution_enabled': False, 'eligible_for_trading': False,
                'limitations': ['Indicative route observation, not a fill or sellability guarantee',
                                'No on-chain token-risk checks, wallet simulation or portfolio risk reservation',
                                'Fees may be incomplete without a taker; quotes may change immediately']}


def collect(client, input_mint, output_mint, amount, round_trip=False):
    entry = client.quote(input_mint, output_mint, amount)
    observations = [entry]
    if round_trip:
        observations.append(client.quote(output_mint, input_mint, entry['quote']['outAmount']))
    return {'schema_version': 1, 'kind': 'quote_observations', 'execution_enabled': False,
            'observations': observations, 'not_realized_pnl': True}


def save_new(path, record):
    """Never overwrite an earlier observation or follow an existing symlink."""
    payload = json.dumps(record, indent=2, allow_nan=False) + '\n'
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix='.quote-', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        # Publish a fully written record atomically without replacing any file.
        os.link(temporary, path)
    finally:
        os.unlink(temporary)


def main(argv=None):
    parser = argparse.ArgumentParser(description='Collect one or two read-only Jupiter quotes; never submits a trade')
    parser.add_argument('--input-mint', default=SOL)
    parser.add_argument('--output-mint', default=USDC)
    parser.add_argument('--amount', default='10000000', help='Integer input base units (default 0.01 SOL)')
    parser.add_argument('--round-trip', action='store_true', help='Also observe the reverse route; not a trade or P&L')
    parser.add_argument('--output', required=True, help='New JSON evidence file; existing files are refused')
    args = parser.parse_args(argv)
    try:
        if os.path.lexists(args.output):
            raise QuoteError('Output already exists; choose a new observation file')
        record = collect(QuoteClient(), args.input_mint, args.output_mint, args.amount, args.round_trip)
        save_new(args.output, record)
    except (QuoteError, OSError) as error:
        print(str(error) if isinstance(error, QuoteError) else 'Could not save observation evidence', file=sys.stderr)
        return 1
    print(f'Saved {len(record["observations"])} quote observations; execution disabled.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
