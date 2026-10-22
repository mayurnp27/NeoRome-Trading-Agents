"""Bounded read-only Robinhood collection and conservative options preview.

Uses Codex's existing OAuth connection. Never extracts credentials or calls an
order tool. A preview is not a fill, signal, shared-risk reservation or backtest.
"""
import argparse
import hashlib
import json
import os
import queue
import subprocess
import threading
import time
from datetime import datetime, timedelta, timezone, date
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from desks.options.verticals import OptionLeg, _decimal, _aware_datetime
from desks.options.paper_proposal import prepare_paper_vertical
from .jupiter_quotes import save_new

SERVER = 'robinhood-trading'
TOOLS = {'get_option_chains', 'get_option_instruments', 'get_option_quotes', 'search'}


class ReadOnlySession:
    """Ephemeral transport session; no model turn or persistent task created."""
    def __enter__(self):
        self.messages = queue.Queue()
        self.counter = 0
        self.process = subprocess.Popen(
            ['codex', 'app-server', '--stdio', '-c',
             'mcp_servers={robinhood-trading={url="https://agent.robinhood.com/mcp/trading"}}'],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, bufsize=1)
        def reader():
            for line in self.process.stdout:
                try: self.messages.put(json.loads(line))
                except ValueError: pass
            self.messages.put(None)
        threading.Thread(target=reader, daemon=True).start()
        try:
            self.request('initialize', {'clientInfo': {'name': 'neorome-options-data', 'version': '1'},
                                       'capabilities': {'experimentalApi': True}})
            self.thread = self.request('thread/start', {'ephemeral': True})['thread']['id']
            return self
        except Exception:
            self.__exit__(None, None, None)
            raise

    def request(self, method, params):
        self.counter += 1
        self.process.stdin.write(json.dumps({'id': self.counter, 'method': method, 'params': params}) + '\n')
        self.process.stdin.flush()
        deadline = time.monotonic() + 45
        while True:
            try: message = self.messages.get(timeout=max(.01, deadline-time.monotonic()))
            except queue.Empty: raise RuntimeError('Robinhood transport timed out') from None
            if message is None: raise RuntimeError('Robinhood transport closed')
            if message.get('id') == self.counter:
                if 'error' in message: raise RuntimeError('Robinhood request rejected')
                return message['result']
            if time.monotonic() >= deadline: raise RuntimeError('Robinhood transport timed out')

    def call(self, tool, arguments):
        if tool not in TOOLS: raise ValueError('Only options data tools permitted')
        result = self.request('mcpServer/tool/call', {'threadId': self.thread,
            'server': SERVER, 'tool': tool, 'arguments': arguments})
        if result.get('isError'): raise RuntimeError('Robinhood data tool failed')
        body = result.get('structuredContent')
        if not isinstance(body, dict):
            texts = [x['text'] for x in result.get('content', []) if x.get('type') == 'text']
            if len(texts) != 1: raise ValueError('Unexpected tool response')
            body = json.loads(texts[0])
        if not isinstance(body.get('data'), dict): raise ValueError('Missing data response')
        return body['data']

    def __exit__(self, *args):
        self.process.terminate()
        try: self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill(); self.process.wait(timeout=5)
        self.process.stdin.close(); self.process.stdout.close()


def collect(call, *, symbol, expiry, right, strikes):
    """Five data calls: underlying identity, chain, two contracts, quote batch."""
    if symbol not in ('SPY', 'QQQ') or right not in ('call', 'put'):
        raise ValueError('Only SPY/QQQ calls or puts supported')
    date.fromisoformat(expiry)
    if len(strikes) != 2 or len(set(strikes)) != 2: raise ValueError('Two distinct strikes required')
    for strike in strikes:
        if _decimal(strike, 'strike') <= 0: raise ValueError('Invalid strike')
    responses = []
    def fetch(tool, arguments):
        started = datetime.now(timezone.utc).isoformat()
        data = call(tool, arguments)
        received = datetime.now(timezone.utc).isoformat()
        encoded = json.dumps(data, sort_keys=True, separators=(',', ':')).encode()
        responses.append({'tool': tool, 'arguments': arguments, 'requested_at': started,
                          'received_at': received, 'data_sha256': hashlib.sha256(encoded).hexdigest(), 'data': data})
        return data
    fetch('search', {'query': symbol, 'asset_type': 'instrument', 'limit': 5})
    chains = fetch('get_option_chains', {'underlying_symbol': symbol})
    matches = [c for c in chains['chains'] if c['symbol'] == symbol and expiry in c['expiration_dates']]
    if len(matches) != 1: raise ValueError('Ambiguous or missing option chain')
    instruments = []
    for strike in strikes:
        data = fetch('get_option_instruments', {'chain_id': matches[0]['id'],
            'expiration_dates': expiry, 'strike_price': str(strike), 'type': right})
        if data.get('next') or len(data['instruments']) != 1:
            raise ValueError('Ambiguous or missing option instrument')
        instruments.extend(data['instruments'])
    fetch('get_option_quotes', {'instrument_ids': [i['id'] for i in instruments]})
    return {'source': SERVER, 'received_at': responses[-1]['received_at'],
            'responses': responses, 'orders_sent': 0}


def normalize(snapshot, *, as_of):
    """Exact joins, immutable provider timestamps, finite prices, both-side depth."""
    now = _aware_datetime(as_of, 'as_of')
    if snapshot.get('source') != SERVER: raise ValueError('Wrong data source')
    received = _aware_datetime(snapshot['received_at'], 'receipt')
    if not timedelta(0) <= now-received <= timedelta(seconds=60):
        raise ValueError('Snapshot receipt is stale or in the future')
    chains, instruments, quotes, securities = {}, {}, {}, {}
    for response in snapshot['responses']:
        data = response['data']
        encoded = json.dumps(data, sort_keys=True, separators=(',', ':')).encode()
        if hashlib.sha256(encoded).hexdigest() != response['data_sha256']:
            raise ValueError('Snapshot checksum mismatch')
        received_row = _aware_datetime(response['received_at'], 'response receipt')
        requested = _aware_datetime(response['requested_at'], 'request timestamp')
        if not requested <= received_row <= received: raise ValueError('Invalid collection chronology')
        tool = response['tool']
        if tool == 'search':
            rows = data['results']; target = securities; key = 'instrument_id'
        elif tool == 'get_option_chains':
            rows = data['chains']; target = chains; key = 'id'
        elif tool == 'get_option_instruments':
            rows = data['instruments']; target = instruments; key = 'id'
        elif tool == 'get_option_quotes':
            rows = [r['quote'] for r in data['results']]; target = quotes; key = 'instrument_id'
        else: raise ValueError('Unexpected data tool')
        for row in rows:
            if row[key] in target: raise ValueError('Duplicate identifier')
            target[row[key]] = row
    if len(instruments) != 2 or set(quotes) != set(instruments):
        raise ValueError('Quotes must exactly match both contracts')
    normalized = []
    for identifier, instrument in instruments.items():
        chain = chains[instrument['chain_id']]; quote = quotes[identifier]
        symbol = instrument['chain_symbol']
        underlying = chain['underlying_instruments']
        if len(underlying) != 1: raise ValueError('Unsupported deliverable basket')
        # Parse only the returned identity; never request the provider's internal URL.
        underlying_id = urlparse(underlying[0]['instrument']).path.rstrip('/').split('/')[-1]
        if securities.get(underlying_id, {}).get('symbol') != symbol:
            raise ValueError('Underlying security identity mismatch')
        if (symbol not in ('SPY', 'QQQ') or chain['symbol'] != symbol or
            instrument['underlying_type'] != 'equity' or instrument['state'] != 'active' or
            instrument['tradability'] != 'tradable' or chain['can_open_position'] is not True or
            chain['settle_on_open'] is not False or len(underlying) != 1 or underlying[0]['symbol'] not in ('', symbol) or
            _decimal(instrument['trade_value_multiplier'], 'multiplier') != 100 or
            _decimal(chain['trade_value_multiplier'], 'chain multiplier') != 100 or
            (chain['cash_component'] is not None and _decimal(chain['cash_component'], 'cash') != 0)):
            raise ValueError('Unsupported or adjusted contract metadata')
        expiry = date.fromisoformat(instrument['expiration_date'])
        if expiry.isoformat() not in chain['expiration_dates']: raise ValueError('Chain expiry mismatch')
        right = {'call': 'C', 'put': 'P'}[instrument['type']]
        strike = _decimal(instrument['strike_price'], 'strike'); encoded = strike * 1000
        if not 0 < encoded <= 99999999 or encoded != encoded.to_integral_value(): raise ValueError('Invalid OCC strike')
        timestamp = _aware_datetime(quote['updated_at'], 'provider quote timestamp')
        if not timedelta(0) <= now-timestamp <= timedelta(seconds=60):
            raise ValueError('Provider quote is stale or in the future')
        if timestamp > received: raise ValueError('Quote timestamp follows receipt')
        bid = _decimal(quote['bid_price'], 'bid'); ask = _decimal(quote['ask_price'], 'ask')
        if not 0 < bid <= ask: raise ValueError('Missing, zero or crossed market')
        for side in ('bid_size', 'ask_size'):
            if type(quote[side]) is not int or quote[side] < 1: raise ValueError('Missing executable depth')
        delta = _decimal(quote['delta'], 'delta'); iv = _decimal(quote['implied_volatility'], 'iv')
        if not -1 <= delta <= 1 or iv <= 0: raise ValueError('Invalid Greeks or IV')
        normalized.append({'symbol': f'{symbol}{expiry:%y%m%d}{right}{int(encoded):08d}',
            'underlying': symbol, 'expiry': expiry.isoformat(), 'right': right, 'strike': str(strike),
            'multiplier': 100, 'adjusted': False, 'bid': str(bid), 'ask': str(ask),
            'timestamp': quote['updated_at'], 'bid_size': quote['bid_size'], 'ask_size': quote['ask_size'],
            'delta': str(delta), 'implied_volatility': str(iv), 'instrument_id': identifier})
    return normalized


def preview(snapshot, *, as_of, long_id, fee_per_spread, slippage_per_spread):
    """Connect observed quotes to existing paper proposal validator; no fills."""
    result = {'status': 'blocked', 'orders_sent': 0, 'simulated_fills': 0, 'blockers': []}
    try:
        rows = normalize(snapshot, as_of=as_of)
        if long_id not in {r['instrument_id'] for r in rows}: raise ValueError('Long contract not in snapshot')
        now = _aware_datetime(as_of, 'as_of')
        legs = []
        for row in rows:
            dte = (date.fromisoformat(row['expiry'])-now.astimezone(ZoneInfo('America/New_York')).date()).days
            if not 30 <= dte <= 60: raise ValueError('Entry requires 30–60 calendar days to expiry')
            legs.append(OptionLeg(**{k: row[k] for k in ('symbol','underlying','expiry','right','strike','multiplier','adjusted','bid','ask','timestamp')},
                                  side='buy' if row['instrument_id'] == long_id else 'sell', quantity=1))
        risk, request = prepare_paper_vertical(legs, as_of=now, client_order_id='rh-readonly-preview',
            max_loss_budget='15', max_quantity=1, max_quote_age=timedelta(seconds=60),
            fee_per_contract=fee_per_spread, slippage_per_contract=slippage_per_spread)
        if risk.net_debit <= 0: raise ValueError('Only debit spreads supported')
        result.update(status='preview_only', normalized_quotes=rows,
                      risk_including_costs=str(risk.risk_including_costs), payload=request.payload())
        result['blockers'] = ['Signal validation, market-session verification, shared risk reservation and execution/lifecycle integration required before simulated entry']
    except (ValueError, KeyError, TypeError) as exc:
        result['blockers'].append(str(exc))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--symbol', choices=['SPY','QQQ'], required=True)
    parser.add_argument('--expiry', required=True)
    parser.add_argument('--right', choices=['call','put'], required=True)
    parser.add_argument('--strikes', nargs=2, required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    if os.path.exists(args.output): parser.error('Output already exists')
    with ReadOnlySession() as session:
        snapshot = collect(session.call, symbol=args.symbol, expiry=args.expiry, right=args.right, strikes=args.strikes)
    save_new(args.output, snapshot)
    try:
        rows = normalize(snapshot, as_of=datetime.now(timezone.utc))
        result = {'status': 'fresh_quotes', 'contracts': len(rows)}
    except (ValueError, KeyError, TypeError) as exc:
        result = {'status': 'blocked', 'reason': str(exc)}
    print(json.dumps(dict(result, output=args.output, orders_sent=0, simulated_fills=0)))

if __name__ == '__main__': main()
