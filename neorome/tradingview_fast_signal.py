"""Private actual-MCP client and explicit closed 5m Stoch(5,3,3) turn selection."""
from __future__ import annotations
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import socket
import stat
import time

UTC = timezone.utc
BROKER_SOCKET = Path('/run/neorome-tradingview-fast-mcp/feed.sock')
BROKER_UID = 997
STATE_NAME = 'tradingview-fast-closed-bars.json'
SYMBOL = 'COINBASE:BTCUSD'
MAX_BYTES = 2_000_000
ENDPOINT = 'wss://data.tradingview.com/socket.io/websocket'

class TradingViewMCPError(ValueError):
    pass

def stamp(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise TradingViewMCPError('timestamp lacks timezone')
    return result.astimezone(UTC)

def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()

def capture():
    info = BROKER_SOCKET.lstat()
    if not stat.S_ISSOCK(info.st_mode) or info.st_uid != BROKER_UID or stat.S_IMODE(info.st_mode) & 0o077:
        raise TradingViewMCPError('private MCP socket identity/permissions differ')
    deadline = time.monotonic() + 25
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(25)
        connection.connect(str(BROKER_SOCKET))
        connection.sendall(json.dumps({'method':'fast_stochastic', 'symbol':'BTCUSD', 'exchange':'COINBASE', 'timeframe':'5m'}).encode()+b'\n')
        body = b''
        while b'\n' not in body:
            remaining = deadline-time.monotonic()
            if remaining <= 0:
                raise TradingViewMCPError('MCP deadline exceeded')
            connection.settimeout(remaining)
            part = connection.recv(65536)
            if not part:
                raise TradingViewMCPError('MCP broker closed before response')
            body += part
            if len(body) > MAX_BYTES:
                raise TradingViewMCPError('MCP response too large')
    value=json.loads(body.split(b'\n',1)[0])
    if value.get('ok') is not True or value.get('tool') != 'fast_stochastic':
        raise TradingViewMCPError('MCP tool refused: '+str(value.get('error','unknown'))[:120])
    if value.get('mcp_server',{}).get('name') != 'TradingView BTCUSD Fast Paper Feed':
        raise TradingViewMCPError('MCP server identity differs')
    return {key:value[key] for key in ('data','mcp_server','mcp_protocol_version','mcp_transcript_sha256','tool')}

def _number(value):
    if type(value) not in (int,float) or not math.isfinite(value) or not 0 <= value <= 100:
        raise TradingViewMCPError('oscillator must be finite and within [0,100]')
    return value

def evaluate(record, root, now):
    data=record['data']
    meta=data.get('source_metadata',{})
    if (data.get('symbol') != SYMBOL or data.get('timeframe') != '5m'
            or data.get('stochastic_settings') != [5,3,3]
            or meta.get('endpoint') != ENDPOINT or meta.get('explicit_source_bar_timestamp') is not True):
        raise TradingViewMCPError('exact source profile differs')
    source_hash=meta.get('source_response_sha256')
    if not isinstance(source_hash,str) or len(source_hash)!=64 or any(c not in '0123456789abcdef' for c in source_hash):
        raise TradingViewMCPError('source digest invalid')
    received=stamp(meta['received_at']); started=stamp(meta['request_started_at']); rest=stamp(meta['http_date'])
    if (not 0 <= (now-received).total_seconds() <= 15 or not 0 <= (received-started).total_seconds() <= 15
            or not -2 <= (received-rest).total_seconds() <= 15
            or type(meta.get('request_count')) is not int or meta['request_count']!=1
            or meta.get('cache_disabled') is not True or meta.get('stale_disabled') is not True
            or type(meta.get('cache_age')) not in (int,float) or not 0 <= meta['cache_age'] <=15):
        raise TradingViewMCPError('source freshness invalid')
    readings=data.get('readings')
    if not isinstance(readings,list) or len(readings)<2 or len(readings)>200:
        raise TradingViewMCPError('adjacent completed readings missing')
    for i,item in enumerate(readings):
        if type(item.get('bar_close_timestamp')) is not int or item['bar_close_timestamp']%300:
            raise TradingViewMCPError('bar close is not aligned')
        _number(item.get('k')); _number(item.get('d'))
        if i and item['bar_close_timestamp']-readings[i-1]['bar_close_timestamp']!=300:
            raise TradingViewMCPError('closed reading gap or disorder')
    prior,current=readings[-2:]
    closed=current['bar_close_timestamp']
    if closed != int(received.timestamp())//300*300 or closed > started.timestamp():
        raise TradingViewMCPError('latest source bar is stale, forming, or request crossed boundary')
    context={'stochastic_settings':[5,3,3], 'closed_k':current['k'],'closed_d':current['d'],
             'previous_k':prior['k'],'previous_d':prior['d'],'closed_at':closed,
             'explicit_source_bar_timestamp':True}
    path=Path(root)/STATE_NAME
    temporary=path.with_suffix('.tmp')
    if path.is_symlink() or temporary.is_symlink():
        raise TradingViewMCPError('signal state symlink refused')
    state={'schema':'tradingview-fast-closed-bars-v1','symbol':SYMBOL,'stochastic_settings':[5,3,3],
           'readings':readings[-2:], 'observed_at':received.isoformat(),'source_hash':record['source_hash']}
    temporary.write_text(json.dumps(state,allow_nan=False)+'\n'); temporary.chmod(0o600); temporary.replace(path)
    direction=('bullish' if prior['k']<=20 and current['k']>prior['k'] and prior['k']<=prior['d'] and current['k']>current['d'] else
               'bearish' if prior['k']>=80 and current['k']<prior['k'] and prior['k']>=prior['d'] and current['k']<current['d'] else None)
    selected=None
    if direction and 0<=now.timestamp()-closed<=90:
        selected={'signal_id':f'btc-tv-stoch-5m-533-{direction}-cross-{closed}',
                  'instrument':'BTC-USD','timeframe_minutes':5,'stochastic_settings':[5,3,3],'method':'fresh_cross',
                  'direction':direction,'contract_side':'yes' if direction=='bullish' else 'no',
                  'bar_close_timestamp':closed,'previous_k':prior['k'],'previous_d':prior['d'],
                  'k':current['k'],'d':current['d'],'oversold':20,'overbought':80,'valid':True,
                  'observed_receipt_at':record['received_at'],'source_hash':record['source_hash'],
                  'signal_provider':'tradingview_mcp','explicit_source_bar_timestamp':True}
    return {'selected_signal':selected,'chart_context':context,'signal_provider':'tradingview_mcp',
            'chart_context_hash':record['source_hash'],
            'reason':'closed_5m_extreme_fresh_cross' if selected else 'no_current_closed_5m_extreme_fresh_cross'}
