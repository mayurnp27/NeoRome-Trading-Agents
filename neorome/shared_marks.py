"""Complete observed marks for the admitted BTC/SPY paper strategies only.

No network or execution. Missing foreign data blocks new exposure; callers may
still independently validate and close their own position.
"""
from __future__ import annotations
from datetime import datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sqlite3
from .crypto_pilot import (_time, _decimal, _exit_value, _position_units, _ledger_records)
from .crypto_daily import _quote_from_observation
from .paper_ledger import MarketMark


class MarkUnavailable(ValueError):
    pass


class MarkIntegrityError(ValueError):
    pass


def equity_quote(state, now):
    root = Path(state)
    db = root / 'recorder.sqlite3'
    if root.is_symlink() or db.is_symlink() or not db.is_file():
        raise MarkUnavailable('SPY recorder unavailable')
    with sqlite3.connect(db.resolve().as_uri()+'?mode=ro', uri=True) as conn:
        row = conn.execute("SELECT observation,sha256 FROM attempts WHERE status='fresh' ORDER BY id DESC LIMIT 1").fetchone()
    if not row or not isinstance(row[0], str):
        raise MarkUnavailable('SPY observation missing')
    digest = hashlib.sha256(row[0].encode()).hexdigest()
    if row[1] != digest:
        raise MarkIntegrityError('SPY observation checksum mismatch')
    body = json.loads(row[0])
    evidence = body.get('evidence', {})
    if evidence.get('provider') != 'alpaca' or evidence.get('feed') != 'iex':
        raise MarkIntegrityError('SPY feed provenance unsupported')
    raw = body.get('raw_response', {}).get('quotes', {}).get('SPY', {})
    timestamp = _time(raw.get('t'), 'SPY quote time')
    if not 0 <= (now-timestamp).total_seconds() <= 60:
        raise MarkUnavailable('SPY quote stale')
    bid,ask,bs,asz = [_decimal(raw.get(k), k, positive=True) for k in ('bp','ap','bs','as')]
    if bid > ask:
        raise MarkUnavailable('SPY quote crossed')
    return {'symbol':'SPY','timestamp':timestamp.isoformat(),'bid':str(bid),
            'ask':str(ask),'bid_size':str(bs),'ask_size':str(asz)}, {'source_kind':'observed','sha256':digest}


def mark_provider(*, crypto_state, equity_state, equity_marker=None, token_marker=None):
    """Return callback(book, now); equity_marker validates corporate actions too."""
    def provide(book, now):
        _, crypto, foreign = _ledger_records(book)
        marks=[]
        if crypto:
            quote,status,source = _quote_from_observation(str(crypto_state), now)
            if quote is None:
                raise MarkUnavailable('BTC quote '+str(status))
            for position, proposal in crypto:
                if _time(quote['timestamp'], 'BTC quote time') < _time(position.opened_at, 'position open'):
                    raise MarkUnavailable('BTC quote precedes position')
                units = _position_units(proposal)
                if Decimal(quote['depth_units']) < units:
                    raise MarkUnavailable('BTC liquidation depth insufficient')
                net = _exit_value(units, Decimal(quote['bid']))[2]
                marks.append(MarketMark(position.position_id,net,quote['timestamp'],source['sha256'],'observed'))
        if foreign:
            for position in foreign:
                if position.desk == 'crypto' and position.instrument.startswith('solana:'):
                    if token_marker is None:
                        raise MarkUnavailable('Memecoin position has no admitted valuation adapter')
                    try:
                        marks.append(token_marker(book, position, now))
                    except Exception as exc:
                        raise MarkUnavailable('Memecoin mark unavailable: '+str(exc)) from None
                else:
                    if equity_marker is None:
                        raise MarkUnavailable('Foreign positions have no admitted valuation adapter')
                    if (position.desk != 'equities' or position.instrument != 'SPY'
                            or position.provenance != 'observed' or position.quantity != 1
                            or position.exposure_group != 'us-broad-equity'):
                        raise MarkUnavailable('Unknown position type')
                    from desks.equities.paper_policy import STRATEGY_VERSION
                    proposals = [e.payload.get('payload', {}) for e in book.audit() if e.event_type == 'reserve']
                    payload = next((p for p in proposals if p.get('proposal_id') == position.proposal_id), {})
                    marker = payload.get('metadata', {}).get('equity_shared')
                    if (payload.get('strategy') != 'EQ-ETF-TREND-COMPARATOR'
                            or payload.get('strategy_version') != STRATEGY_VERSION
                            or not isinstance(marker, dict) or marker.get('symbol') != 'SPY'
                            or payload.get('idea_id') != position.idea_id
                            or payload.get('provenance') != 'observed'):
                        raise MarkIntegrityError('Unregistered equity position')
                    _decimal(marker.get('units'), 'equity units', positive=True)
                    marks.append(equity_marker(book,position,now))
        return marks
    return provide
