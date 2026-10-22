"""Loopback-only, read-only operational view over explicitly registered paper books."""
from __future__ import annotations
from datetime import datetime, timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import hashlib
from pathlib import Path
import re
import sqlite3
import shutil
import tempfile
import threading
import time
from zoneinfo import ZoneInfo

UTC = timezone.utc
BASE = Path('/var/lib')
MAX_JSON = 16 * 1024 * 1024
MAX_ROWS = 10000
BCP3_ACTIVATION = Path('/opt/neorome/coordination/releases/crypto-rebuild-20261005/p3-chain-route-r1/ACTIVATION.json')
BCP3_PARENT = 'neorome-bcp3-streaming-parent-resilience-r2.service'
BCP3_CHILD = re.compile(r'^neorome-bcp3-child-freshness-r2@\d+-\d+-\d+\.service$')
HISTORICAL_CHILD_MIN_AGE_SECONDS = 3600
BOOKS = [
 'neorome-btc-cross5-paper/btc_fast_stochastic_paper.sqlite3',
 'neorome-btc-cross30-paper/btc_stochastic_paper.sqlite3',
 'neorome-btc-fast-stochastic-paper/btc_fast_stochastic_paper.sqlite3',
 'neorome-btc-stochastic-paper/btc_stochastic_paper.sqlite3',
 'neorome-equity-iex-fullfunded-v1/paper.sqlite3',
 'neorome-options-full-port/paper.sqlite3', 'neorome-p2t1/paper.sqlite3',
 'neorome-paper/paper.sqlite3', 'neorome-prediction-paper/paper.sqlite3',
 'neorome-pump-trials-20261007/paper-p1.sqlite3', 'neorome-pump-trials-20261007/paper-p2.sqlite3',
 'neorome-pump-trials-50-20261007/paper-p1.sqlite3', 'neorome-pump-trials-50-20261007/paper-p2.sqlite3',
 'neorome-us-paper-20261007b/session-001/research.sqlite3',
 'neorome-us-stream-20261011/session-001/research.sqlite3', 'neorome-wallet-research/paper.sqlite3',
] + ['neorome-memecoin-program/paper'+suffix+'.sqlite3' for suffix in
     ['', '-b1', '-c1', '-e1', '-e1v100', '-e1v150', '-e2', '-e4', '-e5', '-e6', '-e7', '-e8', '-k1', '-l1', '-l2', '-l3', '-l4', '-l5', '-l6', '-r1', '-w2']]
BOOKS += ['neorome-memecoin-program/'+folder+'/'+file for folder,file in [
 ('h1-flow-event-route-r5','paper-h1.sqlite3'), ('l7t2','paper-l7t2.sqlite3'),
 ('l7t3-revisit','paper-l7t3-revisit.sqlite3'), ('l7t4-coverage-fair','paper-l7t4-coverage-fair.sqlite3'),
 ('l7t5-batch-discovery','paper-l7t5-batch-discovery.sqlite3'), ('l7t6-native','paper-l7t6-native.sqlite3'),
 ('l7t7-watch','paper-l7t7-watch.sqlite3')]]

PREDICTION_SDK_BOOKS = [
    ('btc-stochastic-5m', 'BTC stochastic · 5m', 'stochastic', 'neorome-btc-cross5-paper/btc_fast_stochastic_paper.sqlite3'),
    ('btc-stochastic-30m', 'BTC stochastic · 30m', 'stochastic', 'neorome-btc-cross30-paper/btc_stochastic_paper.sqlite3'),
    ('btc-gap5-5m', 'BTC gap ≤5 · 5m', 'gap5', 'neorome-btc-gap5-fast-paper/btc_fast_stochastic_paper.sqlite3'),
    ('btc-gap5-30m', 'BTC gap ≤5 · 30m', 'gap5', 'neorome-btc-gap5-slow-paper/btc_stochastic_paper.sqlite3'),
]
PREDICTION_ARM_BOOKS = [
    ('brackets', 'BTC TP/SL comparisons', 'neorome-btc-brackets-v1/comparison.sqlite3', 22),
    ('trailing', 'BTC trailing exits', 'neorome-btc-trailing-v1/trailing.sqlite3', 2),
    ('sma1m', 'BTC 1m SMA', 'neorome-btc-sma1m-v1/sma.sqlite3', 4),
    ('interval-expansion', 'BTC interval expansion', 'neorome-btc-interval-expansion-v1/expansion.sqlite3', 54),
]
OPTIONS_ARM_BOOKS = [
    ('original_a', 'Original A · 14/3/3', '14/3/3 stochastic', '20% TP / 20% SL', 'No 15m filter', 'neorome-stochastic-paper/v2/arms/A.json'),
    ('original_b', 'Original B · 5/3/3', '5/3/3 stochastic', '20% TP / 20% SL', 'No 15m filter', 'neorome-stochastic-paper/v2/arms/B.json'),
    ('filtered10_a', 'Filtered 10/10 A · 14/3/3', '14/3/3 stochastic', '10% TP / 10% SL', '15m directional veto: no calls K≥80; no puts K≤20', 'neorome-stochastic-paper/f15-10x10/arms/A.json'),
    ('filtered10_b', 'Filtered 10/10 B · 5/3/3', '5/3/3 stochastic', '10% TP / 10% SL', '15m directional veto: no calls K≥80; no puts K≤20', 'neorome-stochastic-paper/f15-10x10/arms/B.json'),
    ('filtered20_a', 'Filtered 20/20 A · 14/3/3', '14/3/3 stochastic', '20% TP / 20% SL', '15m directional veto: no calls K≥80; no puts K≤20', 'neorome-stochastic-paper/intraday-comparison-v1/arms/A20.json'),
    ('filtered20_b', 'Filtered 20/20 B · 5/3/3', '5/3/3 stochastic', '20% TP / 20% SL', '15m directional veto: no calls K≥80; no puts K≤20', 'neorome-stochastic-paper/intraday-comparison-v1/arms/B20.json'),
    ('sma_2_5', 'SMA 2/5 · completed 5m', 'SMA 2/5 on completed 5m bars', '20% TP / 20% SL', 'No stochastic or directional filter', 'neorome-stochastic-paper/intraday-comparison-v1/arms/SMA.json'),
]
OPTIONS_MARK_MAX_AGE_SECONDS = 15
_target_prediction_paths = {path for _,_,_,path in PREDICTION_SDK_BOOKS}
BOOKS = [path for path in BOOKS if path not in _target_prediction_paths]


def stamp(value):
    parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('timestamp missing timezone')
    return parsed.astimezone(UTC)


def regular(path):
    if path.is_symlink() or any(p.is_symlink() for p in path.parents) or not path.is_file():
        raise ValueError('source is not a regular non-symlink file')
    return path


def read_json(path):
    with regular(path).open('rb') as stream:
        raw = stream.read(MAX_JSON + 1)
    if len(raw) > MAX_JSON:
        raise ValueError('source exceeds read budget')
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError('invalid source shape')
    return value


def cents(value):
    return int((Decimal(str(value)) * 100).quantize(Decimal('1')))


def connection(path):
    regular(path)
    con = sqlite3.connect(path.as_uri()+'?mode=ro', uri=True, timeout=1)
    try:
        con.execute('SELECT name FROM sqlite_master LIMIT 1').fetchone()
    except sqlite3.OperationalError as exc:
        con.close()
        if getattr(exc,'sqlite_errorcode',0) & 255 not in (sqlite3.SQLITE_READONLY,sqlite3.SQLITE_CANTOPEN):raise
        # WAL-mode files without an initialized shared-memory file may require
        # SQLite to create sidecars even for mode=ro. Recover an isolated copy
        # instead of granting the viewer writes to the production directory.
        con = copied_connection(path)
    con.row_factory = sqlite3.Row
    con.execute('PRAGMA query_only=ON')
    deadline = time.monotonic() + 2
    con.set_progress_handler(lambda: int(time.monotonic() > deadline), 10000)
    con.execute('BEGIN')
    return con


class SnapshotConnection(sqlite3.Connection):
    workspace = None
    def close(self):
        try:super().close()
        finally:
            if self.workspace:self.workspace.cleanup();self.workspace=None


def copied_connection(path):
    for attempt in range(3):
        temp=tempfile.TemporaryDirectory(prefix='neorome-view-snapshot-')
        try:
            files=[path,Path(str(path)+'-wal'),Path(str(path)+'-journal')]
            def signature():
                result={}
                for p in files:
                    if p.exists():
                        regular(p);s=p.stat();result[p]=(s.st_ino,s.st_size,s.st_mtime_ns)
                return result
            before=signature()
            if sum(s[1] for s in before.values())>128*1024*1024:raise ValueError('snapshot copy budget exceeded')
            for p in before:shutil.copyfile(p,Path(temp.name)/p.name)
            if before!=signature():
                temp.cleanup();continue
            con=sqlite3.connect(Path(temp.name)/path.name,timeout=1,factory=SnapshotConnection)
            con.workspace=temp
            # Opening may recover WAL/journal in this private temporary copy.
            con.execute('SELECT name FROM sqlite_master LIMIT 1').fetchone()
            con.execute('PRAGMA query_only=ON')
            return con
        except Exception:
            temp.cleanup();raise
    raise ValueError('paper book changed during snapshot copy')


def rows(con, sql, args=()):
    result = con.execute(sql, args).fetchmany(MAX_ROWS + 1)
    if len(result) > MAX_ROWS:
        raise ValueError('row budget exceeded; coverage incomplete')
    return [dict(r) for r in result]


def read_book(path, start, end):
    trades, positions = [], []
    strategy = path.parent.name.removeprefix('neorome-') + '/' + path.stem
    con = connection(path)
    try:
        marker = con.execute("SELECT value FROM settings WHERE key='ledger_marker'").fetchone()
        if not marker or marker[0] != 'neorome-paper-ledger-v1':
            raise ValueError('unrecognized paper ledger')
        for r in rows(con, 'SELECT * FROM fills WHERE julianday(filled_at) BETWEEN julianday(?) AND julianday(?)', (start,end)):
            trades.append(dict(id=strategy+':'+r['fill_id'], desk=r['desk'], strategy=strategy,
                               instrument=r['instrument'], side='entry', quantity=r['quantity'], at=r['filled_at'],
                               value_cents=r['total_cost_cents'], pnl_cents=None))
        for r in rows(con, 'SELECT c.*,p.desk,p.instrument,p.quantity FROM closes c JOIN positions p USING(position_id) WHERE julianday(c.closed_at) BETWEEN julianday(?) AND julianday(?)', (start,end)):
            trades.append(dict(id=strategy+':'+r['close_id'], desk=r['desk'], strategy=strategy,
                               instrument=r['instrument'], side='exit', quantity=r['quantity'], at=r['closed_at'],
                               value_cents=r['proceeds_cents']-r['fee_cents'], pnl_cents=r['realized_pnl_cents']))
        for r in rows(con, "SELECT * FROM positions WHERE state='open'"):
            mark = con.execute('SELECT liquidation_value_cents,source_timestamp FROM market_marks WHERE position_id=? ORDER BY julianday(source_timestamp) DESC LIMIT 1', (r['position_id'],)).fetchone()
            value = mark[0] if mark else None
            positions.append(dict(id=r['position_id'], desk=r['desk'], strategy=strategy, instrument=r['instrument'],
                                  quantity=r['quantity'], basis_cents=r['basis_cents'], mark_cents=value,
                                  pnl_cents=None if value is None else value-r['basis_cents'], mark_at=mark[1] if mark else None))
    finally:
        con.close()
    return trades, positions


def read_arm(path, start, end):
    data = read_json(path)
    trades, positions = [], []
    strategy = data['strategy_id']
    held = data.get('positions', {})
    held = list(held.values()) if isinstance(held, dict) else held
    for r in list(data.get('trades', [])) + held:
        instrument = f"{r['symbol']} {r['expiry']} {r['strike']} {r['type']}"
        common = dict(desk='options', strategy=strategy, instrument=instrument, quantity=r['quantity'])
        if stamp(start) <= stamp(r['entry_time']) <= stamp(end):
            trades.append(dict(common, id=r['position_id']+':entry', side='entry', at=r['entry_time'], value_cents=cents(r['entry_debit']), pnl_cents=None))
        if r.get('exit_time') and stamp(start) <= stamp(r['exit_time']) <= stamp(end):
            trades.append(dict(common, id=r['position_id']+':exit', side='exit', at=r['exit_time'], value_cents=cents(r['exit_credit']), pnl_cents=cents(r['net_pnl'])))
        if not r.get('exit_time'):
            value = cents(Decimal(str(r['last_bid'])) * r['quantity'] * r.get('multiplier', 100)) if r.get('last_bid') is not None else None
            basis = cents(r['entry_debit'])
            positions.append(dict(common, id=r['position_id'], basis_cents=basis, mark_cents=value,
                                  pnl_cents=None if value is None else value-basis, mark_at=r.get('last_quote_time')))
    return trades, positions


def _arm_cash_cents(value):
    if isinstance(value, bool):
        raise ValueError('invalid cash value')
    amount = Decimal(str(value))
    if not amount.is_finite():
        raise ValueError('non-finite cash value')
    return int((amount * 100).quantize(Decimal('1')))


def _arm_trade_exit_pnl(trade, start, end):
    """Return a recorded net exit P&L only when its timestamp is in the NY day."""
    at = trade.get('exit_time')
    if not at:
        return None
    when = stamp(at)
    if not stamp(start) <= when <= stamp(end):
        return None
    if trade.get('net_pnl') is None:
        raise ValueError('exit P&L unavailable')
    return _arm_cash_cents(trade['net_pnl'])


def _arm_open_position(raw, now):
    quantity = raw['quantity']
    if isinstance(quantity, bool) or not isinstance(quantity, (int, float)) or not Decimal(str(quantity)).is_finite():
        raise ValueError('invalid open-position quantity')
    multiplier = raw.get('multiplier', 100)
    if isinstance(multiplier, bool) or not isinstance(multiplier, (int, float)) or not Decimal(str(multiplier)).is_finite():
        raise ValueError('invalid contract multiplier')
    basis = _arm_cash_cents(raw['entry_debit'])
    bid = raw.get('last_bid')
    mark = None if bid is None else _arm_cash_cents(Decimal(str(bid)) * Decimal(str(quantity)) * Decimal(str(multiplier)))
    mark_at = raw.get('last_quote_time')
    age = None
    fresh = None
    if mark_at:
        observed = stamp(mark_at)
        age = int((now - observed).total_seconds())
        fresh = 0 <= age <= OPTIONS_MARK_MAX_AGE_SECONDS
    return dict(position_id=raw.get('position_id'), symbol=raw.get('symbol'), expiry=raw.get('expiry'),
                strike=raw.get('strike'), option_type=raw.get('type'), quantity=quantity,
                multiplier=multiplier, basis_cents=basis, last_observed_mark_cents=mark,
                last_observed_at=mark_at, mark_age_seconds=age, mark_fresh=fresh,
                mark_pnl_cents=None if mark is None else mark-basis)


def options_paper_accounts(base=BASE, now=None):
    """Project only the seven registered options JSON books, with partial coverage."""
    now = now or datetime.now(UTC)
    start = now.astimezone(ZoneInfo('America/New_York')).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(UTC).isoformat()
    end = now.isoformat()
    accounts, errors, sources = [], [], []
    for book_key, title, signal, exit_policy, filter_policy, relative in OPTIONS_ARM_BOOKS:
        account = dict(book_key=book_key, title=title, signal=signal, exit_policy=exit_policy,
                       filter=filter_policy, source=relative, source_status='unavailable', source_as_of=None,
                       strategy_id=None, initial_cash_cents=None, cash_cents=None,
                       total_realized_pnl_cents=None, today_realized_pnl_cents=None,
                       open_positions=None, positions=None)
        try:
            data = read_json(base / relative)
            if not isinstance(data.get('strategy_id'), str) or not data['strategy_id']:
                raise ValueError('strategy identity unavailable')
            cash = _arm_cash_cents(data['cash'])
            initial_cash = _arm_cash_cents(data['initial_cash'])
            total_realized = _arm_cash_cents(data['realized_pnl'])
            trades = data.get('trades')
            held = data.get('positions')
            if not isinstance(trades, list) or not isinstance(held, (dict, list)):
                raise ValueError('invalid options account collections')
            position_rows = list(held.values()) if isinstance(held, dict) else held
            today_realized = 0
            for trade in trades:
                if not isinstance(trade, dict):
                    raise ValueError('invalid trade record')
                pnl = _arm_trade_exit_pnl(trade, start, end)
                if pnl is not None:
                    today_realized += pnl
            projected_positions = [_arm_open_position(row, now) for row in position_rows]
            source_as_of = data.get('last_observed_at')
            if source_as_of is not None:
                stamp(source_as_of)
            account.update(source_status='ok', source_as_of=source_as_of,
                           strategy_id=data['strategy_id'], initial_cash_cents=initial_cash,
                           cash_cents=cash, total_realized_pnl_cents=total_realized,
                           today_realized_pnl_cents=today_realized,
                           open_positions=len(projected_positions), positions=projected_positions)
            sources.append(dict(source=relative, status='ok', error=None))
        except (OSError, ValueError, KeyError, TypeError, ArithmeticError) as exc:
            message = f'{relative}: {type(exc).__name__}; account unavailable'
            errors.append(message)
            sources.append(dict(source=relative, status='error', error=message))
        accounts.append(account)
    expected, observed = len(OPTIONS_ARM_BOOKS), sum(source['status'] == 'ok' for source in sources)
    return dict(as_of=end, accounts=accounts,
                coverage=dict(expected=expected, observed=observed, complete=observed == expected,
                              sources=sources, errors=errors), errors=errors)


def read_p3(path, start, end):
    con = connection(path)
    trades, positions = [], []
    try:
        for r in rows(con,'SELECT * FROM entries WHERE julianday(opened_at) BETWEEN julianday(?) AND julianday(?)',(start,end)):
            trades.append(dict(id=r['entry_id']+':entry',desk='crypto',strategy='P3',instrument=r['mint'],side='entry',quantity=r['token_units'],at=r['opened_at'],value_cents=r['all_in_cents'],pnl_cents=None))
        for r in rows(con,'SELECT s.*,e.mint FROM sales s JOIN entries e USING(entry_id) WHERE julianday(sold_at) BETWEEN julianday(?) AND julianday(?)',(start,end)):
            trades.append(dict(id=r['sale_id'],desk='crypto',strategy='P3',instrument=r['mint'],side='exit',quantity=r['quantity'],at=r['sold_at'],value_cents=r['net_proceeds_cents'],pnl_cents=r['realized_pnl_cents']))
        for r in rows(con,"SELECT * FROM entries WHERE state!='closed' AND remaining_units>0"):
            positions.append(dict(id=r['entry_id'],desk='crypto',strategy='P3',instrument=r['mint'],quantity=r['remaining_units'],basis_cents=r['remaining_basis_cents'],mark_cents=None,pnl_cents=None,mark_at=None))
    finally:
        con.close()
    return trades, positions


def read_bcp3(path, start, end):
    data = read_json(path)
    if data.get('schema') != 'neorome-p3-paper-ledger-v1':
        raise ValueError('invalid BCP3 schema')
    trades, positions = [], []
    for e in data.get('events', []):
        if e.get('type') not in {'modeled_entry','modeled_exit'} or not stamp(start) <= stamp(e['at']) <= stamp(end):
            continue
        r=e['data']; closing=e['type']=='modeled_exit'
        value=int(r['proceedsUsdMicros'] if closing else r['entryBasisUsdMicros'])
        pnl=value-int(r['entryBasisUsdMicros']) if closing else None
        trades.append(dict(id=str(e['sequence']),desk='crypto',strategy='BCP3',instrument=r.get('mint','Unreported mint'),side='exit' if closing else 'entry',quantity=r.get('tokenUnits'),at=e['at'],value_cents=round(value/10000),pnl_cents=None if pnl is None else round(pnl/10000)))
    r=data.get('position')
    if r:
        positions.append(dict(id=r['positionId'],desk='crypto',strategy='BCP3',instrument=r.get('mint','Unreported mint'),quantity=r.get('tokenUnits'),basis_cents=round(int(r['entryBasisUsdMicros'])/10000),mark_cents=None,pnl_cents=None,mark_at=None))
    return trades,positions


def read_prediction_sdk(base, account_id, name, family, relative, start, end, now):
    """Read one existing SDK BTC account, fills and latest retained marks."""
    path = base / relative
    con = connection(path)
    try:
        marker = con.execute("SELECT value FROM settings WHERE key='ledger_marker'").fetchone()
        if not marker or marker[0] != 'neorome-paper-ledger-v1':
            raise ValueError('unrecognized paper ledger')
        ledger_accounts = rows(con, "SELECT * FROM accounts WHERE desk='prediction'")
        if len(ledger_accounts) != 1:
            raise ValueError('expected one prediction account')
        account = ledger_accounts[0]
        positions = rows(con, "SELECT * FROM positions WHERE desk='prediction' AND state='open'")
        open_cost = sum(int(p['basis_cents']) for p in positions)
        marks = []
        for p in positions:
            mark = con.execute('SELECT liquidation_value_cents,source_timestamp FROM market_marks WHERE position_id=? ORDER BY julianday(source_timestamp) DESC LIMIT 1', (p['position_id'],)).fetchone()
            marks.append((mark[0], mark[1]) if mark else (None, None))
        filled = rows(con, "SELECT * FROM fills WHERE desk='prediction' AND julianday(filled_at) BETWEEN julianday(?) AND julianday(?)", (start,end))
        closed = rows(con, "SELECT c.*,p.instrument,p.quantity FROM closes c JOIN positions p USING(position_id) WHERE p.desk='prediction' AND julianday(c.closed_at) BETWEEN julianday(?) AND julianday(?)", (start,end))
        trades = []
        for r in filled:
            trades.append(dict(id=f'{account_id}:{r["fill_id"]}', account_id=account_id, account_name=name,
                desk='prediction', strategy=family, instrument=r['instrument'], side='entry',
                quantity=r['quantity'], at=r['filled_at'], value_cents=r['total_cost_cents'], pnl_cents=None))
        for r in closed:
            trades.append(dict(id=f'{account_id}:{r["close_id"]}', account_id=account_id, account_name=name,
                desk='prediction', strategy=family, instrument=r['instrument'], side='exit',
                quantity=r['quantity'], at=r['closed_at'], value_cents=r['proceeds_cents']-r['fee_cents'], pnl_cents=r['realized_pnl_cents']))
        today_pnl = sum(int(r['realized_pnl_cents']) for r in closed)
        known_marks = [m for m in marks if m[0] is not None]
        latest_at = max((m[1] for m in known_marks), key=stamp) if known_marks else None
        mark_total = sum(int(m[0]) for m in known_marks) if len(known_marks) == len(positions) else None
        age = None if latest_at is None else (now - stamp(latest_at)).total_seconds()
        summary = dict(id=account_id, name=name, family=family, source=relative, coverage_status='ok',
            entries=len(filled), exits=len(closed), settlements=0, realized_pnl_today_cents=today_pnl,
            initial_cash_cents=int(account['initial_cash_cents']),
            cash_cents=int(account['available_cash_cents']), open_cost_cents=open_cost,
            reserved_cents=int(account['reserved_cash_cents']), open_positions=len(positions),
            mark_cents=mark_total, mark_at=latest_at, mark_age_seconds=age, mark_fresh=None,
            error=None)
        for p, (value, mark_at) in zip(positions, marks):
            trades_position = dict(id=p['position_id'], desk='prediction', strategy=family,
                instrument=p['instrument'], quantity=p['quantity'], basis_cents=p['basis_cents'],
                mark_cents=value, pnl_cents=None if value is None else value-p['basis_cents'], mark_at=mark_at,
                account_id=account_id, account_name=name)
            # The caller receives per-account positions through this private field.
            summary.setdefault('_positions', []).append(trades_position)
    finally:
        con.close()
    return summary, trades


def read_prediction_arms(base, family, title, relative, expected, start, end, now):
    """Project the bounded current window and only full lineage for cohorts closed in it."""
    path = base / relative
    con = connection(path)
    try:
        columns = {r['name'] for r in con.execute('PRAGMA table_info(arms)').fetchall()}
        required = {'arm_id','timeframe','initial_cash','available_cash','open_cost','realized','reserved','position','latest_mark','mark_time'}
        if not required.issubset(columns):
            raise ValueError('arm schema mismatch')
        arm_rows = rows(con, 'SELECT * FROM arms ORDER BY arm_id')
        if len(arm_rows) != expected:
            raise ValueError(f'account inventory mismatch ({len(arm_rows)} observed)')
        start_epoch, end_epoch = stamp(start).timestamp(), stamp(end).timestamp()
        window_rows = rows(con, "SELECT * FROM events WHERE at BETWEEN ? AND ? AND kind IN ('entry','exit_fill','settlement') ORDER BY at,event_id",
                           (start_epoch,end_epoch))
        window_by_arm = {}
        for event in window_rows:
            window_by_arm.setdefault(event['arm_id'], []).append(event)
        candidate_keys = set()
        for arm_id, events in window_by_arm.items():
            for event in events:
                if event['kind'] in {'exit_fill','settlement'}:
                    if not event['cohort_id']:
                        raise ValueError('terminal event is missing cohort identity')
                    candidate_keys.add((arm_id,event['cohort_id']))
        lineage_by_key = {key: [] for key in candidate_keys}
        lineage_counts = {}
        lineage_row_count = 0
        candidate_ids = sorted({cohort_id for _,cohort_id in candidate_keys})
        for offset in range(0, len(candidate_ids), 400):
            cohort_ids = candidate_ids[offset:offset+400]
            placeholders = ','.join('?' for _ in cohort_ids)
            history_rows = rows(con,
                f"SELECT * FROM events WHERE cohort_id IN ({placeholders}) AND kind IN ('entry','exit_fill','settlement') ORDER BY event_id",
                tuple(cohort_ids))
            lineage_row_count += len(history_rows)
            if lineage_row_count > MAX_ROWS:
                raise ValueError('cohort lineage row budget exceeded; coverage incomplete')
            for event in history_rows:
                key=(event['arm_id'],event['cohort_id'])
                if key not in candidate_keys:
                    continue
                lineage_counts[key[0]]=lineage_counts.get(key[0],0)+1
                if lineage_counts[key[0]] > MAX_ROWS:
                    raise ValueError('per-arm cohort lineage row budget exceeded; coverage incomplete')
                lineage_by_key[key].append((event,json.loads(event['payload'])))
        trades = []
        summaries = []
        positions = []
        for arm in arm_rows:
            aid = f'btc-{family}-{arm["arm_id"]}'
            today_rows = window_by_arm.get(arm['arm_id'], [])
            decoded = [(e, json.loads(e['payload'])) for e in today_rows]
            entries = [(e,p) for e,p in decoded if e['kind']=='entry']
            exits = [(e,p) for e,p in decoded if e['kind']=='exit_fill']
            settlements = [(e,p) for e,p in decoded if e['kind']=='settlement']
            closed_pnl = 0
            closed_pnl_known = True
            terminal_candidates = {e['cohort_id'] for e,_ in exits+settlements if e['cohort_id']}
            event_pnl = {}
            for cohort_id in terminal_candidates:
                history=lineage_by_key.get((arm['arm_id'],cohort_id), [])
                history=sorted(history, key=lambda item: item[0]['event_id'])
                entry=next(((e,p) for e,p in history if e['kind']=='entry'),None)
                settlement=next(((e,p) for e,p in history if e['kind']=='settlement'),None)
                cohort_exits=[(e,p) for e,p in history if e['kind']=='exit_fill']
                # Individual fill P&L is proceeds net of fees minus the exact
                # allocated basis. Legacy events without basis can be rebuilt
                # from the ordered cohort lineage using the engine's ceil rule.
                remaining_quantity = entry[1].get('quantity') if entry else None
                remaining_basis = entry[1].get('cost_cents') if entry else None
                lineage_valid = (isinstance(remaining_quantity, int) and not isinstance(remaining_quantity, bool)
                                 and remaining_quantity > 0 and isinstance(remaining_basis, int)
                                 and not isinstance(remaining_basis, bool) and remaining_basis >= 0)
                prior_exit_pnl = 0
                prior_exit_pnl_known = True
                for exit_event, exit_payload in cohort_exits:
                    quantity = exit_payload.get('quantity')
                    proceeds = exit_payload.get('proceeds_cents')
                    basis = exit_payload.get('basis_cents')
                    if (basis is None and lineage_valid and isinstance(quantity, int)
                            and not isinstance(quantity, bool) and 0 < quantity <= remaining_quantity):
                        basis = (remaining_basis * quantity + remaining_quantity - 1) // remaining_quantity
                    valid = (lineage_valid and isinstance(quantity, int) and not isinstance(quantity, bool)
                             and 0 < quantity <= remaining_quantity and isinstance(basis, int)
                             and not isinstance(basis, bool) and 0 <= basis <= remaining_basis
                             and isinstance(proceeds, int) and not isinstance(proceeds, bool) and proceeds >= 0)
                    if valid:
                        pnl = proceeds - basis
                        event_pnl[exit_event['event_id']] = (pnl, 'known', None)
                        prior_exit_pnl += pnl
                        remaining_quantity -= quantity
                        remaining_basis -= basis
                    else:
                        reason = 'exit lineage is missing valid quantity, net proceeds, or allocated basis'
                        event_pnl[exit_event['event_id']] = (None, 'unknown', reason)
                        prior_exit_pnl_known = False
                        lineage_valid = False
                if settlement:
                    settlement_event, settlement_payload = settlement
                    full_pnl = settlement_payload.get('pnl_cents')
                    payout = settlement_payload.get('payout_cents')
                    incremental = None
                    if prior_exit_pnl_known and isinstance(full_pnl, int) and not isinstance(full_pnl, bool):
                        incremental = full_pnl - prior_exit_pnl
                    elif (lineage_valid and isinstance(payout, int) and not isinstance(payout, bool)
                          and remaining_basis is not None):
                        incremental = payout - remaining_basis
                    if incremental is None:
                        reason = 'settlement is missing full-cohort P&L or exact remaining-basis lineage'
                        event_pnl[settlement_event['event_id']] = (None, 'unknown', reason)
                    else:
                        event_pnl[settlement_event['event_id']] = (incremental, 'known', None)
                if settlement:
                    # Settlement payload P&L is the authoritative full-cohort
                    # result, including prior exits, even if an older fill
                    # payload is incomplete.
                    terminal_at,pnl=settlement[0]['at'],settlement[1].get('pnl_cents')
                else:
                    terminal_at=max((e['at'] for e,_ in cohort_exits), default=None)
                    entry_quantity=entry[1].get('quantity') if entry else None
                    quantities=[p.get('quantity') for _,p in cohort_exits]
                    quantities_valid=(bool(cohort_exits) and all(isinstance(q,int) and not isinstance(q,bool) and q > 0 for q in quantities))
                    if (entry_quantity is not None and isinstance(entry_quantity,int) and not isinstance(entry_quantity,bool)
                            and entry_quantity > 0 and quantities_valid and sum(quantities) < entry_quantity):
                        continue
                    terminal_quantity_valid=(entry_quantity is not None and isinstance(entry_quantity,int)
                                             and not isinstance(entry_quantity,bool) and entry_quantity > 0
                                             and quantities_valid and sum(quantities) == entry_quantity)
                    cost=entry[1].get('cost_cents') if entry else None
                    proceeds=[p.get('proceeds_cents') for _,p in cohort_exits]
                    accounting_valid=(terminal_quantity_valid and isinstance(cost,int) and not isinstance(cost,bool)
                                      and cost >= 0 and all(isinstance(value,int) and not isinstance(value,bool) and value >= 0
                                                            for value in proceeds))
                    pnl=sum(proceeds)-cost if accounting_valid else None
                if terminal_at is not None and start_epoch <= float(terminal_at) <= end_epoch:
                    if isinstance(pnl,int) and not isinstance(pnl,bool):
                        closed_pnl += pnl
                    else:
                        closed_pnl_known = False
            for e,p in decoded:
                at = datetime.fromtimestamp(float(e['at']), UTC).isoformat()
                kind=e['kind']
                if kind not in {'entry','exit_fill','settlement'}: continue
                side = {'entry':'entry','exit_fill':'exit','settlement':'settlement'}[kind]
                if kind == 'entry':
                    pnl_value, pnl_status, pnl_reason = None, 'not_applicable', None
                else:
                    pnl_value, pnl_status, pnl_reason = event_pnl.get(
                        e['event_id'], (None, 'unknown', 'terminal cohort lineage is unavailable'))
                trades.append(dict(id=f'{aid}:event:{e["event_id"]}', account_id=aid,
                    account_name=f'{arm["arm_id"]} · {arm["timeframe"]}', desk='prediction',
                    strategy=title, instrument=e['ticker'] or 'Unknown contract', side=side,
                    quantity=p.get('quantity'), at=at,
                    value_cents=p.get('cost_cents') if kind=='entry' else p.get('proceeds_cents',p.get('payout_cents')),
                    pnl_cents=pnl_value, pnl_status=pnl_status, pnl_reason=pnl_reason))
            pos = json.loads(arm['position']) if arm['position'] else None
            mark_at = None if arm['mark_time'] is None else datetime.fromtimestamp(float(arm['mark_time']), UTC).isoformat()
            age = None if mark_at is None else (now-stamp(mark_at)).total_seconds()
            if pos:
                basis=int(pos['cost_cents'])
                positions.append(dict(id=f'{aid}:{pos["cohort_id"]}',desk='prediction',strategy=title,
                    instrument=pos['ticker'],quantity=pos['quantity'],basis_cents=basis,
                    mark_cents=arm['latest_mark'],pnl_cents=None if arm['latest_mark'] is None else int(arm['latest_mark'])-basis,
                    mark_at=mark_at,account_id=aid,account_name=f'{arm["arm_id"]} · {arm["timeframe"]}'))
            summaries.append(dict(id=aid,name=f'{arm["arm_id"]} · {arm["timeframe"]}',family=family,
                source=relative,coverage_status='ok',entries=len(entries),
                exits=len(exits),
                settlements=len(settlements),
                realized_pnl_today_cents=closed_pnl if closed_pnl_known else None,cash_cents=int(arm['available_cash']),
                initial_cash_cents=int(arm['initial_cash']),open_cost_cents=int(arm['open_cost']),reserved_cents=int(arm['reserved']),
                open_positions=int(pos is not None),mark_cents=arm['latest_mark'],mark_at=mark_at,
                mark_age_seconds=age,mark_fresh=age is not None and 0 <= age <= 1,error=None))
    finally:
        con.close()
    return summaries, trades, positions


def prediction_accounts(base, start, end, now):
    accounts, trades, positions, source_status, errors = [], [], [], [], []
    for account_id, name, family, relative in PREDICTION_SDK_BOOKS:
        try:
            account, fills = read_prediction_sdk(base, account_id, name, family, relative, start, end, now)
            positions.extend(account.pop('_positions', []))
            accounts.append(account); trades.extend(fills)
            source_status.append(dict(source=relative, expected=1, observed=1, status='ok', error=None))
        except (OSError, ValueError, KeyError, TypeError, sqlite3.Error) as exc:
            message=f'{relative}: {getattr(exc,"sqlite_errorname",type(exc).__name__)}; account coverage incomplete'
            errors.append(message)
            source_status.append(dict(source=relative, expected=1, observed=0, status='error', error=message))
    for family, title, relative, expected in PREDICTION_ARM_BOOKS:
        try:
            found, events, open_positions = read_prediction_arms(base, family, title, relative, expected, start, end, now)
            accounts.extend(found); trades.extend(events); positions.extend(open_positions)
            source_status.append(dict(source=relative, expected=expected, observed=len(found), status='ok', error=None))
        except (OSError, ValueError, KeyError, TypeError, sqlite3.Error) as exc:
            message=f'{relative}: {getattr(exc,"sqlite_errorname",type(exc).__name__)}; account coverage incomplete'
            errors.append(message)
            source_status.append(dict(source=relative, expected=expected, observed=0, status='error', error=message))
    expected=sum(s['expected'] for s in source_status)
    observed=sum(s['observed'] for s in source_status)
    coverage=dict(expected=expected, observed=observed, complete=not errors and observed==expected,
                  sources=source_status, errors=errors)
    return accounts, trades, positions, coverage


def prediction_realized_pnl(accounts, other_trades):
    """Aggregate prediction P&L without converting any unknown account to zero."""
    account_pnl=[account.get('realized_pnl_today_cents') for account in accounts]
    if any(not isinstance(value,int) or isinstance(value,bool) for value in account_pnl):
        return None
    return sum(account_pnl)+sum(trade.get('pnl_cents') or 0 for trade in other_trades)


def bcp3_source(base, activation_path=BCP3_ACTIVATION):
    """Configured source identity is not a liveness or fill attestation."""
    ledger=read_json(base/'neorome-bcp3-paper/ledger.json')
    activation=read_json(activation_path)
    if (activation.get('schema')!='neorome-bcp3-chain-source-activation-v1'
            or activation.get('state')!='committed'
            or activation.get('newPolicySha256')!=ledger.get('policySha256')):
        raise ValueError('BCP3 source epoch is not committed or ledger-bound')
    active=activation['activeDiscoverySource']
    if (active.get('provider')!='Helius' or active.get('endpoint')!='https://mainnet.helius-rpc.com/'
            or active.get('adapterId')!='p3-parent-chain-discovery-v1'):
        raise ValueError('BCP3 source identity is unrecognized')
    result={'status':'configured; awaiting current cycle evidence', 'as_of':activation['asOf'],
        'provider':active['provider'], 'endpoint':active['endpoint'], 'adapter_id':active['adapterId'],
        'epoch':activation['newPolicySha256'], 'source_config_sha256':activation['sourceConfigSha256'],
        'coverage':'newest five eligible signatures from a bounded 20-row page; not a complete launch stream',
        'verification':'a natural paper fill, separate mark and normal exit are not implied by configuration'}
    reports=list((base/'neorome-bcp3-paper/reports').glob('report-*.json'))
    if len(reports)>MAX_ROWS:raise ValueError('BCP3 report inventory exceeds read bound')
    if reports:
        report=read_json(max(reports,key=lambda path:path.stat().st_mtime_ns))
        binding=report.get('discoverySourceBinding',{})
        if (report.get('policySha256')==result['epoch'] and binding.get('sourceConfigSha256')==result['source_config_sha256']
                and binding.get('adapterId')==result['adapter_id']):
            result.update(status='current source cycle observed',cycle_at=report['asOf'],
                cycle_status=report.get('status'),cycle_reason=report.get('cycleResult',{}).get('reason'))
    return result


def retained_spread_mark(base, position, now):
    """Display a last-observed mark from retained evidence; never write the book."""
    state=read_json(base/'neorome-options-full-port/state.json')
    entry=state.get('entry_record',{})
    if entry.get('position_id')!=position['id'] or entry.get('ledger_cost_cents')!=position['basis_cents']:
        return
    root=base/'neorome-options-full-port-inbox/lifecycle'
    pointer=read_json(root/'current.json')
    date=pointer['date']
    if len(date)!=10 or any(c not in '0123456789-' for c in date):raise ValueError('invalid evidence date')
    digest=lambda d:hashlib.sha256(json.dumps(d,sort_keys=True,separators=(',',':'),ensure_ascii=True).encode()).hexdigest()
    candidates=[]
    for event in pointer.get('events',[])[-4:]:
        filename=event['file']
        if Path(filename).name!=filename:raise ValueError('invalid evidence path')
        env=read_json(root/date/filename)
        if digest({k:v for k,v in env.items() if k!='artifact_sha256'})!=event['sha256']:raise ValueError('invalid evidence hash')
        capture=env['capture']
        if capture.get('broker_write_called') is not False:continue
        quotes={r['symbol']:r for r in capture.get('normalized_quotes',[])}
        value=Decimal(0);times=[]
        for leg in entry['legs']:
            quote=quotes[leg['symbol']];at=stamp(quote['timestamp'])
            bid,ask=Decimal(str(quote['bid'])),Decimal(str(quote['ask']))
            if not bid.is_finite() or not ask.is_finite() or not 0<=bid<=ask:raise ValueError('invalid quote')
            if not stamp(entry['entry_as_of'])<=at<=now:raise ValueError('invalid quote time')
            value+=(bid if leg['side']=='buy' else -ask)*leg['multiplier']*entry['quantity']
            times.append(at)
        if len(times)!=2 or (max(times)-min(times)).total_seconds()>60:continue
        if value>=0:candidates.append((min(times),cents(value)))
    if candidates:
        at,value=max(candidates)
        if not position.get('mark_at') or at>stamp(position['mark_at']):
            position.update(mark_cents=value,pnl_cents=value-position['basis_cents'],mark_at=at.isoformat(),mark_source='retained quote evidence; not a persisted ledger mark')


def unit_exit_time(value):
    """Parse explicit UTC systemd clocks or timezone-bound ISO timestamps."""
    if not isinstance(value, str) or not value:
        return None
    try:
        return stamp(value)
    except (ValueError, TypeError):
        try:
            return datetime.strptime(value, '%a %Y-%m-%d %H:%M:%S UTC').replace(tzinfo=UTC)
        except ValueError:
            return None


def incident_groups(maintenance, errors, now):
    """Display classification only: never resolves or mutates raw incidents."""
    current, historical = [], []
    units = maintenance.get('units')
    units = units if isinstance(units, dict) else {}
    try:
        observed = stamp(maintenance.get('as_of'))
        fresh = maintenance.get('status') != 'stale' and -5 <= (now - observed).total_seconds() <= 30
    except (ValueError, TypeError):
        fresh = False
    parent = units.get(BCP3_PARENT, {})
    parent = parent if isinstance(parent, dict) else {}
    parent_active = (parent.get('ActiveState') == 'active'
                     and re.fullmatch(r'[1-9][0-9]{0,9}', str(parent.get('MainPID', ''))) is not None)
    issues = maintenance.get('issues')
    if not isinstance(issues, list):
        issues = [{'key': 'maintenance:issues', 'reason': 'incident_list_unavailable'}]
    for raw in issues:
        issue = dict(raw) if isinstance(raw, dict) else {
            'key': 'maintenance:issue', 'reason': 'incident_record_invalid'}
        key = issue.get('key', '')
        unit = key[5:] if isinstance(key, str) and key.startswith('unit:') else ''
        metadata = units.get(unit, {})
        metadata = metadata if isinstance(metadata, dict) else {}
        exited = unit_exit_time(metadata.get('ExecMainExitTimestamp'))
        if exited is not None:
            issue['last_exit_at'] = exited.isoformat()
        retained = (issue.get('reason') == 'unit_failed' and BCP3_CHILD.fullmatch(unit)
                    and fresh and parent_active and metadata.get('ActiveState') == 'failed'
                    and str(metadata.get('MainPID')) == '0' and exited is not None
                    and (now - exited).total_seconds() >= HISTORICAL_CHILD_MIN_AGE_SECONDS)
        issue['classification'] = 'historical' if retained else 'current'
        if retained:
            issue['classification_detail'] = (
                'Completed failed BC-P3 attempt older than one hour. Retained for review; '
                'this does not mean its cause is resolved.')
            historical.append(issue)
        else:
            current.append(issue)
    for index, error in enumerate(errors):
        current.append({'key': f'snapshot-error-{index + 1}', 'reason': str(error),
                        'classification': 'current'})
    historical.sort(key=lambda issue: stamp(issue['last_exit_at']), reverse=True)
    return {'current': current, 'historical': historical,
            'raw_issue_count': len(issues), 'snapshot_error_count': len(errors),
            'observer_as_of': maintenance.get('as_of'),
            'historical_rule': ('Only completed BC-P3 child failures older than one hour, '
                                'with fresh observer metadata and an active parent. '
                                'Unknown evidence and persistent job failures stay current. '
                                'Historical does not mean resolved.')}


def snapshot(base=BASE, now=None):
    now = now or datetime.now(UTC)
    start = now.astimezone(ZoneInfo('America/New_York')).replace(hour=0,minute=0,second=0,microsecond=0).astimezone(UTC).isoformat()
    end = now.isoformat()
    out = dict(as_of=end,window_start=start,mode='paper_only',errors=[],agents=[],desks=[],trades=[],positions=[],maintenance={},coverage={'books':0,'notes':[
        'Today since midnight New York. Entries and exits are separate fill events. All records are paper simulations.',
        'Independent experiment books are summed; this is not a consolidated portfolio return.',
        'Marks retain their original timestamps. After-hours values are last observations, not executable live quotes.',
        'Coverage includes the E1V100 and E1V150 continuation experiments; newly deployed books require registration.'
    ]})
    accounts, account_trades, account_positions, account_coverage = prediction_accounts(base, start, end, now)
    out['prediction_accounts'] = accounts
    out['prediction_account_coverage'] = account_coverage
    out['trades'].extend(account_trades)
    out['positions'].extend(account_positions)
    out['errors'].extend(account_coverage['errors'])
    out['coverage']['prediction_accounts'] = account_coverage
    out['coverage']['books'] += sum(source['status']=='ok' for source in account_coverage['sources'])
    out['coverage']['notes'].append('BTC comparison accounts are independent paper books; their balances and results are not pooled firm NAV.')
    readers = [(read_book, base/p) for p in BOOKS]
    readers += [(read_arm,base/relative) for _,_,_,_,_,relative in OPTIONS_ARM_BOOKS]
    readers += [(read_p3,base/'neorome-p3-paper-20261007/paper-p3.sqlite3'), (read_bcp3,base/'neorome-bcp3-paper/ledger.json')]
    for reader,path in readers:
        try:
            trades,positions=reader(path,start,end)
            out['trades'].extend(trades);out['positions'].extend(positions);out['coverage']['books']+=1
        except (OSError,ValueError,KeyError,TypeError,sqlite3.Error) as exc:
            out['errors'].append(f'{path.relative_to(base)}: {getattr(exc,"sqlite_errorname",type(exc).__name__)}; coverage incomplete')
    book_errors=bool(out['errors'])
    for position in out['positions']:
        if position['strategy']=='options-full-port/paper':
            try:
                retained_spread_mark(base,position,now)
                if position.get('mark_source'):out['coverage']['notes'].append('SPY spread display uses retained quote evidence; its production ledger mark is still awaiting a successful review.')
            except (OSError,ValueError,KeyError,TypeError):
                out['coverage']['notes'].append('Retained spread quote evidence unavailable; ledger mark shown if present.')
    def status(relative, max_age=60):
        try:
            d=read_json(base/relative)
            at=d.get('as_of',d.get('updated_at'))
            if at is None or not -5 <= (now-stamp(at)).total_seconds() <= max_age:
                out['errors'].append(relative+': stale status')
                d['status']='stale'
            return d
        except (OSError,ValueError,TypeError):
            out['errors'].append(relative+': unavailable')
            return {'status':'unknown'}
    maintenance=status('neorome-maintenance/status.json',30)
    out['maintenance']={k:maintenance.get(k) for k in ('as_of','status','issues','actions','metrics')}
    # Preserve recent repair results beyond the five-second observer pass.
    try:
        p=regular(base/'neorome-maintenance/repair-events.jsonl')
        with p.open('rb') as f:
            f.seek(max(0,p.stat().st_size-65536)); tail=f.read(65536).splitlines()
        recent=[]
        for line in tail:
            try:
                event=json.loads(line)
                if event.get('event')=='repair_result':recent.append({'unit':event['unit'],'status':event['status']+' · '+event['at']})
            except (ValueError,KeyError):pass
        out['maintenance']['actions']=recent[-10:][::-1]
    except (OSError,ValueError):pass
    readiness=status('neorome-maintenance-ai/inbox/desk-readiness.json')
    ai=status('neorome-maintenance-ai/state/status.json',180)
    out['agents']=[dict(id='oversight',name='Firm oversight',role='Architecture, integration and cross-desk review',status='on demand',as_of=None,detail='Conversational task activity is not inferred from service state.'),
                   dict(id='maintenance',name='Maintenance observer',role='Five-second checks and bounded recovery',status=maintenance.get('status','unknown'),as_of=maintenance.get('as_of'),detail='Raw observer status includes retained failed runs. Display grouping does not resolve incidents.'),
                   dict(id='maintenance-ai',name='Maintenance diagnosis',role='Isolated code diagnosis and tested patch preparation',status=ai.get('status','unknown'),as_of=ai.get('updated_at'),detail=f"{ai.get('calls_used_today','?')} / {ai.get('daily_call_limit','?')} diagnosis runs today"),
                   dict(id='data',name='Data and execution runtime',role='Collectors, deterministic signals and paper fills',status='observed',as_of=end,detail='Runtime service states listed below; no LLM call per market tick.')]
    if BCP3_ACTIVATION.exists():
        try:
            source=bcp3_source(base)
            out['bcp3_source']=source
            detail=f"{source['provider']} · {source['endpoint']} · epoch {source['epoch'][:12]} · {source.get('cycle_status','awaiting first cycle')}"
            if source.get('cycle_reason'):detail+=' · '+source['cycle_reason']
            out['agents'].append(dict(id='bcp3-source',name='BC-P3 active discovery',role='Paper verification epoch',
                status=source['status'],as_of=source.get('cycle_at',source['as_of']),detail=detail))
            out['coverage']['notes'].append('BC-P3 active discovery is Helius; frozen PumpPortal template descriptors are historical compatibility metadata, not current source provenance. '+source['coverage']+'.')
        except (OSError,ValueError,KeyError,TypeError):
            out['agents'].append(dict(id='bcp3-source',name='BC-P3 active discovery',role='Source epoch verification',
                status='unknown',as_of=None,detail='Source activation or ledger/report binding is unavailable; no source-health claim.'))
    for desk,name in [('equities','Equities'),('options','Options'),('prediction','Prediction markets'),('crypto','Crypto')]:
        profile=readiness.get('desks',{}).get(desk,{})
        state=profile.get('status','unknown') if readiness.get('status')!='stale' else 'stale'
        if desk=='options' and any(i.get('key','').startswith('outcome:options') for i in maintenance.get('issues',[])):
            state='degraded'
        trades=[r for r in out['trades'] if r['desk']==desk]
        if desk=='prediction':
            account_trades=[r for r in trades if r.get('account_id')]
            other_trades=[r for r in trades if not r.get('account_id')]
            entries=sum(a['entries'] for a in accounts)+sum(r['side']=='entry' for r in other_trades)
            exits=sum(a['exits'] for a in accounts)+sum(r['side']=='exit' for r in other_trades)
            settlements=sum(a['settlements'] for a in accounts)+sum(r['side']=='settlement' for r in other_trades)
            realized=prediction_realized_pnl(accounts, other_trades)
            if not account_coverage['complete']:
                entries=exits=settlements=realized=None
        else:
            entries=sum(r['side']=='entry' for r in trades)
            exits=sum(r['side']=='exit' for r in trades)
            settlements=sum(r['side']=='settlement' for r in trades)
            realized=sum(r['pnl_cents'] or 0 for r in trades)
        out['desks'].append(dict(id=desk,name=name,status=state,entries=entries,exits=exits,settlements=settlements,realized_pnl_cents=realized,open_positions=sum(r['desk']==desk for r in out['positions'])))
        if book_errors:
            # An incomplete book set cannot truthfully report a complete total.
            out['desks'][-1].update(realized_pnl_cents=None,entries=None,exits=None,open_positions=None)
        out['agents'].append(dict(id=desk,name=name+' desk',role='Strategy runtime · paper only',status=state,as_of=readiness.get('as_of'),detail='Operational readiness; trade eligibility is evaluated independently.'))
    for name,unit in maintenance.get('units',{}).items():
        out['agents'].append(dict(id=name,name=name.removeprefix('neorome-'),role='Runtime service' if name.endswith('.service') else 'Runtime schedule',status=unit.get('ActiveState','unknown'),as_of=maintenance.get('as_of'),detail=unit.get('SubState','')))
    out['incident_groups'] = incident_groups(maintenance, out['errors'], now)
    out['trades'].sort(key=lambda row:stamp(row['at']),reverse=True)
    return out


_lock=threading.Lock()
_cached=None
_generated=0.0


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        global _cached,_generated
        if self.headers.get('Host','').split(':')[0] not in {'127.0.0.1','localhost'}:
            self.send_error(403);return
        if self.path=='/api/snapshot':
            with _lock:
                if _cached is None or time.monotonic()-_generated>=15:
                    _cached=json.dumps(snapshot(),allow_nan=False,separators=(',',':')).encode();_generated=time.monotonic()
                body=_cached
            mime='application/json'
        elif self.path=='/api/options-paper':
            body=json.dumps(options_paper_accounts(),allow_nan=False,separators=(',',':')).encode()
            mime='application/json'
        elif self.path in {'/btc-brackets','/api/btc-brackets'}:
            try:
                if self.path=='/api/btc-brackets':
                    body=json.dumps(read_json(BASE/'neorome-btc-brackets-v1/status.json'),allow_nan=False).encode();mime='application/json'
                else:
                    path=regular(BASE/'neorome-btc-brackets-v1/comparison.html')
                    if path.stat().st_size>500000:raise ValueError('comparison too large')
                    body=path.read_bytes();mime='text/html; charset=utf-8'
            except (OSError,ValueError):
                self.send_error(503,'Bracket comparison not yet available');return
        elif self.path in {'/btc-gap5','/api/btc-gap5'}:
            import html
            views=[]
            for label,folder in [('Original 5m','neorome-btc-cross5-paper'),('Gap <=5: 5m','neorome-btc-gap5-fast-paper'),('Original 30m','neorome-btc-cross30-paper'),('Gap <=5: 30m','neorome-btc-gap5-slow-paper')]:
                try:
                    value=read_json(BASE/folder/'feed-status.json');snap=value.get('snapshot',{});ledger=snap.get('ledger_snapshot',{})
                    views.append({'label':label,'as_of':value.get('finished_at',value.get('as_of')),'strategy_id':snap.get('strategy_id'),'cash_cents':ledger.get('available_cash_cents'),'open_cost_cents':ledger.get('open_position_cost_cents'),'reserved_cents':ledger.get('reserved_cash_cents'),'realized_pnl_cents':ledger.get('realized_pnl_cents'),'active':bool(snap.get('active_position')),'status':value.get('status'),'reason':value.get('reason')})
                except (OSError,ValueError,TypeError):
                    views.append({'label':label,'status':'unavailable'})
            projection={'as_of':datetime.now(UTC).isoformat(),'mode':'internal-paper','max_gap_points':5,'exit':'official settlement only','allocation_bps':2000,'allocation_policy_version':'btc-cash20-v1','new_cash_cents_each':50000,'independent_starts':True,'books':views}
            if self.path=='/api/btc-gap5':
                body=json.dumps(projection,allow_nan=False).encode();mime='application/json'
            else:
                def money(n):return '—' if n is None else f'${n/100:,.2f}'
                rows=''
                for view in views:
                    values=[view['label'],money(view.get('cash_cents')),money(view.get('open_cost_cents')),money(view.get('reserved_cents')),money(view.get('realized_pnl_cents')),view.get('status',''),view.get('reason') or '',view.get('as_of') or '—']
                    rows+='<tr>'+''.join('<td>'+html.escape(str(v))+'</td>' for v in values)+'</tr>'
                body=('<!doctype html><meta charset="utf-8"><meta http-equiv="refresh" content="15"><title>BTC gap-five paper comparison</title><style>body{font:15px system-ui;padding:25px;color:#18243b}table{border-collapse:collapse}td,th{padding:9px;text-align:right;border-bottom:1px solid #ddd}td:first-child,th:first-child{text-align:left}</style><h1>BTC gap-five hold-to-settlement test</h1><p>Filtered books initially $500 each; new trades use at most 20% of available cash including modeled costs. Favorable K/D gap is at most 5 points. Both retain the original fresh-cross and extreme-zone rules.</p><p>All four books hold to official settlement. Accounts have separate starting times and balances; P&amp;L here is since each book began. Compare historical replay and future entry outcomes before attributing differences to the filter.</p><table><tr><th>Book</th><th>Cash</th><th>Open cost</th><th>Reserved</th><th>Realized P&amp;L</th><th>Status</th><th>Reason</th><th>Updated UTC</th></tr>'+rows+'</table><p><a href="/btc-brackets">Existing one-second TP/SL comparison</a></p>').encode();mime='text/html; charset=utf-8'
        elif self.path in {'/','/index.html'}:
            body=Path(__file__).with_name('index.html').read_bytes();mime='text/html; charset=utf-8'
        else:
            self.send_error(404);return
        self.send_response(200)
        self.send_header('Content-Type',mime);self.send_header('Content-Length',str(len(body)))
        self.send_header('Cache-Control','no-store');self.send_header('X-Content-Type-Options','nosniff')
        self.send_header('Content-Security-Policy',"default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'")
        self.end_headers();self.wfile.write(body)

    def log_message(self,*args):
        pass


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--once',action='store_true');args=parser.parse_args()
    if args.once:print(json.dumps(snapshot(),separators=(',',':')))
    else:ThreadingHTTPServer(('127.0.0.1',8766),Handler).serve_forever()
