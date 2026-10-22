"""Durable internal paper-trading ledger.

This module is deliberately an internal simulation boundary.  It has no broker,
network, credentials, or market-data dependencies.  A proposal reserves its
full funded cost (including fees), a simulated fill turns that reservation into
an open position, and a close/settlement realizes proceeds and P&L.

The public API is centered on the immutable dataclasses :class:`Proposal`,
:class:`Reservation`, :class:`Fill`, :class:`Position`, and :class:`CloseResult`::

    ledger = PaperLedger("paper.sqlite3")
    reservation = ledger.reserve(proposal)
    fill = ledger.simulate_fill(reservation.reservation_id)
    result = ledger.close(fill.position_id, proceeds_cents=..., close_id="...")

All money values are integer cents.  ``snapshot()`` remains the legacy
cost-basis view.  Ledgers created with ``enforce_market_risk=True`` require a
fresh, complete ``record_marks`` snapshot before each new reservation.  Fills
of already funded reservations and closes remain available when marks expire;
both invalidate mark coverage before any subsequent reservation.
"""

from __future__ import annotations

import contextlib
import datetime as _dt
import hashlib
import json
import os
import re
import sqlite3
import threading
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping

UTC = _dt.timezone.utc
DESKS = ("prediction", "equities", "options", "crypto")
PROVENANCE = ("synthetic", "observed")


class LedgerError(Exception):
    """Base class for expected ledger rejections."""


class ValidationError(LedgerError, ValueError):
    """An input is malformed or outside the paper-ledger contract."""


class IdempotencyConflict(LedgerError):
    """An existing idempotency identifier was reused with other data."""


class InsufficientCash(LedgerError):
    """A desk cannot fund a new reservation without borrowing."""


class RiskLimitExceeded(LedgerError):
    """A per-idea, aggregate, or exposure-group limit would be exceeded."""


class RiskStopActive(LedgerError):
    """The shared risk stop blocks new reservations."""


class InvalidState(LedgerError):
    """A lifecycle operation is not valid for the current state."""


@dataclass(frozen=True)
class RiskLimits:
    """Conservative cost-based limits, in cents."""

    aggregate_cap_cents: int = 8_000
    group_cap_cents: int = 3_000
    per_idea_cents: Mapping[str, int] = field(
        default_factory=lambda: {
            "prediction": 500,
            "equities": 1_500,
            "options": 1_500,
            "crypto": 500,
        }
    )

    def __post_init__(self) -> None:
        _positive_int(self.aggregate_cap_cents, "aggregate_cap_cents")
        _positive_int(self.group_cap_cents, "group_cap_cents")
        for desk in DESKS:
            _positive_int(self.per_idea_cents.get(desk, 0), f"per_idea_cents[{desk}]")


@dataclass(frozen=True)
class MarketRiskPolicy:
    """Per-ledger paper-only automatic loss limits, expressed in basis points."""

    daily_loss_bps: int = 100
    max_drawdown_bps: int = 500

    def __post_init__(self) -> None:
        for name in ("daily_loss_bps", "max_drawdown_bps"):
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= 10_000:
                raise ValidationError(f"{name} must be an integer from 1 to 10000")


@dataclass(frozen=True)
class Proposal:
    """A fully specified, provenance-bearing paper order proposal."""

    proposal_id: str
    desk: str
    strategy: str
    strategy_version: str
    source_hash: str
    source_timestamp: _dt.datetime | str
    instrument: str
    exposure_group: str
    idea_id: str
    expiry_at: _dt.datetime | str
    quantity: int
    unit_cost_cents: int
    fee_cents: int = 0
    provenance: str = "synthetic"
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Reservation:
    reservation_id: str
    proposal_id: str
    desk: str
    instrument: str
    exposure_group: str
    idea_id: str
    quantity: int
    reserved_cents: int
    fee_cents: int
    state: str
    created_at: str
    expiry_at: str


@dataclass(frozen=True)
class Fill:
    fill_id: str
    reservation_id: str
    position_id: str
    proposal_id: str
    desk: str
    instrument: str
    exposure_group: str
    idea_id: str
    quantity: int
    total_cost_cents: int
    fee_cents: int
    filled_at: str
    provenance: str
    execution_mode: str = "internal_simulation"


@dataclass(frozen=True)
class Position:
    position_id: str
    proposal_id: str
    fill_id: str
    desk: str
    instrument: str
    exposure_group: str
    idea_id: str
    quantity: int
    basis_cents: int
    entry_fee_cents: int
    opened_at: str
    state: str
    provenance: str


@dataclass(frozen=True)
class CloseResult:
    close_id: str
    position_id: str
    proceeds_cents: int
    fee_cents: int
    realized_pnl_cents: int
    closed_at: str
    provenance: str
    execution_mode: str = "internal_simulation"


@dataclass(frozen=True)
class Balance:
    desk: str
    initial_cash_cents: int
    available_cash_cents: int
    reserved_cash_cents: int
    open_position_cost_cents: int


@dataclass(frozen=True)
class NavSnapshot:
    as_of: str
    paper_nav_cents: int
    available_cash_cents: int
    reserved_cash_cents: int
    open_position_cost_cents: int
    realized_pnl_cents: int
    nav_basis: str = "cost"
    market_nav_available: bool = False
    unrealized_pnl_cents: int | None = None
    drawdown_cents: int | None = None


@dataclass(frozen=True)
class MarketMark:
    """Total net liquidation value of one position after modeled exit fees."""

    position_id: str
    liquidation_value_cents: int
    source_timestamp: _dt.datetime | str
    source_hash: str
    provenance: str


@dataclass(frozen=True)
class MarketNavSnapshot:
    as_of: str
    batch_id: str
    market_nav_cents: int | None
    available_cash_cents: int
    reserved_cash_cents: int
    marked_liquidation_value_cents: int
    high_water_cents: int
    daily_anchor_cents: int | None
    daily_loss_bps: int | None
    drawdown_bps: int
    coverage_current: bool
    entry_allowed: bool
    automatic_risk_latched: bool
    block_reason: str | None


@dataclass(frozen=True)
class AuditRecord:
    event_id: int
    event_type: str
    entity_id: str
    idempotency_key: str | None
    payload: Mapping[str, Any]
    occurred_at: str


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0 or value > 2**63-1:
        raise ValidationError(f"{name} must be a positive integer")
    return value


def _nonnegative_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0 or value > 2**63-1:
        raise ValidationError(f"{name} must be a non-negative integer")
    return value


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{name} must be a non-empty string")
    return value.strip()


def _utc(value: _dt.datetime | str, name: str) -> _dt.datetime:
    if isinstance(value, str):
        try:
            parsed = _dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValidationError(f"{name} must be an ISO-8601 timestamp") from exc
    elif isinstance(value, _dt.datetime):
        parsed = value
    else:
        raise ValidationError(f"{name} must be an aware UTC timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValidationError(f"{name} must include a UTC offset")
    return parsed.astimezone(UTC)


def _stamp(value: _dt.datetime | str) -> str:
    return _utc(value, "timestamp").isoformat().replace("+00:00", "Z")


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


class PaperLedger:
    """SQLite-backed, transactional internal simulation ledger.

    ``path`` may be a filesystem path or ``":memory:"``.  A file ledger uses a
    short-lived SQLite connection per transaction, which makes separate ledger
    instances and concurrent threads observe the same durable state.  The
    default balances are $100/$300/$300/$100 for prediction/equities/options/
    crypto respectively.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        balances: Mapping[str, int] | None = None,
        limits: RiskLimits | None = None,
        market_risk_policy: MarketRiskPolicy | None = None,
        max_source_age_seconds: int = 300,
        enforce_market_risk: bool = False,
        clock: Callable[[], _dt.datetime] | None = None,
    ) -> None:
        self._ready = False
        self.path = str(path)
        self.limits = limits or RiskLimits()
        if market_risk_policy is not None and not isinstance(market_risk_policy, MarketRiskPolicy):
            raise ValidationError("market_risk_policy must be a MarketRiskPolicy")
        self.market_risk_policy = market_risk_policy or MarketRiskPolicy()
        _positive_int(max_source_age_seconds, "max_source_age_seconds")
        self.max_source_age_seconds = max_source_age_seconds
        if not isinstance(enforce_market_risk, bool):
            raise ValidationError("enforce_market_risk must be a bool")
        self.enforce_market_risk = enforce_market_risk
        self._clock = clock or (lambda: _dt.datetime.now(UTC))
        self._memory = self.path == ":memory:"
        self._lock = threading.RLock()
        self._memory_conn: sqlite3.Connection | None = None
        balances_supplied = balances is not None
        initial = balances if balances is not None else {
            "prediction": 10_000,
            "equities": 30_000,
            "options": 30_000,
            "crypto": 10_000,
        }
        self._validate_balances(initial)
        if not self._memory:
            parent = Path(self.path).expanduser().parent
            parent.mkdir(parents=True, exist_ok=True)
            if Path(self.path).is_symlink():
                raise ValidationError("ledger symlinks are refused")
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                os.close(fd)
            except FileExistsError:
                pass
        else:
            self._memory_conn = self._connect()
        with self._transaction() as conn:
            known_tables = {"accounts", "settings", "proposals", "reservations", "fills", "positions", "closes", "audit_events"}
            existing_tables = {
                r[0]
                for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
            }
            initialize_market = not existing_tables
            if existing_tables:
                if not known_tables.issubset(existing_tables):
                    raise ValidationError("existing SQLite file is incomplete or not a paper ledger")
                marker = conn.execute("SELECT value FROM settings WHERE key='ledger_marker'").fetchone()
                if marker is None or marker[0] != 'neorome-paper-ledger-v1':
                    raise ValidationError("invalid ledger marker")
                stored = conn.execute("SELECT value FROM settings WHERE key='ledger_config'").fetchone()
                if stored is None:
                    raise ValidationError("missing ledger configuration")
                saved = json.loads(stored[0])
                if "enforce_market_risk" not in saved:
                    initialize_market = True
                    saved["enforce_market_risk"] = False
                if "market_risk_policy" not in saved:
                    default_policy = MarketRiskPolicy()
                    if market_risk_policy is not None and market_risk_policy != default_policy:
                        raise ValidationError("legacy ledger must be opened with default market policy before rearm")
                    saved["market_risk_policy"] = self._risk_policy_payload(default_policy)
                    self._audit(conn, "market_risk_policy_migration", "ledger_config", "default-v1",
                                {"policy": saved["market_risk_policy"], "cash_changed": False,
                                 "historical_marks_changed": False}, _stamp(self._clock()))
                else:
                    persisted_policy = self._risk_policy_from_payload(saved["market_risk_policy"])
                    if market_risk_policy is not None and market_risk_policy != persisted_policy:
                        raise ValidationError("market risk policy differs from existing configuration; use audited rearm")
                    self.market_risk_policy = persisted_policy
                conn.execute("UPDATE settings SET value=? WHERE key='ledger_config'", (_canonical(saved),))
                config = self._config_payload(initial)
                if balances_supplied and saved.get('balances') != config['balances']:
                    raise ValidationError("ledger balances do not match existing configuration")
                if (saved.get('limits') != config['limits']
                        or saved.get('max_source_age_seconds') != config['max_source_age_seconds']
                        or saved.get('enforce_market_risk') != config['enforce_market_risk']):
                    raise ValidationError("ledger risk configuration does not match existing database")
            else:
                self._create_schema(conn)
                config = self._config_payload(initial)
                conn.execute("INSERT INTO settings VALUES ('ledger_marker','neorome-paper-ledger-v1')")
                conn.execute("INSERT INTO settings VALUES ('ledger_config',?)", (_canonical(config),))
                conn.execute("INSERT INTO settings VALUES ('risk_stop','0')")
                for desk, cents in initial.items():
                    conn.execute("INSERT INTO accounts VALUES (?,?,?,0)", (desk,cents,cents))
            if initialize_market:
                self._create_market_schema(conn)
                initial_nav = conn.execute('SELECT SUM(initial_cash_cents) FROM accounts').fetchone()[0]
                generation = conn.execute('SELECT (SELECT COUNT(*) FROM fills)+(SELECT COUNT(*) FROM closes)').fetchone()[0]
                conn.execute("INSERT OR IGNORE INTO settings VALUES ('position_generation',?)", (str(generation),))
                conn.execute("INSERT OR IGNORE INTO settings VALUES ('market_high_water',?)", (str(initial_nav),))
                conn.execute("INSERT OR IGNORE INTO settings VALUES ('automatic_risk_latched','0')")
                conn.execute("INSERT OR IGNORE INTO settings VALUES ('market_first_session_date','')")
            elif not {'mark_batches', 'market_marks'}.issubset(existing_tables):
                raise InvalidState('market schema is missing; refusing to recreate it')
            self._check_integrity(conn)
        self._ready = True
        if not self._memory:
            try:
                os.chmod(self.path, 0o600)
            except OSError:
                pass

    def dispose(self) -> None:
        """Close the persistent in-memory connection, if one is in use."""
        if self._memory_conn is not None:
            with self._lock:
                self._memory_conn.close()
                self._memory_conn = None

    def _validate_balances(self, balances: Mapping[str, int]) -> None:
        if set(balances) != set(DESKS):
            raise ValidationError(f"balances must contain exactly {DESKS}")
        for desk, cents in balances.items():
            # A dedicated desk ledger can deliberately disable the other
            # desks.  The regular shared ledger keeps its positive defaults.
            _nonnegative_int(cents, f"balances[{desk}]")

    def _config_payload(self, balances: Mapping[str, int]) -> dict[str, Any]:
        return {
            "balances": {desk: balances[desk] for desk in DESKS},
            "limits": {
                "aggregate_cap_cents": self.limits.aggregate_cap_cents,
                "group_cap_cents": self.limits.group_cap_cents,
                "per_idea_cents": {desk: self.limits.per_idea_cents[desk] for desk in DESKS},
            },
            "max_source_age_seconds": self.max_source_age_seconds,
            "enforce_market_risk": self.enforce_market_risk,
            "market_risk_policy": self._risk_policy_payload(self.market_risk_policy),
        }

    @staticmethod
    def _risk_policy_payload(policy: MarketRiskPolicy) -> dict[str, int]:
        return {"daily_loss_bps": policy.daily_loss_bps,
                "max_drawdown_bps": policy.max_drawdown_bps}

    @staticmethod
    def _risk_policy_from_payload(value: Any) -> MarketRiskPolicy:
        try:
            if not isinstance(value, dict) or set(value) != {"daily_loss_bps", "max_drawdown_bps"}:
                raise ValueError
            return MarketRiskPolicy(value["daily_loss_bps"], value["max_drawdown_bps"])
        except (TypeError, ValueError, ValidationError):
            raise ValidationError("persisted market risk policy is invalid") from None

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=10, isolation_level=None, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 10000")
        if not self._memory:
            conn.execute("PRAGMA journal_mode = WAL")
        return conn

    def _check_integrity(self, conn: sqlite3.Connection) -> None:
        """Reconcile durable rows before reads/retries/mutations, and before commit.

        Detects inconsistent accounting; not a tamper-proof or signed audit log.
        Intended for small offline rehearsals, not a high-throughput exchange.
        """
        def fail(message):
            raise InvalidState("ledger reconciliation failed: " + message)

        cfg = conn.execute("SELECT value FROM settings WHERE key='ledger_config'").fetchone()
        stop = conn.execute("SELECT value FROM settings WHERE key='risk_stop'").fetchone()
        generation = conn.execute("SELECT value FROM settings WHERE key='position_generation'").fetchone()
        high_water = conn.execute("SELECT value FROM settings WHERE key='market_high_water'").fetchone()
        auto_stop = conn.execute("SELECT value FROM settings WHERE key='automatic_risk_latched'").fetchone()
        first_session = conn.execute("SELECT value FROM settings WHERE key='market_first_session_date'").fetchone()
        if (cfg is None or stop is None or stop[0] not in ('0', '1') or generation is None
                or not generation[0].isdigit() or high_water is None or not high_water[0].isdigit()
                or auto_stop is None or auto_stop[0] not in ('0', '1') or first_session is None):
            fail('missing or invalid settings')
        config = json.loads(cfg[0])
        if self._config_payload(config['balances']) != config:
            fail('configuration changed')
        accounts = conn.execute('SELECT * FROM accounts').fetchall()
        if {r['desk'] for r in accounts} != set(DESKS):
            fail('missing or extra desk')
        if conn.execute('PRAGMA foreign_key_check').fetchone() is not None:
            fail('foreign key mismatch')
        expected_generation = conn.execute('SELECT (SELECT COUNT(*) FROM fills)+(SELECT COUNT(*) FROM closes)').fetchone()[0]
        if int(generation[0]) != expected_generation:
            fail('position generation mismatch')
        first_batch = conn.execute('SELECT as_of FROM mark_batches ORDER BY julianday(as_of),rowid LIMIT 1').fetchone()
        expected_first = _utc(first_batch[0], 'as_of').date().isoformat() if first_batch else ''
        if first_session[0] != expected_first:
            fail('first market session mismatch')
        for account in accounts:
            desk = account['desk']
            pending = conn.execute("SELECT COALESCE(SUM(reserved_cents),0) FROM reservations WHERE desk=? AND state='reserved'", (desk,)).fetchone()[0]
            spent = conn.execute('SELECT COALESCE(SUM(total_cost_cents),0) FROM fills WHERE desk=?', (desk,)).fetchone()[0]
            returned = conn.execute('SELECT COALESCE(SUM(c.proceeds_cents-c.fee_cents),0) FROM closes c JOIN positions p USING(position_id) WHERE p.desk=?', (desk,)).fetchone()[0]
            if (account['initial_cash_cents'] != config['balances'][desk]
                    or account['reserved_cash_cents'] != pending
                    or account['available_cash_cents'] != config['balances'][desk]-pending-spent+returned):
                fail('cash or reservation mismatch for '+desk)
        reservations = conn.execute('SELECT * FROM reservations').fetchall()
        if conn.execute('SELECT COUNT(*) FROM proposals').fetchone()[0] != len(reservations):
            fail('orphan proposal')
        for row in reservations:
            payload = json.loads(conn.execute('SELECT payload FROM proposals WHERE proposal_id=?', (row['proposal_id'],)).fetchone()[0])
            for key in ('desk','instrument','exposure_group','idea_id','quantity','fee_cents','expiry_at'):
                if row[key] != payload[key]:
                    fail('reservation differs from proposal')
            if row['reserved_cents'] != payload['quantity']*payload['unit_cost_cents']+payload['fee_cents']:
                fail('reservation amount differs from proposal')
            fill = conn.execute('SELECT * FROM fills WHERE reservation_id=?', (row['reservation_id'],)).fetchone()
            if (row['state']=='filled') != (fill is not None):
                fail('fill state mismatch')
            if fill is not None:
                for key in ('proposal_id','desk','instrument','exposure_group','idea_id','quantity'):
                    if fill[key] != row[key]:
                        fail('fill identity mismatch')
                if fill['total_cost_cents'] > row['reserved_cents'] or fill['provenance'] != payload['provenance']:
                    fail('fill cost or provenance mismatch')
            reserve_audit = conn.execute("SELECT payload FROM audit_events WHERE event_type='reserve' AND entity_id=?", (row['reservation_id'],)).fetchall()
            if len(reserve_audit) != 1 or json.loads(reserve_audit[0][0]) != {'proposal_id':row['proposal_id'],'reserved_cents':row['reserved_cents'],'payload':payload}:
                fail('missing or inconsistent reservation audit')
            cancels = conn.execute("SELECT COUNT(*) FROM audit_events WHERE event_type='cancel' AND entity_id=?", (row['reservation_id'],)).fetchone()[0]
            if cancels != int(row['state']=='cancelled'):
                fail('cancellation audit mismatch')
        positions = conn.execute('SELECT * FROM positions').fetchall()
        if len(positions) != conn.execute('SELECT COUNT(*) FROM fills').fetchone()[0]:
            fail('orphan fill')
        for pos in positions:
            fill = conn.execute('SELECT * FROM fills WHERE fill_id=?', (pos['fill_id'],)).fetchone()
            for key in ('position_id','proposal_id','desk','instrument','exposure_group','idea_id','quantity','provenance'):
                if pos[key] != fill[key]:
                    fail('position differs from fill')
            if pos['basis_cents'] != fill['total_cost_cents'] or pos['entry_fee_cents'] != fill['fee_cents']:
                fail('position basis mismatch')
            close = conn.execute('SELECT * FROM closes WHERE position_id=?', (pos['position_id'],)).fetchone()
            if (pos['state']=='closed') != (close is not None):
                fail('close state mismatch')
            if close is not None:
                if close['realized_pnl_cents'] != close['proceeds_cents']-close['fee_cents']-pos['basis_cents']:
                    fail('realized PnL mismatch')
                count = conn.execute("SELECT COUNT(*) FROM audit_events WHERE event_type='close' AND entity_id=?", (close['close_id'],)).fetchone()[0]
                if count != 1:
                    fail('close audit missing')
            count = conn.execute("SELECT COUNT(*) FROM audit_events WHERE event_type='fill' AND entity_id=?", (fill['fill_id'],)).fetchone()[0]
            if count != 1:
                fail('fill audit missing')
        for batch in conn.execute('SELECT * FROM mark_batches').fetchall():
            marks = conn.execute('SELECT * FROM market_marks WHERE batch_id=?', (batch['batch_id'],)).fetchall()
            if len(marks) != batch['mark_count'] or sum(r['liquidation_value_cents'] for r in marks) != batch['marked_value_cents']:
                fail('market-mark batch mismatch')
            try:
                _utc(batch['as_of'], 'as_of')
                for mark in marks:
                    _utc(mark['source_timestamp'], 'source_timestamp')
                    if re.fullmatch(r'[0-9a-f]{64}', mark['source_hash']) is None:
                        fail('invalid persisted market-mark hash')
            except ValidationError:
                fail('invalid persisted market-mark timestamp')
            payload = json.loads(batch['payload'])
            expected = {'as_of': batch['as_of'], 'marks': [
                {'position_id': r['position_id'], 'liquidation_value_cents': r['liquidation_value_cents'],
                 'source_timestamp': r['source_timestamp'], 'source_hash': r['source_hash'],
                 'provenance': r['provenance']} for r in marks]}
            expected['marks'].sort(key=lambda r: r['position_id'])
            if payload != expected:
                fail('market-mark payload mismatch')
            audits = conn.execute("SELECT payload FROM audit_events WHERE event_type='market_marks' AND entity_id=?", (batch['batch_id'],)).fetchall()
            audit = json.loads(audits[0][0]) if len(audits) == 1 else {}
            if (audit.get('payload') != expected or audit.get('market_nav_cents') != batch['market_nav_cents']
                    or audit.get('daily_anchor_cents') != batch['daily_anchor_cents']
                    or audit.get('high_water_cents') != batch['high_water_cents']
                    or int(bool(audit.get('automatic_latched'))) != batch['automatic_latched']):
                fail('market-mark audit missing or inconsistent')
            anchor = batch['daily_anchor_cents']
            expected_daily = None if anchor is None else max(0, (anchor-batch['market_nav_cents'])*10_000//anchor)
            expected_drawdown = (0 if batch['high_water_cents'] == 0 else
                                 max(0, (batch['high_water_cents']-batch['market_nav_cents'])*10_000//batch['high_water_cents']))
            if batch['daily_loss_bps'] != expected_daily or batch['drawdown_bps'] != expected_drawdown:
                fail('market risk metric mismatch')
        batches = conn.execute('SELECT high_water_cents FROM mark_batches').fetchall()
        initial_nav = sum(r['initial_cash_cents'] for r in accounts)
        earliest = conn.execute("SELECT as_of FROM mark_batches ORDER BY julianday(as_of),rowid LIMIT 1").fetchone()
        expected_first_date = '' if earliest is None else _utc(earliest['as_of'], 'as_of').date().isoformat()
        if first_session[0] != expected_first_date:
            fail('market first-session date mismatch')
        if int(high_water[0]) != max([initial_nav] + [r['high_water_cents'] for r in batches]):
            fail('market high-water mismatch')
        for event in conn.execute("SELECT payload FROM audit_events WHERE event_type='cash_only_day_anchor'").fetchall():
            try:
                payload = json.loads(event['payload'])
                cutoff = _utc(payload['cutoff'], 'cash anchor cutoff')
                if cutoff.time() != _dt.time.min or self._cash_only_prior_day_anchor(conn, cutoff) != payload:
                    fail('cash-only prior-day anchor audit mismatch')
            except (KeyError, TypeError, ValueError, ValidationError):
                fail('invalid cash-only prior-day anchor audit')
        rearm_rows = conn.execute(
            "SELECT event_id,payload FROM audit_events WHERE event_type='automatic_risk_rearm' ORDER BY event_id"
        ).fetchall()
        rearm_by_id = {}
        for event in rearm_rows:
            try:
                payload = json.loads(event['payload'])
                previous = self._risk_policy_from_payload(payload['previous_policy'])
                updated = self._risk_policy_from_payload(payload['new_policy'])
                if (payload.get('cash_changed') is not False
                        or payload.get('historical_marks_changed') is not False
                        or type(payload.get('previous_latched')) is not bool
                        or not isinstance(payload.get('reason'), str) or not payload['reason'].strip()
                        or not isinstance(payload.get('rearm_id'), str) or not payload['rearm_id']):
                    fail('invalid automatic-risk rearm audit')
                if 'allow_open_positions' in payload:
                    ids = payload.get('open_position_ids')
                    if (type(payload['allow_open_positions']) is not bool
                            or payload.get('positions_changed') is not False
                            or not isinstance(ids, list) or any(not isinstance(i, str) or not i for i in ids)
                            or len(ids) != len(set(ids))):
                        fail('invalid in-position policy migration audit')
                    if ids and not payload['allow_open_positions']:
                        fail('open-position migration lacks explicit opt-in')
                    if payload['allow_open_positions'] and (updated.daily_loss_bps < previous.daily_loss_bps
                                                           or updated.max_drawdown_bps < previous.max_drawdown_bps):
                        fail('in-position migration lowered loss limits')
                rearm_by_id[event['event_id']] = (previous, updated)
            except (KeyError, TypeError, ValueError, ValidationError):
                fail('invalid automatic-risk rearm audit')
        if rearm_rows and self._risk_policy_from_payload(config['market_risk_policy']) != rearm_by_id[rearm_rows[-1]['event_id']][1]:
            fail('active market risk policy differs from latest rearm')
        policy = (rearm_by_id[rearm_rows[0]['event_id']][0] if rearm_rows
                  else self._risk_policy_from_payload(config['market_risk_policy']))
        active_latch = False
        events = conn.execute(
            "SELECT event_id,event_type,entity_id,payload FROM audit_events "
            "WHERE event_type IN ('automatic_risk_rearm','market_marks') ORDER BY event_id"
        ).fetchall()
        active_latch_seen = False
        risk_batches = {row["batch_id"]: row for row in conn.execute("SELECT * FROM mark_batches").fetchall()}
        for event in events:
            if event['event_type'] == 'automatic_risk_rearm':
                previous, updated = rearm_by_id[event['event_id']]
                payload = json.loads(event['payload'])
                if previous != policy:
                    fail('automatic-risk rearm policy chain is discontinuous')
                if payload['previous_latched'] != active_latch:
                    fail('automatic-risk rearm latch state is discontinuous')
                policy = updated
                active_latch = False
                active_latch_seen = False
                continue
            batch = risk_batches.get(event["entity_id"])
            if batch is None:
                fail('market mark audit has no batch')
            daily_hit = (batch['daily_loss_bps'] is not None
                         and batch['daily_loss_bps'] >= policy.daily_loss_bps)
            drawdown_hit = batch['drawdown_bps'] >= policy.max_drawdown_bps
            expected_latch = active_latch or daily_hit or drawdown_hit
            if bool(batch['automatic_latched']) != expected_latch:
                fail('automatic risk batch latch differs from active policy')
            active_latch = expected_latch
            active_latch_seen = True
        expected_active_latch = active_latch if active_latch_seen else False
        if int(auto_stop[0]) != int(expected_active_latch):
            fail('automatic risk latch differs from active epoch')

    @contextlib.contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            conn = self._memory_conn or self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                if self._ready:
                    self._check_integrity(conn)
                yield conn
                if self._ready:
                    self._check_integrity(conn)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                if conn is not self._memory_conn:
                    conn.close()

    @staticmethod
    def _create_schema(conn: sqlite3.Connection) -> None:
        schema = """
            CREATE TABLE IF NOT EXISTS accounts (
                desk TEXT PRIMARY KEY,
                initial_cash_cents INTEGER NOT NULL CHECK (initial_cash_cents >= 0),
                available_cash_cents INTEGER NOT NULL CHECK (available_cash_cents >= 0),
                reserved_cash_cents INTEGER NOT NULL CHECK (reserved_cash_cents >= 0)
            );
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS proposals (
                proposal_id TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                source_timestamp TEXT NOT NULL,
                expiry_at TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS reservations (
                reservation_id TEXT PRIMARY KEY,
                proposal_id TEXT NOT NULL UNIQUE REFERENCES proposals(proposal_id),
                desk TEXT NOT NULL REFERENCES accounts(desk),
                instrument TEXT NOT NULL,
                exposure_group TEXT NOT NULL,
                idea_id TEXT NOT NULL,
                quantity INTEGER NOT NULL CHECK (quantity > 0),
                reserved_cents INTEGER NOT NULL CHECK (reserved_cents > 0),
                fee_cents INTEGER NOT NULL CHECK (fee_cents >= 0),
                state TEXT NOT NULL CHECK (state IN ('reserved', 'filled', 'cancelled')),
                created_at TEXT NOT NULL,
                expiry_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS fills (
                fill_id TEXT PRIMARY KEY,
                reservation_id TEXT NOT NULL UNIQUE REFERENCES reservations(reservation_id),
                position_id TEXT NOT NULL UNIQUE,
                proposal_id TEXT NOT NULL,
                desk TEXT NOT NULL,
                instrument TEXT NOT NULL,
                exposure_group TEXT NOT NULL,
                idea_id TEXT NOT NULL,
                quantity INTEGER NOT NULL CHECK (quantity > 0),
                total_cost_cents INTEGER NOT NULL CHECK (total_cost_cents > 0),
                fee_cents INTEGER NOT NULL CHECK (fee_cents >= 0),
                filled_at TEXT NOT NULL,
                provenance TEXT NOT NULL CHECK (provenance IN ('synthetic', 'observed'))
            );
            CREATE TABLE IF NOT EXISTS positions (
                position_id TEXT PRIMARY KEY,
                proposal_id TEXT NOT NULL,
                fill_id TEXT NOT NULL UNIQUE REFERENCES fills(fill_id),
                desk TEXT NOT NULL REFERENCES accounts(desk),
                instrument TEXT NOT NULL,
                exposure_group TEXT NOT NULL,
                idea_id TEXT NOT NULL,
                quantity INTEGER NOT NULL CHECK (quantity > 0),
                basis_cents INTEGER NOT NULL CHECK (basis_cents > 0),
                entry_fee_cents INTEGER NOT NULL CHECK (entry_fee_cents >= 0),
                opened_at TEXT NOT NULL,
                state TEXT NOT NULL CHECK (state IN ('open', 'closed')),
                provenance TEXT NOT NULL CHECK (provenance IN ('synthetic', 'observed'))
            );
            CREATE TABLE IF NOT EXISTS closes (
                close_id TEXT PRIMARY KEY,
                position_id TEXT NOT NULL UNIQUE REFERENCES positions(position_id),
                proceeds_cents INTEGER NOT NULL CHECK (proceeds_cents >= 0),
                fee_cents INTEGER NOT NULL CHECK (fee_cents >= 0),
                realized_pnl_cents INTEGER NOT NULL,
                closed_at TEXT NOT NULL,
                provenance TEXT NOT NULL CHECK (provenance IN ('synthetic', 'observed'))
            );
            CREATE TABLE IF NOT EXISTS audit_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                idempotency_key TEXT,
                payload TEXT NOT NULL,
                occurred_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_reservations_state ON reservations(state);
            CREATE INDEX IF NOT EXISTS idx_positions_state ON positions(state);
            CREATE INDEX IF NOT EXISTS idx_audit_entity ON audit_events(entity_id, event_id);
            """
        for statement in schema.split(";"):
            if statement.strip():
                conn.execute(statement)

    @staticmethod
    def _create_market_schema(conn: sqlite3.Connection) -> None:
        schema = """
            CREATE TABLE IF NOT EXISTS mark_batches (
                batch_id TEXT PRIMARY KEY,
                as_of TEXT NOT NULL,
                position_generation INTEGER NOT NULL CHECK(position_generation >= 0),
                mark_count INTEGER NOT NULL CHECK(mark_count >= 0),
                marked_value_cents INTEGER NOT NULL CHECK(marked_value_cents >= 0),
                market_nav_cents INTEGER NOT NULL CHECK(market_nav_cents >= 0),
                daily_anchor_cents INTEGER,
                daily_loss_bps INTEGER,
                drawdown_bps INTEGER NOT NULL,
                high_water_cents INTEGER NOT NULL CHECK(high_water_cents >= 0),
                automatic_latched INTEGER NOT NULL CHECK(automatic_latched IN (0,1)),
                block_reason TEXT,
                payload TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS market_marks (
                batch_id TEXT NOT NULL REFERENCES mark_batches(batch_id),
                position_id TEXT NOT NULL REFERENCES positions(position_id),
                liquidation_value_cents INTEGER NOT NULL CHECK(liquidation_value_cents >= 0),
                source_timestamp TEXT NOT NULL,
                source_hash TEXT NOT NULL,
                provenance TEXT NOT NULL CHECK(provenance IN ('synthetic','observed')),
                PRIMARY KEY(batch_id, position_id)
            );
            CREATE INDEX IF NOT EXISTS idx_mark_batches_time ON mark_batches(as_of);
        """
        for statement in schema.split(';'):
            if statement.strip():
                conn.execute(statement)

    def _now(self, value: _dt.datetime | str | None = None) -> tuple[_dt.datetime, str]:
        dt = _utc(value, "now") if value is not None else _utc(self._clock(), "clock")
        return dt, _stamp(dt)

    @staticmethod
    def _proposal_payload(proposal: Proposal) -> dict[str, Any]:
        source_hash = _text(proposal.source_hash, "source_hash")
        if re.fullmatch(r"[0-9a-fA-F]{64}", source_hash) is None:
            raise ValidationError("source_hash must be a 64-character hexadecimal digest")
        return {
            "proposal_id": _text(proposal.proposal_id, "proposal_id"),
            "desk": _text(proposal.desk, "desk"),
            "strategy": _text(proposal.strategy, "strategy"),
            "strategy_version": _text(proposal.strategy_version, "strategy_version"),
            "source_hash": source_hash.lower(),
            "source_timestamp": _stamp(proposal.source_timestamp),
            "instrument": _text(proposal.instrument, "instrument"),
            "exposure_group": _text(proposal.exposure_group, "exposure_group"),
            "idea_id": _text(proposal.idea_id, "idea_id"),
            "expiry_at": _stamp(proposal.expiry_at),
            "quantity": _positive_int(proposal.quantity, "quantity"),
            "unit_cost_cents": _positive_int(proposal.unit_cost_cents, "unit_cost_cents"),
            "fee_cents": _nonnegative_int(proposal.fee_cents, "fee_cents"),
            "provenance": _provenance(proposal.provenance),
            "metadata": json.loads(_canonical(dict(proposal.metadata))),
        }

    def _validate_proposal(self, proposal: Proposal, now: _dt.datetime) -> tuple[dict[str, Any], int]:
        if not isinstance(proposal, Proposal):
            raise ValidationError("proposal must be a Proposal")
        payload = self._proposal_payload(proposal)
        if payload["desk"] not in DESKS:
            raise ValidationError(f"desk must be one of {DESKS}")
        source = _utc(payload["source_timestamp"], "source_timestamp")
        expiry = _utc(payload["expiry_at"], "expiry_at")
        if source > now:
            raise ValidationError("source_timestamp cannot be in the future")
        age = (now - source).total_seconds()
        if age > self.max_source_age_seconds:
            raise ValidationError("source_timestamp is stale")
        if expiry <= now:
            raise ValidationError("expiry_at must be in the future at reserve time")
        total = payload["quantity"] * payload["unit_cost_cents"] + payload["fee_cents"]
        if total <= 0:
            raise ValidationError("full funded cost must be positive")
        if total > self.limits.per_idea_cents[payload["desk"]]:
            raise RiskLimitExceeded("proposal exceeds per-idea paper cap")
        return payload, total

    def reserve(
        self, proposal: Proposal, *, now: _dt.datetime | str | None = None
    ) -> Reservation:
        """Atomically reserve a proposal's full cost, or return its replay."""
        current, occurred_at = self._now(now)
        payload = self._proposal_payload(proposal)
        proposal_id = payload["proposal_id"]
        with self._transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM reservations WHERE proposal_id = ?", (proposal_id,)
            ).fetchone()
            serialized = _canonical(payload)
            if existing is not None:
                saved = conn.execute(
                    "SELECT payload FROM proposals WHERE proposal_id = ?", (proposal_id,)
                ).fetchone()
                if saved["payload"] != serialized:
                    raise IdempotencyConflict(f"proposal_id {proposal_id!r} has different data")
                return self._reservation_row(existing)
            payload, total = self._validate_proposal(proposal, current)
            if conn.execute("SELECT value FROM settings WHERE key = 'risk_stop'").fetchone()[0] == "1":
                raise RiskStopActive("risk stop is active")
            if self.enforce_market_risk:
                try:
                    market = self._market_snapshot(conn, current)
                except InvalidState as exc:
                    raise RiskStopActive(str(exc)) from exc
                if not market.entry_allowed:
                    raise RiskStopActive(market.block_reason or "market risk gate is active")
            active = self._active_cost(conn)
            if active + total > self.limits.aggregate_cap_cents:
                raise RiskLimitExceeded("aggregate paper capital-at-risk cap exceeded")
            group_active = self._active_group_cost(conn, payload["exposure_group"])
            if group_active + total > self.limits.group_cap_cents:
                raise RiskLimitExceeded("exposure-group paper cap exceeded")
            idea_active = self._active_idea_cost(conn, payload["idea_id"])
            idea_desks = {r[0] for r in conn.execute(
                "SELECT desk FROM reservations WHERE idea_id=? AND state='reserved' "
                "UNION SELECT desk FROM positions WHERE idea_id=? AND state='open'",
                (payload['idea_id'], payload['idea_id']))}
            idea_desks.add(payload['desk'])
            idea_cap = min(self.limits.per_idea_cents[d] for d in idea_desks)
            if idea_active + total > idea_cap:
                raise RiskLimitExceeded("economic-idea paper cap exceeded")
            account = conn.execute(
                "SELECT available_cash_cents FROM accounts WHERE desk = ?", (payload["desk"],)
            ).fetchone()
            if account is None or account["available_cash_cents"] < total:
                raise InsufficientCash(f"desk {payload['desk']} cannot fund reservation")
            reservation_id = f"res-{uuid.uuid4().hex}"
            conn.execute(
                """INSERT INTO proposals
                   (proposal_id, payload, source_timestamp, expiry_at, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (proposal_id, serialized, payload["source_timestamp"], payload["expiry_at"], occurred_at),
            )
            conn.execute(
                """INSERT INTO reservations
                   (reservation_id, proposal_id, desk, instrument, exposure_group, idea_id,
                    quantity, reserved_cents, fee_cents, state, created_at, expiry_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'reserved', ?, ?)""",
                (
                    reservation_id,
                    proposal_id,
                    payload["desk"],
                    payload["instrument"],
                    payload["exposure_group"],
                    payload["idea_id"],
                    payload["quantity"],
                    total,
                    payload["fee_cents"],
                    occurred_at,
                    payload["expiry_at"],
                ),
            )
            conn.execute(
                """UPDATE accounts
                   SET available_cash_cents = available_cash_cents - ?,
                       reserved_cash_cents = reserved_cash_cents + ?
                   WHERE desk = ?""",
                (total, total, payload["desk"]),
            )
            self._audit(
                conn,
                "reserve",
                reservation_id,
                proposal_id,
                {"proposal_id": proposal_id, "reserved_cents": total, "payload": payload},
                occurred_at,
            )
            return self._reservation_row(
                conn.execute("SELECT * FROM reservations WHERE reservation_id = ?", (reservation_id,)).fetchone()
            )

    reserve_proposal = reserve

    def simulate_fill(
        self,
        reservation_id: str,
        *,
        fill_id: str | None = None,
        quantity: int | None = None,
        unit_cost_cents: int | None = None,
        fee_cents: int | None = None,
        provenance: str = "synthetic",
        filled_at: _dt.datetime | str | None = None,
    ) -> Fill:
        """Turn a full reservation into one open position transactionally."""
        reservation_id = _text(reservation_id, "reservation_id")
        fill_id = _text(fill_id or f"fill-{reservation_id}", "fill_id")
        provenance = _provenance(provenance)
        _, occurred_at = self._now(filled_at)
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT * FROM reservations WHERE reservation_id = ?", (reservation_id,)
            ).fetchone()
            if row is None:
                raise ValidationError(f"unknown reservation {reservation_id!r}")
            requested_qty = row["quantity"] if quantity is None else _positive_int(quantity, "quantity")
            requested_unit = (
                (row["reserved_cents"] - row["fee_cents"]) // row["quantity"]
                if unit_cost_cents is None
                else _positive_int(unit_cost_cents, "unit_cost_cents")
            )
            requested_fee = row["fee_cents"] if fee_cents is None else _nonnegative_int(fee_cents, "fee_cents")
            actual_total = requested_qty * requested_unit + requested_fee
            signature = {
                "reservation_id": reservation_id,
                "quantity": requested_qty,
                "unit_cost_cents": requested_unit,
                "fee_cents": requested_fee,
                "provenance": provenance,
            }
            existing_fill = conn.execute("SELECT * FROM fills WHERE fill_id = ?", (fill_id,)).fetchone()
            if existing_fill is not None:
                existing_signature = {
                    "reservation_id": existing_fill["reservation_id"],
                    "quantity": existing_fill["quantity"],
                    "unit_cost_cents": (existing_fill["total_cost_cents"] - existing_fill["fee_cents"])
                    // existing_fill["quantity"],
                    "fee_cents": existing_fill["fee_cents"],
                    "provenance": existing_fill["provenance"],
                }
                if existing_signature != signature:
                    raise IdempotencyConflict(f"fill_id {fill_id!r} has different data")
                return self._fill_row(existing_fill)
            if _utc(occurred_at, "filled_at") < _utc(row["created_at"], "created_at"):
                raise ValidationError("fill cannot precede reservation")
            proposal_payload = json.loads(conn.execute("SELECT payload FROM proposals WHERE proposal_id=?", (row["proposal_id"],)).fetchone()[0])
            if provenance != proposal_payload['provenance']:
                raise ValidationError("fill price provenance differs from proposal")
            if row["state"] != "reserved":
                raise InvalidState(f"reservation {reservation_id!r} is {row['state']}")
            if _utc(occurred_at, "filled_at") < _utc(row["created_at"], "created_at"):
                raise ValidationError("filled_at cannot precede reservation creation")
            if requested_qty != row["quantity"]:
                raise ValidationError("partial fills are not supported in this bounded ledger")
            if actual_total > row["reserved_cents"]:
                raise InsufficientCash("simulated fill exceeds its full-cost reservation")
            if _utc(row["expiry_at"], "expiry_at") <= _utc(occurred_at, "filled_at"):
                raise InvalidState("reservation expired before simulated fill")
            position_id = f"pos-{fill_id}"
            conn.execute(
                """INSERT INTO fills
                   (fill_id, reservation_id, position_id, proposal_id, desk, instrument,
                    exposure_group, idea_id, quantity, total_cost_cents, fee_cents, filled_at, provenance)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    fill_id,
                    reservation_id,
                    position_id,
                    row["proposal_id"],
                    row["desk"],
                    row["instrument"],
                    row["exposure_group"],
                    row["idea_id"],
                    requested_qty,
                    actual_total,
                    requested_fee,
                    occurred_at,
                    provenance,
                ),
            )
            conn.execute(
                """INSERT INTO positions
                   (position_id, proposal_id, fill_id, desk, instrument, exposure_group, idea_id,
                    quantity, basis_cents, entry_fee_cents, opened_at, state, provenance)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?)""",
                (
                    position_id,
                    row["proposal_id"],
                    fill_id,
                    row["desk"],
                    row["instrument"],
                    row["exposure_group"],
                    row["idea_id"],
                    requested_qty,
                    actual_total,
                    requested_fee,
                    occurred_at,
                    provenance,
                ),
            )
            release = row["reserved_cents"] - actual_total
            conn.execute(
                """UPDATE reservations SET state = 'filled' WHERE reservation_id = ?""",
                (reservation_id,),
            )
            conn.execute(
                """UPDATE accounts
                   SET reserved_cash_cents = reserved_cash_cents - ?,
                       available_cash_cents = available_cash_cents + ?
                   WHERE desk = ?""",
                (row["reserved_cents"], release, row["desk"]),
            )
            self._audit(
                conn,
                "fill",
                fill_id,
                fill_id,
                {"reservation_id": reservation_id, "position_id": position_id, **signature},
                occurred_at,
            )
            self._advance_position_generation(conn)
            return self._fill_row(conn.execute("SELECT * FROM fills WHERE fill_id = ?", (fill_id,)).fetchone())

    fill = simulate_fill

    def close(
        self,
        position_id: str,
        *,
        close_id: str | None = None,
        proceeds_cents: int,
        fee_cents: int = 0,
        provenance: str = "synthetic",
        closed_at: _dt.datetime | str | None = None,
    ) -> CloseResult:
        """Close one full position and realize proceeds, fees, and P&L."""
        position_id = _text(position_id, "position_id")
        close_id = _text(close_id or f"close-{position_id}", "close_id")
        proceeds_cents = _nonnegative_int(proceeds_cents, "proceeds_cents")
        fee_cents = _nonnegative_int(fee_cents, "fee_cents")
        provenance = _provenance(provenance)
        if fee_cents > proceeds_cents:
            raise ValidationError("exit fees exceeding proceeds require an additional reserved-liability model")
        _, occurred_at = self._now(closed_at)
        signature = {
            "position_id": position_id,
            "proceeds_cents": proceeds_cents,
            "fee_cents": fee_cents,
            "provenance": provenance,
        }
        with self._transaction() as conn:
            existing = conn.execute("SELECT * FROM closes WHERE close_id = ?", (close_id,)).fetchone()
            if existing is not None:
                existing_signature = {
                    "position_id": existing["position_id"],
                    "proceeds_cents": existing["proceeds_cents"],
                    "fee_cents": existing["fee_cents"],
                    "provenance": existing["provenance"],
                }
                if existing_signature != signature:
                    raise IdempotencyConflict(f"close_id {close_id!r} has different data")
                return self._close_row(existing)
            position = conn.execute("SELECT * FROM positions WHERE position_id = ?", (position_id,)).fetchone()
            if position is None:
                raise ValidationError(f"unknown position {position_id!r}")
            if position["state"] != "open":
                raise InvalidState(f"position {position_id!r} is {position['state']}")
            if _utc(occurred_at, "closed_at") < _utc(position["opened_at"], "opened_at"):
                raise ValidationError("closed_at cannot precede fill time")
            if _utc(occurred_at, "closed_at") < _utc(position['opened_at'], "opened_at"):
                raise ValidationError("close cannot precede fill")
            if provenance != position['provenance']:
                raise ValidationError("close price provenance differs from position")
            cash = conn.execute("SELECT available_cash_cents FROM accounts WHERE desk=?", (position['desk'],)).fetchone()[0]
            if cash + proceeds_cents < fee_cents:
                raise InsufficientCash("close fees exceed available cash and proceeds")
            realized = proceeds_cents - fee_cents - position["basis_cents"]
            conn.execute(
                """INSERT INTO closes
                   (close_id, position_id, proceeds_cents, fee_cents, realized_pnl_cents,
                    closed_at, provenance)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (close_id, position_id, proceeds_cents, fee_cents, realized, occurred_at, provenance),
            )
            conn.execute("UPDATE positions SET state = 'closed' WHERE position_id = ?", (position_id,))
            conn.execute(
                """UPDATE accounts SET available_cash_cents = available_cash_cents + ?
                   WHERE desk = ?""",
                (proceeds_cents - fee_cents, position["desk"]),
            )
            self._audit(conn, "close", close_id, close_id, {**signature, "realized_pnl_cents": realized}, occurred_at)
            self._advance_position_generation(conn)
            return self._close_row(conn.execute("SELECT * FROM closes WHERE close_id = ?", (close_id,)).fetchone())

    def settle(self, position_id: str, **kwargs: Any) -> CloseResult:
        """Settlement is the same full-close accounting path with a clear name."""
        return self.close(position_id, **kwargs)

    def cancel(
        self,
        reservation_id: str,
        *,
        cancel_id: str | None = None,
        reason: str = "",
        cancelled_at: _dt.datetime | str | None = None,
    ) -> Reservation:
        """Cancel a pending reservation and release its full reserved cost."""
        reservation_id = _text(reservation_id, "reservation_id")
        cancel_id = _text(cancel_id or f"cancel-{reservation_id}", "cancel_id")
        reason = reason.strip() if isinstance(reason, str) else _text(reason, "reason")
        _, occurred_at = self._now(cancelled_at)
        with self._transaction() as conn:
            row = conn.execute("SELECT * FROM reservations WHERE reservation_id = ?", (reservation_id,)).fetchone()
            if row is None:
                raise ValidationError(f"unknown reservation {reservation_id!r}")
            prior = conn.execute("SELECT payload FROM audit_events WHERE event_type='cancel' AND idempotency_key=?", (cancel_id,)).fetchone()
            signature = {"reservation_id": reservation_id, "reason": reason}
            if prior is not None:
                if json.loads(prior[0]) != signature:
                    raise IdempotencyConflict("cancel id reused with different data")
                return self._reservation_row(row)
            if row["state"] == "cancelled":
                raise IdempotencyConflict("reservation already cancelled with another id")
            if _utc(occurred_at, "cancelled_at") < _utc(row['created_at'], "created_at"):
                raise ValidationError("cancel cannot precede reservation")
            if row["state"] != "reserved":
                raise InvalidState(f"reservation {reservation_id!r} is {row['state']}")
            conn.execute("UPDATE reservations SET state = 'cancelled' WHERE reservation_id = ?", (reservation_id,))
            conn.execute(
                """UPDATE accounts
                   SET available_cash_cents = available_cash_cents + ?,
                       reserved_cash_cents = reserved_cash_cents - ?
                   WHERE desk = ?""",
                (row["reserved_cents"], row["reserved_cents"], row["desk"]),
            )
            self._audit(
                conn,
                "cancel",
                reservation_id,
                cancel_id,
                {"reservation_id": reservation_id, "reason": reason},
                occurred_at,
            )
            return self._reservation_row(conn.execute("SELECT * FROM reservations WHERE reservation_id = ?", (reservation_id,)).fetchone())

    cancel_reservation = cancel

    def set_risk_stop(
        self, stopped: bool, *, reason: str = "", at: _dt.datetime | str | None = None
    ) -> bool:
        if not isinstance(stopped, bool):
            raise ValidationError("stopped must be a bool")
        _, occurred_at = self._now(at)
        reason = reason.strip() if isinstance(reason, str) else _text(reason, "reason")
        with self._transaction() as conn:
            conn.execute("UPDATE settings SET value = ? WHERE key = 'risk_stop'", ("1" if stopped else "0",))
            self._audit(conn, "risk_stop", "risk_stop", None, {"stopped": stopped, "reason": reason}, occurred_at)
        return stopped

    def risk_stop_active(self) -> bool:
        with self._transaction() as conn:
            manual = conn.execute("SELECT value FROM settings WHERE key = 'risk_stop'").fetchone()
            automatic = conn.execute("SELECT value FROM settings WHERE key = 'automatic_risk_latched'").fetchone()
            return ((manual is not None and manual["value"] == "1")
                    or (automatic is not None and automatic["value"] == "1"))

    def migrate_risk_limits(
        self,
        limits: RiskLimits,
        *,
        migration_id: str,
        migrated_at: _dt.datetime | str | None = None,
    ) -> bool:
        """Atomically apply a monotonic, audited risk-limit migration.

        This deliberately cannot reduce a limit or alter balances.  It exists
        for persistent paper ledgers whose configured capital authorization
        was expanded without resetting their positions, cash, or audit trail.
        Returns ``True`` when the configuration changed and ``False`` for an
        idempotent replay against the already-migrated configuration.
        """
        if not isinstance(limits, RiskLimits):
            raise ValidationError("limits must be RiskLimits")
        migration_id = _text(migration_id, "migration_id")
        _, occurred_at = self._now(migrated_at)
        old_limits = self.limits
        old_values = (
            old_limits.aggregate_cap_cents,
            old_limits.group_cap_cents,
            *(old_limits.per_idea_cents[desk] for desk in DESKS),
        )
        new_values = (
            limits.aggregate_cap_cents,
            limits.group_cap_cents,
            *(limits.per_idea_cents[desk] for desk in DESKS),
        )
        if any(new < old for old, new in zip(old_values, new_values)):
            raise ValidationError("risk-limit migration must be monotonic")
        requested_limits = {
            "aggregate_cap_cents": limits.aggregate_cap_cents,
            "group_cap_cents": limits.group_cap_cents,
            "per_idea_cents": {desk: limits.per_idea_cents[desk] for desk in DESKS},
        }
        if new_values == old_values:
            with self._transaction() as conn:
                existing = conn.execute(
                    "SELECT payload FROM audit_events WHERE event_type='risk_limit_migration' AND entity_id=?",
                    (migration_id,),
                ).fetchone()
                if existing is not None and json.loads(existing["payload"]).get("new_limits") != requested_limits:
                    raise IdempotencyConflict("risk-limit migration id has different data")
            return False

        try:
            with self._transaction() as conn:
                existing = conn.execute(
                    "SELECT payload FROM audit_events WHERE event_type='risk_limit_migration' AND entity_id=?",
                    (migration_id,),
                ).fetchone()
                payload = {
                    "migration_id": migration_id,
                    "old_limits": self._config_payload({desk: 0 for desk in DESKS})["limits"],
                    "new_limits": requested_limits,
                }
                if existing is not None:
                    if json.loads(existing["payload"]) != payload:
                        raise IdempotencyConflict("risk-limit migration id has different data")
                    return False
                stored = json.loads(conn.execute(
                    "SELECT value FROM settings WHERE key='ledger_config'"
                ).fetchone()[0])
                if stored.get("limits") != payload["old_limits"]:
                    raise IdempotencyConflict("persisted risk limits changed before migration")
                stored["limits"] = payload["new_limits"]
                conn.execute(
                    "UPDATE settings SET value=? WHERE key='ledger_config'",
                    (_canonical(stored),),
                )
                self.limits = limits
                self._audit(conn, "risk_limit_migration", migration_id, migration_id, payload, occurred_at)
            return True
        except Exception:
            self.limits = old_limits
            raise

    @staticmethod
    def _cash_only_prior_day_anchor(conn: sqlite3.Connection, cutoff: _dt.datetime) -> dict | None:
        """Derive historical NAV only when no position was open at day end.

        Pending reservations merely split cash into available/reserved balances.
        All filled positions before cutoff must have closed before cutoff, so
        initial capital plus realized net closes is exact cash NAV, not a price.
        """
        stamp = _stamp(cutoff)
        held = conn.execute(
            "SELECT 1 FROM positions p LEFT JOIN closes c USING(position_id) "
            "WHERE julianday(p.opened_at)<julianday(?) "
            "AND (c.closed_at IS NULL OR julianday(c.closed_at)>=julianday(?)) LIMIT 1",
            (stamp, stamp),
        ).fetchone()
        if held:
            return None
        initial = conn.execute("SELECT SUM(initial_cash_cents) FROM accounts").fetchone()[0]
        closes = [dict(r) for r in conn.execute(
            "SELECT close_id,position_id,closed_at,realized_pnl_cents FROM closes "
            "WHERE julianday(closed_at)<julianday(?) ORDER BY close_id", (stamp,)).fetchall()]
        return {"cutoff": stamp, "initial_nav_cents": initial,
                "market_nav_cents": initial + sum(r['realized_pnl_cents'] for r in closes),
                "close_count": len(closes), "closed_cash_evidence_sha256": hashlib.sha256(_canonical(closes).encode()).hexdigest(),
                "method": "initial_cash_plus_net_realized_closes_flat_at_boundary"}

    def rearm_automatic_risk(
        self,
        *,
        rearm_id: str,
        reason: str,
        policy: MarketRiskPolicy,
        allow_open_positions: bool = False,
        at: _dt.datetime | str | None = None,
    ) -> bool:
        """Audited paper-only risk-policy rearm/configuration for a flat ledger.

        A latched ledger is rearmed; an unlatched ledger can receive its initial
        experimental policy through the same explicit audit trail. The operation
        changes only the saved market-risk policy and active latch.
        Open positions require explicit opt-in, fresh complete liquidation marks,
        and a policy expansion. Outstanding reservations remain prohibited.
        Cash, positions, mark history, daily anchors and high-water are unchanged.
        """
        rearm_id = _text(rearm_id, "rearm_id")
        reason = _text(reason, "reason").strip()
        if not reason:
            raise ValidationError("rearm reason is required")
        if not isinstance(policy, MarketRiskPolicy):
            raise ValidationError("policy must be MarketRiskPolicy")
        if type(allow_open_positions) is not bool:
            raise ValidationError("allow_open_positions must be a bool")
        current, occurred_at = self._now(at)
        old_policy = self.market_risk_policy
        payload = {
            "rearm_id": rearm_id, "reason": reason,
            "previous_policy": self._risk_policy_payload(old_policy),
            "new_policy": self._risk_policy_payload(policy),
            "previous_latched": None,
            "allow_open_positions": allow_open_positions,
            "positions_changed": False,
            "cash_changed": False, "historical_marks_changed": False,
        }
        try:
            with self._transaction() as conn:
                prior = conn.execute(
                    "SELECT payload FROM audit_events WHERE event_type='automatic_risk_rearm' AND entity_id=?",
                    (rearm_id,),
                ).fetchone()
                if prior is not None:
                    existing = json.loads(prior['payload'])
                    if (existing.get("reason") != reason
                            or existing.get("new_policy") != self._risk_policy_payload(policy)
                            or existing.get("allow_open_positions", False) != allow_open_positions):
                        raise IdempotencyConflict("automatic-risk rearm id has different data")
                    return False
                latched = conn.execute(
                    "SELECT value FROM settings WHERE key='automatic_risk_latched'"
                ).fetchone()
                if latched is None or latched['value'] not in {'0', '1'}:
                    raise InvalidState("automatic risk latch state is invalid")
                payload["previous_latched"] = latched['value'] == '1'
                manual = conn.execute("SELECT value FROM settings WHERE key='risk_stop'").fetchone()
                if manual is None or manual['value'] != '0':
                    raise InvalidState("manual risk stop must be cleared separately")
                open_positions = conn.execute("SELECT position_id FROM positions WHERE state='open' ORDER BY position_id").fetchall()
                if open_positions and not allow_open_positions:
                    raise InvalidState("automatic-risk rearm requires a flat ledger")
                if allow_open_positions and (policy.daily_loss_bps < old_policy.daily_loss_bps
                                              or policy.max_drawdown_bps < old_policy.max_drawdown_bps):
                    raise ValidationError("in-position migration requires non-decreasing loss limits")
                payload["open_position_ids"] = [row["position_id"] for row in open_positions]
                if conn.execute("SELECT 1 FROM reservations WHERE state='reserved' LIMIT 1").fetchone():
                    raise InvalidState("automatic-risk rearm requires no reservations")
                snapshot = self._market_snapshot(conn, current)
                if not snapshot.coverage_current or snapshot.market_nav_cents is None:
                    raise InvalidState("automatic-risk rearm requires fresh complete marks")
                if snapshot.daily_anchor_cents is None:
                    raise InvalidState("automatic-risk rearm requires a daily loss anchor")
                if (snapshot.daily_loss_bps is not None
                        and snapshot.daily_loss_bps >= policy.daily_loss_bps):
                    raise InvalidState("current daily loss still exceeds requested policy")
                if snapshot.drawdown_bps >= policy.max_drawdown_bps:
                    raise InvalidState("current drawdown still exceeds requested policy")
                config_row = conn.execute(
                    "SELECT value FROM settings WHERE key='ledger_config'"
                ).fetchone()
                config = json.loads(config_row['value'])
                config['market_risk_policy'] = self._risk_policy_payload(policy)
                conn.execute("UPDATE settings SET value=? WHERE key='ledger_config'", (_canonical(config),))
                conn.execute("UPDATE settings SET value='0' WHERE key='automatic_risk_latched'")
                self.market_risk_policy = policy
                self._audit(conn, "automatic_risk_rearm", rearm_id, rearm_id,
                            {**payload, "market_nav_cents": snapshot.market_nav_cents,
                             "daily_loss_bps": snapshot.daily_loss_bps,
                             "drawdown_bps": snapshot.drawdown_bps,
                             "high_water_cents": snapshot.high_water_cents,
                             "latest_batch_id": snapshot.batch_id}, occurred_at)
            return True
        except Exception:
            self.market_risk_policy = old_policy
            raise

    def balances(self) -> tuple[Balance, ...]:
        with self._transaction() as conn:
            rows = conn.execute("SELECT * FROM accounts ORDER BY desk").fetchall()
            result = []
            for row in rows:
                cost = conn.execute(
                    "SELECT COALESCE(SUM(basis_cents), 0) FROM positions WHERE desk = ? AND state = 'open'",
                    (row["desk"],),
                ).fetchone()[0]
                result.append(
                    Balance(row["desk"], row["initial_cash_cents"], row["available_cash_cents"], row["reserved_cash_cents"], cost)
                )
            return tuple(result)

    def snapshot(self, *, as_of: _dt.datetime | str | None = None) -> NavSnapshot:
        _, stamp = self._now(as_of)
        with self._transaction() as conn:
            available = conn.execute("SELECT COALESCE(SUM(available_cash_cents), 0) FROM accounts").fetchone()[0]
            reserved = conn.execute("SELECT COALESCE(SUM(reserved_cash_cents), 0) FROM accounts").fetchone()[0]
            position_cost = conn.execute(
                "SELECT COALESCE(SUM(basis_cents), 0) FROM positions WHERE state = 'open'"
            ).fetchone()[0]
            realized = conn.execute("SELECT COALESCE(SUM(realized_pnl_cents), 0) FROM closes").fetchone()[0]
            return NavSnapshot(stamp, available + reserved + position_cost, available, reserved, position_cost, realized)

    def record_marks(
        self,
        batch_id: str,
        marks: list[MarketMark] | tuple[MarketMark, ...],
        as_of: _dt.datetime | str,
    ) -> MarketNavSnapshot:
        """Persist one atomic, complete snapshot of every currently open position."""
        batch_id = _text(batch_id, "batch_id")
        as_of_dt, stamp = self._now(as_of)
        if not isinstance(marks, (list, tuple)):
            raise ValidationError("marks must be a list or tuple of MarketMark values")
        clean: list[dict[str, Any]] = []
        for mark in marks:
            if not isinstance(mark, MarketMark):
                raise ValidationError("each mark must be a MarketMark")
            source_hash = _text(mark.source_hash, "source_hash").lower()
            if re.fullmatch(r"[0-9a-f]{64}", source_hash) is None:
                raise ValidationError("source_hash must be a 64-character hexadecimal digest")
            source = _utc(mark.source_timestamp, "source_timestamp")
            if source > as_of_dt:
                raise ValidationError("mark source_timestamp cannot be in the future")
            if (as_of_dt - source).total_seconds() > self.max_source_age_seconds:
                raise ValidationError("mark source_timestamp is stale")
            clean.append({
                "position_id": _text(mark.position_id, "position_id"),
                "liquidation_value_cents": _nonnegative_int(mark.liquidation_value_cents, "liquidation_value_cents"),
                "source_timestamp": _stamp(source),
                "source_hash": source_hash,
                "provenance": _provenance(mark.provenance),
            })
        clean.sort(key=lambda item: item["position_id"])
        if len({m["position_id"] for m in clean}) != len(clean):
            raise ValidationError("a mark batch cannot contain duplicate positions")
        payload = {"as_of": stamp, "marks": clean}
        serialized = _canonical(payload)
        with self._transaction() as conn:
            existing = conn.execute("SELECT * FROM mark_batches WHERE batch_id=?", (batch_id,)).fetchone()
            if existing is not None:
                if existing["payload"] != serialized:
                    raise IdempotencyConflict(f"batch_id {batch_id!r} has different data")
                return self._market_snapshot_from_row(conn, existing, as_of_dt)
            latest = conn.execute("SELECT as_of FROM mark_batches ORDER BY julianday(as_of) DESC,rowid DESC LIMIT 1").fetchone()
            if latest is not None and as_of_dt < _utc(latest["as_of"], "as_of"):
                raise ValidationError("mark as_of cannot precede the prior batch")
            positions = conn.execute("SELECT position_id,opened_at,provenance FROM positions WHERE state='open'").fetchall()
            by_id = {p["position_id"]: p for p in positions}
            if set(by_id) != {m["position_id"] for m in clean}:
                raise ValidationError("mark batch must cover exactly all open positions")
            for mark in clean:
                position = by_id[mark["position_id"]]
                if mark["provenance"] != position["provenance"]:
                    raise ValidationError("mark provenance differs from position")
                if as_of_dt < _utc(position["opened_at"], "opened_at"):
                    raise ValidationError("mark as_of cannot precede position open")
                if _utc(mark["source_timestamp"], "source_timestamp") < _utc(position["opened_at"], "opened_at"):
                    raise ValidationError("mark source_timestamp cannot precede position open")
            available = conn.execute("SELECT COALESCE(SUM(available_cash_cents),0) FROM accounts").fetchone()[0]
            reserved = conn.execute("SELECT COALESCE(SUM(reserved_cash_cents),0) FROM accounts").fetchone()[0]
            marked = sum(m["liquidation_value_cents"] for m in clean)
            nav = available + reserved + marked
            prior_date = as_of_dt.date() - _dt.timedelta(days=1)
            day_start = _dt.datetime.combine(prior_date, _dt.time.min, UTC)
            day_end = day_start + _dt.timedelta(days=1)
            anchor_row = conn.execute(
                """SELECT market_nav_cents FROM mark_batches
                   WHERE julianday(as_of)>=julianday(?) AND julianday(as_of)<julianday(?)
                   ORDER BY julianday(as_of) DESC,rowid DESC LIMIT 1""",
                (_stamp(day_start), _stamp(day_end)),
            ).fetchone()
            initial_nav = conn.execute("SELECT SUM(initial_cash_cents) FROM accounts").fetchone()[0]
            first_date_row = conn.execute("SELECT value FROM settings WHERE key='market_first_session_date'").fetchone()
            first_date = first_date_row[0] or as_of_dt.date().isoformat()
            if not first_date_row[0]:
                conn.execute("UPDATE settings SET value=? WHERE key='market_first_session_date'", (first_date,))
            anchor = initial_nav if as_of_dt.date().isoformat() == first_date else (anchor_row[0] if anchor_row is not None else None)
            if anchor is None:
                cash_anchor = self._cash_only_prior_day_anchor(conn, day_end)
                if cash_anchor is not None:
                    entity_id = prior_date.isoformat()
                    existing = conn.execute("SELECT payload FROM audit_events WHERE event_type='cash_only_day_anchor' AND entity_id=?", (entity_id,)).fetchone()
                    if existing is not None:
                        if json.loads(existing['payload']) != cash_anchor:
                            raise InvalidState("cash-only prior-day anchor evidence changed")
                    else:
                        self._audit(conn, "cash_only_day_anchor", entity_id, entity_id, cash_anchor, stamp)
                    anchor = cash_anchor['market_nav_cents']
            old_high = int(conn.execute("SELECT value FROM settings WHERE key='market_high_water'").fetchone()[0])
            high = max(old_high, nav)
            daily_bps = None if anchor is None else max(0, (anchor - nav) * 10_000 // anchor)
            drawdown_bps = 0 if high == 0 else max(0, (high - nav) * 10_000 // high)
            prior_latch = conn.execute("SELECT value FROM settings WHERE key='automatic_risk_latched'").fetchone()[0] == "1"
            threshold = ((anchor is not None
                          and (anchor - nav) * 10_000 >= anchor * self.market_risk_policy.daily_loss_bps)
                         or (high - nav) * 10_000 >= high * self.market_risk_policy.max_drawdown_bps)
            latched = prior_latch or threshold
            reason = "missing_prior_utc_day_anchor" if anchor is None else ("automatic_loss_latch" if latched else None)
            generation = int(conn.execute("SELECT value FROM settings WHERE key='position_generation'").fetchone()[0])
            conn.execute(
                """INSERT INTO mark_batches
                   (batch_id,as_of,position_generation,mark_count,marked_value_cents,market_nav_cents,
                    daily_anchor_cents,daily_loss_bps,drawdown_bps,high_water_cents,automatic_latched,block_reason,payload)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (batch_id, stamp, generation, len(clean), marked, nav, anchor, daily_bps,
                 drawdown_bps, high, int(latched), reason, serialized),
            )
            for mark in clean:
                conn.execute(
                    """INSERT INTO market_marks
                       (batch_id,position_id,liquidation_value_cents,source_timestamp,source_hash,provenance)
                       VALUES (?,?,?,?,?,?)""",
                    (batch_id, mark["position_id"], mark["liquidation_value_cents"],
                     mark["source_timestamp"], mark["source_hash"], mark["provenance"]),
                )
            conn.execute("UPDATE settings SET value=? WHERE key='market_high_water'", (str(high),))
            conn.execute("UPDATE settings SET value=? WHERE key='automatic_risk_latched'", ("1" if latched else "0",))
            self._audit(conn, "market_marks", batch_id, batch_id,
                        {"payload": payload, "market_nav_cents": nav, "daily_anchor_cents": anchor,
                         "high_water_cents": high, "automatic_latched": latched}, stamp)
            row = conn.execute("SELECT * FROM mark_batches WHERE batch_id=?", (batch_id,)).fetchone()
            return self._market_snapshot_from_row(conn, row, as_of_dt)

    def market_snapshot(self, as_of: _dt.datetime | str | None = None) -> MarketNavSnapshot:
        current, _ = self._now(as_of)
        with self._transaction() as conn:
            return self._market_snapshot(conn, current)

    def _market_snapshot(self, conn: sqlite3.Connection, current: _dt.datetime) -> MarketNavSnapshot:
        row = conn.execute("SELECT * FROM mark_batches ORDER BY julianday(as_of) DESC,rowid DESC LIMIT 1").fetchone()
        if row is None:
            raise InvalidState("complete market marks are missing")
        return self._market_snapshot_from_row(conn, row, current)

    def _market_snapshot_from_row(
        self, conn: sqlite3.Connection, row: sqlite3.Row, current: _dt.datetime
    ) -> MarketNavSnapshot:
        generation = int(conn.execute("SELECT value FROM settings WHERE key='position_generation'").fetchone()[0])
        sources = conn.execute("SELECT source_timestamp FROM market_marks WHERE batch_id=?", (row["batch_id"],)).fetchall()
        times = [_utc(r[0], "source_timestamp") for r in sources] or [_utc(row["as_of"], "as_of")]
        coverage_reason = None
        if int(row["position_generation"]) != generation:
            coverage_reason = "position_coverage_changed"
        elif any(t > current for t in times):
            coverage_reason = "market_marks_from_future"
        elif any((current - t).total_seconds() > self.max_source_age_seconds for t in times):
            coverage_reason = "market_marks_stale"
        elif _utc(row["as_of"], "as_of").date() != current.date():
            coverage_reason = "current_utc_day_snapshot_missing"
        manual = conn.execute("SELECT value FROM settings WHERE key='risk_stop'").fetchone()[0] == "1"
        automatic = conn.execute("SELECT value FROM settings WHERE key='automatic_risk_latched'").fetchone()[0] == "1"
        reason = coverage_reason or row["block_reason"]
        if reason is None and automatic:
            reason = "automatic_loss_latch"
        elif reason is None and manual:
            reason = "manual_risk_stop"
        available = conn.execute("SELECT COALESCE(SUM(available_cash_cents),0) FROM accounts").fetchone()[0]
        reserved = conn.execute("SELECT COALESCE(SUM(reserved_cash_cents),0) FROM accounts").fetchone()[0]
        marked = conn.execute(
            """SELECT COALESCE(SUM(m.liquidation_value_cents),0)
               FROM market_marks m JOIN positions p USING(position_id)
               WHERE m.batch_id=? AND p.state='open'""",
            (row["batch_id"],),
        ).fetchone()[0]
        coverage = coverage_reason is None
        return MarketNavSnapshot(
            row["as_of"], row["batch_id"], available + reserved + marked if coverage else None,
            available, reserved, marked,
            row["high_water_cents"], row["daily_anchor_cents"], row["daily_loss_bps"],
            row["drawdown_bps"], coverage, reason is None, automatic, reason,
        )

    def get_reservation(self, reservation_id: str) -> Reservation:
        with self._transaction() as conn:
            row = conn.execute("SELECT * FROM reservations WHERE reservation_id = ?", (_text(reservation_id, "reservation_id"),)).fetchone()
            if row is None:
                raise ValidationError(f"unknown reservation {reservation_id!r}")
            return self._reservation_row(row)

    def get_position(self, position_id: str) -> Position:
        with self._transaction() as conn:
            row = conn.execute("SELECT * FROM positions WHERE position_id = ?", (_text(position_id, "position_id"),)).fetchone()
            if row is None:
                raise ValidationError(f"unknown position {position_id!r}")
            return self._position_row(row)

    def audit(self) -> tuple[AuditRecord, ...]:
        with self._transaction() as conn:
            rows = conn.execute("SELECT * FROM audit_events ORDER BY event_id").fetchall()
            return tuple(
                AuditRecord(r["event_id"], r["event_type"], r["entity_id"], r["idempotency_key"], json.loads(r["payload"]), r["occurred_at"])
                for r in rows
            )

    def active_exposure_count(self, desk: str) -> int:
        """Return durable reserved-or-open exposure count for one desk."""
        desk = _text(desk, "desk")
        if desk not in DESKS:
            raise ValidationError(f"desk must be one of {DESKS}")
        with self._transaction() as conn:
            reserved = conn.execute("SELECT COUNT(*) FROM reservations WHERE desk=? AND state='reserved'", (desk,)).fetchone()[0]
            opened = conn.execute("SELECT COUNT(*) FROM positions WHERE desk=? AND state='open'", (desk,)).fetchone()[0]
            return int(reserved + opened)

    @staticmethod
    def _active_cost(conn: sqlite3.Connection) -> int:
        pending = conn.execute("SELECT COALESCE(SUM(reserved_cents), 0) FROM reservations WHERE state = 'reserved'").fetchone()[0]
        open_cost = conn.execute("SELECT COALESCE(SUM(basis_cents), 0) FROM positions WHERE state = 'open'").fetchone()[0]
        return pending + open_cost

    @staticmethod
    def _active_group_cost(conn: sqlite3.Connection, group: str) -> int:
        pending = conn.execute(
            "SELECT COALESCE(SUM(reserved_cents), 0) FROM reservations WHERE state = 'reserved' AND exposure_group = ?",
            (group,),
        ).fetchone()[0]
        open_cost = conn.execute(
            "SELECT COALESCE(SUM(basis_cents), 0) FROM positions WHERE state = 'open' AND exposure_group = ?",
            (group,),
        ).fetchone()[0]
        return pending + open_cost

    @staticmethod
    def _active_idea_cost(conn: sqlite3.Connection, idea_id: str) -> int:
        pending = conn.execute(
            "SELECT COALESCE(SUM(reserved_cents), 0) FROM reservations WHERE state = 'reserved' AND idea_id = ?",
            (idea_id,),
        ).fetchone()[0]
        open_cost = conn.execute(
            "SELECT COALESCE(SUM(basis_cents), 0) FROM positions WHERE state = 'open' AND idea_id = ?",
            (idea_id,),
        ).fetchone()[0]
        return pending + open_cost

    @staticmethod
    def _advance_position_generation(conn: sqlite3.Connection) -> None:
        conn.execute(
            "UPDATE settings SET value=CAST(CAST(value AS INTEGER)+1 AS TEXT) WHERE key='position_generation'"
        )

    @staticmethod
    def _audit(conn: sqlite3.Connection, event_type: str, entity_id: str, key: str | None, payload: Mapping[str, Any], at: str) -> None:
        conn.execute(
            "INSERT INTO audit_events (event_type, entity_id, idempotency_key, payload, occurred_at) VALUES (?, ?, ?, ?, ?)",
            (event_type, entity_id, key, _canonical(payload), at),
        )

    @staticmethod
    def _reservation_row(row: sqlite3.Row) -> Reservation:
        return Reservation(row["reservation_id"], row["proposal_id"], row["desk"], row["instrument"], row["exposure_group"], row["idea_id"], row["quantity"], row["reserved_cents"], row["fee_cents"], row["state"], row["created_at"], row["expiry_at"])

    @staticmethod
    def _fill_row(row: sqlite3.Row) -> Fill:
        return Fill(row["fill_id"], row["reservation_id"], row["position_id"], row["proposal_id"], row["desk"], row["instrument"], row["exposure_group"], row["idea_id"], row["quantity"], row["total_cost_cents"], row["fee_cents"], row["filled_at"], row["provenance"])

    @staticmethod
    def _position_row(row: sqlite3.Row) -> Position:
        return Position(row["position_id"], row["proposal_id"], row["fill_id"], row["desk"], row["instrument"], row["exposure_group"], row["idea_id"], row["quantity"], row["basis_cents"], row["entry_fee_cents"], row["opened_at"], row["state"], row["provenance"])

    @staticmethod
    def _close_row(row: sqlite3.Row) -> CloseResult:
        return CloseResult(row["close_id"], row["position_id"], row["proceeds_cents"], row["fee_cents"], row["realized_pnl_cents"], row["closed_at"], row["provenance"])


def _provenance(value: Any) -> str:
    value = _text(value, "provenance")
    if value not in PROVENANCE:
        raise ValidationError(f"provenance must be one of {PROVENANCE}")
    return value


__all__ = [
    "AuditRecord",
    "Balance",
    "CloseResult",
    "DESKS",
    "Fill",
    "IdempotencyConflict",
    "InsufficientCash",
    "InvalidState",
    "LedgerError",
    "MarketMark",
    "MarketNavSnapshot",
    "NavSnapshot",
    "PaperLedger",
    "Position",
    "PROVENANCE",
    "Proposal",
    "Reservation",
    "RiskLimitExceeded",
    "RiskLimits",
    "RiskStopActive",
    "ValidationError",
]
