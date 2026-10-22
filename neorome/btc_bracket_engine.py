"""Deterministic, local-only paper accounting for independent BTC brackets."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_CEILING
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import threading
from typing import Any


class BracketError(ValueError):
    """Invalid or conflicting paper-accounting input."""


TIMEFRAMES = ("5m", "30m")
ARMS = ((None, None), (10, 10), (10, 20), (20, 20), (20, 10),
        (30, 10), (30, 30), (40, 10), (40, 40), (50, 20), (50, 50))
CONFIG = {"version": 1, "initial_cash_cents": 50000,
          "timeframes": list(TIMEFRAMES),
          "brackets": [[tp, sl] for tp, sl in ARMS],
          "fee": "ceil(0.07*N*p*(1-p)*multiplier*100)",
          "entry_buffer_cents": 1, "exit_buffer_cents": 1}
ALLOCATION_BPS = 2_000
ALLOCATION_POLICY_VERSION = "btc-cash20-v1"


def _finite(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BracketError(f"{name} must be a finite number")
    result = float(value)
    if not math.isfinite(result):
        raise BracketError(f"{name} must be a finite number")
    return result


def _hash(value: Any, name: str) -> str:
    if (not isinstance(value, str) or len(value) != 64
            or any(c not in "0123456789abcdefABCDEF" for c in value)):
        raise BracketError(f"{name} must be a SHA-256 digest")
    return value.lower()


def _json(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise BracketError("value must be finite JSON data") from exc


def _fee(quantity: int, premium_cents: Decimal, multiplier: Decimal) -> int:
    p = premium_cents / Decimal(100)
    fee = Decimal("0.07") * quantity * p * (1 - p) * multiplier * 100
    return int(fee.to_integral_value(rounding=ROUND_CEILING))


def _book(value: Any, name: str) -> list[dict[str, int]]:
    if not isinstance(value, list):
        raise BracketError(f"{name} must be an array")
    result = []
    for level in value:
        if not isinstance(level, dict) or set(level) != {"price_cents", "quantity"}:
            raise BracketError(f"{name} levels require price_cents and quantity")
        price, quantity = level["price_cents"], level["quantity"]
        if (isinstance(price, bool) or not isinstance(price, int) or not 0 <= price <= 100
                or isinstance(quantity, bool) or not isinstance(quantity, int) or quantity < 0):
            raise BracketError(f"{name} contains an out-of-range level")
        result.append({"price_cents": price, "quantity": quantity})
    # Normalize depth traversal regardless of the collector's serialization order.
    result.sort(key=lambda level: level["price_cents"], reverse="bids" in name)
    return result


def _vwap(levels: list[dict[str, int]], quantity: int) -> Decimal | None:
    remaining, total = quantity, 0
    for level in levels:
        take = min(remaining, level["quantity"])
        total += take * level["price_cents"]
        remaining -= take
        if not remaining:
            return Decimal(total) / quantity
    return None


class Engine:
    """SQLite-backed paper ledger; all public mutations are single transactions."""

    def __init__(self, path: str | os.PathLike[str], *, started_at: float):
        self.started_at = _finite(started_at, "started_at")
        self.path = Path(path)
        if self.path.is_symlink():
            raise BracketError("state database symlink refused")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.db = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self._schema()
        with self._tx():
            row = self.db.execute("SELECT value FROM meta WHERE key='config'").fetchone()
            if row and json.loads(row[0]) != CONFIG:
                raise BracketError("frozen engine configuration mismatch")
            old_start = self.db.execute("SELECT value FROM meta WHERE key='started_at'").fetchone()
            if old_start and float(old_start[0]) != self.started_at:
                raise BracketError("started_at mismatch on resume")
            if not row:
                self.db.execute("INSERT INTO meta VALUES('config',?)", (_json(CONFIG),))
                self.db.execute("INSERT INTO meta VALUES('started_at',?)", (repr(self.started_at),))
                for timeframe in TIMEFRAMES:
                    for tp, sl in ARMS:
                        arm_id = self._arm_id(timeframe, tp, sl)
                        self.db.execute("INSERT INTO arms VALUES(?,?,?,?,?,?,?,0,0,0,0,0,NULL,NULL,NULL,NULL)",
                                        (arm_id, timeframe, tp, sl, 50000, 50000, 0))

    @staticmethod
    def _arm_id(timeframe: str, tp: int | None, sl: int | None) -> str:
        return f"{timeframe}-hold" if tp is None else f"{timeframe}-tp{tp}-sl{sl}"

    def _schema(self) -> None:
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS arms(
          arm_id TEXT PRIMARY KEY,timeframe TEXT,tp INTEGER,sl INTEGER,
          initial_cash INTEGER NOT NULL,available_cash INTEGER NOT NULL,open_cost INTEGER NOT NULL,
          realized INTEGER NOT NULL,trades INTEGER NOT NULL,wins INTEGER NOT NULL,losses INTEGER NOT NULL,
          reserved INTEGER NOT NULL,position TEXT,latest_mark INTEGER,mark_time REAL,mark_basis TEXT);
        CREATE TABLE IF NOT EXISTS cohorts(
          cohort_id TEXT PRIMARY KEY,timeframe TEXT NOT NULL,ticker TEXT NOT NULL,side TEXT NOT NULL,
          observed REAL NOT NULL,signal REAL NOT NULL,close_time REAL NOT NULL,multiplier TEXT NOT NULL,
          source_hash TEXT NOT NULL,payload TEXT NOT NULL,state TEXT NOT NULL,first_quote_time REAL,
          first_quote_hash TEXT,first_quote_payload TEXT,settlement_hash TEXT);
        CREATE TABLE IF NOT EXISTS quotes(
          ticker TEXT PRIMARY KEY,received REAL NOT NULL,source_hash TEXT NOT NULL,payload TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS events(
          event_id INTEGER PRIMARY KEY AUTOINCREMENT,at REAL NOT NULL,kind TEXT NOT NULL,
          ticker TEXT,cohort_id TEXT,arm_id TEXT,source_hash TEXT,payload TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS settlements(
          ticker TEXT PRIMARY KEY,result TEXT NOT NULL,observed REAL NOT NULL,source_hash TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS controls(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        """)
        columns = {r["name"] for r in self.db.execute("PRAGMA table_info(arms)")}
        if "mark_basis" not in columns:
            self.db.execute("ALTER TABLE arms ADD COLUMN mark_basis TEXT")

    class _Transaction:
        def __init__(self, engine: "Engine"): self.engine = engine
        def __enter__(self): self.engine._lock.acquire(); self.engine.db.execute("BEGIN IMMEDIATE"); return self.engine.db
        def __exit__(self, typ, value, tb):
            try: self.engine.db.execute("ROLLBACK" if typ else "COMMIT")
            finally: self.engine._lock.release()
    def _tx(self): return self._Transaction(self)

    def _event(self, at: float, kind: str, ticker: str | None = None,
               cohort_id: str | None = None, arm_id: str | None = None,
               source_hash: str | None = None, **payload: Any) -> None:
        self.db.execute("INSERT INTO events(at,kind,ticker,cohort_id,arm_id,source_hash,payload) VALUES(?,?,?,?,?,?,?)",
                        (at, kind, ticker, cohort_id, arm_id, source_hash, _json(payload)))

    def register_cohort(self, cohort: dict) -> None:
        required = {"cohort_id", "timeframe", "ticker", "side", "observed_at", "signal_time",
                    "close_time", "fee_multiplier", "source_hash"}
        if not isinstance(cohort, dict) or set(cohort) != required:
            raise BracketError("cohort fields do not match protocol")
        cid, tf, ticker, side = (cohort[k] for k in ("cohort_id", "timeframe", "ticker", "side"))
        if not isinstance(cid, str) or not cid or len(cid) > 256: raise BracketError("invalid cohort_id")
        if tf not in TIMEFRAMES: raise BracketError("invalid timeframe")
        if not isinstance(ticker, str) or not ticker or len(ticker) > 256: raise BracketError("invalid ticker")
        if side not in ("yes", "no"): raise BracketError("side must be yes or no")
        observed = _finite(cohort["observed_at"], "observed_at")
        signal = _finite(cohort["signal_time"], "signal_time")
        close = _finite(cohort["close_time"], "close_time")
        if observed < self.started_at or observed < signal or observed > signal + 90 or observed >= close or signal >= close:
            raise BracketError("cohort is outside its prospective window")
        try: multiplier = Decimal(cohort["fee_multiplier"])
        except (InvalidOperation, TypeError): raise BracketError("fee_multiplier must be a decimal string") from None
        if (not isinstance(cohort["fee_multiplier"], str) or not multiplier.is_finite()
                or multiplier <= 0 or multiplier > Decimal("1000000")):
            raise BracketError("fee_multiplier must be a positive decimal string no greater than 1000000")
        source_hash = _hash(cohort["source_hash"], "source_hash")
        body = {**cohort, "observed_at": observed, "signal_time": signal,
                "close_time": close, "fee_multiplier": str(multiplier), "source_hash": source_hash}
        payload = _json(body)
        with self._tx():
            old = self.db.execute("SELECT payload FROM cohorts WHERE cohort_id=?", (cid,)).fetchone()
            if old:
                if old[0] != payload: raise BracketError("cohort_id payload conflict")
                return
            bound = self.db.execute("SELECT close_time FROM cohorts WHERE ticker=? LIMIT 1", (ticker,)).fetchone()
            if bound and bound[0] != close:
                raise BracketError("ticker close_time conflicts with registered source")
            blocked = self.db.execute("SELECT value FROM controls WHERE key='entry_blocker'").fetchone()
            state = "blocked" if blocked and blocked[0] else "waiting"
            self.db.execute("INSERT INTO cohorts(cohort_id,timeframe,ticker,side,observed,signal,close_time,multiplier,source_hash,payload,state) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                            (cid, tf, ticker, side, observed, signal, close, str(multiplier), source_hash, payload, state))
            self._event(observed, "cohort", ticker, cid, source_hash=source_hash, timeframe=tf, side=side)
            if state == "blocked": self._skip(self.db.execute("SELECT * FROM cohorts WHERE cohort_id=?", (cid,)).fetchone(), observed, "entry_blocked", source_hash)

    def set_entry_blocker(self, reason: str | None, now: float) -> None:
        now = _finite(now, "now")
        if reason is not None and (not isinstance(reason, str) or not reason.strip() or len(reason) > 256):
            raise BracketError("blocker must be a short reason or None")
        with self._tx():
            if reason is None:
                self.db.execute("DELETE FROM controls WHERE key='entry_blocker'")
                return
            self.db.execute("INSERT INTO controls VALUES('entry_blocker',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (reason.strip(),))
            for c in self.db.execute("SELECT * FROM cohorts WHERE state='waiting'").fetchall():
                self._skip(c, now, "entry_blocked:" + reason.strip(), c["source_hash"])

    def expire(self, now: float) -> None:
        now = _finite(now, "now")
        with self._tx():
            for c in self.db.execute("SELECT * FROM cohorts WHERE state='waiting'").fetchall():
                if now < min(c["close_time"], c["signal"] + 90):
                    continue
                self._skip(c, now, "entry_window_expired", c["source_hash"])

    def _skip(self, c: sqlite3.Row, at: float, reason: str, source_hash: str) -> None:
        self.db.execute("UPDATE cohorts SET state='skipped' WHERE cohort_id=?", (c["cohort_id"],))
        for arm in self.db.execute("SELECT * FROM arms WHERE timeframe=?", (c["timeframe"],)):
            self._event(at, "skip", c["ticker"], c["cohort_id"], arm["arm_id"], source_hash,
                        reason=reason)

    def _normalized_quote(self, quote: dict) -> tuple[float, str, dict]:
        if not isinstance(quote, dict) or set(quote) != {"received_at", "source_hash", "yes_bids", "yes_asks", "no_bids", "no_asks"}:
            raise BracketError("quote fields do not match protocol")
        received = _finite(quote["received_at"], "received_at")
        source_hash = _hash(quote["source_hash"], "source_hash")
        normalized = {k: _book(quote[k], k) for k in ("yes_bids", "yes_asks", "no_bids", "no_asks")}
        return received, source_hash, normalized

    def on_quote(self, ticker: str, quote: dict) -> dict:
        if not isinstance(ticker, str) or not ticker: raise BracketError("ticker is required")
        received, source_hash, book = self._normalized_quote(quote)
        with self._tx():
            last = self.db.execute("SELECT received,source_hash FROM quotes WHERE ticker=?", (ticker,)).fetchone()
            if last and received <= last["received"]:
                raise BracketError("quote is duplicate or out of order")
            self.db.execute("INSERT INTO quotes VALUES(?,?,?,?) ON CONFLICT(ticker) DO UPDATE SET received=excluded.received,source_hash=excluded.source_hash,payload=excluded.payload",
                            (ticker, received, source_hash, _json(book)))
            for c in self.db.execute("SELECT * FROM cohorts WHERE ticker=? AND state='waiting' ORDER BY observed,cohort_id", (ticker,)).fetchall():
                if received >= min(c["signal"] + 90, c["close_time"]):
                    self._skip(c, received, "entry_window_expired", source_hash); continue
                if received <= c["observed"]: continue
                if c["first_quote_time"] is None:
                    self.db.execute("UPDATE cohorts SET first_quote_time=?,first_quote_hash=?,first_quote_payload=? WHERE cohort_id=?",
                                    (received, source_hash, _json(book), c["cohort_id"]))
                    continue
                if source_hash == c["first_quote_hash"] or received < c["first_quote_time"] + 2:
                    continue
                blocker = self.db.execute("SELECT value FROM controls WHERE key='entry_blocker'").fetchone()
                if blocker and blocker[0]:
                    self._skip(c, received, "entry_blocked:" + blocker[0], source_hash)
                else:
                    self._try_entries(c, received, source_hash, book, json.loads(c["first_quote_payload"]))
            self._mark_and_manage(ticker, received, source_hash, book)
            return {"ticker": ticker, "received_at": received, "source_hash": source_hash,
                    "pending_tickers": self.tickers()}

    def _try_entries(self, c: sqlite3.Row, at: float, source_hash: str, book: dict,
                     first_book: dict) -> None:
        ask = [x for x in book[f"{c['side']}_asks"] if 0 < x["price_cents"] <= 60]
        first_ask = [x for x in first_book[f"{c['side']}_asks"] if 0 < x["price_cents"] <= 60]
        depth = min(sum(x["quantity"] for x in ask), sum(x["quantity"] for x in first_ask))
        if not depth:
            self._skip(c, at, "insufficient_entry_depth", source_hash); return
        for arm in self.db.execute("SELECT * FROM arms WHERE timeframe=?", (c["timeframe"],)).fetchall():
            if arm["position"] is not None:
                self._event(at, "skip", c["ticker"], c["cohort_id"], arm["arm_id"], source_hash, reason="position_already_open")
                continue
            available_before = arm["available_cash"]
            budget = available_before * ALLOCATION_BPS // 10_000
            best = None
            multiplier = Decimal(c["multiplier"])
            # Sorted ask ladders have nondecreasing total all-in cost. Binary
            # search avoids an entry batch delaying the shared one-second feed.
            def cost_at(levels, quantity):
                avg = _vwap(levels, quantity) + 1
                unit = int(avg.to_integral_value(rounding=ROUND_CEILING))
                fee = _fee(quantity, avg, multiplier)
                return unit, fee, quantity * unit + fee
            low, high = 1, min(depth, budget)
            while low <= high:
                quantity = (low + high) // 2
                premium, fee, cost = cost_at(ask, quantity)
                _, _, first_cost = cost_at(first_ask, quantity)
                if max(cost, first_cost) <= budget:
                    best = (quantity, premium, fee, cost)
                    low = quantity + 1
                else:
                    high = quantity - 1
            if best is None:
                self._event(at, "skip", c["ticker"], c["cohort_id"], arm["arm_id"], source_hash, reason="insufficient_funds_or_depth")
                continue
            quantity, premium, fee, cost = best
            pos = {"cohort_id": c["cohort_id"], "ticker": c["ticker"], "side": c["side"],
                   "quantity": quantity, "entry_premium_cents": premium, "entry_fee_cents": fee,
                   "allocation_bps": ALLOCATION_BPS, "allocation_policy_version": ALLOCATION_POLICY_VERSION,
                   "allocation_budget_cents": budget, "available_cash_before_cents": available_before,
                   "fee_multiplier": c["multiplier"],
                   "close_time": c["close_time"], "last_exit_source_hash": None,
                   "cost_cents": cost, "remaining_basis_cents": cost,
                   "accrued_net_proceeds_cents": 0, "trigger": None}
            self.db.execute("UPDATE arms SET available_cash=available_cash-?,open_cost=?,reserved=0,position=? WHERE arm_id=?",
                            (cost, cost, _json(pos), arm["arm_id"]))
            self._event(at, "entry", c["ticker"], c["cohort_id"], arm["arm_id"], source_hash,
                        quantity=quantity, premium_cents=premium, fee_cents=fee, cost_cents=cost,
                        allocation_bps=ALLOCATION_BPS, allocation_policy_version=ALLOCATION_POLICY_VERSION,
                        allocation_budget_cents=budget, available_cash_before_cents=available_before)
        self.db.execute("UPDATE cohorts SET state='entered' WHERE cohort_id=?", (c["cohort_id"],))

    def _mark_and_manage(self, ticker: str, at: float, source_hash: str, book: dict) -> None:
        for arm in self.db.execute("SELECT * FROM arms WHERE position IS NOT NULL").fetchall():
            pos = json.loads(arm["position"])
            if pos["ticker"] != ticker: continue
            bids = book[f"{pos['side']}_bids"]
            mark = self._net_liquidation_value(bids, pos, pos["quantity"])
            basis = "net_liquidation_cents_including_accrued_proceeds_after_buffer_and_exit_fee" if mark is not None else None
            self.db.execute("UPDATE arms SET latest_mark=?,mark_time=?,mark_basis=? WHERE arm_id=?",
                            (mark, at if mark is not None else None, basis, arm["arm_id"]))
            trigger = pos["trigger"]
            raw_bid_vwap = _vwap(bids, pos["quantity"])
            before_close = at < pos.get("close_time", float("inf"))
            if before_close and trigger is None and arm["tp"] is not None and raw_bid_vwap is not None:
                entry = pos["entry_premium_cents"]
                reason = "take_profit" if raw_bid_vwap >= entry * (100 + arm["tp"]) / 100 else None
                if raw_bid_vwap <= entry * (100 - arm["sl"]) / 100: reason = "stop_loss"
                if reason:
                    pos["trigger"] = {"at": at, "source_hash": source_hash, "reason": reason}
                    self.db.execute("UPDATE arms SET position=? WHERE arm_id=?", (_json(pos), arm["arm_id"]))
                    self._event(at, "trigger", ticker, pos["cohort_id"], arm["arm_id"], source_hash, reason=reason)
                    continue
            elif (before_close and trigger and at >= trigger["at"] + 1
                  and source_hash != trigger["source_hash"]
                  and source_hash != pos.get("last_exit_source_hash")):
                self._exit(arm, pos, at, source_hash, bids)

    @staticmethod
    def _net_liquidation_value(bids: list[dict[str, int]], pos: dict, quantity: int) -> int | None:
        if quantity <= 0:
            return pos["accrued_net_proceeds_cents"]
        if sum(x["quantity"] for x in bids) < quantity:
            return None
        remaining, gross = quantity, 0
        for level in bids:
            take = min(remaining, level["quantity"])
            gross += max(0, level["price_cents"] - 1) * take
            remaining -= take
            if remaining == 0: break
        fee = _fee(quantity, Decimal(gross) / quantity, Decimal(pos["fee_multiplier"]))
        return pos["accrued_net_proceeds_cents"] + max(0, gross - fee)

    def _exit(self, arm: sqlite3.Row, pos: dict, at: float, source_hash: str,
              bids: list[dict[str, int]]) -> None:
        depth = sum(x["quantity"] for x in bids)
        quantity = min(depth, pos["quantity"])
        if quantity <= 0: return
        # Fill each visible level and apply one adverse cent per contract.
        remaining, proceeds_gross = quantity, 0
        for level in bids:
            take = min(remaining, level["quantity"])
            proceeds_gross += max(0, level["price_cents"] - 1) * take
            remaining -= take
            if remaining == 0: break
        fee = _fee(quantity, Decimal(proceeds_gross) / quantity, Decimal(pos["fee_multiplier"]))
        proceeds = max(0, proceeds_gross - fee)
        basis = (pos["remaining_basis_cents"] * quantity + pos["quantity"] - 1) // pos["quantity"]
        pos["remaining_basis_cents"] -= basis
        pos["accrued_net_proceeds_cents"] += proceeds
        pos["quantity"] -= quantity
        pos["last_exit_source_hash"] = source_hash
        self._event(at, "exit_fill", pos["ticker"], pos["cohort_id"], arm["arm_id"], source_hash,
                    quantity=quantity, proceeds_cents=proceeds, fee_cents=fee, basis_cents=basis)
        if pos["quantity"] == 0:
            cash = arm["available_cash"] + pos["accrued_net_proceeds_cents"]
            delta = pos["accrued_net_proceeds_cents"] - pos["cost_cents"]
            wins = arm["wins"] + (delta > 0)
            losses = arm["losses"] + (delta <= 0)
            self.db.execute("UPDATE arms SET available_cash=?,open_cost=0,reserved=0,position=NULL,latest_mark=NULL,mark_time=NULL,mark_basis=NULL,realized=realized+?,trades=trades+1,wins=?,losses=? WHERE arm_id=?",
                            (cash, delta, wins, losses, arm["arm_id"]))
        else:
            self.db.execute("UPDATE arms SET position=?,latest_mark=NULL,mark_time=NULL,mark_basis=NULL WHERE arm_id=?",
                            (_json(pos), arm["arm_id"]))

    def on_settlement(self, ticker: str, result: str, observed_at: float, source_hash: str) -> None:
        if not isinstance(ticker, str) or not ticker or result not in ("yes", "no"):
            raise BracketError("invalid settlement identity or result")
        at, source_hash = _finite(observed_at, "observed_at"), _hash(source_hash, "source_hash")
        with self._tx():
            old = self.db.execute("SELECT * FROM settlements WHERE ticker=?", (ticker,)).fetchone()
            if old:
                if old["result"] == result: return
                raise BracketError("settlement conflict")
            cohort = self.db.execute("SELECT MAX(close_time) AS close_time FROM cohorts WHERE ticker=?", (ticker,)).fetchone()
            if cohort is None or cohort["close_time"] is None or at < cohort["close_time"]:
                raise BracketError("settlement precedes registered contract close")
            self.db.execute("INSERT INTO settlements VALUES(?,?,?,?)", (ticker, result, at, source_hash))
            for pending in self.db.execute("SELECT * FROM cohorts WHERE ticker=? AND state='waiting'", (ticker,)).fetchall():
                self._skip(pending, at, "settled_without_entry", source_hash)
            for arm in self.db.execute("SELECT * FROM arms WHERE position IS NOT NULL").fetchall():
                pos = json.loads(arm["position"])
                if pos["ticker"] != ticker: continue
                payout = pos["quantity"] * 100 if pos["side"] == result else 0
                delta = pos["accrued_net_proceeds_cents"] + payout - pos["cost_cents"]
                cash = arm["available_cash"] + pos["accrued_net_proceeds_cents"] + payout
                wins, losses = arm["wins"] + (delta > 0), arm["losses"] + (delta <= 0)
                self.db.execute("UPDATE arms SET available_cash=?,open_cost=0,reserved=0,position=NULL,latest_mark=NULL,mark_time=NULL,mark_basis=NULL,realized=realized+?,trades=trades+1,wins=?,losses=? WHERE arm_id=?",
                                (cash, delta, wins, losses, arm["arm_id"]))
                self._event(at, "settlement", ticker, pos["cohort_id"], arm["arm_id"], source_hash,
                            result=result, quantity=pos["quantity"], payout_cents=payout, pnl_cents=delta)
            self.db.execute("UPDATE cohorts SET settlement_hash=? WHERE ticker=?", (source_hash, ticker))

    def tickers(self) -> list[str]:
        rows = self.db.execute("SELECT DISTINCT ticker FROM cohorts WHERE state='waiting' UNION SELECT DISTINCT json_extract(position,'$.ticker') FROM arms WHERE position IS NOT NULL ORDER BY 1").fetchall()
        return [r[0] for r in rows if r[0] is not None]

    def status(self, now: float) -> dict:
        now = _finite(now, "now")
        arms = []
        for row in self.db.execute("SELECT * FROM arms ORDER BY timeframe,CASE WHEN tp IS NULL THEN -1 ELSE tp END,sl"):
            pos = None if row["position"] is None else json.loads(row["position"])
            mark_age = None if row["mark_time"] is None else now - row["mark_time"]
            arms.append({"arm_id": row["arm_id"], "timeframe": row["timeframe"], "tp": row["tp"], "sl": row["sl"],
                         "initial_cash_cents": row["initial_cash"], "available_cash_cents": row["available_cash"],
                         "open_cost_cents": row["open_cost"], "reserved_cash_cents": row["reserved"],
                         "realized_pnl_cents": row["realized"], "position": pos,
                         "trades": row["trades"], "wins": row["wins"], "losses": row["losses"],
                         "latest_mark_cents": row["latest_mark"], "latest_mark_basis": row["mark_basis"],
                         "mark_received_at": row["mark_time"], "mark_age_seconds": mark_age,
                         "mark_fresh": row["latest_mark"] is not None and mark_age is not None and 0 <= mark_age <= 1})
        counts = {state: self.db.execute("SELECT COUNT(*) FROM cohorts WHERE state=?", (state,)).fetchone()[0]
                  for state in ("waiting", "entered", "skipped")}
        return {"config": json.loads(_json(CONFIG)), "allocation_bps": ALLOCATION_BPS,
                "allocation_policy_version": ALLOCATION_POLICY_VERSION,
                "started_at": self.started_at, "as_of": now,
                "valuation": {"cost_basis_label": "available cash plus full open entry cost; accrued partial exit proceeds remain in position until close",
                              "mark_basis": "full-quantity net liquidation value after one-cent adverse buffer and modeled exit fee, including accrued partial exit proceeds",
                              "mark_freshness_window_seconds": 1},
                "arms": arms, "counts": counts,
                "exposures": [a for a in arms if a["position"] is not None]}

    def close(self) -> None:
        self.db.close()
