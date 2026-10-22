"""Persistent paper engine for fee-aware, 50% profit activation/trailing exits.

This engine intentionally shares the bracket engine's ledger protocol and fill
accounting while freezing an independent two-account configuration.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from . import btc_bracket_engine as base


class TrailingEngine(base.Engine):
    """Two independent BTC paper accounts with a 50% net-profit trail."""

    ARMS = (("5m", "5m-trailing-a50-d50"), ("30m", "30m-trailing-a50-d50"))
    CONFIG = {
        "version": 1,
        "engine": "btc-trailing-paper",
        "initial_cash_cents": 50000,
        "timeframes": ["5m", "30m"],
        "arms": ["5m-trailing-a50-d50", "30m-trailing-a50-d50"],
        "allocation_bps": 2000,
        "fee": "ceil(0.07*N*p*(1-p)*multiplier*100)",
        "entry_buffer_cents": 1,
        "exit_buffer_cents": 1,
        "activation_net_profit_bps": 5000,
        "trail_from_peak_bps": 5000,
        "mark_basis": "full_quantity_net_liquidation_after_fee_and_buffer_including_accrued_proceeds",
    }

    def __init__(self, path: str, *, started_at: float):
        self.started_at = base._finite(started_at, "started_at")
        self.path = base.Path(path)
        if self.path.is_symlink():
            raise base.BracketError("state database symlink refused")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = base.threading.RLock()
        self.db = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self._schema()
        with self._tx():
            row = self.db.execute("SELECT value FROM meta WHERE key='config'").fetchone()
            if row and json.loads(row[0]) != self.CONFIG:
                raise base.BracketError("frozen trailing engine configuration mismatch")
            old_start = self.db.execute("SELECT value FROM meta WHERE key='started_at'").fetchone()
            if old_start and float(old_start[0]) != self.started_at:
                raise base.BracketError("started_at mismatch on resume")
            existing_arms = self.db.execute("SELECT COUNT(*) FROM arms").fetchone()[0]
            if not row and existing_arms:
                raise base.BracketError("refusing database containing unrecognized paper accounts")
            if not row:
                self.db.execute("INSERT INTO meta VALUES('config',?)", (base._json(self.CONFIG),))
                self.db.execute("INSERT INTO meta VALUES('started_at',?)", (repr(self.started_at),))
                for timeframe, arm_id in self.ARMS:
                    self.db.execute(
                        "INSERT INTO arms VALUES(?,?,?,?,?,?,?,0,0,0,0,0,NULL,NULL,NULL,NULL)",
                        (arm_id, timeframe, None, None, 50000, 50000, 0),
                    )
            else:
                actual = [(r["arm_id"], r["timeframe"]) for r in self.db.execute("SELECT arm_id,timeframe FROM arms ORDER BY arm_id")]
                if actual != sorted((arm_id, timeframe) for timeframe, arm_id in self.ARMS):
                    raise base.BracketError("trailing account set mismatch")

    def _mark_and_manage(self, ticker: str, at: float, source_hash: str,
                         book: dict[str, list[dict[str, int]]]) -> None:
        for arm in self.db.execute("SELECT * FROM arms WHERE position IS NOT NULL").fetchall():
            pos = json.loads(arm["position"])
            if pos["ticker"] != ticker:
                continue
            bids = book[f"{pos['side']}_bids"]
            net_value = self._net_liquidation_value(bids, pos, pos["quantity"])
            basis = self.CONFIG["mark_basis"] if net_value is not None else None
            self.db.execute("UPDATE arms SET latest_mark=?,mark_time=?,mark_basis=? WHERE arm_id=?",
                            (net_value, at if net_value is not None else None, basis, arm["arm_id"]))
            if at >= pos["close_time"]:
                continue
            if net_value is None and pos.get("trailing_trigger") is None:
                # Full depth is required to activate or move the trail. Once a
                # trigger is sticky, however, every visible contract may be sold.
                continue

            if pos.get("trailing_trigger") is not None:
                trigger = pos["trailing_trigger"]
            elif not pos.get("trailing_activated_at"):
                # A complete-depth net liquidation mark must clear entry cost by 50%.
                if net_value * 100 < pos["cost_cents"] * 150:
                    continue
                pos["trailing_activated_at"] = at
                pos["trailing_activation_source_hash"] = source_hash
                pos["trailing_highwater_net_cents"] = net_value
                self.db.execute("UPDATE arms SET position=? WHERE arm_id=?",
                                (base._json(pos), arm["arm_id"]))
                self._event(at, "trailing_activated", ticker, pos["cohort_id"], arm["arm_id"],
                            source_hash, net_value_cents=net_value,
                            cost_cents=pos["cost_cents"], activation_net_profit_bps=5000)
                continue

            else:
                highwater = max(pos["trailing_highwater_net_cents"], net_value)
            if pos.get("trailing_trigger") is not None:
                highwater = pos["trailing_highwater_net_cents"]
            if net_value is not None and highwater != pos["trailing_highwater_net_cents"]:
                pos["trailing_highwater_net_cents"] = highwater
                self._event(at, "trailing_highwater", ticker, pos["cohort_id"], arm["arm_id"],
                            source_hash, net_value_cents=net_value)

            trigger = pos.get("trailing_trigger")
            if trigger is None and net_value is not None and net_value * 100 <= highwater * 50:
                pos["trailing_trigger"] = {"at": at, "source_hash": source_hash,
                                            "net_value_cents": net_value,
                                            "highwater_net_cents": highwater}
                self.db.execute("UPDATE arms SET position=? WHERE arm_id=?",
                                (base._json(pos), arm["arm_id"]))
                self._event(at, "trailing_trigger", ticker, pos["cohort_id"], arm["arm_id"],
                            source_hash, net_value_cents=net_value,
                            highwater_net_cents=highwater)
                continue

            if trigger is None:
                self.db.execute("UPDATE arms SET position=? WHERE arm_id=?",
                                (base._json(pos), arm["arm_id"]))
                continue

            used = set(pos.get("trailing_used_exit_hashes", []))
            if (at < trigger["at"] + 1 or source_hash == trigger["source_hash"]
                    or source_hash in used):
                self.db.execute("UPDATE arms SET position=? WHERE arm_id=?",
                                (base._json(pos), arm["arm_id"]))
                continue
            depth = sum(level["quantity"] for level in bids)
            if depth <= 0:
                self.db.execute("UPDATE arms SET position=? WHERE arm_id=?",
                                (base._json(pos), arm["arm_id"]))
                continue
            # The inherited fill routine mutates the position and accounts for all
            # visible depth. Persist this quote before the call so partial fills keep
            # a durable anti-reuse set.
            pos["trailing_used_exit_hashes"] = sorted((*used, source_hash))
            self._exit(arm, pos, at, source_hash, bids)

    def status(self, now: float) -> dict[str, Any]:
        result = super().status(now)
        result["config"] = base.json.loads(base._json(self.CONFIG))
        result["trailing_policy"] = {
            "activation": "full-quantity net sale value at least 150% of entry cost",
            "trail": "50% of peak full-quantity net sale value, including accrued proceeds",
            "minimum_exit_delay_seconds": 1,
        }
        for arm in result["arms"]:
            pos = arm["position"]
            if pos is not None:
                arm["trailing"] = {
                    "activated_at": pos.get("trailing_activated_at"),
                    "activation_source_hash": pos.get("trailing_activation_source_hash"),
                    "highwater_net_cents": pos.get("trailing_highwater_net_cents"),
                    "trigger": pos.get("trailing_trigger"),
                    "used_exit_hashes": pos.get("trailing_used_exit_hashes", []),
                }
        return result
