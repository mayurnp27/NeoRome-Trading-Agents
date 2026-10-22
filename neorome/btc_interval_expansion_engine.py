"""Independent paper accounts expanding BTC bracket experiments to new intervals."""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from . import btc_bracket_engine as base


BRACKET_ARMS = tuple(arm for arm in base.ARMS if arm != (None, None))
GROUP_ARMS = {
    "15m-original": (("15m-original-hold", None, None),),
    "15m-gap5": (("15m-gap5-hold", None, None),),
    "15m-comparison": (("15m-comparison-hold", None, None),) + tuple(
        (f"15m-comparison-tp{tp}-sl{sl}", tp, sl) for tp, sl in BRACKET_ARMS
    ),
    "15m-trailing": (("15m-trailing-a50-d50", None, None),),
    "1m-sma-brackets": tuple(
        (f"1m-sma-tp{tp}-sl{sl}", tp, sl) for tp, sl in BRACKET_ARMS
    ),
    "1m-sma-stoch-brackets": tuple(
        (f"1m-sma-stoch-tp{tp}-sl{sl}", tp, sl) for tp, sl in BRACKET_ARMS
    ),
    "1m-sma-reversal-brackets": tuple(
        (f"1m-sma-reversal-tp{tp}-sl{sl}", tp, sl) for tp, sl in BRACKET_ARMS
    ),
    "1m-sma-stoch-reversal-brackets": tuple(
        (f"1m-sma-stoch-reversal-tp{tp}-sl{sl}", tp, sl) for tp, sl in BRACKET_ARMS
    ),
}
OPTIONAL_GROUPS = frozenset((
    "1m-sma-reversal-brackets", "1m-sma-stoch-reversal-brackets",
))
GROUP_INTERVAL = {name: (15 if name.startswith("15m-") else 1) for name in GROUP_ARMS}
ENTRY_WINDOW_SECONDS = {15: 90, 1: 30}
INITIAL_CASH_CENTS = 50_000


class ExpansionEngine(base.Engine):
    """Persistent ledger for independent 15-minute and one-minute BTC experiments."""

    CONFIG = {
        "version": 1,
        "engine": "btc-interval-expansion-paper",
        "initial_cash_cents": INITIAL_CASH_CENTS,
        "allocation_bps": base.ALLOCATION_BPS,
        "allocation_policy_version": base.ALLOCATION_POLICY_VERSION,
        "fee": "ceil(0.07*N*p*(1-p)*multiplier*100)",
        "entry_buffer_cents": 1,
        "exit_buffer_cents": 1,
        "entry_quotes": 2,
        "entry_quote_separation_seconds": 2,
        "maximum_entry_ask_cents": 60,
        "entry_windows_seconds": {"15m": 90, "1m": 30},
        "trailing_activation_net_profit_bps": 5000,
        "trailing_from_peak_bps": 5000,
        "trailing_mark_basis": "full_quantity_net_liquidation_after_fee_and_buffer_including_accrued_proceeds",
        "include_reversal_brackets": False,
        "reversal_exit_precedence": "first_observed_tp_sl_or_opposite_sma_cross_is_sticky",
    }

    def __init__(self, path: str, *, started_at: float,
                 include_reversal_brackets: bool = False):
        if not isinstance(include_reversal_brackets, bool):
            raise base.BracketError("include_reversal_brackets must be boolean")
        self.started_at = base._finite(started_at, "started_at")
        self.include_reversal_brackets = include_reversal_brackets
        self.account_groups = tuple(name for name in GROUP_ARMS
                                    if name not in OPTIONAL_GROUPS or include_reversal_brackets)
        self.account_specs = tuple(
            (group, arm_id, tp, sl)
            for group in self.account_groups
            for arm_id, tp, sl in GROUP_ARMS[group]
        )
        self.config = {**self.CONFIG, "include_reversal_brackets": include_reversal_brackets,
                       "groups": list(self.account_groups),
                       "arms": [arm_id for _, arm_id, _, _ in self.account_specs]}
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
            old_start = self.db.execute("SELECT value FROM meta WHERE key='started_at'").fetchone()
            existing_arms = self.db.execute("SELECT COUNT(*) FROM arms").fetchone()[0]
            if row and json.loads(row[0]) != self.config:
                raise base.BracketError("frozen interval expansion configuration mismatch")
            if old_start and float(old_start[0]) != self.started_at:
                raise base.BracketError("started_at mismatch on resume")
            if not row and existing_arms:
                raise base.BracketError("refusing database containing unrecognized paper accounts")
            if not row:
                self.db.execute("INSERT INTO meta VALUES('config',?)", (base._json(self.config),))
                self.db.execute("INSERT INTO meta VALUES('started_at',?)", (repr(self.started_at),))
                for group, arm_id, tp, sl in self.account_specs:
                    self.db.execute(
                        "INSERT INTO arms VALUES(?,?,?,?,?,?,?,0,0,0,0,0,NULL,NULL,NULL,NULL)",
                        (arm_id, group, tp, sl, INITIAL_CASH_CENTS, INITIAL_CASH_CENTS, 0),
                    )
            else:
                actual = [(r["arm_id"], r["timeframe"], r["tp"], r["sl"])
                          for r in self.db.execute("SELECT * FROM arms ORDER BY arm_id")]
                expected = sorted((arm_id, group, tp, sl)
                                  for group, arm_id, tp, sl in self.account_specs)
                if actual != expected:
                    raise base.BracketError("interval expansion account set mismatch")

    def _window_for(self, group: str) -> int:
        try:
            return ENTRY_WINDOW_SECONDS[GROUP_INTERVAL[group]]
        except KeyError:
            raise base.BracketError("invalid interval expansion group") from None

    def register_cohort(self, cohort: dict) -> None:
        required = {"cohort_id", "timeframe", "ticker", "side", "observed_at", "signal_time",
                    "close_time", "fee_multiplier", "source_hash"}
        if not isinstance(cohort, dict) or set(cohort) != required:
            raise base.BracketError("cohort fields do not match protocol")
        cid, group, ticker, side = (cohort[k] for k in ("cohort_id", "timeframe", "ticker", "side"))
        if group not in self.account_groups:
            raise base.BracketError("invalid interval expansion group")
        if not isinstance(cid, str) or not cid or len(cid) > 256:
            raise base.BracketError("invalid cohort_id")
        if not isinstance(ticker, str) or not ticker or len(ticker) > 256:
            raise base.BracketError("invalid ticker")
        if side not in ("yes", "no"):
            raise base.BracketError("side must be yes or no")
        observed = base._finite(cohort["observed_at"], "observed_at")
        signal = base._finite(cohort["signal_time"], "signal_time")
        close = base._finite(cohort["close_time"], "close_time")
        if (observed < self.started_at or observed < signal
                or observed > signal + self._window_for(group)
                or observed >= close or signal >= close):
            raise base.BracketError("cohort is outside its prospective window")
        try:
            multiplier = base.Decimal(cohort["fee_multiplier"])
        except (base.InvalidOperation, TypeError):
            raise base.BracketError("fee_multiplier must be a decimal string") from None
        if (not isinstance(cohort["fee_multiplier"], str) or not multiplier.is_finite()
                or multiplier <= 0 or multiplier > base.Decimal("1000000")):
            raise base.BracketError("fee_multiplier must be a positive decimal string no greater than 1000000")
        source_hash = base._hash(cohort["source_hash"], "source_hash")
        body = {**cohort, "observed_at": observed, "signal_time": signal,
                "close_time": close, "fee_multiplier": str(multiplier), "source_hash": source_hash}
        payload = base._json(body)
        with self._tx():
            old = self.db.execute("SELECT payload FROM cohorts WHERE cohort_id=?", (cid,)).fetchone()
            if old:
                if old[0] != payload:
                    raise base.BracketError("cohort_id payload conflict")
                return
            bound = self.db.execute("SELECT close_time FROM cohorts WHERE ticker=? LIMIT 1", (ticker,)).fetchone()
            if bound and bound[0] != close:
                raise base.BracketError("ticker close_time conflicts with registered source")
            blocker = self.db.execute("SELECT value FROM controls WHERE key='entry_blocker'").fetchone()
            state = "blocked" if blocker and blocker[0] else "waiting"
            self.db.execute(
                "INSERT INTO cohorts(cohort_id,timeframe,ticker,side,observed,signal,close_time,multiplier,source_hash,payload,state) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (cid, group, ticker, side, observed, signal, close, str(multiplier), source_hash, payload, state),
            )
            self._event(observed, "cohort", ticker, cid, source_hash=source_hash,
                        timeframe=group, side=side)
            if state == "blocked":
                self._skip(self.db.execute("SELECT * FROM cohorts WHERE cohort_id=?", (cid,)).fetchone(),
                           observed, "entry_blocked", source_hash)

    def _skip(self, cohort, at, reason, source_hash):
        self.db.execute("UPDATE cohorts SET state='skipped' WHERE cohort_id=?", (cohort["cohort_id"],))
        for arm in self.db.execute("SELECT * FROM arms WHERE timeframe=?", (cohort["timeframe"],)):
            self._event(at, "skip", cohort["ticker"], cohort["cohort_id"], arm["arm_id"],
                        source_hash, reason=reason)

    def expire(self, now: float) -> None:
        now = base._finite(now, "now")
        with self._tx():
            for cohort in self.db.execute("SELECT * FROM cohorts WHERE state='waiting'").fetchall():
                if now < min(cohort["close_time"],
                             cohort["signal"] + self._window_for(cohort["timeframe"])):
                    continue
                self._skip(cohort, now, "entry_window_expired", cohort["source_hash"])

    def on_quote(self, ticker: str, quote: dict) -> dict:
        received, _, _ = self._normalized_quote(quote)
        self.expire(received)
        return base.Engine.on_quote(self, ticker, quote)

    def _try_entries(self, cohort, at, source_hash, book, first_book) -> None:
        group = cohort["timeframe"]
        before = {row["arm_id"] for row in self.db.execute(
            "SELECT arm_id FROM arms WHERE timeframe=? AND position IS NOT NULL",
            (group,)).fetchall()}
        super()._try_entries(cohort, at, source_hash, book, first_book)
        for arm in self.db.execute(
                "SELECT * FROM arms WHERE timeframe=? AND position IS NOT NULL", (group,)).fetchall():
            if arm["arm_id"] in before:
                continue
            position = json.loads(arm["position"])
            if position["cohort_id"] == cohort["cohort_id"]:
                position["entry_source_hash"] = source_hash
                position["entry_time"] = at
                self.db.execute("UPDATE arms SET position=? WHERE arm_id=?",
                                 (base._json(position), arm["arm_id"]))

    def on_reversal(self, side: str, observed_at: float, source_hash: str,
                    signal_time: float) -> None:
        if not self.include_reversal_brackets:
            raise base.BracketError("reversal brackets are disabled in this frozen configuration")
        if side not in ("yes", "no"):
            raise base.BracketError("invalid reversal side")
        at = base._finite(observed_at, "observed_at")
        signal_time = base._finite(signal_time, "signal_time")
        source_hash = base._hash(source_hash, "source_hash")
        if not self.started_at <= signal_time <= at <= signal_time + 30:
            raise base.BracketError("reversal outside prospective window")
        with self._tx():
            for arm in self.db.execute(
                    "SELECT * FROM arms WHERE position IS NOT NULL AND timeframe IN (?,?)",
                    tuple(sorted(OPTIONAL_GROUPS))).fetchall():
                position = json.loads(arm["position"])
                if (position["side"] == side or at >= position["close_time"]
                        or position.get("trigger") is not None):
                    continue
                position["trigger"] = {"at": at, "source_hash": source_hash,
                                       "reason": "opposite_sma_cross"}
                position["reversal_source_hash"] = source_hash
                position["reversal_used_exit_hashes"] = position.get("reversal_used_exit_hashes", [])
                self.db.execute("UPDATE arms SET position=? WHERE arm_id=?",
                                 (base._json(position), arm["arm_id"]))
                self._event(at, "reversal_trigger", position["ticker"], position["cohort_id"],
                            arm["arm_id"], source_hash, new_side=side, signal_time=signal_time)

    def _mark_and_manage(self, ticker: str, at: float, source_hash: str,
                         book: dict[str, list[dict[str, int]]]) -> None:
        for arm in self.db.execute("SELECT * FROM arms WHERE position IS NOT NULL").fetchall():
            pos = json.loads(arm["position"])
            if pos["ticker"] != ticker:
                continue
            bids = book[f"{pos['side']}_bids"]
            is_trailing = arm["timeframe"] == "15m-trailing"
            mark = self._net_liquidation_value(bids, pos, pos["quantity"])
            mark_basis = self.config["trailing_mark_basis"] if mark is not None else None
            if not is_trailing:
                mark_basis = ("net_liquidation_cents_including_accrued_proceeds_after_buffer_and_exit_fee"
                              if mark is not None else None)
            self.db.execute("UPDATE arms SET latest_mark=?,mark_time=?,mark_basis=? WHERE arm_id=?",
                            (mark, at if mark is not None else None, mark_basis, arm["arm_id"]))
            if at >= pos["close_time"]:
                continue
            if is_trailing:
                self._manage_trailing(arm, pos, bids, at, source_hash, ticker, mark)
            else:
                self._manage_bracket_or_reversal(arm, pos, bids, at, source_hash, ticker)

    def _manage_bracket_or_reversal(self, arm, pos, bids, at, source_hash, ticker):
        trigger = pos.get("trigger")
        raw_bid_vwap = base._vwap(bids, pos["quantity"])
        before_close = at < pos["close_time"]
        if before_close and trigger is None and arm["tp"] is not None and raw_bid_vwap is not None:
            entry = pos["entry_premium_cents"]
            reason = "take_profit" if raw_bid_vwap >= entry * (100 + arm["tp"]) / 100 else None
            if raw_bid_vwap <= entry * (100 - arm["sl"]) / 100:
                reason = "stop_loss"
            if reason:
                pos["trigger"] = {"at": at, "source_hash": source_hash, "reason": reason}
                self.db.execute("UPDATE arms SET position=? WHERE arm_id=?",
                                (base._json(pos), arm["arm_id"]))
                self._event(at, "trigger", ticker, pos["cohort_id"], arm["arm_id"],
                            source_hash, reason=reason)
            return
        if not (before_close and trigger and at >= trigger["at"] + 1
                and source_hash != trigger["source_hash"]
                and source_hash != pos.get("last_exit_source_hash")):
            return
        if trigger.get("reason") == "opposite_sma_cross":
            used = set(pos.get("reversal_used_exit_hashes", []))
            if source_hash in used:
                return
            pos["reversal_used_exit_hashes"] = sorted(used | {source_hash})
        self._exit(arm, pos, at, source_hash, bids)

    def _manage_trailing(self, arm, pos, bids, at, source_hash, ticker, net_value):
        # This is the trailing engine's 50% net-value activation and 50% peak drawdown
        # policy, scoped to the single trailing account in this engine.
        if net_value is None and pos.get("trailing_trigger") is None:
            return
        if pos.get("trailing_trigger") is not None:
            trigger = pos["trailing_trigger"]
            highwater = pos["trailing_highwater_net_cents"]
        elif not pos.get("trailing_activated_at"):
            if net_value is None or net_value * 100 < pos["cost_cents"] * 150:
                return
            pos["trailing_activated_at"] = at
            pos["trailing_activation_source_hash"] = source_hash
            pos["trailing_highwater_net_cents"] = net_value
            self.db.execute("UPDATE arms SET position=? WHERE arm_id=?",
                             (base._json(pos), arm["arm_id"]))
            self._event(at, "trailing_activated", ticker, pos["cohort_id"], arm["arm_id"],
                        source_hash, net_value_cents=net_value, cost_cents=pos["cost_cents"],
                        activation_net_profit_bps=5000)
            return
        else:
            highwater = max(pos["trailing_highwater_net_cents"], net_value)
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
                        source_hash, net_value_cents=net_value, highwater_net_cents=highwater)
            return
        if trigger is None:
            self.db.execute("UPDATE arms SET position=? WHERE arm_id=?",
                             (base._json(pos), arm["arm_id"]))
            return
        used = set(pos.get("trailing_used_exit_hashes", []))
        if at < trigger["at"] + 1 or source_hash == trigger["source_hash"] or source_hash in used:
            self.db.execute("UPDATE arms SET position=? WHERE arm_id=?",
                             (base._json(pos), arm["arm_id"]))
            return
        if sum(level["quantity"] for level in bids) <= 0:
            self.db.execute("UPDATE arms SET position=? WHERE arm_id=?",
                             (base._json(pos), arm["arm_id"]))
            return
        pos["trailing_used_exit_hashes"] = sorted(used | {source_hash})
        self._exit(arm, pos, at, source_hash, bids)

    def status(self, now: float) -> dict[str, Any]:
        result = super().status(now)
        result["config"] = json.loads(base._json(self.config))
        result["expansion_policy"] = {
            "entry_windows_seconds": {"15m": 90, "1m": 30},
            "reversal_exit_precedence": "first observed take-profit, stop-loss, or opposite SMA cross is sticky",
            "trailing_policy": "15m-trailing only; activate at 50% net profit and exit after a 50% drop from peak",
        }
        for arm in result["arms"]:
            pos = arm["position"]
            if pos is not None and arm["timeframe"] == "15m-trailing":
                arm["trailing"] = {
                    "activated_at": pos.get("trailing_activated_at"),
                    "activation_source_hash": pos.get("trailing_activation_source_hash"),
                    "highwater_net_cents": pos.get("trailing_highwater_net_cents"),
                    "trigger": pos.get("trailing_trigger"),
                    "used_exit_hashes": pos.get("trailing_used_exit_hashes", []),
                }
        return result
