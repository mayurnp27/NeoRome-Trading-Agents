"""Read-only comparison report for the bounded 72-hour memecoin sleeves.

The report intentionally does not import the runtime or :class:`PaperLedger`.
It reads the durable SQLite schema through ``mode=ro`` connections and rebuilds
the account state at the requested cutoff from timestamped lifecycle rows.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from typing import Any, Mapping

UTC = timezone.utc
REPORT_VERSION = "memecoin-goal-comparison-v1"
GOAL_START = datetime(2026, 9, 26, 19, 29, 58, tzinfo=UTC)
GOAL_END = datetime(2026, 9, 29, 19, 29, 58, tzinfo=UTC)
SLEEVE_CAPITAL_CENTS = 30_000
TARGET_NAV_CENTS = 70_000
MARK_FRESHNESS_SECONDS = 90
REQUIRED_COHORTS = ("E4", "E5", "E6", "E7", "E8")
OPTIONAL_COHORTS = ("M0", "E1", "E2")
MAX_JSON_BYTES = 2_000_000


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp must be a string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must include timezone")
    return parsed.astimezone(UTC)


def _report_now(now: datetime | str | None) -> datetime:
    if now is None:
        return datetime.now(UTC)
    if isinstance(now, str):
        return _timestamp(now)
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    return now.astimezone(UTC)


def _safe_json(path: Path) -> tuple[Mapping[str, Any] | None, str | None]:
    if path.is_symlink() or not path.is_file():
        return None, "missing"
    try:
        if path.stat().st_size > MAX_JSON_BYTES:
            return None, "too_large"
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        return None, type(exc).__name__
    if not isinstance(value, Mapping):
        return None, "not_object"
    return value, None


def _state_path(root: Path, cohort: str) -> Path:
    return root / "state.json" if cohort == "M0" else root / cohort.lower() / "state.json"


def _ledger_path(root: Path, cohort: str) -> Path:
    return root / ("paper.sqlite3" if cohort == "M0" else f"paper-{cohort.lower()}.sqlite3")


def _candidate_summary(state_dir: Path, cohort: str, cutoff: datetime) -> dict[str, Any]:
    directory = state_dir / "candidates"
    summary: dict[str, Any] = {
        "status": "ok" if directory.is_dir() and not directory.is_symlink() else "missing",
        "scope": {"recorded_at_gte": _iso(GOAL_START), "recorded_at_lte": _iso(cutoff)},
        "files_seen": 0,
        "invalid_files": 0,
        "out_of_window": 0,
        "missing_recorded_at": 0,
        "candidates": 0,
        "current_cohort_experiments": 0,
        "status_counts": {"eligible": 0, "rejected": 0},
        "rejection_reasons": {},
    }
    if not directory.is_dir() or directory.is_symlink():
        return summary
    reasons: Counter[str] = Counter()
    for path in sorted(directory.glob("*.json")):
        summary["files_seen"] += 1
        record, error = _safe_json(path)
        if error is not None:
            summary["invalid_files"] += 1
            continue
        try:
            recorded = _timestamp(record.get("recorded_at"))
        except ValueError:
            summary["missing_recorded_at"] += 1
            continue
        if recorded < GOAL_START or recorded > cutoff:
            summary["out_of_window"] += 1
            continue
        summary["candidates"] += 1
        experiments = record.get("experiments")
        matching: list[Mapping[str, Any]] = []
        if isinstance(experiments, list):
            matching = [item for item in experiments
                        if isinstance(item, Mapping) and item.get("cohort_id") == cohort]
        # A compact candidate fixture may carry its cohort/status at top level.
        if not matching and record.get("cohort_id") == cohort:
            matching = [record]
        for experiment in matching:
            status = experiment.get("status")
            if status not in ("eligible", "rejected"):
                continue
            summary["current_cohort_experiments"] += 1
            summary["status_counts"][status] += 1
            if status == "rejected":
                values = experiment.get("reasons")
                if not isinstance(values, list):
                    values = experiment.get("rejection_reasons")
                if not isinstance(values, list):
                    values = record.get("rejection_reasons")
                for reason in set(values) if isinstance(values, list) else []:
                    if isinstance(reason, str) and reason:
                        reasons[reason] += 1
    summary["rejection_reasons"] = dict(sorted(reasons.items()))
    return summary


def _table_names(conn: sqlite3.Connection) -> set[str]:
    return {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _missing_ledger(path: Path, reason: str = "ledger_missing") -> dict[str, Any]:
    return {
        "status": reason,
        "path": str(path),
        "initial_cash_cents": None,
        "execution": None,
        "observed_performance": None,
        "synthetic_diagnostics": None,
        "mark_coverage": {"status": "unknown", "reason": reason},
    }


def _provenance_counts(rows: list[Mapping[str, Any]]) -> dict[str, int]:
    result: Counter[str] = Counter(str(row["provenance"]) for row in rows)
    return dict(sorted(result.items()))


def _ledger_snapshot(path: Path, cutoff: datetime) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        return _missing_ledger(path)
    uri = f"file:{path.resolve()}?mode=ro"
    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(uri, uri=True)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        required = {"accounts", "proposals", "reservations", "fills", "positions", "closes", "audit_events"}
        if not required.issubset(_table_names(conn)):
            raise ValueError("paper ledger schema is incomplete")
        account = conn.execute("SELECT * FROM accounts WHERE desk='crypto'").fetchone()
        if account is None:
            raise ValueError("crypto account is missing")
        initial_cash = int(account["initial_cash_cents"])
        fills_all = conn.execute("SELECT * FROM fills WHERE desk='crypto' ORDER BY filled_at,rowid").fetchall()
        positions = {row["position_id"]: row for row in
                     conn.execute("SELECT * FROM positions WHERE desk='crypto'").fetchall()}
        closes_all = conn.execute(
            "SELECT c.* FROM closes c JOIN positions p USING(position_id) "
            "WHERE p.desk='crypto' ORDER BY c.closed_at,c.rowid"
        ).fetchall()
        fills = [row for row in fills_all if _timestamp(row["filled_at"]) <= cutoff]
        fill_ids = {row["fill_id"] for row in fills}
        fills_by_position = {row["position_id"]: row for row in fills}
        closes = [row for row in closes_all
                  if row["position_id"] in fills_by_position and _timestamp(row["closed_at"]) <= cutoff]
        close_by_position = {row["position_id"]: row for row in closes}
        open_positions = [positions[row["position_id"]] for row in fills
                          if row["position_id"] not in close_by_position]

        entry_cost = sum(int(row["total_cost_cents"]) for row in fills)
        entry_fees = sum(int(row["fee_cents"]) for row in fills)
        exit_proceeds = sum(int(row["proceeds_cents"]) for row in closes)
        exit_fees = sum(int(row["fee_cents"]) for row in closes)
        realized = sum(int(row["proceeds_cents"]) - int(row["fee_cents"])
                       - int(fills_by_position[row["position_id"]]["total_cost_cents"])
                       for row in closes)

        reservations = conn.execute("SELECT * FROM reservations WHERE desk='crypto'").fetchall()
        audits = conn.execute(
            "SELECT entity_id, event_type, occurred_at FROM audit_events "
            "WHERE event_type IN ('cancel','reserve') ORDER BY occurred_at,event_id"
        ).fetchall()
        cancelled_by_cutoff = {row["entity_id"] for row in audits
                               if row["event_type"] == "cancel" and _timestamp(row["occurred_at"]) <= cutoff}
        reserved_rows = []
        for row in reservations:
            if _timestamp(row["created_at"]) > cutoff:
                continue
            fill = next((item for item in fills_all if item["reservation_id"] == row["reservation_id"]), None)
            filled_by_cutoff = fill is not None and _timestamp(fill["filled_at"]) <= cutoff
            if not filled_by_cutoff and row["reservation_id"] not in cancelled_by_cutoff:
                reserved_rows.append(row)
        reserved_cash = sum(int(row["reserved_cents"]) for row in reserved_rows)
        available_cash = initial_cash - entry_cost + sum(
            int(row["proceeds_cents"]) - int(row["fee_cents"]) for row in closes
        ) - reserved_cash

        mark_tables = {"mark_batches", "market_marks"}.issubset(_table_names(conn))
        selected_marks: dict[str, Mapping[str, Any]] = {}
        batches: list[Mapping[str, Any]] = []
        if mark_tables:
            batches = conn.execute(
                "SELECT rowid AS _rowid, * FROM mark_batches ORDER BY as_of,rowid"
            ).fetchall()
            for batch in batches:
                batch_at = _timestamp(batch["as_of"])
                if batch_at > cutoff:
                    continue
                for mark in conn.execute(
                    "SELECT * FROM market_marks WHERE batch_id=?", (batch["batch_id"],)
                ).fetchall():
                    if mark["position_id"] not in fills_by_position:
                        continue
                    source_at = _timestamp(mark["source_timestamp"])
                    if source_at > cutoff:
                        continue
                    prior = selected_marks.get(mark["position_id"])
                    current_key = (batch_at, int(batch["_rowid"]))
                    prior_key = prior["_sort_key"] if prior is not None else None
                    if prior_key is None or current_key > prior_key:
                        selected_marks[mark["position_id"]] = {
                            **dict(mark), "batch_as_of": batch["as_of"], "_sort_key": current_key,
                        }

        mark_details: list[dict[str, Any]] = []
        mark_status = "not_required" if not open_positions else "complete"
        mark_reason: str | None = None
        marked_value: int | None = 0
        for position in open_positions:
            mark = selected_marks.get(position["position_id"])
            if mark is None:
                mark_status = "missing"
                mark_reason = "open_position_has_no_mark_at_cutoff"
                marked_value = None
                mark_details.append({"position_id": position["position_id"], "status": "missing"})
                continue
            source_at = _timestamp(mark["source_timestamp"])
            age = (cutoff - source_at).total_seconds()
            detail = {
                "position_id": position["position_id"],
                "liquidation_value_cents": int(mark["liquidation_value_cents"]),
                "batch_as_of": mark["batch_as_of"],
                "source_timestamp": mark["source_timestamp"],
                "age_seconds": age,
                "provenance": mark["provenance"],
            }
            mark_details.append(detail)
            if source_at > cutoff:
                mark_status = "future"
                mark_reason = "mark_source_after_cutoff"
                marked_value = None
            elif source_at < _timestamp(position["opened_at"]):
                mark_status = "invalid"
                mark_reason = "mark_source_before_position_open"
                marked_value = None
            elif age > MARK_FRESHNESS_SECONDS:
                mark_status = "stale"
                mark_reason = "mark_source_stale_at_cutoff"
                marked_value = None
            elif marked_value is not None:
                marked_value += int(mark["liquidation_value_cents"])
        if mark_status == "complete" and marked_value is not None:
            nav = available_cash + reserved_cash + marked_value
        elif mark_status == "not_required":
            nav = available_cash + reserved_cash
        else:
            nav = None

        def execution_for(rows_fills: list[Mapping[str, Any]], rows_closes: list[Mapping[str, Any]],
                          rows_open: list[Mapping[str, Any]], *, nav_value: int | None) -> dict[str, Any]:
            costs = sum(int(row["total_cost_cents"]) for row in rows_fills)
            fees_in = sum(int(row["fee_cents"]) for row in rows_fills)
            fees_out = sum(int(row["fee_cents"]) for row in rows_closes)
            realized_pnl = sum(int(row["proceeds_cents"]) - int(row["fee_cents"])
                               - int(fills_by_position[row["position_id"]]["total_cost_cents"])
                               for row in rows_closes)
            return {
                "fills": len(rows_fills), "closed_positions": len(rows_closes),
                "open_positions": len(rows_open), "entry_cost_cents": costs,
                "entry_fees_cents": fees_in, "exit_fees_cents": fees_out,
                "total_fees_cents": fees_in + fees_out,
                "gross_exit_proceeds_cents": sum(int(row["proceeds_cents"]) for row in rows_closes),
                "realized_pnl_cents": realized_pnl,
                "marked_open_value_cents": (None if nav_value is None else
                    sum(int(selected_marks[p["position_id"]]["liquidation_value_cents"])
                        for p in rows_open if p["position_id"] in selected_marks)),
                "nav_cents": nav_value,
                "net_pnl_cents": None if nav_value is None else nav_value - initial_cash,
            }

        observed_fills = [row for row in fills if row["provenance"] == "observed"]
        observed_ids = {row["position_id"] for row in observed_fills}
        observed_closes = [row for row in closes if row["provenance"] == "observed"
                           and row["position_id"] in observed_ids]
        observed_open = [row for row in open_positions if row["position_id"] in observed_ids]
        observed_marks_fresh = all(
            position["position_id"] in selected_marks
            and selected_marks[position["position_id"]]["provenance"] == "observed"
            and _timestamp(selected_marks[position["position_id"]]["source_timestamp"]) >=
                _timestamp(position["opened_at"])
            and (cutoff - _timestamp(selected_marks[position["position_id"]]["source_timestamp"])).total_seconds()
            <= MARK_FRESHNESS_SECONDS for position in observed_open
        )
        observed_marked = (sum(int(selected_marks[position["position_id"]]["liquidation_value_cents"])
                               for position in observed_open)
                           if observed_marks_fresh else None)
        observed_entry = sum(int(row["total_cost_cents"]) for row in observed_fills)
        observed_exit = sum(int(row["proceeds_cents"]) - int(row["fee_cents"]) for row in observed_closes)
        observed_nav = (initial_cash - observed_entry + observed_exit + (observed_marked or 0)
                        if observed_marked is not None else None)
        result = {
            "status": "ok", "path": str(path), "initial_cash_cents": initial_cash,
            "capital_scope": "isolated_crypto_sleeve", "as_of": _iso(cutoff),
            "execution": execution_for(fills, closes, open_positions, nav_value=nav),
            "available_cash_cents": available_cash,
            "reserved_cash_cents": reserved_cash,
            "observed_performance": execution_for(observed_fills, observed_closes, observed_open,
                                                   nav_value=observed_nav),
            "synthetic_diagnostics": {
                "fills": sum(row["provenance"] == "synthetic" for row in fills),
                "closes": sum(row["provenance"] == "synthetic" for row in closes),
                "open_positions": sum(row["provenance"] == "synthetic" for row in open_positions),
                "fill_provenance": _provenance_counts(fills),
                "close_provenance": _provenance_counts(closes),
            },
            "mark_coverage": {
                "status": mark_status, "reason": mark_reason,
                "freshness_seconds": MARK_FRESHNESS_SECONDS,
                "open_positions": len(open_positions), "marked_positions": len(mark_details),
                "details": mark_details,
                "latest_recorded_batch_at_or_before_cutoff": (
                    max((batch["as_of"] for batch in batches if _timestamp(batch["as_of"]) <= cutoff),
                        default=None) if mark_tables else None),
                "source_time_semantics": "provider source_timestamp compared with recorded batch as_of",
            },
            "lifecycle_counts": {
                "fills_as_of": len(fills), "future_fills_excluded": len(fills_all) - len(fills),
                "closes_as_of": len(closes), "future_closes_excluded": len(closes_all) - len(closes),
                "reservations_as_of": len(reserved_rows),
                "fills_in_goal_window": sum(GOAL_START <= _timestamp(row["filled_at"]) <= cutoff for row in fills),
                "closes_in_goal_window": sum(GOAL_START <= _timestamp(row["closed_at"]) <= cutoff for row in closes),
                "pre_window_fills_as_of": sum(_timestamp(row["filled_at"]) < GOAL_START for row in fills),
                "pre_window_closes_as_of": sum(_timestamp(row["closed_at"]) < GOAL_START for row in closes),
            },
            "capital_semantics": ("lifetime_asof_including_pre_window_activity"
                                   if any(_timestamp(row["filled_at"]) < GOAL_START for row in fills)
                                   else "goal_window_asof"),
            "provenance": {"fills": _provenance_counts(fills), "closes": _provenance_counts(closes)},
        }
        conn.commit()
        return result
    except (sqlite3.Error, OSError, ValueError, TypeError, KeyError) as exc:
        if conn is not None:
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
        return _missing_ledger(path, "invalid_ledger:" + type(exc).__name__)
    finally:
        if conn is not None:
            conn.close()


def _sleeve(root: Path, cohort: str, cutoff: datetime, required: bool) -> dict[str, Any]:
    ledger = _ledger_path(root, cohort)
    state = _state_path(root, cohort)
    state_value, state_error = _safe_json(state)
    result: dict[str, Any] = {
        "cohort_id": cohort, "required": required,
        "state": {"status": "ok" if state_error is None else state_error, "path": str(state)},
        "ledger": _ledger_snapshot(ledger, cutoff),
        "candidates": _candidate_summary(state.parent, cohort, cutoff),
    }
    if state_value is not None:
        result["state"]["cohort_id"] = state_value.get("cohort_id", cohort)
    return result


def build_report(root: str | Path, *, now: datetime | str | None = None) -> dict[str, Any]:
    """Build one immutable as-of report without creating or changing a ledger."""
    root_path = Path(root)
    generated = _report_now(now)
    cutoff = min(generated, GOAL_END)
    sleeves = [_sleeve(root_path, cohort, cutoff, True) for cohort in REQUIRED_COHORTS]
    for cohort in OPTIONAL_COHORTS:
        if _ledger_path(root_path, cohort).is_file() and not _ledger_path(root_path, cohort).is_symlink():
            sleeves.append(_sleeve(root_path, cohort, cutoff, False))
    return {
        "report_version": REPORT_VERSION,
        "generated_at": _iso(generated), "as_of": _iso(cutoff), "cutoff": _iso(cutoff),
        "horizon_hours": 72,
        "goal": {
            "start": _iso(GOAL_START), "end": _iso(GOAL_END), "horizon_hours": 72,
            "initial_capital_cents_per_sleeve": SLEEVE_CAPITAL_CENTS,
            "target_nav_cents": TARGET_NAV_CENTS,
            "required_cohorts": list(REQUIRED_COHORTS),
            "optional_cohorts": list(OPTIONAL_COHORTS),
        },
        "sleeves": sleeves,
        "combined": {
            "capital_scope": "independent_sleeves",
            "aggregate_nav_cents": None,
            "aggregate_pnl_cents": None,
            "reason": "independent accounts are reported separately; values are not summed",
            "required_sleeves_present": sum(item["ledger"]["status"] == "ok" for item in sleeves
                                             if item["required"]),
            "required_sleeves": len(REQUIRED_COHORTS),
        },
    }


def _atomic_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    finally:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass


def _markdown(report: Mapping[str, Any]) -> str:
    def money(value: Any) -> str:
        if not isinstance(value, int):
            return "unknown"
        return f"${value / 100:,.2f}"

    lines = [
        "# NeoRome memecoin 72-hour goal comparison", "",
        f"Generated: `{report['generated_at']}`  ",
        f"As of / cutoff: `{report['as_of']}`  ",
        f"Goal window: `{report['goal']['start']}` to `{report['goal']['end']}`  ",
        "", "| Strategy | Ledger | Initial cash | Paper NAV | Net P&L | Realized P&L | Fees | Fills / exits | Marks | Candidates |",
        "|---|---|---:|---:|---:|---:|---:|---:|---|---:|",
    ]
    names = {"E4": "Continuation", "E5": "Pullback/reclaim", "E6": "+50% exit",
             "E7": "+100% exit", "E8": "Swing runner", "M0": "Social baseline",
             "E1": "Route controls", "E2": "Established pool"}
    reason_lines = []
    for sleeve in report["sleeves"]:
        ledger = sleeve["ledger"]
        execution = ledger.get("observed_performance") or {}
        candidates = sleeve["candidates"]
        lines.append("| {cohort} | {status} | {cash} | {nav} | {net} | {pnl} | {fees} | {fills} / {exits} | {marks} | {count} |".format(
            cohort=sleeve["cohort_id"] + " — " + names.get(sleeve["cohort_id"], ""), status=ledger["status"],
            cash=money(ledger.get("initial_cash_cents")),
            nav=money(execution.get("nav_cents")),
            pnl=money(execution.get("realized_pnl_cents")),
            net=money(execution.get("net_pnl_cents")),
            fees=money(execution.get("total_fees_cents")),
            fills=execution.get("fills", "unknown"), exits=execution.get("closed_positions", "unknown"),
            marks=(ledger.get("mark_coverage") or {}).get("status", "unknown"),
            count=candidates.get("candidates", 0),
        ))
        reasons = candidates.get("rejection_reasons") or {}
        if reasons:
            top_reasons = ", ".join(f"{name} ({count})" for name, count in
                                    sorted(reasons.items(), key=lambda item: (-item[1], item[0]))[:5])
            reason_lines.append(f"- {sleeve['cohort_id']}: {top_reasons}")
    lines.extend(["", "Entry rejection reasons:", "", *reason_lines,
                  "", "Each sleeve is an independent account; combined NAV and P&L are intentionally not summed.",
                  "Synthetic lifecycle rows are retained as diagnostics and are excluded from observed performance.", ""])
    return "\n".join(lines)


def publish_report(root: str | Path, output: str | Path, markdown: str | Path | None = None,
                   *, now: datetime | str | None = None) -> dict[str, Any]:
    report = build_report(root, now=now)
    output_path = Path(output)
    if output_path.exists() and not output_path.is_symlink():
        try:
            prior = json.loads(output_path.read_text(encoding="utf-8"))
            prior_stamp = str(prior.get("generated_at", "unknown")).replace(":", "").replace("/", "-")
            history = output_path.parent / "history"
            _atomic_bytes(history / f"goal-comparison-{prior_stamp}.json",
                          json.dumps(prior, sort_keys=True, indent=2).encode("utf-8") + b"\n")
        except (OSError, UnicodeError, ValueError, TypeError):
            pass
    _atomic_bytes(output_path, json.dumps(report, sort_keys=True, indent=2).encode("utf-8") + b"\n")
    if markdown is not None:
        _atomic_bytes(Path(markdown), _markdown(report).encode("utf-8"))
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default="/var/lib/neorome-memecoin-program")
    parser.add_argument("--output", default=None)
    parser.add_argument("--markdown", default=None)
    parser.add_argument("--now", default=None, help="timezone-aware timestamp; intended for tests")
    args = parser.parse_args(argv)
    output = Path(args.output) if args.output else Path(args.root) / "reports" / "goal-comparison-latest.json"
    markdown = Path(args.markdown) if args.markdown else output.with_suffix(".md")
    publish_report(args.root, output, markdown, now=args.now)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
