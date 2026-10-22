"""Deterministic, read-only evaluation of observed memecoin paper closes.

This job never connects to a venue or mutates the paper ledger. It incrementally
copies close evidence into a separate local cache and emits a compact evaluation
plus explicitly non-activating challenger hypotheses.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import stat
import sqlite3
import tempfile
import tempfile
from typing import Any, Mapping

UTC = timezone.utc
REPORT_VERSION = "memecoin-learning-v1"
MIN_PROMOTION_CLUSTERS = 30
MIN_VALIDATION_CLUSTERS = 10
MAX_CLOSES_PER_REFRESH = 250
MAX_OUTPUT_BYTES = 2_000_000
MAX_CANDIDATE_BYTES = 131_072
MAX_LEDGER_SNAPSHOT_BYTES = 128 * 1024 * 1024
SNAPSHOT_ATTEMPTS = 3
COPY_CHUNK_BYTES = 1024 * 1024


def _time(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp_missing")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp_timezone_missing")
    return parsed.astimezone(UTC)


def _iso(value: datetime | None = None) -> str:
    return (value or datetime.now(UTC)).astimezone(UTC).isoformat().replace("+00:00", "Z")


def _json_object(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, str):
        return {}
    try:
        result = json.loads(value)
    except (ValueError, TypeError):
        return {}
    return result if isinstance(result, Mapping) else {}


def _mint(instrument: Any, exposure_group: Any) -> str | None:
    for value in (instrument, exposure_group):
        if isinstance(value, str):
            prefix = "solana:" if value.startswith("solana:") else "token:solana:" if value.startswith("token:solana:") else None
            if prefix:
                candidate = value[len(prefix):]
                if 20 <= len(candidate) <= 64 and candidate.isascii() and candidate.isalnum():
                    return candidate
    return None


def _entry_feature_bins(record: Mapping[str, Any]) -> list[str]:
    """Fixed, low-cardinality bins over fields in persisted candidate records."""
    bins: list[str] = []
    pool = record.get("pool") if isinstance(record.get("pool"), Mapping) else {}
    concentration = record.get("account_concentration") if isinstance(record.get("account_concentration"), Mapping) else {}
    try:
        age = float(pool.get("age_hours_at_observation"))
        if math.isfinite(age) and age >= 0:
            label = "0-1" if age < 1 else "1-6" if age < 6 else "6-24" if age < 24 else "24+"
            bins.append("pool_age_hours:" + label)
    except (TypeError, ValueError, OverflowError):
        pass
    try:
        liquidity = float(pool.get("liquidity_usd"))
        if math.isfinite(liquidity) and liquidity >= 0:
            label = "0-5k" if liquidity < 5_000 else "5-25k" if liquidity < 25_000 else "25-100k" if liquidity < 100_000 else "100k+"
            bins.append("liquidity_usd:" + label)
    except (TypeError, ValueError, OverflowError):
        pass
    try:
        buys, sells = float(pool.get("buys_h1")), float(pool.get("sells_h1"))
        if math.isfinite(buys) and math.isfinite(sells) and buys >= 0 and sells >= 0:
            if sells == 0:
                label = "no_sells_observed"
            else:
                ratio = buys / sells
                label = "0-1" if ratio < 1 else "1-2" if ratio < 2 else "2-5" if ratio < 5 else "5+"
            bins.append("flow_buy_sell_ratio:" + label)
    except (TypeError, ValueError, OverflowError):
        pass
    try:
        largest = float(concentration.get("largest_account_supply_fraction"))
        if math.isfinite(largest) and 0 <= largest <= 1:
            pct = largest * 100
            label = "0-10" if pct < 10 else "10-20" if pct < 20 else "20-35" if pct < 35 else "35+"
            bins.append("largest_holder_pct:" + label)
    except (TypeError, ValueError, OverflowError):
        pass
    return sorted(set(bins))


def _readonly_connection(path: Path) -> sqlite3.Connection:
    if path.is_symlink() or not path.is_file():
        raise ValueError("snapshot_missing_or_not_regular")
    conn = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.execute("BEGIN")
    return conn


def _fingerprint(path: Path) -> tuple[int, int, int, int] | None:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise ValueError("ledger_snapshot_source_not_regular")
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)


def _copy_file_stream(source: Path, destination: Path, expected_size: int) -> None:
    copied = 0
    with source.open("rb") as reader, destination.open("wb") as writer:
        while True:
            block = reader.read(COPY_CHUNK_BYTES)
            if not block:
                break
            copied += len(block)
            if copied > expected_size or copied > MAX_LEDGER_SNAPSHOT_BYTES:
                raise ValueError("ledger_snapshot_source_changed_or_over_limit")
            writer.write(block)
        writer.flush()
        os.fsync(writer.fileno())
    os.chmod(destination, 0o600)
    if copied != expected_size:
        raise ValueError("ledger_snapshot_incomplete_copy")


def _snapshot_ledger(source: Path, directory: Path) -> Path:
    """Copy a stable SQLite main/WAL pair without opening source via SQLite."""
    if source.is_symlink():
        raise ValueError("ledger_missing_or_not_regular")
    wal_source = Path(str(source) + "-wal")
    main_target = directory / source.name
    wal_target = Path(str(main_target) + "-wal")
    for _attempt in range(SNAPSHOT_ATTEMPTS):
        main_before = _fingerprint(source)
        if main_before is None:
            raise ValueError("ledger_missing_or_not_regular")
        wal_before = _fingerprint(wal_source)
        total = main_before[2] + (wal_before[2] if wal_before else 0)
        if total <= 0 or total > MAX_LEDGER_SNAPSHOT_BYTES:
            raise ValueError("ledger_snapshot_exceeds_size_limit")
        try:
            _copy_file_stream(source, main_target, main_before[2])
            if wal_before is not None:
                _copy_file_stream(wal_source, wal_target, wal_before[2])
            else:
                try:
                    wal_target.unlink()
                except FileNotFoundError:
                    pass
            main_after = _fingerprint(source)
            wal_after = _fingerprint(wal_source)
            if main_before == main_after and wal_before == wal_after:
                return main_target
        except OSError:
            if _fingerprint(source) != main_before or _fingerprint(wal_source) != wal_before:
                continue
            raise
        except ValueError as exc:
            if str(exc) in {"ledger_snapshot_source_changed_or_over_limit", "ledger_snapshot_incomplete_copy"}:
                if _fingerprint(source) != main_before or _fingerprint(wal_source) != wal_before:
                    continue
            raise
        for target in (main_target, wal_target):
            try:
                target.unlink()
            except FileNotFoundError:
                pass
    raise ValueError("ledger_snapshot_unstable_after_retries")


def _cache_connection(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    conn = sqlite3.connect(path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS state(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS closes(
            close_id TEXT PRIMARY KEY, source_rowid INTEGER NOT NULL,
            position_id TEXT, mint TEXT, closed_at TEXT, strategy TEXT,
            strategy_version TEXT, cohort_id TEXT, cost_cents INTEGER,
            proceeds_cents INTEGER, exit_fee_cents INTEGER, net_pnl_cents INTEGER,
            provenance TEXT, status TEXT NOT NULL, reason TEXT, features_json TEXT NOT NULL DEFAULT '{}',
            raw_sha256 TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_learning_closes_rowid ON closes(source_rowid);
    """)
    conn.commit()
    return conn


def _load_new_closes(source: sqlite3.Connection, after_rowid: int) -> tuple[list[dict[str, Any]], int, bool]:
    tables = {row[0] for row in source.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    required = {"closes", "positions", "fills", "proposals"}
    if not required.issubset(tables):
        raise ValueError("ledger_schema_incomplete")
    max_row = source.execute("SELECT COALESCE(MAX(rowid),0) FROM closes").fetchone()[0]
    rows = source.execute(
        "SELECT c.rowid AS source_rowid,c.*,p.position_id AS p_position_id,p.proposal_id AS p_proposal_id,"
        "p.desk AS p_desk,p.instrument AS p_instrument,p.exposure_group AS p_exposure_group,"
        "p.state AS p_state,p.provenance AS p_provenance, p.opened_at AS p_opened_at,"
        "f.fill_id AS f_fill_id,f.total_cost_cents AS f_cost_cents,f.fee_cents AS f_fee_cents,"
        "f.filled_at AS f_filled_at,f.provenance AS f_provenance,c.provenance AS c_provenance,"
        "pr.payload AS proposal_payload "
        "FROM closes c LEFT JOIN positions p ON p.position_id=c.position_id "
        "LEFT JOIN fills f ON f.position_id=p.position_id "
        "LEFT JOIN proposals pr ON pr.proposal_id=p.proposal_id "
        "WHERE c.rowid>? ORDER BY c.rowid LIMIT ?", (after_rowid, MAX_CLOSES_PER_REFRESH + 1)
    ).fetchall()
    overflow = len(rows) > MAX_CLOSES_PER_REFRESH
    rows = rows[:MAX_CLOSES_PER_REFRESH]
    result: list[dict[str, Any]] = []
    for row in rows:
        raw = {key: row[key] for key in row.keys()}
        signature = json.dumps(raw, sort_keys=True, default=str, separators=(",", ":"))
        result.append({"source_rowid": int(row["source_rowid"]), "close_id": row["close_id"],
                       "raw": raw, "raw_sha256": hashlib.sha256(signature.encode()).hexdigest()})
    next_watermark = result[-1]["source_rowid"] if overflow and result else int(max_row)
    return result, next_watermark, overflow


def _candidate_features(root: Path, proposal: Mapping[str, Any]) -> dict[str, Any]:
    """Read at most one exact-hash candidate file; never scan the archive."""
    metadata = proposal.get("metadata") if isinstance(proposal.get("metadata"), Mapping) else {}
    token = metadata.get("token_memecoin_v1") if isinstance(metadata, Mapping) else None
    token = token if isinstance(token, Mapping) else {}
    digest = token.get("candidate_content_sha256")
    cohort = token.get("cohort_id")
    if cohort not in {"M0", "M1", "M2", "M3", "E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8"}:
        return {"candidate_lookup": "cohort_missing_or_unknown"}
    if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest.lower()):
        return {"candidate_lookup": "hash_missing"}
    candidates_dir = root / cohort.lower() / "candidates"
    if not candidates_dir.is_dir():
        candidates_dir = root / "candidates"
    path = candidates_dir / (digest + ".json")
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_CANDIDATE_BYTES:
            return {"candidate_lookup": "missing_or_over_limit"}
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, Mapping) or value.get("content_sha256") != digest:
            return {"candidate_lookup": "hash_mismatch"}
        unhashed = dict(value)
        unhashed.pop("content_sha256", None)
        actual_digest = hashlib.sha256(json.dumps(unhashed, sort_keys=True, separators=(",", ":"),
            ensure_ascii=True, allow_nan=False).encode()).hexdigest()
        if actual_digest != digest:
            return {"candidate_lookup": "hash_mismatch"}
    except (OSError, UnicodeError, ValueError):
        return {"candidate_lookup": "unreadable_or_invalid"}
    # Retain only fixed candidate-schema bins, never the large evidence payload.
    return {"candidate_lookup": "matched", "entry_bins": _entry_feature_bins(value)}


def _evaluate_close(item: Mapping[str, Any], root: Path) -> dict[str, Any]:
    row = item["raw"]
    reason = None
    mint = _mint(row.get("p_instrument"), row.get("p_exposure_group"))
    proposal = _json_object(row.get("proposal_payload"))
    metadata = proposal.get("metadata") if isinstance(proposal.get("metadata"), Mapping) else {}
    token = metadata.get("token_memecoin_v1") if isinstance(metadata, Mapping) else None
    token = token if isinstance(token, Mapping) else {}
    strategy = proposal.get("strategy")
    strategy_version = proposal.get("strategy_version")
    cohort_id = token.get("cohort_id")
    status = "eligible"
    features = _candidate_features(root, proposal)
    close_at = None
    cost = proceeds = exit_fee = net = None
    try:
        if row.get("p_desk") != "crypto": reason = "not_crypto_position"
        elif row.get("p_state") != "closed": reason = "position_not_fully_closed"
        elif row.get("c_provenance") != "observed" or row.get("p_provenance") != "observed" or row.get("f_provenance") != "observed":
            reason = "non_observed_provenance"
        elif not row.get("f_fill_id"): reason = "fill_missing"
        elif mint is None: reason = "mint_label_missing_or_invalid"
        elif not isinstance(strategy, str) or not strategy.strip() or not isinstance(strategy_version, str) or not strategy_version.strip() or not isinstance(cohort_id, str) or not cohort_id.strip():
            reason = "strategy_or_cohort_label_missing"
        else:
            opened = _time(row.get("p_opened_at"))
            filled = _time(row.get("f_filled_at"))
            close_at_dt = _time(row.get("closed_at"))
            if filled < opened or close_at_dt < filled: reason = "invalid_lifecycle_timestamps"
            else: close_at = _iso(close_at_dt)
        if reason is None:
            cost = int(row["f_cost_cents"])
            proceeds = int(row["proceeds_cents"])
            exit_fee = int(row["fee_cents"])
            if cost <= 0 or proceeds < 0 or exit_fee < 0 or int(row["f_fee_cents"]) < 0:
                reason = "invalid_cost_or_proceeds"
            else:
                # Fill cost already includes entry fee under the paper-ledger contract.
                net = proceeds - exit_fee - cost
    except (KeyError, TypeError, ValueError, OverflowError):
        reason = "invalid_or_partial_close_record"
    if reason:
        status = "excluded"
    return {"close_id": str(item["close_id"]), "source_rowid": item["source_rowid"],
            "position_id": row.get("position_id"), "mint": mint, "closed_at": close_at,
            "strategy": strategy if isinstance(strategy, str) else None,
            "strategy_version": strategy_version if isinstance(strategy_version, str) else None,
            "cohort_id": cohort_id if isinstance(cohort_id, str) else None,
            "cost_cents": cost, "proceeds_cents": proceeds, "exit_fee_cents": exit_fee,
            "net_pnl_cents": net, "provenance": row.get("c_provenance"),
            "status": status, "reason": reason, "features_json": json.dumps(features, sort_keys=True),
            "raw_sha256": item["raw_sha256"]}


def _refresh_cache(ledger_path: Path, cache_path: Path, root: Path) -> dict[str, Any]:
    cache = _cache_connection(cache_path)
    source: sqlite3.Connection | None = None
    snapshot_dir = tempfile.TemporaryDirectory(prefix=".ledger-snapshot-", dir=cache_path.parent)
    try:
        snapshot_path = _snapshot_ledger(ledger_path, Path(snapshot_dir.name))
        source = _readonly_connection(snapshot_path)
        watermark_row = cache.execute("SELECT value FROM state WHERE key='close_rowid_watermark'").fetchone()
        watermark = int(watermark_row[0]) if watermark_row else 0
        pending, next_watermark, overflow = _load_new_closes(source, watermark)
        with cache:
            for item in pending:
                close = _evaluate_close(item, root)
                cache.execute("""INSERT INTO closes(close_id,source_rowid,position_id,mint,closed_at,
                    strategy,strategy_version,cohort_id,cost_cents,proceeds_cents,exit_fee_cents,
                    net_pnl_cents,provenance,status,reason,features_json,raw_sha256)
                    VALUES(:close_id,:source_rowid,:position_id,:mint,:closed_at,:strategy,
                    :strategy_version,:cohort_id,:cost_cents,:proceeds_cents,:exit_fee_cents,
                    :net_pnl_cents,:provenance,:status,:reason,:features_json,:raw_sha256)
                    ON CONFLICT(close_id) DO UPDATE SET source_rowid=excluded.source_rowid,
                    position_id=excluded.position_id,mint=excluded.mint,closed_at=excluded.closed_at,
                    strategy=excluded.strategy,strategy_version=excluded.strategy_version,
                    cohort_id=excluded.cohort_id,cost_cents=excluded.cost_cents,
                    proceeds_cents=excluded.proceeds_cents,exit_fee_cents=excluded.exit_fee_cents,
                    net_pnl_cents=excluded.net_pnl_cents,provenance=excluded.provenance,
                    status=excluded.status,reason=excluded.reason,features_json=excluded.features_json,
                    raw_sha256=excluded.raw_sha256""", close)
            cache.execute("INSERT INTO state(key,value) VALUES('close_rowid_watermark',?) "
                          "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(next_watermark),))
        cache.execute("PRAGMA wal_checkpoint(PASSIVE)")
        return {"status": "ok", "refresh_capped": overflow, "new_close_rows": len(pending), "watermark": next_watermark,
                "source": str(ledger_path), "source_size_bytes": ledger_path.stat().st_size,
                "source_mtime_ns": ledger_path.stat().st_mtime_ns,
                "snapshot_method": "plain main-database plus optional WAL copy; source device/inode/size/mtime_ns verified before and after; source is never opened by SQLite",
                "snapshot_max_bytes": MAX_LEDGER_SNAPSHOT_BYTES,
                "snapshot_attempts": SNAPSHOT_ATTEMPTS}
    finally:
        if source is not None:
            source.rollback()
            source.close()
        cache.close()
        snapshot_dir.cleanup()


def _clusters(records: list[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in records:
        if row["status"] == "eligible" and row["mint"]:
            grouped[str(row["mint"])].append(row)
    clusters = []
    versions: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for mint, rows in grouped.items():
        rows.sort(key=lambda row: (row["closed_at"], row["close_id"]))
        closed_at = max(row["closed_at"] for row in rows)
        pnl = sum(int(row["net_pnl_cents"]) for row in rows)
        cost = sum(int(row["cost_cents"]) for row in rows)
        fee = sum(int(row["exit_fee_cents"]) for row in rows)
        strategies = sorted({f"{row['strategy']}@{row['strategy_version']}" for row in rows})
        cluster = {"mint": mint, "closed_at": closed_at, "net_pnl_cents": pnl,
                   "cost_cents": cost, "exit_fees_cents": fee, "net_return_pct": (100 * pnl / cost if cost else None),
                   "close_records": len(rows), "strategy_versions": strategies}
        clusters.append(cluster)
        by_version: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for row in rows:
            by_version[f"{row['strategy']}@{row['strategy_version']}"].append(row)
        for version, version_rows in by_version.items():
            versions[version].append({"mint": mint, "closed_at": closed_at,
                "net_pnl_cents": sum(int(row["net_pnl_cents"]) for row in version_rows),
                "cost_cents": sum(int(row["cost_cents"]) for row in version_rows),
                "exit_fees_cents": sum(int(row["exit_fee_cents"]) for row in version_rows)})
    clusters.sort(key=lambda row: (row["closed_at"], row["mint"]))
    return clusters, versions


def _stats(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    pnl = sum(int(row["net_pnl_cents"]) for row in rows)
    cost = sum(int(row["cost_cents"]) for row in rows)
    returns = [100 * int(row["net_pnl_cents"]) / int(row["cost_cents"]) for row in rows if int(row["cost_cents"]) > 0]
    return {"independent_mint_clusters": len(rows), "net_pnl_cents": pnl,
            "cost_cents": cost, "net_return_pct_on_cost": 100 * pnl / cost if cost else None,
            "mean_cluster_return_pct": sum(returns) / len(returns) if returns else None,
            "positive_clusters": sum(1 for row in rows if int(row["net_pnl_cents"]) > 0),
            "negative_clusters": sum(1 for row in rows if int(row["net_pnl_cents"]) < 0)}


def candidate_score(candidate: Mapping[str, Any], report: Mapping[str, Any], *,
                    strategy_version: str = "e1-sized-v2", now: datetime | None = None) -> dict[str, Any]:
    """Return a weak causal ranking adjustment for an already-eligible E1 candidate.

    This function cannot admit candidates or set position size. It returns neutral
    zero unless each matched entry-feature bin has ten prior, independent E1 mint
    outcomes for the exact requested strategy version.
    """
    def neutral(reason: str, decision_at: str | None = None) -> dict[str, Any]:
        return {"score": 0.0, "status": reason, "strategy_version": strategy_version,
                "decision_at": decision_at, "matched_bins": []}

    if not isinstance(candidate, Mapping) or not isinstance(report, Mapping):
        return neutral("invalid_candidate_or_report")
    if report.get("schema_version") != REPORT_VERSION:
        return neutral("unsupported_report")
    try:
        clock_value = now or datetime.now(UTC)
        if clock_value.tzinfo is None or clock_value.utcoffset() is None:
            return neutral("invalid_clock")
        clock = clock_value.astimezone(UTC)
        report_at = _time(report.get("generated_at"))
        if report_at > clock or (clock - report_at).total_seconds() > 24 * 60 * 60:
            return neutral("stale_or_future_report")
        decision = _time(candidate.get("known_at") or candidate.get("recorded_at"))
        decision_iso = _iso(decision)
        if decision > clock:
            return neutral("future_candidate_decision", decision_iso)
    except (TypeError, ValueError, OverflowError):
        return neutral("missing_or_invalid_decision_time")
    candidate_bins = _entry_feature_bins(candidate)
    if not candidate_bins:
        return neutral("entry_features_unavailable", decision_iso)
    source_data = report.get("entry_feature_scores")
    if not isinstance(source_data, list):
        return neutral("feature_scores_unavailable", decision_iso)
    contributions = []
    for entry_bin in candidate_bins:
        group_name = "entry_bin:" + entry_bin
        group = next((item for item in source_data if isinstance(item, Mapping)
                      and item.get("strategy_version") == strategy_version
                      and item.get("cohort_id") == "E1" and item.get("group") == group_name), None)
        observations = group.get("observations") if isinstance(group, Mapping) else None
        if not isinstance(observations, list):
            return neutral("unsupported_feature_bin", decision_iso)
        prior = []
        for observation in observations:
            if not isinstance(observation, Mapping):
                continue
            try:
                if _time(observation.get("closed_at")) <= decision:
                    prior.append(observation)
            except (TypeError, ValueError):
                continue
        # One observation per mint is enforced by report construction; dedupe
        # defensively before support testing.
        unique = {str(item.get("mint")): item for item in prior if item.get("mint")}
        if len(unique) < 10:
            return neutral("insufficient_prior_bin_support", decision_iso)
        pnl = sum(int(item["net_pnl_cents"]) for item in unique.values())
        cost = sum(int(item["cost_cents"]) for item in unique.values())
        mean_return = 100 * pnl / cost if cost else 0.0
        shrink = len(unique) / (len(unique) + 20)
        # A capped 20% adjustment, shrunk both for small samples and return size.
        contribution = max(-0.2, min(0.2, 0.2 * max(-1.0, min(1.0, mean_return / 10)) * shrink))
        contributions.append({"feature": group_name, "independent_mint_clusters": len(unique),
                              "mean_net_return_pct": mean_return, "contribution": contribution})
    if not contributions:
        return neutral("no_matched_feature_bins", decision_iso)
    return {"score": max(-0.2, min(0.2, sum(item["contribution"] for item in contributions) / len(contributions))),
            "status": "scored", "strategy_version": strategy_version,
            "decision_at": decision_iso, "matched_bins": contributions,
            "use": "ranking_among_already_eligible_E1_candidates_only"}


def build_report(root: str | Path, ledger_path: str | Path | list[str | Path], output: str | Path,
                 *, now: datetime | None = None) -> dict[str, Any]:
    root_path, out = Path(root), Path(output)
    ledgers = [Path(item) for item in (ledger_path if isinstance(ledger_path, list) else [ledger_path])]
    if not ledgers or len(ledgers) > 8:
        raise ValueError("one to eight paper ledgers are required")
    cache_reports = []
    records = []
    for ledger in ledgers:
        source_key = hashlib.sha256(str(ledger.resolve()).encode()).hexdigest()[:12]
        cache_path = out / f"learning-cache-{source_key}.sqlite3"
        refresh = _refresh_cache(ledger, cache_path, root_path)
        cache_reports.append(refresh)
        cache = sqlite3.connect(f"file:{cache_path.resolve()}?mode=ro", uri=True)
        cache.row_factory = sqlite3.Row
        try:
            for row in cache.execute("SELECT * FROM closes ORDER BY source_rowid"):
                record = dict(row)
                record["close_id"] = source_key + ":" + str(record["close_id"])
                records.append(record)
        finally:
            cache.close()
    records.sort(key=lambda row: (row.get("closed_at") or "", row["close_id"]))
    valid = [row for row in records if row["status"] == "eligible"]
    clusters, version_rows = _clusters(valid)
    split_at = int(len(clusters) * 0.70)
    if clusters and split_at == len(clusters): split_at -= 1
    training = clusters[:split_at]
    validation = clusters[split_at:]
    missing_labels = sum(row["reason"] in {"strategy_or_cohort_label_missing", "mint_label_missing_or_invalid"} for row in records)
    insufficient = len(clusters) < MIN_PROMOTION_CLUSTERS or len(validation) < MIN_VALIDATION_CLUSTERS or missing_labels > 0
    train_cutoff = training[-1]["closed_at"] if training else None
    stress: dict[str, Any] = {}
    for bps in (100, 500, 1000):
        stressed = sum(int(row["net_pnl_cents"]) - int(row["cost_cents"]) * bps / 10_000 for row in validation)
        stress[str(bps)] = {"extra_cost_bps": bps, "validation_net_pnl_cents": round(stressed, 2),
                            "positive": bool(validation) and stressed > 0}
    version_report = []
    for version, rows in sorted(version_rows.items()):
        rows.sort(key=lambda row: (row["closed_at"], row["mint"]))
        train_rows = [row for row in rows if train_cutoff and row["closed_at"] <= train_cutoff]
        val_rows = [row for row in rows if train_cutoff and row["closed_at"] > train_cutoff]
        metrics = {"all": _stats(rows), "training": _stats(train_rows), "validation": _stats(val_rows)}
        version_report.append({"strategy_version": version, **metrics})
    # Retrospective partitions are diagnostic only. They cannot qualify a
    # proposal for activation; no challenger was prospectively preregistered.
    promotion = "proposal_only_no_promotion"
    challenger_specs = [
        ("smaller-size-v1", "Cap all-in paper risk at the lower of 50% of current strategy entry risk and 250 cents; keep other rules fixed.",
         {"max_all_in_risk_cents": 250, "baseline_risk_fraction": 0.50}),
        ("route-display-divergence-v1", "Record independent display and exact-size route prices prospectively; reject new candidates when implied prices diverge by more than 2%.",
         {"max_divergence_pct": 2.0, "requires_independent_display_price": True}),
        ("short-hold-15m-v1", "Compare a 15-minute maximum hold against the current version for the same candidates; leave other exit rules fixed.",
         {"max_hold_minutes": 15, "other_exit_rules": "unchanged_control"}),
        ("e1-flow-signal-v1", "For E1 controls-only candidates, prospectively test an independent momentum or flow signal; safety controls alone do not establish alpha.",
         {"signal_family": "momentum_or_flow", "safety_controls_are_alpha": False}),
    ]
    hypotheses = []
    for challenger_version, hypothesis, change in challenger_specs:
        key = hashlib.sha256(f"{REPORT_VERSION}|{challenger_version}".encode()).hexdigest()[:16]
        hypotheses.append({"proposal_id": f"challenger-{key}", "challenger_version": challenger_version,
            "status": "untested_pending_forward_validation", "hypothesis": hypothesis,
            "bounded_change": change, "admission_or_active_parameters_changed": False,
            "selection_bias_notice": "Closed-trade results are selected outcomes. Score this challenger prospectively with outcome receipts for admitted, rejected, and no-fill candidates.",
            "promotion_gate": "No promotion from this report. Require at least 30 independent closed mint clusters, preregistered post-known_at forward validation, positive results after 100/500/1000 bps additional cost stress, and rejected/no-fill outcome coverage."})
    # Score version, cohort and entry evidence groups descriptively. Collapse
    # duplicate sleeves to one summed outcome per mint before counting n.
    grouped_features: dict[str, dict[str, dict[str, dict[str, int]]]] = defaultdict(lambda: defaultdict(dict))
    for row in valid:
        features = _json_object(row.get("features_json"))
        labels = [f"strategy:{row['strategy']}@{row['strategy_version']}", f"cohort:{row['cohort_id']}"]
        entry_bins = features.get("entry_bins")
        if isinstance(entry_bins, list):
            labels.extend("entry_bin:" + label for label in entry_bins[:20]
                          if isinstance(label, str) and len(label) <= 80)
        for label in labels:
            per_mint = grouped_features[label][str(row["mint"]) ]
            per_mint["pnl"] = per_mint.get("pnl", 0) + int(row["net_pnl_cents"])
            per_mint["cost"] = per_mint.get("cost", 0) + int(row["cost_cents"])
    feature_scores = []
    for label, mints in sorted(grouped_features.items()):
        pnl = sum(values["pnl"] for values in mints.values())
        cost = sum(values["cost"] for values in mints.values())
        feature_scores.append({"group": label, "independent_mint_clusters": len(mints),
            "net_pnl_cents": pnl, "cost_cents": cost,
            "net_return_pct_on_cost": 100 * pnl / cost if cost else None,
            "status": "descriptive_insufficient_sample" if len(mints) < 3 else "descriptive_score_only"})
    e1_bins: dict[tuple[str, str], dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in valid:
        if row.get("cohort_id") != "E1":
            continue
        features = _json_object(row.get("features_json"))
        entry_bins = features.get("entry_bins")
        if not isinstance(entry_bins, list):
            continue
        for entry_bin in entry_bins[:20]:
            if not isinstance(entry_bin, str) or not entry_bin or len(entry_bin) > 80:
                continue
            key = (str(row.get("strategy_version")), "entry_bin:" + entry_bin)
            mint = str(row["mint"])
            observation = e1_bins[key].setdefault(mint, {"mint": mint, "closed_at": row["closed_at"],
                "net_pnl_cents": 0, "cost_cents": 0})
            observation["closed_at"] = max(observation["closed_at"], row["closed_at"])
            observation["net_pnl_cents"] += int(row["net_pnl_cents"])
            observation["cost_cents"] += int(row["cost_cents"])
    entry_feature_scores = []
    for (version, group), observations in sorted(e1_bins.items()):
        clusters_for_bin = list(observations.values())
        clusters_for_bin.sort(key=lambda item: (item["closed_at"], item["mint"]))
        pnl = sum(item["net_pnl_cents"] for item in clusters_for_bin)
        cost = sum(item["cost_cents"] for item in clusters_for_bin)
        entry_feature_scores.append({"strategy_version": version, "cohort_id": "E1", "group": group,
            "independent_mint_clusters": len(clusters_for_bin), "net_pnl_cents": pnl, "cost_cents": cost,
            "mean_net_return_pct": 100 * pnl / cost if cost else None,
            "observations": clusters_for_bin,
            "status": "descriptive_only; prospective scorer requires 10 prior clusters"})
    generated = _iso(now)
    out.mkdir(parents=True, exist_ok=True, mode=0o700)
    registry = sqlite3.connect(out / "learning-proposal-registry.sqlite3")
    try:
        registry.execute("CREATE TABLE IF NOT EXISTS proposals(proposal_id TEXT PRIMARY KEY, first_seen_at TEXT NOT NULL)")
        with registry:
            for proposal in hypotheses:
                registry.execute("INSERT OR IGNORE INTO proposals(proposal_id,first_seen_at) VALUES(?,?)",
                                 (proposal["proposal_id"], generated))
        first_seen = {row[0]: row[1] for row in registry.execute("SELECT proposal_id,first_seen_at FROM proposals")}
        for proposal in hypotheses:
            proposal["known_at"] = first_seen[proposal["proposal_id"]]
    finally:
        registry.close()
    report = {"schema_version": REPORT_VERSION, "generated_at": generated,
        "status": "insufficient_evidence" if insufficient else "evaluated",
        "source": {"ledgers": cache_reports, "read_only": True, "root": str(root_path)},
        "policy": {"eligible_records": "closed crypto positions with observed close, position, and fill provenance; valid mint and strategy/cohort labels",
            "net_pnl": "exit proceeds minus exit fee minus fill total cost; fill total cost includes entry fees",
            "cluster_key": "Solana mint across sleeves; every trade for a mint stays in one chronological partition",
            "cross_sleeve_accounting": "mint-cluster P&L sums across sleeves are diagnostic outcome weights, not firm portfolio P&L; isolated paper ledgers do not imply combined capital",
            "chronological_split": "retrospective diagnostic only: oldest 70% of unique mint clusters vs newest 30%; not untouched validation",
            "minimum_clusters_for_manual_review": MIN_PROMOTION_CLUSTERS, "minimum_validation_clusters": MIN_VALIDATION_CLUSTERS,
            "promotion_policy": "always proposal-only in v1; challenger must have a preregistered known_at before any scored outcome",
            "extra_cost_stress_bps": [100, 500, 1000], "active_parameters_changed": False},
        "counts": {"closed_rows_cached": len(records), "eligible_observed_close_rows": len(valid),
            "unique_mint_clusters": len(clusters), "synthetic_or_unobserved_excluded": sum(row["reason"] == "non_observed_provenance" for row in records),
            "missing_label_rows": missing_labels, "other_excluded_rows": sum(row["status"] == "excluded" and row["reason"] != "non_observed_provenance" for row in records),
            "new_rows_this_refresh": sum(item.get("new_close_rows", 0) for item in cache_reports)},
        "chronological_evaluation": {"training": _stats(training), "validation": _stats(validation),
            "training_through": train_cutoff, "validation_from": validation[0]["closed_at"] if validation else None,
            "validation_cost_stress": stress, "validation_kind": "retrospective_diagnostic_split",
            "retrospective_holdout_clusters": len(validation)}, "promotion_recommendation": promotion,
        "strategy_versions": version_report, "feature_scores": feature_scores,
        "entry_feature_scores": entry_feature_scores, "challenger_proposals": hypotheses,
        "excluded_close_records": [{"close_id": row["close_id"], "reason": row["reason"], "provenance": row["provenance"]}
                                    for row in records if row["status"] != "eligible"],
        "mint_clusters": clusters}
    _atomic_write(out / "learning.json", json.dumps(report, sort_keys=True, indent=2, allow_nan=False) + "\n")
    markdown = ["# Memecoin paper learning report", "", f"Generated: {generated}", "",
        f"Status: **{report['status']}**. Manual review gate: **{promotion}**.", "",
        f"Observed close rows: {len(valid)}; independent mint clusters: {len(clusters)}; excluded closes: {len(records)-len(valid)}.",
        f"Training clusters: {len(training)}; retrospective diagnostic holdout clusters: {len(validation)}.", "",
        "Net returns include recorded entry and exit fees. Synthetic, incomplete, malformed, and unattributed closes are excluded and listed in the JSON report.",
        "The chronological split is retrospective and diagnostic. Closed trades have selection bias; rejected and no-fill candidates need prospective outcome receipts.",
        "Cross-sleeve mint totals are diagnostic outcome weights, not firm portfolio P&L; isolated paper accounts are not combined capital.",
        "No active strategy settings changed. Challenger items are untested proposals only.", ""]
    _atomic_write(out / "learning.md", "\n".join(markdown))
    return report


def _atomic_write(path: Path, content: str) -> None:
    if len(content.encode()) > MAX_OUTPUT_BYTES:
        raise ValueError("learning report exceeds output size limit")
    fd, name = tempfile.mkstemp(prefix="." + path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(name, 0o600)
        os.replace(name, path)
    finally:
        try: os.unlink(name)
        except FileNotFoundError: pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default="/var/lib/neorome-memecoin-program", help="read-only memecoin paper evidence root")
    parser.add_argument("--ledger", action="append", default=None, help="paper ledger path (repeat for separate paper ledgers)")
    parser.add_argument("--output", default="/var/lib/neorome-memecoin-program/learning")
    args = parser.parse_args(argv)
    try:
        ledgers = args.ledger or [
            "/var/lib/neorome-memecoin-program/paper.sqlite3",
            "/var/lib/neorome-memecoin-program/paper-e1.sqlite3",
            "/var/lib/neorome-memecoin-program/paper-e2.sqlite3",
            "/var/lib/neorome-memecoin-program/paper-e4.sqlite3",
            "/var/lib/neorome-memecoin-program/paper-e5.sqlite3",
            "/var/lib/neorome-memecoin-program/paper-e6.sqlite3",
            "/var/lib/neorome-memecoin-program/paper-e7.sqlite3",
            "/var/lib/neorome-memecoin-program/paper-e8.sqlite3",
        ]
        report = build_report(args.root, ledgers, args.output)
    except (OSError, sqlite3.Error, ValueError) as exc:
        print(json.dumps({"schema_version": REPORT_VERSION, "status": "failed_closed", "error": str(exc),
                          "active_parameters_changed": False}, sort_keys=True))
        return 1
    print(json.dumps({"status": report["status"], "promotion_recommendation": report["promotion_recommendation"],
                      "unique_mint_clusters": report["counts"]["unique_mint_clusters"],
                      "new_rows_this_refresh": report["counts"]["new_rows_this_refresh"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
