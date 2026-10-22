from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import shutil
import tempfile
import unittest
from unittest import mock

from neorome.paper_ledger import PaperLedger, Proposal
import neorome.memecoin_learning as learning
from neorome.memecoin_learning import _readonly_connection, _snapshot_ledger, build_report, candidate_score

UTC = timezone.utc


class MemecoinLearningTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.ledger_path = self.base / "paper.sqlite3"
        self.root = self.base / "program"
        self.root.mkdir()
        (self.root / "candidates").mkdir()
        self.output = self.base / "learning"
        self.clock = [datetime(2026, 1, 1, tzinfo=UTC)]
        self.book = PaperLedger(self.ledger_path, max_source_age_seconds=10_000_000,
                                clock=lambda: self.clock[0])

    def tearDown(self):
        self.temp.cleanup()

    @staticmethod
    def mint(index: int) -> str:
        return "M" + f"{index:019d}"

    def close_trade(self, index: int, *, mint: str | None = None, cohort: str = "E4",
                    provenance: str = "observed", strategy: str = "continuation",
                    cost: int = 100, entry_fee: int = 20, proceeds: int = 180,
                    exit_fee: int = 10, with_candidate: bool = False) -> str:
        when = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(days=index)
        self.clock[0] = when
        mint = mint or self.mint(index)
        candidate_hash = None
        if with_candidate:
            candidate = {"schema_version": 1, "source_kind": "observed", "known_at": when.isoformat(),
                         "pool": {"age_hours_at_observation": 3, "liquidity_usd": 20_000,
                                  "buys_h1": 10, "sells_h1": 5},
                         "account_concentration": {"largest_account_supply_fraction": 0.2}}
            candidate_hash = hashlib.sha256(json.dumps(candidate, sort_keys=True, separators=(",", ":"),
                                                ensure_ascii=True).encode()).hexdigest()
            candidate["content_sha256"] = candidate_hash
            (self.root / "candidates" / (candidate_hash + ".json")).write_text(json.dumps(candidate))
        metadata = {"token_memecoin_v1": {"cohort_id": cohort,
                    "candidate_content_sha256": candidate_hash,
                    "sizing_policy": "full_available_paper_balance_less_entry_costs"}}
        proposal = Proposal(
            proposal_id=f"proposal-{index}-{cohort}-{strategy}", desk="crypto",
            strategy=strategy, strategy_version="v1", source_hash=f"{index:064x}",
            source_timestamp=when, instrument="solana:" + mint,
            exposure_group="token:solana:" + mint, idea_id=f"idea-{index}-{cohort}-{strategy}",
            expiry_at=when + timedelta(days=1), quantity=1, unit_cost_cents=cost,
            fee_cents=entry_fee, provenance=provenance, metadata=metadata)
        reservation = self.book.reserve(proposal, now=when)
        fill = self.book.simulate_fill(reservation.reservation_id, fill_id=f"fill-{index}-{cohort}-{strategy}",
            provenance=provenance, filled_at=when)
        close = self.book.close(fill.position_id, close_id=f"close-{index}-{cohort}-{strategy}",
            proceeds_cents=proceeds, fee_cents=exit_fee, provenance=provenance,
            closed_at=when + timedelta(minutes=1))
        self.clock[0] = when + timedelta(minutes=2)
        return fill.position_id

    def test_observed_only_fee_net_and_mint_dedup_incremental_and_read_only(self):
        shared_mint = self.mint(7)
        self.close_trade(0, mint=shared_mint, cohort="E6", with_candidate=True)
        self.close_trade(1, mint=shared_mint, cohort="E7", strategy="swing")
        self.close_trade(2, provenance="synthetic")
        ledger_before = hashlib.sha256(self.ledger_path.read_bytes()).hexdigest()
        first = build_report(self.root, self.ledger_path, self.output,
                             now=datetime(2026, 9, 30, tzinfo=UTC))
        second = build_report(self.root, self.ledger_path, self.output,
                              now=datetime(2026, 9, 30, 1, tzinfo=UTC))
        self.assertEqual(first["counts"]["unique_mint_clusters"], 1)
        self.assertEqual(first["counts"]["synthetic_or_unobserved_excluded"], 1)
        self.assertEqual(first["mint_clusters"][0]["close_records"], 2)
        # (180 - 10) - (100 + 20) for each observed trade.
        self.assertEqual(first["mint_clusters"][0]["net_pnl_cents"], 100)
        self.assertEqual(second["counts"]["new_rows_this_refresh"], 0)
        self.assertEqual(hashlib.sha256(self.ledger_path.read_bytes()).hexdigest(), ledger_before)
        self.assertEqual(first["promotion_recommendation"], "proposal_only_no_promotion")
        self.assertEqual(first["challenger_proposals"][0]["known_at"],
                         second["challenger_proposals"][0]["known_at"])
        self.assertTrue((self.output / "learning.json").is_file())
        self.assertTrue((self.output / "learning.md").is_file())
        self.assertTrue(any(score["group"] == "cohort:E6" for score in first["feature_scores"]))

    def test_chronological_partition_counts_unique_mints_and_is_marked_retrospective(self):
        duplicate = self.mint(5)
        for i in range(40):
            self.close_trade(i, mint=duplicate if i == 5 else None,
                             cohort="E6" if i == 5 else "E4", with_candidate=i == 0)
        # second sleeve for one mint increases trade rows, not independent clusters
        self.close_trade(40, mint=duplicate, cohort="E7", strategy="runner")
        report = build_report(self.root, self.ledger_path, self.output,
                              now=datetime(2026, 9, 30, tzinfo=UTC))
        evaluation = report["chronological_evaluation"]
        self.assertEqual(report["counts"]["unique_mint_clusters"], 40)
        self.assertEqual(evaluation["retrospective_holdout_clusters"], 12)
        self.assertEqual(evaluation["validation_kind"], "retrospective_diagnostic_split")
        self.assertLess(evaluation["training_through"], evaluation["validation_from"])
        self.assertEqual(report["promotion_recommendation"], "proposal_only_no_promotion")
        self.assertTrue(all(item["status"] == "untested_pending_forward_validation"
                            for item in report["challenger_proposals"]))
        self.assertTrue(any(item["challenger_version"] == "e1-flow-signal-v1"
                            for item in report["challenger_proposals"]))

    def test_missing_labels_and_position_open_after_close_are_excluded(self):
        position = self.close_trade(0, cohort="E4")
        with sqlite3.connect(self.ledger_path) as conn:
            conn.execute("UPDATE positions SET state='open' WHERE position_id=?", (position,))
        report = build_report(self.root, self.ledger_path, self.output)
        self.assertEqual(report["counts"]["unique_mint_clusters"], 0)
        self.assertEqual(report["counts"]["other_excluded_rows"], 1)
        self.assertEqual(report["excluded_close_records"][0]["reason"], "position_not_fully_closed")

    def test_invalid_timestamp_missing_strategy_label_and_partial_state_are_preserved(self):
        partial_position = self.close_trade(0)
        invalid_position = self.close_trade(1)
        unlabeled_position = self.close_trade(2)
        with sqlite3.connect(self.ledger_path) as conn:
            conn.execute("UPDATE positions SET state='open' WHERE position_id=?", (partial_position,))
            conn.execute("UPDATE closes SET closed_at='not-a-time' WHERE position_id=?", (invalid_position,))
            row = conn.execute("SELECT proposal_id FROM positions WHERE position_id=?", (unlabeled_position,)).fetchone()
            payload = json.loads(conn.execute("SELECT payload FROM proposals WHERE proposal_id=?", (row[0],)).fetchone()[0])
            payload["metadata"] = {}
            conn.execute("UPDATE proposals SET payload=? WHERE proposal_id=?",
                         (json.dumps(payload), row[0]))
        report = build_report(self.root, self.ledger_path, self.output)
        reasons = {item["reason"] for item in report["excluded_close_records"]}
        self.assertEqual(report["counts"]["unique_mint_clusters"], 0)
        self.assertEqual(report["counts"]["missing_label_rows"], 1)
        self.assertTrue({"position_not_fully_closed", "invalid_or_partial_close_record",
                         "strategy_or_cohort_label_missing"}.issubset(reasons))

    def test_multi_ledger_refresh_clusters_same_mint_across_sources(self):
        shared_mint = self.mint(55)
        self.close_trade(0, mint=shared_mint, cohort="E6")
        other_path = self.base / "paper-options.sqlite3"
        other_book = PaperLedger(other_path, max_source_age_seconds=10_000_000,
                                 clock=lambda: self.clock[0])
        saved_book, self.book = self.book, other_book
        self.close_trade(1, mint=shared_mint, cohort="E8", strategy="runner")
        self.book = saved_book
        report = build_report(self.root, [self.ledger_path, other_path], self.output)
        self.assertEqual(report["counts"]["unique_mint_clusters"], 1)
        self.assertEqual(report["mint_clusters"][0]["close_records"], 2)
        self.assertEqual(len(report["source"]["ledgers"]), 2)

    def test_candidate_score_requires_fresh_exact_version_e1_bins_and_prior_labels(self):
        now = datetime(2026, 9, 30, 12, tzinfo=UTC)
        decision = now - timedelta(minutes=1)
        observations = []
        for i in range(9):
            observations.append({"mint": self.mint(i), "closed_at": (decision - timedelta(days=2)).isoformat(),
                                 "net_pnl_cents": 10, "cost_cents": 100})
        # Future closes must not raise support to ten, even though present in report.
        for i in range(9, 30):
            observations.append({"mint": self.mint(i), "closed_at": (decision + timedelta(minutes=2)).isoformat(),
                                 "net_pnl_cents": 1000, "cost_cents": 100})
        candidate = {"known_at": decision.isoformat(), "pool": {"liquidity_usd": 20_000}}
        matching_bin = "entry_bin:liquidity_usd:5-25k"
        report = {"schema_version": "memecoin-learning-v1", "generated_at": now.isoformat(),
                  "entry_feature_scores": [{"strategy_version": "e1-sized-v2", "cohort_id": "E1",
                      "group": matching_bin, "observations": observations}]}
        score = candidate_score(candidate, report, now=now)
        self.assertEqual(score["score"], 0.0)
        self.assertEqual(score["status"], "insufficient_prior_bin_support")
        observations.append({"mint": self.mint(99), "closed_at": (decision - timedelta(days=1)).isoformat(),
                             "net_pnl_cents": 10, "cost_cents": 100})
        score = candidate_score(candidate, report, now=now)
        self.assertEqual(score["status"], "scored")
        self.assertEqual(score["matched_bins"][0]["independent_mint_clusters"], 10)
        stale = candidate_score(candidate, report, now=now + timedelta(days=2))
        self.assertEqual(stale["status"], "stale_or_future_report")
        wrong_version = candidate_score(candidate, report, strategy_version="e1-v1", now=now)
        self.assertEqual(wrong_version["status"], "unsupported_feature_bin")

    def test_snapshot_reads_wal_without_source_shm_or_source_writes(self):
        writer_path = self.base / "writer.sqlite3"
        source_dir = self.base / "readonly-source"
        source_dir.mkdir()
        writer = sqlite3.connect(writer_path)
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("CREATE TABLE evidence(value INTEGER)")
        writer.commit()
        source = source_dir / "paper.sqlite3"
        shutil.copyfile(writer_path, source)
        writer.execute("INSERT INTO evidence(value) VALUES(42)")
        writer.commit()
        shutil.copyfile(str(writer_path) + "-wal", str(source) + "-wal")
        writer.close()
        self.assertFalse(Path(str(source) + "-shm").exists())
        source_hashes = {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                         for path in (source, Path(str(source) + "-wal"))}
        source.chmod(0o444)
        Path(str(source) + "-wal").chmod(0o444)
        source_dir.chmod(0o555)
        snapshot_dir = self.base / "writable-learning-snapshot"
        snapshot_dir.mkdir()
        try:
            snapshot = _snapshot_ledger(source, snapshot_dir)
            reader = _readonly_connection(snapshot)
            try:
                self.assertEqual(reader.execute("SELECT value FROM evidence").fetchone()[0], 42)
                with self.assertRaises(sqlite3.OperationalError):
                    reader.execute("INSERT INTO evidence(value) VALUES(7)")
            finally:
                reader.rollback()
                reader.close()
        finally:
            source_dir.chmod(0o755)
        self.assertFalse(Path(str(source) + "-shm").exists())
        self.assertEqual(source_hashes,
                         {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                          for path in (source, Path(str(source) + "-wal"))})

    def test_snapshot_fails_closed_after_three_unstable_copies_and_caps_bytes(self):
        snapshot_dir = self.base / "unstable-snapshot"
        snapshot_dir.mkdir()
        original_copy = learning._copy_file_stream
        calls = []

        def mutate_after_copy(source, destination, expected_size):
            original_copy(source, destination, expected_size)
            calls.append(source)
            if source == self.ledger_path:
                info = source.stat()
                os.utime(source, ns=(info.st_atime_ns, info.st_mtime_ns + 1_000_000))

        with mock.patch.object(learning, "_copy_file_stream", side_effect=mutate_after_copy):
            with self.assertRaisesRegex(ValueError, "ledger_snapshot_unstable_after_retries"):
                learning._snapshot_ledger(self.ledger_path, snapshot_dir)
        self.assertEqual(sum(path == self.ledger_path for path in calls), 3)
        with mock.patch.object(learning, "MAX_LEDGER_SNAPSHOT_BYTES", 1):
            with mock.patch.object(learning, "_copy_file_stream") as copy_spy:
                with self.assertRaisesRegex(ValueError, "ledger_snapshot_exceeds_size_limit"):
                    learning._snapshot_ledger(self.ledger_path, snapshot_dir)
                copy_spy.assert_not_called()


if __name__ == "__main__":
    unittest.main()
