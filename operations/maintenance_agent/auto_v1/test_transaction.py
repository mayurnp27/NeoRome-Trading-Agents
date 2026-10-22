from __future__ import annotations

import base64
import hashlib
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from operations.maintenance_agent.auto_v1 import transaction as tx


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def encoded(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


class TransactionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        # macOS may expose the temporary directory through /var -> /private/var.
        self.root = Path(self.temp.name).resolve()
        self.prod = self.root / "production"
        self.prod.mkdir()
        self.stage = self.root / "stage"
        self.stage.mkdir()
        self.first = self.prod / "alpha.py"
        self.second = self.prod / "beta.py"
        self.first.write_bytes(b"VALUE = 1\n")
        self.second.write_bytes(b"VALUE = 2\n")
        self.profile = {"files": {"alpha.py": str(self.first), "beta.py": str(self.second)}}
        self.candidates = {"alpha.py": b"VALUE = 10\n", "beta.py": b"VALUE = 20\n"}

    def tearDown(self) -> None:
        self.temp.cleanup()

    def freeze(self, candidates: dict[str, bytes] | None = None):
        candidates = candidates or self.candidates
        return tx.freeze_candidate(
            self.profile,
            {name: encoded(content) for name, content in candidates.items()},
            {name: digest((self.prod / name).read_bytes()) for name in candidates},
            self.stage,
        )

    def test_freeze_and_install_preserve_file_metadata(self) -> None:
        os.chmod(self.first, 0o640)
        before = self.first.stat()
        manifest = self.freeze({"alpha.py": self.candidates["alpha.py"]})
        frozen = Path(manifest["stage"]) / manifest["transaction_id"] / "original-0000.bin"
        self.assertEqual(frozen.read_bytes(), b"VALUE = 1\n")
        result = tx.install(manifest)
        after = self.first.stat()
        self.assertEqual(result["state"], "installed")
        self.assertEqual(self.first.read_bytes(), self.candidates["alpha.py"])
        self.assertEqual(after.st_mode & 0o777, before.st_mode & 0o777)
        self.assertEqual((after.st_uid, after.st_gid), (before.st_uid, before.st_gid))

    def test_bad_base64_and_compile_error_are_rejected(self) -> None:
        with self.assertRaises(tx.TransactionError):
            tx.freeze_candidate(self.profile, {"alpha.py": "%%%"}, {"alpha.py": digest(self.first.read_bytes())}, self.stage)
        with self.assertRaisesRegex(tx.TransactionError, "does not compile"):
            self.freeze({"alpha.py": b"def unfinished(:\n"})
        self.assertEqual(self.first.read_bytes(), b"VALUE = 1\n")

    def test_path_traversal_unknown_and_new_destination_are_rejected(self) -> None:
        for rel in ("../alpha.py", "/alpha.py", "nested/../../alpha.py", "alpha.txt"):
            with self.subTest(rel=rel), self.assertRaises(tx.TransactionError):
                tx.freeze_candidate(self.profile, {rel: encoded(b"x=1\n")}, {rel: digest(b"x")}, self.stage)
        self.profile["files"]["new.py"] = str(self.prod / "new.py")
        with self.assertRaises(tx.TransactionError):
            tx.freeze_candidate(self.profile, {"new.py": encoded(b"x=1\n")}, {"new.py": digest(b"")}, self.stage)
        self.assertFalse((self.prod / "new.py").exists())

    def test_size_limit_and_duplicate_target_are_rejected(self) -> None:
        with self.assertRaisesRegex(tx.TransactionError, "size limit"):
            tx.freeze_candidate({**self.profile, "max_bytes": 3}, {"alpha.py": encoded(b"x = 123\n")}, {"alpha.py": digest(self.first.read_bytes())}, self.stage)
        duplicate = {"files": {"alpha.py": str(self.first), "alias.py": str(self.first)}}
        with self.assertRaisesRegex(tx.TransactionError, "same target"):
            tx.freeze_candidate(duplicate, {"alpha.py": encoded(b"x=1\n"), "alias.py": encoded(b"x=2\n")}, {"alpha.py": digest(self.first.read_bytes()), "alias.py": digest(self.first.read_bytes())}, self.stage)

    def test_symlink_and_hardlink_targets_are_rejected(self) -> None:
        linked = self.prod / "linked.py"
        linked.symlink_to(self.first)
        self.profile["files"]["alpha.py"] = str(linked)
        with self.assertRaises(tx.TransactionError):
            self.freeze({"alpha.py": b"VALUE = 3\n"})
        self.profile["files"]["alpha.py"] = str(self.first)
        alias = self.prod / "alias.py"
        os.link(self.first, alias)
        with self.assertRaisesRegex(tx.TransactionError, "hard-linked"):
            self.freeze({"alpha.py": b"VALUE = 3\n"})

    def test_stage_symlink_ancestor_is_rejected(self) -> None:
        real = self.root / "real-stage"
        real.mkdir()
        link = self.root / "stage-link"
        link.symlink_to(real, target_is_directory=True)
        with self.assertRaises(tx.TransactionError):
            tx.freeze_candidate(self.profile, {"alpha.py": encoded(b"x=1\n")}, {"alpha.py": digest(self.first.read_bytes())}, link)

    def test_baseline_mismatch_is_rejected_at_freeze_and_install(self) -> None:
        with self.assertRaisesRegex(tx.TransactionError, "baseline hash"):
            tx.freeze_candidate(self.profile, {"alpha.py": encoded(b"x=1\n")}, {"alpha.py": "0" * 64}, self.stage)
        manifest = self.freeze({"alpha.py": self.candidates["alpha.py"]})
        self.first.write_bytes(b"VALUE = concurrent\n")
        with self.assertRaisesRegex(tx.TransactionError, "compare-and-swap"):
            tx.install(manifest)
        self.assertEqual(self.first.read_bytes(), b"VALUE = concurrent\n")

    def test_all_destinations_are_checked_before_any_install(self) -> None:
        manifest = self.freeze()
        self.second.write_bytes(b"external edit\n")
        with self.assertRaisesRegex(tx.TransactionError, "compare-and-swap"):
            tx.install(manifest)
        self.assertEqual(self.first.read_bytes(), b"VALUE = 1\n")

    def test_failed_second_replacement_rolls_back_first(self) -> None:
        manifest = self.freeze()
        replace = tx._replace_target
        calls = 0

        def fail_once(item, data, txid):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError("injected second replacement failure")
            return replace(item, data, txid)

        with mock.patch.object(tx, "_replace_target", side_effect=fail_once):
            with self.assertRaisesRegex(OSError, "injected"):
                tx.install(manifest)
        self.assertEqual(self.first.read_bytes(), b"VALUE = 1\n")
        self.assertEqual(self.second.read_bytes(), b"VALUE = 2\n")

    def test_crash_after_replacement_can_be_recovered(self) -> None:
        manifest = self.freeze()
        replace = tx._replace_target
        calls = 0

        def interrupt_after_replace(item, data, txid):
            nonlocal calls
            calls += 1
            replace(item, data, txid)
            if calls == 1:
                raise KeyboardInterrupt()

        with mock.patch.object(tx, "_replace_target", side_effect=interrupt_after_replace):
            with self.assertRaises(KeyboardInterrupt):
                tx.install(manifest)
        self.assertEqual(self.first.read_bytes(), self.candidates["alpha.py"])
        result = tx.recover_transaction(manifest)
        self.assertEqual(result["state"], "rolled_back")
        self.assertEqual(self.first.read_bytes(), b"VALUE = 1\n")
        self.assertEqual(self.second.read_bytes(), b"VALUE = 2\n")

    def test_rollback_refuses_to_clobber_concurrent_edit(self) -> None:
        manifest = self.freeze({"alpha.py": self.candidates["alpha.py"]})
        tx.install(manifest)
        self.first.write_bytes(b"external edit after deployment\n")
        with self.assertRaisesRegex(tx.TransactionError, "concurrently"):
            tx.rollback(manifest)
        self.assertEqual(self.first.read_bytes(), b"external edit after deployment\n")


if __name__ == "__main__":
    unittest.main()
