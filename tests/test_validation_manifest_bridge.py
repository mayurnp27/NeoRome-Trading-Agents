from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest

from neorome.validation_manifest_bridge import STATUS_SCHEMA, ValidationManifestBridge
from neorome.validation_scheduler import load_manifest


def _current(*, input_sha256: str, boundary_at: str = "2026-09-23T20:00:00Z") -> dict:
    return {
        "input": "input.json",
        "input_sha256": input_sha256,
        "boundary_at": boundary_at,
        "validator": "desks.crypto.walkforward",
        "schedule": "daily",
        "config": {},
    }


class ValidationManifestBridgeTests(unittest.TestCase):
    def _crypto_ready(self, state: Path, payload: bytes = b'{"bars": []}') -> str:
        inbox = state / "inbox" / "crypto"
        inbox.mkdir(parents=True)
        (inbox / "input.json").write_bytes(payload)
        digest = hashlib.sha256(payload).hexdigest()
        (inbox / "current.json").write_text(json.dumps(_current(input_sha256=digest)), encoding="utf-8")
        return digest

    def test_one_ready_desk_is_retained_and_scheduler_compatible(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            digest = self._crypto_ready(state)
            result = ValidationManifestBridge(state).publish()
            self.assertEqual(result["status"], "published")
            self.assertEqual(result["desks"]["crypto"]["status"], "ready")
            manifest = json.loads((state / "manifest.json").read_text())
            self.assertEqual(set(manifest["desks"]), {"crypto"})
            self.assertEqual(manifest["desks"]["crypto"]["input"], f"crypto/{digest}.json")
            loaded = load_manifest(state / "manifest.json", state / "inputs")
            self.assertEqual(loaded.run_id, manifest["run_id"])

    def test_missing_desks_are_recorded_and_omitted(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            self._crypto_ready(state)
            ValidationManifestBridge(state).publish()
            status = json.loads((state / "publisher-status.json").read_text())
            self.assertEqual(status["schema_version"], STATUS_SCHEMA)
            self.assertEqual(status["desks"]["crypto"]["status"], "ready")
            self.assertIn("desk_inbox_missing", status["desks"]["equities"]["blockers"])
            self.assertNotIn("equities", json.loads((state / "manifest.json").read_text())["desks"])

    def test_hash_mismatch_blocks_only_that_desk(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            inbox = state / "inbox" / "crypto"
            inbox.mkdir(parents=True)
            (inbox / "input.json").write_text("{}", encoding="utf-8")
            (inbox / "current.json").write_text(json.dumps(_current(input_sha256="0" * 64)), encoding="utf-8")
            result = ValidationManifestBridge(state).publish()
            self.assertEqual(result["status"], "data_not_ready")
            self.assertIn("input_hash_mismatch", result["desks"]["crypto"]["blockers"])
            self.assertFalse((state / "manifest.json").exists())

    def test_symlink_current_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            inbox = state / "inbox" / "crypto"
            inbox.mkdir(parents=True)
            payload = b"{}"
            (inbox / "input.json").write_bytes(payload)
            target = state / "elsewhere.json"
            target.write_text(json.dumps(_current(input_sha256=hashlib.sha256(payload).hexdigest())), encoding="utf-8")
            os.symlink(target, inbox / "current.json")
            result = ValidationManifestBridge(state).publish()
            self.assertIn("current_symlink", result["desks"]["crypto"]["blockers"])
            self.assertEqual(result["status"], "data_not_ready")

    def test_repeated_publication_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            self._crypto_ready(state)
            bridge = ValidationManifestBridge(state)
            first = bridge.publish()
            manifest_bytes = (state / "manifest.json").read_bytes()
            status_bytes = (state / "publisher-status.json").read_bytes()
            second = bridge.publish()
            self.assertEqual(first, second)
            self.assertEqual((state / "manifest.json").read_bytes(), manifest_bytes)
            self.assertEqual((state / "publisher-status.json").read_bytes(), status_bytes)

    def test_zero_ready_preserves_prior_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            state.mkdir(exist_ok=True)
            prior = b'{"prior":"manifest"}\n'
            (state / "manifest.json").write_bytes(prior)
            result = ValidationManifestBridge(state).publish()
            self.assertEqual(result["status"], "data_not_ready")
            self.assertEqual((state / "manifest.json").read_bytes(), prior)


if __name__ == "__main__":
    unittest.main()
