from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from neorome.equity_intraday_sip import SIPCaptureError, load_frozen_universe
from neorome.equity_intraday_watchlist import WatchlistError, generate, generate_daily


UTC = timezone.utc
SELECTION = datetime(2026, 9, 24, 12, 45, tzinfo=UTC)  # 08:45 New York
SIGNAL = datetime(2026, 9, 24, 13, 46, tzinfo=UTC)


class ProspectiveWatchlistTests(unittest.TestCase):
    def test_generated_watchlist_is_content_addressed_and_admits_only_prospective_membership(self):
        with TemporaryDirectory() as root:
            path = Path(root) / "universe.json"
            generated = generate(path, session_date="2026-09-24", selection_at=SELECTION)
            loaded = load_frozen_universe(path, signal_at=SIGNAL, symbol="MU")
            self.assertEqual(loaded["method"], "prospective_fixed_watchlist")
            self.assertFalse(loaded["historical_claim"])
            self.assertEqual(generated["research_label"], "prospective_fixed_watchlist_evidence_only")

    def test_generator_and_loader_refuse_post_open_or_tampered_membership(self):
        with TemporaryDirectory() as root:
            path = Path(root) / "universe.json"
            with self.assertRaises(WatchlistError):
                generate(path, session_date="2026-09-24", selection_at="2026-09-24T13:31:00Z")
            generate(path, session_date="2026-09-24", selection_at=SELECTION)
            value = path.read_text().replace("static-us-equity:MU", "tampered-id")
            path.unlink(); path.write_text(value)
            with self.assertRaisesRegex(SIPCaptureError, "content-addressed"):
                load_frozen_universe(path, signal_at=SIGNAL, symbol="MU")

    def test_daily_artifacts_roll_over_immutably_and_current_manifest_binds_session(self):
        with TemporaryDirectory() as root:
            root = Path(root)
            first = generate_daily(root, session_date="2026-09-24", selection_at=SELECTION)
            first_path = Path(first["artifact_path"])
            first_bytes = first_path.read_bytes()
            # A restart never changes an already frozen same-day artifact.
            restarted = generate_daily(root, session_date="2026-09-24", selection_at="2026-09-24T12:50:00Z")
            self.assertEqual(restarted["selection_at"], first["selection_at"])
            self.assertEqual(first_path.read_bytes(), first_bytes)

            next_signal = datetime(2026, 9, 25, 13, 46, tzinfo=UTC)
            second = generate_daily(root, session_date="2026-09-25", selection_at="2026-09-25T12:45:00Z")
            self.assertTrue(Path(second["artifact_path"]).is_file())
            self.assertEqual(first_path.read_bytes(), first_bytes)
            current = root / "frozen-universe-current.json"
            self.assertEqual(load_frozen_universe(current, signal_at=next_signal, symbol="MU")["session_date"], "2026-09-25")
            with self.assertRaisesRegex(SIPCaptureError, "bind signal session"):
                load_frozen_universe(current, signal_at=SIGNAL, symbol="MU")

    def test_daily_publisher_refuses_a_symlinked_current_manifest(self):
        with TemporaryDirectory() as root:
            root = Path(root)
            (root / "frozen-universe-current.json").symlink_to(root / "elsewhere.json")
            with self.assertRaisesRegex(WatchlistError, "current watchlist manifest"):
                generate_daily(root, session_date="2026-09-24", selection_at=SELECTION)


if __name__ == "__main__": unittest.main()
