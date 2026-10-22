from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest

from neorome.options_dated_artifacts import save_stage
from neorome.options_trend_capture import TrendCaptureError, collect_paired, current_pair
from tests.test_options_trend_inputs import ARCHIVE, _qqq_archive

UTC = timezone.utc
NOW = datetime(2026, 9, 23, 14, 0, tzinfo=UTC)


class OptionsTrendCaptureTests(unittest.TestCase):
    def test_exactly_two_five_get_archives_are_retained_with_no_orders(self):
        spy, qqq = json.loads(ARCHIVE.read_text()), _qqq_archive()
        def collector(symbol, _clock):
            bundle = spy["bundle"] if symbol == "SPY" else qqq["bundle"]
            return deepcopy(bundle["context"]), deepcopy(bundle["observation"])
        paired = collect_paired(collector=collector, clock=lambda: NOW)
        self.assertEqual(paired["symbols"], ["SPY", "QQQ"])
        self.assertEqual((paired["request_count"], paired["request_limit"], paired["orders_sent"]), (10, 10, 0))
        self.assertEqual(set(paired["captures"]), {"SPY", "QQQ"})

    def test_current_manifest_missing_and_rollover_are_fail_closed_and_literal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(TrendCaptureError, "manifest"):
                current_pair(root)
            spy, qqq = json.loads(ARCHIVE.read_text()), _qqq_archive()
            def collector(symbol, _clock):
                bundle = spy["bundle"] if symbol == "SPY" else qqq["bundle"]
                return deepcopy(bundle["context"]), deepcopy(bundle["observation"])
            first = collect_paired(collector=collector, clock=lambda: NOW)
            save_stage(root, "2026-09-23", "paired-daily.json", first)
            self.assertEqual(current_pair(root)[0]["bundle"]["symbol"], "SPY")
            second = deepcopy(first)
            second["run_date_utc"] = "2026-09-24"
            save_stage(root, "2026-09-24", "paired-daily.json", second)
            self.assertEqual(current_pair(root)[1]["bundle"]["symbol"], "QQQ")
