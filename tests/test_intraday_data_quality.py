from __future__ import annotations

import unittest
from datetime import datetime, timezone

from desks.equities.intraday_data_quality import BarQualityInputError, compare_aligned_bars


def bar(timestamp: str, *, high: float = 11.0, low: float = 9.0, close: float = 10.0) -> dict[str, object]:
    return {"timestamp": timestamp, "open": 10.0, "high": high, "low": low, "close": close}


class IntradayDataQualityTests(unittest.TestCase):
    def test_offsets_align_without_losing_original_timestamps(self):
        report = compare_aligned_bars(
            [bar("2026-09-21T15:42:00-04:00")],
            [bar("2026-09-21T19:42:00Z")],
            absolute_tolerance=0.01,
            left_source="alpaca",
            right_source="massive",
        )
        self.assertEqual(report.counts["aligned_bars"], 1)
        self.assertEqual(report.counts["quarantined_bars"], 0)
        comparison = report.comparisons[0]
        self.assertEqual(comparison.left_timestamp, "2026-09-21T15:42:00-04:00")
        self.assertEqual(comparison.right_timestamp, "2026-09-21T19:42:00Z")

    def test_tolerance_controls_quarantine(self):
        left = [bar("2026-09-21T15:42:00Z", high=11.0)]
        right = [bar("2026-09-21T15:42:00Z", high=11.01)]
        accepted = compare_aligned_bars(left, right, absolute_tolerance=0.01)
        rejected = compare_aligned_bars(left, right, absolute_tolerance=0.009)
        self.assertFalse(accepted.comparisons[0].quarantined)
        self.assertTrue(rejected.comparisons[0].quarantined)
        self.assertIn("material_high_difference", rejected.comparisons[0].reasons)

    def test_discrepancies_form_only_contiguous_quarantine_segments(self):
        timestamps = ["2026-09-21T15:42:00Z", "2026-09-21T15:43:00Z", "2026-09-21T15:44:00Z", "2026-09-21T15:46:00Z"]
        left = [bar(timestamp) for timestamp in timestamps]
        right = [bar(timestamp, high=30.0 if timestamp in (timestamps[0], timestamps[1], timestamps[3]) else 11.0) for timestamp in timestamps]
        report = compare_aligned_bars(left, right, absolute_tolerance=0.01, left_source="alpaca", right_source="massive")
        self.assertEqual(report.counts["quarantined_bars"], 3)
        self.assertEqual(report.counts["quarantined_segments"], 2)
        self.assertEqual([segment.as_dict()["bar_count"] for segment in report.segments], [2, 1])
        self.assertEqual(report.segments[0].start.isoformat(), "2026-09-21T15:42:00+00:00")
        self.assertEqual(report.segments[1].start.isoformat(), "2026-09-21T15:46:00+00:00")

    def test_missing_timestamp_is_quarantined_without_source_substitution(self):
        left = [bar("2026-09-21T15:42:00Z"), bar("2026-09-21T15:43:00Z")]
        right = [bar("2026-09-21T15:42:00Z")]
        report = compare_aligned_bars(left, right, absolute_tolerance=0.01, left_source="alpaca", right_source="massive")
        missing = report.comparisons[1]
        self.assertEqual(missing.reasons, ("missing_right_bar",))
        self.assertIsNotNone(missing.left_bar)
        self.assertIsNone(missing.right_bar)
        self.assertIsNone(report.as_dict()["source_preference"])

    def test_invalid_inputs_fail_closed(self):
        cases = [
            ([bar("2026-09-21T15:42:00")], [bar("2026-09-21T15:42:00Z")]),
            ([bar("2026-09-21T15:42:00Z"), bar("2026-09-21T15:42:00Z")], []),
            ([bar("2026-09-21T15:42:00Z", high=8.0)], [bar("2026-09-21T15:42:00Z")]),
        ]
        for left, right in cases:
            with self.subTest(left=left), self.assertRaises(BarQualityInputError):
                compare_aligned_bars(left, right, absolute_tolerance=0.01)
        with self.assertRaises(BarQualityInputError):
            compare_aligned_bars([], [], absolute_tolerance=-0.01)

    def test_report_preserves_the_three_saved_mu_discrepancy_timestamps(self):
        times = ["2026-09-21T15:42:00Z", "2026-09-21T15:43:00Z", "2026-09-21T15:44:00Z"]
        left = [
            {"timestamp": times[0], "open": 1039.0, "high": 1040.2999, "low": 1038.53, "close": 1039.35},
            {"timestamp": times[1], "open": 1039.34, "high": 1040.4612, "low": 1038.68, "close": 1038.805},
            {"timestamp": times[2], "open": 1038.835, "high": 1039.54, "low": 1038.6, "close": 1039.1},
        ]
        right = [
            {"timestamp": times[0], "open": 1039.0, "high": 1062.7434, "low": 1038.53, "close": 1039.35},
            {"timestamp": times[1], "open": 1039.34, "high": 1063.1063, "low": 1033.238, "close": 1038.805},
            {"timestamp": times[2], "open": 1038.835, "high": 1041.4774, "low": 1036.0226, "close": 1039.1},
        ]
        report = compare_aligned_bars(left, right, absolute_tolerance=0.01, left_source="alpaca", right_source="massive")
        self.assertEqual(report.counts["aligned_bars"], 3)
        self.assertEqual(report.counts["quarantined_bars"], 3)
        self.assertEqual(report.counts["quarantined_segments"], 1)
        self.assertEqual(report.segments[0].timestamps, tuple(datetime.fromisoformat(t.replace("Z", "+00:00")) for t in times))


if __name__ == "__main__":
    unittest.main()
