import unittest

import pandas as pd

from desks.equities.intraday_universe import (
    UniverseValidationError,
    members_on,
    parse_evidence,
    validate_membership,
)


def evidence(**overrides):
    value = {
        "source": "fixture-pit-membership",
        "source_version": "2026-09-23-fixture",
        "captured_at": "2026-09-23T00:00:00Z",
        "includes_delisted": True,
        "coverage_start": "2023-01-01",
        "coverage_end": "2026-09-23",
    }
    value.update(overrides)
    return parse_evidence(value)


def membership():
    return pd.DataFrame([
        {"permanent_id": "A", "symbol": "OLD", "effective_start": "2023-01-01", "effective_end": "2024-01-02", "known_at": "2022-12-30", "security_type": "common_stock", "exchange": "NYSE"},
        {"permanent_id": "A", "symbol": "NEW", "effective_start": "2024-01-02", "effective_end": None, "known_at": "2023-12-29", "security_type": "common_stock", "exchange": "NYSE"},
        {"permanent_id": "B", "symbol": "FUND", "effective_start": "2023-01-01", "effective_end": None, "known_at": "2022-12-30", "security_type": "etf", "exchange": "ARCA"},
        {"permanent_id": "C", "symbol": "GONE", "effective_start": "2023-01-01", "effective_end": "2024-06-01", "known_at": "2022-12-30", "security_type": "ordinary_share", "exchange": "NASDAQ"},
    ])


class PointInTimeUniverseTests(unittest.TestCase):
    def test_symbol_changes_and_delistings_are_time_correct(self):
        old = members_on(membership(), evidence(), "2023-12-29")
        self.assertEqual(old["symbol"].tolist(), ["OLD", "GONE"])
        new = members_on(membership(), evidence(), "2024-06-03")
        self.assertEqual(new["symbol"].tolist(), ["NEW"])

    def test_funds_are_excluded_without_using_a_current_survivor_list(self):
        result = members_on(membership(), evidence(), "2024-03-01")
        self.assertNotIn("FUND", result["symbol"].tolist())

    def test_missing_delisted_coverage_is_blocked(self):
        with self.assertRaises(UniverseValidationError):
            parse_evidence({**evidence().__dict__, "includes_delisted": False})

    def test_as_of_outside_coverage_or_unavailable_membership_is_blocked(self):
        with self.assertRaises(UniverseValidationError):
            members_on(membership(), evidence(), "2022-12-30")
        delayed = membership()
        delayed.loc[0, "known_at"] = "2024-01-01"
        self.assertEqual(members_on(delayed, evidence(), "2023-12-29")["symbol"].tolist(), ["GONE"])

    def test_overlapping_membership_intervals_are_rejected(self):
        data = membership()
        data.loc[1, "effective_start"] = "2023-12-01"
        with self.assertRaises(UniverseValidationError):
            validate_membership(data)


if __name__ == "__main__":
    unittest.main()
