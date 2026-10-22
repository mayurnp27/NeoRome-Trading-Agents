from __future__ import annotations

from datetime import datetime, timezone
import unittest

from neorome.token_memecoin_runtime import _candidate_observation_times, _candidate_record


UTC = timezone.utc
T0 = datetime(2026, 9, 23, tzinfo=UTC)


def iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


class TokenValidationTimestampTests(unittest.TestCase):
    def test_runtime_candidate_uses_provider_receipt_bounds(self):
        candidate = {
            "mint": "11111111111111111111111111111111",
            "pool": {},
            "evidence": [
                {"requested_at": iso(T0), "received_at": iso(T0.replace(second=1))},
                {"requested_at": iso(T0.replace(second=2)), "received_at": iso(T0.replace(second=3))},
            ],
            "quotes": {"observations": [
                {"requested_at": iso(T0.replace(second=4)), "received_at": iso(T0.replace(second=5))},
            ]},
        }
        social = {"requested_at": iso(T0.replace(second=6)), "received_at": iso(T0.replace(second=7))}
        self.assertEqual(_candidate_observation_times(candidate, social),
                         (iso(T0.replace(second=1)), iso(T0.replace(second=7))))
        record = _candidate_record({}, candidate, social, T0.replace(second=20))
        self.assertEqual(record["known_at"], iso(T0.replace(second=1)))
        self.assertEqual(record["received_at"], iso(T0.replace(second=7)))
        self.assertEqual(record["recorded_at"], iso(T0.replace(second=20)))

    def test_missing_provider_receipt_does_not_fall_back_to_local_clock(self):
        known, received = _candidate_observation_times(
            {"evidence": [{"requested_at": iso(T0)}]},
            {"requested_at": iso(T0)},
        )
        self.assertIsNone(known)
        self.assertIsNone(received)


if __name__ == "__main__":
    unittest.main()
