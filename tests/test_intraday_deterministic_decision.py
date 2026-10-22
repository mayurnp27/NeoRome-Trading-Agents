from datetime import datetime, timedelta, timezone
import unittest

from desks.equities.intraday_deterministic_decision import deterministic_decision


UTC = timezone.utc
START = datetime(2026, 9, 21, 13, 30, tzinfo=UTC)


def stamp(minutes):
    return (START + timedelta(minutes=minutes)).isoformat().replace("+00:00", "Z")


def inputs(*, break_out=True, quality=True):
    # Signal at 09:46 NY means the 09:45 bar is the last completed bar.
    bars = []
    for minute in range(16):
        close = 10 if minute < 15 else (10.3 if break_out else 10)
        bars.append({"symbol": "SYNTH", "timestamp": stamp(minute), "high": 10.1,
                     "close": close, "volume": 100, "vwap": 10})
    signal = START + timedelta(minutes=16)
    return {
        "signal_at": signal.isoformat().replace("+00:00", "Z"), "symbol": "SYNTH",
        "raw": {"universe": [{"permanent_id": "p-1", "symbol": "SYNTH", "eligible": True}],
                "calendar": {"session_close": stamp(390)}, "bar_prefix": bars,
                "bar_prefix_quality_clear": quality},
        "evidence": {"universe_evidence_sha256": "a" * 64, "bar_prefix_sha256": "b" * 64,
                     "quote_evidence_sha256": "c" * 64, "universe_known_at": stamp(-2),
                     "bar_prefix_known_at": stamp(-1), "calendar_known_at": stamp(-2)},
    }


class DeterministicDecisionTests(unittest.TestCase):
    def test_orb15_is_deterministic_and_binds_pre_signal_evidence(self):
        result = deterministic_decision(inputs(), fee_cents_per_unit=2)
        self.assertEqual(result["action"], "candidate")
        candidate = result["candidate"]
        self.assertEqual(candidate["strategy_id"], "orb15")
        self.assertEqual(candidate["exit_decision_timestamp"], stamp(385))
        self.assertEqual(candidate["quote_evidence_sha256"], "c" * 64)

    def test_unknown_quality_or_no_signal_abstains(self):
        self.assertEqual(deterministic_decision(inputs(quality=False))["action"], "abstain")
        self.assertEqual(deterministic_decision(inputs(break_out=False))["action"], "abstain")


if __name__ == "__main__":
    unittest.main()
