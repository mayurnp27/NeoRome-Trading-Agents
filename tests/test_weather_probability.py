import copy
from datetime import datetime, timedelta, timezone
import hashlib
import unittest

from neorome.weather_probability import calibrate, forecast_probability
from neorome.weather_validation import digest


UTC = timezone.utc


def mapping():
    body = {"version": "weather-station-settlement-map-v1", "station_identity": "CLINYC-reviewed-v1",
            "source_identity": {"provider": "The Weather Company", "station": "CLINYC"},
            "terms_schema": "kalshi-weather-terms-v1", "cutoff_policy": "reviewed-close-time-v1"}
    return {**body, "mapping_sha256": digest(body)}


def contracts(day):
    values = [67, 68, 69, 70, 71, 72]
    return [{"ticker": f"KXHIGHNY-{day:%Y%m%d}-B{value}", "terms_sha256": hashlib.sha256(f"{day}-{value}".encode()).hexdigest(),
             "cutoff_at": (datetime.combine(day, datetime.min.time(), UTC) + timedelta(hours=4)).isoformat().replace("+00:00", "Z"),
             "outcome_rule": "inclusive_range", "floor_f": str(value), "cap_f": str(value)} for value in values]


def pair(index, identity):
    day = datetime(2026, 1, 1, tzinfo=UTC).date() + timedelta(days=index)
    rows = contracts(day); labels = {row["ticker"]: int(row["floor_f"] == "70") for row in rows}
    known = datetime.combine(day - timedelta(days=1), datetime.min.time(), UTC) + timedelta(hours=1)
    outcome = datetime.combine(day + timedelta(days=1), datetime.min.time(), UTC) + timedelta(hours=12)
    body = {"version": "PM-WEATHER-PAIR-v1", "event_ticker": f"KXHIGHNY-{day:%y%b%d}".upper(), "target_date": str(day),
            "forecast_known_at": known.isoformat().replace("+00:00", "Z"), "outcome_known_at": outcome.isoformat().replace("+00:00", "Z"),
            "predictor_max_f": "70", "venue_reported_max_f": "70", "residual_f": "0", "labels": labels,
            "source_kind": "observed", "temporal_and_identity_validated": True, "station_mapping_certified": True,
            "calibration_identity": {key: identity[key] for key in ("mapping_sha256", "station_identity", "source_identity", "terms_schema", "cutoff_policy")},
            "contract_definitions": rows}
    return {**body, "pair_sha256": digest(body)}


def forecast(identity, contract):
    payload = {"predictor_max_f": "70", "known_at": "2026-02-20T01:00:00Z"}
    return {**payload, "forecast_sha256": digest(payload),
            "calibration_identity": {key: identity[key] for key in ("mapping_sha256", "station_identity", "source_identity", "terms_schema", "cutoff_policy")},
            "contract": contract}


class WeatherProbabilityTests(unittest.TestCase):
    def test_insufficient_or_unmapped_records_abstain(self):
        identity = mapping(); rows = [pair(i, identity) for i in range(50)]
        self.assertIn("mapping_missing", calibrate(rows, None)["abstention_reasons"][0])
        self.assertIn("requires_30", calibrate(rows[:49], identity)["abstention_reasons"][0])

    def test_frozen_30_20_metrics_and_past_only_bounds(self):
        identity = mapping(); rows = [pair(i, identity) for i in range(50)]
        result = calibrate(rows, identity)
        self.assertEqual(result["status"], "eligible")
        self.assertEqual(result["development_events"], 30)
        self.assertEqual(result["holdout_events"], 20)
        self.assertIn("brier", result["metrics"])
        self.assertIn("log_loss", result["baseline"])
        contract = contracts(datetime(2026, 2, 20, tzinfo=UTC).date())[3]
        current = forecast(identity, contract)
        probability = forecast_probability(result, current, contract, decision_at="2026-02-20T01:01:00Z")
        self.assertGreater(float(probability["lower_bound"]), 0.8)
        self.assertEqual(probability["calibration"]["protocol_version"], "prediction-weather-30dev-20holdout-v1")

    def test_identity_tamper_and_future_forecast_refuse_probability(self):
        identity = mapping(); rows = [pair(i, identity) for i in range(50)]
        bad = copy.deepcopy(rows[0]); bad["calibration_identity"]["station_identity"] = "different"; bad.pop("pair_sha256"); bad["pair_sha256"] = digest(bad)
        self.assertEqual(calibrate([bad, *rows[1:]], identity)["status"], "abstained")
        result = calibrate(rows, identity); contract = contracts(datetime(2026, 2, 20, tzinfo=UTC).date())[3]; current = forecast(identity, contract)
        current["known_at"] = "2026-02-20T01:02:00Z"
        with self.assertRaisesRegex(ValueError, "known after"):
            forecast_probability(result, current, contract, decision_at="2026-02-20T01:01:00Z")


if __name__ == "__main__":
    unittest.main()
