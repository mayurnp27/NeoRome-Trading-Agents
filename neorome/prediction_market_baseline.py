"""Pre-registered market-microstructure calibration dataset builder.

This is a reference baseline, not an alpha signal: the probability is the
venue's observed YES bid/ask midpoint known before the decision.  The builder
keeps it separate from any strategy edge, requires finalized outcomes and
exact family terms, and reports chronological development/holdout metrics.
"""

from __future__ import annotations

from datetime import datetime, timezone
import argparse
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence


UTC = timezone.utc
VERSION = "prediction-market-midpoint-calibration-v1"


class BaselineError(ValueError):
    """Calibration input is malformed or fails its pre-registered protocol."""


def _time(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise BaselineError(f"{field} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BaselineError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise BaselineError(f"{field} must include a timezone")
    return parsed.astimezone(UTC)


def _cents(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 100:
        raise BaselineError(f"{field} must be integer cents in [0,100]")
    return value


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _result(value: Any, field: str) -> int:
    if value in ("YES", "yes", 1, True):
        return 1
    if value in ("NO", "no", 0, False):
        return 0
    raise BaselineError(f"{field} must be a finalized binary result")


def _metrics(rows: Sequence[Mapping[str, Any]], name: str) -> dict[str, Any]:
    brier = sum((row["probability"] - row["result"]) ** 2 for row in rows) / len(rows)
    log_loss = -sum(row["result"] * math.log(max(row["probability"], 1e-9)) + (1 - row["result"]) * math.log(max(1 - row["probability"], 1e-9)) for row in rows) / len(rows)
    return {"name": name, "events": len(rows), "brier": round(brier, 12), "log_loss": round(log_loss, 12),
            "first_decision_at": rows[0]["decision_at"], "last_settlement_at": rows[-1]["settlement_at"]}


def build_dataset(
    records: Sequence[Mapping[str, Any]],
    *,
    family: str,
    source_identity: Mapping[str, Any],
    development_events: int = 30,
    holdout_events: int = 20,
) -> dict[str, Any]:
    """Build a chronological, per-family venue-midpoint reference dataset."""
    if not isinstance(family, str) or not family.strip():
        raise BaselineError("family is required")
    if not isinstance(source_identity, Mapping) or not source_identity:
        raise BaselineError("source identity is required")
    if any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in (development_events, holdout_events)):
        raise BaselineError("split sizes must be positive integers")
    rows: list[dict[str, Any]] = []
    terms: set[str] = set()
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise BaselineError(f"record {index} must be an object")
        required = ("event_id", "family", "market_id", "terms_sha256", "decision_at", "known_at", "settlement_at", "settlement_finalized", "settlement_source_hash", "yes_bid_cents", "yes_ask_cents", "result", "source_hash")
        missing = [field for field in required if field not in record]
        if missing:
            raise BaselineError(f"record {index} missing " + ", ".join(missing))
        if record["family"] != family:
            raise BaselineError(f"record {index} belongs to a different family")
        if record["settlement_finalized"] is not True:
            raise BaselineError(f"record {index} settlement is not finalized")
        event_id = record["event_id"]
        if not isinstance(event_id, str) or not event_id.strip():
            raise BaselineError(f"record {index} event_id is required")
        decision, known, settled = (_time(record[field], f"record[{index}].{field}") for field in ("decision_at", "known_at", "settlement_at"))
        if known > decision:
            raise BaselineError(f"record {index} market quote was known after decision")
        if settled <= decision:
            raise BaselineError(f"record {index} settlement is not after decision")
        bid, ask = _cents(record["yes_bid_cents"], f"record[{index}].yes_bid_cents"), _cents(record["yes_ask_cents"], f"record[{index}].yes_ask_cents")
        if bid > ask:
            raise BaselineError(f"record {index} quote is crossed")
        terms_hash = record["terms_sha256"]
        if not isinstance(terms_hash, str) or len(terms_hash) != 64:
            raise BaselineError(f"record {index} terms_sha256 is required")
        terms.add(terms_hash)
        for field in ("source_hash", "settlement_source_hash"):
            if not isinstance(record[field], str) or len(record[field]) != 64 or any(char not in "0123456789abcdefABCDEF" for char in record[field]):
                raise BaselineError(f"record {index} {field} must be a SHA-256 digest")
        probability = (bid + ask) / 200.0
        rows.append({"event_id": event_id, "family": family, "market_id": record["market_id"], "terms_sha256": terms_hash,
                     "decision_at": decision.isoformat().replace("+00:00", "Z"), "settlement_at": settled.isoformat().replace("+00:00", "Z"),
                     "probability": probability, "result": _result(record["result"], f"record[{index}].result"),
                     "yes_bid_cents": bid, "yes_ask_cents": ask, "source_hash": record["source_hash"],
                     "settlement_source_hash": record["settlement_source_hash"]})
    if len({row["event_id"] for row in rows}) != len(rows):
        raise BaselineError("event IDs must be unique")
    if len(terms) != 1:
        raise BaselineError("one exact terms identity is required per family dataset")
    rows.sort(key=lambda row: (row["decision_at"], row["event_id"]))
    if any(rows[index]["decision_at"] >= rows[index + 1]["decision_at"] for index in range(len(rows) - 1)):
        raise BaselineError("decision timestamps must be strictly chronological")
    required = development_events + holdout_events
    if len(rows) < required:
        raise BaselineError(f"at least {required} resolved events are required")
    registration = {"protocol_version": VERSION, "family": family, "baseline": "venue_yes_bid_ask_midpoint_reference",
                    "not_alpha": True, "source_identity": dict(source_identity), "development_events": development_events,
                    "holdout_events": holdout_events, "rules": "probability is midpoint of executable YES bid/ask known before decision"}
    development = rows[:development_events]
    holdout = rows[-holdout_events:]
    return {"schema_version": 1, "version": VERSION, "registration": registration, "terms_sha256": next(iter(terms)),
            "input_sha256": _digest(records), "records": rows,
            "metrics": {"development": _metrics(development, "development"), "holdout": _metrics(holdout, "holdout")},
            "calibration_eligible": True, "alpha_validated": False,
            "research_notice": "Reference calibration only. Venue midpoint is not an alpha signal and does not authorize a trade."}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    source = json.loads(Path(args.input).read_text())
    if Path(args.output).exists():
        parser.error("output already exists")
    dataset = build_dataset(source["records"], family=source["family"], source_identity=source["source_identity"])
    Path(args.output).write_text(json.dumps(dataset, sort_keys=True, separators=(",", ":")) + "\n")
    print(json.dumps({"records": len(dataset["records"]), "calibration_eligible": dataset["calibration_eligible"], "alpha_validated": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["BaselineError", "build_dataset"]
