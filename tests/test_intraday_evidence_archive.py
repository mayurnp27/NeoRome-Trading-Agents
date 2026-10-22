from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from desks.equities.intraday_evidence_archive import EvidenceValidationError, archive_artifact, shadow_evidence


UTC = timezone.utc
BASE = datetime(2026, 9, 21, 13, 45, tzinfo=UTC)


def stamp(seconds=0): return (BASE + timedelta(seconds=seconds)).isoformat()
def artifact(kind, payload, **extra):
    defaults = {
        "universe": {"selection_at": stamp(-1)},
        "bar_prefix": {"timeframe": "1Min", "adjustment": "raw"},
        "calendar": {},
        "quotes": {},
    }
    known_at = stamp(.5) if kind == "quotes" else stamp(-1)
    return {"kind": kind, "source": "alpaca-sip", "source_version": "saved-response-v1", "known_at": known_at, "payload": payload, **defaults[kind], **extra}


class ArchiveTests(unittest.TestCase):
    def test_archive_is_content_addressed_and_no_lookahead(self):
        source = artifact("universe", [{"permanent_id": "CUSIP-MU", "symbol": "MU"}])
        with TemporaryDirectory() as temp:
            first = archive_artifact(temp, source, signal_at=stamp())
            second = archive_artifact(temp, source, signal_at=stamp())
            self.assertEqual(first["digest"], second["digest"])
            self.assertTrue(Path(first["path"]).exists())
        source["known_at"] = stamp(1)
        with self.assertRaisesRegex(EvidenceValidationError, "after signal"):
            archive_artifact("/tmp/not-used", source, signal_at=stamp())

    def test_quote_receipts_and_size_evidence_are_required(self):
        quotes = artifact("quotes", [{"timestamp": stamp(), "received_at": stamp(.5)}])
        with self.assertRaisesRegex(EvidenceValidationError, "size units"):
            shadow_evidence(artifact("universe", []), artifact("bar_prefix", []), artifact("calendar", {"session_close": stamp(6*60*60+15*60)}), quotes, signal_at=stamp())
        quotes.update({"size_units": "shares", "size_units_evidence": "provider-field-contract-v1"})
        output = shadow_evidence(artifact("universe", []), artifact("bar_prefix", []), artifact("calendar", {"session_close": stamp(6*60*60+15*60)}), quotes, signal_at=stamp())
        self.assertTrue(output["size_units_verified"])
        self.assertEqual(len(output["quote_evidence_sha256"]), 64)
        self.assertEqual(output["quote_known_at"], (BASE + timedelta(seconds=.5)).isoformat())
        quotes["payload"][0]["received_at"] = stamp(2)
        with self.assertRaisesRegex(EvidenceValidationError, "one second"):
            shadow_evidence(artifact("universe", []), artifact("bar_prefix", []), artifact("calendar", {"session_close": stamp(6*60*60+15*60)}), quotes, signal_at=stamp())

    def test_artifact_known_at_cannot_predate_source_receipt_or_selection(self):
        quotes = artifact(
            "quotes",
            [{"timestamp": stamp(.25), "received_at": stamp(.5)}],
            known_at=stamp(.25),
            size_units="shares",
            size_units_evidence="provider-field-contract-v1",
        )
        with self.assertRaisesRegex(EvidenceValidationError, "precedes a contained receipt"):
            shadow_evidence(
                artifact("universe", []),
                artifact("bar_prefix", []),
                artifact("calendar", {"session_close": stamp(6*60*60+15*60)}),
                quotes,
                signal_at=stamp(),
            )
        universe = artifact(
            "universe", [{"permanent_id": "CUSIP-MU", "symbol": "MU"}],
            known_at=stamp(-2), selection_at=stamp(-1),
        )
        with self.assertRaisesRegex(EvidenceValidationError, "precedes selection_at"):
            shadow_evidence(
                universe,
                artifact("bar_prefix", []),
                artifact("calendar", {"session_close": stamp(6*60*60+15*60)}),
                artifact(
                    "quotes", [{"timestamp": stamp(.25), "received_at": stamp(.5)}],
                    size_units="shares", size_units_evidence="provider-field-contract-v1",
                ),
                signal_at=stamp(),
            )


if __name__ == "__main__": unittest.main()
