import base64
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from neorome.paper_ledger import PaperLedger, RiskLimits
import neorome.prediction_lifecycle as lifecycle
from neorome.prediction_lifecycle import (
    LIFECYCLE_VERSION,
    PredictionLifecycleError,
    position_mark,
    run,
)

UTC = timezone.utc
T0 = datetime(2026, 9, 22, 12, 0, tzinfo=UTC)
def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def iso(at):
    return at.isoformat().replace("+00:00", "Z")


def raw(payload):
    return {"payload": payload, "sha256": hashlib.sha256(canonical(payload).encode()).hexdigest()}


def terms(base=T0):
    return {"schema_version": "kalshi-weather-terms-v1", "ticker": "KXHIGHNY-TEST-T70",
            "event_ticker": "KXHIGHNY-TEST", "market_type": "binary",
            "rules_primary": "Fixture exact binary rule", "rules_secondary": "Fixture fallback rule",
            "strike_type": "greater", "floor_strike": 70, "cap_strike": None,
            "notional_value_dollars": "1.0000", "early_close_condition": "Fixture close",
            "open_time": iso(base - timedelta(days=1)), "close_time": iso(base + timedelta(seconds=15)),
            "expiration_time": iso(base + timedelta(days=1)),
            "expected_expiration_time": iso(base + timedelta(hours=1)),
            "latest_expiration_time": iso(base + timedelta(days=1))}


def quote(at, *, bid=39, ask=40, bid_depth=20, ask_depth=20, entry_fee=2, exit_fee=2,
          tag="q", source_kind="synthetic", ticker="KXHIGHNY-TEST-T70", event_id="KXHIGHNY-TEST",
          outcome="yes", units=10):
    payload = {"schema_version": "prediction-quote-v1", "source_kind": source_kind, "venue": "kalshi",
               "provider": "fixture-book", "provider_quote_id": tag,
               "ticker": ticker, "event_id": event_id, "outcome": outcome, "units": units,
               "timestamp": iso(at), "received_at": iso(at), "bid_cents": bid, "ask_cents": ask,
               "bid_depth": bid_depth, "ask_depth": ask_depth,
               "total_entry_fee_cents": entry_fee, "total_exit_fee_cents": exit_fee}
    return {"timestamp": iso(at), "received_at": iso(at), "bid_cents": bid, "ask_cents": ask,
            "bid_depth": bid_depth, "ask_depth": ask_depth,
            "total_entry_fee_cents": entry_fee, "total_exit_fee_cents": exit_fee,
            "raw": {"payload": payload, "sha256": hashlib.sha256(canonical(payload).encode()).hexdigest()}}


def admission(at=T0, *, source_kind="synthetic", outcome="yes"):
    cutoff_at = at + timedelta(seconds=15)
    known = iso(at - timedelta(seconds=1))
    evidence = {
        "forecast": raw({"known_at": known, "forecast_id": "forecast-1"}),
        "calibration": raw({"known_at": known, "calibration_id": "cal-1",
                            "calibration_protocol": {"version": "prediction-calibration-30dev-20untouched-v1",
                                                     "development_events": 30, "untouched_events": 20}}),
        "contract": raw({"known_at": known, "ticker": "KXHIGHNY-TEST-T70", "event_id": "KXHIGHNY-TEST",
                         "outcome": outcome, "family": "weather_daily_temperature",
                         "station_identity": "CLINYC", "observation_date": "2026-09-22", "terms": terms(at)}),
        "cutoff": raw({"known_at": known, "ticker": "KXHIGHNY-TEST-T70", "event_id": "KXHIGHNY-TEST",
                       "cutoff_at": iso(cutoff_at)}),
    }
    hashes = {key + "_hash": row["sha256"] for key, row in evidence.items()}
    manifest = {"ticker": "KXHIGHNY-TEST-T70", "event_id": "KXHIGHNY-TEST", "outcome": outcome,
                "decision_at": iso(at), "cutoff_at": iso(cutoff_at), "evidence_hashes": hashes}
    return {"schema_version": 1, "lifecycle_version": LIFECYCLE_VERSION, "source_kind": source_kind,
            "paper_eligible": True, "calibration_eligible": True,
            "venue": "kalshi", "decision_at": iso(at),
            "proposal_id": "pred-proposal-1", "strategy": "reviewed-weather", "strategy_version": "wx-v1",
            "ticker": "KXHIGHNY-TEST-T70", "event_id": "KXHIGHNY-TEST", "outcome": outcome, "units": 10,
            "adapter_review": {"version": "prediction-admission-review-v1", "decision": "approved",
                               "reviewer": "firm-oversight", "manifest": manifest,
                               "review_hash": hashlib.sha256(canonical(manifest).encode()).hexdigest()},
            "evidence": evidence,
            "quote": quote(at, tag="entry", source_kind=source_kind, outcome=outcome)}


def settlement(at, *, result="yes", payout="1.0000", status="finalized", provisional=False,
               ticker="KXHIGHNY-TEST-T70", held_outcome="yes"):
    frozen = terms(); frozen.pop("schema_version")
    market = {**frozen, "ticker": ticker, "status": status, "is_provisional": provisional, "result": result,
              "settlement_value_dollars": payout, "settlement_ts": (at - timedelta(seconds=1)).isoformat(),
              "expiration_value": "71.0"}
    raw = json.dumps({"markets": [market]}, separators=(",", ":")).encode()
    source_hash = hashlib.sha256(raw).hexdigest()
    manifest = {"ticker": "KXHIGHNY-TEST-T70", "event_id": "KXHIGHNY-TEST", "outcome": held_outcome,
                "terms_hash": hashlib.sha256(canonical(terms()).encode()).hexdigest(),
                "source_hash": source_hash, "received_at": iso(at)}
    return {"adapter_review": {"version": "prediction-settlement-adapter-v1", "decision": "approved",
                                "reviewer": "firm-oversight", "manifest": manifest,
                                "review_hash": hashlib.sha256(canonical(manifest).encode()).hexdigest()},
            "wrapper": {"raw_response_b64": base64.b64encode(raw).decode(),
                        "source_hash": source_hash, "received_at": at.isoformat()}}


class PredictionLifecycleTests(unittest.TestCase):
    def book(self, root):
        return PaperLedger(Path(root) / "paper.db", enforce_market_risk=True, clock=lambda: T0)

    @staticmethod
    def marks(book, now):
        marks = []
        for event in book.audit():
            if event.event_type == "fill":
                position = book.get_position(event.payload["position_id"])
                if position.state == "open":
                    marks.append(position_mark(book, position, now, quote(now, bid=38, ask=41, exit_fee=2, tag="mark-" + position.position_id)))
        return marks

    def enter(self, root):
        book = self.book(root)
        reserved = run(book, Path(root) / "state", now=T0, admission=admission(), mark_provider=self.marks)
        self.assertEqual(reserved["actions"][0]["type"], "reserve")
        filled = run(book, Path(root) / "state", now=T0 + timedelta(seconds=11),
                     later_quote=quote(T0 + timedelta(seconds=11), bid=40, ask=40, entry_fee=2, tag="fill"), mark_provider=self.marks)
        self.assertEqual(filled["actions"][0]["type"], "fill")
        return book

    def test_reserve_delayed_fill_restart_mark_and_close(self):
        with tempfile.TemporaryDirectory() as root:
            book = self.book(root)
            first = run(book, Path(root) / "state", now=T0, admission=admission(), mark_provider=self.marks)
            self.assertEqual(book.snapshot().reserved_cash_cents, 402)
            book = self.book(root)
            early = run(book, Path(root) / "state", now=T0 + timedelta(seconds=9),
                        later_quote=quote(T0 + timedelta(seconds=9), tag="early"), mark_provider=self.marks)
            self.assertEqual(early["blocker"], "awaiting_independent_fill_quote")
            filled = run(book, Path(root) / "state", now=T0 + timedelta(seconds=11),
                         later_quote=quote(T0 + timedelta(seconds=11), tag="fill"), mark_provider=self.marks)
            self.assertEqual(filled["actions"][0]["type"], "fill")
            marked = run(book, Path(root) / "state", now=T0 + timedelta(seconds=20), mark_provider=self.marks)
            self.assertIsNone(marked["blocker"])
            intent = run(book, Path(root) / "state", now=T0 + timedelta(seconds=21), request_close=True,
                         later_quote=quote(T0 + timedelta(seconds=21), tag="trigger"), mark_provider=self.marks)
            self.assertEqual(intent["actions"][0]["type"], "pending_close")
            closed = run(book, Path(root) / "state", now=T0 + timedelta(seconds=32),
                         later_quote=quote(T0 + timedelta(seconds=32), bid=45, ask=46, exit_fee=3, tag="exit"),
                         mark_provider=lambda *_: (_ for _ in ()).throw(RuntimeError("foreign unavailable")))
            self.assertEqual(closed["actions"][0]["type"], "close")
            self.assertEqual(book.snapshot().open_position_cost_cents, 0)
            self.assertIn("global_marks_unavailable", closed["blocker"])

    def test_stop_recovery_and_expiry_cancel_pending(self):
        for kwargs, expected in [({"allow_entry": False}, "entries_disabled"), ({"recovery_ok": False}, "entries_disabled")]:
            with self.subTest(kwargs=kwargs), tempfile.TemporaryDirectory() as root:
                book = self.book(root); run(book, Path(root) / "state", now=T0, admission=admission(), mark_provider=self.marks)
                result = run(book, Path(root) / "state", now=T0 + timedelta(seconds=1), mark_provider=self.marks, **kwargs)
                self.assertEqual(result["actions"][0]["type"], "cancel"); self.assertEqual(result["blocker"], expected)
        with tempfile.TemporaryDirectory() as root:
            book = self.book(root); run(book, Path(root) / "state", now=T0, admission=admission(), mark_provider=self.marks)
            result = run(book, Path(root) / "state", now=T0 + timedelta(seconds=61), mark_provider=self.marks)
            self.assertEqual(result["blocker"], "intent_expired")

    def test_missing_gate_and_global_marks_block_new_exposure(self):
        with tempfile.TemporaryDirectory() as root:
            book = self.book(root)
            self.assertEqual(run(book, Path(root) / "state", now=T0, mark_provider=self.marks)["blocker"], "admission_gate_missing")
            bad = admission(); bad.pop("adapter_review")
            with self.assertRaises(PredictionLifecycleError): run(book, Path(root) / "state", now=T0, admission=bad, mark_provider=self.marks)
            with self.assertRaisesRegex(PredictionLifecycleError, "global_marks_unavailable"):
                run(book, Path(root) / "state", now=T0, admission=admission(), mark_provider=lambda *_: (_ for _ in ()).throw(RuntimeError()))
            self.assertEqual(book.snapshot().reserved_cash_cents, 0)

    def test_current_research_only_weather_output_cannot_be_admitted(self):
        with tempfile.TemporaryDirectory() as root:
            data = admission(); data["paper_eligible"] = False; data["calibration_eligible"] = False
            with self.assertRaisesRegex(PredictionLifecycleError, "not paper/calibration eligible"):
                run(self.book(root), Path(root) / "state", now=T0, admission=data, mark_provider=self.marks)

    def test_observed_adapter_is_explicitly_not_activated(self):
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaisesRegex(PredictionLifecycleError, "observed_adapter_not_activated"):
                run(self.book(root), Path(root) / "state", now=T0,
                    admission=admission(source_kind="observed"), mark_provider=self.marks)

    def test_mark_cannot_predate_actual_fill(self):
        with tempfile.TemporaryDirectory() as root:
            book = self.enter(root)
            position = next(book.get_position(e.payload["position_id"]) for e in book.audit() if e.event_type == "fill")
            with self.assertRaisesRegex(PredictionLifecycleError, "precedes position"):
                position_mark(book, position, T0 + timedelta(seconds=12), quote(T0, tag="pre-fill"))

    def test_unrelated_contract_quote_cannot_fill_or_mark(self):
        with tempfile.TemporaryDirectory() as root:
            book = self.book(root); run(book, Path(root) / "state", now=T0, admission=admission(), mark_provider=self.marks)
            unrelated = quote(T0 + timedelta(seconds=11), ticker="OTHER", tag="unrelated")
            with self.assertRaisesRegex(PredictionLifecycleError, "identity mismatch"):
                run(book, Path(root) / "state", now=T0 + timedelta(seconds=11), later_quote=unrelated, mark_provider=self.marks)
            self.assertEqual(book.snapshot().open_position_cost_cents, 0)
        with tempfile.TemporaryDirectory() as root:
            book = self.book(root); run(book, Path(root) / "state", now=T0, admission=admission(), mark_provider=self.marks)
            no_ask_depth = quote(T0 + timedelta(seconds=11), bid_depth=100, ask_depth=9, tag="thin-ask")
            with self.assertRaisesRegex(PredictionLifecycleError, "ask depth"):
                run(book, Path(root) / "state", now=T0 + timedelta(seconds=11),
                    later_quote=no_ask_depth, mark_provider=self.marks)
        with tempfile.TemporaryDirectory() as root:
            book = self.enter(root)
            position = next(book.get_position(e.payload["position_id"]) for e in book.audit() if e.event_type == "fill")
            with self.assertRaisesRegex(PredictionLifecycleError, "bid depth"):
                position_mark(book, position, T0 + timedelta(seconds=12),
                              quote(T0 + timedelta(seconds=12), bid_depth=9, ask_depth=100, tag="thin-bid"))

    def test_evidence_known_after_decision_and_cutoff_mismatch_are_rejected(self):
        for mutation in ("late_evidence", "cutoff_mismatch"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as root:
                data = admission()
                if mutation == "late_evidence":
                    row = data["evidence"]["forecast"]["payload"]
                    row["known_at"] = iso(T0 + timedelta(seconds=1))
                    data["evidence"]["forecast"]["sha256"] = hashlib.sha256(canonical(row).encode()).hexdigest()
                else:
                    row = data["evidence"]["cutoff"]["payload"]
                    row["cutoff_at"] = iso(T0 + timedelta(seconds=14))
                    data["evidence"]["cutoff"]["sha256"] = hashlib.sha256(canonical(row).encode()).hexdigest()
                hashes = {key + "_hash": value["sha256"] for key, value in data["evidence"].items()}
                manifest = data["adapter_review"]["manifest"]
                manifest["evidence_hashes"] = hashes
                if mutation == "cutoff_mismatch": manifest["cutoff_at"] = row["cutoff_at"]
                data["adapter_review"]["review_hash"] = hashlib.sha256(canonical(manifest).encode()).hexdigest()
                with self.assertRaises(PredictionLifecycleError):
                    run(self.book(root), Path(root) / "state", now=T0, admission=data, mark_provider=self.marks)

    def test_non_enforcing_or_loose_book_cannot_admit(self):
        with tempfile.TemporaryDirectory() as root:
            book = PaperLedger(Path(root) / "legacy.db", enforce_market_risk=False, clock=lambda: T0)
            with self.assertRaisesRegex(PredictionLifecycleError, "market risk enforcement"):
                run(book, Path(root) / "legacy-state", now=T0, admission=admission(), mark_provider=self.marks)
            loose = PaperLedger(Path(root) / "loose.db", enforce_market_risk=True,
                                limits=RiskLimits(aggregate_cap_cents=8_001), clock=lambda: T0)
            with self.assertRaisesRegex(PredictionLifecycleError, "ceilings"):
                run(loose, Path(root) / "loose-state", now=T0, admission=admission(), mark_provider=self.marks)

    def test_admission_quote_and_hash_validation(self):
        mutations = [
            lambda x: x.update(units=True),
            lambda x: x["quote"].update(ask_cents=101),
            lambda x: x["quote"].update(timestamp=(T0 - timedelta(seconds=61)).isoformat()),
            lambda x: x["quote"]["raw"].update(sha256="f" * 64),
            lambda x: x["evidence"]["contract"]["payload"].update(ticker="other"),
            lambda x: x["adapter_review"].update(decision=True),
        ]
        for mutate in mutations:
            with self.subTest(mutate=mutate), tempfile.TemporaryDirectory() as root:
                data = admission(); mutate(data)
                with self.assertRaises(PredictionLifecycleError):
                    run(self.book(root), Path(root) / "state", now=T0, admission=data, mark_provider=self.marks)

    def test_terminal_settlement_exact_final_binary_and_persistent(self):
        with tempfile.TemporaryDirectory() as root:
            book = self.enter(root)
            result = run(book, Path(root) / "state", now=T0 + timedelta(seconds=20),
                         settlement=settlement(T0 + timedelta(seconds=20)), mark_provider=self.marks)
            self.assertEqual(result["actions"][0]["type"], "settlement")
            self.assertEqual(result["actions"][0]["result"]["proceeds_cents"], 1000)
            saved = json.loads((Path(root) / "state" / "prediction_state.json").read_text())
            self.assertEqual(saved["history"][-1]["evidence"]["proof"]["ticker"], "KXHIGHNY-TEST-T70")

    def test_no_contract_uses_venue_yes_zero_but_held_full_payout(self):
        with tempfile.TemporaryDirectory() as root:
            book = self.book(root); data = admission(outcome="no")
            run(book, Path(root) / "state", now=T0, admission=data, mark_provider=self.marks)
            run(book, Path(root) / "state", now=T0 + timedelta(seconds=11),
                later_quote=quote(T0 + timedelta(seconds=11), tag="fill-no", outcome="no"), mark_provider=self.marks)
            result = run(book, Path(root) / "state", now=T0 + timedelta(seconds=20),
                         settlement=settlement(T0 + timedelta(seconds=20), result="no", payout="0.0000", held_outcome="no"),
                         mark_provider=self.marks)
            self.assertEqual(result["actions"][0]["result"]["proceeds_cents"], 1000)

    def test_invalid_finalization_hash_dispute_and_nonbinary_block_release(self):
        cases = [
            settlement(T0 + timedelta(seconds=20), status="settled"),
            settlement(T0 + timedelta(seconds=20), provisional=True),
            settlement(T0 + timedelta(seconds=20), payout="0.5000"),
            settlement(T0 + timedelta(seconds=20), ticker="OTHER"),
        ]
        bad_hash = settlement(T0 + timedelta(seconds=20)); bad_hash["wrapper"]["source_hash"] = "f" * 64; cases.append(bad_hash)
        disputed = settlement(T0 + timedelta(seconds=20))
        raw = json.loads(base64.b64decode(disputed["wrapper"]["raw_response_b64"])); raw["markets"][0]["disputed"] = True
        encoded = json.dumps(raw, separators=(",", ":")).encode(); disputed["wrapper"].update(raw_response_b64=base64.b64encode(encoded).decode(), source_hash=hashlib.sha256(encoded).hexdigest()); cases.append(disputed)
        wrong_event = settlement(T0 + timedelta(seconds=20))
        raw_event = json.loads(base64.b64decode(wrong_event["wrapper"]["raw_response_b64"])); raw_event["markets"][0]["event_ticker"] = "OTHER"
        encoded_event = json.dumps(raw_event, separators=(",", ":")).encode(); wrong_event["wrapper"].update(raw_response_b64=base64.b64encode(encoded_event).decode(), source_hash=hashlib.sha256(encoded_event).hexdigest()); cases.append(wrong_event)
        changed_terms = settlement(T0 + timedelta(seconds=20))
        raw_terms = json.loads(base64.b64decode(changed_terms["wrapper"]["raw_response_b64"])); raw_terms["markets"][0]["rules_primary"] = "Changed"
        encoded_terms = json.dumps(raw_terms, separators=(",", ":")).encode(); changed_terms["wrapper"].update(raw_response_b64=base64.b64encode(encoded_terms).decode(), source_hash=hashlib.sha256(encoded_terms).hexdigest()); cases.append(changed_terms)
        for field, changed in (("floor_strike", 71), ("close_time", iso(T0 + timedelta(seconds=16))),
                               ("notional_value_dollars", "2.0000")):
            altered = settlement(T0 + timedelta(seconds=20))
            raw_altered = json.loads(base64.b64decode(altered["wrapper"]["raw_response_b64"]))
            raw_altered["markets"][0][field] = changed
            encoded_altered = json.dumps(raw_altered, separators=(",", ":")).encode()
            altered["wrapper"].update(raw_response_b64=base64.b64encode(encoded_altered).decode(),
                                      source_hash=hashlib.sha256(encoded_altered).hexdigest())
            cases.append(altered)
        for evidence in cases:
            with self.subTest(evidence=evidence), tempfile.TemporaryDirectory() as root:
                book = self.enter(root)
                with self.assertRaises(PredictionLifecycleError):
                    run(book, Path(root) / "state", now=T0 + timedelta(seconds=20), settlement=evidence, mark_provider=self.marks)
                self.assertGreater(book.snapshot().open_position_cost_cents, 0)

    def test_synthetic_rehearsal_is_visible_and_separate(self):
        with tempfile.TemporaryDirectory() as root:
            book = self.book(root); data = admission(source_kind="synthetic")
            run(book, Path(root) / "state", now=T0, admission=data, mark_provider=self.marks)
            payload = next(e.payload["payload"] for e in book.audit() if e.event_type == "reserve")
            self.assertEqual(payload["provenance"], "synthetic")
            self.assertTrue(payload["metadata"]["prediction_lifecycle_v1"]["synthetic"])

    def test_exit_fee_above_gross_requires_reconciliation_and_retains_risk(self):
        with tempfile.TemporaryDirectory() as root:
            book = self.enter(root)
            run(book, Path(root) / "state", now=T0 + timedelta(seconds=20), request_close=True,
                later_quote=quote(T0 + timedelta(seconds=20), tag="trigger"), mark_provider=self.marks)
            impossible = quote(T0 + timedelta(seconds=31), bid=0, ask=1, exit_fee=1, tag="liability")
            with self.assertRaisesRegex(PredictionLifecycleError, "liability_reconciliation_required"):
                run(book, Path(root) / "state", now=T0 + timedelta(seconds=31), later_quote=impossible,
                    mark_provider=lambda *_: (_ for _ in ()).throw(RuntimeError("unavailable")))
            self.assertGreater(book.snapshot().open_position_cost_cents, 0)

    def test_post_close_and_settlement_crash_reconcile_before_new_position(self):
        for terminal in ("close", "settlement"):
            with self.subTest(terminal=terminal), tempfile.TemporaryDirectory() as root:
                book = self.enter(root); state_dir = Path(root) / "state"
                if terminal == "close":
                    run(book, state_dir, now=T0 + timedelta(seconds=20), request_close=True,
                        later_quote=quote(T0 + timedelta(seconds=20), tag="trigger"), mark_provider=self.marks)
                original = lifecycle._write; calls = 0
                def fail_second(path, state):
                    nonlocal calls
                    calls += 1
                    if calls == 2: raise OSError("simulated crash after ledger mutation")
                    return original(path, state)
                with mock.patch.object(lifecycle, "_write", side_effect=fail_second):
                    with self.assertRaises(OSError):
                        if terminal == "close":
                            run(book, state_dir, now=T0 + timedelta(seconds=31),
                                later_quote=quote(T0 + timedelta(seconds=31), bid=45, ask=46, tag="exit"),
                                mark_provider=self.marks)
                        else:
                            run(book, state_dir, now=T0 + timedelta(seconds=20),
                                settlement=settlement(T0 + timedelta(seconds=20)), mark_provider=self.marks)
                self.assertEqual(book.snapshot().open_position_cost_cents, 0)
                next_data = admission(T0 + timedelta(seconds=40)); next_data["proposal_id"] = "pred-proposal-2"
                restarted = run(self.book(root), state_dir, now=T0 + timedelta(seconds=40),
                                admission=next_data, mark_provider=self.marks)
                self.assertEqual(restarted["actions"][0]["type"], "reserve")
                saved = json.loads((state_dir / "prediction_state.json").read_text())
                self.assertNotIn("close_intent", saved); self.assertNotIn("pending_close", saved)
                self.assertNotIn("pending_settlement", saved)

    def test_pre_mutation_state_write_failure_never_mutates_ledger(self):
        with tempfile.TemporaryDirectory() as root:
            book = self.book(root)
            with mock.patch.object(lifecycle, "_write", side_effect=OSError("pre-mutation crash")):
                with self.assertRaises(OSError):
                    run(book, Path(root) / "state", now=T0, admission=admission(), mark_provider=self.marks)
            self.assertEqual(book.snapshot().reserved_cash_cents, 0)

        for mutation in ("fill", "close", "settlement", "cancel"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as root:
                state_dir = Path(root) / "state"; book = self.book(root)
                run(book, state_dir, now=T0, admission=admission(), mark_provider=self.marks)
                if mutation in {"close", "settlement"}:
                    run(book, state_dir, now=T0 + timedelta(seconds=11),
                        later_quote=quote(T0 + timedelta(seconds=11), tag="fill"), mark_provider=self.marks)
                if mutation == "close":
                    run(book, state_dir, now=T0 + timedelta(seconds=20), request_close=True,
                        later_quote=quote(T0 + timedelta(seconds=20), tag="trigger"), mark_provider=self.marks)
                with mock.patch.object(lifecycle, "_write", side_effect=OSError("pre-mutation crash")):
                    with self.assertRaises(OSError):
                        if mutation == "fill":
                            run(book, state_dir, now=T0 + timedelta(seconds=11),
                                later_quote=quote(T0 + timedelta(seconds=11), tag="fill"), mark_provider=self.marks)
                        elif mutation == "close":
                            run(book, state_dir, now=T0 + timedelta(seconds=31),
                                later_quote=quote(T0 + timedelta(seconds=31), tag="exit"), mark_provider=self.marks)
                        else:
                            if mutation == "settlement":
                                run(book, state_dir, now=T0 + timedelta(seconds=20),
                                    settlement=settlement(T0 + timedelta(seconds=20)), mark_provider=self.marks)
                            else:
                                run(book, state_dir, now=T0 + timedelta(seconds=1),
                                    allow_entry=False, mark_provider=self.marks)
                if mutation in {"fill", "cancel"}:
                    self.assertEqual(book.snapshot().open_position_cost_cents, 0)
                    self.assertGreater(book.snapshot().reserved_cash_cents, 0)
                else:
                    self.assertGreater(book.snapshot().open_position_cost_cents, 0)

    def test_post_reserve_fill_and_cancel_crashes_reconcile(self):
        # Reserve committed, final state write failed.
        with tempfile.TemporaryDirectory() as root:
            book = self.book(root); state_dir = Path(root) / "state"; original = lifecycle._write; calls = 0
            def reserve_crash(path, state):
                nonlocal calls
                calls += 1
                if calls == 2: raise OSError("post reserve")
                return original(path, state)
            with mock.patch.object(lifecycle, "_write", side_effect=reserve_crash):
                with self.assertRaises(OSError): run(book, state_dir, now=T0, admission=admission(), mark_provider=self.marks)
            restarted = run(self.book(root), state_dir, now=T0 + timedelta(seconds=1), mark_provider=self.marks)
            self.assertEqual(restarted["blocker"], "awaiting_independent_fill_quote")
            self.assertNotIn("pending_reserve", restarted["state"])

        # Fill committed, final state write failed.
        with tempfile.TemporaryDirectory() as root:
            book = self.book(root); state_dir = Path(root) / "state"
            run(book, state_dir, now=T0, admission=admission(), mark_provider=self.marks)
            original = lifecycle._write; calls = 0
            def fill_crash(path, state):
                nonlocal calls
                calls += 1
                if calls == 2: raise OSError("post fill")
                return original(path, state)
            with mock.patch.object(lifecycle, "_write", side_effect=fill_crash):
                with self.assertRaises(OSError):
                    run(book, state_dir, now=T0 + timedelta(seconds=11),
                        later_quote=quote(T0 + timedelta(seconds=11), tag="fill"), mark_provider=self.marks)
            restarted = run(self.book(root), state_dir, now=T0 + timedelta(seconds=12), mark_provider=self.marks)
            self.assertNotIn("pending_fill", restarted["state"])
            self.assertGreater(self.book(root).snapshot().open_position_cost_cents, 0)

        # Cancellation committed, final state write failed.
        with tempfile.TemporaryDirectory() as root:
            book = self.book(root); state_dir = Path(root) / "state"
            run(book, state_dir, now=T0, admission=admission(), mark_provider=self.marks)
            original = lifecycle._write; calls = 0
            def cancel_crash(path, state):
                nonlocal calls
                calls += 1
                if calls == 2: raise OSError("post cancel")
                return original(path, state)
            with mock.patch.object(lifecycle, "_write", side_effect=cancel_crash):
                with self.assertRaises(OSError):
                    run(book, state_dir, now=T0 + timedelta(seconds=1), allow_entry=False, mark_provider=self.marks)
            restarted = run(self.book(root), state_dir, now=T0 + timedelta(seconds=2), allow_entry=False, mark_provider=self.marks)
            self.assertNotIn("pending_cancel", restarted["state"])
            self.assertEqual(self.book(root).snapshot().reserved_cash_cents, 0)


if __name__ == "__main__":
    unittest.main()
