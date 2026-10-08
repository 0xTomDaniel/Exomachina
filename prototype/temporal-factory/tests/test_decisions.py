"""Pure, in-memory decision tests. No live A2A or Temporal service is used."""
from __future__ import annotations

import copy
import sys
import tempfile
import unittest
from contextlib import nullcontext
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from a2a_outcome import (EffectKind, OutcomeJournal, Phase, ReceiverKind, lookup_result,
                         send_ambiguous, send_completed, submitted)
from incident_projection import incident_result, public_task_state


class A2AOutcomeTests(unittest.TestCase):
    def setUp(self):
        self.base = submitted("action-1", "run-1", "d" * 64,
                              ReceiverKind.PARTICIPATING, lookup_limit=2)
        self.receipt = {"action_id": "action-1", "run_id": "run-1",
                        "definition_digest": "d" * 64, "task_id": "task-1"}

    def test_completed_reply(self):
        result = send_completed(self.base, self.receipt)
        self.assertEqual(result.phase, Phase.CONFIRMED)
        self.assertFalse(result.may_submit)

    def test_ambiguous_then_lookup_confirmed(self):
        unknown = send_ambiguous(self.base)
        self.assertEqual(unknown.phase, Phase.UNKNOWN)
        self.assertFalse(unknown.may_submit)
        result = lookup_result(unknown, self.receipt)
        self.assertEqual(result.phase, Phase.CONFIRMED)
        self.assertEqual(result.lookup_count, 1)

    def test_lookup_missing_is_bounded_then_incident(self):
        unknown = send_ambiguous(self.base)
        still_unknown = lookup_result(unknown, None)
        self.assertEqual(still_unknown.phase, Phase.UNKNOWN)
        result = lookup_result(still_unknown, None)
        self.assertEqual((result.phase, result.reason, result.lookup_count),
                         (Phase.INCIDENT, "lookup-exhausted", 2))

    def test_unavailable_lookup_and_wrong_binding(self):
        unknown = send_ambiguous(self.base)
        self.assertEqual(lookup_result(unknown, None, available=False).phase, Phase.UNKNOWN)
        bad = {**self.receipt, "run_id": "other"}
        result = lookup_result(unknown, bad)
        self.assertEqual((result.phase, result.reason),
                         (Phase.INCIDENT, "lookup-binding-inconsistent"))

    def test_opaque_never_retries(self):
        opaque = submitted("action-1", "run-1", "d" * 64, ReceiverKind.OPAQUE)
        unknown = send_ambiguous(opaque)
        result = lookup_result(unknown, None)
        self.assertEqual((result.phase, result.reason),
                         (Phase.INCIDENT, "opaque-effect-unknown"))
        self.assertFalse(result.may_submit)
        self.assertEqual(send_completed(opaque, self.receipt).phase, Phase.CONFIRMED)

    def test_wrong_reply_and_illegal_transition(self):
        bad = {**self.receipt, "action_id": "other"}
        self.assertEqual(send_completed(self.base, bad).phase, Phase.INCIDENT)
        with self.assertRaises(ValueError):
            send_completed(send_ambiguous(self.base), self.receipt)

    def test_journal_reopen_and_conflicting_binding(self):
        with nullcontext(tempfile.mkdtemp(prefix="exo-proto-interpreter-", dir="/tmp")) as directory:
            path = Path(directory) / "outcomes.sqlite3"
            journal = OutcomeJournal(path)
            record, created = journal.begin(self.base)
            self.assertTrue(created)
            journal.put(send_ambiguous(record))
            journal.close()
            journal = OutcomeJournal(path)
            record, created = journal.begin(self.base)
            self.assertFalse(created)
            self.assertEqual(record.phase, Phase.UNKNOWN)
            self.assertFalse(record.may_submit)
            with self.assertRaises(ValueError):
                journal.begin(submitted("action-1", "different", "d" * 64,
                                        ReceiverKind.PARTICIPATING))
            journal.close()


class ReleaseOutcomeTests(unittest.TestCase):
    def setUp(self):
        self.base = submitted("release-1", "run-1", "d" * 64,
            ReceiverKind.PARTICIPATING, effect_kind=EffectKind.RELEASE,
            revision="r2", sha256="a" * 64, lookup_limit=2, message_id="message-1")
        # The factory-side record of an A2A release receipt.
        self.receipt = {"release_id": "release-1", "run_id": "run-1",
            "definition_digest": "d" * 64, "revision": "r2", "sha256": "a" * 64,
            "message_id": "message-1", "task_id": "task-1", "receipt_id": "receipt-1",
            "byte_length": 12}

    def test_exact_release_reply_confirmed(self):
        result = send_completed(self.base, self.receipt)
        self.assertEqual((result.phase, result.receipt), (Phase.CONFIRMED, self.receipt))
        self.assertFalse(result.may_submit)

    def test_uncertain_participating_release_lookup_confirms(self):
        unknown = send_ambiguous(self.base)
        self.assertFalse(unknown.may_submit)
        result = lookup_result(unknown, self.receipt)
        self.assertEqual((result.phase, result.lookup_count), (Phase.CONFIRMED, 1))
        with self.assertRaises(ValueError):
            send_completed(unknown, self.receipt)

    def test_release_lookup_exhaustion_and_opaque_incident(self):
        unknown = send_ambiguous(self.base)
        still_unknown = lookup_result(unknown, None)
        self.assertEqual(still_unknown.phase, Phase.UNKNOWN)
        incident = lookup_result(still_unknown, None)
        self.assertEqual((incident.phase, incident.reason), (Phase.INCIDENT, "lookup-exhausted"))
        opaque = submitted("release-1", "run-1", "d" * 64,
            ReceiverKind.OPAQUE, effect_kind=EffectKind.RELEASE,
            revision="r2", sha256="a" * 64)
        opaque_incident = lookup_result(send_ambiguous(opaque), None)
        self.assertEqual((opaque_incident.phase, opaque_incident.reason),
                         (Phase.INCIDENT, "opaque-effect-unknown"))
        self.assertFalse(opaque_incident.may_submit)

    def test_release_receipt_inconsistencies(self):
        for change in ({"release_id": "other"}, {"run_id": "other"},
                       {"definition_digest": "other"}, {"revision": "r1"},
                       {"sha256": "b" * 64}, {"message_id": "other"}, {"task_id": ""},
                       {"receipt_id": None}, {"byte_length": "12"}):
            with self.subTest(change=change):
                bad = {**self.receipt, **change}
                self.assertEqual(send_completed(self.base, bad).phase, Phase.INCIDENT)
                self.assertEqual(lookup_result(send_ambiguous(self.base), bad).phase,
                                 Phase.INCIDENT)

    def test_release_journal_reopen_and_exact_binding(self):
        with nullcontext(tempfile.mkdtemp(prefix="exo-proto-interpreter-", dir="/tmp")) as directory:
            path = Path(directory) / "outcomes.sqlite3"
            journal = OutcomeJournal(path)
            record, created = journal.begin(self.base)
            self.assertTrue(created)
            journal.put(send_ambiguous(record))
            journal.close()
            journal = OutcomeJournal(path)
            record, created = journal.begin(self.base)
            self.assertFalse(created)
            self.assertEqual(record.phase, Phase.UNKNOWN)
            self.assertFalse(record.may_submit)
            with self.assertRaises(ValueError):
                journal.begin(submitted("release-1", "run-1", "d" * 64,
                    ReceiverKind.PARTICIPATING, effect_kind=EffectKind.RELEASE,
                    revision="r2", sha256="b" * 64, message_id="message-1"))
            with self.assertRaises(ValueError):
                journal.begin(submitted("release-1", "run-1", "d" * 64,
                    ReceiverKind.PARTICIPATING, effect_kind=EffectKind.RELEASE,
                    revision="r2", sha256="a" * 64, message_id="message-2"))
            journal.close()


class IncidentProjectionTests(unittest.TestCase):
    def test_quality_and_unknown_effect_project_failed_without_release(self):
        for kind in ("quality-incident", "unresolved-assignment", "unresolved-release"):
            with self.subTest(kind=kind):
                result = incident_result("run-1", "d" * 64, kind, {"action_id": "a-1"})
                self.assertEqual(result["status"], "incident")
                self.assertIs(result["released"], False)
                self.assertEqual(public_task_state("child-incident", result["status"]), "failed")

    def test_regular_projection_and_mismatch(self):
        self.assertEqual(public_task_state("awaiting-child"), "input-required")
        self.assertEqual(public_task_state("parallel"), "working")
        self.assertEqual(public_task_state("accepted"), "working")
        self.assertEqual(public_task_state("child-incident"), "working")
        self.assertEqual(public_task_state("accepted", "accepted"), "completed")
        with self.assertRaises(ValueError):
            public_task_state("child-incident", "accepted")


if __name__ == "__main__":
    unittest.main()
