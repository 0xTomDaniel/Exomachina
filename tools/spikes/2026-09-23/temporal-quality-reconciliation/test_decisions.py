"""Pure, in-memory decision tests. No live A2A or Temporal service is used."""
from __future__ import annotations

import copy
import tempfile
import unittest
from pathlib import Path

from a2a_outcome import (EffectKind, OutcomeJournal, Phase, ReceiverKind, lookup_result,
                         send_ambiguous, send_completed, submitted)
from incident_projection import incident_result, public_task_state
from quality_authority import QualityKind, decide_quality, quality_action_id


def evidence(accepted: bool = True) -> dict:
    run, assignment, attempt, revision, sha = "run-1", "run-1:quality", 2, "r2", "a" * 64
    action = quality_action_id(run, assignment, attempt, revision, sha)
    verdict = {"accepted": accepted, "revision": revision, "sha256": sha,
               "reviewer": "quality-1", "reason": "fixture"}
    command = {"op": "review", "action_id": action, "run_id": run,
               "definition_digest": "d" * 64,
               "artifact": {"revision": revision, "sha256": sha, "author": "author-1"}}
    task = {"id": "task-1", "status": {"state": "completed"},
            "metadata": {"action_id": action, "run_id": run,
                         "definition_digest": "d" * 64,
                         "harness_identity": "quality-1", "harness_role": "quality"},
            "artifacts": [{"artifactId": sha, "parts": [{"data": verdict}]}]}
    lookup = {"action_id": action, "run_id": run, "definition_digest": "d" * 64,
              "role": "quality", "task_id": "task-1", "artifact": copy.deepcopy(verdict)}
    return {"binding": {"approved": True, "role": "quality",
                        "url": "http://127.0.0.1:44022", "identity": "quality-1"},
            "observed_endpoint": "http://127.0.0.1:44022",
            "observed_identity": "quality-1", "command": command,
            "assignment_id": assignment, "attempt": attempt,
            "task": task, "lookup": lookup, "send_payload": copy.deepcopy(verdict)}


class QualityDecisionTests(unittest.TestCase):
    def test_genuine_positive_and_negative(self):
        for accepted, kind in ((True, QualityKind.POSITIVE), (False, QualityKind.NEGATIVE)):
            with self.subTest(accepted=accepted):
                self.assertEqual(decide_quality(**evidence(accepted)).kind, kind)

    def test_inconsistencies_route_to_typed_incident(self):
        changes = {
            "identity": lambda x: x.update(observed_identity="other"),
            "endpoint": lambda x: x.update(observed_endpoint="http://127.0.0.1:44023"),
            "task_identity": lambda x: x["task"]["metadata"].update(harness_identity="other"),
            "author_self_review": lambda x: x["command"]["artifact"].update(author="quality-1"),
            "artifact_digest": lambda x: x["task"]["artifacts"][0]["parts"][0]["data"].update(sha256="b" * 64),
            "attempt": lambda x: x.update(attempt=3),
            "assignment": lambda x: x.update(assignment_id="different"),
            "run": lambda x: x["lookup"].update(run_id="other"),
            "task_id": lambda x: x["lookup"].update(task_id="other"),
            "task_state": lambda x: x["task"]["status"].update(state="working"),
            "missing_task_status": lambda x: x["task"].update(status=None),
            "task_lookup_disagreement": lambda x: x["lookup"]["artifact"].update(accepted=False),
            "send_task_disagreement": lambda x: x["send_payload"].update(accepted=False),
            "missing_verdict": lambda x: x["task"].update(artifacts=[]),
            "invalid_boolean": lambda x: x["task"]["artifacts"][0]["parts"][0]["data"].update(accepted=1),
        }
        for name, change in changes.items():
            with self.subTest(name=name):
                data = evidence()
                change(data)
                decision = decide_quality(**data)
                self.assertEqual(decision.kind, QualityKind.INCONSISTENT)
                self.assertEqual(decision.incident, "quality-evidence-inconsistent")
                self.assertIsNone(decision.verdict)


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
        with tempfile.TemporaryDirectory(prefix="exo-tq-quality-", dir="/tmp") as directory:
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
            revision="r2", sha256="a" * 64, lookup_limit=2)
        self.receipt = {"release_id": "release-1", "run_id": "run-1",
            "definition_digest": "d" * 64, "revision": "r2", "sha256": "a" * 64,
            "attempts": 1, "accepted_effect_count": 1}

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
                       {"sha256": "b" * 64}, {"accepted_effect_count": 0},
                       {"attempts": 0}):
            with self.subTest(change=change):
                bad = {**self.receipt, **change}
                self.assertEqual(send_completed(self.base, bad).phase, Phase.INCIDENT)
                self.assertEqual(lookup_result(send_ambiguous(self.base), bad).phase,
                                 Phase.INCIDENT)

    def test_release_journal_reopen_and_exact_binding(self):
        with tempfile.TemporaryDirectory(prefix="exo-tq-quality-", dir="/tmp") as directory:
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
                    revision="r2", sha256="b" * 64))
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
