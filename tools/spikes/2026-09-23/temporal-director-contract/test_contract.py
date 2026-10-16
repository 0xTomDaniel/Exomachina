import copy
import sys
import unittest
from pathlib import Path

from author import materialize, template
from definition import authorize_run_inputs, digest, validate, validate_run_inputs
from failure_projection import (decide_closed_failed_child, failure_incident,
                                no_effect_failure_path, project)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "temporal-quality-reconciliation"))
from a2a_outcome import ReceiverKind, lookup_result, send_ambiguous, send_completed, submitted


class ContractTests(unittest.TestCase):
    def test_mixed_package_schema_is_pinned_and_quality_required(self):
        names = {"source_alpha": "capability", "source_beta": "capability",
                 "counter_alpha": "capability", "counter_beta": "capability",
                 "quality": "quality", "release": "release"}
        bindings = {name: {"role": role, "url": f"http://127.0.0.1:{41470 + n}",
                           "identity": name, "approved": True}
                    for n, (name, role) in enumerate(names.items())}
        package = materialize(template("withheld-a2c-v4-mixed.json"), bindings)
        original = validate(package, bindings)
        self.assertEqual(original, digest(package))
        changed = copy.deepcopy(package)
        changed["run_inputs"] = {}
        with self.assertRaises(ValueError):
            validate(changed, bindings)
        changed = copy.deepcopy(package)
        changed["run_inputs"]["outcome_mode"]["may_affect_acceptance"] = False
        with self.assertRaises(ValueError):
            validate(changed, bindings)
        changed = copy.deepcopy(package)
        del changed["bindings"]["quality"]
        with self.assertRaises(ValueError):
            validate(changed, bindings)

    def test_schema_validation_and_defaults(self):
        schema = {"mode": {"type": "string", "required": True, "enum": ["a", "b"],
                           "source": "caller", "allowed_actors": ["operator"],
                           "may_affect_acceptance": True},
                  "count": {"type": "integer", "required": False, "default": 2,
                            "source": "director", "may_affect_acceptance": False}}
        self.assertEqual(validate_run_inputs(schema, {"mode": "a"}), {"mode": "a", "count": 2})
        self.assertEqual(digest(validate_run_inputs(schema, {"mode": "a"})),
                         digest(validate_run_inputs(schema, {"count": 2, "mode": "a"})))
        values, authority = authorize_run_inputs(schema, {"mode": "a"}, "operator")
        self.assertEqual(values, {"mode": "a", "count": 2})
        self.assertEqual(authority["count"], {"source": "package_default", "actor": "publisher"})
        for wrong in ({}, {"mode": "c"}, {"mode": "a", "unknown": 1},
                      {"mode": "a", "count": True}):
            with self.subTest(wrong=wrong), self.assertRaises(ValueError):
                validate_run_inputs(schema, wrong)

    def test_authorized_sources_and_actor_record(self):
        schema = {
            "mode": {"type": "string", "required": True, "enum": ["a", "b"],
                     "source": "caller", "allowed_actors": ["operator"],
                     "may_affect_acceptance": True},
            "budget": {"type": "integer", "required": True,
                       "source": "director", "may_affect_acceptance": False},
            "upstream": {"type": "string", "required": True,
                         "source": "verified_artifact", "artifact_contract": "outcome@1",
                         "may_affect_acceptance": True},
        }
        artifact = {"contract": "outcome@1", "verified_by": "quality-service",
                    "value": "approved", "sha256": digest("approved")}
        values, authority = authorize_run_inputs(schema, {"mode": "a"}, "operator",
            director_values={"budget": 2}, verified_artifacts={"upstream": artifact})
        self.assertEqual(values, {"mode": "a", "budget": 2, "upstream": "approved"})
        self.assertEqual(authority["mode"], {"source": "caller", "actor": "operator"})
        self.assertEqual(authority["budget"], {"source": "director", "actor": "director"})
        self.assertEqual(authority["upstream"]["artifact_digest"], digest("approved"))
        with self.assertRaisesRegex(ValueError, "actor not authorized"):
            authorize_run_inputs(schema, {"mode": "a"}, "other",
                director_values={"budget": 2}, verified_artifacts={"upstream": artifact})
        for wrong in ({"mode": "a", "budget": 2}, {"mode": "a", "upstream": "approved"}):
            with self.subTest(wrong=wrong), self.assertRaisesRegex(ValueError, "cannot be caller supplied"):
                authorize_run_inputs(schema, wrong, "operator",
                    director_values={"budget": 2}, verified_artifacts={"upstream": artifact})
        with self.assertRaisesRegex(ValueError, "unverified run input artifact"):
            authorize_run_inputs(schema, {"mode": "a"}, "operator",
                director_values={"budget": 2}, verified_artifacts={"upstream": {
                    **artifact, "sha256": "wrong"}})

    def test_status_projection(self):
        for execution in ("FAILED", "TERMINATED", "TIMED_OUT", "CANCELED"):
            self.assertEqual(project({"phase": "awaiting-child"}, execution), "failed")
        self.assertEqual(project({"phase": "child-failed"}, "COMPLETED",
                                 {"status": "failed"}), "failed")
        self.assertEqual(project({"phase": "accepted"}, "COMPLETED",
                                 {"status": "accepted"}), "completed")
        self.assertEqual(project({"phase": "awaiting-director"}, "RUNNING"), "input-required")

    def test_failure_incident_requires_no_authoritative_effect(self):
        expected = {"run_id": "r", "child_id": "r:child:c", "package_digest": "d",
                    "failure_class": "ChildWorkflowError", "timestamp": "t",
                    "authority_conflict": False}
        self.assertEqual(failure_incident("r", "r:child:c", "d", "ChildWorkflowError",
                                          "t", None, None), expected)
        for acceptance, receipt in (({"revision": "r1"}, None), (None, {"release": True})):
            self.assertFalse(no_effect_failure_path(acceptance, receipt))
            self.assertTrue(failure_incident("r", "r:child:c", "d", "ChildWorkflowError",
                                             "t", acceptance, receipt)["authority_conflict"])

    def test_post_acceptance_failure_never_resubmits_release(self):
        binding = {"action_id": "release-r1", "run_id": "child", "definition_digest": "d"}
        accepted = {"revision": "r1", "sha256": "candidate"}
        receipt = {**binding, "task_id": "release-task"}
        participating = submitted(**binding, receiver=ReceiverKind.PARTICIPATING)
        opaque = submitted(**binding, receiver=ReceiverKind.OPAQUE)
        cases = [
            (accepted, receipt, send_completed(participating, receipt),
             "post-acceptance-after-release", "preserve-receipt"),
            (accepted, None, participating,
             "post-acceptance-release-outcome-unknown", "lookup-by-action-id-only"),
            (accepted, None, send_ambiguous(participating),
             "post-acceptance-release-outcome-unknown", "lookup-by-action-id-only"),
            (accepted, None, send_ambiguous(opaque),
             "post-acceptance-release-incident", "manual-incident"),
            (accepted, None, None,
             "post-acceptance-release-unaccounted", "investigate-journal"),
            (None, None, None, "child-failure-before-acceptance", "none"),
        ]
        for acceptance, release_receipt, outcome, incident_class, reconciliation in cases:
            with self.subTest(incident_class=incident_class, reconciliation=reconciliation):
                decision = decide_closed_failed_child(acceptance=acceptance,
                    release_receipt=release_receipt, release_outcome=outcome, **binding)
                self.assertEqual(decision["public_task_state"], "failed")
                self.assertEqual(decision["incident_class"], incident_class)
                self.assertEqual(decision["reconciliation"], reconciliation)
                self.assertFalse(decision["may_submit_release"])
                self.assertEqual(decision["acceptance"], acceptance)
        confirmed_by_lookup = lookup_result(send_ambiguous(participating), receipt)
        decision = decide_closed_failed_child(acceptance=accepted, release_receipt=None,
            release_outcome=confirmed_by_lookup, **binding)
        self.assertEqual(decision["reconciliation"], "preserve-receipt")
        self.assertTrue(decision["preserve_release_receipt"])
        self.assertEqual(decision["release_receipt"], receipt)
        wrong = submitted("another-release", "child", "d", ReceiverKind.PARTICIPATING)
        decision = decide_closed_failed_child(acceptance=accepted, release_receipt=None,
            release_outcome=wrong, **binding)
        self.assertEqual(decision["incident_class"], "release-binding-conflict")
        self.assertFalse(decision["may_submit_release"])


if __name__ == "__main__":
    unittest.main()
