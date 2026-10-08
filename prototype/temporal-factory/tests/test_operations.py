"""Controlled Operations state-machine tests; synthetic, not live qualification."""
from __future__ import annotations

import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from operations import (  # noqa: E402
    FactoryOperations,
    OperationsConflict,
    OperationsForbidden,
    OperationsNotFound,
    OperationsPolicyError,
    OperationsStale,
)


FACTORY = "factory-ops"
PARENT = "a" * 64
QUALITY_POLICY = "b" * 64
REF_1 = "1" * 64
REF_2 = "2" * 64
REF_3 = "3" * 64
REF_4 = "4" * 64
SURFACES = ("/nodes/0/capability", "/nodes/0/name")
NO_CONTEXT_OVERRIDE = object()


class ControlledAdapter:
    """Server-side principals and deterministic authoritative-record controls."""

    actors = {
        "engineer-a": "actor:engineering-a",
        "engineer-b": "actor:engineering-b",
        "director": "actor:director",
        "runtime": "actor:runtime",
        "quality": "actor:quality",
    }

    def __init__(self):
        self.active_manifest = PARENT
        self.quality_digest = QUALITY_POLICY
        self.protected_by_run = {"run-1": [REF_1]}
        self.verify_success = False
        self.recovery_verifications: list[tuple[str, str, list[str]]] = []
        self.snapshot_reads: list[tuple[object, str, str]] = []
        self.publication_context_reads: list[str] = []
        self.current_publication_reads = 0
        self.quality_policy_reads = 0
        self.publication_context_override = NO_CONTEXT_OVERRIDE

    def authorize(self, principal, factory_id, capability, resource_id):
        if factory_id != FACTORY or principal not in self.actors:
            raise PermissionError("not authorized")
        engineering = capability.startswith(("engineering.", "maintenance."))
        director = capability.startswith(("director.", "maintenance."))
        if principal in {"engineer-a", "engineer-b"} and engineering:
            return self.actors[principal]
        if principal == "director" and director:
            return self.actors[principal]
        if principal == "runtime" and capability in {
                "maintenance.recovery.result.record",
                "engineering.candidate.validation.record"}:
            return self.actors[principal]
        if principal == "quality" and capability == "quality.candidate.evaluation.record":
            return self.actors[principal]
        raise PermissionError("capability denied")

    def public_run_snapshot(self, principal, factory_id, run_id):
        if factory_id != FACTORY or principal not in self.actors:
            raise PermissionError("public Observation access denied")
        self.snapshot_reads.append((principal, factory_id, run_id))
        if run_id != "run-1":
            raise KeyError("unknown public run")
        artifacts = []
        quality = []
        for index, digest in enumerate(self.protected_by_run.get(run_id, []), start=1):
            revision = f"accepted-revision-{index}"
            artifacts.append({"artifact_revision": revision, "artifact_sha256": digest})
            quality.append({"artifact_revision": revision, "artifact_sha256": digest,
                            "accepted": True})
        return {"schema_version": 1, "state": {"factory": {"id": FACTORY}, "runs": [{
            "id": run_id,
            "assignments": [{"id": "assignment-1", "attempts": [{
                "provider_identity": "service/provider-1", "capability": "research"}]}],
            "artifacts": artifacts, "quality": quality,
        }]}}

    def current_publication(self, factory_id):
        assert factory_id == FACTORY
        self.current_publication_reads += 1
        return {"manifest_digest": self.active_manifest}

    def quality_policy_digest(self, factory_id):
        assert factory_id == FACTORY
        self.quality_policy_reads += 1
        return self.quality_digest

    def publication_context(self, factory_id):
        assert factory_id == FACTORY
        self.publication_context_reads.append(factory_id)
        if self.publication_context_override is not NO_CONTEXT_OVERRIDE:
            return self.publication_context_override
        return {"manifest_digest": self.active_manifest,
                "quality_policy_digest": self.quality_digest}

    def verify_recovery(self, factory_id, incident, action_id, evidence_refs):
        assert factory_id == FACTORY
        self.recovery_verifications.append((incident["id"], action_id, list(evidence_refs)))
        return self.verify_success


class OperationsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "operations.sqlite"
        self.adapter = ControlledAdapter()
        self.ops = self.new_ops()

    def tearDown(self):
        self.temp.cleanup()

    def new_ops(self, *, attempts=2):
        return FactoryOperations(
            self.adapter, self.database, factory_id=FACTORY,
            allowed_recovery_actions=["restart_assignment", "reconcile_remote_outcome"],
            max_recovery_attempts=attempts, allowed_candidate_surfaces=SURFACES)

    def incident(self, *, generation=1):
        return self.ops.report_incident(
            "engineer-a", run_id="run-1", subject_kind="assignment",
            subject_id="assignment-1", failure_class="worker_timeout",
            generation=generation, evidence_refs=[REF_2])

    def acknowledge_and_claim(self, incident, *, owner="engineer-a"):
        acknowledged = self.ops.acknowledge_incident(
            "director", incident["id"], expected_version=incident["version"])
        claimed = self.ops.claim_incident(
            owner, incident["id"], expected_version=acknowledged["version"])
        return acknowledged, claimed

    def create_validated_candidate(self, *, campaign_id=None, candidate_id="candidate-1"):
        candidate = self.ops.create_candidate(
            "engineer-a", candidate_id=candidate_id, campaign_id=campaign_id,
            parent_manifest_digest=PARENT, hypothesis="Reduce avoidable review repair",
            native_changes=[{"op": "replace", "path": "/nodes/0/name", "value": "research-v2"}],
            allowed_mutable_surface=["/nodes/0/name"],
            predicted_quality_effect="Preserve required acceptance; reduce repair count",
            predicted_cost_effect="Reduce repeated tool use; no amount forecast")
        candidate = self.ops.request_candidate_validation(
            "engineer-a", candidate_id, expected_version=candidate["version"])
        return self.ops.record_candidate_validation(
            "runtime", candidate_id, passed=True, evidence_refs=[REF_2],
            expected_version=candidate["version"])

    def test_duplicate_incidents_coalesce_acknowledgement_is_not_resolution_and_state_is_durable(self):
        first = self.incident()
        duplicate = self.ops.report_incident(
            "engineer-b", run_id="run-1", subject_kind="assignment",
            subject_id="assignment-1", failure_class="worker_timeout", generation=1,
            evidence_refs=[REF_3])

        self.assertEqual(first["id"], duplicate["id"])
        self.assertEqual(duplicate["state"], "open")
        self.assertEqual(duplicate["evidence_refs"], [REF_2, REF_3])
        self.assertEqual(duplicate["protected_evidence_refs"], [REF_1])

        acknowledged = self.ops.acknowledge_incident(
            "director", first["id"], expected_version=duplicate["version"])
        self.assertEqual(acknowledged["state"], "acknowledged")
        self.assertIsNotNone(acknowledged["acknowledged_at"])
        self.assertNotEqual(acknowledged["state"], "closed")
        reopened = self.new_ops().get_incident("engineer-a", first["id"])
        self.assertEqual(reopened["state"], "acknowledged")
        self.assertEqual(reopened["protected_evidence_refs"], [REF_1])
        self.assertIn(("engineer-a", FACTORY, "run-1"), self.adapter.snapshot_reads)

    def test_incident_subject_must_exist_in_authenticated_public_snapshot(self):
        with self.assertRaises(OperationsNotFound):
            self.ops.report_incident(
                "engineer-a", run_id="run-1", subject_kind="assignment",
                subject_id="invented-assignment", failure_class="worker_timeout",
                generation=1, evidence_refs=[REF_2])

        self.assertEqual(self.ops.list_incidents("engineer-a"), [])

    def test_concurrent_claim_has_one_owner_and_stale_contender_cannot_commit(self):
        reported = self.incident()
        acknowledged = self.ops.acknowledge_incident(
            "director", reported["id"], expected_version=reported["version"])

        def claim(principal):
            try:
                result = self.ops.claim_incident(
                    principal, reported["id"], expected_version=acknowledged["version"])
                return ("claimed", result["owner_id"])
            except (OperationsStale, OperationsConflict):
                return ("rejected", None)

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(claim, ["engineer-a", "engineer-b"]))

        self.assertEqual(sum(outcome == "claimed" for outcome, _ in results), 1)
        current = self.ops.get_incident("director", reported["id"])
        self.assertIn(current["owner_id"], {
            "actor:engineering-a", "actor:engineering-b"})
        self.assertEqual(current["claim_epoch"], 1)

    def test_unknown_recovery_requires_reconciliation_and_stale_owner_is_fenced(self):
        reported = self.incident()
        _, claimed = self.acknowledge_and_claim(reported)
        requested = self.ops.request_recovery(
            "engineer-a", reported["id"], action_id="action-1",
            action="restart_assignment", precondition_digest=REF_4,
            claim_epoch=claimed["claim_epoch"], expected_version=claimed["version"])
        unknown = self.ops.record_recovery_result(
            "engineer-a", reported["id"], action_id="action-1", outcome="unknown",
            evidence_refs=[], claim_epoch=claimed["claim_epoch"],
            expected_version=requested["version"])
        self.assertEqual(unknown["state"], "recovery_unknown")
        self.assertEqual(unknown["recovery"]["action_id"], "action-1")

        transferred = self.ops.claim_incident(
            "engineer-b", reported["id"], expected_version=unknown["version"], takeover=True)
        self.assertEqual(transferred["claim_epoch"], claimed["claim_epoch"] + 1)
        with self.assertRaises(OperationsStale):
            self.ops.record_recovery_result(
                "engineer-a", reported["id"], action_id="action-1", outcome="succeeded",
                evidence_refs=[REF_4], claim_epoch=claimed["claim_epoch"],
                expected_version=unknown["version"])

        self.adapter.verify_success = True
        self.adapter.protected_by_run["run-1"].append(REF_3)
        closed = self.ops.record_recovery_result(
            "engineer-b", reported["id"], action_id="action-1", outcome="succeeded",
            evidence_refs=[REF_4], claim_epoch=transferred["claim_epoch"],
            expected_version=transferred["version"])
        self.assertEqual(closed["state"], "closed")
        self.assertEqual(closed["protected_evidence_refs"], [REF_1, REF_3])
        self.assertEqual(closed["recovery_evidence_refs"], [REF_4])
        self.assertEqual(len(self.adapter.recovery_verifications), 1)
        retry = self.ops.record_recovery_result(
            "engineer-b", reported["id"], action_id="action-1", outcome="succeeded",
            evidence_refs=[REF_4], claim_epoch=transferred["claim_epoch"],
            expected_version=transferred["version"])
        self.assertEqual(retry["state"], "closed")
        with self.assertRaises(OperationsConflict):
            self.incident()

    def test_success_without_authoritative_verification_stays_open_for_reconciliation(self):
        reported = self.incident()
        _, claimed = self.acknowledge_and_claim(reported)
        requested = self.ops.request_recovery(
            "engineer-a", reported["id"], action_id="action-unverified",
            action="restart_assignment", precondition_digest=REF_4,
            claim_epoch=claimed["claim_epoch"], expected_version=claimed["version"])
        unverified = self.ops.record_recovery_result(
            "engineer-a", reported["id"], action_id="action-unverified", outcome="succeeded",
            evidence_refs=[REF_4], claim_epoch=claimed["claim_epoch"],
            expected_version=requested["version"])

        self.assertEqual(unverified["state"], "recovery_unverified")
        self.assertIsNotNone(unverified["owner_id"])
        self.assertEqual(unverified["recovery_evidence_refs"], [REF_4])

    def test_attempt_limit_escalates_and_preserves_evidence(self):
        self.ops = self.new_ops(attempts=1)
        reported = self.incident()
        _, claimed = self.acknowledge_and_claim(reported)
        requested = self.ops.request_recovery(
            "engineer-a", reported["id"], action_id="action-failed",
            action="restart_assignment", precondition_digest=REF_4,
            claim_epoch=claimed["claim_epoch"], expected_version=claimed["version"])
        escalated = self.ops.record_recovery_result(
            "engineer-a", reported["id"], action_id="action-failed", outcome="failed",
            evidence_refs=[REF_4], claim_epoch=claimed["claim_epoch"],
            expected_version=requested["version"])

        self.assertEqual(escalated["state"], "escalated")
        self.assertIsNone(escalated["owner_id"])
        self.assertEqual(escalated["evidence_refs"], [REF_2])
        self.assertEqual(escalated["protected_evidence_refs"], [REF_1])
        self.assertEqual(escalated["recovery_evidence_refs"], [REF_4])

    def test_escalation_fences_owner_but_retains_recovery_and_accepted_refs(self):
        reported = self.incident()
        _, claimed = self.acknowledge_and_claim(reported)
        requested = self.ops.request_recovery(
            "engineer-a", reported["id"], action_id="action-pending",
            action="restart_assignment", precondition_digest=REF_4,
            claim_epoch=claimed["claim_epoch"], expected_version=claimed["version"])
        escalated = self.ops.escalate_incident(
            "director", reported["id"], reason="deadline_exhausted", evidence_refs=[REF_3],
            expected_version=requested["version"])

        self.assertEqual(escalated["state"], "escalated")
        self.assertIsNone(escalated["owner_id"])
        self.assertEqual(escalated["protected_evidence_refs"], [REF_1])
        self.assertEqual(escalated["recovery"]["action_id"], "action-pending")
        with self.assertRaises(OperationsStale):
            self.ops.record_recovery_result(
                "engineer-a", reported["id"], action_id="action-pending", outcome="failed",
                evidence_refs=[REF_4], claim_epoch=claimed["claim_epoch"],
                expected_version=requested["version"])

    def test_candidate_requires_allowlisted_native_change_and_current_parent(self):
        with self.assertRaises(OperationsPolicyError):
            self.ops.create_candidate(
                "engineer-a", candidate_id="bad-shell", parent_manifest_digest=PARENT,
                hypothesis="Do a shell operation", native_changes=[{
                    "op": "shell", "path": "/nodes/0/name", "value": "echo unsafe"}],
                allowed_mutable_surface=["/nodes/0/name"],
                predicted_quality_effect="unknown", predicted_cost_effect="unknown")
        with self.assertRaises(OperationsStale):
            self.ops.create_candidate(
                "engineer-a", candidate_id="stale-parent", parent_manifest_digest=REF_4,
                hypothesis="Change one native node", native_changes=[{
                    "op": "replace", "path": "/nodes/0/name", "value": "research-v2"}],
                allowed_mutable_surface=["/nodes/0/name"],
                predicted_quality_effect="Preserve acceptance", predicted_cost_effect="No numeric forecast")

    def test_quality_evaluation_and_promotion_are_independent_and_future_only(self):
        candidate = self.create_validated_candidate()
        candidate = self.ops.request_candidate_evaluation(
            "engineer-a", candidate["id"], expected_version=candidate["version"])
        self.assertEqual(candidate["state"], "evaluation_requested")
        with self.assertRaises(OperationsForbidden):
            self.ops.record_candidate_evaluation(
                "engineer-a", candidate["id"], verdict="passed",
                quality_policy_digest=QUALITY_POLICY, evidence_refs=[REF_3],
                expected_version=candidate["version"])
        evaluated = self.ops.record_candidate_evaluation(
            "quality", candidate["id"], verdict="passed",
            quality_policy_digest=QUALITY_POLICY, evidence_refs=[REF_3],
            expected_version=candidate["version"])

        self.adapter.active_manifest = REF_4
        with self.assertRaises(OperationsStale):
            self.ops.request_candidate_promotion(
                "director", candidate["id"], promotion_request_id="promotion-1",
                expected_version=evaluated["version"])
        self.adapter.active_manifest = PARENT
        promoted = self.ops.request_candidate_promotion(
            "director", candidate["id"], promotion_request_id="promotion-1",
            expected_version=evaluated["version"])

        self.assertEqual(promoted["state"], "promotion_requested")
        self.assertEqual(promoted["parent_manifest_digest"], PARENT)
        request = self.ops.get_promotion_request("director", "promotion-1")
        self.assertEqual(request["parent_manifest_digest"], PARENT)
        self.assertEqual(request["state"], "requested")
        self.assertEqual(self.adapter.active_manifest, PARENT)

    def test_candidate_evaluation_uses_one_coherent_publication_context(self):
        candidate = self.create_validated_candidate(candidate_id="candidate-context")
        self.adapter.publication_context_override = {
            "manifest_digest": PARENT,
            "quality_policy_digest": QUALITY_POLICY,
        }
        # These separate legacy readers now disagree with the exact context.
        # The evaluation request must use only the atomic pair.
        self.adapter.active_manifest = REF_4
        self.adapter.quality_digest = REF_3
        old_reader_counts = (
            self.adapter.current_publication_reads,
            self.adapter.quality_policy_reads,
        )

        requested = self.ops.request_candidate_evaluation(
            "engineer-a", candidate["id"], expected_version=candidate["version"])
        self.assertEqual(requested["state"], "evaluation_requested")
        self.assertEqual(self.adapter.publication_context_reads, [FACTORY])
        self.assertEqual((self.adapter.current_publication_reads,
                          self.adapter.quality_policy_reads), old_reader_counts)
        with self.ops._connect() as db:
            stored = db.execute(
                "SELECT parent_manifest_digest,quality_policy_digest FROM operations_candidates "
                "WHERE candidate_id=?", (candidate["id"],)).fetchone()
        self.assertEqual(stored["parent_manifest_digest"], PARENT)
        self.assertEqual(stored["quality_policy_digest"], QUALITY_POLICY)

    def test_atomic_publication_context_fails_closed_when_missing_or_malformed(self):
        candidate = self.create_validated_candidate(candidate_id="candidate-no-context")
        old_reader_counts = (
            self.adapter.current_publication_reads,
            self.adapter.quality_policy_reads,
        )
        self.adapter.publication_context = None
        with self.assertRaisesRegex(OperationsPolicyError, "unavailable"):
            self.ops.request_candidate_evaluation(
                "engineer-a", candidate["id"], expected_version=candidate["version"])
        del self.adapter.publication_context

        malformed_contexts = (
            None,
            {},
            {"manifest_digest": PARENT},
            {"manifest_digest": PARENT, "quality_policy_digest": QUALITY_POLICY,
             "publication_version": "1"},
            {"manifest_digest": "invalid", "quality_policy_digest": QUALITY_POLICY},
            {"manifest_digest": PARENT, "quality_policy_digest": "invalid"},
        )
        for context in malformed_contexts:
            with self.subTest(context=context):
                self.adapter.publication_context_override = context
                with self.assertRaises(OperationsPolicyError):
                    self.ops.request_candidate_evaluation(
                        "engineer-a", candidate["id"], expected_version=candidate["version"])
        self.assertEqual((self.adapter.current_publication_reads,
                          self.adapter.quality_policy_reads), old_reader_counts)

    def test_enabled_campaign_creation_pins_the_atomic_context_pair(self):
        self.adapter.publication_context_override = {
            "manifest_digest": REF_4,
            "quality_policy_digest": REF_3,
        }
        campaign = self.ops.create_research_campaign(
            "engineer-a", campaign_id="campaign-context", enabled=True,
            allowed_mutable_surface=SURFACES, trial_limit=1, promotion_limit=1,
            quality_limit=1)
        self.assertEqual(campaign["parent_manifest_digest"], REF_4)
        self.assertEqual(campaign["quality_policy_digest"], REF_3)
        self.assertEqual(self.adapter.publication_context_reads, [FACTORY])

    def test_research_is_disabled_with_zero_admissions_until_explicit_finite_enablement(self):
        status = self.ops.research_status("engineer-a")
        self.assertEqual(status["state"], "disabled")
        self.assertEqual(status["admitted_trials"], 0)
        disabled = self.ops.create_research_campaign(
            "engineer-a", campaign_id="campaign-1", allowed_mutable_surface=SURFACES)
        self.assertEqual(disabled["state"], "disabled")
        self.assertEqual(disabled["limits"], {
            "trials": None, "promotions": None, "quality_evaluations": None})
        with self.assertRaises(OperationsPolicyError):
            self.ops.admit_research_trial(
                "engineer-a", "campaign-1", "candidate-1", admission_id="admission-1",
                expected_campaign_version=disabled["version"])
        with self.assertRaises(OperationsPolicyError):
            self.ops.enable_research_campaign(
                "engineer-a", "campaign-1", trial_limit=1, promotion_limit=1,
                quality_limit=0, allowed_mutable_surface=SURFACES,
                expected_version=disabled["version"])

        enabled = self.ops.enable_research_campaign(
            "engineer-a", "campaign-1", trial_limit=1, promotion_limit=1,
            quality_limit=1, allowed_mutable_surface=SURFACES,
            expected_version=disabled["version"])
        self.assertEqual(enabled["state"], "enabled")
        self.assertEqual(enabled["limits"], {
            "trials": 1, "promotions": 1, "quality_evaluations": 1})
        candidate = self.create_validated_candidate(campaign_id="campaign-1")
        admission = self.ops.admit_research_trial(
            "engineer-a", "campaign-1", candidate["id"], admission_id="admission-1",
            expected_campaign_version=enabled["version"])
        self.assertEqual(admission["state"], "admitted")
        campaign = self.ops.get_research_campaign("engineer-a", "campaign-1")
        self.assertEqual(campaign["used"]["trials"], 1)
        self.assertEqual(self.ops.research_status("engineer-a")["admitted_trials"], 1)

        candidate = self.ops.request_candidate_evaluation(
            "engineer-a", candidate["id"], expected_version=candidate["version"])
        campaign = self.ops.get_research_campaign("engineer-a", "campaign-1")
        self.assertEqual(campaign["used"]["quality_evaluations"], 1)
        evaluated = self.ops.record_candidate_evaluation(
            "quality", candidate["id"], verdict="passed",
            quality_policy_digest=QUALITY_POLICY, evidence_refs=[REF_3],
            expected_version=candidate["version"])
        promoted = self.ops.request_candidate_promotion(
            "director", evaluated["id"], promotion_request_id="promotion-research",
            expected_version=evaluated["version"])
        self.assertEqual(promoted["state"], "promotion_requested")
        campaign = self.ops.get_research_campaign("engineer-a", "campaign-1")
        self.assertEqual(campaign["used"], {
            "trials": 1, "promotions": 1, "quality_evaluations": 1})

    def test_enabled_research_admission_limit_is_atomic_and_finite(self):
        disabled = self.ops.create_research_campaign(
            "engineer-a", campaign_id="campaign-race", allowed_mutable_surface=SURFACES)
        enabled = self.ops.enable_research_campaign(
            "engineer-a", "campaign-race", trial_limit=1, promotion_limit=1,
            quality_limit=1, allowed_mutable_surface=SURFACES,
            expected_version=disabled["version"])
        candidate1 = self.create_validated_candidate(
            campaign_id="campaign-race", candidate_id="candidate-race-1")
        candidate2 = self.create_validated_candidate(
            campaign_id="campaign-race", candidate_id="candidate-race-2")

        def admit(candidate, admission_id):
            try:
                return self.ops.admit_research_trial(
                    "engineer-a", "campaign-race", candidate["id"],
                    admission_id=admission_id,
                    expected_campaign_version=enabled["version"])["state"]
            except (OperationsPolicyError, OperationsStale):
                return "rejected"

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(admit, [candidate1, candidate2], ["admit-1", "admit-2"]))

        self.assertEqual(results.count("admitted"), 1)
        self.assertEqual(results.count("rejected"), 1)
        campaign = self.ops.get_research_campaign("engineer-a", "campaign-race")
        self.assertEqual(campaign["used"]["trials"], 1)


if __name__ == "__main__":
    unittest.main()
