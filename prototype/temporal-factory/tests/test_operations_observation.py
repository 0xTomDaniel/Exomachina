"""Synthetic tests for public incident-to-Observation records; not live evidence."""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from operations_observation import (  # noqa: E402
    OperationsObservationContractError,
    project_operations_incidents,
)
from observation import project_source_record  # noqa: E402
from operations import FactoryOperations  # noqa: E402


FACTORY = "factory-observed"
PRINCIPAL = "principal:runtime-session"
RUN = "run-actual"
INCIDENT = "incident-actual"
REF = "a" * 64
ACCEPTED_REF = "b" * 64
WRITTEN_AT = "2026-10-03T16:24:35.127Z"


def public_incident(**changes):
    row = {
        "id": INCIDENT, "factory_id": FACTORY, "run_id": RUN,
        "failure_class": "worker_timeout", "state": "acknowledged",
        "version": 3, "owner_id": "actor:runtime-operator",
        "evidence_refs": [REF], "protected_evidence_refs": [ACCEPTED_REF],
        "recovery_evidence_refs": ["c" * 64], "history": [{"payload": {"private": True}}],
        "updated_at": WRITTEN_AT,
    }
    row.update(changes)
    return row


class PublicOperationsReader:
    factory_id = FACTORY

    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def list_incidents(self, principal, *, run_id=None, limit=100):
        self.calls.append((principal, run_id, limit))
        return self.rows[:limit]


class OperationsObservationTests(unittest.TestCase):
    def test_projects_versioned_current_row_and_preserves_only_safe_reference_digests(self):
        operations = PublicOperationsReader([
            public_incident(),
            public_incident(id="incident-unclaimed", version=1, owner_id=None,
                            evidence_refs=[], protected_evidence_refs=[]),
        ])

        records = project_operations_incidents(
            operations, PRINCIPAL, FACTORY, limit=8)

        self.assertEqual(operations.calls, [(PRINCIPAL, None, 8)])
        self.assertEqual(records[0], {
            "source_kind": "incident",
            "source_id": "incident-actual:v3",
            "factory_id": FACTORY,
            "run_id": RUN,
            "time": WRITTEN_AT,
            "event_type": "com.exomachina.incident.state_changed.v1",
            "fields": {
                "incident_id": INCIDENT, "kind": "worker_timeout",
                "state": "acknowledged", "owner_identity": "actor:runtime-operator",
                "evidence_refs": [REF, ACCEPTED_REF],
            },
        })
        self.assertEqual(records[1]["source_id"], "incident-unclaimed:v1")
        self.assertNotIn("owner_identity", records[1]["fields"])
        self.assertEqual(records[1]["fields"]["evidence_refs"], [])
        event = project_source_record(records[1], FACTORY)[3]
        self.assertEqual(event["data"]["incident_id"], "incident-unclaimed")
        serialized = repr(records)
        for private in ("protected_evidence_refs", "recovery_evidence_refs", "history", "private"):
            self.assertNotIn(private, serialized)

    def test_reads_a_real_factory_operations_public_row_and_keeps_writer_time(self):
        class Adapter:
            def authorize(self, principal, factory_id, capability, resource_id):
                if principal != PRINCIPAL or factory_id != FACTORY:
                    raise PermissionError("not authorized")
                return "actor:runtime-operator"

            def public_run_snapshot(self, principal, factory_id, run_id):
                if (principal, factory_id, run_id) != (PRINCIPAL, FACTORY, RUN):
                    raise KeyError("unknown run")
                return {"schema_version": 1, "state": {
                    "factory": {"id": FACTORY},
                    "runs": [{"id": RUN, "quality": [{
                        "artifact_revision": "accepted-revision",
                        "artifact_sha256": ACCEPTED_REF, "accepted": True}],
                        "assignments": []}],
                }}

        with tempfile.TemporaryDirectory() as home:
            operations = FactoryOperations(
                Adapter(), Path(home) / "operations.sqlite", factory_id=FACTORY,
                allowed_recovery_actions=[], max_recovery_attempts=None)
            incident = operations.report_incident(
                PRINCIPAL, run_id=RUN, subject_kind="run", subject_id=RUN,
                failure_class="worker_timeout", generation=1, evidence_refs=[REF])
            acknowledged = operations.acknowledge_incident(
                PRINCIPAL, incident["id"], expected_version=incident["version"])
            claimed = operations.claim_incident(
                PRINCIPAL, incident["id"], expected_version=acknowledged["version"])

            [record] = project_operations_incidents(
                operations, PRINCIPAL, FACTORY, limit=8)

        self.assertEqual(record["source_id"], f"{incident['id']}:v3")
        self.assertEqual(record["time"], claimed["updated_at"])
        self.assertEqual(record["fields"]["owner_identity"], "actor:runtime-operator")
        self.assertEqual(record["fields"]["evidence_refs"], [REF, ACCEPTED_REF])
        self.assertEqual(set(record["fields"]), {
            "incident_id", "kind", "state", "owner_identity", "evidence_refs"})

    def test_restart_replays_same_version_and_ack_appends_new_writer_fact(self):
        class Adapter:
            def authorize(self, principal, factory_id, capability, resource_id):
                if principal != PRINCIPAL or factory_id != FACTORY:
                    raise PermissionError("not authorized")
                return "actor:runtime-operator"

            def public_run_snapshot(self, principal, factory_id, run_id):
                if (principal, factory_id, run_id) != (PRINCIPAL, FACTORY, RUN):
                    raise KeyError("unknown run")
                return {"schema_version": 1, "state": {
                    "factory": {"id": FACTORY},
                    "runs": [{"id": RUN, "quality": [], "assignments": []}],
                }}

        with tempfile.TemporaryDirectory() as home:
            database = Path(home) / "operations.sqlite"
            adapter = Adapter()
            operations = FactoryOperations(
                adapter, database, factory_id=FACTORY,
                allowed_recovery_actions=[], max_recovery_attempts=None)
            reported = operations.report_incident(
                PRINCIPAL, run_id=RUN, subject_kind="run", subject_id=RUN,
                failure_class="worker_timeout", generation=1, evidence_refs=[REF])
            [reported_fact] = project_operations_incidents(
                operations, PRINCIPAL, FACTORY, limit=8)

            # A process restart must replay the identical version ID and
            # writer timestamp for unchanged incident state.
            operations = FactoryOperations(
                adapter, database, factory_id=FACTORY,
                allowed_recovery_actions=[], max_recovery_attempts=None)
            [replayed_reported_fact] = project_operations_incidents(
                operations, PRINCIPAL, FACTORY, limit=8)
            acknowledged = operations.acknowledge_incident(
                PRINCIPAL, reported["id"], expected_version=reported["version"])
            [acknowledged_fact] = project_operations_incidents(
                operations, PRINCIPAL, FACTORY, limit=8)

            operations = FactoryOperations(
                adapter, database, factory_id=FACTORY,
                allowed_recovery_actions=[], max_recovery_attempts=None)
            [replayed_acknowledged_fact] = project_operations_incidents(
                operations, PRINCIPAL, FACTORY, limit=8)

        self.assertEqual(replayed_reported_fact, reported_fact)
        self.assertEqual(reported_fact["source_id"], f"{reported['id']}:v1")
        self.assertEqual(reported_fact["time"], reported["updated_at"])
        self.assertEqual(acknowledged_fact["source_id"], f"{reported['id']}:v2")
        self.assertEqual(acknowledged_fact["time"], acknowledged["updated_at"])
        self.assertNotEqual(acknowledged_fact["source_id"], reported_fact["source_id"])
        self.assertEqual(replayed_acknowledged_fact, acknowledged_fact)
        self.assertEqual(reported_fact["fields"]["state"], "open")

    def test_rejects_invalid_identity_version_time_and_reference_rows(self):
        invalid_rows = (
            public_incident(id="bad id"),
            public_incident(run_id="bad id"),
            public_incident(owner_id="bad owner"),
            public_incident(version=True),
            public_incident(version=0),
            public_incident(updated_at="not-a-time"),
            public_incident(updated_at="2026-10-03T16:24:35"),
            public_incident(evidence_refs=["not-a-digest"]),
            public_incident(protected_evidence_refs=["not-a-digest"]),
        )
        for row in invalid_rows:
            with self.subTest(row=row):
                operations = PublicOperationsReader([row])
                with self.assertRaises(OperationsObservationContractError):
                    project_operations_incidents(
                        operations, PRINCIPAL, FACTORY, limit=1)

    def test_rejects_unbounded_limits_and_mismatched_factory_before_read(self):
        operations = PublicOperationsReader([public_incident()])
        for invalid_limit in (0, 257, True, 1.5):
            with self.subTest(limit=invalid_limit):
                with self.assertRaises(OperationsObservationContractError):
                    project_operations_incidents(
                        operations, PRINCIPAL, FACTORY, limit=invalid_limit)
        with self.assertRaises(OperationsObservationContractError):
            project_operations_incidents(
                operations, PRINCIPAL, "factory-other", limit=1)
        self.assertEqual(operations.calls, [])


if __name__ == "__main__":
    unittest.main()
