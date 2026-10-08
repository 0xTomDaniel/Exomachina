"""Synthetic mounted Operations adapter test over public Observation snapshots."""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from observation import FactoryObservation, SourcePage  # noqa: E402
from operations import OperationsPolicyError  # noqa: E402
from runtime_operations import (  # noqa: E402
    RuntimeObservationOperationsAdapter,
    install_runtime_operations,
)


TIME = "2026-10-02T12:00:00Z"
FACTORY = "factory-ops"
OTHER_FACTORY = "factory-other"
PRINCIPAL = "principal:runtime-session"
ACCEPTED_DIGEST = "a" * 64
OTHER_DIGEST = "b" * 64


def source_records(factory_id: str, runs: list[tuple[str, str]]) -> list[dict]:
    """Build controlled public facts: (run ID, accepted artifact digest)."""
    records = [{
        "source_kind": "task", "source_id": "factory-discovered",
        "factory_id": factory_id, "time": TIME,
        "event_type": "com.exomachina.factory.discovered.v1",
        "fields": {"name": "Controlled factory", "identity": "org/example",
                   "capability": "temporal-factory"},
    }, {
        "source_kind": "publication", "source_id": "publication-1",
        "factory_id": factory_id, "time": TIME,
        "event_type": "com.exomachina.publication.activated.v1",
        "manifest_digest": "c" * 64, "package_digest": "d" * 64,
        "definition_digest": "e" * 64, "interpreter_build": "build-1",
        "fields": {"publication_version": "1", "graph_nodes": []},
    }]
    for run_id, artifact_digest in runs:
        common = {"run_id": run_id, "task_id": f"task-{run_id}",
                  "context_id": f"context-{run_id}",
                  "manifest_digest": "c" * 64, "package_digest": "d" * 64,
                  "definition_digest": "e" * 64, "interpreter_build": "build-1"}
        records.append({
            "source_kind": "task", "source_id": f"{run_id}-created",
            "factory_id": factory_id, "time": TIME,
            "event_type": "com.exomachina.run.created.v1", **common,
            "fields": {"state": "completed", "phase": "accepted",
                       "started_at": TIME},
        })
        records.append({
            "source_kind": "outcome", "source_id": f"{run_id}-quality",
            "factory_id": factory_id, "time": TIME,
            "event_type": "com.exomachina.quality.verdict.v1",
            "run_id": run_id, "task_id": f"task-{run_id}",
            "fields": {"artifact_revision": f"revision-{run_id}",
                       "artifact_sha256": artifact_digest,
                       "reviewer_identity": "service/quality", "accepted": True,
                       "finding_count": 0},
        })
    return records


class ControlledObservationSource:
    """Finite controlled source that checks a server-resolved principal."""

    def __init__(self):
        self.records = {
            FACTORY: source_records(FACTORY, [
                ("run-actual", ACCEPTED_DIGEST), ("run-sibling", OTHER_DIGEST)]),
            OTHER_FACTORY: source_records(OTHER_FACTORY, [
                ("run-foreign", OTHER_DIGEST)]),
        }
        self.authorizations: list[tuple[object, str]] = []
        self.page_reads: list[tuple[object, str, str | None]] = []

    def authorize(self, principal, factory_id):
        self.authorizations.append((principal, factory_id))
        if principal != PRINCIPAL:
            raise PermissionError("not authorized")
        if factory_id not in self.records:
            raise PermissionError("unknown factory")

    def discover(self, principal):
        if principal != PRINCIPAL:
            raise PermissionError("not authorized")
        return []

    def read_page(self, principal, factory_id, after_cursor, *, limit):
        self.authorize(principal, factory_id)
        self.page_reads.append((principal, factory_id, after_cursor))
        if after_cursor is None:
            rows = self.records[factory_id][:limit]
            return SourcePage(rows, f"source:{factory_id}:loaded", False,
                              {"status": "current", "observed_at": TIME})
        return SourcePage([], after_cursor, False,
                          {"status": "current", "observed_at": TIME})


class RuntimeOperationsAdapterTests(unittest.TestCase):
    def test_harness_publication_context_reads_and_verifies_one_active_record(self):
        import harness

        package_digest = "c" * 64
        manifest_digest = "a" * 64
        quality_policy_digest = "b" * 64
        package = {"root": {"type": "complete"}}
        publication = {
            "manifest_digest": manifest_digest,
            "package_digest": package_digest,
            "build_id": "build-1",
            "closure": {
                "manifest": {"package_digest": package_digest,
                             "quality_policy_digest": quality_policy_digest,
                             "interpreter": {"build_id": "build-1"}},
                "manifest_digest": manifest_digest,
                "quality_policy": {"policy": "pinned"},
            },
        }
        publications = SimpleNamespace(active=Mock(return_value=publication))
        module = SimpleNamespace(publications=publications,
                                 package=Mock(return_value=package))
        director = SimpleNamespace(identity=FACTORY, module=module)

        with patch.object(harness, "verify_closure",
                          return_value=manifest_digest) as verify:
            context = harness._verified_active_publication_context(director, FACTORY)
        self.assertEqual(context, {"manifest_digest": manifest_digest,
                                   "quality_policy_digest": quality_policy_digest})
        publications.active.assert_called_once_with()
        module.package.assert_called_once_with(package_digest)
        verify.assert_called_once()

        publications.active.reset_mock()
        with self.assertRaises(PermissionError):
            harness._verified_active_publication_context(director, OTHER_FACTORY)
        publications.active.assert_not_called()

    def test_atomic_publication_context_fails_closed_without_split_reads(self):
        source = ControlledObservationSource()
        observation = FactoryObservation(source, ":memory:")
        adapter = RuntimeObservationOperationsAdapter(
            observation, factory_id=FACTORY,
            authorize_principal=lambda *_: "actor:runtime-operator")

        with patch.object(adapter, "current_publication",
                          wraps=adapter.current_publication) as manifest_reader, \
                patch.object(adapter, "quality_policy_digest",
                             wraps=adapter.quality_policy_digest) as quality_reader:
            with self.assertRaises(OperationsPolicyError):
                adapter.publication_context(FACTORY)
            with self.assertRaises(PermissionError):
                adapter.publication_context(OTHER_FACTORY)
            manifest_reader.assert_not_called()
            quality_reader.assert_not_called()

    def test_atomic_publication_context_uses_one_scoped_reader_and_validates_pins(self):
        source = ControlledObservationSource()
        observation = FactoryObservation(source, ":memory:")
        expected = {"manifest_digest": "a" * 64,
                    "quality_policy_digest": "b" * 64}
        calls = []

        def read_context(factory_id):
            calls.append(factory_id)
            return expected

        adapter = RuntimeObservationOperationsAdapter(
            observation, factory_id=FACTORY,
            authorize_principal=lambda *_: "actor:runtime-operator",
            publication_context_reader=read_context)
        self.assertEqual(adapter.publication_context(FACTORY), expected)
        self.assertEqual(calls, [FACTORY])
        with self.assertRaises(PermissionError):
            adapter.publication_context(OTHER_FACTORY)
        self.assertEqual(calls, [FACTORY])

        malformed_contexts = (
            {},
            {**expected, "extra": "not-allowed"},
            {"manifest_digest": "bad", "quality_policy_digest": "b" * 64},
        )
        for malformed in malformed_contexts:
            with self.subTest(context=malformed):
                fail_closed = RuntimeObservationOperationsAdapter(
                    observation, factory_id=FACTORY,
                    authorize_principal=lambda *_: "actor:runtime-operator",
                    publication_context_reader=lambda _factory_id: malformed)
                with self.assertRaises(OperationsPolicyError):
                    fail_closed.publication_context(FACTORY)

        unavailable = RuntimeObservationOperationsAdapter(
            observation, factory_id=FACTORY,
            authorize_principal=lambda *_: "actor:runtime-operator",
            publication_context_reader=lambda _factory_id: (_ for _ in ()).throw(
                LookupError("no verified active publication")))
        with self.assertRaises(OperationsPolicyError):
            unavailable.publication_context(FACTORY)

    def test_mounted_incident_uses_authenticated_run_snapshot_and_preserves_acceptance_refs(self):
        with tempfile.TemporaryDirectory() as home:
            source = ControlledObservationSource()
            observation = FactoryObservation(
                source, Path(home) / "observation.sqlite", page_size=32)
            authorizations = []

            def authenticate(request: Request):
                if request.headers.get("authorization") == "Bearer test-session":
                    return PRINCIPAL
                return None

            def authorize_principal(principal, factory_id, capability, resource_id):
                authorizations.append((principal, factory_id, capability, resource_id))
                if principal != PRINCIPAL or factory_id != FACTORY:
                    raise PermissionError("not authorized")
                return "actor:runtime-operator"

            app = FastAPI()
            operations = install_runtime_operations(
                app, observation=observation,
                database=Path(home) / "operations.sqlite", factory_id=FACTORY,
                authenticate=authenticate, authorize_principal=authorize_principal,
                max_list_limit=16)

            # The adapter delegates to a real FactoryObservation snapshot, scoped
            # to the requested run and authorized with the server principal.
            selected = operations.adapter.public_run_snapshot(
                PRINCIPAL, FACTORY, "run-actual")
            self.assertEqual([run["id"] for run in selected["state"]["runs"]],
                             ["run-actual"])
            self.assertEqual(selected["state"]["runs"][0]["quality"][0]["artifact_sha256"],
                             ACCEPTED_DIGEST)
            self.assertTrue(all(principal == PRINCIPAL and factory == FACTORY
                                for principal, factory in source.authorizations))

            with TestClient(app) as client:
                headers = {"authorization": "Bearer test-session"}
                body = {"run_id": "run-actual", "subject_kind": "run",
                        "subject_id": "run-actual", "failure_class": "worker_timeout",
                        "generation": 1, "evidence_refs": []}
                reported = client.post("/incidents", headers=headers, json=body)
                self.assertEqual(reported.status_code, 200, reported.text)
                incident = reported.json()
                self.assertEqual(incident["protected_evidence_refs"], [ACCEPTED_DIGEST])

                acknowledged = client.post(
                    f"/incidents/{incident['id']}/acknowledge", headers=headers,
                    json={"expected_version": incident["version"]})
                self.assertEqual(acknowledged.status_code, 200, acknowledged.text)
                self.assertEqual(acknowledged.json()["protected_evidence_refs"],
                                 [ACCEPTED_DIGEST])

                # A run from another factory and a nonexistent run cannot use
                # this factory-bound incident adapter.
                for unavailable_run in ("run-foreign", "run-missing"):
                    with self.subTest(run_id=unavailable_run):
                        rejected = client.post("/incidents", headers=headers,
                                               json={**body, "run_id": unavailable_run,
                                                     "subject_id": unavailable_run})
                        self.assertEqual(rejected.status_code, 404, rejected.text)

                self.assertEqual(operations.list_incidents(PRINCIPAL, limit=16),
                                 [acknowledged.json()])

            self.assertIn((PRINCIPAL, FACTORY, "maintenance.incident.report", "run-actual"),
                          authorizations)
            self.assertIn((PRINCIPAL, FACTORY, "maintenance.incident.acknowledge",
                           incident["id"]), authorizations)
            self.assertIn((PRINCIPAL, FACTORY, "maintenance.incident.report", "run-foreign"),
                          authorizations)


if __name__ == "__main__":
    unittest.main()
