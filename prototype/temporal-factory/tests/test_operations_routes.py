"""Synthetic HTTP tests for the mounted incident Operations slice."""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

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
from operations_routes import install_operations_routes  # noqa: E402


INCIDENT = {
    "id": "inc-test", "factory_id": "factory-route", "run_id": "run-1",
    "subject_kind": "assignment", "subject_id": "assignment-1",
    "failure_class": "worker_timeout", "generation": 1, "state": "open",
    "version": 1, "acknowledged_by": None, "acknowledged_at": None,
    "owner_id": None, "claim_epoch": 0, "evidence_refs": ["a" * 64],
    "protected_evidence_refs": ["b" * 64], "recovery_evidence_refs": [],
}


class StubOperations:
    factory_id = "factory-route"

    def __init__(self):
        self.calls: list[tuple[str, object, tuple, dict]] = []
        self.error: Exception | None = None

    def _result(self, method, principal, args, kwargs):
        self.calls.append((method, principal, args, kwargs))
        if self.error is not None:
            error = self.error
            self.error = None
            raise error
        return dict(INCIDENT)

    def report_incident(self, principal, **kwargs):
        return self._result("report_incident", principal, (), kwargs)

    def get_incident(self, principal, incident_id):
        return self._result("get_incident", principal, (incident_id,), {})

    def list_incidents(self, principal, *, run_id=None, limit):
        result = self._result("list_incidents", principal, (), {"run_id": run_id, "limit": limit})
        return [result]

    def acknowledge_incident(self, principal, incident_id, *, expected_version):
        return self._result("acknowledge_incident", principal, (incident_id,),
                            {"expected_version": expected_version})

    def claim_incident(self, principal, incident_id, *, expected_version, takeover):
        return self._result("claim_incident", principal, (incident_id,),
                            {"expected_version": expected_version, "takeover": takeover})

    def escalate_incident(self, principal, incident_id, *, expected_version, reason, evidence_refs):
        return self._result("escalate_incident", principal, (incident_id,),
                            {"expected_version": expected_version, "reason": reason,
                             "evidence_refs": evidence_refs})


class OperationsRoutesTests(unittest.TestCase):
    def setUp(self):
        self.current_principal = ["principal-from-runtime-session"]
        self.operations = StubOperations()
        self.app = FastAPI()

        @self.app.middleware("http")
        async def server_session(request, call_next):
            request.state.principal = self.current_principal[0]
            return await call_next(request)

        def authenticate(request):
            return request.state.principal

        install_operations_routes(self.app, self.operations, authenticate, max_list_limit=20)
        self.client = TestClient(self.app)

    def tearDown(self):
        self.client.close()

    def test_report_uses_server_principal_and_rejects_client_identity_or_factory(self):
        request = {
            "run_id": "run-1", "subject_kind": "assignment", "subject_id": "assignment-1",
            "failure_class": "worker_timeout", "generation": 1, "evidence_refs": ["a" * 64],
        }
        rejected = self.client.post("/incidents", json={**request, "actor_identity": "attacker"})
        self.assertEqual(rejected.status_code, 400)
        self.assertEqual(self.operations.calls, [])

        response = self.client.post("/incidents", json=request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.operations.calls[-1][0], "report_incident")
        self.assertEqual(self.operations.calls[-1][1], "principal-from-runtime-session")
        self.assertEqual(self.operations.calls[-1][3], request)

        with_factory = self.client.post("/incidents", json={**request, "factory_id": "other"})
        self.assertEqual(with_factory.status_code, 400)

    def test_get_list_require_auth_and_explicit_bounded_limit(self):
        self.current_principal[0] = None
        self.assertEqual(self.client.get("/incidents/inc-test").status_code, 401)
        self.assertEqual(self.operations.calls, [])

        self.current_principal[0] = "principal-from-runtime-session"
        self.assertEqual(self.client.get("/incidents").status_code, 400)
        self.assertEqual(self.client.get("/incidents?limit=21").status_code, 400)
        self.assertEqual(self.client.get("/incidents?limit=1&factory_id=other").status_code, 400)

        listed = self.client.get("/incidents?run_id=run-1&limit=4")
        self.assertEqual(listed.status_code, 200)
        self.assertEqual(listed.json(), {"incidents": [INCIDENT]})
        self.assertEqual(self.operations.calls[-1], (
            "list_incidents", "principal-from-runtime-session", (),
            {"run_id": "run-1", "limit": 4}))
        fetched = self.client.get("/incidents/inc-test")
        self.assertEqual(fetched.json(), INCIDENT)

    def test_ack_claim_and_escalation_delegate_exact_versioned_contracts(self):
        ack = self.client.post("/incidents/inc-test/acknowledge",
                               json={"expected_version": 1})
        self.assertEqual(ack.status_code, 200)
        self.assertEqual(self.operations.calls[-1][0], "acknowledge_incident")
        self.assertEqual(self.operations.calls[-1][3], {"expected_version": 1})
        self.assertEqual(self.client.post("/incidents/inc-test/acknowledge",
                                          json={"expected_version": "1"}).status_code, 400)

        claim = self.client.post("/incidents/inc-test/claim",
                                 json={"expected_version": 2, "takeover": False})
        self.assertEqual(claim.status_code, 200)
        self.assertEqual(self.operations.calls[-1][0], "claim_incident")
        self.assertEqual(self.operations.calls[-1][3], {"expected_version": 2, "takeover": False})

        escalate = self.client.post("/incidents/inc-test/escalate", json={
            "expected_version": 3, "reason": "deadline_exhausted", "evidence_refs": ["c" * 64],
        })
        self.assertEqual(escalate.status_code, 200)
        self.assertEqual(self.operations.calls[-1][0], "escalate_incident")

    def test_recovery_is_explicitly_unavailable_without_runtime_owner_action(self):
        response = self.client.get("/operations/capabilities")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["recovery"], {
            "available": False, "reason": "no_runtime_owned_per_run_recovery_action"})
        self.assertEqual(self.client.post("/incidents/inc-test/recovery", json={}).status_code, 404)

    def test_operation_errors_are_mapped_without_leaking_internal_exceptions(self):
        cases = [
            (OperationsForbidden("private detail"), 403, "not_authorized"),
            (OperationsNotFound("private detail"), 404, "not_found"),
            (OperationsConflict("stale detail"), 409, "conflict"),
            (OperationsStale("stale detail"), 409, "conflict"),
            (OperationsPolicyError("policy detail"), 422, "invalid_or_disallowed_request"),
            (RuntimeError("secret internal detail"), 503, "operations_unavailable"),
        ]
        for error, status, code in cases:
            with self.subTest(status=status, code=code):
                self.operations.error = error
                response = self.client.get("/incidents/inc-test")
                self.assertEqual(response.status_code, status)
                self.assertEqual(response.json()["error"], code)
                self.assertNotIn("secret internal detail", response.text)

    def test_mount_requires_explicit_runtime_limit(self):
        with self.assertRaises(TypeError):
            install_operations_routes(self.app, self.operations, lambda request: "principal")
        with self.assertRaises(ValueError):
            install_operations_routes(self.app, self.operations,
                                      lambda request: "principal", max_list_limit=0)

    def test_incident_routes_mount_real_operations_with_recovery_explicitly_disabled(self):
        class Adapter:
            def authorize(self, principal, factory_id, capability, resource_id):
                if principal != "server-principal" or factory_id != "factory-route":
                    raise PermissionError("not authorized")
                return "actor:runtime-session"

            def public_run_snapshot(self, principal, factory_id, run_id):
                if (principal, factory_id, run_id) != (
                        "server-principal", "factory-route", "run-1"):
                    raise KeyError("run is unavailable")
                return {"schema_version": 1, "state": {
                    "factory": {"id": factory_id},
                    "runs": [{"id": run_id,
                              "assignments": [{"id": "assignment-1", "attempts": []}],
                              "quality": []}],
                }}

            def current_publication(self, factory_id):
                return {"manifest_digest": "a" * 64}

            def quality_policy_digest(self, factory_id):
                return "b" * 64

            def verify_recovery(self, factory_id, incident, action_id, evidence_refs):
                raise AssertionError("recovery is disabled and must not be called")

        with tempfile.TemporaryDirectory() as home:
            operations = FactoryOperations(
                Adapter(), Path(home) / "operations.sqlite", factory_id="factory-route",
                allowed_recovery_actions=[], max_recovery_attempts=None)
            app = FastAPI()
            install_operations_routes(
                app, operations, lambda request: "server-principal", max_list_limit=12)
            with TestClient(app) as client:
                reported = client.post("/incidents", json={
                    "run_id": "run-1", "subject_kind": "assignment",
                    "subject_id": "assignment-1", "failure_class": "worker_timeout",
                    "generation": 1, "evidence_refs": ["c" * 64],
                })
                self.assertEqual(reported.status_code, 200)
                self.assertEqual(reported.json()["state"], "open")
                incident_id = reported.json()["id"]
                ack = client.post(f"/incidents/{incident_id}/acknowledge",
                                  json={"expected_version": 1})
                self.assertEqual(ack.status_code, 200)
                self.assertEqual(ack.json()["state"], "acknowledged")
                capabilities = client.get("/operations/capabilities")
                self.assertFalse(capabilities.json()["recovery"]["available"])


if __name__ == "__main__":
    unittest.main()
