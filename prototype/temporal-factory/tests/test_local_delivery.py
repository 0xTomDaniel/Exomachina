"""Local byte delivery guarantees; synthetic accepted report, no inference."""
import hashlib
import json
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from threading import Barrier

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from artifact_delivery import accepted_markdown
from local_delivery import DeliveryConflict, LocalDelivery
from local_delivery_routes import install_local_delivery_routes
from fastapi import FastAPI
from fastapi.testclient import TestClient


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.home = Path(self.temporary.name)
        self.content = json.dumps({"kind": "verified_report@1", "revision": "r1",
                                   "markdown": "# Accepted\n\nExact bytes.\n"})
        digest = hashlib.sha256(self.content.encode()).hexdigest()
        self.artifact = accepted_markdown({"revision": "r1", "sha256": digest,
                                          "content": self.content}, revision="r1", sha256=digest)
        self.arguments = {"run_id": "run-1", "task_id": "task-1", "context_id": "context-1",
                          "artifact": self.artifact}

    def ledger(self):
        return LocalDelivery(self.home / "delivery.sqlite3", self.home / "destination",
                             factory_id="factory-1", destination_identity="local-destination-1")

    def test_exact_bytes_distinct_hashes_and_restart_receipt(self):
        first = self.ledger().deliver(**self.arguments)
        self.assertFalse(first["duplicate"])
        self.assertEqual(first["artifact_sha256"], self.artifact.accepted_sha256)
        self.assertNotEqual(first["artifact_sha256"], first["markdown_sha256"])
        self.assertEqual((self.home / "destination" / (first["receipt_id"] + ".md")).read_bytes(),
                         self.artifact.content)
        repeated = self.ledger().deliver(**self.arguments)
        self.assertTrue(repeated["duplicate"])
        self.assertEqual(first["recorded_at"], repeated["recorded_at"])
        self.assertEqual(len(self.ledger().list_receipts(run_id="run-1")), 1)
        self.assertNotIn("content", first)
        self.assertNotIn(str(self.home), json.dumps(first))

    def test_concurrent_independent_readers_commit_one_delivery(self):
        self.ledger()
        barrier = Barrier(2)
        def deliver(_):
            ledger = self.ledger()
            barrier.wait(timeout=5)
            return ledger.deliver(**self.arguments)
        with ThreadPoolExecutor(max_workers=2) as pool:
            receipts = list(pool.map(deliver, range(2)))
        self.assertEqual(sorted(r["duplicate"] for r in receipts), [False, True])
        self.assertEqual(receipts[0]["receipt_id"], receipts[1]["receipt_id"])
        self.assertEqual(len(list((self.home / "destination").glob("*.md"))), 1)

    def test_changed_task_binding_and_corrupted_bytes_fail_closed(self):
        ledger = self.ledger()
        receipt = ledger.deliver(**self.arguments)
        with self.assertRaisesRegex(DeliveryConflict, "binding changed"):
            ledger.deliver(**{**self.arguments, "task_id": "other-task"})
        path = self.home / "destination" / (receipt["receipt_id"] + ".md")
        path.write_bytes(b"corrupt")
        with self.assertRaisesRegex(DeliveryConflict, "bytes changed"):
            ledger.deliver(**self.arguments)
        self.assertEqual(path.read_bytes(), b"corrupt")
        with self.assertRaises(DeliveryConflict):
            ledger.list_receipts()

    def test_bad_digest_and_unconfigured_relative_destination_are_rejected(self):
        with self.assertRaises(DeliveryConflict):
            self.ledger().deliver(**{**self.arguments, "artifact": replace(self.artifact, content=b"wrong")})
        self.assertEqual(self.ledger().list_receipts(), [])
        with self.assertRaises(ValueError):
            LocalDelivery(self.home / "db", Path("relative"), factory_id="factory-1",
                          destination_identity="local-1")

    def test_crash_left_file_is_verified_before_receipt_recovery(self):
        ledger = self.ledger()
        receipt = ledger.deliver(**self.arguments)
        # Simulate a crash after fsynced file publication, before journal commit.
        with ledger._connect() as db:
            db.execute("DELETE FROM local_deliveries")
        recovered = self.ledger().deliver(**self.arguments)
        self.assertEqual(recovered["receipt_id"], receipt["receipt_id"])
        self.assertFalse(recovered["duplicate"])
        self.assertEqual(len(self.ledger().list_receipts()), 1)

    def route_client(self, *, configured=True):
        self.actor = ["operator"]
        artifact = self.artifact
        content = self.content
        self.run = {"id": "run-1", "task": {"id": "task-1", "context_id": "context-1"},
                    "status": {"state": "completed", "phase": "accepted"},
                    "artifacts": [{"artifact_revision": "r1", "artifact_sha256": artifact.accepted_sha256}]}
        run = self.run
        class PublicObservation:
            def snapshot(self, actor, factory_id):
                if actor != "operator" or factory_id != "factory-1":
                    raise PermissionError()
                return {"state": {"factory": {"id": factory_id}, "runs": [run]}}
            def inspect_artifact(self, actor, factory_id, run_id, revision, digest):
                if actor != "operator" or (factory_id, run_id, revision, digest) != (
                        "factory-1", "run-1", "r1", artifact.accepted_sha256):
                    raise PermissionError()
                return {"content": content.encode()}
        app = FastAPI()
        install_local_delivery_routes(app, observation=PublicObservation(), instance_dir=self.home,
                                      factory_id="factory-1", principal=lambda: self.actor[0],
                                      configuration={"destination": str(self.home / "route-destination"),
                                                     "identity": "local-1"} if configured else None)
        return TestClient(app)

    def test_public_route_auth_scope_final_revision_and_no_client_paths(self):
        with self.route_client() as client:
            request = {"run_id": "run-1", "revision": "r1", "sha256": self.artifact.accepted_sha256}
            self.actor[0] = None
            self.assertEqual(client.post("/deliveries", json=request).status_code, 403)
            self.actor[0] = "operator"
            self.assertEqual(client.post("/deliveries", json={**request, "destination": "/unowned"}).status_code, 400)
            self.assertEqual(client.post("/deliveries", json={**request, "run_id": "other-run"}).status_code, 404)
            self.assertEqual(client.post("/deliveries", json={**request, "revision": "old"}).status_code, 409)
            first = client.post("/deliveries", json=request)
            self.assertEqual(first.status_code, 200)
            self.assertFalse(first.json()["receipt"]["duplicate"])
            self.assertTrue(client.post("/deliveries", json=request).json()["receipt"]["duplicate"])
            self.assertEqual(len(client.get("/deliveries?run_id=run-1").json()["receipts"]), 1)

    def test_public_route_does_not_invent_destination_or_acceptance(self):
        with self.route_client(configured=False) as client:
            request = {"run_id": "run-1", "revision": "r1", "sha256": self.artifact.accepted_sha256}
            self.assertEqual(client.post("/deliveries", json=request).status_code, 503)
            self.assertEqual(client.get("/deliveries").json()["status"], "unavailable")
        with self.route_client() as client:
            self.run["status"] = {"state": "working", "phase": "review"}
            self.assertEqual(client.post("/deliveries", json=request).status_code, 409)


if __name__ == "__main__":
    unittest.main()
