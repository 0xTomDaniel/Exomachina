"""One delivery yields exactly one delivery.receipt Observation fact.

Regression: the Runtime source projected a receipt both from the release
Activity's completion and from the run's WORKFLOW_EXECUTION_COMPLETED result
(which repeats the receipt), with different ``destination_id`` and
``delivered_at``. The dashboard reducer correctly flags such a pair as
conflicting receipts, so the floor showed "Delivery unverified" for a
verified delivery. The fact now comes only from the release node, keyed by
the receiver's receipt id, with every field taken from the durable receipt.
"""
from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tests"))

from observation import project_source_record  # noqa: E402
from observation_source import RuntimeObservationSource  # noqa: E402
from test_runtime_observation import FakeDirector  # noqa: E402


RECEIPT_TYPE = "com.exomachina.delivery.receipt.v1"
CONTENT = json.dumps({"kind": "verified_report@1", "revision": "r1", "markdown": "# R\n"},
                     sort_keys=True, separators=(",", ":"))
SHA = hashlib.sha256(CONTENT.encode()).hexdigest()


def a2a_receipt(**changes):
    return {"release_id": "child-1:release:r1", "run_id": "child-1",
            "definition_digest": "c" * 64, "revision": "r1", "sha256": SHA,
            "receipt_id": "4d1b8a8e-4c55-4b8e-9d0e-1f7c2a3b5c6d", "byte_length": len(CONTENT),
            "media_type": "application/json", "accepted_at": "2026-10-08T10:00:01.250000Z",
            "outcome": "delivered", "task_id": "release-task-1", "message_id": "message-1",
            "destination_identity": "a2a-card-0123456789abcdef01234567", "a2a_protocol": "1.0",
            **changes}


class ReleaseReceiptObservationTests(unittest.TestCase):
    def setUp(self):
        self.temp = Path(tempfile.mkdtemp(prefix="exo-release-receipt-obs-", dir="/tmp"))
        self.director = FakeDirector(self.temp / "instance")
        self.source = RuntimeObservationSource(
            self.director, {"name": "factory-test", "capability": {"id": "verified-research@1"}},
            self.temp / "instance" / "runtime-source.sqlite3",
            history_reader=lambda: [], refresh_interval_seconds=0)
        closure = {"manifest_digest": "a" * 64, "manifest": {
            "root_digest": "d" * 64, "interpreter": {"build_id": "b-123456789abc"},
            "services": {}}}
        self.child_document = {"nodes": {
            "publish": {"type": "release", "service": "release", "next": "done",
                        "edges": {"done": "control"}},
            "done": {"type": "complete"}}}
        self.start = lambda run, document: {
            "run": run, "definition_digest": "c" * 64, "package_digest": "b" * 64,
            "closure": closure, "document": document,
            "director": {"identity": self.director.identity, "epoch": 1}}

    def child_history(self, release_result):
        command = {"release_id": "child-1:release:r1", "run_id": "child-1",
                   "definition_digest": "c" * 64, "revision": "r1", "sha256": SHA,
                   "content": CONTENT}
        receipt = {key: value for key, value in release_result.items() if key != "handoff"}
        return [
            {"event_id": 1, "event_type": "WORKFLOW_EXECUTION_STARTED",
             "time": "2026-10-08T10:00:00Z",
             "attributes": {"input": self.start("child-1", self.child_document)}},
            {"event_id": 2, "event_type": "ACTIVITY_TASK_SCHEDULED",
             "time": "2026-10-08T10:00:00.500Z", "attributes": {
                 "activity_id": "release-1", "activity_type": "release",
                 "input": {"identity": "a2a-card-0123456789abcdef01234567",
                           "binding": {"identity": "a2a-card-0123456789abcdef01234567"},
                           "command": command, "node": "publish"}}},
            {"event_id": 3, "event_type": "ACTIVITY_TASK_STARTED",
             "time": "2026-10-08T10:00:00.600Z",
             "attributes": {"scheduled_event_id": 2, "attempt": 1}},
            {"event_id": 4, "event_type": "ACTIVITY_TASK_COMPLETED",
             "time": "2026-10-08T10:00:02Z", "attributes": {
                 "scheduled_event_id": 2, "started_event_id": 3, "result": release_result}},
            {"event_id": 5, "event_type": "WORKFLOW_EXECUTION_COMPLETED",
             "time": "2026-10-08T10:00:03Z", "attributes": {"result": {
                 "status": "accepted", "run": "child-1", "released": True,
                 "artifact": {"revision": "r1", "sha256": SHA, "content": CONTENT},
                 "acceptance": {"revision": "r1", "sha256": SHA}, "receipt": receipt}}},
        ]

    def parent_history(self, receipt):
        document = {"nodes": {"invoke_child": {"type": "nested_factory", "next": "done"},
                              "done": {"type": "complete"}}}
        return [
            {"event_id": 1, "event_type": "WORKFLOW_EXECUTION_STARTED",
             "time": "2026-10-08T09:59:59Z", "attributes": {"input": self.start("run-1", document)}},
            {"event_id": 2, "event_type": "WORKFLOW_EXECUTION_COMPLETED",
             "time": "2026-10-08T10:00:04Z", "attributes": {"result": {
                 "status": "accepted", "run": "run-1", "released": True,
                 "artifact": {"revision": "r1", "sha256": SHA, "content": CONTENT},
                 "receipt": receipt, "child": {"receipt": receipt}}}},
        ]

    def project(self, *histories, times=2):
        for _ in range(times):  # re-projection must be an identical no-op
            for workflow_id, events in histories:
                for record in self.source._history_records(workflow_id, events,
                                                           "task-1", "context-1"):
                    self.source._insert(record)
        with self.source._connect() as db:
            rows = [json.loads(row["record_json"]) for row in db.execute(
                "SELECT record_json FROM source_records ORDER BY source_id")]
        return [row for row in rows if row["event_type"] == RECEIPT_TYPE]

    def test_one_a2a_delivery_projects_exactly_one_receipt_fact(self):
        result = a2a_receipt()
        receipts = self.project(("child-1", self.child_history(result)),
                                ("run-1", self.parent_history(a2a_receipt())))
        self.assertEqual(len(receipts), 1, receipts)
        fact = receipts[0]
        self.assertEqual(fact["source_id"], "delivery-receipt:" + result["receipt_id"])
        self.assertEqual(fact["run_id"], "child-1")
        self.assertEqual(fact["fields"], {
            "receipt_id": result["receipt_id"], "artifact_revision": "r1",
            "artifact_sha256": SHA, "destination_id": result["destination_identity"],
            "delivered_at": result["accepted_at"], "outcome": "delivered"})
        self.assertEqual(fact["time"], result["accepted_at"])
        _key, run_id, _event_id, envelope = project_source_record(fact, "factory-test")
        self.assertEqual((run_id, envelope["type"]), ("child-1", RECEIPT_TYPE))

    def test_unresolved_release_projects_no_receipt(self):
        unresolved = {"unresolved": "output.missing", "release_id": "child-1:release:r1",
                      "task_id": "release-task-1"}
        events = self.child_history(unresolved)[:4]
        self.assertEqual(self.project(("child-1", events)), [])

    def test_receipt_for_other_bytes_is_not_projected(self):
        self.assertEqual(self.project(("child-1", self.child_history(
            a2a_receipt(sha256="e" * 64)))[:4]), [])

    def test_pre_a2a_history_also_projects_one_receipt(self):
        legacy = {"release_id": "child-1:release:r1", "run_id": "child-1",
                  "definition_digest": "c" * 64, "revision": "r1", "sha256": SHA,
                  "attempts": 1, "accepted_effect_count": 1}
        receipts = self.project(("child-1", self.child_history(legacy)),
                                ("run-1", self.parent_history(legacy)))
        self.assertEqual(len(receipts), 1, receipts)
        self.assertEqual(receipts[0]["source_id"], "child-1:4:delivery")


if __name__ == "__main__":
    unittest.main()
