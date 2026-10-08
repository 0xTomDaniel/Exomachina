"""Synthetic admission-to-Observation projection tests; not live evidence."""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from admission import AdmissionQueue  # noqa: E402
from admission_observation import (  # noqa: E402
    AdmissionObservationContractError,
    project_admission_observation,
)
from observation import project_source_record  # noqa: E402


FACTORY = "factory-admission"
OTHER_FACTORY = "factory-other"
TIME_CREATED = "2026-10-03T12:00:00.000001+00:00"
TIME_ADMITTED = "2026-10-03T12:00:00.000002+00:00"
TIME_RELEASED = "2026-10-03T12:00:00.000003+00:00"


def request_row(request_id, task_id, state, *, created_at=TIME_CREATED,
                admitted_at=None, released_at=None, factory_id=FACTORY,
                capacity=2, queue_position=None):
    return {
        "factory_id": factory_id, "request_id": request_id, "task_id": task_id,
        "state": state, "capacity": capacity, "queue_position": queue_position,
        "created_at": created_at, "admitted_at": admitted_at,
        "released_at": released_at, "release_id": "private-release-id",
    }


class ControlledQueue:
    factory_id = FACTORY

    def __init__(self, rows, capacity=None):
        self.rows = rows
        self.capacity = capacity or {
            "factory_id": FACTORY, "capacity": 2, "admitted_count": 1,
            "queued_count": 1, "released_count": 1, "available_slots": 1,
        }
        self.calls = []

    def list_requests(self):
        self.calls.append("list_requests")
        return self.rows

    def capacity_view(self):
        self.calls.append("capacity_view")
        return self.capacity


class AdmissionObservationTests(unittest.TestCase):
    def test_projects_current_states_and_durable_admitted_fact_without_queue_rank(self):
        rows = [
            request_row("request:queued", "task-queued", "queued",
                        queue_position=1),
            request_row("request-admitted", "task-admitted", "admitted",
                        admitted_at=TIME_ADMITTED, queue_position=None),
            request_row("request-released", "task-released", "released",
                        admitted_at=TIME_ADMITTED, released_at=TIME_RELEASED),
        ]
        queue = ControlledQueue(rows)
        calls = []
        bindings = {
            "task-queued": ("workflow-queued", "context-queued"),
            "task-admitted": ("workflow-admitted", "context-admitted"),
            "task-released": ("workflow-released", "context-released"),
        }

        projection = project_admission_observation(
            queue, FACTORY,
            lambda task_id: calls.append(task_id) or bindings[task_id])

        self.assertEqual(queue.calls, ["capacity_view", "list_requests"])
        self.assertEqual(calls, ["task-queued", "task-admitted", "task-released"])
        self.assertEqual(projection["capacity"], {
            "factory_id": FACTORY, "capacity_limit": 2,
            "active_count": 1, "queued_count": 1,
        })
        records = projection["records"]
        self.assertEqual(len(records), 4)
        queued = records[0]
        self.assertEqual((queued["source_id"], queued["time"]),
                         ("admission:14:request:queued:queued", TIME_CREATED))
        self.assertEqual((queued["run_id"], queued["task_id"], queued["context_id"]),
                         ("workflow-queued", "task-queued", "context-queued"))
        self.assertEqual(queued["fields"], {
            "admission_id": "request:queued", "state": "queued", "capacity_limit": 2})

        admitted = [item for item in records if item["fields"]["admission_id"] == "request-admitted"]
        self.assertEqual(len(admitted), 1)
        self.assertEqual(admitted[0]["fields"]["state"], "admitted")
        self.assertEqual(admitted[0]["time"], TIME_ADMITTED)

        released = [item for item in records if item["fields"]["admission_id"] == "request-released"]
        self.assertEqual([item["fields"]["state"] for item in released], ["admitted", "released"])
        self.assertEqual([item["time"] for item in released], [TIME_ADMITTED, TIME_RELEASED])
        self.assertEqual([item["source_id"] for item in released], [
            "admission:16:request-released:admitted",
            "admission:16:request-released:released",
        ])
        self.assertTrue(all("queue_position" not in item["fields"] for item in records))
        self.assertNotIn("release_id", repr(projection))
        self.assertNotIn("released_count", projection["capacity"])

        event_schema = json.loads(
            (ROOT / "schemas" / "dashboard" / "v1" / "event-data.schema.json").read_text())
        validator = Draft202012Validator(event_schema)
        for source_record in records:
            event = project_source_record(source_record, FACTORY)[3]
            validator.validate(event["data"])
            self.assertNotIn("queue_position", event["data"])

        repeated = project_admission_observation(queue, FACTORY, lambda task_id: bindings[task_id])
        self.assertEqual(repeated["records"], records)

    def test_uses_real_public_queue_rows_without_inventing_initial_wait(self):
        with tempfile.TemporaryDirectory() as home:
            queue = AdmissionQueue(Path(home) / "admission.sqlite",
                                   factory_id=FACTORY, capacity=1)
            first = queue.enqueue("request-first", task_id="task-first")
            second = queue.enqueue("request-second", task_id="task-second")
            self.assertEqual((first["state"], second["state"]), ("admitted", "queued"))
            release = queue.release("request-first", release_id="release-first")
            self.assertEqual(release["admitted"][0]["request_id"], "request-second")

            bindings = {
                "task-first": ("run-first-actual", "context-first-actual"),
                "task-second": ("run-second-actual", "context-second-actual"),
            }
            projection = project_admission_observation(
                queue, FACTORY, lambda task_id: bindings[task_id])

        first_facts = [row for row in projection["records"]
                       if row["fields"]["admission_id"] == "request-first"]
        second_facts = [row for row in projection["records"]
                        if row["fields"]["admission_id"] == "request-second"]
        self.assertEqual([row["fields"]["state"] for row in first_facts],
                         ["admitted", "released"])
        self.assertEqual([row["time"] for row in first_facts],
                         [first["admitted_at"], release["released"]["released_at"]])
        self.assertEqual([row["fields"]["state"] for row in second_facts], ["admitted"])
        self.assertEqual(second_facts[0]["time"], release["admitted"][0]["admitted_at"])
        self.assertFalse(any(row["fields"]["state"] == "queued"
                             for row in projection["records"]))
        self.assertEqual(projection["capacity"], {
            "factory_id": FACTORY, "capacity_limit": 1,
            "active_count": 1, "queued_count": 0,
        })

    def test_restart_and_dynamic_queue_rank_change_preserve_immutable_facts(self):
        with tempfile.TemporaryDirectory() as home:
            database = Path(home) / "admission.sqlite"
            queue = AdmissionQueue(database, factory_id=FACTORY, capacity=1)
            queue.enqueue("request-first", task_id="task-first")
            queue.enqueue("request-second", task_id="task-second")
            queue.enqueue("request-third", task_id="task-third")
            bindings = {
                "task-first": ("run-first", "context-first"),
                "task-second": ("run-second", "context-second"),
                "task-third": ("run-third", "context-third"),
            }

            initial = project_admission_observation(
                queue, FACTORY, lambda task_id: bindings[task_id])
            initial_by_id = {row["source_id"]: row for row in initial["records"]}
            first_admitted_id = "admission:13:request-first:admitted"
            third_queued_id = "admission:13:request-third:queued"
            self.assertEqual(queue.get("request-third")["queue_position"], 2)
            self.assertEqual(initial["capacity"]["queued_count"], 2)

            # Reopening reads the same durable rows and timestamps. It must
            # yield the same source identities and bytes for existing facts.
            queue = AdmissionQueue(database, factory_id=FACTORY, capacity=1)
            after_restart = project_admission_observation(
                queue, FACTORY, lambda task_id: bindings[task_id])
            self.assertEqual(after_restart, initial)

            queue.release("request-first", release_id="release-first")
            after_capacity_change = project_admission_observation(
                queue, FACTORY, lambda task_id: bindings[task_id])
            after_by_id = {row["source_id"]: row for row in after_capacity_change["records"]}
            third_queue_position_after = queue.get("request-third")["queue_position"]

        self.assertEqual(third_queue_position_after, 1)
        self.assertEqual(after_capacity_change["capacity"], {
            "factory_id": FACTORY, "capacity_limit": 1,
            "active_count": 1, "queued_count": 1,
        })
        self.assertNotEqual(initial["capacity"], after_capacity_change["capacity"])
        self.assertEqual(after_by_id[first_admitted_id], initial_by_id[first_admitted_id])
        self.assertEqual(after_by_id[third_queued_id], initial_by_id[third_queued_id])
        self.assertNotIn("queue_position", after_by_id[third_queued_id]["fields"])
        self.assertIn("admission:14:request-second:admitted", after_by_id)
        self.assertIn("admission:13:request-first:released", after_by_id)

    def test_rejects_foreign_factory_missing_bindings_and_malformed_facts(self):
        valid = request_row("request-1", "task-1", "queued")
        bindings = lambda task_id: ("run-1", "context-1")

        foreign_queue = ControlledQueue([valid])
        foreign_queue.factory_id = OTHER_FACTORY
        with self.assertRaises(AdmissionObservationContractError):
            project_admission_observation(foreign_queue, FACTORY, bindings)

        foreign_row = ControlledQueue([request_row(
            "request-1", "task-1", "queued", factory_id=OTHER_FACTORY)])
        with self.assertRaises(AdmissionObservationContractError):
            project_admission_observation(foreign_row, FACTORY, bindings)

        invalid_cases = (
            (request_row("bad request", "task-1", "queued"), bindings),
            (request_row("request-1", "bad task", "queued"), bindings),
            (request_row("request-1", "task-1", []), bindings),
            (request_row("request-1", "task-1", "queued", created_at="no-time"), bindings),
            (request_row("request-1", "task-1", "admitted", admitted_at=None), bindings),
            (request_row("request-1", "task-1", "released", admitted_at=TIME_ADMITTED,
                         released_at=None), bindings),
            (valid, lambda task_id: None),
            (valid, lambda task_id: ("bad run", "context-1")),
            (valid, lambda task_id: ("run-1", "bad context")),
        )
        for row, lookup in invalid_cases:
            with self.subTest(row=row, lookup=lookup):
                with self.assertRaises(AdmissionObservationContractError):
                    project_admission_observation(ControlledQueue([row]), FACTORY, lookup)

        bad_capacity = ControlledQueue([valid], capacity={
            "factory_id": FACTORY, "capacity": 1, "admitted_count": 2,
            "queued_count": 0, "available_slots": 0,
        })
        with self.assertRaises(AdmissionObservationContractError):
            project_admission_observation(bad_capacity, FACTORY, bindings)


if __name__ == "__main__":
    unittest.main()
