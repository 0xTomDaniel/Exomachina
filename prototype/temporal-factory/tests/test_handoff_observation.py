"""Hand-off Observation facts: exact allowlist, history projection, snapshot retention."""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator
from referencing import Registry, Resource

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tests"))

from observation import (FactoryObservation, SourceContractError,  # noqa: E402
                         project_source_record)
from observation_source import RuntimeObservationSource  # noqa: E402
from test_observation import ControlledSource, record, seed_records  # noqa: E402
from test_runtime_observation import FakeDirector  # noqa: E402

FACTORY = "factory-one"
D1, D2, D3 = "1" * 64, "2" * 64, "3" * 64
SHA = "5" * 64
T0, T1 = "2026-10-07T12:00:01.000Z", "2026-10-07T12:00:02.000Z"


def item(index, digest, **extra):
    return {"item_index": index, "source": "artifact", "part_kinds": ["data"],
            "media_type": "application/json", "byte_length": 120, "ready_at": T0,
            "digest": digest, **extra}


def produced(source_id, handoff_id, revision, items, *, run_id="run-a", node="gather"):
    return record(FACTORY, source_id, "com.exomachina.handoff.produced.v1", run_id=run_id,
                  top={"task_id": "task-run-a", "context_id": "context-run-a",
                       "assignment_id": "research_findings", "attempt_id": "1",
                       "manifest_digest": "a" * 64},
                  fields={"node": node, "handoff_id": handoff_id, "handoff_revision": revision,
                          "produced_at": T1, "items": items})


def consumed(source_id, inputs, *, node="draft"):
    return record(FACTORY, source_id, "com.exomachina.handoff.consumed.v1", run_id="run-a",
                  top={"task_id": "task-run-a", "assignment_id": "synth", "attempt_id": "1"},
                  fields={"node": node, "consumed_at": T1, "inputs": inputs})


def schema_validator(name):
    schemas = ROOT / "schemas" / "dashboard" / "v1"
    event = json.loads((schemas / "event-data.schema.json").read_text())
    registry = Registry().with_resource(event["$id"], Resource.from_contents(event))
    target = json.loads((schemas / name).read_text())
    return Draft202012Validator(target, registry=registry)


class HandoffProjectionTests(unittest.TestCase):
    def test_produced_consumed_and_ready_project_exact_fields_only(self):
        _, _, _, event = project_source_record(
            produced("p1", "gather.findings", 1, [item(0, D1), item(1, D2,
                     artifact_revision="r1", artifact_sha256=SHA)]), FACTORY)
        data = event["data"]
        self.assertEqual(set(data), {"schema_version", "factory_id", "run_id", "assignment_id",
                                     "attempt_id", "node", "handoff_id", "handoff_revision",
                                     "produced_at", "items"}, "no Task, context or pins")
        self.assertEqual(data["items"][1]["artifact_sha256"], SHA)
        _, _, _, event = project_source_record(
            consumed("c1", [{"handoff_id": "gather.findings", "item_digests": [D1, D2]}]), FACTORY)
        self.assertEqual(set(event["data"]), {"schema_version", "factory_id", "run_id",
                                              "assignment_id", "attempt_id", "node",
                                              "consumed_at", "inputs"})
        ready = record(FACTORY, "r1", "com.exomachina.handoff.item_ready.v1", run_id="run-a",
                       top={"assignment_id": "research_findings", "attempt_id": "1"},
                       fields={"node": "gather", "handoff_id": "gather.findings", "item_index": 0,
                               "part_kinds": ["text"], "media_type": None, "ready_at": T0})
        _, _, _, ready_event = project_source_record(ready, FACTORY)
        self.assertIsNone(ready_event["data"]["media_type"])
        validator = schema_validator("event.schema.json")
        for value in (event, ready_event):
            validator.validate(value)

    def test_content_bearing_or_malformed_facts_are_rejected(self):
        leaks = [
            ("item name", [item(0, D1, name="secret")]),
            ("item text", [item(0, D1, text="secret")]),
            ("item url", [item(0, D1, url="https://example.invalid")]),
            ("item artifactId", [item(0, D1, artifactId="x")]),
            ("item metadata", [item(0, D1, metadata={})]),
            ("empty hand-off", []),
            ("unkeyed digest", [item(0, "not-a-digest")]),
            ("null length without url", [item(0, D1, byte_length=None)]),
            ("half report reference", [item(0, D1, artifact_revision="r1")]),
            ("two message items", [item(0, D1, source="message"), item(1, D2, source="message")]),
            ("duplicate index", [item(0, D1), item(0, D2)]),
        ]
        for label, items in leaks:
            with self.subTest(label), self.assertRaises(SourceContractError):
                project_source_record(produced("p", "h", 1, items), FACTORY)
        bad = produced("p", "h", 1, [item(0, D1)])
        bad["fields"]["filename"] = "secret.pdf"
        with self.assertRaises(SourceContractError):
            project_source_record(bad, FACTORY)
        for inputs in ([], [{"handoff_id": "h", "item_digests": [D1], "url": "x"}],
                       [{"handoff_id": "h", "item_digests": [D1]}, {"handoff_id": "h", "item_digests": [D2]}]):
            with self.subTest(inputs=inputs), self.assertRaises(SourceContractError):
                project_source_record(consumed("c", inputs), FACTORY)

    def test_snapshots_retain_deduplicated_bounded_timed_handoffs(self):
        rows = seed_records(FACTORY) + [
            produced("p1", "gather.findings", 1, [item(0, D1)]),
            produced("p1-replay", "gather.findings", 1, [item(0, D1)]),
            consumed("c1", [{"handoff_id": "gather.findings", "item_digests": [D1]}]),
            produced("p2", "draft", 1, [item(0, D3, artifact_revision="r1", artifact_sha256=SHA)],
                     node="draft"),
        ]
        temp = Path(tempfile.mkdtemp(prefix="exo-handoff-obs-", dir="/tmp"))
        observation = FactoryObservation(ControlledSource({FACTORY: rows}), temp / "obs.sqlite3")
        observation.refresh("server-principal", FACTORY)
        snapshot = observation.snapshot("server-principal", FACTORY)
        run = next(row for row in snapshot["state"]["runs"] if row["id"] == "run-a")
        self.assertEqual([r["handoff_id"] for r in run["handoffs"]["produced"]],
                         ["gather.findings", "draft"], "a replayed fact is kept once")
        self.assertEqual(run["handoffs"]["consumed"][0]["consumed_at"], T1)
        self.assertEqual(run["handoffs"]["ready"], [])
        self.assertEqual(run["handoffs"]["produced"][0]["produced_at"], T1)
        schema_validator("snapshot.schema.json").validate(snapshot)
        many = seed_records(FACTORY) + [produced(f"p{n}", f"h{n}", 1, [item(0, D1)])
                                        for n in range(300)]
        bounded = FactoryObservation(ControlledSource({FACTORY: many}), temp / "bounded.sqlite3")
        bounded.refresh("server-principal", FACTORY)
        run = bounded.snapshot("server-principal", FACTORY)["state"]["runs"][0]
        self.assertEqual(len(run["handoffs"]["produced"]), 256)
        self.assertEqual(run["handoffs"]["produced"][-1]["handoff_id"], "h299")


class HandoffHistoryProjectionTests(unittest.TestCase):
    """The Runtime source projects the Activity hooks' records from Temporal history."""

    def test_history_projects_consumed_before_dispatch_and_produced_on_complete(self):
        temp = Path(tempfile.mkdtemp(prefix="exo-handoff-history-", dir="/tmp"))
        source = RuntimeObservationSource(FakeDirector(temp / "instance"),
            {"name": "factory-test", "capability": {"id": "verified-research@1"}},
            temp / "source.sqlite3", history_reader=lambda: [], refresh_interval_seconds=0)
        document = {"start": "gather", "nodes": {
            "gather": {"type": "parallel", "branches": {}, "next": "draft"},
            "draft": {"type": "synthesize", "service": "synthesizer", "next": "done"},
            "done": {"type": "complete"}}}
        closure = {"manifest_digest": "a" * 64, "manifest": {"root_digest": "c" * 64,
                   "package_digest": "b" * 64, "interpreter": {"build_id": "b-123456789abc"},
                   "services": {}}}
        record_f = {"handoff_id": "gather.research_findings", "handoff_revision": 1,
                    "produced_at": T0, "items": [item(0, D1)]}
        record_d = {"handoff_id": "draft", "handoff_revision": 1, "produced_at": T1,
                    "items": [item(0, D2, artifact_revision="r1", artifact_sha256=SHA)]}
        history = [
            {"event_id": 1, "event_type": "WORKFLOW_EXECUTION_STARTED", "time": "2026-10-07T12:00:00.000Z",
             "attributes": {"input": {"run": "run-1", "definition_digest": "c" * 64,
                 "package_digest": "b" * 64, "closure": closure, "document": document}}},
            {"event_id": 2, "event_type": "ACTIVITY_TASK_SCHEDULED", "time": "2026-10-07T12:00:00.100Z",
             "attributes": {"activity_id": "1", "activity_type": "assign", "input": {
                 "instance": "research_findings", "node": "gather", "capability": "packet_findings@1",
                 "binding": {"identity": "worker"}, "handoff_id": "gather.research_findings",
                 "handoff_revision": 1}}},
            {"event_id": 3, "event_type": "ACTIVITY_TASK_STARTED", "time": "2026-10-07T12:00:00.200Z",
             "attributes": {"scheduled_event_id": 2, "attempt": 1}},
            {"event_id": 4, "event_type": "ACTIVITY_TASK_COMPLETED", "time": "2026-10-07T12:00:01.100Z",
             "attributes": {"scheduled_event_id": 2, "started_event_id": 3,
                            "result": {"artifact": {}, "content": {}, "handoff": record_f}}},
            {"event_id": 5, "event_type": "ACTIVITY_TASK_SCHEDULED", "time": "2026-10-07T12:00:01.200Z",
             "attributes": {"activity_id": "2", "activity_type": "synthesize", "input": {
                 "node": "draft", "assignment_id": "assign-uuid", "attempt_id": "attempt-uuid",
                 "handoff_id": "draft", "handoff_revision": 1, "consumes": [
                     {"handoff_id": "gather.research_findings", "item_digests": [D1]}]}}},
            {"event_id": 6, "event_type": "ACTIVITY_TASK_STARTED", "time": "2026-10-07T12:00:01.300Z",
             "attributes": {"scheduled_event_id": 5, "attempt": 1}},
            {"event_id": 7, "event_type": "ACTIVITY_TASK_COMPLETED", "time": "2026-10-07T12:00:02.100Z",
             "attributes": {"scheduled_event_id": 5, "started_event_id": 6,
                            "result": {"revision": "r1", "sha256": SHA, "content": "{}", "handoff": record_d}}},
        ]
        records = source._history_records("run-1", history, "task-1", "context-1")
        facts = [r for r in records if ".handoff." in r["event_type"]]
        self.assertEqual([r["event_type"].split(".")[3] for r in facts], ["produced", "consumed", "produced"])
        events = [project_source_record(r, source.factory_id)[3] for r in facts]
        findings, consumption, draft = (event["data"] for event in events)
        self.assertEqual((findings["assignment_id"], findings["attempt_id"], findings["node"]),
                         ("research_findings", "1", "gather"))
        self.assertEqual(consumption["consumed_at"], "2026-10-07T12:00:01.300Z")
        self.assertEqual(consumption["inputs"][0]["item_digests"], [findings["items"][0]["digest"]],
                         "consumer digests chain to the producer's")
        self.assertEqual((draft["assignment_id"], draft["node"]), ("assign-uuid", "draft"))
        self.assertEqual(draft["items"][0]["artifact_sha256"], SHA)
        self.assertEqual(events[0]["time"], T0)
        self.assertTrue(all("task_id" not in event["data"] for event in events))


if __name__ == "__main__":
    unittest.main()
