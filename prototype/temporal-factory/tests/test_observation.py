"""Controlled-record tests for the observation projection; not live evidence."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator, ValidationError
from referencing import Registry, Resource

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from observation import (  # noqa: E402
    CursorExpired,
    FactoryObservation,
    InvalidCursor,
    ObservationForbidden,
    SourcePage,
    SourceContractError,
    project_source_record,
)


DIGEST = "a" * 64
TIME = "2026-10-02T12:00:00Z"
GRAPH = [{"id": "start", "type": "worker", "next": ["finish"]},
         {"id": "finish", "type": "review"}]


def record(factory_id: str, source_id: str, event_type: str, *, run_id: str | None = None,
           source_kind: str = "temporal", top: dict | None = None,
           fields: dict | None = None) -> dict:
    result = {"source_kind": source_kind, "source_id": source_id,
              "factory_id": factory_id, "time": TIME, "event_type": event_type,
              "fields": fields or {}}
    if run_id:
        result["run_id"] = run_id
    if top:
        result.update(top)
    return result


def seed_records(factory_id: str, run_id: str = "run-a") -> list[dict]:
    return [
        record(factory_id, "factory-discovered", "com.exomachina.factory.discovered.v1",
               source_kind="task", fields={"name": "Factory One", "identity": "org/factory-one",
                                            "capability": "temporal-factory"}),
        record(factory_id, "publication-1", "com.exomachina.publication.activated.v1",
               source_kind="publication",
               top={"manifest_digest": DIGEST, "package_digest": "b" * 64,
                    "definition_digest": "c" * 64, "interpreter_build": "build-1"},
               fields={"publication_version": "1.0", "graph_nodes": GRAPH}),
        record(factory_id, f"{run_id}-created", "com.exomachina.run.created.v1",
               source_kind="task", run_id=run_id,
               top={"task_id": f"task-{run_id}", "context_id": f"context-{run_id}",
                    "manifest_digest": DIGEST, "package_digest": "b" * 64,
                    "definition_digest": "c" * 64, "interpreter_build": "build-1"},
               fields={"state": "running", "phase": "execution", "started_at": TIME}),
    ]


class ControlledSource:
    """Opaque source cursors over an ordered, append-only controlled record list."""

    def __init__(self, records_by_factory: dict[str, list[dict]], *, page_size: int = 100):
        self.records_by_factory = records_by_factory
        self.page_size = page_size
        self.allowed_principal = "server-principal"
        self.command_calls: list[tuple[object, str, dict]] = []
        self.prior_outcomes: dict[str, dict] = {}
        self.artifact = b"controlled artifact bytes"
        self._offsets: dict[tuple[str, str], int] = {}

    def discover(self, principal):
        self._check(principal)
        return [{"factory_id": factory_id, "identity": "org/factory-one",
                 "capability": "temporal-factory", "name": "Factory One",
                 "prompt": "private"}
                for factory_id in self.records_by_factory]

    def authorize(self, principal, factory_id):
        self._check(principal)
        if factory_id not in self.records_by_factory:
            raise PermissionError("not found")

    def _check(self, principal):
        if principal != self.allowed_principal:
            raise PermissionError("denied")

    def read_page(self, principal, factory_id, after_cursor, *, limit):
        self.authorize(principal, factory_id)
        offset = 0 if after_cursor is None else self._offsets[(factory_id, after_cursor)]
        rows = self.records_by_factory[factory_id][offset:offset + min(limit, self.page_size)]
        next_offset = offset + len(rows)
        next_cursor = f"source-cursor-{factory_id}-{next_offset}"
        self._offsets[(factory_id, next_cursor)] = next_offset
        return SourcePage(rows, next_cursor, next_offset < len(self.records_by_factory[factory_id]),
                          {"status": "current", "observed_at": TIME})

    def inspect_artifact(self, principal, factory_id, run_id, revision, sha256):
        self.authorize(principal, factory_id)
        return {"content": self.artifact, "sha256": hashlib.sha256(self.artifact).hexdigest(),
                "media_type": "application/octet-stream"}

    def submit_command(self, principal, factory_id, command):
        self.authorize(principal, factory_id)
        self.command_calls.append((principal, factory_id, dict(command)))
        receipt = {"command_id": command["command_id"], "lifecycle": "received"}
        if command["command_id"] in self.prior_outcomes:
            receipt["prior_outcome"] = dict(self.prior_outcomes[command["command_id"]])
        return receipt


class ObservationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.records = {"factory-a": seed_records("factory-a")}
        self.source = ControlledSource(self.records)
        self.projection = FactoryObservation(self.source, Path(self.temp.name) / "observation.sqlite")
        self.principal = self.source.allowed_principal

    def tearDown(self):
        self.temp.cleanup()

    def append_state(self, factory_id="factory-a", run_id="run-a", suffix="next", state="waiting"):
        self.records[factory_id].append(record(
            factory_id, f"state-{suffix}", "com.exomachina.run.state_changed.v1",
            run_id=run_id, fields={"state": state, "phase": "execution"}))

    def test_allowlist_stable_cloudevent_and_dataschema(self):
        unsafe = record("factory-a", "stable-id", "com.exomachina.factory.discovered.v1",
                        source_kind="task", fields={"name": "Factory One", "identity": "org/f1",
                            "capability": "worker", "prompt": "secret prompt",
                            "access_token": "credential"})
        first = project_source_record(unsafe, "factory-a")[3]
        second = project_source_record(unsafe, "factory-a")[3]
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(first["dataschema"], "urn:exomachina:dashboard:event:v1")
        self.assertEqual(first["data"]["schema_version"], 1)
        self.assertNotIn("prompt", first["data"])
        self.assertNotIn("access_token", first["data"])

    def test_assignment_event_preserves_optional_definition_node(self):
        fact = record(
            "factory-a", "assignment-at-pinned-node", "com.exomachina.assignment.state_changed.v1",
            run_id="run-a", top={"task_id": "task-run-a", "assignment_id": "assignment-a",
                                  "attempt_id": "attempt-a"},
            fields={"capability": "research", "state": "running", "node": "research-node"})

        data = project_source_record(fact, "factory-a")[3]["data"]

        self.assertEqual(data["node"], "research-node")
        event_schema = json.loads(
            (ROOT / "schemas" / "dashboard" / "v1" / "event-data.schema.json").read_text())
        Draft202012Validator(event_schema).validate(data)

    def test_assignment_attempt_updates_preserve_linkage_and_clear_invalidated_end(self):
        started_at = "2026-10-02T12:01:00.000Z"
        ended_at = "2026-10-02T12:02:00.000Z"
        initial_cursor = self.projection.snapshot(
            self.principal, "factory-a", run_id="run-a")["cursor"]
        start = record(
            "factory-a", "assignment-attempt-1-start",
            "com.exomachina.assignment.state_changed.v1", run_id="run-a",
            top={"task_id": "task-run-a", "assignment_id": "assignment-a",
                 "attempt_id": "attempt-1"},
            fields={"capability": "research", "provider_identity": "provider-one",
                    "node": "research-node", "state": "running", "started_at": started_at})
        completed = record(
            "factory-a", "assignment-attempt-1-completed",
            "com.exomachina.assignment.state_changed.v1", run_id="run-a",
            top={"task_id": "task-run-a", "assignment_id": "assignment-a",
                 "attempt_id": "attempt-1"},
            fields={"capability": "research", "state": "completed", "ended_at": ended_at})
        self.records["factory-a"].extend([start, completed])

        completed_snapshot = self.projection.snapshot(
            self.principal, "factory-a", run_id="run-a")
        attempts = completed_snapshot["state"]["runs"][0]["assignments"][0]["attempts"]
        first = next(attempt for attempt in attempts if attempt["attempt_id"] == "attempt-1")
        self.assertEqual(first["state"], "completed")
        self.assertEqual(first["started_at"], started_at)
        self.assertEqual(first["ended_at"], ended_at)
        self.assertEqual(first["provider_identity"], "provider-one")
        self.assertEqual(first["node"], "research-node")

        next_attempt = record(
            "factory-a", "assignment-attempt-2-scheduled",
            "com.exomachina.assignment.state_changed.v1", run_id="run-a",
            top={"task_id": "task-run-a", "assignment_id": "assignment-a",
                 "attempt_id": "attempt-2"},
            fields={"capability": "research", "state": "scheduled"})
        correction = record(
            "factory-a", "assignment-attempt-1-attribution-correction-v1",
            "com.exomachina.assignment.state_changed.v1", run_id="run-a",
            top={"task_id": "task-run-a", "assignment_id": "assignment-a",
                 "attempt_id": "attempt-1"},
            fields={"capability": "research", "state": "unknown"})
        self.records["factory-a"].extend([next_attempt, correction])

        corrected_snapshot = self.projection.snapshot(
            self.principal, "factory-a", run_id="run-a")
        attempts = corrected_snapshot["state"]["runs"][0]["assignments"][0]["attempts"]
        by_id = {attempt["attempt_id"]: attempt for attempt in attempts}
        self.assertEqual(set(by_id), {"attempt-1", "attempt-2"})
        self.assertEqual(by_id["attempt-1"]["state"], "unknown")
        self.assertEqual(by_id["attempt-1"]["started_at"], started_at)
        self.assertEqual(by_id["attempt-1"]["provider_identity"], "provider-one")
        self.assertEqual(by_id["attempt-1"]["node"], "research-node")
        self.assertNotIn("ended_at", by_id["attempt-1"])
        self.assertEqual(by_id["attempt-2"]["state"], "scheduled")
        for field in ("started_at", "provider_identity", "node", "ended_at"):
            self.assertNotIn(field, by_id["attempt-2"])

        history = self.projection.events_after(
            self.principal, "factory-a", initial_cursor,
            run_id="run-a")
        expected_event_ids = [project_source_record(fact, "factory-a")[3]["id"]
                             for fact in (start, completed, next_attempt, correction)]
        self.assertEqual(
            [row["event"]["id"] for row in history["events"]], expected_event_ids)
        self.assertEqual(history["events"][1]["event"]["data"]["state"], "completed")
        self.assertEqual(history["events"][1]["event"]["data"]["ended_at"], ended_at)
        self.assertEqual(history["events"][3]["event"]["data"]["state"], "unknown")
        self.assertNotIn("ended_at", history["events"][3]["event"]["data"])

    def test_run_activity_node_is_valid_outside_wait_phases_and_survives_snapshot(self):
        fact = record(
            "factory-a", "run-synthesis-node", "com.exomachina.run.state_changed.v1",
            run_id="run-a", fields={
                "state": "running", "phase": "synthesis", "node": "synthesize"})

        data = project_source_record(fact, "factory-a")[3]["data"]
        self.assertEqual(data["node"], "synthesize")

        self.records["factory-a"].append(fact)
        snapshot = self.projection.snapshot(self.principal, "factory-a")
        status = snapshot["state"]["runs"][0]["status"]
        self.assertEqual(status["state"], "running")
        self.assertEqual(status["phase"], "synthesis")
        self.assertEqual(status["node"], "synthesize")

    def test_incident_owner_identity_is_safe_identifier_in_event_and_snapshot(self):
        fact = record(
            "factory-a", "incident-1-v3", "com.exomachina.incident.state_changed.v1",
            source_kind="incident", run_id="run-a",
            fields={"incident_id": "incident-1", "kind": "worker_timeout",
                    "state": "claimed", "owner_identity": "actor:ops-engineer",
                    "evidence_refs": [DIGEST], "history": [{"private": "details"}],
                    "owner_email": "private@example.invalid"})

        event_schema = json.loads(
            (ROOT / "schemas" / "dashboard" / "v1" / "event-data.schema.json").read_text())
        snapshot_schema = json.loads(
            (ROOT / "schemas" / "dashboard" / "v1" / "snapshot.schema.json").read_text())
        registry = Registry().with_resource(
            event_schema["$id"], Resource.from_contents(event_schema))
        event_validator = Draft202012Validator(event_schema)
        snapshot_validator = Draft202012Validator(
            snapshot_schema, registry=registry,
            format_checker=Draft202012Validator.FORMAT_CHECKER)

        envelope = project_source_record(fact, "factory-a")[3]
        data = envelope["data"]
        event_validator.validate(data)
        self.assertEqual(data["owner_identity"], "actor:ops-engineer")
        self.assertNotIn("history", data)
        self.assertNotIn("owner_email", data)

        self.records["factory-a"].append(fact)
        snapshot = self.projection.snapshot(self.principal, "factory-a", run_id="run-a")
        incident = snapshot["state"]["runs"][0]["incidents"][0]
        self.assertEqual(incident["owner_identity"], "actor:ops-engineer")
        snapshot_validator.validate(snapshot)

        for bad_owner in ("bad owner", "", 42, None, {"id": "actor"}, "x" * 257):
            with self.subTest(owner_identity=bad_owner):
                invalid = json.loads(json.dumps(fact))
                invalid["fields"]["owner_identity"] = bad_owner
                with self.assertRaises(SourceContractError):
                    project_source_record(invalid, "factory-a")

    def test_incident_snapshot_replaces_complete_state_but_journal_keeps_history(self):
        start = self.projection.snapshot(
            self.principal, "factory-a", run_id="run-a")["cursor"]
        self.records["factory-a"].extend([
            record(
                "factory-a", "incident-1-claimed-v1",
                "com.exomachina.incident.state_changed.v1", source_kind="incident",
                run_id="run-a",
                fields={"incident_id": "incident-1", "kind": "worker_timeout",
                        "state": "claimed", "owner_identity": "actor:ops-engineer",
                        "evidence_refs": [DIGEST]}),
            record(
                "factory-a", "incident-1-escalated-v2",
                "com.exomachina.incident.state_changed.v1", source_kind="incident",
                run_id="run-a",
                fields={"incident_id": "incident-1", "kind": "worker_timeout",
                        "state": "escalated", "evidence_refs": [DIGEST]}),
            record(
                "factory-a", "incident-2-open-v1",
                "com.exomachina.incident.state_changed.v1", source_kind="incident",
                run_id="run-a",
                fields={"incident_id": "incident-2", "kind": "artifact_failure",
                        "state": "open", "evidence_refs": ["b" * 64]}),
        ])

        snapshot = self.projection.snapshot(self.principal, "factory-a", run_id="run-a")
        incidents = snapshot["state"]["runs"][0]["incidents"]
        self.assertEqual(len(incidents), 2)
        by_id = {incident["incident_id"]: incident for incident in incidents}
        self.assertEqual(set(by_id), {"incident-1", "incident-2"})
        self.assertEqual(by_id["incident-1"]["state"], "escalated")
        self.assertNotIn("owner_identity", by_id["incident-1"])
        self.assertEqual(by_id["incident-2"]["state"], "open")
        repeated = self.projection.snapshot(self.principal, "factory-a", run_id="run-a")
        self.assertEqual(repeated["state"]["runs"][0]["incidents"], incidents)

        history = self.projection.events_after(
            self.principal, "factory-a", start, run_id="run-a")
        incident_events = [row["event"]["data"] for row in history["events"]
                           if row["event"]["type"] ==
                           "com.exomachina.incident.state_changed.v1"]
        self.assertEqual(
            [(event["incident_id"], event["state"]) for event in incident_events],
            [("incident-1", "claimed"), ("incident-1", "escalated"),
             ("incident-2", "open")])

    def test_snapshot_is_versioned_and_cursor_is_opaque(self):
        snapshot = self.projection.snapshot(self.principal, "factory-a")
        self.assertEqual(snapshot["schema_version"], 1)
        self.assertRegex(snapshot["cursor"], r"^c1\.[A-Za-z0-9_-]{16}\.[A-Za-z0-9_-]{40,48}$")
        self.assertNotIsInstance(snapshot["cursor"], int)
        self.assertNotIn("factory_id", snapshot)
        self.assertEqual(snapshot["freshness"]["status"], "fresh")
        self.assertEqual(snapshot["state"]["factory"]["id"], "factory-a")
        self.assertEqual(snapshot["state"]["factory"]["graph"]["edges"],
                         [{"from": "start", "to": "finish"}])
        run = snapshot["state"]["runs"][0]
        self.assertEqual(run["task"], {"id": "task-run-a", "context_id": "context-run-a"})
        self.assertEqual(run["started_at"], "2026-10-02T12:00:00.000Z")
        self.assertEqual(run["status"]["started_at"], run["started_at"])

    def test_snapshot_without_publication_uses_empty_observed_graph(self):
        self.records["factory-a"] = [record(
            "factory-a", "factory-discovered", "com.exomachina.factory.discovered.v1",
            source_kind="task", fields={"name": "Factory One", "identity": "org/factory-one",
                                         "capability": "temporal-factory"})]

        snapshot = self.projection.snapshot(self.principal, "factory-a")

        self.assertIsNone(snapshot["state"]["active_publication"])
        self.assertEqual(snapshot["state"]["factory"]["graph"], {"nodes": [], "edges": []})
        self.assertEqual(snapshot["state"]["runs"], [])

    def test_factory_snapshot_projects_current_publication_bindings(self):
        expected = [{"name": "provider", "role": "model", "identity": "service/provider",
                     "capability": "inference", "contract_digest": "c" * 64}]
        self.records["factory-a"][1]["fields"]["service_bindings"] = expected

        full_snapshot = self.projection.snapshot(self.principal, "factory-a")
        run_snapshot = self.projection.snapshot(self.principal, "factory-a", run_id="run-a")

        self.assertEqual(full_snapshot["state"]["factory"]["agent_bindings"], expected)
        self.assertEqual(run_snapshot["state"]["factory"]["agent_bindings"], expected)
        schema_dir = ROOT / "schemas" / "dashboard" / "v1"
        event_schema = json.loads((schema_dir / "event-data.schema.json").read_text())
        snapshot_schema = json.loads((schema_dir / "snapshot.schema.json").read_text())
        registry = Registry().with_resource(
            event_schema["$id"], Resource.from_contents(event_schema))
        validator = Draft202012Validator(
            snapshot_schema, registry=registry,
            format_checker=Draft202012Validator.FORMAT_CHECKER)
        validator.validate(full_snapshot)
        validator.validate(run_snapshot)

    def test_historical_run_does_not_borrow_active_publication_bindings(self):
        records = json.loads(json.dumps(seed_records("factory-historical")))
        active_bindings = [{"name": "current-provider", "role": "model",
                            "identity": "service/current", "contract_digest": "e" * 64}]
        records[1]["fields"]["service_bindings"] = active_bindings
        records[2]["manifest_digest"] = "d" * 64
        source = ControlledSource({"factory-historical": records})
        projection = FactoryObservation(
            source, Path(self.temp.name) / "historical-bindings.sqlite")

        snapshot = projection.snapshot(
            source.allowed_principal, "factory-historical", run_id="run-a")

        self.assertEqual(snapshot["state"]["active_publication"]["service_bindings"],
                         active_bindings)
        self.assertNotIn("agent_bindings", snapshot["state"]["factory"])
        self.assertEqual(snapshot["state"]["runs"][0]["pinned"]["manifest_digest"],
                         "d" * 64)

    def test_first_run_event_captures_original_task_and_context_binding(self):
        root_fact = record(
            "factory-a", "root-start", "com.exomachina.run.state_changed.v1",
            run_id="root-run", top={"task_id": "root-task", "context_id": "root-context"},
            fields={"state": "running", "phase": "execution"})
        child_fact = record(
            "factory-a", "child-start", "com.exomachina.run.state_changed.v1",
            run_id="child-run", top={"task_id": "child-task", "context_id": "root-context"},
            fields={"state": "running", "phase": "execution"})
        self.records["factory-a"] = seed_records("factory-a")[:2] + [root_fact, child_fact]
        event_schema = json.loads(
            (ROOT / "schemas" / "dashboard" / "v1" / "event-data.schema.json").read_text())
        event_validator = Draft202012Validator(event_schema)
        for fact in (root_fact, child_fact):
            event_data = project_source_record(fact, "factory-a")[3]["data"]
            self.assertEqual(event_data["context_id"], "root-context")
            event_validator.validate(event_data)
        snapshot = self.projection.snapshot(self.principal, "factory-a")
        runs = {run["id"]: run for run in snapshot["state"]["runs"]}
        self.assertEqual(runs["root-run"]["task"],
                         {"id": "root-task", "context_id": "root-context"})
        self.assertEqual(runs["child-run"]["task"],
                         {"id": "child-task", "context_id": "root-context"})
        snapshot_schema = json.loads(
            (ROOT / "schemas" / "dashboard" / "v1" / "snapshot.schema.json").read_text())
        registry = Registry().with_resource(
            event_schema["$id"], Resource.from_contents(event_schema))
        Draft202012Validator(snapshot_schema, registry=registry,
                             format_checker=Draft202012Validator.FORMAT_CHECKER).validate(snapshot)

    def test_run_created_graph_is_pinned_across_lifecycle_refresh(self):
        self.records["factory-a"].append(record(
            "factory-a", "run-a-created-graph", "com.exomachina.run.created.v1",
            run_id="run-a", top={"task_id": "task-run-a", "context_id": "context-run-a",
                                  "manifest_digest": DIGEST, "package_digest": "b" * 64,
                                  "definition_digest": "c" * 64, "interpreter_build": "build-1"},
            fields={"state": "running", "phase": "execution", "graph_nodes": GRAPH}))
        self.append_state(suffix="after-pinned-graph", state="waiting")

        snapshot = self.projection.snapshot(self.principal, "factory-a")
        run = snapshot["state"]["runs"][0]

        self.assertEqual(run["graph"], {
            "nodes": [{"id": "start", "kind": "worker"},
                      {"id": "finish", "kind": "review"}],
            "edges": [{"from": "start", "to": "finish"}],
        })
        self.assertEqual(run["status"]["state"], "waiting")
        self.assertEqual(run["graph"]["nodes"][0]["kind"], "worker")
        schema_dir = ROOT / "schemas" / "dashboard" / "v1"
        event_schema = json.loads((schema_dir / "event-data.schema.json").read_text())
        snapshot_schema = json.loads((schema_dir / "snapshot.schema.json").read_text())
        registry = Registry().with_resource(
            event_schema["$id"], Resource.from_contents(event_schema))
        Draft202012Validator(snapshot_schema, registry=registry,
                             format_checker=Draft202012Validator.FORMAT_CHECKER).validate(snapshot)
        snapshot["state"]["runs"][0]["graph"]["nodes"][0]["type"] = "worker"
        with self.assertRaises(ValidationError):
            Draft202012Validator(snapshot_schema, registry=registry,
                                 format_checker=Draft202012Validator.FORMAT_CHECKER).validate(snapshot)

    def test_conflicting_run_created_graph_is_rejected(self):
        original = record(
            "factory-a", "run-a-created-graph", "com.exomachina.run.created.v1",
            run_id="run-a", top={"task_id": "task-run-a", "context_id": "context-run-a",
                                  "manifest_digest": DIGEST, "package_digest": "b" * 64,
                                  "definition_digest": "c" * 64, "interpreter_build": "build-1"},
            fields={"state": "running", "phase": "execution", "graph_nodes": GRAPH})
        self.records["factory-a"].append(original)
        self.projection.snapshot(self.principal, "factory-a")

        conflicting = record(
            "factory-a", "run-a-created-graph-conflict", "com.exomachina.run.created.v1",
            run_id="run-a", top={"task_id": "task-run-a", "context_id": "context-run-a",
                                  "manifest_digest": DIGEST, "package_digest": "b" * 64,
                                  "definition_digest": "c" * 64, "interpreter_build": "build-1"},
            fields={"state": "running", "phase": "execution",
                    "graph_nodes": [{"id": "start", "type": "different"}]})
        self.records["factory-a"].append(conflicting)

        with self.assertRaisesRegex(SourceContractError, "pinned run graph changed"):
            self.projection.snapshot(self.principal, "factory-a")

    def test_known_run_pins_survive_unknowns_and_reject_changed_values(self):
        initial = self.projection.snapshot(self.principal, "factory-a")
        expected_pins = initial["state"]["runs"][0]["pinned"]
        self.records["factory-a"].append(record(
            "factory-a", "run-state-with-unknown-pins", "com.exomachina.run.state_changed.v1",
            run_id="run-a", top={"manifest_digest": None, "package_digest": None},
            fields={"state": "waiting", "phase": "execution"}))
        after_unknown = self.projection.snapshot(self.principal, "factory-a")["state"]["runs"][0]
        self.assertEqual(after_unknown["pinned"], expected_pins)

        self.records["factory-a"].append(record(
            "factory-a", "run-state-with-conflicting-pin", "com.exomachina.run.state_changed.v1",
            run_id="run-a", top={"manifest_digest": "d" * 64},
            fields={"state": "waiting", "phase": "execution"}))
        with self.assertRaisesRegex(SourceContractError, "pinned run publication changed"):
            self.projection.snapshot(self.principal, "factory-a")

    def test_run_graph_rejects_duplicate_nodes_and_unknown_edge_endpoints(self):
        invalid_graphs = [
            ([{"id": "start", "type": "worker"},
              {"id": "start", "type": "review"}], "duplicate node identifiers"),
            ([{"id": "start", "type": "worker", "next": ["missing"]}],
             "unknown edge target"),
        ]
        for index, (graph_nodes, message) in enumerate(invalid_graphs):
            with self.subTest(message=message):
                factory_id = f"graph-invalid-{index}"
                invalid_source = ControlledSource({factory_id: [
                    record(factory_id, "factory-discovered",
                           "com.exomachina.factory.discovered.v1", source_kind="task",
                           fields={"name": "Factory One", "identity": "org/factory-one",
                                   "capability": "temporal-factory"}),
                    record(factory_id, "run-created", "com.exomachina.run.created.v1",
                           run_id="run-invalid", top={"task_id": "task-invalid",
                               "context_id": "context-invalid", "manifest_digest": DIGEST,
                               "package_digest": "b" * 64, "definition_digest": "c" * 64,
                               "interpreter_build": "build-1"},
                           fields={"state": "running", "graph_nodes": graph_nodes}),
                ]})
                invalid_projection = FactoryObservation(
                    invalid_source, Path(self.temp.name) / f"graph-invalid-{index}.sqlite")
                with self.assertRaisesRegex(SourceContractError, message):
                    invalid_projection.snapshot(self.principal, factory_id)

    def test_historical_run_without_graph_keeps_graph_unavailable(self):
        run = self.projection.snapshot(self.principal, "factory-a")["state"]["runs"][0]
        self.assertNotIn("graph", run)

    def test_run_lifecycle_timestamps_survive_refresh_and_keep_earliest_start(self):
        self.records["factory-a"][2]["fields"]["started_at"] = "2026-10-02T12:05:00Z"
        initial = self.projection.snapshot(self.principal, "factory-a")
        initial_run = initial["state"]["runs"][0]
        self.assertEqual(initial_run["started_at"], "2026-10-02T12:05:00.000Z")
        self.assertEqual(initial_run["status"]["started_at"], initial_run["started_at"])

        self.records["factory-a"].append(record(
            "factory-a", "run-waiting", "com.exomachina.run.state_changed.v1",
            run_id="run-a", fields={
                "state": "waiting", "phase": "awaiting-director",
                "started_at": "2026-10-02T12:00:00Z",
                "wait_deadline": "2026-10-02T12:20:00Z",
            }))
        waiting = self.projection.snapshot(self.principal, "factory-a")["state"]["runs"][0]
        self.assertEqual(waiting["started_at"], "2026-10-02T12:00:00.000Z")
        self.assertEqual(waiting["status"]["wait_deadline"], "2026-10-02T12:20:00.000Z")

        self.records["factory-a"].append(record(
            "factory-a", "run-expired", "com.exomachina.run.state_changed.v1",
            run_id="run-a", fields={
                "state": "failed", "phase": "director-expired",
                "started_at": "2026-10-02T12:03:00Z",
                "ended_at": "2026-10-02T12:31:00Z",
            }))
        self.projection.snapshot(self.principal, "factory-a")
        self.records["factory-a"].append(record(
            "factory-a", "run-closed", "com.exomachina.run.state_changed.v1",
            run_id="run-a", fields={"state": "failed", "phase": "execution-closed"}))
        final_snapshot = self.projection.snapshot(self.principal, "factory-a")
        final_run = final_snapshot["state"]["runs"][0]

        self.assertEqual(final_run["started_at"], "2026-10-02T12:00:00.000Z")
        self.assertEqual(final_run["status"], {
            "state": "failed", "phase": "execution-closed",
            "started_at": "2026-10-02T12:00:00.000Z",
            "ended_at": "2026-10-02T12:31:00.000Z",
        })

        event_schema = json.loads(
            (ROOT / "schemas" / "dashboard" / "v1" / "event-data.schema.json").read_text())
        snapshot_schema = json.loads(
            (ROOT / "schemas" / "dashboard" / "v1" / "snapshot.schema.json").read_text())
        registry = Registry().with_resource(
            event_schema["$id"], Resource.from_contents(event_schema))
        validator = Draft202012Validator(snapshot_schema, registry=registry,
                                          format_checker=Draft202012Validator.FORMAT_CHECKER)
        validator.validate(final_snapshot)
        invalid_snapshot = json.loads(json.dumps(final_snapshot))
        invalid_snapshot["state"]["runs"][0]["status"]["raw_status"] = "private"
        with self.assertRaises(ValidationError):
            validator.validate(invalid_snapshot)

    def test_wait_facts_are_allowlisted_phase_bound_and_preserve_historical_absence(self):
        schema_dir = ROOT / "schemas" / "dashboard" / "v1"
        event_schema = json.loads((schema_dir / "event-data.schema.json").read_text())
        snapshot_schema = json.loads((schema_dir / "snapshot.schema.json").read_text())
        registry = Registry().with_resource(
            event_schema["$id"], Resource.from_contents(event_schema))
        event_validator = Draft202012Validator(
            event_schema, format_checker=Draft202012Validator.FORMAT_CHECKER)
        snapshot_validator = Draft202012Validator(
            snapshot_schema, registry=registry,
            format_checker=Draft202012Validator.FORMAT_CHECKER)

        historical = self.projection.snapshot(self.principal, "factory-a")
        old_status = historical["state"]["runs"][0]["status"]
        for field in ("wait_role", "wait_actor_identity", "wait_started_at", "permitted_actions"):
            self.assertNotIn(field, old_status)

        wait_fact = record(
            "factory-a", "run-a-human-wait", "com.exomachina.run.state_changed.v1",
            run_id="run-a", top={"task_id": "task-run-a", "context_id": "context-run-a"},
            fields={
                "state": "waiting", "phase": "awaiting-human",
                "node": "human-review",
                "wait_role": "human", "wait_actor_identity": "human-user",
                "wait_started_at": "2026-10-02T12:10:00Z",
                "permitted_actions": ["abort", "escalate"],
                "raw_context": "private context", "recommendation": "private recommendation",
            })
        envelope = project_source_record(wait_fact, "factory-a")[3]
        event_validator.validate(envelope["data"])
        self.assertEqual(envelope["data"]["wait_role"], "human")
        self.assertEqual(envelope["data"]["wait_actor_identity"], "human-user")
        self.assertEqual(envelope["data"]["wait_started_at"], "2026-10-02T12:10:00.000Z")
        self.assertEqual(envelope["data"]["permitted_actions"], ["abort", "escalate"])
        self.assertNotIn("raw_context", envelope["data"])
        self.assertNotIn("recommendation", envelope["data"])

        director_fact = record(
            "factory-a", "run-a-director-wait", "com.exomachina.run.state_changed.v1",
            run_id="run-a", fields={
                "state": "waiting", "phase": "awaiting-director", "wait_role": "director",
                "wait_actor_identity": "director-agent", "permitted_actions": ["abort"],
            })
        self.assertEqual(
            project_source_record(director_fact, "factory-a")[3]["data"]["wait_role"],
            "director")

        self.records["factory-a"].append(wait_fact)
        snapshot = self.projection.snapshot(self.principal, "factory-a")
        status = snapshot["state"]["runs"][0]["status"]
        self.assertEqual(status["node"], "human-review")
        self.assertEqual(status["wait_role"], "human")
        self.assertEqual(status["wait_actor_identity"], "human-user")
        self.assertEqual(status["wait_started_at"], "2026-10-02T12:10:00.000Z")
        self.assertEqual(status["permitted_actions"], ["abort", "escalate"])
        snapshot_validator.validate(snapshot)

        # Escalation can restart a human wait. The projection takes the new
        # writer timestamp verbatim after UTC normalization; it never derives
        # it from the run start or the observation capture time.
        restarted_wait = record(
            "factory-a", "run-a-human-wait-restarted", "com.exomachina.run.state_changed.v1",
            run_id="run-a", top={"task_id": "task-run-a", "context_id": "context-run-a"},
            fields={
                "state": "waiting", "phase": "awaiting-human", "wait_role": "human",
                "wait_actor_identity": "human-user",
                "wait_started_at": "2026-10-02T12:25:00Z", "permitted_actions": ["abort"],
            })
        self.records["factory-a"].append(restarted_wait)
        restarted = self.projection.snapshot(self.principal, "factory-a")
        self.assertEqual(
            restarted["state"]["runs"][0]["status"]["wait_started_at"],
            "2026-10-02T12:25:00.000Z")
        snapshot_validator.validate(restarted)

        self.records["factory-a"].append(record(
            "factory-a", "run-a-resumed-execution", "com.exomachina.run.state_changed.v1",
            run_id="run-a", fields={"state": "running", "phase": "execution"}))
        resumed = self.projection.snapshot(self.principal, "factory-a")
        resumed_status = resumed["state"]["runs"][0]["status"]
        for field in ("node", "wait_deadline", "wait_role", "wait_actor_identity",
                      "wait_started_at", "permitted_actions"):
            self.assertNotIn(field, resumed_status)
        snapshot_validator.validate(resumed)

        invalid_facts = [
            ({"wait_role": "operator"}, "invalid wait role"),
            ({"wait_role": None}, "invalid wait role"),
            ({"wait_role": "director"}, "wait role does not match"),
            ({"wait_actor_identity": "human user"}, "invalid observation field"),
            ({"wait_actor_identity": {"id": "human-user"}}, "invalid observation field"),
            ({"wait_started_at": "not-a-date"}, "ISO timestamp"),
            ({"permitted_actions": ["abort now"]}, "invalid observation field"),
            ({"permitted_actions": [None]}, "invalid observation field"),
            ({"permitted_actions": ["abort", "abort"]}, "must be distinct"),
            ({"permitted_actions": [f"action-{index}" for index in range(17)]},
             "invalid observation field"),
        ]
        for index, (replacement, message) in enumerate(invalid_facts):
            with self.subTest(replacement=replacement):
                invalid = json.loads(json.dumps(wait_fact))
                invalid["source_id"] = f"invalid-human-wait-{index}"
                invalid["fields"].update(replacement)
                with self.assertRaisesRegex(SourceContractError, message):
                    project_source_record(invalid, "factory-a")

        non_waiting = json.loads(json.dumps(wait_fact))
        non_waiting["source_id"] = "wait-facts-on-execution-phase"
        non_waiting["fields"]["phase"] = "execution"
        with self.assertRaisesRegex(SourceContractError, "actual waiting phase"):
            project_source_record(non_waiting, "factory-a")

        invalid_data = dict(envelope["data"], wait_role="director")
        with self.assertRaises(ValidationError):
            event_validator.validate(invalid_data)
        invalid_data = dict(envelope["data"], permitted_actions=["abort"] * 17)
        with self.assertRaises(ValidationError):
            event_validator.validate(invalid_data)
        invalid_data = dict(envelope["data"], permitted_actions=["abort", "abort"])
        with self.assertRaises(ValidationError):
            event_validator.validate(invalid_data)
        invalid_data = dict(envelope["data"], wait_started_at="not-a-date")
        with self.assertRaises(ValidationError):
            event_validator.validate(invalid_data)
        invalid_status = dict(status, wait_started_at="not-a-date")
        invalid_snapshot = dict(snapshot, state=dict(
            snapshot["state"], runs=[dict(snapshot["state"]["runs"][0], status=invalid_status)]))
        with self.assertRaises(ValidationError):
            snapshot_validator.validate(invalid_snapshot)

    def test_artifact_revision_is_projected_as_a_safe_identifier(self):
        artifact_record = record(
            "factory-a", "artifact-1", "com.exomachina.artifact.revised.v1",
            run_id="run-a", source_kind="artifact",
            fields={"artifact_revision": "revision-1", "artifact_sha256": DIGEST})
        envelope = project_source_record(artifact_record, "factory-a")[3]
        self.assertEqual(envelope["data"]["artifact_revision"], "revision-1")
        self.assertEqual(envelope["data"]["artifact_sha256"], DIGEST)
        event_schema = json.loads(
            (ROOT / "schemas" / "dashboard" / "v1" / "event-data.schema.json").read_text())
        Draft202012Validator(event_schema).validate(envelope["data"])

    def test_commercial_usage_keeps_safe_attribution_and_omits_unreported_quantity(self):
        event_schema = json.loads(
            (ROOT / "schemas" / "dashboard" / "v1" / "event-data.schema.json").read_text())
        validator = Draft202012Validator(event_schema)
        shared = {"service_identity": "service.synthetic", "unit": "input_units",
                  "measurement_source": "provider_meter", "model_id": "model-test",
                  "reasoning_effort": "medium", "evidence_refs": [DIGEST]}
        reported = record(
            "factory-a", "usage-reported", "com.exomachina.commercial.usage.v1",
            run_id="run-a", source_kind="commercial",
            top={"assignment_id": "assignment-a", "attempt_id": "attempt-a"},
            fields={**shared, "usage_id": "usage-reported", "model_call_id": "call-123",
                    "quantity": "12.5", "completeness": "complete",
                    "evidence_status": "calculated_from_measured_usage"})
        unknown = record(
            "factory-a", "usage-unknown", "com.exomachina.commercial.usage.v1",
            run_id="run-a", source_kind="commercial",
            top={"assignment_id": "assignment-a", "attempt_id": "attempt-a"},
            fields={**shared, "usage_id": "usage-unknown", "quantity": None,
                    "model_call_id": None, "completeness": "unknown",
                    "evidence_status": "measured"})
        undisclosed = record(
            "factory-a", "usage-undisclosed", "com.exomachina.commercial.usage.v1",
            run_id="run-a", source_kind="commercial",
            top={"assignment_id": "assignment-a", "attempt_id": "attempt-a"},
            fields={**shared, "usage_id": "usage-undisclosed", "completeness": "undisclosed",
                    "evidence_status": "provider_reported"})
        self.records["factory-a"].extend([reported, unknown, undisclosed])
        projected = {
            row["source_id"]: project_source_record(row, "factory-a")[3]["data"]
            for row in (reported, unknown, undisclosed)
        }
        reported_data = projected["usage-reported"]
        self.assertEqual(reported_data["service_identity"], "service.synthetic")
        self.assertEqual(reported_data["model_call_id"], "call-123")
        self.assertEqual(reported_data["quantity"], "12.5")
        self.assertEqual(reported_data["evidence_status"], "calculated_from_measured_usage")
        self.assertEqual(reported_data["evidence_refs"], [DIGEST])
        for name in ("usage-unknown", "usage-undisclosed"):
            self.assertNotIn("quantity", projected[name])
            validator.validate(projected[name])
        self.assertNotIn("model_call_id", projected["usage-unknown"])
        self.assertEqual(projected["usage-unknown"]["evidence_status"], "measured")
        validator.validate(reported_data)

        # Evidence quality is independent from the quantity completeness pair.
        # Every shared evidence value is valid with a reported quantity, even
        # when the evidence itself is unknown or undisclosed.
        evidence_statuses = (
            "measured", "calculated_from_measured_usage", "provider_reported",
            "estimated", "unknown", "undisclosed",
        )
        for index, evidence_status in enumerate(evidence_statuses):
            candidate = record(
                "factory-a", f"usage-evidence-{index}", "com.exomachina.commercial.usage.v1",
                run_id="run-a", source_kind="commercial",
                top={"assignment_id": "assignment-a", "attempt_id": "attempt-a"},
                fields={**shared, "usage_id": f"usage-evidence-{index}",
                        "quantity": "0.125", "completeness": "complete",
                        "evidence_status": evidence_status})
            data = project_source_record(candidate, "factory-a")[3]["data"]
            validator.validate(data)
            self.assertEqual(data["quantity"], "0.125")
            self.assertEqual(data["completeness"], "complete")
            self.assertEqual(data["evidence_status"], evidence_status)

        bad_unknown_quantity = {**unknown, "fields": {**unknown["fields"], "quantity": "1"}}
        bad_missing_quantity = {**reported, "fields": {
            key: value for key, value in reported["fields"].items() if key != "quantity"}}
        bad_completeness = {**reported, "fields": {
            **reported["fields"], "completeness": "partial"}}
        missing_evidence_status = {**reported, "fields": {
            key: value for key, value in reported["fields"].items() if key != "evidence_status"}}
        old_evidence_status = {**reported, "fields": {
            **reported["fields"], "evidence_status": "calculated"}}
        with self.assertRaises(SourceContractError):
            project_source_record(bad_unknown_quantity, "factory-a")
        with self.assertRaises(SourceContractError):
            project_source_record(bad_missing_quantity, "factory-a")
        with self.assertRaises(SourceContractError):
            project_source_record(bad_completeness, "factory-a")
        with self.assertRaises(SourceContractError):
            project_source_record(missing_evidence_status, "factory-a")
        with self.assertRaises(SourceContractError):
            project_source_record(old_evidence_status, "factory-a")
        self.assertFalse(validator.is_valid({**projected["usage-unknown"], "quantity": "1"}))
        self.assertFalse(validator.is_valid({**projected["usage-reported"], "completeness": "unknown"}))
        self.assertFalse(validator.is_valid({**projected["usage-reported"], "completeness": "partial"}))
        self.assertFalse(validator.is_valid({**projected["usage-reported"], "evidence_status": "calculated"}))
        self.assertFalse(validator.is_valid({
            key: value for key, value in projected["usage-reported"].items()
            if key != "evidence_status"}))
        bad_decimal = {**reported, "fields": {**reported["fields"], "quantity": "1e3"}}
        with self.assertRaises(SourceContractError):
            project_source_record(bad_decimal, "factory-a")

        for unsafe_ref in ("/var/private/evidence.json", "https://example.invalid/evidence",
                           "file:///private/secret.pem"):
            unsafe = {**reported, "fields": {**reported["fields"], "evidence_refs": [unsafe_ref]}}
            with self.assertRaises(SourceContractError):
                project_source_record(unsafe, "factory-a")

    def test_commercial_obligation_rows_are_componentized_and_money_is_conditional(self):
        components = ("inference_cost", "hosting_cost", "markup", "supplier_charge",
                      "payment_fees", "owner_overhead", "production_cost", "customer_price")
        base_fields = {"offer_digest": DIGEST, "currency": "TST", "atomic_scale": 2,
                       "price_basis": "usage", "payment_trigger": "incremental_use",
                       "state": "settlement_pending",
                       "evidence_refs": [DIGEST]}
        rows = []
        for component in components:
            amount = None if component == "hosting_cost" else 1
            evidence = "unknown" if amount is None else "calculated_from_measured_usage"
            fields = {**base_fields, "obligation_id": f"obligation-{component}",
                      "component": component, "amount_atoms": amount,
                      "evidence_status": evidence}
            if component == "markup":
                fields["markup_bps"] = 0
            rows.append(record(
                "factory-a", f"component-{component}", "com.exomachina.commercial.obligation.v1",
                run_id="run-a", source_kind="commercial",
                top={"assignment_id": "assignment-a", "attempt_id": "attempt-a"}, fields=fields))
        self.records["factory-a"].extend(rows)
        event_schema = json.loads(
            (ROOT / "schemas" / "dashboard" / "v1" / "event-data.schema.json").read_text())
        validator = Draft202012Validator(event_schema)
        data_rows = [project_source_record(row, "factory-a")[3]["data"] for row in rows]
        for data in data_rows:
            validator.validate(data)
        self.assertEqual({data["component"] for data in data_rows}, set(components))
        self.assertEqual({data["obligation_id"] for data in data_rows},
                         {f"obligation-{component}" for component in components})
        self.assertEqual({data["state"] for data in data_rows}, {"settlement_pending"})
        hosting = next(data for data in data_rows if data["component"] == "hosting_cost")
        self.assertIsNone(hosting["amount_atoms"])
        self.assertEqual(hosting["evidence_status"], "unknown")
        self.assertEqual(next(data for data in data_rows if data["component"] == "markup")["markup_bps"], 0)

        invalid_null = {**rows[0], "fields": {**rows[0]["fields"],
                                                "amount_atoms": None, "evidence_status": "measured"}}
        invalid_numeric = {**rows[0], "fields": {**rows[0]["fields"],
                                                   "amount_atoms": 1, "evidence_status": "undisclosed"}}
        for invalid in (invalid_null, invalid_numeric):
            with self.assertRaises(SourceContractError):
                project_source_record(invalid, "factory-a")

        snapshot = self.projection.snapshot(self.principal, "factory-a")
        self.assertEqual(len(snapshot["state"]["commercial"]["obligations"]), len(components))
        snapshot_schema = json.loads(
            (ROOT / "schemas" / "dashboard" / "v1" / "snapshot.schema.json").read_text())
        registry = Registry().with_resource(event_schema["$id"], Resource.from_contents(event_schema))
        Draft202012Validator(snapshot_schema, registry=registry,
                             format_checker=Draft202012Validator.FORMAT_CHECKER).validate(snapshot)

    def test_resume_replays_durable_event_with_stable_id_and_opaque_cursor(self):
        start = self.projection.snapshot(self.principal, "factory-a")["cursor"]
        self.append_state()
        page = self.projection.events_after(self.principal, "factory-a", start)
        repeat = self.projection.events_after(self.principal, "factory-a", start)
        self.assertEqual(len(page["events"]), 1)
        self.assertEqual(page["events"][0]["event"]["id"], repeat["events"][0]["event"]["id"])
        self.assertRegex(page["events"][0]["cursor"], r"^c1\.[A-Za-z0-9_-]{16}\.[A-Za-z0-9_-]{40,48}$")
        self.assertRegex(page["continuation_cursor"], r"^c1\.[A-Za-z0-9_-]{16}\.[A-Za-z0-9_-]{40,48}$")
        self.assertEqual(page["events"][0]["event"]["dataschema"],
                         "urn:exomachina:dashboard:event:v1")

    def test_reopening_populated_projection_database_preserves_rows_and_cursor(self):
        database = Path(self.temp.name) / "populated-existing.sqlite"
        original = FactoryObservation(self.source, database)
        before_snapshot = original.snapshot(self.principal, "factory-a")
        with sqlite3.connect(database) as db:
            before = (db.execute("SELECT COUNT(*) FROM observation_events").fetchone()[0],
                      db.execute("SELECT COUNT(*) FROM observation_source_records").fetchone()[0],
                      db.execute("SELECT MIN(cursor), MAX(cursor) FROM observation_events").fetchone())
        self.assertGreater(before[0], 0)

        reopened = FactoryObservation(self.source, database)
        after_snapshot = reopened.snapshot(self.principal, "factory-a")
        with sqlite3.connect(database) as db:
            after = (db.execute("SELECT COUNT(*) FROM observation_events").fetchone()[0],
                     db.execute("SELECT COUNT(*) FROM observation_source_records").fetchone()[0],
                     db.execute("SELECT MIN(cursor), MAX(cursor) FROM observation_events").fetchone())

        self.assertEqual(after, before)
        self.assertEqual(after_snapshot["cursor"], before_snapshot["cursor"])
        self.assertEqual(after_snapshot["state"], before_snapshot["state"])

    def test_run_filter_advances_over_unrelated_events_with_checkpoint_cursor(self):
        self.records["factory-a"].extend(seed_records("factory-a", "run-b")[2:])
        start = self.projection.snapshot(self.principal, "factory-a", run_id="run-a")["cursor"]
        self.append_state(run_id="run-b", suffix="unrelated")
        self.append_state(run_id="run-a", suffix="wanted")
        self.append_state(run_id="run-b", suffix="unrelated-later")
        skipped = self.projection.events_after(self.principal, "factory-a", start,
                                               run_id="run-a", limit=1)
        self.assertEqual(skipped["events"], [])
        self.assertTrue(skipped["checkpoint_advanced"])
        wanted = self.projection.events_after(self.principal, "factory-a",
                                              skipped["continuation_cursor"],
                                              run_id="run-a", limit=2)
        self.assertEqual([row["event"]["data"]["run_id"] for row in wanted["events"]], ["run-a"])
        self.assertTrue(wanted["checkpoint_advanced"])
        self.assertNotEqual(wanted["continuation_cursor"], wanted["events"][0]["cursor"])

    def test_retention_gap_requires_resync_and_cross_factory_cursor_is_rejected(self):
        self.records["factory-b"] = seed_records("factory-b")
        source = ControlledSource(self.records)
        projection = FactoryObservation(source, Path(self.temp.name) / "retention.sqlite",
                                        retention_events=2)
        old = projection.snapshot(self.principal, "factory-a")["cursor"]
        projection.snapshot(self.principal, "factory-b")
        for index in range(3):
            self.records["factory-a"].append(record(
                "factory-a", f"gap-{index}", "com.exomachina.run.state_changed.v1",
                run_id="run-a", fields={"state": "waiting", "phase": "execution"}))
        with self.assertRaises(CursorExpired) as expired:
            projection.events_after(self.principal, "factory-a", old)
        self.assertRegex(expired.exception.minimum_cursor, r"^c1\.[A-Za-z0-9_-]{16}\.[A-Za-z0-9_-]{40,48}$")
        self.assertRegex(expired.exception.latest_cursor, r"^c1\.[A-Za-z0-9_-]{16}\.[A-Za-z0-9_-]{40,48}$")
        current = projection.snapshot(self.principal, "factory-a")["cursor"]
        with projection._connect() as db:
            db.execute("UPDATE observation_streams SET source_cursor=NULL WHERE factory_id=?",
                       ("factory-a",))
            db.commit()
        projection.refresh(self.principal, "factory-a")
        self.assertEqual(projection.snapshot(self.principal, "factory-a")["cursor"], current)
        with self.assertRaises(InvalidCursor):
            projection.events_after(self.principal, "factory-b", old)

    def test_artifact_digest_and_command_receipt_delegate_with_principal(self):
        digest = hashlib.sha256(self.source.artifact).hexdigest()
        artifact = self.projection.inspect_artifact(self.principal, "factory-a", "run-a",
                                                    "rev-1", digest)
        self.assertEqual(artifact["content"], self.source.artifact)
        receipt = self.projection.submit_command(self.principal, "factory-a", {
            "command_id": "command-1", "task_id": "task-run-a", "context_id": "context-run-a",
            "action": "retry", "expected_state": "waiting"})
        self.assertEqual(receipt, {"command_id": "command-1", "lifecycle": "received"})
        self.assertIs(self.source.command_calls[0][0], self.principal)

    def test_local_file_delivery_receipt_is_conditional_and_safe(self):
        schema_dir = ROOT / "schemas" / "dashboard" / "v1"
        event_schema = json.loads((schema_dir / "event-data.schema.json").read_text())
        snapshot_schema = json.loads((schema_dir / "snapshot.schema.json").read_text())
        registry = Registry().with_resource(
            event_schema["$id"], Resource.from_contents(event_schema))
        event_validator = Draft202012Validator(
            event_schema, format_checker=Draft202012Validator.FORMAT_CHECKER)
        snapshot_validator = Draft202012Validator(
            snapshot_schema, registry=registry,
            format_checker=Draft202012Validator.FORMAT_CHECKER)

        bundle = json.loads((ROOT / "dashboard" / "recordings" /
                             "codex-subscription-3.bundle.json").read_text())
        legacy_event = next(frame["event"] for frame in bundle["frames"]
                            if frame.get("op") == "event"
                            and frame["event"].get("type") ==
                            "com.exomachina.delivery.receipt.v1")
        self.assertNotIn("delivery_kind", legacy_event["data"])
        event_validator.validate(legacy_event["data"])

        fact = record(
            "factory-a", "local-delivery-receipt", "com.exomachina.delivery.receipt.v1",
            source_kind="outcome", run_id="run-a",
            top={"task_id": "task-run-a", "context_id": "context-run-a"},
            fields={
                "receipt_id": "receipt-local-1",
                "artifact_revision": "revision-1",
                "artifact_sha256": "d" * 64,
                "markdown_sha256": "e" * 64,
                "destination_identity": "local/customer-report",
                "delivery_kind": "local_file",
                "byte_length": 37,
                "delivered_at": TIME,
                "outcome": "local-file-delivered",
                "path": "/private/report.md",
                "markdown": "private report body",
                "cost": "unreported",
                "remote_claim": "sent to customer",
            })
        envelope = project_source_record(fact, "factory-a")[3]
        data = envelope["data"]
        event_validator.validate(data)
        self.assertEqual(data["delivery_kind"], "local_file")
        self.assertEqual(data["outcome"], "local-file-delivered")
        self.assertEqual(data["delivered_at"], "2026-10-02T12:00:00.000Z")
        self.assertEqual(data["destination_identity"], "local/customer-report")
        self.assertEqual(data["markdown_sha256"], "e" * 64)
        self.assertEqual(data["byte_length"], 37)
        for forbidden in ("path", "markdown", "cost", "remote_claim"):
            self.assertNotIn(forbidden, data)

        self.records["factory-a"].append(fact)
        snapshot = self.projection.snapshot(self.principal, "factory-a")
        run = snapshot["state"]["runs"][0]
        self.assertEqual(run["delivery"][-1], data)
        snapshot_validator.validate(snapshot)

        conditional_fields = (
            "run_id", "task_id", "context_id", "receipt_id", "artifact_revision",
            "artifact_sha256", "markdown_sha256", "destination_identity", "byte_length",
            "delivered_at", "outcome",
        )
        for field in conditional_fields:
            with self.subTest(missing=field):
                invalid_data = json.loads(json.dumps(data))
                del invalid_data[field]
                with self.assertRaises(ValidationError):
                    event_validator.validate(invalid_data)

                invalid_fact = json.loads(json.dumps(fact))
                if field in {"run_id", "task_id", "context_id"}:
                    del invalid_fact[field]
                else:
                    del invalid_fact["fields"][field]
                with self.assertRaises(SourceContractError):
                    project_source_record(invalid_fact, "factory-a")

        for field in ("run_id", "task_id", "context_id", "receipt_id",
                      "artifact_revision", "artifact_sha256", "markdown_sha256",
                      "destination_identity", "delivered_at"):
            with self.subTest(null=field):
                invalid_data = json.loads(json.dumps(data))
                invalid_data[field] = None
                with self.assertRaises(ValidationError):
                    event_validator.validate(invalid_data)

        invalid_kind = json.loads(json.dumps(fact))
        invalid_kind["fields"]["delivery_kind"] = "remote"
        with self.assertRaises(SourceContractError):
            project_source_record(invalid_kind, "factory-a")
        invalid_outcome = json.loads(json.dumps(fact))
        invalid_outcome["fields"]["outcome"] = "accepted"
        with self.assertRaises(SourceContractError):
            project_source_record(invalid_outcome, "factory-a")
        invalid_length = json.loads(json.dumps(fact))
        invalid_length["fields"]["byte_length"] = -1
        with self.assertRaises(SourceContractError):
            project_source_record(invalid_length, "factory-a")

    def test_submit_command_preserves_safe_prior_terminal_outcome(self):
        self.source.prior_outcomes["command-1"] = {
            "lifecycle": "applied", "outcome": "abort-recorded",
            "resulting_state": "abort-recorded", "private_token": "discard-me"}
        command = {"command_id": "command-1", "task_id": "task-run-a",
                   "context_id": "context-run-a", "action": "abort",
                   "expected_state": "waiting"}
        receipt = self.projection.submit_command(self.principal, "factory-a", command)
        self.assertEqual(receipt, {"command_id": "command-1", "lifecycle": "received",
                                   "prior_outcome": {"lifecycle": "applied",
                                                     "outcome": "abort-recorded",
                                                     "resulting_state": "abort-recorded"}})

    def test_schemas_are_valid_json_schema_documents(self):
        schema_dir = ROOT / "schemas" / "dashboard" / "v1"
        for path in sorted(schema_dir.glob("*.schema.json")):
            schema = json.loads(path.read_text())
            Draft202012Validator.check_schema(schema)

    def test_python_snapshot_matches_public_snapshot_schema(self):
        schema_dir = ROOT / "schemas" / "dashboard" / "v1"
        event_schema = json.loads((schema_dir / "event-data.schema.json").read_text())
        snapshot_schema = json.loads((schema_dir / "snapshot.schema.json").read_text())
        registry = Registry().with_resource(
            event_schema["$id"], Resource.from_contents(event_schema))
        Draft202012Validator(snapshot_schema, registry=registry,
                             format_checker=Draft202012Validator.FORMAT_CHECKER).validate(
            self.projection.snapshot(self.principal, "factory-a"))

    def test_snapshot_schema_supports_scoped_freshness_and_bounded_history_warnings(self):
        schema_dir = ROOT / "schemas" / "dashboard" / "v1"
        event_schema = json.loads((schema_dir / "event-data.schema.json").read_text())
        snapshot_schema = json.loads((schema_dir / "snapshot.schema.json").read_text())
        registry = Registry().with_resource(
            event_schema["$id"], Resource.from_contents(event_schema))
        validator = Draft202012Validator(
            snapshot_schema, registry=registry,
            format_checker=Draft202012Validator.FORMAT_CHECKER)

        snapshot = self.projection.snapshot(self.principal, "factory-a")
        snapshot["freshness"].update({
            "scope": "run",
            "run_id": "run-a",
            "included_run_ids": ["run-a", "run-a:child:1"],
            "factory_status": "disconnected",
            "unavailable_run_ids": ["old-root", "old-child"],
        })
        validator.validate(snapshot)

        invalid_freshness = [
            {"scope": "cluster"},
            {"run_id": "bad run"},
            {"factory_status": "current"},
            {"included_run_ids": ["run-a", "run-a"]},
            {"unavailable_run_ids": ["old-run"] * 257},
            {"included_run_ids": [f"run-{index}" for index in range(257)]},
        ]
        for invalid in invalid_freshness:
            with self.subTest(invalid=invalid):
                candidate = json.loads(json.dumps(snapshot))
                candidate["freshness"].update(invalid)
                with self.assertRaises(ValidationError):
                    validator.validate(candidate)

        invalid_scopes = [
            {"status": "fresh", "observed_at": snapshot["freshness"]["observed_at"],
             "scope": "run"},
            {"status": "fresh", "observed_at": snapshot["freshness"]["observed_at"],
             "scope": "factory", "run_id": "run-a"},
            {"status": "fresh", "observed_at": snapshot["freshness"]["observed_at"],
             "scope": "run", "run_id": "run-a", "included_run_ids": [],
             "factory_status": "disconnected"},
        ]
        for freshness in invalid_scopes:
            with self.subTest(freshness=freshness):
                candidate = json.loads(json.dumps(snapshot))
                candidate["freshness"] = freshness
                with self.assertRaises(ValidationError):
                    validator.validate(candidate)

    def test_existing_recorded_bundle_snapshot_matches_public_snapshot_schema(self):
        schema_dir = ROOT / "schemas" / "dashboard" / "v1"
        event_schema = json.loads((schema_dir / "event-data.schema.json").read_text())
        snapshot_schema = json.loads((schema_dir / "snapshot.schema.json").read_text())
        registry = Registry().with_resource(
            event_schema["$id"], Resource.from_contents(event_schema))
        bundle_path = ROOT / "dashboard" / "recordings" / "codex-subscription-3.bundle.json"
        recorded_bundle = json.loads(bundle_path.read_text())
        validator = Draft202012Validator(snapshot_schema, registry=registry,
                                          format_checker=Draft202012Validator.FORMAT_CHECKER)
        validator.validate(recorded_bundle["snapshot"])
        normalized_contract_sample = json.loads(json.dumps(recorded_bundle["snapshot"]))
        normalized_contract_sample["state"]["factory"]["graph"] = {
            "nodes": [
                {"id": "worker", "type": "worker", "name": "Worker", "next": ["review"]},
                {"id": "review", "kind": "review", "short": "Check"},
            ],
            "edges": [{"from": "worker", "to": "review", "label": "Next", "kind": "flow"}],
        }
        normalized_contract_sample["state"]["factory"]["agent_bindings"] = [{
            "name": "provider", "role": "model", "identity": "service/provider",
            "capability": "inference", "contract_digest": DIGEST,
        }]
        validator.validate(normalized_contract_sample)
        for invalid_role in (7, None, {}, "r" * 129):
            with self.subTest(invalid_role_type=type(invalid_role).__name__):
                invalid_sample = json.loads(json.dumps(normalized_contract_sample))
                invalid_sample["state"]["factory"]["agent_bindings"][0]["role"] = invalid_role
                with self.assertRaises(ValidationError):
                    validator.validate(invalid_sample)
        undeclared_sample = json.loads(json.dumps(normalized_contract_sample))
        undeclared_sample["state"]["factory"]["agent_bindings"][0]["prompt"] = "private"
        with self.assertRaises(ValidationError):
            validator.validate(undeclared_sample)

    def test_authorization_failure_does_not_read_source(self):
        with self.assertRaises(ObservationForbidden):
            self.projection.snapshot("untrusted", "factory-a")

    def test_source_cursor_must_be_opaque_text(self):
        class NumericSource(ControlledSource):
            def read_page(self, principal, factory_id, after_cursor, *, limit):
                page = super().read_page(principal, factory_id, after_cursor, limit=limit)
                return SourcePage(page.records, 7, page.has_more, page.freshness)

        numeric_source = NumericSource(self.records)
        projection = FactoryObservation(numeric_source, Path(self.temp.name) / "numeric.sqlite")
        with self.assertRaises(SourceContractError):
            projection.refresh(self.principal, "factory-a")


if __name__ == "__main__":
    unittest.main()
