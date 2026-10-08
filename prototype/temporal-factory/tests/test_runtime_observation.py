"""Synthetic Runtime tracer coverage; never starts Temporal or a model provider."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

import harness  # noqa: E402
from artifact_delivery import accepted_markdown  # noqa: E402
from factory import nested_workflow_input  # noqa: E402
from observation import FactoryObservation  # noqa: E402
from observation_source import RuntimeObservationSource, _canonical  # noqa: E402
from commercial import CommercialLedger  # noqa: E402
from admission import AdmissionQueue  # noqa: E402


class FakePublications:
    def __init__(self):
        self.value = None

    def active(self):
        return self.value

    def get(self, manifest_digest):
        if self.value and self.value.get("manifest_digest") == manifest_digest:
            return self.value
        raise KeyError(manifest_digest)


class FakeRunner:
    def is_running(self):
        return False


class FakeDirector:
    identity = "factory-test"

    def __init__(self, instance: Path, *, authorized_actor="fixture-operator"):
        self.database = instance / "director.sqlite3"
        self.database.parent.mkdir(parents=True, exist_ok=True)
        (instance / "instance.json").write_text("{}\n")
        catalog = instance / "catalog"
        catalog.mkdir()
        (catalog / "active-publication.json").write_text("{}\n")
        self.module = type("Module", (), {})()
        self.module.runner = FakeRunner()
        self.module.publications = FakePublications()
        self.module.catalog = catalog
        self.module.package_record = {}
        self.module.package = lambda _digest: self.module.package_record
        self.authorized_actor = authorized_actor
        self.perform_calls = []
        self.wait_view = {"phase": "awaiting-director", "current_revision": "r1",
                          "current_sha256": "d" * 64, "decision_actor": self.identity,
                          "permitted_actions": ["abort", "escalate"],
                          "applied_decisions": {}}
        with self.connect() as db:
            db.execute("CREATE TABLE runs (run_id TEXT, task_id TEXT, context_id TEXT, "
                       "package_digest TEXT, manifest_digest TEXT, build_id TEXT, label TEXT, "
                       "run_inputs_json TEXT, run_inputs_digest TEXT, authorized_actor TEXT, "
                       "input_authority_json TEXT, closed INTEGER, outcome_json TEXT)")
            db.execute("INSERT INTO runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                       ("run-1", "task-1", "context-1", "b" * 64, "a" * 64,
                        "b-123456789abc", "v1", "{}", "c" * 64, authorized_actor,
                        "{}", 0, None))

    def connect(self):
        db = sqlite3.connect(self.database, timeout=5, isolation_level=None)
        db.row_factory = sqlite3.Row
        return db

    def task_binding(self, task_id):
        return ("run-1", "context-1") if task_id == "task-1" else None

    def run_record(self, run_id):
        return {"run_id": run_id, "authorized_actor": self.authorized_actor}

    def inspect_bound_run(self, task_id):
        return dict(self.wait_view)

    def perform(self, command, task_id, context_id):
        self.perform_calls.append((command, task_id, context_id))
        action = command["op"]
        return {"lifecycle": "applied", "outcome": f"{action}-recorded"}


class FakePublicCommercialLedger:
    """Public reader fake; intentionally has no database or write methods."""

    def __init__(self, purchases=()):
        self.purchases = list(purchases)
        self.calls = []

    def list_purchases(self, *, run_id=None):
        self.calls.append(run_id)
        return [row for row in self.purchases if run_id is None or row["run_id"] == run_id]


class FakePublicLocalDelivery:
    """Public receipt reader fake with no database or filesystem access."""

    def __init__(self, receipts=()):
        self.receipts = list(receipts)
        self.calls = []

    def list_receipts(self, *, run_id=None):
        self.calls.append(run_id)
        return [row for row in self.receipts if run_id is None or row["run_id"] == run_id]


class RuntimeObservationTests(unittest.TestCase):
    def setUp(self):
        self.temp = Path(tempfile.mkdtemp(prefix="exo-runtime-observation-", dir="/tmp"))
        self.instance = self.temp / "instance"
        self.director = FakeDirector(self.instance)
        self.run_id = "run-1"

    def _assignment_retry_history(self, *, started_event_id=4,
                                  started_schedule_id=2):
        closure = {"manifest_digest": "a" * 64,
                   "manifest": {"root_digest": "c" * 64,
                                "interpreter": {"build_id": "b-123456789abc"},
                                "services": {}}}
        return [
            {"event_id": 1, "event_type": "WORKFLOW_EXECUTION_STARTED",
             "time": "2026-10-03T15:54:00.000Z", "attributes": {"input": {
                 "run": self.run_id, "definition_digest": "c" * 64,
                 "package_digest": "b" * 64, "closure": closure,
                 "document": {"nodes": {}}}}},
            {"event_id": 2, "event_type": "ACTIVITY_TASK_SCHEDULED",
             "time": "2026-10-03T15:54:01.000Z", "attributes": {
                 "activity_id": "assign-temporal-id", "activity_type": "assign",
                 "input": {"instance": "assignment-retained",
                           "capability": "research",
                           "binding": {"identity": "worker-safe"}}}},
            {"event_id": 3, "event_type": "ACTIVITY_TASK_STARTED",
             "time": "2026-10-03T15:54:10.000Z", "attributes": {
                 "scheduled_event_id": 2, "attempt": 1}},
            {"event_id": 4, "event_type": "ACTIVITY_TASK_STARTED",
             "time": "2026-10-03T15:54:11.000Z", "attributes": {
                 "scheduled_event_id": started_schedule_id, "attempt": 2}},
            {"event_id": 5, "event_type": "ACTIVITY_TASK_COMPLETED",
             "time": "2026-10-03T15:54:23.206Z", "attributes": {
                 "scheduled_event_id": 2, "started_event_id": started_event_id,
                 "result": None}},
        ]

    def test_retry_completion_uses_started_event_and_corrects_only_durable_wrong_row(self):
        history = self._assignment_retry_history()
        database = self.instance / "runtime-source.sqlite3"
        source = RuntimeObservationSource(self.director,
            {"name": "factory-test", "capability": {"id": "verified-research@1"}},
            database, history_reader=lambda: [(self.run_id, history)],
            refresh_interval_seconds=0)
        for record in source._history_records(self.run_id, history, "task-1", "context-1"):
            if record.get("event_type") == "com.exomachina.assignment.state_changed.v1" and \
                    record.get("fields", {}).get("state") == "completed":
                continue
            source._insert(record)
        completion_time = "2026-10-03T15:54:23.206Z"
        legacy_id = f"{self.run_id}:5:assignment"
        prior = source._record("temporal", legacy_id,
            "com.exomachina.assignment.state_changed.v1", completion_time,
            run_id=self.run_id,
            fields={"state": "completed", "ended_at": completion_time,
                    "node": "research-node", "capability": "research",
                    "provider_identity": "worker-safe"},
            task_id="task-1", context_id="context-1",
            assignment_id="assignment-retained", attempt_id="1")
        source._insert(prior)
        prior_json = _canonical(prior)
        with source._connect() as db:
            db.execute("INSERT INTO history_watermarks VALUES (?,?)", (self.run_id, 5))

        source._refresh_records("fixture-operator")
        with source._connect() as db:
            rows = {row["source_id"]: json.loads(row["record_json"])
                    for row in db.execute("SELECT source_id,record_json FROM source_records")}
        correction_id = (
            "projection-correction:temporal-start-link-v1:"
            f"{self.run_id}:5:assignment:attempt-1")
        correction = rows[correction_id]
        self.assertEqual(correction["event_type"],
                         "com.exomachina.assignment.state_changed.v1")
        self.assertEqual(correction["source_kind"], "temporal")
        self.assertEqual(correction["time"], completion_time)
        self.assertEqual(correction["fields"], {
            "state": "unknown", "node": "research-node",
            "capability": "research", "provider_identity": "worker-safe"})
        self.assertEqual(correction["attempt_id"], "1")
        self.assertNotIn("ended_at", correction["fields"])
        self.assertNotIn("evidence_status", correction["fields"])
        self.assertEqual(rows[legacy_id], prior)

        linked = rows[f"{legacy_id}:attempt-2"]
        self.assertEqual(linked["attempt_id"], "2")
        self.assertEqual(linked["fields"]["state"], "completed")
        self.assertEqual(linked["fields"]["ended_at"], completion_time)

        restarted = RuntimeObservationSource(self.director,
            {"name": "factory-test", "capability": {"id": "verified-research@1"}},
            database, history_reader=lambda: [(self.run_id, history)],
            refresh_interval_seconds=0)
        restarted._refresh_records("fixture-operator")
        with restarted._connect() as db:
            after_restart = {row["source_id"]: row["record_json"]
                             for row in db.execute(
                                 "SELECT source_id,record_json FROM source_records")}
        self.assertEqual(after_restart[legacy_id], prior_json)
        self.assertEqual(sum(key == correction_id for key in after_restart), 1)

    def test_assignment_node_link_corrections_match_pinned_graph_and_replay(self):
        history = self._assignment_retry_history()
        history[0]["attributes"]["input"]["document"] = {
            "nodes": {"gather": {"type": "parallel", "branches": {}}}}
        history[1]["attributes"]["input"]["node"] = "gather"
        database = self.instance / "assignment-node-links.sqlite3"
        config = {"name": "factory-test",
                  "capability": {"id": "verified-research@1"}}
        source = RuntimeObservationSource(
            self.director, config, database,
            history_reader=lambda: [(self.run_id, history)],
            refresh_interval_seconds=0)
        records = source._history_records(
            self.run_id, history, "task-1", "context-1")
        correction_prefix = "projection-correction:assignment-node-link-v1:"
        corrections = {record["source_id"]: record for record in records
                       if record["source_id"].startswith(correction_prefix)}
        self.assertEqual(len(corrections), 4)

        event_by_id = {event["event_id"]: event for event in history}
        attempts = {2: "1", 3: "1", 4: "2", 5: "2"}
        original_rows = {}
        for event_id, attempt_id in attempts.items():
            correction_id = f"{correction_prefix}{self.run_id}:{event_id}:assignment"
            correction = corrections[correction_id]
            if event_id == 5:
                original_id = f"{self.run_id}:5:assignment:attempt-2"
            else:
                original_id = f"{self.run_id}:{event_id}:assignment"
            original = next(row for row in records if row["source_id"] == original_id)
            self.assertEqual(correction["time"], event_by_id[event_id]["time"])
            self.assertEqual(correction["time"], original["time"])
            expected_fields = {**original["fields"], "node": "gather"}
            if event_id == 5:
                # The completion correction carries the exact started-event
                # timestamp for this same Temporal attempt so a public snapshot
                # can retain both start and end after applying the correction.
                expected_fields["started_at"] = event_by_id[4]["time"]
            self.assertEqual(correction["fields"], expected_fields)
            self.assertEqual(correction["task_id"], original["task_id"])
            self.assertEqual(correction["assignment_id"], original["assignment_id"])
            self.assertEqual(correction["attempt_id"], attempt_id)
            original_rows[original_id] = _canonical(original)

        # Seed the pre-correction durable projection and watermark. Refresh must
        # append links under new IDs without changing the previously stored rows.
        for record in records:
            if not record["source_id"].startswith(correction_prefix):
                source._insert(record)
        with source._connect() as db:
            db.execute("INSERT INTO history_watermarks VALUES (?,?)", (self.run_id, 5))
        source._refresh_records("fixture-operator")
        with source._connect() as db:
            persisted = {row["source_id"]: row["record_json"] for row in db.execute(
                "SELECT source_id,record_json FROM source_records")}
        for original_id, original_json in original_rows.items():
            self.assertEqual(persisted[original_id], original_json)
        self.assertTrue(set(corrections) <= set(persisted))

        restarted = RuntimeObservationSource(
            self.director, config, database,
            history_reader=lambda: [(self.run_id, history)],
            refresh_interval_seconds=0)
        restarted._refresh_records("fixture-operator")
        with restarted._connect() as db:
            replayed = {row["source_id"]: row["record_json"] for row in db.execute(
                "SELECT source_id,record_json FROM source_records")}
        self.assertEqual(replayed, persisted)

        projection = FactoryObservation(
            restarted, self.instance / "assignment-node-links-observation.sqlite3")
        snapshot = projection.snapshot(
            "fixture-operator", self.director.identity, run_id=self.run_id)
        projected_run = next(row for row in snapshot["state"]["runs"]
                             if row["id"] == self.run_id)
        assignment = next(row for row in projected_run["assignments"]
                          if row["id"] == "assignment-retained")
        attempts_by_id = {row["attempt_id"]: row for row in assignment["attempts"]}
        self.assertEqual(attempts_by_id["1"]["state"], "running")
        self.assertEqual(attempts_by_id["1"]["node"], "gather")
        self.assertEqual(attempts_by_id["1"]["started_at"], event_by_id[3]["time"])
        self.assertEqual(attempts_by_id["1"]["provider_identity"], "worker-safe")
        self.assertEqual(attempts_by_id["2"]["state"], "completed")
        self.assertEqual(attempts_by_id["2"]["node"], "gather")
        self.assertEqual(attempts_by_id["2"]["started_at"], event_by_id[4]["time"])
        self.assertEqual(attempts_by_id["2"]["ended_at"], event_by_id[5]["time"])
        self.assertEqual(attempts_by_id["2"]["provider_identity"], "worker-safe")

        # Activity IDs are not graph-node evidence. Unknown/nonmatching explicit
        # node values do not produce a public assignment-node correction.
        mismatch = deepcopy(history)
        mismatch[1]["attributes"]["input"]["node"] = "assign-temporal-id"
        mismatch_records = restarted._history_records(
            self.run_id, mismatch, "task-1", "context-1")
        self.assertFalse(any(record["source_id"].startswith(correction_prefix)
                             for record in mismatch_records))

    def test_run_node_link_corrections_backfill_schedule_and_start_only(self):
        history, _, _ = self._history()
        workflow_start = deepcopy(history[0])
        workflow_start["attributes"]["input"]["document"] = {
            "start": "independent_quality",
            "nodes": {
                "independent_quality": {"type": "quality", "next": "complete"},
                "complete": {"type": "complete", "next": []},
            },
        }
        schedule_time = "2026-10-03T15:54:10.000Z"
        start_time = "2026-10-03T15:54:11.000Z"
        activity_schedule = {
            "event_id": 2, "event_type": "ACTIVITY_TASK_SCHEDULED",
            "time": schedule_time, "attributes": {
                "activity_id": "5", "activity_type": "review",
                "input": {"node": "independent_quality", "identity": "quality-safe",
                          "candidate": {"revision": "r1", "sha256": "e" * 64}},
            },
        }
        activity_start = {
            "event_id": 3, "event_type": "ACTIVITY_TASK_STARTED",
            "time": start_time, "attributes": {"scheduled_event_id": 2, "attempt": 1},
        }
        activity_complete = {
            "event_id": 4, "event_type": "ACTIVITY_TASK_COMPLETED",
            "time": "2026-10-03T15:54:12.000Z",
            "attributes": {"scheduled_event_id": 2, "started_event_id": 3,
                           "result": {"artifact": {
                               "accepted": True, "findings": []}}},
        }
        history = [workflow_start, activity_schedule, activity_start, activity_complete]
        config = {"name": "factory-test",
                  "capability": {"id": "verified-research@1"}}
        database = self.instance / "run-node-links.sqlite3"
        source = RuntimeObservationSource(
            self.director, config, database,
            history_reader=lambda: [(self.run_id, history)],
            refresh_interval_seconds=0)
        records = source._history_records(self.run_id, history, "task-1", "context-1")
        correction_prefix = "projection-correction:run-node-link-v1:"
        corrections = {item["source_id"]: item for item in records
                       if item["source_id"].startswith(correction_prefix)}
        schedule_id = f"{correction_prefix}{self.run_id}:2:run"
        start_id = f"{correction_prefix}{self.run_id}:3:run"
        self.assertEqual(set(corrections), {schedule_id, start_id})

        scheduled_row = next(item for item in records
                             if item["source_id"] == f"{self.run_id}:2")
        self.assertEqual(scheduled_row["fields"]["node"], "5")
        expected_schedule_fields = {
            **scheduled_row["fields"], "node": "independent_quality"}
        self.assertEqual(corrections[schedule_id]["fields"], expected_schedule_fields)
        self.assertEqual(corrections[schedule_id]["time"], schedule_time)
        self.assertEqual(corrections[start_id]["fields"], expected_schedule_fields)
        self.assertEqual(corrections[start_id]["time"], start_time)
        self.assertFalse(any(item["source_id"].startswith(correction_prefix)
                             and item["source_id"].endswith(":4:run")
                             for item in records))

        # Preserve the original scheduled fact and watermark, then verify the
        # node links append once and remain stable on restart.
        for item in records:
            if not item["source_id"].startswith(correction_prefix):
                source._insert(item)
        with source._connect() as db:
            db.execute("INSERT INTO history_watermarks VALUES (?,?)", (self.run_id, 4))
            original_schedule_json = db.execute(
                "SELECT record_json FROM source_records WHERE source_id=?",
                (f"{self.run_id}:2",)).fetchone()[0]

        source._refresh_records("fixture-operator")
        with source._connect() as db:
            persisted = {row["source_id"]: row["record_json"] for row in db.execute(
                "SELECT source_id,record_json FROM source_records")}
        self.assertEqual(persisted[f"{self.run_id}:2"], original_schedule_json)
        self.assertTrue(set(corrections) <= set(persisted))

        restarted = RuntimeObservationSource(
            self.director, config, database,
            history_reader=lambda: [(self.run_id, history)],
            refresh_interval_seconds=0)
        restarted._refresh_records("fixture-operator")
        with restarted._connect() as db:
            replayed = {row["source_id"]: row["record_json"] for row in db.execute(
                "SELECT source_id,record_json FROM source_records")}
        self.assertEqual(replayed, persisted)
        self.assertEqual(replayed[f"{self.run_id}:2"], original_schedule_json)

        projection = FactoryObservation(
            restarted, self.instance / "run-node-links-observation.sqlite3")
        snapshot = projection.snapshot(
            "fixture-operator", self.director.identity, run_id=self.run_id)
        projected_run = next(row for row in snapshot["state"]["runs"]
                             if row["id"] == self.run_id)
        self.assertEqual(projected_run["status"]["node"], "independent_quality")

        unknown = deepcopy(history)
        unknown[1]["attributes"]["input"]["node"] = "5"
        unknown_records = restarted._history_records(
            self.run_id, unknown, "task-1", "context-1")
        self.assertFalse(any(item["source_id"].startswith(correction_prefix)
                             for item in unknown_records))

    def test_run_node_backfill_preserves_terminal_state_across_restart(self):
        history, _, _ = self._history()
        workflow_start = deepcopy(history[0])
        workflow_start["attributes"]["input"]["document"] = {
            "start": "independent_quality",
            "nodes": {
                "independent_quality": {"type": "quality", "next": "complete"},
                "complete": {"type": "complete", "next": []},
            },
        }
        history = [
            workflow_start,
            {"event_id": 2, "event_type": "ACTIVITY_TASK_SCHEDULED",
             "time": "2026-10-03T15:54:10.000Z", "attributes": {
                 "activity_id": "5", "activity_type": "review",
                 "input": {"node": "independent_quality", "identity": "quality-safe",
                           "candidate": {"revision": "r1", "sha256": "e" * 64}}}},
            {"event_id": 3, "event_type": "ACTIVITY_TASK_STARTED",
             "time": "2026-10-03T15:54:11.000Z", "attributes": {
                 "scheduled_event_id": 2, "attempt": 1}},
            {"event_id": 4, "event_type": "ACTIVITY_TASK_COMPLETED",
             "time": "2026-10-03T15:54:12.000Z", "attributes": {
                 "scheduled_event_id": 2, "started_event_id": 3,
                 "result": {"artifact": {
                     "accepted": True, "findings": []}}}},
            {"event_id": 5, "event_type": "WORKFLOW_EXECUTION_COMPLETED",
             "time": "2026-10-03T15:54:13.000Z", "attributes": {
                 "result": {"status": "accepted"}}},
        ]
        correction_prefix = "projection-correction:run-node-link-v1:"
        records_source = RuntimeObservationSource(
            self.director, {"name": "factory-test",
                             "capability": {"id": "verified-research@1"}},
            self.instance / "terminal-run-node-links.sqlite3",
            history_reader=lambda: [], refresh_interval_seconds=0)
        records = records_source._history_records(
            self.run_id, history, "task-1", "context-1")
        self.assertTrue(any(item["source_id"].startswith(correction_prefix)
                            for item in records))

        database = self.instance / "terminal-run-node-links.sqlite3"
        current_history = history
        config = {"name": "factory-test",
                  "capability": {"id": "verified-research@1"}}
        source = RuntimeObservationSource(
            self.director, config, database,
            history_reader=lambda: [(self.run_id, current_history)],
            refresh_interval_seconds=0)
        for item in records:
            if not item["source_id"].startswith(correction_prefix):
                source._insert(item)
        with source._connect() as db:
            db.execute("INSERT INTO history_watermarks VALUES (?,?)", (self.run_id, 5))
            original_schedule = db.execute(
                "SELECT record_json FROM source_records WHERE source_id=?",
                (f"{self.run_id}:2",)).fetchone()[0]

        # A terminal event in the current history blocks late run-node rows.
        source._refresh_records("fixture-operator")
        with source._connect() as db:
            after_terminal_history = {row["source_id"]: row["record_json"]
                                      for row in db.execute(
                                          "SELECT source_id,record_json FROM source_records")}
        self.assertFalse(any(source_id.startswith(correction_prefix)
                             for source_id in after_terminal_history))
        self.assertEqual(after_terminal_history[f"{self.run_id}:2"], original_schedule)

        # Even if a later history read is nonterminal/truncated, the persisted
        # run end fact still prevents a working correction from being appended.
        current_history = history[:-1]
        restarted = RuntimeObservationSource(
            self.director, config, database,
            history_reader=lambda: [(self.run_id, current_history)],
            refresh_interval_seconds=0)
        restarted._refresh_records("fixture-operator")
        with restarted._connect() as db:
            after_restart = {row["source_id"]: row["record_json"] for row in db.execute(
                "SELECT source_id,record_json FROM source_records")}
        self.assertEqual(after_restart, after_terminal_history)

        projection = FactoryObservation(
            restarted, self.instance / "terminal-run-node-observation.sqlite3")
        snapshot = projection.snapshot(
            "fixture-operator", self.director.identity, run_id=self.run_id)
        projected_run = next(row for row in snapshot["state"]["runs"]
                             if row["id"] == self.run_id)
        self.assertEqual(projected_run["status"]["state"], "completed")
        self.assertEqual(projected_run["status"]["phase"], "accepted")
        self.assertEqual(projected_run["status"]["ended_at"],
                         "2026-10-03T15:54:13.000Z")

    def test_run_node_backfill_does_not_replace_later_wait(self):
        history, _, _ = self._history()
        workflow_start = deepcopy(history[0])
        workflow_start["attributes"]["input"]["document"] = {
            "start": "independent_quality",
            "nodes": {
                "independent_quality": {"type": "quality", "next": "complete"},
                "complete": {"type": "complete", "next": []},
            },
        }
        history = [
            workflow_start,
            {"event_id": 2, "event_type": "ACTIVITY_TASK_SCHEDULED",
             "time": "2026-10-03T15:54:10.000Z", "attributes": {
                 "activity_id": "5", "activity_type": "review",
                 "input": {"node": "independent_quality", "identity": "quality-safe",
                           "candidate": {"revision": "r1", "sha256": "e" * 64}}}},
            {"event_id": 3, "event_type": "ACTIVITY_TASK_STARTED",
             "time": "2026-10-03T15:54:11.000Z", "attributes": {
                 "scheduled_event_id": 2, "attempt": 1}},
            {"event_id": 4, "event_type": "ACTIVITY_TASK_COMPLETED",
             "time": "2026-10-03T15:54:12.000Z", "attributes": {
                 "scheduled_event_id": 2, "started_event_id": 3,
                 "result": {"artifact": {
                     "accepted": True, "findings": []}}}},
            {"event_id": 5, "event_type": "TIMER_STARTED",
             "time": "2026-10-03T15:54:15.000Z", "attributes": {
                 "timeout_seconds": 30}},
        ]
        correction_prefix = "projection-correction:run-node-link-v1:"
        database = self.instance / "wait-run-node-links.sqlite3"
        config = {"name": "factory-test",
                  "capability": {"id": "verified-research@1"}}
        source = RuntimeObservationSource(
            self.director, config, database,
            history_reader=lambda: [(self.run_id, history)],
            refresh_interval_seconds=0)
        records = source._history_records(self.run_id, history, "task-1", "context-1")
        self.assertTrue(any(item["source_id"].startswith(correction_prefix)
                            for item in records))

        # Seed the pre-existing journal at the same watermark without node
        # corrections. Its newest run-state fact is the later director wait.
        for item in records:
            if not item["source_id"].startswith(correction_prefix):
                source._insert(item)
        with source._connect() as db:
            db.execute("INSERT INTO history_watermarks VALUES (?,?)", (self.run_id, 5))
            original_schedule = db.execute(
                "SELECT record_json FROM source_records WHERE source_id=?",
                (f"{self.run_id}:2",)).fetchone()[0]

        source._refresh_records("fixture-operator")
        with source._connect() as db:
            persisted = {row["source_id"]: json.loads(row["record_json"])
                         for row in db.execute(
                             "SELECT source_id,record_json FROM source_records")}
        self.assertFalse(any(source_id.startswith(correction_prefix)
                             for source_id in persisted))
        self.assertEqual(_canonical(persisted[f"{self.run_id}:2"]), original_schedule)

        projection = FactoryObservation(
            source, self.instance / "wait-run-node-observation.sqlite3")
        snapshot = projection.snapshot(
            "fixture-operator", self.director.identity, run_id=self.run_id)
        projected_run = next(row for row in snapshot["state"]["runs"]
                             if row["id"] == self.run_id)
        self.assertEqual(projected_run["status"]["state"], "input-required")
        self.assertEqual(projected_run["status"]["phase"], "awaiting-director")
        self.assertEqual(projected_run["status"]["wait_deadline"],
                         "2026-10-03T15:54:45.000Z")

    def test_unlinked_or_cross_schedule_completion_never_defaults_to_attempt_one(self):
        scenarios = ((None, 2, False), (4, 77, False), (4, 2, True))
        for started_event_id, started_schedule_id, duplicate_start in scenarios:
            with self.subTest(started_event_id=started_event_id,
                              started_schedule_id=started_schedule_id,
                              duplicate_start=duplicate_start):
                history = self._assignment_retry_history(
                    started_event_id=started_event_id,
                    started_schedule_id=started_schedule_id)
                if duplicate_start:
                    duplicate = deepcopy(history[3])
                    duplicate["attributes"]["attempt"] = 3
                    history.insert(4, duplicate)
                source = RuntimeObservationSource(self.director,
                    {"name": "factory-test", "capability": {"id": "verified-research@1"}},
                    self.instance / f"unlinked-{started_event_id}-{started_schedule_id}.sqlite3",
                    history_reader=lambda: [], refresh_interval_seconds=0)
                completion_time = "2026-10-03T15:54:23.206Z"
                legacy_id = f"{self.run_id}:5:assignment"
                prior = source._record("temporal", legacy_id,
                    "com.exomachina.assignment.state_changed.v1", completion_time,
                    run_id=self.run_id,
                    fields={"state": "completed", "ended_at": completion_time,
                            "capability": "research", "provider_identity": "worker-safe"},
                    task_id="task-1", context_id="context-1",
                    assignment_id="assignment-retained", attempt_id="1")
                source._insert(prior)
                prior_json = _canonical(prior)
                records = source._history_records(
                    self.run_id, history, "task-1", "context-1")
                assignment_records = [record for record in records if record.get("event_type") ==
                    "com.exomachina.assignment.state_changed.v1"]
                self.assertFalse(any(record["fields"].get("state") == "completed"
                                     for record in assignment_records))
                correction_id = (
                    "projection-correction:temporal-start-link-v1:"
                    f"{self.run_id}:5:assignment:attempt-1")
                correction = next(record for record in assignment_records
                                  if record["source_id"] == correction_id)
                self.assertEqual(correction["fields"]["state"], "unknown")
                self.assertEqual(correction["time"], completion_time)
                self.assertNotIn("ended_at", correction["fields"])
                self.assertNotIn("evidence_status", correction["fields"])
                with source._connect() as db:
                    preserved = db.execute(
                        "SELECT record_json FROM source_records WHERE source_id=?",
                        (legacy_id,)).fetchone()[0]
                self.assertEqual(preserved, prior_json)

        fresh = RuntimeObservationSource(self.director,
            {"name": "factory-test", "capability": {"id": "verified-research@1"}},
            self.instance / "fresh-unlinked.sqlite3", history_reader=lambda: [],
            refresh_interval_seconds=0)
        fresh_records = fresh._history_records(
            self.run_id, self._assignment_retry_history(started_event_id=None),
            "task-1", "context-1")
        self.assertFalse(any(record["source_id"].startswith(
            "projection-correction:temporal-start-link-v1:") for record in fresh_records))

    def test_temporal_fetch_preserves_raw_completion_started_event_id(self):
        from temporalio.api.enums.v1 import EventType

        completion = SimpleNamespace(
            event_type=EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, event_id=5,
            event_time=SimpleNamespace(ToDatetime=lambda *, tzinfo: datetime(
                2026, 10, 3, 15, 54, 23, 206000, tzinfo=tzinfo)),
            activity_task_completed_event_attributes=SimpleNamespace(
                scheduled_event_id=2, started_event_id=4, result=None))
        handle = SimpleNamespace(fetch_history=lambda: None)

        async def fetch_history():
            return SimpleNamespace(events=[completion])

        handle.fetch_history = fetch_history
        client = SimpleNamespace(
            data_converter=SimpleNamespace(payload_converter=SimpleNamespace(
                from_payloads=lambda _payloads: [])),
            get_workflow_handle=lambda _workflow_id: handle)

        async def get_client():
            return client

        self.director.client = get_client
        source = RuntimeObservationSource(self.director,
            {"name": "factory-test", "capability": {"id": "verified-research@1"}},
            self.instance / "runtime-source.sqlite3", history_reader=None,
            refresh_interval_seconds=0)
        fetched = asyncio.run(source._fetch_histories())
        self.assertEqual(fetched[0][1][0]["attributes"], {
            "scheduled_event_id": 2, "started_event_id": 4, "result": None})

    def test_admission_projection_appends_public_transitions_with_exact_writer_times(self):
        history, _, _ = self._history()
        queue = AdmissionQueue(self.instance / "admission.sqlite3",
                               factory_id=self.director.identity, capacity=1)
        self.director.admission_queue = queue
        admitted = queue.enqueue("start-action-1", task_id="task-1")
        self.assertEqual(admitted["state"], "admitted")
        queue.release("start-action-1", release_id="terminal-fact-1")
        released = queue.get("start-action-1")

        source = RuntimeObservationSource(self.director,
            {"name": "factory-test", "capability": {"id": "verified-research@1"}},
            self.instance / "runtime-source.sqlite3",
            history_reader=lambda: [(self.run_id, history)],
            refresh_interval_seconds=0)
        projection = FactoryObservation(source, self.instance / "observation.sqlite3")
        snapshot = projection.snapshot("fixture-operator", self.director.identity,
                                       run_id=self.run_id)
        run = next(row for row in snapshot["state"]["runs"]
                   if row["id"] == self.run_id)
        admission_events = run["admissions"]

        self.assertEqual([event["state"] for event in admission_events],
                         ["admitted", "released"])
        self.assertTrue(all(event["admission_id"] == "start-action-1"
                            and event["run_id"] == self.run_id
                            for event in admission_events))
        self.assertEqual(source.admission_observation_status, "current")
        self.assertEqual(source.admission_capacity_snapshot, {
            "factory_id": self.director.identity, "capacity_limit": 1,
            "active_count": 0, "queued_count": 0})

        with source._connect() as db:
            persisted = [json.loads(row[0]) for row in db.execute(
                "SELECT record_json FROM source_records ORDER BY position")]
        persisted = [item for item in persisted if item.get("source_kind") == "admission"]
        self.assertEqual(len(persisted), 2)
        self.assertEqual([item["time"] for item in persisted],
                         [admitted["admitted_at"], released["released_at"]])
        self.assertEqual([item["source_id"] for item in persisted], [
            "admission:14:start-action-1:admitted",
            "admission:14:start-action-1:released",
        ])

    def _history(self):
        report = {"kind": "verified_report@1", "revision": "r1",
                  "packet_digest": "e" * 64, "markdown": "# Accepted report\n",
                  "claims": []}
        content = json.dumps(report, sort_keys=True, separators=(",", ":"),
                             ensure_ascii=False)
        sha = hashlib.sha256(content.encode()).hexdigest()
        closure = {"manifest_digest": "a" * 64,
                   "manifest": {"root_digest": "c" * 64,
                                "interpreter": {"build_id": "b-123456789abc"},
                                "services": {}}}
        document = {"nodes": {"synthesize": {"type": "synthesize",
                                                "next": "complete"},
                              "complete": {"type": "complete", "next": []}}}
        self.director.module.publications.value = {
            "manifest_digest": "a" * 64, "package_digest": "b" * 64,
            "build_id": "b-123456789abc", "label": "v1", "closure": closure}
        self.director.module.package_record = {"root": document, "bindings": {}}
        start = {"run": self.run_id, "definition_digest": "c" * 64,
                 "package_digest": "b" * 64, "closure": closure,
                 "document": document,
                 "director": {"identity": self.director.identity, "epoch": 1}}
        return [
            {"event_id": 1, "event_type": "WORKFLOW_EXECUTION_STARTED",
             "time": "2026-10-02T12:00:00Z", "attributes": {"input": start}},
            {"event_id": 2, "event_type": "ACTIVITY_TASK_SCHEDULED",
             "time": "2026-10-02T12:00:01Z", "attributes": {
                 "activity_id": "synth-1", "activity_type": "synthesize",
                 "input": {"identity": "report-writer"}}},
            {"event_id": 3, "event_type": "ACTIVITY_TASK_COMPLETED",
             "time": "2026-10-02T12:00:02Z", "attributes": {
                 "scheduled_event_id": 2, "result": {
                     "revision": "r1", "sha256": sha, "content": content,
                     "author": "report-writer"}}},
            {"event_id": 4, "event_type": "ACTIVITY_TASK_SCHEDULED",
             "time": "2026-10-02T12:00:03Z", "attributes": {
                 "activity_id": "review-1", "activity_type": "review",
                 "input": {"identity": "quality-reviewer",
                           "binding": {"identity": "quality-reviewer"},
                           "candidate": {"revision": "r1", "sha256": sha}}}},
            {"event_id": 5, "event_type": "ACTIVITY_TASK_COMPLETED",
             "time": "2026-10-02T12:00:04Z", "attributes": {
                 "scheduled_event_id": 4, "result": {
                     "task_id": "quality-task-1",
                     "artifact": {"revision": "r1", "sha256": sha,
                                  "reviewer": "quality-reviewer",
                                  "accepted": True, "findings": []}}}},
            {"event_id": 6, "event_type": "START_CHILD_WORKFLOW_EXECUTION_INITIATED",
             "time": "2026-10-02T12:00:04.500Z", "attributes": {
                 "workflow_id": self.run_id + ":child:abc"}},
            {"event_id": 7, "event_type": "WORKFLOW_EXECUTION_COMPLETED",
             "time": "2026-10-02T12:00:05Z", "attributes": {"result": {
                 "status": "accepted", "artifact": {"revision": "r1", "sha256": sha,
                                                         "content": content},
                 "receipt": {"receipt_id": "fixture-receipt-1", "revision": "r1",
                             "sha256": sha}}}},
        ], {"revision": "r1", "sha256": sha, "content": content}, report

    def test_factory_discovery_timestamp_survives_config_touch_and_source_restart(self):
        database = self.instance / "runtime-source.sqlite3"
        config = {"name": "factory-test", "capability": {"id": "verified-research@1"}}
        first_source = RuntimeObservationSource(self.director, config, database,
                                                history_reader=lambda: [],
                                                refresh_interval_seconds=0)
        first_source._add_factory_and_publication()
        with first_source._connect() as db:
            first = json.loads(db.execute(
                "SELECT record_json FROM source_records WHERE source_id=?",
                ("factory:" + self.director.identity,)).fetchone()[0])
        identity_path = self.director.database.parent / "instance.json"
        stat = identity_path.stat()
        os.utime(identity_path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 5_000_000_000))

        restarted_source = RuntimeObservationSource(self.director, config, database,
                                                   history_reader=lambda: [],
                                                   refresh_interval_seconds=0)
        restarted_source._add_factory_and_publication()
        with restarted_source._connect() as db:
            rows = db.execute("SELECT record_json FROM source_records WHERE source_id=?",
                              ("factory:" + self.director.identity,)).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(json.loads(rows[0][0]), first)
        self.assertEqual(json.loads(rows[0][0])["time"], first["time"])

    def test_local_outcome_timestamp_survives_director_db_mtime_change_and_restart(self):
        _history, accepted, _report = self._history()
        with self.director.connect() as db:
            db.execute("UPDATE runs SET outcome_json=? WHERE run_id=?",
                       (json.dumps({"result": {"artifact": accepted}}), self.run_id))
        database = self.instance / "runtime-source.sqlite3"
        config = {"name": "factory-test", "capability": {"id": "verified-research@1"}}
        first_source = RuntimeObservationSource(self.director, config, database,
                                                history_reader=lambda: [])
        first_source._local_outcome_records(first_source._run_rows())
        source_id = f"{self.run_id}:local-outcome:accepted-artifact"
        with first_source._connect() as db:
            first = json.loads(db.execute(
                "SELECT record_json FROM source_records WHERE source_id=?",
                (source_id,)).fetchone()[0])

        stat = self.director.database.stat()
        os.utime(self.director.database,
                 ns=(stat.st_atime_ns, stat.st_mtime_ns + 5_000_000_000))
        restarted = RuntimeObservationSource(self.director, config, database,
                                             history_reader=lambda: [])
        restarted._local_outcome_records(restarted._run_rows())
        with restarted._connect() as db:
            rows = db.execute("SELECT record_json FROM source_records WHERE source_id=?",
                              (source_id,)).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(json.loads(rows[0][0]), first)

    def test_history_source_projects_pins_quality_fixture_and_exact_artifact(self):
        history, accepted, report = self._history()
        child_id = self.run_id + ":child:abc"
        child_input = dict(history[0]["attributes"]["input"])
        child_input.update(run=child_id, definition_digest="f" * 64,
                           document={"nodes": {"done": {"type": "complete", "next": []}}})
        child_history = [{"event_id": 1, "event_type": "WORKFLOW_EXECUTION_STARTED",
                          "time": "2026-10-02T12:00:04.600Z",
                          "attributes": {"input": child_input}},
                         {"event_id": 2, "event_type": "WORKFLOW_EXECUTION_COMPLETED",
                          "time": "2026-10-02T12:00:04.700Z",
                          "attributes": {"result": {"status": "completed"}}}]
        source = RuntimeObservationSource(self.director, {"name": "factory-test",
            "capability": {"id": "verified-research@1"}},
            self.instance / "runtime-source.sqlite3",
            history_reader=lambda: [(self.run_id, history), (child_id, child_history)],
            refresh_interval_seconds=0)
        projection = FactoryObservation(source, self.instance / "observation.sqlite3")
        snapshot = projection.snapshot("fixture-operator", self.director.identity)
        self.assertTrue(isinstance(snapshot["cursor"], str)
                        and snapshot["cursor"].startswith("c1."))
        runs = {run["id"]: run for run in snapshot["state"]["runs"]}
        run = runs[self.run_id]
        child_run = runs[child_id]
        self.assertEqual(run["pinned"], {"manifest_digest": "a" * 64,
            "package_digest": "b" * 64, "definition_digest": "c" * 64,
            "interpreter_build": "b-123456789abc"})
        self.assertEqual(run["status"]["state"], "completed")
        self.assertEqual(run["quality"][0]["accepted"], True)
        self.assertEqual(run["quality"][0]["reviewer_identity"], "quality-reviewer")
        self.assertEqual(run["quality"][0]["artifact_revision"], "r1")
        self.assertEqual(run["quality"][0]["artifact_sha256"], accepted["sha256"])
        # The completed run's result repeats a receipt, but a receipt fact comes
        # only from a release node's own completion (one fact per delivery).
        self.assertEqual(run["delivery"], [])
        self.assertEqual(run["task"], {"id": "task-1", "context_id": "context-1"})
        self.assertEqual(child_run["task"], {"id": "task-1", "context_id": "context-1"})
        self.assertEqual(child_run["pinned"]["definition_digest"], "f" * 64)
        self.assertNotIn("run_inputs", json.dumps(snapshot).lower())
        first_page = source.read_page("fixture-operator", self.director.identity,
                                      None, limit=1)
        self.assertTrue(first_page.next_cursor.startswith("h1."))
        self.assertIsInstance(first_page.next_cursor, str)

        delivered = projection.inspect_artifact("fixture-operator", self.director.identity,
            self.run_id, "r1", accepted["sha256"])
        self.assertEqual(delivered["content"], accepted["content"].encode())
        self.assertEqual(hashlib.sha256(delivered["content"]).hexdigest(), accepted["sha256"])
        markdown = accepted_markdown(accepted, revision="r1", sha256=accepted["sha256"])
        self.assertEqual(markdown.content, report["markdown"].encode())
        self.assertNotEqual(markdown.markdown_sha256, markdown.accepted_sha256)

    def test_local_delivery_public_receipt_projects_exact_accepted_bytes_once(self):
        history, accepted, report = self._history()
        markdown = report["markdown"].encode("utf-8")
        receipt = {
            "factory_id": self.director.identity,
            "run_id": self.run_id,
            "task_id": "task-1",
            "context_id": "context-1",
            "receipt_id": "delivery-retained-run-r1",
            "artifact_revision": accepted["revision"],
            "artifact_sha256": accepted["sha256"],
            "markdown_sha256": hashlib.sha256(markdown).hexdigest(),
            "destination_identity": "runtime-test-destination",
            "delivery_kind": "local_file",
            "byte_length": len(markdown),
            "state": "delivered",
            "recorded_at": "2026-10-02T13:00:00Z",
        }
        reader = FakePublicLocalDelivery([receipt])
        source = RuntimeObservationSource(self.director, {"name": "factory-test",
            "capability": {"id": "verified-research@1"}},
            self.instance / "runtime-source.sqlite3",
            history_reader=lambda: [(self.run_id, history)],
            delivery_reader=reader, refresh_interval_seconds=0)
        projection = FactoryObservation(source, self.instance / "observation.sqlite3")

        first = projection.snapshot("fixture-operator", self.director.identity)
        second = projection.snapshot("fixture-operator", self.director.identity)
        run = next(row for row in second["state"]["runs"] if row["id"] == self.run_id)
        local = [row for row in run["delivery"]
                 if row.get("receipt_id") == receipt["receipt_id"]]
        self.assertEqual(len(local), 1)
        self.assertEqual(reader.calls, [self.run_id, self.run_id])
        with source._connect() as db:
            source_record = json.loads(db.execute(
                "SELECT record_json FROM source_records WHERE source_id=?",
                ("local-delivery:" + receipt["receipt_id"],)).fetchone()[0])
        self.assertEqual(source_record["fields"]["delivered_at"], receipt["recorded_at"])
        self.assertEqual(source_record["time"], receipt["recorded_at"])
        self.assertEqual(local[0]["outcome"], "local-file-delivered")
        self.assertEqual(local[0]["delivery_kind"], "local_file")
        # The shared public Observation contract canonicalizes UTC timestamps
        # to milliseconds; the append-only source above retains the exact
        # writer timestamp.
        self.assertEqual(local[0]["delivered_at"], "2026-10-02T13:00:00.000Z")
        self.assertEqual(local[0]["artifact_sha256"], accepted["sha256"])
        self.assertEqual(local[0]["markdown_sha256"], receipt["markdown_sha256"])
        self.assertEqual(local[0]["byte_length"], len(markdown))
        self.assertEqual(run["task"], {"id": "task-1", "context_id": "context-1"})
        self.assertNotIn("state", local[0])
        self.assertNotIn("recorded_at", local[0])
        self.assertEqual(first["state"]["runs"], second["state"]["runs"])

    def test_local_delivery_receipt_rejects_wrong_binding_or_markdown_bytes(self):
        history, accepted, report = self._history()
        markdown_digest = hashlib.sha256(report["markdown"].encode()).hexdigest()
        base = {
            "factory_id": self.director.identity,
            "run_id": self.run_id,
            "task_id": "task-1",
            "context_id": "context-1",
            "artifact_revision": accepted["revision"],
            "artifact_sha256": accepted["sha256"],
            "markdown_sha256": markdown_digest,
            "destination_identity": "runtime-test-destination",
            "delivery_kind": "local_file",
            "byte_length": len(report["markdown"].encode()),
            "state": "delivered",
            "recorded_at": "2026-10-02T13:00:00Z",
        }
        invalid = [
            {**base, "receipt_id": "wrong-context", "context_id": "other-context"},
            {**base, "receipt_id": "wrong-markdown", "markdown_sha256": "f" * 64},
        ]
        source = RuntimeObservationSource(self.director, {"name": "factory-test",
            "capability": {"id": "verified-research@1"}},
            self.instance / "runtime-source.sqlite3",
            history_reader=lambda: [(self.run_id, history)],
            delivery_reader=FakePublicLocalDelivery(invalid), refresh_interval_seconds=0)
        projection = FactoryObservation(source, self.instance / "observation.sqlite3")
        snapshot = projection.snapshot("fixture-operator", self.director.identity)
        run = next(row for row in snapshot["state"]["runs"] if row["id"] == self.run_id)
        self.assertFalse(any(row.get("receipt_id") in {"wrong-context", "wrong-markdown"}
                             for row in run["delivery"]))

    def test_old_history_watermark_appends_child_quality_backfill_without_rewriting_facts(self):
        parent_history, _, _ = self._history()
        child_id = self.run_id + ":child:quality"
        parent_history = [deepcopy(parent_history[0]), {
            "event_id": 2, "event_type": "START_CHILD_WORKFLOW_EXECUTION_INITIATED",
            "time": "2026-10-02T12:00:04Z", "attributes": {"workflow_id": child_id}}]
        child_history, _, _ = self._history()
        child_history = deepcopy(child_history[:5])
        child_history[0]["attributes"]["input"].update(
            run=child_id, definition_digest="f" * 64)
        histories = [(self.run_id, parent_history), (child_id, child_history)]
        source = RuntimeObservationSource(self.director,
            {"name": "factory-test", "capability": {"id": "verified-research@1"}},
            self.instance / "runtime-source.sqlite3",
            history_reader=lambda: histories, refresh_interval_seconds=0)

        # Model a projection that consumed both complete histories but omitted
        # quality. Preserve every original fact and the old high-water marks.
        for workflow_id, events in histories:
            for record in source._history_records(workflow_id, events,
                                                   "task-1", "context-1"):
                if record["event_type"] != "com.exomachina.quality.verdict.v1":
                    source._insert(record)
            with source._connect() as db:
                db.execute("INSERT INTO history_watermarks VALUES (?,?)",
                           (workflow_id, max(event["event_id"] for event in events)))
        with source._connect() as db:
            old_rows = {row["source_id"]: row["record_json"] for row in db.execute(
                "SELECT source_id,record_json FROM source_records")}

        source._refresh_records("fixture-operator")
        with source._connect() as db:
            after_rows = {row["source_id"]: row["record_json"] for row in db.execute(
                "SELECT source_id,record_json FROM source_records")}
        for source_id, encoded in old_rows.items():
            self.assertEqual(after_rows[source_id], encoded)
        quality = [json.loads(encoded) for source_id, encoded in after_rows.items()
                   if source_id.startswith(child_id + ":") and
                   source_id.endswith(":quality:backfill-v1")]
        self.assertEqual(len(quality), 1)
        self.assertEqual(quality[0]["task_id"], "task-1")
        self.assertEqual(quality[0]["context_id"], "context-1")
        self.assertTrue(quality[0]["fields"]["accepted"])
        self.assertEqual(quality[0]["fields"]["finding_count"], 0)

        source._refresh_records("fixture-operator")
        with source._connect() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM source_records").fetchone()[0],
                             len(after_rows))

    def test_incomplete_command_retries_then_returns_terminal_outcome(self):
        source = RuntimeObservationSource(self.director, {"name": "factory-test",
            "capability": {"id": "verified-research@1"}},
            self.instance / "runtime-source.sqlite3", history_reader=lambda: [],
            refresh_interval_seconds=0)
        command = {"command_id": "abort-command-1", "task_id": "task-1",
                   "context_id": "context-1", "action": "abort",
                   "expected_state": "input-required", "expected_revision": "r1",
                   "expected_sha256": "d" * 64}
        fingerprint = hashlib.sha256(_canonical(command).encode()).hexdigest()
        with source._connect() as db:
            db.execute("INSERT INTO command_intents VALUES (?,?,?,?,?,?,?)",
                       (command["command_id"], fingerprint, "run-1", "task-1", "context-1",
                        "abort", "2026-10-02T12:00:00Z"))
        source._command_event(command, "run-1", "received")  # crash point before Director call

        receipt = source.submit_command("fixture-operator", self.director.identity, command)
        self.assertEqual(receipt["lifecycle"], "received")
        self.assertEqual(len(self.director.perform_calls), 1)
        repeated = source.submit_command("fixture-operator", self.director.identity, command)
        self.assertEqual(repeated["lifecycle"], "received")
        self.assertEqual(repeated["prior_outcome"], {
            "lifecycle": "applied", "outcome": "abort-recorded",
            "resulting_state": "abort-recorded"})
        self.assertEqual(len(self.director.perform_calls), 1)

        conflict = {**command, "expected_revision": "r2"}
        conflicted = source.submit_command("fixture-operator", self.director.identity, conflict)
        self.assertEqual(conflicted["lifecycle"], "received")
        replayed = source.submit_command("fixture-operator", self.director.identity, command)
        self.assertEqual(replayed["prior_outcome"], {
            "lifecycle": "applied", "outcome": "abort-recorded",
            "resulting_state": "abort-recorded"})
        self.assertEqual(len(self.director.perform_calls), 1)
        with source._connect() as db:
            conflict_rows = db.execute("SELECT source_id,record_json FROM source_records "
                "WHERE source_id LIKE 'command:abort-command-1:rejected:%'").fetchall()
        self.assertEqual(len(conflict_rows), 1)
        conflict_record = json.loads(conflict_rows[0]["record_json"])
        self.assertEqual(conflict_record["fields"]["lifecycle"], "rejected")
        self.assertEqual(conflict_record["fields"]["outcome"], "command-id-conflict")

    def test_director_rejection_projects_only_finite_safe_reason_code(self):
        source = RuntimeObservationSource(self.director, {"name": "factory-test",
            "capability": {"id": "verified-research@1"}},
            self.instance / "runtime-source.sqlite3", history_reader=lambda: [],
            refresh_interval_seconds=0)

        class SeparatelyLoadedDirectorRejected(harness.Rejected):
            safe_code = "decision-deadline-unavailable-or-expired"

        def reject(command, task_id, context_id):
            if command["action_id"] == "named-rejection":
                raise SeparatelyLoadedDirectorRejected("private detail must not escape")
            raise harness.Rejected("another private detail must not escape")

        self.director.perform = reject
        common = {"task_id": "task-1", "context_id": "context-1",
                  "action": "abort", "expected_state": "input-required",
                  "expected_revision": "r1", "expected_sha256": "d" * 64}
        for command_id in ("named-rejection", "unclassified-rejection"):
            source.submit_command("fixture-operator", self.director.identity,
                                  {"command_id": command_id, **common})

        with source._connect() as db:
            records = {row["source_id"]: json.loads(row["record_json"])
                       for row in db.execute("SELECT source_id,record_json FROM source_records "
                                             "WHERE source_id LIKE 'command:%:rejected'")}
        named = records["command:named-rejection:rejected"]["fields"]
        unclassified = records["command:unclassified-rejection:rejected"]["fields"]
        self.assertEqual(named["lifecycle"], "rejected")
        self.assertEqual(named["outcome"], "decision-deadline-unavailable-or-expired")
        self.assertEqual(unclassified["outcome"], "decision-rejected-unclassified")
        self.assertNotIn("private detail", _canonical(records))

    def test_expired_director_wait_uses_named_safe_rejection(self):
        class Handle:
            def __init__(self, value):
                self.value = value

            async def query(self, _query):
                return dict(self.value)

        class Client:
            def __init__(self):
                self.parent = Handle({"child_id": "child-run"})
                self.child = Handle({"phase": "awaiting-director",
                    "applied_decisions": {}, "permitted_actions": ["abort"],
                    "deadline": 1, "decision_actor": "director",
                    "current_revision": "r1", "current_sha256": "d" * 64,
                    "owner_epoch": 1})

            def get_workflow_handle(self, run_id):
                return self.child if run_id == "child-run" else self.parent

        director = harness.Director.__new__(harness.Director)
        director.identity = "director"
        director.incarnation = 1
        client = Client()

        async def configured_client():
            return client

        director.client = configured_client
        with self.assertRaises(harness.DirectorDecisionRejected) as rejected:
            asyncio.run(director._director_decision("run-1", "command-1", "r1",
                                                     "d" * 64, "abort", "director"))
        self.assertEqual(rejected.exception.safe_code,
                         "decision-deadline-unavailable-or-expired")

    def test_escalate_and_human_abort_use_pinned_actor_and_original_task(self):
        source = RuntimeObservationSource(self.director, {"name": "factory-test",
            "capability": {"id": "verified-research@1"}},
            self.instance / "runtime-source.sqlite3", history_reader=lambda: [],
            refresh_interval_seconds=0)
        escalate = {"command_id": "escalate-1", "task_id": "task-1",
                    "context_id": "context-1", "action": "escalate",
                    "expected_state": "input-required", "expected_revision": "r1",
                    "expected_sha256": "d" * 64}
        receipt = source.submit_command("fixture-operator", self.director.identity, escalate)
        self.assertEqual(receipt["lifecycle"], "received")
        self.assertEqual(self.director.perform_calls[-1][0]["op"], "escalate")
        self.assertEqual(self.director.perform_calls[-1][1:], ("task-1", "context-1"))

        self.director.wait_view = {"phase": "awaiting-human", "current_revision": "r1",
            "current_sha256": "d" * 64, "decision_actor": "fixture-observer",
            "permitted_actions": ["abort"],
            "applied_decisions": {"escalate-1": "escalate-recorded"}}
        # Simulate Temporal accepting escalation before the Runtime appended
        # its terminal source event. Retry the exact original owner command.
        with source._connect() as db:
            db.execute("DELETE FROM source_records WHERE source_id=?",
                       ("command:escalate-1:applied",))
        replay = source.submit_command("fixture-operator", self.director.identity, escalate)
        self.assertEqual(replay["lifecycle"], "received")
        self.assertEqual(self.director.perform_calls[-1][0]["op"], "escalate")
        human_abort = {"command_id": "human-abort-1", "task_id": "task-1",
                       "context_id": "context-1", "action": "abort",
                       "expected_state": "input-required", "expected_revision": "r1",
                       "expected_sha256": "d" * 64}
        receipt = source.submit_command("fixture-observer", self.director.identity, human_abort)
        self.assertEqual(receipt["lifecycle"], "received")
        self.assertEqual(self.director.perform_calls[-1][0]["op"], "abort")
        self.assertEqual(self.director.perform_calls[-1][1:], ("task-1", "context-1"))

        before = len(self.director.perform_calls)
        # A distinct authenticated principal cannot act as the pinned human.
        rejected = source.submit_command("fixture-operator", self.director.identity,
            {**human_abort, "command_id": "wrong-human-1"})
        self.assertEqual(rejected["lifecycle"], "received")
        self.assertEqual(len(self.director.perform_calls), before)

    def test_closed_bound_run_does_not_query_drained_temporal_workflow(self):
        director = object.__new__(harness.Director)
        director.task_binding = lambda task_id: (
            ("closed-run", "original-context") if task_id == "original-task" else None)
        director.run_record = lambda run_id: {"run_id": run_id, "closed": 1}
        director.client = lambda: self.fail("closed run must not issue a Temporal query")

        view = director.inspect_bound_run("original-task")

        self.assertEqual(view["run_id"], "closed-run")
        self.assertIsNone(view["phase"])
        self.assertIsNone(view["node"])
        self.assertIsNone(view["wait_started_at"])
        self.assertEqual(view["permitted_actions"], [])

    def test_parent_and_child_temporal_payloads_are_token_free(self):
        history, _, _ = self._history()
        started = history[0]["attributes"]["input"]
        parent = {"run_id": self.run_id, "task_id": "task-1", "context_id": "context-1",
                  "package_digest": "b" * 64,
                  "run_inputs_json": "{}", "run_inputs_digest": "c" * 64,
                  "authorized_actor": "fixture-operator", "input_authority_json": "{}"}
        publication = {"closure": started["closure"]}
        package = {"root": started["document"]}
        parent_payload = harness.build_workflow_input(self.director.identity, 1,
            parent, package, publication, 40)
        child_payload = nested_workflow_input(parent_payload, "run-1:child:abc",
            "f" * 64, {"nodes": {"done": {"type": "complete", "next": []}}})
        self.assertEqual(parent_payload["director"], {"identity": self.director.identity,
                                                       "epoch": 1})
        self.assertEqual(child_payload["director"], parent_payload["director"])
        self.assertEqual((parent_payload["task_id"], parent_payload["context_id"]),
                         ("task-1", "context-1"))
        self.assertEqual((child_payload["task_id"], child_payload["context_id"]),
                         ("task-1", "context-1"))
        for payload in (parent_payload, child_payload):
            encoded = json.dumps(payload, sort_keys=True)
            self.assertNotIn("token", encoded.lower())
            self.assertNotIn("Bearer fixture-token", encoded)

    def test_legacy_nested_workflow_binding_stays_absent_and_partial_binding_fails(self):
        parent = {"director": {"identity": "director-1", "epoch": 2},
                  "package_digest": "b" * 64, "package": {}, "closure": {},
                  "run_inputs": {}, "run_inputs_digest": "c" * 64,
                  "authorized_actor": "fixture-operator", "input_authority": {},
                  "wait_seconds": 40}
        child = nested_workflow_input(parent, "child-run", "d" * 64, {})
        self.assertNotIn("task_id", child)
        self.assertNotIn("context_id", child)
        parent["task_id"] = "task-only"
        with self.assertRaisesRegex(ValueError, "incomplete"):
            nested_workflow_input(parent, "child-run", "d" * 64, {})

    def test_public_commercial_reader_projects_durable_attributed_facts(self):
        history, _, _ = self._history()  # active publication and original run binding
        scale = 4
        recorded_at = "2026-10-02T12:01:00Z"
        purchase = {
            "purchase_id": "purchase-1", "run_id": "run-1", "task_id": "task-1",
            "assignment_id": "assignment-7", "attempt_id": "2",
            "service_identity": "research-worker", "offer_digest": "f" * 64,
            "price_basis": "usage", "payment_trigger": "acceptance",
            "payment_state": "settled",
            "currency": "USD", "atomic_scale": scale,
            "usage": [
                {"usage_id": "usage-known-zero", "unit": "tokens", "quantity": 0,
                 "evidence_state": "measured", "source": "provider_meter",
                 "service_identity": "research-worker", "assignment_id": "assignment-7",
                 "attempt_id": "2", "task_id": "task-1", "model_call_id": "call-9",
                 "recorded_at": recorded_at},
                {"usage_id": "usage-unreported", "unit": "tokens", "quantity": None,
                 "evidence_state": "unknown", "source": "provider_meter",
                 "service_identity": "research-worker", "assignment_id": "assignment-7",
                 "attempt_id": "2", "task_id": "task-1", "model_call_id": None,
                 "recorded_at": "2026-10-02T12:01:01Z"},
            ],
            "obligation": {"obligation_id": "obligation-1",
                "created_at": "2026-10-02T12:01:02Z",
                "details": {
                    "inference_cost": {"amount_units": 0, "currency": "USD",
                        "atomic_scale": scale,
                        "evidence": "calculated_from_measured_usage", "basis": {}},
                    "hosting_cost": {"amount_units": None, "currency": "USD",
                        "atomic_scale": scale, "evidence": "unknown", "basis": {}},
                    "supplier_charge": {"amount_units": 1250, "currency": "USD",
                        "atomic_scale": scale,
                        "evidence": "calculated_from_measured_usage", "basis": {}},
                }},
            "economics": {},
            "settlements": [{"settlement_id": "settlement-1", "direction": "charge",
                "amount_units": 1250, "currency": "USD", "atomic_scale": scale,
                "state": "settled", "evidence_state": "provider_reported",
                "recorded_at": "2026-10-02T12:01:03Z", "receipt_ref": "private receipt text"}],
            "credits": [{"credit_id": "credit-1", "amount_units": 25,
                "currency": "USD", "atomic_scale": scale,
                "evidence_state": "calculated_from_measured_usage",
                "recorded_at": "2026-10-02T12:01:04Z", "reason": "private credit reason"}],
        }
        reader = FakePublicCommercialLedger([purchase])
        source = RuntimeObservationSource(self.director, {"name": "factory-test",
            "capability": {"id": "verified-research@1"}},
            self.instance / "runtime-source.sqlite3",
            history_reader=lambda: [(self.run_id, history)],
            commercial_reader=reader, refresh_interval_seconds=0)
        projection = FactoryObservation(source, self.instance / "observation.sqlite3")

        snapshot = projection.snapshot("fixture-operator", self.director.identity)
        commercial = snapshot["state"]["commercial"]
        usage = {row["usage_id"]: row for row in commercial["usage"]}
        self.assertEqual(usage["usage-known-zero"]["quantity"], "0")
        self.assertEqual(usage["usage-known-zero"]["service_identity"], "research-worker")
        self.assertEqual(usage["usage-known-zero"]["model_call_id"], "call-9")
        self.assertEqual(usage["usage-known-zero"]["run_id"], "run-1")
        self.assertEqual(usage["usage-known-zero"]["task_id"], "task-1")
        self.assertEqual(usage["usage-known-zero"]["context_id"], "context-1")
        self.assertEqual(usage["usage-known-zero"]["assignment_id"], "assignment-7")
        self.assertEqual(usage["usage-known-zero"]["attempt_id"], "2")
        self.assertEqual(usage["usage-known-zero"]["completeness"], "complete")
        self.assertEqual(usage["usage-known-zero"]["evidence_status"], "measured")
        self.assertNotIn("quantity", usage["usage-unreported"])
        self.assertEqual(usage["usage-unreported"]["completeness"], "unknown")
        self.assertEqual(usage["usage-unreported"]["evidence_status"], "unknown")

        costs = {row["component"]: row for row in commercial["obligations"]}
        self.assertEqual(len({row["obligation_id"] for row in costs.values()}), len(costs))
        self.assertEqual(costs["inference_cost"]["amount_atoms"], 0)
        self.assertEqual(costs["inference_cost"]["atomic_scale"], scale)
        self.assertEqual(costs["inference_cost"]["evidence_status"],
                         "calculated_from_measured_usage")
        self.assertEqual(costs["inference_cost"]["assignment_id"], "assignment-7")
        self.assertEqual(costs["inference_cost"]["attempt_id"], "2")
        self.assertEqual(costs["inference_cost"]["run_id"], "run-1")
        self.assertEqual(costs["hosting_cost"]["amount_atoms"], None)
        self.assertEqual(costs["hosting_cost"]["evidence_status"], "unknown")
        self.assertEqual(costs["supplier_charge"]["amount_atoms"], 1250)
        self.assertEqual(costs["supplier_charge"]["currency"], "USD")
        self.assertEqual(costs["supplier_charge"]["state"], "settled")

        payments = {row["payment_id"]: row for row in commercial["payments"]}
        self.assertEqual(payments["settlement-1"]["amount_atoms"], 1250)
        self.assertEqual(payments["settlement-1"]["atomic_scale"], scale)
        self.assertEqual(payments["credit-1"]["state"], "credited")
        self.assertEqual(payments["credit-1"]["evidence_status"],
                         "calculated_from_measured_usage")
        with source._connect() as db:
            persisted = {row["source_id"]: json.loads(row["record_json"])
                         for row in db.execute(
                             "SELECT source_id,record_json FROM source_records")
                         if row["source_id"].startswith(
                             ("usage:", "obligation:", "payment:", "credit:"))}
        self.assertEqual(persisted["usage:usage-known-zero"]["time"], recorded_at)
        self.assertEqual(persisted["usage:usage-known-zero"]["fields"]["evidence_status"],
                         "measured")
        self.assertEqual(persisted["usage:usage-known-zero"]["fields"]["completeness"],
                         "complete")
        self.assertNotIn("quantity", persisted["usage:usage-unreported"]["fields"])
        self.assertEqual(persisted["usage:usage-unreported"]["fields"]["evidence_status"],
                         "unknown")
        self.assertEqual(persisted[
            "obligation:obligation-1:supplier_charge"]["time"],
            "2026-10-02T12:01:02Z")
        self.assertEqual(persisted[
            "obligation:obligation-1:supplier_charge"]["fields"]["obligation_id"],
            "obligation-1:supplier_charge")
        settlement_record = next(value for key, value in persisted.items()
                                 if key.startswith("payment:settlement-1:"))
        self.assertEqual(settlement_record["time"], "2026-10-02T12:01:03Z")
        self.assertEqual(persisted["credit:credit-1"]["time"], "2026-10-02T12:01:04Z")
        encoded = json.dumps(snapshot)
        self.assertNotIn("private receipt text", encoded)
        self.assertNotIn("private credit reason", encoded)
        self.assertEqual(source.commercial_reporting_status, "reported")
        self.assertFalse(source.commercial_costs_known)

        # Re-reading the same durable rows is idempotent. A later settlement
        # record revision gets a distinct source identity and remains visible.
        with source._connect() as db:
            before = db.execute("SELECT COUNT(*) FROM source_records").fetchone()[0]
        projection.snapshot("fixture-operator", self.director.identity)
        with source._connect() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM source_records").fetchone()[0],
                             before)
        purchase["settlements"][0]["state"] = "failed"
        purchase["settlements"][0]["recorded_at"] = "2026-10-02T12:01:05Z"
        updated = projection.snapshot("fixture-operator", self.director.identity)
        settlement_history = [row for row in updated["state"]["commercial"]["payments"]
                              if row["payment_id"] == "settlement-1"]
        self.assertEqual([row["state"] for row in settlement_history], ["settled", "failed"])
        self.assertEqual(reader.calls, ["run-1", "run-1:child:abc"] * 3)

    def test_empty_commercial_reader_is_unreported_not_zero(self):
        self._history()  # supplies the fake Director's active, pinned publication
        reader = FakePublicCommercialLedger()
        source = RuntimeObservationSource(self.director, {"name": "factory-test",
            "capability": {"id": "verified-research@1"}},
            self.instance / "runtime-source.sqlite3", history_reader=lambda: [],
            commercial_reader=reader, refresh_interval_seconds=0)
        projection = FactoryObservation(source, self.instance / "observation.sqlite3")
        snapshot = projection.snapshot("fixture-operator", self.director.identity)
        self.assertEqual(snapshot["state"]["commercial"], {
            "usage": [], "obligations": [], "payments": []})
        self.assertEqual(source.commercial_reporting_status, "unreported")
        self.assertFalse(source.commercial_costs_known)
        self.assertEqual(reader.calls, ["run-1"])


class RuntimeRoutesTests(unittest.TestCase):
    def test_pinned_usage_owner_uses_outer_descriptor_and_binding_digest_pins(self):
        identity = "pinned-service-1"
        descriptor = {
            "role": "capability", "name": "research_findings",
            "capability": "packet_findings@1", "reconcile": "a2a-idempotent-resend",
            "card_sha256": "a" * 64,
            "a2a_extension": {"uri": "urn:exomachina:a2a-action-contract:v1",
                "contract": "action-idempotent-async@1",
                "contract_digest": "b" * 64},
        }
        binding = {"approved": True, "identity": identity, "role": "capability",
                   "url": "http://127.0.0.1:47100"}
        manifest_service = {
            "binding_digest": harness.agent_contract_digest(binding),
            "contract_digest": harness.agent_contract_digest(descriptor),
        }
        publication = {
            "manifest_digest": "m" * 64, "package_digest": "p" * 64,
            "closure": {
                "manifest": {"services": {"research_findings": manifest_service}},
                "contracts": {"research_findings": descriptor},
            },
        }
        package = {"bindings": {"research_findings": binding}}
        module = type("Module", (), {})()
        module.home = Path(tempfile.mkdtemp(prefix="exo-pinned-reader-", dir="/tmp"))
        module.publications = type("Publications", (), {
            "get": lambda _self, _digest: publication})()
        module.package = lambda _digest: package
        director = type("DirectorScope", (), {"module": module})()
        scope = {"pinned": {"manifest_digest": publication["manifest_digest"],
                            "package_digest": publication["package_digest"]}}

        with patch.object(harness, "resolve_agent_binding",
                          return_value=(binding["url"], {"card_sha256": "a" * 64})) as resolver:
            owners, failures, non_usage = harness._pinned_usage_owners(director, scope)
        self.assertEqual(failures, 0)
        self.assertEqual(non_usage, 0)
        self.assertEqual(owners, [{"name": "research_findings", "identity": identity,
                                   "url": binding["url"]}])
        resolver.assert_called_once_with(
            module.home / "testbed" / "agent_snapshot.json", identity, descriptor)

        # Approval and role remain explicit checks even when the package's
        # binding digest is internally consistent.
        unapproved = {**binding, "approved": False}
        package["bindings"] = {"research_findings": unapproved}
        publication["closure"]["manifest"]["services"]["research_findings"] = {
            "binding_digest": harness.agent_contract_digest(unapproved),
            "contract_digest": harness.agent_contract_digest(descriptor),
        }
        owners, failures, non_usage = harness._pinned_usage_owners(director, scope)
        self.assertEqual(owners, [])
        self.assertEqual(failures, 1)
        self.assertEqual(non_usage, 0)

        # A separately pinned receiver service has no model-agent extension and
        # is explicitly counted as outside token-measurement coverage.
        receiver_contract = {"role": "release", "name": "release",
                             "capability": "local_report_release@1"}
        receiver_binding = {"approved": True, "identity": "receiver-1",
                            "role": "release", "url": "http://127.0.0.1:47104"}
        package["bindings"] = {"release": receiver_binding}
        publication["closure"] = {
            "manifest": {"services": {"release": {
                "binding_digest": harness.agent_contract_digest(receiver_binding),
                "contract_digest": harness.agent_contract_digest(receiver_contract)}}},
            "contracts": {"release": receiver_contract},
        }
        owners, failures, non_usage = harness._pinned_usage_owners(director, scope)
        self.assertEqual(owners, [])
        self.assertEqual(failures, 0)
        self.assertEqual(non_usage, 1)

        module.publications.get = lambda _digest: {
            **publication, "manifest_digest": "wrong-manifest"}
        owners, failures, non_usage = harness._pinned_usage_owners(director, scope)
        self.assertEqual((owners, failures, non_usage), ([], 1, 0))
        module.publications.get = lambda _digest: publication
        module.package = lambda _digest: {"bindings": None}
        owners, failures, non_usage = harness._pinned_usage_owners(director, scope)
        self.assertEqual((owners, failures, non_usage), ([], 1, 0))

    def test_usage_filter_matches_remote_agent_task_not_original_factory_task(self):
        original_task, remote_task = "parent-task-1", "remote-agent-task-9"
        run_id, context_id, definition_digest = "parent-run-1", "context-parent-1", "d" * 64

        class DirectorScope:
            def task_binding(self, task_id):
                return (run_id, context_id) if task_id == original_task else None

            def run_record(self, selected_run):
                return {"run_id": selected_run, "task_id": original_task,
                        "context_id": context_id, "manifest_digest": "a" * 64,
                        "package_digest": "b" * 64}

        snapshot = {"state": {"runs": [{"id": run_id,
            "task": {"id": original_task, "context_id": context_id},
            "pinned": {"manifest_digest": "a" * 64,
                       "package_digest": "b" * 64,
                       "definition_digest": definition_digest}}]}}
        filters = {"run_id": None, "task_id": remote_task, "assignment_id": None,
                   "attempt_id": None, "model_call_id": None, "call_scope": None}
        scopes = harness._usage_scopes(snapshot, DirectorScope(), filters)
        self.assertEqual(len(scopes), 1)
        self.assertEqual(scopes[0]["task_id"], original_task)

        usage = {name: {"value": 1 if name == "input_tokens" else None,
                        "status": "reported" if name == "input_tokens" else "unavailable"}
                 for name in harness.USAGE_CATEGORIES}
        service_row = {"measurement_id": "measurement-remote-9",
            "model_call_id": "call-remote-9", "recorded_at": "2026-10-03T16:00:00Z",
            "call_scope": "assignment_call", "provider": "codex-subscription",
            "model_id": "gpt-6-luna", "unit": "tokens",
            "measurement_source": "provider_reported", "completeness": "partial",
            "evidence_status": "provider_reported", "usage": usage,
            "service_identity": "research-worker", "task_id": remote_task,
            "run_id": run_id, "definition_digest": definition_digest,
            "assignment_id": "assignment-1", "attempt_id": "1"}
        class Response:
            def __enter__(self):
                return self
            def __exit__(self, *_args):
                return False
            def read(self):
                return json.dumps({"measurements": [service_row,
                    {**service_row, "measurement_id": "measurement-wrong-definition",
                     "definition_digest": "e" * 64},
                    {**service_row, "measurement_id": "measurement-wrong-service",
                     "service_identity": "other-service"}]}).encode()

        seen_request = {}
        def open_request(request, timeout):
            seen_request["url"] = request.full_url
            seen_request["timeout"] = timeout
            return Response()

        with patch.object(harness, "_pinned_usage_owners",
                          return_value=([{"name": "research", "identity": "research-worker",
                                          "url": "http://127.0.0.1:45871"}], 0, 0)), \
                patch.object(harness.urllib.request, "urlopen", side_effect=open_request):
            service_rows, report = harness._pinned_service_measurements(
                DirectorScope(), scopes, filters)
        self.assertEqual(len(service_rows), 1)
        self.assertEqual(service_rows[0]["task_id"], remote_task)
        self.assertEqual(report["rows_rejected"], 2)
        params = parse_qs(urlsplit(seen_request["url"]).query)
        self.assertEqual(params, {"run_id": [run_id], "task_id": [remote_task]})

        class PublicJournal:
            def __init__(self):
                self.queries = []
            def list_measurements(self, **query):
                self.queries.append(query)
                return [{**service_row, "measurement_id": "director-measurement",
                         "call_scope": "director_call"}]
        journal = PublicJournal()
        director_rows, director_report = harness._director_measurements(
            journal, scopes, filters)
        self.assertEqual(len(director_rows), 1)
        self.assertEqual(director_rows[0]["task_id"], remote_task)
        self.assertEqual(journal.queries[0]["run_id"], run_id)
        self.assertNotIn("task_id", journal.queries[0])
        self.assertEqual(director_report["queries_failed"], 0)

    def test_factory_harness_uses_shared_empty_public_ledger_without_charging(self):
        state = Path(tempfile.mkdtemp(prefix="exo-runtime-ledger-home-", dir="/tmp"))
        instance = state / "instances" / "factory"

        class StubRunner:
            def __init__(self, *args, **kwargs):
                self.address = "127.0.0.1:7233"

            def is_running(self):
                return False

        with patch.object(harness, "Runner", StubRunner):
            harness.init_instance(instance, name="ledger-route-test", mode="factory",
                                  port=47870, home=state)
        original_source = harness.RuntimeObservationSource
        captured = {}

        class EmptyJournal:
            def list_measurements(self, **_filters):
                return []

        class EmptyUsageBroker:
            def usage_journal(self):
                return EmptyJournal()

        def capture_source(*args, **kwargs):
            source = original_source(*args, **kwargs)
            captured["source"] = source
            return source

        with patch.object(harness, "Runner", StubRunner), \
                patch.object(harness, "RuntimeObservationSource", capture_source):
            app = harness.create_app(instance, usage_broker=EmptyUsageBroker())

        self.assertIsNotNone(app)
        source = captured["source"]
        ledger = source.commercial_reader
        self.assertIsInstance(ledger, CommercialLedger)
        self.assertEqual(ledger.path.resolve(), (state / "commercial.sqlite3").resolve())
        calls = []
        public_list = ledger.list_purchases

        def observed_public_list(*, run_id=None):
            calls.append(run_id)
            return public_list(run_id=run_id)

        ledger.list_purchases = observed_public_list
        source._commercial_records({"run-live-test": ("task-live-test", "context-live-test")})
        self.assertEqual(calls, ["run-live-test"])
        self.assertEqual(source.commercial_reporting_status, "unreported")
        self.assertFalse(source.commercial_costs_known)

        tables = ("offers", "budgets", "authorizations", "purchases", "reservations",
                  "usage_records", "obligations", "settlements", "credits", "economic_facts",
                  "completion_keys")
        with sqlite3.connect(ledger.path) as db:
            rows = {table: db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    for table in tables}
        self.assertEqual(rows, {table: 0 for table in tables})
        with TestClient(app) as client:
            self.assertEqual(client.get("/discover").status_code, 401)
            self.assertEqual(client.get("/qa/login").status_code, 401)
            self.assertEqual(client.get("/deliveries").status_code, 401)
            auth = {"Authorization": harness.TOKEN}
            self.assertEqual(client.get("/deliveries", headers=auth).json()["status"],
                             "unavailable")
            self.assertEqual(client.post("/deliveries", headers={
                **auth}, json={}).status_code, 503)
            usage = client.get("/usage/measurements", headers=auth).json()
            self.assertEqual(usage["measurements"], [])
            self.assertEqual(usage["coverage"]["director"]["status"], "available")
            self.assertEqual(usage["coverage"]["director_unbound"]["status"], "unavailable")
            self.assertEqual(usage["coverage"]["commercial_costs"], "not_included")
            self.assertFalse(any(getattr(route, "path", None) == "/qa/login"
                                 for route in app.routes))
            self.assertFalse(any(getattr(route, "path", None) == "/qa/session"
                                 for route in app.routes))
            self.assertEqual(client.get("/discover", headers={
                "Authorization": harness.OBSERVER_TOKEN}).status_code, 200)
            self.assertIn("fixtureBearer", client.get("/.well-known/agent-card.json").text)
            self.assertNotIn("loopbackSession", client.get("/.well-known/agent-card.json").text)
            default_floor = client.get("/floor")
            self.assertEqual(default_floor.status_code, 200)
            self.assertNotIn("window.EXO_DASHBOARD_BOOTSTRAP={", default_floor.text)
            readiness = client.get("/health").json()["readiness"]
            self.assertEqual(readiness["observation_auth_status"], "fixture_only")
            self.assertEqual(readiness["browser_auth_resolver_status"], "not_selected")
            self.assertIn(readiness["submission_status"], {"ready", "blocked"})
            self.assertIsInstance(readiness["submission_ready"], bool)
            self.assertNotIn("author_provider_configured", readiness)
        self.assertEqual(readiness["commercial_reporting_status"], "unreported")
        self.assertIs(readiness["commercial_costs_known"], False)
        self.assertEqual(readiness["payment_adapter_status"], "unconfigured")

    def test_operations_mount_scopes_server_principal_without_snapshot_recursion(self):
        state = Path(tempfile.mkdtemp(prefix="exo-runtime-operations-mount-", dir="/tmp"))
        instance = state / "instances" / "factory"

        class StubRunner:
            def __init__(self, *args, **kwargs):
                self.address = "127.0.0.1:7233"

            def is_running(self):
                return False

        class EmptyJournal:
            def list_measurements(self, **_filters):
                return []

        class EmptyUsageBroker:
            def usage_journal(self):
                return EmptyJournal()

        captured = {}
        reader_calls = []
        original_source = harness.RuntimeObservationSource
        original_installer = harness.install_runtime_operations
        original_projector = harness.project_operations_incidents

        def capture_source(director, config, database, **kwargs):
            kwargs["history_reader"] = lambda: []
            kwargs["refresh_interval_seconds"] = 0
            source = original_source(director, config, database, **kwargs)
            captured["source"] = source
            return source

        def capture_installer(*args, **kwargs):
            operations = original_installer(*args, **kwargs)
            captured["operations"] = operations
            return operations

        def capture_projection(operations, principal, factory_id, *, limit):
            reader_calls.append((principal, factory_id, limit))
            return original_projector(operations, principal, factory_id, limit=limit)

        with patch.object(harness, "Runner", StubRunner):
            harness.init_instance(instance, name="Operations mount test", mode="factory",
                                  port=47871, home=state,
                                  operations_max_list_limit=128)
        with patch.object(harness, "Runner", StubRunner), \
                patch.object(harness, "RuntimeObservationSource", capture_source), \
                patch.object(harness, "install_runtime_operations", capture_installer), \
                patch.object(harness, "project_operations_incidents", capture_projection):
            app = harness.create_app(instance, usage_broker=EmptyUsageBroker())

        auth = {"Authorization": harness.TOKEN}
        body_for = lambda run_id: {
            "run_id": run_id, "subject_kind": "run", "subject_id": run_id,
            "failure_class": "worker_timeout", "generation": 1, "evidence_refs": [],
        }
        with TestClient(app) as client:
            self.assertEqual(client.get("/incidents?limit=128").status_code, 401)
            self.assertEqual(client.get("/incidents?limit=128", headers=auth).json(),
                             {"incidents": []})
            capabilities = client.get("/operations/capabilities", headers=auth)
            self.assertEqual(capabilities.status_code, 200)
            self.assertTrue(capabilities.json()["incidents"]["available"])
            self.assertFalse(capabilities.json()["recovery"]["available"])
            self.assertEqual(client.post("/incidents", headers=auth,
                json=body_for("unknown-run-not-owned")).status_code, 404)
            self.assertEqual(client.post("/incidents", headers=auth,
                json=body_for("foreign-factory-run")).status_code, 404)
            with client.websocket_connect("/observations", headers=auth) as websocket:
                websocket.send_json({"op": "subscribe",
                    "factory_id": captured["operations"].factory_id})
                frame = websocket.receive_json()
                self.assertEqual(frame["op"], "snapshot")
                self.assertEqual(frame["snapshot"]["state"]["runs"], [])

        self.assertIsNotNone(captured["source"].operations_reader)
        # Force the mounted source reader through the server-resolved principal.
        # Its list-only path must not recurse through public_run_snapshot.
        captured["operations"].adapter.public_run_snapshot = lambda *_args, **_kwargs: (
            self.fail("incident source reader must not request a run snapshot"))
        with patch.object(harness, "project_operations_incidents", capture_projection):
            captured["source"].read_page(harness.TOKEN_ACTOR,
                captured["operations"].factory_id, None, limit=64)
        self.assertTrue(reader_calls)
        self.assertTrue(all(call[0] == harness.TOKEN_ACTOR for call in reader_calls))
        self.assertTrue(all(call[1] == captured["operations"].factory_id
                            and call[2] == 128 for call in reader_calls))
        self.assertEqual(captured["operations"].list_incidents(
            harness.TOKEN_ACTOR, limit=128), [])

    def test_discovery_floor_and_dashboard_modules_share_harness(self):
        state = Path(tempfile.mkdtemp(prefix="exo-runtime-routes-", dir="/tmp"))
        instance = state / "instances" / "factory"

        class StubRunner:
            def __init__(self, *args, **kwargs):
                self.address = "127.0.0.1:7233"

            def is_running(self):
                return False

        with patch.object(harness, "Runner", StubRunner):
            harness.init_instance(instance, name="route-test", mode="factory",
                                  port=47869, home=state)
            config_path = instance / "instance.json"
            config = json.loads(config_path.read_text())
            config["loopback_qa_session"] = True
            config_path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n")
            app = harness.create_app(instance,
                                      commercial_reader=FakePublicCommercialLedger())
        with TestClient(app, base_url="http://127.0.0.1:47869",
                         client=("127.0.0.1", 51234)) as client:
            self.assertEqual(client.get("/discover").status_code, 401)
            self.assertEqual(client.get("/discover", headers={
                "Authorization": "Bearer fixture-observer"}).status_code, 401)
            no_session_floor = client.get("/floor", follow_redirects=False)
            self.assertEqual(no_session_floor.status_code, 303)
            self.assertEqual(no_session_floor.headers["location"], "/qa/login")
            self.assertIn("no-store", no_session_floor.headers["cache-control"])
            self.assertEqual(client.get("/submission/readiness").status_code, 401)
            client.cookies.set(harness.QA_SESSION_COOKIE, "expired-or-invalid-session")
            expired_floor = client.get("/floor", follow_redirects=False)
            self.assertEqual(expired_floor.status_code, 303)
            self.assertEqual(expired_floor.headers["location"], "/qa/login")
            self.assertIn("no-store", expired_floor.headers["cache-control"])
            client.cookies.clear()
            login = client.get("/qa/login")
            self.assertEqual(login.status_code, 200)
            self.assertIn("no-store", login.headers["cache-control"])
            session = client.post("/qa/session", headers={
                "Origin": "http://127.0.0.1:47869"}, follow_redirects=False)
            self.assertEqual(session.status_code, 303)
            self.assertIn("no-store", session.headers["cache-control"])
            self.assertIn("httponly", session.headers["set-cookie"].lower())
            self.assertIn("samesite=strict", session.headers["set-cookie"].lower())
            for unsafe_headers in (
                    {"Origin": "http://evil.example"},
                    {"Origin": "http://127.0.0.1:47869", "Forwarded": "for=127.0.0.1"},
                    {"Origin": "http://127.0.0.1:47869",
                     "X-Forwarded-For": "127.0.0.1"}):
                rejected = client.post("/qa/session", headers=unsafe_headers,
                                       follow_redirects=False)
                self.assertEqual(rejected.status_code, 403)
            discovered = client.get("/discover").json()
            self.assertEqual(discovered["schema_version"], 1)
            factory = discovered["factories"][0]
            self.assertEqual(set(factory), {"id", "name", "factory_id"})
            self.assertEqual(factory["id"], factory["factory_id"])
            self.assertEqual(factory["name"], "route-test")
            self.assertNotIn("token", json.dumps(discovered).lower())
            floor = client.get("/floor")
            self.assertEqual(floor.status_code, 200)
            self.assertIn("no-store", floor.headers["cache-control"])
            self.assertIn('"authorizedSession":true', floor.text)
            self.assertIn('"usageEndpoint":"/usage/measurements"', floor.text)
            self.assertIn('"deliveryEndpoint":"/deliveries"', floor.text)
            self.assertIn('"qaSessionLogin":"/qa/login"', floor.text)
            self.assertIn('"submissionReadinessEndpoint":"/submission/readiness"', floor.text)
            self.assertNotIn("Bearer fixture-", floor.text)
            import_prefix = "from '/dashboard-assets/"
            self.assertIn(import_prefix, floor.text)
            asset_version = floor.text.split(import_prefix, 1)[1].split("/", 1)[0]
            self.assertEqual(len(asset_version), 64)
            self.assertTrue(all(char in "0123456789abcdef" for char in asset_version))
            self.assertEqual(asset_version,
                             harness._dashboard_js_content_digest(harness.DASHBOARD_ASSETS))
            self.assertNotIn("from '/dashboard-assets/contract.mjs'", floor.text)
            module = client.get("/dashboard-assets/contract.mjs")
            self.assertEqual(module.status_code, 200)
            self.assertNotIn("Bearer fixture-", module.text)
            versioned_module = client.get(
                f"/dashboard-assets/{asset_version}/contract.mjs")
            self.assertEqual(versioned_module.status_code, 200)
            self.assertEqual(versioned_module.content, module.content)

            query_response = client.get("/submission/readiness?factory_id=foreign")
            self.assertEqual(query_response.status_code, 400)

            def check_submission_readiness(readiness_value, job_value, unfinished_value,
                                           expected_reason):
                with patch.object(harness, "_submission_readiness",
                                  return_value=readiness_value) as readiness_reader, \
                        patch.object(harness.Director, "basic_job_status",
                                     return_value=job_value), \
                        patch.object(harness.Director, "unfinished",
                                     return_value=unfinished_value):
                    response = client.get("/submission/readiness")
                self.assertEqual(response.status_code, 200)
                payload = response.json()
                self.assertEqual(set(payload), {
                    "schema_version", "factory_id", "observed_at", "status", "reason_code"})
                self.assertEqual(payload["schema_version"], 1)
                self.assertEqual(payload["factory_id"], factory["id"])
                self.assertTrue(payload["observed_at"])
                self.assertEqual(payload["status"],
                                 "ready" if expected_reason is None else "blocked")
                self.assertEqual(payload["reason_code"], expected_reason)
                self.assertIn("no-store", response.headers["cache-control"])
                readiness_reader.assert_called_once()
                return response

            idle_job = {"configured": True, "busy": False, "state": "available"}
            ready_runtime = {
                "submission_status": "ready", "submission_ready": True,
                "submission_blockers": [], "verification_status": "not_checked",
                "live_inference_ready": False,
            }
            check_submission_readiness(ready_runtime, idle_job, [], None)
            check_submission_readiness(
                {"submission_status": "blocked", "submission_ready": False,
                 "submission_blockers": ["subscription_status_unavailable"],
                 "verification_status": "not_checked", "live_inference_ready": False},
                idle_job, [], "subscription_status_unavailable")
            check_submission_readiness(
                ready_runtime,
                {"configured": True, "busy": True, "state": "active"},
                [], "factory_busy")
            check_submission_readiness(
                ready_runtime,
                {"configured": True, "busy": True, "state": "uncertain"},
                [], "factory_uncertain")
            check_submission_readiness(
                ready_runtime,
                {"configured": True, "busy": None, "state": "available"},
                [], "current_state_unavailable")
            check_submission_readiness(
                ready_runtime,
                {"configured": True, "busy": False, "state": "unknown"},
                [], "current_state_unavailable")
            hidden_unfinished_id = "must-not-appear-in-response"
            unfinished_response = check_submission_readiness(
                ready_runtime, idle_job, [hidden_unfinished_id], "unfinished_runs")
            self.assertNotIn(hidden_unfinished_id, unfinished_response.text)
            check_submission_readiness(
                ready_runtime, idle_job, None, "current_state_unavailable")

            health = client.get("/health").json()["readiness"]
            self.assertEqual(health["verification_status"], "not_checked")
            self.assertIs(health["live_inference_ready"], False)
            self.assertIn(health["submission_status"], {"ready", "blocked"})
            self.assertIsInstance(health["submission_ready"], bool)
            self.assertNotIn("author_provider_configured", health)
            self.assertEqual(client.get("/health").json()["basic_job"],
                             {"configured": False, "busy": False})
            self.assertEqual(health["observation_auth_status"], "loopback_only_session")
            self.assertEqual(health["browser_auth_resolver_status"],
                             "loopback_session_adapter")
            self.assertIs(health["live_browser_authenticated"], False)
            self.assertEqual(health["commercial_ledger_status"], "unreported")
            self.assertEqual(health["commercial_reporting_status"], "unreported")
            self.assertEqual(health["assignment_ledger_writer_status"], "not_enabled")
            self.assertIn("spend authorization",
                          health["assignment_ledger_writer_reason"])
            self.assertEqual(health["payment_adapter_status"], "unconfigured")
            self.assertIs(health["commercial_costs_known"], False)
            card = client.get("/.well-known/agent-card.json").text
            self.assertIn("loopbackSession", card)
            self.assertNotIn("fixtureBearer", card)
            remote = TestClient(app, base_url="http://127.0.0.1:47869",
                                client=("192.0.2.9", 51235))
            try:
                wrong_host = TestClient(app, base_url="http://example.invalid",
                                        client=("127.0.0.1", 51236))
                try:
                    self.assertEqual(wrong_host.get("/qa/login").status_code, 403)
                    self.assertEqual(wrong_host.post("/qa/session", headers={
                        "Origin": "http://example.invalid"},
                        follow_redirects=False).status_code, 403)
                finally:
                    wrong_host.close()
                forwarded_session = remote.post("/qa/session", headers={
                    "Origin": "http://127.0.0.1:47869",
                    "X-Forwarded-For": "127.0.0.1"}, follow_redirects=False)
                self.assertEqual(forwarded_session.status_code, 403)
                remote_session = remote.post("/qa/session", headers={
                    "Origin": "http://127.0.0.1:47869"}, follow_redirects=False)
                self.assertEqual(remote_session.status_code, 403)
                self.assertEqual(remote.get("/floor", follow_redirects=False).status_code, 403)
                self.assertEqual(remote.get("/floor", headers={
                    "Forwarded": "for=127.0.0.1"}, follow_redirects=False).status_code, 403)
                remote.cookies.set(harness.QA_SESSION_COOKIE,
                                   client.cookies.get(harness.QA_SESSION_COOKIE))
                self.assertEqual(remote.get("/discover").status_code, 401)
                self.assertEqual(remote.get("/discover", headers={
                    "X-Forwarded-For": "127.0.0.1"}).status_code, 401)
            finally:
                remote.close()

    def test_expired_loopback_qa_session_is_removed_without_granting_actor(self):
        sessions = harness.LoopbackSessionAdapter()
        sessions.sessions["expired-session"] = (harness.QA_ACTOR, time.monotonic() - 1)
        actor = sessions.resolve(session_id="expired-session", client_host="127.0.0.1",
                                  host="127.0.0.1:47869", headers={})
        self.assertIsNone(actor)
        self.assertNotIn("expired-session", sessions.sessions)


if __name__ == "__main__":
    unittest.main()
