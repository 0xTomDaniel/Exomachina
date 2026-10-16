"""Synthetic public Operations-to-Runtime Observation integration; no live service evidence."""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from observation import FactoryObservation, SourceContractError  # noqa: E402
from observation_source import RuntimeObservationSource  # noqa: E402
from operations import FactoryOperations  # noqa: E402
from operations_observation import project_operations_incidents  # noqa: E402


FACTORY = "factory-operations-observation"
PRINCIPAL = "fixture-operator"
RUN = "run-observed"
CHILD_RUN = "wf-opaque-child-9ac2"
TASK = "task-original"
CONTEXT = "context-original"
MANIFEST = "a" * 64
PACKAGE = "b" * 64
DEFINITION = "c" * 64
EVIDENCE = "d" * 64
BUILD = "build-observation-test"
WRITTEN_AT = "2026-10-03T17:00:00.000Z"


class FakeDirector:
    """Only the existing owned Runtime run row and Task binding authority."""

    identity = FACTORY

    def __init__(self, instance: Path):
        self.database = instance / "director.sqlite3"
        self.database.parent.mkdir(parents=True, exist_ok=True)
        (instance / "instance.json").write_text("{}\n")
        catalog = instance / "catalog"
        catalog.mkdir()
        self.module = SimpleNamespace(
            publications=SimpleNamespace(active=lambda: None), catalog=catalog)
        with self.connect() as db:
            db.execute("CREATE TABLE runs (run_id TEXT, task_id TEXT, context_id TEXT, "
                       "package_digest TEXT, manifest_digest TEXT, build_id TEXT, label TEXT, "
                       "run_inputs_json TEXT, run_inputs_digest TEXT, authorized_actor TEXT, "
                       "input_authority_json TEXT, closed INTEGER, outcome_json TEXT)")
            db.execute("INSERT INTO runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                RUN, TASK, CONTEXT, PACKAGE, MANIFEST, BUILD, "v1", "{}", EVIDENCE,
                "actor:operator", "{}", 0, None))

    def connect(self):
        db = sqlite3.connect(self.database, timeout=5, isolation_level=None)
        db.row_factory = sqlite3.Row
        return db

    def task_binding(self, task_id):
        return (RUN, CONTEXT) if task_id == TASK else None


def _history(*, include_child: bool = False):
    start_input = {
        "closure": {
            "manifest_digest": MANIFEST,
            "manifest": {"root_digest": DEFINITION,
                         "interpreter": {"build_id": BUILD}},
        },
        "package_digest": PACKAGE,
        "definition_digest": DEFINITION,
        "document": {"nodes": {"start": {"type": "worker", "next": []}}},
    }
    root_events = [{"event_id": 1, "event_type": "WORKFLOW_EXECUTION_STARTED",
                    "time": WRITTEN_AT,
                    "attributes": {"input": start_input}}]
    histories = [(RUN, root_events)]
    if include_child:
        root_events.append({
            "event_id": 2, "event_type": "START_CHILD_WORKFLOW_EXECUTION_INITIATED",
            "time": "2026-10-03T17:00:01.000Z",
            "attributes": {"workflow_id": CHILD_RUN},
        })
        child_input = {
            **start_input,
            "run": CHILD_RUN,
            "closure": {
                "manifest_digest": "e" * 64,
                "manifest": {"root_digest": "f" * 64,
                             "interpreter": {"build_id": "build-child-test"}},
            },
            "definition_digest": "f" * 64,
            "document": {"nodes": {"child-entry": {"type": "worker", "next": []}}},
        }
        histories.append((CHILD_RUN, [{
            "event_id": 1, "event_type": "WORKFLOW_EXECUTION_STARTED",
            "time": "2026-10-03T17:00:02.000Z",
            "attributes": {"input": child_input},
        }]))
    return histories


def _incident_fact(*, factory_id=FACTORY, run_id=RUN, task_id=None, context_id=None):
    fact = {
        "source_kind": "incident", "source_id": "incident-known:v1",
        "factory_id": factory_id, "run_id": run_id, "time": WRITTEN_AT,
        "event_type": "com.exomachina.incident.state_changed.v1",
        "fields": {"incident_id": "incident-known", "kind": "worker_timeout",
                   "state": "claimed", "owner_identity": "actor:operator",
                   "evidence_refs": [EVIDENCE]},
    }
    if task_id is not None:
        fact["task_id"] = task_id
    if context_id is not None:
        fact["context_id"] = context_id
    return fact


class RuntimeOperationsObservationTests(unittest.TestCase):
    def test_public_writer_facts_insert_once_across_restart_and_claim_snapshot(self):
        with tempfile.TemporaryDirectory() as home:
            root = Path(home)
            director = FakeDirector(root / "factory")
            config = {"name": "Operations Observation Test",
                      "capability": {"id": "verified-research@1"}}
            source_db = root / "runtime-source.sqlite3"
            observation_db = root / "observation.sqlite3"
            operations_holder = {}
            reader_calls = []

            def incident_reader(principal, *, factory_id, limit):
                reader_calls.append((principal, factory_id, limit))
                operations = operations_holder.get("operations")
                if operations is None:
                    return []
                return project_operations_incidents(
                    operations, principal, factory_id, limit=limit)

            source = RuntimeObservationSource(
                director, config, source_db, history_reader=_history,
                operations_reader=incident_reader, refresh_interval_seconds=0)
            projection = FactoryObservation(source, observation_db)

            class PublicOperationsAdapter:
                def authorize(self, principal, factory_id, capability, resource_id):
                    if principal != PRINCIPAL or factory_id != FACTORY:
                        raise PermissionError("not authorized")
                    return "actor:operator"

                def public_run_snapshot(self, principal, factory_id, run_id):
                    return projection.snapshot(principal, factory_id, run_id=run_id)

            operations = FactoryOperations(
                PublicOperationsAdapter(), root / "operations.sqlite3",
                factory_id=FACTORY, allowed_recovery_actions=[],
                max_recovery_attempts=None)
            operations_holder["operations"] = operations

            reported = operations.report_incident(
                PRINCIPAL, run_id=RUN, subject_kind="run", subject_id=RUN,
                failure_class="worker_timeout", generation=1, evidence_refs=[EVIDENCE])
            opened = projection.snapshot(PRINCIPAL, FACTORY, run_id=RUN)
            self.assertEqual(opened["state"]["runs"][0]["incidents"][0]["state"], "open")

            acknowledged = operations.acknowledge_incident(
                PRINCIPAL, reported["id"], expected_version=reported["version"])
            acknowledged_snapshot = projection.snapshot(PRINCIPAL, FACTORY, run_id=RUN)
            self.assertEqual(
                acknowledged_snapshot["state"]["runs"][0]["incidents"][-1]["state"],
                "acknowledged")

            claimed = operations.claim_incident(
                PRINCIPAL, reported["id"], expected_version=acknowledged["version"])
            claimed_snapshot = projection.snapshot(PRINCIPAL, FACTORY, run_id=RUN)
            run = claimed_snapshot["state"]["runs"][0]
            self.assertEqual(run["task"], {"id": TASK, "context_id": CONTEXT})
            self.assertEqual(run["incidents"][-1]["state"], "claimed")
            self.assertEqual(run["incidents"][-1]["owner_identity"], "actor:operator")
            self.assertEqual(run["incidents"][-1]["task_id"], TASK)
            self.assertEqual(run["incidents"][-1]["context_id"], CONTEXT)

            with source._connect() as db:
                before_restart = {
                    row["source_id"]: row["record_json"]
                    for row in db.execute(
                        "SELECT source_id,record_json FROM source_records "
                        "WHERE source_id LIKE ? ORDER BY source_id",
                        (reported["id"] + ":v%",))
                }
            self.assertEqual(set(before_restart), {
                f"{reported['id']}:v1", f"{reported['id']}:v2", f"{reported['id']}:v3"})

            restarted_source = RuntimeObservationSource(
                director, config, source_db, history_reader=_history,
                operations_reader=incident_reader, refresh_interval_seconds=0)
            restarted_projection = FactoryObservation(restarted_source, observation_db)
            after_restart_snapshot = restarted_projection.snapshot(
                PRINCIPAL, FACTORY, run_id=RUN)
            after_run = after_restart_snapshot["state"]["runs"][0]
            self.assertEqual(after_run["incidents"][-1]["owner_identity"], "actor:operator")
            with restarted_source._connect() as db:
                after_restart = {
                    row["source_id"]: row["record_json"]
                    for row in db.execute(
                        "SELECT source_id,record_json FROM source_records "
                        "WHERE source_id LIKE ? ORDER BY source_id",
                        (reported["id"] + ":v%",))
                }

        self.assertEqual(after_restart, before_restart)
        self.assertEqual([call[2] for call in reader_calls], [128] * len(reader_calls))
        claimed_source = json.loads(before_restart[f"{reported['id']}:v3"])
        self.assertEqual(claimed_source["time"], claimed["updated_at"])
        self.assertEqual((claimed_source["task_id"], claimed_source["context_id"]),
                         (TASK, CONTEXT))
        for field in ("manifest_digest", "package_digest", "definition_digest", "interpreter_build"):
            self.assertNotIn(field, claimed_source)

    def test_foreign_or_unknown_incident_run_is_fail_closed(self):
        with tempfile.TemporaryDirectory() as home:
            root = Path(home)
            director = FakeDirector(root / "factory")
            config = {"name": "Operations Observation Test",
                      "capability": {"id": "verified-research@1"}}
            for fact, message in (
                (_incident_fact(run_id="run-not-owned"), "not an owned Runtime run"),
                (_incident_fact(factory_id="factory-foreign"), "different factory"),
                (_incident_fact(task_id="task-foreign"), "Task binding conflicts"),
                (_incident_fact(context_id="context-foreign"), "Task binding conflicts"),
            ):
                with self.subTest(fact=fact):
                    source = RuntimeObservationSource(
                        director, config, root / (message.split()[0] + ".sqlite3"),
                        operations_reader=lambda _principal, *, factory_id, limit, row=fact: [row])
                    with self.assertRaisesRegex(SourceContractError, message):
                        source._operations_records(PRINCIPAL, {RUN: (TASK, CONTEXT)})

    def test_temporal_child_start_fact_supplies_exact_incident_binding(self):
        with tempfile.TemporaryDirectory() as home:
            root = Path(home)
            director = FakeDirector(root / "factory")
            config = {"name": "Operations Observation Test",
                      "capability": {"id": "verified-research@1"}}
            operations_holder = {}

            def incident_reader(principal, *, factory_id, limit):
                operations = operations_holder.get("operations")
                if operations is None:
                    return []
                return project_operations_incidents(
                    operations, principal, factory_id, limit=limit)

            source = RuntimeObservationSource(
                director, config, root / "runtime-source.sqlite3",
                history_reader=lambda: _history(include_child=True),
                operations_reader=incident_reader, refresh_interval_seconds=0)
            projection = FactoryObservation(source, root / "observation.sqlite3")

            class PublicOperationsAdapter:
                def authorize(self, principal, factory_id, capability, resource_id):
                    if principal != PRINCIPAL or factory_id != FACTORY:
                        raise PermissionError("not authorized")
                    return "actor:operator"

                def public_run_snapshot(self, principal, factory_id, run_id):
                    return projection.snapshot(principal, factory_id, run_id=run_id)

            operations = FactoryOperations(
                PublicOperationsAdapter(), root / "operations.sqlite3",
                factory_id=FACTORY, allowed_recovery_actions=[],
                max_recovery_attempts=None)
            operations_holder["operations"] = operations

            incident = operations.report_incident(
                PRINCIPAL, run_id=CHILD_RUN, subject_kind="run", subject_id=CHILD_RUN,
                failure_class="worker_timeout", generation=1, evidence_refs=[EVIDENCE])
            acknowledged = operations.acknowledge_incident(
                PRINCIPAL, incident["id"], expected_version=incident["version"])
            claimed = operations.claim_incident(
                PRINCIPAL, incident["id"], expected_version=acknowledged["version"])
            snapshot = projection.snapshot(PRINCIPAL, FACTORY, run_id=CHILD_RUN)
            child = snapshot["state"]["runs"][0]

            with source._connect() as db:
                source_row = db.execute(
                    "SELECT record_json FROM source_records WHERE source_id=?",
                    (f"{incident['id']}:v{claimed['version']}",)).fetchone()
            self.assertIsNotNone(source_row)
            incident_source = json.loads(source_row["record_json"])
            self.assertEqual([row["run_id"] for row in source._run_rows()], [RUN])

        self.assertEqual(child["id"], CHILD_RUN)
        self.assertEqual(child["task"], {"id": TASK, "context_id": CONTEXT})
        self.assertEqual(child["incidents"][-1]["owner_identity"], "actor:operator")
        self.assertEqual(child["incidents"][-1]["task_id"], TASK)
        self.assertEqual(child["incidents"][-1]["context_id"], CONTEXT)
        self.assertEqual(incident_source["run_id"], CHILD_RUN)
        self.assertEqual(incident_source["task_id"], TASK)
        self.assertEqual(incident_source["context_id"], CONTEXT)
        self.assertEqual(incident_source["time"], claimed["updated_at"])
        self.assertNotIn("manifest_digest", incident_source)


if __name__ == "__main__":
    unittest.main()
