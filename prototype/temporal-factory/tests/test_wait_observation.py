"""Synthetic public Director wait projection tests; not live run evidence."""
from __future__ import annotations

import json
import math
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from observation import SourceContractError, _merge_run_state, project_source_record  # noqa: E402
from observation_source import RuntimeObservationSource  # noqa: E402


FACTORY = "factory-wait-projection"
ROOT_RUN = "run-wait-root"
CHILD_RUN = "wf-opaque-child-wait-7f21"
TASK = "task-original-wait"
CONTEXT = "context-original-wait"
WAIT_STARTED = "2026-10-03T18:10:00.000Z"
WAIT_DEADLINE = "2026-10-03T18:20:00.000Z"
ARTIFACT_SHA = "a" * 64


def director_wait_view(**overrides):
    return {
        "run_id": ROOT_RUN,
        "phase": "awaiting-director",
        "decision_actor": "director-agent",
        "wait_started_at": WAIT_STARTED,
        "wait_deadline": WAIT_DEADLINE,
        "permitted_actions": ["abort", "escalate"],
        "node": "approval-gate",
        "current_revision": "r4",
        "current_sha256": ARTIFACT_SHA,
        # These private query details must never cross the event projection.
        "quality_findings": ["private finding"],
        "raw_context": "private context",
        "recommendation": "private recommendation",
        **overrides,
    }


class FakeDirector:
    identity = FACTORY

    def __init__(self, home: Path, view: dict):
        self.database = home / "factory" / "director.sqlite3"
        self.database.parent.mkdir(parents=True)
        (self.database.parent / "instance.json").write_text("{}\n")
        self.view = view
        with self.connect() as db:
            db.execute("CREATE TABLE runs (run_id TEXT, task_id TEXT, context_id TEXT)")
            db.execute("INSERT INTO runs VALUES (?,?,?)", (ROOT_RUN, TASK, CONTEXT))

    def connect(self):
        db = sqlite3.connect(self.database, timeout=5, isolation_level=None)
        db.row_factory = sqlite3.Row
        return db

    def inspect_bound_run(self, task_id):
        if task_id != TASK:
            raise AssertionError("wait inspection must use the original Task ID")
        return dict(self.view)


def open_source(home: Path, director: FakeDirector) -> RuntimeObservationSource:
    return RuntimeObservationSource(
        director, {"name": "Wait Projection", "capability": {"id": "factory@1"}},
        home / "source.sqlite3", refresh_interval_seconds=0)


class WaitObservationTests(unittest.TestCase):
    def test_director_wait_is_append_only_across_restart_and_candidate_change_fails_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            director = FakeDirector(home, director_wait_view())
            source = open_source(home, director)
            rows = source._run_rows()
            bindings = {ROOT_RUN: (TASK, CONTEXT)}

            timer_fact = source._record(
                "temporal", f"{ROOT_RUN}:timer:12",
                "com.exomachina.run.state_changed.v1", WAIT_STARTED,
                run_id=ROOT_RUN, task_id=TASK, context_id=CONTEXT,
                fields={"state": "input-required", "phase": "awaiting-director",
                        "wait_deadline": WAIT_DEADLINE})
            source._insert(timer_fact)
            with source._connect() as db:
                timer_before = db.execute(
                    "SELECT record_json FROM source_records WHERE source_id=?",
                    (timer_fact["source_id"],)).fetchone()["record_json"]

            source._public_wait_records(rows, bindings)
            with source._connect() as db:
                wait_row = db.execute(
                    "SELECT record_json FROM source_records WHERE source_id LIKE ?",
                    (f"{ROOT_RUN}:wait:%",)).fetchone()
            persisted = json.loads(wait_row["record_json"])
            self.assertEqual(persisted["time"], WAIT_STARTED)
            self.assertEqual(persisted["task_id"], TASK)
            self.assertEqual(persisted["context_id"], CONTEXT)
            self.assertIn(ROOT_RUN, persisted["source_id"])
            self.assertIn("awaiting-director", persisted["source_id"])
            self.assertIn(WAIT_STARTED, persisted["source_id"])

            envelope = project_source_record(persisted, FACTORY)[3]
            self.assertEqual(envelope["data"], {
                "schema_version": 1,
                "factory_id": FACTORY,
                "run_id": ROOT_RUN,
                "task_id": TASK,
                "context_id": CONTEXT,
                "state": "input-required",
                "phase": "awaiting-director",
                "node": "approval-gate",
                "wait_role": "director",
                "wait_actor_identity": "director-agent",
                "wait_started_at": WAIT_STARTED,
                "wait_deadline": WAIT_DEADLINE,
                "permitted_actions": ["abort", "escalate"],
            })
            self.assertNotIn("quality_findings", envelope["data"])
            self.assertNotIn("raw_context", envelope["data"])
            self.assertNotIn("recommendation", envelope["data"])
            self.assertNotIn("current_revision", envelope["data"])
            self.assertNotIn("_wait_candidate", envelope["data"])

            restarted = open_source(home, director)
            restarted._public_wait_records(restarted._run_rows(), bindings)
            with restarted._connect() as db:
                wait_count = db.execute(
                    "SELECT COUNT(*) AS n FROM source_records WHERE source_id LIKE ?",
                    (f"{ROOT_RUN}:wait:%",)).fetchone()["n"]
                timer_after = db.execute(
                    "SELECT record_json FROM source_records WHERE source_id=?",
                    (timer_fact["source_id"],)).fetchone()["record_json"]
            self.assertEqual(wait_count, 1)
            self.assertEqual(timer_after, timer_before)

            director.view["current_revision"] = "r5"
            director.view["current_sha256"] = "b" * 64
            with self.assertRaisesRegex(SourceContractError, "candidate changed"):
                restarted._public_wait_records(restarted._run_rows(), bindings)

    def test_numeric_runtime_deadline_normalizes_to_utc_and_rejects_invalid_values(self):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            director = FakeDirector(
                home, director_wait_view(wait_deadline=1791051600.0))
            source = open_source(home, director)
            source._public_wait_records(
                source._run_rows(), {ROOT_RUN: (TASK, CONTEXT)})
            with source._connect() as db:
                row = db.execute(
                    "SELECT record_json FROM source_records WHERE source_id LIKE ?",
                    (f"{ROOT_RUN}:wait:%",)).fetchone()
            record = json.loads(row["record_json"])
            event_data = project_source_record(record, FACTORY)[3]["data"]
            self.assertEqual(record["time"], WAIT_STARTED)
            self.assertEqual(event_data["wait_deadline"], "2026-10-03T18:20:00.000Z")

            for index, invalid_deadline in enumerate((True, math.nan, math.inf, -math.inf, 1e300)):
                with self.subTest(deadline=invalid_deadline):
                    invalid_home = home / f"invalid-{index}"
                    invalid_director = FakeDirector(
                        invalid_home, director_wait_view(wait_deadline=invalid_deadline))
                    invalid_source = open_source(invalid_home, invalid_director)
                    with self.assertRaisesRegex(SourceContractError, "deadline"):
                        invalid_source._public_wait_records(
                            invalid_source._run_rows(), {ROOT_RUN: (TASK, CONTEXT)})

            # ISO fixture values remain supported and retain the writer's
            # stated instant through the same public-event validator.
            fixture = project_source_record({
                "factory_id": FACTORY, "source_kind": "temporal",
                "source_id": "fixture-iso-wait", "event_type": "com.exomachina.run.state_changed.v1",
                "time": WAIT_STARTED, "run_id": ROOT_RUN, "task_id": TASK,
                "context_id": CONTEXT,
                "fields": {"state": "input-required", "phase": "awaiting-director",
                           "node": "approval-gate", "wait_role": "director",
                           "wait_actor_identity": "director-agent",
                           "wait_started_at": WAIT_STARTED,
                           "wait_deadline": WAIT_DEADLINE,
                           "permitted_actions": ["abort"]},
            }, FACTORY)[3]["data"]
            self.assertEqual(fixture["wait_deadline"], WAIT_DEADLINE)

    def test_later_candidate_ref_does_not_retrofill_initial_partial_source_row(self):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            director = FakeDirector(
                home, director_wait_view(current_sha256=None))
            source = open_source(home, director)
            bindings = {ROOT_RUN: (TASK, CONTEXT)}
            source._public_wait_records(source._run_rows(), bindings)
            with source._connect() as db:
                before = db.execute(
                    "SELECT record_json FROM source_records WHERE source_id LIKE ?",
                    (f"{ROOT_RUN}:wait:%",)).fetchone()["record_json"]
            prior = json.loads(before)
            self.assertEqual(prior["_wait_candidate"], {"current_revision": "r4"})
            self.assertNotIn("current_sha256", prior["_wait_candidate"])

            director.view["current_sha256"] = ARTIFACT_SHA
            source._public_wait_records(source._run_rows(), bindings)
            with source._connect() as db:
                after = db.execute(
                    "SELECT record_json FROM source_records WHERE source_id LIKE ?",
                    (f"{ROOT_RUN}:wait:%",)).fetchone()["record_json"]
            self.assertEqual(after, before)
            self.assertEqual(
                json.loads(after)["_wait_candidate"], {"current_revision": "r4"})

    def test_human_child_wait_uses_only_temporal_derived_original_task_binding(self):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            view = director_wait_view(
                run_id=CHILD_RUN,
                phase="awaiting-human",
                decision_actor="human:reviewer-7",
                wait_started_at="2026-10-03T18:11:00.000Z",
                wait_deadline="2026-10-03T18:16:00.000Z",
                permitted_actions=["abort"],
                node="human-review",
            )
            director = FakeDirector(home, view)
            source = open_source(home, director)
            bindings = {
                ROOT_RUN: (TASK, CONTEXT),
                CHILD_RUN: (TASK, CONTEXT),
            }
            source._public_wait_records(source._run_rows(), bindings)

            with source._connect() as db:
                row = db.execute(
                    "SELECT record_json FROM source_records WHERE source_id LIKE ?",
                    (f"{CHILD_RUN}:wait:%",)).fetchone()
            record = json.loads(row["record_json"])
            event = project_source_record(record, FACTORY)[3]["data"]
            self.assertEqual(record["run_id"], CHILD_RUN)
            self.assertEqual((record["task_id"], record["context_id"]), (TASK, CONTEXT))
            self.assertEqual(event["wait_role"], "human")
            self.assertEqual(event["wait_actor_identity"], "human:reviewer-7")
            self.assertEqual(event["node"], "human-review")

            # An opaque child ID with no actual Temporal-derived binding is
            # unknown, even though the Director returned it for this Task.
            unbound_source = open_source(home / "unbound", director)
            unbound_source._public_wait_records(
                unbound_source._run_rows(), {ROOT_RUN: (TASK, CONTEXT)})
            with unbound_source._connect() as db:
                unbound_count = db.execute(
                    "SELECT COUNT(*) AS n FROM source_records WHERE source_id LIKE ?",
                    (f"{CHILD_RUN}:wait:%",)).fetchone()["n"]
            self.assertEqual(unbound_count, 0)

    def test_unknown_or_nonwaiting_view_emits_nothing_and_terminal_history_clears_annotations(self):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            director = FakeDirector(home, director_wait_view(run_id="run-not-bound"))
            source = open_source(home, director)
            source._public_wait_records(source._run_rows(), {ROOT_RUN: (TASK, CONTEXT)})
            director.view = director_wait_view(phase="execution")
            source._public_wait_records(source._run_rows(), {ROOT_RUN: (TASK, CONTEXT)})
            with source._connect() as db:
                count = db.execute(
                    "SELECT COUNT(*) AS n FROM source_records WHERE source_id LIKE '%:wait:%'"
                ).fetchone()["n"]
            self.assertEqual(count, 0)

        prior = {
            "state": "input-required", "phase": "awaiting-human",
            "node": "human-review", "wait_deadline": WAIT_DEADLINE,
            "wait_role": "human", "wait_actor_identity": "human:reviewer-7",
            "wait_started_at": WAIT_STARTED, "permitted_actions": ["abort"],
        }
        terminal = _merge_run_state(prior, {
            "state": "completed", "phase": "execution-closed",
            "ended_at": "2026-10-03T18:30:00.000Z",
        })
        for field in ("node", "wait_deadline", "wait_role", "wait_actor_identity",
                      "wait_started_at", "permitted_actions"):
            self.assertNotIn(field, terminal)
        self.assertEqual(terminal["ended_at"], "2026-10-03T18:30:00.000Z")


if __name__ == "__main__":
    unittest.main()
