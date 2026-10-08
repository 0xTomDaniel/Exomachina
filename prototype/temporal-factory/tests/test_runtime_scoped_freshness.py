"""Scoped Temporal freshness tests using only the existing synthetic Runtime fixture."""
from __future__ import annotations

import sys
import json
import threading
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from observation import (FactoryObservation, ObservationForbidden, ObservationNotFound,
                         SourceContractError, SourcePage)  # noqa: E402
from observation_source import RuntimeObservationSource  # noqa: E402
from test_runtime_observation import FakeDirector  # noqa: E402
from temporalio.api.enums.v1 import EventType  # noqa: E402
from temporalio.service import RPCError, RPCStatusCode  # noqa: E402


FACTORY_ID = "factory-test"
ROOT_RUN = "run-1"
CHILD_RUN = "run-1:child:actual"
OLD_RUN = "old-root"


class _FakeHandle:
    def __init__(self, workflow_id, client):
        self.workflow_id = workflow_id
        self.client = client

    async def fetch_history(self):
        self.client.fetches.append(self.workflow_id)
        if self.workflow_id in self.client.errors:
            raise self.client.errors[self.workflow_id]
        return SimpleNamespace(events=self.client.histories[self.workflow_id])


class _FakeTemporalClient:
    def __init__(self, histories, errors=None):
        self.histories = dict(histories)
        self.errors = dict(errors or {})
        self.fetches = []
        self.data_converter = SimpleNamespace(payload_converter=SimpleNamespace(
            from_payloads=lambda payloads: list(payloads)))

    def get_workflow_handle(self, workflow_id):
        return _FakeHandle(workflow_id, self)


def _event(event_type, event_id, **attributes):
    at = datetime(2026, 10, 4, 12, 0, event_id, tzinfo=timezone.utc)
    event_attributes = SimpleNamespace(**attributes)
    names = {
        EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
            "workflow_execution_started_event_attributes",
        EventType.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:
            "start_child_workflow_execution_initiated_event_attributes",
        EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
            "workflow_execution_completed_event_attributes",
    }
    return SimpleNamespace(
        event_type=event_type,
        event_id=event_id,
        event_time=SimpleNamespace(ToDatetime=lambda *, tzinfo: at.astimezone(tzinfo)),
        **{names[event_type]: event_attributes})


def _workflow_history(run_id, *, child_id=None):
    start_input = {
        "run": run_id,
        "definition_digest": "c" * 64,
        "package_digest": "b" * 64,
        "closure": {"manifest_digest": "a" * 64,
                    "manifest": {"root_digest": "c" * 64,
                                 "interpreter": {"build_id": "b-123456789abc"},
                                 "services": {}}},
        "document": {"nodes": {}},
    }
    events = [_event(
        EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
        1,
        input=SimpleNamespace(payloads=[start_input]),
    )]
    if child_id is not None:
        events.append(_event(
            EventType.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
            2,
            workflow_id=child_id,
        ))
    events.append(_event(
        EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
        3 if child_id is not None else 2,
        result=SimpleNamespace(payloads=[]),
    ))
    return events


class RuntimeScopedFreshnessTests(unittest.TestCase):
    def setUp(self):
        self.temp = Path(tempfile.mkdtemp(prefix="exo-scoped-freshness-", dir="/tmp"))
        self.instance = self.temp / "instance"
        self.director = FakeDirector(self.instance)
        self._add_run(OLD_RUN, "task-old", "context-old")
        self.client = _FakeTemporalClient({
            ROOT_RUN: _workflow_history(ROOT_RUN, child_id=CHILD_RUN),
            CHILD_RUN: _workflow_history(CHILD_RUN),
            OLD_RUN: _workflow_history(OLD_RUN),
        })

        async def connect():
            return self.client

        self.director.client = connect
        self.source = RuntimeObservationSource(
            self.director,
            {"name": "factory-test", "capability": {"id": "verified-research@1"}},
            self.instance / "runtime-source.sqlite3",
            history_reader=None,
            refresh_interval_seconds=0,
        )
        self.projection = FactoryObservation(
            self.source, self.instance / "observation.sqlite3")

    def _add_run(self, run_id, task_id, context_id):
        with self.director.connect() as db:
            db.execute("INSERT INTO runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                       (run_id, task_id, context_id, "b" * 64, "a" * 64,
                        "b-123456789abc", "v1", "{}", "c" * 64,
                        "fixture-operator", "{}", 0, None))

    def test_authenticated_run_snapshot_is_fresh_for_root_and_actual_child_only(self):
        # The unrelated old Workflow ID is genuinely absent. It keeps factory
        # freshness disconnected without suppressing a fully read selected run.
        self.client.errors[OLD_RUN] = RPCError(
            "workflow not found", RPCStatusCode.NOT_FOUND, b"")

        snapshot = self.projection.snapshot("fixture-operator", FACTORY_ID,
                                            run_id=ROOT_RUN)
        freshness = snapshot["freshness"]
        self.assertEqual(freshness["status"], "fresh")
        self.assertEqual(freshness["scope"], "run")
        self.assertEqual(freshness["run_id"], ROOT_RUN)
        self.assertEqual(freshness["included_run_ids"], [ROOT_RUN, CHILD_RUN])
        self.assertEqual(freshness["factory_status"], "disconnected")
        self.assertEqual(freshness["unavailable_run_ids"], [OLD_RUN])
        self.assertEqual(self.client.fetches.count(ROOT_RUN), 1)
        self.assertEqual(self.client.fetches.count(CHILD_RUN), 1)
        self.assertEqual(self.client.fetches.count(OLD_RUN), 1)

        # Projection asks the source for its captured result inside the snapshot
        # transaction; it must not perform a second Temporal read there.
        self.assertEqual(len(self.client.fetches), 3)

        factory_snapshot = self.projection.snapshot("fixture-operator", FACTORY_ID)
        self.assertEqual(factory_snapshot["freshness"]["scope"], "factory")
        self.assertEqual(factory_snapshot["freshness"]["status"], "disconnected")
        self.assertEqual(factory_snapshot["freshness"]["unavailable_run_ids"], [OLD_RUN])

    def test_missing_selected_history_is_disconnected_and_does_not_infer_children(self):
        # First persist the run and its child link from a successful public read.
        initial = self.projection.snapshot("fixture-operator", FACTORY_ID,
                                           run_id=ROOT_RUN)
        self.assertEqual(initial["freshness"]["status"], "fresh")

        # The same already-known run subsequently returns exact Temporal NOT_FOUND.
        self.client.errors[ROOT_RUN] = RPCError(
            "workflow not found", RPCStatusCode.NOT_FOUND, b"")
        self.client.fetches.clear()
        snapshot = self.projection.snapshot("fixture-operator", FACTORY_ID,
                                            run_id=ROOT_RUN)
        freshness = snapshot["freshness"]
        self.assertEqual(freshness["status"], "disconnected")
        self.assertEqual(freshness["scope"], "run")
        self.assertEqual(freshness["run_id"], ROOT_RUN)
        self.assertEqual(freshness["included_run_ids"], [ROOT_RUN])
        self.assertEqual(freshness["unavailable_run_ids"], [ROOT_RUN])
        self.assertNotIn(CHILD_RUN, freshness["included_run_ids"])
        self.assertEqual(freshness["factory_status"], "disconnected")
        self.assertEqual(self.client.fetches.count(ROOT_RUN), 1)

    def test_actual_child_not_found_blocks_run_scope_but_keeps_linked_child_id(self):
        self.client.errors[CHILD_RUN] = RPCError(
            "workflow not found", RPCStatusCode.NOT_FOUND, b"")

        snapshot = self.projection.snapshot("fixture-operator", FACTORY_ID,
                                            run_id=ROOT_RUN)
        freshness = snapshot["freshness"]
        self.assertEqual(freshness["status"], "disconnected")
        self.assertEqual(freshness["scope"], "run")
        self.assertEqual(freshness["run_id"], ROOT_RUN)
        self.assertEqual(freshness["included_run_ids"], [ROOT_RUN, CHILD_RUN])
        self.assertEqual(freshness["factory_status"], "disconnected")
        self.assertEqual(freshness["unavailable_run_ids"], [CHILD_RUN])
        self.assertEqual(self.client.fetches.count(ROOT_RUN), 1)
        self.assertEqual(self.client.fetches.count(CHILD_RUN), 1)

    def test_non_not_found_temporal_error_fails_closed_for_run_and_factory(self):
        initial = self.projection.snapshot("fixture-operator", FACTORY_ID,
                                           run_id=ROOT_RUN)
        self.assertEqual(initial["freshness"]["status"], "fresh")
        self.client.errors[CHILD_RUN] = RPCError(
            "temporal service unavailable", RPCStatusCode.UNAVAILABLE, b"")
        self.client.fetches.clear()

        snapshot = self.projection.snapshot("fixture-operator", FACTORY_ID,
                                            run_id=ROOT_RUN)
        self.assertEqual(snapshot["freshness"]["scope"], "run")
        self.assertEqual(snapshot["freshness"]["status"], "disconnected")
        self.assertEqual(snapshot["freshness"]["factory_status"], "disconnected")
        self.assertEqual(self.client.fetches.count(CHILD_RUN), 1)

        factory_snapshot = self.projection.snapshot("fixture-operator", FACTORY_ID)
        self.assertEqual(factory_snapshot["freshness"]["status"], "disconnected")

    def test_run_freshness_requires_existing_authorized_factory_and_run(self):
        with self.assertRaises(ObservationForbidden):
            self.source.get_run_freshness("untrusted", FACTORY_ID, ROOT_RUN)
        with self.assertRaises(ObservationForbidden):
            self.source.get_run_freshness("fixture-operator", "other-factory", ROOT_RUN)
        with self.assertRaises(ObservationNotFound):
            self.source.get_run_freshness("fixture-operator", FACTORY_ID, "unknown-run")
        self.assertEqual(self.client.fetches, [])

    def test_snapshot_rejects_source_freshness_bound_to_a_different_run(self):
        self.source.get_run_freshness = lambda principal, factory_id, run_id: {
            "status": "current", "observed_at": "2026-10-04T12:00:00Z",
            "scope": "run", "run_id": OLD_RUN,
            "included_run_ids": [OLD_RUN], "factory_status": "fresh",
            "unavailable_run_ids": [],
        }
        with self.assertRaises(SourceContractError):
            self.projection.snapshot("fixture-operator", FACTORY_ID,
                                     run_id=ROOT_RUN)

    def test_snapshot_freshness_stays_bound_to_its_captured_source_page(self):
        # A second source reader races between projection refresh and snapshot
        # capture. Its newer disconnected read cannot be paired with the older
        # state/cursor captured by this snapshot.
        original_refresh = self.projection.refresh
        competing_thread = None
        competing_done = threading.Event()

        def refresh_then_race(principal, factory_id, *, max_pages=None):
            nonlocal competing_thread
            inserted = original_refresh(principal, factory_id, max_pages=max_pages)
            self.client.errors[CHILD_RUN] = RPCError(
                "temporal service unavailable", RPCStatusCode.UNAVAILABLE, b"")

            def second_reader():
                try:
                    self.source.read_page(principal, factory_id, None, limit=10_000)
                finally:
                    competing_done.set()

            competing_thread = threading.Thread(target=second_reader, daemon=True)
            competing_thread.start()
            competing_thread.join(timeout=0.25)
            return inserted

        self.projection.refresh = refresh_then_race
        snapshot = self.projection.snapshot("fixture-operator", FACTORY_ID,
                                            run_id=ROOT_RUN)
        self.assertTrue(competing_done.wait(2.0), "competing source read did not finish")
        assert competing_thread is not None
        competing_thread.join(timeout=1.0)
        self.assertEqual(snapshot["freshness"]["status"], "fresh")
        self.assertEqual(snapshot["freshness"]["factory_status"], "fresh")
        with self.projection._connect() as db:
            row = db.execute("SELECT freshness_json FROM observation_streams "
                             "WHERE factory_id=?", (FACTORY_ID,)).fetchone()
        captured = json.loads(row[0])
        self.assertEqual(captured["factory"]["status"], "fresh")
        self.assertEqual(captured["runs"][ROOT_RUN]["factory_status"], "fresh")

    def test_incomplete_modern_capture_never_falls_back_to_factory_freshness(self):
        initial = self.projection.snapshot("fixture-operator", FACTORY_ID,
                                           run_id=ROOT_RUN)
        self.assertEqual(initial["freshness"]["status"], "fresh")
        read_page = self.source.read_page

        def omit_scoped_entry(principal, factory_id, after_cursor, *, limit):
            page = read_page(principal, factory_id, after_cursor, limit=limit)
            return SourcePage(page.records, page.next_cursor, page.has_more,
                              page.freshness, run_freshness={})

        self.source.read_page = omit_scoped_entry
        try:
            snapshot = self.projection.snapshot("fixture-operator", FACTORY_ID,
                                                run_id=ROOT_RUN)
        except (ObservationNotFound, SourceContractError):
            return
        self.assertNotEqual(snapshot["freshness"].get("scope"), "factory")
        self.assertEqual(snapshot["freshness"].get("run_id"), ROOT_RUN)
        self.assertIn(snapshot["freshness"].get("status"), {"unknown", "disconnected"})


if __name__ == "__main__":
    unittest.main()
