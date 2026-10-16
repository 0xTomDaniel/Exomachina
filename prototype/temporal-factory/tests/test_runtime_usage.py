"""Focused safe-projection tests for the Runtime Director usage reader."""
from __future__ import annotations

from contextlib import ExitStack
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
from unittest.mock import patch
import unittest

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import harness  # noqa: E402


TASK = "task-synthetic-owned-1"
OTHER_TASK = "task-synthetic-unowned-2"
RUN = "run-synthetic-owned-1"
OTHER_RUN = "run-synthetic-other-2"
MESSAGE = "message-synthetic-1"
DEFINITION = "d" * 64


def measurement(call: int, *, run_id: str | None,
                task_id: str | None = TASK,
                definition_digest: str | None = None,
                call_scope: str = "director_call") -> dict:
    return {
        "measurement_id": f"measurement-synthetic-{call}",
        "model_call_id": f"call-synthetic-{call}",
        "recorded_at": f"2026-10-03T12:00:{call:02d}+00:00",
        "call_scope": call_scope,
        "provider": "synthetic-provider-test-only",
        "model_id": "synthetic-model-test-only-v1",
        "reasoning_effort": "xhigh",
        "unit": "tokens",
        "measurement_source": "provider_reported",
        "completeness": "complete",
        "evidence_status": "provider_reported",
        "usage": {
            "input_tokens": {"status": "reported", "value": 10 + call},
            "output_tokens": {"status": "reported", "value": call},
            "cache_read_tokens": {"status": "reported", "value": 0},
            "cache_write_tokens": {"status": "reported", "value": 0},
            "total_tokens": {"status": "reported", "value": 10 + call + call},
        },
        "service_identity": None,
        "task_id": task_id,
        "message_id": MESSAGE,
        "run_id": run_id,
        "definition_digest": definition_digest,
        "assignment_id": None,
        "attempt_id": None,
    }


class DirectorFacadeFixture(harness.Director):
    """Synthetic public facade; never initializes a provider or a real ledger."""

    def __init__(self, rows: list[dict], *, ignore_task_and_scope_filters: bool = False):
        self.rows = rows
        self.queries: list[dict] = []
        self.ignore_task_and_scope_filters = ignore_task_and_scope_filters

    def list_measurements(self, **filters):
        self.queries.append(filters)
        return [row for row in self.rows if all(
            expected is None or (key in {"task_id", "call_scope"} and
                                 self.ignore_task_and_scope_filters)
            or row.get(key) == expected for key, expected in filters.items())]


class PlainFacadeFixture:
    def __init__(self, rows: list[dict]):
        self.rows = rows
        self.queries: list[dict] = []

    def list_measurements(self, **filters):
        self.queries.append(filters)
        return [row for row in self.rows if all(
            expected is None or row.get(key) == expected
            for key, expected in filters.items())]


class DirectorUsageReaderTests(unittest.TestCase):
    def setUp(self):
        self.scopes = [{
            "run_id": RUN,
            "task_id": TASK,
            "context_id": "context-synthetic-1",
            "pinned": {"definition_digest": DEFINITION},
        }]

    def test_returns_pre_run_and_bound_calls_without_filling_null_bindings(self):
        pre_run = measurement(1, run_id=None, definition_digest=None)
        bound = [measurement(index, run_id=RUN, definition_digest=DEFINITION)
                 for index in (2, 3, 4)]
        reader = DirectorFacadeFixture([pre_run, *bound, pre_run])

        rows, report = harness._director_measurements(reader, self.scopes, {
            "run_id": RUN, "task_id": TASK, "assignment_id": None,
            "attempt_id": None, "model_call_id": None, "call_scope": "director_call",
        })

        by_id = {row["measurement_id"]: row for row in rows}
        self.assertEqual(set(by_id), {row["measurement_id"] for row in [pre_run, *bound]})
        self.assertEqual(len(rows), 4)
        self.assertEqual(by_id[pre_run["measurement_id"]]["recorded_at"], pre_run["recorded_at"])
        self.assertIsNone(by_id[pre_run["measurement_id"]]["run_id"])
        self.assertIsNone(by_id[pre_run["measurement_id"]]["definition_digest"])
        self.assertIsNone(by_id[pre_run["measurement_id"]]["assignment_id"])
        self.assertIsNone(by_id[pre_run["measurement_id"]]["attempt_id"])
        self.assertTrue(all("cost" not in row and "charge" not in row for row in rows))
        self.assertEqual(reader.queries, [
            {"run_id": RUN, "call_scope": "director_call"},
            {"task_id": TASK, "call_scope": "director_call", "run_id": None},
        ])
        self.assertEqual(report, {"queries_failed": 0, "rows_rejected": 0, "conflicts": 0})

    def test_unbound_lookup_uses_only_authorized_scoped_task_and_rejects_mismatches(self):
        owned = measurement(1, run_id=None)
        unowned = measurement(2, run_id=None, task_id=OTHER_TASK)
        wrong_scope = measurement(3, run_id=None, call_scope="assignment_call")
        reader = DirectorFacadeFixture(
            [owned, unowned, wrong_scope], ignore_task_and_scope_filters=True)

        rows, report = harness._director_measurements(reader, self.scopes, {
            "run_id": RUN, "task_id": TASK, "assignment_id": None,
            "attempt_id": None, "model_call_id": None, "call_scope": "director_call",
        })

        self.assertEqual([row["measurement_id"] for row in rows], [owned["measurement_id"]])
        self.assertEqual(reader.queries[-1], {
            "task_id": TASK, "call_scope": "director_call", "run_id": None,
        })
        self.assertEqual(report["rows_rejected"], 2)

    def test_unbound_rows_are_not_read_through_a_generic_or_other_owner_facade(self):
        unbound = measurement(1, run_id=None)
        bound = measurement(2, run_id=RUN, definition_digest=DEFINITION)
        reader = PlainFacadeFixture([unbound, bound])

        rows, report = harness._director_measurements(reader, self.scopes, {
            "run_id": RUN, "task_id": TASK, "assignment_id": None,
            "attempt_id": None, "model_call_id": None, "call_scope": "director_call",
        })

        self.assertEqual([row["measurement_id"] for row in rows], [bound["measurement_id"]])
        self.assertEqual(reader.queries, [{"run_id": RUN, "call_scope": "director_call"}])
        self.assertEqual(report["queries_failed"], 0)


class UsageAggregateRouteTests(unittest.TestCase):
    def test_authenticated_usage_route_marks_prerun_director_rows_partial(self):
        context = "context-synthetic-aggregate"
        manifest = "manifest-synthetic-aggregate"
        package = "package-synthetic-aggregate"
        aggregate_run = "run-synthetic-aggregate"
        aggregate_task = "task-synthetic-aggregate"
        pinned_definition = "e" * 64

        class StubRunner:
            def __init__(self, *_args, **_kwargs):
                pass

            def is_running(self):
                return False

        class FakeObservation:
            def __init__(self, *_args, **_kwargs):
                pass

            def snapshot(self, _principal, _factory_id):
                return {"state": {"runs": [{
                    "id": aggregate_run,
                    "task": {"id": aggregate_task, "context_id": context},
                    "pinned": {"manifest_digest": manifest,
                               "package_digest": package,
                               "definition_digest": pinned_definition},
                }]}}

        class FakeSource:
            def __init__(self, *_args, **_kwargs):
                self.delivery_reader = None

        class EmptyUsageBroker:
            def usage_journal(self):
                return object()

        report = {"queries_failed": 0, "rows_rejected": 0, "conflicts": 0}
        pre_run = measurement(90, run_id=None, task_id=aggregate_task,
                              definition_digest=None)
        with tempfile.TemporaryDirectory(prefix="exo-usage-route-") as temp:
            home = Path(temp)
            instance = home / "instances" / "factory"
            with patch.object(harness, "Runner", StubRunner):
                harness.init_instance(instance, name="usage-route-test", mode="factory",
                                      port=44981, home=home)
            patches = (
                patch.object(harness, "Runner", StubRunner),
                patch.object(harness, "RuntimeObservationSource", FakeSource),
                patch.object(harness, "FactoryObservation", FakeObservation),
                patch.object(harness, "install_observation_transport", lambda *_a, **_k: None),
                patch.object(harness, "install_local_delivery_routes", lambda *_a, **_k: None),
                patch.object(harness.Director, "task_binding",
                             return_value=(aggregate_run, context)),
                patch.object(harness.Director, "run_record", return_value={
                    "task_id": aggregate_task, "context_id": context,
                    "manifest_digest": manifest, "package_digest": package}),
                patch.object(harness, "_authoring_measurements", return_value=([], report)),
                patch.object(harness, "_director_measurements",
                             return_value=([pre_run], report)),
                patch.object(harness, "_pinned_service_measurements", return_value=([], {
                    "pinned_owner_count": 0, "owner_resolution_failures": 0,
                    "queries_failed": 0, "non_usage_service_count": 0,
                    "rows_rejected": 0, "conflicts": 0})),
            )
            with ExitStack() as stack:
                for active_patch in patches:
                    stack.enter_context(active_patch)
                app = harness.create_app(instance, commercial_reader=object(),
                                         usage_broker=EmptyUsageBroker())
                with TestClient(app) as client:
                    self.assertEqual(client.get("/usage/measurements").status_code, 401)
                    response = client.get("/usage/measurements?run_id=" + aggregate_run,
                                          headers={"Authorization": harness.TOKEN})

            self.assertEqual(response.status_code, 200)
            result = response.json()
            self.assertEqual(result["measurements"], [pre_run])
            self.assertEqual(result["measurements"][0]["run_id"], None)
            self.assertEqual(result["measurements"][0]["definition_digest"], None)
            self.assertEqual(result["coverage"]["director"]["status"], "partial")
            self.assertEqual(result["coverage"]["bound_sources_status"], "partial")
            self.assertEqual(result["coverage"]["director_unbound"]["status"], "unavailable")
            self.assertNotIn("cost", result["measurements"][0])


if __name__ == "__main__":
    unittest.main()
