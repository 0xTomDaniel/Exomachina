"""Harness identity, public contract, publication, and agent mode boundary tests."""
from __future__ import annotations

import json
import asyncio
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import AsyncMock, patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))
sys.path.insert(0, str(ROOT / "services"))

import harness  # noqa: E402
import a2a_v1_server  # noqa: E402
sys.path.insert(0, str(ROOT / "scenarios"))
from single_factory import CHECK_IDS, FOLLOW_UP, check_evidence  # noqa: E402
from authoring import approve, materialize  # noqa: E402
from binding import build_id_for, source_digest  # noqa: E402
from fixture import assignment  # noqa: E402
import long_client  # noqa: E402
import sqlite3  # noqa: E402
from testbed import (REPORT_CAPABILITIES, REPORT_NAMES, REPORT_QUALITY_POLICY,  # noqa: E402
                     report_bindings, report_role)


class StubRunner:
    """Records the runner boundary without starting local infrastructure."""

    def __init__(self, home: Path, *, port_base=None, member_base=None):
        self.home = home
        self.address = "127.0.0.1:44542"
        self.started = []
        self.built = []
        self.workers = []

    def is_running(self):
        return bool(self.started)

    def ensure_started(self, *, reason, timeout=180):
        self.started.append(reason)
        return {"reason": reason}

    def ensure_build(self, source_dir):
        self.built.append(source_dir)
        digest = source_digest(source_dir)
        return {"build_id": build_id_for(digest), "source_digest": digest}

    def wait_worker(self, build_id, timeout=60):
        self.workers.append(build_id)
        return {"build_id": build_id}


class DirectorBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.state = Path(tempfile.mkdtemp(prefix="exo-proto-harness-test-", dir="/tmp"))
        self.home = self.state / "home"
        self.instance = self.home / "instances" / "factory"
        self.config = harness.init_instance(
            self.instance, name="factory-test", mode="factory", port=44874, home=self.home)
        runner_patch = patch.object(harness, "Runner", StubRunner)
        runner_patch.start()
        self.addCleanup(runner_patch.stop)
        self.director = harness.Director(self.instance, self.config)

    def test_identity_persists_and_stale_incarnation_is_fenced(self):
        first = self.director
        second = harness.Director(self.instance, self.config)
        self.assertEqual(first.identity, second.identity)
        self.assertEqual(first.token, second.token)
        self.assertEqual(second.incarnation, first.incarnation + 1)
        with first.connect() as db, self.assertRaisesRegex(harness.Rejected, "stale"):
            first.fence(db)
        with second.connect() as db:
            second.fence(db)

    def test_direct_script_and_import_share_the_authenticated_actor_context(self):
        source_path = SRC / "harness.py"
        probe = r'''
import ast, pathlib, sys, types
path = pathlib.Path(sys.argv[1])
tree = ast.parse(path.read_text())
assert isinstance(tree.body[-1], ast.If)
module = types.ModuleType("__main__")
module.__file__ = str(path)
sys.modules["__main__"] = module
exec(compile(ast.Module(body=tree.body[:-1], type_ignores=[]), str(path), "exec"),
     module.__dict__)
import harness
assert harness is module
token = harness.CURRENT_ACTOR.set("fixture-operator")
assert module.CURRENT_ACTOR.get() == "fixture-operator"
harness.CURRENT_ACTOR.reset(token)
print("same-context")
'''
        completed = subprocess.run(
            [sys.executable, "-c", probe, str(source_path)],
            cwd=SRC, capture_output=True, text=True, timeout=30)
        self.assertEqual(completed.returncode, 0, "direct-entrypoint identity probe failed")
        self.assertEqual(completed.stdout.strip(), "same-context")

    def test_start_preserves_start_rpc_error_when_reconciliation_query_is_not_found(self):
        class MissingWorkflow(Exception):
            pass

        start_error = RuntimeError("synthetic start RPC failure")
        handle = SimpleNamespace(query=AsyncMock(side_effect=[
            MissingWorkflow("no existing execution"),
            MissingWorkflow("still no execution"),
        ]))
        start_workflow = AsyncMock(side_effect=start_error)
        client = SimpleNamespace(
            get_workflow_handle=lambda _run_id: handle,
            start_workflow=start_workflow,
        )
        run = {"run_id": "run-synthetic", "package_digest": "package-synthetic",
               "manifest_digest": "manifest-synthetic", "run_inputs_digest": "inputs-synthetic"}
        package = {"root": {"nodes": {}}}
        publication = {"build_id": "build-synthetic", "closure": {}}
        with (patch.object(self.director, "client", new_callable=AsyncMock,
                           return_value=client),
              patch.object(harness, "RPCError", MissingWorkflow),
              patch.object(harness, "verify_closure"),
              patch.object(harness, "build_workflow_input", return_value={"run": "run-synthetic"}),
              patch.object(harness, "_workflow_execution_timeout_seconds", return_value=30),
              self.assertRaises(RuntimeError) as raised):
            asyncio.run(self.director._start(run, package, publication))
        self.assertIs(raised.exception, start_error)
        self.assertEqual(handle.query.await_count, 2)
        self.assertEqual(start_workflow.await_count, 1)

    def test_basic_readiness_requires_both_live_pollers_for_exact_pinned_build(self):
        ready = harness._worker_readiness_evidence("build-pinned", {
            "build_id": "build-pinned", "registered": True,
            "live_pollers": {
                "TASK_QUEUE_TYPE_WORKFLOW": ["worker-a"],
                "TASK_QUEUE_TYPE_ACTIVITY": ["worker-b"],
            },
        })
        self.assertEqual(ready, {"status": "ready", "build_id": "build-pinned",
            "registered": True, "workflow_poller_count": 1,
            "activity_poller_count": 1})
        missing_activity = harness._worker_readiness_evidence("build-pinned", {
            "build_id": "build-pinned", "registered": True,
            "live_pollers": {"TASK_QUEUE_TYPE_WORKFLOW": ["worker-a"]},
        })
        self.assertEqual(missing_activity["status"], "unavailable")
        self.assertEqual(missing_activity["activity_poller_count"], 0)
        wrong_build = harness._worker_readiness_evidence("build-pinned", {
            "build_id": "other-build", "registered": True,
            "live_pollers": {"TASK_QUEUE_TYPE_WORKFLOW": ["worker-a"],
                             "TASK_QUEUE_TYPE_ACTIVITY": ["worker-b"]},
        })
        self.assertEqual(wrong_build["status"], "unavailable")
        self.assertIsNone(wrong_build["workflow_poller_count"])

    def test_basic_preflight_starts_only_the_pinned_worker_before_ready(self):
        self.director.basic_single_active_job = True
        unavailable = {"submission_ready": False,
            "submission_blockers": ["pinned_worker_pollers_unavailable"],
            "temporal_worker": {"build_id": "build-pinned", "status": "unavailable"}}
        ready = {"submission_ready": True, "submission_blockers": [],
            "temporal_worker": {"build_id": "build-pinned", "status": "ready"}}
        with patch.object(harness, "_submission_readiness",
                          side_effect=[unavailable, ready]) as readiness:
            _read, preflight = harness._submission_readiness_callbacks(
                self.director, self.config)
            preflight()
        self.assertEqual(readiness.call_count, 2)
        self.assertEqual(self.director.module.runner.started,
                         ["basic-submission-worker-readiness"])
        self.assertEqual(self.director.module.runner.workers, ["build-pinned"])

    def test_prerun_director_rows_keep_coverage_partial_without_filling_run_pin(self):
        report = {"queries_failed": 0, "rows_rejected": 0, "conflicts": 0}
        bound = [{"call_scope": "director_call", "run_id": "run-bound"}]
        unbound = [{"call_scope": "director_call", "run_id": None,
                    "definition_digest": None, "task_id": "task-authorized"}]
        self.assertEqual(harness._director_coverage_status(report, bound), "available")
        self.assertEqual(harness._director_coverage_status(report, unbound), "partial")
        self.assertEqual(unbound[0]["run_id"], None)
        self.assertEqual(unbound[0]["definition_digest"], None)

    def test_recovery_starts_only_for_unfinished_runs(self):
        runner = self.director.module.runner
        self.assertIsNone(self.director.recover())
        self.assertEqual(runner.started, [])
        with self.director.connect() as db:
            db.execute(
                "INSERT INTO runs (run_id, task_id, context_id, package_digest, "
                "manifest_digest, build_id, label, run_inputs_json, run_inputs_digest, "
                "authorized_actor, input_authority_json, closed) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)",
                ("unfinished", "task", "context", "package", "manifest", "build",
                 "v1", "{}", "inputs", "fixture-operator", "{}"),
            )
        self.assertEqual(self.director.recover(), {"reason": "recover-unfinished:1"})
        self.assertEqual(len(runner.started), 1)
        self.assertTrue(runner.started[0].startswith("recover-unfinished"))

    def test_admission_capacity_requires_explicit_nonnegative_factory_config(self):
        configured = harness.init_instance(
            self.home / "instances" / "admission",
            name="admission-test", mode="factory", port=44876, home=self.home,
            admission_capacity=0)
        self.assertEqual(configured["admission_capacity"], 0)
        self.assertEqual(harness.load_config(self.home / "instances" / "admission")
                         ["admission_capacity"], 0)
        with self.assertRaisesRegex(ValueError, "non-negative integer"):
            harness.init_instance(self.home / "instances" / "invalid-admission",
                name="invalid-admission", mode="factory", port=44877, home=self.home,
                admission_capacity=True)
        with self.assertRaisesRegex(ValueError, "factory mode"):
            harness.init_instance(self.home / "instances" / "agent-admission",
                name="agent-admission", mode="agent", port=44878, home=self.home,
                admission_capacity=1)

    def test_start_waits_for_admission_and_terminal_release_promotes_fifo(self):
        queue = harness.AdmissionQueue(
            self.instance / "admission.sqlite3", factory_id=self.director.identity,
            capacity=1)
        self.director.admission_queue = queue
        publication = {"manifest_digest": "a" * 64, "package_digest": "b" * 64,
                       "build_id": "build-test", "closure": {}}
        package = {"root": {"nodes": {}}, "bindings": {}, "run_inputs": {}}
        with (patch.object(self.director.module.publications, "active",
                           return_value=publication),
              patch.object(self.director.module.publications, "get",
                           return_value=publication),
              patch.object(self.director.module, "package", return_value=package),
              patch.object(self.director, "ensure_runner") as ensure_runner,
              patch.object(self.director, "_start", new_callable=AsyncMock) as start_workflow):
            actor = harness.CURRENT_ACTOR.set("fixture-operator")
            try:
                first = self.director.perform(
                    {"op": "start", "action_id": "action-1", "inputs": {}},
                    "task-1", "context-1")
                second = self.director.perform(
                    {"op": "start", "action_id": "action-2", "inputs": {}},
                    "task-2", "context-2")
            finally:
                harness.CURRENT_ACTOR.reset(actor)

            self.assertEqual(first["admission_state"], "admitted")
            self.assertEqual(second["admission_state"], "queued")
            self.assertEqual(start_workflow.await_count, 1)
            self.assertEqual(ensure_runner.call_count, 1)
            queued_task = harness.sync(harness.FactoryTaskStore(self.director).get("task-2"))
            self.assertEqual(queued_task.status.state, harness.task_state("working"))
            self.assertEqual(start_workflow.await_count, 1)

            run_id = self.director.run_id_for("action-1")
            self.director.close_run(run_id, {"state": "completed", "status": {},
                                               "result": None, "incident": None})
            with self.assertRaisesRegex(harness.Rejected, "persisted Task outcome"):
                harness.sync(self.director._release_terminal_admission(
                    "task-1", run_id, "failed"))
            released = harness.sync(self.director._release_terminal_admission(
                "task-1", run_id, "completed"))

        self.assertEqual(released["released"]["state"], "released")
        self.assertEqual([item["task_id"] for item in released["admitted"]], ["task-2"])
        self.assertEqual(queue.get("action-2")["state"], "admitted")
        self.assertEqual(start_workflow.await_count, 2)
        self.assertEqual(ensure_runner.call_count, 2)
        self.assertEqual(queue.capacity_view()["admitted_count"], 1)

    def test_zero_capacity_leaves_task_queued_without_starting_runner(self):
        queue = harness.AdmissionQueue(
            self.instance / "admission-zero.sqlite3", factory_id=self.director.identity,
            capacity=0)
        self.director.admission_queue = queue
        publication = {"manifest_digest": "a" * 64, "package_digest": "b" * 64,
                       "build_id": "build-test", "closure": {}}
        package = {"root": {"nodes": {}}, "bindings": {}, "run_inputs": {}}
        with (patch.object(self.director.module.publications, "active",
                           return_value=publication),
              patch.object(self.director.module, "package", return_value=package),
              patch.object(self.director, "ensure_runner") as ensure_runner,
              patch.object(self.director, "_start", new_callable=AsyncMock) as start_workflow):
            actor = harness.CURRENT_ACTOR.set("fixture-operator")
            try:
                accepted = self.director.perform(
                    {"op": "start", "action_id": "action-paused", "inputs": {}},
                    "task-paused", "context-paused")
            finally:
                harness.CURRENT_ACTOR.reset(actor)
            self.assertEqual(accepted["admission_state"], "queued")
            task = harness.sync(harness.FactoryTaskStore(self.director).get("task-paused"))
        self.assertEqual(task.status.state, harness.task_state("working"))
        self.assertEqual(queue.get("action-paused")["state"], "queued")
        self.assertEqual(ensure_runner.call_count, 0)
        self.assertEqual(start_workflow.await_count, 0)

    def test_lifecycle_reconciler_releases_terminal_and_promotes_without_task_poll(self):
        queue = harness.AdmissionQueue(
            self.instance / "admission.sqlite3", factory_id=self.director.identity,
            capacity=1)
        self.director.admission_queue = queue
        publication = {"manifest_digest": "a" * 64, "package_digest": "b" * 64,
                       "build_id": "build-test", "closure": {}}
        package = {"root": {"nodes": {}}, "bindings": {}, "run_inputs": {}}

        class Handle:
            async def describe(self):
                return SimpleNamespace(status=SimpleNamespace(name="COMPLETED"))

            async def query(self, _query):
                return {"phase": "accepted"}

            async def result(self):
                return {"status": "accepted"}

        class Client:
            def get_workflow_handle(self, _run_id):
                return Handle()

        class QueryFailureHandle:
            async def describe(self):
                raise TimeoutError("synthetic Temporal query timeout")

        class QueryFailureClient:
            def get_workflow_handle(self, _run_id):
                return QueryFailureHandle()

        with (patch.object(self.director.module.publications, "active",
                           return_value=publication),
              patch.object(self.director.module.publications, "get",
                           return_value=publication),
              patch.object(self.director.module, "package", return_value=package),
              patch.object(self.director, "_start", new_callable=AsyncMock) as start_workflow):
            actor = harness.CURRENT_ACTOR.set("fixture-operator")
            try:
                self.director.perform(
                    {"op": "start", "action_id": "action-1", "inputs": {}},
                    "task-1", "context-1")
                self.director.perform(
                    {"op": "start", "action_id": "action-2", "inputs": {}},
                    "task-2", "context-2")
            finally:
                harness.CURRENT_ACTOR.reset(actor)
            store = harness.FactoryTaskStore(self.director)
            with patch.object(self.director, "client", new_callable=AsyncMock,
                              return_value=QueryFailureClient()):
                failed = harness.sync(store.reconcile_admitted())
            self.assertEqual(failed, {"checked": 0, "query_errors": 1})
            self.assertEqual(queue.get("action-1")["state"], "admitted")
            self.assertEqual(queue.get("action-2")["state"], "queued")

            with patch.object(self.director, "client", new_callable=AsyncMock,
                              return_value=Client()):
                passed = harness.sync(store.reconcile_admitted())

        self.assertEqual(passed, {"checked": 1, "query_errors": 0})
        self.assertEqual(queue.get("action-1")["state"], "released")
        self.assertEqual(queue.get("action-2")["state"], "admitted")
        self.assertEqual(start_workflow.await_count, 2)
        self.assertEqual(queue.capacity_view()["admitted_count"], 1)

    def test_public_commands_keep_graph_selection_inside_instance(self):
        director = self.director
        action_id = "same-logical-action"
        self.assertEqual(director.run_id_for(action_id), director.run_id_for(action_id))
        self.assertTrue(director.run_id_for(action_id).startswith(director.identity + "."))
        token = harness.CURRENT_ACTOR.set("fixture-operator")
        try:
            for field, value in (("package_digest", "caller-choice"),
                                 ("graph", {}), ("version", "v2")):
                with self.subTest(field=field), self.assertRaisesRegex(
                        harness.Rejected, "invalid start fields"):
                    director.perform({"op": "start", "action_id": action_id,
                                      "inputs": {}, field: value}, "task", "context")
            with self.assertRaisesRegex(harness.Rejected, "unsupported command"):
                director.perform({"op": "publish", "action_id": action_id}, "task", "context")
            for command in ({"op": "inspect"},
                            {"op": "abort", "action_id": "abort-1"}):
                with self.subTest(op=command["op"]), self.assertRaisesRegex(
                        harness.Rejected, "original factory Task"):
                    director.perform(command, "unbound-task", "context")
        finally:
            harness.CURRENT_ACTOR.reset(token)
        with self.assertRaisesRegex(harness.Rejected, "authenticated caller required"):
            director.perform({"op": "start", "action_id": "anonymous", "inputs": {}},
                             "task", "context")
        self.assertEqual(director.module.runner.started, [])

    def test_publication_activates_without_starting_runner(self):
        template_path = ROOT / "definitions" / "report-template.json"
        health = {name: {"identity": "fixture-" + name} for name in REPORT_NAMES}
        bindings = report_bindings(health, 45780)
        contracts = {name: {"name": name, "role": report_role(name),
                            "capability": REPORT_CAPABILITIES[name]}
                     for name in REPORT_NAMES}
        catalog = self.director.module.catalog
        for name, value in (("approved_bindings", bindings),
                            ("contracts", contracts),
                            ("quality_policy", REPORT_QUALITY_POLICY)):
            (catalog / f"{name}.json").write_text(json.dumps(value))
        template = json.loads(template_path.read_text())
        packet = json.loads((ROOT / "packets" / "exo-qualification-2026-09-23" /
                             "packet.json").read_text())
        package = materialize(template, bindings, evidence_packet=packet)
        decision = approve(package, approver="fixture-policy",
                           policy={"mode": "auto", "approved_bindings": bindings})
        with self.assertRaisesRegex(ValueError, "approved decision"):
            self.director.module.publish(package, label="rejected",
                                         approval={**decision, "status": "pending"})
        self.assertEqual(self.director.module.runner.built, [])
        published = self.director.module.publish(package, label="v1", approval=decision)
        active = self.director.module.publications.active()
        self.assertEqual(active["manifest_digest"], published["manifest_digest"])
        self.assertEqual(active["label"], "v1")
        self.assertEqual(active["package_digest"], decision["package_digest"])
        self.assertEqual(self.director.module.runner.built, [SRC])
        self.assertEqual(self.director.module.runner.started, [])
        self.assertFalse((self.home / "runner").exists())

    def test_accepted_report_projection_and_abort(self):
        store = harness.FactoryTaskStore(self.director)
        record = {"run_id": "run", "label": "v1", "manifest_digest": "manifest",
                  "package_digest": "package", "build_id": "build",
                  "run_inputs_digest": "inputs", "authorized_actor": "fixture-operator"}
        report = {"kind": "verified_report@1", "revision": "r2", "packet_digest": "packet",
                  "markdown": "# Accepted report\n", "claims": []}
        accepted = {"revision": "r2", "sha256": "a" * 64,
                    "content": json.dumps(report)}
        result = {"status": "accepted", "artifact": accepted,
                  "acceptance": {"sha256": "a" * 64},
                  "receipt": {"sha256": "a" * 64}}
        task = store._task("task", "context", record,
                           {"state": "completed", "status": {}, "result": result})
        self.assertEqual(len(task.artifacts), 1)
        self.assertEqual(task.artifacts[0].artifact_id, "a" * 64)
        self.assertEqual(task.artifacts[0].parts[0].text, "# Accepted report\n")
        self.assertEqual(task.artifacts[0].parts[0].media_type, "text/markdown")
        self.assertEqual(a2a_v1_server.part_data(task.artifacts[0].parts[1])["packet_digest"],
                         "packet")
        aborted = store._task("task", "context", record,
                              {"state": "completed", "status": {},
                               "result": {"status": "aborted"}})
        self.assertEqual(len(aborted.artifacts), 0)

    def test_inspection_returns_only_semantic_wait_fields(self):
        raw = {"phase": "awaiting-director", "current_revision": "r3",
               "current_sha256": "a" * 64, "repair_count": 2, "max_repairs": 2,
               "quality_verdict": {"findings": [{"problem": "uncited claim"}]},
               "deadline": 123.0, "token": "hidden", "graph": {"secret": "hidden"},
               "owner_epoch": 4, "run_inputs": {"question": "hidden"}}
        raw.update(decision_actor="director-id", permitted_actions=["abort", "escalate"],
                   applied_decisions={"decision-1": "escalate-recorded"})
        def fake_sync(coroutine):
            coroutine.close()
            return raw
        with patch.object(self.director, "task_binding", return_value=("run", "context")), \
             patch.object(self.director, "run_record", return_value={"closed": 0}), \
             patch.object(harness, "sync", side_effect=fake_sync):
            observed = self.director.inspect_bound_run("task")
        self.assertEqual(observed, {"run_id": None, "node": None,
            "phase": "awaiting-director",
            "current_revision": "r3", "current_sha256": "a" * 64,
            "repair_count": 2, "max_repairs": 2,
            "quality_findings": [{"problem": "uncited claim"}], "wait_started_at": None,
            "wait_deadline": 123.0,
            "decision_actor": "director-id", "permitted_actions": ["abort", "escalate"],
            "applied_decisions": {"decision-1": "escalate-recorded"}})

    def test_execution_timeout_covers_published_director_and_human_waits(self):
        package = {"root": {"nodes": {
            "wait": {"type": "director_wait", "human": {
                "actor": "operator-7", "timeout_seconds": 3600}}}},
            "children": {"child": {"nodes": {
                "second": {"type": "director_wait"}}}}}
        self.assertEqual(harness._workflow_execution_timeout_seconds(package, 900),
                         3 * ((900 + 3600) + 900) + 600)

    def test_human_wait_task_projection_keeps_original_binding_and_safe_actions(self):
        store = harness.FactoryTaskStore(self.director)
        task = store._task("original-task", "original-context", {
            "run_id": "run-human", "label": "pinned", "manifest_digest": "m" * 64,
            "package_digest": "p" * 64, "build_id": "build-1",
            "run_inputs_digest": "d" * 64}, {
                "state": "input-required", "status": {
                    "phase": "awaiting-child", "child_id": "run-human:child:abc"},
                "decision_status": {"phase": "awaiting-human",
                    "decision_actor": "fixture-observer", "permitted_actions": ["abort"],
                    "deadline": 1234.5,
                    "applied_decisions": {"escalate-1": "escalate-recorded",
                                          "ignored": {"secret": True}}},
                "result": None, "incident": None})
        self.assertEqual(task.id, "original-task")
        self.assertEqual(task.context_id, "original-context")
        self.assertEqual(task.status.state, harness.task_state("input-required"))
        self.assertEqual(a2a_v1_server.part_data(task.status.message.parts[0]), {"director_wait": {
            "phase": "awaiting-human", "child_id": "run-human:child:abc",
            "decision_actor": "fixture-observer", "permitted_actions": ["abort"],
            "deadline": 1234.5, "applied_decisions": {"escalate-1": "escalate-recorded"}}})

    def test_director_perform_binds_human_abort_to_authenticated_principal(self):
        run_id = "run-human"
        with self.director.connect() as db:
            db.execute("INSERT INTO runs (run_id,task_id,context_id,package_digest,manifest_digest,"
                       "build_id,label,run_inputs_json,run_inputs_digest,authorized_actor,"
                       "input_authority_json,closed) VALUES (?,?,?,?,?,?,?,?,?,?,?,0)",
                       (run_id,"original-task","original-context","p"*64,"m"*64,
                        "build-1","pinned","{}","d"*64,"fixture-operator","{}"))
            db.execute("INSERT INTO aliases VALUES (?,?,?)",
                       ("original-task",run_id,"original-context"))
        human_wait = {"phase": "awaiting-human", "decision_actor": "fixture-observer",
            "permitted_actions": ["abort"], "current_revision": "r1",
            "current_sha256": "a"*64, "applied_decisions": {}}
        with patch.object(self.director, "inspect_bound_run", return_value=human_wait), \
             patch.object(self.director, "ensure_runner"), \
             patch.object(self.director, "_director_decision", new_callable=AsyncMock,
                          return_value={"command_id":"human-abort","action":"abort",
                              "lifecycle":"applied","outcome":"abort-recorded"}) as update:
            actor = harness.CURRENT_ACTOR.set("fixture-observer")
            try:
                result = self.director.perform({"op":"abort","action_id":"human-abort",
                    "revision":"r1","sha256":"a"*64},"original-task","original-context")
            finally:
                harness.CURRENT_ACTOR.reset(actor)
            self.assertEqual(result["outcome"], "abort-recorded")
            self.assertEqual(update.await_args.args[-1], "fixture-observer")

            actor = harness.CURRENT_ACTOR.set("fixture-operator")
            try:
                with self.assertRaisesRegex(harness.Rejected, "not authorized"):
                    self.director.perform({"op":"abort","action_id":"wrong-human",
                        "revision":"r1","sha256":"a"*64},
                        "original-task","original-context")
            finally:
                harness.CURRENT_ACTOR.reset(actor)
            self.assertEqual(update.await_count, 1)

    def test_temporal_human_update_uses_pinned_actor_and_live_deadline(self):
        child_status = {"phase": "awaiting-human", "decision_actor": "fixture-observer",
            "permitted_actions": ["abort"], "current_revision": "r1",
            "current_sha256": "a" * 64, "owner_epoch": self.director.incarnation,
            "deadline": time.time() + 60, "run": "run-human",
            "definition_digest": "f" * 64, "applied_decisions": {}}
        updates = []

        class ParentHandle:
            async def query(self, _query):
                return {"child_id": "run-human:child:abc"}

        class ChildHandle:
            async def query(self, _query):
                return dict(child_status)

            async def execute_update(self, _update, command):
                updates.append(command)
                return "abort-recorded"

        class Client:
            def get_workflow_handle(self, workflow_id):
                return ParentHandle() if workflow_id == "run-human" else ChildHandle()

        with patch.object(self.director, "client", new_callable=AsyncMock,
                          return_value=Client()):
            result = harness.sync(self.director._director_decision(
                "run-human", "human-abort-1", "r1", "a" * 64,
                "abort", "fixture-observer"))
            self.assertEqual(result["outcome"], "abort-recorded")
            self.assertEqual(updates[0]["actor"], "fixture-observer")
            with self.assertRaisesRegex(harness.Rejected, "decision actor"):
                harness.sync(self.director._director_decision(
                    "run-human", "wrong-human-2", "r1", "a" * 64,
                    "abort", "fixture-operator"))
            self.assertEqual(len(updates), 1)


class AgentModeTests(unittest.TestCase):
    def test_a2a_agent_mode_survives_restart_without_runner(self):
        state = Path(tempfile.mkdtemp(prefix="exo-proto-harness-test-", dir="/tmp"))
        home = state / "home"
        instance = home / "instances" / "agent"
        harness.init_instance(instance, name="agent-test", mode="agent", port=44875,
                              home=home)
        base = "http://127.0.0.1:44875"

        def get_json(path):
            with urllib.request.urlopen(base + path, timeout=2) as response:
                return json.load(response)

        def launch():
            # A stale server left on the fixed port would answer the card for
            # this launch and mask the incarnation under test.
            with socket.socket() as probe:
                if probe.connect_ex(("127.0.0.1", 44875)) == 0:
                    self.fail("port 44875 is already serving; stop the stale agent first")
            log = (state / "agent.log").open("a")
            try:
                process = subprocess.Popen(
                    [sys.executable, "-B", str(SRC / "harness.py"), "serve",
                     "--instance-dir", str(instance)],
                    stdout=log, stderr=subprocess.STDOUT)
            finally:
                log.close()
            try:
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    if process.poll() is not None:
                        self.fail(f"agent server exited during startup; see {state / 'agent.log'}")
                    try:
                        card = get_json("/.well-known/agent-card.json")
                        return process, card["capabilities"]["extensions"][0]["params"]
                    except (urllib.error.URLError, TimeoutError, ValueError):
                        time.sleep(0.1)
                self.fail(f"agent server did not serve its card; see {state / 'agent.log'}")
            except BaseException:
                stop(process)
                raise

        def stop(process):
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)

        process = None
        try:
            process, first = launch()
            card = get_json("/.well-known/agent-card.json")
            self.assertEqual([skill["id"] for skill in card["skills"]], ["capability"])
            self.assertNotIn("verified-research@1", [skill["id"] for skill in card["skills"]])
            brief = assignment("run-agent-mode", "definition-agent-mode",
                               "source_evidence")["brief"]
            task = long_client.send_async(base, brief, message_id="agent-mode-message",
                                          context_id="agent-mode-context")
            # The work product itself: one text Part, no envelope or author echo.
            self.assertEqual(task["artifacts"][0]["parts"],
                             [{"text": "fixture-result:" + brief, "mediaType": "text/plain"}])
            self.assertEqual(task["metadata"], {"agent_identity": first["identity"]})
            again = long_client.send_async(base, brief, message_id="agent-mode-message",
                                           context_id="agent-mode-context")
            self.assertEqual(again["id"], task["id"])
            self.assertFalse((home / "runner").exists())
            database = instance / "agent-state" / "harness.sqlite3"

            def incarnation():
                with sqlite3.connect(database) as db:
                    return db.execute("SELECT incarnation FROM identity").fetchone()[0]

            before = incarnation()
            stop(process)
            process = None
            process, second = launch()
            self.assertEqual(second["identity"], first["identity"])
            self.assertEqual(incarnation(), before + 1)
            self.assertEqual(long_client.get_task(base, task["id"])["id"], task["id"])
            self.assertFalse((home / "runner").exists())
        finally:
            if process is not None:
                stop(process)


class SingleFactoryCheckerTests(unittest.TestCase):
    def test_missing_evidence_fails_closed(self):
        checks = check_evidence({"provider": "scripted"})
        self.assertEqual(tuple(checks), CHECK_IDS)
        self.assertTrue(all(not result["pass"] for result in checks.values()))

    def test_induced_stimulus_and_model_decided_abort_require_decisive_records(self):
        route = {"run_id": "run-2", "child_run_id": "run-2:child", "stimulus_log": [
            {"run_id": "run-2:child", "revision": "r1",
            "task_id": "synth-task", "sha256_after": "a" * 64,
            "planted_text": "unsupported", "claim_id": "C4"}],
            "stimulus_absent_from_factory": True}
        evidence = {"provider": "scripted", "routes": {"2": route},
            "agents": {"synthesizer": {"tasks": [{"action_id": "run-2:child:synthesize:r1",
                "task_id": "synth-task", "artifact": {"revision": "r1", "sha256": "a" * 64,
                    "content": '{"markdown":"unsupported"}'}}]}}}
        self.assertFalse(check_evidence(evidence)["R2-a"]["pass"])
        route["stimulus_log"][0]["sha256_after"] = "b" * 64
        self.assertFalse(check_evidence(evidence)["R2-a"]["pass"])
        route["stimulus_log"][0]["sha256_after"] = "a" * 64
        route["stimulus_absent_from_factory"] = False
        self.assertFalse(check_evidence(evidence)["R2-a"]["pass"])
        evidence["agents"]["quality"] = {"tasks": [{"action_id": "run-2:child:quality:r1",
            "live": False, "verdict": {"candidate": {"revision": "r1"},
                "accepted": False, "decided_by": "model", "findings": [{
                    "severity": "blocking", "problem": "unsupported claim", "claim_id": "C4"}]}}]}
        self.assertEqual(check_evidence(evidence)["R2-b"]["behavior"],
                         "scripted route control")

        wait = {"run_id": "run-3", "follow_up_text": FOLLOW_UP,
                "follow_up_original_task": True,
                "child_status": {"current_revision": "r3", "current_sha256": "a" * 64},
                "task": {"status": {"state": "TASK_STATE_COMPLETED"}}, "result_status": "aborted",
                "director_calls": [
                    {"tool": "inspect_run", "accepted": 1},
                    {"tool": "decide_wait", "accepted": 1, "model_kind": "synthetic", "arguments":
                        {"action": "abort", "revision": "r3", "sha256": "a" * 64}}]}
        evidence["routes"]["3"] = wait
        self.assertFalse(check_evidence(evidence)["R3-d"]["pass"])
        wait["director_calls"].reverse()
        self.assertFalse(check_evidence(evidence)["R3-d"]["pass"])

    def test_exact_release_and_pinned_version_checks(self):
        sha = "b" * 64
        data = {"revision": "r1", "sha256": sha, "packet_digest": "packet",
                "acceptance": {"revision": "r1", "sha256": sha},
                "release_receipt": {"revision": "r1", "sha256": sha}}
        report = {"run_id": "parent", "child_run_id": "child",
                  "task": {"artifacts": [{"artifactId": sha,
                      "parts": [{"text": "# Report"},
                      {"data": data}]}]},
                  "usefulness": {"ok": True, "reasons": [], "sections_present": True},
                  "report_path": "/tmp/report.md"}
        content = {"claims": [{"evidence": ["E1"]}, {"evidence": ["E2"]},
                              {"evidence": ["E1", "E2"]}]}
        evidence = {"provider": "scripted", "routes": {"1": report},
                    "packet_ids": ["E1", "E2"],
                    "agents": {"synthesizer": {"tasks": [{"action_id": "child:synthesize:r1",
                        "artifact": {"revision": "r1", "sha256": sha,
                                     "content": json.dumps(content)}}]}},
                    "releases": [{"run_id": "child", "revision": "r1", "sha256": sha,
                                  "accepted_effect_count": 1}]}
        self.assertFalse(check_evidence(evidence)["R1-d"]["pass"])
        self.assertEqual(check_evidence(evidence)["R1-d"]["semantic_reading"],
                         "pending-orchestrator-reading")
        evidence["releases"][0]["sha256"] = "c" * 64
        self.assertFalse(check_evidence(evidence)["R1-d"]["pass"])

        version = {"manifest_digest": "m", "package_digest": "p", "build_id": "b",
                   "versioning_behavior": "PINNED"}
        evidence["routes"] = {str(n): {"workflows": [dict(version), dict(version)],
            "caller_messages": [{"parts": [{"text": "question"}]}]}
            for n in (1, 2, 3)}
        self.assertFalse(check_evidence(evidence)["SF-3"]["pass"])
        evidence["routes"]["3"]["workflows"][1]["build_id"] = "different"
        self.assertFalse(check_evidence(evidence)["SF-3"]["pass"])


if __name__ == "__main__":
    unittest.main()
