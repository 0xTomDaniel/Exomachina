"""Factory A2A caller boundary and public Task projection."""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from a2a.types import Task, TaskStatus
from fastapi.testclient import TestClient

SRC = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC))

import director_agent  # noqa: E402
import harness  # noqa: E402


AUTH = {"Authorization": "Bearer fixture-token"}
ERROR = "factory caller messages must contain text parts only"


class StubRunner:
    def __init__(self, home, *, port_base=None, member_base=None):
        pass

    def is_running(self):
        return False


def send(parts, *, task_id=None, context_id=None):
    message = {"kind": "message", "role": "user", "messageId": str(uuid4()),
               "parts": parts}
    if task_id:
        message["taskId"] = task_id
    if context_id:
        message["contextId"] = context_id
    return {"jsonrpc": "2.0", "id": str(uuid4()), "method": "message/send",
            "params": {"message": message, "configuration": {"blocking": False}}}


class FactoryBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.state = Path(tempfile.mkdtemp(prefix="exo-sf-boundary-", dir="/tmp"))
        self.instance = self.state / "instances" / "factory"
        runner_patch = patch.object(harness, "Runner", StubRunner)
        runner_patch.start()
        self.addCleanup(runner_patch.stop)

    def app(self, *, legacy=False, text_model=False):
        config = harness.init_instance(self.instance, name="factory", mode="factory",
                                       port=44872, home=self.state,
                                       legacy_structured_commands=legacy)
        if text_model:
            config["director_model"] = {"provider": "synthetic-loopback"}
            (self.instance / "instance.json").write_text(json.dumps(config))
        return harness.create_app(self.instance)

    def counts(self):
        with sqlite3.connect(self.instance / "director.sqlite3") as db:
            return tuple(db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                         for table in ("runs", "commands", "director_tool_calls"))

    def test_structured_start_and_abort_are_rejected_before_director(self):
        app = self.app()
        with patch.object(harness.Director, "invoke", side_effect=AssertionError("invoked")), \
             patch.object(harness.Director, "perform", side_effect=AssertionError("performed")), \
             TestClient(app) as client:
            for parts in (
                [{"kind": "data", "data": {"op": "start", "action_id": "a",
                                            "inputs": {"question": "report?"}}}],
                [{"kind": "data", "data": {"op": "abort", "action_id": "b",
                                            "revision": "r1", "sha256": "a" * 64}}],
                [{"kind": "text", "text": "Start a report"},
                 {"kind": "data", "data": {"op": "inspect"}}],
            ):
                result = client.post("/", json=send(parts), headers=AUTH).json()["result"]
                self.assertEqual(result["parts"][0]["data"], {"error": ERROR})
                self.assertEqual(self.counts(), (0, 0, 0))

    def test_text_reaches_director_turn_and_starts(self):
        app = self.app(text_model=True)

        async def turn_run(turn, brief, model):
            self.assertEqual(brief, "Research this report")
            result = turn.call("start_research", {"question": brief})
            self.assertTrue(result["ok"])
            return {"accepted": ["start"]}

        async def no_temporal_start(director, run, package, publication):
            pass

        with patch.object(director_agent, "selected_model", return_value=(object(), "synthetic")), \
             patch.object(director_agent.DirectorTurn, "run", turn_run), \
             patch.object(harness.PublicationStore, "active", return_value={
                 "package_digest": "package", "manifest_digest": "manifest",
                 "build_id": "build", "label": "v1"}), \
             patch.object(harness.FactoryModule, "package", return_value={"run_inputs": {}}), \
             patch.object(harness, "authorize_run_inputs", side_effect=lambda spec, inputs, actor:
                          (inputs, {"question": actor})), \
             patch.object(harness.Director, "ensure_runner", return_value={}), \
             patch.object(harness.Director, "_start", no_temporal_start), \
             TestClient(app) as client:
            result = client.post("/", json=send([{"kind": "text", "text": "Research this report"}]),
                                 headers=AUTH).json()["result"]
        self.assertEqual(result["kind"], "task")
        self.assertEqual(result["status"]["state"], "working")
        self.assertEqual(self.counts(), (1, 1, 1))
        with sqlite3.connect(self.instance / "director.sqlite3") as db:
            self.assertEqual(db.execute("SELECT op FROM commands").fetchone()[0], "start")

    def test_explicit_legacy_flag_keeps_structured_fixture_path(self):
        app = self.app(legacy=True)
        commands = []

        def perform(director, command, task_id, context_id):
            commands.append(command)
            return {"accepted_command": "start"}

        async def task_get(store, task_id, context=None):
            return Task(id=task_id, context_id="context", status=TaskStatus(state="working"))

        async def task_save(store, task, context=None):
            pass

        with patch.object(harness.Director, "perform", perform), \
             patch.object(harness.FactoryTaskStore, "get", task_get), \
             patch.object(harness.FactoryTaskStore, "save", task_save), \
             TestClient(app) as client:
            result = client.post("/", json=send([{"kind": "data", "data": {
                "op": "start", "action_id": "legacy-a",
                "inputs": {"question": "report?"}}}]), headers=AUTH).json()["result"]
        self.assertEqual(result["kind"], "task")
        self.assertEqual([command["op"] for command in commands], ["start"])

    def test_task_metadata_and_artifacts_have_no_actor_epoch_or_token(self):
        self.app()
        director = harness.Director(self.instance, harness.load_config(self.instance))
        record = {"run_id": "run", "label": "v1", "manifest_digest": "manifest",
                  "package_digest": "package", "build_id": "build",
                  "run_inputs_digest": "inputs", "authorized_actor": "fixture-operator"}
        report = {"kind": "verified_report@1", "revision": "r1", "packet_digest": "packet",
                  "markdown": "# Report\n"}
        accepted = {"revision": "r1", "sha256": "a" * 64, "content": json.dumps(report)}
        task = harness.FactoryTaskStore(director)._task("task", "context", record, {
            "state": "completed", "status": {}, "result": {"status": "accepted",
                "artifact": accepted, "acceptance": {"sha256": "a" * 64},
                "receipt": {"sha256": "a" * 64}}})
        visible = json.dumps(task.model_dump(mode="json"), sort_keys=True)
        for secret in ("authorized_input_actor", "fixture-operator", "incarnation",
                       "actor", "epoch", "token"):
            self.assertNotIn(secret, visible)


if __name__ == "__main__":
    unittest.main()
