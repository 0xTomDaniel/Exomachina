"""Focused no-provider Runtime acceptance tests for nested supplier envelopes."""
from __future__ import annotations

import hashlib
import asyncio
import json
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from google.protobuf.json_format import MessageToDict

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

import harness  # noqa: E402
from supplier_protocol import (MAX_CANONICAL_INPUT_BYTES, SUPPLIER_ECHO_FIELDS,
                               nested_factory_fingerprint)  # noqa: E402


class StubRunner:
    def __init__(self, home: Path, **_kwargs):
        self.address = "127.0.0.1:44542"

    def is_running(self):
        return False

    def ensure_started(self, *, reason, timeout=180):
        return {"reason": reason}

    def ensure_build(self, source_dir):
        return {"build_id": "build-test"}

    def wait_worker(self, build_id, timeout=60):
        return {"build_id": build_id}


class EmptyUsageBroker:
    def usage_journal(self):
        return self

    def list_measurements(self, **_filters):
        return []


def envelope(root: dict) -> dict:
    return {
        "op": "nested_factory", "action_id": "supplier-action-1",
        "run_id": "caller-selected-child-run", "definition_digest": harness.digest(root),
        "parent_task_id": "real-parent-task", "parent_run_id": "real-parent-run",
        "parent_definition_digest": "b" * 64,
        "parent_assignment_id": "parent-assignment-1",
        "parent_attempt_id": "parent-attempt-1",
        "assignment_id": "child-assignment-1", "attempt_id": "child-attempt-1",
        "payload": {"inputs": {}},
    }


class RuntimeNestedSupplierTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="exo-runtime-nested-supplier-", dir="/tmp")
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name) / "home"
        self.instance = self.home / "instances" / "supplier"
        self.runner_patch = patch.object(harness, "Runner", StubRunner)
        self.runner_patch.start()
        self.addCleanup(self.runner_patch.stop)
        self.config = harness.init_instance(
            self.instance, name="nested-supplier-test", mode="factory", port=44891,
            home=self.home, nested_supplier_enabled=True)
        self.director = harness.Director(self.instance, self.config)
        self.root = {"nodes": {}}
        self.publication = {"manifest_digest": "c" * 64, "package_digest": "d" * 64,
                            "build_id": "build-test", "label": "test", "closure": {}}
        self.package = {"root": self.root, "run_inputs": {}, "bindings": {}, "children": {}}
        self.active_patch = patch.object(self.director.module.publications, "active",
                                          return_value=self.publication)
        self.get_publication_patch = patch.object(
            self.director.module.publications, "get", return_value=self.publication)
        self.package_patch = patch.object(self.director.module, "package",
                                          return_value=self.package)
        self.active_mock = self.active_patch.start()
        self.get_publication_patch.start()
        self.package_patch.start()
        self.addCleanup(self.active_patch.stop)
        self.addCleanup(self.get_publication_patch.stop)
        self.addCleanup(self.package_patch.stop)
        self.start_patch = patch.object(self.director, "_start", new_callable=AsyncMock)
        self.ensure_patch = patch.object(self.director, "ensure_runner", return_value={})
        self.start_mock = self.start_patch.start()
        self.ensure_mock = self.ensure_patch.start()
        self.addCleanup(self.start_patch.stop)
        self.addCleanup(self.ensure_patch.stop)

    def call_nested(self, command=None, *, task_id="received-child-task",
                     context_id="received-child-context"):
        actor = harness.CURRENT_ACTOR.set("fixture-operator")
        try:
            return asyncio.run(self.director.invoke(
                command if command is not None else envelope(self.root),
                task_id, context_id))
        finally:
            harness.CURRENT_ACTOR.reset(actor)

    def test_accepts_and_replays_exact_binding_on_original_received_task(self):
        command = envelope(self.root)
        first = self.call_nested(command)
        replay = self.call_nested(command)
        self.assertEqual(first["run_id"], command["run_id"])
        self.assertFalse(first["replayed"])
        self.assertTrue(replay["replayed"])
        self.assertEqual(self.director.task_binding("received-child-task"),
                         (command["run_id"], "received-child-context"))
        run = self.director.run_record(command["run_id"])
        self.assertEqual(run["task_id"], "received-child-task")
        self.assertEqual(run["context_id"], "received-child-context")
        self.assertEqual(run["run_inputs_json"], "{}")
        stored = self.director.supplier_assignment(command["run_id"])
        self.assertEqual(stored["fingerprint"], nested_factory_fingerprint(command))
        self.assertEqual(stored["task_id"], "received-child-task")
        self.assertEqual(stored["context_id"], "received-child-context")
        self.assertEqual(stored["echo"], {key: command[key] for key in SUPPLIER_ECHO_FIELDS})
        self.assertEqual(self.start_mock.await_count, 2)
        self.assertTrue(all(call.args[0]["run_id"] == command["run_id"]
                            for call in self.start_mock.await_args_list))

    def test_a2a_executor_allows_only_the_opted_in_structured_entry(self):
        command = envelope(self.root)

        class TaskReader:
            async def get(self, task_id):
                return {"task_id": task_id, "kind": "synthetic-task-projection"}

        event_queue = SimpleNamespace(enqueue_event=AsyncMock())
        executor = harness.HarnessExecutor(
            self.director, TaskReader(), allow_structured_commands=True)
        context = SimpleNamespace(
            task_id="received-child-task", context_id="received-child-context",
            message=SimpleNamespace(message_id="message-1", parts=[
                harness.data_part(command)]))
        actor = harness.CURRENT_ACTOR.set("fixture-operator")
        try:
            asyncio.run(executor.execute(context, event_queue))
        finally:
            harness.CURRENT_ACTOR.reset(actor)
        event_queue.enqueue_event.assert_awaited_once_with({
            "task_id": "received-child-task", "kind": "synthetic-task-projection"})

    def test_payload_parent_task_context_and_action_reuse_conflict(self):
        original = envelope(self.root)
        self.call_nested(original)
        changed_payload = json.loads(json.dumps(original))
        changed_payload["payload"]["inputs"]["extra"] = "different"
        with self.assertRaisesRegex(harness.Rejected, "conflicts"):
            self.call_nested(changed_payload)
        changed_parent = json.loads(json.dumps(original))
        changed_parent["parent_attempt_id"] = "other-parent-attempt"
        with self.assertRaisesRegex(harness.Rejected, "conflicts"):
            self.call_nested(changed_parent)
        with self.assertRaisesRegex(harness.Rejected, "conflicts"):
            self.call_nested(original, task_id="different-received-task")
        with self.assertRaisesRegex(harness.Rejected, "conflicts"):
            self.call_nested(original, context_id="different-received-context")
        self.assertEqual(self.start_mock.await_count, 1)

    def test_active_definition_mismatch_and_normal_run_collision_fail_closed(self):
        command = envelope(self.root)
        command["definition_digest"] = "e" * 64
        with self.assertRaisesRegex(harness.Rejected, "active root"):
            self.call_nested(command)
        self.assertEqual(self.director.unfinished(), [])
        self.assertEqual(self.ensure_mock.call_count, 0)

        normal = {"op": "start", "action_id": "ordinary-action", "inputs": {}}
        actor = harness.CURRENT_ACTOR.set("fixture-operator")
        try:
            self.director.perform(normal, "normal-task", "normal-context")
        finally:
            harness.CURRENT_ACTOR.reset(actor)
        colliding = envelope(self.root)
        colliding["run_id"] = self.director.run_id_for("ordinary-action")
        with self.assertRaisesRegex(harness.Rejected, "already in use"):
            self.call_nested(colliding)

    def test_protocol_input_bounds_reject_before_publication_or_runner_access(self):
        command = envelope(self.root)
        command["payload"]["inputs"] = {"oversized": "x" * MAX_CANONICAL_INPUT_BYTES}
        with self.assertRaisesRegex(harness.Rejected, "canonical bytes"):
            self.call_nested(command)
        self.active_mock.assert_not_called()
        self.ensure_mock.assert_not_called()
        self.assertEqual(self.director.unfinished(), [])

    def test_task_metadata_and_single_artifact_echo_exact_accepted_report(self):
        command = envelope(self.root)
        self.call_nested(command)
        record = self.director.run_record(command["run_id"])
        content = '{"kind":"verified_report@1","markdown":"Actual accepted bytes"}'
        accepted_sha = hashlib.sha256(content.encode("utf-8")).hexdigest()
        result = {"status": "accepted", "artifact": {
            "content": content, "revision": "accepted-r7", "sha256": accepted_sha}}
        task = harness.FactoryTaskStore(self.director)._task(
            "received-child-task", "received-child-context", record,
            {"state": "completed", "status": {}, "result": result, "incident": None})
        self.assertEqual(task.id, "received-child-task")
        self.assertEqual(task.context_id, "received-child-context")
        self.assertEqual(task.metadata["agent_identity"], self.director.identity)
        for field in SUPPLIER_ECHO_FIELDS:
            self.assertEqual(task.metadata[field], command[field])
        self.assertEqual(len(task.artifacts), 1)
        self.assertEqual(len(task.artifacts[0].parts), 1)
        artifact = harness.a2a_v1.normalize_numbers(
            MessageToDict(task.artifacts[0].parts[0].data))
        self.assertEqual(artifact["content"], content)
        self.assertEqual(artifact["revision"], "accepted-r7")
        self.assertEqual(artifact["sha256"], accepted_sha)
        self.assertEqual(artifact["author"], self.director.identity)
        self.assertEqual({key: artifact[key] for key in SUPPLIER_ECHO_FIELDS},
                         {key: command[key] for key in SUPPLIER_ECHO_FIELDS})

    def test_card_contract_and_storage_are_opt_in(self):
        enabled_app = harness.create_app(self.instance, usage_broker=EmptyUsageBroker())
        async def enabled_http():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=enabled_app),
                                         base_url="http://test") as client:
                card = (await client.get("/.well-known/agent-card.json")).json()
                extension = card["capabilities"]["extensions"][0]
                contract = (await client.get("/contract", headers={
                    "Authorization": harness.TOKEN})).json()
                # The supplier extension is required: a v1 send that does not
                # activate it is refused before the Director sees it.
                body = harness.a2a_v1.rpc(harness.a2a_v1.SEND_MESSAGE, harness.a2a_v1.send_params(
                    harness.a2a_v1.user_message([harness.a2a_v1.data_part(envelope(self.root))]),
                    return_immediately=True))
                unactivated = (await client.post("/", json=body, headers={
                    "Authorization": harness.TOKEN, **harness.a2a_v1.headers()})).json()
                return card, extension, contract, unactivated
        card, extension, contract, unactivated = asyncio.run(enabled_http())
        with self.subTest(required_extension_not_activated=True):
            self.assertIs(extension["required"], True)
            self.assertEqual(unactivated["error"]["code"], -32008)
            self.assertIn(harness.A2A_ACTION_EXTENSION_URI, unactivated["error"]["message"])
        with self.subTest(enabled_contract=True):
            self.assertEqual(extension["params"]["identity"], self.director.identity)
            self.assertEqual(extension["params"]["contract_digest"],
                             harness.agent_contract_digest(contract))
            self.assertEqual(contract["supplier_assignment_echo"],
                             harness.supplier_assignment_echo_declaration())
            self.assertEqual(contract["reconcile"], "opaque")

        disabled_instance = self.home / "instances" / "ordinary"
        disabled_config = harness.init_instance(
            disabled_instance, name="ordinary-test", mode="factory", port=44892, home=self.home)
        self.assertNotIn("nested_supplier_enabled", disabled_config)
        disabled_director = harness.Director(disabled_instance, disabled_config)
        self.assertFalse(disabled_director.nested_supplier_enabled)
        with disabled_director.connect() as db:
            tables = {row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertNotIn("supplier_assignments", tables)
        disabled_app = harness.create_app(disabled_instance, usage_broker=EmptyUsageBroker())
        async def disabled_http():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=disabled_app),
                                         base_url="http://test") as client:
                card = (await client.get("/.well-known/agent-card.json")).json()
                contract_response = await client.get("/contract", headers={
                    "Authorization": harness.TOKEN})
                return card, contract_response.status_code
        card, contract_status = asyncio.run(disabled_http())
        self.assertNotIn("extensions", card["capabilities"])
        self.assertEqual(contract_status, 404)
        with self.assertRaisesRegex(harness.Rejected, "not enabled"):
            actor = harness.CURRENT_ACTOR.set("fixture-operator")
            try:
                asyncio.run(disabled_director.invoke(
                    envelope(self.root), "disabled-task", "disabled-context"))
            finally:
                harness.CURRENT_ACTOR.reset(actor)
        with self.assertRaisesRegex(ValueError, "explicit boolean"):
            harness.init_instance(self.home / "instances" / "invalid-opt-in",
                name="invalid-opt-in", mode="factory", port=44893, home=self.home,
                nested_supplier_enabled=1)


if __name__ == "__main__":
    unittest.main()
