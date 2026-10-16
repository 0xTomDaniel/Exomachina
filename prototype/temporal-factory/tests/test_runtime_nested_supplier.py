"""Focused no-provider Runtime tests for a factory acting as a plain A2A agent service.

With the nested-supplier entry enabled, a factory accepts an ordinary A2A v1
Message from any client: one brief Part carrying its run inputs. Its Agent Card
declares no extension, callers send no factory identifiers, and the completed
Task returns the accepted work product as an ordinary artifact (A2A decisions 7
and 9). Correlation stays with the caller (taskId/contextId); the receiving
factory derives its own action and run identity from the A2A messageId.
"""
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
from supplier_protocol import MAX_CANONICAL_INPUT_BYTES  # noqa: E402


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


def brief(question: str = "What qualified?") -> list[dict]:
    """A plain A2A brief Part: the supplier factory's run inputs, nothing else."""
    return [{"data": {"question": question}, "mediaType": "application/json"}]


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
        self.package = {"root": self.root, "bindings": {}, "children": {}, "run_inputs": {
            "question": {"type": "string", "required": True, "source": "caller",
                         "allowed_actors": ["fixture-operator"], "may_affect_acceptance": False}}}
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

    def supply(self, parts=None, *, task_id="received-child-task",
               context_id="received-child-context", message_id="message-1",
               actor="fixture-operator"):
        token = harness.CURRENT_ACTOR.set(actor)
        try:
            return self.director.supply(parts if parts is not None else brief(),
                                        task_id, context_id, message_id)
        finally:
            harness.CURRENT_ACTOR.reset(token)

    def test_accepts_and_replays_a_plain_message_on_its_original_task(self):
        first = self.supply()
        replay = self.supply()
        self.assertFalse(first["replayed"])
        self.assertTrue(replay["replayed"])
        self.assertEqual(first["run_id"], replay["run_id"])
        # The run identity is the receiving factory's own, derived from the
        # caller's A2A messageId; no caller-chosen run id exists.
        self.assertTrue(first["run_id"].startswith(self.director.identity + "."))
        self.assertEqual(self.director.task_binding("received-child-task"),
                         (first["run_id"], "received-child-context"))
        run = self.director.run_record(first["run_id"])
        self.assertEqual((run["task_id"], run["context_id"]),
                         ("received-child-task", "received-child-context"))
        self.assertEqual(json.loads(run["run_inputs_json"]), {"question": "What qualified?"})
        self.assertTrue(self.director.supplied_run(first["run_id"]))
        self.assertEqual(self.start_mock.await_count, 2)
        # A JSON text brief is the same request.
        other = self.supply([{"text": json.dumps({"question": "What qualified?"}),
                              "mediaType": "application/json"}],
                            task_id="t2", context_id="c2", message_id="message-2")
        self.assertNotEqual(other["run_id"], first["run_id"])

    def test_message_id_reuse_never_starts_a_second_run(self):
        first = self.supply()
        with self.assertRaisesRegex(harness.Rejected, "different Message"):
            self.supply(brief("Another question?"))
        with self.assertRaisesRegex(harness.Rejected, "another Task"):
            self.supply(task_id="different-received-task")
        with self.assertRaisesRegex(harness.Rejected, "another Task"):
            self.supply(context_id="different-received-context")
        # Another caller's identical messageId is its own request.
        with self.assertRaisesRegex(harness.Rejected, "already bound"):
            self.supply(actor="fixture-operator-2")
        self.assertEqual(self.start_mock.await_count, 1)
        self.assertEqual(self.director.unfinished(), [first["run_id"]])

    def test_factory_identifiers_from_a_caller_are_not_bindings(self):
        """The retired factory-to-factory envelope is refused as run inputs."""
        envelope = {"op": "nested_factory", "action_id": "a", "run_id": "caller-run",
                    "definition_digest": harness.digest(self.root), "parent_task_id": "p",
                    "parent_run_id": "p", "parent_definition_digest": "b" * 64,
                    "parent_assignment_id": "p", "parent_attempt_id": "p",
                    "assignment_id": "c", "attempt_id": "c", "payload": {"inputs": {}}}
        with self.assertRaisesRegex(harness.Rejected, "run input|unknown|not declared|invalid"):
            self.supply([harness.a2a_v1.data_part(envelope)])
        with self.assertRaisesRegex(harness.Rejected, "exactly one brief Part"):
            self.supply([*brief(), *brief()])
        self.assertEqual(self.director.unfinished(), [])
        with self.assertRaisesRegex(harness.Rejected, "unknown run"):
            self.director.run_record("caller-run")

    def test_no_active_publication_fails_closed(self):
        self.active_mock.side_effect = FileNotFoundError("no publication")
        with self.assertRaisesRegex(harness.Rejected, "active publication is unavailable"):
            self.supply()
        self.assertEqual(self.director.unfinished(), [])
        self.assertEqual(self.ensure_mock.call_count, 0)

    def test_protocol_input_bounds_reject_before_publication_or_runner_access(self):
        with self.assertRaisesRegex(harness.Rejected, "canonical bytes"):
            self.supply(brief("x" * MAX_CANONICAL_INPUT_BYTES))
        self.active_mock.assert_not_called()
        self.ensure_mock.assert_not_called()
        self.assertEqual(self.director.unfinished(), [])

    def test_a2a_executor_routes_a_plain_json_brief_to_the_supplier_entry(self):
        class TaskReader:
            async def get(self, task_id):
                return {"task_id": task_id, "kind": "synthetic-task-projection"}

        event_queue = SimpleNamespace(enqueue_event=AsyncMock())
        executor = harness.HarnessExecutor(
            self.director, TaskReader(), allow_structured_commands=True)
        context = SimpleNamespace(
            task_id="received-child-task", context_id="received-child-context",
            message=SimpleNamespace(message_id="message-1", parts=[
                harness.text_part(json.dumps({"question": "What qualified?"}),
                                  "application/json")]))
        actor = harness.CURRENT_ACTOR.set("fixture-operator")
        try:
            asyncio.run(executor.execute(context, event_queue))
        finally:
            harness.CURRENT_ACTOR.reset(actor)
        event_queue.enqueue_event.assert_awaited_once_with({
            "task_id": "received-child-task", "kind": "synthetic-task-projection"})
        self.assertEqual(len(self.director.unfinished()), 1)

    def test_completed_task_is_an_ordinary_artifact_without_factory_metadata(self):
        accepted = self.supply()
        record = self.director.run_record(accepted["run_id"])
        content = '{"kind":"verified_report@1","markdown":"Actual accepted bytes"}'
        accepted_sha = hashlib.sha256(content.encode("utf-8")).hexdigest()
        result = {"status": "accepted", "artifact": {
            "content": content, "revision": "accepted-r7", "sha256": accepted_sha}}
        task = harness.FactoryTaskStore(self.director)._task(
            "received-child-task", "received-child-context", record,
            {"state": "completed", "status": {}, "result": result, "incident": None})
        wire = harness.a2a_v1.normalize_numbers(MessageToDict(task))
        self.assertEqual((wire["id"], wire["contextId"]),
                         ("received-child-task", "received-child-context"))
        self.assertNotIn("metadata", wire, "no factory concept crosses to a plain client")
        self.assertEqual(wire["artifacts"], [{"artifactId": accepted_sha, "parts": [
            {"text": content, "mediaType": "application/json"}]}])
        encoded = json.dumps(wire)
        for leaked in (record["run_id"], self.director.identity, "definition_digest",
                       "manifest_digest", "assignment_id", "parent_"):
            self.assertNotIn(leaked, encoded)
        # An ordinary (non-supplier) run keeps its caller-facing projection.
        normal = {"op": "start", "action_id": "ordinary-action",
                  "inputs": {"question": "What qualified?"}}
        token = harness.CURRENT_ACTOR.set("fixture-operator")
        try:
            self.director.perform(normal, "normal-task", "normal-context")
        finally:
            harness.CURRENT_ACTOR.reset(token)
        self.assertFalse(self.director.supplied_run(self.director.run_id_for("ordinary-action")))

    def test_card_declares_no_extension_and_serves_no_contract_route(self):
        enabled_app = harness.create_app(self.instance, usage_broker=EmptyUsageBroker())
        async def enabled_http():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=enabled_app),
                                         base_url="http://test") as client:
                card = (await client.get("/.well-known/agent-card.json")).json()
                contract = await client.get("/contract", headers={"Authorization": harness.TOKEN})
                # A plain v1 send activating no extension reaches the factory.
                body = harness.a2a_v1.rpc(harness.a2a_v1.SEND_MESSAGE, harness.a2a_v1.send_params(
                    harness.a2a_v1.user_message([harness.a2a_v1.data_part(
                        {"question": "What qualified?"})]), return_immediately=True))
                sent = (await client.post("/", json=body, headers={
                    "Authorization": harness.TOKEN, **harness.a2a_v1.headers()})).json()
                return card, contract.status_code, sent
        card, contract_status, sent = asyncio.run(enabled_http())
        self.assertNotIn("extensions", card["capabilities"])
        self.assertEqual(contract_status, 404)
        self.assertNotIn("error", sent, sent)
        self.assertNotIn("urn:exomachina", json.dumps(card))
        self.assertFalse(hasattr(harness, "A2A_ACTION_EXTENSION_URI"))

        disabled_instance = self.home / "instances" / "ordinary"
        disabled_config = harness.init_instance(
            disabled_instance, name="ordinary-test", mode="factory", port=44892, home=self.home)
        self.assertNotIn("nested_supplier_enabled", disabled_config)
        disabled_director = harness.Director(disabled_instance, disabled_config)
        self.assertFalse(disabled_director.nested_supplier_enabled)
        with self.assertRaisesRegex(harness.Rejected, "does not accept nested-supplier"):
            token = harness.CURRENT_ACTOR.set("fixture-operator")
            try:
                disabled_director.supply(brief(), "disabled-task", "disabled-context", "m")
            finally:
                harness.CURRENT_ACTOR.reset(token)
        with self.assertRaisesRegex(ValueError, "explicit boolean"):
            harness.init_instance(self.home / "instances" / "invalid-opt-in",
                name="invalid-opt-in", mode="factory", port=44893, home=self.home,
                nested_supplier_enabled=1)


if __name__ == "__main__":
    unittest.main()
