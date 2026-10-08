"""Synthetic checks for the optional service-wide execution admission gate."""
from __future__ import annotations

import asyncio
import hashlib
import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from uuid import uuid4
from unittest.mock import patch

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services"))
sys.path.insert(0, str(ROOT / "src"))
import model_agent  # noqa: E402
from admission import AdmissionConfigurationConflict  # noqa: E402


class SyntheticRole:
    def system_prompt(self, _capability):
        return "synthetic prompt"

    def user_prompt(self, brief):
        return json.dumps(brief)

    def scripted_reply(self, brief, _identity):
        return json.dumps({"revision": brief["revision"], "markdown": "synthetic",
                           "claims": []})

    def parse(self, text, brief, _identity):
        value = json.loads(text)
        if value["revision"] != brief["revision"]:
            raise ValueError("revision mismatch")
        return value

    def precheck(self, _brief, _identity):
        return None


def command(action_id: str, factory_id: str) -> dict:
    return {"op": "assign", "action_id": action_id,
            "run_id": "synthetic-run", "definition_digest": "synthetic-definition",
            "brief": model_agent.canonical({"revision": "r1"}),
            "factory_id": factory_id}


def service(state: Path, queue_db: Path, capacity: int = 2) -> model_agent.Service:
    return model_agent.Service(state, "synthesis", "report_synthesis@1", "scripted",
        model_agent.DEFAULT_MODEL_ID, False, {"synthesis": SyntheticRole()},
        admission_database=queue_db, execution_capacity=capacity)


class ModelAgentAdmissionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.services = []

    async def asyncTearDown(self):
        for item in self.services:
            monitor = item.capacity_monitor
            if monitor is not None:
                monitor.cancel()
                try:
                    await monitor
                except asyncio.CancelledError:
                    pass

    def roots(self):
        root = Path(tempfile.mkdtemp(prefix="exo-admission-synthetic-", dir="/tmp"))
        return root / "owner", root / "admission.sqlite3"

    def accept(self, owner: model_agent.Service, task_id: str, factory_id: str):
        return owner.ledger.accept(command(f"synthetic-action-{task_id}", factory_id),
                                   task_id, f"synthetic-context-{task_id}")

    async def wait_terminal(self, owner: model_agent.Service, task_ids: list[str]):
        end = time.monotonic() + 5
        while time.monotonic() < end:
            if all(owner.ledger.row(task_id)["state"] in {"completed", "failed"}
                   for task_id in task_ids):
                return
            await asyncio.sleep(0.01)
        self.fail("synthetic admitted Tasks did not reach terminal state")

    async def test_two_service_workers_and_two_factory_scopes_share_global_capacity(self):
        state, queue_db = self.roots()
        primary = service(state, queue_db, 2)
        secondary = service(state, queue_db, 2)
        self.services = [primary, secondary]
        factories = ["factory:west@v2", "factory-east_2"]
        tasks = [str(uuid4()) for _ in range(4)]
        for index, task_id in enumerate(tasks):
            self.accept(primary, task_id, factories[index // 2])

        gate = asyncio.Event()
        started = asyncio.Event()
        metrics = {"active": 0, "max_active": 0, "starts": 0}
        original = model_agent.ScriptedModel

        class BlockingSyntheticModel(original):
            async def stream(self, messages, tool_specs=None, system_prompt=None, **kwargs):
                metrics["starts"] += 1
                metrics["active"] += 1
                metrics["max_active"] = max(metrics["max_active"], metrics["active"])
                started.set()
                try:
                    await gate.wait()
                    async for event in super().stream(messages, tool_specs, system_prompt, **kwargs):
                        yield event
                finally:
                    metrics["active"] -= 1

        with patch.object(model_agent, "ScriptedModel", BlockingSyntheticModel):
            primary.recover()
            # A second Service object racing the same admitted Task cannot claim it again.
            secondary.schedule(tasks[0])
            end = time.monotonic() + 3
            while metrics["starts"] < 2 and time.monotonic() < end:
                await asyncio.sleep(0.01)
            self.assertEqual(metrics["starts"], 2)
            await asyncio.sleep(0.1)
            self.assertEqual(metrics["starts"], 2, "queued factory Tasks must not invoke the model")
            self.assertEqual(metrics["active"], 2)
            gate.set()
            await self.wait_terminal(primary, tasks)

        self.assertEqual(metrics["max_active"], 2)
        self.assertEqual(metrics["starts"], 4)
        self.assertEqual([primary.ledger.call_count(task_id) for task_id in tasks], [1, 1, 1, 1])
        self.assertTrue(all(primary.ledger.row(task_id)["factory_id"] == factories[index // 2]
                            for index, task_id in enumerate(tasks)))

    async def test_authenticated_capacity_projection_scopes_counts_and_unknown_get_writes_nothing(self):
        state, queue_db = self.roots()
        app = model_agent.create_app(state, 45761, role="synthesis",
            capability="report_synthesis@1", model_provider="scripted",
            roles={"synthesis": SyntheticRole()}, admission_database=queue_db,
            execution_capacity=2)
        owner = app.state.model_agent_service
        self.services = [owner]
        own_factory, other_factory = str(uuid4()), str(uuid4())
        unknown_factory = "unprovisioned:factory"
        ids = [str(uuid4()), str(uuid4())]
        for task_id, factory_id in zip(ids, (own_factory, other_factory)):
            self.accept(owner, task_id, factory_id)
            view = owner.admission_queue.enqueue(task_id, task_id=task_id)
            if view["state"] == "admitted":
                self.assertIsNotNone(owner.ledger.claim_execution(task_id))

        with patch.object(owner.admission_queue, "capacity_view",
                          side_effect=AssertionError("split capacity read")), \
                patch.object(owner.admission_queue, "list_requests",
                             side_effect=AssertionError("split request read")):
            coherent = owner.capacity_snapshot(own_factory)
        self.assertEqual((coherent["totals"]["admitted"], coherent["own"]["admitted"],
                          coherent["other"]["admitted"]), (2, 1, 1))

        before = hashlib.sha256(queue_db.read_bytes()).hexdigest()
        with TestClient(app) as client:
            response = client.get("/admission/capacity", params={"factory_id": own_factory},
                                  headers={"Authorization": model_agent.TOKEN})
            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertEqual(body["service_identity"], owner.ledger.identity)
            self.assertEqual(body["factory_id"], own_factory)
            self.assertEqual(body["capacity"], 2)
            self.assertEqual(body["totals"]["admitted"], 2)
            self.assertEqual(body["own"]["admitted"], 1)
            self.assertEqual(body["other"]["admitted"], 1)
            self.assertNotIn(other_factory, json.dumps(body))
            self.assertTrue(all(task_id not in json.dumps(body) for task_id in ids))

            unavailable = client.get("/admission/capacity", params={"factory_id": unknown_factory},
                                     headers={"Authorization": model_agent.TOKEN})
            self.assertEqual(unavailable.status_code, 404)
        after = hashlib.sha256(queue_db.read_bytes()).hexdigest()
        self.assertEqual(before, after, "unknown-scope GET must not mutate/create queue configuration")

    async def test_duplicate_worker_fence_and_restart_leave_inflight_task_held_without_resend(self):
        state, queue_db = self.roots()
        first = service(state, queue_db, 1)
        self.services = [first]
        factory_id, task_id = str(uuid4()), str(uuid4())
        self.accept(first, task_id, factory_id)
        first.admission_queue.enqueue(task_id, task_id=task_id)
        token = first.ledger.claim_execution(task_id)
        self.assertIsNotNone(token)
        call_id, _ = first.ledger.call_start(task_id, "synthetic-session", "scripted",
            model_agent.DEFAULT_MODEL_ID, False, execution_claim_token=token,
            require_execution_claim=True)
        self.assertGreater(call_id, 0)

        invoked = {"count": 0}
        original = model_agent.ScriptedModel

        class CountingSyntheticModel(original):
            async def stream(self, *args, **kwargs):
                invoked["count"] += 1
                async for event in super().stream(*args, **kwargs):
                    yield event

        restarted = service(state, queue_db, 1)
        self.services.append(restarted)
        with patch.object(model_agent, "ScriptedModel", CountingSyntheticModel):
            restarted.recover()
            restarted.schedule(task_id)
            await asyncio.sleep(0.2)
        self.assertEqual(invoked["count"], 0)
        self.assertEqual(restarted.ledger.row(task_id)["state"], "working")
        self.assertEqual(restarted.ledger.call_count(task_id), 1)
        self.assertEqual(restarted.ledger.execution_claim(task_id)["state"], "provider_started")
        self.assertEqual(restarted.admission_queue.get(task_id)["state"], "admitted")
        with self.assertRaises(AdmissionConfigurationConflict):
            service(state, queue_db, 2)

    async def test_capacity_has_no_default_and_readonly_mode_rejects_it(self):
        state, queue_db = self.roots()
        normal = model_agent.create_app(state, 45762, role="synthesis",
            capability="report_synthesis@1", model_provider="scripted",
            roles={"synthesis": SyntheticRole()})
        self.assertIsNone(normal.state.model_agent_service.admission_queue)
        self.assertFalse(queue_db.exists())
        with self.assertRaisesRegex(ValueError, "read-only usage mode cannot configure execution capacity"):
            model_agent.create_app(state, 45762, role="synthesis",
                capability="report_synthesis@1", read_only_usage=True,
                admission_database=queue_db, execution_capacity=1)
        with self.assertRaisesRegex(ValueError, "configured together"):
            model_agent.Service(state, "synthesis", "report_synthesis@1", "scripted",
                model_agent.DEFAULT_MODEL_ID, False, {"synthesis": SyntheticRole()},
                admission_database=queue_db)

        gated_state, gated_queue = self.roots()
        gated = model_agent.create_app(gated_state, 45763, role="synthesis",
            capability="report_synthesis@1", model_provider="scripted",
            roles={"synthesis": SyntheticRole()}, admission_database=gated_queue,
            execution_capacity=1)
        gated_owner = gated.state.model_agent_service
        self.services.append(gated_owner)
        missing_scope = {"op": "assign", "action_id": "synthetic-missing-scope",
                         "run_id": "synthetic-run", "definition_digest": "synthetic-definition",
                         "brief": model_agent.canonical({"revision": "r1"})}
        request = {"jsonrpc": "2.0", "id": "synthetic-request", "method": "message/send",
                   "params": {"message": {"role": "user", "messageId": str(uuid4()),
                       "contextId": "synthetic-context",
                       "parts": [{"kind": "data", "data": missing_scope}]},
                       "configuration": {"blocking": False}}}
        with TestClient(gated) as client:
            response = client.post("/", json=request,
                                   headers={"Authorization": model_agent.TOKEN})
        self.assertIn("error", response.json())
        self.assertEqual(gated_owner.ledger.task_rows(), [])
        self.assertEqual(gated_owner.admission_queue.list_requests(), [])
        unsafe = {**request, "id": "synthetic-unsafe-request",
                  "params": {**request["params"], "message": {**request["params"]["message"],
                      "parts": [{"kind": "data", "data": {**missing_scope,
                          "factory_id": "factory with spaces"}}]}}}
        with TestClient(gated) as client:
            unsafe_response = client.post("/", json=unsafe,
                                          headers={"Authorization": model_agent.TOKEN})
        self.assertIn("error", unsafe_response.json())
        self.assertEqual(gated_owner.ledger.task_rows(), [])


if __name__ == "__main__":
    unittest.main()
