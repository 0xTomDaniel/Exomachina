"""Synthetic tests for durable, safe, provider-reported model usage."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
from pathlib import Path
import sys
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services"))
sys.path.insert(0, str(ROOT / "src"))

from model_broker import (MeasurementConflict, ModelBroker, ModelUsageJournal, PiBrokerModel,
                          normalize_provider_usage, unavailable_usage)  # noqa: E402
import model_agent  # noqa: E402


import a2a_v1  # noqa: E402
import a2a_extensions  # noqa: E402

AUTH = {"Authorization": "Bearer fixture-token",
        **a2a_v1.headers([model_agent.EXTENSION_URI])}


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class AttachedBroker(ModelBroker):
    def __init__(self, socket_path: Path):
        self.socket = socket_path

    def ensure_started(self, *, reason: str, timeout: float = 30) -> dict:
        return {"signed_in": True, "pid": 1}


class FakeSocketBroker:
    def __init__(self, path: Path, *, text: str, usage: dict | None = None):
        self.path, self.text, self.usage = path, text, usage
        self.server = None
        self.requests = []

    async def start(self):
        self.server = await asyncio.start_unix_server(self.handle, path=str(self.path))

    async def close(self):
        self.server.close()
        await self.server.wait_closed()

    async def handle(self, reader, writer):
        request = json.loads(await reader.readline())
        self.requests.append(request)
        request_id = request["id"]
        for event in (
            {"type": "text_start", "contentIndex": 0},
            {"type": "text_delta", "contentIndex": 0, "delta": self.text},
            {"type": "text_end", "contentIndex": 0},
        ):
            writer.write((canonical({"id": request_id, "ev": event}) + "\n").encode())
        done = {"content": [{"type": "text", "text": self.text}], "stopReason": "stop"}
        if self.usage is not None:
            done["usage"] = self.usage
        writer.write((canonical({"id": request_id, "done": done}) + "\n").encode())
        await writer.drain()
        writer.close()
        await writer.wait_closed()


class ModelUsageJournalTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="exo-model-usage-")
        self.database = Path(self.directory.name) / "usage.sqlite3"
        self.journal = ModelUsageJournal(self.database)
        self.facts = {
            "model_call_id": "urn:exo:model-call:test:1",
            "service_identity": "service-test",
            "task_id": "task-1",
            "action_id": "action-1",
            "run_id": "run-1",
            "definition_digest": "digest-1",
            "provider": "synthetic-broker",
            "model_id": "synthetic-model-v1",
            "reasoning_effort": "xhigh",
        }

    def tearDown(self):
        self.directory.cleanup()

    def test_exact_provider_categories_restart_replay_and_conflict(self):
        normalized = normalize_provider_usage({
            "input": 0, "output": 12, "cacheRead": 3,
            "totalTokens": 15, "cost": {"total": 987654}, "credential": "discard-me",
        })
        row = self.journal.record(**self.facts, usage=normalized)
        self.assertEqual(row["usage"]["input_tokens"], {"value": 0, "status": "reported"})
        self.assertEqual(row["usage"]["output_tokens"], {"value": 12, "status": "reported"})
        self.assertEqual(row["usage"]["cache_read_tokens"], {"value": 3, "status": "reported"})
        self.assertEqual(row["usage"]["cache_write_tokens"],
                         {"value": None, "status": "unavailable"})
        self.assertEqual(row["usage"]["total_tokens"], {"value": 15, "status": "reported"})
        self.assertEqual(row["measurement_source"], "provider_reported")
        self.assertEqual(row["completeness"], "partial")
        self.assertNotIn("cost", canonical(row))
        self.assertNotIn("credential", canonical(row))
        self.assertNotIn("prompt", canonical(row))
        self.assertNotIn("response", canonical(row))

        reopened = ModelUsageJournal(self.database)
        replay = reopened.record(**self.facts, usage=normalized)
        self.assertEqual(replay, row)
        with self.assertRaises(MeasurementConflict):
            reopened.record(**self.facts, usage=normalize_provider_usage(
                {"input": 1, "output": 12, "cacheRead": 3, "totalTokens": 16}))
        self.assertEqual(reopened.get(self.facts["model_call_id"])["recorded_at"], row["recorded_at"])

    def test_unavailable_is_distinct_from_reported_zero_and_filters_are_public(self):
        zero = self.journal.record(**self.facts, usage=normalize_provider_usage({"input": 0}))
        second = {**self.facts, "model_call_id": "urn:exo:model-call:test:2",
                  "task_id": "task-2", "action_id": "action-2"}
        unavailable = self.journal.record(**second, usage=unavailable_usage())
        self.assertEqual(zero["usage"]["input_tokens"], {"value": 0, "status": "reported"})
        self.assertEqual(unavailable["usage"]["input_tokens"],
                         {"value": None, "status": "unavailable"})
        self.assertEqual(unavailable["evidence_status"], "unknown")
        self.assertEqual([row["model_call_id"] for row in self.journal.list_measurements(run_id="run-1")],
                         ["urn:exo:model-call:test:1", "urn:exo:model-call:test:2"])
        self.assertEqual([row["task_id"] for row in self.journal.list_measurements(action_id="action-2")],
                         ["task-2"])
        self.assertEqual(self.journal.list_measurements(task_id="missing"), [])
        with self.assertRaises(TypeError):
            self.journal.record(**self.facts, usage=unavailable_usage(), recorded_at="backdated")

    def test_unbound_authoring_overhead_scope_is_nullable_durable_and_public(self):
        facts = {
            "model_call_id": "urn:exo:authoring-model-call:synthetic:1",
            "provider": "synthetic-broker", "model_id": "synthetic-model-v1",
            "reasoning_effort": "xhigh", "call_scope": "authoring_overhead",
            "usage": normalize_provider_usage({"input": 9, "output": 4, "totalTokens": 13}),
        }
        row = self.journal.record(**facts)
        self.assertEqual(row["call_scope"], "authoring_overhead")
        self.assertIsNone(row["service_identity"])
        self.assertIsNone(row["task_id"])
        self.assertIsNone(row["run_id"])
        self.assertIsNone(row["assignment_id"])
        self.assertIsNone(row["attempt_id"])
        self.assertEqual(row["usage"]["input_tokens"], {"value": 9, "status": "reported"})
        self.assertNotIn("cost", canonical(row))

        reopened = ModelUsageJournal(self.database)
        replay = reopened.record(**facts)
        self.assertEqual(replay, row)
        self.assertEqual(reopened.list_measurements(call_scope="authoring_overhead"), [row])
        self.assertEqual(reopened.list_measurements(task_id="task-1", call_scope="authoring_overhead"), [])

    def test_non_authoring_measurement_requires_real_task_binding(self):
        with self.assertRaisesRegex(ValueError, "task_id may be null only for authoring overhead"):
            self.journal.record(model_call_id="urn:exo:unbound-director-call",
                                provider="synthetic-broker", model_id="synthetic-model-v1",
                                reasoning_effort=None, call_scope="director_call",
                                usage=unavailable_usage())

    def test_model_broker_exposes_journal_as_public_facade(self):
        broker = ModelBroker(home=Path(self.directory.name) / "model-home")
        journal = broker.usage_journal()
        self.assertIsInstance(journal, ModelUsageJournal)
        row = journal.record(**self.facts, usage=unavailable_usage())
        self.assertEqual(broker.usage_journal().list_measurements(model_call_id=row["model_call_id"]),
                         [row])

    def test_legacy_schema_migration_preserves_fingerprint_and_replay(self):
        database = Path(self.directory.name) / "legacy.sqlite3"
        usage = normalize_provider_usage({"input": 1, "output": 2, "cacheRead": 3,
                                          "cacheWrite": 4, "totalTokens": 10})
        fields = {**self.facts, "usage": usage}
        fingerprint = hashlib.sha256(canonical(fields).encode()).hexdigest()
        measurement_id = "mu-" + hashlib.sha256(self.facts["model_call_id"].encode()).hexdigest()[:28]
        recorded_at = "2026-10-02T03:04:05.000006+00:00"
        with sqlite3.connect(database) as db:
            db.execute("""CREATE TABLE model_usage_measurements(
                model_call_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
                measurement_id TEXT NOT NULL UNIQUE, service_identity TEXT NOT NULL,
                task_id TEXT NOT NULL, action_id TEXT NOT NULL, run_id TEXT NOT NULL,
                definition_digest TEXT NOT NULL, provider TEXT NOT NULL, model_id TEXT NOT NULL,
                reasoning_effort TEXT, usage_json TEXT NOT NULL, measurement_source TEXT NOT NULL,
                completeness TEXT NOT NULL, evidence_status TEXT NOT NULL, recorded_at TEXT NOT NULL)""")
            db.execute("""INSERT INTO model_usage_measurements VALUES
                (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                self.facts["model_call_id"], fingerprint, measurement_id,
                self.facts["service_identity"], self.facts["task_id"], self.facts["action_id"],
                self.facts["run_id"], self.facts["definition_digest"], self.facts["provider"],
                self.facts["model_id"], self.facts["reasoning_effort"], canonical(usage),
                "provider_reported", "complete", "provider_reported", recorded_at))
        migrated = ModelUsageJournal(database)
        replay = migrated.record(**self.facts, usage=usage)
        self.assertIsNone(replay["message_id"])
        self.assertEqual(replay["recorded_at"], recorded_at)
        self.assertEqual(migrated.get(self.facts["model_call_id"]), replay)


class BrokerMeasurementTests(unittest.IsolatedAsyncioTestCase):
    async def test_safe_callback_preserves_provider_zero_and_omits_missing(self):
        with tempfile.TemporaryDirectory(prefix="exo-model-usage-broker-") as folder:
            path = Path(folder) / "broker.sock"
            reported = FakeSocketBroker(path, text="synthetic reply", usage={
                "input": 0, "output": 4, "cacheRead": 2, "cacheWrite": 0,
                "totalTokens": 6, "cost": {"total": 123456}, "raw": "discard-me",
            })
            await reported.start()
            try:
                seen = []
                model = PiBrokerModel(AttachedBroker(path), model_id="synthetic-model-v1",
                                      session_id="synthetic-session", reasoning_effort="high",
                                      usage_callback=seen.append)
                chunks = [chunk async for chunk in model.stream([])]
                record = seen[0]
                self.assertEqual(record["model_call_id"], reported.requests[0]["id"])
                self.assertEqual((record["model_id"], record["reasoning_effort"]),
                                 ("synthetic-model-v1", "high"))
                self.assertEqual(record["usage"]["input_tokens"], {"value": 0, "status": "reported"})
                self.assertEqual(record["usage"]["cache_write_tokens"], {"value": 0, "status": "reported"})
                self.assertNotIn("cost", canonical(record))
                metadata = next(item["metadata"] for item in chunks if "metadata" in item)
                self.assertEqual(metadata["usage"]["inputTokens"], 0)
                self.assertEqual(metadata["usage"]["cacheWriteTokens"], 0)
            finally:
                await reported.close()

            missing_path = Path(folder) / "missing.sock"
            missing = FakeSocketBroker(missing_path, text="no usage field")
            await missing.start()
            try:
                seen = []
                model = PiBrokerModel(AttachedBroker(missing_path), model_id="synthetic-model-v1",
                                      session_id="synthetic-session", usage_callback=seen.append)
                chunks = [chunk async for chunk in model.stream([])]
                self.assertTrue(all(item["value"] is None and item["status"] == "unavailable"
                                    for item in seen[0]["usage"].values()))
                metadata = next(item["metadata"] for item in chunks if "metadata" in item)
                self.assertEqual(metadata["usage"], {})
            finally:
                await missing.close()

    async def test_agent_usage_leaves_only_through_budget_extension(self):
        with tempfile.TemporaryDirectory(prefix="exo-model-usage-task-") as folder:
            state = Path(folder)
            socket_path = state / "broker.sock"
            reply = canonical({"revision": "r1", "result": "synthetic evidence"})
            fake = FakeSocketBroker(socket_path, text=reply, usage={
                "input": 7, "output": 5, "cacheRead": 0,
                "cacheWrite": 1, "totalTokens": 13,
            })

            class Role:
                def system_prompt(self, capability):
                    return "Synthetic test role."

                def user_prompt(self, brief):
                    return canonical(brief)

                def precheck(self, brief, identity):
                    return None

                def parse(self, text, brief, identity):
                    return json.loads(text)

            async def exercise():
                await fake.start()
                try:
                    app = model_agent.create_app(state, 45761, role="synthesis",
                        capability="report_synthesis@1", model_provider="codex-subscription",
                        model="synthetic-model-v1", roles={"synthesis": Role()}, deadline_seconds=10)

                    def use_client():
                        with TestClient(app) as client:
                            # A plain A2A Message: the brief is the only content.
                            result = client.post("/", json={"jsonrpc": "2.0", "id": "send-1",
                                "method": "SendMessage", "params": {"message": {
                                    "role": "ROLE_USER", "messageId": "message-1",
                                    "contextId": "context-1", "parts": [
                                        {"text": canonical({"revision": "r1"}),
                                         "mediaType": "application/json"}]},
                                    "configuration": {"returnImmediately": True}}},
                                headers=AUTH).json()["result"]["task"]
                            budget_headers = {**AUTH, a2a_v1.EXTENSIONS_HEADER:
                                              a2a_extensions.BUDGET_URI}
                            deadline = time.monotonic() + 5
                            while time.monotonic() < deadline:
                                task = client.post("/", json={"jsonrpc": "2.0", "id": "get-1",
                                    "method": "GetTask", "params": {"id": result["id"]}},
                                    headers=budget_headers).json()["result"]
                                if task["status"]["state"] in {"TASK_STATE_COMPLETED", "TASK_STATE_FAILED"}:
                                    break
                                time.sleep(0.02)
                            self.assertEqual(task["status"]["state"], "TASK_STATE_COMPLETED")
                            # Usage leaves the agent only through the budget
                            # extension on its terminal Task (decision 8).
                            incurred = a2a_extensions.parse_incurred(task["metadata"])
                            self.assertEqual(incurred["tokens"]["input"], 7)
                            self.assertEqual(incurred["tokens"]["output"], 5)
                            self.assertNotIn("cost", incurred)
                            self.assertEqual(client.get("/usage/measurements?run_id=x",
                                                        headers=AUTH).status_code, 404)
                            exposed = canonical(task["metadata"])
                            self.assertNotIn("synthetic evidence", exposed)
                            self.assertNotIn("prompt", exposed)

                    await asyncio.to_thread(use_client)
                finally:
                    await fake.close()

            with patch.dict(os.environ, {}, clear=True), \
                    patch.object(model_agent, "ModelBroker", return_value=AttachedBroker(socket_path)):
                await exercise()


if __name__ == "__main__":
    unittest.main()
