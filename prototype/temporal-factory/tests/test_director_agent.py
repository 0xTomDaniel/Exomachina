"""Semantic Director boundary tests; integration proof lives in spike_c_director."""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

SRC = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC))

from director_agent import BudgetedModel, DirectorBudgetExhausted, DirectorTurn, selected_model
from harness_server import Rejected
from model_broker import ModelBroker, PiBrokerModel
from model_usage import ModelUsageJournal


class StubDirector:
    def __init__(self):
        self.file = Path(tempfile.mkdtemp(prefix="exo-qual-c-unit-", dir="/tmp")) / "audit.sqlite3"
        self.database = self.file
        self.identity = "synthetic-director-identity"
        self.commands = []
        self.binding = None
        self.module = SimpleNamespace(publications=SimpleNamespace(get=lambda _: {
            "closure": {"manifest": {"root_digest": "synthetic-root-definition"}}}))
        with self.connect() as db:
            db.execute("CREATE TABLE director_tool_calls (task_id, message_id, model_kind, "
                       "tool, arguments_json, result_json, accepted, created_at)")
            db.execute("CREATE TABLE director_turns (task_id, message_id, model_kind, "
                       "model_calls, tool_calls, result_json, created_at)")

    def connect(self):
        return sqlite3.connect(self.file)

    def perform(self, command, task_id, context_id):
        self.commands.append((command, task_id, context_id))
        if command.get("inputs", {}).get("question") == "forbidden":
            raise Rejected("invalid question")
        if command["op"] == "abort" and command["revision"] == "old":
            raise Rejected("stale revision/digest")
        if command["op"] == "start":
            self.binding = ("synthetic-run-after-start", context_id)
        return {"accepted_command": command["op"]}

    def task_binding(self, task_id):
        return self.binding

    def run_record(self, run_id):
        self.assert_run = run_id
        return {"manifest_digest": "synthetic-manifest"}

    def inspect_bound_run(self, task_id):
        return {"phase": "awaiting-director", "current_revision": "new",
                "current_sha256": "a" * 64, "repair_count": 2,
                "max_repairs": 2, "quality_findings": [], "wait_deadline": 123.0}


class EmptyModel:
    def update_config(self, **config):
        pass

    def get_config(self):
        return {"model_id": "unit"}

    async def stream(self, messages, tool_specs=None, system_prompt=None, **kwargs):
        yield {"messageStart": {"role": "assistant"}}


class SocketBroker(ModelBroker):
    def __init__(self, socket_path: Path):
        self.socket = socket_path

    def ensure_started(self, *, reason: str, timeout: float = 30):
        return {"signed_in": True, "pid": 1}


class SyntheticUsageBroker:
    def __init__(self, path: Path):
        self.path = path
        self.server = None
        self.model_call_ids = []

    async def start(self):
        self.server = await asyncio.start_unix_server(self.handle, path=str(self.path))

    async def close(self):
        self.server.close()
        await self.server.wait_closed()

    async def handle(self, reader, writer):
        request = json.loads(await reader.readline())
        request_id = request["id"]
        self.model_call_ids.append(request_id)
        if len(self.model_call_ids) == 1:
            arguments = json.dumps({"question": "synthetic research question"}, separators=(",", ":"))
            events = [
                {"type": "toolcall_start", "contentIndex": 0,
                 "toolCall": {"id": "synthetic-tool-call", "name": "start_research"}},
                {"type": "toolcall_delta", "contentIndex": 0, "delta": arguments},
                {"type": "toolcall_end", "contentIndex": 0},
            ]
            final = {"content": [{"type": "toolCall", "id": "synthetic-tool-call",
                                   "name": "start_research",
                                   "arguments": {"question": "synthetic research question"}}],
                     "stopReason": "toolUse",
                     "usage": {"input": 9, "output": 4, "cacheRead": 0,
                               "cacheWrite": 1, "totalTokens": 14}}
        else:
            events = [
                {"type": "text_start", "contentIndex": 0},
                {"type": "text_delta", "contentIndex": 0, "delta": "Run started."},
                {"type": "text_end", "contentIndex": 0},
            ]
            final = {"content": [{"type": "text", "text": "Run started."}],
                     "stopReason": "stop",
                     "usage": {"input": 12, "output": 3, "cacheRead": 2,
                               "cacheWrite": 0, "totalTokens": 17}}
        for item in events:
            writer.write((json.dumps({"id": request_id, "ev": item}) + "\n").encode())
        writer.write((json.dumps({"id": request_id, "done": final}) + "\n").encode())
        await writer.drain()
        writer.close()
        await writer.wait_closed()


class DirectorAgentTests(unittest.TestCase):
    def setUp(self):
        self.director = StubDirector()
        self.turn = DirectorTurn(self.director, "original-task", "context", "message-1",
                                 "fixture-injected")

    def test_stable_action_id_and_one_task(self):
        args = {"question": "evidence?"}
        self.assertTrue(self.turn.call("start_research", args)["ok"])
        self.assertEqual(self.director.commands[0][1:], ("original-task", "context"))
        other = DirectorTurn(self.director, "original-task", "context", "message-1", "fixture-injected")
        self.assertEqual(other.action_id("start"), self.turn.action_id("start"))
        self.assertNotEqual(other.action_id("abort"), self.turn.action_id("start"))
        self.assertFalse(any(key in str(self.turn.calls[0]["result"])
                             for key in ("action_id", "run_id", "token", "epoch")))
        with self.director.connect() as db:
            arguments = db.execute("SELECT arguments_json FROM director_tool_calls").fetchone()[0]
        self.assertEqual(arguments, '{"question": "evidence?"}')

    def test_invalid_tool_arguments_and_rejection_row(self):
        for args in ({"question": "x", "graph": "chosen"},
                     {"question": "x", "extra_input": "x"}):
            self.assertEqual(self.turn.call("start_research", args)["error"]["code"],
                             "invalid_arguments")
        self.assertEqual(len(self.director.commands), 0)
        self.assertEqual(self.turn.call("start_research", {"question": "forbidden"})
                         ["error"]["code"], "rejected")
        self.assertEqual(self.turn.call("decide_wait", {"action": "abort", "revision": "old",
            "sha256": "b" * 64, "rationale": "test"})["error"]["message"], "stale revision/digest")
        with self.director.connect() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM director_tool_calls WHERE accepted=0")
                             .fetchone()[0], 4)

    def test_research_question_words_do_not_block_structurally_valid_inputs(self):
        for index, question in enumerate(("Which Python version: 3.12 or 3.13?",
                                          "Should we use the package manager's lockfile?")):
            turn = DirectorTurn(self.director, f"task-{index}", "context",
                                f"message-{index}", "fixture-injected")
            result = turn.call("start_research", {"question": question})
            self.assertEqual(result, {"ok": True, "accepted_command": "start"})
        self.assertEqual(len(self.director.commands), 2)

    def test_tool_budget_issues_no_fifth_command(self):
        self.turn.limits["max_tool_calls"] = 1
        self.assertTrue(self.turn.call("start_research", {"question": "x"})["ok"])
        self.assertEqual(self.turn.call("inspect_run", {})["error"]["code"], "budget")
        self.assertEqual(len(self.director.commands), 1)

    def test_model_budget_and_live_override_guard(self):
        async def run():
            model = BudgetedModel(EmptyModel(), time.monotonic() + 5, 1)
            self.assertEqual(len([e async for e in model.stream([])]), 1)
            with self.assertRaises(DirectorBudgetExhausted):
                _ = [e async for e in model.stream([])]
        asyncio.run(run())
        with patch.dict(os.environ, {"EXO_MODEL_HOME": "/tmp/fixture"}):
            with self.assertRaisesRegex(ValueError, "default model home"):
                selected_model({"director_model": {"provider": "codex-subscription"}},
                               session_id="unit")

    def test_director_usage_keeps_pre_run_call_unbound_and_binds_later_call(self):
        async def exercise():
            socket_path = self.director.file.parent / "director-usage.sock"
            broker = SyntheticUsageBroker(socket_path)
            await broker.start()
            try:
                turn = DirectorTurn(self.director, "task-director", "context-director",
                                    "message-director", "synthetic")
                model = PiBrokerModel(SocketBroker(socket_path), model_id="synthetic-director-v1",
                                      session_id="synthetic-director-session",
                                      reasoning_effort="xhigh")
                outcome = await turn.run("synthetic brief", model)
                self.assertEqual(outcome["model_calls"], 2)
                self.assertEqual(outcome["accepted"], ["start_research"])
                journal = ModelUsageJournal(self.director.database)
                measurements = journal.list_measurements(task_id="task-director",
                                                         message_id="message-director")
                self.assertEqual(len(measurements), 2)
                self.assertEqual([row["model_call_id"] for row in measurements],
                                 broker.model_call_ids)
                first, second = measurements
                self.assertIsNone(first["run_id"])
                self.assertIsNone(first["definition_digest"])
                self.assertEqual((first["task_id"], first["message_id"]),
                                 ("task-director", "message-director"))
                self.assertEqual((second["run_id"], second["definition_digest"]),
                                 ("synthetic-run-after-start", "synthetic-root-definition"))
                self.assertEqual(first["usage"]["cache_read_tokens"],
                                 {"value": 0, "status": "reported"})
                self.assertEqual(second["usage"]["cache_write_tokens"],
                                 {"value": 0, "status": "reported"})
                for measurement in measurements:
                    self.assertEqual((measurement["provider"], measurement["model_id"],
                                      measurement["reasoning_effort"]),
                                     ("codex-subscription", "synthetic-director-v1", "xhigh"))
                    self.assertNotIn("arguments", measurement)
                    self.assertNotIn("result", measurement)
                    self.assertNotIn("content", measurement)
                    self.assertNotIn("synthetic brief", json.dumps(measurement))
            finally:
                await broker.close()

        asyncio.run(exercise())


if __name__ == "__main__":
    unittest.main()
