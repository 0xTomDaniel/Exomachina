"""Semantic Director boundary tests; integration proof lives in spike_c_director."""
from __future__ import annotations

import asyncio
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

SRC = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC))

from director_agent import BudgetedModel, DirectorBudgetExhausted, DirectorTurn, selected_model
from harness_server import Rejected


class StubDirector:
    def __init__(self):
        self.file = Path(tempfile.mkdtemp(prefix="exo-qual-c-unit-", dir="/tmp")) / "audit.sqlite3"
        self.commands = []
        with self.connect() as db:
            db.execute("CREATE TABLE director_tool_calls (task_id, message_id, model_kind, "
                       "tool, arguments_json, result_json, accepted, created_at)")

    def connect(self):
        return sqlite3.connect(self.file)

    def perform(self, command, task_id, context_id):
        self.commands.append((command, task_id, context_id))
        if command.get("inputs", {}).get("question") == "forbidden":
            raise Rejected("invalid question")
        if command["op"] == "abort" and command["revision"] == "old":
            raise Rejected("stale revision/digest")
        return {"accepted_command": command["op"]}

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


if __name__ == "__main__":
    unittest.main()
