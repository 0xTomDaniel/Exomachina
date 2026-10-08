"""Synthetic Director admission proofs; these tests make no provider calls."""
import asyncio
import json
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import harness


class BasicJobGateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        home = Path(self.temp.name)
        self.instance = home / "instances" / "basic"
        self.config = harness.init_instance(
            self.instance, name="basic", mode="factory", port=49990, home=home)
        self.config["basic_single_active_job"] = True
        self.director = harness.Director(self.instance, self.config)

    def claim(self, task="task-1", message="message-1", brief="bounded brief"):
        return self.director._basic_claim_submission(
            task, "context-1", message, "fixture-operator", brief)

    def bind(self, task="task-1"):
        with self.director.connect() as db:
            db.execute("INSERT INTO runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                "run-1", task, "context-1", "package", "manifest", "build",
                "v1", "{}", "inputs", "fixture-operator", "{}", 0, None))
            db.execute("INSERT INTO aliases VALUES (?,?,?)", (task, "run-1", "context-1"))

    def test_pending_turn_blocks_another_task_and_duplicate_execution(self):
        self.assertEqual(self.claim(), (False, None))
        with self.assertRaisesRegex(harness.Rejected, "one active job"):
            self.claim(task="task-2", message="message-2")
        with self.assertRaisesRegex(harness.Rejected, "pending"):
            self.claim()
        self.assertTrue(self.director.basic_job_status()["busy"])

    def test_atomic_race_admits_only_one_task(self):
        def attempt(index):
            try:
                return self.claim(task=f"task-{index}", message=f"message-{index}")[0] is False
            except harness.Rejected:
                return False
        with ThreadPoolExecutor(max_workers=8) as pool:
            self.assertEqual(sum(pool.map(attempt, range(8))), 1)

    def test_replay_is_bound_to_message_task_context_actor_and_brief(self):
        self.claim()
        self.bind()
        result = {"accepted_command": "start"}
        self.director._basic_finish_submission("task-1", "message-1", result)
        self.assertEqual(self.claim(), (True, result))
        with self.assertRaisesRegex(harness.Rejected, "conflict"):
            self.claim(brief="different brief")
        with self.assertRaisesRegex(harness.Rejected, "conflict"):
            self.director._basic_claim_submission(
                "task-1", "context-1", "message-1", "fixture-observer", "bounded brief")
        reopened = harness.Director(self.instance, self.config, claim=False)
        self.assertEqual(reopened._basic_claim_submission(
            "task-1", "context-1", "message-1", "fixture-operator", "bounded brief"),
            (True, result))

    def test_only_durable_terminal_fact_releases_slot(self):
        self.claim()
        self.bind()
        self.director._basic_finish_submission("task-1", "message-1", {"accepted_command": "start"})
        with self.assertRaisesRegex(harness.Rejected, "one active job"):
            self.claim(task="task-2", message="message-2")
        with self.director.connect() as db:
            db.execute("UPDATE runs SET closed=1,outcome_json=?", (json.dumps({"state": "working"}),))
        with self.assertRaisesRegex(harness.Rejected, "one active job"):
            self.claim(task="task-2", message="message-2")
        with self.director.connect() as db:
            db.execute("UPDATE runs SET outcome_json=?", (json.dumps({"state": "completed"}),))
        self.assertEqual(self.director.basic_job_status(),
                         {"configured": True, "busy": False, "state": "available"})
        self.assertEqual(self.claim(task="task-2", message="message-2"), (False, None))

    def test_known_unscheduled_rejection_releases_but_uncertainty_stays_busy(self):
        self.claim()
        self.director._basic_finish_submission("task-1", "message-1", {"error": "unready"})
        self.assertFalse(self.director.basic_job_status()["busy"])
        self.claim(task="task-2", message="message-2")
        self.director._basic_finish_submission(
            "task-2", "message-2", {"error": "unknown"}, uncertain=True)
        with self.assertRaisesRegex(harness.Rejected, "one active job"):
            self.claim(task="task-3", message="message-3")

    def test_missing_preflight_rejects_before_any_model_selection(self):
        self.director.config["director_model"] = {"provider": "codex-subscription", "model": "gpt-6-luna"}
        token = harness.CURRENT_ACTOR.set("fixture-operator")
        try:
            with patch("director_agent.selected_model", side_effect=AssertionError("provider must not run")):
                with self.assertRaisesRegex(harness.Rejected, "readiness"):
                    asyncio.run(self.director.invoke(
                        "bounded brief", "task-1", "context-1", message_id="message-1"))
        finally:
            harness.CURRENT_ACTOR.reset(token)
        self.assertFalse(self.director.basic_job_status()["busy"])

    def test_terminal_refresh_uses_existing_task_store_authority(self):
        self.claim()
        self.bind()
        self.director._basic_finish_submission("task-1", "message-1", {"accepted_command": "start"})
        with patch.object(harness.FactoryTaskStore, "get", new_callable=AsyncMock) as get:
            asyncio.run(self.director._basic_refresh_slot())
            get.assert_awaited_once_with("task-1")

    def test_bound_uncertain_slot_reconciles_then_releases_only_at_terminal(self):
        self.claim()
        self.bind()
        self.director._basic_finish_submission(
            "task-1", "message-1", {"error": "unknown"}, uncertain=True)
        with self.assertRaisesRegex(harness.Rejected, "one active job"):
            self.claim(task="task-2", message="message-2")
        async def terminal(task_id):
            self.assertEqual(task_id, "task-1")
            with self.director.connect() as db:
                db.execute("UPDATE runs SET closed=1,outcome_json=?", (json.dumps({"state": "completed"}),))
        with patch.object(harness.FactoryTaskStore, "get", new_callable=AsyncMock,
                          side_effect=terminal) as get:
            asyncio.run(self.director._basic_refresh_slot())
            get.assert_awaited_once_with("task-1")
        self.assertFalse(self.director.basic_job_status()["busy"])
        self.assertEqual(self.claim(task="task-2", message="message-2"), (False, None))

    def test_invoke_second_task_rejected_before_second_preflight_or_model(self):
        self.director.config["director_model"] = {"provider": "codex-subscription", "model": "gpt-6-luna"}
        preflight_calls = []
        self.director.submission_preflight = lambda: preflight_calls.append(True)

        async def exercise():
            entered, release = asyncio.Event(), asyncio.Event()

            async def run(_brief, _model):
                entered.set()
                await release.wait()
                self.bind()
                return {"accepted": ["start"]}

            turn = type("SyntheticTurn", (), {})()
            turn.run = run
            with patch("director_agent.selected_model", return_value=(object(), "synthetic")) as select, \
                    patch("director_agent.DirectorTurn", return_value=turn), \
                    patch.object(harness.FactoryTaskStore, "get", new_callable=AsyncMock):
                first = asyncio.create_task(self.director.invoke(
                    "bounded brief", "task-1", "context-1", message_id="message-1"))
                await asyncio.wait_for(entered.wait(), timeout=2)
                try:
                    with self.assertRaisesRegex(harness.Rejected, "one active job"):
                        await self.director.invoke(
                            "second brief", "task-2", "context-2", message_id="message-2")
                finally:
                    release.set()
                self.assertEqual(await first, {"accepted_command": "start"})
                self.assertEqual(await self.director.invoke(
                    "bounded brief", "task-1", "context-1", message_id="message-1"),
                    {"accepted_command": "start"})
                self.assertEqual(select.call_count, 1)
                self.assertEqual(len(preflight_calls), 1)

        token = harness.CURRENT_ACTOR.set("fixture-operator")
        try:
            asyncio.run(exercise())
        finally:
            harness.CURRENT_ACTOR.reset(token)

    def test_unready_preflight_rejects_and_releases_without_model_selection(self):
        self.director.config["director_model"] = {"provider": "codex-subscription", "model": "gpt-6-luna"}
        def unavailable():
            raise harness.Rejected("submission readiness blocked")
        self.director.submission_preflight = unavailable
        token = harness.CURRENT_ACTOR.set("fixture-operator")
        try:
            with patch("director_agent.selected_model", side_effect=AssertionError("provider must not run")):
                with self.assertRaisesRegex(harness.Rejected, "readiness blocked"):
                    asyncio.run(self.director.invoke(
                        "bounded brief", "task-1", "context-1", message_id="message-1"))
        finally:
            harness.CURRENT_ACTOR.reset(token)
        self.assertFalse(self.director.basic_job_status()["busy"])

    def test_explicit_boolean_required(self):
        for value in (1, "true", None):
            config = dict(self.config, basic_single_active_job=value)
            with self.assertRaisesRegex(ValueError, "boolean"):
                harness.Director(self.instance, config, claim=False)

    def test_deferred_entry_points_cannot_bypass_basic_gate(self):
        for key in ("legacy_structured_commands", "nested_supplier_enabled"):
            with self.assertRaisesRegex(ValueError, "deferred command paths"):
                harness.Director(self.instance, dict(self.config, **{key: True}), claim=False)


if __name__ == "__main__":
    unittest.main()
