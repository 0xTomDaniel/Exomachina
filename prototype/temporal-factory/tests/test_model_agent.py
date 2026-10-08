"""A2A and durable-state checks using injected role logic and scripted Strands models."""
from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import sys
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services"))
sys.path.insert(0, str(ROOT / "src"))
import agent_binding  # noqa: E402
import a2a_extensions as ext  # noqa: E402
from model_agent import canonical, create_app  # noqa: E402
import model_agent  # noqa: E402

import a2a_v1  # noqa: E402

AUTH = {"Authorization": "Bearer fixture-token", **a2a_v1.headers()}
BUDGET = {"Authorization": "Bearer fixture-token", **a2a_v1.headers([ext.BUDGET_URI])}
TEST = {"Authorization": "Bearer fixture-token", **a2a_v1.headers([ext.TEST_STIMULUS_URI])}
FACTORY_NAMES = ("run_id", "assignment_id", "attempt_id", "action_id", "definition_digest",
                 "factory_id", "node")


class FakeRole:
    def __init__(self, fail=False, claim_text="Normal claim"):
        self.fail = fail
        self.claim_text = claim_text

    def system_prompt(self, capability):
        return "Return the requested JSON."

    def user_prompt(self, brief):
        return canonical(brief)

    def scripted_reply(self, brief, identity):
        return canonical({"kind": "verified_report@1", "revision": brief["revision"],
                          "markdown": "A report.", "claims": [{"id": "C1", "text": self.claim_text, "evidence": ["E1"]}]})

    def parse(self, text, brief, identity):
        if self.fail:
            raise ValueError("fixture validation failure")
        value = json.loads(text)
        if value["revision"] != brief["revision"]:
            raise ValueError("revision")
        return value

    def precheck(self, brief, identity):
        return None


def brief(revision="r1", **extra):
    return canonical({"kind": "synthesis_assignment@1", "revision": revision, **extra})


def send(text=None, *, message_id=None, context_id=None, metadata=None, parts=None,
         blocking=False, message_metadata=None):
    message = {"role": "ROLE_USER", "messageId": message_id or str(uuid4()),
               "parts": parts if parts is not None else [{"text": text or brief()}]}
    if context_id:
        message["contextId"] = context_id
    if message_metadata:
        message["metadata"] = message_metadata
    params = {"message": message}
    if not blocking:
        params["configuration"] = {"returnImmediately": True}
    if metadata is not None:
        params["metadata"] = metadata
    return {"jsonrpc": "2.0", "id": str(uuid4()), "method": "SendMessage", "params": params}


def get(task_id):
    return {"jsonrpc": "2.0", "id": str(uuid4()), "method": "GetTask",
            "params": {"id": task_id}}


def control(value):
    return send(parts=[{"data": value, "mediaType": "application/json"}])


def completed(client, task_id, headers=AUTH):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        task = client.post("/", json=get(task_id), headers=headers).json()["result"]
        if task["status"]["state"] in {"TASK_STATE_COMPLETED", "TASK_STATE_FAILED"}:
            return task
        time.sleep(0.02)
    raise AssertionError("Task did not finish")


def calls(state):
    with sqlite3.connect(state / "model-agent.sqlite3") as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute("SELECT * FROM model_calls ORDER BY id")]


class ModelAgentTests(unittest.TestCase):
    def setUp(self):
        self.state = Path(tempfile.mkdtemp(prefix="exo-sf-agents-unit-", dir="/tmp"))
        self.roles = {"synthesis": FakeRole()}

    def app(self, port=45748, **kw):
        return create_app(self.state, port, role="synthesis", capability="report_synthesis@1",
                          model_provider="scripted", roles=self.roles, **kw)

    def test_plain_message_resend_conflict_card_pin_and_artifact(self):
        with TestClient(self.app()) as client:
            card = client.get("/.well-known/agent-card.json").json()
            self.assertEqual([skill["id"] for skill in card["skills"]], ["report_synthesis@1"])
            uris = [item["uri"] for item in card["capabilities"]["extensions"]]
            self.assertEqual(uris, [ext.AGENT_URI, ext.BUDGET_URI])
            self.assertTrue(all(item.get("required") is not True
                                for item in card["capabilities"]["extensions"]))
            identity = card["capabilities"]["extensions"][0]["params"]["identity"]
            with patch.object(agent_binding, "read_json", return_value=card):
                pinned = agent_binding.pin("http://127.0.0.1:45748")
            self.assertEqual(pinned["identity"], identity)
            self.assertEqual(pinned["reconcile"], "a2a-idempotent-resend")
            message_id = str(uuid4())
            first = client.post("/", json=send(message_id=message_id), headers=AUTH).json()["result"]["task"]
            self.assertEqual(first["status"]["state"], "TASK_STATE_WORKING")
            again = client.post("/", json=send(message_id=message_id), headers=AUTH).json()["result"]["task"]
            self.assertEqual(again["id"], first["id"])
            changed = send(brief("r2"), message_id=message_id)
            self.assertEqual(client.post("/", json=changed, headers=AUTH).json()["error"]["code"], -32602)
            done = completed(client, first["id"])
            self.assertEqual(done["status"]["state"], "TASK_STATE_COMPLETED")
            self.assertEqual(done["metadata"], {"agent_identity": identity})
            self.assertEqual(done["history"][0]["messageId"], message_id)
            self.assertEqual(done["history"][0]["parts"][0]["text"], brief())
            artifact = done["artifacts"][0]
            data = artifact["parts"][0]["data"]
            self.assertEqual(set(data), {"revision", "sha256", "author", "content"})
            self.assertEqual(artifact["artifactId"], data["sha256"])
            self.assertEqual(data["sha256"], hashlib.sha256(data["content"].encode()).hexdigest())
            self.assertEqual(data["author"], identity)
            self.assertEqual(json.loads(data["content"])["revision"], "r1")

    def test_rejects_non_message_shapes(self):
        with TestClient(self.app()) as client:
            for request in (
                    send(parts=[{"data": {"op": "assign", "brief": brief()}}]),
                    send("not json"),
                    send(canonical({"no": "revision"})),
                    send(parts=[{"text": brief()}, {"text": brief()}])):
                self.assertEqual(client.post("/", json=request, headers=AUTH).json()["error"]["code"],
                                 -32602)

    def test_factory_identifiers_are_ignored_and_never_echoed(self):
        leaked = {name: f"leak-{name}" for name in FACTORY_NAMES}
        with TestClient(self.app()) as client:
            request = send(brief(), metadata=leaked, message_metadata=leaked)
            task = client.post("/", json=request, headers=BUDGET).json()["result"]["task"]
            done = completed(client, task["id"], headers=BUDGET)
            wire = json.dumps(done) + json.dumps(task)
            for value in leaked.values():
                self.assertNotIn(value, wire)
        with sqlite3.connect(self.state / "model-agent.sqlite3") as db:
            dump = "\n".join(db.iterdump())
            for name, value in leaked.items():
                self.assertNotIn(value, dump)
                self.assertNotIn(name, dump)

    def test_budget_extension_reports_only_provider_reported_tokens(self):
        with TestClient(self.app()) as client:
            task = client.post("/", json=send(), headers=AUTH).json()["result"]["task"]
            plain = completed(client, task["id"])
            self.assertNotIn(ext.BUDGET_URI, plain["metadata"])
            reported = completed(client, task["id"], headers=BUDGET)
            incurred = ext.parse_incurred(reported["metadata"])
            # The scripted provider reports input/output only: no cache, total or cost.
            self.assertEqual(set(incurred), {"tokens"})
            self.assertEqual(set(incurred["tokens"]), {"input", "output"})
            self.assertTrue(all(value > 0 for value in incurred["tokens"].values()))
            row = calls(self.state)[0]
            self.assertEqual(incurred["tokens"], {"input": row["input_tokens"],
                                                  "output": row["output_tokens"]})
            self.assertIsNone(row["total_tokens"])

    def test_unreported_usage_is_omitted_not_zero(self):
        async def silent(self, messages, tool_specs=None, system_prompt=None, **kwargs):
            yield {"messageStart": {"role": "assistant"}}
            yield {"contentBlockStart": {"start": {}}}
            yield {"contentBlockDelta": {"delta": {"text": self.reply}}}
            yield {"contentBlockStop": {}}
            yield {"messageStop": {"stopReason": "end_turn"}}

        with patch.object(model_agent.ScriptedModel, "stream", silent), \
                TestClient(self.app()) as client:
            task = client.post("/", json=send(), headers=BUDGET).json()["result"]["task"]
            done = completed(client, task["id"], headers=BUDGET)
            self.assertEqual(done["metadata"][ext.BUDGET_URI], {"incurred": {}})

    def test_budget_is_honoured_or_rejected(self):
        past = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
        with TestClient(self.app()) as client:
            for budget, reason in (({"tokens": {"limit": 0}}, "below this agent's minimum"),
                                   ({"deadline": past}, "budget.deadline leaves"),
                                   ({"cost": {"amount": "0", "currency": "USD"}}, "zero"),
                                   ({"unknown": 1}, "unsupported budget fields")):
                error = client.post("/", json=send(metadata={ext.BUDGET_URI: {"budget": budget}}),
                                    headers=BUDGET).json()["error"]
                self.assertEqual(error["code"], -32602)
                self.assertIn(reason, error["message"])
            future = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
            accepted = client.post("/", json=send(metadata={ext.BUDGET_URI: {"budget": {
                "tokens": {"limit": 100000}, "deadline": future,
                "cost": {"amount": "1.50", "currency": "USD"}}}}), headers=BUDGET).json()
            done = completed(client, accepted["result"]["task"]["id"], headers=BUDGET)
            self.assertEqual(done["status"]["state"], "TASK_STATE_COMPLETED")
            self.assertNotIn("cost", done["metadata"][ext.BUDGET_URI]["incurred"])

    def test_token_limit_stops_further_model_calls(self):
        self.roles["synthesis"] = FakeRole(fail=True)
        with TestClient(self.app()) as client:
            task = client.post("/", json=send(metadata={ext.BUDGET_URI: {"budget": {
                "tokens": {"limit": 1}}}}), headers=BUDGET).json()["result"]["task"]
            failed = completed(client, task["id"])
            self.assertEqual(failed["status"]["state"], "TASK_STATE_FAILED")
        self.assertEqual(len(calls(self.state)), 1)

    def test_blocking_send_returns_terminal_task(self):
        with TestClient(self.app()) as client:
            task = client.post("/", json=send(blocking=True), headers=AUTH).json()["result"]["task"]
            self.assertEqual(task["status"]["state"], "TASK_STATE_COMPLETED")

    def test_restart_keeps_identity_task_and_distinct_sessions(self):
        message_id = str(uuid4())
        with TestClient(self.app()) as client:
            identity = client.get("/.well-known/agent-card.json").json()[
                "capabilities"]["extensions"][0]["params"]["identity"]
            one = client.post("/", json=send(message_id=message_id), headers=AUTH).json()["result"]["task"]
            completed(client, one["id"])
        with TestClient(self.app(port=45749)) as client:
            self.assertEqual(client.app.state.model_agent_service.ledger.identity, identity)
            self.assertEqual(client.app.state.model_agent_service.ledger.incarnation, 2)
            self.assertEqual(completed(client, one["id"])["id"], one["id"])
            self.assertEqual(client.post("/", json=send(message_id=message_id), headers=AUTH).json()["result"]["task"]["id"], one["id"])
            two = client.post("/", json=send(), headers=AUTH).json()["result"]["task"]
            completed(client, two["id"])
        rows = calls(self.state)
        self.assertEqual(len(rows), 2)
        self.assertEqual({row["session_id"] for row in rows},
                         {f"{identity}:{one['id']}", f"{identity}:{two['id']}"})
        self.assertTrue(all(row["live"] == 0 and row["model_id"] == model_agent.DEFAULT_MODEL_ID
                            for row in rows))

    def test_only_a2a_and_agent_card_routes_are_served(self):
        with TestClient(self.app(test_controls=True)) as client:
            for path in ("/health", "/contract", "/usage/measurements", "/fixture/actions/a",
                         "/_test/stimulus-log", "/_test/observe"):
                self.assertIn(client.get(path, headers=AUTH).status_code, {404, 405})

    def test_schema_one_ledger_migrates_without_caller_fields(self):
        state = Path(tempfile.mkdtemp(prefix="exo-sf-agent-legacy-", dir="/tmp"))
        database = state / "model-agent.sqlite3"
        old_artifact = {"revision": "r1", "sha256": "a" * 64, "author": "x", "content": "{}",
                        "action_id": "legacy-action", "run_id": "legacy-run",
                        "definition_digest": "legacy-definition"}
        with sqlite3.connect(database) as db:
            db.execute("""CREATE TABLE tasks (
                task_id TEXT PRIMARY KEY, context_id TEXT NOT NULL,
                action_id TEXT NOT NULL UNIQUE, fingerprint TEXT NOT NULL, state TEXT NOT NULL,
                run_id TEXT NOT NULL, definition_digest TEXT NOT NULL, brief TEXT NOT NULL,
                artifact TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL,
                assignment_id TEXT, attempt_id TEXT, factory_id TEXT)""")
            db.execute("""INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                "legacy-task", "legacy-context", "legacy-action", "fp", "completed",
                "legacy-run", "legacy-definition", brief(), canonical(old_artifact), 1.0, 2.0,
                "legacy-assignment", "legacy-attempt", "legacy-factory"))
            db.execute("CREATE TABLE stimulus_log (run_id TEXT)")
            db.execute("CREATE TABLE model_usage_measurements (run_id TEXT)")
        ledger = model_agent.Ledger(state)
        task = ledger.task("legacy-task")
        self.assertEqual(task.context_id, "legacy-context")
        with sqlite3.connect(database) as db:
            dump = "\n".join(db.iterdump())
            tables = {row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
        for value in ("legacy-action", "legacy-run", "legacy-definition", "legacy-assignment",
                      "legacy-attempt", "legacy-factory"):
            self.assertNotIn(value, dump)
        self.assertNotIn("stimulus_log", tables)
        self.assertNotIn("model_usage_measurements", tables)

    def test_unfinished_task_recovers_after_restart(self):
        async def stalled(self, messages, tool_specs=None, system_prompt=None, **kwargs):
            await asyncio.sleep(30)
            yield {"messageStart": {"role": "assistant"}}

        message_id = str(uuid4())
        with patch.object(model_agent.ScriptedModel, "stream", stalled):
            with TestClient(self.app()) as client:
                first = client.post("/", json=send(message_id=message_id), headers=AUTH).json()["result"]["task"]
                self.assertEqual(first["status"]["state"], "TASK_STATE_WORKING")
        with TestClient(self.app(port=45749)) as client:
            done = completed(client, first["id"])
            self.assertEqual(done["status"]["state"], "TASK_STATE_COMPLETED")
            self.assertEqual(client.post("/", json=send(message_id=message_id), headers=AUTH).json()["result"]["task"]["id"], first["id"])

    def test_output_exhaustion_is_fixed_failure(self):
        self.roles["synthesis"] = FakeRole(fail=True)
        with TestClient(self.app()) as client:
            task = client.post("/", json=send(), headers=AUTH).json()["result"]["task"]
            failed = completed(client, task["id"])
            self.assertEqual(failed["status"]["state"], "TASK_STATE_FAILED")
            self.assertEqual(failed["status"]["message"]["parts"][0]["text"], "Agent work failed.")
            self.assertFalse(failed.get("artifacts"))
        rows = calls(self.state)
        self.assertEqual([row["call_no"] for row in rows], [1, 2, 3])
        self.assertTrue(all(row["outcome_kind"] == "validation-error" for row in rows))


class TestStimulusExtensionTests(unittest.TestCase):
    """The test-only stimulus extension binds by contextId, never by caller IDs."""

    def setUp(self):
        self.state = Path(tempfile.mkdtemp(prefix="exo-sf-agents-stimulus-", dir="/tmp"))
        self.roles = {"synthesis": FakeRole()}

    def app(self, port=45748, **kw):
        return create_app(self.state, port, role="synthesis", capability="report_synthesis@1",
                          model_provider="scripted", roles=self.roles, test_controls=True, **kw)

    def arm(self, client, body):
        reply = client.post("/", json=control({"arm": body}), headers=TEST).json()["result"]
        return reply["message"]["parts"][0]["data"]

    def log(self, client):
        reply = client.post("/", json=control({"stimulus_log": True}), headers=TEST).json()["result"]
        return reply["message"]["parts"][0]["data"]["stimulus_log"]

    def test_production_card_never_declares_test_extension(self):
        production = create_app(self.state, 45750, role="synthesis",
                                capability="report_synthesis@1", model_provider="scripted",
                                roles=self.roles)
        with TestClient(production) as client:
            card = client.get("/.well-known/agent-card.json").json()
            self.assertNotIn(ext.TEST_STIMULUS_URI,
                             [item["uri"] for item in card["capabilities"]["extensions"]])
            # Without test controls the control message is an ordinary, invalid brief.
            self.assertEqual(client.post("/", json=control({"stimulus_log": True}),
                                         headers=TEST).json()["error"]["code"], -32602)
        with TestClient(self.app()) as client:
            card = client.get("/.well-known/agent-card.json").json()
            self.assertIn(ext.TEST_STIMULUS_URI,
                          [item["uri"] for item in card["capabilities"]["extensions"]])

    def test_stimulus_binds_next_new_context_revision_and_hash(self):
        with TestClient(self.app()) as client:
            body = {"append_claim": {"text": "Planted defect.", "evidence": ["E1"]},
                    "revisions": ["r1"]}
            self.assertTrue(self.arm(client, body)["armed"])
            first = client.post("/", json=send(context_id="ctx-1"), headers=AUTH).json()["result"]["task"]
            done = completed(client, first["id"])
            artifact = done["artifacts"][0]["parts"][0]["data"]
            content = json.loads(artifact["content"])
            self.assertEqual(content["claims"][-1]["id"], "C2")
            self.assertIn("Planted defect.", content["markdown"])
            self.assertEqual(artifact["sha256"], hashlib.sha256(artifact["content"].encode()).hexdigest())
            repair = client.post("/", json=send(brief("r2"), context_id="ctx-1"), headers=AUTH).json()["result"]["task"]
            repaired = completed(client, repair["id"])
            self.assertNotIn("Planted defect.", repaired["artifacts"][0]["parts"][0]["data"]["content"])
            other = client.post("/", json=send(context_id="ctx-2"), headers=AUTH).json()["result"]["task"]
            completed(client, other["id"])
            self.assertEqual(self.log(client), [{
                "stimulus_id": 1, "context_id": "ctx-1", "revision": "r1", "task_id": first["id"],
                "planted_text": "Planted defect.", "sha256_after": artifact["sha256"]}])

    def test_stimulus_waits_for_next_new_context(self):
        with TestClient(self.app()) as client:
            prior = client.post("/", json=send(context_id="old"), headers=AUTH).json()["result"]["task"]
            completed(client, prior["id"])
            self.arm(client, {"append_claim": {"text": "Next context only.", "evidence": ["E1"]},
                              "revisions": "all"})
            same = client.post("/", json=send(brief("r2"), context_id="old"), headers=AUTH).json()["result"]["task"]
            old = completed(client, same["id"])
            self.assertNotIn("Next context only.", old["artifacts"][0]["parts"][0]["data"]["content"])
            new = client.post("/", json=send(context_id="new"), headers=AUTH).json()["result"]["task"]
            done = completed(client, new["id"])
            self.assertIn("Next context only.", done["artifacts"][0]["parts"][0]["data"]["content"])
            self.assertEqual([row["context_id"] for row in self.log(client)], ["new"])

    def test_stimulus_log_committed_before_finish_recovers_one_identical_artifact(self):
        body = {"append_claim": {"text": "Planted defect.", "evidence": ["E1"]},
                "revisions": ["r1"]}
        original_finish = model_agent.Ledger.finish
        interrupted = []

        def die_after_log(ledger, task_id, artifact, stimulus=None):
            if stimulus is not None and not interrupted:
                db = ledger.connect()
                db.execute("""INSERT INTO test_stimulus_log
                    (stimulus_id,context_id,revision,task_id,planted_text,sha256_after,content_after)
                    VALUES (?,?,?,?,?,?,?)""", tuple(stimulus[key] for key in (
                        "stimulus_id", "context_id", "revision", "task_id", "planted_text",
                        "sha256_after", "content_after")))
                db.close()
                interrupted.append(stimulus)
                raise asyncio.CancelledError("process stopped before Task finish")
            return original_finish(ledger, task_id, artifact, stimulus)

        with patch.object(model_agent.Ledger, "finish", die_after_log):
            with TestClient(self.app()) as client:
                self.assertTrue(self.arm(client, body)["armed"])
                task = client.post("/", json=send(context_id="ctx-1"), headers=AUTH).json()["result"]["task"]
                deadline = time.monotonic() + 5
                while not interrupted and time.monotonic() < deadline:
                    time.sleep(0.02)
                self.assertEqual(len(interrupted), 1)
                self.assertEqual(client.post("/", json=get(task["id"]), headers=AUTH).json()["result"]["status"]["state"],
                                 "TASK_STATE_WORKING")

        self.roles["synthesis"] = FakeRole(claim_text="Different recovered model output")
        with TestClient(self.app(port=45749)) as client:
            done = completed(client, task["id"])
            artifact = done["artifacts"][0]["parts"][0]["data"]
            content = json.loads(artifact["content"])
            self.assertEqual(content["claims"][0]["text"], "Normal claim")
            self.assertEqual(content["claims"][1], {"id": "C2", "text": "Planted defect.", "evidence": ["E1"]})
            self.assertEqual(artifact["sha256"], interrupted[0]["sha256_after"])
            self.assertEqual(len(self.log(client)), 1)
        self.assertEqual(len([row for row in calls(self.state) if row["task_id"] == task["id"]]), 1)


if __name__ == "__main__":
    unittest.main()
