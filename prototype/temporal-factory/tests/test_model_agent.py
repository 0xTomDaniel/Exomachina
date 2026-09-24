"""A2A and durable-state checks using injected role logic and scripted Strands models."""
from __future__ import annotations

import asyncio
import hashlib
import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services"))
sys.path.insert(0, str(ROOT / "src"))
import agent_binding  # noqa: E402
from model_agent import canonical, create_app, digest  # noqa: E402
import model_agent  # noqa: E402

AUTH = {"Authorization": "Bearer fixture-token"}


class FakeRole:
    def __init__(self, fail=False):
        self.fail = fail

    def system_prompt(self, capability):
        return "Return the requested JSON."

    def user_prompt(self, brief):
        return canonical(brief)

    def scripted_reply(self, brief, identity):
        return canonical({"kind": "verified_report@1", "revision": brief["revision"],
                          "markdown": "A report.", "claims": [{"id": "C1", "text": "Normal claim", "evidence": ["E1"]}]})

    def parse(self, text, brief, identity):
        if self.fail:
            raise ValueError("fixture validation failure")
        value = json.loads(text)
        if value["revision"] != brief["revision"]:
            raise ValueError("revision")
        return value

    def precheck(self, brief, identity):
        return None


def command(action="a1", run="run-1", revision="r1"):
    return {"op": "assign", "action_id": action, "run_id": run,
            "definition_digest": "definition-1",
            "brief": canonical({"kind": "synthesis_assignment@1", "revision": revision})}


def send(value):
    return {"jsonrpc": "2.0", "id": str(uuid4()), "method": "message/send",
            "params": {"message": {"role": "user", "messageId": str(uuid4()),
                                   "parts": [{"kind": "data", "data": value}]},
                       "configuration": {"blocking": False}}}


def get(task_id):
    return {"jsonrpc": "2.0", "id": str(uuid4()), "method": "tasks/get",
            "params": {"id": task_id}}


def completed(client, task_id):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        task = client.post("/", json=get(task_id), headers=AUTH).json()["result"]
        if task["status"]["state"] in {"completed", "failed"}:
            return task
        time.sleep(0.02)
    raise AssertionError("Task did not finish")


class ModelAgentTests(unittest.TestCase):
    def setUp(self):
        self.state = Path(tempfile.mkdtemp(prefix="exo-sf-agents-unit-", dir="/tmp"))
        self.roles = {"synthesis": FakeRole()}

    def app(self, port=45748, **kw):
        return create_app(self.state, port, role="synthesis", capability="report_synthesis@1",
                          model_provider="scripted", roles=self.roles, **kw)

    def test_replay_conflict_card_pin_and_artifact(self):
        with TestClient(self.app()) as client:
            card = client.get("/.well-known/agent-card.json").json()
            contract = client.get("/contract", headers=AUTH).json()
            self.assertEqual(contract["capability"], "report_synthesis@1")
            self.assertEqual([skill["id"] for skill in card["skills"]], ["report_synthesis@1"])
            self.assertEqual(len(card["capabilities"]["extensions"]), 1)
            extension = card["capabilities"]["extensions"][0]
            self.assertEqual(extension["params"]["contract_digest"], digest(contract))
            with patch.object(agent_binding, "read_json", side_effect=lambda url:
                              card if url.endswith("agent-card.json") else contract):
                pinned = agent_binding.pin("http://127.0.0.1:45748", extension["params"]["identity"])
            self.assertEqual(pinned["reconcile"], "a2a-idempotent-resend")
            first = client.post("/", json=send(command()), headers=AUTH).json()["result"]
            self.assertEqual(first["status"]["state"], "working")
            again = client.post("/", json=send(command()), headers=AUTH).json()["result"]
            self.assertEqual(again["id"], first["id"])
            changed = command()
            changed["brief"] = canonical({"revision": "r2"})
            self.assertEqual(client.post("/", json=send(changed), headers=AUTH).json()["error"]["code"], -32602)
            done = completed(client, first["id"])
            self.assertEqual(done["status"]["state"], "completed")
            self.assertEqual(len(done["artifacts"]), 1)
            artifact = done["artifacts"][0]
            data = artifact["parts"][0]["data"]
            self.assertEqual(artifact["artifactId"], data["sha256"])
            self.assertEqual(data["sha256"], hashlib.sha256(data["content"].encode()).hexdigest())
            self.assertEqual(data["author"], extension["params"]["identity"])
            self.assertEqual(json.loads(data["content"])["revision"], "r1")
            self.assertEqual(len(client.get("/_test/observe", headers=AUTH).json().get("tasks", [])), 0)

    def test_restart_keeps_identity_task_and_distinct_sessions(self):
        with TestClient(self.app()) as client:
            one = client.post("/", json=send(command()), headers=AUTH).json()["result"]
            completed(client, one["id"])
            identity = client.get("/health").json()["identity"]
        with TestClient(self.app(port=45749, test_controls=True)) as client:
            self.assertEqual(client.get("/health").json()["identity"], identity)
            self.assertEqual(client.get("/health").json()["incarnation"], 2)
            self.assertEqual(completed(client, one["id"])["id"], one["id"])
            self.assertEqual(client.post("/", json=send(command()), headers=AUTH).json()["result"]["id"], one["id"])
            two = client.post("/", json=send(command("a2", "run-2")), headers=AUTH).json()["result"]
            completed(client, two["id"])
            calls = client.get("/_test/observe", headers=AUTH).json()["model_calls"]
            self.assertEqual(len(calls), 2)
            self.assertEqual({row["session_id"] for row in calls},
                             {f"{identity}:{one['id']}", f"{identity}:{two['id']}"})
            self.assertTrue(all(row["live"] is False and row["model_id"] == "gpt-6-sol"
                                for row in calls))

    def test_unfinished_task_recovers_after_restart(self):
        async def stalled(self, messages, tool_specs=None, system_prompt=None, **kwargs):
            await asyncio.sleep(30)
            yield {"messageStart": {"role": "assistant"}}

        with patch.object(model_agent.ScriptedModel, "stream", stalled):
            with TestClient(self.app()) as client:
                first = client.post("/", json=send(command()), headers=AUTH).json()["result"]
                identity = client.get("/health").json()["identity"]
                self.assertEqual(first["status"]["state"], "working")
        with TestClient(self.app(port=45749, test_controls=True)) as client:
            self.assertEqual(client.get("/health").json()["identity"], identity)
            done = completed(client, first["id"])
            self.assertEqual(done["status"]["state"], "completed")
            self.assertEqual(client.post("/", json=send(command()), headers=AUTH).json()["result"]["id"], first["id"])

    def test_budget_exhaustion_is_fixed_failure(self):
        self.roles["synthesis"] = FakeRole(fail=True)
        with TestClient(self.app(test_controls=True)) as client:
            task = client.post("/", json=send(command()), headers=AUTH).json()["result"]
            failed = completed(client, task["id"])
            self.assertEqual(failed["status"]["state"], "failed")
            self.assertEqual(failed["status"]["message"]["parts"][0]["text"], "Agent work failed.")
            self.assertFalse(failed.get("artifacts"))
            calls = client.get("/_test/observe", headers=AUTH).json()["model_calls"]
            self.assertEqual(len(calls), 3)
            self.assertEqual([row["call_no"] for row in calls], [1, 2, 3])
            self.assertTrue(all(row["outcome_kind"] == "validation-error" for row in calls))

    def test_stimulus_next_run_revision_and_hash(self):
        with TestClient(self.app(test_controls=True)) as client:
            body = {"append_claim": {"text": "Planted defect.", "evidence": ["E1"]},
                    "revisions": ["r1"]}
            self.assertTrue(client.post("/_test/stimulus", json=body, headers=AUTH).json()["armed"])
            first = client.post("/", json=send(command()), headers=AUTH).json()["result"]
            done = completed(client, first["id"])
            artifact = done["artifacts"][0]["parts"][0]["data"]
            content = json.loads(artifact["content"])
            self.assertEqual(content["claims"][-1]["id"], "C2")
            self.assertIn("Planted defect.", content["markdown"])
            self.assertEqual(artifact["sha256"], hashlib.sha256(artifact["content"].encode()).hexdigest())
            repair = client.post("/", json=send(command("a2", "run-1", "r2")), headers=AUTH).json()["result"]
            repaired = completed(client, repair["id"])
            self.assertNotIn("Planted defect.", repaired["artifacts"][0]["parts"][0]["data"]["content"])
            next_run = client.post("/", json=send(command("a3", "run-2")), headers=AUTH).json()["result"]
            completed(client, next_run["id"])
            log = client.get("/_test/stimulus-log", headers=AUTH).json()
            self.assertEqual(log, [{"run_id": "run-1", "revision": "r1", "task_id": first["id"],
                                    "planted_text": "Planted defect.", "sha256_after": artifact["sha256"]}])
            self.assertNotIn("/_test/stimulus", [route.path for route in self.app().routes])

    def test_stimulus_waits_for_next_new_run(self):
        with TestClient(self.app(test_controls=True)) as client:
            prior = client.post("/", json=send(command("prior", "old-run")), headers=AUTH).json()["result"]
            completed(client, prior["id"])
            body = {"append_claim": {"text": "Next run only.", "evidence": ["E1"]}, "revisions": "all"}
            client.post("/_test/stimulus", json=body, headers=AUTH)
            same_run = client.post("/", json=send(command("same", "old-run", "r2")), headers=AUTH).json()["result"]
            old = completed(client, same_run["id"])
            self.assertNotIn("Next run only.", old["artifacts"][0]["parts"][0]["data"]["content"])
            new_run = client.post("/", json=send(command("new", "new-run")), headers=AUTH).json()["result"]
            done = completed(client, new_run["id"])
            self.assertIn("Next run only.", done["artifacts"][0]["parts"][0]["data"]["content"])
            self.assertEqual([row["run_id"] for row in client.get("/_test/stimulus-log", headers=AUTH).json()],
                             ["new-run"])


if __name__ == "__main__":
    unittest.main()
