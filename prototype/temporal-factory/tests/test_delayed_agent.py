"""Wire and durability tests for the independent delayed A2A agent."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "services"))
sys.path.insert(0, str(ROOT / "src"))
import a2a_extensions  # noqa: E402
import a2a_v1  # noqa: E402
from delayed_agent import canonical, create_app, digest  # noqa: E402


AUTH = {"Authorization": "Bearer fixture-token", **a2a_v1.headers()}


def send_body(brief="counter brief", *, message_id=None, context_id="context-1"):
    return {"jsonrpc": "2.0", "id": str(uuid4()), "method": "SendMessage",
            "params": {"message": {"role": "ROLE_USER", "messageId": message_id or "message-1",
                                   "contextId": context_id,
                                   "parts": [{"text": brief, "mediaType": "text/plain"}]},
                       "configuration": {"returnImmediately": True}}}


def get_body(task_id):
    return {"jsonrpc": "2.0", "id": str(uuid4()), "method": "GetTask",
            "params": {"id": task_id}}


def http(base, path, *, body=None, authenticated=True):
    headers = {"Content-Type": "application/json"}
    if authenticated:
        headers.update(AUTH)
    request = urllib.request.Request(
        base + path, data=None if body is None else canonical(body).encode(),
        headers=headers, method="GET" if body is None else "POST")
    with urllib.request.urlopen(request, timeout=3) as response:
        return json.load(response)


class DelayedAgentTests(unittest.TestCase):
    def setUp(self):
        self.state = Path(tempfile.mkdtemp(prefix="exo-qual-a-unit-", dir="/tmp"))

    def rows(self):
        with sqlite3.connect(self.state / "delayed-agent.sqlite3") as db:
            return db.execute("SELECT COUNT(*) FROM messages").fetchone()[0]

    def test_card_working_completion_and_message_id_idempotency(self):
        with TestClient(create_app(self.state, 46210, delay_seconds=0.15)) as client:
            card = client.get("/.well-known/agent-card.json").json()
            self.assertEqual(card["supportedInterfaces"], [{
                "url": "http://127.0.0.1:46210/", "protocolBinding": "JSONRPC",
                "protocolVersion": "1.0"}])
            self.assertEqual([skill["id"] for skill in card["skills"]],
                             ["counter_evidence@1"])
            extensions = card["capabilities"]["extensions"]
            self.assertEqual(len(extensions), 1)
            self.assertEqual(extensions[0]["uri"], a2a_extensions.AGENT_URI)
            self.assertFalse(extensions[0].get("required", False))
            params = extensions[0]["params"]
            self.assertEqual(params["resend"], a2a_extensions.RESEND_RULE)
            for path in ("/contract", "/health", "/_test/effects", "/_test/faults"):
                self.assertEqual(client.get(path, headers=AUTH).status_code, 404, path)
            first = client.post("/", json=send_body(), headers=AUTH).json()["result"]["task"]
            self.assertEqual(first["status"]["state"], "TASK_STATE_WORKING")
            self.assertEqual(first["metadata"], {"agent_identity": params["identity"]})
            self.assertEqual(first["contextId"], "context-1")
            task_id = first["id"]
            again = client.post("/", json=send_body(), headers=AUTH).json()["result"]["task"]
            self.assertEqual(again["id"], task_id)
            self.assertEqual(self.rows(), 1)
            conflict = client.post("/", json=send_body("changed"), headers=AUTH).json()
            self.assertEqual(conflict["error"]["code"], -32602)
            self.assertEqual(self.rows(), 1)
            time.sleep(0.18)
            done = client.post("/", json=get_body(task_id), headers=AUTH).json()["result"]
            self.assertEqual(done["status"]["state"], "TASK_STATE_COMPLETED")
            artifact = done["artifacts"][0]
            data = artifact["parts"][0]["data"]
            self.assertEqual(data, {
                "revision": "r2", "sha256": hashlib.sha256(
                    b"fixture-result:counter brief").hexdigest(),
                "author": params["identity"], "content": "fixture-result:counter brief"})
            self.assertEqual(artifact["artifactId"], data["sha256"])

    def test_restart_port_change_keeps_identity_task_and_elapsed_delay(self):
        script = str(ROOT / "services" / "delayed_agent.py")
        args = [sys.executable, "-B", script, "--state", str(self.state)]
        old_base = "http://127.0.0.1:46211"
        first = subprocess.Popen(args + ["--port", "46211", "--delay-seconds", "0.12"],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.addCleanup(self._stop, first)
        self._ready(old_base, first)
        old_card = http(old_base, "/.well-known/agent-card.json", authenticated=False)
        task = http(old_base, "/", body=send_body())["result"]["task"]
        self.assertEqual(task["status"]["state"], "TASK_STATE_WORKING")
        self._stop(first)
        time.sleep(0.15)
        new_base = "http://127.0.0.1:46212"
        second = subprocess.Popen(args + ["--port", "46212", "--delay-seconds", "100"],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.addCleanup(self._stop, second)
        self._ready(new_base, second)
        new_card = http(new_base, "/.well-known/agent-card.json", authenticated=False)
        self.assertNotEqual(a2a_v1.card_url(old_card), a2a_v1.card_url(new_card))
        self.assertEqual(digest(a2a_v1.card_without_endpoint(old_card)),
                         digest(a2a_v1.card_without_endpoint(new_card)))
        done = http(new_base, "/", body=get_body(task["id"]))["result"]
        self.assertEqual(done["status"]["state"], "TASK_STATE_COMPLETED")
        self.assertEqual(done["metadata"]["agent_identity"],
                         task["metadata"]["agent_identity"])
        self.assertEqual(http(new_base, "/", body=send_body())["result"]["task"]["id"],
                         task["id"])
        self.assertEqual(self.rows(), 1)

    def test_mismatch_fault_and_identity_file_override(self):
        impostor_file = self.state / "impostor-identity"
        with TestClient(create_app(self.state, 46213, delay_seconds=0,
                                   identity_file=impostor_file,
                                   mismatch_artifact=True)) as client:
            card = client.get("/.well-known/agent-card.json").json()
            identity = card["capabilities"]["extensions"][0]["params"]["identity"]
            result = client.post("/", json=send_body(), headers=AUTH).json()["result"]["task"]
            data = result["artifacts"][0]["parts"][0]["data"]
            self.assertEqual(data["revision"], "r2-mismatch")
            self.assertEqual(data["author"], identity)
        with TestClient(create_app(self.state, 46214, delay_seconds=0,
                                   identity_file=impostor_file)) as client:
            again = client.get("/.well-known/agent-card.json").json()
            self.assertEqual(again["capabilities"]["extensions"][0]["params"]["identity"],
                             identity)

    def test_drop_first_response_commits_before_process_exit(self):
        port = 46215
        base = f"http://127.0.0.1:{port}"
        args = [sys.executable, "-B", str(ROOT / "services" / "delayed_agent.py"),
                "--state", str(self.state), "--port", str(port), "--delay-seconds", "0.4"]
        process = subprocess.Popen(args + ["--drop-first-response"],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.addCleanup(self._stop, process)
        self._ready(base, process)
        with self.assertRaises((OSError, urllib.error.URLError)):
            http(base, "/", body=send_body())
        self.assertEqual(process.wait(timeout=5), 23)
        with sqlite3.connect(self.state / "delayed-agent.sqlite3") as db:
            original_task_id = db.execute(
                "SELECT task_id FROM messages WHERE message_id='message-1'").fetchone()[0]
        restarted = subprocess.Popen(args + ["--drop-first-response"],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.addCleanup(self._stop, restarted)
        self._ready(base, restarted)
        # The fault fires once per state directory; the resend recovers the Task.
        task = http(base, "/", body=send_body())["result"]["task"]
        self.assertEqual(task["id"], original_task_id)
        self.assertEqual(http(base, "/", body=get_body(task["id"]))["result"]["id"], task["id"])
        self.assertEqual(self.rows(), 1)

    def test_storage_never_holds_caller_factory_identifiers(self):
        brief = canonical({"question": "q", "revision": "r1"})
        with TestClient(create_app(self.state, 46216, delay_seconds=0)) as client:
            body = send_body(brief)
            body["params"]["metadata"] = {"run_id": "run-secret", "action_id": "action-secret"}
            reply = client.post("/", json=body, headers=AUTH).json()["result"]["task"]
            self.assertNotIn("run-secret", json.dumps(reply))
        with sqlite3.connect(self.state / "delayed-agent.sqlite3") as db:
            dump = "\n".join(db.iterdump())
        for value in ("run-secret", "action-secret", "run_id", "action_id",
                      "definition_digest"):
            self.assertNotIn(value, dump)

    @staticmethod
    def _ready(base, process):
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            try:
                http(base, "/.well-known/agent-card.json", authenticated=False)
                return
            except (OSError, urllib.error.URLError):
                if process.poll() is not None:
                    raise AssertionError(f"agent exited at startup: {process.returncode}")
                time.sleep(0.05)
        raise AssertionError("agent did not become ready")

    @staticmethod
    def _stop(process):
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
