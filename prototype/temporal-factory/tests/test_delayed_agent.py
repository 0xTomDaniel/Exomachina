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
from delayed_agent import (CONTRACT_DIGEST, CONTRACT_NAME, EXTENSION_URI,  # noqa: E402
                           canonical, create_app, digest)


AUTH = {"Authorization": "Bearer fixture-token"}


def command(action_id="action-1", **changes):
    value = {"op": "assign", "action_id": action_id, "run_id": "run-1",
             "definition_digest": "definition-1", "brief": "counter brief"}
    value.update(changes)
    return value


def send_body(value, request_id=None):
    return {"jsonrpc": "2.0", "id": request_id or str(uuid4()),
            "method": "message/send",
            "params": {"message": {"role": "user", "messageId": str(uuid4()),
                                   "parts": [{"kind": "data", "data": value}]},
                       "configuration": {"blocking": False}}}


def get_body(task_id):
    return {"jsonrpc": "2.0", "id": str(uuid4()), "method": "tasks/get",
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

    def test_card_contract_working_completion_and_idempotency(self):
        with TestClient(create_app(self.state, 46210, delay_seconds=0.15)) as client:
            card = client.get("/.well-known/agent-card.json").json()
            self.assertEqual(card["protocolVersion"], "0.3.0")
            self.assertEqual([skill["id"] for skill in card["skills"]],
                             ["counter_evidence@1"])
            extensions = card["capabilities"]["extensions"]
            self.assertEqual(len(extensions), 1)
            self.assertEqual(extensions[0]["uri"], EXTENSION_URI)
            self.assertIs(extensions[0]["required"], True)
            params = extensions[0]["params"]
            self.assertEqual(set(params), {"identity", "contract", "contract_digest"})
            self.assertEqual(params["contract"], CONTRACT_NAME)
            self.assertEqual(params["contract_digest"], CONTRACT_DIGEST)
            self.assertEqual(digest(client.get("/contract", headers=AUTH).json()),
                             CONTRACT_DIGEST)
            self.assertEqual(client.get("/contract").status_code, 401)
            first = client.post("/", json=send_body(command()), headers=AUTH).json()["result"]
            self.assertEqual(first["status"]["state"], "working")
            self.assertEqual(first["metadata"], {
                "action_id": "action-1", "run_id": "run-1",
                "definition_digest": "definition-1", "agent_identity": params["identity"]})
            task_id = first["id"]
            again = client.post("/", json=send_body(command()), headers=AUTH).json()["result"]
            self.assertEqual(again["id"], task_id)
            self.assertEqual(client.get("/_test/effects", headers=AUTH).json(), {
                "identity": params["identity"], "effects": {"action-1": 1}, "total": 1})
            conflict = client.post("/", json=send_body(command(brief="changed")),
                                   headers=AUTH).json()
            self.assertEqual(conflict["error"]["code"], -32602)
            self.assertEqual(client.get("/_test/effects", headers=AUTH).json()["total"], 1)
            time.sleep(0.18)
            done = client.post("/", json=get_body(task_id), headers=AUTH).json()["result"]
            self.assertEqual(done["status"]["state"], "completed")
            self.assertEqual(len(done["artifacts"]), 1)
            artifact = done["artifacts"][0]
            data = artifact["parts"][0]["data"]
            self.assertEqual(data, {
                "revision": "r2", "sha256": hashlib.sha256(
                    b"fixture-result:counter brief").hexdigest(),
                "author": params["identity"], "content": "fixture-result:counter brief",
                "action_id": "action-1", "run_id": "run-1",
                "definition_digest": "definition-1"})
            self.assertEqual(artifact["artifactId"], data["sha256"])
            self.assertEqual(client.post("/", json=get_body(task_id),
                                         headers=AUTH).json()["result"]["id"], task_id)

    def test_restart_port_change_keeps_identity_task_and_elapsed_delay(self):
        script = str(ROOT / "services" / "delayed_agent.py")
        args = [sys.executable, "-B", script, "--state", str(self.state)]
        old_base = "http://127.0.0.1:46211"
        first = subprocess.Popen(args + ["--port", "46211", "--delay-seconds", "0.12"],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.addCleanup(self._stop, first)
        self._ready(old_base, first)
        old_card = http(old_base, "/.well-known/agent-card.json", authenticated=False)
        task = http(old_base, "/", body=send_body(command()))["result"]
        self.assertEqual(task["status"]["state"], "working")
        self._stop(first)
        time.sleep(0.15)
        new_base = "http://127.0.0.1:46212"
        second = subprocess.Popen(args + ["--port", "46212", "--delay-seconds", "100"],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.addCleanup(self._stop, second)
        self._ready(new_base, second)
        new_card = http(new_base, "/.well-known/agent-card.json", authenticated=False)
        self.assertNotEqual(old_card["url"], new_card["url"])
        self.assertEqual(digest({key: value for key, value in old_card.items()
                                 if key != "url"}),
                         digest({key: value for key, value in new_card.items()
                                 if key != "url"}))
        done = http(new_base, "/", body=get_body(task["id"]))["result"]
        self.assertEqual(done["status"]["state"], "completed")
        self.assertEqual(done["id"], task["id"])
        self.assertEqual(done["metadata"]["agent_identity"],
                         task["metadata"]["agent_identity"])
        self.assertEqual(http(new_base, "/", body=send_body(command()))
                         ["result"]["id"], task["id"])
        self.assertEqual(http(new_base, "/_test/effects")["total"], 1)

    def test_mismatch_fault_and_identity_file_override(self):
        impostor_file = self.state / "impostor-identity"
        with TestClient(create_app(self.state, 46213, delay_seconds=0,
                                   identity_file=impostor_file,
                                   mismatch_artifact_for="action-1")) as client:
            card = client.get("/.well-known/agent-card.json").json()
            result = client.post("/", json=send_body(command()), headers=AUTH).json()["result"]
            self.assertEqual(result["status"]["state"], "working")
            completed = client.post("/", json=get_body(result["id"]),
                                    headers=AUTH).json()["result"]
            data = completed["artifacts"][0]["parts"][0]["data"]
            self.assertEqual(data["run_id"], "run-1-mismatch")
            self.assertEqual(data["author"], card["capabilities"]["extensions"][0]
                             ["params"]["identity"])
            self.assertEqual(client.get("/_test/effects", headers=AUTH).json()["total"], 1)
        with TestClient(create_app(self.state, 46214, delay_seconds=0,
                                   identity_file=impostor_file)) as client:
            self.assertEqual(client.get("/health").json()["identity"],
                             card["capabilities"]["extensions"][0]["params"]["identity"])

    def test_drop_response_once_commits_before_process_exit(self):
        port = 46215
        base = f"http://127.0.0.1:{port}"
        args = [sys.executable, "-B", str(ROOT / "services" / "delayed_agent.py"),
                "--state", str(self.state), "--port", str(port),
                "--delay-seconds", "0.4"]
        process = subprocess.Popen(args + ["--drop-response-once-for", "action-1"],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.addCleanup(self._stop, process)
        self._ready(base, process)
        with self.assertRaises((OSError, urllib.error.URLError)):
            http(base, "/", body=send_body(command()))
        self.assertEqual(process.wait(timeout=5), 23)
        with sqlite3.connect(self.state / "delayed-agent.sqlite3") as db:
            original_task_id = db.execute(
                "SELECT task_id FROM actions WHERE action_id='action-1'").fetchone()[0]
        restarted = subprocess.Popen(args, stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL)
        self.addCleanup(self._stop, restarted)
        self._ready(base, restarted)
        effects = http(base, "/_test/effects")
        self.assertEqual(effects["effects"], {"action-1": 1})
        task = http(base, "/", body=send_body(command()))["result"]
        self.assertEqual(task["id"], original_task_id)
        self.assertEqual(task["metadata"]["agent_identity"], effects["identity"])
        self.assertEqual(http(base, "/", body=get_body(task["id"]))["result"]["id"],
                         task["id"])
        self.assertEqual(http(base, "/_test/effects")["total"], 1)

    @staticmethod
    def _ready(base, process):
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            try:
                http(base, "/health", authenticated=False)
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
