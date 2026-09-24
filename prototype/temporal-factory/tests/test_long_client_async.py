"""In-test A2A 0.3 wire stub for async, resend and artifact incidents."""
import json
import asyncio
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import adapter
import agent_binding
import fixture
from a2a_outcome import (OutcomeJournal, Phase, ReceiverKind, StaleOutcome,
                         submitted, task_incident, task_started)


class StubHandler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def reply(self, value):
        body = json.dumps(value).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.endswith("agent-card.json"):
            self.reply(self.server.card)
        elif self.path == "/contract":
            self.reply(self.server.contract)
        else:
            self.send_error(404)

    def do_POST(self):
        rpc = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if rpc["method"] == "message/send":
            self.server.blocking.append(rpc["params"]["configuration"]["blocking"])
            command = rpc["params"]["message"]["parts"][0]["data"]
            action_id = command["action_id"]
            if action_id not in self.server.tasks:
                self.server.tasks[action_id] = {"id": "remote-task-1", "command": command,
                                                "accepted": time.monotonic()}
                self.server.effects += 1
            elif command != self.server.tasks[action_id]["command"]:
                self.reply({"jsonrpc": "2.0", "id": rpc["id"], "error": {"code": -32000}})
                return
            if self.server.drop_once:
                self.server.drop_once = False
                self.close_connection = True
                return
            task = self.server.task(action_id)
        else:
            remote_id = rpc["params"]["id"]
            task = next(self.server.task(action_id) for action_id, value in self.server.tasks.items()
                        if value["id"] == remote_id)
        self.reply({"jsonrpc": "2.0", "id": rpc["id"], "result": task})


class StubServer(ThreadingHTTPServer):
    def __init__(self, address):
        super().__init__(address, StubHandler)
        self.identity = "stub-agent-identity"
        self.contract = {"name": agent_binding.CONTRACT,
                         "reconcile": "a2a-idempotent-resend",
                         "idempotency": {"key": "action_id", "same_payload": "original_task_id",
                                         "commit_before_response": True}}
        self.card = {"name": "counter evidence", "url": f"http://127.0.0.1:{address[1]}",
                     "skills": [{"id": "counter_evidence@1"}],
                     "capabilities": {"extensions": [{"uri": agent_binding.EXTENSION_URI,
                         "required": True, "params": {"identity": self.identity,
                         "contract": agent_binding.CONTRACT,
                         "contract_digest": agent_binding.digest(self.contract)}}]}}
        self.tasks = {}
        self.effects = 0
        self.drop_once = False
        self.mismatch = False
        self.blocking = []

    def task(self, action_id):
        value = self.tasks[action_id]
        command = value["command"]
        completed = time.monotonic() - value["accepted"] >= 0.15
        metadata = {key: command[key] for key in ("action_id", "run_id", "definition_digest")}
        metadata["agent_identity"] = self.identity
        task = {"kind": "task", "id": value["id"], "metadata": metadata,
                "status": {"state": "completed" if completed else "working"}}
        if completed:
            content = "fixture-result:" + command["brief"]
            import hashlib
            artifact = {"revision": "r2", "sha256": hashlib.sha256(content.encode()).hexdigest(),
                        "author": self.identity, "content": content,
                        "action_id": action_id, "run_id": "wrong" if self.mismatch else command["run_id"],
                        "definition_digest": command["definition_digest"]}
            task["artifacts"] = [{"artifactId": artifact["sha256"],
                                  "parts": [{"kind": "data", "data": artifact}]}]
        return task


class AsyncClientTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = StubServer(("127.0.0.1", 46220))
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def setUp(self):
        self.server.tasks = {}
        self.server.effects = 0
        self.server.drop_once = False
        self.server.mismatch = False
        self.server.blocking = []
        self.home = Path(tempfile.mkdtemp(prefix="exo-qual-a-unit-", dir="/tmp"))
        (self.home / "testbed").mkdir()
        (self.home / "runner").mkdir()
        self.url = self.server.card["url"]
        (self.home / "testbed" / "agent_snapshot.json").write_text(json.dumps({
            "snapshot_version": 1, "agents": {self.server.identity: {"url": self.url}}}))
        self.binding = {"url": self.url, "identity": self.server.identity,
                        "role": "capability", "approved": True}
        self.contract = agent_binding.pin(self.url, self.server.identity)
        self.command = fixture.assignment("run-1", "d" * 64, "counter_beta",
                                          result_type="counter_evidence",
                                          scope_status="requires_scope")

    def invoke(self):
        with patch.dict("os.environ", {"EXO_OUTCOME_DB": str(self.home / "runner" / "outcomes.sqlite3")}):
            return adapter._invoke_async(self.binding, self.contract, self.command)

    def journal(self):
        with sqlite3.connect(self.home / "runner" / "outcomes.sqlite3") as db:
            return json.loads(db.execute("SELECT value FROM outcomes").fetchone()[0])

    def test_working_task_is_journaled_and_completed(self):
        result = self.invoke()
        self.assertEqual(self.server.effects, 1)
        self.assertEqual(self.server.blocking, [False])
        self.assertEqual(self.journal()["task_id"], "remote-task-1")
        self.assertEqual(self.journal()["phase"], "confirmed")
        fixture.branch_value(result, "counter_beta", run_id="run-1",
            definition_digest="d" * 64, result_type="counter_evidence",
            scope_status="requires_scope")

    def test_lost_reply_resends_exact_payload_once(self):
        self.server.drop_once = True
        result = self.invoke()
        self.assertEqual(result["task_id"], "remote-task-1")
        self.assertEqual(self.server.effects, 1)
        self.assertEqual(self.server.blocking, [False, False])

    def test_mismatched_artifact_is_incident(self):
        self.server.mismatch = True
        result = self.invoke()
        self.assertEqual(result["unresolved"], "async-artifact-inconsistent")
        self.assertEqual(self.journal()["phase"], "incident")
        self.assertEqual(self.server.effects, 1)

    def test_opaque_lost_response_never_resends(self):
        self.contract["reconcile"] = "opaque"
        self.server.drop_once = True
        result = self.invoke()
        self.assertEqual(result["unresolved"], "opaque-effect-unknown")
        self.assertEqual(self.server.effects, 1)
        self.assertEqual(self.server.blocking, [False])

    def test_incomplete_async_pin_does_not_fall_back_to_legacy_send(self):
        inp = {"run": "run-1", "digest": "d" * 64, "instance": "counter_beta",
               "result_type": "counter_evidence", "scope_status": "requires_scope",
               "url": self.url, "identity": self.server.identity,
               "binding": self.binding, "lookup_supported": True,
               "contract": {"reconcile": "a2a-idempotent-resend"}}
        with patch.object(adapter, "_invoke") as legacy, patch.object(adapter, "_invoke_async") as async_call:
            result = asyncio.run(adapter.assign(inp))
        self.assertEqual(result["unresolved"], "async-pin-incomplete")
        legacy.assert_not_called()
        async_call.assert_not_called()
        self.assertEqual(self.server.effects, 0)

    def test_stale_attempt_cannot_overwrite_incident(self):
        path = self.home / "runner" / "stale.sqlite3"
        first = OutcomeJournal(path)
        second = OutcomeJournal(path)
        try:
            base, created = first.begin(submitted("action-1", "run-1", "d" * 64,
                ReceiverKind.PARTICIPATING))
            self.assertTrue(created)
            stale_working = task_started(base, "remote-task-1")
            incident = task_incident(base, "pinned-agent-verification-failed")
            second.put(incident)
            with self.assertRaises(StaleOutcome):
                first.put(stale_working)
            current = first.get("action-1")
            self.assertEqual(current.phase, Phase.INCIDENT)
            self.assertEqual(current.reason, "pinned-agent-verification-failed")
            with self.assertRaises(StaleOutcome):
                first.put(task_started(base, "remote-task-1"))
        finally:
            first.close()
            second.close()

    def test_heartbeat_runs_on_activity_event_loop(self):
        event_loop_thread = threading.get_ident()
        observed = []
        with patch.object(adapter.activity, "heartbeat",
                          side_effect=lambda: observed.append(threading.get_ident())):
            asyncio.run(adapter._thread_with_heartbeat(time.sleep, 2.1))
        self.assertEqual(observed, [event_loop_thread])


if __name__ == "__main__":
    unittest.main()
