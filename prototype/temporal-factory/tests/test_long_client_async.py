"""In-test A2A v1.0 wire stub for async, resend and artifact incidents."""
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
import a2a_extensions
import adapter
import agent_binding
from model_usage import ModelUsageJournal
from a2a_outcome import (OutcomeJournal, Phase, ReceiverKind, StaleOutcome,
                         submitted, task_incident, task_started)


FACTORY_NAMES = ("run_id", "assignment_id", "attempt_id", "action_id", "definition_digest",
                 "factory_id", "run-1", "d" * 64)


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
        self.server.gets.append(self.path)
        if self.path.endswith("agent-card.json"):
            self.reply(self.server.card)
        else:
            self.send_error(404)

    def do_POST(self):
        raw = self.rfile.read(int(self.headers["Content-Length"]))
        self.server.bodies.append(raw.decode())
        rpc = json.loads(raw)
        self.server.headers.append((self.headers.get("A2A-Version"),
                                    self.headers.get("A2A-Extensions")))
        if rpc["method"] == "SendMessage":
            self.server.return_immediately.append(
                rpc["params"]["configuration"]["returnImmediately"])
            message = rpc["params"]["message"]
            part = message["parts"][0]
            assert "kind" not in part and message["role"] == "ROLE_USER"
            assert len(message["parts"]) == 1 and isinstance(part["text"], str)
            message_id = message["messageId"]
            if message_id not in self.server.tasks:
                self.server.tasks[message_id] = {"id": "remote-task-1", "brief": part["text"],
                                                 "context_id": message["contextId"],
                                                 "accepted": time.monotonic()}
                self.server.effects += 1
            elif part["text"] != self.server.tasks[message_id]["brief"]:
                self.reply({"jsonrpc": "2.0", "id": rpc["id"], "error": {"code": -32000}})
                return
            if self.server.drop_once:
                self.server.drop_once = False
                self.close_connection = True
                return
            task = {"task": self.server.task(message_id)}
        else:
            assert rpc["method"] == "GetTask"
            remote_id = rpc["params"]["id"]
            task = next(self.server.task(message_id) for message_id, value in self.server.tasks.items()
                        if value["id"] == remote_id)
        self.reply({"jsonrpc": "2.0", "id": rpc["id"], "result": task})


class StubServer(ThreadingHTTPServer):
    def __init__(self, address):
        super().__init__(address, StubHandler)
        self.identity = "stub-agent-identity"
        self.card = {"name": "counter evidence",
                     "supportedInterfaces": [{"url": f"http://127.0.0.1:{address[1]}",
                                              "protocolBinding": "JSONRPC",
                                              "protocolVersion": "1.0"}],
                     "skills": [{"id": "counter_evidence@1"}],
                     "capabilities": {"extensions": [
                         {"uri": agent_binding.EXTENSION_URI, "required": False,
                          "params": {"identity": self.identity,
                                     "resend": a2a_extensions.RESEND_RULE}},
                         {"uri": a2a_extensions.BUDGET_URI, "required": False}]}}
        self.tasks = {}
        self.effects = 0
        self.drop_once = False
        self.mismatch = False
        self.report = {"incurred": {"tokens": {"input": 11, "output": 7}}}
        self.return_immediately = []
        self.headers = []
        self.bodies = []
        self.gets = []

    def task(self, message_id):
        import hashlib
        value = self.tasks[message_id]
        completed = time.monotonic() - value["accepted"] >= 0.15
        metadata = {"agent_identity": self.identity}
        if completed and self.report is not None:
            metadata[a2a_extensions.BUDGET_URI] = self.report
        task = {"id": value["id"], "contextId": value["context_id"], "metadata": metadata,
                "status": {"state": "TASK_STATE_COMPLETED" if completed
                           else "TASK_STATE_WORKING"}}
        if completed:
            content = "fixture-result:" + value["brief"]
            artifact = {"revision": "r3" if self.mismatch else "r2",
                        "sha256": hashlib.sha256(content.encode()).hexdigest(),
                        "author": self.identity, "content": content}
            task["artifacts"] = [{"artifactId": artifact["sha256"],
                                  "parts": [{"data": artifact}]}]
        return task


class AsyncClientTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = StubServer(("127.0.0.1", 46451))
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
        self.server.report = {"incurred": {"tokens": {"input": 11, "output": 7}}}
        self.server.return_immediately = []
        self.server.headers = []
        self.server.bodies = []
        self.server.gets = []
        self.home = Path(tempfile.mkdtemp(prefix="exo-qual-a-unit-", dir="/tmp"))
        (self.home / "testbed").mkdir()
        (self.home / "runner").mkdir()
        self.url = self.server.card["supportedInterfaces"][0]["url"]
        (self.home / "testbed" / "agent_snapshot.json").write_text(json.dumps({
            "snapshot_version": 1, "agents": {self.server.identity: {"url": self.url}}}))
        self.binding = {"url": self.url, "identity": self.server.identity,
                        "role": "capability", "approved": True}
        self.contract = agent_binding.pin(self.url, self.server.identity)
        self.command = {"action_id": "run-1:counter_beta", "run_id": "run-1",
                        "definition_digest": "d" * 64, "assignment_id": "assignment-1",
                        "attempt_id": "attempt-1", "factory_id": "factory-1",
                        "brief": json.dumps({"kind": "fixture", "revision": "r2"})}

    def invoke(self):
        with patch.dict("os.environ", {"EXO_OUTCOME_DB": str(self.home / "runner" / "outcomes.sqlite3")}):
            return adapter._invoke_async(self.binding, self.contract, self.command, "r2", "research")

    def journal(self):
        with sqlite3.connect(self.home / "runner" / "outcomes.sqlite3") as db:
            return json.loads(db.execute("SELECT value FROM outcomes").fetchone()[0])

    def test_working_task_is_journaled_and_completed(self):
        result = self.invoke()
        self.assertEqual(self.server.effects, 1)
        self.assertEqual(self.server.return_immediately, [True])
        record = self.journal()
        self.assertEqual(record["task_id"], "remote-task-1")
        self.assertEqual(record["phase"], "confirmed")
        message_id = next(iter(self.server.tasks))
        self.assertEqual(record["message_id"], message_id)
        self.assertEqual(record["context_id"], self.server.tasks[message_id]["context_id"])
        self.assertEqual(result["artifact"]["revision"], "r2")
        self.assertEqual((result["action_id"], result["run_id"], result["context_id"]),
                         ("run-1:counter_beta", "run-1", record["context_id"]))
        # Every v1 request carries the version header and activates only the
        # generic budget extension; the only GET is Agent Card discovery.
        self.assertTrue(self.server.headers)
        self.assertEqual(set(self.server.headers), {("1.0", a2a_extensions.BUDGET_URI)})
        self.assertTrue(all(path.endswith("/.well-known/agent-card.json")
                            for path in self.server.gets))

    def test_no_factory_identifier_crosses_the_wire(self):
        self.invoke()
        wire = "\n".join(self.server.bodies)
        for name in FACTORY_NAMES + ("assignment-1", "attempt-1", "factory-1",
                                     "run-1:counter_beta"):
            self.assertNotIn(name, wire)

    def test_agent_reported_usage_is_recorded_factory_side(self):
        self.invoke()
        rows = ModelUsageJournal(self.home / "runner" / "agent-usage.sqlite3").list_measurements(
            run_id="run-1")
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual((row["service_identity"], row["task_id"], row["assignment_id"],
                          row["attempt_id"], row["action_id"]),
                         (self.server.identity, "remote-task-1", "assignment-1", "attempt-1",
                          "run-1:counter_beta"))
        self.assertEqual(row["evidence_status"], "agent_reported")
        self.assertEqual(row["completeness"], "partial")
        self.assertEqual(row["usage"]["input_tokens"], {"value": 11, "status": "reported"})
        self.assertEqual(row["usage"]["total_tokens"], {"value": None, "status": "unavailable"})

    def test_missing_usage_report_stays_unknown(self):
        self.server.report = None
        self.invoke()
        rows = ModelUsageJournal(self.home / "runner" / "agent-usage.sqlite3").list_measurements(
            run_id="run-1")
        self.assertEqual([row["evidence_status"] for row in rows], ["unknown"])
        self.assertTrue(all(item["value"] is None for item in rows[0]["usage"].values()))

    def test_lost_reply_resends_exact_message_once(self):
        self.server.drop_once = True
        result = self.invoke()
        self.assertEqual(result["task_id"], "remote-task-1")
        self.assertEqual(self.server.effects, 1)
        self.assertEqual(self.server.return_immediately, [True, True])
        sends = [json.loads(body)["params"]["message"] for body in self.server.bodies
                 if json.loads(body)["method"] == "SendMessage"]
        self.assertEqual(sends[0], sends[1])

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
        self.assertEqual(self.server.return_immediately, [True])

    def test_incomplete_async_pin_is_incident(self):
        contract = {"reconcile": "a2a-idempotent-resend"}
        with patch.dict("os.environ", {"EXO_OUTCOME_DB": str(self.home / "runner" / "outcomes.sqlite3")}):
            result = adapter._invoke_async(self.binding, contract, self.command, "r2", "research")
        self.assertEqual(result["unresolved"], "pinned-agent-verification-failed")
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
