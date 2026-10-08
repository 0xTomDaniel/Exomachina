"""Supplier fan-out over plain A2A Messages and original-Task reconciliation proof."""
from __future__ import annotations

import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sys
import tempfile
import threading
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import a2a_extensions
import agent_binding
from a2a_outcome import OutcomeJournal, Phase
from supplier import (ParentAssignment, SupplierBindingError, SupplierFanout,
                      SupplierRequest)


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class SupplierHandler(BaseHTTPRequestHandler):
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
        if self.path == "/.well-known/agent-card.json":
            self.reply(self.server.card)
            return
        self.send_error(404)

    def do_POST(self):
        raw = self.rfile.read(int(self.headers["Content-Length"]))
        self.server.bodies.append(raw.decode())
        request = json.loads(raw)
        if request.get("method") == "SendMessage":
            assert self.headers.get("A2A-Version") == "1.0"
            self.server.sends += 1
            message = request["params"]["message"]
            assert [set(part) for part in message["parts"]] == [{"text", "mediaType"}]
            key = message["messageId"]
            if key not in self.server.tasks:
                self.server.effects += 1
                self.server.tasks[key] = {"text": message["parts"][0]["text"],
                                          "task": self.server.make_task(message)}
            if self.server.mismatch_context:
                self.server.tasks[key]["task"]["contextId"] = "wrong-context"
            if self.server.drop_next:
                self.server.drop_next = False
                self.close_connection = True
                return
            self.reply({"jsonrpc": "2.0", "id": request["id"],
                        "result": {"task": self.server.tasks[key]["task"]}})
            return
        if request.get("method") == "GetTask":
            task_id = request["params"]["id"]
            self.server.task_get_ids.append(task_id)
            saved = next((value for value in self.server.tasks.values()
                          if value["task"]["id"] == task_id), None)
            if saved is None:
                self.reply({"jsonrpc": "2.0", "id": request["id"],
                            "error": {"code": -32004, "message": "unknown Task"}})
                return
            self.reply({"jsonrpc": "2.0", "id": request["id"], "result": saved["task"]})
            return
        self.reply({"jsonrpc": "2.0", "id": request.get("id"),
                    "error": {"code": -32601, "message": "unknown method"}})


class SupplierServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, *, resend=True):
        super().__init__(address, SupplierHandler)
        self.identity = "supplier.synthetic.local"
        self.role = "supplier"
        self.resend = resend
        self.drop_next = False
        self.mismatch_context = False
        self.sends = 0
        self.effects = 0
        self.task_get_ids = []
        self.tasks = {}
        self.bodies = []
        self.gets = []
        self.refresh_card()

    def refresh_card(self):
        port = self.server_address[1]
        params = {"identity": self.identity}
        if self.resend:
            params["resend"] = a2a_extensions.RESEND_RULE
        self.card = {
            "name": "synthetic supplier",
            "supportedInterfaces": [{"url": f"http://127.0.0.1:{port}",
                                     "protocolBinding": "JSONRPC", "protocolVersion": "1.0"}],
            "skills": [{"id": "supplier@1"}],
            "capabilities": {"extensions": [{"uri": agent_binding.EXTENSION_URI,
                                             "required": False, "params": params}]},
        }

    def make_task(self, message):
        content = _canonical({"result": "synthetic-public-artifact",
                              "supplier": self.identity,
                              "input": message["parts"][0]["text"]})
        # The work product itself; the factory normalizes revision and author.
        return {"id": f"remote-task-{self.effects}", "contextId": message["contextId"],
                "metadata": {"agent_identity": self.identity},
                "status": {"state": "TASK_STATE_COMPLETED"},
                "artifacts": [{"artifactId": hashlib.sha256(content.encode()).hexdigest(),
                               "parts": [{"text": content, "mediaType": "application/json"}]}]}


class SupplierFanoutTests(unittest.TestCase):
    def setUp(self):
        self.server = SupplierServer(("127.0.0.1", 0))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.directory = Path(tempfile.mkdtemp(prefix="exo-proto-supplier-", dir="/tmp"))
        self.database = self.directory / "outcomes.sqlite3"
        self.snapshot = self.directory / "agent_snapshot.json"
        url = self.server.card["supportedInterfaces"][0]["url"]
        self.snapshot.write_text(json.dumps({"snapshot_version": 1,
            "agents": {self.server.identity: {"url": url}}}))
        self.parent = ParentAssignment("parent-task-7", "parent-run-3", "d" * 64,
                                       "parent-assignment-2", "parent-attempt-1")
        self.contract = agent_binding.pin(url, self.server.identity)
        self.journal = OutcomeJournal(self.database)
        self.fanout = SupplierFanout(self.journal, self.snapshot, max_fanout=2)

    def tearDown(self):
        self.journal.close()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def child(self, *, suffix="a", contract=None):
        return SupplierRequest(
            identity=self.server.identity,
            role=self.server.role,
            contract=contract or dict(self.contract),
            action_id=f"child-action-{suffix}",
            run_id=f"child-run-{suffix}",
            definition_digest=(suffix[0].lower() * 64),
            assignment_id=f"child-assignment-{suffix}",
            attempt_id="attempt-1",
            expected_revision="supplier-revision-test-only",
            payload={"request": f"synthetic-{suffix}"},
        )

    def test_fanout_validates_explicit_capacity_and_returns_safe_artifacts(self):
        children = [self.child(suffix="a"), self.child(suffix="b")]
        rows = self.fanout.fan_out(self.parent, children)
        self.assertEqual([row["phase"] for row in rows], ["confirmed", "confirmed"])
        self.assertEqual([row["task_id"] for row in rows],
                         ["remote-task-1", "remote-task-2"])
        self.assertTrue(all(row["artifact"]["content"].startswith("{") for row in rows))
        self.assertNotIn("payload", rows[0])
        self.assertEqual(self.server.sends, 2)
        with self.assertRaises(ValueError):
            self.fanout.fan_out(self.parent, [*children, self.child(suffix="c")])
        with self.assertRaises(ValueError):
            SupplierFanout(self.journal, self.snapshot, max_fanout=True)

    def test_no_parent_or_child_binding_crosses_the_wire(self):
        self.fanout.fan_out(self.parent, [self.child(suffix="a")])
        wire = "\n".join(self.server.bodies)
        for value in ("parent-task-7", "parent-run-3", "d" * 64, "parent-assignment-2",
                      "parent-attempt-1", "child-action-a", "child-run-a",
                      "child-assignment-a", "attempt-1", "action_id", "run_id"):
            self.assertNotIn(value, wire)
        self.assertTrue(all(path.endswith("/.well-known/agent-card.json")
                            for path in self.server.gets))

    def test_dropped_response_reconciles_same_original_task_after_restart(self):
        child = self.child()
        self.server.drop_next = True
        first = self.fanout.dispatch_child(self.parent, child)
        self.assertEqual(first["phase"], "unknown")
        self.assertIsNone(first["task_id"])
        self.assertEqual(self.server.effects, 1)
        self.assertEqual(self.server.sends, 1)
        self.journal.close()

        restarted_journal = OutcomeJournal(self.database)
        try:
            restarted = SupplierFanout(restarted_journal, self.snapshot, max_fanout=2)
            recovered = restarted.reconcile_child(self.parent, child)
            self.assertEqual(recovered["phase"], "confirmed")
            self.assertEqual(recovered["task_id"], "remote-task-1")
            self.assertEqual(recovered["artifact"]["author"], self.server.identity)
            # The resend reused the journaled messageId: no second effect.
            self.assertEqual(self.server.effects, 1)
            self.assertEqual(self.server.sends, 2)
            sends = [json.loads(body)["params"]["message"] for body in self.server.bodies
                     if json.loads(body)["method"] == "SendMessage"]
            self.assertEqual(sends[0], sends[1])
            replay = restarted.reconcile_child(self.parent, child)
            self.assertEqual(replay, recovered)
            self.assertEqual(self.server.sends, 2)
        finally:
            restarted_journal.close()
            self.journal = OutcomeJournal(self.database)

    def test_opaque_mode_stays_unknown_without_resubmit(self):
        self.server.resend = False
        self.server.refresh_card()
        contract = agent_binding.pin(self.server.card["supportedInterfaces"][0]["url"],
                                     self.server.identity)
        self.assertEqual(contract["reconcile"], "opaque")
        child = self.child(suffix="o", contract=contract)
        self.server.drop_next = True
        first = self.fanout.dispatch_child(self.parent, child)
        self.assertEqual(first["phase"], "unknown")
        second = self.fanout.reconcile_child(self.parent, child)
        self.assertEqual(second["phase"], "unknown")
        self.assertEqual(second["reason"], "submission-outcome-unknown")
        self.assertEqual(self.server.sends, 1)
        self.assertEqual(self.server.effects, 1)

    def test_parent_identity_reuse_conflicts_without_parsing_action_ids(self):
        child = self.child()
        self.fanout.dispatch_child(self.parent, child)
        changed_parent = ParentAssignment(self.parent.task_id, self.parent.run_id,
            self.parent.definition_digest, "other-parent-assignment", self.parent.attempt_id)
        with self.assertRaisesRegex(ValueError, "reused with different receiver or binding"):
            self.fanout.dispatch_child(changed_parent, child)
        self.assertEqual(self.server.sends, 1)

    def test_pinned_resend_requires_card_declaration_before_send(self):
        self.server.resend = False
        self.server.refresh_card()
        contract = dict(self.contract)
        contract["card_sha256"] = agent_binding.digest(
            {key: value for key, value in self.server.card.items()
             if key != "supportedInterfaces"})
        with self.assertRaises((SupplierBindingError, ValueError)):
            self.fanout.dispatch_child(self.parent, self.child(contract=contract))
        self.assertEqual(self.server.sends, 0)
        self.assertIsNone(self.journal.get("child-action-a"))

    def test_task_context_mismatch_is_incident(self):
        self.server.mismatch_context = True
        result = self.fanout.dispatch_child(self.parent, self.child())
        self.assertEqual(result["phase"], Phase.INCIDENT.value)
        self.assertEqual(result["reason"], "supplier-task-binding-inconsistent")
        self.assertEqual(self.server.sends, 1)
        self.assertEqual(self.fanout.reconcile_child(self.parent, self.child())["phase"],
                         Phase.INCIDENT.value)
        self.assertEqual(self.server.sends, 1)


if __name__ == "__main__":
    unittest.main()
