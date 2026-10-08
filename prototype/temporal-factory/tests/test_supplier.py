"""Fixture-only supplier fan-out and original-Task reconciliation proof."""
from __future__ import annotations

import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import agent_binding
from a2a_outcome import OutcomeJournal, Phase
from supplier import (ParentAssignment, SupplierBindingError, SupplierFanout,
                      SupplierRequest)


ECHO_FIELDS = sorted(("parent_task_id", "parent_run_id", "parent_definition_digest",
                      "parent_assignment_id", "parent_attempt_id", "action_id", "run_id",
                      "definition_digest", "assignment_id", "attempt_id"))


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
        if self.path == "/.well-known/agent-card.json":
            self.reply(self.server.card)
            return
        if self.path == "/contract":
            self.reply(self.server.contract)
            return
        if self.path.startswith("/fixture/actions/"):
            self.server.lookups += 1
            action_id = unquote(self.path.removeprefix("/fixture/actions/"))
            saved = self.server.tasks.get(action_id)
            if saved is None:
                self.send_error(404)
                return
            command = saved["command"]
            record = {key: command[key] for key in ECHO_FIELDS}
            record.update({"task_id": saved["task"]["id"],
                           "harness_identity": self.server.identity,
                           "harness_role": "nested-supplier"})
            self.reply(record)
            return
        self.send_error(404)

    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if request.get("method") == "SendMessage":
            assert self.headers.get("A2A-Version") == "1.0"
            assert self.headers.get("A2A-Extensions") == agent_binding.EXTENSION_URI
            self.server.sends += 1
            message = request["params"]["message"]
            command = message["parts"][0]["data"]
            action_id = command["action_id"]
            if action_id not in self.server.tasks:
                self.server.effects += 1
                self.server.tasks[action_id] = {
                    "command": command,
                    "task": self.server.make_task(command),
                }
            if self.server.mismatch_metadata:
                self.server.tasks[action_id]["task"]["metadata"]["assignment_id"] = "wrong"
            if self.server.drop_next:
                self.server.drop_next = False
                self.close_connection = True
                return
            self.reply({"jsonrpc": "2.0", "id": request["id"],
                        "result": {"task": self.server.tasks[action_id]["task"]}})
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
            self.reply({"jsonrpc": "2.0", "id": request["id"],
                        "result": saved["task"]})
            return
        self.reply({"jsonrpc": "2.0", "id": request.get("id"),
                    "error": {"code": -32601, "message": "unknown method"}})


class SupplierServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, *, reconciliation="fixture-lookup",
                 declares_echo=True):
        super().__init__(address, SupplierHandler)
        self.identity = "supplier.synthetic.local"
        self.role = "nested-supplier"
        self.contract = {
            "name": agent_binding.CONTRACT,
            "reconcile": reconciliation,
            "supplier_assignment_echo": ({"version": 1, "fields": ECHO_FIELDS}
                                          if declares_echo else None),
        }
        if reconciliation == "a2a-idempotent-resend":
            self.contract["idempotency"] = {
                "key": "action_id", "same_payload": "original_task_id",
                "commit_before_response": True,
            }
        self.drop_next = False
        self.mismatch_metadata = False
        self.sends = 0
        self.effects = 0
        self.lookups = 0
        self.task_get_ids = []
        self.tasks = {}
        self.refresh_card()

    def refresh_card(self):
        port = self.server_address[1]
        self.card = {
            "name": "synthetic nested supplier",
            "supportedInterfaces": [{"url": f"http://127.0.0.1:{port}",
                                     "protocolBinding": "JSONRPC", "protocolVersion": "1.0"}],
            "skills": [{"id": "nested_factory@1"}],
            "capabilities": {"extensions": [{
                "uri": agent_binding.EXTENSION_URI,
                "required": True,
                "params": {"identity": self.identity,
                           "contract": agent_binding.CONTRACT,
                           "contract_digest": agent_binding.digest(self.contract)},
            }]},
        }

    def make_task(self, command):
        number = self.effects
        echoed = {key: command[key] for key in ECHO_FIELDS}
        metadata = {key: command[key] for key in ECHO_FIELDS}
        metadata["agent_identity"] = self.identity
        content = _canonical({"result": "synthetic-public-artifact",
                              "supplier": self.identity,
                              "assignment_id": command["assignment_id"]})
        artifact = {
            **echoed,
            "author": self.identity,
            "revision": "supplier-revision-test-only",
            "content": content,
            "sha256": hashlib.sha256(content.encode()).hexdigest(),
        }
        return {
            "id": f"remote-task-{number}",
            "metadata": metadata,
            "status": {"state": "TASK_STATE_COMPLETED"},
            "artifacts": [{"artifactId": artifact["sha256"],
                           "parts": [{"data": artifact}]}],
        }


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
            self.assertEqual(self.server.lookups, 1)
            self.assertEqual(self.server.task_get_ids, ["remote-task-1"])
            self.assertEqual(self.server.effects, 1)
            self.assertEqual(self.server.sends, 1)
            replay = restarted.reconcile_child(self.parent, child)
            self.assertEqual(replay, recovered)
            self.assertEqual(self.server.task_get_ids, ["remote-task-1"])
        finally:
            restarted_journal.close()
            self.journal = OutcomeJournal(self.database)

    def test_opaque_and_generic_idempotent_modes_stay_unknown_without_resubmit(self):
        for mode in ("opaque", "a2a-idempotent-resend"):
            with self.subTest(mode=mode):
                sends_before = self.server.sends
                effects_before = self.server.effects
                lookups_before = self.server.lookups
                self.server.contract["reconcile"] = mode
                if mode == "a2a-idempotent-resend":
                    self.server.contract["idempotency"] = {
                        "key": "action_id", "same_payload": "original_task_id",
                        "commit_before_response": True,
                    }
                else:
                    self.server.contract.pop("idempotency", None)
                self.server.refresh_card()
                contract = agent_binding.pin(self.server.card["supportedInterfaces"][0]["url"], self.server.identity)
                child = self.child(suffix=mode[0], contract=contract)
                child = SupplierRequest(**{**child.__dict__, "action_id": f"action-{mode}",
                    "run_id": f"run-{mode}", "assignment_id": f"assignment-{mode}"})
                self.server.drop_next = True
                first = self.fanout.dispatch_child(self.parent, child)
                self.assertEqual(first["phase"], "unknown")
                second = self.fanout.reconcile_child(self.parent, child)
                self.assertEqual(second["phase"], "unknown")
                self.assertEqual(second["reason"], "submission-outcome-unknown")
                self.assertEqual(self.server.lookups - lookups_before, 0)
                self.assertEqual(self.server.sends - sends_before, 1)
                self.assertEqual(self.server.effects - effects_before, 1)

    def test_parent_identity_reuse_conflicts_without_parsing_action_ids(self):
        child = self.child()
        self.fanout.dispatch_child(self.parent, child)
        changed_parent = ParentAssignment(self.parent.task_id, self.parent.run_id,
            self.parent.definition_digest, "other-parent-assignment", self.parent.attempt_id)
        with self.assertRaisesRegex(ValueError, "reused with different receiver or binding"):
            self.fanout.dispatch_child(changed_parent, child)
        self.assertEqual(self.server.sends, 1)

    def test_supplier_must_declare_parent_child_echo_before_send(self):
        self.server.contract["supplier_assignment_echo"] = None
        self.server.refresh_card()
        contract = agent_binding.pin(self.server.card["supportedInterfaces"][0]["url"], self.server.identity)
        with self.assertRaisesRegex(SupplierBindingError, "does not declare"):
            self.fanout.dispatch_child(self.parent, self.child(contract=contract))
        self.assertEqual(self.server.sends, 0)
        self.assertIsNone(self.journal.get("child-action-a"))

    def test_task_parent_or_child_echo_mismatch_is_incident(self):
        self.server.mismatch_metadata = True
        result = self.fanout.dispatch_child(self.parent, self.child())
        self.assertEqual(result["phase"], Phase.INCIDENT.value)
        self.assertEqual(result["reason"], "supplier-task-binding-inconsistent")
        self.assertEqual(self.server.sends, 1)
        self.assertEqual(self.fanout.reconcile_child(self.parent, self.child())["phase"],
                         Phase.INCIDENT.value)
        self.assertEqual(self.server.sends, 1)


if __name__ == "__main__":
    unittest.main()
