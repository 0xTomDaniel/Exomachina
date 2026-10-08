"""End-to-end local A2A SDK proof for the explicitly pinned echo fixture."""
from __future__ import annotations

import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from urllib.error import URLError
from urllib.request import Request, urlopen
from uuid import UUID

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SERVICES = ROOT / "services"
sys.path.insert(0, str(SRC))
sys.path.insert(0, str(SERVICES))

import agent_binding
import long_client
import supplier_echo_fixture
from a2a_outcome import OutcomeJournal
from supplier import ParentAssignment, SupplierFanout, SupplierRequest


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def _request_json(url: str, path: str, value: dict | None = None) -> dict:
    data = None if value is None else json.dumps(value).encode("utf-8")
    request = Request(url.rstrip("/") + path, data=data,
        method="GET" if data is None else "POST",
        headers={"Authorization": long_client.TOKEN,
                 "Content-Type": "application/json"})
    with urlopen(request, timeout=3) as response:
        return json.loads(response.read())


class SupplierEchoFixtureA2ATests(unittest.TestCase):
    def setUp(self):
        self.directory = Path(tempfile.mkdtemp(prefix="exo-proto-supplier-echo-", dir="/tmp"))
        self.state = self.directory / "supplier-state"
        self.snapshot = self.directory / "agent_snapshot.json"
        self.database = self.directory / "outcomes.sqlite3"
        self.port = _free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.process: subprocess.Popen | None = None
        self.journal: OutcomeJournal | None = None

    def tearDown(self):
        if self.journal is not None:
            self.journal.close()
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            self.process.wait(timeout=5)

    def start_service(self) -> dict:
        self.process = subprocess.Popen(
            [sys.executable, str(SERVICES / "supplier_echo_fixture.py"),
             "--state", str(self.state), "--port", str(self.port)],
            cwd=ROOT, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                self.fail(f"supplier echo fixture exited during startup: {self.process.returncode}")
            try:
                return _request_json(self.url, "/health")
            except (URLError, TimeoutError, ConnectionError, OSError):
                time.sleep(0.05)
        self.fail("supplier echo fixture did not become ready")

    def test_declared_echo_contract_fanout_and_restart_reconcile_original_task(self):
        first_health = self.start_service()
        identity = first_health["identity"]
        self.snapshot.write_text(json.dumps({"snapshot_version": 1,
            "agents": {identity: {"url": self.url}}}))
        pin = agent_binding.pin(self.url, identity)
        self.assertEqual(pin["reconcile"], "opaque")  # fixture-lookup is not generic resend
        _resolved_url, observed = long_client.resolve_pinned(self.snapshot, identity, pin)
        self.assertEqual(observed["contract_document"]["reconcile"], "fixture-lookup")
        self.assertEqual(observed["contract_document"]["supplier_assignment_echo"],
                         {"version": 1,
                          "fields": supplier_echo_fixture.ECHO_FIELDS})

        parent = ParentAssignment(
            task_id="original-parent-a2a-task-7",
            run_id="parent-run-from-runtime",
            definition_digest="a" * 64,
            assignment_id="parent-assignment-from-runtime",
            attempt_id="parent-attempt-from-runtime",
        )
        child = SupplierRequest(
            identity=identity,
            role="nested-supplier-echo-fixture",
            contract=pin,
            action_id="supplier-child-action-7",
            run_id="child-run-from-runtime",
            definition_digest="b" * 64,
            assignment_id="child-assignment-from-runtime",
            attempt_id="child-attempt-from-runtime",
            expected_revision=supplier_echo_fixture.REVISION,
            payload={"fixture_input": "deterministic", "sample": 7},
        )

        _request_json(self.url, "/_test/faults", {
            "drop_response_once_for": child.action_id,
        })
        self.journal = OutcomeJournal(self.database)
        fanout = SupplierFanout(self.journal, self.snapshot, max_fanout=1)
        unknown = fanout.dispatch_child(parent, child)
        self.assertEqual(unknown["phase"], "unknown")
        self.assertIsNone(unknown["task_id"])
        self.assertEqual(self.process.wait(timeout=10), 23)

        # Restart only the synthetic fixture service. It retains its normal
        # UUID identity and the committed original Task mapping.
        second_health = self.start_service()
        self.assertEqual(second_health["identity"], identity)
        self.assertGreater(second_health["incarnation"], first_health["incarnation"])
        self.journal.close()
        self.journal = OutcomeJournal(self.database)
        restarted_fanout = SupplierFanout(self.journal, self.snapshot, max_fanout=1)
        recovered = restarted_fanout.reconcile_child(parent, child)

        self.assertEqual(recovered["phase"], "confirmed")
        self.assertEqual(recovered["parent_task_id"], parent.task_id)
        self.assertNotEqual(recovered["task_id"], parent.task_id)
        self.assertEqual(str(UUID(recovered["task_id"])), recovered["task_id"])
        self.assertEqual(recovered["artifact"]["parent_task_id"], parent.task_id)
        self.assertEqual(recovered["artifact"]["assignment_id"], child.assignment_id)
        self.assertEqual(recovered["artifact"]["author"], identity)
        self.assertEqual(recovered["artifact"]["revision"], child.expected_revision)
        self.assertEqual(_request_json(self.url, "/_test/effects")["count"], 1)

        # A confirmed replay is journal-only and cannot create another effect.
        self.assertEqual(restarted_fanout.reconcile_child(parent, child), recovered)
        self.assertEqual(_request_json(self.url, "/_test/effects")["count"], 1)


if __name__ == "__main__":
    unittest.main()
