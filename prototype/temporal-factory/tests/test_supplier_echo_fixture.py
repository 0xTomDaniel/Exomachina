"""End-to-end local A2A SDK proof for the plain-message supplier echo fixture."""
from __future__ import annotations

import json
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from urllib.error import URLError
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

    def start_service(self, *extra: str) -> dict:
        self.process = subprocess.Popen(
            [sys.executable, str(SERVICES / "supplier_echo_fixture.py"),
             "--state", str(self.state), "--port", str(self.port), *extra],
            cwd=ROOT, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                self.fail(f"supplier echo fixture exited during startup: {self.process.returncode}")
            try:
                return agent_binding.card_observation(self.url)
            except (URLError, TimeoutError, ConnectionError, OSError,
                    agent_binding.UnavailableBinding):
                time.sleep(0.05)
        self.fail("supplier echo fixture did not become ready")

    def fixture_rows(self) -> list[tuple]:
        with sqlite3.connect(self.state / "supplier-echo.sqlite3") as db:
            return db.execute("SELECT * FROM messages").fetchall()

    def test_plain_message_fanout_and_restart_reconcile_original_task(self):
        first = self.start_service("--drop-first-response")
        identity = first["identity"]
        self.snapshot.write_text(json.dumps({"snapshot_version": 1,
            "agents": {identity: {"url": self.url}}}))
        pin = agent_binding.pin(self.url, identity)
        self.assertEqual(pin["reconcile"], "a2a-idempotent-resend")
        long_client.resolve_pinned(self.snapshot, identity, pin)

        parent = ParentAssignment(
            task_id="original-parent-a2a-task-7",
            run_id="parent-run-from-runtime",
            definition_digest="a" * 64,
            assignment_id="parent-assignment-from-runtime",
            attempt_id="parent-attempt-from-runtime",
        )
        child = SupplierRequest(
            identity=identity,
            role="supplier-echo-fixture",
            contract=pin,
            action_id="supplier-child-action-7",
            run_id="child-run-from-runtime",
            definition_digest="b" * 64,
            assignment_id="child-assignment-from-runtime",
            attempt_id="child-attempt-from-runtime",
            expected_revision=supplier_echo_fixture.REVISION,
            payload={"fixture_input": "deterministic", "sample": 7},
        )

        self.journal = OutcomeJournal(self.database)
        fanout = SupplierFanout(self.journal, self.snapshot, max_fanout=1)
        unknown = fanout.dispatch_child(parent, child)
        self.assertEqual(unknown["phase"], "unknown")
        self.assertIsNone(unknown["task_id"])
        self.assertEqual(self.process.wait(timeout=10), 23)

        # Restart only the synthetic fixture service. It retains its normal
        # UUID identity and its committed Task for the journaled messageId.
        self.start_service()
        self.journal.close()
        self.journal = OutcomeJournal(self.database)
        restarted_fanout = SupplierFanout(self.journal, self.snapshot, max_fanout=1)
        recovered = restarted_fanout.reconcile_child(parent, child)

        self.assertEqual(recovered["phase"], "confirmed")
        self.assertEqual(recovered["parent_task_id"], parent.task_id)
        self.assertNotEqual(recovered["task_id"], parent.task_id)
        self.assertEqual(str(UUID(recovered["task_id"])), recovered["task_id"])
        self.assertEqual(recovered["artifact"]["author"], identity)
        self.assertEqual(recovered["artifact"]["revision"], child.expected_revision)
        receipt = self.journal.get(child.action_id).receipt
        self.assertEqual((receipt["parent_task_id"], receipt["assignment_id"]),
                         (parent.task_id, child.assignment_id))
        self.assertEqual(len(self.fixture_rows()), 1)

        # A confirmed replay is journal-only and cannot create another effect.
        self.assertEqual(restarted_fanout.reconcile_child(parent, child), recovered)
        self.assertEqual(len(self.fixture_rows()), 1)

        # The fixture never stored a factory binding.
        with sqlite3.connect(self.state / "supplier-echo.sqlite3") as db:
            dump = "\n".join(db.iterdump())
        for value in ("parent-run-from-runtime", "child-run-from-runtime", "a" * 64, "b" * 64,
                      "supplier-child-action-7", "child-assignment-from-runtime",
                      "parent-assignment-from-runtime", parent.task_id):
            self.assertNotIn(value, dump)


if __name__ == "__main__":
    unittest.main()
