"""Harness identity, public contract, publication, and agent mode boundary tests."""
from __future__ import annotations

import json
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))
sys.path.insert(0, str(ROOT / "services"))

import harness  # noqa: E402
from authoring import approve, materialize  # noqa: E402
from binding import build_id_for, source_digest  # noqa: E402
from fixture import assignment  # noqa: E402
from long_client import send  # noqa: E402
from testbed import (QUALITY_POLICY, SERVICE_NAMES, binding_records,  # noqa: E402
                     contract_records)


class StubRunner:
    """Records the runner boundary without starting local infrastructure."""

    def __init__(self, home: Path, *, port_base=None, member_base=None):
        self.home = home
        self.address = "127.0.0.1:45302"
        self.started = []
        self.built = []
        self.workers = []

    def is_running(self):
        return bool(self.started)

    def ensure_started(self, *, reason, timeout=180):
        self.started.append(reason)
        return {"reason": reason}

    def ensure_build(self, source_dir):
        self.built.append(source_dir)
        digest = source_digest(source_dir)
        return {"build_id": build_id_for(digest), "source_digest": digest}

    def wait_worker(self, build_id, timeout=60):
        self.workers.append(build_id)
        return {"build_id": build_id}


class DirectorBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.state = Path(tempfile.mkdtemp(prefix="exo-proto-harness-test-", dir="/tmp"))
        self.home = self.state / "home"
        self.instance = self.home / "instances" / "factory"
        self.config = harness.init_instance(
            self.instance, name="factory-test", mode="factory", port=45300, home=self.home)
        runner_patch = patch.object(harness, "Runner", StubRunner)
        runner_patch.start()
        self.addCleanup(runner_patch.stop)
        self.director = harness.Director(self.instance, self.config)

    def test_identity_persists_and_stale_incarnation_is_fenced(self):
        first = self.director
        second = harness.Director(self.instance, self.config)
        self.assertEqual(first.identity, second.identity)
        self.assertEqual(first.token, second.token)
        self.assertEqual(second.incarnation, first.incarnation + 1)
        with first.connect() as db, self.assertRaisesRegex(harness.Rejected, "stale"):
            first.fence(db)
        with second.connect() as db:
            second.fence(db)

    def test_recovery_starts_only_for_unfinished_runs(self):
        runner = self.director.module.runner
        self.assertIsNone(self.director.recover())
        self.assertEqual(runner.started, [])
        with self.director.connect() as db:
            db.execute(
                "INSERT INTO runs (run_id, task_id, context_id, package_digest, "
                "manifest_digest, build_id, label, run_inputs_json, run_inputs_digest, "
                "authorized_actor, input_authority_json, closed) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)",
                ("unfinished", "task", "context", "package", "manifest", "build",
                 "v1", "{}", "inputs", "fixture-operator", "{}"),
            )
        self.assertEqual(self.director.recover(), {"reason": "recover-unfinished:1"})
        self.assertEqual(len(runner.started), 1)
        self.assertTrue(runner.started[0].startswith("recover-unfinished"))

    def test_public_commands_keep_graph_selection_inside_instance(self):
        director = self.director
        action_id = "same-logical-action"
        self.assertEqual(director.run_id_for(action_id), director.run_id_for(action_id))
        self.assertTrue(director.run_id_for(action_id).startswith(director.identity + "."))
        token = harness.CURRENT_ACTOR.set("fixture-operator")
        try:
            for field, value in (("package_digest", "caller-choice"),
                                 ("graph", {}), ("version", "v2")):
                with self.subTest(field=field), self.assertRaisesRegex(
                        harness.Rejected, "invalid start fields"):
                    director.perform({"op": "start", "action_id": action_id,
                                      "inputs": {}, field: value}, "task", "context")
            with self.assertRaisesRegex(harness.Rejected, "unsupported command"):
                director.perform({"op": "publish", "action_id": action_id}, "task", "context")
            for command in ({"op": "inspect"},
                            {"op": "abort", "action_id": "abort-1"}):
                with self.subTest(op=command["op"]), self.assertRaisesRegex(
                        harness.Rejected, "original factory Task"):
                    director.perform(command, "unbound-task", "context")
        finally:
            harness.CURRENT_ACTOR.reset(token)
        with self.assertRaisesRegex(harness.Rejected, "authenticated caller required"):
            director.perform({"op": "start", "action_id": "anonymous", "inputs": {}},
                             "task", "context")
        self.assertEqual(director.module.runner.started, [])

    def test_publication_activates_without_starting_runner(self):
        health = {name: {"identity": "fixture-" + name} for name in SERVICE_NAMES}
        bindings = binding_records(health, 45200)
        catalog = self.director.module.catalog
        for name, value in (("approved_bindings", bindings),
                            ("contracts", contract_records()),
                            ("quality_policy", QUALITY_POLICY)):
            (catalog / f"{name}.json").write_text(json.dumps(value))
        template = json.loads((ROOT / "definitions" / "v1-template.json").read_text())
        package = materialize(template, bindings)
        decision = approve(package, approver="fixture-policy",
                           policy={"mode": "auto", "approved_bindings": bindings})
        with self.assertRaisesRegex(ValueError, "approved decision"):
            self.director.module.publish(package, label="rejected",
                                         approval={**decision, "status": "pending"})
        self.assertEqual(self.director.module.runner.built, [])
        published = self.director.module.publish(package, label="v1", approval=decision)
        active = self.director.module.publications.active()
        self.assertEqual(active["manifest_digest"], published["manifest_digest"])
        self.assertEqual(active["label"], "v1")
        self.assertEqual(active["package_digest"], decision["package_digest"])
        self.assertEqual(self.director.module.runner.built, [SRC])
        self.assertEqual(self.director.module.runner.started, [])
        self.assertFalse((self.home / "runner").exists())


class AgentModeTests(unittest.TestCase):
    def test_a2a_agent_mode_survives_restart_without_runner(self):
        state = Path(tempfile.mkdtemp(prefix="exo-proto-harness-test-", dir="/tmp"))
        home = state / "home"
        instance = home / "instances" / "agent"
        harness.init_instance(instance, name="agent-test", mode="agent", port=45300,
                              home=home)
        base = "http://127.0.0.1:45300"

        def get_json(path):
            with urllib.request.urlopen(base + path, timeout=2) as response:
                return json.load(response)

        def launch():
            log = (state / "agent.log").open("a")
            try:
                process = subprocess.Popen(
                    [sys.executable, "-B", str(SRC / "harness.py"), "serve",
                     "--instance-dir", str(instance)],
                    stdout=log, stderr=subprocess.STDOUT)
            finally:
                log.close()
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    self.fail(f"agent server exited during startup; see {state / 'agent.log'}")
                try:
                    return process, get_json("/health")
                except (urllib.error.URLError, TimeoutError, ValueError):
                    time.sleep(0.1)
            self.fail(f"agent server did not become healthy; see {state / 'agent.log'}")

        def stop(process):
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
            process.wait(timeout=15)

        process = None
        try:
            process, first = launch()
            card = get_json("/.well-known/agent-card.json")
            self.assertEqual([skill["id"] for skill in card["skills"]], ["capability"])
            self.assertNotIn("verified-research@1", [skill["id"] for skill in card["skills"]])
            command = assignment("run-agent-mode", "definition-agent-mode", "source_evidence")
            receipt = send(base, command)
            self.assertEqual(receipt["artifact"]["content"], "fixture-result:" + command["brief"])
            self.assertEqual(receipt["harness_identity"], first["identity"])
            self.assertFalse((home / "runner").exists())
            stop(process)
            process = None
            process, second = launch()
            self.assertEqual(second["identity"], first["identity"])
            self.assertEqual(second["incarnation"], first["incarnation"] + 1)
            self.assertFalse((home / "runner").exists())
        finally:
            if process is not None:
                stop(process)


if __name__ == "__main__":
    unittest.main()
