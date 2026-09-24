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
sys.path.insert(0, str(ROOT / "scenarios"))
from single_factory import CHECK_IDS, FOLLOW_UP, check_evidence  # noqa: E402
from authoring import approve, materialize  # noqa: E402
from binding import build_id_for, source_digest  # noqa: E402
from fixture import assignment  # noqa: E402
from long_client import send  # noqa: E402
from testbed import (REPORT_CAPABILITIES, REPORT_NAMES, REPORT_QUALITY_POLICY,  # noqa: E402
                     report_bindings, report_role)


class StubRunner:
    """Records the runner boundary without starting local infrastructure."""

    def __init__(self, home: Path, *, port_base=None, member_base=None):
        self.home = home
        self.address = "127.0.0.1:44542"
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
            self.instance, name="factory-test", mode="factory", port=44874, home=self.home)
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
        template_path = ROOT / "definitions" / "report-template.json"
        health = {name: {"identity": "fixture-" + name} for name in REPORT_NAMES}
        bindings = report_bindings(health, 45780)
        contracts = {name: {"name": name, "role": report_role(name),
                            "capability": REPORT_CAPABILITIES[name]}
                     for name in REPORT_NAMES}
        catalog = self.director.module.catalog
        for name, value in (("approved_bindings", bindings),
                            ("contracts", contracts),
                            ("quality_policy", REPORT_QUALITY_POLICY)):
            (catalog / f"{name}.json").write_text(json.dumps(value))
        template = json.loads(template_path.read_text())
        packet = json.loads((ROOT / "packets" / "exo-qualification-2026-09-23" /
                             "packet.json").read_text())
        package = materialize(template, bindings, evidence_packet=packet)
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

    def test_accepted_report_projection_and_abort(self):
        store = harness.FactoryTaskStore(self.director)
        record = {"run_id": "run", "label": "v1", "manifest_digest": "manifest",
                  "package_digest": "package", "build_id": "build",
                  "run_inputs_digest": "inputs", "authorized_actor": "fixture-operator"}
        report = {"kind": "verified_report@1", "revision": "r2", "packet_digest": "packet",
                  "markdown": "# Accepted report\n", "claims": []}
        accepted = {"revision": "r2", "sha256": "a" * 64,
                    "content": json.dumps(report)}
        result = {"status": "accepted", "artifact": accepted,
                  "acceptance": {"sha256": "a" * 64},
                  "receipt": {"sha256": "a" * 64}}
        task = store._task("task", "context", record,
                           {"state": "completed", "status": {}, "result": result})
        self.assertEqual(len(task.artifacts), 1)
        self.assertEqual(task.artifacts[0].artifact_id, "a" * 64)
        self.assertEqual(task.artifacts[0].parts[0].root.text, "# Accepted report\n")
        self.assertEqual(task.artifacts[0].parts[1].root.data["packet_digest"], "packet")
        aborted = store._task("task", "context", record,
                              {"state": "completed", "status": {},
                               "result": {"status": "aborted"}})
        self.assertIsNone(aborted.artifacts)

    def test_inspection_returns_only_semantic_wait_fields(self):
        raw = {"phase": "awaiting-director", "current_revision": "r3",
               "current_sha256": "a" * 64, "repair_count": 2, "max_repairs": 2,
               "quality_verdict": {"findings": [{"problem": "uncited claim"}]},
               "deadline": 123.0, "token": "hidden", "graph": {"secret": "hidden"},
               "owner_epoch": 4, "run_inputs": {"question": "hidden"}}
        def fake_sync(coroutine):
            coroutine.close()
            return raw
        with patch.object(self.director, "task_binding", return_value=("run", "context")), \
             patch.object(harness, "sync", side_effect=fake_sync):
            observed = self.director.inspect_bound_run("task")
        self.assertEqual(observed, {"phase": "awaiting-director",
            "current_revision": "r3", "current_sha256": "a" * 64,
            "repair_count": 2, "max_repairs": 2,
            "quality_findings": [{"problem": "uncited claim"}], "wait_deadline": 123.0})


class AgentModeTests(unittest.TestCase):
    def test_a2a_agent_mode_survives_restart_without_runner(self):
        state = Path(tempfile.mkdtemp(prefix="exo-proto-harness-test-", dir="/tmp"))
        home = state / "home"
        instance = home / "instances" / "agent"
        harness.init_instance(instance, name="agent-test", mode="agent", port=44875,
                              home=home)
        base = "http://127.0.0.1:44875"

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


class SingleFactoryCheckerTests(unittest.TestCase):
    def test_missing_evidence_fails_closed(self):
        checks = check_evidence({"provider": "scripted"})
        self.assertEqual(tuple(checks), CHECK_IDS)
        self.assertTrue(all(not result["pass"] for result in checks.values()))

    def test_induced_stimulus_and_model_decided_abort_require_decisive_records(self):
        route = {"run_id": "run-2", "child_run_id": "run-2:child", "stimulus_log": [
            {"run_id": "run-2:child", "revision": "r1",
            "task_id": "synth-task", "sha256_after": "a" * 64,
            "planted_text": "unsupported", "claim_id": "C4"}],
            "stimulus_absent_from_factory": True}
        evidence = {"provider": "scripted", "routes": {"2": route},
            "agents": {"synthesizer": {"tasks": [{"action_id": "run-2:child:synthesize:r1",
                "task_id": "synth-task", "artifact": {"revision": "r1", "sha256": "a" * 64,
                    "content": '{"markdown":"unsupported"}'}}]}}}
        self.assertFalse(check_evidence(evidence)["R2-a"]["pass"])
        route["stimulus_log"][0]["sha256_after"] = "b" * 64
        self.assertFalse(check_evidence(evidence)["R2-a"]["pass"])
        route["stimulus_log"][0]["sha256_after"] = "a" * 64
        route["stimulus_absent_from_factory"] = False
        self.assertFalse(check_evidence(evidence)["R2-a"]["pass"])
        evidence["agents"]["quality"] = {"tasks": [{"action_id": "run-2:child:quality:r1",
            "live": False, "verdict": {"candidate": {"revision": "r1"},
                "accepted": False, "decided_by": "model", "findings": [{
                    "severity": "blocking", "problem": "unsupported claim", "claim_id": "C4"}]}}]}
        self.assertEqual(check_evidence(evidence)["R2-b"]["behavior"],
                         "scripted route control")

        wait = {"run_id": "run-3", "follow_up_text": FOLLOW_UP,
                "follow_up_original_task": True,
                "child_status": {"current_revision": "r3", "current_sha256": "a" * 64},
                "task": {"status": {"state": "completed"}}, "result_status": "aborted",
                "director_calls": [
                    {"tool": "inspect_run", "accepted": 1},
                    {"tool": "decide_wait", "accepted": 1, "model_kind": "synthetic", "arguments":
                        {"action": "abort", "revision": "r3", "sha256": "a" * 64}}]}
        evidence["routes"]["3"] = wait
        self.assertFalse(check_evidence(evidence)["R3-d"]["pass"])
        wait["director_calls"].reverse()
        self.assertFalse(check_evidence(evidence)["R3-d"]["pass"])

    def test_exact_release_and_pinned_version_checks(self):
        sha = "b" * 64
        data = {"revision": "r1", "sha256": sha, "packet_digest": "packet",
                "acceptance": {"revision": "r1", "sha256": sha},
                "release_receipt": {"revision": "r1", "sha256": sha}}
        report = {"run_id": "parent", "child_run_id": "child",
                  "task": {"artifacts": [{"artifactId": sha,
                      "parts": [{"kind": "text", "text": "# Report"},
                      {"kind": "data", "data": data}]}]},
                  "usefulness": {"ok": True, "reasons": [], "sections_present": True},
                  "report_path": "/tmp/report.md"}
        content = {"claims": [{"evidence": ["E1"]}, {"evidence": ["E2"]},
                              {"evidence": ["E1", "E2"]}]}
        evidence = {"provider": "scripted", "routes": {"1": report},
                    "packet_ids": ["E1", "E2"],
                    "agents": {"synthesizer": {"tasks": [{"action_id": "child:synthesize:r1",
                        "artifact": {"revision": "r1", "sha256": sha,
                                     "content": json.dumps(content)}}]}},
                    "releases": [{"run_id": "child", "revision": "r1", "sha256": sha,
                                  "accepted_effect_count": 1}]}
        self.assertFalse(check_evidence(evidence)["R1-d"]["pass"])
        self.assertEqual(check_evidence(evidence)["R1-d"]["semantic_reading"],
                         "pending-orchestrator-reading")
        evidence["releases"][0]["sha256"] = "c" * 64
        self.assertFalse(check_evidence(evidence)["R1-d"]["pass"])

        version = {"manifest_digest": "m", "package_digest": "p", "build_id": "b",
                   "versioning_behavior": "PINNED"}
        evidence["routes"] = {str(n): {"workflows": [dict(version), dict(version)],
            "caller_messages": [{"parts": [{"kind": "text", "text": "question"}]}]}
            for n in (1, 2, 3)}
        self.assertFalse(check_evidence(evidence)["SF-3"]["pass"])
        evidence["routes"]["3"]["workflows"][1]["build_id"] = "different"
        self.assertFalse(check_evidence(evidence)["SF-3"]["pass"])


if __name__ == "__main__":
    unittest.main()
