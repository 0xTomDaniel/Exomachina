"""Review 2 false-pass probes against a complete preserved scripted observation."""
from __future__ import annotations

import base64
import copy
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scenarios"))
from single_factory import (_contains_token, _redact_history, check_evidence)  # noqa: E402


class ReviewTwoCheckerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.snapshot = json.loads((ROOT / "evidence" / "single-factory" / "scripted-7.json").read_text())

    def setUp(self):
        self.e = copy.deepcopy(self.snapshot)
        self.temp = tempfile.TemporaryDirectory(prefix="exo-sf-check-", dir="/tmp")
        self.addCleanup(self.temp.cleanup)
        self.assertTrue(all(v["pass"] for v in check_evidence(self.e).values()))

    def _tasks(self, route: int, agent: str) -> list[dict]:
        run = self.e["routes"][str(route)]["child_run_id"]
        return [t for t in self.e["agents"][agent]["tasks"]
                if t["action_id"].startswith(run + ":")]

    def fails(self, check: str):
        self.assertFalse(check_evidence(self.e)[check]["pass"], check)

    def test_f1_quality_candidate_task_and_journal(self):
        accepting = next(t for t in self._tasks(2, "quality") if t["verdict"]["accepted"])
        accepting["verdict"]["candidate"]["sha256"] = "unrelated"
        self.fails("R2-d")
        self.e = copy.deepcopy(self.snapshot)
        self._tasks(1, "quality")[0]["verdict"] = {}
        self.fails("R1-e")
        self.e = copy.deepcopy(self.snapshot)
        self._tasks(3, "quality")[0]["journal_task_id"] = "different"
        self.fails("R3-b")

    def test_f2_route_and_journal_completeness(self):
        routes = self.e["routes"]
        routes["1"]["workflows"].extend(routes["2"]["workflows"] + routes["3"]["workflows"])
        routes["2"]["workflows"] = []
        routes["3"]["workflows"] = []
        self.fails("SF-3")
        self.e = copy.deepcopy(self.snapshot)
        self.e["journal"] = self.e["journal"][:1]
        self.fails("G-1")

    def test_f3_base64_history_redaction_and_decoded_scan(self):
        token = "private-director-token-for-test"
        payload = base64.b64encode(json.dumps({"director": {"token": token,
            "actor": "operator", "epoch": 2}, "closure": {"bindings": {"agent": "secret"}}}).encode()).decode()
        events = [{key: {"input": {"payloads": [{"data": payload}]}}} for key in
            ("workflowExecutionStartedEventAttributes",
             "startChildWorkflowExecutionInitiatedEventAttributes")]
        events.append({"workflowExecutionUpdateAcceptedEventAttributes": {
            "acceptedRequest": {"input": {"args": {"payloads": [{"data": payload}]}}}}})
        raw = Path(self.temp.name) / "raw.json"
        exported = Path(self.temp.name) / "export.json"
        raw.write_text(json.dumps({"events": events}))
        result = _redact_history(raw, exported)
        self.assertEqual(len(result["redacted_json_paths"]), 3)
        self.assertTrue(_contains_token(raw, token))
        self.assertFalse(_contains_token(exported, token))
        self.assertNotIn(payload, exported.read_text())

    def test_f5_report_bytes_and_release_digest(self):
        self.e["routes"]["1"]["task"]["artifacts"][0]["parts"][0]["text"] = "unrelated"
        self.fails("R1-d")
        self.e = copy.deepcopy(self.snapshot)
        self.e["routes"]["1"]["report_sha256"] = "0" * 64
        self.fails("R1-d")
        self.e = copy.deepcopy(self.snapshot)
        self.e["releases"][0]["sha256"] = "unrelated"
        self.fails("R1-d")

    def test_f6_structured_controls_and_whole_stimulus_log(self):
        self.e["routes"]["2"]["caller_messages"][0]["append_claim"] = {"text": "hidden"}
        self.fails("R2-a")
        self.e = copy.deepcopy(self.snapshot)
        self._tasks(2, "synthesizer")[0]["brief"]["test_controls"] = True
        self.fails("R2-a")
        self.e = copy.deepcopy(self.snapshot)
        self.e["stimulus_log"].append({**self.e["stimulus_log"][0],
            "run_id": self.e["routes"]["1"]["child_run_id"]})
        self.fails("R3-a")

    def test_f7_follow_up_turn_binding_and_counts(self):
        inspect = next(c for c in self.e["routes"]["3"]["director_calls"]
                       if c["tool"] == "inspect_run")
        inspect["message_id"] = "old-turn"
        self.fails("R3-d")
        self.e = copy.deepcopy(self.snapshot)
        self.e["routes"]["3"]["director_turns"].pop()
        self.fails("R3-e")

    def test_f8_pin_task_and_broker_inventory(self):
        action = self._tasks(1, "quality")[0]["action_id"]
        self.e["activity_log"] = [x for x in self.e["activity_log"] if not
            (x.get("action_id") == action and x.get("kind") == "agent-card-verified")]
        self.fails("SF-2")
        self.e = copy.deepcopy(self.snapshot)
        self.e["agents"]["quality"]["tasks"].pop()
        self.fails("G-3")
        self.e = copy.deepcopy(self.snapshot)
        self.e["broker_events"].append({"event": "start", "pid": 123456})
        self.fails("G-2")

    def test_f9_scan_coverage(self):
        self.e["leak_scan"]["candidate_manifest"] = []
        self.fails("G-4")
        self.e = copy.deepcopy(self.snapshot)
        self.e["leak_scan"]["raw"]["files_scanned"] = 0
        self.fails("G-4")
        self.e = copy.deepcopy(self.snapshot)
        leak = self.e["leak_scan"]
        self.e["evidence_schema"] = 2
        leak["required_scan_inputs"] = leak["scan_inputs"][:3]
        leak["coverage"] = {"manifest_count": len(leak["candidate_manifest"]),
            "scanner_file_count": leak["raw"]["files_scanned"], "zero_hits": True,
            "positive_control_detected": True, "candidate_files_hashed": True}
        self.assertTrue(check_evidence(self.e)["G-4"]["pass"])
        leak["coverage"]["scanner_file_count"] = 0
        self.fails("G-4")

    def test_pure_replay_with_different_worktree_prefix(self):
        original = "/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina-sf-director"
        moved = "/tmp/exo-relocated-checkout"
        self.e = json.loads(json.dumps(self.snapshot).replace(original, moved))
        with patch.object(Path, "read_bytes", side_effect=AssertionError("checker read bytes")), \
             patch.object(Path, "read_text", side_effect=AssertionError("checker read text")), \
             patch.object(Path, "is_file", side_effect=AssertionError("checker inspected disk")):
            checks = check_evidence(self.e)
        self.assertTrue(all(v["pass"] for v in checks.values()),
                        {k: v["pass"] for k, v in checks.items()})

    def test_f10_cleanup_process_and_stop_results(self):
        self.e["cleanup"]["started_processes"] = []
        self.fails("G-5")
        self.e = copy.deepcopy(self.snapshot)
        self.e["cleanup"]["services"]["quality"] = "timeout"
        self.e["cleanup"]["stop_results_valid"] = False
        self.fails("G-5")

    def test_f11_author_model_record(self):
        self.e["authoring"]["outcome"]["model"]["id"] = "other-model"
        self.fails("SF-1")
        self.e = copy.deepcopy(self.snapshot)
        self.e["authoring"]["outcome"]["model"]["live"] = True
        self.fails("SF-1")

    def test_f2_live_synthetic_evidence_binding(self):
        path = ROOT / "evidence" / "single-factory" / "scripted-7.json"
        synthetic = {key: self.e[key] for key in ("status", "provider", "checks",
            "git_commit", "checker_sha256", "interpreter_build", "manifest_digest", "route_inventory")}
        synthetic["evidence_path"] = str(path)
        synthetic["evidence_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        synthetic["record"] = {key: self.e[key] for key in ("status", "provider", "checks",
            "git_commit", "checker_sha256", "interpreter_build", "manifest_digest", "route_inventory")}
        self.e["synthetic_scenario"] = synthetic
        self.e["provider"] = "codex-subscription"
        with patch.object(Path, "read_bytes", side_effect=AssertionError("checker read bytes")), \
             patch.object(Path, "read_text", side_effect=AssertionError("checker read text")):
            self.assertTrue(check_evidence(self.e)["G-7"]["pass"])
        synthetic["evidence_sha256"] = "wrong"
        self.fails("G-7")
        synthetic["evidence_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        synthetic["record"]["git_commit"] = "wrong"
        self.fails("G-7")


if __name__ == "__main__":
    unittest.main()
