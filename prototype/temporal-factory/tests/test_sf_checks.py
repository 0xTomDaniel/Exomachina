"""Review 2 false-pass probes against a complete preserved scripted observation."""
from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scenarios"))
from single_factory import (_findings_on_claim, _redact_history,
                            _run_actions, check_evidence)  # noqa: E402
from sf_attest import verify_redaction, attest, resolve


class ReviewTwoCheckerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.snapshot = json.loads((ROOT / "evidence" / "single-factory" / "scripted-7.json").read_text())

    def setUp(self):
        self.e = copy.deepcopy(self.snapshot)
        self.e["setup"]["preflight_home_lstat"] = {"result": "FileNotFoundError"}
        card = {"skills": [{"id": "test"}]}
        self.e["setup"]["served_factory_card"] = {"body": card,
            "sha256": hashlib.sha256(json.dumps(card, sort_keys=True,
                separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()}
        self.e["setup"]["broker_status_fields"] = self.e["setup"]["broker_status"]
        self.e["binding_names"] = sorted(self.e["testbed"]["bindings"])
        self.e["leak_scan"]["sensitive_fields_redacted"] = {"pass": True, "files_checked": 9,
            "decoded_payloads_checked": 0, "redaction_objects_checked": 1}
        self.e["leak_scan"]["exported_file_count"] = 9
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
        self.assertFalse(verify_redaction([raw])["pass"])
        self.assertTrue(verify_redaction([exported])["pass"])
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

    def test_live_attempt_1_corrected_predicates(self):
        """Replay preserved live observations without changing the evidence file."""
        live = json.loads((ROOT / "evidence" / "single-factory" /
                           "codex-subscription-1.json").read_text())
        checks = check_evidence(live)
        for key in ("R2-b", "R3-b", "R3-d", "G-5"):
            self.assertTrue(checks[key]["pass"], key)
        self.assertFalse(checks["G-2"]["pass"])

        wrong_claim = copy.deepcopy(live)
        route2 = wrong_claim["routes"]["2"]["child_run_id"]
        r1 = next(t for t in wrong_claim["agents"]["quality"]["tasks"]
                  if t["action_id"].startswith(route2 + ":") and
                  t["verdict"]["candidate"]["revision"] == "r1")
        r1["verdict"]["findings"][0]["claim_id"] = "not-the-planted-claim"
        self.assertFalse(_findings_on_claim(r1["verdict"],
            wrong_claim["routes"]["2"]["stimulus_log"][0],
            _run_actions(wrong_claim, 2, "synthesizer")[0]))
        self.assertFalse(check_evidence(wrong_claim)["R2-b"]["pass"])

        wrong_route3_claim = copy.deepcopy(live)
        route3 = wrong_route3_claim["routes"]["3"]["child_run_id"]
        r3 = next(t for t in wrong_route3_claim["agents"]["quality"]["tasks"]
                  if t["action_id"].startswith(route3 + ":") and
                  t["verdict"]["candidate"]["revision"] == "r3")
        r3["verdict"]["findings"][0]["claim_id"] = "not-the-planted-claim"
        self.assertFalse(_findings_on_claim(r3["verdict"],
            wrong_route3_claim["routes"]["3"]["stimulus_log"][2],
            _run_actions(wrong_route3_claim, 3, "synthesizer")[2]))
        self.assertFalse(check_evidence(wrong_route3_claim)["R3-b"]["pass"])

        no_follow_inspect = copy.deepcopy(live)
        follow = no_follow_inspect["routes"]["3"]["caller_messages"][-1]["messageId"]
        no_follow_inspect["routes"]["3"]["director_calls"] = [
            row for row in no_follow_inspect["routes"]["3"]["director_calls"]
            if not (row["tool"] == "inspect_run" and row["message_id"] == follow)]
        self.assertFalse(check_evidence(no_follow_inspect)["R3-d"]["pass"])

        incomplete_ports = copy.deepcopy(live)
        incomplete_ports["cleanup"]["ports_checked"].pop()
        self.assertFalse(check_evidence(incomplete_ports)["G-5"]["pass"])

        # Reconstruct the session field that the old installed broker omitted.
        # The 34 saved streams comprise 4 authoring, 18 agent and 12 Director calls.
        repaired_broker = copy.deepcopy(live)
        sessions = [call["session_id"] for name in
                    ("research_findings", "research_risks", "synthesizer", "quality")
                    for task in repaired_broker["agents"][name]["tasks"]
                    for call in task["model_calls"]]
        streams = [row for row in repaired_broker["broker_events"]
                   if row.get("event") == "stream"]
        self.assertEqual((len(streams), len(sessions)), (34, 18))
        for row, session in zip(streams[4:22], sessions):
            row["session"] = session
        repaired_broker["broker_owner_counts"]["director"] = 12
        self.assertTrue(check_evidence(repaired_broker)["G-2"]["pass"])
        repaired_broker["broker_events"].extend([
            {"event": "start", "pid": live["broker_pid_before"]},
            {"event": "start", "pid": live["broker_pid_before"]}])
        self.assertFalse(check_evidence(repaired_broker)["G-2"]["pass"])

    def test_f9_scan_coverage(self):
        baseline = copy.deepcopy(self.e)
        self.e["leak_scan"]["candidate_manifest"] = []
        self.fails("G-4")
        self.e = copy.deepcopy(baseline)
        self.e["leak_scan"]["raw"]["files_scanned"] = 0
        self.fails("G-4")
        self.e = copy.deepcopy(baseline)
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
        self.e = json.loads(json.dumps(self.e).replace(original, moved))
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

    def _live_with_synthetic_prerequisite(self) -> dict:
        path = ROOT / "evidence" / "single-factory" / "scripted-7.json"
        synthetic = {key: self.e[key] for key in ("status", "provider", "checks",
            "git_commit", "checker_sha256", "interpreter_build", "manifest_digest", "route_inventory")}
        synthetic["evidence_path"] = str(path)
        synthetic["evidence_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        synthetic["record"] = {key: self.e[key] for key in ("status", "provider", "checks",
            "git_commit", "checker_sha256", "interpreter_build", "manifest_digest", "route_inventory")}
        synthetic["attestation_path"] = "test-attestation.json"
        synthetic["attestation_sha256"] = "a" * 64
        synthetic["attestation"] = {"evidence_sha256": synthetic["evidence_sha256"],
            "g4_final": {"pass": True}}
        self.e["synthetic_scenario"] = synthetic
        self.e["provider"] = "codex-subscription"
        return synthetic

    def test_f2_live_synthetic_evidence_binding(self):
        synthetic = self._live_with_synthetic_prerequisite()
        valid_sha = synthetic["evidence_sha256"]
        with patch.object(Path, "read_bytes", side_effect=AssertionError("checker read bytes")), \
             patch.object(Path, "read_text", side_effect=AssertionError("checker read text")):
            self.assertTrue(check_evidence(self.e)["G-7"]["pass"])
        synthetic["evidence_sha256"] = "wrong"
        self.fails("G-7")
        synthetic["evidence_sha256"] = valid_sha
        synthetic["record"]["git_commit"] = "wrong"
        self.fails("G-7")

    def test_live_g7_accepts_different_manifest(self):
        synthetic = self._live_with_synthetic_prerequisite()
        live_manifest = "0" * 64
        self.assertNotEqual(live_manifest, synthetic["manifest_digest"])
        self.e["manifest_digest"] = live_manifest
        for route in self.e["routes"].values():
            for workflow in route["workflows"]:
                workflow["manifest_digest"] = live_manifest
        self.assertEqual(synthetic["record"]["manifest_digest"], synthetic["manifest_digest"])
        self.assertTrue(check_evidence(self.e)["G-7"]["pass"])

    def test_live_g7_rejects_different_interpreter_build(self):
        synthetic = self._live_with_synthetic_prerequisite()
        synthetic["interpreter_build"] = "b-other-interpreter"
        synthetic["record"]["interpreter_build"] = synthetic["interpreter_build"]
        self.fails("G-7")

    def test_live_g7_rejects_different_checker_sha(self):
        synthetic = self._live_with_synthetic_prerequisite()
        synthetic["checker_sha256"] = "0" * 64
        synthetic["record"]["checker_sha256"] = synthetic["checker_sha256"]
        self.assertNotEqual(synthetic["checker_sha256"], self.e["checker_sha256"])
        self.fails("G-7")

    def test_review3_no_token_selection_or_credential_path(self):
        for name in ("single_factory.py", "sf_attest.py"):
            source = (ROOT / "scenarios" / name).read_text()
            self.assertNotRegex(source, r'_rows\([^\n]*["\']identity["\']')
            self.assertNotRegex(source, r'\bSELECT\b[^\n]*\btoken\b')
            self.assertNotIn('"secrets/', source)
            self.assertNotIn("'secrets/", source)
            self.assertNotRegex(source, r'open\([^\n]*credential')

    def test_review3_decoded_sensitive_plaintext_fails(self):
        path = Path(self.temp.name) / "payload.json"
        payload = base64.b64encode(json.dumps({"token": "eyJabc.eyJdef.signature"}).encode()).decode()
        path.write_text(json.dumps({"data": payload}))
        result = verify_redaction([path])
        self.assertFalse(result["pass"])
        self.assertEqual(result["decoded_payloads_checked"], 1)

    def test_review3_export_selector_includes_nested_histories(self):
        from sf_attest import exported_files
        evidence = Path(self.temp.name) / "scripted-test.json"
        evidence.write_text("{}")
        route = Path(self.temp.name) / "scripted-test-route1"
        route.mkdir()
        history = route / "parent.json"
        history.write_text("{}")
        self.assertEqual(set(exported_files(evidence)), {evidence, history})

    def test_review3_attestation_rejects_changed_final_evidence_bytes(self):
        path = Path(self.temp.name) / "scripted-test.json"
        path.write_text(json.dumps({"provider": "scripted"}))
        home = Path(self.temp.name) / "home"
        home.mkdir()
        def change(*_args):
            path.write_text(json.dumps({"provider": "scripted", "changed": True}))
            return {"hits": [], "files_scanned": 10}
        with patch("sf_attest.candidate_files", return_value=[]), \
             patch("sf_attest.positive_control", return_value={"scan": {"hits": [{}]}}), \
             patch("sf_attest.scan_paths", side_effect=change), \
             patch("sf_attest.packet_provenance", return_value={"observed": False}):
            result = attest(path, home, "test")
        self.assertFalse(result["g4_final"]["pass"])
        self.assertFalse(result["g4_final"]["terms"]["manifest_bound"])

    def test_review3_relative_evidence_manifest_resolves_exact_bytes(self):
        path = Path(self.temp.name) / "scripted-test.json"
        path.write_text(json.dumps({"provider": "scripted"}))
        home = Path(self.temp.name) / "home"
        home.mkdir()
        with patch("sf_attest.candidate_files", return_value=[path]), \
             patch("sf_attest.positive_control", return_value={"scan": {"hits": [{}]}}), \
             patch("sf_attest.scan_paths", return_value={"hits": [], "files_scanned": 10}), \
             patch("sf_attest.packet_provenance", return_value={"observed": False}):
            result = attest(Path(os.path.relpath(path)), home, "test")
        rows = result["manifest"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(resolve(rows[0]["path"]), path.resolve())
        self.assertEqual(rows[0]["sha256"], hashlib.sha256(path.read_bytes()).hexdigest())
        self.assertTrue(result["g4_final"]["terms"]["manifest_bound"])

    def test_review3_g7_requires_attestation(self):
        synthetic = self._live_with_synthetic_prerequisite()
        self.assertTrue(check_evidence(self.e)["G-7"]["pass"])
        synthetic.pop("attestation")
        self.fails("G-7")

    def test_review3_sf0_artifacts_and_labels(self):
        self.e["setup"]["preflight_home_lstat"] = {"result": "exists"}
        self.fails("SF-0")
        self.e["setup"]["preflight_home_lstat"] = {"result": "FileNotFoundError"}
        self.e["setup"]["served_factory_card"]["sha256"] = "0" * 64
        self.fails("SF-0")
        self._live_with_synthetic_prerequisite()
        checks = check_evidence(self.e)
        self.assertEqual(checks["R2-b"]["behavior"], "spontaneous verdict on induced defect")
        self.assertEqual(checks["R3-b"]["behavior"], "spontaneous verdict on induced defect")
        self.assertEqual(checks["R2-c"]["behavior"], "live repair content on assigned repair")
        self.assertEqual(checks["R2-d"]["behavior"], "spontaneous verdict")
        for key in ("R3-d", "R3-e"):
            self.assertEqual(checks[key]["behavior"],
                "caller-prompted, model-decided (abort is the only permitted action)")


if __name__ == "__main__":
    unittest.main()
