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
sys.path.insert(0, str(ROOT / "src"))
import a2a_v1  # noqa: E402


def as_a2a_v1(value):
    """Project the preserved 0.3-wire observation onto A2A v1 shapes in memory.

    ``scripted-7.json`` was observed on a2a-sdk 0.3.26 and stays byte-identical
    as evidence. The checker accepts only v1 Tasks, Messages and Parts, so the
    false-pass probes run against the same observation re-expressed in v1:
    no ``kind`` on A2A objects, ``TASK_STATE_*`` states and ``ROLE_*`` roles.
    Non-A2A records that also use a ``kind`` field are left unchanged.
    """
    if isinstance(value, list):
        return [as_a2a_v1(item) for item in value]
    if not isinstance(value, dict):
        return value
    value = {key: as_a2a_v1(item) for key, item in value.items()}
    kind = value.get("kind")
    if kind in ("task", "message") and {"status", "parts", "messageId"} & set(value):
        del value["kind"]
    elif kind in ("text", "data") and kind in value:
        del value["kind"]
    status = value.get("status")
    if isinstance(status, dict) and status.get("state") in ("working", "completed"):
        value["status"] = {**status, "state": a2a_v1.wire_state(status["state"])}
    if "parts" in value and value.get("role") in ("user", "agent"):
        value["role"] = "ROLE_" + value["role"].upper()
    return value


AGENT_NAMES = ("research_findings", "research_risks", "synthesizer", "quality")


def as_factory_side(e):
    """Re-express preserved agent-side evidence as factory-side evidence in memory.

    ``scripted-7.json`` predates agent decoupling (8 Oct 2026): its agent
    records were copied from agent stores and carried run/action echoes. The
    checker now accepts only factory-side evidence, so the false-pass probes
    run against the same observation projected onto it: the factory journal's
    taskId/contextId/messageId correlation and receipt digest, and one
    factory-recorded agent_reported usage row per Task that made model calls.
    """
    agents = e.get("agents") or {}
    journal = e.get("journal") or []
    e.setdefault("import_audit", {})["factory_names_clean"] = True
    for name in AGENT_NAMES:
        agent = agents.get(name) or {}
        agent["card_sha256"] = hashlib.sha256(name.encode()).hexdigest()
        records, usage_rows = [], []
        for task in agent.get("tasks") or []:
            match = next((x for x in journal if x.get("action_id") == task.get("action_id")), {})
            records.append(match)
            artifact = task.get("artifact") or {}
            context = f"context:{match.get('run_id')}:{agent.get('identity')}"
            task.update({"run_id": match.get("run_id"), "context_id": context,
                         "journal_context_id": context, "message_id": f"m:{task.get('action_id')}",
                         "journal_message_id": f"m:{task.get('action_id')}",
                         "receipt_sha256": artifact.get("sha256"),
                         "correlated": task.get("journal_task_id") == task.get("task_id")})
            if task.get("model_calls"):
                usage = {category: {"value": None, "status": "unavailable"} for category in
                         ("input_tokens", "output_tokens", "cache_read_tokens",
                          "cache_write_tokens", "total_tokens")}
                usage["output_tokens"] = {"value": 1, "status": "reported"}
                row = {"task_id": task.get("task_id"), "service_identity": agent.get("identity"),
                       "evidence_status": "agent_reported",
                       "measurement_source": "agent_reported", "usage": usage}
                usage_rows.append(row)
                task["agent_usage"] = row
            for retired in ("model_calls", "live"):
                task.pop(retired, None)
        agent["factory_records"] = {"journal": records, "agent_usage": usage_rows}
        agent.pop("sqlite", None)
        agent.pop("store_path", None)
    return e


def as_a2a_release(e: dict) -> dict:
    """Re-express the preserved observation's release evidence as A2A release evidence.

    ``scripted-7.json`` predates the A2A release agent (8 Oct 2026): its
    receiver rows carried factory run ids. The A2A agent is factory-unaware, so
    its rows are keyed by its own Task, message and receipt ids; the factory
    journal maps each release attempt to them, and the route's Observation
    holds exactly one delivery.receipt fact per delivery.
    """
    contents = {(task.get("artifact") or {}).get("sha256"): (task.get("artifact") or {}).get("content")
                for task in e["agents"]["synthesizer"]["tasks"]}
    deliveries = []
    for number in ("1", "2", "3"):
        route = e["routes"][number]
        route["receipt_audit"] = {"events": [], "source_ids": [],
                                  "dashboard_contract_valid": True, "histories_checked": 2}
        for row in e["journal"]:
            if row.get("effect_kind") != "release" or row.get("run_id") != route["child_run_id"]:
                continue
            content = contents[row["sha256"]]
            task_id, message_id, receipt_id = (f"release-task-{number}", f"message-{number}",
                                               f"receipt-{number}")
            receipt = {"release_id": row["action_id"], "run_id": row["run_id"],
                       "definition_digest": row["definition_digest"], "revision": row["revision"],
                       "sha256": row["sha256"], "receipt_id": receipt_id,
                       "byte_length": len(content.encode("utf-8")),
                       "media_type": "application/json", "accepted_at": "2026-10-08T10:00:00Z",
                       "outcome": "delivered", "task_id": task_id, "message_id": message_id,
                       "destination_identity": e["agents"]["release"]["identity"],
                       "a2a_protocol": "1.0"}
            row.update(task_id=task_id, message_id=message_id, receipt=receipt)
            for part in route["task"]["artifacts"][0]["parts"]:
                if "data" in part and "release_receipt" in part["data"]:
                    part["data"]["release_receipt"] = receipt
            deliveries.append({"task_id": task_id, "context_id": "context-" + number,
                "message_id": message_id, "fingerprint": "f" * 64, "state": "completed",
                "status_text": "Delivered.", "receipt_id": receipt_id,
                "media_type": "application/json", "sha256": row["sha256"],
                "byte_length": receipt["byte_length"], "accepted_at": receipt["accepted_at"],
                "sends": 1, "effect_count": 1})
            route["receipt_audit"]["source_ids"].append("delivery-receipt:" + receipt_id)
            route["receipt_audit"]["events"].append({
                "type": "com.exomachina.delivery.receipt.v1", "time": receipt["accepted_at"],
                "data": {"schema_version": 1, "factory_id": "report-factory",
                         "run_id": row["run_id"], "receipt_id": receipt_id,
                         "artifact_revision": row["revision"], "artifact_sha256": row["sha256"],
                         "destination_id": receipt["destination_identity"],
                         "delivered_at": receipt["accepted_at"], "outcome": "delivered"}})
    e["releases"] = deliveries
    return e


def handoff_audit() -> dict:
    """In-memory G-8 hand-off evidence for preserved observations.

    The preserved scripted-7 run predates hand-off records (7 Oct 2026), so its
    false-pass probes carry a minimal recorded chain: two research hand-offs
    consumed by synthesis, the report draft consumed by Quality and release.
    """
    base = {"schema_version": 1, "factory_id": "report-factory", "run_id": "run",
            "assignment_id": "a", "attempt_id": "1"}
    item = lambda digest, **extra: {"item_index": 0, "source": "artifact", "part_kinds": ["data"],
        "media_type": None, "byte_length": 9, "ready_at": "2026-10-07T12:00:01.000Z",
        "digest": digest, **extra}
    produced = lambda node, hid, digest, at, **extra: {"type": "com.exomachina.handoff.produced.v1",
        "time": at, "data": {**base, "node": node, "handoff_id": hid, "handoff_revision": 1,
                             "produced_at": at, "items": [item(digest, **extra)]}}
    consumed = lambda node, at, inputs: {"type": "com.exomachina.handoff.consumed.v1", "time": at,
        "data": {**base, "node": node, "consumed_at": at,
                 "inputs": [{"handoff_id": h, "item_digests": [d]} for h, d in inputs]}}
    events = [produced("gather", "gather.f", "1" * 64, "2026-10-07T12:00:01.000Z"),
              produced("gather", "gather.r", "2" * 64, "2026-10-07T12:00:01.000Z"),
              consumed("draft", "2026-10-07T12:00:02.000Z", [("gather.f", "1" * 64), ("gather.r", "2" * 64)]),
              produced("draft", "draft", "3" * 64, "2026-10-07T12:00:03.000Z",
                       artifact_revision="r1", artifact_sha256="4" * 64),
              consumed("independent_quality", "2026-10-07T12:00:04.000Z", [("draft", "3" * 64)])]
    return {"events": events, "key_configured": True, "key_absent_history": True,
            "key_absent_events": True, "dashboard_contract_valid": True, "histories_checked": 2}


class ReviewTwoCheckerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.snapshot = as_factory_side(as_a2a_release(as_a2a_v1(json.loads(
            (ROOT / "evidence" / "single-factory" / "scripted-7.json").read_text()))))

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
        for route in self.e["routes"].values():
            route["handoff_audit"] = handoff_audit()
        self.temp = tempfile.TemporaryDirectory(prefix="exo-sf-check-", dir="/tmp")
        self.addCleanup(self.temp.cleanup)
        self.assertTrue(all(v["pass"] for v in check_evidence(self.e).values()))

    def _tasks(self, route: int, agent: str) -> list[dict]:
        run = self.e["routes"][str(route)]["child_run_id"]
        return [t for t in self.e["agents"][agent]["tasks"]
                if t["action_id"].startswith(run + ":")]

    def fails(self, check: str):
        self.assertFalse(check_evidence(self.e)[check]["pass"], check)

    def test_g8_requires_chained_content_free_keyed_handoffs(self):
        audit = self.e["routes"]["2"]["handoff_audit"]
        checks = check_evidence(self.e)
        self.assertTrue(checks["G-8"]["pass"])
        self.assertEqual(checks["G-8"]["decisive_evidence"]["2"]["links"][0]["revision"], 1)
        audit["events"][4]["data"]["inputs"][0]["item_digests"] = ["9" * 64]
        self.fails("G-8"); self.fails("G-7")
        for key in ("key_absent_history", "key_absent_events", "dashboard_contract_valid",
                    "key_configured"):
            self.e["routes"]["2"]["handoff_audit"] = {**handoff_audit(), key: False}
            self.fails("G-8")
        leaked = handoff_audit()
        leaked["events"][0]["data"]["items"][0]["name"] = "secret"
        self.e["routes"]["2"]["handoff_audit"] = leaked
        self.fails("G-8")
        self.e["routes"]["2"]["handoff_audit"] = {**handoff_audit(), "events": handoff_audit()["events"][:2]}
        self.fails("G-8")
        del self.e["routes"]["3"]["handoff_audit"]
        self.fails("G-8")

    def test_model_selection_must_match_authoring_observation(self):
        self.e["model_id"] = "gpt-6-luna"
        self.fails("SF-1")
        # Agent model selection is the agent's private implementation since
        # agent decoupling (8 Oct 2026); R1-b, R1-c and G-2 now require
        # factory-recorded agent-reported model work instead.
        for name in AGENT_NAMES:
            for task in self.e["agents"][name]["tasks"]:
                task.pop("agent_usage", None)
        for check in ("R1-b", "R1-c", "G-2", "G-3"):
            self.fails(check)

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

    def test_release_is_one_a2a_delivery_with_exactly_one_receipt_fact(self):
        def mutate(change):
            self.e = copy.deepcopy(self.snapshot)
            for route in self.e["routes"].values():
                route["handoff_audit"] = handoff_audit()
            change()
        facts = lambda n: self.e["routes"][n]["receipt_audit"]["events"]
        # A duplicate receipt fact for the same delivery (the 8 Oct floor bug).
        mutate(lambda: facts("1").append(copy.deepcopy(facts("1")[0])))
        self.fails("R1-d")
        mutate(lambda: facts("2").clear())
        self.fails("R2-d")
        mutate(lambda: facts("1")[0]["data"].update(receipt_id="another-receipt"))
        self.fails("R1-d")
        mutate(lambda: self.e["routes"]["1"]["receipt_audit"].update(
            dashboard_contract_valid=False))
        self.fails("R1-d")
        # The agent's own record must bind the journaled Task, message and receipt.
        for field in ("task_id", "message_id", "receipt_id", "byte_length"):
            mutate(lambda: self.e["releases"][0].update({field: "other"}))
            self.fails("R1-d")
        mutate(lambda: self.e["releases"][0].update(effect_count=2))
        self.fails("R1-d")
        # A delivery no factory release attempt accounts for, or any route-3 receipt.
        mutate(lambda: self.e["releases"].append({**self.e["releases"][0], "task_id": "stray"}))
        self.fails("R3-d")
        mutate(lambda: facts("3").append(copy.deepcopy(facts("1")[0])))
        self.fails("R3-d")

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

    def test_preserved_0_3_wire_observation_does_not_satisfy_v1_checks(self):
        legacy = json.loads((ROOT / "evidence" / "single-factory" / "scripted-7.json").read_text())
        for key in ("setup", "binding_names", "leak_scan"):
            legacy[key] = copy.deepcopy(self.e[key])
        checks = check_evidence(legacy)
        for key in ("SF-3", "R1-a", "R1-d", "G-7"):
            self.assertFalse(checks[key]["pass"], key)

    def test_live_attempt_1_corrected_predicates(self):
        """Replay preserved live observations without changing the evidence file."""
        live = as_factory_side(as_a2a_v1(json.loads((ROOT / "evidence" / "single-factory" /
                                     "codex-subscription-1.json").read_text())))
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
        # Agent sessions follow the shared broker label <identity>:<taskId>.
        repaired_broker = copy.deepcopy(live)
        preserved = json.loads((ROOT / "evidence" / "single-factory" /
                                "codex-subscription-1.json").read_text())
        sessions = [call["session_id"] for name in AGENT_NAMES
                    for task in preserved["agents"][name]["tasks"]
                    for call in task["model_calls"]]
        self.assertTrue(all(session == f"{preserved['agents'][name]['identity']}:{task['task_id']}"
                            for name in AGENT_NAMES
                            for task in preserved["agents"][name]["tasks"]
                            for session in [c["session_id"] for c in task["model_calls"]]))
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
        # scripted-7 predates G-8; the in-memory prerequisite carries its G-8 result.
        self.e["checks"] = {**self.e["checks"], "G-8": check_evidence(self.e)["G-8"]}
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

    def test_live_route1_quality_labels_have_no_stimulus(self):
        live = json.loads((ROOT / "evidence" / "single-factory" /
                           "codex-subscription-3.json").read_text())
        checks = check_evidence(live)
        for key in ("R1-c", "R1-e"):
            self.assertEqual(checks[key]["behavior"], "spontaneous verdict (no stimulus)")
            self.assertNotIn("induced", checks[key]["behavior"])
        for key in ("R2-b", "R3-b"):
            self.assertEqual(checks[key]["behavior"], "spontaneous verdict on induced defect")


if __name__ == "__main__":
    unittest.main()
