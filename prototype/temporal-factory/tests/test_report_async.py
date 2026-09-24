"""In-test independent async A2A agents for the report activity path."""
from __future__ import annotations

import asyncio
import hashlib
import json
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import adapter
import agent_binding
from report_contract import canonical, digest, validate_verdict
from report_fixture import packet


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def answer(self, value):
        data = json.dumps(value).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path.endswith("agent-card.json"):
            self.answer(self.server.card)
        elif self.path == "/contract":
            self.answer(self.server.contract)
        else:
            self.send_error(404)

    def do_POST(self):
        rpc = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if rpc["method"] == "message/send":
            assert rpc["params"]["configuration"]["blocking"] is False
            command = rpc["params"]["message"]["parts"][0]["data"]
            action = command["action_id"]
            if action not in self.server.tasks:
                self.server.tasks[action] = (command, time.monotonic())
            elif self.server.tasks[action][0] != command:
                self.answer({"jsonrpc": "2.0", "id": rpc["id"], "error": {"code": -32000}})
                return
            self.server.briefs.append(json.loads(command["brief"]))
        else:
            task_id = rpc["params"]["id"]
            action = next(key for key in self.server.tasks if f"{self.server.identity}:{key}" == task_id)
        self.answer({"jsonrpc": "2.0", "id": rpc["id"], "result": self.server.task(action)})


class Agent(ThreadingHTTPServer):
    def __init__(self, port, role, capability):
        super().__init__(("127.0.0.1", port), Handler)
        self.identity = f"unit-{role}-{capability}"
        self.role = role
        self.capability = capability
        self.tamper = None
        self.tasks = {}
        self.briefs = []
        self.contract = {"name": agent_binding.CONTRACT, "capability": capability,
            "reconcile": "a2a-idempotent-resend", "idempotency": {"key": "action_id",
            "same_payload": "original_task_id", "commit_before_response": True}}
        self.card = {"name": role, "url": f"http://127.0.0.1:{port}",
            "skills": [{"id": capability}], "capabilities": {"extensions": [{
                "uri": agent_binding.EXTENSION_URI, "required": True, "params": {
                    "identity": self.identity, "contract": agent_binding.CONTRACT,
                    "contract_digest": agent_binding.digest(self.contract)}}]}}

    def result(self, brief):
        if self.role == "research":
            risk = self.capability == "packet_risks@1"
            prefix = "R" if risk else "F"
            return {"kind": self.capability, "packet_digest": brief["packet_digest"],
                    "items": [{"id": f"{prefix}{n}", "statement": f"Statement {n}",
                        "evidence": [f"E{n}"], **({"severity": "medium"} if risk else {})}
                        for n in (1, 2)]}
        if self.role == "synthesis":
            return {"kind": "verified_report@1", "revision": brief["revision"],
                    "packet_digest": brief["packet_digest"], "question": brief["question"],
                    "title": "Qualification report", "markdown": "# Qualification report\nThree claims.",
                    "claims": [{"id": f"C{n}", "text": f"Claim {n}", "evidence": [f"E{n}"]}
                               for n in (1, 2, 3)]}
        candidate = brief["candidate"]
        verdict = {"kind": "quality_verdict@1", "candidate": {key: candidate[key]
                   for key in ("revision", "sha256", "author")}, "reviewer": self.identity,
                   "accepted": True, "decided_by": "model", "findings": [],
                   "rubric": "report-quality@1", "rubric_digest": "rubric-digest"}
        if self.tamper == "revision":
            verdict["candidate"]["revision"] = "r3"
        elif self.tamper == "sha":
            verdict["candidate"]["sha256"] = "0" * 64
        elif self.tamper == "reviewer":
            verdict["reviewer"] = "impostor"
        elif self.tamper == "author_review":
            verdict["reviewer"] = candidate["author"]
        return verdict

    def task(self, action):
        command, created = self.tasks[action]
        brief = json.loads(command["brief"])
        completed = time.monotonic() - created > 0.03
        task = {"kind": "task", "id": f"{self.identity}:{action}",
                "metadata": {**{key: command[key] for key in
                    ("action_id", "run_id", "definition_digest")}, "agent_identity": self.identity},
                "status": {"state": "completed" if completed else "working"}}
        if completed:
            content = canonical(self.result(brief))
            sha = hashlib.sha256(content.encode()).hexdigest()
            artifact = {"revision": brief["revision"], "sha256": sha, "author": self.identity,
                "content": content, **{key: command[key] for key in
                ("action_id", "run_id", "definition_digest")}}
            task["artifacts"] = [{"artifactId": sha, "parts": [{"kind": "data", "data": artifact}]}]
        return task


class ReportAsyncTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.agents = {
            "research_findings": Agent(46452, "research", "packet_findings@1"),
            "research_risks": Agent(46453, "research", "packet_risks@1"),
            "synthesizer": Agent(46454, "synthesis", "report_synthesis@1"),
            "quality": Agent(46455, "quality", "report_quality_review@1")}
        cls.threads = [threading.Thread(target=agent.serve_forever, daemon=True)
                       for agent in cls.agents.values()]
        for thread in cls.threads:
            thread.start()

    @classmethod
    def tearDownClass(cls):
        for agent in cls.agents.values():
            agent.shutdown()
            agent.server_close()
        for thread in cls.threads:
            thread.join(timeout=2)

    def setUp(self):
        self.home = Path(tempfile.mkdtemp(prefix="exo-sf-interp-unit-", dir="/tmp"))
        (self.home / "testbed").mkdir()
        (self.home / "runner").mkdir()
        snapshot = {"snapshot_version": 1, "agents": {agent.identity: {"url": agent.card["url"]}
                    for agent in self.agents.values()}}
        (self.home / "testbed" / "agent_snapshot.json").write_text(json.dumps(snapshot))
        self.pins = {name: agent_binding.pin(agent.card["url"], agent.identity)
                     for name, agent in self.agents.items()}
        self.packet = packet()
        self.question = "What qualified?"
        for agent in self.agents.values():
            agent.tasks = {}
            agent.briefs = []
            agent.tamper = None

    def call(self, fn, value):
        with patch.dict("os.environ", {"EXO_OUTCOME_DB": str(self.home / "runner" / "outcomes.sqlite3")}):
            return asyncio.run(fn(value))

    def args(self, name):
        agent = self.agents[name]
        return {"run": "run-1", "digest": "d" * 64, "binding": {"role": agent.role if
                agent.role == "quality" else "capability", "url": agent.card["url"],
                "identity": agent.identity, "approved": True}, "contract": self.pins[name]}

    def candidate(self):
        results = {}
        for name, capability in (("research_findings", "packet_findings@1"),
                                 ("research_risks", "packet_risks@1")):
            results[name] = self.call(adapter.assign, {**self.args(name), "instance": name,
                "capability": capability, "question": self.question, "packet": self.packet})
            self.assertNotIn("unresolved", results[name])
        evidence = self.call(adapter.typed_join, {"receipts": results, "packet": self.packet})
        candidate = self.call(adapter.synthesize, {**self.args("synthesizer"), "revision": "r1",
            "question": self.question, "packet": self.packet, "evidence": evidence})
        self.assertNotIn("unresolved", candidate)
        return results, evidence, candidate

    def review(self, candidate):
        return self.call(adapter.review, {**self.args("quality"), "candidate": candidate,
            "question": self.question, "packet": self.packet, "policy_digest": "p" * 64,
            "assignment_id": "run-1:quality", "attempt": 1,
            "rubric_digest": "rubric-digest"})

    def test_all_roles_async_and_repair_brief(self):
        results, evidence, candidate = self.candidate()
        self.assertEqual(evidence["kind"], "packet_evidence_join@1")
        self.assertEqual(set(evidence["branch_artifact_sha256"]), set(results))
        self.assertEqual(candidate["author"], self.agents["synthesizer"].identity)
        verdict = self.review(candidate)
        self.assertNotIn("inconsistent", verdict)
        self.assertTrue(verdict["artifact"]["accepted"])
        findings = [{"claim_id": "C1", "severity": "blocking", "problem": "Unsupported",
                     "evidence": ["E1"]}]
        repaired = self.call(adapter.synthesize, {**self.args("synthesizer"), "revision": "r2",
            "question": self.question, "packet": self.packet, "evidence": evidence,
            "prior": {key: candidate[key] for key in ("revision", "sha256", "content")},
            "quality_findings": findings})
        self.assertEqual(repaired["revision"], "r2")
        brief = self.agents["synthesizer"].briefs[-1]
        self.assertEqual(brief["mode"], "repair")
        self.assertEqual(brief["prior"]["sha256"], candidate["sha256"])
        self.assertEqual(brief["quality_findings"], findings)

    def test_verdict_wrong_revision_sha_or_reviewer_is_incident(self):
        for mutation in ("revision", "sha", "reviewer", "author_review"):
            with self.subTest(mutation=mutation):
                _, _, candidate = self.candidate()
                self.agents["quality"].tamper = mutation
                verdict = self.review(candidate)
                self.assertEqual(verdict["inconsistent"], "quality-evidence-inconsistent")
                self.agents["quality"].tasks = {}
                # Each subcase needs a fresh journal action identity.
                self.home = Path(tempfile.mkdtemp(prefix="exo-sf-interp-unit-", dir="/tmp"))
                (self.home / "testbed").mkdir()
                (self.home / "runner").mkdir()
                (self.home / "testbed" / "agent_snapshot.json").write_text(json.dumps({
                    "snapshot_version": 1, "agents": {agent.identity: {"url": agent.card["url"]}
                    for agent in self.agents.values()}}))
                for agent in self.agents.values():
                    agent.tasks = {}


if __name__ == "__main__":
    unittest.main()
