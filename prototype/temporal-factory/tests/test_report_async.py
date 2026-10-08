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
import a2a_extensions
import a2a_v1
import adapter
import agent_binding
import handoff
from report_contract import REPORT_ACCEPTANCE_CRITERIA, canonical, digest, validate_verdict
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
        else:
            self.send_error(404)

    def do_POST(self):
        rpc = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        assert self.headers.get("A2A-Version") == "1.0"
        if rpc["method"] == "SendMessage":
            assert rpc["params"]["configuration"]["returnImmediately"] is True
            assert self.headers.get("A2A-Extensions") == a2a_extensions.BUDGET_URI
            message = rpc["params"]["message"]
            # The brief is the first text Part; consumed items follow verbatim.
            assert set(message["parts"][0]) == {"text", "mediaType"}
            parts = message["parts"]
            key = message["messageId"]
            if key not in self.server.tasks:
                self.server.tasks[key] = (parts, message["contextId"], time.monotonic())
            elif self.server.tasks[key][0] != parts:
                self.answer({"jsonrpc": "2.0", "id": rpc["id"], "error": {"code": -32000}})
                return
            self.server.briefs.append(json.loads(parts[0]["text"]))
            self.server.messages.append(parts)
            result = {"task": self.server.task(key)}
        else:
            assert rpc["method"] == "GetTask"
            task_id = rpc["params"]["id"]
            key = next(key for key in self.server.tasks if f"{self.server.identity}:{key}" == task_id)
            result = self.server.task(key)
        self.answer({"jsonrpc": "2.0", "id": rpc["id"], "result": result})


class Agent(ThreadingHTTPServer):
    def __init__(self, port, role, capability):
        super().__init__(("127.0.0.1", port), Handler)
        self.role = role
        self.capability = capability
        self.tamper = None
        self.tasks = {}
        self.briefs = []
        self.messages = []
        self.card = {"name": role, "supportedInterfaces": [{
                "url": f"http://127.0.0.1:{port}", "protocolBinding": "JSONRPC",
                "protocolVersion": "1.0"}],
            "skills": [{"id": capability, "tags": ["message-id-idempotent"]}]}
        # A plain A2A card: the factory derives this agent's identity from it.
        self.identity = agent_binding.card_identity(
            agent_binding.digest(a2a_v1.card_without_endpoint(self.card)))

    def result(self, brief, inputs):
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
        # Quality reviews the draft it received as a Part, naming it by the
        # draft's own revision and the digest it computes over the text.
        draft = inputs[0]["text"]
        verdict = {"kind": "quality_verdict@1", "candidate": {
                       "revision": json.loads(draft)["revision"],
                       "sha256": hashlib.sha256(draft.encode()).hexdigest()},
                   "accepted": True, "decided_by": "model", "findings": [],
                   "rubric": brief["acceptance_criteria"]["kind"],
                   "rubric_digest": digest(brief["acceptance_criteria"])}
        if self.tamper == "revision":
            verdict["candidate"]["revision"] = "r3"
        elif self.tamper == "sha":
            verdict["candidate"]["sha256"] = "0" * 64
        elif self.tamper == "reviewer":
            verdict["reviewer"] = "impostor"
        return verdict

    def task(self, key):
        parts, context_id, created = self.tasks[key]
        brief = json.loads(parts[0]["text"])
        completed = time.monotonic() - created > 0.03
        task = {"id": f"{self.identity}:{key}", "contextId": context_id,
                "status": {"state": "TASK_STATE_COMPLETED" if completed else "TASK_STATE_WORKING"}}
        if completed:
            content = canonical(self.result(brief, parts[1:]))
            sha = hashlib.sha256(content.encode()).hexdigest()
            if self.tamper != "no_artifacts":
                # The work product itself: one JSON text Part, no envelope.
                task["artifacts"] = [{"artifactId": sha, "parts": [
                    {"text": content, "mediaType": "application/json"}]}]
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
        snapshot = {"snapshot_version": 1, "agents": {agent.identity: {"url": agent.card["supportedInterfaces"][0]["url"]}
                    for agent in self.agents.values()}}
        (self.home / "testbed" / "agent_snapshot.json").write_text(json.dumps(snapshot))
        self.pins = {name: agent_binding.pin(agent.card["supportedInterfaces"][0]["url"], agent.identity)
                     for name, agent in self.agents.items()}
        self.packet = packet()
        self.question = "What qualified?"
        for agent in self.agents.values():
            agent.tasks = {}
            agent.briefs = []
            agent.messages = []
            agent.tamper = None

    def call(self, fn, value):
        with patch.dict("os.environ", {"EXO_OUTCOME_DB": str(self.home / "runner" / "outcomes.sqlite3")}):
            return asyncio.run(fn(value))

    def args(self, name):
        agent = self.agents[name]
        return {"run": "run-1", "digest": "d" * 64, "binding": {"role": agent.role if
                agent.role == "quality" else "capability", "url": agent.card["supportedInterfaces"][0]["url"],
                "identity": agent.identity, "approved": True}, "contract": self.pins[name]}

    @staticmethod
    def upstream(**receipts):
        return [{"handoff_id": name, "item_parts": receipt["item_parts"]}
                for name, receipt in receipts.items()]

    def candidate(self):
        results = {}
        for name, capability in (("research_findings", "packet_findings@1"),
                                 ("research_risks", "packet_risks@1")):
            results[name] = self.call(adapter.assign, {**self.args(name), "instance": name,
                "capability": capability, "question": self.question, "packet": self.packet})
            self.assertNotIn("unresolved", results[name])
        evidence = self.call(adapter.typed_join, {"receipts": results, "packet": self.packet})
        candidate = self.call(adapter.synthesize, {**self.args("synthesizer"), "revision": "r1",
            "question": self.question, "packet": self.packet,
            "upstream": self.upstream(**results)})
        self.assertNotIn("unresolved", candidate)
        return results, evidence, candidate

    def review(self, candidate):
        return self.call(adapter.review, {**self.args("quality"), "candidate": candidate,
            "question": self.question, "packet": self.packet, "policy_digest": "p" * 64,
            "acceptance_criteria": REPORT_ACCEPTANCE_CRITERIA,
            "assignment_id": "run-1:quality", "attempt": 1,
            "rubric_digest": digest(REPORT_ACCEPTANCE_CRITERIA), "upstream": self.upstream(draft=candidate)})

    def test_all_roles_async_and_repair_brief(self):
        results, evidence, candidate = self.candidate()
        self.assertEqual(evidence["kind"], "packet_evidence_join@1")
        self.assertEqual(set(evidence["branch_artifact_sha256"]), set(results))
        self.assertEqual(candidate["author"], self.agents["synthesizer"].identity)
        # Synthesis receives the research artifacts' own Parts after its brief,
        # and the brief embeds none of their content.
        sent = self.agents["synthesizer"].messages[-1]
        self.assertEqual(sent[1:], [part for name in results
                                    for part in results[name]["item_parts"][0]])
        brief = self.agents["synthesizer"].briefs[-1]
        self.assertFalse({"evidence", "prior", "candidate"} & set(brief))
        for receipt in results.values():
            self.assertNotIn(receipt["artifact"]["content"], sent[0]["text"])
        self.assertEqual(candidate["item_parts"], [[{"text": candidate["content"],
                                                     "mediaType": "application/json"}]])
        verdict = self.review(candidate)
        self.assertNotIn("inconsistent", verdict)
        self.assertTrue(verdict["artifact"]["accepted"])
        self.assertEqual(verdict["artifact"]["candidate"],
                         {"revision": "r1", "sha256": candidate["sha256"]})
        sent = self.agents["quality"].messages[-1]
        self.assertEqual(sent[1:], candidate["item_parts"][0])
        self.assertNotIn("candidate", self.agents["quality"].briefs[-1])
        self.assertNotIn(candidate["content"], sent[0]["text"])
        findings = [{"claim_id": "C1", "severity": "blocking", "problem": "Unsupported",
                     "evidence": ["E1"]}]
        repaired = self.call(adapter.synthesize, {**self.args("synthesizer"), "revision": "r2",
            "question": self.question, "packet": self.packet, "quality_findings": findings,
            "upstream": self.upstream(**results, draft=candidate)})
        self.assertEqual(repaired["revision"], "r2")
        brief = self.agents["synthesizer"].briefs[-1]
        self.assertEqual(brief["mode"], "repair")
        self.assertNotIn("prior", brief)
        self.assertEqual(self.agents["synthesizer"].messages[-1][-1],
                         candidate["item_parts"][0][0])
        self.assertEqual(brief["quality_findings"], findings)

    def test_verdict_wrong_revision_sha_reviewer_or_self_review_is_incident(self):
        for mutation in ("revision", "sha", "reviewer", "author_review"):
            with self.subTest(mutation=mutation):
                _, _, candidate = self.candidate()
                if mutation == "author_review":
                    # Factory-side independence: the pinned Quality identity
                    # may never be the candidate's pinned author.
                    candidate = {**candidate, "author": self.agents["quality"].identity}
                else:
                    self.agents["quality"].tamper = mutation
                verdict = self.review(candidate)
                self.assertEqual(verdict["inconsistent"], "quality-evidence-inconsistent")
                self.agents["quality"].tasks = {}
                self.agents["quality"].tamper = None
                # Each subcase needs a fresh journal action identity.
                self.home = Path(tempfile.mkdtemp(prefix="exo-sf-interp-unit-", dir="/tmp"))
                (self.home / "testbed").mkdir()
                (self.home / "runner").mkdir()
                (self.home / "testbed" / "agent_snapshot.json").write_text(json.dumps({
                    "snapshot_version": 1, "agents": {agent.identity: {"url": agent.card["supportedInterfaces"][0]["url"]}
                    for agent in self.agents.values()}}))
                for agent in self.agents.values():
                    agent.tasks = {}

    def test_oversize_input_fails_loudly_and_is_never_sent(self):
        results, _, _ = self.candidate()
        self.agents["synthesizer"].messages = []
        with patch.object(handoff, "MAX_MESSAGE_BYTES", 2000):
            result = self.call(adapter.synthesize, {**self.args("synthesizer"), "revision": "r1",
                "question": self.question, "packet": self.packet, "run": "run-oversize",
                "upstream": self.upstream(**results)})
        self.assertEqual(result["unresolved"], "input.oversize")
        self.assertEqual(self.agents["synthesizer"].messages, [])

    # A2A v1 mediation decisions 3 and 4: the on-complete hook enforces the
    # node's output contract and records content-free produced hand-offs.
    def call_keyed(self, fn, value, key_file):
        with patch.dict("os.environ", {"EXO_OUTCOME_DB": str(self.home / "runner" / "outcomes.sqlite3"),
                                       "EXO_HANDOFF_KEY_FILE": str(key_file)}):
            return asyncio.run(fn(value))

    def test_produced_handoffs_are_keyed_content_free_and_chain_to_the_report(self):
        import hmac
        key_file = self.home / "handoff-digest.key"
        results = {}
        for name, capability in (("research_findings", "packet_findings@1"),
                                 ("research_risks", "packet_risks@1")):
            results[name] = self.call_keyed(adapter.assign, {**self.args(name), "instance": name,
                "capability": capability, "question": self.question, "packet": self.packet,
                "handoff_id": f"gather.{name}", "handoff_revision": 1}, key_file)
        self.assertEqual(key_file.stat().st_mode & 0o777, 0o600)
        key = key_file.read_bytes()
        self.assertEqual(len(key), 32)
        evidence = self.call_keyed(adapter.typed_join, {"receipts": results, "packet": self.packet}, key_file)
        consumes = handoff.consumed_inputs([results[name]["handoff"] for name in results])
        candidate = self.call_keyed(adapter.synthesize, {**self.args("synthesizer"), "revision": "r1",
            "question": self.question, "packet": self.packet,
            "upstream": [{"handoff_id": results[name]["handoff"]["handoff_id"],
                          "item_parts": results[name]["item_parts"]} for name in results],
            "consumes": consumes, "handoff_id": "draft", "handoff_revision": 1}, key_file)
        records = [results["research_findings"]["handoff"], results["research_risks"]["handoff"],
                   candidate["handoff"]]
        for record, (handoff_id, receipt) in zip(records, (
                ("gather.research_findings", results["research_findings"]),
                ("gather.research_risks", results["research_risks"]), ("draft", candidate))):
            self.assertEqual(record["handoff_id"], handoff_id)
            self.assertEqual(record["handoff_revision"], 1)
            self.assertEqual(len(record["items"]), 1)
            item = record["items"][0]
            artifact = receipt["artifact"] if "artifact" in receipt else receipt
            self.assertEqual((item["source"], item["part_kinds"], item["media_type"]),
                             ("artifact", ["text"], "application/json"))
            self.assertEqual(item["byte_length"], len(artifact["content"].encode()))
            payload = json.dumps([["text", artifact["content"]]], sort_keys=True,
                                 separators=(",", ":"), ensure_ascii=False).encode()
            self.assertEqual(item["digest"], hmac.new(key, payload, hashlib.sha256).hexdigest())
            self.assertNotEqual(item["digest"], hashlib.sha256(payload).hexdigest())
            self.assertEqual(item["ready_at"], record["produced_at"])
            encoded = json.dumps(record)
            self.assertNotIn(key.hex(), encoded)
            for secret in (artifact["content"], artifact["sha256"] if handoff_id != "draft" else "~",
                           "artifactId", "content", "metadata", "name", "filename", "url\"", "\"text\":"):
                self.assertNotIn(secret, encoded)
        report = candidate["handoff"]["items"][0]
        self.assertEqual((report["artifact_revision"], report["artifact_sha256"]),
                         (candidate["revision"], candidate["sha256"]))
        verdict = self.call_keyed(adapter.review, {**self.args("quality"),
            "candidate": {k: v for k, v in candidate.items() if k != "handoff"},
            "question": self.question, "packet": self.packet, "policy_digest": "p" * 64,
            "acceptance_criteria": REPORT_ACCEPTANCE_CRITERIA,
            "assignment_id": "run-1:quality", "attempt": 1, "rubric_digest": digest(REPORT_ACCEPTANCE_CRITERIA),
            "upstream": [{"handoff_id": "draft", "item_parts": candidate["item_parts"]}],
            "consumes": handoff.consumed_inputs([candidate["handoff"]])}, key_file)
        self.assertNotIn("inconsistent", verdict)
        self.assertNotIn("handoff", verdict, "a gate seals the carrier; it mints no hand-off")
        # The journal replays the same record on an Activity retry.
        again = self.call_keyed(adapter.assign, {**self.args("research_findings"), "instance": "research_findings",
            "capability": "packet_findings@1", "question": self.question, "packet": self.packet,
            "handoff_id": "gather.research_findings", "handoff_revision": 1}, key_file)
        self.assertEqual(again["handoff"], results["research_findings"]["handoff"])

    def test_consumed_record_must_name_exactly_the_composed_items(self):
        key_file = self.home / "handoff-digest.key"
        results = {}
        for name, capability in (("research_findings", "packet_findings@1"),
                                 ("research_risks", "packet_risks@1")):
            results[name] = self.call_keyed(adapter.assign, {**self.args(name), "instance": name,
                "capability": capability, "question": self.question, "packet": self.packet,
                "handoff_id": f"gather.{name}", "handoff_revision": 1}, key_file)
        upstream = [{"handoff_id": f"gather.{name}", "item_parts": results[name]["item_parts"]}
                    for name in results]
        consumes = handoff.consumed_inputs([results[name]["handoff"] for name in results])
        tampered = json.loads(json.dumps(upstream))
        tampered[1]["item_parts"][0][0]["text"] += " "
        cases = {"missing": upstream[:1], "reordered": upstream[::-1], "altered": tampered,
                 "extra": upstream + upstream[:1]}
        self.agents["synthesizer"].messages = []
        for label, value in cases.items():
            with self.subTest(case=label):
                result = self.call_keyed(adapter.synthesize, {**self.args("synthesizer"),
                    "run": f"run-{label}", "revision": "r1", "question": self.question,
                    "packet": self.packet, "upstream": value, "consumes": consumes,
                    "handoff_id": "draft", "handoff_revision": 1}, key_file)
                self.assertEqual(result["unresolved"], "input.composition-mismatch")
        self.assertEqual(self.agents["synthesizer"].messages, [], "a mismatch never sends")
        # Quality and release fail the same way when their consumed draft differs.
        draft = self.call_keyed(adapter.synthesize, {**self.args("synthesizer"), "revision": "r1",
            "question": self.question, "packet": self.packet, "upstream": upstream,
            "consumes": consumes, "handoff_id": "draft", "handoff_revision": 1}, key_file)
        self.assertNotIn("unresolved", draft)
        verdict = self.call_keyed(adapter.review, {**self.args("quality"),
            "candidate": {k: v for k, v in draft.items() if k != "handoff"},
            "question": self.question, "packet": self.packet, "policy_digest": "p" * 64,
            "acceptance_criteria": REPORT_ACCEPTANCE_CRITERIA,
            "assignment_id": "run-1:quality", "attempt": 1, "rubric_digest": digest(REPORT_ACCEPTANCE_CRITERIA),
            "upstream": upstream[:1], "consumes": handoff.consumed_inputs([draft["handoff"]])},
            key_file)
        self.assertEqual(verdict["inconsistent"], "input.composition-mismatch")
        self.assertEqual(self.agents["quality"].messages, [])

    def test_without_workflow_identity_or_key_no_record_is_returned(self):
        plain = self.call(adapter.assign, {**self.args("research_findings"), "instance": "research_findings",
            "capability": "packet_findings@1", "question": self.question, "packet": self.packet,
            "handoff_id": "gather.research_findings", "handoff_revision": 1})
        self.assertNotIn("handoff", plain)
        for agent in self.agents.values():
            agent.tasks = {}
        self.home = Path(tempfile.mkdtemp(prefix="exo-sf-interp-unit-", dir="/tmp"))
        self.setUp()
        legacy = self.call_keyed(adapter.assign, {**self.args("research_risks"), "instance": "research_risks",
            "capability": "packet_risks@1", "question": self.question, "packet": self.packet},
            self.home / "handoff-digest.key")
        self.assertNotIn("unresolved", legacy)
        self.assertNotIn("handoff", legacy)

    def test_artifacts_node_without_an_artifact_fails_output_missing(self):
        self.agents["research_findings"].tamper = "no_artifacts"
        try:
            result = self.call_keyed(adapter.assign, {**self.args("research_findings"),
                "instance": "research_findings", "capability": "packet_findings@1",
                "question": self.question, "packet": self.packet,
                "handoff_id": "gather.research_findings", "handoff_revision": 1},
                self.home / "handoff-digest.key")
        finally:
            self.agents["research_findings"].tamper = None
        self.assertEqual(result["unresolved"], "output.missing")
        self.assertNotIn("handoff", result)


if __name__ == "__main__":
    unittest.main()
