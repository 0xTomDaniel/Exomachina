"""Contracts for independent report roles and the pinned evidence packet."""

import copy
import hashlib
import json
import re
import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services"))

from agent_roles import (REPORT_SECTIONS, ROLES, RUBRIC, RUBRIC_DIGEST,  # noqa: E402
                         RoleOutputError, usefulness_check)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


class AgentRoleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.packet = json.loads((ROOT / "packets" / "exo-qualification-2026-09-23" / "packet.json").read_text())
        cls.packet_digest = digest(cls.packet)
        cls.question = cls.packet["default_question"]

    def research_brief(self, capability):
        return {"kind": "research_assignment@1", "capability": capability, "revision": "r1",
                "question": self.question, "packet": self.packet, "packet_digest": self.packet_digest}

    def synthesis_brief(self, revision="r1", mode="draft", prior=None, findings=None):
        return {"kind": "synthesis_assignment@1", "mode": mode, "revision": revision,
                "question": self.question, "packet": self.packet, "packet_digest": self.packet_digest,
                "evidence": {"kind": "packet_evidence_join@1", "packet_digest": self.packet_digest,
                             "findings": [], "risks": [], "branch_artifact_sha256": {}},
                "prior": prior, "quality_findings": findings}

    def report(self, brief=None):
        brief = brief or self.synthesis_brief()
        role = ROLES["synthesis"]
        return role.parse(role.scripted_reply(brief, "synth-id"), brief, "synth-id")

    def quality_brief(self, report, author="synth-id"):
        content = canonical(report)
        return {"kind": "quality_review_request@1", "revision": report["revision"],
                "question": self.question, "packet": self.packet, "packet_digest": self.packet_digest,
                "candidate": {"revision": report["revision"], "sha256": hashlib.sha256(content.encode()).hexdigest(),
                              "author": author, "content": content},
                "policy_digest": "a" * 64}

    def test_prompts_are_role_specific_and_canonical(self):
        for capability in ("packet_findings@1", "packet_risks@1"):
            brief = self.research_brief(capability)
            self.assertIn(capability, ROLES["research"].system_prompt(capability))
            self.assertIn(canonical(brief), ROLES["research"].user_prompt(brief))
        synth = ROLES["synthesis"].system_prompt("report_synthesis@1")
        for section in REPORT_SECTIONS:
            self.assertIn(section, synth)
        self.assertIn("blocking", ROLES["quality"].system_prompt("report_quality_review@1"))
        self.assertEqual(RUBRIC_DIGEST, digest(RUBRIC))

    def test_research_scripted_parses_and_fence_is_tolerated(self):
        for capability in ("packet_findings@1", "packet_risks@1"):
            with self.subTest(capability=capability):
                brief = self.research_brief(capability)
                reply = ROLES["research"].scripted_reply(brief, "research-id")
                parsed = ROLES["research"].parse("```json\n" + reply + "\n```", brief, "research-id")
                self.assertEqual(parsed["kind"], capability)
                self.assertGreaterEqual(len(parsed["items"]), 2)
                self.assertEqual({"severity" in item for item in parsed["items"]},
                                 {capability == "packet_risks@1"})

    def test_research_rejects_invalid_schema_items_and_evidence(self):
        for capability in ("packet_findings@1", "packet_risks@1"):
            brief = self.research_brief(capability)
            original = json.loads(ROLES["research"].scripted_reply(brief, "research-id"))
            variants = []
            for field, value in (("kind", "wrong@1"), ("packet_digest", "0" * 64),
                                 ("items", original["items"][:1]), ("items", original["items"] * 3)):
                bad = copy.deepcopy(original)
                bad[field] = value
                variants.append(bad)
            for field, value in (("id", "C1"), ("evidence", ["E999"]), ("statement", "")):
                bad = copy.deepcopy(original)
                bad["items"][0][field] = value
                variants.append(bad)
            bad = copy.deepcopy(original)
            bad["items"][1]["id"] = bad["items"][0]["id"]
            variants.append(bad)
            bad = copy.deepcopy(original)
            if capability == "packet_risks@1":
                bad["items"][0]["severity"] = "critical"
            else:
                bad["items"][0]["severity"] = "high"
            variants.append(bad)
            for index, variant in enumerate(variants):
                with self.subTest(capability=capability, variant=index):
                    with self.assertRaises(RoleOutputError):
                        ROLES["research"].parse(canonical(variant), brief, "research-id")

    def test_synthesis_draft_schema_and_echo_rejections(self):
        brief = self.synthesis_brief()
        report = self.report(brief)
        self.assertEqual(report["revision"], "r1")
        self.assertEqual(report["question"], self.question)
        self.assertTrue(usefulness_check(report, self.packet)["ok"])
        variants = []
        for field, value in (("kind", "report@1"), ("revision", "r2"),
                             ("packet_digest", "0" * 64), ("question", "different"),
                             ("markdown", "x" * 12001), ("markdown", ""),
                             ("claims", report["claims"][:2])):
            bad = copy.deepcopy(report)
            bad[field] = value
            variants.append(bad)
        bad = copy.deepcopy(report)
        bad["markdown"] = bad["markdown"].replace("## Fixture-only", "Fixture-only")
        variants.append(bad)
        for field, value in (("id", "F1"), ("evidence", ["E404"]), ("text", "")):
            bad = copy.deepcopy(report)
            bad["claims"][0][field] = value
            variants.append(bad)
        bad = copy.deepcopy(report)
        bad["claims"][1]["id"] = "C1"
        variants.append(bad)
        for index, variant in enumerate(variants):
            with self.subTest(variant=index):
                with self.assertRaises(RoleOutputError):
                    ROLES["synthesis"].parse(canonical(variant), brief, "synth-id")
        for reply in ("not JSON", "```json\n{}\n``` trailing", '{"x":1,"x":2}'):
            with self.assertRaises(RoleOutputError):
                ROLES["synthesis"].parse(reply, brief, "synth-id")

    def test_quality_precheck_accepts_valid_candidate(self):
        brief = self.quality_brief(self.report())
        self.assertIsNone(ROLES["quality"].precheck(brief, "quality-id"))
        self.assertIsNone(ROLES["research"].precheck(brief, "quality-id"))

    def test_quality_precheck_rejects_hash_author_schema_and_evidence(self):
        original = self.quality_brief(self.report())
        variants = []
        bad = copy.deepcopy(original)
        bad["candidate"]["sha256"] = "0" * 64
        variants.append(bad)
        bad = copy.deepcopy(original)
        bad["candidate"]["author"] = "quality-id"
        variants.append(bad)
        for change in (lambda r: r.pop("title"),
                       lambda r: r["claims"][0].update(evidence=["E999"])):
            bad = copy.deepcopy(original)
            content = json.loads(bad["candidate"]["content"])
            change(content)
            bad["candidate"]["content"] = canonical(content)
            bad["candidate"]["sha256"] = hashlib.sha256(bad["candidate"]["content"].encode()).hexdigest()
            variants.append(bad)
        for index, brief in enumerate(variants):
            with self.subTest(variant=index):
                verdict = ROLES["quality"].precheck(brief, "quality-id")
                self.assertFalse(verdict["accepted"])
                self.assertEqual(verdict["decided_by"], "deterministic-precheck")
                self.assertEqual(verdict["reviewer"], "quality-id")
                self.assertEqual(verdict["candidate"]["sha256"], brief["candidate"]["sha256"])
                self.assertEqual(verdict["findings"][0]["severity"], "blocking")

    def test_quality_parse_binds_candidate_reviewer_and_rubric(self):
        brief = self.quality_brief(self.report())
        quality = ROLES["quality"]
        result = quality.parse('{"accepted":true,"findings":[]}', brief, "quality-id")
        self.assertEqual(result, {"kind": "quality_verdict@1",
                                  "candidate": {key: brief["candidate"][key] for key in ("revision", "sha256", "author")},
                                  "reviewer": "quality-id", "accepted": True,
                                  "decided_by": "model", "findings": [],
                                  "rubric": RUBRIC["kind"], "rubric_digest": RUBRIC_DIGEST})
        self.assertEqual(quality.parse(canonical(result), brief, "quality-id"), result)

    def test_quality_rejects_inconsistent_or_unbound_verdicts(self):
        brief = self.quality_brief(self.report())
        quality = ROLES["quality"]
        blocking = {"claim_id": "C1", "severity": "blocking", "problem": "unsupported", "evidence": ["E7"]}
        minor = {"claim_id": None, "severity": "minor", "problem": "wording", "evidence": []}
        self.assertFalse(quality.parse(canonical({"accepted": False, "findings": [blocking]}), brief,
                                       "quality-id")["accepted"])
        self.assertTrue(quality.parse(canonical({"accepted": True, "findings": [minor]}), brief,
                                      "quality-id")["accepted"])
        variants = [{"accepted": True, "findings": [blocking]},
                    {"accepted": False, "findings": []},
                    {"accepted": 1, "findings": []},
                    {"accepted": True, "findings": [{**minor, "evidence": ["E404"]}]},
                    {"accepted": True, "findings": [{**minor, "claim_id": "C99"}]}]
        full = quality.parse('{"accepted":true,"findings":[]}', brief, "quality-id")
        for field, value in (("reviewer", "wrong"), ("decided_by", "deterministic-precheck"),
                             ("candidate", {**full["candidate"], "sha256": "0" * 64}),
                             ("rubric_digest", "0" * 64)):
            bad = copy.deepcopy(full)
            bad[field] = value
            variants.append(bad)
        for index, variant in enumerate(variants):
            with self.subTest(variant=index):
                with self.assertRaises(RoleOutputError):
                    quality.parse(canonical(variant), brief, "quality-id")

    def test_scripted_draft_quality_accept(self):
        for capability in ("packet_findings@1", "packet_risks@1"):
            brief = self.research_brief(capability)
            ROLES["research"].parse(ROLES["research"].scripted_reply(brief, "research-id"), brief, "research-id")
        report = self.report()
        brief = self.quality_brief(report)
        self.assertIsNone(ROLES["quality"].precheck(brief, "quality-id"))
        verdict = ROLES["quality"].parse(ROLES["quality"].scripted_reply(brief, "quality-id"), brief, "quality-id")
        self.assertTrue(verdict["accepted"])
        self.assertEqual(verdict["decided_by"], "model")

    def test_planted_claim_rejected_then_repaired_for_both_routes(self):
        stimuli = json.loads((ROOT / "scenarios" / "sf_stimuli.json").read_text())
        for route in stimuli.values():
            with self.subTest(route=route):
                draft = self.report()
                planted = route["append_claim"]
                planted_id = f"C{len(draft['claims']) + 1}"
                draft["claims"].append({"id": planted_id, **planted})
                draft["markdown"] += "\n" + planted["text"]
                review = self.quality_brief(draft)
                self.assertIsNone(ROLES["quality"].precheck(review, "quality-id"))
                verdict = ROLES["quality"].parse(ROLES["quality"].scripted_reply(review, "quality-id"),
                                                 review, "quality-id")
                self.assertFalse(verdict["accepted"])
                self.assertEqual(verdict["findings"][0]["claim_id"], planted_id)
                repair_brief = self.synthesis_brief("r2", "repair", prior=review["candidate"],
                                                    findings=verdict["findings"])
                repaired = self.report(repair_brief)
                self.assertNotIn(planted["text"], canonical(repaired))
                reintroduced = copy.deepcopy(repaired)
                reintroduced["claims"][0]["text"] = planted["text"]
                with self.assertRaises(RoleOutputError):
                    ROLES["synthesis"].parse(canonical(reintroduced), repair_brief, "synth-id")
                self.assertTrue(usefulness_check(repaired, self.packet)["ok"])
                repaired_review = self.quality_brief(repaired)
                self.assertTrue(ROLES["quality"].parse(
                    ROLES["quality"].scripted_reply(repaired_review, "quality-id"),
                    repaired_review, "quality-id")["accepted"])

    def test_usefulness_checker_returns_reasons(self):
        report = self.report()
        self.assertEqual(usefulness_check(report, self.packet), {"ok": True, "reasons": []})
        report["claims"] = report["claims"][:2]
        report["claims"][0]["evidence"] = ["E999"]
        report["markdown"] = "not a report"
        result = usefulness_check(report, self.packet)
        self.assertFalse(result["ok"])
        self.assertIn("at least three claims required", result["reasons"])
        self.assertTrue(any("missing Next priority" in reason for reason in result["reasons"]))
        self.assertFalse(usefulness_check({"claims": [], "markdown": ""}, self.packet)["ok"])

    def test_every_packet_excerpt_is_verbatim_from_pinned_commit(self):
        self.assertTrue(8 <= len(self.packet["items"]) <= 14)
        self.assertLessEqual(len(canonical(self.packet).encode()), 12 * 1024)
        cache = {}
        for item in self.packet["items"]:
            match = re.fullmatch(r"(.+)#L([1-9][0-9]*)-L([1-9][0-9]*)", item["source"])
            self.assertIsNotNone(match, item["source"])
            path, first, last = match.group(1), int(match.group(2)), int(match.group(3))
            if path not in cache:
                source = subprocess.run(["git", "show", f"2d609e3:{path}"], cwd=ROOT,
                                        capture_output=True, text=True, check=True)
                cache[path] = source.stdout.splitlines()
            self.assertLessEqual(first, last)
            self.assertLessEqual(last, len(cache[path]))
            self.assertEqual(item["text"], "\n".join(cache[path][first - 1:last]), item["id"])


if __name__ == "__main__":
    unittest.main()
