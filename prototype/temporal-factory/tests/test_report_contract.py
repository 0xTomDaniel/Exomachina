"""Report vocabulary and exact candidate/verdict contract checks."""
from __future__ import annotations

import copy
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from authoring import materialize
from definition import validate
from report_contract import (canonical, digest, packet_digest, research_assignment,
    quality_review_request, synthesis_assignment, validate_research_result, validate_report,
    validate_verdict)
from report_fixture import bindings, packet, template


class ReportContractTests(unittest.TestCase):
    def setUp(self):
        self.packet = packet()
        self.bindings = bindings()
        self.package = materialize(template(), self.bindings, evidence_packet=self.packet)

    def test_reference_graph_and_packet_pin(self):
        self.assertEqual(validate(self.package, self.bindings), digest(self.package))
        self.assertEqual(self.package["evidence_packet"], self.packet)
        self.assertEqual(research_assignment("packet_findings@1", "Question?", self.packet)["packet_digest"],
                         packet_digest(self.packet))

    def test_old_caller_mode_and_synthesis_flags_are_rejected(self):
        for mutation in ("outcome_mode", "resolved", "scope_status", "wrong_synthesizer"):
            with self.subTest(mutation=mutation):
                package = copy.deepcopy(self.package)
                child = next(iter(package["children"].values()))
                if mutation == "outcome_mode":
                    package["run_inputs"]["outcome_mode"] = {"type": "string"}
                elif mutation == "resolved":
                    child["nodes"]["draft"]["resolved"] = "from_run"
                elif mutation == "scope_status":
                    child["nodes"]["gather"]["branches"]["research_findings"]["scope_status"] = None
                else:
                    child["nodes"]["draft"]["service"] = "research_findings"
                package["children"] = {digest(child): child}
                package["root"]["nodes"]["invoke_child"]["child_digest"] = digest(child)
                with self.assertRaises(ValueError):
                    validate(package, self.bindings)

    def test_packet_and_report_evidence_ids(self):
        wrong = copy.deepcopy(self.package)
        wrong["evidence_packet"]["items"][0]["id"] = "bad"
        with self.assertRaises(ValueError):
            validate(wrong, self.bindings)
        findings = {"kind": "packet_findings@1", "packet_digest": packet_digest(self.packet),
                    "items": [{"id": f"F{n}", "statement": "Found", "evidence": [f"E{n}"]}
                              for n in (1, 2)]}
        self.assertEqual(validate_research_result(findings, "packet_findings@1", self.packet), findings)
        findings["items"][0]["evidence"] = ["E99"]
        with self.assertRaises(ValueError):
            validate_research_result(findings, "packet_findings@1", self.packet)

    def test_verdict_exact_revision_sha_and_factory_side_independence(self):
        report = {"kind": "verified_report@1", "revision": "r1", "packet_digest": packet_digest(self.packet),
                  "question": "Question?", "title": "Report", "markdown": "# Report",
                  "claims": [{"id": f"C{n}", "text": "Claim", "evidence": [f"E{n}"]}
                             for n in (1, 2, 3)]}
        validate_report(report, "r1", "Question?", self.packet)
        candidate = {"revision": "r1", "sha256": digest(report), "author": "synthesizer",
                     "content": canonical(report)}
        # The verdict names its candidate by {revision, sha256} only and
        # carries no reviewer or author: those are the factory's bindings.
        verdict = {"kind": "quality_verdict@1", "candidate": {key: candidate[key]
                   for key in ("revision", "sha256")},
                   "accepted": True, "decided_by": "model", "findings": [],
                   "rubric": "report-quality@1", "rubric_digest": "rubric-digest"}
        self.assertEqual(validate_verdict(verdict, candidate, "quality", packet=self.packet), verdict)
        for key, value in (("revision", "r2"), ("sha256", "0" * 64)):
            wrong = copy.deepcopy(verdict)
            wrong["candidate"][key] = value
            with self.assertRaises(ValueError):
                validate_verdict(wrong, candidate, "quality", packet=self.packet)
        for extra in ({"reviewer": "quality"}, {"candidate": {**verdict["candidate"], "author": "x"}}):
            with self.assertRaises(ValueError):
                validate_verdict({**verdict, **extra}, candidate, "quality", packet=self.packet)
        # Factory-side independence: the pinned reviewer is never the author.
        with self.assertRaises(ValueError):
            validate_verdict(verdict, candidate, "synthesizer", packet=self.packet)

    def test_briefs_carry_no_upstream_content(self):
        report = {"kind": "verified_report@1", "revision": "r1", "packet_digest": packet_digest(self.packet),
                  "question": "Question?", "title": "Report", "markdown": "# Report",
                  "claims": [{"id": f"C{n}", "text": "Claim", "evidence": [f"E{n}"]}
                             for n in (1, 2, 3)]}
        candidate = {"revision": "r1", "sha256": digest(report), "author": "synthesizer",
                     "content": canonical(report)}
        draft = synthesis_assignment("r1", "Question?", self.packet)
        repair = synthesis_assignment("r2", "Question?", self.packet, quality_findings=[])
        review = quality_review_request(candidate, "Question?", self.packet, "a" * 64)
        for brief in (draft, repair, review):
            self.assertFalse({"evidence", "prior", "candidate"} & set(brief), brief)
            self.assertNotIn(candidate["content"], canonical(brief))
        self.assertEqual(repair["mode"], "repair")
        with self.assertRaises(ValueError):
            synthesis_assignment("r2", "Question?", self.packet)

if __name__ == "__main__":
    unittest.main()
