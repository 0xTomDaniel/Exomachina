"""Offline checks for the integrated pinned interpreter."""
from __future__ import annotations

import copy
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from binding import (INTERPRETER_FILES, build_id_for, digest, make_manifest,
                     source_digest, verify_closure)
from definition import digest as definition_digest, validate
from failure_projection import project
from fixture import branch_brief


from authoring import materialize
from report_fixture import packet, template, bindings


def package_v1() -> dict:
    return materialize(template(), bindings(), evidence_packet=packet())


class InterpreterTests(unittest.TestCase):
    def test_materialized_v1_validates(self):
        package = package_v1()
        self.assertEqual(validate(package, package["bindings"]), definition_digest(package))

    def test_closure_rejects_changed_definition_contract_policy_and_build(self):
        package = package_v1()
        capabilities = {"research_findings": "packet_findings@1",
                        "research_risks": "packet_risks@1",
                        "synthesizer": "report_synthesis@1",
                        "quality": "report_quality_review@1"}
        contracts = {name: {"contract": name, "version": 1,
                            "capability": capabilities.get(name, "http-release@1")}
                     for name in package["bindings"]}
        policy = {"authority": "quality", "version": 1}
        code_digest = source_digest(SRC)
        build = build_id_for(code_digest)
        manifest = make_manifest(package, contracts, policy, build_id=build,
            code_digest=code_digest, python="3.12.9", temporalio="1.33.0")
        closure = {"manifest": manifest, "manifest_digest": digest(manifest),
                   "contracts": contracts, "quality_policy": policy}
        root_digest = definition_digest(package["root"])
        self.assertEqual(verify_closure(closure, package, build_id=build,
            definition_digest=root_digest, document=package["root"]), closure["manifest_digest"])
        changed_document = copy.deepcopy(package["root"])
        changed_document["revision"] = "changed"
        changed_contract = copy.deepcopy(closure)
        changed_contract["contracts"]["quality"]["version"] = 2
        changed_policy = copy.deepcopy(closure)
        changed_policy["quality_policy"]["version"] = 2
        changed_source = copy.deepcopy(closure)
        changed_source["manifest"]["interpreter"]["source_digest"] = "b" * 64
        changed_source["manifest_digest"] = digest(changed_source["manifest"])
        for candidate, document, candidate_build in (
                (closure, changed_document, build),
                (changed_contract, package["root"], build),
                (changed_policy, package["root"], build),
                (changed_source, package["root"], build),
                (closure, package["root"], "b-other")):
            with self.assertRaises(ValueError):
                verify_closure(candidate, package, build_id=candidate_build,
                    definition_digest=root_digest, document=document)

    def test_incident_projection(self):
        self.assertEqual(project({"phase": "quality-incident"}, "COMPLETED",
            {"status": "incident"}), "failed")
        self.assertEqual(project({"phase": "awaiting-child"}, "RUNNING"), "input-required")

    def test_question_in_brief(self):
        from report_contract import research_assignment
        brief = research_assignment("packet_findings@1", "What changed?", packet())
        self.assertEqual(brief["question"], "What changed?")
        self.assertEqual(brief["packet_digest"], definition_digest(packet()))

    def test_source_digest_stability_and_file_set(self):
        first = source_digest(SRC)
        self.assertEqual(first, source_digest(SRC))
        directory = Path(tempfile.mkdtemp(prefix="exo-proto-interpreter-", dir="/tmp"))
        for name in INTERPRETER_FILES:
            shutil.copyfile(SRC / name, directory / name)
        self.assertEqual(source_digest(directory), first)
        with (directory / "worker.py").open("a") as stream:
            stream.write("\n# changed copy\n")
        self.assertNotEqual(source_digest(directory), first)

    def test_factory_has_no_fault_inputs(self):
        self.assertNotIn("faults", (SRC / "factory.py").read_text())


if __name__ == "__main__":
    unittest.main()
