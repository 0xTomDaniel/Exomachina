"""Offline checks for provider-specific scenario acceptance."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scenarios"))

from live_authoring import authoring_acceptance


def authoring_record(*, revise: bool = False) -> dict:
    invalid = {"round": 1, "draft_digest": "invalid-draft", "valid": False,
               "errors": [{"code": "validation_error",
                           "message": "route must cover each typed value",
                           "path": "child.nodes.route_scope.cases",
                           "missing_cases": ["requires_scope"]}]}
    valid = {"round": 2 if revise else 1, "draft_digest": "valid-draft",
             "valid": True, "errors": []}
    calls = [{"tool": "describe_vocabulary", "result": {"route_values": {}}}]
    if revise:
        calls.append({"tool": "validate_draft", "draft_digest": "invalid-draft",
                      "result": {"ok": False, "draft_digest": "invalid-draft",
                                 "errors": deepcopy(invalid["errors"])}})
    else:
        calls.append({"tool": "validate_draft", "draft_digest": "valid-draft",
                      "result": {"ok": True, "draft_digest": "valid-draft",
                                 "package_digest": "package-2", "errors": []}})
    calls.append({"tool": "submit_draft", "draft_digest": "valid-draft",
                  "result": {"ok": True, "draft_digest": "valid-draft",
                             "package_digest": "package-2", "errors": []}})
    return {"status": "published", "admin_exit_code": 0, "model_calls": 4,
            "limits": {"max_rounds": 4, "max_model_calls": 12, "max_tool_calls": 24,
                       "deadline_seconds": 600},
            "outcome": {"status": "approved", "rounds": [invalid, valid] if revise else [valid],
                        "tool_calls": calls, "package_digest": "package-2",
                        "template": {"child": {"nodes": {"route_scope": {
                            "cases": {"clear": "draft_clear",
                                      "requires_scope": "draft_unresolved"}}}}},
                        "abort": None, "error": None},
            "approval": {"status": "approved", "policy": "auto",
                         "package_digest": "package-2"},
            "publication": {"package_digest": "package-2"}}


class AuthoringAcceptanceTests(unittest.TestCase):
    def test_synthetic_without_invalid_round_fails(self):
        ok, reason, facts = authoring_acceptance("synthetic-loopback", authoring_record())
        self.assertFalse(ok)
        self.assertEqual(reason, "synthetic_repair_loop_missing")
        self.assertEqual((facts["first_pass_valid"], facts["round_count"]), (True, 1))

    def test_synthetic_invalid_then_valid_passes(self):
        ok, reason, facts = authoring_acceptance(
            "synthetic-loopback", authoring_record(revise=True))
        self.assertTrue(ok, reason)
        self.assertEqual((facts["first_pass_valid"], facts["round_count"]), (False, 2))
        self.assertEqual(facts["invalid_rounds"][0]["errors"][0]["missing_cases"],
                         ["requires_scope"])

    def test_live_first_pass_valid_passes(self):
        ok, reason, facts = authoring_acceptance("codex-subscription", authoring_record())
        self.assertTrue(ok, reason)
        self.assertEqual((facts["first_pass_valid"], facts["round_count"]), (True, 1))
        self.assertEqual(facts["invalid_rounds"], [])

    def test_live_submit_before_validation_fails(self):
        record = authoring_record()
        calls = record["outcome"]["tool_calls"]
        calls[1], calls[2] = calls[2], calls[1]
        ok, reason, _ = authoring_acceptance("codex-subscription", record)
        self.assertFalse(ok)
        self.assertEqual(reason, "validate_draft_must_precede_submit_draft")

    def test_live_submitted_digest_without_valid_round_fails(self):
        record = authoring_record()
        submission = record["outcome"]["tool_calls"][-1]
        submission["draft_digest"] = "unvalidated-draft"
        submission["result"]["draft_digest"] = "unvalidated-draft"
        ok, _, _ = authoring_acceptance("codex-subscription", record)
        self.assertFalse(ok)

    def test_live_aborted_or_failed_fails(self):
        for status in ("aborted", "failed"):
            with self.subTest(status=status):
                record = authoring_record()
                record["status"] = status
                record["outcome"]["status"] = status
                if status == "aborted":
                    record["outcome"]["abort"] = {"reason": "round_limit"}
                else:
                    record["outcome"]["error"] = {"kind": "provider"}
                ok, reason, _ = authoring_acceptance("codex-subscription", record)
                self.assertFalse(ok)
                self.assertEqual(reason, "authoring_not_published")

    def test_live_revise_path_records_invalid_rounds(self):
        ok, reason, facts = authoring_acceptance(
            "codex-subscription", authoring_record(revise=True))
        self.assertTrue(ok, reason)
        self.assertEqual((facts["first_pass_valid"], facts["round_count"]), (False, 2))
        self.assertEqual(facts["invalid_rounds"][0]["draft_digest"], "invalid-draft")
        self.assertEqual(facts["invalid_rounds"][0]["errors"][0]["code"],
                         "validation_error")

    def test_live_over_budget_fails(self):
        record = authoring_record()
        record["model_calls"] = 13
        ok, reason, _ = authoring_acceptance("codex-subscription", record)
        self.assertFalse(ok)
        self.assertEqual(reason, "authoring_budget_exceeded_or_missing")

    def test_live_unstructured_validation_fails(self):
        record = authoring_record()
        record["outcome"]["tool_calls"][1]["result"].pop("errors")
        ok, reason, _ = authoring_acceptance("codex-subscription", record)
        self.assertFalse(ok)
        self.assertEqual(reason, "unstructured_validation_result")

    def test_synthetic_correction_must_cover_feedback(self):
        record = authoring_record(revise=True)
        del record["outcome"]["template"]["child"]["nodes"]["route_scope"]["cases"]["requires_scope"]
        ok, reason, _ = authoring_acceptance("synthetic-loopback", record)
        self.assertFalse(ok)
        self.assertEqual(reason, "synthetic_correction_not_derived_from_errors")


if __name__ == "__main__":
    unittest.main()
