"""Submission-readiness checks are configuration/public-health reads only."""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

import harness  # noqa: E402
from definition import digest  # noqa: E402
from harness_server import Rejected  # noqa: E402


CAPABILITIES = {
    "research_findings": "packet_findings@1",
    "research_risks": "packet_risks@1",
    "synthesizer": "report_synthesis@1",
    "quality": "report_quality_review@1",
}


class ActivePublications:
    def __init__(self, publication):
        self.publication = publication

    def active(self):
        return self.publication


class SubmissionReadinessTests(unittest.TestCase):
    def setUp(self):
        contracts = {
            name: {"name": name, "capability": capability,
                   "role": "quality" if name == "quality" else "capability"}
            for name, capability in CAPABILITIES.items()
        }
        package_digest = "b" * 64
        manifest = {
            "package_digest": package_digest,
            "root_digest": "c" * 64,
            "services": {name: {"binding_digest": "d" * 64,
                                "contract_digest": "e" * 64}
                         for name in CAPABILITIES},
        }
        manifest_digest = digest(manifest)
        closure = {"manifest": manifest, "manifest_digest": manifest_digest,
                   "contracts": contracts}
        publication = {"manifest_digest": manifest_digest,
                       "package_digest": package_digest, "build_id": "build-ready-1",
                       "closure": closure}
        self.director = type("Director", (), {})()
        self.director.identity = "factory-safe"
        self.director.module = type("Module", (), {})()
        self.director.module.publications = ActivePublications(publication)
        self.config = {"director_model": {"provider": "codex-subscription",
                                           "model": "gpt-6-luna"}}
        # Owners as resolved from their pinned Agent Cards: the only agent
        # evidence readiness may use (no private health routes).
        self.owners = [{"name": name, "identity": f"identity-{name}",
                        "url": f"http://127.0.0.1:{47100 + index}",
                        "skills": [CAPABILITIES[name]]}
                       for index, name in enumerate(CAPABILITIES)]

    def readiness(self, *, owners=None, config=None, broker_status="available"):
        owner_rows = self.owners if owners is None else owners
        with patch.dict(os.environ, {}, clear=True), \
                patch.object(harness, "_verified_active_publication_context",
                             return_value={"manifest_digest":
                                           self.director.module.publications.publication[
                                               "manifest_digest"],
                                           "quality_policy_digest": "f" * 64}), \
                patch.object(harness, "_pinned_usage_owners",
                             return_value=(owner_rows, 0, 0)):
            return harness._submission_readiness(
                self.director, self.config if config is None else config,
                broker_status_reader=lambda: broker_status)

    def test_ready_requires_approved_profile_broker_and_all_pinned_writable_owners(self):
        result = self.readiness()
        self.assertTrue(result["submission_ready"])
        self.assertEqual(result["submission_status"], "ready")
        self.assertEqual(result["pinned_model_owners"],
                         {"status": "ready", "expected": 4, "ready": 4})
        self.assertEqual(result["director_reasoning_effort"], "xhigh")
        self.assertEqual(result["verification_status"], "not_checked")
        self.assertFalse(result["live_inference_ready"])
        self.assertNotIn("account", str(result).lower())

    def test_owner_card_without_expected_skill_blocks_preflight(self):
        owners = [dict(owner) for owner in self.owners]
        owners[-1]["skills"] = ["some_other_skill@1"]
        callback, require = harness._submission_readiness_callbacks(self.director, self.config)
        with patch.dict(os.environ, {}, clear=True), \
                patch.object(harness, "_verified_active_publication_context",
                             return_value={"manifest_digest":
                                           self.director.module.publications.publication[
                                               "manifest_digest"],
                                           "quality_policy_digest": "f" * 64}), \
                patch.object(harness, "_pinned_usage_owners",
                             return_value=(owners, 0, 0)), \
                patch.object(harness, "_redacted_subscription_status",
                             return_value="available"):
            state = callback(refresh=True)
            with self.assertRaisesRegex(Rejected, "pinned_writable_model_owners_unavailable"):
                require()
        self.assertFalse(state["submission_ready"])
        self.assertEqual(state["pinned_model_owners"]["ready"], 3)

    def test_unapproved_model_or_default_path_override_fails_before_status_probe(self):
        for config in (
            {"director_model": {"provider": "openrouter", "model": "jev-1.13"}},
            {"director_model": {"provider": "codex-subscription", "model": "gpt-6-luna",
                                 "reasoning_effort": "high"}},
        ):
            calls = []
            with patch.dict(os.environ, {}, clear=True), \
                    patch.object(harness, "_verified_active_publication_context",
                                 return_value={"manifest_digest":
                                               self.director.module.publications.publication[
                                                   "manifest_digest"],
                                               "quality_policy_digest": "f" * 64}):
                result = harness._submission_readiness(
                    self.director, config,
                    broker_status_reader=lambda: calls.append("broker") or "available",
                    owner_reader=lambda *_args: (self.owners, 0, 0))
            self.assertFalse(result["submission_ready"])
            self.assertIn("director_profile_unapproved", result["submission_blockers"])
            self.assertEqual(calls, [])

        with patch.dict(os.environ, {"EXO_MODEL_HOME": "/custom/model-home"}, clear=True):
            with patch.object(harness, "_verified_active_publication_context",
                              return_value={"manifest_digest":
                                            self.director.module.publications.publication[
                                                "manifest_digest"],
                                            "quality_policy_digest": "f" * 64}):
                result = harness._submission_readiness(
                    self.director, self.config,
                    broker_status_reader=lambda: self.fail("must not read an overridden broker"),
                    owner_reader=lambda *_args: (self.owners, 0, 0))
        self.assertFalse(result["submission_ready"])
        self.assertIn("default_broker_path_overridden", result["submission_blockers"])

    def test_broker_expiry_and_missing_pinned_owner_fail_closed(self):
        expired = self.readiness(broker_status="expired")
        self.assertIn("subscription_status_unavailable", expired["submission_blockers"])
        self.assertFalse(expired["submission_ready"])

        missing_owner = self.readiness(owners=self.owners[:-1])
        self.assertIn("pinned_writable_model_owners_unavailable",
                      missing_owner["submission_blockers"])
        self.assertEqual(missing_owner["pinned_model_owners"]["ready"], 0)

    def test_author_provider_environment_flag_is_not_readiness_evidence(self):
        with patch.dict(os.environ, {"EXO_AUTHOR_PROVIDER": "codex-subscription"}, clear=True), \
                patch.object(harness, "_verified_active_publication_context",
                             return_value={"manifest_digest":
                                           self.director.module.publications.publication[
                                               "manifest_digest"],
                                           "quality_policy_digest": "f" * 64}), \
                patch.object(harness, "_pinned_usage_owners",
                             return_value=(self.owners, 0, 0)):
            result = harness._submission_readiness(
                self.director, self.config,
                broker_status_reader=lambda: "signed_out")
        self.assertFalse(result["submission_ready"])
        self.assertEqual(result["broker_status"], "signed_out")
        self.assertNotIn("author_provider_configured", result)

    def test_basic_terminal_reconciler_runs_without_admission_queue(self):
        state = Path(tempfile.mkdtemp(prefix="exo-basic-terminal-reconciler-", dir="/tmp"))
        instance = state / "instances" / "factory"

        class StubRunner:
            def __init__(self, *args, **kwargs):
                self.address = "127.0.0.1:7233"
            def is_running(self):
                return False

        class EmptyJournal:
            def list_measurements(self, **_filters):
                return []

        class EmptyUsageBroker:
            def usage_journal(self):
                return EmptyJournal()

        with patch.object(harness, "Runner", StubRunner):
            harness.init_instance(instance, name="basic-reconciler", mode="factory",
                                  port=47879, home=state)
        config_path = instance / "instance.json"
        config = json.loads(config_path.read_text())
        config["basic_single_active_job"] = True
        config_path.write_text(json.dumps(config, sort_keys=True) + "\n")

        refreshed = threading.Event()
        async def refresh(_director):
            refreshed.set()

        with patch.object(harness, "Runner", StubRunner), \
                patch.object(harness.Director, "_basic_refresh_slot", refresh):
            app = harness.create_app(instance, usage_broker=EmptyUsageBroker())
            with TestClient(app) as client:
                self.assertTrue(refreshed.wait(2.0))
                health = client.get("/health").json()
                self.assertEqual(health["admission"]["status"], "unconfigured")
                self.assertEqual(health["admission_reconciler"]["status"], "unconfigured")
                self.assertEqual(health["basic_terminal_reconciler"]["status"], "running")
                self.assertGreaterEqual(health["basic_terminal_reconciler"]["checked"], 1)
                self.assertEqual(health["basic_job"], {
                    "configured": True, "busy": False, "state": "available"})


if __name__ == "__main__":
    unittest.main()
