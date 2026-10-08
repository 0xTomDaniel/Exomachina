"""Strands authoring contract and bounded validation tests."""
from __future__ import annotations

import json
import tempfile
from unittest.mock import patch
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from strands.models import Model
from authoring import (AuthoringSession, ScriptedAuthoringModel, StrandsGraphAuthor,
                       _authoring_usage_callback, authoring_vocabulary, materialize,
                       model_from_environment)
from definition import validate
from model_broker import ModelBroker, normalize_provider_usage


from report_fixture import packet, template, bindings


class SubmittingModel(Model):
    """Submit a chosen draft through the actual Agent tool loop."""
    def __init__(self, draft):
        self.draft = draft
        self.feedback = None

    def update_config(self, **model_config):
        pass

    def get_config(self):
        return {"model_id": "submission-test", "context_window_limit": 16000}

    async def structured_output(self, *args, **kwargs):
        raise NotImplementedError
        yield  # pragma: no cover

    async def stream(self, messages, tool_specs=None, system_prompt=None, **kwargs):
        last = messages[-1]["content"]
        result = next((block["toolResult"] for block in last if "toolResult" in block), None)
        yield {"messageStart": {"role": "assistant"}}
        if result is None:
            yield {"contentBlockStart": {"start": {"toolUse": {
                "toolUseId": "submission-test-call", "name": "submit_draft"}}}}
            yield {"contentBlockDelta": {"delta": {"toolUse": {
                "input": json.dumps({"template_json": json.dumps(self.draft)})}}}}
            yield {"contentBlockStop": {}}
            yield {"messageStop": {"stopReason": "tool_use"}}
        else:
            block = result["content"][0]
            self.feedback = json.loads(block.get("text") or json.dumps(block["json"]))
            yield {"contentBlockStart": {"start": {}}}
            yield {"contentBlockDelta": {"delta": {"text": "Done"}}}
            yield {"contentBlockStop": {}}
            yield {"messageStop": {"stopReason": "end_turn"}}


class UsageSubmittingModel(SubmittingModel):
    """Synthetic provider boundary that reports only safe usage facts."""
    def __init__(self, draft, usage_callback):
        super().__init__(draft)
        self.provider = "synthetic-loopback"
        self.reasoning_effort = "xhigh"
        self.usage_callback = usage_callback
        self.usage_context = None

    def set_usage_context(self, *, model_call_id, callback):
        self.usage_context = (model_call_id, callback)

    async def stream(self, messages, tool_specs=None, system_prompt=None, **kwargs):
        model_call_id, callback = self.usage_context
        callback({"model_call_id": model_call_id, "provider": self.provider,
                  "model_id": "synthetic-author-model", "reasoning_effort": self.reasoning_effort,
                  "usage": normalize_provider_usage({
                      "input": 17, "output": 8, "cacheRead": 2,
                      "cacheWrite": 0, "totalTokens": 27})})
        async for event in super().stream(messages, tool_specs=tool_specs,
                                          system_prompt=system_prompt, **kwargs):
            yield event


class AuthoringTests(unittest.TestCase):
    def setUp(self):
        self.bindings = bindings()
        self.v1 = template()
        self.brief = (ROOT / "definitions" / "authoring-brief-report.md").read_text()

    def scripted(self, max_rounds=4):
        return AuthoringSession(StrandsGraphAuthor(ScriptedAuthoringModel()),
                                approved_bindings=self.bindings, evidence_packet=packet(), max_rounds=max_rounds).run(
                                    self.brief, self.v1)

    def test_scripted_revises_validation_error_and_approves_new_package(self):
        outcome = self.scripted()
        self.assertEqual(outcome.status, "approved")
        self.assertEqual(len(outcome.rounds), 2)
        self.assertFalse(outcome.rounds[0]["valid"])
        self.assertEqual(outcome.rounds[0]["errors"][0]["message"],
                         "route must cover each typed value")
        self.assertEqual(outcome.rounds[0]["errors"][0]["missing_cases"], ["false"])
        self.assertTrue(outcome.rounds[1]["valid"])
        self.assertEqual([call["tool"] for call in outcome.tool_calls],
                         ["describe_vocabulary", "validate_draft", "submit_draft"])
        self.assertFalse(outcome.model["live"])
        self.assertEqual((outcome.model["kind"], outcome.model["provider"], outcome.model["billing"]),
                         ("scripted", "scripted", "none"))
        self.assertEqual(outcome.approval["status"], "approved")
        self.assertEqual(outcome.package_digest, validate(outcome.package, self.bindings))
        v1_digest = validate(materialize(self.v1, self.bindings, evidence_packet=packet()), self.bindings)
        self.assertEqual(outcome.package_digest, v1_digest)
        self.assertEqual(outcome.package["run_inputs"]["question"]["allowed_actors"],
                         ["fixture-operator"])
        branches = outcome.template["child"]["nodes"]["gather"]["branches"]
        self.assertEqual(set(branches), {"research_findings", "research_risks"})
        self.assertEqual(outcome.template["child"]["nodes"]["repair"]["max_repairs"], 2)

    def test_round_cap_blocks_corrected_submission(self):
        outcome = self.scripted(max_rounds=1)
        self.assertEqual(outcome.status, "aborted")
        self.assertEqual(outcome.abort["reason"], "round_limit")
        self.assertEqual(len(outcome.rounds), 1)
        self.assertFalse(outcome.rounds[0]["valid"])
        self.assertIsNone(outcome.package)
        self.assertIsNone(outcome.approval)

    def test_submit_rejects_unknown_binding_and_arbitrary_node(self):
        valid = self.scripted().template
        for mutation, expected in (("binding", "unapproved capability service"),
                                   ("node", "arbitrary code or unsupported block")):
            with self.subTest(mutation=mutation):
                draft = json.loads(json.dumps(valid))
                if mutation == "binding":
                    draft["child"]["nodes"]["gather"]["branches"]["research_findings"]["service"] = "unknown"
                else:
                    draft["child"]["nodes"]["draft"]["type"] = "python"
                model = SubmittingModel(draft)
                outcome = AuthoringSession(StrandsGraphAuthor(model),
                                           approved_bindings=self.bindings, evidence_packet=packet()).run(self.brief)
                self.assertEqual(outcome.status, "no_submission")
                self.assertFalse(model.feedback["ok"])
                self.assertEqual(model.feedback["errors"][0]["message"], expected)

    def test_vocabulary_and_unavailable_live_model(self):
        vocabulary = authoring_vocabulary(self.bindings)
        self.assertEqual(vocabulary["route_values"]["verdict.accepted"],
                         ["false", "true"])
        with patch("model_broker.ModelBroker.ensure_started", return_value={"signed_in": False}), \
                patch.dict("os.environ", {"EXO_AUTHOR_PROVIDER": "codex-subscription"}, clear=True):
            model, reason = model_from_environment()
        self.assertIsNone(model)
        self.assertTrue(reason)

    def test_authoring_call_persists_nullable_overhead_usage_via_broker_public_api(self):
        with tempfile.TemporaryDirectory(prefix="exo-authoring-usage-") as folder:
            broker = ModelBroker(home=Path(folder) / "model-home")
            model = UsageSubmittingModel(self.v1, _authoring_usage_callback(broker))
            outcome = AuthoringSession(StrandsGraphAuthor(model),
                approved_bindings=self.bindings, evidence_packet=packet()).run(self.brief, self.v1)

            self.assertEqual(outcome.status, "approved")
            rows = broker.usage_journal().list_measurements(call_scope="authoring_overhead")
            self.assertGreaterEqual(len(rows), 1)
            self.assertEqual(len({row["model_call_id"] for row in rows}), len(rows))
            for measurement in rows:
                self.assertTrue(measurement["model_call_id"].startswith(
                    "urn:exomachina:authoring-model-call:"))
                self.assertEqual(measurement["call_scope"], "authoring_overhead")
                self.assertIsNone(measurement["task_id"])
                self.assertIsNone(measurement["run_id"])
                self.assertIsNone(measurement["assignment_id"])
                self.assertIsNone(measurement["attempt_id"])
                self.assertEqual(measurement["usage"]["input_tokens"],
                                 {"value": 17, "status": "reported"})
                self.assertEqual(measurement["usage"]["cache_write_tokens"],
                                 {"value": 0, "status": "reported"})
                self.assertNotIn("cost", json.dumps(measurement, sort_keys=True))


if __name__ == "__main__":
    unittest.main()
