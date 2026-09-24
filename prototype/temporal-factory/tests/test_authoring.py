"""Strands authoring contract and bounded validation tests."""
from __future__ import annotations

import json
from unittest.mock import patch
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from strands.models import Model
from authoring import (AuthoringSession, ScriptedAuthoringModel, StrandsGraphAuthor,
                       authoring_vocabulary, materialize, model_from_environment)
from definition import validate


NAMES = ("source_alpha", "source_beta", "counter_alpha", "counter_beta", "quality", "release")


def bindings():
    return {name: {"role": "capability" if name.startswith(("source", "counter")) else name,
                   "url": f"http://127.0.0.1:{45200 + i}",
                   "identity": f"test-identity-{name}", "approved": True}
            for i, name in enumerate(NAMES)}


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


class AuthoringTests(unittest.TestCase):
    def setUp(self):
        self.bindings = bindings()
        self.v1 = json.loads((ROOT / "definitions" / "v1-template.json").read_text())
        self.brief = (ROOT / "definitions" / "authoring-brief-v2.md").read_text()

    def scripted(self, max_rounds=4):
        return AuthoringSession(StrandsGraphAuthor(ScriptedAuthoringModel()),
                                approved_bindings=self.bindings, max_rounds=max_rounds).run(
                                    self.brief, self.v1)

    def test_scripted_revises_validation_error_and_approves_new_package(self):
        outcome = self.scripted()
        self.assertEqual(outcome.status, "approved")
        self.assertEqual(len(outcome.rounds), 2)
        self.assertFalse(outcome.rounds[0]["valid"])
        self.assertEqual(outcome.rounds[0]["errors"][0]["message"],
                         "route must cover each typed value")
        self.assertEqual(outcome.rounds[0]["errors"][0]["missing_cases"], ["requires_scope"])
        self.assertTrue(outcome.rounds[1]["valid"])
        self.assertEqual([call["tool"] for call in outcome.tool_calls],
                         ["describe_vocabulary", "validate_draft", "submit_draft"])
        self.assertFalse(outcome.model["live"])
        self.assertEqual((outcome.model["kind"], outcome.model["provider"], outcome.model["billing"]),
                         ("scripted", "scripted", "none"))
        self.assertEqual(outcome.approval["status"], "approved")
        self.assertEqual(outcome.package_digest, validate(outcome.package, self.bindings))
        v1_digest = validate(materialize(self.v1, self.bindings), self.bindings)
        self.assertNotEqual(outcome.package_digest, v1_digest)
        self.assertEqual(outcome.package["run_inputs"]["question"]["allowed_actors"],
                         ["fixture-operator"])
        branches = outcome.template["child"]["nodes"]["gather"]["branches"]
        self.assertEqual(set(branches), {"source_alpha", "source_beta", "counter_alpha"})
        self.assertEqual(outcome.template["child"]["nodes"]["repair"]["max_repairs"], 1)

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
                    draft["child"]["nodes"]["gather"]["branches"]["source_alpha"]["service"] = "unknown"
                else:
                    draft["child"]["nodes"]["draft_clear"]["type"] = "python"
                model = SubmittingModel(draft)
                outcome = AuthoringSession(StrandsGraphAuthor(model),
                                           approved_bindings=self.bindings).run(self.brief)
                self.assertEqual(outcome.status, "no_submission")
                self.assertFalse(model.feedback["ok"])
                self.assertEqual(model.feedback["errors"][0]["message"], expected)

    def test_vocabulary_and_unavailable_live_model(self):
        vocabulary = authoring_vocabulary(self.bindings)
        self.assertEqual(vocabulary["route_values"]["join.route_status"],
                         ["clear", "requires_scope"])
        with patch("model_broker.ModelBroker.ensure_started", return_value={"signed_in": False}), \
                patch.dict("os.environ", {"EXO_AUTHOR_PROVIDER": "codex-subscription"}, clear=True):
            model, reason = model_from_environment()
        self.assertIsNone(model)
        self.assertTrue(reason)


if __name__ == "__main__":
    unittest.main()
