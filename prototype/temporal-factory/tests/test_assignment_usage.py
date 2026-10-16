"""Synthetic adapter tests for explicit assignment usage binding passthrough."""
from __future__ import annotations

from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import adapter  # noqa: E402


class AssignmentUsageAdapterTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.commands = []

    def review_input(self, *, include_attempt_id: bool) -> dict:
        value = {
            "candidate": {"revision": "r1", "sha256": "a" * 64,
                          "author": "synthetic-author"},
            "question": "synthetic review question",
            "packet": {"evidence": []},
            "policy_digest": "synthetic-policy-digest",
            "run": "synthetic-run-1",
            "digest": "synthetic-definition-digest",
            "assignment_id": "synthetic-quality-assignment-1",
            # This is the existing workflow review ordinal used for the action ID.
            "attempt": 2,
            "binding": {"identity": "synthetic-quality-owner"},
            "contract": {"name": "synthetic-only"},
            "factory_id": "a40ec000-0000-4000-8000-000000000001",
        }
        if include_attempt_id:
            # A distinct authoritative workflow ID, not the integer ordinal above.
            value["attempt_id"] = "synthetic-quality-attempt-id-2"
        return value

    async def invoke_review_with_capture(self, value: dict) -> dict:
        async def capture(*args, **kwargs):
            self.commands.append(args[3])
            return {"unresolved": "synthetic-response-lost"}

        with patch.object(adapter, "quality_review_request", return_value={"synthetic": True}), \
                patch.object(adapter, "_thread_with_heartbeat", new=capture):
            return await adapter.review(value)

    async def test_quality_forwards_only_explicit_first_class_bindings(self):
        result = await self.invoke_review_with_capture(self.review_input(include_attempt_id=True))
        self.assertEqual(result["inconsistent"], "quality-action-outcome-unknown")
        command = self.commands[0]
        self.assertEqual(command["assignment_id"], "synthetic-quality-assignment-1")
        self.assertEqual(command["attempt_id"], "synthetic-quality-attempt-id-2")
        self.assertEqual(command["factory_id"], "a40ec000-0000-4000-8000-000000000001")
        self.assertIn(":2:", command["action_id"])
        self.assertNotEqual(command["attempt_id"], "2")

    async def test_legacy_workflow_ordinal_does_not_fabricate_attempt_id(self):
        result = await self.invoke_review_with_capture(self.review_input(include_attempt_id=False))
        self.assertEqual(result["inconsistent"], "quality-action-outcome-unknown")
        command = self.commands[0]
        self.assertEqual(command["assignment_id"], "synthetic-quality-assignment-1")
        self.assertNotIn("attempt_id", command)

    async def test_invalid_binding_is_rejected_before_remote_invocation(self):
        value = self.review_input(include_attempt_id=True)
        value["attempt_id"] = 2
        async def forbidden_invoke(*args, **kwargs):
            self.fail("invalid usage binding reached remote invocation")

        with patch.object(adapter, "quality_review_request", return_value={"synthetic": True}), \
                patch.object(adapter, "_thread_with_heartbeat", new=forbidden_invoke):
            with self.assertRaisesRegex(ValueError, "attempt_id must be a non-empty explicit identifier"):
                await adapter.review(value)

    async def test_invalid_factory_scope_is_rejected_before_remote_invocation(self):
        value = self.review_input(include_attempt_id=True)
        value["factory_id"] = "factory with spaces"
        async def forbidden_invoke(*args, **kwargs):
            self.fail("invalid factory scope reached remote invocation")

        with patch.object(adapter, "quality_review_request", return_value={"synthetic": True}), \
                patch.object(adapter, "_thread_with_heartbeat", new=forbidden_invoke):
            with self.assertRaisesRegex(ValueError, "factory_id must be a safe identifier"):
                await adapter.review(value)

    async def test_opaque_safe_factory_scope_is_forwarded_without_uuid_interpretation(self):
        value = self.review_input(include_attempt_id=True)
        value["factory_id"] = "factory:west@v2"
        await self.invoke_review_with_capture(value)
        self.assertEqual(self.commands[0]["factory_id"], "factory:west@v2")


if __name__ == "__main__":
    unittest.main()
