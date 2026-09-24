"""Unit checks for static identity snapshots and URL-less card pins."""
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import agent_binding
import long_client


class BindingTests(unittest.TestCase):
    def setUp(self):
        self.identity = "agent-1"
        self.url = "http://127.0.0.1:46200"
        self.contract = {"name": agent_binding.CONTRACT,
                         "reconcile": "a2a-idempotent-resend",
                         "idempotency": {"key": "action_id", "same_payload": "original_task_id",
                                         "commit_before_response": True}}
        self.card = self.make_card(self.url, self.identity)
        self.path = Path(tempfile.mkdtemp(prefix="exo-qual-a-bind-", dir="/tmp")) / "agent_snapshot.json"
        self.snapshot(self.url)

    def make_card(self, url, identity):
        return {"name": "counter evidence", "url": url, "skills": [{"id": "counter_evidence@1"}],
                "capabilities": {"extensions": [{"uri": agent_binding.EXTENSION_URI,
                    "required": True, "params": {"identity": identity,
                    "contract": agent_binding.CONTRACT,
                    "contract_digest": agent_binding.digest(self.contract)}}]}}

    def snapshot(self, url):
        self.path.write_text(json.dumps({"snapshot_version": 1,
            "agents": {self.identity: {"url": url}}}))

    def responses(self, card):
        return lambda url: card if url.endswith("agent-card.json") else self.contract

    def test_pin_and_move_preserve_url_less_digest(self):
        with patch.object(agent_binding, "read_json", side_effect=self.responses(self.card)):
            pin = agent_binding.pin(self.url, self.identity)
            self.assertEqual(agent_binding.resolve(self.path, self.identity, pin)[0], self.url)
        moved = "http://127.0.0.1:46201"
        self.snapshot(moved)
        with patch.object(agent_binding, "read_json", side_effect=self.responses(self.make_card(moved, self.identity))):
            self.assertEqual(agent_binding.resolve(self.path, self.identity, pin)[0], moved)

    def test_impostor_and_changed_contract_rejected_before_send(self):
        with patch.object(agent_binding, "read_json", side_effect=self.responses(self.card)):
            pin = agent_binding.pin(self.url, self.identity)
        with patch.object(agent_binding, "read_json", side_effect=self.responses(self.make_card(self.url, "other"))):
            with self.assertRaisesRegex(ValueError, "pinned"):
                agent_binding.resolve(self.path, self.identity, pin)
        with patch.object(agent_binding, "read_json", side_effect=lambda url: self.card if url.endswith("agent-card.json") else {"changed": True}):
            with self.assertRaisesRegex(ValueError, "pinned"):
                agent_binding.resolve(self.path, self.identity, pin)

    def test_worker_verifier_matches_pin(self):
        with patch.object(agent_binding, "read_json", side_effect=self.responses(self.card)):
            pin = agent_binding.pin(self.url, self.identity)
        with patch.object(agent_binding, "read_json", side_effect=self.responses(self.card)):
            url, observed = long_client.resolve_pinned(self.path, self.identity, pin)
        self.assertEqual(url, self.url)
        self.assertEqual(observed["card_sha256"], pin["card_sha256"])

    def test_self_consistent_opaque_document_cannot_grant_resend(self):
        self.contract["reconcile"] = "opaque"
        card = self.make_card(self.url, self.identity)
        with patch.object(agent_binding, "read_json", side_effect=self.responses(card)):
            pin = agent_binding.pin(self.url, self.identity)
        self.assertEqual(pin["reconcile"], "opaque")
        self.assertEqual(pin["a2a_extension"]["contract_digest"],
                         agent_binding.digest(self.contract))


if __name__ == "__main__":
    unittest.main()
