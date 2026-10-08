"""Unit checks for static identity snapshots and URL-less Agent Card pins."""
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import a2a_extensions
import agent_binding
import long_client


class BindingTests(unittest.TestCase):
    def setUp(self):
        self.identity = "agent-1"
        self.url = "http://127.0.0.1:46200"
        self.card = self.make_card(self.url, self.identity)
        self.path = Path(tempfile.mkdtemp(prefix="exo-qual-a-bind-", dir="/tmp")) / "agent_snapshot.json"
        self.snapshot(self.url)

    def make_card(self, url, identity, *, resend=a2a_extensions.RESEND_RULE, required=False):
        return {"name": "counter evidence",
                "supportedInterfaces": [{"url": url, "protocolBinding": "JSONRPC",
                                         "protocolVersion": "1.0"}],
                "skills": [{"id": "counter_evidence@1"}],
                "capabilities": {"extensions": [
                    {"uri": agent_binding.EXTENSION_URI, "required": required,
                     "params": {"identity": identity, "resend": resend}},
                    {"uri": a2a_extensions.BUDGET_URI, "required": False}]}}

    def snapshot(self, url):
        self.path.write_text(json.dumps({"snapshot_version": 1,
            "agents": {self.identity: {"url": url}}}))

    def test_pin_reads_only_the_agent_card(self):
        urls = []

        def read(url):
            urls.append(url)
            return self.card

        with patch.object(agent_binding, "read_json", side_effect=read):
            pin = agent_binding.pin(self.url)
            agent_binding.resolve(self.path, self.identity, pin)
        self.assertEqual(set(urls), {self.url + "/.well-known/agent-card.json"})
        self.assertEqual(set(pin), {"card_sha256", "identity", "reconcile"})

    def test_pin_and_move_preserve_url_less_digest(self):
        with patch.object(agent_binding, "read_json", return_value=self.card):
            pin = agent_binding.pin(self.url, self.identity)
            self.assertEqual(agent_binding.resolve(self.path, self.identity, pin)[0], self.url)
        moved = "http://127.0.0.1:46201"
        self.snapshot(moved)
        with patch.object(agent_binding, "read_json", return_value=self.make_card(moved, self.identity)):
            self.assertEqual(agent_binding.resolve(self.path, self.identity, pin)[0], moved)

    def test_impostor_and_changed_card_rejected_before_send(self):
        with patch.object(agent_binding, "read_json", return_value=self.card):
            pin = agent_binding.pin(self.url, self.identity)
        with patch.object(agent_binding, "read_json", return_value=self.make_card(self.url, "other")):
            with self.assertRaisesRegex(ValueError, "pinned"):
                agent_binding.resolve(self.path, self.identity, pin)
        changed = dict(self.card, name="changed")
        with patch.object(agent_binding, "read_json", return_value=changed):
            with self.assertRaisesRegex(ValueError, "pinned"):
                agent_binding.resolve(self.path, self.identity, pin)
        with patch.object(agent_binding, "read_json", return_value=self.card):
            with self.assertRaisesRegex(ValueError, "identity"):
                agent_binding.pin(self.url, "expected-other")

    def test_worker_verifier_matches_pin(self):
        with patch.object(agent_binding, "read_json", return_value=self.card):
            pin = agent_binding.pin(self.url, self.identity)
            url, observed = long_client.resolve_pinned(self.path, self.identity, pin)
        self.assertEqual(url, self.url)
        self.assertEqual(observed["card_sha256"], pin["card_sha256"])
        self.assertEqual(observed["skills"], ["counter_evidence@1"])

    def test_undeclared_resend_rule_stays_opaque(self):
        card = self.make_card(self.url, self.identity, resend=None)
        with patch.object(agent_binding, "read_json", return_value=card):
            pin = agent_binding.pin(self.url, self.identity)
        self.assertEqual(pin["reconcile"], "opaque")

    def test_required_extension_or_missing_identity_is_rejected(self):
        with patch.object(agent_binding, "read_json",
                          return_value=self.make_card(self.url, self.identity, required=True)):
            with self.assertRaisesRegex(ValueError, "requires extensions"):
                agent_binding.pin(self.url)
        anonymous = dict(self.card, capabilities={"extensions": []})
        with patch.object(agent_binding, "read_json", return_value=anonymous):
            with self.assertRaisesRegex(ValueError, "no agent identity"):
                agent_binding.pin(self.url)

    def test_card_that_is_not_v1_only_is_rejected(self):
        legacy = {key: value for key, value in self.card.items() if key != "supportedInterfaces"}
        legacy.update({"url": self.url, "protocolVersion": "0.3.0"})
        dual = dict(self.card, supportedInterfaces=[
            *self.card["supportedInterfaces"],
            {"url": self.url + "/v03", "protocolBinding": "JSONRPC", "protocolVersion": "0.3"}])
        extra_legacy_field = dict(self.card, protocolVersion="0.3.0")
        for card in (legacy, dual, extra_legacy_field):
            with self.subTest(card=sorted(card)), patch.object(
                    agent_binding, "read_json", return_value=card):
                with self.assertRaisesRegex(ValueError, "A2A v1.0 only"):
                    agent_binding.pin(self.url, self.identity)


if __name__ == "__main__":
    unittest.main()
