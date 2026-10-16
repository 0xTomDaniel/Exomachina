"""Unit checks for static identity snapshots and URL-less Agent Card pins.

An agent's identity is its pinned Agent Card (A2A decision 9): no extension
or metadata from the agent is needed to pin it.
"""
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import a2a_extensions
import a2a_v1
import agent_binding
import long_client


class BindingTests(unittest.TestCase):
    def setUp(self):
        self.url = "http://127.0.0.1:46200"
        self.card = self.make_card(self.url)
        self.identity = self.derived(self.card)
        self.path = Path(tempfile.mkdtemp(prefix="exo-qual-a-bind-", dir="/tmp")) / "agent_snapshot.json"
        self.snapshot(self.url)

    @staticmethod
    def derived(card):
        return agent_binding.card_identity(agent_binding.digest(a2a_v1.card_without_endpoint(card)))

    def make_card(self, url, *, name="counter evidence", tags=("message-id-idempotent",),
                  extensions=None):
        return {"name": name,
                "supportedInterfaces": [{"url": url, "protocolBinding": "JSONRPC",
                                         "protocolVersion": "1.0"}],
                "skills": [{"id": "counter_evidence@1", "tags": list(tags)}],
                "capabilities": {"extensions": extensions if extensions is not None else [
                    {"uri": a2a_extensions.BUDGET_URI, "required": False}]}}

    def snapshot(self, url, identity=None):
        self.path.write_text(json.dumps({"snapshot_version": 1,
            "agents": {identity or self.identity: {"url": url}}}))

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

    def test_identity_is_derived_from_the_pinned_card(self):
        with patch.object(agent_binding, "read_json", return_value=self.card):
            pin = agent_binding.pin(self.url)
        self.assertEqual(pin["identity"], agent_binding.card_identity(pin["card_sha256"]))
        self.assertTrue(pin["identity"].startswith(agent_binding.CARD_IDENTITY_PREFIX))
        other = self.make_card(self.url, name="another agent")
        with patch.object(agent_binding, "read_json", return_value=other):
            self.assertNotEqual(agent_binding.pin(self.url)["identity"], pin["identity"])

    def test_pin_and_move_preserve_url_less_digest(self):
        with patch.object(agent_binding, "read_json", return_value=self.card):
            pin = agent_binding.pin(self.url, self.identity)
            self.assertEqual(agent_binding.resolve(self.path, self.identity, pin)[0], self.url)
        moved = "http://127.0.0.1:46201"
        self.snapshot(moved)
        with patch.object(agent_binding, "read_json", return_value=self.make_card(moved)):
            self.assertEqual(agent_binding.resolve(self.path, self.identity, pin)[0], moved)

    def test_impostor_and_changed_card_rejected_before_send(self):
        with patch.object(agent_binding, "read_json", return_value=self.card):
            pin = agent_binding.pin(self.url, self.identity)
        changed = dict(self.card, name="changed")
        with patch.object(agent_binding, "read_json", return_value=changed):
            with self.assertRaisesRegex(ValueError, "pinned"):
                agent_binding.resolve(self.path, self.identity, pin)
        # A card claiming the pinned digest under another identity is refused too.
        with patch.object(agent_binding, "read_json", return_value=self.card):
            with self.assertRaisesRegex(ValueError, "pinned"):
                agent_binding.resolve(self.path, self.identity,
                                      dict(pin, identity="a2a-card-impostor"))
            with self.assertRaisesRegex(ValueError, "identity"):
                agent_binding.pin(self.url, "expected-other")

    def test_worker_verifier_matches_pin(self):
        with patch.object(agent_binding, "read_json", return_value=self.card):
            pin = agent_binding.pin(self.url, self.identity)
            url, observed = long_client.resolve_pinned(self.path, self.identity, pin)
        self.assertEqual(url, self.url)
        self.assertEqual(observed["card_sha256"], pin["card_sha256"])
        self.assertEqual(observed["identity"], self.identity)
        self.assertEqual(observed["skills"], ["counter_evidence@1"])

    def test_reconcile_follows_the_message_id_idempotent_skill_tag(self):
        self.assertEqual(agent_binding.IDEMPOTENT_RESEND_TAG, "message-id-idempotent")
        with patch.object(agent_binding, "read_json", return_value=self.card):
            self.assertEqual(agent_binding.pin(self.url)["reconcile"], "a2a-idempotent-resend")
        untagged = self.make_card(self.url, tags=())
        with patch.object(agent_binding, "read_json", return_value=untagged):
            pin = agent_binding.pin(self.url)
        self.assertEqual(pin["reconcile"], "opaque")
        # A pin that promises resend cannot resolve against a card that does not.
        self.snapshot(self.url, pin["identity"])
        with patch.object(agent_binding, "read_json", return_value=untagged):
            with self.assertRaisesRegex(ValueError, "pinned"):
                agent_binding.resolve(self.path, pin["identity"],
                                      dict(pin, reconcile="a2a-idempotent-resend"))

    def test_plain_card_without_extensions_is_pinned(self):
        plain = self.make_card(self.url, tags=(), extensions=[])
        del plain["capabilities"]
        with patch.object(agent_binding, "read_json", return_value=plain):
            pin = agent_binding.pin(self.url)
            self.snapshot(self.url, pin["identity"])
            url, observed = agent_binding.resolve(self.path, pin["identity"], pin)
        self.assertEqual((url, pin["reconcile"]), (self.url, "opaque"))
        self.assertEqual(observed["required_extensions"], [])

    def test_required_extension_is_rejected(self):
        required = self.make_card(self.url, extensions=[
            {"uri": "urn:example:something", "required": True}])
        with patch.object(agent_binding, "read_json", return_value=required):
            with self.assertRaisesRegex(ValueError, "requires extensions"):
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
