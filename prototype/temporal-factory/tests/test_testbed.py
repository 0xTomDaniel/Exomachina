"""Pure testbed metadata and authoring closure checks."""
import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "services"))

from authoring import materialize  # noqa: E402
from definition import digest, validate  # noqa: E402
from testbed import (QUALITY_POLICY, SERVICE_NAMES, REPORT_CAPABILITIES, REPORT_NAMES,
                     REPORT_QUALITY_POLICY, binding_records, command_for, contract_records,
                     down, report_bindings, report_contracts, require_distinct_identities,
                     role_for, up, write_metadata)  # noqa: E402
from agent_binding import card_identity, digest as card_digest, resolve  # noqa: E402
from a2a_v1_server import card_pin_projection  # noqa: E402
import harness_server  # noqa: E402


class TestbedTests(unittest.TestCase):
    def setUp(self):
        self.health = {name: {"identity": "fixture-" + name}
                       for name in SERVICE_NAMES}
        self.bindings = binding_records(self.health, 45100)

    def test_metadata_file_shapes(self):
        home = Path(tempfile.mkdtemp(prefix="exo-proto-testbed-test-", dir="/tmp"))
        pids = {name: {"pid": 1000 + index, "port": 45100 + index,
                       "identity": "fixture-" + name}
                for index, name in enumerate(SERVICE_NAMES)}
        write_metadata(home, self.bindings, pids)
        generated = {path.stem: json.loads(path.read_text())
                     for path in (home / "testbed").glob("*.json")}
        self.assertEqual(set(generated), {"approved_bindings", "contracts",
                                          "quality_policy", "pids"})
        self.assertEqual(generated["approved_bindings"], self.bindings)
        self.assertEqual(generated["pids"], pids)
        self.assertEqual(generated["quality_policy"], QUALITY_POLICY)
        contracts = generated["contracts"]
        self.assertEqual(contracts, contract_records())
        self.assertEqual(set(contracts), set(SERVICE_NAMES))
        for index, name in enumerate(SERVICE_NAMES):
            binding = self.bindings[name]
            self.assertEqual(set(binding), {"role", "url", "identity", "approved"})
            self.assertEqual(binding["url"], f"http://127.0.0.1:{45100 + index}")
            self.assertTrue(binding["approved"])
            contract = contracts[name]
            self.assertEqual(set(contract), {"name", "role", "capability", "a2a_protocol",
                                             "input", "output", "operations", "attested"})
            self.assertEqual(contract["role"], binding["role"])
            self.assertEqual(contract["a2a_protocol"], "1.0")
            self.assertIs(contract["attested"], False)
            if name == "release":
                # An ordinary A2A agent whose receipt is its result artifact.
                self.assertEqual(contract["input"]["transport"], "a2a-SendMessage")
                self.assertEqual(contract["output"]["mode"], "artifacts")
                self.assertEqual(contract["operations"],
                                 {"idempotency": "messageId", "task_lookup": "GetTask"})
                self.assertNotIn("/", json.dumps(contract).replace("application/json", ""))
            else:
                # Agent services: plain Message, messageId idempotency, GetTask.
                self.assertEqual(contract["input"], {"transport": "a2a-SendMessage",
                                                     "message": "text-brief"})
                self.assertEqual(contract["operations"],
                                 {"idempotent_message_id": True, "task_lookup": "GetTask"})
                self.assertNotIn("/fixture/", json.dumps(contract))

    def test_template_materializes_and_validates_with_generated_bindings(self):
        template = json.loads((ROOT / "definitions" / "report-template.json").read_text())
        self.assertEqual(template["run_inputs"]["question"], {
            "type": "string", "required": True, "source": "caller",
            "allowed_actors": ["fixture-operator"], "may_affect_acceptance": False,
        })
        health = {name: {"identity": "fixture-" + name} for name in REPORT_NAMES}
        bindings = report_bindings(health, 45740)
        packet = json.loads((ROOT / "packets" / "exo-qualification-2026-09-23" /
                             "packet.json").read_text())
        package = materialize(template, bindings, evidence_packet=packet)
        self.assertEqual(package["evidence_packet"], packet)
        self.assertIn(digest(template["child"]), package["children"])
        self.assertEqual(validate(package, bindings), digest(package))

    def test_report_profile_starts_agents_and_writes_pinned_files(self):
        home = Path(tempfile.mkdtemp(prefix="exo-sf-agents-testbed-", dir="/tmp"))
        port_base = 45740
        self.addCleanup(down, home)
        result = up(home, port_base, profile="report", model_provider="scripted")
        self.assertEqual(set(result["bindings"]), set(REPORT_NAMES))
        directory = home / "testbed"
        files = {path.stem for path in directory.glob("*.json")}
        self.assertEqual(files, {"approved_bindings", "contracts", "quality_policy",
                                 "agent_snapshot", "pids"})
        bindings = json.loads((directory / "approved_bindings.json").read_text())
        contracts = json.loads((directory / "contracts.json").read_text())
        policy = json.loads((directory / "quality_policy.json").read_text())
        snapshot = json.loads((directory / "agent_snapshot.json").read_text())
        self.assertEqual(bindings, report_bindings(result["health"], port_base))
        self.assertEqual(contracts, report_contracts(bindings))
        self.assertEqual(policy, REPORT_QUALITY_POLICY)
        self.assertEqual(snapshot["snapshot_version"], 1)
        self.assertEqual(len(snapshot["agents"]), 5)
        # Every agent, release included, is identified by its pinned Agent Card.
        self.assertEqual(len({binding["identity"] for binding in bindings.values()}), 5)
        for name in REPORT_NAMES:
            binding = bindings[name]
            contract = contracts[name]
            self.assertEqual(binding["identity"], card_identity(contract["card_sha256"]))
            self.assertEqual(contract["identity"], binding["identity"])
            self.assertEqual(result["pids"][name]["identity"], binding["identity"])
            self.assertEqual(contract["reconcile"], "a2a-idempotent-resend")
            resolved_url, observation = resolve(directory / "agent_snapshot.json",
                                                 binding["identity"], contract)
            self.assertEqual(resolved_url, binding["url"])
            self.assertEqual(observation["card_sha256"], contract["card_sha256"])
            if name == "release":
                continue
            self.assertEqual(contract["capability"], REPORT_CAPABILITIES[name])
            self.assertIn(REPORT_CAPABILITIES[name], observation["skills"])
            self.assertEqual(set(contract), {"name", "role", "capability", "card_sha256",
                                             "identity", "reconcile"})
        self.assertEqual(bindings["release"]["output"], "artifacts")
        self.assertEqual(result["health"]["release"]["reconcile"], "a2a-idempotent-resend")

    def test_identical_cards_are_refused_rather_than_merged(self):
        health = {"source_alpha": {"identity": "a2a-card-same"},
                  "source_beta": {"identity": "a2a-card-same"}}
        with self.assertRaisesRegex(RuntimeError, "source_alpha and source_beta"):
            require_distinct_identities(health)
        require_distinct_identities({"a": {"identity": "a2a-card-1"},
                                     "b": {"identity": "a2a-card-2"}})

    def test_legacy_capability_fixtures_publish_distinct_cards(self):
        # Several legacy services run the same fixture program; each card names
        # its service so the card-derived identities never collide.
        identities = set()
        for name in SERVICE_NAMES:
            if role_for(name) != "capability":
                continue
            command = command_for(name, Path("/tmp/unused"), 45100)
            self.assertEqual(command[command.index("--name") + 1], name)
            card = harness_server.fixture_card("Decision round " + name, "fixture",
                                               "capability", 45100, ["fixture", "capability"])
            identities.add(card_identity(card_digest(card_pin_projection(card))))
        self.assertEqual(len(identities), 4)


if __name__ == "__main__":
    unittest.main()
