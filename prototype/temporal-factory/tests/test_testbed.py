"""Pure testbed metadata and authoring closure checks."""
import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "services"))

from definition import digest, validate  # noqa: E402
from testbed import (QUALITY_POLICY, SERVICE_NAMES, REPORT_CAPABILITIES, REPORT_NAMES,
                     REPORT_QUALITY_POLICY, binding_records, contract_records, down,
                     report_bindings, report_contracts, up, write_metadata)  # noqa: E402
from agent_binding import resolve  # noqa: E402


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
            self.assertEqual(contract["a2a_protocol"], "0.3.0")
            self.assertIs(contract["attested"], False)
            self.assertIs(contract["operations"]["idempotent_action_id"], True)
            self.assertEqual(contract["operations"]["lookup"],
                             "/receipts/{id}" if name == "release" else "/fixture/actions/{id}")

    def test_template_materializes_and_validates_with_generated_bindings(self):
        template = json.loads((ROOT / "definitions" / "v1-template.json").read_text())
        self.assertEqual(template["run_inputs"]["outcome_mode"]["enum"],
                         ["after_first_repair", "never"])
        self.assertEqual(template["run_inputs"]["question"], {
            "type": "string", "required": False, "source": "caller",
            "allowed_actors": ["fixture-operator"], "may_affect_acceptance": False,
        })
        child = copy.deepcopy(template["child"])
        child_digest = digest(child)
        root = copy.deepcopy(template["root"])
        for node in root["nodes"].values():
            if node["type"] == "nested_factory" and node["child_digest"] == "@child":
                node["child_digest"] = child_digest
        package = {"schema": template["schema"], "root": root,
                   "children": {child_digest: child},
                   "bindings": copy.deepcopy(self.bindings),
                   "run_inputs": copy.deepcopy(template["run_inputs"])}
        self.assertEqual(validate(package, self.bindings), digest(package))

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
        for name in REPORT_NAMES[:-1]:
            binding = bindings[name]
            contract = contracts[name]
            self.assertEqual(contract["capability"], REPORT_CAPABILITIES[name])
            self.assertEqual(contract["reconcile"], "a2a-idempotent-resend")
            resolved_url, observation = resolve(directory / "agent_snapshot.json",
                                                 binding["identity"], contract)
            self.assertEqual(resolved_url, binding["url"])
            self.assertEqual(observation["contract_document"]["capability"],
                             REPORT_CAPABILITIES[name])


if __name__ == "__main__":
    unittest.main()
