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
                     REPORT_QUALITY_POLICY, alive, binding_records, command_for,
                     contract_records, down, plan, port_holders, report_bindings,
                     report_contracts, require_distinct_identities, role_for, up,
                     write_metadata)  # noqa: E402
import shutil  # noqa: E402
import subprocess  # noqa: E402
import time  # noqa: E402
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


def _rewrite_pids(home: Path, change) -> dict:
    path = home / "testbed" / "pids.json"
    pids = json.loads(path.read_text())
    change(pids)
    path.write_text(json.dumps(pids, indent=2, sort_keys=True) + "\n")
    return pids


class ProcessOwnershipTests(unittest.TestCase):
    """down/up act only on this home's verified processes (real processes)."""

    def home(self, prefix: str) -> Path:
        home = Path(tempfile.mkdtemp(prefix=prefix, dir="/tmp"))
        self.addCleanup(shutil.rmtree, home, True)
        self.addCleanup(down, home)
        return home

    def foreign_listener(self, port: int) -> subprocess.Popen:
        process = subprocess.Popen(
            [sys.executable, "-c", "import socket,time\ns=socket.socket()\n"
             "s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\n"
             f"s.bind(('127.0.0.1',{port}))\ns.listen()\ntime.sleep(120)"])
        self.addCleanup(lambda: (process.kill(), process.wait()))
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and process.pid not in port_holders(port):
            time.sleep(0.05)
        self.assertIn(process.pid, port_holders(port))
        return process

    def test_down_stops_own_processes_whose_recorded_identity_no_longer_matches(self):
        home, port_base = self.home("exo-tb-identity-"), 45750
        started = up(home, port_base, profile="report", model_provider="scripted")
        # An identity-scheme change: every record names an identity the card no
        # longer derives. Ownership is pid + command line + port + home, so all stop.
        _rewrite_pids(home, lambda pids: [record.update(identity="a2a-card-retired")
                                          for record in pids.values()])
        result = down(home)
        self.assertEqual(result, {name: "stopped" for name in REPORT_NAMES})
        for index, name in enumerate(REPORT_NAMES):
            self.assertFalse(alive(started["pids"][name]["pid"]))
            self.assertEqual(port_holders(port_base + index), set())
        self.assertEqual(down(home), {name: "already-stopped" for name in REPORT_NAMES})

    def test_down_never_touches_a_foreign_process_named_by_a_record(self):
        home = self.home("exo-tb-foreign-")
        listener = self.foreign_listener(45770)
        (home / "testbed").mkdir()
        # A reused pid or a hand-edited record: the recorded pid is alive and
        # holds the recorded port, but its command line is not this home's.
        (home / "testbed" / "pids.json").write_text(json.dumps(
            {"release": {"pid": listener.pid, "port": 45770, "identity": "a2a-card-x"}}))
        self.assertEqual(down(home), {"release": "not-touched:foreign-process"})
        self.assertIsNone(listener.poll())
        self.assertEqual(port_holders(45770), {listener.pid})
        # up refuses to start over a port held outside this home, untouched.
        with self.assertRaisesRegex(RuntimeError, "held by a process outside this home"):
            up(home, 45766, profile="report", model_provider="scripted")
        self.assertIsNone(listener.poll())

    def test_up_after_a_card_change_replaces_only_that_agent_and_repins_it(self):
        home, port_base = self.home("exo-tb-upgrade-"), 45760
        first = up(home, port_base, profile="report", model_provider="scripted")
        self.assertEqual((first["restarted"], first["identity_changed"]), ([], []))
        before = json.loads((home / "testbed" / "approved_bindings.json").read_text())
        # Unchanged: everything is reused.
        self.assertEqual({name: value["action"] for name, value in
                          plan(home, port_base, model_provider="scripted").items()},
                         {name: "reuse" for name in REPORT_NAMES})
        # A contract change: the synthesizer now declares an extra extension,
        # so its command and Agent Card (hence identity) change.
        planned = plan(home, port_base, model_provider="scripted", test_controls=True)
        self.assertEqual(planned["synthesizer"]["action"], "replace")
        self.assertTrue(planned["synthesizer"]["upgrade"])
        second = up(home, port_base, profile="report", model_provider="scripted",
                    test_controls=True)
        self.assertEqual(second["restarted"], ["synthesizer"])
        self.assertEqual(second["identity_changed"], ["synthesizer"])
        self.assertFalse(alive(first["pids"]["synthesizer"]["pid"]))
        for name in REPORT_NAMES:
            if name != "synthesizer":
                self.assertEqual(second["pids"][name]["pid"], first["pids"][name]["pid"])
        after = json.loads((home / "testbed" / "approved_bindings.json").read_text())
        self.assertNotEqual(after["synthesizer"]["identity"], before["synthesizer"]["identity"])
        self.assertEqual({k: v for k, v in after.items() if k != "synthesizer"},
                         {k: v for k, v in before.items() if k != "synthesizer"})
        # A record from an older launcher schema (no build) is an upgrade too.
        _rewrite_pids(home, lambda pids: pids["release"].pop("build"))
        self.assertEqual(plan(home, port_base, model_provider="scripted",
                              test_controls=True)["release"]["reason"], "unrecorded-build")


if __name__ == "__main__":
    unittest.main()
