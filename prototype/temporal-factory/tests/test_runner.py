"""Pure runner contract checks; live process smoke is in evidence/runner-smoke.json."""
import hashlib
import json
import os
import sys
import types
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from runner import Runner, parse_ready, port_map, render_config


class RunnerTests(unittest.TestCase):
    def test_port_mapping(self):
        self.assertEqual(port_map(46000, 32500), {
        "postgres": 32500, "front_member": 32501, "match_member": 32502,
        "history_member": 32503, "worker_member": 32504,
        "frontend": 46002, "http": 46003, "matching": 46004,
        "history": 46005, "worker": 46006, "pprof": 46011,
        "metrics": 46012,
        })

    def test_port_limits_and_default(self):
        with mock.patch.dict(os.environ):
            os.environ.pop("EXO_RUNNER_PORT_BASE", None)
            os.environ.pop("EXO_RUNNER_MEMBER_BASE", None)
            self.assertEqual(Runner(Path("/tmp/exo-proto-runner-unit-default")).ports["worker_member"], 32404)
        self.assertLessEqual(port_map(44000, 32400)["worker_member"], 32767)
        self.assertEqual(port_map(46000, 32763)["worker_member"], 32767)
        for member_base in (1023, 32764, 33600, 65535):
            with self.subTest(member_base=member_base):
                with self.assertRaisesRegex(ValueError, "member_base"):
                    port_map(46000, member_base)
                with self.assertRaisesRegex(ValueError, "member_base"):
                    Runner(Path("/tmp/exo-proto-runner-unit-invalid"), member_base=member_base)
        for port_base in (1023, 65524, 65535):
            with self.subTest(port_base=port_base):
                with self.assertRaisesRegex(ValueError, "port_base"):
                    port_map(port_base, 32500)
                with self.assertRaisesRegex(ValueError, "port_base"):
                    Runner(Path("/tmp/exo-proto-runner-unit-invalid"), port_base=port_base)

    def test_no_schema_widening(self):
        source = (Path(__file__).resolve().parents[1] / "src" / "runner.py").read_text()
        self.assertNotIn("ALTER TABLE", source)
        self.assertNotIn("rpc_port", source)


    def test_config_rendering(self):
        template = (Path(__file__).resolve().parents[1] / "src" / "server.template.yaml").read_text()
        config = render_config(template, port_map(46000, 32500), "safe-secret", Path("/tmp/example-runtime"))
        self.assertIn('connectAddr: "127.0.0.1:32500"', config)
        self.assertIn("grpcPort: 46002", config)
        self.assertIn("membershipPort: 32504", config)
        self.assertIn('password: "safe-secret"', config)
        self.assertIn("/tmp/example-runtime/config/dynamicconfig/development-sql.yaml", config)
        self.assertIn("maxConns: 8", config)


    def test_build_snapshot_immutable_and_conflict(self):
        root = Path(tempfile.mkdtemp(prefix="exo-proto-runner-unit-", dir="/tmp"))
        source = root / "source"
        source.mkdir()
        (source / "worker.py").write_text("print('one')\n")
        binding = types.ModuleType("binding")
        binding.INTERPRETER_FILES = ("worker.py",)
        binding.source_digest = lambda directory: hashlib.sha256((directory / "worker.py").read_bytes()).hexdigest()
        binding.build_id_for = lambda digest: "b-" + digest[:12]
        prior = sys.modules.get("binding")
        sys.modules["binding"] = binding
        try:
            runner = Runner(root / "home")
            record = runner.ensure_build(source)
            self.assertEqual(runner.ensure_build(source), record)
            self.assertEqual((Path(record["path"]) / "worker.py").read_text(), "print('one')\n")
            (Path(record["path"]) / "worker.py").write_text("print('changed')\n")
            with self.assertRaisesRegex(ValueError, "immutable build content conflict"):
                runner.ensure_build(source)
            (Path(record["path"]) / "build.json").write_text("{}")
            with self.assertRaisesRegex(ValueError, "immutable build conflict"):
                runner.ensure_build(source)
        finally:
            if prior is None:
                sys.modules.pop("binding", None)
            else:
                sys.modules["binding"] = prior


    def test_ready_file_parsing(self):
        path = Path(tempfile.mkdtemp(prefix="exo-proto-runner-unit-", dir="/tmp")) / "runner-ready.json"
        self.assertIsNone(parse_ready(path))
        path.write_text("{")
        self.assertIsNone(parse_ready(path))
        path.write_text(json.dumps({"pid": 10, "address": "127.0.0.1:46002",
                                    "namespace": "exomachina", "builds": {}}))
        self.assertEqual(parse_ready(path)["pid"], 10)
        path.write_text(json.dumps({"pid": 0, "address": "127.0.0.1:46002",
                                    "namespace": "exomachina", "builds": {}}))
        self.assertIsNone(parse_ready(path))


if __name__ == "__main__":
    unittest.main()
