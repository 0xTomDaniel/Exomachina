"""Home-owned runner configuration and instance provisioning boundaries."""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC))
from harness import init_instance  # noqa: E402
from runner import Runner  # noqa: E402


class SameHomeTests(unittest.TestCase):
    def setUp(self):
        self.home = Path(tempfile.mkdtemp(prefix="exo-qual-b-unit-", dir="/tmp"))
        self.alpha = self.home / "instances/alpha"
        self.beta = self.home / "instances/beta"
        self.runner = {"port_base": 44300, "member_base": 32460}

    def test_home_owns_runner_ports_and_rejects_conflict(self):
        init_instance(self.alpha, name="alpha", mode="factory", port=46250,
                      home=self.home, runner=self.runner)
        path = self.home / "runner-config.json"
        self.assertEqual(json.loads(path.read_text()), self.runner)
        self.assertEqual(Runner(self.home).address, "127.0.0.1:44302")
        with self.assertRaisesRegex(ValueError, "runner ports differ from home"):
            init_instance(self.beta, name="beta", mode="factory", port=46251,
                          home=self.home, runner={"port_base": 44320, "member_base": 32470})
        self.assertFalse((self.beta / "instance.json").exists())
        self.assertEqual(json.loads(path.read_text()), self.runner)

    def test_duplicate_harness_port_does_not_provision_peer(self):
        config = init_instance(self.alpha, name="alpha", mode="factory", port=46250,
                               home=self.home, runner=self.runner)
        self.assertEqual(init_instance(self.alpha, name="alpha", mode="factory", port=46250,
                                       home=self.home, runner=self.runner), config)
        with self.assertRaisesRegex(ValueError, "harness port 46250 already configured"):
            init_instance(self.beta, name="beta", mode="factory", port=46250,
                          home=self.home, runner=self.runner)
        self.assertFalse((self.beta / "instance.json").exists())


if __name__ == "__main__":
    unittest.main()
