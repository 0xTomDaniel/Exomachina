"""Operator stack launcher: argument validation, /tmp refusal and port plan.

These tests never start a process, bind a port or touch the operator's home.
"""
from __future__ import annotations

import contextlib
import io
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scenarios"))
sys.path.insert(0, str(ROOT / "services"))
import operator_stack as stack  # noqa: E402

NOT_CREATED = Path.home() / ".exomachina" / "operator-stack-unit-test-never-created"


def parse_error(argv: list[str]) -> str:
    """Return argparse's stderr for a rejected command line (exit status 2)."""
    stderr = io.StringIO()
    try:
        with contextlib.redirect_stderr(stderr):
            stack.parse_args(argv)
    except SystemExit as error:
        assert error.code == 2, error.code
        return stderr.getvalue()
    raise AssertionError(f"accepted {argv}")


class HomeValidation(unittest.TestCase):
    def test_refuses_tmp_and_private_tmp(self):
        for home in ("/tmp/operator-stack", "/private/tmp/operator-stack", "/tmp",
                     "/private/var/folders/x5/abc/T/operator-stack"):
            with self.subTest(home=home), self.assertRaisesRegex(ValueError, "durable"):
                stack.validate_home(Path(home))

    def test_refuses_symlinked_tmp_and_tmpdir(self):
        with tempfile.TemporaryDirectory() as scratch:
            with self.assertRaisesRegex(ValueError, "durable"):
                stack.validate_home(Path(scratch) / "home")
            with patch.dict("os.environ", {"TMPDIR": scratch}), \
                    self.assertRaisesRegex(ValueError, "durable"):
                stack.validate_home(Path(scratch) / "nested" / "home")

    def test_accepts_durable_home_without_creating_it(self):
        self.assertEqual(stack.validate_home(NOT_CREATED), NOT_CREATED.resolve())
        self.assertEqual(stack.validate_home(Path("~/.exomachina/x")),
                         (Path.home() / ".exomachina" / "x").resolve())
        self.assertFalse(NOT_CREATED.exists())

    def test_default_home_is_durable(self):
        self.assertEqual(stack.DEFAULT_HOME, Path.home() / ".exomachina" / "operator-stack")
        stack.validate_home(stack.DEFAULT_HOME)

    def test_refuses_filesystem_root(self):
        with self.assertRaisesRegex(ValueError, "root"):
            stack.validate_home(Path("/"))


class PortPlan(unittest.TestCase):
    def test_default_plan_matches_operator_topology(self):
        plan = stack.port_plan()
        self.assertEqual(plan["harness"], 47053)
        self.assertEqual([plan[name] for name in stack.SERVICE_NAMES],
                         [47100, 47101, 47102, 47103, 47104])
        self.assertEqual(stack.SERVICE_NAMES[-1], "release")
        self.assertEqual(plan["runner.frontend"], stack.DEFAULT_RUNNER_PORT_BASE + 2)
        self.assertEqual(plan["runner.postgres"], stack.DEFAULT_RUNNER_MEMBER_BASE)
        self.assertLessEqual(plan["runner.worker_member"], 32767)
        self.assertEqual(len(set(plan.values())), len(plan))

    def test_service_order_matches_testbed(self):
        import testbed
        self.assertEqual(stack.SERVICE_NAMES, testbed.REPORT_NAMES)

    def test_member_base_must_stay_at_or_below_32767(self):
        stack.port_plan(runner_member_base=32763)
        with self.assertRaisesRegex(ValueError, "32767"):
            stack.port_plan(runner_member_base=32764)

    def test_overlaps_are_refused(self):
        with self.assertRaisesRegex(ValueError, "harness"):
            stack.port_plan(agent_port_base=47050)          # quality lands on 47053
        with self.assertRaisesRegex(ValueError, "planned for both"):
            stack.port_plan(runner_port_base=47090)         # pprof 47101 = research_risks
        with self.assertRaisesRegex(ValueError, "planned for both"):
            stack.port_plan(harness_port=47104)

    def test_out_of_range_ports_are_refused(self):
        with self.assertRaisesRegex(ValueError, "1024..65535"):
            stack.port_plan(harness_port=80)
        with self.assertRaisesRegex(ValueError, "1024..65535"):
            stack.port_plan(agent_port_base=65533)

    def test_conflicts_ignore_own_processes(self):
        plan = stack.port_plan()
        listening = {47053: 10, 47100: 11, 47022: 12, 9999: 13}
        self.assertEqual(stack.port_conflicts(plan, listening, {10, 12}),
                         [{"component": "research_findings", "port": 47100, "pid": 11}])
        self.assertEqual(stack.port_conflicts(plan, {}, set()), [])


class Arguments(unittest.TestCase):
    def test_defaults(self):
        args = stack.parse_args(["status", "--home", str(NOT_CREATED)])
        self.assertEqual(args.command, "status")
        self.assertEqual(args.home, NOT_CREATED.resolve())
        self.assertEqual(args.python, stack.DEFAULT_PYTHON)
        self.assertEqual(args.plan, stack.port_plan())
        self.assertEqual(stack.parse_args(["up"]).home, stack.DEFAULT_HOME.resolve())

    def test_rejects_unknown_command_and_bad_types(self):
        self.assertIn("invalid choice", parse_error(["restart"]))
        self.assertIn("invalid int value", parse_error(["up", "--harness-port", "x"]))

    def test_rejects_tmp_home_before_any_side_effect(self):
        with patch.object(stack, "up") as up:
            self.assertIn("durable", parse_error(["up", "--home", "/tmp/exo-operator"]))
            up.assert_not_called()

    def test_rejects_bad_port_plan_and_relative_python(self):
        self.assertIn("32767", parse_error(["up", "--home", str(NOT_CREATED),
                                            "--runner-member-base", "32765"]))
        self.assertIn("absolute", parse_error(["up", "--home", str(NOT_CREATED),
                                               "--python", "python3"]))


class Environment(unittest.TestCase):
    def test_stack_env_is_durable_and_drops_overrides(self):
        plan = stack.port_plan()
        base = {"PATH": "/usr/bin", "TMPDIR": "/tmp/worker.1", "PYTHONPATH": "/private/tmp/deps",
                "EXO_MODEL_HOME": "/tmp/model", "EXO_CODEX_BASE_URL": "http://127.0.0.1:1"}
        env = stack.stack_env(NOT_CREATED, plan, base=base)
        for key in ("PYTHONPATH", "EXO_MODEL_HOME", "EXO_CODEX_BASE_URL"):
            self.assertNotIn(key, env)
        self.assertEqual(env["TMPDIR"], str(NOT_CREATED / "tmp"))
        self.assertEqual(env["EXO_RUNNER_PORT_BASE"], str(stack.DEFAULT_RUNNER_PORT_BASE))
        self.assertEqual(env["EXO_RUNNER_MEMBER_BASE"], str(stack.DEFAULT_RUNNER_MEMBER_BASE))
        self.assertTrue(env["EXO_TEMPORAL_RUNTIME"].endswith("/server"))
        self.assertTrue(env["EXO_TEMPORAL_CLI"].endswith("/cli/temporal"))
        self.assertFalse([value for value in env.values()
                          if value.startswith(("/tmp", "/private/tmp"))])

    def test_instance_options_select_the_approved_director_profile(self):
        from model_broker import DEFAULT_MODEL_ID
        self.assertEqual(stack.INSTANCE_OPTIONS["director_model"],
                         {"provider": "codex-subscription", "model": DEFAULT_MODEL_ID})
        self.assertIs(stack.INSTANCE_OPTIONS["loopback_qa_session"], True)
        self.assertIs(stack.INSTANCE_OPTIONS["basic_single_active_job"], True)


if __name__ == "__main__":
    unittest.main()
