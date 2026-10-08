"""Operator stack launcher: arguments, /tmp refusal, port plan, upgrades and down.

These tests never start a process, bind a port or touch the operator's home;
every process and CLI call is faked.
"""
from __future__ import annotations

import contextlib
import io
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

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


def _write(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


class UpgradeAndReprovision(unittest.TestCase):
    """Upgrade decisions, refusals and --reprovision; every process call is faked."""

    def setUp(self):
        self.scratch = Path(tempfile.mkdtemp(prefix="exo-opstack-unit-")).resolve()
        self.addCleanup(shutil.rmtree, self.scratch, True)
        self.home = self.scratch / "opstack"
        self.home.mkdir()
        for target, value in (("_check_binaries", None), ("listening_ports", {}),
                              ("status", {})):
            patcher = patch.object(stack, target, return_value=value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.calls: list[tuple[str, ...]] = []

    def args(self, *extra: str):
        return stack.parse_args(["up", "--home", str(self.home), "--scratch-home",
                                 "--model-provider", "scripted", *extra])

    def fake_cli(self, plan: dict | None = None, testbed_up: dict | None = None):
        import admin
        state = {"published": False}

        def cli(args, env, *command, timeout=300):
            self.calls.append(command)
            program, verb = Path(command[0]).name, command[1]
            if (program, verb) == ("testbed.py", "plan"):
                return plan or {}
            if (program, verb) == ("testbed.py", "up"):
                return testbed_up or {"restarted": [], "identity_changed": []}
            if (program, verb) == ("admin.py", "repin"):
                return admin.repin(Path(command[3]), testbed=Path(command[5]))
            if (program, verb) == ("admin.py", "publish-template"):
                state["published"] = True
            return {}
        return cli, state

    def verbs(self) -> list[tuple[str, str]]:
        return [(Path(command[0]).name, command[1]) for command in self.calls]

    def test_newer_schema_is_refused_with_the_reprovision_flag_and_nothing_changes(self):
        _write(self.home / "operator-stack.json", {"schema": stack.LAUNCHER_SCHEMA + 1})
        cli, _ = self.fake_cli()
        with patch.object(stack, "_cli", side_effect=cli), \
                self.assertRaises(stack.Refused) as refused:
            stack.up(self.args())
        self.assertIn("--reprovision", str(refused.exception))
        self.assertIn("newer launcher", str(refused.exception))
        self.assertEqual(self.verbs(), [])
        self.assertEqual(sorted(p.name for p in self.home.iterdir()),
                         ["logs", "operator-stack.json", "operator-stack.lock", "run", "tmp"])

    def test_agent_upgrade_with_unfinished_runs_is_refused_before_any_agent_restarts(self):
        args = self.args()
        _write(self.home / "operator-stack.json", {"schema": 1, "ports": args.plan})
        database = self.home / "instances" / stack.INSTANCE / "director.sqlite3"
        database.parent.mkdir(parents=True)
        with sqlite3.connect(database) as db:
            db.execute("CREATE TABLE runs (run_id TEXT, closed INTEGER)")
            db.execute("INSERT INTO runs VALUES ('run-open', 0)")
        cli, _ = self.fake_cli(plan={"synthesizer": {"action": "replace", "upgrade": True},
                                     "release": {"action": "reuse", "upgrade": False}})
        with patch.object(stack, "_cli", side_effect=cli), \
                self.assertRaises(stack.Refused) as refused:
            stack.up(args)
        message = str(refused.exception)
        self.assertIn("['synthesizer']", message)
        self.assertIn("1 run(s) are unfinished", message)
        self.assertIn("--reprovision", message)
        self.assertEqual(self.verbs(), [("testbed.py", "plan")])

    def test_reprovision_moves_the_home_aside_and_never_deletes_it(self):
        _write(self.home / "operator-stack.json", {"schema": stack.LAUNCHER_SCHEMA + 1})
        key = self.home / "instances" / stack.INSTANCE / "handoff-digest.key"
        key.parent.mkdir(parents=True)
        key.write_bytes(b"k" * 32)
        args = self.args("--reprovision")
        with patch.object(stack, "down", return_value={"ports_still_held_by_this_home": {}}) \
                as down, patch.object(stack, "_own_pids", return_value=set()):
            first = stack.reprovision(args)
            self.assertFalse(self.home.exists())
            backup = Path(first["backup"])
            self.assertEqual(backup.parent, self.home.parent)
            self.assertRegex(backup.name, r"^opstack\.backup-\d{8}T\d{6}Z$")
            self.assertEqual((backup / "instances" / stack.INSTANCE /
                              "handoff-digest.key").read_bytes(), b"k" * 32)
            self.home.mkdir()
            second = stack.reprovision(args)
            self.assertNotEqual(second["backup"], first["backup"])
            self.assertTrue(backup.exists())
            self.assertEqual(down.call_count, 2)
            # Nothing to move: no home.
            self.assertIsNone(stack.reprovision(args)["backup"])

    def test_reprovision_refuses_to_move_a_home_whose_processes_still_run(self):
        args = self.args("--reprovision")
        with patch.object(stack, "down", return_value={"ports_still_held_by_this_home": {}}), \
                patch.object(stack, "_own_pids", return_value={4242}), \
                self.assertRaisesRegex(RuntimeError, "not moving the home aside"):
            stack.reprovision(args)
        self.assertTrue(self.home.exists())

    def test_up_with_reprovision_provisions_fresh_after_the_backup(self):
        _write(self.home / "operator-stack.json", {"schema": stack.LAUNCHER_SCHEMA + 1})
        cli, _ = self.fake_cli()

        def stop_at_testbed_up(args, env, *command, timeout=300):
            if command[1] == "up":
                raise RuntimeError("stop-here")
            return cli(args, env, *command, timeout=timeout)
        with patch.object(stack, "down", return_value={"ports_still_held_by_this_home": {}}), \
                patch.object(stack, "_own_pids", return_value=set()), \
                patch.object(stack, "_cli", side_effect=stop_at_testbed_up), \
                self.assertRaisesRegex(RuntimeError, "stop-here"):
            stack.up(self.args("--reprovision"))
        backups = list(self.scratch.glob("opstack.backup-*"))
        self.assertEqual(len(backups), 1)
        self.assertTrue((backups[0] / "operator-stack.json").exists())
        self.assertFalse((self.home / "operator-stack.json").exists())
        self.assertEqual(self.verbs(), [("testbed.py", "plan")])

    def test_up_after_a_contract_change_repins_republishes_and_restarts_the_harness(self):
        args = self.args()
        instance = self.home / "instances" / stack.INSTANCE
        old = {"synthesizer": {"identity": "a2a-card-old", "url": "http://127.0.0.1:1"}}
        new = {"synthesizer": {"identity": "a2a-card-new", "url": "http://127.0.0.1:1"}}
        for directory, bindings in ((instance / "catalog", old), (self.home / "testbed", new)):
            _write(directory / "approved_bindings.json", bindings)
            _write(directory / "contracts.json", {"synthesizer": bindings["synthesizer"]})
            _write(directory / "quality_policy.json", {"policy": "p"})
        _write(instance / "instance.json", {"port": args.plan["harness"],
                                            **stack.instance_options("scripted")})
        (instance / "handoff-digest.key").write_bytes(b"k" * 32)
        (instance / "factory-observation.sqlite3").write_bytes(b"observation history")
        _write(self.home / "operator-stack.json",
               {"schema": 1, "ports": args.plan, "temporal": str(args.temporal),
                "template_sha256": stack._sha256(stack.TEMPLATE)})
        _write(self.home / "run" / "harness.json", {"pid": 4242, "port": args.plan["harness"]})
        cli, published = self.fake_cli(
            plan={"synthesizer": {"action": "replace", "upgrade": True}},
            testbed_up={"restarted": ["synthesizer"], "identity_changed": ["synthesizer"]})
        process = MagicMock(pid=5151)
        process.poll.return_value = None
        with patch.object(stack, "_cli", side_effect=cli), \
                patch.object(stack, "_desired_build_id", return_value="build-1"), \
                patch.object(stack, "_active_publication", side_effect=lambda home: {
                    "build_id": "build-1",
                    "manifest_digest": "m2" if published["published"] else "m1"}), \
                patch.object(stack, "_worker", return_value={}), \
                patch.object(stack, "_harness_pid", return_value=4242), \
                patch.object(stack, "_terminate", return_value="stopped") as terminate, \
                patch.object(stack, "_wait", return_value=True), \
                patch.object(stack.subprocess, "Popen", return_value=process):
            result = stack.up(args)
        self.assertEqual(result["actions"],
                         ["testbed-up", "agents-upgraded", "repinned", "published",
                          "runner-ready", "harness-restarted", "harness-started"])
        self.assertEqual(result["upgrade"]["identity_changed"], ["synthesizer"])
        terminate.assert_called_once_with(4242)
        self.assertEqual(json.loads((instance / "catalog" / "approved_bindings.json")
                                    .read_text()), new)
        # Observation history and the digest key are the instance's own; kept.
        self.assertEqual((instance / "handoff-digest.key").read_bytes(), b"k" * 32)
        self.assertEqual((instance / "factory-observation.sqlite3").read_bytes(),
                         b"observation history")
        harness = json.loads((self.home / "run" / "harness.json").read_text())
        self.assertEqual((harness["pid"], harness["manifest_digest"]), (5151, "m2"))
        self.assertEqual(harness["pins_sha256"], stack._pins_sha256(self.home / "testbed"))
        state = json.loads((self.home / "operator-stack.json").read_text())
        self.assertEqual(state["schema"], stack.LAUNCHER_SCHEMA)
        self.assertEqual(state["model_provider"], "scripted")

    def test_reprovision_is_an_up_flag_only_and_scratch_homes_need_the_flag(self):
        self.assertIn("up only", parse_error(["down", "--home", str(NOT_CREATED),
                                              "--reprovision"]))
        self.assertIn("durable", parse_error(["up", "--home", str(self.home)]))


class DownOwnership(unittest.TestCase):
    def test_down_never_signals_an_unverified_harness_and_reports_foreign_ports(self):
        scratch = Path(tempfile.mkdtemp(prefix="exo-opstack-unit-"))
        self.addCleanup(shutil.rmtree, scratch, True)
        home = scratch / "opstack"
        _write(home / "run" / "harness.json", {"pid": os.getpid(), "port": 47053})
        args = stack.parse_args(["down", "--home", str(home), "--scratch-home"])
        plan = args.plan
        with patch.object(stack, "_cli") as cli, \
                patch.object(stack, "_terminate") as terminate, \
                patch.object(stack, "port_holders",
                             side_effect=lambda port: {777} if port == plan["harness"] else set()), \
                patch.object(stack, "in_home", return_value=False), \
                patch.object(stack, "listening_ports", return_value={plan["harness"]: 777}):
            result = stack.down(args)
        # The recorded pid is alive (this test process) but is not this home's harness.
        self.assertEqual(result["harness"], "not-touched:unverified")
        terminate.assert_not_called()
        cli.assert_not_called()
        self.assertFalse(result["ports_free"])
        self.assertEqual(result["ports_held_outside_this_home"],
                         {"harness": {"port": plan["harness"], "pids": [777]}})
        self.assertEqual(result["ports_still_held_by_this_home"], {})


if __name__ == "__main__":
    unittest.main()
