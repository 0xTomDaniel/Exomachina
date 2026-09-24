"""Hard authoring budgets through real Strands and the fixture broker."""
from __future__ import annotations

import asyncio
import base64
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from strands.models import Model
from authoring import (AuthoringBudgetExhausted, AuthoringSession, StrandsGraphAuthor,
                       _BudgetedModel)
import admin
from model_broker import (BrokerLost, ModelBroker, PiBrokerModel, SubscriptionAuthRequired,
                          SubscriptionQuotaExhausted)
from strands.types.exceptions import ModelThrottledException

PY = "/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python"
MOCK = ROOT / "broker" / "testing" / "mock-codex.mjs"


def fixture_store(folder: Path) -> Path:
    """Make a test-only store; never use the default install home."""
    home = folder / "model"
    home.mkdir(mode=0o700)
    (home / "FIXTURE_STORE").write_text("synthetic fixture\n")
    secret_dir = home / "secrets"
    secret_dir.mkdir(mode=0o700)
    encode = lambda value: base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip("=")
    access = ".".join((encode({"alg": "none"}),
                       encode({"exp": 4102444800, "https://api.openai.com/auth":
                               {"chatgpt_account_id": "acct_synthetic"}}),
                       secrets.token_urlsafe(24)))
    credential = {"type": "oauth", "access": access,
                  "refresh": "synthetic-refresh-" + secrets.token_urlsafe(24),
                  "expires": 4102444800000, "accountId": "acct_synthetic"}
    fd = os.open(secret_dir / "openai-codex.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump(credential, stream)
    return home


def start_mock(folder: Path, *, port: int = 46140, hold_ms: int = 0) -> subprocess.Popen:
    command = [os.environ.get("EXO_NODE", "node"), str(MOCK), "--port", str(port),
               "--record", str(folder / "mock-requests.jsonl"), "--script", "runaway"]
    if hold_ms:
        command += ["--hold-ms", str(hold_ms)]
    process = subprocess.Popen(command, cwd=ROOT, stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL, start_new_session=True)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError("runaway mock exited during startup")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return process
        except OSError:
            time.sleep(0.02)
    raise AssertionError("runaway mock did not listen")


def mock_rows(folder: Path) -> list[dict]:
    return [json.loads(line) for line in (folder / "mock-requests.jsonl").read_text().splitlines()]


class RunawayModel(Model):
    def __init__(self, *, hold: bool = False, alternate: bool = False):
        self.calls = 0
        self.cancelled = False
        self.hold = hold
        self.alternate = alternate

    def get_config(self):
        return {"model_id": "runaway-python", "context_window_limit": 16000}

    def update_config(self, **cfg):
        pass

    async def structured_output(self, *args, **kwargs):
        raise NotImplementedError
        yield  # pragma: no cover

    async def stream(self, messages, tool_specs=None, system_prompt=None, **kwargs):
        self.calls += 1
        if self.hold:
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                self.cancelled = True
                raise
        name = "validate_draft" if self.alternate and self.calls % 2 == 0 else "describe_vocabulary"
        args = {"template_json": json.dumps({"schema": 1})} if name == "validate_draft" else {}
        yield {"messageStart": {"role": "assistant"}}
        yield {"contentBlockStart": {"start": {"toolUse": {
            "toolUseId": f"runaway-{self.calls}", "name": name}}}}
        yield {"contentBlockDelta": {"delta": {"toolUse": {"input": json.dumps(args)}}}}
        yield {"contentBlockStop": {}}
        yield {"messageStop": {"stopReason": "tool_use"}}


class DraftSequenceModel(Model):
    def __init__(self, actions):
        self.actions = actions
        self.calls = 0

    def get_config(self):
        return {"model_id": "draft-sequence", "context_window_limit": 16000}

    def update_config(self, **cfg):
        pass

    async def structured_output(self, *args, **kwargs):
        raise NotImplementedError
        yield  # pragma: no cover

    async def stream(self, messages, tool_specs=None, system_prompt=None, **kwargs):
        self.calls += 1
        yield {"messageStart": {"role": "assistant"}}
        if self.calls <= len(self.actions):
            name, draft = self.actions[self.calls - 1]
            yield {"contentBlockStart": {"start": {"toolUse": {
                "toolUseId": f"draft-{self.calls}", "name": name}}}}
            yield {"contentBlockDelta": {"delta": {"toolUse": {"input": json.dumps({
                "template_json": json.dumps(draft)})}}}}
            yield {"contentBlockStop": {}}
            yield {"messageStop": {"stopReason": "tool_use"}}
        else:
            yield {"contentBlockStart": {"start": {}}}
            yield {"contentBlockDelta": {"delta": {"text": "Done"}}}
            yield {"contentBlockStop": {}}
            yield {"messageStop": {"stopReason": "end_turn"}}


class FailingModel(RunawayModel):
    def __init__(self, error):
        super().__init__()
        self.error = error

    async def stream(self, messages, tool_specs=None, system_prompt=None, **kwargs):
        self.calls += 1
        raise self.error
        yield  # pragma: no cover


class SlowStartingBroker(ModelBroker):
    def ensure_started(self, *, reason: str, timeout: float = 30) -> dict:
        time.sleep(0.15)
        raise BrokerLost("startup failed after deadline")


def approved_bindings() -> dict:
    names = ("source_alpha", "source_beta", "counter_alpha", "counter_beta", "quality", "release")
    return {name: {"role": "capability" if name.startswith(("source", "counter")) else name,
                   "url": f"http://127.0.0.1:{45200 + index}", "identity": f"test-{name}",
                   "approved": True} for index, name in enumerate(names)}


class BudgetTests(unittest.TestCase):
    def run_session(self, model, **limits):
        return AuthoringSession(StrandsGraphAuthor(model), approved_bindings={},
                                **limits).run("keep asking")

    def test_model_call_limit(self):
        model = RunawayModel()
        outcome = self.run_session(model, max_model_calls=3)
        self.assertEqual((outcome.status, outcome.abort["reason"]), ("aborted", "model_call_limit"))
        self.assertEqual((model.calls, outcome.abort["model_calls"]), (3, 3))
        self.assertEqual(outcome.limits["max_model_calls"], 3)
        self.assertIsNone(outcome.approval)

    def test_tool_call_limit(self):
        model = RunawayModel()
        outcome = self.run_session(model, max_tool_calls=2)
        self.assertEqual((outcome.status, outcome.abort["reason"]), ("aborted", "tool_call_limit"))
        self.assertEqual((model.calls, outcome.abort["model_calls"]), (3, 3))
        self.assertEqual(outcome.abort["tool_calls"], 2)
        self.assertIsNone(outcome.approval)

    def test_round_limit(self):
        model = RunawayModel(alternate=True)
        outcome = self.run_session(model, max_rounds=1)
        self.assertEqual((outcome.status, outcome.abort["reason"]), ("aborted", "round_limit"))
        self.assertEqual((model.calls, outcome.abort["model_calls"]), (3, 3))
        self.assertEqual((len(outcome.rounds), outcome.abort["tool_calls"]), (1, 2))
        self.assertIsNone(outcome.approval)

    def test_valid_fourth_round_can_be_revalidated_and_submitted(self):
        valid = json.loads((ROOT / "definitions" / "v1-template.json").read_text())
        invalid = [{"schema": 1, "variant": index} for index in range(3)]
        actions = [("validate_draft", draft) for draft in invalid]
        actions += [("validate_draft", valid), ("validate_draft", valid),
                    ("submit_draft", valid)]
        model = DraftSequenceModel(actions)
        outcome = AuthoringSession(StrandsGraphAuthor(model),
                                   approved_bindings=approved_bindings(),
                                   max_rounds=4).run("submit the valid draft")
        self.assertEqual(outcome.status, "approved")
        self.assertEqual((len(outcome.rounds), len(outcome.tool_calls)), (4, 6))
        self.assertTrue(outcome.rounds[3]["valid"])
        self.assertEqual(outcome.tool_calls[-1]["tool"], "submit_draft")
        self.assertIsNotNone(outcome.approval)

    def test_describe_vocabulary_after_valid_round_limit_aborts(self):
        valid = json.loads((ROOT / "definitions" / "v1-template.json").read_text())
        class DescribeAfterDraft(DraftSequenceModel):
            async def stream(self, messages, tool_specs=None, system_prompt=None, **kwargs):
                self.calls += 1
                name = "validate_draft" if self.calls == 1 else "describe_vocabulary"
                args = {"template_json": json.dumps(valid)} if self.calls == 1 else {}
                yield {"messageStart": {"role": "assistant"}}
                yield {"contentBlockStart": {"start": {"toolUse": {
                    "toolUseId": f"draft-{self.calls}", "name": name}}}}
                yield {"contentBlockDelta": {"delta": {"toolUse": {"input": json.dumps(args)}}}}
                yield {"contentBlockStop": {}}
                yield {"messageStop": {"stopReason": "tool_use"}}
        outcome = AuthoringSession(StrandsGraphAuthor(DescribeAfterDraft([])),
                                   approved_bindings=approved_bindings(), max_rounds=1).run("try another tool")
        self.assertEqual((outcome.status, outcome.abort["reason"]), ("aborted", "round_limit"))
        self.assertEqual((len(outcome.rounds), outcome.abort["tool_calls"]), (1, 1))

    def test_subscription_errors_fail_without_retry(self):
        for cls, kind in ((SubscriptionQuotaExhausted, "quota"),
                          (SubscriptionAuthRequired, "reauth_required")):
            with self.subTest(kind=kind):
                error = cls(f"model broker {kind}")
                error.kind, error.status, error.code = kind, 429, "insufficient_quota"
                model = FailingModel(error)
                outcome = self.run_session(model, max_model_calls=1)
                self.assertEqual(outcome.status, "failed")
                self.assertEqual(outcome.error, {"kind": kind, "status": 429,
                                                 "code": "insufficient_quota"})
                self.assertIsNone(outcome.abort)
                self.assertEqual(model.calls, 1)

    def test_rate_limit_followed_by_budget_abort_keeps_provider_error(self):
        error = ModelThrottledException("model broker rate_limit")
        error.kind, error.status, error.code = "rate_limit", 429, "rate_limit_exceeded"
        model = FailingModel(error)
        outcome = self.run_session(model, max_model_calls=1)
        self.assertEqual((outcome.status, outcome.abort["reason"]), ("aborted", "model_call_limit"))
        self.assertEqual(outcome.abort["last_provider_error"],
                         {"kind": "rate_limit", "status": 429, "code": "rate_limit_exceeded"})
        self.assertEqual(model.calls, 1)

    def test_deadline_bounds_broker_startup(self):
        folder = Path(tempfile.mkdtemp(prefix="exo-proto-budget-", dir="/tmp"))
        model = PiBrokerModel(SlowStartingBroker(folder), model_id="gpt-6-sol", session_id="slow")
        started = time.monotonic()
        outcome = self.run_session(model, deadline_seconds=0.025)
        self.assertEqual((outcome.status, outcome.abort["reason"]), ("aborted", "deadline"))
        self.assertLess(time.monotonic() - started, 0.12)

    def test_fifth_distinct_draft_aborts_before_evaluation(self):
        valid = json.loads((ROOT / "definitions" / "v1-template.json").read_text())
        actions = [("validate_draft", {"schema": 1, "variant": index}) for index in range(3)]
        actions += [("validate_draft", valid),
                    ("validate_draft", {"schema": 1, "variant": "fifth"})]
        model = DraftSequenceModel(actions)
        outcome = AuthoringSession(StrandsGraphAuthor(model),
                                   approved_bindings=approved_bindings(),
                                   max_rounds=4).run("try a fifth draft")
        self.assertEqual((outcome.status, outcome.abort["reason"]), ("aborted", "round_limit"))
        self.assertEqual((len(outcome.rounds), outcome.abort["tool_calls"]), (4, 4))
        self.assertEqual(model.calls, 5)
        self.assertIsNone(outcome.approval)

    def test_deadline_cancels_in_flight(self):
        model = RunawayModel(hold=True)
        outcome = self.run_session(model, deadline_seconds=0.03)
        self.assertEqual((outcome.status, outcome.abort["reason"]), ("aborted", "deadline"))
        self.assertEqual((model.calls, outcome.abort["model_calls"]), (1, 1))
        self.assertTrue(model.cancelled)
        self.assertIsNone(outcome.approval)

    def test_admin_abort_exits_nonzero_without_publication(self):
        folder = Path(tempfile.mkdtemp(prefix="exo-proto-budget-", dir="/tmp"))
        brief = folder / "brief.txt"
        brief.write_text("keep asking")
        class Module:
            published = False
            def approved(self):
                return {}
            def publish(self, *args, **kwargs):
                self.published = True
                raise AssertionError("aborted authoring must not publish")
        module = Module()
        director = type("DirectorStub", (), {"module": module})()
        output = io.StringIO()
        argv = ["admin.py", "author", "--instance-dir", str(folder),
                "--brief", str(brief), "--label", "budget-test",
                "--max-rounds", "6", "--max-model-calls", "3",
                "--max-tool-calls", "7", "--deadline-seconds", "12"]
        with patch.object(admin, "_instance", return_value=director), \
                patch("authoring.model_from_environment", return_value=(RunawayModel(), "test")), \
                patch.object(sys, "argv", argv), redirect_stdout(output):
            with self.assertRaises(SystemExit) as exit_status:
                admin.main()
        self.assertEqual(exit_status.exception.code, 2)
        result = json.loads(output.getvalue())
        self.assertEqual(result["outcome"]["abort"]["reason"], "model_call_limit")
        self.assertIn("selection_seconds", result)
        self.assertEqual(result["selection_seconds"], result["outcome"]["selection_seconds"])
        self.assertEqual(result["limits"], {"max_rounds": 6, "max_model_calls": 3,
                                             "max_tool_calls": 7, "deadline_seconds": 12.0})
        self.assertEqual(result["outcome"]["limits"], result["limits"])
        self.assertFalse(module.published)

    def test_admin_subscription_failure_is_failed_without_publication(self):
        folder = Path(tempfile.mkdtemp(prefix="exo-proto-budget-", dir="/tmp"))
        brief = folder / "brief.txt"
        brief.write_text("author a draft")
        class Module:
            def approved(self):
                return {}
            def publish(self, *args, **kwargs):
                raise AssertionError("failed authoring must not publish")
        error = SubscriptionQuotaExhausted("model broker quota")
        error.kind, error.status, error.code = "quota", 429, "insufficient_quota"
        output = io.StringIO()
        argv = ["admin.py", "author", "--instance-dir", str(folder),
                "--brief", str(brief), "--label", "failed-test"]
        with patch.object(admin, "_instance", return_value=type("DirectorStub", (), {"module": Module()})()), \
                patch("authoring.model_from_environment", return_value=(FailingModel(error), "test")), \
                patch.object(sys, "argv", argv), redirect_stdout(output):
            with self.assertRaises(SystemExit) as exit_status:
                admin.main()
        self.assertEqual(exit_status.exception.code, 2)
        result = json.loads(output.getvalue())
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["outcome"]["error"],
                         {"kind": "quota", "status": 429, "code": "insufficient_quota"})
        self.assertIsNone(result["outcome"]["abort"])


class DeadlineCancelTests(unittest.IsolatedAsyncioTestCase):
    async def test_deadline_sends_explicit_broker_cancel(self):
        folder = Path(tempfile.mkdtemp(prefix="exo-proto-budget-", dir="/tmp"))
        socket_path = folder / "broker.sock"
        cancelled = asyncio.Event()
        observed = []

        async def handle(reader, writer):
            request = json.loads(await reader.readline())
            observed.append(request)
            writer.write((json.dumps({"id": request["id"], "ev": {
                "type": "text_start", "contentIndex": 0}}) + "\n").encode())
            await writer.drain()
            line = await reader.readline()
            if line:
                observed.append(json.loads(line))
                cancelled.set()
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_unix_server(handle, path=str(socket_path))
        try:
            broker = SlowStartingBroker(folder)
            broker.socket = socket_path
            with patch.object(broker, "ensure_started", return_value={"pid": 1}):
                model = PiBrokerModel(broker, model_id="gpt-6-sol", session_id="deadline-cancel")
                budget = _BudgetedModel(model, max_calls=1,
                                        deadline=time.monotonic() + 0.04,
                                        state={"reason": None, "last_provider_error": None})
                with self.assertRaisesRegex(AuthoringBudgetExhausted, "deadline"):
                    [event async for event in budget.stream([])]
            await asyncio.wait_for(cancelled.wait(), 2)
            self.assertEqual(observed[1], {"id": observed[0]["id"], "op": "cancel"})
        finally:
            server.close()
            await server.wait_closed()


class NodeBrokerBudgetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if shutil.which(os.environ.get("EXO_NODE", "node")) is None:
            raise AssertionError("Node is required for the broker budget tests")
        if not MOCK.is_file() or not (ROOT / "broker" / "node_modules" / "@earendil-works" / "pi-ai").is_dir():
            raise AssertionError("Node mock and installed pi-ai are required for broker budget tests")

    def test_runaway_mock_stops_at_each_budget(self):
        for name, limits, expected, hold_ms in (
                ("model_call_limit", {"max_model_calls": 2}, 2, 0),
                ("tool_call_limit", {"max_tool_calls": 2}, 3, 0),
                ("round_limit", {"max_rounds": 1}, 3, 0),
                ("deadline", {"deadline_seconds": 0.15}, 1, 600)):
            with self.subTest(reason=name):
                folder = Path(tempfile.mkdtemp(prefix="exo-proto-budget-", dir="/tmp"))
                home = fixture_store(folder)
                mock = start_mock(folder, hold_ms=hold_ms)
                broker = ModelBroker(home)
                try:
                    with patch.dict(os.environ, {"EXO_MODEL_HOME": str(home),
                                                 "EXO_CODEX_BASE_URL": "http://127.0.0.1:46140/backend-api"}):
                        broker.ensure_started(reason="budget-test")
                        model = PiBrokerModel(broker, model_id="gpt-6-sol",
                                              session_id=f"budget-{name}")
                        model.provider, model.billing, model.live = "synthetic-loopback", "none", False
                        outcome = AuthoringSession(StrandsGraphAuthor(model), approved_bindings={},
                                                   **limits).run("keep calling")
                    self.assertEqual((outcome.status, outcome.abort["reason"]), ("aborted", name))
                    self.assertEqual(outcome.abort["model_calls"], expected)
                    self.assertIsNone(outcome.approval)
                    self.assertEqual(outcome.limits["max_model_calls"], limits.get("max_model_calls", 12))
                    rows = mock_rows(folder)
                    requests = [row for row in rows if row["kind"] == "request"]
                    self.assertEqual(len(requests), expected)
                    self.assertFalse(any(row["kind"] == "rejected" for row in rows))
                    if name == "deadline":
                        time.sleep(0.1)
                        self.assertTrue(any(row["kind"] == "stream" and row["aborted"]
                                            for row in mock_rows(folder)))
                finally:
                    if broker.is_running():
                        broker.stop()
                    mock.terminate()
                    mock.wait(timeout=5)

    def test_two_processes_share_one_broker_and_keep_sessions_separate(self):
        folder = Path(tempfile.mkdtemp(prefix="exo-proto-budget-", dir="/tmp"))
        home = fixture_store(folder)
        mock = start_mock(folder, hold_ms=120)
        broker = ModelBroker(home)
        child_code = """
import json, sys, time
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from authoring import AuthoringSession, StrandsGraphAuthor
from model_broker import ModelBroker, PiBrokerModel
home, session, start = Path(sys.argv[2]), sys.argv[3], float(sys.argv[4])
time.sleep(max(0, start - time.time()))
broker = ModelBroker(home)
health = broker.ensure_started(reason='process-race', timeout=20)
model = PiBrokerModel(broker, model_id='gpt-6-sol', session_id=session)
outcome = AuthoringSession(StrandsGraphAuthor(model), approved_bindings={},
                           max_model_calls=3, deadline_seconds=20).run('race fixture')
print(json.dumps({'pid': health['pid'], 'session': session,
                  'status': outcome.status, 'reason': outcome.abort['reason']}))
"""
        processes = []
        try:
            environment = {**os.environ, "EXO_MODEL_HOME": str(home),
                           "EXO_CODEX_BASE_URL": "http://127.0.0.1:46140/backend-api"}
            start_at = time.time() + 0.5
            for session in ("race-a", "race-b"):
                processes.append(subprocess.Popen([PY, "-B", "-c", child_code, str(ROOT / "src"),
                                                   str(home), session, str(start_at)],
                                                  cwd=ROOT, env=environment,
                                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                                  text=True))
            results = []
            for process in processes:
                stdout, _ = process.communicate(timeout=30)
                self.assertEqual(process.returncode, 0, "broker race child failed")
                results.append(json.loads(stdout.strip().splitlines()[-1]))
            self.assertEqual({row["pid"] for row in results}, {broker.health()["pid"]})
            events = [json.loads(line) for line in (home / "broker-events.jsonl").read_text().splitlines()]
            self.assertGreaterEqual(sum(row.get("reason") == "process-race" for row in events), 2)
            self.assertEqual({row["session"] for row in results}, {"race-a", "race-b"})
            self.assertTrue(all(row["status"] == "aborted" and
                                row["reason"] == "model_call_limit" for row in results))
            rows = mock_rows(folder)
            self.assertFalse(any(row["kind"] == "rejected" for row in rows))
            requests = [row for row in rows if row["kind"] == "request"]
            sessions = [row["headers"]["session-id"] for row in requests]
            self.assertEqual({session: sessions.count(session) for session in set(sessions)},
                             {"race-a": 3, "race-b": 3})
            self.assertLess(sessions.index("race-a"), len(sessions) - 1)
            self.assertLess(sessions.index("race-b"), len(sessions) - 1)
            self.assertLess(sessions.index("race-a"), max(i for i, s in enumerate(sessions) if s == "race-b"))
            self.assertLess(sessions.index("race-b"), max(i for i, s in enumerate(sessions) if s == "race-a"))
            replay = {session: [row for row in rows if row["kind"] == "runaway" and
                                 row["session_id"] == session] for session in ("race-a", "race-b")}
            self.assertTrue(all(len(turns) == 3 and turns[1]["replayed_reasoning"]
                                for turns in replay.values()))
            self.assertFalse(set(replay["race-a"][-1]["replayed_reasoning"]) &
                             set(replay["race-b"][-1]["replayed_reasoning"]))
        finally:
            for process in processes:
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=5)
            if broker.is_running():
                broker.stop()
            mock.terminate()
            mock.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
