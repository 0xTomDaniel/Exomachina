"""Unix socket broker boundary tests; all replies are generated in this Python file."""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from strands import Agent, tool
from strands.types.exceptions import ModelThrottledException

from authoring import model_from_environment
from model_broker import (BrokerLost, ModelBroker, PiBrokerModel, SIG,
                          SubscriptionAuthRequired)


class AttachedBroker(ModelBroker):
    def ensure_started(self, *, reason: str, timeout: float = 30) -> dict:
        return {"signed_in": True, "pid": 1}


class FakeSocketBroker:
    def __init__(self, path: Path, script):
        self.path = path
        self.script = script
        self.requests = []
        self.cancels = []
        self.server = None

    async def start(self):
        self.server = await asyncio.start_unix_server(self.handle, path=str(self.path))

    async def close(self):
        self.server.close()
        await self.server.wait_closed()

    async def handle(self, reader, writer):
        try:
            line = await reader.readline()
            if not line:
                return
            request = json.loads(line)
            self.requests.append(request)
            if request["op"] == "health":
                writer.write((json.dumps({"id": request["id"], "health": {
                    "pid": 1, "signed_in": True, "provider": "openai-codex"}}) + "\n").encode())
                await writer.drain()
            else:
                await self.script(request, reader, writer, self)
        finally:
            writer.close()
            await writer.wait_closed()


def send(writer, request, *, event=None, done=None, error=None):
    reply = {"id": request["id"]}
    if event is not None:
        reply["ev"] = event
    elif done is not None:
        reply["done"] = done
    else:
        reply["error"] = error
    writer.write((json.dumps(reply) + "\n").encode())


def event(kind, index, **extra):
    return {"type": kind, "contentIndex": index, **extra}


def finished(content, stop="toolUse"):
    return {"content": content, "stopReason": stop,
            "usage": {"input": 3, "output": 4, "totalTokens": 7}}


class StreamTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.home = Path(tempfile.mkdtemp(prefix="exo-proto-pybroker-", dir="/tmp"))
        self.path = self.home / "s.sock"

    async def with_script(self, script):
        fake = FakeSocketBroker(self.path, script)
        await fake.start()
        self.addAsyncCleanup(fake.close)
        broker = AttachedBroker(self.home)
        broker.socket = self.path
        return fake, PiBrokerModel(broker, model_id="gpt-6-sol", session_id="test-session")

    async def test_attach_is_lazy_idempotent_and_records_reason(self):
        fake = FakeSocketBroker(self.path, lambda *args: None)
        await fake.start()
        try:
            broker = ModelBroker(self.home)
            broker.socket = self.path
            with patch("model_broker.subprocess.Popen") as spawned:
                first = await asyncio.to_thread(broker.ensure_started, reason="first-attach")
                second = await asyncio.to_thread(broker.ensure_started, reason="second-attach")
            self.assertEqual((first["pid"], second["pid"]), (1, 1))
            spawned.assert_not_called()
            events = [json.loads(line) for line in (self.home / "broker-events.jsonl").read_text().splitlines()]
            self.assertEqual([(row["event"], row["reason"]) for row in events],
                             [("attach", "first-attach"), ("attach", "second-attach")])
            self.assertTrue(all(row["caller_pid"] == os.getpid() and "pid" not in row
                                for row in events))
            self.assertEqual(len(fake.requests), 2)
        finally:
            await fake.close()

    async def test_interleaved_blocks_and_reasoning_replay(self):
        async def script(request, reader, writer, fake):
            sequence = [
                event("toolcall_start", 1, toolCall={"id": "call-a", "name": "first"}),
                event("toolcall_start", 2, toolCall={"id": "call-b", "name": "second"}),
                event("toolcall_delta", 1, delta='{"value":"'),
                event("toolcall_delta", 2, delta='{"value":"'),
                event("toolcall_delta", 1, delta='one"}'),
                event("toolcall_delta", 2, delta='two"}'),
                event("toolcall_end", 2), event("toolcall_end", 1),
            ]
            for item in sequence:
                send(writer, request, event=item)
            send(writer, request, done=finished([
                {"type": "thinking", "thinking": "plan", "thinkingSignature": "cipher"},
                {"type": "toolCall", "id": "call-a", "name": "first", "arguments": {"value": "one"}},
                {"type": "toolCall", "id": "call-b", "name": "second", "arguments": {"value": "two"}},
            ]))
            await writer.drain()

        fake, model = await self.with_script(script)
        chunks = [chunk async for chunk in model.stream([{"role": "user", "content": [{"text": "go"}]}])]
        starts = [chunk["contentBlockStart"]["start"]["toolUse"]["toolUseId"]
                  for chunk in chunks if "contentBlockStart" in chunk and
                  "toolUse" in chunk["contentBlockStart"]["start"]]
        self.assertEqual(starts, ["call-a", "call-b"])
        inputs = [chunk["contentBlockDelta"]["delta"]["toolUse"]["input"]
                  for chunk in chunks if "contentBlockDelta" in chunk and
                  "toolUse" in chunk["contentBlockDelta"]["delta"]]
        self.assertEqual(inputs, ['{"value":"', 'one"}', '{"value":"', 'two"}'])
        signature = next(chunk["contentBlockDelta"]["delta"]["reasoningContent"]["signature"]
                         for chunk in chunks if "contentBlockDelta" in chunk and
                         "signature" in chunk["contentBlockDelta"]["delta"].get("reasoningContent", {}))
        self.assertTrue(signature.startswith(SIG))
        replay = model.to_pi([{"role": "assistant", "content": [
            {"toolUse": {"toolUseId": "call-a", "name": "first", "input": {"value": "one"}}},
            {"toolUse": {"toolUseId": "call-b", "name": "second", "input": {"value": "two"}}},
            {"reasoningContent": {"reasoningText": {"text": "plan", "signature": signature}}},
        ]}])
        self.assertEqual([block["type"] for block in replay[0]["content"]],
                         ["thinking", "toolCall", "toolCall"])
        self.assertEqual(replay[0]["content"][0]["thinkingSignature"], "cipher")
        self.assertEqual(fake.requests[0]["options"]["reasoningEffort"], "low")

    async def test_real_agent_tool_loop_replays_reasoning_and_results(self):
        async def script(request, reader, writer, fake):
            if len(fake.requests) == 1:
                for item in [event("toolcall_start", 1, toolCall={"id": "call-add", "name": "add"}),
                             event("toolcall_delta", 1, delta='{"left":2,"right":3}'),
                             event("toolcall_end", 1)]:
                    send(writer, request, event=item)
                send(writer, request, done=finished([
                    {"type": "thinking", "thinking": "calculate", "thinkingSignature": "sealed"},
                    {"type": "toolCall", "id": "call-add", "name": "add",
                     "arguments": {"left": 2, "right": 3}},
                ]))
            else:
                for item in [event("text_start", 0), event("text_delta", 0, delta="five"),
                             event("text_end", 0)]:
                    send(writer, request, event=item)
                send(writer, request, done=finished([{"type": "text", "text": "five"}], "stop"))
            await writer.drain()

        fake, model = await self.with_script(script)

        @tool
        def add(left: int, right: int) -> int:
            """Add two integers.

            Args:
                left: First integer.
                right: Second integer.
            """
            return left + right

        agent = Agent(model=model, tools=[add], callback_handler=None)
        result = await asyncio.to_thread(agent, "add 2 and 3")
        self.assertIn("five", str(result))
        self.assertEqual(len(fake.requests), 2)
        history = fake.requests[1]["context"]["messages"]
        assistant = next(message for message in history if message["role"] == "assistant")
        self.assertEqual([item["type"] for item in assistant["content"]], ["thinking", "toolCall"])
        self.assertEqual(assistant["content"][0]["thinkingSignature"], "sealed")
        self.assertEqual(assistant["content"][1]["id"], "call-add")
        self.assertTrue(any(message["role"] == "toolResult" and message["toolCallId"] == "call-add"
                            for message in history))

    async def test_error_mapping_and_lost_connection(self):
        for kind, expected in [("reauth_required", SubscriptionAuthRequired),
                               ("rate_limit", ModelThrottledException),
                               ("quota", ModelThrottledException), ("lost", BrokerLost)]:
            with self.subTest(kind=kind):
                path = self.home / f"{kind}.sock"
                async def script(request, reader, writer, fake):
                    if kind != "lost":
                        send(writer, request, error={"kind": kind, "message": kind})
                        await writer.drain()
                fake = FakeSocketBroker(path, script)
                await fake.start()
                try:
                    model = PiBrokerModel(AttachedBroker(path.parent), model_id="gpt-6-sol", session_id="s")
                    model.broker.socket = path
                    with self.assertRaises(expected):
                        [chunk async for chunk in model.stream([])]
                finally:
                    await fake.close()

    async def test_cancel_on_generator_close(self):
        received = asyncio.Event()
        async def script(request, reader, writer, fake):
            send(writer, request, event=event("text_start", 0))
            await writer.drain()
            line = await reader.readline()
            if line:
                fake.cancels.append(json.loads(line))
                received.set()
        fake, model = await self.with_script(script)
        stream = model.stream([])
        await anext(stream)  # message start
        await anext(stream)  # content block start
        await stream.aclose()
        await asyncio.wait_for(received.wait(), 2)
        self.assertEqual(fake.cancels[0]["op"], "cancel")
        self.assertEqual(fake.cancels[0]["id"], fake.requests[0]["id"])

    async def test_invalid_indices_are_rejected(self):
        cases = [
            ([event("text_start", None)], "omitted"),
            ([event("text_start", 0), event("text_end", 0), event("text_start", 0)], "reused"),
            ([event("text_start", 0), event("toolcall_delta", 0, delta="x")], "match"),
            ([event("text_start", 0)], "unfinished"),
        ]
        for number, (events, fragment) in enumerate(cases):
            with self.subTest(case=number):
                path = self.home / f"bad-{number}.sock"
                async def script(request, reader, writer, fake):
                    for item in events:
                        send(writer, request, event=item)
                    send(writer, request, done=finished([], "stop"))
                    await writer.drain()
                fake = FakeSocketBroker(path, script)
                await fake.start()
                try:
                    model = PiBrokerModel(AttachedBroker(path.parent), model_id="gpt-6-sol", session_id="s")
                    model.broker.socket = path
                    with self.assertRaisesRegex(RuntimeError, fragment):
                        [chunk async for chunk in model.stream([])]
                finally:
                    await fake.close()


class SelectionTests(unittest.TestCase):
    def test_default_never_falls_back_to_api_keys(self):
        with patch.dict(os.environ, {"EXO_AUTHOR_PROVIDER": "codex-subscription",
                                     "OPENAI_API_KEY": "placeholder", "ANTHROPIC_API_KEY": "placeholder"},
                        clear=True), patch.object(ModelBroker, "ensure_started", return_value={"signed_in": False}):
            model, reason = model_from_environment()
        self.assertIsNone(model)
        self.assertEqual(reason, "codex-subscription: not signed in; run node broker/exo-model.mjs login")

    def test_model_records_and_loopback_requirement(self):
        with patch.dict(os.environ, {"EXO_AUTHOR_PROVIDER": "synthetic-loopback"}, clear=True):
            model, reason = model_from_environment()
            self.assertIsNone(model)
            self.assertIn("requires", reason)
        with patch.dict(os.environ, {"EXO_AUTHOR_PROVIDER": "synthetic-loopback",
                                     "EXO_CODEX_BASE_URL": "http://127.0.0.1:46140/backend-api",
                                     "EXO_MODEL_HOME": str(Path(tempfile.mkdtemp(prefix="exo-proto-pybroker-", dir="/tmp")))}, clear=True):
            (Path(os.environ["EXO_MODEL_HOME"]) / "FIXTURE_STORE").write_text("fixture\n")
            model, _ = model_from_environment()
            self.assertEqual((model.provider, model.billing, model.live),
                             ("synthetic-loopback", "none", False))
        with patch.dict(os.environ, {"EXO_AUTHOR_PROVIDER": "codex-subscription"}, clear=True), \
                patch.object(ModelBroker, "ensure_started", return_value={"signed_in": True}):
            model, _ = model_from_environment()
            self.assertEqual((model.provider, model.billing, model.live),
                             ("codex-subscription", "subscription", True))

    def test_default_home_never_spawns_with_override(self):
        with patch.dict(os.environ, {"EXO_CODEX_BASE_URL": "http://127.0.0.1:46140"}, clear=True), \
                patch("model_broker.subprocess.Popen") as spawned, \
                patch.object(ModelBroker, "health") as health:
            with self.assertRaisesRegex(ValueError, "default model home"):
                ModelBroker().ensure_started(reason="test")
        spawned.assert_not_called()
        health.assert_not_called()


class OverrideRefusalTests(unittest.IsolatedAsyncioTestCase):
    async def test_provider_refusals_make_zero_socket_connections(self):
        home = Path(tempfile.mkdtemp(prefix="exo-proto-pybroker-", dir="/tmp")) / "model"
        run = home / "run"
        run.mkdir(parents=True)
        fake = FakeSocketBroker(run / "broker.sock", lambda *args: None)
        await fake.start()
        try:
            url = "http://127.0.0.1:46140/backend-api"
            cases = [
                {"EXO_AUTHOR_PROVIDER": "codex-subscription", "EXO_MODEL_HOME": str(home),
                 "EXO_CODEX_BASE_URL": url},
                {"EXO_AUTHOR_PROVIDER": "synthetic-loopback", "EXO_CODEX_BASE_URL": url},
                {"EXO_AUTHOR_PROVIDER": "synthetic-loopback", "EXO_MODEL_HOME": str(home),
                 "EXO_CODEX_BASE_URL": url},
            ]
            for env in cases:
                with self.subTest(env=sorted(env)), patch.dict(os.environ, env, clear=True):
                    model, reason = model_from_environment()
                    self.assertIsNone(model)
                    self.assertTrue(reason)
            self.assertEqual(fake.requests, [])
        finally:
            await fake.close()


if __name__ == "__main__":
    unittest.main()
