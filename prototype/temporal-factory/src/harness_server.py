"""Deterministic Strands+A2A fixture shared by the three runtime countertrials.

This is a real Strands tool loop and A2A v1.0 server with a fixture model. Its
agent roles are ordinary A2A agents: a plain Message whose first text Part is
the brief, optionally followed by input Parts; messageId resend returns the
original Task, and only JSON-RPC plus the Agent Card are served. A result is
one artifact whose single text Part is the work product itself. It does not
claim real model quality.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import uvicorn
from fastapi.responses import JSONResponse
from a2a.server.agent_execution import AgentExecutor
from a2a.types import (AgentCapabilities, AgentCard, AgentSkill, Artifact,
                       InvalidParamsError, Task, TaskStatus)
from strands import Agent, tool
from strands.models import Model
from strands.plugins import Plugin

from google.protobuf.json_format import MessageToDict
import a2a_v1
from supplier_protocol import is_nested_request
from a2a_v1_server import (LegacyRequestHandler, ProjectionTaskStore, agent_message,
                           bearer_security, build_app, data_part, interfaces, part_content,
                           part_data, task_state, text_part)


TOKEN = "Bearer fixture-token"


class Rejected(Exception):
    pass


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


class ToolCallingModelFixture(Model):
    """Exercise the real Strands agent/tool path without an external provider."""

    def update_config(self, **model_config):
        pass

    def get_config(self):
        return {"model_id": "decision-round-fixture", "context_window_limit": 16000}

    async def structured_output(self, *args, **kwargs):
        raise NotImplementedError("not part of this fixture")
        yield  # pragma: no cover

    async def stream(self, messages, tool_specs=None, system_prompt=None, **kwargs):
        last = messages[-1]["content"]
        yield {"messageStart": {"role": "assistant"}}
        if "toolResult" not in last[0]:
            command = json.loads(last[0]["text"])
            yield {"contentBlockStart": {"start": {"toolUse": {
                "toolUseId": "fixture-call", "name": tool_specs[0]["name"],
            }}}}
            yield {"contentBlockDelta": {"delta": {"toolUse": {
                "input": canonical({"command_json": canonical(command)}),
            }}}}
            yield {"contentBlockStop": {}}
            yield {"messageStop": {"stopReason": "tool_use"}}
        else:
            result = last[0]["toolResult"]
            # Strands tool failures may contain plain exception text. Preserve an
            # explicit failure envelope instead of turning it into a JSON parse
            # error in the fixture caller. Internal exception details stay local.
            if result.get("status") == "error":
                value = canonical({"error": "fixture tool execution failed"})
            else:
                block = result["content"][0]
                value = block.get("text")
                if value is None:
                    value = canonical(block["json"])
            yield {"contentBlockStart": {"start": {}}}
            yield {"contentBlockDelta": {"delta": {"text": value}}}
            yield {"contentBlockStop": {}}
            yield {"messageStop": {"stopReason": "end_turn"}}


def request_from_params(params, *, received: bool = False) -> dict:
    """Plain A2A v1 Message: the text brief Part, then input Parts, keyed by messageId.

    ``received`` is the executor's view, where the SDK has already bound the
    new Task id onto the Message.
    """
    message = params.message
    if not message.message_id:
        raise Rejected("messageId is required")
    if message.task_id and not received:
        raise Rejected("this agent does not continue Tasks")
    parts = message.parts
    if not parts or part_content(parts[0]) != "text" or not parts[0].text:
        raise Rejected("the first Part must be the text brief")
    return {"message_id": message.message_id, "brief": parts[0].text,
            "inputs": [a2a_v1.normalize_numbers(MessageToDict(part)) for part in parts[1:]],
            "context_id": message.context_id or ""}


def input_candidate(inputs: list) -> dict | None:
    """The draft received as the first input text Part, named by its own digest."""
    text = next((part.get("text") for part in inputs or []
                 if isinstance(part, dict) and isinstance(part.get("text"), str)), None)
    if text is None:
        return None
    try:
        revision = json.loads(text).get("revision")
    except (ValueError, AttributeError):
        revision = None
    return {"revision": revision, "sha256": hashlib.sha256(text.encode()).hexdigest(),
            "content": text}


class Harness:
    """Deterministic fixture agent ledger.

    It stores only its own Task identities, keyed by the caller's messageId.
    Roles: ``capability`` returns ``fixture-result:<brief>``; ``quality``
    reviews the draft received as an input Part (or, for older fixture
    callers, named by a JSON brief ``{"artifact": {...}}``).
    """

    def __init__(self, state: Path, role: str, *, drop_first_response: bool = False):
        self.state = state
        self.role = role
        self.drop_first_response = drop_first_response
        state.mkdir(parents=True, exist_ok=True)
        self.database = state / "harness.sqlite3"
        with self.connect() as db:
            # Schema 1 stored caller run/action/definition bindings.
            db.execute("DROP TABLE IF EXISTS actions")
            db.execute("DROP TABLE IF EXISTS aliases")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS identity (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    id TEXT NOT NULL, role TEXT NOT NULL, incarnation INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS messages (
                    message_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
                    task_id TEXT NOT NULL UNIQUE, context_id TEXT NOT NULL,
                    artifact TEXT NOT NULL, attempts INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS dropped (singleton INTEGER PRIMARY KEY);
            """)
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM identity WHERE singleton=1").fetchone()
            if row:
                if row["role"] != role:
                    raise Rejected("state role mismatch")
                self.identity = row["id"]
                self.incarnation = row["incarnation"] + 1
                db.execute("UPDATE identity SET incarnation=? WHERE singleton=1", (self.incarnation,))
            else:
                self.identity = str(uuid4())
                self.incarnation = 1
                db.execute("INSERT INTO identity VALUES (1, ?, ?, 1)", (self.identity, role))

    def connect(self):
        db = sqlite3.connect(self.database, timeout=15)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        return db

    def existing(self, message_id: str, fingerprint: str) -> str | None:
        with self.connect() as db:
            row = db.execute("SELECT fingerprint, task_id FROM messages WHERE message_id=?",
                             (message_id,)).fetchone()
            if row is None:
                return None
            if row["fingerprint"] != fingerprint:
                raise Rejected("messageId reused with a different message")
            db.execute("UPDATE messages SET attempts=attempts+1 WHERE message_id=?", (message_id,))
            return row["task_id"]

    def task(self, task_id):
        with self.connect() as db:
            row = db.execute("SELECT * FROM messages WHERE task_id=?", (task_id,)).fetchone()
        if row is None:
            return None
        return json.loads(row["artifact"]), row["context_id"]

    def verdict(self, brief: str, inputs: list) -> dict:
        source = input_candidate(inputs)
        if source is None:
            try:
                source = json.loads(brief).get("artifact")
            except (ValueError, AttributeError):
                source = None
        if not isinstance(source, dict):
            raise Rejected("quality needs a draft input Part")
        valid = (isinstance(source.get("content"), str)
                 and source.get("sha256") == hashlib.sha256(source["content"].encode()).hexdigest())
        return {"accepted": bool(valid), "revision": source.get("revision"),
                "sha256": source.get("sha256"),
                "reason": "fixture digest" if valid else "invalid artifact"}

    def result(self, brief: str, inputs: list | None = None) -> dict:
        """The work product itself, as the text of one artifact Part."""
        if self.role == "capability":
            content, media_type = "fixture-result:" + brief, "text/plain"
        else:
            content, media_type = canonical(self.verdict(brief, inputs or [])), "application/json"
        return {"sha256": hashlib.sha256(content.encode()).hexdigest(), "content": content,
                "media_type": media_type}

    def perform(self, command, task_id, context_id):
        """Tool body: commit the Task for ``command = {message_id, brief, inputs, fingerprint}``."""
        artifact = self.result(command["brief"], command.get("inputs"))
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT task_id FROM messages WHERE message_id=?",
                             (command["message_id"],)).fetchone()
            drop = False
            if row is None:
                db.execute("INSERT INTO messages VALUES (?, ?, ?, ?, ?, 1)",
                           (command["message_id"], command["fingerprint"], task_id,
                            context_id, canonical(artifact)))
                if (self.drop_first_response
                        and db.execute("SELECT 1 FROM dropped").fetchone() is None):
                    db.execute("INSERT INTO dropped VALUES (1)")
                    drop = True
        if drop:
            # Committed with FULL sync; lose only the response by exiting.
            os._exit(23)
        return artifact

    async def invoke(self, command, task_id, context_id):
        agent = Agent(name="Decision round " + self.role, model=ToolCallingModelFixture(),
                      plugins=[HarnessPlugin(self, task_id, context_id)], callback_handler=None)
        result = await agent.invoke_async(canonical(command))
        return json.loads(str(result))


class HarnessPlugin(Plugin):
    name = "exomachina-decision-round"

    def __init__(self, harness, task_id, context_id):
        self.harness = harness
        self.task_id = task_id
        self.context_id = context_id
        super().__init__()

    @tool
    def fixture_command(self, command_json: str) -> str:
        """Perform a deterministic capability or Quality fixture command.

        Args:
            command_json: A JSON command using the fixture contract.
        """
        try:
            return canonical(self.harness.perform(json.loads(command_json), self.task_id, self.context_id))
        except Rejected as error:
            return canonical({"error": str(error)})


class LedgerTaskStore(ProjectionTaskStore):
    def __init__(self, harness):
        self.harness = harness

    async def get(self, task_id, context=None):
        record = self.harness.task(task_id)
        if record is None:
            return None
        artifact, context_id = record
        return Task(id=task_id, context_id=context_id,
                    status=TaskStatus(state=task_state("completed")),
                    artifacts=[Artifact(artifact_id=artifact["sha256"],
                                        parts=[text_part(artifact["content"], artifact.get(
                                            "media_type", "application/json"))])])

    async def save(self, task, context=None):
        if self.harness.task(task.id) is None:
            raise Rejected("task has no committed message")


class HarnessExecutor(AgentExecutor):
    """Executor shared by fixture agents and the factory Director.

    Fixture agents receive a plain text brief. The Director (factory mode)
    exposes ``inspect_bound_run`` and invokes with the caller's messageId.
    """

    def __init__(self, harness, store, *, allow_structured_commands=True):
        self.harness = harness
        self.store = store
        self.allow_structured_commands = allow_structured_commands

    async def execute(self, context, event_queue):
        try:
            if not self.allow_structured_commands and any(
                    part_content(part) != "text" for part in context.message.parts):
                raise Rejected("factory caller messages must contain text parts only")
            if hasattr(self.harness, "inspect_bound_run"):
                # Factory mode. With the nested-supplier entry enabled, the
                # factory acts as an ordinary agent service: a plain A2A
                # Message whose brief Part is its run inputs (A2A decisions 7
                # and 9). No extension, metadata or caller identifier is used.
                received = [a2a_v1.normalize_numbers(MessageToDict(part))
                            for part in context.message.parts]
                legacy_op = (self.harness.config.get("legacy_structured_commands") is True
                             and isinstance(received[:1] and received[0].get("data"), dict)
                             and "op" in received[0]["data"])
                command = next((part_data(part) for part in context.message.parts
                                if part_content(part) == "data"), None)
                if (getattr(self.harness, "nested_supplier_enabled", False) and not legacy_op
                        and is_nested_request(received)):
                    result = self.harness.supply(received, context.task_id,
                                                 context.context_id,
                                                 context.message.message_id)
                elif command is not None:
                    result = await self.harness.invoke(command, context.task_id,
                                                       context.context_id)
                else:
                    brief = next((part.text for part in context.message.parts
                                  if part_content(part) == "text"), None)
                    if brief is None:
                        raise Rejected("text brief required")
                    result = await self.harness.invoke(brief, context.task_id, context.context_id,
                                                       message_id=context.message.message_id)
            else:
                request = request_from_params(SimpleNamespace(message=context.message),
                                              received=True)
                command = {"message_id": request["message_id"], "brief": request["brief"],
                           "inputs": request["inputs"], "fingerprint": fingerprint(request)}
                result = await self.harness.invoke(command, context.task_id, context.context_id)
            if "error" in result:
                await event_queue.enqueue_event(agent_message([data_part(result)]))
            else:
                await event_queue.enqueue_event(await self.store.get(context.task_id))
        except Rejected as error:
            await event_queue.enqueue_event(agent_message([data_part({"error": str(error)})]))

    async def cancel(self, context, event_queue):
        raise Rejected("fixture actions are immediate and cannot be cancelled")


def fingerprint(request: dict) -> str:
    inputs = request.get("inputs")
    return hashlib.sha256(canonical({"brief": request["brief"],
                                     "context_id": request["context_id"],
                                     **({"inputs": inputs} if inputs else {})}).encode()).hexdigest()


class FixtureHandler(LegacyRequestHandler):
    """A resent messageId returns the original Task before any new execution."""

    def __init__(self, harness, store, card):
        super().__init__(HarnessExecutor(harness, store), store, card)
        self.harness = harness
        self._send_lock = asyncio.Lock()

    async def on_message_send(self, params, context=None):
        try:
            request = request_from_params(params)
            async with self._send_lock:
                task_id = self.harness.existing(request["message_id"], fingerprint(request))
                if task_id is not None:
                    return await self.task_store.get(task_id)
                return await super().on_message_send(params, context)
        except Rejected as error:
            raise InvalidParamsError(message=str(error)) from error


# The skill tag that publicly promises a resent messageId returns the original Task.
IDEMPOTENT_RESEND_TAG = "message-id-idempotent"


def fixture_card(name: str, description: str, skill: str, port: int,
                 tags: list[str]) -> AgentCard:
    """A plain A2A v1 card: no extensions; the card itself is the agent's identity."""
    return AgentCard(
        name=name, description=description,
        supported_interfaces=interfaces(f"http://127.0.0.1:{port}/"), version="2.0.0",
        default_input_modes=["text/plain"], default_output_modes=["application/json"],
        capabilities=AgentCapabilities(streaming=False),
        skills=[AgentSkill(id=skill, name=skill, description=description,
                           tags=[*tags, IDEMPOTENT_RESEND_TAG, "get-task"])],
        **bearer_security(),
    )


def fixture_app(harness, card):
    store = LedgerTaskStore(harness)
    app = build_app(card, FixtureHandler(harness, store, card))
    app.state.harness = harness

    @app.middleware("http")
    async def fixture_auth(request, call_next):
        if request.url.path == "/.well-known/agent-card.json":
            return await call_next(request)
        if request.headers.get("authorization") != TOKEN:
            return JSONResponse({"error": "fixture authentication required"}, status_code=401)
        return await call_next(request)

    return app


def create_app(state: Path, role: str, port: int, *, name: str | None = None,
               drop_first_response: bool = False):
    harness = Harness(state, role, drop_first_response=drop_first_response)
    # Distinct services carry distinct card names, so their card-derived
    # identities differ.
    card = fixture_card("Decision round " + (name or role), "Deterministic Strands harness fixture",
                        role, port, ["fixture", role])
    return fixture_app(harness, card)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--role", choices=["capability", "quality"], required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--name", help="service name shown on the Agent Card")
    parser.add_argument("--drop-first-response", action="store_true",
                        help="test fault: lose the first committed response")
    args = parser.parse_args()
    uvicorn.run(create_app(args.state, args.role, args.port, name=args.name,
                           drop_first_response=args.drop_first_response),
                host="127.0.0.1", port=args.port, log_level="warning")
