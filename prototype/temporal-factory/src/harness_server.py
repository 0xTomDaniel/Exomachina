"""Deterministic Strands+A2A fixture shared by the three runtime countertrials.

This is a real Strands tool loop and A2A 0.3.0 server, with a fixture model and
fixture-specific action lookup. It does not claim real model quality or general
A2A receiver idempotency.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
from pathlib import Path
from uuid import uuid4

import uvicorn
from fastapi.responses import JSONResponse
from a2a.server.agent_execution import AgentExecutor
from a2a.server.apps import A2AFastAPIApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import TaskStore
from a2a.types import (
    AgentCard, AgentCapabilities, AgentSkill, Artifact, DataPart, Message, Part,
    Task, TaskStatus, TextPart,
)
from strands import Agent, tool
from strands.models import Model
from strands.plugins import Plugin


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
            block = last[0]["toolResult"]["content"][0]
            value = block.get("text")
            if value is None:
                value = canonical(block["json"])
            yield {"contentBlockStart": {"start": {}}}
            yield {"contentBlockDelta": {"delta": {"text": value}}}
            yield {"contentBlockStop": {}}
            yield {"messageStop": {"stopReason": "end_turn"}}


class Harness:
    def __init__(self, state: Path, role: str):
        self.state = state
        self.role = role
        state.mkdir(parents=True, exist_ok=True)
        self.database = state / "harness.sqlite3"
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS identity (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    id TEXT NOT NULL, role TEXT NOT NULL, incarnation INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS actions (
                    action_id TEXT PRIMARY KEY, run_id TEXT NOT NULL,
                    definition_digest TEXT NOT NULL, role TEXT NOT NULL,
                    fingerprint TEXT NOT NULL, task_id TEXT NOT NULL,
                    artifact TEXT NOT NULL, attempts INTEGER NOT NULL,
                    accepted_count INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS aliases (
                    task_id TEXT PRIMARY KEY, action_id TEXT NOT NULL,
                    context_id TEXT NOT NULL);
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

    def action(self, action_id):
        with self.connect() as db:
            row = db.execute("SELECT * FROM actions WHERE action_id=?", (action_id,)).fetchone()
        if row is None:
            return None
        return {
            "action_id": row["action_id"], "run_id": row["run_id"],
            "definition_digest": row["definition_digest"], "role": row["role"],
            "task_id": row["task_id"], "artifact": json.loads(row["artifact"]),
            "attempts": row["attempts"], "accepted_count": row["accepted_count"],
        }

    def task(self, task_id):
        with self.connect() as db:
            row = db.execute("SELECT action_id, context_id FROM aliases WHERE task_id=?", (task_id,)).fetchone()
        if row is None:
            return None
        return self.action(row["action_id"]), row["context_id"]

    def perform(self, command, task_id, context_id):
        expected_op = "assign" if self.role == "capability" else "review"
        if command.get("op") != expected_op:
            raise Rejected("operation not allowed for harness role")
        for field in ("action_id", "run_id", "definition_digest"):
            if not isinstance(command.get(field), str) or not command[field]:
                raise Rejected("missing " + field)
        if expected_op == "assign" and not isinstance(command.get("brief"), str):
            raise Rejected("missing brief")
        if expected_op == "review" and not isinstance(command.get("artifact"), dict):
            raise Rejected("missing artifact")
        stable_command = {key: value for key, value in command.items() if key != "drop_ack"}
        fingerprint = hashlib.sha256(canonical(stable_command).encode()).hexdigest()
        newly_created = False
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM actions WHERE action_id=?", (command["action_id"],)).fetchone()
            if row:
                if row["fingerprint"] != fingerprint:
                    raise Rejected("action ID reused with different payload")
                db.execute("UPDATE actions SET attempts=attempts+1 WHERE action_id=?", (command["action_id"],))
            else:
                newly_created = True
                if expected_op == "assign":
                    content = "fixture-result:" + command["brief"]
                    artifact = {"revision": "r2", "sha256": hashlib.sha256(content.encode()).hexdigest(),
                                "author": self.identity, "content": content}
                else:
                    source = command["artifact"]
                    valid = (
                        isinstance(source.get("content"), str)
                        and source.get("sha256") == hashlib.sha256(source["content"].encode()).hexdigest()
                        and source.get("author") != self.identity
                    )
                    artifact = {"accepted": bool(valid), "revision": source.get("revision"),
                                "sha256": source.get("sha256"), "reviewer": self.identity,
                                "reason": "fixture digest and independent identity" if valid else "invalid artifact"}
                db.execute("INSERT INTO actions VALUES (?, ?, ?, ?, ?, ?, ?, 1, 1)",
                           (command["action_id"], command["run_id"], command["definition_digest"],
                            self.role, fingerprint, task_id, canonical(artifact)))
            db.execute("INSERT OR IGNORE INTO aliases VALUES (?, ?, ?)",
                       (task_id, command["action_id"], context_id))
        # The receiver has committed its action and task mapping. Drop the HTTP
        # response by killing only this fixture process, before A2A can emit it.
        if newly_created and expected_op == "assign" and command.get("drop_ack") is True:
            os._exit(23)
        return self.action(command["action_id"])

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


class LedgerTaskStore(TaskStore):
    def __init__(self, harness):
        self.harness = harness

    async def get(self, task_id, context=None):
        record = self.harness.task(task_id)
        if record is None:
            return None
        action, context_id = record
        artifact = action["artifact"]
        return Task(id=task_id, context_id=context_id, status=TaskStatus(state="completed"),
                    artifacts=[Artifact(artifact_id=artifact["sha256"],
                                        parts=[Part(root=DataPart(data=artifact))])],
                    metadata={"action_id": action["action_id"], "run_id": action["run_id"],
                              "definition_digest": action["definition_digest"],
                              "harness_identity": self.harness.identity,
                              "harness_role": self.harness.role})

    async def save(self, task, context=None):
        if self.harness.task(task.id) is None:
            raise Rejected("task has no committed action")

    async def delete(self, task_id, context=None):
        raise NotImplementedError("deletion is outside this trial")


class HarnessExecutor(AgentExecutor):
    def __init__(self, harness, store):
        self.harness = harness
        self.store = store

    async def execute(self, context, event_queue):
        try:
            command = next((part.root.data for part in context.message.parts
                            if isinstance(part.root, DataPart)), None)
            if command is None:
                brief = next((part.root.text for part in context.message.parts
                              if isinstance(part.root, TextPart)), None)
                if brief is None or not hasattr(self.harness, "inspect_bound_run"):
                    raise Rejected("structured data command required")
                result = await self.harness.invoke(brief, context.task_id, context.context_id,
                                                   message_id=context.message.message_id)
            else:
                result = await self.harness.invoke(command, context.task_id, context.context_id)
            if "error" in result:
                await event_queue.enqueue_event(Message(message_id=str(uuid4()), role="agent",
                    parts=[Part(root=DataPart(data=result))]))
            else:
                await event_queue.enqueue_event(await self.store.get(context.task_id))
        except Rejected as error:
            await event_queue.enqueue_event(Message(message_id=str(uuid4()), role="agent",
                parts=[Part(root=DataPart(data={"error": str(error)}))]))

    async def cancel(self, context, event_queue):
        raise Rejected("fixture actions are immediate and cannot be cancelled")


def create_app(state: Path, role: str, port: int):
    harness = Harness(state, role)
    store = LedgerTaskStore(harness)
    card = AgentCard(
        name="Decision round " + role, description="Deterministic Strands harness fixture",
        url=f"http://127.0.0.1:{port}/", version="0.0.1", protocol_version="0.3.0",
        default_input_modes=["application/json"], default_output_modes=["application/json"],
        capabilities=AgentCapabilities(streaming=False),
        skills=[AgentSkill(id=role, name=role, description="Decision-round fixture role",
                           tags=["fixture", role])],
        security_schemes={"fixtureBearer": {"type": "http", "scheme": "bearer"}},
        security=[{"fixtureBearer": []}],
    )
    app = A2AFastAPIApplication(card, DefaultRequestHandler(HarnessExecutor(harness, store), store)).build()

    @app.middleware("http")
    async def fixture_auth(request, call_next):
        if request.url.path in {"/health", "/.well-known/agent-card.json"}:
            return await call_next(request)
        if request.headers.get("authorization") != TOKEN:
            return JSONResponse({"error": "fixture authentication required"}, status_code=401)
        return await call_next(request)

    @app.get("/health")
    def health():
        return {"identity": harness.identity, "incarnation": harness.incarnation,
                "role": harness.role, "a2a_protocol": "0.3.0"}

    @app.get("/fixture/actions/{action_id}")
    def get_action(action_id: str):
        record = harness.action(action_id)
        if record is None:
            return JSONResponse({"error": "unknown action"}, status_code=404)
        return record

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--role", choices=["capability", "quality"], required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    uvicorn.run(create_app(args.state, args.role, args.port), host="127.0.0.1",
                port=args.port, log_level="warning")
