"""Loopback-only A2A fixture for explicit nested supplier binding/recovery.

This service is a deterministic protocol fixture. Its digest-pinned contract
declares parent/child assignment echoes and fixture-only action lookup. It does
not invoke a model or represent a qualified production supplier.
"""
from __future__ import annotations

import argparse
import asyncio
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
from typing import Any
from uuid import uuid4

import uvicorn
from a2a.server.agent_execution import AgentExecutor
from a2a.types import (AgentCapabilities, AgentCard, AgentExtension, AgentSkill,
                       Artifact, InvalidParamsError, Task, TaskStatus)
from fastapi import Request
from fastapi.responses import JSONResponse


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import agent_binding  # noqa: E402
import a2a_v1  # noqa: E402
from a2a_v1_server import (LegacyRequestHandler, ProjectionTaskStore,  # noqa: E402
                           bearer_security, build_app, data_part, interfaces, part_content,
                           part_data, task_state)


TOKEN = "Bearer fixture-token"
EXTENSION_URI = agent_binding.EXTENSION_URI
CONTRACT_NAME = agent_binding.CONTRACT
REVISION = "supplier-echo-fixture-test-v1"
PARENT_FIELDS = ("parent_task_id", "parent_run_id", "parent_definition_digest",
                 "parent_assignment_id", "parent_attempt_id")
CHILD_FIELDS = ("action_id", "run_id", "definition_digest", "assignment_id", "attempt_id")
ECHO_FIELDS = sorted((*PARENT_FIELDS, *CHILD_FIELDS))


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


CONTRACT = {
    "name": CONTRACT_NAME,
    "protocol": a2a_v1.PROTOCOL,
    "request": {
        "method": a2a_v1.SEND_MESSAGE, "returnImmediately": True,
        "data": {"op": "nested_factory", "fields": sorted((
            "action_id", "run_id", "definition_digest", "parent_task_id",
            "parent_run_id", "parent_definition_digest", "parent_assignment_id",
            "parent_attempt_id", "assignment_id", "attempt_id", "payload"))},
    },
    "response": {
        "result": "task", "metadata": sorted((
            "action_id", "run_id", "definition_digest", "agent_identity",
            *PARENT_FIELDS, "assignment_id", "attempt_id")),
    },
    "completion": {
        "method": a2a_v1.GET_TASK, "state": a2a_v1.wire_state("completed"),
        "artifact_count": 1,
        "data": sorted(("revision", "sha256", "author", "content",
                         "action_id", "run_id", "definition_digest",
                         *PARENT_FIELDS, "assignment_id", "attempt_id")),
    },
    "supplier_assignment_echo": {"version": 1, "fields": ECHO_FIELDS},
    "action_lookup": {
        "qualification": "fixture-only",
        "method": "GET", "path": "/fixture/actions/{action_id}",
        "returns": "original_remote_task_id",
    },
    "reconcile": "fixture-lookup",
}
CONTRACT_DIGEST = digest(CONTRACT)


class Rejected(ValueError):
    """The local fixture received a command outside its pinned contract."""


def checked_command(value: object) -> dict[str, Any]:
    required = {"op", "action_id", "run_id", "definition_digest",
                *PARENT_FIELDS, "assignment_id", "attempt_id", "payload"}
    if not isinstance(value, dict) or set(value) != required:
        raise Rejected("nested supplier command fields differ from the pinned contract")
    if value.get("op") != "nested_factory":
        raise Rejected("expected nested_factory operation")
    for name in (*PARENT_FIELDS, *CHILD_FIELDS):
        if not isinstance(value.get(name), str) or not value[name]:
            raise Rejected("missing explicit binding field: " + name)
    if not isinstance(value["payload"], dict):
        raise Rejected("payload must be an object")
    try:
        canonical(value)
    except (TypeError, ValueError) as error:
        raise Rejected("command must be JSON-compatible") from error
    return value


def command_from_params(params) -> dict[str, Any]:
    if not params.HasField("configuration") or params.configuration.return_immediately is not True:
        raise Rejected("configuration.returnImmediately must be true")
    data = [part_data(part) for part in params.message.parts
            if part_content(part) == "data"]
    if len(params.message.parts) != 1 or len(data) != 1:
        raise Rejected("exactly one data Part is required")
    return checked_command(data[0])


class Ledger:
    """Durable fixture identity and original Task mapping; payload is not stored."""

    def __init__(self, state: Path):
        state.mkdir(parents=True, exist_ok=True)
        self.database = state / "supplier-echo.sqlite3"
        with self._connection() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS identity (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    identity TEXT NOT NULL, incarnation INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS actions (
                    action_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
                    task_id TEXT NOT NULL UNIQUE, context_id TEXT NOT NULL,
                    parent_task_id TEXT NOT NULL, parent_run_id TEXT NOT NULL,
                    parent_definition_digest TEXT NOT NULL,
                    parent_assignment_id TEXT NOT NULL, parent_attempt_id TEXT NOT NULL,
                    run_id TEXT NOT NULL, definition_digest TEXT NOT NULL,
                    assignment_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
                    accepted_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS faults (
                    action_id TEXT PRIMARY KEY,
                    drop_response_once INTEGER NOT NULL DEFAULT 0,
                    drop_consumed INTEGER NOT NULL DEFAULT 0);
            """)
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT identity,incarnation FROM identity WHERE singleton=1").fetchone()
            if row is None:
                self.identity = str(uuid4())
                self.incarnation = 1
                db.execute("INSERT INTO identity VALUES (1,?,1)", (self.identity,))
            else:
                self.identity = row["identity"]
                self.incarnation = int(row["incarnation"]) + 1
                db.execute("UPDATE identity SET incarnation=? WHERE singleton=1",
                           (self.incarnation,))
            db.commit()
        self._send_lock = asyncio.Lock()

    @contextmanager
    def _connection(self):
        db = sqlite3.connect(self.database, timeout=15, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        try:
            yield db
        finally:
            db.close()

    def configure_drop_once(self, action_id: str) -> None:
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("""INSERT INTO faults(action_id,drop_response_once,drop_consumed)
                VALUES (?,1,0) ON CONFLICT(action_id) DO UPDATE SET
                drop_response_once=1,drop_consumed=0""", (action_id,))
            db.commit()

    def accept(self, command: dict[str, Any], task_id: str,
               context_id: str) -> tuple[str, bool, bool]:
        checked_command(command)
        fingerprint = digest(command)
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM actions WHERE action_id=?",
                             (command["action_id"],)).fetchone()
            if row is not None:
                if row["fingerprint"] != fingerprint:
                    raise Rejected("action_id reused with different supplier assignment")
                db.commit()
                return row["task_id"], False, False
            fault = db.execute("SELECT * FROM faults WHERE action_id=?",
                               (command["action_id"],)).fetchone()
            drop = bool(fault and fault["drop_response_once"] and not fault["drop_consumed"])
            db.execute("""INSERT INTO actions(action_id,fingerprint,task_id,context_id,
                parent_task_id,parent_run_id,parent_definition_digest,parent_assignment_id,
                parent_attempt_id,run_id,definition_digest,assignment_id,attempt_id,accepted_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))""",
                (command["action_id"], fingerprint, task_id, context_id,
                 command["parent_task_id"], command["parent_run_id"],
                 command["parent_definition_digest"], command["parent_assignment_id"],
                 command["parent_attempt_id"], command["run_id"],
                 command["definition_digest"], command["assignment_id"],
                 command["attempt_id"]))
            if drop:
                db.execute("UPDATE faults SET drop_consumed=1 WHERE action_id=?",
                           (command["action_id"],))
            db.commit()
            return task_id, True, drop

    def _by_task(self, task_id: str):
        with self._connection() as db:
            return db.execute("SELECT * FROM actions WHERE task_id=?", (task_id,)).fetchone()

    def action(self, action_id: str) -> dict[str, Any] | None:
        with self._connection() as db:
            row = db.execute("SELECT * FROM actions WHERE action_id=?", (action_id,)).fetchone()
        if row is None:
            return None
        return {
            "action_id": row["action_id"], "run_id": row["run_id"],
            "definition_digest": row["definition_digest"],
            "parent_task_id": row["parent_task_id"], "parent_run_id": row["parent_run_id"],
            "parent_definition_digest": row["parent_definition_digest"],
            "parent_assignment_id": row["parent_assignment_id"],
            "parent_attempt_id": row["parent_attempt_id"],
            "assignment_id": row["assignment_id"], "attempt_id": row["attempt_id"],
            "task_id": row["task_id"], "harness_identity": self.identity,
            "harness_role": "nested-supplier-echo-fixture",
        }

    def task(self, task_id: str) -> Task | None:
        row = self._by_task(task_id)
        if row is None:
            return None
        bindings = {name: row[name] for name in ECHO_FIELDS}
        metadata = {**bindings, "agent_identity": self.identity}
        content = canonical({"kind": "supplier_echo_artifact@fixture",
                            "supplier_identity": self.identity,
                            "assignment_id": row["assignment_id"],
                            "attempt_id": row["attempt_id"]})
        sha256 = hashlib.sha256(content.encode("utf-8")).hexdigest()
        artifact = {**bindings, "revision": REVISION, "sha256": sha256,
                    "author": self.identity, "content": content}
        return Task(id=row["task_id"], context_id=row["context_id"],
                    status=TaskStatus(state=task_state("completed")), metadata=metadata,
                    artifacts=[Artifact(artifact_id=sha256,
                        parts=[data_part(artifact)])])

    def counts(self) -> dict[str, Any]:
        with self._connection() as db:
            rows = db.execute("SELECT action_id,task_id FROM actions ORDER BY action_id").fetchall()
        return {"identity": self.identity, "incarnation": self.incarnation,
                "count": len(rows), "actions": [dict(row) for row in rows]}


class LedgerTaskStore(ProjectionTaskStore):
    def __init__(self, ledger: Ledger):
        self.ledger = ledger

    async def get(self, task_id, context=None):
        return self.ledger.task(task_id)

    async def save(self, task, context=None):
        if self.ledger.task(task.id) is None:
            raise Rejected("Task has no committed supplier action")


class Executor(AgentExecutor):
    def __init__(self, ledger: Ledger, store: LedgerTaskStore):
        self.ledger = ledger
        self.store = store

    async def execute(self, context, event_queue):
        command = checked_command(next(part_data(part) for part in context.message.parts
                                       if part_content(part) == "data"))
        task_id, _created, drop = self.ledger.accept(
            command, context.task_id, context.context_id)
        if drop:
            # The durable task/effect is committed before losing the response.
            os._exit(23)
        await event_queue.enqueue_event(await self.store.get(task_id))

    async def cancel(self, context, event_queue):
        raise InvalidParamsError(message="fixture Task cannot be cancelled")


class Handler(LegacyRequestHandler):
    def __init__(self, ledger: Ledger, store: LedgerTaskStore, card: AgentCard):
        super().__init__(Executor(ledger, store), store, card)
        self.ledger = ledger
        self._send_lock = asyncio.Lock()

    async def on_message_send(self, params, context=None):
        try:
            command = command_from_params(params)
            async with self._send_lock:
                existing = self.ledger.action(command["action_id"])
                if existing is not None:
                    # This fixture can recover an already accepted task; it
                    # does not advertise generic providers as resend-capable.
                    prior_task = self.ledger.task(existing["task_id"])
                    if self.ledger.accept(command, existing["task_id"],
                                          prior_task.context_id)[1]:
                        raise RuntimeError("existing supplier Task changed unexpectedly")
                    return prior_task
                task_id = str(uuid4())
                context_id = params.message.context_id or str(uuid4())
                accepted_id, created, drop = self.ledger.accept(command, task_id, context_id)
                if not created or accepted_id != task_id:
                    return self.ledger.task(accepted_id)
                if drop:
                    os._exit(23)
                return self.ledger.task(task_id)
        except Rejected as error:
            raise InvalidParamsError(message=str(error)) from error


def create_app(state: Path, port: int):
    ledger = Ledger(state)
    store = LedgerTaskStore(ledger)
    extension = AgentExtension(uri=EXTENSION_URI, required=True,
        params={"identity": ledger.identity, "contract": CONTRACT_NAME,
                "contract_digest": CONTRACT_DIGEST})
    card = AgentCard(
        name="Supplier echo fixture", description="Local deterministic nested A2A supplier fixture",
        supported_interfaces=interfaces(f"http://127.0.0.1:{port}/"), version="1.0.0",
        default_input_modes=["application/json"], default_output_modes=["application/json"],
        capabilities=AgentCapabilities(streaming=False, extensions=[extension]),
        skills=[AgentSkill(id="nested_factory@1", name="Nested factory fixture",
                           description="Returns a synthetic public artifact",
                           tags=["supplier-fixture", "nested-factory"])],
        **bearer_security(),
    )
    app = build_app(card, Handler(ledger, store, card))

    @app.middleware("http")
    async def fixture_auth(request: Request, call_next):
        if request.url.path in {"/health", "/.well-known/agent-card.json"}:
            return await call_next(request)
        if request.headers.get("authorization") != TOKEN:
            return JSONResponse({"error": "fixture authentication required"}, status_code=401)
        return await call_next(request)

    @app.get("/health")
    def health():
        return {"identity": ledger.identity, "incarnation": ledger.incarnation,
                "role": "nested-supplier-echo-fixture",
                "a2a_protocol": a2a_v1.PROTOCOL_VERSION}

    @app.get("/contract")
    def contract():
        return CONTRACT

    @app.get("/fixture/actions/{action_id}")
    def action(action_id: str):
        record = ledger.action(action_id)
        if record is None:
            return JSONResponse({"error": "unknown fixture action"}, status_code=404)
        return record

    @app.get("/_test/effects")
    def effects():
        return ledger.counts()

    @app.post("/_test/faults")
    async def faults(request: Request):
        value = await request.json()
        if not isinstance(value, dict) or set(value) != {"drop_response_once_for"}:
            return JSONResponse({"error": "invalid fixture fault"}, status_code=400)
        action_id = value["drop_response_once_for"]
        if not isinstance(action_id, str) or not action_id:
            return JSONResponse({"error": "action ID must be non-empty"}, status_code=400)
        ledger.configure_drop_once(action_id)
        return {"configured": True}

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--port", required=True, type=int)
    args = parser.parse_args()
    uvicorn.run(create_app(args.state, args.port), host="127.0.0.1",
                port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
