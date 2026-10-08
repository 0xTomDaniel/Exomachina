"""Loopback-only A2A supplier fixture for fan-out recovery tests.

This service is a deterministic protocol fixture. It is an ordinary A2A agent:
it accepts a plain Message with one text Part (it consumes no upstream
items), returns its result as one artifact whose single text Part is the work
product, returns the original Task when
a ``messageId`` is resent, and serves only JSON-RPC and its Agent Card. It
never sees a caller's run, assignment, attempt or parent bindings. It does not
invoke a model or represent a qualified production supplier.

``--drop-first-response`` is a test fault: the first newly committed Message's
response is lost by exiting the process before it is written.
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
import a2a_extensions as ext  # noqa: E402
from a2a_v1_server import (LegacyRequestHandler, ProjectionTaskStore,  # noqa: E402
                           bearer_security, build_app, data_part, interfaces, part_content,
                           text_part,
                           task_state)


TOKEN = "Bearer fixture-token"
REVISION = "supplier-echo-fixture-test-v2"


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


class Rejected(ValueError):
    """The local fixture received a message it does not accept."""


def request_from_params(params) -> dict[str, str]:
    message = params.message
    if not message.message_id:
        raise Rejected("messageId is required")
    parts = message.parts
    if len(parts) != 1 or part_content(parts[0]) != "text" or not parts[0].text:
        raise Rejected("exactly one text Part is required")
    return {"message_id": message.message_id, "text": parts[0].text,
            "context_id": message.context_id or ""}


class Ledger:
    """Durable fixture identity and its own Task mapping; input text is not stored."""

    def __init__(self, state: Path, *, drop_first_response: bool = False):
        state.mkdir(parents=True, exist_ok=True)
        self.database = state / "supplier-echo.sqlite3"
        self.drop_first_response = drop_first_response
        with self._connection() as db:
            db.execute("DROP TABLE IF EXISTS actions")
            db.execute("DROP TABLE IF EXISTS faults")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS identity (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    identity TEXT NOT NULL, incarnation INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS messages (
                    message_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
                    task_id TEXT NOT NULL UNIQUE, context_id TEXT NOT NULL,
                    input_sha256 TEXT NOT NULL, accepted_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS dropped (singleton INTEGER PRIMARY KEY);
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
            db.execute("COMMIT")

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

    def accept(self, request: dict[str, str]) -> tuple[str, bool, bool]:
        input_sha256 = hashlib.sha256(request["text"].encode("utf-8")).hexdigest()
        fingerprint = digest({"input": input_sha256, "context_id": request["context_id"]})
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM messages WHERE message_id=?",
                             (request["message_id"],)).fetchone()
            if row is not None:
                db.execute("COMMIT")
                if row["fingerprint"] != fingerprint:
                    raise Rejected("messageId reused with a different message")
                return row["task_id"], False, False
            task_id = str(uuid4())
            db.execute("""INSERT INTO messages(message_id,fingerprint,task_id,context_id,
                input_sha256,accepted_at) VALUES (?,?,?,?,?,datetime('now'))""",
                (request["message_id"], fingerprint, task_id,
                 request["context_id"] or str(uuid4()), input_sha256))
            drop = False
            if self.drop_first_response and db.execute("SELECT 1 FROM dropped").fetchone() is None:
                db.execute("INSERT INTO dropped VALUES (1)")
                drop = True
            db.execute("COMMIT")
            return task_id, True, drop

    def task(self, task_id: str) -> Task | None:
        with self._connection() as db:
            row = db.execute("SELECT * FROM messages WHERE task_id=?", (task_id,)).fetchone()
        if row is None:
            return None
        content = canonical({"kind": "supplier_echo_artifact@fixture",
                             "supplier_identity": self.identity,
                             "input_sha256": row["input_sha256"]})
        sha256 = hashlib.sha256(content.encode("utf-8")).hexdigest()
        return Task(id=row["task_id"], context_id=row["context_id"],
                    status=TaskStatus(state=task_state("completed")),
                    metadata={"agent_identity": self.identity},
                    artifacts=[Artifact(artifact_id=sha256,
                                        parts=[text_part(content, "application/json")])])


class LedgerTaskStore(ProjectionTaskStore):
    def __init__(self, ledger: Ledger):
        self.ledger = ledger

    async def get(self, task_id, context=None):
        return self.ledger.task(task_id)

    async def save(self, task, context=None):
        if self.ledger.task(task.id) is None:
            raise Rejected("Task has no committed message")


class Executor(AgentExecutor):
    async def execute(self, context, event_queue):
        raise NotImplementedError("sends are handled by Handler")

    async def cancel(self, context, event_queue):
        raise InvalidParamsError(message="fixture Task cannot be cancelled")


class Handler(LegacyRequestHandler):
    def __init__(self, ledger: Ledger, store: LedgerTaskStore, card: AgentCard):
        super().__init__(Executor(), store, card)
        self.ledger = ledger
        self._send_lock = asyncio.Lock()

    async def on_message_send(self, params, context=None):
        try:
            request = request_from_params(params)
            async with self._send_lock:
                task_id, _created, drop = self.ledger.accept(request)
            if drop:
                # The durable Task is committed before losing the response.
                os._exit(23)
            return self.ledger.task(task_id)
        except Rejected as error:
            raise InvalidParamsError(message=str(error)) from error


def create_app(state: Path, port: int, *, drop_first_response: bool = False):
    ledger = Ledger(state, drop_first_response=drop_first_response)
    store = LedgerTaskStore(ledger)
    card = AgentCard(
        name="Supplier echo fixture", description="Local deterministic A2A supplier fixture",
        supported_interfaces=interfaces(f"http://127.0.0.1:{port}/"), version="2.0.0",
        default_input_modes=["text/plain"], default_output_modes=["application/json"],
        capabilities=AgentCapabilities(streaming=False, extensions=[
            AgentExtension(uri=ext.AGENT_URI, required=False,
                           params={"identity": ledger.identity, "resend": ext.RESEND_RULE})]),
        skills=[AgentSkill(id="supplier_echo@1", name="Supplier echo fixture",
                           description="Returns a synthetic public artifact",
                           tags=["supplier-fixture"])],
        **bearer_security(),
    )
    app = build_app(card, Handler(ledger, store, card))
    app.state.ledger = ledger

    @app.middleware("http")
    async def fixture_auth(request: Request, call_next):
        if request.url.path == "/.well-known/agent-card.json":
            return await call_next(request)
        if request.headers.get("authorization") != TOKEN:
            return JSONResponse({"error": "fixture authentication required"}, status_code=401)
        return await call_next(request)

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--drop-first-response", action="store_true",
                        help="test fault: lose the first committed response")
    args = parser.parse_args()
    uvicorn.run(create_app(args.state, args.port, drop_first_response=args.drop_first_response),
                host="127.0.0.1", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
