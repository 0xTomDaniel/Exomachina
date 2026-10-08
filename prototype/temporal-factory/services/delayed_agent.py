"""Independent, durable A2A counter-evidence fixture with delayed completion.

It is an ordinary A2A agent: it accepts a plain Message with one text Part (the
brief), returns the original Task when a ``messageId`` is resent, and serves
only JSON-RPC and its Agent Card. It never sees a caller's run, action or
definition identifiers.

Test faults are process options of this fixture, never wire controls:
``--drop-first-response`` loses the first committed response by exiting, and
``--mismatch-artifact`` serves artifacts whose revision differs from the brief.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from uuid import UUID, uuid4

import uvicorn
from a2a.server.agent_execution import AgentExecutor
from a2a.types import (AgentCapabilities, AgentCard, AgentExtension, AgentSkill,
                       Artifact, InvalidParamsError, Task, TaskStatus)
from fastapi import Request
from fastapi.responses import JSONResponse

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import a2a_extensions as ext  # noqa: E402
from a2a_v1_server import (LegacyRequestHandler, ProjectionTaskStore,  # noqa: E402
                           bearer_security, build_app, data_part, interfaces, part_content,
                           task_state)


TOKEN = "Bearer fixture-token"
EXTENSION_URI = ext.AGENT_URI


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value: object) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


class Rejected(ValueError):
    pass


def request_from_params(params) -> dict:
    message = params.message
    if not message.message_id:
        raise Rejected("messageId is required")
    parts = message.parts
    if len(parts) != 1 or part_content(parts[0]) != "text" or not parts[0].text:
        raise Rejected("exactly one text Part (the brief) is required")
    return {"message_id": message.message_id, "brief": parts[0].text,
            "context_id": message.context_id or ""}


class Ledger:
    def __init__(self, state: Path, delay_seconds: float, identity_file: Path | None = None,
                 *, drop_first_response: bool = False, mismatch_artifact: bool = False):
        if delay_seconds < 0:
            raise ValueError("delay must be nonnegative")
        state.mkdir(parents=True, exist_ok=True)
        self.database = state / "delayed-agent.sqlite3"
        self.delay_seconds = delay_seconds
        self.drop_first_response = drop_first_response
        self.mismatch_artifact = mismatch_artifact
        with self.connect() as db:
            columns = {row["name"] for row in db.execute("PRAGMA table_info(actions)")}
            if columns:
                # Schema 1 stored caller run/action bindings; this fixture
                # keeps only its own Task identities.
                db.execute("DROP TABLE actions")
                db.execute("DROP TABLE IF EXISTS faults")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS identity (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    id TEXT NOT NULL, incarnation INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS messages (
                    message_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
                    task_id TEXT NOT NULL UNIQUE, context_id TEXT NOT NULL,
                    brief TEXT NOT NULL, accepted_at REAL NOT NULL,
                    complete_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS dropped (singleton INTEGER PRIMARY KEY);
            """)
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT id, incarnation FROM identity WHERE singleton=1").fetchone()
            if row:
                durable_identity = row["id"]
                self.incarnation = row["incarnation"] + 1
                db.execute("UPDATE identity SET incarnation=? WHERE singleton=1",
                           (self.incarnation,))
            else:
                durable_identity = str(uuid4())
                self.incarnation = 1
                db.execute("INSERT INTO identity VALUES (1, ?, 1)", (durable_identity,))
        self.identity = self._identity_override(identity_file) if identity_file else durable_identity

    @staticmethod
    def _identity_override(path: Path) -> str:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            return str(UUID(path.read_text().strip()))
        identity = str(uuid4())
        with path.open("x") as stream:
            stream.write(identity + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        return identity

    def connect(self):
        db = sqlite3.connect(self.database, timeout=15)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        return db

    def accept(self, request: dict) -> tuple[str, bool, bool]:
        """Return (task_id, created, drop_response)."""
        fingerprint = digest({"brief": request["brief"], "context_id": request["context_id"]})
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM messages WHERE message_id=?",
                             (request["message_id"],)).fetchone()
            if row:
                if row["fingerprint"] != fingerprint:
                    raise Rejected("messageId reused with a different message")
                return row["task_id"], False, False
            task_id = str(uuid4())
            accepted_at = time.time()
            db.execute("INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?, ?)",
                       (request["message_id"], fingerprint, task_id,
                        request["context_id"] or str(uuid4()), request["brief"],
                        accepted_at, accepted_at + self.delay_seconds))
            drop = False
            if self.drop_first_response and db.execute("SELECT 1 FROM dropped").fetchone() is None:
                db.execute("INSERT INTO dropped VALUES (1)")
                drop = True
        return task_id, True, drop

    def task(self, task_id: str) -> Task | None:
        with self.connect() as db:
            row = db.execute("SELECT * FROM messages WHERE task_id=?", (task_id,)).fetchone()
        if row is None:
            return None
        metadata = {"agent_identity": self.identity}
        if time.time() < row["complete_at"]:
            return Task(id=task_id, context_id=row["context_id"],
                        status=TaskStatus(state=task_state("working")), metadata=metadata)
        content = "fixture-result:" + row["brief"]
        sha256 = hashlib.sha256(content.encode("utf-8")).hexdigest()
        artifact = {"revision": "r2-mismatch" if self.mismatch_artifact else "r2",
                    "sha256": sha256, "author": self.identity, "content": content}
        return Task(id=task_id, context_id=row["context_id"],
                    status=TaskStatus(state=task_state("completed")), metadata=metadata,
                    artifacts=[Artifact(artifact_id=sha256,
                                        parts=[data_part(artifact)])])


class LedgerTaskStore(ProjectionTaskStore):
    def __init__(self, ledger: Ledger):
        self.ledger = ledger

    async def get(self, task_id, context=None):
        return self.ledger.task(task_id)

    async def save(self, task, context=None):
        if self.ledger.task(task.id) is None:
            raise Rejected("task has no committed message")


class Executor(AgentExecutor):
    async def execute(self, context, event_queue):
        raise NotImplementedError("sends are handled by Handler")

    async def cancel(self, context, event_queue):
        raise InvalidParamsError(message="this Task cannot be cancelled")


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
                # The Task is already committed with FULL sync. The client sees
                # an uncertain send and can safely resend the same messageId.
                os._exit(23)
            return self.ledger.task(task_id)
        except Rejected as error:
            raise InvalidParamsError(message=str(error)) from error


def create_app(state: Path, port: int, *, delay_seconds: float = 15,
               identity_file: Path | None = None, drop_first_response: bool = False,
               mismatch_artifact: bool = False):
    ledger = Ledger(state, delay_seconds, identity_file,
                    drop_first_response=drop_first_response,
                    mismatch_artifact=mismatch_artifact)
    card = AgentCard(
        name="Delayed counter evidence", description="Independent delayed A2A fixture",
        supported_interfaces=interfaces(f"http://127.0.0.1:{port}/"), version="2.0.0",
        default_input_modes=["text/plain"], default_output_modes=["application/json"],
        capabilities=AgentCapabilities(streaming=False, extensions=[
            AgentExtension(uri=ext.AGENT_URI, required=False,
                           params={"identity": ledger.identity, "resend": ext.RESEND_RULE})]),
        skills=[AgentSkill(id="counter_evidence@1", name="Counter evidence",
                           description="Deterministic counter evidence from a brief",
                           tags=["counter-evidence"])],
        **bearer_security(),
    )
    app = build_app(card, Handler(ledger, LedgerTaskStore(ledger), card))
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
    parser.add_argument("--delay-seconds", type=float, default=15)
    parser.add_argument("--identity-file", type=Path)
    parser.add_argument("--drop-first-response", action="store_true",
                        help="test fault: lose the first committed response")
    parser.add_argument("--mismatch-artifact", action="store_true",
                        help="test fault: serve artifacts with a mismatched revision")
    args = parser.parse_args()
    app = create_app(args.state, args.port, delay_seconds=args.delay_seconds,
                     identity_file=args.identity_file,
                     drop_first_response=args.drop_first_response,
                     mismatch_artifact=args.mismatch_artifact)
    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
