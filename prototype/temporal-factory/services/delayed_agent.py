"""Independent, durable A2A counter-evidence fixture with delayed completion.

The A2A surface and /contract are the only product-facing interfaces. Routes
under /_test/ are fault and observation controls for qualification trials.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path
from uuid import UUID, uuid4

import uvicorn
from a2a.server.agent_execution import AgentExecutor
from a2a.server.apps import A2AFastAPIApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import TaskStore
from a2a.types import (AgentCapabilities, AgentCard, AgentExtension, AgentSkill,
                       Artifact, DataPart, InvalidParamsError, Part, Task,
                       TaskState, TaskStatus)
from a2a.utils.errors import ServerError
from fastapi import Request
from fastapi.responses import JSONResponse


TOKEN = "Bearer fixture-token"
EXTENSION_URI = "urn:exomachina:a2a-action-contract:v1"
CONTRACT_NAME = "action-idempotent-async@1"
CONTRACT = {
    "name": CONTRACT_NAME,
    "protocol": "a2a/0.3.0",
    "request": {"method": "message/send", "blocking": False,
                "data": {"op": "assign", "fields": ["action_id", "run_id",
                                                    "definition_digest", "brief"]}},
    "response": {"kind": "task", "initial_states": ["submitted", "working"],
                 "metadata": ["action_id", "run_id", "definition_digest",
                              "agent_identity"]},
    "completion": {"method": "tasks/get", "state": "completed",
                   "artifact_count": 1,
                   "data": ["revision", "sha256", "author", "content",
                            "action_id", "run_id", "definition_digest"]},
    "idempotency": {"key": "action_id", "same_payload": "original_task_id",
                    "conflict": "json-rpc-error", "commit_before_response": True},
    "reconcile": "a2a-idempotent-resend",
}


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value: object) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


CONTRACT_DIGEST = digest(CONTRACT)


class Rejected(ValueError):
    pass


def checked_command(value: object) -> dict:
    if not isinstance(value, dict) or set(value) != {
        "op", "action_id", "run_id", "definition_digest", "brief"
    } or value.get("op") != "assign":
        raise Rejected("expected one assign DataPart with the declared fields")
    for field in ("action_id", "run_id", "definition_digest"):
        if not isinstance(value[field], str) or not value[field]:
            raise Rejected("missing " + field)
    if not isinstance(value["brief"], str):
        raise Rejected("missing brief")
    return value


def command_from_params(params) -> dict:
    if not params.configuration or params.configuration.blocking is not False:
        raise Rejected("configuration.blocking must be false")
    data = [part.root.data for part in params.message.parts
            if isinstance(part.root, DataPart)]
    if len(params.message.parts) != 1 or len(data) != 1:
        raise Rejected("exactly one DataPart required")
    return checked_command(data[0])


class Ledger:
    def __init__(self, state: Path, delay_seconds: float, identity_file: Path | None = None):
        if delay_seconds < 0:
            raise ValueError("delay must be nonnegative")
        state.mkdir(parents=True, exist_ok=True)
        self.database = state / "delayed-agent.sqlite3"
        self.delay_seconds = delay_seconds
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS identity (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    id TEXT NOT NULL, incarnation INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS actions (
                    action_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
                    task_id TEXT NOT NULL UNIQUE, context_id TEXT NOT NULL,
                    run_id TEXT NOT NULL, definition_digest TEXT NOT NULL,
                    brief TEXT NOT NULL, accepted_at REAL NOT NULL,
                    complete_at REAL NOT NULL,
                    mismatch_artifact INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS faults (
                    action_id TEXT PRIMARY KEY, drop_response_once INTEGER NOT NULL DEFAULT 0,
                    drop_consumed INTEGER NOT NULL DEFAULT 0,
                    mismatch_artifact INTEGER NOT NULL DEFAULT 0);
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

    def configure_faults(self, *, drop_response_once_for: str | None = None,
                         mismatch_artifact_for: str | None = None) -> None:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if drop_response_once_for:
                db.execute("""INSERT INTO faults (action_id, drop_response_once)
                              VALUES (?, 1) ON CONFLICT(action_id) DO UPDATE SET
                              drop_response_once=1""", (drop_response_once_for,))
            if mismatch_artifact_for:
                db.execute("""INSERT INTO faults (action_id, mismatch_artifact)
                              VALUES (?, 1) ON CONFLICT(action_id) DO UPDATE SET
                              mismatch_artifact=1""", (mismatch_artifact_for,))

    def existing(self, command: dict) -> sqlite3.Row | None:
        with self.connect() as db:
            row = db.execute("SELECT * FROM actions WHERE action_id=?",
                             (command["action_id"],)).fetchone()
        if row and row["fingerprint"] != digest(command):
            raise Rejected("action_id reused with different payload")
        return row

    def accept(self, command: dict, task_id: str, context_id: str) -> tuple[str, bool]:
        checked_command(command)
        fingerprint = digest(command)
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM actions WHERE action_id=?",
                             (command["action_id"],)).fetchone()
            if row:
                if row["fingerprint"] != fingerprint:
                    raise Rejected("action_id reused with different payload")
                return row["task_id"], False
            fault = db.execute("SELECT * FROM faults WHERE action_id=?",
                               (command["action_id"],)).fetchone()
            mismatch = bool(fault and fault["mismatch_artifact"])
            drop = bool(fault and fault["drop_response_once"] and
                        not fault["drop_consumed"])
            accepted_at = time.time()
            db.execute("INSERT INTO actions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                       (command["action_id"], fingerprint, task_id, context_id,
                        command["run_id"], command["definition_digest"],
                        command["brief"], accepted_at,
                        accepted_at + self.delay_seconds, int(mismatch)))
            if drop:
                db.execute("UPDATE faults SET drop_consumed=1 WHERE action_id=?",
                           (command["action_id"],))
        return task_id, drop

    def task(self, task_id: str) -> Task | None:
        with self.connect() as db:
            row = db.execute("SELECT * FROM actions WHERE task_id=?", (task_id,)).fetchone()
        if row is None:
            return None
        metadata = {"action_id": row["action_id"], "run_id": row["run_id"],
                    "definition_digest": row["definition_digest"],
                    "agent_identity": self.identity}
        if time.time() < row["complete_at"]:
            return Task(id=task_id, context_id=row["context_id"],
                        status=TaskStatus(state=TaskState.working), metadata=metadata)
        content = "fixture-result:" + row["brief"]
        sha256 = hashlib.sha256(content.encode("utf-8")).hexdigest()
        artifact = {"revision": "r2", "sha256": sha256, "author": self.identity,
                    "content": content, "action_id": row["action_id"],
                    "run_id": row["run_id"] + "-mismatch" if row["mismatch_artifact"]
                    else row["run_id"],
                    "definition_digest": row["definition_digest"]}
        return Task(id=task_id, context_id=row["context_id"],
                    status=TaskStatus(state=TaskState.completed), metadata=metadata,
                    artifacts=[Artifact(artifact_id=sha256,
                                        parts=[Part(root=DataPart(data=artifact))])])

    def effects(self) -> dict:
        with self.connect() as db:
            actions = db.execute("SELECT action_id FROM actions ORDER BY action_id").fetchall()
        return {"identity": self.identity,
                "effects": {row["action_id"]: 1 for row in actions},
                "total": len(actions)}


class LedgerTaskStore(TaskStore):
    def __init__(self, ledger: Ledger):
        self.ledger = ledger

    async def get(self, task_id, context=None):
        return self.ledger.task(task_id)

    async def save(self, task, context=None):
        if self.ledger.task(task.id) is None:
            raise Rejected("task has no committed action")

    async def delete(self, task_id, context=None):
        raise NotImplementedError("task deletion is outside this contract")


class Executor(AgentExecutor):
    def __init__(self, ledger: Ledger, store: LedgerTaskStore):
        self.ledger = ledger
        self.store = store

    async def execute(self, context, event_queue):
        command = checked_command(next(part.root.data for part in context.message.parts
                                       if isinstance(part.root, DataPart)))
        task_id, drop = self.ledger.accept(command, context.task_id, context.context_id)
        if drop:
            # The SQLite transaction has committed with FULL sync. The client
            # sees an uncertain send and can safely resend the same payload.
            os._exit(23)
        await event_queue.enqueue_event(await self.store.get(task_id))

    async def cancel(self, context, event_queue):
        raise ServerError(error=InvalidParamsError(message="action cannot be cancelled"))


class Handler(DefaultRequestHandler):
    def __init__(self, ledger: Ledger, store: LedgerTaskStore):
        super().__init__(Executor(ledger, store), store)
        self.ledger = ledger
        self._send_lock = asyncio.Lock()

    async def on_message_send(self, params, context=None):
        try:
            command = command_from_params(params)
            async with self._send_lock:
                existing = self.ledger.existing(command)
                if existing:
                    return self.ledger.task(existing["task_id"])
                task_id = str(uuid4())
                context_id = params.message.context_id or str(uuid4())
                accepted_task_id, drop = self.ledger.accept(command, task_id, context_id)
                if accepted_task_id != task_id:
                    return self.ledger.task(accepted_task_id)
                if drop:
                    # The mapping and effect are already committed with FULL sync.
                    os._exit(23)
                return Task(id=task_id, context_id=context_id,
                            status=TaskStatus(state=TaskState.working),
                            metadata={"action_id": command["action_id"],
                                      "run_id": command["run_id"],
                                      "definition_digest": command["definition_digest"],
                                      "agent_identity": self.ledger.identity})
        except Rejected as error:
            raise ServerError(error=InvalidParamsError(message=str(error))) from error


def create_app(state: Path, port: int, *, delay_seconds: float = 15,
               identity_file: Path | None = None,
               drop_response_once_for: str | None = None,
               mismatch_artifact_for: str | None = None):
    ledger = Ledger(state, delay_seconds, identity_file)
    ledger.configure_faults(drop_response_once_for=drop_response_once_for,
                            mismatch_artifact_for=mismatch_artifact_for)
    extension = AgentExtension(uri=EXTENSION_URI, required=True,
                               params={"identity": ledger.identity,
                                       "contract": CONTRACT_NAME,
                                       "contract_digest": CONTRACT_DIGEST})
    card = AgentCard(
        name="Delayed counter evidence", description="Independent delayed A2A fixture",
        url=f"http://127.0.0.1:{port}/", version="1.0.0", protocol_version="0.3.0",
        default_input_modes=["application/json"], default_output_modes=["application/json"],
        capabilities=AgentCapabilities(streaming=False, extensions=[extension]),
        skills=[AgentSkill(id="counter_evidence@1", name="Counter evidence",
                           description="Deterministic counter evidence from a brief",
                           tags=["counter-evidence"])],
        security_schemes={"fixtureBearer": {"type": "http", "scheme": "bearer"}},
        security=[{"fixtureBearer": []}],
    )
    app = A2AFastAPIApplication(card, Handler(ledger, LedgerTaskStore(ledger))).build()

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
                "role": "capability", "a2a_protocol": "0.3.0"}

    @app.get("/contract")
    def contract():
        return CONTRACT

    @app.get("/_test/effects")
    def effects():
        return ledger.effects()

    @app.post("/_test/faults")
    async def faults(request: Request):
        body = await request.json()
        if not isinstance(body, dict) or not set(body).issubset({
            "drop_response_once_for", "mismatch_artifact_for"
        }) or not all(isinstance(value, str) and value for value in body.values()):
            return JSONResponse({"error": "invalid fault controls"}, status_code=400)
        ledger.configure_faults(**body)
        return {"configured": body}

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--delay-seconds", type=float, default=15)
    parser.add_argument("--identity-file", type=Path)
    parser.add_argument("--drop-response-once-for")
    parser.add_argument("--mismatch-artifact-for")
    args = parser.parse_args()
    app = create_app(args.state, args.port, delay_seconds=args.delay_seconds,
                     identity_file=args.identity_file,
                     drop_response_once_for=args.drop_response_once_for,
                     mismatch_artifact_for=args.mismatch_artifact_for)
    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
