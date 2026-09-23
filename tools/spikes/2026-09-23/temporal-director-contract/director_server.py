"""Strands/A2A Director shell for immutable Temporal factory runs.

The shell owns only identity, A2A task aliases, command dedup and local fencing.
Temporal history owns factory execution, acceptance and release state.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import secrets
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import uvicorn
from fastapi.responses import JSONResponse
from a2a.server.apps import A2AFastAPIApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import TaskStore
from a2a.types import AgentCard, AgentCapabilities, AgentSkill, Artifact, DataPart, Message, Part, Task, TaskStatus
from strands import Agent
from temporalio.client import Client
from temporalio.service import RPCError

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "2026-09-22" / "decision-round" / "common"))
from harness_server import (HarnessExecutor, HarnessPlugin, Rejected,
                            ToolCallingModelFixture, canonical)  # noqa: E402
from definition import authorize_run_inputs, digest, validate  # noqa: E402
from factory import FactoryRun  # noqa: E402
from failure_projection import failure_incident, project  # noqa: E402

TOKEN = "Bearer fixture-token"
TOKEN_ACTOR = "fixture-operator"
CURRENT_ACTOR: ContextVar[str | None] = ContextVar("director_authenticated_actor", default=None)


def sync(coro):
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(lambda: asyncio.run(coro)).result()


class Director:
    role = "director"

    def __init__(self, state: Path, catalog: Path, address: str):
        self.state, self.catalog, self.address = state, catalog, address
        state.mkdir(parents=True, exist_ok=True)
        self.database = state / "director.sqlite3"
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS identity (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    id TEXT NOT NULL, token TEXT NOT NULL, incarnation INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY, package_digest TEXT NOT NULL,
                    run_inputs_json TEXT NOT NULL, run_inputs_digest TEXT NOT NULL,
                    authorized_actor TEXT NOT NULL, input_authority_json TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS commands (
                    action_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
                    run_id TEXT NOT NULL, op TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS aliases (
                    task_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, context_id TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS incidents (
                    run_id TEXT PRIMARY KEY, child_id TEXT, package_digest TEXT NOT NULL,
                    failure_class TEXT NOT NULL, timestamp TEXT NOT NULL,
                    authority_conflict INTEGER NOT NULL);
            """)
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM identity WHERE singleton=1").fetchone()
            if row:
                self.identity, self.token = row["id"], row["token"]
                self.incarnation = row["incarnation"] + 1
                db.execute("UPDATE identity SET incarnation=? WHERE singleton=1", (self.incarnation,))
            else:
                self.identity, self.token, self.incarnation = str(uuid4()), secrets.token_hex(24), 1
                db.execute("INSERT INTO identity VALUES (1, ?, ?, 1)",
                           (self.identity, self.token))

    def connect(self):
        db = sqlite3.connect(self.database, timeout=15)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        return db

    def fence(self, db):
        row = db.execute("SELECT incarnation FROM identity WHERE singleton=1").fetchone()
        if row is None or row["incarnation"] != self.incarnation:
            raise Rejected("stale Director owner incarnation")

    def package(self, package_digest: str) -> dict:
        if not isinstance(package_digest, str) or len(package_digest) != 64:
            raise Rejected("invalid package digest")
        path = self.catalog / f"{package_digest}.json"
        if not path.is_file():
            raise Rejected("unpublished package")
        package = json.loads(path.read_text())
        approved = json.loads((self.catalog / "approved_bindings.json").read_text())
        if validate(package, approved) != package_digest:
            raise Rejected("published closure changed")
        return package

    async def client(self):
        return await Client.connect(self.address, namespace="exomachina")

    async def _status(self, run_id: str) -> dict:
        client = await self.client()
        return await client.get_workflow_handle(run_id).query(FactoryRun.status)

    async def _start(self, run_id: str, package_digest: str, package: dict,
                     run_inputs: dict, run_inputs_digest: str,
                     authorized_actor: str, input_authority: dict) -> None:
        client = await self.client()
        handle = client.get_workflow_handle(run_id)
        try:
            status = await handle.query(FactoryRun.status)
            if status["package_digest"] != package_digest:
                raise Rejected("run exists with different closure")
            if status.get("run_inputs_digest") != run_inputs_digest:
                raise Rejected("run exists with different inputs")
            if status.get("authorized_actor") != authorized_actor:
                raise Rejected("run exists with different input actor")
            return
        except RPCError:
            pass
        root = package["root"]
        input_value = {"run": run_id, "definition_digest": digest(root),
            "package_digest": package_digest, "document": root, "package": package,
            "director": {"identity": self.identity, "token": self.token,
                         "epoch": self.incarnation},
            "run_inputs": run_inputs, "run_inputs_digest": run_inputs_digest,
            "authorized_actor": authorized_actor, "input_authority": input_authority,
            "faults": {}, "wait_seconds": 180}
        try:
            await client.start_workflow(FactoryRun.run, input_value, id=run_id,
                task_queue="arbitration-temporal", execution_timeout=timedelta(minutes=10))
        except Exception:
            status = await handle.query(FactoryRun.status)
            if status["package_digest"] != package_digest:
                raise Rejected("concurrent run closure conflict")
            if status.get("run_inputs_digest") != run_inputs_digest:
                raise Rejected("concurrent run inputs conflict")
            if status.get("authorized_actor") != authorized_actor:
                raise Rejected("concurrent run input actor conflict")

    async def _abort(self, run_id: str, action_id: str, revision: str, sha256: str) -> None:
        client = await self.client()
        parent = await client.get_workflow_handle(run_id).query(FactoryRun.status)
        if not parent["child_id"]:
            raise Rejected("parent has no child")
        child = client.get_workflow_handle(parent["child_id"])
        status = await child.query(FactoryRun.status)
        if status["phase"] != "awaiting-director":
            raise Rejected("child has no Director wait")
        if status["current_revision"] != revision or status["current_sha256"] != sha256:
            raise Rejected("stale revision/digest")
        if status["owner_epoch"] < self.incarnation:
            await child.execute_update(FactoryRun.claim_owner,
                {"actor": self.identity, "token": self.token, "epoch": self.incarnation})
        await child.execute_update(FactoryRun.director_command, {
            "command_id": action_id, "action": "abort", "actor": self.identity,
            "token": self.token, "epoch": self.incarnation,
            "run": status["run"], "definition_digest": status["definition_digest"],
            "revision": revision, "sha256": sha256})

    def perform(self, command: dict, task_id: str, context_id: str) -> dict:
        op = command.get("op")
        run_id = command.get("run_id")
        action_id = command.get("action_id")
        if op not in {"start", "inspect", "abort"} or not isinstance(run_id, str) or not run_id:
            raise Rejected("unsupported Director command")
        if op != "inspect" and (not isinstance(action_id, str) or not action_id):
            raise Rejected("mutation requires stable action ID")
        fingerprint = hashlib.sha256(canonical(command).encode()).hexdigest()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self.fence(db)
            existing = db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if op == "start":
                actor = CURRENT_ACTOR.get()
                if actor is None:
                    raise Rejected("authenticated input actor required")
                if set(command) not in ({"op", "run_id", "action_id", "package_digest"},
                                        {"op", "run_id", "action_id", "package_digest", "run_inputs"}):
                    raise Rejected("invalid start command fields")
                package = self.package(command.get("package_digest"))
                package_digest = command["package_digest"]
                try:
                    run_inputs, input_authority = authorize_run_inputs(
                        package["run_inputs"], command.get("run_inputs", {}), actor)
                except ValueError as error:
                    raise Rejected(str(error)) from error
                run_inputs_digest = digest(run_inputs)
                if existing and existing["package_digest"] != package_digest:
                    raise Rejected("run closure conflict")
                if existing and existing["run_inputs_digest"] != run_inputs_digest:
                    raise Rejected("run inputs conflict")
                if existing and existing["authorized_actor"] != actor:
                    raise Rejected("run input actor conflict")
                db.execute("INSERT OR IGNORE INTO runs VALUES (?, ?, ?, ?, ?, ?)",
                           (run_id, package_digest, canonical(run_inputs), run_inputs_digest,
                            actor, canonical(input_authority)))
            elif existing is None:
                raise Rejected("unknown factory run")
            if op != "inspect":
                prior = db.execute("SELECT * FROM commands WHERE action_id=?", (action_id,)).fetchone()
                if prior and (prior["fingerprint"] != fingerprint or prior["run_id"] != run_id):
                    raise Rejected("Director action ID conflict")
                db.execute("INSERT OR IGNORE INTO commands VALUES (?, ?, ?, ?)",
                           (action_id, fingerprint, run_id, op))
            db.execute("INSERT OR IGNORE INTO aliases VALUES (?, ?, ?)",
                       (task_id, run_id, context_id))
        if op == "start":
            sync(self._start(run_id, package_digest, package,
                             run_inputs, run_inputs_digest, actor, input_authority))
        elif op == "abort":
            sync(self._abort(run_id, action_id, command.get("revision"), command.get("sha256")))
        return sync(self._status(run_id))

    def task_binding(self, task_id: str):
        with self.connect() as db:
            self.fence(db)
            row = db.execute("SELECT * FROM aliases WHERE task_id=?", (task_id,)).fetchone()
            return (row["run_id"], row["context_id"]) if row else None

    def run_record(self, run_id: str) -> dict:
        with self.connect() as db:
            row = db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None:
                raise Rejected("unknown run")
            return dict(row)

    def pinned_child_id(self, run_id: str, package_digest: str) -> str | None:
        path = self.catalog / f"{package_digest}.json"
        package = json.loads(path.read_text())
        if digest(package) != package_digest:
            raise Rejected("pinned package changed")
        children = [node["child_digest"] for node in package["root"]["nodes"].values()
                    if node["type"] == "nested_factory"]
        return f"{run_id}:child:{children[0][:12]}" if len(children) == 1 else None

    def record_incident(self, incident: dict) -> dict:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self.fence(db)
            db.execute("INSERT OR IGNORE INTO incidents VALUES (?, ?, ?, ?, ?, ?)",
                       (incident["run_id"], incident["child_id"],
                        incident["package_digest"], incident["failure_class"],
                        incident["timestamp"], int(incident["authority_conflict"])))
            return dict(db.execute("SELECT * FROM incidents WHERE run_id=?",
                                   (incident["run_id"],)).fetchone())

    async def invoke(self, command, task_id, context_id):
        agent = Agent(name="Temporal factory Director", model=ToolCallingModelFixture(),
                      plugins=[HarnessPlugin(self, task_id, context_id)], callback_handler=None)
        result = await agent.invoke_async(canonical(command))
        return json.loads(str(result))


class DirectorTaskStore(TaskStore):
    def __init__(self, director: Director):
        self.director = director

    async def get(self, task_id, context=None):
        binding = self.director.task_binding(task_id)
        if not binding:
            return None
        run_id, context_id = binding
        run_record = self.director.run_record(run_id)
        client = await self.director.client()
        handle = client.get_workflow_handle(run_id)
        description = await handle.describe()
        execution = description.status.name
        try:
            status = await handle.query(FactoryRun.status)
        except Exception:
            status = None
        payload = None
        result = None
        if execution == "COMPLETED":
            result = await handle.result()
        state = project(status, execution, result)
        incident = None
        if state == "failed":
            child_id = ((status or {}).get("child_id") or
                        self.director.pinned_child_id(run_id, run_record["package_digest"]))
            child_status = None
            if child_id:
                try:
                    child_status = await client.get_workflow_handle(child_id).query(FactoryRun.status)
                except Exception:
                    pass
            acceptance = ((status or {}).get("authoritative_acceptance") or
                          (child_status or {}).get("authoritative_acceptance"))
            receipt = ((status or {}).get("release_receipt") or
                       (child_status or {}).get("release_receipt"))
            if status and status.get("incident"):
                raw = status["incident"]
                incident = failure_incident(run_id, raw["child_id"] or child_id,
                    run_record["package_digest"], raw["failure_class"], raw["timestamp"],
                    acceptance, receipt)
            else:
                try:
                    await handle.result()
                except Exception as error:
                    failure_class = type(getattr(error, "cause", None) or error).__name__
                else:
                    failure_class = execution
                incident = failure_incident(run_id, child_id,
                    run_record["package_digest"], failure_class,
                    datetime.now(timezone.utc).isoformat(), acceptance, receipt)
            incident = self.director.record_incident(incident)
        if state == "completed":
            payload = {"status": result["status"], "run_id": run_id,
                       "package_digest": run_record["package_digest"], "public_result": result}
        artifacts = None
        if payload:
            artifact_id = hashlib.sha256(canonical(payload).encode()).hexdigest()
            artifacts = [Artifact(artifact_id=artifact_id,
                                  parts=[Part(root=DataPart(data=payload))])]
        message = (Message(message_id=str(uuid4()), role="agent",
                           parts=[Part(root=DataPart(data={"incident": incident}))])
                   if incident else None)
        return Task(id=task_id, context_id=context_id, status=TaskStatus(state=state, message=message),
                    artifacts=artifacts, metadata={"run_id": run_id,
                        "package_digest": run_record["package_digest"],
                        "run_inputs_digest": run_record["run_inputs_digest"],
                        "authorized_input_actor": run_record["authorized_actor"],
                        "harness_identity": self.director.identity,
                        "harness_incarnation": self.director.incarnation,
                        "engine": "temporal"})

    async def save(self, task, context=None):
        if self.director.task_binding(task.id) is None:
            raise Rejected("task has no durable Director binding")

    async def delete(self, task_id, context=None):
        raise NotImplementedError("deletion outside arbitration")


def create_app(state: Path, catalog: Path, address: str, port: int):
    director = Director(state, catalog, address)
    store = DirectorTaskStore(director)
    card = AgentCard(name="Temporal factory Director",
        description="Pinned Temporal factory through Strands/A2A",
        url=f"http://127.0.0.1:{port}/", version="0.0.1", protocol_version="0.3.0",
        default_input_modes=["application/json"], default_output_modes=["application/json"],
        capabilities=AgentCapabilities(streaming=False),
        skills=[AgentSkill(id="factory", name="factory", description="Temporal-backed factory",
                           tags=["factory", "fixture"])],
        security_schemes={"fixtureBearer": {"type": "http", "scheme": "bearer"}},
        security=[{"fixtureBearer": []}])
    app = A2AFastAPIApplication(card, DefaultRequestHandler(HarnessExecutor(director, store), store)).build()

    @app.middleware("http")
    async def fixture_auth(request, call_next):
        if request.url.path in {"/health", "/.well-known/agent-card.json"}:
            return await call_next(request)
        if request.headers.get("authorization") != TOKEN:
            return JSONResponse({"error": "fixture authentication required"}, status_code=401)
        token = CURRENT_ACTOR.set(TOKEN_ACTOR)
        try:
            return await call_next(request)
        finally:
            CURRENT_ACTOR.reset(token)

    @app.get("/health")
    def health():
        return {"identity": director.identity, "incarnation": director.incarnation,
                "role": "director", "a2a_protocol": "0.3.0"}

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--address", required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    uvicorn.run(create_app(args.state, args.catalog, args.address, args.port),
                host="127.0.0.1", port=args.port, log_level="warning")
