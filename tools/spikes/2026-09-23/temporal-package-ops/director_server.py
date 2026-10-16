"""Strands/A2A Director shell for immutable Temporal factory runs.

The shell owns only identity, A2A task aliases, command dedup and local fencing.
Temporal history owns factory execution, acceptance and release state.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import secrets
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import uvicorn
from fastapi.responses import JSONResponse
from a2a.server.apps import A2AFastAPIApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import TaskStore
from a2a.types import AgentCard, AgentCapabilities, AgentSkill, Artifact, DataPart, Part, Task, TaskStatus
from strands import Agent
from temporalio.client import Client
from temporalio.service import RPCError

sys.path.insert(0, str(Path(__file__).resolve().parent / "prior_common"))
from harness_server import (HarnessExecutor, HarnessPlugin, Rejected,
                            ToolCallingModelFixture, canonical)  # noqa: E402
from definition import digest, validate  # noqa: E402
from factory import FactoryRun  # noqa: E402

TOKEN = "Bearer " + os.environ["EXO_DIRECTOR_BEARER"]


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
                    run_id TEXT PRIMARY KEY, package_digest TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS commands (
                    action_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
                    run_id TEXT NOT NULL, op TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS aliases (
                    task_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, context_id TEXT NOT NULL);
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

    def trial_faults(self, run_id: str, profile: str | None) -> dict:
        if profile is None:
            return {}
        raise Rejected("trial fault profiles are unavailable in this package")

    async def _start(self, run_id: str, package_digest: str, package: dict,
                     trial_fault_profile: str | None) -> None:
        client = await self.client()
        handle = client.get_workflow_handle(run_id)
        try:
            status = await handle.query(FactoryRun.status)
            if status["package_digest"] != package_digest:
                raise Rejected("run exists with different closure")
            return
        except RPCError:
            pass
        root = package["root"]
        input_value = {"run": run_id, "definition_digest": digest(root),
            "package_digest": package_digest, "document": root, "package": package,
            "director": {"identity": self.identity, "token": self.token,
                         "epoch": self.incarnation},
            "faults": self.trial_faults(run_id, trial_fault_profile), "wait_seconds": 180}
        try:
            await client.start_workflow(FactoryRun.run, input_value, id=run_id,
                task_queue="arbitration-temporal", execution_timeout=timedelta(minutes=10))
        except Exception:
            status = await handle.query(FactoryRun.status)
            if status["package_digest"] != package_digest:
                raise Rejected("concurrent run closure conflict")

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
                package = self.package(command.get("package_digest"))
                package_digest = command["package_digest"]
                if existing and existing["package_digest"] != package_digest:
                    raise Rejected("run closure conflict")
                db.execute("INSERT OR IGNORE INTO runs VALUES (?, ?)", (run_id, package_digest))
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
                             command.get("trial_fault_profile")))
        elif op == "abort":
            sync(self._abort(run_id, action_id, command.get("revision"), command.get("sha256")))
        return sync(self._status(run_id))

    def task_binding(self, task_id: str):
        with self.connect() as db:
            self.fence(db)
            row = db.execute("SELECT * FROM aliases WHERE task_id=?", (task_id,)).fetchone()
            return (row["run_id"], row["context_id"]) if row else None

    async def invoke(self, command, task_id, context_id):
        agent = Agent(name="Temporal factory Director", model=ToolCallingModelFixture(),
                      plugins=[HarnessPlugin(self, task_id, context_id)], callback_handler=None)
        result = await agent.invoke_async(canonical(command))
        content = str(result).strip()
        if content:
            return json.loads(content)
        # The tool has durably recorded the task alias even when the fixture
        # model produces an empty final text block under a minimal environment.
        binding = self.task_binding(task_id)
        if binding is None:
            raise Rejected("Director tool did not commit the task")
        return sync(self._status(binding[0]))


class DirectorTaskStore(TaskStore):
    def __init__(self, director: Director):
        self.director = director

    async def get(self, task_id, context=None):
        binding = self.director.task_binding(task_id)
        if not binding:
            return None
        run_id, context_id = binding
        client = await self.director.client()
        handle = client.get_workflow_handle(run_id)
        status = await handle.query(FactoryRun.status)
        state = "input-required" if status["phase"] in {"awaiting-child", "awaiting-director"} else "working"
        payload = None
        if status["phase"] in {"accepted", "child-aborted", "child-expired"}:
            result = await handle.result()
            state = "completed"
            payload = {"status": result["status"], "run_id": run_id,
                       "package_digest": status["package_digest"], "public_result": result}
        artifacts = None
        if payload:
            artifact_id = hashlib.sha256(canonical(payload).encode()).hexdigest()
            artifacts = [Artifact(artifact_id=artifact_id,
                                  parts=[Part(root=DataPart(data=payload))])]
        return Task(id=task_id, context_id=context_id, status=TaskStatus(state=state),
                    artifacts=artifacts, metadata={"run_id": run_id,
                        "package_digest": status["package_digest"],
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
        return await call_next(request)

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
