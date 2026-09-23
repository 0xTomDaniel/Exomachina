"""Strands/A2A Director facade for the pinned Dagu parent factory."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
from uuid import uuid4

import uvicorn
from fastapi.responses import JSONResponse
from a2a.server.apps import A2AFastAPIApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import TaskStore
from a2a.types import AgentCard, AgentCapabilities, AgentSkill, Artifact, DataPart, Part, Task, TaskStatus
from strands import Agent

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "2026-09-22" / "decision-round" / "common"))
import harness_server as prior  # noqa: E402
import ops  # noqa: E402


class Director:
    role = "factory-director"

    def __init__(self, state: Path):
        state.mkdir(parents=True, exist_ok=True)
        self.database = state / "director.sqlite3"
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS identity (
                  singleton INTEGER PRIMARY KEY CHECK(singleton=1), id TEXT NOT NULL,
                  incarnation INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS aliases (
                  task_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, context_id TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS actions (
                  key TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, run_id TEXT NOT NULL,
                  operation TEXT NOT NULL);
            """)
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM identity WHERE singleton=1").fetchone()
            if row:
                self.identity = row["id"]
                self.incarnation = row["incarnation"] + 1
                db.execute("UPDATE identity SET incarnation=? WHERE singleton=1", (self.incarnation,))
            else:
                self.identity = str(uuid4())
                self.incarnation = 1
                db.execute("INSERT INTO identity VALUES (1,?,1)", (self.identity,))

    def connect(self):
        db = sqlite3.connect(self.database, timeout=20)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        return db

    def fence(self, db):
        row = db.execute("SELECT incarnation FROM identity WHERE singleton=1").fetchone()
        if row["incarnation"] != self.incarnation:
            raise prior.Rejected("stale Director incarnation")

    def alias(self, task_id: str):
        with self.connect() as db:
            self.fence(db)
            row = db.execute("SELECT * FROM aliases WHERE task_id=?", (task_id,)).fetchone()
        return (row["run_id"], row["context_id"]) if row else None

    def projection(self, run_id: str) -> dict:
        run = ops.get_run(run_id)
        native = ops.engine_status(run["dag_name"], run_id)
        child = None
        with ops.db_connect() as db:
            child_row = db.execute("SELECT run_id,dag_name,state FROM runs WHERE parent_id=?",
                                   (run_id,)).fetchone()
        if child_row:
            child = {"run_id": child_row["run_id"], "state": child_row["state"]}
            child_native = ops.engine_status(child_row["dag_name"], child_row["run_id"])
            child["engine_status"] = child_native["statusLabel"] if child_native else "not_started"
        value = {"run_id": run_id, "dag_name": run["dag_name"], "root_name": run["root_name"],
                 "definition_digest": run["definition_digest"], "product_state": run["state"],
                 "engine_status": native["statusLabel"] if native else "not_started", "child": child}
        if run["public_result_json"]:
            value["public_result"] = json.loads(run["public_result_json"])
        return value

    def perform(self, command: dict, task_id: str, context_id: str) -> dict:
        op = command.get("op")
        if op not in {"start", "inspect", "abort"}:
            raise prior.Rejected("unsupported Director command")
        run_id = command.get("run_id")
        if not isinstance(run_id, str) or not run_id.startswith("exo-arb-"):
            raise prior.Rejected("invalid Director run ID")
        key = command.get("key")
        if op in {"start", "abort"} and (not isinstance(key, str) or not key):
            raise prior.Rejected("mutation requires action key")
        fingerprint = hashlib.sha256(prior.canonical(command).encode()).hexdigest()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self.fence(db)
            if key:
                prior_action = db.execute("SELECT * FROM actions WHERE key=?", (key,)).fetchone()
                if prior_action and (prior_action["fingerprint"] != fingerprint or prior_action["run_id"] != run_id):
                    raise prior.Rejected("Director action key conflict")
                db.execute("INSERT OR IGNORE INTO actions VALUES (?,?,?,?)", (key, fingerprint, run_id, op))
            db.execute("INSERT OR IGNORE INTO aliases VALUES (?,?,?)", (task_id, run_id, context_id))
        if op == "start":
            root = command.get("root_name")
            if not isinstance(root, str) or not root.startswith("exo_arb_parent_v"):
                raise prior.Rejected("Director may start only a published parent")
            published = ops.manifest(root)
            if command.get("definition_digest") != published["closure_sha256"]:
                raise prior.Rejected("start closure digest mismatch")
            ops.register_run(run_id, root, root,
                             resolve_input=command.get("resolve_input") is True,
                             release_mode=command.get("release_mode", "participating"))
            native = ops.engine_status(root, run_id)
            if native is None:
                try:
                    ops.http_json(ops.service_url("dagu") + f"/api/v1/dags/{root}.yaml/start",
                                  {"dagRunId": run_id})
                except OSError:
                    if ops.engine_status(root, run_id) is None:
                        raise
        elif op == "abort":
            try:
                ops.director_command(run_id, command.get("revision"), key,
                                     command.get("token"), command.get("owner_epoch"),
                                     command.get("expires_at"))
            except ValueError as error:
                raise prior.Rejected(str(error)) from error
        return self.projection(run_id)

    async def invoke(self, command, task_id, context_id):
        agent = Agent(name="Dagu arbitration Director", model=prior.ToolCallingModelFixture(),
                      plugins=[prior.HarnessPlugin(self, task_id, context_id)], callback_handler=None)
        result = await agent.invoke_async(prior.canonical(command))
        return json.loads(str(result))


class DirectorTaskStore(TaskStore):
    def __init__(self, director: Director):
        self.director = director

    async def get(self, task_id, context=None):
        alias = self.director.alias(task_id)
        if alias is None:
            return None
        run_id, context_id = alias
        try:
            projection = self.director.projection(run_id)
        except ValueError:
            return None
        if projection["engine_status"] == "succeeded" and projection["product_state"] in {"released", "aborted"}:
            state = "completed"
        elif projection["child"] and projection["child"]["engine_status"] == "waiting":
            state = "input-required"
        elif projection["engine_status"] == "waiting":
            state = "input-required"
        elif projection["engine_status"] in {"failed", "aborted"}:
            state = "failed"
        else:
            state = "working"
        payload = projection.get("public_result") if state == "completed" else None
        artifacts = None
        if payload:
            raw = prior.canonical(payload).encode()
            artifacts = [Artifact(artifact_id=hashlib.sha256(raw).hexdigest(),
                                  parts=[Part(root=DataPart(data=payload))])]
        return Task(id=task_id, context_id=context_id, status=TaskStatus(state=state),
                    artifacts=artifacts, metadata={"run_id": run_id,
                      "definition_digest": projection["definition_digest"],
                      "director_identity": self.director.identity,
                      "director_incarnation": self.director.incarnation,
                      "engine": "dagu", "child": projection["child"]})

    async def save(self, task, context=None):
        if self.director.alias(task.id) is None:
            raise prior.Rejected("unknown Director task")

    async def delete(self, task_id, context=None):
        raise NotImplementedError("deletion is outside the trial")


def create_app(state: Path, port: int):
    director = Director(state)
    store = DirectorTaskStore(director)
    card = AgentCard(name="Arbitration Factory Director", description="Pinned Dagu factory facade",
        url=f"http://127.0.0.1:{port}/", version="0.0.1", protocol_version="0.3.0",
        default_input_modes=["application/json"], default_output_modes=["application/json"],
        capabilities=AgentCapabilities(streaming=False),
        skills=[AgentSkill(id="factory", name="Factory Director", description="Start and inspect pinned factories",
                           tags=["fixture", "factory"])],
        security_schemes={"fixtureBearer": {"type": "http", "scheme": "bearer"}},
        security=[{"fixtureBearer": []}])
    app = A2AFastAPIApplication(card, DefaultRequestHandler(prior.HarnessExecutor(director, store), store)).build()

    @app.middleware("http")
    async def auth(request, call_next):
        if request.url.path in {"/health", "/.well-known/agent-card.json"}:
            return await call_next(request)
        if request.headers.get("authorization") != prior.TOKEN:
            return JSONResponse({"error": "fixture authentication required"}, status_code=401)
        return await call_next(request)

    @app.get("/health")
    def health():
        return {"identity": director.identity, "incarnation": director.incarnation,
                "role": "factory-director", "a2a_protocol": "0.3.0"}

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    uvicorn.run(create_app(args.state, args.port), host="127.0.0.1", port=args.port,
                log_level="warning")
