"""Bounded Strands Director A2A bridge over the native Dagu factory trial.

This is candidate-owned glue, not a Dagu feature. It uses the same deterministic
Strands tool loop and A2A SDK as the shared capability and Quality harnesses.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from uuid import uuid4

import uvicorn
from fastapi.responses import JSONResponse
from a2a.server.apps import A2AFastAPIApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import TaskStore
from a2a.types import AgentCard, AgentCapabilities, AgentSkill, Artifact, DataPart, Part, Task, TaskStatus
from strands import Agent

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from harness_server import (HarnessExecutor, HarnessPlugin, Rejected, ToolCallingModelFixture,
                            canonical)  # noqa: E402


TOKEN = "Bearer fixture-token"


def json_http(url: str, value: dict | None = None) -> dict:
    payload = None if value is None else canonical(value).encode()
    request = urllib.request.Request(url, data=payload,
        headers={"Content-Type": "application/json"}, method="POST" if payload is not None else "GET")
    with urllib.request.urlopen(request, timeout=8) as response:
        return json.load(response)


class DirectorHarness:
    role = "factory-director"

    def __init__(self, state: Path, runtime: Path, dagu_url: str):
        self.state = state
        self.runtime = runtime
        self.dagu_url = dagu_url.rstrip("/")
        state.mkdir(parents=True, exist_ok=True)
        self.database = state / "director.sqlite3"
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS identity (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    id TEXT NOT NULL, incarnation INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY, version TEXT NOT NULL,
                    closure_sha256 TEXT NOT NULL, start_key TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS commands (
                    key TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
                    run_id TEXT NOT NULL, op TEXT NOT NULL, state TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS aliases (
                    task_id TEXT PRIMARY KEY, run_id TEXT NOT NULL,
                    context_id TEXT NOT NULL);
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
                db.execute("INSERT INTO identity VALUES (1, ?, 1)", (self.identity,))

    def connect(self):
        db = sqlite3.connect(self.database, timeout=15)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        return db

    def fence(self, db: sqlite3.Connection) -> None:
        current = db.execute("SELECT incarnation FROM identity WHERE singleton=1").fetchone()
        if current is None or current["incarnation"] != self.incarnation:
            raise Rejected("stale Director incarnation")

    def manifest(self, version: str) -> dict:
        if version not in {"v1", "v2"}:
            raise Rejected("unsupported factory version")
        path = self.runtime / "home" / "manifests" / f"{version}.json"
        if not path.is_file():
            raise Rejected("unpublished factory version")
        manifest = json.loads(path.read_text())
        expected_closure = hashlib.sha256(canonical(manifest["files"]).encode()).hexdigest()
        if expected_closure != manifest["closure_sha256"]:
            raise Rejected("closure manifest digest changed")
        for name, expected in manifest["files"].items():
            actual = hashlib.sha256((self.runtime / "home" / "dags" / name).read_bytes()).hexdigest()
            if actual != expected:
                raise Rejected("published closure changed")
        return manifest

    def engine(self, run_id: str, version: str) -> dict | None:
        url = f"{self.dagu_url}/api/v1/dag-runs/exo_decision_factory_{version}/{run_id}"
        try:
            return json_http(url)["dagRunDetails"]
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return None
            raise

    def run(self, run_id: str) -> dict:
        with self.connect() as db:
            self.fence(db)
            row = db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            raise Rejected("unknown factory run")
        engine = self.engine(run_id, row["version"])
        return {"run_id": run_id, "version": row["version"],
                "closure_sha256": row["closure_sha256"],
                "engine_status": engine["statusLabel"] if engine else "not_started",
                "nodes": {n["step"]["id"]: n["statusLabel"] for n in engine["nodes"]} if engine else {}}

    def task(self, task_id: str):
        with self.connect() as db:
            self.fence(db)
            row = db.execute("SELECT * FROM aliases WHERE task_id=?", (task_id,)).fetchone()
        return (self.run(row["run_id"]), row["context_id"]) if row else None

    def accepted_artifact(self, run: dict, require_released: bool = True) -> dict:
        ledger = self.runtime / "ledger.sqlite"
        with sqlite3.connect(ledger) as db:
            db.row_factory = sqlite3.Row
            row = db.execute("SELECT * FROM runs WHERE run_id=?", (run["run_id"],)).fetchone()
        if row is None or row["definition_digest"] != run["closure_sha256"]:
            raise Rejected("accepted result has wrong run/definition binding")
        if (not row["accepted_sha256"] or row["accepted_sha256"] != row["current_sha256"]
            or row["accepted_revision"] != row["current_revision"]
            or row["accepted_reviewer"] == row["author"]):
            raise Rejected("factory result is not independently accepted")
        if require_released and row["release_count"] != 1:
            raise Rejected("factory result has not been released")
        artifact = json.loads(row["artifact_json"])
        if artifact.get("sha256") != row["accepted_sha256"] or artifact.get("revision") != row["accepted_revision"]:
            raise Rejected("accepted artifact changed")
        return {"run_id": run["run_id"], "version": run["version"],
                "closure_sha256": run["closure_sha256"], "accepted_artifact": artifact,
                "accepted_revision": row["accepted_revision"],
                "accepted_sha256": row["accepted_sha256"],
                "quality_reviewer": row["accepted_reviewer"]}

    def _bind(self, db, task_id: str, context_id: str, run_id: str) -> None:
        db.execute("INSERT OR IGNORE INTO aliases VALUES (?, ?, ?)", (task_id, run_id, context_id))

    def perform(self, command: dict, task_id: str, context_id: str) -> dict:
        op = command.get("op")
        run_id = command.get("run_id")
        if op not in {"start", "inspect", "decide"} or not isinstance(run_id, str):
            raise Rejected("unsupported Director command")
        if op == "inspect":
            result = self.run(run_id)
            with self.connect() as db:
                self.fence(db)
                self._bind(db, task_id, context_id, run_id)
            return result

        key = command.get("key")
        if not isinstance(key, str) or not key:
            raise Rejected("mutation requires action key")
        fingerprint = hashlib.sha256(canonical(command).encode()).hexdigest()
        new_command = False
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self.fence(db)
            prior = db.execute("SELECT * FROM commands WHERE key=?", (key,)).fetchone()
            if prior and (prior["fingerprint"] != fingerprint or prior["run_id"] != run_id):
                raise Rejected("action key conflict")
            if op == "start":
                version = command.get("version")
                manifest = self.manifest(version)
                run_ids = self.runtime / "run_ids.json"
                expected_run = (json.loads(run_ids.read_text())[version] if run_ids.exists()
                                else f"dagu-product-{version}-001")
                if run_id != expected_run or command.get("closure_sha256") != manifest["closure_sha256"]:
                    raise Rejected("start binding mismatch")
                existing = db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
                if existing and (existing["version"] != version or existing["closure_sha256"] != manifest["closure_sha256"]):
                    raise Rejected("run definition conflict")
                db.execute("INSERT OR IGNORE INTO runs VALUES (?, ?, ?, ?)",
                           (run_id, version, manifest["closure_sha256"], key))
            else:
                existing = db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
                if existing is None:
                    raise Rejected("unknown factory run")
                version = existing["version"]
                if command.get("gate") not in {"publication_gate", "director"}:
                    raise Rejected("unknown Director gate")
            if not prior:
                new_command = True
                db.execute("INSERT INTO commands VALUES (?, ?, ?, ?, 'intent')",
                           (key, fingerprint, run_id, op))
            self._bind(db, task_id, context_id, run_id)

        observed = self.engine(run_id, version)
        if op == "start":
            if observed is None and new_command:
                json_http(f"{self.dagu_url}/api/v1/dags/exo_decision_factory_{version}.yaml/start",
                          {"dagRunId": run_id})
            elif observed is None:
                raise Rejected("start outcome unknown; no blind resubmission")
        else:
            gate = command["gate"]
            nodes = {n["step"]["id"]: n["statusLabel"] for n in observed["nodes"]} if observed else {}
            if gate == "director":
                self.accepted_artifact({"run_id": run_id, "version": version,
                    "closure_sha256": existing["closure_sha256"]}, require_released=False)
            if nodes.get(gate) == "waiting" and new_command:
                json_http(f"{self.dagu_url}/api/v1/dag-runs/exo_decision_factory_{version}/{run_id}"
                          f"/human-tasks/{gate}/complete", {"accepted": True})
            elif nodes.get(gate) != "succeeded":
                raise Rejected("decision outcome unknown or gate not waiting")
        for _ in range(30):
            result = self.run(run_id)
            if result["engine_status"] != "not_started":
                break
            time.sleep(.1)
        with self.connect() as db:
            self.fence(db)
            db.execute("UPDATE commands SET state='observed' WHERE key=?", (key,))
        return result

    async def invoke(self, command: dict, task_id: str, context_id: str) -> dict:
        agent = Agent(name="Dagu factory Director", model=ToolCallingModelFixture(),
                      plugins=[HarnessPlugin(self, task_id, context_id)], callback_handler=None)
        result = await agent.invoke_async(canonical(command))
        return json.loads(str(result))


class DirectorTaskStore(TaskStore):
    def __init__(self, harness: DirectorHarness):
        self.harness = harness

    async def get(self, task_id, context=None):
        record = self.harness.task(task_id)
        if record is None:
            return None
        run, context_id = record
        state = {"waiting": "input-required", "succeeded": "completed", "failed": "failed",
                 "aborted": "failed"}.get(run["engine_status"], "working")
        payload = self.harness.accepted_artifact(run) if state == "completed" else None
        artifacts = ([Artifact(artifact_id=payload["accepted_sha256"],
                               parts=[Part(root=DataPart(data=payload))])]
                     if payload else None)
        return Task(id=task_id, context_id=context_id, status=TaskStatus(state=state),
                    artifacts=artifacts, metadata={"run_id": run["run_id"],
                        "definition_digest": run["closure_sha256"], "version": run["version"],
                        "engine": "dagu", "engine_status": run["engine_status"],
                        "harness_identity": self.harness.identity,
                        "harness_incarnation": self.harness.incarnation})

    async def save(self, task, context=None):
        if self.harness.task(task.id) is None:
            raise Rejected("task has no durable Director run binding")

    async def delete(self, task_id, context=None):
        raise NotImplementedError("deletion outside this trial")


def create_app(state: Path, runtime: Path, dagu_url: str, port: int):
    harness = DirectorHarness(state, runtime, dagu_url)
    store = DirectorTaskStore(harness)
    card = AgentCard(name="Dagu factory Director", description="Bounded factory-service Director",
        url=f"http://127.0.0.1:{port}/", version="0.0.1", protocol_version="0.3.0",
        default_input_modes=["application/json"], default_output_modes=["application/json"],
        capabilities=AgentCapabilities(streaming=False),
        skills=[AgentSkill(id="factory", name="factory", description="Dagu-backed factory fixture",
                           tags=["factory", "fixture"])],
        security_schemes={"fixtureBearer": {"type": "http", "scheme": "bearer"}},
        security=[{"fixtureBearer": []}])
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
                "role": harness.role, "dagu_url": dagu_url, "a2a_protocol": "0.3.0"}

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--dagu-url", required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    uvicorn.run(create_app(args.state, args.runtime, args.dagu_url, args.port),
                host="127.0.0.1", port=args.port, log_level="warning")
