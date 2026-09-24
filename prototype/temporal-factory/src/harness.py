"""Customized Strands harness instance: ordinary agent or factory mode.

In factory mode the Director agent and Factory Module live inside this instance.
Callers see only the instance's normal A2A identity, Agent Card and capability
contract; there is no separate factory endpoint, and callers never name a graph,
package or version. New runs use the instance's active publication; open runs
keep the closure pinned at their start. The shared local runner (Temporal,
PostgreSQL, interpreter workers) is started lazily on first factory work, or at
startup only when this instance has unfinished runs to recover.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import platform
import secrets
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from importlib import metadata
from pathlib import Path
from uuid import uuid4

import uvicorn
from fastapi.responses import JSONResponse
from a2a.server.apps import A2AFastAPIApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import TaskStore
from a2a.types import (AgentCapabilities, AgentCard, AgentSkill, Artifact, DataPart,
                       Message, Part, Task, TaskStatus)
from strands import Agent
from temporalio.client import Client
from temporalio.common import PinnedVersioningOverride, WorkerDeploymentVersion
from temporalio.service import RPCError

SRC = Path(__file__).resolve().parent
sys.path.insert(0, str(SRC))
import harness_server  # noqa: E402
from harness_server import (HarnessExecutor, HarnessPlugin, Rejected,  # noqa: E402
                            ToolCallingModelFixture, canonical)
from binding import (DEPLOYMENT, NAMESPACE, QUEUE, PublicationStore,  # noqa: E402
                     make_manifest, verify_closure)
from definition import authorize_run_inputs, digest, validate  # noqa: E402
from definition import publish as store_package  # noqa: E402
from factory import FactoryRun  # noqa: E402
from failure_projection import failure_incident, project  # noqa: E402
from runner import Runner  # noqa: E402

TOKEN = "Bearer fixture-token"
TOKEN_ACTOR = "fixture-operator"
CURRENT_ACTOR: ContextVar[str | None] = ContextVar("harness_authenticated_actor", default=None)
TERMINAL = {"completed", "failed", "canceled", "rejected"}


def sync(coro):
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(lambda: asyncio.run(coro)).result()


def load_config(instance_dir: Path) -> dict:
    config = json.loads((instance_dir / "instance.json").read_text())
    if config.get("mode") not in {"agent", "factory"}:
        raise ValueError("instance mode must be agent or factory")
    return config


class FactoryModule:
    """Publication, closure pinning and lazy runner access for one instance."""

    def __init__(self, instance_dir: Path, config: dict):
        self.catalog = instance_dir / "catalog"
        self.catalog.mkdir(parents=True, exist_ok=True)
        self.home = Path(config["home"])
        runner_config = config.get("runner", {})
        self.runner = Runner(self.home, port_base=runner_config.get("port_base"),
                             member_base=runner_config.get("member_base"))
        self.publications = PublicationStore(self.catalog)

    def approved(self) -> dict:
        return json.loads((self.catalog / "approved_bindings.json").read_text())

    def publish(self, package: dict, *, label: str, approval: dict,
                interpreter_source: Path = SRC) -> dict:
        """Pin definition, contracts, Quality policy and interpreter build; activate.

        This never starts the runner. Temporal registration of the build is
        verified lazily before the first run that uses it.
        """
        approved = self.approved()
        package_digest = validate(package, approved)
        if approval.get("package_digest") != package_digest or approval.get("status") != "approved":
            raise ValueError("publication requires an approved decision for this exact package")
        if not (self.catalog / f"{package_digest}.json").exists():
            store_package(package, self.catalog, approved)
        contracts = json.loads((self.catalog / "contracts.json").read_text())
        quality_policy = json.loads((self.catalog / "quality_policy.json").read_text())
        build = self.runner.ensure_build(interpreter_source)
        manifest = make_manifest(package, {name: contracts[name] for name in package["bindings"]},
                                 quality_policy, build_id=build["build_id"],
                                 code_digest=build["source_digest"],
                                 python=platform.python_version(),
                                 temporalio=metadata.version("temporalio"))
        closure = {"manifest": manifest, "manifest_digest": digest(manifest),
                   "contracts": {name: contracts[name] for name in package["bindings"]},
                   "quality_policy": quality_policy}
        key = self.publications.publish(package, closure, label=label)
        record = self.publications.activate(
            key, registered_version=f"{DEPLOYMENT}.{build['build_id']}",
            registered_source_digest=build["source_digest"])
        approvals = self.catalog / "approvals.jsonl"
        with approvals.open("a") as stream:
            stream.write(json.dumps({**approval, "manifest_digest": key, "label": label,
                                     "activated_at": time.time()}, sort_keys=True) + "\n")
        return {"manifest_digest": key, "package_digest": package_digest,
                "build_id": record["build_id"], "label": label}

    def package(self, package_digest: str) -> dict:
        package = json.loads((self.catalog / f"{package_digest}.json").read_text())
        if validate(package, self.approved()) != package_digest:
            raise Rejected("published closure changed")
        return package


class Director:
    """The factory-mode agent: owns identity, Task aliases, dedup and fencing."""

    role = "director"

    def __init__(self, instance_dir: Path, config: dict, *, claim: bool = True):
        """claim=False opens the instance for operator tooling without a new incarnation,
        so it never fences the serving harness process."""
        self.config = config
        self.module = FactoryModule(instance_dir, config)
        self.database = instance_dir / "director.sqlite3"
        self.wait_seconds = int(config.get("director_wait_seconds", 900))
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS identity (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    id TEXT NOT NULL, token TEXT NOT NULL, incarnation INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, context_id TEXT NOT NULL,
                    package_digest TEXT NOT NULL, manifest_digest TEXT NOT NULL,
                    build_id TEXT NOT NULL, label TEXT NOT NULL,
                    run_inputs_json TEXT NOT NULL, run_inputs_digest TEXT NOT NULL,
                    authorized_actor TEXT NOT NULL, input_authority_json TEXT NOT NULL,
                    closed INTEGER NOT NULL DEFAULT 0, outcome_json TEXT);
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
                self.incarnation = row["incarnation"] + (1 if claim else 0)
                if claim:
                    db.execute("UPDATE identity SET incarnation=? WHERE singleton=1",
                               (self.incarnation,))
            else:
                self.identity, self.token, self.incarnation = str(uuid4()), secrets.token_hex(24), 1
                db.execute("INSERT INTO identity VALUES (1, ?, ?, 1)", (self.identity, self.token))

    def connect(self):
        db = sqlite3.connect(self.database, timeout=15)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        return db

    def fence(self, db):
        row = db.execute("SELECT incarnation FROM identity WHERE singleton=1").fetchone()
        if row is None or row["incarnation"] != self.incarnation:
            raise Rejected("stale harness incarnation")

    def unfinished(self) -> list[str]:
        with self.connect() as db:
            return [row["run_id"] for row in db.execute("SELECT run_id FROM runs WHERE closed=0")]

    def run_id_for(self, action_id: str) -> str:
        return f"{self.identity}.{hashlib.sha256(action_id.encode()).hexdigest()[:20]}"

    async def client(self):
        return await Client.connect(self.module.runner.address, namespace=NAMESPACE)

    def ensure_runner(self, reason: str, build_id: str | None = None) -> dict:
        ready = self.module.runner.ensure_started(reason=reason)
        if build_id is not None:
            ready = {**ready, "worker": self.module.runner.wait_worker(build_id)}
        return ready

    async def _start(self, run: dict, package: dict, publication: dict) -> None:
        client = await self.client()
        handle = client.get_workflow_handle(run["run_id"])
        try:
            status = await handle.query(FactoryRun.status)
            if (status["package_digest"] != run["package_digest"]
                    or status["manifest_digest"] != run["manifest_digest"]
                    or status.get("run_inputs_digest") != run["run_inputs_digest"]):
                raise Rejected("run exists with a different pinned closure or inputs")
            return
        except RPCError:
            pass
        root = package["root"]
        closure = publication["closure"]
        verify_closure(closure, package, build_id=publication["build_id"],
                       definition_digest=digest(root), document=root)
        value = {"run": run["run_id"], "definition_digest": digest(root),
                 "package_digest": run["package_digest"], "document": root,
                 "package": package, "closure": closure,
                 "director": {"identity": self.identity, "token": self.token,
                              "epoch": self.incarnation},
                 "run_inputs": json.loads(run["run_inputs_json"]),
                 "run_inputs_digest": run["run_inputs_digest"],
                 "authorized_actor": run["authorized_actor"],
                 "input_authority": json.loads(run["input_authority_json"]),
                 "wait_seconds": self.wait_seconds}
        try:
            await client.start_workflow(
                FactoryRun.run, value, id=run["run_id"], task_queue=QUEUE,
                execution_timeout=timedelta(seconds=self.wait_seconds + 600),
                versioning_override=PinnedVersioningOverride(
                    WorkerDeploymentVersion(DEPLOYMENT, publication["build_id"])))
        except Exception:
            status = await handle.query(FactoryRun.status)
            if status["manifest_digest"] != run["manifest_digest"]:
                raise Rejected("concurrent run closure conflict")

    async def _abort(self, run_id: str, action_id: str, revision: str, sha256: str) -> None:
        client = await self.client()
        parent = await client.get_workflow_handle(run_id).query(FactoryRun.status)
        if not parent["child_id"]:
            raise Rejected("run has no nested factory child")
        child = client.get_workflow_handle(parent["child_id"])
        status = await child.query(FactoryRun.status)
        if status["phase"] != "awaiting-director":
            raise Rejected("run has no Director wait")
        if status["current_revision"] != revision or status["current_sha256"] != sha256:
            raise Rejected("stale revision/digest")
        if status["owner_epoch"] < self.incarnation:
            await child.execute_update(FactoryRun.claim_owner, {
                "actor": self.identity, "token": self.token, "epoch": self.incarnation})
        await child.execute_update(FactoryRun.director_command, {
            "command_id": action_id, "action": "abort", "actor": self.identity,
            "token": self.token, "epoch": self.incarnation, "run": status["run"],
            "definition_digest": status["definition_digest"],
            "revision": revision, "sha256": sha256})

    def perform(self, command: dict, task_id: str, context_id: str) -> dict:
        """Capability contract verified-research@1, as seen by A2A callers.

        start:   {op:"start", action_id, inputs:{question?, outcome_mode}}
        inspect: {op:"inspect"} on the original Task
        abort:   {op:"abort", action_id, revision, sha256} on the original Task,
                 answering its input-required Director wait.
        """
        op = command.get("op")
        action_id = command.get("action_id")
        if op not in {"start", "inspect", "abort"}:
            raise Rejected("unsupported command for verified-research@1")
        if op != "inspect" and (not isinstance(action_id, str) or not action_id):
            raise Rejected("mutation requires a stable action_id")
        fingerprint = hashlib.sha256(canonical(command).encode()).hexdigest()
        actor = CURRENT_ACTOR.get()
        if actor is None:
            raise Rejected("authenticated caller required")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self.fence(db)
            alias = db.execute("SELECT * FROM aliases WHERE task_id=?", (task_id,)).fetchone()
            if op == "start":
                if alias is not None and alias["run_id"] != self.run_id_for(action_id):
                    raise Rejected("this Task is already bound to a different factory run")
                if set(command) - {"op", "action_id", "inputs"}:
                    raise Rejected("invalid start fields; callers do not choose graphs or versions")
                run_id = self.run_id_for(action_id)
                existing = db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
                if existing:
                    publication = self.module.publications.get(existing["manifest_digest"])
                else:
                    publication = self.module.publications.active()
                package = self.module.package(publication["package_digest"])
                try:
                    run_inputs, authority = authorize_run_inputs(
                        package["run_inputs"], command.get("inputs", {}), actor)
                except ValueError as error:
                    raise Rejected(str(error)) from error
                inputs_digest = digest(run_inputs)
                if existing and (existing["run_inputs_digest"] != inputs_digest
                                 or existing["authorized_actor"] != actor):
                    raise Rejected("action_id reused with different inputs or actor")
                if existing and existing["task_id"] != task_id:
                    # One result per run, on its original Task: never alias a second Task.
                    raise Rejected("duplicate start; continue original Task " + existing["task_id"])
                db.execute("INSERT OR IGNORE INTO runs (run_id, task_id, context_id, package_digest,"
                           " manifest_digest, build_id, label, run_inputs_json, run_inputs_digest,"
                           " authorized_actor, input_authority_json) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                           (run_id, task_id, context_id, publication["package_digest"],
                            publication["manifest_digest"], publication["build_id"],
                            publication.get("label", ""), canonical(run_inputs), inputs_digest,
                            actor, canonical(authority)))
                run = dict(db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone())
            else:
                if alias is None:
                    raise Rejected("inspect/abort must continue the original factory Task")
                run_id = alias["run_id"]
            if op != "inspect":
                prior = db.execute("SELECT * FROM commands WHERE action_id=?", (action_id,)).fetchone()
                if prior and (prior["fingerprint"] != fingerprint or prior["run_id"] != run_id):
                    raise Rejected("action_id conflict")
                db.execute("INSERT OR IGNORE INTO commands VALUES (?, ?, ?, ?)",
                           (action_id, fingerprint, run_id, op))
            db.execute("INSERT OR IGNORE INTO aliases VALUES (?, ?, ?)", (task_id, run_id, context_id))
        if op == "start":
            self.ensure_runner(f"factory-work:{run_id}", publication["build_id"])
            sync(self._start(run, package, publication))
        elif op == "abort":
            self.ensure_runner(f"director-command:{run_id}")
            sync(self._abort(run_id, action_id, command.get("revision"), command.get("sha256")))
        return {"run_id": run_id, "accepted_command": op}

    def task_binding(self, task_id: str):
        with self.connect() as db:
            row = db.execute("SELECT * FROM aliases WHERE task_id=?", (task_id,)).fetchone()
            return (row["run_id"], row["context_id"]) if row else None

    def run_record(self, run_id: str) -> dict:
        with self.connect() as db:
            row = db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None:
                raise Rejected("unknown run")
            return dict(row)

    def close_run(self, run_id: str, projection: dict) -> None:
        with self.connect() as db:
            db.execute("UPDATE runs SET closed=1, outcome_json=? WHERE run_id=? AND closed=0",
                       (canonical(projection), run_id))

    def record_incident(self, incident: dict) -> dict:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("INSERT OR IGNORE INTO incidents VALUES (?, ?, ?, ?, ?, ?)",
                       (incident["run_id"], incident["child_id"], incident["package_digest"],
                        incident["failure_class"], incident["timestamp"],
                        int(incident["authority_conflict"])))
            return dict(db.execute("SELECT * FROM incidents WHERE run_id=?",
                                   (incident["run_id"],)).fetchone())

    async def invoke(self, command, task_id, context_id):
        agent = Agent(name="Factory Director", model=ToolCallingModelFixture(),
                      plugins=[HarnessPlugin(self, task_id, context_id)], callback_handler=None)
        result = await agent.invoke_async(canonical(command))
        return json.loads(str(result))

    def recover(self) -> dict | None:
        """At startup, start the runner only if this instance has unfinished runs."""
        runs = self.unfinished()
        if not runs:
            return None
        return self.ensure_runner(f"recover-unfinished:{len(runs)}")


class FactoryTaskStore(TaskStore):
    """Projects Temporal state onto the original A2A Task; Temporal stays authoritative."""

    def __init__(self, director: Director):
        self.director = director

    async def get(self, task_id, context=None):
        binding = self.director.task_binding(task_id)
        if not binding:
            return None
        run_id, context_id = binding
        record = self.director.run_record(run_id)
        if record["closed"] and record["outcome_json"]:
            return self._task(task_id, context_id, record, json.loads(record["outcome_json"]))
        if not self.director.module.runner.is_running():
            return self._task(task_id, context_id, record, {"state": "working", "status": None,
                                                              "result": None, "incident": None})
        client = await self.director.client()
        handle = client.get_workflow_handle(run_id)
        description = await handle.describe()
        execution = description.status.name
        try:
            status = await handle.query(FactoryRun.status)
        except Exception:
            status = None
        result = await handle.result() if execution == "COMPLETED" else None
        state = project(status, execution, result)
        if state in TERMINAL and execution == "RUNNING":
            # Phase changes precede Workflow closure; publish only the closed outcome.
            state = "working"
        if state == "input-required" and (status or {}).get("phase") == "awaiting-child":
            # Only a Director wait needs caller input; a running child is still work.
            try:
                child = await client.get_workflow_handle(status["child_id"]).query(FactoryRun.status)
            except Exception:
                child = None
            if (child or {}).get("phase") != "awaiting-director":
                state = "working"
        incident = None
        if state == "failed":
            raw = (status or {}).get("incident") or (result or {}).get("incident") or {}
            failure_class = raw.get("failure_class") or (result or {}).get("kind") or execution
            incident = self.director.record_incident(failure_incident(
                run_id, raw.get("child_id") or (status or {}).get("child_id"),
                record["package_digest"], failure_class,
                raw.get("timestamp") or datetime.now(timezone.utc).isoformat(),
                (status or {}).get("authoritative_acceptance"),
                (status or {}).get("release_receipt")))
        projection = {"state": state, "status": status, "result": result, "incident": incident}
        if state in TERMINAL:
            self.director.close_run(run_id, projection)
        return self._task(task_id, context_id, record, projection)

    def _task(self, task_id: str, context_id: str, record: dict, projection: dict) -> Task:
        state, result, status = projection["state"], projection["result"], projection["status"]
        artifacts = None
        message = None
        if state == "completed" and result:
            payload = {"capability": "verified-research@1", "status": result["status"],
                       "run_id": record["run_id"], "acceptance": result.get("acceptance"),
                       "release_receipt": result.get("receipt"),
                       "report": result.get("artifact"),
                       "interpreter_revision": result.get("interpreter_revision")}
            artifact_id = hashlib.sha256(canonical(payload).encode()).hexdigest()
            artifacts = [Artifact(artifact_id=artifact_id, parts=[Part(root=DataPart(data=payload))])]
        if projection.get("incident"):
            message = Message(message_id=str(uuid4()), role="agent",
                              parts=[Part(root=DataPart(data={"incident": projection["incident"]}))])
        elif state == "input-required" and status:
            wait = {"director_wait": {"phase": status.get("phase"), "child_id": status.get("child_id")}}
            message = Message(message_id=str(uuid4()), role="agent", parts=[Part(root=DataPart(data=wait))])
        return Task(id=task_id, context_id=context_id, status=TaskStatus(state=state, message=message),
                    artifacts=artifacts, metadata={
                        "run_id": record["run_id"], "harness_identity": self.director.identity,
                        "harness_incarnation": self.director.incarnation,
                        "capability": "verified-research@1",
                        "publication_label": record["label"],
                        "manifest_digest": record["manifest_digest"],
                        "package_digest": record["package_digest"],
                        "interpreter_build": record["build_id"],
                        "run_inputs_digest": record["run_inputs_digest"],
                        "authorized_input_actor": record["authorized_actor"]})

    async def save(self, task, context=None):
        if self.director.task_binding(task.id) is None:
            raise Rejected("task has no durable factory binding")

    async def delete(self, task_id, context=None):
        raise NotImplementedError("Task deletion is outside the prototype")


def _auth(app):
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


def create_app(instance_dir: Path):
    config = load_config(instance_dir)
    port = config["port"]
    if config["mode"] == "agent":
        # Ordinary agent mode: the same harness serves one capability directly.
        return harness_server.create_app(instance_dir / "agent-state",
                                         config.get("agent_role", "capability"), port)
    director = Director(instance_dir, config)
    store = FactoryTaskStore(director)
    capability = config["capability"]
    card = AgentCard(
        name=config["name"], description=capability["description"],
        url=f"http://127.0.0.1:{port}/", version="0.1.0", protocol_version="0.3.0",
        default_input_modes=["application/json"], default_output_modes=["application/json"],
        capabilities=AgentCapabilities(streaming=False),
        skills=[AgentSkill(id=capability["id"], name=capability["name"],
                           description=capability["description"], tags=capability.get("tags", []))],
        security_schemes={"fixtureBearer": {"type": "http", "scheme": "bearer"}},
        security=[{"fixtureBearer": []}])
    app = A2AFastAPIApplication(card, DefaultRequestHandler(HarnessExecutor(director, store), store)).build()
    _auth(app)
    startup = {"runner_started_at_startup": False, "recovery": None}

    @app.on_event("startup")
    async def recover_unfinished():
        def work():
            try:
                startup["recovery"] = director.recover()
                startup["runner_started_at_startup"] = startup["recovery"] is not None
            except Exception as error:  # surfaced through /health
                startup["recovery"] = {"error": repr(error)}
        threading.Thread(target=work, daemon=True).start()

    @app.get("/health")
    def health():
        return {"identity": director.identity, "incarnation": director.incarnation,
                "role": "factory-harness", "name": config["name"],
                "capability": capability["id"], "a2a_protocol": "0.3.0",
                "runner_running": director.module.runner.is_running(),
                "unfinished_runs": len(director.unfinished()), "startup": startup}

    return app


def init_instance(instance_dir: Path, *, name: str, mode: str, port: int, home: Path,
                  runner: dict | None = None, wait_seconds: int = 900) -> dict:
    import fcntl
    import os

    home = home.resolve()
    instance_dir = instance_dir.resolve()
    instances = home / "instances"
    instances.mkdir(parents=True, exist_ok=True)
    with (instances / "provision.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        config = {"name": name, "mode": mode, "port": port, "home": str(home),
                  "runner": {}, "director_wait_seconds": wait_seconds,
                  "capability": {"id": "verified-research@1", "name": "Verified research",
                                 "description": "Researches a question with independent counter-evidence "
                                                "review; returns one accepted report and release receipt.",
                                 "tags": ["research", "verified"]}}
        path = instance_dir / "instance.json"
        if path.exists():
            existing = json.loads(path.read_text())
            if existing != config:
                raise FileExistsError("instance already configured differently")
        for other in instances.glob("*/instance.json"):
            if other.parent.resolve() != instance_dir and json.loads(other.read_text()).get("port") == port:
                raise ValueError(f"harness port {port} already configured for {other.parent}")
        if mode == "factory":
            requested = runner or {}
            Runner(home, port_base=requested.get("port_base", os.getenv("EXO_RUNNER_PORT_BASE")),
                   member_base=requested.get("member_base", os.getenv("EXO_RUNNER_MEMBER_BASE")))
        if path.exists():
            return existing
        instance_dir.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n")
        return config


if __name__ == "__main__":
    import fcntl

    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["serve"])
    parser.add_argument("--instance-dir", type=Path, required=True)
    args = parser.parse_args()
    config = load_config(args.instance_dir)
    with (args.instance_dir / "harness.lock").open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError(f"instance already serving: {args.instance_dir}") from error
        uvicorn.run(create_app(args.instance_dir), host="127.0.0.1", port=config["port"],
                    log_level="warning")
