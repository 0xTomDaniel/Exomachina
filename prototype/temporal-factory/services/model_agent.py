"""Independent, durable A2A report agent. No factory state is imported here."""
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
from uuid import uuid4

import uvicorn
from a2a.server.agent_execution import AgentExecutor
from a2a.server.apps import A2AFastAPIApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import TaskStore
from a2a.types import (AgentCapabilities, AgentCard, AgentExtension, AgentSkill,
                       Artifact, DataPart, InvalidParamsError, Message, Part, Task,
                       TaskState, TaskStatus, TextPart)
from a2a.utils.errors import ServerError
from fastapi import Request
from fastapi.responses import JSONResponse
from strands import Agent
from strands.models import Model

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from model_broker import ModelBroker, PiBrokerModel  # noqa: E402
from agent_roles import ROLES  # noqa: E402


TOKEN = "Bearer fixture-token"
EXTENSION_URI = "urn:exomachina:a2a-action-contract:v1"
CONTRACT_NAME = "action-idempotent-async@1"
FAILURE_TEXT = "Agent work failed."
MAX_CALLS = 3
DEADLINE_SECONDS = 240


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value: object) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def contract_for(capability: str) -> dict:
    return {
        "name": CONTRACT_NAME, "protocol": "a2a/0.3.0", "capability": capability,
        "request": {"method": "message/send", "blocking": False,
                    "data": {"op": "assign", "fields": ["action_id", "run_id",
                                                        "definition_digest", "brief"]}},
        "response": {"kind": "task", "initial_states": ["submitted", "working"],
                     "metadata": ["action_id", "run_id", "definition_digest", "agent_identity"]},
        "completion": {"method": "tasks/get", "state": "completed", "artifact_count": 1,
                       "data": ["revision", "sha256", "author", "content", "action_id",
                                "run_id", "definition_digest"]},
        "idempotency": {"key": "action_id", "same_payload": "original_task_id",
                        "conflict": "json-rpc-error", "commit_before_response": True},
        "reconcile": "a2a-idempotent-resend",
    }


class Rejected(ValueError):
    pass


def command_from_params(params) -> tuple[dict, dict]:
    if not params.configuration or params.configuration.blocking is not False:
        raise Rejected("configuration.blocking must be false")
    parts = params.message.parts
    if len(parts) != 1 or not isinstance(parts[0].root, DataPart):
        raise Rejected("exactly one DataPart required")
    command = parts[0].root.data
    if not isinstance(command, dict) or set(command) != {
        "op", "action_id", "run_id", "definition_digest", "brief"
    } or command["op"] != "assign":
        raise Rejected("invalid assign command")
    if any(not isinstance(command[key], str) or not command[key]
           for key in ("action_id", "run_id", "definition_digest", "brief")):
        raise Rejected("invalid assign fields")
    try:
        brief = json.loads(command["brief"])
    except ValueError as error:
        raise Rejected("brief must be JSON") from error
    if not isinstance(brief, dict) or not isinstance(brief.get("revision"), str):
        raise Rejected("brief must contain revision")
    return command, brief


class Ledger:
    def __init__(self, state: Path):
        state.mkdir(parents=True, exist_ok=True)
        self.database = state / "model-agent.sqlite3"
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS identity (singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    id TEXT NOT NULL, incarnation INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS tasks (task_id TEXT PRIMARY KEY, context_id TEXT NOT NULL,
                    action_id TEXT NOT NULL UNIQUE, fingerprint TEXT NOT NULL, state TEXT NOT NULL,
                    run_id TEXT NOT NULL, definition_digest TEXT NOT NULL, brief TEXT NOT NULL,
                    artifact TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS model_calls (id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL, session_id TEXT NOT NULL, provider TEXT NOT NULL,
                    model_id TEXT NOT NULL, live INTEGER NOT NULL, call_no INTEGER NOT NULL,
                    started_at REAL NOT NULL, ended_at REAL, outcome_kind TEXT);
                CREATE TABLE IF NOT EXISTS stimulus (id INTEGER PRIMARY KEY AUTOINCREMENT,
                    claim TEXT NOT NULL, revisions TEXT NOT NULL, bound_run_id TEXT,
                    armed_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS stimulus_log (id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stimulus_id INTEGER NOT NULL, run_id TEXT NOT NULL, revision TEXT NOT NULL,
                    task_id TEXT NOT NULL, planted_text TEXT NOT NULL, sha256_after TEXT NOT NULL);
            """)
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT id, incarnation FROM identity WHERE singleton=1").fetchone()
            if row:
                self.identity, self.incarnation = row["id"], row["incarnation"] + 1
                db.execute("UPDATE identity SET incarnation=? WHERE singleton=1", (self.incarnation,))
            else:
                self.identity, self.incarnation = str(uuid4()), 1
                db.execute("INSERT INTO identity VALUES (1,?,1)", (self.identity,))

    def connect(self):
        db = sqlite3.connect(self.database, timeout=15)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        return db

    def accept(self, command: dict, task_id: str, context_id: str) -> tuple[str, bool]:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT task_id, fingerprint FROM tasks WHERE action_id=?",
                             (command["action_id"],)).fetchone()
            fingerprint = digest(command)
            if row:
                if row["fingerprint"] != fingerprint:
                    raise Rejected("action_id reused with different payload")
                return row["task_id"], False
            now = time.time()
            db.execute("""INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                       (task_id, context_id, command["action_id"], fingerprint, "working",
                        command["run_id"], command["definition_digest"], command["brief"],
                        None, now, now))
            # One armed control binds when its next *new* run arrives, including
            # a revision outside its selection. Replays never enter this branch.
            known = db.execute("SELECT 1 FROM tasks WHERE run_id=? AND task_id<>? LIMIT 1",
                               (command["run_id"], task_id)).fetchone()
            if not known:
                pending = db.execute("SELECT id FROM stimulus WHERE bound_run_id IS NULL ORDER BY id LIMIT 1").fetchone()
                if pending:
                    db.execute("UPDATE stimulus SET bound_run_id=? WHERE id=?",
                               (command["run_id"], pending["id"]))
        return task_id, True

    def row(self, task_id: str):
        with self.connect() as db:
            return db.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()

    def task(self, task_id: str) -> Task | None:
        row = self.row(task_id)
        if row is None:
            return None
        metadata = {"action_id": row["action_id"], "run_id": row["run_id"],
                    "definition_digest": row["definition_digest"],
                    "agent_identity": self.identity}
        artifact = json.loads(row["artifact"]) if row["artifact"] else None
        status_message = None
        if row["state"] == "failed":
            status_message = Message(message_id=str(uuid4()), role="agent",
                                     parts=[Part(root=TextPart(text=FAILURE_TEXT))])
        return Task(id=task_id, context_id=row["context_id"],
                    status=TaskStatus(state=TaskState(row["state"]), message=status_message),
                    metadata=metadata,
                    artifacts=[Artifact(artifact_id=artifact["sha256"],
                                        parts=[Part(root=DataPart(data=artifact))])]
                    if artifact else None)

    def finish(self, task_id: str, artifact: dict | None):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("UPDATE tasks SET state=?, artifact=?, updated_at=? WHERE task_id=? AND state='working'",
                       ("completed" if artifact else "failed", canonical(artifact) if artifact else None,
                        time.time(), task_id))

    def call_start(self, task_id: str, session_id: str, provider: str, model_id: str,
                   live: bool) -> tuple[int, int]:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            call_no = db.execute("SELECT COUNT(*) FROM model_calls WHERE task_id=?", (task_id,)).fetchone()[0] + 1
            if call_no > MAX_CALLS:
                raise RuntimeError("model call budget exhausted")
            cursor = db.execute("""INSERT INTO model_calls
                (task_id,session_id,provider,model_id,live,call_no,started_at)
                VALUES (?,?,?,?,?,?,?)""", (task_id, session_id, provider, model_id,
                                             int(live), call_no, time.time()))
            return cursor.lastrowid, call_no

    def call_end(self, call_id: int, outcome: str):
        with self.connect() as db:
            db.execute("UPDATE model_calls SET ended_at=?, outcome_kind=? WHERE id=?",
                       (time.time(), outcome, call_id))

    def call_count(self, task_id: str) -> int:
        with self.connect() as db:
            return db.execute("SELECT COUNT(*) FROM model_calls WHERE task_id=?", (task_id,)).fetchone()[0]

    def arm(self, body: dict) -> dict:
        if not isinstance(body, dict) or set(body) != {"append_claim", "revisions"}:
            raise Rejected("invalid stimulus")
        claim, revisions = body["append_claim"], body["revisions"]
        if (not isinstance(claim, dict) or set(claim) != {"text", "evidence"}
                or not isinstance(claim["text"], str) or not claim["text"].strip()
                or not isinstance(claim["evidence"], list) or not claim["evidence"]
                or any(not isinstance(e, str) or not e for e in claim["evidence"])
                or not (revisions == "all" or isinstance(revisions, list) and revisions
                        and all(isinstance(r, str) and r.startswith("r") for r in revisions))):
            raise Rejected("invalid stimulus")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if db.execute("SELECT 1 FROM stimulus WHERE bound_run_id IS NULL").fetchone():
                raise Rejected("a stimulus is already armed")
            cursor = db.execute("INSERT INTO stimulus (claim,revisions,armed_at) VALUES (?,?,?)",
                                (canonical(claim), canonical(revisions), time.time()))
        return {"armed": True, "stimulus_id": cursor.lastrowid}

    def apply_stimulus(self, task_id: str, run_id: str, revision: str, content: dict) -> dict:
        with self.connect() as db:
            row = db.execute("SELECT * FROM stimulus WHERE bound_run_id=? ORDER BY id LIMIT 1",
                             (run_id,)).fetchone()
        if not row:
            return content
        revisions = json.loads(row["revisions"])
        if revisions != "all" and revision not in revisions:
            return content
        claim = json.loads(row["claim"])
        used = {item.get("id") for item in content["claims"]}
        number = 1
        while f"C{number}" in used:
            number += 1
        modified = json.loads(canonical(content))
        modified["claims"].append({"id": f"C{number}", **claim})
        modified["markdown"] += "\n\n" + claim["text"]
        sha256 = hashlib.sha256(canonical(modified).encode()).hexdigest()
        with self.connect() as db:
            db.execute("INSERT INTO stimulus_log (stimulus_id,run_id,revision,task_id,planted_text,sha256_after) VALUES (?,?,?,?,?,?)",
                       (row["id"], run_id, revision, task_id, claim["text"], sha256))
        return modified

    def stimulus_log(self) -> list[dict]:
        with self.connect() as db:
            rows = db.execute("SELECT run_id,revision,task_id,planted_text,sha256_after FROM stimulus_log ORDER BY id").fetchall()
        return [dict(row) for row in rows]

    def observe(self) -> dict:
        with self.connect() as db:
            tasks = [dict(row) for row in db.execute("""SELECT task_id,context_id,action_id,run_id,
                definition_digest,state,created_at,updated_at FROM tasks ORDER BY created_at""")]
            calls = [dict(row) for row in db.execute("""SELECT task_id,session_id,provider,model_id,
                live,call_no,started_at,ended_at,outcome_kind FROM model_calls ORDER BY id""")]
        for call in calls:
            call["live"] = bool(call["live"])
        return {"tasks": tasks, "model_calls": calls}


class ScriptedModel(Model):
    def __init__(self, reply: str, model_id: str):
        self.reply, self.config = reply, {"model_id": model_id, "context_window_limit": 16000}

    def update_config(self, **config):
        self.config.update(config)

    def get_config(self):
        return self.config

    async def structured_output(self, *args, **kwargs):
        raise NotImplementedError
        yield  # pragma: no cover

    async def stream(self, messages, tool_specs=None, system_prompt=None, **kwargs):
        yield {"messageStart": {"role": "assistant"}}
        yield {"contentBlockStart": {"start": {}}}
        yield {"contentBlockDelta": {"delta": {"text": self.reply}}}
        yield {"contentBlockStop": {}}
        yield {"messageStop": {"stopReason": "end_turn"}}


class RecordedModel(Model):
    def __init__(self, inner: Model, ledger: Ledger, task_id: str, session_id: str,
                 provider: str, model_id: str, deadline: float):
        self.inner, self.ledger, self.task_id = inner, ledger, task_id
        self.session_id, self.provider, self.model_id = session_id, provider, model_id
        self.deadline = deadline

    def update_config(self, **config):
        self.inner.update_config(**config)

    def get_config(self):
        return self.inner.get_config()

    async def structured_output(self, *args, **kwargs):
        raise NotImplementedError
        yield  # pragma: no cover

    async def stream(self, messages, tool_specs=None, system_prompt=None, **kwargs):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError("agent deadline exhausted")
        call_id, _ = self.ledger.call_start(self.task_id, self.session_id, self.provider,
                                             self.model_id, self.provider == "codex-subscription")
        outcome = "completed"
        try:
            async with asyncio.timeout(remaining):
                async for event in self.inner.stream(messages, tool_specs=tool_specs,
                                                     system_prompt=system_prompt, **kwargs):
                    yield event
        except TimeoutError:
            outcome = "deadline"
            raise
        except BaseException:
            outcome = "error"
            raise
        finally:
            self.ledger.call_end(call_id, outcome)


class Service:
    def __init__(self, state: Path, role_name: str, capability: str, provider: str,
                 model_id: str, test_controls: bool, roles: dict | None = None,
                 deadline_seconds: float = DEADLINE_SECONDS):
        if role_name not in {"research", "synthesis", "quality"}:
            raise ValueError("invalid role")
        if provider not in {"codex-subscription", "synthetic-loopback", "scripted"}:
            raise ValueError("invalid model provider")
        allowed = {"research": {"packet_findings@1", "packet_risks@1"},
                   "synthesis": {"report_synthesis@1"},
                   "quality": {"report_quality_review@1"}}
        if capability not in allowed[role_name]:
            raise ValueError("capability does not match role")
        if provider == "codex-subscription" and (
                "EXO_MODEL_HOME" in os.environ or "EXO_CODEX_BASE_URL" in os.environ):
            raise ValueError("codex-subscription requires the default model home and endpoint")
        if provider == "synthetic-loopback":
            home = os.environ.get("EXO_MODEL_HOME")
            if not home or not (Path(home) / "FIXTURE_STORE").is_file() or not os.environ.get("EXO_CODEX_BASE_URL"):
                raise ValueError("synthetic-loopback requires a marked fixture home and loopback endpoint")
        if test_controls and role_name != "synthesis":
            raise ValueError("test controls are for synthesis only")
        self.ledger = Ledger(state)
        self.role_name, self.capability, self.provider, self.model_id = role_name, capability, provider, model_id
        self.role = (roles if roles is not None else ROLES)[role_name]
        self.test_controls = test_controls
        self.max_calls, self.deadline_seconds = MAX_CALLS, deadline_seconds
        self.running: dict[str, asyncio.Task] = {}

    def schedule(self, task_id: str):
        if task_id not in self.running:
            task = asyncio.create_task(self.work(task_id))
            self.running[task_id] = task
            task.add_done_callback(lambda _: self.running.pop(task_id, None))

    def recover(self):
        for row in self.ledger.observe()["tasks"]:
            if row["state"] == "working":
                self.schedule(row["task_id"])

    async def work(self, task_id: str):
        row = self.ledger.row(task_id)
        if row is None or row["state"] != "working":
            return
        try:
            brief = json.loads(row["brief"])
            prechecked = self.role.precheck(brief, self.ledger.identity)
            if prechecked is not None:
                content = prechecked
            else:
                session_id = f"{self.ledger.identity}:{task_id}"
                remaining = self.deadline_seconds - (time.time() - row["created_at"])
                deadline = time.monotonic() + max(0, remaining)
                content = None
                while self.ledger.call_count(task_id) < self.max_calls and time.monotonic() < deadline:
                    if self.provider == "scripted":
                        inner = ScriptedModel(self.role.scripted_reply(brief, self.ledger.identity), self.model_id)
                    else:
                        inner = PiBrokerModel(ModelBroker(), model_id=self.model_id, session_id=session_id)
                    model = RecordedModel(inner, self.ledger, task_id, session_id, self.provider,
                                          self.model_id, deadline)
                    agent = Agent(name=f"{self.role_name} agent", model=model, tools=[],
                                  system_prompt=self.role.system_prompt(self.capability),
                                  callback_handler=None)
                    result = await agent.invoke_async(self.role.user_prompt(brief))
                    message = result.message
                    blocks = message.get("content", []) if isinstance(message, dict) else message.content
                    response = "".join(block.get("text", "") if isinstance(block, dict) else getattr(block, "text", "")
                                       for block in blocks)
                    try:
                        content = self.role.parse(response, brief, self.ledger.identity)
                        break
                    except ValueError:
                        self.ledger.call_end(self._last_call_id(task_id), "validation-error")
                if content is None:
                    raise RuntimeError("model output budget exhausted")
            if self.test_controls:
                content = self.ledger.apply_stimulus(task_id, row["run_id"], brief["revision"], content)
            rendered = canonical(content)
            sha256 = hashlib.sha256(rendered.encode()).hexdigest()
            artifact = {"revision": brief["revision"], "sha256": sha256,
                        "author": self.ledger.identity, "content": rendered,
                        "action_id": row["action_id"], "run_id": row["run_id"],
                        "definition_digest": row["definition_digest"]}
            self.ledger.finish(task_id, artifact)
        except asyncio.CancelledError:
            # A stopped process leaves the committed Task working for recovery.
            raise
        except Exception:
            self.ledger.finish(task_id, None)

    def _last_call_id(self, task_id: str) -> int:
        with self.ledger.connect() as db:
            return db.execute("SELECT id FROM model_calls WHERE task_id=? ORDER BY id DESC LIMIT 1",
                              (task_id,)).fetchone()[0]


class LedgerTaskStore(TaskStore):
    def __init__(self, ledger: Ledger):
        self.ledger = ledger

    async def get(self, task_id, context=None):
        return self.ledger.task(task_id)

    async def save(self, task, context=None):
        if not self.ledger.task(task.id):
            raise Rejected("task has no committed action")

    async def delete(self, task_id, context=None):
        raise NotImplementedError("task deletion is outside this contract")


class Executor(AgentExecutor):
    async def execute(self, context, event_queue):
        raise NotImplementedError("nonblocking sends are handled by Handler")

    async def cancel(self, context, event_queue):
        raise ServerError(error=InvalidParamsError(message="action cannot be cancelled"))


class Handler(DefaultRequestHandler):
    def __init__(self, service: Service):
        super().__init__(Executor(), LedgerTaskStore(service.ledger))
        self.service = service
        self.send_lock = asyncio.Lock()

    async def on_message_send(self, params, context=None):
        try:
            command, _ = command_from_params(params)
            async with self.send_lock:
                task_id, created = self.service.ledger.accept(command, str(uuid4()),
                                  params.message.context_id or str(uuid4()))
                if created:
                    self.service.schedule(task_id)
                return self.service.ledger.task(task_id)
        except Rejected as error:
            raise ServerError(error=InvalidParamsError(message=str(error))) from error


def create_app(state: Path, port: int, *, role: str, capability: str,
               model_provider: str = "scripted", model: str = "gpt-6-sol",
               test_controls: bool = False, roles: dict | None = None,
               deadline_seconds: float = DEADLINE_SECONDS):
    service = Service(state, role, capability, model_provider, model, test_controls,
                      roles, deadline_seconds)
    contract = contract_for(capability)
    extension = AgentExtension(uri=EXTENSION_URI, required=True,
                               params={"identity": service.ledger.identity,
                                       "contract": CONTRACT_NAME,
                                       "contract_digest": digest(contract)})
    card = AgentCard(name=f"{role.title()} report agent", description="Independent report agent",
        url=f"http://127.0.0.1:{port}/", version="1.0.0", protocol_version="0.3.0",
        default_input_modes=["application/json"], default_output_modes=["application/json"],
        capabilities=AgentCapabilities(streaming=False, extensions=[extension]),
        skills=[AgentSkill(id=capability, name=capability, description=f"{role} report work",
                           tags=[role])],
        security_schemes={"fixtureBearer": {"type": "http", "scheme": "bearer"}},
        security=[{"fixtureBearer": []}])
    app = A2AFastAPIApplication(card, Handler(service)).build()

    @app.on_event("startup")
    async def recover():
        service.recover()

    @app.middleware("http")
    async def fixture_auth(request: Request, call_next):
        if request.url.path in {"/health", "/.well-known/agent-card.json"}:
            return await call_next(request)
        if request.headers.get("authorization") != TOKEN:
            return JSONResponse({"error": "fixture authentication required"}, status_code=401)
        return await call_next(request)

    @app.get("/health")
    def health():
        return {"identity": service.ledger.identity, "incarnation": service.ledger.incarnation,
                "role": "quality" if role == "quality" else "capability",
                "capability": capability, "a2a_protocol": "0.3.0"}

    @app.get("/contract")
    def served_contract():
        return contract

    @app.get("/fixture/actions/{action_id}")
    def action(action_id: str):
        with service.ledger.connect() as db:
            row = db.execute("SELECT task_id FROM tasks WHERE action_id=?", (action_id,)).fetchone()
        if row is None:
            return JSONResponse({"error": "unknown action"}, status_code=404)
        task = service.ledger.task(row["task_id"])
        return {"action_id": task.metadata["action_id"], "run_id": task.metadata["run_id"],
                "definition_digest": task.metadata["definition_digest"], "task_id": task.id,
                "state": task.status.state.value}

    if test_controls:
        @app.post("/_test/stimulus")
        async def arm(request: Request):
            try:
                return service.ledger.arm(await request.json())
            except (Rejected, ValueError):
                return JSONResponse({"error": "invalid stimulus"}, status_code=400)

        @app.get("/_test/stimulus-log")
        def stimulus_log():
            return service.ledger.stimulus_log()

        @app.get("/_test/observe")
        def observe():
            return service.ledger.observe()

    return app


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--role", required=True, choices=("research", "synthesis", "quality"))
    parser.add_argument("--capability", required=True)
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--model-provider", required=True,
                        choices=("codex-subscription", "synthetic-loopback", "scripted"))
    parser.add_argument("--model", default="gpt-6-sol")
    parser.add_argument("--test-controls", action="store_true")
    args = parser.parse_args()
    app = create_app(args.state, args.port, role=args.role, capability=args.capability,
                     model_provider=args.model_provider, model=args.model,
                     test_controls=args.test_controls)
    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
