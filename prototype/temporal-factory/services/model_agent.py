"""Independent, durable A2A report agent. No factory state is imported here."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
from pathlib import Path
from uuid import uuid4

import uvicorn
from a2a.server.agent_execution import AgentExecutor
from a2a.types import (AgentCapabilities, AgentCard, AgentExtension, AgentSkill,
                       Artifact, InvalidParamsError, Task, TaskStatus)
from fastapi import Request
from fastapi.responses import JSONResponse
from strands import Agent
from strands.models import Model

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
# Services consume the journal through the public src facade; model_usage is src-internal.
from model_broker import (DEFAULT_MODEL_ID, ModelBroker, ModelUsageJournal, PiBrokerModel,
                          DEFAULT_REASONING_EFFORT, unavailable_usage)  # noqa: E402
from agent_roles import ROLES  # noqa: E402
import a2a_v1  # noqa: E402
from a2a_v1_server import (LegacyRequestHandler, ProjectionTaskStore,  # noqa: E402
                           agent_message, bearer_security, build_app, card_pin_projection,
                           data_part, interfaces, part_content, part_data, task_state,
                           text_part)


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
        "name": CONTRACT_NAME, "protocol": a2a_v1.PROTOCOL, "capability": capability,
        "request": {"method": a2a_v1.SEND_MESSAGE, "returnImmediately": True,
                    "data": {"op": "assign", "fields": ["action_id", "run_id",
                                                        "definition_digest", "brief"],
                             "optional_fields": ["assignment_id", "attempt_id", "factory_id"]}},
        "response": {"result": "task",
                     "initial_states": [a2a_v1.wire_state("submitted"),
                                        a2a_v1.wire_state("working")],
                     "metadata": ["action_id", "run_id", "definition_digest", "agent_identity"]},
        "completion": {"method": a2a_v1.GET_TASK, "state": a2a_v1.wire_state("completed"),
                       "artifact_count": 1,
                       "data": ["revision", "sha256", "author", "content", "action_id",
                                "run_id", "definition_digest"]},
        "idempotency": {"key": "action_id", "same_payload": "original_task_id",
                        "conflict": "json-rpc-error", "commit_before_response": True},
        "reconcile": "a2a-idempotent-resend",
    }


def validate_role_capability(role_name: str, capability: str) -> None:
    allowed = {"research": {"packet_findings@1", "packet_risks@1"},
               "synthesis": {"report_synthesis@1"},
               "quality": {"report_quality_review@1"}}
    if role_name not in allowed or capability not in allowed[role_name]:
        raise ValueError("capability does not match role")


PINNED_DECLARATION_IDENTITY = {
    ("quality", "report_quality_review@1"): ("quality", "quality"),
    ("research", "packet_findings@1"): ("capability", "research_findings"),
    ("research", "packet_risks@1"): ("capability", "research_risks"),
    ("synthesis", "report_synthesis@1"): ("capability", "synthesizer"),
}


class Rejected(ValueError):
    pass


class ReadOnlyUsageJournal(ModelUsageJournal):
    """Existing-owner reader using the safe projection on a read-only DB handle."""

    CALL_SCOPES = {"authoring_overhead", "director_call", "assignment_call"}

    def __init__(self, database: Path):
        self.database = Path(database).expanduser().resolve()
        if not self.database.is_file():
            raise FileNotFoundError("existing usage owner state is required")
        db = self._connect()
        try:
            db.execute("SELECT * FROM model_usage_measurements LIMIT 0")
        except sqlite3.Error as error:
            raise RuntimeError("existing model-agent usage journal is unavailable") from error
        finally:
            db.close()

    def _connect(self):
        db = sqlite3.connect(self.database.as_uri() + "?mode=ro", uri=True, timeout=15)
        db.row_factory = sqlite3.Row
        return db

    def record(self, **_facts):
        raise PermissionError("read-only usage owner")

    @staticmethod
    def _view(row):
        # Existing pinned owners may predate the nullable per-call binding columns.
        # Project the same nulls and scope classification the writer migration uses,
        # but keep this read-only and never infer an identifier from action text.
        values = dict(row)
        for field in ("message_id", "assignment_id", "attempt_id"):
            values.setdefault(field, None)
        scope = values.get("call_scope")
        if scope is None:
            if values.get("action_id") is not None:
                scope = "assignment_call"
            elif values.get("message_id") is not None:
                scope = "director_call"
            elif values.get("task_id") is None:
                scope = "authoring_overhead"
            else:
                scope = "assignment_call"
            values["call_scope"] = scope
        if scope not in ReadOnlyUsageJournal.CALL_SCOPES:
            raise sqlite3.DatabaseError("unsupported persisted usage call scope")
        return ModelUsageJournal._view(values)

    def list_measurements(self, *, run_id: str | None = None, task_id: str | None = None,
                          action_id: str | None = None, message_id: str | None = None,
                          assignment_id: str | None = None, attempt_id: str | None = None,
                          model_call_id: str | None = None,
                          call_scope: str | None = None) -> list[dict]:
        # Filter the safe normalized projections in memory so a query for a new
        # nullable field on an old table returns no matches instead of SQL error.
        filters = {"run_id": run_id, "task_id": task_id, "action_id": action_id,
                   "message_id": message_id, "assignment_id": assignment_id,
                   "attempt_id": attempt_id, "model_call_id": model_call_id,
                   "call_scope": call_scope}
        for name, value in filters.items():
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(f"{name} must be non-empty text")
        if call_scope is not None and call_scope not in self.CALL_SCOPES:
            raise ValueError("unsupported call_scope filter")
        rows = super().list_measurements()
        return [row for row in rows if all(row[name] == value
                                           for name, value in filters.items()
                                           if value is not None)]


def command_from_params(params) -> tuple[dict, dict]:
    if not params.HasField("configuration") or params.configuration.return_immediately is not True:
        raise Rejected("configuration.returnImmediately must be true")
    parts = params.message.parts
    if len(parts) != 1 or part_content(parts[0]) != "data":
        raise Rejected("exactly one data Part required")
    command = part_data(parts[0])
    required = {"op", "action_id", "run_id", "definition_digest", "brief"}
    optional = {"assignment_id", "attempt_id", "factory_id"}
    if (not isinstance(command, dict) or not required.issubset(command) or
            set(command) - required - optional or command["op"] != "assign"):
        raise Rejected("invalid assign command")
    if any(not isinstance(command[key], str) or not command[key]
           for key in ("action_id", "run_id", "definition_digest", "brief")):
        raise Rejected("invalid assign fields")
    if any(key in command and (not isinstance(command[key], str) or not command[key].strip())
           for key in optional):
        raise Rejected("invalid assignment binding fields")
    if ("factory_id" in command
            and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}",
                             command["factory_id"]) is None):
        raise Rejected("factory_id must be a safe identifier")
    try:
        brief = json.loads(command["brief"])
    except ValueError as error:
        raise Rejected("brief must be JSON") from error
    if not isinstance(brief, dict) or not isinstance(brief.get("revision"), str):
        raise Rejected("brief must contain revision")
    return command, brief


class Ledger:
    def __init__(self, state: Path, *, read_only: bool = False):
        state = Path(state)
        self.database = state / "model-agent.sqlite3"
        self.read_only = read_only
        if read_only:
            # SQLite URI mode=ro requires an absolute URI. Resolve existing
            # paths without creating their parent or the database.
            self.database = self.database.expanduser().resolve()
            if not self.database.is_file():
                raise FileNotFoundError("existing model-agent owner state is required")
            db = self.connect()
            try:
                row = db.execute("SELECT id, incarnation FROM identity WHERE singleton=1").fetchone()
            except sqlite3.Error as error:
                raise RuntimeError("existing model-agent identity is unavailable") from error
            finally:
                db.close()
            if row is None:
                raise RuntimeError("existing model-agent identity is unavailable")
            self.identity, self.incarnation = row["id"], row["incarnation"]
            return
        state.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS identity (singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    id TEXT NOT NULL, incarnation INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS tasks (task_id TEXT PRIMARY KEY, context_id TEXT NOT NULL,
                    action_id TEXT NOT NULL UNIQUE, fingerprint TEXT NOT NULL, state TEXT NOT NULL,
                    run_id TEXT NOT NULL, definition_digest TEXT NOT NULL, brief TEXT NOT NULL,
                    artifact TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL,
                    assignment_id TEXT, attempt_id TEXT, factory_id TEXT);
                CREATE TABLE IF NOT EXISTS model_calls (id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL, session_id TEXT NOT NULL, provider TEXT NOT NULL,
                    model_id TEXT NOT NULL, live INTEGER NOT NULL, call_no INTEGER NOT NULL,
                    started_at REAL NOT NULL, ended_at REAL, outcome_kind TEXT);
                CREATE TABLE IF NOT EXISTS stimulus (id INTEGER PRIMARY KEY AUTOINCREMENT,
                    claim TEXT NOT NULL, revisions TEXT NOT NULL, bound_run_id TEXT,
                    armed_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS stimulus_log (id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stimulus_id INTEGER NOT NULL, run_id TEXT NOT NULL, revision TEXT NOT NULL,
                    task_id TEXT NOT NULL, planted_text TEXT NOT NULL, sha256_after TEXT NOT NULL,
                    content_after TEXT,
                    UNIQUE(stimulus_id, task_id, revision));
            """)
            if "content_after" not in {column["name"] for column in db.execute("PRAGMA table_info(stimulus_log)")}:
                db.execute("ALTER TABLE stimulus_log ADD COLUMN content_after TEXT")
            task_columns = {column["name"] for column in db.execute("PRAGMA table_info(tasks)")}
            for column in ("assignment_id", "attempt_id", "factory_id"):
                if column not in task_columns:
                    db.execute(f"ALTER TABLE tasks ADD COLUMN {column} TEXT")
            db.execute("CREATE UNIQUE INDEX IF NOT EXISTS stimulus_log_action_revision ON stimulus_log(stimulus_id, task_id, revision)")
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT id, incarnation FROM identity WHERE singleton=1").fetchone()
            if row:
                self.identity, self.incarnation = row["id"], row["incarnation"] + 1
                db.execute("UPDATE identity SET incarnation=? WHERE singleton=1", (self.incarnation,))
            else:
                self.identity, self.incarnation = str(uuid4()), 1
                db.execute("INSERT INTO identity VALUES (1,?,1)", (self.identity,))

    def connect(self):
        if self.read_only:
            db = sqlite3.connect(self.database.as_uri() + "?mode=ro", uri=True, timeout=15)
            db.row_factory = sqlite3.Row
            return db
        db = sqlite3.connect(self.database, timeout=15)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        return db

    def accept(self, command: dict, task_id: str, context_id: str) -> tuple[str, bool]:
        if self.read_only:
            raise Rejected("read-only usage owner")
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
            db.execute("""INSERT INTO tasks
                (task_id,context_id,action_id,fingerprint,state,run_id,definition_digest,
                 brief,artifact,created_at,updated_at,assignment_id,attempt_id,factory_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                       (task_id, context_id, command["action_id"], fingerprint, "working",
                        command["run_id"], command["definition_digest"], command["brief"],
                        None, now, now, command.get("assignment_id"), command.get("attempt_id"),
                        command.get("factory_id")))
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
        if self.read_only:
            db = self.connect()
            try:
                return db.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            finally:
                db.close()
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
            status_message = agent_message([text_part(FAILURE_TEXT)])
        # The ledger keeps version-neutral state names; map them at the boundary.
        return Task(id=task_id, context_id=row["context_id"],
                    status=TaskStatus(state=task_state(row["state"]), message=status_message),
                    metadata=metadata,
                    artifacts=[Artifact(artifact_id=artifact["sha256"],
                                        parts=[data_part(artifact)])]
                    if artifact else None)

    def finish(self, task_id: str, artifact: dict | None, stimulus: dict | None = None):
        if self.read_only:
            raise Rejected("read-only usage owner")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if stimulus is not None:
                if artifact is None or artifact["sha256"] != stimulus["sha256_after"]:
                    raise ValueError("stimulus artifact digest mismatch")
                db.execute("""INSERT OR IGNORE INTO stimulus_log
                    (stimulus_id,run_id,revision,task_id,planted_text,sha256_after,content_after)
                    VALUES (?,?,?,?,?,?,?)""", tuple(stimulus[key] for key in (
                        "stimulus_id", "run_id", "revision", "task_id", "planted_text",
                        "sha256_after", "content_after")))
                logged = db.execute("""SELECT sha256_after,content_after FROM stimulus_log
                    WHERE stimulus_id=? AND task_id=? AND revision=?""",
                    (stimulus["stimulus_id"], task_id, stimulus["revision"])).fetchone()
                if (logged["sha256_after"] != artifact["sha256"]
                        or logged["content_after"] != artifact["content"]):
                    raise ValueError("recovered stimulus content mismatch")
            updated = db.execute("UPDATE tasks SET state=?, artifact=?, updated_at=? WHERE task_id=? AND state='working'",
                                 ("completed" if artifact else "failed", canonical(artifact) if artifact else None,
                                  time.time(), task_id))
            if updated.rowcount != 1:
                raise ValueError("Task is no longer working")

    def task_rows(self) -> list[dict]:
        with self.connect() as db:
            columns = {row["name"] for row in db.execute("PRAGMA table_info(tasks)")}
            factory_id = "factory_id" if "factory_id" in columns else "NULL AS factory_id"
            rows = db.execute(
                f"SELECT task_id,state,{factory_id} FROM tasks ORDER BY created_at,task_id"
            ).fetchall()
        return [dict(row) for row in rows]

    def call_start(self, task_id: str, session_id: str, provider: str, model_id: str,
                   live: bool) -> tuple[int, int]:
        if self.read_only:
            raise Rejected("read-only usage owner")
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
        if self.read_only:
            raise Rejected("read-only usage owner")
        with self.connect() as db:
            db.execute("UPDATE model_calls SET ended_at=?, outcome_kind=? WHERE id=?",
                       (time.time(), outcome, call_id))

    def public_model_call_id(self, call_id: int) -> str:
        if type(call_id) is not int or call_id < 1:
            raise ValueError("invalid internal model call ID")
        return f"urn:exomachina:model-call:{self.identity}:{call_id}"

    def call_count(self, task_id: str) -> int:
        db = self.connect()
        try:
            return db.execute("SELECT COUNT(*) FROM model_calls WHERE task_id=?", (task_id,)).fetchone()[0]
        finally:
            db.close()

    def arm(self, body: dict) -> dict:
        if self.read_only:
            raise Rejected("read-only usage owner")
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

    def logged_stimulus(self, task_id: str, revision: str) -> tuple[dict, dict] | None:
        with self.connect() as db:
            logged = db.execute("""SELECT * FROM stimulus_log
                WHERE task_id=? AND revision=?""", (task_id, revision)).fetchone()
        if logged is None:
            return None
        if logged["content_after"] is None:
            raise ValueError("committed stimulus lacks replay content")
        rendered = logged["content_after"]
        if hashlib.sha256(rendered.encode()).hexdigest() != logged["sha256_after"]:
            raise ValueError("committed stimulus digest mismatch")
        return json.loads(rendered), {key: logged[key] for key in (
            "stimulus_id", "run_id", "revision", "task_id", "planted_text",
            "sha256_after", "content_after")}

    def apply_stimulus(self, task_id: str, run_id: str, revision: str,
                       content: dict) -> tuple[dict, dict | None]:
        recovered = self.logged_stimulus(task_id, revision)
        if recovered is not None:
            return recovered
        with self.connect() as db:
            row = db.execute("SELECT * FROM stimulus WHERE bound_run_id=? ORDER BY id LIMIT 1",
                             (run_id,)).fetchone()
        if not row:
            return content, None
        revisions = json.loads(row["revisions"])
        if revisions != "all" and revision not in revisions:
            return content, None
        claim = json.loads(row["claim"])
        used = {item.get("id") for item in content["claims"]}
        number = 1
        while f"C{number}" in used:
            number += 1
        modified = json.loads(canonical(content))
        modified["claims"].append({"id": f"C{number}", **claim})
        modified["markdown"] += "\n\n" + claim["text"]
        rendered = canonical(modified)
        return modified, {"stimulus_id": row["id"], "run_id": run_id,
                          "revision": revision, "task_id": task_id,
                          "planted_text": claim["text"],
                          "sha256_after": hashlib.sha256(rendered.encode()).hexdigest(),
                          "content_after": rendered}

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
                 provider: str, model_id: str, deadline: float,
                 usage_journal: ModelUsageJournal | None = None,
                 service_identity: str | None = None, action_id: str | None = None,
                 run_id: str | None = None, definition_digest: str | None = None,
                 assignment_id: str | None = None, attempt_id: str | None = None):
        self.inner, self.ledger, self.task_id = inner, ledger, task_id
        self.session_id, self.provider, self.model_id = session_id, provider, model_id
        self.deadline = deadline
        self.usage_journal = usage_journal
        self.usage_binding = {"service_identity": service_identity, "task_id": task_id,
                              "action_id": action_id, "run_id": run_id,
                              "definition_digest": definition_digest,
                              "assignment_id": assignment_id, "attempt_id": attempt_id}

    def _record_usage(self, *, model_call_id: str, model_id: str,
                      reasoning_effort: str | None, usage: dict) -> None:
        if self.usage_journal is None:
            return
        self.usage_journal.record(model_call_id=model_call_id,
                                  service_identity=self.usage_binding["service_identity"],
                                  task_id=self.usage_binding["task_id"],
                                  action_id=self.usage_binding["action_id"],
                                  run_id=self.usage_binding["run_id"],
                                  definition_digest=self.usage_binding["definition_digest"],
                                  call_scope="assignment_call",
                                  assignment_id=self.usage_binding["assignment_id"],
                                  attempt_id=self.usage_binding["attempt_id"],
                                  provider=self.provider, model_id=model_id,
                                  reasoning_effort=reasoning_effort, usage=usage)

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
        public_call_id = self.ledger.public_model_call_id(call_id)
        measurement_recorded = False
        reasoning_effort = getattr(self.inner, "reasoning_effort", None)
        set_usage_context = getattr(self.inner, "set_usage_context", None)
        if callable(set_usage_context) and self.usage_journal is not None:
            def record_provider_usage(record: dict) -> None:
                nonlocal measurement_recorded
                measurement_recorded = True
                self._record_usage(model_call_id=record["model_call_id"],
                                   model_id=record["model_id"],
                                   reasoning_effort=record["reasoning_effort"],
                                   usage=record["usage"])
            set_usage_context(model_call_id=public_call_id, callback=record_provider_usage)
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
            try:
                if self.usage_journal is not None and not measurement_recorded:
                    self._record_usage(model_call_id=public_call_id, model_id=self.model_id,
                                       reasoning_effort=reasoning_effort,
                                       usage=unavailable_usage())
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
        validate_role_capability(role_name, capability)
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
        self.usage = ModelUsageJournal(self.ledger.database)
        self.role_name, self.capability, self.provider, self.model_id = role_name, capability, provider, model_id
        self.role = (roles if roles is not None else ROLES)[role_name]
        self.test_controls = test_controls
        self.max_calls, self.deadline_seconds = MAX_CALLS, deadline_seconds
        self.running: dict[str, asyncio.Task] = {}

    def schedule(self, task_id: str):
        """Run an accepted Task immediately; agents hold no capacity queue."""
        if task_id not in self.running:
            task = asyncio.create_task(self.work(task_id))
            self.running[task_id] = task
            task.add_done_callback(lambda _: self.running.pop(task_id, None))

    def recover(self):
        for row in self.ledger.task_rows():
            if row["state"] == "working":
                self.schedule(row["task_id"])

    async def work(self, task_id: str):
        row = self.ledger.row(task_id)
        if row is None or row["state"] != "working":
            return
        try:
            brief = json.loads(row["brief"])
            recovered = (self.ledger.logged_stimulus(task_id, brief["revision"])
                         if self.test_controls else None)
            stimulus = None
            if recovered is not None:
                content, stimulus = recovered
            else:
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
                                              self.model_id, deadline, usage_journal=self.usage,
                                              service_identity=self.ledger.identity,
                                              action_id=row["action_id"], run_id=row["run_id"],
                                              definition_digest=row["definition_digest"],
                                              assignment_id=row["assignment_id"],
                                              attempt_id=row["attempt_id"])
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
                    content, stimulus = self.ledger.apply_stimulus(task_id, row["run_id"],
                                                                   brief["revision"], content)
            rendered = canonical(content)
            sha256 = hashlib.sha256(rendered.encode()).hexdigest()
            artifact = {"revision": brief["revision"], "sha256": sha256,
                        "author": self.ledger.identity, "content": rendered,
                        "action_id": row["action_id"], "run_id": row["run_id"],
                        "definition_digest": row["definition_digest"]}
            self.ledger.finish(task_id, artifact, stimulus)
        except asyncio.CancelledError:
            # A stopped process leaves the committed Task working for recovery.
            raise
        except Exception:
            self.ledger.finish(task_id, None)

    def _last_call_id(self, task_id: str) -> int:
        with self.ledger.connect() as db:
            return db.execute("SELECT id FROM model_calls WHERE task_id=? ORDER BY id DESC LIMIT 1",
                              (task_id,)).fetchone()[0]


class ReadOnlyUsageService:
    """Minimal existing-owner facade that never constructs model execution state."""

    def __init__(self, state: Path, role_name: str, capability: str, *,
                 pinned_descriptor_document: Path, protocol_document: Path,
                 expected_descriptor_digest: str,
                 expected_protocol_digest: str,
                 expected_identity: str):
        validate_role_capability(role_name, capability)
        self.ledger = Ledger(state, read_only=True)
        if (not isinstance(expected_identity, str) or not expected_identity.strip()
                or self.ledger.identity != expected_identity):
            raise RuntimeError("existing model-agent identity differs from the pinned identity")
        self.usage = ReadOnlyUsageJournal(self.ledger.database)
        self.descriptor, self.contract = self._load_pinned_contract(
            pinned_descriptor_document, protocol_document, role_name, capability,
            expected_descriptor_digest, expected_protocol_digest)
        self.role_name, self.capability = role_name, capability
        self.test_controls = False
        self.read_only_usage = True

    @staticmethod
    def _read_pinned_json(path: Path) -> dict:
        source = Path(path).expanduser().resolve(strict=True)
        if not source.is_file() or source.stat().st_size > 65536:
            raise ValueError("pinned declaration is not a bounded local file")
        try:
            document = json.loads(source.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("pinned declaration is unreadable") from error
        if not isinstance(document, dict):
            raise ValueError("pinned declaration must be a JSON object")
        return document

    @staticmethod
    def _sha256_pin(value: object, name: str) -> str:
        if (not isinstance(value, str) or len(value) != 64
                or any(char not in "0123456789abcdef" for char in value)):
            raise ValueError(f"{name} must be a lowercase SHA-256 digest")
        return value

    @classmethod
    def _load_pinned_contract(cls, descriptor_path: Path, protocol_path: Path,
                              role_name: str, capability: str,
                              expected_descriptor_digest: str,
                              expected_protocol_digest: str) -> tuple[dict, dict]:
        expected_pin = cls._sha256_pin(expected_descriptor_digest,
                                       "expected descriptor digest")
        expected_protocol_pin = cls._sha256_pin(expected_protocol_digest,
                                                "expected protocol digest")
        descriptor = cls._read_pinned_json(descriptor_path)
        if digest(descriptor) != expected_pin:
            raise ValueError("pinned descriptor digest mismatch")
        if set(descriptor) != {"a2a_extension", "capability", "card_sha256",
                               "name", "reconcile", "role"}:
            raise ValueError("pinned descriptor schema mismatch")
        expected_declaration_role, expected_name = PINNED_DECLARATION_IDENTITY[
            (role_name, capability)]
        if (descriptor["role"] != expected_declaration_role
                or descriptor["capability"] != capability
                or descriptor["name"] != expected_name
                or descriptor["reconcile"] != "a2a-idempotent-resend"):
            raise ValueError("pinned descriptor role or capability mismatch")
        card_digest = cls._sha256_pin(descriptor["card_sha256"], "pinned card digest")
        extension = descriptor["a2a_extension"]
        if (not isinstance(extension, dict)
                or set(extension) != {"uri", "contract", "contract_digest"}
                or extension["uri"] != EXTENSION_URI
                or extension["contract"] != CONTRACT_NAME):
            raise ValueError("pinned A2A extension declaration mismatch")
        contract_digest = cls._sha256_pin(extension["contract_digest"],
                                           "pinned protocol digest")
        if contract_digest != expected_protocol_pin:
            raise ValueError("pinned descriptor protocol digest differs from the expected protocol pin")
        document = cls._read_pinned_json(protocol_path)
        if digest(document) != expected_protocol_pin:
            raise ValueError("pinned protocol document digest mismatch")
        expected_schema = contract_for(capability)
        expected_schema["request"]["data"].pop("optional_fields")
        if document != expected_schema:
            raise ValueError("pinned protocol role, capability, or schema mismatch")
        return descriptor, document

    def recover(self):
        raise Rejected("read-only usage owner cannot recover Tasks")

    def schedule(self, _task_id: str):
        raise Rejected("read-only usage owner cannot schedule Tasks")


class LedgerTaskStore(ProjectionTaskStore):
    def __init__(self, ledger: Ledger):
        self.ledger = ledger

    async def get(self, task_id, context=None):
        return self.ledger.task(task_id)

    async def save(self, task, context=None):
        if not self.ledger.task(task.id):
            raise Rejected("task has no committed action")


class Executor(AgentExecutor):
    async def execute(self, context, event_queue):
        raise NotImplementedError("nonblocking sends are handled by Handler")

    async def cancel(self, context, event_queue):
        raise InvalidParamsError(message="action cannot be cancelled")


class Handler(LegacyRequestHandler):
    def __init__(self, service: Service | ReadOnlyUsageService, card: AgentCard):
        super().__init__(Executor(), LedgerTaskStore(service.ledger), card)
        self.service = service
        self.send_lock = asyncio.Lock()

    async def on_message_send(self, params, context=None):
        if getattr(self.service, "read_only_usage", False):
            raise InvalidParamsError(message="read-only usage owner")
        try:
            command, _ = command_from_params(params)
            async with self.send_lock:
                task_id, created = self.service.ledger.accept(command, str(uuid4()),
                                  params.message.context_id or str(uuid4()))
                if created:
                    self.service.schedule(task_id)
                return self.service.ledger.task(task_id)
        except Rejected as error:
            raise InvalidParamsError(message=str(error)) from error


class ReadOnlyUsageMiddleware:
    """Permit safe owner reads and GetTask while returning HTTP 503 for writes."""

    PUBLIC_GET = {"/health", "/.well-known/agent-card.json"}
    PRIVATE_GET = {"/contract", "/usage/measurements"}

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        method, path = scope["method"], scope["path"]
        headers = dict(scope.get("headers", []))
        public_get = method in {"GET", "HEAD"} and path in self.PUBLIC_GET
        if not public_get and headers.get(b"authorization", b"").decode("latin-1") != TOKEN:
            response = JSONResponse({"error": "fixture authentication required"}, status_code=401)
            await response(scope, receive, send)
            return

        safe_get = (method == "GET" and
                    (path in self.PUBLIC_GET | self.PRIVATE_GET or
                     path.startswith("/fixture/actions/")))
        if safe_get or (method == "HEAD" and path in self.PUBLIC_GET):
            await self.app(scope, receive, send)
            return

        if method == "POST" and path == "/":
            messages, body = [], bytearray()
            while True:
                message = await receive()
                messages.append(message)
                if message["type"] == "http.disconnect":
                    break
                if message["type"] == "http.request":
                    body.extend(message.get("body", b""))
                    if len(body) > 1_048_576:
                        break
                    if not message.get("more_body", False):
                        break
            try:
                request = json.loads(body)
            except (UnicodeDecodeError, json.JSONDecodeError):
                request = None
            if (not isinstance(request, dict) or request.get("method") != a2a_v1.GET_TASK
                    or len(body) > 1_048_576):
                response = JSONResponse({"error": "read-only usage owner"}, status_code=503)
                await response(scope, receive, send)
                return

            replay_index = 0

            async def replay_receive():
                nonlocal replay_index
                if replay_index < len(messages):
                    message = messages[replay_index]
                    replay_index += 1
                    return message
                return await receive()

            await self.app(scope, replay_receive, send)
            return

        response = JSONResponse({"error": "read-only usage owner"}, status_code=503)
        await response(scope, receive, send)


def agent_card(role: str, capability: str, port: int, identity: str,
               contract: dict) -> AgentCard:
    extension = AgentExtension(uri=EXTENSION_URI, required=True,
                               params={"identity": identity, "contract": contract["name"],
                                       "contract_digest": digest(contract)})
    return AgentCard(name=f"{role.title()} report agent", description="Independent report agent",
        supported_interfaces=interfaces(f"http://127.0.0.1:{port}/"), version="1.0.0",
        default_input_modes=["application/json"], default_output_modes=["application/json"],
        capabilities=AgentCapabilities(streaming=False, extensions=[extension]),
        skills=[AgentSkill(id=capability, name=capability, description=f"{role} report work",
                           tags=[role])],
        **bearer_security())


def agent_card_digest(card: AgentCard) -> str:
    """Hash exactly the endpoint-free public AgentCard projection used by bindings."""
    return digest(card_pin_projection(card))


def create_app(state: Path, port: int, *, role: str, capability: str,
               model_provider: str = "scripted", model: str = DEFAULT_MODEL_ID,
               test_controls: bool = False, roles: dict | None = None,
               deadline_seconds: float = DEADLINE_SECONDS,
               read_only_usage: bool = False,
               pinned_descriptor_document: Path | None = None,
               protocol_document: Path | None = None,
               expected_descriptor_digest: str | None = None,
               expected_protocol_digest: str | None = None,
               expected_identity: str | None = None):
    if read_only_usage:
        if test_controls:
            raise ValueError("test controls are unavailable in read-only usage mode")
        if (pinned_descriptor_document is None or protocol_document is None
                or expected_descriptor_digest is None or expected_protocol_digest is None
                or expected_identity is None):
            raise ValueError("read-only usage requires pinned descriptor/protocol documents, both digests, and identity")
        service = ReadOnlyUsageService(
            state, role, capability, pinned_descriptor_document=pinned_descriptor_document,
            protocol_document=protocol_document,
            expected_descriptor_digest=expected_descriptor_digest,
            expected_protocol_digest=expected_protocol_digest,
            expected_identity=expected_identity)
        contract = service.contract
    else:
        if any(value is not None for value in (pinned_descriptor_document, protocol_document,
                                                expected_descriptor_digest,
                                                expected_protocol_digest, expected_identity)):
            raise ValueError("pinned read-only contract inputs are only valid in read-only mode")
        service = Service(state, role, capability, model_provider, model, test_controls,
                          roles, deadline_seconds)
        contract = contract_for(capability)
    card = agent_card(role, capability, port, service.ledger.identity, contract)
    if read_only_usage and agent_card_digest(card) != service.descriptor["card_sha256"]:
        raise ValueError("existing owner identity does not match the pinned AgentCard")
    app = build_app(card, Handler(service, card))
    app.state.model_agent_service = service

    if not read_only_usage:
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

    if read_only_usage:
        app.add_middleware(ReadOnlyUsageMiddleware)

    @app.get("/health")
    def health():
        inference_enabled = (not read_only_usage and service.provider in {
            "codex-subscription", "synthetic-loopback"})
        provider = None if read_only_usage else service.provider
        model_id = None
        reasoning_effort = None
        if inference_enabled:
            configured_model_id = service.model_id
            if (isinstance(configured_model_id, str) and len(configured_model_id) <= 256
                    and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/@_+-]*",
                                     configured_model_id)):
                model_id = configured_model_id
            reasoning_effort = DEFAULT_REASONING_EFFORT
        response = {"identity": service.ledger.identity, "incarnation": service.ledger.incarnation,
                    "role": "quality" if role == "quality" else "capability",
                    "capability": capability, "a2a_protocol": a2a_v1.PROTOCOL_VERSION,
                    "provider": provider, "model_id": model_id,
                    "reasoning_effort": reasoning_effort,
                    "inference_enabled": inference_enabled,
                    "read_only_usage": bool(read_only_usage)}
        return response

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

    @app.get("/usage/measurements")
    def usage_measurements(run_id: str | None = None, task_id: str | None = None,
                           assignment_id: str | None = None, attempt_id: str | None = None,
                           model_call_id: str | None = None, call_scope: str | None = None,
                           action_id: str | None = None):
        try:
            measurements = service.usage.list_measurements(
                run_id=run_id, task_id=task_id, assignment_id=assignment_id,
                attempt_id=attempt_id, model_call_id=model_call_id,
                call_scope=call_scope, action_id=action_id)
        except (OSError, sqlite3.Error):
            return JSONResponse({"error": "usage journal unavailable"}, status_code=503)
        return {"measurements": measurements}

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
    parser.add_argument("--model-provider",
                        choices=("codex-subscription", "synthetic-loopback", "scripted"))
    parser.add_argument("--model", default=DEFAULT_MODEL_ID)
    parser.add_argument("--test-controls", action="store_true")
    parser.add_argument("--read-only-usage", action="store_true",
                        help="serve existing usage records without Task recovery or model startup")
    parser.add_argument("--read-only-pin-document", type=Path,
                        help="local original digest-pinned closure descriptor")
    parser.add_argument("--read-only-protocol-document", type=Path,
                        help="local original protocol document pinned by the closure descriptor")
    parser.add_argument("--read-only-descriptor-digest",
                        help="expected lowercase SHA-256 digest of the closure descriptor")
    parser.add_argument("--read-only-protocol-digest",
                        help="expected lowercase SHA-256 digest of the original protocol document")
    parser.add_argument("--read-only-expected-identity",
                        help="expected identity from the existing owner pin")
    args = parser.parse_args()
    if not args.read_only_usage and args.model_provider is None:
        parser.error("--model-provider is required unless --read-only-usage is set")
    if args.read_only_usage and any(value is None for value in (
            args.read_only_pin_document, args.read_only_protocol_document,
            args.read_only_descriptor_digest,
            args.read_only_protocol_digest,
            args.read_only_expected_identity)):
        parser.error("read-only mode requires pinned descriptor/protocol documents, both digests, and identity")
    app = create_app(args.state, args.port, role=args.role, capability=args.capability,
                     model_provider=args.model_provider or "scripted", model=args.model,
                     test_controls=args.test_controls, read_only_usage=args.read_only_usage,
                     pinned_descriptor_document=args.read_only_pin_document,
                     protocol_document=args.read_only_protocol_document,
                     expected_descriptor_digest=args.read_only_descriptor_digest,
                     expected_protocol_digest=args.read_only_protocol_digest,
                     expected_identity=args.read_only_expected_identity)
    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
