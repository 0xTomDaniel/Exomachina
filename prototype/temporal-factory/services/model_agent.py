"""Independent, durable A2A report agent. No factory state is imported here.

The agent speaks A2A v1 only and cannot tell what kind of client calls it. It
exposes exactly the JSON-RPC endpoint and the well-known Agent Card:

- A request is an ordinary Message whose first text Part is the brief (a JSON
  object with a ``revision``); any further Parts are the inputs this agent
  works from (research results, a draft), each identified by its JSON
  ``kind``. Request metadata other than the optional budget extension entry is
  ignored and never stored or echoed.
- The result is one artifact whose single text Part is the work product
  itself (canonical JSON); there is no envelope.
- A resend of the same ``messageId`` returns the original Task; a reused
  ``messageId`` with a different payload is rejected.
- With the budget extension activated, the terminal Task reports ``incurred``
  tokens exactly as the model provider reported them; unknown fields are
  omitted, never zero.
- ``--test-controls`` declares a test-only stimulus extension used by
  qualification fixtures. Production cards never declare it.
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
from uuid import uuid4

import uvicorn
from a2a.server.agent_execution import AgentExecutor
from a2a.types import (AgentCapabilities, AgentCard, AgentExtension, AgentSkill,
                       Artifact, InvalidParamsError, Message, Part, Role, Task, TaskStatus)
from fastapi import Request
from fastapi.responses import JSONResponse
from google.protobuf.json_format import MessageToDict, ParseDict
from strands import Agent
from strands.models import Model

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from model_broker import DEFAULT_MODEL_ID, ModelBroker, PiBrokerModel  # noqa: E402
from agent_roles import ROLES, working_view  # noqa: E402
import a2a_extensions as ext  # noqa: E402
import a2a_v1  # noqa: E402
from a2a_v1_server import (LegacyRequestHandler, ProjectionTaskStore,  # noqa: E402
                           agent_message, bearer_security, build_app, card_pin_projection,
                           data_part, interfaces, part_content, part_data, task_state,
                           text_part)


TOKEN = "Bearer fixture-token"
EXTENSION_URI = ext.AGENT_URI
FAILURE_TEXT = "Agent work failed."
MAX_CALLS = 3
DEADLINE_SECONDS = 240
MINIMUM_BUDGET_TOKENS = 1
MINIMUM_BUDGET_SECONDS = 5.0
SCHEMA_VERSION = 2
_PROVIDER_TOKENS = {"inputTokens": "input", "outputTokens": "output",
                    "cacheReadTokens": "cache_read", "cacheWriteTokens": "cache_write",
                    "totalTokens": "total"}


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value: object) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def validate_role_capability(role_name: str, capability: str) -> None:
    allowed = {"research": {"packet_findings@1", "packet_risks@1"},
               "synthesis": {"report_synthesis@1"},
               "quality": {"report_quality_review@1"}}
    if role_name not in allowed or capability not in allowed[role_name]:
        raise ValueError("capability does not match role")


class Rejected(ValueError):
    pass


def request_from_params(params) -> dict:
    """An ordinary A2A v1 Message: the JSON brief Part, then input Parts."""
    message = params.message
    if not message.message_id:
        raise Rejected("messageId is required")
    if message.task_id:
        raise Rejected("this agent does not continue an existing Task")
    parts = message.parts
    if not parts or part_content(parts[0]) != "text" or not parts[0].text.strip():
        raise Rejected("the first Part must be the non-empty text brief")
    inputs = [a2a_v1.normalize_numbers(MessageToDict(part)) for part in parts[1:]]
    text = parts[0].text
    try:
        brief = json.loads(text)
    except ValueError as error:
        raise Rejected("the brief text must be a JSON object") from error
    if not isinstance(brief, dict) or not isinstance(brief.get("revision"), str):
        raise Rejected("the brief must be a JSON object with a revision")
    metadata = MessageToDict(params.metadata) if params.HasField("metadata") else {}
    entry = metadata.get(ext.BUDGET_URI) if isinstance(metadata, dict) else None
    if entry is not None and (not isinstance(entry, dict) or set(entry) != {"budget"}):
        raise Rejected("budget extension metadata must contain only budget")
    try:
        budget = ext.parse_budget(a2a_v1.normalize_numbers(entry["budget"]) if entry else None)
        ext.check_budget(budget, minimum_tokens=MINIMUM_BUDGET_TOKENS,
                         minimum_seconds=MINIMUM_BUDGET_SECONDS)
    except ext.BudgetError as error:
        raise Rejected(str(error)) from error
    return {"message_id": message.message_id, "context_id": message.context_id or None,
            "brief_text": text, "brief": brief, "inputs": inputs, "budget": budget,
            "blocking": not (params.HasField("configuration")
                             and params.configuration.return_immediately is True)}


class Ledger:
    def __init__(self, state: Path, *, test_controls: bool = False):
        state = Path(state)
        self.database = state / "model-agent.sqlite3"
        self.test_controls = test_controls
        state.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS identity (singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    id TEXT NOT NULL, incarnation INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS model_calls (id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL, session_id TEXT NOT NULL, provider TEXT NOT NULL,
                    model_id TEXT NOT NULL, live INTEGER NOT NULL, call_no INTEGER NOT NULL,
                    started_at REAL NOT NULL, ended_at REAL, outcome_kind TEXT);
            """)
            self._migrate(db)
            if test_controls:
                db.executescript("""
                    CREATE TABLE IF NOT EXISTS test_stimulus (id INTEGER PRIMARY KEY AUTOINCREMENT,
                        claim TEXT NOT NULL, revisions TEXT NOT NULL, bound_context_id TEXT,
                        armed_at REAL NOT NULL);
                    CREATE TABLE IF NOT EXISTS test_stimulus_log (id INTEGER PRIMARY KEY AUTOINCREMENT,
                        stimulus_id INTEGER NOT NULL, context_id TEXT NOT NULL,
                        revision TEXT NOT NULL, task_id TEXT NOT NULL, planted_text TEXT NOT NULL,
                        sha256_after TEXT NOT NULL, content_after TEXT NOT NULL,
                        UNIQUE(stimulus_id, task_id, revision));
                """)
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT id, incarnation FROM identity WHERE singleton=1").fetchone()
            if row:
                self.identity, self.incarnation = row["id"], row["incarnation"] + 1
                db.execute("UPDATE identity SET incarnation=? WHERE singleton=1", (self.incarnation,))
            else:
                self.identity, self.incarnation = str(uuid4()), 1
                db.execute("INSERT INTO identity VALUES (1,?,1)", (self.identity,))

    @staticmethod
    def _create_tasks(db, name: str = "tasks") -> None:
        db.execute(f"""CREATE TABLE IF NOT EXISTS {name} (task_id TEXT PRIMARY KEY,
            context_id TEXT NOT NULL, message_id TEXT NOT NULL UNIQUE,
            fingerprint TEXT NOT NULL, state TEXT NOT NULL, brief TEXT NOT NULL,
            budget TEXT, artifact TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL,
            inputs TEXT)""")

    def _migrate(self, db) -> None:
        """Forward migration to schema 2: only the agent's own Task identities.

        Schema 1 stored caller-supplied run, action and definition bindings with
        each Task, a run-bound test stimulus, and a caller-keyed usage journal.
        Those rows are rewritten without the caller fields; old Tasks keep
        their task/context IDs and gain a ``legacy:<task_id>`` message ID.
        """
        columns = {row["name"] for row in db.execute("PRAGMA table_info(tasks)")}
        if not columns:
            self._create_tasks(db)
        elif "message_id" not in columns:
            db.execute("BEGIN IMMEDIATE")
            self._create_tasks(db, "tasks_v2")
            for row in db.execute("SELECT * FROM tasks").fetchall():
                artifact = json.loads(row["artifact"]) if row["artifact"] else None
                if isinstance(artifact, dict):
                    artifact = {key: artifact[key] for key in
                                ("revision", "sha256", "author", "content") if key in artifact}
                db.execute("""INSERT INTO tasks_v2 (task_id,context_id,message_id,fingerprint,
                    state,brief,budget,artifact,created_at,updated_at)
                    VALUES (?,?,?,?,?,?,NULL,?,?,?)""",
                           (row["task_id"], row["context_id"], "legacy:" + row["task_id"],
                            row["fingerprint"], row["state"], row["brief"],
                            canonical(artifact) if artifact is not None else None,
                            row["created_at"], row["updated_at"]))
            db.execute("DROP TABLE tasks")
            db.execute("ALTER TABLE tasks_v2 RENAME TO tasks")
            for table in ("stimulus", "stimulus_log", "model_usage_measurements"):
                db.execute(f"DROP TABLE IF EXISTS {table}")
            db.execute("COMMIT")
        if "inputs" not in {row["name"] for row in db.execute("PRAGMA table_info(tasks)")}:
            db.execute("ALTER TABLE tasks ADD COLUMN inputs TEXT")
        call_columns = {row["name"] for row in db.execute("PRAGMA table_info(model_calls)")}
        for field in ext.TOKEN_FIELDS:
            if f"{field}_tokens" not in call_columns:
                db.execute(f"ALTER TABLE model_calls ADD COLUMN {field}_tokens INTEGER")
        db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")

    def connect(self):
        db = sqlite3.connect(self.database, timeout=15, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        return db

    def _transaction(self):
        return _Transaction(self.connect())

    def accept(self, request: dict, task_id: str) -> tuple[str, bool]:
        fingerprint = digest({"brief": request["brief_text"], "budget": request["budget"],
                              "context_id": request["context_id"],
                              **({"inputs": request["inputs"]} if request["inputs"] else {})})
        with self._transaction() as db:
            row = db.execute("SELECT task_id, fingerprint FROM tasks WHERE message_id=?",
                             (request["message_id"],)).fetchone()
            if row:
                if row["fingerprint"] != fingerprint:
                    raise Rejected("messageId reused with a different message")
                return row["task_id"], False
            context_id = request["context_id"] or str(uuid4())
            now = time.time()
            db.execute("""INSERT INTO tasks (task_id,context_id,message_id,fingerprint,state,
                brief,budget,artifact,created_at,updated_at,inputs)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                       (task_id, context_id, request["message_id"], fingerprint, "working",
                        request["brief_text"],
                        canonical(request["budget"]) if request["budget"] is not None else None,
                        None, now, now, canonical(request["inputs"])))
            if self.test_controls:
                # An armed test stimulus binds to the next context this agent
                # has never seen. Replays never reach this branch.
                known = db.execute("SELECT 1 FROM tasks WHERE context_id=? AND task_id<>? LIMIT 1",
                                   (context_id, task_id)).fetchone()
                if not known:
                    pending = db.execute("""SELECT id FROM test_stimulus
                        WHERE bound_context_id IS NULL ORDER BY id LIMIT 1""").fetchone()
                    if pending:
                        db.execute("UPDATE test_stimulus SET bound_context_id=? WHERE id=?",
                                   (context_id, pending["id"]))
        return task_id, True

    def row(self, task_id: str):
        db = self.connect()
        try:
            return db.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        finally:
            db.close()

    def incurred_tokens(self, task_id: str) -> dict[str, int]:
        db = self.connect()
        try:
            rows = db.execute("""SELECT * FROM model_calls WHERE task_id=? AND ended_at IS NOT NULL
                                 ORDER BY id""", (task_id,)).fetchall()
        finally:
            db.close()
        return ext.sum_reported_tokens({field: row[f"{field}_tokens"] for field in ext.TOKEN_FIELDS}
                                       for row in rows)

    def task(self, task_id: str, *, extensions=()) -> Task | None:
        row = self.row(task_id)
        if row is None:
            return None
        metadata = {"agent_identity": self.identity}
        if row["state"] in {"completed", "failed"} and ext.requested(extensions, ext.BUDGET_URI):
            metadata.update(ext.incurred_metadata(self.incurred_tokens(task_id)))
        artifact = json.loads(row["artifact"]) if row["artifact"] else None
        status_message = None
        if row["state"] == "failed":
            status_message = agent_message([text_part(FAILURE_TEXT)])
        inputs = json.loads(row["inputs"]) if row["inputs"] else []
        request = Message(message_id=row["message_id"], role=Role.ROLE_USER,
                          task_id=task_id, context_id=row["context_id"],
                          parts=[text_part(row["brief"], a2a_v1.JSON_MEDIA_TYPE),
                                 *(ParseDict(part, Part()) for part in inputs)])
        # The ledger keeps version-neutral state names; map them at the boundary.
        return Task(id=task_id, context_id=row["context_id"],
                    status=TaskStatus(state=task_state(row["state"]), message=status_message),
                    metadata=metadata, history=[request],
                    artifacts=[Artifact(artifact_id=artifact["sha256"],
                                        parts=[text_part(artifact["content"],
                                                         a2a_v1.JSON_MEDIA_TYPE)])]
                    if artifact else None)

    def finish(self, task_id: str, artifact: dict | None, stimulus: dict | None = None):
        with self._transaction() as db:
            if stimulus is not None:
                if artifact is None or artifact["sha256"] != stimulus["sha256_after"]:
                    raise ValueError("stimulus artifact digest mismatch")
                db.execute("""INSERT OR IGNORE INTO test_stimulus_log
                    (stimulus_id,context_id,revision,task_id,planted_text,sha256_after,content_after)
                    VALUES (?,?,?,?,?,?,?)""", tuple(stimulus[key] for key in (
                        "stimulus_id", "context_id", "revision", "task_id", "planted_text",
                        "sha256_after", "content_after")))
                logged = db.execute("""SELECT sha256_after,content_after FROM test_stimulus_log
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

    def working(self) -> list[str]:
        db = self.connect()
        try:
            return [row["task_id"] for row in db.execute(
                "SELECT task_id FROM tasks WHERE state='working' ORDER BY created_at,task_id")]
        finally:
            db.close()

    def call_start(self, task_id: str, session_id: str, provider: str, model_id: str,
                   live: bool) -> tuple[int, int]:
        with self._transaction() as db:
            call_no = db.execute("SELECT COUNT(*) FROM model_calls WHERE task_id=?", (task_id,)).fetchone()[0] + 1
            if call_no > MAX_CALLS:
                raise RuntimeError("model call budget exhausted")
            cursor = db.execute("""INSERT INTO model_calls
                (task_id,session_id,provider,model_id,live,call_no,started_at)
                VALUES (?,?,?,?,?,?,?)""", (task_id, session_id, provider, model_id,
                                             int(live), call_no, time.time()))
            return cursor.lastrowid, call_no

    def call_end(self, call_id: int, outcome: str, tokens: dict[str, int] | None = None):
        tokens = tokens or {}
        db = self.connect()
        try:
            db.execute(f"""UPDATE model_calls SET ended_at=?, outcome_kind=?,
                {", ".join(f"{field}_tokens=COALESCE(?, {field}_tokens)" for field in ext.TOKEN_FIELDS)}
                WHERE id=?""", (time.time(), outcome,
                                *(tokens.get(field) for field in ext.TOKEN_FIELDS), call_id))
        finally:
            db.close()

    def call_count(self, task_id: str) -> int:
        db = self.connect()
        try:
            return db.execute("SELECT COUNT(*) FROM model_calls WHERE task_id=?", (task_id,)).fetchone()[0]
        finally:
            db.close()

    def last_call_id(self, task_id: str) -> int:
        db = self.connect()
        try:
            return db.execute("SELECT id FROM model_calls WHERE task_id=? ORDER BY id DESC LIMIT 1",
                              (task_id,)).fetchone()[0]
        finally:
            db.close()

    # Test-only stimulus control (declared only with --test-controls).
    def arm(self, body: dict) -> dict:
        if not self.test_controls:
            raise Rejected("test controls are not enabled")
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
        with self._transaction() as db:
            if db.execute("SELECT 1 FROM test_stimulus WHERE bound_context_id IS NULL").fetchone():
                raise Rejected("a stimulus is already armed")
            cursor = db.execute("INSERT INTO test_stimulus (claim,revisions,armed_at) VALUES (?,?,?)",
                                (canonical(claim), canonical(revisions), time.time()))
        return {"armed": True, "stimulus_id": cursor.lastrowid}

    def logged_stimulus(self, task_id: str, revision: str) -> tuple[dict, dict] | None:
        db = self.connect()
        try:
            logged = db.execute("""SELECT * FROM test_stimulus_log
                WHERE task_id=? AND revision=?""", (task_id, revision)).fetchone()
        finally:
            db.close()
        if logged is None:
            return None
        rendered = logged["content_after"]
        if hashlib.sha256(rendered.encode()).hexdigest() != logged["sha256_after"]:
            raise ValueError("committed stimulus digest mismatch")
        return json.loads(rendered), {key: logged[key] for key in (
            "stimulus_id", "context_id", "revision", "task_id", "planted_text",
            "sha256_after", "content_after")}

    def apply_stimulus(self, task_id: str, context_id: str, revision: str,
                       content: dict) -> tuple[dict, dict | None]:
        recovered = self.logged_stimulus(task_id, revision)
        if recovered is not None:
            return recovered
        db = self.connect()
        try:
            row = db.execute("SELECT * FROM test_stimulus WHERE bound_context_id=? ORDER BY id LIMIT 1",
                             (context_id,)).fetchone()
        finally:
            db.close()
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
        return modified, {"stimulus_id": row["id"], "context_id": context_id,
                          "revision": revision, "task_id": task_id,
                          "planted_text": claim["text"],
                          "sha256_after": hashlib.sha256(rendered.encode()).hexdigest(),
                          "content_after": rendered}

    def stimulus_log(self) -> list[dict]:
        if not self.test_controls:
            raise Rejected("test controls are not enabled")
        db = self.connect()
        try:
            rows = db.execute("""SELECT stimulus_id,context_id,revision,task_id,planted_text,
                sha256_after FROM test_stimulus_log ORDER BY id""").fetchall()
        finally:
            db.close()
        return [dict(row) for row in rows]


class _Transaction:
    def __init__(self, db):
        self.db = db

    def __enter__(self):
        self.db.execute("BEGIN IMMEDIATE")
        return self.db

    def __exit__(self, kind, value, traceback):
        try:
            self.db.execute("COMMIT" if kind is None else "ROLLBACK")
        finally:
            self.db.close()
        return False


class ScriptedModel(Model):
    """Fixture provider. It reports input/output token estimates and nothing else."""

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
        prompt = canonical(messages) + (system_prompt or "")
        yield {"messageStart": {"role": "assistant"}}
        yield {"contentBlockStart": {"start": {}}}
        yield {"contentBlockDelta": {"delta": {"text": self.reply}}}
        yield {"contentBlockStop": {}}
        yield {"messageStop": {"stopReason": "end_turn"}}
        yield {"metadata": {"usage": {"inputTokens": len(prompt) // 4 + 1,
                                      "outputTokens": len(self.reply) // 4 + 1},
                            "metrics": {"latencyMs": 0}}}


def _reported_tokens(event: object) -> dict[str, int] | None:
    usage = ((event or {}).get("metadata") or {}).get("usage") if isinstance(event, dict) else None
    if not isinstance(usage, dict):
        return None
    return {field: usage[name] for name, field in _PROVIDER_TOKENS.items()
            if type(usage.get(name)) is int and usage[name] >= 0}


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
        tokens: dict[str, int] | None = None
        outcome = "completed"
        try:
            async with asyncio.timeout(remaining):
                async for event in self.inner.stream(messages, tool_specs=tool_specs,
                                                     system_prompt=system_prompt, **kwargs):
                    reported = _reported_tokens(event)
                    if reported is not None:
                        tokens = reported
                    yield event
        except TimeoutError:
            outcome = "deadline"
            raise
        except BaseException:
            outcome = "error"
            raise
        finally:
            self.ledger.call_end(call_id, outcome, tokens)


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
        self.ledger = Ledger(state, test_controls=test_controls)
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
        return self.running.get(task_id)

    def recover(self):
        for task_id in self.ledger.working():
            self.schedule(task_id)

    def _token_limit_reached(self, task_id: str, budget: dict | None) -> bool:
        limit = ((budget or {}).get("tokens") or {}).get("limit")
        if limit is None or self.ledger.call_count(task_id) == 0:
            return False
        used = self.ledger.incurred_tokens(task_id)
        if "total" in used:
            spent = used["total"]
        elif "input" in used and "output" in used:
            spent = used["input"] + used["output"]
        else:
            # Unknown usage cannot prove the limit is unspent; one call is the bound.
            return True
        return spent >= limit

    async def work(self, task_id: str):
        row = self.ledger.row(task_id)
        if row is None or row["state"] != "working":
            return
        try:
            # The working view joins the brief with the input Parts received.
            brief = working_view(json.loads(row["brief"]),
                                 json.loads(row["inputs"]) if row["inputs"] else [])
            budget = json.loads(row["budget"]) if row["budget"] else None
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
                    budget_deadline = ext.deadline_epoch(budget)
                    if budget_deadline is not None:
                        remaining = min(remaining, budget_deadline - time.time())
                    deadline = time.monotonic() + max(0, remaining)
                    content = None
                    while (self.ledger.call_count(task_id) < self.max_calls
                           and time.monotonic() < deadline
                           and not self._token_limit_reached(task_id, budget)):
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
                            self.ledger.call_end(self.ledger.last_call_id(task_id), "validation-error")
                    if content is None:
                        raise RuntimeError("model output budget exhausted")
                if self.test_controls:
                    content, stimulus = self.ledger.apply_stimulus(task_id, row["context_id"],
                                                                   brief["revision"], content)
            rendered = canonical(content)
            sha256 = hashlib.sha256(rendered.encode()).hexdigest()
            artifact = {"revision": brief["revision"], "sha256": sha256, "content": rendered}
            self.ledger.finish(task_id, artifact, stimulus)
        except asyncio.CancelledError:
            # A stopped process leaves the committed Task working for recovery.
            raise
        except Exception:
            self.ledger.finish(task_id, None)


class LedgerTaskStore(ProjectionTaskStore):
    def __init__(self, ledger: Ledger):
        self.ledger = ledger

    async def get(self, task_id, context=None):
        return self.ledger.task(task_id, extensions=getattr(context, "requested_extensions", ()))

    async def save(self, task, context=None):
        if not self.ledger.row(task.id):
            raise Rejected("task has no committed message")


class Executor(AgentExecutor):
    async def execute(self, context, event_queue):
        raise NotImplementedError("nonblocking sends are handled by Handler")

    async def cancel(self, context, event_queue):
        raise InvalidParamsError(message="this Task cannot be cancelled")


class Handler(LegacyRequestHandler):
    def __init__(self, service: Service, card: AgentCard):
        super().__init__(Executor(), LedgerTaskStore(service.ledger), card)
        self.service = service
        self.send_lock = asyncio.Lock()

    def _test_control(self, params) -> Message:
        parts = params.message.parts
        if len(parts) != 1 or part_content(parts[0]) != "data":
            raise Rejected("a test control message carries one data Part")
        control = part_data(parts[0])
        if isinstance(control, dict) and set(control) == {"arm"}:
            return agent_message([data_part(self.service.ledger.arm(control["arm"]))])
        if control == {"stimulus_log": True}:
            return agent_message([data_part({"stimulus_log": self.service.ledger.stimulus_log()})])
        raise Rejected("unknown test control")

    async def on_message_send(self, params, context=None):
        extensions = getattr(context, "requested_extensions", ())
        try:
            if self.service.test_controls and ext.requested(extensions, ext.TEST_STIMULUS_URI):
                return self._test_control(params)
            request = request_from_params(params)
            async with self.send_lock:
                task_id, created = self.service.ledger.accept(request, str(uuid4()))
                running = self.service.schedule(task_id) if created else None
            if request["blocking"]:
                running = running or self.service.running.get(task_id)
                if running is not None:
                    await asyncio.wait({running}, timeout=self.service.deadline_seconds)
            return self.service.ledger.task(task_id, extensions=extensions)
        except Rejected as error:
            raise InvalidParamsError(message=str(error)) from error


def agent_card(role: str, capability: str, port: int, identity: str, *,
               test_controls: bool = False) -> AgentCard:
    extensions = [
        AgentExtension(uri=ext.AGENT_URI, required=False,
                       description="Stable agent identity and messageId resend rule",
                       params={"identity": identity, "resend": ext.RESEND_RULE}),
        AgentExtension(uri=ext.BUDGET_URI, required=False,
                       description="Optional budget; reports incurred tokens as the provider reported them"),
    ]
    if test_controls:
        extensions.append(AgentExtension(uri=ext.TEST_STIMULUS_URI, required=False,
                                         description="Test-only stimulus control"))
    return AgentCard(name=f"{role.title()} report agent", description="Independent report agent",
        supported_interfaces=interfaces(f"http://127.0.0.1:{port}/"), version="2.0.0",
        default_input_modes=["text/plain", "application/json"],
        default_output_modes=["application/json"],
        capabilities=AgentCapabilities(streaming=False, extensions=extensions),
        skills=[AgentSkill(id=capability, name=capability, description=f"{role} report work",
                           tags=[role])],
        **bearer_security())


def agent_card_digest(card: AgentCard) -> str:
    """Hash exactly the endpoint-free public AgentCard projection used by pins."""
    return digest(card_pin_projection(card))


def create_app(state: Path, port: int, *, role: str, capability: str,
               model_provider: str = "scripted", model: str = DEFAULT_MODEL_ID,
               test_controls: bool = False, roles: dict | None = None,
               deadline_seconds: float = DEADLINE_SECONDS):
    service = Service(state, role, capability, model_provider, model, test_controls,
                      roles, deadline_seconds)
    card = agent_card(role, capability, port, service.ledger.identity,
                      test_controls=test_controls)
    app = build_app(card, Handler(service, card))
    app.state.model_agent_service = service

    @app.on_event("startup")
    async def recover():
        service.recover()

    @app.middleware("http")
    async def fixture_auth(request: Request, call_next):
        if request.url.path == "/.well-known/agent-card.json":
            return await call_next(request)
        if request.headers.get("authorization") != TOKEN:
            return JSONResponse({"error": "fixture authentication required"}, status_code=401)
        return await call_next(request)

    return app


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--role", required=True, choices=("research", "synthesis", "quality"))
    parser.add_argument("--capability", required=True)
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--model-provider", required=True,
                        choices=("codex-subscription", "synthetic-loopback", "scripted"))
    parser.add_argument("--model", default=DEFAULT_MODEL_ID)
    parser.add_argument("--test-controls", action="store_true",
                        help="declare the test-only stimulus extension (qualification only)")
    args = parser.parse_args()
    app = create_app(args.state, args.port, role=args.role, capability=args.capability,
                     model_provider=args.model_provider, model=args.model,
                     test_controls=args.test_controls)
    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
