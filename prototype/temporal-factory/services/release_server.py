"""Release receiver: an ordinary A2A v1 agent with ``output: none``.

A client delivers one document as a single Message Part (text, raw bytes or
structured data) carrying a ``mediaType``. The agent records the exact bytes it
received and completes the Task with one receipt (a data-part artifact): the
receipt id, the sha256 and byte length of the delivered bytes, the acceptance
time and the outcome. It knows nothing about its caller: no run, assignment,
attempt, action, definition or factory identifier crosses the wire or reaches
its state, which is its own SQLite store keyed by its own Task and message ids.

Modes (each expressed only through A2A):

- ``participating``: A2A ``messageId`` is the idempotency key. A repeated
  identical SendMessage returns the original Task and receipt; reuse of a
  ``messageId`` with different content is rejected. Tasks stay readable
  through GetTask.
- ``opaque``: a non-participating receiver. Every SendMessage is a new
  delivery effect, no Task is retained, and the completed Task carries no
  receipt, so a caller can never confirm an exact delivery.

A Message the receiver will not deliver (no Part, several Parts, a missing
``mediaType``, a URL Part, oversize content) yields a ``rejected`` Task with a
descriptive status message. The only HTTP routes are the A2A JSON-RPC endpoint
and the Agent Card.
"""
from __future__ import annotations

import argparse
import asyncio
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
from uuid import uuid4

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from google.protobuf.json_format import MessageToDict
from a2a.server.agent_execution import AgentExecutor
from a2a.types import (AgentCapabilities, AgentCard, AgentSkill, Artifact, InvalidParamsError,
                       Message, Role, Task, TaskNotFoundError, TaskStatus,
                       UnsupportedOperationError)

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import a2a_v1  # noqa: E402
from a2a_v1_server import (LegacyRequestHandler, ProjectionTaskStore,  # noqa: E402
                           bearer_security, build_app, data_part, interfaces,
                           part_content, task_state, text_part)

TOKEN = "Bearer fixture-token"
MODES = ("participating", "opaque")
SKILL_ID = "release@1"
# Skill tags are the A2A-visible statement of what the receiver promises.
PARTICIPATING_TAGS = ["delivery", "release", "receipt", "message-id-idempotent", "get-task"]
OPAQUE_TAGS = ["delivery", "release"]
INPUT_MODES = ["application/json", "text/markdown", "text/plain"]
MAX_BYTES = 1_000_000
RECEIPT_FIELDS = ("receipt_id", "sha256", "byte_length", "media_type", "accepted_at", "outcome")


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def delivered_bytes(part) -> tuple[bytes | None, str | None]:
    """The exact bytes one Part delivers, or a rejection reason."""
    kind = part_content(part)
    if kind == "text":
        return part.text.encode("utf-8"), None
    if kind == "raw":
        return bytes(part.raw), None
    if kind == "data":
        value = a2a_v1.normalize_numbers(MessageToDict(part.data))
        return canonical(value).encode("utf-8"), None
    if kind == "url":
        return None, "URL parts are not fetched; deliver the document bytes inline"
    return None, "the Part carries no content"


def message_fingerprint(message: Message) -> str:
    """Digest of the delivered content of one Message, for messageId reuse checks."""
    value = MessageToDict(message, preserving_proto_field_name=True)
    value.pop("message_id", None)
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


class Store:
    """The receiver's own durable state, keyed by its own Task and message ids."""

    def __init__(self, state: Path, mode: str):
        if mode not in MODES:
            raise ValueError("unknown receiver mode")
        state.mkdir(parents=True, exist_ok=True)
        self.database = state / "release.sqlite3"
        self.mode = mode
        with self.connection() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS identity (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    id TEXT NOT NULL, mode TEXT NOT NULL, incarnation INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS deliveries (
                    task_id TEXT PRIMARY KEY, context_id TEXT NOT NULL,
                    message_id TEXT UNIQUE, fingerprint TEXT NOT NULL,
                    state TEXT NOT NULL, status_text TEXT NOT NULL,
                    receipt_id TEXT UNIQUE, media_type TEXT, sha256 TEXT,
                    byte_length INTEGER, accepted_at TEXT NOT NULL,
                    sends INTEGER NOT NULL, effect_count INTEGER NOT NULL);
            """)
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM identity WHERE singleton=1").fetchone()
            if row:
                if row["mode"] != mode:
                    raise ValueError("receiver state mode mismatch")
                self.identity, self.incarnation = row["id"], row["incarnation"] + 1
                db.execute("UPDATE identity SET incarnation=? WHERE singleton=1",
                           (self.incarnation,))
            else:
                self.identity, self.incarnation = str(uuid4()), 1
                db.execute("INSERT INTO identity VALUES (1, ?, ?, 1)", (self.identity, mode))
            db.commit()

    @contextmanager
    def connection(self):
        db = sqlite3.connect(self.database, timeout=15, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        try:
            yield db
        finally:
            db.close()

    def deliver(self, message: Message) -> dict:
        """Commit one delivery (or rejection) before any response is produced."""
        if message.task_id:
            raise InvalidParamsError(
                message="release Tasks are terminal; send a new Message without taskId")
        if not message.message_id:
            raise InvalidParamsError(message="A2A messageId is required")
        fingerprint = message_fingerprint(message)
        content, reason, media_type = None, None, None
        if len(message.parts) != 1:
            reason = "deliver exactly one Part carrying the document"
        else:
            part = message.parts[0]
            media_type = part.media_type or None
            content, reason = delivered_bytes(part)
            if reason is None and not media_type:
                reason = "the delivered Part must declare its mediaType"
            elif reason is None and len(content) > MAX_BYTES:
                reason = f"the delivered document exceeds {MAX_BYTES} bytes"
        participating = self.mode == "participating"
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            prior = (db.execute("SELECT * FROM deliveries WHERE message_id=?",
                                (message.message_id,)).fetchone() if participating else None)
            if prior is not None:
                if prior["fingerprint"] != fingerprint:
                    db.rollback()
                    raise InvalidParamsError(
                        message="messageId reused with different content; send a new messageId")
                db.execute("UPDATE deliveries SET sends=sends+1 WHERE task_id=?",
                           (prior["task_id"],))
                db.commit()
                return dict(prior)
            row = {"task_id": str(uuid4()),
                   "context_id": message.context_id or str(uuid4()),
                   "message_id": message.message_id if participating else None,
                   "fingerprint": fingerprint, "accepted_at": _now(), "sends": 1}
            if reason is not None:
                row.update(state="rejected", status_text="Delivery rejected: " + reason,
                           receipt_id=None, media_type=media_type, sha256=None,
                           byte_length=None, effect_count=0)
            else:
                row.update(state="completed", receipt_id=str(uuid4()), media_type=media_type,
                           sha256=hashlib.sha256(content).hexdigest(),
                           byte_length=len(content), effect_count=1,
                           status_text=(f"Delivered {len(content)} bytes of {media_type}."
                                        if participating else
                                        f"Delivered {len(content)} bytes of {media_type}; this "
                                        "receiver issues no receipt and keeps no Task."))
            db.execute("""INSERT INTO deliveries (task_id, context_id, message_id, fingerprint,
                state, status_text, receipt_id, media_type, sha256, byte_length, accepted_at,
                sends, effect_count) VALUES (:task_id, :context_id, :message_id, :fingerprint,
                :state, :status_text, :receipt_id, :media_type, :sha256, :byte_length,
                :accepted_at, :sends, :effect_count)""", row)
            db.commit()
        return row

    def row(self, task_id: str) -> dict | None:
        if self.mode != "participating":
            return None  # The opaque receiver keeps no readable Task.
        with self.connection() as db:
            value = db.execute("SELECT * FROM deliveries WHERE task_id=?", (task_id,)).fetchone()
        return None if value is None else dict(value)

    def task(self, row: dict) -> Task:
        status = TaskStatus(state=task_state(row["state"]), message=Message(
            message_id=f"{row['task_id']}:status", role=Role.ROLE_AGENT,
            task_id=row["task_id"], context_id=row["context_id"],
            parts=[text_part(row["status_text"])]))
        task = Task(id=row["task_id"], context_id=row["context_id"], status=status)
        if row["state"] == "completed" and self.mode == "participating":
            receipt = {"receipt_id": row["receipt_id"], "sha256": row["sha256"],
                       "byte_length": row["byte_length"], "media_type": row["media_type"],
                       "accepted_at": row["accepted_at"], "outcome": "delivered"}
            task.artifacts.append(Artifact(artifact_id=row["receipt_id"],
                                           name="delivery-receipt",
                                           parts=[data_part(receipt)]))
        return task


class ReceiptTaskStore(ProjectionTaskStore):
    def __init__(self, store: Store):
        self.store = store

    async def get(self, task_id, context=None):
        row = self.store.row(task_id)
        return None if row is None else self.store.task(row)

    async def save(self, task, context=None):
        raise UnsupportedOperationError(message="release Tasks are written only by delivery")


class Executor(AgentExecutor):
    async def execute(self, context, event_queue):
        raise UnsupportedOperationError(message="delivery is handled by SendMessage")

    async def cancel(self, context, event_queue):
        raise UnsupportedOperationError(message="release Tasks are terminal")


class Handler(LegacyRequestHandler):
    def __init__(self, store: Store, task_store: ReceiptTaskStore, card: AgentCard):
        super().__init__(Executor(), task_store, card)
        self.store = store
        self._lock = asyncio.Lock()

    async def on_message_send(self, params, context=None):
        async with self._lock:
            row = await asyncio.to_thread(self.store.deliver, params.message)
        return self.store.task(row)

    async def on_get_task(self, params, context=None):
        row = self.store.row(params.id)
        if row is None:
            raise TaskNotFoundError(message="unknown release Task")
        return self.store.task(row)


def agent_card(port: int, mode: str) -> AgentCard:
    participating = mode == "participating"
    description = ("Records one delivered document per Task and returns a receipt with the "
                   "sha256 and byte length of the exact delivered bytes. A repeated identical "
                   "Message (same messageId) returns the original Task; a reused messageId "
                   "with different content is rejected." if participating else
                   "Records each delivered document. Issues no receipt, keeps no Task and "
                   "does not deduplicate repeated Messages.")
    return AgentCard(
        name="Release receiver", description=description,
        supported_interfaces=interfaces(f"http://127.0.0.1:{port}/"), version="1.0.0",
        default_input_modes=INPUT_MODES, default_output_modes=["application/json"],
        capabilities=AgentCapabilities(streaming=False),
        skills=[AgentSkill(id=SKILL_ID, name="Release delivery", description=description,
                           tags=PARTICIPATING_TAGS if participating else OPAQUE_TAGS,
                           input_modes=INPUT_MODES, output_modes=["application/json"])],
        **bearer_security(),
    )


def create_app(state: Path, port: int, mode: str):
    store = Store(state, mode)
    card = agent_card(port, mode)
    # No FastAPI documentation routes: the JSON-RPC endpoint and the Agent
    # Card are the only routes this agent serves.
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    app = build_app(card, Handler(store, ReceiptTaskStore(store), card), app=app)

    @app.middleware("http")
    async def fixture_auth(request: Request, call_next):
        if request.url.path == "/.well-known/agent-card.json":
            return await call_next(request)
        if request.headers.get("authorization") != TOKEN:
            return JSONResponse({"error": "fixture authentication required"}, status_code=401)
        return await call_next(request)

    app.state.release_store = store
    return app


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--mode", choices=MODES, required=True)
    args = parser.parse_args()
    uvicorn.run(create_app(args.state, args.port, args.mode), host="127.0.0.1",
                port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
