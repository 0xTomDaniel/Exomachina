"""Bounded Effect-backed Director role using the same S2 Strands/A2A harness path.

The Effect helper owns graph progress. This Adapter owns only Director identity,
stable run/task correlation, and local HTTP bridging. Model reasoning is a fixture.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sqlite3
import sys
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

S2 = Path(os.environ.get("EXO_S2_ROOT", Path(__file__).resolve().parents[1].parent / "s2"))
sys.path.insert(0, str(S2))

import uvicorn
from fastapi.responses import JSONResponse
from a2a.server.apps import A2AFastAPIApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.types import AgentCard, AgentCapabilities, AgentSkill
from strands import Agent

from harness import FactoryPlugin, Rejected, ToolCallingModelFixture, canonical
from server import ACTOR, HarnessExecutor, LedgerTaskStore


class EffectDirectorHarness:
    """One persistent Director identity, with no graph scheduler of its own."""

    def __init__(self, state: Path, effect_url: str):
        self.state = state
        self.state.mkdir(parents=True, exist_ok=True)
        self.effect_url = effect_url.rstrip("/")
        self.path = state / "director.sqlite3"
        self.role = "factory"
        self.organization = "org-fixture"
        self.kestra_config = None
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS identity (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    id TEXT NOT NULL, incarnation INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY, digest TEXT NOT NULL, command_key TEXT NOT NULL UNIQUE);
                CREATE TABLE IF NOT EXISTS aliases (
                    task_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, context_id TEXT NOT NULL);
            """)
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM identity WHERE singleton=1").fetchone()
            if row:
                self.identity, self.incarnation = row["id"], row["incarnation"] + 1
                db.execute("UPDATE identity SET incarnation=? WHERE singleton=1", (self.incarnation,))
            else:
                self.identity, self.incarnation = str(uuid4()), 1
                db.execute("INSERT INTO identity VALUES (1,?,1)", (self.identity,))

    def connect(self):
        db = sqlite3.connect(self.path, timeout=15)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        return db

    def effect(self, route: str, body: dict):
        request = Request(self.effect_url + route, data=json.dumps(body).encode(),
                          headers={"content-type": "application/json"})
        try:
            with urlopen(request, timeout=20) as response:
                return json.load(response)
        except HTTPError as error:
            reason = json.load(error)
            raise Rejected(f"Effect {error.code}: {reason.get('error')}") from error

    def _check_owner(self, db):
        row = db.execute("SELECT incarnation FROM identity WHERE singleton=1").fetchone()
        if row[0] != self.incarnation:
            raise Rejected("stale Director incarnation")

    def _alias(self, db, delivery, run_id):
        if delivery:
            db.execute("INSERT OR IGNORE INTO aliases VALUES (?,?,?)",
                       (delivery[0], run_id, delivery[1]))

    def _project(self, run_id: str, digest: str):
        result = self.effect("/poll", {"id": run_id})
        ledger = self.effect("/ledger", {"id": run_id})
        row = ledger["run"]
        if row["digest"] != digest:
            raise Rejected("Director run digest changed")
        native = result["state"]
        state = "completed" if native == "Complete" and result.get("exit") == "Success" \
            and ledger["delivery"] else "failed" if native == "Complete" else \
            "input-required" if native == "Suspended" else "working"
        accepted = None
        if state == "completed":
            artifact = json.loads(row["artifact"])
            accepted = {"revision": row["accepted_revision"],
                        "sha256": row["accepted_sha256"],
                        "reviewer": row["accepted_by"],
                        "content": artifact["content"], "definition": digest}
        return {"id": run_id, "owner": self.identity, "organization": self.organization,
                "definition": digest, "state": state, "accepted_output": accepted,
                "engine": {"kind": "effect", "native_state": native}}

    def command(self, command: dict, actor="director", delivery=None):
        op = command.get("op")
        if actor != "director" and op != "inspect":
            raise Rejected("unauthorized Director mutation")
        run_id = command.get("run_id")
        if not isinstance(run_id, str) or not run_id:
            raise Rejected("run_id required")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self._check_owner(db)
            if op == "start":
                digest, key = command.get("digest"), command.get("key")
                if not isinstance(digest, str) or not isinstance(key, str) or not key:
                    raise Rejected("digest and stable start key required")
                row = db.execute("SELECT * FROM runs WHERE id=? OR command_key=?",
                                 (run_id, key)).fetchone()
                if row and (row["id"] != run_id or row["digest"] != digest or row["command_key"] != key):
                    raise Rejected("Director start binding conflict")
                if not row:
                    db.execute("INSERT INTO runs VALUES (?,?,?)", (run_id, digest, key))
            else:
                row = db.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
                if not row:
                    raise Rejected("unknown Director run")
                digest = row["digest"]
                if command.get("digest", digest) != digest:
                    raise Rejected("Director digest mismatch")
            self._alias(db, delivery, run_id)
        if op == "start":
            # Intent and A2A alias precede the cross-process call; replayed
            # starts converge through Effect's stable run binding.
            self.effect("/start", {"id": run_id, "digest": digest})
        elif op == "decide":
            ledger = self.effect("/ledger", {"id": run_id})["run"]
            if not ledger["quality_accepted"]:
                raise Rejected("independent Quality verdict required")
            if not ledger["accepted_revision"]:
                # Trial trust boundary: this forwards a ledger identity string,
                # not a credential or signed Quality authorization.
                self.effect("/accept", {"id": run_id, "digest": digest,
                                        "revision": ledger["revision"],
                                        "sha256": ledger["sha256"],
                                        "actor": ledger["reviewer"]})
            self.effect("/flush", {"id": run_id, "digest": digest})
        elif op != "inspect":
            raise Rejected("unsupported Director command")
        return self._project(run_id, digest)

    def task_record(self, task_id):
        with self.connect() as db:
            self._check_owner(db)
            row = db.execute("""SELECT a.context_id,r.id,r.digest FROM aliases a
                                JOIN runs r ON r.id=a.run_id WHERE a.task_id=?""",
                             (task_id,)).fetchone()
        if not row:
            return None
        return self._project(row["id"], row["digest"]), row["context_id"]

    def bind_task(self, task_id, run_id, context_id):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self._check_owner(db)
            self._alias(db, (task_id, context_id), run_id)

    async def invoke(self, command, actor="director", delivery=None):
        agent = Agent(name="Exomachina Effect Director fixture",
                      model=ToolCallingModelFixture(),
                      plugins=[FactoryPlugin(self, actor, delivery)],
                      callback_handler=None)
        result = await agent.invoke_async(canonical(command))
        return json.loads(str(result))


def create_app(state: Path, effect_url: str, port: int):
    harness = EffectDirectorHarness(state, effect_url)
    store = LedgerTaskStore(harness)
    card = AgentCard(name="Exomachina Effect Director fixture",
                     description="Strands Director backed by a bundled local Effect helper",
                     url=f"http://127.0.0.1:{port}/", version="0.0.1",
                     protocol_version="0.3.0", default_input_modes=["application/json"],
                     default_output_modes=["application/json"],
                     capabilities=AgentCapabilities(streaming=False),
                     skills=[AgentSkill(id="factory", name="factory",
                                        description="Start, inspect, decide a pinned factory run",
                                        tags=["fixture", "effect"])],
                     security_schemes={"fixtureBearer": {"type": "http", "scheme": "bearer"}},
                     security=[{"fixtureBearer": []}])
    app = A2AFastAPIApplication(card, DefaultRequestHandler(HarnessExecutor(harness, store), store)).build()

    @app.middleware("http")
    async def fixture_auth(request, call_next):
        if request.url.path in {"/health", "/.well-known/agent-card.json"}:
            return await call_next(request)
        actors = {"Bearer director-test-token": "director",
                  "Bearer observer-test-token": "observer"}
        actor = actors.get(request.headers.get("authorization"))
        if actor is None:
            return JSONResponse({"error": "fixture authentication required"}, status_code=401)
        token = ACTOR.set(actor)
        try:
            return await call_next(request)
        finally:
            ACTOR.reset(token)

    @app.get("/health")
    def health():
        return {"identity": harness.identity, "incarnation": harness.incarnation,
                "role": "factory", "engine": "effect"}

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--effect", required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    uvicorn.run(create_app(args.state, args.effect, args.port), host="127.0.0.1",
                port=args.port, log_level="warning")
