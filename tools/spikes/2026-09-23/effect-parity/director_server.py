"""Post-freeze Strands/A2A Director facade for the frozen Effect graph helper.

Effect owns parent and nested-child progress. This adapter owns persistent A2A
Task correlation and authorization. Model reasoning remains a fixture.
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

S2 = Path(os.environ.get("EXO_S2_ROOT", Path(__file__).resolve().parents[2] / "2026-09-22" / "s2"))
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
        self.decision_token = "director-a2a-test-token"
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS identity (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    id TEXT NOT NULL, incarnation INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY, digest TEXT NOT NULL, command_key TEXT NOT NULL UNIQUE,
                    input_json TEXT NOT NULL);
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
        parent = result["run"]
        ledger = self.effect("/ledger", {"id": run_id})["run"]
        if ledger["package_digest"] != digest:
            raise Rejected("Director package digest changed")
        native = result["state"]
        child_id = parent.get("child_id")
        child = self.effect("/poll", {"id": child_id}) if child_id else None
        child_run = child["run"] if child else None
        completed = native == "Complete" and result.get("exit") == "Success"
        state = "completed" if completed else "failed" if native == "Complete" else \
            "input-required" if child_run and child_run["phase"] == "awaiting-director" else "working"
        accepted = None
        business_status = result.get("value", {}).get("status") if completed else None
        if completed and business_status == "accepted":
            child_ledger = self.effect("/ledger", {"id": child_id})["run"]
            artifact = json.loads(child_ledger["artifact_json"])
            acceptance = result["value"]["acceptance"]
            accepted = {"revision": acceptance["revision"],
                        "sha256": acceptance["sha256"],
                        "reviewer": acceptance["quality_identity"],
                        "content": artifact["content"], "definition": digest,
                        "child_definition": child_ledger["digest"],
                        "release_receipt": result["value"]["receipt"]}
        return {"id": run_id, "owner": self.identity, "organization": self.organization,
                "definition": digest, "state": state, "accepted_output": accepted,
                "child": child_id,
                "engine": {"kind": "effect", "native_state": native,
                           "business_status": business_status,
                           "child_phase": child_run and child_run["phase"],
                           "child_revision": child_run and child_run["revision"]}}

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
                digest, key = command.get("package_digest"), command.get("key")
                if not isinstance(digest, str) or not isinstance(key, str) or not key:
                    raise Rejected("package digest and stable start key required")
                resolve_after = command.get("resolution_after_repairs")
                if type(resolve_after) is not int or not 0 <= resolve_after <= 3:
                    raise Rejected("bounded resolution_after_repairs required")
                run_input = {"resolution_after_repairs": resolve_after,
                             "wait_seconds": 300,
                             "director": {"identity": self.identity,
                                          "token": self.decision_token, "epoch": 1}}
                input_json = canonical(run_input)
                row = db.execute("SELECT * FROM runs WHERE id=? OR command_key=?",
                                 (run_id, key)).fetchone()
                if row and (row["id"] != run_id or row["digest"] != digest or
                            row["command_key"] != key or row["input_json"] != input_json):
                    raise Rejected("Director start binding conflict")
                if not row:
                    db.execute("INSERT INTO runs VALUES (?,?,?,?)", (run_id, digest, key, input_json))
            else:
                row = db.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
                if not row:
                    raise Rejected("unknown Director run")
                digest = row["digest"]
                if command.get("package_digest", digest) != digest:
                    raise Rejected("Director package digest mismatch")
            self._alias(db, delivery, run_id)
        if op == "start":
            # Intent and A2A alias precede the cross-process call; replayed
            # starts converge through Effect's stable run binding.
            self.effect("/start", {"id": run_id, "package_digest": digest,
                                   "input": json.loads(input_json)})
        elif op == "decide":
            parent = self.effect("/poll", {"id": run_id})["run"]
            child_id = parent.get("child_id")
            if not child_id:
                raise Rejected("no nested child for Director decision")
            child = self.effect("/poll", {"id": child_id})["run"]
            if child["phase"] != "awaiting-director":
                raise Rejected("child has no current Director wait")
            if (command.get("child_id") != child_id or
                    command.get("revision") != child["revision"] or
                    command.get("sha256") != child["sha256"] or
                    command.get("child_digest") != child["digest"]):
                raise Rejected("stale or wrong-child Director decision")
            self.effect("/decide", {"id": child_id, "action": "abort",
                                    "actor": self.identity, "token": self.decision_token,
                                    "epoch": 1, "digest": child["digest"],
                                    "revision": child["revision"],
                                    "sha256": child["sha256"]})
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
