"""Independent deterministic Quality service for the arbitration fixture.

It reuses the decision-round Strands Agent, A2A executor, and durable task store.
The only new policy is a semantic reject/accept rule for valid candidate content.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import uvicorn
from fastapi.responses import JSONResponse
from a2a.server.apps import A2AFastAPIApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.types import AgentCard, AgentCapabilities, AgentSkill

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "decision-round" / "common"))
import harness_server as prior  # noqa: E402
from fixture import canonical, quality_decision  # noqa: E402


class QualityHarness(prior.Harness):
    def __init__(self, state: Path):
        super().__init__(state, "quality")

    def perform(self, command, task_id, context_id):
        if command.get("op") != "review":
            raise prior.Rejected("Quality accepts only review")
        for field in ("action_id", "run_id", "definition_digest"):
            if not isinstance(command.get(field), str) or not command[field]:
                raise prior.Rejected("missing " + field)
        source = command.get("artifact")
        if not isinstance(source, dict):
            raise prior.Rejected("missing artifact")
        for field in ("revision", "sha256", "author", "content"):
            if not isinstance(source.get(field), str) or not source[field]:
                raise prior.Rejected("missing artifact " + field)
        fingerprint = hashlib.sha256(canonical(command).encode()).hexdigest()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM actions WHERE action_id=?",
                             (command["action_id"],)).fetchone()
            if row:
                if row["fingerprint"] != fingerprint:
                    raise prior.Rejected("action ID reused with different payload")
                db.execute("UPDATE actions SET attempts=attempts+1 WHERE action_id=?",
                           (command["action_id"],))
            else:
                accepted, reason = quality_decision(source, self.identity)
                verdict = {"accepted": accepted, "revision": source["revision"],
                           "sha256": source["sha256"], "reviewer": self.identity,
                           "reason": reason}
                # accepted_count is the inherited receiver-action count. It is
                # one for a rejection too; it is never an acceptance count.
                db.execute("INSERT INTO actions VALUES (?, ?, ?, ?, ?, ?, ?, 1, 1)",
                           (command["action_id"], command["run_id"],
                            command["definition_digest"], "quality", fingerprint,
                            task_id, canonical(verdict)))
            db.execute("INSERT OR IGNORE INTO aliases VALUES (?, ?, ?)",
                       (task_id, command["action_id"], context_id))
        return self.action(command["action_id"])


def create_app(state: Path, port: int):
    harness = QualityHarness(state)
    store = prior.LedgerTaskStore(harness)
    card = AgentCard(
        name="Arbitration Quality", description="Deterministic independent Quality fixture",
        url=f"http://127.0.0.1:{port}/", version="0.0.1", protocol_version="0.3.0",
        default_input_modes=["application/json"], default_output_modes=["application/json"],
        capabilities=AgentCapabilities(streaming=False),
        skills=[AgentSkill(id="quality", name="Quality", description="Exact candidate review",
                           tags=["fixture", "quality"])],
        security_schemes={"fixtureBearer": {"type": "http", "scheme": "bearer"}},
        security=[{"fixtureBearer": []}],
    )
    app = A2AFastAPIApplication(card, DefaultRequestHandler(
        prior.HarnessExecutor(harness, store), store)).build()

    @app.middleware("http")
    async def fixture_auth(request, call_next):
        if request.url.path in {"/health", "/.well-known/agent-card.json"}:
            return await call_next(request)
        if request.headers.get("authorization") != prior.TOKEN:
            return JSONResponse({"error": "fixture authentication required"}, status_code=401)
        return await call_next(request)

    @app.get("/health")
    def health():
        return {"identity": harness.identity, "incarnation": harness.incarnation,
                "role": "quality", "a2a_protocol": "0.3.0"}

    @app.get("/fixture/actions/{action_id}")
    def get_action(action_id: str):
        record = harness.action(action_id)
        if record is None:
            return JSONResponse({"error": "unknown action"}, status_code=404)
        return record

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    uvicorn.run(create_app(args.state, args.port), host="127.0.0.1",
                port=args.port, log_level="warning")
