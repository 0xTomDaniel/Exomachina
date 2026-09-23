"""Fixed Temporal interpreter for a deliberately small factory document vocabulary."""

import asyncio
import json
import os
import sqlite3
from datetime import timedelta
from pathlib import Path

from temporalio import activity, workflow


def _event(event: dict) -> None:
    path = Path(os.environ["EXO_TEMPORAL_EVENTS"])
    with path.open("a") as file:
        file.write(json.dumps(event, sort_keys=True) + "\n")


@activity.defn
async def assign(input: dict) -> dict:
    await asyncio.sleep(0.03)
    capability = input["capability"]
    _event({"kind": "assign", "run": input["run"], "capability": capability})
    return {"capability": capability, "artifact": f"artifact:{input['run']}:{capability}"}


@activity.defn
async def review(input: dict) -> dict:
    seen = {item["capability"] for item in input["artifacts"]}
    accepted = {"research-a@1", "research-b@1"}.issubset(seen)
    _event({"kind": "review", "run": input["run"], "accepted": accepted})
    return {"accepted": accepted, "evidence": sorted(seen)}


@activity.defn
async def deliver(input: dict) -> dict:
    db = sqlite3.connect(os.environ["EXO_TEMPORAL_LEDGER"], timeout=10)
    try:
        db.execute("CREATE TABLE IF NOT EXISTS delivery (run TEXT PRIMARY KEY, digest TEXT NOT NULL)")
        db.execute("INSERT OR IGNORE INTO delivery VALUES (?, ?)", (input["run"], input["digest"]))
        db.commit()
        count = db.execute("SELECT count(*) FROM delivery WHERE run=?", (input["run"],)).fetchone()[0]
    finally:
        db.close()
    _event({"kind": "deliver-attempt", "run": input["run"], "accepted_receipts": count})
    return {"accepted": count == 1, "receipt": input["run"]}


@workflow.defn
class FactoryRun:
    def __init__(self) -> None:
        self.phase = "starting"
        self.decision: dict | None = None
        self.digest = ""
        self.completed: list[str] = []

    @workflow.query
    def status(self) -> dict:
        return {"phase": self.phase, "digest": self.digest, "completed": self.completed}

    @workflow.update
    def decide(self, input: dict) -> str:
        if self.decision is not None:
            return "already-decided"
        if not input.get("approved") or not input.get("actor"):
            return "invalid-decision"
        self.decision = input
        return "accepted"

    @workflow.run
    async def run(self, input: dict) -> dict:
        document = input["document"]
        self.digest = input["digest"]
        artifacts: list[dict] = []
        decision_seen = False
        reviewed = False
        for step in document["steps"]:
            kind = step["type"]
            if kind == "parallel":
                jobs = [
                    workflow.execute_activity(
                        assign,
                        {"run": input["run"], "capability": capability},
                        start_to_close_timeout=timedelta(seconds=30),
                    )
                    for capability in step["capabilities"]
                ]
                artifacts.extend(await asyncio.gather(*jobs))
                self.completed.append("parallel")
            elif kind == "assign":
                artifacts.append(
                    await workflow.execute_activity(
                        assign,
                        {"run": input["run"], "capability": step["capability"]},
                        start_to_close_timeout=timedelta(seconds=30),
                    )
                )
                self.completed.append(step["capability"])
            elif kind == "review":
                outcome = await workflow.execute_activity(
                    review,
                    {"run": input["run"], "artifacts": artifacts},
                    start_to_close_timeout=timedelta(seconds=30),
                )
                if not outcome["accepted"]:
                    raise RuntimeError("independent review rejected")
                reviewed = True
                self.completed.append("review")
            elif kind == "director-decision":
                if not reviewed:
                    raise RuntimeError("review bypass")
                self.phase = "awaiting-decision"
                await workflow.wait_condition(lambda: self.decision is not None)
                # A brief test grace keeps this execution open while clients race duplicate Updates.
                await workflow.sleep(1)
                decision_seen = True
                self.completed.append("director-decision")
            elif kind == "deliver":
                if not decision_seen:
                    raise RuntimeError("director bypass")
                receipt = await workflow.execute_activity(
                    deliver,
                    {"run": input["run"], "digest": input["digest"]},
                    start_to_close_timeout=timedelta(seconds=30),
                )
                self.completed.append("deliver")
            else:
                raise RuntimeError(f"unsupported step: {kind}")
        self.phase = "complete"
        return {
            "run": input["run"],
            "identity": input["identity"],
            "revision": document["revision"],
            "digest": self.digest,
            "completed": self.completed,
            "receipt": receipt,
        }
