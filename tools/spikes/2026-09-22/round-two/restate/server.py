"""A deliberately small, fixed Restate handler for immutable factory documents."""

import hashlib
import json
import os
from pathlib import Path

import restate
from restate import Workflow, WorkflowContext, WorkflowSharedContext
from restate.context import RunOptions


CATALOG = Path(os.environ["EXOMACHINA_RESTATE_CATALOG"])
EVENTS = Path(os.environ["EXOMACHINA_RESTATE_EVENTS"])
factory = Workflow("FactoryRun")


def load_definition(digest: str) -> dict:
    raw = (CATALOG / f"{digest}.json").read_bytes()
    if hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError("definition digest mismatch")
    return json.loads(raw)


def record_node(run_id: str, node: str) -> str:
    event = json.dumps({"run_id": run_id, "node": node}, sort_keys=True) + "\n"
    fd = os.open(EVENTS, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
    try:
        os.write(fd, event.encode())
    finally:
        os.close(fd)
    return node


@factory.main()
async def run(ctx: WorkflowContext, request: dict) -> dict:
    run_id = ctx.key()
    digest = request["definition_digest"]
    definition = await ctx.run_typed("definition", load_definition, RunOptions(), digest)
    steps = definition["steps"]
    if steps[:2] != ["research_a", "research_b"] or steps[-2:] != ["director_wait", "deliver"]:
        raise ValueError("unsupported factory shape")

    research = [ctx.run_typed(node, record_node, RunOptions(), run_id, node) for node in steps[:2]]
    completed = await restate.gather(*research)
    for future in completed:
        await future
    for node in steps[2:-2]:
        await ctx.run_typed(node, record_node, RunOptions(), run_id, node)
    await ctx.run_typed("director_wait", record_node, RunOptions(), run_id, "director_wait")
    decision = await ctx.promise("director_decision").value()
    if decision != {"accepted": True}:
        return {"run_id": run_id, "definition_digest": digest, "accepted": False}
    await ctx.run_typed("deliver", record_node, RunOptions(), run_id, "deliver")
    return {
        "run_id": run_id,
        "definition_digest": digest,
        "version": definition["version"],
        "accepted": True,
        "steps": steps,
    }


@factory.handler()
async def approve(ctx: WorkflowSharedContext, decision: dict) -> str:
    await ctx.promise("director_decision").resolve(decision)
    return "recorded"


app = restate.app(services=[factory])
