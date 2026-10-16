"""Static factory interpreter on Hatchet's local embedded engine.

This is a spike fixture, not product code. The worker never defines a task per
published factory version; the immutable factory document travels in run input.
"""

import asyncio
from datetime import timedelta
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import threading
import time

from hatchet_sdk import ClientConfig, Context, DurableContext, EmbeddedHatchetConfig, EmptyModel, Hatchet


ROOT = Path(__file__).resolve().parent
STATE = Path(os.environ["HATCHET_SPIKE_STATE"])
DB = STATE / "observed.sqlite3"
SIDECAR = Path.home() / ".hatchet/embedded/v0.107.0/hatchet-embedded-sidecar_darwin_arm64"
SIDECAR_SHA256 = "3903d6c7057ee2d1bc4d3809981287191e3106a1f3be812ae78f7d77855b98ff"

hatchet = Hatchet.from_embedded(
    ClientConfig(
        embedded=EmbeddedHatchetConfig(
            version="v0.107.0",
            binary_path=str(SIDECAR),
            checksum=SIDECAR_SHA256,
            postgres_data_dir=str(STATE / "postgres"),
            start_api=False,
        )
    )
)


class WorkInput(EmptyModel):
    harness_id: str
    run_id: str
    step: str


class FactoryInput(EmptyModel):
    harness_id: str
    run_id: str
    digest: str
    definition: dict


def event(kind: str, **fields: object) -> None:
    print("SPIKE_JSON " + json.dumps({"kind": kind, **fields}, sort_keys=True), flush=True)


def record_step(inp: WorkInput) -> bool:
    with sqlite3.connect(DB, timeout=30) as db:
        db.execute("PRAGMA busy_timeout = 30000")
        db.execute(
            "CREATE TABLE IF NOT EXISTS steps (harness_id TEXT, run_id TEXT, step TEXT, "
            "PRIMARY KEY (harness_id, run_id, step))"
        )
        result = db.execute(
            "INSERT OR IGNORE INTO steps VALUES (?, ?, ?)",
            (inp.harness_id, inp.run_id, inp.step),
        )
        return bool(result.rowcount)


@hatchet.task(name="exomachina-round-two-work", input_validator=WorkInput)
def work(inp: WorkInput, ctx: Context) -> dict:
    accepted = record_step(inp)
    event("step", harness_id=inp.harness_id, run_id=inp.run_id, step=inp.step, accepted=accepted)
    return {"step": inp.step, "accepted": accepted}


@hatchet.durable_task(
    name="exomachina-round-two-factory-run", input_validator=FactoryInput,
    execution_timeout="5m", retries=1,
)
async def factory(inp: FactoryInput, ctx: DurableContext) -> dict:
    raw = json.dumps(inp.definition, sort_keys=True, separators=(",", ":")).encode()
    if hashlib.sha256(raw).hexdigest() != inp.digest:
        raise ValueError("definition digest changed")
    steps = inp.definition["steps"]
    research = [s for s in steps if s.startswith("research_")]
    await asyncio.gather(*(
        work.aio_run(WorkInput(harness_id=inp.harness_id, run_id=inp.run_id, step=s))
        for s in research
    ))
    await work.aio_run(WorkInput(harness_id=inp.harness_id, run_id=inp.run_id, step="join"))
    if "verify" in steps:
        await work.aio_run(WorkInput(harness_id=inp.harness_id, run_id=inp.run_id, step="verify"))
    await work.aio_run(WorkInput(harness_id=inp.harness_id, run_id=inp.run_id, step="review"))
    decision = await ctx.aio_wait_for_event(
        f"exomachina:decision:{inp.run_id}",
        scope=f"run:{inp.run_id}", lookback_window=timedelta(minutes=10),
    )
    if not decision["approved"]:
        return {"run_id": inp.run_id, "decision": "rejected"}
    await work.aio_run(WorkInput(harness_id=inp.harness_id, run_id=inp.run_id, step="delivery"))
    return {"run_id": inp.run_id, "decision": "approved", "digest": inp.digest}


def main() -> None:
    STATE.mkdir(parents=True, exist_ok=True)
    record_step(WorkInput(harness_id="probe", run_id="schema", step="init"))
    worker = hatchet.worker(
        "exomachina-round-two-static-worker", slots=8, durable_slots=4,
        workflows=[work, factory],
    )
    threading.Thread(target=worker.start, daemon=True).start()
    time.sleep(3)
    event("ready", pid=os.getpid(), source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())

    commands = STATE / "commands.jsonl"
    with commands.open("r", encoding="utf-8") as reader:
        reader.seek(0, os.SEEK_END)
        while True:
            line = reader.readline()
            if not line:
                time.sleep(0.1)
                continue
            command = json.loads(line)
            try:
                if command["op"] == "start":
                    path = STATE / "catalog" / f"{command['digest']}.json"
                    definition = json.loads(path.read_text())
                    raw = json.dumps(definition, sort_keys=True, separators=(",", ":")).encode()
                    if hashlib.sha256(raw).hexdigest() != command["digest"]:
                        raise ValueError("catalog digest changed")
                    ref = factory.run(
                        FactoryInput(harness_id=command["harness_id"], run_id=command["run_id"],
                                     digest=command["digest"], definition=definition),
                        wait_for_result=False,
                    )
                    event("started", run_id=command["run_id"], workflow_run_id=ref.workflow_run_id)
                elif command["op"] == "decide":
                    def send() -> None:
                        pushed = hatchet.event.push(
                            f"exomachina:decision:{command['run_id']}",
                            {"approved": command["approved"], "run_id": command["run_id"]},
                            scope=f"run:{command['run_id']}",
                        )
                        event("decision_sent", run_id=command["run_id"], event_id=str(pushed.event_id))
                    threads = [threading.Thread(target=send) for _ in range(command.get("copies", 1))]
                    for thread in threads:
                        thread.start()
                    for thread in threads:
                        thread.join()
                elif command["op"] == "stop":
                    hatchet.stop_embedded()
                    return
                else:
                    raise ValueError(f"unknown command: {command['op']}")
            except Exception as exc:
                event("command_error", op=command["op"], error=repr(exc))


if __name__ == "__main__":
    main()
