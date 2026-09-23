"""Prepare and verify a normal Director decision wait across cold backup/restore."""
from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import time
import urllib.request
from pathlib import Path

from temporalio.client import Client

import runtime
from author import materialize, template
from definition import publish
from factory import FactoryRun
from probe import SERVICE_PORTS, bindings, director_send, director_task

PREFIX = Path(__file__).resolve().parent.parent
STATE = PREFIX / "state"
runtime.STATE = STATE
RUN_ID = "package-backup-director-wait"


async def status(client):
    parent = await client.get_workflow_handle(RUN_ID).query(FactoryRun.status)
    child = await client.get_workflow_handle(parent["child_id"]).query(FactoryRun.status) if parent["child_id"] else None
    return parent, child


def original_task_id() -> str:
    with sqlite3.connect(STATE / "director/director.sqlite3") as db:
        return db.execute("SELECT task_id FROM aliases WHERE run_id=? ORDER BY rowid LIMIT 1", (RUN_ID,)).fetchone()[0]


async def run(mode: str) -> dict:
    started = time.monotonic()
    client = await Client.connect(f"127.0.0.1:{runtime.PORTS['frontend']}", namespace="exomachina")
    if mode == "prepare":
        identities = {}
        for name in ("source", "counter", "quality", "release"):
            with urllib.request.urlopen(f"http://127.0.0.1:{SERVICE_PORTS[name]}/health") as response:
                identities[name] = json.load(response)
        approved = bindings(identities)
        (STATE / "catalog/approved_bindings.json").write_text(json.dumps(approved, sort_keys=True))
        package = materialize(template("visible-exhausted.json"), approved)
        digest = publish(package, STATE / "catalog", approved)
        task = await asyncio.to_thread(director_send, {"op": "start", "action_id": "start:" + RUN_ID,
            "run_id": RUN_ID, "package_digest": digest})
        task_id = task["id"]
    else:
        task_id = original_task_id()
    async def wait():
        parent, child = await status(client)
        if child and child["phase"] == "awaiting-director":
            return parent, child
        return False
    if mode == "prepare":
        parent, child = await runtime.until(wait, seconds=90)
    else:
        parent, child = await status(client)
    task = await asyncio.to_thread(director_task, task_id)
    result = {"mode": mode, "run_id": RUN_ID, "original_task_id": task_id,
        "parent_id": RUN_ID, "child_id": parent["child_id"],
        "child_phase": child["phase"], "task_state": task["status"]["state"],
        "definition_digest": child["definition_digest"],
        "elapsed_seconds": time.monotonic() - started}
    if mode == "verify":
        if child["phase"] == "awaiting-director":
            await asyncio.to_thread(director_send, {"op": "abort", "action_id": "backup-abort:" + RUN_ID,
                "run_id": RUN_ID, "revision": child["current_revision"], "sha256": child["current_sha256"]})
        async def done():
            current = await asyncio.to_thread(director_task, task_id)
            return current if current["status"]["state"] == "completed" else False
        final = await runtime.until(done, seconds=40)
        native = await client.get_workflow_handle(RUN_ID).result()
        result["after_decision"] = {"original_task_state": final["status"]["state"],
            "native_state": native["status"], "child_id": native["child"]["run"]}
    (PREFIX / f"backup-{mode}.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, sort_keys=True))
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["prepare", "verify"])
    args = parser.parse_args()
    asyncio.run(run(args.mode))
