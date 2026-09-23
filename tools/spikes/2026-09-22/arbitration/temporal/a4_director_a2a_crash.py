#!/usr/bin/env python3
"""Probe-only Director A2A-origin crash while a capability Activity is active."""
from __future__ import annotations

import asyncio
import json
import os
import signal
import tempfile
import urllib.request
from pathlib import Path

from temporalio.client import Client

import runtime
from author import materialize, template
from definition import publish
from factory import FactoryRun
from fault_probe import events, kill_engine_pg, sqlite_rows, wait_engine_restarted
from hidden_trial import launch_supervisor, ready, verify_freeze
from probe import SERVICE_PORTS, bindings, director_send, director_task

HERE = Path(__file__).resolve().parent


async def in_flight(state, client, run):
    async def check():
        path = state / "activities.jsonl"
        if not path.exists():
            return False
        lines = [json.loads(line) for line in path.read_text().splitlines()]
        starts = [line for line in lines if line.get("kind") == "assign-start"
                  and line.get("instance") == "source_evidence" and line.get("run", "").startswith(run + ":child:")]
        completes = [line for line in lines if line.get("kind") == "assign-complete"
                     and line.get("instance") == "source_evidence" and line.get("run", "").startswith(run + ":child:")]
        if not starts or completes:
            return False
        try:
            parent = await client.get_workflow_handle(run).query(FactoryRun.status)
            child = await client.get_workflow_handle(parent["child_id"]).query(FactoryRun.status)
            return {"activity_start": starts[0], "parent": parent, "child": child}
        except Exception:
            return False
    return await runtime.until(check, seconds=30)


async def main():
    before_freeze = verify_freeze()
    state = Path(tempfile.mkdtemp(prefix="exo-temporal-a2a-crash-", dir="/tmp"))
    runtime.STATE = state
    observed = {"status": "running", "state": str(state), "freeze_before": before_freeze}
    supervisor = launch_supervisor(state, "a2a-crash")
    source_pid = None
    source_stopped = False
    try:
        await ready(supervisor, state)
        client = await Client.connect(f"127.0.0.1:{runtime.PORTS['frontend']}", namespace="exomachina")
        identities = {}
        for name in ("source", "counter", "quality", "release"):
            with urllib.request.urlopen(f"http://127.0.0.1:{SERVICE_PORTS[name]}/health") as response:
                identities[name] = json.load(response)
        approved = bindings(identities)
        (state / "catalog" / "approved_bindings.json").write_text(json.dumps(approved, sort_keys=True))
        package = materialize(template("visible-v3.json"), approved)
        package_digest = publish(package, state / "catalog", approved)
        source_pid = next(event["pid"] for event in reversed(events(state))
                          if event["kind"] == "service-start" and event.get("name") == "source")
        os.kill(source_pid, signal.SIGSTOP)
        source_stopped = True
        run = "a4-director-a2a-inflight"
        task = await asyncio.to_thread(director_send, {"op": "start",
            "action_id": "a4-director-a2a-start", "run_id": run,
            "package_digest": package_digest})
        active = await in_flight(state, client, run)
        assert active["child"]["phase"] == "parallel"
        stopped = kill_engine_pg(state)
        os.kill(source_pid, signal.SIGCONT)
        source_stopped = False
        client = await wait_engine_restarted(state)
        result = await asyncio.wait_for(client.get_workflow_handle(run).result(), timeout=100)
        original_task = await asyncio.to_thread(director_task, task["id"])
        child_run = result["child"]["run"]
        source = sqlite_rows(state / "source" / "harness.sqlite3",
            "SELECT action_id,run_id,definition_digest,attempts,accepted_count FROM actions")
        counter = sqlite_rows(state / "counter" / "harness.sqlite3",
            "SELECT action_id,run_id,definition_digest,attempts,accepted_count FROM actions")
        quality = sqlite_rows(state / "quality" / "harness.sqlite3",
            "SELECT action_id,run_id,definition_digest,attempts,accepted_count,artifact FROM actions")
        release = sqlite_rows(state / "release" / "release.sqlite3", "SELECT * FROM releases")
        assert task["id"] == original_task["id"]
        assert original_task["status"]["state"] == "completed"
        assert original_task["metadata"]["run_id"] == run
        assert result["status"] == "accepted"
        assert result["child"]["acceptance"]["revision"] == "r2"
        assert len(source) == len(counter) == 1 and len(release) == 1
        assert source[0]["accepted_count"] == counter[0]["accepted_count"] == 1
        assert release[0]["accepted_effect_count"] == 1
        observed.update({"status": "passed", "package_digest": package_digest,
            "task_at_start": task, "task_after_recovery": original_task,
            "active_before_kill": active, "stopped": stopped,
            "result": result, "source_receiver": source,
            "counter_receiver": counter, "quality_receiver": quality,
            "release_receiver": release, "supervisor_events": events(state),
            "freeze_after": verify_freeze(),
            "scope": "Activity active before remote source commit; post-commit crash remains separately proven by direct-start fault probe."})
        assert observed["freeze_after"] == before_freeze
    except Exception as error:
        observed["status"] = "failed"
        observed["error"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        if source_stopped and source_pid is not None:
            os.kill(source_pid, signal.SIGCONT)
        (HERE / "a4-a2a-observed.json").write_text(json.dumps(observed, indent=2, sort_keys=True) + "\n")
        if supervisor.poll() is None:
            supervisor.send_signal(signal.SIGTERM)
            supervisor.wait(timeout=20)
        print(json.dumps({"status": observed["status"], "state": str(state)}))


if __name__ == "__main__":
    asyncio.run(main())
