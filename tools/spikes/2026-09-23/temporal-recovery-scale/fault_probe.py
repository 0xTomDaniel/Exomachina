#!/usr/bin/env python3
"""Crash and uncertainty injections; supervisor/native retries own continuation."""
from __future__ import annotations

import asyncio
import copy
import json
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.request
from datetime import timedelta
from pathlib import Path

from temporalio.client import Client

import runtime
from author import materialize, template
from definition import digest, publish
from factory import FactoryRun
from probe import SERVICE_PORTS, bindings, director_send, director_task, health, input_for

HERE = Path(__file__).resolve().parent


def events(state):
    path = state / "supervisor-events.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


async def until_phase(client, run_id, phase, *, child=True, seconds=90):
    async def check():
        try:
            parent = client.get_workflow_handle(run_id)
            status = await parent.query(FactoryRun.status)
            if not child:
                return status if status["phase"] == phase else False
            if not status["child_id"]:
                return False
            c = client.get_workflow_handle(status["child_id"])
            s = await c.query(FactoryRun.status)
            return (s, c) if s["phase"] == phase else False
        except Exception:
            return False
    return await runtime.until(check, seconds=seconds)


async def wait_marker(path: Path):
    await runtime.until(lambda: asyncio.sleep(0, result=path.exists()), seconds=45)


def kill_engine_pg(state: Path) -> dict:
    """Fault controller stops both durable engine processes while restart is held."""
    (state / "hold-restart").touch()
    prior = [event for event in events(state) if event["kind"] in {"temporal-start", "temporal-restart"}]
    server_pid = prior[-1]["pid"]
    pg_pid = int((state / "pgdata" / "postmaster.pid").read_text().splitlines()[0])
    os.killpg(server_pid, signal.SIGKILL)
    runtime.pg_command("-m", "immediate", "stop")
    stopped = {"temporal_pid": server_pid, "postgres_pid": pg_pid,
               "temporal_port_closed": not runtime.port_open(runtime.PORTS["frontend"]),
               "postgres_port_closed": not runtime.port_open(runtime.PORTS["postgres"])}
    (state / "hold-restart").unlink()
    return stopped


async def wait_engine_restarted(state):
    async def check():
        latest = events(state)
        if (any(event["kind"] == "temporal-restart" for event in latest)
                and any(event["kind"] == "postgres-restart" for event in latest)
                and runtime.port_open(runtime.PORTS["frontend"])):
            try:
                client = await Client.connect(f"127.0.0.1:{runtime.PORTS['frontend']}",
                                              namespace="exomachina")
                await client.service_client.check_health()
                return client
            except Exception:
                return False
        return False
    return await runtime.until(check, seconds=70)


def sqlite_rows(path: Path, query: str):
    with sqlite3.connect(path) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute(query)]


async def main():
    state = Path(tempfile.mkdtemp(prefix="exo-temporal-faults-", dir="/tmp"))
    runtime.STATE = state
    observed = {"status": "running", "state": str(state), "checks": {}}
    log = (state / "supervisor.log").open("a")
    supervisor = subprocess.Popen([sys.executable, str(HERE / "supervisor.py"),
                                   "--state", str(state)], cwd=HERE,
                                  stdout=log, stderr=log, start_new_session=True)
    log.close()
    replacement = None
    try:
        async def ready():
            if supervisor.poll() is not None:
                raise RuntimeError(f"supervisor exited {supervisor.returncode}")
            return (state / "supervisor-ready").exists()
        await runtime.until(ready, seconds=90)
        client = await Client.connect(f"127.0.0.1:{runtime.PORTS['frontend']}",
                                      namespace="exomachina")
        identities = {}
        for name in ("source", "counter", "quality", "release", "opaque", "director"):
            with urllib.request.urlopen(f"http://127.0.0.1:{SERVICE_PORTS[name]}/health") as response:
                identities[name] = json.load(response)
        approved = bindings(identities)
        approved["opaque_release"] = {"role": "release",
            "url": f"http://127.0.0.1:{SERVICE_PORTS['opaque']}",
            "identity": identities["opaque"]["identity"], "approved": True}
        (state / "catalog" / "approved_bindings.json").write_text(json.dumps(approved, sort_keys=True))
        director = {"identity": "fault-probe-director", "token": "fixture-local-token", "epoch": 1}
        visible = materialize(template("visible-v3.json"), approved)
        visible_digest = publish(visible, state / "catalog", approved)
        exhausted = materialize(template("visible-exhausted.json"), approved)
        exhausted_digest = publish(exhausted, state / "catalog", approved)
        observed["definitions"] = {"visible_digest": visible_digest,
            "visible_child": next(iter(visible["children"])),
            "exhausted_digest": exhausted_digest,
            "exhausted_child": next(iter(exhausted["children"]))}

        # A4-1: an Activity is active after the counter receiver committed. Stop
        # both Temporal Server and PostgreSQL. The supervisor restarts them.
        marker = state / "assignment-committed"
        mid = await client.start_workflow(FactoryRun.run,
            input_for("fault-mid-assignment", visible, director,
                {"assignment_barrier": {"instance": "counter_evidence",
                    "marker": str(marker), "hold_seconds": 5}}),
            id="fault-mid-assignment", task_queue="arbitration-temporal",
            execution_timeout=timedelta(minutes=10))
        await wait_marker(marker)
        stopped = kill_engine_pg(state)
        client = await wait_engine_restarted(state)
        mid_result = await asyncio.wait_for(client.get_workflow_handle("fault-mid-assignment").result(),
                                            timeout=100)
        observed["checks"]["mid_assignment_engine_pg_kill"] = {
            "stopped": stopped, "result": mid_result}

        # A4-2: Quality committed an r1 verdict, but the Activity has not
        # returned it to Temporal. Kill engine/store and let native retry replay.
        marker = state / "quality-committed"
        quality_run = await client.start_workflow(FactoryRun.run,
            input_for("fault-quality-gap", visible, director,
                {"quality_barrier": {"marker": str(marker), "hold_seconds": 5}}),
            id="fault-quality-gap", task_queue="arbitration-temporal",
            execution_timeout=timedelta(minutes=10))
        await wait_marker(marker)
        stopped = kill_engine_pg(state)
        client = await wait_engine_restarted(state)
        quality_result = await asyncio.wait_for(client.get_workflow_handle("fault-quality-gap").result(),
                                                timeout=100)
        observed["checks"]["quality_remote_commit_gap"] = {
            "stopped": stopped, "result": quality_result}

        # A4-3: Workflow history already exposes exact acceptance while a
        # durable timer delays release. No driver continuation command exists.
        accepted_run = await client.start_workflow(FactoryRun.run,
            input_for("fault-acceptance-gap", visible, director,
                {"acceptance_timer_seconds": 5}),
            id="fault-acceptance-gap", task_queue="arbitration-temporal",
            execution_timeout=timedelta(minutes=10))
        accepted_before, _ = await until_phase(client, "fault-acceptance-gap",
                                                "accepted-before-release")
        stopped = kill_engine_pg(state)
        client = await wait_engine_restarted(state)
        accepted_result = await asyncio.wait_for(client.get_workflow_handle("fault-acceptance-gap").result(),
                                                 timeout=100)
        observed["checks"]["acceptance_before_continuation"] = {
            "before": accepted_before, "stopped": stopped, "result": accepted_result}

        # A4-4/5: a Director A2A Task waits through restart. A replacement
        # Director claims a newer epoch; the old process cannot issue abort.
        task = await asyncio.to_thread(director_send, {"op": "start",
            "action_id": "start-stale-owner", "run_id": "fault-wait-owner",
            "package_digest": exhausted_digest})
        wait, child = await until_phase(client, "fault-wait-owner", "awaiting-director")
        stopped = kill_engine_pg(state)
        client = await wait_engine_restarted(state)
        recovered, child = await until_phase(client, "fault-wait-owner", "awaiting-director")
        replacement_port = 35567
        repl_log = (state / "director-replacement.log").open("a")
        replacement = subprocess.Popen([sys.executable, str(HERE / "director_server.py"),
            "--state", str(state / "director"), "--catalog", str(state / "catalog"),
            "--address", f"127.0.0.1:{runtime.PORTS['frontend']}",
            "--port", str(replacement_port)], cwd=HERE, stdout=repl_log,
            stderr=repl_log, start_new_session=True)
        repl_log.close()
        async def replacement_ready():
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{replacement_port}/health", timeout=1) as response:
                    return json.load(response)
            except Exception:
                return False
        replacement_health = await runtime.until(replacement_ready, seconds=30)
        abort = {"op": "abort", "action_id": "abort-stale-owner",
                 "run_id": "fault-wait-owner", "revision": recovered["current_revision"],
                 "sha256": recovered["current_sha256"]}
        old_response = await asyncio.to_thread(director_send, abort)
        new_response = await asyncio.to_thread(director_send,
            {**abort, "action_id": "abort-new-owner"}, replacement_port)
        parent_result = await client.get_workflow_handle("fault-wait-owner").result()
        original_task = await asyncio.to_thread(director_task, task["id"], replacement_port)
        observed["checks"]["wait_restart_stale_owner"] = {
            "before": wait, "after": recovered, "stopped": stopped,
            "replacement": replacement_health, "old_response": old_response,
            "new_response": new_response, "parent_result": parent_result,
            "original_task_id": task["id"], "original_task_after": original_task}

        # A5: receiver commits both assignment and release then drops replies.
        # Supervisor restarts each receiver; Activities reconcile by stable ID.
        lost = await client.start_workflow(FactoryRun.run,
            input_for("fault-lost-acks", visible, director,
                {"drop_assignment_ack": "source_evidence", "drop_release_ack": True}),
            id="fault-lost-acks", task_queue="arbitration-temporal",
            execution_timeout=timedelta(minutes=10))
        lost_result = await asyncio.wait_for(lost.result(), timeout=100)
        observed["checks"]["participating_lost_acks"] = lost_result

        # A5 opaque peer has no receipt lookup/dedup. It may have committed,
        # so the Workflow stays durably unresolved and never resubmits.
        opaque_template = template("visible-v3.json")
        opaque_template["root"]["revision"] = "v3-opaque"
        opaque_template["child"]["revision"] = "v3-opaque"
        opaque_template["child"]["nodes"]["publish"]["service"] = "opaque_release"
        opaque = materialize(opaque_template, approved)
        opaque_digest = publish(opaque, state / "catalog", approved)
        opaque_run = await client.start_workflow(FactoryRun.run,
            input_for("fault-opaque", opaque, director,
                {"release_mode": "opaque", "drop_release_ack": True}),
            id="fault-opaque", task_queue="arbitration-temporal",
            execution_timeout=timedelta(minutes=10))
        unresolved, _ = await until_phase(client, "fault-opaque", "unresolved-release")
        before_effects = sqlite_rows(state / "opaque" / "release.sqlite3",
                                     "SELECT count(*) AS effects FROM opaque_effects")
        stopped = kill_engine_pg(state)
        client = await wait_engine_restarted(state)
        still_unresolved, _ = await until_phase(client, "fault-opaque", "unresolved-release")
        after_effects = sqlite_rows(state / "opaque" / "release.sqlite3",
                                    "SELECT count(*) AS effects FROM opaque_effects")
        observed["checks"]["opaque_durable_unknown"] = {
            "package_digest": opaque_digest, "before": unresolved,
            "after": still_unresolved, "effects_before": before_effects,
            "effects_after": after_effects, "stopped": stopped}

        observed["receiver_rows"] = {
            "source": sqlite_rows(state / "source" / "harness.sqlite3",
                                  "SELECT action_id,run_id,attempts,accepted_count,artifact FROM actions"),
            "counter": sqlite_rows(state / "counter" / "harness.sqlite3",
                                   "SELECT action_id,run_id,attempts,accepted_count,artifact FROM actions"),
            "quality": sqlite_rows(state / "quality" / "harness.sqlite3",
                                   "SELECT action_id,run_id,attempts,accepted_count,artifact FROM actions"),
            "release": sqlite_rows(state / "release" / "release.sqlite3",
                                   "SELECT * FROM releases"),
        }
        observed["supervisor_events"] = events(state)
        assert all(row["stopped"]["temporal_port_closed"] and row["stopped"]["postgres_port_closed"]
                   for name, row in observed["checks"].items() if "stopped" in row)
        assert lost_result["status"] == "accepted"
        assert still_unresolved["authoritative_acceptance"] is not None
        assert before_effects == after_effects == [{"effects": 1}]
        observed["status"] = "passed"
    except Exception as error:
        observed["status"] = "failed"
        observed["error"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        (HERE / "fault-observed.json").write_text(json.dumps(observed, indent=2, sort_keys=True) + "\n")
        if replacement is not None:
            runtime.kill(replacement, signal.SIGTERM)
        if supervisor.poll() is None:
            supervisor.send_signal(signal.SIGTERM)
            try:
                supervisor.wait(timeout=20)
            except subprocess.TimeoutExpired:
                runtime.kill(supervisor, signal.SIGKILL)
        print(json.dumps({"status": observed["status"], "state": str(state),
                          "checks": sorted(observed["checks"])}))


if __name__ == "__main__":
    asyncio.run(main())
