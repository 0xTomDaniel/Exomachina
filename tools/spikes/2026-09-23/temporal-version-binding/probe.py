#!/usr/bin/env python3
"""Focused real Temporal/PostgreSQL/Strands trial; state and logs live under /tmp."""
from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import urllib.request
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

from temporalio.client import Client

import runtime
from author import materialize, template
from definition import digest, publish, validate
from factory import FactoryRun

HERE = Path(__file__).resolve().parent
COMMON = HERE.parent.parent / "2026-09-22" / "arbitration" / "common"
PRIOR_COMMON = HERE.parent.parent / "2026-09-22" / "decision-round" / "common"
S2_PYTHON = HERE.parent.parent / "2026-09-22" / "s2" / ".venv" / "bin" / "python"
SERVICE_PORTS = {"source": 42161, "counter": 42162, "quality": 42163,
                 "release": 42164, "opaque": 42165, "director": 42166}


def start_service(name: str, state: Path) -> subprocess.Popen:
    port = SERVICE_PORTS[name]
    service_state = state / name
    if name in {"source", "counter"}:
        script = HERE / "slow_harness_server.py" if name == "source" else PRIOR_COMMON / "harness_server.py"
        argv = [str(S2_PYTHON), str(script), "--state", str(service_state),
                "--role", "capability", "--port", str(port)]
    elif name == "quality":
        argv = [str(S2_PYTHON), str(COMMON / "quality_server.py"),
                "--state", str(service_state), "--port", str(port)]
    elif name == "director":
        argv = [sys.executable, str(HERE / "director_server.py"),
                "--state", str(service_state), "--catalog", str(state / "catalog"),
                "--address", f"127.0.0.1:{runtime.PORTS['frontend']}",
                "--port", str(port)]
    else:
        argv = [str(S2_PYTHON), str(COMMON / "release_server.py"),
                "--state", str(service_state), "--mode",
                "opaque" if name == "opaque" else "participating", "--port", str(port)]
    log = (state / f"{name}.log").open("a")
    process = subprocess.Popen(argv, stdout=log, stderr=log, start_new_session=True)
    log.close()
    return process


async def health(name: str, process: subprocess.Popen) -> dict:
    url = f"http://127.0.0.1:{SERVICE_PORTS[name]}/health"
    async def check():
        if process.poll() is not None:
            raise RuntimeError(f"{name} exited {process.returncode}")
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                return json.load(response)
        except Exception:
            return False
    return await runtime.until(check, seconds=30)


def bindings(identities: dict) -> dict:
    result = {}
    for name in ("source", "counter", "quality", "release"):
        role = "capability" if name in {"source", "counter"} else name
        result[name] = {"role": role,
            "url": f"http://127.0.0.1:{SERVICE_PORTS[name]}",
            "identity": identities[name]["identity"], "approved": True}
    return result


def input_for(run: str, package: dict, director: dict, faults: dict | None = None,
              wait_seconds: int = 180) -> dict:
    return {"run": run, "definition_digest": digest(package["root"]),
            "package_digest": digest(package), "document": package["root"],
            "package": package, "director": director,
            "faults": faults or {}, "wait_seconds": wait_seconds}


async def child_wait(client: Client, parent) -> tuple[dict, object]:
    async def check():
        status = await parent.query(FactoryRun.status)
        if not status["child_id"]:
            return False
        child = client.get_workflow_handle(status["child_id"])
        try:
            child_status = await child.query(FactoryRun.status)
        except Exception:
            return False
        return (child_status, child) if child_status["phase"] == "awaiting-director" else False
    return await runtime.until(check, seconds=60)


async def start(client: Client, run: str, package: dict, director: dict,
                faults: dict | None = None):
    return await client.start_workflow(FactoryRun.run,
        input_for(run, package, director, faults), id=run,
        task_queue="arbitration-temporal", execution_timeout=timedelta(minutes=10))


def director_rpc(method: str, params: dict, port: int | None = None) -> dict:
    rpc = {"jsonrpc": "2.0", "id": str(uuid4()), "method": method, "params": params}
    req = urllib.request.Request(f"http://127.0.0.1:{port or SERVICE_PORTS['director']}/",
        data=json.dumps(rpc).encode(), method="POST",
        headers={"Authorization": "Bearer fixture-token", "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=20) as response:
        body = json.load(response)
    if "error" in body:
        raise RuntimeError("Director A2A error: " + json.dumps(body["error"]))
    return body["result"]


def director_send(command: dict, port: int | None = None) -> dict:
    return director_rpc("message/send", {"message": {"role": "user",
        "messageId": str(uuid4()), "parts": [{"kind": "data", "data": command}]}}, port)


def director_task(task_id: str, port: int | None = None) -> dict:
    return director_rpc("tasks/get", {"id": task_id}, port)


async def main() -> None:
    runtime.STATE = Path(tempfile.mkdtemp(prefix="exo-temporal-arbitration-", dir="/tmp"))
    state = runtime.STATE
    (state / "pgsocket").mkdir()
    (state / "catalog").mkdir()
    observed = {"status": "running", "state": str(state), "checks": {}, "definitions": {},
                "topology": "Temporal Server v1.32.0 + PostgreSQL 16.15 + Python SDK worker + two Strands capability services + Strands Quality + release receiver"}
    processes: dict[str, subprocess.Popen] = {}
    server = worker = None
    pg_running = False
    try:
        for port in list(runtime.PORTS.values()) + list(SERVICE_PORTS.values()):
            if runtime.port_open(port):
                raise RuntimeError(f"port occupied: {port}")
        for binary in (runtime.TEMPORAL, runtime.SQL_TOOL, runtime.SCHEMA,
                       runtime.PG_BIN / "initdb", runtime.CLI, S2_PYTHON):
            if not binary.exists():
                raise FileNotFoundError(binary)
        observed["versions"] = {
            "server": runtime.shell([str(runtime.TEMPORAL), "--version"]).strip(),
            "server_sha256": hashlib.sha256(runtime.TEMPORAL.read_bytes()).hexdigest(),
            "postgres": runtime.shell([str(runtime.PG_BIN / "postgres"), "--version"]).strip(),
            "python_sdk": __import__("importlib.metadata", fromlist=["version"]).version("temporalio"),
        }
        runtime.start_postgres(True)
        pg_running = True
        config = runtime.render_config()
        server = runtime.start_temporal(config)
        await runtime.wait_server(server)
        runtime.shell([str(runtime.CLI), "--disable-config-env", "--disable-config-file",
                       "--address", f"127.0.0.1:{runtime.PORTS['frontend']}",
                       "--namespace", "exomachina", "operator", "namespace", "create",
                       "--retention", "1d"])
        for name in ("source", "counter", "quality", "release"):
            processes[name] = start_service(name, state)
        identities = {name: await health(name, process) for name, process in processes.items()}
        observed["services"] = identities
        worker = runtime.start_worker()
        client = await Client.connect(f"127.0.0.1:{runtime.PORTS['frontend']}",
                                      namespace="exomachina")
        bound = bindings(identities)
        (state / "catalog" / "approved_bindings.json").write_text(
            json.dumps(bound, sort_keys=True))
        processes["director"] = start_service("director", state)
        observed["services"]["director"] = await health("director", processes["director"])
        director = {"identity": "temporal-director-fixture", "token": "local-director-token",
                    "epoch": 1}

        old_template = template("visible-exhausted.json")
        old_template["root"]["revision"] = "v2"
        old_template["child"]["revision"] = "v2"
        old = materialize(old_template, bound)
        old_digest = publish(old, state / "catalog", bound)
        old_run = await start(client, "temporal-old-v2", old, director)
        old_wait, old_child = await child_wait(client, old_run)
        observed["definitions"]["old_v2"] = {"digest": old_digest,
            "child_digest": digest(old["child"] if "child" in old else next(iter(old["children"].values()))),
            "waiting": old_wait, "catalog_path": str(state / "catalog" / f"{old_digest}.json")}

        success = materialize(template("visible-v3.json"), bound)
        success_digest = publish(success, state / "catalog", bound)
        worker_pid_before = worker.pid
        success_run = await start(client, "temporal-v3-success", success, director,
            {"delay_assignment": {"counter_evidence": 1.25}})
        success_result = await asyncio.wait_for(success_run.result(), timeout=90)
        observed["checks"]["visible_success"] = success_result
        activity_events = [json.loads(line) for line in (state / "activities.jsonl").read_text().splitlines()]
        child_run = success_result["child"]["run"]
        timed = [event for event in activity_events if event.get("run") == child_run]
        def when(kind, instance=None):
            return next(event["wall_time"] for event in timed
                        if event["kind"] == kind and
                        (instance is None or event.get("instance") == instance))
        overlap = (when("assign-start", "counter_evidence") <
                   when("assign-complete", "source_evidence") <
                   when("assign-complete", "counter_evidence") <
                   when("join-start"))
        observed["checks"]["branch_overlap_join_wait"] = {"passed": overlap, "events": timed}
        assert overlap
        observed["definitions"]["v3_success"] = {"digest": success_digest,
            "child_digest": next(iter(success["children"])),
            "catalog_path": str(state / "catalog" / f"{success_digest}.json")}
        observed["checks"]["worker_unchanged_during_v3_publication"] = (
            worker.pid == worker_pid_before and worker.poll() is None)
        observed["checks"]["old_wait_still_pinned"] = await old_child.query(FactoryRun.status)

        exhausted = materialize(template("visible-exhausted.json"), bound)
        exhausted_digest = publish(exhausted, state / "catalog", bound)
        exhausted_run = await start(client, "temporal-v3-exhausted", exhausted, director)
        exhausted_wait, exhausted_child = await child_wait(client, exhausted_run)
        observed["checks"]["exhausted_wait"] = exhausted_wait
        invalid = []
        base = {"command_id": "bad", "action": "abort", "actor": director["identity"],
                "token": director["token"], "run": exhausted_wait["run"],
                "definition_digest": exhausted_wait["definition_digest"],
                "revision": exhausted_wait["current_revision"],
                "sha256": exhausted_wait["current_sha256"], "epoch": 1}
        for change in ({"actor": "forged"}, {"revision": "r1"},
                       {"definition_digest": "stale-digest"}):
            command = {**base, **change}
            try:
                await exhausted_child.execute_update(FactoryRun.director_command, command)
                raise AssertionError("invalid Director command admitted")
            except Exception as error:
                invalid.append({"change": change, "error": type(error).__name__})
        observed["checks"]["invalid_director_commands"] = invalid
        await exhausted_child.execute_update(FactoryRun.director_command,
                                            {**base, "command_id": "authorized-abort"})
        observed["checks"]["exhausted_result"] = await exhausted_run.result()
        observed["definitions"]["v3_exhausted"] = {"digest": exhausted_digest,
            "child_digest": next(iter(exhausted["children"]))}
        old_command = {**base, "command_id": "abort-old-v2",
                       "run": old_wait["run"],
                       "definition_digest": old_wait["definition_digest"],
                       "revision": old_wait["current_revision"],
                       "sha256": old_wait["current_sha256"]}
        await old_child.execute_update(FactoryRun.director_command, old_command)
        observed["checks"]["old_result"] = await old_run.result()
        director_run_id = "temporal-director-a2a"
        started_task = await asyncio.to_thread(director_send, {
            "op": "start", "action_id": "start-director-a2a",
            "run_id": director_run_id, "package_digest": success_digest})
        director_handle = client.get_workflow_handle(director_run_id)
        director_result = await asyncio.wait_for(director_handle.result(), timeout=90)
        completed_task = await asyncio.to_thread(director_task, started_task["id"])
        observed["checks"]["director_a2a"] = {
            "task_id": started_task["id"],
            "original_task_state": started_task["status"]["state"],
            "completed_task_state": completed_task["status"]["state"],
            "task_metadata": completed_task["metadata"],
            "result": director_result}
        assert completed_task["status"]["state"] == "completed"
        observed["resources"] = runtime.sample(server, worker)
        observed["status"] = "passed"
    except Exception as error:
        observed["status"] = "failed"
        observed["error"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        (HERE / "observed.json").write_text(json.dumps(observed, indent=2, sort_keys=True) + "\n")
        for process in processes.values():
            runtime.kill(process, signal.SIGTERM)
        if worker is not None:
            runtime.kill(worker, signal.SIGTERM)
        if server is not None:
            runtime.kill(server, signal.SIGTERM)
        if pg_running:
            runtime.pg_command("-m", "immediate", "stop")
        print(json.dumps({"status": observed["status"], "state": str(state),
                          "checks": sorted(observed["checks"])}))


if __name__ == "__main__":
    asyncio.run(main())
