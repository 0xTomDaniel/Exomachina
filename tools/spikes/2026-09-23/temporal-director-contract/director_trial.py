#!/usr/bin/env python3
"""Ordinary Director A2A typed-input runs; no failure or crash injection."""
from __future__ import annotations

import asyncio
import hashlib
import json
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

from temporalio.client import Client

import runtime
from author import materialize, template
from definition import digest, publish
from factory import FactoryRun
from probe import SERVICE_PORTS, bindings, director_send, director_task, health, start_service

HERE = Path(__file__).resolve().parent
COMMON = HERE.parent.parent / "2026-09-22" / "decision-round" / "common"
S2_PYTHON = HERE.parent.parent / "2026-09-22" / "s2" / ".venv" / "bin" / "python"
EXTRA = {"source_beta": 41468, "counter_beta": 41469}


def service_health(port: int) -> dict:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=2) as response:
        return json.load(response)


async def until(check, label: str, seconds: float = 90):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = await check()
        if value:
            return value
        await asyncio.sleep(.25)
    raise TimeoutError(label)


def rows(path: Path, statement: str, params: tuple = ()) -> list[dict]:
    if not path.exists():
        return []
    with sqlite3.connect(path) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute(statement, params)]


async def main():
    state = Path(tempfile.mkdtemp(prefix="exo-tq-director-", dir="/tmp"))
    runtime.STATE = state
    (state / "pgsocket").mkdir()
    (state / "catalog").mkdir()
    observed = {"status": "running", "state_prefix": "/tmp/exo-tq-director-*",
                "command": f"{sys.executable} -B {__file__}", "runs": {}, "invalid": {}}
    processes: dict[str, subprocess.Popen] = {}
    server = worker = None
    pg_running = False
    cleanup_errors = []
    try:
        for port in list(runtime.PORTS.values()) + list(SERVICE_PORTS.values()) + list(EXTRA.values()):
            if runtime.port_open(port):
                raise RuntimeError(f"port occupied: {port}")
        runtime.start_postgres(True)
        pg_running = True
        server = runtime.start_temporal(runtime.render_config())
        await runtime.wait_server(server)
        runtime.shell([str(runtime.CLI), "--disable-config-env", "--disable-config-file",
                       "--address", f"127.0.0.1:{runtime.PORTS['frontend']}",
                       "--namespace", "exomachina", "operator", "namespace", "create",
                       "--retention", "1d"])
        for name in ("source", "counter", "quality", "release"):
            processes[name] = start_service(name, state)
        identities = {name: await health(name, process) for name, process in processes.items()}
        for name, port in EXTRA.items():
            with (state / f"{name}.log").open("a") as log:
                processes[name] = subprocess.Popen(
                    [str(S2_PYTHON), str(COMMON / "harness_server.py"),
                     "--state", str(state / name), "--role", "capability", "--port", str(port)],
                    stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            async def ready(name=name, port=port):
                if processes[name].poll() is not None:
                    raise RuntimeError(f"{name} exited")
                try:
                    return service_health(port)
                except Exception:
                    return False
            identities[name] = await until(ready, f"{name} health", 30)
        worker = runtime.start_worker()
        client = await Client.connect(f"127.0.0.1:{runtime.PORTS['frontend']}",
                                      namespace="exomachina")
        initial = bindings(identities)
        bound = {
            "source_alpha": initial["source"],
            "source_beta": {"role": "capability", "url": f"http://127.0.0.1:{EXTRA['source_beta']}",
                            "identity": identities["source_beta"]["identity"], "approved": True},
            "counter_alpha": initial["counter"],
            "counter_beta": {"role": "capability", "url": f"http://127.0.0.1:{EXTRA['counter_beta']}",
                             "identity": identities["counter_beta"]["identity"], "approved": True},
            "quality": initial["quality"], "release": initial["release"],
        }
        (state / "catalog" / "approved_bindings.json").write_text(json.dumps(bound, sort_keys=True))
        package = materialize(template("withheld-a2c-v4-mixed.json"), bound)
        package_digest = publish(package, state / "catalog", bound)
        observed["package"] = {"digest": package_digest,
                               "child_digest": next(iter(package["children"])),
                               "run_input_schema": package["run_inputs"]}
        processes["director"] = start_service("director", state)
        identities["director"] = await health("director", processes["director"])
        observed["identities"] = {name: value["identity"] for name, value in identities.items()}

        def start_command(run: str, mode: str | None):
            command = {"op": "start", "action_id": "start:" + run,
                       "run_id": run, "package_digest": package_digest}
            if mode is not None:
                command["run_inputs"] = {"outcome_mode": mode}
            return command

        for label, mode in (("invalid_enum", "unexpected"), ("missing_required", None)):
            run = f"director-{label}"
            response = await asyncio.to_thread(director_send, start_command(run, mode))
            run_rows = rows(state / "director" / "director.sqlite3",
                            "SELECT * FROM runs WHERE run_id=?", (run,))
            try:
                await client.get_workflow_handle(run).describe()
            except Exception as error:
                workflow_absent = True
                lookup_error = type(error).__name__
            else:
                workflow_absent = False
                lookup_error = None
            observed["invalid"][label] = {"a2a_response": response,
                                           "run_rows": run_rows,
                                           "workflow_absent": workflow_absent,
                                           "lookup_error": lookup_error}
            assert not run_rows and workflow_absent
            assert response.get("kind") == "message"
            assert "error" in response["parts"][0]["data"]

        accepted_run = "director-mixed-accept"
        accepted_start = await asyncio.to_thread(director_send,
            start_command(accepted_run, "after_first_repair"))
        accepted_result = await asyncio.wait_for(
            client.get_workflow_handle(accepted_run).result(), timeout=120)
        accepted_task = await asyncio.to_thread(director_task, accepted_start["id"])
        observed["runs"]["after_first_repair"] = {
            "run_id": accepted_run, "original_task_id": accepted_start["id"],
            "original_task_state": accepted_task["status"]["state"],
            "task": accepted_task, "native_result": accepted_result}
        assert accepted_task["status"]["state"] == "completed"
        assert accepted_result["child"]["acceptance"]["revision"] == "r2"
        assert accepted_result["child"]["released"] is True

        exhausted_run = "director-mixed-exhaust"
        exhausted_start = await asyncio.to_thread(director_send,
            start_command(exhausted_run, "never"))
        async def wait_status():
            parent = await client.get_workflow_handle(exhausted_run).query(FactoryRun.status)
            if not parent["child_id"]:
                return False
            child = await client.get_workflow_handle(parent["child_id"]).query(FactoryRun.status)
            return child if child["phase"] == "awaiting-director" else False
        waiting = await until(wait_status, "r3 awaiting Director", 120)
        original_wait = await asyncio.to_thread(director_task, exhausted_start["id"])
        abort = await asyncio.to_thread(director_send, {
            "op": "abort", "action_id": "abort:" + exhausted_run,
            "run_id": exhausted_run, "revision": waiting["current_revision"],
            "sha256": waiting["current_sha256"]})
        exhausted_result = await asyncio.wait_for(
            client.get_workflow_handle(exhausted_run).result(), timeout=90)
        exhausted_task = await asyncio.to_thread(director_task, exhausted_start["id"])
        observed["runs"]["never"] = {
            "run_id": exhausted_run, "original_task_id": exhausted_start["id"],
            "original_task_wait_state": original_wait["status"]["state"],
            "waiting_child": waiting, "abort_response": abort,
            "original_task_state": exhausted_task["status"]["state"],
            "task": exhausted_task, "native_result": exhausted_result}
        assert waiting["current_revision"] == "r3"
        assert original_wait["status"]["state"] == "input-required"
        assert exhausted_task["status"]["state"] == "completed"
        assert exhausted_result["status"] == "aborted"
        assert exhausted_result["child"]["released"] is False
        assert exhausted_result["child"].get("acceptance") is None

        run_rows = rows(state / "director" / "director.sqlite3",
                        "SELECT * FROM runs ORDER BY run_id")
        observed["run_records"] = run_rows
        for label, run in (("after_first_repair", accepted_run), ("never", exhausted_run)):
            record = next(row for row in run_rows if row["run_id"] == run)
            parent = await client.get_workflow_handle(run).query(FactoryRun.status)
            child = await client.get_workflow_handle(parent["child_id"]).query(FactoryRun.status)
            observed["runs"][label]["parent_status"] = parent
            observed["runs"][label]["child_status"] = child
            assert record["run_inputs_digest"] == digest(json.loads(record["run_inputs_json"]))
            assert parent["run_inputs_digest"] == child["run_inputs_digest"] == record["run_inputs_digest"]
            assert parent["run_inputs"] == child["run_inputs"] == json.loads(record["run_inputs_json"])
            child_run = parent["child_id"]
            release_rows = rows(state / "release" / "release.sqlite3",
                                "SELECT * FROM releases WHERE run_id=?", (child_run,))
            quality_rows = rows(state / "quality" / "harness.sqlite3",
                                "SELECT * FROM actions WHERE run_id=?", (child_run,))
            observed["runs"][label]["release_rows"] = release_rows
            observed["runs"][label]["quality_count"] = len(quality_rows)
        assert len(observed["runs"]["after_first_repair"]["release_rows"]) == 1
        assert observed["runs"]["after_first_repair"]["quality_count"] == 2
        assert len(observed["runs"]["never"]["release_rows"]) == 0
        assert observed["runs"]["never"]["quality_count"] == 3
        assert (observed["runs"]["after_first_repair"]["child_status"]["definition_digest"]
                == observed["runs"]["never"]["child_status"]["definition_digest"])
        history_dir = HERE / "histories"
        history_dir.mkdir(exist_ok=True)
        observed["histories"] = {}
        for label, run in (("after_first_repair", accepted_run), ("never", exhausted_run)):
            child_id = observed["runs"][label]["parent_status"]["child_id"]
            for role, workflow_id in (("parent", run), ("child", child_id)):
                history = await client.get_workflow_handle(workflow_id).fetch_history()
                content = history.to_json()
                path = history_dir / f"{label}-{role}.json"
                path.write_text(content)
                observed["histories"][f"{label}-{role}"] = {
                    "workflow_id": workflow_id, "file": str(path.relative_to(HERE)),
                    "events": len(history.events),
                    "sha256": hashlib.sha256(content.encode()).hexdigest()}
        observed["status"] = "passed"
    except Exception as error:
        observed["status"] = "failed"
        observed["error"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        if observed["status"] == "failed":
            observed["log_tails"] = {
                name: path.read_text(errors="replace")[-5000:]
                for name in ("temporal.log", "postgres.log", "worker.log", "director.log")
                if (path := state / name).exists()}
        (HERE / "trial_observed.json").write_text(json.dumps(observed, indent=2, sort_keys=True) + "\n")
        for process in processes.values():
            try:
                runtime.kill(process, signal.SIGTERM)
            except Exception as error:
                cleanup_errors.append(f"process {process.pid}: {error}")
        for process in (worker, server):
            if process is not None:
                try:
                    runtime.kill(process, signal.SIGTERM)
                except Exception as error:
                    cleanup_errors.append(f"process {process.pid}: {error}")
        if pg_running:
            try:
                runtime.pg_command("-m", "fast", "stop")
            except Exception as error:
                cleanup_errors.append(f"postgres: {error}")
        if not cleanup_errors:
            shutil.rmtree(state)
        observed["cleanup"] = {"errors": cleanup_errors, "state_removed": not state.exists()}
        (HERE / "trial_observed.json").write_text(json.dumps(observed, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"status": observed["status"], "cleanup": observed["cleanup"]}))


if __name__ == "__main__":
    asyncio.run(main())
