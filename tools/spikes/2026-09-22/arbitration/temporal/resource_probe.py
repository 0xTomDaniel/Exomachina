"""Measure the full local topology with two child Director waits."""
from __future__ import annotations

import asyncio
import json
import signal
import subprocess
import sys
import tempfile
import time
import re
import urllib.request
from datetime import timedelta
from pathlib import Path

from temporalio.client import Client

import runtime
from author import materialize, template
from definition import publish
from factory import FactoryRun
from probe import SERVICE_PORTS, bindings, input_for

HERE = Path(__file__).resolve().parent


def ps_table():
    table = {}
    for line in runtime.shell(["ps", "-axo", "pid=,ppid=,rss="]).splitlines():
        values = line.split()
        if len(values) == 3:
            table[int(values[0])] = (int(values[1]), int(values[2]))
    return table


def sample(state, supervisor_pid):
    log = [json.loads(line) for line in (state / "supervisor-events.jsonl").read_text().splitlines()]
    pids = {"supervisor": supervisor_pid}
    for event in log:
        if event["kind"] in {"temporal-start", "temporal-restart"}:
            pids["temporal_server"] = event["pid"]
        if event["kind"] in {"worker-start", "worker-restart"}:
            pids["worker"] = event["pid"]
        if event["kind"] in {"service-start", "service-restart"}:
            pids[event["name"]] = event["pid"]
    pg_pid = int((state / "pgdata" / "postmaster.pid").read_text().splitlines()[0])
    table = ps_table()
    pg_tree = {pg_pid}
    while True:
        added = {pid for pid, (parent, _) in table.items() if parent in pg_tree}
        if added <= pg_tree:
            break
        pg_tree |= added
    values = {name: table.get(pid, (None, 0))[1] for name, pid in pids.items()}
    values["postgres_summed_rss_kib_shared_pages_double_counted"] = sum(
        table.get(pid, (None, 0))[1] for pid in pg_tree)
    target_pids = sorted(set(pids.values()) | pg_tree)
    footprint = subprocess.run(["footprint", "--noCategories", "--format", "bytes",
        *[str(pid) for pid in target_pids]], capture_output=True, text=True, timeout=30)
    match = re.search(r"Summary Footprint:\s*([0-9]+) B", footprint.stdout)
    return {"pids": pids, "rss_kib": values,
            "sum_rss_kib_shared_pages_double_counted": sum(values.values()),
            "postgres_process_count": len(pg_tree),
            "physical_footprint_bytes_deduplicated": int(match.group(1)) if match else None,
            "footprint_exit_code": footprint.returncode,
            "footprint_target_pids": target_pids}


async def main():
    state = Path(tempfile.mkdtemp(prefix="exo-temporal-resources-", dir="/tmp"))
    runtime.STATE = state
    log = (state / "supervisor.log").open("a")
    started_at = time.monotonic()
    supervisor = subprocess.Popen([sys.executable, str(HERE / "supervisor.py"),
        "--state", str(state)], cwd=HERE, stdout=log, stderr=log, start_new_session=True)
    log.close()
    observed = {"status": "running", "state": str(state)}
    try:
        async def ready():
            if supervisor.poll() is not None:
                raise RuntimeError(f"supervisor exited {supervisor.returncode}")
            return (state / "supervisor-ready").exists()
        await runtime.until(ready, seconds=90)
        observed["first_bootstrap_seconds_to_ready"] = time.monotonic() - started_at
        observed["warm"] = sample(state, supervisor.pid)
        client = await Client.connect(f"127.0.0.1:{runtime.PORTS['frontend']}",
                                      namespace="exomachina")
        identities = {}
        for name in ("source", "counter", "quality", "release", "opaque"):
            with urllib.request.urlopen(f"http://127.0.0.1:{SERVICE_PORTS[name]}/health") as response:
                identities[name] = json.load(response)
        authority = bindings(identities)
        (state / "catalog" / "approved_bindings.json").write_text(json.dumps(authority))
        package = materialize(template("visible-exhausted.json"), authority)
        digest_value = publish(package, state / "catalog", authority)
        director = {"identity": "resource-director", "token": "fixture-local-token", "epoch": 1}
        runs = []
        for number in (1, 2):
            run_id = f"resource-wait-{number}"
            await client.start_workflow(FactoryRun.run, input_for(run_id, package, director),
                id=run_id, task_queue="arbitration-temporal",
                execution_timeout=timedelta(minutes=10))
            runs.append(run_id)
        async def two_waits():
            statuses = []
            try:
                for run_id in runs:
                    parent = await client.get_workflow_handle(run_id).query(FactoryRun.status)
                    if not parent["child_id"]:
                        return False
                    child = await client.get_workflow_handle(parent["child_id"]).query(FactoryRun.status)
                    if child["phase"] != "awaiting-director":
                        return False
                    statuses.append({"parent": parent, "child": child})
            except Exception:
                return False
            return statuses
        observed["two_waits"] = await runtime.until(two_waits, seconds=60)
        observed["active"] = sample(state, supervisor.pid)
        observed["package_digest"] = digest_value
        observed["disk_kib"] = {
            "state": int(runtime.shell(["du", "-sk", str(state)]).split()[0]),
            "venv": int(runtime.shell(["du", "-sk", str(HERE / ".venv")]).split()[0]),
            "server_binary": int(runtime.shell(["du", "-sk", str(runtime.TEMPORAL)]).split()[0]),
            "sql_tool": int(runtime.shell(["du", "-sk", str(runtime.SQL_TOOL)]).split()[0]),
        }
        supervisor.send_signal(signal.SIGTERM)
        supervisor.wait(timeout=20)
        relaunch_at = time.monotonic()
        log = (state / "supervisor-relaunch.log").open("a")
        supervisor = subprocess.Popen([sys.executable, str(HERE / "supervisor.py"),
            "--state", str(state)], cwd=HERE, stdout=log, stderr=log,
            start_new_session=True)
        log.close()
        await runtime.until(ready, seconds=90)
        observed["existing_state_relaunch_seconds_to_ready"] = time.monotonic() - relaunch_at
        client = await Client.connect(f"127.0.0.1:{runtime.PORTS['frontend']}",
                                      namespace="exomachina")
        observed["restored_two_waits"] = await runtime.until(two_waits, seconds=60)
        observed["status"] = "passed"
    except Exception as error:
        observed["status"] = "failed"
        observed["error"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        (HERE / "resource-observed.json").write_text(json.dumps(observed, indent=2, sort_keys=True) + "\n")
        if supervisor.poll() is None:
            supervisor.send_signal(signal.SIGTERM)
            supervisor.wait(timeout=20)
        print(json.dumps({"status": observed["status"], "state": str(state)}))


if __name__ == "__main__":
    asyncio.run(main())
