"""Cumulative 0/2/10 nested Director-wait footprint for frozen Temporal."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import urllib.request

HERE = Path(__file__).resolve().parent
CANDIDATE = HERE.parent / "temporal"
sys.path.insert(0, str(CANDIDATE))

from temporalio.client import Client

from footprint_probe import sample
import runtime
from author import materialize, template
from definition import publish
from factory import FactoryRun
from probe import SERVICE_PORTS, bindings, director_send, director_task


def _roles(state: Path, supervisor_pid: int) -> dict[int, str]:
    events = [json.loads(line) for line in
              (state / "supervisor-events.jsonl").read_text().splitlines()]
    roles = {supervisor_pid: "supervisor"}
    for event in events:
        if event["kind"] in {"temporal-start", "temporal-restart"}:
            roles[event["pid"]] = "temporal_server"
        elif event["kind"] in {"worker-start", "worker-restart"}:
            roles[event["pid"]] = "workflow_worker"
        elif event["kind"] in {"service-start", "service-restart"}:
            roles[event["pid"]] = event["name"]
    return roles


async def _waits(client: Client, rows: list[dict]) -> list[dict] | bool:
    observed = []
    try:
        for row in rows:
            parent_handle = client.get_workflow_handle(row["parent_workflow_id"])
            parent = await parent_handle.query(FactoryRun.status)
            if parent["phase"] != "awaiting-child" or not parent["child_id"]:
                return False
            child_handle = client.get_workflow_handle(parent["child_id"])
            child = await child_handle.query(FactoryRun.status)
            if child["phase"] != "awaiting-director":
                return False
            child_description = await child_handle.describe()
            observed.append({
                "parent_workflow_id": row["parent_workflow_id"],
                "parent_native_run_id": row["parent_native_run_id"],
                "parent_phase": parent["phase"],
                "child_workflow_id": parent["child_id"],
                "child_native_run_id": child_description.run_id,
                "child_phase": child["phase"],
                "child_current_revision": child["current_revision"],
                "child_repair_count": child["repair_count"],
                "child_definition_digest": child["definition_digest"],
                "observed_wait_unix_seconds": time.time()})
    except Exception:
        return False
    return observed


async def main() -> None:
    state = Path(tempfile.mkdtemp(prefix="exo-arb-wait-scaling-temporal-", dir="/tmp"))
    runtime.STATE = state
    result = {"schema": "exomachina.arbitration.wait-scaling/1",
              "candidate": "temporal-server-v1.32.0", "state": str(state),
              "start_route": "Director A2A Task",
              "started_unix_seconds": time.time(), "status": "running",
              "levels": {}, "admissions": [], "failures": [],
              "ports": {"engine": runtime.PORTS, "services": SERVICE_PORTS}}
    supervisor = None
    try:
        occupied = [port for port in list(runtime.PORTS.values()) + list(SERVICE_PORTS.values())
                    if runtime.port_open(port)]
        if occupied:
            raise RuntimeError(f"candidate ports occupied: {occupied}")
        for required in (runtime.TEMPORAL, runtime.SQL_TOOL, runtime.SCHEMA,
                         runtime.PG_BIN / "initdb", runtime.CLI):
            if not required.exists():
                raise FileNotFoundError(required)
        launched = time.monotonic()
        with (state / "supervisor.log").open("wb") as log:
            supervisor = subprocess.Popen(
                [sys.executable, str(CANDIDATE / "supervisor.py"), "--state", str(state)],
                cwd=CANDIDATE, stdout=log, stderr=log, start_new_session=True)
        async def ready():
            if supervisor.poll() is not None:
                raise RuntimeError(f"supervisor exited {supervisor.returncode}")
            return (state / "supervisor-ready").is_file()
        await runtime.until(ready, seconds=120)
        result["ready_elapsed_seconds"] = time.monotonic() - launched
        pg_pid = int((state / "pgdata" / "postmaster.pid").read_text().splitlines()[0])
        def footprint():
            return sample(supervisor.pid, state, _roles(state, supervisor.pid),
                          {pg_pid: "postgres_postmaster"})
        result["levels"]["0"] = {"waiting_pairs": [], "snapshot": footprint()}
        client = await Client.connect(f"127.0.0.1:{runtime.PORTS['frontend']}",
                                      namespace="exomachina")
        identities = {}
        for name in ("source", "counter", "quality", "release", "opaque"):
            with urllib.request.urlopen(f"http://127.0.0.1:{SERVICE_PORTS[name]}/health") as response:
                identities[name] = json.load(response)
        authority = bindings(identities)
        (state / "catalog" / "approved_bindings.json").write_text(json.dumps(authority))
        package = materialize(template("visible-exhausted.json"), authority)
        result["package_digest"] = publish(package, state / "catalog", authority)
        for target in (2, 10):
            level_started = time.monotonic()
            for index in range(len(result["admissions"]) + 1, target + 1):
                workflow_id = f"wait-scaling-{index}-{state.name}"
                attempted = time.monotonic()
                task = await asyncio.to_thread(director_send, {
                    "op": "start", "action_id": "start:" + workflow_id,
                    "run_id": workflow_id, "package_digest": result["package_digest"]})
                description = await client.get_workflow_handle(workflow_id).describe()
                result["admissions"].append({
                    "parent_workflow_id": workflow_id,
                    "parent_native_run_id": description.run_id,
                    "director_task_id": task["id"],
                    "director_task_initial_state": task["status"]["state"],
                    "admitted_unix_seconds": time.time(),
                    "admission_seconds": time.monotonic() - attempted})
            async def all_waits():
                return await _waits(client, result["admissions"])
            waits = await runtime.until(all_waits, seconds=180)
            await asyncio.sleep(2)
            waits = await all_waits()
            if waits is False:
                raise RuntimeError(f"{target} waits did not remain stable through settle")
            level = {"waiting_pairs": waits,
                     "all_waits_elapsed_seconds": time.monotonic() - level_started,
                     "snapshot": footprint()}
            level["director_task_states"] = [
                {"task_id": row["director_task_id"],
                 "state": (await asyncio.to_thread(director_task,
                                                    row["director_task_id"]))["status"]["state"]}
                for row in result["admissions"]]
            if target == 10:
                await asyncio.sleep(3)
                level["stability_waiting_pairs"] = await all_waits()
                if level["stability_waiting_pairs"] is False:
                    raise RuntimeError("10 waits did not remain stable at repeat sample")
                level["stability_snapshot"] = footprint()
            result["levels"][str(target)] = level
        result["status"] = "passed"
    except Exception as error:
        result["status"] = "failed"
        result["failures"].append({"type": type(error).__name__, "message": str(error),
                                   "at_unix_seconds": time.time()})
    finally:
        if supervisor is not None and supervisor.poll() is None:
            supervisor.send_signal(signal.SIGTERM)
            try:
                supervisor.wait(timeout=30)
            except subprocess.TimeoutExpired:
                supervisor.kill()
                supervisor.wait(timeout=10)
                result["failures"].append({"type": "ShutdownTimeout",
                                           "message": "supervisor required SIGKILL"})
        result["supervisor_exit_code"] = supervisor.returncode if supervisor else None
        result["finished_unix_seconds"] = time.time()
        (HERE / "temporal-director.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"candidate": result["candidate"], "status": result["status"],
                          "state": str(state), "levels": sorted(result["levels"])}, sort_keys=True))
    if result["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
