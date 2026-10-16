"""0/2/10 complete-bundle footprint with working external A2A source Tasks."""
from __future__ import annotations

import asyncio
import hashlib
import json
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

from temporalio.client import Client

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "2026-09-22" / "arbitration" / "wait-scaling"))
from footprint_probe import sample  # noqa: E402
from long_client import get_task as get_remote_task  # noqa: E402
import runtime  # noqa: E402
from author import materialize, template  # noqa: E402
from definition import publish  # noqa: E402
from factory import FactoryRun  # noqa: E402
from probe import SERVICE_PORTS, bindings, director_send, director_task  # noqa: E402
from recovery_probe import frozen_original_unchanged, source_hashes  # noqa: E402


def roles(state: Path, supervisor_pid: int) -> dict[int, str]:
    events = [json.loads(line) for line in (state / "supervisor-events.jsonl").read_text().splitlines()]
    result = {supervisor_pid: "supervisor"}
    for event in events:
        if event["kind"] in {"temporal-start", "temporal-restart"}:
            result[event["pid"]] = "temporal_server"
        elif event["kind"] in {"worker-start", "worker-restart"}:
            result[event["pid"]] = "workflow_worker"
        elif event["kind"] in {"service-start", "service-restart"}:
            result[event["pid"]] = event["name"]
    return result


def pending(state: Path) -> list[dict]:
    path = state / "source" / "harness.sqlite3"
    if not path.is_file():
        return []
    with sqlite3.connect(path) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute("SELECT * FROM pending ORDER BY action_id")]


def count_rows(path: Path, table: str) -> int:
    with sqlite3.connect(path) as db:
        return int(db.execute(f"SELECT count(*) FROM {table}").fetchone()[0])


def guard_counts(state: Path) -> dict:
    return {
        "source_pending_tasks": count_rows(state / "source" / "harness.sqlite3", "pending"),
        "source_committed_actions": count_rows(state / "source" / "harness.sqlite3", "actions"),
        "counter_committed_actions": count_rows(state / "counter" / "harness.sqlite3", "actions"),
        "quality_committed_verdicts": count_rows(state / "quality" / "harness.sqlite3", "actions"),
        "release_rows": count_rows(state / "release" / "release.sqlite3", "releases"),
    }


async def in_flight(state: Path, client: Client, admissions: list[dict]) -> list[dict] | bool:
    rows = pending(state)
    by_action = {row["action_id"]: row for row in rows}
    if len(rows) != len(admissions) or len(by_action) != len(rows):
        return False
    observed = []
    try:
        for admitted in admissions:
            parent_id = admitted["parent_workflow_id"]
            parent_handle = client.get_workflow_handle(parent_id)
            parent = await parent_handle.query(FactoryRun.status)
            if parent["phase"] != "awaiting-child" or not parent["child_id"]:
                return False
            child_handle = client.get_workflow_handle(parent["child_id"])
            child = await child_handle.query(FactoryRun.status)
            child_description = await child_handle.describe()
            parent_description = await parent_handle.describe()
            action_id = parent["child_id"] + ":source_evidence"
            remote = by_action.get(action_id)
            if remote is None:
                return False
            external = await asyncio.to_thread(get_remote_task,
                f"http://127.0.0.1:{SERVICE_PORTS['source']}", remote["task_id"])
            director = await asyncio.to_thread(director_task, admitted["director_task_id"])
            if (child["phase"] != "parallel" or child["authoritative_acceptance"] is not None
                    or child["release_receipt"] is not None
                    or director["status"]["state"] != "input-required"
                    or external["status"]["state"] != "working"
                    or external["metadata"]["action_id"] != action_id
                    or external["metadata"]["definition_digest"] != child["definition_digest"]):
                return False
            observed.append({"parent_workflow_id": parent_id,
                "parent_native_run_id": parent_description.run_id, "parent_phase": parent["phase"],
                "child_workflow_id": parent["child_id"], "child_native_run_id": child_description.run_id,
                "child_phase": child["phase"], "child_definition_digest": child["definition_digest"],
                "child_authoritative_acceptance": child["authoritative_acceptance"],
                "child_release_receipt": child["release_receipt"],
                "director_task_id": director["id"], "director_task_state": director["status"]["state"],
                "source_action_id": action_id, "source_a2a_task_id": remote["task_id"],
                "source_a2a_task_state": external["status"]["state"]})
    except Exception:
        return False
    return observed


async def run() -> dict:
    state = Path(tempfile.mkdtemp(prefix="exo-temporal-inflight-scale-", dir="/tmp"))
    runtime.STATE = state
    result = {"schema": "exomachina.temporal.inflight-scale/1", "state": str(state),
              "status": "running", "levels": {}, "admissions": [],
              "source_hashes_before": source_hashes(),
              "original_frozen_unchanged_before": frozen_original_unchanged(),
              "ports": {"engine": runtime.PORTS, "services": SERVICE_PORTS}}
    supervisor = None
    try:
        occupied = [port for port in list(runtime.PORTS.values()) + list(SERVICE_PORTS.values())
                    if runtime.port_open(port)]
        if occupied:
            raise RuntimeError(f"Temporal fixture ports occupied: {occupied}")
        assert result["original_frozen_unchanged_before"]
        (state / "hold-source-assignments").write_text("bounded source fixture hold\n")
        started = time.monotonic()
        with (state / "supervisor.log").open("a") as log:
            supervisor = subprocess.Popen([sys.executable, str(HERE / "supervisor.py"),
                "--state", str(state)], cwd=HERE, stdout=log, stderr=log, start_new_session=True)
        async def ready():
            if supervisor.poll() is not None:
                raise RuntimeError(f"supervisor exited {supervisor.returncode}")
            return (state / "supervisor-ready").is_file()
        await runtime.until(ready, seconds=120)
        result["ready_elapsed_seconds"] = time.monotonic() - started
        client = await Client.connect(f"127.0.0.1:{runtime.PORTS['frontend']}", namespace="exomachina")
        identities = {}
        for name in ("source", "counter", "quality", "release"):
            with urllib.request.urlopen(f"http://127.0.0.1:{SERVICE_PORTS[name]}/health") as response:
                identities[name] = json.load(response)
        approved = bindings(identities)
        (state / "catalog" / "approved_bindings.json").write_text(json.dumps(approved, sort_keys=True))
        package = materialize(template("visible-v3.json"), approved)
        result["package_digest"] = publish(package, state / "catalog", approved)
        result["child_definition_digest"] = next(iter(package["children"]))
        pg_pid = int((state / "pgdata" / "postmaster.pid").read_text().splitlines()[0])
        def footprint():
            return sample(supervisor.pid, state, roles(state, supervisor.pid),
                          {pg_pid: "postgres_postmaster"})
        result["levels"]["0"] = {"in_flight": [], "guard_counts": guard_counts(state),
                                  "snapshot": footprint()}
        for target in (2, 10):
            stage = time.monotonic()
            for index in range(len(result["admissions"]) + 1, target + 1):
                parent_id = f"temporal-inflight-{index}-{state.name}"
                task = await asyncio.to_thread(director_send, {"op": "start",
                    "action_id": "start:" + parent_id, "run_id": parent_id,
                    "package_digest": result["package_digest"]})
                description = await client.get_workflow_handle(parent_id).describe()
                result["admissions"].append({"parent_workflow_id": parent_id,
                    "parent_native_run_id": description.run_id, "director_task_id": task["id"],
                    "initial_task_state": task["status"]["state"]})
            async def all_held():
                return await in_flight(state, client, result["admissions"])
            held = await runtime.until(all_held, seconds=75)
            await asyncio.sleep(2)
            held = await all_held()
            if held is False:
                raise RuntimeError(f"{target} working A2A assignments changed during settle")
            snapshot = footprint()
            again = await all_held()
            if again is False:
                raise RuntimeError(f"{target} working A2A assignments changed during footprint sample")
            result["levels"][str(target)] = {"in_flight": held,
                "stage_elapsed_seconds": time.monotonic() - stage,
                "snapshot": snapshot, "after_sample": again,
                "guard_counts": guard_counts(state)}
            counts = result["levels"][str(target)]["guard_counts"]
            assert counts == {"source_pending_tasks": target, "source_committed_actions": 0,
                              "counter_committed_actions": target,
                              "quality_committed_verdicts": 0, "release_rows": 0}
        result["source_hashes_after"] = source_hashes()
        result["original_frozen_unchanged_after"] = frozen_original_unchanged()
        assert result["source_hashes_after"] == result["source_hashes_before"]
        assert result["original_frozen_unchanged_after"]
        result["status"] = "passed"
    except Exception as error:
        result["status"] = "failed"
        result["error"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        if supervisor and supervisor.poll() is None:
            supervisor.send_signal(signal.SIGTERM)
            supervisor.wait(timeout=30)
        result["supervisor_exit_code"] = supervisor.returncode if supervisor else None
        (HERE / "held_assignment_observed.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return result


if __name__ == "__main__":
    observed = asyncio.run(run())
    print(json.dumps({"status": observed["status"], "state": observed["state"],
                      "levels": sorted(observed["levels"])}, sort_keys=True))
