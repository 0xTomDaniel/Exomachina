"""Measure complete local Dagu bundle while A2A assignments remain working."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import time

HERE = Path(__file__).resolve().parent
PRIOR = HERE.parent.parent / "2026-09-22"
sys.path.insert(0, str(PRIOR / "arbitration" / "wait-scaling"))
sys.path.insert(0, str(PRIOR / "decision-round" / "common"))
from footprint_probe import sample  # noqa: E402
from client import get_task as get_remote_task  # noqa: E402

import ops  # noqa: E402
from publisher import publish  # noqa: E402
from trial import DAGU, PYTHON, ROOT, child, native, start, supervisor_start, supervisor_stop, task, until  # noqa: E402


def pending(rt: Path) -> list[dict]:
    path = rt / "source_evidence" / "harness.sqlite3"
    if not path.is_file():
        return []
    with sqlite3.connect(path) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute("SELECT * FROM pending ORDER BY action_id")]


def roles(rt: Path, supervisor_pid: int) -> dict[int, str]:
    registry = json.loads((rt / "processes.json").read_text())
    return {supervisor_pid: "supervisor", **{item["pid"]: name for name, item in registry.items()}}


def in_flight(rt: Path, admissions: list[dict]) -> list[dict] | bool:
    by_action = {row["action_id"]: row for row in pending(rt)}
    url = json.loads((rt / "config.json").read_text())["urls"]["source_evidence"]
    rows = []
    for admitted in admissions:
        parent_id = admitted["parent_run_id"]
        bound = child(parent_id)
        if not bound:
            return False
        action_id = bound["run_id"] + ":source_evidence"
        remote = by_action.get(action_id)
        if remote is None:
            return False
        parent_native = native(ROOT, parent_id)
        child_native = native(bound["dag_name"], bound["run_id"])
        director_task = task(rt, admitted["task_id"])
        remote_task = get_remote_task(url, remote["task_id"])
        if (not parent_native or not child_native
                or parent_native["nodes"]["child_gate"]["status"] != "waiting"
                or child_native["status"] != "running"
                or child_native["nodes"]["source"]["status"] != "running"
                or director_task["id"] != admitted["task_id"]
                or director_task["status"]["state"] != "input-required"
                or remote_task["id"] != remote["task_id"]
                or remote_task["status"]["state"] != "working"
                or remote_task["metadata"]["action_id"] != action_id):
            return False
        rows.append({"parent_run_id": parent_id, "parent_native": parent_native["status"],
                     "child_run_id": bound["run_id"], "child_native": child_native["status"],
                     "child_source_node": child_native["nodes"]["source"]["status"],
                     "child_definition_digest": bound["definition_digest"],
                     "director_task_id": admitted["task_id"],
                     "director_task_status": director_task["status"]["state"],
                     "source_action_id": action_id, "source_a2a_task_id": remote["task_id"],
                     "source_a2a_task_status": remote_task["status"]["state"]})
    return rows


def run(max_level: int) -> dict:
    if not PYTHON.is_file() or not DAGU.is_file():
        raise RuntimeError("pinned S2 Python or Dagu binary unavailable")
    rt = Path(tempfile.mkdtemp(prefix="exo-dagu-inflight-", dir="/tmp"))
    os.environ["EXO_ARB_RUNTIME"] = str(rt)
    result = {"schema": "exomachina.dagu.inflight-scale/1", "runtime": str(rt),
              "method": "actual Director A2A Tasks and working source A2A Tasks",
              "levels": {}, "admissions": [], "status": "running"}
    supervisor = None
    try:
        manifest = publish(HERE / "definitions" / "v6", rt / "home", ROOT, DAGU)
        result["definition_closure_sha256"] = manifest["closure_sha256"]
        (rt / "hold-source-assignments").write_text("bounded source fixture hold\n")
        supervisor = supervisor_start(rt)
        result["levels"]["0"] = {"in_flight": [],
            "snapshot": sample(supervisor.pid, rt, roles(rt, supervisor.pid))}
        for target in (2, 10):
            if target > max_level:
                break
            stage = time.monotonic()
            for index in range(len(result["admissions"]) + 1, target + 1):
                parent_id = f"exo-arb-inflight-{index}-{rt.name}"
                started = start(rt, parent_id, resolve=True)
                result["admissions"].append({"parent_run_id": parent_id,
                    "task_id": started["id"], "start_task_status": started["status"]["state"]})
            rows = until(lambda: in_flight(rt, result["admissions"]),
                         f"{target} native running assignments and A2A working Tasks", 60)
            level = {"in_flight": rows, "stage_elapsed_seconds": time.monotonic() - stage,
                     "snapshot": sample(supervisor.pid, rt, roles(rt, supervisor.pid))}
            # Confirm the same tasks remain pending after the footprint invocation.
            level["after_sample"] = in_flight(rt, result["admissions"])
            if level["after_sample"] is False:
                raise RuntimeError("held assignments changed status during footprint sample")
            result["levels"][str(target)] = level
        result["status"] = "passed"
    except Exception as error:
        result["status"] = "failed"
        result["error"] = repr(error)
        raise
    finally:
        supervisor_stop(rt, supervisor)
        result["supervisor_exit_code"] = supervisor.returncode if supervisor else None
        (HERE / "observed.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-level", type=int, choices=(2, 10), default=10)
    args = parser.parse_args()
    observed = run(args.max_level)
    print(json.dumps({"status": observed["status"],
                      "levels": sorted(observed["levels"]), "runtime": observed["runtime"]},
                     sort_keys=True))
