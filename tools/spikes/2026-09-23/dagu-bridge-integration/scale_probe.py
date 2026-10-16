"""Matched whole-bundle 0/2/10 processless nested-wait probe."""
from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import time

HERE = Path(__file__).resolve().parent
PRIOR = HERE.parent.parent / "2026-09-22"
sys.path.insert(0, str(PRIOR / "arbitration" / "wait-scaling"))
from footprint_probe import sample  # noqa: E402

import ops  # noqa: E402
from publisher import publish  # noqa: E402
from trial import DAGU, PYTHON, ROOT, child, native, start, supervisor_start, supervisor_stop, task, until  # noqa: E402


def roles(rt: Path, supervisor_pid: int) -> dict[int, str]:
    registry = json.loads((rt / "processes.json").read_text())
    return {supervisor_pid: "supervisor", **{item["pid"]: name for name, item in registry.items()}}


def waiting(rt: Path, rows: list[dict]) -> list[dict] | bool:
    result = []
    for item in rows:
        bound = child(item["parent_run_id"])
        if not bound:
            return False
        parent_native = native(ROOT, item["parent_run_id"])
        child_native = native(bound["dag_name"], bound["run_id"])
        director_task = task(rt, item["task_id"])
        if (not parent_native or not child_native
                or parent_native["status"] != "waiting"
                or parent_native["nodes"]["child_gate"]["status"] != "waiting"
                or child_native["status"] != "waiting"
                or child_native["nodes"]["director_wait"]["status"] != "waiting"
                or director_task["id"] != item["task_id"]
                or director_task["status"]["state"] != "input-required"
                or director_task["metadata"]["child"]["run_id"] != bound["run_id"]
                or director_task["metadata"]["definition_digest"] != bound["definition_digest"]):
            return False
        result.append({"parent_run_id": item["parent_run_id"], "parent_native": parent_native["status"],
                       "child_run_id": bound["run_id"], "child_native": child_native["status"],
                       "child_definition_digest": bound["definition_digest"],
                       "task_id": item["task_id"], "task_status": director_task["status"]["state"],
                       "observed_wait_unix_seconds": time.time()})
    return result


def run() -> dict:
    if not PYTHON.is_file() or not DAGU.is_file():
        raise RuntimeError("pinned S2 Python or Dagu binary unavailable")
    rt = Path(tempfile.mkdtemp(prefix="exo-dagu-bridge-scaling-", dir="/tmp"))
    os.environ["EXO_ARB_RUNTIME"] = str(rt)
    result = {"schema": "exomachina.dagu-bridge-integration.wait-scaling/1",
              "runtime": str(rt), "levels": {}, "admissions": [], "status": "running"}
    supervisor = None
    try:
        manifest = publish(HERE / "definitions" / "v6", rt / "home", ROOT, DAGU)
        result["definition_closure_sha256"] = manifest["closure_sha256"]
        launched = time.monotonic()
        supervisor = supervisor_start(rt)
        result["ready_elapsed_seconds"] = time.monotonic() - launched
        result["levels"]["0"] = {"waiting_pairs": [],
            "snapshot": sample(supervisor.pid, rt, roles(rt, supervisor.pid))}
        for target in (2, 10):
            stage = time.monotonic()
            for index in range(len(result["admissions"]) + 1, target + 1):
                parent_id = f"exo-arb-bridge-scale-{index}-{rt.name}"
                admitted = time.monotonic()
                task = start(rt, parent_id, resolve=False)
                result["admissions"].append({"parent_run_id": parent_id,
                    "task_id": task["id"], "admission_seconds": time.monotonic() - admitted})
            pairs = until(lambda: waiting(rt, result["admissions"]), f"{target} native wait pairs", 180)
            time.sleep(2)
            pairs = waiting(rt, result["admissions"])
            if pairs is False:
                raise RuntimeError("waits did not remain stable through settle")
            level = {"waiting_pairs": pairs, "all_waits_elapsed_seconds": time.monotonic() - stage,
                "snapshot": sample(supervisor.pid, rt, roles(rt, supervisor.pid))}
            if target == 10:
                time.sleep(3)
                level["stability_waiting_pairs"] = waiting(rt, result["admissions"])
                level["stability_snapshot"] = sample(supervisor.pid, rt, roles(rt, supervisor.pid))
            result["levels"][str(target)] = level
        result["service_start_counts"] = {name: info["starts"] for name, info in
            json.loads((rt / "processes.json").read_text()).items()}
        result["status"] = "passed"
    except Exception as error:
        result["status"] = "failed"
        result["error"] = repr(error)
        raise
    finally:
        supervisor_stop(rt, supervisor)
        result["supervisor_exit_code"] = supervisor.returncode if supervisor else None
        (HERE / "scale_observed.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return result


if __name__ == "__main__":
    result = run()
    print(json.dumps({"status": result["status"], "levels": sorted(result["levels"]),
                      "runtime": result["runtime"]}, sort_keys=True))
