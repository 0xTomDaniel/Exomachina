"""Cumulative 0/2/10 nested Director-wait footprint for frozen Dagu."""
from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

HERE = Path(__file__).resolve().parent
CANDIDATE = HERE.parent / "dagu"
sys.path.insert(0, str(CANDIDATE))

from footprint_probe import sample
from publisher import publish
from trial import DAGU, PYTHON, child_for, get_run, start_director, status, until


def _roles(state: Path, supervisor_pid: int) -> dict[int, str]:
    registry = json.loads((state / "processes.json").read_text())
    return {supervisor_pid: "supervisor", **{
        value["pid"]: name for name, value in registry.items()}}


def _waits(rows: list[dict]) -> list[dict] | bool:
    observed = []
    for row in rows:
        child = child_for(row["parent_run_id"])
        if child is None:
            return False
        native_child = status(child["dag_name"], child["run_id"], "waiting")
        if native_child is None:
            return False
        parent = get_run(row["parent_run_id"])
        native_parent = status(parent["dag_name"], row["parent_run_id"], "running")
        if native_parent is None:
            return False
        observed.append({"parent_run_id": row["parent_run_id"],
                         "parent_native_status": native_parent["statusLabel"],
                         "parent_product_state": parent["state"],
                         "child_run_id": child["run_id"],
                         "child_native_status": native_child["statusLabel"],
                         "child_product_state": child["state"],
                         "child_definition_digest": child["definition_digest"],
                         "observed_wait_unix_seconds": time.time()})
    return observed


def main() -> None:
    if not PYTHON.is_file() or not DAGU.is_file():
        raise SystemExit("pinned S2 Python or Dagu binary unavailable")
    state = Path(tempfile.mkdtemp(prefix="exo-arb-wait-scaling-dagu-", dir="/tmp"))
    os.environ["EXO_ARB_RUNTIME"] = str(state)
    result = {"schema": "exomachina.arbitration.wait-scaling/1",
              "candidate": "dagu-community-v2.17.0", "state": str(state),
              "start_route": "Director A2A Task",
              "started_unix_seconds": time.time(), "status": "running",
              "levels": {}, "admissions": [], "failures": []}
    supervisor = None
    try:
        manifest = publish(CANDIDATE / "definitions" / "v3", state / "home",
                           "exo_arb_parent_v3", DAGU)
        result["definition_closure_sha256"] = manifest["closure_sha256"]
        launched = time.monotonic()
        with (state / "supervisor.log").open("wb") as log:
            supervisor = subprocess.Popen(
                [str(PYTHON), str(CANDIDATE / "supervisor.py"), str(state),
                 "--dagu", str(DAGU)], stdout=log, stderr=log,
                start_new_session=True)
        def ready():
            if supervisor.poll() is not None:
                raise RuntimeError(f"supervisor exited {supervisor.returncode}")
            return (state / "ready").is_file()
        until(ready, "supervisor ready", 60)
        result["ready_elapsed_seconds"] = time.monotonic() - launched
        result["ports"] = json.loads((state / "ports.json").read_text())
        result["levels"]["0"] = {"waiting_pairs": [],
                                  "snapshot": sample(supervisor.pid, state, _roles(state, supervisor.pid))}
        for target in (2, 10):
            level_started = time.monotonic()
            for index in range(len(result["admissions"]) + 1, target + 1):
                parent = f"exo-arb-scaling-{index}-{state.name}"
                attempted = time.monotonic()
                task = start_director(state, parent, "exo_arb_parent_v3", resolve_input=False)
                result["admissions"].append({
                    "parent_run_id": parent, "director_task_id": task["id"],
                    "admitted_unix_seconds": time.time(),
                    "admission_seconds": time.monotonic() - attempted})
            waits = until(lambda: _waits(result["admissions"]),
                          f"{target} nested child Director waits", 180)
            time.sleep(2)
            waits = _waits(result["admissions"])
            if waits is False:
                raise RuntimeError(f"{target} waits did not remain stable through settle")
            level = {"waiting_pairs": waits,
                     "all_waits_elapsed_seconds": time.monotonic() - level_started,
                     "snapshot": sample(supervisor.pid, state, _roles(state, supervisor.pid))}
            if target == 10:
                time.sleep(3)
                level["stability_waiting_pairs"] = _waits(result["admissions"])
                if level["stability_waiting_pairs"] is False:
                    raise RuntimeError("10 waits did not remain stable at repeat sample")
                level["stability_snapshot"] = sample(supervisor.pid, state,
                                                      _roles(state, supervisor.pid))
            result["levels"][str(target)] = level
        result["status"] = "passed"
        result["service_start_counts"] = {
            name: value["starts"] for name, value in
            json.loads((state / "processes.json").read_text()).items()}
    except Exception as error:
        result["status"] = "failed"
        result["failures"].append({"type": type(error).__name__, "message": str(error),
                                   "at_unix_seconds": time.time()})
    finally:
        if supervisor is not None and supervisor.poll() is None:
            supervisor.send_signal(signal.SIGTERM)
            try:
                supervisor.wait(timeout=20)
            except subprocess.TimeoutExpired:
                supervisor.kill()
                supervisor.wait(timeout=10)
                result["failures"].append({"type": "ShutdownTimeout",
                                           "message": "supervisor required SIGKILL"})
        result["supervisor_exit_code"] = supervisor.returncode if supervisor else None
        result["finished_unix_seconds"] = time.time()
        (HERE / "dagu.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"candidate": result["candidate"], "status": result["status"],
                          "state": str(state), "levels": sorted(result["levels"])}, sort_keys=True))
    if result["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
