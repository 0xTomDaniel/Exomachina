"""Measure de-duplicated macOS footprint for the supervised local bundle."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time

from publisher import publish
from trial import DAGU, HERE, PYTHON, child_for, start_director, status, until


def processes(root_pid: int, runtime: Path) -> dict[int, str]:
    rows = {}
    for line in subprocess.check_output(["ps", "-axo", "pid=,ppid=,command="], text=True).splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) == 3 and parts[0].isdigit() and parts[1].isdigit():
            rows[int(parts[0])] = (int(parts[1]), parts[2])
    selected = {root_pid}
    changed = True
    while changed:
        before = len(selected)
        selected.update(pid for pid, (parent, _) in rows.items() if parent in selected)
        changed = len(selected) != before
    selected.update(pid for pid, (_, command) in rows.items()
                    if str(runtime) in command and "resource_probe.py" not in command)
    return {pid: rows[pid][1] for pid in sorted(selected) if pid in rows}


def footprint(root_pid: int, runtime: Path) -> dict:
    process_set = processes(root_pid, runtime)
    command = ["footprint", "-f", "bytes", "--noCategories"]
    for pid in process_set:
        command.extend(["-p", str(pid)])
    run = subprocess.run(command, capture_output=True, text=True)
    if run.returncode:
        raise RuntimeError(f"footprint failed: {run.stderr.strip()}")
    match = re.search(r"Summary Footprint: (\d+) B", run.stdout)
    if match is None:
        # A one-process set is printed without a summary row.
        match = re.search(r"Footprint: (\d+) B", run.stdout)
    if match is None:
        raise ValueError("footprint summary missing")
    return {"bytes": int(match[1]), "pid_count": len(process_set),
            "processes": [{"pid": pid, "command": command.split()[0]} for pid, command in process_set.items()]}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("runtime", type=Path)
    args = parser.parse_args()
    rt = args.runtime.resolve()
    if rt.exists() and any(rt.iterdir()):
        raise SystemExit("runtime must be fresh")
    rt.mkdir(parents=True)
    os.environ["EXO_ARB_RUNTIME"] = str(rt)
    manifest = publish(HERE / "definitions" / "v3", rt / "home", "exo_arb_parent_v3", DAGU)
    started = time.monotonic()
    with (rt / "supervisor.log").open("wb") as stream:
        supervisor = subprocess.Popen([str(PYTHON), str(HERE / "supervisor.py"), str(rt),
                                       "--dagu", str(DAGU)], stdout=stream, stderr=stream,
                                      start_new_session=True)
    try:
        until(lambda: (rt / "ready").is_file(), "supervisor ready", 50)
        ready_seconds = time.monotonic() - started
        warm = footprint(supervisor.pid, rt)
        parents = []
        tasks = []
        children = []
        for index in (1, 2):
            parent = f"exo-arb-resource-wait-{index}-{rt.name}"
            parents.append(parent)
            task = start_director(rt, parent, "exo_arb_parent_v3", resolve_input=False)
            tasks.append(task["id"])
        for parent in parents:
            child = until(lambda parent=parent: child_for(parent), "child for " + parent, 30)
            until(lambda child=child: status(child["dag_name"], child["run_id"], "waiting"),
                  "child Director wait", 100)
            children.append(child["run_id"])
        # Parent nested adapters remain live while both child waits persist.
        active = footprint(supervisor.pid, rt)
        result = {"schema": "exomachina.arbitration.dagu.resources/1",
                  "engine": "dagu-community-v2.17.0", "host": "macOS local spike",
                  "topology": "supervisor plus Dagu, two capability servers, Quality, Director, participating and opaque release receivers, reconciler, two waiting parent/child DAG pairs",
                  "v3_closure_sha256": manifest["closure_sha256"],
                  "fresh_supervisor_to_ready_seconds": ready_seconds,
                  "warm_ready": warm, "two_child_director_waits": active,
                  "parents": parents, "children": children, "director_task_ids": tasks}
        (rt / "resources.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"result": "measured", "evidence": str(rt / "resources.json"),
                          "warm_bytes": warm["bytes"], "two_wait_bytes": active["bytes"]}, sort_keys=True))
    finally:
        supervisor.terminate()
        try:
            supervisor.wait(timeout=12)
        except subprocess.TimeoutExpired:
            supervisor.kill()


if __name__ == "__main__":
    main()
