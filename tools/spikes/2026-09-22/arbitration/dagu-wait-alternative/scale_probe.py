"""Minimal 0/2/10 processless-wait footprint, without product services."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import subprocess

import auto_bridge
import bridge
import shared_trial
import trial

HERE = Path(__file__).resolve().parent
RUNTIME = Path("/tmp/exomachina-dagu-wait-shared-scale-001")


def processes(root_pid: int, runtime: Path) -> dict[int, str]:
    rows = {}
    for line in subprocess.check_output(["ps", "-axo", "pid=,ppid=,command="], text=True).splitlines():
        fields = line.strip().split(None, 2)
        if len(fields) == 3 and fields[0].isdigit() and fields[1].isdigit():
            rows[int(fields[0])] = (int(fields[1]), fields[2])
    selected = {root_pid}
    while True:
        next_set = selected | {pid for pid, (parent, _) in rows.items() if parent in selected}
        if next_set == selected:
            break
        selected = next_set
    selected |= {pid for pid, (_, command) in rows.items()
                 if str(runtime / "home" / "dags") in command}
    return {pid: rows[pid][1] for pid in sorted(selected) if pid in rows}


def footprint(supervisor_pid: int, runtime: Path) -> dict:
    selected = processes(supervisor_pid, runtime)
    command = ["footprint", "-f", "bytes", "--noCategories"]
    for pid in selected:
        command.extend(["-p", str(pid)])
    run = subprocess.run(command, capture_output=True, text=True, timeout=30)
    if run.returncode:
        raise RuntimeError(f"footprint failed: {run.stderr.strip()}")
    match = re.search(r"Summary Footprint: (\d+) B", run.stdout)
    if match is None:
        match = re.search(r"Footprint: (\d+) B", run.stdout)
    if match is None:
        raise ValueError("footprint summary missing")
    return {"bytes": int(match[1]), "pid_count": len(selected),
            "processes": [{"pid": pid, "command": argv} for pid, argv in selected.items()]}


def add_waits(runtime: Path, base: str, start: int, stop: int) -> list[dict]:
    pairs = []
    parent_digest = bridge.digest(runtime, bridge.PARENT_NAME)
    child_digest = bridge.digest(runtime, bridge.CHILD_NAME)
    for index in range(start, stop):
        parent_id = f"exo-wait-scale-parent-{index:02d}"
        child_id = f"exo-wait-scale-child-{index:02d}"
        with auto_bridge.connect(runtime) as db:
            db.execute("INSERT INTO bridge VALUES (?,?,?,?,?,?,?,?)",
                       (parent_id, bridge.PARENT_NAME, child_id, bridge.CHILD_NAME,
                        parent_digest, child_digest, 1, "waiting"))
            db.execute("INSERT INTO auto_state(parent_id) VALUES (?)", (parent_id,))
        trial.api(base, f"/api/v1/dags/{bridge.PARENT_NAME}.yaml/start", {"dagRunId": parent_id})
        trial.api(base, f"/api/v1/dags/{bridge.CHILD_NAME}.yaml/start", {"dagRunId": child_id})
        pairs.append({"parent_id": parent_id, "child_id": child_id})
    for pair in pairs:
        trial.until(lambda pair=pair: trial.is_waiting(base, bridge.PARENT_NAME,
                                                       pair["parent_id"], "child_gate"),
                    "parent scale wait", 45)
        trial.until(lambda pair=pair: trial.is_waiting(base, bridge.CHILD_NAME,
                                                       pair["child_id"], "director_wait"),
                    "child scale wait", 45)
    return pairs


def main() -> None:
    if hashlib.sha256(trial.DAGU.read_bytes()).hexdigest() != trial.DAGU_SHA256:
        raise SystemExit("pinned Dagu binary unavailable or changed")
    info = shared_trial.prepare(RUNTIME)
    port = trial.free_port()
    base = f"http://127.0.0.1:{port}"
    supervisor = None
    evidence = {"dagu_binary_sha256": trial.DAGU_SHA256,
                "shared_supervisor_sha256": hashlib.sha256((HERE / "shared_supervisor.py").read_bytes()).hexdigest(),
                "runtime": str(RUNTIME), "definitions": info["definition_sha256"],
                "topology": "one Python supervisor/reconciler plus one Dagu start-all; no A2A, capability, Quality, release, or receiver services"}
    try:
        supervisor = shared_trial.start_supervisor(RUNTIME, port)
        evidence["zero_waits"] = footprint(supervisor.pid, RUNTIME)
        evidence["pairs_1_to_2"] = add_waits(RUNTIME, base, 1, 3)
        evidence["two_waits"] = footprint(supervisor.pid, RUNTIME)
        evidence["pairs_3_to_10"] = add_waits(RUNTIME, base, 3, 11)
        evidence["ten_waits"] = footprint(supervisor.pid, RUNTIME)
        evidence["outcome"] = "measured"
    except Exception as error:
        evidence["outcome"] = "failed"
        evidence["error"] = repr(error)
        raise
    finally:
        shared_trial.stop_supervisor(supervisor)
        (HERE / "scale_observed.json").write_text(json.dumps(evidence, sort_keys=True, indent=2) + "\n")
    print(json.dumps({key: {"bytes": evidence[key]["bytes"], "pid_count": evidence[key]["pid_count"]}
                      for key in ("zero_waits", "two_waits", "ten_waits")}, sort_keys=True))


if __name__ == "__main__":
    main()
