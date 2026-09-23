"""0/2/10 held external A2A assignments in the frozen Effect trial bundle."""
from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time

HERE = Path(__file__).resolve().parent
EFFECT = HERE.parent / "effect-parity"
OLD = HERE.parent.parent / "2026-09-22"
COMMON = OLD / "arbitration" / "common"
sys.path.insert(0, str(EFFECT))
sys.path.insert(0, str(COMMON))
sys.path.insert(0, str(OLD / "decision-round" / "common"))
sys.path.insert(0, str(OLD / "arbitration" / "wait-scaling"))
import definition  # noqa: E402
import service_probe  # noqa: E402
from client import get_task as get_remote_task  # noqa: E402
from director_probe import rpc, send  # noqa: E402
from footprint_probe import sample  # noqa: E402
from probe import api, free_port, until  # noqa: E402


def pending(base: Path) -> list[dict]:
    db_path = base / "source" / "harness.sqlite3"
    if not db_path.is_file():
        return []
    with sqlite3.connect(db_path) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute("SELECT * FROM pending ORDER BY action_id")]


def footprint(base: Path, processes: dict[str, subprocess.Popen]) -> dict:
    effect_pid = processes["effect"].pid
    roles = {p.pid: name for name, p in processes.items()}
    roots = {p.pid: name for name, p in processes.items() if name != "effect"}
    snapshot = sample(effect_pid, base, roles, extra_roots=roots)
    for item in snapshot["processes"]:
        if item["role"] in {"bundle_child", "postgres_child"} and "bridge.py" in item["command"]:
            item["role"] = "python_a2a_bridge"
    snapshot["selection"] = "all six bundle process roots, their descendants, and state-bound processes"
    return snapshot


def inflight(base: Path, effect_url: str, director_url: str, source_url: str,
             admissions: list[dict]) -> list[dict] | bool:
    by_action = {row["action_id"]: row for row in pending(base)}
    found = []
    for row in admissions:
        parent = api(effect_url, "/poll", {"id": row["run_id"]})[1]
        child_id = parent["run"]["child_id"]
        if not child_id:
            return False
        child = api(effect_url, "/poll", {"id": child_id})[1]
        action_id = child_id + ":source_evidence"
        held = by_action.get(action_id)
        if held is None:
            return False
        director = rpc(director_url, "tasks/get", {"id": row["director_task_id"]})
        remote = get_remote_task(source_url, held["task_id"])
        if (director["id"] != row["director_task_id"]
                or director["status"]["state"] != "working"
                or remote["id"] != held["task_id"]
                or remote["status"]["state"] != "working"
                or remote["metadata"]["action_id"] != action_id
                or child["run"]["phase"] != "parallel"):
            return False
        found.append({"parent_run_id": row["run_id"],
                      "parent_engine_state": parent["state"],
                      "child_run_id": child_id,
                      "child_engine_state": child["state"],
                      "child_phase": child["run"]["phase"],
                      "director_task_id": row["director_task_id"],
                      "director_task_status": director["status"]["state"],
                      "source_action_id": action_id,
                      "source_a2a_task_id": held["task_id"],
                      "source_a2a_task_status": remote["status"]["state"]})
    return found


def main() -> dict:
    base = Path(tempfile.mkdtemp(prefix="exo-effect-inflight-", dir="/tmp"))
    (base / "hold-source-assignments").write_text("bounded source fixture hold\n")
    processes: dict[str, subprocess.Popen] = {}
    evidence = {"schema": "exomachina.effect.inflight-scale/1",
                "runtime": str(base), "status": "running", "levels": {}, "admissions": []}
    last_snapshot = None

    def service(name: str, script: Path, args: list[str]):
        process, url, health, log = service_probe.launch(base, name, script, args)
        processes[name] = process
        return url, health

    def launch(name: str, command: list[str], url: str, env: dict | None = None):
        with (base / f"{name}.log").open("a") as log:
            process = subprocess.Popen(command, cwd=EFFECT, env=env,
                                       stdout=log, stderr=subprocess.STDOUT)
        processes[name] = process
        def ready():
            if process.poll() is not None:
                raise RuntimeError(f"{name} exited during startup: {process.returncode}")
            try:
                return api(url, "/health")[1]
            except Exception:
                return None
        return until(ready, label=f"{name} ready")

    try:
        source_url, source_info = service("source", HERE / "slow_harness_server.py",
                                          ["--role", "capability"])
        counter_url, counter_info = service("counter",
            OLD / "decision-round" / "common" / "harness_server.py",
            ["--role", "capability"])
        quality_url, quality_info = service("quality", COMMON / "quality_server.py", [])
        release_url, release_info = service("release", COMMON / "release_server.py",
                                             ["--mode", "participating"])
        def binding(role, url, info):
            return {"role": role, "url": url, "identity": info["identity"], "approved": True}
        bindings = {"source": binding("capability", source_url, source_info),
                    "counter": binding("capability", counter_url, counter_info),
                    "quality": binding("quality", quality_url, quality_info),
                    "release": binding("release", release_url, release_info)}
        (base / "approved.json").write_text(json.dumps(bindings))
        effect_port, director_port = free_port(), free_port()
        effect_url, director_url = f"http://127.0.0.1:{effect_port}", f"http://127.0.0.1:{director_port}"
        env = os.environ.copy()
        env.update(EFFECT_PARITY_ROOT=str(base / "effect"),
                   EFFECT_PARITY_APPROVED=str(base / "approved.json"),
                   EFFECT_PARITY_PORT=str(effect_port), EFFECT_PARITY_PYTHON=sys.executable)
        launch("effect", ["node", str(EFFECT / "helper.mjs")], effect_url, env)
        launch("director", [sys.executable, str(EFFECT / "director_server.py"),
            "--state", str(base / "director"), "--effect", effect_url,
            "--port", str(director_port)], director_url)
        template = json.loads((EFFECT / "definitions" / "visible-v3.json").read_text())
        child_digest = definition.digest(template["child"])
        template["root"]["nodes"]["invoke_child"]["child_digest"] = child_digest
        package = {"schema": 1, "root": template["root"],
                   "children": {child_digest: template["child"]}, "bindings": bindings}
        definition.validate(package, bindings)
        code, published = api(effect_url, "/publish", package)
        assert code == 200, published
        digest = published["package_digest"]
        evidence["package_digest"] = digest
        evidence["levels"]["0"] = {"in_flight": [],
            "snapshot": footprint(base, processes)}
        for target in (2, 10):
            start_at = time.monotonic()
            for index in range(len(evidence["admissions"]) + 1, target + 1):
                run_id = f"effect-inflight-{index}"
                task = send(director_url, {"op": "start", "run_id": run_id,
                    "key": f"effect-inflight-start-{index}", "package_digest": digest,
                    "resolution_after_repairs": 3})
                evidence["admissions"].append({"run_id": run_id,
                    "director_task_id": task["id"],
                    "start_task_status": task["status"]["state"]})
            rows = until(lambda: inflight(base, effect_url, director_url, source_url,
                                          evidence["admissions"]),
                         seconds=60, label=f"{target} Effect working A2A assignments")
            last_snapshot = footprint(base, processes)
            level = {"in_flight": rows, "snapshot": last_snapshot,
                     "stage_elapsed_seconds": time.monotonic() - start_at}
            level["after_sample"] = inflight(base, effect_url, director_url,
                                               source_url, evidence["admissions"])
            if level["after_sample"] is False:
                raise RuntimeError("held Effect assignments changed during footprint sample")
            evidence["levels"][str(target)] = level
        evidence["status"] = "passed"
    except Exception as error:
        evidence["status"] = "failed"
        evidence["error"] = repr(error)
        raise
    finally:
        # Kill only the observed Python bridge descendants and this trial's
        # named process roots; no host-wide process matching is used.
        if last_snapshot is not None:
            named = {process.pid for process in processes.values()}
            for item in last_snapshot["processes"]:
                if item["pid"] not in named and item["role"] == "python_a2a_bridge":
                    try: os.kill(item["pid"], signal.SIGTERM)
                    except ProcessLookupError: pass
        for process in reversed(list(processes.values())):
            if process.poll() is None:
                process.terminate()
                try: process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        evidence["exit_codes"] = {name: process.returncode for name, process in processes.items()}
        (HERE / "observed.json").write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    return evidence


if __name__ == "__main__":
    result = main()
    print(json.dumps({"status": result["status"], "runtime": result["runtime"],
                      "levels": sorted(result["levels"])}, sort_keys=True))
