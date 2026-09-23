"""A2A smoke for the bounded Graph-backed S2 Strands Director harness."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
import urllib.request
from pathlib import Path
from uuid import uuid4

from probe import HERE, canonical, disk_kib, free_port, launch_server, request_json, rss_kib


def rpc(url: str, method: str, params: dict, token: str = "director-test-token") -> dict:
    body = {"jsonrpc": "2.0", "id": str(uuid4()), "method": method, "params": params}
    request = urllib.request.Request(url + "/", data=canonical(body).encode(), headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=30) as response:
        value = json.load(response)
    if "error" in value:
        raise RuntimeError(value["error"])
    return value["result"]


def send(url: str, command: dict, token: str = "director-test-token") -> dict:
    return rpc(url, "message/send", {"message": {"role": "user", "messageId": str(uuid4()), "parts": [{"kind": "data", "data": command}]}}, token)


def launch_director(python: Path, state: Path, port: int, environment: dict) -> subprocess.Popen:
    state.mkdir(parents=True, exist_ok=True)
    log = (state / "director.log").open("a")
    process = subprocess.Popen([str(python), str(HERE / "director_server.py"), "--state", str(state), "--port", str(port)], stdout=log, stderr=log, env=environment)
    log.close()
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("Director failed to start: " + (state / "director.log").read_text()[-3000:])
        try:
            request_json(f"http://127.0.0.1:{port}/health", timeout=0.3)
            return process
        except Exception:
            time.sleep(0.05)
    process.kill()
    raise TimeoutError("Director startup")


def exercise(root: Path, python: Path) -> dict:
    root.mkdir(parents=True, exist_ok=True)
    cap_port, quality_port, director_port = free_port(), free_port(), free_port()
    capability = launch_server(python, root / "capability", "capability", cap_port)
    quality = launch_server(python, root / "quality", "quality", quality_port)
    environment = os.environ.copy()
    environment["EXO_CAPABILITY_URL"] = f"http://127.0.0.1:{cap_port}"
    environment["EXO_QUALITY_URL"] = f"http://127.0.0.1:{quality_port}"
    state = root / "director"
    director = launch_director(python, state, director_port, environment)
    processes = [capability, quality, director]
    url = f"http://127.0.0.1:{director_port}"
    try:
        identity1 = request_json(url + "/health")
        assert identity1["role"] == "factory"
        started = {}
        for revision in (1, 2):
            run_id = f"a2a-director-v{revision}"
            task = send(url, {"op": "start", "run_id": run_id, "revision": revision, "key": f"start-v{revision}"})
            assert task["status"]["state"] == "input-required" and not task.get("artifacts")
            assert task["metadata"]["run_id"] == run_id
            started[run_id] = task
        assert started["a2a-director-v1"]["metadata"]["definition"] != started["a2a-director-v2"]["metadata"]["definition"]
        measurement = {"director_rss_kib": rss_kib(director), "capability_rss_kib": rss_kib(capability),
                       "quality_rss_kib": rss_kib(quality), "director_state_kib": disk_kib(state),
                       "capability_state_kib": disk_kib(root / "capability"),
                       "quality_state_kib": disk_kib(root / "quality"), "paused_graph_runs": 2}
        director.kill()
        assert director.wait(timeout=10) < 0
        director = launch_director(python, state, director_port, environment)
        processes.append(director)
        identity2 = request_json(url + "/health")
        assert identity2["identity"] == identity1["identity"] and identity2["incarnation"] == identity1["incarnation"] + 1
        for task in started.values():
            restored = rpc(url, "tasks/get", {"id": task["id"]})
            assert restored["status"]["state"] == "input-required" and not restored.get("artifacts")
        denied = send(url, {"op": "decide", "run_id": "a2a-director-v1", "key": "denied"}, token="observer-test-token")
        assert denied["parts"][0]["data"]["error"] == "unauthorized command"
        completed = {}
        for revision in (1, 2):
            run_id = f"a2a-director-v{revision}"
            task = send(url, {"op": "decide", "run_id": run_id, "key": f"approve-v{revision}"})
            assert task["status"]["state"] == "completed"
            artifact = task["artifacts"][0]["parts"][0]["data"]
            assert artifact["revision"] == "r2" and artifact["sha256"] == task["artifacts"][0]["artifactId"]
            original = rpc(url, "tasks/get", {"id": started[run_id]["id"]})
            assert original["status"]["state"] == "completed"
            assert original["artifacts"][0]["parts"][0]["data"] == artifact
            completed[run_id] = {"task_id": task["id"], "sha256": artifact["sha256"], "original_task_id": original["id"]}
        outcome = {"status": "passed", "identity_before": identity1, "identity_after_restart": identity2,
                   "measurement": measurement,
                   "started": {run: {"task_id": value["id"], "definition": value["metadata"]["definition"]} for run, value in started.items()},
                   "completed": completed, "observer_decision_rejected": denied["parts"][0]["data"]["error"],
                   "director_same_package": "S2 Strands Harness subclass + S2 A2A server/task adapter + core Graph"}
        (HERE / "director_observed.json").write_text(json.dumps(outcome, indent=2, sort_keys=True) + "\n")
        return outcome
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=10)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--python", type=Path, required=True)
    args = parser.parse_args()
    print(canonical(exercise(args.root, args.python)))


if __name__ == "__main__":
    main()
