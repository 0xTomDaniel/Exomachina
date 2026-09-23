"""Probe host-global Dagu run socket collisions across independent DAGU_HOME dirs."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import socket
import subprocess
import time
import urllib.error
import urllib.request


DAGU = Path("/tmp/exomachina-countertrials/dagu/dagu")


def port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def http(url: str, data: dict | None = None) -> dict:
    request = urllib.request.Request(url, data=json.dumps(data).encode() if data is not None else None,
        headers={"Content-Type": "application/json"}, method="POST" if data is not None else "GET")
    with urllib.request.urlopen(request, timeout=8) as response:
        return json.load(response)


def poll(check, description: str, seconds: float = 50):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(.12)
    raise TimeoutError(description)


def stop(process: subprocess.Popen) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


ROOT_YAML = """steps:
  - id: gate
    action: human.task
    with:
      prompt: Start a long child run
      form:
        type: object
        properties:
          accepted:
            type: boolean
            enum: [true]
        required: [accepted]
  - id: child
    depends: gate
    action: dag.run
    with:
      dag: socket_child
"""
CHILD_YAML = "steps:\n  - id: sleep\n    run: /bin/sleep 8\n"


def phase(root: Path, name: str, distinct_ids: bool, isolated_tmp: bool) -> dict:
    phase_dir = root / name
    phase_dir.mkdir()
    instances = []
    for label in ("a", "b"):
        home = phase_dir / label / "home"
        dags = home / "dags"
        dags.mkdir(parents=True)
        (dags / "socket_root.yaml").write_text(ROOT_YAML)
        (dags / "socket_child.yaml").write_text(CHILD_YAML)
        tmp = phase_dir / label / "tmp"
        tmp.mkdir()
        api_port, scheduler_port = port(), port()
        env = {**os.environ, "DAGU_HOME": str(home), "DAGU_AUTH_MODE": "none",
               "DAGU_COORDINATOR_ENABLED": "false", "DAGU_SCHEDULER_PORT": str(scheduler_port)}
        if isolated_tmp:
            env["TMPDIR"] = str(tmp)
        else:
            env.pop("TMPDIR", None)
        base = f"http://127.0.0.1:{api_port}"
        run_id = f"socket-phase-{name}-{'a' if label == 'a' else 'b'}" if distinct_ids else f"socket-phase-{name}-shared"
        instances.append({"label": label, "home": home, "tmp": tmp, "env": env,
                          "base": base, "port": api_port, "run_id": run_id})
    processes = []
    try:
        for entry in instances:
            log = phase_dir / entry["label"] / "dagu.log"
            with log.open("wb") as stream:
                process = subprocess.Popen([str(DAGU), "start-all", "--host", "127.0.0.1",
                    "--port", str(entry["port"])], env=entry["env"], stdout=stream, stderr=stream)
            processes.append(process)
            poll(lambda: process.poll() is None and http(entry["base"] + "/api/v1/dags"),
                 f"{name}/{entry['label']} server startup", 20)
        for entry in instances:
            http(entry["base"] + "/api/v1/dags/socket_root.yaml/start",
                 {"dagRunId": entry["run_id"]})
            poll(lambda entry=entry: http(entry["base"] +
                f"/api/v1/dag-runs/socket_root/{entry['run_id']}")["dagRunDetails"]["statusLabel"] == "waiting",
                f"{name}/{entry['label']} gate wait", 20)
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(lambda entry: http(entry["base"] +
                f"/api/v1/dag-runs/socket_root/{entry['run_id']}/human-tasks/gate/complete",
                {"accepted": True}), instances))
        outcomes = {}
        for entry in instances:
            result = poll(lambda entry=entry: (lambda data: data if data["statusLabel"] in
                {"succeeded", "failed", "aborted"} else None)(http(entry["base"] +
                f"/api/v1/dag-runs/socket_root/{entry['run_id']}")["dagRunDetails"]),
                f"{name}/{entry['label']} terminal", 50)
            outcomes[entry["label"]] = {"status": result["statusLabel"],
                "nodes": {node["step"]["id"]: {"status": node["statusLabel"],
                    "stderr": node.get("stderr"), "subRuns": node.get("subRuns", [])} for node in result["nodes"]},
                "run_id": entry["run_id"], "api_port": entry["port"]}
        logs = {entry["label"]: "\n".join(path.read_text(errors="replace") for path in
                [phase_dir / entry["label"] / "dagu.log",
                 *(entry["home"] / "logs").rglob("*.log")]) for entry in instances}
        return {"distinct_ids": distinct_ids, "isolated_tmp": isolated_tmp,
                "outcomes": outcomes,
                "socket_conflict_in_logs": {label: "already running" in log and "socket=" in log
                                            for label, log in logs.items()},
                "socket_paths": {label: [line for line in log.splitlines() if "socket=" in line][-5:]
                                 for label, log in logs.items()}}
    finally:
        for process in processes:
            stop(process)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("runtime", type=Path)
    args = parser.parse_args()
    rt = args.runtime.resolve()
    if rt.exists() and any(rt.iterdir()):
        raise SystemExit("runtime must be empty")
    rt.mkdir(parents=True, exist_ok=True)
    result = {}
    for name, distinct, isolated in (("same-id-shared-tmp", False, False),
                                     ("same-id-isolated-tmp", False, True),
                                     ("distinct-id-shared-tmp", True, False)):
        result[name] = phase(rt, name, distinct, isolated)
        (rt / "namespace-partial.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    (rt / "namespace-result.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({name: {label: outcome["status"] for label, outcome in phase_result["outcomes"].items()}
                      for name, phase_result in result.items()}, sort_keys=True))


if __name__ == "__main__":
    main()
