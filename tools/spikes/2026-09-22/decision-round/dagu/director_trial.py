"""Separate bounded Strands Director/A2A bridge proof over Dagu sidecar."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shlex
import sqlite3
import subprocess
import urllib.request
from uuid import uuid4

from publisher import publish
from trial import (COMMON, DAGU, HERE, S2_PYTHON, at_node, get, launch, port, rss_kib, status,
                   stop, until)


def rpc(url: str, method: str, params: dict) -> dict:
    body = {"jsonrpc": "2.0", "id": str(uuid4()), "method": method, "params": params}
    request = urllib.request.Request(url + "/", data=json.dumps(body).encode(), method="POST",
        headers={"Content-Type": "application/json", "Authorization": "Bearer fixture-token"})
    with urllib.request.urlopen(request, timeout=12) as response:
        result = json.load(response)
    if "error" in result:
        raise RuntimeError(result["error"])
    return result["result"]


def send(url: str, command: dict) -> dict:
    task = rpc(url, "message/send", {"message": {"role": "user", "messageId": str(uuid4()),
                       "parts": [{"kind": "data", "data": command}]}})
    if task.get("kind") != "task":
        raise RuntimeError(f"Director returned no Task: {task}")
    return task


def inspect(url: str, task_id: str) -> dict:
    return rpc(url, "tasks/get", {"id": task_id})


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("runtime", type=Path)
    args = parser.parse_args()
    rt = args.runtime.resolve()
    if rt.exists() and any(rt.iterdir()):
        raise SystemExit(f"runtime must be empty: {rt}")
    rt.mkdir(parents=True, exist_ok=True)
    cap_port, quality_port, dagu_port, director_port = port(), port(), port(), port()
    cap_url, quality_url, dagu_url, director_url = (
        f"http://127.0.0.1:{item}" for item in (cap_port, quality_port, dagu_port, director_port))
    bin_dir = rt / "bin"
    bin_dir.mkdir()
    wrapper = bin_dir / "exo-dagu-adapter"
    wrapper.write_text("#!/bin/sh\n"
        + "export EXO_DAGU_RUNTIME=" + shlex.quote(str(rt)) + "\n"
        + "export EXO_CAPABILITY_URL=" + shlex.quote(cap_url) + "\n"
        + "export EXO_QUALITY_URL=" + shlex.quote(quality_url) + "\n"
        + "exec python3 " + shlex.quote(str(HERE / "adapter.py")) + " \"$@\"\n")
    wrapper.chmod(0o755)
    env = {**os.environ, "PATH": str(bin_dir) + os.pathsep + os.environ["PATH"],
           "DAGU_HOME": str(rt / "home"), "DAGU_AUTH_MODE": "none",
           "DAGU_COORDINATOR_ENABLED": "false"}
    manifests = {"v1": publish(HERE / "definitions", rt / "home", "v1", DAGU)}
    cap_cmd = [str(S2_PYTHON), str(COMMON / "harness_server.py"), "--state", str(rt / "capability"),
               "--role", "capability", "--port", str(cap_port)]
    quality_cmd = [str(S2_PYTHON), str(COMMON / "harness_server.py"), "--state", str(rt / "quality"),
                   "--role", "quality", "--port", str(quality_port)]
    dagu_cmd = [str(DAGU), "start-all", "--host", "127.0.0.1", "--port", str(dagu_port)]
    director_cmd = [str(S2_PYTHON), str(HERE / "director_server.py"), "--state", str(rt / "director"),
                    "--runtime", str(rt), "--dagu-url", dagu_url, "--port", str(director_port)]
    cap = quality = dagu = director = successor = None
    observations: dict[str, object] = {"manifests": manifests}
    try:
        cap = launch(cap_cmd, env, rt / "capability.log", cap_url + "/health")
        quality = launch(quality_cmd, env, rt / "quality.log", quality_url + "/health")
        dagu = launch(dagu_cmd, env, rt / "dagu.log", dagu_url + "/api/v1/dags")
        director = launch(director_cmd, env, rt / "director.log", director_url + "/health")
        observations["identities_initial"] = {role: get(url + "/health") for role, url in
             (("capability", cap_url), ("quality", quality_url), ("director", director_url))}

        def start(version: str) -> dict:
            return send(director_url, {"op": "start", "key": f"{version}:start",
                "run_id": f"dagu-product-{version}-001", "version": version,
                "closure_sha256": manifests[version]["closure_sha256"]})

        v1 = start("v1")
        until(lambda: at_node(dagu_url, "v1", "publication_gate"), "Director-started v1 wait")
        manifests["v2"] = publish(HERE / "definitions", rt / "home", "v2", DAGU)
        v2 = start("v2")
        until(lambda: at_node(dagu_url, "v2", "publication_gate"), "Director-started v2 wait")
        observations["factory_cards"] = get(director_url + "/.well-known/agent-card.json")
        observations["waiting_tasks"] = {"v1": inspect(director_url, v1["id"]),
                                          "v2": inspect(director_url, v2["id"])}
        director.kill()  # hard crash while both factory A2A tasks are input-required
        director.wait(timeout=5)
        observations["director_crash_exit"] = director.returncode
        director = launch(director_cmd, env, rt / "director-restarted.log", director_url + "/health")
        observations["director_after_restart"] = get(director_url + "/health")
        observations["restored_tasks"] = {"v1": inspect(director_url, v1["id"]),
                                           "v2": inspect(director_url, v2["id"])}

        send(director_url, {"op": "decide", "key": "v1:release-publication",
            "run_id": "dagu-product-v1-001", "gate": "publication_gate"})
        until(lambda: cap.poll() is not None, "remote commit and lost acknowledgement", 30)
        cap = launch(cap_cmd, env, rt / "capability-restarted.log", cap_url + "/health")
        until(lambda: at_node(dagu_url, "v1", "director"), "v1 Director gate", 45)
        send(director_url, {"op": "decide", "key": "v2:release-publication",
            "run_id": "dagu-product-v2-001", "gate": "publication_gate"})
        until(lambda: at_node(dagu_url, "v2", "director"), "v2 Director gate", 45)
        send(director_url, {"op": "decide", "key": "v1:release-delivery",
            "run_id": "dagu-product-v1-001", "gate": "director"})
        send(director_url, {"op": "decide", "key": "v2:release-delivery",
            "run_id": "dagu-product-v2-001", "gate": "director"})
        until(lambda: at_node(dagu_url, "v1", "deliver", "succeeded"), "v1 delivery", 30)
        until(lambda: at_node(dagu_url, "v2", "deliver", "succeeded"), "v2 delivery", 30)
        observations["completed_tasks"] = {"v1": inspect(director_url, v1["id"]),
                                             "v2": inspect(director_url, v2["id"])}
        observations["runs"] = {version: status(dagu_url, version) for version in ("v1", "v2")}
        observations["rss_kib_final"] = {"dagu": rss_kib(dagu), "capability": rss_kib(cap),
                                         "quality": rss_kib(quality), "director": rss_kib(director)}
        with sqlite3.connect(rt / "director" / "director.sqlite3") as db:
            observations["director_run_bindings"] = db.execute(
                "SELECT run_id, version, closure_sha256 FROM runs ORDER BY run_id").fetchall()
            observations["director_run_bindings"] = [list(row) for row in observations["director_run_bindings"]]
        assert observations["identities_initial"]["director"]["identity"] == observations["director_after_restart"]["identity"]
        assert observations["director_after_restart"]["incarnation"] == 2
        assert all(task["status"]["state"] == "input-required" for task in observations["restored_tasks"].values())
        assert all(task["status"]["state"] == "completed" for task in observations["completed_tasks"].values())
        assert len({item["identity"] for item in observations["identities_initial"].values()}) == 3
        successor_port = port()
        successor_url = f"http://127.0.0.1:{successor_port}"
        successor_cmd = director_cmd[:-1] + [str(successor_port)]
        successor = launch(successor_cmd, env, rt / "director-successor.log",
                           successor_url + "/health")
        observations["overlap_successor"] = get(successor_url + "/health")
        try:
            send(director_url, {"op": "inspect", "run_id": "dagu-product-v1-001"})
        except RuntimeError as error:
            if "stale Director incarnation" not in str(error):
                raise
            observations["old_owner_rejected"] = str(error)
        else:
            raise AssertionError("old Director accepted a command after successor launch")
        observations["successor_inspect"] = send(successor_url, {"op": "inspect",
                                                    "run_id": "dagu-product-v1-001"})
        assert observations["successor_inspect"]["status"]["state"] == "completed"
        (rt / "director-result.json").write_text(json.dumps(observations, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"result": "passed", "evidence": str(rt / "director-result.json"),
                          "rss_kib_final": observations["rss_kib_final"]}, sort_keys=True))
    except Exception:
        (rt / "director-partial.json").write_text(json.dumps(observations, indent=2, sort_keys=True) + "\n")
        raise
    finally:
        for process in (successor, director, dagu, cap, quality):
            stop(process)


if __name__ == "__main__":
    main()
