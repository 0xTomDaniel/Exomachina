"""Copied-bundle one-command lifecycle trial: two Directors, one Dagu helper."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shlex
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from uuid import uuid4

from publisher import publish


HERE = Path(__file__).resolve().parent
BUNDLE = HERE.parent
PYTHON = BUNDLE / "venv" / "bin" / "python"
DAGU = BUNDLE / "bin" / "dagu"
COMMON = BUNDLE / "common"


def port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def http(url: str, value: dict | None = None, auth: bool = False) -> dict:
    data = None if value is None else json.dumps(value).encode()
    request = urllib.request.Request(url, data=data,
        headers={"Content-Type": "application/json", **({"Authorization": "Bearer fixture-token"} if auth else {})},
        method="POST" if value is not None else "GET")
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def rpc(url: str, method: str, params: dict) -> dict:
    result = http(url + "/", {"jsonrpc": "2.0", "id": str(uuid4()),
               "method": method, "params": params}, auth=True)
    if "error" in result:
        raise RuntimeError(result["error"])
    return result["result"]


def send(url: str, command: dict) -> dict:
    result = rpc(url, "message/send", {"message": {"role": "user",
        "messageId": str(uuid4()), "parts": [{"kind": "data", "data": command}]}})
    if result.get("kind") != "task":
        raise RuntimeError(f"factory command did not return a task: {result}")
    return result


def poll(check, description: str, seconds: float = 45) -> object:
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except (OSError, urllib.error.URLError) as error:
            last = repr(error)
        time.sleep(.1)
    raise TimeoutError(f"{description} timed out; last={last}")


def launch(argv: list[str], env: dict, logfile: Path, health: str) -> subprocess.Popen:
    with logfile.open("ab") as stream:
        process = subprocess.Popen(argv, env=env, stdout=stream, stderr=stream)
    poll(lambda: process.poll() is None and http(health), f"launch {argv[0]}", 20)
    return process


def stop(process: subprocess.Popen | None) -> None:
    if process and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def run_status(dagu_url: str, version: str) -> dict:
    return http(f"{dagu_url}/api/v1/dag-runs/exo_decision_factory_{version}/dagu-product-{version}-001")["dagRunDetails"]


def waiting_at(dagu_url: str, version: str, node: str) -> bool:
    run = run_status(dagu_url, version)
    nodes = {entry["step"]["id"]: entry for entry in run["nodes"]}
    if run["statusLabel"] == "failed":
        raise AssertionError(f"Dagu {version} failed: {nodes}")
    return nodes[node]["statusLabel"] == "waiting"


def delivered(dagu_url: str, version: str) -> bool:
    run = run_status(dagu_url, version)
    nodes = {entry["step"]["id"]: entry for entry in run["nodes"]}
    if run["statusLabel"] == "failed":
        raise AssertionError(f"Dagu {version} failed: {nodes}")
    return run["statusLabel"] == "succeeded" and nodes["deliver"]["statusLabel"] == "succeeded"


def tree_rss_kib() -> dict:
    lines = subprocess.check_output(["ps", "-A", "-o", "pid=,ppid=,rss="], text=True).splitlines()
    rows = []
    for line in lines:
        parts = line.split()
        if len(parts) == 3 and all(item.isdigit() for item in parts):
            rows.append(tuple(map(int, parts)))
    ours = {os.getpid()}
    changed = True
    while changed:
        before = len(ours)
        ours.update(pid for pid, ppid, _ in rows if ppid in ours)
        changed = len(ours) > before
    matches = {pid: rss for pid, _, rss in rows if pid in ours}
    return {"process_count": len(matches), "rss_sum_kib": sum(matches.values()),
            "pids": matches}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("runtime", type=Path)
    args = parser.parse_args()
    rt = args.runtime.resolve()
    if rt.exists() and any(rt.iterdir()):
        raise SystemExit(f"runtime must be empty: {rt}")
    rt.mkdir(parents=True, exist_ok=True)
    cap_port, quality_port, dagu_port, a_port, b_port, scheduler_port = (port() for _ in range(6))
    cap_url, quality_url, dagu_url, a_url, b_url = (
        f"http://127.0.0.1:{number}" for number in (cap_port, quality_port, dagu_port, a_port, b_port))
    bin_dir = rt / "bin"
    bin_dir.mkdir()
    wrapper = bin_dir / "exo-dagu-adapter"
    wrapper.write_text("#!/bin/sh\n"
        + "export EXO_DAGU_RUNTIME=" + shlex.quote(str(rt)) + "\n"
        + "export EXO_CAPABILITY_URL=" + shlex.quote(cap_url) + "\n"
        + "export EXO_QUALITY_URL=" + shlex.quote(quality_url) + "\n"
        + "exec " + shlex.quote(str(PYTHON)) + " " + shlex.quote(str(HERE / "adapter.py")) + " \"$@\"\n")
    wrapper.chmod(0o755)
    env = {**os.environ, "PATH": str(bin_dir) + os.pathsep + os.environ["PATH"],
           "DAGU_HOME": str(rt / "home"), "DAGU_AUTH_MODE": "none",
           "DAGU_COORDINATOR_ENABLED": "false", "DAGU_SCHEDULER_PORT": str(scheduler_port)}
    manifests = {version: publish(HERE / "definitions", rt / "home", version, DAGU)
                 for version in ("v1", "v2")}
    cap_cmd = [str(PYTHON), str(COMMON / "harness_server.py"), "--state", str(rt / "capability"),
               "--role", "capability", "--port", str(cap_port)]
    quality_cmd = [str(PYTHON), str(COMMON / "harness_server.py"), "--state", str(rt / "quality"),
                   "--role", "quality", "--port", str(quality_port)]
    dagu_cmd = [str(DAGU), "start-all", "--host", "127.0.0.1", "--port", str(dagu_port)]
    def director_cmd(name: str, number: int) -> list[str]:
        return [str(PYTHON), str(HERE / "director_server.py"), "--state", str(rt / name),
                "--runtime", str(rt), "--dagu-url", dagu_url, "--port", str(number)]
    cap = quality = dagu = a = b = None
    observations: dict[str, object] = {"bundle_root": str(BUNDLE), "runtime": str(rt),
        "ports": {"capability": cap_port, "quality": quality_port, "dagu": dagu_port,
                  "director_a": a_port, "director_b": b_port,
                  "dagu_scheduler": scheduler_port}, "manifests": manifests}
    try:
        cap = launch(cap_cmd, env, rt / "capability.log", cap_url + "/health")
        quality = launch(quality_cmd, env, rt / "quality.log", quality_url + "/health")
        dagu = launch(dagu_cmd, env, rt / "dagu.log", dagu_url + "/api/v1/dags")
        a = launch(director_cmd("director_a", a_port), env, rt / "director_a.log", a_url + "/health")
        b = launch(director_cmd("director_b", b_port), env, rt / "director_b.log", b_url + "/health")
        observations["identities_initial"] = {name: http(url + "/health") for name, url in
            (("capability", cap_url), ("quality", quality_url), ("director_a", a_url), ("director_b", b_url))}
        observations["rss_startup"] = tree_rss_kib()
        v1 = send(a_url, {"op": "start", "key": "bundle:a:v1:start",
            "run_id": "dagu-product-v1-001", "version": "v1",
            "closure_sha256": manifests["v1"]["closure_sha256"]})
        v2 = send(b_url, {"op": "start", "key": "bundle:b:v2:start",
            "run_id": "dagu-product-v2-001", "version": "v2",
            "closure_sha256": manifests["v2"]["closure_sha256"]})
        poll(lambda: waiting_at(dagu_url, "v1", "publication_gate"), "v1 publication wait")
        poll(lambda: waiting_at(dagu_url, "v2", "publication_gate"), "v2 publication wait")
        observations["rss_warm_waiting"] = tree_rss_kib()
        dagu.kill()  # shared helper hard crash with both Director A2A tasks waiting
        dagu.wait(timeout=5)
        observations["dagu_crash_exit"] = dagu.returncode
        dagu = launch(dagu_cmd, env, rt / "dagu-restarted.log", dagu_url + "/api/v1/dags")
        poll(lambda: waiting_at(dagu_url, "v1", "publication_gate"), "v1 wait after Dagu restart")
        poll(lambda: waiting_at(dagu_url, "v2", "publication_gate"), "v2 wait after Dagu restart")
        observations["tasks_after_dagu_restart"] = {
            "v1": rpc(a_url, "tasks/get", {"id": v1["id"]}),
            "v2": rpc(b_url, "tasks/get", {"id": v2["id"]})}
        assert all(task["status"]["state"] == "input-required" for task in
                   observations["tasks_after_dagu_restart"].values())
        a.kill()
        a.wait(timeout=5)
        observations["director_a_crash_exit"] = a.returncode
        # B continues its own factory while A is absent; Dagu remains owned by this supervisor.
        send(b_url, {"op": "decide", "key": "bundle:b:v2:publication",
            "run_id": "dagu-product-v2-001", "gate": "publication_gate"})
        active_snapshots = []
        def active_check():
            active_snapshots.append(tree_rss_kib())
            return waiting_at(dagu_url, "v2", "director")
        poll(active_check, "v2 Director gate")
        observations["rss_active_peak_observed"] = max(active_snapshots,
            key=lambda sample: sample["rss_sum_kib"])
        observations["active_samples"] = len(active_snapshots)
        send(b_url, {"op": "decide", "key": "bundle:b:v2:director",
            "run_id": "dagu-product-v2-001", "gate": "director"})
        poll(lambda: delivered(dagu_url, "v2"), "v2 delivery")
        observations["b_completed_while_a_down"] = rpc(b_url, "tasks/get", {"id": v2["id"]})
        a = launch(director_cmd("director_a", a_port), env, rt / "director_a-restarted.log",
                   a_url + "/health")
        observations["director_a_restarted"] = http(a_url + "/health")
        observations["a_restored_task"] = rpc(a_url, "tasks/get", {"id": v1["id"]})
        send(a_url, {"op": "decide", "key": "bundle:a:v1:publication",
            "run_id": "dagu-product-v1-001", "gate": "publication_gate"})
        poll(lambda: cap.poll() is not None, "v1 capability lost acknowledgement", 30)
        cap = launch(cap_cmd, env, rt / "capability-restarted.log", cap_url + "/health")
        poll(lambda: waiting_at(dagu_url, "v1", "director"), "v1 Director gate")
        send(a_url, {"op": "decide", "key": "bundle:a:v1:director",
            "run_id": "dagu-product-v1-001", "gate": "director"})
        poll(lambda: delivered(dagu_url, "v1"), "v1 delivery")
        observations["a_completed_after_restart"] = rpc(a_url, "tasks/get", {"id": v1["id"]})
        observations["rss_final"] = tree_rss_kib()
        observations["runs"] = {version: run_status(dagu_url, version) for version in ("v1", "v2")}
        with sqlite3.connect(rt / "ledger.sqlite") as db:
            db.row_factory = sqlite3.Row
            observations["ledger"] = [dict(row) for row in db.execute("SELECT * FROM runs ORDER BY run_id")]
        assert observations["identities_initial"]["director_a"]["identity"] == observations["director_a_restarted"]["identity"]
        assert observations["director_a_restarted"]["incarnation"] == 2
        assert observations["a_restored_task"]["status"]["state"] == "input-required"
        assert observations["b_completed_while_a_down"]["status"]["state"] == "completed"
        assert observations["a_completed_after_restart"]["status"]["state"] == "completed"
        assert all(row["release_count"] == 1 for row in observations["ledger"])
        (rt / "bundle-result.json").write_text(json.dumps(observations, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"result": "passed", "evidence": str(rt / "bundle-result.json"),
                          "rss_final": observations["rss_final"]}, sort_keys=True))
    except Exception:
        (rt / "bundle-partial.json").write_text(json.dumps(observations, indent=2, sort_keys=True) + "\n")
        raise
    finally:
        for process in (a, b, dagu, cap, quality):
            stop(process)


if __name__ == "__main__":
    main()
