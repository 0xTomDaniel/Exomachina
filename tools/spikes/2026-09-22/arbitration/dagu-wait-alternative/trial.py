"""Run the isolated, real-Dagu two-root wait and one-shot bridge trial."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

import bridge

HERE = Path(__file__).resolve().parent
DAGU = Path("/tmp/exomachina-countertrials/dagu/dagu")
DAGU_SHA256 = "fd855996bab956835043ae8cbb00ac3ea5211a6be6992374727b7908d393aac3"
RUNTIME = Path("/tmp/exomachina-dagu-wait-alternative-001")


def api(base: str, path: str, payload: dict | None = None) -> dict:
    return bridge.request(base, path, payload)


def until(check, label: str, timeout: float = 25):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except (OSError, ValueError, KeyError, urllib.error.URLError) as error:
            last = repr(error)
        time.sleep(.15)
    raise TimeoutError(f"{label}: {last}")


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def native(base: str, name: str, run_id: str) -> dict:
    detail = bridge.status(base, name, run_id)
    return {"run_id": run_id, "dag_name": name, "status": detail["statusLabel"],
            "nodes": bridge.nodes(detail)}


def is_waiting(base: str, name: str, run_id: str, step: str) -> bool:
    item = native(base, name, run_id)
    return item["status"] == "waiting" and item["nodes"].get(step) == "waiting"


def is_succeeded(base: str, name: str, run_id: str, continuation: str) -> bool:
    item = native(base, name, run_id)
    return item["status"] == "succeeded" and item["nodes"].get(continuation) == "succeeded"


def process_snapshot(runtime: Path, server_pid: int) -> dict:
    listing = subprocess.check_output(["ps", "-axo", "pid=,ppid=,rss=,command="], text=True)
    matched = []
    for line in listing.splitlines():
        parts = line.strip().split(None, 3)
        if len(parts) != 4:
            continue
        pid, ppid, rss, command = parts
        if (int(pid) == server_pid or str(runtime / "home" / "dags") in command
                or bridge.PARENT_ID in command or bridge.CHILD_ID in command):
            matched.append({"pid": int(pid), "ppid": int(ppid), "rss_kib": int(rss),
                            "command": command})
    dagu_rows = [row for row in matched if "dagu" in Path(row["command"].split()[0]).name]
    bridge_rows = [row for row in matched if "bridge.py" in row["command"]]
    return {"matched_processes": matched, "dagu_process_count": len(dagu_rows),
            "bridge_process_count": len(bridge_rows)}


def bridge_command(operation: str, *options: str, success: bool = True) -> dict:
    result = subprocess.run([sys.executable, str(HERE / "bridge.py"), operation, str(RUNTIME), *options],
                            capture_output=True, text=True, timeout=12)
    if success and result.returncode:
        raise RuntimeError(f"bridge {operation} failed: {result.stderr[-1000:]}")
    if not success and not result.returncode:
        raise AssertionError(f"bridge {operation} unexpectedly succeeded")
    return json.loads(result.stdout) if success else {"returncode": result.returncode,
                                                       "error": result.stderr.strip().splitlines()[-1]}


def start_server(runtime: Path, port: int, env: dict) -> subprocess.Popen:
    stream = (runtime / "dagu.log").open("ab")
    process = subprocess.Popen([str(DAGU), "start-all", "--host", "127.0.0.1", "--port", str(port)],
                               env=env, stdout=stream, stderr=stream, start_new_session=True)
    stream.close()
    base = f"http://127.0.0.1:{port}"
    until(lambda: api(base, "/api/v1/dags"), "Dagu readiness", 30)
    return process


def stop_server(process: subprocess.Popen | None) -> None:
    if process is None or process.poll() is not None:
        return
    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=8)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=3)


def main() -> None:
    if RUNTIME.exists():
        raise SystemExit(f"runtime must be fresh: {RUNTIME}")
    if hashlib.sha256(DAGU.read_bytes()).hexdigest() != DAGU_SHA256:
        raise SystemExit("pinned Dagu binary unavailable or changed")
    dags = RUNTIME / "home" / "dags"
    dags.mkdir(parents=True)
    for source in (HERE / "definitions").glob("*.yaml"):
        shutil.copy2(source, dags / source.name)
    env = {**os.environ, "DAGU_HOME": str(RUNTIME / "home"),
           "DAGU_AUTH_MODE": "none", "DAGU_COORDINATOR_ENABLED": "false"}
    evidence: dict = {"dagu_binary_sha256": DAGU_SHA256, "runtime": str(RUNTIME),
                      "definition_sha256": {source.name: hashlib.sha256(source.read_bytes()).hexdigest()
                                            for source in dags.glob("*.yaml")}}
    server = None
    try:
        for source in dags.glob("*.yaml"):
            validated = subprocess.run([str(DAGU), "validate", str(source)], env=env,
                                       capture_output=True, text=True, timeout=15)
            if validated.returncode:
                raise RuntimeError(f"Dagu validation failed: {source.name}: {validated.stderr}")
        evidence["registered"] = bridge_command("register")
        port = free_port()
        base = f"http://127.0.0.1:{port}"
        server = start_server(RUNTIME, port, env)
        evidence["first_dagu_pid"] = server.pid
        evidence["parent_start"] = api(base, f"/api/v1/dags/{bridge.PARENT_NAME}.yaml/start",
                                       {"dagRunId": bridge.PARENT_ID})
        evidence["child_start"] = api(base, f"/api/v1/dags/{bridge.CHILD_NAME}.yaml/start",
                                      {"dagRunId": bridge.CHILD_ID})
        until(lambda: is_waiting(base, bridge.PARENT_NAME, bridge.PARENT_ID, "child_gate"), "parent wait")
        until(lambda: is_waiting(base, bridge.CHILD_NAME, bridge.CHILD_ID, "director_wait"), "child wait")
        evidence["both_waiting"] = {
            "parent": native(base, bridge.PARENT_NAME, bridge.PARENT_ID),
            "child": native(base, bridge.CHILD_NAME, bridge.CHILD_ID),
            "processes": process_snapshot(RUNTIME, server.pid)}
        stop_server(server)
        server = start_server(RUNTIME, port, env)
        evidence["restarted_dagu_pid"] = server.pid
        until(lambda: is_waiting(base, bridge.PARENT_NAME, bridge.PARENT_ID, "child_gate"),
              "parent wait after restart")
        until(lambda: is_waiting(base, bridge.CHILD_NAME, bridge.CHILD_ID, "director_wait"),
              "child wait after restart")
        evidence["after_restart"] = {
            "parent": native(base, bridge.PARENT_NAME, bridge.PARENT_ID),
            "child": native(base, bridge.CHILD_NAME, bridge.CHILD_ID),
            "processes": process_snapshot(RUNTIME, server.pid)}
        evidence["claimed"] = bridge_command("claim", "--expected-epoch", "1")
        evidence["stale_child_command"] = bridge_command(
            "complete-child", "--base", base, "--expected-epoch", "1",
            "--token", bridge.FIXTURE_TOKEN, success=False)
        evidence["unauthorized_child_command"] = bridge_command(
            "complete-child", "--base", base, "--expected-epoch", "2",
            "--token", "invalid", success=False)
        evidence["authorized_child_command"] = bridge_command(
            "complete-child", "--base", base, "--expected-epoch", "2",
            "--token", bridge.FIXTURE_TOKEN)
        until(lambda: is_succeeded(base, bridge.CHILD_NAME, bridge.CHILD_ID, "child_continuation"),
              "child completion")
        evidence["child_done_parent_waiting"] = {
            "parent": native(base, bridge.PARENT_NAME, bridge.PARENT_ID),
            "child": native(base, bridge.CHILD_NAME, bridge.CHILD_ID)}
        if evidence["child_done_parent_waiting"]["parent"]["status"] != "waiting":
            raise AssertionError("parent advanced without product reconciliation")
        evidence["stale_reconcile"] = bridge_command("reconcile", "--base", base,
                                                      "--expected-epoch", "1", success=False)
        if not is_waiting(base, bridge.PARENT_NAME, bridge.PARENT_ID, "child_gate"):
            raise AssertionError("stale owner advanced parent")
        evidence["reconciled"] = bridge_command("reconcile", "--base", base,
                                                "--expected-epoch", "2")
        until(lambda: is_succeeded(base, bridge.PARENT_NAME, bridge.PARENT_ID, "parent_continuation"),
              "parent continuation")
        evidence["idempotent_reconcile"] = bridge_command("reconcile", "--base", base,
                                                           "--expected-epoch", "2")
        evidence["final"] = {"parent": native(base, bridge.PARENT_NAME, bridge.PARENT_ID),
                             "child": native(base, bridge.CHILD_NAME, bridge.CHILD_ID),
                             "processes": process_snapshot(RUNTIME, server.pid)}
        with bridge.connect(RUNTIME) as db:
            evidence["bridge_row"] = dict(db.execute("SELECT * FROM bridge").fetchone())
        evidence["outcome"] = "passed"
    except Exception as error:
        evidence["outcome"] = "failed"
        evidence["error"] = repr(error)
        raise
    finally:
        stop_server(server)
        (HERE / "observed.json").write_text(json.dumps(evidence, sort_keys=True, indent=2) + "\n")
    print(json.dumps({"outcome": evidence["outcome"], "runtime": str(RUNTIME)}, sort_keys=True))


if __name__ == "__main__":
    main()
