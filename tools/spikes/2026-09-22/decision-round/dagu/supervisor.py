"""Long-running copied-bundle supervisor for one Dagu helper and two factories.

The supervisor is intentionally small and local. It owns child process startup,
restart and shutdown; it does not own workflow graph scheduling or acceptance.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path
import shlex
import signal
import socket
import subprocess
import sys
import time
import urllib.request
from uuid import uuid4

from publisher import publish


HERE = Path(__file__).resolve().parent
BUNDLE = HERE.parent
PYTHON = BUNDLE / "venv" / "bin" / "python"
DAGU = BUNDLE / "bin" / "dagu"
COMMON = BUNDLE / "common"
stopping = False


def port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def healthy(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=.6) as response:
            return response.status == 200
    except OSError:
        return False


def stop_requested(*_args) -> None:
    global stopping
    stopping = True


def stop(process: subprocess.Popen | None) -> None:
    if process and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def terminate_orphan(pid: int) -> None:
    """Stop a recorded child group after the former supervisor was hard-killed."""
    try:
        if os.getpgid(pid) != pid:
            raise RuntimeError(f"recorded child {pid} is no longer its own process group")
        command = subprocess.check_output(["ps", "-p", str(pid), "-o", "command="], text=True)
        if str(BUNDLE) not in command:
            raise RuntimeError(f"refusing to signal changed child PID {pid}: {command}")
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    for _ in range(30):
        try:
            os.killpg(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(.1)
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("runtime", type=Path)
    args = parser.parse_args()
    rt = args.runtime.resolve()
    rt.mkdir(parents=True, exist_ok=True)
    lock = (rt / "supervisor.lock").open("w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit("supervisor is already running for this runtime")
    signal.signal(signal.SIGTERM, stop_requested)
    signal.signal(signal.SIGINT, stop_requested)
    existing = json.loads((rt / "endpoints.json").read_text()) if (rt / "endpoints.json").exists() else None
    if existing:
        if existing["bundle"] != str(BUNDLE):
            raise SystemExit("runtime belongs to another bundle path")
        for old_pid in existing["pids"].values():
            terminate_orphan(old_pid)
        numbers, run_ids = existing["ports"], existing["run_ids"]
    else:
        numbers = {key: port() for key in
            ("capability", "quality", "dagu", "director_a", "director_b", "dagu_scheduler")}
        run_ids = {version: f"dagu-product-{version}-{uuid4().hex}" for version in ("v1", "v2")}
        (rt / "run_ids.json").write_text(json.dumps(run_ids, sort_keys=True) + "\n")
    urls = {key: f"http://127.0.0.1:{value}" for key, value in numbers.items() if key != "dagu_scheduler"}
    bin_dir = rt / "bin"
    bin_dir.mkdir(exist_ok=True)
    wrapper = bin_dir / "exo-dagu-adapter"
    wrapper.write_text("#!/bin/sh\n"
        + "export EXO_DAGU_RUNTIME=" + shlex.quote(str(rt)) + "\n"
        + "export EXO_CAPABILITY_URL=" + shlex.quote(urls["capability"]) + "\n"
        + "export EXO_QUALITY_URL=" + shlex.quote(urls["quality"]) + "\n"
        + "exec " + shlex.quote(str(PYTHON)) + " " + shlex.quote(str(HERE / "adapter.py")) + " \"$@\"\n")
    wrapper.chmod(0o755)
    env = {**os.environ, "PATH": str(bin_dir) + os.pathsep + os.environ["PATH"],
           "DAGU_HOME": str(rt / "home"), "DAGU_AUTH_MODE": "none",
           "DAGU_COORDINATOR_ENABLED": "false",
           "DAGU_SCHEDULER_PORT": str(numbers["dagu_scheduler"])}
    manifests = {version: publish(HERE / "definitions", rt / "home", version, DAGU)
                 for version in ("v1", "v2")}

    def director(name: str) -> list[str]:
        return [str(PYTHON), str(HERE / "director_server.py"),
                "--state", str(rt / name), "--runtime", str(rt),
                "--dagu-url", urls["dagu"], "--port", str(numbers[name])]

    commands = {
        "capability": [str(PYTHON), str(COMMON / "harness_server.py"),
            "--state", str(rt / "capability"), "--role", "capability",
            "--port", str(numbers["capability"])],
        "quality": [str(PYTHON), str(COMMON / "harness_server.py"),
            "--state", str(rt / "quality"), "--role", "quality",
            "--port", str(numbers["quality"])],
        "dagu": [str(DAGU), "start-all", "--host", "127.0.0.1", "--port", str(numbers["dagu"])],
        "director_a": director("director_a"), "director_b": director("director_b"),
    }
    health = {name: urls[name] + ("/api/v1/dags" if name == "dagu" else "/health")
              for name in commands}
    processes: dict[str, subprocess.Popen] = {}
    starts = existing["starts"] if existing else {name: 0 for name in commands}
    observed_exits: set[int] = set()
    events = rt / "supervisor-events.jsonl"

    def event(kind: str, name: str, **fields) -> None:
        with events.open("a") as stream:
            stream.write(json.dumps({"time": time.time(), "kind": kind, "name": name, **fields}) + "\n")

    def registry() -> None:
        body = {"supervisor_pid": os.getpid(), "bundle": str(BUNDLE), "runtime": str(rt),
                "ports": numbers, "urls": urls, "pids": {name: p.pid for name, p in processes.items()},
                "starts": starts, "manifests": manifests, "run_ids": run_ids}
        tmp = rt / "endpoints.json.tmp"
        tmp.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n")
        os.replace(tmp, rt / "endpoints.json")

    def start(name: str) -> None:
        starts[name] += 1
        with (rt / f"{name}-{starts[name]}.log").open("ab") as output:
            process = subprocess.Popen(commands[name], env=env, stdout=output, stderr=output,
                                       start_new_session=True)
        processes[name] = process
        deadline = time.monotonic() + 20
        while not stopping and time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(f"{name} exited during startup: {process.returncode}")
            if healthy(health[name]):
                event("started", name, pid=process.pid, count=starts[name])
                registry()
                return
            time.sleep(.1)
        raise TimeoutError(f"{name} did not become healthy")

    try:
        for name in commands:
            if stopping:
                break
            start(name)
        event("ready", "supervisor", pid=os.getpid())
        while not stopping:
            for name, process in list(processes.items()):
                code = process.poll()
                if code is None:
                    continue
                if process.pid not in observed_exits:
                    event("exited", name, pid=process.pid, code=code)
                    observed_exits.add(process.pid)
                if (rt / f"hold-{name}").exists():
                    continue  # test-only operator hold; other instances continue
                time.sleep(.15)
                if not stopping:
                    start(name)
            time.sleep(.1)
    finally:
        event("stopping", "supervisor")
        for name in reversed(list(commands)):
            stop(processes.get(name))
        event("stopped", "supervisor")


if __name__ == "__main__":
    main()
