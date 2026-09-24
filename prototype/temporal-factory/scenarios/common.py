"""Caller-side helpers for the integrated scenarios: A2A client, processes, evidence."""
from __future__ import annotations

import json
import hashlib
import os
import signal
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
PY = sys.executable
TOKEN = "Bearer fixture-token"
sys.path.insert(0, str(SRC))
sys.path.insert(0, str(ROOT / "services"))


def http(url: str, payload: dict | None = None, *, token: bool = True, timeout: float = 300) -> dict:
    data = None if payload is None else json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = TOKEN
    request = urllib.request.Request(url, data=data, headers=headers,
                                     method="GET" if payload is None else "POST")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read())


def a2a_send(base: str, data: dict, *, task_id: str | None = None,
             context_id: str | None = None) -> dict:
    message = {"role": "user", "messageId": str(uuid4()), "kind": "message",
               "parts": [{"kind": "data", "data": data}]}
    if task_id:
        message["taskId"] = task_id
    if context_id:
        message["contextId"] = context_id
    reply = http(base.rstrip("/") + "/", {"jsonrpc": "2.0", "id": str(uuid4()),
                                          "method": "message/send", "params": {"message": message}})
    if "error" in reply:
        raise RuntimeError("A2A error: " + json.dumps(reply["error"]))
    return reply["result"]


def a2a_get(base: str, task_id: str) -> dict:
    reply = http(base.rstrip("/") + "/", {"jsonrpc": "2.0", "id": str(uuid4()),
                                          "method": "tasks/get", "params": {"id": task_id}})
    if "error" in reply:
        raise RuntimeError("A2A error: " + json.dumps(reply["error"]))
    return reply["result"]


def poll_task(base: str, task_id: str, states: set[str], *, seconds: float = 240) -> dict:
    deadline = time.monotonic() + seconds
    seen = []
    while time.monotonic() < deadline:
        task = a2a_get(base, task_id)
        state = task["status"]["state"]
        if not seen or seen[-1] != state:
            seen.append(state)
        if state in states:
            task["_observed_states"] = seen
            return task
        time.sleep(0.5)
    raise TimeoutError(f"task {task_id} did not reach {states}; saw {seen}")


def wait_http(url: str, seconds: float = 60) -> dict:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            return http(url, token=False, timeout=3)
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            time.sleep(0.25)
    raise TimeoutError(url)


def start_harness(instance_dir: Path, port: int) -> subprocess.Popen:
    log = (instance_dir / "harness.log").open("a")
    process = subprocess.Popen([PY, "-B", str(SRC / "harness.py"), "serve",
                                "--instance-dir", str(instance_dir)],
                               stdout=log, stderr=log, start_new_session=True)
    log.close()
    wait_http(f"http://127.0.0.1:{port}/health")
    return process


def stop_process(process: subprocess.Popen, timeout: float = 30) -> int:
    if process.poll() is None:
        os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=timeout)
    return process.returncode


def run_cli(*args: str, timeout: float = 300) -> str:
    completed = subprocess.run([PY, "-B", *args], text=True, capture_output=True, timeout=timeout)
    if completed.returncode:
        raise RuntimeError(f"{args} exited {completed.returncode}: {completed.stderr[-2000:]}")
    return completed.stdout


def jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def sqlite_rows(path: Path, query: str) -> list[dict]:
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in db.execute(query)]
    finally:
        db.close()


def write_evidence(name: str, value: dict) -> Path:
    path = ROOT / "evidence" / name
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n")
    return path


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
