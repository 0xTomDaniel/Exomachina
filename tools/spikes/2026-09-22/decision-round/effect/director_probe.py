"""Exercise the Effect-backed Director through the real Strands/A2A harness path."""
from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
from uuid import uuid4

import httpx

HERE = Path(__file__).resolve().parent
COMMON = HERE.parent / "common" / "harness_server.py"
PYTHON = HERE.parents[1] / "s2" / ".venv" / "bin" / "python"
ROOT = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else Path(tempfile.mkdtemp(prefix="exomachina-effect-director-"))
ROOT.mkdir(parents=True, exist_ok=True)
OBSERVED = HERE / "director-observed.json"
TOKEN = {"authorization": "Bearer director-test-token"}


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def start(argv, url, log, env=None):
    process = subprocess.Popen(argv, cwd=HERE, env=env, stdout=log, stderr=subprocess.STDOUT)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"{process.pid} exited during boot: {process.returncode}")
        try:
            response = httpx.get(url + "/health", timeout=.5)
            if response.status_code == 200:
                return process, response.json()
        except httpx.HTTPError:
            pass
        time.sleep(.1)
    raise TimeoutError("startup timeout: " + str(argv))


def stop(process, hard=False):
    if process is not None and process.poll() is None:
        process.send_signal(signal.SIGKILL if hard else signal.SIGTERM)
        process.wait(timeout=10)


def rpc(url, method, params):
    response = httpx.post(url + "/", headers=TOKEN, timeout=25,
                          json={"jsonrpc": "2.0", "id": str(uuid4()),
                                "method": method, "params": params})
    response.raise_for_status()
    body = response.json()
    assert "error" not in body, body
    return body["result"]


def send(url, command, task_id=None):
    message = {"role": "user", "messageId": str(uuid4()),
               "parts": [{"kind": "data", "data": command}]}
    if task_id:
        message["taskId"] = task_id
    return rpc(url, "message/send", {"message": message})


def task_until(url, task_id, state, seconds=45):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        last = rpc(url, "tasks/get", {"id": task_id})
        if last["status"]["state"] == state:
            return last
        time.sleep(.2)
    raise TimeoutError(f"task did not reach {state}: {last}")


def rss_kib(process):
    return int(subprocess.check_output(["ps", "-o", "rss=", "-p", str(process.pid)], text=True))


def main():
    evidence = {"runtime_root": str(ROOT), "python": subprocess.check_output(
        [str(PYTHON), "--version"], text=True).strip()}
    processes = []
    with (ROOT / "director.log").open("a") as director_log, \
         (ROOT / "effect.log").open("a") as effect_log, \
         (ROOT / "worker.log").open("a") as worker_log, \
         (ROOT / "quality.log").open("a") as quality_log:
        try:
            ports = {name: free_port() for name in ("worker", "quality", "effect", "director")}
            urls = {name: f"http://127.0.0.1:{port}" for name, port in ports.items()}
            worker, worker_info = start([str(PYTHON), str(COMMON), "--state", str(ROOT / "worker"),
                                         "--role", "capability", "--port", str(ports["worker"])],
                                        urls["worker"], worker_log)
            processes.append(worker)
            quality, quality_info = start([str(PYTHON), str(COMMON), "--state", str(ROOT / "quality"),
                                           "--role", "quality", "--port", str(ports["quality"])],
                                          urls["quality"], quality_log)
            processes.append(quality)
            env = os.environ.copy()
            env["EFFECT_DECISION_ROOT"] = str(ROOT / "effect")
            env["EFFECT_DECISION_PORT"] = str(ports["effect"])
            effect, effect_info = start(["node", str(HERE / "helper.mjs")], urls["effect"], effect_log, env)
            processes.append(effect)
            director_args = [str(PYTHON), str(HERE / "director_server.py"),
                             "--state", str(ROOT / "director"), "--effect", urls["effect"],
                             "--port", str(ports["director"])]
            director, director_info = start(director_args, urls["director"], director_log)
            processes.append(director)
            evidence["initial"] = {"worker": worker_info, "quality": quality_info,
                                   "effect": effect_info, "director": director_info}
            evidence["ports"] = ports
            assert len({worker_info["identity"], quality_info["identity"], director_info["identity"]}) == 3
            doc = {"name": "verified-research", "version": 1,
                   "capabilities": {
                       "worker": {"identity": worker_info["identity"], "url": urls["worker"], "version": "v1"},
                       "quality": {"identity": quality_info["identity"], "url": urls["quality"], "version": "v1"}},
                   "steps": ["assign", "review", "director_wait", "deliver"]}
            published = httpx.post(urls["effect"] + "/publish", json=doc).json()
            digest = published["digest"]
            evidence["published"] = published
            command = {"op": "start", "key": "director-start-1", "run_id": "director-run", "digest": digest}
            started = send(urls["director"], command)
            assert started["kind"] == "task", started
            task_id = started["id"]
            evidence["started"] = started
            waiting = task_until(urls["director"], task_id, "input-required")
            assert not waiting.get("artifacts")
            evidence["waiting"] = waiting
            evidence["rss_kib_waiting"] = {"worker": rss_kib(worker), "quality": rss_kib(quality),
                                           "effect": rss_kib(effect), "director": rss_kib(director)}
            stop(director, hard=True)
            evidence["director_killed_pid"] = director.pid
            restarted, info2 = start(director_args, urls["director"], director_log)
            processes.append(restarted)
            evidence["director_restart"] = info2
            assert info2["identity"] == director_info["identity"]
            assert info2["incarnation"] == director_info["incarnation"] + 1
            restored = rpc(urls["director"], "tasks/get", {"id": task_id})
            assert restored["status"]["state"] == "input-required"
            assert restored["metadata"]["run_id"] == "director-run"
            evidence["restored"] = restored
            duplicate = send(urls["director"], command)
            assert duplicate["metadata"]["run_id"] == "director-run"
            evidence["duplicate_start"] = duplicate
            decided = send(urls["director"], {"op": "decide", "key": "director-decision-1",
                                                    "run_id": "director-run", "digest": digest}, task_id)
            evidence["decision_response"] = decided
            completed = task_until(urls["director"], task_id, "completed")
            evidence["completed"] = completed
            accepted = completed["artifacts"][0]["parts"][0]["data"]
            assert accepted["definition"] == digest and accepted["revision"] == "r2"
            ledger = httpx.post(urls["effect"] + "/ledger", json={"id": "director-run"}).json()
            assert ledger["delivery"] and ledger["outbox"]["state"] == "signaled"
            assert accepted["sha256"] == ledger["run"]["accepted_sha256"]
            evidence["final_ledger"] = ledger
            evidence["receiver_actions"] = {
                role: httpx.get(urls[role] + "/fixture/actions/director-run-" + action,
                                headers={"authorization": "Bearer fixture-token"}).json()
                for role, action in (("worker", "assign"), ("quality", "review"))}
            assert all(row["accepted_count"] == 1 for row in evidence["receiver_actions"].values())
            evidence["passed"] = True
        except Exception as error:
            evidence["passed"] = False
            evidence["error"] = repr(error)
            raise
        finally:
            for process in processes:
                stop(process)
            OBSERVED.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
            print(json.dumps({"passed": evidence["passed"], "error": evidence.get("error"),
                              "rss_kib_waiting": evidence.get("rss_kib_waiting")}, indent=2))


if __name__ == "__main__":
    main()
