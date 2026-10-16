"""Drive two factory Directors in the copied one-command local bundle."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import time
from urllib.error import URLError
from urllib.request import Request, urlopen
from uuid import uuid4

HERE = Path(__file__).resolve().parent


def request(url, route, body=None, token=None):
    headers = {"content-type": "application/json"}
    if token:
        headers["authorization"] = "Bearer " + token
    data = json.dumps(body).encode() if body is not None else None
    req = Request(url + route, data=data, headers=headers)
    with urlopen(req, timeout=25) as response:
        return json.load(response)


def rpc(url, method, params):
    response = request(url, "/", {"jsonrpc": "2.0", "id": str(uuid4()),
                                  "method": method, "params": params}, "director-test-token")
    assert "error" not in response, response
    return response["result"]


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
    raise TimeoutError(f"{task_id} did not reach {state}: {last}")


def ready_until(path, condition, seconds=45):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        if path.exists():
            last = json.loads(path.read_text())
            if condition(last):
                return last
        time.sleep(.1)
    raise TimeoutError(f"bundle ready state not reached: {last}")


def rss_kib(pid):
    return int(subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)], text=True))


def disk_kib(path):
    return int(subprocess.check_output(["du", "-sk", str(path)], text=True).split()[0])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    args = parser.parse_args()
    bundle, state = args.bundle.resolve(), args.state.resolve()
    if state.exists():
        raise SystemExit("fresh state path required")
    state.mkdir(parents=True)
    observed = {"bundle": str(bundle), "state": str(state)}
    observed["bundle_manifest"] = json.loads((bundle / "manifest.json").read_text())
    supervisor = None
    with (state / "supervisor.log").open("a") as log:
        try:
            start_time = time.monotonic()
            supervisor = subprocess.Popen([str(bundle / "run"), "--state", str(state)],
                                          cwd=bundle, stdout=log, stderr=subprocess.STDOUT)
            ready_path = state / "ready.json"
            ready = ready_until(ready_path, lambda data: len(data["children"]) == 5)
            observed["startup_wall_seconds"] = round(time.monotonic() - start_time, 3)
            observed["ready_startup_seconds"] = ready["startup_seconds"]
            observed["initial_ready"] = ready
            ports = ready["ports"]
            urls = {name: f"http://127.0.0.1:{port}" for name, port in ports.items()}
            observed["rss_kib_warm"] = {"supervisor": rss_kib(supervisor.pid),
                **{name: rss_kib(item["pid"]) for name, item in ready["children"].items()}}
            observed["process_commands"] = {"supervisor": subprocess.check_output(
                ["ps", "-o", "command=", "-p", str(supervisor.pid)], text=True).strip(),
                **{name: subprocess.check_output(["ps", "-o", "command=", "-p", str(item["pid"])],
                                               text=True).strip()
                   for name, item in ready["children"].items()}}
            assert all("/Exomachina/" not in command
                       for command in observed["process_commands"].values())
            observed["python_runtime_paths"] = json.loads(subprocess.check_output(
                [str(bundle / "venv" / "bin" / "python"), "-c",
                 "import json,sys,strands,a2a; print(json.dumps({'executable':sys.executable,"
                 "'base_prefix':sys.base_prefix,'paths':sys.path,"
                 "'strands':strands.__file__,'a2a':a2a.__file__}))"], text=True))
            assert "/Exomachina/" not in json.dumps(observed["python_runtime_paths"])
            worker = ready["children"]["capability"]["health"]
            quality = ready["children"]["quality"]["health"]
            a = ready["children"]["director_a"]["health"]
            b = ready["children"]["director_b"]["health"]
            assert len({worker["identity"], quality["identity"], a["identity"], b["identity"]}) == 4
            doc = {"name": "verified-research", "version": 1,
                   "capabilities": {
                       "worker": {"identity": worker["identity"], "url": urls["capability"], "version": "v1"},
                       "quality": {"identity": quality["identity"], "url": urls["quality"], "version": "v1"}},
                   "steps": ["assign", "review", "director_wait", "deliver"]}
            digest = request(urls["effect"], "/publish", doc)["digest"]
            observed["digest"] = digest
            first = send(urls["director_a"], {"op": "start", "key": "bundle-a-start",
                                               "run_id": "bundle-run-a", "digest": digest})
            second = send(urls["director_b"], {"op": "start", "key": "bundle-b-start",
                                                "run_id": "bundle-run-b", "digest": digest})
            task_until(urls["director_a"], first["id"], "input-required")
            task_until(urls["director_b"], second["id"], "input-required")
            observed["waiting_tasks"] = {"a": first["id"], "b": second["id"]}
            observed["rss_kib_two_waiting"] = {"supervisor": rss_kib(supervisor.pid),
                **{name: rss_kib(item["pid"]) for name, item in ready["children"].items()}}
            old_a_pid = ready["children"]["director_a"]["pid"]
            old_b_pid = ready["children"]["director_b"]["pid"]
            old_effect_pid = ready["children"]["effect"]["pid"]
            os.kill(old_a_pid, signal.SIGKILL)
            after_director = ready_until(ready_path,
                lambda data: data["children"]["director_a"]["pid"] != old_a_pid)
            observed["after_director_restart"] = after_director
            assert after_director["children"]["director_a"]["health"]["identity"] == a["identity"]
            assert after_director["children"]["director_a"]["health"]["incarnation"] == a["incarnation"] + 1
            assert after_director["children"]["director_b"]["pid"] == old_b_pid
            assert task_until(urls["director_b"], second["id"], "input-required")
            assert task_until(urls["director_a"], first["id"], "input-required")
            os.kill(old_effect_pid, signal.SIGKILL)
            after_effect = ready_until(ready_path,
                lambda data: data["children"]["effect"]["pid"] != old_effect_pid)
            observed["after_effect_restart"] = after_effect
            assert after_effect["children"]["director_a"]["pid"] == after_director["children"]["director_a"]["pid"]
            assert after_effect["children"]["director_b"]["pid"] == old_b_pid
            assert task_until(urls["director_a"], first["id"], "input-required")
            assert task_until(urls["director_b"], second["id"], "input-required")
            send(urls["director_b"], {"op": "decide", "run_id": "bundle-run-b", "digest": digest}, second["id"])
            send(urls["director_a"], {"op": "decide", "run_id": "bundle-run-a", "digest": digest}, first["id"])
            completed_a = task_until(urls["director_a"], first["id"], "completed")
            completed_b = task_until(urls["director_b"], second["id"], "completed")
            observed["completed"] = {"a": completed_a, "b": completed_b}
            for name, run_id in (("a", "bundle-run-a"), ("b", "bundle-run-b")):
                assert observed["completed"][name]["metadata"]["run_id"] == run_id
            observed["ledgers"] = {run: request(urls["effect"], "/ledger", {"id": run})
                                   for run in ("bundle-run-a", "bundle-run-b")}
            assert all(row["delivery"] and row["outbox"]["state"] == "signaled"
                       for row in observed["ledgers"].values())
            observed["receipts"] = {
                run: {role: request(urls[endpoint], "/fixture/actions/" + run + "-" + action,
                                    token="fixture-token")
                      for role, endpoint, action in (("assign", "capability", "assign"),
                                                     ("review", "quality", "review"))}
                for run in ("bundle-run-a", "bundle-run-b")}
            assert all(a["accepted_count"] == 1 for row in observed["receipts"].values()
                       for a in row.values())
            observed["bundle_disk_kib"] = disk_kib(bundle)
            observed["state_disk_kib"] = disk_kib(state)
            observed["passed"] = True
        except Exception as error:
            observed["passed"] = False
            observed["error"] = repr(error)
            raise
        finally:
            if supervisor is not None and supervisor.poll() is None:
                supervisor.terminate()
                supervisor.wait(timeout=20)
            observed["supervisor_exit_code"] = None if supervisor is None else supervisor.returncode
            (HERE / "bundle-observed.json").write_text(json.dumps(observed, indent=2, sort_keys=True) + "\n")
            print(json.dumps({"passed": observed["passed"], "error": observed.get("error"),
                              "startup_seconds": observed.get("startup_wall_seconds"),
                              "rss_kib_warm": observed.get("rss_kib_warm"),
                              "rss_kib_two_waiting": observed.get("rss_kib_two_waiting")}, indent=2))


if __name__ == "__main__":
    main()
