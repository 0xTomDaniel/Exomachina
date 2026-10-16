"""Kill and relaunch the copied-bundle supervisor with two waiting A2A factories."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import time

from bundle_probe import ready_until, request, rpc, send, task_until

HERE = Path(__file__).resolve().parent


def status(pid):
    result = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)],
                            capture_output=True, text=True)
    return result.stdout.strip() if result.returncode == 0 else ""


def command(pid):
    result = subprocess.run(["ps", "-o", "command=", "-p", str(pid)],
                            capture_output=True, text=True)
    return result.stdout.strip() if result.returncode == 0 else ""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    args = parser.parse_args()
    bundle, state = args.bundle.resolve(), args.state.resolve()
    if state.exists():
        raise SystemExit("fresh state path required")
    state.mkdir(parents=True)
    observed = {"bundle": str(bundle), "state": str(state),
                "bundle_manifest": json.loads((bundle / "manifest.json").read_text())}
    first = second = None
    old_children = {}
    with (state / "supervisor-first.log").open("a") as log1, \
         (state / "supervisor-second.log").open("a") as log2:
        try:
            first = subprocess.Popen([str(bundle / "run"), "--state", str(state)],
                                     cwd=bundle, stdout=log1, stderr=subprocess.STDOUT)
            ready_path = state / "ready.json"
            initial = ready_until(ready_path, lambda x: x.get("generation") == 1 and len(x["children"]) == 5)
            observed["initial"] = initial
            old_children = {name: item["pid"] for name, item in initial["children"].items()}
            urls = {name: f"http://127.0.0.1:{port}" for name, port in initial["ports"].items()}
            worker = initial["children"]["capability"]["health"]
            quality = initial["children"]["quality"]["health"]
            doc = {"name": "verified-research", "version": 1,
                   "capabilities": {
                       "worker": {"identity": worker["identity"], "url": urls["capability"], "version": "v1"},
                       "quality": {"identity": quality["identity"], "url": urls["quality"], "version": "v1"}},
                   "steps": ["assign", "review", "director_wait", "deliver"]}
            digest = request(urls["effect"], "/publish", doc)["digest"]
            observed["definition_digest"] = digest
            tasks = {}
            for name, director in (("a", "director_a"), ("b", "director_b")):
                run_id = "supervisor-run-" + name
                task = send(urls[director], {"op": "start", "key": run_id + "-start",
                                             "run_id": run_id, "digest": digest})
                task_until(urls[director], task["id"], "input-required")
                tasks[name] = task["id"]
            observed["task_ids"] = tasks
            os.kill(first.pid, signal.SIGKILL)
            first.wait(timeout=10)
            observed["first_supervisor_exit"] = first.returncode
            observed["orphan_status_before_relaunch"] = {name: status(pid)
                for name, pid in old_children.items()}
            assert all(value and not value.startswith("Z")
                       for value in observed["orphan_status_before_relaunch"].values())
            second = subprocess.Popen([str(bundle / "run"), "--state", str(state)],
                                      cwd=bundle, stdout=log2, stderr=subprocess.STDOUT)
            recovered = ready_until(ready_path,
                lambda x: x.get("generation") == 2 and x.get("supervisor_pid") == second.pid
                          and len(x["children"]) == 5)
            observed["recovered"] = recovered
            assert recovered["ports"] == initial["ports"]
            assert all(recovered["children"][name]["pid"] != old_children[name]
                       for name in old_children)
            for name in ("capability", "quality", "director_a", "director_b"):
                before = initial["children"][name]["health"]
                after = recovered["children"][name]["health"]
                assert after["identity"] == before["identity"]
                assert after["incarnation"] == before["incarnation"] + 1
            observed["old_child_status_after_reclaim"] = {name: status(pid)
                for name, pid in old_children.items()}
            assert all(not value or value.startswith("Z")
                       for value in observed["old_child_status_after_reclaim"].values())
            for name, director in (("a", "director_a"), ("b", "director_b")):
                task = rpc(urls[director], "tasks/get", {"id": tasks[name]})
                assert task["status"]["state"] == "input-required"
                assert task["metadata"]["run_id"] == "supervisor-run-" + name
            observed["recovered_original_tasks"] = True
            for name, director in (("b", "director_b"), ("a", "director_a")):
                send(urls[director], {"op": "decide", "run_id": "supervisor-run-" + name,
                                      "digest": digest}, tasks[name])
            observed["completed"] = {name: task_until(urls[director], tasks[name], "completed")
                for name, director in (("a", "director_a"), ("b", "director_b"))}
            observed["ledgers"] = {name: request(urls["effect"], "/ledger",
                                                {"id": "supervisor-run-" + name})
                                   for name in ("a", "b")}
            assert all(row["delivery"] and row["outbox"]["state"] == "signaled"
                       for row in observed["ledgers"].values())
            observed["receipts"] = {name: {
                action: request(urls[endpoint], "/fixture/actions/supervisor-run-" + name + "-" + action,
                                token="fixture-token")
                for action, endpoint in (("assign", "capability"), ("review", "quality"))}
                for name in ("a", "b")}
            assert all(item["accepted_count"] == 1 for row in observed["receipts"].values()
                       for item in row.values())
            observed["passed"] = True
        except Exception as error:
            observed["passed"] = False
            observed["error"] = repr(error)
            raise
        finally:
            if second is not None and second.poll() is None:
                second.terminate()
                second.wait(timeout=20)
            if first is not None and first.poll() is None:
                first.terminate()
                first.wait(timeout=20)
            # If the relaunched supervisor failed, clean only the exact old
            # bundle children recorded before the fault.
            for name, pid in old_children.items():
                if status(pid) and str(bundle) in command(pid) and str(state) in command(pid):
                    os.kill(pid, signal.SIGTERM)
            observed["second_supervisor_exit"] = None if second is None else second.returncode
            (HERE / "supervisor-observed.json").write_text(json.dumps(observed, indent=2,
                                                                       sort_keys=True) + "\n")
            print(json.dumps({"passed": observed["passed"], "error": observed.get("error"),
                              "first_exit": observed.get("first_supervisor_exit"),
                              "recovered_ports": observed.get("recovered", {}).get("ports")}, indent=2))


if __name__ == "__main__":
    main()
