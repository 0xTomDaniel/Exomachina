"""Separate black-box probe for the long-running copied-bundle supervisor."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import time
import urllib.error
import urllib.request
from uuid import uuid4


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


def poll(check, description: str, seconds: float = 75) -> object:
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


def registry(rt: Path) -> dict:
    return json.loads((rt / "endpoints.json").read_text())


def run_status(dagu_url: str, version: str, run_id: str) -> dict:
    return http(f"{dagu_url}/api/v1/dag-runs/exo_decision_factory_{version}/{run_id}")["dagRunDetails"]


def waiting_at(dagu_url: str, version: str, run_id: str, node: str) -> bool:
    run = run_status(dagu_url, version, run_id)
    nodes = {entry["step"]["id"]: entry for entry in run["nodes"]}
    if run["statusLabel"] == "failed":
        raise AssertionError(f"Dagu {version} failed: {nodes}")
    return nodes[node]["statusLabel"] == "waiting"


def delivered(dagu_url: str, version: str, run_id: str) -> bool:
    run = run_status(dagu_url, version, run_id)
    nodes = {entry["step"]["id"]: entry for entry in run["nodes"]}
    if run["statusLabel"] == "failed":
        raise AssertionError(f"Dagu {version} failed: {nodes}")
    return run["statusLabel"] == "succeeded" and nodes["deliver"]["statusLabel"] == "succeeded"


def tree_rss_kib(root_pid: int) -> dict:
    lines = subprocess.check_output(["ps", "-A", "-o", "pid=,ppid=,rss="], text=True).splitlines()
    rows = []
    for line in lines:
        parts = line.split()
        if len(parts) == 3 and all(item.isdigit() for item in parts):
            rows.append(tuple(map(int, parts)))
    ours = {root_pid}
    while True:
        before = len(ours)
        ours.update(pid for pid, ppid, _ in rows if ppid in ours)
        if len(ours) == before:
            break
    matches = {pid: rss for pid, _, rss in rows if pid in ours}
    return {"process_count": len(matches), "rss_sum_kib": sum(matches.values()), "pids": matches}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("runtime", type=Path)
    args = parser.parse_args()
    rt = args.runtime.resolve()
    info = registry(rt)
    urls, run_ids, manifests = info["urls"], info["run_ids"], info["manifests"]
    dagu_url, a_url, b_url, cap_url = (urls[name] for name in
        ("dagu", "director_a", "director_b", "capability"))
    observations: dict[str, object] = {"bundle_root": info["bundle"], "runtime": str(rt),
        "ports": info["ports"], "run_ids": run_ids, "manifests": manifests,
        "supervisor_pid": info["supervisor_pid"]}
    relaunched = None
    try:
        observations["identities_initial"] = {name: http(urls[name] + "/health") for name in
            ("capability", "quality", "director_a", "director_b")}
        observations["rss_startup"] = tree_rss_kib(info["supervisor_pid"])
        v1 = send(a_url, {"op": "start", "key": "bundle:a:v1:start",
            "run_id": run_ids["v1"], "version": "v1",
            "closure_sha256": manifests["v1"]["closure_sha256"]})
        v2 = send(b_url, {"op": "start", "key": "bundle:b:v2:start",
            "run_id": run_ids["v2"], "version": "v2",
            "closure_sha256": manifests["v2"]["closure_sha256"]})
        for version in ("v1", "v2"):
            poll(lambda v=version: waiting_at(dagu_url, v, run_ids[v], "publication_gate"),
                 f"{version} publication wait")
        observations["rss_warm_waiting"] = tree_rss_kib(info["supervisor_pid"])
        os.kill(info["supervisor_pid"], signal.SIGKILL)
        observations["supervisor_killed_pid"] = info["supervisor_pid"]
        with (rt / "supervisor-restarted.log").open("ab") as output:
            relaunched = subprocess.Popen([str(Path(info["bundle"]) / "run"), str(rt)],
                                          stdout=output, stderr=output)
        def fully_restarted():
            state = registry(rt)
            if relaunched.poll() is not None or state["supervisor_pid"] != relaunched.pid:
                return False
            if any(state["starts"][name] < 2 for name in
                   ("dagu", "capability", "quality", "director_a", "director_b")):
                return False
            for name in ("dagu", "capability", "quality", "director_a", "director_b"):
                http(urls[name] + ("/api/v1/dags" if name == "dagu" else "/health"))
            return True
        poll(fully_restarted, "supervisor relaunch against same state")
        resumed = registry(rt)
        observations["supervisor_after_relaunch"] = resumed
        assert resumed["run_ids"] == run_ids and resumed["ports"] == info["ports"]
        for version in ("v1", "v2"):
            poll(lambda v=version: waiting_at(dagu_url, v, run_ids[v], "publication_gate"),
                 f"{version} wait after supervisor hard crash and relaunch")
        observations["tasks_after_supervisor_restart"] = {
            "v1": rpc(a_url, "tasks/get", {"id": v1["id"]}),
            "v2": rpc(b_url, "tasks/get", {"id": v2["id"]})}
        assert all(task["status"]["state"] == "input-required" for task in
                   observations["tasks_after_supervisor_restart"].values())
        observations["rss_after_supervisor_restart"] = tree_rss_kib(relaunched.pid)
        info = resumed
        os.kill(info["pids"]["dagu"], signal.SIGKILL)
        observations["dagu_killed_pid"] = info["pids"]["dagu"]
        poll(lambda: registry(rt)["starts"]["dagu"] >= 3 and http(dagu_url + "/api/v1/dags"),
             "supervisor auto-restarted Dagu")
        for version in ("v1", "v2"):
            poll(lambda v=version: waiting_at(dagu_url, v, run_ids[v], "publication_gate"),
                 f"{version} wait after Dagu automatic restart")
        observations["tasks_after_dagu_restart"] = {
            "v1": rpc(a_url, "tasks/get", {"id": v1["id"]}),
            "v2": rpc(b_url, "tasks/get", {"id": v2["id"]})}
        assert all(task["status"]["state"] == "input-required" for task in
                   observations["tasks_after_dagu_restart"].values())
        hold = rt / "hold-director_a"
        hold.write_text("test-only hold so B can finish while A is down\n")
        os.kill(info["pids"]["director_a"], signal.SIGKILL)
        observations["director_a_killed_pid"] = info["pids"]["director_a"]
        send(b_url, {"op": "decide", "key": "bundle:b:v2:publication",
            "run_id": run_ids["v2"], "gate": "publication_gate"})
        active_snapshots = []
        def active_check():
            active_snapshots.append(tree_rss_kib(relaunched.pid))
            return waiting_at(dagu_url, "v2", run_ids["v2"], "director")
        poll(active_check, "v2 Director gate")
        observations["rss_active_peak_observed"] = max(active_snapshots,
            key=lambda sample: sample["rss_sum_kib"])
        observations["active_samples"] = len(active_snapshots)
        send(b_url, {"op": "decide", "key": "bundle:b:v2:director",
            "run_id": run_ids["v2"], "gate": "director"})
        poll(lambda: delivered(dagu_url, "v2", run_ids["v2"]), "v2 delivery")
        observations["b_completed_while_a_down"] = rpc(b_url, "tasks/get", {"id": v2["id"]})
        assert registry(rt)["starts"]["director_a"] == 2
        hold.unlink()
        poll(lambda: registry(rt)["starts"]["director_a"] >= 3 and http(a_url + "/health"),
             "supervisor auto-restarted Director A")
        observations["director_a_restarted"] = http(a_url + "/health")
        observations["a_restored_task"] = rpc(a_url, "tasks/get", {"id": v1["id"]})
        send(a_url, {"op": "decide", "key": "bundle:a:v1:publication",
            "run_id": run_ids["v1"], "gate": "publication_gate"})
        poll(lambda: registry(rt)["starts"]["capability"] >= 3 and http(cap_url + "/health"),
             "supervisor auto-restarted capability after lost acknowledgement")
        poll(lambda: waiting_at(dagu_url, "v1", run_ids["v1"], "director"), "v1 Director gate")
        send(a_url, {"op": "decide", "key": "bundle:a:v1:director",
            "run_id": run_ids["v1"], "gate": "director"})
        poll(lambda: delivered(dagu_url, "v1", run_ids["v1"]), "v1 delivery")
        observations["a_completed_after_restart"] = rpc(a_url, "tasks/get", {"id": v1["id"]})
        observations["rss_final"] = tree_rss_kib(relaunched.pid)
        observations["supervisor_final"] = registry(rt)
        observations["runs"] = {v: run_status(dagu_url, v, run_ids[v]) for v in ("v1", "v2")}
        with sqlite3.connect(rt / "ledger.sqlite") as db:
            db.row_factory = sqlite3.Row
            observations["ledger"] = [dict(row) for row in db.execute("SELECT * FROM runs ORDER BY run_id")]
        assert observations["identities_initial"]["director_a"]["identity"] == observations["director_a_restarted"]["identity"]
        assert observations["director_a_restarted"]["incarnation"] == 3
        assert observations["a_restored_task"]["status"]["state"] == "input-required"
        assert observations["b_completed_while_a_down"]["status"]["state"] == "completed"
        assert observations["a_completed_after_restart"]["status"]["state"] == "completed"
        assert all(row["release_count"] == 1 for row in observations["ledger"])
        events = [json.loads(line) for line in (rt / "supervisor-events.jsonl").read_text().splitlines()]
        observations["supervisor_events"] = events
        for name in ("dagu", "director_a", "capability"):
            assert [e["count"] for e in events if e["kind"] == "started" and e["name"] == name] == [1, 2, 3]
        (rt / "bundle-result.json").write_text(json.dumps(observations, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"result": "passed", "evidence": str(rt / "bundle-result.json"),
                          "rss_final": observations["rss_final"]}, sort_keys=True))
    except Exception:
        (rt / "bundle-partial.json").write_text(json.dumps(observations, indent=2, sort_keys=True) + "\n")
        raise
    finally:
        (rt / "hold-director_a").unlink(missing_ok=True)
        if relaunched is not None and relaunched.poll() is None:
            relaunched.terminate()
            try:
                relaunched.wait(timeout=10)
            except subprocess.TimeoutExpired:
                relaunched.kill()
                relaunched.wait(timeout=5)


if __name__ == "__main__":
    main()
