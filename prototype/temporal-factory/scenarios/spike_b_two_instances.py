"""Pre-registered same-home, two-process factory trial (fixture Director)."""
from __future__ import annotations

import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import threading
import time
import urllib.error

from common import (PY, ROOT, SRC, a2a_get, a2a_send, http, jsonl, poll_task,
                    run_cli, sqlite_rows, start_harness, stop_process)


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def events(home: Path) -> list[dict]:
    return jsonl(home / "runner/runner-events.jsonl")


def health(port: int) -> dict:
    return http(f"http://127.0.0.1:{port}/health", token=False)


def card(port: int) -> dict:
    return http(f"http://127.0.0.1:{port}/.well-known/agent-card.json", token=False)


def release_rows(home: Path) -> list[dict]:
    return sqlite_rows(home / "services/release/release.sqlite3", "SELECT * FROM releases")


def run_rows(instance: Path) -> list[dict]:
    return sqlite_rows(instance / "director.sqlite3", "SELECT run_id, task_id, closed, outcome_json FROM runs")


async def child_status(address: str, run_id: str) -> dict:
    from temporalio.client import Client
    from factory import FactoryRun
    client = await Client.connect(address, namespace="exomachina")
    parent = await client.get_workflow_handle(run_id).query(FactoryRun.status)
    return await client.get_workflow_handle(parent["child_id"]).query(FactoryRun.status)


async def temporal_result(address: str, run_id: str) -> dict:
    from temporalio.client import Client
    client = await Client.connect(address, namespace="exomachina")
    return await client.get_workflow_handle(run_id).result()


def recovered(port: int) -> dict:
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        value = health(port)
        if value["startup"]["recovery"] is not None:
            return value
        time.sleep(.4)
    raise TimeoutError("recovery did not report")


def provision(instance: Path, name: str, port: int, home: Path, wait: int = 900) -> dict:
    return json.loads(run_cli(str(SRC / "admin.py"), "provision", "--instance-dir", str(instance),
                              "--name", name, "--port", str(port), "--home", str(home),
                              "--testbed", str(home / "testbed"), "--wait-seconds", str(wait)))


def raw_provision(instance: Path, name: str, port: int, home: Path) -> dict:
    proc = subprocess.run([PY, "-B", str(SRC / "admin.py"), "provision", "--instance-dir", str(instance),
                           "--name", name, "--port", str(port), "--home", str(home),
                           "--testbed", str(home / "testbed")], capture_output=True, text=True)
    return {"returncode": proc.returncode, "stdout": proc.stdout[-1200:], "stderr": proc.stderr[-1200:]}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--home", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    home = args.home
    if home.exists():
        raise FileExistsError("fresh home required")
    home.mkdir(parents=True)
    alpha_dir, beta_dir = home / "instances/alpha", home / "instances/beta"
    base = {"alpha": "http://127.0.0.1:44850", "beta": "http://127.0.0.1:44851"}
    processes: dict[str, subprocess.Popen] = {}
    evidence: dict = {"claim_type": "observed-real", "director_model": "fixture",
                      "home": str(home), "started_at": time.time(), "checks": {}, "setup": {}}

    def save() -> None:
        args.evidence.parent.mkdir(parents=True, exist_ok=True)
        args.evidence.write_text(json.dumps(evidence, indent=2, sort_keys=True, default=str) + "\n")

    def trial(name: str, function) -> None:
        row: dict = {"claim_type": "observed-real", "director_model": "fixture"}
        evidence["checks"][name] = row
        try:
            function(row)
            row["verdict"] = "pass"
        except Exception as error:
            row["verdict"] = "fail"
            row["error"] = f"{type(error).__name__}: {error}"
        save()

    try:
        evidence["setup"]["testbed"] = json.loads(run_cli(str(ROOT / "services/testbed.py"), "up",
                                                          "--home", str(home), "--port-base", "45500"))
        evidence["setup"]["alpha_config"] = provision(alpha_dir, "alpha", 44850, home, wait=1800)
        evidence["setup"]["beta_config"] = provision(beta_dir, "beta", 44851, home, wait=1800)
        for instance in (alpha_dir, beta_dir):
            evidence["setup"][instance.name + "_publication"] = json.loads(run_cli(
                str(SRC / "admin.py"), "publish-template", "--instance-dir", str(instance),
                "--template", str(ROOT / "definitions/v1-template.json"), "--label", "v1"))
        evidence["setup"]["runner_before"] = json.loads(run_cli(str(SRC / "runner.py"), "status",
                                                                     "--home", str(home)))
        for name, instance, port in (("alpha", alpha_dir, 44850), ("beta", beta_dir, 44851)):
            processes[name] = start_harness(instance, port)
        evidence["setup"]["health_before"] = {n: health(p) for n, p in (("alpha", 44850), ("beta", 44851))}
        evidence["setup"]["pgdata_before"] = (home / "runner/pgdata").exists()
        save()

        def b1(row):
            pre = evidence["setup"]
            check(not pre["runner_before"]["running"] and not pre["pgdata_before"], "runner was not cold")
            check(all(not h["runner_running"] for h in pre["health_before"].values()), "runner started with harness")
            barrier = threading.Barrier(3)
            def send(name):
                barrier.wait()
                return a2a_send(base[name], {"op": "start", "action_id": "B1:" + name,
                                             "inputs": {"question": "B1 " + name,
                                                        "outcome_mode": "after_first_repair"}})
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = {name: pool.submit(send, name) for name in base}
                barrier.wait()
                row["sent"] = {name: future.result(timeout=240) for name, future in futures.items()}
            row["tasks"] = {name: poll_task(base[name], row["sent"][name]["id"],
                                            {"completed", "failed"}, seconds=300) for name in base}
            row["events"] = events(home)
            row["releases"] = release_rows(home)
            row["runner_status"] = json.loads(run_cli(str(SRC / "runner.py"), "status", "--home", str(home)))
            kinds = [e["kind"] for e in row["events"]]
            check(kinds.count("serve-ready") == 1 and kinds.count("start") == 1 and kinds.count("attach") >= 1,
                  "one serve-ready/start plus attach required")
            check(len({e["pid"] for e in row["events"] if e["kind"] == "serve-ready"}) == 1,
                  "multiple serve PIDs")
            for name in base:
                task = row["tasks"][name]
                identity = pre["health_before"][name]["identity"]
                check(task["status"]["state"] == "completed", f"{name} did not complete")
                check(task["metadata"]["run_id"].startswith(identity + "."), f"{name} run identity")
                artifact = task["artifacts"][0]["parts"][0]["data"]
                check(artifact["report"] and artifact["release_receipt"], f"{name} missing artifact/receipt")
                check(artifact["release_receipt"]["run_id"].startswith(task["metadata"]["run_id"] + ":child:"),
                      "receipt belongs to another parent run")

        trial("B-1", b1)

        def b2(row):
            b1data = evidence["checks"]["B-1"]
            check("tasks" in b1data, "B-1 tasks unavailable")
            row["cross_get"] = {}
            for own, other in (("alpha", "beta"), ("beta", "alpha")):
                try:
                    row["cross_get"][own] = a2a_get(base[other], b1data["tasks"][own]["id"])
                except RuntimeError as error:
                    row["cross_get"][own] = str(error)
            row["cards"] = {n: card(p) for n, p in (("alpha", 44850), ("beta", 44851))}
            row["director_runs"] = {n: run_rows(d) for n, d in (("alpha", alpha_dir), ("beta", beta_dir))}
            row["releases"] = release_rows(home)
            row["outcome_journal"] = sqlite_rows(home / "runner/outcomes.sqlite3",
                                                  "SELECT action_id, value FROM outcomes")
            identities = {n: evidence["setup"]["health_before"][n]["identity"] for n in base}
            for own, other in (("alpha", "beta"), ("beta", "alpha")):
                check(isinstance(row["cross_get"][own], str) and "not found" in row["cross_get"][own].lower(),
                      f"{own} Task visible to {other}")
                check(row["cards"][own]["name"] == own and str(44850 if own == "alpha" else 44851) in row["cards"][own]["url"],
                      "Agent Card mismatch")
                check(all(r["run_id"].startswith(identities[own] + ".") for r in row["director_runs"][own]),
                      "director run contamination")
            known = tuple(i + "." for i in identities.values())
            check(all(r["run_id"].startswith(known) for r in row["releases"]), "release contamination")
            check(all(json.loads(r["value"]).get("run_id", "").startswith(known)
                      for r in row["outcome_journal"]), "outcome contamination")
            check(all(r["release_id"].startswith(r["run_id"] + ":") for r in row["releases"]),
                  "release id crosses runs")
            check(all(r["action_id"].startswith(json.loads(r["value"])["run_id"] + ":")
                      for r in row["outcome_journal"]), "journal action id crosses runs")

        trial("B-2", b2)

        def b3(row):
            waiting = a2a_send(base["alpha"], {"op": "start", "action_id": "B3:alpha:wait",
                                                     "inputs": {"question": "B3 wait", "outcome_mode": "never"}})
            row["alpha_waiting"] = poll_task(base["alpha"], waiting["id"], {"input-required", "failed"}, seconds=300)
            check(row["alpha_waiting"]["status"]["state"] == "input-required", "alpha did not park")
            row["alpha_sent"] = waiting
            row["beta_sent"] = a2a_send(base["beta"], {"op": "start", "action_id": "B3:beta",
                                                                 "inputs": {"question": "B3 beta",
                                                                            "outcome_mode": "after_first_repair"}})
            row["beta_at_stop"] = a2a_get(base["beta"], row["beta_sent"]["id"])["status"]["state"]
            row["runner_before"] = json.loads(run_cli(str(SRC / "runner.py"), "status", "--home", str(home)))
            row["events_before"] = len(events(home))
            row["alpha_exit"] = stop_process(processes["alpha"])
            row["beta_final"] = poll_task(base["beta"], row["beta_sent"]["id"], {"completed", "failed"}, seconds=300)
            row["runner_after"] = json.loads(run_cli(str(SRC / "runner.py"), "status", "--home", str(home)))
            row["new_events"] = events(home)[row["events_before"]:]
            check(row["beta_at_stop"] == "working", "beta was not in flight at alpha shutdown")
            check(row["beta_final"]["status"]["state"] == "completed", "beta failed")
            check(row["runner_before"]["pid"] == row["runner_after"]["pid"], "runner PID changed")
            check(not any(e["kind"] in {"serve-stop", "serve-ready", "stop"} for e in row["new_events"]),
                  "runner restarted/stopped")

        trial("B-3", b3)

        def b4(row):
            waiting = evidence["checks"]["B-3"].get("alpha_sent")
            check(waiting is not None, "B-3 wait unavailable")
            previous = evidence["setup"]["health_before"]["alpha"]
            row["events_before"] = len(events(home))
            processes["alpha"] = start_harness(alpha_dir, 44850)
            row["health"] = recovered(44850)
            row["task_before_abort"] = a2a_get(base["alpha"], waiting["id"])
            address = json.loads((home / "runner/runner-ready.json").read_text())["address"]
            status = asyncio.run(child_status(address, row["task_before_abort"]["metadata"]["run_id"]))
            row["child_status"] = status
            row["abort"] = a2a_send(base["alpha"], {"op": "abort", "action_id": "B4:abort",
                                                      "revision": status["current_revision"],
                                                      "sha256": status["current_sha256"]},
                                       task_id=waiting["id"], context_id=waiting["contextId"])
            row["final"] = poll_task(base["alpha"], waiting["id"], {"completed", "failed"}, seconds=180)
            row["releases_for_run"] = [r for r in release_rows(home) if r["run_id"] == row["final"]["metadata"]["run_id"]]
            row["new_events"] = events(home)[row["events_before"]:]
            check(row["health"]["identity"] == previous["identity"] and row["health"]["incarnation"] == previous["incarnation"] + 1,
                  "identity/incarnation mismatch")
            check(row["health"]["startup"]["recovery"] and
                  any(e["kind"] == "attach" and e.get("reason", "").startswith("recover-unfinished")
                      for e in row["new_events"]) and
                  not any(e["kind"] == "serve-ready" for e in row["new_events"]),
                  "recovery did not attach")
            check(row["task_before_abort"]["status"]["state"] == "input-required", "parked Task lost")
            check(row["final"]["status"]["state"] == "completed" and
                  row["final"]["artifacts"][0]["parts"][0]["data"]["status"] == "aborted", "abort failed")
            check(not row["releases_for_run"], "aborted run released")

        trial("B-4", b4)

        def b5(row):
            row["sent"] = a2a_send(base["alpha"], {"op": "start", "action_id": "B5:active",
                                                         "inputs": {"question": "B5 active",
                                                                    "outcome_mode": "after_first_repair"}})
            row["task_at_kill"] = a2a_get(base["alpha"], row["sent"]["id"])
            check(row["task_at_kill"]["status"]["state"] == "working", "run not active at kill")
            row["run_id"] = row["task_at_kill"]["metadata"]["run_id"]
            previous = health(44850)
            os.kill(processes["alpha"].pid, signal.SIGKILL)
            processes["alpha"].wait(timeout=20)
            row["kill_exit"] = processes["alpha"].returncode
            address = json.loads((home / "runner/runner-ready.json").read_text())["address"]
            row["temporal_result_while_down"] = asyncio.run(temporal_result(address, row["run_id"]))
            row["events_before_restart"] = len(events(home))
            processes["alpha"] = start_harness(alpha_dir, 44850)
            row["health_after_restart"] = recovered(44850)
            row["final"] = poll_task(base["alpha"], row["sent"]["id"], {"completed", "failed"}, seconds=180)
            row["runs"] = [r for r in run_rows(alpha_dir) if r["run_id"] == row["run_id"]]
            row["releases"] = [r for r in release_rows(home) if r["run_id"].startswith(row["run_id"] + ":child:")]
            check(row["health_after_restart"]["identity"] == previous["identity"] and
                  row["health_after_restart"]["incarnation"] == previous["incarnation"] + 1,
                  "hard-kill restart identity/incarnation")
            check(row["final"]["status"]["state"] == "completed" and len(row["runs"]) == 1 and len(row["releases"]) == 1,
                  "lost or duplicated result")
            check(row["final"]["metadata"]["run_id"] == row["run_id"], "different projected run")

        trial("B-5", b5)

        def b6(row):
            from harness import init_instance
            row["runner_config_file_before"] = (home / "runner-config.json").exists()
            row["runner_pid_before"] = json.loads(run_cli(str(SRC / "runner.py"), "status", "--home", str(home)))["pid"]
            row["a"] = raw_provision(home / "instances/gamma", "gamma", 44850, home)
            try:
                init_instance(home / "instances/delta", name="delta", mode="factory", port=44852,
                              home=home, runner={"port_base": 44320, "member_base": 32470})
                row["b"] = {"accepted": True}
            except Exception as error:
                row["b"] = {"accepted": False, "error": f"{type(error).__name__}: {error}"}
            row["c"] = raw_provision(alpha_dir, "alpha", 44853, home)
            with (beta_dir / "duplicate-harness.log").open("a") as log:
                duplicate = subprocess.Popen([PY, "-B", str(SRC / "harness.py"), "serve",
                                              "--instance-dir", str(beta_dir)], stdout=log, stderr=log,
                                             start_new_session=True)
            try:
                duplicate.wait(timeout=20)
            except subprocess.TimeoutExpired:
                duplicate.terminate()
                duplicate.wait(timeout=10)
            row["d"] = {"exit": duplicate.returncode,
                        "log": (beta_dir / "duplicate-harness.log").read_text()[-1500:],
                        "live_health": health(44851),
                        "identity_db": sqlite_rows(beta_dir / "director.sqlite3",
                                                    "SELECT id, incarnation FROM identity")}
            row["runner_pid_after"] = json.loads(run_cli(str(SRC / "runner.py"), "status", "--home", str(home)))["pid"]
            row["runner_config_file_after"] = (home / "runner-config.json").exists()
            check(row["a"]["returncode"] != 0 and "port" in row["a"]["stderr"].lower(), "duplicate harness port accepted")
            check(not row["b"]["accepted"] and "runner" in row["b"]["error"].lower(), "different runner config accepted")
            check(row["c"]["returncode"] != 0 and "different" in row["c"]["stderr"].lower(), "changed instance accepted")
            check(row["d"]["exit"] != 0 and row["d"]["identity_db"][0]["incarnation"] == row["d"]["live_health"]["incarnation"],
                  "duplicate process fenced live beta")
            check(row["runner_pid_before"] == row["runner_pid_after"], "peer runner disturbed")
            check(row["runner_config_file_after"], "runner config does not belong to home")

        trial("B-6", b6)
    except Exception as error:
        evidence["setup_error"] = f"{type(error).__name__}: {error}"
        save()
    finally:
        evidence["cleanup"] = {}
        for name, process in processes.items():
            try:
                evidence["cleanup"][name] = stop_process(process)
            except Exception as error:
                evidence["cleanup"][name] = f"{type(error).__name__}: {error}"
        try:
            evidence["cleanup"]["runner"] = json.loads(run_cli(str(SRC / "runner.py"), "stop", "--home", str(home)))
        except Exception as error:
            evidence["cleanup"]["runner"] = f"{type(error).__name__}: {error}"
        try:
            evidence["cleanup"]["testbed"] = json.loads(run_cli(str(ROOT / "services/testbed.py"), "down",
                                                                 "--home", str(home), "--port-base", "45500"))
        except Exception as error:
            evidence["cleanup"]["testbed"] = f"{type(error).__name__}: {error}"
        evidence["finished_at"] = time.time()
        save()
    print(json.dumps({"evidence": str(args.evidence), "checks": {k: v["verdict"] for k, v in evidence["checks"].items()},
                      "setup_error": evidence.get("setup_error")}))


if __name__ == "__main__":
    main()
