"""Registered A-1..A-5 through harness A2A, Temporal and independent delayed A2A."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

from common import (PY, ROOT, SRC, a2a_get, a2a_send, http, jsonl, poll_task,
                    run_cli, sqlite_rows, start_harness, stop_process, wait_http)
from definition import digest as definition_digest

PORT = 44840
TESTBED = 45400
AGENT_PORTS = (45403, 45406, 45407, 45408)
EVIDENCE = ROOT / "evidence" / "spike-a" / "delayed.json"
CHILD_DIGEST = definition_digest(json.loads(
    (ROOT / "definitions" / "spike-a-template.json").read_text())["child"])


def action_id_for(parent_run: str) -> str:
    return f"{parent_run}:child:{CHILD_DIGEST[:12]}:counter_beta"


def save(value: dict) -> None:
    EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    EVIDENCE.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n")


def snapshot_url(home: Path, identity: str, url: str) -> None:
    path = home / "testbed" / "agent_snapshot.json"
    value = json.loads(path.read_text())
    value["agents"][identity] = {"url": url}
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def journal(home: Path, action_id: str) -> dict | None:
    path = home / "runner" / "outcomes.sqlite3"
    if not path.exists():
        return None
    try:
        rows = sqlite_rows(path, "SELECT action_id, value FROM outcomes")
    except sqlite3.OperationalError as error:
        if "no such table: outcomes" in str(error):
            return None
        raise
    for row in rows:
        if row["action_id"] == action_id:
            return {"action_id": action_id, **json.loads(row["value"])}
    return None


def wait_journal(home: Path, action_id: str, *, seconds: float = 120) -> dict:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = journal(home, action_id)
        if value and value.get("task_id"):
            return value
        time.sleep(0.1)
    raise TimeoutError(f"remote Task id was not journaled: {action_id}")


def wait_effect(home: Path, action_id: str, *, seconds: float = 120) -> dict:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        path = home / "services" / "counter_beta" / "delayed-agent.sqlite3"
        if path.exists():
            rows = sqlite_rows(path, "SELECT action_id, task_id FROM actions")
            for row in rows:
                if row["action_id"] == action_id:
                    return row
        time.sleep(0.1)
    raise TimeoutError(f"agent did not commit {action_id}")


def wait_unknown(home: Path, action_id: str, *, seconds: float = 30) -> dict:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        row = journal(home, action_id)
        if row and row["phase"] == "unknown" and row["task_id"] is None:
            return row
        time.sleep(0.1)
    raise TimeoutError(f"lost response did not become unknown without Task id: {action_id}")


def test_remote_task_id(home: Path, action_id: str) -> str:
    rows = sqlite_rows(home / "services" / "counter_beta" / "delayed-agent.sqlite3",
                       "SELECT action_id, task_id FROM actions")
    return next(row["task_id"] for row in rows if row["action_id"] == action_id)


def effects(url: str) -> dict:
    return http(url.rstrip("/") + "/_test/effects")


def release_rows(home: Path, run_id: str) -> list[dict]:
    path = home / "services" / "release" / "release.sqlite3"
    if not path.exists():
        return []
    return [row for row in sqlite_rows(path, "SELECT * FROM releases")
            if row["run_id"].startswith(run_id)]


async def workflows(address: str, run_id: str) -> dict:
    from temporalio.client import Client
    from factory import FactoryRun
    client = await Client.connect(address, namespace="exomachina")
    parent = client.get_workflow_handle(run_id)
    parent_status = await parent.query(FactoryRun.status)
    result = {"parent": {"phase": parent_status["phase"],
                          "execution": (await parent.describe()).status.name,
                          "incident": parent_status.get("incident"),
                          "unresolved": parent_status.get("unresolved")}}
    child_id = parent_status.get("child_id")
    if child_id:
        child = client.get_workflow_handle(child_id)
        child_status = await child.query(FactoryRun.status)
        result["child"] = {"id": child_id, "phase": child_status["phase"],
                           "execution": (await child.describe()).status.name,
                           "incident": child_status.get("incident"),
                           "unresolved": child_status.get("unresolved")}
    return result


def port_open(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            return True
    except OSError:
        return False


def stop_pid(pid: int, port: int) -> None:
    if not port_open(port):
        return
    try:
        os.killpg(pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        if not port_open(port):
            return
        raise
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and port_open(port):
        time.sleep(0.2)
    if port_open(port):
        raise TimeoutError(f"agent did not stop on {port}")


def start_agent(state: Path, port: int, *, identity_file: Path | None = None):
    state.mkdir(parents=True, exist_ok=True)
    command = [PY, "-B", str(ROOT / "services" / "delayed_agent.py"),
               "--state", str(state), "--port", str(port), "--delay-seconds", "15"]
    if identity_file:
        command.extend(["--identity-file", str(identity_file)])
    log = (state / f"agent-{port}.log").open("a")
    process = subprocess.Popen(command, stdout=log, stderr=log, start_new_session=True)
    log.close()
    wait_http(f"http://127.0.0.1:{port}/health")
    return process


def capture(home: Path, base: str, sent: dict, before: dict, agent_url: str) -> dict:
    final = poll_task(base, sent["id"], {"completed", "failed"}, seconds=300)
    run_id = sent["metadata"]["run_id"]
    action_id = action_id_for(run_id)
    ready = home / "runner" / "runner-ready.json"
    address = json.loads(ready.read_text())["address"]
    row = journal(home, action_id)
    logs = [value for value in jsonl(home / "runner" / "activities.jsonl")
            if value.get("action_id") == action_id]
    return {"run_id": run_id, "action_id": action_id,
            "caller_task_id": sent["id"], "caller_task": final,
            "caller_artifact_count": len(final.get("artifacts") or []),
            "remote_task_id": row.get("task_id") if row else None,
            "journal": row, "activity_events": logs,
            "effects_before": before, "effects_after": effects(agent_url),
            "workflows": asyncio.run(workflows(address, run_id)),
            "release_rows": release_rows(home, run_id)}


def started(base: str, label: str) -> dict:
    return a2a_send(base, {"op": "start", "action_id": f"spike-a:{label}",
                           "inputs": {"outcome_mode": "after_first_repair"}})


def verdict(name: str, record: dict, *, accepted: bool, identity: str,
            original_task: str | None = None, impostor: dict | None = None) -> dict:
    task = record["caller_task"]
    state = task["status"]["state"]
    journal_row = record["journal"] or {}
    remote_effect = record["effects_after"]["effects"].get(record["action_id"], 0)
    pin_logs = [x for x in record["activity_events"] if x["kind"] == "agent-card-verified"]
    conditions = {"effect_at_most_one": remote_effect <= 1,
                  "remote_task_journaled": bool(record["remote_task_id"]),
                  "pin_observed": bool(pin_logs),
                  "caller_state": state == ("completed" if accepted else "failed"),
                  "release_effect": bool(record["release_rows"]) == accepted,
                  "acceptance": (journal_row.get("phase") == "confirmed") == accepted,
                  "artifact_count": record["caller_artifact_count"] == (1 if accepted else 0)}
    if original_task is not None:
        conditions["same_remote_task"] = record["remote_task_id"] == original_task
    if impostor is not None:
        conditions["impostor_effects_zero"] = impostor["total"] == 0
    if accepted:
        receipt = journal_row.get("receipt") or {}
        artifact = receipt.get("artifact") or {}
        import hashlib
        conditions["accepted_artifact_binding"] = (
            artifact.get("author") == identity
            and artifact.get("action_id") == record["action_id"]
            and artifact.get("run_id") == journal_row.get("run_id")
            and artifact.get("definition_digest") == journal_row.get("definition_digest")
            and isinstance(artifact.get("content"), str)
            and artifact.get("sha256") == hashlib.sha256(artifact.get("content", "").encode()).hexdigest())
    record["conditions"] = conditions
    record["verdict"] = "pass" if all(conditions.values()) else "fail"
    return record


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--home", required=True, type=Path)
    args = parser.parse_args()
    home = args.home
    if home.exists():
        raise FileExistsError("use a fresh /tmp/exo-qual-a-* trial directory")
    home.mkdir(parents=True)
    os.environ["EXO_RUNNER_PORT_BASE"] = "44200"
    os.environ["EXO_RUNNER_MEMBER_BASE"] = "32440"
    base = f"http://127.0.0.1:{PORT}"
    result = {"claim": "observed-real", "director_model": "fixture", "home": str(home),
              "checks": {}, "started_at": time.time(), "commands": []}
    harness = None
    extra_agents = []
    active_agent_pid = None
    active_port = 45403
    try:
        result["testbed"] = json.loads(run_cli(str(ROOT / "services" / "testbed.py"),
            "up", "--home", str(home), "--port-base", str(TESTBED),
            "--delayed-agent", "--delay-seconds", "15"))
        active_agent_pid = result["testbed"]["pids"]["counter_beta"]["pid"]
        identity = result["testbed"]["bindings"]["counter_beta"]["identity"]
        result["pin"] = json.loads((home / "testbed" / "contracts.json").read_text())["counter_beta"]
        instance = home / "instances" / "research-factory"
        run_cli(str(SRC / "admin.py"), "provision", "--instance-dir", str(instance),
                "--name", "research-factory", "--port", str(PORT), "--home", str(home),
                "--testbed", str(home / "testbed"))
        result["publication"] = json.loads(run_cli(str(SRC / "admin.py"), "publish-template",
            "--instance-dir", str(instance), "--template",
            str(ROOT / "definitions" / "spike-a-template.json"), "--label", "spike-a"))
        harness = start_harness(instance, PORT)
        agent_url = f"http://127.0.0.1:{active_port}"
        before = effects(agent_url)
        sent = started(base, "async")
        first = wait_journal(home, action_id_for(sent["metadata"]["run_id"]))
        entry = capture(home, base, sent, before, agent_url)
        entry["first_journal"] = first
        entry = verdict("A-1", entry, accepted=True, identity=identity,
                        original_task=first["task_id"])
        entry["conditions"]["initial_working"] = any(x.get("state") == "working"
            for x in entry["activity_events"] if x["kind"] == "agent-task-journaled")
        entry["verdict"] = "pass" if all(entry["conditions"].values()) else "fail"
        result["checks"]["A-1"] = entry
        result["checks"]["A-2"] = {**entry, "verdict": "pass" if entry["conditions"]["pin_observed"]
            and entry["activity_events"][0]["kind"] == "agent-card-verified"
            and all(x.get("observed", {}).get("card_sha256") == result["pin"]["card_sha256"]
                    and x.get("observed", {}).get("extension_identity") == identity
                    and x.get("observed", {}).get("contract_sha256") == result["pin"]["a2a_extension"]["contract_digest"]
                    for x in entry["activity_events"] if x["kind"] == "agent-card-verified")
            else "fail"}
        save(result)

        before = effects(agent_url)
        sent = started(base, "move")
        first = wait_journal(home, action_id_for(sent["metadata"]["run_id"]))
        stop_pid(active_agent_pid, active_port)
        moved = start_agent(home / "services" / "counter_beta", 45406)
        extra_agents.append(moved)
        active_agent_pid, active_port = moved.pid, 45406
        agent_url = f"http://127.0.0.1:{active_port}"
        snapshot_url(home, identity, agent_url)
        entry = capture(home, base, sent, before, agent_url)
        entry["first_journal"] = first
        result["checks"]["A-3a"] = verdict("A-3a", entry, accepted=True,
            identity=identity, original_task=first["task_id"])
        conditions = result["checks"]["A-3a"]["conditions"]
        conditions["working_before_move"] = first["phase"] == "working"
        conditions["polled_on_new_url"] = any(
            event["kind"] == "agent-task-polled" and event.get("url") == agent_url
            and event.get("task_id") == first["task_id"]
            for event in entry["activity_events"])
        result["checks"]["A-3a"]["verdict"] = "pass" if all(conditions.values()) else "fail"
        save(result)

        before = effects(agent_url)
        sent = started(base, "impostor")
        first = wait_journal(home, action_id_for(sent["metadata"]["run_id"]))
        impostor = start_agent(home / "services" / "impostor", 45407)
        extra_agents.append(impostor)
        impostor_before = effects("http://127.0.0.1:45407")
        snapshot_url(home, identity, "http://127.0.0.1:45407")
        entry = capture(home, base, sent, before, agent_url)
        entry["first_journal"] = first
        entry["impostor_effects_before"] = impostor_before
        entry["impostor_effects_after"] = effects("http://127.0.0.1:45407")
        result["checks"]["A-3b"] = verdict("A-3b", entry, accepted=False,
            identity=identity, original_task=first["task_id"],
            impostor=entry["impostor_effects_after"])
        result["checks"]["A-3b"]["conditions"]["impostor_effects_before_zero"] = (
            impostor_before["total"] == 0)
        if not all(result["checks"]["A-3b"]["conditions"].values()):
            result["checks"]["A-3b"]["verdict"] = "fail"
        result["checks"]["A-3b"]["conditions"]["pin_incident"] = (
            entry["journal"]["reason"] == "pinned-agent-verification-failed")
        result["checks"]["A-3b"]["conditions"]["working_before_swap"] = (
            first["phase"] == "working")
        result["checks"]["A-3b"]["verdict"] = (
            "pass" if all(result["checks"]["A-3b"]["conditions"].values()) else "fail")
        snapshot_url(home, identity, agent_url)
        save(result)

        before = effects(agent_url)
        # The Director run id is a deterministic function of its caller action id.
        import hashlib
        director_identity = http(base + "/health", token=False)["identity"]
        run_id = f"{director_identity}.{hashlib.sha256('spike-a:lost'.encode()).hexdigest()[:20]}"
        action_id = action_id_for(run_id)
        http(agent_url + "/_test/faults", {"drop_response_once_for": action_id})
        sent = started(base, "lost")
        wait_effect(home, action_id)
        first = wait_unknown(home, action_id)
        original_task = test_remote_task_id(home, action_id)
        stop_process(harness)
        harness = None
        result["runner_stop_A4"] = json.loads(run_cli(str(SRC / "runner.py"), "stop", "--home", str(home)))
        stop_pid(active_agent_pid, active_port)
        restarted = start_agent(home / "services" / "counter_beta", 45408)
        extra_agents.append(restarted)
        active_agent_pid, active_port = restarted.pid, 45408
        agent_url = f"http://127.0.0.1:{active_port}"
        snapshot_url(home, identity, agent_url)
        harness = start_harness(instance, PORT)
        entry = capture(home, base, sent, before, agent_url)
        entry["first_journal"] = first
        result["checks"]["A-4"] = verdict("A-4", entry, accepted=True,
            identity=identity, original_task=original_task)
        conditions = result["checks"]["A-4"]["conditions"]
        conditions["unknown_without_task_before_restart"] = (
            first["phase"] == "unknown" and first["task_id"] is None)
        conditions["resend_original_task_after_restart"] = any(
            event["kind"] == "agent-task-journaled" and event.get("resend") is True
            and event.get("task_id") == original_task and event.get("url") == agent_url
            for event in entry["activity_events"])
        result["checks"]["A-4"]["verdict"] = "pass" if all(conditions.values()) else "fail"
        save(result)

        before = effects(agent_url)
        run_id = f"{director_identity}.{hashlib.sha256('spike-a:mismatch'.encode()).hexdigest()[:20]}"
        http(agent_url + "/_test/faults", {"mismatch_artifact_for": action_id_for(run_id)})
        sent = started(base, "mismatch")
        entry = capture(home, base, sent, before, agent_url)
        result["checks"]["A-5"] = verdict("A-5", entry, accepted=False, identity=identity)
        result["all_delayed_effects"] = effects(agent_url)
        result["global_invariants"] = {
            "each_action_effect_at_most_one": all(
                count <= 1 for count in result["all_delayed_effects"]["effects"].values()),
            "all_accepted_artifacts_bound_to_pin": all(
                check["conditions"].get("accepted_artifact_binding") is True
                for name, check in result["checks"].items()
                if name in {"A-1", "A-3a", "A-4"})}
        result["finished_at"] = time.time()
        save(result)
    except Exception as error:
        result["failure"] = {"type": type(error).__name__, "message": str(error),
                             "at": time.time()}
        save(result)
        raise
    finally:
        if harness is not None:
            result["harness_exit"] = stop_process(harness)
        try:
            result["runner_stop_final"] = json.loads(run_cli(str(SRC / "runner.py"),
                "stop", "--home", str(home)))
        except Exception as error:
            result["cleanup_runner_error"] = str(error)
        for process in extra_agents:
            if process.poll() is None:
                stop_process(process)
        try:
            result["testbed_down"] = json.loads(run_cli(str(ROOT / "services" / "testbed.py"),
                "down", "--home", str(home), "--port-base", str(TESTBED)))
        except Exception as error:
            result["cleanup_testbed_error"] = str(error)
        result["remaining_listeners"] = [port for port in [*range(45400, 45420),
            *range(44200, 44213), *range(32440, 32445), PORT, 44841]
            if port_open(port)]
        save(result)
    print(json.dumps({"evidence": str(EVIDENCE),
                      "verdicts": {key: value["verdict"] for key, value in result["checks"].items()}},
                     sort_keys=True))


if __name__ == "__main__":
    main()
