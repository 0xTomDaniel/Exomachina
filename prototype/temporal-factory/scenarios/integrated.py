"""Integrated happy paths for the factory-mode harness prototype.

Path 1: a cold factory-mode harness receives work on its normal A2A identity,
lazily starts the local runner, invokes independent A2A services in parallel,
completes review/repair and exact-revision acceptance, and returns one
result/receipt on the original Task.

Path 2: a Strands authoring agent produces and revises a new graph from
validation feedback; it is approved, published as v2 and run through the same
factory instance, while a waiting v1 run keeps its original pinned bindings
(including across a graceful harness + runner restart that recovers it).
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import time
from pathlib import Path

from common import (PY, ROOT, SRC, a2a_get, a2a_send, http, jsonl, poll_task, run_cli,
                    sqlite_rows, start_harness, stop_process, wait_http, write_evidence)

PORT = 44800
TESTBED_PORT_BASE = 45200


def card(port: int) -> dict:
    return http(f"http://127.0.0.1:{port}/.well-known/agent-card.json", token=False)


def health(port: int) -> dict:
    return http(f"http://127.0.0.1:{port}/health", token=False)


async def histories(address: str, workflow_ids: list[str]) -> dict:
    from temporalio.client import Client
    client = await Client.connect(address, namespace="exomachina")
    out = {}
    for workflow_id in workflow_ids:
        handle = client.get_workflow_handle(workflow_id)
        description = await handle.describe()
        history = await handle.fetch_history()
        events = json.loads(history.to_json())["events"]
        scheduled = [e for e in events if e["eventType"] == "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED"]
        raw = description.raw_description.workflow_execution_info
        versioning = raw.versioning_info
        out[workflow_id] = {
            "status": description.status.name,
            "event_count": len(events),
            "activities": [(e["activityTaskScheduledEventAttributes"]["activityType"]["name"],
                            e["activityTaskScheduledEventAttributes"]["workflowTaskCompletedEventId"])
                           for e in scheduled],
            "versioning": {"behavior": int(versioning.behavior),
                           "deployment_version": {
                               "deployment": versioning.deployment_version.deployment_name,
                               "build_id": versioning.deployment_version.build_id},
                           "override_pinned": {
                               "deployment": versioning.versioning_override.pinned.version.deployment_name,
                               "build_id": versioning.versioning_override.pinned.version.build_id}},
        }
    return out


def parallel_assignments(history: dict) -> dict:
    assigns = [batch for name, batch in history["activities"] if name == "assign"]
    return {"assign_count": len(assigns),
            "scheduled_in_one_workflow_task": len(set(assigns)) == 1 and len(assigns) > 1,
            "workflow_task_completed_event_ids": sorted(set(assigns))}


def overlapping_remote_assignments(activity_log: list[dict], run_id: str) -> dict:
    rows = [row for row in activity_log if row.get("run", "").startswith(run_id)]
    starts = sorted(r["wall_time"] for r in rows if r["kind"] == "assign-start")
    ends = sorted(r["wall_time"] for r in rows if r["kind"] == "assign-complete")
    return {"assign_starts": len(starts), "assign_completes": len(ends),
            "all_started_before_first_completed": bool(starts and ends and starts[-1] <= ends[0])}


def service_actions(home: Path, name: str, run_prefix: str) -> list[dict]:
    path = home / "services" / name / "harness.sqlite3"
    return [r for r in sqlite_rows(path, "SELECT action_id, run_id, task_id, attempts, artifact FROM actions")
            if r["run_id"].startswith(run_prefix)]


def releases(home: Path, run_prefix: str) -> list[dict]:
    path = home / "services" / "release" / "release.sqlite3"
    return [r for r in sqlite_rows(path, "SELECT * FROM releases") if r["run_id"].startswith(run_prefix)]


def journal(home: Path, run_prefix: str) -> list[dict]:
    rows = sqlite_rows(home / "runner" / "outcomes.sqlite3", "SELECT action_id, value FROM outcomes")
    out = []
    for row in rows:
        value = json.loads(row["value"])
        if str(value.get("run_id", "")).startswith(run_prefix):
            out.append({"action_id": row["action_id"], **{k: value.get(k) for k in
                        ("run_id", "phase", "receiver", "effect_kind", "revision", "reason")}})
    return out


def runner_events(home: Path) -> list[dict]:
    return jsonl(home / "runner" / "runner-events.jsonl")


def publications(instance: Path) -> dict:
    return json.loads(run_cli(str(SRC / "admin.py"), "publications", "--instance-dir", str(instance)))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--home", type=Path, required=True,
                        help="fresh /tmp/exo-proto-int-* install root")
    args = parser.parse_args()
    home: Path = args.home
    if home.exists():
        raise FileExistsError("use a fresh install root")
    home.mkdir(parents=True)
    instance = home / "instances" / "research-factory"
    evidence: dict = {"home": str(home), "python": PY, "started_at": time.time()}
    base = f"http://127.0.0.1:{PORT}"

    # ---- Pinned independent A2A services (stand-in for the future directory).
    evidence["testbed_up"] = json.loads(run_cli(str(ROOT / "services" / "testbed.py"), "up",
                                                "--home", str(home), "--port-base", str(TESTBED_PORT_BASE)))
    run_cli(str(SRC / "admin.py"), "provision", "--instance-dir", str(instance),
            "--name", "research-factory", "--port", str(PORT), "--home", str(home),
            "--testbed", str(home / "testbed"))
    evidence["v1_publication"] = json.loads(run_cli(
        str(SRC / "admin.py"), "publish-template", "--instance-dir", str(instance),
        "--template", str(ROOT / "definitions" / "v1-template.json"), "--label", "v1"))
    evidence["cold_before_harness"] = {
        "runner_dir_has_pgdata": (home / "runner" / "pgdata").exists(),
        "runner_ready_file": (home / "runner" / "runner-ready.json").exists(),
        "runner_events": runner_events(home)}

    harness = start_harness(instance, PORT)
    try:
        # ---------------- Path 1 ----------------
        p1: dict = {}
        p1["health_at_start"] = health(PORT)
        p1["agent_card"] = card(PORT)
        p1["runner_running_before_work"] = p1["health_at_start"]["runner_running"]
        sent = a2a_send(base, {"op": "start", "action_id": "caller-1:research-1",
                               "inputs": {"question": "Can the customer use the documented capability?",
                                          "outcome_mode": "after_first_repair"}})
        task_id, context_id = sent["id"], sent["contextId"]
        p1["send_reply_state"] = sent["status"]["state"]
        final = poll_task(base, task_id, {"completed", "failed"})
        p1["task"] = final
        run_id = final["metadata"]["run_id"]
        p1["run_id"] = run_id
        p1["runner_events"] = runner_events(home)
        address = json.loads((home / "runner" / "runner-ready.json").read_text())["address"]
        child_id = f"{run_id}:child:"
        hist = asyncio.run(histories(address, [run_id]))
        status_child = [w for w in [asyncio.run(_child_id(address, run_id))] if w]
        hist.update(asyncio.run(histories(address, status_child)))
        p1["histories"] = hist
        p1["parallel"] = parallel_assignments(hist[status_child[0]])
        p1["parallel_remote_overlap"] = overlapping_remote_assignments(
            jsonl(home / "runner" / "activities.jsonl"), run_id)
        p1["quality_actions"] = service_actions(home, "quality", run_id)
        p1["capability_actions"] = {name: service_actions(home, name, run_id) for name in
                                    ("source_alpha", "source_beta", "counter_alpha", "counter_beta")}
        p1["releases"] = releases(home, run_id)
        p1["outcome_journal"] = journal(home, run_id)
        p1["refetch_same_task"] = a2a_get(base, task_id)
        evidence["path1"] = p1

        # Duplicate start of an existing action on a new Task must not alias it.
        try:
            dup = a2a_send(base, {"op": "start", "action_id": "caller-1:research-1",
                                  "inputs": {"question": "Can the customer use the documented capability?",
                                             "outcome_mode": "after_first_repair"}})
            p1["duplicate_start_on_new_task"] = dup
        except RuntimeError as error:
            p1["duplicate_start_on_new_task"] = {"a2a_error": str(error)}
        evidence["path1"] = p1

        # ---------------- Path 2 ----------------
        p2: dict = {"harness_pid": harness.pid}
        waiting = a2a_send(base, {"op": "start", "action_id": "caller-2:needs-director",
                                  "inputs": {"question": "What limits the same customer use?",
                                             "outcome_mode": "never"}})
        p2["v1_send_reply_state"] = waiting["status"]["state"]
        v1_task = poll_task(base, waiting["id"], {"input-required", "failed", "completed"})
        p2["v1_waiting_task_before"] = v1_task
        v1_run = v1_task["metadata"]["run_id"]
        p2["health_before_authoring"] = health(PORT)

        # A synthetic interpreter release B2 (one added result field) stands in
        # for a code upgrade; v2 is published on B2 while v1 stays on B1.
        p2["interpreter_b2"] = make_b2_source(home / "interpreter-b2")
        authored = json.loads(run_cli(
            str(SRC / "admin.py"), "author", "--instance-dir", str(instance),
            "--brief", str(ROOT / "definitions" / "authoring-brief-v2.md"), "--label", "v2",
            "--base-template", str(ROOT / "definitions" / "v1-template.json"),
            "--interpreter-source", str(home / "interpreter-b2"),
            "--allow-scripted", timeout=600))
        p2["authoring"] = authored
        p2["publications_after_v2"] = publications(instance)

        # v2 runs through the same, still-running harness process.
        fresh = a2a_send(base, {"op": "start", "action_id": "caller-3:research-v2",
                                "inputs": {"question": "Is the documented capability usable now?"}})
        v2_task = poll_task(base, fresh["id"], {"completed", "failed"})
        p2["v2_task"] = v2_task
        v2_run = v2_task["metadata"]["run_id"]
        p2["health_after_v2"] = health(PORT)
        p2["same_harness_process_for_v2"] = harness.poll() is None and harness.pid == p2["harness_pid"]
        p2["v1_waiting_task_after_v2"] = a2a_get(base, waiting["id"])
        p2["runner_status_two_builds"] = json.loads(run_cli(str(SRC / "runner.py"), "status",
                                                            "--home", str(home)))

        # Graceful restart of harness and runner while v1 waits: recovery must
        # start the runner at harness startup and keep v1's pinned closure.
        stop_process(harness)
        p2["runner_stop"] = json.loads(run_cli(str(SRC / "runner.py"), "stop", "--home", str(home)))
        harness = start_harness(instance, PORT)
        p2["health_after_restart"] = _wait_recovered(PORT)
        p2["v1_waiting_task_after_restart"] = poll_task(
            base, waiting["id"], {"input-required", "failed", "completed"})

        # The v1 Director wait is answered on its original Task.
        address = json.loads((home / "runner" / "runner-ready.json").read_text())["address"]
        wait_status = asyncio.run(_child_status(address, v1_run))
        p2["v1_child_status_at_wait"] = wait_status
        abort = a2a_send(base, {"op": "abort", "action_id": "operator:abort-v1-wait",
                                "revision": wait_status["current_revision"],
                                "sha256": wait_status["current_sha256"]},
                         task_id=waiting["id"], context_id=waiting["contextId"])
        p2["abort_reply"] = {"id": abort.get("id"), "state": abort.get("status", {}).get("state")}
        p2["v1_task_final"] = poll_task(base, waiting["id"], {"completed", "failed"})
        ids = [v1_run, v2_run]
        ids += [c for c in [asyncio.run(_child_id(address, r)) for r in ids] if c]
        p2["histories"] = asyncio.run(histories(address, ids))
        p2["releases"] = {"v1": releases(home, v1_run), "v2": releases(home, v2_run)}
        p2["quality_actions"] = {"v1": service_actions(home, "quality", v1_run),
                                 "v2": service_actions(home, "quality", v2_run)}
        p2["outcome_journal"] = {"v1": journal(home, v1_run), "v2": journal(home, v2_run)}
        p2["director_runs"] = sqlite_rows(instance / "director.sqlite3",
            "SELECT run_id, task_id, label, manifest_digest, package_digest, build_id, closed FROM runs")
        p2["runner_events"] = runner_events(home)
        evidence["path2"] = p2
    finally:
        evidence["harness_exit"] = stop_process(harness)
    # Graceful shutdown; all trial state is preserved for inspection and replay.
    evidence["runner_stop_final"] = json.loads(run_cli(str(SRC / "runner.py"), "stop", "--home", str(home)))
    evidence["testbed_down"] = json.loads(run_cli(str(ROOT / "services" / "testbed.py"), "down",
                                                  "--home", str(home), "--port-base", str(TESTBED_PORT_BASE)))
    evidence["finished_at"] = time.time()
    path = write_evidence("integrated-observed.json", evidence)
    print(json.dumps({"evidence": str(path)}))


def make_b2_source(target: Path) -> dict:
    """Copy the interpreter files and add one result field: a real, distinct build."""
    from binding import INTERPRETER_FILES, build_id_for, source_digest
    target.mkdir(parents=True)
    for name in INTERPRETER_FILES:
        (target / name).write_bytes((SRC / name).read_bytes())
    factory = target / "factory.py"
    text = factory.read_text()
    marker = '"acceptance": self.acceptance, "artifact": accepted,'
    if text.count(marker) != 1:
        raise RuntimeError("B2 patch marker missing")
    factory.write_text(text.replace(marker, marker + ' "interpreter_revision": "b2",'))
    digest = source_digest(target)
    return {"path": str(target), "source_digest": digest, "build_id": build_id_for(digest),
            "b1_build_id": build_id_for(source_digest(SRC)), "change": "complete result adds interpreter_revision"}


def _wait_recovered(port: int, seconds: float = 180) -> dict:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = health(port)
        if value["startup"]["recovery"] is not None:
            return value
        time.sleep(0.5)
    raise TimeoutError("harness did not report recovery")


async def _child_id(address: str, run_id: str) -> str | None:
    from temporalio.client import Client
    from factory import FactoryRun
    client = await Client.connect(address, namespace="exomachina")
    status = await client.get_workflow_handle(run_id).query(FactoryRun.status)
    return status.get("child_id")


async def _child_status(address: str, run_id: str) -> dict:
    from temporalio.client import Client
    from factory import FactoryRun
    client = await Client.connect(address, namespace="exomachina")
    child = await _child_id(address, run_id)
    return await client.get_workflow_handle(child).query(FactoryRun.status)


if __name__ == "__main__":
    main()
