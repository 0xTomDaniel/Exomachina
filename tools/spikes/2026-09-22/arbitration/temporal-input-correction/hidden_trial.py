#!/usr/bin/env python3
"""Post-freeze A2 hidden composition driver; authored JSON only, frozen code unchanged."""
from __future__ import annotations

import asyncio
import json
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.request
from datetime import timedelta
from pathlib import Path

from temporalio.client import Client

import runtime
from author import materialize, template
from definition import digest, publish
from factory import FactoryRun
from fault_probe import until_phase
from probe import SERVICE_PORTS, bindings, director_send, director_task, input_for

HERE = Path(__file__).resolve().parent
FREEZE = HERE / "freeze.json"


def verify_freeze():
    result = subprocess.run([sys.executable, str(HERE.parent / "common" / "freeze.py"),
        "verify", str(FREEZE)], capture_output=True, text=True, check=True)
    return json.loads(result.stdout)


def supervisor_events(state):
    return [json.loads(line) for line in (state / "supervisor-events.jsonl").read_text().splitlines()]


def worker_pid(state):
    return [event["pid"] for event in supervisor_events(state)
            if event["kind"] in {"worker-start", "worker-restart"}][-1]


def rows(state, service, run):
    with sqlite3.connect(state / service / "harness.sqlite3") as db:
        db.row_factory = sqlite3.Row
        return [{**dict(row), "artifact": json.loads(row["artifact"])} for row in
                db.execute("SELECT action_id,run_id,definition_digest,task_id,artifact,attempts,accepted_count FROM actions WHERE run_id=?", (run,))]


def release_rows(state, run):
    with sqlite3.connect(state / "release" / "release.sqlite3") as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute("SELECT * FROM releases WHERE run_id=?", (run,))]


def evidence(state, parent_result, template_name):
    child = parent_result["child"]
    run = child["run"]
    source = rows(state, "source", run)
    counter = rows(state, "counter", run)
    quality = rows(state, "quality", run)
    times = [json.loads(line) for line in (state / "activities.jsonl").read_text().splitlines()
             if run in line]
    source_by_instance = {row["action_id"].rsplit(":", 1)[-1]: row for row in source}
    counter_by_instance = {row["action_id"].rsplit(":", 1)[-1]: row for row in counter}
    branch_receipts = {**source_by_instance, **counter_by_instance}
    declarations = {name: branch["result_type"] for name, branch in
                    template(template_name)["child"]["nodes"]["gather"]["branches"].items()}
    scopes = {name: branch["scope_status"] for name, branch in
              template(template_name)["child"]["nodes"]["gather"]["branches"].items()
              if branch["scope_status"] is not None}
    sys.path.insert(0, str(HERE.parent / "common"))
    import fixture
    join = fixture.typed_join(branch_receipts, run_id=run,
        definition_digest=child["definition_digest"], declarations=declarations,
        scope_status_by_instance=scopes)
    return {"run": run, "definition_digest": child["definition_digest"],
            "branch_receipts": branch_receipts, "join": join,
            "activity_times": times, "quality_actions": quality,
            "release_rows": release_rows(state, run),
            "public_child_result": child, "public_parent_result": parent_result}


async def ready(supervisor, state):
    async def check():
        if supervisor.poll() is not None:
            raise RuntimeError(f"supervisor exited {supervisor.returncode}")
        return (state / "supervisor-ready").exists()
    await runtime.until(check, seconds=90)


def launch_supervisor(state, suffix):
    log = (state / f"supervisor-{suffix}.log").open("a")
    process = subprocess.Popen([sys.executable, str(HERE / "supervisor.py"),
        "--state", str(state)], cwd=HERE, stdout=log, stderr=log,
        start_new_session=True)
    log.close()
    return process


async def start_direct(client, run, package, director, faults=None, outcome_mode=None):
    payload = input_for(run, package, director, faults)
    if outcome_mode is not None:
        payload["outcome_mode"] = outcome_mode
    return await client.start_workflow(FactoryRun.run,
        payload, id=run,
        task_queue="arbitration-temporal", execution_timeout=timedelta(minutes=10))


async def main():
    before_freeze = verify_freeze()
    state = Path(tempfile.mkdtemp(prefix="exo-temporal-hidden-a2-", dir="/tmp"))
    runtime.STATE = state
    observed = {"status": "running", "state": str(state),
                "freeze_before": before_freeze, "definitions": {}, "runs": {}}
    supervisor = launch_supervisor(state, "first")
    try:
        await ready(supervisor, state)
        client = await Client.connect(f"127.0.0.1:{runtime.PORTS['frontend']}",
                                      namespace="exomachina")
        identities = {}
        for name in ("source", "counter", "quality", "release"):
            with urllib.request.urlopen(f"http://127.0.0.1:{SERVICE_PORTS[name]}/health") as response:
                identities[name] = json.load(response)
        approved = bindings(identities)
        (state / "catalog" / "approved_bindings.json").write_text(json.dumps(approved, sort_keys=True))
        direct_authority = {"identity": "hidden-a2-director", "token": "fixture-hidden-token", "epoch": 1}

        old = materialize(template("visible-exhausted.json"), approved)
        old_digest = publish(old, state / "catalog", approved)
        old_handle = await start_direct(client, "hidden-old-v3-wait", old, direct_authority)
        old_wait, old_child = await until_phase(client, "hidden-old-v3-wait", "awaiting-director")
        observed["runs"]["old_wait_before"] = old_wait
        observed["definitions"]["old_v3"] = {"package_digest": old_digest,
            "child_digest": next(iter(old["children"]))}

        worker_before_publication = worker_pid(state)
        definitions = {}
        for name, filename in (("requires_success", "hidden-v4-requires-scope.json"),
                               ("requires_exhaust_separate", "hidden-v4-exhausted-separate-digest.json"),
                               ("clear", "hidden-v5-clear.json")):
            package = materialize(template(filename), approved)
            package_digest = publish(package, state / "catalog", approved)
            definitions[name] = package
            observed["definitions"][name] = {"native_file": str(HERE / "definitions" / filename),
                "package_digest": package_digest,
                "child_digest": next(iter(package["children"])),
                "catalog_file": str(state / "catalog" / f"{package_digest}.json")}
        observed["worker_pid_during_publication"] = worker_before_publication
        observed["worker_unchanged_during_publication"] = (
            worker_pid(state) == worker_before_publication)
        observed["runs"]["old_wait_after_publication"] = await old_child.query(FactoryRun.status)

        supervisor.send_signal(signal.SIGTERM)
        supervisor.wait(timeout=20)
        supervisor = launch_supervisor(state, "restart")
        await ready(supervisor, state)
        client = await Client.connect(f"127.0.0.1:{runtime.PORTS['frontend']}",
                                      namespace="exomachina")
        old_after, old_child = await until_phase(client, "hidden-old-v3-wait", "awaiting-director")
        observed["runs"]["old_wait_after_restart"] = old_after
        observed["worker_pid_after_normal_restart"] = worker_pid(state)

        # Retrospective correction: a typed run input selects whether the
        # same immutable V4 package resolves after one repair or exhausts.
        v4 = definitions["requires_success"]
        first = await start_direct(client, "hidden-v4-one-repair", v4, direct_authority,
            {"delay_assignment": {"counter_scope": 1.25}}, "after_first_repair")
        first_result = await asyncio.wait_for(first.result(), timeout=90)
        observed["runs"]["requires_success"] = evidence(state, first_result,
            "hidden-v4-requires-scope.json")
        second = await start_direct(client, "hidden-v4-same-digest-attempt-exhaust", v4,
                                    direct_authority, outcome_mode="never")
        second_wait, second_child = await until_phase(client,
            "hidden-v4-same-digest-attempt-exhaust", "awaiting-director")
        await second_child.execute_update(FactoryRun.director_command, {
            "command_id": "hidden-abort-same-digest", "action": "abort",
            "actor": direct_authority["identity"], "token": direct_authority["token"],
            "epoch": direct_authority["epoch"], "run": second_wait["run"],
            "definition_digest": second_wait["definition_digest"],
            "revision": second_wait["current_revision"],
            "sha256": second_wait["current_sha256"]})
        second_result = await asyncio.wait_for(second.result(), timeout=90)
        observed["runs"]["same_digest_expected_exhaustion"] = evidence(state, second_result,
            "hidden-v4-requires-scope.json")
        observed["runs"]["same_digest_expected_exhaustion"]["director_wait"] = second_wait
        observed["same_digest_divergent_outcomes_met"] = (
            first_result["status"] == "accepted" and second_result["status"] == "aborted")

        # A different authored digest can express the exhausted path. This is
        # evidence of block flexibility, not a substitute for the same-version gate.
        v4x_digest = observed["definitions"]["requires_exhaust_separate"]["package_digest"]
        task = await asyncio.to_thread(director_send, {"op": "start",
            "action_id": "hidden-start-exhaust", "run_id": "hidden-v4-exhaust-separate",
            "package_digest": v4x_digest})
        exhausted_wait, exhausted_child = await until_phase(client,
            "hidden-v4-exhaust-separate", "awaiting-director")
        abort_task = await asyncio.to_thread(director_send, {"op": "abort",
            "action_id": "hidden-abort-exhaust", "run_id": "hidden-v4-exhaust-separate",
            "revision": exhausted_wait["current_revision"],
            "sha256": exhausted_wait["current_sha256"]})
        exhausted_result = await client.get_workflow_handle("hidden-v4-exhaust-separate").result()
        observed["runs"]["requires_exhaust_separate"] = evidence(state,
            exhausted_result, "hidden-v4-exhausted-separate-digest.json")
        observed["runs"]["requires_exhaust_separate"]["director"] = {
            "original_task_id": task["id"], "abort_task": abort_task,
            "wait": exhausted_wait,
            "original_task_after": await asyncio.to_thread(director_task, task["id"])}

        v5_digest = observed["definitions"]["clear"]["package_digest"]
        clear_task = await asyncio.to_thread(director_send, {"op": "start",
            "action_id": "hidden-start-clear", "run_id": "hidden-v5-clear",
            "package_digest": v5_digest})
        clear_result = await asyncio.wait_for(
            client.get_workflow_handle("hidden-v5-clear").result(), timeout=90)
        observed["runs"]["clear"] = evidence(state, clear_result, "hidden-v5-clear.json")
        observed["runs"]["clear"]["director_task"] = await asyncio.to_thread(
            director_task, clear_task["id"])

        old_status = await old_child.query(FactoryRun.status)
        old_command = {"command_id": "hidden-abort-old", "action": "abort",
            "actor": direct_authority["identity"], "token": direct_authority["token"],
            "epoch": 1, "run": old_status["run"],
            "definition_digest": old_status["definition_digest"],
            "revision": old_status["current_revision"],
            "sha256": old_status["current_sha256"]}
        await old_child.execute_update(FactoryRun.director_command, old_command)
        observed["runs"]["old_result"] = await client.get_workflow_handle(
            "hidden-old-v3-wait").result()
        observed["freeze_after"] = verify_freeze()
        assert observed["worker_unchanged_during_publication"]
        assert old_after["package_digest"] == old_wait["package_digest"]
        assert first_result["child"]["acceptance"]["revision"] == "r2"
        assert second_result["status"] == "aborted"
        assert second_result["child"].get("acceptance") is None
        assert second_result["child"].get("released") is False
        assert first_result["child"]["definition_digest"] == second_result["child"]["definition_digest"]
        assert exhausted_result["status"] == "aborted"
        assert clear_result["child"]["acceptance"]["revision"] == "r1"
        assert observed["freeze_after"] == before_freeze
        assert observed["same_digest_divergent_outcomes_met"]
        observed["status"] = "retrospective_fix_pass"
    except Exception as error:
        observed["status"] = "failed"
        observed["error"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        (HERE / "hidden-observed.json").write_text(json.dumps(observed, indent=2, sort_keys=True) + "\n")
        if supervisor.poll() is None:
            supervisor.send_signal(signal.SIGTERM)
            try:
                supervisor.wait(timeout=20)
            except subprocess.TimeoutExpired:
                runtime.kill(supervisor, signal.SIGKILL)
        print(json.dumps({"status": observed["status"], "state": str(state),
                          "same_digest_divergent_outcomes_met":
                          observed.get("same_digest_divergent_outcomes_met")}))


if __name__ == "__main__":
    asyncio.run(main())
