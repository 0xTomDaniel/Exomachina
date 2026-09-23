#!/usr/bin/env python3
"""Withheld A2C graph trial; frozen engine-facing code remains unchanged."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.request
from datetime import timedelta

from temporalio.client import Client

import runtime
from author import materialize, template
from definition import digest, publish
from factory import FactoryRun
from probe import SERVICE_PORTS, bindings, director_send, director_task, input_for

HERE = Path(__file__).resolve().parent
OLD = HERE.parent.parent / "2026-09-22"
S2_PYTHON = OLD / "s2" / ".venv" / "bin" / "python"
sys.path.insert(0, str(OLD / "decision-round" / "common"))
from client import get_task as remote_task  # noqa: E402


def verify_freeze() -> dict:
    out = subprocess.check_output([sys.executable,
        str(OLD / "arbitration" / "common" / "freeze.py"),
        "verify", str(HERE / "freeze.json")], text=True)
    return json.loads(out)


def service_health(url: str) -> dict:
    with urllib.request.urlopen(url + "/health", timeout=2) as response:
        return json.load(response)


async def until(check, label: str, seconds: float = 60):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            value = await check()
            if value:
                return value
        except Exception as error:
            last = repr(error)
        await asyncio.sleep(.2)
    raise TimeoutError(f"{label}: {last}")


def launch_supervisor(state: Path, label: str) -> subprocess.Popen:
    with (state / f"supervisor-{label}.log").open("a") as log:
        return subprocess.Popen([sys.executable, str(HERE / "supervisor.py"),
                                 "--state", str(state)], cwd=HERE,
                                stdout=log, stderr=subprocess.STDOUT,
                                start_new_session=True)


async def supervisor_ready(state: Path, process: subprocess.Popen):
    async def check():
        if process.poll() is not None:
            raise RuntimeError(f"supervisor exited {process.returncode}")
        return (state / "supervisor-ready").exists()
    await until(check, "supervisor ready", 90)


def launch_extra(state: Path, name: str, port: int, *, held: bool) -> subprocess.Popen:
    script = ((HERE.parent / "effect-inflight-scale" / "slow_harness_server.py") if held
              else (OLD / "decision-round" / "common" / "harness_server.py"))
    with (state / f"{name}.log").open("a") as log:
        return subprocess.Popen([str(S2_PYTHON), str(script), "--state", str(state / name),
                                 "--role", "capability", "--port", str(port)],
                                stdout=log, stderr=subprocess.STDOUT,
                                start_new_session=True)


def records(state: Path, service: str, run: str) -> list[dict]:
    path = state / service / "harness.sqlite3"
    if not path.is_file():
        return []
    with sqlite3.connect(path) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute(
            "SELECT action_id,run_id,definition_digest,task_id,artifact FROM actions WHERE run_id=?",
            (run,))]


def pending(state: Path, service: str, action_id: str) -> dict | None:
    path = state / service / "harness.sqlite3"
    if not path.is_file():
        return None
    with sqlite3.connect(path) as db:
        db.row_factory = sqlite3.Row
        row = db.execute("SELECT * FROM pending WHERE action_id=?", (action_id,)).fetchone()
    return dict(row) if row else None


def release_rows(state: Path, run: str) -> list[dict]:
    path = state / "release" / "release.sqlite3"
    with sqlite3.connect(path) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute("SELECT * FROM releases WHERE run_id=?", (run,))]


def worker_pid(state: Path) -> int:
    lines = (state / "supervisor-events.jsonl").read_text().splitlines()
    return [row["pid"] for line in lines if (row := json.loads(line))["kind"] in
            {"worker-start", "worker-restart"}][-1]


async def child_phase(client: Client, parent_id: str, phase: str):
    async def check():
        parent = client.get_workflow_handle(parent_id)
        status = await parent.query(FactoryRun.status)
        if not status["child_id"]:
            return False
        child = client.get_workflow_handle(status["child_id"])
        child_status = await child.query(FactoryRun.status)
        return (child_status, child) if child_status["phase"] == phase else False
    return await until(check, f"{parent_id} child phase {phase}", 75)


async def start_direct(client: Client, run: str, package: dict, director: dict,
                       *, mode: str | None = None):
    payload = input_for(run, package, director)
    if mode is not None:
        payload["outcome_mode"] = mode
    return await client.start_workflow(FactoryRun.run, payload, id=run,
        task_queue="arbitration-temporal", execution_timeout=timedelta(minutes=10))


def joined_from_candidate(result: dict) -> dict:
    candidate = result["child"]["acceptance"]
    assert candidate is not None
    # The fixture's accepted output includes the exact candidate in Quality's
    # A2A action store, inspected separately below.
    return candidate


def candidate_join(state: Path, child_run: str, revision: str) -> dict:
    rows = records(state, "quality", child_run)
    matching = [row for row in rows if json.loads(row["artifact"])["revision"] == revision]
    if len(matching) != 1:
        raise AssertionError(f"expected exactly one Quality action for {revision}")
    # Quality input is not stored in its output row; reconstruct and validate
    # from the exact branch artifacts using the frozen fixture's typed join.
    sys.path.insert(0, str(OLD / "arbitration" / "common"))
    import fixture
    return matching[0], fixture


async def main() -> dict:
    before = verify_freeze()
    state = Path(tempfile.mkdtemp(prefix="exo-temporal-a2c-", dir="/tmp"))
    runtime.STATE = state
    result: dict = {"schema": "exomachina.temporal.a2c/1", "status": "running",
                    "state": str(state), "freeze_before": before,
                    "definitions": {}, "runs": {}}
    supervisor = launch_supervisor(state, "first")
    extras: list[subprocess.Popen] = []
    try:
        await supervisor_ready(state, supervisor)
        client = await Client.connect(f"127.0.0.1:{runtime.PORTS['frontend']}",
                                      namespace="exomachina")
        identities = {}
        for name in ("source", "counter", "quality", "release"):
            identities[name] = service_health(f"http://127.0.0.1:{SERVICE_PORTS[name]}")
        old_bindings = bindings(identities)
        extra_specs = (("source_beta", 29568, False), ("counter_beta", 29569, True))
        for name, port, held in extra_specs:
            extras.append(launch_extra(state, name, port, held=held))
            async def check(name=name, port=port):
                return service_health(f"http://127.0.0.1:{port}")
            identities[name] = await until(check, f"{name} ready", 30)
        def binding(role, url, identity):
            return {"role": role, "url": url, "identity": identity, "approved": True}
        four = {
            "source_alpha": binding("capability", old_bindings["source"]["url"],
                                    identities["source"]["identity"]),
            "source_beta": binding("capability", "http://127.0.0.1:29568",
                                   identities["source_beta"]["identity"]),
            "counter_alpha": binding("capability", old_bindings["counter"]["url"],
                                     identities["counter"]["identity"]),
            "counter_beta": binding("capability", "http://127.0.0.1:29569",
                                    identities["counter_beta"]["identity"]),
            "quality": old_bindings["quality"], "release": old_bindings["release"],
        }
        assert len({four[name]["identity"] for name in
                    ("source_alpha", "source_beta", "counter_alpha", "counter_beta")}) == 4
        approved = {**old_bindings, **four}
        (state / "catalog" / "approved_bindings.json").write_text(json.dumps(approved, sort_keys=True))
        result["service_identities"] = {name: value["identity"] for name, value in identities.items()}
        direct_authority = {"identity": "a2c-director", "token": "a2c-fixture-token", "epoch": 1}

        old = materialize(template("visible-exhausted.json"), old_bindings)
        old_digest = publish(old, state / "catalog", approved)
        old_handle = await start_direct(client, "a2c-old-visible", old, direct_authority)
        old_wait, old_child = await child_phase(client, "a2c-old-visible", "awaiting-director")
        result["runs"]["old_before"] = old_wait
        result["definitions"]["old_visible"] = {"package_digest": old_digest,
            "child_digest": next(iter(old["children"]))}

        before_publication_pid = worker_pid(state)
        packages = {}
        for label, name in (("mixed", "withheld-a2c-v4-mixed.json"),
                            ("clear", "withheld-a2c-v5-clear.json")):
            package = materialize(template(name), four)
            package_digest = publish(package, state / "catalog", approved)
            packages[label] = package
            result["definitions"][label] = {"template": name,
                "package_digest": package_digest,
                "child_digest": next(iter(package["children"])),
                "node_count": len(next(iter(package["children"].values()))["nodes"])}
        result["worker_unchanged_during_publication"] = worker_pid(state) == before_publication_pid
        result["runs"]["old_after_publications"] = await old_child.query(FactoryRun.status)

        supervisor.send_signal(signal.SIGTERM)
        supervisor.wait(timeout=25)
        (state / "supervisor-ready").unlink(missing_ok=True)
        supervisor = launch_supervisor(state, "restart")
        await supervisor_ready(state, supervisor)
        client = await Client.connect(f"127.0.0.1:{runtime.PORTS['frontend']}",
                                      namespace="exomachina")
        old_after_restart, old_child = await child_phase(client, "a2c-old-visible", "awaiting-director")
        result["runs"]["old_after_restart"] = old_after_restart
        result["worker_pid_after_restart"] = worker_pid(state)

        # Mixed true branch: delay the separate counter-beta A2A Task until
        # source-alpha, source-beta and counter-alpha have completed remotely.
        gate = state / "hold-source-assignments"
        gate.write_text("hold counter beta until other three complete\n")
        success_run = "a2c-mixed-success"
        success = await start_direct(client, success_run, packages["mixed"],
                                     direct_authority, mode="after_first_repair")
        mixed_child = success_run + ":child:" + result["definitions"]["mixed"]["child_digest"][:12]
        async def other_three():
            selected = {"source_alpha": "source", "source_beta": "source_beta",
                        "counter_alpha": "counter"}
            items = {name: records(state, service, mixed_child) for name, service in selected.items()}
            if any(len(rows) != 1 for rows in items.values()):
                return False
            tasks = {}
            for name, rows in items.items():
                task = await asyncio.to_thread(remote_task, four[name]["url"], rows[0]["task_id"])
                if task["status"]["state"] != "completed":
                    return False
                tasks[name] = task
            return items, tasks
        three_rows, three_tasks = await until(other_three,
            "three A2A assignments complete before counter beta", 15)
        async def pending_beta():
            return pending(state, "counter_beta", mixed_child + ":counter_beta")
        beta = await until(pending_beta, "counter beta working", 15)
        beta_url = four["counter_beta"]["url"]
        beta_task = await asyncio.to_thread(remote_task, beta_url, beta["task_id"])
        assert beta_task["status"]["state"] == "working"
        child_before_release = await client.get_workflow_handle(mixed_child).query(FactoryRun.status)
        assert child_before_release["phase"] == "parallel"
        result["runs"]["mixed_delay"] = {"completed_three": {
            name: rows[0]["task_id"] for name, rows in three_rows.items()},
            "completed_three_states": {name: task["status"]["state"]
                for name, task in three_tasks.items()},
            "counter_beta_task_id": beta["task_id"],
            "counter_beta_task_status_before_release": beta_task["status"]["state"],
            "child_status_before_release": child_before_release}
        gate.unlink()
        success_result = await asyncio.wait_for(success.result(), timeout=90)
        result["runs"]["mixed_success"] = success_result

        # The *same* immutable package now receives the other typed input.
        exhausted_run = "a2c-mixed-exhausted"
        exhausted = await start_direct(client, exhausted_run, packages["mixed"],
                                       direct_authority, mode="never")
        exhausted_wait, exhausted_child = await child_phase(client, exhausted_run,
                                                             "awaiting-director")
        result["runs"]["mixed_exhausted_wait"] = exhausted_wait
        await exhausted_child.execute_update(FactoryRun.director_command, {
            "command_id": "a2c-mixed-authorized-abort", "action": "abort",
            "actor": direct_authority["identity"], "token": direct_authority["token"],
            "epoch": 1, "run": exhausted_wait["run"],
            "definition_digest": exhausted_wait["definition_digest"],
            "revision": exhausted_wait["current_revision"],
            "sha256": exhausted_wait["current_sha256"]})
        exhausted_result = await asyncio.wait_for(exhausted.result(), timeout=90)
        result["runs"]["mixed_exhausted"] = exhausted_result

        # All-clear uses the original Director A2A Task path; it never reaches
        # the from_run repair node and needs no extra input.
        clear_task = await asyncio.to_thread(director_send, {
            "op": "start", "action_id": "a2c-clear-start",
            "run_id": "a2c-clear", "package_digest": result["definitions"]["clear"]["package_digest"]})
        clear_result = await asyncio.wait_for(client.get_workflow_handle("a2c-clear").result(),
                                              timeout=90)
        result["runs"]["clear"] = {"result": clear_result,
            "original_task_id": clear_task["id"],
            "original_task_after": await asyncio.to_thread(director_task, clear_task["id"])}

        # Complete the retained old run under its original child digest.
        old_now = await old_child.query(FactoryRun.status)
        await old_child.execute_update(FactoryRun.director_command, {
            "command_id": "a2c-old-authorized-abort", "action": "abort",
            "actor": direct_authority["identity"], "token": direct_authority["token"],
            "epoch": 1, "run": old_now["run"],
            "definition_digest": old_now["definition_digest"],
            "revision": old_now["current_revision"],
            "sha256": old_now["current_sha256"]})
        result["runs"]["old_final"] = await asyncio.wait_for(old_handle.result(), timeout=90)

        # Exact four-branch join and remote artifact checks.
        sys.path.insert(0, str(OLD / "arbitration" / "common"))
        import fixture
        declarations = {name: value["result_type"] for name, value in
            template("withheld-a2c-v4-mixed.json")["child"]["nodes"]["gather"]["branches"].items()}
        scopes = {name: value["scope_status"] for name, value in
            template("withheld-a2c-v4-mixed.json")["child"]["nodes"]["gather"]["branches"].items()
            if value["scope_status"] is not None}
        services = {"source_alpha": "source", "source_beta": "source_beta",
                    "counter_alpha": "counter", "counter_beta": "counter_beta"}
        branch_receipts = {}
        for instance, service in services.items():
            item = records(state, service, mixed_child)
            assert len(item) == 1 and item[0]["action_id"] == mixed_child + ":" + instance
            item = item[0]
            branch_receipts[instance] = {"action_id": item["action_id"],
                "run_id": item["run_id"], "definition_digest": item["definition_digest"],
                "artifact": json.loads(item["artifact"])}
        join = fixture.typed_join(branch_receipts, run_id=mixed_child,
            definition_digest=result["definitions"]["mixed"]["child_digest"],
            declarations=declarations, scope_status_by_instance=scopes)
        result["runs"]["mixed_join"] = join
        result["runs"]["mixed_branch_task_ids"] = {instance:
            records(state, service, mixed_child)[0]["task_id"]
            for instance, service in services.items()}
        result["runs"]["mixed_success_release_rows"] = release_rows(state, mixed_child)
        exhausted_child_run = exhausted_result["child"]["run"]
        result["runs"]["mixed_exhausted_release_rows"] = release_rows(state, exhausted_child_run)
        result["runs"]["mixed_exhausted_quality_count"] = len(records(state, "quality", exhausted_child_run))
        result["runs"]["clear_release_rows"] = release_rows(state, clear_result["child"]["run"])
        activities = [json.loads(line) for line in (state / "activities.jsonl").read_text().splitlines()]
        success_events = [event for event in activities if event.get("run") == mixed_child]
        result["runs"]["mixed_success_activity_events"] = success_events
        timestamps = {(event["kind"], event.get("instance")): event["wall_time"]
            for event in success_events if event["kind"] in {"assign-complete", "join-start"}}
        result["runs"]["join_after_all_four"] = (
            all(timestamps[("assign-complete", instance)] < timestamps[("assign-complete", "counter_beta")]
                for instance in ("source_alpha", "source_beta", "counter_alpha"))
            and timestamps[("assign-complete", "counter_beta")] < timestamps[("join-start", None)])

        assert join["requires_scope"] is True
        assert set(join["branch_artifact_sha256"]) == set(services)
        assert len(join["claims"]) == 4 and len(join["objections"]) == 4
        assert {claim["branch_instance"] for claim in join["claims"]} == {"source_alpha", "source_beta"}
        assert {item["branch_instance"] for item in join["objections"]} == {"counter_alpha", "counter_beta"}
        assert result["runs"]["join_after_all_four"]
        assert success_result["status"] == "accepted"
        assert success_result["child"]["acceptance"]["revision"] == "r2"
        assert len(result["runs"]["mixed_success_release_rows"]) == 1
        assert exhausted_result["status"] == "aborted"
        assert exhausted_result["child"].get("acceptance") is None
        assert exhausted_result["child"]["released"] is False
        assert exhausted_wait["current_revision"] == "r3"
        assert result["runs"]["mixed_exhausted_quality_count"] == 3
        assert len(result["runs"]["mixed_exhausted_release_rows"]) == 0
        assert success_result["child"]["definition_digest"] == exhausted_result["child"]["definition_digest"]
        assert clear_result["status"] == "accepted"
        assert clear_result["child"]["acceptance"]["revision"] == "r1"
        assert result["runs"]["clear"]["original_task_after"]["status"]["state"] == "completed"
        assert len(result["runs"]["clear_release_rows"]) == 1
        assert result["worker_unchanged_during_publication"]
        assert old_after_restart["definition_digest"] == old_wait["definition_digest"]
        assert result["runs"]["old_final"]["child"]["definition_digest"] == old_wait["definition_digest"]
        result["freeze_after"] = verify_freeze()
        assert result["freeze_after"] == before
        result["status"] = "blind_pass_with_direct_mixed_starts"
    except Exception as error:
        result["status"] = "failed"
        result["error"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        (HERE / "a2c-observed.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        for proc in extras:
            if proc.poll() is None:
                proc.terminate()
                try: proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill(); proc.wait(timeout=5)
        if supervisor.poll() is None:
            supervisor.send_signal(signal.SIGTERM)
            try: supervisor.wait(timeout=25)
            except subprocess.TimeoutExpired:
                runtime.kill(supervisor, signal.SIGKILL)
        print(json.dumps({"status": result["status"], "state": str(state),
                          "freeze": before["inventory_sha256"]}))
    return result


if __name__ == "__main__":
    asyncio.run(main())
