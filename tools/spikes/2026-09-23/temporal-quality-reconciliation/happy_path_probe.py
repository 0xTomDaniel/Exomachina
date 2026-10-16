#!/usr/bin/env python3
"""One ordinary Director A2A run, then offline replay after graceful shutdown.

No injected faults, altered verdicts, ambiguous replies, or process kills.
The private Temporal histories and databases are removed after summary evidence.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path
from uuid import uuid4

from temporalio.client import Client, WorkflowHistory
from temporalio.worker import Replayer

import runtime
from author import materialize, template
from definition import publish
from factory import FactoryRun


HERE = Path(__file__).resolve().parent
COMMON = HERE.parent.parent / "2026-09-22" / "arbitration" / "common"
PRIOR_COMMON = HERE.parent.parent / "2026-09-22" / "decision-round" / "common"
S2_PYTHON = HERE.parent.parent / "2026-09-22" / "s2" / ".venv" / "bin" / "python"
SERVICE_PORTS = {"source": 44021, "counter": 44022, "quality": 44023,
                 "release": 44024, "director": 44025}
SOURCE_FILES = ("adapter.py", "a2a_outcome.py", "quality_authority.py",
                "incident_projection.py", "factory.py", "director_server.py",
                "runtime.py", "worker.py")


def hashes() -> dict[str, str]:
    return {name: hashlib.sha256((HERE / name).read_bytes()).hexdigest()
            for name in SOURCE_FILES}


def rows(path: Path, query: str, *args: str) -> list[dict]:
    with sqlite3.connect(path) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute(query, args)]


def start_service(name: str, state: Path) -> subprocess.Popen:
    port = SERVICE_PORTS[name]
    service_state = state / name
    if name in {"source", "counter"}:
        argv = [str(S2_PYTHON), str(PRIOR_COMMON / "harness_server.py"),
                "--state", str(service_state), "--role", "capability", "--port", str(port)]
    elif name == "quality":
        argv = [str(S2_PYTHON), str(COMMON / "quality_server.py"),
                "--state", str(service_state), "--port", str(port)]
    elif name == "director":
        argv = [sys.executable, str(HERE / "director_server.py"),
                "--state", str(service_state), "--catalog", str(state / "catalog"),
                "--address", f"127.0.0.1:{runtime.PORTS['frontend']}", "--port", str(port)]
    else:
        argv = [str(S2_PYTHON), str(COMMON / "release_server.py"),
                "--state", str(service_state), "--mode", "participating", "--port", str(port)]
    with (state / f"{name}.log").open("a") as log:
        return subprocess.Popen(argv, stdout=log, stderr=log, start_new_session=True)


async def health(name: str, process: subprocess.Popen) -> dict:
    url = f"http://127.0.0.1:{SERVICE_PORTS[name]}/health"
    async def check():
        if process.poll() is not None:
            raise RuntimeError(f"{name} exited {process.returncode}")
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                return json.load(response)
        except Exception:
            return False
    return await runtime.until(check, seconds=30)


def binding(identities: dict) -> dict:
    return {name: {"role": "capability" if name in {"source", "counter"} else name,
                   "url": f"http://127.0.0.1:{SERVICE_PORTS[name]}",
                   "identity": identities[name]["identity"], "approved": True}
            for name in ("source", "counter", "quality", "release")}


def director_rpc(method: str, params: dict) -> dict:
    rpc = {"jsonrpc": "2.0", "id": str(uuid4()), "method": method, "params": params}
    req = urllib.request.Request(f"http://127.0.0.1:{SERVICE_PORTS['director']}/",
        data=json.dumps(rpc).encode(), method="POST",
        headers={"Authorization": "Bearer fixture-token", "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as response:
        body = json.load(response)
    if "error" in body:
        raise RuntimeError("Director A2A error: " + json.dumps(body["error"]))
    return body["result"]


def director_send(command: dict) -> dict:
    return director_rpc("message/send", {"message": {"role": "user",
        "messageId": str(uuid4()), "parts": [{"kind": "data", "data": command}]}})


def director_task(task_id: str) -> dict:
    return director_rpc("tasks/get", {"id": task_id})


def graceful_stop(process: subprocess.Popen, name: str) -> dict:
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        process.wait(timeout=30)
    return {"name": name, "pid": process.pid, "exit_code": process.returncode}


async def main() -> None:
    state = Path(tempfile.mkdtemp(prefix="exo-tq-quality-", dir="/tmp"))
    runtime.STATE = state
    (state / "pgsocket").mkdir()
    (state / "catalog").mkdir()
    evidence: dict = {"status": "running", "state_prefix": "/tmp/exo-tq-quality-*",
                      "source_hashes_before": hashes(), "ports": {**runtime.PORTS, **SERVICE_PORTS},
                      "processes_started": [], "graceful_stops": []}
    processes: dict[str, subprocess.Popen] = {}
    postgres_running = False
    histories: dict[str, tuple[str, Path]] = {}
    failure: BaseException | None = None
    try:
        low = ("postgres", "front_member", "match_member", "history_member", "worker_member")
        high = set(runtime.PORTS) - set(low)
        assert all(31400 <= runtime.PORTS[key] <= 31499 for key in low)
        assert all(44000 <= runtime.PORTS[key] <= 44999 for key in high)
        assert all(44000 <= port <= 44999 for port in SERVICE_PORTS.values())
        ports = list(runtime.PORTS.values()) + list(SERVICE_PORTS.values())
        assert len(ports) == len(set(ports))
        occupied = [port for port in ports if runtime.port_open(port)]
        if occupied:
            raise RuntimeError(f"lane ports occupied: {occupied}")
        for binary in (runtime.TEMPORAL, runtime.SQL_TOOL, runtime.SCHEMA,
                       runtime.PG_BIN / "initdb", runtime.CLI, S2_PYTHON):
            if not binary.exists():
                raise FileNotFoundError(binary)

        runtime.start_postgres(True)
        postgres_running = True
        config = runtime.render_config()
        config_text = config.read_text()
        for port in (runtime.PORTS[key] for key in low):
            assert str(port) in config_text
        processes["temporal"] = runtime.start_temporal(config)
        await runtime.wait_server(processes["temporal"])
        runtime.shell([str(runtime.CLI), "--disable-config-env", "--disable-config-file",
            "--address", f"127.0.0.1:{runtime.PORTS['frontend']}",
            "--namespace", "exomachina", "operator", "namespace", "create", "--retention", "1d"])
        for name in ("source", "counter", "quality", "release"):
            processes[name] = start_service(name, state)
        identities = {name: await health(name, processes[name])
                      for name in ("source", "counter", "quality", "release")}
        evidence["service_identities"] = {name: value["identity"] for name, value in identities.items()}
        approved = binding(identities)
        (state / "catalog" / "approved_bindings.json").write_text(json.dumps(approved, sort_keys=True))
        package = materialize(template("visible-v3.json"), approved)
        package_digest = publish(package, state / "catalog", approved)
        evidence["package_digest"] = package_digest
        evidence["child_definition_digest"] = next(iter(package["children"]))
        processes["worker"] = runtime.start_worker()
        processes["director"] = start_service("director", state)
        evidence["director_identity"] = (await health("director", processes["director"]))["identity"]
        evidence["processes_started"] = [{"name": name, "pid": process.pid}
                                         for name, process in processes.items()]

        run_id = "exo-tq-quality-visible-v3-" + state.name.removeprefix("exo-tq-quality-")
        started = await asyncio.to_thread(director_send, {"op": "start",
            "action_id": "start:" + run_id, "run_id": run_id,
            "package_digest": package_digest})
        task_id = started["id"]
        evidence["original_task_id"] = task_id
        evidence["initial_task_state"] = started["status"]["state"]
        client = await Client.connect(f"127.0.0.1:{runtime.PORTS['frontend']}",
                                      namespace="exomachina")
        parent_handle = client.get_workflow_handle(run_id)
        result = await asyncio.wait_for(parent_handle.result(), timeout=120)
        final_task = await asyncio.to_thread(director_task, task_id)
        parent_status = await parent_handle.query(FactoryRun.status)
        child_id = parent_status["child_id"]
        child_status = await client.get_workflow_handle(child_id).query(FactoryRun.status)
        evidence["run_id"] = run_id
        evidence["child_id"] = child_id
        evidence["terminal_task_state"] = final_task["status"]["state"]
        evidence["terminal_task_id"] = final_task["id"]
        evidence["result_status"] = result["status"]
        evidence["accepted_revision"] = result["child"].get("acceptance", {}).get("revision")
        evidence["acceptance"] = result["child"].get("acceptance")
        evidence["parent_phase"] = parent_status["phase"]
        evidence["child_phase"] = child_status["phase"]
        evidence["task_artifact_status"] = final_task["artifacts"][0]["parts"][0]["data"]["status"]

        quality_rows = rows(state / "quality" / "harness.sqlite3",
            "SELECT action_id,task_id,attempts,artifact FROM actions WHERE run_id=? ORDER BY action_id", child_id)
        evidence["quality_actions"] = [{"action_id": row["action_id"],
            "task_id": row["task_id"], "attempts": row["attempts"],
            "verdict": json.loads(row["artifact"])} for row in quality_rows]
        activity_events = [json.loads(line) for line in (state / "activities.jsonl").read_text().splitlines()]
        evidence["quality_decisions"] = [{key: event[key] for key in
            ("decision_kind", "revision", "accepted", "task_id")}
            for event in activity_events if event["kind"] == "quality-verdict" and event["run"] == child_id]
        evidence["release_rows"] = rows(state / "release" / "release.sqlite3",
            "SELECT release_id,run_id,revision,sha256,attempts,accepted_effect_count "
            "FROM releases WHERE run_id=?", child_id)
        journal_rows = rows(state / "a2a-outcomes.sqlite3", "SELECT action_id,value FROM outcomes ORDER BY action_id")
        evidence["journal_rows"] = [json.loads(row["value"]) for row in journal_rows]
        evidence["director_alias_rows"] = rows(state / "director" / "director.sqlite3",
            "SELECT task_id,run_id FROM aliases WHERE task_id=?", task_id)

        assert task_id == final_task["id"]
        assert final_task["status"]["state"] == "completed"
        assert result["status"] == "accepted" and result["released"] is True
        assert result["child"]["acceptance"] == result["acceptance"]
        assert result["child"]["acceptance"]["revision"] == "r2"
        assert child_status["authoritative_acceptance"] == result["acceptance"]
        assert parent_status["phase"] == child_status["phase"] == "accepted"
        assert len(evidence["quality_actions"]) == len(evidence["quality_decisions"]) == 2
        assert [item["decision_kind"] for item in evidence["quality_decisions"]] == ["negative", "positive"]
        assert [item["accepted"] for item in evidence["quality_decisions"]] == [False, True]
        assert all(item["attempts"] == 1 for item in evidence["quality_actions"])
        assert len(evidence["release_rows"]) == 1
        assert evidence["release_rows"][0]["accepted_effect_count"] == 1
        assert evidence["release_rows"][0]["attempts"] == 1
        assert len(evidence["journal_rows"]) == 5
        assert all(item["phase"] == "confirmed" for item in evidence["journal_rows"])
        release_journal = [item for item in evidence["journal_rows"]
            if item["effect_kind"] == "release"]
        assert len(release_journal) == 1
        assert release_journal[0]["action_id"] == evidence["release_rows"][0]["release_id"]
        assert release_journal[0]["receipt"]["accepted_effect_count"] == 1
        assert len(evidence["director_alias_rows"]) == 1

        for role, workflow_id in (("parent", run_id), ("child", child_id)):
            history = await client.get_workflow_handle(workflow_id).fetch_history()
            content = history.to_json()
            path = state / f"{role}-history.json"
            path.write_text(content)
            histories[role] = (workflow_id, path)
            evidence.setdefault("histories", {})[role] = {
                "workflow_id": workflow_id, "event_count": len(history.events),
                "sha256": hashlib.sha256(content.encode()).hexdigest()}
        evidence["source_hashes_after"] = hashes()
        assert evidence["source_hashes_after"] == evidence["source_hashes_before"]
    except BaseException as error:
        failure = error
        evidence["error"] = {"type": type(error).__name__, "message": str(error)[:1000]}
    finally:
        for name in ("director", "worker", "source", "counter", "quality", "release", "temporal"):
            process = processes.get(name)
            if process is not None:
                try:
                    evidence["graceful_stops"].append(graceful_stop(process, name))
                except Exception as error:
                    evidence.setdefault("stop_errors", []).append({"name": name,
                        "type": type(error).__name__, "message": str(error)[:500]})
        if postgres_running or runtime.port_open(runtime.PORTS["postgres"]):
            try:
                runtime.pg_command("-m", "smart", "stop")
                evidence["postgres_stopped"] = True
            except Exception as error:
                evidence["postgres_stopped"] = False
                evidence.setdefault("stop_errors", []).append({"name": "postgres",
                    "type": type(error).__name__, "message": str(error)[:500]})

    if failure is None and not evidence.get("stop_errors"):
        try:
            for role, (workflow_id, path) in histories.items():
                history = WorkflowHistory.from_json(workflow_id, path.read_text())
                await Replayer(workflows=[FactoryRun]).replay_workflow(history)
                evidence.setdefault("offline_replay", {})[role] = "pass"
        except Exception as error:
            failure = error
            evidence["offline_replay_error"] = {"type": type(error).__name__,
                                                 "message": str(error)[:1000]}
    evidence["status"] = "passed" if failure is None and not evidence.get("stop_errors") else "failed"
    evidence["state_removed"] = False
    output = HERE / "live_observed.json"
    if not evidence.get("stop_errors"):
        try:
            shutil.rmtree(state)
            evidence["state_removed"] = True
        except Exception as error:
            evidence["cleanup_error"] = {"type": type(error).__name__, "message": str(error)[:500]}
            evidence["status"] = "failed"
    output.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"status": evidence["status"], "original_task_id": evidence.get("original_task_id"),
                      "terminal_task_state": evidence.get("terminal_task_state"),
                      "offline_replay": evidence.get("offline_replay"),
                      "state_removed": evidence["state_removed"], "error": evidence.get("error"),
                      "stop_errors": evidence.get("stop_errors")}, sort_keys=True))
    if failure is not None:
        raise failure
    if evidence.get("stop_errors") or not evidence["state_removed"]:
        raise RuntimeError("graceful cleanup incomplete")


if __name__ == "__main__":
    asyncio.run(main())
