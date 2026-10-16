#!/usr/bin/env python3
"""Ordinary publication and Director-run trial; no process or workflow fault injection."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import timedelta
from importlib.metadata import version
from pathlib import Path

from temporalio.client import Client

import runtime
from author import materialize, template
from binding import DEPLOYMENT, PublicationStore, make_manifest, may_retire, source_digest
from definition import digest, publish
from factory import FactoryRun
from probe import SERVICE_PORTS, bindings, director_send, director_task, health, start_service


HERE = Path(__file__).resolve().parent
OUT = HERE / "observed.json"


def cli(*args: str) -> dict:
    raw = runtime.shell([str(runtime.CLI), "--disable-config-env", "--disable-config-file",
        "--address", f"127.0.0.1:{runtime.PORTS['frontend']}", "--namespace", "exomachina",
        "--output", "json", *args], timeout=30)
    return json.loads(raw) if raw.strip() else {}


def start_build(build: str, state: Path) -> subprocess.Popen:
    directory = HERE / f"build_{build}"
    env = dict(os.environ, EXO_TEMPORAL_ADDRESS=f"127.0.0.1:{runtime.PORTS['frontend']}",
               EXO_TEMPORAL_ACTIVITY_LOG=str(state / f"activities-{build}.jsonl"),
               EXO_WORKER_SOURCE_DIGEST=source_digest(directory))
    log = (state / f"worker-{build}.log").open("a")
    process = subprocess.Popen([sys.executable, str(directory / "worker.py")],
        cwd=directory, env=env, stdout=log, stderr=log)
    log.close()
    return process


async def wait_deployment(build: str, process: subprocess.Popen) -> dict:
    async def check():
        if process.poll() is not None:
            raise RuntimeError(f"worker {build} exited; inspect worker-{build}.log")
        try:
            value = await asyncio.to_thread(cli, "worker", "deployment", "describe-version",
                "--deployment-name", DEPLOYMENT, "--build-id", build)
            queues = {item.get("type") for item in value.get("taskQueuesInfos", [])
                      if item.get("name") == DEPLOYMENT}
            return value if (value.get("deploymentName") == DEPLOYMENT and
                             value.get("BuildID") == build and
                             {"workflow", "activity"} <= queues) else False
        except Exception:
            return False
    return await runtime.until(check, seconds=45)


def publication(package: dict, build: str, state: Path, contracts: dict, policy: dict) -> dict:
    package_digest = publish(package, state / "catalog", package["bindings"])
    manifest = make_manifest(package, contracts, policy, build_id=build,
        code_digest=source_digest(HERE / f"build_{build}"),
        python=platform.python_version(), temporalio=version("temporalio"))
    closure = {"manifest": manifest, "manifest_digest": digest(manifest),
               "contracts": contracts, "quality_policy": policy}
    store = PublicationStore(state / "catalog")
    manifest_digest = store.publish(package, closure)
    assert manifest_digest == closure["manifest_digest"]
    return {"package_digest": package_digest, "manifest_digest": manifest_digest,
            "build_id": build, "source_digest": manifest["interpreter"]["source_digest"]}


async def child_wait(client: Client, parent_id: str) -> tuple[dict, dict]:
    async def check():
        try:
            parent = await client.get_workflow_handle(parent_id).query(FactoryRun.status)
            if not parent["child_id"]:
                return False
            child = await client.get_workflow_handle(parent["child_id"]).query(FactoryRun.status)
            return (parent, child) if child["phase"] == "awaiting-director" else False
        except Exception:
            return False
    return await runtime.until(check, seconds=100)


async def pinned(workflow_id: str) -> str:
    async def check():
        try:
            info = await asyncio.to_thread(cli, "workflow", "describe", "--workflow-id", workflow_id)
            return info["workflowExecutionInfo"].get("versioningInfo", {}).get("version") or False
        except Exception:
            return False
    return await runtime.until(check, seconds=40)


async def normal_stop(process: subprocess.Popen | None) -> None:
    if process and process.poll() is None:
        process.terminate()
        await asyncio.to_thread(process.wait, 30)


async def run() -> dict:
    state = Path(tempfile.mkdtemp(prefix="exo-tq-version-", dir="/tmp"))
    runtime.STATE = state
    (state / "pgsocket").mkdir()
    (state / "catalog").mkdir()
    evidence: dict = {"status": "running", "versions": {}, "publications": {},
                      "runs": {}, "activation": {}, "retirement": {}, "replay": {},
                      "cleanup": {}, "ports": {**runtime.PORTS, **SERVICE_PORTS}}
    pg_running = False
    server = None
    workers: dict[str, subprocess.Popen] = {}
    services: dict[str, subprocess.Popen] = {}
    try:
        busy = [p for p in evidence["ports"].values() if runtime.port_open(p)]
        if busy:
            raise RuntimeError(f"owned port occupied: {busy}")
        evidence["versions"] = {"server": runtime.shell([str(runtime.TEMPORAL), "--version"]).strip(),
            "postgres": runtime.shell([str(runtime.PG_BIN / "postgres"), "--version"]).strip(),
            "temporalio": version("temporalio"), "python": platform.python_version()}
        runtime.start_postgres(True)
        pg_running = True
        server = runtime.start_temporal(runtime.render_config())
        await runtime.wait_server(server)
        runtime.shell([str(runtime.CLI), "--disable-config-env", "--disable-config-file",
            "--address", f"127.0.0.1:{runtime.PORTS['frontend']}",
            "--namespace", "exomachina", "operator", "namespace", "create", "--retention", "1d"])
        for name in ("source", "counter", "quality", "release"):
            services[name] = start_service(name, state)
        identities = {name: await health(name, proc) for name, proc in services.items()}
        evidence["independent_services_before"] = {
            name: {"pid": services[name].pid, "identity": identities[name]["identity"]}
            for name in identities}
        bound = bindings(identities)
        (state / "catalog" / "approved_bindings.json").write_text(json.dumps(bound, sort_keys=True))
        contracts = {name: {"role": bound[name]["role"],
                            "artifact_contract": {"source": "source_evidence@1", "counter": "counter_evidence@1",
                                                  "quality": "quality_verdict@1", "release": "release_receipt@1"}[name]}
                     for name in bound}
        policy = {"id": "quality-trial-v1", "authority": identities["quality"]["identity"],
                  "accepts": "resolved-artifact"}
        workers["b1"] = start_build("b1", state)
        evidence["activation"]["b1_worker_registered"] = await wait_deployment("b1", workers["b1"])
        old_template = template("visible-exhausted.json")
        old_template["root"]["revision"] = "version-binding-v1"
        old_template["child"]["revision"] = "version-binding-v1"
        old = materialize(old_template, bound)
        pub1 = publication(old, "b1", state, contracts, policy)
        evidence["publications"]["b1"] = pub1
        evidence["activation"]["b1_set_current"] = cli("worker", "deployment", "set-current-version",
            "--deployment-name", DEPLOYMENT, "--build-id", "b1", "--yes")
        PublicationStore(state / "catalog").activate(pub1["manifest_digest"],
            registered_version=f"{DEPLOYMENT}.b1", registered_source_digest=pub1["source_digest"])
        services["director"] = start_service("director", state)
        await health("director", services["director"])
        client = await Client.connect(f"127.0.0.1:{runtime.PORTS['frontend']}", namespace="exomachina")

        r1 = "tq-version-r1"
        task1 = await asyncio.to_thread(director_send, {"op": "start", "action_id": "start:" + r1,
            "run_id": r1, "package_digest": pub1["package_digest"]})
        parent1, child1 = await child_wait(client, r1)
        pin1 = await pinned(r1)
        child_pin1 = await pinned(child1["run"])
        evidence["runs"]["r1_wait"] = {"task_id": task1["id"], "parent": parent1,
            "child": child1, "pinned_version": pin1, "child_pinned_version": child_pin1,
            "original_task": await asyncio.to_thread(director_task, task1["id"])}
        assert pin1 == child_pin1 == f"{DEPLOYMENT}.b1" and parent1["manifest_digest"] == pub1["manifest_digest"]
        evidence["retained_build_rss_kib"] = {"b1_alone": runtime.rss_kib(workers["b1"].pid)}

        workers["b2"] = start_build("b2", state)
        evidence["activation"]["b2_worker_registered"] = await wait_deployment("b2", workers["b2"])
        newer_template = template("visible-exhausted.json")
        newer_template["root"]["revision"] = "version-binding-v2"
        newer_template["child"]["revision"] = "version-binding-v2"
        newer = materialize(newer_template, bound)
        pub2 = publication(newer, "b2", state, contracts, policy)
        evidence["publications"]["b2"] = pub2
        # A successful set-current reply is NOT used as the Director's routing barrier.
        evidence["activation"]["b2_set_current"] = cli("worker", "deployment", "set-current-version",
            "--deployment-name", DEPLOYMENT, "--build-id", "b2", "--yes")
        PublicationStore(state / "catalog").activate(pub2["manifest_digest"],
            registered_version=f"{DEPLOYMENT}.b2", registered_source_digest=pub2["source_digest"])
        evidence["activation"]["active"] = PublicationStore(state / "catalog").active()["manifest_digest"]
        evidence["activation"]["deployment_after_activation"] = cli("worker", "deployment", "describe",
            "--name", DEPLOYMENT)
        r2 = "tq-version-r2"
        task2 = await asyncio.to_thread(director_send, {"op": "start", "action_id": "start:" + r2,
            "run_id": r2, "package_digest": pub2["package_digest"]})
        parent2, child2 = await child_wait(client, r2)
        pin2 = await pinned(r2)
        child_pin2 = await pinned(child2["run"])
        evidence["runs"]["r2_wait"] = {"task_id": task2["id"], "parent": parent2,
            "child": child2, "pinned_version": pin2, "child_pinned_version": child_pin2,
            "original_task": await asyncio.to_thread(director_task, task2["id"])}
        assert pin2 == child_pin2 == f"{DEPLOYMENT}.b2" and parent2["manifest_digest"] == pub2["manifest_digest"]
        assert await pinned(r1) == pin1
        evidence["retirement"]["b1_retained_while_open"] = workers["b1"].poll() is None
        evidence["retained_build_rss_kib"].update({"b1_with_b2": runtime.rss_kib(workers["b1"].pid),
            "b2": runtime.rss_kib(workers["b2"].pid), "worker_process_count": 2})
        evidence["independent_services_after"] = {
            name: {"pid": services[name].pid,
                   "identity": (await health(name, services[name]))["identity"]}
            for name in identities}
        assert evidence["independent_services_after"] == evidence["independent_services_before"]

        for label, run_id, task, child in (("r1", r1, task1, child1), ("r2", r2, task2, child2)):
            await asyncio.to_thread(director_send, {"op": "abort", "action_id": "abort:" + run_id,
                "run_id": run_id, "revision": child["current_revision"], "sha256": child["current_sha256"]})
            result = await asyncio.wait_for(client.get_workflow_handle(run_id).result(), timeout=50)
            original = await asyncio.to_thread(director_task, task["id"])
            evidence["runs"][label + "_done"] = {"result": result,
                "pinned_version": await pinned(run_id), "original_task": original}
            assert result["status"] == "aborted" and original["status"]["state"] == "completed"
        assert "output_shape" not in evidence["runs"]["r1_done"]["result"]
        assert evidence["runs"]["r2_done"]["result"]["output_shape"] == 2

        history = await client.get_workflow_handle(child1["run"]).fetch_history()
        (HERE / "b1-child-history.json").write_text(history.to_json())
        for build in ("b1", "b2"):
            completed = subprocess.run([sys.executable, "-B", str(HERE / "replay_check.py"),
                "--build", build, "--history", str(HERE / "b1-child-history.json"),
                "--workflow-id", child1["run"]], text=True, capture_output=True, timeout=60)
            evidence["replay"][build] = {"exit_code": completed.returncode,
                "stdout": completed.stdout[-1200:], "stderr": completed.stderr[-1200:]}
        evidence["retirement"]["deployment_after_close"] = cli("worker", "deployment", "describe-version",
            "--deployment-name", DEPLOYMENT, "--build-id", "b1")
        evidence["retirement"]["b1_open_run_closed"] = True
        async def drained():
            value = await asyncio.to_thread(cli, "worker", "deployment", "describe", "--name", DEPLOYMENT)
            return value if may_retire("b1", "b2", [], value) else False
        try:
            evidence["retirement"]["drained_deployment_view"] = await runtime.until(drained, seconds=55)
            evidence["retirement"]["b1_retirement_gate_open"] = True
        except TimeoutError:
            evidence["retirement"]["b1_retirement_gate_open"] = False
        evidence["status"] = "passed"
    except Exception as error:
        evidence["status"] = "failed"
        evidence["error"] = repr(error)
        evidence["diagnostic_log_tails"] = {
            path.name: path.read_text(errors="replace")[-4000:]
            for path in state.glob("*.log") if path.is_file()}
        raise
    finally:
        OUT.write_text(json.dumps(evidence, indent=2, sort_keys=True, default=str))
        for process in reversed(list(services.values())):
            await normal_stop(process)
        for process in reversed(list(workers.values())):
            await normal_stop(process)
        await normal_stop(server)
        if pg_running:
            runtime.pg_command("-m", "smart", "stop")
        evidence["cleanup"] = {"all_started_processes_stopped": all(p.poll() is not None for p in
            [*services.values(), *workers.values(), *([server] if server else [])]),
            "owned_ports_closed": all(not runtime.port_open(p) for p in evidence["ports"].values()),
            "state_removed": False}
        shutil.rmtree(state)
        evidence["cleanup"]["state_removed"] = not state.exists()
        OUT.write_text(json.dumps(evidence, indent=2, sort_keys=True, default=str))
    return evidence


if __name__ == "__main__":
    asyncio.run(run())
