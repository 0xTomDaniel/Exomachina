"""Original Director A2A Task across post-remote-Quality and post-acceptance crashes."""
from __future__ import annotations

import asyncio
import hashlib
import json
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

from temporalio.client import Client

import runtime
from author import materialize, template
from definition import publish
from factory import FactoryRun
from fault_probe import kill_engine_pg, wait_engine_restarted
from probe import SERVICE_PORTS, bindings, director_send, director_task

HERE = Path(__file__).resolve().parent
ORIGINAL = HERE.parent.parent / "2026-09-22" / "arbitration" / "temporal"


def source_hashes() -> dict:
    names = ("adapter.py", "author.py", "definition.py", "director_server.py", "factory.py",
             "probe.py", "runtime.py", "supervisor.py", "worker.py")
    return {name: hashlib.sha256((HERE / name).read_bytes()).hexdigest() for name in names}


def frozen_original_unchanged() -> bool:
    inventory = json.loads((ORIGINAL / "freeze.json").read_text())
    repo = HERE.parents[3]
    return all(hashlib.sha256((repo / name).read_bytes()).hexdigest() == entry["sha256"]
               for name, entry in inventory["files"].items())


def marker(state: Path, run_id: str) -> Path:
    suffix = hashlib.sha256(run_id.encode()).hexdigest()[:16]
    return state / f"quality-committed-{suffix}"


def receiver_rows(path: Path, sql: str, run_id: str) -> list[dict]:
    with sqlite3.connect(path) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute(sql, (run_id,))]


def exact_counts(state: Path, child_id: str, result: dict) -> dict:
    source = receiver_rows(state / "source" / "harness.sqlite3",
        "SELECT action_id,task_id,attempts,accepted_count FROM actions WHERE run_id=?", child_id)
    counter = receiver_rows(state / "counter" / "harness.sqlite3",
        "SELECT action_id,task_id,attempts,accepted_count FROM actions WHERE run_id=?", child_id)
    quality = receiver_rows(state / "quality" / "harness.sqlite3",
        "SELECT action_id,task_id,attempts,accepted_count,artifact FROM actions WHERE run_id=?", child_id)
    release = receiver_rows(state / "release" / "release.sqlite3",
        "SELECT release_id,attempts,accepted_effect_count FROM releases WHERE run_id=?", child_id)
    verdicts = [json.loads(row["artifact"]) for row in quality]
    return {"source": source, "counter": counter, "quality": quality,
            "quality_negative": sum(v["accepted"] is False for v in verdicts),
            "quality_positive": sum(v["accepted"] is True for v in verdicts),
            "authoritative_acceptance": 1 if result["child"].get("acceptance") else 0,
            "accepted_revision": result["child"].get("acceptance", {}).get("revision"),
            "release": release, "release_effects": sum(r["accepted_effect_count"] for r in release)}


async def launch(state: Path) -> subprocess.Popen:
    with (state / "supervisor.log").open("a") as log:
        proc = subprocess.Popen([sys.executable, str(HERE / "supervisor.py"),
            "--state", str(state)], cwd=HERE, stdout=log, stderr=log, start_new_session=True)
    async def ready():
        if proc.poll() is not None:
            raise RuntimeError(f"supervisor exited {proc.returncode}")
        return (state / "supervisor-ready").exists()
    await runtime.until(ready, seconds=120)
    return proc


async def child_phase(client: Client, parent_id: str, phase: str):
    async def check():
        try:
            parent = await client.get_workflow_handle(parent_id).query(FactoryRun.status)
            if not parent["child_id"]:
                return False
            child = await client.get_workflow_handle(parent["child_id"]).query(FactoryRun.status)
            return (parent, child) if child["phase"] == phase else False
        except Exception:
            return False
    return await runtime.until(check, seconds=90)


async def run() -> dict:
    state = Path(tempfile.mkdtemp(prefix="exo-temporal-recovery-scale-", dir="/tmp"))
    runtime.STATE = state
    evidence = {"status": "running", "state": str(state),
                "source_hashes_before": source_hashes(),
                "original_frozen_unchanged_before": frozen_original_unchanged(), "cases": {}}
    supervisor = None
    try:
        occupied = [port for port in list(runtime.PORTS.values()) + list(SERVICE_PORTS.values())
                    if runtime.port_open(port)]
        if occupied:
            raise RuntimeError(f"Temporal fixture ports occupied: {occupied}")
        assert max(runtime.PORTS[key] for key in ("front_member", "match_member", "history_member", "worker_member")) < 32768
        assert evidence["original_frozen_unchanged_before"]
        supervisor = await launch(state)
        client = await Client.connect(f"127.0.0.1:{runtime.PORTS['frontend']}", namespace="exomachina")
        identities = {}
        for name in ("source", "counter", "quality", "release"):
            with urllib.request.urlopen(f"http://127.0.0.1:{SERVICE_PORTS[name]}/health") as response:
                identities[name] = json.load(response)
        approved = bindings(identities)
        (state / "catalog" / "approved_bindings.json").write_text(json.dumps(approved, sort_keys=True))
        package = materialize(template("visible-v3.json"), approved)
        package_digest = publish(package, state / "catalog", approved)
        evidence["package_digest"] = package_digest
        evidence["child_definition_digest"] = next(iter(package["children"]))

        for name, profile in (("quality_commit", "quality_committed"),
                              ("acceptance", "acceptance_timer")):
            parent_id = "temporal-a2a-" + name + "-" + state.name
            started = await asyncio.to_thread(director_send, {"op": "start",
                "action_id": "start:" + parent_id, "run_id": parent_id,
                "package_digest": package_digest, "trial_fault_profile": profile})
            assert started["kind"] == "task"
            if name == "quality_commit":
                await runtime.until(lambda: asyncio.sleep(0, result=marker(state, parent_id).exists()),
                                    seconds=70)
                before_parent = await client.get_workflow_handle(parent_id).query(FactoryRun.status)
                before_child = await client.get_workflow_handle(before_parent["child_id"]).query(FactoryRun.status)
                before_quality = receiver_rows(state / "quality" / "harness.sqlite3",
                    "SELECT action_id,task_id,artifact FROM actions WHERE run_id=?", before_parent["child_id"])
                assert len(before_quality) == 1 and json.loads(before_quality[0]["artifact"])["accepted"] is False
            else:
                before_parent, before_child = await child_phase(client, parent_id, "accepted-before-release")
                before_quality = receiver_rows(state / "quality" / "harness.sqlite3",
                    "SELECT action_id,task_id,artifact FROM actions WHERE run_id=?", before_parent["child_id"])
                assert before_child["authoritative_acceptance"] is not None
                assert before_child["release_receipt"] is None
            before_task = await asyncio.to_thread(director_task, started["id"])
            stopped = kill_engine_pg(state)
            client = await wait_engine_restarted(state)
            result = await asyncio.wait_for(client.get_workflow_handle(parent_id).result(), timeout=120)
            after_task = await asyncio.to_thread(director_task, started["id"])
            after_parent = await client.get_workflow_handle(parent_id).query(FactoryRun.status)
            after_child = await client.get_workflow_handle(before_parent["child_id"]).query(FactoryRun.status)
            counts = exact_counts(state, before_parent["child_id"], result)
            assert after_task["id"] == started["id"] and after_task["status"]["state"] == "completed"
            assert result["status"] == "accepted" and result["child"]["acceptance"]["revision"] == "r2"
            assert counts["authoritative_acceptance"] == counts["quality_negative"] == counts["quality_positive"] == 1
            assert len(counts["source"]) == len(counts["counter"]) == len(counts["release"]) == 1
            assert counts["release_effects"] == 1
            evidence["cases"][name] = {"parent_id": parent_id, "child_id": before_parent["child_id"],
                "original_task_id": started["id"], "task_before": before_task, "task_after": after_task,
                "parent_before": before_parent, "child_before": before_child,
                "quality_before_crash": before_quality, "stopped": stopped,
                "parent_after": after_parent, "child_after": after_child,
                "result": result, "counts": counts}
        evidence["source_hashes_after"] = source_hashes()
        evidence["original_frozen_unchanged_after"] = frozen_original_unchanged()
        assert evidence["source_hashes_after"] == evidence["source_hashes_before"]
        assert evidence["original_frozen_unchanged_after"]
        evidence["status"] = "passed"
    except Exception as error:
        evidence["status"] = "failed"
        evidence["error"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        if supervisor and supervisor.poll() is None:
            supervisor.send_signal(signal.SIGTERM)
            supervisor.wait(timeout=30)
        (HERE / "recovery_observed.json").write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    return evidence


if __name__ == "__main__":
    value = asyncio.run(run())
    print(json.dumps({"status": value["status"], "state": value["state"]}, sort_keys=True))
