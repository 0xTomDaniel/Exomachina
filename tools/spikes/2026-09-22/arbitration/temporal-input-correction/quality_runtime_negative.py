#!/usr/bin/env python3
"""Post-freeze probe-only stale and forged Quality identity observations."""
from __future__ import annotations

import asyncio
import json
import signal
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

from temporalio.client import Client

import runtime
from adapter import _identity, a2a
from author import materialize, template
from definition import publish
from factory import FactoryRun
from hidden_trial import launch_supervisor, ready, rows, verify_freeze
from probe import SERVICE_PORTS, S2_PYTHON, bindings, input_for

HERE = Path(__file__).resolve().parent


async def current_r2(client, run):
    async def check():
        try:
            parent = await client.get_workflow_handle(run).query(FactoryRun.status)
            if not parent["child_id"]:
                return False
            child = await client.get_workflow_handle(parent["child_id"]).query(FactoryRun.status)
            return child if child["phase"] == "quality" and child["current_revision"] == "r2" else False
        except Exception:
            return False
    return await runtime.until(check, seconds=75)


async def main():
    freeze_before = verify_freeze()
    state = Path(tempfile.mkdtemp(prefix="exo-temporal-quality-neg-", dir="/tmp"))
    runtime.STATE = state
    observed = {"status": "running", "state": str(state), "freeze_before": freeze_before}
    supervisor = launch_supervisor(state, "quality-negative")
    alternate = None
    try:
        await ready(supervisor, state)
        client = await Client.connect(f"127.0.0.1:{runtime.PORTS['frontend']}", namespace="exomachina")
        identities = {}
        for name in ("source", "counter", "quality", "release"):
            with urllib.request.urlopen(f"http://127.0.0.1:{SERVICE_PORTS[name]}/health") as response:
                identities[name] = json.load(response)
        approved = bindings(identities)
        (state / "catalog" / "approved_bindings.json").write_text(json.dumps(approved, sort_keys=True))
        package = materialize(template("hidden-v4-requires-scope.json"), approved)
        observed["package_digest"] = publish(package, state / "catalog", approved)
        run = "quality-negative-live-r2"
        await client.start_workflow(FactoryRun.run,
            input_for(run, package, {"identity": "quality-neg-director", "token": "quality-neg-token", "epoch": 1},
                      {"quality_barrier": {"marker": str(state / "quality-remote-commit"),
                                           "hold_seconds": 12}}),
            id=run, task_queue="arbitration-temporal")
        before = await current_r2(client, run)
        assert before["authoritative_acceptance"] is None
        child_run = before["run"]
        branch_rows = rows(state, "source", child_run) + rows(state, "counter", child_run)
        receipts = {row["action_id"].rsplit(":", 1)[-1]: row for row in branch_rows}
        child_def = template("hidden-v4-requires-scope.json")["child"]
        branches = child_def["nodes"]["gather"]["branches"]
        sys.path.insert(0, str(HERE.parent / "common"))
        import fixture
        joined = fixture.typed_join(receipts, run_id=child_run,
            definition_digest=before["definition_digest"],
            declarations={name: branch["result_type"] for name, branch in branches.items()},
            scope_status_by_instance={name: branch["scope_status"] for name, branch in branches.items()
                                      if branch["scope_status"] is not None})
        stale_candidate = fixture.candidate_artifact(joined, "r1", f"factory:{child_run}", resolved=True)
        assert stale_candidate["sha256"] != before["current_sha256"]
        stale_command = {"op": "review", "action_id": f"{child_run}:injected-stale-r1-positive",
            "run_id": child_run, "definition_digest": before["definition_digest"],
            "artifact": stale_candidate}
        quality_url = approved["quality"]["url"]
        stale_positive = await asyncio.to_thread(a2a.send, quality_url, stale_command)
        assert stale_positive["artifact"]["accepted"] is True
        observed["stale_positive"] = stale_positive

        alternate_port = 35568
        alternate_url = f"http://127.0.0.1:{alternate_port}"
        log = (state / "forged-quality.log").open("a")
        alternate = subprocess.Popen([str(S2_PYTHON), str(HERE.parent / "common" / "quality_server.py"),
            "--state", str(state / "forged-quality"), "--port", str(alternate_port)],
            cwd=HERE, stdout=log, stderr=log, start_new_session=True)
        log.close()
        async def alt_health():
            try:
                with urllib.request.urlopen(alternate_url + "/health", timeout=1) as response:
                    return json.load(response)
            except Exception:
                return False
        forged_health = await runtime.until(alt_health, seconds=30)
        assert forged_health["identity"] != identities["quality"]["identity"]
        forged_command = {**stale_command,
            "action_id": f"{child_run}:injected-forged-quality-r1-positive"}
        forged_positive = await asyncio.to_thread(a2a.send, alternate_url, forged_command)
        assert forged_positive["artifact"]["accepted"] is True
        try:
            await asyncio.to_thread(_identity, alternate_url, "quality", identities["quality"]["identity"])
            raise AssertionError("forged Quality identity admitted")
        except ValueError as error:
            identity_guard = str(error)
        after_injection = await client.get_workflow_handle(child_run).query(FactoryRun.status)
        assert after_injection["current_revision"] == "r2"
        assert after_injection["authoritative_acceptance"] is None
        result = await asyncio.wait_for(client.get_workflow_handle(run).result(), timeout=90)
        assert result["child"]["acceptance"]["revision"] == "r2"
        assert result["child"]["acceptance"]["sha256"] == before["current_sha256"]
        observed.update({"status": "partial", "quality_identity": identities["quality"],
            "forged_health": forged_health, "forged_positive": forged_positive,
            "identity_guard": identity_guard, "before": before,
            "after_injection": after_injection, "final": result,
            "quality_receiver_rows": rows(state, "quality", child_run),
            "forged_receiver_rows": rows(state, "forged-quality", child_run),
            "freeze_after": verify_freeze(),
            "limit": "Extra Quality A2A reviews are out-of-band: the frozen Workflow has no Quality verdict ingress; direct adapter identity guard is separate from the active Activity."})
        assert observed["freeze_after"] == freeze_before
    except Exception as error:
        observed["status"] = "failed"
        observed["error"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        (HERE / "quality-negative-observed.json").write_text(json.dumps(observed, indent=2, sort_keys=True) + "\n")
        if alternate is not None:
            runtime.kill(alternate, signal.SIGTERM)
        if supervisor.poll() is None:
            supervisor.send_signal(signal.SIGTERM)
            supervisor.wait(timeout=20)
        print(json.dumps({"status": observed["status"], "state": str(state)}))


if __name__ == "__main__":
    asyncio.run(main())
