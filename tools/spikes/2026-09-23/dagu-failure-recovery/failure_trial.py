"""End-to-end failed-child propagation through the original Director A2A Task."""
from __future__ import annotations

import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile

from publisher import publish
import ops
from trial import (DAGU, HERE, ROOT, child, crash_bundle, frozen_hashes, native,
                   records, source_hashes, start, supervisor_start, supervisor_stop, task, until)


def run() -> dict:
    rt = Path(tempfile.mkdtemp(prefix="exo-dagu-failure-recovery-", dir="/tmp"))
    os.environ["EXO_ARB_RUNTIME"] = str(rt)
    evidence = {"runtime": str(rt), "source_hashes_before": source_hashes(),
                "frozen_hashes_before": frozen_hashes()}
    supervisor = None
    try:
        assert all(v["expected"] == v["actual"] for v in evidence["frozen_hashes_before"].values())
        evidence["manifest"] = publish(HERE / "definitions" / "v7", rt / "home", ROOT, DAGU)
        (rt / "fault-child-before-terminal").write_text("r1 Quality failure\n")
        supervisor = supervisor_start(rt)
        parent_id = "exo-arb-failure-" + rt.name
        original_task = start(rt, parent_id, resolve=True)
        child_row = until(lambda: child(parent_id), "child binding", 35)
        until(lambda: (n := native(child_row["dag_name"], child_row["run_id"])) and
              n["status"] == "failed", "child native failure", 100)
        final_parent = until(lambda: (n := native(ROOT, parent_id)) and
                             n["status"] == "failed" and n, "parent native failed", 100)
        final_task = until(lambda: (t := task(rt, original_task["id"])) and
                           t["status"]["state"] == "failed" and t,
                           "original Director Task failed", 20)
        until(lambda: ops.get_run(parent_id)["bridge_state"] == "failed",
              "bridge failure reconciled", 20)
        with sqlite3.connect(rt / "ledger.sqlite") as db:
            retries = [dict(zip(("run_id", "step_id", "attempts", "last_at", "completed_at"), row)) for row in db.execute(
                "SELECT run_id,step_id,attempts,last_at,completed_at FROM engine_retries")]
        evidence["before_restart"] = {
            "task_id": original_task["id"], "task": final_task,
            "parent": final_parent, "parent_row": ops.get_run(parent_id),
            "child": native(child_row["dag_name"], child_row["run_id"]),
            "child_row": ops.get_run(child_row["run_id"]), "retries": retries}
        assert final_task["id"] == original_task["id"]
        assert final_task.get("artifacts") in (None, [])
        assert final_parent["nodes"]["child_gate"]["status"] == "succeeded"
        assert final_parent["nodes"]["public_result"]["status"] == "failed"
        assert evidence["before_restart"]["parent_row"]["bridge_state"] == "failed"
        assert evidence["before_restart"]["child_row"]["acceptance_count"] == 0
        assert evidence["before_restart"]["child_row"]["release_count"] == 0
        assert evidence["before_restart"]["child_row"]["state"] == "failed"
        assert json.loads(evidence["before_restart"]["child_row"]["failure_json"])["failed_step"] == "quality_r1"
        assert evidence["before_restart"]["child_row"]["accepted_revision"] is None
        assert evidence["before_restart"]["child_row"]["receipt_json"] is None
        assert any(r["run_id"] == child_row["run_id"] and r["step_id"] == "quality_r1"
                   and r["attempts"] == 2 and r["completed_at"] is not None for r in retries)

        evidence["killed_service_pids"] = crash_bundle(rt, supervisor)
        supervisor = None
        supervisor = supervisor_start(rt)
        after = task(rt, original_task["id"])
        evidence["after_restart"] = {"task": after, "parent": native(ROOT, parent_id),
            "child": native(child_row["dag_name"], child_row["run_id"]),
            "parent_row": ops.get_run(parent_id), "child_row": ops.get_run(child_row["run_id"])}
        assert after["id"] == original_task["id"] and after["status"]["state"] == "failed"
        assert evidence["after_restart"]["parent"]["status"] == "failed"
        assert evidence["after_restart"]["child"]["status"] == "failed"
        assert evidence["after_restart"]["child_row"]["state"] == "failed"
        assert evidence["after_restart"]["parent_row"]["definition_digest"] == evidence["manifest"]["closure_sha256"]
        assert evidence["after_restart"]["child_row"]["definition_digest"] == evidence["manifest"]["closure_sha256"]
        (rt / "fault-child-before-terminal").unlink()
        denied = subprocess.run([str(DAGU), "retry", "--run-id", child_row["run_id"],
            "--step", "quality_r1", "--downstream", child_row["dag_name"]],
            env={**os.environ, "DAGU_HOME": str(rt / "home"), "DAGU_AUTH_MODE": "none",
                 "PATH": str(rt / "bin") + os.pathsep + os.environ["PATH"]},
            capture_output=True, text=True, timeout=35)
        evidence["unauthorized_same_run_retry"] = {"returncode": denied.returncode,
            "stderr": denied.stderr[-600:], "task": task(rt, original_task["id"]),
            "child_row": ops.get_run(child_row["run_id"])}
        assert denied.returncode != 0
        assert "failed product run is sealed" in denied.stderr
        assert evidence["unauthorized_same_run_retry"]["task"]["status"]["state"] == "failed"
        assert evidence["unauthorized_same_run_retry"]["child_row"]["state"] == "failed"
        assert evidence["unauthorized_same_run_retry"]["child_row"]["acceptance_count"] == 0
        assert evidence["unauthorized_same_run_retry"]["child_row"]["release_count"] == 0
        evidence["status"] = "passed"
    except Exception as error:
        evidence["status"] = "failed"
        evidence["error"] = repr(error)
        raise
    finally:
        supervisor_stop(rt, supervisor)
        evidence["runs"] = records(rt, "runs")
        evidence["releases"] = records(rt, "releases")
        events = rt / "bridge-events.jsonl"
        evidence["bridge_events"] = [json.loads(line) for line in events.read_text().splitlines()] if events.exists() else []
        evidence["source_hashes_after"] = source_hashes()
        evidence["frozen_hashes_after"] = frozen_hashes()
        (HERE / "failure_observed.json").write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    return evidence


if __name__ == "__main__":
    result = run()
    print(json.dumps({"status": result["status"], "runtime": result["runtime"]}, sort_keys=True))
