"""Persisted Dagu human input with failed enqueue, crash, then same-run recovery."""
from __future__ import annotations

import json
import os
from pathlib import Path
import stat
import tempfile

from publisher import publish
import ops
from trial import (DAGU, HERE, ROOT, child, crash_bundle, frozen_hashes, native,
                   records, source_hashes, start, supervisor_start, supervisor_stop, task, until)


def bridge_events(rt: Path) -> list[dict]:
    path = rt / "bridge-events.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def run() -> dict:
    rt = Path(tempfile.mkdtemp(prefix="exo-dagu-queue-gap-", dir="/tmp"))
    os.environ["EXO_ARB_RUNTIME"] = str(rt)
    evidence = {"runtime": str(rt), "source_hashes_before": source_hashes(),
                "frozen_hashes_before": frozen_hashes()}
    supervisor = None
    queue_path = rt / "home" / "data" / "queue"
    original_mode = None
    try:
        evidence["manifest"] = publish(HERE / "definitions" / "v7", rt / "home", ROOT, DAGU)
        (rt / "pause-scan-before-gate").write_text("pause before native gate\n")
        supervisor = supervisor_start(rt)
        parent_id = "exo-arb-queue-gap-" + rt.name
        original_task = start(rt, parent_id, resolve=True)
        child_row = until(lambda: child(parent_id), "child binding", 35)
        until(lambda: (n := native(child_row["dag_name"], child_row["run_id"])) and
              n["status"] == "succeeded", "child terminal", 100)
        until(lambda: (rt / "scanner-entered").exists(), "scanner paused", 20)
        assert queue_path.is_dir()
        original_mode = stat.S_IMODE(queue_path.stat().st_mode)
        queue_path.chmod(0)
        (rt / "pause-scan-before-gate").unlink()
        gap_parent = until(lambda: (n := native(ROOT, parent_id)) and
                           n["status"] == "waiting" and
                           n["nodes"]["child_gate"]["status"] == "succeeded" and n,
                           "stored human input while queue unavailable", 20)
        evidence["gap_before_crash"] = {"task": task(rt, original_task["id"]),
            "parent": gap_parent, "child": native(child_row["dag_name"], child_row["run_id"]),
            "parent_row": ops.get_run(parent_id), "events": bridge_events(rt)}
        evidence["killed_service_pids"] = crash_bundle(rt, supervisor)
        supervisor = None
        queue_path.chmod(original_mode)
        original_mode = None
        supervisor = supervisor_start(rt)
        final_parent = until(lambda: (n := native(ROOT, parent_id)) and
                             n["status"] == "succeeded" and n,
                             "parent resumed from stored input", 80)
        until(lambda: ops.get_run(parent_id)["bridge_state"] == "completed",
              "bridge reconciled", 20)
        final_task = task(rt, original_task["id"])
        evidence["after_restart"] = {"task": final_task, "parent": final_parent,
            "child": native(child_row["dag_name"], child_row["run_id"]),
            "parent_row": ops.get_run(parent_id), "child_row": ops.get_run(child_row["run_id"]),
            "events": bridge_events(rt)}
        assert final_task["id"] == original_task["id"]
        assert final_task["status"]["state"] == "completed"
        assert final_parent["nodes"]["child_gate"]["done_count"] == 1
        assert final_parent["nodes"]["public_result"]["done_count"] == 1
        assert evidence["after_restart"]["child_row"]["acceptance_count"] == 1
        assert evidence["after_restart"]["child_row"]["release_count"] == 1
        assert evidence["after_restart"]["parent_row"]["definition_digest"] == evidence["manifest"]["closure_sha256"]
        assert evidence["after_restart"]["child_row"]["definition_digest"] == evidence["manifest"]["closure_sha256"]
        assert any(e["kind"] == "parent_resume_posted" for e in bridge_events(rt))
        evidence["status"] = "passed"
    except Exception as error:
        evidence["status"] = "failed"
        evidence["error"] = repr(error)
        raise
    finally:
        if original_mode is not None and queue_path.exists():
            queue_path.chmod(original_mode)
        supervisor_stop(rt, supervisor)
        evidence["runs"] = records(rt, "runs")
        evidence["releases"] = records(rt, "releases")
        evidence["bridge_events"] = bridge_events(rt)
        evidence["source_hashes_after"] = source_hashes()
        evidence["frozen_hashes_after"] = frozen_hashes()
        (HERE / "queue_observed.json").write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    return evidence


if __name__ == "__main__":
    result = run()
    print(json.dumps({"status": result["status"], "runtime": result["runtime"]}, sort_keys=True))
