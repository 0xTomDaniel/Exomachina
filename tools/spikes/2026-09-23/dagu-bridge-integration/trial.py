"""Director-routed end-to-end processless Dagu bridge fault trial."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time

from director_client import get as get_task, send as send_director
import ops
from publisher import publish

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
PRIOR = HERE.parent.parent / "2026-09-22"
PYTHON = PRIOR / "s2" / ".venv" / "bin" / "python"
DAGU = Path("/tmp/exomachina-countertrials/dagu/dagu")
ROOT = "exo_arb_parent_v6"


def until(check, label: str, seconds: float = 90):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (OSError, ValueError, KeyError, sqlite3.OperationalError) as error:
            last = repr(error)
        time.sleep(.2)
    raise TimeoutError(f"{label}: {last}")


def records(rt: Path, table: str) -> list[dict]:
    path = rt / "ledger.sqlite"
    if not path.exists():
        return []
    with sqlite3.connect(path) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute(f"SELECT * FROM {table}")]


def child(parent_id: str) -> dict | None:
    with ops.db_connect() as db:
        row = db.execute("SELECT * FROM runs WHERE parent_id=?", (parent_id,)).fetchone()
    return dict(row) if row else None


def native(name: str, run_id: str) -> dict | None:
    status = ops.engine_status(name, run_id)
    if status is None:
        return None
    return {"id": run_id, "status": status["statusLabel"],
            "nodes": {item["step"]["id"]: {"status": item["statusLabel"],
                "done_count": item["doneCount"], "retry_count": item["retryCount"]}
                for item in status["nodes"]}}


def start(rt: Path, run_id: str, *, resolve: bool) -> dict:
    config = json.loads((rt / "config.json").read_text())
    return send_director(config["urls"]["director"], {"op": "start", "key": "start:" + run_id,
        "run_id": run_id, "root_name": ROOT, "definition_digest": ops.manifest(ROOT)["closure_sha256"],
        "resolve_input": resolve, "release_mode": "participating"})


def task(rt: Path, task_id: str) -> dict:
    config = json.loads((rt / "config.json").read_text())
    return get_task(config["urls"]["director"], task_id)


def supervisor_start(rt: Path) -> subprocess.Popen:
    with (rt / "supervisor.log").open("ab") as stream:
        process = subprocess.Popen([str(PYTHON), "-B", str(HERE / "supervisor.py"), str(rt),
            "--dagu", str(DAGU)], stdout=stream, stderr=stream, start_new_session=True)
    def ready():
        if process.poll() is not None:
            raise RuntimeError(f"supervisor exited {process.returncode}")
        if not (rt / "ready").exists():
            return False
        config = json.loads((rt / "config.json").read_text())
        return bool(ops.http_json(config["urls"]["dagu"] + "/api/v1/dags")
                    and ops.http_json(config["urls"]["director"] + "/health"))
    until(ready, "full bundle ready", 50)
    return process


def supervisor_stop(rt: Path, process: subprocess.Popen | None) -> None:
    if process is None or process.poll() is not None:
        return
    process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def crash_bundle(rt: Path, process: subprocess.Popen) -> dict:
    registry = json.loads((rt / "processes.json").read_text())
    process.kill()
    process.wait(timeout=5)
    for info in registry.values():
        try:
            os.killpg(info["pid"], signal.SIGKILL)
        except ProcessLookupError:
            pass
    for info in registry.values():
        until(lambda info=info: not Path(f"/proc/{info['pid']}").exists() if sys.platform == "linux" else
              subprocess.run(["kill", "-0", str(info["pid"])], capture_output=True).returncode != 0,
              "old service exit", 8)
    return {name: info["pid"] for name, info in registry.items()}


def source_hashes() -> dict:
    return {name: hashlib.sha256((HERE / name).read_bytes()).hexdigest() for name in
            ("ops.py", "publisher.py", "director_server.py", "director_client.py",
             "supervisor.py", "reconciler.py", "bridge.py", "trial.py")}


def frozen_hashes() -> dict:
    fixture = PRIOR / "arbitration" / "dagu"
    frozen = json.loads((fixture / "freeze.json").read_text())
    return {name: {"expected": value["sha256"],
                   "actual": hashlib.sha256((REPO / name).read_bytes()).hexdigest()}
            for name, value in frozen["files"].items()}


def run() -> dict:
    if not PYTHON.is_file() or not DAGU.is_file():
        raise RuntimeError("pinned S2 Python or Dagu binary missing")
    rt = Path(tempfile.mkdtemp(prefix="exo-dagu-bridge-integration-", dir="/tmp"))
    os.environ["EXO_ARB_RUNTIME"] = str(rt)
    evidence: dict = {"runtime": str(rt), "source_hashes_before": source_hashes(),
                      "frozen_hashes_before": frozen_hashes(), "events": []}
    supervisor = None
    try:
        if any(row["expected"] != row["actual"] for row in evidence["frozen_hashes_before"].values()):
            raise ValueError("frozen Dagu source changed before trial")
        evidence["manifest"] = publish(HERE / "definitions" / "v6", rt / "home", ROOT, DAGU)
        (rt / "pause-scan-before-gate").write_text("pause before native gate\n")
        (rt / "fault-gate-ack-loss-once").write_text("one lost scanner acknowledgement\n")
        supervisor = supervisor_start(rt)
        accepted_parent = "exo-arb-bridge-accepted-" + rt.name
        accepted_task = start(rt, accepted_parent, resolve=True)
        accepted_child = until(lambda: child(accepted_parent), "accepted child binding", 35)
        until(lambda: (n := native(ROOT, accepted_parent)) and
              n["nodes"]["child_gate"]["status"] == "waiting", "native parent wait", 40)
        until(lambda: (n := native(accepted_child["dag_name"], accepted_child["run_id"])) and
              n["status"] == "succeeded", "accepted child native terminal", 100)
        until(lambda: (rt / "scanner-entered").exists(), "shared scanner paused", 20)
        evidence["accepted_wait_before_restart"] = {
            "task_id": accepted_task["id"], "task": task(rt, accepted_task["id"]),
            "parent": native(ROOT, accepted_parent),
            "child": native(accepted_child["dag_name"], accepted_child["run_id"]),
            "child_row": ops.get_run(accepted_child["run_id"]),
            "parent_row": ops.get_run(accepted_parent)}
        evidence["killed_service_pids"] = crash_bundle(rt, supervisor)
        supervisor = None
        (rt / "pause-scan-before-gate").unlink()
        supervisor = supervisor_start(rt)
        until(lambda: (n := native(ROOT, accepted_parent)) and n["status"] == "succeeded",
              "accepted parent continuation after restart", 75)
        until(lambda: ops.get_run(accepted_parent)["bridge_state"] == "completed",
              "gate ack loss reconciled", 25)
        accepted_final_task = task(rt, accepted_task["id"])
        evidence["accepted_final"] = {"task": accepted_final_task,
            "parent": native(ROOT, accepted_parent),
            "child": native(accepted_child["dag_name"], accepted_child["run_id"]),
            "parent_row": ops.get_run(accepted_parent),
            "child_row": ops.get_run(accepted_child["run_id"])}
        assert accepted_final_task["id"] == accepted_task["id"]
        assert accepted_final_task["status"]["state"] == "completed"
        assert evidence["accepted_final"]["parent_row"]["state"] == "released"
        assert evidence["accepted_final"]["parent"]["nodes"]["public_result"]["done_count"] == 1
        exhausted_parent = "exo-arb-bridge-aborted-" + rt.name
        aborted_task = start(rt, exhausted_parent, resolve=False)
        aborted_child = until(lambda: child(exhausted_parent), "abort child binding", 35)
        until(lambda: (n := native(aborted_child["dag_name"], aborted_child["run_id"])) and
              n["nodes"]["director_wait"]["status"] == "waiting", "child Director wait", 100)
        until(lambda: (n := native(ROOT, exhausted_parent)) and
              n["nodes"]["child_gate"]["status"] == "waiting", "abort parent native wait", 25)
        evidence["abort_wait"] = {"task_id": aborted_task["id"],
            "task": task(rt, aborted_task["id"]), "parent": native(ROOT, exhausted_parent),
            "child": native(aborted_child["dag_name"], aborted_child["run_id"])}
        evidence["nested_wait_killed_service_pids"] = crash_bundle(rt, supervisor)
        supervisor = None
        supervisor = supervisor_start(rt)
        until(lambda: (p := native(ROOT, exhausted_parent)) and
              (c := native(aborted_child["dag_name"], aborted_child["run_id"])) and
              p["nodes"]["child_gate"]["status"] == "waiting" and
              c["nodes"]["director_wait"]["status"] == "waiting",
              "same nested native waits after restart", 45)
        evidence["abort_wait_after_restart"] = {"task": task(rt, aborted_task["id"]),
            "parent": native(ROOT, exhausted_parent),
            "child": native(aborted_child["dag_name"], aborted_child["run_id"])}
        assert evidence["abort_wait_after_restart"]["task"]["id"] == aborted_task["id"]
        child_now = ops.get_run(aborted_child["run_id"])
        config = json.loads((rt / "config.json").read_text())
        decision = send_director(config["urls"]["director"], {"op": "abort",
            "key": "abort:" + aborted_child["run_id"], "run_id": aborted_child["run_id"],
            "revision": "r3", "owner_epoch": child_now["owner_epoch"],
            "token": "fixture-director-token", "expires_at": time.time() + 90})
        until(lambda: (n := native(ROOT, exhausted_parent)) and n["status"] == "succeeded",
              "authorized abort parent continuation", 70)
        until(lambda: ops.get_run(exhausted_parent)["bridge_state"] == "completed",
              "abort bridge completion", 20)
        aborted_final_task = task(rt, aborted_task["id"])
        evidence["aborted_final"] = {"decision_task": decision, "task": aborted_final_task,
            "parent": native(ROOT, exhausted_parent),
            "child": native(aborted_child["dag_name"], aborted_child["run_id"]),
            "parent_row": ops.get_run(exhausted_parent),
            "child_row": ops.get_run(aborted_child["run_id"])}
        assert aborted_final_task["id"] == aborted_task["id"]
        assert aborted_final_task["status"]["state"] == "completed"
        assert evidence["aborted_final"]["parent_row"]["state"] == "aborted"
        assert evidence["aborted_final"]["child_row"]["acceptance_count"] == 0
        assert evidence["aborted_final"]["child_row"]["release_count"] == 0
        assert evidence["aborted_final"]["parent"]["nodes"]["public_result"]["done_count"] == 1
        # Persistent pre-terminal child failure must not release the parent gate.
        (rt / "fault-child-before-terminal").write_text("r1 Quality failure\n")
        failed_parent = "exo-arb-bridge-failed-" + rt.name
        failed_task = start(rt, failed_parent, resolve=True)
        failed_child = until(lambda: child(failed_parent), "failed child binding", 35)
        until(lambda: (n := native(failed_child["dag_name"], failed_child["run_id"])) and
              n["status"] == "failed", "native child failed", 80)
        time.sleep(4)
        evidence["failed_child"] = {"task": task(rt, failed_task["id"]),
            "parent": native(ROOT, failed_parent),
            "child": native(failed_child["dag_name"], failed_child["run_id"]),
            "parent_row": ops.get_run(failed_parent),
            "child_row": ops.get_run(failed_child["run_id"])}
        assert evidence["failed_child"]["parent"]["nodes"]["child_gate"]["status"] == "waiting"
        assert evidence["failed_child"]["parent_row"]["bridge_state"] == "waiting"
        evidence["status"] = "passed"
    except Exception as error:
        evidence["status"] = "failed"
        evidence["error"] = repr(error)
        raise
    finally:
        supervisor_stop(rt, supervisor)
        evidence["runs"] = records(rt, "runs")
        evidence["verdicts"] = records(rt, "verdicts")
        evidence["releases"] = records(rt, "releases")
        evidence["director_commands"] = records(rt, "director_commands")
        evidence["bridge_events"] = [json.loads(line) for line in (rt / "bridge-events.jsonl").read_text().splitlines()] if (rt / "bridge-events.jsonl").exists() else []
        evidence["source_hashes_after"] = source_hashes()
        evidence["frozen_hashes_after"] = frozen_hashes()
        (HERE / "observed.json").write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    return evidence


if __name__ == "__main__":
    result = run()
    print(json.dumps({"status": result["status"], "runtime": result["runtime"]}, sort_keys=True))
