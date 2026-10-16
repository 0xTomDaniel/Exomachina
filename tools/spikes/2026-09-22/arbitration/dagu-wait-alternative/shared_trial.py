"""Shared reconciler recovery and ordering proof with real Dagu v2.17."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import auto_bridge
import bridge
import trial

HERE = Path(__file__).resolve().parent


def prepare(runtime: Path) -> dict:
    if runtime.exists():
        raise ValueError(f"runtime must be fresh: {runtime}")
    dags = runtime / "home" / "dags"
    dags.mkdir(parents=True)
    for template in (HERE / "shared" / "definitions").glob("*.yaml.in"):
        rendered = (template.read_text().replace("@PYTHON@", sys.executable)
                    .replace("@BRIDGE@", str(HERE / "auto_bridge.py"))
                    .replace("@RUNTIME@", str(runtime)))
        (dags / template.name.removesuffix(".in")).write_text(rendered)
    env = {**os.environ, "DAGU_HOME": str(runtime / "home"),
           "DAGU_AUTH_MODE": "none", "DAGU_COORDINATOR_ENABLED": "false"}
    for definition in dags.glob("*.yaml"):
        checked = subprocess.run([str(trial.DAGU), "validate", str(definition)], env=env,
                                 capture_output=True, text=True, timeout=15)
        if checked.returncode:
            raise RuntimeError(f"Dagu validation failed: {definition.name}: {checked.stderr}")
    registered = bridge.register(runtime)
    with auto_bridge.connect(runtime):
        pass
    return {"registered": registered,
            "definition_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                  for p in dags.glob("*.yaml")}}


def start_supervisor(runtime: Path, port: int) -> subprocess.Popen:
    (runtime / "base_url").write_text(f"http://127.0.0.1:{port}\n")
    with (runtime / "supervisor.log").open("ab") as stream:
        process = subprocess.Popen([sys.executable, "-B", str(HERE / "shared_supervisor.py"),
                                    str(runtime), "--dagu", str(trial.DAGU), "--port", str(port)],
                                   stdout=stream, stderr=stream, start_new_session=True)
    base = f"http://127.0.0.1:{port}"
    trial.until(lambda: process.poll() is None and trial.api(base, "/api/v1/dags"),
                "shared supervisor readiness", 30)
    return process


def stop_supervisor(process: subprocess.Popen | None) -> None:
    if process is None or process.poll() is not None:
        return
    os.kill(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        os.kill(process.pid, signal.SIGKILL)
        process.wait(timeout=3)


def accept(runtime: Path) -> dict:
    result = subprocess.run([sys.executable, "-B", str(HERE / "auto_bridge.py"),
                             "accept-child", str(runtime), "--token", bridge.FIXTURE_TOKEN],
                            capture_output=True, text=True, timeout=12)
    if result.returncode:
        raise RuntimeError(result.stderr[-1000:])
    return json.loads(result.stdout)


def state(runtime: Path) -> dict:
    with auto_bridge.connect(runtime) as db:
        return {"bridge": dict(db.execute("SELECT * FROM bridge WHERE parent_id=?",
                                          (bridge.PARENT_ID,)).fetchone()),
                "auto": dict(db.execute("SELECT * FROM auto_state WHERE parent_id=?",
                                        (bridge.PARENT_ID,)).fetchone())}


def events(runtime: Path) -> list[dict]:
    path = runtime / "shared_events.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def fault_recovery(runtime: Path) -> dict:
    info = prepare(runtime)
    port = trial.free_port()
    base = f"http://127.0.0.1:{port}"
    supervisor = None
    try:
        supervisor = start_supervisor(runtime, port)
        info["first_supervisor_pid"] = supervisor.pid
        info["parent_start"] = trial.api(base, f"/api/v1/dags/{bridge.PARENT_NAME}.yaml/start",
                                         {"dagRunId": bridge.PARENT_ID})
        info["child_start"] = trial.api(base, f"/api/v1/dags/{bridge.CHILD_NAME}.yaml/start",
                                        {"dagRunId": bridge.CHILD_ID})
        trial.until(lambda: trial.is_waiting(base, bridge.PARENT_NAME, bridge.PARENT_ID, "child_gate"),
                    "shared parent wait")
        trial.until(lambda: trial.is_waiting(base, bridge.CHILD_NAME, bridge.CHILD_ID, "director_wait"),
                    "shared child wait")
        info["both_waiting"] = {"parent": trial.native(base, bridge.PARENT_NAME, bridge.PARENT_ID),
                                "child": trial.native(base, bridge.CHILD_NAME, bridge.CHILD_ID),
                                "processes": trial.process_snapshot(runtime, int((runtime / "dagu_pid").read_text()))}
        (runtime / "pause_reconcile").write_text("pause scanner before parent gate ack\n")
        (runtime / "lose_ack_once").write_text("simulate loss after parent gate ack\n")
        info["authorized_child"] = accept(runtime)
        trial.until(lambda: trial.is_succeeded(base, bridge.CHILD_NAME, bridge.CHILD_ID,
                                               "child_outcome"), "native child terminal")
        trial.until(lambda: (runtime / "scanner_entered").exists(), "scanner before parent ack")
        info["before_kill"] = {"parent": trial.native(base, bridge.PARENT_NAME, bridge.PARENT_ID),
                               "child": trial.native(base, bridge.CHILD_NAME, bridge.CHILD_ID)}
        if info["before_kill"]["parent"]["status"] != "waiting":
            raise AssertionError("parent advanced before fault")
        old_dagu_pid = int((runtime / "dagu_pid").read_text())
        os.kill(supervisor.pid, signal.SIGKILL)
        os.kill(old_dagu_pid, signal.SIGKILL)
        supervisor.wait(timeout=5)
        (runtime / "pause_reconcile").unlink()
        info["killed"] = {"supervisor_pid": supervisor.pid, "dagu_pid": old_dagu_pid}
        supervisor = start_supervisor(runtime, port)
        info["replacement_supervisor_pid"] = supervisor.pid
        info["replacement_dagu_pid"] = int((runtime / "dagu_pid").read_text())
        trial.until(lambda: trial.is_succeeded(base, bridge.PARENT_NAME, bridge.PARENT_ID,
                                               "parent_continuation"), "automatic parent recovery", 45)
        trial.until(lambda: state(runtime)["bridge"]["state"] == "completed",
                    "bridge ack-loss recovery", 15)
        parent = bridge.status(base, bridge.PARENT_NAME, bridge.PARENT_ID)
        info["final"] = {"parent": trial.native(base, bridge.PARENT_NAME, bridge.PARENT_ID),
                         "child": trial.native(base, bridge.CHILD_NAME, bridge.CHILD_ID),
                         "parent_continuation_done_count": next(
                             item["doneCount"] for item in parent["nodes"]
                             if item["step"]["id"] == "parent_continuation"),
                         "state": state(runtime), "events": events(runtime)}
        info["outcome"] = "passed"
        return info
    finally:
        stop_supervisor(supervisor)


def child_before_parent(runtime: Path) -> dict:
    info = prepare(runtime)
    port = trial.free_port()
    base = f"http://127.0.0.1:{port}"
    supervisor = None
    try:
        supervisor = start_supervisor(runtime, port)
        info["child_start"] = trial.api(base, f"/api/v1/dags/{bridge.CHILD_NAME}.yaml/start",
                                        {"dagRunId": bridge.CHILD_ID})
        trial.until(lambda: trial.is_waiting(base, bridge.CHILD_NAME, bridge.CHILD_ID, "director_wait"),
                    "early child wait")
        info["authorized_child"] = accept(runtime)
        trial.until(lambda: trial.is_succeeded(base, bridge.CHILD_NAME, bridge.CHILD_ID,
                                               "child_outcome"), "early child terminal")
        info["child_terminal_before_parent_start"] = trial.native(
            base, bridge.CHILD_NAME, bridge.CHILD_ID)
        time.sleep(.7)
        info["parent_start"] = trial.api(base, f"/api/v1/dags/{bridge.PARENT_NAME}.yaml/start",
                                         {"dagRunId": bridge.PARENT_ID})
        trial.until(lambda: trial.is_succeeded(base, bridge.PARENT_NAME, bridge.PARENT_ID,
                                               "parent_continuation"), "late parent continuation", 30)
        info["final"] = {"parent": trial.native(base, bridge.PARENT_NAME, bridge.PARENT_ID),
                         "child": trial.native(base, bridge.CHILD_NAME, bridge.CHILD_ID),
                         "state": state(runtime), "events": events(runtime)}
        info["outcome"] = "passed"
        return info
    finally:
        stop_supervisor(supervisor)


def main() -> None:
    source_hash = {name: hashlib.sha256((HERE / name).read_bytes()).hexdigest()
                   for name in ("bridge.py", "auto_bridge.py", "shared_supervisor.py", "shared_trial.py")}
    evidence: dict = {"source_sha256_before": source_hash,
                      "dagu_binary_sha256": trial.DAGU_SHA256}
    try:
        evidence["fault_recovery"] = fault_recovery(Path("/tmp/exomachina-dagu-wait-shared-001"))
        evidence["child_before_parent"] = child_before_parent(Path("/tmp/exomachina-dagu-wait-shared-002"))
        evidence["outcome"] = "passed"
    except Exception as error:
        evidence["outcome"] = "failed"
        evidence["error"] = repr(error)
        raise
    finally:
        evidence["source_sha256_after"] = {name: hashlib.sha256((HERE / name).read_bytes()).hexdigest()
                                           for name in source_hash}
        (HERE / "shared_observed.json").write_text(json.dumps(evidence, sort_keys=True, indent=2) + "\n")
    print(json.dumps({"outcome": evidence["outcome"]}, sort_keys=True))


if __name__ == "__main__":
    main()
