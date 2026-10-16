"""Automatic child-final-step bridge and injected restart recovery trial."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys

import auto_bridge
import bridge
import trial

HERE = Path(__file__).resolve().parent
RUNTIME = Path("/tmp/exomachina-dagu-wait-auto-001")


def command(operation: str, *options: str, success: bool = True) -> dict:
    run = subprocess.run([sys.executable, "-B", str(HERE / "auto_bridge.py"), operation,
                          str(RUNTIME), *options], capture_output=True, text=True, timeout=12)
    if success and run.returncode:
        raise RuntimeError(f"auto bridge {operation} failed: {run.stderr[-1000:]}")
    if not success and not run.returncode:
        raise AssertionError(f"auto bridge {operation} unexpectedly succeeded")
    return json.loads(run.stdout) if success else {"returncode": run.returncode,
                                                    "error": run.stderr.strip().splitlines()[-1]}


def node(detail: dict, step: str) -> dict:
    return next(item for item in detail["nodes"] if item["step"]["id"] == step)


def main() -> None:
    if RUNTIME.exists():
        raise SystemExit(f"runtime must be fresh: {RUNTIME}")
    if hashlib.sha256(trial.DAGU.read_bytes()).hexdigest() != trial.DAGU_SHA256:
        raise SystemExit("pinned Dagu binary unavailable or changed")
    dags = RUNTIME / "home" / "dags"
    dags.mkdir(parents=True)
    for template in (HERE / "auto" / "definitions").glob("*.yaml.in"):
        source = template.read_text()
        rendered = (source.replace("@PYTHON@", sys.executable)
                    .replace("@BRIDGE@", str(HERE / "auto_bridge.py"))
                    .replace("@RUNTIME@", str(RUNTIME)))
        (dags / template.name.removesuffix(".in")).write_text(rendered)
    env = {**os.environ, "DAGU_HOME": str(RUNTIME / "home"),
           "DAGU_AUTH_MODE": "none", "DAGU_COORDINATOR_ENABLED": "false"}
    source_hash = {name: hashlib.sha256((HERE / name).read_bytes()).hexdigest()
                   for name in ("bridge.py", "auto_bridge.py", "auto_trial.py")}
    evidence: dict = {"dagu_binary_sha256": trial.DAGU_SHA256,
                      "source_sha256_before": source_hash,
                      "definition_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                            for p in dags.glob("*.yaml")},
                      "runtime": str(RUNTIME)}
    server = None
    try:
        for definition in dags.glob("*.yaml"):
            checked = subprocess.run([str(trial.DAGU), "validate", str(definition)], env=env,
                                     capture_output=True, text=True, timeout=15)
            if checked.returncode:
                raise RuntimeError(f"Dagu validation failed: {definition.name}: {checked.stderr}")
        evidence["registered"] = bridge.register(RUNTIME)
        with auto_bridge.connect(RUNTIME):
            pass
        port = trial.free_port()
        base = f"http://127.0.0.1:{port}"
        (RUNTIME / "base_url").write_text(base + "\n")
        server = trial.start_server(RUNTIME, port, env)
        evidence["first_dagu_pid"] = server.pid
        evidence["parent_start"] = trial.api(base, f"/api/v1/dags/{bridge.PARENT_NAME}.yaml/start",
                                             {"dagRunId": bridge.PARENT_ID})
        evidence["child_start"] = trial.api(base, f"/api/v1/dags/{bridge.CHILD_NAME}.yaml/start",
                                            {"dagRunId": bridge.CHILD_ID})
        trial.until(lambda: trial.is_waiting(base, bridge.PARENT_NAME, bridge.PARENT_ID, "child_gate"),
                    "parent wait")
        trial.until(lambda: trial.is_waiting(base, bridge.CHILD_NAME, bridge.CHILD_ID, "director_wait"),
                    "child wait")
        evidence["both_waiting"] = {"parent": trial.native(base, bridge.PARENT_NAME, bridge.PARENT_ID),
                                    "child": trial.native(base, bridge.CHILD_NAME, bridge.CHILD_ID),
                                    "processes": trial.process_snapshot(RUNTIME, server.pid)}
        trial.stop_server(server)
        server = trial.start_server(RUNTIME, port, env)
        evidence["restarted_dagu_pid"] = server.pid
        trial.until(lambda: trial.is_waiting(base, bridge.PARENT_NAME, bridge.PARENT_ID, "child_gate"),
                    "parent wait after restart")
        trial.until(lambda: trial.is_waiting(base, bridge.CHILD_NAME, bridge.CHILD_ID, "director_wait"),
                    "child wait after restart")
        evidence["wrong_child_rejected"] = command("accept-child", "--token", bridge.FIXTURE_TOKEN,
                                                    "--child-id", "exo-wait-wrong-001", success=False)
        evidence["unauthorized_rejected"] = command("accept-child", "--token", "wrong", success=False)
        (RUNTIME / "pause_notify").write_text("pause first notify attempt before parent-gate ack\n")
        (RUNTIME / "lose_ack_once").write_text("fail second notify after parent-gate ack\n")
        evidence["authorized_child"] = command("accept-child", "--token", bridge.FIXTURE_TOKEN)
        trial.until(lambda: (RUNTIME / "bridge_entered").exists(), "child final bridge entered", 25)
        child_at_fault = bridge.status(base, bridge.CHILD_NAME, bridge.CHILD_ID)
        evidence["before_kill"] = {
            "parent": trial.native(base, bridge.PARENT_NAME, bridge.PARENT_ID),
            "child": trial.native(base, bridge.CHILD_NAME, bridge.CHILD_ID),
            "child_outcome_node": {"status": node(child_at_fault, "child_outcome")["statusLabel"],
                                   "done_count": node(child_at_fault, "child_outcome")["doneCount"]},
            "bridge_pid": int((RUNTIME / "bridge_entered").read_text().strip())}
        if evidence["before_kill"]["child_outcome_node"]["status"] != "succeeded":
            raise AssertionError("child outcome did not precede bridge fault")
        if evidence["before_kill"]["parent"]["status"] != "waiting":
            raise AssertionError("parent advanced before bridge ack")
        os.kill(evidence["before_kill"]["bridge_pid"], signal.SIGKILL)
        (RUNTIME / "pause_notify").unlink()
        trial.stop_server(server)
        server = trial.start_server(RUNTIME, port, env)
        evidence["fault_restarted_dagu_pid"] = server.pid
        trial.until(lambda: trial.is_succeeded(base, bridge.PARENT_NAME, bridge.PARENT_ID,
                                               "parent_continuation"), "automatic parent continuation", 65)
        trial.until(lambda: trial.is_succeeded(base, bridge.CHILD_NAME, bridge.CHILD_ID,
                                               "notify_parent"), "child final step retry", 65)
        parent_final = bridge.status(base, bridge.PARENT_NAME, bridge.PARENT_ID)
        child_final = bridge.status(base, bridge.CHILD_NAME, bridge.CHILD_ID)
        with auto_bridge.connect(RUNTIME) as db:
            evidence["bridge_row"] = dict(db.execute("SELECT * FROM bridge").fetchone())
            evidence["auto_state_row"] = dict(db.execute("SELECT * FROM auto_state").fetchone())
        evidence["final"] = {
            "parent": trial.native(base, bridge.PARENT_NAME, bridge.PARENT_ID),
            "child": trial.native(base, bridge.CHILD_NAME, bridge.CHILD_ID),
            "parent_continuation_done_count": node(parent_final, "parent_continuation")["doneCount"],
            "notify_parent_done_count": node(child_final, "notify_parent")["doneCount"],
            "notify_parent_retry_count": node(child_final, "notify_parent")["retryCount"],
            "processes": trial.process_snapshot(RUNTIME, server.pid)}
        evidence["fault_markers"] = {name: (RUNTIME / name).exists()
                                     for name in ("pause_notify", "lose_ack_once")}
        evidence["outcome"] = "passed"
    except Exception as error:
        evidence["outcome"] = "failed"
        evidence["error"] = repr(error)
        raise
    finally:
        trial.stop_server(server)
        evidence["source_sha256_after"] = {name: hashlib.sha256((HERE / name).read_bytes()).hexdigest()
                                           for name in source_hash}
        (HERE / "auto_observed.json").write_text(json.dumps(evidence, sort_keys=True, indent=2) + "\n")
    print(json.dumps({"outcome": evidence["outcome"], "runtime": str(RUNTIME)}, sort_keys=True))


if __name__ == "__main__":
    main()
