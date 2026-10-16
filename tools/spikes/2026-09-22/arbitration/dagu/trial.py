"""Black-box arbitration driver: faults and observations, never a run retry."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import time
import urllib.error

from director_client import get as get_task, send as send_director
from ops import db_connect, engine_status, get_run, manifest
from publisher import publish

HERE = Path(__file__).resolve().parent
PYTHON = HERE.parents[1] / "s2" / ".venv" / "bin" / "python"
DAGU = Path("/tmp/exomachina-countertrials/dagu/dagu")


def until(check, label: str, timeout: float = 90):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except (OSError, ValueError, KeyError, urllib.error.URLError) as error:
            last = repr(error)
        time.sleep(.2)
    raise TimeoutError(f"{label} timed out; last={last}")


def load_config(rt: Path) -> dict:
    return json.loads((rt / "config.json").read_text())


def start_director(rt: Path, run_id: str, root: str, *, resolve_input: bool,
                   release_mode: str = "participating") -> dict:
    config = load_config(rt)
    return send_director(config["urls"]["director"], {"op": "start", "key": "start:" + run_id,
          "run_id": run_id, "root_name": root, "definition_digest": manifest(root)["closure_sha256"],
          "resolve_input": resolve_input, "release_mode": release_mode})


def child_for(parent_id: str) -> dict | None:
    with db_connect() as db:
        row = db.execute("SELECT * FROM runs WHERE parent_id=?", (parent_id,)).fetchone()
    return dict(row) if row else None


def status(name: str, run_id: str, desired: str) -> dict | None:
    native = engine_status(name, run_id)
    return native if native and native["statusLabel"] == desired else None


def records(path: Path, table: str) -> list[dict]:
    if not path.exists():
        return []
    with sqlite3.connect(path) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute(f"SELECT * FROM {table}")]


def step_pid(operation: str, rt: Path) -> int:
    listing = subprocess.check_output(["ps", "-axo", "pid=,command="], text=True)
    matches = []
    for line in listing.splitlines():
        parts = line.strip().split(None, 1)
        if len(parts) == 2 and "ops.py " + operation in parts[1] and "python" in parts[1]:
            pid = int(parts[0])
            current = pid
            for _ in range(8):
                parent = subprocess.run(["ps", "-o", "ppid=,command=", "-p", str(current)],
                                        capture_output=True, text=True).stdout.strip()
                if not parent:
                    break
                parent_id, _, command = parent.partition(" ")
                if str(rt) in command:
                    matches.append(pid)
                    break
                if not parent_id.isdigit() or int(parent_id) <= 1:
                    break
                current = int(parent_id)
    if len(matches) != 1:
        raise AssertionError(f"expected one active {operation} adapter, found {matches}")
    return matches[0]


def snapshot(rt: Path, tasks: dict, history: list[dict]) -> dict:
    config = load_config(rt)
    runs = records(rt / "ledger.sqlite", "runs")
    native = {}
    for run in runs:
        try:
            native[run["run_id"]] = engine_status(run["dag_name"], run["run_id"])
        except OSError:
            native[run["run_id"]] = None
    result = {"version": "dagu-community-v2.17.0", "manifest_v2": manifest("exo_arb_parent_v2"),
              "manifest_v3": manifest("exo_arb_parent_v3"), "ports": config["urls"],
              "identities": config["identities"], "director_identity": None,
              "runs": runs, "native": native, "assignments": records(rt / "ledger.sqlite", "assignments"),
              "candidates": records(rt / "ledger.sqlite", "candidates"),
              "verdicts": records(rt / "ledger.sqlite", "verdicts"),
              "releases": records(rt / "ledger.sqlite", "releases"),
              "director_commands": records(rt / "ledger.sqlite", "director_commands"),
              "events": records(rt / "ledger.sqlite", "events"),
              "capability_receivers": {
                  role: records(rt / role / "harness.sqlite3", "actions") for role in
                  ("source_evidence", "counter_evidence")},
              "quality_receiver": records(rt / "quality" / "harness.sqlite3", "actions"),
              "release_receiver": records(rt / "release" / "release.sqlite3", "releases"),
              "opaque_receiver": records(rt / "opaque" / "release.sqlite3", "opaque_effects"),
              "process_registry": json.loads((rt / "processes.json").read_text()),
              "reconciler_events": [json.loads(line) for line in (rt / "reconciler-events.jsonl").read_text().splitlines()]
                if (rt / "reconciler-events.jsonl").exists() else [],
              "history": history, "tasks": {}}
    for label, task_id in tasks.items():
        result["tasks"][label] = get_task(config["urls"]["director"], task_id)
    if tasks:
        result["director_identity"] = next(iter(result["tasks"].values()))["metadata"]["director_identity"]
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("runtime", type=Path)
    parser.add_argument("--faults", action="store_true")
    parser.add_argument("--opaque", action="store_true")
    args = parser.parse_args()
    rt = args.runtime.resolve()
    if rt.exists() and any(rt.iterdir()):
        raise SystemExit("runtime must be fresh")
    rt.mkdir(parents=True)
    os.environ["EXO_ARB_RUNTIME"] = str(rt)
    if not PYTHON.is_file() or not DAGU.is_file():
        raise SystemExit("pinned S2 Python or Dagu binary unavailable")
    publish(HERE / "definitions" / "v2", rt / "home", "exo_arb_parent_v2", DAGU)
    with (rt / "supervisor.log").open("wb") as stream:
        supervisor = subprocess.Popen([str(PYTHON), str(HERE / "supervisor.py"), str(rt), "--dagu", str(DAGU)],
                                      stdout=stream, stderr=stream, start_new_session=True)
    tasks: dict[str, str] = {}
    history: list[dict] = []
    try:
        until(lambda: (rt / "ready").is_file(), "supervisor ready", 50)
        old = start_director(rt, "exo-arb-old-" + rt.name, "exo_arb_parent_v2", resolve_input=False)
        tasks["old"] = old["id"]
        until(lambda: status("exo_arb_parent_v2", "exo-arb-old-" + rt.name, "waiting"), "old v2 wait")
        history.append({"event": "old_wait", "at": time.time(), "run_id": "exo-arb-old-" + rt.name,
                        "task_id": old["id"]})
        publish(HERE / "definitions" / "v3", rt / "home", "exo_arb_parent_v3", DAGU)
        history.append({"event": "v3_published", "at": time.time(),
                        "old_engine_status": engine_status("exo_arb_parent_v2", "exo-arb-old-" + rt.name)["statusLabel"]})
        if args.faults:
            (rt / "pause-assignment-source_evidence").write_text("hold\n")
            (rt / "fault-quality-before-record").write_text("one shot\n")
            (rt / "fault-accept-before-engine-ack").write_text("one shot\n")
            (rt / "fault-release-drop-ack").write_text("one shot\n")
        success_id = "exo-arb-success-" + rt.name
        started = start_director(rt, success_id, "exo_arb_parent_v3", resolve_input=True)
        tasks["success"] = started["id"]
        child = until(lambda: child_for(success_id), "nested success child", 30)
        history.append({"event": "child_started", "at": time.time(), "parent_id": success_id,
                        "child_id": child["run_id"], "task_id": started["id"]})
        if args.faults:
            until(lambda: (rt / f"entered-assignment-{child['run_id']}-source_evidence").exists(),
                  "active assignment after remote commit", 45)
            adapter_pid = step_pid("assign --instance source_evidence", rt)
            os.kill(adapter_pid, signal.SIGKILL)
            registry = json.loads((rt / "processes.json").read_text())
            os.killpg(registry["dagu"]["pid"], signal.SIGKILL)
            history.append({"event": "dagu_killed_during_active_assignment", "at": time.time(),
                            "dagu_pid": registry["dagu"]["pid"], "adapter_pid": adapter_pid,
                            "child_id": child["run_id"]})
            (rt / "pause-assignment-source_evidence").unlink()
            until(lambda: json.loads((rt / "processes.json").read_text())["dagu"]["starts"] >= 2,
                  "supervisor Dagu restart", 30)
        until(lambda: get_run(child["run_id"])["state"] == "released", "successful release", 130)
        until(lambda: get_run(success_id)["state"] == "released", "parent public accepted result", 40)
        until(lambda: status("exo_arb_parent_v3", success_id, "succeeded"), "parent succeeded", 30)
        history.append({"event": "success_complete", "at": time.time(), "parent_id": success_id,
                        "child_id": child["run_id"]})
        exhausted_id = "exo-arb-exhausted-" + rt.name
        exhausted = start_director(rt, exhausted_id, "exo_arb_parent_v3", resolve_input=False)
        tasks["exhausted"] = exhausted["id"]
        child2 = until(lambda: child_for(exhausted_id), "nested exhausted child", 30)
        until(lambda: status(child2["dag_name"], child2["run_id"], "waiting"), "nested Director wait", 90)
        history.append({"event": "nested_director_wait", "at": time.time(),
                        "parent_id": exhausted_id, "child_id": child2["run_id"],
                        "task_id": exhausted["id"]})
        for label, override in (
            ("unauthorized", {"token": "wrong-token"}),
            ("expired", {"expires_at": time.time() - 10}),
            ("stale_revision", {"revision": "r2"}),
        ):
            command = {"op": "abort", "key": label + ":" + child2["run_id"],
                       "run_id": child2["run_id"], "revision": "r3",
                       "owner_epoch": child2["owner_epoch"],
                       "token": "fixture-director-token", "expires_at": time.time() + 60}
            command.update(override)
            try:
                send_director(load_config(rt)["urls"]["director"], command)
                raise AssertionError(label + " Director command accepted")
            except RuntimeError as error:
                history.append({"event": label + "_director_rejected", "at": time.time(),
                                "error": str(error)})
        if args.faults:
            nested_pid = step_pid("nested --child exo_arb_research_v3", rt)
            os.kill(nested_pid, signal.SIGKILL)
            registry = json.loads((rt / "processes.json").read_text())
            os.killpg(registry["dagu"]["pid"], signal.SIGKILL)
            history.append({"event": "dagu_killed_at_nested_wait", "at": time.time(),
                            "dagu_pid": registry["dagu"]["pid"], "nested_adapter_pid": nested_pid,
                            "child_id": child2["run_id"]})
            until(lambda: json.loads((rt / "processes.json").read_text())["dagu"]["starts"] >= 3,
                  "Dagu restart at nested wait", 30)
            until(lambda: status(child2["dag_name"], child2["run_id"], "waiting"), "restored child wait", 30)
            # Replacement owner changes the epoch. The stale command must fail.
            from ops import claim_owner
            new_epoch = claim_owner(child2["run_id"])
            try:
                send_director(load_config(rt)["urls"]["director"], {"op": "abort", "key": "stale-abort",
                    "run_id": child2["run_id"], "revision": "r3", "owner_epoch": new_epoch-1,
                    "token": "fixture-director-token"})
                raise AssertionError("stale owner command accepted")
            except RuntimeError as error:
                history.append({"event": "stale_owner_rejected", "at": time.time(), "error": str(error)})
        else:
            new_epoch = child2["owner_epoch"]
        decision = send_director(load_config(rt)["urls"]["director"], {"op": "abort",
            "key": "abort:" + child2["run_id"], "run_id": child2["run_id"], "revision": "r3",
            "owner_epoch": new_epoch, "token": "fixture-director-token",
            "expires_at": time.time() + 60})
        history.append({"event": "authorized_abort", "at": time.time(),
                        "child_id": child2["run_id"], "director_task_id": decision["id"]})
        until(lambda: get_run(child2["run_id"])["state"] == "aborted", "child aborted", 50)
        until(lambda: get_run(exhausted_id)["state"] == "aborted", "parent aborted", 40)
        until(lambda: status("exo_arb_parent_v3", exhausted_id, "succeeded"), "aborted parent finished", 30)
        history.append({"event": "exhaustion_complete", "at": time.time(),
                        "parent_id": exhausted_id, "child_id": child2["run_id"]})
        if args.opaque:
            (rt / "fault-release-drop-ack").write_text("drop one opaque reply\n")
            opaque_id = "exo-arb-opaque-" + rt.name
            opaque = start_director(rt, opaque_id, "exo_arb_parent_v3", resolve_input=True,
                                    release_mode="opaque")
            tasks["opaque"] = opaque["id"]
            opaque_child = until(lambda: child_for(opaque_id), "nested opaque child", 30)
            def opaque_unresolved():
                with db_connect() as db:
                    return db.execute("SELECT * FROM releases WHERE run_id=? AND state='unresolved'",
                                      (opaque_child["run_id"],)).fetchone()
            until(opaque_unresolved, "durable opaque unresolved release", 90)
            until(lambda: len(records(rt / "opaque" / "release.sqlite3", "opaque_effects")) == 1,
                  "one opaque receiver effect", 20)
            release_pid = step_pid("release --revision r2", rt)
            os.kill(release_pid, signal.SIGKILL)
            history.append({"event": "opaque_adapter_killed_after_unknown", "at": time.time(),
                            "parent_id": opaque_id, "child_id": opaque_child["run_id"],
                            "adapter_pid": release_pid})
            until(lambda: (opaque_unresolved() is not None and
                          next(n for n in engine_status(opaque_child["dag_name"], opaque_child["run_id"])["nodes"]
                               if n["step"]["id"] == "release_r2").get("retryCount", 0) >= 1),
                  "opaque unresolved after native retry", 35)
            assert len(records(rt / "opaque" / "release.sqlite3", "opaque_effects")) == 1
            with db_connect() as db:
                row = db.execute("SELECT attempts FROM releases WHERE run_id=?",
                                 (opaque_child["run_id"],)).fetchone()
            assert row["attempts"] == 1
            history.append({"event": "opaque_still_unresolved_after_restart", "at": time.time(),
                            "child_id": opaque_child["run_id"], "submit_attempts": row["attempts"],
                            "receiver_effects": 1})
        result = snapshot(rt, tasks, history)
        (rt / "result.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"result": "passed", "evidence": str(rt / "result.json"),
                          "run_ids": [row["run_id"] for row in result["runs"]]}, sort_keys=True))
    except Exception:
        try:
            result = snapshot(rt, tasks, history)
            (rt / "partial-result.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        except Exception:
            pass
        raise
    finally:
        supervisor.terminate()
        try: supervisor.wait(timeout=12)
        except subprocess.TimeoutExpired:
            supervisor.kill()


if __name__ == "__main__":
    main()
