"""Post-freeze A2 driver: publish only new native DAGs, observe one v3 wait."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import time

from director_client import get as get_task, send as send_director
from ops import db_connect, engine_status, get_run, manifest
from publisher import publish
from trial import DAGU, HERE, PYTHON, child_for, load_config, records, start_director, status, until


def snapshot(rt: Path, tasks: dict[str, str], history: list[dict]) -> dict:
    runs = records(rt / "ledger.sqlite", "runs")
    config = load_config(rt)
    return {
        "version": "dagu-community-v2.17.0",
        "manifests": {f"v{i}": manifest(f"exo_arb_parent_v{i}") for i in (3, 4, 5)
                      if (rt / "home" / "manifests" / f"exo_arb_parent_v{i}.json").is_file()},
        "runs": runs,
        "native": {r["run_id"]: engine_status(r["dag_name"], r["run_id"]) for r in runs},
        "assignments": records(rt / "ledger.sqlite", "assignments"),
        "joins": records(rt / "ledger.sqlite", "joins"),
        "verdicts": records(rt / "ledger.sqlite", "verdicts"),
        "releases": records(rt / "ledger.sqlite", "releases"),
        "receiver_release": records(rt / "release" / "release.sqlite3", "releases"),
        "director_commands": records(rt / "ledger.sqlite", "director_commands"),
        "events": records(rt / "ledger.sqlite", "events"),
        "tasks": {key: get_task(config["urls"]["director"], task_id)
                  for key, task_id in tasks.items()},
        "history": history,
        "process_registry": json.loads((rt / "processes.json").read_text()),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("runtime", type=Path)
    args = parser.parse_args()
    rt = args.runtime.resolve()
    if rt.exists() and any(rt.iterdir()):
        raise SystemExit("runtime must be fresh")
    rt.mkdir(parents=True)
    os.environ["EXO_ARB_RUNTIME"] = str(rt)
    publish(HERE / "definitions" / "v3", rt / "home", "exo_arb_parent_v3", DAGU)
    with (rt / "supervisor.log").open("wb") as stream:
        supervisor = subprocess.Popen([str(PYTHON), str(HERE / "supervisor.py"), str(rt),
                                       "--dagu", str(DAGU)], stdout=stream, stderr=stream,
                                      start_new_session=True)
    tasks: dict[str, str] = {}
    history: list[dict] = []
    try:
        until(lambda: (rt / "ready").is_file(), "supervisor ready", 50)
        v3_parent = "exo-arb-a2-old-v3-" + rt.name
        old_task = start_director(rt, v3_parent, "exo_arb_parent_v3", resolve_input=False)
        tasks["old_v3"] = old_task["id"]
        v3_child = until(lambda: child_for(v3_parent), "v3 child", 30)
        until(lambda: status(v3_child["dag_name"], v3_child["run_id"], "waiting"),
              "old v3 child Director wait", 100)
        old_digest = v3_child["definition_digest"]
        history.append({"event": "old_v3_wait", "parent": v3_parent,
                        "child": v3_child["run_id"], "task_id": old_task["id"],
                        "closure_sha256": old_digest})

        v4 = publish(HERE / "definitions" / "v4", rt / "home", "exo_arb_parent_v4", DAGU)
        history.append({"event": "v4_published", "closure_sha256": v4["closure_sha256"],
                        "old_status": engine_status(v3_child["dag_name"], v3_child["run_id"])["statusLabel"]})
        success_parent = "exo-arb-a2-v4-success-" + rt.name
        tasks["v4_success"] = start_director(rt, success_parent, "exo_arb_parent_v4",
                                             resolve_input=True)["id"]
        success_child = until(lambda: child_for(success_parent), "v4 success child", 30)
        until(lambda: get_run(success_child["run_id"])["state"] == "released", "v4 child release", 120)
        until(lambda: status("exo_arb_parent_v4", success_parent, "succeeded"),
              "v4 success parent", 40)
        history.append({"event": "v4_success", "parent": success_parent,
                        "child": success_child["run_id"]})

        exhausted_parent = "exo-arb-a2-v4-exhausted-" + rt.name
        tasks["v4_exhausted"] = start_director(rt, exhausted_parent,
                                               "exo_arb_parent_v4", resolve_input=False)["id"]
        exhausted_child = until(lambda: child_for(exhausted_parent), "v4 exhausted child", 30)
        until(lambda: status(exhausted_child["dag_name"], exhausted_child["run_id"], "waiting"),
              "v4 child Director wait", 100)
        history.append({"event": "v4_director_wait", "parent": exhausted_parent,
                        "child": exhausted_child["run_id"]})
        command = send_director(load_config(rt)["urls"]["director"], {
            "op": "abort", "key": "abort:" + exhausted_child["run_id"],
            "run_id": exhausted_child["run_id"], "revision": "r3",
            "owner_epoch": exhausted_child["owner_epoch"],
            "token": "fixture-director-token", "expires_at": time.time() + 60})
        history.append({"event": "v4_authorized_abort", "child": exhausted_child["run_id"],
                        "director_task_id": command["id"]})
        until(lambda: get_run(exhausted_child["run_id"])["state"] == "aborted", "v4 abort", 60)
        until(lambda: status("exo_arb_parent_v4", exhausted_parent, "succeeded"),
              "v4 aborted parent", 40)

        v5 = publish(HERE / "definitions" / "v5", rt / "home", "exo_arb_parent_v5", DAGU)
        history.append({"event": "v5_published", "closure_sha256": v5["closure_sha256"],
                        "old_status": engine_status(v3_child["dag_name"], v3_child["run_id"])["statusLabel"]})
        clear_parent = "exo-arb-a2-v5-clear-" + rt.name
        tasks["v5_clear"] = start_director(rt, clear_parent, "exo_arb_parent_v5",
                                           resolve_input=False)["id"]
        clear_child = until(lambda: child_for(clear_parent), "v5 clear child", 30)
        until(lambda: get_run(clear_child["run_id"])["state"] == "released", "v5 direct release", 120)
        until(lambda: status("exo_arb_parent_v5", clear_parent, "succeeded"),
              "v5 clear parent", 40)
        history.append({"event": "v5_direct_r1_success", "parent": clear_parent,
                        "child": clear_child["run_id"]})

        before = json.loads((rt / "processes.json").read_text())["dagu"]
        os.killpg(before["pid"], signal.SIGKILL)
        until(lambda: json.loads((rt / "processes.json").read_text())["dagu"]["starts"] > before["starts"],
              "normal supervisor Dagu restart", 35)
        until(lambda: status(v3_child["dag_name"], v3_child["run_id"], "waiting"),
              "old v3 wait after restart", 35)
        assert get_run(v3_child["run_id"])["definition_digest"] == old_digest
        assert child_for(v3_parent)["run_id"] == v3_child["run_id"]
        assert get_task(load_config(rt)["urls"]["director"], old_task["id"])["status"]["state"] == "input-required"
        history.append({"event": "v3_pinned_after_restart", "parent": v3_parent,
                        "child": v3_child["run_id"], "closure_sha256": old_digest,
                        "dagu_starts": before["starts"] + 1})
        result = snapshot(rt, tasks, history)
        (rt / "result.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"result": "passed", "evidence": str(rt / "result.json")}, sort_keys=True))
    except Exception:
        try:
            (rt / "partial-result.json").write_text(json.dumps(snapshot(rt, tasks, history),
                                                                  indent=2, sort_keys=True) + "\n")
        except Exception:
            pass
        raise
    finally:
        supervisor.terminate()
        try:
            supervisor.wait(timeout=12)
        except subprocess.TimeoutExpired:
            supervisor.kill()


if __name__ == "__main__":
    main()
