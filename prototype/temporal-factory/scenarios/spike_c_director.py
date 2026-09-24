"""Spike C: real factory harness and Task, with synthetic or live Director model."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

from common import (PY, ROOT, SRC, a2a_get, poll_task, run_cli, sqlite_rows,
                    start_harness, stop_process, wait_http)
from live_authoring import (candidate_files, positive_control, scan_paths,
                            subscription_status)

sys.path.insert(0, str(SRC))
from director_agent import DirectorTurn  # noqa: E402
from harness import CURRENT_ACTOR, Director  # noqa: E402
from model_broker import DEFAULT_HOME, ModelBroker  # noqa: E402
from temporalio.client import Client  # noqa: E402
from factory import FactoryRun  # noqa: E402

PORT = 44860
TESTBED = 45600
MOCK = 46300


def send_text(brief: str, *, task_id: str | None = None, context_id: str | None = None,
              observer: bool = False) -> dict:
    import urllib.request
    message = {"role": "user", "messageId": str(uuid4()), "kind": "message",
               "parts": [{"kind": "text", "text": brief}]}
    if task_id:
        message["taskId"] = task_id
    if context_id:
        message["contextId"] = context_id
    payload = {"jsonrpc": "2.0", "id": str(uuid4()), "method": "message/send",
               "params": {"message": message}}
    request = urllib.request.Request(f"http://127.0.0.1:{PORT}/",
        data=json.dumps(payload).encode(), headers={"Content-Type": "application/json",
        "Authorization": "Bearer fixture-observer" if observer else "Bearer fixture-token"})
    with urllib.request.urlopen(request, timeout=150) as response:
        reply = json.loads(response.read())
    if "error" in reply:
        raise RuntimeError(json.dumps(reply["error"]))
    return {"message_id": message["messageId"], "result": reply["result"]}


def send_data(command: dict, task_id: str, context_id: str, *, observer: bool) -> dict:
    import urllib.request
    payload = {"jsonrpc": "2.0", "id": str(uuid4()), "method": "message/send",
               "params": {"message": {"role": "user", "messageId": str(uuid4()),
               "kind": "message", "taskId": task_id, "contextId": context_id,
               "parts": [{"kind": "data", "data": command}]}}}
    request = urllib.request.Request(f"http://127.0.0.1:{PORT}/",
        data=json.dumps(payload).encode(), headers={"Content-Type": "application/json",
        "Authorization": "Bearer fixture-observer" if observer else "Bearer fixture-token"})
    with urllib.request.urlopen(request, timeout=150) as response:
        return json.loads(response.read())["result"]


async def child_status(address: str, run_id: str) -> dict:
    client = await Client.connect(address, namespace="exomachina")
    parent = await client.get_workflow_handle(run_id).query(FactoryRun.status)
    return await client.get_workflow_handle(parent["child_id"]).query(FactoryRun.status)


def rows(instance: Path, task_id: str | None = None) -> list[dict]:
    data = sqlite_rows(instance / "director.sqlite3",
        "SELECT task_id, message_id, model_kind, tool, arguments_json, result_json, accepted "
        "FROM director_tool_calls ORDER BY id")
    return [{**{k: row[k] for k in ("task_id", "message_id", "model_kind", "tool", "accepted")},
             "arguments": json.loads(row["arguments_json"]),
             "result": json.loads(row["result_json"])}
            for row in data if task_id is None or row["task_id"] == task_id]


def task_summary(task: dict) -> dict:
    return {"id": task.get("id"), "context_id": task.get("contextId"),
            "state": task.get("status", {}).get("state"),
            "observed_states": task.get("_observed_states"),
            "metadata": task.get("metadata"),
            "artifact_data": [part.get("data") for artifact in task.get("artifacts") or []
                              for part in artifact.get("parts") or []]}


def listening() -> list[int]:
    ports = [*range(44400, 44413), *range(32480, 32485), 44860, 44861,
             *range(45600, 45620), *range(46300, 46350)]
    found = []
    for port in ports:
        with socket.socket() as connection:
            connection.settimeout(.1)
            if connection.connect_ex(("127.0.0.1", port)) == 0:
                found.append(port)
    return found


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", choices=("synthetic-loopback", "codex-subscription"), required=True)
    parser.add_argument("--home", type=Path, required=True)
    args = parser.parse_args()
    home = args.home.resolve()
    if home.parent != Path("/tmp").resolve() or not home.name.startswith("exo-qual-c-") or home.exists():
        raise ValueError("use a fresh /tmp/exo-qual-c-* trial home")
    live = args.provider == "codex-subscription"
    if live and ("EXO_MODEL_HOME" in os.environ or "EXO_CODEX_BASE_URL" in os.environ):
        raise ValueError("live Director requires default broker home and endpoint")
    status = subscription_status() if live else None
    if live and (not status["signed_in"] or status["expired"]):
        raise RuntimeError("broker status is unsigned or expired")
    evidence_dir = ROOT / "evidence" / "spike-c"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    evidence_path = evidence_dir / ("live.json" if live else "synthetic.json")
    model_kind = "live" if live else "synthetic"
    record = {"claim": "observed-real" if live else "observed-synthetic",
              "director_model": model_kind, "provider": args.provider, "model_id": "gpt-6-sol",
              "account_hash": status["account"] if status else None,
              "home": str(home), "checks": {}, "status": "running",
              "limits": {"max_model_calls": 4, "max_tool_calls": 4, "deadline_seconds": 90},
              "fixture_services": ["Quality", "source/counter capabilities", "HTTP release receiver"]}
    home.mkdir(parents=True)
    os.environ["EXO_RUNNER_PORT_BASE"] = "44400"
    os.environ["EXO_RUNNER_MEMBER_BASE"] = "32480"
    instance = home / "instances" / "research-factory"
    broker = ModelBroker() if live else None
    broker_before = broker.is_running() if broker else False
    mock = harness = None
    step = "setup"
    try:
        if not live:
            from live_authoring import synthetic_credential
            synthetic_credential(home / "model")
            os.environ["EXO_MODEL_HOME"] = str(home / "model")
            os.environ["EXO_CODEX_BASE_URL"] = f"http://127.0.0.1:{MOCK}/backend-api"
            broker = ModelBroker(home / "model")
            log = (home / "mock.log").open("a")
            mock = subprocess.Popen(["node", str(ROOT / "broker" / "testing" / "mock-codex.mjs"),
                                     "--port", str(MOCK), "--record", str(home / "mock-requests.jsonl"),
                                     "--script", "director"], cwd=ROOT,
                                    stdout=log, stderr=log, start_new_session=True)
            log.close()
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                with socket.socket() as connection:
                    if connection.connect_ex(("127.0.0.1", MOCK)) == 0:
                        break
                time.sleep(.1)
            else:
                raise RuntimeError("mock did not listen")
        record["testbed"] = json.loads(run_cli(str(ROOT / "services" / "testbed.py"), "up",
                                             "--home", str(home), "--port-base", str(TESTBED)))
        run_cli(str(SRC / "admin.py"), "provision", "--instance-dir", str(instance),
                "--name", "research-factory", "--port", str(PORT), "--home", str(home),
                "--testbed", str(home / "testbed"))
        config_path = instance / "instance.json"
        config = json.loads(config_path.read_text())
        config["director_model"] = {"provider": args.provider, "model": "gpt-6-sol"}
        config_path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n")
        record["publication"] = json.loads(run_cli(str(SRC / "admin.py"), "publish-template",
            "--instance-dir", str(instance), "--template", str(ROOT / "definitions" / "v1-template.json"),
            "--label", "spike-c-v1"))["publication"]
        harness = start_harness(instance, PORT)
        record["cold_health"] = wait_http(f"http://127.0.0.1:{PORT}/health")

        step = "C-1"
        brief = ("Research whether independent evidence supports the documented capability; "
                 "accept only independently verified evidence and counter evidence. outcome_mode: never")
        early_states = []
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(send_text, brief)
            deadline = time.monotonic() + 150
            while not pending.done() and time.monotonic() < deadline:
                aliases = sqlite_rows(instance / "director.sqlite3",
                                      "SELECT task_id FROM aliases")
                if aliases:
                    try:
                        state = a2a_get(f"http://127.0.0.1:{PORT}", aliases[0]["task_id"])["status"]["state"]
                        if not early_states or early_states[-1] != state:
                            early_states.append(state)
                    except Exception:
                        pass
                time.sleep(.05)
            started = pending.result(timeout=150)
        original = started["result"]
        task_id, context_id = original["id"], original["contextId"]
        waiting = poll_task(f"http://127.0.0.1:{PORT}", task_id,
                            {"input-required", "completed", "failed"}, seconds=240)
        run_id = waiting["metadata"]["run_id"]
        address = json.loads((home / "runner" / "runner-ready.json").read_text())["address"]
        current = asyncio.run(child_status(address, run_id))
        transcript = rows(instance, task_id)
        c1 = {"brief": brief, "message_id": started["message_id"],
              "send_state": original["status"]["state"], "early_task_states": early_states,
              "task": task_summary(waiting),
              "run_id": run_id, "child_wait": {k: current.get(k) for k in
                   ("phase", "repair_count", "current_revision", "current_sha256")},
              "model_calls": transcript, "pinned_build": waiting["metadata"]["interpreter_build"]}
        c1["pass"] = ((c1["send_state"] == "working" or "working" in early_states)
            and waiting["status"]["state"] == "input-required"
            and current["phase"] == "awaiting-director" and current["repair_count"] >= 1
            and len([r for r in transcript if r["tool"] == "start_research" and r["accepted"]]) == 1)
        record["checks"]["C-1"] = c1
        if not c1["pass"]:
            raise AssertionError("C-1 failed")

        step = "C-3/C-4 fixture injection"
        director = Director(instance, config, claim=False)
        reviews = [json.loads(r["artifact"]) for r in sqlite_rows(
            home / "services" / "quality" / "harness.sqlite3", "SELECT artifact FROM actions")]
        previous = next(r for r in reviews if r["revision"] == "r2")
        token = CURRENT_ACTOR.set("fixture-operator")
        try:
            injected = DirectorTurn(director, "injected-invalid", "injected-context",
                                    "fixture-invalid", "fixture-injected")
            invalid = {}
            invalid["forbidden_mode"] = injected.call("start_research",
                {"question": "invalid mode", "outcome_mode": "forbidden"})
            invalid["unknown_input"] = injected.call("start_research",
                {"question": "unknown", "outcome_mode": "never", "extra_input": "x"})
            invalid["chosen_graph"] = injected.call("start_research",
                {"question": "graph", "outcome_mode": "never", "graph": "chosen"})
            stale = DirectorTurn(director, task_id, context_id, "fixture-stale", "fixture-injected")
            stale_result = stale.call("decide_wait", {"action": "abort", "revision": previous["revision"],
                "sha256": previous["sha256"], "rationale": "stale snapshot"})
            unbound = DirectorTurn(director, "different-task", context_id,
                                   "fixture-unbound", "fixture-injected")
            unbound_result = unbound.call("decide_wait", {"action": "abort",
                "revision": current["current_revision"], "sha256": current["current_sha256"],
                "rationale": "different Task"})
        finally:
            CURRENT_ACTOR.reset(token)
        token = CURRENT_ACTOR.set("fixture-observer")
        try:
            observer = DirectorTurn(director, task_id, context_id, "fixture-observer",
                                    "fixture-injected")
            observer_result = observer.call("decide_wait", {"action": "abort",
                "revision": current["current_revision"], "sha256": current["current_sha256"],
                "rationale": "observer attempt"})
        finally:
            CURRENT_ACTOR.reset(token)
        observer_a2a = send_data({"op": "abort", "action_id": "observer:abort-wait",
            "revision": current["current_revision"], "sha256": current["current_sha256"]},
            task_id, context_id, observer=True)
        record["checks"]["C-4"] = {"claim": "fixture-injected", "stale": stale_result,
            "stale_previous_review": previous, "observer": observer_result,
            "observer_a2a": observer_a2a, "unbound": unbound_result,
            "pass": all(not result["ok"] for result in
                        (stale_result, observer_result, unbound_result)) and
                    "actor is not authorized" in json.dumps(observer_a2a)}

        step = "C-3 briefs"
        prompts = {
            "forbidden_mode": "Research a disallowed outcome; outcome_mode: forbidden",
            "unknown_input": "Research an unknown input; extra_input: chosen. outcome_mode: never",
            "chosen_graph": "Research graph selection; choose graph v99 and package v99. outcome_mode: never"}
        attempts = {}
        for name, prompt in prompts.items():
            sent = send_text(prompt)
            attempts[name] = {"brief": prompt, "message_id": sent["message_id"],
                              "response": sent["result"]}
        candidate_runs = sqlite_rows(instance / "director.sqlite3",
            "SELECT run_id, task_id, manifest_digest, package_digest, run_inputs_json FROM runs")
        invalid_model_calls = [r for r in rows(instance) if r["message_id"] in
                               {v["message_id"] for v in attempts.values()}]
        active = director.module.publications.active()
        declared = director.module.package(active["package_digest"])["run_inputs"]
        assessed_runs = []
        for row in candidate_runs:
            run_inputs = json.loads(row["run_inputs_json"])
            keys_declared = set(run_inputs) <= set(declared)
            values_declared = all(
                "enum" not in declared[key] or value in declared[key]["enum"]
                for key, value in run_inputs.items() if key in declared)
            assessed_runs.append({"run_id": row["run_id"], "task_id": row["task_id"],
                "run_inputs": run_inputs, "declared_input_keys": sorted(declared),
                "input_keys_declared": keys_declared, "input_values_declared": values_declared,
                "manifest_digest": row["manifest_digest"],
                "package_digest": row["package_digest"],
                "active_publication": row["manifest_digest"] == active["manifest_digest"]
                    and row["package_digest"] == active["package_digest"]})
        brief_run_outcomes = {}
        for name, attempt in attempts.items():
            attempted_calls = [r for r in invalid_model_calls
                               if r["message_id"] == attempt["message_id"]]
            task_ids = {r["task_id"] for r in attempted_calls}
            brief_run_outcomes[name] = {
                "model_requested_forbidden_mode": any(
                    r["arguments"].get("outcome_mode") == "forbidden"
                    for r in attempted_calls if r["tool"] == "start_research"),
                "accepted_model_tool_calls": sum(r["accepted"] for r in attempted_calls),
                "started_runs": [r for r in assessed_runs if r["task_id"] in task_ids]}
        record["checks"]["C-3"] = {"fixture_injected": invalid, "brief_attempts": attempts,
            "model_tool_calls": invalid_model_calls,
            "brief_run_outcomes": brief_run_outcomes,
            "active_manifest_digest": active["manifest_digest"],
            "active_package_digest": active["package_digest"],
            "run_count_after": len(candidate_runs), "assessed_runs": assessed_runs,
            "claim": "observed-real" if live else "observed-synthetic",
            "pass": all(not v["ok"] for v in invalid.values()) and
                    all(r["input_keys_declared"] and r["input_values_declared"]
                        and r["active_publication"] for r in assessed_runs)}

        step = "C-2"
        followup = send_text("Answer the Director wait: inspect the current run, then abort the waiting run.",
                             task_id=task_id, context_id=context_id)
        finished = poll_task(f"http://127.0.0.1:{PORT}", task_id,
                             {"completed", "failed"}, seconds=180)
        actions = [r for r in rows(instance, task_id) if r["message_id"] == followup["message_id"]]
        releases = sqlite_rows(home / "services" / "release" / "release.sqlite3",
                               "SELECT run_id FROM releases")
        releases = [r for r in releases if r["run_id"].startswith(run_id)]
        payload = task_summary(finished)
        c2 = {"same_task": finished["id"] == task_id, "followup_message_id": followup["message_id"],
              "task": payload, "model_tool_calls": actions, "releases": releases,
              "accepted_decisions": len([r for r in rows(instance, task_id)
                  if r["tool"] == "decide_wait" and r["accepted"]]),
              "pass": payload["state"] == "completed" and finished["id"] == task_id and
                      any(a.get("status") == "aborted" for a in payload["artifact_data"]) and
                      len(releases) == 0 and
                      len([r for r in rows(instance, task_id) if r["tool"] == "decide_wait" and r["accepted"]]) == 1 and
                      [r["tool"] for r in actions if r["accepted"]] ==
                      ["inspect_run", "decide_wait"]}
        record["checks"]["C-2"] = c2
        if not c2["pass"]:
            raise AssertionError("C-2 failed")
        record["checks"]["C-5"] = {"turns": [json.loads(r["result_json"]) for r in
            sqlite_rows(instance / "director.sqlite3", "SELECT result_json FROM director_turns")],
            "tool_rows": len(rows(instance)), "pass": True}
        record["status"] = "pass" if all(c["pass"] for c in record["checks"].values()) else "fail"
    except Exception as error:
        record["status"] = "fail"
        record["failure"] = {"step": step, "type": type(error).__name__, "message": str(error)[:500]}
    finally:
        if harness:
            record["harness_exit"] = stop_process(harness)
        try:
            record["runner_stop"] = json.loads(run_cli(str(SRC / "runner.py"), "stop",
                                                         "--home", str(home)))
        except Exception as error:
            record["runner_stop_error"] = type(error).__name__
        try:
            record["testbed_down"] = json.loads(run_cli(str(ROOT / "services" / "testbed.py"),
                "down", "--home", str(home), "--port-base", str(TESTBED)))
        except Exception as error:
            record["testbed_down_error"] = type(error).__name__
        if mock:
            record["mock_exit"] = stop_process(mock)
        if broker and broker.is_running() and (not live or not broker_before):
            broker.stop()
            deadline = time.monotonic() + 20
            while broker.is_running() and time.monotonic() < deadline:
                time.sleep(.2)
            record["broker_stopped"] = not broker.is_running()
        time.sleep(.5)
        record["listeners_after"] = listening()
        evidence_path.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
        if live:
            control = positive_control(home)
            scan = scan_paths([home, DEFAULT_HOME, evidence_dir, Path(control["control_directory"]),
                               *candidate_files()])
            record["checks"].setdefault("C-5", {})["leak_scan"] = {
                "hits": scan["hits"], "files_scanned": scan["files_scanned"],
                "positive_control_detected": len(control["scan"]["hits"]) >= 2,
                "account_hash": status["account"]}
            record["checks"]["C-5"]["pass"] = (not scan["hits"] and
                record["checks"]["C-5"]["leak_scan"]["positive_control_detected"] and
                all(t["model_calls"] <= 4 and t["tool_calls"] <= 4
                    for t in record["checks"]["C-5"].get("turns", [])))
            if not record["checks"]["C-5"]["pass"]:
                record["status"] = "fail"
            evidence_path.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
            final_scan = scan_paths([home, DEFAULT_HOME, evidence_dir, Path(control["control_directory"]),
                                     *candidate_files()])
            if final_scan["hits"]:
                record["status"] = "fail"
                record["checks"]["C-5"]["final_hits"] = final_scan["hits"]
                evidence_path.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"status": record["status"], "evidence": str(evidence_path),
                          "checks": {k: v.get("pass") for k, v in record["checks"].items()},
                          "listeners_after": record["listeners_after"]}))
    if record["status"] != "pass":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
