"""Bounded pre-integration probe (not counted) of the four report agents.

This is a direct A2A driver, not a factory or a pre-registered scenario.
Trial state is preserved. The default broker is never stopped or configured here.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from uuid import uuid4

from live_authoring import positive_control, scan_paths


ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parents[1]
EVIDENCE = ROOT / "evidence" / "single-factory"
PACKET_PATH = ROOT / "packets" / "exo-qualification-2026-09-23" / "packet.json"
STIMULI_PATH = ROOT / "scenarios" / "sf_stimuli.json"
PY = sys.executable
PORTS = {"research_findings": 45740, "research_risks": 45741,
         "synthesizer": 45742, "quality": 45743}
CAPABILITIES = {"research_findings": "packet_findings@1",
                "research_risks": "packet_risks@1",
                "synthesizer": "report_synthesis@1",
                "quality": "report_quality_review@1"}
ROLES = {"research_findings": "research", "research_risks": "research",
         "synthesizer": "synthesis", "quality": "quality"}
AUTH = "Bearer fixture-token"


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value: object) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def http(port: int, path: str, body: dict | None = None, *, authenticated: bool = True,
         timeout: float = 15) -> dict:
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json", **({"Authorization": AUTH} if authenticated else {})},
        method="GET" if body is None else "POST")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read())


def rpc(port: int, method: str, params: dict) -> dict:
    response = http(port, "/", {"jsonrpc": "2.0", "id": str(uuid4()),
                                "method": method, "params": params})
    if "error" in response:
        raise RuntimeError(f"{method} error: {response['error']}")
    return response["result"]


def send(name: str, run_id: str, revision: str, brief: dict, definition_digest: str) -> dict:
    command = {"op": "assign", "action_id": f"{run_id}:{name}:{revision}",
               "run_id": run_id, "definition_digest": definition_digest,
               "brief": canonical(brief)}
    message = {"kind": "message", "role": "user", "messageId": str(uuid4()),
               "parts": [{"kind": "data", "data": command}]}
    task = rpc(PORTS[name], "message/send", {"message": message,
                                               "configuration": {"blocking": False}})
    if task["status"]["state"] not in ("submitted", "working"):
        raise AssertionError(f"{name} did not return a non-blocking Task")
    if task.get("metadata", {}).get("action_id") != command["action_id"]:
        raise AssertionError(f"{name} action binding mismatch")
    return task


def poll(name: str, task_id: str, *, seconds: float = 255) -> tuple[dict, list[str]]:
    deadline = time.monotonic() + seconds
    states = []
    while time.monotonic() < deadline:
        task = rpc(PORTS[name], "tasks/get", {"id": task_id})
        state = task["status"]["state"]
        if not states or states[-1] != state:
            states.append(state)
        if state in ("completed", "failed"):
            return task, states
        time.sleep(0.5)
    raise TimeoutError(f"{name} Task {task_id} timed out after states {states}")


def call_audit(state: Path, task_id: str) -> list[dict]:
    with sqlite3.connect(f"file:{state / 'model-agent.sqlite3'}?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        rows = db.execute("SELECT session_id,provider,model_id,live,call_no,started_at,"
                          "ended_at,outcome_kind FROM model_calls WHERE task_id=? ORDER BY call_no",
                          (task_id,)).fetchall()
    return [{**dict(row), "live": bool(row["live"]),
             "duration_s": (round(row["ended_at"] - row["started_at"], 3)
                            if row["ended_at"] is not None else None),
             "tokens": None} for row in rows]


def completed_record(name: str, initial: dict, state: Path, identity: str) -> tuple[dict, dict]:
    task, states = poll(name, initial["id"])
    calls = call_audit(state, task["id"])
    record = {"role": ROLES[name], "service": name, "capability": CAPABILITIES[name],
              "identity": identity, "task_id": task["id"], "state": task["status"]["state"],
              "observed_states": [initial["status"]["state"], *states],
              "metadata": task.get("metadata"), "model_call_count": len(calls), "model_calls": calls}
    if task["status"]["state"] != "completed":
        record["failure_status"] = task["status"].get("message")
        return record, {}
    artifacts = task.get("artifacts") or []
    if len(artifacts) != 1 or len(artifacts[0].get("parts", [])) != 1:
        raise AssertionError(f"{name} artifact count or parts mismatch")
    data = artifacts[0]["parts"][0]["data"]
    if (artifacts[0]["artifactId"] != data["sha256"] or
            hashlib.sha256(data["content"].encode()).hexdigest() != data["sha256"] or
            data["author"] != identity or data["action_id"] != task["metadata"]["action_id"] or
            data["run_id"] != task["metadata"]["run_id"] or
            data["definition_digest"] != task["metadata"]["definition_digest"]):
        raise AssertionError(f"{name} artifact binding mismatch")
    content = json.loads(data["content"])
    record["artifact"] = {"revision": data["revision"], "sha256": data["sha256"],
                          "author": data["author"], "content": content}
    return record, data


def free_ports() -> list[int]:
    busy = []
    for port in range(45740, 45760):
        with socket.socket() as sock:
            sock.settimeout(0.1)
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                busy.append(port)
    return busy


def broker_status() -> dict:
    result = subprocess.run(["node", str(ROOT / "broker" / "exo-model.mjs"), "status"],
                            cwd=ROOT, capture_output=True, text=True, timeout=20, check=True)
    value = json.loads(result.stdout)
    return {key: value.get(key) for key in ("signed_in", "expired", "account")}


def start_services(home: Path, provider: str) -> tuple[dict, dict]:
    processes, identities = {}, {}
    try:
        for name, port in PORTS.items():
            state = home / name
            state.mkdir(parents=True)
            command = [PY, "-B", str(ROOT / "services" / "model_agent.py"),
                       "--role", ROLES[name], "--capability", CAPABILITIES[name],
                       "--state", str(state), "--port", str(port),
                       "--model-provider", provider, "--model", "gpt-6-sol"]
            if name == "synthesizer":
                command.append("--test-controls")
            log = (home / f"{name}.log").open("a")
            try:
                process = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=log,
                                           start_new_session=True)
            finally:
                log.close()
            processes[name] = process
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError(f"{name} exited during startup with {process.returncode}")
                try:
                    health = http(port, "/health", authenticated=False, timeout=2)
                    identities[name] = health["identity"]
                    break
                except (urllib.error.URLError, TimeoutError, ConnectionError):
                    time.sleep(0.2)
            else:
                raise TimeoutError(f"{name} health did not respond")
        if len(set(identities.values())) != 4:
            raise AssertionError("agent identities are not distinct")
    except Exception:
        stop_services(processes)
        raise
    return processes, identities


def stop_services(processes: dict) -> dict:
    exits = {}
    for name, process in processes.items():
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
    for name, process in processes.items():
        try:
            exits[name] = process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            exits[name] = process.wait(timeout=5)
    return exits


def assign(name: str, run_id: str, brief: dict, definition_digest: str,
           home: Path, identities: dict, evidence: dict) -> dict:
    initial = send(name, run_id, brief["revision"], brief, definition_digest)
    record, artifact = completed_record(name, initial, home / name, identities[name])
    evidence["tasks"].append(record)
    write_json(EVIDENCE / evidence["evidence_file"], evidence)
    if record["state"] != "completed":
        raise RuntimeError(f"{name} Task failed: {record['task_id']}")
    return artifact


def report_file(provider: str, route: str, revision: str, artifact: dict) -> str:
    suffix = "live" if provider == "codex-subscription" else "scripted"
    name = f"agent-probe-{suffix}-1-{route}-{revision}.md"
    content = json.loads(artifact["content"])
    (EVIDENCE / name).write_text(content["markdown"] + "\n")
    return name


def review(run_id: str, artifact: dict, packet: dict, packet_digest: str,
           question: str, policy_digest: str, definition_digest: str,
           home: Path, identities: dict, evidence: dict) -> dict:
    brief = {"kind": "quality_review_request@1", "revision": artifact["revision"],
             "question": question, "packet": packet, "packet_digest": packet_digest,
             "candidate": {key: artifact[key] for key in ("revision", "sha256", "author", "content")},
             "policy_digest": policy_digest}
    verdict_artifact = assign("quality", run_id, brief, definition_digest, home, identities, evidence)
    verdict = json.loads(verdict_artifact["content"])
    if verdict["candidate"] != {key: artifact[key] for key in ("revision", "sha256", "author")}:
        raise AssertionError("Quality candidate binding mismatch")
    evidence["verdicts"].append({"run_id": run_id, **verdict})
    return verdict


def run_probe(home: Path, provider: str, evidence: dict) -> None:
    packet = json.loads(PACKET_PATH.read_text())
    packet_digest = digest(packet)
    question = packet["default_question"]
    definition_digest = digest({"kind": "pre-integration-probe@1", "packet_digest": packet_digest})
    policy_digest = digest({"rubric": "report-quality@1", "policy": "packet-grounded review"})
    evidence.update({"packet_digest": packet_digest, "question": question,
                     "definition_digest": definition_digest, "policy_digest": policy_digest})
    processes = {}
    try:
        processes, identities = start_services(home, provider)
        evidence["services"] = {name: {"identity": identities[name], "port": PORTS[name],
                                      "state_dir": str(home / name), "pid": processes[name].pid}
                                for name in PORTS}
        findings_run = f"probe-{provider}-research-{uuid4().hex[:8]}"
        research = {}
        for name in ("research_findings", "research_risks"):
            brief = {"kind": "research_assignment@1", "capability": CAPABILITIES[name],
                     "revision": "r1", "question": question, "packet": packet,
                     "packet_digest": packet_digest}
            research[name] = assign(name, findings_run, brief, definition_digest,
                                    home, identities, evidence)
        joined = {"kind": "packet_evidence_join@1", "packet_digest": packet_digest,
                  "findings": json.loads(research["research_findings"]["content"])["items"],
                  "risks": json.loads(research["research_risks"]["content"])["items"],
                  "branch_artifact_sha256": {name: research[name]["sha256"] for name in research}}
        evidence["join"] = joined

        def synthesis_brief(revision: str, mode: str, prior=None, quality_findings=None):
            return {"kind": "synthesis_assignment@1", "mode": mode, "revision": revision,
                    "question": question, "packet": packet, "packet_digest": packet_digest,
                    "evidence": joined, "prior": prior, "quality_findings": quality_findings}

        clean_run = f"probe-{provider}-clean-{uuid4().hex[:8]}"
        clean = assign("synthesizer", clean_run, synthesis_brief("r1", "draft"),
                       definition_digest, home, identities, evidence)
        evidence["reports"]["clean_r1"] = report_file(provider, "clean", "r1", clean)
        clean_verdict = review(clean_run, clean, packet, packet_digest, question, policy_digest,
                               definition_digest, home, identities, evidence)
        evidence["clean_review"] = {"accepted": clean_verdict["accepted"],
                                    "decided_by": clean_verdict["decided_by"],
                                    "findings": clean_verdict["findings"]}

        stimulus = json.loads(STIMULI_PATH.read_text())["route2"]
        evidence["stimulus_arm"] = http(PORTS["synthesizer"], "/_test/stimulus", stimulus)
        planted_run = f"probe-{provider}-planted-{uuid4().hex[:8]}"
        planted = assign("synthesizer", planted_run, synthesis_brief("r1", "draft"),
                         definition_digest, home, identities, evidence)
        planted_content = json.loads(planted["content"])
        evidence["reports"]["planted_r1"] = report_file(provider, "planted", "r1", planted)
        text = stimulus["append_claim"]["text"]
        planted_claims = [claim for claim in planted_content["claims"] if claim["text"] == text]
        if len(planted_claims) != 1:
            raise AssertionError("exactly one planted claim was not found")
        planted_id = planted_claims[0]["id"]
        evidence["planted_claim_id"] = planted_id
        evidence["stimulus_log"] = http(PORTS["synthesizer"], "/_test/stimulus-log")
        planted_verdict = review(planted_run, planted, packet, packet_digest, question,
                                 policy_digest, definition_digest, home, identities, evidence)
        blocking_match = any(f["severity"] == "blocking" and
                             (f["claim_id"] == planted_id or text in f["problem"])
                             for f in planted_verdict["findings"])
        evidence["planted_review"] = {"accepted": planted_verdict["accepted"],
                                      "decided_by": planted_verdict["decided_by"],
                                      "findings": planted_verdict["findings"],
                                      "blocking_names_or_quotes_planted": blocking_match}
        prior = {key: planted[key] for key in ("revision", "sha256", "content")}
        repaired = assign("synthesizer", planted_run,
                          synthesis_brief("r2", "repair", prior, planted_verdict["findings"]),
                          definition_digest, home, identities, evidence)
        evidence["reports"]["repair_r2"] = report_file(provider, "repair", "r2", repaired)
        evidence["repair_lacks_planted_text"] = text not in repaired["content"]
        repair_verdict = review(planted_run, repaired, packet, packet_digest, question,
                                policy_digest, definition_digest, home, identities, evidence)
        evidence["repair_review"] = {"accepted": repair_verdict["accepted"],
                                     "decided_by": repair_verdict["decided_by"],
                                     "findings": repair_verdict["findings"]}
        evidence["probe_checks"] = {
            "clean_accepted_by_model": clean_verdict["accepted"] and clean_verdict["decided_by"] == "model",
            "planted_rejected_with_named_blocker": (not planted_verdict["accepted"] and
                                                      planted_verdict["decided_by"] == "model" and blocking_match),
            "repair_accepted_by_model": repair_verdict["accepted"] and repair_verdict["decided_by"] == "model",
            "repair_removed_planted_text": evidence["repair_lacks_planted_text"],
        }
    finally:
        evidence["process_exits"] = stop_services(processes)
        evidence["remaining_listeners_45740_45759"] = free_ports()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", required=True, choices=("scripted", "codex-subscription"))
    parser.add_argument("--home", type=Path)
    args = parser.parse_args()
    live = args.provider == "codex-subscription"
    overrides = {name: name in os.environ for name in
                 ("EXO_MODEL_HOME", "EXO_CODEX_BASE_URL", "OPENAI_API_KEY", "ANTHROPIC_API_KEY")}
    if live and (overrides["EXO_MODEL_HOME"] or overrides["EXO_CODEX_BASE_URL"]):
        raise RuntimeError("live provider requires default broker home and endpoint")
    if free_ports():
        raise RuntimeError(f"probe service port block occupied: {free_ports()}")
    home = args.home or Path("/tmp/exo-sf-probe-2" if live else "/tmp/exo-sf-probe-1")
    if home.exists():
        raise FileExistsError(f"trial home already exists: {home}")
    home.mkdir(mode=0o700)
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    filename = "agent-probe-live-1.json" if live else "agent-probe-scripted-1.json"
    evidence = {"label": "pre-integration probe (not counted)",
                "observation": "observed-real" if live else "observed-synthetic",
                "provider": args.provider, "model_id": "gpt-6-sol", "home": str(home),
                "evidence_file": filename, "credential_variable_names_set": overrides,
                "started_at": time.time(), "status": "running", "tasks": [],
                "verdicts": [], "reports": {}}
    if live:
        evidence["broker_status_before"] = broker_status()
        if not evidence["broker_status_before"]["signed_in"] or evidence["broker_status_before"]["expired"]:
            raise RuntimeError("default broker is not signed in and current")
    path = EVIDENCE / filename
    write_json(path, evidence)
    try:
        run_probe(home, args.provider, evidence)
        evidence["status"] = "pass" if all(evidence["probe_checks"].values()) else "behavioral-fail"
    except Exception as error:
        evidence["status"] = "error"
        evidence["failure"] = {"type": type(error).__name__, "message": str(error)}
    finally:
        evidence["ended_at"] = time.time()
        evidence["duration_s"] = round(evidence["ended_at"] - evidence["started_at"], 3)
        evidence["remaining_listeners_45740_45759"] = free_ports()
        if live:
            evidence["broker_status_after"] = broker_status()
        write_json(path, evidence)
    if live:
        try:
            control = positive_control(home)
            evidence["leak_scan_positive_control"] = control
            from model_broker import DEFAULT_HOME  # default path only; no credential read
            scan = scan_paths([Path("/tmp/exo-sf-probe-1"), home, EVIDENCE, DEFAULT_HOME])
            evidence["leak_scan"] = scan
            if scan.get("hits"):
                evidence["status"] = "leak-scan-fail"
            write_json(path, evidence)
            evidence["leak_scan_final"] = scan_paths([Path("/tmp/exo-sf-probe-1"), home,
                                                      EVIDENCE, DEFAULT_HOME])
            if evidence["leak_scan_final"].get("hits"):
                evidence["status"] = "leak-scan-fail"
            write_json(path, evidence)
        except Exception as error:
            evidence["status"] = "leak-scan-error"
            evidence["leak_scan_failure"] = {"type": type(error).__name__, "message": str(error)}
            write_json(path, evidence)
    print(json.dumps({"status": evidence["status"], "label": evidence["label"],
                      "evidence": str(path), "home": str(home),
                      "checks": evidence.get("probe_checks"),
                      "remaining_listeners": evidence["remaining_listeners_45740_45759"]}))
    return 0 if evidence["status"] == "pass" and not evidence["remaining_listeners_45740_45759"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
