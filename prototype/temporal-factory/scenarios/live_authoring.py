"""Broker-backed authoring followed by a real factory A2A/Temporal run.

The synthetic provider is a strict loopback Codex protocol fixture. Its model
output and credential are synthetic; the harness, runner, and Temporal run are
ordinary factory-mode components.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import secrets
import socket
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from common import (PY, ROOT, SRC, a2a_send, file_sha256, http, jsonl, poll_task, run_cli,
                    sqlite_rows, start_harness, stop_process, write_evidence)

PORT = 44830
TESTBED_PORT = 45300
MOCK_PORT = 46130


def claim(value, level: str) -> dict:
    return {"level": level, "value": value}


def reject_inherited_live_override(provider: str, environment: dict) -> None:
    if provider == "codex-subscription" and "EXO_CODEX_BASE_URL" in environment:
        raise ValueError("codex-subscription refuses an inherited EXO_CODEX_BASE_URL")
    if provider == "codex-subscription" and "EXO_MODEL_HOME" in environment:
        raise ValueError("codex-subscription requires the default model home")


def self_test_live_guard() -> None:
    requests = []

    class Recorder(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            self.send_response(200)
            self.end_headers()

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 46131), Recorder)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        try:
            reject_inherited_live_override("codex-subscription",
                {"EXO_CODEX_BASE_URL": "http://127.0.0.1:46131/backend-api"})
        except ValueError:
            pass
        else:
            raise AssertionError("inherited override was accepted")
        try:
            reject_inherited_live_override("codex-subscription",
                {"EXO_MODEL_HOME": "/tmp/fixture-model-home"})
        except ValueError:
            pass
        else:
            raise AssertionError("inherited model home was accepted")
        checked(not requests, "live guard made an HTTP request")
        print(json.dumps({"status": "pass", "test": "live_override_guard",
                          "loopback_requests": len(requests)}))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def checked(condition: bool, description: str) -> None:
    if not condition:
        raise AssertionError(description)


def cli_json(*args: str, timeout: float = 300) -> dict:
    return json.loads(run_cli(*args, timeout=timeout))


def subscription_status() -> dict:
    """Use the broker's local, network-free status command before live setup."""
    completed = subprocess.run([os.environ.get("EXO_NODE", "node"),
        str(ROOT / "broker" / "exo-model.mjs"), "status"], cwd=ROOT,
        text=True, capture_output=True, timeout=30)
    if completed.returncode:
        raise RuntimeError("broker status command failed")
    status = json.loads(completed.stdout)
    checked(isinstance(status.get("signed_in"), bool), "broker status omitted signed_in")
    account = status.get("account")
    checked((not status["signed_in"] and account is None) or
            (isinstance(account, str) and account.startswith("sha256:") and
             len(account) == 19), "invalid broker account hash")
    return {key: status.get(key) for key in ("signed_in", "account", "expires_at", "expired")}


def broker_errors(events: list[dict]) -> list[dict]:
    """Keep only the structured broker error fields allowed in evidence."""
    return [{key: row[key] for key in ("kind", "status", "code") if key in row}
            for row in events if row.get("event") == "error"]


def _b64(value: dict) -> str:
    return base64.urlsafe_b64encode(json.dumps(value, separators=(",", ":")).encode()).decode().rstrip("=")


def synthetic_credential(model_home: Path) -> None:
    """Create a fresh, fixture-marked OAuth store without exposing its bytes."""
    secrets_dir = model_home / "secrets"
    model_home.mkdir(mode=0o700)
    secrets_dir.mkdir(mode=0o700)
    marker = model_home / "FIXTURE_STORE"
    marker_fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(marker_fd, "w") as stream:
        stream.write("synthetic test store\n")
    access = ".".join((_b64({"alg": "none", "typ": "JWT",
                             "kid": secrets.token_hex(12)}),
                       _b64({"exp": 4102444800, "https://api.openai.com/auth":
                             {"chatgpt_account_id": "acct_synthetic"},
                             "jti": secrets.token_hex(12)}),
                       secrets.token_urlsafe(24)))
    credential = {"type": "oauth", "access": access,
                  "refresh": "synthetic-refresh-" + secrets.token_urlsafe(32),
                  "expires": 4102444800000, "accountId": "acct_synthetic"}
    path = secrets_dir / "openai-codex.json"
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        json.dump(credential, stream)
        stream.write("\n")
    checked((path.stat().st_mode & 0o777) == 0o600, "synthetic credential mode")


def mock_headers(home: Path) -> list[dict]:
    path = home / "mock-requests.jsonl"
    rows = jsonl(path)
    return [{"kind": row.get("kind"),
             "originator": row.get("originator") or row.get("headers", {}).get("originator"),
             "user_agent": row.get("user_agent") or row.get("user-agent") or row.get("ua") or
                           row.get("headers", {}).get("user-agent"),
             "rejected": row.get("kind") == "rejected"}
            for row in rows]


async def export_histories(address: str, run_id: str, directory: Path) -> dict:
    from temporalio.client import Client
    from factory import FactoryRun

    client = await Client.connect(address, namespace="exomachina")
    parent = client.get_workflow_handle(run_id)
    status = await parent.query(FactoryRun.status)
    ids = [run_id]
    if status.get("child_id"):
        ids.append(status["child_id"])
    directory.mkdir(parents=True, exist_ok=True)
    result = {}
    for workflow_id in ids:
        handle = client.get_workflow_handle(workflow_id)
        description = await handle.describe()
        history = await handle.fetch_history()
        document = json.loads(history.to_json())
        name = "parent" if workflow_id == run_id else "child"
        path = directory / f"{name}.json"
        path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
        info = description.raw_description.workflow_execution_info
        version = info.versioning_info
        result[name] = {"workflow_id": workflow_id, "status": description.status.name,
                        "history_path": str(path), "event_count": len(document["events"]),
                        "versioning": {
                            "behavior": int(version.behavior),
                            "deployment": version.versioning_override.pinned.version.deployment_name,
                            "build_id": version.versioning_override.pinned.version.build_id}}
    return {"parent_status_query": status, "workflows": result}


def candidate_files() -> list[Path]:
    repo = ROOT.parents[1]
    completed = subprocess.run(["git", "ls-files", "-z", "--cached", "--others",
                                "--exclude-standard", "--", "prototype/temporal-factory/"],
                               cwd=repo, capture_output=True)
    checked(completed.returncode == 0, "commit-candidate listing failed")
    paths = {repo / name.decode() for name in completed.stdout.split(b"\0") if name}
    return sorted(path for path in paths if path.is_file())


def scan_paths(paths: list[Path], *, fixture_model_home: Path | None = None) -> dict:
    command = [os.environ.get("EXO_NODE", "node"), str(ROOT / "broker" / "exo-model.mjs"),
               "leak-scan", *(str(path) for path in paths)]
    environment = os.environ.copy()
    if fixture_model_home is not None:
        checked((fixture_model_home / "FIXTURE_STORE").is_file(),
                "positive-control model home lacks fixture marker")
        environment["EXO_MODEL_HOME"] = str(fixture_model_home)
    completed = subprocess.run(command, cwd=ROOT, env=environment,
                               text=True, capture_output=True, timeout=180)
    checked(completed.returncode == 0, "broker leak-scan failed")
    return json.loads(completed.stdout)


def positive_control(home: Path) -> dict:
    control = home.with_name(home.name + "-leak-control")
    checked(not control.exists(), "leak-control directory already exists")
    control.mkdir(mode=0o700)
    fixture_home = control / "model"
    synthetic_credential(fixture_home)
    source = fixture_home / "secrets" / "openai-codex.json"
    copied = control / "credential-copy.json"
    bare = control / "bare-access.txt"
    script = ("import fs from 'node:fs';"
              "const [source, copied, bare] = process.argv.slice(1);"
              "const raw = fs.readFileSync(source);"
              "const credential = JSON.parse(raw.toString());"
              "if (typeof credential.access !== 'string' || !credential.access) process.exit(2);"
              "fs.writeFileSync(copied, raw, {flag:'wx',mode:0o600});"
              "fs.writeFileSync(bare, credential.access, {flag:'wx',mode:0o600});")
    completed = subprocess.run([os.environ.get("EXO_NODE", "node"), "--input-type=module",
                                "-e", script, str(source), str(copied), str(bare)],
                               cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    checked(completed.returncode == 0, "leak-control planting failed")
    observed = scan_paths([control], fixture_model_home=fixture_home)
    found = {row["path"] for row in observed.get("hits", [])}
    checked(str(copied) in found and str(bare) in found,
            "leak-scan positive control did not detect both planted files")
    return {"canary": "fresh synthetic credential; no live token used",
            "control_directory": str(control), "fixture_model_home": str(fixture_home),
            "copied_credential": str(copied), "bare_access": str(bare), "scan": observed}


def leak_scan(home: Path, model_home: Path, evidence_path: Path, histories: Path,
              control_directory: Path, *, live: bool) -> dict:
    if live:
        from model_broker import DEFAULT_HOME
        checked("EXO_MODEL_HOME" not in os.environ and model_home.resolve() == DEFAULT_HOME.resolve(),
                "live leak scan must use the default model home")
    files = candidate_files()
    paths = [home, model_home, control_directory, evidence_path, histories, *files]
    checked(control_directory in paths, "real-store scan omitted positive-control directory")
    result = scan_paths(paths)
    return {**result, "candidate_file_count": len(files),
            "input_path_count": len(paths), "model_home": str(model_home),
            "evidence_path": str(evidence_path), "histories_path": str(histories),
            "control_directory": str(control_directory), "control_included": True,
            "scan_purpose": "real-store token absent outside canonical store" if live else
                            "scenario-store token absent outside canonical store"}


def main() -> None:
    if sys.argv[1:] == ["--self-test-live-guard"]:
        self_test_live_guard()
        return
    parser = argparse.ArgumentParser()
    parser.add_argument("--home", type=Path, required=True)
    parser.add_argument("--provider", choices=("synthetic-loopback", "codex-subscription"), required=True)
    parser.add_argument("--max-rounds", type=int, default=4)
    parser.add_argument("--max-model-calls", type=int, default=12)
    parser.add_argument("--max-tool-calls", type=int, default=24)
    parser.add_argument("--deadline-seconds", type=float, default=600)
    args = parser.parse_args()
    evidence_path = ROOT / "evidence" / f"live-authoring-{args.provider}.json"
    scan_path = ROOT / "evidence" / f"live-authoring-{args.provider}-scan.json"
    broker_status = None
    if args.provider == "codex-subscription":
        try:
            reject_inherited_live_override(args.provider, os.environ)
        except ValueError:
            override = ("EXO_CODEX_BASE_URL" if "EXO_CODEX_BASE_URL" in os.environ
                        else "EXO_MODEL_HOME")
            path = write_evidence(evidence_path.name, {
                "provider": args.provider, "status": "fail",
                "failure": {"step": "inherited live override guard",
                            "reason": f"{override} is inherited"},
                "claims": {"http_requests": claim(0, "real")}})
            print(json.dumps({"status": "fail", "step": "inherited live override guard",
                              "evidence": str(path)}))
            raise SystemExit(1)
        try:
            broker_status = subscription_status()
        except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as error:
            path = write_evidence(evidence_path.name, {
                "provider": args.provider, "status": "fail",
                "failure": {"step": "broker status", "error_type": type(error).__name__}})
            print(json.dumps({"status": "fail", "step": "broker status", "evidence": str(path)}))
            raise SystemExit(1) from None
        if not broker_status["signed_in"]:
            path = write_evidence(evidence_path.name, {
                "provider": args.provider, "status": "fail",
                "failure": {"step": "broker status", "reason": "not signed in"},
                "claims": {"broker_status": claim(broker_status, "real")}})
            print(json.dumps({"status": "fail", "reason": "not signed in", "evidence": str(path)}))
            raise SystemExit(1)
    home = args.home.resolve()
    checked(home.parent == Path("/tmp").resolve() and home.name.startswith("exo-proto-live-"),
            "home must be a /tmp/exo-proto-live-* trial")
    checked(not home.exists(), "use a fresh trial home")
    checked(all(value > 0 for value in (args.max_rounds, args.max_model_calls,
                                        args.max_tool_calls, args.deadline_seconds)),
            "authoring budget limits must be positive")
    home.mkdir(parents=True)
    level = "synthetic" if args.provider == "synthetic-loopback" else "real"
    limits = {"max_rounds": args.max_rounds, "max_model_calls": args.max_model_calls,
              "max_tool_calls": args.max_tool_calls, "deadline_seconds": args.deadline_seconds}
    evidence: dict = {"provider": args.provider, "home": str(home),
                      "status": "running", "started_at": time.time(), "claims": {},
                      "limits": limits, "model": {"provider": args.provider,
                        "billing": "none" if level == "synthetic" else "subscription",
                        "live": level == "real"},
                      "director_model": "fixture", "release": "http-release (fixture)",
                      "wall_times": {"level": "real", "value": {}}}
    if broker_status is not None:
        evidence["claims"]["broker_status"] = claim(broker_status, "real")
    histories_dir = ROOT / "evidence" / f"live-authoring-{args.provider}"
    instance = home / "instances" / "research-factory"
    mock = None
    harness = None
    cleaned = False
    step = "configure"
    started = time.monotonic()
    os.environ["EXO_RUNNER_PORT_BASE"] = "44100"
    os.environ["EXO_RUNNER_MEMBER_BASE"] = "32420"
    os.environ["EXO_AUTHOR_PROVIDER"] = args.provider
    if args.provider == "synthetic-loopback":
        os.environ["EXO_MODEL_HOME"] = str(home / "model")
        os.environ["EXO_CODEX_BASE_URL"] = f"http://127.0.0.1:{MOCK_PORT}/backend-api"
        os.environ["MOCK_PORT"] = str(MOCK_PORT)
        os.environ["MOCK_LOG"] = str(home / "mock-requests.jsonl")
        os.environ["OPENAI_API_KEY"] = "synthetic-api-key-must-not-be-used"

    def cleanup() -> list[str]:
        nonlocal cleaned
        if cleaned:
            return []
        cleaned = True
        errors = []
        if harness is not None:
            try:
                evidence["cleanup_harness_exit"] = stop_process(harness)
            except Exception as error:
                errors.append("harness:" + type(error).__name__)
        if (home / "runner").exists():
            try:
                evidence["cleanup_runner"] = cli_json(str(SRC / "runner.py"), "stop", "--home", str(home))
            except Exception as error:
                errors.append("runner:" + type(error).__name__)
        if mock is not None:
            try:
                evidence["cleanup_mock_exit"] = stop_process(mock)
            except Exception as error:
                errors.append("mock:" + type(error).__name__)
        if level == "synthetic":
            try:
                from model_broker import ModelBroker
                broker_to_stop = ModelBroker(home / "model")
                if broker_to_stop.is_running():
                    broker_to_stop.stop()
                    deadline = time.monotonic() + 10
                    while broker_to_stop.is_running() and time.monotonic() < deadline:
                        time.sleep(0.1)
                    checked(not broker_to_stop.is_running(), "broker did not stop")
                    evidence["cleanup_broker"] = "stopped"
            except Exception as error:
                errors.append("broker:" + type(error).__name__)
        if (home / "testbed").exists():
            try:
                evidence["cleanup_testbed"] = cli_json(str(ROOT / "services" / "testbed.py"),
                    "down", "--home", str(home), "--port-base", str(TESTBED_PORT))
            except Exception as error:
                errors.append("testbed:" + type(error).__name__)
        evidence["cleanup_errors"] = errors
        return errors

    try:
        if args.provider == "synthetic-loopback":
            step = "synthetic credential and mock"
            synthetic_credential(home / "model")
            from authoring import _scripted_first_draft
            draft = home / "mock-first-draft.json"
            draft.write_text(json.dumps(_scripted_first_draft(json.loads(
                (ROOT / "definitions" / "v1-template.json").read_text()))) + "\n")
            log = (home / "mock.log").open("a")
            mock = subprocess.Popen([os.environ.get("EXO_NODE", "node"),
                                     str(ROOT / "broker" / "testing" / "mock-codex.mjs"),
                                     "--port", str(MOCK_PORT), "--record",
                                     str(home / "mock-requests.jsonl"),
                                     "--script", "authoring", "--draft", str(draft)],
                                    cwd=ROOT, stdout=log, stderr=log,
                                    start_new_session=True)
            log.close()
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if mock.poll() is not None:
                    raise RuntimeError("mock exited during startup")
                try:
                    with socket.create_connection(("127.0.0.1", MOCK_PORT), timeout=1):
                        pass
                    break
                except OSError:
                    time.sleep(0.1)
            else:
                raise TimeoutError("mock did not become ready")

        step = "testbed and v1 publication"
        tick = time.monotonic()
        evidence["claims"]["testbed"] = claim(cli_json(str(ROOT / "services" / "testbed.py"),
            "up", "--home", str(home), "--port-base", str(TESTBED_PORT)), "synthetic")
        run_cli(str(SRC / "admin.py"), "provision", "--instance-dir", str(instance),
                "--name", "research-factory", "--port", str(PORT), "--home", str(home),
                "--testbed", str(home / "testbed"))
        v1 = cli_json(str(SRC / "admin.py"), "publish-template", "--instance-dir", str(instance),
                      "--template", str(ROOT / "definitions" / "v1-template.json"), "--label", "v1")
        evidence["claims"]["v1_publication"] = claim(v1, "real")
        evidence["wall_times"]["value"]["provision_and_publish_v1_s"] = round(time.monotonic() - tick, 3)

        step = "cold startup assertions"
        from model_broker import ModelBroker
        broker = ModelBroker(home / "model" if level == "synthetic" else None)
        broker_events_path = broker.home / "broker-events.jsonl"
        broker_running_before = broker.is_running()
        before_events = jsonl(broker_events_path) if broker_events_path else []
        if level == "synthetic":
            checked(not broker_running_before, "broker running before authoring")
            checked(not before_events, "broker started before authoring")
        checked(not (home / "runner" / "runner-ready.json").exists(), "runner started before harness")
        harness = start_harness(instance, PORT)
        health = http(f"http://127.0.0.1:{PORT}/health", token=False)
        agent_card = http(f"http://127.0.0.1:{PORT}/.well-known/agent-card.json", token=False)
        checked([skill["id"] for skill in agent_card["skills"]] == ["verified-research@1"],
                "factory did not expose its normal A2A capability")
        checked(not health["runner_running"], "routine harness startup started runner")
        checked(not (home / "runner" / "runner-ready.json").exists(), "runner started at harness startup")
        if level == "synthetic":
            checked(not broker.is_running(), "harness startup started broker")
        evidence["claims"]["cold_start"] = claim({"health": health, "broker_events_before": before_events,
            "runner_ready_before": False, "broker_ready_before": broker_running_before}, "real")
        evidence["claims"]["agent_card"] = claim(agent_card, "real")

        step = "broker authoring and v2 publication"
        tick = time.monotonic()
        author_started_at = time.time()
        author_args = [str(SRC / "admin.py"), "author", "--instance-dir", str(instance),
                       "--brief", str(ROOT / "definitions" / "authoring-brief-v2.md"),
                       "--label", "v2", "--base-template", str(ROOT / "definitions" / "v1-template.json"),
                       "--max-rounds", str(args.max_rounds), "--max-model-calls", str(args.max_model_calls),
                       "--max-tool-calls", str(args.max_tool_calls),
                       "--deadline-seconds", str(args.deadline_seconds)]
        completed = subprocess.run([PY, "-B", *author_args], cwd=ROOT, text=True,
                                   capture_output=True, timeout=args.deadline_seconds + 90)
        try:
            authored = json.loads(completed.stdout)
        except ValueError:
            authored = {"status": "failed", "outcome": {},
                        "failure_reason": "admin_author_no_json"}
        checked(isinstance(authored, dict), "admin author did not return an object")
        evidence["wall_times"]["value"]["author_and_publish_v2_s"] = round(time.monotonic() - tick, 3)
        outcome = authored.get("outcome", {})
        events = jsonl(broker_events_path) if broker_events_path else []
        author_events = events[len(before_events):]
        selection_events = [row for row in author_events
                            if row.get("reason") == "authoring-selection" and
                            row.get("event") in {"start", "attach"}]
        selection_seconds = authored.get("selection_seconds")
        if selection_seconds is None and selection_events:
            selection_seconds = round(max(0, selection_events[0]["at"] - author_started_at), 3)
        if selection_seconds is None:
            selection_seconds = round(time.monotonic() - tick, 3)
        evidence["wall_times"]["value"]["selection_seconds"] = selection_seconds
        model_calls = sum(row.get("reason") == "model-call" and
                          row.get("event") in {"start", "attach"} for row in author_events)
        error_fields = broker_errors(author_events)
        evidence["claims"]["originator_acceptance"] = claim(
            {"result": "success"} if authored.get("status") == "published" else
            {"result": "error", **error_fields[-1]} if error_fields else
            {"result": "unobserved"}, level)
        if broker.is_running():
            evidence["claims"]["broker_health"] = claim(broker.health(), "real")
        else:
            evidence["claims"]["broker_health"] = claim({"available": False}, "real")
        evidence["claims"]["authoring"] = claim({"status": authored.get("status"),
            "model": outcome.get("model"), "rounds": outcome.get("rounds"),
            "tool_calls": [call.get("tool") for call in outcome.get("tool_calls", [])],
            "provider_selection": authored.get("live"), "limits": limits,
            "abort": outcome.get("abort"), "model_calls": model_calls,
            "tool_call_count": (outcome.get("abort") or {}).get("tool_calls",
                len(outcome.get("tool_calls", []))),
            "selection_seconds": selection_seconds,
            "admin_exit_code": completed.returncode}, level)
        if completed.returncode != 0 or authored.get("status") != "published":
            publications = cli_json(str(SRC / "admin.py"), "publications",
                                    "--instance-dir", str(instance))
            checked(publications["active"]["label"] == "v1" and
                    len(publications["all"]) == 1, "failed authoring changed publication")
            evidence["claims"]["failed_publication_state"] = claim({
                "active": publications["active"], "publication_count": 1,
                "v2_run_started": False}, "real")
            reason = ((outcome.get("abort") or {}).get("reason") or
                      authored.get("failure_reason") or
                      (error_fields[-1].get("kind") if error_fields else None) or
                      authored.get("status") or "admin_error")
            evidence["claims"]["authoring"]["value"]["failure_reason"] = reason
            raise RuntimeError("authoring failed")
        evidence["claims"]["approval"] = claim(authored.get("approval"), "real")
        evidence["claims"]["v2_publication"] = claim(authored.get("publication"), "real")
        checked(authored.get("status") == "published", "authoring did not publish")
        checked(outcome.get("rounds") and not outcome["rounds"][0]["valid"] and
                any(row["valid"] for row in outcome["rounds"]), "authoring did not revise an invalid draft")
        checked(authored["approval"]["status"] == "approved" and
                authored["approval"]["policy"] == "auto", "v2 lacked auto approval")
        checked(authored["publication"]["package_digest"] != v1["publication"]["package_digest"],
                "authored v2 equals v1")
        from definition import validate
        checked(validate(outcome["package"], json.loads((instance / "catalog" /
            "approved_bindings.json").read_text())) == authored["publication"]["package_digest"],
            "authored v2 did not validate")
        checked(outcome.get("model", {}).get("provider") == args.provider, "API-key provider selected")
        checked(outcome.get("model", {}).get("billing") ==
                ("none" if level == "synthetic" else "subscription"), "wrong billing path")
        checked(outcome.get("model", {}).get("live") is (level == "real"), "wrong live label")
        checked(any(row.get("reason") in {"model-call", "authoring-selection"}
                    for row in author_events), "authoring did not attach to broker")
        checked(0 < model_calls <= limits["max_model_calls"],
                "authoring model-call count exceeded budget")
        evidence["claims"]["broker_start"] = claim({"events": author_events,
            "already_running": broker_running_before}, "real")
        broker_health = broker.health()
        evidence["claims"]["broker_health"] = claim(broker_health, "real")
        checked(broker_health.get("provider") == "openai-codex", "wrong broker provider")
        if level == "synthetic":
            headers = mock_headers(home)
            evidence["claims"]["mock_headers"] = claim(headers, "synthetic")
            requests = [row for row in headers if row["kind"] == "request"]
            checked(requests and not any(row["rejected"] for row in headers), "mock rejected provider call")
            checked(all(row["originator"] == "exomachina" for row in requests), "wrong originator")
            checked(all(str(row["user_agent"]).startswith("exomachina-model-broker/")
                        for row in requests), "wrong user-agent")
            checked(len(requests) == model_calls, "mock/model-call counts differ")
        evidence["claims"]["api_key_provider"] = claim({
            "consulted": False, "selected_provider": args.provider,
            "observed_model_calls": model_calls,
            "synthetic_api_key_poisoned": level == "synthetic"}, level)

        step = "v2 A2A run"
        tick = time.monotonic()
        base = f"http://127.0.0.1:{PORT}"
        sent = a2a_send(base, {"op": "start", "action_id": "live-authoring:v2",
                               "inputs": {"question": "Is the documented capability usable now?"}})
        final = poll_task(base, sent["id"], {"completed", "failed"}, seconds=300)
        evidence["wall_times"]["value"]["v2_a2a_terminal_s"] = round(time.monotonic() - tick, 3)
        evidence["claims"]["a2a_task"] = claim({"send_state": sent["status"]["state"],
            "task": final}, "real")
        evidence["claims"]["director_model"] = claim({"director_model": "fixture",
            "implementation": "ToolCallingModelFixture", "broker_scope": "authoring only"}, "synthetic")
        checked(final["status"]["state"] == "completed", "v2 task failed")
        run_id = final["metadata"]["run_id"]
        rows = sqlite_rows(instance / "director.sqlite3",
                           "SELECT run_id, task_id, label, manifest_digest, package_digest, build_id, closed FROM runs")
        run = next(row for row in rows if row["run_id"] == run_id)
        checked(run["manifest_digest"] == authored["publication"]["manifest_digest"],
                "v2 run used wrong publication")
        checked(run["build_id"] == authored["publication"]["build_id"], "v2 run used wrong build")
        evidence["claims"]["pinned_run"] = claim(run, "real")
        released = [row for row in sqlite_rows(home / "services" / "release" / "release.sqlite3",
                                                "SELECT * FROM releases")
                    if row["run_id"].startswith(run_id)]
        checked(len(released) == 1, "expected one HTTP release fixture effect")
        evidence["claims"]["release"] = claim({"transport": "http-release (fixture)",
            "effects": released}, "synthetic")

        step = "Temporal history export"
        tick = time.monotonic()
        address = json.loads((home / "runner" / "runner-ready.json").read_text())["address"]
        histories = asyncio.run(export_histories(address, run_id, histories_dir))
        evidence["wall_times"]["value"]["history_export_s"] = round(time.monotonic() - tick, 3)
        checked(all(row["status"] == "COMPLETED" for row in histories["workflows"].values()),
                "v2 workflows did not complete")
        checked(all(row["versioning"]["build_id"] == run["build_id"]
                    for row in histories["workflows"].values()), "Temporal build pin mismatch")
        evidence["claims"]["temporal"] = claim(histories, "real")

        evidence["status"] = "pass"
    except Exception as error:
        evidence["status"] = "fail"
        evidence["failure"] = {"step": step, "error_type": type(error).__name__}
        if step == "broker authoring and v2 publication":
            reason = evidence.get("claims", {}).get("authoring", {}).get("value", {}).get("failure_reason")
            if reason:
                evidence["failure"]["reason"] = reason
    cleanup_errors = cleanup()
    if cleanup_errors and evidence["status"] == "pass":
        evidence["status"] = "fail"
        evidence["failure"] = {"step": "cleanup", "reason": "process cleanup failed"}
    if level == "synthetic":
        model_home = home / "model"
        scan_enabled = (model_home / "secrets" / "openai-codex.json").is_file()
    else:
        from model_broker import DEFAULT_HOME
        model_home = DEFAULT_HOME
        scan_enabled = True
    control_directory = home.with_name(home.name + "-leak-control")
    try:
        control = positive_control(home)
        evidence["claims"]["leak_scan_positive_control"] = claim(control, "synthetic")
    except Exception as error:
        evidence["status"] = "fail"
        evidence["failure"] = {"step": "leak scan positive control",
                               "error_type": type(error).__name__}
    evidence["finished_at"] = time.time()
    evidence["wall_times"]["value"]["total_s"] = round(time.monotonic() - started, 3)
    write_evidence(evidence_path.name, evidence)
    if scan_enabled:
        try:
            tick = time.monotonic()
            scan = leak_scan(home, model_home, evidence_path, histories_dir,
                             control_directory, live=level == "real")
            evidence["wall_times"]["value"]["leak_scan_s"] = round(time.monotonic() - tick, 3)
            evidence["claims"]["leak_scan"] = claim(scan, "real")
            if level == "real":
                evidence["claims"]["real_token_absence"] = claim({
                    "zero_hits": not scan["hits"], "control_directory_scanned": scan["control_included"],
                    "scope": "trial home, default model home, synthetic control, evidence, commit candidates"},
                    "real")
            if scan["hits"]:
                evidence["status"] = "fail"
                evidence["failure"] = {"step": "leak scan", "reason": "credential material found"}
            write_evidence(evidence_path.name, evidence)
            final_scan = leak_scan(home, model_home, evidence_path, histories_dir,
                                   control_directory, live=level == "real")
            if final_scan["hits"] and evidence["status"] == "pass":
                evidence["status"] = "fail"
                evidence["failure"] = {"step": "final leak scan",
                                       "reason": "credential material found"}
                write_evidence(evidence_path.name, evidence)
                final_scan = leak_scan(home, model_home, evidence_path, histories_dir,
                                       control_directory, live=level == "real")
            write_evidence(scan_path.name, {"status": evidence["status"], "evidence": str(evidence_path),
                "evidence_sha256": file_sha256(evidence_path), "final_scan": final_scan,
                "positive_control_detected": "leak_scan_positive_control" in evidence["claims"],
                "positive_control_canary": "fresh synthetic credential; no live token used",
                "real_store_zero_hits": not final_scan["hits"] if level == "real" else None})
        except Exception as error:
            evidence["status"] = "fail"
            evidence["failure"] = {"step": "final leak scan", "error_type": type(error).__name__}
            write_evidence(evidence_path.name, evidence)
            write_evidence(scan_path.name, {"status": "fail", "evidence": str(evidence_path),
                "evidence_sha256": file_sha256(evidence_path),
                "reason": "final leak scan could not verify evidence"})
    else:
        if evidence["status"] == "pass":
            evidence["status"] = "fail"
            evidence["failure"] = {"step": "final leak scan", "reason": "credential unavailable"}
            write_evidence(evidence_path.name, evidence)
        write_evidence(scan_path.name, {"status": "unverified", "evidence": str(evidence_path),
            "evidence_sha256": file_sha256(evidence_path), "reason": "credential unavailable"})
    result = {"status": evidence["status"], "evidence": str(evidence_path),
              "scan_evidence": str(scan_path), "provider": args.provider}
    if evidence["status"] == "pass":
        result["run_id"] = run_id
    else:
        result["failure"] = evidence.get("failure")
    print(json.dumps(result))
    if evidence["status"] != "pass":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
