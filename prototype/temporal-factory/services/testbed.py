"""Start pinned, independent fixture services for local factory trials."""
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
from agent_binding import pin
from agent_roles import RUBRIC_DIGEST
SERVICE_NAMES = (
    "source_alpha", "source_beta", "counter_alpha", "counter_beta", "quality", "release"
)
CAPABILITIES = {
    "source_alpha": "source_evidence@1",
    "source_beta": "source_evidence@1",
    "counter_alpha": "counter_evidence@1",
    "counter_beta": "counter_evidence@1",
    "quality": "quality-review@1",
    "release": "release@1",
}
QUALITY_POLICY = {
    "policy": "exact-revision-independent-review@1",
    "reviewer_binding": "quality",
    "author_may_not_review": True,
    "repair_bound_max": 2,
}
REPORT_NAMES = ("research_findings", "research_risks", "synthesizer", "quality", "release")
REPORT_CAPABILITIES = {
    "research_findings": "packet_findings@1",
    "research_risks": "packet_risks@1",
    "synthesizer": "report_synthesis@1",
    "quality": "report_quality_review@1",
    "release": "release@1",
}
REPORT_QUALITY_POLICY = {**QUALITY_POLICY, "rubric": "report-quality@1",
                         "rubric_digest": RUBRIC_DIGEST}
_CHILDREN: dict[int, subprocess.Popen] = {}


def report_role(name: str) -> str:
    return "quality" if name == "quality" else "release" if name == "release" else "capability"


def report_bindings(health: dict[str, dict], port_base: int) -> dict[str, dict]:
    return {name: {"role": report_role(name), "url": f"http://127.0.0.1:{port_base + index}",
                   "identity": health[name]["identity"], "approved": True}
            for index, name in enumerate(REPORT_NAMES)}


def report_contracts(bindings: dict[str, dict]) -> dict[str, dict]:
    contracts = {}
    for name in REPORT_NAMES:
        if name == "release":
            contracts[name] = contract_records()["release"]
        else:
            binding = bindings[name]
            contracts[name] = {"name": name, "role": report_role(name),
                               "capability": REPORT_CAPABILITIES[name],
                               **pin(binding["url"], binding["identity"])}
    return contracts


def role_for(name: str) -> str:
    return "quality" if name == "quality" else "release" if name == "release" else "capability"


def binding_records(health: dict[str, dict], port_base: int) -> dict[str, dict]:
    """Build the exact binding shape accepted by definition.validate."""
    return {
        name: {
            "role": role_for(name),
            "url": f"http://127.0.0.1:{port_base + index}",
            "identity": health[name]["identity"],
            "approved": True,
        }
        for index, name in enumerate(SERVICE_NAMES)
    }


def contract_records() -> dict[str, dict]:
    contracts = {}
    for name in SERVICE_NAMES:
        role = role_for(name)
        if role == "capability":
            input_contract = {"transport": "a2a-message/send", "operation": "assign",
                              "fields": ["action_id", "run_id", "definition_digest", "brief"]}
            output_contract = {"artifact": ["revision", "sha256", "author", "content"]}
            lookup = "/fixture/actions/{id}"
        elif role == "quality":
            input_contract = {"transport": "a2a-message/send", "operation": "review",
                              "fields": ["action_id", "run_id", "definition_digest", "artifact"]}
            output_contract = {"verdict": ["accepted", "revision", "sha256", "reviewer", "reason"]}
            lookup = "/fixture/actions/{id}"
        else:
            input_contract = {"transport": "http-post", "path": "/release",
                              "fields": ["release_id", "run_id", "definition_digest",
                                         "revision", "sha256", "content"]}
            output_contract = {"receipt": ["release_id", "run_id", "definition_digest",
                                           "revision", "sha256", "attempts",
                                           "accepted_effect_count"]}
            lookup = "/receipts/{id}"
        contracts[name] = {
            "name": name, "role": role, "capability": CAPABILITIES[name],
            "a2a_protocol": "0.3.0", "input": input_contract, "output": output_contract,
            "operations": {"idempotent_action_id": True, "lookup": lookup},
            "attested": False,
        }
    return contracts


def write_metadata(home: Path, bindings: dict[str, dict], pids: dict[str, dict],
                   contracts: dict[str, dict] | None = None, *, snapshot: bool = False,
                   quality_policy: dict | None = None) -> None:
    directory = home / "testbed"
    directory.mkdir(parents=True, exist_ok=True)
    for name, value in (
        ("approved_bindings.json", bindings),
        ("contracts.json", contracts if contracts is not None else contract_records()),
        ("quality_policy.json", quality_policy if quality_policy is not None else QUALITY_POLICY),
        ("pids.json", pids),
    ):
        (directory / name).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    if snapshot:
        value = {"snapshot_version": 1,
                 "agents": {binding["identity"]: {"url": binding["url"]}
                            for binding in bindings.values()}}
        (directory / "agent_snapshot.json").write_text(json.dumps(value, indent=2,
                                                                sort_keys=True) + "\n")


def read_pids(home: Path) -> dict[str, dict]:
    path = home / "testbed" / "pids.json"
    return json.loads(path.read_text()) if path.exists() else {}


def health_at(port: int) -> dict | None:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1) as response:
            value = json.load(response)
        return value if isinstance(value, dict) and isinstance(value.get("identity"), str) else None
    except (OSError, ValueError, urllib.error.URLError):
        return None


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def command_for(name: str, state: Path, port: int, *, delayed_agent: bool = False,
                delay_seconds: float = 15, profile: str = "legacy",
                model_provider: str = "scripted", model: str = "gpt-6-sol") -> list[str]:
    if profile == "report" and name != "release":
        role = "research" if name.startswith("research_") else "synthesis" if name == "synthesizer" else "quality"
        return [sys.executable, "-B", str(ROOT / "services" / "model_agent.py"),
                "--role", role, "--capability", REPORT_CAPABILITIES[name],
                "--state", str(state), "--port", str(port),
                "--model-provider", model_provider, "--model", model,
                *(["--test-controls"] if name == "synthesizer" else [])]
    if name == "counter_beta" and delayed_agent:
        return [sys.executable, "-B", str(ROOT / "services" / "delayed_agent.py"),
                "--state", str(state), "--port", str(port),
                "--delay-seconds", str(delay_seconds)]
    if role_for(name) == "capability":
        script = ROOT / "src" / "harness_server.py"
        extra = ["--role", "capability"]
    elif name == "quality":
        script = ROOT / "services" / "quality_server.py"
        extra = []
    else:
        script = ROOT / "services" / "release_server.py"
        extra = ["--mode", "participating"]
    return [sys.executable, "-B", str(script), "--state", str(state), "--port", str(port), *extra]


def up(home: Path, port_base: int, *, delayed_agent: bool = False,
       delay_seconds: float = 15, profile: str = "report",
       model_provider: str = "scripted", model: str = "gpt-6-sol") -> dict:
    if delayed_agent:
        profile = "legacy"
    if profile not in {"report", "legacy"}:
        raise ValueError("unknown testbed profile")
    names = REPORT_NAMES if profile == "report" else SERVICE_NAMES
    pids = read_pids(home)
    health = {}
    for index, name in enumerate(names):
        port = port_base + index
        prior = pids.get(name)
        observed = health_at(port)
        if observed is not None:
            if (not prior or prior.get("port") != port or not alive(prior["pid"])
                    or prior.get("identity") != observed.get("identity")):
                raise RuntimeError(f"{name}: occupied port does not match recorded service")
        else:
            if prior and alive(prior["pid"]):
                raise RuntimeError(f"{name}: recorded process is alive but unhealthy")
            state = home / "services" / name
            state.mkdir(parents=True, exist_ok=True)
            with (state / "service.log").open("a") as log:
                process = subprocess.Popen(command_for(name, state, port,
                    delayed_agent=delayed_agent, delay_seconds=delay_seconds,
                    profile=profile, model_provider=model_provider, model=model), stdout=log, stderr=log,
                                           start_new_session=True)
                _CHILDREN[process.pid] = process
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                observed = health_at(port)
                if observed is not None:
                    break
                if process.poll() is not None:
                    raise RuntimeError(f"{name}: exited during startup; inspect {state / 'service.log'}")
                time.sleep(0.2)
            if observed is None:
                raise RuntimeError(f"{name}: startup timed out; inspect {state / 'service.log'}")
            if prior and prior.get("identity") != observed.get("identity"):
                raise RuntimeError(f"{name}: durable identity changed")
            pids[name] = {"pid": process.pid, "port": port, "identity": observed["identity"]}
            # Record each start so a later startup failure leaves a usable down command.
            directory = home / "testbed"
            directory.mkdir(parents=True, exist_ok=True)
            (directory / "pids.json").write_text(json.dumps(pids, indent=2, sort_keys=True) + "\n")
        if not (name == "counter_beta" and delayed_agent) and observed.get("role", observed.get("mode")) != (
            "participating" if name == "release" else
            report_role(name) if profile == "report" else role_for(name)
        ):
            raise RuntimeError(f"{name}: health role mismatch")
        health[name] = observed
    bindings = report_bindings(health, port_base) if profile == "report" else binding_records(health, port_base)
    contracts = report_contracts(bindings) if profile == "report" else contract_records()
    if delayed_agent:
        for value in contracts.values():
            value["reconcile"] = "fixture-lookup"
        delayed = bindings["counter_beta"]
        delayed_pin = pin(delayed["url"], delayed["identity"])
        contracts["counter_beta"].update(delayed_pin)
        contracts["counter_beta"]["operations"] = {
            "idempotent_action_id": delayed_pin["reconcile"] == "a2a-idempotent-resend",
            "task_lookup": "tasks/get"}
        contracts["counter_beta"]["input"]["blocking"] = False
        contracts["counter_beta"]["attested"] = True
    write_metadata(home, bindings, pids, contracts, snapshot=profile == "report" or delayed_agent,
                   quality_policy=REPORT_QUALITY_POLICY if profile == "report" else None)
    return {"bindings": bindings, "health": health, "pids": pids}


def down(home: Path) -> dict:
    pids = read_pids(home)
    result = {}
    for name in pids:
        record = pids.get(name)
        if not record:
            result[name] = "unrecorded"
            continue
        pid = record["pid"]
        observed = health_at(record["port"])
        if observed is not None and observed.get("identity") != record["identity"]:
            result[name] = "identity-mismatch"
            continue
        if not alive(pid):
            result[name] = "already-stopped"
            continue
        os.kill(pid, signal.SIGTERM)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and health_at(record["port"]) is not None:
            time.sleep(0.2)
        child = _CHILDREN.pop(pid, None)
        if child is not None:
            child.wait(timeout=5)
        result[name] = "stopped" if health_at(record["port"]) is None else "timeout"
    return result


def status(home: Path, port_base: int, *, profile: str = "report") -> dict:
    pids = read_pids(home)
    services = {}
    for index, name in enumerate(REPORT_NAMES if profile == "report" else SERVICE_NAMES):
        port = port_base + index
        observed = health_at(port)
        record = pids.get(name)
        services[name] = {
            "port": port, "pid": record.get("pid") if record else None,
            "healthy": bool(observed and record and record.get("port") == port
                            and record.get("identity") == observed.get("identity")),
            "health": observed,
        }
    return {"services": services}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("up", "down", "status"))
    parser.add_argument("--home", type=Path, required=True)
    parser.add_argument("--port-base", type=int, default=45200)
    parser.add_argument("--profile", choices=("report", "legacy"), default="report")
    parser.add_argument("--model-provider", choices=("codex-subscription", "synthetic-loopback", "scripted"),
                        default="scripted")
    parser.add_argument("--model", default="gpt-6-sol")
    parser.add_argument("--delayed-agent", action="store_true")
    parser.add_argument("--delay-seconds", type=float, default=15)
    args = parser.parse_args()
    if args.command == "up":
        value = up(args.home, args.port_base, delayed_agent=args.delayed_agent,
                   delay_seconds=args.delay_seconds, profile=args.profile,
                   model_provider=args.model_provider, model=args.model)
    elif args.command == "down":
        value = down(args.home)
    else:
        value = status(args.home, args.port_base, profile=args.profile)
    print(json.dumps(value, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
