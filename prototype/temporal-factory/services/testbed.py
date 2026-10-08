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
from agent_binding import UnavailableBinding, card_observation, card_pin, pin
from agent_roles import RUBRIC_DIGEST
from model_broker import DEFAULT_MODEL_ID
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


RELEASE_RECEIPT = ["receipt_id", "sha256", "byte_length", "media_type", "accepted_at",
                   "outcome"]


def report_bindings(health: dict[str, dict], port_base: int) -> dict[str, dict]:
    # The release receiver is an A2A agent bound with strict artifacts output:
    # its receipt is its result artifact. Release stays a side-effect node
    # through its control-only outgoing edge in the definition.
    return {name: {"role": report_role(name), "url": f"http://127.0.0.1:{port_base + index}",
                   "identity": health[name]["identity"], "approved": True,
                   **({"output": "artifacts"} if name == "release" else {})}
            for index, name in enumerate(REPORT_NAMES)}


def release_contract(binding: dict) -> dict:
    """The release receiver's pinned contract: its public Agent Card, nothing else."""
    observed = card_pin(binding["url"])
    if observed["identity"] != binding["identity"]:
        raise ValueError("release receiver Agent Card differs from its binding identity")
    tags = {tag for skill in observed["skills"] if skill["id"] == CAPABILITIES["release"]
            for tag in skill["tags"]}
    return {**contract_records()["release"], "card_sha256": observed["card_sha256"],
            "reconcile": ("a2a-idempotent-resend" if "message-id-idempotent" in tags
                          else "opaque")}


def report_contracts(bindings: dict[str, dict]) -> dict[str, dict]:
    contracts = {}
    for name in REPORT_NAMES:
        if name == "release":
            contracts[name] = release_contract(bindings[name])
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
        if role in {"capability", "quality"}:
            # Agent services: a plain A2A Message carrying a text brief. The
            # factory correlates by its own journal, never by agent echo.
            input_contract = {"transport": "a2a-SendMessage", "message": "text-brief"}
            output_contract = ({"artifact": ["revision", "sha256", "author", "content"]}
                               if role == "capability" else
                               {"verdict": ["accepted", "revision", "sha256", "reviewer",
                                            "reason"]})
            operations = {"idempotent_message_id": True, "task_lookup": "GetTask"}
        else:
            # An ordinary A2A agent: one Part carrying the accepted document
            # and its mediaType; the receipt is the Task's (strict) data-part
            # result artifact; messageId is the idempotency key.
            contracts[name] = {
                "name": name, "role": role, "capability": CAPABILITIES[name],
                "a2a_protocol": "1.0",
                "input": {"transport": "a2a-SendMessage", "parts": 1,
                          "media_type": "application/json"},
                "output": {"mode": "artifacts", "receipt": RELEASE_RECEIPT},
                "operations": {"idempotency": "messageId", "task_lookup": "GetTask"},
                "attested": False,
            }
            continue
        contracts[name] = {
            "name": name, "role": role, "capability": CAPABILITIES[name],
            "a2a_protocol": "1.0", "input": input_contract, "output": output_contract,
            "operations": operations, "attested": False,
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


def release_card_at(port: int) -> dict | None:
    """The release receiver serves only A2A: observe it through its Agent Card."""
    try:
        observed = card_pin(f"http://127.0.0.1:{port}")
    except (UnavailableBinding, ValueError, OSError):
        return None
    tags = {tag for skill in observed["skills"] for tag in skill["tags"]}
    return {"identity": observed["identity"], "card_sha256": observed["card_sha256"],
            "role": "release", "a2a_protocol": "1.0",
            "mode": "participating" if "message-id-idempotent" in tags else "opaque"}


def observe(name: str, port: int) -> dict | None:
    return release_card_at(port) if name == "release" else health_at(port)


def health_at(port: int) -> dict | None:
    try:
        observed = card_observation(f"http://127.0.0.1:{port}")
    except (OSError, ValueError, UnavailableBinding, urllib.error.URLError):
        return None
    if not isinstance(observed.get("identity"), str) or not observed["identity"]:
        return None
    return {"identity": observed["identity"], "skills": observed["skills"],
            "card_sha256": observed["card_sha256"]}


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def command_for(name: str, state: Path, port: int, *, delayed_agent: bool = False,
                delay_seconds: float = 15, profile: str = "legacy",
                model_provider: str = "scripted", model: str = DEFAULT_MODEL_ID,
                test_controls: bool = False) -> list[str]:
    if profile == "report" and name != "release":
        role = "research" if name.startswith("research_") else "synthesis" if name == "synthesizer" else "quality"
        return [sys.executable, "-B", str(ROOT / "services" / "model_agent.py"),
                "--role", role, "--capability", REPORT_CAPABILITIES[name],
                "--state", str(state), "--port", str(port),
                "--model-provider", model_provider, "--model", model,
                *(["--test-controls"] if test_controls and name == "synthesizer" else [])]
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
       model_provider: str = "scripted", model: str = DEFAULT_MODEL_ID,
       test_controls: bool = False) -> dict:
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
        observed = observe(name, port)
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
                    profile=profile, model_provider=model_provider, model=model,
                    test_controls=test_controls), stdout=log, stderr=log,
                                           start_new_session=True)
                _CHILDREN[process.pid] = process
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                observed = observe(name, port)
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
        if name == "release" and observed.get("mode") != "participating":
            raise RuntimeError("release: Agent Card does not offer messageId idempotency")
            raise RuntimeError(f"{name}: health role mismatch")
        if (profile == "report" and name != "release"
                and REPORT_CAPABILITIES[name] not in observed.get("skills", [])):
            raise RuntimeError(f"{name}: Agent Card skill mismatch")
        health[name] = observed
    bindings = report_bindings(health, port_base) if profile == "report" else binding_records(health, port_base)
    contracts = report_contracts(bindings) if profile == "report" else contract_records()
    if profile == "legacy":
        # Every A2A agent is pinned by its Agent Card; reconciliation follows
        # the card's declared messageId resend rule.
        for name, value in contracts.items():
            if value["role"] == "release":
                continue
            agent_pin = pin(bindings[name]["url"], bindings[name]["identity"])
            value.update(agent_pin)
            value["operations"]["idempotent_message_id"] = (
                agent_pin["reconcile"] == "a2a-idempotent-resend")
            value["attested"] = True
        contracts["release"] = release_contract(bindings["release"])
        if delayed_agent:
            contracts["counter_beta"]["input"]["returnImmediately"] = True
    write_metadata(home, bindings, pids, contracts, snapshot=True,
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
        observed = observe(name, record["port"])
        if observed is not None and observed.get("identity") != record["identity"]:
            result[name] = "identity-mismatch"
            continue
        if not alive(pid):
            result[name] = "already-stopped"
            continue
        os.kill(pid, signal.SIGTERM)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and observe(name, record["port"]) is not None:
            time.sleep(0.2)
        child = _CHILDREN.pop(pid, None)
        if child is not None:
            child.wait(timeout=5)
        result[name] = "stopped" if observe(name, record["port"]) is None else "timeout"
    return result


def status(home: Path, port_base: int, *, profile: str = "report") -> dict:
    pids = read_pids(home)
    services = {}
    for index, name in enumerate(REPORT_NAMES if profile == "report" else SERVICE_NAMES):
        port = port_base + index
        observed = observe(name, port)
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
    parser.add_argument("--model", default=DEFAULT_MODEL_ID)
    parser.add_argument("--delayed-agent", action="store_true")
    parser.add_argument("--delay-seconds", type=float, default=15)
    parser.add_argument("--test-controls", action="store_true",
                        help="qualification only: declare the synthesizer's test-only stimulus extension")
    args = parser.parse_args()
    if args.command == "up":
        value = up(args.home, args.port_base, delayed_agent=args.delayed_agent,
                   delay_seconds=args.delay_seconds, profile=args.profile,
                   model_provider=args.model_provider, model=args.model,
                   test_controls=args.test_controls)
    elif args.command == "down":
        value = down(args.home)
    else:
        value = status(args.home, args.port_base, profile=args.profile)
    print(json.dumps(value, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
