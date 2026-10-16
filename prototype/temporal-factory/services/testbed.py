"""Start pinned, independent fixture services for local factory trials."""
from __future__ import annotations

import argparse
import hashlib
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
from agent_binding import UnavailableBinding, card_observation, pin
from report_contract import REPORT_ACCEPTANCE_CRITERIA, digest as report_digest
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
REPORT_QUALITY_POLICY = {**QUALITY_POLICY, "rubric": REPORT_ACCEPTANCE_CRITERIA["kind"],
                         "rubric_digest": report_digest(REPORT_ACCEPTANCE_CRITERIA),
                         "acceptance_criteria": REPORT_ACCEPTANCE_CRITERIA}
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
    return {**contract_records()["release"], **pin(binding["url"], binding["identity"])}


def report_contracts(bindings: dict[str, dict]) -> dict[str, dict]:
    """Every agent, release included, is pinned by its Agent Card alone."""
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


def health_at(port: int) -> dict | None:
    """Observe one agent through its Agent Card; the card is its identity."""
    try:
        observed = card_observation(f"http://127.0.0.1:{port}")
    except (OSError, ValueError, UnavailableBinding, urllib.error.URLError):
        return None
    return {"identity": observed["identity"], "skills": observed["skills"],
            "card_sha256": observed["card_sha256"], "reconcile": observed["reconcile"]}


def observe(name: str, port: int) -> dict | None:
    """Every service, release included, is observed the same way."""
    return health_at(port)


def require_distinct_identities(health: dict[str, dict]) -> None:
    """Two services whose cards derive the same identity would merge in the snapshot."""
    seen: dict[str, str] = {}
    for name, observed in health.items():
        other = seen.setdefault(observed["identity"], name)
        if other != name:
            raise RuntimeError(f"{other} and {name}: identical Agent Cards derive the same "
                               "identity; refusing to merge them")


def alive(pid: int) -> bool:
    if type(pid) is not int or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


# --------------------------------------------------------------------------- process ownership
# A recorded pid is acted on only after verifying the process is this home's:
# it is alive, its command line names this home's service state directory and
# its recorded port, and no other process holds that port. Card identity is
# never part of the check, so an identity-scheme change cannot strand a
# process, and a foreign process (or a reused pid) is never signalled.

def command_line(pid: int) -> str:
    try:
        result = subprocess.run(["ps", "-ww", "-o", "command=", "-p", str(pid)],
                                capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.TimeoutExpired):
        return ""
    return result.stdout.strip()


def process_cwd(pid: int) -> str:
    try:
        output = subprocess.run(["lsof", "-a", "-p", str(pid), "-d", "cwd", "-Fn"],
                                capture_output=True, text=True, timeout=10).stdout
    except (OSError, subprocess.TimeoutExpired):
        return ""
    return next((line[1:] for line in output.splitlines() if line.startswith("n")), "")


def port_holders(port: int) -> set[int]:
    """Every pid listening on a local TCP port."""
    try:
        output = subprocess.run(["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-t"],
                                capture_output=True, text=True, timeout=10).stdout
    except (OSError, subprocess.TimeoutExpired):
        return set()
    return {int(line) for line in output.split() if line.strip().isdigit()}


def _home_forms(path: Path) -> set[str]:
    forms = {str(path)}
    try:
        forms.add(str(path.resolve()))
    except OSError:
        pass
    return forms


def in_home(pid: int, home: Path) -> bool:
    """The process names this home in its command line or runs inside it."""
    text, cwd = command_line(pid), process_cwd(pid)
    return any(form + "/" in text + "/" or cwd == form or cwd.startswith(form + "/")
               for form in _home_forms(home))


def owned_service(home: Path, name: str, record: dict | None) -> tuple[bool, str]:
    """(True, "owned") only for this home's recorded service process."""
    if not isinstance(record, dict):
        return False, "unrecorded"
    pid, port = record.get("pid"), record.get("port")
    if not alive(pid):
        return False, "not-running"
    text = command_line(pid)
    states = _home_forms(home / "services" / name)
    if not any(f"--state {state}" in text for state in states) or f"--port {port}" not in text:
        return False, "foreign-process"
    holders = port_holders(port) if type(port) is int else set()
    if holders and pid not in holders:
        return False, "port-held-by-another-process"
    return True, "owned"


def terminate(pid: int, *, grace: float = 15) -> str:
    """SIGTERM, wait for the process to exit, then SIGKILL; reaps our own children."""
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return "already-stopped"
    child = _CHILDREN.pop(pid, None)
    deadline = time.monotonic() + grace
    while time.monotonic() < deadline:
        if child is not None and child.poll() is not None:
            return "stopped"
        if child is None and not alive(pid):
            return "stopped"
        time.sleep(0.1)
    os.kill(pid, signal.SIGKILL)
    if child is not None:
        child.wait(timeout=5)
    else:
        end = time.monotonic() + 5
        while time.monotonic() < end and alive(pid):
            time.sleep(0.1)
    return "killed-after-sigterm-timeout"


SOURCE_DIRS = (ROOT / "services", ROOT / "src")


def service_build(command: list[str]) -> str:
    """What a service process runs: its command line and the code it loads.

    A different interpreter, arguments or any service/interpreter source file
    is an upgrade; the running process is replaced on the next ``up``.
    """
    value = hashlib.sha256(json.dumps(command, separators=(",", ":")).encode())
    for directory in SOURCE_DIRS:
        for path in sorted(directory.glob("*.py")):
            value.update(path.relative_to(ROOT).as_posix().encode() + b"\0")
            value.update(path.read_bytes() + b"\0")
    return value.hexdigest()


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
        extra = ["--role", "capability", "--name", name]
    elif name == "quality":
        script = ROOT / "services" / "quality_server.py"
        extra = []
    else:
        script = ROOT / "services" / "release_server.py"
        extra = ["--mode", "participating"]
    return [sys.executable, "-B", str(script), "--state", str(state), "--port", str(port), *extra]


def _names(profile: str, delayed_agent: bool) -> tuple[str, ...]:
    if delayed_agent:
        profile = "legacy"
    if profile not in {"report", "legacy"}:
        raise ValueError("unknown testbed profile")
    return REPORT_NAMES if profile == "report" else SERVICE_NAMES


def plan(home: Path, port_base: int, *, delayed_agent: bool = False,
         delay_seconds: float = 15, profile: str = "report",
         model_provider: str = "scripted", model: str = DEFAULT_MODEL_ID,
         test_controls: bool = False) -> dict[str, dict]:
    """What ``up`` would do to each service, without touching any process.

    ``reuse``: this home's process runs the current command and code.
    ``replace``: this home's process is from an older build, command or
    record schema (an upgrade) and will be stopped and restarted.
    ``start``: nothing of this home's is running. ``blocked``: the port is
    held by a process outside this home, which is never touched.
    """
    if delayed_agent:
        profile = "legacy"
    pids = read_pids(home)
    result = {}
    for index, name in enumerate(_names(profile, delayed_agent)):
        port = port_base + index
        command = command_for(name, home / "services" / name, port,
                              delayed_agent=delayed_agent, delay_seconds=delay_seconds,
                              profile=profile, model_provider=model_provider, model=model,
                              test_controls=test_controls)
        prior = pids.get(name)
        build = service_build(command)
        # An upgrade: this home ran the service from another build, command or
        # record schema, whether or not it is running now.
        upgrade = isinstance(prior, dict) and prior.get("build") != build
        owned, reason = owned_service(home, name, prior)
        holders = port_holders(port)
        if owned and prior.get("port") == port:
            result[name] = {"action": "replace" if upgrade else "reuse",
                            "reason": ("current" if not upgrade else "unrecorded-build"
                                       if "build" not in prior else "build-changed"),
                            "pid": prior["pid"], "upgrade": upgrade}
        elif holders:
            result[name] = {"action": "blocked", "reason": "port held outside this home",
                            "holders": sorted(holders), "upgrade": upgrade}
        else:
            result[name] = {"action": "start", "reason": reason, "upgrade": upgrade}
    return result


def up(home: Path, port_base: int, *, delayed_agent: bool = False,
       delay_seconds: float = 15, profile: str = "report",
       model_provider: str = "scripted", model: str = DEFAULT_MODEL_ID,
       test_controls: bool = False) -> dict:
    """Start or upgrade every service; identities follow the served cards.

    This home's own processes are reused when current and replaced when their
    command or code changed (or the card no longer answers); a changed card
    identity is reported in ``identity_changed`` for the caller to re-pin. A
    port held by a process outside this home is refused and never touched.
    """
    names = _names(profile, delayed_agent)
    if delayed_agent:
        profile = "legacy"
    pids = read_pids(home)
    health = {}
    restarted, identity_changed = [], []
    for index, name in enumerate(names):
        port = port_base + index
        state = home / "services" / name
        command = command_for(name, state, port, delayed_agent=delayed_agent,
                              delay_seconds=delay_seconds, profile=profile,
                              model_provider=model_provider, model=model,
                              test_controls=test_controls)
        build = service_build(command)
        prior = pids.get(name)
        owned, _reason = owned_service(home, name, prior)
        observed = observe(name, port) if owned else None
        if (owned and prior.get("port") == port and prior.get("build") == build
                and observed is not None and observed.get("identity") == prior.get("identity")):
            health[name] = observed
        else:
            if owned:
                # This home's process from an older build, command or record
                # schema (or no longer answering): an upgrade replaces it.
                terminate(prior["pid"])
                restarted.append(name)
            holders = port_holders(port)
            if holders:
                raise RuntimeError(f"{name}: port {port} is held by a process outside this "
                                   f"home (pids {sorted(holders)}); refusing to touch it")
            state.mkdir(parents=True, exist_ok=True)
            with (state / "service.log").open("a") as log:
                process = subprocess.Popen(command, stdout=log, stderr=log,
                                           start_new_session=True)
                _CHILDREN[process.pid] = process
            # Record each start so a later startup failure leaves a usable down command.
            pids[name] = {"pid": process.pid, "port": port, "build": build,
                          "identity": (prior or {}).get("identity")}
            directory = home / "testbed"
            directory.mkdir(parents=True, exist_ok=True)
            (directory / "pids.json").write_text(json.dumps(pids, indent=2, sort_keys=True) + "\n")
            deadline = time.monotonic() + 30
            observed = None
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
                identity_changed.append(name)
            pids[name] = {"pid": process.pid, "port": port, "build": build,
                          "identity": observed["identity"]}
            (directory / "pids.json").write_text(json.dumps(pids, indent=2, sort_keys=True) + "\n")
        if name == "release" and observed.get("reconcile") != "a2a-idempotent-resend":
            raise RuntimeError("release: Agent Card does not offer messageId idempotency")
        if (profile == "report" and name != "release"
                and REPORT_CAPABILITIES[name] not in observed.get("skills", [])):
            raise RuntimeError(f"{name}: Agent Card skill mismatch")
        health[name] = observed
    require_distinct_identities(health)
    bindings = report_bindings(health, port_base) if profile == "report" else binding_records(health, port_base)
    contracts = report_contracts(bindings) if profile == "report" else contract_records()
    if profile == "legacy":
        # Every A2A agent is pinned by its Agent Card; reconciliation follows
        # the card's message-id-idempotent skill tag.
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
    return {"bindings": bindings, "health": health, "pids": pids,
            "restarted": restarted, "identity_changed": identity_changed}


def down(home: Path) -> dict:
    """Stop every service this home's pid records name, verified as this home's.

    Each process is checked by pid, command line (this home's state directory
    and its recorded port) and port holder, never by card identity; a process
    that fails the check is reported and never signalled. Each stopped
    service's port is then confirmed free.
    """
    pids = read_pids(home)
    result = {}
    for name, record in pids.items():
        owned, reason = owned_service(home, name, record)
        if not owned:
            result[name] = {"unrecorded": "unrecorded", "not-running": "already-stopped"}.get(
                reason, f"not-touched:{reason}")
            continue
        outcome = terminate(record["pid"])
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and port_holders(record["port"]):
            time.sleep(0.1)
        result[name] = outcome if not port_holders(record["port"]) else f"{outcome}:port-still-held"
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
    parser.add_argument("command", choices=("up", "down", "status", "plan"))
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
    elif args.command == "plan":
        value = plan(args.home, args.port_base, delayed_agent=args.delayed_agent,
                     delay_seconds=args.delay_seconds, profile=args.profile,
                     model_provider=args.model_provider, model=args.model,
                     test_controls=args.test_controls)
    else:
        value = status(args.home, args.port_base, profile=args.profile)
    print(json.dumps(value, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
