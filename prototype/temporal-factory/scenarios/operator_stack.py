#!/usr/bin/env python3
"""Repeatable launcher for the operator's local factory stack (A2A v1).

``up`` provisions a durable home on first use and reuses it afterwards,
republishing the shipped report definition only when the template or the
interpreter build changed. Every process starts detached with its log in the
home. ``down`` stops them gracefully. ``status`` reports processes, ports, the
v1 Agent Cards, runner/Temporal/PostgreSQL health and the broker's signed-in
flags only.

This launcher never submits work and never calls a model. Model agents start
with the subscription provider; their startup only recovers ``working`` rows
from their own ledger, which is empty on a fresh home.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SERVICES = ROOT / "services"
sys.path.insert(0, str(SRC))
from runner import port_map  # noqa: E402

DEFAULT_HOME = Path.home() / ".exomachina" / "operator-stack"
DEFAULT_PYTHON = Path("/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/"
                      "tools/spikes/2026-10-07/a2a-v1/.venv/bin/python")
DEFAULT_TEMPORAL = Path.home() / ".exomachina" / "temporal" / "1.32.0"
DEFAULT_PG_BIN = Path("/opt/homebrew/opt/postgresql@16/bin")
DEFAULT_HARNESS_PORT = 47053
DEFAULT_AGENT_PORT_BASE = 47100
DEFAULT_RUNNER_PORT_BASE = 47020
DEFAULT_RUNNER_MEMBER_BASE = 32620

INSTANCE = "report-factory"
CAPABILITY = "verified-research@1"
MODEL_PROVIDER = "codex-subscription"
MODEL = "gpt-6-luna"
LABEL = "report"
TEMPLATE = ROOT / "definitions" / "report-template.json"
PACKET = ROOT / "packets" / "exo-qualification-2026-09-23" / "packet.json"
# Same order as services/testbed.py REPORT_NAMES: port = agent base + index.
SERVICE_NAMES = ("research_findings", "research_risks", "synthesizer", "quality", "release")
MODEL_AGENTS = SERVICE_NAMES[:4]
# Instance options the operator stack runs with, beyond `admin provision`.
INSTANCE_OPTIONS = {
    "loopback_qa_session": True,
    "basic_single_active_job": True,
    "operations_max_list_limit": 128,
    "director_model": {"provider": MODEL_PROVIDER, "model": MODEL},
}
# The macOS temp cleaners empty these trees; durable state must not live there.
TEMP_ROOTS = (Path("/tmp"), Path("/private/tmp"), Path("/var/folders"),
              Path("/private/var/folders"))
STRIPPED_ENV = ("EXO_MODEL_HOME", "EXO_CODEX_BASE_URL", "PYTHONPATH", "EXO_HOME")


def validate_home(home: Path) -> Path:
    """Return the resolved home, refusing temporary trees. Touches nothing."""
    if not str(home):
        raise ValueError("home is required")
    resolved = Path(os.path.expanduser(str(home))).resolve()
    roots = list(TEMP_ROOTS)
    tmpdir = os.environ.get("TMPDIR")
    if tmpdir:
        roots.append(Path(tmpdir).resolve())
    for root in roots:
        if resolved == root or root in resolved.parents:
            raise ValueError(f"home must be durable, not under {root}: {resolved}")
    if resolved == Path(resolved.anchor):
        raise ValueError("home must not be the filesystem root")
    return resolved


def port_plan(*, harness_port: int = DEFAULT_HARNESS_PORT,
              agent_port_base: int = DEFAULT_AGENT_PORT_BASE,
              runner_port_base: int = DEFAULT_RUNNER_PORT_BASE,
              runner_member_base: int = DEFAULT_RUNNER_MEMBER_BASE) -> dict[str, int]:
    """Every port the stack listens on, by component; validates ranges and overlap."""
    plan = {"harness": harness_port}
    for index, name in enumerate(SERVICE_NAMES):
        plan[name] = agent_port_base + index
    plan.update({f"runner.{name}": port
                 for name, port in port_map(runner_port_base, runner_member_base).items()})
    for name, port in plan.items():
        if type(port) is not int or not 1024 <= port <= 65535:
            raise ValueError(f"{name} port {port!r} is outside 1024..65535")
    seen: dict[int, str] = {}
    for name, port in plan.items():
        if port in seen:
            raise ValueError(f"port {port} is planned for both {seen[port]} and {name}")
        seen[port] = name
    return plan


def port_conflicts(plan: dict[str, int], listening: dict[int, int],
                   own_pids: set[int]) -> list[dict]:
    """Planned ports held by a process that is not part of this stack."""
    return [{"component": name, "port": port, "pid": listening[port]}
            for name, port in plan.items()
            if port in listening and listening[port] not in own_pids]


def listening_ports() -> dict[int, int]:
    """Map listening TCP port -> pid via lsof (first pid wins)."""
    try:
        output = subprocess.run(["lsof", "-nP", "-iTCP", "-sTCP:LISTEN", "-Fpn"],
                                capture_output=True, text=True, timeout=20).stdout
    except (OSError, subprocess.TimeoutExpired):
        return {}
    ports: dict[int, int] = {}
    pid = None
    for line in output.splitlines():
        if line.startswith("p"):
            pid = int(line[1:])
        elif line.startswith("n") and pid is not None:
            try:
                ports.setdefault(int(line.rsplit(":", 1)[1]), pid)
            except (IndexError, ValueError):
                continue
    return ports


def stack_env(home: Path, plan: dict[str, int], *, temporal: Path = DEFAULT_TEMPORAL,
              pg_bin: Path = DEFAULT_PG_BIN, base: dict | None = None) -> dict[str, str]:
    """The environment every stack process inherits: durable paths only."""
    env = {key: value for key, value in (os.environ if base is None else base).items()
           if key not in STRIPPED_ENV}
    env.update({
        "TMPDIR": str(home / "tmp"),
        "PYTHONDONTWRITEBYTECODE": "1",
        "EXO_TEMPORAL_RUNTIME": str(temporal / "server"),
        "EXO_TEMPORAL_CLI": str(temporal / "cli" / "temporal"),
        "EXO_PG_BIN": str(pg_bin),
        "EXO_RUNNER_PORT_BASE": str(plan["runner.frontend"] - 2),
        "EXO_RUNNER_MEMBER_BASE": str(plan["runner.postgres"]),
    })
    return env


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("up", "down", "status"))
    parser.add_argument("--home", type=Path, default=DEFAULT_HOME)
    parser.add_argument("--python", type=Path, default=DEFAULT_PYTHON,
                        help="interpreter for every stack process (A2A v1 venv)")
    parser.add_argument("--temporal", type=Path, default=DEFAULT_TEMPORAL,
                        help="durable Temporal runtime holding server/ and cli/temporal")
    parser.add_argument("--pg-bin", type=Path, default=DEFAULT_PG_BIN)
    parser.add_argument("--harness-port", type=int, default=DEFAULT_HARNESS_PORT)
    parser.add_argument("--agent-port-base", type=int, default=DEFAULT_AGENT_PORT_BASE,
                        help="four model agents then the release receiver")
    parser.add_argument("--runner-port-base", type=int, default=DEFAULT_RUNNER_PORT_BASE)
    parser.add_argument("--runner-member-base", type=int, default=DEFAULT_RUNNER_MEMBER_BASE,
                        help="PostgreSQL and Temporal membership ports; base+4 must be <= 32767")
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.home = validate_home(args.home)
        args.plan = port_plan(harness_port=args.harness_port,
                              agent_port_base=args.agent_port_base,
                              runner_port_base=args.runner_port_base,
                              runner_member_base=args.runner_member_base)
    except ValueError as error:
        parser.error(str(error))
    if not args.python.is_absolute():
        parser.error("--python must be an absolute path")
    return args


# --------------------------------------------------------------------------- helpers

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _alive(pid: int | None) -> bool:
    if not isinstance(pid, int) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _command_line(pid: int) -> str:
    result = subprocess.run(["ps", "-ww", "-o", "command=", "-p", str(pid)],
                            capture_output=True, text=True)
    return result.stdout.strip()


def _http_json(url: str, *, timeout: float = 3) -> tuple[int | None, object]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return response.status, json.loads(response.read(1024 * 1024))
    except urllib.error.HTTPError as error:
        return error.code, None
    except (OSError, ValueError):
        return None, None


def _read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text())
        return value if isinstance(value, dict) else {}
    except (FileNotFoundError, ValueError):
        return {}


def _write_json(path: Path, value: dict) -> None:
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)


def _cli(args: argparse.Namespace, env: dict, *command: str, timeout: float = 300) -> dict:
    log = args.home / "logs" / "launcher.log"
    with log.open("a") as stream:
        stream.write(f"{_now()} run {' '.join(command[:3])}\n")
        stream.flush()
        result = subprocess.run([str(args.python), "-B", *command], cwd=ROOT, env=env,
                                stdout=subprocess.PIPE, stderr=stream, text=True,
                                timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"{Path(command[0]).name} {command[1]} exited {result.returncode}; "
                           f"see {log}")
    return json.loads(result.stdout) if result.stdout.strip() else {}


def _instance_dir(home: Path) -> Path:
    return home / "instances" / INSTANCE


def _harness_record(home: Path) -> dict:
    return _read_json(home / "run" / "harness.json")


def _harness_pid(home: Path) -> int | None:
    pid = _harness_record(home).get("pid")
    if _alive(pid) and "harness.py serve" in _command_line(pid) and \
            str(_instance_dir(home)) in _command_line(pid):
        return pid
    return None


def _own_pids(home: Path) -> set[int]:
    pids = {_harness_pid(home)}
    for record in _read_json(home / "testbed" / "pids.json").values():
        if isinstance(record, dict):
            pids.add(record.get("pid"))
    ready = _read_json(home / "runner" / "runner-ready.json")
    pids.add(ready.get("pid"))
    pids.update((ready.get("pids") or {}).values())
    pids.update(build.get("pid") for build in (ready.get("builds") or {}).values()
                if isinstance(build, dict))
    return {pid for pid in pids if _alive(pid)}


def _check_binaries(args: argparse.Namespace) -> None:
    required = [args.temporal / "server" / "temporal-server",
                args.temporal / "server" / "temporal-sql-tool",
                args.temporal / "server" / "config" / "dynamicconfig" / "development-sql.yaml",
                args.temporal / "server" / "temporal-1.32.0" / "schema" / "postgresql" / "v12",
                args.temporal / "cli" / "temporal",
                args.pg_bin / "initdb", args.pg_bin / "pg_ctl", args.pg_bin / "pg_isready"]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise RuntimeError(f"missing runtime binaries/config: {missing}")
    for path in (args.temporal, args.pg_bin):
        validate_home(path)  # binaries must be durable too


def _desired_build_id() -> str:
    from binding import build_id_for, source_digest
    return build_id_for(source_digest(SRC))


def _active_publication(home: Path) -> dict | None:
    from binding import PublicationStore
    try:
        return PublicationStore(_instance_dir(home) / "catalog").active()
    except (FileNotFoundError, KeyError, ValueError):
        return None


def _wait(predicate, seconds: float, interval: float = .25) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return bool(predicate())


def _terminate(pid: int, *, grace: float = 30) -> str:
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return "already-stopped"
    if _wait(lambda: not _alive(pid), grace):
        return "stopped"
    os.kill(pid, signal.SIGKILL)
    _wait(lambda: not _alive(pid), 5)
    return "killed-after-sigterm-timeout"


# --------------------------------------------------------------------------- commands

def up(args: argparse.Namespace) -> dict:
    home, plan = args.home, args.plan
    _check_binaries(args)
    home.mkdir(mode=0o700, parents=True, exist_ok=True)
    for name in ("logs", "run", "tmp"):
        (home / name).mkdir(mode=0o700, exist_ok=True)
    env = stack_env(home, plan, temporal=args.temporal, pg_bin=args.pg_bin)
    with (home / "operator-stack.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        state_path = home / "operator-stack.json"
        state = _read_json(state_path)
        if state.get("ports") and state["ports"] != plan:
            raise RuntimeError("port plan differs from this home's recorded plan; "
                               "use the recorded ports or a new home")
        conflicts = port_conflicts(plan, listening_ports(), _own_pids(home))
        if conflicts:
            raise RuntimeError(f"planned ports are held by other processes: {conflicts}")
        actions = []

        # 1. Model agents and release receiver (idempotent; durable identities).
        _cli(args, env, str(SERVICES / "testbed.py"), "up", "--home", str(home),
             "--port-base", str(plan[SERVICE_NAMES[0]]), "--profile", "report",
             "--model-provider", MODEL_PROVIDER, "--model", MODEL)
        actions.append("testbed-up")

        # 2. Factory instance: provision once, then verify the pins still hold.
        instance = _instance_dir(home)
        config_path = instance / "instance.json"
        if not config_path.exists():
            _cli(args, env, str(SRC / "admin.py"), "provision", "--instance-dir", str(instance),
                 "--name", INSTANCE, "--port", str(plan["harness"]), "--home", str(home),
                 "--testbed", str(home / "testbed"), "--wait-seconds", "900",
                 "--evidence-packet", str(PACKET))
            config = json.loads(config_path.read_text())
            config.update(INSTANCE_OPTIONS)
            _write_json(config_path, config)
            actions.append("provisioned")
        else:
            config = json.loads(config_path.read_text())
            drift = {key: config.get(key) for key, value in INSTANCE_OPTIONS.items()
                     if config.get(key) != value}
            if config.get("port") != plan["harness"] or drift:
                raise RuntimeError(f"instance.json differs from the operator stack: {drift}")
            for name in ("approved_bindings.json", "contracts.json", "quality_policy.json"):
                if _read_json(instance / "catalog" / name) != _read_json(home / "testbed" / name):
                    raise RuntimeError(f"agent pins drifted ({name}); the served Agent Cards no "
                                       "longer match the publication. Re-provision deliberately "
                                       "in a new home.")

        # 3. Publication: only when the shipped definition or the build changed.
        desired = {"template_sha256": _sha256(TEMPLATE), "build_id": _desired_build_id()}
        active = _active_publication(home)
        if (active is None or active.get("build_id") != desired["build_id"]
                or state.get("template_sha256") != desired["template_sha256"]):
            _cli(args, env, str(SRC / "admin.py"), "publish-template", "--instance-dir",
                 str(instance), "--template", str(TEMPLATE), "--label", LABEL)
            active = _active_publication(home)
            actions.append("published")
        if active is None or active.get("build_id") != desired["build_id"]:
            raise RuntimeError("active publication is not on the current interpreter build")

        # 4. Runner (PostgreSQL, Temporal, one worker per build), then a live poller.
        _cli(args, env, str(SRC / "runner.py"), "start", "--home", str(home),
             "--reason", "operator-stack-up", timeout=240)
        _worker(home, active["build_id"], timeout=90)
        actions.append("runner-ready")

        # 5. Harness.
        if _harness_pid(home) is None:
            log = (home / "logs" / "harness.log").open("a")
            process = subprocess.Popen([str(args.python), "-B", str(SRC / "harness.py"), "serve",
                                        "--instance-dir", str(instance)],
                                       cwd=ROOT, env=env, stdin=subprocess.DEVNULL,
                                       stdout=log, stderr=log, start_new_session=True)
            log.close()
            _write_json(home / "run" / "harness.json",
                        {"pid": process.pid, "port": plan["harness"], "started_at": _now()})
            health_url = f"http://127.0.0.1:{plan['harness']}/health"
            if not _wait(lambda: _http_json(health_url)[0] == 200 or process.poll() is not None,
                         90, .5) or process.poll() is not None:
                raise RuntimeError(f"harness did not become healthy; see {home / 'logs'}")
            actions.append("harness-started")
        state.update({"schema": 1, "ports": plan, "instance": INSTANCE, "label": LABEL,
                      "python": str(args.python), "temporal": str(args.temporal),
                      "template_sha256": desired["template_sha256"],
                      "build_id": active["build_id"],
                      "manifest_digest": active.get("manifest_digest"),
                      "updated_at": _now()})
        state.setdefault("created_at", state["updated_at"])
        _write_json(state_path, state)
        fcntl.flock(lock, fcntl.LOCK_UN)
    return {"actions": actions, **status(args)}


def _worker(home: Path, build_id: str, *, timeout: float) -> dict:
    from runner import Runner
    return Runner(home).wait_worker(build_id, timeout=timeout)


def down(args: argparse.Namespace) -> dict:
    home = args.home
    if not (home / "operator-stack.json").exists():
        return {"home": str(home), "result": "not-provisioned"}
    env = stack_env(home, args.plan, temporal=args.temporal, pg_bin=args.pg_bin)
    result: dict[str, object] = {}
    with (home / "operator-stack.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        pid = _harness_pid(home)
        result["harness"] = _terminate(pid) if pid else "not-running"
        result["services"] = _cli(args, env, str(SERVICES / "testbed.py"), "down",
                                  "--home", str(home), timeout=120)
        for name, outcome in list(result["services"].items()):
            record = _read_json(home / "testbed" / "pids.json").get(name) or {}
            if outcome == "timeout" and _alive(record.get("pid")):
                result["services"][name] = _terminate(record["pid"], grace=15)
        if (home / "runner-config.json").exists():
            runner = _cli(args, env, str(SRC / "runner.py"), "stop", "--home", str(home),
                          timeout=120)
            result["runner"] = {"running": runner.get("running")}
        fcntl.flock(lock, fcntl.LOCK_UN)
    listening = listening_ports()
    result["ports_still_listening"] = {name: port for name, port in args.plan.items()
                                       if port in listening}
    return result


def _card_summary(port: int) -> dict:
    import a2a_v1
    code, card = _http_json(f"http://127.0.0.1:{port}/.well-known/agent-card.json")
    if code != 200 or not isinstance(card, dict):
        return {"status": code, "v1_only": False}
    interfaces = card.get("supportedInterfaces") or []
    try:
        a2a_v1.card_interface(card)
        v1_only = True
    except a2a_v1.ProtocolError:
        v1_only = False
    return {"status": code, "v1_only": v1_only,
            "supported_interfaces": [{"protocolBinding": i.get("protocolBinding"),
                                      "protocolVersion": i.get("protocolVersion")}
                                     for i in interfaces if isinstance(i, dict)],
            "skills": [skill.get("id") for skill in card.get("skills") or []
                       if isinstance(skill, dict)],
            "identity": _card_identity(card)}


def _card_identity(card: dict) -> str | None:
    import a2a_extensions
    for extension in (card.get("capabilities") or {}).get("extensions") or []:
        if isinstance(extension, dict) and extension.get("uri") == a2a_extensions.AGENT_URI:
            identity = (extension.get("params") or {}).get("identity")
            return identity if isinstance(identity, str) and identity else None
    return None


def _broker_flags() -> dict:
    """Only the signed_in/expired booleans of the broker's redacted status."""
    try:
        result = subprocess.run([os.environ.get("EXO_NODE", "node"),
                                 str(ROOT / "broker" / "exo-model.mjs"), "status"],
                                cwd=ROOT, capture_output=True, text=True, timeout=10,
                                env={k: v for k, v in os.environ.items()
                                     if k not in ("EXO_MODEL_HOME", "EXO_CODEX_BASE_URL")})
        value = json.loads(result.stdout) if result.returncode == 0 else {}
    except (OSError, subprocess.TimeoutExpired, ValueError):
        value = {}
    ready = _read_json(Path.home() / ".exomachina" / "model-broker" / "run" / "broker-ready.json")
    return {"signed_in": value.get("signed_in"), "expired": value.get("expired"),
            "broker_running": _alive(ready.get("pid"))}


def status(args: argparse.Namespace) -> dict:
    home, plan = args.home, args.plan
    listening = listening_ports()
    report: dict[str, object] = {"home": str(home), "provisioned":
                                 (home / "operator-stack.json").exists()}
    harness_pid = _harness_pid(home)
    code, health = _http_json(f"http://127.0.0.1:{plan['harness']}/health")
    readiness = (health or {}).get("readiness") or {} if isinstance(health, dict) else {}
    report["harness"] = {
        "pid": harness_pid, "alive": harness_pid is not None, "port": plan["harness"],
        "listening": plan["harness"] in listening, "health": code,
        "a2a_protocol": (health or {}).get("a2a_protocol") if isinstance(health, dict) else None,
        "card": _card_summary(plan["harness"]),
        "submission_status": readiness.get("submission_status"),
        "submission_blockers": readiness.get("submission_blockers"),
        "unfinished_runs": (health or {}).get("unfinished_runs") if isinstance(health, dict) else None,
    }
    pids = _read_json(home / "testbed" / "pids.json")
    services = {}
    for name in SERVICE_NAMES:
        record = pids.get(name) or {}
        port = plan[name]
        entry = {"pid": record.get("pid"), "alive": _alive(record.get("pid")), "port": port,
                 "listening": port in listening}
        if name in MODEL_AGENTS:
            # A2A agents are observed only through their public Agent Card
            # plus process/port checks; their model configuration is private.
            entry["card"] = _card_summary(port)
            entry["identity_matches"] = (entry["card"].get("identity") is not None and
                                         entry["card"].get("identity") == record.get("identity"))
        else:
            service_code, service_health = _http_json(f"http://127.0.0.1:{port}/health")
            entry["health"] = service_code
            if isinstance(service_health, dict):
                entry["identity_matches"] = service_health.get("identity") == record.get("identity")
        services[name] = entry
    report["services"] = services
    report["runner"] = _runner_status(args)
    active = _active_publication(home) if report["provisioned"] else None
    report["publication"] = ({key: active.get(key) for key in
                              ("label", "manifest_digest", "package_digest", "build_id")}
                             if active else None)
    if active and report["runner"].get("running"):
        try:
            worker = _worker(home, active["build_id"], timeout=5)
            report["runner"]["worker"] = {"build_id": active["build_id"], "polling": True,
                                          "pollers": {k: len(v) for k, v in
                                                      worker["live_pollers"].items()}}
        except Exception as error:  # noqa: BLE001 - status reports, never raises
            report["runner"]["worker"] = {"build_id": active["build_id"], "polling": False,
                                          "error": type(error).__name__}
    report["broker"] = _broker_flags()
    return report


def _runner_status(args: argparse.Namespace) -> dict:
    home, plan = args.home, args.plan
    if not (home / "runner-config.json").exists():
        return {"running": False, "provisioned": False}
    from runner import Runner
    runner = Runner(home)
    value = runner.status()
    report = {"running": value["running"], "address": value["address"], "pid": value["pid"],
              "pids": value["pids"], "builds": sorted(value["builds"])}
    pg = subprocess.run([str(args.pg_bin / "pg_isready"),
                         "-h", "127.0.0.1", "-p", str(plan["runner.postgres"])],
                        capture_output=True, text=True)
    report["postgres"] = {"port": plan["runner.postgres"], "ready": pg.returncode == 0}
    cli = args.temporal / "cli" / "temporal"
    try:
        health = subprocess.run([str(cli), "--disable-config-env", "--disable-config-file",
                                 "--address", value["address"], "operator", "cluster",
                                 "health"], capture_output=True, text=True, timeout=10)
        temporal_ok = health.returncode == 0 and "SERVING" in health.stdout
    except (OSError, subprocess.TimeoutExpired):
        temporal_ok = False
    report["temporal"] = {"frontend": plan["runner.frontend"], "serving": temporal_ok}
    return report


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if (Path(sys.executable) != args.python
            and os.environ.get("EXO_OPERATOR_STACK_REEXEC") != "1"):
        os.environ["EXO_OPERATOR_STACK_REEXEC"] = "1"
        os.execv(str(args.python), [str(args.python), "-B", str(Path(__file__).resolve()),
                                    *(sys.argv[1:] if argv is None else argv)])
    command = {"up": up, "down": down, "status": status}[args.command]
    try:
        value = command(args)
    except (RuntimeError, ValueError, TimeoutError) as error:
        print(json.dumps({"command": args.command, "error": str(error)}, indent=2))
        return 1
    print(json.dumps(value, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
