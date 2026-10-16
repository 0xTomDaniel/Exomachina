#!/usr/bin/env python3
"""Bounded Zigflow publication/rollout test on prepared local binaries.

All mutable runtime state and logs stay in /tmp. This is a disposable trial
launcher, not an installer, product supervisor, or A2A integration.
"""

from __future__ import annotations

import hashlib
import json
import signal
import socket
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
RUNTIME = Path("/tmp/exomachina-countertrials/temporal/runtime")
CLI = Path("/tmp/exomachina-temporal-evaluation/temporal")
PG = Path("/opt/homebrew/opt/postgresql@16/bin")
if not (PG / "initdb").exists():
    PG = RUNTIME / "pg/bin"
A2A_PYTHON = REPO / "tools/spikes/2026-09-22/s2/.venv/bin/python"
A2A_SERVER = REPO / "tools/spikes/2026-09-22/decision-round/common/harness_server.py"
A2A_CLIENT = REPO / "tools/spikes/2026-09-22/decision-round/common/client.py"
PORTS = {"pg": 31645, "frontend": 35633, "http": 35643, "matching": 35635,
         "history": 35634, "worker": 35639, "front_member": 30633,
         "match_member": 30635, "history_member": 30634, "worker_member": 30639,
         "pprof": 35946, "metrics": 36010, "v1_health": 37701,
         "v1_metrics": 37801, "v2_health": 37702, "v2_metrics": 37802,
         "v3_health": 37704, "v3_metrics": 37804, "a2a": 37703}
DEPLOYMENT = "exo-zigflow-reassessment"
QUEUE = "exo-zigflow-reassessment"
STATE: Path
CHILDREN: dict[str, subprocess.Popen] = {}


def command(args: list[str], *, timeout: int = 60, env=None) -> str:
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout, env=env)
    if result.returncode:
        raise RuntimeError(f"{Path(args[0]).name} exited {result.returncode}: {result.stderr[-900:]} {result.stdout[-300:]}")
    return result.stdout


def cli(*args: str, timeout: int = 20):
    base = [str(CLI), "--disable-config-env", "--disable-config-file",
            "--address", f"127.0.0.1:{PORTS['frontend']}", "--output", "json"]
    raw = command(base + list(args), timeout=timeout)
    return json.loads(raw) if raw.strip() else None


def open_port(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.15):
            return True
    except OSError:
        return False


def until(check, seconds: int = 60):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(0.25)
    raise TimeoutError("readiness condition not met")


def spawn(name: str, args: list[str], *, env=None) -> subprocess.Popen:
    log = (STATE / f"{name}.log").open("ab")
    try:
        process = subprocess.Popen(args, stdout=log, stderr=log, env=env,
                                   start_new_session=True)
    finally:
        log.close()
    CHILDREN[name] = process
    return process


def stop(name: str, sig=signal.SIGTERM):
    process = CHILDREN.pop(name, None)
    if process is None:
        return
    if process.poll() is None:
        process.send_signal(sig)
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def sql_command(database: str, *args: str):
    command([str(RUNTIME / "temporal-sql-tool"), "--endpoint", "127.0.0.1",
             "--port", str(PORTS["pg"]), "--user", "temporal", "--database", database,
             "--plugin", "postgres12", *args], timeout=120)


def setup_postgres():
    command([str(PG / "initdb"), "-D", str(STATE / "pgdata"),
             "--auth-local=trust", "--auth-host=trust", "--no-instructions"], timeout=120)
    command([str(PG / "pg_ctl"), "-D", str(STATE / "pgdata"), "-l",
             str(STATE / "postgres.log"), "-o",
             f"-p {PORTS['pg']} -h 127.0.0.1 -k {STATE / 'pgsocket'}", "start"])
    base = [str(PG / "psql"), "-h", "127.0.0.1", "-p", str(PORTS["pg"]), "-d", "postgres"]
    command(base + ["-c", "CREATE ROLE temporal LOGIN"])
    schema = RUNTIME / "temporal-1.32.0/schema/postgresql/v12"
    for database, subdir in (("temporal", "temporal"), ("temporal_visibility", "visibility")):
        command(base + ["-c", f"CREATE DATABASE {database} OWNER temporal"])
        sql_command(database, "setup-schema", "--version", "0.0")
        sql_command(database, "update-schema", "--schema-dir", str(schema / subdir / "versioned"))


def server_config() -> Path:
    config = (HERE / "server.template.yaml").read_text()
    for old, new in {"25545": "pg", "27233": "frontend", "27243": "http",
                     "27235": "matching", "27234": "history", "27239": "worker",
                     "26933": "front_member", "26935": "match_member",
                     "26934": "history_member", "26939": "worker_member",
                     "27936": "pprof", "28000": "metrics"}.items():
        config = config.replace(old, str(PORTS[new]))
    config = config.replace(
        "/tmp/exomachina-countertrials/temporal/runtime/config/dynamicconfig/development-sql.yaml",
        str(RUNTIME / "config/dynamicconfig/development-sql.yaml"))
    path = STATE / "server.yaml"
    path.write_text(config)
    return path


def start_engine():
    setup_postgres()
    server = spawn("temporal", [str(RUNTIME / "temporal-server"), "--config-file",
                                str(server_config()), "--allow-no-auth", "start"])
    until(lambda: open_port(PORTS["frontend"]) or server.poll() is not None)
    if server.poll() is not None:
        raise RuntimeError("Temporal failed to start")
    until(lambda: subprocess.run([str(CLI), "--disable-config-env", "--disable-config-file",
                                  "--address", f"127.0.0.1:{PORTS['frontend']}",
                                  "operator", "cluster", "health"], capture_output=True,
                                 timeout=5).returncode == 0)
    cli("operator", "namespace", "create", "--namespace", "exo-zig-reassess")


def json_cli(*args: str, timeout: int = 20):
    # Namespace is explicit on every publication and workflow operation.
    return cli("--namespace", "exo-zig-reassess", *args, timeout=timeout)


def http_json(url: str):
    with urllib.request.urlopen(url, timeout=3) as response:
        return json.loads(response.read())


def start_a2a_fixture() -> dict:
    process = spawn("a2a-agent", [str(A2A_PYTHON), str(A2A_SERVER),
                                  "--state", str(STATE / "a2a-state"),
                                  "--role", "capability", "--port", str(PORTS["a2a"])])
    def ready():
        if process.poll() is not None:
            raise RuntimeError("A2A fixture exited before ready")
        try:
            return http_json(f"http://127.0.0.1:{PORTS['a2a']}/health")
        except Exception:
            return False
    return until(ready)


def a2a_call(phase: str, digest: str) -> dict:
    # Real A2A 0.3.0 message/send to the independent Strands fixture. Zigflow
    # does not call this service in this publication-only test.
    return json.loads(command([str(A2A_PYTHON), str(A2A_CLIENT), "assign",
        "--url", f"http://127.0.0.1:{PORTS['a2a']}",
        "--action-id", "reassess:" + phase, "--run-id", "reassess",
        "--definition-digest", digest, "--brief", phase], timeout=30))


def publication(revision: int) -> dict:
    source = HERE / "definitions" / f"v{revision}.json"
    data = source.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    bundle = STATE / "bundles" / digest
    bundle.mkdir()
    target = bundle / "factory.json"
    target.write_bytes(data)
    if hashlib.sha256(target.read_bytes()).hexdigest() != digest:
        raise RuntimeError("bundle digest mismatch")
    validation = command([str(RUNTIME / "zigflow"), "--disable-telemetry",
                          "validate", str(target), "--output-json"])
    build_id = f"factory-v{revision}-{digest[:12]}"
    name = f"zigflow-v{revision}"
    process = spawn(name, [str(RUNTIME / "zigflow"), "--disable-telemetry", "run",
        "--file", str(target), "--temporal-address", f"127.0.0.1:{PORTS['frontend']}",
        "--temporal-namespace", "exo-zig-reassess", "--enable-versioning",
        "--default-versioning-type", "pinned", "--temporal-deployment-name", DEPLOYMENT,
        "--temporal-worker-build-id", build_id,
        "--health-listen-address", f"127.0.0.1:{PORTS[f'v{revision}_health']}",
        "--metrics-listen-address", f"127.0.0.1:{PORTS[f'v{revision}_metrics']}"])
    def ready():
        if process.poll() is not None:
            raise RuntimeError(f"{name} exited before ready")
        try:
            return http_json(f"http://127.0.0.1:{PORTS[f'v{revision}_health']}/readyz")
        except Exception:
            return False
    health = until(ready)
    def activate():
        try:
            return json_cli("worker", "deployment", "set-current-version",
                "--deployment-name", DEPLOYMENT, "--build-id", build_id, "--yes") or True
        except RuntimeError:
            return False
    until(activate)
    expected = f"{DEPLOYMENT}.{build_id}"
    activation_probes = []
    consecutive = 0
    # This exact Workflow type has no external effect before its signal. A
    # product rollout needs an equally safe route check or explicit start pin.
    for attempt in range(1, 16):
        canary_id = f"zig-reassess-v{revision}-canary-{attempt}"
        json_cli("workflow", "start", "--workflow-id", canary_id,
                 "--type", "reassessment-factory", "--task-queue", QUEUE,
                 "--fail-existing")
        def version_seen():
            info = json_cli("workflow", "describe", "--workflow-id", canary_id)
            return info["workflowExecutionInfo"].get("versioningInfo", {}).get("version")
        observed = until(version_seen)
        json_cli("workflow", "terminate", "--workflow-id", canary_id,
                 "--reason", "bounded rollout route probe")
        activation_probes.append({"workflow_id": canary_id, "pinned_version": observed})
        consecutive = consecutive + 1 if observed == expected else 0
        if consecutive == 2:
            break
        time.sleep(0.25)
    if consecutive < 2:
        raise RuntimeError(f"{expected} did not become stable current version")
    routing = json_cli("worker", "deployment", "describe", "--name", DEPLOYMENT)
    return {"revision": revision, "digest": digest, "build_id": build_id,
            "pid": process.pid, "validation": json.loads(validation), "health": health,
            "activation_probes": activation_probes, "routing": routing}


def start_run(revision: int) -> dict:
    workflow_id = f"zig-reassess-v{revision}"
    started = json_cli("workflow", "start", "--workflow-id", workflow_id,
                       "--type", "reassessment-factory", "--task-queue", QUEUE,
                       "--fail-existing")
    def pinned():
        info = json_cli("workflow", "describe", "--workflow-id", workflow_id)
        version = info["workflowExecutionInfo"].get("versioningInfo", {}).get("version")
        return info if version else False
    info = until(pinned)
    return {"workflow_id": workflow_id, "run_id": started.get("runId"),
            "status_before_signal": info["workflowExecutionInfo"]["status"],
            "pinned_version": info["workflowExecutionInfo"]["versioningInfo"]["version"]}


def finish_run(revision: int) -> dict:
    workflow_id = f"zig-reassess-v{revision}"
    json_cli("workflow", "signal", "--workflow-id", workflow_id,
             "--name", "approve", "--input", '{"approved":true}')
    result = json_cli("workflow", "result", "--workflow-id", workflow_id, timeout=40)
    return result.get("result")


def process_sample(label: str) -> dict:
    rows = command(["ps", "-axo", "pid=,ppid=,rss="]).splitlines()
    table = {}
    for row in rows:
        fields = row.split()
        if len(fields) == 3 and all(field.isdigit() for field in fields):
            table[int(fields[0])] = (int(fields[1]), int(fields[2]))
    pg_root = int((STATE / "pgdata/postmaster.pid").read_text().splitlines()[0])
    pg_pids = {pg_root}
    while True:
        additions = {pid for pid, (ppid, _) in table.items() if ppid in pg_pids}
        if additions <= pg_pids:
            break
        pg_pids |= additions
    groups = {"postgres": sorted(pg_pids)}
    for name, proc in CHILDREN.items():
        if proc.poll() is None and name != "a2a-agent":
            groups[name] = [proc.pid]
    groups["a2a-agent-excluded"] = [CHILDREN["a2a-agent"].pid]
    result = {"label": label, "groups": {}}
    for name, pids in groups.items():
        result["groups"][name] = {"pids": pids, "rss_kib": sum(table.get(pid, (0, 0))[1] for pid in pids)}
    engine_pids = [pid for name, pids in groups.items() if name != "a2a-agent-excluded" for pid in pids]
    result["engine_process_count"] = len(engine_pids)
    result["engine_summed_rss_kib"] = sum(table.get(pid, (0, 0))[1] for pid in engine_pids)
    footprint = subprocess.run(["footprint", "--noCategories", "-f", "bytes", *sum((["-p", str(pid)] for pid in engine_pids), [])],
                               capture_output=True, text=True, timeout=30)
    import re
    match = re.search(r"Summary Footprint:\s+(\d+) B", footprint.stdout)
    result["engine_macos_group_footprint_bytes"] = int(match.group(1)) if match else None
    return result


def main():
    global STATE
    for item in (RUNTIME / "temporal-server", RUNTIME / "zigflow", CLI, A2A_PYTHON):
        if not item.exists():
            raise FileNotFoundError(f"prepared host binary missing: {item}")
    for port in PORTS.values():
        if open_port(port):
            raise RuntimeError(f"trial port already in use: {port}")
    STATE = Path(tempfile.mkdtemp(prefix="exo-zig-reassess-", dir="/tmp"))
    (STATE / "pgsocket").mkdir()
    (STATE / "bundles").mkdir()
    output = {"state_dir": str(STATE), "topology": "Temporal 1.32.0 + PostgreSQL 16.15 + Zigflow 0.15.2; prepared host",
              "trial_security": "loopback trust/no-auth only", "samples": [], "publications": [], "runs": []}
    try:
        start_engine()
        a2a_before = start_a2a_fixture()
        output["a2a_before"] = {**a2a_before, "pid": CHILDREN["a2a-agent"].pid}
        output["a2a_before_call"] = a2a_call("before", "fixture-before")
        pub1 = publication(1)
        output["publications"].append(pub1)
        run1 = start_run(1)
        output["runs"].append(run1)
        output["samples"].append(process_sample("one waiting version"))
        pub2 = publication(2)
        output["publications"].append(pub2)
        run2 = start_run(2)
        output["runs"].append(run2)
        output["samples"].append(process_sample("two waiting versions"))
        pub3 = publication(3)
        output["publications"].append(pub3)
        run3 = start_run(3)
        output["runs"].append(run3)
        output["samples"].append(process_sample("three waiting versions"))
        stop("zigflow-v1", signal.SIGKILL)
        output["old_worker_killed_while_waiting"] = True
        # A supervisor replacement uses the original immutable bundle/build ID.
        old = STATE / "bundles" / pub1["digest"] / "factory.json"
        proc = spawn("zigflow-v1", [str(RUNTIME / "zigflow"), "--disable-telemetry", "run",
            "--file", str(old), "--temporal-address", f"127.0.0.1:{PORTS['frontend']}",
            "--temporal-namespace", "exo-zig-reassess", "--enable-versioning",
            "--default-versioning-type", "pinned", "--temporal-deployment-name", DEPLOYMENT,
            "--temporal-worker-build-id", pub1["build_id"],
            "--health-listen-address", f"127.0.0.1:{PORTS['v1_health']}",
            "--metrics-listen-address", f"127.0.0.1:{PORTS['v1_metrics']}"])
        def old_ready():
            if proc.poll() is not None:
                raise RuntimeError("replacement old worker exited")
            try:
                return http_json(f"http://127.0.0.1:{PORTS['v1_health']}/readyz")
            except Exception:
                return False
        until(old_ready)
        output["old_worker_replacement_pid"] = proc.pid
        output["samples"].append(process_sample("old worker replaced; three waiting versions"))
        a2a_after = http_json(f"http://127.0.0.1:{PORTS['a2a']}/health")
        output["a2a_after"] = {**a2a_after, "pid": CHILDREN["a2a-agent"].pid}
        output["a2a_after_call"] = a2a_call("after", "fixture-after")
        for run in output["runs"]:
            run["result"] = finish_run(int(run["workflow_id"][-1]))
            run["version_after"] = json_cli("workflow", "describe", "--workflow-id", run["workflow_id"])["workflowExecutionInfo"].get("versioningInfo", {}).get("version")
        output["status"] = "passed" if (
            [r["result"] for r in output["runs"]] == [
                {"revision": "1.0.0"},
                {"revision": "2.0.0", "path": "expanded"},
                {"revision": "3.0.0", "path": "expanded", "marker": "third-graph"}]
            and all(r["pinned_version"] == r["version_after"] for r in output["runs"])
            and output["a2a_before"] == output["a2a_after"]
            and len({p["pid"] for p in output["publications"]}) == 3
            and output["old_worker_replacement_pid"] != pub1["pid"]
        ) else "failed"
    except Exception as error:
        output["status"] = "failed"
        output["error"] = str(error)
        raise
    finally:
        for name in list(CHILDREN):
            stop(name)
        if (STATE / "pgdata").exists():
            subprocess.run([str(PG / "pg_ctl"), "-D", str(STATE / "pgdata"),
                            "-m", "fast", "stop"], capture_output=True, text=True, timeout=30)
        output["owned_ports_closed"] = all(not open_port(port) for port in PORTS.values())
        (HERE / "observed.json").write_text(json.dumps(output, indent=2) + "\n")
        print(json.dumps({"status": output["status"], "state_dir": str(STATE),
                          "owned_ports_closed": output["owned_ports_closed"]}, indent=2))
    if output["status"] != "passed" or not output["owned_ports_closed"]:
        raise RuntimeError("rollout probe failed; inspect observed.json")


if __name__ == "__main__":
    main()
