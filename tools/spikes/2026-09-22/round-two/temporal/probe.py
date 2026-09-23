#!/usr/bin/env python3
"""Run the v1/v2 publication and crash/recovery countertrial on a real server."""

import asyncio
import hashlib
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
from datetime import timedelta
from pathlib import Path

from temporalio.client import Client

from factory import FactoryRun


HERE = Path(__file__).resolve().parent
RUNTIME = Path(os.environ.get("EXO_TEMPORAL_RUNTIME", "/tmp/exomachina-countertrials/temporal/runtime"))
TEMPORAL = RUNTIME / "temporal-server"
SQL_TOOL = RUNTIME / "temporal-sql-tool"
SCHEMA = RUNTIME / "temporal-1.32.0/schema/postgresql/v12"
PG_BIN = Path(os.environ.get("EXO_PG_BIN", "/opt/homebrew/opt/postgresql@16/bin"))
if not (PG_BIN / "initdb").exists():
    PG_BIN = RUNTIME / "pg/bin"
CLI = Path(os.environ.get("EXO_TEMPORAL_CLI", "/tmp/exomachina-temporal-evaluation/temporal"))
PORTS = {"postgres": 31545, "frontend": 35233, "http": 35243, "matching": 35235,
         "history": 35234, "worker": 35239, "front_member": 30933,
         "match_member": 30935, "history_member": 30934, "worker_member": 30939,
         "pprof": 35936, "metrics": 36000}


def shell(args: list[str], *, env=None, timeout=60) -> str:
    completed = subprocess.run(args, text=True, capture_output=True, env=env, timeout=timeout)
    if completed.returncode:
        raise RuntimeError(f"{Path(args[0]).name} exited {completed.returncode}: {completed.stderr[-1000:]}")
    return completed.stdout


def port_open(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            return True
    except OSError:
        return False


async def until(check, *, seconds=60):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = await check()
        if value:
            return value
        await asyncio.sleep(0.25)
    raise TimeoutError("condition not reached")


def pg_command(*args: str) -> None:
    shell([str(PG_BIN / "pg_ctl"), "-D", str(STATE / "pgdata"), *args], timeout=60)


def sql_command(database: str, *args: str) -> None:
    shell([str(SQL_TOOL), "--endpoint", "127.0.0.1", "--port", str(PORTS["postgres"]),
           "--user", "temporal", "--database", database, "--plugin", "postgres12", *args], timeout=120)


def start_postgres(first: bool) -> None:
    if first:
        shell([str(PG_BIN / "initdb"), "-D", str(STATE / "pgdata"), "--auth-local=trust",
               "--auth-host=trust", "--no-instructions"], timeout=120)
    pg_command("-l", str(STATE / "postgres.log"), "-o",
               f"-p {PORTS['postgres']} -h 127.0.0.1 -k {STATE / 'pgsocket'}", "start")
    if first:
        psql = str(PG_BIN / "psql")
        base = [psql, "-h", "127.0.0.1", "-p", str(PORTS["postgres"]), "-d", "postgres"]
        shell(base + ["-c", "CREATE ROLE temporal LOGIN"])
        for name in ("temporal", "temporal_visibility"):
            shell(base + ["-c", f"CREATE DATABASE {name} OWNER temporal"])
            sql_command(name, "setup-schema", "--version", "0.0")
            directory = "temporal" if name == "temporal" else "visibility"
            sql_command(name, "update-schema", "--schema-dir", str(SCHEMA / directory / "versioned"))


def render_config() -> Path:
    config = (HERE / "server.template.yaml").read_text()
    replacements = {
        "25545": PORTS["postgres"], "27233": PORTS["frontend"], "27243": PORTS["http"],
        "27235": PORTS["matching"], "27234": PORTS["history"], "27239": PORTS["worker"],
        "26933": PORTS["front_member"], "26935": PORTS["match_member"],
        "26934": PORTS["history_member"], "26939": PORTS["worker_member"],
        "27936": PORTS["pprof"], "28000": PORTS["metrics"],
    }
    for before, after in replacements.items():
        config = config.replace(before, str(after))
    old_config = "/tmp/exomachina-countertrials/temporal/runtime/config/dynamicconfig/development-sql.yaml"
    new_config = RUNTIME / "config/dynamicconfig/development-sql.yaml"
    config = config.replace(old_config, str(new_config))
    path = STATE / "server.yaml"
    path.write_text(config)
    return path


def start_temporal(config: Path) -> subprocess.Popen:
    log = (STATE / "temporal.log").open("a")
    process = subprocess.Popen([str(TEMPORAL), "--config-file", str(config), "--allow-no-auth", "start"],
                               cwd=RUNTIME, stdout=log, stderr=log, start_new_session=True)
    log.close()
    return process


def start_worker() -> subprocess.Popen:
    env = dict(os.environ, EXO_TEMPORAL_ADDRESS=f"127.0.0.1:{PORTS['frontend']}",
               EXO_TEMPORAL_EVENTS=str(STATE / "events.jsonl"),
               EXO_TEMPORAL_LEDGER=str(STATE / "delivery.sqlite"))
    log = (STATE / "worker.log").open("a")
    process = subprocess.Popen([sys.executable, str(HERE / "worker.py")],
                               cwd=HERE, env=env, stdout=log, stderr=log, start_new_session=True)
    log.close()
    return process


def validate(document: dict) -> None:
    if document.get("schema") != 1 or document.get("factory") != "verified-research":
        raise ValueError("unsupported factory schema")
    steps = document.get("steps", [])
    kinds = [step.get("type") for step in steps]
    if kinds not in (["parallel", "review", "director-decision", "deliver"],
                     ["parallel", "assign", "review", "director-decision", "deliver"]):
        raise ValueError("unreviewed, unbounded, or unsupported factory path")
    if steps[0].get("capabilities") != ["research-a@1", "research-b@1"]:
        raise ValueError("unapproved research capabilities")
    if "assign" in kinds and steps[1].get("capability") != "verify@1":
        raise ValueError("unapproved verifier")
    if steps[-3].get("capability") != "review@1" or steps[-1].get("capability") != "delivery@1":
        raise ValueError("unapproved reviewer or delivery capability")


def publish(name: str) -> tuple[dict, str]:
    document = json.loads((HERE / "fixtures" / name).read_text())
    validate(document)
    canonical = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    digest = hashlib.sha256(canonical).hexdigest()
    path = STATE / "catalog" / f"{digest}.json"
    with path.open("xb") as file:
        file.write(canonical)
        file.flush()
        os.fsync(file.fileno())
    return document, digest


def rss_kib(pid: int) -> int:
    return int(shell(["ps", "-o", "rss=", "-p", str(pid)]).strip())


def sample(server: subprocess.Popen, worker: subprocess.Popen) -> dict:
    pg_parent = int((STATE / "pgdata/postmaster.pid").read_text().splitlines()[0])
    rows = shell(["ps", "-axo", "pid=,ppid=,rss="]).splitlines()
    table = {}
    for row in rows:
        fields = row.split()
        if len(fields) == 3:
            table[int(fields[0])] = (int(fields[1]), int(fields[2]))
    pg_pids = {pg_parent}
    while True:
        added = {pid for pid, (parent, _) in table.items() if parent in pg_pids}
        if added <= pg_pids:
            break
        pg_pids |= added
    return {
        "temporal_server_rss_kib": rss_kib(server.pid),
        "python_worker_rss_kib": rss_kib(worker.pid),
        "postgres_process_count": len(pg_pids),
        "postgres_summed_rss_kib_shared_pages_double_counted": sum(table.get(pid, (0, 0))[1] for pid in pg_pids),
    }


async def wait_server(process: subprocess.Popen) -> None:
    async def check():
        if process.poll() is not None:
            raise RuntimeError(f"Temporal exited {process.returncode}; inspect {STATE / 'temporal.log'}")
        if not port_open(PORTS["frontend"]):
            return False
        try:
            client = await Client.connect(f"127.0.0.1:{PORTS['frontend']}", namespace="exomachina")
            await client.service_client.check_health()
            return True
        except Exception:
            return False
    await until(check, seconds=60)


async def wait_phase(handle, phase: str) -> dict:
    async def check():
        try:
            status = await handle.query(FactoryRun.status)
            return status if status["phase"] == phase else False
        except Exception:
            return False
    return await until(check, seconds=40)


def kill(process: subprocess.Popen, sig: signal.Signals) -> None:
    if process.poll() is None:
        os.killpg(process.pid, sig)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)


async def main() -> None:
    global STATE
    STATE = Path(tempfile.mkdtemp(prefix="exomachina-temporal-round2-", dir="/tmp"))
    (STATE / "pgsocket").mkdir()
    (STATE / "catalog").mkdir()
    observed = {"status": "running", "versions": {}, "topology": "non-development Temporal Server + PostgreSQL + one Python SDK worker"}
    server = worker = None
    pg_running = False
    try:
        for port in PORTS.values():
            if port_open(port):
                raise RuntimeError(f"port {port} is already occupied")
        for binary in (TEMPORAL, SQL_TOOL, SCHEMA, PG_BIN / "initdb", CLI):
            if not binary.exists():
                raise FileNotFoundError(binary)
        observed["versions"] = {
            "temporal_server": shell([str(TEMPORAL), "--version"]).strip(),
            "postgres": shell([str(PG_BIN / "postgres"), "--version"]).strip(),
            "python_sdk": __import__("importlib.metadata", fromlist=["version"]).version("temporalio"),
            "server_sha256": hashlib.sha256(TEMPORAL.read_bytes()).hexdigest(),
        }
        start_postgres(first=True)
        pg_running = True
        config = render_config()
        server = start_temporal(config)
        await wait_server(server)
        shell([str(CLI), "--disable-config-env", "--disable-config-file", "--address",
               f"127.0.0.1:{PORTS['frontend']}", "--namespace", "exomachina",
               "operator", "namespace", "create", "--retention", "1d"])
        worker = start_worker()
        client_a = await Client.connect(f"127.0.0.1:{PORTS['frontend']}", namespace="exomachina")
        client_b = await Client.connect(f"127.0.0.1:{PORTS['frontend']}", namespace="exomachina")

        try:
            publish("bypass.json")
            raise AssertionError("review bypass was accepted")
        except ValueError as error:
            observed["bypass_rejection"] = str(error)
        v1, digest1 = publish("v1.json")
        run1 = "harness-a-v1"
        handle1 = await client_a.start_workflow(FactoryRun.run,
            {"run": run1, "identity": "harness-a", "document": v1, "digest": digest1},
            id=run1, task_queue="factory-interpreter", execution_timeout=timedelta(minutes=5))
        observed["v1_before_publication"] = await wait_phase(handle1, "awaiting-decision")
        worker_pid_before_v2 = worker.pid
        v2, digest2 = publish("v2.json")
        run2 = "harness-b-v2"
        handle2 = await client_b.start_workflow(FactoryRun.run,
            {"run": run2, "identity": "harness-b", "document": v2, "digest": digest2},
            id=run2, task_queue="factory-interpreter", execution_timeout=timedelta(minutes=5))
        observed["v2_before_restart"] = await wait_phase(handle2, "awaiting-decision")
        observed["worker_pid_stable_through_v2_publication"] = worker.pid == worker_pid_before_v2 and worker.poll() is None
        observed["warm_paused_resource"] = sample(server, worker)
        observed["catalog_digests"] = {"v1": digest1, "v2": digest2}

        kill(worker, signal.SIGKILL)
        kill(server, signal.SIGKILL)
        pg_command("-m", "immediate", "stop")
        pg_running = False
        start_postgres(first=False)
        pg_running = True
        server = start_temporal(config)
        await wait_server(server)
        worker = start_worker()
        client_a = await Client.connect(f"127.0.0.1:{PORTS['frontend']}", namespace="exomachina")
        client_b = await Client.connect(f"127.0.0.1:{PORTS['frontend']}", namespace="exomachina")
        handle1 = client_a.get_workflow_handle(run1)
        handle1_retry = client_b.get_workflow_handle(run1)
        handle2 = client_b.get_workflow_handle(run2)
        observed["v1_after_restart"] = await wait_phase(handle1, "awaiting-decision")
        observed["v2_after_restart"] = await wait_phase(handle2, "awaiting-decision")
        observed["warm_restarted_resource"] = sample(server, worker)

        async def vote(handle, actor: str):
            try:
                return await handle.execute_update(FactoryRun.decide, {"actor": actor, "approved": True})
            except Exception as error:
                return type(error).__name__
        observed["raced_v1_decisions"] = await asyncio.gather(
            vote(handle1, "director-a"), vote(handle1_retry, "director-a-retry"))
        observed["v2_decision"] = await handle2.execute_update(
            FactoryRun.decide, {"actor": "director-b", "approved": True})
        observed["results"] = {"v1": await handle1.result(), "v2": await handle2.result()}
        events = [json.loads(line) for line in (STATE / "events.jsonl").read_text().splitlines()]
        observed["events"] = events
        db = __import__("sqlite3").connect(STATE / "delivery.sqlite")
        observed["accepted_delivery_rows"] = dict(db.execute("SELECT run, count(*) FROM delivery GROUP BY run"))
        db.close()
        assert observed["worker_pid_stable_through_v2_publication"]
        assert observed["v1_after_restart"]["digest"] == digest1
        assert observed["v2_after_restart"]["digest"] == digest2
        assert sorted(observed["raced_v1_decisions"]) == ["accepted", "already-decided"]
        assert observed["v2_decision"] == "accepted"
        assert "verify@1" not in observed["results"]["v1"]["completed"]
        assert "verify@1" in observed["results"]["v2"]["completed"]
        assert observed["accepted_delivery_rows"] == {run1: 1, run2: 1}
        assert sum(event["kind"] == "deliver-attempt" for event in events) == 2
        observed["status"] = "passed"
    except Exception as error:
        observed["status"] = "failed"
        observed["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        if worker is not None:
            kill(worker, signal.SIGTERM)
        if server is not None:
            kill(server, signal.SIGTERM)
        if pg_running:
            try:
                pg_command("-m", "fast", "stop")
            except Exception as error:
                observed["cleanup_error"] = str(error)
        observed["owned_ports_closed"] = not port_open(PORTS["frontend"]) and not port_open(PORTS["postgres"])
        observed["state_directory"] = str(STATE)
        (HERE / "observed.json").write_text(json.dumps(observed, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"status": observed["status"], "state_directory": str(STATE),
                          "owned_ports_closed": observed["owned_ports_closed"]}, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
