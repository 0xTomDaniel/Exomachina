#!/usr/bin/env python3
"""Pinned non-development Temporal Server and PostgreSQL trial lifecycle."""

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
PORTS = {"postgres": 31265, "frontend": 42033, "http": 42043, "matching": 42035,
         "history": 42034, "worker": 42039, "front_member": 31253,
         "match_member": 31255, "history_member": 31254, "worker_member": 31259,
         "pprof": 42056, "metrics": 42020}


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
               EXO_TEMPORAL_ACTIVITY_LOG=str(STATE / "activities.jsonl"))
    log = (STATE / "worker.log").open("a")
    process = subprocess.Popen([sys.executable, str(HERE / "worker.py")],
                               cwd=HERE, env=env, stdout=log, stderr=log, start_new_session=True)
    log.close()
    return process


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
    return await until(check, seconds=60)


def kill(process: subprocess.Popen, sig: signal.Signals) -> None:
    """Legacy caller name; only orderly termination is permitted in this lane."""
    if sig != signal.SIGTERM:
        raise ValueError("only graceful SIGTERM shutdown is allowed")
    if process.poll() is None:
        process.terminate()
        process.wait(timeout=30)
