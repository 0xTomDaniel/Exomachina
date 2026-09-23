"""Local trial supervisor for the Temporal/Pg/Strands process topology.

It restarts failed children from persistent state. It is product-owned lifecycle
code, separate from Temporal's Workflow history and PostgreSQL stores.
"""
from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import os
import signal
import sys
import time
from pathlib import Path

import runtime
from probe import SERVICE_PORTS, health, start_service


def run(state: Path):
    state.mkdir(parents=True, exist_ok=True)
    runtime.STATE = state
    (state / "pgsocket").mkdir(exist_ok=True)
    (state / "catalog").mkdir(exist_ok=True)
    lock_file = (state / "supervisor.lock").open("w")
    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    stopped = False
    def stop(*_):
        nonlocal stopped
        stopped = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    processes = {}
    server = worker = None
    config = None
    def event(kind, **fields):
        with (state / "supervisor-events.jsonl").open("a") as stream:
            stream.write(json.dumps({"kind": kind, "time": time.time(), **fields},
                                    sort_keys=True) + "\n")
    try:
        first = not (state / "pgdata").exists()
        runtime.start_postgres(first)
        event("postgres-start", first=first)
        config = runtime.render_config()
        server = runtime.start_temporal(config)
        asyncio.run(runtime.wait_server(server))
        event("temporal-start", pid=server.pid)
        if first:
            runtime.shell([str(runtime.CLI), "--disable-config-env", "--disable-config-file",
                "--address", f"127.0.0.1:{runtime.PORTS['frontend']}",
                "--namespace", "exomachina", "operator", "namespace", "create",
                "--retention", "1d"])
        worker = runtime.start_worker()
        event("worker-start", pid=worker.pid)
        for name in ("source", "counter", "quality", "release", "opaque", "director"):
            processes[name] = start_service(name, state)
            asyncio.run(health(name, processes[name]))
            event("service-start", name=name, pid=processes[name].pid)
        (state / "supervisor-ready").write_text(str(os.getpid()))
        while not stopped:
            if (state / "hold-restart").exists():
                time.sleep(.2)
                continue
            if not runtime.port_open(runtime.PORTS["postgres"]):
                try:
                    runtime.start_postgres(False)
                    event("postgres-restart")
                except Exception as error:
                    event("postgres-restart-error", error=str(error))
                    time.sleep(.5)
                    continue
            if server.poll() is not None:
                try:
                    server = runtime.start_temporal(config)
                    asyncio.run(runtime.wait_server(server))
                    event("temporal-restart", pid=server.pid)
                except Exception as error:
                    event("temporal-restart-error", error=str(error))
                    time.sleep(.5)
                    continue
            if worker.poll() is not None:
                worker = runtime.start_worker()
                event("worker-restart", pid=worker.pid)
            for name, process in tuple(processes.items()):
                if process.poll() is not None:
                    processes[name] = start_service(name, state)
                    event("service-restart", name=name, pid=processes[name].pid)
            time.sleep(.2)
    finally:
        for process in processes.values():
            runtime.kill(process, signal.SIGTERM)
        if worker is not None:
            runtime.kill(worker, signal.SIGTERM)
        if server is not None:
            runtime.kill(server, signal.SIGTERM)
        if runtime.port_open(runtime.PORTS["postgres"]):
            runtime.pg_command("-m", "immediate", "stop")
        (state / "supervisor-ready").unlink(missing_ok=True)
        event("supervisor-stop")
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        lock_file.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", type=Path, required=True)
    args = parser.parse_args()
    run(args.state)
