#!/usr/bin/env python3
"""Detached, install-wide PostgreSQL, Temporal, and versioned worker runner."""
from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import os
import secrets
import signal
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


HERE = Path(__file__).resolve().parent
NAMESPACE = "exomachina"
DEPLOYMENT = "exo-factory"
QUEUE = "exo-factory"  # must equal binding.QUEUE
DEFAULT_RUNTIME = Path("/tmp/exomachina-countertrials/temporal/runtime")
DEFAULT_PG_BIN = Path("/opt/homebrew/opt/postgresql@16/bin")
DEFAULT_CLI = Path("/tmp/exomachina-temporal-evaluation/temporal")


def port_map(port_base: int, member_base: int) -> dict[str, int]:
    if not 1024 <= port_base <= 65535 or port_base + 12 > 65535:
        raise ValueError("port_base must keep all runner ports within 1024..65535")
    if not 1024 <= member_base <= 65535 or member_base + 4 > 32767:
        raise ValueError("member_base must keep PostgreSQL and membership ports within 1024..32767")
    return {
        "postgres": member_base, "front_member": member_base + 1,
        "match_member": member_base + 2, "history_member": member_base + 3,
        "worker_member": member_base + 4, "frontend": port_base + 2,
        "http": port_base + 3, "matching": port_base + 4,
        "history": port_base + 5, "worker": port_base + 6,
        "pprof": port_base + 11, "metrics": port_base + 12,
    }


def render_config(template: str, ports: dict[str, int], password: str,
                  runtime: Path, *, max_conns: int = 8) -> str:
    replacements = {
        "25545": ports["postgres"], "27233": ports["frontend"],
        "27243": ports["http"], "27235": ports["matching"],
        "27234": ports["history"], "27239": ports["worker"],
        "26933": ports["front_member"], "26935": ports["match_member"],
        "26934": ports["history_member"], "26939": ports["worker_member"],
        "27936": ports["pprof"], "28000": ports["metrics"],
    }
    for old, new in replacements.items():
        template = template.replace(old, str(new))
    template = template.replace(
        "/tmp/exomachina-countertrials/temporal/runtime/config/dynamicconfig/development-sql.yaml",
        str(runtime / "config/dynamicconfig/development-sql.yaml"))
    template = template.replace('password: "temporal"', f'password: "{password}"')
    template = template.replace("maxConns: 20", f"maxConns: {max_conns}")
    template = template.replace("maxIdleConns: 20", f"maxIdleConns: {max_conns}")
    return template


def parse_ready(path: Path) -> dict | None:
    try:
        value = json.loads(path.read_text())
        if (not isinstance(value.get("pid"), int) or value["pid"] <= 0 or
                not isinstance(value.get("address"), str) or
                value.get("namespace") != NAMESPACE or
                not isinstance(value.get("builds"), dict)):
            return None
        return value
    except (FileNotFoundError, ValueError, TypeError, KeyError):
        return None


def _write_json(path: Path, value: dict) -> None:
    with path.open("w") as stream:
        json.dump(value, stream, sort_keys=True)
        stream.write("\n")


def _event(state: Path, kind: str, **fields: object) -> None:
    state.mkdir(parents=True, exist_ok=True)
    with (state / "runner-events.jsonl").open("a") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        stream.write(json.dumps({"kind": kind, "time": time.time(), **fields}, sort_keys=True) + "\n")
        stream.flush()
        fcntl.flock(stream, fcntl.LOCK_UN)


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


def _lock_held(path: Path) -> bool:
    with path.open("a+") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(stream, fcntl.LOCK_UN)
    return False


def _run(args: list[str], *, env: dict | None = None, timeout: float = 120) -> str:
    result = subprocess.run(args, env=env, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"{Path(args[0]).name} exited {result.returncode}: {result.stderr[-1200:]}")
    return result.stdout


def _port_open(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=.2):
            return True
    except OSError:
        return False


def _rss(pid: int | None) -> int | None:
    if not pid:
        return None
    result = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], text=True, capture_output=True)
    try:
        return int(result.stdout.strip()) if result.returncode == 0 else None
    except ValueError:
        return None


class Runner:
    def __init__(self, home: Path, *, port_base: int | None = None,
                 member_base: int | None = None):
        self.home = Path(home).resolve()
        self.state = self.home / "runner"
        self.ports = port_map(
            int(port_base if port_base is not None else os.getenv("EXO_RUNNER_PORT_BASE", "44000")),
            int(member_base if member_base is not None else os.getenv("EXO_RUNNER_MEMBER_BASE", "32400")))
        self.address = f"127.0.0.1:{self.ports['frontend']}"
        self.namespace = NAMESPACE
        self.runtime = Path(os.getenv("EXO_TEMPORAL_RUNTIME", str(DEFAULT_RUNTIME)))
        self.pg_bin = Path(os.getenv("EXO_PG_BIN", str(DEFAULT_PG_BIN)))
        self.cli = Path(os.getenv("EXO_TEMPORAL_CLI", str(DEFAULT_CLI)))

    def is_running(self) -> bool:
        ready = parse_ready(self.state / "runner-ready.json")
        return bool(ready and ready.get("running", True) and
                    ready["address"] == self.address and _pid_alive(ready["pid"]) and
                    _lock_held(self.state / "runner.lock"))

    def ensure_started(self, *, reason: str, timeout: float = 180) -> dict:
        if not reason:
            raise ValueError("start reason is required")
        self.state.mkdir(parents=True, exist_ok=True)
        if self.is_running():
            _event(self.state, "attach", reason=reason, pid=parse_ready(self.state / "runner-ready.json")["pid"])
            return self.status()
        env = dict(os.environ, EXO_RUNNER_PORT_BASE=str(self.ports["frontend"] - 2),
                   EXO_RUNNER_MEMBER_BASE=str(self.ports["postgres"]))
        with (self.state / "runner-supervisor.log").open("a") as log:
            child = subprocess.Popen([sys.executable, str(HERE / "runner.py"), "serve", "--home", str(self.home)],
                                     env=env, stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                                     start_new_session=True)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.is_running():
                ready = parse_ready(self.state / "runner-ready.json")
                _event(self.state, "start" if ready["pid"] == child.pid else "attach",
                       reason=reason, pid=ready["pid"])
                return self.status()
            if child.poll() is not None and child.returncode != 0 and not _lock_held(self.state / "runner.lock"):
                raise RuntimeError(f"runner serve exited {child.returncode}; see runner-supervisor.log")
            time.sleep(.2)
        raise TimeoutError(f"runner not ready within {timeout}s; see runner-supervisor.log")

    def ensure_build(self, source_dir: Path) -> dict:
        # The interpreter lane owns this list and digest. Import only when publishing a build.
        from binding import INTERPRETER_FILES, build_id_for, source_digest

        source_dir = Path(source_dir).resolve()
        digest = source_digest(source_dir)
        build_id = build_id_for(digest)
        files = tuple(INTERPRETER_FILES)
        record = {"build_id": build_id, "source_digest": digest, "files": list(files)}
        builds = self.state / "builds"
        builds.mkdir(parents=True, exist_ok=True)
        with (builds / "build.lock").open("a+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            path = builds / build_id
            if path.exists():
                manifest = path / "build.json"
                if not manifest.exists() or json.loads(manifest.read_text()) != record:
                    raise ValueError(f"immutable build conflict: {build_id}")
                for name in files:
                    if not (path / name).is_file() or (path / name).read_bytes() != (source_dir / name).read_bytes():
                        raise ValueError(f"immutable build content conflict: {build_id}/{name}")
            else:
                path.mkdir()
                for name in files:
                    target = path / name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with target.open("xb") as output:
                        output.write((source_dir / name).read_bytes())
                if source_digest(path) != digest:
                    raise ValueError(f"source changed while snapshotting {build_id}")
                with (path / "build.json").open("x") as output:
                    json.dump(record, output, sort_keys=True)
                    output.write("\n")
                _event(self.state, "build-added", build_id=build_id, source_digest=digest)
            fcntl.flock(lock, fcntl.LOCK_UN)
        return {**record, "path": str(path)}

    def wait_worker(self, build_id: str, timeout: float = 60) -> dict:
        from temporalio.api.workflowservice.v1 import request_response_pb2
        from temporalio.client import Client

        async def probe() -> dict | None:
            from temporalio.api.enums.v1 import TaskQueueType
            from temporalio.api.taskqueue.v1 import TaskQueue
            client = await Client.connect(self.address, namespace=self.namespace)
            service = client.service_client.workflow_service
            version = f"{DEPLOYMENT}.{build_id}"
            await service.describe_worker_deployment_version(
                request_response_pb2.DescribeWorkerDeploymentVersionRequest(
                    namespace=self.namespace, version=version))
            # Registration alone is not enough: require a live poller of this
            # exact build on both the Workflow and Activity task queues.
            live = {}
            for kind in (TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW, TaskQueueType.TASK_QUEUE_TYPE_ACTIVITY):
                response = await service.describe_task_queue(
                    request_response_pb2.DescribeTaskQueueRequest(
                        namespace=self.namespace, task_queue=TaskQueue(name=QUEUE),
                        task_queue_type=kind))
                now = time.time()
                pollers = [p for p in response.pollers
                           if p.deployment_options.build_id == build_id
                           and now - p.last_access_time.ToSeconds() < 70]
                live[TaskQueueType.Name(kind)] = [p.identity for p in pollers]
            if all(live.values()):
                return {"build_id": build_id, "version": version, "registered": True,
                        "api": "describe_worker_deployment_version+describe_task_queue",
                        "live_pollers": live}
            return None

        deadline = time.monotonic() + timeout
        last_error = None
        while time.monotonic() < deadline:
            try:
                result = asyncio.run(probe())
                if result:
                    return result
            except Exception as error:
                last_error = str(error)
            time.sleep(.5)
        raise TimeoutError(f"worker {build_id} not registered in {timeout}s: {last_error}")

    def status(self) -> dict:
        ready = parse_ready(self.state / "runner-ready.json")
        running = self.is_running()
        pids = (ready or {}).get("pids", {}) if running else {}
        builds = (ready or {}).get("builds", {}) if running else {}
        return {"running": running, "address": self.address, "namespace": self.namespace,
                "pid": ready["pid"] if running else None, "pids": pids, "builds": builds,
                "rss_kib": {name: _rss(pid) for name, pid in pids.items()},
                "worker_rss_kib": {name: _rss(value.get("pid")) for name, value in builds.items()}}

    def stop(self, timeout: float = 60) -> dict:
        ready = parse_ready(self.state / "runner-ready.json")
        if not self.is_running() or ready is None:
            return self.status()
        os.kill(ready["pid"], signal.SIGTERM)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if not self.is_running():
                return self.status()
            time.sleep(.2)
        raise TimeoutError("runner did not stop gracefully")

    def _secret(self, name: str) -> str:
        directory = self.state / "secrets"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / name
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            pass
        else:
            with os.fdopen(fd, "w") as stream:
                stream.write(secrets.token_hex(24) + "\n")
        if path.stat().st_mode & 0o077:
            raise ValueError(f"secret permissions too broad: {path}")
        return path.read_text().strip()

    def _pg_ctl(self, *args: str) -> str:
        return _run([str(self.pg_bin / "pg_ctl"), "-D", str(self.state / "pgdata"), *args])

    def _start_postgres(self) -> bool:
        first = not (self.state / "pgdata" / "PG_VERSION").exists()
        (self.state / "pgsocket").mkdir(parents=True, exist_ok=True)
        postgres_password = self._secret("postgres-password")
        temporal_password = self._secret("temporal-password")
        if first:
            _run([str(self.pg_bin / "initdb"), "-D", str(self.state / "pgdata"),
                  "--auth-local=scram-sha-256", "--auth-host=scram-sha-256",
                  "--pwfile", str(self.state / "secrets/postgres-password"),
                  "--no-instructions"])
        if not _port_open(self.ports["postgres"]):
            self._pg_ctl("-l", str(self.state / "postgres.log"), "-o",
                         f"-p {self.ports['postgres']} -h 127.0.0.1 -k {self.state / 'pgsocket'} "
                         f"-c shared_buffers={os.getenv('EXO_PG_SHARED_BUFFERS', '16MB')} "
                         f"-c max_connections={os.getenv('EXO_PG_MAX_CONNECTIONS', '30')}", "start")
        if first:
            base = [str(self.pg_bin / "psql"), "-h", "127.0.0.1", "-p",
                    str(self.ports["postgres"]), "-d", "postgres"]
            env = dict(os.environ, PGPASSWORD=postgres_password)
            _run(base + ["-c", f"CREATE ROLE temporal LOGIN PASSWORD '{temporal_password}'"], env=env)
            for database in ("temporal", "temporal_visibility"):
                _run(base + ["-c", f"CREATE DATABASE {database} OWNER temporal"], env=env)
                sql = [str(self.runtime / "temporal-sql-tool"), "--endpoint", "127.0.0.1",
                       "--port", str(self.ports["postgres"]), "--user", "temporal",
                       "--password", temporal_password, "--database", database,
                       "--plugin", "postgres12"]
                _run(sql + ["setup-schema", "--version", "0.0"])
                schema = self.runtime / "temporal-1.32.0/schema/postgresql/v12" / (
                    "temporal" if database == "temporal" else "visibility") / "versioned"
                _run(sql + ["update-schema", "--schema-dir", str(schema)])
        return first

    def _start_temporal(self) -> subprocess.Popen:
        config = render_config((HERE / "server.template.yaml").read_text(), self.ports,
                               self._secret("temporal-password"), self.runtime,
                               max_conns=int(os.getenv("EXO_TEMPORAL_MAX_CONNS", "8")))
        path = self.state / "server.yaml"
        path.write_text(config)
        path.chmod(0o600)
        with (self.state / "temporal.log").open("a") as log:
            return subprocess.Popen([str(self.runtime / "temporal-server"), "--config-file",
                                     str(path), "--allow-no-auth", "start"], cwd=self.runtime,
                                    stdout=log, stderr=log)

    def _start_worker(self, build_id: str, path: Path, manifest: dict) -> subprocess.Popen:
        env = dict(os.environ, EXO_TEMPORAL_ADDRESS=self.address,
                   EXO_WORKER_BUILD_ID=build_id,
                   EXO_WORKER_SOURCE_DIGEST=manifest["source_digest"],
                   EXO_OUTCOME_DB=str(self.state / "outcomes.sqlite3"),
                   EXO_ACTIVITY_LOG=str(self.state / "activities.jsonl"))
        with (self.state / f"worker-{build_id}.log").open("a") as log:
            return subprocess.Popen([sys.executable, "worker.py"], cwd=path, env=env,
                                    stdout=log, stderr=log)

    def _serve(self) -> None:
        self.state.mkdir(parents=True, exist_ok=True)
        with (self.state / "runner.lock").open("a+") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return
            stopping = False
            def request_stop(*_: object) -> None:
                nonlocal stopping
                stopping = True
            signal.signal(signal.SIGTERM, request_stop)
            signal.signal(signal.SIGINT, request_stop)
            workers: dict[str, subprocess.Popen] = {}
            server: subprocess.Popen | None = None
            first = False
            started_at = datetime.now(timezone.utc).isoformat()

            def ready(running: bool) -> None:
                pg_pid_path = self.state / "pgdata/postmaster.pid"
                pg_pid = int(pg_pid_path.read_text().splitlines()[0]) if pg_pid_path.exists() else None
                _write_json(self.state / "runner-ready.json", {
                    "pid": os.getpid(), "address": self.address, "namespace": NAMESPACE,
                    "started_at": started_at, "running": running,
                    "pids": {"postgres": pg_pid, "temporal": server.pid if server else None},
                    "builds": {key: {"pid": process.pid,
                                     "source_digest": json.loads((self.state / "builds" / key / "build.json").read_text())["source_digest"]}
                               for key, process in workers.items()},
                })

            try:
                first = self._start_postgres()
                _event(self.state, "postgres-start", first=first)
                server = self._start_temporal()
                _event(self.state, "temporal-start", pid=server.pid)
                deadline = time.monotonic() + 90
                while time.monotonic() < deadline and not stopping:
                    if server.poll() is not None:
                        raise RuntimeError(f"Temporal exited {server.returncode}")
                    if _port_open(self.ports["frontend"]):
                        break
                    time.sleep(.25)
                else:
                    raise TimeoutError("Temporal frontend not ready")
                namespace_command = [str(self.cli), "--disable-config-env", "--disable-config-file",
                                     "--address", self.address, "--namespace", NAMESPACE,
                                     "operator", "namespace"]
                described = subprocess.run(namespace_command + ["describe"], text=True,
                                           capture_output=True, timeout=30)
                if described.returncode:
                    _run(namespace_command + ["create", "--retention", "1d"])
                    _event(self.state, "namespace-create", namespace=NAMESPACE)
                ready(True)
                _event(self.state, "serve-ready", pid=os.getpid())
                while not stopping:
                    if not _port_open(self.ports["postgres"]):
                        try:
                            self._start_postgres()
                            _event(self.state, "postgres-restart")
                            ready(True)
                        except Exception as error:
                            _event(self.state, "postgres-restart-error", error=str(error))
                    if server.poll() is not None:
                        try:
                            server = self._start_temporal()
                            _event(self.state, "temporal-restart", pid=server.pid)
                            ready(True)
                        except Exception as error:
                            _event(self.state, "temporal-restart-error", error=str(error))
                    build_root = self.state / "builds"
                    if build_root.exists():
                        for manifest_path in build_root.glob("*/build.json"):
                            build_id = manifest_path.parent.name
                            try:
                                manifest = json.loads(manifest_path.read_text())
                                if manifest.get("retired") or (manifest_path.parent / "retired").exists():
                                    prior = workers.pop(build_id, None)
                                    if prior is not None and prior.poll() is None:
                                        prior.terminate()
                                        prior.wait(timeout=20)
                                        _event(self.state, "worker-retired", build_id=build_id)
                                        ready(True)
                                    continue
                                if manifest["build_id"] != build_id or not (manifest_path.parent / "worker.py").is_file():
                                    raise ValueError("invalid build manifest")
                                prior = workers.get(build_id)
                                if prior is None or prior.poll() is not None:
                                    workers[build_id] = self._start_worker(build_id, manifest_path.parent, manifest)
                                    _event(self.state, "worker-start" if prior is None else "worker-restart",
                                           build_id=build_id, pid=workers[build_id].pid)
                                    ready(True)
                            except Exception as error:
                                _event(self.state, "worker-start-error", build_id=build_id, error=str(error))
                    time.sleep(.5)
            finally:
                for process in workers.values():
                    if process.poll() is None:
                        process.terminate()
                for process in workers.values():
                    try:
                        process.wait(timeout=20)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
                if server is not None and server.poll() is None:
                    server.terminate()
                    try:
                        server.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        server.kill()
                        server.wait(timeout=5)
                if _port_open(self.ports["postgres"]):
                    self._pg_ctl("-m", "fast", "stop")
                ready(False)
                _event(self.state, "serve-stop", pid=os.getpid())
                fcntl.flock(lock, fcntl.LOCK_UN)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("serve", "start", "stop", "status"))
    parser.add_argument("--home", required=True, type=Path)
    parser.add_argument("--reason", default="cli")
    args = parser.parse_args()
    runner = Runner(args.home)
    if args.command == "serve":
        runner._serve()
    else:
        value = (runner.ensure_started(reason=args.reason) if args.command == "start" else
                 runner.stop() if args.command == "stop" else runner.status())
        print(json.dumps(value, sort_keys=True))


if __name__ == "__main__":
    main()
