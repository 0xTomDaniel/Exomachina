"""Single local owner of the Dagu arbitration trial's long-running services."""
from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path
import shlex
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

import bridge

HERE = Path(__file__).resolve().parent
SPIKES = HERE.parent.parent / "2026-09-22"
PYTHON = SPIKES / "s2" / ".venv" / "bin" / "python"
COMMON = SPIKES / "arbitration" / "common"
PRIOR_COMMON = SPIKES / "decision-round" / "common"


def port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def health(url: str) -> dict | None:
    try:
        with urllib.request.urlopen(url + "/health", timeout=1) as response:
            return json.load(response)
    except (OSError, urllib.error.URLError):
        return None


def dagu_health(url: str) -> bool:
    try:
        with urllib.request.urlopen(url + "/api/v1/dags", timeout=1) as response:
            return response.status == 200
    except (OSError, urllib.error.URLError):
        return False


class Supervisor:
    def __init__(self, rt: Path, dagu: Path):
        self.rt = rt
        self.dagu = dagu
        os.environ["EXO_ARB_RUNTIME"] = str(rt)
        rt.mkdir(parents=True, exist_ok=True)
        self.lock = (rt / "supervisor.lock").open("w")
        fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        ports_file = rt / "ports.json"
        if ports_file.exists():
            self.ports = json.loads(ports_file.read_text())
        else:
            self.ports = {name: port() for name in ("dagu", "source_evidence", "counter_evidence",
                                                    "quality", "release", "opaque", "director")}
            ports_file.write_text(json.dumps(self.ports, sort_keys=True, indent=2) + "\n")
        self.urls = {name: f"http://127.0.0.1:{value}" for name, value in self.ports.items()}
        self.children: dict[str, subprocess.Popen] = {}
        self.restart_counts: dict[str, int] = {}
        self.stopping = False
        self.env = {**os.environ, "EXO_ARB_RUNTIME": str(rt), "DAGU_HOME": str(rt / "home"),
                    "DAGU_AUTH_MODE": "none", "DAGU_COORDINATOR_ENABLED": "false"}
        bin_dir = rt / "bin"
        bin_dir.mkdir(exist_ok=True)
        wrapper = bin_dir / "exo-arb"
        wrapper.write_text("#!/bin/sh\nexport EXO_ARB_RUNTIME=" + shlex.quote(str(rt)) + "\nexec " + shlex.quote(str(PYTHON)) + " "
                           + shlex.quote(str(HERE / "ops.py")) + " \"$@\"\n")
        wrapper.chmod(0o755)
        self.env["PATH"] = str(bin_dir) + os.pathsep + self.env["PATH"]

    def specs(self) -> dict[str, list[str]]:
        return {
            "source_evidence": [str(PYTHON), str(HERE / "slow_harness_server.py"),
                                "--state", str(self.rt / "source_evidence"), "--role", "capability",
                                "--port", str(self.ports["source_evidence"])],
            "counter_evidence": [str(PYTHON), str(PRIOR_COMMON / "harness_server.py"),
                                 "--state", str(self.rt / "counter_evidence"), "--role", "capability",
                                 "--port", str(self.ports["counter_evidence"])],
            "quality": [str(PYTHON), str(COMMON / "quality_server.py"), "--state", str(self.rt / "quality"),
                        "--port", str(self.ports["quality"])],
            "release": [str(PYTHON), str(COMMON / "release_server.py"), "--state", str(self.rt / "release"),
                        "--mode", "participating", "--port", str(self.ports["release"])],
            "opaque": [str(PYTHON), str(COMMON / "release_server.py"), "--state", str(self.rt / "opaque"),
                       "--mode", "opaque", "--port", str(self.ports["opaque"])],
            "dagu": [str(self.dagu), "start-all", "--host", "127.0.0.1", "--port", str(self.ports["dagu"])],
            "director": [str(PYTHON), str(HERE / "director_server.py"), "--state", str(self.rt / "director"),
                         "--port", str(self.ports["director"])],
            "reconciler": [str(PYTHON), str(HERE / "reconciler.py"), "--dagu", str(self.dagu)],
        }

    def write_registry(self) -> None:
        path = self.rt / "processes.json"
        temp = path.with_suffix(".tmp")
        temp.write_text(json.dumps({name: {"pid": proc.pid, "starts": self.restart_counts[name]}
                                    for name, proc in self.children.items()}, indent=2, sort_keys=True) + "\n")
        temp.replace(path)

    def start(self, name: str, argv: list[str]) -> None:
        log = self.rt / f"{name}.log"
        with log.open("ab") as stream:
            proc = subprocess.Popen(argv, env=self.env, stdout=stream, stderr=stream,
                                    start_new_session=True)
        self.children[name] = proc
        self.restart_counts[name] = self.restart_counts.get(name, 0) + 1
        self.write_registry()
        with (self.rt / "supervisor-events.jsonl").open("a") as stream:
            stream.write(json.dumps({"name": name, "pid": proc.pid,
                                     "start": self.restart_counts[name], "time": time.time()}) + "\n")

    def ready(self, name: str) -> bool:
        return dagu_health(self.urls[name]) if name == "dagu" else bool(health(self.urls[name]))

    def boot(self) -> None:
        specs = self.specs()
        for name in ("source_evidence", "counter_evidence", "quality", "release", "opaque", "dagu"):
            self.start(name, specs[name])
        deadline = time.monotonic() + 35
        while time.monotonic() < deadline:
            if all(self.ready(name) for name in ("source_evidence", "counter_evidence", "quality",
                                                "release", "opaque", "dagu")):
                break
            time.sleep(.2)
        else:
            raise TimeoutError("supervised services did not become healthy")
        identities = {name: health(self.urls[name])["identity"] for name in
                      ("source_evidence", "counter_evidence", "quality", "release", "opaque")}
        if len(set(identities.values())) != len(identities):
            raise ValueError("fixture service identities are not distinct")
        config_path = self.rt / "config.json"
        data = {"urls": self.urls, "identities": identities,
                "director_token": "fixture-director-token", "dagu_binary": str(self.dagu)}
        if config_path.exists() and json.loads(config_path.read_text()) != data:
            raise ValueError("service identity or port binding changed")
        config_path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
        self.start("director", specs["director"])
        self.start("reconciler", specs["reconciler"])
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and not self.ready("director"):
            time.sleep(.1)
        if not self.ready("director"):
            raise TimeoutError("Director did not become healthy")
        (self.rt / "ready").write_text("ready\n")

    def loop(self) -> None:
        specs = self.specs()
        while not self.stopping:
            for name, process in list(self.children.items()):
                if process.poll() is not None:
                    self.start(name, specs[name])
            if self.ready("dagu"):
                try:
                    bridge.scan()
                except Exception as error:
                    with (self.rt / "bridge-events.jsonl").open("a") as stream:
                        stream.write(json.dumps({"kind": "scan_error", "error": repr(error)}) + "\n")
            time.sleep(.2)

    def stop(self, *_args) -> None:
        self.stopping = True
        for process in self.children.values():
            if process.poll() is None:
                try: os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError: pass
        for process in self.children.values():
            try: process.wait(timeout=6)
            except subprocess.TimeoutExpired:
                try: os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError: pass
        # Dagu's run commands may live in independent sessions after start-all
        # exits. Scope cleanup to this run's DAG files, never a host-wide kill.
        listing = subprocess.check_output(["ps", "-axo", "pid=,pgid=,command="], text=True)
        groups = set()
        for line in listing.splitlines():
            parts = line.strip().split(None, 2)
            if (len(parts) == 3 and "dagu start" in parts[2]
                and str(self.rt / "home" / "dags") in parts[2]):
                groups.add(int(parts[1]))
        for group in groups:
            try: os.killpg(group, signal.SIGKILL)
            except ProcessLookupError: pass


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("runtime", type=Path)
    parser.add_argument("--dagu", type=Path, default=Path("/tmp/exomachina-countertrials/dagu/dagu"))
    args = parser.parse_args()
    supervisor = Supervisor(args.runtime.resolve(), args.dagu.resolve())
    signal.signal(signal.SIGTERM, supervisor.stop)
    signal.signal(signal.SIGINT, supervisor.stop)
    try:
        supervisor.boot()
        supervisor.loop()
    finally:
        supervisor.stop()


if __name__ == "__main__":
    main()
