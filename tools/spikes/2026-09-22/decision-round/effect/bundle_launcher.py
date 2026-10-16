"""One-command local supervisor for a copied Effect/Strands trial bundle.

This file is copied to bundle/launcher.py. It reads no repository source at runtime.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
from urllib.error import URLError
from urllib.request import urlopen

BUNDLE = Path(__file__).resolve().parent
PYTHON = BUNDLE / "venv" / "bin" / "python"
NODE = BUNDLE / "bin" / "node"
APP = BUNDLE / "app"


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def health(port):
    with urlopen(f"http://127.0.0.1:{port}/health", timeout=.5) as response:
        return json.load(response)


def process_field(pid, field):
    result = subprocess.run(["ps", "-o", field + "=", "-p", str(pid)],
                            capture_output=True, text=True)
    return result.stdout.strip() if result.returncode == 0 else ""


class Supervisor:
    def __init__(self, state: Path):
        self.state = state
        self.state.mkdir(parents=True, exist_ok=True)
        (self.state / "logs").mkdir(exist_ok=True)
        self.lock = (self.state / "supervisor.lock").open("a+")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("another supervisor owns this state") from error
        names = ("capability", "quality", "effect", "director_a", "director_b")
        topology = self.state / "topology.json"
        if topology.exists():
            prior = json.loads(topology.read_text())
            if prior.get("bundle_root") != str(BUNDLE) or set(prior.get("ports", {})) != set(names):
                raise RuntimeError("persisted topology does not match this bundle")
            self.ports = prior["ports"]
        else:
            ports = {}
            for name in names:
                candidate = free_port()
                while candidate in ports.values():
                    candidate = free_port()
                ports[name] = candidate
            self.ports = ports
            topology.write_text(json.dumps({"bundle_root": str(BUNDLE), "ports": ports}, indent=2) + "\n")
        ready = self.state / "ready.json"
        self.previous = json.loads(ready.read_text()) if ready.exists() else None
        if self.previous and (self.previous.get("bundle_root") != str(BUNDLE) or
                              self.previous.get("ports") != self.ports):
            raise RuntimeError("previous supervisor record conflicts with topology")
        self.children = {}
        self.restart_counts = dict(self.previous["restart_counts"]) if self.previous else \
            {name: 0 for name in self.ports}
        self.generation = (self.previous.get("generation", 0) + 1) if self.previous else 1
        self.stopping = False
        self.started_at = time.monotonic()

    def reclaim_previous_children(self):
        if not self.previous:
            return
        live = []
        for name, record in self.previous["children"].items():
            pid = record["pid"]
            status = process_field(pid, "stat")
            if not status or status.startswith("Z"):
                continue
            expected = " ".join(self.command(name))
            actual = process_field(pid, "command")
            if actual != expected or record.get("command") != expected or \
               process_field(pid, "lstart") != record.get("lstart"):
                raise RuntimeError(f"refusing to reclaim changed process {name} PID {pid}")
            live.append(pid)
        for pid in live:
            os.kill(pid, signal.SIGTERM)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            remaining = [pid for pid in live if (status := process_field(pid, "stat")) and
                         not status.startswith("Z")]
            if not remaining:
                return
            time.sleep(.1)
        for pid in remaining:
            os.kill(pid, signal.SIGKILL)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if all(not (status := process_field(pid, "stat")) or status.startswith("Z")
                   for pid in remaining):
                return
            time.sleep(.1)
        raise RuntimeError("previous children did not stop")

    def command(self, name):
        port = self.ports[name]
        if name in ("capability", "quality"):
            return [str(PYTHON), str(APP / "common" / "harness_server.py"),
                    "--state", str(self.state / name), "--role", name,
                    "--port", str(port)]
        if name == "effect":
            return [str(NODE), str(APP / "effect" / "helper.mjs")]
        return [str(PYTHON), str(APP / "effect" / "director_server.py"),
                "--state", str(self.state / name),
                "--effect", f"http://127.0.0.1:{self.ports['effect']}",
                "--port", str(port)]

    def start_one(self, name):
        port = self.ports[name]
        number = self.restart_counts[name]
        log_path = self.state / "logs" / f"{name}-{number}.log"
        env = os.environ.copy()
        env["EXO_S2_ROOT"] = str(APP / "s2")
        env["EFFECT_DECISION_ROOT"] = str(self.state / "effect")
        env["EFFECT_DECISION_PORT"] = str(self.ports["effect"])
        with log_path.open("ab") as log:
            process = subprocess.Popen(self.command(name), cwd=APP / "effect",
                                       env=env, stdout=log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(f"{name} exited {process.returncode}; see {log_path}")
            try:
                info = health(port)
                self.children[name] = {"process": process, "health": info, "log": str(log_path),
                                       "command": " ".join(self.command(name)),
                                       "lstart": process_field(process.pid, "lstart")}
                self.write_ready()
                return
            except (URLError, TimeoutError, ConnectionError):
                time.sleep(.1)
        process.kill()
        process.wait(timeout=5)
        raise TimeoutError(f"{name} failed to become ready; see {log_path}")

    def write_ready(self):
        body = {"bundle_root": str(BUNDLE), "state_root": str(self.state),
                "supervisor_pid": os.getpid(), "generation": self.generation,
                "startup_seconds": round(time.monotonic() - self.started_at, 3),
                "ports": self.ports, "restart_counts": self.restart_counts,
                "children": {name: {"pid": item["process"].pid,
                                    "health": item["health"], "log": item["log"],
                                    "command": item["command"], "lstart": item["lstart"]}
                             for name, item in self.children.items()}}
        temporary = self.state / "ready.json.tmp"
        temporary.write_text(json.dumps(body, indent=2) + "\n")
        temporary.replace(self.state / "ready.json")

    def run(self):
        self.reclaim_previous_children()
        for name in self.ports:
            self.start_one(name)
        self.write_ready()
        print(json.dumps({"ready": str(self.state / "ready.json"),
                          "startup_seconds": round(time.monotonic() - self.started_at, 3)}), flush=True)
        while not self.stopping:
            for name, item in list(self.children.items()):
                process = item["process"]
                if process.poll() is not None:
                    self.children.pop(name)
                    self.restart_counts[name] += 1
                    self.start_one(name)
            time.sleep(.1)

    def stop(self, *_):
        self.stopping = True

    def shutdown(self):
        self.stopping = True
        for item in self.children.values():
            if item["process"].poll() is None:
                item["process"].terminate()
        for item in self.children.values():
            try:
                item["process"].wait(timeout=5)
            except subprocess.TimeoutExpired:
                item["process"].kill()
                item["process"].wait(timeout=5)
        (self.state / "ready.json").unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", type=Path, default=BUNDLE / "state")
    args = parser.parse_args()
    supervisor = Supervisor(args.state.resolve())
    signal.signal(signal.SIGINT, supervisor.stop)
    signal.signal(signal.SIGTERM, supervisor.stop)
    try:
        supervisor.run()
    finally:
        supervisor.shutdown()


if __name__ == "__main__":
    main()
