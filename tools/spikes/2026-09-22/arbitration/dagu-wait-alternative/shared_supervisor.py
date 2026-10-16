"""One process owns Dagu and reconciles pinned terminal child outcomes.

The scan has no graph queue: Dagu owns steps and waits. This process checks
only product rows with an authorized terminal child outcome, then completes
their existing parent human.task gate. A single process serves all pairs.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import urllib.error

import auto_bridge
import bridge


class Supervisor:
    def __init__(self, runtime: Path, dagu: Path, port: int):
        self.runtime = runtime
        self.dagu = dagu
        self.port = port
        self.base = f"http://127.0.0.1:{port}"
        self.runtime.mkdir(parents=True, exist_ok=True)
        self.lock = (runtime / "shared_supervisor.lock").open("w")
        fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.dagu_process: subprocess.Popen | None = None
        self.stopping = False
        self.env = {**os.environ, "DAGU_HOME": str(runtime / "home"),
                    "DAGU_AUTH_MODE": "none", "DAGU_COORDINATOR_ENABLED": "false"}

    def event(self, kind: str, detail: dict) -> None:
        with (self.runtime / "shared_events.jsonl").open("a") as stream:
            stream.write(json.dumps({"kind": kind, **detail}, sort_keys=True) + "\n")

    def start_dagu(self) -> None:
        with (self.runtime / "dagu.log").open("ab") as stream:
            self.dagu_process = subprocess.Popen(
                [str(self.dagu), "start-all", "--host", "127.0.0.1", "--port", str(self.port)],
                env=self.env, stdout=stream, stderr=stream, start_new_session=True)
        (self.runtime / "dagu_pid").write_text(str(self.dagu_process.pid) + "\n")
        self.event("dagu_start", {"pid": self.dagu_process.pid})

    def ready(self) -> bool:
        try:
            return bool(bridge.request(self.base, "/api/v1/dags"))
        except (OSError, urllib.error.URLError):
            return False

    def scan(self) -> None:
        with auto_bridge.connect(self.runtime) as db:
            pending = [dict(row) for row in db.execute("SELECT * FROM bridge WHERE state='waiting'")]
        for candidate in pending:
            with auto_bridge.connect(self.runtime) as db:
                db.execute("BEGIN IMMEDIATE")
                row = db.execute("SELECT * FROM bridge WHERE parent_id=?", (candidate["parent_id"],)).fetchone()
                if row is None or row["state"] != "waiting":
                    continue
                state = db.execute("SELECT * FROM auto_state WHERE parent_id=?", (row["parent_id"],)).fetchone()
                if state is None or state["authorized_decision"] != "accept" or state["child_outcome"] != "accepted":
                    continue
                if (row["parent_sha256"] != bridge.digest(self.runtime, row["parent_name"])
                        or row["child_sha256"] != bridge.digest(self.runtime, row["child_name"])):
                    raise ValueError("pinned closure mutation")
                child = bridge.status(self.base, row["child_name"], row["child_id"])
                child_nodes = bridge.nodes(child)
                if (child["statusLabel"] != "succeeded"
                        or child_nodes.get("director_wait") != "succeeded"
                        or child_nodes.get("child_outcome") != "succeeded"):
                    continue
                try:
                    parent = bridge.status(self.base, row["parent_name"], row["parent_id"])
                except urllib.error.HTTPError as error:
                    if error.code == 404:
                        continue
                    raise
                parent_nodes = bridge.nodes(parent)
                if parent["statusLabel"] == "waiting" and parent_nodes.get("child_gate") == "waiting":
                    if (self.runtime / "pause_reconcile").exists():
                        (self.runtime / "scanner_entered").write_text(str(os.getpid()) + "\n")
                        while (self.runtime / "pause_reconcile").exists() and not self.stopping:
                            time.sleep(.1)
                    response = bridge.request(self.base,
                        f"/api/v1/dag-runs/{row['parent_name']}/{row['parent_id']}/human-tasks/child_gate/complete",
                        {"decision": "child_completed", "child_run_id": row["child_id"]})
                    self.event("parent_gate_posted", {"parent_id": row["parent_id"],
                                                      "child_id": row["child_id"],
                                                      "already_completed": response.get("alreadyCompleted")})
                    if (self.runtime / "lose_ack_once").exists():
                        (self.runtime / "lose_ack_once").unlink()
                        raise RuntimeError("synthetic lost scanner continuation after parent gate ack")
                elif parent_nodes.get("child_gate") != "succeeded":
                    raise ValueError("parent gate is neither current nor completed")
                db.execute("UPDATE bridge SET state='completed' WHERE parent_id=?", (row["parent_id"],))
                self.event("bridge_completed", {"parent_id": row["parent_id"],
                                                "child_id": row["child_id"]})

    def loop(self) -> None:
        self.start_dagu()
        while not self.stopping:
            if self.dagu_process is not None and self.dagu_process.poll() is not None:
                self.start_dagu()
            if self.ready():
                (self.runtime / "ready").write_text("ready\n")
                try:
                    self.scan()
                except Exception as error:
                    self.event("scan_error", {"error": repr(error)})
            time.sleep(.35)

    def stop(self, *_args) -> None:
        self.stopping = True
        if self.dagu_process is not None and self.dagu_process.poll() is None:
            os.killpg(self.dagu_process.pid, signal.SIGTERM)
            try:
                self.dagu_process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                os.killpg(self.dagu_process.pid, signal.SIGKILL)
                self.dagu_process.wait(timeout=3)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("runtime", type=Path)
    parser.add_argument("--dagu", type=Path, required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    supervisor = Supervisor(args.runtime, args.dagu, args.port)
    signal.signal(signal.SIGTERM, supervisor.stop)
    signal.signal(signal.SIGINT, supervisor.stop)
    try:
        supervisor.loop()
    finally:
        supervisor.stop()


if __name__ == "__main__":
    main()
