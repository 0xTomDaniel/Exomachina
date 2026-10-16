"""Exercise two distinct Strands/A2A harness processes and the lost-ack path."""
from __future__ import annotations

import argparse
import hashlib
import json
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

from client import UncertainSubmission, get_task, reconcile, send


HERE = Path(__file__).parent


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def launch(state, role, number):
    log = (state.parent / f"{role}-{time.time_ns()}.log").open("w")
    process = subprocess.Popen([sys.executable, str(HERE / "harness_server.py"),
        "--state", str(state), "--role", role, "--port", str(number)],
        stdout=log, stderr=log)
    log.close()
    url = f"http://127.0.0.1:{number}"
    for _ in range(100):
        if process.poll() is not None:
            raise RuntimeError(f"{role} exited; see {log.name}")
        try:
            with urllib.request.urlopen(url + "/health", timeout=.25) as response:
                return process, url, json.loads(response.read()), log.name
        except Exception:
            time.sleep(.1)
    process.kill()
    raise RuntimeError(f"{role} startup timeout; see {log.name}")


def exercise(base):
    base.mkdir(parents=True, exist_ok=True)
    processes = []
    try:
        capability, capability_url, cap_identity, cap_log = launch(base / "capability", "capability", port())
        processes.append(capability)
        quality, quality_url, quality_identity, quality_log = launch(base / "quality", "quality", port())
        processes.append(quality)
        assert cap_identity["identity"] != quality_identity["identity"]
        digest = hashlib.sha256(b"definition-v1").hexdigest()
        first = send(capability_url, {"op": "assign", "action_id": "run-1:research",
            "run_id": "run-1", "definition_digest": digest, "brief": "verified research"})
        assert first["artifact"]["revision"] == "r2"
        reviewed = send(quality_url, {"op": "review", "action_id": "run-1:review",
            "run_id": "run-1", "definition_digest": digest, "artifact": first["artifact"]})
        assert reviewed["artifact"]["accepted"] is True
        assert reviewed["artifact"]["reviewer"] != first["artifact"]["author"]
        lost = False
        try:
            send(capability_url, {"op": "assign", "action_id": "run-2:research",
                "run_id": "run-2", "definition_digest": digest,
                "brief": "lost acknowledgement", "drop_ack": True})
        except UncertainSubmission:
            lost = True
        assert lost, "fixture did not drop the acknowledgement"
        assert capability.wait(timeout=5) == 23
        restarted, capability_url, cap_restarted, restart_log = launch(
            base / "capability", "capability", port())
        processes.append(restarted)
        assert cap_restarted["identity"] == cap_identity["identity"]
        assert cap_restarted["incarnation"] == cap_identity["incarnation"] + 1
        found = reconcile(capability_url, "run-2:research", "run-2", digest)
        assert found["accepted_count"] == 1 and found["attempts"] == 1
        restored_task = get_task(capability_url, found["task_id"])
        assert restored_task["status"]["state"] == "completed"
        assert restored_task["artifacts"][0]["parts"][0]["data"] == found["artifact"]
        retried = send(capability_url, {"op": "assign", "action_id": "run-2:research",
            "run_id": "run-2", "definition_digest": digest, "brief": "lost acknowledgement"})
        assert retried["artifact"] == found["artifact"]
        found_again = reconcile(capability_url, "run-2:research", "run-2", digest)
        assert found_again["accepted_count"] == 1 and found_again["attempts"] == 2
        result = {"passed": True, "a2a_protocol": "0.3.0", "capability": cap_identity,
                  "quality": quality_identity, "capability_restart": cap_restarted,
                  "first": first, "review": reviewed, "lost_ack": found_again,
                  "restored_a2a_task_id": restored_task["id"],
                  "logs": [cap_log, quality_log, restart_log]}
        (base / "smoke-result.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result, indent=2))
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path)
    args = parser.parse_args()
    if args.base is None:
        with tempfile.TemporaryDirectory(prefix="exomachina-decision-common-") as directory:
            exercise(Path(directory))
    else:
        exercise(args.base)
