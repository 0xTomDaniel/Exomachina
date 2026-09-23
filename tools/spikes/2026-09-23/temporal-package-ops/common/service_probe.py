"""Prove the common real-service fixture independently of any candidate engine.

This does not prove publication, authoritative acceptance, Director behavior, or
engine recovery. It verifies that every candidate can use the same remote peers.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "decision-round" / "common"))
from client import UncertainSubmission, reconcile, send  # noqa: E402
from fixture import assignment, candidate_artifact, typed_join  # noqa: E402
from oracle import summarize_service  # noqa: E402
from receiver_client import UncertainDelivery, opaque_submit, receipt, release  # noqa: E402


HERE = Path(__file__).resolve().parent
OLD_HARNESS = HERE.parents[1] / "decision-round" / "common" / "harness_server.py"


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def launch(base: Path, name: str, script: Path, args: list[str]):
    port = free_port()
    log_path = base / f"{name}-{time.time_ns()}.log"
    with log_path.open("w") as log:
        process = subprocess.Popen([sys.executable, str(script), "--state",
            str(base / name), "--port", str(port), *args], stdout=log, stderr=log)
    url = f"http://127.0.0.1:{port}"
    for _ in range(100):
        if process.poll() is not None:
            raise RuntimeError(f"{name} exited during startup; see {log_path}")
        try:
            with urllib.request.urlopen(url + "/health", timeout=.25) as response:
                return process, url, json.loads(response.read()), str(log_path)
        except Exception:
            time.sleep(.1)
    process.kill()
    raise RuntimeError(f"{name} startup timeout; see {log_path}")


def stop(processes):
    for process in processes:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


def branch_pair(urls, run_id, digest):
    barrier = threading.Barrier(2)

    def one(branch):
        barrier.wait(timeout=5)
        started = time.monotonic_ns()
        result = send(urls[branch], assignment(run_id, digest, branch))
        return result, started, time.monotonic_ns()

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {name: pool.submit(one, name) for name in urls}
        completed = {name: future.result() for name, future in futures.items()}
    starts = [value[1] for value in completed.values()]
    ends = [value[2] for value in completed.values()]
    if max(starts) >= min(ends):
        raise AssertionError("branch assignments did not overlap")
    receipts = {name: value[0] for name, value in completed.items()}
    intervals = {name: {"start": value[1], "end": value[2]}
                 for name, value in completed.items()}
    return typed_join(receipts, run_id=run_id, definition_digest=digest), receipts, intervals


def review(quality_url, run_id, digest, artifact):
    result = send(quality_url, {"op": "review",
        "action_id": f"{run_id}:quality:{artifact['revision']}", "run_id": run_id,
        "definition_digest": digest, "artifact": artifact})
    verdict = result["artifact"]
    if verdict["revision"] != artifact["revision"] or verdict["sha256"] != artifact["sha256"]:
        raise AssertionError("Quality verdict is not bound to exact artifact")
    if verdict["reviewer"] == artifact["author"]:
        raise AssertionError("Quality self-review")
    return result


def exercise(base: Path, summary_out: Path | None = None):
    base.mkdir(parents=True, exist_ok=True)
    processes = []
    logs = []

    def started(name, script, args):
        process, url, health, log = launch(base, name, script, args)
        processes.append(process)
        logs.append(log)
        return process, url, health

    try:
        cap_a, cap_a_url, a_health = started("source", OLD_HARNESS,
                                            ["--role", "capability"])
        _, cap_b_url, b_health = started("counter", OLD_HARNESS,
                                        ["--role", "capability"])
        _, quality_url, q_health = started("quality", HERE / "quality_server.py", [])
        release_process, release_url, release_health = started(
            "release", HERE / "release_server.py", ["--mode", "participating"])
        opaque, opaque_url, opaque_health = started("opaque", HERE / "release_server.py",
                                                   ["--mode", "opaque"])
        identities = {a_health["identity"], b_health["identity"], q_health["identity"]}
        assert len(identities) == 3
        digest = hashlib.sha256(b"arbitration-visible-v3-service-fixture").hexdigest()
        urls = {"source_evidence": cap_a_url, "counter_evidence": cap_b_url}

        joined, branches, intervals = branch_pair(urls, "repair-success", digest)
        r1 = candidate_artifact(joined, "r1", a_health["identity"], resolved=False)
        r2 = candidate_artifact(joined, "r2", a_health["identity"], resolved=True)
        rejected = review(quality_url, "repair-success", digest, r1)
        approved = review(quality_url, "repair-success", digest, r2)
        assert rejected["artifact"]["accepted"] is False
        assert approved["artifact"]["accepted"] is True
        rejected_action = reconcile(quality_url, rejected["action_id"],
                                    "repair-success", digest)
        assert rejected_action["accepted_count"] == 1

        exhausted_join, _, _ = branch_pair(urls, "repair-exhausted", digest)
        exhausted = [review(quality_url, "repair-exhausted", digest,
            candidate_artifact(exhausted_join, f"r{index}", a_health["identity"],
                               resolved=False)) for index in (1, 2, 3)]
        assert all(item["artifact"]["accepted"] is False for item in exhausted)

        lost_command = assignment("lost-reply", digest, "source_evidence", drop_ack=True)
        try:
            send(cap_a_url, lost_command)
            raise AssertionError("receiver did not drop acknowledgment")
        except UncertainSubmission:
            pass
        assert cap_a.wait(timeout=5) == 23
        _, cap_a_url, a_restart = started("source", OLD_HARNESS,
                                         ["--role", "capability"])
        assert a_restart["identity"] == a_health["identity"]
        lost = reconcile(cap_a_url, lost_command["action_id"], "lost-reply", digest)
        assert lost["attempts"] == 1 and lost["accepted_count"] == 1

        release_command = {"release_id": "repair-success:release", "run_id": "repair-success",
            "definition_digest": digest, "revision": r2["revision"],
            "sha256": r2["sha256"], "content": r2["content"], "drop_ack": True}
        try:
            release(release_url, release_command)
            raise AssertionError("release receiver did not drop acknowledgment")
        except UncertainDelivery:
            pass
        assert release_process.wait(timeout=5) == 23
        _, release_url, release_restart = started(
            "release", HERE / "release_server.py", ["--mode", "participating"])
        assert release_restart["identity"] == release_health["identity"]
        release_record = receipt(release_url, release_command["release_id"])
        assert release_record["accepted_effect_count"] == 1 and release_record["attempts"] == 1
        repeated = release(release_url, {**release_command, "drop_ack": False})
        assert repeated["accepted_effect_count"] == 1 and repeated["attempts"] == 2

        opaque_command = {key: release_command[key] for key in
                          ("run_id", "definition_digest", "revision", "sha256", "content")}
        opaque_command["drop_ack"] = True
        try:
            opaque_submit(opaque_url, opaque_command)
            raise AssertionError("opaque receiver did not drop acknowledgment")
        except UncertainDelivery:
            pass
        assert opaque.wait(timeout=5) == 23
        _, opaque_url, opaque_restart = started("opaque", HERE / "release_server.py",
                                                ["--mode", "opaque"])
        assert opaque_restart["identity"] == opaque_health["identity"]
        try:
            receipt(opaque_url, "unknown")
            raise AssertionError("opaque receiver exposed discovery")
        except RuntimeError as error:
            assert "404" in str(error)
        with sqlite3.connect(base / "opaque" / "release.sqlite3") as db:
            opaque_effects = db.execute("SELECT COUNT(*) FROM opaque_effects").fetchone()[0]
        assert opaque_effects == 1  # Evaluator-only inspection; caller cannot know it.

        result = {"fixture_only": True, "passed": True, "protocol": "A2A 0.3.0",
            "identities": {"source": a_health, "counter": b_health, "quality": q_health,
                           "release": release_health, "opaque": opaque_health},
            "branch_receipts": branches, "branch_intervals_ns": intervals,
            "typed_join": joined,
            "candidate_artifacts": {"r1": r1, "r2": r2},
            "quality_rejected": rejected, "quality_rejected_receiver_action": rejected_action,
            "quality_approved": approved,
            "quality_exhausted": exhausted, "receiver_lost_ack": lost,
            "release_receipt": repeated, "opaque_effects_evaluator_only": opaque_effects,
            "logs": logs}
        (base / "service-result.json").write_text(json.dumps(result, indent=2) + "\n")
        if summary_out is not None:
            summary = summarize_service(result)
            summary_out.parent.mkdir(parents=True, exist_ok=True)
            summary_out.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"passed": True, "result": str(base / "service-result.json"),
                          "summary": str(summary_out) if summary_out else None,
                          "distinct_strands_identities": 3,
                          "quality_verdicts": [False, True, False, False, False],
                          "receiver_effect_count": lost["accepted_count"],
                          "release_effect_count": repeated["accepted_effect_count"],
                          "opaque_caller_outcome": "unresolved"}, indent=2))
    finally:
        stop(processes)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path)
    parser.add_argument("--summary-out", type=Path)
    args = parser.parse_args()
    if args.base:
        exercise(args.base, args.summary_out)
    else:
        with tempfile.TemporaryDirectory(prefix="exo-arbitration-common-") as directory:
            exercise(Path(directory), args.summary_out)
