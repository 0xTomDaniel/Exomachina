"""Process-level Graph claim/fence and synthetic remote delivery countertrial.

The remote ledger is a deliberately synthetic service seam, not A2A evidence.
All databases and rendezvous files are confined to the --root runtime path.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

from file_session_probe import runner


def database(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=10)
    connection.row_factory = sqlite3.Row
    return connection


def claim(root: Path, run_id: str, owner: str, lease_seconds: float) -> int | None:
    with database(root / "local.sqlite") as db:
        db.execute("CREATE TABLE IF NOT EXISTS claims (run_id TEXT PRIMARY KEY, epoch INTEGER NOT NULL, owner TEXT NOT NULL, lease_until REAL NOT NULL, completed INTEGER NOT NULL DEFAULT 0)")
        db.execute("CREATE TABLE IF NOT EXISTS effects (effect_key TEXT PRIMARY KEY, remote_id TEXT, state TEXT NOT NULL)")
        db.execute("BEGIN IMMEDIATE")
        previous = db.execute("SELECT epoch, lease_until, completed FROM claims WHERE run_id=?", (run_id,)).fetchone()
        now = time.time()
        if previous is not None and (previous["completed"] or previous["lease_until"] > now):
            return None
        epoch = 1 if previous is None else previous["epoch"] + 1
        db.execute(
            "INSERT INTO claims (run_id,epoch,owner,lease_until) VALUES (?,?,?,?) "
            "ON CONFLICT(run_id) DO UPDATE SET epoch=excluded.epoch, owner=excluded.owner, lease_until=excluded.lease_until",
            (run_id, epoch, owner, now + lease_seconds),
        )
        return epoch


def effect_key(root: Path, run_id: str) -> str:
    binding = json.loads((root / f"{run_id}.json").read_text())
    return hashlib.sha256(f"{run_id}:deliver:{binding['digest']}".encode()).hexdigest()


def assert_fence_and_mark_pending(root: Path, run_id: str, owner: str, epoch: int, key: str) -> None:
    with database(root / "local.sqlite") as db:
        db.execute("BEGIN IMMEDIATE")
        current = db.execute("SELECT * FROM claims WHERE run_id=?", (run_id,)).fetchone()
        if current is None or current["epoch"] != epoch or current["owner"] != owner or current["lease_until"] <= time.time():
            raise RuntimeError(f"stale run claim: {owner} epoch {epoch}")
        db.execute("INSERT OR IGNORE INTO effects (effect_key,state) VALUES (?,'pending')", (key,))


def remote_lookup(root: Path, key: str) -> str | None:
    with database(root / "remote.sqlite") as db:
        db.execute("CREATE TABLE IF NOT EXISTS accepted (effect_key TEXT PRIMARY KEY, remote_id TEXT NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS attempts (effect_key TEXT NOT NULL, owner TEXT NOT NULL)")
        row = db.execute("SELECT remote_id FROM accepted WHERE effect_key=?", (key,)).fetchone()
        return None if row is None else str(row["remote_id"])


def remote_accept(root: Path, key: str, owner: str) -> str:
    """Synthetic remote: stable key suppresses duplicate accepted deliveries."""
    with database(root / "remote.sqlite") as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute("INSERT INTO attempts VALUES (?,?)", (key, owner))
        db.execute("INSERT OR IGNORE INTO accepted VALUES (?,?)", (key, key[:16]))
        row = db.execute("SELECT remote_id FROM accepted WHERE effect_key=?", (key,)).fetchone()
        assert row is not None
        return str(row["remote_id"])


def deliver(root: Path, run_id: str) -> None:
    owner = os.environ["EXO_OWNER_ID"]
    epoch = int(os.environ["EXO_CLAIM_EPOCH"])
    key = effect_key(root, run_id)
    assert_fence_and_mark_pending(root, run_id, owner, epoch, key)
    remote_id = remote_lookup(root, key)
    if remote_id is None:
        if os.environ.get("EXO_HOLD_BEFORE_REMOTE_ACCEPT") == "1":
            (root / "pre-submit.sentinel").touch()
            while not (root / "release-submit.sentinel").exists():
                time.sleep(0.02)
        remote_id = remote_accept(root, key, owner)
        if os.environ.get("EXO_HOLD_AFTER_REMOTE_ACCEPT") == "1":
            (root / "remote-accepted.sentinel").touch()
            while True:
                time.sleep(0.1)
    with database(root / "local.sqlite") as db:
        db.execute("UPDATE effects SET state='accepted', remote_id=? WHERE effect_key=?", (remote_id, key))


class DeliveryModel(runner.FixtureModel):
    async def stream(self, messages, tool_specs=None, system_prompt=None, **kwargs):
        if self.marker == "deliver":
            deliver(self.events.parent, self.events.name.removesuffix(".events.jsonl"))
        async for event in super().stream(messages, tool_specs, system_prompt, **kwargs):
            yield event


runner.FixtureModel = DeliveryModel


def finish(root: Path, run_id: str, owner: str, epoch: int) -> None:
    with database(root / "local.sqlite") as db:
        db.execute("BEGIN IMMEDIATE")
        changed = db.execute(
            "UPDATE claims SET completed=1 WHERE run_id=? AND owner=? AND epoch=? AND lease_until>?",
            (run_id, owner, epoch, time.time()),
        ).rowcount
        if changed != 1:
            raise RuntimeError("run claim expired before completion")


def resume(root: Path, run_id: str, owner: str, lease_seconds: float, hold_after_remote: bool) -> dict:
    epoch = claim(root, run_id, owner, lease_seconds)
    if epoch is None:
        return {"owner": owner, "claim": "rejected"}
    os.environ["EXO_OWNER_ID"] = owner
    os.environ["EXO_CLAIM_EPOCH"] = str(epoch)
    if hold_after_remote:
        os.environ["EXO_HOLD_AFTER_REMOTE_ACCEPT"] = "1"
    try:
        result = runner.resume(root, run_id, "approve")
        if result["status"] == "completed":
            finish(root, run_id, owner, epoch)
        return {"owner": owner, "epoch": epoch, "graph": result}
    except Exception as error:
        return {"owner": owner, "epoch": epoch, "error": f"{type(error).__name__}: {error}"}


def invoke(*args: str) -> dict:
    done = subprocess.run([sys.executable, __file__, *args], text=True, capture_output=True, timeout=30)
    if done.returncode != 0:
        raise RuntimeError(f"child exited {done.returncode}: {done.stderr[-2000:]}")
    return json.loads(done.stdout)


def wait_for(path: Path, children: list[subprocess.Popen]) -> None:
    deadline = time.monotonic() + 20
    while not path.exists():
        for child in children:
            if child.poll() is not None:
                raise RuntimeError(f"child exited before {path.name}: {child.stderr.read()[-1000:]}")
        if time.monotonic() > deadline:
            raise TimeoutError(path)
        time.sleep(0.02)


def launch(root: Path, owner: str, lease: float, gate: Path | None = None, hold_after_remote: bool = False, hold_before_remote: bool = False) -> subprocess.Popen:
    env = os.environ.copy()
    if gate is not None:
        env["EXO_RENDEZVOUS_DIR"] = str(gate)
        env["EXO_OWNER_ID"] = owner
    if hold_before_remote:
        env["EXO_HOLD_BEFORE_REMOTE_ACCEPT"] = "1"
    command = [sys.executable, __file__, "resume", "--root", str(root), "--run-id", "run-v1", "--owner", owner, "--lease", str(lease)]
    if hold_after_remote:
        command.append("--hold-after-remote")
    return subprocess.Popen(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)


def remote_observation(root: Path) -> dict:
    with database(root / "remote.sqlite") as db:
        accepted = db.execute("SELECT effect_key, remote_id FROM accepted").fetchall()
        attempts = db.execute("SELECT owner FROM attempts").fetchall()
    with database(root / "local.sqlite") as db:
        effects = db.execute("SELECT effect_key, remote_id, state FROM effects").fetchall()
        claim_row = db.execute("SELECT run_id, epoch, owner, completed FROM claims WHERE run_id='run-v1'").fetchone()
    return {
        "accepted_count": len(accepted),
        "submit_attempt_owners": [row["owner"] for row in attempts],
        "local_effects": [{"state": row["state"], "remote_id": row["remote_id"]} for row in effects],
        "claim": dict(claim_row) if claim_row else None,
    }


def exercise(root: Path) -> dict:
    root.mkdir(parents=True, exist_ok=True)
    stale_root = root / "stale-owner"
    overlap_root = root / "in-flight-overlap"
    lost_root = root / "lost-ack"
    setup_stale = invoke("scenario", "--root", str(stale_root))
    assert setup_stale["v1"]["status"] == setup_stale["v2"]["status"] == "interrupted"
    gate = stale_root / "gate"
    gate.mkdir()
    children: list[subprocess.Popen] = []
    try:
        a = launch(stale_root, "a", 1.5, gate)
        children.append(a)
        wait_for(gate / "ready-a", children)
        rejected_while_leased = invoke("resume", "--root", str(stale_root), "--run-id", "run-v1", "--owner", "early", "--lease", "5")
        assert rejected_while_leased["claim"] == "rejected"
        time.sleep(1.6)
        b = launch(stale_root, "b", 5, gate)
        children.append(b)
        wait_for(gate / "ready-b", children)
        (gate / "release-b").touch()
        b_out, b_err = b.communicate(timeout=20)
        assert b.returncode == 0, b_err
        (gate / "release-a").touch()
        a_out, a_err = a.communicate(timeout=20)
        assert a.returncode == 0, a_err
        a_result, b_result = json.loads(a_out), json.loads(b_out)
        stale_observation = remote_observation(stale_root)
        assert b_result["graph"]["status"] == "completed", b_result
        assert a_result.get("graph", {}).get("status") != "completed", a_result
        assert stale_observation["accepted_count"] == 1, stale_observation
        assert stale_observation["submit_attempt_owners"] == ["b"], stale_observation
        rejected_after_completion = invoke("resume", "--root", str(stale_root), "--run-id", "run-v1", "--owner", "c", "--lease", "5")
        assert rejected_after_completion["claim"] == "rejected"
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=10)

    setup_overlap = invoke("scenario", "--root", str(overlap_root))
    assert setup_overlap["v1"]["status"] == "interrupted"
    first = launch(overlap_root, "first", 1.5, hold_before_remote=True)
    overlap_children = [first]
    try:
        wait_for(overlap_root / "pre-submit.sentinel", overlap_children)
        time.sleep(1.6)
        second = launch(overlap_root, "second", 5)
        overlap_children.append(second)
        second_out, second_err = second.communicate(timeout=20)
        assert second.returncode == 0, second_err
        assert json.loads(second_out)["graph"]["status"] == "completed"
        (overlap_root / "release-submit.sentinel").touch()
        first_out, first_err = first.communicate(timeout=20)
        assert first.returncode == 0, first_err
        first_result = json.loads(first_out)
        overlap_observation = remote_observation(overlap_root)
        assert "expired before completion" in first_result["error"]
        assert overlap_observation["accepted_count"] == 1
        assert overlap_observation["submit_attempt_owners"] == ["second", "first"]
    finally:
        for child in overlap_children:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=10)

    setup_lost = invoke("scenario", "--root", str(lost_root))
    assert setup_lost["v1"]["status"] == "interrupted"
    victim = launch(lost_root, "first", 1.5, hold_after_remote=True)
    try:
        wait_for(lost_root / "remote-accepted.sentinel", [victim])
        before_kill = remote_observation(lost_root)
        assert before_kill["accepted_count"] == 1
        assert before_kill["local_effects"][0]["state"] == "pending"
        victim.kill()
        assert victim.wait(timeout=10) < 0
        time.sleep(1.6)
        recovered = invoke("resume", "--root", str(lost_root), "--run-id", "run-v1", "--owner", "second", "--lease", "5")
        after_recovery = remote_observation(lost_root)
        assert recovered["graph"]["status"] == "completed", recovered
        assert after_recovery["accepted_count"] == 1
        assert after_recovery["submit_attempt_owners"] == ["first"]
        assert after_recovery["local_effects"][0]["state"] == "accepted"
    finally:
        if victim.poll() is None:
            victim.kill()
        victim.wait(timeout=10)
    return {
        "sdk": "strands-agents==1.57.0",
        "session_manager": "FileSessionManager",
        "stale_owner": {"owner_a": a_result, "owner_b": b_result, "remote": stale_observation, "claim_rejected_during_valid_lease": rejected_while_leased, "post_completion_claim": rejected_after_completion},
        "in_flight_overlap": {"stale_completion": first_result, "remote": overlap_observation},
        "lost_ack": {"sigkill_after_remote_accept": True, "before_kill": before_kill, "recovered": recovered, "after_recovery": after_recovery},
        "synthetic_remote_only": True,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["scenario", "resume", "exercise"])
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--run-id")
    parser.add_argument("--owner")
    parser.add_argument("--lease", type=float, default=5)
    parser.add_argument("--hold-after-remote", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.action == "scenario":
        runner.scenario(args.root, hold=False)
    elif args.action == "resume":
        if args.run_id is None or args.owner is None:
            parser.error("resume requires --run-id and --owner")
        print(json.dumps(resume(args.root, args.run_id, args.owner, args.lease, args.hold_after_remote), sort_keys=True))
    else:
        result = exercise(args.root)
        if args.output is not None:
            args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"status": "passed", "stale_owner": result["stale_owner"]["remote"], "lost_ack": result["lost_ack"]["after_recovery"]}, sort_keys=True))


if __name__ == "__main__":
    main()
