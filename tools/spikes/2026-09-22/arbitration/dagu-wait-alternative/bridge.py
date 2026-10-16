"""One-shot product bridge for two separately rooted Dagu runs.

This is an isolated feasibility candidate, not a modification to the frozen
arbitration adapter. A caller invokes reconcile once after the child succeeds.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
import urllib.error
import urllib.request

PARENT_NAME = "exo_wait_parent_v1"
CHILD_NAME = "exo_wait_child_v1"
PARENT_ID = "exo-wait-parent-001"
CHILD_ID = "exo-wait-child-001"
FIXTURE_TOKEN = "fixture-director-token"


def connect(runtime: Path) -> sqlite3.Connection:
    db = sqlite3.connect(runtime / "bridge.sqlite", timeout=10)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("""CREATE TABLE IF NOT EXISTS bridge (
        parent_id TEXT PRIMARY KEY, parent_name TEXT NOT NULL,
        child_id TEXT NOT NULL, child_name TEXT NOT NULL,
        parent_sha256 TEXT NOT NULL, child_sha256 TEXT NOT NULL,
        owner_epoch INTEGER NOT NULL, state TEXT NOT NULL)""")
    return db


def digest(runtime: Path, name: str) -> str:
    return hashlib.sha256((runtime / "home" / "dags" / f"{name}.yaml").read_bytes()).hexdigest()


def request(base: str, path: str, payload: dict | None = None) -> dict:
    data = None if payload is None else json.dumps(payload, sort_keys=True).encode()
    req = urllib.request.Request(base + path, data=data,
                                 headers={"Content-Type": "application/json"},
                                 method="POST" if data is not None else "GET")
    with urllib.request.urlopen(req, timeout=5) as response:
        return json.load(response)


def status(base: str, name: str, run_id: str) -> dict:
    return request(base, f"/api/v1/dag-runs/{name}/{run_id}")["dagRunDetails"]


def nodes(detail: dict) -> dict[str, str]:
    return {node["step"]["id"]: node["statusLabel"] for node in detail["nodes"]}


def register(runtime: Path) -> dict:
    binding = (PARENT_ID, PARENT_NAME, CHILD_ID, CHILD_NAME,
               digest(runtime, PARENT_NAME), digest(runtime, CHILD_NAME), 1, "waiting")
    with connect(runtime) as db:
        db.execute("BEGIN IMMEDIATE")
        prior = db.execute("SELECT * FROM bridge WHERE parent_id=?", (PARENT_ID,)).fetchone()
        if prior is None:
            db.execute("INSERT INTO bridge VALUES (?,?,?,?,?,?,?,?)", binding)
        elif tuple(prior) != binding:
            raise ValueError("pinned bridge binding conflict")
    return {"parent_id": PARENT_ID, "child_id": CHILD_ID, "owner_epoch": 1}


def claim(runtime: Path, expected_epoch: int) -> dict:
    with connect(runtime) as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT owner_epoch,state FROM bridge WHERE parent_id=?", (PARENT_ID,)).fetchone()
        if row is None or row["owner_epoch"] != expected_epoch or row["state"] != "waiting":
            raise ValueError("stale owner or completed bridge")
        epoch = expected_epoch + 1
        db.execute("UPDATE bridge SET owner_epoch=? WHERE parent_id=?", (epoch, PARENT_ID))
    return {"owner_epoch": epoch}


def complete_child(runtime: Path, base: str, expected_epoch: int, token: str) -> dict:
    if token != FIXTURE_TOKEN:
        raise ValueError("unauthorized Director")
    with connect(runtime) as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT * FROM bridge WHERE parent_id=?", (PARENT_ID,)).fetchone()
        if row is None or row["owner_epoch"] != expected_epoch or row["state"] != "waiting":
            raise ValueError("stale owner or completed bridge")
        if row["child_sha256"] != digest(runtime, CHILD_NAME):
            raise ValueError("pinned child definition changed")
        child = status(base, row["child_name"], row["child_id"])
        if child["statusLabel"] != "waiting" or nodes(child).get("director_wait") != "waiting":
            raise ValueError("pinned child Director task is not waiting")
        response = request(base, f"/api/v1/dag-runs/{CHILD_NAME}/{CHILD_ID}/human-tasks/director_wait/complete",
                           {"decision": "accept"})
    return {"decision": "accept", "child_id": CHILD_ID, "owner_epoch": expected_epoch,
            "dagu_response": response}


def reconcile(runtime: Path, base: str, expected_epoch: int) -> dict:
    # Serialize competing bridge commands and hold the fence across Dagu's
    # completion call. A lost HTTP reply is reconciled from native node state.
    with connect(runtime) as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT * FROM bridge WHERE parent_id=?", (PARENT_ID,)).fetchone()
        if row is None or row["owner_epoch"] != expected_epoch:
            raise ValueError("stale owner")
        if (row["parent_id"], row["parent_name"], row["child_id"], row["child_name"]) != (
                PARENT_ID, PARENT_NAME, CHILD_ID, CHILD_NAME):
            raise ValueError("bridge binding mismatch")
        if row["parent_sha256"] != digest(runtime, PARENT_NAME) or row["child_sha256"] != digest(runtime, CHILD_NAME):
            raise ValueError("pinned definition changed")
        child = status(base, row["child_name"], row["child_id"])
        child_nodes = nodes(child)
        if (child["statusLabel"] != "succeeded" or child_nodes.get("director_wait") != "succeeded"
                or child_nodes.get("child_continuation") != "succeeded"):
            raise ValueError("pinned child has not completed")
        parent = status(base, row["parent_name"], row["parent_id"])
        parent_nodes = nodes(parent)
        if parent["statusLabel"] == "waiting" and parent_nodes.get("child_gate") == "waiting":
            response = request(base, f"/api/v1/dag-runs/{PARENT_NAME}/{PARENT_ID}/human-tasks/child_gate/complete",
                               {"decision": "child_completed", "child_run_id": CHILD_ID})
            outcome = "completed_now"
        elif parent_nodes.get("child_gate") == "succeeded":
            response = None
            outcome = "already_completed"
        else:
            raise ValueError("pinned parent gate is not waiting or completed")
        db.execute("UPDATE bridge SET state='completed' WHERE parent_id=?", (PARENT_ID,))
    return {"outcome": outcome, "parent_id": PARENT_ID, "child_id": CHILD_ID,
            "owner_epoch": expected_epoch, "dagu_response": response}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=["register", "claim", "complete-child", "reconcile"])
    parser.add_argument("runtime", type=Path)
    parser.add_argument("--base")
    parser.add_argument("--expected-epoch", type=int)
    parser.add_argument("--token")
    args = parser.parse_args()
    if args.operation == "register":
        result = register(args.runtime)
    elif args.operation == "claim":
        result = claim(args.runtime, args.expected_epoch)
    elif args.operation == "complete-child":
        result = complete_child(args.runtime, args.base, args.expected_epoch, args.token)
    else:
        result = reconcile(args.runtime, args.base, args.expected_epoch)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
