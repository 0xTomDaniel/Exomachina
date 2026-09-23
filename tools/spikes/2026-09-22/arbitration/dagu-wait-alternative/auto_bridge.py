"""Child-final-step, one-shot parent completion for the isolated auto trial."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sqlite3
import time

import bridge


def connect(runtime: Path) -> sqlite3.Connection:
    db = bridge.connect(runtime)
    db.execute("""CREATE TABLE IF NOT EXISTS auto_state (
        parent_id TEXT PRIMARY KEY, authorized_decision TEXT,
        child_outcome TEXT, public_result_json TEXT)""")
    db.execute("INSERT OR IGNORE INTO auto_state(parent_id) VALUES (?)", (bridge.PARENT_ID,))
    db.commit()
    return db


def base(runtime: Path) -> str:
    return (runtime / "base_url").read_text().strip()


def bound(db: sqlite3.Connection, runtime: Path, expected_epoch: int) -> sqlite3.Row:
    row = db.execute("SELECT * FROM bridge WHERE parent_id=?", (bridge.PARENT_ID,)).fetchone()
    if row is None or row["owner_epoch"] != expected_epoch:
        raise ValueError("stale owner")
    if (row["parent_id"], row["parent_name"], row["child_id"], row["child_name"]) != (
            bridge.PARENT_ID, bridge.PARENT_NAME, bridge.CHILD_ID, bridge.CHILD_NAME):
        raise ValueError("wrong-child binding")
    if (row["parent_sha256"] != bridge.digest(runtime, bridge.PARENT_NAME)
            or row["child_sha256"] != bridge.digest(runtime, bridge.CHILD_NAME)):
        raise ValueError("pinned definition changed")
    return row


def accept_child(runtime: Path, expected_epoch: int, token: str, child_id: str) -> dict:
    if token != bridge.FIXTURE_TOKEN:
        raise ValueError("unauthorized Director")
    with connect(runtime) as db:
        db.execute("BEGIN IMMEDIATE")
        row = bound(db, runtime, expected_epoch)
        if child_id != row["child_id"]:
            raise ValueError("wrong-child binding")
        child = bridge.status(base(runtime), row["child_name"], row["child_id"])
        if child["statusLabel"] != "waiting" or bridge.nodes(child).get("director_wait") != "waiting":
            raise ValueError("pinned child Director task is not waiting")
        prior = db.execute("SELECT authorized_decision FROM auto_state WHERE parent_id=?",
                           (bridge.PARENT_ID,)).fetchone()[0]
        if prior not in (None, "accept"):
            raise ValueError("competing Director decision")
        db.execute("UPDATE auto_state SET authorized_decision='accept' WHERE parent_id=?", (bridge.PARENT_ID,))
        response = bridge.request(base(runtime),
            f"/api/v1/dag-runs/{bridge.CHILD_NAME}/{bridge.CHILD_ID}/human-tasks/director_wait/complete",
            {"decision": "accept"})
    return {"child_id": child_id, "decision": "accept", "dagu_response": response}


def require_step(name: str, run_id: str) -> None:
    if os.environ.get("DAG_NAME") != name or os.environ.get("DAG_RUN_ID") != run_id:
        raise ValueError("Dagu step context differs from pinned run")


def record_outcome(runtime: Path) -> dict:
    require_step(bridge.CHILD_NAME, bridge.CHILD_ID)
    with connect(runtime) as db:
        db.execute("BEGIN IMMEDIATE")
        bound(db, runtime, 1)
        child = bridge.status(base(runtime), bridge.CHILD_NAME, bridge.CHILD_ID)
        if bridge.nodes(child).get("director_wait") != "succeeded":
            raise ValueError("child Director task is not complete")
        state = db.execute("SELECT authorized_decision,child_outcome FROM auto_state WHERE parent_id=?",
                           (bridge.PARENT_ID,)).fetchone()
        if state["authorized_decision"] != "accept" or state["child_outcome"] not in (None, "accepted"):
            raise ValueError("missing exact authorized child outcome")
        db.execute("UPDATE auto_state SET child_outcome='accepted' WHERE parent_id=?", (bridge.PARENT_ID,))
    return {"child_id": bridge.CHILD_ID, "child_outcome": "accepted"}


def reconcile(runtime: Path, expected_epoch: int) -> dict:
    require_step(bridge.CHILD_NAME, bridge.CHILD_ID)
    with connect(runtime) as db:
        db.execute("BEGIN IMMEDIATE")
        row = bound(db, runtime, expected_epoch)
        state = db.execute("SELECT * FROM auto_state WHERE parent_id=?", (bridge.PARENT_ID,)).fetchone()
        if state["authorized_decision"] != "accept" or state["child_outcome"] != "accepted":
            raise ValueError("child outcome not authoritative")
        child = bridge.status(base(runtime), row["child_name"], row["child_id"])
        child_nodes = bridge.nodes(child)
        if child_nodes.get("director_wait") != "succeeded" or child_nodes.get("child_outcome") != "succeeded":
            raise ValueError("pinned child outcome step has not completed")
        parent = bridge.status(base(runtime), row["parent_name"], row["parent_id"])
        parent_nodes = bridge.nodes(parent)
        if parent["statusLabel"] == "waiting" and parent_nodes.get("child_gate") == "waiting":
            pause = runtime / "pause_notify"
            if pause.exists():
                (runtime / "bridge_entered").write_text(str(os.getpid()) + "\n")
                while pause.exists():
                    time.sleep(.1)
            response = bridge.request(base(runtime),
                f"/api/v1/dag-runs/{bridge.PARENT_NAME}/{bridge.PARENT_ID}/human-tasks/child_gate/complete",
                {"decision": "child_completed", "child_run_id": bridge.CHILD_ID})
            if (runtime / "lose_ack_once").exists():
                (runtime / "lose_ack_once").unlink()
                raise RuntimeError("synthetic lost bridge continuation after parent gate ack")
            outcome = "completed_now"
        elif parent_nodes.get("child_gate") == "succeeded":
            response = None
            outcome = "already_completed"
        else:
            raise ValueError("parent gate is not current")
        db.execute("UPDATE bridge SET state='completed' WHERE parent_id=?", (bridge.PARENT_ID,))
    return {"outcome": outcome, "parent_id": bridge.PARENT_ID,
            "child_id": bridge.CHILD_ID, "dagu_response": response}


def public_result(runtime: Path) -> dict:
    require_step(bridge.PARENT_NAME, bridge.PARENT_ID)
    with connect(runtime) as db:
        db.execute("BEGIN IMMEDIATE")
        bound(db, runtime, 1)
        state = db.execute("SELECT * FROM auto_state WHERE parent_id=?", (bridge.PARENT_ID,)).fetchone()
        if state["authorized_decision"] != "accept" or state["child_outcome"] != "accepted":
            raise ValueError("parent lacks pinned accepted child result")
        parent = bridge.status(base(runtime), bridge.PARENT_NAME, bridge.PARENT_ID)
        if bridge.nodes(parent).get("child_gate") != "succeeded":
            raise ValueError("parent gate not completed")
        value = {"parent_run_id": bridge.PARENT_ID, "child_run_id": bridge.CHILD_ID,
                 "child_outcome": "accepted"}
        if state["public_result_json"] not in (None, json.dumps(value, sort_keys=True)):
            raise ValueError("public result conflict")
        db.execute("UPDATE auto_state SET public_result_json=? WHERE parent_id=?",
                   (json.dumps(value, sort_keys=True), bridge.PARENT_ID))
    return value


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=["accept-child", "record-outcome", "reconcile", "public-result"])
    parser.add_argument("runtime", type=Path)
    parser.add_argument("--expected-epoch", type=int, default=1)
    parser.add_argument("--token")
    parser.add_argument("--child-id", default=bridge.CHILD_ID)
    args = parser.parse_args()
    if args.operation == "accept-child":
        result = accept_child(args.runtime, args.expected_epoch, args.token, args.child_id)
    elif args.operation == "record-outcome":
        result = record_outcome(args.runtime)
    elif args.operation == "reconcile":
        result = reconcile(args.runtime, args.expected_epoch)
    else:
        result = public_result(args.runtime)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
