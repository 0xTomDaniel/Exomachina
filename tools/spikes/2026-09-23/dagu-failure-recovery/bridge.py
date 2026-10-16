"""One shared scanner for pinned, separately rooted arbitration children."""
from __future__ import annotations

import json
import os
import time

import ops


def node_status(native: dict) -> dict[str, str]:
    return {item["step"]["id"]: item["statusLabel"] for item in native["nodes"]}


def record(kind: str, **detail) -> None:
    with (ops.runtime() / "bridge-events.jsonl").open("a") as stream:
        stream.write(json.dumps({"kind": kind, **detail}, sort_keys=True) + "\n")


def terminal_authority(db, child: dict, nodes: dict[str, str]) -> bool:
    run_id = child["run_id"]
    verdicts = [dict(row) for row in db.execute(
        "SELECT revision,sha256,reviewer,accepted,state FROM verdicts WHERE run_id=?", (run_id,))]
    if child["state"] == "released":
        revision = child["accepted_revision"]
        return bool(
            revision in {"r1", "r2", "r3"}
            and child["accepted_sha256"] == child["current_sha256"]
            and child["current_revision"] == revision
            and child["acceptance_count"] == child["release_count"] == 1
            and child["receipt_json"]
            and nodes.get("quality_" + revision) == "succeeded"
            and nodes.get("accept_" + revision) == "succeeded"
            and nodes.get("release_" + revision) == "succeeded"
            and any(v["revision"] == revision and v["sha256"] == child["current_sha256"]
                    and v["reviewer"] == child["accepted_reviewer"]
                    and v["accepted"] == 1 and v["state"] == "observed" for v in verdicts))
    if child["state"] == "aborted":
        commands = [dict(row) for row in db.execute(
            "SELECT revision,decision,owner_epoch,state FROM director_commands WHERE run_id=?", (run_id,))]
        return bool(
            child["current_revision"] == "r3" and child["repair_count"] == 2
            and child["acceptance_count"] == child["release_count"] == 0
            and not child["accepted_revision"] and not child["receipt_json"]
            and {(v["revision"], v["accepted"], v["state"]) for v in verdicts}
                == {(revision, 0, "observed") for revision in ("r1", "r2", "r3")}
            and any(c["revision"] == "r3" and c["decision"] == "abort"
                    and c["state"] == "delivered" and c["owner_epoch"] == child["owner_epoch"]
                    for c in commands)
            and nodes.get("director_wait") == "succeeded"
            and nodes.get("abort_after_wait") == "succeeded")
    return False


def exhausted_failure(db, child: dict, native: dict, nodes: dict[str, str]) -> str | None:
    """Only a pre-acceptance native failure after two product-owned retries is terminal."""
    if native["statusLabel"] != "failed" or child["state"] not in {"active", "failed"}:
        return None
    if (child["acceptance_count"] or child["release_count"] or child["accepted_revision"]
        or child["receipt_json"]):
        return None
    failed = [step for step, status in nodes.items() if status == "failed"]
    if len(failed) != 1:
        return None
    row = db.execute("SELECT attempts,completed_at FROM engine_retries WHERE run_id=? AND step_id=?",
                     (child["run_id"], failed[0])).fetchone()
    if row is None or row["attempts"] < 2 or row["completed_at"] is None:
        return None
    if child["state"] == "failed":
        evidence = json.loads(child["failure_json"] or "null")
        if not evidence or evidence.get("failed_step") != failed[0]:
            raise ValueError("sealed child failure evidence mismatch")
    return failed[0]


def scan() -> None:
    with ops.db_connect() as db:
        db.execute("CREATE TABLE IF NOT EXISTS engine_retries (run_id TEXT NOT NULL, step_id TEXT NOT NULL, "
                   "attempts INTEGER NOT NULL, last_at REAL NOT NULL, completed_at REAL, PRIMARY KEY(run_id,step_id))")
        db.execute("CREATE TABLE IF NOT EXISTS gate_resumes (parent_id TEXT PRIMARY KEY, "
                   "attempts INTEGER NOT NULL, last_at REAL NOT NULL)")
        pending = [row[0] for row in db.execute(
            "SELECT p.run_id FROM runs p JOIN runs c ON c.parent_id=p.run_id "
            "WHERE p.bridge_state='waiting' AND (c.state IN ('released','aborted','failed') OR "
            "EXISTS (SELECT 1 FROM engine_retries e WHERE e.run_id=c.run_id AND e.attempts>=2))")]
    for parent_id in pending:
        with ops.db_connect() as db:
            db.execute("BEGIN IMMEDIATE")
            parent_row = db.execute("SELECT * FROM runs WHERE run_id=?", (parent_id,)).fetchone()
            child_row = db.execute("SELECT * FROM runs WHERE parent_id=?", (parent_id,)).fetchone()
            if not parent_row or not child_row or parent_row["bridge_state"] != "waiting":
                continue
            parent, child = dict(parent_row), dict(child_row)
            if (parent["definition_digest"] != child["definition_digest"]
                    or parent["root_name"] != child["root_name"]
                    or parent["dag_name"] != child["root_name"]
                    or child["parent_id"] != parent_id):
                raise ValueError("pinned bridge binding mismatch")
            ops.manifest(parent["root_name"])
            child_native = ops.engine_status(child["dag_name"], child["run_id"])
            if not child_native:
                continue
            child_nodes = node_status(child_native)
            if child["state"] in {"released", "aborted"}:
                if child_native["statusLabel"] != "succeeded":
                    continue
                if not terminal_authority(db, child, child_nodes):
                    raise ValueError("child terminal outcome lacks exact product/native authority")
                failure_step = None
            else:
                failure_step = exhausted_failure(db, child, child_native, child_nodes)
                if failure_step is None:
                    continue
                if child["state"] == "active":
                    failure = {"failed_step": failure_step, "child_run_id": child["run_id"],
                               "definition_digest": child["definition_digest"],
                               "native_status": "failed", "retries_exhausted": 2}
                    db.execute("UPDATE runs SET state='failed',failure_json=? WHERE run_id=? AND state='active'",
                               (ops.canonical(failure), child["run_id"]))
                    ops.event(db, child["run_id"], "native_failure_sealed", failure)
                    child["state"] = "failed"
                    child["failure_json"] = ops.canonical(failure)
            parent_native = ops.engine_status(parent["dag_name"], parent_id)
            if not parent_native:
                continue
            parent_nodes = node_status(parent_native)
            if parent_nodes.get("start_child") != "succeeded":
                raise ValueError("parent child start did not complete")
            value = {"child_run_id": child["run_id"], "state": "failed" if failure_step else child["state"],
                     "accepted_revision": child["accepted_revision"],
                     "accepted_sha256": child["accepted_sha256"],
                     "receipt": json.loads(child["receipt_json"]) if child["receipt_json"] else None}
            if failure_step:
                value["failed_step"] = failure_step
            if parent_nodes.get("child_gate") not in {"waiting", "succeeded"}:
                raise ValueError("parent child gate neither waiting nor completed")
            if parent["public_result_json"]:
                prior = json.loads(parent["public_result_json"])
                if prior["child_run_id"] != child["run_id"] or prior["state"] != value["state"]:
                    raise ValueError("parent public result conflicts with pinned child")
            else:
                db.execute("UPDATE runs SET state=?,public_result_json=? WHERE run_id=?",
                           ("child_failed" if failure_step else "child_finished", ops.canonical(value), parent_id))
        # The public result must be durable before the native gate is released:
        # its downstream Dagu step may start as soon as the HTTP call returns.
        if parent_nodes["child_gate"] == "waiting":
            if (ops.runtime() / "pause-scan-before-gate").exists():
                (ops.runtime() / "scanner-entered").write_text(str(os.getpid()) + "\n")
                while (ops.runtime() / "pause-scan-before-gate").exists():
                    time.sleep(.1)
            ops.http_json(ops.service_url("dagu") +
                f"/api/v1/dag-runs/{parent['dag_name']}/{parent_id}/human-tasks/child_gate/complete",
                {"decision": "child_failed" if failure_step else "child_completed", "child_run_id": child["run_id"]})
            record("parent_gate_posted", parent_id=parent_id, child_id=child["run_id"])
            marker = ops.runtime() / "fault-gate-ack-loss-once"
            if marker.exists():
                marker.unlink()
                raise RuntimeError("injected lost scanner continuation after native gate completion")
        parent_after = ops.engine_status(parent["dag_name"], parent_id)
        after_nodes = node_status(parent_after) if parent_after else {}
        if (parent_after and parent_after["statusLabel"] == "waiting"
            and after_nodes.get("child_gate") == "succeeded"):
            with ops.db_connect() as db:
                db.execute("BEGIN IMMEDIATE")
                prior = db.execute("SELECT attempts,last_at FROM gate_resumes WHERE parent_id=?",
                                   (parent_id,)).fetchone()
                if prior and (prior["attempts"] >= 3 or time.time() - prior["last_at"] < 1):
                    may_resume = False
                else:
                    attempts = prior["attempts"] + 1 if prior else 1
                    db.execute("INSERT OR REPLACE INTO gate_resumes VALUES (?,?,?)",
                               (parent_id, attempts, time.time()))
                    may_resume = True
            if may_resume:
                ops.http_json(ops.service_url("dagu") +
                    f"/api/v1/dag-runs/{parent['dag_name']}/{parent_id}/human-tasks/resume", {})
                record("parent_resume_posted", parent_id=parent_id, child_id=child["run_id"])
            continue
        expected_native = "failed" if failure_step else "succeeded"
        if not parent_after or parent_after["statusLabel"] != expected_native:
            continue
        with ops.db_connect() as db:
            db.execute("BEGIN IMMEDIATE")
            latest = db.execute("SELECT bridge_state FROM runs WHERE run_id=?", (parent_id,)).fetchone()
            if latest and latest["bridge_state"] == "waiting":
                outcome = "failed" if failure_step else "completed"
                db.execute("UPDATE runs SET bridge_state=? WHERE run_id=?", (outcome, parent_id))
                ops.event(db, parent_id, "bridge_" + outcome, {"child_run_id": child["run_id"],
                    "state": "failed" if failure_step else child["state"], "failed_step": failure_step})
                record("bridge_" + outcome, parent_id=parent_id, child_id=child["run_id"],
                       failed_step=failure_step)
