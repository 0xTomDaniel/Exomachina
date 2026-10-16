"""Product-owned remote and acceptance seam used by native Dagu nodes.

The workflow engine dispatches graph steps. This adapter owns caller action IDs,
reconciliation and exact-revision acceptance; the remote Strands services own
their own identities and task receipts. All credentials here are test fixtures.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from client import UncertainSubmission, reconcile, send  # noqa: E402


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def runtime() -> Path:
    path = Path(os.environ["EXO_DAGU_RUNTIME"])
    path.mkdir(parents=True, exist_ok=True)
    return path


def connect() -> sqlite3.Connection:
    db = sqlite3.connect(runtime() / "ledger.sqlite", timeout=20)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            definition_digest TEXT NOT NULL,
            current_revision TEXT,
            current_sha256 TEXT,
            author TEXT,
            artifact_json TEXT,
            accepted_revision TEXT,
            accepted_sha256 TEXT,
            accepted_reviewer TEXT,
            accepted_action_id TEXT,
            release_count INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS assignments (
            action_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            definition_digest TEXT NOT NULL,
            state TEXT NOT NULL,
            task_id TEXT,
            attempts INTEGER NOT NULL DEFAULT 0);
    """)
    return db


def binding(version: str) -> tuple[str, str]:
    manifest = json.loads((runtime() / "home" / "manifests" / f"{version}.json").read_text())
    if manifest["version"] != version:
        raise ValueError("manifest version mismatch")
    files = manifest["files"]
    calculated = hashlib.sha256(canonical(files).encode()).hexdigest()
    if calculated != manifest["closure_sha256"]:
        raise ValueError("closure manifest digest mismatch")
    for filename, expected in files.items():
        actual = hashlib.sha256((runtime() / "home" / "dags" / filename).read_bytes()).hexdigest()
        if actual != expected:
            raise ValueError("published child definition changed: " + filename)
    run_ids = runtime() / "run_ids.json"
    run_id = json.loads(run_ids.read_text())[version] if run_ids.exists() else f"dagu-product-{version}-001"
    return run_id, manifest["closure_sha256"]


def recover_receipt(url: str, action_id: str, timeout: float) -> dict | None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            return reconcile(url, action_id)
        except (OSError, UncertainSubmission, RuntimeError):
            pass
        time.sleep(.15)
    return None


def _ensure_run(db: sqlite3.Connection, run_id: str, definition_digest: str) -> None:
    db.execute("INSERT OR IGNORE INTO runs(run_id, definition_digest) VALUES (?, ?)",
               (run_id, definition_digest))
    row = db.execute("SELECT definition_digest FROM runs WHERE run_id=?", (run_id,)).fetchone()
    if row["definition_digest"] != definition_digest:
        raise ValueError("run definition binding conflict")


def assign(version: str) -> dict:
    run_id, definition_digest = binding(version)
    action_id = run_id + ":capability"
    url = os.environ["EXO_CAPABILITY_URL"]
    with connect() as db:
        db.execute("BEGIN IMMEDIATE")
        _ensure_run(db, run_id, definition_digest)
        existing = db.execute("SELECT * FROM assignments WHERE action_id=?", (action_id,)).fetchone()
        if existing and existing["definition_digest"] != definition_digest:
            raise ValueError("assignment definition conflict")
        if existing is None:
            db.execute("INSERT INTO assignments VALUES (?, ?, ?, 'intent', NULL, 0)",
                       (action_id, run_id, definition_digest))
        run = db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if run["artifact_json"]:
            return json.loads(run["artifact_json"])
    found = recover_receipt(url, action_id, 1)
    if found is None:
        with connect() as db:
            prior = db.execute("SELECT state, attempts FROM assignments WHERE action_id=?", (action_id,)).fetchone()
            if prior["attempts"]:
                raise RuntimeError("remote outcome unknown; no blind resubmission")
            db.execute("UPDATE assignments SET attempts=attempts+1, state='submitted-unknown' WHERE action_id=?",
                       (action_id,))
        command = {"op": "assign", "action_id": action_id, "run_id": run_id,
                   "definition_digest": definition_digest, "brief": f"Dagu {version} research brief",
                   "drop_ack": version == "v1"}
        try:
            found = send(url, command)
        except UncertainSubmission:
            # The remote may already have committed. Poll by stable caller action ID.
            found = recover_receipt(url, action_id, 30)
            if found is None:
                raise RuntimeError("remote outcome unknown; no blind resubmission")
    if found.get("run_id") != run_id or found.get("definition_digest") != definition_digest:
        raise ValueError("remote receipt binding mismatch")
    artifact = found["artifact"]
    if artifact.get("revision") != "r2" or not artifact.get("sha256") or not artifact.get("author"):
        raise ValueError("invalid capability artifact")
    if hashlib.sha256(artifact["content"].encode()).hexdigest() != artifact["sha256"]:
        raise ValueError("artifact content digest mismatch")
    with connect() as db:
        db.execute("BEGIN IMMEDIATE")
        run = db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if run["artifact_json"] and json.loads(run["artifact_json"]) != artifact:
            raise ValueError("conflicting current artifact")
        db.execute("UPDATE runs SET current_revision=?, current_sha256=?, author=?, artifact_json=? WHERE run_id=?",
                   (artifact["revision"], artifact["sha256"], artifact["author"], canonical(artifact), run_id))
        db.execute("UPDATE assignments SET state='observed', task_id=? WHERE action_id=?",
                   (found["task_id"], action_id))
    return artifact


def accept_quality(db: sqlite3.Connection, run_id: str, definition_digest: str,
                   decision: dict, action_id: str) -> None:
    db.execute("BEGIN IMMEDIATE")
    row = db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
    if row is None or not row["artifact_json"]:
        raise ValueError("no current artifact")
    if row["definition_digest"] != definition_digest:
        raise ValueError("acceptance definition binding mismatch")
    if decision.get("accepted") is not True:
        raise ValueError("Quality rejected artifact")
    if decision.get("revision") != row["current_revision"] or decision.get("sha256") != row["current_sha256"]:
        raise ValueError("stale or different artifact revision")
    if not decision.get("reviewer") or decision["reviewer"] == row["author"]:
        raise ValueError("self-review or missing reviewer")
    if row["accepted_action_id"]:
        if row["accepted_action_id"] == action_id and row["accepted_sha256"] == decision["sha256"]:
            return  # retry of the same command, never a second acceptance
        raise ValueError("duplicate acceptance")
    db.execute("UPDATE runs SET accepted_revision=?, accepted_sha256=?, accepted_reviewer=?, accepted_action_id=? WHERE run_id=?",
               (decision["revision"], decision["sha256"], decision["reviewer"], action_id, run_id))


def review(version: str) -> dict:
    run_id, definition_digest = binding(version)
    action_id = run_id + ":quality"
    with connect() as db:
        row = db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None or not row["artifact_json"]:
            raise RuntimeError("no capability artifact")
        if row["definition_digest"] != definition_digest:
            raise RuntimeError("definition digest changed")
        artifact = json.loads(row["artifact_json"])
    command = {"op": "review", "action_id": action_id, "run_id": run_id,
               "definition_digest": definition_digest, "artifact": artifact}
    decision = send(os.environ["EXO_QUALITY_URL"], command)["artifact"]
    with connect() as db:
        accept_quality(db, run_id, definition_digest, decision, action_id)
    if os.environ.get("EXO_DAGU_CRASH_AFTER_ACCEPT_VERSION") == version:
        marker = runtime() / f"fault-after-accept-{version}.marker"
        try:
            with marker.open("xb") as stream:
                stream.write(b"accepted-then-crashed\n")
                stream.flush()
                os.fsync(stream.fileno())
        except FileExistsError:
            pass
        else:
            # Product acceptance is committed, but Dagu has not seen review-step success.
            os._exit(44)
    return decision


def verify(version: str) -> dict:
    run_id, _ = binding(version)
    with connect() as db:
        row = db.execute("SELECT current_revision, current_sha256 FROM runs WHERE run_id=?", (run_id,)).fetchone()
    if row is None or row["current_revision"] != "r2" or not row["current_sha256"]:
        raise RuntimeError("v2 verification lacks current artifact")
    return {"verified_revision": row["current_revision"], "sha256": row["current_sha256"]}


def deliver(version: str) -> dict:
    run_id, definition_digest = binding(version)
    with connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None or row["definition_digest"] != definition_digest:
            raise ValueError("missing run definition binding")
        if not row["accepted_sha256"] or row["accepted_sha256"] != row["current_sha256"] or row["accepted_revision"] != row["current_revision"]:
            raise ValueError("delivery requires exact current accepted artifact")
        db.execute("UPDATE runs SET release_count=1 WHERE run_id=? AND release_count=0", (run_id,))
        result = db.execute("SELECT release_count, accepted_sha256 FROM runs WHERE run_id=?", (run_id,)).fetchone()
    return {"run_id": run_id, "release_count": result["release_count"], "accepted_sha256": result["accepted_sha256"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=["assign", "verify", "review", "deliver"])
    parser.add_argument("--version", choices=["v1", "v2"], required=True)
    args = parser.parse_args()
    try:
        print(canonical(globals()[args.operation](args.version)))
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        raise
