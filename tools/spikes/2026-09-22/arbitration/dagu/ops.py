"""Frozen product seam for native Dagu steps and uncertain A2A effects.

Dagu schedules steps and persists waits. This module owns the domain evidence
that Dagu cannot infer: action identity, typed artifacts, verified Quality,
current-revision acceptance, receiver receipts, and nested-root correlation.
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
import urllib.error
import urllib.parse
import urllib.request

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "common"))
sys.path.insert(0, str(HERE.parents[1] / "decision-round" / "common"))
from fixture import assignment, candidate_artifact, canonical, sha256_text, typed_join  # noqa: E402
from client import UncertainSubmission, reconcile, send  # noqa: E402


def runtime() -> Path:
    return Path(os.environ["EXO_ARB_RUNTIME"])


def config() -> dict:
    return json.loads((runtime() / "config.json").read_text())


def db_connect() -> sqlite3.Connection:
    path = runtime() / "ledger.sqlite"
    db = sqlite3.connect(path, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS runs (
          run_id TEXT PRIMARY KEY, dag_name TEXT NOT NULL, root_name TEXT NOT NULL,
          definition_digest TEXT NOT NULL, parent_id TEXT,
          resolve_input INTEGER NOT NULL DEFAULT 0, release_mode TEXT NOT NULL DEFAULT 'participating',
          state TEXT NOT NULL DEFAULT 'active', owner_epoch INTEGER NOT NULL DEFAULT 1,
          join_json TEXT, current_revision TEXT, current_sha256 TEXT, current_artifact_json TEXT,
          repair_count INTEGER NOT NULL DEFAULT 0,
          accepted_revision TEXT, accepted_sha256 TEXT, accepted_reviewer TEXT,
          acceptance_count INTEGER NOT NULL DEFAULT 0,
          receipt_json TEXT, release_count INTEGER NOT NULL DEFAULT 0,
          public_result_json TEXT);
        CREATE TABLE IF NOT EXISTS assignments (
          action_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, instance TEXT NOT NULL,
          result_type TEXT NOT NULL, scope_status TEXT,
          definition_digest TEXT NOT NULL, service_identity TEXT NOT NULL,
          state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
          task_id TEXT, receipt_json TEXT);
        CREATE TABLE IF NOT EXISTS candidates (
          run_id TEXT NOT NULL, revision TEXT NOT NULL, sha256 TEXT NOT NULL,
          artifact_json TEXT NOT NULL, PRIMARY KEY(run_id, revision));
        CREATE TABLE IF NOT EXISTS joins (
          run_id TEXT NOT NULL, step_id TEXT NOT NULL, join_json TEXT NOT NULL,
          PRIMARY KEY(run_id, step_id));
        CREATE TABLE IF NOT EXISTS verdicts (
          action_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, revision TEXT NOT NULL,
          sha256 TEXT NOT NULL, reviewer TEXT NOT NULL, accepted INTEGER NOT NULL,
          state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
          task_id TEXT, verdict_json TEXT);
        CREATE TABLE IF NOT EXISTS releases (
          release_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, state TEXT NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0, receipt_json TEXT);
        CREATE TABLE IF NOT EXISTS director_commands (
          key TEXT PRIMARY KEY, run_id TEXT NOT NULL, revision TEXT NOT NULL,
          decision TEXT NOT NULL, owner_epoch INTEGER NOT NULL, state TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS events (
          id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL,
          kind TEXT NOT NULL, detail_json TEXT NOT NULL);
    """)
    return db


def event(db: sqlite3.Connection, run_id: str, kind: str, detail: dict) -> None:
    db.execute("INSERT INTO events(run_id,kind,detail_json) VALUES (?,?,?)",
               (run_id, kind, canonical(detail)))


def manifest(root_name: str) -> dict:
    path = runtime() / "home" / "manifests" / f"{root_name}.json"
    if not path.is_file():
        raise ValueError("unpublished root")
    data = json.loads(path.read_text())
    expected = hashlib.sha256(canonical(data["files"]).encode()).hexdigest()
    if data.get("root") != root_name or data.get("closure_sha256") != expected:
        raise ValueError("manifest closure binding mismatch")
    for name, digest in data["files"].items():
        raw = (runtime() / "home" / "dags" / name).read_bytes()
        if hashlib.sha256(raw).hexdigest() != digest:
            raise ValueError("active closure mutation: " + name)
    return data


def register_run(run_id: str, dag_name: str, root_name: str, *, parent_id: str | None = None,
                 resolve_input: bool = False, release_mode: str = "participating") -> dict:
    published = manifest(root_name)
    if f"{dag_name}.yaml" not in published["files"]:
        raise ValueError("run DAG outside pinned closure")
    if release_mode not in {"participating", "opaque"}:
        raise ValueError("invalid receiver mode")
    with db_connect() as db:
        db.execute("BEGIN IMMEDIATE")
        prior = db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if prior:
            if (prior["dag_name"], prior["root_name"], prior["definition_digest"], prior["parent_id"],
                prior["resolve_input"], prior["release_mode"]) != (dag_name, root_name, published["closure_sha256"],
                                                                     parent_id, int(resolve_input), release_mode):
                raise ValueError("run binding conflict")
            return dict(prior)
        db.execute("INSERT INTO runs(run_id,dag_name,root_name,definition_digest,parent_id,resolve_input,release_mode) "
                   "VALUES (?,?,?,?,?,?,?)", (run_id, dag_name, root_name, published["closure_sha256"],
                                                parent_id, int(resolve_input), release_mode))
        event(db, run_id, "run_registered", {"dag_name": dag_name, "root_name": root_name,
                                               "definition_digest": published["closure_sha256"],
                                               "parent_id": parent_id})
    return get_run(run_id)


def get_run(run_id: str | None = None) -> dict:
    run_id = run_id or os.environ.get("DAG_RUN_ID")
    if not run_id:
        raise ValueError("DAG_RUN_ID required")
    with db_connect() as db:
        row = db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
    if row is None:
        raise ValueError("unregistered run")
    run = dict(row)
    if (os.environ.get("DAG_NAME") and os.environ.get("DAG_RUN_ID") == run_id
        and os.environ["DAG_NAME"] != run["dag_name"]):
        raise ValueError("Dagu name differs from registered run")
    if manifest(run["root_name"])["closure_sha256"] != run["definition_digest"]:
        raise ValueError("run definition binding changed")
    return run


def http_json(url: str, payload: dict | None = None, *, token: bool = False,
              timeout: float = 8) -> dict:
    data = None if payload is None else canonical(payload).encode()
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer fixture-token"
    request = urllib.request.Request(url, data=data, headers=headers,
                                     method="POST" if data is not None else "GET")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def expected_identity(role: str) -> str:
    value = config()["identities"][role]
    if not isinstance(value, str) or not value:
        raise ValueError("missing pinned service identity")
    return value


def service_url(role: str) -> str:
    return config()["urls"][role]


def lookup(url: str, action_id: str, run: dict) -> dict | None:
    try:
        return reconcile(url, action_id, run["run_id"], run["definition_digest"])
    except RuntimeError as error:
        if "HTTP 404:" in str(error):
            return None
        raise
    except (OSError, UncertainSubmission):
        return None


def wait_lookup(url: str, action_id: str, run: dict, table: str) -> dict:
    while True:
        found = lookup(url, action_id, run)
        if found is not None:
            return found
        with db_connect() as db:
            db.execute(f"UPDATE {table} SET state='unresolved' WHERE action_id=?", (action_id,))
        time.sleep(.25)


def participating_recovery(url: str, action_id: str, run: dict, table: str,
                           command: dict, identity: str) -> dict:
    """Lookup first, then one same-ID resend only under pinned dedup service."""
    started = time.monotonic()
    while True:
        found = lookup(url, action_id, run)
        if found is not None:
            return found
        with db_connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(f"UPDATE {table} SET state='unresolved' WHERE action_id=?", (action_id,))
            row = db.execute(f"SELECT attempts FROM {table} WHERE action_id=?", (action_id,)).fetchone()
        if row["attempts"] < 2 and time.monotonic() - started >= 1:
            try:
                service = http_json(url + "/health")
            except OSError:
                service = None
            if service and service.get("identity") == identity:
                with db_connect() as db:
                    db.execute("BEGIN IMMEDIATE")
                    current = db.execute(f"SELECT attempts FROM {table} WHERE action_id=?", (action_id,)).fetchone()
                    if current["attempts"] < 2:
                        db.execute(f"UPDATE {table} SET attempts=attempts+1,state='submitted_unknown' WHERE action_id=?",
                                   (action_id,))
                        event(db, run["run_id"], "same_id_resubmit", {"action_id": action_id,
                                                                          "service_identity": identity})
                        may_send = True
                    else:
                        may_send = False
                if may_send:
                    try:
                        return send(url, {**command, "drop_ack": False})
                    except UncertainSubmission:
                        pass
        time.sleep(.25)


def assign(instance: str, result_type: str, scope: str = "requires_scope",
           drop_ack: str = "false") -> dict:
    run = get_run()
    if result_type not in {"source_evidence", "counter_evidence"}:
        raise ValueError("unapproved capability")
    if result_type == "source_evidence" and scope != "requires_scope":
        raise ValueError("source branch cannot set counter scope")
    if scope not in {"requires_scope", "clear"}:
        raise ValueError("invalid counter scope")
    role = result_type
    action_id = f"{run['run_id']}:{instance}"
    url = service_url(role)
    identity = expected_identity(role)
    with db_connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT * FROM assignments WHERE action_id=?", (action_id,)).fetchone()
        if row is None:
            db.execute("INSERT INTO assignments(action_id,run_id,instance,result_type,scope_status,definition_digest,service_identity,state) "
                       "VALUES (?,?,?,?,?,?,?,'intent')",
                       (action_id, run["run_id"], instance, result_type, scope if result_type == "counter_evidence" else None,
                        run["definition_digest"], identity))
            row = db.execute("SELECT * FROM assignments WHERE action_id=?", (action_id,)).fetchone()
        if (row["run_id"], row["instance"], row["result_type"], row["definition_digest"], row["service_identity"]) != (
            run["run_id"], instance, result_type, run["definition_digest"], identity):
            raise ValueError("assignment binding conflict")
        if row["receipt_json"]:
            return json.loads(row["receipt_json"])
        may_send = row["attempts"] == 0
        if may_send:
            db.execute("UPDATE assignments SET attempts=1,state='submitted_unknown' WHERE action_id=?", (action_id,))
            event(db, run["run_id"], "assignment_intent", {"action_id": action_id, "type": result_type})
    command = assignment(run["run_id"], run["definition_digest"], instance,
                         result_type=result_type,
                         scope_status=scope if result_type == "counter_evidence" else None,
                         drop_ack=drop_ack == "true")
    found = lookup(url, action_id, run)
    if found is None and may_send:
        try:
            found = send(url, command)
        except UncertainSubmission:
            pass
    if found is None:
        found = participating_recovery(url, action_id, run, "assignments", command, identity)
    pause = runtime() / f"pause-assignment-{instance}"
    entered = runtime() / f"entered-assignment-{run['run_id']}-{instance}"
    if pause.is_file():
        entered.write_text("remote observation held before local record\n")
        while pause.is_file():
            time.sleep(.1)
    from fixture import branch_value  # noqa: E402
    branch_value(found, instance, run_id=run["run_id"],
                 definition_digest=run["definition_digest"], result_type=result_type,
                 scope_status=scope if result_type == "counter_evidence" else None)
    artifact = found["artifact"]
    if artifact["author"] != identity:
        raise ValueError("capability author differs from pinned service identity")
    if found.get("harness_identity") not in {None, identity}:
        raise ValueError("A2A service identity mismatch")
    with db_connect() as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute("UPDATE assignments SET state='observed',task_id=?,receipt_json=? WHERE action_id=?",
                   (found.get("task_id"), canonical(found), action_id))
        event(db, run["run_id"], "assignment_observed", {"action_id": action_id,
                     "task_id": found.get("task_id"), "artifact_sha256": artifact["sha256"]})
    return found


def join(branches: str) -> dict:
    run = get_run()
    names = branches.split(",")
    if len(names) != len(set(names)):
        raise ValueError("duplicate join branch")
    with db_connect() as db:
        rows = db.execute("SELECT * FROM assignments WHERE run_id=?", (run["run_id"],)).fetchall()
    by_instance = {row["instance"]: row for row in rows}
    if not set(names) <= set(by_instance) or any(not by_instance[name]["receipt_json"] for name in names):
        raise ValueError("join requires all declared branch receipts")
    receipts = {name: json.loads(by_instance[name]["receipt_json"]) for name in names}
    declarations = {name: by_instance[name]["result_type"] for name in names}
    scopes = {name: by_instance[name]["scope_status"] for name in names if by_instance[name]["scope_status"]}
    value = typed_join(receipts, run_id=run["run_id"],
                       definition_digest=run["definition_digest"],
                       declarations=declarations, scope_status_by_instance=scopes)
    step_id = os.environ.get("DAG_RUN_STEP_NAME", "manual")
    with db_connect() as db:
        db.execute("BEGIN IMMEDIATE")
        existing = db.execute("SELECT join_json FROM joins WHERE run_id=? AND step_id=?",
                              (run["run_id"], step_id)).fetchone()
        if existing and json.loads(existing["join_json"]) != value:
            raise ValueError("immutable typed join step conflict")
        row = db.execute("SELECT join_json,current_revision FROM runs WHERE run_id=?", (run["run_id"],)).fetchone()
        if row["current_revision"] and row["join_json"] and json.loads(row["join_json"]) != value:
            raise ValueError("cannot change join after candidate synthesis")
        if row["join_json"] and json.loads(row["join_json"]) != value:
            previous = json.loads(row["join_json"])["branch_artifact_sha256"]
            current = value["branch_artifact_sha256"]
            if not set(previous) < set(current) or any(current[key] != digest for key, digest in previous.items()):
                raise ValueError("later join must extend validated branch evidence")
        db.execute("INSERT OR IGNORE INTO joins VALUES (?,?,?)",
                   (run["run_id"], step_id, canonical(value)))
        db.execute("UPDATE runs SET join_json=? WHERE run_id=?", (canonical(value), run["run_id"]))
        event(db, run["run_id"], "typed_join", {"step_id": step_id, "branches": names,
                                                  "route_status": value["route_status"]})
    return value


def _new_candidate(run: dict, revision: str, resolved: bool, *, from_revision: str | None) -> dict:
    if revision not in {"r1", "r2", "r3"}:
        raise ValueError("invalid candidate revision")
    with db_connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT * FROM runs WHERE run_id=?", (run["run_id"],)).fetchone()
        if not row["join_json"]:
            raise ValueError("candidate requires typed join")
        artifact = candidate_artifact(json.loads(row["join_json"]), revision,
                                      "exo-factory-synthesis", resolved=resolved)
        existing = db.execute("SELECT * FROM candidates WHERE run_id=? AND revision=?",
                              (run["run_id"], revision)).fetchone()
        if existing and json.loads(existing["artifact_json"]) != artifact:
            raise ValueError("immutable candidate revision conflict")
        if row["current_revision"] == revision and existing:
            return artifact  # same step replay after committed repair
        if from_revision is None:
            if revision != "r1" or row["current_revision"] not in {None, "r1"}:
                raise ValueError("first synthesis must create r1")
        else:
            if (row["current_revision"] != from_revision or row["repair_count"] >= 2
                or row["accepted_revision"]):
                raise ValueError("repair bound or current revision conflict")
            prior = db.execute("SELECT * FROM verdicts WHERE run_id=? AND revision=? AND state='observed'",
                               (run["run_id"], from_revision)).fetchone()
            if prior is None or prior["accepted"]:
                raise ValueError("repair requires verified rejection")
        db.execute("INSERT OR IGNORE INTO candidates VALUES (?,?,?,?)",
                   (run["run_id"], revision, artifact["sha256"], canonical(artifact)))
        if row["current_revision"] != revision:
            db.execute("UPDATE runs SET current_revision=?,current_sha256=?,current_artifact_json=?,repair_count=repair_count+? "
                       "WHERE run_id=?", (revision, artifact["sha256"], canonical(artifact),
                                          int(from_revision is not None), run["run_id"]))
            event(db, run["run_id"], "candidate_current", {"revision": revision,
                                                              "sha256": artifact["sha256"]})
    return artifact


def _resolved(value: str, run: dict) -> bool:
    if value == "input":
        return bool(run["resolve_input"])
    if value in {"true", "false"}:
        return value == "true"
    raise ValueError("invalid resolution value")


def synthesize(revision: str, resolved: str) -> dict:
    run = get_run()
    return _new_candidate(run, revision, _resolved(resolved, run), from_revision=None)


def repair(from_revision: str, to: str, resolved: str) -> dict:
    run = get_run()
    if from_revision not in {"r1", "r2"} or to != f"r{int(from_revision[1:])+1}":
        raise ValueError("invalid repair sequence")
    return _new_candidate(run, to, _resolved(resolved, run), from_revision=from_revision)


def _record_quality(run: dict, revision: str, found: dict, identity: str) -> dict:
    action_id = f"{run['run_id']}:quality:{revision}"
    if (found.get("action_id") != action_id or found.get("run_id") != run["run_id"]
        or found.get("definition_digest") != run["definition_digest"]):
        raise ValueError("Quality A2A binding mismatch")
    if found.get("harness_identity") not in {None, identity}:
        raise ValueError("Quality A2A service identity mismatch")
    decision = found.get("artifact")
    if not isinstance(decision, dict) or not isinstance(decision.get("accepted"), bool):
        raise ValueError("invalid Quality verdict")
    with db_connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT * FROM runs WHERE run_id=?", (run["run_id"],)).fetchone()
        candidate = db.execute("SELECT * FROM candidates WHERE run_id=? AND revision=?",
                               (run["run_id"], revision)).fetchone()
        if (candidate is None or row["current_revision"] != revision
            or row["current_sha256"] != candidate["sha256"]
            or decision.get("revision") != revision
            or decision.get("sha256") != candidate["sha256"]):
            raise ValueError("stale Quality revision or digest")
        if decision.get("reviewer") != identity or identity == json.loads(candidate["artifact_json"])["author"]:
            raise ValueError("Quality reviewer is not verified independent service")
        prior = db.execute("SELECT * FROM verdicts WHERE action_id=?", (action_id,)).fetchone()
        if prior and prior["state"] == "observed" and json.loads(prior["verdict_json"]) != decision:
            raise ValueError("conflicting Quality verdict")
        db.execute("UPDATE verdicts SET state='observed',task_id=?,sha256=?,reviewer=?,accepted=?,verdict_json=? "
                   "WHERE action_id=?", (found.get("task_id"), decision["sha256"], identity,
                                          int(decision["accepted"]), canonical(decision), action_id))
        event(db, run["run_id"], "quality_verdict", {"revision": revision,
              "accepted": decision["accepted"], "sha256": decision["sha256"], "reviewer": identity})
    return decision


def quality(revision: str) -> str:
    run = get_run()
    url = service_url("quality")
    identity = expected_identity("quality")
    action_id = f"{run['run_id']}:quality:{revision}"
    with db_connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT * FROM runs WHERE run_id=?", (run["run_id"],)).fetchone()
        if row["current_revision"] != revision or not row["current_artifact_json"]:
            raise ValueError("Quality requested for stale or missing candidate")
        artifact = json.loads(row["current_artifact_json"])
        prior = db.execute("SELECT * FROM verdicts WHERE action_id=?", (action_id,)).fetchone()
        if prior and prior["state"] == "observed":
            if prior["sha256"] != artifact["sha256"] or prior["reviewer"] != identity:
                raise ValueError("verdict binding changed")
            return "accepted" if prior["accepted"] else "rejected"
        if prior is None:
            db.execute("INSERT INTO verdicts(action_id,run_id,revision,sha256,reviewer,accepted,state) "
                       "VALUES (?,?,?,?,?,0,'intent')",
                       (action_id, run["run_id"], revision, artifact["sha256"], identity))
            prior = db.execute("SELECT * FROM verdicts WHERE action_id=?", (action_id,)).fetchone()
        may_send = prior["attempts"] == 0
        if may_send:
            db.execute("UPDATE verdicts SET attempts=1,state='submitted_unknown' WHERE action_id=?", (action_id,))
            event(db, run["run_id"], "quality_intent", {"action_id": action_id,
                                                        "revision": revision, "sha256": artifact["sha256"]})
    command = {"op": "review", "action_id": action_id, "run_id": run["run_id"],
               "definition_digest": run["definition_digest"], "artifact": artifact}
    found = lookup(url, action_id, run)
    if found is None and may_send:
        try:
            found = send(url, command)
        except UncertainSubmission:
            pass
    if found is None:
        found = participating_recovery(url, action_id, run, "verdicts", command, identity)
    # The fixture commits its verdict before this process records it. A fault
    # here tests recovery by action lookup, without issuing another review.
    fault = runtime() / "fault-quality-before-record"
    marker = runtime() / f"fault-quality-before-record-{run['run_id']}-{revision}.done"
    if fault.is_file() and not marker.exists():
        marker.write_text("fault injected\n")
        os._exit(42)
    decision = _record_quality(run, revision, found, identity)
    return "accepted" if decision["accepted"] else "rejected"


def route_value(field: str) -> str:
    run = get_run()
    if not run["join_json"]:
        raise ValueError("typed route needs join")
    join_value = json.loads(run["join_json"])
    if field == "route_status" and join_value.get(field) in {"requires_scope", "clear"}:
        return join_value[field]
    if field == "requires_scope" and isinstance(join_value.get(field), bool):
        return "true" if join_value[field] else "false"
    raise ValueError("unapproved or malformed typed route field")


def accept(revision: str) -> dict:
    run = get_run()
    identity = expected_identity("quality")
    with db_connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT * FROM runs WHERE run_id=?", (run["run_id"],)).fetchone()
        verdict = db.execute("SELECT * FROM verdicts WHERE run_id=? AND revision=? AND state='observed'",
                             (run["run_id"], revision)).fetchone()
        if (row["current_revision"] != revision or verdict is None or not verdict["accepted"]
            or verdict["sha256"] != row["current_sha256"] or verdict["reviewer"] != identity
            or not row["current_artifact_json"]):
            raise ValueError("no verified positive Quality verdict for current exact revision")
        if row["accepted_revision"]:
            if (row["accepted_revision"], row["accepted_sha256"], row["accepted_reviewer"]) != (
                revision, row["current_sha256"], identity):
                raise ValueError("duplicate or stale authoritative acceptance")
        else:
            db.execute("UPDATE runs SET accepted_revision=?,accepted_sha256=?,accepted_reviewer=?,acceptance_count=1 "
                       "WHERE run_id=?", (revision, row["current_sha256"], identity, run["run_id"]))
            event(db, run["run_id"], "authoritative_acceptance", {"revision": revision,
                     "sha256": row["current_sha256"], "reviewer": identity})
    fault = runtime() / "fault-accept-before-engine-ack"
    marker = runtime() / f"fault-accept-before-engine-ack-{run['run_id']}-{revision}.done"
    if fault.is_file() and not marker.exists():
        marker.write_text("fault injected\n")
        os._exit(43)
    return {"revision": revision, "accepted": True}


def release(revision: str) -> dict:
    run = get_run()
    with db_connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT * FROM runs WHERE run_id=?", (run["run_id"],)).fetchone()
        if (row["accepted_revision"] != revision or row["accepted_sha256"] != row["current_sha256"]
            or row["current_revision"] != revision or row["acceptance_count"] != 1):
            raise ValueError("release requires authoritative exact current acceptance")
        artifact = json.loads(row["current_artifact_json"])
        release_id = run["run_id"] + ":release"
        prior = db.execute("SELECT * FROM releases WHERE release_id=?", (release_id,)).fetchone()
        if prior and prior["receipt_json"]:
            return json.loads(prior["receipt_json"])
        if prior is None:
            db.execute("INSERT INTO releases(release_id,run_id,state) VALUES (?,?,'intent')",
                       (release_id, run["run_id"]))
            prior = db.execute("SELECT * FROM releases WHERE release_id=?", (release_id,)).fetchone()
        may_send = prior["attempts"] == 0
        if may_send:
            db.execute("UPDATE releases SET attempts=1,state='submitted_unknown' WHERE release_id=?", (release_id,))
            event(db, run["run_id"], "release_intent", {"release_id": release_id,
                     "revision": revision, "sha256": artifact["sha256"]})
    mode = run["release_mode"]
    url = service_url("opaque" if mode == "opaque" else "release")
    payload = {"release_id": release_id, "run_id": run["run_id"],
               "definition_digest": run["definition_digest"], "revision": revision,
               "sha256": artifact["sha256"], "content": artifact["content"],
               "drop_ack": (runtime() / "fault-release-drop-ack").is_file()}
    found = None
    if mode == "participating":
        try:
            found = http_json(url + "/receipts/" + urllib.parse.quote(release_id, safe=""), token=True)
        except (OSError, urllib.error.HTTPError):
            pass
    if found is None and may_send:
        try:
            found = http_json(url + ("/release" if mode == "participating" else "/submit"),
                              payload, token=True)
        except (OSError, urllib.error.HTTPError):
            pass
    while found is None:
        with db_connect() as db:
            db.execute("UPDATE releases SET state='unresolved' WHERE release_id=?", (release_id,))
        if mode == "participating":
            try:
                found = http_json(url + "/receipts/" + urllib.parse.quote(release_id, safe=""), token=True)
            except (OSError, urllib.error.HTTPError):
                pass
        time.sleep(.25)
    if (found.get("release_id") != release_id or found.get("run_id") != run["run_id"]
        or found.get("definition_digest") != run["definition_digest"]
        or found.get("revision") != revision or found.get("sha256") != artifact["sha256"]):
        raise ValueError("release receipt binding mismatch")
    with db_connect() as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute("UPDATE releases SET state='observed',receipt_json=? WHERE release_id=?",
                   (canonical(found), release_id))
        db.execute("UPDATE runs SET receipt_json=?,release_count=1,state='released' WHERE run_id=?",
                   (canonical(found), run["run_id"]))
        event(db, run["run_id"], "release_observed", {"release_id": release_id,
                   "accepted_effect_count": found.get("accepted_effect_count")})
    return found


def engine_status(dag_name: str, run_id: str) -> dict | None:
    url = service_url("dagu") + f"/api/v1/dag-runs/{dag_name}/{run_id}"
    try:
        return http_json(url)["dagRunDetails"]
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise


def nested(child: str) -> dict:
    parent = get_run()
    published = manifest(parent["root_name"])
    if f"{child}.yaml" not in published["files"]:
        raise ValueError("child outside pinned closure")
    suffix = hashlib.sha256((parent["run_id"] + ":" + child).encode()).hexdigest()[:20]
    child_id = "exo-arb-child-" + suffix
    register_run(child_id, child, parent["root_name"], parent_id=parent["run_id"],
                 resolve_input=bool(parent["resolve_input"]), release_mode=parent["release_mode"])
    status = engine_status(child, child_id)
    if status is None:
        try:
            http_json(service_url("dagu") + f"/api/v1/dags/{child}.yaml/start", {"dagRunId": child_id})
        except (OSError, urllib.error.HTTPError):
            # The start may have committed. Reconcile by stable run ID.
            if engine_status(child, child_id) is None:
                raise
    while True:
        current = get_run(child_id)
        if current["state"] in {"released", "aborted"}:
            with db_connect() as db:
                db.execute("BEGIN IMMEDIATE")
                db.execute("UPDATE runs SET state=?,public_result_json=? WHERE run_id=?",
                           ("child_finished", canonical({"child_run_id": child_id,
                           "state": current["state"], "accepted_revision": current["accepted_revision"],
                           "accepted_sha256": current["accepted_sha256"],
                           "receipt": json.loads(current["receipt_json"]) if current["receipt_json"] else None}),
                            parent["run_id"]))
            return {"child_run_id": child_id, "state": current["state"]}
        time.sleep(.25)


def parent_result() -> dict:
    run = get_run()
    if run["dag_name"].endswith("_v2"):
        return {"old_closure": run["definition_digest"]}
    if not run["public_result_json"]:
        raise ValueError("parent has no child public result")
    value = json.loads(run["public_result_json"])
    if value["state"] == "released":
        child = get_run(value["child_run_id"])
        if (child["accepted_revision"] != child["current_revision"]
            or child["accepted_sha256"] != child["current_sha256"] or child["release_count"] != 1):
            raise ValueError("child public accepted result mismatch")
        value["artifact"] = json.loads(child["current_artifact_json"])
    with db_connect() as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute("UPDATE runs SET state=?,public_result_json=? WHERE run_id=?",
                   (value["state"], canonical(value), run["run_id"]))
    return value


def abort(revision: str) -> dict:
    run = get_run()
    with db_connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT * FROM runs WHERE run_id=?", (run["run_id"],)).fetchone()
        verdicts = db.execute("SELECT revision,accepted FROM verdicts WHERE run_id=? AND state='observed'",
                              (run["run_id"],)).fetchall()
        command = db.execute("SELECT * FROM director_commands WHERE run_id=? AND revision=? AND decision='abort' "
                             "AND state IN ('authorized','delivered')", (run["run_id"], revision)).fetchone()
        if (revision != row["current_revision"] or row["repair_count"] != 2
            or {(r["revision"], r["accepted"]) for r in verdicts} != {("r1", 0), ("r2", 0), ("r3", 0)}
            or row["accepted_revision"] or row["release_count"] or command is None):
            raise ValueError("abort lacks current authorized exhausted Director decision")
        db.execute("UPDATE runs SET state='aborted' WHERE run_id=?", (run["run_id"],))
        event(db, run["run_id"], "director_abort", {"revision": revision, "key": command["key"]})
    return {"state": "aborted", "revision": revision}


def director_command(run_id: str, revision: str, key: str, token: str, owner_epoch: int,
                     expires_at: float | None = None) -> dict:
    if token != config()["director_token"]:
        raise ValueError("unauthorized Director")
    if expires_at is not None and (not isinstance(expires_at, (int, float)) or expires_at <= time.time()):
        raise ValueError("expired Director command")
    run = get_run(run_id)
    with db_connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        verdicts = db.execute("SELECT revision,accepted FROM verdicts WHERE run_id=? AND state='observed'",
                              (run_id,)).fetchall()
        if (row["owner_epoch"] != owner_epoch or row["state"] != "active"
            or row["current_revision"] != revision or revision != "r3"
            or row["repair_count"] != 2 or row["accepted_revision"]
            or {(r["revision"], r["accepted"]) for r in verdicts} != {("r1", 0), ("r2", 0), ("r3", 0)}):
            raise ValueError("stale owner, revision, or non-exhausted decision")
        prior = db.execute("SELECT * FROM director_commands WHERE key=?", (key,)).fetchone()
        if prior and (prior["run_id"], prior["revision"], prior["decision"], prior["owner_epoch"]) != (
            run_id, revision, "abort", owner_epoch):
            raise ValueError("Director action key conflict")
        competing = db.execute("SELECT * FROM director_commands WHERE run_id=? AND key<>?", (run_id, key)).fetchone()
        if competing:
            raise ValueError("Director decision already claimed")
        db.execute("INSERT OR IGNORE INTO director_commands VALUES (?,?,?,?,?,'authorized')",
                   (key, run_id, revision, "abort", owner_epoch))
        event(db, run_id, "director_authorized", {"revision": revision, "key": key,
                                                    "owner_epoch": owner_epoch})
    # Hold the write lock across the engine call. A newer owner cannot race an
    # old command between fencing and completion. Authorization is durable first.
    with db_connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT owner_epoch,current_revision FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if row["owner_epoch"] != owner_epoch or row["current_revision"] != revision:
            raise ValueError("stale Director before engine completion")
        status = engine_status(run["dag_name"], run_id)
        nodes = {node["step"]["id"]: node["statusLabel"] for node in status["nodes"]} if status else {}
        if nodes.get("director_wait") == "waiting":
            http_json(service_url("dagu") + f"/api/v1/dag-runs/{run['dag_name']}/{run_id}/human-tasks/director_wait/complete",
                      {"decision": "abort", "revision": revision})
        elif nodes.get("director_wait") != "succeeded":
            raise ValueError("Director wait not current")
        db.execute("UPDATE director_commands SET state='delivered' WHERE key=?", (key,))
    return {"run_id": run_id, "revision": revision, "decision": "abort", "key": key}


def claim_owner(run_id: str) -> int:
    with db_connect() as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute("UPDATE runs SET owner_epoch=owner_epoch+1 WHERE run_id=?", (run_id,))
        row = db.execute("SELECT owner_epoch FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            raise ValueError("unknown run")
        event(db, run_id, "owner_claim", {"owner_epoch": row["owner_epoch"]})
        return row["owner_epoch"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=["assign", "join", "synthesize", "quality", "route-value",
                                             "repair", "accept", "release", "abort", "nested", "parent-result",
                                             "register-run", "director-command", "claim-owner", "inspect"])
    parser.add_argument("--instance")
    parser.add_argument("--type", dest="result_type")
    parser.add_argument("--scope", default="requires_scope")
    parser.add_argument("--drop-ack", default="false")
    parser.add_argument("--branches")
    parser.add_argument("--revision")
    parser.add_argument("--resolved")
    parser.add_argument("--from", dest="from_revision")
    parser.add_argument("--to")
    parser.add_argument("--field")
    parser.add_argument("--child")
    parser.add_argument("--run-id")
    parser.add_argument("--dag-name")
    parser.add_argument("--root-name")
    parser.add_argument("--parent-id")
    parser.add_argument("--resolve-input", action="store_true")
    parser.add_argument("--release-mode", default="participating")
    parser.add_argument("--key")
    parser.add_argument("--token")
    parser.add_argument("--owner-epoch", type=int)
    parser.add_argument("--expires-at", type=float)
    args = parser.parse_args()
    op = args.operation
    try:
        if op == "assign": value = assign(args.instance, args.result_type, args.scope, args.drop_ack)
        elif op == "join": value = join(args.branches)
        elif op == "synthesize": value = synthesize(args.revision, args.resolved)
        elif op == "quality": value = quality(args.revision)
        elif op == "route-value": value = route_value(args.field)
        elif op == "repair": value = repair(args.from_revision, args.to, args.resolved)
        elif op == "accept": value = accept(args.revision)
        elif op == "release": value = release(args.revision)
        elif op == "abort": value = abort(args.revision)
        elif op == "nested": value = nested(args.child)
        elif op == "parent-result": value = parent_result()
        elif op == "register-run": value = register_run(args.run_id, args.dag_name, args.root_name,
            parent_id=args.parent_id, resolve_input=args.resolve_input, release_mode=args.release_mode)
        elif op == "director-command": value = director_command(args.run_id, args.revision,
            args.key, args.token, args.owner_epoch, args.expires_at)
        elif op == "claim-owner": value = {"owner_epoch": claim_owner(args.run_id)}
        else: value = get_run(args.run_id)
        print(value if isinstance(value, str) else canonical(value), flush=True)
    except Exception as error:
        print(f"{type(error).__name__}: {error}", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
