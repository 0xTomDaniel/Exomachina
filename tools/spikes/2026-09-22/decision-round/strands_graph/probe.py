"""Product-shaped Strands Graph decision probe against the shared A2A harness.

Only the prior pinned Graph builder and documented FileSessionManager adapter
are reused. This file is candidate-owned product glue, not an SDK feature.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from uuid import uuid4


HERE = Path(__file__).resolve().parent
PRIOR = HERE.parents[1] / "round-two" / "strands_graph"
sys.path.insert(0, str(PRIOR))
from file_session_probe import runner  # noqa: E402  (patches FileSessionManager)


BASE_MODEL = runner.FixtureModel
BASE_VALIDATE = runner.validate
COMMON_SERVER = HERE.parent / "common" / "harness_server.py"
AUTH = {"Authorization": "Bearer fixture-token", "Content-Type": "application/json"}


def validate_factory(definition: dict) -> None:
    BASE_VALIDATE(definition)
    if definition.get("capabilities") != {
        "research_a": "a2a:capability@r2",
        "research_b": "a2a:capability@r2",
        "review": "a2a:quality@r2",
    }:
        raise ValueError("unapproved capability reference closure")


runner.SOURCE = HERE / "definitions"
runner.validate = validate_factory


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def db(root: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(root / "product.sqlite", timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.executescript(
        "CREATE TABLE IF NOT EXISTS actions (action_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, "
        "definition_digest TEXT NOT NULL, node TEXT NOT NULL, state TEXT NOT NULL, "
        "task_id TEXT, artifact TEXT, attempts INTEGER NOT NULL DEFAULT 0);"
        "CREATE TABLE IF NOT EXISTS acceptances (run_id TEXT PRIMARY KEY, definition_digest TEXT NOT NULL, "
        "artifact_sha256 TEXT NOT NULL, revision TEXT NOT NULL, reviewer TEXT NOT NULL, "
        "quality_action_id TEXT NOT NULL);"
        "CREATE TABLE IF NOT EXISTS deliveries (run_id TEXT PRIMARY KEY, artifact_sha256 TEXT NOT NULL);"
        "CREATE TABLE IF NOT EXISTS decisions (run_id TEXT PRIMARY KEY, command_key TEXT NOT NULL, "
        "definition_digest TEXT NOT NULL, artifact_sha256 TEXT NOT NULL, state TEXT NOT NULL);"
        "CREATE TABLE IF NOT EXISTS claims (run_id TEXT PRIMARY KEY, epoch INTEGER NOT NULL, "
        "owner TEXT NOT NULL, lease_until REAL NOT NULL, completed INTEGER NOT NULL DEFAULT 0);"
    )
    return connection


def binding(root: Path, run_id: str) -> dict:
    return json.loads((root / f"{run_id}.json").read_text())


def request_json(url: str, *, payload: dict | None = None, timeout: float = 3) -> dict:
    body = None if payload is None else canonical(payload).encode()
    request = urllib.request.Request(url, data=body, headers=AUTH, method="POST" if payload else "GET")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def lookup(url: str, action_id: str) -> dict | None:
    try:
        return request_json(url + "/fixture/actions/" + urllib.parse.quote(action_id, safe=""), timeout=0.6)
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise


def send(url: str, command: dict) -> dict:
    message = {"role": "user", "messageId": str(uuid4()), "parts": [{"kind": "data", "data": command}]}
    response = request_json(url + "/", payload={"jsonrpc": "2.0", "id": str(uuid4()), "method": "message/send", "params": {"message": message}}, timeout=8)
    if "error" in response:
        raise RuntimeError(f"A2A error: {response['error']}")
    return response["result"]


def artifact_from_task(task: dict) -> dict:
    if task.get("status", {}).get("state") != "completed":
        raise RuntimeError(f"remote task incomplete: {task.get('status')}")
    return task["artifacts"][0]["parts"][0]["data"]


def verify_remote_binding(record: dict, command: dict, expected_role: str, *, task: bool) -> None:
    source = record.get("metadata", {}) if task else record
    for field in ("action_id", "run_id", "definition_digest"):
        if source.get(field) != command[field]:
            raise ValueError(f"remote {field} does not match action intent")
    role = source.get("harness_role") if task else source.get("role")
    if role != expected_role:
        raise ValueError("remote harness role mismatch")


def record_intent(root: Path, run_id: str, node: str, action_id: str) -> dict | None:
    digest = binding(root, run_id)["digest"]
    with db(root) as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "INSERT OR IGNORE INTO actions (action_id,run_id,definition_digest,node,state) VALUES (?,?,?,?,'pending')",
            (action_id, run_id, digest, node),
        )
        row = connection.execute("SELECT * FROM actions WHERE action_id=?", (action_id,)).fetchone()
        if (row["run_id"], row["definition_digest"], row["node"]) != (run_id, digest, node):
            raise ValueError("action identity reused for another binding")
        return json.loads(row["artifact"]) if row["artifact"] else None


def record_remote(root: Path, action_id: str, task_id: str, artifact: dict, *, submitted: bool) -> dict:
    with db(root) as connection:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute("SELECT node,artifact FROM actions WHERE action_id=?", (action_id,)).fetchone()
        if row is None:
            raise ValueError("remote action has no local intent")
        if row["node"] == "review":
            if artifact.get("revision") != "r2" or not artifact.get("reviewer") or not artifact.get("sha256"):
                raise ValueError("remote Quality decision invalid")
        elif artifact.get("revision") != "r2" or not isinstance(artifact.get("content"), str) or hashlib.sha256(artifact["content"].encode()).hexdigest() != artifact.get("sha256") or not artifact.get("author"):
            raise ValueError("remote capability artifact revision/hash mismatch")
        if row["artifact"] and json.loads(row["artifact"]) != artifact:
            raise ValueError("remote action changed its accepted artifact")
        connection.execute(
            "UPDATE actions SET state='completed',task_id=?,artifact=? WHERE action_id=?",
            (task_id, canonical(artifact), action_id),
        )
    return artifact


def mark_attempt(root: Path, action_id: str) -> None:
    with db(root) as connection:
        connection.execute("UPDATE actions SET attempts=attempts+1 WHERE action_id=?", (action_id,))


def remote_action(root: Path, run_id: str, node: str, url: str, command: dict) -> dict:
    action_id = command["action_id"]
    expected_role = "quality" if node == "review" else "capability"
    existing = record_intent(root, run_id, node, action_id)
    if existing is not None:
        return existing
    receipt = lookup(url, action_id)
    if receipt is not None:
        verify_remote_binding(receipt, command, expected_role, task=False)
        return record_remote(root, action_id, receipt["task_id"], receipt["artifact"], submitted=False)
    initial_health = None
    try:
        initial_health = request_json(url + "/health", timeout=0.6)
    except OSError:
        if expected_role == "capability" and os.environ.get("EXO_CAPABILITY_INCARNATION"):
            initial_health = {"identity": os.environ["EXO_CAPABILITY_IDENTITY"], "incarnation": int(os.environ["EXO_CAPABILITY_INCARNATION"])}
    mark_attempt(root, action_id)
    try:
        task = send(url, command)
    except urllib.error.HTTPError:
        raise
    except Exception as error:
        # A reply loss is ambiguous: never resubmit until a receiver query
        # establishes whether the action committed. A2A has no such query by
        # caller key; this is an application extension of the common fixture.
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                receipt = lookup(url, action_id)
                if receipt is not None:
                    verify_remote_binding(receipt, command, expected_role, task=False)
                    return record_remote(root, action_id, receipt["task_id"], receipt["artifact"], submitted=True)
                # The common fixture commits the action before replying. Once
                # this exact receiver identity has restarted, a missing row is
                # authoritative for its *local* state, so retry the stable key.
                # General A2A peers make no such promise.
                current_health = request_json(url + "/health", timeout=0.6)
                if initial_health and current_health.get("identity") == initial_health.get("identity") and current_health.get("incarnation", 0) > initial_health.get("incarnation", 0):
                    mark_attempt(root, action_id)
                    retry_command = {**command, "drop_ack": False}
                    task = send(url, retry_command)
                    verify_remote_binding(task, command, expected_role, task=True)
                    return record_remote(root, action_id, task["id"], artifact_from_task(task), submitted=True)
            except (OSError, TimeoutError):
                pass
            time.sleep(0.1)
        raise RuntimeError(f"ambiguous remote action {action_id}; no safe resubmit") from error
    verify_remote_binding(task, command, expected_role, task=True)
    return record_remote(root, action_id, task["id"], artifact_from_task(task), submitted=True)


def current_candidate(root: Path, run_id: str) -> dict:
    with db(root) as connection:
        row = connection.execute("SELECT artifact FROM actions WHERE action_id=? AND state='completed'", (f"{run_id}:research_a",)).fetchone()
    if row is None:
        raise ValueError("no current candidate")
    return json.loads(row["artifact"])


def accept(root: Path, run_id: str, candidate: dict, review: dict, quality_action_id: str) -> dict:
    expected = current_candidate(root, run_id)
    digest = binding(root, run_id)["digest"]
    if candidate != expected or candidate.get("revision") != "r2":
        raise ValueError("review candidate is not the exact current revision")
    if review.get("accepted") is not True or review.get("revision") != candidate["revision"] or review.get("sha256") != candidate["sha256"]:
        raise ValueError("Quality did not accept exact artifact revision")
    if not review.get("reviewer") or review["reviewer"] == candidate.get("author"):
        raise ValueError("self-review is not independent Quality")
    with db(root) as connection:
        connection.execute("BEGIN IMMEDIATE")
        action = connection.execute("SELECT * FROM actions WHERE action_id=?", (quality_action_id,)).fetchone()
        if action is None or action["run_id"] != run_id or action["definition_digest"] != digest or action["state"] != "completed":
            raise ValueError("Quality action is not bound to this exact run")
        previous = connection.execute("SELECT * FROM acceptances WHERE run_id=?", (run_id,)).fetchone()
        if previous:
            if (previous["definition_digest"], previous["artifact_sha256"], previous["revision"], previous["reviewer"], previous["quality_action_id"]) != (digest, candidate["sha256"], "r2", review["reviewer"], quality_action_id):
                raise ValueError("duplicate acceptance differs from authoritative revision")
            return dict(previous)
        connection.execute(
            "INSERT INTO acceptances VALUES (?,?,?,?,?,?)",
            (run_id, digest, candidate["sha256"], candidate["revision"], review["reviewer"], quality_action_id),
        )
    return {"run_id": run_id, "definition_digest": digest, "artifact_sha256": candidate["sha256"], "revision": "r2", "reviewer": review["reviewer"]}


def deliver(root: Path, run_id: str) -> None:
    bound = binding(root, run_id)
    candidate = current_candidate(root, run_id)
    with db(root) as connection:
        connection.execute("BEGIN IMMEDIATE")
        ownership = connection.execute("SELECT * FROM claims WHERE run_id=?", (run_id,)).fetchone()
        if ownership is None or ownership["owner"] != os.environ.get("EXO_OWNER_ID") or ownership["epoch"] != int(os.environ.get("EXO_CLAIM_EPOCH", "-1")) or ownership["lease_until"] <= time.time():
            raise ValueError("delivery owner lost its run claim")
        acceptance = connection.execute("SELECT * FROM acceptances WHERE run_id=?", (run_id,)).fetchone()
        if acceptance is None or (acceptance["definition_digest"], acceptance["artifact_sha256"], acceptance["revision"]) != (bound["digest"], candidate["sha256"], candidate["revision"]):
            raise ValueError("Director continuation lacks exact Quality acceptance")
        connection.execute("INSERT OR IGNORE INTO deliveries VALUES (?,?)", (run_id, candidate["sha256"]))
        if connection.execute("SELECT artifact_sha256 FROM deliveries WHERE run_id=?", (run_id,)).fetchone()[0] != candidate["sha256"]:
            raise ValueError("delivery changed artifact")


def queue_decision(root: Path, run_id: str) -> dict:
    bound = binding(root, run_id)
    key = f"{run_id}:director-approve"
    with db(root) as connection:
        connection.execute("BEGIN IMMEDIATE")
        accepted = connection.execute("SELECT * FROM acceptances WHERE run_id=?", (run_id,)).fetchone()
        if accepted is None or accepted["definition_digest"] != bound["digest"]:
            raise ValueError("decision needs exact accepted artifact")
        connection.execute("INSERT OR IGNORE INTO decisions VALUES (?,?,?,?,'queued')", (run_id, key, bound["digest"], accepted["artifact_sha256"]))
        row = connection.execute("SELECT * FROM decisions WHERE run_id=?", (run_id,)).fetchone()
        if (row["command_key"], row["definition_digest"], row["artifact_sha256"]) != (key, bound["digest"], accepted["artifact_sha256"]):
            raise ValueError("decision outbox changed")
        return dict(row)


class ProductModel(BASE_MODEL):
    async def stream(self, messages, tool_specs=None, system_prompt=None, **kwargs):
        root = self.events.parent
        run_id = self.events.name.removesuffix(".events.jsonl")
        digest = binding(root, run_id)["digest"]
        if self.marker in ("research_a", "research_b"):
            command = {"op": "assign", "action_id": f"{run_id}:{self.marker}", "run_id": run_id, "definition_digest": digest, "brief": f"{run_id}:{self.marker}"}
            if os.environ.get("EXO_DROP_ACK_ACTION") == command["action_id"]:
                command["drop_ack"] = True
            await asyncio.to_thread(remote_action, root, run_id, self.marker, os.environ["EXO_CAPABILITY_URL"], command)
        elif self.marker == "verify":
            await asyncio.to_thread(current_candidate, root, run_id)
        elif self.marker == "review":
            candidate = await asyncio.to_thread(current_candidate, root, run_id)
            action_id = f"{run_id}:review"
            review = await asyncio.to_thread(remote_action, root, run_id, "review", os.environ["EXO_QUALITY_URL"], {"op": "review", "action_id": action_id, "run_id": run_id, "definition_digest": digest, "artifact": candidate})
            await asyncio.to_thread(accept, root, run_id, candidate, review, action_id)
        elif self.marker == "deliver":
            await asyncio.to_thread(deliver, root, run_id)
        async for event in super().stream(messages, tool_specs, system_prompt, **kwargs):
            yield event


runner.FixtureModel = ProductModel


def claim(root: Path, run_id: str, owner: str, lease_seconds: float) -> int | None:
    with db(root) as connection:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute("SELECT * FROM claims WHERE run_id=?", (run_id,)).fetchone()
        now = time.time()
        if row and (row["completed"] or row["lease_until"] > now):
            return None
        epoch = 1 if row is None else row["epoch"] + 1
        connection.execute(
            "INSERT INTO claims VALUES (?,?,?,?,0) ON CONFLICT(run_id) DO UPDATE SET "
            "epoch=excluded.epoch,owner=excluded.owner,lease_until=excluded.lease_until",
            (run_id, epoch, owner, now + lease_seconds),
        )
        return epoch


def finish(root: Path, run_id: str, owner: str, epoch: int) -> None:
    with db(root) as connection:
        connection.execute("BEGIN IMMEDIATE")
        if connection.execute("UPDATE claims SET completed=1 WHERE run_id=? AND owner=? AND epoch=? AND lease_until>?", (run_id, owner, epoch, time.time())).rowcount != 1:
            raise RuntimeError("run claim expired before completion")
        connection.execute("UPDATE decisions SET state='delivered' WHERE run_id=?", (run_id,))


def resume(root: Path, run_id: str, owner: str, lease_seconds: float) -> dict:
    command = queue_decision(root, run_id)
    epoch = claim(root, run_id, owner, lease_seconds)
    if epoch is None:
        return {"owner": owner, "claim": "rejected", "command_key": command["command_key"]}
    os.environ["EXO_OWNER_ID"] = owner
    os.environ["EXO_CLAIM_EPOCH"] = str(epoch)
    result = runner.resume(root, run_id, "approve")
    if result["status"] == "completed":
        finish(root, run_id, owner, epoch)
    return {"owner": owner, "epoch": epoch, "command_key": command["command_key"], "graph": result}


def start_one(root: Path, run_id: str, revision: int) -> dict:
    published = runner.publish(root, revision)
    result = runner.start(root, run_id, published)
    if len(result["interrupts"]) != 1:
        raise RuntimeError("run did not reach Director gate")
    (root / f"{run_id}.interrupt.json").write_text(canonical(result["interrupts"][0]))
    return result


def inspect(root: Path) -> dict:
    with db(root) as connection:
        actions = [dict(row) for row in connection.execute("SELECT * FROM actions ORDER BY action_id")]
        acceptance = [dict(row) for row in connection.execute("SELECT * FROM acceptances ORDER BY run_id")]
        deliveries = [dict(row) for row in connection.execute("SELECT * FROM deliveries ORDER BY run_id")]
        claims = [dict(row) for row in connection.execute("SELECT * FROM claims ORDER BY run_id")]
        decisions = [dict(row) for row in connection.execute("SELECT * FROM decisions ORDER BY run_id")]
    for action in actions:
        if action["artifact"]:
            action["artifact"] = json.loads(action["artifact"])
    return {"actions": actions, "acceptances": acceptance, "deliveries": deliveries, "claims": claims, "decisions": decisions}


def negative_checks(root: Path, run_id: str) -> dict:
    candidate = current_candidate(root, run_id)
    action_id = f"{run_id}:review"
    with db(root) as connection:
        review = json.loads(connection.execute("SELECT artifact FROM actions WHERE action_id=?", (action_id,)).fetchone()[0])
    rejected = {}
    identical = accept(root, run_id, candidate, review, action_id)
    with db(root) as connection:
        count = connection.execute("SELECT COUNT(*) FROM acceptances WHERE run_id=?", (run_id,)).fetchone()[0]
    assert count == 1 and identical["artifact_sha256"] == candidate["sha256"]
    rejected["identical_repeat"] = "idempotent same-result; one stored acceptance"
    cases = {
        "stale_revision": ({**candidate, "revision": "r1"}, review),
        "self_review": (candidate, {**review, "reviewer": candidate["author"]}),
        "wrong_sha": (candidate, {**review, "sha256": "0" * 64}),
        "divergent_repeat": (candidate, {**review, "reviewer": "other-quality-id"}),
    }
    for label, (artifact, decision) in cases.items():
        try:
            accept(root, run_id, artifact, decision, action_id)
        except ValueError as error:
            rejected[label] = str(error)
        else:
            raise AssertionError(f"{label} was accepted")
    try:
        accept(root, run_id, candidate, review, f"another-run:review")
    except ValueError as error:
        rejected["wrong_quality_action"] = str(error)
    else:
        raise AssertionError("wrong run Quality action was accepted")
    return rejected


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def rss_kib(process: subprocess.Popen) -> int:
    return int(subprocess.check_output(["ps", "-o", "rss=", "-p", str(process.pid)], text=True).strip())


def disk_kib(path: Path) -> int:
    return int(subprocess.check_output(["du", "-sk", str(path)], text=True).split()[0])


def launch_server(python: Path, state: Path, role: str, port: int) -> subprocess.Popen:
    state.mkdir(parents=True, exist_ok=True)
    log = (state / "server.log").open("a")
    process = subprocess.Popen([str(python), str(COMMON_SERVER), "--state", str(state), "--role", role, "--port", str(port)], stdout=log, stderr=log)
    log.close()
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"{role} server exited: {(state / 'server.log').read_text()[-2000:]}")
        try:
            request_json(f"http://127.0.0.1:{port}/health", timeout=0.3)
            return process
        except Exception:
            time.sleep(0.05)
    process.kill()
    raise TimeoutError(f"{role} server did not start")


def invoke(root: Path, action: str, *extra: str, environment: dict | None = None) -> dict:
    process = subprocess.run([sys.executable, __file__, action, "--root", str(root), *extra], text=True, capture_output=True, timeout=90, env=environment)
    if process.returncode:
        raise RuntimeError(f"{action} failed: {process.stderr[-4000:]} {process.stdout[-1000:]}")
    return json.loads(process.stdout.strip().splitlines()[-1])


def exercise(root: Path, server_python: Path) -> dict:
    root.mkdir(parents=True, exist_ok=True)
    cap_port, quality_port = free_port(), free_port()
    cap_state, quality_state = root / "remote-capability", root / "remote-quality"
    cap = launch_server(server_python, cap_state, "capability", cap_port)
    quality = launch_server(server_python, quality_state, "quality", quality_port)
    base_env = os.environ.copy()
    base_env["EXO_CAPABILITY_URL"] = f"http://127.0.0.1:{cap_port}"
    base_env["EXO_QUALITY_URL"] = f"http://127.0.0.1:{quality_port}"
    cap_identity = request_json(base_env["EXO_CAPABILITY_URL"] + "/health")
    quality_identity = request_json(base_env["EXO_QUALITY_URL"] + "/health")
    assert cap_identity["identity"] != quality_identity["identity"]
    assert cap_identity["a2a_protocol"] == quality_identity["a2a_protocol"] == "0.3.0"
    children = [cap, quality]
    try:
        director = subprocess.Popen([sys.executable, __file__, "setup", "--root", str(root), "--hold"], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=base_env)
        children.append(director)
        assert director.stdout is not None
        started = json.loads(director.stdout.readline())
        assert started["v1"]["status"] == started["v2"]["status"] == "interrupted"
        assert director.poll() is None
        queued_v1 = invoke(root, "queue", "--run-id", "run-v1", environment=base_env)
        queued_v2 = invoke(root, "queue", "--run-id", "run-v2", environment=base_env)
        assert queued_v1["state"] == queued_v2["state"] == "queued"
        director.kill()
        assert director.wait(timeout=10) < 0
        resumed_v1 = invoke(root, "resume", "--run-id", "run-v1", "--owner", "one", environment=base_env)
        resumed_v2 = invoke(root, "resume", "--run-id", "run-v2", "--owner", "two", environment=base_env)
        assert resumed_v1["graph"]["status"] == resumed_v2["graph"]["status"] == "completed"
        assert "verify" not in resumed_v1["graph"]["execution_order"]
        assert "verify" in resumed_v2["graph"]["execution_order"]
        negatives = invoke(root, "negative", "--run-id", "run-v2", environment=base_env)

        lost_env = base_env.copy()
        lost_env["EXO_DROP_ACK_ACTION"] = "run-lost:research_a"
        lost_env["EXO_CAPABILITY_IDENTITY"] = cap_identity["identity"]
        lost_env["EXO_CAPABILITY_INCARNATION"] = str(cap_identity["incarnation"])
        victim = subprocess.Popen([sys.executable, __file__, "start-one", "--root", str(root), "--run-id", "run-lost", "--revision", "2"], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=lost_env)
        children.append(victim)
        deadline = time.monotonic() + 20
        while cap.poll() is None and victim.poll() is None and time.monotonic() < deadline:
            time.sleep(0.05)
        assert cap.poll() is not None, "fixture did not drop capability acknowledgement"
        cap = launch_server(server_python, cap_state, "capability", cap_port)
        children.append(cap)
        cap_restarted = request_json(base_env["EXO_CAPABILITY_URL"] + "/health")
        assert cap_restarted["identity"] == cap_identity["identity"]
        assert cap_restarted["incarnation"] == cap_identity["incarnation"] + 1
        victim_out, victim_err = victim.communicate(timeout=40)
        if victim.returncode:
            raise RuntimeError(f"lost-ack Graph failed: {victim_err[-4000:]} {victim_out[-1000:]}")
        lost_started = json.loads(victim_out.strip().splitlines()[-1])
        assert lost_started["status"] == "interrupted"
        lost_receipt = lookup(base_env["EXO_CAPABILITY_URL"], "run-lost:research_a")
        assert lost_receipt and lost_receipt["accepted_count"] == 1 and lost_receipt["attempts"] == 1
        lost_resumed = invoke(root, "resume", "--run-id", "run-lost", "--owner", "recovered", environment=base_env)
        assert lost_resumed["graph"]["status"] == "completed"

        race_started = invoke(root, "start-one", "--run-id", "run-race", "--revision", "1", environment=base_env)
        assert race_started["status"] == "interrupted"
        gate = root / "race-gate"
        gate.mkdir()
        race_env = base_env.copy()
        race_env["EXO_RENDEZVOUS_DIR"] = str(gate)
        race_env["EXO_OWNER_ID"] = "race-first"
        first = subprocess.Popen([sys.executable, __file__, "resume", "--root", str(root), "--run-id", "run-race", "--owner", "race-first"], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=race_env)
        children.append(first)
        deadline = time.monotonic() + 15
        while not (gate / "ready-race-first").exists() and time.monotonic() < deadline:
            if first.poll() is not None:
                raise RuntimeError(f"race owner died: {first.stderr.read()[-2000:]}")
            time.sleep(0.02)
        assert (gate / "ready-race-first").exists()
        rejected = invoke(root, "resume", "--run-id", "run-race", "--owner", "race-second", environment=base_env)
        assert rejected["claim"] == "rejected"
        (gate / "release-race-first").touch()
        first_out, first_err = first.communicate(timeout=20)
        if first.returncode:
            raise RuntimeError(f"race first failed: {first_err[-2000:]}")
        race_first = json.loads(first_out.strip().splitlines()[-1])
        assert race_first["graph"]["status"] == "completed"
        race_remote = {node: lookup(base_env["EXO_CAPABILITY_URL"], f"run-race:{node}") for node in ("research_a", "research_b")}
        assert all(receipt and receipt["attempts"] == receipt["accepted_count"] == 1 for receipt in race_remote.values())
        state = inspect(root)
        assert len(state["deliveries"]) == 4
        assert len(state["acceptances"]) == 4
        assert all(row["state"] == "delivered" for row in state["decisions"])
        assert all(row["state"] == "completed" for row in state["actions"])
        bypass = json.loads((runner.SOURCE / "v1.json").read_text())
        bypass["edges"] = [edge for edge in bypass["edges"] if edge["to"] != "deliver"] + [{"from": "join", "to": "deliver"}]
        try:
            runner.validate(bypass)
        except ValueError as error:
            bypass_rejected = str(error)
        else:
            raise AssertionError("review bypass published")
        held_root = root / "measurement"
        held = subprocess.Popen([sys.executable, __file__, "setup", "--root", str(held_root), "--hold"], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=base_env)
        children.append(held)
        assert held.stdout is not None
        ready = json.loads(held.stdout.readline())
        assert ready["v1"]["status"] == ready["v2"]["status"] == "interrupted"
        assert held.poll() is None
        measurement = {"graph_rss_kib": rss_kib(held), "capability_rss_kib": rss_kib(cap), "quality_rss_kib": rss_kib(quality), "graph_state_kib": disk_kib(held_root), "capability_state_kib": disk_kib(cap_state), "quality_state_kib": disk_kib(quality_state), "graph_paused_runs": 2}
        result = {"sdk": "strands-agents==1.57.0", "a2a_protocol": "0.3.0", "started": started, "acceptance_engine_gap": {"queued_v1": queued_v1, "queued_v2": queued_v2, "director_sigkill_after_queue": True}, "resumed": {"v1": resumed_v1, "v2": resumed_v2, "lost": lost_resumed}, "negative_checks": negatives, "bypass_rejected": bypass_rejected, "lost_ack": {"receipt": lost_receipt, "graph": lost_started, "original_capability": cap_identity, "restarted_capability": cap_restarted}, "same_run_race": {"second_owner": rejected, "first_owner": race_first, "remote_receipts_from_prior_start": race_remote}, "state": state, "measurement": measurement, "topology": {"graph_processes": "fresh per command except held measurement", "capability": "separate Strands harness process", "quality": "separate Strands harness process", "database": "product.sqlite + two remote harness state dirs"}}
        (HERE / "observed.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        return result
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=10)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["exercise", "setup", "start-one", "queue", "resume", "negative", "inspect"])
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--server-python", type=Path)
    parser.add_argument("--run-id")
    parser.add_argument("--owner")
    parser.add_argument("--revision", type=int, default=1)
    parser.add_argument("--lease", type=float, default=15)
    parser.add_argument("--hold", action="store_true")
    args = parser.parse_args()
    if args.action == "exercise":
        if args.server_python is None:
            parser.error("exercise requires --server-python")
        result = exercise(args.root, args.server_python)
        print(canonical({"status": "passed", "runs": [row["run_id"] for row in result["state"]["deliveries"]]}))
    elif args.action == "setup":
        result = runner.scenario(args.root, hold=args.hold)
        if not args.hold:
            print(canonical(result))
    elif args.action == "start-one":
        if args.run_id is None:
            parser.error("start-one requires --run-id")
        print(canonical(start_one(args.root, args.run_id, args.revision)))
    elif args.action == "resume":
        if args.run_id is None or args.owner is None:
            parser.error("resume requires --run-id and --owner")
        print(canonical(resume(args.root, args.run_id, args.owner, args.lease)))
    elif args.action == "queue":
        if args.run_id is None:
            parser.error("queue requires --run-id")
        print(canonical(queue_decision(args.root, args.run_id)))
    elif args.action == "negative":
        print(canonical(negative_checks(args.root, args.run_id)))
    else:
        print(canonical(inspect(args.root)))


if __name__ == "__main__":
    main()
