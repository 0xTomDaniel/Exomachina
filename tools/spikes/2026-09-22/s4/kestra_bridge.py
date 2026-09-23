"""S4: custom remote receipt + product acceptance + real Kestra continuation."""

import json
import os
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.parse
import urllib.error
from pathlib import Path

import spike

S1 = Path("/tmp/exomachina-spikes/s1")
sys.path.insert(0, str(S1))
import probe  # noqa: E402

ROOT = Path(__file__).parent
KESTRA = "http://127.0.0.1:28081"
probe.AUTH = json.loads((S1 / "auth.json").read_text())


def db(base):
    return spike.connect(base / "product.sqlite")


def init(base):
    spike.init(base)
    with db(base) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS binding (action_id TEXT PRIMARY KEY, execution_id TEXT NOT NULL)")


def one_binding(base):
    with db(base) as conn:
        return conn.execute("SELECT action_id, execution_id FROM binding").fetchone()


def prepare(base, remote_port):
    code, execution = probe.call(
        KESTRA, "POST", "/executions/exomachina.spikes/exomachina_s1_pause",
        b"--empty--\r\n", "multipart/form-data; boundary=empty",
    )
    assert code in (200, 201), (code, execution)
    execution_id = execution["id"]
    probe.wait_for(KESTRA, execution_id, {"PAUSED"})
    action_id = "remote-" + execution_id
    with db(base) as conn:
        conn.execute("INSERT INTO binding VALUES (?, ?)", (action_id, execution_id))
        conn.execute("INSERT INTO assignments VALUES (?, 'cooperative', 'intent', NULL)", (action_id,))
    status, _ = spike.request(remote_port, "POST", "/assign", {"action_id": action_id, "mode": "cooperative"})
    assert status == 200
    os._exit(31)  # Remote accepted. Adapter did not retain acknowledgement.


def recover_remote(base, remote_port):
    action_id, _ = one_binding(base)
    route = "/action?" + urllib.parse.urlencode({"mode": "cooperative", "action_id": action_id})
    status, body = spike.request(remote_port, "GET", route)
    assert status == 200
    with db(base) as conn:
        conn.execute("UPDATE assignments SET status='submitted', task_id=? WHERE action_id=?", (body["task_id"], action_id))


def accept(base):
    action_id, execution_id = one_binding(base)
    with db(base) as conn:
        assert conn.execute("SELECT status FROM assignments WHERE action_id=?", (action_id,)).fetchone()[0] == "submitted"
        conn.execute("INSERT INTO artifacts VALUES (?, 'writer-1', 'r2', NULL, NULL)", (action_id,))

        def approve(revision, reviewer):
            author, current, accepted = conn.execute(
                "SELECT author, current_revision, accepted_revision FROM artifacts WHERE action_id=?", (action_id,)
            ).fetchone()
            if revision != current or reviewer == author or accepted is not None:
                return False
            conn.execute("UPDATE artifacts SET accepted_revision=?, reviewer=? WHERE action_id=?", (revision, reviewer, action_id))
            conn.execute("INSERT INTO outbox(id,action_id,revision) VALUES (?, ?, ?)", ("resume-" + execution_id, action_id, revision))
            return True

        assert not approve("r1", "quality-1")
        assert not approve("r2", "writer-1")
        assert approve("r2", "quality-1")
        assert not approve("r2", "quality-1")
    os._exit(32)  # Acceptance committed. Engine still PAUSED.


def drain(base, drop):
    _, execution_id = one_binding(base)
    with db(base) as conn:
        done = conn.execute("SELECT done FROM outbox").fetchone()[0]
        assert done == 0
        code, current = probe.call(KESTRA, "GET", "/executions/" + execution_id)
        assert code == 200
        state = probe.state(current)
        if state == "PAUSED":
            code, _ = probe.call(
                KESTRA, "POST", f"/executions/{execution_id}/actions/resume",
                b"--empty--\r\n", "multipart/form-data; boundary=empty",
            )
            assert code == 200, (code, state)
            if drop:
                os._exit(33)  # Kestra accepted resume; product did not record acknowledgement.
        elif state not in ("RUNNING", "SUCCESS"):
            raise AssertionError(f"Cannot reconcile engine state: {state}")
        conn.execute("UPDATE outbox SET done=1")


def verify(base, remote_port):
    action_id, execution_id = one_binding(base)
    final = probe.wait_for(KESTRA, execution_id, {"SUCCESS", "FAILED", "KILLED"})
    after = [r for r in final.get("taskRunList", []) if r.get("taskId") == "after"]
    code, remote = spike.request(remote_port, "GET", "/stats")
    assert code == 200
    with db(base) as conn:
        assignment = conn.execute("SELECT status,task_id FROM assignments WHERE action_id=?", (action_id,)).fetchone()
        artifact = conn.execute("SELECT accepted_revision,reviewer FROM artifacts WHERE action_id=?", (action_id,)).fetchone()
        outbox_done = conn.execute("SELECT done FROM outbox").fetchone()[0]
    assert probe.state(final) == "SUCCESS"
    assert len(after) == 1 and after[0]["state"]["current"] == "SUCCESS"
    assert remote["cooperative_effects"] == 1
    assert assignment == ("submitted", "task-" + action_id)
    assert artifact == ("r2", "quality-1") and outbox_done == 1
    evidence = {
        "result": "PASS",
        "engine": "Kestra OSS 2.0.3 on PostgreSQL 16.15",
        "remote": "custom HTTP fixture, not A2A",
        "execution_id": execution_id,
        "engine_terminal": probe.state(final),
        "downstream_task_runs": len(after),
        "remote_effects": remote["cooperative_effects"],
        "accepted_revision": artifact[0],
        "reviewer": artifact[1],
        "outbox_done": outbox_done,
        "forced_client_exits": [31, 32, 33],
    }
    (base / "result.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print(json.dumps(evidence, indent=2))


def test(base):
    if base.exists() and any(base.iterdir()):
        raise SystemExit(f"Refusing to overwrite {base}")
    init(base)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = subprocess.Popen([sys.executable, str(ROOT / "spike.py"), "serve", str(base), str(port)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def phase(name, expected=0):
        child = subprocess.run([sys.executable, __file__, name, str(base), str(port)], capture_output=True, text=True)
        assert child.returncode == expected, f"{name} exit={child.returncode}: {child.stdout} {child.stderr}"
        return child.stdout

    try:
        for _ in range(50):
            try:
                spike.request(port, "GET", "/stats")
                break
            except urllib.error.URLError:
                time.sleep(0.1)
        else:
            raise AssertionError("remote fixture did not start")
        phase("prepare", expected=31)
        phase("recover")
        phase("accept", expected=32)
        phase("drain-drop", expected=33)
        phase("drain-reconcile")
        print(phase("verify"), end="")
    finally:
        server.terminate()
        server.wait(timeout=5)


if __name__ == "__main__":
    command = sys.argv[1]
    base = Path(sys.argv[2])
    if command == "test":
        test(base)
    else:
        remote_port = int(sys.argv[3])
        if command == "prepare":
            prepare(base, remote_port)
        elif command == "recover":
            recover_remote(base, remote_port)
        elif command == "accept":
            accept(base)
        elif command == "drain-drop":
            drain(base, True)
        elif command == "drain-reconcile":
            drain(base, False)
        elif command == "verify":
            verify(base, remote_port)
        else:
            raise SystemExit(f"Unknown command: {command}")
