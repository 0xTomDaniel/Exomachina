"""Deterministic S4 transaction-gap fixture. No workflow engine or A2A wire claim."""

import argparse
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
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def connect(path):
    db = sqlite3.connect(path, timeout=5)
    db.execute("PRAGMA journal_mode=WAL")
    return db


def init(base):
    base.mkdir(parents=True, exist_ok=True)
    with connect(base / "remote.sqlite") as db:
        db.execute("CREATE TABLE IF NOT EXISTS cooperative (action_id TEXT PRIMARY KEY, task_id TEXT NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS opaque (id INTEGER PRIMARY KEY, action_id TEXT NOT NULL)")
    with connect(base / "engine.sqlite") as db:
        db.execute("CREATE TABLE IF NOT EXISTS commands (id TEXT PRIMARY KEY, action_id TEXT NOT NULL, revision TEXT NOT NULL)")
    with connect(base / "product.sqlite") as db:
        db.execute("CREATE TABLE IF NOT EXISTS assignments (action_id TEXT PRIMARY KEY, mode TEXT NOT NULL, status TEXT NOT NULL, task_id TEXT)")
        db.execute("CREATE TABLE IF NOT EXISTS artifacts (action_id TEXT PRIMARY KEY, author TEXT NOT NULL, current_revision TEXT NOT NULL, accepted_revision TEXT, reviewer TEXT)")
        db.execute("CREATE TABLE IF NOT EXISTS outbox (id TEXT PRIMARY KEY, action_id TEXT NOT NULL, revision TEXT NOT NULL, done INTEGER NOT NULL DEFAULT 0)")


def request(port, method, route, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{route}",
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=3) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read())


def serve(base, port):
    init(base)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, _format, *_args):
            pass

        def reply(self, status, body):
            payload = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            if self.path == "/assign":
                with connect(base / "remote.sqlite") as db:
                    if body["mode"] == "cooperative":
                        action_id = body["action_id"]
                        db.execute("INSERT OR IGNORE INTO cooperative VALUES (?, ?)", (action_id, f"task-{action_id}"))
                        task_id = db.execute("SELECT task_id FROM cooperative WHERE action_id=?", (action_id,)).fetchone()[0]
                    else:
                        cursor = db.execute("INSERT INTO opaque(action_id) VALUES (?)", (body["action_id"],))
                        task_id = f"opaque-{cursor.lastrowid}"
                self.reply(200, {"task_id": task_id})
            elif self.path == "/engine-complete":
                with connect(base / "engine.sqlite") as db:
                    db.execute("INSERT OR IGNORE INTO commands VALUES (?, ?, ?)", (body["id"], body["action_id"], body["revision"]))
                self.reply(200, {"recorded": True})
            else:
                self.reply(404, {"error": "unknown route"})

        def do_GET(self):
            parsed = urllib.parse.urlsplit(self.path)
            if parsed.path == "/action":
                params = urllib.parse.parse_qs(parsed.query)
                if params.get("mode") != ["cooperative"]:
                    self.reply(405, {"error": "outcome discovery unsupported"})
                    return
                with connect(base / "remote.sqlite") as db:
                    row = db.execute("SELECT task_id FROM cooperative WHERE action_id=?", (params["action_id"][0],)).fetchone()
                self.reply(200 if row else 404, {"task_id": row[0]} if row else {"error": "unknown action"})
            elif parsed.path == "/stats":
                with connect(base / "remote.sqlite") as remote, connect(base / "engine.sqlite") as engine:
                    body = {
                        "cooperative_effects": remote.execute("SELECT COUNT(*) FROM cooperative").fetchone()[0],
                        "opaque_effects": remote.execute("SELECT COUNT(*) FROM opaque").fetchone()[0],
                        "engine_continuations": engine.execute("SELECT COUNT(*) FROM commands").fetchone()[0],
                    }
                self.reply(200, body)
            else:
                self.reply(404, {"error": "unknown route"})

    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


def submit(base, port, action_id, mode, drop):
    with connect(base / "product.sqlite") as db:
        db.execute("INSERT INTO assignments VALUES (?, ?, 'intent', NULL)", (action_id, mode))
    status, body = request(port, "POST", "/assign", {"action_id": action_id, "mode": mode})
    assert status == 200
    if drop:
        os._exit(23)  # The receiver committed, but the caller did not retain its task ID.
    with connect(base / "product.sqlite") as db:
        db.execute("UPDATE assignments SET status='submitted', task_id=? WHERE action_id=?", (body["task_id"], action_id))


def recover(base, port, action_id):
    with connect(base / "product.sqlite") as db:
        mode, status, task_id = db.execute("SELECT mode, status, task_id FROM assignments WHERE action_id=?", (action_id,)).fetchone()
        assert status == "intent" and task_id is None
        route = "/action?" + urllib.parse.urlencode({"mode": mode, "action_id": action_id})
        code, body = request(port, "GET", route)
        if code == 200:
            db.execute("UPDATE assignments SET status='submitted', task_id=? WHERE action_id=?", (body["task_id"], action_id))
        else:
            db.execute("UPDATE assignments SET status='unknown' WHERE action_id=?", (action_id,))
        return "submitted" if code == 200 else "unknown"


def prepare_acceptance(base, crash):
    with connect(base / "product.sqlite") as db:
        db.execute("INSERT INTO artifacts VALUES ('co-op-1', 'writer-1', 'r2', NULL, NULL)")
        def approve(revision, reviewer):
            artifact = db.execute("SELECT author, current_revision, accepted_revision FROM artifacts WHERE action_id='co-op-1'").fetchone()
            if revision != artifact[1] or reviewer == artifact[0] or artifact[2] is not None:
                return False
            db.execute("UPDATE artifacts SET accepted_revision=?, reviewer=? WHERE action_id='co-op-1'", (revision, reviewer))
            db.execute("INSERT INTO outbox(id, action_id, revision) VALUES ('complete-co-op-1-r2', 'co-op-1', 'r2')")
            return True
        assert not approve("r1", "quality-1"), "stale r1 was accepted"
        assert not approve("r2", "writer-1"), "author self-approval was accepted"
        assert approve("r2", "quality-1")
        assert not approve("r2", "quality-1"), "duplicate approval was accepted"
    if crash:
        os._exit(24)  # Product acceptance committed; engine not yet updated.


def drain(base, port, drop):
    with connect(base / "product.sqlite") as db:
        rows = db.execute("SELECT id, action_id, revision FROM outbox WHERE done=0").fetchall()
        for command_id, action_id, revision in rows:
            code, _ = request(port, "POST", "/engine-complete", {"id": command_id, "action_id": action_id, "revision": revision})
            assert code == 200
            if drop:
                os._exit(25)  # Engine committed, but acknowledgement did not reach product state.
            db.execute("UPDATE outbox SET done=1 WHERE id=?", (command_id,))


def test(base):
    if base.exists() and any(base.iterdir()):
        raise SystemExit(f"Refusing to overwrite evidence directory: {base}")
    init(base)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    proc = subprocess.Popen([sys.executable, __file__, "serve", str(base), str(port)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    def child(*args, expected=0):
        result = subprocess.run([sys.executable, __file__, *map(str, args)], capture_output=True, text=True)
        assert result.returncode == expected, f"{args}: exit {result.returncode}: {result.stderr}"
        return result.stdout.strip()
    try:
        for _ in range(50):
            try:
                request(port, "GET", "/stats")
                break
            except urllib.error.URLError:
                time.sleep(0.1)
        else:
            raise AssertionError("fixture server did not start")
        child("submit", base, port, "co-op-1", "cooperative", "drop", expected=23)
        assert child("recover", base, port, "co-op-1") == "submitted"
        duplicate_code, duplicate = request(port, "POST", "/assign", {"action_id": "co-op-1", "mode": "cooperative"})
        assert duplicate_code == 200 and duplicate["task_id"] == "task-co-op-1"
        child("submit", base, port, "opaque-1", "opaque", "drop", expected=23)
        assert child("recover", base, port, "opaque-1") == "unknown"
        child("accept", base, "crash", expected=24)
        child("drain", base, port, "drop", expected=25)
        child("drain", base, port, "keep")
        code, stats = request(port, "GET", "/stats")
        assert code == 200 and stats == {"cooperative_effects": 1, "opaque_effects": 1, "engine_continuations": 1}, stats
        with connect(base / "product.sqlite") as db:
            assert db.execute("SELECT status,task_id FROM assignments WHERE action_id='co-op-1'").fetchone() == ("submitted", "task-co-op-1")
            assert db.execute("SELECT status,task_id FROM assignments WHERE action_id='opaque-1'").fetchone() == ("unknown", None)
            assert db.execute("SELECT accepted_revision,reviewer FROM artifacts WHERE action_id='co-op-1'").fetchone() == ("r2", "quality-1")
            assert db.execute("SELECT done FROM outbox").fetchone() == (1,)
        evidence = {"result": "PASS", "fixture": "custom HTTP, not A2A", "engine": "stub HTTP endpoint, not candidate runtime", "process_restarts": 4, "remote_and_engine_counts": stats, "product_status": {"cooperative": "submitted", "opaque": "unknown", "accepted_revision": "r2", "outbox": "done"}, "evidence_dir": str(base)}
        (base / "result.json").write_text(json.dumps(evidence, indent=2) + "\n")
        print(json.dumps(evidence, indent=2))
    finally:
        proc.terminate()
        proc.wait(timeout=5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["serve", "submit", "recover", "accept", "drain", "test"])
    parser.add_argument("rest", nargs="*")
    args = parser.parse_args()
    if args.command == "serve":
        serve(Path(args.rest[0]), int(args.rest[1]))
    elif args.command == "submit":
        submit(Path(args.rest[0]), int(args.rest[1]), args.rest[2], args.rest[3], args.rest[4] == "drop")
    elif args.command == "recover":
        print(recover(Path(args.rest[0]), int(args.rest[1]), args.rest[2]))
    elif args.command == "accept":
        prepare_acceptance(Path(args.rest[0]), args.rest[1] == "crash")
    elif args.command == "drain":
        drain(Path(args.rest[0]), int(args.rest[1]), args.rest[2] == "drop")
    elif args.command == "test":
        test(Path(args.rest[0]))
