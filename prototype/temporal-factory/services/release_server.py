"""Durable local delivery receivers for participating and opaque-peer cases.

The participating receiver deduplicates a stable release ID and supports lookup.
The opaque receiver deliberately provides neither promise, so a lost reply must
remain unresolved at the caller. These are product-test HTTP extensions, not A2A.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from fixture import canonical, sha256_text


TOKEN = "Bearer fixture-token"


class Receiver:
    def __init__(self, state: Path, mode: str):
        state.mkdir(parents=True, exist_ok=True)
        self.database = state / "release.sqlite3"
        self.mode = mode
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS identity (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    id TEXT NOT NULL, mode TEXT NOT NULL, incarnation INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS releases (
                    release_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
                    run_id TEXT NOT NULL, definition_digest TEXT NOT NULL,
                    revision TEXT NOT NULL, sha256 TEXT NOT NULL,
                    attempts INTEGER NOT NULL, accepted_effect_count INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS opaque_effects (
                    id INTEGER PRIMARY KEY, fingerprint TEXT NOT NULL,
                    run_id TEXT NOT NULL);
            """)
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM identity WHERE singleton=1").fetchone()
            if row:
                if row["mode"] != mode:
                    raise ValueError("receiver state mode mismatch")
                self.identity = row["id"]
                self.incarnation = row["incarnation"] + 1
                db.execute("UPDATE identity SET incarnation=? WHERE singleton=1",
                           (self.incarnation,))
            else:
                self.identity = str(uuid4())
                self.incarnation = 1
                db.execute("INSERT INTO identity VALUES (1, ?, ?, 1)",
                           (self.identity, mode))

    def connect(self):
        db = sqlite3.connect(self.database, timeout=15)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        return db

    @staticmethod
    def checked(body):
        for field in ("run_id", "definition_digest", "revision", "sha256", "content"):
            if not isinstance(body.get(field), str) or not body[field]:
                raise ValueError("missing " + field)
        if body["sha256"] != sha256_text(body["content"]):
            raise ValueError("release content digest mismatch")

    def release(self, body):
        self.checked(body)
        release_id = body.get("release_id")
        if not isinstance(release_id, str) or not release_id:
            raise ValueError("missing release_id")
        stable = {key: body[key] for key in
                  ("release_id", "run_id", "definition_digest", "revision", "sha256", "content")}
        fingerprint = hashlib.sha256(canonical(stable).encode()).hexdigest()
        new = False
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM releases WHERE release_id=?", (release_id,)).fetchone()
            if row:
                if row["fingerprint"] != fingerprint:
                    raise ValueError("release ID reused with different payload")
                db.execute("UPDATE releases SET attempts=attempts+1 WHERE release_id=?",
                           (release_id,))
            else:
                new = True
                db.execute("INSERT INTO releases VALUES (?, ?, ?, ?, ?, ?, 1, 1)",
                           (release_id, fingerprint, body["run_id"],
                            body["definition_digest"], body["revision"], body["sha256"]))
        if new and body.get("drop_ack") is True:
            os._exit(23)
        return self.receipt(release_id)

    def receipt(self, release_id):
        with self.connect() as db:
            row = db.execute("SELECT * FROM releases WHERE release_id=?",
                             (release_id,)).fetchone()
        return None if row is None else {key: row[key] for key in (
            "release_id", "run_id", "definition_digest", "revision", "sha256",
            "attempts", "accepted_effect_count")}

    def opaque_submit(self, body):
        self.checked(body)
        stable = {key: body[key] for key in
                  ("run_id", "definition_digest", "revision", "sha256", "content")}
        fingerprint = hashlib.sha256(canonical(stable).encode()).hexdigest()
        with self.connect() as db:
            db.execute("INSERT INTO opaque_effects (fingerprint, run_id) VALUES (?, ?)",
                       (fingerprint, body["run_id"]))
        if body.get("drop_ack") is True:
            os._exit(23)
        return {"status": "received"}  # No stable receipt or discovery contract.


def handler_for(receiver: Receiver):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def respond(self, status, value):
            payload = canonical(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def authorized(self):
            if self.headers.get("Authorization") != TOKEN:
                self.respond(401, {"error": "fixture authentication required"})
                return False
            return True

        def do_GET(self):
            if self.path == "/health":
                self.respond(200, {"identity": receiver.identity,
                                   "incarnation": receiver.incarnation, "mode": receiver.mode})
                return
            if not self.authorized():
                return
            if receiver.mode == "participating" and self.path.startswith("/receipts/"):
                value = receiver.receipt(unquote(self.path[len("/receipts/"):]))
                self.respond(200, value) if value else self.respond(404, {"error": "unknown receipt"})
                return
            self.respond(404, {"error": "no discovery endpoint"})

        def do_POST(self):
            if not self.authorized():
                return
            route = "/release" if receiver.mode == "participating" else "/submit"
            if self.path != route:
                self.respond(404, {"error": "unknown route"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length <= 0 or length > 1_000_000:
                    raise ValueError("invalid request size")
                body = json.loads(self.rfile.read(length))
                value = receiver.release(body) if receiver.mode == "participating" else receiver.opaque_submit(body)
            except (ValueError, json.JSONDecodeError) as error:
                self.respond(409 if "reused" in str(error) else 400, {"error": str(error)})
                return
            self.respond(200, value)

    return Handler


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--mode", choices=["participating", "opaque"], required=True)
    args = parser.parse_args()
    server = ThreadingHTTPServer(("127.0.0.1", args.port), handler_for(Receiver(args.state, args.mode)))
    server.serve_forever()
