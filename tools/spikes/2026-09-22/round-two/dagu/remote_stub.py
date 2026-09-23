"""Local receiver that commits once, then loses the first acknowledgement."""

import json
import socket
import sqlite3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


DB = Path("/tmp/exomachina-round-two-dagu/remote.sqlite")
with sqlite3.connect(DB) as db:
    db.execute("CREATE TABLE IF NOT EXISTS attempts (key TEXT NOT NULL)")
    db.execute("CREATE TABLE IF NOT EXISTS receipts (key TEXT PRIMARY KEY, body TEXT NOT NULL)")


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/submit":
            self.send_error(404)
            return
        key = self.headers.get("Idempotency-Key")
        if not key:
            self.send_error(400, "missing Idempotency-Key")
            return
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode()
        with sqlite3.connect(DB, timeout=5, isolation_level=None) as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("INSERT INTO attempts (key) VALUES (?)", (key,))
            inserted = db.execute(
                "INSERT OR IGNORE INTO receipts (key, body) VALUES (?, ?)", (key, body)
            ).rowcount == 1
            db.commit()
        if inserted:
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
            return
        payload = json.dumps({"key": key, "accepted": True, "alreadyAccepted": True}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


ThreadingHTTPServer(("127.0.0.1", 18418), Handler).serve_forever()
