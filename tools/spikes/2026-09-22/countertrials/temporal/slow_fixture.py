#!/usr/bin/env python3
"""Deterministic, test-only HTTP Activity that holds a small overlap window."""
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        time.sleep(3)
        body = json.dumps({'ok': True}).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        return


ThreadingHTTPServer(('127.0.0.1', 27310), Handler).serve_forever()
