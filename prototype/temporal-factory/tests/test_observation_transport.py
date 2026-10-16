"""Synthetic WebSocket protocol tests; no live runtime or inference evidence."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import socket
import sqlite3
import sys
import struct
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlsplit

import yaml
from fastapi import FastAPI
from fastapi.testclient import TestClient
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tests"))

from observation import FactoryObservation  # noqa: E402
import observation_transport  # noqa: E402
from observation_source import RuntimeObservationSource, _canonical  # noqa: E402
from test_observation import (ControlledSource, project_source_record, record,
                              seed_records)  # noqa: E402


_WEBSOCKET_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def _websocket_frame(opcode: int, payload: bytes, *, mask: bool) -> bytes:
    first = 0x80 | opcode
    size = len(payload)
    if size < 126:
        header = bytes((first, (0x80 if mask else 0) | size))
    elif size < (1 << 16):
        header = bytes((first, (0x80 if mask else 0) | 126)) + struct.pack("!H", size)
    else:
        header = bytes((first, (0x80 if mask else 0) | 127)) + struct.pack("!Q", size)
    if not mask:
        return header + payload
    key = os.urandom(4)
    masked = bytes(value ^ key[index % 4] for index, value in enumerate(payload))
    return header + key + masked


class _RawWebSocketServer:
    """Minimal RFC 6455 loopback endpoint that invokes the mounted ASGI app."""

    def __init__(self, app):
        self.app = app
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen()
        self.listener.settimeout(0.1)
        self.port = self.listener.getsockname()[1]
        self.stopping = threading.Event()
        self.lock = threading.Lock()
        self.connections: set[socket.socket] = set()
        self.threads: list[threading.Thread] = []
        self.errors: list[str] = []
        self.thread = threading.Thread(target=self._accept, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stopping.set()
        try:
            self.listener.close()
        except OSError:
            pass
        with self.lock:
            open_sockets = tuple(self.connections)
        for conn in open_sockets:
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                conn.close()
            except OSError:
                pass
        self.thread.join(timeout=2)
        for thread in self.threads:
            thread.join(timeout=2)

    def _accept(self):
        while not self.stopping.is_set():
            try:
                conn, address = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            with self.lock:
                self.connections.add(conn)
            thread = threading.Thread(target=self._serve, args=(conn, address), daemon=True)
            self.threads.append(thread)
            thread.start()

    def _serve(self, conn: socket.socket, address):
        try:
            conn.settimeout(2)
            raw = bytearray()
            while b"\r\n\r\n" not in raw:
                part = conn.recv(4096)
                if not part:
                    return
                raw.extend(part)
            head, initial = bytes(raw).split(b"\r\n\r\n", 1)
            lines = head.split(b"\r\n")
            method, target, _ = lines[0].decode("ascii").split(" ", 2)
            headers = {}
            for line in lines[1:]:
                key, _, value = line.partition(b":")
                headers[key.decode("ascii").strip().lower()] = value.decode("latin-1").strip()
            if method != "GET" or "sec-websocket-key" not in headers:
                conn.sendall(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
                return
            parsed = urlsplit(target)
            path = parsed.path
            scope = {
                "type": "websocket", "asgi": {"version": "3.0", "spec_version": "2.3"},
                "http_version": "1.1", "scheme": "ws", "server": ("127.0.0.1", self.port),
                "client": address, "root_path": "", "path": path,
                "raw_path": path.encode("ascii"), "query_string": parsed.query.encode("ascii"),
                "headers": [(key.encode("ascii"), value.encode("latin-1"))
                            for key, value in headers.items()],
                "subprotocols": [], "state": {},
            }
            conn.setblocking(False)
            buffered = bytearray(initial)
            connected = False
            accepted = False

            async def read_exact(size: int) -> bytes:
                loop = asyncio.get_running_loop()
                while len(buffered) < size:
                    part = await loop.sock_recv(conn, max(4096, size - len(buffered)))
                    if not part:
                        raise ConnectionError("WebSocket peer disconnected")
                    buffered.extend(part)
                value = bytes(buffered[:size])
                del buffered[:size]
                return value

            async def read_client_frame():
                first, second = await read_exact(2)
                opcode = first & 0x0f
                size = second & 0x7f
                if size == 126:
                    size = struct.unpack("!H", await read_exact(2))[0]
                elif size == 127:
                    size = struct.unpack("!Q", await read_exact(8))[0]
                if not second & 0x80:
                    raise ValueError("client WebSocket frame was not masked")
                key = await read_exact(4)
                payload = await read_exact(size)
                return opcode, bytes(value ^ key[index % 4]
                                     for index, value in enumerate(payload))

            async def send_frame(opcode: int, payload: bytes):
                await asyncio.get_running_loop().sock_sendall(
                    conn, _websocket_frame(opcode, payload, mask=False))

            async def receive():
                nonlocal connected
                if not connected:
                    connected = True
                    return {"type": "websocket.connect"}
                while True:
                    opcode, payload = await read_client_frame()
                    if opcode == 8:
                        code = struct.unpack("!H", payload[:2])[0] if len(payload) >= 2 else 1000
                        return {"type": "websocket.disconnect", "code": code}
                    if opcode == 9:
                        await send_frame(10, payload)
                        continue
                    if opcode == 1:
                        return {"type": "websocket.receive", "text": payload.decode("utf-8")}
                    if opcode == 2:
                        return {"type": "websocket.receive", "bytes": payload}

            async def send(message):
                nonlocal accepted
                kind = message["type"]
                loop = asyncio.get_running_loop()
                if kind == "websocket.accept":
                    key = headers["sec-websocket-key"]
                    accept = base64.b64encode(hashlib.sha1(
                        (key + _WEBSOCKET_GUID).encode("ascii")).digest()).decode("ascii")
                    response = (
                        "HTTP/1.1 101 Switching Protocols\r\n"
                        "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                        f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
                    ).encode("ascii")
                    await loop.sock_sendall(conn, response)
                    accepted = True
                elif kind == "websocket.send":
                    text = message.get("text")
                    payload = text.encode("utf-8") if text is not None else message.get("bytes", b"")
                    delay_ms = headers.get("x-test-delay-event")
                    if delay_ms and text:
                        try:
                            is_event = json.loads(text).get("op") == "event"
                        except (ValueError, AttributeError):
                            is_event = False
                        if is_event:
                            await asyncio.sleep(min(int(delay_ms), 1000) / 1000)
                    await send_frame(1 if text is not None else 2, payload)
                elif kind == "websocket.close":
                    if not accepted:
                        await loop.sock_sendall(conn, b"HTTP/1.1 403 Forbidden\r\n"
                                                b"Content-Length: 0\r\nConnection: close\r\n\r\n")
                    else:
                        code = message.get("code", 1000)
                        reason = message.get("reason", "").encode("utf-8")
                        await send_frame(8, struct.pack("!H", code) + reason)

            async def run_app():
                await self.app(scope, receive, send)

            asyncio.run(run_app())
        except (ConnectionError, OSError):
            pass
        except Exception as error:
            self.errors.append(type(error).__name__ + ": " + str(error))
        finally:
            try:
                conn.close()
            except OSError:
                pass
            with self.lock:
                self.connections.discard(conn)


class _RawWebSocketClient:
    def __init__(self, sock: socket.socket, status: int, buffer: bytes = b""):
        self.socket = sock
        self.status = status
        self.buffer = bytearray(buffer)
        self.close_code: int | None = None

    @classmethod
    def connect(cls, host: str, port: int, headers: dict[str, str] | None = None):
        sock = socket.create_connection((host, port), timeout=3)
        sock.settimeout(3)
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        request_headers = {
            "Host": f"{host}:{port}", "Upgrade": "websocket",
            "Connection": "Upgrade", "Sec-WebSocket-Key": key,
            "Sec-WebSocket-Version": "13", "Origin": f"http://{host}",
            **(headers or {}),
        }
        request = "GET /observations HTTP/1.1\r\n" + "".join(
            f"{name}: {value}\r\n" for name, value in request_headers.items()) + "\r\n"
        sock.sendall(request.encode("ascii"))
        response = bytearray()
        while b"\r\n\r\n" not in response:
            part = sock.recv(4096)
            if not part:
                break
            response.extend(part)
        head, _, rest = bytes(response).partition(b"\r\n\r\n")
        status = int(head.split(b" ", 2)[1]) if head else 0
        client = cls(sock, status, rest)
        if status == 101:
            response_headers = {}
            for line in head.split(b"\r\n")[1:]:
                name, _, value = line.partition(b":")
                response_headers[name.decode("ascii").lower()] = value.decode("latin-1").strip()
            expected = base64.b64encode(hashlib.sha1(
                (key + _WEBSOCKET_GUID).encode("ascii")).digest()).decode("ascii")
            if response_headers.get("sec-websocket-accept") != expected:
                client.close()
                raise AssertionError("invalid WebSocket handshake accept value")
        return client

    def send_json(self, value: dict):
        payload = json.dumps(value, separators=(",", ":")).encode("utf-8")
        self.socket.sendall(_websocket_frame(1, payload, mask=True))

    def _read_exact(self, size: int) -> bytes:
        while len(self.buffer) < size:
            part = self.socket.recv(max(4096, size - len(self.buffer)))
            if not part:
                raise ConnectionError("WebSocket peer disconnected")
            self.buffer.extend(part)
        value = bytes(self.buffer[:size])
        del self.buffer[:size]
        return value

    def receive(self, timeout: float = 3):
        self.socket.settimeout(timeout)
        first, second = self._read_exact(2)
        opcode = first & 0x0f
        size = second & 0x7f
        if size == 126:
            size = struct.unpack("!H", self._read_exact(2))[0]
        elif size == 127:
            size = struct.unpack("!Q", self._read_exact(8))[0]
        payload = self._read_exact(size)
        if opcode == 8:
            code = struct.unpack("!H", payload[:2])[0] if len(payload) >= 2 else 1000
            self.close_code = code
            return {"$close_code": code,
                    "$close_reason": payload[2:].decode("utf-8", errors="replace")}
        if opcode == 9:
            self.socket.sendall(_websocket_frame(10, payload, mask=True))
            return self.receive(timeout)
        if opcode != 1:
            raise AssertionError(f"unexpected WebSocket opcode {opcode}")
        return json.loads(payload.decode("utf-8"))

    def receive_json(self, timeout: float = 3):
        value = self.receive(timeout)
        if "$close_code" in value:
            raise ConnectionError(f"WebSocket closed with code {value['$close_code']}")
        return value

    def close(self):
        if self.status == 101:
            try:
                self.socket.sendall(_websocket_frame(8, struct.pack("!H", 1000), mask=True))
            except OSError:
                pass
        try:
            self.socket.close()
        except OSError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class _SyntheticRuntimeDirector:
    """Runtime action seam with deterministic authority and no worker/provider."""

    identity = "factory-runtime-smoke"

    def __init__(self, instance: Path):
        self.database = instance / "director.sqlite3"
        self.database.parent.mkdir(parents=True, exist_ok=True)
        (instance / "instance.json").write_text("{}\n")
        catalog = instance / "catalog"
        catalog.mkdir()
        (catalog / "active-publication.json").write_text("{}\n")
        self.module = type("RuntimeModule", (), {})()
        closure = {"manifest_digest": "a" * 64,
                   "manifest": {"root_digest": "c" * 64, "services": {}}}
        publication = {"manifest_digest": "a" * 64, "package_digest": "b" * 64,
                       "build_id": "build-smoke", "label": "synthetic", "closure": closure}
        self.module.publications = type("Publications", (), {
            "active": lambda _self: publication})()
        self.module.catalog = catalog
        self.module.package = lambda _digest: {"root": {"nodes": {}}, "bindings": {}}
        self.perform_calls: list[tuple[dict, str, str]] = []
        with self.connect() as db:
            db.execute("CREATE TABLE runs (run_id TEXT, task_id TEXT, context_id TEXT, "
                       "package_digest TEXT, manifest_digest TEXT, build_id TEXT, label TEXT, "
                       "run_inputs_json TEXT, run_inputs_digest TEXT, authorized_actor TEXT, "
                       "input_authority_json TEXT, closed INTEGER, outcome_json TEXT)")
            db.execute("INSERT INTO runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                "run-smoke", "task-smoke", "context-smoke", "b" * 64, "a" * 64,
                "build-smoke", "synthetic", "{}", "c" * 64, "fixture-operator", "{}", 0, None))

    def connect(self):
        db = sqlite3.connect(self.database, timeout=5, isolation_level=None)
        db.row_factory = sqlite3.Row
        return db

    def task_binding(self, task_id: str):
        if task_id == "task-smoke":
            return "run-smoke", "context-smoke"
        return None

    def run_record(self, run_id: str):
        return {"run_id": run_id, "authorized_actor": "fixture-operator"}

    def inspect_bound_run(self, task_id: str):
        return {"phase": "awaiting-director", "current_revision": "revision-smoke",
                "current_sha256": "d" * 64, "permitted_actions": ["abort"]}

    def perform(self, command: dict, task_id: str, context_id: str):
        self.perform_calls.append((command, task_id, context_id))
        return {"lifecycle": "applied", "outcome": "abort-recorded"}


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.records = {"factory-a": seed_records("factory-a"),
                        "factory-b": seed_records("factory-b", "run-b")}
        self.source = ControlledSource(self.records)
        self.projection = FactoryObservation(self.source, Path(self.temp.name) / "transport.sqlite",
                                            retention_events=2)
        self.app = FastAPI()

        async def authenticate(websocket):
            self.assertEqual(websocket.url.path, "/observations")
            return self.source.allowed_principal

        self.authenticate = authenticate
        self.mount = observation_transport.install_observation_transport
        self.mount(self.app, self.projection, self.authenticate)
        self.client = TestClient(self.app)
        schema_dir = ROOT / "schemas" / "dashboard" / "v1"
        self.schemas = [json.loads(path.read_text()) for path in schema_dir.glob("*.schema.json")]
        registry = Registry()
        for schema in self.schemas:
            registry = registry.with_resource(schema["$id"], Resource.from_contents(schema))
        server_schema = next(schema for schema in self.schemas
                             if schema["$id"] == "urn:exomachina:dashboard:v1:server-message")
        self.server_validator = Draft202012Validator(server_schema, registry=registry)

    def tearDown(self):
        self.client.close()
        self.temp.cleanup()

    def append_state(self, factory_id="factory-a", run_id="run-a", source_id="next", state="waiting"):
        self.records[factory_id].append(record(
            factory_id, source_id, "com.exomachina.run.state_changed.v1", run_id=run_id,
            fields={"state": state, "phase": "execution"}))

    def assert_server_frame(self, frame):
        self.server_validator.validate(frame)

    def test_snapshot_resume_and_authoritative_event_frames(self):
        with self.client.websocket_connect("/observations") as socket:
            socket.send_json({"op": "subscribe", "factory_id": "factory-a"})
            snapshot_frame = socket.receive_json()
            self.assert_server_frame(snapshot_frame)
            self.assertEqual(snapshot_frame["op"], "snapshot")
            self.assertEqual(snapshot_frame["snapshot"]["schema_version"], 1)
            cursor = snapshot_frame["snapshot"]["cursor"]
            self.assertIsInstance(cursor, str)
            self.assertNotIn("factory_id", snapshot_frame["snapshot"])
            self.assertNotIn("prompt", json.dumps(snapshot_frame))

        self.append_state(source_id="resumed-state")
        with self.client.websocket_connect("/observations") as socket:
            socket.send_json({"op": "subscribe", "factory_id": "factory-a",
                              "after_cursor": cursor})
            resumed = socket.receive_json()
            event = socket.receive_json()
            self.assert_server_frame(resumed)
            self.assert_server_frame(event)
            self.assertEqual(resumed["op"], "resumed")
            self.assertEqual(resumed["after_cursor"], cursor)
            self.assertEqual(resumed["continuation_cursor"], cursor)
            self.assertEqual(event["op"], "event")
            self.assertEqual(event["event"]["type"], "com.exomachina.run.state_changed.v1")
            self.assertEqual(event["event"]["dataschema"], "urn:exomachina:dashboard:event:v1")
            self.assertIsInstance(event["cursor"], str)
            self.assertNotIn("token", json.dumps(event).lower())

    def test_resume_without_new_events_acknowledges_catchup_once_at_same_cursor(self):
        cursor = self.projection.snapshot(
            self.source.allowed_principal, "factory-a")["cursor"]
        with _RawWebSocketServer(self.app) as server:
            with _RawWebSocketClient.connect("127.0.0.1", server.port) as client:
                client.send_json({"op": "subscribe", "factory_id": "factory-a",
                                  "after_cursor": cursor})
                resumed = client.receive_json()
                checkpoint = client.receive_json()
                self.assertEqual(resumed["op"], "resumed")
                self.assertEqual(checkpoint, {"op": "checkpoint", "cursor": cursor})
                with self.assertRaises(socket.timeout):
                    client.receive_json(timeout=0.35)

    def test_resume_catchup_checkpoint_waits_until_all_pages_are_drained(self):
        cursor = self.projection.snapshot(
            self.source.allowed_principal, "factory-a")["cursor"]
        self.append_state(source_id="catchup-page-one")
        self.append_state(source_id="catchup-page-two")
        events_after = self.projection.events_after

        def one_event_page(principal, factory_id, after_cursor, *, run_id=None):
            return events_after(principal, factory_id, after_cursor,
                                run_id=run_id, limit=1)

        with patch.object(self.projection, "events_after", side_effect=one_event_page):
            with _RawWebSocketServer(self.app) as server:
                with _RawWebSocketClient.connect("127.0.0.1", server.port) as client:
                    client.send_json({"op": "subscribe", "factory_id": "factory-a",
                                      "after_cursor": cursor})
                    resumed = client.receive_json()
                    first = client.receive_json()
                    second = client.receive_json()
                    checkpoint = client.receive_json()
                    self.assertEqual(resumed["op"], "resumed")
                    self.assertEqual(first["op"], "event")
                    self.assertEqual(second["op"], "event")
                    self.assertEqual(checkpoint, {
                        "op": "checkpoint", "cursor": second["cursor"]})
                    with self.assertRaises(socket.timeout):
                        client.receive_json(timeout=0.35)

    def test_run_filter_checkpoint_advances_over_other_run(self):
        with self.client.websocket_connect("/observations") as socket:
            socket.send_json({"op": "subscribe", "factory_id": "factory-a", "run_id": "run-a"})
            snapshot = socket.receive_json()
            self.assertEqual(snapshot["op"], "snapshot")
            self.append_state(run_id="run-b", source_id="other-run-state")
            checkpoint = socket.receive_json()
            self.assert_server_frame(checkpoint)
            self.assertEqual(checkpoint["op"], "checkpoint")
            self.assertIsInstance(checkpoint["cursor"], str)
            self.append_state(run_id="run-a", source_id="selected-run-state")
            event = socket.receive_json()
            self.assertEqual(event["op"], "event")
            self.assertEqual(event["event"]["data"]["run_id"], "run-a")

    def test_retention_expiry_sends_resync_then_fresh_snapshot(self):
        with self.client.websocket_connect("/observations") as socket:
            socket.send_json({"op": "subscribe", "factory_id": "factory-a"})
            initial = socket.receive_json()
            old_cursor = initial["snapshot"]["cursor"]
        for index in range(3):
            self.append_state(source_id=f"expired-{index}")
        with self.client.websocket_connect("/observations") as socket:
            socket.send_json({"op": "subscribe", "factory_id": "factory-a",
                              "after_cursor": old_cursor})
            resync = socket.receive_json()
            fresh = socket.receive_json()
            self.assert_server_frame(resync)
            self.assert_server_frame(fresh)
            self.assertEqual(resync["op"], "resync_required")
            self.assertEqual(resync["reason"], "retention_expired")
            self.assertIsInstance(resync["minimum_cursor"], str)
            self.assertIsInstance(resync["latest_cursor"], str)
            self.assertEqual(fresh["op"], "snapshot")
            self.assertNotEqual(fresh["snapshot"]["cursor"], old_cursor)

    def test_cross_factory_cursor_is_rejected_by_transport(self):
        cursor_a = self.projection.snapshot(self.source.allowed_principal, "factory-a")["cursor"]
        with self.client.websocket_connect("/observations") as socket:
            socket.send_json({"op": "subscribe", "factory_id": "factory-b",
                              "after_cursor": cursor_a})
            error = socket.receive_json()
            self.assertEqual(error["op"], "error")
            self.assertEqual(error["code"], "invalid_request")

    def test_command_ack_means_received_only_and_principal_is_server_resolved(self):
        with self.client.websocket_connect("/observations") as socket:
            socket.send_json({"op": "subscribe", "factory_id": "factory-a"})
            self.assertEqual(socket.receive_json()["op"], "snapshot")
            socket.send_json({"op": "command", "factory_id": "factory-a",
                              "command_id": "command-1", "task_id": "task-run-a",
                              "context_id": "context-run-a", "action": "retry",
                              "expected_state": "waiting"})
            acknowledgement = socket.receive_json()
            self.assert_server_frame(acknowledgement)
            self.assertEqual(acknowledgement, {"op": "command_ack", "command_id": "command-1",
                                               "lifecycle": "received"})
            self.assertIs(self.source.command_calls[0][0], self.source.allowed_principal)

    def test_retry_ack_stays_received_then_replays_durable_terminal_event(self):
        outcome_record = record(
            "factory-a", "command:command-1:applied", "com.exomachina.command.outcome.v1",
            source_kind="command", run_id="run-a", top={"task_id": "task-run-a"},
            fields={"command_id": "command-1", "lifecycle": "applied",
                    "outcome": "abort-recorded", "resulting_state": "abort-recorded"})
        self.records["factory-a"].append(outcome_record)
        self.source.prior_outcomes["command-1"] = {
            "lifecycle": "applied", "outcome": "abort-recorded",
            "resulting_state": "abort-recorded"}
        expected = project_source_record(outcome_record, "factory-a")[3]
        with self.client.websocket_connect("/observations") as socket:
            socket.send_json({"op": "subscribe", "factory_id": "factory-a"})
            snapshot = socket.receive_json()
            self.assert_server_frame(snapshot)
            current_cursor = snapshot["snapshot"]["cursor"]
            socket.send_json({"op": "command", "factory_id": "factory-a",
                              "command_id": "command-1", "task_id": "task-run-a",
                              "context_id": "context-run-a", "action": "abort",
                              "expected_state": "waiting"})
            acknowledgement = socket.receive_json()
            replay = socket.receive_json()
            self.assert_server_frame(acknowledgement)
            self.assert_server_frame(replay)
            self.assertEqual(acknowledgement, {"op": "command_ack", "command_id": "command-1",
                                               "lifecycle": "received"})
            self.assertEqual(replay["op"], "event")
            self.assertEqual(replay["cursor"], current_cursor)
            self.assertEqual(replay["event"]["id"], expected["id"])
            self.assertEqual(replay["event"]["data"]["lifecycle"], "applied")

    def test_asyncapi_has_wire_path_without_deployment_host_or_port(self):
        asyncapi = yaml.safe_load((ROOT / "specs" / "dashboard-asyncapi.yaml").read_text())
        self.assertNotIn("servers", asyncapi)
        self.assertEqual(asyncapi["channels"]["observationSocket"]["address"], "/observations")
        self.assertIn("runtime configuration", asyncapi["info"]["description"])

    def test_slow_consumer_gets_resync_attempt_and_close_1013(self):
        class SlowSocket:
            def __init__(self):
                self.calls = 0
                self.frames = []
                self.closed = None

            async def send_json(self, message):
                self.calls += 1
                if self.calls == 1:
                    await asyncio.sleep(0.04)
                self.frames.append(message)

            async def close(self, *, code, reason):
                self.closed = (code, reason)

        slow = SlowSocket()

        async def send():
            with patch.object(observation_transport, "_SEND_TIMEOUT_SECONDS", 0.001):
                with self.assertRaises(observation_transport._SlowViewer):
                    await observation_transport._send(slow, {"op": "event"})

        asyncio.run(send())
        self.assertEqual(slow.frames[0]["op"], "resync_required")
        self.assertEqual(slow.frames[0]["reason"], "slow_consumer")
        self.assertEqual(slow.closed[0], 1013)

    def test_loopback_sockets_auth_snapshot_resume_and_retention_gap(self):
        source = ControlledSource({"factory-a": seed_records("factory-a")})
        projection = FactoryObservation(
            source, Path(self.temp.name) / "socket-resume.sqlite", retention_events=2)
        app = FastAPI()

        def authenticate(websocket):
            if websocket.headers.get("authorization") == "Bearer synthetic-observer":
                return source.allowed_principal
            return None

        observation_transport.install_observation_transport(app, projection, authenticate)
        with _RawWebSocketServer(app) as server:
            denied = _RawWebSocketClient.connect("127.0.0.1", server.port)
            self.assertEqual(denied.status, 403)
            self.assertEqual(source._offsets, {})
            denied.close()

            with _RawWebSocketClient.connect("127.0.0.1", server.port, {
                    "Authorization": "Bearer synthetic-observer"}) as client:
                self.assertEqual(client.status, 101)
                client.send_json({"op": "subscribe", "factory_id": "factory-a"})
                snapshot = client.receive_json()
                self.assertEqual(snapshot["op"], "snapshot")
                self.assertEqual(snapshot["snapshot"]["schema_version"], 1)
                old_cursor = snapshot["snapshot"]["cursor"]
                self.assertIsInstance(old_cursor, str)
                self.assertNotIn("run_inputs", json.dumps(snapshot))

            replay_record = record(
                "factory-a", "socket-resume", "com.exomachina.run.state_changed.v1",
                run_id="run-a", fields={"state": "waiting", "phase": "execution"})
            source.records_by_factory["factory-a"].append(replay_record)
            with _RawWebSocketClient.connect("127.0.0.1", server.port, {
                    "Authorization": "Bearer synthetic-observer"}) as client:
                client.send_json({"op": "subscribe", "factory_id": "factory-a",
                                  "after_cursor": old_cursor})
                resumed = client.receive_json()
                event = client.receive_json()
                self.assertEqual(resumed["op"], "resumed")
                self.assertEqual(resumed["after_cursor"], old_cursor)
                self.assertEqual(resumed["continuation_cursor"], old_cursor)
                self.assertEqual(event["op"], "event")
                self.assertNotEqual(event["cursor"], old_cursor)
                self.assertEqual(
                    event["event"]["id"],
                    project_source_record(replay_record, "factory-a")[3]["id"])
                self.assertEqual(event["event"]["data"]["state"], "waiting")

            for index in range(3):
                source.records_by_factory["factory-a"].append(record(
                    "factory-a", f"socket-gap-{index}",
                    "com.exomachina.run.state_changed.v1", run_id="run-a",
                    fields={"state": "waiting", "phase": "execution"}))
            with _RawWebSocketClient.connect("127.0.0.1", server.port, {
                    "Authorization": "Bearer synthetic-observer"}) as client:
                client.send_json({"op": "subscribe", "factory_id": "factory-a",
                                  "after_cursor": old_cursor})
                resync = client.receive_json()
                fresh = client.receive_json()
                self.assertEqual(resync["op"], "resync_required")
                self.assertEqual(resync["reason"], "retention_expired")
                self.assertEqual(fresh["op"], "snapshot")
                self.assertNotEqual(fresh["snapshot"]["cursor"], old_cursor)

    def test_loopback_filtered_checkpoint_and_source_duplicate_conflict(self):
        source = ControlledSource({"factory-a": seed_records("factory-a")})
        projection = FactoryObservation(
            source, Path(self.temp.name) / "socket-filter.sqlite", retention_events=20)
        app = FastAPI()
        principals = {"Bearer synthetic-filter": source.allowed_principal}
        observation_transport.install_observation_transport(
            app, projection, lambda ws: principals.get(ws.headers.get("authorization")))

        with _RawWebSocketServer(app) as server:
            unauthenticated = _RawWebSocketClient.connect("127.0.0.1", server.port)
            self.assertEqual(unauthenticated.status, 403)
            unauthenticated.close()

            with _RawWebSocketClient.connect("127.0.0.1", server.port, {
                    "Authorization": "Bearer synthetic-filter"}) as client:
                client.send_json({"op": "subscribe", "factory_id": "factory-a",
                                  "run_id": "run-a"})
                self.assertEqual(client.receive_json()["op"], "snapshot")
                source.records_by_factory["factory-a"].append(
                    seed_records("factory-a", "run-b")[2])
                checkpoint = client.receive_json()
                self.assertEqual(checkpoint["op"], "checkpoint")
                self.assertIsInstance(checkpoint["cursor"], str)
                source.records_by_factory["factory-a"].append(record(
                    "factory-a", "filtered-selected-run",
                    "com.exomachina.run.state_changed.v1", run_id="run-a",
                    fields={"state": "waiting", "phase": "execution"}))
                selected = client.receive_json()
                self.assertEqual(selected["op"], "event")
                self.assertEqual(selected["event"]["data"]["run_id"], "run-a")

        duplicate = record(
            "factory-a", "stable-source-duplicate",
            "com.exomachina.run.state_changed.v1", run_id="run-a",
            fields={"state": "waiting", "phase": "execution"})
        with _RawWebSocketServer(app) as server:
            with _RawWebSocketClient.connect("127.0.0.1", server.port, {
                    "Authorization": "Bearer synthetic-filter"}) as client:
                client.send_json({"op": "subscribe", "factory_id": "factory-a"})
                first_frame = client.receive_json()
                self.assertEqual(first_frame["op"], "snapshot", first_frame)
                source.records_by_factory["factory-a"].append(duplicate)
                first = client.receive_json()
                self.assertEqual(first["op"], "event")
                self.assertEqual(first["event"]["id"],
                                 project_source_record(duplicate, "factory-a")[3]["id"])
                with projection._connect() as db:
                    before = db.execute(
                        "SELECT COUNT(*) FROM observation_events WHERE factory_id=?",
                        ("factory-a",)).fetchone()[0]
                source.records_by_factory["factory-a"].append(dict(duplicate))
                with self.assertRaises(socket.timeout):
                    client.receive_json(timeout=0.35)
                with projection._connect() as db:
                    after = db.execute(
                        "SELECT COUNT(*) FROM observation_events WHERE factory_id=?",
                        ("factory-a",)).fetchone()[0]
                self.assertEqual(after, before)

                conflict = {**duplicate, "fields": {"state": "failed", "phase": "execution"}}
                source.records_by_factory["factory-a"].append(conflict)
                error = client.receive_json()
                closed = client.receive()
                self.assertEqual(error["op"], "error")
                self.assertEqual(error["code"], "observation_unavailable")
                self.assertEqual(closed["$close_code"], 1011)

    def test_authenticated_loopback_slow_viewer_does_not_affect_fast_viewer(self):
        """Synthetic sockets prove one bounded viewer cannot stall another."""
        source = ControlledSource({"factory-a": seed_records("factory-a")})
        projection = FactoryObservation(
            source, Path(self.temp.name) / "socket-viewer-isolation.sqlite",
            retention_events=20)
        app = FastAPI()
        principals = {
            "Bearer synthetic-slow": source.allowed_principal,
            "Bearer synthetic-fast": source.allowed_principal,
        }
        observation_transport.install_observation_transport(
            app, projection, lambda ws: principals.get(ws.headers.get("authorization")))

        with patch.object(observation_transport, "_SEND_TIMEOUT_SECONDS", 0.01):
            with _RawWebSocketServer(app) as server:
                with _RawWebSocketClient.connect("127.0.0.1", server.port, {
                        "Authorization": "Bearer synthetic-slow",
                        "x-test-delay-event": "200"}) as slow, \
                        _RawWebSocketClient.connect("127.0.0.1", server.port, {
                            "Authorization": "Bearer synthetic-fast"}) as fast:
                    for client, run_filter in ((slow, "run-a"), (fast, None)):
                        request = {"op": "subscribe", "factory_id": "factory-a"}
                        if run_filter is not None:
                            request["run_id"] = run_filter
                        client.send_json(request)
                        self.assertEqual(client.receive_json()["op"], "snapshot")

                    unrelated = record(
                        "factory-a", "viewer-isolation-other-run",
                        "com.exomachina.run.state_changed.v1", run_id="run-b",
                        fields={"state": "waiting", "phase": "execution"})
                    source.records_by_factory["factory-a"].append(unrelated)
                    checkpoint = slow.receive_json()
                    fast_unrelated = fast.receive_json()
                    self.assertEqual(checkpoint["op"], "checkpoint")
                    self.assertIsInstance(checkpoint["cursor"], str)
                    self.assertEqual(fast_unrelated["op"], "event")
                    self.assertEqual(fast_unrelated["event"]["data"]["run_id"], "run-b")

                    selected = record(
                        "factory-a", "viewer-isolation-selected-run",
                        "com.exomachina.run.state_changed.v1", run_id="run-a",
                        fields={"state": "waiting", "phase": "execution"})
                    source.records_by_factory["factory-a"].append(selected)
                    slow_resync = slow.receive_json()
                    slow_close = slow.receive()
                    fast_selected = fast.receive_json()

                    self.assertEqual(slow_resync["op"], "resync_required")
                    self.assertEqual(slow_resync["reason"], "slow_consumer")
                    self.assertEqual(slow_close["$close_code"], 1013)
                    self.assertEqual(fast_selected["op"], "event")
                    self.assertEqual(fast_selected["event"]["id"],
                                     project_source_record(selected, "factory-a")[3]["id"])
                    self.assertEqual(fast_selected["event"]["data"]["run_id"], "run-a")
                    self.assertNotIn("synthetic-fast", json.dumps(fast_selected))

    def test_loopback_runtime_command_lifecycle_duplicates_conflict_and_two_observers(self):
        instance = Path(self.temp.name) / "synthetic-instance"
        director = _SyntheticRuntimeDirector(instance)
        history = [{
            "event_id": 1, "event_type": "WORKFLOW_EXECUTION_STARTED",
            "time": "2026-10-03T12:00:00Z", "attributes": {"input": {
                "run": "run-smoke", "definition_digest": "c" * 64,
                "package_digest": "b" * 64,
                "closure": {"manifest_digest": "a" * 64,
                            "manifest": {"root_digest": "c" * 64,
                                         "interpreter": {"build_id": "build-smoke"},
                                         "services": {}}},
                "document": {"nodes": {"await-director": {
                    "type": "wait", "next": []}}},
                "director": {"identity": director.identity, "epoch": 1},
            }},
        }, {
            "event_id": 2, "event_type": "TIMER_STARTED",
            "time": "2026-10-03T12:00:01Z", "attributes": {"timeout_seconds": 60},
        }]
        source = RuntimeObservationSource(
            director, {"name": "Synthetic Loopback", "capability": {"id": "synthetic@1"}},
            instance / "runtime-observation.sqlite",
            history_reader=lambda: [("run-smoke", history)],
            refresh_interval_seconds=0)
        projection = FactoryObservation(source, instance / "observation.sqlite")
        command = {"command_id": "command-synthetic-1", "task_id": "task-smoke",
                   "context_id": "context-smoke", "action": "abort",
                   "expected_state": "input-required", "expected_revision": "revision-smoke",
                   "expected_sha256": "d" * 64}
        fingerprint = hashlib.sha256(_canonical(command).encode("utf-8")).hexdigest()
        with source._connect() as db:
            db.execute("INSERT INTO command_intents VALUES (?,?,?,?,?,?,?)",
                       (command["command_id"], fingerprint, "run-smoke", "task-smoke",
                        "context-smoke", "abort", "2026-10-03T12:00:00Z"))
        # A durable received record models a command interrupted before its
        # synthetic Director action; RuntimeObservationSource resumes it below.
        source._command_event(command, "run-smoke", "received")

        app = FastAPI()
        principals = {"Bearer synthetic-operator": "fixture-operator",
                      "Bearer synthetic-observer": "fixture-observer"}
        observation_transport.install_observation_transport(
            app, projection, lambda ws: principals.get(ws.headers.get("authorization")))
        with _RawWebSocketServer(app) as server:
            with _RawWebSocketClient.connect("127.0.0.1", server.port, {
                    "Authorization": "Bearer synthetic-operator"}) as operator, \
                    _RawWebSocketClient.connect("127.0.0.1", server.port, {
                        "Authorization": "Bearer synthetic-observer"}) as observer:
                for client in (operator, observer):
                    client.send_json({"op": "subscribe", "factory_id": director.identity})
                    first_frame = client.receive_json()
                    self.assertEqual(first_frame["op"], "snapshot", first_frame)
                    run = first_frame["snapshot"]["state"]["runs"][0]
                    self.assertEqual(run["id"], "run-smoke")
                    self.assertEqual(run["task"], {
                        "id": "task-smoke", "context_id": "context-smoke"})
                    self.assertEqual(run["status"]["state"], "input-required")
                    self.assertNotIn("run_inputs", json.dumps(first_frame))

                operator.send_json({"op": "command", "factory_id": director.identity,
                                    **command})
                ack = operator.receive_json()
                self.assertEqual(ack, {"op": "command_ack",
                                       "command_id": command["command_id"],
                                       "lifecycle": "received"})

                def terminal_frames(client):
                    result = []
                    while not result or result[-1]["event"]["data"]["lifecycle"] != "applied":
                        frame = client.receive_json()
                        if frame.get("op") == "event":
                            result.append(frame)
                    return result

                operator_events = terminal_frames(operator)
                observer_events = terminal_frames(observer)
                self.assertEqual([row["event"]["data"]["lifecycle"] for row in operator_events],
                                 ["validated", "applied"])
                self.assertEqual([row["event"]["id"] for row in observer_events],
                                 [row["event"]["id"] for row in operator_events])
                applied_id = operator_events[-1]["event"]["id"]

                operator.send_json({"op": "command", "factory_id": director.identity,
                                    **command})
                repeated_ack = operator.receive_json()
                replay = operator.receive_json()
                self.assertEqual(repeated_ack["lifecycle"], "received")
                self.assertEqual(replay["op"], "event")
                self.assertEqual(replay["event"]["id"], applied_id)
                self.assertEqual(replay["event"]["data"]["lifecycle"], "applied")
                with self.assertRaises(socket.timeout):
                    observer.receive_json(timeout=0.35)

                conflicting = {**command, "action": "retry"}
                operator.send_json({"op": "command", "factory_id": director.identity,
                                    **conflicting})
                conflict_ack = operator.receive_json()
                conflict_event = operator.receive_json()
                observer_conflict = observer.receive_json()
                self.assertEqual(conflict_ack["lifecycle"], "received")
                self.assertEqual(conflict_event["event"]["data"]["lifecycle"], "rejected")
                self.assertEqual(conflict_event["event"]["data"]["outcome"],
                                 "command-id-conflict")
                self.assertEqual(observer_conflict["event"]["id"],
                                 conflict_event["event"]["id"])
                self.assertNotEqual(conflict_event["event"]["id"], applied_id)

        self.assertEqual(len(director.perform_calls), 1)
        with source._connect() as db:
            persisted_rows = {row["source_id"]: json.loads(row["record_json"])
                              for row in db.execute(
                                  "SELECT source_id,record_json FROM source_records ORDER BY position")}
        command_rows = [row for source_id, row in persisted_rows.items()
                        if source_id.startswith("command:command-synthetic-1:")]
        self.assertEqual([row["fields"]["lifecycle"] for row in command_rows],
                         ["received", "validated", "applied", "rejected"])
        self.assertTrue(all(row["source_kind"] == "command"
                            and row["event_type"] == "com.exomachina.command.outcome.v1"
                            and row["run_id"] == "run-smoke"
                            and row["task_id"] == "task-smoke"
                            and row["context_id"] == "context-smoke"
                            for row in command_rows))
        self.assertNotIn("fixture-operator", json.dumps(command_rows))
        self.assertEqual(persisted_rows["run-smoke:1"]["event_type"],
                         "com.exomachina.run.created.v1")
        self.assertEqual(persisted_rows["run-smoke:2:director-wait"]["event_type"],
                         "com.exomachina.run.state_changed.v1")
        self.assertEqual(persisted_rows["run-smoke:1"]["task_id"], "task-smoke")
        self.assertEqual(persisted_rows["run-smoke:1"]["context_id"], "context-smoke")
        self.assertEqual(persisted_rows["run-smoke:2:director-wait"]["fields"]["state"],
                         "input-required")

    def test_loopback_slow_send_deadline_resyncs_and_closes(self):
        source = ControlledSource({"factory-a": seed_records("factory-a")})
        projection = FactoryObservation(
            source, Path(self.temp.name) / "socket-slow.sqlite", retention_events=20)
        app = FastAPI()
        observation_transport.install_observation_transport(
            app, projection, lambda _ws: source.allowed_principal)
        with patch.object(observation_transport, "_SEND_TIMEOUT_SECONDS", 0.01):
            with _RawWebSocketServer(app) as server:
                with _RawWebSocketClient.connect("127.0.0.1", server.port, {
                        "x-test-delay-event": "200"}) as client:
                    client.send_json({"op": "subscribe", "factory_id": "factory-a"})
                    self.assertEqual(client.receive_json()["op"], "snapshot")
                    source.records_by_factory["factory-a"].append(record(
                        "factory-a", "slow-client-event",
                        "com.exomachina.run.state_changed.v1", run_id="run-a",
                        fields={"state": "waiting", "phase": "execution"}))
                    resync = client.receive_json()
                    closed = client.receive()
                    self.assertEqual(resync["op"], "resync_required")
                    self.assertEqual(resync["reason"], "slow_consumer")
                    self.assertEqual(closed["$close_code"], 1013)


if __name__ == "__main__":
    unittest.main()
