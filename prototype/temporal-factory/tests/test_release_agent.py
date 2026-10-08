"""The release receiver is an ordinary A2A v1 agent with output none.

Its only routes are the JSON-RPC endpoint and the Agent Card; it deduplicates by
A2A ``messageId``; its receipt is a data-part artifact over the exact delivered
bytes; and nothing factory-specific reaches its wire contract or its state.
"""
from __future__ import annotations

import base64
import hashlib
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services"))
sys.path.insert(0, str(ROOT / "src"))
import a2a_v1  # noqa: E402
import release_server  # noqa: E402


TOKEN = {"Authorization": "Bearer fixture-token"}
INVALID_PARAMS, TASK_NOT_FOUND, METHOD_NOT_FOUND = -32602, -32001, -32601
REPORT = json.dumps({"markdown": "# Report\n\nAccepted bytes ✓", "revision": "r1"},
                    sort_keys=True, separators=(",", ":"), ensure_ascii=False)
FACTORY_WORDS = ("run_id", "assignment", "attempt", "action_id", "definition_digest",
                 "factory", "release_id", "revision")


def send(parts, *, message_id=None, request_id="send"):
    message = a2a_v1.user_message(parts, message_id=message_id or str(uuid4()))
    return a2a_v1.rpc(a2a_v1.SEND_MESSAGE, a2a_v1.send_params(message), request_id)


class ReleaseAgentTests(unittest.TestCase):
    def setUp(self):
        self.state = Path(tempfile.mkdtemp(prefix="exo-release-agent-", dir="/tmp"))

    def client(self, mode="participating", port=46310):
        return TestClient(release_server.create_app(self.state / mode, port, mode))

    def post(self, client, body):
        response = client.post("/", json=body, headers={**TOKEN, **a2a_v1.headers()})
        self.assertEqual(response.status_code, 200)
        return response.json()

    def rows(self, mode="participating"):
        with sqlite3.connect(self.state / mode / "release.sqlite3") as db:
            db.row_factory = sqlite3.Row
            return [dict(row) for row in db.execute("SELECT * FROM deliveries ORDER BY rowid")]

    def test_card_is_v1_only_with_a_release_skill_and_no_extension(self):
        with self.client() as client:
            card = client.get("/.well-known/agent-card.json").json()
        self.assertEqual(card["supportedInterfaces"], [{
            "url": "http://127.0.0.1:46310/", "protocolBinding": "JSONRPC",
            "protocolVersion": "1.0"}])
        for legacy in ("url", "protocolVersion", "preferredTransport", "additionalInterfaces"):
            self.assertNotIn(legacy, card)
        self.assertEqual(a2a_v1.card_url(card), "http://127.0.0.1:46310/")
        self.assertNotIn("extensions", card.get("capabilities") or {})
        self.assertEqual([skill["id"] for skill in card["skills"]], [release_server.SKILL_ID])
        self.assertIn("message-id-idempotent", card["skills"][0]["tags"])
        lowered = json.dumps(card).lower()
        self.assertFalse([word for word in FACTORY_WORDS if word in lowered])

    def test_only_a2a_routes_exist(self):
        app = release_server.create_app(self.state / "routes", 46311, "participating")
        served = sorted((route.path, tuple(sorted(route.methods or ())))
                        for route in app.router.routes)
        self.assertEqual(served, [("/", ("POST",)),
                                  ("/.well-known/agent-card.json", ("GET", "HEAD"))])
        with TestClient(app) as client:
            for path in ("/health", "/release", "/receipts/x", "/submit", "/contract",
                         "/docs", "/openapi.json", "/_test/effects"):
                with self.subTest(path=path):
                    self.assertIn(client.get(path, headers=TOKEN).status_code, {404, 405})
                    self.assertIn(client.post(path, headers=TOKEN, json={}).status_code,
                                  {404, 405})
            legacy = client.post("/", headers={**TOKEN, **a2a_v1.headers()}, json={
                "jsonrpc": "2.0", "id": "legacy", "method": "message/send",
                "params": {"message": {"role": "user", "messageId": "m",
                                       "parts": [{"kind": "text", "text": "x"}]}}})
            self.assertEqual(legacy.json()["error"]["code"], METHOD_NOT_FOUND)
            self.assertEqual(client.post("/", json=send([a2a_v1.text_part("x")])).status_code,
                             401)

    def test_receipt_digest_matches_the_exact_delivered_bytes(self):
        delivered = REPORT.encode("utf-8")
        with self.client() as client:
            task = self.post(client, send([a2a_v1.text_part(REPORT, "application/json")]))
        task = a2a_v1.require_task(task["result"])
        self.assertEqual(a2a_v1.task_state(task), "completed")
        self.assertEqual(len(task["artifacts"]), 1)
        artifact = task["artifacts"][0]
        receipt = a2a_v1.part_data(artifact["parts"][0])
        self.assertEqual(artifact["parts"][0]["mediaType"], "application/json")
        self.assertEqual(set(receipt), set(release_server.RECEIPT_FIELDS))
        self.assertEqual(receipt["sha256"], hashlib.sha256(delivered).hexdigest())
        self.assertEqual(receipt["byte_length"], len(delivered))
        self.assertEqual(receipt["media_type"], "application/json")
        self.assertEqual(receipt["outcome"], "delivered")
        self.assertEqual(artifact["artifactId"], receipt["receipt_id"])
        self.assertNotIn("metadata", task)
        row, = self.rows()
        self.assertEqual((row["sha256"], row["byte_length"], row["effect_count"]),
                         (receipt["sha256"], len(delivered), 1))
        # Raw bytes are delivered exactly too.
        raw = b"\x00\xffbinary report"
        with self.client() as client:
            task = a2a_v1.require_task(self.post(client, send([{
                "raw": base64.b64encode(raw).decode(), "mediaType": "application/pdf"}]))["result"])
        receipt = a2a_v1.part_data(task["artifacts"][0]["parts"][0])
        self.assertEqual((receipt["sha256"], receipt["byte_length"]),
                         (hashlib.sha256(raw).hexdigest(), len(raw)))

    def test_message_id_is_the_idempotency_key(self):
        message_id = str(uuid4())
        body = send([a2a_v1.text_part(REPORT, "application/json")], message_id=message_id)
        with self.client() as client:
            first = a2a_v1.require_task(self.post(client, body)["result"])
            again = a2a_v1.require_task(self.post(client, {**body, "id": "resend"})["result"])
            fetched = self.post(client, a2a_v1.rpc(a2a_v1.GET_TASK, {"id": first["id"]}))
        self.assertEqual(again, first)
        self.assertEqual(a2a_v1.normalize_numbers(fetched["result"]), first)
        row, = self.rows()
        self.assertEqual((row["task_id"], row["message_id"], row["sends"], row["effect_count"]),
                         (first["id"], message_id, 2, 1))

    def test_message_id_reuse_with_different_content_is_rejected_without_effect(self):
        message_id = str(uuid4())
        with self.client() as client:
            first = a2a_v1.require_task(self.post(client, send(
                [a2a_v1.text_part(REPORT, "application/json")], message_id=message_id))["result"])
            for parts in ([a2a_v1.text_part(REPORT + " ", "application/json")],
                          [a2a_v1.text_part(REPORT, "text/plain")]):
                with self.subTest(parts=parts):
                    reply = self.post(client, send(parts, message_id=message_id))
                    self.assertEqual(reply["error"]["code"], INVALID_PARAMS)
                    self.assertIn("messageId reused", reply["error"]["message"])
        row, = self.rows()
        self.assertEqual((row["task_id"], row["sends"], row["effect_count"]),
                         (first["id"], 1, 1))

    def test_undeliverable_messages_become_rejected_tasks_with_reasons(self):
        cases = {
            "two parts": [a2a_v1.text_part("a", "text/plain"), a2a_v1.text_part("b", "text/plain")],
            "no media type": [{"text": REPORT}],
            "url part": [{"url": "https://example.invalid/report", "mediaType": "text/plain"}],
            "oversize": [a2a_v1.text_part("x" * (release_server.MAX_BYTES + 1), "text/plain")],
        }
        with self.client() as client:
            for name, parts in cases.items():
                with self.subTest(case=name):
                    task = a2a_v1.require_task(self.post(client, send(parts))["result"])
                    self.assertEqual(a2a_v1.task_state(task), "rejected")
                    self.assertNotIn("artifacts", task)
                    status = a2a_v1.part_text(task["status"]["message"]["parts"][0])
                    self.assertTrue(status.startswith("Delivery rejected: "), status)
        self.assertTrue(all(row["effect_count"] == 0 for row in self.rows()))

    def test_unknown_task_is_not_found(self):
        with self.client() as client:
            reply = self.post(client, a2a_v1.rpc(a2a_v1.GET_TASK, {"id": str(uuid4())}))
        self.assertEqual(reply["error"]["code"], TASK_NOT_FOUND)

    def test_state_survives_restart_and_holds_no_factory_identifiers(self):
        message_id = str(uuid4())
        body = send([a2a_v1.text_part(REPORT, "application/json")], message_id=message_id)
        with self.client() as client:
            first = a2a_v1.require_task(self.post(client, body)["result"])
        with self.client() as client:
            again = a2a_v1.require_task(self.post(client, body)["result"])
        self.assertEqual(again, first)
        with sqlite3.connect(self.state / "participating" / "release.sqlite3") as db:
            columns = {row[1] for table in ("identity", "deliveries")
                       for row in db.execute(f"PRAGMA table_info({table})")}
        self.assertFalse([column for column in columns
                          if any(word in column for word in FACTORY_WORDS)])

    def test_opaque_mode_issues_no_receipt_and_keeps_no_task(self):
        message_id = str(uuid4())
        body = send([a2a_v1.text_part(REPORT, "application/json")], message_id=message_id)
        with self.client("opaque", 46312) as client:
            card = client.get("/.well-known/agent-card.json").json()
            first = a2a_v1.require_task(self.post(client, body)["result"])
            second = a2a_v1.require_task(self.post(client, body)["result"])
            lookup = self.post(client, a2a_v1.rpc(a2a_v1.GET_TASK, {"id": first["id"]}))
        self.assertNotIn("message-id-idempotent", card["skills"][0]["tags"])
        self.assertEqual(a2a_v1.task_state(first), "completed")
        self.assertNotIn("artifacts", first)
        self.assertNotEqual(first["id"], second["id"])
        self.assertIn("no receipt", a2a_v1.part_text(first["status"]["message"]["parts"][0]))
        self.assertEqual(lookup["error"]["code"], TASK_NOT_FOUND)
        self.assertEqual(sum(row["effect_count"] for row in self.rows("opaque")), 2)

    def test_mode_is_durable(self):
        release_server.Store(self.state / "mode", "participating")
        with self.assertRaises(ValueError):
            release_server.Store(self.state / "mode", "opaque")


if __name__ == "__main__":
    unittest.main()
