"""Hand-off records: output contracts, keyed digests, streaming, Workflow composition."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import factory  # noqa: E402
import handoff  # noqa: E402

CANARY = "CANARY-content-never-observed"


def task(artifacts=None, message=None):
    value = {"id": "task-1", "status": {"state": "TASK_STATE_COMPLETED"}}
    if artifacts is not None:
        value["artifacts"] = artifacts
    if message is not None:
        value["status"]["message"] = {"role": "ROLE_AGENT", "messageId": "m1", "parts": message}
    return value


class HandoffModuleTests(unittest.TestCase):
    def setUp(self):
        self.home = Path(tempfile.mkdtemp(prefix="exo-handoff-unit-", dir=os.environ.get("TMPDIR", "/tmp")))
        self.key = handoff.instance_key(self.home / handoff.KEY_FILE_NAME)

    def test_instance_key_is_created_once_private_and_stable(self):
        path = self.home / handoff.KEY_FILE_NAME
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(handoff.instance_key(path), self.key)
        path.chmod(0o644)
        with self.assertRaises(PermissionError):
            handoff.instance_key(path)
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(handoff.instance_key())

    def test_items_describe_every_part_kind_without_content(self):
        raw = base64.b64encode(b"\x00\x01binary").decode()
        artifacts = [
            {"artifactId": "art-secret-id", "name": "secret name", "description": "secret description",
             "metadata": {"secret": True}, "parts": [{"text": CANARY, "mediaType": "text/Markdown; charset=utf-8"}]},
            {"artifactId": "a2", "parts": [{"data": {"answer": CANARY}, "mediaType": "application/json"}]},
            {"artifactId": "a3", "parts": [{"raw": raw, "mediaType": "application/pdf", "filename": "secret.pdf"}]},
            {"artifactId": "a4", "parts": [{"url": "https://example.invalid/secret", "mediaType": "x"}]},
            {"artifactId": "a5", "parts": [{"text": "a"}, {"url": "https://example.invalid/b"}]},
        ]
        items = handoff.produced_items(task(artifacts), "artifacts", key=self.key,
                                       ready_at="2026-10-07T12:00:00.000Z")
        self.assertEqual([i["item_index"] for i in items], [0, 1, 2, 3, 4])
        self.assertEqual([i["part_kinds"] for i in items], [["text"], ["data"], ["raw"], ["url"], ["text", "url"]])
        self.assertEqual([i["media_type"] for i in items],
                         ["text/markdown", "application/json", "application/pdf", None, None])
        self.assertEqual([i["byte_length"] for i in items],
                         [len(CANARY), len(json.dumps({"answer": CANARY}, separators=(",", ":"))), 8, None, None])
        payload = json.dumps([["text", CANARY]], separators=(",", ":")).encode()
        self.assertEqual(items[0]["digest"], hmac.new(self.key, payload, hashlib.sha256).hexdigest())
        encoded = json.dumps(items)
        for forbidden in (CANARY, "art-secret-id", "secret", "example.invalid", raw, "artifactId",
                          "filename", "metadata", "description", self.key.hex()):
            self.assertNotIn(forbidden, encoded)
        for item in items:
            self.assertEqual(set(item), {"item_index", "source", "part_kinds", "media_type",
                                         "byte_length", "ready_at", "digest"})

    def test_digests_are_keyed_per_instance(self):
        other = handoff.instance_key(Path(tempfile.mkdtemp(dir=self.home)) / "k")
        parts = [{"text": "yes"}]
        mine = handoff.describe_item(parts, index=0, source="artifact", ready_at="t", key=self.key)
        theirs = handoff.describe_item(parts, index=0, source="artifact", ready_at="t", key=other)
        self.assertNotEqual(mine["digest"], theirs["digest"])
        self.assertNotEqual(mine["digest"], hashlib.sha256(b"yes").hexdigest())

    def test_output_contracts(self):
        with self.assertRaises(handoff.OutputMissing) as missing:
            handoff.produced_items(task([]), "artifacts", key=self.key)
        self.assertEqual(missing.exception.reason, "output.missing")
        with self.assertRaises(handoff.OutputMissing):
            handoff.produced_items(task(None), "artifacts", key=None)
        items = handoff.produced_items(task([], message=[{"text": CANARY}]), "message", key=self.key)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["source"], "message")
        with self.assertRaises(handoff.OutputMissing):
            handoff.produced_items(task([{"parts": [{"text": "x"}]}]), "message", key=self.key)
        self.assertIsNone(handoff.produced_items(task([]), "none", key=self.key))
        self.assertIsNone(handoff.produced_items(task([{"parts": [{"text": "x"}]}]), "artifacts", key=None))

    def test_streamed_last_chunks_mark_item_ready(self):
        clock = iter(["2026-10-07T12:00:01.000Z", "2026-10-07T12:00:02.000Z"])
        events = [
            {"statusUpdate": {"status": {"state": "TASK_STATE_WORKING"}}},
            {"artifactUpdate": {"artifact": {"artifactId": "b", "parts": [{"text": CANARY, "mediaType": "text/plain"}]}}},
            {"artifactUpdate": {"artifact": {"artifactId": "a", "parts": [{"data": {"x": 1}}]}, "lastChunk": True}},
            {"artifactUpdate": {"append": True, "lastChunk": True,
                                "artifact": {"artifactId": "b", "parts": [{"url": "https://example.invalid"}]}}},
        ]
        ready, records = handoff.stream_ready_items(events, clock=lambda: next(clock))
        self.assertEqual(ready, {1: "2026-10-07T12:00:01.000Z", 0: "2026-10-07T12:00:02.000Z"})
        self.assertEqual(records[1], {"item_index": 0, "part_kinds": ["text", "url"],
                                      "media_type": "text/plain", "ready_at": "2026-10-07T12:00:02.000Z"})
        self.assertNotIn(CANARY, json.dumps(records))
        artifacts = [{"artifactId": "b", "parts": [{"text": "x"}]}, {"artifactId": "a", "parts": [{"data": {}}]}]
        items = handoff.produced_items(task(artifacts), "artifacts", key=self.key,
                                       ready_at="2026-10-07T12:00:03.000Z", streamed=ready)
        self.assertEqual([i["ready_at"] for i in items], ["2026-10-07T12:00:02.000Z", "2026-10-07T12:00:01.000Z"])


class WorkflowHandoffCompositionTests(unittest.IsolatedAsyncioTestCase):
    """Workflow code composes consumption from content-free records only."""

    async def test_consumption_names_upstream_handoffs_and_the_key_stays_in_activities(self):
        key_file = Path(tempfile.mkdtemp(prefix="exo-handoff-wf-", dir=os.environ.get("TMPDIR", "/tmp"))) / "k"
        key = handoff.instance_key(key_file)
        run = factory.FactoryRun(); run.run_id = "run"; run.definition_digest = "definition"
        run.run_inputs = {"question": "synthetic only"}
        run._explicit_assignment_bindings = True; run._handoff_records = True
        bindings = {"research": {"role": "capability", "url": "http://127.0.0.1:1", "identity": "research"},
                    "synthesis": {"role": "capability"}, "quality": {"role": "quality"},
                    "release": {"role": "release", "url": "http://127.0.0.1:1", "identity": "release",
                                "output": "none"}}
        branch = lambda name: dict(service="research", result_type=name, capability=name)
        document = {"start": "gather", "nodes": {
            "gather": dict(type="parallel", branches={"findings": branch("findings"), "risks": branch("risks")}, next="join"),
            "join": dict(type="join", branches=["findings", "risks"], next="draft"),
            "draft": dict(type="synthesize", service="synthesis", next="review"),
            "review": dict(type="quality", next="publish"),
            "publish": dict(type="release", service="release", next="done"), "done": dict(type="complete")}}
        closure = {"contracts": {name: {} for name in bindings}, "manifest": {"quality_policy_digest": "p"},
                   "quality_policy": {}}
        record = lambda handoff_id, digest: {"handoff_id": handoff_id, "handoff_revision": 1,
            "produced_at": "2026-10-07T12:00:00.000Z", "items": [{"item_index": 0, "digest": digest}]}
        calls = []

        async def execute(fn, value):
            calls.append((fn, value))
            if fn is factory.assign:
                return {"handoff": record(value["handoff_id"], value["instance"][0] * 64)}
            if fn is factory.typed_join:
                return {}
            if fn is factory.synthesize:
                return dict(revision="r1", sha256="c" * 64, content="{}", handoff=record("draft", "d" * 64))
            if fn is factory.review:
                return dict(task_id="quality-task", artifact=dict(accepted=True, reviewer="quality"))
            if fn is factory.release:
                return dict(receipt_id="synthetic")
            raise AssertionError("unexpected activity")
        with patch.object(factory, "verify_closure"), patch.object(factory, "_activity", side_effect=execute), \
                patch.object(factory.workflow, "uuid4", side_effect=[uuid4() for _ in range(10)]):
            result = await run.run_node(document, {"bindings": bindings, "evidence_packet": {}}, {"closure": closure})
        self.assertEqual(result["status"], "accepted")
        self.assertNotIn("handoff", result["artifact"], "the draft record never rides in the Workflow artifact")
        by_fn = {}
        for fn, value in calls:
            by_fn.setdefault(fn, []).append(value)
        self.assertEqual([v["handoff_id"] for v in by_fn[factory.assign]], ["gather.findings", "gather.risks"])
        self.assertEqual(by_fn[factory.synthesize][0]["consumes"],
                         [{"handoff_id": "gather.findings", "item_digests": ["f" * 64]},
                          {"handoff_id": "gather.risks", "item_digests": ["r" * 64]}])
        self.assertEqual((by_fn[factory.synthesize][0]["handoff_id"], by_fn[factory.synthesize][0]["handoff_revision"]),
                         ("draft", 1))
        draft = [{"handoff_id": "draft", "item_digests": ["d" * 64]}]
        self.assertEqual(by_fn[factory.review][0]["consumes"], draft)
        self.assertEqual(by_fn[factory.release][0]["consumes"], draft)
        self.assertEqual(by_fn[factory.release][0]["node"], "publish")
        # Every Workflow-recorded Activity input (what Temporal history holds) is key-free.
        encoded = json.dumps([value for _, value in calls], default=str)
        self.assertNotIn(key.hex(), encoded)
        self.assertNotIn(str(key_file), encoded)
        source = (ROOT / "src" / "factory.py").read_text()
        for name in ("instance_key", "KEY_ENV", "EXO_HANDOFF_KEY_FILE", "hmac"):
            self.assertNotIn(name, source)


if __name__ == "__main__":
    unittest.main()
