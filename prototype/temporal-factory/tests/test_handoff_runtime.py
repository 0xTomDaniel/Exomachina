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


class ComposePartsTests(unittest.TestCase):
    """Before-dispatch composition (decision 9): brief, then items verbatim."""

    def setUp(self):
        self.key = b"k" * 32
        self.brief = {"text": '{"kind":"brief"}', "mediaType": "application/json"}
        self.first = [[{"text": "a", "mediaType": "text/markdown", "filename": "a.md"}],
                      [{"raw": "aGk=", "mediaType": "application/octet-stream"},
                       {"data": {"n": 1, "x": [True, None]}}]]
        self.second = [[{"url": "https://example.invalid/r", "mediaType": "text/plain"}]]
        self.upstream = [{"handoff_id": "gather.a", "item_parts": self.first},
                         {"handoff_id": "gather.b", "item_parts": self.second}]

    def consumes(self, upstream):
        return [{"handoff_id": entry["handoff_id"], "item_digests": [
            handoff.describe_item(item, index=i, source="artifact", ready_at="",
                                  key=self.key)["digest"]
            for i, item in enumerate(entry["item_parts"])]} for entry in upstream]

    def test_parts_are_copied_verbatim_in_handoff_then_item_order(self):
        parts = handoff.compose_parts([self.brief], self.upstream,
                                      consumes=self.consumes(self.upstream), key=self.key)
        self.assertEqual(parts, [self.brief, *self.first[0], *self.first[1], *self.second[0]])
        parts[1]["text"] = "changed"
        self.assertEqual(self.first[0][0]["text"], "a", "composition copies; it never aliases")
        self.assertEqual(handoff.compose_parts([], self.upstream[1:]), self.second[0])

    def test_consumes_must_name_exactly_the_included_items(self):
        consumes = self.consumes(self.upstream)
        altered = json.loads(json.dumps(self.upstream))
        altered[0]["item_parts"][0][0]["text"] = "b"
        for label, upstream in (("missing", self.upstream[:1]), ("reordered", self.upstream[::-1]),
                                ("altered", altered), ("extra", self.upstream + self.upstream[:1])):
            with self.subTest(case=label), self.assertRaises(handoff.CompositionError) as caught:
                handoff.compose_parts([self.brief], upstream, consumes=consumes, key=self.key)
            self.assertEqual(caught.exception.reason, "input.composition-mismatch")
        # Without a configured key the factory records no digests to compare.
        self.assertEqual(len(handoff.compose_parts([self.brief], self.upstream[:1],
                                                   consumes=consumes, key=None)), 4)
        with self.assertRaises(handoff.CompositionError):
            handoff.compose_parts([], [{"handoff_id": "x", "item_parts": [[{"kind": "text"}]]}])

    def test_oversize_message_fails_loudly_and_is_never_truncated(self):
        big = [{"handoff_id": "draft", "item_parts": [[{"text": "x" * handoff.MAX_MESSAGE_BYTES,
                                                         "mediaType": "text/plain"}]]}]
        message = {"role": "ROLE_USER", "messageId": "m",
                   "parts": handoff.compose_parts([self.brief], big)}
        self.assertEqual(len(message["parts"][1]["text"]), handoff.MAX_MESSAGE_BYTES)
        with self.assertRaises(handoff.CompositionError) as caught:
            handoff.require_within_bound(message)
        self.assertEqual(caught.exception.reason, "input.oversize")
        small = {"role": "ROLE_USER", "messageId": "m", "parts": [self.brief]}
        self.assertIs(handoff.require_within_bound(small), small)


FINDINGS_PARTS = [{"text": '{"kind":"findings"}', "mediaType": "application/json",
                   "filename": "findings.json"}]
DRAFT_PARTS = [{"text": "{}", "mediaType": "application/json"}]


class WorkflowHandoffCompositionTests(unittest.IsolatedAsyncioTestCase):
    """Workflow code composes consumption from content-free records only."""

    async def test_consumption_names_upstream_handoffs_and_the_key_stays_in_activities(self):
        key_file = Path(tempfile.mkdtemp(prefix="exo-handoff-wf-", dir=os.environ.get("TMPDIR", "/tmp"))) / "k"
        key = handoff.instance_key(key_file)
        run = factory.FactoryRun(); run.run_id = "run"; run.definition_digest = "definition"
        run.run_inputs = {"question": "synthetic only"}
        run._explicit_assignment_bindings = True; run._handoff_records = True
        bindings = {"research": {"role": "capability", "url": "http://127.0.0.1:1", "identity": "research"},
                    "synthesis": {"role": "capability", "identity": "synthesis"},
                    "quality": {"role": "quality", "identity": "quality"},
                    "release": {"role": "release", "url": "http://127.0.0.1:1", "identity": "release",
                                "output": "artifacts"}}
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
                produced = {"handoff": record(value["handoff_id"], value["instance"][0] * 64)}
                if value["instance"] == "findings":
                    return {**produced, "item_parts": [FINDINGS_PARTS]}
                # A receipt recorded before ``item_parts``: the content falls back.
                return {**produced, "artifact": {"content": '{"kind":"risks"}'}}
            if fn is factory.typed_join:
                return {}
            if fn is factory.synthesize:
                return dict(revision="r1", sha256="c" * 64, content="{}", handoff=record("draft", "d" * 64),
                            item_parts=[DRAFT_PARTS])
            if fn is factory.review:
                return dict(task_id="quality-task", artifact=dict(accepted=True))
            if fn is factory.release:
                return dict(receipt_id="synthetic", handoff=record("publish", "e" * 64))
            raise AssertionError("unexpected activity")
        with patch.object(factory, "verify_closure"), patch.object(factory, "_activity", side_effect=execute), \
                patch.object(factory.workflow, "uuid4", side_effect=[uuid4() for _ in range(10)]):
            result = await run.run_node(document, {"bindings": bindings, "evidence_packet": {}}, {"closure": closure})
        self.assertEqual(result["status"], "accepted")
        self.assertNotIn("handoff", result["artifact"], "the draft record never rides in the Workflow artifact")
        self.assertNotIn("item_parts", result["artifact"])
        self.assertEqual(result["acceptance"]["reviewer"], "quality", "the pinned binding, not an echo")
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
        # The upstream items travel with the same hand-off ids, in the same order.
        self.assertEqual(by_fn[factory.synthesize][0]["upstream"], [
            {"handoff_id": "gather.findings", "item_parts": [FINDINGS_PARTS]},
            {"handoff_id": "gather.risks",
             "item_parts": [[{"text": '{"kind":"risks"}', "mediaType": "application/json"}]]}])
        self.assertNotIn("evidence", by_fn[factory.synthesize][0])
        self.assertNotIn("prior", by_fn[factory.synthesize][0])
        for fn in (factory.review, factory.release):
            self.assertEqual(by_fn[fn][0]["upstream"],
                             [{"handoff_id": "draft", "item_parts": [DRAFT_PARTS]}])
        self.assertEqual(by_fn[factory.release][0]["node"], "publish")
        # The receipt hand-off has no consumer: it never rides in the Workflow result.
        self.assertEqual((by_fn[factory.release][0]["handoff_id"],
                          by_fn[factory.release][0]["handoff_revision"]), ("publish", 1))
        self.assertEqual(result["receipt"], {"receipt_id": "synthetic"})
        self.assertEqual(set(by_fn[factory.release][0]) & {"url", "mode"}, set())
        self.assertEqual(by_fn[factory.release][0]["binding"], bindings["release"])
        # Every Workflow-recorded Activity input (what Temporal history holds) is key-free.
        encoded = json.dumps([value for _, value in calls], default=str)
        self.assertNotIn(key.hex(), encoded)
        self.assertNotIn(str(key_file), encoded)
        source = (ROOT / "src" / "factory.py").read_text()
        for name in ("instance_key", "KEY_ENV", "EXO_HANDOFF_KEY_FILE", "hmac"):
            self.assertNotIn(name, source)


if __name__ == "__main__":
    unittest.main()
