"""Factory release node over A2A: SendMessage, journaled taskId, GetTask recovery.

The factory is the release agent's ordinary A2A client. Its journal maps the
release attempt to the A2A ``messageId`` and ``taskId``; recovery never uses a
private route; a completed Task without a receipt artifact fails the strict
output contract; and nothing factory-specific crosses the wire.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "services"))

import a2a_v1  # noqa: E402
import adapter  # noqa: E402
import handoff  # noqa: E402
import release_delivery  # noqa: E402
from a2a_outcome import OutcomeJournal, Phase, task_started  # noqa: E402
from agent_binding import UnavailableBinding, card_pin  # noqa: E402
from testbed import contract_records  # noqa: E402


CONTENT = json.dumps({"markdown": "# Accepted\n\nexact bytes ✓", "revision": "r1"},
                     sort_keys=True, separators=(",", ":"), ensure_ascii=False)
SHA = hashlib.sha256(CONTENT.encode("utf-8")).hexdigest()
FACTORY_FACTS = ("run-release-test", "definition-digest", "release_id", "run_id",
                 "definition_digest", ":release:", "revision")


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def command(revision="r1", content=CONTENT):
    return {"release_id": f"run-release-test:release:{revision}", "run_id": "run-release-test",
            "definition_digest": "definition-digest", "revision": revision,
            "sha256": hashlib.sha256(content.encode("utf-8")).hexdigest(), "content": content}


def accepted_part(value, **extra):
    """The accepted report artifact's own Part, as the producing agent returned it."""
    return {"text": value["content"], "mediaType": "application/json", **extra}


def message(value, **extra):
    return release_delivery.release_message(value, [accepted_part(value, **extra)])


def payload(value) -> str:
    return hashlib.sha256(json.dumps(message(value), sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False).encode()).hexdigest()


class ReleaseDeliveryTests(unittest.TestCase):
    mode = "participating"

    @classmethod
    def setUpClass(cls):
        cls.directory = Path(tempfile.mkdtemp(prefix="exo-release-delivery-", dir="/tmp"))
        cls.port = _free_port()
        cls.url = f"http://127.0.0.1:{cls.port}"
        cls.state = cls.directory / "release-state"
        cls.process = subprocess.Popen(
            [sys.executable, "-B", str(ROOT / "services" / "release_server.py"),
             "--state", str(cls.state), "--port", str(cls.port), "--mode", cls.mode],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 30
        while True:
            try:
                cls.pin = card_pin(cls.url)
                break
            except UnavailableBinding:
                if time.monotonic() > deadline or cls.process.poll() is not None:
                    raise
                time.sleep(0.1)

    @classmethod
    def tearDownClass(cls):
        cls.process.terminate()
        cls.process.wait(timeout=10)

    def setUp(self):
        self.case = Path(tempfile.mkdtemp(prefix="case-", dir=self.directory))
        self.journal_path = self.case / "runner" / "outcomes.sqlite3"
        self.snapshot = self.case / "testbed" / "agent_snapshot.json"
        self.snapshot.parent.mkdir(parents=True)
        self.identity = self.pin["identity"]
        self.snapshot.write_text(json.dumps({"snapshot_version": 1,
            "agents": {self.identity: {"url": self.url}}}))
        tags = {tag for skill in self.pin["skills"] for tag in skill["tags"]}
        self.contract = {**contract_records()["release"], "card_sha256": self.pin["card_sha256"],
                         "reconcile": ("a2a-idempotent-resend" if "message-id-idempotent" in tags
                                       else "opaque")}
        self.binding = {"role": "release", "url": self.url, "identity": self.identity,
                        "approved": True, "output": "artifacts"}
        self.logs = []

    def input(self, value=None, part=None):
        value = value or command()
        return {"identity": self.identity, "binding": self.binding,
                "contract": self.contract, "command": value,
                "upstream": [{"handoff_id": "draft",
                              "item_parts": [[part or accepted_part(value)]]}]}

    def deliver(self, value=None, part=None, **options):
        return release_delivery.deliver(self.input(value, part), journal_path=self.journal_path,
            snapshot=self.snapshot, log=lambda kind, **fields: self.logs.append((kind, fields)),
            deadline_seconds=options.pop("deadline_seconds", 10), poll_seconds=0.05, **options)

    def journal(self, release_id):
        journal = OutcomeJournal(self.journal_path)
        try:
            return journal.get(release_id)
        finally:
            journal.close()

    def deliveries(self):
        with sqlite3.connect(self.state / "release.sqlite3") as db:
            db.row_factory = sqlite3.Row
            return {row["task_id"]: dict(row) for row in db.execute("SELECT * FROM deliveries")}


class ParticipatingReleaseTests(ReleaseDeliveryTests):
    def test_first_delivery_confirms_an_exact_receipt_and_journals_task_and_message(self):
        value = command("r-first")
        receipt = self.deliver(value)
        self.assertNotIn("unresolved", receipt, receipt)
        row = self.deliveries()[receipt["task_id"]]
        self.assertEqual((row["sha256"], row["byte_length"], row["effect_count"], row["sends"]),
                         (value["sha256"], len(CONTENT.encode("utf-8")), 1, 1))
        self.assertEqual((receipt["sha256"], receipt["revision"], receipt["receipt_id"],
                          receipt["destination_identity"], receipt["outcome"]),
                         (value["sha256"], "r-first", row["receipt_id"], self.identity,
                          "delivered"))
        record = self.journal(value["release_id"])
        self.assertEqual((record.phase, record.task_id, record.message_id),
                         (Phase.CONFIRMED, receipt["task_id"],
                          release_delivery.message_id_for(value["release_id"])))
        self.assertEqual(row["message_id"], record.message_id)
        kinds = [kind for kind, _ in self.logs]
        self.assertEqual(kinds[:2], ["agent-card-verified", "agent-task-journaled"])
        # A retried Activity reuses the confirmed journal row: no second send.
        self.assertEqual(self.deliver(value), receipt)
        self.assertEqual(self.deliveries()[receipt["task_id"]]["sends"], 1)

    def test_the_wire_message_is_ordinary_and_factory_free(self):
        message = release_delivery.release_message(command(), [accepted_part(command())])
        self.assertEqual(set(message), {"role", "messageId", "parts"})
        self.assertEqual(message["parts"], [{"text": CONTENT, "mediaType": "application/json"}])
        self.assertIsNone(a2a_v1.request_violation(a2a_v1.rpc(
            a2a_v1.SEND_MESSAGE, a2a_v1.send_params(message))))
        envelope = json.dumps({key: value for key, value in message.items() if key != "parts"})
        self.assertFalse([fact for fact in FACTORY_FACTS if fact in envelope])
        self.assertEqual(message["messageId"], release_delivery.message_id_for(
            command()["release_id"]))
        self.assertNotEqual(message["messageId"], release_delivery.message_id_for(
            command("r2")["release_id"]))

    def test_uncertain_send_resends_the_identical_message_and_gets_the_original_task(self):
        value = command("r-resend")
        # The receiver committed, but the reply was lost before the taskId was journaled.
        original = release_delivery.send_message(self.url, message(value))
        journal = OutcomeJournal(self.journal_path)
        journal.begin(release_delivery.submitted(value["release_id"], value["run_id"],
            value["definition_digest"], release_delivery.ReceiverKind.PARTICIPATING,
            effect_kind=release_delivery.EffectKind.RELEASE, revision=value["revision"],
            sha256=value["sha256"], pinned_identity=self.identity,
            message_id=release_delivery.message_id_for(value["release_id"]),
            payload_sha256=payload(value)))
        journal.close()
        receipt = self.deliver(value)
        self.assertEqual(receipt["task_id"], original["id"])
        row = self.deliveries()[original["id"]]
        self.assertEqual((row["sends"], row["effect_count"]), (2, 1))
        self.assertIn(("agent-task-journaled", True),
                      [(kind, fields.get("resend")) for kind, fields in self.logs])

    def test_journaled_task_is_recovered_through_get_task_without_a_new_send(self):
        value = command("r-get")
        original = release_delivery.send_message(self.url, message(value))
        journal = OutcomeJournal(self.journal_path)
        expected = release_delivery.submitted(value["release_id"], value["run_id"],
            value["definition_digest"], release_delivery.ReceiverKind.PARTICIPATING,
            effect_kind=release_delivery.EffectKind.RELEASE, revision=value["revision"],
            sha256=value["sha256"], pinned_identity=self.identity,
            message_id=release_delivery.message_id_for(value["release_id"]),
            payload_sha256=payload(value))
        record, _ = journal.begin(expected)
        journal.put(task_started(record, original["id"]))
        journal.close()
        receipt = self.deliver(value)
        self.assertEqual(receipt["task_id"], original["id"])
        self.assertEqual(self.deliveries()[original["id"]]["sends"], 1)
        self.assertIn("agent-task-polled", [kind for kind, _ in self.logs])
        self.assertNotIn("agent-task-journaled", [kind for kind, _ in self.logs])

    def test_conflicting_message_id_reuse_is_refused_and_unresolved(self):
        value = command("r-conflict")
        other = {**message(value),
                 "parts": [a2a_v1.text_part(CONTENT + " ", "application/json")]}
        release_delivery.send_message(self.url, other)
        result = self.deliver(value)
        self.assertEqual(result["unresolved"], "release-send-refused")
        self.assertIn("messageId reused", result["detail"])

    def test_card_pin_mismatch_is_an_incident_before_any_send(self):
        self.contract["card_sha256"] = "0" * 64
        before = len(self.deliveries())
        result = self.deliver(command("r-pin"))
        self.assertEqual(result["unresolved"], "pinned-agent-verification-failed")
        self.assertEqual(len(self.deliveries()), before)

    def test_content_must_match_its_accepted_digest(self):
        with self.assertRaisesRegex(ValueError, "accepted digest"):
            self.deliver({**command("r-digest"), "sha256": "f" * 64})

    def test_activity_binds_the_receipt_hand_off_to_the_release_node(self):
        os.environ["EXO_OUTCOME_DB"] = str(self.journal_path)
        os.environ["EXO_HANDOFF_KEY_FILE"] = str(self.case / "handoff-digest.key")
        self.addCleanup(os.environ.pop, "EXO_OUTCOME_DB", None)
        self.addCleanup(os.environ.pop, "EXO_HANDOFF_KEY_FILE", None)
        key = handoff.instance_key()
        base = self.input(command("r-activity"))
        digest = handoff.describe_item(base["upstream"][0]["item_parts"][0], index=0,
                                       source="artifact", ready_at="", key=key)["digest"]
        value = {**base, "node": "publish", "handoff_id": "publish", "handoff_revision": 1,
                 "consumes": [{"handoff_id": "draft", "item_digests": [digest]}]}
        result = asyncio.run(adapter.release(value))
        self.assertNotIn("unresolved", result, result)
        record = result["handoff"]
        self.assertEqual((record["handoff_id"], record["handoff_revision"]), ("publish", 1))
        item, = record["items"]
        self.assertEqual((item["source"], item["part_kinds"], item["media_type"]),
                         ("artifact", ["data"], "application/json"))
        self.assertEqual(result["sha256"], value["command"]["sha256"])

    def test_consumed_record_must_match_the_delivered_parts(self):
        os.environ["EXO_OUTCOME_DB"] = str(self.journal_path)
        os.environ["EXO_HANDOFF_KEY_FILE"] = str(self.case / "handoff-digest.key")
        self.addCleanup(os.environ.pop, "EXO_OUTCOME_DB", None)
        self.addCleanup(os.environ.pop, "EXO_HANDOFF_KEY_FILE", None)
        before = len(self.deliveries())
        value = {**self.input(command("r-mismatch")), "node": "publish",
                 "handoff_id": "publish", "handoff_revision": 1,
                 "consumes": [{"handoff_id": "draft", "item_digests": ["d" * 64]}]}
        result = asyncio.run(adapter.release(value))
        self.assertEqual(result["unresolved"], "input.composition-mismatch")
        self.assertEqual(len(self.deliveries()), before, "a mismatch never sends")

    def test_delivered_part_is_the_accepted_artifact_part_verbatim(self):
        for media_type, filename in (("application/json", "report.json"),
                                     ("text/markdown", None)):
            with self.subTest(media_type=media_type):
                value = command("r-verbatim-" + media_type.split("/")[1])
                part = {"text": value["content"], "mediaType": media_type,
                        **({"filename": filename} if filename else {})}
                sent = []
                original = release_delivery.send_message

                def spy(url, message):
                    sent.append(message)
                    return original(url, message)

                with patch.object(release_delivery, "send_message", side_effect=spy):
                    receipt = self.deliver(value, part=part)
                self.assertNotIn("unresolved", receipt, receipt)
                self.assertEqual([message["parts"] for message in sent], [[part]])
                self.assertEqual((receipt["sha256"], receipt["media_type"]),
                                 (value["sha256"], media_type))

    def test_release_without_upstream_sends_the_recorded_content_part(self):
        value = command("r-legacy")
        legacy = {key: item for key, item in self.input(value).items() if key != "upstream"}
        self.assertEqual(release_delivery.release_parts(legacy),
                         [{"text": value["content"], "mediaType": "application/json"}])

    def test_a_part_other_than_the_accepted_bytes_is_never_sent(self):
        value = command("r-other-part")
        before = len(self.deliveries())
        with self.assertRaisesRegex(ValueError, "accepted artifact digest"):
            self.deliver(value, part={"text": value["content"] + " ",
                                      "mediaType": "application/json"})
        self.assertEqual(len(self.deliveries()), before)


class FakeTaskReleaseTests(ReleaseDeliveryTests):
    """Receiver outcomes the real participating agent never produces."""

    def deliver_with(self, task):
        with patch.object(release_delivery, "send_message", return_value=task):
            return self.deliver(command("r-fake-" + task["id"]))

    def test_completed_task_without_a_receipt_artifact_is_output_missing(self):
        task = {"id": "no-receipt", "contextId": "c", "status": {
            "state": "TASK_STATE_COMPLETED", "message": {"role": "ROLE_AGENT", "messageId": "s",
            "parts": [a2a_v1.text_part("Delivered; no receipt.")]}}}
        result = self.deliver_with(task)
        self.assertEqual(result["unresolved"], "output.missing")
        self.assertEqual(self.journal(result["release_id"]).phase, Phase.INCIDENT)

    def test_receipt_over_other_bytes_is_inconsistent(self):
        receipt = {"receipt_id": "rcpt", "sha256": "e" * 64, "byte_length": 1,
                   "media_type": "application/json", "accepted_at": "2026-10-08T00:00:00Z",
                   "outcome": "delivered"}
        task = {"id": "wrong-bytes", "contextId": "c", "status": {"state": "TASK_STATE_COMPLETED"},
                "artifacts": [{"artifactId": "rcpt", "parts": [a2a_v1.data_part(receipt)]}]}
        self.assertEqual(self.deliver_with(task)["unresolved"], "release-receipt-inconsistent")

    def test_receipt_must_equal_the_accepted_artifact_sha256(self):
        """Delivery is provably the accepted revision: any receipt that does
        not report the accepted artifact_sha256 over the delivered bytes fails
        the release node at runtime."""
        value = command("r-fake-accepted")
        good = {"receipt_id": "rcpt", "sha256": value["sha256"],
                "byte_length": len(CONTENT.encode("utf-8")),
                "media_type": "application/json", "accepted_at": "2026-10-08T00:00:00Z",
                "outcome": "delivered"}
        other = hashlib.sha256((CONTENT + " ").encode("utf-8")).hexdigest()
        for label, change in (("other-sha", {"sha256": other}),
                              ("byte-length", {"byte_length": 1}),
                              ("media-type", {"media_type": "text/plain"})):
            with self.subTest(case=label):
                task = {"id": "receipt-" + label, "contextId": "c",
                        "status": {"state": "TASK_STATE_COMPLETED"},
                        "artifacts": [{"artifactId": "rcpt",
                                       "parts": [a2a_v1.data_part({**good, **change})]}]}
                with patch.object(release_delivery, "send_message", return_value=task):
                    result = self.deliver({**value, "release_id": value["release_id"] + label})
                self.assertEqual(result["unresolved"], "release-receipt-inconsistent")
                self.assertEqual(self.journal(result["release_id"]).phase, Phase.INCIDENT)
        task = {"id": "receipt-good", "contextId": "c", "status": {"state": "TASK_STATE_COMPLETED"},
                "artifacts": [{"artifactId": "rcpt", "parts": [a2a_v1.data_part(good)]}]}
        # The receipt covers the delivered bytes but not the accepted digest.
        with self.assertRaisesRegex(ValueError, "exact accepted bytes"):
            release_delivery.receipt_from_task(task, {**value, "sha256": other},
                delivered=accepted_part(value), identity=self.identity, task_id="t",
                message_id="m")
        self.assertEqual(release_delivery.receipt_from_task(task, value,
            delivered=accepted_part(value), identity=self.identity, task_id="t",
            message_id="m")["sha256"], value["sha256"])

    def test_rejected_and_failed_tasks_carry_their_status_message(self):
        for state in ("rejected", "failed"):
            with self.subTest(state=state):
                task = {"id": "task-" + state, "contextId": "c", "status": {
                    "state": a2a_v1.wire_state(state), "message": {
                        "role": "ROLE_AGENT", "messageId": "s",
                        "parts": [a2a_v1.text_part("Delivery rejected: reason")]}}}
                result = self.deliver_with(task)
                self.assertEqual((result["unresolved"], result["detail"]),
                                 ("release-task-" + state, "Delivery rejected: reason"))


class OpaqueReleaseTests(ReleaseDeliveryTests):
    mode = "opaque"

    def test_opaque_receiver_never_confirms_and_never_resends(self):
        self.assertEqual(self.contract["reconcile"], "opaque")
        first = self.deliver(command("r-opaque"))
        self.assertEqual(first["unresolved"], "output.missing")
        before = sum(row["effect_count"] for row in self.deliveries().values())
        # A fresh attempt that finds only dispatch intent cannot resend.
        value = command("r-opaque-intent")
        journal = OutcomeJournal(self.journal_path)
        journal.begin(release_delivery.submitted(value["release_id"], value["run_id"],
            value["definition_digest"], release_delivery.ReceiverKind.OPAQUE,
            effect_kind=release_delivery.EffectKind.RELEASE, revision=value["revision"],
            sha256=value["sha256"], pinned_identity=self.identity,
            message_id=release_delivery.message_id_for(value["release_id"]),
            payload_sha256=payload(value)))
        journal.close()
        self.assertEqual(self.deliver(value)["unresolved"], "opaque-effect-unknown")
        self.assertEqual(sum(row["effect_count"] for row in self.deliveries().values()), before)


del ReleaseDeliveryTests


if __name__ == "__main__":
    unittest.main()
