"""Conformance proof for A2A v1 mediation decision 9: any A2A agent can be bound.

The agent below is a third party's: it is built only on the installed
``a2a-sdk`` server APIs (its own AgentExecutor, the SDK's default request
handler, in-memory task store and Starlette routes) and imports no repository
module. Its Agent Card declares no extension, its Tasks carry no metadata, and
no skill promises ``message-id-idempotent``. The factory pins it by its card
alone and runs a research assignment through it end to end.

Any change that breaks this test violates decision 7.
"""
from __future__ import annotations

import asyncio
import json
import socket
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import uvicorn
from google.protobuf.json_format import MessageToDict
from starlette.applications import Starlette

from a2a.helpers.proto_helpers import new_task, new_text_part
from a2a.server.agent_execution import AgentExecutor
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.routes import create_agent_card_routes, create_jsonrpc_routes
from a2a.server.tasks import InMemoryTaskStore, TaskUpdater
from a2a.types import AgentCapabilities, AgentCard, AgentInterface, AgentSkill, TaskState


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "services"))


# --- The third-party agent: a2a-sdk only -----------------------------------

class FindingsExecutor(AgentExecutor):
    """Answers a research brief with findings built from the brief's own packet."""

    def __init__(self):
        self.received: list[dict] = []

    async def execute(self, context, event_queue):
        message = MessageToDict(context.message)
        self.received.append(message)
        if context.current_task is None:
            await event_queue.enqueue_event(new_task(
                context.task_id, context.context_id, TaskState.TASK_STATE_SUBMITTED,
                history=[context.message]))
        updater = TaskUpdater(event_queue, context.task_id, context.context_id)
        await updater.start_work()
        brief = json.loads(message["parts"][0]["text"])
        evidence = [item["id"] for item in brief["packet"]["items"]]
        result = {"kind": brief["capability"], "packet_digest": brief["packet_digest"],
                  "items": [{"id": f"F{number}", "statement": f"Finding {number} from {source}.",
                             "evidence": [source]}
                            for number, source in enumerate(evidence[:2], start=1)]}
        text = json.dumps(result, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        await updater.add_artifact([new_text_part(text, media_type="application/json")])
        await updater.complete()

    async def cancel(self, context, event_queue):
        raise NotImplementedError("this agent does not cancel")


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def third_party_agent(port: int):
    card = AgentCard(
        name="Third-party findings agent", description="Findings from an evidence packet",
        supported_interfaces=[AgentInterface(url=f"http://127.0.0.1:{port}/",
                                             protocol_binding="JSONRPC",
                                             protocol_version="1.0")],
        version="1.0.0", default_input_modes=["application/json"],
        default_output_modes=["application/json"],
        capabilities=AgentCapabilities(streaming=False),
        skills=[AgentSkill(id="packet_findings@1", name="Packet findings",
                           description="Findings from an evidence packet", tags=["research"])])
    executor = FindingsExecutor()
    handler = DefaultRequestHandler(executor, InMemoryTaskStore(), card)
    app = Starlette(routes=[*create_agent_card_routes(card), *create_jsonrpc_routes(handler, "/")])
    requests: list[dict] = []

    async def recorded(scope, receive, send):
        """Record every HTTP request this agent receives, then serve it."""
        if scope["type"] != "http":
            return await app(scope, receive, send)
        chunks = []
        while True:
            message = await receive()
            chunks.append(message.get("body", b""))
            if not message.get("more_body"):
                break
        requests.append({"path": scope["path"],
                         "headers": {key.decode(): value.decode()
                                     for key, value in scope["headers"]},
                         "body": b"".join(chunks).decode("utf-8", errors="replace")})
        replayed = False

        async def replay():
            nonlocal replayed
            if replayed:
                return await receive()
            replayed = True
            return {"type": "http.request", "body": b"".join(chunks), "more_body": False}

        return await app(scope, replay, send)

    return recorded, executor, requests


# --- The factory side --------------------------------------------------------

import a2a_v1  # noqa: E402
import adapter  # noqa: E402
import agent_binding  # noqa: E402
import long_client  # noqa: E402
import testbed  # noqa: E402
from a2a_outcome import OutcomeJournal, Phase  # noqa: E402
from report_contract import validate_research_result  # noqa: E402
from report_fixture import packet  # noqa: E402


PACKET = packet()
FACTORY_KEYS = ("agent_identity", "run_id", "assignment_id", "attempt_id", "action_id",
                "definition_digest", "factory_id", "policy_digest", "handoff_id")


class ThirdPartyAgentConformanceTests(unittest.TestCase):
    def setUp(self):
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        app, self.executor, self.requests = third_party_agent(self.port)
        self.server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=self.port,
                                                    log_level="warning", lifespan="off"))
        thread = threading.Thread(target=self.server.run, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 5)
        self.addCleanup(setattr, self.server, "should_exit", True)
        deadline = time.monotonic() + 10
        while not self.server.started:
            if time.monotonic() > deadline or not thread.is_alive():
                self.fail("third-party agent did not start")
            time.sleep(0.02)
        self.home = Path(tempfile.mkdtemp(prefix="exo-third-party-", dir="/tmp"))
        (self.home / "runner").mkdir()

    def test_plain_a2a_sdk_agent_runs_an_assignment_end_to_end(self):
        card = agent_binding.read_json(self.url + "/.well-known/agent-card.json")
        self.assertNotIn("extensions", card.get("capabilities") or {})
        self.assertNotIn("securitySchemes", card)

        # Pin from the card alone; the identity is derived from it.
        pin = agent_binding.pin(self.url)
        self.assertEqual(pin["identity"], agent_binding.card_identity(pin["card_sha256"]))
        self.assertEqual(pin["reconcile"], "opaque")
        binding = {"role": "capability", "url": self.url, "identity": pin["identity"],
                   "approved": True}
        contract = {"name": "research_findings", "role": "capability",
                    "capability": "packet_findings@1", **pin}
        testbed.write_metadata(self.home, {"research_findings": binding}, {},
                               {"research_findings": contract}, snapshot=True)

        run, digest = "run-third-party-7", "7f" * 32
        factory_values = {"run": run, "digest": digest, "assignment_id": "assignment-3p-1",
                          "attempt_id": "attempt-3p-1", "factory_id": "factory-3p",
                          "handoff_id": "gather.research_findings"}
        key_file = self.home / "handoff-digest.key"
        with patch.dict("os.environ", {
                "EXO_OUTCOME_DB": str(self.home / "runner" / "outcomes.sqlite3"),
                "EXO_HANDOFF_KEY_FILE": str(key_file)}):
            result = asyncio.run(adapter.assign({
                **factory_values, "binding": binding, "contract": contract,
                "instance": "research_findings", "capability": "packet_findings@1",
                "question": "What happened?", "packet": PACKET, "handoff_revision": 1}))

        # The assignment completed with validated content, authored by the pin.
        self.assertNotIn("unresolved", result, result)
        validate_research_result(result["content"], "packet_findings@1", PACKET)
        self.assertEqual(result["artifact"]["author"], pin["identity"])
        self.assertEqual(result["artifact"]["revision"], "r1")

        # The factory journal correlates its action to the A2A identities.
        action_id = f"{run}:research_findings"
        journal = OutcomeJournal(self.home / "runner" / "outcomes.sqlite3")
        try:
            record = journal.get(action_id)
        finally:
            journal.close()
        self.assertEqual(record.phase, Phase.CONFIRMED)
        self.assertEqual(record.pinned_identity, pin["identity"])
        self.assertEqual(len(self.executor.received), 1)
        received = self.executor.received[0]
        self.assertEqual(received["messageId"], record.message_id)
        self.assertEqual(received["contextId"], record.context_id)
        self.assertEqual(result["task_id"], record.task_id)
        served = long_client.get_task(self.url, record.task_id)
        self.assertEqual((served["id"], served["contextId"]), (record.task_id, record.context_id))
        self.assertEqual(a2a_v1.task_state(served), "completed")
        # No Exomachina metadata was needed: the served Task carries none.
        self.assertNotIn("metadata", served)

        # A produced hand-off record exists, content-free and keyed.
        handoff = result["handoff"]
        self.assertEqual((handoff["handoff_id"], handoff["handoff_revision"]),
                         ("gather.research_findings", 1))
        self.assertEqual(len(handoff["items"]), 1)
        self.assertEqual(handoff["items"][0]["media_type"], "application/json")
        self.assertNotIn(result["artifact"]["content"], json.dumps(handoff))

        # The agent never received a factory identifier.
        self.assertTrue(any(request["path"] == "/" for request in self.requests))
        wire = json.dumps([self.executor.received, self.requests])
        for value in (run, digest, action_id, pin["identity"], *factory_values.values()):
            self.assertNotIn(value, wire)
        for key in FACTORY_KEYS:
            self.assertNotIn(key, wire)

    def test_opaque_agent_lost_send_is_never_resent(self):
        # Opaque reconcile stays fully usable; only a resend after an uncertain
        # send is refused, because the card promises no messageId dedupe.
        pin = agent_binding.pin(self.url)
        binding = {"role": "capability", "url": self.url, "identity": pin["identity"],
                   "approved": True}
        contract = {"name": "research_findings", "role": "capability",
                    "capability": "packet_findings@1", **pin}
        testbed.write_metadata(self.home, {"research_findings": binding}, {},
                               {"research_findings": contract}, snapshot=True)
        lost = long_client.UncertainSubmission("connection reset after send")
        with patch.dict("os.environ", {
                "EXO_OUTCOME_DB": str(self.home / "runner" / "outcomes.sqlite3")}), \
                patch.object(long_client, "send_message_async", side_effect=lost) as send:
            result = asyncio.run(adapter.assign({
                "run": "run-3p-lost", "digest": "8e" * 32, "binding": binding,
                "contract": contract, "instance": "research_findings",
                "capability": "packet_findings@1", "question": "What happened?",
                "packet": PACKET}))
        self.assertEqual(result["unresolved"], "opaque-effect-unknown")
        self.assertEqual(send.call_count, 1)


if __name__ == "__main__":
    unittest.main()
