"""Every Exomachina A2A server speaks A2A v1.0 only.

There is no 0.3 interface, dual advertisement, compatibility shim, or fallback:
0.3 methods and shapes are refused, Agent Cards advertise only v1.0, and a send
that does not activate a required extension gets ExtensionSupportRequiredError.
"""
from __future__ import annotations

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
import a2a_v1_server  # noqa: E402
import delayed_agent  # noqa: E402
import harness_server  # noqa: E402
import model_agent  # noqa: E402
import quality_server  # noqa: E402
import supplier_echo_fixture  # noqa: E402


TOKEN = {"Authorization": "Bearer fixture-token"}
EXTENSION = "urn:example:required-for-test:v1"
METHOD_NOT_FOUND, INVALID_PARAMS = -32601, -32602
EXTENSION_SUPPORT_REQUIRED, VERSION_NOT_SUPPORTED = -32008, -32009


def brief(text="v1 brief"):
    return {"text": text}


def v1_send(parts, *, request_id="send-v1", configuration=None):
    return {"jsonrpc": "2.0", "id": request_id, "method": "SendMessage",
            "params": {"message": {"role": "ROLE_USER", "messageId": str(uuid4()),
                                   "parts": parts},
                       "configuration": configuration or {"returnImmediately": True}}}


class A2AV1OnlyTests(unittest.TestCase):
    def setUp(self):
        self.state = Path(tempfile.mkdtemp(prefix="exo-a2a-v1-only-", dir="/tmp"))

    def delayed(self):
        return TestClient(delayed_agent.create_app(self.state / "delayed", 46230,
                                                   delay_seconds=0))

    def requiring(self):
        """The delayed ledger behind a card that requires one extension (SDK enforcement)."""
        from a2a.types import AgentCapabilities, AgentCard, AgentExtension, AgentSkill
        ledger = delayed_agent.Ledger(self.state / "delayed", 0)
        card = AgentCard(name="required", description="required extension",
                         supported_interfaces=a2a_v1_server.interfaces("http://127.0.0.1:46230/"),
                         version="1", default_input_modes=["text/plain"],
                         default_output_modes=["application/json"],
                         capabilities=AgentCapabilities(streaming=False, extensions=[
                             AgentExtension(uri=EXTENSION, required=True)]),
                         skills=[AgentSkill(id="x", name="x", description="x", tags=["x"])],
                         **a2a_v1_server.bearer_security())
        store = delayed_agent.LedgerTaskStore(ledger)
        return TestClient(a2a_v1_server.build_app(card, delayed_agent.Handler(ledger, store, card)))

    def effects(self):
        with sqlite3.connect(self.state / "delayed" / "delayed-agent.sqlite3") as db:
            return db.execute("SELECT COUNT(*) FROM messages").fetchone()[0]

    def post(self, client, body, *, extensions=(), version=True):
        headers = dict(TOKEN)
        if version:
            headers.update(a2a_v1.headers(extensions))
        elif extensions:
            headers[a2a_v1.EXTENSIONS_HEADER] = ",".join(extensions)
        response = client.post("/", json=body, headers=headers)
        self.assertEqual(response.status_code, 200)
        return response.json()

    def test_a2a_0_3_message_send_is_rejected_without_effect(self):
        legacy = {"jsonrpc": "2.0", "id": "legacy-1", "method": "message/send",
                  "params": {"message": {"kind": "message", "role": "user",
                                         "messageId": str(uuid4()),
                                         "parts": [{"kind": "text", "text": "v1 brief"}]},
                             "configuration": {"blocking": False}}}
        with self.delayed() as client:
            # Neither the 0.3 method name nor an absent A2A-Version is served.
            for version in (True, False):
                with self.subTest(version_header=version):
                    reply = self.post(client, legacy, version=version)
                    self.assertEqual(reply["error"]["code"], METHOD_NOT_FOUND)
                    self.assertNotIn("result", reply)
            for method in ("tasks/get", "tasks/cancel", "message/stream"):
                with self.subTest(method=method):
                    reply = self.post(client, {"jsonrpc": "2.0", "id": method,
                                               "method": method, "params": {"id": "x"}})
                    self.assertEqual(reply["error"]["code"], METHOD_NOT_FOUND)
        self.assertEqual(self.effects(), 0)

    def test_v1_method_without_version_header_is_treated_as_0_3_and_refused(self):
        with self.delayed() as client:
            reply = self.post(client, v1_send([brief()]), version=False)
            self.assertEqual(reply["error"]["code"], VERSION_NOT_SUPPORTED)
            reply = client.post("/", json=v1_send([brief()]), headers={
                **TOKEN, a2a_v1.VERSION_HEADER: "0.3"}).json()
            self.assertEqual(reply["error"]["code"], VERSION_NOT_SUPPORTED)
        self.assertEqual(self.effects(), 0)

    def test_part_with_kind_and_other_0_3_shapes_are_not_accepted(self):
        cases = {
            "data part kind": v1_send([{"kind": "data", "data": {"brief": "x"}}]),
            "text part kind": v1_send([{"kind": "text", "text": "brief"}]),
            "message kind": {**v1_send([brief()]), "params": {
                "message": {"kind": "message", "role": "ROLE_USER",
                            "messageId": str(uuid4()), "parts": [brief()]},
                "configuration": {"returnImmediately": True}}},
            "0.3 role": {**v1_send([brief()]), "params": {
                "message": {"role": "user", "messageId": str(uuid4()),
                            "parts": [brief()]},
                "configuration": {"returnImmediately": True}}},
            "0.3 blocking": v1_send([brief()], configuration={"blocking": False}),
            "two contents": v1_send([{"data": {"brief": "x"}, "text": "also"}]),
            "structured command instead of a brief": v1_send([{"data": {"brief": "x"}}]),
        }
        with self.delayed() as client:
            for name, body in cases.items():
                with self.subTest(case=name):
                    reply = self.post(client, body)
                    self.assertEqual(reply["error"]["code"], INVALID_PARAMS)
            accepted = self.post(client, v1_send([brief()]))
            self.assertEqual(set(accepted["result"]), {"task"})
            self.assertEqual(accepted["result"]["task"]["status"]["state"],
                             "TASK_STATE_COMPLETED")
        self.assertEqual(self.effects(), 1)

    def test_missing_required_extension_yields_extension_support_required(self):
        with self.requiring() as client:
            card = client.get("/.well-known/agent-card.json").json()
            self.assertEqual(a2a_v1.required_extensions(card), [EXTENSION])
            for extensions in ((), ("urn:example:unrelated:v1",)):
                with self.subTest(extensions=extensions):
                    reply = self.post(client, v1_send([brief()]),
                                      extensions=extensions)
                    self.assertEqual(reply["error"]["code"], EXTENSION_SUPPORT_REQUIRED)
                    self.assertIn(EXTENSION, reply["error"]["message"])
            self.assertEqual(self.effects(), 0)
            activated = self.post(client, v1_send([brief()]),
                                  extensions=("urn:example:unrelated:v1", EXTENSION))
            self.assertEqual(set(activated["result"]), {"task"})
        self.assertEqual(self.effects(), 1)

    def test_production_agent_cards_require_no_extension(self):
        with self.delayed() as client:
            card = client.get("/.well-known/agent-card.json").json()
            self.assertEqual(a2a_v1.required_extensions(card), [])

    def test_every_agent_card_advertises_only_v1(self):
        apps = {
            "fixture harness": (46231, lambda: harness_server.create_app(
                self.state / "capability", "capability", 46231)),
            "quality": (46232, lambda: quality_server.create_app(self.state / "quality", 46232)),
            "model agent": (46233, lambda: model_agent.create_app(
                self.state / "model", 46233, role="synthesis",
                capability="report_synthesis@1")),
            "delayed agent": (46234, lambda: delayed_agent.create_app(
                self.state / "delayed", 46234)),
            "supplier echo": (46235, lambda: supplier_echo_fixture.create_app(
                self.state / "supplier", 46235)),
        }
        for name, (port, build) in apps.items():
            with self.subTest(service=name), TestClient(build()) as client:
                card = client.get("/.well-known/agent-card.json").json()
                url = f"http://127.0.0.1:{port}/"
                self.assertEqual(card["supportedInterfaces"], [{
                    "url": url, "protocolBinding": "JSONRPC", "protocolVersion": "1.0"}])
                for legacy in ("url", "protocolVersion", "preferredTransport",
                               "additionalInterfaces"):
                    self.assertNotIn(legacy, card)
                self.assertEqual(a2a_v1.card_url(card), url)
                self.assertNotIn("'kind'", str(card))
                legacy = client.post("/", headers={**TOKEN, **a2a_v1.headers()}, json={
                    "jsonrpc": "2.0", "id": "legacy", "method": "message/send",
                    "params": {"message": {"role": "user", "messageId": "m",
                                           "parts": [{"kind": "text", "text": "x"}]}}})
                self.assertEqual(legacy.json()["error"]["code"], METHOD_NOT_FOUND)

    def test_get_task_returns_an_unwrapped_v1_task(self):
        with self.delayed() as client:
            first = self.post(client, v1_send([brief("numbers")]))["result"]["task"]
            reply = self.post(client, {"jsonrpc": "2.0", "id": 7, "method": "GetTask",
                                       "params": {"id": first["id"]}})
            self.assertEqual(reply["id"], 7)
            self.assertEqual(reply["result"]["status"]["state"], "TASK_STATE_COMPLETED")
            self.assertEqual(a2a_v1.task_state(reply["result"]), "completed")


class WireAdapterTests(unittest.TestCase):
    def test_state_mapping_is_the_only_boundary(self):
        self.assertEqual(a2a_v1.observed_state("TASK_STATE_INPUT_REQUIRED"), "input-required")
        self.assertEqual(a2a_v1.wire_state("working"), "TASK_STATE_WORKING")
        for value in ("working", "input-required", "completed", None, "TASK_STATE_UNSPECIFIED"):
            with self.subTest(value=value), self.assertRaises(a2a_v1.ProtocolError):
                a2a_v1.observed_state(value)

    def test_send_result_requires_v1_wrapper(self):
        task = {"id": "t", "status": {"state": "TASK_STATE_WORKING"}}
        self.assertEqual(a2a_v1.unwrap_send_result({"task": task}), ("task", task))
        for result in ({"kind": "task", **task}, task, {"task": task, "message": {}}):
            with self.subTest(result=result), self.assertRaises(a2a_v1.ProtocolError):
                a2a_v1.unwrap_send_result(result)
        with self.assertRaises(a2a_v1.ProtocolError):
            a2a_v1.part_data({"kind": "data", "data": {}})
        self.assertEqual(a2a_v1.part_data({"data": {"n": 1.0, "f": 1.5}}), {"n": 1, "f": 1.5})


if __name__ == "__main__":
    unittest.main()
