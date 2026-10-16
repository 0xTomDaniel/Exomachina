"""Guards for the 8 Oct 2026 rule: A2A only between the factory and agent services.

Agent services expose only A2A JSON-RPC and the well-known Agent Card, never
echo a factory identifier, and reference none in their source. No agent card,
and no factory-service card (a factory acting as an agent service), declares a
required Exomachina extension, and no dispatch brief the factory composes
carries another node's output text: upstream outputs travel only as their own
verbatim Parts (decisions 5, 7 and 9).
"""
from __future__ import annotations

import ast
import asyncio
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from fastapi.testclient import TestClient
from starlette.routing import Mount


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services"))
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scenarios"))
import a2a_extensions  # noqa: E402
import a2a_v1  # noqa: E402
import adapter  # noqa: E402
import agent_binding  # noqa: E402
from agent_roles import ROLES, working_view  # noqa: E402
from report_contract import (REPORT_ACCEPTANCE_CRITERIA, canonical,  # noqa: E402
                             digest as report_digest, research_assignment,
                             synthesis_assignment)
PACKET = ROOT / "packets" / "exo-qualification-2026-09-23" / "packet.json"
import delayed_agent  # noqa: E402
import harness  # noqa: E402
import harness_server  # noqa: E402
import model_agent  # noqa: E402
import quality_server  # noqa: E402
import release_server  # noqa: E402
import supplier_echo_fixture  # noqa: E402
import single_factory  # noqa: E402


A2A_ROUTES = {("/", ("POST",)), ("/.well-known/agent-card.json", ("GET", "HEAD"))}
FACTORY_NAMES = ("run_id", "assignment_id", "attempt_id", "action_id", "definition_digest",
                 "factory_id", "node")
AUTH = {"Authorization": "Bearer fixture-token", **a2a_v1.headers()}


def routes(app) -> set[tuple[str, tuple[str, ...]]]:
    observed = set()
    for route in app.routes:
        if isinstance(route, Mount):
            observed.add((route.path, ("MOUNT",)))
        else:
            observed.add((route.path, tuple(sorted(getattr(route, "methods", None) or ()))))
    return observed


class AgentServiceGuardTests(unittest.TestCase):
    def setUp(self):
        self.state = Path(tempfile.mkdtemp(prefix="exo-agent-guard-", dir="/tmp"))

    def apps(self):
        instance = self.state / "agent-instance"
        harness.init_instance(instance, name="guard-agent", mode="agent", port=46261,
                              home=self.state / "home")
        return {
            "model_agent": model_agent.create_app(
                self.state / "model", 46262, role="synthesis",
                capability="report_synthesis@1", model_provider="scripted"),
            "quality_server": quality_server.create_app(self.state / "quality", 46263),
            "delayed_agent": delayed_agent.create_app(self.state / "delayed", 46264,
                                                      delay_seconds=0),
            "supplier_echo_fixture": supplier_echo_fixture.create_app(
                self.state / "supplier", 46265),
            "harness_server capability": harness_server.create_app(
                self.state / "capability", "capability", 46266),
            "harness agent mode": harness.create_app(instance),
            "release_server": release_server.create_app(self.state / "release", 46267,
                                                        "participating"),
        }

    def test_agent_services_register_only_a2a_routes(self):
        for name, app in self.apps().items():
            with self.subTest(service=name):
                self.assertEqual(routes(app), A2A_ROUTES)
                with TestClient(app) as client:
                    for path in ("/health", "/contract", "/usage/measurements",
                                 "/fixture/actions/x", "/_test/stimulus", "/_test/effects",
                                 "/openapi.json", "/docs"):
                        self.assertEqual(client.get(path, headers=AUTH).status_code, 404, path)

    def test_agent_cards_offer_only_the_generic_budget_extension(self):
        # Decision 9: an agent's identity is its pinned card; the only
        # Exomachina URI an agent may offer is the optional budget extension,
        # plus the test-only stimulus control when started with test controls.
        apps = {**self.apps(), "model_agent test controls": model_agent.create_app(
            self.state / "model-test", 46268, role="synthesis",
            capability="report_synthesis@1", model_provider="scripted", test_controls=True)}
        for name, app in apps.items():
            with self.subTest(service=name), TestClient(app) as client:
                card = client.get("/.well-known/agent-card.json").json()
                extensions = (card.get("capabilities") or {}).get("extensions") or []
                allowed = {a2a_extensions.BUDGET_URI}
                if name == "model_agent test controls":
                    allowed.add(a2a_extensions.TEST_STIMULUS_URI)
                self.assertLessEqual({item["uri"] for item in extensions}, allowed)
                self.assertFalse(any(item.get("required") for item in extensions))
                self.assertNotIn("identity", json.dumps(extensions))

    def factory_service_cards(self) -> dict[str, dict]:
        """Agent Cards of a factory acting as an agent service (nested supplier on/off)."""
        class StubRunner:
            def __init__(self, home, **_kwargs):
                self.address = "127.0.0.1:1"

            def is_running(self):
                return False

        class EmptyUsageBroker:
            def usage_journal(self):
                return self

            def list_measurements(self, **_filters):
                return []

        cards = {}
        with patch.object(harness, "Runner", StubRunner):
            for name, enabled in (("factory", False), ("factory nested supplier", True)):
                instance = self.state / "home" / "instances" / name.replace(" ", "-")
                harness.init_instance(instance, name=name.replace(" ", "-"), mode="factory",
                                      port=46270 + int(enabled), home=self.state / "home",
                                      **({"nested_supplier_enabled": True} if enabled else {}))
                app = harness.create_app(instance, usage_broker=EmptyUsageBroker())
                with TestClient(app) as client:
                    cards[name] = client.get("/.well-known/agent-card.json").json()
        return cards

    def test_no_agent_or_factory_service_card_requires_an_exomachina_extension(self):
        """Decisions 7 and 9: a caller needs nothing Exomachina-specific."""
        cards = {}
        for name, app in self.apps().items():
            with TestClient(app) as client:
                cards[name] = client.get("/.well-known/agent-card.json").json()
        cards.update(self.factory_service_cards())
        self.assertIn("factory nested supplier", cards)
        for name, card in cards.items():
            with self.subTest(card=name):
                extensions = (card.get("capabilities") or {}).get("extensions") or []
                self.assertEqual(agent_binding.describe(card)["required_extensions"], [])
                self.assertFalse(any(item.get("required") for item in extensions))
                exomachina = {item["uri"] for item in extensions
                              if "exomachina" in item["uri"].lower()}
                self.assertLessEqual(exomachina, {a2a_extensions.BUDGET_URI})
        # The retired factory-to-factory extension is gone from every source.
        for path in [*(ROOT / "src").glob("*.py"), *(ROOT / "services").glob("*.py")]:
            self.assertNotIn("a2a-action-contract", path.read_text(), path.name)

    def test_no_dispatch_brief_carries_another_nodes_output(self):
        """Every brief the factory composes holds only the node's own assignment;
        upstream outputs (research, the judged draft, Quality's findings) follow
        it as verbatim Parts and never appear inside it."""
        packet, question = json.loads(PACKET.read_text()), "What qualified?"
        def produce(role, brief, inputs=()):
            view = working_view(brief, list(inputs))
            return canonical(ROLES[role].parse(ROLES[role].scripted_reply(view, "id"), view, "id"))
        research = {capability: produce("research", research_assignment(capability, question, packet))
                    for capability in ("packet_findings@1", "packet_risks@1")}
        research_parts = {f"gather.{c}": [[{"text": t, "mediaType": "application/json"}]]
                          for c, t in research.items()}
        draft = produce("synthesis", synthesis_assignment("r1", question, packet),
                        [part for item in research_parts.values() for part in item[0]])
        verdict = canonical({"kind": "quality_verdict@1",
            "candidate": {"revision": "r1", "sha256": hashlib.sha256(draft.encode()).hexdigest()},
            "accepted": False, "decided_by": "model", "rubric": "report-quality@1",
            "rubric_digest": report_digest(REPORT_ACCEPTANCE_CRITERIA),
            "findings": [{"claim_id": "C1", "severity": "blocking",
                          "problem": "DISTINCT-FINDING-PROBLEM-TEXT contradicts E7",
                          "evidence": ["E7"]}]})
        draft_parts = [[{"text": draft, "mediaType": "application/json"}]]
        verdict_parts = [[{"text": verdict, "mediaType": "application/json"}]]
        upstream = lambda **items: [{"handoff_id": k, "item_parts": v} for k, v in items.items()]
        candidate = {"revision": "r1", "sha256": hashlib.sha256(draft.encode()).hexdigest(),
                     "author": "synth", "content": draft}
        base = {"run": "run-guard", "digest": "d" * 64, "question": question, "packet": packet,
                "binding": {"role": "capability", "identity": "agent"}, "contract": {}}
        calls = {
            "research": (adapter.assign, {**base, "instance": "research_findings",
                                          "capability": "packet_findings@1"}),
            "draft": (adapter.synthesize, {**base, "revision": "r1",
                                           "upstream": upstream(**research_parts)}),
            "repair": (adapter.synthesize, {**base, "revision": "r2",
                "quality_findings": json.loads(verdict)["findings"],  # pre-patch input: ignored
                "upstream": upstream(**research_parts, draft=draft_parts,
                                     independent_quality=verdict_parts)}),
            "quality": (adapter.review, {**base, "binding": {"role": "quality", "identity": "q"},
                "candidate": candidate, "acceptance_criteria": REPORT_ACCEPTANCE_CRITERIA,
                "rubric_digest": report_digest(REPORT_ACCEPTANCE_CRITERIA),
                "policy_digest": "p" * 64, "assignment_id": "a", "attempt": 1,
                "upstream": upstream(draft=draft_parts)}),
        }
        # The node's own assignment content (packet, its digest, the question,
        # the criteria) may appear in an output; it is not upstream content.
        own = canonical(synthesis_assignment("r1", question, packet)) + canonical(
            REPORT_ACCEPTANCE_CRITERIA)
        outputs = [*research.values(), draft, verdict]
        def leaves(value):
            if isinstance(value, str):
                yield value
            elif isinstance(value, dict):
                for child in value.values():
                    yield from leaves(child)
            elif isinstance(value, list):
                for child in value:
                    yield from leaves(child)
        distinctive = sorted({text for output in outputs for text in leaves(json.loads(output))
                              if len(text) >= 24 and text not in own})
        self.assertIn("DISTINCT-FINDING-PROBLEM-TEXT contradicts E7", distinctive)
        def dispatched(activity, value):
            captured = []

            async def capture(fn, *args):
                captured.append(args[2])
                raise RuntimeError("captured before any send")
            with patch.object(adapter, "_thread_with_heartbeat", capture):
                result = asyncio.run(activity(value))
            self.assertTrue({"unresolved", "inconsistent"} & set(result), result)
            [action] = captured
            return action
        # The guard detects the retired coupling: findings pasted into a brief.
        pasted = lambda *args: {**synthesis_assignment(*args),
                                "quality_findings": json.loads(verdict)["findings"]}
        with patch.object(adapter, "synthesis_assignment", pasted):
            leaked = dispatched(*calls["repair"])["brief"]
        self.assertTrue([text for text in distinctive if text in leaked])
        for node, (activity, value) in calls.items():
            with self.subTest(node=node):
                action = dispatched(activity, value)
                parts = action["parts"]
                brief = action["brief"]
                self.assertEqual(parts[0], {"text": brief, "mediaType": "application/json"})
                self.assertEqual(parts[1:], [part for entry in value.get("upstream", [])
                                             for item in entry["item_parts"] for part in item],
                                 "upstream items follow the brief verbatim")
                embedded = [text for text in [*outputs, *distinctive] if text in brief]
                self.assertEqual(embedded, [], f"{node} brief embeds upstream output")
                self.assertFalse({"quality_findings", "findings", "prior", "candidate",
                                  "evidence", "verdict", "report"} & set(json.loads(brief)))

    def test_agent_service_sources_declare_no_routes_of_their_own(self):
        decorators = {"get", "post", "put", "patch", "delete", "route", "api_route",
                      "add_api_route", "add_route", "mount", "websocket"}
        for path in single_factory.AGENT_SERVICE_FILES:
            tree = ast.parse(path.read_text())
            with self.subTest(path=path.name):
                found = [node.lineno for node in ast.walk(tree)
                         if isinstance(node, ast.Attribute) and node.attr in decorators
                         and isinstance(node.value, ast.Name) and node.value.id == "app"]
                self.assertEqual(found, [])

    def test_no_factory_identifier_is_echoed(self):
        secret = {name: f"{name}-secret-{uuid4().hex[:6]}" for name in FACTORY_NAMES}
        brief = json.dumps({"revision": "r1", "question": "q"})
        for name, app in self.apps().items():
            if name == "model_agent":
                continue  # covered with a full model round in test_model_agent
            with self.subTest(service=name), TestClient(app) as client:
                text = (json.dumps({"artifact": {"revision": "r1", "sha256": "0" * 64,
                                                 "author": "someone", "content": "{}"}})
                        if name == "quality_server" else brief)
                body = a2a_v1.rpc(a2a_v1.SEND_MESSAGE, {
                    **a2a_v1.send_params(a2a_v1.user_message(
                        [a2a_v1.text_part(text)], context_id="guard-context")),
                    "metadata": dict(secret)})
                reply = client.post("/", json=body, headers=AUTH).json()
                self.assertIn("result", reply, reply)
                task = reply["result"]["task"]
                fetched = client.post("/", json=a2a_v1.rpc(a2a_v1.GET_TASK, {"id": task["id"]}),
                                      headers=AUTH).json()["result"]
                wire = json.dumps([reply, fetched])
                for key, value in secret.items():
                    self.assertNotIn(value, wire)
                    self.assertNotIn(f'"{key}"', wire)
                # The agent's identity is its card; no Task carries it.
                self.assertNotIn('"agent_identity"', wire)

    def test_factory_name_audit_is_clean_and_detects_a_violation(self):
        audit = single_factory._factory_name_audit()
        self.assertEqual(audit["violations"], [])
        probe = self.state / "probe_service.py"
        probe.write_text('def f(task):\n    return task["run_id"]\n')
        with patch.object(single_factory, "AGENT_SERVICE_FILES", (probe,)), \
                patch.object(single_factory, "_record_path", lambda path: str(path)):
            caught = single_factory._factory_name_audit()["violations"]
        self.assertEqual([x["name"] for x in caught], ["run_id"])
        self.assertTrue(single_factory._import_audit()["factory_names_clean"])


if __name__ == "__main__":
    unittest.main()
