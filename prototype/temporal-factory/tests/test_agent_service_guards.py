"""Guards for the 8 Oct 2026 rule: A2A only between the factory and agent services.

Agent services expose only A2A JSON-RPC and the well-known Agent Card, never
echo a factory identifier, and reference none in their source.
"""
from __future__ import annotations

import ast
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
