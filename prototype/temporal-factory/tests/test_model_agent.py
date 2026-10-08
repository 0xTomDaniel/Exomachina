"""A2A and durable-state checks using injected role logic and scripted Strands models."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services"))
sys.path.insert(0, str(ROOT / "src"))
import agent_binding  # noqa: E402
from model_agent import canonical, contract_for, create_app, digest  # noqa: E402
import model_agent  # noqa: E402

import a2a_v1  # noqa: E402

AUTH = {"Authorization": "Bearer fixture-token", **a2a_v1.headers([agent_binding.EXTENSION_URI])}


class FakeRole:
    def __init__(self, fail=False, claim_text="Normal claim"):
        self.fail = fail
        self.claim_text = claim_text

    def system_prompt(self, capability):
        return "Return the requested JSON."

    def user_prompt(self, brief):
        return canonical(brief)

    def scripted_reply(self, brief, identity):
        return canonical({"kind": "verified_report@1", "revision": brief["revision"],
                          "markdown": "A report.", "claims": [{"id": "C1", "text": self.claim_text, "evidence": ["E1"]}]})

    def parse(self, text, brief, identity):
        if self.fail:
            raise ValueError("fixture validation failure")
        value = json.loads(text)
        if value["revision"] != brief["revision"]:
            raise ValueError("revision")
        return value

    def precheck(self, brief, identity):
        return None


def command(action="a1", run="run-1", revision="r1"):
    return {"op": "assign", "action_id": action, "run_id": run,
            "definition_digest": "definition-1",
            "brief": canonical({"kind": "synthesis_assignment@1", "revision": revision})}


def send(value):
    return {"jsonrpc": "2.0", "id": str(uuid4()), "method": "SendMessage",
            "params": {"message": {"role": "ROLE_USER", "messageId": str(uuid4()),
                                   "parts": [{"data": value}]},
                       "configuration": {"returnImmediately": True}}}


def get(task_id):
    return {"jsonrpc": "2.0", "id": str(uuid4()), "method": "GetTask",
            "params": {"id": task_id}}


def completed(client, task_id):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        task = client.post("/", json=get(task_id), headers=AUTH).json()["result"]
        if task["status"]["state"] in {"TASK_STATE_COMPLETED", "TASK_STATE_FAILED"}:
            return task
        time.sleep(0.02)
    raise AssertionError("Task did not finish")


class ModelAgentTests(unittest.TestCase):
    def setUp(self):
        self.state = Path(tempfile.mkdtemp(prefix="exo-sf-agents-unit-", dir="/tmp"))
        self.roles = {"synthesis": FakeRole()}

    def app(self, port=45748, **kw):
        return create_app(self.state, port, role="synthesis", capability="report_synthesis@1",
                          model_provider="scripted", roles=self.roles, **kw)

    def pinned_legacy_contract(self, identity, *, capability="report_synthesis@1"):
        document = contract_for(capability)
        document["request"]["data"].pop("optional_fields")
        protocol_path = self.state / f"pinned-protocol-{identity}.json"
        protocol_path.write_text(canonical(document), encoding="utf-8")
        protocol_digest = digest(document)
        declaration_role, declaration_name = model_agent.PINNED_DECLARATION_IDENTITY[
            ("synthesis", capability)]
        descriptor = {
            "a2a_extension": {"uri": model_agent.EXTENSION_URI,
                               "contract": model_agent.CONTRACT_NAME,
                               "contract_digest": protocol_digest},
            "capability": capability,
            "card_sha256": model_agent.agent_card_digest(model_agent.agent_card(
                "synthesis", capability, 45748, identity, document)),
            "name": declaration_name,
            "reconcile": "a2a-idempotent-resend",
            "role": declaration_role,
        }
        descriptor_path = self.state / f"pinned-descriptor-{identity}.json"
        descriptor_path.write_text(canonical(descriptor), encoding="utf-8")
        return {"pinned_descriptor_document": descriptor_path,
                "protocol_document": protocol_path,
                "expected_descriptor_digest": digest(descriptor),
                "expected_protocol_digest": protocol_digest,
                "expected_identity": identity}, document

    def test_replay_conflict_card_pin_and_artifact(self):
        with TestClient(self.app()) as client:
            card = client.get("/.well-known/agent-card.json").json()
            contract = client.get("/contract", headers=AUTH).json()
            self.assertEqual(contract["capability"], "report_synthesis@1")
            self.assertEqual([skill["id"] for skill in card["skills"]], ["report_synthesis@1"])
            self.assertEqual(len(card["capabilities"]["extensions"]), 1)
            extension = card["capabilities"]["extensions"][0]
            self.assertEqual(extension["params"]["contract_digest"], digest(contract))
            with patch.object(agent_binding, "read_json", side_effect=lambda url:
                              card if url.endswith("agent-card.json") else contract):
                pinned = agent_binding.pin("http://127.0.0.1:45748", extension["params"]["identity"])
            self.assertEqual(pinned["reconcile"], "a2a-idempotent-resend")
            first = client.post("/", json=send(command()), headers=AUTH).json()["result"]["task"]
            self.assertEqual(first["status"]["state"], "TASK_STATE_WORKING")
            again = client.post("/", json=send(command()), headers=AUTH).json()["result"]["task"]
            self.assertEqual(again["id"], first["id"])
            changed = command()
            changed["brief"] = canonical({"revision": "r2"})
            self.assertEqual(client.post("/", json=send(changed), headers=AUTH).json()["error"]["code"], -32602)
            done = completed(client, first["id"])
            self.assertEqual(done["status"]["state"], "TASK_STATE_COMPLETED")
            self.assertEqual(len(done["artifacts"]), 1)
            artifact = done["artifacts"][0]
            data = artifact["parts"][0]["data"]
            self.assertEqual(artifact["artifactId"], data["sha256"])
            self.assertEqual(data["sha256"], hashlib.sha256(data["content"].encode()).hexdigest())
            self.assertEqual(data["author"], extension["params"]["identity"])
            self.assertEqual(json.loads(data["content"])["revision"], "r1")
            self.assertEqual(len(client.get("/_test/observe", headers=AUTH).json().get("tasks", [])), 0)

    def test_restart_keeps_identity_task_and_distinct_sessions(self):
        with TestClient(self.app()) as client:
            one = client.post("/", json=send(command()), headers=AUTH).json()["result"]["task"]
            completed(client, one["id"])
            identity = client.get("/health").json()["identity"]
        with TestClient(self.app(port=45749, test_controls=True)) as client:
            self.assertEqual(client.get("/health").json()["identity"], identity)
            self.assertEqual(client.get("/health").json()["incarnation"], 2)
            self.assertEqual(completed(client, one["id"])["id"], one["id"])
            self.assertEqual(client.post("/", json=send(command()), headers=AUTH).json()["result"]["task"]["id"], one["id"])
            two = client.post("/", json=send(command("a2", "run-2")), headers=AUTH).json()["result"]["task"]
            completed(client, two["id"])
            calls = client.get("/_test/observe", headers=AUTH).json()["model_calls"]
            self.assertEqual(len(calls), 2)
            self.assertEqual({row["session_id"] for row in calls},
                             {f"{identity}:{one['id']}", f"{identity}:{two['id']}"})
            self.assertTrue(all(row["live"] is False and row["model_id"] == model_agent.DEFAULT_MODEL_ID
                                for row in calls))

    def test_public_health_reports_configured_inference_mode_without_model_calls(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(
                model_agent.ModelBroker, "ensure_started",
                side_effect=AssertionError("health must not start a model broker")) as started:
            live_app = create_app(
                self.state, 45748, role="synthesis", capability="report_synthesis@1",
                model_provider="codex-subscription", model=model_agent.DEFAULT_MODEL_ID,
                roles=self.roles)
            with TestClient(live_app) as client:
                response = client.get("/health")  # health is intentionally public
                self.assertEqual(response.status_code, 200)
                health = response.json()
                self.assertEqual(health["provider"], "codex-subscription")
                self.assertEqual(health["model_id"], model_agent.DEFAULT_MODEL_ID)
                self.assertEqual(health["reasoning_effort"], "xhigh")
                self.assertIs(health["inference_enabled"], True)
                self.assertIs(health["read_only_usage"], False)
                self.assertNotIn("credential_status", health)
                self.assertNotIn("signed_in", health)
            scripted_app = self.app()
            with TestClient(scripted_app) as client:
                health = client.get("/health").json()
                self.assertEqual(health["provider"], "scripted")
                self.assertIsNone(health["model_id"])
                self.assertIsNone(health["reasoning_effort"])
                self.assertIs(health["inference_enabled"], False)
                self.assertIs(health["read_only_usage"], False)
            started.assert_not_called()

    def test_explicit_assignment_usage_bindings_persist_and_legacy_stays_null(self):
        bound_command = {**command("bound", "run-bound"),
                         "assignment_id": "assignment-synthetic-1",
                         "attempt_id": "attempt-synthetic-1"}
        legacy_command = command("legacy", "run-legacy")
        with TestClient(self.app()) as client:
            contract = client.get("/contract", headers=AUTH).json()
            self.assertEqual(contract["request"]["data"]["optional_fields"],
                             ["assignment_id", "attempt_id", "factory_id"])
            bound_task = client.post("/", json=send(bound_command), headers=AUTH).json()["result"]["task"]
            completed(client, bound_task["id"])
            replay = client.post("/", json=send(bound_command), headers=AUTH).json()["result"]["task"]
            self.assertEqual(replay["id"], bound_task["id"])
            changed_binding = {**bound_command, "attempt_id": "attempt-synthetic-conflict"}
            conflict = client.post("/", json=send(changed_binding), headers=AUTH).json()
            self.assertEqual(conflict["error"]["code"], -32602)
            legacy_task = client.post("/", json=send(legacy_command), headers=AUTH).json()["result"]["task"]
            completed(client, legacy_task["id"])

            bound = client.get("/usage/measurements", params={
                "run_id": "run-bound", "assignment_id": "assignment-synthetic-1",
                "attempt_id": "attempt-synthetic-1",
            }, headers=AUTH).json()["measurements"]
            self.assertEqual(len(bound), 1)
            self.assertEqual(bound[0]["call_scope"], "assignment_call")
            self.assertEqual(bound[0]["assignment_id"], "assignment-synthetic-1")
            self.assertEqual(bound[0]["attempt_id"], "attempt-synthetic-1")
            self.assertEqual(bound[0]["evidence_status"], "unknown")
            self.assertNotIn("cost", bound[0])

            legacy = client.get("/usage/measurements", params={"run_id": "run-legacy"},
                                headers=AUTH).json()["measurements"]
            self.assertEqual(len(legacy), 1)
            self.assertEqual(legacy[0]["call_scope"], "assignment_call")
            self.assertIsNone(legacy[0]["assignment_id"])
            self.assertIsNone(legacy[0]["attempt_id"])
            self.assertEqual(legacy[0]["evidence_status"], "unknown")

        # A new service instance over the same owner state reads the original rows.
        with TestClient(self.app(port=45749)) as restarted:
            restored = restarted.get("/usage/measurements", params={"run_id": "run-bound"},
                                     headers=AUTH).json()["measurements"]
            self.assertEqual(restored, bound)

    def test_read_only_usage_owner_preserves_card_reads_journal_and_never_recovers(self):
        port = 45748
        with TestClient(self.app(port=port)) as normal:
            pinned_card = normal.get("/.well-known/agent-card.json").json()
            pinned_contract = normal.get("/contract", headers=AUTH).json()

        ledger = model_agent.Ledger(self.state)
        task_id = "working-task-synthetic"
        task_command = command("working-action-synthetic", "working-run-synthetic")
        committed_id, created = ledger.accept(task_command, task_id, "working-context-synthetic")
        self.assertTrue(created)
        self.assertEqual(committed_id, task_id)
        identity, incarnation = ledger.identity, ledger.incarnation

        journal = model_agent.ModelUsageJournal(ledger.database)
        measurement = journal.record(
            model_call_id=f"urn:exomachina:model-call:{identity}:synthetic",
            service_identity=identity, task_id=task_id, action_id=task_command["action_id"],
            run_id=task_command["run_id"],
            definition_digest=task_command["definition_digest"],
            call_scope="assignment_call", assignment_id="assignment-synthetic",
            attempt_id="attempt-synthetic", provider="synthetic-provider",
            model_id="synthetic-model-v1", reasoning_effort="xhigh",
            usage=model_agent.unavailable_usage())
        pinned_args, old_contract = self.pinned_legacy_contract(identity)

        with patch.object(model_agent, "Service", side_effect=AssertionError(
                "read-only mode must not construct the recovering service")), \
             patch.object(model_agent.ModelBroker, "ensure_started", side_effect=AssertionError(
                 "read-only mode must not start a model broker")):
            with TestClient(self.app(port=port, read_only_usage=True, **pinned_args)) as readonly:
                readonly_card = readonly.get("/.well-known/agent-card.json").json()
                self.assertEqual(readonly_card["capabilities"]["extensions"][0]["params"]["identity"],
                                 identity)
                self.assertEqual(readonly_card["capabilities"]["extensions"][0]["params"][
                    "contract_digest"], digest(old_contract))
                self.assertEqual(readonly.get("/contract", headers=AUTH).json(), old_contract)
                self.assertNotEqual(pinned_card["capabilities"]["extensions"][0]["params"][
                    "contract_digest"], digest(old_contract))
                self.assertEqual(pinned_contract["request"]["data"]["optional_fields"],
                                 ["assignment_id", "attempt_id", "factory_id"])
                health = readonly.get("/health").json()
                self.assertEqual((health["identity"], health["incarnation"]),
                                 (identity, incarnation))
                self.assertTrue(health["read_only_usage"])
                self.assertIsNone(health["provider"])
                self.assertIsNone(health["model_id"])
                self.assertIsNone(health["reasoning_effort"])
                self.assertIs(health["inference_enabled"], False)

                response = readonly.get("/usage/measurements", headers=AUTH)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["measurements"], [measurement])
                self.assertEqual(readonly.get("/usage/measurements").status_code, 401)

                # A2A GetTask remains a safe read; mutation RPC and test writes are rejected.
                pending = readonly.post("/", json=get(task_id), headers=AUTH)
                self.assertEqual(pending.status_code, 200)
                self.assertEqual(pending.json()["result"]["status"]["state"], "TASK_STATE_WORKING")
                self.assertEqual(readonly.post("/", json=send(command(
                    "blocked-action-synthetic", "blocked-run-synthetic")), headers=AUTH).status_code,
                    503)
                cancel = {"jsonrpc": "2.0", "id": "cancel-synthetic", "method": "CancelTask",
                          "params": {"id": task_id}}
                self.assertEqual(readonly.post("/", json=cancel, headers=AUTH).status_code, 503)
                self.assertEqual(readonly.post("/_test/stimulus", json={}, headers=AUTH).status_code,
                                 503)
                self.assertEqual(readonly.put("/usage/measurements", json={}, headers=AUTH).status_code,
                                 503)

        restored = model_agent.Ledger(self.state, read_only=True)
        self.assertEqual(restored.task(task_id).status.state, model_agent.task_state("working"))
        self.assertEqual(restored.call_count(task_id), 0)
        self.assertEqual((restored.identity, restored.incarnation), (identity, incarnation))
        self.assertEqual(journal.list_measurements(model_call_id=measurement["model_call_id"]),
                         [measurement])

        # Relative --state values are resolved to an existing file before mode=ro URI access.
        relative_state = Path(os.path.relpath(self.state, Path.cwd()))
        with TestClient(create_app(relative_state, port, role="synthesis",
                                   capability="report_synthesis@1", read_only_usage=True,
                                   **pinned_args)) as client:
            self.assertEqual(client.get("/usage/measurements", headers=AUTH).status_code, 200)

    def test_read_only_usage_owner_fails_closed_without_existing_identity_or_journal(self):
        pinned_args, _ = self.pinned_legacy_contract("identity-not-created")
        missing = self.state / "missing-owner"
        with self.assertRaisesRegex(FileNotFoundError, "existing model-agent owner state"):
            create_app(missing, 45750, role="synthesis", capability="report_synthesis@1",
                       read_only_usage=True, **pinned_args)
        self.assertFalse(missing.exists())

        empty = self.state / "empty-owner"
        empty.mkdir()
        database = empty / "model-agent.sqlite3"
        with sqlite3.connect(database) as db:
            db.execute("CREATE TABLE marker(value TEXT)")
        with self.assertRaisesRegex(RuntimeError, "existing model-agent identity"):
            create_app(empty, 45750, role="synthesis", capability="report_synthesis@1",
                       read_only_usage=True, **pinned_args)
        with sqlite3.connect(database) as db:
            self.assertEqual(db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall(),
                [("marker",)])

    def test_read_only_usage_owner_projects_legacy_journal_without_migration(self):
        state = self.state / "legacy-usage-owner"
        state.mkdir()
        database = state / "model-agent.sqlite3"
        usage = model_agent.unavailable_usage()
        recorded_at = "2026-10-03T10:11:12.123456+00:00"
        with sqlite3.connect(database) as db:
            db.execute("CREATE TABLE identity(singleton INTEGER PRIMARY KEY, id TEXT, incarnation INTEGER)")
            db.execute("INSERT INTO identity VALUES (1, 'identity-legacy-synthetic', 7)")
            db.execute("""CREATE TABLE model_usage_measurements(
                model_call_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
                measurement_id TEXT NOT NULL UNIQUE, service_identity TEXT NOT NULL,
                task_id TEXT NOT NULL, action_id TEXT NOT NULL, run_id TEXT NOT NULL,
                definition_digest TEXT NOT NULL, provider TEXT NOT NULL, model_id TEXT NOT NULL,
                reasoning_effort TEXT, usage_json TEXT NOT NULL, measurement_source TEXT NOT NULL,
                completeness TEXT NOT NULL, evidence_status TEXT NOT NULL, recorded_at TEXT NOT NULL)""")
            db.execute("""INSERT INTO model_usage_measurements VALUES
                (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                "urn:synthetic:model-call:legacy-1", "fingerprint-legacy-synthetic",
                "mu-legacy-synthetic", "identity-legacy-synthetic", "task-legacy-synthetic",
                "action-legacy-synthetic", "run-legacy-synthetic", "definition-legacy-synthetic",
                "synthetic-provider", "synthetic-model-v1", "xhigh", canonical(usage),
                "unknown", "unknown", "unknown", recorded_at))

        pinned_args, old_contract = self.pinned_legacy_contract("identity-legacy-synthetic")
        app = create_app(state, 45751, role="synthesis", capability="report_synthesis@1",
                         read_only_usage=True, **pinned_args)
        with TestClient(app) as client:
            contract_response = client.get("/contract", headers=AUTH)
            self.assertEqual(contract_response.status_code, 200)
            self.assertEqual(contract_response.json(), old_contract)
            card = client.get("/.well-known/agent-card.json").json()
            extension = card["capabilities"]["extensions"][0]
            self.assertEqual(extension["params"]["identity"], "identity-legacy-synthetic")
            self.assertEqual(extension["params"]["contract_digest"], pinned_args[
                "expected_protocol_digest"])
            response = client.get("/usage/measurements", headers=AUTH)
            self.assertEqual(response.status_code, 200)
            rows = response.json()["measurements"]
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row["measurement_id"], "mu-legacy-synthetic")
            self.assertEqual(row["recorded_at"], recorded_at)
            self.assertEqual(row["usage"], usage)
            self.assertEqual(row["call_scope"], "assignment_call")
            self.assertIsNone(row["assignment_id"])
            self.assertIsNone(row["attempt_id"])
            self.assertIsNone(row["message_id"])
            self.assertEqual(client.get("/usage/measurements", params={
                "assignment_id": "assignment-not-recorded"}, headers=AUTH).json()["measurements"], [])
            self.assertEqual(client.get("/usage/measurements", params={
                "attempt_id": "attempt-not-recorded"}, headers=AUTH).json()["measurements"], [])
            scoped = client.get("/usage/measurements", params={"call_scope": "assignment_call"},
                                headers=AUTH)
            self.assertEqual(scoped.json()["measurements"], rows)

        with sqlite3.connect(database) as db:
            columns = {row[1] for row in db.execute(
                "PRAGMA table_info(model_usage_measurements)").fetchall()}
        self.assertNotIn("call_scope", columns)
        self.assertNotIn("assignment_id", columns)
        self.assertNotIn("attempt_id", columns)

        wrong_identity = {**pinned_args, "expected_identity": "identity-other-synthetic"}
        with self.assertRaisesRegex(RuntimeError, "differs from the pinned identity"):
            create_app(state, 45751, role="synthesis", capability="report_synthesis@1",
                       read_only_usage=True, **wrong_identity)
        wrong_digest = {**pinned_args, "expected_descriptor_digest": "0" * 64}
        with self.assertRaisesRegex(ValueError, "digest mismatch"):
            create_app(state, 45751, role="synthesis", capability="report_synthesis@1",
                       read_only_usage=True, **wrong_digest)
        wrong_protocol_digest = {**pinned_args, "expected_protocol_digest": "0" * 64}
        with self.assertRaisesRegex(ValueError, "differs from the expected protocol pin"):
            create_app(state, 45751, role="synthesis", capability="report_synthesis@1",
                       read_only_usage=True, **wrong_protocol_digest)
        protocol_document = json.loads(pinned_args["protocol_document"].read_text())
        protocol_document["completion"]["artifact_count"] = 2
        tampered_protocol_path = self.state / "tampered-pinned-protocol.json"
        tampered_protocol_path.write_text(canonical(protocol_document), encoding="utf-8")
        tampered_protocol = {**pinned_args, "protocol_document": tampered_protocol_path}
        with self.assertRaisesRegex(ValueError, "protocol document digest mismatch"):
            create_app(state, 45751, role="synthesis", capability="report_synthesis@1",
                       read_only_usage=True, **tampered_protocol)

        descriptor = json.loads(pinned_args["pinned_descriptor_document"].read_text())
        descriptor["name"] = "unrecognized-synthetic-owner"
        bad_descriptor_path = self.state / "bad-pinned-descriptor.json"
        bad_descriptor_path.write_text(canonical(descriptor), encoding="utf-8")
        wrong_name = {**pinned_args,
                      "pinned_descriptor_document": bad_descriptor_path,
                      "expected_descriptor_digest": digest(descriptor)}
        with self.assertRaisesRegex(ValueError, "role or capability mismatch"):
            create_app(state, 45751, role="synthesis", capability="report_synthesis@1",
                       read_only_usage=True, **wrong_name)

    def test_legacy_task_schema_migrates_missing_usage_bindings_as_null(self):
        state = Path(tempfile.mkdtemp(prefix="exo-sf-agent-legacy-", dir="/tmp"))
        database = state / "model-agent.sqlite3"
        with sqlite3.connect(database) as db:
            db.execute("""CREATE TABLE tasks (
                task_id TEXT PRIMARY KEY, context_id TEXT NOT NULL,
                action_id TEXT NOT NULL UNIQUE, fingerprint TEXT NOT NULL, state TEXT NOT NULL,
                run_id TEXT NOT NULL, definition_digest TEXT NOT NULL, brief TEXT NOT NULL,
                artifact TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL)""")
            db.execute("""INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?)""", (
                "legacy-task-synthetic", "legacy-context-synthetic", "legacy-action-synthetic",
                "legacy-fingerprint-synthetic", "completed", "legacy-run-synthetic",
                "legacy-definition-synthetic", canonical({"revision": "r1"}), None, 1.0, 2.0))

        ledger = model_agent.Ledger(state)
        restored = ledger.row("legacy-task-synthetic")
        self.assertEqual(restored["action_id"], "legacy-action-synthetic")
        self.assertIsNone(restored["assignment_id"])
        self.assertIsNone(restored["attempt_id"])

    def test_read_only_task_rows_supports_pre_factory_id_schema(self):
        state = self.state / "legacy-task-rows"
        state.mkdir()
        database = state / "model-agent.sqlite3"
        with sqlite3.connect(database) as db:
            db.execute("CREATE TABLE identity(singleton INTEGER PRIMARY KEY, id TEXT NOT NULL, incarnation INTEGER NOT NULL)")
            db.execute("INSERT INTO identity VALUES (1, 'identity-legacy-synthetic', 4)")
            db.execute("""CREATE TABLE tasks(
                task_id TEXT PRIMARY KEY, state TEXT NOT NULL, created_at REAL NOT NULL)""")
            db.executemany("INSERT INTO tasks VALUES (?,?,?)", [
                ("task-working-synthetic", "working", 1.0),
                ("task-completed-synthetic", "completed", 2.0),
            ])

        ledger = model_agent.Ledger(state, read_only=True)
        rows = ledger.task_rows()
        self.assertEqual(rows, [
            {"task_id": "task-working-synthetic", "state": "working", "factory_id": None},
            {"task_id": "task-completed-synthetic", "state": "completed", "factory_id": None},
        ])
        self.assertEqual(sum(row["state"] == "working" for row in rows), 1)

        with sqlite3.connect(database) as db:
            columns = {row[1] for row in db.execute("PRAGMA table_info(tasks)")}
            self.assertNotIn("factory_id", columns)
            self.assertEqual(db.execute("SELECT incarnation FROM identity WHERE singleton=1").fetchone(), (4,))

    def test_unfinished_task_recovers_after_restart(self):
        async def stalled(self, messages, tool_specs=None, system_prompt=None, **kwargs):
            await asyncio.sleep(30)
            yield {"messageStart": {"role": "assistant"}}

        with patch.object(model_agent.ScriptedModel, "stream", stalled):
            with TestClient(self.app()) as client:
                first = client.post("/", json=send(command()), headers=AUTH).json()["result"]["task"]
                identity = client.get("/health").json()["identity"]
                self.assertEqual(first["status"]["state"], "TASK_STATE_WORKING")
        with TestClient(self.app(port=45749, test_controls=True)) as client:
            self.assertEqual(client.get("/health").json()["identity"], identity)
            done = completed(client, first["id"])
            self.assertEqual(done["status"]["state"], "TASK_STATE_COMPLETED")
            self.assertEqual(client.post("/", json=send(command()), headers=AUTH).json()["result"]["task"]["id"], first["id"])

    def test_budget_exhaustion_is_fixed_failure(self):
        self.roles["synthesis"] = FakeRole(fail=True)
        with TestClient(self.app(test_controls=True)) as client:
            task = client.post("/", json=send(command()), headers=AUTH).json()["result"]["task"]
            failed = completed(client, task["id"])
            self.assertEqual(failed["status"]["state"], "TASK_STATE_FAILED")
            self.assertEqual(failed["status"]["message"]["parts"][0]["text"], "Agent work failed.")
            self.assertFalse(failed.get("artifacts"))
            calls = client.get("/_test/observe", headers=AUTH).json()["model_calls"]
            self.assertEqual(len(calls), 3)
            self.assertEqual([row["call_no"] for row in calls], [1, 2, 3])
            self.assertTrue(all(row["outcome_kind"] == "validation-error" for row in calls))

    def test_stimulus_next_run_revision_and_hash(self):
        with TestClient(self.app(test_controls=True)) as client:
            body = {"append_claim": {"text": "Planted defect.", "evidence": ["E1"]},
                    "revisions": ["r1"]}
            self.assertTrue(client.post("/_test/stimulus", json=body, headers=AUTH).json()["armed"])
            first = client.post("/", json=send(command()), headers=AUTH).json()["result"]["task"]
            done = completed(client, first["id"])
            artifact = done["artifacts"][0]["parts"][0]["data"]
            content = json.loads(artifact["content"])
            self.assertEqual(content["claims"][-1]["id"], "C2")
            self.assertIn("Planted defect.", content["markdown"])
            self.assertEqual(artifact["sha256"], hashlib.sha256(artifact["content"].encode()).hexdigest())
            repair = client.post("/", json=send(command("a2", "run-1", "r2")), headers=AUTH).json()["result"]["task"]
            repaired = completed(client, repair["id"])
            self.assertNotIn("Planted defect.", repaired["artifacts"][0]["parts"][0]["data"]["content"])
            next_run = client.post("/", json=send(command("a3", "run-2")), headers=AUTH).json()["result"]["task"]
            completed(client, next_run["id"])
            log = client.get("/_test/stimulus-log", headers=AUTH).json()
            self.assertEqual(log, [{"run_id": "run-1", "revision": "r1", "task_id": first["id"],
                                    "planted_text": "Planted defect.", "sha256_after": artifact["sha256"]}])
            self.assertNotIn("/_test/stimulus", [route.path for route in self.app().routes])

    def test_stimulus_waits_for_next_new_run(self):
        with TestClient(self.app(test_controls=True)) as client:
            prior = client.post("/", json=send(command("prior", "old-run")), headers=AUTH).json()["result"]["task"]
            completed(client, prior["id"])
            body = {"append_claim": {"text": "Next run only.", "evidence": ["E1"]}, "revisions": "all"}
            client.post("/_test/stimulus", json=body, headers=AUTH)
            same_run = client.post("/", json=send(command("same", "old-run", "r2")), headers=AUTH).json()["result"]["task"]
            old = completed(client, same_run["id"])
            self.assertNotIn("Next run only.", old["artifacts"][0]["parts"][0]["data"]["content"])
            new_run = client.post("/", json=send(command("new", "new-run")), headers=AUTH).json()["result"]["task"]
            done = completed(client, new_run["id"])
            self.assertIn("Next run only.", done["artifacts"][0]["parts"][0]["data"]["content"])
            self.assertEqual([row["run_id"] for row in client.get("/_test/stimulus-log", headers=AUTH).json()],
                             ["new-run"])

    def test_stimulus_log_committed_before_finish_recovers_one_identical_artifact(self):
        body = {"append_claim": {"text": "Planted defect.", "evidence": ["E1"]},
                "revisions": ["r1"]}
        original_finish = model_agent.Ledger.finish
        interrupted = []

        def die_after_log(ledger, task_id, artifact, stimulus=None):
            if stimulus is not None and not interrupted:
                with ledger.connect() as db:
                    db.execute("""INSERT INTO stimulus_log
                        (stimulus_id,run_id,revision,task_id,planted_text,sha256_after,content_after)
                        VALUES (?,?,?,?,?,?,?)""", tuple(stimulus[key] for key in (
                            "stimulus_id", "run_id", "revision", "task_id", "planted_text",
                            "sha256_after", "content_after")))
                interrupted.append(stimulus)
                raise asyncio.CancelledError("process stopped before Task finish")
            return original_finish(ledger, task_id, artifact, stimulus)

        with patch.object(model_agent.Ledger, "finish", die_after_log):
            with TestClient(self.app(test_controls=True)) as client:
                self.assertTrue(client.post("/_test/stimulus", json=body, headers=AUTH).json()["armed"])
                task = client.post("/", json=send(command()), headers=AUTH).json()["result"]["task"]
                deadline = time.monotonic() + 5
                while not interrupted and time.monotonic() < deadline:
                    time.sleep(0.02)
                self.assertEqual(len(interrupted), 1)
                self.assertEqual(client.post("/", json=get(task["id"]), headers=AUTH).json()["result"]["status"]["state"],
                                 "TASK_STATE_WORKING")

        self.roles["synthesis"] = FakeRole(claim_text="Different recovered model output")
        with TestClient(self.app(port=45749, test_controls=True)) as client:
            done = completed(client, task["id"])
            artifact = done["artifacts"][0]["parts"][0]["data"]
            content = json.loads(artifact["content"])
            self.assertEqual(content["claims"][0]["text"], "Normal claim")
            self.assertEqual(content["claims"][1], {"id": "C2", "text": "Planted defect.", "evidence": ["E1"]})
            self.assertEqual(artifact["sha256"], interrupted[0]["sha256_after"])
            self.assertEqual(client.get("/_test/stimulus-log", headers=AUTH).json(), [
                {"run_id": "run-1", "revision": "r1", "task_id": task["id"],
                 "planted_text": "Planted defect.", "sha256_after": artifact["sha256"]}])
        with sqlite3.connect(self.state / "model-agent.sqlite3") as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM stimulus_log").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM model_calls WHERE task_id=?",
                                        (task["id"],)).fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
