from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
import re
import sys
import tempfile
import unittest
from contextlib import redirect_stdout

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scenarios"))
import dashboard_qualification as qualification  # noqa: E402


class DashboardQualificationTests(unittest.TestCase):
    def test_document_and_runner_cover_every_matrix_row_once(self):
        document = (ROOT / "DASHBOARD-QUALIFICATION.md").read_text()
        documented = re.findall(r"^\|\s*((?:S|P)\d{2})\s*\|", document, re.MULTILINE)
        self.assertEqual(len(documented), len(set(documented)))
        self.assertEqual(set(documented), set(qualification.ROW_IDS))
        self.assertEqual(33, len(qualification.S_ROWS))
        self.assertEqual(10, len(qualification.P_ROWS))
        qualified = set()
        partial = set()
        for row_id in qualification.ROW_IDS:
            line = next(line for line in document.splitlines() if line.startswith(f"| {row_id} |"))
            status = line.split("|")[3].strip()
            if status.startswith("Qualified"):
                qualified.add(row_id)
            elif status.startswith("Partial"):
                partial.add(row_id)
            else:
                self.assertEqual("Unqualified", status, row_id)
        self.assertEqual({"S01", "S02", "S03", "S04", "S09", "S24", "S25", "S33"}, qualified)
        self.assertEqual({"S16", "S22", "S23", "S28", "S29", "S30"}, partial)

    def _write_public_export_fixture(self, root: Path, *, model_id="gpt-6-luna",
                                     reasoning_effort="xhigh", source="recorded"):
        artifact = b"public artifact bytes for digest verification\n"
        artifact_digest = hashlib.sha256(artifact).hexdigest()
        (root / "artifacts").mkdir(parents=True)
        (root / "artifacts" / "accepted.md").write_bytes(artifact)
        digest_a, digest_b, digest_c = "a" * 64, "b" * 64, "c" * 64
        graph = {"nodes": [{"id": "node-a", "kind": "start"},
                           {"id": "node-b", "kind": "finish"}],
                 "edges": [{"from": "node-a", "to": "node-b"}]}
        run = {
            "id": "run-1", "task": {"id": "parent-task", "context_id": "parent-context"},
            "status": {"state": "accepted"},
            "pinned": {"manifest_digest": digest_a, "package_digest": digest_b,
                       "definition_digest": digest_c, "interpreter_build": "build-1",
                       "model_id": model_id},
            "graph": graph,
            "assignments": [{"assignment_id": "assign-1", "attempt_id": "attempt-1",
                             "task_id": "parent-task", "context_id": "parent-context",
                             "state": "completed"}],
            "artifacts": [{"schema_version": 1, "factory_id": "factory-1",
                           "run_id": "run-1", "artifact_revision": "r1",
                           "artifact_sha256": artifact_digest}],
            "quality": [{"schema_version": 1, "factory_id": "factory-1",
                         "run_id": "run-1", "task_id": "parent-task",
                         "artifact_revision": "r1", "artifact_sha256": artifact_digest,
                         "reviewer_identity": "reviewer-1", "accepted": True,
                         "finding_count": 0}],
            "decisions": [], "commands": [], "delivery": [], "incidents": [], "admissions": [],
        }
        snapshot = {
            "schema_version": 1, "cursor": "c1.abcdefghijklmnop." + "z" * 40,
            "captured_at": "2026-10-03T12:00:00Z",
            "freshness": {"status": "fresh", "observed_at": "2026-10-03T12:00:00Z"},
            "state": {
                "factory": {"id": "factory-1", "name": "Qualification fixture", "graph": graph},
                "runs": [run], "active_publication": None, "capacity": None,
                "commercial": {"usage": [], "obligations": [], "payments": []},
            },
        }
        bundle = {
            "schema_version": 1, "source": source, "snapshot": snapshot,
            "frames": [
                {"op": "event", "event": {"data": {"run_id": "run-1", "task_id": "child-findings",
                                                          "context_id": "child-findings-context"}}},
                {"op": "event", "event": {"data": {"run_id": "run-1", "task_id": "child-risks",
                                                          "context_id": "child-risks-context"}}},
            ],
            "provenance": {"label": "observed-real", "model_label": model_id,
                           "fixture_label": None, "release_label": "public delivery",
                           "defect_labels": [], "completeness": "complete",
                           "evidence_ref": "public-route-1"},
        }
        categories = {
            name: {"status": "unavailable", "value": None}
            for name in qualification.USAGE_CATEGORIES
        }
        categories["input_tokens"] = {"status": "reported", "value": 0}
        usage = {
            "measurements": [{
                "measurement_id": "measure-1", "model_call_id": "call-1",
                "recorded_at": "2026-10-03T12:00:01Z", "call_scope": "assignment_call",
                "provider": "codex-subscription", "model_id": model_id,
                "reasoning_effort": reasoning_effort, "unit": "tokens",
                "measurement_source": "provider_reported", "completeness": "partial",
                "evidence_status": "provider_reported", "run_id": "run-1",
                "task_id": "child-findings", "definition_digest": digest_c,
                "assignment_id": "assign-1", "attempt_id": "attempt-1",
                "usage": categories,
            }],
            "coverage": {"status": "partial"},
        }
        task_bindings = {
            "schema_version": 1, "source": "a2a-public-task-exports",
            "parent": {"task_id": "parent-task", "context_id": "parent-context", "run_id": "run-1"},
            "children": [
                {"task_id": "child-findings", "context_id": "child-findings-context",
                 "parent_task_id": "parent-task", "parent_context_id": "parent-context", "run_id": "run-1"},
                {"task_id": "child-risks", "context_id": "child-risks-context",
                 "parent_task_id": "parent-task", "parent_context_id": "parent-context", "run_id": "run-1"},
            ],
        }
        files = {
            "snapshot.json": snapshot, "bundle.json": bundle, "usage.json": usage,
            "task-bindings.json": task_bindings,
            "artifacts.json": {"schema_version": 1, "artifacts": [
                {"run_id": "run-1", "revision": "r1", "sha256": artifact_digest,
                 "file": "artifacts/accepted.md"},
            ]},
        }
        for name, value in files.items():
            (root / name).write_text(json.dumps(value), encoding="utf-8")
        return artifact_digest

    def test_preflight_fails_closed_without_explicit_runtime_configuration(self):
        report = qualification.build_preflight(
            root=ROOT,
            environment={},
            executable_finder=lambda _name: None,
            port_check=lambda: {"status": "clear", "listeners": 0, "filters": []},
            now=lambda: "2026-10-02T12:00:00Z",
        )
        self.assertEqual("blocked", report["status"])
        self.assertEqual("unqualified", report["qualification_status"])
        self.assertFalse(report["runtime"]["temporal_server"]["fallback_used"])
        self.assertFalse(report["runtime"]["temporal_cli"]["fallback_used"])
        self.assertEqual("unconfigured", report["runtime"]["temporal_server"]["status"])
        self.assertEqual("unconfigured", report["runtime"]["temporal_cli"]["status"])
        self.assertTrue(any("EXO_TEMPORAL_RUNTIME is unset" in item for item in report["blockers"]))
        self.assertTrue(any("EXO_TEMPORAL_CLI is unset" in item for item in report["blockers"]))
        self.assertFalse(any("EXO_AUTHOR_PROVIDER is unset" in item for item in report["blockers"]))
        self.assertFalse(report["model_requirement"]["EXO_AUTHOR_PROVIDER_configured"])
        self.assertTrue(report["model_requirement"]["operator_authorized_for_fresh_workflow"])
        self.assertEqual(
            "CLI --provider codex-subscription",
            report["model_requirement"]["access_method"],
        )
        self.assertEqual(
            "pending_default_custom_broker_status",
            report["model_requirement"]["provider_readiness_status"],
        )
        self.assertFalse(report["model_requirement"]["codex_cli_login_is_sufficient"])
        self.assertTrue(any("default custom broker status is pending" in item for item in report["blockers"]))
        self.assertFalse(report["model_requirement"]["live_inference_ready"])
        self.assertEqual("not_observed", report["evidence_fields"]["usage"]["status"])
        self.assertEqual([], report["side_effects"])

    def test_preflight_reports_missing_public_interfaces_without_private_fallback(self):
        empty_checkout = Path(tempfile.mkdtemp(prefix="exo-qual-dashboard-preflight-", dir="/tmp"))
        report = qualification.build_preflight(
            root=empty_checkout,
            environment={},
            executable_finder=lambda _name: None,
            port_check=lambda: {"status": "clear", "listeners": 0, "filters": []},
        )
        self.assertIn("discover", report["public_operations"]["observation"])
        self.assertIn("settlement reconciliation", report["public_operations"]["commercial"])
        self.assertIn("MPP profile", report["public_operations"]["payment"])
        for name in ("observation", "dashboard", "commercial", "payment_adapter"):
            self.assertNotEqual("available", report["interfaces"][name]["status"])
        self.assertTrue(any("public Interface unavailable" in item for item in report["blockers"]))
        self.assertEqual("Commerce", report["payment_adapter"]["owner"])
        self.assertFalse(report["payment_adapter"]["separate_payment_lane"])
        self.assertFalse(report["payment_adapter"]["network_ready"])
        self.assertEqual([], report["side_effects"])

    def test_commerce_payment_adapter_declarations_are_structural_not_network_readiness(self):
        checkout = Path(tempfile.mkdtemp(prefix="exo-qual-commerce-contract-", dir="/tmp"))
        source = checkout / "src" / "commercial.py"
        source.parent.mkdir()
        source.write_text(
            """
class PaymentAdapter: pass

PAYMENT_ADAPTER_PROFILES = {
    "mpp": {"profile": "MPP", "version": "v1", "operations": [],
            "environment_prerequisites": [], "status": "unconfigured"},
    "x402": {"profile": "x402", "version": "v1", "operations": [],
             "environment_prerequisites": [], "status": "unconfigured"},
    "ap2-v0.2": {"profile": "AP2", "version": "0.2", "operations": [],
            "environment_prerequisites": [], "status": "unconfigured"},
}
""",
            encoding="utf-8",
        )
        contract = qualification._payment_adapter_contract(checkout)
        self.assertTrue(contract["seam_declared"])
        self.assertEqual(["ap2", "mpp", "x402"], contract["profiles_declared"])
        self.assertTrue(contract["required_metadata_present"])
        self.assertFalse(contract["network_ready"])
        self.assertEqual("unqualified", contract["qualification_status"])

        source.write_text(source.read_text(encoding="utf-8").replace('"version": "0.2"', '"version": "0.1"'))
        incomplete = qualification._payment_adapter_contract(checkout)
        self.assertFalse(incomplete["required_metadata_present"])

    def test_landed_commerce_profiles_report_ap2_version_without_network_readiness(self):
        contract = qualification._payment_adapter_contract(ROOT)
        self.assertTrue(contract["seam_declared"])
        self.assertEqual({"ap2", "mpp", "x402"}, set(contract["profiles_declared"]))
        self.assertTrue(contract["required_metadata_present"])
        self.assertFalse(contract["network_ready"])
        self.assertEqual("unqualified", contract["qualification_status"])
        report = qualification.build_preflight(
            root=ROOT,
            environment={},
            executable_finder=lambda _name: None,
            port_check=lambda: {"status": "clear", "listeners": 0, "filters": []},
        )
        self.assertEqual("declared", report["interfaces"]["payment_adapter"]["status"])
        self.assertEqual(["ap2", "mpp", "x402"], report["payment_adapter"]["declarations"]["profiles_declared"])
        self.assertFalse(report["payment_adapter"]["network_ready"])
        self.assertEqual([], report["payment_adapter"]["configured_profiles"])
        self.assertEqual({"total": 10, "qualified": 0}, report["matrix"]["P"])

    def test_pinned_binary_check_requires_exact_path_and_digest(self):
        trial = Path(tempfile.mkdtemp(prefix="exo-qual-dashboard-binary-", dir="/tmp"))
        executable = trial / "temporal"
        payload = b"pinned binary fixture\n"
        executable.write_bytes(payload)
        executable.chmod(0o700)
        digest = hashlib.sha256(payload).hexdigest()
        verified = qualification.verify_pinned_binary(
            str(executable), expected_path=executable, expected_sha256=digest
        )
        self.assertEqual("verified", verified["status"])
        wrong_digest = qualification.verify_pinned_binary(
            str(executable), expected_path=executable, expected_sha256="0" * 64
        )
        self.assertEqual("hash_mismatch", wrong_digest["status"])
        wrong_path = qualification.verify_pinned_binary(
            str(executable), expected_path=trial / "other", expected_sha256=digest
        )
        self.assertEqual("path_mismatch_or_non_executable", wrong_path["status"])

    def test_temporal_archive_and_installed_executable_digests_are_distinct(self):
        self.assertEqual(
            "f95748376241f5941327fa4c4e8e76641e8c4a9acabf77de9c86eb3d8238f4d7",
            qualification.PROVISIONED_TEMPORAL_SERVER_ARCHIVE_SHA256,
        )
        self.assertEqual(
            "41e0425378fcb4fb5766340b97435e20fe47bbff2d7bf644ec2d51f7662b7c56",
            qualification.PROVISIONED_TEMPORAL_CLI_ARCHIVE_SHA256,
        )
        self.assertEqual(
            "verified",
            qualification.verify_pinned_binary(
                str(qualification.PROVISIONED_TEMPORAL_SERVER),
                expected_path=qualification.PROVISIONED_TEMPORAL_SERVER,
                expected_sha256=qualification.PROVISIONED_TEMPORAL_SERVER_SHA256,
            )["status"],
        )
        self.assertEqual(
            "verified",
            qualification.verify_pinned_binary(
                str(qualification.PROVISIONED_TEMPORAL_CLI),
                expected_path=qualification.PROVISIONED_TEMPORAL_CLI,
                expected_sha256=qualification.PROVISIONED_TEMPORAL_CLI_SHA256,
            )["status"],
        )

    def test_fixture_and_unit_evidence_never_qualify_live_rows(self):
        fixture = {
            "evidence_label": "observed-synthetic",
            "execution_mode": "demo",
            "interface_level": True,
            "model": {"status": "observed", "id": "gpt-6-luna", "reasoning_effort": "xhigh"},
            "evidence_refs": [{"id": "recorded-demo"}],
        }
        self.assertEqual("observed-synthetic-only", qualification.classify_evidence(fixture))
        unit = {"evidence_label": "unit-tested", "execution_mode": "unit"}
        self.assertEqual("unit-tested-only", qualification.classify_evidence(unit))

    def test_historical_sol_is_partial_and_new_luna_smoke_is_review_only(self):
        historical = {
            "evidence_label": "observed-real",
            "execution_mode": "live",
            "interface_level": True,
            "model": {"status": "observed", "id": "gpt-6-sol", "reasoning_effort": "low"},
            "evidence_refs": [{"id": "codex-subscription-3"}],
        }
        self.assertEqual("historical-partial", qualification.classify_evidence(historical))
        current = {
            **historical,
            "model": {"status": "observed", "id": "gpt-6-luna", "reasoning_effort": "xhigh"},
        }
        self.assertEqual("candidate-for-review", qualification.classify_evidence(current))
        self.assertNotEqual("qualified", qualification.classify_evidence(current))

    def test_safe_export_allows_only_safe_evidence_projection(self):
        record = {
            "schema_version": 1,
            "report_type": "dashboard-qualification-candidate",
            "evidence_label": "observed-real",
            "claim_ids": ["S01"],
            "execution_mode": "live",
            "interface_level": True,
            "model": {"id": "gpt-6-luna", "reasoning_effort": "xhigh", "status": "observed"},
            "graph": {"definition_digest": "a" * 64, "package_digest": "b" * 64},
            "bindings": {"manifest_digest": "c" * 64, "build_id": "b-123"},
            "contracts": [{"name": "verified-research", "version": "1", "digest": "d" * 64}],
            "task_refs": [{"task_id": "task-1", "run_id": "run-1", "state": "completed"}],
            "attempts": [{"attempt_id": "attempt-1", "assignment_id": "assignment-1", "index": 1}],
            "usage": {"status": "unknown", "records": []},
            "artifact_refs": [{"revision": "r1", "sha256": "e" * 64}],
            "evidence_refs": [{"id": "smoke-1", "label": "observed-real", "sha256": "f" * 64}],
            "interface_results": [{"interface": "observation", "operation": "snapshot", "status": "passed"}],
        }
        exported = qualification.export_safe_evidence(record)
        self.assertEqual(record, exported)
        self.assertEqual("unknown", exported["usage"]["status"])

    def test_safe_export_rejects_secret_and_unallowlisted_payload_fields(self):
        with self.assertRaises(ValueError):
            qualification.export_safe_evidence({"schema_version": 1, "access_token": "do-not-copy"})
        with self.assertRaises(ValueError):
            qualification.export_safe_evidence({"schema_version": 1, "raw_event": {"id": "evt"}})
        with self.assertRaisesRegex(ValueError, "sensitive-looking evidence value"):
            qualification.export_safe_evidence({
                "schema_version": 1,
                "evidence_refs": [{"id": "smoke", "uri": "Bearer abcdefghijklmnop"}],
            })

    def test_public_export_attestation_correlates_pins_tasks_quality_usage_and_hashes(self):
        export = Path(tempfile.mkdtemp(prefix="exo-public-export-", dir="/tmp"))
        expected_artifact_digest = self._write_public_export_fixture(export)
        report = qualification.attest_public_exports(export)

        self.assertEqual("candidate-for-review", report["status"])
        self.assertEqual("unqualified", report["qualification_status"])
        self.assertEqual("run-1", report["run"]["run_id"])
        self.assertEqual("parent-task", report["task_bindings"]["parent"]["task_id"])
        self.assertEqual("verified", report["task_bindings"]["relationship_status"])
        self.assertNotEqual(
            report["task_bindings"]["parent"]["task_id"],
            report["task_bindings"]["children"][0]["task_id"],
        )
        self.assertTrue(report["checks"]["graph_pins_complete"])
        self.assertTrue(report["checks"]["accepted_quality_exact_artifact"])
        self.assertEqual(expected_artifact_digest, report["artifacts"][0]["bytes_sha256"])
        self.assertTrue(report["artifacts"][0]["accepted_quality_match"])

        measurement = report["usage"]["records"][0]
        self.assertEqual("reported", measurement["usage"]["input_tokens"]["status"])
        self.assertEqual(0, measurement["usage"]["input_tokens"]["value"])
        self.assertEqual("unavailable", measurement["usage"]["output_tokens"]["status"])
        self.assertIsNone(measurement["usage"]["output_tokens"]["value"])
        self.assertEqual("run-1", measurement["run_id"])

        envelope = (json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False,
                               allow_nan=False) + "\n").encode("utf-8")
        markdown = qualification.render_public_attestation_markdown(report)
        self.assertNotEqual(expected_artifact_digest, hashlib.sha256(envelope).hexdigest())
        self.assertNotEqual(expected_artifact_digest, hashlib.sha256(markdown).hexdigest())
        self.assertNotIn(b"public artifact bytes", envelope)
        self.assertNotIn(b"public artifact bytes", markdown)
        json_output = export / "attestation.json"
        markdown_output = export / "attestation.md"
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            self.assertEqual(0, qualification.main([
                "--attest-public-exports", str(export),
                "--attestation-output", str(json_output),
                "--markdown-output", str(markdown_output),
            ]))
        summary = json.loads(stdout.getvalue())
        self.assertEqual(hashlib.sha256(json_output.read_bytes()).hexdigest(),
                         summary["output_digests"]["attestation_envelope_sha256"])
        self.assertEqual(hashlib.sha256(markdown_output.read_bytes()).hexdigest(),
                         summary["output_digests"]["attestation_markdown_sha256"])

    def test_public_export_attests_same_original_task_across_distinct_temporal_runs(self):
        export = Path(tempfile.mkdtemp(prefix="exo-shared-task-runs-", dir="/tmp"))
        expected_artifact_digest = self._write_public_export_fixture(export)
        snapshot = json.loads((export / "snapshot.json").read_text())
        root = snapshot["state"]["runs"][0]
        child = json.loads(json.dumps(root))
        child_run_id = "run-1:child-1"
        child["id"] = child_run_id
        child["pinned"]["definition_digest"] = "e" * 64
        child["pinned"]["package_digest"] = "f" * 64
        child["graph"] = {"nodes": [{"id": "child-start", "kind": "start"}], "edges": []}
        child["artifacts"][0]["run_id"] = child_run_id
        child["quality"][0]["run_id"] = child_run_id
        snapshot["state"]["runs"] = [root, child]
        (export / "snapshot.json").write_text(json.dumps(snapshot), encoding="utf-8")

        bundle = json.loads((export / "bundle.json").read_text())
        bundle["snapshot"] = snapshot
        bundle["frames"] = []
        (export / "bundle.json").write_text(json.dumps(bundle), encoding="utf-8")

        task_bindings = json.loads((export / "task-bindings.json").read_text())
        parent = task_bindings["parent"]
        task_bindings["children"] = [{
            "task_id": parent["task_id"], "context_id": parent["context_id"],
            "parent_task_id": parent["task_id"], "parent_context_id": parent["context_id"],
            "parent_run_id": "run-1", "run_id": child_run_id,
            "relationship": "same_original_task_run",
        }]
        (export / "task-bindings.json").write_text(json.dumps(task_bindings), encoding="utf-8")

        usage = json.loads((export / "usage.json").read_text())
        usage["measurements"][0]["task_id"] = parent["task_id"]
        usage["measurements"][0]["context_id"] = parent["context_id"]
        (export / "usage.json").write_text(json.dumps(usage), encoding="utf-8")

        artifact_manifest = json.loads((export / "artifacts.json").read_text())
        artifact_manifest["artifacts"][0]["run_id"] = child_run_id
        (export / "artifacts.json").write_text(json.dumps(artifact_manifest), encoding="utf-8")

        report = qualification.attest_public_exports(export, run_id=child_run_id)
        self.assertEqual("candidate-for-review", report["status"])
        self.assertEqual("unqualified", report["qualification_status"])
        self.assertEqual("run-1", report["run"]["parent_run_id"])
        self.assertEqual(parent["task_id"], report["run"]["task_id"])
        self.assertNotEqual(report["run"]["parent_run_id"], report["run"]["run_id"])
        self.assertEqual("same_original_task_run",
                         report["task_bindings"]["children"][0]["relationship"])
        self.assertEqual(expected_artifact_digest, report["artifacts"][0]["bytes_sha256"])
        self.assertTrue(report["checks"]["task_run_bindings_verified"])

    def test_public_export_attestation_never_upgrades_demo_or_historical_model(self):
        demo_export = Path(tempfile.mkdtemp(prefix="exo-public-demo-", dir="/tmp"))
        self._write_public_export_fixture(demo_export, source="demo")
        demo = qualification.attest_public_exports(demo_export)
        self.assertEqual("observed-synthetic-only", demo["status"])
        self.assertEqual("observed-synthetic", demo["evidence_label"])
        self.assertEqual("unqualified", demo["qualification_status"])

        historical_export = Path(tempfile.mkdtemp(prefix="exo-public-historical-", dir="/tmp"))
        self._write_public_export_fixture(historical_export, model_id="gpt-6-sol",
                                          reasoning_effort="low")
        historical = qualification.attest_public_exports(historical_export)
        self.assertEqual("historical-partial", historical["status"])
        self.assertEqual("unqualified", historical["qualification_status"])

    def test_fixture_receiver_label_does_not_relabel_actual_execution(self):
        export = Path(tempfile.mkdtemp(prefix="exo-public-fixture-receiver-", dir="/tmp"))
        self._write_public_export_fixture(export)
        bundle_path = export / "bundle.json"
        bundle = json.loads(bundle_path.read_text())
        bundle["provenance"]["fixture_label"] = (
            "Temporal workflow execution; receiver result is fixture evidence")
        bundle["provenance"]["release_label"] = "fixture receiver"
        bundle_path.write_text(json.dumps(bundle), encoding="utf-8")

        report = qualification.attest_public_exports(export)
        self.assertEqual("candidate-for-review", report["status"])
        self.assertEqual("observed-real", report["evidence_label"])
        self.assertEqual("live", report["execution_mode"])
        self.assertEqual("fixture", report["delivery_evidence"])
        self.assertEqual("unqualified", report["qualification_status"])

    def test_public_export_attestation_rejects_payload_fields_and_mismatched_artifact_bytes(self):
        export = Path(tempfile.mkdtemp(prefix="exo-public-unsafe-", dir="/tmp"))
        self._write_public_export_fixture(export)
        usage_path = export / "usage.json"
        usage = json.loads(usage_path.read_text())
        usage["measurements"][0]["prompt"] = "must not be loaded into evidence"
        usage_path.write_text(json.dumps(usage), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "sensitive field"):
            qualification.attest_public_exports(export)

        export = Path(tempfile.mkdtemp(prefix="exo-public-badhash-", dir="/tmp"))
        self._write_public_export_fixture(export)
        (export / "artifacts" / "accepted.md").write_bytes(b"changed bytes\n")
        report = qualification.attest_public_exports(export)
        self.assertEqual("incomplete-public-export", report["status"])
        self.assertFalse(report["checks"]["accepted_quality_exact_artifact"])
        self.assertFalse(report["artifacts"][0]["bytes_match_manifest"])
        self.assertEqual("unqualified", report["qualification_status"])

    def test_public_export_event_data_rows_use_finite_item_union(self):
        export = Path(tempfile.mkdtemp(prefix="exo-public-event-data-", dir="/tmp"))
        self._write_public_export_fixture(export)
        report = qualification.attest_public_exports(export)
        self.assertEqual("candidate-for-review", report["status"])
        self.assertTrue(report["checks"]["accepted_quality_exact_artifact"])

        snapshot_path = export / "snapshot.json"
        snapshot = json.loads(snapshot_path.read_text())
        snapshot["state"]["runs"][0]["artifacts"][0]["factory_id"] = "foreign-factory"
        snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
        bundle_path = export / "bundle.json"
        bundle = json.loads(bundle_path.read_text())
        bundle["snapshot"] = snapshot
        bundle_path.write_text(json.dumps(bundle), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "different factory"):
            qualification.attest_public_exports(export)

        export = Path(tempfile.mkdtemp(prefix="exo-public-event-data-extra-", dir="/tmp"))
        self._write_public_export_fixture(export)
        snapshot_path = export / "snapshot.json"
        snapshot = json.loads(snapshot_path.read_text())
        snapshot["state"]["runs"][0]["quality"][0]["unexpected_private"] = "blocked"
        snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
        bundle_path = export / "bundle.json"
        bundle = json.loads(bundle_path.read_text())
        bundle["snapshot"] = snapshot
        bundle_path.write_text(json.dumps(bundle), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "event data has unallowlisted fields"):
            qualification.attest_public_exports(export)

    def _rewrite_snapshot(self, export: Path, change) -> None:
        snapshot_path = export / "snapshot.json"
        snapshot = json.loads(snapshot_path.read_text())
        change(snapshot["state"]["runs"][0])
        snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
        bundle_path = export / "bundle.json"
        bundle = json.loads(bundle_path.read_text())
        bundle["snapshot"] = snapshot
        bundle_path.write_text(json.dumps(bundle), encoding="utf-8")

    def test_s33_hand_off_records_pass_only_with_exact_content_free_fields(self):
        base = {"schema_version": 1, "factory_id": "factory-1", "run_id": "run-1",
                "assignment_id": "assign-1", "attempt_id": "1", "node": "node-a"}
        produced = {**base, "handoff_id": "node-a", "handoff_revision": 1,
                    "produced_at": "2026-10-03T12:00:01Z", "items": [
                        {"item_index": 0, "source": "artifact", "part_kinds": ["data"],
                         "media_type": None, "byte_length": 10, "ready_at": "2026-10-03T12:00:01Z",
                         "digest": "d" * 64, "artifact_revision": "r1", "artifact_sha256": "e" * 64}]}
        consumed = {**base, "node": "node-b", "consumed_at": "2026-10-03T12:00:02Z",
                    "inputs": [{"handoff_id": "node-a", "item_digests": ["d" * 64]}]}
        ready = {**base, "handoff_id": "node-a", "item_index": 0, "part_kinds": ["text", "url"],
                 "media_type": "text/plain", "ready_at": "2026-10-03T12:00:00Z"}
        records = {"produced": [produced], "consumed": [consumed], "ready": [ready]}

        export = Path(tempfile.mkdtemp(prefix="exo-public-handoff-", dir="/tmp"))
        self._write_public_export_fixture(export)
        self._rewrite_snapshot(export, lambda run: run.update(handoffs=records))
        self.assertEqual("candidate-for-review", qualification.attest_public_exports(export)["status"])

        def leak(path, value):
            def change(run):
                rows = json.loads(json.dumps(records))
                target = rows
                for key in path[:-1]:
                    target = target[key]
                target[path[-1]] = value
                run["handoffs"] = rows
            return change
        cases = [
            (leak(("produced", 0, "items", 0, "name"), "secret"), "unallowlisted"),
            (leak(("produced", 0, "items", 0, "url"), "https://example.invalid"), "unallowlisted"),
            (leak(("produced", 0, "task_id"), "parent-task"), "unallowlisted"),
            (leak(("produced", 0, "metadata"), {}), "unallowlisted"),
            (leak(("consumed", 0, "inputs", 0, "text"), "secret"), "unallowlisted"),
            (leak(("ready", 0, "artifactId"), "x"), "unallowlisted"),
            (leak(("produced", 0, "items", 0, "digest"), "not-a-digest"), "digest"),
            (leak(("produced", 0, "run_id"), "run-other"), "different run"),
            (lambda run: run.update(handoffs={**records, "bytes": []}), "unsupported lists"),
        ]
        for change, message in cases:
            with self.subTest(message=message):
                export = Path(tempfile.mkdtemp(prefix="exo-public-handoff-leak-", dir="/tmp"))
                self._write_public_export_fixture(export)
                self._rewrite_snapshot(export, change)
                with self.assertRaisesRegex(ValueError, message):
                    qualification.attest_public_exports(export)

    def test_s33_accepts_the_runtime_snapshot_freshness_scope_fields_only(self):
        """Live Observation snapshots carry schema-declared scope fields (snapshot.schema.json)."""
        def snapshot(freshness):
            return {"schema_version": 1, "cursor": "c1", "captured_at": "2026-10-03T12:00:00Z",
                    "freshness": {"status": "fresh", "observed_at": "2026-10-03T12:00:00Z", **freshness},
                    "state": {"factory": {"id": "factory-1", "graph": {"nodes": [], "edges": []}},
                              "runs": [], "active_publication": None, "capacity": None,
                              "commercial": {"usage": [], "obligations": [], "payments": []}}}
        for freshness in ({}, {"unavailable_run_ids": []}, {"scope": "factory", "unavailable_run_ids": []},
                          {"scope": "run", "run_id": "run-1", "included_run_ids": ["run-1", "run-1:child:a"],
                           "factory_status": "stale", "unavailable_run_ids": ["run-2"]}):
            with self.subTest(accepted=freshness):
                qualification._validate_snapshot_envelope(snapshot(freshness))
        for freshness in ({"scope": "tenant"}, {"scope": "factory", "run_id": "run-1"},
                          {"run_id": "run-1"}, {"scope": "run", "run_id": "run-1"},
                          {"scope": "run", "run_id": "run-1", "included_run_ids": [], "factory_status": "fresh"},
                          {"scope": "factory", "unavailable_run_ids": ["run-1", "run-1"]},
                          {"scope": "factory", "unavailable_run_ids": ["secret text with spaces"]},
                          {"scope": "factory", "note": "x"}):
            with self.subTest(rejected=freshness), self.assertRaisesRegex(ValueError, "freshness|identifier"):
                qualification._validate_snapshot_envelope(snapshot(freshness))

    def test_public_export_attestation_names_missing_canonical_inputs(self):
        export = Path(tempfile.mkdtemp(prefix="exo-public-incomplete-", dir="/tmp"))
        (export / "snapshot.json").write_text("{}", encoding="utf-8")
        (export / "bundle.json").write_text("{}", encoding="utf-8")
        with self.assertRaisesRegex(
                ValueError,
                r"required public export files are unavailable: usage\.json, task-bindings\.json, artifacts\.json"):
            qualification.attest_public_exports(export)

    def test_collector_one_route_cannot_pass_all_inventory_routes_or_g4(self):
        routes = ["route1", "route2", "route3"]
        declared = [{"route_id": route, "path": filename}
                    for route in routes for filename in ("parent.json", "child.json", "report.md")]
        expected_paths = {f"{row['route_id']}/{row['path']}" for row in declared}
        present = {"route1/parent.json", "route1/child.json", "route1/report.md", "route1/extra.txt"}
        result = qualification.assess_collector_coverage(
            {"route_ids": routes, "declared_exports": declared},
            {"reported_status": "fail", "observed_route_ids": ["route1"],
             "present_exports": sorted(present), "scan_paths": sorted(present),
             "leak_hits": 0, "redaction_pass": True, "positive_control_detected": True},
        )
        self.assertEqual("fail", result["reported_status_preserved"])
        self.assertEqual(9, result["declared_export_count"])
        self.assertEqual(["route2", "route3"], result["missing_route_ids"])
        self.assertFalse(result["route_scope_complete"])
        self.assertEqual("fail", result["g4_status"])
        self.assertEqual("fail", result["overall_status"])
        self.assertIn("inconclusive", result["leak_conclusion"])
        self.assertNotIn("no findings", result["leak_conclusion"])
        self.assertEqual(6, len(expected_paths - present))

    def test_collector_g4_derives_minimum_from_inventory_and_requires_scan_controls(self):
        routes = ["route1", "route2", "route3"]
        declared = [{"route_id": route, "path": filename}
                    for route in routes for filename in ("parent.json", "child.json", "report.md")]
        paths = sorted(f"{row['route_id']}/{row['path']}" for row in declared)
        summary = {"reported_status": "fail", "observed_route_ids": routes,
                   "present_exports": paths, "scan_paths": paths, "leak_hits": 0,
                   "redaction_pass": True, "positive_control_detected": True}
        result = qualification.assess_collector_coverage(
            {"route_ids": routes, "declared_exports": declared}, summary)
        self.assertEqual(9, result["declared_export_count"])
        self.assertEqual("pass", result["g4_status"])
        self.assertEqual("pass", result["overall_status"])
        self.assertEqual("fail", result["reported_status_preserved"])

        summary["present_exports"] = paths[:-1]
        summary["scan_paths"] = paths[:-1]
        missing = qualification.assess_collector_coverage(
            {"route_ids": routes, "declared_exports": declared}, summary)
        self.assertEqual("fail", missing["g4_status"])
        self.assertIn("inconclusive", missing["leak_conclusion"])


if __name__ == "__main__":
    unittest.main()
