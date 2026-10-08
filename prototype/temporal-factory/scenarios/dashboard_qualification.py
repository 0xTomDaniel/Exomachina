#!/usr/bin/env python3
"""Fail-closed preflight and safe evidence gate for dashboard qualification.

This module does not start services, call model providers, send payments, or
read internal Temporal state. ``preflight`` checks public Interface artifacts,
dashboard source wiring, the declared adapter test file, and explicit runtime
prerequisites. Smoke evidence remains a separately reviewed input; this runner
never promotes a row to qualified on its own.
"""

from __future__ import annotations

import argparse
import ast
from datetime import datetime, timezone
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Mapping


FACTORY_ROOT = Path(__file__).resolve().parents[1]
DOCUMENTED_TEMPORAL_RUNTIME = Path("/tmp/exomachina-countertrials/temporal/runtime")
DOCUMENTED_TEMPORAL_CLI = Path("/tmp/exomachina-temporal-evaluation/temporal")
PROVISIONED_TEMPORAL_RUNTIME = Path("/private/tmp/exo-v2-temporal-9u041h3e/server")
PROVISIONED_TEMPORAL_SERVER = PROVISIONED_TEMPORAL_RUNTIME / "temporal-server"
PROVISIONED_TEMPORAL_SERVER_ARCHIVE_SHA256 = "f95748376241f5941327fa4c4e8e76641e8c4a9acabf77de9c86eb3d8238f4d7"
PROVISIONED_TEMPORAL_SERVER_SHA256 = "f1663788fd4d8d702576b659db212b4d45a8dbfc0909eb7f7a86d0d442de6a60"
PROVISIONED_TEMPORAL_CLI = Path("/private/tmp/exo-v2-temporal-9u041h3e/cli/temporal")
PROVISIONED_TEMPORAL_CLI_ARCHIVE_SHA256 = "41e0425378fcb4fb5766340b97435e20fe47bbff2d7bf644ec2d51f7662b7c56"
PROVISIONED_TEMPORAL_CLI_SHA256 = "33c298d41f6eefebf01a28a3511f444480e939e21d3518cb0c12a4abad467c4b"
DEFAULT_POSTGRES_BIN = Path("/opt/homebrew/opt/postgresql@16/bin")
PINNED_PYTHON = Path(
    "/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/"
    "tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python"
)

# These are the documented historical blocks. A clear check is only a point-
# in-time observation; the lead must allocate a separate block for each run.
PORT_FILTERS = (
    "32400-32484",
    "44000-44412",
    "44800-44861",
    "45200-45619",
    "46100-46349",
)

S_ROWS = (
    ("S01", "Brief submission and identity"),
    ("S02", "Pinned graph and readouts"),
    ("S03", "Parallel work and join"),
    ("S04", "First-pass acceptance"),
    ("S05", "Rejection and repair"),
    ("S06", "Exhaustion and abort"),
    ("S07", "Extra repair"),
    ("S08", "Human escalation"),
    ("S09", "Wait expiry and stale response"),
    ("S10", "Decision lifecycle and concurrency"),
    ("S11", "Inference attribution"),
    ("S12", "Hosting and markup"),
    ("S13", "Reservations and budget limits"),
    ("S14", "Usage charging and credits"),
    ("S15", "Capacity and admission queues"),
    ("S16", "Shared-agent contention"),
    ("S17", "Versions in flight"),
    ("S18", "Nested factory and fan-out"),
    ("S19", "Nested outcome unknown"),
    ("S20", "Approval and send-back"),
    ("S21", "Small graph without a gate"),
    ("S22", "Artifact lineage and delivery"),
    ("S23", "Worker/harness restart"),
    ("S24", "Incident, alarm, and acknowledgement"),
    ("S25", "Maintenance"),
    ("S26", "Improvement"),
    ("S27", "Optional research program"),
    ("S28", "Snapshot, reconnect, and gaps"),
    ("S29", "Multiple observers and slow consumer"),
    ("S30", "All dashboard views"),
    ("S31", "Demo isolation and parity"),
    ("S32", "Live presentation"),
    ("S33", "Observation secrecy"),
)

P_ROWS = (
    ("P01", "MPP metered session"),
    ("P02", "x402 usage scheme"),
    ("P03", "Protocol-specific fixed charge"),
    ("P04", "Purchase binding"),
    ("P05", "AP2 authorization mapping"),
    ("P06", "Nested authority"),
    ("P07", "Payment concurrency/retry"),
    ("P08", "Settlement uncertainty"),
    ("P09", "Failure and refund/credit"),
    ("P10", "Outcome charging"),
)

ROW_IDS = tuple(row_id for row_id, _ in (*S_ROWS, *P_ROWS))
VALID_EVIDENCE_LABELS = {
    "observed-real",
    "observed-synthetic",
    "unit-tested",
    "unqualified",
}

OBSERVATION_FILES = (
    "src/observation.py",
    "src/observation_transport.py",
    "specs/dashboard-asyncapi.yaml",
    "schemas/dashboard/v1/client-message.schema.json",
    "schemas/dashboard/v1/server-message.schema.json",
    "schemas/dashboard/v1/event.schema.json",
    "schemas/dashboard/v1/event-data.schema.json",
    "schemas/dashboard/v1/snapshot.schema.json",
)
DASHBOARD_CORE_FILES = (
    "dashboard/contract.mjs",
    "dashboard/reducer.mjs",
)
DASHBOARD_ADAPTER_FILES = (
    "dashboard/adapters/recorded.mjs",
    "dashboard/adapters/demo.mjs",
    "dashboard/adapters/live.mjs",
)
DASHBOARD_PAGE = "../../docs/design/exomachina-floor.html"
DASHBOARD_NODE_TEST = "dashboard/test/dashboard.test.mjs"

PUBLIC_EXPORT_FILES = {
    "snapshot": "snapshot.json",
    "bundle": "bundle.json",
    "usage": "usage.json",
    "task_bindings": "task-bindings.json",
    "artifacts": "artifacts.json",
}
MAX_PUBLIC_JSON_BYTES = 8 * 1024 * 1024
MAX_PUBLIC_ARTIFACT_BYTES = 16 * 1024 * 1024
MAX_PUBLIC_ROWS = 1000
MAX_PUBLIC_COLLECTION_ITEMS = 50000
PUBLIC_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}$")
SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")
USAGE_CATEGORIES = (
    "input_tokens", "output_tokens", "cache_read_tokens",
    "cache_write_tokens", "total_tokens",
)
USAGE_FIELDS = {
    "measurement_id", "model_call_id", "recorded_at", "call_scope", "provider",
    "model_id", "reasoning_effort", "unit", "measurement_source",
    "completeness", "evidence_status", "usage", "service_identity", "task_id",
    "context_id", "message_id", "run_id", "definition_digest", "assignment_id", "attempt_id",
}
PUBLIC_EVENT_FIELDS = {
    "com.exomachina.factory.discovered.v1": {"name", "identity", "capability"},
    "com.exomachina.publication.activated.v1": {
        "publication_version", "graph_nodes", "service_bindings",
    },
    "com.exomachina.run.created.v1": {"state", "phase", "started_at", "graph_nodes"},
    "com.exomachina.run.state_changed.v1": {
        "state", "phase", "node", "repair_count", "max_repairs", "started_at",
        "ended_at", "wait_deadline",
    },
    "com.exomachina.assignment.state_changed.v1": {
        "capability", "provider_identity", "state", "started_at", "ended_at", "queue_position",
    },
    "com.exomachina.artifact.revised.v1": {
        "artifact_revision", "artifact_sha256", "previous_revision", "previous_sha256",
        "media_type", "byte_length", "author_identity",
    },
    "com.exomachina.quality.verdict.v1": {
        "artifact_revision", "artifact_sha256", "reviewer_identity", "accepted",
        "finding_count", "finding_codes",
    },
    "com.exomachina.decision.outcome.v1": {
        "decision_id", "action", "outcome", "expected_state", "resulting_state",
        "artifact_revision", "artifact_sha256", "actor_identity",
    },
    "com.exomachina.command.outcome.v1": {
        "command_id", "lifecycle", "outcome", "expected_state", "resulting_state",
        "artifact_revision", "artifact_sha256",
    },
    "com.exomachina.delivery.receipt.v1": {
        "receipt_id", "artifact_revision", "artifact_sha256", "destination_id",
        "delivered_at", "outcome",
    },
    "com.exomachina.incident.state_changed.v1": {
        "incident_id", "kind", "state", "owner_identity", "evidence_refs",
    },
    "com.exomachina.admission.state_changed.v1": {
        "admission_id", "state", "queue_position", "capacity_limit",
    },
    "com.exomachina.capacity.state_changed.v1": {
        "capacity_limit", "active_count", "queued_count",
    },
    "com.exomachina.commercial.usage.v1": {
        "usage_id", "unit", "quantity", "measurement_source", "completeness",
        "evidence_status", "model_id", "model_call_id", "service_identity", "reasoning_effort",
    },
    "com.exomachina.commercial.obligation.v1": {
        "obligation_id", "component", "offer_digest", "amount_atoms", "currency",
        "atomic_scale", "evidence_status", "price_basis", "payment_trigger", "markup_bps", "state",
    },
    "com.exomachina.commercial.payment.v1": {
        "payment_id", "network", "asset", "amount_atoms", "currency", "atomic_scale",
        "state", "receipt_id", "evidence_status",
    },
    # Content-free hand-off facts (A2A v1 mediation decision 4; S33 requalified
    # 2026-10-07). Exact fields at every depth; see _validate_public_handoff.
    "com.exomachina.handoff.produced.v1": {
        "node", "handoff_id", "handoff_revision", "produced_at", "items",
    },
    "com.exomachina.handoff.consumed.v1": {"node", "consumed_at", "inputs"},
    "com.exomachina.handoff.item_ready.v1": {
        "node", "handoff_id", "item_index", "part_kinds", "media_type", "ready_at",
    },
}
PUBLIC_HANDOFF_LISTS = {
    "produced": "com.exomachina.handoff.produced.v1",
    "consumed": "com.exomachina.handoff.consumed.v1",
    "ready": "com.exomachina.handoff.item_ready.v1",
}
PUBLIC_HANDOFF_COMMON_FIELDS = {"factory_id", "run_id", "assignment_id", "attempt_id"}
PUBLIC_HANDOFF_ITEM_FIELDS = {
    "item_index", "source", "part_kinds", "media_type", "byte_length", "ready_at", "digest",
    "artifact_revision", "artifact_sha256",
}
PUBLIC_HANDOFF_PART_KINDS = {"text", "data", "raw", "url"}
PUBLIC_MEDIA_TYPE = re.compile(r"^[a-z0-9.+-]+/[a-z0-9.+-]+$")
PUBLIC_EVENT_COMMON_FIELDS = {
    "factory_id", "run_id", "task_id", "context_id", "assignment_id", "attempt_id",
    "manifest_digest", "package_digest", "definition_digest", "interpreter_build",
}
PUBLIC_EVENT_REQUIRED_FIELDS = {
    "com.exomachina.assignment.state_changed.v1": {
        "run_id", "task_id", "assignment_id", "attempt_id", "capability", "state",
    },
    "com.exomachina.artifact.revised.v1": {"run_id", "artifact_revision", "artifact_sha256"},
    "com.exomachina.quality.verdict.v1": {
        "run_id", "task_id", "artifact_revision", "artifact_sha256", "reviewer_identity",
        "accepted", "finding_count",
    },
}
PUBLIC_RUN_RECORD_EVENT_TYPES = {
    "assignments": "com.exomachina.assignment.state_changed.v1",
    "artifacts": "com.exomachina.artifact.revised.v1",
    "quality": "com.exomachina.quality.verdict.v1",
    "decisions": "com.exomachina.decision.outcome.v1",
    "commands": "com.exomachina.command.outcome.v1",
    "delivery": "com.exomachina.delivery.receipt.v1",
    "incidents": "com.exomachina.incident.state_changed.v1",
    "admissions": "com.exomachina.admission.state_changed.v1",
}
PUBLIC_EVENT_DATA_SCHEMA = FACTORY_ROOT / "schemas/dashboard/v1/event-data.schema.json"

# This is a safe evidence schema, not a general event serializer. Raw event
# bodies, prompts, artifact bytes, Temporal history, auth material, and
# mandate presentations have no representation here.
SAFE_TOP_LEVEL_KEYS = {
    "schema_version", "report_type", "created_at_utc", "status",
    "evidence_label", "claim_ids", "execution_mode", "interface_level",
    "model", "graph", "bindings", "contracts", "task_refs", "attempts",
    "usage", "artifact_refs", "evidence_refs", "interface_results",
    "checks", "prerequisites", "blockers",
}
SAFE_NESTED_KEYS = {
    "id", "name", "version", "digest", "sha256", "status", "kind",
    "provider", "access_method", "reasoning_effort", "verification_status",
    "factory_id", "run_id", "definition_digest", "package_digest",
    "manifest_digest", "build_id", "contract_digests", "task_id",
    "assignment_id", "attempt_id", "state", "index", "started_at",
    "ended_at", "evidence_ref", "unit", "source", "completeness", "records",
    "amount_atomic", "currency", "scale", "amount_evidence", "revision",
    "acceptance_ref", "delivery_ref", "label", "uri", "interface",
    "operation", "event_ids", "cursor_from", "cursor_to", "passed",
    "observed", "path", "reason", "profile", "row_id",
}
SENSITIVE_KEY = re.compile(
    r"(?i)(token|secret|password|authorization|credential|run.?inputs?|"
    r"history|system.?prompt|prompt|content|raw|signing|mandate|wallet|headers)"
)
SENSITIVE_VALUE = re.compile(
    r"(?i)(\bbearer\s+\S+|eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}|"
    r"\bsk-[A-Za-z0-9_-]{12,}|\b(?:access|refresh)_token\s*[:=])"
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _required_files(root: Path, paths: tuple[str, ...]) -> list[str]:
    return [path for path in paths if not (root / path).is_file()]


def _lsof_port_check(
    filters: tuple[str, ...] = PORT_FILTERS,
    *,
    executable_finder: Callable[[str], str | None] = shutil.which,
) -> dict[str, Any]:
    executable = executable_finder("lsof")
    if not executable:
        return {"status": "unknown", "reason": "lsof is unavailable", "listeners": None}
    command = [executable, "-nP"]
    for port_filter in filters:
        command.extend((f"-iTCP:{port_filter}",))
    command.extend(("-sTCP:LISTEN",))
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False, timeout=5)
    except (OSError, subprocess.TimeoutExpired):
        return {"status": "unknown", "reason": "lsof check failed", "listeners": None}
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    listener_lines = lines[1:] if lines and lines[0].startswith("COMMAND ") else lines
    if result.returncode not in (0, 1):
        return {"status": "unknown", "reason": "lsof returned an error", "listeners": None}
    return {
        "status": "clear" if not listener_lines else "occupied",
        "listeners": len(listener_lines),
        "filters": list(filters),
    }


def _file_group(root: Path, paths: tuple[str, ...]) -> dict[str, Any]:
    missing = _required_files(root, paths)
    return {
        "status": "available" if not missing else "missing",
        "required_files": list(paths),
        "missing_files": missing,
    }


def _has_call(root: Path, relative_path: str, function_name: str) -> bool:
    """Check for an actual application call, not an import or comment."""
    path = root / relative_path
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (OSError, SyntaxError, UnicodeError):
        return False
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Name) and func.id == function_name:
            return True
        if isinstance(func, ast.Attribute) and func.attr == function_name:
            return True
    return False


def _dashboard_page_adapter_wiring(root: Path) -> dict[str, Any]:
    """Report direct adapter references in the dashboard page without running it."""
    page = root / DASHBOARD_PAGE
    result: dict[str, Any] = {
        "page": DASHBOARD_PAGE,
        "status": "missing",
        "adapter_references": [],
        "missing_adapter_references": [
            "recorded", "demo", "live",
        ],
    }
    try:
        source = page.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return result

    references = {
        "recorded": ("adapters/recorded.mjs", "createRecordedAdapter"),
        "demo": ("adapters/demo.mjs", "createDemoAdapter"),
        "live": ("adapters/live.mjs", "createLiveAdapter"),
    }
    found = [
        name for name, markers in references.items()
        if any(marker in source for marker in markers)
    ]
    result["adapter_references"] = found
    result["missing_adapter_references"] = [name for name in references if name not in found]
    result["status"] = "referenced" if not result["missing_adapter_references"] else "not_detected"
    return result


def _payment_adapter_contract(root: Path) -> dict[str, Any]:
    """Inspect Commerce's explicit public seam and declarative profile metadata.

    A declaration is source evidence only. It does not imply a configured or
    network-ready Adapter, and does not change any P-row qualification status.
    """
    relative_path = "src/commercial.py"
    path = root / relative_path
    result: dict[str, Any] = {
        "source": relative_path,
        "seam_declared": False,
        "profiles_declared": [],
        "required_metadata_present": False,
        "network_ready": False,
        "qualification_status": "unqualified",
    }
    if not path.is_file():
        result["missing_file"] = relative_path
        return result
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (OSError, SyntaxError, UnicodeError):
        result["reason"] = "commercial module is unreadable or invalid Python"
        return result

    public_declarations: list[tuple[str, ast.AST | None]] = []
    for node in tree.body:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            public_declarations.append((node.name, node))
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if isinstance(target, ast.Name):
                    public_declarations.append((target.id, node.value))

    def normalized(value: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", value.casefold())

    result["seam_declared"] = any(
        "paymentadapter" in normalized(name)
        for name, _node in public_declarations
        if not name.startswith("_")
    )

    required_profiles = {"mpp", "x402", "ap2"}
    declared_profiles: set[str] = set()
    valid_profiles: set[str] = set()
    for name, node in public_declarations:
        if name.startswith("_") or node is None:
            continue
        try:
            value = ast.literal_eval(node)
        except (ValueError, TypeError, SyntaxError):
            continue
        if not isinstance(value, (Mapping, list, tuple)):
            continue
        rows: list[tuple[str, Mapping[str, Any]]] = []
        if isinstance(value, Mapping):
            for key, item in value.items():
                if isinstance(item, Mapping):
                    rows.append((str(key), item))
                elif isinstance(item, (list, tuple)):
                    rows.extend(
                        (str(key), record)
                        for record in item if isinstance(record, Mapping)
                    )
        elif isinstance(value, (list, tuple)):
            rows.extend(("", record) for record in value if isinstance(record, Mapping))

        for key, declaration in rows:
            row_labels = [key]
            row_labels.extend(
                str(declaration[field])
                for field in ("protocol", "profile")
                if isinstance(declaration.get(field), str)
            )
            row_profiles = {
                profile for profile in required_profiles
                if any(re.search(rf"(?i)(?<![a-z0-9]){re.escape(profile)}(?![a-z0-9])", label)
                       for label in row_labels)
            }
            if len(row_profiles) != 1:
                continue
            profile = next(iter(row_profiles))
            declared_profiles.add(profile)
            normalized_keys = {normalized(str(field)) for field in declaration}
            has_metadata = (
                ("protocol" in normalized_keys or "profile" in normalized_keys)
                and "version" in normalized_keys
                and "operations" in normalized_keys
                and ("prerequisites" in normalized_keys or "environmentprerequisites" in normalized_keys)
                and any(key in normalized_keys for key in ("status", "versionstatus", "configured"))
            )
            version = declaration.get("version")
            # AP2's declared specification version is exactly 0.2. Check the
            # version field itself; a profile key such as "ap2-v0.2" cannot
            # conceal a wrong or missing version value.
            has_required_version = (
                profile != "ap2"
                or normalized(str(version)) in {"02", "v02"}
            )
            if has_metadata and has_required_version:
                valid_profiles.add(profile)

    result["profiles_declared"] = sorted(declared_profiles)
    result["required_metadata_present"] = required_profiles.issubset(valid_profiles)
    if not result["seam_declared"]:
        result["reason"] = "Commerce has no public Payment Adapter seam declaration"
    elif not required_profiles.issubset(valid_profiles):
        result["reason"] = (
            "MPP, x402, and AP2 v0.2 declarations must pin profile/version, "
            "operations, environment prerequisites, and status"
        )
    else:
        result["reason"] = "declarations exist; profiles remain unconfigured and unqualified"
    return result


def _sha256_file(path: Path) -> str | None:
    try:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()
    except OSError:
        return None


def verify_pinned_binary(
    configured_path: str | None,
    *,
    expected_path: Path,
    expected_sha256: str,
) -> dict[str, Any]:
    """Verify one explicitly configured binary; never search for alternatives."""
    if not configured_path:
        return {
            "status": "unconfigured",
            "configured_path": None,
            "expected_path": str(expected_path),
            "expected_sha256": expected_sha256,
            "observed_sha256": None,
        }
    candidate = Path(configured_path)
    try:
        same_path = candidate.resolve(strict=True) == expected_path.resolve(strict=True)
    except OSError:
        same_path = False
    if not same_path or not candidate.is_file() or not (candidate.stat().st_mode & 0o111):
        return {
            "status": "path_mismatch_or_non_executable",
            "configured_path": configured_path,
            "expected_path": str(expected_path),
            "expected_sha256": expected_sha256,
            "observed_sha256": None,
        }
    observed = _sha256_file(candidate)
    return {
        "status": "verified" if observed == expected_sha256 else "hash_mismatch",
        "configured_path": configured_path,
        "expected_path": str(expected_path),
        "expected_sha256": expected_sha256,
        "observed_sha256": observed,
    }


def build_preflight(
    root: Path = FACTORY_ROOT,
    *,
    environment: Mapping[str, str] | None = None,
    executable_finder: Callable[[str], str | None] = shutil.which,
    port_check: Callable[[], dict[str, Any]] = _lsof_port_check,
    now: Callable[[], str] = _utc_now,
) -> dict[str, Any]:
    """Build a no-network readiness report; never substitute runtime binaries."""
    root = Path(root)
    environment = os.environ if environment is None else environment
    blockers: list[str] = []

    observation = _file_group(root, OBSERVATION_FILES)
    observation_mounted = _has_call(
        root, "src/harness.py", "install_observation_transport"
    )
    observation["runtime_mount"] = observation_mounted
    if observation["missing_files"]:
        observation["status"] = "missing"
        observation["reason"] = "observation source or protocol schemas are incomplete"
    elif not observation_mounted:
        observation["status"] = "partial"
        observation["reason"] = (
            "projection, transport, and schemas exist, but Runtime has not mounted "
            "the transport in the A2A application"
        )

    dashboard_core = _file_group(root, DASHBOARD_CORE_FILES)
    dashboard_adapters = _file_group(root, DASHBOARD_ADAPTER_FILES)
    dashboard_page_wiring = _dashboard_page_adapter_wiring(root)
    dashboard_artifacts_complete = (
        not dashboard_core["missing_files"] and not dashboard_adapters["missing_files"]
    )
    dashboard = {
        "status": (
            "missing" if dashboard_core["missing_files"]
            else "partial" if dashboard_adapters["missing_files"]
            else "available" if dashboard_page_wiring["status"] == "referenced"
            else "partial"
        ),
        "artifact_status": "available" if dashboard_artifacts_complete else "incomplete",
        "required_files": [*DASHBOARD_CORE_FILES, *DASHBOARD_ADAPTER_FILES],
        "missing_files": [*dashboard_core["missing_files"], *dashboard_adapters["missing_files"]],
        "page_wiring": dashboard_page_wiring,
    }
    if dashboard["status"] == "partial" and dashboard_adapters["missing_files"]:
        dashboard["reason"] = "browser contract and reducer exist, but one or more adapters are absent"
    elif dashboard["status"] == "partial":
        dashboard["reason"] = (
            "contract, reducer, and adapters exist, but the floor page source does not "
            "reference all three adapters"
        )

    dashboard_node_test = {
        "path": DASHBOARD_NODE_TEST,
        "status": "available" if (root / DASHBOARD_NODE_TEST).is_file() else "missing",
        "command": ["node", "--test", DASHBOARD_NODE_TEST],
        "working_directory": "prototype/temporal-factory",
    }

    commercial = _file_group(root, ("src/commercial.py",))
    payment_adapter = _payment_adapter_contract(root)
    payment_adapter["status"] = (
        "missing" if payment_adapter.get("missing_file")
        else "declared" if (
            payment_adapter["seam_declared"]
            and payment_adapter["required_metadata_present"]
            and {"mpp", "x402", "ap2"}.issubset(payment_adapter["profiles_declared"])
        )
        else "partial"
    )
    interfaces: dict[str, dict[str, Any]] = {
        "a2a": _file_group(root, ("src/harness.py", "src/long_client.py")),
        "observation": observation,
        "dashboard": dashboard,
        "commercial": commercial,
        "payment_adapter": payment_adapter,
    }

    for name, interface in interfaces.items():
        if interface["status"] not in {"available", "declared"}:
            missing = ", ".join(interface.get("missing_files", [])) or interface.get(
                "reason", "public contract declarations are incomplete"
            )
            blockers.append(f"{name} public Interface unavailable: {missing}")
    if dashboard_node_test["status"] != "available":
        blockers.append(f"dashboard Node adapter tests unavailable: {DASHBOARD_NODE_TEST}")

    server_binary = verify_pinned_binary(
        environment.get("EXO_TEMPORAL_RUNTIME") and str(
            Path(environment["EXO_TEMPORAL_RUNTIME"]) / "temporal-server"
        ),
        expected_path=PROVISIONED_TEMPORAL_SERVER,
        expected_sha256=PROVISIONED_TEMPORAL_SERVER_SHA256,
    )
    temporal_cli = verify_pinned_binary(
        environment.get("EXO_TEMPORAL_CLI"),
        expected_path=PROVISIONED_TEMPORAL_CLI,
        expected_sha256=PROVISIONED_TEMPORAL_CLI_SHA256,
    )
    runtime = {
        "python": {
            "path": str(PINNED_PYTHON),
            "available": PINNED_PYTHON.is_file() and bool(PINNED_PYTHON.stat().st_mode & 0o111),
        },
        "node": {"path": executable_finder("node"), "available": bool(executable_finder("node"))},
        "npm": {"path": executable_finder("npm"), "available": bool(executable_finder("npm"))},
        "postgres": {
            "path": str(DEFAULT_POSTGRES_BIN),
            "available": all((DEFAULT_POSTGRES_BIN / name).is_file() for name in ("initdb", "pg_ctl", "postgres")),
        },
        "temporal_server": {
            **server_binary,
            "release_archive_sha256": PROVISIONED_TEMPORAL_SERVER_ARCHIVE_SHA256,
            "release_archive_sha256_source": "operator-verified server.tar.gz",
            "executable_sha256": PROVISIONED_TEMPORAL_SERVER_SHA256,
            "environment_variable": "EXO_TEMPORAL_RUNTIME",
            "configured_runtime_directory": environment.get("EXO_TEMPORAL_RUNTIME"),
            "documented_default": str(DOCUMENTED_TEMPORAL_RUNTIME),
            "reported_version": "1.32.0",
            "version_source": "operator-provisioning note",
            "available": server_binary["status"] == "verified",
            "fallback_used": False,
        },
        "temporal_cli": {
            **temporal_cli,
            "release_archive_sha256": PROVISIONED_TEMPORAL_CLI_ARCHIVE_SHA256,
            "release_archive_sha256_source": "operator-verified cli.tar.gz",
            "executable_sha256": PROVISIONED_TEMPORAL_CLI_SHA256,
            "environment_variable": "EXO_TEMPORAL_CLI",
            "documented_default": str(DOCUMENTED_TEMPORAL_CLI),
            "reported_version": "1.9.1",
            "server_version_reported_by_cli": "1.32.0",
            "version_source": "operator-provisioning note",
            "available": temporal_cli["status"] == "verified",
            "fallback_used": False,
        },
    }
    for name, item in runtime.items():
        if not item["available"]:
            configured_path = item.get("configured_path")
            expected_path = item.get("expected_path")
            path = item.get("path") or configured_path or expected_path
            env_name = item.get("environment_variable")
            if item.get("status") == "unconfigured" and env_name:
                blockers.append(f"{env_name} is unset; explicitly configure the pinned {name} path")
            elif item.get("status") == "hash_mismatch":
                blockers.append(
                    f"runtime prerequisite hash mismatch: {name} "
                    f"(expected {item.get('expected_sha256')}, observed {item.get('observed_sha256')})"
                )
            else:
                blockers.append(f"runtime prerequisite unavailable: {name} ({path})")

    ports = port_check()
    if ports.get("status") != "clear":
        blockers.append(f"documented qualification port blocks are {ports.get('status', 'unknown')}")

    # Do not inspect credentials, initiate sign-in, or infer broker readiness.
    author_provider = environment.get("EXO_AUTHOR_PROVIDER")
    author_model = environment.get("EXO_AUTHOR_MODEL")
    model = {
        "required_id": "gpt-6-luna",
        "required_reasoning_effort": "xhigh",
        "verified_by_live_smoke": False,
        "access_method": "CLI --provider codex-subscription",
        "provider_readiness_status": "pending_default_custom_broker_status",
        "provider_readiness_source": "default custom broker status",
        "codex_cli_login_is_sufficient": False,
        "operator_authorized_for_fresh_workflow": True,
        "EXO_AUTHOR_PROVIDER_configured": bool(author_provider),
        "EXO_AUTHOR_MODEL_configured": bool(author_model),
        "live_inference_ready": False,
    }
    blockers.append(
        "default custom broker status is pending; provider readiness has not been established"
    )
    blockers.append("Luna xhigh access and support are not verified by a new live smoke")

    evidence = {
        "model": {"id": None, "reasoning_effort": None, "status": "not_observed"},
        "graph": {"definition_digest": None, "package_digest": None, "status": "not_observed"},
        "bindings": {"manifest_digest": None, "build_id": None, "status": "not_observed"},
        "contracts": [],
        "task_refs": [],
        "attempts": [],
        "usage": {"status": "not_observed", "records": []},
        "artifact_refs": [],
        "evidence_refs": [],
    }
    return {
        "schema_version": 1,
        "report_type": "dashboard-qualification-preflight",
        "created_at_utc": now(),
        "status": "ready" if not blockers else "blocked",
        "qualification_status": "unqualified",
        "evidence_label": "unqualified",
        "interfaces": interfaces,
        "public_operations": {
            "a2a": ["submit via the normal factory identity", "inspect original A2A Task"],
            "observation": ["discover", "snapshot", "observe", "inspect_artifact", "command", "submit"],
            "commercial": ["offer binding", "usage attribution", "reservation", "obligation", "credit", "settlement reconciliation"],
            "payment": ["MPP profile", "x402 profile", "AP2 authorization verification"],
        },
        "payment_adapter": {
            "owner": "Commerce",
            "separate_payment_lane": False,
            "declaration_status": payment_adapter["status"],
            "network_ready": False,
            "configured_profiles": [],
            "qualification_status": "unqualified",
            "declarations": payment_adapter,
        },
        "qualification_harnesses": {"dashboard_node": dashboard_node_test},
        "runtime": runtime,
        "ports": ports,
        "model_requirement": model,
        "evidence_fields": evidence,
        "matrix": {"S": {"total": len(S_ROWS), "qualified": 0},
                   "P": {"total": len(P_ROWS), "qualified": 0}},
        "blockers": blockers,
        "side_effects": [],
    }


def classify_evidence(record: Mapping[str, Any]) -> str:
    """Classify evidence without upgrading fixtures or old-model runs."""
    label = record.get("evidence_label")
    mode = record.get("execution_mode")
    if label == "observed-synthetic" or mode in {"demo", "fixture", "mock"}:
        return "observed-synthetic-only"
    if label == "unit-tested":
        return "unit-tested-only"
    if label != "observed-real" or mode != "live":
        return "unqualified"
    if record.get("interface_level") is not True or not record.get("evidence_refs"):
        return "unqualified"
    model = record.get("model") or {}
    if model.get("status") == "observed" and (
        model.get("id") != "gpt-6-luna" or model.get("reasoning_effort") != "xhigh"
    ):
        return "historical-partial"
    return "candidate-for-review"


def _check_safe_values(value: Any, *, path: str = "evidence") -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str) or SENSITIVE_KEY.search(key):
                raise ValueError(f"unsafe evidence key at {path}")
            if key not in SAFE_TOP_LEVEL_KEYS and key not in SAFE_NESTED_KEYS:
                raise ValueError(f"unallowlisted evidence key at {path}.{key}")
            _check_safe_values(item, path=f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _check_safe_values(item, path=f"{path}[{index}]")
    elif isinstance(value, str) and SENSITIVE_VALUE.search(value):
        raise ValueError(f"sensitive-looking evidence value at {path}")
    elif value is not None and not isinstance(value, (str, int, float, bool)):
        raise ValueError(f"unsupported evidence value at {path}")


def export_safe_evidence(record: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and copy the allowlisted evidence projection; never export raw data."""
    if not isinstance(record, Mapping):
        raise ValueError("evidence record must be an object")
    unknown = set(record) - SAFE_TOP_LEVEL_KEYS
    if unknown:
        raise ValueError("evidence record contains unallowlisted fields")
    _check_safe_values(record)
    return json.loads(json.dumps(record, ensure_ascii=False, allow_nan=False))


def _read_bounded_json(root: Path, relative: str, *, limit: int = MAX_PUBLIC_JSON_BYTES) -> tuple[Any, bytes]:
    """Read one named public export under root; never follow it outside the export set."""
    root = Path(root).resolve(strict=True)
    relative_path = Path(relative)
    if relative_path.is_absolute() or ".." in relative_path.parts:
        raise ValueError("public export path must be relative and contained")
    path = root / relative_path
    try:
        resolved = path.resolve(strict=True)
        if not resolved.is_relative_to(root) or not resolved.is_file():
            raise ValueError("public export is not a regular file inside the export directory")
        if resolved.stat().st_size > limit:
            raise ValueError("public export exceeds the size limit")
        raw = resolved.read_bytes()
    except OSError as exc:
        raise ValueError("required public export is unavailable") from exc
    if len(raw) > limit:
        raise ValueError("public export exceeds the size limit")
    try:
        parsed = json.loads(raw, parse_constant=lambda _value: (_ for _ in ()).throw(
            ValueError("non-finite number in public export")))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("public export is not valid UTF-8 JSON") from exc
    _scan_public_export(parsed)
    return parsed, raw


def _scan_public_export(value: Any, *, path: str = "export", depth: int = 0) -> None:
    """Reject secret or raw-payload fields; callers project only specific safe facts."""
    if depth > 32:
        raise ValueError("public export nesting exceeds the limit")
    if isinstance(value, Mapping):
        for key, item in value.items():
            safe_token_category = isinstance(key, str) and key in USAGE_CATEGORIES
            if not isinstance(key, str) or (not safe_token_category and SENSITIVE_KEY.search(key)):
                raise ValueError(f"sensitive field in public export at {path}")
            _scan_public_export(item, path=path, depth=depth + 1)
    elif isinstance(value, (list, tuple)):
        if len(value) > MAX_PUBLIC_COLLECTION_ITEMS:
            raise ValueError("public export collection exceeds the row limit")
        for item in value:
            _scan_public_export(item, path=path, depth=depth + 1)
    elif isinstance(value, str) and SENSITIVE_VALUE.search(value):
        raise ValueError(f"sensitive-looking value in public export at {path}")
    elif value is not None and not isinstance(value, (str, int, float, bool)):
        raise ValueError("unsupported value in public export")


def _expect_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    return value


def _expect_id(value: Any, label: str) -> str:
    if not isinstance(value, str) or not PUBLIC_ID.fullmatch(value):
        raise ValueError(f"{label} is not a valid public identifier")
    return value


def _expect_digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_HEX.fullmatch(value):
        raise ValueError(f"{label} is not a SHA-256 digest")
    return value


def _record_data(value: Any, label: str) -> Mapping[str, Any]:
    row = _expect_mapping(value, label)
    if isinstance(row.get("data"), Mapping) and isinstance(row.get("type"), str):
        return row["data"]
    return row


def _public_time(value: Any, label: str) -> None:
    if not isinstance(value, str) or len(value) > 64:
        raise ValueError(f"{label} must be a timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{label} must be a timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{label} must include a timezone")


def _public_part_kinds(value: Any, label: str) -> list[str]:
    if (not isinstance(value, list) or not value or len(value) > 64
            or any(kind not in PUBLIC_HANDOFF_PART_KINDS for kind in value)):
        raise ValueError(f"{label} part kinds are invalid")
    return value


def _public_media_type(value: Any, label: str) -> None:
    if value is not None and (not isinstance(value, str) or len(value) > 127
                              or not PUBLIC_MEDIA_TYPE.fullmatch(value)):
        raise ValueError(f"{label} media type is invalid")


def _validate_public_handoff(data: Any, event_type: str, label: str, *,
                             factory_id: str | None = None, run_id: str | None = None) -> None:
    """Exact content-free hand-off fact: no Task, pins, content, names or URLs at any depth."""
    data = _expect_mapping(data, label)
    allowed = {"schema_version", *PUBLIC_HANDOFF_COMMON_FIELDS, *PUBLIC_EVENT_FIELDS[event_type]}
    if set(data) - allowed:
        raise ValueError(f"{label} event data has unallowlisted fields")
    if set(data) != allowed:
        raise ValueError(f"{label} event data is missing required public fields")
    if data["schema_version"] != 1:
        raise ValueError(f"{label} event data schema_version must be 1")
    for field in (*sorted(PUBLIC_HANDOFF_COMMON_FIELDS), "node"):
        _expect_id(data[field], f"{label} {field}")
    if factory_id is not None and data["factory_id"] != factory_id:
        raise ValueError(f"{label} event data belongs to a different factory")
    if run_id is not None and data["run_id"] != run_id:
        raise ValueError(f"{label} event data belongs to a different run")
    if event_type == "com.exomachina.handoff.produced.v1":
        _expect_id(data["handoff_id"], f"{label} handoff id")
        if type(data["handoff_revision"]) is not int or data["handoff_revision"] < 1:
            raise ValueError(f"{label} handoff revision must be positive")
        _public_time(data["produced_at"], f"{label} produced_at")
        items = data["items"]
        if not isinstance(items, list) or not 1 <= len(items) <= 256:
            raise ValueError(f"{label} hand-off items must be a bounded non-empty list")
        for item in items:
            item = _expect_mapping(item, f"{label} item")
            if set(item) - PUBLIC_HANDOFF_ITEM_FIELDS:
                raise ValueError(f"{label} item has unallowlisted fields")
            if (PUBLIC_HANDOFF_ITEM_FIELDS - {"artifact_revision", "artifact_sha256"}) - set(item):
                raise ValueError(f"{label} item is missing required public fields")
            if item["source"] not in {"artifact", "message"}:
                raise ValueError(f"{label} item source is invalid")
            kinds = _public_part_kinds(item["part_kinds"], f"{label} item")
            _public_media_type(item["media_type"], f"{label} item")
            if item["byte_length"] is None and "url" not in kinds:
                raise ValueError(f"{label} item byte length may be null only for url parts")
            if item["byte_length"] is not None and (type(item["byte_length"]) is not int
                                                    or item["byte_length"] < 0):
                raise ValueError(f"{label} item byte length is invalid")
            _public_time(item["ready_at"], f"{label} item ready_at")
            _expect_digest(item["digest"], f"{label} item digest")
            report = {"artifact_revision", "artifact_sha256"} & set(item)
            if report:
                if len(report) != 2 or item["source"] != "artifact":
                    raise ValueError(f"{label} report reference is incomplete")
                _expect_id(item["artifact_revision"], f"{label} artifact revision")
                _expect_digest(item["artifact_sha256"], f"{label} artifact sha256")
    elif event_type == "com.exomachina.handoff.consumed.v1":
        _public_time(data["consumed_at"], f"{label} consumed_at")
        inputs = data["inputs"]
        if not isinstance(inputs, list) or not 1 <= len(inputs) <= 64:
            raise ValueError(f"{label} inputs must be a bounded non-empty list")
        for entry in inputs:
            entry = _expect_mapping(entry, f"{label} input")
            if set(entry) != {"handoff_id", "item_digests"}:
                raise ValueError(f"{label} input has unallowlisted fields")
            _expect_id(entry["handoff_id"], f"{label} input handoff id")
            digests = entry["item_digests"]
            if not isinstance(digests, list) or not 1 <= len(digests) <= 256:
                raise ValueError(f"{label} input digests must be a bounded non-empty list")
            for digest in digests:
                _expect_digest(digest, f"{label} input digest")
    else:
        _expect_id(data["handoff_id"], f"{label} handoff id")
        if type(data["item_index"]) is not int or not 0 <= data["item_index"] <= 255:
            raise ValueError(f"{label} item index is invalid")
        _public_part_kinds(data["part_kinds"], label)
        _public_media_type(data["media_type"], label)
        _public_time(data["ready_at"], f"{label} ready_at")


def _validate_public_record(value: Any, label: str, *, factory_id: str | None = None,
                            event_type: str | None = None) -> None:
    row = _expect_mapping(value, label)
    if row.get("specversion") == "1.0" and row.get("type") in PUBLIC_HANDOFF_LISTS.values():
        if set(row) != {"specversion", "id", "source", "type", "time", "subject",
                        "datacontenttype", "dataschema", "data"}:
            raise ValueError(f"{label} is not a supported public event")
        _validate_public_handoff(row.get("data"), row["type"], f"{label} event data",
                                 factory_id=factory_id)
        return
    if row.get("specversion") == "1.0":
        allowed_envelope = {"specversion", "id", "source", "type", "time", "subject",
                            "datacontenttype", "dataschema", "data"}
        if set(row) != allowed_envelope or row.get("type") not in PUBLIC_EVENT_FIELDS:
            raise ValueError(f"{label} is not a supported public event")
        if event_type is not None and row["type"] != event_type:
            raise ValueError(f"{label} event type does not match its snapshot row")
        data = _expect_mapping(row.get("data"), f"{label} event data")
        allowed_data = {"schema_version", *PUBLIC_EVENT_COMMON_FIELDS,
                        *PUBLIC_EVENT_FIELDS[row["type"]]}
        if set(data) - allowed_data:
            raise ValueError(f"{label} event data has unallowlisted fields")
        required = {"schema_version", "factory_id",
                    *PUBLIC_EVENT_REQUIRED_FIELDS.get(row["type"], set())}
        if not required.issubset(data):
            raise ValueError(f"{label} event data is missing required public fields")
        if data.get("schema_version") != 1:
            raise ValueError(f"{label} event data schema_version must be 1")
        if factory_id is not None and data.get("factory_id") != factory_id:
            raise ValueError(f"{label} event data belongs to a different factory")
        return
    # normalizedPublicRecord is defined by the checked-in v1 snapshot schema.
    schema_path = FACTORY_ROOT / "schemas/dashboard/v1/snapshot.schema.json"
    try:
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        allowed = set(schema["$defs"]["normalizedPublicRecord"]["properties"])
    except (OSError, KeyError, json.JSONDecodeError) as exc:
        raise ValueError("checked-in public snapshot schema is unavailable") from exc
    if "schema_version" in row or "factory_id" in row:
        if event_type is None:
            raise ValueError(f"{label} event-data row has no snapshot item schema")
        try:
            event_data_schema = json.loads(PUBLIC_EVENT_DATA_SCHEMA.read_text(encoding="utf-8"))
            event_data_allowed = set(event_data_schema["properties"])
            event_data_required = set(event_data_schema["required"])
        except (OSError, KeyError, json.JSONDecodeError) as exc:
            raise ValueError("checked-in public event-data schema is unavailable") from exc
        event_type_allowed = {"schema_version", *PUBLIC_EVENT_COMMON_FIELDS,
                              *PUBLIC_EVENT_FIELDS[event_type]}
        if set(row) - event_data_allowed or set(row) - event_type_allowed:
            raise ValueError(f"{label} event data has unallowlisted fields")
        if not event_data_required.issubset(row):
            raise ValueError(f"{label} event data is missing required public fields")
        if row.get("schema_version") != 1:
            raise ValueError(f"{label} event data schema_version must be 1")
        _expect_id(row.get("factory_id"), f"{label} factory id")
        if factory_id is not None and row["factory_id"] != factory_id:
            raise ValueError(f"{label} event data belongs to a different factory")
        required = PUBLIC_EVENT_REQUIRED_FIELDS.get(event_type, set())
        if not required.issubset(row):
            raise ValueError(f"{label} event data is missing required row fields")
        return
    if set(row) - allowed:
        raise ValueError(f"{label} has unallowlisted normalized public fields")


def _validate_snapshot_envelope(snapshot: Any) -> Mapping[str, Any]:
    record = _expect_mapping(snapshot, "snapshot")
    if set(record) != {"schema_version", "cursor", "captured_at", "freshness", "state"}:
        raise ValueError("snapshot envelope fields are unsupported")
    if record.get("schema_version") != 1:
        raise ValueError("snapshot schema_version must be 1")
    state = _expect_mapping(record.get("state"), "snapshot state")
    if set(state) != {"factory", "runs", "active_publication", "capacity", "commercial"}:
        raise ValueError("snapshot projection fields are unsupported")
    factory = _expect_mapping(state.get("factory"), "snapshot factory")
    if set(factory) - {"id", "name", "identity", "capability", "graph", "agent_bindings",
                       "agents", "source_label", "model_label", "fixture_label"}:
        raise ValueError("snapshot factory has unsupported public fields")
    _expect_id(factory.get("id"), "factory id")
    if not isinstance(factory.get("graph"), Mapping):
        raise ValueError("snapshot factory graph is missing")
    if not isinstance(record.get("cursor"), str) or not isinstance(record.get("captured_at"), str):
        raise ValueError("snapshot cursor and capture time are required")
    runs = state.get("runs")
    if not isinstance(runs, list) or len(runs) > 256:
        raise ValueError("snapshot runs must be a bounded array")
    if not isinstance(record.get("freshness"), Mapping):
        raise ValueError("snapshot freshness is missing")
    freshness = record["freshness"]
    if set(freshness) != {"status", "observed_at"} or freshness.get("status") not in {
        "fresh", "stale", "disconnected", "unknown",
    }:
        raise ValueError("snapshot freshness fields are unsupported")
    commercial = _expect_mapping(state.get("commercial"), "snapshot commercial projection")
    if set(commercial) != {"usage", "obligations", "payments"}:
        raise ValueError("snapshot commercial projection fields are unsupported")
    return record


def _select_run(snapshot: Mapping[str, Any], requested_run_id: str | None) -> Mapping[str, Any]:
    state = _expect_mapping(snapshot.get("state"), "snapshot state")
    factory = _expect_mapping(state.get("factory"), "snapshot factory")
    factory_id = _expect_id(factory.get("id"), "factory id")
    runs = state.get("runs")
    matches = [row for row in runs if isinstance(row, Mapping) and
               (requested_run_id is None or row.get("id") == requested_run_id)]
    if len(matches) != 1:
        raise ValueError("select exactly one public run; provide --run-id when needed")
    run = matches[0]
    _expect_id(run.get("id"), "run id")
    task = _expect_mapping(run.get("task"), "run Task binding")
    _expect_id(task.get("id"), "parent Task id")
    _expect_id(task.get("context_id"), "parent context id")
    for field in ("assignments", "artifacts", "quality", "decisions", "commands",
                  "delivery", "incidents", "admissions"):
        if not isinstance(run.get(field), list) or len(run[field]) > 256:
            raise ValueError(f"run {field} must be a bounded public record list")
        for row in run[field]:
            _validate_public_record(
                row, f"run {field} record", factory_id=factory_id,
                event_type=PUBLIC_RUN_RECORD_EVENT_TYPES[field],
            )
    if "handoffs" in run:
        handoffs = _expect_mapping(run["handoffs"], "run hand-off records")
        if set(handoffs) != set(PUBLIC_HANDOFF_LISTS):
            raise ValueError("run hand-off records have unsupported lists")
        for name, event_type in PUBLIC_HANDOFF_LISTS.items():
            rows = handoffs[name]
            if not isinstance(rows, list) or len(rows) > 256:
                raise ValueError(f"run hand-off {name} must be a bounded public record list")
            for row in rows:
                _validate_public_handoff(row, event_type, f"run hand-off {name} record",
                                         factory_id=factory_id, run_id=run["id"])
    if not isinstance(run.get("pinned"), Mapping):
        raise ValueError("run graph pins are missing")
    _validate_public_record(run["pinned"], "run pins", factory_id=factory_id)
    return run


def _graph_projection(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    nodes, edges = value.get("nodes"), value.get("edges")
    if not isinstance(nodes, list) or not isinstance(edges, list) or len(nodes) > 500 or len(edges) > 50000:
        return None
    node_ids: list[str] = []
    for node in nodes:
        if not isinstance(node, Mapping):
            return None
        try:
            node_ids.append(_expect_id(node.get("id"), "graph node id"))
        except ValueError:
            return None
    if not node_ids or len(set(node_ids)) != len(node_ids):
        return None
    for edge in edges:
        if not isinstance(edge, Mapping):
            return None
        if edge.get("from") not in node_ids or edge.get("to") not in node_ids:
            return None
    canonical = json.dumps(
        {"nodes": nodes, "edges": edges}, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False,
    ).encode("utf-8")
    return {"node_count": len(nodes), "edge_count": len(edges),
            "structure_sha256": hashlib.sha256(canonical).hexdigest()}


def _project_pins(run: Mapping[str, Any]) -> dict[str, Any]:
    pinned = _record_data(run.get("pinned", {}), "run pins")
    fields = ("manifest_digest", "package_digest", "definition_digest", "interpreter_build",
              "publication_version", "model_id", "model_label", "fixture_label")
    projected: dict[str, Any] = {}
    for field in fields:
        value = pinned.get(field)
        if value is None:
            continue
        if field.endswith("_digest"):
            projected[field] = _expect_digest(value, f"run pin {field}")
        elif field in {"interpreter_build", "model_id"}:
            projected[field] = _expect_id(value, f"run pin {field}")
        elif isinstance(value, str) and len(value) <= 128:
            projected[field] = value
    graph = _graph_projection(run.get("graph"))
    return {"pins": projected, "graph": graph,
            "complete": all(projected.get(name) for name in
                            ("manifest_digest", "package_digest", "definition_digest", "interpreter_build"))
                         and graph is not None}


def _project_task_bindings(value: Any, run: Mapping[str, Any], run_id: str,
                           bundle: Mapping[str, Any], snapshot_runs: list[Any]) -> dict[str, Any]:
    data = _expect_mapping(value, "Task binding export")
    if data.get("schema_version") != 1 or data.get("source") != "a2a-public-task-exports":
        raise ValueError("Task bindings must be exported from public A2A Task responses")
    if set(data) != {"schema_version", "source", "parent", "children"}:
        raise ValueError("Task binding export has unsupported fields")
    parent = _expect_mapping(data.get("parent"), "parent Task export")
    task = _expect_mapping(run.get("task"), "snapshot parent Task")
    parent_ref = {
        "task_id": _expect_id(parent.get("task_id"), "exported parent Task id"),
        "context_id": _expect_id(parent.get("context_id"), "exported parent context id"),
        "run_id": _expect_id(parent.get("run_id"), "exported parent run id"),
    }
    run_by_id = {_expect_id(candidate.get("id"), "snapshot run id"): candidate
                 for candidate in snapshot_runs if isinstance(candidate, Mapping)}
    parent_run = run_by_id.get(parent_ref["run_id"])
    parent_task = _expect_mapping(parent_run.get("task"), "snapshot parent Task") if parent_run else {}
    if (parent_task.get("id"), parent_task.get("context_id")) != (
            parent_ref["task_id"], parent_ref["context_id"]):
        raise ValueError("public A2A parent Task does not match its Observation run binding")
    children = data.get("children")
    if not isinstance(children, list) or len(children) > 100:
        raise ValueError("child Task exports must be a bounded array")
    seen_bindings: set[tuple[str, str, str]] = set()
    projected_children = []
    bundle_data: list[Mapping[str, Any]] = []
    for frame in bundle.get("frames", []):
        if isinstance(frame, Mapping) and frame.get("op") == "event":
            event = frame.get("event")
            if isinstance(event, Mapping):
                event_data = event.get("data")
                if isinstance(event_data, Mapping):
                    bundle_data.append(event_data)
    for child in children:
        row = _expect_mapping(child, "child Task export")
        base_fields = {"task_id", "context_id", "parent_task_id", "parent_context_id", "run_id"}
        run_relation_fields = base_fields | {"parent_run_id", "relationship"}
        if set(row) not in (base_fields, run_relation_fields):
            raise ValueError("child Task export has unsupported fields")
        item = {key: _expect_id(row.get(key), f"child Task {key}")
                for key in base_fields}
        relation = row.get("relationship", "delegated_a2a_task")
        if relation not in {"delegated_a2a_task", "same_original_task_run"}:
            raise ValueError("child Task relationship is unsupported")
        if (item["parent_task_id"] != parent_ref["task_id"] or
                item["parent_context_id"] != parent_ref["context_id"]):
            raise ValueError("child Task does not point to the exported parent Task and context")
        if relation == "same_original_task_run":
            parent_run_id = _expect_id(row.get("parent_run_id"), "child Task parent run id")
            child_run = run_by_id.get(item["run_id"])
            child_task = _expect_mapping(child_run.get("task"), "snapshot child Task") if child_run else {}
            if (parent_run_id != parent_ref["run_id"] or item["run_id"] == parent_run_id or
                    item["task_id"] != parent_ref["task_id"] or
                    item["context_id"] != parent_ref["context_id"] or
                    (child_task.get("id"), child_task.get("context_id")) !=
                    (item["task_id"], item["context_id"])):
                raise ValueError("same-original-Task child run does not match its snapshot binding")
            observed = True  # bundle.snapshot was checked byte-for-byte against Observation above
        elif "relationship" in row or "parent_run_id" in row:
            parent_run_id = _expect_id(row.get("parent_run_id"), "child Task parent run id")
            child_run = run_by_id.get(item["run_id"])
            child_task = _expect_mapping(child_run.get("task"), "snapshot child Task") if child_run else {}
            if (parent_run_id != parent_ref["run_id"] or item["run_id"] == parent_run_id or
                    item["task_id"] == parent_ref["task_id"] or
                    item["context_id"] == parent_ref["context_id"] or
                    (child_task.get("id"), child_task.get("context_id")) !=
                    (item["task_id"], item["context_id"])):
                raise ValueError("delegated child run does not match its snapshot binding")
            observed = True
        else:
            if (item["run_id"] != parent_ref["run_id"] or
                    item["task_id"] == parent_ref["task_id"] or
                    item["context_id"] == parent_ref["context_id"]):
                raise ValueError("delegated A2A Task identity must be distinct within its parent run")
            observed = any(event_data.get("run_id") == item["run_id"] and
                       event_data.get("task_id") == item["task_id"] and
                       event_data.get("context_id") == item["context_id"]
                       for event_data in bundle_data)
        relation_key = (item["run_id"], item["task_id"], item["context_id"])
        if relation_key in seen_bindings:
            raise ValueError("child Task/run bindings must be unique")
        seen_bindings.add(relation_key)
        projected_children.append({"task_id": item["task_id"], "context_id": item["context_id"],
                                   "parent_task_id": parent_ref["task_id"],
                                   "parent_context_id": parent_ref["context_id"],
                                   "parent_run_id": parent_ref["run_id"],
                                   "run_id": item["run_id"], "relationship": relation,
                                   "observed_in_bundle": observed})
    selected_task = _expect_mapping(run.get("task"), "selected run Task")
    selected_binding_matches = (
        run_id == parent_ref["run_id"] and
        (selected_task.get("id"), selected_task.get("context_id")) ==
        (parent_ref["task_id"], parent_ref["context_id"])
    ) or any(child["run_id"] == run_id and
             (child["task_id"], child["context_id"]) ==
             (selected_task.get("id"), selected_task.get("context_id"))
             for child in projected_children)
    if not selected_binding_matches:
        raise ValueError("selected run has no verified parent/child Task binding")
    run_ids = sorted({parent_ref["run_id"], *(child["run_id"] for child in projected_children)})
    return {"parent": parent_ref, "children": projected_children, "run_ids": run_ids,
            "selected_run_binding_matches": selected_binding_matches,
            "relationship_status": "verified" if projected_children and all(
                child["observed_in_bundle"] for child in projected_children) else "partial"}


def _project_usage(value: Any, selected_run_id: str, task_ids: set[str],
                   allowed_run_ids: set[str] | None = None) -> dict[str, Any]:
    export = _expect_mapping(value, "Usage export")
    if set(export) != {"measurements", "coverage"}:
        raise ValueError("Usage export must contain only measurements and coverage")
    rows = export.get("measurements")
    if not isinstance(rows, list) or len(rows) > MAX_PUBLIC_ROWS:
        raise ValueError("Usage measurements must be a bounded array")
    coverage = _expect_mapping(export.get("coverage"), "Usage coverage")
    projected = []
    model_pairs: set[tuple[str, str]] = set()
    for row in rows:
        item = _expect_mapping(row, "Usage measurement")
        if set(item) - USAGE_FIELDS:
            raise ValueError("Usage measurement contains an unallowlisted field")
        required = {"measurement_id", "model_call_id", "recorded_at", "call_scope", "provider",
                    "model_id", "unit", "measurement_source", "completeness", "evidence_status", "usage"}
        if not required.issubset(item):
            raise ValueError("Usage measurement is missing a public attribution field")
        for key in ("measurement_id", "model_call_id", "provider"):
            _expect_id(item[key], f"usage {key}")
        if not isinstance(item["recorded_at"], str):
            raise ValueError("usage recorded_at must be a timestamp")
        try:
            recorded_at = datetime.fromisoformat(item["recorded_at"].replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("usage recorded_at must be a timezone-qualified timestamp") from exc
        if recorded_at.tzinfo is None:
            raise ValueError("usage recorded_at must be timezone-qualified")
        if item["unit"] != "tokens" or item["call_scope"] not in {
            "authoring_overhead", "director_call", "assignment_call",
        }:
            raise ValueError("Usage measurement scope or unit is unsupported")
        if item["completeness"] not in {"complete", "partial", "unknown"}:
            raise ValueError("Usage completeness is invalid")
        if item["evidence_status"] not in {"provider_reported", "unknown"} or \
                item["measurement_source"] not in {"provider_reported", "unknown"}:
            raise ValueError("Usage evidence status is invalid")
        if (item["evidence_status"] == "unknown") != (item["measurement_source"] == "unknown"):
            raise ValueError("unknown usage source and evidence status must agree")
        safe_usage = _expect_mapping(item["usage"], "Usage categories")
        if set(safe_usage) != set(USAGE_CATEGORIES):
            raise ValueError("Usage categories must be explicit and complete")
        known = 0
        usage_projection: dict[str, dict[str, Any]] = {}
        for category in USAGE_CATEGORIES:
            fact = _expect_mapping(safe_usage[category], f"Usage category {category}")
            if set(fact) != {"status", "value"}:
                raise ValueError("Usage category must contain status and value only")
            if fact["status"] == "reported":
                if type(fact["value"]) is not int or fact["value"] < 0:
                    raise ValueError("reported token quantity must be a non-negative integer")
                known += 1
            elif fact["status"] == "unavailable":
                if fact["value"] is not None:
                    raise ValueError("unavailable token quantities must remain null")
            else:
                raise ValueError("token category status must be reported or unavailable")
            usage_projection[category] = {"status": fact["status"], "value": fact["value"]}
        expected_completeness = ("complete" if known == len(USAGE_CATEGORIES) else
                                 "partial" if known else "unknown")
        expected_evidence = "provider_reported" if known else "unknown"
        if item["completeness"] != expected_completeness or item["evidence_status"] != expected_evidence:
            raise ValueError("usage completeness does not match reported versus unavailable categories")
        binding: dict[str, Any] = {}
        for key in ("run_id", "task_id", "context_id", "definition_digest", "assignment_id",
                    "attempt_id", "message_id", "service_identity"):
            candidate = item.get(key)
            if candidate is not None:
                if key == "definition_digest":
                    binding[key] = _expect_digest(candidate, f"usage {key}")
                else:
                    binding[key] = _expect_id(candidate, f"usage {key}")
            else:
                binding[key] = None
        if binding["run_id"] not in (None, *(allowed_run_ids or {selected_run_id})):
            raise ValueError("Usage export contains a measurement bound to a different run")
        if binding["task_id"] is not None and binding["task_id"] not in task_ids:
            raise ValueError("Usage export contains an uncorrelated Task binding")
        if binding["task_id"] is None and item["call_scope"] != "authoring_overhead":
            raise ValueError("only authoring-overhead measurements may have an unknown Task binding")
        model_id = _expect_id(item["model_id"], "usage model id")
        effort = item.get("reasoning_effort")
        if effort is not None:
            effort = _expect_id(effort, "usage reasoning effort")
            model_pairs.add((model_id, effort))
        projected.append({
            "measurement_id": _expect_id(item["measurement_id"], "measurement id"),
            "model_call_id": _expect_id(item["model_call_id"], "model call id"),
            "recorded_at": item["recorded_at"],
            "provider": _expect_id(item["provider"], "usage provider"),
            "model_id": model_id,
            "reasoning_effort": effort,
            "call_scope": item["call_scope"],
            "unit": "tokens",
            "measurement_source": item["measurement_source"],
            "completeness": item["completeness"],
            "evidence_status": item["evidence_status"],
            "usage": usage_projection,
            **binding,
        })
    return {
        "status": "observed" if projected else "not_observed",
        "coverage_status": coverage.get("status") if isinstance(coverage.get("status"), str) else "unknown",
        "measurement_count": len(projected),
        "model_attribution_complete": bool(projected) and all(
            record["reasoning_effort"] is not None for record in projected),
        "records": projected,
        "model_pairs": [{"id": model, "reasoning_effort": effort}
                        for model, effort in sorted(model_pairs)],
    }


def _project_assignments(run: Mapping[str, Any], run_id: str) -> list[dict[str, Any]]:
    projected = []
    for raw in run["assignments"]:
        row = _record_data(raw, "assignment record")
        assignment_id = row.get("assignment_id") or row.get("id")
        if not isinstance(assignment_id, str):
            continue
        item: dict[str, Any] = {"assignment_id": _expect_id(assignment_id, "assignment id"),
                                "run_id": _expect_id(row.get("run_id", run_id), "assignment run id")}
        if item["run_id"] != run_id:
            raise ValueError("assignment belongs to a different run")
        for field in ("task_id", "context_id", "state", "capability"):
            candidate = row.get(field)
            if candidate is not None:
                item[field] = _expect_id(candidate, f"assignment {field}")
        attempts = row.get("attempts")
        if isinstance(attempts, list):
            if len(attempts) > 256:
                raise ValueError("assignment attempts exceed the bound")
            item["attempts"] = []
            for raw_attempt in attempts:
                attempt = _record_data(raw_attempt, "assignment attempt")
                attempt_id = attempt.get("attempt_id") or attempt.get("id")
                if attempt_id is None:
                    raise ValueError("public assignment attempt has no attempt identity")
                attempt_view = {"attempt_id": _expect_id(attempt_id, "attempt id")}
                for field in ("state", "started_at", "ended_at"):
                    if isinstance(attempt.get(field), str):
                        attempt_view[field] = attempt[field]
                item["attempts"].append(attempt_view)
        elif isinstance(row.get("attempt_id"), str):
            item["attempt_id"] = _expect_id(row["attempt_id"], "assignment attempt id")
        projected.append(item)
    return projected


def _project_contracts(snapshot: Mapping[str, Any]) -> list[dict[str, Any]]:
    factory = snapshot["state"]["factory"]
    found: dict[tuple[str | None, str], dict[str, Any]] = {}
    for collection_name in ("agent_bindings", "agents"):
        collection = factory.get(collection_name)
        if not isinstance(collection, list):
            continue
        for raw in collection:
            row = _record_data(raw, "public contract binding")
            candidates = [row]
            if isinstance(row.get("service_bindings"), list):
                candidates.extend(item for item in row["service_bindings"] if isinstance(item, Mapping))
            for candidate in candidates:
                digest = candidate.get("contract_digest")
                if not isinstance(digest, str):
                    continue
                digest = _expect_digest(digest, "public contract digest")
                identity = candidate.get("identity") or candidate.get("service_identity")
                capability = candidate.get("capability")
                projected = {"contract_digest": digest}
                if identity is not None:
                    projected["identity"] = _expect_id(identity, "contract service identity")
                if capability is not None:
                    projected["capability"] = _expect_id(capability, "contract capability")
                found[(projected.get("identity"), digest)] = projected
    return [found[key] for key in sorted(found, key=lambda item: (item[0] or "", item[1]))]


def _project_attempts(assignments: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    attempts: list[dict[str, Any]] = []
    for assignment in assignments:
        direct = assignment.get("attempt_id")
        if isinstance(direct, str):
            item = {"assignment_id": assignment["assignment_id"], "attempt_id": direct}
            if isinstance(assignment.get("state"), str):
                item["state"] = assignment["state"]
            attempts.append(item)
        for nested in assignment.get("attempts", []):
            item = {"assignment_id": assignment["assignment_id"],
                    "attempt_id": nested["attempt_id"]}
            if isinstance(nested.get("state"), str):
                item["state"] = nested["state"]
            attempts.append(item)
    return attempts


def _artifact_rows(run: Mapping[str, Any], run_id: str, artifact_manifest: Any,
                   export_root: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    manifest = _expect_mapping(artifact_manifest, "artifact manifest")
    if set(manifest) != {"schema_version", "artifacts"} or manifest.get("schema_version") != 1:
        raise ValueError("artifact manifest schema is unsupported")
    declared = manifest.get("artifacts")
    if not isinstance(declared, list) or len(declared) > 256:
        raise ValueError("artifact manifest must contain a bounded list")

    public_artifacts: set[tuple[str, str, str]] = set()
    for raw in run["artifacts"]:
        row = _record_data(raw, "public artifact record")
        revision = row.get("artifact_revision") or row.get("revision")
        digest = row.get("artifact_sha256") or row.get("sha256")
        row_run = row.get("run_id", run_id)
        if isinstance(revision, str) and isinstance(digest, str):
            public_artifacts.add((_expect_id(row_run, "artifact run id"),
                                  _expect_id(revision, "artifact revision"),
                                  _expect_digest(digest, "public artifact digest")))
    accepted_quality: set[tuple[str, str, str]] = set()
    quality_rows = []
    for raw in run["quality"]:
        row = _record_data(raw, "public Quality record")
        revision = row.get("artifact_revision") or row.get("revision")
        digest = row.get("artifact_sha256") or row.get("sha256")
        row_run = _expect_id(row.get("run_id", run_id), "Quality run id")
        accepted = row.get("accepted") is True or row.get("state") == "accepted"
        if isinstance(revision, str) and isinstance(digest, str):
            pair = (row_run, _expect_id(revision, "Quality artifact revision"),
                    _expect_digest(digest, "Quality artifact digest"))
            quality_rows.append({"run_id": pair[0], "revision": pair[1], "sha256": pair[2],
                                 "accepted": accepted,
                                 "task_id": row.get("task_id") if isinstance(row.get("task_id"), str) else None})
            if accepted:
                accepted_quality.add(pair)

    artifacts = []
    manifest_paths: set[str] = set()
    manifest_refs: set[tuple[str, str, str]] = set()
    safe_root = export_root.resolve(strict=True)
    for raw in declared:
        row = _expect_mapping(raw, "artifact manifest row")
        if set(row) != {"run_id", "revision", "sha256", "file"}:
            raise ValueError("artifact manifest row has unsupported fields")
        row_run = _expect_id(row.get("run_id"), "artifact manifest run id")
        revision = _expect_id(row.get("revision"), "artifact manifest revision")
        expected = _expect_digest(row.get("sha256"), "artifact manifest digest")
        file = row.get("file")
        if not isinstance(file, str) or not file.startswith("artifacts/"):
            raise ValueError("artifact path must be relative to the artifacts directory")
        relative = Path(file)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("artifact path must stay inside the export directory")
        ref = (row_run, revision, expected)
        if file in manifest_paths or ref in manifest_refs:
            raise ValueError("artifact manifest references must be unique")
        manifest_paths.add(file)
        manifest_refs.add(ref)
        path = export_root / relative
        try:
            resolved = path.resolve(strict=True)
            if not resolved.is_relative_to(safe_root) or not resolved.is_file():
                raise ValueError("artifact bytes are not a regular file inside the export directory")
            if resolved.stat().st_size > MAX_PUBLIC_ARTIFACT_BYTES:
                raise ValueError("artifact bytes exceed the size limit")
            payload = resolved.read_bytes()
        except OSError as exc:
            raise ValueError("declared artifact bytes are unavailable") from exc
        if row_run != run_id:
            raise ValueError("artifact belongs to a different run")
        observed = hashlib.sha256(payload).hexdigest()
        artifacts.append({
            "run_id": row_run, "revision": revision,
            "manifest_sha256": expected, "bytes_sha256": observed,
            "byte_length": len(payload), "bytes_match_manifest": observed == expected,
            "public_observation_match": (row_run, revision, expected) in public_artifacts,
            "accepted_quality_match": (row_run, revision, expected) in accepted_quality,
        })
    return artifacts, quality_rows


def assess_collector_coverage(inventory: Mapping[str, Any], summary: Mapping[str, Any]) -> dict[str, Any]:
    """Reassess a sanitized collector summary against its explicit scenario inventory."""
    route_ids = inventory.get("route_ids")
    exports = inventory.get("declared_exports")
    if not isinstance(route_ids, list) or not route_ids or not isinstance(exports, list):
        raise ValueError("collector scenario inventory must declare route ids and exports")
    if any(not isinstance(route, str) or not PUBLIC_ID.fullmatch(route) for route in route_ids):
        raise ValueError("collector inventory has an invalid route id")
    if len(set(route_ids)) != len(route_ids):
        raise ValueError("collector route ids must be unique")
    expected_exports: set[str] = set()
    for row in exports:
        item = _expect_mapping(row, "declared collector export")
        if set(item) != {"route_id", "path"}:
            raise ValueError("declared collector export has unsupported fields")
        route, path = item.get("route_id"), item.get("path")
        if route not in route_ids or not isinstance(path, str) or not path or path.startswith("/") or ".." in Path(path).parts:
            raise ValueError("declared collector export is outside its scenario inventory")
        key = f"{route}/{path}"
        if key in expected_exports:
            raise ValueError("declared collector exports must be unique")
        expected_exports.add(key)

    observed_routes = summary.get("observed_route_ids")
    present_exports = summary.get("present_exports")
    scan_paths = summary.get("scan_paths")
    if not all(isinstance(value, list) for value in (observed_routes, present_exports, scan_paths)):
        raise ValueError("collector summary must enumerate observed routes, exports, and scan paths")
    if any(not isinstance(route, str) or not PUBLIC_ID.fullmatch(route) for route in observed_routes):
        raise ValueError("collector summary has an invalid observed route id")
    if any(not isinstance(path, str) for path in (*present_exports, *scan_paths)):
        raise ValueError("collector export and scan paths must be strings")
    for path in (*present_exports, *scan_paths):
        relative = Path(path)
        if relative.is_absolute() or ".." in relative.parts or not re.fullmatch(r"[A-Za-z0-9._/-]+", path):
            raise ValueError("collector export and scan paths must be safe relative names")
    present = set(present_exports)
    scanned = set(scan_paths)
    if len(present) != len(present_exports) or len(scanned) != len(scan_paths):
        raise ValueError("collector export and scan paths must be unique")
    unknown_exports = sorted(present - expected_exports)
    missing_exports = sorted(expected_exports - present)
    missing_routes = sorted(set(route_ids) - set(observed_routes))
    leak_hits = summary.get("leak_hits")
    if type(leak_hits) is not int or leak_hits < 0:
        raise ValueError("collector leak hit count must be a non-negative integer")
    redaction_pass = summary.get("redaction_pass") is True
    positive_control = summary.get("positive_control_detected") is True
    complete_scan = not missing_exports and not unknown_exports and scanned == present
    g4_pass = complete_scan and leak_hits == 0 and redaction_pass and positive_control
    leak_conclusion = (
        "no findings in the complete declared-export scan"
        if g4_pass else
        "inconclusive; declared exports or scan controls are incomplete"
    )
    reported_status = summary.get("reported_status")
    if reported_status not in (None, "pass", "fail", "partial"):
        raise ValueError("reported collector status must be pass, fail, or partial")
    return {
        "reported_status_preserved": reported_status,
        "required_route_ids": list(route_ids),
        "observed_route_ids": sorted(set(observed_routes)),
        "missing_route_ids": missing_routes,
        "route_scope_complete": not missing_routes and not (set(observed_routes) - set(route_ids)),
        "declared_export_count": len(expected_exports),
        "present_export_count": len(present),
        "missing_exports": missing_exports,
        "unexpected_exports": unknown_exports,
        "scan_path_count": len(scanned),
        "redaction_pass": redaction_pass,
        "positive_control_detected": positive_control,
        "leak_hits": leak_hits,
        "g4_status": "pass" if g4_pass else "fail",
        "leak_conclusion": leak_conclusion,
        "overall_status": "pass" if not missing_routes and g4_pass else "fail",
    }


def attest_public_exports(export_dir: Path, *, run_id: str | None = None) -> dict[str, Any]:
    """Attest a bounded set of public API exports without reading runtime state or payloads."""
    export_dir = Path(export_dir).resolve(strict=True)
    missing = [name for name in PUBLIC_EXPORT_FILES.values()
               if not (export_dir / name).is_file()]
    if missing:
        raise ValueError("required public export files are unavailable: " + ", ".join(missing))
    snapshot, snapshot_bytes = _read_bounded_json(export_dir, PUBLIC_EXPORT_FILES["snapshot"])
    bundle, bundle_bytes = _read_bounded_json(export_dir, PUBLIC_EXPORT_FILES["bundle"])
    usage, usage_bytes = _read_bounded_json(export_dir, PUBLIC_EXPORT_FILES["usage"])
    task_bindings, task_bytes = _read_bounded_json(export_dir, PUBLIC_EXPORT_FILES["task_bindings"])
    artifact_manifest, artifact_manifest_bytes = _read_bounded_json(export_dir, PUBLIC_EXPORT_FILES["artifacts"])

    snapshot = _validate_snapshot_envelope(snapshot)
    bundle = _expect_mapping(bundle, "Dashboard bundle")
    if set(bundle) != {"schema_version", "source", "snapshot", "frames", "provenance"} or bundle.get("schema_version") != 1:
        raise ValueError("Dashboard bundle envelope is unsupported")
    if bundle.get("source") not in {"recorded", "demo"} or bundle.get("snapshot") != snapshot:
        raise ValueError("Dashboard bundle must match the exported Observation snapshot")
    frames = bundle.get("frames")
    if not isinstance(frames, list) or len(frames) > MAX_PUBLIC_ROWS:
        raise ValueError("Dashboard bundle frames must be bounded")
    for frame in frames:
        if not isinstance(frame, Mapping) or frame.get("op") not in {
            "event", "checkpoint", "resumed", "resync_required", "error", "snapshot",
        }:
            raise ValueError("Dashboard bundle contains an unsupported public frame")
        if frame.get("op") == "event" and not isinstance(frame.get("event"), Mapping):
            raise ValueError("Dashboard event frame has no event envelope")
    provenance = _expect_mapping(bundle.get("provenance"), "Dashboard bundle provenance")
    run = _select_run(snapshot, run_id)
    selected_run_id = run["id"]
    bindings = _project_task_bindings(task_bindings, run, selected_run_id, bundle,
                                      snapshot["state"]["runs"])
    task_ids = {bindings["parent"]["task_id"], *(child["task_id"] for child in bindings["children"])}
    usage_projection = _project_usage(usage, selected_run_id, task_ids, set(bindings["run_ids"]))
    assignments = _project_assignments(run, selected_run_id)
    attempts = _project_attempts(assignments)
    contracts = _project_contracts(snapshot)
    artifacts, quality_rows = _artifact_rows(run, selected_run_id, artifact_manifest, export_dir)

    accepted_quality = [row for row in quality_rows if row["accepted"]]
    quality_matches = [
        {"run_id": row["run_id"], "revision": row["revision"], "sha256": row["sha256"],
         "task_id": row["task_id"],
         "artifact_bytes_verified": any(
             artifact["run_id"] == row["run_id"] and artifact["revision"] == row["revision"] and
             artifact["bytes_match_manifest"] and artifact["public_observation_match"] and
             artifact["manifest_sha256"] == row["sha256"]
             for artifact in artifacts),
         "assignment_ids": sorted({assignment["assignment_id"] for assignment in assignments
                                    if row["task_id"] and assignment.get("task_id") == row["task_id"]})}
        for row in accepted_quality
    ]
    pins = _project_pins(run)
    model_pairs = usage_projection["model_pairs"]
    model_status = ("observed" if usage_projection["model_attribution_complete"] and
                    len(model_pairs) == 1 else "partial" if model_pairs else "not_observed")
    model = {"status": model_status,
             "id": model_pairs[0]["id"] if len(model_pairs) == 1 else None,
             "reasoning_effort": model_pairs[0]["reasoning_effort"] if len(model_pairs) == 1 else None}

    def execution_fixture_label(value: Any) -> bool:
        if not isinstance(value, str) or not re.search(r"(?i)(fixture|demo|mock|synthetic)", value):
            return False
        # Receiver/release fixture labels qualify delivery only, not model execution.
        if re.search(r"(?i)(receiver|delivery|release|artifact)", value):
            return False
        return True

    fixture_evidence = (bundle.get("source") == "demo" or any(
        execution_fixture_label(value)
        for value in (provenance.get("fixture_label"), provenance.get("release_label"),
                      snapshot["state"]["factory"].get("fixture_label"),
                      run.get("fixture_label"), run.get("model_label"))))
    delivery_evidence = "fixture" if any(
        isinstance(value, str) and re.search(r"(?i)(fixture|demo|mock|synthetic)", value) and
        re.search(r"(?i)(receiver|delivery|release|artifact)", value)
        for value in (provenance.get("fixture_label"), provenance.get("release_label"))) else "not_observed"
    provenance_label = provenance.get("label")
    if fixture_evidence:
        evidence_label, execution_mode = "observed-synthetic", "fixture" if bundle.get("source") != "demo" else "demo"
    elif provenance_label == "observed-real" or model_status == "observed":
        evidence_label, execution_mode = "observed-real", "live"
    else:
        evidence_label, execution_mode = "unqualified", "recorded"
    evidence_candidate = {
        "evidence_label": evidence_label,
        "execution_mode": execution_mode,
        "delivery_evidence": delivery_evidence,
        "interface_level": True,
        "model": model,
        "graph": {"definition_digest": pins["pins"].get("definition_digest"),
                  "package_digest": pins["pins"].get("package_digest"),
                  "manifest_digest": pins["pins"].get("manifest_digest"),
                  "build_id": pins["pins"].get("interpreter_build")},
        "evidence_refs": [{"id": "public-snapshot", "sha256": hashlib.sha256(snapshot_bytes).hexdigest()},
                          {"id": "public-bundle", "sha256": hashlib.sha256(bundle_bytes).hexdigest()},
                          {"id": "public-usage", "sha256": hashlib.sha256(usage_bytes).hexdigest()}],
    }
    evidence_class = classify_evidence(evidence_candidate)
    if not fixture_evidence and model.get("status") == "observed" and (
            model.get("id") != "gpt-6-luna" or model.get("reasoning_effort") != "xhigh"):
        evidence_class = "historical-partial"

    checks = {
        "snapshot_schema_v1": True,
        "bundle_matches_snapshot": True,
        "snapshot_fresh": snapshot["freshness"].get("status") == "fresh",
        "graph_pins_complete": pins["complete"],
        "run_model_pin_matches_usage": (pins["pins"].get("model_id") in (None, model.get("id"))),
        "selected_run_task_binding_matches": bindings["selected_run_binding_matches"],
        "task_run_bindings_verified": bindings["relationship_status"] == "verified",
        "accepted_quality_exact_artifact": bool(quality_matches) and all(
            row["artifact_bytes_verified"] for row in quality_matches),
        "artifact_bytes_match_public_digest": bool(artifacts) and all(
            row["bytes_match_manifest"] and row["public_observation_match"] for row in artifacts),
        "usage_export_present": usage_projection["status"] == "observed",
        "current_luna_xhigh_observed": (model.get("id") == "gpt-6-luna" and
                                         model.get("reasoning_effort") == "xhigh"),
        "non_fixture_source": not fixture_evidence,
    }
    complete_for_review = all(checks.values()) and evidence_class == "candidate-for-review"
    optional_collector = None
    collector_path = export_dir / "collector-summary.json"
    if collector_path.exists():
        collector_summary, _collector_bytes = _read_bounded_json(export_dir, "collector-summary.json", limit=1024 * 1024)
        summary_obj = _expect_mapping(collector_summary, "collector summary")
        inventory = _expect_mapping(summary_obj.get("scenario_inventory"), "scenario inventory")
        optional_collector = assess_collector_coverage(inventory, summary_obj)

    review_status = (
        "observed-synthetic-only" if fixture_evidence else
        "historical-partial" if evidence_class == "historical-partial" else
        "candidate-for-review" if complete_for_review else
        "incomplete-public-export" if evidence_class == "candidate-for-review" else
        evidence_class
    )

    return {
        "schema_version": 1,
        "report_type": "dashboard-public-export-attestation",
        "qualification_status": "unqualified",
        "status": review_status,
        "evidence_label": evidence_label,
        "execution_mode": execution_mode,
        "delivery_evidence": delivery_evidence,
        "claim_ids": [],
        "run": {"run_id": selected_run_id,
                 "task_id": _expect_id(run["task"]["id"], "selected Task id"),
                 "context_id": _expect_id(run["task"]["context_id"], "selected context id"),
                 "parent_run_id": bindings["parent"]["run_id"],
                 "parent_task_id": bindings["parent"]["task_id"],
                 "parent_context_id": bindings["parent"]["context_id"]},
        "graph": pins,
        "task_bindings": bindings,
        "assignments": assignments,
        "attempts": attempts,
        "bindings": {"manifest_digest": pins["pins"].get("manifest_digest"),
                     "package_digest": pins["pins"].get("package_digest"),
                     "definition_digest": pins["pins"].get("definition_digest"),
                     "build_id": pins["pins"].get("interpreter_build")},
        "contracts": contracts,
        "quality_correlations": quality_matches,
        "usage": usage_projection,
        "artifacts": artifacts,
        "artifact_refs": artifacts,
        "evidence_refs": [
            {"id": "public-snapshot", "sha256": hashlib.sha256(snapshot_bytes).hexdigest()},
            {"id": "public-bundle", "sha256": hashlib.sha256(bundle_bytes).hexdigest()},
            {"id": "public-usage", "sha256": hashlib.sha256(usage_bytes).hexdigest()},
            {"id": "public-task-bindings", "sha256": hashlib.sha256(task_bytes).hexdigest()},
            {"id": "public-artifact-manifest", "sha256": hashlib.sha256(artifact_manifest_bytes).hexdigest()},
        ],
        "model": model,
        "checks": checks,
        "collector": optional_collector,
        "source_envelopes": {
            "snapshot_sha256": hashlib.sha256(snapshot_bytes).hexdigest(),
            "bundle_sha256": hashlib.sha256(bundle_bytes).hexdigest(),
            "usage_sha256": hashlib.sha256(usage_bytes).hexdigest(),
            "task_bindings_sha256": hashlib.sha256(task_bytes).hexdigest(),
            "artifact_manifest_sha256": hashlib.sha256(artifact_manifest_bytes).hexdigest(),
        },
        "side_effects": [],
    }


def render_public_attestation_markdown(attestation: Mapping[str, Any]) -> bytes:
    """Render a value-free Markdown review note from an attestation projection."""
    run = attestation["run"]
    pins = attestation["graph"]["pins"]
    lines = [
        "# Dashboard public-export attestation",
        "",
        f"- Review status: `{attestation['status']}`",
        "- Qualification status: `unqualified`",
        f"- Evidence label: `{attestation['evidence_label']}`",
        f"- Execution mode: `{attestation['execution_mode']}`",
        f"- Delivery evidence: `{attestation['delivery_evidence']}`",
        f"- Run ID: `{run['run_id']}`",
        f"- Parent Task/context: `{run['parent_task_id']}` / `{run['parent_context_id']}`",
        f"- Graph pin completeness: `{attestation['graph']['complete']}`",
        f"- Definition digest: `{pins.get('definition_digest', 'unknown')}`",
        f"- Bundle Task child-link status: `{attestation['task_bindings']['relationship_status']}`",
        f"- Accepted Quality artifact correlations: `{len(attestation['quality_correlations'])}`",
        f"- Safe usage measurements: `{attestation['usage']['measurement_count']}`",
        f"- Verified artifact byte records: `{sum(1 for row in attestation['artifacts'] if row['bytes_match_manifest'])}`",
        "",
        "This note contains no artifact contents, runtime history, prompts, credentials, or payment data.",
    ]
    return ("\n".join(lines) + "\n").encode("utf-8")


def _write_new_json(path: Path, payload: Mapping[str, Any]) -> None:
    """Write evidence without overwriting an existing file."""
    encoded = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n"
    with Path(path).open("x", encoding="utf-8") as stream:
        stream.write(encoded)


def _write_new_bytes(path: Path, payload: bytes) -> None:
    with Path(path).open("xb") as stream:
        stream.write(payload)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="write a new safe preflight JSON file")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--validate-evidence", type=Path,
                       help="validate a safe evidence candidate; performs no network calls")
    modes.add_argument("--attest-public-exports", type=Path, metavar="DIRECTORY",
                       help="attest bounded snapshot/bundle/usage/Task/artifact public exports")
    modes.add_argument("--assess-collector-summary", type=Path,
                       help="assess an already-sanitized collector summary against its inventory")
    parser.add_argument("--run-id", help="select exactly one run from a public snapshot export")
    parser.add_argument("--attestation-output", type=Path,
                        help="write a new safe JSON attestation (only with --attest-public-exports)")
    parser.add_argument("--markdown-output", type=Path,
                        help="write a new safe Markdown attestation (only with --attest-public-exports)")
    args = parser.parse_args(argv)

    if args.attestation_output or args.markdown_output:
        if not args.attest_public_exports:
            parser.error("attestation output paths require --attest-public-exports")
        if args.attestation_output and args.markdown_output and args.attestation_output.resolve() == args.markdown_output.resolve():
            parser.error("JSON and Markdown attestation paths must be different")

    if args.validate_evidence:
        try:
            record = json.loads(args.validate_evidence.read_text())
            safe = export_safe_evidence(record)
            result = {
                "schema_version": 1,
                "report_type": "dashboard-qualification-evidence-review",
                "status": classify_evidence(safe),
                "qualification_status": "unqualified",
                "claim_ids": safe.get("claim_ids", []),
                "evidence_refs": safe.get("evidence_refs", []),
                "side_effects": [],
            }
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            result = {"status": "rejected", "qualification_status": "unqualified",
                      "reason": str(exc), "side_effects": []}
            print(json.dumps(result, indent=2, sort_keys=True))
            return 2
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["status"] == "candidate-for-review" else 2

    if args.assess_collector_summary:
        try:
            source = Path(args.assess_collector_summary)
            if source.stat().st_size > 1024 * 1024:
                raise ValueError("collector summary exceeds the size limit")
            data = json.loads(source.read_bytes(), parse_constant=lambda _value: (_ for _ in ()).throw(
                ValueError("non-finite number in collector summary")))
            _scan_public_export(data)
            summary = _expect_mapping(data, "collector summary")
            inventory = _expect_mapping(summary.get("scenario_inventory"), "scenario inventory")
            result = {"report_type": "dashboard-collector-scope-review",
                      "assessment": assess_collector_coverage(inventory, summary),
                      "qualification_status": "unqualified", "side_effects": []}
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            result = {"report_type": "dashboard-collector-scope-review",
                      "status": "rejected", "qualification_status": "unqualified",
                      "reason": str(exc), "side_effects": []}
            print(json.dumps(result, indent=2, sort_keys=True))
            return 2
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["assessment"]["overall_status"] == "pass" else 2

    if args.attest_public_exports:
        try:
            result = attest_public_exports(args.attest_public_exports, run_id=args.run_id)
            envelope = (json.dumps(result, indent=2, sort_keys=True, ensure_ascii=False,
                                   allow_nan=False) + "\n").encode("utf-8")
            markdown = render_public_attestation_markdown(result)
            if args.attestation_output:
                _write_new_bytes(args.attestation_output, envelope)
            if args.markdown_output:
                _write_new_bytes(args.markdown_output, markdown)
            response = {
                "report_type": "dashboard-public-export-attestation-review",
                "attestation": result,
                "output_digests": {
                    "attestation_envelope_sha256": hashlib.sha256(envelope).hexdigest(),
                    "attestation_markdown_sha256": hashlib.sha256(markdown).hexdigest(),
                },
            }
        except (OSError, json.JSONDecodeError, ValueError, FileExistsError) as exc:
            response = {"report_type": "dashboard-public-export-attestation-review",
                        "status": "rejected", "qualification_status": "unqualified",
                        "reason": str(exc), "side_effects": []}
            print(json.dumps(response, indent=2, sort_keys=True))
            return 2
        print(json.dumps(response, indent=2, sort_keys=True))
        return 0

    if args.run_id:
        parser.error("--run-id requires --attest-public-exports")
    if args.attestation_output or args.markdown_output:
        parser.error("attestation output paths require --attest-public-exports")

    result = build_preflight()
    if args.output:
        try:
            _write_new_json(args.output, result)
        except (OSError, FileExistsError) as exc:
            print(f"could not write new preflight evidence: {exc}", file=sys.stderr)
            return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] == "ready" else 2


if __name__ == "__main__":
    raise SystemExit(main())
