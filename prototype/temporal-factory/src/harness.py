"""Customized Strands harness instance: ordinary agent or factory mode.

In factory mode the Director agent and Factory Module live inside this instance.
Callers see only the instance's normal A2A identity, Agent Card and capability
contract; there is no separate factory endpoint, and callers never name a graph,
package or version. New runs use the instance's active publication; open runs
keep the closure pinned at their start. The shared local runner (Temporal,
PostgreSQL, interpreter workers) is started lazily on first factory work, or at
startup only when this instance has unfinished runs to recover.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import ipaddress
import json
import platform
import re
import secrets
import sqlite3
import sys
import subprocess
import threading
import time
import os
import urllib.error
import urllib.request
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from importlib import metadata
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import uvicorn
from fastapi import Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response
from a2a.types import (AgentCapabilities, AgentCard, AgentExtension, AgentSkill, Artifact,
                       Task, TaskStatus)
from strands import Agent
from temporalio.client import Client
from temporalio.common import PinnedVersioningOverride, WorkerDeploymentVersion
from temporalio.service import RPCError
from fastapi.staticfiles import StaticFiles

SRC = Path(__file__).resolve().parent
sys.path.insert(0, str(SRC))
import harness_server  # noqa: E402
import a2a_v1  # noqa: E402
from a2a_v1_server import (LegacyRequestHandler, ProjectionTaskStore,  # noqa: E402
                           agent_message, bearer_security, build_app, cookie_security,
                           data_part, interfaces, task_state, text_part)
from harness_server import (HarnessExecutor, HarnessPlugin, Rejected,  # noqa: E402
                            ToolCallingModelFixture, canonical)
from binding import (DEPLOYMENT, NAMESPACE, QUEUE, PublicationStore,  # noqa: E402
                     make_manifest, verify_closure)
from definition import authorize_run_inputs, digest, validate  # noqa: E402
from definition import publish as store_package  # noqa: E402
from factory import FactoryRun  # noqa: E402
from failure_projection import failure_incident, project  # noqa: E402
from runner import Runner  # noqa: E402
from observation import FactoryObservation  # noqa: E402
from observation_transport import install_observation_transport  # noqa: E402
from artifact_delivery import accepted_markdown  # noqa: E402
from observation_source import RuntimeObservationSource  # noqa: E402
from operations_observation import project_operations_incidents  # noqa: E402
from runtime_operations import install_runtime_operations  # noqa: E402
from local_delivery_routes import install_local_delivery_routes  # noqa: E402
from commercial import CommercialLedger  # noqa: E402
from model_broker import (BROKER_PROGRAM, DEFAULT_HOME, DEFAULT_MODEL_ID,
                          DEFAULT_REASONING_EFFORT, ModelBroker)  # noqa: E402
from model_usage import AGENT_USAGE_DATABASE, ModelUsageJournal  # noqa: E402
from agent_binding import digest as agent_contract_digest  # noqa: E402
from agent_binding import resolve as resolve_agent_binding  # noqa: E402
from agent_binding import card_identity as agent_card_identity  # noqa: E402
from agent_binding import resolve_card as resolve_card_binding  # noqa: E402
from admission import AdmissionQueue  # noqa: E402
from supplier_protocol import (  # noqa: E402
    SupplierEnvelopeError,
    nested_factory_fingerprint,
    parse_nested_factory_envelope,
    project_supplier_echo,
    supplier_assignment_echo_declaration,
)

TOKEN = "Bearer fixture-token"
TOKEN_ACTOR = "fixture-operator"
OBSERVER_TOKEN = "Bearer fixture-observer"
OBSERVER_ACTOR = "fixture-observer"
QA_ACTOR = TOKEN_ACTOR
QA_SESSION_COOKIE = "exo_loopback_qa_session"
CURRENT_ACTOR: ContextVar[str | None] = ContextVar("harness_authenticated_actor", default=None)
TERMINAL = {"completed", "failed", "canceled", "rejected"}
SUBMISSION_READINESS_REASON_CODES = frozenset({
    "director_profile_unapproved", "default_broker_path_overridden",
    "subscription_status_unavailable", "pinned_writable_model_owners_unavailable",
    "pinned_worker_pollers_unavailable", "factory_busy", "factory_uncertain",
    "unfinished_runs", "current_state_unavailable",
})
PROJECT_ROOT = SRC.parents[2]
FLOOR_PAGE = PROJECT_ROOT / "docs" / "design" / "exomachina-floor.html"
DASHBOARD_ASSETS = SRC.parent / "dashboard"


def _dashboard_js_content_digest(directory: Path) -> str:
    digest = hashlib.sha256()
    assets = sorted((*directory.rglob("*.js"), *directory.rglob("*.mjs")))
    if not assets:
        raise RuntimeError("dashboard JavaScript assets are unavailable")
    for asset in assets:
        digest.update(asset.relative_to(directory).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(asset.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


USAGE_CATEGORIES = ("input_tokens", "output_tokens", "cache_read_tokens",
                    "cache_write_tokens", "total_tokens")
USAGE_CALL_SCOPES = {"authoring_overhead", "director_call", "assignment_call"}
# The nested-supplier mode is this factory's own caller-facing A2A server (a
# factory service, not an agent service); it keeps its own action extension.
A2A_ACTION_EXTENSION_URI = "urn:exomachina:a2a-action-contract:v1"
A2A_ACTION_CONTRACT = "action-idempotent-async@1"
# provider_reported: this factory's own model calls; agent_reported: an agent
# service's A2A budget-extension report recorded by the factory's on-complete hook.
USAGE_EVIDENCE = {"provider_reported", "agent_reported", "unknown"}
USAGE_FILTERS = ("run_id", "task_id", "assignment_id", "attempt_id",
                 "model_call_id", "call_scope")
USAGE_VIEW_FIELDS = ("measurement_id", "model_call_id", "recorded_at", "call_scope",
                     "provider", "model_id", "reasoning_effort", "unit",
                     "measurement_source", "completeness", "evidence_status", "usage",
                     "service_identity", "task_id", "message_id", "run_id",
                     "definition_digest", "assignment_id", "attempt_id")

DIRECTOR_DECISION_REJECTION_CODES = frozenset({
    "decision-child-missing", "decision-command-id-conflict",
    "decision-wait-missing", "decision-action-not-permitted",
    "decision-deadline-unavailable-or-expired", "decision-actor-mismatch",
    "decision-artifact-stale", "decision-owner-epoch-newer",
    "decision-wait-changed-during-owner-claim",
    "decision-receipt-unexpected", "decision-original-task-binding-missing",
    "decision-task-context-mismatch", "decision-actor-unauthorized",
    "decision-action-id-conflict", "decision-command-receipt-unbound",
    "decision-admission-not-admitted", "decision-admission-missing",
    "decision-rejected-unclassified",
})


class DirectorDecisionRejected(Rejected):
    """A Director rejection with a finite, non-sensitive public reason code."""

    def __init__(self, safe_code: str, message: str):
        if safe_code not in DIRECTOR_DECISION_REJECTION_CODES:
            raise ValueError("unknown Director decision rejection code")
        self.safe_code = safe_code
        super().__init__(message)


def _register_harness_module_identity() -> None:
    """Make imports of ``harness`` resolve to this module, including CLI startup."""
    current = sys.modules[__name__]
    existing = sys.modules.get("harness")
    if existing is not None and existing is not current:
        raise RuntimeError("duplicate harness module identity")
    sys.modules["harness"] = current


if __name__ == "__main__":
    # Observation dispatch imports ``harness`` while this CLI module is still
    # running. Alias before app construction so auth ContextVars have one owner.
    _register_harness_module_identity()


def sync(coro):
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(lambda: asyncio.run(coro)).result()


def load_config(instance_dir: Path) -> dict:
    config = json.loads((instance_dir / "instance.json").read_text())
    if config.get("mode") not in {"agent", "factory"}:
        raise ValueError("instance mode must be agent or factory")
    if "admission_capacity" in config:
        capacity = config["admission_capacity"]
        if type(capacity) is not int or capacity < 0:
            raise ValueError("admission_capacity must be an explicit non-negative integer")
        if config["mode"] != "factory":
            raise ValueError("admission_capacity is available only in factory mode")
    if "operations_max_list_limit" in config:
        limit = config["operations_max_list_limit"]
        if type(limit) is not int or not 1 <= limit <= 256:
            raise ValueError("operations_max_list_limit must be an explicit integer from 1 to 256")
        if config["mode"] != "factory":
            raise ValueError("Operations routes are available only in factory mode")
    if "nested_supplier_enabled" in config:
        if type(config["nested_supplier_enabled"]) is not bool:
            raise ValueError("nested_supplier_enabled must be an explicit boolean")
        if config["nested_supplier_enabled"] and config["mode"] != "factory":
            raise ValueError("nested suppliers are available only in factory mode")
    return config


def configure_admission_capacity(instance_dir: Path, capacity: int) -> dict:
    """Persist an explicit CLI capacity before the harness claims the instance."""
    if type(capacity) is not int or capacity < 0:
        raise ValueError("admission_capacity must be an explicit non-negative integer")
    import fcntl

    instance_dir = Path(instance_dir).resolve()
    instance_dir.mkdir(parents=True, exist_ok=True)
    with (instance_dir / "provision.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        config = load_config(instance_dir)
        if config.get("mode") != "factory":
            raise ValueError("admission_capacity is available only in factory mode")
        previous = config.get("admission_capacity")
        if previous is not None and previous != capacity:
            raise ValueError("admission_capacity differs from the existing instance configuration")
        if "admission_capacity" not in config:
            config["admission_capacity"] = capacity
            temporary = instance_dir / ".instance.json.admission.tmp"
            with temporary.open("w") as stream:
                stream.write(json.dumps(config, indent=2, sort_keys=True) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, instance_dir / "instance.json")
        return config


def configure_operations_max_list_limit(instance_dir: Path, limit: int) -> dict:
    """Persist the deployment's explicit bound before the harness claims it."""
    if type(limit) is not int or not 1 <= limit <= 256:
        raise ValueError("operations_max_list_limit must be an explicit integer from 1 to 256")
    import fcntl

    instance_dir = Path(instance_dir).resolve()
    instance_dir.mkdir(parents=True, exist_ok=True)
    with (instance_dir / "provision.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        config = load_config(instance_dir)
        if config.get("mode") != "factory":
            raise ValueError("Operations routes are available only in factory mode")
        previous = config.get("operations_max_list_limit")
        if previous is not None and previous != limit:
            raise ValueError("Operations list limit differs from the existing instance configuration")
        if "operations_max_list_limit" not in config:
            config["operations_max_list_limit"] = limit
            temporary = instance_dir / ".instance.json.operations.tmp"
            with temporary.open("w") as stream:
                stream.write(json.dumps(config, indent=2, sort_keys=True) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, instance_dir / "instance.json")
        return config


def build_workflow_input(identity: str, epoch: int, run: dict, package: dict,
                         publication: dict, wait_seconds: int) -> dict:
    """Construct the persisted start payload without accepting a secret token."""
    task_id, context_id = run.get("task_id"), run.get("context_id")
    if (not isinstance(task_id, str) or not task_id or
            not isinstance(context_id, str) or not context_id):
        raise ValueError("workflow start requires the original A2A Task/context binding")
    root = package["root"]
    return {"run": run["run_id"], "definition_digest": digest(root),
            "task_id": task_id, "context_id": context_id,
            "package_digest": run["package_digest"], "document": root,
            "package": package, "closure": publication["closure"],
            "director": {"identity": identity, "epoch": epoch},
            "run_inputs": json.loads(run["run_inputs_json"]),
            "run_inputs_digest": run["run_inputs_digest"],
            "authorized_actor": run["authorized_actor"],
            "input_authority": json.loads(run["input_authority_json"]),
            "wait_seconds": wait_seconds}


def _workflow_execution_timeout_seconds(package: Mapping, wait_seconds: int) -> int:
    """Bound the root execution by every published Director and human wait.

    Definitions are validated to have no cycles except the globally bounded
    repair loop (at most two repeats). Three visits per declared wait is a
    conservative bound. Include all pinned child definitions because the
    parent execution remains open while its child runs. The existing 600s
    activity/closeout allowance is preserved; Runtime supplies no human actor
    or deadline.
    """
    if type(wait_seconds) is not int or wait_seconds <= 0:
        raise ValueError("Director wait must be a positive configured integer")
    documents = [package.get("root")]
    children = package.get("children", {})
    if isinstance(children, Mapping):
        documents.extend(children.values())
    one_pass_wait = 0
    for document in documents:
        nodes = document.get("nodes") if isinstance(document, Mapping) else None
        if not isinstance(nodes, Mapping):
            raise ValueError("published definition nodes are unavailable")
        for node in nodes.values():
            if not isinstance(node, Mapping) or node.get("type") != "director_wait":
                continue
            duration = wait_seconds
            human = node.get("human")
            if human is not None:
                timeout = human.get("timeout_seconds") if isinstance(human, Mapping) else None
                if type(timeout) is not int or timeout <= 0:
                    raise ValueError("published human wait deadline is invalid")
                duration += timeout
            one_pass_wait += duration
    return max(wait_seconds, one_pass_wait * 3) + 600


def _safe_measurement(value: object, *, service_identity: str | None = None) -> dict | None:
    """Allowlist one owner's public, non-financial token measurement."""
    if not isinstance(value, Mapping):
        return None
    required_text = ("measurement_id", "model_call_id", "recorded_at", "call_scope",
                     "provider", "model_id", "unit", "measurement_source",
                     "completeness", "evidence_status")
    if not all(isinstance(value.get(key), str) and value[key] for key in required_text):
        return None
    if value["call_scope"] not in USAGE_CALL_SCOPES or value["unit"] != "tokens":
        return None
    if value["completeness"] not in {"complete", "partial", "unknown"}:
        return None
    if value["evidence_status"] not in USAGE_EVIDENCE:
        return None
    if value["measurement_source"] not in USAGE_EVIDENCE:
        return None
    try:
        timestamp = datetime.fromisoformat(value["recorded_at"].replace("Z", "+00:00"))
    except ValueError:
        return None
    if timestamp.tzinfo is None:
        return None
    usage = value.get("usage")
    if not isinstance(usage, Mapping) or set(usage) != set(USAGE_CATEGORIES):
        return None
    safe_usage = {}
    reported = 0
    for category in USAGE_CATEGORIES:
        item = usage.get(category)
        if not isinstance(item, Mapping) or item.get("status") not in {"reported", "unavailable"}:
            return None
        amount = item.get("value")
        if item["status"] == "reported":
            if type(amount) is not int or amount < 0:
                return None
            reported += 1
        elif amount is not None:
            return None
        safe_usage[category] = {"value": amount, "status": item["status"]}
    expected_completeness = ("complete" if reported == len(USAGE_CATEGORIES) else
                             "partial" if reported else "unknown")
    if (value["completeness"] != expected_completeness or
            (value["evidence_status"] == "unknown") is bool(reported) or
            value["measurement_source"] != value["evidence_status"]):
        return None
    safe = {key: value.get(key) for key in USAGE_VIEW_FIELDS if key in value}
    safe["usage"] = safe_usage
    for key in ("service_identity", "task_id", "message_id", "run_id",
                "definition_digest", "assignment_id", "attempt_id", "reasoning_effort"):
        if key in safe and safe[key] is not None and not isinstance(safe[key], str):
            return None
    if safe.get("task_id") is None and safe["call_scope"] != "authoring_overhead":
        return None
    if service_identity is not None:
        if safe.get("service_identity") not in (None, service_identity):
            return None
        safe["service_identity"] = service_identity
    return safe


def _measurement_matches(value: Mapping, filters: Mapping[str, str | None]) -> bool:
    return all(expected is None or value.get(key) == expected
               for key, expected in filters.items())


def _merge_measurement(rows: dict[str, dict], conflicts: set[str], value: dict) -> None:
    identity = value["measurement_id"]
    if identity in conflicts:
        return
    previous = rows.get(identity)
    if previous is None:
        rows[identity] = value
    elif previous != value:
        rows.pop(identity, None)
        conflicts.add(identity)


def _journal_measurements(reader, scopes: list[dict], filters: Mapping[str, str | None],
                          *, call_scope: str):
    """Read a journal only through authoritative run bindings.

    The configured model home can be shared across factories. Deliberately never
    call list_measurements without an owned run_id, which would expose its
    unbound authoring rows to every factory using that home.
    """
    if filters.get("call_scope") is not None and filters["call_scope"] != call_scope:
        return [], {"queries_failed": 0, "rows_rejected": 0, "conflicts": 0}
    rows: dict[str, dict] = {}
    conflicts: set[str] = set()
    failures = malformed = 0
    secondary = {key: value for key, value in filters.items()
                 if value is not None and key not in {"run_id", "task_id"}}
    secondary["call_scope"] = call_scope
    for scope in scopes:
        if filters.get("run_id") is not None and scope["run_id"] != filters["run_id"]:
            continue
        # Restrict each read to the authoritative factory run. A returned
        # measurement's task_id is its owning agent's remote Task; apply the
        # caller's task filter after reading instead of substituting the
        # original parent A2A Task here.
        selectors = [{"run_id": scope["run_id"]}]
        for selector in selectors:
            query = {**selector, **secondary}
            try:
                result = reader.list_measurements(**query)
                if not isinstance(result, list):
                    failures += 1
                    continue
            except Exception:
                failures += 1
                continue
            binding_key, binding_value = next(iter(selector.items()))
            for raw in result:
                view = _safe_measurement(raw)
                if view is None:
                    malformed += 1
                    continue
                if view.get(binding_key) != binding_value:
                    malformed += 1
                    continue
                if not _measurement_matches(view, filters):
                    continue
                _merge_measurement(rows, conflicts, view)
    return list(rows.values()), {"queries_failed": failures,
        "rows_rejected": malformed, "conflicts": len(conflicts)}


def _authoring_measurements(reader, scopes: list[dict], filters: Mapping[str, str | None]):
    return _journal_measurements(reader, scopes, filters, call_scope="authoring_overhead")


def _director_measurements(reader, scopes: list[dict], filters: Mapping[str, str | None]):
    """Read run-bound Director rows and safe pre-run rows for owned Tasks only.

    Unlike the shared model-home authoring journal, this reader is the
    factory's Director-owned public facade. Its task-scoped reads are therefore
    safe only for original Task IDs already authorized by ``scopes``. Keep the
    pre-run row's null run/definition bindings as recorded; never infer them
    from a later run.
    """
    bound_rows, base_report = _journal_measurements(
        reader, scopes, filters, call_scope="director_call")
    if filters.get("call_scope") not in (None, "director_call"):
        return bound_rows, base_report

    # A task-scoped query without run_id is safe only through the concrete
    # Director facade supplied by this Runtime route. Other owners, including
    # the shared authoring journal, stay restricted to run-scoped reads.
    if not isinstance(reader, Director):
        return bound_rows, base_report

    tasks = set()
    for scope in scopes:
        if not isinstance(scope, Mapping):
            continue
        if filters.get("run_id") is not None and scope.get("run_id") != filters["run_id"]:
            continue
        task_id = scope.get("task_id")
        if (isinstance(task_id, str) and task_id and
                (filters.get("task_id") is None or task_id == filters["task_id"])):
            tasks.add(task_id)

    rows = {row["measurement_id"]: row for row in bound_rows}
    conflicts: set[str] = set()
    rejected = queries_failed = 0
    unbound_filters = {key: value for key, value in filters.items() if key != "run_id"}
    for task_id in sorted(tasks):
        try:
            result = reader.list_measurements(
                task_id=task_id, call_scope="director_call", run_id=None)
        except Exception:
            queries_failed += 1
            continue
        if not isinstance(result, list):
            queries_failed += 1
            continue
        for raw in result:
            view = _safe_measurement(raw)
            if view is None:
                rejected += 1
                continue
            # The public facade returns all task rows for this query. Bound
            # rows are already admitted by the exact run-scoped queries above.
            # Missing bindings are not interpreted as explicit null bindings.
            if "run_id" not in view or "task_id" not in view:
                rejected += 1
                continue
            if view["run_id"] is not None:
                continue
            if view["task_id"] != task_id or view["call_scope"] != "director_call":
                rejected += 1
                continue
            if not _measurement_matches(view, unbound_filters):
                continue
            _merge_measurement(rows, conflicts, view)

    report = dict(base_report)
    report["queries_failed"] += queries_failed
    report["rows_rejected"] += rejected
    report["conflicts"] += len(conflicts)
    return list(rows.values()), report


def _director_coverage_status(report: Mapping, rows: list[Mapping]) -> str:
    """Keep task-scoped pre-run rows visibly partial while preserving null pins."""
    if (report.get("queries_failed") or report.get("rows_rejected") or
            report.get("conflicts") or any(
                row.get("call_scope") == "director_call" and row.get("run_id") is None
                for row in rows)):
        return "partial"
    return "available"


def _usage_scopes(snapshot: Mapping, director: Director,
                  filters: Mapping[str, str | None]) -> list[dict]:
    state = snapshot.get("state") or {}
    runs = state.get("runs") or []
    scopes = []
    seen = set()
    for run in runs:
        if not isinstance(run, Mapping) or not isinstance(run.get("id"), str):
            continue
        run_id = run["id"]
        task = run.get("task") or {}
        task_id, context_id = task.get("id"), task.get("context_id")
        if not isinstance(task_id, str) or not isinstance(context_id, str):
            continue
        if filters.get("run_id") is not None and run_id != filters["run_id"]:
            continue
        binding = director.task_binding(task_id)
        if not binding or binding[1] != context_id:
            continue
        try:
            root = director.run_record(binding[0])
        except Exception:
            continue
        if root.get("task_id") != task_id or root.get("context_id") != context_id:
            continue
        pinned = run.get("pinned") or {}
        if (not isinstance(pinned.get("manifest_digest"), str) or
                not isinstance(pinned.get("package_digest"), str) or
                not isinstance(pinned.get("definition_digest"), str)):
            continue
        if run_id == binding[0] and (pinned["manifest_digest"] != root.get("manifest_digest") or
                                     pinned["package_digest"] != root.get("package_digest")):
            continue
        key = (run_id, task_id, context_id, pinned["manifest_digest"],
               pinned["package_digest"], pinned["definition_digest"])
        if key in seen:
            continue
        seen.add(key)
        scopes.append({"run_id": run_id, "task_id": task_id, "context_id": context_id,
                       "pinned": dict(pinned)})
    return scopes


def _pinned_usage_owners(director: Director, scope: Mapping, *,
                         resolve: bool = True) -> tuple[list[dict], int, int]:
    """Pinned A2A agent services in this run's immutable closure.

    With ``resolve`` the Agent Card is fetched and checked against its pin; that
    standard A2A discovery read is the only request made to an agent here.
    """
    try:
        pinned = scope["pinned"]
        publication = director.module.publications.get(pinned["manifest_digest"])
        if (publication.get("manifest_digest") != pinned["manifest_digest"] or
                publication.get("package_digest") != pinned["package_digest"]):
            return [], 1, 0
        package = director.module.package(publication["package_digest"])
        closure = publication.get("closure") or {}
        manifest_services = (closure.get("manifest") or {}).get("services") or {}
        contracts = closure.get("contracts") or {}
        package_bindings = package.get("bindings") or {}
        if not isinstance(manifest_services, Mapping) or not isinstance(package_bindings, Mapping):
            return [], 1, 0
        snapshot_path = Path(director.module.home) / "testbed" / "agent_snapshot.json"
        owners = []
        failures = 0
        non_usage_services = 0
        for name in sorted(set(manifest_services) & set(package_bindings)):
            manifest_service = manifest_services[name]
            binding = package_bindings[name]
            if not isinstance(manifest_service, Mapping) or not isinstance(binding, Mapping):
                failures += 1
                continue
            # binding_digest covers the package binding; contract_digest covers
            # the pinned Agent Card descriptor.
            identity = binding.get("identity")
            binding_digest = manifest_service.get("binding_digest")
            descriptor_digest = manifest_service.get("contract_digest")
            contract = contracts.get(name)
            if (set(manifest_service) != {"binding_digest", "contract_digest"} or
                    binding.get("approved") is not True or
                    not isinstance(binding.get("role"), str) or
                    not isinstance(contract, Mapping) or
                    binding.get("role") != contract.get("role") or
                    not isinstance(identity, str) or not identity or
                    not isinstance(binding_digest, str) or
                    agent_contract_digest(binding) != binding_digest or
                    not isinstance(descriptor_digest, str) or
                    contract.get("name") != name or
                    agent_contract_digest(contract) != descriptor_digest):
                failures += 1
                continue
            if "card_sha256" not in contract:
                # Pinned services without an Agent Card pin (for example the
                # local HTTP release receiver) are not A2A agent services.
                non_usage_services += 1
                continue
            if not isinstance(contract.get("card_sha256"), str):
                failures += 1
                continue
            role = binding.get("role")
            if not resolve:
                owners.append({"name": name, "identity": identity, "role": role})
                continue
            # A card-pinned agent (identity derived from its Agent Card digest,
            # for example the A2A release agent) re-verifies through its card
            # alone; other agents through their pinned card and identity.
            resolver = (resolve_card_binding
                        if identity == agent_card_identity(contract["card_sha256"])
                        else resolve_agent_binding)
            try:
                url, observed = resolver(snapshot_path, identity, dict(contract))
            except Exception:
                failures += 1
                continue
            parsed = urlsplit(url)
            if parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or not parsed.port:
                failures += 1
                continue
            owners.append({"name": name, "identity": identity, "role": role, "url": url,
                           "skills": list(observed.get("skills") or [])})
        if set(manifest_services) != set(package_bindings):
            failures += 1
        return owners, failures, non_usage_services
    except Exception:
        return [], 1, 0


_BASIC_MODEL_OWNERS = {
    "research_findings": "packet_findings@1",
    "research_risks": "packet_risks@1",
    "synthesizer": "report_synthesis@1",
    "quality": "report_quality_review@1",
}


def _redacted_subscription_status() -> str:
    """Use only the broker's established redacted status command; never expose output."""
    try:
        result = subprocess.run(
            [os.environ.get("EXO_NODE", "node"), str(BROKER_PROGRAM), "status"],
            cwd=str(BROKER_PROGRAM.parent.parent), capture_output=True, text=True,
            timeout=5, check=False)
        if result.returncode != 0:
            return "unavailable"
        status = json.loads(result.stdout)
        if not isinstance(status, dict):
            return "unavailable"
        if status.get("signed_in") is True and status.get("expired") is False:
            return "available"
        if status.get("expired") is True:
            return "expired"
        if status.get("signed_in") is False:
            return "signed_out"
        return "unavailable"
    except (OSError, subprocess.TimeoutExpired, ValueError, json.JSONDecodeError):
        return "unavailable"


def _worker_readiness_evidence(build_id: str | None, value: object) -> dict:
    """Reduce Runner's live poller proof to a safe, build-bound readiness view."""
    unavailable = {"status": "unavailable", "build_id": build_id,
                   "registered": False, "workflow_poller_count": None,
                   "activity_poller_count": None}
    if not isinstance(build_id, str) or not build_id or not isinstance(value, Mapping):
        return unavailable
    pollers = value.get("live_pollers")
    if value.get("build_id") != build_id or value.get("registered") is not True or not isinstance(pollers, Mapping):
        return unavailable
    counts = {"workflow": 0, "activity": 0}
    for name, identities in pollers.items():
        if not isinstance(name, str) or not isinstance(identities, list):
            continue
        key = name.upper()
        if "WORKFLOW" in key:
            counts["workflow"] += len(identities)
        if "ACTIVITY" in key:
            counts["activity"] += len(identities)
    return {"status": "ready" if counts["workflow"] > 0 and counts["activity"] > 0
            else "unavailable", "build_id": build_id, "registered": True,
            "workflow_poller_count": counts["workflow"],
            "activity_poller_count": counts["activity"]}


def _submission_readiness(director: Director, config: Mapping, *,
                          broker_status_reader=None, owner_reader=None,
                          worker_readiness_reader=None) -> dict:
    """Check provider, pin, agent card, and live worker prerequisites without inference.

    Agent services are checked only through A2A discovery: each pinned Agent
    Card must resolve to its pin and declare the expected skill. Their model
    configuration is their own implementation and is not observable here.
    """
    broker_status_reader = broker_status_reader or _redacted_subscription_status
    owner_reader = owner_reader or _pinned_usage_owners
    worker_readiness_reader = worker_readiness_reader or (
        lambda build_id: director.module.runner.wait_worker(build_id, timeout=1.0))
    selection = config.get("director_model")
    selection = selection if isinstance(selection, Mapping) else {}
    provider = selection.get("provider")
    model_id = selection.get("model")
    configured_effort = selection.get("reasoning_effort", DEFAULT_REASONING_EFFORT)
    selection_ready = (provider == "codex-subscription" and
                       model_id == DEFAULT_MODEL_ID and
                       configured_effort == DEFAULT_REASONING_EFFORT)
    blockers = []
    if not selection_ready:
        blockers.append("director_profile_unapproved")
    if provider == "codex-subscription" and (
            "EXO_MODEL_HOME" in os.environ or "EXO_CODEX_BASE_URL" in os.environ):
        blockers.append("default_broker_path_overridden")

    broker_status = "not_checked"
    if selection_ready and "default_broker_path_overridden" not in blockers:
        broker_status = broker_status_reader()
        if broker_status != "available":
            blockers.append("subscription_status_unavailable")

    publication_status = "unavailable"
    owners_ready = 0
    publication_build_id = None
    try:
        verified_context = _verified_active_publication_context(
            director, director.identity)
        publication = director.module.publications.active()
        closure = publication.get("closure")
        if not isinstance(closure, Mapping):
            raise ValueError("active closure unavailable")
        manifest = closure.get("manifest")
        contracts = closure.get("contracts")
        manifest_digest = publication.get("manifest_digest")
        package_digest = publication.get("package_digest")
        publication_build_id = publication.get("build_id")
        if (not isinstance(manifest, Mapping) or not isinstance(contracts, Mapping) or
                not isinstance(manifest_digest, str) or
                not re.fullmatch(r"[0-9a-f]{64}", manifest_digest) or
                not isinstance(publication_build_id, str) or not publication_build_id or
                closure.get("manifest_digest") != manifest_digest or
                verified_context.get("manifest_digest") != manifest_digest or
                manifest.get("package_digest") != package_digest or
                digest(manifest) != manifest_digest):
            raise ValueError("active closure pin mismatch")
        manifest_services = manifest.get("services")
        if not isinstance(manifest_services, Mapping):
            raise ValueError("active service pins unavailable")
        for name, expected_capability in _BASIC_MODEL_OWNERS.items():
            contract = contracts.get(name)
            if (not isinstance(contract, Mapping) or contract.get("name") != name or
                    contract.get("capability") != expected_capability or
                    name not in manifest_services):
                raise ValueError("required model owner pin unavailable")
        scope = {"pinned": {"manifest_digest": manifest_digest,
                            "package_digest": package_digest,
                            "definition_digest": manifest.get("root_digest")}}
        owners, owner_failures, _non_usage_services = owner_reader(director, scope)
        owner_by_name = {owner.get("name"): owner for owner in owners
                         if owner.get("name") in _BASIC_MODEL_OWNERS}
        if owner_failures or set(owner_by_name) != set(_BASIC_MODEL_OWNERS):
            raise ValueError("pinned model owner resolution incomplete")
        publication_status = "pinned"
        for name, expected_capability in _BASIC_MODEL_OWNERS.items():
            if expected_capability in (owner_by_name[name].get("skills") or []):
                owners_ready += 1
    except Exception:
        publication_status = "unavailable"

    if publication_status != "pinned" or owners_ready != len(_BASIC_MODEL_OWNERS):
        blockers.append("pinned_writable_model_owners_unavailable")
    temporal_worker = {"status": "not_required", "build_id": publication_build_id,
                       "registered": None, "workflow_poller_count": None,
                       "activity_poller_count": None}
    if config.get("basic_single_active_job") is True:
        if publication_status == "pinned" and owners_ready == len(_BASIC_MODEL_OWNERS):
            try:
                temporal_worker = _worker_readiness_evidence(
                    publication_build_id, worker_readiness_reader(publication_build_id))
            except Exception:
                temporal_worker = _worker_readiness_evidence(publication_build_id, None)
        else:
            temporal_worker = _worker_readiness_evidence(publication_build_id, None)
        if temporal_worker["status"] != "ready":
            blockers.append("pinned_worker_pollers_unavailable")
    blockers = list(dict.fromkeys(blockers))
    return {
        "submission_status": "ready" if not blockers else "blocked",
        "submission_ready": not blockers,
        "submission_blockers": blockers,
        "director_provider": provider if isinstance(provider, str) else None,
        "director_model": model_id if isinstance(model_id, str) else None,
        "director_reasoning_effort": (DEFAULT_REASONING_EFFORT if selection_ready else None),
        "broker_status": broker_status,
        "publication_status": publication_status,
        "temporal_worker": temporal_worker,
        "pinned_model_owners": {"status": "ready" if owners_ready == 4 else "unavailable",
                                "expected": 4, "ready": owners_ready},
        # Agent Card and configuration checks do not prove that inference succeeded.
        "verification_status": "not_checked",
        "live_inference_ready": False,
    }


def _submission_readiness_callbacks(director: Director, config: Mapping):
    """Return cached health reader and fresh synchronous preflight callbacks."""
    cache_lock = threading.Lock()
    cache = {"at": 0.0, "value": None}

    def read(*, refresh: bool = False) -> dict:
        with cache_lock:
            now = time.monotonic()
            if not refresh and cache["value"] is not None and now - cache["at"] < 2.0:
                return dict(cache["value"])
            value = _submission_readiness(director, config)
            cache.update(at=time.monotonic(), value=value)
            return dict(value)

    def require_ready() -> None:
        value = read(refresh=True)
        if ("pinned_worker_pollers_unavailable" in value["submission_blockers"] and
                set(value["submission_blockers"]) == {"pinned_worker_pollers_unavailable"}):
            build_id = value.get("temporal_worker", {}).get("build_id")
            if isinstance(build_id, str) and build_id:
                try:
                    # Basic mode may start/attach its exact pinned worker here,
                    # before Director model selection. `ensure_runner` returns
                    # only after registration and both live task-queue pollers.
                    director.ensure_runner("basic-submission-worker-readiness", build_id)
                except Exception:
                    pass
                value = read(refresh=True)
        if value["submission_ready"] is not True:
            reason = value["submission_blockers"][0] if value["submission_blockers"] else "unavailable"
            raise Rejected("submission readiness blocked: " + reason)

    return read, require_ready


def _pinned_service_measurements(director: Director, scopes: list[dict],
                                 filters: Mapping[str, str | None]):
    """Agent-reported usage the factory recorded for this run's pinned agents.

    The factory's on-complete hook records each agent Task's budget-extension
    report in its own journal (A2A decision 8). Agents are never polled.
    """
    rows: dict[str, dict] = {}
    conflicts: set[str] = set()
    owners_total = owner_resolution_failures = queries_failed = malformed = 0
    non_usage_services = 0
    database = Path(director.module.home) / "runner" / AGENT_USAGE_DATABASE
    journal = None
    for scope in scopes:
        owners, owner_failures, skipped_services = _pinned_usage_owners(
            director, scope, resolve=False)
        owner_resolution_failures += owner_failures
        non_usage_services += skipped_services
        values, queried = [], False
        if database.is_file():
            try:
                journal = journal or ModelUsageJournal(database)
                values = journal.list_measurements(run_id=scope["run_id"],
                                                   call_scope="assignment_call")
                queried = True
            except Exception:
                queries_failed += 1
        # A release agent makes no model calls: it is a usage owner only when
        # it actually reported usage for this run, otherwise a non-usage service.
        reporting = {raw.get("service_identity") for raw in values if isinstance(raw, Mapping)}
        usage_owners = [owner for owner in owners
                        if owner.get("role") != "release" or owner["identity"] in reporting]
        non_usage_services += len(owners) - len(usage_owners)
        owners_total += len(usage_owners) + owner_failures
        identities = {owner["identity"] for owner in usage_owners}
        if not queried:
            continue
        for raw in values:
            identity = raw.get("service_identity") if isinstance(raw, Mapping) else None
            view = _safe_measurement(raw, service_identity=identity)
            if (view is None or identity not in identities or
                    view.get("evidence_status") not in {"agent_reported", "unknown"} or
                    view.get("run_id") != scope["run_id"] or
                    view.get("definition_digest") != scope["pinned"].get("definition_digest")):
                malformed += 1
                continue
            if not _measurement_matches(view, filters):
                continue
            _merge_measurement(rows, conflicts, view)
    return list(rows.values()), {"pinned_owner_count": owners_total,
        "owner_resolution_failures": owner_resolution_failures,
        "queries_failed": queries_failed,
        "non_usage_service_count": non_usage_services,
        "rows_rejected": malformed, "conflicts": len(conflicts)}


class FactoryModule:
    """Publication, closure pinning and lazy runner access for one instance."""

    def __init__(self, instance_dir: Path, config: dict):
        self.catalog = instance_dir / "catalog"
        self.catalog.mkdir(parents=True, exist_ok=True)
        self.home = Path(config["home"])
        runner_config = config.get("runner", {})
        self.runner = Runner(self.home, port_base=runner_config.get("port_base"),
                             member_base=runner_config.get("member_base"))
        self.publications = PublicationStore(self.catalog)

    def approved(self) -> dict:
        return json.loads((self.catalog / "approved_bindings.json").read_text())

    def publish(self, package: dict, *, label: str, approval: dict,
                interpreter_source: Path = SRC) -> dict:
        """Pin definition, contracts, Quality policy and interpreter build; activate.

        This never starts the runner. Temporal registration of the build is
        verified lazily before the first run that uses it.
        """
        approved = self.approved()
        package_digest = validate(package, approved)
        if approval.get("package_digest") != package_digest or approval.get("status") != "approved":
            raise ValueError("publication requires an approved decision for this exact package")
        if not (self.catalog / f"{package_digest}.json").exists():
            store_package(package, self.catalog, approved)
        contracts = json.loads((self.catalog / "contracts.json").read_text())
        quality_policy = json.loads((self.catalog / "quality_policy.json").read_text())
        build = self.runner.ensure_build(interpreter_source)
        manifest = make_manifest(package, {name: contracts[name] for name in package["bindings"]},
                                 quality_policy, build_id=build["build_id"],
                                 code_digest=build["source_digest"],
                                 python=platform.python_version(),
                                 temporalio=metadata.version("temporalio"))
        closure = {"manifest": manifest, "manifest_digest": digest(manifest),
                   "contracts": {name: contracts[name] for name in package["bindings"]},
                   "quality_policy": quality_policy}
        key = self.publications.publish(package, closure, label=label)
        record = self.publications.activate(
            key, registered_version=f"{DEPLOYMENT}.{build['build_id']}",
            registered_source_digest=build["source_digest"])
        approvals = self.catalog / "approvals.jsonl"
        with approvals.open("a") as stream:
            stream.write(json.dumps({**approval, "manifest_digest": key, "label": label,
                                     "activated_at": time.time()}, sort_keys=True) + "\n")
        return {"manifest_digest": key, "package_digest": package_digest,
                "build_id": record["build_id"], "label": label}

    def package(self, package_digest: str) -> dict:
        package = json.loads((self.catalog / f"{package_digest}.json").read_text())
        if validate(package, self.approved()) != package_digest:
            raise Rejected("published closure changed")
        return package


class Director:
    """The factory-mode agent: owns identity, Task aliases, dedup and fencing."""

    role = "director"

    def __init__(self, instance_dir: Path, config: dict, *, claim: bool = True):
        """claim=False opens the instance for operator tooling without a new incarnation,
        so it never fences the serving harness process."""
        if ("basic_single_active_job" in config and
                type(config["basic_single_active_job"]) is not bool):
            raise ValueError("basic_single_active_job must be an explicit boolean")
        self.basic_single_active_job = config.get("basic_single_active_job") is True
        if self.basic_single_active_job and (config.get("legacy_structured_commands") is True or
                                            config.get("nested_supplier_enabled") is True):
            raise ValueError("Basic single-job mode requires deferred command paths to be disabled")
        # create_app supplies the bounded, value-blind provider readiness check.
        self.submission_preflight = None
        if ("nested_supplier_enabled" in config and
                type(config["nested_supplier_enabled"]) is not bool):
            raise ValueError("nested_supplier_enabled must be an explicit boolean")
        if config.get("nested_supplier_enabled") is True and config.get("mode") != "factory":
            raise ValueError("nested suppliers are available only in factory mode")
        self.config = config
        self.nested_supplier_enabled = config.get("nested_supplier_enabled") is True
        self.module = FactoryModule(instance_dir, config)
        self.database = instance_dir / "director.sqlite3"
        self.wait_seconds = int(config.get("director_wait_seconds", 900))
        with self.connect() as db:
            if self.basic_single_active_job:
                db.executescript("""
                    CREATE TABLE IF NOT EXISTS basic_job_slot (
                        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                        task_id TEXT NOT NULL, context_id TEXT NOT NULL,
                        actor TEXT NOT NULL, state TEXT NOT NULL);
                    CREATE TABLE IF NOT EXISTS basic_job_submissions (
                        message_id TEXT PRIMARY KEY, task_id TEXT NOT NULL,
                        context_id TEXT NOT NULL, actor TEXT NOT NULL,
                        fingerprint TEXT NOT NULL, outcome_json TEXT);
                """)
            db.executescript("""
                CREATE TABLE IF NOT EXISTS identity (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    id TEXT NOT NULL, token TEXT NOT NULL, incarnation INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, context_id TEXT NOT NULL,
                    package_digest TEXT NOT NULL, manifest_digest TEXT NOT NULL,
                    build_id TEXT NOT NULL, label TEXT NOT NULL,
                    run_inputs_json TEXT NOT NULL, run_inputs_digest TEXT NOT NULL,
                    authorized_actor TEXT NOT NULL, input_authority_json TEXT NOT NULL,
                    closed INTEGER NOT NULL DEFAULT 0, outcome_json TEXT);
                CREATE TABLE IF NOT EXISTS commands (
                    action_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
                    run_id TEXT NOT NULL, op TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS aliases (
                    task_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, context_id TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS incidents (
                    run_id TEXT PRIMARY KEY, child_id TEXT, package_digest TEXT NOT NULL,
                    failure_class TEXT NOT NULL, timestamp TEXT NOT NULL,
                    authority_conflict INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS director_tool_calls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL,
                    message_id TEXT NOT NULL, model_kind TEXT NOT NULL, tool TEXT NOT NULL,
                    arguments_json TEXT NOT NULL, result_json TEXT NOT NULL,
                    accepted INTEGER NOT NULL, created_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS director_turns (
                    task_id TEXT NOT NULL, message_id TEXT NOT NULL, model_kind TEXT NOT NULL,
                    model_calls INTEGER NOT NULL, tool_calls INTEGER NOT NULL,
                    result_json TEXT NOT NULL, created_at REAL NOT NULL);
            """)
            if self.nested_supplier_enabled:
                db.execute("""CREATE TABLE IF NOT EXISTS supplier_assignments (
                    action_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL UNIQUE,
                    fingerprint TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    context_id TEXT NOT NULL,
                    echo_json TEXT NOT NULL)""")
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM identity WHERE singleton=1").fetchone()
            if row:
                self.identity, self.token = row["id"], row["token"]
                self.incarnation = row["incarnation"] + (1 if claim else 0)
                if claim:
                    db.execute("UPDATE identity SET incarnation=? WHERE singleton=1",
                               (self.incarnation,))
            else:
                self.identity, self.token, self.incarnation = str(uuid4()), secrets.token_hex(24), 1
                db.execute("INSERT INTO identity VALUES (1, ?, ?, 1)", (self.identity, self.token))
        # This is the Director's public journal owner. The same instance is
        # injected into DirectorTurn and exposed only through its safe reader.
        self.usage_journal = ModelUsageJournal(self.database)
        self.admission_queue = None
        if "admission_capacity" in config:
            capacity = config["admission_capacity"]
            if type(capacity) is not int or capacity < 0:
                raise ValueError("admission_capacity must be an explicit non-negative integer")
            self.admission_queue = AdmissionQueue(
                self.database.parent / "admission.sqlite3",
                factory_id=self.identity, capacity=capacity)
        elif (self.database.parent / "admission.sqlite3").exists():
            raise ValueError("durable admission state exists without explicit admission_capacity")

    def connect(self):
        db = sqlite3.connect(self.database, timeout=15)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        return db

    def list_measurements(self, *, run_id: str | None = None,
                          task_id: str | None = None,
                          assignment_id: str | None = None,
                          attempt_id: str | None = None,
                          model_call_id: str | None = None,
                          call_scope: str | None = None) -> list[dict]:
        """Return this Director's safe public model usage views."""
        return self.usage_journal.list_measurements(
            run_id=run_id, task_id=task_id, assignment_id=assignment_id,
            attempt_id=attempt_id, model_call_id=model_call_id,
            call_scope=call_scope)

    def fence(self, db):
        row = db.execute("SELECT incarnation FROM identity WHERE singleton=1").fetchone()
        if row is None or row["incarnation"] != self.incarnation:
            raise Rejected("stale harness incarnation")

    def unfinished(self) -> list[str]:
        with self.connect() as db:
            return [row["run_id"] for row in db.execute(
                "SELECT run_id FROM runs WHERE closed=0 ORDER BY rowid")]

    def run_id_for(self, action_id: str) -> str:
        return f"{self.identity}.{hashlib.sha256(action_id.encode()).hexdigest()[:20]}"

    def admission_for_task(self, task_id: str) -> dict | None:
        queue = self.admission_queue
        if queue is None:
            return None
        matches = [row for row in queue.list_requests() if row["task_id"] == task_id]
        if len(matches) > 1:
            raise Rejected("Task has conflicting durable admission requests")
        return matches[0] if matches else None

    def _start_action_for_run(self, run_id: str) -> str | None:
        with self.connect() as db:
            row = db.execute("SELECT action_id FROM commands WHERE run_id=? "
                             "AND op IN ('start','nested_factory') "
                             "ORDER BY rowid LIMIT 1", (run_id,)).fetchone()
        return row["action_id"] if row else None

    def supplier_assignment(self, run_id: str) -> dict | None:
        """Return the durable opt-in supplier binding for a run, if present."""
        if not self.nested_supplier_enabled:
            return None
        with self.connect() as db:
            row = db.execute("SELECT * FROM supplier_assignments WHERE run_id=?",
                             (run_id,)).fetchone()
        if row is None:
            return None
        value = dict(row)
        try:
            echo = json.loads(value.pop("echo_json"))
            projected = project_supplier_echo(echo)
        except (json.JSONDecodeError, SupplierEnvelopeError) as error:
            raise Rejected("stored nested_factory binding is invalid") from error
        if set(echo) != set(projected) or echo != projected:
            raise Rejected("stored nested_factory binding contains unapproved fields")
        value["echo"] = projected
        return value

    async def _dispatch_admitted(self, request: Mapping) -> dict | None:
        if request.get("state") != "admitted":
            return None
        task_id = request.get("task_id")
        binding = self.task_binding(task_id) if isinstance(task_id, str) else None
        if binding is None:
            raise Rejected("admitted request has no original Task binding")
        run_id, _context_id = binding
        run = self.run_record(run_id)
        if run["closed"]:
            # A closed run is released by its authoritative terminal projection,
            # never restarted to fill an already-finished admission.
            return None
        publication = self.module.publications.get(run["manifest_digest"])
        if (publication.get("manifest_digest") != run["manifest_digest"] or
                publication.get("package_digest") != run["package_digest"]):
            raise Rejected("admitted run's pinned publication is unavailable")
        package = self.module.package(run["package_digest"])
        if not self.module.runner.is_running():
            self.ensure_runner(f"admitted-factory-work:{run_id}", publication["build_id"])
        await self._start(run, package, publication)
        return {"run_id": run_id, "task_id": task_id, "state": "admitted"}

    async def _release_terminal_admission(self, task_id: str, run_id: str,
                                          terminal_state: str) -> dict | None:
        queue = self.admission_queue
        if queue is None or terminal_state not in TERMINAL:
            return None
        binding = self.task_binding(task_id)
        if binding is None or binding[0] != run_id:
            raise Rejected("terminal release does not match the original Task binding")
        run = self.run_record(run_id)
        if not run["closed"] or not run["outcome_json"]:
            raise Rejected("admission release requires a persisted terminal Task outcome")
        outcome = json.loads(run["outcome_json"])
        if outcome.get("state") != terminal_state:
            raise Rejected("terminal release state does not match the persisted Task outcome")
        request = self.admission_for_task(task_id)
        if request is None or request["state"] != "admitted":
            return None
        release_id = "terminal-v1:" + hashlib.sha256(canonical({
            "factory_id": self.identity, "run_id": run_id,
            "task_id": task_id, "state": terminal_state,
        }).encode()).hexdigest()
        result = queue.release(request["request_id"], release_id=release_id)
        for promoted in result.get("admitted", []):
            await self._dispatch_admitted(promoted)
        return result

    async def client(self):
        return await Client.connect(self.module.runner.address, namespace=NAMESPACE)

    def ensure_runner(self, reason: str, build_id: str | None = None) -> dict:
        ready = self.module.runner.ensure_started(reason=reason)
        if build_id is not None:
            ready = {**ready, "worker": self.module.runner.wait_worker(build_id)}
        return ready

    async def _start(self, run: dict, package: dict, publication: dict) -> None:
        client = await self.client()
        handle = client.get_workflow_handle(run["run_id"])
        try:
            status = await handle.query(FactoryRun.status)
            if (status["package_digest"] != run["package_digest"]
                    or status["manifest_digest"] != run["manifest_digest"]
                    or status.get("run_inputs_digest") != run["run_inputs_digest"]):
                raise Rejected("run exists with a different pinned closure or inputs")
            return
        except RPCError:
            pass
        root = package["root"]
        closure = publication["closure"]
        verify_closure(closure, package, build_id=publication["build_id"],
                       definition_digest=digest(root), document=root)
        # The server-side Director token is never accepted by this persisted
        # payload constructor, for parent or child Workflow history.
        value = build_workflow_input(self.identity, self.incarnation, run, package,
                                     publication, self.wait_seconds)
        try:
            execution_timeout_seconds = _workflow_execution_timeout_seconds(
                package, self.wait_seconds)
            await client.start_workflow(
                FactoryRun.run, value, id=run["run_id"], task_queue=QUEUE,
                execution_timeout=timedelta(seconds=execution_timeout_seconds),
                versioning_override=PinnedVersioningOverride(
                    WorkerDeploymentVersion(DEPLOYMENT, publication["build_id"])))
        except Exception as start_error:
            try:
                status = await handle.query(FactoryRun.status)
            except Exception as reconcile_error:
                # A failed reconciliation read must not hide the original
                # start RPC error (for example, worker build/version missing).
                raise start_error from reconcile_error
            if (not isinstance(status, Mapping) or
                    status.get("package_digest") != run["package_digest"] or
                    status.get("manifest_digest") != run["manifest_digest"] or
                    status.get("run_inputs_digest") != run["run_inputs_digest"]):
                raise Rejected("concurrent run closure conflict") from start_error

    async def _director_decision(self, run_id: str, action_id: str, revision: str,
                                 sha256: str, action: str, authenticated_actor: str) -> dict:
        client = await self.client()
        parent = await client.get_workflow_handle(run_id).query(FactoryRun.status)
        if not parent["child_id"]:
            raise DirectorDecisionRejected("decision-child-missing",
                                           "run has no nested factory child")
        child = client.get_workflow_handle(parent["child_id"])
        status = await child.query(FactoryRun.status)
        expected_outcome = f"{action}-recorded"
        applied = status.get("applied_decisions") or {}
        prior = applied.get(action_id) if isinstance(applied, Mapping) else None
        # A stable workflow receipt survives a Runtime crash between Update
        # acceptance and local acknowledgement. Keep the old abort receipt path
        # for histories written before applied_decisions was introduced.
        if prior is not None or (action == "abort" and status.get("decision_id") == action_id):
            if prior is not None and prior != expected_outcome:
                raise DirectorDecisionRejected("decision-action-id-conflict",
                                               "command_id was already applied to another decision")
            return {"command_id": action_id, "action": action,
                    "lifecycle": "applied", "outcome": expected_outcome,
                    "update_result": expected_outcome}
        phase = status.get("phase")
        if ((action == "escalate" and phase != "awaiting-director") or
                (action == "abort" and phase not in {"awaiting-director", "awaiting-human"})):
            raise DirectorDecisionRejected("decision-wait-missing",
                                           "run has no matching Director wait")
        if action not in status.get("permitted_actions", []):
            raise DirectorDecisionRejected("decision-action-not-permitted",
                                           "action is not permitted at the current wait")
        deadline = status.get("deadline")
        if (isinstance(deadline, bool) or not isinstance(deadline, (int, float)) or
                time.time() >= deadline):
            raise DirectorDecisionRejected("decision-deadline-unavailable-or-expired",
                                           "decision wait deadline expired or unavailable")
        workflow_actor = (authenticated_actor if phase == "awaiting-human" else self.identity)
        if status.get("decision_actor") != workflow_actor:
            raise DirectorDecisionRejected("decision-actor-mismatch",
                                           "decision actor does not match the pinned wait authority")
        if status["current_revision"] != revision or status["current_sha256"] != sha256:
            raise DirectorDecisionRejected("decision-artifact-stale", "stale revision/digest")
        if status["owner_epoch"] > self.incarnation:
            raise DirectorDecisionRejected("decision-owner-epoch-newer",
                                           "Temporal owner epoch is newer than this Runtime")
        if status["owner_epoch"] < self.incarnation:
            await child.execute_update(FactoryRun.claim_owner, {
                "actor": self.identity, "epoch": self.incarnation})
            status = await child.query(FactoryRun.status)
            if (status.get("phase") != phase or action not in status.get("permitted_actions", [])
                    or status.get("decision_actor") != workflow_actor
                    or status.get("current_revision") != revision
                    or status.get("current_sha256") != sha256
                    or isinstance(status.get("deadline"), bool)
                    or not isinstance(status.get("deadline"), (int, float))
                    or time.time() >= status["deadline"]):
                raise DirectorDecisionRejected("decision-wait-changed-during-owner-claim",
                                               "wait changed while claiming Director ownership")
        result = await child.execute_update(FactoryRun.director_command, {
            "command_id": action_id, "action": action, "actor": workflow_actor,
            "epoch": self.incarnation, "run": status["run"],
            "definition_digest": status["definition_digest"],
            "revision": revision, "sha256": sha256})
        if result != expected_outcome:
            raise DirectorDecisionRejected("decision-receipt-unexpected",
                                           "Temporal returned an unexpected decision receipt")
        return {"command_id": action_id, "action": action,
                "lifecycle": "applied", "outcome": expected_outcome,
                "update_result": result}

    def perform(self, command: dict, task_id: str, context_id: str) -> dict:
        """Capability contract verified-research@1, as seen by A2A callers.

        start:   {op:"start", action_id, inputs:{question}}
        inspect: {op:"inspect"} on the original Task
        escalate:{op:"escalate", action_id, revision, sha256} on the original Task,
                 escalating its input-required Director wait.
        abort:   {op:"abort", action_id, revision, sha256} on that same Task,
                 answering a Director or explicitly assigned human wait.
        """
        op = command.get("op")
        action_id = command.get("action_id")
        if op not in {"start", "inspect", "abort", "escalate"}:
            raise Rejected("unsupported command for verified-research@1")
        if op != "inspect" and (not isinstance(action_id, str) or not action_id):
            raise Rejected("mutation requires a stable action_id")
        actor = CURRENT_ACTOR.get()
        if actor is None:
            raise Rejected("authenticated caller required")
        if op in {"abort", "escalate"}:
            binding = self.task_binding(task_id)
            if binding is None or binding[1] != context_id:
                code = ("decision-original-task-binding-missing" if binding is None else
                        "decision-task-context-mismatch")
                raise DirectorDecisionRejected(code,
                                               "decision must continue the original factory Task")
            if (set(command) != {"op", "action_id", "revision", "sha256"} or
                    not isinstance(command.get("revision"), str) or not command["revision"] or
                    not isinstance(command.get("sha256"), str) or
                    not re.fullmatch(r"[0-9a-f]{64}", command["sha256"])):
                raise Rejected("decision requires the exact inspected revision and digest")
            run_id = binding[0]
            run_record = self.run_record(run_id)
            current = self.inspect_bound_run(task_id)
            owner = run_record["authorized_actor"]
            decision_outcome = (current.get("applied_decisions") or {}).get(action_id)
            expected_outcome = f"{op}-recorded"
            prior_command = None
            with self.connect() as db:
                prior_command = db.execute("SELECT fingerprint,run_id FROM commands "
                                           "WHERE action_id=?", (action_id,)).fetchone()
            bound_fingerprint = hashlib.sha256(canonical({
                "command": command, "actor": actor}).encode()).hexdigest()
            legacy_fingerprint = hashlib.sha256(canonical(command).encode()).hexdigest()
            if prior_command and (prior_command["run_id"] != run_id or
                    (prior_command["fingerprint"] != bound_fingerprint and
                     not (prior_command["fingerprint"] == legacy_fingerprint and actor == owner))):
                raise DirectorDecisionRejected("decision-action-id-conflict", "action_id conflict")
            if decision_outcome is not None:
                if decision_outcome != expected_outcome or not prior_command:
                    raise DirectorDecisionRejected("decision-command-receipt-unbound",
                        "command receipt is not bound to this actor and action")
            else:
                if op == "escalate":
                    allowed_actor = actor == owner
                    allowed_phase = current.get("phase") == "awaiting-director"
                elif current.get("phase") == "awaiting-director":
                    allowed_actor = actor == owner
                    allowed_phase = True
                elif current.get("phase") == "awaiting-human":
                    allowed_actor = actor == current.get("decision_actor")
                    allowed_phase = True
                else:
                    allowed_actor = allowed_phase = False
                if not allowed_phase or not allowed_actor:
                    raise DirectorDecisionRejected("decision-actor-unauthorized",
                                                   "actor is not authorized for the current decision wait")
                if op not in current.get("permitted_actions", []):
                    raise DirectorDecisionRejected("decision-action-not-permitted",
                                                   "decision action is not permitted at the current wait")
                if (current.get("current_revision") != command["revision"] or
                        current.get("current_sha256") != command["sha256"]):
                    raise DirectorDecisionRejected("decision-artifact-stale",
                                                   "stale revision/digest")
        else:
            bound_fingerprint = hashlib.sha256(canonical(command).encode()).hexdigest()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self.fence(db)
            alias = db.execute("SELECT * FROM aliases WHERE task_id=?", (task_id,)).fetchone()
            if op == "start":
                if alias is not None and alias["run_id"] != self.run_id_for(action_id):
                    raise Rejected("this Task is already bound to a different factory run")
                if set(command) - {"op", "action_id", "inputs"}:
                    raise Rejected("invalid start fields; callers do not choose graphs or versions")
                run_id = self.run_id_for(action_id)
                existing = db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
                if existing:
                    publication = self.module.publications.get(existing["manifest_digest"])
                else:
                    publication = self.module.publications.active()
                package = self.module.package(publication["package_digest"])
                try:
                    run_inputs, authority = authorize_run_inputs(
                        package["run_inputs"], command.get("inputs", {}), actor)
                except ValueError as error:
                    raise Rejected(str(error)) from error
                inputs_digest = digest(run_inputs)
                if existing and (existing["run_inputs_digest"] != inputs_digest
                                 or existing["authorized_actor"] != actor):
                    raise Rejected("action_id reused with different inputs or actor")
                if existing and existing["task_id"] != task_id:
                    # One result per run, on its original Task: never alias a second Task.
                    raise Rejected("duplicate start; continue original Task " + existing["task_id"])
                db.execute("INSERT OR IGNORE INTO runs (run_id, task_id, context_id, package_digest,"
                           " manifest_digest, build_id, label, run_inputs_json, run_inputs_digest,"
                           " authorized_actor, input_authority_json) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                           (run_id, task_id, context_id, publication["package_digest"],
                            publication["manifest_digest"], publication["build_id"],
                            publication.get("label", ""), canonical(run_inputs), inputs_digest,
                            actor, canonical(authority)))
                run = dict(db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone())
            else:
                if alias is None:
                    if op in {"abort", "escalate"}:
                        raise DirectorDecisionRejected("decision-original-task-binding-missing",
                                                       "decision must continue the original factory Task")
                    raise Rejected("inspect/decision must continue the original factory Task")
                if alias["context_id"] != context_id:
                    if op in {"abort", "escalate"}:
                        raise DirectorDecisionRejected("decision-task-context-mismatch",
                                                       "original factory Task context mismatch")
                    raise Rejected("original factory Task context mismatch")
                run_id = alias["run_id"]
                owner = db.execute("SELECT authorized_actor FROM runs WHERE run_id=?",
                                   (run_id,)).fetchone()
                if owner is None or (op in {"inspect", "escalate"} and
                                     owner["authorized_actor"] != actor):
                    if op in {"abort", "escalate"}:
                        raise DirectorDecisionRejected("decision-actor-unauthorized",
                                                       "actor is not authorized for this factory run")
                    raise Rejected("actor is not authorized for this factory run")
            if op != "inspect":
                prior = db.execute("SELECT * FROM commands WHERE action_id=?", (action_id,)).fetchone()
                if prior and (prior["fingerprint"] not in {
                        bound_fingerprint, hashlib.sha256(canonical(command).encode()).hexdigest()
                        if op in {"abort", "escalate"} else bound_fingerprint} or
                        prior["run_id"] != run_id):
                    if op in {"abort", "escalate"}:
                        raise DirectorDecisionRejected("decision-action-id-conflict",
                                                       "action_id conflict")
                    raise Rejected("action_id conflict")
                db.execute("INSERT OR IGNORE INTO commands VALUES (?, ?, ?, ?)",
                           (action_id, bound_fingerprint, run_id, op))
            db.execute("INSERT OR IGNORE INTO aliases VALUES (?, ?, ?)", (task_id, run_id, context_id))
        admission = None
        if self.admission_queue is not None:
            admission = self.admission_for_task(task_id)
        if op == "start":
            if self.admission_queue is not None:
                admission = self.admission_queue.enqueue(action_id, task_id=task_id)
                if admission["state"] == "admitted":
                    sync(self._dispatch_admitted(admission))
                outcome = {"queued": "queued", "admitted": "admitted",
                           "released": "terminal"}.get(admission["state"], "unavailable")
                return {"run_id": run_id, "accepted_command": op,
                        "command_id": action_id, "lifecycle": "applied",
                        "outcome": outcome, "admission_state": admission["state"]}
            self.ensure_runner(f"factory-work:{run_id}", publication["build_id"])
            sync(self._start(run, package, publication))
        elif op == "inspect":
            if admission is not None and admission["state"] != "released":
                return {"run_id": run_id, "accepted_command": op,
                        "lifecycle": "applied", "outcome": admission["state"],
                        "admission_state": admission["state"]}
        elif op in {"abort", "escalate"}:
            if admission is not None and admission["state"] != "admitted":
                raise DirectorDecisionRejected("decision-admission-not-admitted",
                    "only admitted factory work can receive a Director command")
            if self.admission_queue is not None and admission is None:
                raise DirectorDecisionRejected("decision-admission-missing",
                    "factory run has no durable admission request")
            if admission is not None:
                # An admitted run is already authoritative work. A not-yet-
                # started admission cannot receive a human/Director decision.
                sync(self._dispatch_admitted(admission))
            self.ensure_runner(f"director-command:{run_id}")
            outcome = sync(self._director_decision(
                run_id, action_id, command["revision"], command["sha256"], op, actor))
            return {"run_id": run_id, "accepted_command": op, **outcome}
        return {"run_id": run_id, "accepted_command": op,
                "command_id": action_id, "lifecycle": "applied", "outcome": op}

    def task_binding(self, task_id: str):
        with self.connect() as db:
            row = db.execute("SELECT * FROM aliases WHERE task_id=?", (task_id,)).fetchone()
            return (row["run_id"], row["context_id"]) if row else None

    def inspect_bound_run(self, task_id: str) -> dict:
        binding = self.task_binding(task_id)
        if binding is None:
            raise Rejected("inspect must continue the original factory Task")
        # Closed executions can be pinned to a drained Temporal worker
        # deployment, for which a live Workflow query is unavailable. The
        # Director's durable terminal row is sufficient to say there is no
        # current wait or permitted decision; do not fail source refresh by
        # querying that closed execution.
        run_record = self.run_record(binding[0])
        if run_record.get("closed") == 1:
            return {"run_id": binding[0], "node": None, "phase": None,
                    "current_revision": None, "current_sha256": None,
                    "wait_started_at": None, "wait_deadline": None,
                    "decision_actor": None, "permitted_actions": [],
                    "applied_decisions": {}}
        async def status():
            client = await self.client()
            parent = await client.get_workflow_handle(binding[0]).query(FactoryRun.status)
            if not parent.get("child_id"):
                return parent
            return await client.get_workflow_handle(parent["child_id"]).query(FactoryRun.status)
        raw = sync(status())
        verdict = raw.get("quality_verdict") or {}
        return {"run_id": raw.get("run"), "node": raw.get("node"),
                "phase": raw.get("phase"), "current_revision": raw.get("current_revision"),
                "current_sha256": raw.get("current_sha256"),
                "repair_count": raw.get("repair_count"),
                "max_repairs": raw.get("max_repairs"),
                "quality_findings": verdict.get("findings"),
                "wait_started_at": raw.get("wait_started_at"),
                "wait_deadline": raw.get("deadline"),
                "decision_actor": raw.get("decision_actor"),
                "permitted_actions": [action for action in raw.get("permitted_actions", [])
                    if isinstance(action, str) and action in {"abort", "escalate"}],
                "applied_decisions": {command_id: outcome for command_id, outcome in
                    (raw.get("applied_decisions") or {}).items()
                    if isinstance(command_id, str) and outcome in {
                        "abort-recorded", "escalate-recorded"}}}

    def run_record(self, run_id: str) -> dict:
        with self.connect() as db:
            row = db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None:
                raise Rejected("unknown run")
            return dict(row)

    def close_run(self, run_id: str, projection: dict) -> None:
        with self.connect() as db:
            db.execute("UPDATE runs SET closed=1, outcome_json=? WHERE run_id=? AND closed=0",
                       (canonical(projection), run_id))

    def record_incident(self, incident: dict) -> dict:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("INSERT OR IGNORE INTO incidents VALUES (?, ?, ?, ?, ?, ?)",
                       (incident["run_id"], incident["child_id"], incident["package_digest"],
                        incident["failure_class"], incident["timestamp"],
                        int(incident["authority_conflict"])))
            return dict(db.execute("SELECT * FROM incidents WHERE run_id=?",
                                   (incident["run_id"],)).fetchone())

    def _nested_factory(self, command: Mapping, task_id: str, context_id: str) -> dict:
        """Accept one pinned nested run on the original receiving A2A Task."""
        if not self.nested_supplier_enabled:
            raise Rejected("nested_factory is not enabled for this factory")
        actor = CURRENT_ACTOR.get()
        if actor is None:
            raise Rejected("authenticated caller required")
        if not isinstance(task_id, str) or not task_id or not isinstance(context_id, str) or not context_id:
            raise Rejected("nested_factory requires the receiving A2A Task/context")
        try:
            envelope = parse_nested_factory_envelope(command)
            request_fingerprint = nested_factory_fingerprint(envelope)
        except SupplierEnvelopeError as error:
            raise Rejected(str(error)) from error

        publication = package = run = None
        replayed = False
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self.fence(db)
            prior_command = db.execute(
                "SELECT * FROM commands WHERE action_id=?", (envelope.action_id,)).fetchone()
            prior_run = db.execute(
                "SELECT * FROM runs WHERE run_id=?", (envelope.run_id,)).fetchone()
            prior_assignment = db.execute(
                "SELECT * FROM supplier_assignments WHERE action_id=?",
                (envelope.action_id,)).fetchone()
            assignment_for_run = db.execute(
                "SELECT * FROM supplier_assignments WHERE run_id=?",
                (envelope.run_id,)).fetchone()
            alias = db.execute("SELECT * FROM aliases WHERE task_id=?", (task_id,)).fetchone()

            if prior_command is not None:
                if (prior_command["op"] != "nested_factory" or prior_assignment is None or
                        prior_command["fingerprint"] != request_fingerprint or
                        prior_command["run_id"] != envelope.run_id or
                        prior_assignment["run_id"] != envelope.run_id or
                        prior_assignment["fingerprint"] != request_fingerprint or
                        prior_assignment["task_id"] != task_id or
                        prior_assignment["context_id"] != context_id or
                        prior_assignment["echo_json"] != canonical(envelope.echo_tuple()) or
                        prior_run is None or prior_run["task_id"] != task_id or
                        prior_run["context_id"] != context_id or
                        prior_run["authorized_actor"] != actor or alias is None or
                        alias["run_id"] != envelope.run_id or
                        alias["context_id"] != context_id):
                    raise Rejected("nested_factory action_id conflicts with its accepted binding")
                replayed = True
                run = dict(prior_run)
                try:
                    publication = self.module.publications.get(run["manifest_digest"])
                    package = self.module.package(run["package_digest"])
                except Exception as error:
                    raise Rejected("nested_factory pinned publication is unavailable") from error
                if (publication.get("manifest_digest") != run["manifest_digest"] or
                        publication.get("package_digest") != run["package_digest"] or
                        digest(package.get("root", {})) != envelope.definition_digest):
                    raise Rejected("nested_factory pinned publication changed")
            else:
                if prior_assignment is not None or assignment_for_run is not None:
                    raise Rejected("nested_factory run or action ID is already bound")
                if prior_run is not None:
                    # Explicit supplier run IDs may not collide with ordinary starts
                    # or with a different nested assignment.
                    raise Rejected("nested_factory run_id is already in use")
                if alias is not None:
                    raise Rejected("receiving A2A Task is already bound to another run")
                if db.execute("SELECT 1 FROM aliases WHERE task_id=?", (task_id,)).fetchone():
                    raise Rejected("receiving A2A Task binding conflict")

                try:
                    publication = self.module.publications.active()
                    if (not isinstance(publication, Mapping) or
                            not all(isinstance(publication.get(key), str) and publication[key]
                                    for key in ("manifest_digest", "package_digest", "build_id"))):
                        raise ValueError("active publication envelope is malformed")
                    package = self.module.package(publication["package_digest"])
                except Exception as error:
                    raise Rejected("active publication is unavailable") from error
                root = package.get("root")
                if not isinstance(root, Mapping) or digest(root) != envelope.definition_digest:
                    raise Rejected("nested_factory definition_digest is not the active root")
                try:
                    run_inputs, authority = authorize_run_inputs(
                        package["run_inputs"], envelope.inputs, actor)
                except ValueError as error:
                    raise Rejected(str(error)) from error
                inputs_digest = digest(run_inputs)
                db.execute(
                    "INSERT INTO runs (run_id, task_id, context_id, package_digest, "
                    "manifest_digest, build_id, label, run_inputs_json, run_inputs_digest, "
                    "authorized_actor, input_authority_json) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (envelope.run_id, task_id, context_id, publication["package_digest"],
                     publication["manifest_digest"], publication["build_id"],
                     publication.get("label", ""), canonical(run_inputs), inputs_digest,
                     actor, canonical(authority)))
                run = dict(db.execute("SELECT * FROM runs WHERE run_id=?",
                                      (envelope.run_id,)).fetchone())
                db.execute("INSERT INTO commands VALUES (?, ?, ?, 'nested_factory')",
                           (envelope.action_id, request_fingerprint, envelope.run_id))
                db.execute("INSERT INTO aliases VALUES (?, ?, ?)",
                           (task_id, envelope.run_id, context_id))
                db.execute("INSERT INTO supplier_assignments "
                           "(action_id,run_id,fingerprint,task_id,context_id,echo_json) "
                           "VALUES (?,?,?,?,?,?)",
                           (envelope.action_id, envelope.run_id, request_fingerprint,
                            task_id, context_id, canonical(envelope.echo_tuple())))

        if run is None or package is None or publication is None:
            raise Rejected("nested_factory acceptance did not persist a runnable binding")
        if run["closed"]:
            prior_outcome = json.loads(run["outcome_json"] or "{}")
            outcome = prior_outcome.get("state", "terminal")
            admission_state = "released"
        elif self.admission_queue is not None:
            admission = self.admission_queue.enqueue(envelope.action_id, task_id=task_id)
            admission_state = admission["state"]
            if admission_state == "admitted":
                sync(self._dispatch_admitted(admission))
            outcome = {"queued": "queued", "admitted": "admitted",
                       "released": "terminal"}.get(admission_state, "unavailable")
        else:
            self.ensure_runner(f"nested-factory:{envelope.run_id}", publication["build_id"])
            sync(self._start(run, package, publication))
            outcome, admission_state = "nested_factory", None
        return {"run_id": envelope.run_id, "accepted_command": "nested_factory",
                "command_id": envelope.action_id, "lifecycle": "applied",
                "outcome": outcome, "admission_state": admission_state,
                "replayed": replayed}

    def basic_job_status(self) -> dict:
        """Safe occupancy fact; never expose caller identities through health."""
        if not self.basic_single_active_job:
            return {"configured": False, "busy": False}
        with self.connect() as db:
            row = db.execute("SELECT task_id,state FROM basic_job_slot WHERE singleton=1").fetchone()
            if row and row["state"] in {"active", "uncertain"}:
                terminal = db.execute("SELECT r.closed,r.outcome_json FROM runs r "
                                      "JOIN aliases a ON a.run_id=r.run_id WHERE a.task_id=?",
                                      (row["task_id"],)).fetchone()
                if (terminal and terminal["closed"] == 1 and terminal["outcome_json"] and
                        json.loads(terminal["outcome_json"]).get("state") in TERMINAL):
                    row = None
            existing = db.execute("SELECT 1 FROM runs WHERE closed=0 LIMIT 1").fetchone()
        if row is None and existing:
            return {"configured": True, "busy": True, "state": "active"}
        return {"configured": True, "busy": row is not None,
                "state": row["state"] if row else "available"}

    async def _basic_refresh_slot(self) -> None:
        """Use the owned Task reader to persist authoritative terminal facts."""
        if not self.basic_single_active_job:
            return
        with self.connect() as db:
            slot = db.execute("SELECT task_id,state FROM basic_job_slot WHERE singleton=1").fetchone()
            bound = (db.execute("SELECT 1 FROM aliases WHERE task_id=?",
                                (slot["task_id"],)).fetchone() if slot else None)
            tasks = ([slot["task_id"]] if slot and bound and slot["state"] in {"active", "uncertain"} else
                     [row["task_id"] for row in db.execute("SELECT task_id FROM runs WHERE closed=0")]
                     if slot is None else [])
        for original_task in dict.fromkeys(tasks):
            try:
                await FactoryTaskStore(self).get(original_task)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Unknown execution state never frees an occupied slot.
                pass

    def _basic_claim_submission(self, task_id: str, context_id: str, message_id: str,
                                actor: str, brief: str) -> tuple[bool, dict | None]:
        if not self.basic_single_active_job:
            return False, None
        if not all(isinstance(value, str) and value for value in
                   (task_id, context_id, message_id, actor, brief)):
            raise Rejected("submission requires an authenticated original Task binding")
        fingerprint = hashlib.sha256(canonical({"task_id": task_id,
            "context_id": context_id, "message_id": message_id,
            "actor": actor, "brief": brief}).encode()).hexdigest()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self.fence(db)
            prior = db.execute("SELECT * FROM basic_job_submissions WHERE message_id=?",
                               (message_id,)).fetchone()
            if prior:
                if prior["fingerprint"] != fingerprint:
                    raise Rejected("submission message identity conflict")
                if prior["outcome_json"] is None:
                    raise Rejected("original submission is pending; no duplicate model scheduling")
                return True, json.loads(prior["outcome_json"])
            slot = db.execute("SELECT * FROM basic_job_slot WHERE singleton=1").fetchone()
            if slot and slot["state"] in {"active", "uncertain"}:
                terminal = db.execute("SELECT r.closed,r.outcome_json FROM runs r "
                                      "JOIN aliases a ON a.run_id=r.run_id WHERE a.task_id=?",
                                      (slot["task_id"],)).fetchone()
                if (terminal and terminal["closed"] == 1 and terminal["outcome_json"] and
                        json.loads(terminal["outcome_json"]).get("state") in TERMINAL):
                    db.execute("DELETE FROM basic_job_slot WHERE singleton=1")
                    slot = None
            if slot:
                if (slot["task_id"] != task_id or slot["context_id"] != context_id or
                        slot["actor"] != actor):
                    raise Rejected("factory permits one active job; another original Task is busy")
                if slot["state"] != "active":
                    raise Rejected("original submission is pending or uncertain; no duplicate model scheduling")
            else:
                # Activating Basic mode cannot bypass a pre-existing unfinished run.
                unfinished = db.execute("SELECT task_id,context_id,authorized_actor FROM runs WHERE closed=0").fetchall()
                if any(row["task_id"] != task_id or row["context_id"] != context_id or
                       row["authorized_actor"] != actor for row in unfinished):
                    raise Rejected("factory permits one active job; an existing original Task is busy")
                db.execute("INSERT INTO basic_job_slot VALUES (1,?,?,?,?)",
                           (task_id, context_id, actor, "pending"))
            db.execute("UPDATE basic_job_slot SET state='pending' WHERE singleton=1")
            db.execute("INSERT INTO basic_job_submissions VALUES (?,?,?,?,?,NULL)",
                       (message_id, task_id, context_id, actor, fingerprint))
        return False, None

    def _basic_finish_submission(self, task_id: str, message_id: str, result: dict, *,
                                 uncertain: bool = False) -> None:
        if not self.basic_single_active_job:
            return
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self.fence(db)
            db.execute("UPDATE basic_job_submissions SET outcome_json=? WHERE message_id=? AND task_id=?",
                       (canonical(result), message_id, task_id))
            binding = db.execute("SELECT run_id FROM aliases WHERE task_id=?", (task_id,)).fetchone()
            if uncertain or binding:
                db.execute("UPDATE basic_job_slot SET state=? WHERE singleton=1 AND task_id=?",
                           ("uncertain" if uncertain else "active", task_id))
            else:
                db.execute("DELETE FROM basic_job_slot WHERE singleton=1 AND task_id=?", (task_id,))

    async def invoke(self, command, task_id, context_id, *, message_id=None):
        if isinstance(command, str):
            from director_agent import DirectorTurn, selected_model
            selection = self.config.get("director_model", {"provider": "fixture"})
            if selection.get("provider", "fixture") == "fixture":
                return {"error": "text briefs require a configured broker-backed Director"}
            message_id = message_id or str(uuid4())
            await self._basic_refresh_slot()
            replayed, prior = self._basic_claim_submission(
                task_id, context_id, message_id, CURRENT_ACTOR.get(), command)
            if replayed:
                return prior
            model_started = False
            try:
                if self.basic_single_active_job:
                    if not callable(self.submission_preflight):
                        raise Rejected("live provider readiness is unavailable")
                    await asyncio.to_thread(self.submission_preflight)
                model, kind = selected_model(self.config, session_id=str(uuid4()))
                turn = DirectorTurn(self, task_id, context_id, message_id, kind,
                                    usage_journal=self.usage_journal)
                model_started = True
                outcome = await turn.run(command, model)
                result = ({"error": "Director issued no accepted command", "director_turn": outcome}
                          if not outcome["accepted"] else
                          {"accepted_command": outcome["accepted"][-1]})
                self._basic_finish_submission(task_id, message_id, result)
                return result
            except BaseException:
                self._basic_finish_submission(task_id, message_id,
                    {"error": ("submission outcome is uncertain; no automatic retry" if model_started
                               else "submission unavailable before model scheduling")},
                    uncertain=model_started)
                raise
        if isinstance(command, Mapping) and command.get("op") == "nested_factory":
            return self._nested_factory(command, task_id, context_id)
        if self.config.get("legacy_structured_commands") is not True:
            raise Rejected("factory caller messages must contain text parts only")
        agent = Agent(name="Factory Director", model=ToolCallingModelFixture(),
                      plugins=[HarnessPlugin(self, task_id, context_id)], callback_handler=None)
        result = await agent.invoke_async(canonical(command))
        return json.loads(str(result))

    def recover(self) -> dict | None:
        """At startup, start the runner only if this instance has unfinished runs."""
        if self.admission_queue is not None:
            queue = self.admission_queue
            # Recover a crash after Director persisted the original Task/run but
            # before its admission intent was committed.
            for run_id in self.unfinished():
                run = self.run_record(run_id)
                request_id = self._start_action_for_run(run_id)
                if not request_id:
                    raise Rejected("unfinished run has no durable start action")
                existing = queue.get(request_id)
                if existing is None:
                    existing = queue.enqueue(request_id, task_id=run["task_id"])
                elif existing["task_id"] != run["task_id"]:
                    raise Rejected("admission request does not match the original Task")
                elif existing["state"] == "released":
                    raise Rejected("unfinished run has a released admission slot")
            queue.admit_waiting()
            started = 0
            while True:
                requests = queue.list_requests(state="admitted")
                closed = []
                for request in requests:
                    binding = self.task_binding(request["task_id"])
                    if binding is None:
                        raise Rejected("admitted request has no original Task binding")
                    run = self.run_record(binding[0])
                    if run["closed"] and run["outcome_json"]:
                        outcome = json.loads(run["outcome_json"])
                        if outcome.get("state") in TERMINAL:
                            closed.append((request["task_id"], binding[0], outcome["state"]))
                if not closed:
                    for request in requests:
                        run = self.run_record(self.task_binding(request["task_id"])[0])
                        if not run["closed"]:
                            sync(self._dispatch_admitted(request))
                            started += 1
                    break
                for task_id, run_id, state in closed:
                    sync(self._release_terminal_admission(task_id, run_id, state))
            return ({"reason": f"recover-admitted:{started}"} if started else None)
        runs = self.unfinished()
        if not runs:
            return None
        return self.ensure_runner(f"recover-unfinished:{len(runs)}")


class FactoryTaskStore(ProjectionTaskStore):
    """Projects Temporal state onto the original A2A Task; Temporal stays authoritative."""

    def __init__(self, director: Director):
        self.director = director

    async def reconcile_admitted(self) -> dict[str, int]:
        """Observe admitted Tasks and release only durable terminal outcomes.

        This lifecycle path uses the public AdmissionQueue reader and this
        factory's TaskStore projection. Query failures leave the slot admitted.
        """
        queue = self.director.admission_queue
        if queue is None:
            return {"checked": 0, "query_errors": 0}
        requests = await asyncio.to_thread(
            queue.list_requests, state="admitted")
        checked = errors = 0
        for request in requests:
            try:
                await self.get(request["task_id"])
                checked += 1
            except asyncio.CancelledError:
                raise
            except Exception:
                # Unknown Temporal state is not a terminal fact; retain the
                # slot and retry during the next bounded reconciliation pass.
                errors += 1
        return {"checked": checked, "query_errors": errors}

    async def get(self, task_id, context=None):
        binding = self.director.task_binding(task_id)
        if not binding:
            return None
        run_id, context_id = binding
        record = self.director.run_record(run_id)
        if record["closed"] and record["outcome_json"]:
            projection = json.loads(record["outcome_json"])
            if projection.get("state") in TERMINAL:
                await self.director._release_terminal_admission(
                    task_id, run_id, projection["state"])
            return self._task(task_id, context_id, record, projection)
        admission = self.director.admission_for_task(task_id)
        if self.director.admission_queue is not None:
            if admission is None:
                raise Rejected("factory Task has no durable admission request")
            if admission["state"] == "queued":
                return self._task(task_id, context_id, record, {
                    "state": "working", "status": {"admission_state": "queued"},
                    "result": None, "incident": None})
            if admission["state"] == "released":
                raise Rejected("released factory Task has no terminal outcome")
            if admission["state"] != "admitted":
                raise Rejected("factory Task has an invalid admission state")
            if not self.director.module.runner.is_running():
                await self.director._dispatch_admitted(admission)
        if not self.director.module.runner.is_running():
            return self._task(task_id, context_id, record, {"state": "working", "status": None,
                                                              "result": None, "incident": None})
        client = await self.director.client()
        handle = client.get_workflow_handle(run_id)
        description = await handle.describe()
        execution = description.status.name
        try:
            status = await handle.query(FactoryRun.status)
        except Exception:
            status = None
        result = await handle.result() if execution == "COMPLETED" else None
        state = project(status, execution, result)
        decision_status = status
        if state in TERMINAL and execution == "RUNNING":
            # Phase changes precede Workflow closure; publish only the closed outcome.
            state = "working"
        if state == "input-required" and (status or {}).get("phase") == "awaiting-child":
            # Only a Director wait needs caller input; a running child is still work.
            try:
                child = await client.get_workflow_handle(status["child_id"]).query(FactoryRun.status)
            except Exception:
                child = None
            if (child or {}).get("phase") not in {"awaiting-director", "awaiting-human"}:
                state = "working"
            else:
                decision_status = child
        elif state == "input-required" and (status or {}).get("phase") == "awaiting-human":
            decision_status = status
        incident = None
        if state == "failed":
            raw = (status or {}).get("incident") or (result or {}).get("incident") or {}
            failure_class = raw.get("failure_class") or (result or {}).get("kind") or execution
            incident = self.director.record_incident(failure_incident(
                run_id, raw.get("child_id") or (status or {}).get("child_id"),
                record["package_digest"], failure_class,
                raw.get("timestamp") or datetime.now(timezone.utc).isoformat(),
                (status or {}).get("authoritative_acceptance"),
                (status or {}).get("release_receipt")))
        projection = {"state": state, "status": status, "decision_status": decision_status,
                      "result": result, "incident": incident}
        if state in TERMINAL:
            self.director.close_run(run_id, projection)
            await self.director._release_terminal_admission(task_id, run_id, state)
        return self._task(task_id, context_id, record, projection)

    def _task(self, task_id: str, context_id: str, record: dict, projection: dict) -> Task:
        state, result, status = projection["state"], projection["result"], projection["status"]
        artifacts = None
        message = None
        supplier_assignment = self.director.supplier_assignment(record["run_id"])
        supplier_echo = (supplier_assignment or {}).get("echo")
        if state == "completed" and result and result.get("status") == "accepted":
            accepted = result.get("artifact") or {}
            content = accepted.get("content")
            if supplier_echo is not None:
                revision = accepted.get("revision")
                accepted_sha256 = accepted.get("sha256")
                if (not isinstance(content, str) or not content or
                        not isinstance(revision, str) or not revision or
                        not isinstance(accepted_sha256, str) or
                        not re.fullmatch(r"[0-9a-f]{64}", accepted_sha256) or
                        hashlib.sha256(content.encode("utf-8")).hexdigest() != accepted_sha256):
                    raise ValueError("accepted supplier artifact bytes do not match their digest")
                payload = {**supplier_echo, "revision": revision,
                           "sha256": accepted_sha256, "author": self.director.identity,
                           "content": content}
                artifacts = [Artifact(artifact_id=accepted_sha256,
                                      parts=[data_part(payload)])]
            else:
                report = None
                if isinstance(content, str):
                    try:
                        report = json.loads(content)
                    except ValueError:
                        pass
                if isinstance(report, dict) and report.get("kind") == "verified_report@1":
                    markdown = report.get("markdown")
                    if not isinstance(markdown, str) or not markdown:
                        raise ValueError("accepted report has no markdown")
                    payload = {"revision": accepted["revision"], "sha256": accepted["sha256"],
                               "packet_digest": report["packet_digest"],
                               "acceptance": result["acceptance"],
                               "release_receipt": result["receipt"]}
                    artifacts = [Artifact(artifact_id=accepted["sha256"], parts=[
                        text_part(markdown, "text/markdown"), data_part(payload)])]
                else:
                    # The legacy structured fixture path still returns its original data Part.
                    payload = {"capability": "verified-research@1", "status": result["status"],
                               "run_id": record["run_id"], "acceptance": result.get("acceptance"),
                               "release_receipt": result.get("receipt"), "report": accepted,
                               "interpreter_revision": result.get("interpreter_revision")}
                    artifact_id = hashlib.sha256(canonical(payload).encode()).hexdigest()
                    artifacts = [Artifact(artifact_id=artifact_id,
                                          parts=[data_part(payload)])]
        if projection.get("incident"):
            message = agent_message([data_part({"incident": projection["incident"]})])
        elif state == "input-required" and status:
            decision_status = projection.get("decision_status") or status
            applied = decision_status.get("applied_decisions") or {}
            safe_applied = {key: value for key, value in applied.items()
                            if isinstance(key, str) and isinstance(value, str) and value in {
                                "abort-recorded", "escalate-recorded"}}
            wait_view = {"phase": decision_status.get("phase"),
                         "child_id": status.get("child_id"),
                         "decision_actor": decision_status.get("decision_actor"),
                         "permitted_actions": [action for action in
                             decision_status.get("permitted_actions", [])
                             if isinstance(action, str) and action in {"abort", "escalate"}],
                         "deadline": decision_status.get("deadline"),
                         "applied_decisions": safe_applied}
            wait = {"director_wait": wait_view}
            message = agent_message([data_part(wait)])
        metadata = {
                        "run_id": record["run_id"], "harness_identity": self.director.identity,
                        "capability": "verified-research@1",
                        "publication_label": record["label"],
                        "manifest_digest": record["manifest_digest"],
                        "package_digest": record["package_digest"],
                        "interpreter_build": record["build_id"],
                        "run_inputs_digest": record["run_inputs_digest"]}
        if supplier_echo is not None:
            metadata.update(supplier_echo)
            metadata["agent_identity"] = self.director.identity
        # A2A Adapter boundary: the version-neutral projection state becomes TASK_STATE_*.
        return Task(id=task_id, context_id=context_id,
                    status=TaskStatus(state=task_state(state), message=message),
                    artifacts=artifacts, metadata=metadata)

    async def save(self, task, context=None):
        if self.director.task_binding(task.id) is None:
            raise Rejected("task has no durable factory binding")



class LoopbackSessionAdapter:
    """Short-lived local QA sessions; no bearer token or remote auth bypass."""

    lifetime_seconds = 8 * 60 * 60

    def __init__(self):
        self.sessions: dict[str, tuple[str, float]] = {}
        self.lock = threading.Lock()

    @staticmethod
    def _loopback_host(host: str | None) -> bool:
        if not host:
            return False
        try:
            hostname = urlsplit("//" + host).hostname
            if hostname == "localhost":
                return True
            return bool(hostname and ipaddress.ip_address(hostname).is_loopback)
        except ValueError:
            return False

    @classmethod
    def _local_request(cls, client_host: str | None, host: str | None) -> bool:
        try:
            peer_is_loopback = bool(client_host and ipaddress.ip_address(client_host).is_loopback)
        except ValueError:
            peer_is_loopback = False
        return peer_is_loopback and cls._loopback_host(host)

    @classmethod
    def _same_origin(cls, host: str | None, origin: str | None) -> bool:
        if not host or not origin or not cls._loopback_host(host):
            return False
        try:
            parsed = urlsplit(origin)
            return (parsed.scheme == "http" and parsed.netloc.casefold() == host.casefold()
                    and not parsed.username and not parsed.password)
        except ValueError:
            return False

    def issue(self, request: Request) -> str | None:
        host = request.headers.get("host")
        if (self._has_forwarding_headers(request.headers)
                or not self._local_request(request.client.host if request.client else None, host)
                or not self._same_origin(host, request.headers.get("origin"))):
            return None
        session_id = secrets.token_urlsafe(32)
        with self.lock:
            self.sessions[session_id] = (QA_ACTOR, time.monotonic() + self.lifetime_seconds)
        return session_id

    def resolve(self, *, session_id: str | None, client_host: str | None,
                host: str | None, origin: str | None = None,
                require_origin: bool = False, headers=None) -> str | None:
        if (self._has_forwarding_headers(headers)
                or not self._local_request(client_host, host)):
            return None
        if require_origin and not self._same_origin(host, origin):
            return None
        if origin is not None and not self._same_origin(host, origin):
            return None
        if not isinstance(session_id, str):
            return None
        with self.lock:
            entry = self.sessions.get(session_id)
            if entry is None:
                return None
            actor, expiry = entry
            if time.monotonic() >= expiry:
                self.sessions.pop(session_id, None)
                return None
            return actor

    def resolve_request(self, request: Request) -> str | None:
        return self.resolve(session_id=request.cookies.get(QA_SESSION_COOKIE),
            client_host=request.client.host if request.client else None,
            host=request.headers.get("host"), origin=request.headers.get("origin"),
            require_origin=request.method not in {"GET", "HEAD", "OPTIONS"},
            headers=request.headers)

    def resolve_websocket(self, websocket) -> str | None:
        return self.resolve(session_id=websocket.cookies.get(QA_SESSION_COOKIE),
            client_host=websocket.client.host if websocket.client else None,
            host=websocket.headers.get("host"), origin=websocket.headers.get("origin"),
            require_origin=True, headers=websocket.headers)

    @staticmethod
    def _has_forwarding_headers(headers) -> bool:
        if headers is None:
            return False
        return any(key.lower() == "forwarded" or key.lower().startswith("x-forwarded-")
                   for key in headers.keys())


def _auth(app, sessions: LoopbackSessionAdapter | None = None):
    if sessions is None:
        @app.middleware("http")
        async def fixture_bearer_auth(request, call_next):
            if (request.url.path in {"/health", "/.well-known/agent-card.json", "/floor"}
                    or request.url.path.startswith(("/assets/", "/dashboard-assets/"))):
                return await call_next(request)
            bearer = request.headers.get("authorization")
            if bearer not in {TOKEN, OBSERVER_TOKEN}:
                return JSONResponse({"error": "fixture authentication required"}, status_code=401)
            token = CURRENT_ACTOR.set(TOKEN_ACTOR if bearer == TOKEN else OBSERVER_ACTOR)
            try:
                return await call_next(request)
            finally:
                CURRENT_ACTOR.reset(token)
        return

    @app.middleware("http")
    async def loopback_session_auth(request, call_next):
        if (request.url.path in {"/health", "/.well-known/agent-card.json", "/floor",
                                 "/qa/login", "/qa/session"}
                or request.url.path.startswith(("/assets/", "/dashboard-assets/"))):
            return await call_next(request)
        actor = sessions.resolve_request(request)
        if actor is None:
            return JSONResponse({"error": "loopback QA session required"}, status_code=401)
        token = CURRENT_ACTOR.set(actor)
        try:
            return await call_next(request)
        finally:
            CURRENT_ACTOR.reset(token)


def _verified_active_publication_context(director: Director,
                                         factory_id: str) -> dict[str, str]:
    """Read both Engineering pins from one verified active publication closure."""
    if factory_id != director.identity:
        raise PermissionError("factory is outside this publication authority")
    try:
        publication = director.module.publications.active()
        if not isinstance(publication, Mapping):
            raise ValueError("active publication record is malformed")
        package_digest = publication.get("package_digest")
        build_id = publication.get("build_id")
        closure = publication.get("closure")
        if (not isinstance(package_digest, str) or not package_digest
                or not isinstance(build_id, str) or not build_id
                or not isinstance(closure, Mapping)):
            raise ValueError("active publication record is incomplete")
        package = director.module.package(package_digest)
        if not isinstance(package, Mapping):
            raise ValueError("active publication package is unavailable")
        root = package.get("root")
        if not isinstance(root, Mapping):
            raise ValueError("active publication root is unavailable")
        manifest = closure.get("manifest")
        if not isinstance(manifest, Mapping):
            raise ValueError("active publication manifest is unavailable")
        verified_manifest_digest = verify_closure(
            closure, package, build_id=build_id,
            definition_digest=digest(root), document=root)
        quality_policy_digest = manifest.get("quality_policy_digest")
        if (publication.get("manifest_digest") != verified_manifest_digest
                or manifest.get("package_digest") != package_digest
                or manifest.get("interpreter", {}).get("build_id") != build_id
                or not isinstance(quality_policy_digest, str)):
            raise ValueError("active publication envelope does not match its closure")
        return {"manifest_digest": verified_manifest_digest,
                "quality_policy_digest": quality_policy_digest}
    except PermissionError:
        raise
    except Exception as error:
        # A damaged pointer, publication, package, or closure has no usable
        # Engineering context. Do not fall back to split or cached values.
        raise LookupError("verified active publication context is unavailable") from error


def create_app(instance_dir: Path, *, commercial_reader=None, usage_broker=None):
    config = load_config(instance_dir)
    port = config["port"]
    if config["mode"] == "agent":
        # Ordinary agent mode: the same harness serves one capability directly.
        return harness_server.create_app(instance_dir / "agent-state",
                                         config.get("agent_role", "capability"), port)
    director = Director(instance_dir, config)
    read_submission_readiness, submission_preflight = _submission_readiness_callbacks(
        director, config)
    # The basic single-job gate invokes this synchronously before selecting a
    # model or doing any provider work. It is intentionally independent of the
    # observation/UI readiness projection below.
    director.submission_preflight = submission_preflight
    director.submission_readiness = read_submission_readiness
    loopback_qa_session = config.get("loopback_qa_session") is True
    sessions = LoopbackSessionAdapter() if loopback_qa_session else None
    dashboard_asset_version = (_dashboard_js_content_digest(DASHBOARD_ASSETS)
                               if loopback_qa_session else None)
    store = FactoryTaskStore(director)
    capability = config["capability"]
    nested_supplier_enabled = director.nested_supplier_enabled
    card_extensions = []
    supplier_contract = None
    if nested_supplier_enabled:
        supplier_contract = {
            "name": A2A_ACTION_CONTRACT,
            "protocol": a2a_v1.PROTOCOL,
            "request": {"method": a2a_v1.SEND_MESSAGE, "returnImmediately": True,
                        "data": {"op": "nested_factory", "fields": sorted((
                            "action_id", "run_id", "definition_digest", "parent_task_id",
                            "parent_run_id", "parent_definition_digest", "parent_assignment_id",
                            "parent_attempt_id", "assignment_id", "attempt_id", "payload"))}},
            "response": {"result": "task", "metadata": sorted((
                "action_id", "run_id", "definition_digest", "agent_identity",
                "parent_task_id", "parent_run_id", "parent_definition_digest",
                "parent_assignment_id", "parent_attempt_id", "assignment_id", "attempt_id"))},
            "completion": {"method": a2a_v1.GET_TASK, "state": a2a_v1.wire_state("completed"),
                           "artifact_count": 1, "data": sorted((
                               "revision", "sha256", "author", "content", "action_id",
                               "run_id", "definition_digest", "parent_task_id",
                               "parent_run_id", "parent_definition_digest",
                               "parent_assignment_id", "parent_attempt_id",
                               "assignment_id", "attempt_id"))},
            "supplier_assignment_echo": supplier_assignment_echo_declaration(),
            "reconcile": "opaque",
        }
        card_extensions = [AgentExtension(
            uri=A2A_ACTION_EXTENSION_URI, required=True,
            params={"identity": director.identity, "contract": A2A_ACTION_CONTRACT,
                    "contract_digest": agent_contract_digest(supplier_contract)})]
    card = AgentCard(
        name=config["name"], description=capability["description"],
        supported_interfaces=interfaces(f"http://127.0.0.1:{port}/"), version="0.1.0",
        default_input_modes=["application/json"], default_output_modes=["application/json"],
        capabilities=(AgentCapabilities(streaming=False, extensions=card_extensions)
                      if nested_supplier_enabled else AgentCapabilities(streaming=False)),
        skills=[AgentSkill(id=capability["id"], name=capability["name"],
                           description=capability["description"], tags=capability.get("tags", []))],
        **(cookie_security("loopbackSession", QA_SESSION_COOKIE) if loopback_qa_session
           else bearer_security()))
    app = build_app(card, LegacyRequestHandler(HarnessExecutor(
        director, store, allow_structured_commands=(
            config.get("legacy_structured_commands") is True or nested_supplier_enabled)
    ), store, card))
    _auth(app, sessions)
    if nested_supplier_enabled:
        @app.get("/contract")
        def supplier_contract_document():
            return supplier_contract
    if commercial_reader is None:
        # Factory instances share one local ledger at EXO_HOME. The ledger
        # constructor initializes empty accounting tables only; no commercial
        # terms, budgets, authority, purchases, or settlements are seeded.
        exo_home = Path(instance_dir).resolve().parents[1]
        commercial_reader = CommercialLedger(exo_home / "commercial.sqlite3")
    if usage_broker is None:
        configured_model_home = Path(os.environ.get("EXO_MODEL_HOME", str(DEFAULT_HOME))).expanduser()
        usage_broker = ModelBroker(home=configured_model_home)
    operations_holder = {}
    operations_reader = None
    if "operations_max_list_limit" in config:
        def read_public_incidents(principal, *, factory_id: str, limit: int):
            if factory_id != director.identity:
                raise PermissionError("factory is outside this Operations instance")
            operations = operations_holder.get("operations")
            if operations is None:
                raise RuntimeError("Operations reader is not mounted")
            return project_operations_incidents(
                operations, principal, factory_id, limit=limit)
        operations_reader = read_public_incidents
    observation_source = RuntimeObservationSource(
        director, config, instance_dir / "runtime-observation-source.sqlite3",
        commercial_reader=commercial_reader,
        operations_reader=operations_reader)
    observation = FactoryObservation(observation_source,
                                     instance_dir / "factory-observation.sqlite3")

    def authenticate_observer(websocket):
        # The caller cannot choose an actor in an Observation frame. Existing
        # installations retain their established fixture bearer principals.
        if sessions is not None:
            return sessions.resolve_websocket(websocket)
        bearer = websocket.headers.get("authorization")
        return {TOKEN: TOKEN_ACTOR, OBSERVER_TOKEN: OBSERVER_ACTOR}.get(bearer)

    install_observation_transport(app, observation, authenticate_observer)
    operations = None
    if "operations_max_list_limit" in config:
        def authenticate_operations(request: Request):
            # _auth resolves the bearer/session through the existing server-side
            # authority path before route handling; never take identity from input.
            return CURRENT_ACTOR.get()

        def authorize_operations(principal, factory_id: str, capability_name: str,
                                 resource_id: str) -> str:
            if factory_id != director.identity or not isinstance(resource_id, str) or not resource_id:
                raise PermissionError("Operations resource is outside this factory")
            fixture_capabilities = {
                "maintenance.incident.report",
                "maintenance.incident.read",
                "maintenance.incident.acknowledge",
                "maintenance.incident.claim",
                "maintenance.incident.claim.takeover",
                "maintenance.incident.claim.release",
                "maintenance.incident.escalate",
            }
            if principal == OBSERVER_ACTOR and capability_name == "maintenance.incident.read":
                return OBSERVER_ACTOR
            if principal == TOKEN_ACTOR and capability_name in fixture_capabilities:
                return TOKEN_ACTOR
            raise PermissionError("principal lacks the fixture Operations capability")

        operations = install_runtime_operations(
            app, observation=observation,
            database=instance_dir / "operations.sqlite3",
            factory_id=director.identity,
            authenticate=authenticate_operations,
            authorize_principal=authorize_operations,
            max_list_limit=config["operations_max_list_limit"],
            publication_context_reader=lambda requested_factory_id:
                _verified_active_publication_context(director, requested_factory_id))
        operations_holder["operations"] = operations
    delivery_reader = install_local_delivery_routes(app, observation=observation,
        instance_dir=instance_dir, factory_id=director.identity,
        principal=lambda: CURRENT_ACTOR.get(),
        configuration=config.get("local_delivery"))
    # The Observation source consumes only LocalDelivery's public receipt
    # reader; it never opens that ledger or its destination directly.
    observation_source.delivery_reader = delivery_reader
    if dashboard_asset_version is not None:
        app.mount(f"/dashboard-assets/{dashboard_asset_version}",
                  StaticFiles(directory=DASHBOARD_ASSETS), name="dashboard-assets-versioned")
    app.mount("/dashboard-assets", StaticFiles(directory=DASHBOARD_ASSETS),
              name="dashboard-assets")
    startup = {"runner_started_at_startup": False, "recovery": None}
    recovery_complete = asyncio.Event()
    admission_reconciler = {
        "status": ("unconfigured" if director.admission_queue is None and
                   not director.basic_single_active_job else "pending"),
        "checked": 0, "query_errors": 0, "last_error": None,
        "task": None, "stop": None,
    }
    operations_status = ("unconfigured" if operations is None else "incident_only")

    @app.on_event("startup")
    async def recover_unfinished():
        loop = asyncio.get_running_loop()

        def work():
            try:
                startup["recovery"] = director.recover()
                startup["runner_started_at_startup"] = startup["recovery"] is not None
            except Exception as error:  # surfaced through /health
                startup["recovery"] = {"error": repr(error)}
            finally:
                loop.call_soon_threadsafe(recovery_complete.set)
        threading.Thread(target=work, daemon=True).start()

    @app.on_event("startup")
    async def start_admission_reconciler():
        if director.admission_queue is None and not director.basic_single_active_job:
            return
        stop = asyncio.Event()
        admission_reconciler["stop"] = stop
        admission_reconciler["status"] = "running"

        async def reconcile_loop():
            await recovery_complete.wait()
            interval = observation_source.refresh_interval_seconds
            while not stop.is_set():
                try:
                    if director.admission_queue is not None:
                        result = await store.reconcile_admitted()
                        admission_reconciler["checked"] += result["checked"]
                        admission_reconciler["query_errors"] = result["query_errors"]
                    if director.basic_single_active_job:
                        await director._basic_refresh_slot()
                        if director.admission_queue is None:
                            admission_reconciler["checked"] += 1
                    admission_reconciler["last_error"] = None
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    admission_reconciler["last_error"] = type(error).__name__
                try:
                    await asyncio.wait_for(stop.wait(), timeout=interval)
                except asyncio.TimeoutError:
                    pass

        admission_reconciler["task"] = asyncio.create_task(
            reconcile_loop(), name=f"admission-reconciler-{director.identity}")

    @app.on_event("shutdown")
    async def stop_admission_reconciler():
        task = admission_reconciler.get("task")
        stop = admission_reconciler.get("stop")
        if stop is not None:
            stop.set()
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if director.admission_queue is not None:
            admission_reconciler["status"] = "stopped"

    @app.get("/submission/readiness")
    def current_submission_readiness(request: Request):
        headers = {"Cache-Control": "no-store"}
        if request.query_params:
            return JSONResponse({"error": "submission readiness accepts no selectors"},
                                status_code=400, headers=headers)
        actor = CURRENT_ACTOR.get()
        if actor != TOKEN_ACTOR:
            return JSONResponse({"error": "submission principal is not authorized"},
                                status_code=403, headers=headers)
        try:
            owned = observation.discover(actor)
            if not any((item.get("factory_id") or item.get("id")) == director.identity
                       for item in owned):
                return JSONResponse({"error": "factory is not authorized"},
                                    status_code=403, headers=headers)
            value = read_submission_readiness(refresh=True)
            slot = director.basic_job_status()
            unfinished = director.unfinished()
            if not isinstance(unfinished, list):
                raise ValueError("current run state unavailable")
            reasons = {"director_profile_unapproved", "default_broker_path_overridden",
                       "subscription_status_unavailable", "pinned_writable_model_owners_unavailable",
                       "pinned_worker_pollers_unavailable"}
            if slot.get("configured") is not True:
                reason = "current_state_unavailable"
            elif slot.get("state") == "uncertain":
                reason = "factory_uncertain"
            elif slot.get("busy") is True:
                reason = "factory_busy"
            elif slot.get("busy") is not False or slot.get("state") != "available":
                reason = "current_state_unavailable"
            elif unfinished:
                reason = "unfinished_runs"
            elif value.get("submission_ready") is not True:
                reason = next((item for item in value.get("submission_blockers", []) if item in reasons),
                              "current_state_unavailable")
            else:
                reason = None
        except Exception:
            reason = "current_state_unavailable"
        return JSONResponse({"schema_version": 1, "factory_id": director.identity,
                             "observed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                             "status": "ready" if reason is None else "blocked",
                             "reason_code": reason}, headers=headers)

    @app.get("/health")
    def health():
        readiness = read_submission_readiness()
        admission_status = ({"status": "unconfigured"} if director.admission_queue is None
                            else {"status": "configured",
                                  **director.admission_queue.capacity_view()})
        return {"identity": director.identity, "incarnation": director.incarnation,
                "role": "factory-harness", "name": config["name"],
                "capability": capability["id"], "a2a_protocol": a2a_v1.PROTOCOL_VERSION,
                "runner_running": director.module.runner.is_running(),
                "unfinished_runs": len(director.unfinished()), "startup": startup,
                "admission": admission_status,
                "basic_job": director.basic_job_status(),
                "admission_reconciler": {
                    key: ("unconfigured" if key == "status" and
                          director.admission_queue is None else admission_reconciler[key])
                    for key in ("status", "checked", "query_errors", "last_error")},
                "basic_terminal_reconciler": {
                    "status": ("running" if director.basic_single_active_job and
                               admission_reconciler["status"] == "running" else
                               "unconfigured" if not director.basic_single_active_job else
                               admission_reconciler["status"]),
                    "checked": admission_reconciler["checked"],
                    "query_errors": admission_reconciler["query_errors"],
                    "last_error": admission_reconciler["last_error"]},
                "operations_incidents": {
                    "status": operations_status,
                    "max_list_limit": config.get("operations_max_list_limit"),
                    "recovery_available": False,
                },
                "readiness": {
                    **readiness,
                    "temporal_runtime_configured": bool(os.getenv("EXO_TEMPORAL_RUNTIME")),
                    "temporal_cli_configured": bool(os.getenv("EXO_TEMPORAL_CLI")),
                    "observation_auth_status": ("loopback_only_session" if loopback_qa_session
                                                 else "fixture_only"),
                    "browser_auth_resolver_status": ("loopback_session_adapter"
                        if loopback_qa_session else "not_selected"),
                    "live_browser_authenticated": False,
                    "commercial_ledger_status": observation_source.commercial_ledger_status,
                    "commercial_reporting_status": observation_source.commercial_reporting_status,
                    "commercial_costs_known": observation_source.commercial_costs_known,
                    "assignment_ledger_writer_status": "not_enabled",
                    "assignment_ledger_writer_reason": (
                        "assignment offer, spend authorization, and provider-usage evidence "
                        "are not configured"),
                    "payment_adapter_status": "unconfigured",
                    "admission_status": admission_status["status"],
                    "operations_incident_api_status": operations_status,
                }}

    if loopback_qa_session:
        @app.get("/qa/login")
        def qa_login(request: Request):
            if not sessions._local_request(request.client.host if request.client else None,
                                           request.headers.get("host")):
                return JSONResponse({"error": "loopback QA session is local-only"},
                                    status_code=403, headers={"Cache-Control": "no-store"})
            return HTMLResponse("""<!doctype html><html><head><meta charset="utf-8">
<title>Loopback dashboard QA</title></head><body>
<main><h1>Loopback dashboard QA</h1><p>This creates an eight-hour local session
for the fixture operator. It is accepted only over loopback.</p>
<form method="post" action="/qa/session"><button type="submit">Start local QA session</button></form>
</main></body></html>""", headers={"Cache-Control": "no-store"})

        @app.post("/qa/session")
        def qa_session(request: Request):
            session_id = sessions.issue(request)
            if session_id is None:
                return JSONResponse({"error": "loopback same-origin request required"},
                                    status_code=403, headers={"Cache-Control": "no-store"})
            response = RedirectResponse("/floor", status_code=303,
                                        headers={"Cache-Control": "no-store"})
            response.set_cookie(QA_SESSION_COOKIE, session_id, max_age=sessions.lifetime_seconds,
                                httponly=True, secure=False, samesite="strict", path="/")
            return response

        @app.get("/floor")
        def floor(request: Request):
            client_host = request.client.host if request.client else None
            host = request.headers.get("host")
            if (sessions._has_forwarding_headers(request.headers)
                    or not sessions._local_request(client_host, host)):
                return JSONResponse({"error": "loopback QA page is local-only"},
                                    status_code=403, headers={"Cache-Control": "no-store"})
            actor = sessions.resolve_request(request)
            if actor != QA_ACTOR:
                return RedirectResponse("/qa/login", status_code=303,
                                        headers={"Cache-Control": "no-store"})
            bootstrap = {
                "source": "live", "authorizedSession": actor == QA_ACTOR,
                "discoveryEndpoint": "/discover", "a2aMessageSendEndpoint": "/",
                "usageEndpoint": "/usage/measurements", "deliveryEndpoint": "/deliveries",
                "qaSessionLogin": "/qa/login",
                "submissionReadinessEndpoint": "/submission/readiness",
            }
            encoded = json.dumps(bootstrap, separators=(",", ":"))
            script = f"""<script>
window.EXO_DASHBOARD_BOOTSTRAP={encoded};
window.EXO_DASHBOARD_BOOTSTRAP.observationEndpoint=
  (location.protocol==='https:'?'wss:':'ws:')+'//'+location.host+'/observations';
window.EXO_DASHBOARD_BOOTSTRAP.artifactEndpoint=({{run_id,revision,sha256}})=>
  '/artifacts/'+encodeURIComponent(run_id)+'/'+encodeURIComponent(revision)+'/'+encodeURIComponent(sha256);
</script>"""
            html = FLOOR_PAGE.read_text()
            html = html.replace("/dashboard-assets/",
                                f"/dashboard-assets/{dashboard_asset_version}/")
            html = html.replace("</head>", script + "</head>", 1)
            return HTMLResponse(html, headers={"Cache-Control": "no-store"})
    else:
        @app.get("/floor")
        def floor():
            return FileResponse(FLOOR_PAGE, media_type="text/html")

    @app.get("/discover")
    def discover():
        factories = []
        for item in observation.discover(CURRENT_ACTOR.get()):
            factory_id = item.get("id") or item.get("factory_id")
            name = item.get("name")
            if not isinstance(factory_id, str) or not isinstance(name, str):
                continue
            factories.append({"id": factory_id, "name": name,
                              "factory_id": item.get("factory_id", factory_id)})
        return {"schema_version": 1, "factories": factories}

    @app.get("/usage/measurements")
    def usage_measurements(run_id: str | None = None, task_id: str | None = None,
                           assignment_id: str | None = None, attempt_id: str | None = None,
                           model_call_id: str | None = None, call_scope: str | None = None):
        """Read safe token measurements this factory recorded or received."""
        filters = {"run_id": run_id, "task_id": task_id,
                   "assignment_id": assignment_id, "attempt_id": attempt_id,
                   "model_call_id": model_call_id, "call_scope": call_scope}
        if any(value is not None and (not value or len(value) > 256)
               for value in filters.values()):
            return JSONResponse({"error": "invalid measurement filter"}, status_code=400)
        if call_scope is not None and call_scope not in USAGE_CALL_SCOPES:
            return JSONResponse({"error": "invalid measurement filter"}, status_code=400)
        principal = CURRENT_ACTOR.get()
        try:
            snapshot = observation.snapshot(principal, director.identity)
        except Exception:
            return JSONResponse({"error": "measurement scope unavailable",
                "measurements": [], "coverage": {"status": "unavailable",
                    "reason": "factory observation source unavailable",
                    "authoring_unbound": "unavailable",
                    "director": "unavailable_observation_scope_unavailable",
                    "director_unbound": "unavailable_observation_scope_unavailable"}},
                status_code=503)
        scopes = _usage_scopes(snapshot, director, filters)
        try:
            journal = usage_broker.usage_journal()
            authoring_rows, authoring_report = _authoring_measurements(
                journal, scopes, filters)
        except Exception:
            authoring_rows, authoring_report = [], {"queries_failed": 1,
                "rows_rejected": 0, "conflicts": 0}
        try:
            director_rows, director_report = _director_measurements(
                director, scopes, filters)
        except Exception:
            director_rows, director_report = [], {"queries_failed": 1,
                "rows_rejected": 0, "conflicts": 0}
        service_rows, service_report = _pinned_service_measurements(director, scopes, filters)
        rows: dict[str, dict] = {}
        conflicts: set[str] = set()
        for value in (*authoring_rows, *director_rows, *service_rows):
            _merge_measurement(rows, conflicts, value)
        measurements = sorted(rows.values(),
            key=lambda item: (item["recorded_at"], item["model_call_id"]))
        authoring_partial = bool(authoring_report["queries_failed"] or
                                 authoring_report["rows_rejected"] or
                                 authoring_report["conflicts"])
        # Task-scoped pre-run Director measurements are intentionally included
        # with their durable null run/definition bindings. Mark aggregate
        # coverage partial when they are present so the UI does not imply a
        # complete run binding for those calls. Rows without an authorized
        # original Task remain unavailable in director_unbound below.
        director_coverage = _director_coverage_status(director_report, director_rows)
        service_partial = bool(service_report["owner_resolution_failures"] or
                               service_report["queries_failed"] or
                               service_report["rows_rejected"] or service_report["conflicts"])
        partial = bool(authoring_partial or director_coverage == "partial" or service_partial or
                       conflicts)
        return {"measurements": measurements, "coverage": {
            "status": "partial",
            "factory_id": director.identity,
            "scoped_run_count": len(scopes),
            "bound_sources_status": "partial" if partial else "available",
            "authoring_bound": {"status": "partial" if authoring_partial else "available",
                                **authoring_report},
            "authoring_unbound": {
                "status": "unavailable",
                "reason": "shared_model_home_factory_exclusivity_not_proven"},
            "pinned_services": {"status": "agent_reported",
                "availability": "partial" if service_partial else "available",
                "source": "factory_journal", **service_report},
            "director": {"status": director_coverage,
                **director_report},
            "director_unbound": {"status": "unavailable",
                "reason": "pre_task_or_unbound_director_rows_have_no_authoritative_factory_run"},
            "commercial_costs": "not_included"}}

    @app.get("/artifacts/{run_id}/{revision}/{sha256}")
    def inspect_artifact(run_id: str, revision: str, sha256: str):
        actor = CURRENT_ACTOR.get()
        artifact = observation.inspect_artifact(actor, director.identity, run_id,
                                                revision, sha256)
        markdown = accepted_markdown({"revision": revision, "sha256": sha256,
                                      "content": artifact["content"].decode("utf-8")},
                                     revision=revision, sha256=sha256)
        return Response(content=artifact["content"], media_type=artifact["media_type"],
                        headers={"ETag": f'"sha256:{sha256}"',
                                 "X-Markdown-SHA256": markdown.markdown_sha256,
                                 "X-Markdown-SHA256-Matches-Accepted":
                                     str(markdown.markdown_sha256 == sha256).lower(),
                                 "X-Report-Revision": revision})

    @app.get("/artifacts/{run_id}/{revision}/{sha256}/markdown")
    def inspect_markdown(run_id: str, revision: str, sha256: str):
        actor = CURRENT_ACTOR.get()
        artifact = observation.inspect_artifact(actor, director.identity, run_id,
                                                revision, sha256)
        delivered = accepted_markdown({"revision": revision, "sha256": sha256,
                                       "content": artifact["content"].decode("utf-8")},
                                      revision=revision, sha256=sha256)
        return Response(content=delivered.content, media_type=delivered.media_type,
                        headers={"ETag": f'"sha256:{delivered.markdown_sha256}"',
                                 "X-Markdown-SHA256": delivered.markdown_sha256,
                                 "X-Accepted-SHA256": delivered.accepted_sha256,
                                 "X-Accepted-Content-SHA256": delivered.accepted_content_sha256,
                                 "X-Markdown-SHA256-Matches-Accepted":
                                     str(delivered.markdown_sha256 == delivered.accepted_sha256).lower(),
                                 "X-Report-Revision": revision})

    return app


def init_instance(instance_dir: Path, *, name: str, mode: str, port: int, home: Path,
                  runner: dict | None = None, wait_seconds: int = 900,
                  legacy_structured_commands: bool = False,
                  admission_capacity: int | None = None,
                  operations_max_list_limit: int | None = None,
                  nested_supplier_enabled: bool | None = None) -> dict:
    import fcntl
    import os

    home = home.resolve()
    instance_dir = instance_dir.resolve()
    instances = home / "instances"
    instances.mkdir(parents=True, exist_ok=True)
    with (instances / "provision.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        config = {"name": name, "mode": mode, "port": port, "home": str(home),
                  "runner": {}, "director_wait_seconds": wait_seconds,
                  "capability": {"id": "verified-research@1", "name": "Verified research",
                                 "description": "Researches a question with independent counter-evidence "
                                                "review; returns one accepted report and release receipt.",
                                 "tags": ["research", "verified"]}}
        if admission_capacity is not None:
            if type(admission_capacity) is not int or admission_capacity < 0:
                raise ValueError("admission_capacity must be an explicit non-negative integer")
            if mode != "factory":
                raise ValueError("admission_capacity is available only in factory mode")
            config["admission_capacity"] = admission_capacity
        if operations_max_list_limit is not None:
            if type(operations_max_list_limit) is not int or not 1 <= operations_max_list_limit <= 256:
                raise ValueError("operations_max_list_limit must be an explicit integer from 1 to 256")
            if mode != "factory":
                raise ValueError("Operations routes are available only in factory mode")
            config["operations_max_list_limit"] = operations_max_list_limit
        if nested_supplier_enabled is not None:
            if type(nested_supplier_enabled) is not bool:
                raise ValueError("nested_supplier_enabled must be an explicit boolean")
            if nested_supplier_enabled and mode != "factory":
                raise ValueError("nested suppliers are available only in factory mode")
            config["nested_supplier_enabled"] = nested_supplier_enabled
        if legacy_structured_commands:
            if mode != "factory":
                raise ValueError("legacy structured commands require factory mode")
            config["legacy_structured_commands"] = True
        path = instance_dir / "instance.json"
        if path.exists():
            existing = json.loads(path.read_text())
            if existing != config:
                raise FileExistsError("instance already configured differently")
        for other in instances.glob("*/instance.json"):
            if other.parent.resolve() != instance_dir and json.loads(other.read_text()).get("port") == port:
                raise ValueError(f"harness port {port} already configured for {other.parent}")
        if mode == "factory":
            requested = runner or {}
            Runner(home, port_base=requested.get("port_base", os.getenv("EXO_RUNNER_PORT_BASE")),
                   member_base=requested.get("member_base", os.getenv("EXO_RUNNER_MEMBER_BASE")))
        if path.exists():
            return existing
        instance_dir.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n")
        return config


if __name__ == "__main__":
    import fcntl

    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["serve"])
    parser.add_argument("--instance-dir", type=Path, required=True)
    parser.add_argument("--admission-capacity", type=int)
    parser.add_argument("--operations-max-list-limit", type=int)
    args = parser.parse_args()
    if args.admission_capacity is not None:
        configure_admission_capacity(args.instance_dir, args.admission_capacity)
    if args.operations_max_list_limit is not None:
        configure_operations_max_list_limit(args.instance_dir, args.operations_max_list_limit)
    config = load_config(args.instance_dir)
    with (args.instance_dir / "harness.lock").open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError(f"instance already serving: {args.instance_dir}") from error
        uvicorn.run(create_app(args.instance_dir), host="127.0.0.1", port=config["port"],
                    proxy_headers=config.get("loopback_qa_session") is not True,
                    log_level="warning")
