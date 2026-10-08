"""Safe, durable projection for the factory dashboard observation Interface.

The runtime supplies an ObservationSource that reads authoritative Task,
publication, Temporal, outcome-journal, artifact, and commercial records. This
module does not query those systems or infer transitions from polled status; it
deduplicates and projects source records that the adapter has already verified.
"""
from __future__ import annotations

import hashlib
import json
import re
import secrets
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Protocol
from urllib.parse import quote


SCHEMA_VERSION = 1
EVENT_DATA_SCHEMA = "urn:exomachina:dashboard:event:v1"
SOURCE_KINDS = frozenset({"task", "publication", "temporal", "outcome",
                          "artifact", "commercial", "capacity", "admission",
                          "command", "incident"})

# These fields are shared by event data, but are selected from the normalized
# source record explicitly. Raw source objects never cross this boundary.
COMMON_FIELDS = frozenset({
    "factory_id", "run_id", "task_id", "context_id", "assignment_id", "attempt_id",
    "manifest_digest", "package_digest", "definition_digest", "interpreter_build",
})

# Event-specific fields are deliberately finite. Free-form prompts, messages,
# raw Temporal status, credentials, URLs, artifact contents, and mandate data
# are not representable in this map.
EVENT_FIELDS: dict[str, frozenset[str]] = {
    "com.exomachina.factory.discovered.v1": frozenset({"name", "identity", "capability", "graph_nodes"}),
    "com.exomachina.publication.activated.v1": frozenset({
        "publication_version", "graph_nodes", "service_bindings"}),
    "com.exomachina.run.created.v1": frozenset({
        "state", "phase", "started_at", "graph_nodes"}),
    "com.exomachina.run.state_changed.v1": frozenset({
        "state", "phase", "node", "repair_count", "max_repairs", "started_at",
        "ended_at", "wait_deadline", "wait_role", "wait_actor_identity",
        "wait_started_at", "permitted_actions"}),
    "com.exomachina.assignment.state_changed.v1": frozenset({
        "node", "capability", "provider_identity", "state", "started_at", "ended_at",
        "queue_position"}),
    "com.exomachina.artifact.revised.v1": frozenset({
        "artifact_revision", "artifact_sha256", "previous_revision", "previous_sha256",
        "media_type", "byte_length", "author_identity"}),
    "com.exomachina.quality.verdict.v1": frozenset({
        "artifact_revision", "artifact_sha256", "reviewer_identity", "accepted",
        "finding_count", "finding_codes"}),
    "com.exomachina.decision.outcome.v1": frozenset({
        "decision_id", "action", "outcome", "expected_state", "resulting_state",
        "artifact_revision", "artifact_sha256", "actor_identity"}),
    "com.exomachina.command.outcome.v1": frozenset({
        "command_id", "lifecycle", "outcome", "expected_state", "resulting_state",
        "artifact_revision", "artifact_sha256"}),
    "com.exomachina.delivery.receipt.v1": frozenset({
        "receipt_id", "artifact_revision", "artifact_sha256", "destination_id",
        "markdown_sha256", "destination_identity", "delivery_kind", "byte_length",
        "delivered_at", "outcome"}),
    "com.exomachina.incident.state_changed.v1": frozenset({
        "incident_id", "kind", "state", "owner_identity", "evidence_refs"}),
    "com.exomachina.admission.state_changed.v1": frozenset({
        "admission_id", "state", "queue_position", "capacity_limit"}),
    "com.exomachina.capacity.state_changed.v1": frozenset({
        "capacity_limit", "active_count", "queued_count"}),
    "com.exomachina.commercial.usage.v1": frozenset({
        "usage_id", "service_identity", "model_call_id", "unit", "quantity",
        "measurement_source", "completeness", "evidence_status", "model_id",
        "reasoning_effort", "evidence_refs"}),
    "com.exomachina.commercial.obligation.v1": frozenset({
        "obligation_id", "offer_digest", "component", "amount_atoms", "currency",
        "atomic_scale", "evidence_status", "price_basis", "payment_trigger",
        "markup_bps", "state", "evidence_refs"}),
    "com.exomachina.commercial.payment.v1": frozenset({
        "payment_id", "network", "asset", "amount_atoms", "currency", "atomic_scale",
        "state", "receipt_id", "evidence_status", "evidence_refs"}),
}
EVENT_REQUIRED: dict[str, frozenset[str]] = {
    "com.exomachina.factory.discovered.v1": frozenset({"identity", "capability", "name"}),
    "com.exomachina.publication.activated.v1": frozenset({
        "manifest_digest", "package_digest", "definition_digest", "interpreter_build", "graph_nodes"}),
    "com.exomachina.run.created.v1": frozenset({
        "run_id", "task_id", "context_id", "manifest_digest", "package_digest",
        "definition_digest", "interpreter_build", "state"}),
    "com.exomachina.run.state_changed.v1": frozenset({"run_id", "state", "phase"}),
    "com.exomachina.assignment.state_changed.v1": frozenset({
        "run_id", "task_id", "assignment_id", "attempt_id", "capability", "state"}),
    "com.exomachina.artifact.revised.v1": frozenset({
        "run_id", "artifact_revision", "artifact_sha256"}),
    "com.exomachina.quality.verdict.v1": frozenset({
        "run_id", "task_id", "artifact_revision", "artifact_sha256", "reviewer_identity",
        "accepted", "finding_count"}),
    "com.exomachina.decision.outcome.v1": frozenset({
        "run_id", "decision_id", "action", "outcome"}),
    "com.exomachina.command.outcome.v1": frozenset({"run_id", "command_id", "lifecycle"}),
    "com.exomachina.delivery.receipt.v1": frozenset({
        "run_id", "receipt_id", "artifact_revision", "artifact_sha256", "outcome"}),
    "com.exomachina.incident.state_changed.v1": frozenset({"run_id", "incident_id", "kind", "state"}),
    "com.exomachina.admission.state_changed.v1": frozenset({
        "run_id", "admission_id", "state", "capacity_limit"}),
    "com.exomachina.capacity.state_changed.v1": frozenset({
        "capacity_limit", "active_count", "queued_count"}),
    "com.exomachina.commercial.usage.v1": frozenset({
        "run_id", "assignment_id", "attempt_id", "usage_id", "service_identity", "unit",
        "measurement_source", "completeness", "evidence_status"}),
    "com.exomachina.commercial.obligation.v1": frozenset({
        "run_id", "assignment_id", "attempt_id", "obligation_id", "offer_digest", "component",
        "amount_atoms", "currency", "atomic_scale", "evidence_status", "price_basis", "state"}),
    "com.exomachina.commercial.payment.v1": frozenset({
        "run_id", "payment_id", "amount_atoms", "currency", "atomic_scale", "state",
        "evidence_status"}),
}

_ID_FIELDS = frozenset({
    "factory_id", "run_id", "task_id", "context_id", "assignment_id", "attempt_id", "node",
    "artifact_revision", "previous_revision",
    "identity", "provider_identity", "author_identity", "reviewer_identity",
    "actor_identity", "owner_identity", "service_identity", "model_call_id", "destination_id",
    "destination_identity", "decision_id", "wait_actor_identity",
    "command_id", "receipt_id",
    "incident_id", "admission_id", "usage_id", "obligation_id", "payment_id",
})
_DIGEST_FIELDS = frozenset({
    "manifest_digest", "package_digest", "definition_digest", "artifact_sha256",
    "previous_sha256", "offer_digest", "contract_digest", "markdown_sha256",
})
_TIME_FIELDS = frozenset({
    "started_at", "ended_at", "wait_deadline", "wait_started_at", "delivered_at",
})
_INT_FIELDS = frozenset({
    "repair_count", "max_repairs", "queue_position", "byte_length", "finding_count",
    "capacity_limit", "active_count", "queued_count", "amount_atoms", "atomic_scale",
    "markup_bps",
})
_BOOL_FIELDS = frozenset({"accepted"})
_ENUM_FIELDS = frozenset({
    "state", "phase", "outcome", "expected_state", "resulting_state", "lifecycle",
    "kind", "measurement_source", "completeness", "reasoning_effort", "evidence_status",
    "price_basis", "payment_trigger",
})
_COMMERCIAL_COMPONENTS = frozenset({
    "inference_cost", "hosting_cost", "markup", "supplier_charge", "payment_fees",
    "owner_overhead", "production_cost", "customer_price",
})
_COMMERCIAL_EVIDENCE = frozenset({
    "measured", "calculated_from_measured_usage", "provider_reported", "estimated",
    "unknown", "undisclosed",
})
_PRICE_BASES = frozenset({"usage", "fixed_assignment", "fixed_attempt", "accepted_outcome"})
_PAYMENT_TRIGGERS = frozenset({
    "upfront", "incremental_use", "attempt_completion", "acceptance",
})
_WAIT_ROLES = frozenset({"director", "human"})
_WAIT_PHASE_ROLES = {"awaiting-director": "director", "awaiting-human": "human"}
_WAIT_FIELDS = frozenset({
    "node", "wait_deadline", "wait_role", "wait_actor_identity",
    "wait_started_at", "permitted_actions",
})
_UNKNOWN_EVIDENCE = frozenset({"unknown", "undisclosed"})
_MONEY_EVENT_TYPES = frozenset({
    "com.exomachina.commercial.obligation.v1", "com.exomachina.commercial.payment.v1",
})
_SAFE_ENUM = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}$")
_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 _./-]{0,127}$")
_SAFE_CAPABILITY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._@+-]{0,127}$")
_SAFE_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_SAFE_CURRENCY = re.compile(r"^[A-Z0-9][A-Z0-9._-]{0,15}$")
_SAFE_DECIMAL = re.compile(r"^(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")
_SAFE_MEDIA = re.compile(r"^[a-z0-9.+-]+/[a-z0-9.+-]+$")
_SAFE_UNIT = re.compile(r"^[a-z][a-z0-9._-]{0,31}$")
_SAFE_MODEL = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_OPAQUE_CURSOR = re.compile(r"^c1\.([A-Za-z0-9_-]{16})\.([A-Za-z0-9_-]{40,48})$")


class ObservationError(Exception):
    """Base error for an observation request or source contract violation."""


class ObservationForbidden(ObservationError):
    pass


class ObservationNotFound(ObservationError):
    pass


class InvalidCursor(ObservationError):
    pass


class CursorExpired(ObservationError):
    def __init__(self, minimum_cursor: str, latest_cursor: str):
        super().__init__("observation cursor is older than retained events")
        self.minimum_cursor = minimum_cursor
        self.latest_cursor = latest_cursor


class SourceContractError(ObservationError):
    pass


@dataclass(frozen=True)
class SourcePage:
    records: Iterable[Mapping[str, Any]]
    next_cursor: str | None
    has_more: bool = False
    freshness: Mapping[str, Any] | None = None
    run_freshness: Mapping[str, Mapping[str, Any]] | None = None


class ObservationSource(Protocol):
    """Server-side adapter over actual runtime records.

    `read_page` returns durable source facts in source order. `source_id` must
    remain stable when a record is replayed. Its cursor is opaque to the
    dashboard. Authorization is enforced here and rechecked before every read.
    Optional SourcePage.run_freshness maps exact owned runs to the same captured
    source-read coverage; projection commits it with state and cursor.
    """

    def discover(self, principal: object) -> Iterable[Mapping[str, Any]]: ...
    def authorize(self, principal: object, factory_id: str) -> None: ...
    def read_page(self, principal: object, factory_id: str, after_cursor: str | None,
                  *, limit: int) -> SourcePage: ...


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _utc_time(value: Any) -> str:
    if not isinstance(value, str) or len(value) > 64:
        raise SourceContractError("source event time must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise SourceContractError("source event time must be an ISO timestamp") from error
    if parsed.tzinfo is None:
        raise SourceContractError("source event time must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _string(field: str, value: Any) -> str:
    if not isinstance(value, str) or len(value) > 256 or any(ord(ch) < 32 for ch in value):
        raise SourceContractError(f"invalid observation field: {field}")
    if field in _ID_FIELDS:
        pattern = _SAFE_CAPABILITY if field == "capability" else _SAFE_ID
    elif field in _DIGEST_FIELDS:
        pattern = _SAFE_DIGEST
    elif field in _ENUM_FIELDS:
        pattern = _SAFE_ENUM
    elif field in {"name", "publication_version"}:
        pattern = _SAFE_NAME
    elif field in {"currency", "asset", "network"}:
        pattern = _SAFE_CURRENCY
    elif field == "media_type":
        pattern = _SAFE_MEDIA
    elif field == "unit":
        pattern = _SAFE_UNIT
    elif field == "model_id":
        pattern = _SAFE_MODEL
    elif field == "capability":
        pattern = _SAFE_CAPABILITY
    elif field == "quantity":
        pattern = _SAFE_DECIMAL
    else:
        pattern = _SAFE_ID
    if not pattern.fullmatch(value):
        raise SourceContractError(f"invalid observation field: {field}")
    return value


def _safe_nodes(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or len(value) > 500:
        raise SourceContractError("invalid graph node projection")
    result = []
    for item in value:
        if not isinstance(item, Mapping) or set(item) - {
                "id", "type", "next", "capability", "output", "control"}:
            raise SourceContractError("invalid graph node projection")
        node = {key: _string(key, item[key]) for key in ("id", "type") if key in item}
        if "id" not in node or "type" not in node:
            raise SourceContractError("graph nodes require id and type")
        if "capability" in item:
            node["capability"] = _string("capability", item["capability"])
        if "next" in item:
            targets = item["next"]
            if (not isinstance(targets, list) or len(targets) > 100
                    or any(not isinstance(target, str) or not _SAFE_ID.fullmatch(target)
                           for target in targets)):
                raise SourceContractError("invalid graph edge projection")
            node["next"] = list(targets)
        if "output" in item:
            if item["output"] not in {"artifacts", "message", "none"}:
                raise SourceContractError("invalid graph node output mode")
            node["output"] = item["output"]
        if "control" in item:
            # Control targets are the subset of `next` that only sequences work.
            control = item["control"]
            if (not isinstance(control, list) or len(control) > 100
                    or any(target not in node.get("next", []) for target in control)
                    or len(set(control)) != len(control)):
                raise SourceContractError("invalid graph control edge projection")
            node["control"] = list(control)
        result.append(node)
    return result


def _safe_bindings(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list) or len(value) > 100:
        raise SourceContractError("invalid service binding projection")
    result = []
    allowed = {"name", "role", "identity", "capability", "contract_digest"}
    for item in value:
        if not isinstance(item, Mapping) or set(item) - allowed:
            raise SourceContractError("invalid service binding projection")
        required = {"name", "role", "identity", "contract_digest"}
        if not required <= set(item):
            raise SourceContractError("incomplete service binding projection")
        row = {key: _string(key, item[key]) for key in required}
        if "capability" in item:
            row["capability"] = _string("capability", item["capability"])
        result.append(row)
    return result


def _safe_string_list(field: str, value: Any, *, digest_only: bool = False) -> list[str]:
    max_items = 16 if field == "permitted_actions" else 100
    if not isinstance(value, list) or len(value) > max_items:
        raise SourceContractError(f"invalid observation field: {field}")
    result = []
    for item in value:
        if digest_only:
            result.append(_string("offer_digest", item))
        elif field in {"finding_codes", "permitted_actions"}:
            if not isinstance(item, str) or not _SAFE_ENUM.fullmatch(item):
                raise SourceContractError(f"invalid observation field: {field}")
            result.append(item)
        else:
            result.append(_string("identity", item))
    if field == "permitted_actions" and len(result) != len(set(result)):
        raise SourceContractError("permitted actions must be distinct")
    return result


def _project_fields(event_type: str, record: Mapping[str, Any]) -> dict[str, Any]:
    if event_type not in EVENT_FIELDS:
        raise SourceContractError("unsupported observation event type")
    fields = record.get("fields", {})
    if not isinstance(fields, Mapping):
        raise SourceContractError("source event fields must be an object")
    result: dict[str, Any] = {"schema_version": SCHEMA_VERSION}
    for field in COMMON_FIELDS:
        value = record.get(field)
        if value is not None:
            result[field] = _string(field, value)
    # Top-level factory ID is required, irrespective of whether the source also
    # repeats it in `fields`.
    if "factory_id" not in result:
        raise SourceContractError("source event is missing factory identity")
    permitted = EVENT_FIELDS[event_type]
    for field in permitted:
        if field not in fields:
            continue
        value = fields[field]
        if field in {"quantity", "model_call_id"} and value is None:
            # The ledger represents unreported quantities and optional call IDs
            # as null. The dashboard contract represents both by omission.
            continue
        if field in {"graph_nodes"}:
            result[field] = _safe_nodes(value)
        elif field == "service_bindings":
            result[field] = _safe_bindings(value)
        elif field in {"finding_codes", "permitted_actions", "evidence_refs"}:
            result[field] = _safe_string_list(field, value, digest_only=(field == "evidence_refs"))
        elif field == "component":
            if not isinstance(value, str) or value not in _COMMERCIAL_COMPONENTS:
                raise SourceContractError("invalid commercial cost component")
            result[field] = value
        elif field == "evidence_status":
            if not isinstance(value, str) or value not in _COMMERCIAL_EVIDENCE:
                raise SourceContractError("invalid commercial evidence status")
            result[field] = value
        elif field == "price_basis":
            if not isinstance(value, str) or value not in _PRICE_BASES:
                raise SourceContractError("invalid commercial price basis")
            result[field] = value
        elif field == "payment_trigger":
            if not isinstance(value, str) or value not in _PAYMENT_TRIGGERS:
                raise SourceContractError("invalid commercial payment trigger")
            result[field] = value
        elif field == "delivery_kind":
            if value != "local_file":
                raise SourceContractError("unsupported delivery kind")
            result[field] = value
        elif field == "wait_role":
            if not isinstance(value, str) or value not in _WAIT_ROLES:
                raise SourceContractError("invalid wait role")
            result[field] = value
        elif field == "amount_atoms" and value is None and event_type in _MONEY_EVENT_TYPES:
            result[field] = None
        elif field in _ID_FIELDS | _DIGEST_FIELDS | _ENUM_FIELDS | {
                "name", "publication_version", "currency", "asset", "network",
                "media_type", "unit", "model_id", "quantity", "capability"}:
            result[field] = _string(field, value)
        elif field in _TIME_FIELDS:
            result[field] = _utc_time(value)
        elif field in _INT_FIELDS:
            if type(value) is not int or value < 0:
                raise SourceContractError(f"invalid observation field: {field}")
            result[field] = value
        elif field in _BOOL_FIELDS:
            if type(value) is not bool:
                raise SourceContractError(f"invalid observation field: {field}")
            result[field] = value
        elif field == "model_id":
            result[field] = _string(field, value)
        else:
            raise SourceContractError(f"unclassified observation field: {field}")
    missing = EVENT_REQUIRED[event_type] - set(result)
    if missing:
        raise SourceContractError("source event is missing required allowlisted facts")
    if event_type == "com.exomachina.commercial.usage.v1":
        completeness = result["completeness"]
        has_quantity = "quantity" in result
        if has_quantity and completeness != "complete":
            raise SourceContractError("reported usage quantity requires complete completeness")
        if not has_quantity and completeness not in _UNKNOWN_EVIDENCE:
            raise SourceContractError(
                "unreported usage quantity must be omitted and marked unknown or undisclosed")
    elif event_type == "com.exomachina.delivery.receipt.v1" and "delivery_kind" in result:
        required_local_file = {
            "run_id", "task_id", "context_id", "receipt_id", "artifact_revision",
            "artifact_sha256", "markdown_sha256", "destination_identity", "byte_length",
            "delivered_at", "outcome",
        }
        if required_local_file - set(result):
            raise SourceContractError("local delivery receipt is missing required public facts")
        if result["outcome"] != "local-file-delivered":
            raise SourceContractError("local delivery receipt has an invalid outcome")
    elif event_type in _MONEY_EVENT_TYPES:
        evidence = result["evidence_status"]
        amount = result["amount_atoms"]
        if (amount is None) != (evidence in _UNKNOWN_EVIDENCE):
            raise SourceContractError(
                "money amount is null only when its evidence is unknown or undisclosed")
    elif event_type == "com.exomachina.run.state_changed.v1":
        wait_fields = (_WAIT_FIELDS - {"node"}) & set(result)
        if wait_fields:
            phase = result.get("phase")
            if phase not in _WAIT_PHASE_ROLES:
                raise SourceContractError("wait facts require an actual waiting phase")
            expected_role = _WAIT_PHASE_ROLES[phase]
            if "wait_role" in result and result["wait_role"] != expected_role:
                raise SourceContractError("wait role does not match the observed phase")
    return result


def project_source_record(record: Mapping[str, Any], factory_id: str) -> tuple[str, str, str, dict[str, Any]]:
    """Return stable source key, run ID, and a safe CloudEvents envelope."""
    if not isinstance(record, Mapping):
        raise SourceContractError("source record must be an object")
    if record.get("factory_id") != factory_id:
        raise SourceContractError("source record factory does not match requested factory")
    source_kind, source_id = record.get("source_kind"), record.get("source_id")
    if (not isinstance(source_kind, str) or source_kind not in SOURCE_KINDS
            or not isinstance(source_id, str) or not source_id or len(source_id) > 512):
        raise SourceContractError("source record requires a stable source identity")
    event_type = record.get("event_type")
    if not isinstance(event_type, str) or event_type not in EVENT_FIELDS:
        raise SourceContractError("unsupported observation event type")
    run_id = record.get("run_id")
    if run_id is not None:
        run_id = _string("run_id", run_id)
    event_id = "obs-" + hashlib.sha256(f"{source_kind}\0{source_id}".encode()).hexdigest()
    source_key = hashlib.sha256(f"{factory_id}\0{source_kind}\0{source_id}".encode()).hexdigest()
    envelope = {
        "specversion": "1.0",
        "id": event_id,
        "source": f"/factories/{quote(factory_id, safe='')}",
        "type": event_type,
        "time": _utc_time(record.get("time")),
        "subject": (f"runs/{quote(run_id, safe='')}" if run_id else "factory"),
        "datacontenttype": "application/json",
        "dataschema": EVENT_DATA_SCHEMA,
        "data": _project_fields(event_type, record),
    }
    return source_key, run_id or "", event_id, envelope


def _initial_state(factory_id: str) -> dict[str, Any]:
    return {"schema_version": SCHEMA_VERSION, "factory_id": factory_id,
            "active_publication": None, "factory": None, "capacity": None,
            "runs": {}, "commercial": {"usage": [], "obligations": [], "payments": []}}


def _append_bounded(items: list, value: Any, limit: int = 256) -> None:
    items.append(value)
    if len(items) > limit:
        del items[:len(items) - limit]


def _snapshot_graph(graph_nodes: list[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Normalize observed graph nodes without consulting a replaceable publication."""
    node_ids = {node["id"] for node in graph_nodes}
    if len(node_ids) != len(graph_nodes):
        raise SourceContractError("pinned graph has duplicate node identifiers")
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    for node in graph_nodes:
        projected_node = {"id": node["id"], "kind": node["type"]}
        if "capability" in node:
            projected_node["capability"] = node["capability"]
        if "output" in node:
            projected_node["output"] = node["output"]
        nodes.append(projected_node)
        control = node.get("control")
        for target in node.get("next", []):
            if target not in node_ids:
                raise SourceContractError("pinned publication graph has an unknown edge target")
            edge = {"from": node["id"], "to": target}
            if control is not None:
                edge["kind"] = "control" if target in control else "material"
            edges.append(edge)
    side_effects = {node["id"] for node in nodes if node.get("output") == "none"}
    if any(edge["from"] in side_effects and edge.get("kind", "material") == "material"
           for edge in edges):
        raise SourceContractError("side-effect node has an outgoing material edge")
    return {"nodes": nodes, "edges": edges}


def _merge_run_state(previous: Mapping[str, Any], update: Mapping[str, Any]) -> dict[str, Any]:
    """Retain lifecycle facts across refreshes and keep the earliest start time."""
    merged = dict(previous)
    if "phase" in update:
        phase = update["phase"]
        if phase not in _WAIT_PHASE_ROLES or phase != previous.get("phase"):
            # Wait annotations describe the current wait only. Durable event
            # records remain available, while the materialized status drops
            # them when a wait ends or changes phase.
            for field in _WAIT_FIELDS:
                merged.pop(field, None)
    merged.update(update)
    starts = [
        _utc_time(value)
        for value in (previous.get("started_at"), update.get("started_at"))
        if value is not None
    ]
    if starts:
        merged["started_at"] = min(
            starts,
            key=lambda value: datetime.fromisoformat(value.replace("Z", "+00:00")),
        )
    return merged


def _reduce(state: dict[str, Any], event_type: str, event: Mapping[str, Any]) -> None:
    data = event["data"]
    run_id = data.get("run_id")
    if event_type == "com.exomachina.factory.discovered.v1":
        state["factory"] = data
        return
    if event_type == "com.exomachina.publication.activated.v1":
        state["active_publication"] = data
        return
    if event_type == "com.exomachina.capacity.state_changed.v1":
        state["capacity"] = data
        return
    if event_type.startswith("com.exomachina.commercial."):
        category = event_type.removeprefix("com.exomachina.commercial.").removesuffix(".v1")
        category = {"usage": "usage", "obligation": "obligations", "payment": "payments"}.get(category)
        if category:
            _append_bounded(state["commercial"][category], data)
        if not run_id:
            return
    if not run_id:
        return
    run = state["runs"].setdefault(run_id, {
        "run_id": run_id, "task": None, "pinned": {}, "state": None, "assignments": {},
        "graph": None, "artifacts": [], "quality": [], "decisions": [], "commands": [],
        "delivery": [], "incidents": [], "admissions": [],
    })
    if event_type == "com.exomachina.run.created.v1" and "graph_nodes" in data:
        observed_graph = _snapshot_graph(data["graph_nodes"])
        existing_graph = run.get("graph")
        if existing_graph is not None and existing_graph != observed_graph:
            raise SourceContractError("pinned run graph changed")
        run["graph"] = observed_graph
    pinned = {key: data[key] for key in (
        "manifest_digest", "package_digest", "definition_digest", "interpreter_build"
    ) if key in data}
    for key, value in pinned.items():
        previous = run["pinned"].get(key)
        if previous is not None and previous != value:
            raise SourceContractError("pinned run publication changed")
    run["pinned"].update(pinned)
    binding = dict(run.get("task") or {})
    for source_field, task_field in (("task_id", "id"), ("context_id", "context_id")):
        value = data.get(source_field)
        if value is None:
            continue
        existing = binding.get(task_field)
        if existing is not None and existing != value:
            raise SourceContractError("original Task binding changed during a run")
        binding[task_field] = value
    if binding:
        run["task"] = binding
    if event_type in {"com.exomachina.run.created.v1", "com.exomachina.run.state_changed.v1"}:
        run["state"] = _merge_run_state(run.get("state") or {}, data)
    elif event_type == "com.exomachina.assignment.state_changed.v1":
        assignment_id = data.get("assignment_id") or "unknown"
        attempt_id = data.get("attempt_id") or "current"
        attempts = run["assignments"].setdefault(assignment_id, {})
        previous = attempts.get(attempt_id, {})
        current = dict(data)
        # A later state transition for this exact attempt can omit facts that
        # were recorded when it started. Carry forward only stable assignment
        # linkage facts; do not carry queue position or arbitrary old fields.
        for field in ("started_at", "provider_identity", "node"):
            if field not in current and field in previous:
                current[field] = previous[field]
        if (data.get("state") in {"scheduled", "running", "working", "active", "unknown"}
                and "ended_at" not in data):
            # A projection correction can invalidate an older completion
            # attribution. Its old end time must not survive as current state.
            current.pop("ended_at", None)
        attempts[attempt_id] = current
    elif event_type == "com.exomachina.artifact.revised.v1":
        _append_bounded(run["artifacts"], data)
    elif event_type == "com.exomachina.quality.verdict.v1":
        _append_bounded(run["quality"], data)
    elif event_type == "com.exomachina.decision.outcome.v1":
        _append_bounded(run["decisions"], data)
    elif event_type == "com.exomachina.command.outcome.v1":
        _append_bounded(run["commands"], data)
    elif event_type == "com.exomachina.delivery.receipt.v1":
        _append_bounded(run["delivery"], data)
    elif event_type == "com.exomachina.incident.state_changed.v1":
        incident_id = data["incident_id"]
        for index, existing in enumerate(run["incidents"]):
            if existing.get("incident_id") == incident_id:
                # Snapshot rows are the latest complete state for each
                # incident. Replace the row so omitted optional fields (such
                # as an owner cleared by escalation) do not survive from an
                # older version. The append-only CloudEvent journal retains
                # every source transition independently.
                run["incidents"][index] = data
                break
        else:
            _append_bounded(run["incidents"], data)
    elif event_type == "com.exomachina.admission.state_changed.v1":
        _append_bounded(run["admissions"], data)


def _snapshot_state(state: Mapping[str, Any], *, selected_run_id: str | None = None) -> dict[str, Any]:
    """Shape durable source facts for the dashboard's shared snapshot contract."""
    factory_record = state.get("factory")
    if not isinstance(factory_record, Mapping):
        raise ObservationNotFound("factory discovery facts are unavailable")
    name = factory_record.get("name")
    if not isinstance(name, str):
        raise SourceContractError("factory discovery record has no public name")
    publication = state.get("active_publication")
    graph_nodes = (publication or {}).get("graph_nodes", factory_record.get("graph_nodes"))
    if graph_nodes is None:
        # A newly discovered factory may not have an active publication yet.
        # Preserve the observed absence of graph facts as an empty projection.
        graph_nodes = []
    graph = _snapshot_graph(graph_nodes)
    factory = {"id": state["factory_id"], "name": name,
               "graph": graph}
    for key in ("identity", "capability"):
        if key in factory_record:
            factory[key] = factory_record[key]

    # Bindings describe the currently active publication. A run-filtered
    # snapshot may expose them only when that run is pinned to this exact
    # publication; historical pins must never inherit today's bindings.
    service_bindings = (publication or {}).get("service_bindings")
    include_bindings = selected_run_id is None
    if selected_run_id is not None and isinstance(publication, Mapping):
        selected_run = state.get("runs", {}).get(selected_run_id)
        selected_pins = selected_run.get("pinned") if isinstance(selected_run, Mapping) else None
        include_bindings = (
            isinstance(selected_pins, Mapping)
            and isinstance(selected_pins.get("manifest_digest"), str)
            and selected_pins.get("manifest_digest") == publication.get("manifest_digest")
        )
    if service_bindings is not None and include_bindings:
        factory["agent_bindings"] = _safe_bindings(service_bindings)

    runs = []
    for run_id, record in sorted(state["runs"].items()):
        current = record.get("state") or {}
        task = record.get("task")
        if not isinstance(task, Mapping):
            raise SourceContractError("run snapshot has no original Task binding")
        status = {"state": current["state"]}
        for field in ("phase", "node", "started_at", "ended_at", "wait_deadline", "wait_role",
                      "wait_actor_identity", "wait_started_at", "permitted_actions"):
            if current.get(field) is not None:
                status[field] = current[field]
        run = {
            "id": run_id,
            "task": dict(task),
            "status": status,
            "pinned": dict(record["pinned"]),
            "assignments": [
                {"id": assignment_id, "attempts": list(attempts.values())}
                for assignment_id, attempts in sorted(record["assignments"].items())
            ],
            "artifacts": list(record["artifacts"]),
            "decisions": list(record["decisions"]),
            "quality": list(record["quality"]),
            "commands": list(record["commands"]),
            "delivery": list(record["delivery"]),
            "incidents": list(record["incidents"]),
            "admissions": list(record["admissions"]),
        }
        if current.get("started_at") is not None:
            run["started_at"] = current["started_at"]
        if record.get("graph") is not None:
            run["graph"] = record["graph"]
        runs.append(run)
    return {"factory": factory, "runs": runs,
            "active_publication": state.get("active_publication"),
            "capacity": state.get("capacity"),
            "commercial": state.get("commercial")}


class FactoryObservation:
    """Durable, allowlisted CloudEvents projection over a runtime source adapter.

    `source` is intentionally injected by the runtime owner. This keeps
    Temporal, Task, publication, artifact and commercial access behind their
    existing authority Interfaces. SQLite commits projected facts, the
    materialized snapshot, and the opaque source checkpoint atomically.
    """

    def __init__(self, source: ObservationSource, database: Path, *,
                 retention_events: int = 10_000, page_size: int = 250,
                 max_refresh_pages: int = 16):
        if type(retention_events) is not int or retention_events < 1:
            raise ValueError("retention_events must be positive")
        if type(page_size) is not int or page_size < 1 or page_size > 10_000:
            raise ValueError("page_size must be between 1 and 10000")
        if type(max_refresh_pages) is not int or max_refresh_pages < 1:
            raise ValueError("max_refresh_pages must be positive")
        self.source = source
        self.database = Path(database)
        self.retention_events = retention_events
        self.page_size = page_size
        self.max_refresh_pages = max_refresh_pages
        self._factory_locks: dict[str, threading.Lock] = {}
        self._factory_locks_lock = threading.Lock()
        self.database.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS observation_streams (
                    factory_id TEXT PRIMARY KEY,
                    cursor_scope TEXT NOT NULL,
                    source_cursor TEXT,
                    last_cursor INTEGER NOT NULL DEFAULT 0,
                    minimum_cursor INTEGER NOT NULL DEFAULT 1,
                    state_json TEXT NOT NULL,
                    freshness_json TEXT NOT NULL,
                    captured_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS observation_events (
                    factory_id TEXT NOT NULL,
                    cursor INTEGER NOT NULL,
                    source_key TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    envelope_json TEXT NOT NULL,
                    PRIMARY KEY (factory_id, cursor),
                    UNIQUE (factory_id, source_key),
                    UNIQUE (factory_id, event_id)
                );
                CREATE TABLE IF NOT EXISTS observation_source_records (
                    factory_id TEXT NOT NULL,
                    source_key TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    envelope_digest TEXT NOT NULL,
                    projection_cursor INTEGER NOT NULL,
                    envelope_json TEXT NOT NULL,
                    PRIMARY KEY (factory_id, source_key),
                    UNIQUE (factory_id, event_id)
                );
                CREATE TABLE IF NOT EXISTS observation_cursors (
                    token TEXT PRIMARY KEY,
                    factory_id TEXT NOT NULL,
                    sequence INTEGER NOT NULL,
                    UNIQUE (factory_id, sequence)
                );
                CREATE INDEX IF NOT EXISTS observation_events_run_cursor
                    ON observation_events(factory_id, run_id, cursor);
            """)
            columns = {row[1] for row in db.execute("PRAGMA table_info(observation_streams)")}
            if "cursor_scope" not in columns:
                db.execute("ALTER TABLE observation_streams ADD COLUMN cursor_scope TEXT")
            for row in db.execute("SELECT factory_id FROM observation_streams WHERE cursor_scope IS NULL").fetchall():
                db.execute("UPDATE observation_streams SET cursor_scope=? WHERE factory_id=?",
                           (secrets.token_urlsafe(12), row[0]))
            db.execute("CREATE UNIQUE INDEX IF NOT EXISTS observation_cursor_scope "
                       "ON observation_streams(cursor_scope)")
            source_columns = {row[1] for row in db.execute(
                "PRAGMA table_info(observation_source_records)")}
            if "projection_cursor" not in source_columns:
                db.execute("ALTER TABLE observation_source_records ADD COLUMN projection_cursor INTEGER")
            if "envelope_json" not in source_columns:
                db.execute("ALTER TABLE observation_source_records ADD COLUMN envelope_json TEXT")
            # Preserve dedupe knowledge if an existing projection database is
            # reopened after this table is introduced.
            for row in db.execute("SELECT factory_id, cursor, source_key, event_id, envelope_json "
                                  "FROM observation_events").fetchall():
                digest = hashlib.sha256(row["envelope_json"].encode("utf-8")).hexdigest()
                db.execute("INSERT OR IGNORE INTO observation_source_records "
                           "(factory_id,source_key,event_id,envelope_digest,projection_cursor,envelope_json) "
                           "VALUES (?,?,?,?,?,?)",
                           (row["factory_id"], row["source_key"], row["event_id"], digest,
                            row["cursor"], row["envelope_json"]))
                db.execute("UPDATE observation_source_records SET envelope_digest=?,projection_cursor=?,"
                           "envelope_json=? WHERE factory_id=? AND source_key=?",
                           (digest, row["cursor"], row["envelope_json"],
                            row["factory_id"], row["source_key"]))
            db.commit()

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.database, timeout=15, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        db.execute("PRAGMA foreign_keys=ON")
        return db

    def _factory_lock(self, factory_id: str) -> threading.Lock:
        with self._factory_locks_lock:
            return self._factory_locks.setdefault(factory_id, threading.Lock())

    @staticmethod
    def _opaque_cursor(db: sqlite3.Connection, factory_id: str, sequence: int) -> str:
        stream = db.execute("SELECT cursor_scope FROM observation_streams WHERE factory_id=?",
                            (factory_id,)).fetchone()
        if stream is None:
            raise ObservationNotFound("unknown factory")
        row = db.execute("SELECT token FROM observation_cursors "
                          "WHERE factory_id=? AND sequence=?", (factory_id, sequence)).fetchone()
        if row is not None:
            return row["token"]
        token = "c1." + stream["cursor_scope"] + "." + secrets.token_urlsafe(32)
        db.execute("INSERT INTO observation_cursors VALUES (?,?,?)", (token, factory_id, sequence))
        return token

    @staticmethod
    def _decode_cursor(db: sqlite3.Connection, factory_id: str, token: Any) -> int | None:
        if not isinstance(token, str) or len(token) > 80:
            raise InvalidCursor("cursor is malformed")
        matched = _OPAQUE_CURSOR.fullmatch(token)
        if matched is None:
            raise InvalidCursor("cursor is malformed")
        scope = matched.group(1)
        owner = db.execute("SELECT factory_id FROM observation_streams WHERE cursor_scope=?",
                           (scope,)).fetchone()
        if owner is not None and owner["factory_id"] != factory_id:
            raise InvalidCursor("cursor belongs to another factory")
        row = db.execute("SELECT factory_id, sequence FROM observation_cursors WHERE token=?",
                         (token,)).fetchone()
        if row is None:
            # Well-formed but no longer mapped tokens have expired. The caller
            # will be given fresh opaque boundaries and a snapshot.
            return None
        if row["factory_id"] != factory_id:
            raise InvalidCursor("cursor belongs to another factory")
        return row["sequence"]

    def _expired_cursor(self, db: sqlite3.Connection, factory_id: str,
                        minimum: int, latest: int) -> CursorExpired:
        return CursorExpired(self._opaque_cursor(db, factory_id, max(0, minimum - 1)),
                             self._opaque_cursor(db, factory_id, latest))

    def _authorize(self, principal: object, factory_id: str) -> None:
        try:
            self.source.authorize(principal, factory_id)
        except ObservationError:
            raise
        except PermissionError as error:
            raise ObservationForbidden("factory is not accessible") from error

    def discover(self, principal: object) -> list[dict[str, Any]]:
        rows = []
        for record in self.source.discover(principal):
            if not isinstance(record, Mapping):
                continue
            factory_id = record.get("factory_id")
            if not isinstance(factory_id, str) or not _SAFE_ID.fullmatch(factory_id):
                continue
            try:
                self._authorize(principal, factory_id)
            except ObservationForbidden:
                continue
            public = {"factory_id": factory_id}
            for key in ("identity", "capability", "name", "active_manifest_digest",
                        "active_package_digest", "active_interpreter_build"):
                if key not in record or record[key] is None:
                    continue
                safe_key = {"active_manifest_digest": "manifest_digest",
                            "active_package_digest": "package_digest",
                            "active_interpreter_build": "interpreter_build"}.get(key, key)
                public[safe_key] = _string(safe_key, record[key])
            rows.append(public)
        rows.sort(key=lambda row: row["factory_id"])
        return rows

    def _lock_for(self, factory_id: str) -> threading.Lock:
        return self._factory_lock(factory_id)

    def _ensure_stream(self, factory_id: str) -> None:
        with self._connect() as db:
            db.execute("INSERT OR IGNORE INTO observation_streams "
                       "(factory_id, cursor_scope, state_json, freshness_json, captured_at) "
                       "VALUES (?,?,?,?,?)",
                       (factory_id, secrets.token_urlsafe(12), _canonical(_initial_state(factory_id)),
                        _canonical({"status": "unknown", "observed_at": None}),
                        datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")))
            db.commit()

    @staticmethod
    def _coerce_page(value: Any) -> SourcePage:
        if isinstance(value, SourcePage):
            return value
        if isinstance(value, Mapping) and {"records", "next_cursor", "has_more"} <= set(value):
            if type(value["has_more"]) is not bool:
                raise SourceContractError("source page has_more must be boolean")
            return SourcePage(value["records"], value["next_cursor"], value["has_more"],
                              value.get("freshness"), value.get("run_freshness"))
        raise SourceContractError("source read_page returned an invalid page")

    def refresh(self, principal: object, factory_id: str, *, max_pages: int | None = None) -> int:
        """Ingest durable source facts; duplicate source IDs do not advance cursor."""
        if not _SAFE_ID.fullmatch(factory_id):
            raise ObservationNotFound("unknown factory")
        self._authorize(principal, factory_id)
        self._ensure_stream(factory_id)
        page_limit = self.max_refresh_pages if max_pages is None else max_pages
        if type(page_limit) is not int or page_limit < 1:
            raise ValueError("max_pages must be positive")
        inserted = 0
        with self._lock_for(factory_id):
            for _ in range(page_limit):
                with self._connect() as db:
                    stream = db.execute("SELECT source_cursor, last_cursor FROM observation_streams "
                                        "WHERE factory_id=?", (factory_id,)).fetchone()
                before_cursor = json.loads(stream["source_cursor"]) if stream["source_cursor"] is not None else None
                if before_cursor is not None and not isinstance(before_cursor, str):
                    raise SourceContractError("source cursor must be an opaque string")
                page = self._coerce_page(self.source.read_page(
                    principal, factory_id, before_cursor, limit=self.page_size))
                if type(page.has_more) is not bool:
                    raise SourceContractError("source page has_more must be boolean")
                if not isinstance(page.records, Iterable):
                    raise SourceContractError("source page records must be iterable")
                if page.next_cursor is not None:
                    if not isinstance(page.next_cursor, str) or len(page.next_cursor) > 4096:
                        raise SourceContractError("source cursor must be an opaque string")
                    try:
                        next_cursor_json = _canonical(page.next_cursor)
                    except (TypeError, ValueError) as error:
                        raise SourceContractError("source cursor must be JSON serializable") from error
                else:
                    next_cursor_json = None
                records = list(page.records)
                if len(records) > self.page_size:
                    raise SourceContractError("source returned more records than requested")
                normalized = [project_source_record(record, factory_id) for record in records]
                freshness = _normalize_freshness(page.freshness)
                captured_freshness: dict[str, Any] = freshness
                if page.run_freshness is not None:
                    if not isinstance(page.run_freshness, Mapping) or len(page.run_freshness) > 256:
                        raise SourceContractError("invalid bounded run freshness map")
                    scopes = {}
                    for run_key, run_value in page.run_freshness.items():
                        if not isinstance(run_key, str) or not _SAFE_ID.fullmatch(run_key):
                            raise SourceContractError("invalid run freshness map key")
                        scoped = _normalize_freshness(run_value)
                        if scoped.get("scope") != "run" or scoped.get("run_id") != run_key or scoped.get("factory_status") != freshness["status"]:
                            raise SourceContractError("run freshness map binding mismatch")
                        scopes[run_key] = scoped
                    if page.has_more:
                        freshness = {**freshness, "status": "stale"}
                        scopes = {key: {**value, "status": "stale", "factory_status": "stale"}
                                  for key, value in scopes.items()}
                    captured_freshness = {"factory": freshness, "runs": scopes}
                with self._connect() as db:
                    db.execute("BEGIN IMMEDIATE")
                    stream = db.execute("SELECT source_cursor, last_cursor, state_json "
                                        "FROM observation_streams WHERE factory_id=?",
                                        (factory_id,)).fetchone()
                    actual_json = stream["source_cursor"]
                    expected_json = _canonical(before_cursor) if before_cursor is not None else None
                    if actual_json != expected_json:
                        db.rollback()
                        continue
                    state = json.loads(stream["state_json"])
                    last_cursor = stream["last_cursor"]
                    page_inserted = 0
                    for (source_key, run_id, event_id, envelope), record in zip(normalized, records):
                        envelope_json = _canonical(envelope)
                        envelope_digest = hashlib.sha256(envelope_json.encode("utf-8")).hexdigest()
                        exists = db.execute("SELECT envelope_digest FROM observation_source_records "
                                            "WHERE factory_id=? AND source_key=?",
                                            (factory_id, source_key)).fetchone()
                        if exists:
                            if exists["envelope_digest"] != envelope_digest:
                                raise SourceContractError("durable source record changed after projection")
                            continue
                        last_cursor += 1
                        db.execute("INSERT INTO observation_source_records "
                                   "(factory_id,source_key,event_id,envelope_digest,projection_cursor,envelope_json) "
                                   "VALUES (?,?,?,?,?,?)",
                                   (factory_id, source_key, event_id, envelope_digest, last_cursor, envelope_json))
                        db.execute("INSERT INTO observation_events VALUES (?,?,?,?,?,?,?)",
                                   (factory_id, last_cursor, source_key, event_id, run_id,
                                    envelope["type"], envelope_json))
                        _reduce(state, envelope["type"], envelope)
                        page_inserted += 1
                    now = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
                    db.execute("UPDATE observation_streams SET source_cursor=?, last_cursor=?, "
                               "minimum_cursor=?, state_json=?, freshness_json=?, captured_at=? "
                               "WHERE factory_id=?",
                               (next_cursor_json, last_cursor, stream["last_cursor"] + 1,
                                _canonical(state), _canonical(captured_freshness), now, factory_id))
                    # Retain a bounded tail per factory. The high-water mark remains
                    # durable even after old event rows are removed.
                    excess = db.execute("SELECT COUNT(*) - ? FROM observation_events "
                                         "WHERE factory_id=?",
                                         (self.retention_events, factory_id)).fetchone()[0]
                    if excess > 0:
                        db.execute("DELETE FROM observation_events WHERE factory_id=? AND cursor IN "
                                   "(SELECT cursor FROM observation_events WHERE factory_id=? "
                                   "ORDER BY cursor LIMIT ?)", (factory_id, factory_id, excess))
                    minimum = db.execute("SELECT MIN(cursor) FROM observation_events WHERE factory_id=?",
                                         (factory_id,)).fetchone()[0]
                    minimum = minimum if minimum is not None else last_cursor + 1
                    db.execute("UPDATE observation_streams SET minimum_cursor=? WHERE factory_id=?",
                               (minimum, factory_id))
                    db.execute("DELETE FROM observation_cursors WHERE factory_id=? AND sequence<?",
                               (factory_id, max(0, minimum - 1)))
                    db.commit()
                    inserted += page_inserted
                if not page.has_more:
                    break
        return inserted

    def snapshot(self, principal: object, factory_id: str, run_id: str | None = None) -> dict[str, Any]:
        self._authorize(principal, factory_id)
        self.refresh(principal, factory_id)
        if run_id is not None and not _SAFE_ID.fullmatch(run_id):
            raise ObservationNotFound("unknown run")
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT last_cursor, state_json, freshness_json, captured_at "
                             "FROM observation_streams WHERE factory_id=?", (factory_id,)).fetchone()
            if row is None:
                raise ObservationNotFound("unknown factory")
            state = json.loads(row["state_json"])
            if run_id is not None:
                run = state["runs"].get(run_id)
                if run is None:
                    raise ObservationNotFound("unknown run")
                state["runs"] = {run_id: run}
            cursor = self._opaque_cursor(db, factory_id, row["last_cursor"])
            captured = json.loads(row["freshness_json"])
            if isinstance(captured, dict) and set(captured) == {"factory", "runs"}:
                if run_id is not None and run_id not in captured["runs"]:
                    raise SourceContractError("run freshness is unavailable for captured scope")
                freshness = captured["runs"][run_id] if run_id is not None else captured["factory"]
            else:
                freshness = captured  # legacy projection rows retain factory-wide authority
            snapshot = {"schema_version": SCHEMA_VERSION,
                        "cursor": cursor, "captured_at": row["captured_at"],
                        "freshness": freshness,
                        "state": _snapshot_state(state, selected_run_id=run_id)}
            db.commit()
        return snapshot

    def events_after(self, principal: object, factory_id: str, after_cursor: str, *,
                     run_id: str | None = None, limit: int = 250) -> dict[str, Any]:
        self._authorize(principal, factory_id)
        if not isinstance(after_cursor, str):
            raise InvalidCursor("cursor must be an opaque string")
        if type(limit) is not int or limit < 1 or limit > 10_000:
            raise ValueError("limit must be between 1 and 10000")
        if run_id is not None and not _SAFE_ID.fullmatch(run_id):
            raise ObservationNotFound("unknown run")
        self.refresh(principal, factory_id)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            stream = db.execute("SELECT last_cursor, minimum_cursor FROM observation_streams "
                                "WHERE factory_id=?", (factory_id,)).fetchone()
            if stream is None:
                raise ObservationNotFound("unknown factory")
            last, minimum = stream["last_cursor"], stream["minimum_cursor"]
            after_sequence = self._decode_cursor(db, factory_id, after_cursor)
            if after_sequence is None:
                error = self._expired_cursor(db, factory_id, minimum, last)
                db.commit()
                raise error
            if after_sequence > last:
                raise InvalidCursor("cursor is ahead of the stream")
            if after_sequence < minimum - 1:
                error = self._expired_cursor(db, factory_id, minimum, last)
                db.commit()
                raise error
            rows = db.execute("SELECT cursor, run_id, envelope_json FROM observation_events "
                              "WHERE factory_id=? AND cursor>? ORDER BY cursor LIMIT ?",
                              (factory_id, after_sequence, limit)).fetchall()
            scanned = rows[-1]["cursor"] if rows else after_sequence
            selected = [row for row in rows if run_id is None or row["run_id"] == run_id]
            projected = []
            for row in selected:
                projected.append({"cursor": self._opaque_cursor(db, factory_id, row["cursor"]),
                                  "event": json.loads(row["envelope_json"])})
            result = {"events": projected,
                      "continuation_cursor": self._opaque_cursor(db, factory_id, scanned),
                      "checkpoint_advanced": scanned > after_sequence,
                      "has_more": scanned < last}
            db.commit()
        return result

    def inspect_artifact(self, principal: object, factory_id: str, run_id: str,
                         revision: str, sha256: str) -> dict[str, Any]:
        """Delegate artifact bytes to the authenticated runtime source and verify digest."""
        self._authorize(principal, factory_id)
        if not _SAFE_ID.fullmatch(run_id) or not re.fullmatch(r"[0-9a-f]{64}", sha256):
            raise ObservationNotFound("artifact not found")
        reader = getattr(self.source, "inspect_artifact", None)
        if reader is None:
            raise ObservationNotFound("artifact inspection is unavailable")
        result = reader(principal, factory_id, run_id, revision, sha256)
        if not isinstance(result, Mapping) or not isinstance(result.get("content"), bytes):
            raise SourceContractError("artifact source returned an invalid result")
        actual = hashlib.sha256(result["content"]).hexdigest()
        if actual != sha256 or result.get("sha256") != sha256:
            raise SourceContractError("artifact digest does not match its accepted identity")
        media_type = _string("media_type", result.get("media_type"))
        return {"content": result["content"], "sha256": sha256,
                "revision": _string("artifact_revision", revision), "media_type": media_type}

    def prior_command_outcome_event(self, principal: object, factory_id: str,
                                    command_id: str, lifecycle: str,
                                    after_cursor: str) -> dict[str, Any] | None:
        """Replay an already-applied durable command outcome after its receipt.

        If the original outcome is ahead of the caller's cursor, the ordinary
        ordered stream will deliver it. If already behind the caller's cursor,
        return the exact persisted CloudEvent for idempotent retransmission.
        """
        self._authorize(principal, factory_id)
        command_id = _string("command_id", command_id)
        if lifecycle not in {"applied", "rejected", "failed"}:
            raise SourceContractError("prior command outcome is not terminal")
        self.refresh(principal, factory_id)
        source_id = f"command:{command_id}:{lifecycle}"
        source_key = hashlib.sha256(
            f"{factory_id}\0command\0{source_id}".encode()).hexdigest()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            stream = db.execute("SELECT last_cursor, minimum_cursor FROM observation_streams "
                                "WHERE factory_id=?", (factory_id,)).fetchone()
            if stream is None:
                raise ObservationNotFound("unknown factory")
            sequence = self._decode_cursor(db, factory_id, after_cursor)
            if sequence is None or sequence < stream["minimum_cursor"] - 1:
                error = self._expired_cursor(db, factory_id, stream["minimum_cursor"],
                                             stream["last_cursor"])
                db.commit()
                raise error
            if sequence > stream["last_cursor"]:
                raise InvalidCursor("cursor is ahead of the stream")
            row = db.execute("SELECT projection_cursor, envelope_json "
                              "FROM observation_source_records WHERE factory_id=? AND source_key=?",
                              (factory_id, source_key)).fetchone()
            if row is None or row["envelope_json"] is None or row["projection_cursor"] is None:
                raise SourceContractError("durable prior command outcome event is unavailable")
            envelope = json.loads(row["envelope_json"])
            data = envelope.get("data", {})
            if (envelope.get("type") != "com.exomachina.command.outcome.v1"
                    or data.get("command_id") != command_id
                    or data.get("lifecycle") != lifecycle):
                raise SourceContractError("durable prior command outcome does not match its receipt")
            db.commit()
        return envelope if row["projection_cursor"] <= sequence else None

    def submit_command(self, principal: object, factory_id: str, command: Mapping[str, Any]) -> dict[str, Any]:
        """Delegate a command to the runtime authority; never authorize it here."""
        self._authorize(principal, factory_id)
        if not isinstance(command, Mapping):
            raise SourceContractError("command must be an object")
        allowed = {"command_id", "task_id", "context_id", "action", "expected_state",
                   "expected_revision", "expected_sha256"}
        required = {"command_id", "task_id", "context_id", "action", "expected_state"}
        if set(command) - allowed or not required <= set(command):
            raise SourceContractError("invalid command envelope")
        projected = {key: _string(key, command[key]) for key in required}
        if "expected_revision" in command:
            projected["expected_revision"] = _string("artifact_revision", command["expected_revision"])
        if "expected_sha256" in command:
            projected["expected_sha256"] = _string("artifact_sha256", command["expected_sha256"])
        dispatcher = getattr(self.source, "submit_command", None)
        if dispatcher is None:
            raise ObservationNotFound("command dispatch is unavailable")
        receipt = dispatcher(principal, factory_id, projected)
        if (not isinstance(receipt, Mapping) or receipt.get("command_id") != projected["command_id"]
                or receipt.get("lifecycle") != "received"):
            raise SourceContractError("runtime must return a durable received acknowledgement")
        result = {"command_id": projected["command_id"], "lifecycle": "received"}
        prior = receipt.get("prior_outcome")
        if prior is not None:
            if not isinstance(prior, Mapping):
                raise SourceContractError("runtime returned an invalid prior command outcome")
            lifecycle = prior.get("lifecycle")
            if not isinstance(lifecycle, str) or lifecycle not in {"applied", "rejected", "failed"}:
                raise SourceContractError("runtime returned a non-terminal prior command outcome")
            safe_prior: dict[str, str] = {"lifecycle": lifecycle}
            for key in ("outcome", "resulting_state"):
                if key in prior:
                    safe_prior[key] = _string(key, prior[key])
            result["prior_outcome"] = safe_prior
        # Never call this 'applied'; the authoritative command-outcome event is
        # projected later from the runtime's command journal.
        return result


def _normalize_freshness(value: Mapping[str, Any] | None) -> dict[str, Any]:
    if value is None:
        return {"status": "unknown", "observed_at": None}
    if not isinstance(value, Mapping):
        raise SourceContractError("source freshness must be an object")
    status = value.get("status", "unknown")
    if not isinstance(status, str) or status not in {"current", "fresh", "stale", "disconnected", "unknown"}:
        raise SourceContractError("invalid source freshness status")
    observed = value.get("observed_at")
    result = {"status": "fresh" if status == "current" else status,
              "observed_at": _utc_time(observed) if observed is not None else None}
    if any(key not in {"status", "observed_at", "scope", "run_id", "included_run_ids", "factory_status", "unavailable_run_ids"} for key in value):
        raise SourceContractError("invalid source freshness field")
    scope = value.get("scope")
    if scope is not None:
        if scope not in {"factory", "run"}:
            raise SourceContractError("invalid source freshness scope")
        result["scope"] = scope
    for key in ("included_run_ids", "unavailable_run_ids"):
        if key in value:
            rows = value[key]
            if not isinstance(rows, list) or len(rows) > 256 or any(not isinstance(item, str) or not _SAFE_ID.fullmatch(item) for item in rows) or len(set(rows)) != len(rows):
                raise SourceContractError("invalid freshness run IDs")
            result[key] = list(rows)
    if scope == "run":
        run_id = value.get("run_id")
        factory_status = value.get("factory_status")
        if not isinstance(run_id, str) or not _SAFE_ID.fullmatch(run_id) or run_id not in result.get("included_run_ids", []) or factory_status not in {"fresh", "stale", "disconnected", "unknown"}:
            raise SourceContractError("invalid freshness run binding")
        if result["status"] == "fresh" and set(result["included_run_ids"]) & set(result.get("unavailable_run_ids", [])):
            raise SourceContractError("fresh run includes unavailable history")
        result.update(run_id=run_id, factory_status=factory_status)
    elif "run_id" in value or "included_run_ids" in value or "factory_status" in value:
        raise SourceContractError("run freshness fields require run scope")
    return result


# Earlier lane notes used ObservationProjection; keep the descriptive alias
# while exposing FactoryObservation as the runtime-facing constructor.
ObservationProjection = FactoryObservation
