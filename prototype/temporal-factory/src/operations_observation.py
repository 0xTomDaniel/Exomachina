"""Allowlisted incident records sourced through FactoryOperations' public API.

Runtime Observation sources may merge ``project_operations_incidents`` output
into their durable source stream. This module never discovers factories,
opens Operations storage, or copies private incident history.
"""
from __future__ import annotations

from datetime import datetime
import re
from typing import Any, Mapping


_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}$")
_ENUM = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_MAX_LIST_LIMIT = 256
_MAX_EVENT_REFS = 100
_MAX_SQLITE_INTEGER = (1 << 63) - 1
_EVENT_TYPE = "com.exomachina.incident.state_changed.v1"


class OperationsObservationContractError(ValueError):
    """A public Operations row is not safe to project as an incident fact."""


def _identifier(field: str, value: Any) -> str:
    if not isinstance(value, str) or not _ID.fullmatch(value):
        raise OperationsObservationContractError(f"invalid incident {field}")
    return value


def _enum(field: str, value: Any) -> str:
    if not isinstance(value, str) or not _ENUM.fullmatch(value):
        raise OperationsObservationContractError(f"invalid incident {field}")
    return value


def _writer_time(value: Any) -> str:
    """Validate the writer timestamp while preserving its exact spelling."""
    if not isinstance(value, str) or not value or len(value) > 64:
        raise OperationsObservationContractError("invalid incident updated_at")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise OperationsObservationContractError("invalid incident updated_at") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise OperationsObservationContractError("incident updated_at must include a timezone")
    return value


def _evidence_refs(value: Any, *, field: str) -> list[str]:
    if not isinstance(value, list) or len(value) > _MAX_EVENT_REFS:
        raise OperationsObservationContractError(f"invalid incident {field}")
    for reference in value:
        if not isinstance(reference, str) or not _DIGEST.fullmatch(reference):
            raise OperationsObservationContractError("invalid incident evidence reference")
    return list(value)


def project_operations_incidents(operations: Any, principal: object,
                                 factory_id: str, *, limit: int) -> list[dict[str, Any]]:
    """Return safe Observation source facts from public incident list rows.

    ``operations`` must be the factory-bound ``FactoryOperations`` instance.
    The caller supplies the authenticated server-side principal and a finite
    list limit explicitly. Each source ID combines the incident ID and
    durable row version, so every state update has a stable, distinct source
    identity. ``time`` is copied exactly from the writer's ``updated_at``.
    """
    factory_id = _identifier("factory_id", factory_id)
    if type(limit) is not int or not 1 <= limit <= _MAX_LIST_LIMIT:
        raise OperationsObservationContractError(
            "incident list limit must be explicitly set from 1 to 256")
    if getattr(operations, "factory_id", None) != factory_id:
        raise OperationsObservationContractError(
            "Operations reader is bound to a different factory")
    reader = getattr(operations, "list_incidents", None)
    if not callable(reader):
        raise TypeError("operations must expose the public list_incidents method")

    rows = reader(principal, limit=limit)
    if not isinstance(rows, list) or len(rows) > limit:
        raise OperationsObservationContractError(
            "Operations returned an invalid incident list")

    projected = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise OperationsObservationContractError("Operations returned an invalid incident row")
        if row.get("factory_id") != factory_id:
            raise OperationsObservationContractError(
                "Operations returned an incident for a different factory")
        incident_id = _identifier("id", row.get("id"))
        run_id = _identifier("run_id", row.get("run_id"))
        failure_class = _enum("failure_class", row.get("failure_class"))
        state = _enum("state", row.get("state"))
        version = row.get("version")
        if (type(version) is not int or version < 1
                or version > _MAX_SQLITE_INTEGER):
            raise OperationsObservationContractError("invalid incident version")
        updated_at = _writer_time(row.get("updated_at"))
        references = _evidence_refs(row.get("evidence_refs"), field="evidence_refs")
        # Accepted artifact references are already exposed as safe digests by
        # FactoryOperations. Preserve those references without copying any
        # protected artifact content or the private row field itself.
        accepted_refs = _evidence_refs(
            row.get("protected_evidence_refs", []), field="protected_evidence_refs")
        references = list(dict.fromkeys([*references, *accepted_refs]))
        if len(references) > _MAX_EVENT_REFS:
            raise OperationsObservationContractError(
                "incident evidence_refs exceed the public event limit")

        fields: dict[str, Any] = {
            "incident_id": incident_id,
            "kind": failure_class,
            "state": state,
            "evidence_refs": references,
        }
        owner_id = row.get("owner_id")
        if owner_id is not None:
            fields["owner_identity"] = _identifier("owner_id", owner_id)

        projected.append({
            "source_kind": "incident",
            "source_id": f"{incident_id}:v{version}",
            "factory_id": factory_id,
            "run_id": run_id,
            "time": updated_at,
            "event_type": _EVENT_TYPE,
            "fields": fields,
        })
    return projected


__all__ = ["OperationsObservationContractError", "project_operations_incidents"]
