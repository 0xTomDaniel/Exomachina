"""Allowlisted Observation facts sourced from the public AdmissionQueue API.

Runtime supplies an authoritative Task binding lookup. This module neither
opens queue storage nor derives run or context identities from queue IDs.
"""
from __future__ import annotations

from datetime import datetime
import re
from typing import Any, Callable, Mapping


_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}$")
_EVENT_TYPE = "com.exomachina.admission.state_changed.v1"
_MAX_CAPACITY = (1 << 63) - 1
BindingLookup = Callable[[str], tuple[str, str]]


class AdmissionObservationContractError(ValueError):
    """A public AdmissionQueue row or Task binding is not safe to project."""


def _identifier(field: str, value: Any) -> str:
    if not isinstance(value, str) or not _ID.fullmatch(value):
        raise AdmissionObservationContractError(f"invalid admission {field}")
    return value


def _nonnegative_integer(field: str, value: Any) -> int:
    if type(value) is not int or value < 0 or value > _MAX_CAPACITY:
        raise AdmissionObservationContractError(f"invalid admission {field}")
    return value


def _writer_time(field: str, value: Any) -> str:
    """Validate the durable timestamp without changing the writer's spelling."""
    if not isinstance(value, str) or not value or len(value) > 64:
        raise AdmissionObservationContractError(f"invalid admission {field}")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise AdmissionObservationContractError(f"invalid admission {field}") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise AdmissionObservationContractError(f"admission {field} must include a timezone")
    return value


def _source_id(request_id: str, state: str) -> str:
    # Length-prefixing makes the request/state pair unambiguous even when a
    # caller-owned request ID itself contains a colon.
    return f"admission:{len(request_id)}:{request_id}:{state}"


def project_admission_observation(queue: Any, factory_id: str,
                                  binding_lookup: BindingLookup) -> dict[str, Any]:
    """Project current queue rows and capacity through their public readers.

    Returns ``{"records": [...], "capacity": {...}}``. Admission records
    are durable Observation source facts. Capacity is a current normalized
    snapshot because ``capacity_view`` has no durable writer timestamp; this
    function does not invent a CloudEvent time or source identity for it.
    """
    factory_id = _identifier("factory_id", factory_id)
    if getattr(queue, "factory_id", None) != factory_id:
        raise AdmissionObservationContractError(
            "AdmissionQueue is bound to a different factory")
    list_requests = getattr(queue, "list_requests", None)
    capacity_reader = getattr(queue, "capacity_view", None)
    if not callable(list_requests) or not callable(capacity_reader):
        raise TypeError("queue must expose list_requests and capacity_view")
    if not callable(binding_lookup):
        raise TypeError("binding_lookup must resolve an authoritative Task binding")

    capacity_view = capacity_reader()
    if not isinstance(capacity_view, Mapping) or capacity_view.get("factory_id") != factory_id:
        raise AdmissionObservationContractError(
            "AdmissionQueue returned a capacity view for a different factory")
    capacity_limit = _nonnegative_integer("capacity", capacity_view.get("capacity"))
    active_count = _nonnegative_integer("admitted_count", capacity_view.get("admitted_count"))
    queued_count = _nonnegative_integer("queued_count", capacity_view.get("queued_count"))
    if active_count > capacity_limit:
        raise AdmissionObservationContractError("admitted count exceeds configured capacity")
    if "available_slots" in capacity_view:
        available = _nonnegative_integer("available_slots", capacity_view["available_slots"])
        if available != capacity_limit - active_count:
            raise AdmissionObservationContractError("capacity view counts are inconsistent")

    rows = list_requests()
    if not isinstance(rows, list):
        raise AdmissionObservationContractError("AdmissionQueue returned an invalid request list")

    records: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise AdmissionObservationContractError("AdmissionQueue returned an invalid request row")
        if row.get("factory_id") != factory_id:
            raise AdmissionObservationContractError(
                "AdmissionQueue returned a request for a different factory")
        request_id = _identifier("request_id", row.get("request_id"))
        task_id = _identifier("task_id", row.get("task_id"))
        state = row.get("state")
        if not isinstance(state, str) or state not in {"queued", "admitted", "released"}:
            raise AdmissionObservationContractError("invalid admission state")
        row_capacity = _nonnegative_integer("row capacity", row.get("capacity"))
        if row_capacity != capacity_limit:
            raise AdmissionObservationContractError(
                "request capacity differs from the current factory capacity")

        try:
            binding = binding_lookup(task_id)
        except (KeyError, LookupError) as error:
            raise AdmissionObservationContractError(
                "AdmissionQueue Task has no Runtime binding") from error
        if not isinstance(binding, tuple) or len(binding) != 2:
            raise AdmissionObservationContractError(
                "binding_lookup must return (actual_run_id, context_id)")
        run_id = _identifier("bound run_id", binding[0])
        context_id = _identifier("bound context_id", binding[1])

        if state == "queued":
            transitions = [("queued", "created_at")]
        elif state == "admitted":
            # created_at does not prove that an admitted request ever waited.
            transitions = [("admitted", "admitted_at")]
        else:
            # A released row retains the durable admission transition. Emit it
            # before release so a consumer that first observes the row later
            # still sees the actual slot grant, without inventing a queued fact.
            transitions = [("admitted", "admitted_at"), ("released", "released_at")]

        for transition, time_field in transitions:
            event_time = _writer_time(time_field, row.get(time_field))
            records.append({
                "source_kind": "admission",
                "source_id": _source_id(request_id, transition),
                "factory_id": factory_id,
                "run_id": run_id,
                "task_id": task_id,
                "context_id": context_id,
                "time": event_time,
                "event_type": _EVENT_TYPE,
                "fields": {
                    "admission_id": request_id,
                    "state": transition,
                    "capacity_limit": row_capacity,
                },
            })

    return {
        "records": records,
        "capacity": {
            "factory_id": factory_id,
            "capacity_limit": capacity_limit,
            "active_count": active_count,
            "queued_count": queued_count,
        },
    }


__all__ = ["AdmissionObservationContractError", "project_admission_observation"]
