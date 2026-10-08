"""Safe projection of the Runtime's current public Director wait view.

The view is polled only to enrich a wait already backed by an exact original
Task/run binding. It is never used to invent a source timestamp or infer a
workflow transition. The append-only source identity is the run, wait phase,
and writer-recorded wait start.
"""
from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
import math
from typing import Any

from observation import SourceContractError, project_source_record


WAIT_EVENT = "com.exomachina.run.state_changed.v1"
WAIT_PHASES = {
    "awaiting-director": "director",
    "awaiting-human": "human",
}
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}$")
_SAFE_DIGEST = re.compile(r"^[a-f0-9]{64}$")


def _deadline_iso(value: Any) -> str | None:
    """Normalize the Inspector's writer deadline without consulting a clock."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SourceContractError("wait view has an invalid deadline")
    try:
        finite = math.isfinite(value)
    except OverflowError as error:
        raise SourceContractError("wait view deadline is outside timestamp range") from error
    if not finite:
        raise SourceContractError("wait view deadline must be finite")
    try:
        timestamp = datetime.fromtimestamp(value, timezone.utc)
    except (OverflowError, OSError, ValueError) as error:
        raise SourceContractError("wait view deadline is outside timestamp range") from error
    return timestamp.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def project_public_wait_records(
    director: Any,
    factory_id: str,
    rows: Sequence[Mapping[str, Any]],
    binding_by_run: Mapping[str, tuple[str, str]],
) -> list[tuple[dict[str, Any], dict[str, str]]]:
    """Return validated wait records and any exact candidate refs observed.

    ``rows`` are Director-owned run rows. Their original Task IDs are passed
    to ``inspect_bound_run``; the returned run ID is accepted only when the
    Temporal-derived binding map proves the same Task and context. Candidate
    revision/hash values are returned separately for private immutable-source
    consistency checks and are never included in the CloudEvent.
    """
    inspect = getattr(director, "inspect_bound_run", None)
    if not callable(inspect):
        return []

    projected: list[tuple[dict[str, Any], dict[str, str]]] = []
    seen: set[tuple[str, str, str]] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        original_run_id = row.get("run_id")
        task_id, context_id = row.get("task_id"), row.get("context_id")
        if not all(isinstance(value, str) and value for value in
                   (original_run_id, task_id, context_id)):
            continue
        if binding_by_run.get(original_run_id) != (task_id, context_id):
            continue

        # Runtime's public inspector is bound to the original A2A Task. A
        # child run is accepted only through the exact Temporal-derived map.
        view = inspect(task_id)
        if not isinstance(view, Mapping):
            continue
        run_id = view.get("run_id")
        if not isinstance(run_id, str) or binding_by_run.get(run_id) != (task_id, context_id):
            continue
        phase = view.get("phase")
        role = WAIT_PHASES.get(phase) if isinstance(phase, str) else None
        if role is None:
            # A non-waiting query is not a new durable fact. Terminal history
            # remains responsible for closing the prior wait annotations.
            continue

        started_at = view.get("wait_started_at")
        deadline_value = view.get("wait_deadline")
        if deadline_value is None:
            deadline_value = view.get("deadline")
        deadline = _deadline_iso(deadline_value)
        actor = view.get("decision_actor")
        node = view.get("node")
        actions = view.get("permitted_actions")
        # Missing query facts stay unknown. Do not borrow a Task principal,
        # timer event, capture time, or guessed graph node to fill them.
        if not all(isinstance(value, str) and value for value in
                   (started_at, deadline, actor, node)) or not isinstance(actions, list):
            continue

        identity = (run_id, phase, started_at)
        if identity in seen:
            continue
        seen.add(identity)
        source_id = f"{run_id}:wait:{phase}:{started_at}"
        fields = {
            "state": "input-required",
            "phase": phase,
            "node": node,
            "wait_role": role,
            "wait_actor_identity": actor,
            "wait_started_at": started_at,
            "wait_deadline": deadline,
            "permitted_actions": actions,
        }
        record = {
            "factory_id": factory_id,
            "source_kind": "temporal",
            "source_id": source_id,
            "event_type": WAIT_EVENT,
            "time": started_at,
            "run_id": run_id,
            "task_id": task_id,
            "context_id": context_id,
            "fields": fields,
        }

        candidate: dict[str, str] = {}
        revision = view.get("current_revision")
        digest = view.get("current_sha256")
        if revision is not None:
            if not isinstance(revision, str) or not _SAFE_ID.fullmatch(revision):
                raise SourceContractError("wait view has an invalid current revision")
            candidate["current_revision"] = revision
        if digest is not None:
            if not isinstance(digest, str) or not _SAFE_DIGEST.fullmatch(digest):
                raise SourceContractError("wait view has an invalid current artifact digest")
            candidate["current_sha256"] = digest

        # Use the Observation allowlist and schema-normalization path before
        # the source adapter persists this fact.
        project_source_record(record, factory_id)
        projected.append((record, candidate))
    return projected
