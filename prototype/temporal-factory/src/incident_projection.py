"""Deterministic public projection of a bounded factory incident."""
from __future__ import annotations

from typing import Any, Mapping


def incident_result(run_id: str, definition_digest: str, kind: str,
                    evidence: Mapping[str, Any]) -> dict[str, Any]:
    if not all(isinstance(x, str) and x for x in (run_id, definition_digest, kind)):
        raise ValueError("missing incident binding")
    return {"status": "incident", "run": run_id,
            "definition_digest": definition_digest,
            "incident": {"kind": kind, "evidence": dict(evidence)},
            "released": False}


def public_task_state(phase: str, result_status: str | None = None) -> str:
    if result_status is None and phase in {"child-incident", "accepted", "child-aborted", "child-expired"}:
        # DirectorTaskStore first chooses a provisional state, then fetches the
        # terminal Workflow result and calls this function again with its status.
        return "working"
    if phase == "child-incident":
        if result_status != "incident":
            raise ValueError("incident phase/result mismatch")
        return "failed"
    if phase in {"accepted", "child-aborted", "child-expired"}:
        if result_status not in {"accepted", "aborted", "expired"}:
            raise ValueError("terminal phase/result mismatch")
        return "completed"
    if phase in {"awaiting-child", "awaiting-director"}:
        return "input-required"
    return "working"
