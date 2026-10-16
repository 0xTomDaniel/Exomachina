"""Engine-neutral data and checks for the arbitration trial's A2A services.

These functions describe test data and observable results, not a factory DSL or
an implementation of publication, scheduling, acceptance, or delivery.
"""
from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from typing import Any


CONTENT_PREFIX = "fixture-result:"
BRIEFS: dict[str, dict[str, Any]] = {
    "source_evidence": {
        "kind": "source_evidence@1",
        "question": "Can the customer use the documented capability?",
        "availability": "documented",
        "claims": [
            {"claim": "The capability is documented", "source": "source:A"},
            {"claim": "The local contract is versioned", "source": "source:B"},
        ],
    },
    "counter_evidence": {
        "kind": "counter_evidence@1",
        "question": "What limits the same customer use?",
        "scope_status": "requires_scope",
        "objections": [
            {"objection": "A documented limit requires explicit scope", "source": "source:C"},
            {"objection": "A missing receipt must be reconciled", "source": "source:D"},
        ],
    },
}


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def branch_brief(instance: str, result_type: str, *,
                 scope_status: str | None = None,
                 question: str | None = None) -> dict[str, Any]:
    """Create one named instance of either already-declared result type."""
    if not instance or not isinstance(instance, str) or result_type not in BRIEFS:
        raise ValueError("unknown branch instance or result type")
    value = deepcopy(BRIEFS[result_type])
    value["instance"] = instance
    if question is not None:
        if not isinstance(question, str):
            raise ValueError("question must be a string")
        value["question"] = question
    if scope_status is not None:
        if result_type != "counter_evidence" or scope_status not in {"requires_scope", "clear"}:
            raise ValueError("invalid scope_status for branch result type")
        value["scope_status"] = scope_status
    return value


def assignment(run_id: str, definition_digest: str, instance: str, *,
               result_type: str | None = None, scope_status: str | None = None,
               question: str | None = None) -> dict[str, Any]:
    """Make a stable logical assignment for an existing Strands/A2A receiver."""
    kind = result_type or instance
    brief = branch_brief(instance, kind, scope_status=scope_status, question=question)
    return {
        "op": "assign", "action_id": f"{run_id}:{instance}",
        "run_id": run_id, "definition_digest": definition_digest,
        "brief": canonical(brief),
    }


def branch_value(receipt: dict[str, Any], instance: str, *, run_id: str,
                 definition_digest: str, result_type: str | None = None,
                 scope_status: str | None = None,
                 question: str | None = None) -> dict[str, Any]:
    """Validate one A2A result and decode its typed application payload."""
    kind = result_type or instance
    expected = branch_brief(instance, kind, scope_status=scope_status, question=question)
    if receipt.get("action_id") != f"{run_id}:{instance}":
        raise ValueError("branch action binding mismatch")
    if receipt.get("run_id") != run_id or receipt.get("definition_digest") != definition_digest:
        raise ValueError("branch run/definition binding mismatch")
    artifact = receipt.get("artifact")
    if not isinstance(artifact, dict) or not isinstance(artifact.get("content"), str):
        raise ValueError("missing branch artifact")
    content = artifact["content"]
    if artifact.get("sha256") != sha256_text(content):
        raise ValueError("branch artifact digest mismatch")
    if not isinstance(artifact.get("author"), str) or not artifact["author"]:
        raise ValueError("missing branch author")
    if not content.startswith(CONTENT_PREFIX):
        raise ValueError("unexpected branch artifact content")
    try:
        value = json.loads(content[len(CONTENT_PREFIX):])
    except json.JSONDecodeError as error:
        raise ValueError("branch content is not JSON") from error
    if value != expected:
        raise ValueError(f"wrong {instance} result type or data")
    return value


def typed_join(receipts: dict[str, dict[str, Any]], *, run_id: str,
               definition_digest: str, declarations: dict[str, str] | None = None,
               scope_status_by_instance: dict[str, str] | None = None,
               question: str | None = None) -> dict[str, Any]:
    """Join both useful branch types only after their exact results validate."""
    declared = declarations or {"source_evidence": "source_evidence",
                                "counter_evidence": "counter_evidence"}
    scopes = scope_status_by_instance or {}
    if set(receipts) != set(declared) or set(declared.values()) != set(BRIEFS):
        raise ValueError("join requires declared source and counterevidence results")
    values = {instance: branch_value(receipts[instance], instance,
              run_id=run_id, definition_digest=definition_digest,
              result_type=result_type, scope_status=scopes.get(instance),
              question=question)
              for instance, result_type in declared.items()}
    claims = [{**claim, "branch_instance": instance}
              for instance, value in values.items()
              if declared[instance] == "source_evidence" for claim in value["claims"]]
    objections = [{**objection, "branch_instance": instance}
                  for instance, value in values.items()
                  if declared[instance] == "counter_evidence" for objection in value["objections"]]
    requires_scope = any(value["scope_status"] == "requires_scope"
                         for instance, value in values.items()
                         if declared[instance] == "counter_evidence")
    return {"kind": "evidence_join@1", "run_id": run_id,
            "definition_digest": definition_digest,
            "claims": claims, "objections": objections,
            "route_status": "requires_scope" if requires_scope else "clear",
            "requires_scope": requires_scope,
            "branch_artifact_sha256": {
                name: receipts[name]["artifact"]["sha256"] for name in sorted(declared)
            }}


def candidate_artifact(join: dict[str, Any], revision: str, author: str,
                       *, resolved: bool) -> dict[str, str]:
    """Make a valid candidate that Quality may reject for unresolved objections."""
    if join.get("kind") != "evidence_join@1" or not join.get("claims") or not join.get("objections"):
        raise ValueError("candidate requires a typed evidence join")
    if not isinstance(author, str) or not author:
        raise ValueError("missing candidate author")
    body = {"kind": "verified_research_candidate@1", "revision": revision,
            "join": join, "objections_resolved": resolved,
            "assessment": "documented limits addressed" if resolved else "limits unresolved"}
    content = canonical(body)
    return {"revision": revision, "sha256": sha256_text(content),
            "author": author, "content": content}


def quality_decision(artifact: dict[str, Any], reviewer: str) -> tuple[bool, str]:
    """Deterministic independent Quality rule used by the local service."""
    if not isinstance(reviewer, str) or not reviewer or artifact.get("author") == reviewer:
        return False, "author cannot review own artifact"
    content = artifact.get("content")
    if not isinstance(content, str) or artifact.get("sha256") != sha256_text(content):
        return False, "artifact content digest mismatch"
    try:
        body = json.loads(content)
    except json.JSONDecodeError:
        return False, "artifact content is not JSON"
    if not isinstance(body, dict):
        return False, "candidate content is not an object"
    join = body.get("join")
    if (body.get("kind") != "verified_research_candidate@1"
            or body.get("revision") != artifact.get("revision")
            or not isinstance(join, dict)
            or join.get("kind") != "evidence_join@1"
            or join.get("route_status") not in {"requires_scope", "clear"}
            or join.get("requires_scope") is not (join.get("route_status") == "requires_scope")
            or not isinstance(body.get("objections_resolved"), bool)):
        return False, "candidate contract mismatch"
    if not body.get("objections_resolved"):
        return False, "counterevidence remains unresolved"
    return True, "counterevidence addressed for exact revision"
