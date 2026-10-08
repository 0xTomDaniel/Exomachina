"""Pure Quality evidence decision for the bounded Temporal candidate.

The decision is made from the factory's own evidence: its pinned bindings, its
outcome-journal receipt for the Quality Task, and the verdict content. The
Quality agent echoes no factory identifier and no identity.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Mapping



class QualityKind(StrEnum):
    POSITIVE = "positive"
    NEGATIVE = "negative"
    INCONSISTENT = "inconsistent"


@dataclass(frozen=True)
class QualityDecision:
    kind: QualityKind
    reasons: tuple[str, ...]
    verdict: Mapping[str, Any] | None = None
    incident: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {"kind": self.kind.value, "reasons": list(self.reasons),
                "verdict": dict(self.verdict) if self.verdict is not None else None,
                "incident": self.incident}


def quality_action_id(run_id: str, assignment_id: str, attempt: int,
                      revision: str, sha256: str) -> str:
    if not all(isinstance(x, str) and x for x in (run_id, assignment_id, revision, sha256)):
        raise ValueError("empty Quality action component")
    if type(attempt) is not int or attempt < 1:
        raise ValueError("invalid Quality attempt")
    return f"{run_id}:quality:{assignment_id}:{attempt}:{revision}:{sha256}"


def decide_quality_async(*, binding: Mapping[str, Any], command: Mapping[str, Any],
                         candidate: Mapping[str, Any], receipt: Mapping[str, Any],
                         verdict: Mapping[str, Any], expected_task_id: str) -> QualityDecision:
    """Bind the async Task journal receipt and decoded verdict to one candidate.

    Author/reviewer independence is decided from the factory's own bindings:
    the pinned Quality identity against the candidate's pinned author.
    """
    problems = []
    if binding.get("role") != "quality" or binding.get("approved") is not True:
        problems.append("pinned-quality-binding")
    identity = binding.get("identity")
    if identity == candidate.get("author") or not identity:
        problems.append("author-quality-independence")
    if (receipt.get("action_id") != command.get("action_id")
            or receipt.get("run_id") != command.get("run_id")
            or receipt.get("definition_digest") != command.get("definition_digest")
            or receipt.get("task_id") != expected_task_id
            or receipt.get("harness_identity") != identity
            or receipt.get("harness_role") != "quality"):
        problems.append("task-journal-binding")
    # The normalized record is the factory's own: its author is the pinned
    # Quality identity and its revision the one the factory assigned. The
    # verdict names the candidate by the revision and sha256 the agent read
    # from the Part it received; they must be the accepted candidate's.
    wire = receipt.get("artifact") or {}
    if (wire.get("author") != identity or wire.get("revision") != candidate.get("revision")
            or verdict.get("candidate") != {key: candidate.get(key) for key in
                                              ("revision", "sha256")}):
        problems.append("verdict-candidate-binding")
    if problems:
        return QualityDecision(QualityKind.INCONSISTENT, tuple(problems),
                               incident="quality-evidence-inconsistent")
    return QualityDecision(QualityKind.POSITIVE if verdict["accepted"] else QualityKind.NEGATIVE,
                           (), verdict)
