"""Pure Quality evidence decision for the bounded Temporal candidate.

The fixture closure pins URL and service identity, but has no Agent Card digest or
service key. This function does not claim to authenticate either absent field.
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
    """Bind the async Task journal receipt and decoded verdict to one candidate."""
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
    wire = receipt.get("artifact") or {}
    if (wire.get("author") != identity or wire.get("revision") != candidate.get("revision")
            or verdict.get("reviewer") != identity
            or verdict.get("candidate") != {key: candidate.get(key) for key in
                                              ("revision", "sha256", "author")}):
        problems.append("verdict-candidate-binding")
    if problems:
        return QualityDecision(QualityKind.INCONSISTENT, tuple(problems),
                               incident="quality-evidence-inconsistent")
    return QualityDecision(QualityKind.POSITIVE if verdict["accepted"] else QualityKind.NEGATIVE,
                           (), verdict)


def _task_verdict(task: Mapping[str, Any]) -> Mapping[str, Any] | None:
    try:
        artifacts = task["artifacts"]
        if len(artifacts) != 1 or len(artifacts[0]["parts"]) != 1:
            return None
        verdict = artifacts[0]["parts"][0]["data"]
        return verdict if isinstance(verdict, Mapping) else None
    except (KeyError, TypeError, IndexError):
        return None


def decide_quality(*, binding: Mapping[str, Any], observed_endpoint: str,
                   observed_identity: str, command: Mapping[str, Any],
                   assignment_id: str, attempt: int,
                   task: Mapping[str, Any], lookup: Mapping[str, Any],
                   send_payload: Mapping[str, Any] | None = None) -> QualityDecision:
    """Fail closed on any disagreement before Workflow records acceptance.

    ``task`` is the actual A2A Task (send response or tasks/get), ``lookup`` is
    the independent caller-action-ID record. ``send_payload`` preserves the
    first message/send verdict when a subsequent tasks/get is also performed.
    """
    problems: list[str] = []
    artifact = command.get("artifact")
    if not isinstance(artifact, Mapping):
        artifact = {}
        problems.append("missing-candidate")
    expected_run = command.get("run_id")
    expected_definition = command.get("definition_digest")
    expected_revision = artifact.get("revision")
    expected_sha = artifact.get("sha256")
    try:
        expected_action = quality_action_id(expected_run, assignment_id, attempt,
                                            expected_revision, expected_sha)
    except ValueError:
        expected_action = None
        problems.append("invalid-review-context")
    if command.get("op") != "review" or command.get("action_id") != expected_action:
        problems.append("action-assignment-attempt")
    if (binding.get("approved") is not True or binding.get("role") != "quality"
            or binding.get("identity") != observed_identity
            or binding.get("url") != observed_endpoint):
        problems.append("pinned-quality-identity-endpoint")
    if artifact.get("author") == observed_identity or not artifact.get("author"):
        problems.append("author-quality-independence")
    metadata = task.get("metadata")
    if not isinstance(metadata, Mapping):
        metadata = {}
    status = task.get("status")
    if not isinstance(status, Mapping) or status.get("state") != "completed":
        problems.append("task-not-completed")
    for key, expected in (("action_id", expected_action), ("run_id", expected_run),
                          ("definition_digest", expected_definition)):
        if metadata.get(key) != expected or lookup.get(key) != expected:
            problems.append("task-lookup-" + key)
    if (metadata.get("harness_identity") != observed_identity
            or metadata.get("harness_role") != "quality"
            or lookup.get("role") != "quality"):
        problems.append("task-lookup-quality-identity")
    if not task.get("id") or lookup.get("task_id") != task.get("id"):
        problems.append("task-lookup-id")
    verdict = _task_verdict(task)
    lookup_verdict = lookup.get("artifact")
    if verdict is None or not isinstance(lookup_verdict, Mapping):
        problems.append("missing-verdict")
    elif dict(verdict) != dict(lookup_verdict):
        problems.append("task-lookup-verdict-disagreement")
    if send_payload is not None and (verdict is None or dict(send_payload) != dict(verdict)):
        problems.append("send-task-verdict-disagreement")
    if verdict is not None:
        if (verdict.get("reviewer") != observed_identity
                or verdict.get("revision") != expected_revision
                or verdict.get("sha256") != expected_sha
                or type(verdict.get("accepted")) is not bool):
            problems.append("verdict-identity-artifact-digest")
        try:
            if task["artifacts"][0]["artifactId"] != expected_sha:
                problems.append("task-artifact-id")
        except (KeyError, TypeError, IndexError):
            problems.append("task-artifact-id")
    if problems:
        return QualityDecision(QualityKind.INCONSISTENT, tuple(dict.fromkeys(problems)),
                               incident="quality-evidence-inconsistent")
    return QualityDecision(QualityKind.POSITIVE if verdict["accepted"] else QualityKind.NEGATIVE,
                           (), verdict)
