"""Pure public Task projection and failure incident decisions."""
from __future__ import annotations


def project(status: dict | None, execution: str, result: dict | None = None) -> str:
    if execution in {"FAILED", "TERMINATED", "TIMED_OUT", "CANCELED"}:
        return "failed"
    if status and (status.get("phase") == "child-failed" or
                   (result and result.get("status") == "failed")):
        return "failed"
    if execution == "COMPLETED" and result and result.get("status") in {
            "accepted", "aborted", "expired"}:
        return "completed"
    if status and status.get("phase") in {"accepted", "child-aborted", "child-expired"}:
        return "completed"
    if status and status.get("phase") in {"awaiting-child", "awaiting-director"}:
        return "input-required"
    return "working"


def failure_incident(run_id: str, child_id: str | None, package_digest: str,
                     failure_class: str, timestamp: str,
                     acceptance: dict | None, release_receipt: dict | None) -> dict:
    return {"run_id": run_id, "child_id": child_id,
            "package_digest": package_digest, "failure_class": failure_class,
            "timestamp": timestamp,
            "authority_conflict": acceptance is not None or release_receipt is not None}


def no_effect_failure_path(acceptance: dict | None, release_receipt: dict | None) -> bool:
    """Only this path supports a no-acceptance/no-release claim."""
    return acceptance is None and release_receipt is None


def decide_closed_failed_child(*, acceptance: dict | None,
                               release_receipt: dict | None,
                               release_outcome: object | None,
                               action_id: str, run_id: str,
                               definition_digest: str) -> dict:
    """Classify a closed failed child without authorizing any new release send.

    release_outcome follows lane 4's OutcomeRecord interface. Its dispatch-intent
    row is already ambiguous until confirmed; only action-ID lookup may resolve it.
    """
    decision = {"public_task_state": "failed", "may_submit_release": False,
                "preserve_acceptance": acceptance is not None,
                "preserve_release_receipt": release_receipt is not None,
                "acceptance": acceptance, "release_receipt": release_receipt}
    if release_outcome is not None:
        if getattr(release_outcome, "may_submit", True):
            raise ValueError("release outcome does not prohibit resubmission")
        if (getattr(release_outcome, "action_id", None) != action_id
                or getattr(release_outcome, "run_id", None) != run_id
                or getattr(release_outcome, "definition_digest", None) != definition_digest):
            return {**decision, "incident_class": "release-binding-conflict",
                    "reconciliation": "manual-incident"}
    if acceptance is None:
        if release_receipt is not None or release_outcome is not None:
            return {**decision, "incident_class": "release-without-acceptance",
                    "reconciliation": "manual-incident"}
        return {**decision, "incident_class": "child-failure-before-acceptance",
                "reconciliation": "none"}
    if release_receipt is not None:
        return {**decision, "incident_class": "post-acceptance-after-release",
                "reconciliation": "preserve-receipt"}
    if release_outcome is None:
        return {**decision, "incident_class": "post-acceptance-release-unaccounted",
                "reconciliation": "investigate-journal"}
    phase = str(release_outcome.phase)
    receiver = str(release_outcome.receiver)
    if phase == "confirmed":
        if getattr(release_outcome, "receipt", None) is not None:
            return {**decision, "preserve_release_receipt": True,
                    "release_receipt": release_outcome.receipt,
                    "incident_class": "post-acceptance-after-release",
                    "reconciliation": "preserve-receipt"}
        return {**decision, "incident_class": "post-acceptance-release-incident",
                "reconciliation": "manual-incident"}
    if phase in {"submitted", "unknown"} and receiver == "participating":
        return {**decision, "incident_class": "post-acceptance-release-outcome-unknown",
                "reconciliation": "lookup-by-action-id-only"}
    return {**decision, "incident_class": "post-acceptance-release-incident",
            "reconciliation": "manual-incident"}
