"""A2A and release Activities. Remote receipts never become product acceptance here.

Agent services receive ordinary A2A Messages (decision 7). The factory composes
the brief, journals its own action, run, assignment and attempt against the
A2A ``messageId``/``contextId``/``taskId`` in the outcome journal, re-attaches
with ``GetTask`` after a restart, and records agent-reported usage from the
budget extension (decision 8) in its own usage journal.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import time
import urllib.error
import urllib.request
from pathlib import Path
from uuid import uuid4

from temporalio import activity

from a2a_outcome import (EffectKind, OutcomeJournal, Phase, ReceiverKind, lookup_result,
                         send_ambiguous, send_completed, submitted, task_started,
                         task_finished, task_incident, StaleOutcome)
from quality_authority import QualityKind, quality_action_id
from report_contract import (canonical, packet_evidence_join, research_assignment,
    synthesis_assignment, quality_review_request, validate_research_result,
    validate_report, validate_verdict)
import a2a_extensions
import long_client as a2a
import fixture
import handoff
import release_delivery
from definition import binding_output
from model_usage import AGENT_USAGE_DATABASE, ModelUsageJournal, normalize_agent_tokens


class PendingTask(Exception):
    """Retry the Activity; its remote Task id is durable in the journal."""


def _assignment_usage_bindings(input: dict) -> dict[str, str]:
    """Factory-side measurement bindings; they stay in the factory's journals."""
    bindings = {}
    for name in ("assignment_id", "attempt_id"):
        value = input.get(name)
        if value is None:
            continue
        if not isinstance(value, str) or not value:
            raise ValueError(f"{name} must be a non-empty explicit identifier")
        bindings[name] = value
    factory_id = input.get("factory_id")
    if factory_id is not None:
        if (not isinstance(factory_id, str)
                or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}", factory_id) is None):
            raise ValueError("factory_id must be a safe identifier")
        bindings["factory_id"] = factory_id
    return bindings


def _async_unresolved(record) -> dict:
    return {"unresolved": record.reason, "action_id": record.action_id,
            "task_id": record.task_id}


def _wire_binding(record) -> dict:
    """The factory's journal binding for one A2A exchange."""
    return {"action_id": record.action_id, "run_id": record.run_id,
            "definition_digest": record.definition_digest, "task_id": record.task_id,
            "context_id": record.context_id, "message_id": record.message_id}


def agent_usage_journal(outcome_db: Path | str) -> ModelUsageJournal:
    return ModelUsageJournal(Path(outcome_db).parent / AGENT_USAGE_DATABASE)


def _record_agent_usage(task: dict, action: dict, identity: str) -> None:
    """On-complete hook: record the agent's own report as agent-reported usage.

    An absent or malformed report is recorded as unavailable, never zero.
    Recording never changes the Task outcome.
    """
    journal_path = os.environ.get("EXO_OUTCOME_DB")
    if not journal_path:
        return
    try:
        report = a2a_extensions.parse_incurred(task.get("metadata"))
        status = "reported" if report is not None else "absent"
    except ValueError:
        report, status = None, "malformed"
    try:
        agent_usage_journal(journal_path).record(
            model_call_id=f"urn:exomachina:agent-task:{identity}:{task['id']}",
            service_identity=identity, task_id=task["id"], call_scope="assignment_call",
            assignment_id=action.get("assignment_id"), attempt_id=action.get("attempt_id"),
            action_id=action["action_id"], run_id=action["run_id"],
            definition_digest=action["definition_digest"],
            provider="a2a-agent", model_id="unreported", reasoning_effort=None,
            usage=normalize_agent_tokens((report or {}).get("tokens")), source="agent")
        _log("agent-usage-recorded", action_id=action["action_id"], task_id=task["id"],
             report=status)
    except Exception as error:
        _log("agent-usage-unrecorded", action_id=action["action_id"], task_id=task["id"],
             report=status, error_type=type(error).__name__)


def _invoke_async(binding: dict, contract: dict, action: dict,
                  expected_revision: str, role: str,
                  expected_capability: str | None = None) -> dict:
    journal_path = os.environ.get("EXO_OUTCOME_DB")
    if not journal_path:
        raise RuntimeError("durable A2A outcome journal is not configured")
    mode = contract.get("reconcile")
    if mode not in {"a2a-idempotent-resend", "opaque"}:
        raise ValueError("async receiver reconciliation mode is undeclared")
    path = Path(journal_path)
    snapshot = path.parent.parent / "testbed" / "agent_snapshot.json"
    brief = action["brief"]
    payload_sha256 = hashlib.sha256(brief.encode()).hexdigest()
    journal = OutcomeJournal(path)
    try:
        context_id = journal.context_for(action["run_id"], binding["identity"])
        expected = submitted(action["action_id"], action["run_id"],
            action["definition_digest"],
            ReceiverKind.PARTICIPATING if mode == "a2a-idempotent-resend" else ReceiverKind.OPAQUE,
            payload_sha256=payload_sha256, pinned_identity=binding["identity"],
            message_id=str(uuid4()), context_id=context_id)
        record, created = journal.begin(expected)
        if record.phase == Phase.CONFIRMED:
            return record.receipt
        if record.phase == Phase.INCIDENT:
            return _async_unresolved(record)
        if record.message_id is None or record.context_id is None:
            record = task_incident(record, "a2a-correlation-missing")
            journal.put(record)
            return _async_unresolved(record)
        if record.task_id is None and not created and mode != "a2a-idempotent-resend":
            record = task_incident(record, "opaque-effect-unknown")
            journal.put(record)
            return _async_unresolved(record)
        deadline = time.monotonic() + 70
        while time.monotonic() < deadline:
            try:
                url, observed = a2a.resolve_pinned(snapshot, binding["identity"], contract)
            except (a2a.UncertainSubmission, a2a.UnavailableBinding):
                time.sleep(0.5)
                continue
            except Exception as error:
                record = task_incident(record, "pinned-agent-verification-failed")
                journal.put(record)
                _log("agent-pin-incident", action_id=record.action_id,
                     pinned_identity=binding["identity"], error_type=type(error).__name__)
                return _async_unresolved(record)
            if (expected_capability is not None
                    and expected_capability not in observed["skills"]):
                record = task_incident(record, "pinned-agent-capability-mismatch")
                journal.put(record)
                return _async_unresolved(record)
            _log("agent-card-verified", action_id=record.action_id,
                 pinned_identity=binding["identity"], pinned_card_sha256=contract["card_sha256"],
                 observed=observed)
            if record.task_id is None:
                # First dispatch and replay send the same journal-bound Message:
                # same messageId, contextId and brief.
                try:
                    task = a2a.send_async(url, brief, message_id=record.message_id,
                                          context_id=record.context_id)
                    state = a2a.validate_async_task(task, context_id=record.context_id,
                                                    identity=binding["identity"])
                    record = task_started(record, task["id"])
                    journal.put(record)
                    _log("agent-task-journaled", action_id=record.action_id,
                         task_id=record.task_id, context_id=record.context_id,
                         message_id=record.message_id, state=state, url=url,
                         resend=not created)
                except a2a.UncertainSubmission:
                    if record.phase == Phase.SUBMITTED:
                        record = send_ambiguous(record)
                        journal.put(record)
                    if mode != "a2a-idempotent-resend":
                        record = task_incident(record, "opaque-effect-unknown")
                        journal.put(record)
                        return _async_unresolved(record)
                    created = False
                    time.sleep(0.5)
                    continue
                except Exception as error:
                    record = task_incident(record, "async-send-binding-inconsistent")
                    journal.put(record)
                    _log("agent-task-incident", action_id=record.action_id,
                         error_type=type(error).__name__)
                    return _async_unresolved(record)
                if state == "completed":
                    return _finish(journal, record, task, action, binding,
                                   expected_revision, role)
            try:
                # Re-attach by the journaled Task id only; never ask about a run.
                task = a2a.get_task(url, record.task_id)
                state = a2a.validate_async_task(task, context_id=record.context_id,
                                                identity=binding["identity"],
                                                task_id=record.task_id)
            except a2a.UncertainSubmission:
                time.sleep(0.5)
                continue
            except Exception as error:
                record = task_incident(record, "async-task-binding-inconsistent")
                journal.put(record)
                _log("agent-task-incident", action_id=record.action_id,
                     task_id=record.task_id, error_type=type(error).__name__)
                return _async_unresolved(record)
            _log("agent-task-polled", action_id=record.action_id,
                 task_id=record.task_id, state=state, url=url)
            if state == "completed":
                return _finish(journal, record, task, action, binding, expected_revision, role)
            if state not in {"submitted", "working"}:
                _record_agent_usage(task, action, binding["identity"])
                record = task_incident(record, "async-task-terminal-without-artifact")
                journal.put(record)
                return _async_unresolved(record)
            time.sleep(0.5)
        raise PendingTask("remote Task still working; retry from durable Task id")
    except StaleOutcome:
        current = journal.get(action["action_id"])
        if current.phase == Phase.CONFIRMED:
            return current.receipt
        if current.phase == Phase.INCIDENT:
            return _async_unresolved(current)
        raise PendingTask("another Activity attempt advanced the outcome journal")
    finally:
        journal.close()


def _finish(journal: OutcomeJournal, record, task: dict, action: dict, binding: dict,
            expected_revision: str, role: str) -> dict:
    _record_agent_usage(task, action, binding["identity"])
    try:
        receipt = _completed_receipt(task, record, binding, expected_revision, role)
    except handoff.OutputMissing:
        record = task_incident(record, handoff.OutputMissing.reason)
        _log("agent-output-missing", action_id=record.action_id, task_id=record.task_id)
    except Exception as error:
        record = task_incident(record, "async-artifact-inconsistent")
        _log("agent-artifact-incident", action_id=record.action_id,
             task_id=record.task_id, error_type=type(error).__name__)
    else:
        record = task_finished(record, receipt)
    journal.put(record)
    return record.receipt if record.phase == Phase.CONFIRMED else _async_unresolved(record)


def _completed_receipt(task: dict, record, binding: dict,
                       expected_revision: str, role: str) -> dict:
    """On-complete hook: enforce the node's output contract, then normalize.

    A strict `artifacts` node whose completed Task carries no artifact fails
    here with `output.missing` (never an empty hand-off). With an instance
    digest key configured, the receipt also carries the content-free produced
    hand-off items; it is journaled, so Activity retries replay the same record.
    """
    at = handoff.now_iso()
    items = handoff.produced_items(task, binding_output(binding),
                                   key=handoff.instance_key(), ready_at=at)
    receipt = a2a.async_receipt(task, _wire_binding(record), identity=binding["identity"],
                                expected_revision=expected_revision, role=role)
    if items:
        receipt["handoff"] = {"produced_at": at, "items": items}
    return receipt


def _with_handoff(result: dict, input: dict, *, report: dict | None = None) -> dict:
    """Bind a produced hand-off record to the identity the Workflow assigned.

    Workflows without hand-off records (older histories) never see the record.
    The report artifact item keeps the plain sha256 chain reference.
    """
    record = result.pop("handoff", None)
    if not isinstance(record, dict) or not isinstance(input.get("handoff_id"), str):
        return result
    items = [dict(item) for item in record["items"]]
    if report is not None and len(items) == 1 and items[0]["source"] == "artifact":
        items[0].update(artifact_revision=report["revision"], artifact_sha256=report["sha256"])
    result["handoff"] = {"handoff_id": input["handoff_id"],
                         "handoff_revision": input["handoff_revision"],
                         "produced_at": record["produced_at"], "items": items}
    return result


async def _thread_with_heartbeat(fn, *args):
    task = asyncio.create_task(asyncio.to_thread(fn, *args))
    while True:
        done, _ = await asyncio.wait({task}, timeout=2)
        if done:
            return await task
        try:
            activity.heartbeat()
        except RuntimeError:
            pass  # Direct unit invocation has no Activity context.


def _get_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.loads(response.read())


def _log(kind: str, **fields) -> None:
    path = os.environ.get("EXO_ACTIVITY_LOG")
    if path:
        with Path(path).open("a") as stream:
            stream.write(json.dumps({"kind": kind, "wall_time": time.time(), **fields},
                                    sort_keys=True) + "\n")


def _action(action_id: str, input: dict, brief: dict) -> dict:
    """The factory's own record of one agent action; only ``brief`` is sent."""
    return {"action_id": action_id, "run_id": input["run"],
            "definition_digest": input["digest"], "brief": canonical(brief),
            **_assignment_usage_bindings(input)}


@activity.defn
async def assign(input: dict) -> dict:
    capability = input["capability"]
    brief = research_assignment(capability, input["question"], input["packet"])
    action = _action(f"{input['run']}:{input['instance']}", input, brief)
    try:
        result = await _thread_with_heartbeat(_invoke_async, input["binding"],
            input["contract"], action, "r1", "research", capability)
        if "unresolved" in result:
            return result
        artifact = result["artifact"]
        content = json.loads(artifact["content"])
        if canonical(content) != artifact["content"]:
            raise ValueError("noncanonical research content")
        validate_research_result(content, capability, input["packet"])
        return _with_handoff({**result, "content": content}, input)
    except PendingTask:
        raise
    except Exception as error:
        return {"unresolved": "research-evidence-inconsistent", "action_id": action["action_id"],
                "error_type": type(error).__name__}


@activity.defn
async def typed_join(input: dict) -> dict:
    results = {name: {"content": receipt["content"], "sha256": receipt["artifact"]["sha256"]}
               for name, receipt in input["receipts"].items()}
    return packet_evidence_join(results, input["packet"])


@activity.defn
async def synthesize(input: dict) -> dict:
    brief = synthesis_assignment(input["revision"], input["question"], input["packet"],
        input["evidence"], prior=input.get("prior"), quality_findings=input.get("quality_findings"))
    action = _action(f"{input['run']}:synthesize:{input['revision']}", input, brief)
    try:
        result = await _thread_with_heartbeat(_invoke_async, input["binding"],
            input["contract"], action, input["revision"], "synthesis", "report_synthesis@1")
        if "unresolved" in result:
            return result
        artifact = result["artifact"]
        content = json.loads(artifact["content"])
        if canonical(content) != artifact["content"]:
            raise ValueError("noncanonical report content")
        validate_report(content, input["revision"], input["question"], input["packet"])
        if "handoff" not in result:
            return artifact
        return _with_handoff({**artifact, "handoff": result["handoff"]}, input, report=artifact)
    except PendingTask:
        raise
    except Exception as error:
        return {"unresolved": "synthesis-evidence-inconsistent", "action_id": action["action_id"],
                "error_type": type(error).__name__}


@activity.defn
async def review(input: dict) -> dict:
    from quality_authority import decide_quality_async
    candidate = input["candidate"]
    brief = quality_review_request(candidate, input["question"], input["packet"],
                                   input["policy_digest"])
    action = _action(quality_action_id(input["run"], input["assignment_id"], input["attempt"],
                                       candidate["revision"], candidate["sha256"]), input, brief)
    try:
        result = await _thread_with_heartbeat(_invoke_async, input["binding"],
            input["contract"], action, candidate["revision"], "quality", "report_quality_review@1")
        if "unresolved" in result:
            return {"inconsistent": "quality-action-outcome-unknown", "detail": result}
        artifact = result["artifact"]
        content = json.loads(artifact["content"])
        if canonical(content) != artifact["content"]:
            raise ValueError("noncanonical verdict content")
        validate_verdict(content, candidate, input["binding"]["identity"],
                         packet=input["packet"], rubric_digest=input.get("rubric_digest"))
        decision = decide_quality_async(binding=input["binding"], command=action,
            candidate=candidate, receipt=result, verdict=content,
            expected_task_id=result["task_id"])
        if decision.kind == QualityKind.INCONSISTENT:
            return {"inconsistent": decision.incident, "reasons": list(decision.reasons)}
        # A gate seals the carrier it consumed; it never mints a new hand-off.
        return {key: value for key, value in result.items() if key != "handoff"} | {
            "artifact": content}
    except PendingTask:
        raise
    except Exception as error:
        return {"inconsistent": "quality-evidence-inconsistent", "action_id": action["action_id"],
                "error_type": type(error).__name__}



def _release(input: dict) -> dict:
    """Deliver the accepted artifact to the pinned A2A release agent."""
    journal_path = os.environ.get("EXO_OUTCOME_DB")
    if not journal_path:
        raise RuntimeError("durable release outcome journal is not configured")
    path = Path(journal_path)
    return release_delivery.deliver(input, journal_path=path,
                                    snapshot=path.parent.parent / "testbed" / "agent_snapshot.json",
                                    log=_log)


@activity.defn
async def release(input: dict) -> dict:
    try:
        result = await _thread_with_heartbeat(_release, input)
    except release_delivery.PendingRelease:
        raise
    except Exception as error:
        result = {"unresolved": "release-adapter-incident",
                  "release_id": input["command"]["release_id"],
                  "error_type": type(error).__name__}
    _log("release-observed", run=input["command"]["run_id"],
         receipt=result.get("receipt_id"), release_id=result.get("release_id"),
         task_id=result.get("task_id"), unresolved=result.get("unresolved"))
    return _with_handoff(result, input) if "unresolved" not in result else result
