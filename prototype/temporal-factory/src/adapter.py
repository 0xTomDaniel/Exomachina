"""A2A and release Activities. Remote receipts never become product acceptance here."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path

from temporalio import activity

from a2a_outcome import (EffectKind, OutcomeJournal, Phase, ReceiverKind, lookup_result,
                         send_ambiguous, send_completed, submitted, task_started,
                         task_finished, task_incident, StaleOutcome)
from quality_authority import QualityKind, quality_action_id
from report_contract import (canonical, packet_evidence_join, research_assignment,
    synthesis_assignment, quality_review_request, validate_research_result,
    validate_report, validate_verdict)
import long_client as a2a
import fixture
import receiver_client


class PendingTask(Exception):
    """Retry the Activity; its remote Task id is durable in the journal."""


def _async_unresolved(record) -> dict:
    return {"unresolved": record.reason, "action_id": record.action_id,
            "task_id": record.task_id}


def _invoke_async(binding: dict, contract: dict, command: dict,
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
    payload_sha256 = hashlib.sha256(json.dumps(command, sort_keys=True,
        separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    expected = submitted(command["action_id"], command["run_id"],
        command["definition_digest"],
        ReceiverKind.PARTICIPATING if mode == "a2a-idempotent-resend" else ReceiverKind.OPAQUE,
        payload_sha256=payload_sha256, pinned_identity=binding["identity"])
    journal = OutcomeJournal(path)
    try:
        record, created = journal.begin(expected)
        if record.phase == Phase.CONFIRMED:
            return record.receipt
        if record.phase == Phase.INCIDENT:
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
                    and observed["contract_document"].get("capability") != expected_capability):
                record = task_incident(record, "pinned-agent-capability-mismatch")
                journal.put(record)
                return _async_unresolved(record)
            _log("agent-card-verified", action_id=record.action_id,
                 pinned_identity=binding["identity"], pinned_card_sha256=contract["card_sha256"],
                 pinned_contract_digest=contract["a2a_extension"]["contract_digest"],
                 observed={key: value for key, value in observed.items()
                           if key != "contract_document"})
            if record.task_id is None:
                # Both first dispatch and replay use the exact journal-bound payload.
                try:
                    task = a2a.send_async(url, command)
                    state = a2a.validate_async_task(task, command, binding["identity"])
                    record = task_started(record, task["id"])
                    journal.put(record)
                    _log("agent-task-journaled", action_id=record.action_id,
                         task_id=record.task_id, state=state, url=url,
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
                    try:
                        receipt = a2a.async_receipt(task, command, binding["identity"],
                                                    expected_revision, role)
                    except Exception:
                        record = task_incident(record, "async-artifact-inconsistent")
                    else:
                        record = task_finished(record, receipt)
                    journal.put(record)
                    return record.receipt if record.phase == Phase.CONFIRMED else _async_unresolved(record)
            try:
                task = a2a.get_task(url, record.task_id)
                state = a2a.validate_async_task(task, command, binding["identity"])
                if task["id"] != record.task_id:
                    raise ValueError("remote Task id changed")
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
                try:
                    receipt = a2a.async_receipt(task, command, binding["identity"],
                                                expected_revision, role)
                except Exception as error:
                    record = task_incident(record, "async-artifact-inconsistent")
                    _log("agent-artifact-incident", action_id=record.action_id,
                         task_id=record.task_id, error_type=type(error).__name__)
                else:
                    record = task_finished(record, receipt)
                journal.put(record)
                return record.receipt if record.phase == Phase.CONFIRMED else _async_unresolved(record)
            if state not in {"submitted", "working"}:
                record = task_incident(record, "async-task-terminal-without-artifact")
                journal.put(record)
                return _async_unresolved(record)
            time.sleep(0.5)
        raise PendingTask("remote Task still working; retry from durable Task id")
    except StaleOutcome:
        current = journal.get(command["action_id"])
        if current.phase == Phase.CONFIRMED:
            return current.receipt
        if current.phase == Phase.INCIDENT:
            return _async_unresolved(current)
        raise PendingTask("another Activity attempt advanced the outcome journal")
    finally:
        journal.close()


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


def _identity(url: str, role: str, pinned: str) -> None:
    health = _get_json(url.rstrip("/") + "/health")
    if health.get("role") != role or health.get("identity") != pinned:
        raise ValueError("remote service identity/role changed")


def _existing(url: str, action_id: str, run: str, digest: str) -> dict | None:
    try:
        return a2a.reconcile(url, action_id, run, digest)
    except RuntimeError as error:
        if str(error).startswith("HTTP 404:"):
            return None
        raise


def _receipt(record: dict, command: dict, identity: str, role: str) -> dict:
    for key in ("action_id", "run_id", "definition_digest"):
        if record.get(key) != command[key]:
            raise ValueError("remote receipt binding mismatch: " + key)
    if record.get("harness_identity") not in (None, identity):
        raise ValueError("A2A metadata identity mismatch")
    if record.get("harness_role") not in (None, role):
        raise ValueError("A2A metadata role mismatch")
    if record.get("role") not in (None, role):
        raise ValueError("lookup role mismatch")
    artifact = record.get("artifact")
    if not isinstance(artifact, dict):
        raise ValueError("remote receipt lacks artifact")
    return {"action_id": command["action_id"], "run_id": command["run_id"],
            "definition_digest": command["definition_digest"],
            "task_id": record["task_id"], "artifact": artifact,
            "harness_identity": identity, "harness_role": role,
            "receiver_attempts": record.get("attempts"),
            "receiver_effect_count": record.get("accepted_count")}


def _invoke(url: str, identity: str, role: str, command: dict,
            lookup_supported: bool) -> dict:
    """Record intent before send; reconcile uncertainty without resubmission."""
    _identity(url, role, identity)
    journal_path = os.environ.get("EXO_OUTCOME_DB")
    if not journal_path:
        raise RuntimeError("durable A2A outcome journal is not configured")
    journal = OutcomeJournal(Path(journal_path))
    receiver = ReceiverKind.PARTICIPATING if lookup_supported else ReceiverKind.OPAQUE
    expected = submitted(command["action_id"], command["run_id"],
                         command["definition_digest"], receiver)
    try:
        return _invoke_journaled(journal, expected, url, identity, role, command,
                                 lookup_supported)
    finally:
        journal.close()


def _invoke_journaled(journal: OutcomeJournal, expected, url: str, identity: str,
                      role: str, command: dict, lookup_supported: bool) -> dict:
    record, created = journal.begin(expected)
    if record.phase == Phase.CONFIRMED:
        return record.receipt
    if record.phase == Phase.INCIDENT:
        return {"unresolved": record.reason, "action_id": record.action_id}
    if not created:
        # A prior dispatch intent may have been sent before the Activity stopped.
        if record.phase == Phase.SUBMITTED:
            record = send_ambiguous(record)
            journal.put(record)
        return _bounded_lookup(journal, record, url, identity, role, command)
    if lookup_supported:
        try:
            prior = _existing(url, command["action_id"], command["run_id"],
                              command["definition_digest"])
        except Exception:
            record = send_ambiguous(record)
            journal.put(record)
            return _bounded_lookup(journal, record, url, identity, role, command)
        if prior is not None:
            receipt = _receipt(prior, command, identity, role)
            record = send_completed(record, receipt)
            journal.put(record)
            return receipt
    try:
        result = a2a.send(url, command)
    except Exception as error:
        record = send_ambiguous(record)
        journal.put(record)
        return _bounded_lookup(journal, record, url, identity, role, command)
    receipt = _receipt(result, command, identity, role)
    record = send_completed(record, receipt)
    journal.put(record)
    if record.phase == Phase.INCIDENT:
        return {"unresolved": record.reason, "action_id": record.action_id}
    return receipt


def _bounded_lookup(journal: OutcomeJournal, record, url: str, identity: str,
                    role: str, command: dict) -> dict:
    if record.receiver == ReceiverKind.OPAQUE:
        record = lookup_result(record, None)
        journal.put(record)
        return {"unresolved": record.reason, "action_id": record.action_id}
    while record.phase == Phase.UNKNOWN:
        try:
            prior = _existing(url, command["action_id"], command["run_id"],
                              command["definition_digest"])
            receipt = _receipt(prior, command, identity, role) if prior else None
            record = lookup_result(record, receipt)
        except Exception:
            record = lookup_result(record, None, available=False)
        journal.put(record)
        if record.phase == Phase.UNKNOWN:
            time.sleep(0.2)
    if record.phase == Phase.CONFIRMED:
        return record.receipt
    return {"unresolved": record.reason, "action_id": record.action_id}


@activity.defn
async def assign(input: dict) -> dict:
    capability = input["capability"]
    brief = research_assignment(capability, input["question"], input["packet"])
    command = {"op": "assign", "action_id": f"{input['run']}:{input['instance']}",
               "run_id": input["run"], "definition_digest": input["digest"],
               "brief": canonical(brief)}
    try:
        result = await _thread_with_heartbeat(_invoke_async, input["binding"],
            input["contract"], command, "r1", "research", capability)
        if "unresolved" in result:
            return result
        artifact = result["artifact"]
        content = json.loads(artifact["content"])
        if canonical(content) != artifact["content"]:
            raise ValueError("noncanonical research content")
        validate_research_result(content, capability, input["packet"])
        return {**result, "content": content}
    except PendingTask:
        raise
    except Exception as error:
        return {"unresolved": "research-evidence-inconsistent", "action_id": command["action_id"],
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
    command = {"op": "assign", "action_id": f"{input['run']}:synthesize:{input['revision']}",
               "run_id": input["run"], "definition_digest": input["digest"],
               "brief": canonical(brief)}
    try:
        result = await _thread_with_heartbeat(_invoke_async, input["binding"],
            input["contract"], command, input["revision"], "synthesis", "report_synthesis@1")
        if "unresolved" in result:
            return result
        artifact = result["artifact"]
        content = json.loads(artifact["content"])
        if canonical(content) != artifact["content"]:
            raise ValueError("noncanonical report content")
        validate_report(content, input["revision"], input["question"], input["packet"])
        return artifact
    except PendingTask:
        raise
    except Exception as error:
        return {"unresolved": "synthesis-evidence-inconsistent", "action_id": command["action_id"],
                "error_type": type(error).__name__}


@activity.defn
async def review(input: dict) -> dict:
    from quality_authority import decide_quality_async
    candidate = input["candidate"]
    brief = quality_review_request(candidate, input["question"], input["packet"],
                                   input["policy_digest"])
    command = {"op": "assign", "action_id": quality_action_id(input["run"],
        input["assignment_id"], input["attempt"], candidate["revision"], candidate["sha256"]),
        "run_id": input["run"], "definition_digest": input["digest"], "brief": canonical(brief)}
    try:
        result = await _thread_with_heartbeat(_invoke_async, input["binding"],
            input["contract"], command, candidate["revision"], "quality", "report_quality_review@1")
        if "unresolved" in result:
            return {"inconsistent": "quality-action-outcome-unknown", "detail": result}
        artifact = result["artifact"]
        content = json.loads(artifact["content"])
        if canonical(content) != artifact["content"]:
            raise ValueError("noncanonical verdict content")
        validate_verdict(content, candidate, input["binding"]["identity"],
                         packet=input["packet"], rubric_digest=input.get("rubric_digest"))
        decision = decide_quality_async(binding=input["binding"], command=command,
            candidate=candidate, receipt=result, verdict=content,
            expected_task_id=result["task_id"])
        if decision.kind == QualityKind.INCONSISTENT:
            return {"inconsistent": decision.incident, "reasons": list(decision.reasons)}
        return {**result, "artifact": content}
    except PendingTask:
        raise
    except Exception as error:
        return {"inconsistent": "quality-evidence-inconsistent", "action_id": command["action_id"],
                "error_type": type(error).__name__}


def _release(input: dict) -> dict:
    url = input["url"]
    health = _get_json(url.rstrip("/") + "/health")
    if health.get("identity") != input["identity"] or health.get("mode") != input["mode"]:
        raise ValueError("release receiver identity/mode changed")
    command = input["command"]
    journal_path = os.environ.get("EXO_OUTCOME_DB")
    if not journal_path:
        raise RuntimeError("durable release outcome journal is not configured")
    kind = ReceiverKind.PARTICIPATING if input["mode"] == "participating" else ReceiverKind.OPAQUE
    expected = submitted(command["release_id"], command["run_id"],
        command["definition_digest"], kind, effect_kind=EffectKind.RELEASE,
        revision=command["revision"], sha256=command["sha256"])
    journal = OutcomeJournal(Path(journal_path))
    try:
        return _release_journaled(journal, expected, url, command)
    finally:
        journal.close()


def _release_lookup(url: str, release_id: str) -> dict | None:
    try:
        return receiver_client.receipt(url, release_id)
    except RuntimeError as error:
        if str(error).startswith("HTTP 404:"):
            return None
        raise


def _release_unresolved(record) -> dict:
    return {"unresolved": record.reason, "release_id": record.action_id}


def _release_bounded_lookup(journal: OutcomeJournal, record, url: str) -> dict:
    if record.receiver == ReceiverKind.OPAQUE:
        record = lookup_result(record, None)
        journal.put(record)
        return _release_unresolved(record)
    while record.phase == Phase.UNKNOWN:
        try:
            receipt = _release_lookup(url, record.action_id)
            record = lookup_result(record, receipt)
        except Exception:
            record = lookup_result(record, None, available=False)
        journal.put(record)
        if record.phase == Phase.UNKNOWN:
            time.sleep(0.2)
    return record.receipt if record.phase == Phase.CONFIRMED else _release_unresolved(record)


def _release_journaled(journal: OutcomeJournal, expected, url: str, command: dict) -> dict:
    record, created = journal.begin(expected)
    if record.phase == Phase.CONFIRMED:
        return record.receipt
    if record.phase == Phase.INCIDENT:
        return _release_unresolved(record)
    if not created:
        # A persisted intent may have been sent before the Activity stopped.
        if record.phase == Phase.SUBMITTED:
            record = send_ambiguous(record)
            journal.put(record)
        return _release_bounded_lookup(journal, record, url)
    if record.receiver == ReceiverKind.PARTICIPATING:
        try:
            prior = _release_lookup(url, record.action_id)
        except Exception:
            record = send_ambiguous(record)
            journal.put(record)
            return _release_bounded_lookup(journal, record, url)
        if prior is not None:
            record = send_completed(record, prior)
            journal.put(record)
            return prior if record.phase == Phase.CONFIRMED else _release_unresolved(record)
        try:
            reply = receiver_client.release(url, command)
        except Exception:
            record = send_ambiguous(record)
            journal.put(record)
            return _release_bounded_lookup(journal, record, url)
        record = send_completed(record, reply)
        journal.put(record)
        return reply if record.phase == Phase.CONFIRMED else _release_unresolved(record)
    try:
        receiver_client.opaque_submit(url, command)
    except Exception:
        pass
    # Opaque acknowledgement cannot prove an exact effect or furnish a receipt.
    record = send_ambiguous(record)
    journal.put(record)
    return _release_bounded_lookup(journal, record, url)


@activity.defn
async def release(input: dict) -> dict:
    try:
        result = await asyncio.to_thread(_release, input)
    except Exception as error:
        result = {"unresolved": "release-adapter-incident",
                  "release_id": input["command"]["release_id"],
                  "error_type": type(error).__name__}
    _log("release-observed", run=input["command"]["run_id"],
         receipt=result.get("release_id"), unresolved=result.get("unresolved"))
    return result
