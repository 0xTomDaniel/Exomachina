"""A2A and release Activities. Remote receipts never become product acceptance here."""
from __future__ import annotations

import asyncio
import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path

from temporalio import activity

from a2a_outcome import (EffectKind, OutcomeJournal, Phase, ReceiverKind, lookup_result,
                         send_ambiguous, send_completed, submitted)
from quality_authority import QualityKind, decide_quality
import long_client as a2a
import fixture
import receiver_client


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
    _log("assign-start", run=input["run"], instance=input["instance"])
    command = fixture.assignment(input["run"], input["digest"], input["instance"],
        result_type=input["result_type"], scope_status=input["scope_status"],
        question=input.get("question"))
    try:
        result = await asyncio.to_thread(_invoke, input["url"], input["identity"],
            "capability", command, input["lookup_supported"])
    except Exception as error:
        return {"unresolved": "assignment-adapter-incident",
                "action_id": command["action_id"], "error_type": type(error).__name__}
    if "unresolved" not in result:
        try:
            artifact = result["artifact"]
            content = artifact.get("content", "")
            import hashlib
            if not isinstance(content, str) or artifact.get("sha256") != hashlib.sha256(content.encode()).hexdigest():
                raise ValueError("assignment artifact digest mismatch")
            if artifact.get("author") != input["identity"]:
                raise ValueError("assignment author identity mismatch")
            fixture.branch_value(result, input["instance"], run_id=input["run"],
                definition_digest=input["digest"], result_type=input["result_type"],
                scope_status=input["scope_status"], question=input.get("question"))
        except Exception as error:
            return {"unresolved": "assignment-evidence-inconsistent",
                    "action_id": command["action_id"], "error_type": type(error).__name__}
        _log("assign-remote-receipt", run=input["run"], instance=input["instance"],
             task_id=result["task_id"])
    _log("assign-complete", run=input["run"], instance=input["instance"])
    return result


@activity.defn
async def review(input: dict) -> dict:
    try:
        result = await asyncio.to_thread(_invoke, input["url"], input["identity"],
            "quality", input["command"], True)
    except Exception as error:
        return {"inconsistent": "quality-action-incident",
                "error_type": type(error).__name__,
                "action_id": input["command"]["action_id"]}
    if "unresolved" in result:
        return {"inconsistent": "quality-action-outcome-unknown", "detail": result}
    try:
        task = await asyncio.to_thread(a2a.get_task, input["url"], result["task_id"])
        lookup = await asyncio.to_thread(a2a.reconcile, input["url"],
            input["command"]["action_id"], input["command"]["run_id"],
            input["command"]["definition_digest"])
    except Exception as error:
        return {"inconsistent": "quality-evidence-unavailable",
                "error_type": type(error).__name__}
    try:
        decision = decide_quality(binding=input["binding"],
            observed_endpoint=input["url"], observed_identity=input["identity"],
            command=input["command"], assignment_id=input["assignment_id"],
            attempt=input["attempt"], task=task, lookup=lookup,
            send_payload=result["artifact"])
    except Exception as error:
        return {"inconsistent": "quality-evidence-inconsistent",
                "error_type": type(error).__name__,
                "action_id": input["command"]["action_id"]}
    if decision.kind == QualityKind.INCONSISTENT:
        return {"inconsistent": decision.incident, "reasons": list(decision.reasons),
                "action_id": input["command"]["action_id"]}
    artifact = dict(decision.verdict)
    result["artifact"] = artifact
    _log("quality-verdict", run=input["command"]["run_id"],
         decision_kind=decision.kind.value,
         revision=artifact["revision"], accepted=artifact["accepted"],
         task_id=result["task_id"])
    return result


@activity.defn
async def typed_join(input: dict) -> dict:
    _log("join-start", run=input["run"], instances=sorted(input["receipts"]))
    return fixture.typed_join(input["receipts"], run_id=input["run"],
        definition_digest=input["digest"], declarations=input["declarations"],
        scope_status_by_instance=input["scopes"], question=input.get("question"))


@activity.defn
async def synthesize(input: dict) -> dict:
    return fixture.candidate_artifact(input["join"], input["revision"],
                                      input["author"], resolved=input["resolved"])


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
