"""Factory-side release node: deliver an accepted artifact to an A2A agent.

The release receiver is an ordinary A2A v1 agent with ``output: none``. The
factory is its ordinary A2A client:

- the node records dispatch intent (release attempt -> ``messageId``) in the
  durable outcome journal before any network I/O;
- it sends one ``SendMessage`` carrying exactly the accepted report artifact's
  own Parts, copied verbatim from the producing artifact (``handoff.compose_parts``,
  decision 9): no brief and nothing factory-specific;
- it journals the returned ``taskId`` and re-reads only through ``GetTask``;
- after an uncertain send it re-sends the identical Message (same
  ``messageId``) only when the pinned Agent Card promises ``messageId``
  idempotency, otherwise the outcome stays unresolved;
- the node is bound with strict ``artifacts`` output: a completed Task with
  no receipt artifact fails the output contract (``output.missing``), so an
  unverified delivery can never look complete;
- the receipt artifact must cover the exact delivered bytes, and those must
  be the bytes Quality accepted (``artifact_sha256``); any mismatch fails the
  node (``release-receipt-inconsistent``).

The confirmed receipt is the node's evidence. Its content-free hand-off record
has no consumer (release has control edges only) and retires at the station.
Correlation (run, revision, release id) stays on the factory side.
"""
from __future__ import annotations

import base64
import hashlib
import json
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime
from pathlib import Path

import a2a_v1
import handoff
from definition import binding_output
from a2a_outcome import (EffectKind, OutcomeJournal, Phase, ReceiverKind, StaleOutcome,
                         send_ambiguous, submitted, task_finished, task_incident,
                         task_started)
from agent_binding import UnavailableBinding, resolve


TOKEN = "Bearer fixture-token"
MEDIA_TYPE = "application/json"
RECEIPT_FIELDS = frozenset({"receipt_id", "sha256", "byte_length", "media_type",
                            "accepted_at", "outcome"})
# A stable, opaque namespace: the messageId reveals nothing about the run.
_MESSAGE_NAMESPACE = uuid.UUID("0b8f3c52-6a8e-4f5e-9d61-2a7b4c1e9f30")
_RECEIVER = {"a2a-idempotent-resend": ReceiverKind.PARTICIPATING,
             "opaque": ReceiverKind.OPAQUE}


class UncertainDelivery(Exception):
    """The request may have committed remotely; reconcile through A2A only."""


class PendingRelease(Exception):
    """Retry the Activity; the remote Task id is durable in the journal."""


def message_id_for(release_id: str) -> str:
    """Deterministic, opaque A2A messageId for one factory release attempt."""
    return str(uuid.uuid5(_MESSAGE_NAMESPACE, release_id))


def release_parts(input: dict) -> list[dict]:
    """The accepted draft's item Parts, verbatim, verified against ``consumes``.

    Release has no brief. A release scheduled by an older Workflow carries no
    ``upstream``; its accepted content is then the recorded text Part.
    """
    command = input["command"]
    upstream = input.get("upstream")
    if upstream is None:
        upstream = [{"handoff_id": None,
                     "item_parts": handoff.fallback_item_parts(command["content"])}]
    consumes = input.get("consumes")
    key = handoff.instance_key() if consumes is not None else None
    return handoff.compose_parts([], upstream, consumes=consumes, key=key)


def release_message(command: dict, parts: list[dict]) -> dict:
    """The ordinary A2A Message the receiver gets: the accepted artifact's Parts."""
    return handoff.require_within_bound(
        a2a_v1.user_message(parts, message_id=message_id_for(command["release_id"])))


def delivered_bytes(part: dict) -> bytes:
    """The exact bytes a receiver derives from one delivered Part."""
    kind = a2a_v1.part_content(part)
    if kind == "text":
        return a2a_v1.part_text(part).encode("utf-8")
    if kind == "raw":
        return base64.b64decode(part["raw"], validate=True)
    if kind == "data":
        return json.dumps(a2a_v1.part_data(part), sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False).encode("utf-8")
    raise ValueError("a URL Part delivers no bytes")


def _rpc(url: str, method: str, params: dict) -> dict:
    body = json.dumps(a2a_v1.rpc(method, params)).encode()
    request = urllib.request.Request(url.rstrip("/") + "/", data=body, method="POST",
        headers={"Authorization": TOKEN, "Content-Type": "application/json",
                 **a2a_v1.headers()})
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            reply = json.loads(response.read())
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"HTTP {error.code}") from error
    except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
        raise UncertainDelivery(str(error)) from error
    if "error" in reply:
        raise RuntimeError("A2A error: " + json.dumps(reply["error"], sort_keys=True))
    return reply["result"]


def send_message(url: str, message: dict) -> dict:
    return a2a_v1.require_task(_rpc(url, a2a_v1.SEND_MESSAGE, a2a_v1.send_params(message)))


def get_task(url: str, task_id: str) -> dict:
    task = a2a_v1.normalize_numbers(_rpc(url, a2a_v1.GET_TASK, {"id": task_id}))
    if not isinstance(task, dict) or "kind" in task:
        raise a2a_v1.ProtocolError("GetTask did not return an A2A v1 Task")
    return task


def status_text(task: dict) -> str | None:
    message = (task.get("status") or {}).get("message")
    try:
        texts = [a2a_v1.part_text(part) for part in a2a_v1.message_parts(message)
                 if a2a_v1.part_content(part) == "text"]
    except a2a_v1.ProtocolError:
        return None
    return " ".join(texts) or None


def receipt_from_task(task: dict, command: dict, *, delivered: dict, identity: str,
                      task_id: str, message_id: str) -> dict:
    """Validate the receipt against the delivered Part and the accepted digest.

    The receipt's sha256 must equal both the digest of the bytes the receiver
    derives from ``delivered`` (the one Part sent) and the accepted
    ``artifact_sha256`` (``command["sha256"]``); its byte length and media
    type must be those of the delivered Part.
    """
    artifacts = task.get("artifacts") or []
    if len(artifacts) != 1 or len(artifacts[0].get("parts") or []) != 1:
        raise ValueError("release Task must carry exactly one receipt artifact")
    receipt = a2a_v1.part_data(artifacts[0]["parts"][0])
    if not isinstance(receipt, dict) or set(receipt) != RECEIPT_FIELDS:
        raise ValueError("release receipt fields differ from the pinned contract")
    content = delivered_bytes(delivered)
    if (receipt["sha256"] != hashlib.sha256(content).hexdigest()
            or receipt["sha256"] != command["sha256"]
            or receipt["byte_length"] != len(content)
            or receipt["media_type"] != delivered.get("mediaType")
            or receipt["outcome"] != "delivered"):
        raise ValueError("release receipt does not cover the exact accepted bytes")
    if (not isinstance(receipt["receipt_id"], str) or not receipt["receipt_id"]
            or artifacts[0].get("artifactId") != receipt["receipt_id"]):
        raise ValueError("release receipt id is invalid")
    try:
        datetime.fromisoformat(str(receipt["accepted_at"]).replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("release receipt time is invalid") from error
    return {"release_id": command["release_id"], "run_id": command["run_id"],
            "definition_digest": command["definition_digest"],
            "revision": command["revision"], "sha256": receipt["sha256"],
            "receipt_id": receipt["receipt_id"], "byte_length": receipt["byte_length"],
            "media_type": receipt["media_type"], "accepted_at": receipt["accepted_at"],
            "outcome": receipt["outcome"], "task_id": task_id, "message_id": message_id,
            "destination_identity": identity, "a2a_protocol": a2a_v1.PROTOCOL_VERSION}


def unresolved(record, detail: str | None = None) -> dict:
    value = {"unresolved": record.reason, "release_id": record.action_id,
             "task_id": record.task_id}
    if detail:
        value["detail"] = detail
    return value


def deliver(input: dict, *, journal_path: Path, snapshot: Path, log=lambda *a, **k: None,
            deadline_seconds: float = 70, poll_seconds: float = 0.5) -> dict:
    """Run one release node to a confirmed receipt or an unresolved outcome."""
    binding, contract, command = input["binding"], input["contract"], input["command"]
    identity = binding["identity"]
    if (contract.get("role") != "release" or binding_output(binding) != "artifacts"
            or contract.get("reconcile") not in _RECEIVER
            or not isinstance(contract.get("card_sha256"), str)):
        raise ValueError("release node requires a card-pinned strict-artifacts A2A agent")
    if hashlib.sha256(command["content"].encode("utf-8")).hexdigest() != command["sha256"]:
        raise ValueError("release content differs from its accepted digest")
    parts = release_parts(input)
    if len(parts) != 1:
        raise ValueError("release delivers the accepted artifact as exactly one Part")
    if hashlib.sha256(delivered_bytes(parts[0])).hexdigest() != command["sha256"]:
        raise ValueError("release Part differs from the accepted artifact digest")
    message = release_message(command, parts)
    receiver = _RECEIVER[contract["reconcile"]]
    expected = submitted(command["release_id"], command["run_id"], command["definition_digest"],
        receiver, effect_kind=EffectKind.RELEASE, revision=command["revision"],
        sha256=command["sha256"], pinned_identity=identity, message_id=message["messageId"],
        payload_sha256=hashlib.sha256(json.dumps(message, sort_keys=True,
            separators=(",", ":"), ensure_ascii=False).encode()).hexdigest())
    journal = OutcomeJournal(journal_path)
    try:
        record, created = journal.begin(expected)
        return _drive(journal, record, created, message, command, contract, binding,
                      snapshot, log, deadline_seconds, poll_seconds)
    except StaleOutcome:
        current = journal.get(command["release_id"])
        if current.phase == Phase.CONFIRMED:
            return current.receipt
        if current.phase == Phase.INCIDENT:
            return unresolved(current)
        raise PendingRelease("another Activity attempt advanced the release journal")
    finally:
        journal.close()


def _incident(journal, record, reason: str, log, detail: str | None = None) -> dict:
    record = task_incident(record, reason)
    journal.put(record)
    log("release-incident", action_id=record.action_id, task_id=record.task_id,
        reason=reason, detail=detail)
    return unresolved(record, detail)


def _drive(journal, record, created, message, command, contract, binding, snapshot,
           log, deadline_seconds, poll_seconds) -> dict:
    identity = binding["identity"]
    if record.phase == Phase.CONFIRMED:
        return record.receipt
    if record.phase == Phase.INCIDENT:
        return unresolved(record)
    participating = record.receiver == ReceiverKind.PARTICIPATING
    if record.task_id is None and not created and not participating:
        # A prior attempt may have delivered; an opaque receiver cannot say.
        if record.phase == Phase.SUBMITTED:
            record = send_ambiguous(record)
            journal.put(record)
        return _incident(journal, record, "opaque-effect-unknown", log)
    deadline = time.monotonic() + deadline_seconds
    while time.monotonic() < deadline:
        try:
            url, observed = resolve(snapshot, identity, contract)
        except UnavailableBinding:
            time.sleep(poll_seconds)
            continue
        except Exception as error:
            return _incident(journal, record, "pinned-agent-verification-failed", log,
                             type(error).__name__)
        log("agent-card-verified", action_id=record.action_id, pinned_identity=identity,
            pinned_card_sha256=contract["card_sha256"], observed=observed)
        try:
            if record.task_id is None:
                # First dispatch and any replay send the identical journal-bound Message.
                task = send_message(url, message)
                task_id = task.get("id")
                record = task_started(record, task_id)
                journal.put(record)
                if record.phase == Phase.INCIDENT:
                    return unresolved(record)
                log("agent-task-journaled", action_id=record.action_id, task_id=task_id,
                    message_id=record.message_id, url=url, resend=not created)
            else:
                task = get_task(url, record.task_id)
                if task.get("id") != record.task_id:
                    return _incident(journal, record, "release-task-binding-inconsistent", log)
                log("agent-task-polled", action_id=record.action_id, task_id=record.task_id,
                    url=url)
            state = a2a_v1.task_state(task)
        except UncertainDelivery:
            if record.task_id is None:
                if record.phase == Phase.SUBMITTED:
                    record = send_ambiguous(record)
                    journal.put(record)
                if not participating:
                    return _incident(journal, record, "opaque-effect-unknown", log)
                created = False
            time.sleep(poll_seconds)
            continue
        except (RuntimeError, a2a_v1.ProtocolError, ValueError) as error:
            reason = ("release-send-refused" if record.task_id is None
                      else "release-task-lookup-failed")
            return _incident(journal, record, reason, log, str(error)[:300])
        if state == "completed":
            # On-complete hook: the node's strict output contract comes first.
            produced_at = handoff.now_iso()
            try:
                items = handoff.produced_items(task, binding_output(binding),
                                               key=handoff.instance_key(), ready_at=produced_at)
                receipt = receipt_from_task(task, command, delivered=message["parts"][0],
                                            identity=identity,
                                            task_id=record.task_id,
                                            message_id=record.message_id)
            except handoff.OutputMissing:
                return _incident(journal, record, handoff.OutputMissing.reason, log,
                                 status_text(task))
            except (ValueError, a2a_v1.ProtocolError) as error:
                return _incident(journal, record, "release-receipt-inconsistent", log,
                                 str(error)[:300])
            if items:
                receipt["handoff"] = {"produced_at": produced_at, "items": items}
            record = task_finished(record, receipt)
            journal.put(record)
            log("release-receipt-confirmed", action_id=record.action_id,
                task_id=record.task_id, receipt_id=receipt["receipt_id"])
            return record.receipt if record.phase == Phase.CONFIRMED else unresolved(record)
        if state in {"rejected", "failed", "canceled"}:
            return _incident(journal, record, "release-task-" + state, log, status_text(task))
        if state not in {"submitted", "working"}:
            return _incident(journal, record, "release-task-" + state, log, status_text(task))
        time.sleep(poll_seconds)
    raise PendingRelease("release Task still working; retry from durable Task id")
