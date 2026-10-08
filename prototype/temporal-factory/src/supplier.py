"""Pinned, bounded A2A fan-out over the shared outcome authority.

This module owns no workflow, spending, or outcome database. Callers supply the
existing ``OutcomeJournal``, immutable identity snapshot, parent assignment,
and an explicit finite set of child assignments.

Suppliers are ordinary A2A agent services (A2A decision 7): each child is sent
as a plain Message whose text Part is the canonical child payload. Parent and
child factory bindings never cross the wire; the journal maps them to the
A2A ``messageId``/``contextId``/``taskId``. A lost response is reconciled by
resending the same journaled Message only when the supplier's Agent Card
declares the ``messageId`` resend rule; otherwise it stays unknown.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import a2a_extensions
import long_client as a2a
from uuid import uuid4
from a2a_outcome import (OutcomeJournal, OutcomeRecord, Phase, ReceiverKind,
                         send_ambiguous, send_completed, submitted,
                         task_finished, task_incident, task_started)


class SupplierBindingError(ValueError):
    """A supplier assignment or remote result does not match its pinned IDs."""


@dataclass(frozen=True)
class ParentAssignment:
    task_id: str
    run_id: str
    definition_digest: str
    assignment_id: str
    attempt_id: str


@dataclass(frozen=True)
class SupplierRequest:
    identity: str
    role: str
    contract: Mapping[str, Any]
    action_id: str
    run_id: str
    definition_digest: str
    assignment_id: str
    attempt_id: str
    expected_revision: str
    payload: Mapping[str, Any]


_PARENT_KEYS = ("parent_task_id", "parent_run_id", "parent_definition_digest",
                "parent_assignment_id", "parent_attempt_id")
_CHILD_KEYS = ("action_id", "run_id", "definition_digest", "assignment_id", "attempt_id")
_RECONCILIATION_PINS = {"a2a-idempotent-resend", "opaque"}


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be non-empty text")
    return value


def _json_copy(value: Any, name: str) -> Any:
    try:
        return json.loads(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False, allow_nan=False))
    except (TypeError, ValueError) as error:
        raise ValueError(f"{name} must be JSON-compatible") from error


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


class SupplierFanout:
    """Dispatch/reconcile an explicitly configured finite set of A2A children.

    The interface is intentionally dependency-injected: a Runtime or workflow
    owner constructs the established ``OutcomeJournal`` and passes its pinned
    snapshot path. No SQLite access is performed here beyond journal methods.
    """

    def __init__(self, journal: OutcomeJournal, snapshot_path: Path, *,
                 max_fanout: int):
        if type(max_fanout) is not int or max_fanout < 1:
            raise ValueError("max_fanout must be an explicit positive integer")
        if not isinstance(snapshot_path, Path):
            raise TypeError("snapshot_path must be a Path")
        self.journal = journal
        self.snapshot_path = snapshot_path
        self.max_fanout = max_fanout

    def fan_out(self, parent: ParentAssignment,
                children: Sequence[SupplierRequest]) -> list[dict[str, Any]]:
        """Dispatch the declared children in order, never exceeding the cap."""
        self._validate_parent(parent)
        if not isinstance(children, Sequence) or isinstance(children, (str, bytes)):
            raise TypeError("children must be a finite sequence")
        if len(children) > self.max_fanout:
            raise ValueError("supplier fan-out exceeds configured max_fanout")
        action_ids: set[str] = set()
        for child in children:
            self._validate_child(child)
            if child.action_id in action_ids:
                raise ValueError("supplier fan-out action IDs must be unique")
            action_ids.add(child.action_id)
        return [self.dispatch_child(parent, child) for child in children]

    def dispatch_child(self, parent: ParentAssignment,
                       child: SupplierRequest) -> dict[str, Any]:
        """Submit once, or resume a journaled child by its journaled Message."""
        self._validate_parent(parent)
        self._validate_child(child)
        expected = self._expected(parent, child)

        # Resolve before creating durable intent. Pin checks are read-only; a
        # missing or changed pin cannot leave a false dispatch record behind.
        url, _observed = self._resolve(child)
        record, created = self.journal.begin(expected)
        if record.phase in {Phase.CONFIRMED, Phase.INCIDENT}:
            return self._view(parent, child, record)
        if not created and record.phase == Phase.SUBMITTED:
            # Another process may have stopped after committing intent and
            # before recording the response. Treat that boundary as unknown.
            record = self.journal.put(send_ambiguous(record))
        if created:
            return self._send(parent, child, record, url)
        return self._resume(parent, child, record)

    def reconcile_child(self, parent: ParentAssignment,
                        child: SupplierRequest) -> dict[str, Any]:
        """Reconcile only an existing child action from its journaled Message."""
        self._validate_parent(parent)
        self._validate_child(child)
        expected = self._expected(parent, child)
        record = self.journal.get(child.action_id)
        if record is None:
            return self._view(parent, child, None, reason="no-dispatch-record")
        record, created = self.journal.begin(expected)
        if created:
            # Defensive only: ``get`` found an existing row immediately above.
            raise RuntimeError("outcome journal changed during reconciliation")
        if record.phase == Phase.SUBMITTED:
            record = self.journal.put(send_ambiguous(record))
        if record.phase in {Phase.CONFIRMED, Phase.INCIDENT}:
            return self._view(parent, child, record)
        return self._resume(parent, child, record)

    @staticmethod
    def _validate_parent(parent: ParentAssignment) -> None:
        if not isinstance(parent, ParentAssignment):
            raise TypeError("parent must be a ParentAssignment")
        for name in ("task_id", "run_id", "definition_digest", "assignment_id", "attempt_id"):
            _text(getattr(parent, name), f"parent.{name}")

    @staticmethod
    def _validate_child(child: SupplierRequest) -> None:
        if not isinstance(child, SupplierRequest):
            raise TypeError("child must be a SupplierRequest")
        for name in ("identity", "role", "action_id", "run_id", "definition_digest",
                     "assignment_id", "attempt_id", "expected_revision"):
            _text(getattr(child, name), f"child.{name}")
        if not isinstance(child.contract, Mapping):
            raise TypeError("child.contract must be a pinned mapping")
        if not isinstance(child.payload, Mapping):
            raise TypeError("child.payload must be a mapping")
        if child.contract.get("reconcile") not in _RECONCILIATION_PINS:
            raise ValueError("child contract has an unsupported reconciliation mode")
        _json_copy(dict(child.contract), "child.contract")
        _json_copy(dict(child.payload), "child.payload")

    @staticmethod
    def _text_part(child: SupplierRequest) -> str:
        """The only content sent: the child payload, with no factory binding."""
        return _canonical(_json_copy(dict(child.payload), "child.payload")).decode("utf-8")

    def _expected(self, parent: ParentAssignment, child: SupplierRequest) -> OutcomeRecord:
        bound = {"parent": [parent.task_id, parent.run_id, parent.definition_digest,
                            parent.assignment_id, parent.attempt_id],
                 "child": [child.assignment_id, child.attempt_id],
                 "message": self._text_part(child), "supplier_identity": child.identity,
                 "supplier_role": child.role, "contract": dict(child.contract),
                 "expected_revision": child.expected_revision}
        payload_sha256 = hashlib.sha256(_canonical(bound)).hexdigest()
        receiver = (ReceiverKind.OPAQUE if child.contract.get("reconcile") == "opaque"
                    else ReceiverKind.PARTICIPATING)
        return submitted(child.action_id, child.run_id, child.definition_digest,
                         receiver, payload_sha256=payload_sha256,
                         pinned_identity=child.identity, message_id=str(uuid4()),
                         context_id=self.journal.context_for(child.run_id, child.identity))

    def _resolve(self, child: SupplierRequest) -> tuple[str, dict[str, Any]]:
        url, observed = a2a.resolve_pinned(
            self.snapshot_path, child.identity, dict(child.contract))
        resend = observed.get("resend") == a2a_extensions.RESEND_RULE
        if child.contract.get("reconcile") == "a2a-idempotent-resend" and not resend:
            raise SupplierBindingError("pinned resend mode is not declared by the Agent Card")
        return url, observed

    def _send(self, parent: ParentAssignment, child: SupplierRequest,
              record: OutcomeRecord, url: str) -> dict[str, Any]:
        try:
            task = a2a.send_async(url, self._text_part(child), message_id=record.message_id,
                                  context_id=record.context_id)
        except Exception:
            # The receiver might have committed before the connection failed.
            uncertain = self.journal.put(send_ambiguous(record))
            return self._view(parent, child, uncertain)
        return self._accept_task(parent, child, record, task)

    def _resume(self, parent: ParentAssignment, child: SupplierRequest,
                record: OutcomeRecord) -> dict[str, Any]:
        if record.phase == Phase.UNKNOWN:
            if record.receiver == ReceiverKind.OPAQUE:
                return self._view(parent, child, record)
            try:
                url, _observed = self._resolve(child)
                # The declared resend rule returns the original Task for the
                # journaled messageId; it cannot create a second effect.
                task = a2a.send_async(url, self._text_part(child),
                                      message_id=record.message_id,
                                      context_id=record.context_id)
            except Exception:
                return self._view(parent, child, record)
            return self._accept_task(parent, child, record, task)
        if record.phase == Phase.WORKING:
            return self._poll(parent, child, record, record.task_id)
        return self._view(parent, child, record)

    def _poll(self, parent: ParentAssignment, child: SupplierRequest,
              record: OutcomeRecord, task_id: str | None) -> dict[str, Any]:
        if not isinstance(task_id, str) or not task_id:
            return self._view(parent, child, record)
        try:
            url, _observed = self._resolve(child)
            task = a2a.get_task(url, task_id)
        except Exception:
            # A failed read is not evidence that the original Task disappeared.
            return self._view(parent, child, record)
        try:
            state = self._validate_task(child, record, task, task_id=task_id)
        except Exception:
            incident = self.journal.put(task_incident(
                record, "supplier-task-binding-inconsistent"))
            return self._view(parent, child, incident)
        if state in {"failed", "canceled", "rejected"}:
            incident = self.journal.put(task_incident(record, "supplier-task-not-completed"))
            return self._view(parent, child, incident)
        if state != "completed":
            return self._view(parent, child, record)
        return self._complete(parent, child, record, task)

    def _accept_task(self, parent: ParentAssignment, child: SupplierRequest,
                     record: OutcomeRecord, task: dict[str, Any]) -> dict[str, Any]:
        try:
            state = self._validate_task(child, record, task)
            record = self.journal.put(task_started(record, task["id"]))
        except Exception:
            incident = self.journal.put(task_incident(
                record, "supplier-task-binding-inconsistent"))
            return self._view(parent, child, incident)
        if record.phase == Phase.INCIDENT:
            return self._view(parent, child, record)
        if state in {"failed", "canceled", "rejected"}:
            incident = self.journal.put(task_incident(record, "supplier-task-not-completed"))
            return self._view(parent, child, incident)
        if state == "completed":
            return self._complete(parent, child, record, task)
        return self._view(parent, child, record)

    @staticmethod
    def _validate_task(child: SupplierRequest, record: OutcomeRecord,
                       task: Mapping[str, Any], *, task_id: str | None = None) -> str:
        return a2a.validate_async_task(dict(task), context_id=record.context_id,
                                       identity=child.identity,
                                       task_id=task_id or record.task_id)

    def _complete(self, parent: ParentAssignment, child: SupplierRequest,
                  record: OutcomeRecord, task: Mapping[str, Any]) -> dict[str, Any]:
        try:
            receipt = a2a.async_receipt(dict(task), {
                "action_id": record.action_id, "run_id": record.run_id,
                "definition_digest": record.definition_digest, "task_id": record.task_id,
                "context_id": record.context_id, "message_id": record.message_id},
                identity=child.identity, expected_revision=child.expected_revision,
                role=child.role)
            # Parent and child bindings come from the factory, never the agent.
            receipt.update({"parent_task_id": parent.task_id,
                            "parent_run_id": parent.run_id,
                            "parent_definition_digest": parent.definition_digest,
                            "parent_assignment_id": parent.assignment_id,
                            "parent_attempt_id": parent.attempt_id,
                            "assignment_id": child.assignment_id,
                            "attempt_id": child.attempt_id})
            confirmed = self.journal.put(task_finished(record, receipt))
        except Exception:
            incident = self.journal.put(task_incident(
                record, "supplier-artifact-binding-inconsistent"))
            return self._view(parent, child, incident)
        return self._view(parent, child, confirmed)

    @staticmethod
    def _view(parent: ParentAssignment, child: SupplierRequest,
              record: OutcomeRecord | None, *, reason: str | None = None) -> dict[str, Any]:
        value: dict[str, Any] = {
            "parent_task_id": parent.task_id,
            "parent_run_id": parent.run_id,
            "parent_definition_digest": parent.definition_digest,
            "parent_assignment_id": parent.assignment_id,
            "parent_attempt_id": parent.attempt_id,
            "supplier_identity": child.identity,
            "action_id": child.action_id,
            "run_id": child.run_id,
            "definition_digest": child.definition_digest,
            "assignment_id": child.assignment_id,
            "attempt_id": child.attempt_id,
            "task_id": record.task_id if record else None,
            "phase": record.phase.value if record else "unknown",
            "reason": reason if record is None else record.reason,
            "artifact": None,
        }
        if record and record.phase == Phase.CONFIRMED and record.receipt:
            value["artifact"] = record.receipt.get("artifact")
        return value
