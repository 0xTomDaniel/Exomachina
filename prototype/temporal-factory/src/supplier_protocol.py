"""Strict protocol helpers for opt-in nested-factory A2A commands.

This module validates and projects protocol data only. It does not create
Tasks, persist state, contact a supplier, or authorize a nested execution.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from typing import Any, Mapping


class SupplierEnvelopeError(ValueError):
    """A nested-factory envelope or its assignment echo is invalid."""


PARENT_FIELDS = (
    "parent_task_id",
    "parent_run_id",
    "parent_definition_digest",
    "parent_assignment_id",
    "parent_attempt_id",
)
CHILD_FIELDS = (
    "action_id",
    "run_id",
    "definition_digest",
    "assignment_id",
    "attempt_id",
)
SUPPLIER_ECHO_FIELDS = tuple(sorted((*PARENT_FIELDS, *CHILD_FIELDS)))
# Bounds apply only to this opt-in nested_factory profile. Input bytes count
# the canonical JSON value at payload.inputs (excluding the wrapper). Container
# depth counts the root inputs object as depth one.
MAX_CANONICAL_INPUT_BYTES = 128 * 1024
MAX_INPUT_NESTING = 32
_ENVELOPE_FIELDS = frozenset((
    "op", "action_id", "run_id", "definition_digest",
    *PARENT_FIELDS, "assignment_id", "attempt_id", "payload",
))
_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}\Z", re.ASCII)
_DIGEST_PATTERN = re.compile(r"[0-9a-f]{64}\Z", re.ASCII)
_DECLARATION = {"version": 1, "fields": list(SUPPLIER_ECHO_FIELDS)}


@dataclass(frozen=True)
class NestedFactoryEnvelope:
    """Validated caller-supplied binding tuple and detached JSON inputs."""

    action_id: str
    run_id: str
    definition_digest: str
    parent_task_id: str
    parent_run_id: str
    parent_definition_digest: str
    parent_assignment_id: str
    parent_attempt_id: str
    assignment_id: str
    attempt_id: str
    inputs: dict[str, Any]

    def echo_tuple(self) -> dict[str, str]:
        """Return the exact ten declared binding fields in canonical order."""
        return {field: getattr(self, field) for field in SUPPLIER_ECHO_FIELDS}

    def to_command(self) -> dict[str, Any]:
        """Return a detached canonical command suitable for fingerprinting."""
        return {
            "op": "nested_factory",
            "action_id": self.action_id,
            "run_id": self.run_id,
            "definition_digest": self.definition_digest,
            "parent_task_id": self.parent_task_id,
            "parent_run_id": self.parent_run_id,
            "parent_definition_digest": self.parent_definition_digest,
            "parent_assignment_id": self.parent_assignment_id,
            "parent_attempt_id": self.parent_attempt_id,
            "assignment_id": self.assignment_id,
            "attempt_id": self.attempt_id,
            "payload": {"inputs": _copy_json(self.inputs, "inputs")},
        }


def supplier_assignment_echo_declaration() -> dict[str, Any]:
    """Return a fresh declaration for the ten explicit parent/child fields."""
    return {"version": 1, "fields": list(_DECLARATION["fields"])}


def _copy_json(value: Any, name: str) -> Any:
    """Bound canonical JSON size/depth before making a detached copy."""
    _canonical_json_size(value, name)
    def visit(item: Any, path: str) -> None:
        if item is None or type(item) in (bool, int, str):
            return
        if type(item) is float:
            if not math.isfinite(item):
                raise SupplierEnvelopeError(f"{path} must contain only finite JSON numbers")
            return
        if type(item) is list:
            for index, child in enumerate(item):
                visit(child, f"{path}[{index}]")
            return
        if type(item) is dict:
            for key, child in item.items():
                if type(key) is not str:
                    raise SupplierEnvelopeError(f"{path} object keys must be strings")
                visit(child, f"{path}.{key}")
            return
        raise SupplierEnvelopeError(f"{path} must contain only finite JSON values")

    try:
        visit(value, name)
        return json.loads(json.dumps(
            value, sort_keys=True, separators=(",", ":"),
            ensure_ascii=False, allow_nan=False,
        ))
    except SupplierEnvelopeError:
        raise
    except (TypeError, ValueError, RecursionError) as error:
        raise SupplierEnvelopeError(f"{name} must be finite JSON data") from error


def _canonical_json_size(value: Any, name: str) -> int:
    """Measure canonical UTF-8 JSON without serializing or copying the tree."""
    total = 0

    def add(amount: int, path: str) -> None:
        nonlocal total
        total += amount
        if total > MAX_CANONICAL_INPUT_BYTES:
            raise SupplierEnvelopeError(
                f"{name} exceeds {MAX_CANONICAL_INPUT_BYTES} canonical bytes at {path}")

    def string_size(text: str, path: str) -> int:
        # Match json.dumps(..., ensure_ascii=False) escaping without constructing
        # a second potentially large string. Lone surrogates are not UTF-8.
        size = 2  # surrounding quotes
        for character in text:
            codepoint = ord(character)
            if character in ('"', "\\") or character in "\b\t\n\f\r":
                size += 2
            elif codepoint < 0x20:
                size += 6  # lowercase \\u00xx JSON escape
            elif 0xD800 <= codepoint <= 0xDFFF:
                raise SupplierEnvelopeError(f"{path} contains a non-UTF-8 surrogate")
            elif codepoint < 0x80:
                size += 1
            elif codepoint < 0x800:
                size += 2
            elif codepoint < 0x10000:
                size += 3
            else:
                size += 4
            if size > MAX_CANONICAL_INPUT_BYTES:
                raise SupplierEnvelopeError(
                    f"{name} exceeds {MAX_CANONICAL_INPUT_BYTES} canonical bytes at {path}")
        return size

    def measure(item: Any, path: str, container_depth: int = 0) -> None:
        if item is None:
            add(4, path)
        elif type(item) is bool:
            add(4 if item else 5, path)
        elif type(item) is str:
            add(string_size(item, path), path)
        elif type(item) is int:
            # Avoid converting arbitrarily large integers merely to learn that
            # their decimal representation cannot fit the profile limit.
            if abs(item).bit_length() > (MAX_CANONICAL_INPUT_BYTES + 1) * 4:
                raise SupplierEnvelopeError(
                    f"{name} integer at {path} cannot fit the canonical byte limit")
            try:
                add(len(str(item)), path)
            except ValueError as error:
                raise SupplierEnvelopeError(
                    f"{name} integer at {path} cannot be represented as JSON") from error
        elif type(item) is float:
            if not math.isfinite(item):
                raise SupplierEnvelopeError(f"{path} must contain only finite JSON numbers")
            add(len(json.dumps(item, allow_nan=False, separators=(",", ":"))), path)
        elif type(item) in (dict, list):
            depth = container_depth + 1
            if depth > MAX_INPUT_NESTING:
                raise SupplierEnvelopeError(
                    f"{name} exceeds maximum container nesting {MAX_INPUT_NESTING} at {path}")
            add(2, path)  # braces or brackets
            if type(item) is dict:
                first = True
                for key, child in item.items():
                    if type(key) is not str:
                        raise SupplierEnvelopeError(f"{path} object keys must be strings")
                    if not first:
                        add(1, path)
                    first = False
                    add(string_size(key, f"{path} object key"), path)
                    add(1, path)  # colon
                    measure(child, f"{path}.{key}", depth)
            else:
                for index, child in enumerate(item):
                    if index:
                        add(1, path)
                    measure(child, f"{path}[{index}]", depth)
        else:
            raise SupplierEnvelopeError(f"{path} must contain only finite JSON values")

    measure(value, name)
    return total


def _require_identifier(value: Any, field: str) -> str:
    if type(value) is not str or _ID_PATTERN.fullmatch(value) is None:
        raise SupplierEnvelopeError(f"{field} must be a safe opaque identifier")
    return value


def _require_digest(value: Any, field: str) -> str:
    if type(value) is not str or _DIGEST_PATTERN.fullmatch(value) is None:
        raise SupplierEnvelopeError(f"{field} must be a lowercase SHA-256 digest")
    return value


def parse_nested_factory_envelope(command: Any) -> NestedFactoryEnvelope:
    """Validate the exact nested_factory command envelope.

    Caller-supplied run IDs remain opaque. This parser does not establish that
    the caller is authorized to select one or that its digest matches an active
    publication; the accepting factory must enforce those runtime checks.
    """
    if not isinstance(command, Mapping):
        raise SupplierEnvelopeError("command must be an object")
    if any(type(key) is not str for key in command):
        raise SupplierEnvelopeError("command keys must be strings")
    keys = set(command)
    if keys != _ENVELOPE_FIELDS:
        missing = sorted(_ENVELOPE_FIELDS - keys)
        extra = sorted(keys - _ENVELOPE_FIELDS)
        raise SupplierEnvelopeError(f"command fields mismatch (missing={missing}, extra={extra})")
    if type(command["op"]) is not str or command["op"] != "nested_factory":
        raise SupplierEnvelopeError("op must be nested_factory")

    identifiers = {
        field: _require_identifier(command[field], field)
        for field in (*PARENT_FIELDS[:2], PARENT_FIELDS[3], PARENT_FIELDS[4],
                      "action_id", "run_id", "assignment_id", "attempt_id")
    }
    digests = {
        field: _require_digest(command[field], field)
        for field in ("definition_digest", "parent_definition_digest")
    }
    payload = command["payload"]
    if type(payload) is not dict or set(payload) != {"inputs"}:
        raise SupplierEnvelopeError("payload must contain exactly inputs")
    if type(payload["inputs"]) is not dict:
        raise SupplierEnvelopeError("payload.inputs must be an object")
    inputs = _copy_json(payload["inputs"], "payload.inputs")

    return NestedFactoryEnvelope(
        action_id=identifiers["action_id"],
        run_id=identifiers["run_id"],
        definition_digest=digests["definition_digest"],
        parent_task_id=identifiers["parent_task_id"],
        parent_run_id=identifiers["parent_run_id"],
        parent_definition_digest=digests["parent_definition_digest"],
        parent_assignment_id=identifiers["parent_assignment_id"],
        parent_attempt_id=identifiers["parent_attempt_id"],
        assignment_id=identifiers["assignment_id"],
        attempt_id=identifiers["attempt_id"],
        inputs=inputs,
    )


def project_supplier_echo(value: Any) -> dict[str, str]:
    """Validate and return only the ten declared echo fields from a record."""
    if not isinstance(value, Mapping):
        raise SupplierEnvelopeError("supplier echo must be an object")
    projected: dict[str, str] = {}
    for field in SUPPLIER_ECHO_FIELDS:
        if field not in value:
            raise SupplierEnvelopeError(f"supplier echo is missing {field}")
        if field in {"definition_digest", "parent_definition_digest"}:
            projected[field] = _require_digest(value[field], field)
        else:
            projected[field] = _require_identifier(value[field], field)
    return projected


def validate_supplier_echo(envelope: NestedFactoryEnvelope,
                           value: Any) -> dict[str, str]:
    """Project an echo and reject any changed parent or child binding."""
    if not isinstance(envelope, NestedFactoryEnvelope):
        raise TypeError("envelope must be a NestedFactoryEnvelope")
    projected = project_supplier_echo(value)
    expected = envelope.echo_tuple()
    mismatches = [field for field in SUPPLIER_ECHO_FIELDS
                  if projected[field] != expected[field]]
    if mismatches:
        raise SupplierEnvelopeError(
            "supplier echo binding mismatch: " + ", ".join(mismatches))
    return projected


def nested_factory_fingerprint(command: Any) -> str:
    """Return SHA-256 of the canonical, fully validated command envelope."""
    envelope = (command if isinstance(command, NestedFactoryEnvelope)
                else parse_nested_factory_envelope(command))
    canonical = json.dumps(
        envelope.to_command(), sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()
