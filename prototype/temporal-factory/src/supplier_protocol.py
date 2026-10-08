"""Plain A2A requests to a factory acting as an agent service (nested supplier).

A factory that opts in (``nested_supplier_enabled``) accepts an ordinary A2A
v1 Message from any client (A2A decisions 7 and 9). The Message's one Part is
the brief: the run inputs object of the factory's active root definition, as
a ``data`` Part or a JSON ``text`` Part with ``mediaType: application/json``.
No Exomachina extension, Task metadata or caller-side identifier (run,
assignment, attempt, action, definition digest, parent bindings) is required
or accepted. The receiving factory derives its own action and run identity
from the A2A ``messageId`` and its active publication, and the caller keeps
its correlation on its side through the A2A ``taskId``/``contextId``.

This module validates and fingerprints the received Parts only. It does not
create Tasks, persist state or authorize a run; the accepting factory checks
the inputs against its pinned run-input schema and caller authority.
"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from typing import Any, Mapping, Sequence


class SupplierRequestError(ValueError):
    """A plain A2A request to a nested-supplier factory is invalid."""


JSON_MEDIA_TYPE = "application/json"
# Bounds on the canonical JSON run inputs (the whole brief object). Container
# depth counts the root inputs object as depth one.
MAX_CANONICAL_INPUT_BYTES = 128 * 1024
MAX_INPUT_NESTING = 32


@dataclass(frozen=True)
class NestedRequest:
    """Detached, validated run inputs from one received brief Part."""

    inputs: dict[str, Any]

    def fingerprint(self) -> str:
        """SHA-256 of the canonical inputs: the content a resent messageId must repeat."""
        canonical = json.dumps(self.inputs, sort_keys=True, separators=(",", ":"),
                               ensure_ascii=False, allow_nan=False).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()


def is_nested_request(parts: Sequence[Mapping[str, Any]]) -> bool:
    """True when the Message's first Part is a structured (JSON) brief.

    A ``data`` Part, or a ``text`` Part declared ``application/json``. Plain
    text briefs stay with the factory's Director.
    """
    if not parts or not isinstance(parts[0], Mapping):
        return False
    first = parts[0]
    if "data" in first:
        return True
    media = first.get("mediaType")
    return ("text" in first and isinstance(media, str)
            and media.split(";", 1)[0].strip().lower() == JSON_MEDIA_TYPE)


def parse_nested_request(parts: Sequence[Mapping[str, Any]]) -> NestedRequest:
    """Validate one received Message's Parts as a nested-supplier brief.

    Exactly one Part: the run inputs object. Upstream items have no place in a
    root definition's run inputs, so additional Parts are refused rather than
    silently ignored.
    """
    if not isinstance(parts, Sequence) or isinstance(parts, (str, bytes)):
        raise SupplierRequestError("Message Parts must be a list")
    if len(parts) != 1:
        raise SupplierRequestError("a nested-supplier Message carries exactly one brief Part")
    if not is_nested_request(parts):
        raise SupplierRequestError(
            "the brief must be a data Part or an application/json text Part")
    part = parts[0]
    if "data" in part:
        value = part["data"]
    else:
        text = part["text"]
        if not isinstance(text, str):
            raise SupplierRequestError("the brief text must be a string")
        if len(text.encode("utf-8", "surrogatepass")) > MAX_CANONICAL_INPUT_BYTES * 2:
            raise SupplierRequestError(
                f"brief exceeds {MAX_CANONICAL_INPUT_BYTES} canonical bytes")
        try:
            value = json.loads(text)
        except ValueError as error:
            raise SupplierRequestError("the brief text is not JSON") from error
    if type(value) is not dict:
        raise SupplierRequestError("the brief must be a JSON object of run inputs")
    return NestedRequest(inputs=_copy_json(value, "brief"))


def _copy_json(value: Any, name: str) -> Any:
    """Bound canonical JSON size/depth before making a detached copy."""
    _canonical_json_size(value, name)
    def visit(item: Any, path: str) -> None:
        if item is None or type(item) in (bool, int, str):
            return
        if type(item) is float:
            if not math.isfinite(item):
                raise SupplierRequestError(f"{path} must contain only finite JSON numbers")
            return
        if type(item) is list:
            for index, child in enumerate(item):
                visit(child, f"{path}[{index}]")
            return
        if type(item) is dict:
            for key, child in item.items():
                if type(key) is not str:
                    raise SupplierRequestError(f"{path} object keys must be strings")
                visit(child, f"{path}.{key}")
            return
        raise SupplierRequestError(f"{path} must contain only finite JSON values")

    try:
        visit(value, name)
        return json.loads(json.dumps(
            value, sort_keys=True, separators=(",", ":"),
            ensure_ascii=False, allow_nan=False,
        ))
    except SupplierRequestError:
        raise
    except (TypeError, ValueError, RecursionError) as error:
        raise SupplierRequestError(f"{name} must be finite JSON data") from error


def _canonical_json_size(value: Any, name: str) -> int:
    """Measure canonical UTF-8 JSON without serializing or copying the tree."""
    total = 0

    def add(amount: int, path: str) -> None:
        nonlocal total
        total += amount
        if total > MAX_CANONICAL_INPUT_BYTES:
            raise SupplierRequestError(
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
                raise SupplierRequestError(f"{path} contains a non-UTF-8 surrogate")
            elif codepoint < 0x80:
                size += 1
            elif codepoint < 0x800:
                size += 2
            elif codepoint < 0x10000:
                size += 3
            else:
                size += 4
            if size > MAX_CANONICAL_INPUT_BYTES:
                raise SupplierRequestError(
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
                raise SupplierRequestError(
                    f"{name} integer at {path} cannot fit the canonical byte limit")
            try:
                add(len(str(item)), path)
            except ValueError as error:
                raise SupplierRequestError(
                    f"{name} integer at {path} cannot be represented as JSON") from error
        elif type(item) is float:
            if not math.isfinite(item):
                raise SupplierRequestError(f"{path} must contain only finite JSON numbers")
            add(len(json.dumps(item, allow_nan=False, separators=(",", ":"))), path)
        elif type(item) in (dict, list):
            depth = container_depth + 1
            if depth > MAX_INPUT_NESTING:
                raise SupplierRequestError(
                    f"{name} exceeds maximum container nesting {MAX_INPUT_NESTING} at {path}")
            add(2, path)  # braces or brackets
            if type(item) is dict:
                first = True
                for key, child in item.items():
                    if type(key) is not str:
                        raise SupplierRequestError(f"{path} object keys must be strings")
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
            raise SupplierRequestError(f"{path} must contain only finite JSON values")

    measure(value, name)
    return total
