"""Content-free hand-off records at the factory's own A2A boundary.

A2A v1 mediation decision 4 (`docs/a2a-v1-mediation-decision-2026-10-07.md`).
The factory, never an agent, describes what travelled between stations: one
item per completed-Task artifact (or one `message` item), each with its part
kinds, media type, byte length, ready time and a keyed digest. Item digests are
HMAC-SHA256 under a per-factory-instance key kept in the instance home (mode
0600). Only Activity code calls this module: the key never enters Workflow
inputs or history, logs, Observation, evidence or git. Records hold no text,
data, bytes, artifact names, descriptions, artifactIds, filenames, URLs or
metadata.
"""
from __future__ import annotations

import base64
import binascii
import copy
import hashlib
import hmac
import json
import os
import re
import secrets
from datetime import datetime, timezone
from pathlib import Path

import a2a_v1

KEY_ENV = "EXO_HANDOFF_KEY_FILE"
KEY_FILE_NAME = "handoff-digest.key"
KEY_BYTES = 32
OUTPUT_MODES = ("artifacts", "message", "none")
MAX_ITEMS = 256
# Declared input size bound (decision 9): the JSON-encoded A2A Message the
# factory dispatches. An input that does not fit fails loudly at the node.
MAX_MESSAGE_BYTES = 4_000_000
COMPOSITION_MISMATCH = "input.composition-mismatch"
OVERSIZE = "input.oversize"
_MEDIA = re.compile(r"^[a-z0-9.+-]+/[a-z0-9.+-]+$")


class OutputMissing(ValueError):
    """A completed Task did not satisfy its node's output contract."""

    reason = "output.missing"


class CompositionError(ValueError):
    """The node's input cannot be composed as recorded; nothing is sent."""

    def __init__(self, reason: str, message: str):
        super().__init__(message)
        self.reason = reason


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def key_path() -> Path | None:
    """The configured instance key path, or None when records are disabled."""
    value = os.environ.get(KEY_ENV)
    return Path(value) if value else None


def instance_key(path: Path | None = None) -> bytes | None:
    """Load the instance digest key, creating it (0600) on first use."""
    path = path if path is not None else key_path()
    if path is None:
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        pass
    else:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(secrets.token_bytes(KEY_BYTES))
            stream.flush()
            os.fsync(stream.fileno())
    if path.stat().st_mode & 0o077:
        raise PermissionError("hand-off digest key must be private to its owner")
    key = path.read_bytes()
    if len(key) != KEY_BYTES:
        raise ValueError("hand-off digest key is malformed")
    return key


def _part_value(part: dict) -> tuple[str, object, int | None]:
    """(kind, canonical value, byte length) of one v1 Part; content stays local."""
    kind = a2a_v1.part_content(part)
    value = part[kind]
    if kind == "text":
        if not isinstance(value, str):
            raise ValueError("text Part must be a string")
        return kind, value, len(value.encode("utf-8"))
    if kind == "raw":
        if not isinstance(value, str):
            raise ValueError("raw Part must be base64 text")
        try:
            size = len(base64.b64decode(value, validate=True))
        except (binascii.Error, ValueError) as error:
            raise ValueError("raw Part is not base64") from error
        return kind, value, size
    if kind == "url":
        if not isinstance(value, str):
            raise ValueError("url Part must be a string")
        return kind, value, None
    data = a2a_v1.normalize_numbers(value)
    encoded = json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return kind, data, len(encoded.encode("utf-8"))


def _media_type(parts: list[dict]) -> str | None:
    """A2A `mediaType` is optional; a missing or non-simple type records null."""
    declared = parts[0].get("mediaType") if parts else None
    if not isinstance(declared, str):
        return None
    simple = declared.split(";", 1)[0].strip().lower()
    return simple if len(simple) <= 127 and _MEDIA.fullmatch(simple) else None


def describe_item(parts: list, *, index: int, source: str, ready_at: str,
                  key: bytes) -> dict:
    """One content-free hand-off item for an artifact's (or message's) parts."""
    if not isinstance(parts, list) or not parts or len(parts) > 64:
        raise ValueError("hand-off item requires 1..64 Parts")
    values = [_part_value(part) for part in parts]
    kinds = list(dict.fromkeys(kind for kind, _, _ in values))
    sizes = [size for _, _, size in values]
    payload = json.dumps([[kind, value] for kind, value, _ in values], sort_keys=True,
                         separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return {"item_index": index, "source": source, "part_kinds": kinds,
            "media_type": _media_type(parts),
            "byte_length": None if None in sizes else sum(sizes),
            "ready_at": ready_at,
            "digest": hmac.new(key, payload, hashlib.sha256).hexdigest()}


def enforce_output(task: dict, mode: str) -> list[tuple[str, list]]:
    """Apply a node's output contract to a completed v1 Task.

    Returns the (source, parts) of each item. `artifacts` requires at least one
    artifact and never yields an empty hand-off; `message` yields one item from
    the Task's status message; `none` yields nothing.
    """
    if mode not in OUTPUT_MODES:
        raise ValueError("unknown node output mode")
    if mode == "none":
        return []
    if mode == "message":
        message = (task.get("status") or {}).get("message") if isinstance(task, dict) else None
        parts = a2a_v1.message_parts(message) if isinstance(message, dict) else []
        if not parts:
            raise OutputMissing("message output node completed without a message")
        return [("message", parts)]
    artifacts = task.get("artifacts") if isinstance(task, dict) else None
    if not isinstance(artifacts, list) or not artifacts:
        raise OutputMissing("artifacts output node completed without an artifact")
    if len(artifacts) > MAX_ITEMS:
        raise ValueError("too many artifacts for one hand-off")
    result = []
    for artifact in artifacts:
        parts = artifact.get("parts") if isinstance(artifact, dict) else None
        if not isinstance(parts, list) or not parts:
            raise OutputMissing("artifact completed without a Part")
        result.append(("artifact", parts))
    return result


def produced_items(task: dict, mode: str, *, key: bytes | None,
                   ready_at: str | None = None,
                   streamed: dict[int, str] | None = None) -> list[dict] | None:
    """Enforce the output contract, then describe its items (None without a key)."""
    sources = enforce_output(task, mode)
    if key is None or not sources:
        return None
    ready_at = ready_at or now_iso()
    streamed = streamed or {}
    return [describe_item(parts, index=index, source=source,
                          ready_at=streamed.get(index, ready_at), key=key)
            for index, (source, parts) in enumerate(sources)]


def consumed_inputs(upstream: list) -> list[dict]:
    """Name each upstream produced hand-off once with all of its item digests."""
    inputs: dict[str, list[str]] = {}
    for record in upstream:
        if not isinstance(record, dict) or not isinstance(record.get("handoff_id"), str):
            continue
        digests = [item["digest"] for item in record.get("items") or []
                   if isinstance(item, dict) and isinstance(item.get("digest"), str)]
        if digests:
            inputs[record["handoff_id"]] = digests
    return [{"handoff_id": handoff_id, "item_digests": digests}
            for handoff_id, digests in inputs.items()]


def stream_ready_items(events, *, clock=now_iso) -> tuple[dict[int, str], list[dict]]:
    """Track artifact chunks in a v1 `SendStreamingMessage` response stream.

    Each `artifactUpdate` with `lastChunk: true` marks that artifact ready at
    the time the factory received it. Returns (item_index -> ready_at, ready
    records in completion order); item indexes follow first appearance, the
    order the completed Task lists its artifacts.
    """
    order: dict[str, int] = {}
    kinds_seen: dict[int, list[str]] = {}
    media: dict[int, str | None] = {}
    ready: dict[int, str] = {}
    records: list[dict] = []
    for event in events:
        update = event.get("artifactUpdate") if isinstance(event, dict) else None
        if not isinstance(update, dict) or not isinstance(update.get("artifact"), dict):
            continue
        artifact = update["artifact"]
        identity = artifact.get("artifactId")
        if not isinstance(identity, str):
            continue
        index = order.setdefault(identity, len(order))
        parts = artifact.get("parts") or []
        kinds = kinds_seen.setdefault(index, [])
        kinds.extend(kind for kind in (a2a_v1.part_content(part) for part in parts)
                     if kind not in kinds)
        if index not in media and parts:
            media[index] = _media_type(parts)
        if update.get("lastChunk") is True and index not in ready and kinds:
            ready[index] = clock()
            records.append({"item_index": index, "part_kinds": list(kinds),
                            "media_type": media.get(index), "ready_at": ready[index]})
    return ready, records


def fallback_item_parts(content: str) -> list[list[dict]]:
    """Item parts for a result recorded before receipts carried ``item_parts``."""
    return [[{"text": content, "mediaType": a2a_v1.JSON_MEDIA_TYPE}]]


def _upstream(upstream: object) -> list[dict]:
    if not isinstance(upstream, list):
        raise CompositionError(COMPOSITION_MISMATCH, "upstream must be a list")
    for entry in upstream:
        items = entry.get("item_parts") if isinstance(entry, dict) else None
        if (not isinstance(items, list) or not items or
                not all(isinstance(item, list) and item for item in items)):
            raise CompositionError(COMPOSITION_MISMATCH, "upstream item without Parts")
        for item in items:
            for part in item:
                try:
                    a2a_v1.part_content(part)
                except a2a_v1.ProtocolError as error:
                    raise CompositionError(COMPOSITION_MISMATCH, str(error)) from error
    return upstream


def verify_consumed(consumes: list, upstream: list, *, key: bytes) -> None:
    """``consumes`` must name exactly the hand-offs whose items are included.

    Same hand-off ids in the same order, and each included item's keyed digest
    equals the produced record's item digest.
    """
    try:
        included = [(entry.get("handoff_id"),
                     [describe_item(item, index=index, source="artifact", ready_at="",
                                    key=key)["digest"]
                      for index, item in enumerate(entry["item_parts"])])
                    for entry in _upstream(upstream)]
    except (ValueError, TypeError) as error:
        if isinstance(error, CompositionError):
            raise
        raise CompositionError(COMPOSITION_MISMATCH, str(error)) from error
    recorded = [(entry.get("handoff_id"), list(entry.get("item_digests") or []))
                for entry in consumes if isinstance(entry, dict)]
    if len(recorded) != len(consumes) or included != recorded:
        raise CompositionError(COMPOSITION_MISMATCH,
                               "consumed hand-offs differ from the composed items")


def compose_parts(lead: list[dict], upstream: list, *, consumes: list | None = None,
                  key: bytes | None = None) -> list[dict]:
    """Before-dispatch composition of one A2A Message's Parts (decision 9).

    ``lead`` is the node's own Parts (its brief, or nothing for release),
    followed by every consumed hand-off item's Parts copied verbatim from the
    producing artifact: same part kind, value, ``mediaType`` and ``filename``,
    in hand-off order and item order. Nothing is re-encoded or embedded. With
    an instance key and a recorded ``consumes``, the composition must match it.
    """
    upstream = _upstream(upstream)
    if key is not None and consumes is not None:
        verify_consumed(consumes, upstream, key=key)
    return [copy.deepcopy(part) for part in lead] + [
        copy.deepcopy(part) for entry in upstream for item in entry["item_parts"]
        for part in item]


def message_size(message: dict) -> int:
    return len(json.dumps(message, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False).encode("utf-8"))


def require_within_bound(message: dict) -> dict:
    """Fail loudly when the composed Message exceeds the declared bound."""
    size = message_size(message)
    if size > MAX_MESSAGE_BYTES:
        raise CompositionError(OVERSIZE, f"composed Message is {size} bytes; "
                               f"the bound is {MAX_MESSAGE_BYTES}")
    return message
