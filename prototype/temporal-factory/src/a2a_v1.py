"""A2A v1.0 wire Adapter shared by every Exomachina A2A client and server.

Exomachina speaks A2A v1.0 only: there is no 0.3 interface, dual
advertisement, compatibility shim, or version-negotiation fallback. This module
holds the JSON-RPC wire vocabulary and the one boundary mapping from v1
``TASK_STATE_*`` values to the version-neutral state names Observation and the
factory use internally (``working``, ``input-required``, ...). It has no SDK
dependency so plain-HTTP clients and scenarios can use it directly.
"""
from __future__ import annotations

import math
from uuid import uuid4


PROTOCOL_VERSION = "1.0"
PROTOCOL = "a2a/1.0"
VERSION_HEADER = "A2A-Version"
EXTENSIONS_HEADER = "A2A-Extensions"
JSONRPC_BINDING = "JSONRPC"

SEND_MESSAGE = "SendMessage"
SEND_STREAMING_MESSAGE = "SendStreamingMessage"
GET_TASK = "GetTask"
CANCEL_TASK = "CancelTask"
SEND_METHODS = frozenset({SEND_MESSAGE, SEND_STREAMING_MESSAGE})

ROLE_USER = "ROLE_USER"
ROLE_AGENT = "ROLE_AGENT"
ROLES = frozenset({ROLE_USER, ROLE_AGENT})
PART_CONTENT_FIELDS = ("text", "raw", "url", "data")
JSON_MEDIA_TYPE = "application/json"

# The A2A Adapter boundary: v1 wire states <-> version-neutral state names.
# Observation's vocabulary (and every recording) uses the right-hand names.
_OBSERVED_STATE = {
    "TASK_STATE_SUBMITTED": "submitted",
    "TASK_STATE_WORKING": "working",
    "TASK_STATE_COMPLETED": "completed",
    "TASK_STATE_FAILED": "failed",
    "TASK_STATE_CANCELED": "canceled",
    "TASK_STATE_INPUT_REQUIRED": "input-required",
    "TASK_STATE_REJECTED": "rejected",
    "TASK_STATE_AUTH_REQUIRED": "auth-required",
}
_WIRE_STATE = {name: wire for wire, name in _OBSERVED_STATE.items()}
WIRE_STATES = frozenset(_OBSERVED_STATE)


class ProtocolError(ValueError):
    """A peer response or request is not valid A2A v1.0."""


def observed_state(wire: object) -> str:
    """Map a v1 ``TASK_STATE_*`` value to its version-neutral state name."""
    if not isinstance(wire, str) or wire not in _OBSERVED_STATE:
        raise ProtocolError(f"not an A2A v1 task state: {wire!r}")
    return _OBSERVED_STATE[wire]


def wire_state(name: str) -> str:
    """Map a version-neutral state name to its v1 ``TASK_STATE_*`` value."""
    try:
        return _WIRE_STATE[name]
    except KeyError:
        raise ProtocolError(f"no A2A v1 task state for {name!r}") from None


def normalize_numbers(value):
    """Undo protobuf ``Value`` widening: integral doubles become ints again.

    JSON numbers carry no int/float distinction, but v1 SDKs encode ``data``
    parts and metadata as ``google.protobuf.Value`` doubles (``1`` -> ``1.0``).
    Exomachina digests canonical JSON, so integral values are restored.
    """
    if isinstance(value, float):
        if math.isfinite(value) and value.is_integer() and abs(value) <= 2 ** 53:
            return int(value)
        return value
    if isinstance(value, dict):
        return {key: normalize_numbers(item) for key, item in value.items()}
    if isinstance(value, list):
        return [normalize_numbers(item) for item in value]
    return value


def headers(extensions=()) -> dict:
    """Per-request A2A service parameters for the HTTP JSON-RPC binding."""
    values = {VERSION_HEADER: PROTOCOL_VERSION}
    extensions = [uri for uri in extensions if uri]
    if extensions:
        values[EXTENSIONS_HEADER] = ",".join(sorted(set(extensions)))
    return values


def data_part(value, media_type: str = JSON_MEDIA_TYPE) -> dict:
    return {"data": value, "mediaType": media_type}


def text_part(text: str, media_type: str = "text/plain") -> dict:
    return {"text": text, "mediaType": media_type}


def user_message(parts: list, *, task_id: str | None = None, context_id: str | None = None,
                 message_id: str | None = None) -> dict:
    message = {"role": ROLE_USER, "messageId": message_id or str(uuid4()), "parts": list(parts)}
    if task_id:
        message["taskId"] = task_id
    if context_id:
        message["contextId"] = context_id
    return message


def send_params(message: dict, *, return_immediately: bool = False) -> dict:
    params = {"message": message}
    if return_immediately:
        params["configuration"] = {"returnImmediately": True}
    return params


def rpc(method: str, params: dict, request_id: str | None = None) -> dict:
    return {"jsonrpc": "2.0", "id": request_id or str(uuid4()), "method": method,
            "params": params}


def part_content(part: object) -> str:
    """Return which v1 content field a Part carries; reject 0.3 shapes."""
    if not isinstance(part, dict):
        raise ProtocolError("A2A v1 Part must be an object")
    if "kind" in part:
        raise ProtocolError("A2A 0.3 'kind' discriminator is not accepted on a v1 Part")
    present = [field for field in PART_CONTENT_FIELDS if field in part]
    if len(present) != 1:
        raise ProtocolError("A2A v1 Part must hold exactly one of text, raw, url, or data")
    return present[0]


def part_data(part: object):
    if part_content(part) != "data":
        raise ProtocolError("expected an A2A v1 data Part")
    return normalize_numbers(part["data"])


def part_text(part: object) -> str:
    if part_content(part) != "text" or not isinstance(part["text"], str):
        raise ProtocolError("expected an A2A v1 text Part")
    return part["text"]


def unwrap_send_result(result: object) -> tuple[str, dict]:
    """Split a v1 SendMessage result into (``task``|``message``, object)."""
    if not isinstance(result, dict) or "kind" in result:
        raise ProtocolError("SendMessage result is not an A2A v1 response")
    present = [key for key in ("task", "message") if key in result]
    if len(present) != 1 or not isinstance(result[present[0]], dict):
        raise ProtocolError("SendMessage result must hold exactly one of task or message")
    return present[0], normalize_numbers(result[present[0]])


def require_task(result: object) -> dict:
    shape, value = unwrap_send_result(result)
    if shape != "task":
        raise ProtocolError("A2A v1 SendMessage returned a Message where a Task was required")
    return value


def task_state(task: object) -> str:
    """Version-neutral state of a v1 Task object."""
    if not isinstance(task, dict) or "kind" in task:
        raise ProtocolError("not an A2A v1 Task")
    return observed_state((task.get("status") or {}).get("state"))


def message_parts(message: object) -> list:
    if not isinstance(message, dict) or "kind" in message:
        raise ProtocolError("not an A2A v1 Message")
    parts = message.get("parts") or []
    for part in parts:
        part_content(part)
    return parts


def request_violation(body: object) -> str | None:
    """Strict v1 checks the SDK's lenient protobuf parse would let through.

    The SDK ignores unknown JSON fields, so a 0.3 ``kind`` on a Part or an
    unknown ``role`` would otherwise be silently dropped or defaulted.
    """
    if not isinstance(body, dict) or body.get("method") not in SEND_METHODS:
        return None
    params = body.get("params")
    message = params.get("message") if isinstance(params, dict) else None
    if not isinstance(message, dict):
        return "A2A v1 SendMessage requires params.message"
    if "kind" in message:
        return "A2A 0.3 'kind' discriminator is not accepted on a v1 Message"
    if message.get("role") not in ROLES:
        return "A2A v1 Message role must be ROLE_USER or ROLE_AGENT"
    parts = message.get("parts")
    if not isinstance(parts, list) or not parts:
        return "A2A v1 Message requires at least one Part"
    try:
        for part in parts:
            part_content(part)
    except ProtocolError as error:
        return str(error)
    configuration = params.get("configuration")
    if isinstance(configuration, dict) and "blocking" in configuration:
        return "A2A 0.3 'blocking' is not accepted; use returnImmediately"
    return None


def card_interface(card: object) -> dict:
    """Validate a v1-only Agent Card and return its JSON-RPC interface."""
    if not isinstance(card, dict):
        raise ProtocolError("Agent Card must be an object")
    for legacy in ("url", "protocolVersion", "preferredTransport", "additionalInterfaces"):
        if legacy in card:
            raise ProtocolError(f"A2A 0.3 Agent Card field {legacy!r} is not accepted")
    interfaces = card.get("supportedInterfaces")
    if not isinstance(interfaces, list) or not interfaces:
        raise ProtocolError("Agent Card must advertise supportedInterfaces")
    for interface in interfaces:
        if not isinstance(interface, dict) or interface.get("protocolVersion") != PROTOCOL_VERSION:
            raise ProtocolError("Agent Card advertises a non-v1.0 interface")
    jsonrpc = [interface for interface in interfaces
               if interface.get("protocolBinding") == JSONRPC_BINDING]
    if len(jsonrpc) != 1 or not isinstance(jsonrpc[0].get("url"), str):
        raise ProtocolError("Agent Card must advertise exactly one v1 JSON-RPC interface")
    return jsonrpc[0]


def card_url(card: object) -> str:
    return card_interface(card)["url"]


def card_without_endpoint(card: dict) -> dict:
    """The endpoint-free Agent Card projection Exomachina pins by digest."""
    projection = {key: value for key, value in card.items() if key != "supportedInterfaces"}
    projection["supportedInterfaces"] = [
        {key: value for key, value in interface.items() if key != "url"}
        for interface in card.get("supportedInterfaces") or []]
    return projection


def required_extensions(card: dict) -> list[str]:
    return sorted(extension.get("uri") for extension in
                  (card.get("capabilities") or {}).get("extensions") or []
                  if extension.get("required") is True and extension.get("uri"))
