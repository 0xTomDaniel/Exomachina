"""Small A2A v1.0 JSON-RPC client for factory-side calls to agent services.

This is the client side of the A2A Adapter boundary: requests carry the
``A2A-Version: 1.0`` header, Tasks arrive wrapped as ``{"task": ...}``, and
``TASK_STATE_*`` values are mapped to version-neutral state names here.

An agent receives an ordinary Message: the node's brief as a text Part,
followed by the consumed hand-off items' Parts copied verbatim from their
producing artifacts (``handoff.compose_parts``), the factory-journaled
``messageId`` and ``contextId``, and optionally a budget in the budget
extension's request metadata. No factory identifier crosses the wire; the
factory's own journal maps its assignment to the A2A identities.

An agent returns its work product itself: one artifact whose single text Part
is the content. The factory normalizes it on complete (``async_receipt``).
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import sys
import urllib.error
import urllib.request

import a2a_extensions
import a2a_v1
import handoff
from agent_binding import UnavailableBinding, resolve as resolve_pinned  # noqa: F401


TOKEN = "Bearer fixture-token"
EXTENSIONS = (a2a_extensions.BUDGET_URI,)


class UncertainSubmission(Exception):
    """The request may have committed remotely; reconcile before retrying."""


def _request(url, method, data=None, extensions=()):
    payload = None if data is None else json.dumps(data).encode()
    request = urllib.request.Request(url, data=payload, method=method,
        headers={"Authorization": TOKEN, "Content-Type": "application/json",
                 **a2a_v1.headers(extensions)})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as error:
        body = error.read().decode(errors="replace")
        raise RuntimeError(f"HTTP {error.code}: {body}") from error
    except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
        raise UncertainSubmission(str(error)) from error


def message(brief_text: str, *, message_id: str, context_id: str,
            upstream: list | None = None, consumes: list | None = None,
            key: bytes | None = None) -> dict:
    """The plain A2A Message the factory composes for one assignment attempt.

    The brief Part comes first, then each consumed hand-off item's Parts
    verbatim. Raises ``handoff.CompositionError`` on a mismatch with
    ``consumes`` or when the Message exceeds ``handoff.MAX_MESSAGE_BYTES``.
    """
    parts = handoff.compose_parts([a2a_v1.text_part(brief_text, a2a_v1.JSON_MEDIA_TYPE)],
                                  upstream or [], consumes=consumes, key=key)
    return handoff.require_within_bound(
        a2a_v1.user_message(parts, context_id=context_id, message_id=message_id))


def payload_sha256(message: dict) -> str:
    """Digest of the whole composed Message content; its ids are journal-bound."""
    content = {key: value for key, value in message.items()
               if key not in {"messageId", "contextId", "taskId"}}
    return hashlib.sha256(json.dumps(content, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False).encode()).hexdigest()


def send_message_async(url: str, message: dict, *, budget: dict | None = None) -> dict:
    """SendMessage with returnImmediately; a resend reuses the journaled Message."""
    params = a2a_v1.send_params(message, return_immediately=True)
    if budget is not None:
        params["metadata"] = {a2a_extensions.BUDGET_URI: {"budget": budget}}
    rpc = a2a_v1.rpc(a2a_v1.SEND_MESSAGE, params)
    response = _request(url.rstrip("/") + "/", "POST", rpc, extensions=EXTENSIONS)
    if "error" in response:
        raise RuntimeError("A2A error: " + json.dumps(response["error"]))
    try:
        return a2a_v1.require_task(response["result"])
    except a2a_v1.ProtocolError as error:
        raise ValueError("A2A v1 response invalid: " + str(error)) from error


def send_async(url: str, brief_text: str, *, message_id: str, context_id: str,
               budget: dict | None = None, upstream: list | None = None) -> dict:
    """SendMessage with returnImmediately; a resend reuses the journaled messageId."""
    return send_message_async(url, message(brief_text, message_id=message_id,
                                           context_id=context_id, upstream=upstream),
                              budget=budget)


def validate_async_task(task: dict, *, context_id: str, task_id: str | None = None) -> str:
    """Return the version-neutral state of a Task bound by the factory journal.

    Correlation is the journal's: the Task id and contextId it recorded. The
    agent's identity is its pinned Agent Card; no Task metadata is required.
    """
    if (not isinstance(task, dict) or "kind" in task or not isinstance(task.get("id"), str)
            or not task["id"]):
        raise ValueError("A2A response lacks Task id")
    if task_id is not None and task["id"] != task_id:
        raise ValueError("A2A Task id differs from the journaled Task")
    if task.get("contextId") != context_id:
        raise ValueError("A2A Task contextId differs from the journaled context")
    try:
        state = a2a_v1.task_state(task)
    except a2a_v1.ProtocolError as error:
        raise ValueError("A2A Task state invalid") from error
    if state not in {"submitted", "working", "completed", "failed", "canceled", "rejected"}:
        raise ValueError("A2A Task state invalid")
    return state


def async_receipt(task: dict, binding: dict, *, identity: str,
                  expected_revision: str, role: str) -> dict:
    """On-complete normalization. ``binding`` is the factory's own journal binding.

    The agent returns its work product as one artifact with one text Part. The
    factory computes the content digest and builds its own normalized record:
    the revision it assigned and the author from its own pin, never an agent
    echo. ``item_parts`` keeps the artifact's Parts verbatim for consumers.
    """
    if validate_async_task(task, context_id=binding["context_id"],
                           task_id=binding.get("task_id")) != "completed":
        raise ValueError("A2A Task is not complete")
    artifacts = task.get("artifacts") or []
    parts = artifacts[0].get("parts") if len(artifacts) == 1 and isinstance(artifacts[0], dict) else None
    if not isinstance(parts, list) or len(parts) != 1:
        raise ValueError("expected one artifact with one Part")
    try:
        content = a2a_v1.part_text(parts[0])
    except a2a_v1.ProtocolError as error:
        raise ValueError("expected an artifact text Part") from error
    artifact = {"revision": expected_revision,
                "sha256": hashlib.sha256(content.encode("utf-8")).hexdigest(),
                "author": identity, "content": content}
    return {"action_id": binding["action_id"], "run_id": binding["run_id"],
               "definition_digest": binding["definition_digest"], "task_id": task["id"],
               "context_id": binding["context_id"], "message_id": binding["message_id"],
               "artifact": artifact, "item_parts": [copy.deepcopy(parts)],
               "harness_identity": identity,
               "harness_role": role, "a2a_protocol": a2a_v1.PROTOCOL_VERSION}


def get_task(url, task_id, *, history_length: int | None = 0):
    params = {"id": task_id}
    if history_length is not None:
        params["historyLength"] = history_length
    rpc = a2a_v1.rpc(a2a_v1.GET_TASK, params)
    response = _request(url.rstrip("/") + "/", "POST", rpc, extensions=EXTENSIONS)
    if "error" in response:
        raise RuntimeError("A2A task lookup error: " + json.dumps(response["error"]))
    return a2a_v1.normalize_numbers(response["result"])


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    task = sub.add_parser("task-get")
    task.add_argument("--url", required=True)
    task.add_argument("--task-id", required=True)
    args = parser.parse_args()
    try:
        print(json.dumps(get_task(args.url, args.task_id, history_length=None), sort_keys=True))
    except UncertainSubmission as error:
        print("lookup outcome unknown: " + str(error), file=sys.stderr)
        raise SystemExit(75)


if __name__ == "__main__":
    main()
