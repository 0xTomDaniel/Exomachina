"""Small A2A v1.0 JSON-RPC client for the decision-round fixture and async agents.

This is the client side of the A2A Adapter boundary: requests carry the
``A2A-Version: 1.0`` header, Tasks arrive wrapped as ``{"task": ...}``, and
``TASK_STATE_*`` values are mapped to version-neutral state names here.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import urllib.error
import urllib.parse
import urllib.request

import a2a_v1
from agent_binding import EXTENSION_URI, UnavailableBinding, resolve as resolve_pinned


TOKEN = "Bearer fixture-token"


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


def send(url, command):
    """Submit a data Part over actual A2A v1 SendMessage and extract its artifact."""
    rpc = a2a_v1.rpc(a2a_v1.SEND_MESSAGE, a2a_v1.send_params(
        a2a_v1.user_message([a2a_v1.data_part(command)])))
    response = _request(url.rstrip("/") + "/", "POST", rpc)
    if "error" in response:
        raise RuntimeError("A2A error: " + json.dumps(response["error"]))
    try:
        shape, result = a2a_v1.unwrap_send_result(response["result"])
        completed = shape == "task" and a2a_v1.task_state(result) == "completed"
    except a2a_v1.ProtocolError as error:
        raise RuntimeError("A2A v1 response invalid: " + str(error)) from error
    if not completed:
        raise RuntimeError("A2A did not return a completed Task: " + json.dumps(result))
    metadata = result.get("metadata") or {}
    for field in ("action_id", "run_id", "definition_digest"):
        if metadata.get(field) != command[field]:
            raise RuntimeError("A2A result binding mismatch: " + field)
    artifacts = result.get("artifacts") or []
    if len(artifacts) != 1:
        raise RuntimeError("expected one structured artifact")
    artifact = a2a_v1.part_data(artifacts[0]["parts"][0])
    if artifacts[0]["artifactId"] != artifact["sha256"]:
        raise RuntimeError("artifact digest mismatch")
    return {"action_id": command["action_id"], "run_id": command["run_id"],
            "definition_digest": command["definition_digest"], "task_id": result["id"],
            "artifact": artifact, "harness_identity": metadata.get("harness_identity"),
            "harness_role": metadata.get("harness_role"),
            "a2a_protocol": a2a_v1.PROTOCOL_VERSION}


def send_async(url: str, command: dict) -> dict:
    """Submit the pinned async contract; retain the Task, including its id.

    The send activates the required action-contract extension and asks the
    agent to return immediately; the v1 ``{"task": ...}`` wrapper is removed.
    """
    rpc = a2a_v1.rpc(a2a_v1.SEND_MESSAGE, a2a_v1.send_params(
        a2a_v1.user_message([a2a_v1.data_part(command)]), return_immediately=True))
    response = _request(url.rstrip("/") + "/", "POST", rpc, extensions=[EXTENSION_URI])
    if "error" in response:
        raise RuntimeError("A2A error: " + json.dumps(response["error"]))
    try:
        return a2a_v1.require_task(response["result"])
    except a2a_v1.ProtocolError as error:
        raise ValueError("A2A v1 response invalid: " + str(error)) from error


def validate_async_task(task: dict, command: dict, identity: str) -> str:
    """Return the version-neutral state of a bound v1 Task."""
    if (not isinstance(task, dict) or "kind" in task or not isinstance(task.get("id"), str)
            or not task["id"]):
        raise ValueError("A2A response lacks Task id")
    metadata = task.get("metadata") or {}
    for key in ("action_id", "run_id", "definition_digest"):
        if metadata.get(key) != command[key]:
            raise ValueError("A2A Task binding mismatch: " + key)
    if metadata.get("agent_identity") != identity:
        raise ValueError("A2A Task identity mismatch")
    try:
        state = a2a_v1.task_state(task)
    except a2a_v1.ProtocolError as error:
        raise ValueError("A2A Task state invalid") from error
    if state not in {"submitted", "working", "completed", "failed", "canceled", "rejected"}:
        raise ValueError("A2A Task state invalid")
    return state


def async_receipt(task: dict, command: dict, identity: str,
                  expected_revision: str, role: str) -> dict:
    if validate_async_task(task, command, identity) != "completed":
        raise ValueError("A2A Task is not complete")
    artifacts = task.get("artifacts") or []
    if len(artifacts) != 1 or len(artifacts[0].get("parts") or []) != 1:
        raise ValueError("expected one structured artifact")
    try:
        artifact = a2a_v1.part_data(artifacts[0]["parts"][0])
    except a2a_v1.ProtocolError as error:
        raise ValueError("expected artifact data Part") from error
    if not isinstance(artifact, dict):
        raise ValueError("expected artifact data Part")
    for key in ("action_id", "run_id", "definition_digest"):
        if artifact.get(key) != command[key]:
            raise ValueError("artifact binding mismatch: " + key)
    if artifact.get("author") != identity or artifact.get("revision") != expected_revision:
        raise ValueError("artifact author or revision mismatch")
    content = artifact.get("content")
    if not isinstance(content, str) or artifact.get("sha256") != hashlib.sha256(content.encode()).hexdigest():
        raise ValueError("artifact content digest mismatch")
    if artifacts[0].get("artifactId") != artifact["sha256"]:
        raise ValueError("A2A Artifact id differs from content digest")
    return {"action_id": command["action_id"], "run_id": command["run_id"],
            "definition_digest": command["definition_digest"], "task_id": task["id"],
            "artifact": artifact, "harness_identity": identity,
            "harness_role": role, "a2a_protocol": a2a_v1.PROTOCOL_VERSION}


def reconcile(url, action_id, run_id=None, definition_digest=None):
    route = "/fixture/actions/" + urllib.parse.quote(action_id, safe="")
    record = _request(url.rstrip("/") + route, "GET")
    if record["action_id"] != action_id:
        raise RuntimeError("action lookup mismatch")
    if run_id is not None and record["run_id"] != run_id:
        raise RuntimeError("run lookup mismatch")
    if definition_digest is not None and record["definition_digest"] != definition_digest:
        raise RuntimeError("definition lookup mismatch")
    return record


def get_task(url, task_id):
    rpc = a2a_v1.rpc(a2a_v1.GET_TASK, {"id": task_id})
    response = _request(url.rstrip("/") + "/", "POST", rpc, extensions=[EXTENSION_URI])
    if "error" in response:
        raise RuntimeError("A2A task lookup error: " + json.dumps(response["error"]))
    return a2a_v1.normalize_numbers(response["result"])


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    assign = sub.add_parser("assign")
    assign.add_argument("--url", required=True)
    assign.add_argument("--action-id", required=True)
    assign.add_argument("--run-id", required=True)
    assign.add_argument("--definition-digest", required=True)
    assign.add_argument("--brief", required=True)
    review = sub.add_parser("review")
    review.add_argument("--url", required=True)
    review.add_argument("--action-id", required=True)
    review.add_argument("--run-id", required=True)
    review.add_argument("--definition-digest", required=True)
    artifact = review.add_mutually_exclusive_group(required=True)
    artifact.add_argument("--artifact-file")
    artifact.add_argument("--artifact-json")
    lookup = sub.add_parser("reconcile")
    lookup.add_argument("--url", required=True)
    lookup.add_argument("--action-id", required=True)
    lookup.add_argument("--run-id")
    lookup.add_argument("--definition-digest")
    task = sub.add_parser("task-get")
    task.add_argument("--url", required=True)
    task.add_argument("--task-id", required=True)
    args = parser.parse_args()
    try:
        if args.command == "assign":
            result = send(args.url, {"op": "assign", "action_id": args.action_id,
                "run_id": args.run_id, "definition_digest": args.definition_digest,
                "brief": args.brief})
        elif args.command == "review":
            source = open(args.artifact_file).read() if args.artifact_file else args.artifact_json
            result = send(args.url, {"op": "review", "action_id": args.action_id,
                "run_id": args.run_id, "definition_digest": args.definition_digest,
                "artifact": json.loads(source)})
        elif args.command == "reconcile":
            result = reconcile(args.url, args.action_id, args.run_id, args.definition_digest)
        else:
            result = get_task(args.url, args.task_id)
        print(json.dumps(result, sort_keys=True))
    except UncertainSubmission as error:
        print("submission outcome unknown; reconcile by action ID: " + str(error), file=sys.stderr)
        raise SystemExit(75)


if __name__ == "__main__":
    main()
