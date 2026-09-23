"""A2A and release Activities. Remote receipts never become product acceptance here."""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from temporalio import activity

COMMON = Path(__file__).resolve().parent / "prior_common"
sys.path.insert(0, str(COMMON))
import long_client as a2a  # noqa: E402
sys.path.insert(0, str(Path(__file__).resolve().parent / "common"))
import fixture  # noqa: E402
import receiver_client  # noqa: E402


def _get_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.loads(response.read())


def _log(kind: str, **fields) -> None:
    path = os.environ.get("EXO_TEMPORAL_ACTIVITY_LOG")
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
    """One Activity attempt. With no lookup, a replay cannot safely resubmit."""
    _identity(url, role, identity)
    if lookup_supported:
        prior = _existing(url, command["action_id"], command["run_id"],
                          command["definition_digest"])
        if prior is not None:
            return _receipt(prior, command, identity, role)
    elif activity.info().attempt > 1:
        return {"unresolved": "noncooperative receiver after Activity retry",
                "action_id": command["action_id"]}
    try:
        result = a2a.send(url, command)
    except Exception as error:
        if not lookup_supported:
            return {"unresolved": "noncooperative receiver acknowledgement unknown",
                    "action_id": command["action_id"], "error_type": type(error).__name__}
        # A committed receiver can be temporarily down after dropping its reply.
        # The Activity retry rechecks lookup before issuing any new submission.
        raise RuntimeError("receiver outcome unknown; reconcile on Activity retry") from error
    return _receipt(result, command, identity, role)


async def _barrier(input: dict) -> None:
    """External fault controller pauses an Activity after a remote commit."""
    if not input.get("barrier"):
        return
    marker = Path(input["barrier"]["marker"])
    marker.write_text(str(time.time()))
    if "hold_seconds" in input["barrier"]:
        await asyncio.sleep(input["barrier"]["hold_seconds"])
        return
    resume = Path(input["barrier"]["resume"])
    deadline = time.monotonic() + input["barrier"].get("timeout_seconds", 45)
    while not resume.exists():
        if time.monotonic() > deadline:
            raise TimeoutError("fault barrier was not released")
        await asyncio.sleep(0.1)


@activity.defn
async def assign(input: dict) -> dict:
    _log("assign-start", run=input["run"], instance=input["instance"])
    command = fixture.assignment(input["run"], input["digest"], input["instance"],
        result_type=input["result_type"], scope_status=input["scope_status"],
        drop_ack=input.get("drop_ack", False))
    result = await asyncio.to_thread(_invoke, input["url"], input["identity"],
        "capability", command, input["lookup_supported"])
    if "unresolved" not in result:
        artifact = result["artifact"]
        content = artifact.get("content", "")
        import hashlib
        if not isinstance(content, str) or artifact.get("sha256") != hashlib.sha256(content.encode()).hexdigest():
            raise ValueError("assignment artifact digest mismatch")
        if artifact.get("author") != input["identity"]:
            raise ValueError("assignment author identity mismatch")
        _log("assign-remote-receipt", run=input["run"], instance=input["instance"],
             task_id=result["task_id"])
        await _barrier(input)
        if input.get("delay_after_remote"):
            await asyncio.sleep(input["delay_after_remote"])
    _log("assign-complete", run=input["run"], instance=input["instance"])
    return result


@activity.defn
async def review(input: dict) -> dict:
    result = await asyncio.to_thread(_invoke, input["url"], input["identity"],
        "quality", input["command"], True)
    artifact = result["artifact"]
    candidate = input["command"]["artifact"]
    if (artifact.get("reviewer") != input["identity"]
            or input["identity"] == candidate["author"]
            or artifact.get("revision") != candidate["revision"]
            or artifact.get("sha256") != candidate["sha256"]
            or type(artifact.get("accepted")) is not bool):
        raise ValueError("Quality verdict does not bind exact independent candidate")
    await _barrier(input)
    _log("quality-verdict", run=input["command"]["run_id"],
         revision=artifact["revision"], accepted=artifact["accepted"],
         task_id=result["task_id"])
    return result


@activity.defn
async def typed_join(input: dict) -> dict:
    _log("join-start", run=input["run"], instances=sorted(input["receipts"]))
    return fixture.typed_join(input["receipts"], run_id=input["run"],
        definition_digest=input["digest"], declarations=input["declarations"],
        scope_status_by_instance=input["scopes"])


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
    if input["mode"] == "participating":
        try:
            prior = receiver_client.receipt(url, command["release_id"])
        except RuntimeError as error:
            if not str(error).startswith("HTTP 404:"):
                raise
            prior = None
        if prior is None:
            try:
                prior = receiver_client.release(url, command)
            except Exception as error:
                raise RuntimeError("release outcome unknown; reconcile on Activity retry") from error
        if any(prior.get(field) != command[field] for field in
               ("release_id", "run_id", "definition_digest", "revision", "sha256")):
            raise ValueError("release receipt binding mismatch")
        return prior
    if activity.info().attempt > 1:
        return {"unresolved": "opaque receiver after Activity retry",
                "release_id": command["release_id"]}
    try:
        receiver_client.opaque_submit(url, command)
    except Exception as error:
        return {"unresolved": "opaque receiver acknowledgement unknown",
                "release_id": command["release_id"], "error_type": type(error).__name__}
    return {"unresolved": "opaque receiver has no discoverable receipt",
            "release_id": command["release_id"]}


@activity.defn
async def release(input: dict) -> dict:
    result = await asyncio.to_thread(_release, input)
    if "unresolved" not in result:
        await _barrier(input)
    _log("release-observed", run=input["command"]["run_id"],
         receipt=result.get("release_id"), unresolved=result.get("unresolved"))
    return result
