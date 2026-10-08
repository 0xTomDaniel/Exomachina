"""One-home qualification of the three single-factory report routes.

The collector is intentionally separate from ``check_evidence``. A missing or
unparseable observation fails its pre-registered check instead of being inferred
from a successful final Task. The live provider is run only by the orchestrator.
"""
from __future__ import annotations

import argparse
import asyncio
import ast
import base64
import hashlib
import json
import re
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

from common import (task_state, ROOT, SRC, a2a_get, http, jsonl, run_cli, start_harness,
                    stop_process)
from live_authoring import (authoring_acceptance, candidate_files, positive_control,
                            scan_paths, subscription_status, synthetic_credential,
                            export_histories)
import stimulus_client

sys.path.insert(0, str(SRC))
from model_broker import DEFAULT_HOME, DEFAULT_MODEL_ID, DEFAULT_REASONING_EFFORT, ModelBroker  # noqa: E402
import a2a_v1  # noqa: E402

FOLLOW_UP = "The factory is waiting for a Director decision on this request. Please review the run and decide."
CHECK_IDS = ("SF-0", "SF-1", "SF-2", "SF-3", *(f"R1-{x}" for x in "abcde"),
             *(f"R2-{x}" for x in "abcd"), *(f"R3-{x}" for x in "abcde"),
             *(f"G-{x}" for x in (1, 2, 3, 4, 5, 7, 8)))
# G-8 (A2A v1 mediation decision 4): content-free hand-off records observed
# at the factory boundary, digests chaining from producer to consumer.
RECEIPT_TYPE = "com.exomachina.delivery.receipt.v1"
HANDOFF_TYPES = {"produced": "com.exomachina.handoff.produced.v1",
                 "consumed": "com.exomachina.handoff.consumed.v1",
                 "ready": "com.exomachina.handoff.item_ready.v1"}
HANDOFF_CONTENT_KEYS = {"text", "data", "raw", "bytes", "name", "description", "artifactId",
                        "filename", "url", "metadata", "content", "parts", "task_id",
                        "context_id"}
AGENTS = ("research_findings", "research_risks", "synthesizer", "quality")
SERVICES = (*AGENTS, "release")
REPO = ROOT.parents[1]
SYNTHETIC_BINDING_KEYS = ("status", "provider", "checks", "git_commit",
                          "checker_sha256", "interpreter_build", "manifest_digest",
                          "route_inventory")
FORBIDDEN_CONTROLS = {"append_claim", "revisions", "planted_text", "stimulus",
                      "stimulus_id", "test_controls"}


def verdict(ok: bool, evidence: dict, label: str, *, behavior: str | None = None) -> dict:
    return {"pass": bool(ok), "label": label, "behavior": behavior,
            "decisive_evidence": evidence}


def _part_is(part: object, content: str) -> bool:
    """A v1 Part carrying ``content``; 0.3 ``kind`` Parts never match."""
    try:
        return a2a_v1.part_content(part) == content
    except a2a_v1.ProtocolError:
        return False


def _recorded_state(task: dict) -> str | None:
    """Version-neutral state of a recorded v1 Task, or None if not v1."""
    try:
        return a2a_v1.task_state(task)
    except a2a_v1.ProtocolError:
        return None


def _data(task: dict) -> dict:
    for artifact in task.get("artifacts") or []:
        for part in artifact.get("parts") or []:
            if _part_is(part, "data"):
                return part.get("data") or {}
    return {}


def _text(task: dict) -> str | None:
    for artifact in task.get("artifacts") or []:
        for part in artifact.get("parts") or []:
            if _part_is(part, "text"):
                return part.get("text")
    return None


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _record_path(path: Path) -> str:
    """Store repository paths independently of the collecting worktree."""
    try:
        return str(path.relative_to(REPO))
    except ValueError:
        return str(path)


def _sha256_field(value: object) -> bool:
    return (isinstance(value, str) and len(value) == 64 and
            all(char in "0123456789abcdef" for char in value))


def _report(action: dict) -> dict:
    artifact = action.get("artifact") or {}
    if isinstance(artifact, str):
        try:
            artifact = json.loads(artifact)
        except ValueError:
            return {}
    if not isinstance(artifact, dict):
        return {}
    content = artifact.get("content") or "{}"
    if isinstance(content, str):
        try:
            content = json.loads(content)
        except ValueError:
            return {}
    return content if isinstance(content, dict) else {}


def _revision(action: dict) -> str | None:
    artifact = action.get("artifact") or {}
    return artifact.get("revision") if isinstance(artifact, dict) else None


def _run_actions(evidence: dict, route: int, agent: str) -> list[dict]:
    run_id = (evidence.get("routes", {}).get(str(route)) or {}).get("child_run_id")
    return [row for row in evidence.get("agents", {}).get(agent, {}).get("tasks", [])
            if row.get("action_id", "").startswith(str(run_id) + ":") and run_id]


def _release_journal(evidence: dict, route: int) -> list[dict]:
    run_id = (evidence.get("routes", {}).get(str(route)) or {}).get("child_run_id")
    return [j for j in evidence.get("journal") or []
            if j.get("effect_kind") == "release" and run_id and j.get("run_id") == run_id]


def _release_rows(evidence: dict, route: int) -> list[dict]:
    """The A2A release agent's own deliveries for one route.

    The agent is factory-unaware: its store has no run id. The factory journal
    maps each release attempt to the A2A messageId and taskId it used.
    """
    journal = _release_journal(evidence, route)
    tasks = {j.get("task_id") for j in journal if j.get("task_id")}
    messages = {j.get("message_id") for j in journal if j.get("message_id")}
    return [row for row in evidence.get("releases", [])
            if row.get("task_id") in tasks or row.get("message_id") in messages]


def _unjournaled_releases(evidence: dict) -> list[dict]:
    """Agent deliveries no factory release attempt accounts for (must be none)."""
    journal = [j for j in evidence.get("journal") or [] if j.get("effect_kind") == "release"]
    tasks = {j.get("task_id") for j in journal}
    return [row for row in evidence.get("releases", []) if row.get("task_id") not in tasks]


def _receipt_facts(route: dict) -> list[dict]:
    return [event["data"] for event in (route.get("receipt_audit") or {}).get("events") or []
            if isinstance(event, dict) and event.get("type") == RECEIPT_TYPE]


def _findings_on_claim(verdict_value: dict, stimulus: dict, synthesis: dict) -> bool:
    # R2-b/R3-b: "its claim_id equals the planted claim id, or its text quotes it."
    # The stimulus log records text, while the served synthesis assigns the claim id.
    planted = stimulus.get("planted_text") or ""
    claims = [claim for claim in _report(synthesis).get("claims") or []
              if claim.get("text") == planted]
    claim_id = claims[0].get("id") if len(claims) == 1 else None
    recorded_id = stimulus.get("claim_id")
    if not planted or not claim_id or (recorded_id and recorded_id != claim_id):
        return False
    return any(f.get("severity") == "blocking" and
               ((claim_id and f.get("claim_id") == claim_id) or
                planted in (f.get("problem") or ""))
               for f in verdict_value.get("findings") or [])


def _stimulus_bound(row: dict, synthesis_tasks: list[dict]) -> bool:
    """The test control must be the exact bytes finally served by its Task."""
    matches = [task for task in synthesis_tasks if task.get("task_id") == row.get("task_id")
               and _revision(task) == row.get("revision")]
    return (len(matches) == 1 and bool(row.get("sha256_after")) and
            row["sha256_after"] == (matches[0].get("artifact") or {}).get("sha256") and
            bool(row.get("planted_text")) and row["planted_text"] in json.dumps(_report(matches[0])))


def _exact_release(route: dict, releases: list[dict], revision: str, sha: str) -> bool:
    """One A2A delivery of exactly the accepted bytes, one receipt, one receipt fact."""
    task = route.get("task") or {}
    data = _data(task)
    artifacts = task.get("artifacts") or []
    acceptance = data.get("acceptance") or {}
    receipt = data.get("release_receipt") or {}
    facts = _receipt_facts(route)
    return (len(releases) == 1 and len(artifacts) == 1 and
            artifacts[0].get("artifactId") == sha and
            data.get("revision") == revision and data.get("sha256") == sha and
            acceptance.get("revision") == revision and acceptance.get("sha256") == sha and
            receipt.get("revision") == revision and receipt.get("sha256") == sha and
            # The agent's own record of the delivered bytes and its receipt.
            releases[0].get("state") == "completed" and releases[0].get("sha256") == sha and
            releases[0].get("effect_count") == 1 and
            releases[0].get("task_id") == receipt.get("task_id") and
            releases[0].get("message_id") == receipt.get("message_id") and
            releases[0].get("receipt_id") == receipt.get("receipt_id") and
            releases[0].get("byte_length") == receipt.get("byte_length") and
            receipt.get("outcome") == "delivered" and
            # Exactly one delivery.receipt Observation fact for this delivery.
            len(facts) == 1 and facts[0].get("receipt_id") == receipt.get("receipt_id") and
            facts[0].get("artifact_revision") == revision and
            facts[0].get("artifact_sha256") == sha and facts[0].get("outcome") == "delivered" and
            facts[0].get("destination_id") == receipt.get("destination_identity") and
            (route.get("receipt_audit") or {}).get("dashboard_contract_valid") is True)


def _delivery_of_accepted(e: dict, route_no: int, *, delivered: bool) -> dict:
    """Decision 9: delivery is provably the accepted revision.

    A delivered route has exactly one delivery.receipt fact and one confirmed
    factory release record, and both receipt digests equal the accepted
    report's plain sha256: the one accepted Quality verdict's candidate digest
    (the artifact.revised / quality.verdict ``artifact_sha256``), which is also
    the Workflow's authoritative acceptance. A route that does not deliver has
    no receipt at all.
    """
    route = (e.get("routes") or {}).get(str(route_no)) or {}
    facts = _receipt_facts(route)
    releases = [j for j in e.get("journal") or [] if j.get("effect_kind") == "release"
                and j.get("run_id") == route.get("child_run_id")]
    accepted = [q for q in _run_actions(e, route_no, "quality")
                if (q.get("verdict") or {}).get("accepted") is True]
    accepted_sha = ((accepted[0].get("verdict") or {}).get("candidate") or {}).get("sha256") \
        if len(accepted) == 1 else None
    acceptance = _data(route.get("task") or {}).get("acceptance") or {}
    receipts = [{"fact_sha256": fact.get("artifact_sha256")} for fact in facts]
    if not delivered:
        ok = not facts and not releases
    else:
        ok = (bool(accepted_sha) and acceptance.get("sha256") == accepted_sha and
              len(facts) == 1 and facts[0].get("artifact_sha256") == accepted_sha and
              len(releases) == 1 and releases[0].get("phase") == "confirmed" and
              (releases[0].get("receipt") or {}).get("sha256") == accepted_sha)
    return {"ok": ok, "delivered": delivered, "accepted_sha256": accepted_sha,
            "receipts": receipts,
            "journal_sha256": [(j.get("receipt") or {}).get("sha256") for j in releases]}


def _quality_bound(e: dict, route_no: int, synthesis: dict, quality: dict,
                   *, live: bool) -> bool:
    """Bind one verdict to its synthesis, remote Task, pin and confirmed outcome."""
    route = (e.get("routes") or {}).get(str(route_no)) or {}
    artifact = quality.get("artifact") or {}
    source = synthesis.get("artifact") or {}
    verdict_value = quality.get("verdict") or {}
    reviewer = ((e.get("agents") or {}).get("quality") or {}).get("identity")
    content = artifact.get("content")
    try:
        decoded = json.loads(content) if isinstance(content, str) else None
    except ValueError:
        decoded = None
    # The verdict names its candidate by revision and digest only; authorship
    # and the reviewer are the factory's pinned bindings, never agent echoes.
    candidate = {"revision": source.get("revision"), "sha256": source.get("sha256")}
    received = [row.get("sha256") for row in quality.get("inputs") or [] if isinstance(row, dict)]
    matches = [row for row in e.get("journal") or []
               if row.get("action_id") == quality.get("action_id") and
               row.get("run_id") == route.get("child_run_id")]
    return bool(
        quality.get("state") == "completed" and quality.get("task_id") and
        len(matches) == 1 and matches[0].get("phase") == "confirmed" and
        matches[0].get("task_id") == quality.get("task_id") == quality.get("journal_task_id") and
        matches[0].get("pinned_identity") == reviewer and
        matches[0].get("definition_digest") == quality.get("definition_digest") and
        quality.get("exclusive_store") is True and quality.get("pin_verified") is True and
        quality.get("correlated") is True and
        quality.get("run_id") == route.get("child_run_id") and
        artifact.get("sha256") == quality.get("receipt_sha256") and
        artifact.get("author") == reviewer and "reviewer" not in verdict_value and
        artifact.get("revision") == candidate["revision"] and
        # Quality received exactly the candidate draft as its consumed Part.
        received == [candidate["sha256"]] and
        isinstance(content, str) and _sha(content) == artifact.get("sha256") ==
            quality.get("artifact_id") and decoded == verdict_value and
        all(candidate.values()) and verdict_value.get("candidate") == candidate and
        bool(source.get("author")) and source.get("author") != reviewer and
        verdict_value.get("decided_by") == "model" and
        _model_work(quality))


def _expected_inventory(e: dict) -> dict:
    """Derive each expected action from the three observed child routes."""
    inventory = {}
    for number in (1, 2, 3):
        route = (e.get("routes") or {}).get(str(number)) or {}
        run = route.get("child_run_id")
        revisions = {_revision(t) for t in _run_actions(e, number, "synthesizer")}
        revisions.discard(None)
        rows = [j for j in e.get("journal") or [] if j.get("run_id") == run]
        expected_names = {"research_findings": 1, "research_risks": 1,
                          "synthesizer": len(revisions), "quality": len(revisions),
                          "release": int(number != 3)}
        action_rows = {name: [j for j in rows if
            (j.get("effect_kind") == "release" if name == "release" else
             j.get("effect_kind") == "a2a" and
             (j.get("receipt") or {}).get("harness_identity") ==
                ((e.get("agents") or {}).get(name) or {}).get("identity"))]
                       for name in expected_names}
        inventory[str(number)] = {"run_id": run, "revisions": sorted(revisions),
            "expected_counts": expected_names, "actions": action_rows,
            "complete": bool(run and len(revisions) == (1 if number == 1 else 2 if number == 2 else
                1 + ((route.get("child_status") or {}).get("max_repairs") or -100))) and
                all(len(action_rows[name]) == count for name, count in expected_names.items()) and
                all(j.get("phase") == "confirmed" and j.get("action_id") for j in rows) and
                len(rows) == sum(expected_names.values())}
    return inventory


def _has_control_key(value: object) -> bool:
    if isinstance(value, dict):
        return bool(FORBIDDEN_CONTROLS & set(value)) or any(_has_control_key(v) for v in value.values())
    if isinstance(value, list):
        return any(_has_control_key(v) for v in value)
    return False


def _has_any_key(value: object, forbidden: set[str]) -> bool:
    if isinstance(value, dict):
        return bool(forbidden & set(value)) or any(_has_any_key(v, forbidden)
            for v in value.values())
    if isinstance(value, list):
        return any(_has_any_key(v, forbidden) for v in value)
    return False


def _decoded_payloads(value: object):
    if isinstance(value, dict):
        data = value.get("data")
        if isinstance(data, str):
            try:
                raw = base64.b64decode(data, validate=True)
                yield raw
                try:
                    yield from _decoded_payloads(json.loads(raw))
                except (ValueError, UnicodeDecodeError):
                    pass
            except (ValueError, UnicodeDecodeError):
                pass
        for child in value.values():
            yield from _decoded_payloads(child)
    elif isinstance(value, list):
        for child in value:
            yield from _decoded_payloads(child)


def _redact_history(raw_path: Path, export_path: Path) -> dict:
    """Digest every Temporal input, including base64 start and update payloads."""
    original = raw_path.read_bytes()
    document = json.loads(original)
    paths = []
    sensitive = {"token", "actor", "authorized_actor", "owner_epoch", "epoch",
                 "package", "closure", "bindings"}
    def walk(value: object, path: str) -> None:
        if isinstance(value, dict):
            for key, child in list(value.items()):
                at = f"{path}.{key}"
                if key == "input" or key in sensitive:
                    digest = hashlib.sha256(json.dumps(child, sort_keys=True,
                        separators=(",", ":")).encode()).hexdigest()
                    decoded_digests = [hashlib.sha256(raw).hexdigest()
                        for raw in _decoded_payloads(child)]
                    value[key] = {"redacted_sha256": digest,
                                  "decoded_payload_sha256": decoded_digests}
                    paths.append(at)
                else:
                    walk(child, at)
        elif isinstance(value, list):
            for index, child in enumerate(value):
                walk(child, f"{path}[{index}]")
    walk(document, "$")
    if not paths:
        raise ValueError("history has no start or child input to redact")
    export_path.parent.mkdir(parents=True, exist_ok=True)
    export_path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
    return {"raw_path": _record_path(raw_path), "raw_sha256": hashlib.sha256(original).hexdigest(),
            "history_path": _record_path(export_path),
            "redacted_sha256": hashlib.sha256(export_path.read_bytes()).hexdigest(),
            "redacted_json_paths": paths}


def _redact_export(value: object) -> object:
    """Redact sensitive observation fields before evidence is exported."""
    sensitive = {"token", "actor", "authorized_actor", "owner_epoch", "epoch",
                 "package", "closure", "bindings", "input"}
    if isinstance(value, dict):
        result = {}
        for key, child in value.items():
            if key in sensitive:
                result[key] = {"redacted_sha256": hashlib.sha256(json.dumps(child,
                    sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest(),
                    "decoded_payload_sha256": [hashlib.sha256(raw).hexdigest()
                        for raw in _decoded_payloads(child)]}
            else:
                result[key] = _redact_export(child)
        return result
    if isinstance(value, list):
        return [_redact_export(child) for child in value]
    return value


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def _group_alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False


def check_evidence(e: dict) -> dict[str, dict]:
    """Pure SF-0..G-7 checks over observed evidence; absent evidence fails closed."""
    label = "observed-real" if e.get("provider") == "codex-subscription" else "observed-synthetic"
    live = e.get("provider") == "codex-subscription"
    quality_behavior = "spontaneous verdict on induced defect" if live else "scripted route control"
    route1_quality_behavior = "spontaneous verdict (no stimulus)" if live else quality_behavior
    model_behavior = "spontaneous" if live else "scripted"
    c: dict[str, dict] = {}
    setup = e.get("setup") or {}
    env = setup.get("environment") or {}
    broker = setup.get("broker_status") or {}
    preflight = setup.get("preflight_home_lstat") or {}
    card = setup.get("served_factory_card") or {}
    c["SF-0"] = verdict(bool(setup.get("fresh_home") and
        preflight.get("result") == "FileNotFoundError" and
        setup.get("preflight_listeners") == [] and
        setup.get("factory_count") == 1 and
        setup.get("card_skill_count") == 1 and isinstance(card.get("body"), dict) and
        len(card["body"].get("skills") or []) == 1 and
        card.get("sha256") == _sha(json.dumps(card["body"], sort_keys=True,
            separators=(",", ":"), ensure_ascii=False)) and
        setup.get("broker_status_fields") == broker and broker.get("signed_in") is True and
        broker.get("expired") is False and str(broker.get("account", "")).startswith("sha256:") and
        set(env) == {"EXO_MODEL_HOME", "EXO_CODEX_BASE_URL", "OPENAI_API_KEY", "ANTHROPIC_API_KEY"} and
        not env["EXO_MODEL_HOME"] and not env["EXO_CODEX_BASE_URL"]), setup, label)
    author = e.get("authoring") or {}
    # The report brief permits a valid first draft for either provider.
    auth_ok, auth_reason, auth_facts = authoring_acceptance(
        "codex-subscription" if live else "synthetic-loopback", author)
    publication = author.get("publication") or {}
    active = e.get("active_publication") or {}
    bindings = (e.get("testbed") or {}).get("bindings") or {}
    author_model = ((author.get("outcome") or {}).get("model") or {})
    stream_events = [x for x in e.get("broker_events") or [] if x.get("event") == "stream"]
    author_streams = author.get("broker_streams") or []
    c["SF-1"] = verdict(auth_ok and (author.get("live") or {}).get("provider") ==
        ("codex-subscription" if live else "synthetic-loopback") and
        author_model.get("kind") == "broker" and
        author_model.get("provider") == ("codex-subscription" if live else "synthetic-loopback") and
        author_model.get("id") == e.get("model_id", "gpt-6-sol") and author_model.get("live") is live and
        isinstance(author.get("model_calls"), int) and author["model_calls"] > 0 and
        (not live or len(author_streams) == author["model_calls"]) and
        (author.get("live") or {}).get("live_model_available") is True and
        author.get("seconds", 9999) <=
        (author.get("limits") or {}).get("deadline_seconds", 0) and
        publication.get("manifest_digest") == active.get("manifest_digest")
        and len(e.get("publications") or []) == 1 and
        set(e.get("binding_names") or
            (bindings if set(bindings) != {"redacted_sha256", "decoded_payload_sha256"} else [])) ==
            set(SERVICES) and
        active.get("bindings") == bindings and
        active.get("packet_digest") == e.get("packet_digest"),
        {"authoring": author, "acceptance_reason": auth_reason, "facts": auth_facts,
         "active": active, "catalog_count": len(e.get("publications") or [])}, label)
    agents = e.get("agents") or {}
    identities = [agents.get(name, {}).get("identity") for name in SERVICES]
    ports = [agents.get(name, {}).get("port") for name in SERVICES]
    states = [agents.get(name, {}).get("state_dir") for name in SERVICES]
    release_store = agents.get("release", {}).get("store_path")
    pins = [agents.get(name, {}).get("pin_verified") for name in AGENTS]
    import_audit = e.get("import_audit") or {}
    # Factory-side copies replace agent sqlite copies: the factory's journal
    # rows and agent-usage rows for each agent, plus the pinned card digests.
    factory_copies = [agents.get(name, {}).get("factory_records") for name in AGENTS]
    cards = [agents.get(name, {}).get("card_sha256") for name in AGENTS]
    activity = e.get("activity_log") or []
    interactions = [x for x in activity if x.get("kind") in
                    ("agent-task-journaled", "agent-task-polled")]
    pins_paired = bool(interactions) and all(any(
        pin.get("kind") == "agent-card-verified" and
        pin.get("action_id") == row.get("action_id") and
        pin.get("wall_time", float("inf")) <= row.get("wall_time", float("-inf")) and
        not any(other.get("kind") == "agent-task-polled" and
            other.get("action_id") == row.get("action_id") and
            pin.get("wall_time", 0) < other.get("wall_time", 0) < row.get("wall_time", 0)
            for other in activity)
        for pin in activity) for row in interactions)
    c["SF-2"] = verdict(all(identities) and len(set(identities)) == 5 and
        all(ports) and len(set(ports)) == 5 and all(states) and len(set(states)) == 5 and
        all(cards) and len(set(cards)) == 4 and release_store and
        not any(release_store.startswith(str(x)) for x in states[:-1]) and
        all(pins) and pins_paired and all(
            isinstance(copy, dict) and copy.get("journal") and
            len(copy.get("agent_usage") or []) == len(copy["journal"])
            for copy in factory_copies) and
        import_audit.get("product_clean") is True and
        import_audit.get("service_clean") is True and
        import_audit.get("factory_names_clean") is True and
        (import_audit.get("launcher_exception") or {}).get("allowed") is True,
        {"identities": identities, "ports": ports, "states": states, "cards": cards,
         "release_store": release_store,
         "pins": pins, "pins_paired": pins_paired, "factory_copies": factory_copies,
         "import_audit": import_audit}, label)
    routes = e.get("routes") or {}
    workflows = [w for r in routes.values() for w in r.get("workflows", [])]
    versions = {(w.get("manifest_digest"), w.get("package_digest"), w.get("build_id"))
                for w in workflows}
    caller_messages = [m for r in routes.values() for m in r.get("caller_messages", [])]
    ids = [(r.get("task_id"), r.get("run_id"), r.get("child_run_id")) for r in routes.values()]
    ids_flat = [item for triple in ids for item in triple]
    route_workflows = all(len(r.get("workflows") or []) == 2 and
        {w.get("role") for w in r["workflows"]} == {"parent", "child"} and
        {w.get("workflow_id") for w in r["workflows"]} ==
            {r.get("run_id"), r.get("child_run_id")} and
        all(w.get("history_path") and w.get("redacted_sha256") and
            w.get("raw_sha256") and w.get("redacted_json_paths") for w in r["workflows"])
        for r in routes.values())
    c["SF-3"] = verdict(set(routes) == {"1", "2", "3"} and
        all(ids_flat) and len(set(ids_flat)) == 9 and
        all(r.get("task", {}).get("id") == r.get("task_id") and
            r.get("run_id") != r.get("child_run_id") for r in routes.values()) and
        route_workflows and len(workflows) == 6 and len(versions) == 1 and
        all(all(w.get(k) for k in ("manifest_digest", "package_digest", "build_id"))
            for w in workflows) and
        all(w.get("versioning_behavior") == "PINNED" for w in workflows) and
        bool(caller_messages) and all(len(m.get("parts") or []) == 1 and
        _part_is(m["parts"][0], "text") and
        not _has_any_key(m, {"graph", "version", "package", "quality"})
        for m in caller_messages), {"versions": list(versions), "workflows": workflows,
                               "route_workflows": route_workflows,
                               "caller_messages": caller_messages}, label)

    r1 = routes.get("1") or {}; r2 = routes.get("2") or {}; r3 = routes.get("3") or {}
    t1 = r1.get("task") or {}; t2 = r2.get("task") or {}; t3 = r3.get("task") or {}
    d1 = r1.get("director_calls") or []
    starts = [x for x in d1 if x.get("tool") == "start_research" and x.get("accepted")]
    c["R1-a"] = verdict(len(starts) == 1 and r1.get("director_turn_count") == 1 and
        (r1.get("director_turns") or [{}])[0].get("model_kind") == ("live" if live else "synthetic") and
        "working" in (r1.get("observed_states") or []) and
        _recorded_state(t1) == "completed", {"calls": d1,
        "states": r1.get("observed_states"), "task_state": _recorded_state(t1)}, label,
        behavior=model_behavior)
    research = [_run_actions(e, 1, name) for name in AGENTS[:2]]
    ids = [agents.get(name, {}).get("identity") for name in AGENTS[:2]]
    def bound(a: dict, identity: str) -> bool:
        artifact = a.get("artifact") or {}
        content = artifact.get("content")
        return (a.get("state") == "completed" and a.get("journal_task_id") == a.get("task_id") and
            a.get("exclusive_store") is True and a.get("correlated") is True and
            a.get("run_id") == r1.get("child_run_id") and artifact.get("author") == identity and
            artifact.get("sha256") == a.get("receipt_sha256") and
            isinstance(content, str) and _sha(content) == artifact.get("sha256") == a.get("artifact_id") and
            a.get("content_valid") is True and a.get("pin_verified") is True and _model_work(a))
    # Two independent executions: distinct agent Tasks and factory contexts.
    contexts = [a.get("context_id") for group in research for a in group]
    c["R1-b"] = verdict(all(len(group) == 1 for group in research) and
        all(bound(group[0], identity) for group, identity in zip(research, ids)) and
        all(contexts) and len(set(contexts)) == 2, {"research": research}, label,
        behavior=model_behavior)
    synth1 = _run_actions(e, 1, "synthesizer")
    quality1 = _run_actions(e, 1, "quality")
    a1 = synth1[0] if synth1 else {}; q1 = quality1[0] if quality1 else {}
    v1 = q1.get("verdict") or {}
    h1 = (a1.get("artifact") or {}).get("sha256")
    c["R1-c"] = verdict(len(synth1) == len(quality1) == 1 and
        _revision(a1) == "r1" and _model_work(a1) and
        v1.get("accepted") is True and _quality_bound(e, 1, a1, q1, live=live),
        {"synthesis": a1, "quality": q1}, label, behavior=route1_quality_behavior)
    useful = r1.get("usefulness") or {}
    report1 = _report(a1)
    packet_ids = set(e.get("packet_ids") or [])
    claims = report1.get("claims") or []
    cited = len(claims) >= 3 and bool(packet_ids) and all(
        isinstance(claim, dict) and isinstance(claim.get("evidence"), list) and
        bool(claim["evidence"]) and set(claim["evidence"]) <= packet_ids for claim in claims)
    report_path = r1.get("report_path")
    report_sha256 = r1.get("report_sha256")
    markdown = report1.get("markdown")
    release1 = _release_rows(e, 1)
    release_journal1 = [j for j in e.get("journal") or [] if j.get("run_id") == r1.get("child_run_id")
                        and j.get("effect_kind") == "release"]
    c["R1-d"] = verdict(_exact_release(r1, release1, "r1", h1) and
        useful.get("ok") is True and useful.get("sections_present") is True and
        cited and isinstance(markdown, str) and bool(markdown) and
        _text(t1) == markdown and isinstance(report_path, str) and bool(report_path) and
        ((report_sha256 is None and e.get("evidence_schema") != 2) or
         report_sha256 == _sha(markdown)) and
        len(release_journal1) == 1 and release_journal1[0].get("phase") == "confirmed" and
        release1[0].get("sha256") == _sha((a1.get("artifact") or {}).get("content", "")) and
        release1[0].get("byte_length") ==
            len(((a1.get("artifact") or {}).get("content") or "").encode("utf-8")) and
        (release_journal1[0].get("receipt") or {}).get("sha256") == h1 and
        release_journal1[0].get("task_id") == release1[0].get("task_id") and
        release_journal1[0].get("message_id") == release1[0].get("message_id"),
        {"release": _release_rows(e, 1), "artifact": t1.get("artifacts"),
         "receipt_facts": _receipt_facts(r1),
         "usefulness": useful, "claim_count": len(claims), "claims_cite_packet": cited,
         "packet_ids": sorted(packet_ids), "report_path": report_path,
         "report_sha256": report_sha256}, label)
    c["R1-d"]["scope"] = "structural"
    c["R1-d"]["semantic_reading"] = "pending-orchestrator-reading"
    c["R1-e"] = verdict(len(quality1) == 1 and v1.get("accepted") is True and
        _quality_bound(e, 1, a1, q1, live=live),
        {"quality": quality1}, label, behavior=route1_quality_behavior)

    stimuli2 = r2.get("stimulus_log") or []
    planted2 = stimuli2[0] if len(stimuli2) == 1 else {}
    s2 = _run_actions(e, 2, "synthesizer"); q2 = _run_actions(e, 2, "quality")
    global_stimuli = e.get("stimulus_log") or []
    expected_stimuli = {(route.get("child_run_id"), _revision(task), task.get("task_id"),
                         (task.get("artifact") or {}).get("sha256"))
        for number, route in ((2, r2), (3, r3))
        for task in _run_actions(e, number, "synthesizer")
        if number == 3 or _revision(task) == "r1"}
    actual_stimuli = {(x.get("run_id"), x.get("revision"), x.get("task_id"),
                       x.get("sha256_after")) for x in global_stimuli}
    stimulus_exact = bool(expected_stimuli) and len(actual_stimuli) == len(global_stimuli) and \
        actual_stimuli == expected_stimuli
    sources = (e.get("control_audit") or {}).get("sources") or []
    control_clean = ((e.get("control_audit") or {}).get("forbidden_found") is False and
        len(sources) == 15 and all(row.get("clean") is True for row in sources) and
        {(row.get("route"), row.get("kind")) for row in sources} ==
            {(n, kind) for n in (1, 2, 3) for kind in
                ("caller", "run_inputs", "briefs", "director_args", "history_inputs")} and
        all(not _has_control_key(route.get("caller_messages")) and
            not _has_control_key((route.get("child_status") or {}).get("run_inputs")) and
            not _has_control_key([x.get("arguments") for x in route.get("director_calls") or []]) and
            not _has_control_key([x.get("brief") for name in AGENTS for x in
                _run_actions(e, n, name)]) and route.get("history_control_clean") is True
            for n, route in ((1, r1), (2, r2), (3, r3))))
    c["R2-a"] = verdict(len(stimuli2) == 1 and planted2.get("revision") == "r1" and
        planted2.get("run_id") == r2.get("child_run_id") and
        _stimulus_bound(planted2, s2) and
        len({(x.get("task_id"), x.get("revision")) for x in stimuli2}) == len(stimuli2) and
        stimulus_exact and control_clean,
        {"stimulus": stimuli2, "all_stimulus": global_stimuli,
         "expected_stimulus": sorted(expected_stimuli), "control_audit": e.get("control_audit")},
        label, behavior="induced")
    first2 = next((x for x in s2 if _revision(x) == "r1"), {})
    review2 = next((x for x in q2 if (x.get("verdict") or {}).get("candidate", {}).get("revision") == "r1"), {})
    rejected2 = review2.get("verdict") or {}
    c["R2-b"] = verdict(rejected2.get("accepted") is False and
        _quality_bound(e, 2, first2, review2, live=live) and
        _findings_on_claim(rejected2, planted2, first2),
        {"review": review2, "planted": planted2}, label, behavior=quality_behavior)
    repair2 = next((x for x in s2 if _revision(x) == "r2"), {})
    brief2 = repair2.get("brief") or {}
    h2 = (repair2.get("artifact") or {}).get("sha256")
    # The repair Message carried the rejected draft as its own Part after the
    # research items; the brief embeds none of it.
    repair_inputs = [row.get("sha256") for row in repair2.get("inputs") or [] if isinstance(row, dict)]
    c["R2-c"] = verdict(bool(repair2) and brief2.get("mode") == "repair" and
        "prior" not in brief2 and "evidence" not in brief2 and len(repair_inputs) == 3 and
        repair_inputs[-1] == (first2.get("artifact") or {}).get("sha256") and
        brief2.get("quality_findings") == rejected2.get("findings") and
        _model_work(repair2) and h2 and h2 != (first2.get("artifact") or {}).get("sha256") and
        bool(planted2.get("planted_text")) and
        planted2["planted_text"] not in json.dumps(_report(repair2)) and
        not any(x.get("revision") == "r2" for x in stimuli2),
        {"first": first2, "repair": repair2, "stimulus": stimuli2}, label,
        behavior="live repair content on assigned repair" if live else model_behavior)
    accepted2 = next((x for x in q2 if (x.get("verdict") or {}).get("accepted") is True), {})
    av2 = accepted2.get("verdict") or {}
    ak2 = av2.get("candidate", {}).get("revision")
    max_repairs = (r2.get("child_status") or {}).get("max_repairs")
    c["R2-d"] = verdict(ak2 == "r2" and isinstance(max_repairs, int) and max_repairs >= 1 and
        _quality_bound(e, 2, repair2, accepted2, live=live) and
        av2.get("candidate", {}).get("sha256") == h2 and
        _exact_release(r2, _release_rows(e, 2), ak2, h2) and
        all(x.get("sha256") != (first2.get("artifact") or {}).get("sha256") for x in _release_rows(e, 2)),
        {"acceptance": accepted2, "release": _release_rows(e, 2), "artifact": t2.get("artifacts"),
         "receipt_facts": _receipt_facts(r2), "max_repairs": max_repairs}, label, behavior="spontaneous verdict" if live else quality_behavior)

    stimuli3 = r3.get("stimulus_log") or []
    s3 = _run_actions(e, 3, "synthesizer"); q3 = _run_actions(e, 3, "quality")
    max3 = (r3.get("child_status") or {}).get("max_repairs")
    expected = {f"r{i}" for i in range(1, 2 + max3)} if isinstance(max3, int) else set()
    planted_revisions = {x.get("revision") for x in stimuli3}
    c["R3-a"] = verdict(bool(expected) and planted_revisions == expected and
        len(stimuli3) == len(expected) and all(x.get("run_id") == r3.get("child_run_id") for x in stimuli3) and
        stimulus_exact and control_clean and
        len({(x.get("task_id"), x.get("revision")) for x in stimuli3}) == len(stimuli3) and
        all(_stimulus_bound(x, s3) for x in stimuli3) and
        all(any(_revision(a) == x["revision"] and x.get("planted_text") in json.dumps(_report(a))
                for a in s3) for x in stimuli3), {"stimulus": stimuli3, "expected": sorted(expected)},
        label, behavior="induced")
    reviews3 = {(x.get("verdict") or {}).get("candidate", {}).get("revision"): x for x in q3}
    synthesis3 = {_revision(x): x for x in s3}
    stimuli3_by_revision = {x.get("revision"): x for x in stimuli3}
    child3 = r3.get("child_status") or {}
    c["R3-b"] = verdict(bool(expected) and len(q3) == len(expected) and
        len(s3) == len(expected) and set(reviews3) == set(synthesis3) == expected and
        all((reviews3[x].get("verdict") or {}).get("accepted") is False and
            _quality_bound(e, 3, synthesis3[x], reviews3[x], live=live) and
            _findings_on_claim(reviews3[x]["verdict"], stimuli3_by_revision.get(x) or {},
                               synthesis3[x])
            for x in expected) and child3.get("repair_count") == max3 and
        any("repair:exhausted" in x for x in child3.get("completed") or []),
        {"reviews": q3, "child_status": child3}, label, behavior=quality_behavior)
    c["R3-c"] = verdict("input-required" in (r3.get("observed_states") or []) and
        r3.get("wait_status", {}).get("phase") == "awaiting-director",
        {"states": r3.get("observed_states"), "wait_status": r3.get("wait_status")}, label)
    calls3 = r3.get("director_calls") or []
    follow_id = ((r3.get("caller_messages") or [{}])[-1]).get("messageId")
    # R3-d: "The live Director turn calls `inspect_run`, then one accepted
    # `decide_wait`" refers to the follow-up turn, not the earlier START turn.
    inspected = [i for i, x in enumerate(calls3) if x.get("tool") == "inspect_run" and
                 x.get("accepted") and x.get("message_id") == follow_id]
    aborted = [i for i, x in enumerate(calls3) if x.get("tool") == "decide_wait" and x.get("accepted")]
    follow_turns = [x for x in r3.get("director_turns") or [] if x.get("message_id") == follow_id]
    c["R3-d"] = verdict(r3.get("follow_up_text") == FOLLOW_UP and
        r3.get("follow_up_original_task") is True and follow_id and
        len(follow_turns) == 1 and len(inspected) == len(aborted) == 1 and
        all(calls3[i].get("message_id") == follow_id and
            calls3[i].get("task_id") == r3.get("task_id") for i in inspected + aborted) and
        (r3.get("caller_messages") or [{}])[-1].get("taskId") == r3.get("task_id") and
        (r3.get("caller_messages") or [{}])[-1].get("contextId") == r3.get("context_id") and
        follow_turns[0].get("model_kind") == ("live" if live else "synthetic") and
        follow_turns[0].get("accepted") == ["inspect_run", "decide_wait"] and
        calls3[aborted[0]].get("model_kind") == ("live" if live else "synthetic") and
        inspected[0] < aborted[0] and calls3[aborted[0]].get("arguments", {}).get("action") == "abort" and
        calls3[aborted[0]].get("arguments", {}).get("revision") == child3.get("current_revision") and
        calls3[aborted[0]].get("arguments", {}).get("sha256") == child3.get("current_sha256") and
        _recorded_state(t3) == "completed" and
        r3.get("result_status") == "aborted" and not (t3.get("artifacts") or []) and
        len(_release_rows(e, 3)) == 0 and not _release_journal(e, 3) and
        not _receipt_facts(r3) and not _unjournaled_releases(e),
        {"calls": calls3, "task": t3, "release": _release_rows(e, 3),
         "receipt_facts": _receipt_facts(r3), "unjournaled": _unjournaled_releases(e)}, label,
        behavior="caller-prompted, model-decided (abort is the only permitted action)" if live else
                 "caller-prompted scripted abort")
    turns = [x for r in routes.values() for x in r.get("director_turns") or []]
    c["R3-e"] = verdict(all(len((routes.get(str(n)) or {}).get("director_turns") or []) == count
        for n, count in ((1, 1), (2, 1), (3, 2))) and
        all(x.get("model_kind") == ("live" if live else "synthetic") and
        1 <= x.get("model_calls", 99) <= 4 and
        x.get("tool_calls", 99) <= 4 and x.get("elapsed_seconds", 999) <= 90 and
        x.get("failure") is None for x in turns),
        {"turns": turns}, label, behavior="caller-prompted, model-decided (abort is the only permitted action)" if live else
                                    "caller-prompted scripted abort")

    journal = e.get("journal") or []
    run_ids = {identifier for r in routes.values() for identifier in
               (r.get("run_id"), r.get("child_run_id")) if identifier}
    run_journal = [x for x in journal if x.get("run_id") in run_ids]
    inventory = _expected_inventory(e)
    c["G-1"] = verdict(len(run_ids) == 6 and len(inventory) == 3 and
        all(v["complete"] for v in inventory.values()) and
        len(run_journal) == sum(sum(v["expected_counts"].values()) for v in inventory.values()) and
        all(x.get("phase") == "confirmed" for x in run_journal) and
        e.get("incidents") == [], {"inventory": inventory,
        "journal": run_journal, "incidents": e.get("incidents")}, label)
    # Agent model work is the factory-recorded budget-extension report; broker
    # attribution uses the shared broker's session label <identity>:<taskId>,
    # both factory-known. Agent stores are never read.
    worked = [a for name in AGENTS for a in agents.get(name, {}).get("tasks") or []]
    sessions = [_agent_session(a, agents) for a in worked]
    streams = {x.get("session") for x in stream_events}
    stream_counts = {session: sum(x.get("session") == session for x in stream_events)
                     for session in streams}
    call_counts = {session: stream_counts.get(session, 0) for session in sessions}
    model_inventory = [task for name in AGENTS for task in agents.get(name, {}).get("tasks") or []]
    expected_task_ids = {(j.get("receipt") or {}).get("task_id") for v in inventory.values()
        for name in AGENTS for j in v["actions"][name]}
    observed_task_ids = {task.get("task_id") for task in model_inventory}
    store_binding = all({(j.get("receipt") or {}).get("task_id") for v in inventory.values()
        for j in v["actions"][name]} ==
        {task.get("task_id") for task in (agents.get(name) or {}).get("tasks") or []}
        for name in AGENTS)
    broker_records = e.get("broker_events") or []
    broker_starts = [x for x in broker_records if x.get("event") == "start" and x.get("pid")]
    broker_attach = [x for x in broker_records if x.get("event") == "attach"]
    # G-2: "one broker PID across the scenario" also permits a broker already
    # running before the observation window, which produces attaches but no start.
    broker_pid = broker_starts[0]["pid"] if len(broker_starts) == 1 else e.get("broker_pid_before")
    broker_stable = (len(broker_starts) <= 1 and bool(broker_attach) and bool(broker_pid) and
        (not broker_starts or e.get("broker_pid_before") in (None, broker_pid)) and
        all(pid == broker_pid for pid in e.get("broker_pids_during") or []))
    # Live: every agent Task streamed through the broker. Scripted: no agent
    # Task did (agent sessions are absent from broker streams).
    stream_reconciled = (all(n >= 1 for n in call_counts.values()) if live
                         else not (set(sessions) & streams))
    author_count = e.get("authoring_call_count")
    director_count = e.get("director_call_count")
    broker_owner_counts = e.get("broker_owner_counts") or {}
    c["G-2"] = verdict(e.get("broker_pid_before") == e.get("broker_pid_after") and
        len(set(e.get("broker_pids_during") or [])) == 1 and
        bool((e.get("broker_pids_during") or [None])[0]) and broker_stable and
        expected_task_ids == observed_task_ids and store_binding and bool(worked) and
        all(_model_work(a) for a in worked) and
        all(sessions) and len(set(sessions)) == len(worked) and stream_reconciled and
        isinstance(director_count, int) and director_count > 0 and
        isinstance(author_count, int) and author_count > 0 and
        broker_owner_counts.get("authoring") == author_count and
        broker_owner_counts.get("director") == director_count,
        {"broker_pid_before": e.get("broker_pid_before"),
         "broker_pid_after": e.get("broker_pid_after"), "sessions": sessions,
         "stream_sessions": sorted(str(x) for x in streams),
         "director_calls": director_count, "authoring_calls": author_count,
         "broker_owner_counts": broker_owner_counts,
         "reconciliation_method": "authoring/Director stream windows; agent Tasks by "
             "factory-journaled identity:taskId broker sessions and agent-reported usage",
         "stream_counts": stream_counts, "call_counts": call_counts,
         "broker_starts": broker_starts, "broker_attach": broker_attach}, label)
    tasks = [a for name in AGENTS for a in agents.get(name, {}).get("tasks") or []]
    c["G-3"] = verdict(expected_task_ids == observed_task_ids and store_binding and bool(tasks) and
        all(a.get("state") in ("completed", "failed") for a in tasks) and
        all(_model_work(a) and a.get("duration_seconds") is not None and
            0 <= a["duration_seconds"] <= 240 for a in tasks), {"tasks": tasks}, label)
    leak = e.get("leak_scan") or {}; control = e.get("positive_control") or {}
    manifest = leak.get("candidate_manifest") or []
    scan_inputs = leak.get("scan_inputs") or []
    raw_scan = leak.get("raw") or {}
    required_inputs = leak.get("required_scan_inputs")
    if required_inputs is None:  # Preserved pre-schema evidence has absolute paths.
        required_inputs = ([e["home"], str(Path(r1.get("report_path", "")).parent),
                            str(Path(e["home"]) / "model")] if e.get("home") else [])
    coverage = leak.get("coverage") or {}
    recorded_coverage = (e.get("evidence_schema") != 2 or
        (coverage.get("manifest_count") == len(manifest) and
         coverage.get("scanner_file_count") == raw_scan.get("files_scanned") and
         coverage.get("zero_hits") is True and
         coverage.get("positive_control_detected") is True and
         coverage.get("candidate_files_hashed") is True))
    c["G-4"] = verdict(leak.get("hit_count") == 0 and raw_scan.get("hits") == [] and
        control.get("detected") is True and
        bool((control.get("scan") or {}).get("hits")) and
        len(manifest) > 0 and all(isinstance(row, dict) and
            isinstance(row.get("path"), str) and bool(row["path"]) and
            _sha256_field(row.get("sha256")) for row in manifest) and
        len({row.get("path") for row in manifest}) == len(manifest) and
        isinstance(required_inputs, list) and bool(required_inputs) and
        e.get("home") in required_inputs and
        all(path in scan_inputs for path in required_inputs) and
        all(row["path"] in scan_inputs for row in manifest) and
        isinstance(raw_scan.get("files_scanned"), int) and
        raw_scan["files_scanned"] >= len(manifest) and recorded_coverage and
        (leak.get("sensitive_fields_redacted") or {}).get("pass") is True and
        (leak.get("sensitive_fields_redacted") or {}).get("files_checked", 0) >= 9 and
        leak.get("exported_file_count") ==
            (leak.get("sensitive_fields_redacted") or {}).get("files_checked"),
        {"leak_scan": leak, "positive_control": control}, label)
    cleanup = e.get("cleanup") or {}
    stop_results_valid = (isinstance(cleanup.get("services"), dict) and
        set(cleanup["services"]) == set(SERVICES) and
        all(value in ("stopped", "already-stopped") for value in cleanup["services"].values()) and
        isinstance(cleanup.get("runner"), dict) and
        cleanup["runner"].get("running") is False and cleanup["runner"].get("pid") is None and
        isinstance(cleanup.get("harness_exit"), int) and
        (live or isinstance(cleanup.get("mock_exit"), int) and
            isinstance(e.get("mock_authoring_exit"), int)))
    c["G-5"] = verdict(cleanup.get("errors") == [] and cleanup.get("listeners") == [] and
        # G-5: "no listener remains in the block." The live block has 39
        # ports; the scripted provider adds one mock port.
        cleanup.get("ports_checked") and len(cleanup["ports_checked"]) >= (39 if live else 40) and
        cleanup.get("started_processes") and all(row.get("exited") is True and
            row.get("group_exited") is True
            for row in cleanup["started_processes"]) and
        cleanup.get("stop_results_valid") is True and stop_results_valid and
        e.get("broker_pid_before") == e.get("broker_pid_after"), cleanup, label)
    handoff_routes = {}
    for number in ("1", "2", "3"):
        audit = (e.get("routes", {}).get(number) or {}).get("handoff_audit") or {}
        chain = _handoff_chain(audit)
        handoff_routes[number] = {**chain, **{key: audit.get(key) for key in (
            "key_configured", "key_absent_history", "key_absent_events",
            "dashboard_contract_valid", "histories_checked")},
            "delivery": _delivery_of_accepted(e, int(number), delivered=number != "3")}
    c["G-8"] = verdict(all(
        row["delivery"]["ok"] and
        row["produced"] >= 3 and row["consumed"] >= 2 and row["chain_ok"] and
        row["report_items"] >= 1 and row["content_free"] and
        row["key_configured"] is True and row["key_absent_history"] is True and
        row["key_absent_events"] is True and row["dashboard_contract_valid"] is True
        for row in handoff_routes.values()), handoff_routes, label)
    synthetic = e.get("synthetic_scenario") or {}
    synthetic_attestation = synthetic.get("attestation") or {}
    synthetic_record = synthetic.get("record") or {}
    synthetic_record_bound = (isinstance(synthetic_record, dict) and
        all(synthetic_record.get(key) == synthetic.get(key)
            for key in SYNTHETIC_BINDING_KEYS))
    c["G-7"] = verdict((not live and all(value["pass"] for key, value in c.items() if key != "G-7"))
        or (live and synthetic.get("status") == "structural-pass" and synthetic.get("provider") == "scripted"
            and bool(synthetic.get("checks"))
            and set(synthetic["checks"]) == set(CHECK_IDS) and
            all(v.get("pass") for v in synthetic["checks"].values()) and
            bool(synthetic.get("evidence_path")) and
            _sha256_field(synthetic.get("evidence_sha256")) and
            synthetic_attestation.get("g4_final", {}).get("pass") is True and
            synthetic_attestation.get("evidence_sha256") == synthetic.get("evidence_sha256") and
            _sha256_field(synthetic.get("attestation_sha256")) and
            bool(synthetic.get("attestation_path")) and
            synthetic_record_bound and synthetic.get("git_commit") and
            synthetic.get("checker_sha256") == e.get("checker_sha256") and
            synthetic.get("interpreter_build") == next(iter(versions), (None, None, None))[2] and
            synthetic.get("route_inventory") == {"1": True, "2": True, "3": True}),
        {"structural_checks": {key: value["pass"] for key, value in c.items()},
         "synthetic_scenario": synthetic},
        "observed-synthetic")
    return {key: c[key] for key in CHECK_IDS}


def _rows(path: Path, table: str) -> list[dict]:
    if not path.exists():
        return []
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        rows = [dict(x) for x in connection.execute(f"SELECT * FROM {table}")]
    except sqlite3.OperationalError:
        rows = []
    finally:
        connection.close()
    return rows


def _json_field(row: dict, field: str) -> object:
    raw = row.get(field)
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except ValueError:
            return raw
    return raw


def _listen(ports: list[int]) -> list[int]:
    found = []
    for port in ports:
        with socket.socket() as connection:
            connection.settimeout(.1)
            if connection.connect_ex(("127.0.0.1", port)) == 0:
                found.append(port)
    return found


def _pid(broker: ModelBroker) -> int | None:
    try:
        health = broker.health()
        return health.get("pid") if isinstance(health, dict) else None
    except (OSError, RuntimeError, ValueError):
        return None


def _send_text(base: str, brief: str, task_id: str | None = None,
               context_id: str | None = None) -> dict:
    message = a2a_v1.user_message([a2a_v1.text_part(brief)], task_id=task_id,
                                  context_id=context_id)
    reply = http(base + "/", a2a_v1.rpc(a2a_v1.SEND_MESSAGE, a2a_v1.send_params(message)),
                 timeout=150)
    if "error" in reply:
        raise RuntimeError("A2A send failed: " + json.dumps(reply["error"]))
    return {"message": message, "task": a2a_v1.require_task(reply["result"])}


def _route(base: str, instance: Path, question: str, number: int,
           *, seconds: int = 900) -> dict:
    seen: list[str] = []
    before = {x["task_id"] for x in _rows(instance / "director.sqlite3", "aliases")}
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(_send_text, base, question)
        started = time.monotonic()
        task_id = None
        while time.monotonic() - started < 150:
            if task_id is None:
                new = [x["task_id"] for x in _rows(instance / "director.sqlite3", "aliases")
                       if x["task_id"] not in before]
                task_id = new[0] if new else None
            if task_id:
                try:
                    state = task_state(a2a_get(base, task_id))
                    if not seen or seen[-1] != state:
                        seen.append(state)
                except (OSError, RuntimeError, KeyError):
                    pass
            if pending.done():
                break
            time.sleep(.05)
        sent = pending.result(timeout=150)
    task = sent["task"]
    task_id = task.get("id") or task_id
    if not seen or seen[-1] != task_state(task):
        seen.append(task_state(task))
    deadline = time.monotonic() + seconds
    target = "input-required" if number == 3 else "completed"
    while time.monotonic() < deadline and task_state(task) not in {target, "failed"}:
        task = a2a_get(base, task_id)
        state = task_state(task)
        if seen[-1] != state:
            seen.append(state)
        time.sleep(.25)
    if task_state(task) != target:
        raise TimeoutError(f"route {number} ended at {task['status']['state']}")
    return {"caller_messages": [sent["message"]], "task": task,
            "task_id": task_id, "context_id": task["contextId"],
            "run_id": task.get("metadata", {}).get("run_id"), "observed_states": seen,
            "sent_state": task_state(sent["task"])}


FACTORY_NAMES = ("run_id", "assignment_id", "attempt_id", "action_id", "definition_digest",
                 "factory_id", "node")
AGENT_SERVICE_FILES = (ROOT / "services" / "model_agent.py", ROOT / "services" / "agent_roles.py",
                       ROOT / "services" / "delayed_agent.py", ROOT / "services" / "quality_server.py",
                       ROOT / "services" / "supplier_echo_fixture.py", SRC / "harness_server.py",
                       ROOT / "services" / "release_server.py")


def _factory_name_audit() -> dict:
    """Agent services reference no factory identifier (operator rule, 8 Oct 2026).

    Every identifier, attribute, argument, keyword and string constant in the
    agent service modules is checked for the factory correlation names.
    """
    pattern = re.compile(r"\b(" + "|".join(FACTORY_NAMES) + r")\b")
    violations = []
    for path in AGENT_SERVICE_FILES:
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            names = []
            if isinstance(node, ast.Name):
                names.append(node.id)
            elif isinstance(node, ast.Attribute):
                names.append(node.attr)
            elif isinstance(node, ast.arg):
                names.append(node.arg)
            elif isinstance(node, ast.keyword) and node.arg:
                names.append(node.arg)
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                names.append(node.name)
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                names.extend(pattern.findall(node.value))
            for name in names:
                if name in FACTORY_NAMES:
                    violations.append({"path": _record_path(path),
                                       "line": getattr(node, "lineno", None), "name": name})
    return {"files": [_record_path(p) for p in AGENT_SERVICE_FILES],
            "names": list(FACTORY_NAMES), "violations": violations}


def _import_audit() -> dict:
    # The A2A v1 wire Adapters are shared protocol code, not factory state; they
    # stay shared only while they import no other factory module. The admission
    # queue is factory-only: WIP limits are factory settings and agent services
    # hold no capacity queue (operator decision, 8 Oct 2026).
    shared = {"a2a_v1", "a2a_v1_server", "a2a_extensions"}
    src_names = {p.stem for p in SRC.glob("*.py")} - shared - {"model_broker"}
    def names(path: Path) -> set[str]:
        tree = ast.parse(path.read_text())
        return {alias.name.split(".")[0] for node in ast.walk(tree)
                if isinstance(node, ast.Import) for alias in node.names} | {
                (node.module or "").split(".")[0] for node in ast.walk(tree)
                if isinstance(node, ast.ImportFrom)}
    product = [p for p in SRC.glob("*.py") if "services" in names(p)]
    running = [ROOT / "services" / name for name in
               ("model_agent.py", "agent_roles.py", "release_server.py")]
    service = [p for p in running if names(p) & src_names] + [
        SRC / f"{name}.py" for name in sorted(shared)
        if names(SRC / f"{name}.py") & (src_names | {"model_broker"})]
    launcher = ROOT / "services" / "testbed.py"
    launcher_imports = sorted(names(launcher) & (src_names | {"model_broker", "agent_roles"}))
    launcher_tree = ast.parse(launcher.read_text())
    broker_imports = [node for node in ast.walk(launcher_tree)
                      if (isinstance(node, ast.Import) and any(
                          alias.name.split(".")[0] == "model_broker" for alias in node.names)) or
                      (isinstance(node, ast.ImportFrom) and
                       (node.module or "").split(".")[0] == "model_broker")]
    dynamic_imports = [node for node in ast.walk(launcher_tree) if isinstance(node, ast.Call) and
                       ((isinstance(node.func, ast.Name) and node.func.id == "__import__") or
                        (isinstance(node.func, ast.Attribute) and
                         node.func.attr == "import_module"))]
    broker_config_only = (len(broker_imports) == 1 and
        isinstance(broker_imports[0], ast.ImportFrom) and
        broker_imports[0].module == "model_broker" and broker_imports[0].level == 0 and
        [(alias.name, alias.asname) for alias in broker_imports[0].names] ==
            [("DEFAULT_MODEL_ID", None)] and
        not dynamic_imports and
        not any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and
                node.func.id == "DEFAULT_MODEL_ID" for node in ast.walk(launcher_tree)))
    factory_names = _factory_name_audit()
    return {"product_clean": not product, "service_clean": not service,
            "factory_names_clean": not factory_names["violations"],
            "factory_names": factory_names,
            "product_violations": [_record_path(p) for p in product],
            "service_violations": [_record_path(p) for p in service],
            "running_service_modules": [_record_path(p) for p in running],
            "launcher_exception": {"path": _record_path(launcher), "imports": launcher_imports,
                                   "model_broker_config_imports": ["DEFAULT_MODEL_ID"]
                                       if broker_config_only else [],
                                   "allowed": launcher_imports ==
                                       ["agent_binding", "agent_roles", "model_broker"] and
                                       broker_config_only},
            "legacy_not_running": ["quality_server.py", "delayed_agent.py"]}


def _factory_agent_usage(home: Path) -> list[dict]:
    """Agent-reported usage the factory recorded from budget-extension reports."""
    from model_usage import AGENT_USAGE_DATABASE, ModelUsageJournal
    path = home / "runner" / AGENT_USAGE_DATABASE
    return ModelUsageJournal(path).list_measurements() if path.is_file() else []


def _model_work(task: dict) -> bool:
    """The agent reported model work for this Task; the factory recorded it.

    Factory-side replacement for the retired agent ``model_calls`` table: the
    terminal Task's budget-extension report, journaled by the factory, shows
    output tokens. Nothing reported is ``unknown`` and never counts as work.
    """
    row = task.get("agent_usage") or {}
    output = ((row.get("usage") or {}).get("output_tokens") or {})
    return (row.get("evidence_status") == "agent_reported" and
            row.get("measurement_source") == "agent_reported" and
            row.get("task_id") == task.get("task_id") and
            output.get("status") == "reported" and (output.get("value") or 0) > 0)


def _agent_session(task: dict, agents: dict) -> str | None:
    """Broker session label of an agent Task, from factory-known identity and taskId."""
    identity = (agents.get(task.get("agent")) or {}).get("identity")
    return f"{identity}:{task['task_id']}" if identity and task.get("task_id") else None


def _collect_agents(home: Path, testbed: dict, journal: list[dict],
                    activity: list[dict]) -> dict:
    """Per-agent evidence built only from factory-side records.

    Sources: the factory outcome journal (taskId/contextId/messageId per
    action), A2A GetTask on the journaled taskId, the factory's agent-usage
    journal, and the factory activity log. Agent stores are never read.
    """
    import long_client
    result = {}
    pids = testbed.get("pids") or {}
    contracts_path = home / "testbed" / "contracts.json"
    contracts = json.loads(contracts_path.read_text()) if contracts_path.exists() else {}
    usage_rows = _factory_agent_usage(home)
    for name in SERVICES:
        state = home / "services" / name
        identity = pids.get(name, {}).get("identity")
        if name not in AGENTS:
            stores = list(state.glob("*.sqlite3"))
            result[name] = {"identity": identity, "port": pids.get(name, {}).get("port"),
                            "state_dir": str(state),
                            "store_path": str(stores[0]) if stores else None,
                            "pin_verified": False, "tasks": [], "factory_records": None}
            continue
        url = f"http://127.0.0.1:{pids[name]['port']}"
        pin = contracts.get(name) or {}
        records = [x for x in journal
                   if x.get("pinned_identity") == identity and x.get("task_id")]
        tasks = []
        for match in records:
            remote = long_client.get_task(url, match["task_id"], history_length=None)
            history = remote.get("history") or []
            sent = history[0] if history else {}
            # The Message the factory composed: its brief Part, then the
            # consumed items' Parts. Only the items' digests are kept.
            sent_parts = sent.get("parts") or []
            brief_text = (sent_parts[0].get("text") if sent_parts and _part_is(sent_parts[0], "text")
                          else None)
            input_parts = sent_parts[1:]
            remote_artifacts = remote.get("artifacts") or []
            parts = (remote_artifacts[0].get("parts") or []) if len(remote_artifacts) == 1 else []
            # The agent returns its work product itself (one text Part); the
            # revision and author are the factory's own normalized record.
            wire_text = (parts[0].get("text") if len(parts) == 1 and _part_is(parts[0], "text")
                         else None)
            normalized = (match.get("receipt") or {}).get("artifact") or {}
            artifact = ({"revision": normalized.get("revision"), "sha256": _sha(wire_text),
                         "author": normalized.get("author"), "content": wire_text}
                        if isinstance(wire_text, str) else {})
            action_log = sorted((x for x in activity if x.get("action_id") == match["action_id"]),
                                key=lambda row: row.get("wall_time", 0))
            journaled = [x for x in action_log if x.get("kind") == "agent-task-journaled"]
            finished = [x for x in action_log if x.get("kind") in
                        ("agent-usage-recorded", "agent-usage-unrecorded")]
            usage = [x for x in usage_rows if x.get("task_id") == match["task_id"]
                     and x.get("service_identity") == identity]
            receipt_artifact = (match.get("receipt") or {}).get("artifact") or {}
            task = {"agent": name, "task_id": remote.get("id"),
                    "journal_task_id": match["task_id"], "action_id": match["action_id"],
                    "run_id": match.get("run_id"),
                    "definition_digest": match.get("definition_digest"),
                    "context_id": remote.get("contextId"),
                    "journal_context_id": match.get("context_id"),
                    "message_id": sent.get("messageId"),
                    "journal_message_id": match.get("message_id"),
                    "state": a2a_v1.task_state(remote),
                    "brief": _json_field({"brief": brief_text}, "brief"),
                    "inputs": [{"part_kinds": [kind for kind in ("text", "data", "raw", "url")
                                               if kind in part],
                                "media_type": part.get("mediaType"),
                                "sha256": _sha(part["text"]) if isinstance(part.get("text"), str)
                                          else None}
                               for part in input_parts if isinstance(part, dict)],
                    "artifact": artifact,
                    "artifact_id": remote_artifacts[0].get("artifactId")
                                   if len(remote_artifacts) == 1 else None,
                    "receipt_sha256": receipt_artifact.get("sha256"),
                    "agent_usage": usage[0] if len(usage) == 1 else None,
                    "duration_seconds": (finished[-1]["wall_time"] - journaled[0]["wall_time"]
                                         if journaled and finished else None),
                    "exclusive_store": True}
            # Correlation is the factory's: the served Task carries the journaled
            # contextId and its history the journaled messageId.
            task["correlated"] = (task["task_id"] == task["journal_task_id"] and
                bool(task["context_id"]) and task["context_id"] == task["journal_context_id"] and
                bool(task["message_id"]) and task["message_id"] == task["journal_message_id"] and
                (remote.get("metadata") or {}).get("agent_identity") == identity)
            pin_logs = [x for x in action_log if x.get("kind") == "agent-card-verified"]
            interactions = [x for x in action_log
                            if x.get("kind") in ("agent-task-journaled", "agent-task-polled")]
            last_poll_at = float("-inf")
            interactions_verified = True
            for interaction in interactions:
                verified = any(last_poll_at < pin_log.get("wall_time", 0) <=
                               interaction.get("wall_time", 0) for pin_log in pin_logs)
                interactions_verified = interactions_verified and verified
                if interaction.get("kind") == "agent-task-polled":
                    last_poll_at = interaction.get("wall_time", 0)
            task["pin_verified"] = bool(pin_logs and interactions_verified and
                task["correlated"] and match.get("pinned_identity") == identity and all(
                    x.get("pinned_identity") == identity and
                    x.get("pinned_card_sha256") == pin.get("card_sha256") and
                    x.get("observed", {}).get("card_sha256") == pin.get("card_sha256") and
                    x.get("observed", {}).get("identity") == identity
                    for x in pin_logs))
            if name == "quality" and isinstance(artifact, dict):
                content = artifact.get("content") or "{}"
                try:
                    task["verdict"] = json.loads(content) if isinstance(content, str) else content
                except ValueError:
                    task["verdict"] = {}
            task["content_valid"] = False
            if task["state"] == "completed" and isinstance(artifact, dict):
                try:
                    from agent_roles import ROLES, working_view
                    role = "quality" if name == "quality" else "synthesis" if name == "synthesizer" else "research"
                    view = working_view(task["brief"], input_parts)
                    parsed = ROLES[role].parse(artifact["content"], view, identity)
                    task["content_valid"] = parsed == json.loads(artifact["content"])
                except (KeyError, ValueError, TypeError):
                    pass
            tasks.append(task)
        result[name] = {"identity": identity, "port": pids.get(name, {}).get("port"),
            "state_dir": str(state), "card_sha256": pin.get("card_sha256"),
            "pin_verified": all(x.get("pin_verified") for x in tasks) if tasks else False,
            "tasks": tasks,
            "factory_records": {"journal": records,
                                "agent_usage": [x for x in usage_rows
                                                if x.get("service_identity") == identity]}}
    all_ids = [t.get("task_id") for name in AGENTS for t in result[name]["tasks"]]
    for name in AGENTS:
        for task in result[name]["tasks"]:
            task["exclusive_store"] = all_ids.count(task.get("task_id")) == 1
    return result


def _journal(home: Path) -> list[dict]:
    return [{"action_id": x["action_id"], **json.loads(x["value"])}
            for x in _rows(home / "runner" / "outcomes.sqlite3", "outcomes")]


def _history_summary(histories: dict, route: dict) -> list[dict]:
    from temporalio.api.enums.v1 import VersioningBehavior
    parent = histories.get("parent_status_query") or {}
    child = route.get("child_status") or {}
    return [{"role": name, "workflow_id": value.get("workflow_id"),
             "manifest_digest": (parent if name == "parent" else child).get("manifest_digest"),
             "package_digest": (parent if name == "parent" else child).get("package_digest"),
             "build_id": value.get("versioning", {}).get("build_id"),
             "versioning_behavior": "PINNED" if value.get("versioning", {}).get("behavior") ==
                 VersioningBehavior.VERSIONING_BEHAVIOR_PINNED else "OTHER",
             **(value.get("redaction") or {}),
             "history_path": value.get("history_path")}
            for name, value in histories.get("workflows", {}).items()]


def _history_dict_events(document: dict) -> list[dict]:
    """Temporal JSON history -> the Runtime source's decoded event shape."""
    def decode(holder):
        payloads = (holder or {}).get("payloads") or []
        values = [json.loads(base64.b64decode(p["data"])) for p in payloads if p.get("data")]
        return values[0] if len(values) == 1 else (values or None)
    events = []
    for event in document.get("events") or []:
        name = str(event.get("eventType", "")).removeprefix("EVENT_TYPE_")
        attrs: dict = {}
        if name == "WORKFLOW_EXECUTION_STARTED":
            attrs["input"] = decode(event["workflowExecutionStartedEventAttributes"].get("input"))
        elif name == "ACTIVITY_TASK_SCHEDULED":
            raw = event["activityTaskScheduledEventAttributes"]
            attrs = {"activity_id": raw.get("activityId"),
                     "activity_type": (raw.get("activityType") or {}).get("name"),
                     "input": decode(raw.get("input"))}
        elif name == "ACTIVITY_TASK_STARTED":
            raw = event["activityTaskStartedEventAttributes"]
            attrs = {"scheduled_event_id": int(raw["scheduledEventId"]),
                     "attempt": int(raw.get("attempt", 1))}
        elif name == "ACTIVITY_TASK_COMPLETED":
            raw = event["activityTaskCompletedEventAttributes"]
            attrs = {"scheduled_event_id": int(raw["scheduledEventId"]),
                     "started_event_id": int(raw["startedEventId"]),
                     "result": decode(raw.get("result"))}
        elif name == "WORKFLOW_EXECUTION_COMPLETED":
            # A run's result repeats its release receipt; the receipt audit
            # must see it to prove that repetition is not a second fact.
            raw = event["workflowExecutionCompletedEventAttributes"]
            attrs = {"result": decode(raw.get("result"))}
        else:
            continue
        events.append({"event_id": int(event["eventId"]), "event_type": name,
                       "time": event.get("eventTime"), "attributes": attrs})
    return events


def _handoff_audit(raw_paths: list[tuple[str, Path]], factory_id: str, key_path: Path,
                   scratch: Path) -> dict:
    """Project hand-off facts from the run's raw histories through the Runtime
    source and Observation allowlist, validate them with the dashboard contract,
    and confirm the instance digest key appears in neither histories nor events.
    Only the projected, content-free event data is kept as evidence."""
    from types import SimpleNamespace
    from observation import project_source_record
    from observation_source import RuntimeObservationSource
    key = key_path.read_bytes() if key_path.is_file() else b""
    markers = [key.hex(), base64.b64encode(key).decode()] if key else []
    key_absent_history = bool(key)
    events = []
    source = RuntimeObservationSource.__new__(RuntimeObservationSource)
    source.director = SimpleNamespace(identity=factory_id)
    source.database = scratch / "handoff-audit.sqlite3"
    for workflow_id, raw_path in raw_paths:
        text = raw_path.read_text()
        document = json.loads(text)
        decoded = list(_decoded_payloads(document))
        if any(m in text for m in markers) or any(
                key in blob or any(m.encode() in blob for m in markers) for blob in decoded):
            key_absent_history = False
        for record in source._history_records(workflow_id, _history_dict_events(document),
                                              None, None):
            if record.get("event_type") in HANDOFF_TYPES.values():
                events.append(project_source_record(record, factory_id)[3])
    encoded = json.dumps(events, sort_keys=True)
    contract = subprocess.run([os.environ.get("EXO_NODE", "node"), "--input-type=module", "-e",
        "import {validateCloudEvent} from './dashboard/contract.mjs';"
        "import fs from 'node:fs';"
        "for (const e of JSON.parse(fs.readFileSync(0,'utf8'))) validateCloudEvent(e);"
        "console.log('ok');"], cwd=ROOT, input=encoded, capture_output=True, text=True, timeout=60)
    return {"events": [{"type": e["type"], "time": e["time"], "data": e["data"]} for e in events],
            "key_configured": bool(key), "key_absent_history": key_absent_history,
            "key_absent_events": bool(key) and not any(m in encoded for m in markers),
            "dashboard_contract_valid": contract.returncode == 0 and contract.stdout.strip() == "ok",
            "histories_checked": len(raw_paths)}


def _receipt_audit(raw_paths: list[tuple[str, Path]], factory_id: str, scratch: Path) -> dict:
    """Project the route's histories through the Runtime source and Observation
    allowlist and keep every delivery.receipt fact, validated by the dashboard
    contract. One delivery must yield exactly one fact across all workflows."""
    from types import SimpleNamespace
    from observation import project_source_record
    from observation_source import RuntimeObservationSource
    source = RuntimeObservationSource.__new__(RuntimeObservationSource)
    source.director = SimpleNamespace(identity=factory_id)
    source.database = scratch / "receipt-audit.sqlite3"
    events, source_ids = [], []
    for workflow_id, raw_path in raw_paths:
        document = json.loads(raw_path.read_text())
        for record in source._history_records(workflow_id, _history_dict_events(document),
                                              None, None):
            if record.get("event_type") == RECEIPT_TYPE:
                source_ids.append(record.get("source_id"))
                events.append(project_source_record(record, factory_id)[3])
    contract = subprocess.run([os.environ.get("EXO_NODE", "node"), "--input-type=module", "-e",
        "import {validateCloudEvent} from './dashboard/contract.mjs';"
        "import fs from 'node:fs';"
        "for (const e of JSON.parse(fs.readFileSync(0,'utf8'))) validateCloudEvent(e);"
        "console.log('ok');"], cwd=ROOT, input=json.dumps(events), capture_output=True,
        text=True, timeout=60)
    return {"events": [{"type": e["type"], "time": e["time"], "data": e["data"]} for e in events],
            "source_ids": source_ids,
            "dashboard_contract_valid": contract.returncode == 0 and contract.stdout.strip() == "ok",
            "histories_checked": len(raw_paths)}


def _handoff_chain(audit: dict) -> dict:
    """Recompute the producer-to-consumer digest chain from recorded event data."""
    rows = [e for e in (audit or {}).get("events") or [] if isinstance(e, dict)]
    produced = [e["data"] for e in rows if e.get("type") == HANDOFF_TYPES["produced"]]
    consumed = [e["data"] for e in rows if e.get("type") == HANDOFF_TYPES["consumed"]]
    def keys(value):
        if isinstance(value, dict):
            yield from value
            for child in value.values():
                yield from keys(child)
        elif isinstance(value, list):
            for child in value:
                yield from keys(child)
    links = []
    for row in consumed:
        for entry in row.get("inputs") or []:
            revisions = [p for p in produced if p.get("handoff_id") == entry.get("handoff_id")
                         and p.get("produced_at", "") <= row.get("consumed_at", "")]
            source = next((p for p in reversed(revisions)
                           if [i.get("digest") for i in p.get("items") or []] ==
                           entry.get("item_digests")), None)
            links.append({"consumer": row.get("node"), "handoff_id": entry.get("handoff_id"),
                          "revision": source.get("handoff_revision") if source else None,
                          "match": source is not None})
    reports = [i for p in produced for i in p.get("items") or [] if "artifact_sha256" in i]
    return {"produced": len(produced), "consumed": len(consumed), "links": links,
            "chain_ok": bool(links) and all(link["match"] for link in links),
            "report_items": len(reports),
            "content_free": all(set(e) == {"type", "time", "data"} for e in rows) and
                not (set(keys([e["data"] for e in rows])) & HANDOFF_CONTENT_KEYS)}


def _history_control_audit(path: Path) -> bool:
    document = json.loads(path.read_text())
    for event in document.get("events") or []:
        for key in ("workflowExecutionStartedEventAttributes",
                    "startChildWorkflowExecutionInitiatedEventAttributes",
                    "activityTaskScheduledEventAttributes"):
            value = event.get(key)
            if not isinstance(value, dict):
                continue
            if _has_control_key(value):
                return False
            for payload in _decoded_payloads(value):
                try:
                    if _has_control_key(json.loads(payload)):
                        return False
                except (ValueError, UnicodeDecodeError):
                    continue
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--home", type=Path, required=True)
    parser.add_argument("--provider", choices=("scripted", "codex-subscription"), required=True)
    parser.add_argument("--model", default=DEFAULT_MODEL_ID)
    parser.add_argument("--evidence-dir", type=Path)
    parser.add_argument("--routes", default="1,2,3")
    parser.add_argument("--runner-port-base", type=int, default=44540)
    parser.add_argument("--runner-member-base", type=int, default=32520)
    parser.add_argument("--harness-port", type=int, default=44874)
    parser.add_argument("--services-port-base", type=int, default=45780)
    parser.add_argument("--mock-port", type=int, default=46510)
    parser.add_argument("--attempt", default="1")
    parser.add_argument("--synthetic-evidence", type=Path)
    parser.add_argument("--synthetic-attestation", type=Path)
    args = parser.parse_args()
    routes = [int(x) for x in args.routes.split(",")]
    if sorted(set(routes)) != routes or any(x not in (1, 2, 3) for x in routes):
        parser.error("routes must be an ordered subset of 1,2,3")
    home = args.home.resolve()
    if home.parent != Path("/tmp").resolve() or not home.name.startswith("exo-sf-") or home.exists():
        parser.error("home must be a fresh /tmp/exo-sf-* directory")
    if args.provider == "codex-subscription" and any(
            name in os.environ for name in ("EXO_MODEL_HOME", "EXO_CODEX_BASE_URL")):
        parser.error("live provider requires default model home and endpoint")
    label = "observed-real" if args.provider == "codex-subscription" else "observed-synthetic"
    evidence_dir = (args.evidence_dir.resolve() if args.evidence_dir else
                    ROOT / "evidence" / "single-factory")
    evidence_dir.mkdir(parents=True, exist_ok=True)
    evidence_path = evidence_dir / f"{args.provider}-{args.attempt}.json"
    if args.provider == "codex-subscription":
        synthetic_path = args.synthetic_evidence or evidence_dir / "scripted-1.json"
        synthetic = json.loads(synthetic_path.read_text()) if synthetic_path.exists() else {}
        synthetic_attestation = (json.loads(args.synthetic_attestation.read_text())
            if args.synthetic_attestation and args.synthetic_attestation.is_file() else {})
    else:
        synthetic = {}
        synthetic_attestation = {}
    env_names = ("EXO_MODEL_HOME", "EXO_CODEX_BASE_URL", "OPENAI_API_KEY", "ANTHROPIC_API_KEY")
    evidence = {"evidence_schema": 2, "provider": args.provider, "label": label, "home": str(home),
                "model_id": args.model, "reasoning_effort": DEFAULT_REASONING_EFFORT,
                "status": "running", "routes": {}, "checks": {},
                "synthetic_scenario": ({k: synthetic.get(k) for k in SYNTHETIC_BINDING_KEYS}
                    | {"evidence_path": _record_path(synthetic_path),
                       "evidence_sha256": hashlib.sha256(synthetic_path.read_bytes()).hexdigest(),
                       "attestation_path": _record_path(args.synthetic_attestation)
                           if args.synthetic_attestation else None,
                       "attestation_sha256": hashlib.sha256(args.synthetic_attestation.read_bytes()).hexdigest()
                           if args.synthetic_attestation and args.synthetic_attestation.is_file() else None,
                       "attestation": synthetic_attestation,
                       "record": {k: synthetic.get(k) for k in SYNTHETIC_BINDING_KEYS}})
                    if synthetic else {},
                "setup": {"fresh_home": True,
                          "environment": {name: name in os.environ for name in env_names}},
                "release_label": "a2a-release (fixture agent)"}
    evidence["git_commit"] = subprocess.check_output(["git", "rev-parse", "HEAD"],
        cwd=ROOT, text=True).strip()
    evidence["checker_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    try:
        args.home.lstat()
        evidence["setup"]["preflight_home_lstat"] = {"result": "exists"}
    except FileNotFoundError:
        evidence["setup"]["preflight_home_lstat"] = {"result": "FileNotFoundError"}
    home.mkdir(parents=True)
    instance = home / "instances" / "report-factory"
    broker = ModelBroker() if args.provider == "codex-subscription" else None
    broker_running_before = broker.is_running() if broker is not None else False
    model_home = home / "model" if args.provider == "scripted" else DEFAULT_HOME
    broker_event_start = len(jsonl(model_home / "broker-events.jsonl"))
    mock = harness = None
    started_processes = []
    def record_process(kind: str, pid: int | None) -> None:
        if isinstance(pid, int) and pid > 0 and not any(x["pid"] == pid for x in started_processes):
            try:
                group = os.getpgid(pid)
            except ProcessLookupError:
                group = None
            started_processes.append({"kind": kind, "pid": pid, "pgid": group})
    step = "preflight"
    try:
        trial_ports = [*range(args.runner_port_base, args.runner_port_base + 13),
            *range(args.runner_member_base, args.runner_member_base + 5), args.harness_port,
            *range(args.services_port_base, args.services_port_base + 20)]
        if args.provider == "scripted":
            trial_ports.append(args.mock_port)
        evidence["setup"]["preflight_listeners"] = _listen(trial_ports)
        if evidence["setup"]["preflight_listeners"]:
            raise RuntimeError("trial port block is occupied")
        if args.provider == "scripted":
            synthetic_credential(home / "model")
            os.environ["EXO_MODEL_HOME"] = str(home / "model")
            os.environ["EXO_CODEX_BASE_URL"] = f"http://127.0.0.1:{args.mock_port}/backend-api"
            broker = ModelBroker(home / "model")
            with (home / "mock.log").open("a") as log:
                mock = subprocess.Popen([os.environ.get("EXO_NODE", "node"),
                    str(ROOT / "broker" / "testing" / "mock-codex.mjs"),
                    "--port", str(args.mock_port), "--record", str(home / "mock-requests.jsonl"),
                    "--script", "authoring"], cwd=ROOT, stdout=log, stderr=log,
                    start_new_session=True)
            record_process("mock-authoring", mock.pid)
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline and args.mock_port not in _listen([args.mock_port]):
                if mock.poll() is not None:
                    raise RuntimeError("loopback mock exited during startup")
                time.sleep(.1)
            if args.mock_port not in _listen([args.mock_port]):
                raise TimeoutError("loopback mock did not listen")
        evidence["setup"]["broker_status"] = subscription_status()
        evidence["setup"]["broker_status_fields"] = {key: evidence["setup"]["broker_status"].get(key)
            for key in ("signed_in", "expired", "account", "expires_at")}
        if not evidence["setup"]["broker_status"].get("signed_in") or \
                evidence["setup"]["broker_status"].get("expired"):
            raise RuntimeError("broker status is unsigned or expired")
        evidence["broker_pid_before"] = _pid(broker)
        os.environ["EXO_RUNNER_PORT_BASE"] = str(args.runner_port_base)
        os.environ["EXO_RUNNER_MEMBER_BASE"] = str(args.runner_member_base)
        os.environ["EXO_AUTHOR_PROVIDER"] = ("synthetic-loopback" if args.provider == "scripted"
                                              else "codex-subscription")
        os.environ["EXO_AUTHOR_MODEL"] = args.model
        step = "testbed"
        testbed = json.loads(run_cli(str(ROOT / "services" / "testbed.py"), "up",
            "--profile", "report", "--model-provider", args.provider, "--model", args.model,
            "--home", str(home), "--port-base", str(args.services_port_base),
            "--test-controls"))
        evidence["testbed"] = testbed
        evidence["binding_names"] = sorted((testbed.get("bindings") or {}).keys())
        for name, member in (testbed.get("pids") or {}).items():
            record_process("service-" + name, member.get("pid"))
        step = "provision"
        run_cli(str(SRC / "admin.py"), "provision", "--instance-dir", str(instance),
            "--name", "report-factory", "--port", str(args.harness_port), "--home", str(home),
            "--testbed", str(home / "testbed"), "--wait-seconds", "900",
            "--evidence-packet", str(ROOT / "packets" / "exo-qualification-2026-09-23" / "packet.json"))
        config_path = instance / "instance.json"
        config = json.loads(config_path.read_text())
        config["director_model"] = {"provider": ("synthetic-loopback" if args.provider == "scripted"
                                        else "codex-subscription"), "model": args.model}
        config_path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n")
        evidence["setup"]["factory_count"] = sum(
            json.loads(p.read_text()).get("mode") == "factory"
            for p in (home / "instances").glob("*/instance.json"))
        packet = json.loads((instance / "evidence_packet.json").read_text())
        evidence["packet_ids"] = [item["id"] for item in packet["items"]]
        evidence["packet_digest"] = hashlib.sha256(json.dumps(packet, sort_keys=True,
            separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
        step = "author"
        author_event_before = len(jsonl(model_home / "broker-events.jsonl"))
        started = time.monotonic()
        author = json.loads(run_cli(str(SRC / "admin.py"), "author", "--instance-dir", str(instance),
            "--brief", str(ROOT / "definitions" / "authoring-brief-report.md"),
            "--label", "report", "--base-template", str(ROOT / "definitions" / "report-template.json"),
            timeout=650))
        author["admin_exit_code"] = 0
        author["broker_streams"] = [x for x in jsonl(model_home / "broker-events.jsonl")[author_event_before:]
                                     if x.get("event") == "stream"]
        author["model_calls"] = len(author["broker_streams"])
        author["seconds"] = round(time.monotonic() - started, 3)
        evidence["authoring"] = author
        evidence["authoring_call_count"] = author.get("model_calls")
        evidence["broker_pids_during"] = [_pid(broker)]
        if args.provider == "scripted":
            evidence["mock_authoring_exit"] = stop_process(mock)
            mock = None
            with (home / "mock.log").open("a") as log:
                mock = subprocess.Popen([os.environ.get("EXO_NODE", "node"),
                    str(ROOT / "broker" / "testing" / "mock-codex.mjs"),
                    "--port", str(args.mock_port), "--record", str(home / "mock-requests.jsonl"),
                    "--script", "director"], cwd=ROOT, stdout=log, stderr=log,
                    start_new_session=True)
            record_process("mock-director", mock.pid)
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline and args.mock_port not in _listen([args.mock_port]):
                if mock.poll() is not None:
                    raise RuntimeError("loopback Director mock exited during startup")
                time.sleep(.1)
            if args.mock_port not in _listen([args.mock_port]):
                raise TimeoutError("loopback Director mock did not listen")
        from binding import PublicationStore
        catalog = PublicationStore(instance / "catalog")
        evidence["publications"] = catalog.list()
        active = catalog.active()
        published_package = json.loads((instance / "catalog" /
            f"{active['package_digest']}.json").read_text())
        evidence["active_publication"] = {**active,
            "bindings": published_package.get("bindings"),
            "packet_digest": hashlib.sha256(json.dumps(published_package.get("evidence_packet"),
                sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()}
        evidence["import_audit"] = _import_audit()
        step = "harness"
        harness = start_harness(instance, args.harness_port)
        record_process("harness", harness.pid)
        card = http(f"http://127.0.0.1:{args.harness_port}/.well-known/agent-card.json", token=False)
        evidence["setup"]["card_skill_count"] = len(card.get("skills") or [])
        evidence["setup"]["served_factory_card"] = {"body": card,
            "sha256": _sha(json.dumps(card, sort_keys=True, separators=(",", ":"),
                ensure_ascii=False))}
        base = f"http://127.0.0.1:{args.harness_port}"
        question = packet["default_question"]
        director_stream_windows = []
        for number in routes:
            step = f"route-{number}"
            evidence["broker_pids_during"].append(_pid(broker))
            route_event_before = len(jsonl(model_home / "broker-events.jsonl"))
            if number in (2, 3):
                stimuli = json.loads((ROOT / "scenarios" / "sf_stimuli.json").read_text())
                control = stimuli[f"route{number}"]
                # Test-only A2A extension; it binds to the next new contextId.
                stimulus_client.arm(f"http://127.0.0.1:{args.services_port_base + 2}", control)
            route = _route(base, instance, question, number)
            evidence["broker_pids_during"].append(_pid(broker))
            runner_ready = home / "runner" / "runner-ready.json"
            if runner_ready.exists():
                ready = json.loads(runner_ready.read_text())
                record_process("runner", ready.get("pid"))
                for kind, pid in (ready.get("pids") or {}).items():
                    record_process("runner-" + kind, pid)
                for build in (ready.get("builds") or {}).values():
                    record_process("runner-worker", build.get("pid"))
            if number == 3:
                route["wait_status"] = None
                address = json.loads((home / "runner" / "runner-ready.json").read_text())["address"]
                from temporalio.client import Client
                from factory import FactoryRun
                async def child_status():
                    client = await Client.connect(address, namespace="exomachina")
                    parent = await client.get_workflow_handle(route["run_id"]).query(FactoryRun.status)
                    return await client.get_workflow_handle(parent["child_id"]).query(FactoryRun.status)
                route["wait_status"] = asyncio.run(child_status())
                route["follow_up_text"] = FOLLOW_UP
                follow = _send_text(base, FOLLOW_UP, route["task_id"], route["context_id"])
                route["caller_messages"].append(follow["message"])
                route["follow_up_original_task"] = (follow["task"].get("id") == route["task_id"] and
                    follow["task"].get("contextId") == route["context_id"])
                deadline = time.monotonic() + 180
                while time.monotonic() < deadline:
                    route["task"] = a2a_get(base, route["task_id"])
                    state = task_state(route["task"])
                    if route["observed_states"][-1] != state:
                        route["observed_states"].append(state)
                    if state in ("completed", "failed"):
                        break
                    time.sleep(.25)
                if task_state(route["task"]) != "completed":
                    evidence["routes"][str(number)] = route
                    raise TimeoutError("route 3 Director follow-up did not complete the original Task")
            address = json.loads((home / "runner" / "runner-ready.json").read_text())["address"]
            histories = asyncio.run(export_histories(address, route["run_id"],
                home / "evidence-raw" / f"{args.provider}-{args.attempt}-route{number}"))
            for role, workflow in histories["workflows"].items():
                raw_path = Path(workflow["history_path"])
                export_path = evidence_dir / f"{args.provider}-{args.attempt}-route{number}" / f"{role}.json"
                workflow["redaction"] = _redact_history(raw_path, export_path)
                workflow["history_path"] = _record_path(export_path)
            route["child_run_id"] = histories.get("parent_status_query", {}).get("child_id")
            async def final_state():
                from temporalio.client import Client
                from factory import FactoryRun
                client = await Client.connect(address, namespace="exomachina")
                parent = client.get_workflow_handle(route["run_id"])
                parent_status = await parent.query(FactoryRun.status)
                child = client.get_workflow_handle(parent_status["child_id"])
                return await child.query(FactoryRun.status), await asyncio.wait_for(
                    parent.result(), timeout=60)
            route["child_status"], final_result = asyncio.run(final_state())
            route["result_status"] = final_result.get("status")
            route["workflows"] = _history_summary(histories, route)
            calls = _rows(instance / "director.sqlite3", "director_tool_calls")
            route["director_calls"] = [{**x, "arguments": _json_field(x, "arguments_json"),
                "result": _json_field(x, "result_json")} for x in calls if x.get("task_id") == route["task_id"]]
            route["director_turns"] = [{**(_json_field(x, "result_json") or {}),
                "model_kind": x.get("model_kind"), "message_id": x.get("message_id"),
                "task_id": x.get("task_id")} for x in
                _rows(instance / "director.sqlite3", "director_turns") if x.get("task_id") == route["task_id"]]
            route["director_turn_count"] = len(route["director_turns"])
            route["history_control_clean"] = all(_history_control_audit(Path(w["raw_path"]))
                for w in route["workflows"])
            route["handoff_audit"] = _handoff_audit(
                [(w["workflow_id"], Path(w["raw_path"])) for w in route["workflows"]],
                testbed.get("factory_id") or "report-factory", home / "handoff-digest.key",
                home)
            route["receipt_audit"] = _receipt_audit(
                [(w["workflow_id"], Path(w["raw_path"])) for w in route["workflows"]],
                testbed.get("factory_id") or "report-factory", home)
            director_stream_windows.extend(x for x in
                jsonl(model_home / "broker-events.jsonl")[route_event_before:]
                if x.get("event") == "stream")
            evidence["routes"][str(number)] = route
            evidence_path.write_text(json.dumps(evidence, indent=2, sort_keys=True, default=str) + "\n")
        step = "collect"
        evidence["journal"] = _journal(home)
        evidence["activity_log"] = jsonl(home / "runner" / "activities.jsonl")
        evidence["agents"] = _collect_agents(home, testbed, evidence["journal"],
                                              evidence["activity_log"])
        evidence["broker_events"] = jsonl(model_home / "broker-events.jsonl")[broker_event_start:]
        evidence["director_call_count"] = sum(t.get("model_calls", 0) for r in evidence["routes"].values()
                                               for t in r.get("director_turns") or [])
        agent_sessions = {_agent_session(task, evidence["agents"]) for name in AGENTS
            for task in evidence["agents"][name]["tasks"]}
        evidence["broker_owner_counts"] = {
            "authoring": len(author["broker_streams"]),
            "director": sum(row.get("session") not in agent_sessions
                for row in director_stream_windows)}
        evidence["incidents"] = _rows(instance / "director.sqlite3", "incidents")
        evidence["releases"] = _rows(home / "services" / "release" / "release.sqlite3", "deliveries")
        # The agent logs only its own contextIds; the factory journal says which
        # run owns each context (factory-side binding, no agent echo of run_id).
        evidence["stimulus_log"] = stimulus_client.bind_to_runs(
            stimulus_client.log(f"http://127.0.0.1:{args.services_port_base + 2}"),
            stimulus_client.factory_contexts(home / "runner" / "outcomes.sqlite3"),
            (testbed.get("pids") or {}).get("synthesizer", {}).get("identity"))
        for number in routes:
            route = evidence["routes"][str(number)]
            route["stimulus_log"] = [x for x in evidence["stimulus_log"]
                                     if x.get("run_id") == route["child_run_id"]]
        control_sources = []
        for number in routes:
            route = evidence["routes"][str(number)]
            for kind, value in (("caller", route.get("caller_messages")),
                ("run_inputs", (route.get("child_status") or {}).get("run_inputs")),
                ("briefs", [x.get("brief") for name in AGENTS for x in
                    _run_actions(evidence, number, name)]),
                ("director_args", [x.get("arguments") for x in route.get("director_calls") or []])):
                control_sources.append({"route": number, "kind": kind,
                                        "clean": not _has_control_key(value)})
            control_sources.append({"route": number, "kind": "history_inputs",
                                    "clean": route.get("history_control_clean") is True})
        evidence["control_audit"] = {"forbidden_keys": sorted(FORBIDDEN_CONTROLS),
            "sources": control_sources,
            "forbidden_found": any(not x["clean"] for x in control_sources)}
        for number in routes:
            route = evidence["routes"][str(number)]
            if number in (1, 2):
                report = _text(route["task"])
                if report:
                    path = evidence_dir / f"{args.provider}-{args.attempt}-route{number}.md"
                    path.write_text(report)
                    route["report_path"] = _record_path(path)
                    route["report_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
                    if number == 1:
                        from agent_roles import REPORT_SECTIONS, usefulness_check
                        route["usefulness"] = usefulness_check(_report(
                            _run_actions(evidence, 1, "synthesizer")[0]), packet)
                        route["usefulness"]["required_sections"] = list(REPORT_SECTIONS)
                        route["usefulness"]["sections_present"] = all(
                            section in report for section in REPORT_SECTIONS)
                        evidence["semantic_reading"] = {
                            "status": "pending-orchestrator-reading",
                            "report_path": _record_path(path), "packet_ids": evidence["packet_ids"]}
        step = "leak-scan"
        for route in evidence["routes"].values():
            for workflow in route.get("workflows") or []:
                workflow.pop("raw_path", None)
        evidence = _redact_export(evidence)
        evidence_path.write_text(json.dumps(evidence, indent=2, sort_keys=True, default=str) + "\n")
        control = positive_control(home)
        evidence["positive_control"] = {"detected": bool(control.get("scan", {}).get("hits")),
            "control_directory": control["control_directory"], "scan": control.get("scan")}
        candidates = candidate_files()
        paths = [home, evidence_dir, model_home, *candidates]
        scan = scan_paths(paths)
        from sf_attest import exported_files, verify_redaction
        redaction = verify_redaction(exported_files(evidence_path))
        manifest = [{"path": _record_path(path),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest()} for path in candidates]
        evidence["leak_scan"] = {"hit_count": len(scan.get("hits") or []),
            "scan_inputs": [_record_path(path) for path in paths],
            "required_scan_inputs": [_record_path(path) for path in (home, evidence_dir, model_home)],
            "candidate_manifest": manifest,
            "coverage": {"manifest_count": len(manifest),
                "scanner_file_count": scan.get("files_scanned"),
                "zero_hits": scan.get("hits") == [],
                "positive_control_detected": evidence["positive_control"]["detected"],
                "candidate_files_hashed": bool(manifest) and all(
                    _sha256_field(row["sha256"]) for row in manifest)},
            "sensitive_fields_redacted": redaction,
            "exported_file_count": len(exported_files(evidence_path)), "raw": scan}
        version = evidence["routes"].get("1", {}).get("workflows") or []
        evidence["interpreter_build"] = version[0].get("build_id") if version else None
        evidence["manifest_digest"] = version[0].get("manifest_digest") if version else None
        inventory = _expected_inventory(evidence)
        evidence["route_inventory"] = {str(n): bool(inventory[str(n)]["complete"])
                                        for n in (1, 2, 3)}
        evidence["status"] = "collected"
    except Exception as error:
        evidence["status"] = "failed"
        evidence["failure"] = {"step": step, "type": type(error).__name__, "message": str(error)[:500]}
    finally:
        cleanup = {"errors": [], "listeners": None}
        if harness is not None:
            try:
                cleanup["harness_exit"] = stop_process(harness)
            except Exception as error:
                cleanup["errors"].append("harness:" + type(error).__name__)
        if (home / "runner").exists():
            try:
                runner_ready = home / "runner" / "runner-ready.json"
                if runner_ready.exists():
                    ready = json.loads(runner_ready.read_text())
                    record_process("runner", ready.get("pid"))
                    for kind, pid in (ready.get("pids") or {}).items():
                        record_process("runner-" + kind, pid)
                    for build in (ready.get("builds") or {}).values():
                        record_process("runner-worker", build.get("pid"))
                cleanup["runner"] = json.loads(run_cli(str(SRC / "runner.py"), "stop", "--home", str(home)))
            except Exception as error:
                cleanup["errors"].append("runner:" + type(error).__name__)
        if (home / "testbed").exists():
            try:
                cleanup["services"] = json.loads(run_cli(str(ROOT / "services" / "testbed.py"),
                    "down", "--home", str(home), "--port-base", str(args.services_port_base)))
            except Exception as error:
                cleanup["errors"].append("services:" + type(error).__name__)
        if broker is not None and not broker_running_before:
            try:
                if broker.is_running():
                    broker.stop()
            except Exception as error:
                cleanup["errors"].append("broker:" + type(error).__name__)
        if mock is not None:
            try:
                cleanup["mock_exit"] = stop_process(mock)
            except Exception as error:
                cleanup["errors"].append("mock:" + type(error).__name__)
        evidence["broker_pid_after"] = _pid(broker) if broker is not None else None
        ports = [*range(args.runner_port_base, args.runner_port_base + 13),
                 *range(args.runner_member_base, args.runner_member_base + 5),
                 args.harness_port, *range(args.services_port_base, args.services_port_base + 20)]
        if args.provider == "scripted":
            ports.append(args.mock_port)
        cleanup["listeners"] = _listen(ports)
        cleanup["ports_checked"] = ports
        cleanup["started_processes"] = [{**row, "exited": not _alive(row["pid"]),
            "group_exited": not _group_alive(row["pgid"]) if row["pgid"] else False}
            for row in started_processes]
        cleanup["stop_results_valid"] = (
            isinstance(cleanup.get("services"), dict) and
            set(cleanup["services"]) == set(SERVICES) and
            all(value in ("stopped", "already-stopped") for value in cleanup["services"].values()) and
            isinstance(cleanup.get("runner"), dict) and cleanup["runner"].get("running") is False and
            cleanup["runner"].get("pid") is None and
            (harness is None or isinstance(cleanup.get("harness_exit"), int)) and
            (mock is None or isinstance(cleanup.get("mock_exit"), int)))
        evidence["cleanup"] = cleanup
        evidence["checks"] = check_evidence(evidence)
        evidence["status"] = ("structural-pass" if all(x["pass"] for x in
            evidence["checks"].values()) else "fail")
        evidence_path.write_text(json.dumps(evidence, indent=2, sort_keys=True, default=str) + "\n")
    print(json.dumps({"status": evidence["status"], "evidence": str(evidence_path),
                      "failed_checks": [k for k, v in evidence["checks"].items() if not v["pass"]]}))
    return 0 if evidence["status"] == "structural-pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
