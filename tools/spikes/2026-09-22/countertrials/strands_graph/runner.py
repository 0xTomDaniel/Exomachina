"""Bounded Strands Graph publication/restart probe; no provider calls.

This intentionally shows the application-owned definition loader, binding and
approval hook. It is not a proposed production interpreter or scheduler.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

from strands import Agent
from strands.hooks import BeforeNodeCallEvent
from strands.models import Model
from strands.multiagent import GraphBuilder
from strands.multiagent.base import Status
from strands.session import SnapshotSessionManager
from strands.storage import LocalFileStorage


SOURCE = Path(__file__).parent / "definitions"
ALLOWED_NODES = {"research_a", "research_b", "join", "verify", "review", "deliver"}
ALLOWED_CONDITIONS = {None, "both_research_complete"}


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def validate(definition: dict[str, Any]) -> None:
    """Only enough policy to keep this fixture within its approved vocabulary."""
    if definition.get("factory") != "verified-research-fixture":
        raise ValueError("unapproved factory")
    if type(definition.get("revision")) is not int or definition["revision"] < 1:
        raise ValueError("invalid revision")
    nodes = definition.get("nodes")
    if not isinstance(nodes, list) or len(nodes) != len(set(nodes)):
        raise ValueError("duplicate or invalid nodes")
    if not set(nodes) <= ALLOWED_NODES or not {"research_a", "research_b", "join", "review", "deliver"} <= set(nodes):
        raise ValueError("unapproved or missing nodes")
    edges = definition.get("edges")
    if not isinstance(edges, list):
        raise ValueError("invalid edges")
    for edge in edges:
        if edge.get("from") not in nodes or edge.get("to") not in nodes:
            raise ValueError("unknown endpoint")
        if edge.get("condition") not in ALLOWED_CONDITIONS:
            raise ValueError("unapproved condition")
    incoming_deliver = [edge for edge in edges if edge["to"] == "deliver"]
    if len(incoming_deliver) != 1 or incoming_deliver[0]["from"] != "review":
        raise ValueError("review must be the sole delivery predecessor")
    incoming_join = [edge for edge in edges if edge["to"] == "join"]
    if {(edge["from"], edge.get("condition")) for edge in incoming_join} != {
        ("research_a", "both_research_complete"),
        ("research_b", "both_research_complete"),
    }:
        raise ValueError("explicit AND join is required")


class FixtureModel(Model):
    """External model substitute: fixed node marker, no business decisions."""

    def __init__(self, marker: str, root: Path, run_id: str) -> None:
        self.marker = marker
        self.events = root / f"{run_id}.events.jsonl"

    def update_config(self, **model_config: Any) -> None:
        pass

    def get_config(self) -> dict[str, Any]:
        return {"model_id": "deterministic-fixture", "context_window_limit": 16000}

    async def structured_output(self, *args: Any, **kwargs: Any):
        raise NotImplementedError("not used")
        yield  # pragma: no cover

    async def stream(self, messages, tool_specs=None, system_prompt=None, **kwargs):
        with self.events.open("a") as log:
            log.write(canonical({"node": self.marker}) + "\n")
        yield {"messageStart": {"role": "assistant"}}
        yield {"contentBlockStart": {"start": {}}}
        yield {"contentBlockDelta": {"delta": {"text": self.marker}}}
        yield {"contentBlockStop": {}}
        yield {"messageStop": {"stopReason": "end_turn"}}


def publish(root: Path, revision: int) -> dict[str, Any]:
    definition = json.loads((SOURCE / f"v{revision}.json").read_text())
    validate(definition)
    if definition["revision"] != revision:
        raise ValueError("revision mismatch")
    catalog = root / "catalog"
    catalog.mkdir(parents=True, exist_ok=True)
    target = catalog / f"v{revision}.json"
    serialized = canonical(definition)
    if target.exists() and target.read_text() != serialized:
        raise ValueError("published revision is immutable")
    if not target.exists():
        temporary = catalog / f"v{revision}.tmp"
        temporary.write_text(serialized)
        os.replace(temporary, target)
    return {"revision": revision, "digest": digest(definition)}


def identity(root: Path) -> str:
    root.mkdir(parents=True, exist_ok=True)
    path = root / "identity.json"
    if path.exists():
        return json.loads(path.read_text())["id"]
    value = str(uuid4())
    path.write_text(canonical({"id": value}))
    return value


def get_definition(root: Path, revision: int, expected_digest: str) -> dict[str, Any]:
    definition = json.loads((root / "catalog" / f"v{revision}.json").read_text())
    validate(definition)
    if digest(definition) != expected_digest:
        raise ValueError("published definition was modified")
    return definition


def build_graph(root: Path, run_id: str, definition: dict[str, Any]):
    revision = definition["revision"]
    builder = GraphBuilder()
    builder.set_graph_id(f"verified-research-v{revision}-{run_id}")
    for node_id in definition["nodes"]:
        agent = Agent(
            name=node_id,
            system_prompt=f"Fixture node: {node_id}",
            model=FixtureModel(node_id, root, run_id),
            callback_handler=None,
        )
        builder.add_node(agent, node_id)
    for edge in definition["edges"]:
        condition = None
        if edge.get("condition") == "both_research_complete":
            def condition(state):
                return all(
                    node in state.results and state.results[node].status == Status.COMPLETED
                    for node in ("research_a", "research_b")
                )
        builder.add_edge(edge["from"], edge["to"], condition=condition)
    builder.set_max_node_executions(20)
    builder.set_session_manager(SnapshotSessionManager(
        session_id=run_id,
        storage=LocalFileStorage(str(root / "sessions")),
        multi_agent_save_latest_on="node",
    ))
    graph = builder.build()

    def wait_before_delivery(event: BeforeNodeCallEvent) -> None:
        if event.node_id != "deliver":
            return
        response = event.interrupt(
            "director_gate",
            reason={"run_id": run_id, "revision": revision},
        )
        if response != "approve":
            event.cancel_node = "approval denied"
            return
        # Fixture-only rendezvous: hold two already-restored processes at the
        # same node, then release one after the other to expose stale ownership.
        rendezvous = os.environ.get("EXO_RENDEZVOUS_DIR")
        owner = os.environ.get("EXO_OWNER_ID")
        if rendezvous and owner:
            gate = Path(rendezvous)
            gate.mkdir(parents=True, exist_ok=True)
            (gate / f"ready-{owner}").touch()
            deadline = time.monotonic() + 20
            while not (gate / f"release-{owner}").exists():
                if time.monotonic() > deadline:
                    raise TimeoutError("contended-resume fixture release timeout")
                time.sleep(0.02)

    graph.add_hook(wait_before_delivery, BeforeNodeCallEvent)
    return graph


def summarize(result) -> dict[str, Any]:
    return {
        "status": result.status.value,
        "execution_order": [node.node_id for node in result.execution_order],
        "node_statuses": {key: value.status.value for key, value in result.results.items()},
        "interrupts": [
            {"id": interrupt.id, "name": interrupt.name, "reason": interrupt.reason}
            for interrupt in result.interrupts
        ],
    }


def start(root: Path, run_id: str, published: dict[str, Any]) -> dict[str, Any]:
    run_path = root / f"{run_id}.json"
    if run_path.exists():
        raise ValueError("run already exists")
    binding = {"run_id": run_id, "revision": published["revision"], "digest": published["digest"]}
    run_path.write_text(canonical(binding))
    definition = get_definition(root, binding["revision"], binding["digest"])
    result = build_graph(root, run_id, definition)("fixture brief")
    return {**binding, **summarize(result)}


def resume(root: Path, run_id: str, response: str) -> dict[str, Any]:
    binding = json.loads((root / f"{run_id}.json").read_text())
    definition = get_definition(root, binding["revision"], binding["digest"])
    graph = build_graph(root, run_id, definition)
    # Probe the restored interrupt from the pinned graph. The resumed graph must
    # not silently execute the currently latest factory definition.
    snapshot = root / f"{run_id}.interrupt.json"
    interrupt = json.loads(snapshot.read_text())
    result = graph([{"interruptResponse": {"interruptId": interrupt["id"], "response": response}}])
    return {**binding, **summarize(result)}


def scenario(root: Path, hold: bool) -> dict[str, Any]:
    harness_id = identity(root)
    first = publish(root, 1)
    first_result = start(root, "run-v1", first)
    if len(first_result["interrupts"]) != 1:
        raise RuntimeError("v1 did not interrupt")
    (root / "run-v1.interrupt.json").write_text(canonical(first_result["interrupts"][0]))
    second = publish(root, 2)
    second_result = start(root, "run-v2", second)
    if len(second_result["interrupts"]) != 1:
        raise RuntimeError("v2 did not interrupt")
    (root / "run-v2.interrupt.json").write_text(canonical(second_result["interrupts"][0]))
    ready = {"harness_id": harness_id, "v1": first_result, "v2": second_result}
    print(canonical(ready), flush=True)
    if hold:
        while True:
            time.sleep(1)
    return ready


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["scenario", "resume", "validate-bypass"])
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--run-id")
    parser.add_argument("--response", default="approve")
    parser.add_argument("--hold", action="store_true")
    args = parser.parse_args()
    if args.action == "scenario":
        scenario(args.root, args.hold)
    elif args.action == "resume":
        if not args.run_id:
            parser.error("--run-id required")
        print(canonical(resume(args.root, args.run_id, args.response)))
    else:
        bypass = json.loads((SOURCE / "v1.json").read_text())
        bypass["edges"] = [edge for edge in bypass["edges"] if edge["to"] != "deliver"]
        bypass["edges"].append({"from": "join", "to": "deliver"})
        try:
            validate(bypass)
        except ValueError as exc:
            print(canonical({"rejected": str(exc)}))
        else:
            raise RuntimeError("review bypass passed fixture validator")


if __name__ == "__main__":
    main()
