"""Stable Temporal interpreter for the arbitration factory node vocabulary."""
from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy, VersioningBehavior

with workflow.unsafe.imports_passed_through():
    from adapter import assign, release, review, synthesize, typed_join
    from binding import QUEUE, verify_closure


ACTIVITY_TIMEOUT = timedelta(seconds=90)
RETRY = RetryPolicy(initial_interval=timedelta(seconds=1), maximum_attempts=12)
BUILD_ID = "b1"


def _activity(fn, input: dict):
    return workflow.execute_activity(fn, input, start_to_close_timeout=ACTIVITY_TIMEOUT,
                                     retry_policy=RETRY)


@workflow.defn(versioning_behavior=VersioningBehavior.PINNED)
class FactoryRun:
    def __init__(self) -> None:
        self.phase = "starting"
        self.run_id = ""
        self.definition_digest = ""
        self.package_digest = ""
        self.manifest_digest = ""
        self.node = ""
        self.completed: list[str] = []
        self.child_id: str | None = None
        self.current: dict | None = None
        self.verdict: dict | None = None
        self.acceptance: dict | None = None
        self.release_receipt: dict | None = None
        self.repair_count = 0
        self.decision: dict | None = None
        self.deadline: float | None = None
        self.unresolved: dict | None = None
        self.owner_epoch = 0
        self.child_result: dict | None = None

    @workflow.query
    def status(self) -> dict:
        return {
            "phase": self.phase, "run": self.run_id,
            "definition_digest": self.definition_digest,
            "package_digest": self.package_digest, "node": self.node,
            "manifest_digest": self.manifest_digest, "interpreter_build": BUILD_ID,
            "completed": self.completed, "child_id": self.child_id,
            "current_revision": self.current and self.current["revision"],
            "current_sha256": self.current and self.current["sha256"],
            "repair_count": self.repair_count,
            "quality_verdict": self.verdict,
            "authoritative_acceptance": self.acceptance,
            "release_receipt": self.release_receipt,
            "deadline": self.deadline, "unresolved": self.unresolved,
            "owner_epoch": self.owner_epoch,
        }

    @workflow.update
    def claim_owner(self, command: dict) -> str:
        self.owner_epoch = command["epoch"]
        return "owner-claimed"

    @claim_owner.validator
    def validate_claim_owner(self, command: dict) -> None:
        if (not isinstance(command, dict) or set(command) != {"actor", "token", "epoch"}
                or command["actor"] != self._director["identity"]
                or command["token"] != self._director["token"]
                or type(command["epoch"]) is not int or command["epoch"] <= self.owner_epoch):
            raise ValueError("stale or unauthorized owner claim")

    @workflow.update
    def director_command(self, command: dict) -> str:
        self.decision = command
        return "abort-recorded"

    @director_command.validator
    def validate_director_command(self, command: dict) -> None:
        if self.phase != "awaiting-director" or self.decision is not None:
            raise ValueError("no current Director wait")
        if self.deadline is None or workflow.now().timestamp() >= self.deadline:
            raise ValueError("Director command expired")
        if not isinstance(command, dict) or set(command) != {
            "command_id", "action", "actor", "token", "run", "definition_digest",
            "revision", "sha256", "epoch"}:
            raise ValueError("invalid Director command envelope")
        if command["action"] != "abort" or command["actor"] != self._director["identity"]:
            raise ValueError("unauthorized Director action")
        if command["token"] != self._director["token"] or not command["command_id"]:
            raise ValueError("unauthorized Director credential")
        if command["epoch"] != self.owner_epoch:
            raise ValueError("stale Director owner")
        if (command["run"] != self.run_id
                or command["definition_digest"] != self.definition_digest
                or self.current is None
                or command["revision"] != self.current["revision"]
                or command["sha256"] != self.current["sha256"]):
            raise ValueError("stale or wrong-run Director command")

    async def _hold_unresolved(self, reason: str, receipt: dict) -> None:
        self.phase = reason
        self.unresolved = receipt
        await workflow.wait_condition(lambda: False)

    async def run_node(self, document: dict, package: dict, input: dict) -> dict:
        nodes = document["nodes"]
        bindings = package["bindings"]
        branches: dict[str, dict] = {}
        joined: dict | None = None
        at = document["start"]
        while True:
            verify_closure(input["closure"], package, build_id=BUILD_ID,
                           definition_digest=self.definition_digest, document=document)
            self.node = at
            node = nodes[at]
            kind = node["type"]
            self.phase = kind
            if kind == "parallel":
                jobs = []
                for instance, branch in node["branches"].items():
                    verify_closure(input["closure"], package, build_id=BUILD_ID,
                                   definition_digest=self.definition_digest, document=document)
                    service = bindings[branch["service"]]
                    barrier = input.get("faults", {}).get("assignment_barrier")
                    jobs.append(_activity(assign, {
                        "run": self.run_id, "digest": self.definition_digest,
                        "instance": instance, "result_type": branch["result_type"],
                        "scope_status": branch["scope_status"],
                        "drop_ack": input.get("faults", {}).get("drop_assignment_ack") == instance,
                        "url": service["url"], "identity": service["identity"],
                        "lookup_supported": not input.get("faults", {}).get("opaque_assignment", False),
                        "barrier": barrier if barrier and barrier["instance"] == instance else None,
                        "delay_after_remote": input.get("faults", {}).get("delay_assignment", {}).get(instance, 0),
                    }))
                receipts = await asyncio.gather(*jobs)
                branches = dict(zip(node["branches"], receipts, strict=True))
                unknown = next((value for value in receipts if "unresolved" in value), None)
                if unknown:
                    await self._hold_unresolved("unresolved-assignment", unknown)
                self.completed.append(at)
                at = node["next"]
            elif kind == "join":
                selected = {name: branches[name] for name in node["branches"]}
                declarations = {name: document["nodes"][self._parallel_node]["branches"][name]["result_type"]
                                for name in node["branches"]}
                scopes = {name: document["nodes"][self._parallel_node]["branches"][name]["scope_status"]
                          for name in node["branches"] if
                          document["nodes"][self._parallel_node]["branches"][name]["scope_status"] is not None}
                joined = await _activity(typed_join, {
                    "receipts": selected, "run": self.run_id,
                    "digest": self.definition_digest,
                    "declarations": declarations, "scopes": scopes,
                })
                self.completed.append(at)
                at = node["next"]
            elif kind == "synthesize":
                if node["resolved"] == "from_run":
                    mode = input.get("outcome_mode")
                    if mode not in {"after_first_repair", "never"}:
                        raise ValueError("missing or invalid typed run outcome mode")
                    resolved = mode == "after_first_repair" and self.repair_count >= 1
                else:
                    resolved = node["resolved"] is True or (
                        node["resolved"] == "after_repair" and self.repair_count >= 1)
                self.current = await _activity(synthesize, {
                    "join": joined, "revision": f"r{self.repair_count + 1}",
                    "author": f"factory:{self.run_id}", "resolved": resolved,
                })
                self.verdict = None
                self.acceptance = None
                self.completed.append(at + ":" + self.current["revision"])
                at = node["next"]
            elif kind == "quality":
                quality = next(value for value in bindings.values() if value["role"] == "quality")
                command = {
                    "op": "review", "action_id": f"{self.run_id}:quality:{self.current['revision']}:{self.current['sha256'][:12]}",
                    "run_id": self.run_id, "definition_digest": self.definition_digest,
                    "artifact": self.current,
                }
                outcome = await _activity(review, {
                    "url": quality["url"], "identity": quality["identity"],
                    "command": command,
                    "barrier": input.get("faults", {}).get("quality_barrier"),
                })
                artifact = outcome["artifact"]
                if (artifact["revision"] != self.current["revision"]
                        or artifact["sha256"] != self.current["sha256"]
                        or artifact["reviewer"] != quality["identity"]
                        or self.current["author"] == artifact["reviewer"]):
                    raise ValueError("Quality verdict stale or forged")
                self.verdict = artifact
                if artifact["accepted"] is True:
                    # This assignment is the authoritative acceptance transition
                    # in Workflow history, after verified remote Quality evidence.
                    self.acceptance = {
                        "run": self.run_id, "definition_digest": self.definition_digest,
                        "revision": self.current["revision"],
                        "sha256": self.current["sha256"],
                        "attempt": self.repair_count + 1,
                        "reviewer": artifact["reviewer"],
                        "quality_task_id": outcome["task_id"],
                    }
                    if input.get("faults", {}).get("acceptance_timer_seconds"):
                        self.phase = "accepted-before-release"
                        await workflow.sleep(input["faults"]["acceptance_timer_seconds"])
                self.completed.append(at + ":" + self.current["revision"])
                at = node["next"]
            elif kind == "route":
                field = node["field"]
                if field == "join.route_status":
                    value = joined["route_status"]
                elif field == "join.requires_scope":
                    value = "true" if joined["requires_scope"] is True else "false"
                elif field == "verdict.accepted":
                    value = "true" if self.verdict["accepted"] is True else "false"
                else:
                    raise ValueError("unvalidated typed route")
                if value not in node["cases"]:
                    raise ValueError("unknown typed route result")
                self.completed.append(at + ":" + value)
                at = node["cases"][value]
            elif kind == "repair":
                if self.verdict is None or self.verdict["accepted"] is not False:
                    raise ValueError("repair without current Quality rejection")
                if self.repair_count < node["max_repairs"]:
                    self.repair_count += 1
                    self.completed.append(at + f":{self.repair_count}")
                    at = node["next"]
                else:
                    self.completed.append(at + ":exhausted")
                    at = node["exhausted"]
            elif kind == "director_wait":
                self.phase = "awaiting-director"
                self.deadline = (workflow.now() + timedelta(seconds=input.get("wait_seconds", 40))).timestamp()
                try:
                    await workflow.wait_condition(lambda: self.decision is not None,
                                                  timeout=timedelta(seconds=input.get("wait_seconds", 40)))
                except asyncio.TimeoutError:
                    self.phase = "director-expired"
                    return {"status": "expired", "run": self.run_id,
                            "definition_digest": self.definition_digest,
                            "revision": self.current["revision"], "released": False}
                self.completed.append(at + ":abort")
                at = node["next"]
            elif kind == "abort":
                self.phase = "aborted"
                return {"status": "aborted", "run": self.run_id,
                        "definition_digest": self.definition_digest,
                        "revision": self.current["revision"], "released": False}
            elif kind == "release":
                if (self.acceptance is None or self.current is None
                        or self.acceptance["revision"] != self.current["revision"]
                        or self.acceptance["sha256"] != self.current["sha256"]):
                    raise ValueError("release lacks authoritative exact acceptance")
                receiver = bindings[node["service"]]
                command = {
                    "release_id": f"{self.run_id}:release:{self.current['revision']}",
                    "run_id": self.run_id, "definition_digest": self.definition_digest,
                    "revision": self.current["revision"],
                    "sha256": self.current["sha256"],
                    "content": self.current["content"],
                    "drop_ack": input.get("faults", {}).get("drop_release_ack", False),
                }
                receipt = await _activity(release, {
                    "url": receiver["url"], "identity": receiver["identity"],
                    "mode": input.get("faults", {}).get("release_mode", "participating"),
                    "command": command,
                    "barrier": input.get("faults", {}).get("release_barrier"),
                })
                if "unresolved" in receipt:
                    await self._hold_unresolved("unresolved-release", receipt)
                self.release_receipt = receipt
                self.completed.append(at)
                at = node["next"]
            elif kind == "nested_factory":
                verify_closure(input["closure"], package, build_id=BUILD_ID,
                               definition_digest=node["child_digest"],
                               document=package["children"][node["child_digest"]])
                child = package["children"][node["child_digest"]]
                self.child_id = f"{self.run_id}:child:{node['child_digest'][:12]}"
                self.phase = "awaiting-child"
                child_result = await workflow.execute_child_workflow(
                    FactoryRun.run,
                    {"run": self.child_id, "definition_digest": node["child_digest"],
                     "package_digest": self.package_digest, "document": child,
                     "package": package, "closure": input["closure"],
                     "director": input["director"],
                     "outcome_mode": input.get("outcome_mode"),
                     "faults": input.get("faults", {}),
                     "wait_seconds": input.get("wait_seconds", 40)},
                    id=self.child_id, task_queue=QUEUE)
                if child_result["status"] != "accepted":
                    self.phase = "child-" + child_result["status"]
                    return {"status": child_result["status"], "run": self.run_id,
                            "definition_digest": self.definition_digest,
                            "child": child_result, "released": False}
                self.release_receipt = child_result["receipt"]
                self.acceptance = child_result["acceptance"]
                self.child_result = child_result
                self.completed.append(at)
                at = node["next"]
            elif kind == "complete":
                if self.release_receipt is None or self.acceptance is None:
                    raise ValueError("completion without accepted release")
                self.phase = "accepted"
                return {"status": "accepted", "run": self.run_id,
                        "definition_digest": self.definition_digest,
                        "acceptance": self.acceptance,
                        "receipt": self.release_receipt, "released": True,
                        "completed": self.completed,
                        "child": self.child_result}
            else:
                raise ValueError("unsupported factory node")

    @workflow.run
    async def run(self, input: dict) -> dict:
        self.run_id = input["run"]
        self.definition_digest = input["definition_digest"]
        self.package_digest = input["package_digest"]
        self.manifest_digest = verify_closure(input["closure"], input["package"],
            build_id=BUILD_ID, definition_digest=self.definition_digest,
            document=input["document"])
        if self.package_digest != input["closure"]["manifest"]["package_digest"]:
            raise ValueError("package digest differs from run closure")
        self._director = input["director"]
        self.owner_epoch = input["director"].get("epoch", 1)
        document = input["document"]
        self._parallel_node = next((name for name, node in document["nodes"].items()
                                    if node["type"] == "parallel"), "")
        return await self.run_node(document, input["package"], input)
