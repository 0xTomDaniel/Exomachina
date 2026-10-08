"""Stable Temporal interpreter for the arbitration factory node vocabulary."""
from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy, VersioningBehavior
from temporalio.exceptions import ChildWorkflowError

with workflow.unsafe.imports_passed_through():
    from adapter import assign, release, review, synthesize, typed_join
    from binding import QUEUE, verify_closure
    from buildinfo import BUILD_ID
    from definition import digest, validate_run_inputs
    from incident_projection import incident_result
    from quality_authority import quality_action_id


ACTIVITY_TIMEOUT = timedelta(seconds=90)
RETRY = RetryPolicy(initial_interval=timedelta(seconds=1), maximum_attempts=12)


def _activity(fn, input: dict):
    options = {"start_to_close_timeout": ACTIVITY_TIMEOUT, "retry_policy": RETRY}
    if fn in {assign, synthesize, review}:
        options["heartbeat_timeout"] = timedelta(seconds=15)
    return workflow.execute_activity(fn, input, **options)


def nested_workflow_input(parent: dict, child_run: str, definition_digest: str,
                          document: dict) -> dict:
    """Build child input from the parent's non-secret pinned authority record."""
    director = parent["director"]
    if not isinstance(director, dict) or set(director) != {"identity", "epoch"}:
        raise ValueError("nested workflow authority must be token-free")
    child_input = {"run": child_run, "definition_digest": definition_digest,
            "package_digest": parent["package_digest"], "document": document,
            "package": parent["package"], "closure": parent["closure"],
            "director": {"identity": director["identity"], "epoch": director["epoch"]},
            "run_inputs": parent["run_inputs"],
            "run_inputs_digest": parent["run_inputs_digest"],
            "authorized_actor": parent["authorized_actor"],
            "input_authority": parent["input_authority"],
            "wait_seconds": parent["wait_seconds"]}
    # New root inputs carry the original A2A binding explicitly so nested
    # Temporal histories keep it. Legacy workflow histories remain replayable.
    has_task = "task_id" in parent
    has_context = "context_id" in parent
    if has_task != has_context:
        raise ValueError("nested workflow Task/context binding is incomplete")
    if has_task:
        task_id, context_id = parent["task_id"], parent["context_id"]
        if (not isinstance(task_id, str) or not task_id or
                not isinstance(context_id, str) or not context_id):
            raise ValueError("nested workflow Task/context binding is invalid")
        child_input.update(task_id=task_id, context_id=context_id)
    return child_input


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
        self.last_verdict: dict | None = None
        self.max_repairs = 0
        self.acceptance: dict | None = None
        self.release_receipt: dict | None = None
        self.repair_count = 0
        self.decision: dict | None = None
        self.deadline: float | None = None
        self.wait_started_at: str | None = None
        self.unresolved: dict | None = None
        self.owner_epoch = 0
        self.child_result: dict | None = None
        self.run_inputs: dict = {}
        self.run_inputs_digest = ""
        self.authorized_actor = ""
        self.input_authority: dict = {}
        self.incident: dict | None = None
        self.director_identity = ""
        self.human_actor: str | None = None
        self.applied_decisions: dict[str, str] = {}
        self.permitted_actions: list[str] = []
        self._explicit_assignment_bindings = False
        self._explicit_factory_binding = False
        self._assignment_ids: dict[tuple[str, str | None], str] = {}

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
            "max_repairs": self.max_repairs,
            "quality_verdict": self.last_verdict,
            "authoritative_acceptance": self.acceptance,
            "release_receipt": self.release_receipt,
            "deadline": self.deadline, "wait_started_at": self.wait_started_at,
            "unresolved": self.unresolved,
            "owner_epoch": self.owner_epoch,
            "permitted_actions": list(self.permitted_actions),
            "decision_id": self.decision and self.decision.get("command_id"),
            "decision_actor": self.human_actor if self.phase == "awaiting-human" else self.director_identity,
            "applied_decisions": dict(self.applied_decisions),
            "run_inputs": self.run_inputs, "run_inputs_digest": self.run_inputs_digest,
            "authorized_actor": self.authorized_actor,
            "input_authority": self.input_authority,
            "incident": self.incident,
        }

    @workflow.update
    def claim_owner(self, command: dict) -> str:
        self.owner_epoch = command["epoch"]
        return "owner-claimed"

    @claim_owner.validator
    def validate_claim_owner(self, command: dict) -> None:
        if (not isinstance(command, dict) or set(command) != {"actor", "epoch"}
                or command["actor"] != self.director_identity
                or type(command["epoch"]) is not int or command["epoch"] <= self.owner_epoch):
            raise ValueError("stale or unauthorized owner claim")

    @workflow.update
    def director_command(self, command: dict) -> str:
        self.decision = command
        outcome = f"{command['action']}-recorded"
        self.applied_decisions[command["command_id"]] = outcome
        return outcome

    @director_command.validator
    def validate_director_command(self, command: dict) -> None:
        if self.phase not in {"awaiting-director", "awaiting-human"} or self.decision is not None:
            raise ValueError("no current Director wait")
        if self.deadline is None or workflow.now().timestamp() >= self.deadline:
            raise ValueError("Director command expired")
        if not isinstance(command, dict) or set(command) != {
            "command_id", "action", "actor", "run", "definition_digest",
            "revision", "sha256", "epoch"}:
            raise ValueError("invalid Director command envelope")
        expected_actor = self.human_actor if self.phase == "awaiting-human" else self.director_identity
        if (command["action"] not in self.permitted_actions or
                command["actor"] != expected_actor):
            raise ValueError("unauthorized decision action or actor")
        if not command["command_id"]:
            raise ValueError("missing Director command identity")
        if command["command_id"] in self.applied_decisions:
            raise ValueError("decision identity is already applied")
        if command["epoch"] != self.owner_epoch:
            raise ValueError("stale Director owner")
        if (command["run"] != self.run_id
                or command["definition_digest"] != self.definition_digest
                or self.current is None
                or command["revision"] != self.current["revision"]
                or command["sha256"] != self.current["sha256"]):
            raise ValueError("stale or wrong-run Director command")

    async def _hold_unresolved(self, reason: str, receipt: dict) -> dict:
        self.phase = reason
        self.unresolved = receipt
        return incident_result(self.run_id, self.definition_digest, reason, receipt)

    def _assignment_input(self, node: str, value: dict, *, branch: str | None = None) -> dict:
        """Issue distinct opaque execution facts once per logical invocation.

        Temporal's deterministic UUID stream is recorded/replayed by the engine.
        Activity retries reuse the resulting input; model-call retries never issue
        a new assignment or attempt. Old histories retain their exact envelopes.
        """
        if not self._explicit_assignment_bindings:
            return value
        key = (node, branch)
        if key not in self._assignment_ids:
            self._assignment_ids[key] = str(workflow.uuid4())
        factory_binding = {}
        if self._explicit_factory_binding:
            if not isinstance(self.director_identity, str) or not self.director_identity:
                raise ValueError("assignment factory authority is unavailable")
            factory_binding = {"factory_id": self.director_identity}
        return {**value, **factory_binding, "node": node,
                "assignment_id": self._assignment_ids[key],
                "attempt_id": str(workflow.uuid4())}

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
                    jobs.append(_activity(assign, self._assignment_input(at, {
                        "run": self.run_id, "digest": self.definition_digest,
                        "instance": instance, "result_type": branch["result_type"],
                        "capability": branch["capability"],
                        "url": service["url"], "identity": service["identity"],
                        "binding": service,
                        "contract": input["closure"]["contracts"][branch["service"]],
                        "packet": package["evidence_packet"],
                        "question": self.run_inputs["question"],
                    }, branch=instance)))
                receipts = await asyncio.gather(*jobs)
                branches = dict(zip(node["branches"], receipts, strict=True))
                unknown = next((value for value in receipts if "unresolved" in value), None)
                if unknown:
                    return await self._hold_unresolved("unresolved-assignment", unknown)
                self.completed.append(at)
                at = node["next"]
            elif kind == "join":
                selected = {name: branches[name] for name in node["branches"]}
                joined = await _activity(typed_join, {
                    "receipts": selected, "run": self.run_id, "packet": package["evidence_packet"],
                })
                self.completed.append(at)
                at = node["next"]
            elif kind == "synthesize":
                prior = ({key: self.current[key] for key in ("revision", "sha256", "content")}
                         if self.repair_count else None)
                findings = self.verdict["findings"] if self.repair_count else None
                service = bindings[node["service"]]
                self.current = await _activity(synthesize, self._assignment_input(at, {
                    "run": self.run_id, "digest": self.definition_digest,
                    "revision": f"r{self.repair_count + 1}", "question": self.run_inputs["question"],
                    "packet": package["evidence_packet"], "evidence": joined,
                    "prior": prior, "quality_findings": findings,
                    "binding": service, "contract": input["closure"]["contracts"][node["service"]],
                }))
                if "unresolved" in self.current:
                    return await self._hold_unresolved("synthesis-incident", self.current)
                self.verdict = None
                self.acceptance = None
                self.completed.append(at + ":" + self.current["revision"])
                at = node["next"]
            elif kind == "quality":
                quality = next(value for value in bindings.values() if value["role"] == "quality")
                quality_name = next(name for name, value in bindings.items() if value["role"] == "quality")
                assignment_id = self.run_id + ":quality"
                attempt = self.repair_count + 1
                outcome = await _activity(review, self._assignment_input(at, {
                    "run": self.run_id, "digest": self.definition_digest,
                    "binding": quality, "contract": input["closure"]["contracts"][quality_name],
                    "candidate": self.current, "question": self.run_inputs["question"],
                    "packet": package["evidence_packet"],
                    "policy_digest": input["closure"]["manifest"]["quality_policy_digest"],
                    "rubric_digest": input["closure"]["quality_policy"].get("rubric_digest"),
                    "assignment_id": assignment_id, "attempt": attempt,
                }))
                if "inconsistent" in outcome:
                    return await self._hold_unresolved("quality-incident", outcome)
                artifact = outcome["artifact"]
                self.verdict = artifact
                self.last_verdict = artifact
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
                policy = node.get("human")
                self.human_actor = policy["actor"] if policy is not None else None
                if self.human_actor == self.director_identity:
                    raise ValueError("human escalation actor must differ from Director authority")
                self.permitted_actions = ["abort", "escalate"] if policy is not None else ["abort"]
                self.wait_started_at = workflow.now().isoformat()
                self.deadline = (workflow.now() + timedelta(seconds=input.get("wait_seconds", 40))).timestamp()
                try:
                    await workflow.wait_condition(lambda: self.decision is not None,
                                                  timeout=timedelta(seconds=input.get("wait_seconds", 40)))
                except asyncio.TimeoutError:
                    self.phase = "director-expired"
                    return {"status": "expired", "run": self.run_id,
                            "definition_digest": self.definition_digest,
                            "revision": self.current["revision"], "released": False}
                if self.decision["action"] == "escalate":
                    self.completed.append(at + ":escalate")
                    self.phase = "awaiting-human"
                    self.permitted_actions = ["abort"]
                    self.decision = None
                    timeout = timedelta(seconds=policy["timeout_seconds"])
                    self.wait_started_at = workflow.now().isoformat()
                    self.deadline = (workflow.now() + timeout).timestamp()
                    try:
                        await workflow.wait_condition(lambda: self.decision is not None, timeout=timeout)
                    except asyncio.TimeoutError:
                        self.phase = "human-expired"
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
                }
                receipt = await _activity(release, {
                    "url": receiver["url"], "identity": receiver["identity"],
                    "mode": "participating",
                    "command": command,
                })
                if "unresolved" in receipt:
                    return await self._hold_unresolved("unresolved-release", receipt)
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
                try:
                    child_result = await workflow.execute_child_workflow(
                        FactoryRun.run,
                        nested_workflow_input(input, self.child_id,
                                              node["child_digest"], child),
                        id=self.child_id, task_queue=QUEUE)
                except ChildWorkflowError as error:
                    # This state is written in the parent's durable Workflow history.
                    # The parent has not copied child acceptance or invoked release.
                    self.incident = {"run_id": self.run_id, "child_id": self.child_id,
                        "package_digest": self.package_digest,
                        "failure_class": type(error).__name__,
                        "timestamp": workflow.now().isoformat()}
                    self.phase = "child-failed"
                    return {"status": "failed", "run": self.run_id,
                            "incident": self.incident, "released": False}
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
                # Deliver exactly the accepted revision, never a later or earlier one.
                accepted = self.child_result["artifact"] if self.child_result else self.current
                if (accepted is None or accepted["revision"] != self.acceptance["revision"]
                        or accepted["sha256"] != self.acceptance["sha256"]):
                    raise ValueError("delivered artifact differs from accepted revision")
                self.phase = "accepted"
                return {"status": "accepted", "run": self.run_id,
                        "definition_digest": self.definition_digest,
                        "acceptance": self.acceptance, "artifact": accepted,
                        "receipt": self.release_receipt, "released": True,
                        "completed": self.completed,
                        "child": self.child_result}
            else:
                raise ValueError("unsupported factory node")

    @workflow.run
    async def run(self, input: dict) -> dict:
        expected = {"run", "definition_digest", "package_digest", "document",
                    "package", "closure", "director", "run_inputs",
                    "run_inputs_digest", "authorized_actor", "input_authority",
                    "wait_seconds"}
        new_binding = {"task_id", "context_id"}
        if set(input) not in (expected, expected | new_binding):
            raise ValueError("invalid factory start input")
        if new_binding & set(input):
            task_id, context_id = input.get("task_id"), input.get("context_id")
            if (not isinstance(task_id, str) or not task_id or
                    not isinstance(context_id, str) or not context_id):
                raise ValueError("invalid original A2A Task/context binding")
        director = input["director"]
        if (not isinstance(director, dict) or set(director) != {"identity", "epoch"}
                or not isinstance(director["identity"], str) or not director["identity"]
                or type(director["epoch"]) is not int or director["epoch"] < 1):
            raise ValueError("invalid token-free Director authority")
        self.run_id = input["run"]
        self.definition_digest = input["definition_digest"]
        self.package_digest = input["package_digest"]
        self.manifest_digest = verify_closure(input["closure"], input["package"],
            build_id=BUILD_ID, definition_digest=self.definition_digest,
            document=input["document"])
        if self.package_digest != input["closure"]["manifest"]["package_digest"]:
            raise ValueError("package digest differs from run closure")
        self.run_inputs = input.get("run_inputs", {})
        self.run_inputs_digest = input.get("run_inputs_digest", "")
        if (validate_run_inputs(input["package"]["run_inputs"], self.run_inputs) != self.run_inputs
                or digest(self.run_inputs) != self.run_inputs_digest):
            raise ValueError("run inputs differ from pinned values")
        self.authorized_actor = input.get("authorized_actor", "")
        self.input_authority = input.get("input_authority", {})
        self.director_identity = director["identity"]
        # Reject a self-escalation policy before any child or model activity.
        documents = [input["document"], *input["package"].get("children", {}).values()]
        if any(node.get("human", {}).get("actor") == self.director_identity
               for document in documents for node in document["nodes"].values()):
            raise ValueError("human escalation actor must differ from Director authority")
        self.owner_epoch = director["epoch"]
        document = input["document"]
        self._parallel_node = next((name for name, node in document["nodes"].items()
                                    if node["type"] == "parallel"), "")
        self.max_repairs = next((node["max_repairs"] for node in document["nodes"].values()
                                 if node["type"] == "repair"), 0)
        self._explicit_assignment_bindings = workflow.patched("exo-explicit-assignment-bindings-v1")
        self._explicit_factory_binding = workflow.patched("exo-explicit-factory-binding-v1")
        return await self.run_node(document, input["package"], input)
