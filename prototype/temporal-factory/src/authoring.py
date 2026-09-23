"""Bounded Strands authoring of validated factory graph packages."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
import importlib.util
import json
import os
from pathlib import Path
from typing import Protocol

from strands import Agent, tool
from strands.models import Model

from definition import ALLOWED, INPUT_SOURCES, INPUT_TYPES, RESULT_TYPES, ROUTE_VALUES
from definition import digest, validate


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def authoring_vocabulary(approved_bindings: dict) -> dict:
    """Describe the entire bounded grammar without exposing executable blocks."""
    return {
        "schema": 1,
        "template_fields": ["schema", "root", "child", "run_inputs"],
        "node_fields": {kind: sorted(fields) for kind, fields in ALLOWED.items()},
        "route_values": {field: sorted(values) for field, values in ROUTE_VALUES.items()},
        "result_types": deepcopy(RESULT_TYPES),
        "input_types": sorted(INPUT_TYPES),
        "input_sources": sorted(INPUT_SOURCES),
        "bounds": {"nodes_per_definition": 32, "parallel_branches": [2, 4],
                   "max_repairs": [1, 2], "run_input_fields": 16},
        "bindings": {name: {"role": binding["role"], "approved": binding.get("approved") is True}
                     for name, binding in approved_bindings.items()},
        "rules": ["All route cases must cover their typed values.",
                  "Source and counter evidence must meet at a typed join.",
                  "Quality must review a candidate before release.",
                  "Repair must have a bounded exhausted edge to Director wait and abort.",
                  "The root may only invoke its digest-pinned child and complete."],
    }


def materialize(template: dict, bindings: dict) -> dict:
    """Pin the child digest and the supplied service bindings in a package."""
    if not isinstance(template, dict) or set(template) != {"schema", "root", "child", "run_inputs"}:
        raise ValueError("template: expected schema, root, child, run_inputs")
    child = deepcopy(template["child"])
    root = deepcopy(template["root"])
    child_digest = digest(child)
    for node in root.get("nodes", {}).values():
        if isinstance(node, dict) and node.get("type") == "nested_factory" and node.get("child_digest") == "@child":
            node["child_digest"] = child_digest
    return {"schema": template["schema"], "root": root,
            "children": {child_digest: child}, "bindings": deepcopy(bindings),
            "run_inputs": deepcopy(template["run_inputs"])}


@dataclass
class AuthoringOutcome:
    status: str
    template: dict | None
    package: dict | None
    package_digest: str | None
    rounds: list[dict]
    model: dict
    approval: dict | None = None
    tool_calls: list[dict] = field(default_factory=list)


class GraphAuthor(Protocol):
    def author(self, brief: str, *, approved_bindings: dict, max_rounds: int,
               base_template: dict | None = None) -> AuthoringOutcome: ...


def approve(package: dict, *, approver: str, policy: dict | str) -> dict:
    """Record bounded automatic approval or a pending human policy decision."""
    if not isinstance(approver, str) or not approver:
        raise ValueError("approver identity required")
    mode = policy if isinstance(policy, str) else policy.get("mode")
    if mode not in {"auto", "human"}:
        raise ValueError("approval policy must be auto or human")
    approved_bindings = policy.get("approved_bindings") if isinstance(policy, dict) else None
    package_digest = validate(package, approved_bindings)
    return {"package_digest": package_digest, "approver": approver,
            "policy": mode, "status": "approved" if mode == "auto" else "pending"}


def _errors(error: Exception, template: dict | None = None) -> list[dict]:
    item: dict = {"code": "validation_error", "message": str(error)}
    if str(error) == "route must cover each typed value" and isinstance(template, dict):
        for name, node in template.get("child", {}).get("nodes", {}).items():
            if isinstance(node, dict) and node.get("type") == "route" and node.get("field") in ROUTE_VALUES:
                missing = sorted(ROUTE_VALUES[node["field"]] - set(node.get("cases", {})))
                if missing:
                    item.update(path=f"child.nodes.{name}.cases", missing_cases=missing)
                    break
    return [item]


class StrandsGraphAuthor:
    def __init__(self, model: Model):
        self.model = model

    def author(self, brief: str, *, approved_bindings: dict, max_rounds: int,
               base_template: dict | None = None) -> AuthoringOutcome:
        rounds: list[dict] = []
        calls: list[dict] = []
        seen: dict[str, dict] = {}
        accepted: dict = {}
        vocabulary = authoring_vocabulary(approved_bindings)

        def evaluate(template_json: str, action: str) -> str:
            try:
                template = json.loads(template_json)
                draft_digest = digest(template)
            except (ValueError, TypeError) as error:
                draft_digest = digest(template_json)
                if len(rounds) >= max_rounds:
                    result = {"ok": False, "draft_digest": draft_digest,
                              "errors": [{"code": "round_limit", "message": "authoring round cap reached"}]}
                else:
                    result = {"ok": False, "draft_digest": draft_digest, "errors": _errors(error)}
                    rounds.append({"round": len(rounds) + 1, "draft_digest": draft_digest,
                                   "valid": False, "errors": deepcopy(result["errors"])})
                calls.append({"tool": action, "draft_digest": draft_digest, "result": result})
                return _json(result)
            if draft_digest in seen:
                result = deepcopy(seen[draft_digest])
            elif len(rounds) >= max_rounds:
                result = {"ok": False, "draft_digest": draft_digest,
                          "errors": [{"code": "round_limit", "message": "authoring round cap reached"}]}
            else:
                try:
                    package = materialize(template, approved_bindings)
                    package_digest = validate(package, approved_bindings)
                    result = {"ok": True, "draft_digest": draft_digest,
                              "package_digest": package_digest, "errors": []}
                    accepted[draft_digest] = (template, package, package_digest)
                except (ValueError, TypeError, KeyError) as error:
                    result = {"ok": False, "draft_digest": draft_digest,
                              "errors": _errors(error, template)}
                seen[draft_digest] = deepcopy(result)
                rounds.append({"round": len(rounds) + 1, "draft_digest": draft_digest,
                               "valid": result["ok"], "errors": deepcopy(result["errors"])})
            if action == "submit_draft" and result["ok"]:
                accepted["submitted"] = accepted[draft_digest]
            calls.append({"tool": action, "draft_digest": draft_digest, "result": deepcopy(result)})
            return _json(result)

        @tool
        def describe_vocabulary() -> str:
            """Return the allowed graph grammar, bounds, and approved service names."""
            calls.append({"tool": "describe_vocabulary", "result": vocabulary})
            return _json(vocabulary)

        @tool
        def validate_draft(template_json: str) -> str:
            """Check a proposed template and return structured errors.

            Args:
                template_json: JSON object with schema, root, child, and run_inputs.
            """
            return evaluate(template_json, "validate_draft")

        @tool
        def submit_draft(template_json: str) -> str:
            """Submit a fully validated graph template for bounded approval.

            Args:
                template_json: JSON object with schema, root, child, and run_inputs.
            """
            return evaluate(template_json, "submit_draft")

        agent = Agent(model=self.model, tools=[describe_vocabulary, validate_draft, submit_draft],
                      system_prompt=("Author a bounded factory graph. Call describe_vocabulary first. "
                                     "Use validate_draft and revise from its errors. Submit only a valid "
                                     "template. A submission must pass package validation. "),
                      callback_handler=None)
        prompt = _json({"brief": brief, "base_template": base_template, "max_rounds": max_rounds})
        agent(prompt)
        config = self.model.get_config()
        model_id = config.get("model_id", type(self.model).__name__) if isinstance(config, dict) else type(self.model).__name__
        scripted = isinstance(self.model, ScriptedAuthoringModel)
        model_info = {"kind": "scripted" if scripted else "strands",
                      "id": model_id, "live": not scripted}
        if "submitted" in accepted:
            template, package, package_digest = accepted["submitted"]
            approval = approve(package, approver="authoring-session",
                               policy={"mode": "auto", "approved_bindings": approved_bindings})
            return AuthoringOutcome("approved", template, package, package_digest,
                                    rounds, model_info, approval, calls)
        status = "round_limit" if len(rounds) >= max_rounds else "no_submission"
        return AuthoringOutcome(status, None, None, None, rounds, model_info, None, calls)


class AuthoringSession:
    def __init__(self, author: GraphAuthor, *, approved_bindings: dict, max_rounds: int = 4):
        if type(max_rounds) is not int or max_rounds < 1:
            raise ValueError("max_rounds must be a positive integer")
        self.author = author
        self.approved_bindings = deepcopy(approved_bindings)
        self.max_rounds = max_rounds

    def run(self, brief: str, base_template: dict | None = None) -> AuthoringOutcome:
        return self.author.author(brief, approved_bindings=self.approved_bindings,
                                  max_rounds=self.max_rounds, base_template=base_template)


def _scripted_first_draft(base: dict | None) -> dict:
    root = deepcopy(base["root"]) if base else {
        "name": "parent_verified_research", "revision": "v5", "start": "invoke_child",
        "nodes": {"invoke_child": {"type": "nested_factory", "child": "verified_research",
                                  "child_digest": "@child", "next": "done"},
                  "done": {"type": "complete"}}}
    root["revision"] = "v5"
    root["nodes"]["invoke_child"]["child_digest"] = "@child"
    branch = lambda kind, service, scope: {"result_type": kind,
        "capability": RESULT_TYPES[kind], "service": service, "scope_status": scope}
    child = {"name": "verified_research", "revision": "v5", "start": "gather",
             "nodes": {
                 "gather": {"type": "parallel", "branches": {
                     "source_alpha": branch("source_evidence", "source_alpha", None),
                     "source_beta": branch("source_evidence", "source_beta", None),
                     "counter_alpha": branch("counter_evidence", "counter_alpha", "clear")},
                     "next": "join_all"},
                 "join_all": {"type": "join", "branches": ["source_alpha", "source_beta", "counter_alpha"],
                              "next": "route_scope"},
                 "route_scope": {"type": "route", "field": "join.route_status",
                                 "cases": {"clear": "draft_clear"}},
                 "draft_unresolved": {"type": "synthesize", "resolved": False,
                                      "next": "independent_quality"},
                 "draft_clear": {"type": "synthesize", "resolved": True,
                                 "next": "independent_quality"},
                 "independent_quality": {"type": "quality", "next": "route_verdict"},
                 "route_verdict": {"type": "route", "field": "verdict.accepted",
                                   "cases": {"true": "publish", "false": "repair"}},
                 "repair": {"type": "repair", "max_repairs": 1,
                            "next": "draft_repair", "exhausted": "director"},
                 "draft_repair": {"type": "synthesize", "resolved": "after_repair",
                                  "next": "independent_quality"},
                 "director": {"type": "director_wait", "reason": "repair_exhausted", "next": "abort"},
                 "abort": {"type": "abort"},
                 "publish": {"type": "release", "service": "release", "next": "done"},
                 "done": {"type": "complete"}}}
    return {"schema": 1, "root": root, "child": child,
            "run_inputs": {"question": {"type": "string", "required": False,
                                        "source": "caller", "allowed_actors": ["fixture-operator"],
                                        "may_affect_acceptance": False}}}


class ScriptedAuthoringModel(Model):
    """Deterministic tool-calling fixture that repairs a returned validation error."""
    def __init__(self):
        self._draft: dict | None = None
        self._vocabulary: dict | None = None
        self._call = 0

    def update_config(self, **model_config):
        pass

    def get_config(self):
        return {"model_id": "scripted-authoring-fixture", "context_window_limit": 16000}

    async def structured_output(self, *args, **kwargs):
        raise NotImplementedError("scripted model uses tools")
        yield  # pragma: no cover

    async def stream(self, messages, tool_specs=None, system_prompt=None, **kwargs):
        last = messages[-1]["content"]
        result_block = next((block["toolResult"] for block in last if "toolResult" in block), None)
        tool_name = None
        argument = {}
        if result_block is None:
            tool_name = "describe_vocabulary"
        else:
            content = result_block["content"][0]
            result = json.loads(content.get("text") or _json(content["json"]))
            if self._vocabulary is None:
                self._vocabulary = result
                prompt = json.loads(messages[0]["content"][0]["text"])
                self._draft = _scripted_first_draft(prompt.get("base_template"))
                tool_name = "validate_draft"
                argument = {"template_json": _json(self._draft)}
            elif not result.get("ok") and self._draft is not None:
                error = result["errors"][0]
                if error.get("message") == "route must cover each typed value" and error.get("missing_cases"):
                    cases = self._draft["child"]["nodes"]["route_scope"]["cases"]
                    for missing in error["missing_cases"]:
                        if missing not in self._vocabulary["route_values"]["join.route_status"]:
                            raise ValueError("feedback requested an undeclared route case")
                        cases[missing] = "draft_unresolved"
                    tool_name = "submit_draft"
                    argument = {"template_json": _json(self._draft)}
        yield {"messageStart": {"role": "assistant"}}
        if tool_name:
            self._call += 1
            yield {"contentBlockStart": {"start": {"toolUse": {
                "toolUseId": f"authoring-call-{self._call}", "name": tool_name}}}}
            yield {"contentBlockDelta": {"delta": {"toolUse": {"input": _json(argument)}}}}
            yield {"contentBlockStop": {}}
            yield {"messageStop": {"stopReason": "tool_use"}}
        else:
            yield {"contentBlockStart": {"start": {}}}
            yield {"contentBlockDelta": {"delta": {"text": "Authoring session finished."}}}
            yield {"contentBlockStop": {}}
            yield {"messageStop": {"stopReason": "end_turn"}}


def model_from_environment() -> tuple[Model | None, str]:
    """Select an installed, configured provider without touching the network."""
    reasons = []
    if os.environ.get("ANTHROPIC_API_KEY"):
        if importlib.util.find_spec("anthropic"):
            from strands.models import AnthropicModel
            return AnthropicModel(), "Anthropic provider available"
        reasons.append("anthropic package unavailable")
    else:
        reasons.append("ANTHROPIC_API_KEY absent")
    aws_configured = any(key.startswith("AWS_") and value for key, value in os.environ.items()) or any(
        (Path.home() / ".aws" / name).exists() for name in ("credentials", "config"))
    if aws_configured:
        if importlib.util.find_spec("boto3"):
            from strands.models import BedrockModel
            return BedrockModel(), "Bedrock provider available"
        reasons.append("boto3 package unavailable")
    else:
        reasons.append("AWS credentials/configuration absent")
    if os.environ.get("OPENAI_API_KEY"):
        if importlib.util.find_spec("openai"):
            from strands.models import OpenAIModel
            return OpenAIModel(), "OpenAI provider available"
        reasons.append("openai package unavailable")
    else:
        reasons.append("OPENAI_API_KEY absent")
    return None, "; ".join(reasons)
