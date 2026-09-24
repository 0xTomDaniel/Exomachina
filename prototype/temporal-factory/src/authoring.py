"""Bounded Strands authoring of validated factory graph packages."""
from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass, field
import importlib.util
import json
import os
from pathlib import Path
import threading
import time
from typing import Protocol
from urllib.parse import urlparse
import uuid

from strands import Agent, tool
from strands.models import Model
from strands.types.exceptions import ModelThrottledException

from definition import ALLOWED, INPUT_SOURCES, INPUT_TYPES, RESULT_TYPES, ROUTE_VALUES
from definition import digest, validate
from model_broker import (BrokerLost, SubscriptionAuthRequired, SubscriptionQuotaExhausted,
                          provider_error_record)


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
                  "Packet findings and risks must meet at a typed join.",
                  "Quality must review a candidate before release.",
                  "Repair must have a bounded exhausted edge to Director wait and abort.",
                  "The root may only invoke its digest-pinned child and complete."],
    }


def materialize(template: dict, bindings: dict, *, evidence_packet: dict) -> dict:
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
            "run_inputs": deepcopy(template["run_inputs"]),
            "evidence_packet": deepcopy(evidence_packet)}


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
    abort: dict | None = None
    limits: dict = field(default_factory=dict)
    error: dict | None = None
    selection_seconds: float = 0.0


class AuthoringBudgetExhausted(RuntimeError):
    """An author exceeded its hard model-call or elapsed-time allowance."""


class _BudgetedModel(Model):
    def __init__(self, model: Model, *, max_calls: int, deadline: float, state: dict):
        self.model = model
        self.max_calls = max_calls
        self.deadline = deadline
        self.state = state
        self.calls = 0

    def update_config(self, **model_config):
        self.model.update_config(**model_config)

    def get_config(self):
        return self.model.get_config()

    async def structured_output(self, *args, **kwargs):
        raise NotImplementedError("authoring uses the bounded tool loop")
        yield  # pragma: no cover

    async def stream(self, messages, tool_specs=None, system_prompt=None, **kwargs):
        remaining = self.deadline - time.monotonic()
        if self.state["reason"] is not None:
            raise AuthoringBudgetExhausted(self.state["reason"])
        if remaining <= 0:
            raise AuthoringBudgetExhausted("deadline")
        if self.calls >= self.max_calls:
            raise AuthoringBudgetExhausted("model_call_limit")
        self.calls += 1
        try:
            async with asyncio.timeout(remaining):
                async for event in self.model.stream(messages, tool_specs=tool_specs,
                                                      system_prompt=system_prompt, **kwargs):
                    yield event
        except TimeoutError as error:
            raise AuthoringBudgetExhausted("deadline") from error
        except ModelThrottledException as error:
            self.state["last_provider_error"] = provider_error_record(error) if hasattr(error, "kind") else {"kind": "rate_limit"}
            raise
        except BrokerLost as error:
            if time.monotonic() >= self.deadline:
                raise AuthoringBudgetExhausted("deadline") from error
            raise


class GraphAuthor(Protocol):
    def author(self, brief: str, *, approved_bindings: dict, max_rounds: int,
               max_model_calls: int, max_tool_calls: int, deadline_seconds: float,
               evidence_packet: dict, base_template: dict | None = None) -> AuthoringOutcome: ...


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
               max_model_calls: int, max_tool_calls: int, deadline_seconds: float,
               evidence_packet: dict, base_template: dict | None = None) -> AuthoringOutcome:
        started = time.monotonic()
        deadline = started + deadline_seconds
        rounds: list[dict] = []
        calls: list[dict] = []
        seen: dict[str, dict] = {}
        accepted: dict = {}
        state: dict = {"reason": None, "tool_calls": 0, "last_provider_error": None}
        tool_lock = threading.RLock()
        limits = {"max_rounds": max_rounds, "max_model_calls": max_model_calls,
                  "max_tool_calls": max_tool_calls, "deadline_seconds": deadline_seconds}
        vocabulary = authoring_vocabulary(approved_bindings)

        def check_tool_call(draft_digest: str | None = None, action: str | None = None) -> None:
            with tool_lock:
                if state["reason"] is not None:
                    raise AuthoringBudgetExhausted(state["reason"])
                if time.monotonic() >= deadline:
                    state["reason"] = "deadline"
                elif len(rounds) >= max_rounds and not (
                        action in {"validate_draft", "submit_draft"} and
                        draft_digest in accepted):
                    state["reason"] = "round_limit"
                elif state["tool_calls"] >= max_tool_calls:
                    state["reason"] = "tool_call_limit"
                if state["reason"] is not None:
                    raise AuthoringBudgetExhausted(state["reason"])
                state["tool_calls"] += 1

        def evaluate(template_json: str, action: str) -> str:
            try:
                template = json.loads(template_json)
                draft_digest = digest(template)
            except (ValueError, TypeError) as error:
                template = None
                parse_error = error
                draft_digest = digest(template_json)
            else:
                parse_error = None
            with tool_lock:
                check_tool_call(draft_digest, action)
                if draft_digest in seen:
                    result = deepcopy(seen[draft_digest])
                else:
                    if parse_error is not None:
                        result = {"ok": False, "draft_digest": draft_digest,
                                  "errors": _errors(parse_error)}
                    else:
                        try:
                            package = materialize(template, approved_bindings,
                                                  evidence_packet=evidence_packet)
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
            check_tool_call()
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

        budgeted = _BudgetedModel(self.model, max_calls=max_model_calls, deadline=deadline, state=state)
        agent = Agent(model=budgeted, tools=[describe_vocabulary, validate_draft, submit_draft],
                      system_prompt=("Author a bounded factory graph. Call describe_vocabulary first. "
                                     "Use validate_draft and revise from its errors. Submit only a valid "
                                     "template. A submission must pass package validation. "),
                      callback_handler=None)
        prompt = _json({"brief": brief, "base_template": base_template, "max_rounds": max_rounds})
        abort_reason = None
        provider_failure = None
        try:
            agent(prompt)
        except Exception as error:
            seen_errors = set()
            current = error
            while current is not None and id(current) not in seen_errors:
                seen_errors.add(id(current))
                if isinstance(current, AuthoringBudgetExhausted):
                    abort_reason = str(current)
                    break
                if isinstance(current, (SubscriptionAuthRequired, SubscriptionQuotaExhausted,
                                        ModelThrottledException)):
                    provider_failure = (provider_error_record(current) if hasattr(current, "kind")
                                        else {"kind": "rate_limit"})
                current = current.__cause__ or current.__context__
            if abort_reason is None and provider_failure is None:
                raise
        if abort_reason is None and state["reason"] is not None:
            abort_reason = state["reason"]
        if abort_reason is None and provider_failure is None and time.monotonic() >= deadline:
            abort_reason = "deadline"
        config = self.model.get_config()
        model_id = config.get("model_id", type(self.model).__name__) if isinstance(config, dict) else type(self.model).__name__
        scripted = isinstance(self.model, ScriptedAuthoringModel)
        broker = getattr(self.model, "broker", None) is not None
        model_info = {"kind": "scripted" if scripted else "broker" if broker else "strands",
                      "id": model_id,
                      "provider": getattr(self.model, "provider", "scripted" if scripted else "custom"),
                      "billing": getattr(self.model, "billing", "none" if scripted else "unknown"),
                      "live": bool(getattr(self.model, "live", False))}
        selection_seconds = float(getattr(self.model, "selection_seconds", 0.0))
        if abort_reason is not None:
            abort = {"reason": abort_reason, "model_calls": budgeted.calls,
                     "tool_calls": state["tool_calls"],
                     "elapsed_seconds": round(time.monotonic() - started, 3)}
            if state["last_provider_error"] is not None:
                abort["last_provider_error"] = state["last_provider_error"]
            return AuthoringOutcome("aborted", None, None, None, rounds, model_info,
                                    None, calls, abort, limits, None, selection_seconds)
        if provider_failure is not None:
            return AuthoringOutcome("failed", None, None, None, rounds, model_info,
                                    None, calls, None, limits, provider_failure, selection_seconds)
        if "submitted" in accepted:
            template, package, package_digest = accepted["submitted"]
            approval = approve(package, approver="authoring-session",
                               policy={"mode": "auto", "approved_bindings": approved_bindings})
            return AuthoringOutcome("approved", template, package, package_digest,
                                    rounds, model_info, approval, calls, None, limits,
                                    None, selection_seconds)
        return AuthoringOutcome("no_submission", None, None, None, rounds, model_info,
                                None, calls, None, limits, None, selection_seconds)


class AuthoringSession:
    def __init__(self, author: GraphAuthor, *, approved_bindings: dict, evidence_packet: dict, max_rounds: int = 4,
                 max_model_calls: int = 12, max_tool_calls: int = 24,
                 deadline_seconds: float = 600):
        for name, value in (("max_rounds", max_rounds), ("max_model_calls", max_model_calls),
                            ("max_tool_calls", max_tool_calls)):
            if type(value) is not int or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if not isinstance(deadline_seconds, (int, float)) or deadline_seconds <= 0:
            raise ValueError("deadline_seconds must be positive")
        self.author = author
        self.approved_bindings = deepcopy(approved_bindings)
        self.evidence_packet = deepcopy(evidence_packet)
        self.max_rounds = max_rounds
        self.max_model_calls = max_model_calls
        self.max_tool_calls = max_tool_calls
        self.deadline_seconds = deadline_seconds

    def run(self, brief: str, base_template: dict | None = None) -> AuthoringOutcome:
        return self.author.author(brief, approved_bindings=self.approved_bindings,
                                  evidence_packet=self.evidence_packet,
                                  max_rounds=self.max_rounds, max_model_calls=self.max_model_calls,
                                  max_tool_calls=self.max_tool_calls,
                                  deadline_seconds=self.deadline_seconds,
                                  base_template=base_template)


def _scripted_first_draft(base: dict | None) -> dict:
    draft = deepcopy(base) if base else json.loads((Path(__file__).parent.parent /
        "definitions" / "report-template.json").read_text())
    draft["child"]["nodes"]["route_verdict"]["cases"].pop("false", None)
    return draft


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
                    cases = self._draft["child"]["nodes"]["route_verdict"]["cases"]
                    for missing in error["missing_cases"]:
                        if missing not in self._vocabulary["route_values"]["verdict.accepted"]:
                            raise ValueError("feedback requested an undeclared route case")
                        cases[missing] = "repair"
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
    """Select only the requested billing path; default to subscription broker."""
    provider = os.environ.get("EXO_AUTHOR_PROVIDER") or "codex-subscription"
    base_url = os.environ.get("EXO_CODEX_BASE_URL")
    if provider in {"codex-subscription", "synthetic-loopback"}:
        if provider == "codex-subscription" and "EXO_CODEX_BASE_URL" in os.environ:
            return None, "codex-subscription: EXO_CODEX_BASE_URL override is fixture-only"
        if base_url is not None:
            parsed = urlparse(base_url)
            if parsed.scheme not in {"http", "https"} or parsed.hostname not in {
                    "localhost", "127.0.0.1", "::1"} or parsed.username or parsed.password:
                return None, "EXO_CODEX_BASE_URL must be a loopback URL"
        if provider == "synthetic-loopback" and not base_url:
            return None, "synthetic-loopback requires a loopback EXO_CODEX_BASE_URL"
        from model_broker import DEFAULT_HOME, ModelBroker, PiBrokerModel
        if provider == "synthetic-loopback":
            explicit_home = os.environ.get("EXO_MODEL_HOME")
            if not explicit_home or Path(explicit_home).resolve() == DEFAULT_HOME.resolve():
                return None, "synthetic-loopback requires an explicit non-default EXO_MODEL_HOME"
            if not (Path(explicit_home) / "FIXTURE_STORE").is_file():
                return None, "synthetic-loopback requires EXO_MODEL_HOME/FIXTURE_STORE"
        broker = ModelBroker()
        selection_started = time.monotonic()
        if provider == "codex-subscription":
            try:
                health = broker.ensure_started(reason="authoring-selection")
            except (OSError, RuntimeError, ValueError) as error:
                return None, f"codex-subscription: broker unavailable ({type(error).__name__})"
            if not health.get("signed_in"):
                return None, "codex-subscription: not signed in; run node broker/exo-model.mjs login"
        model = PiBrokerModel(broker, model_id=os.environ.get("EXO_AUTHOR_MODEL") or "gpt-6-sol",
                              session_id=str(uuid.uuid4()))
        model.provider = provider
        model.billing = "subscription" if provider == "codex-subscription" else "none"
        model.live = provider == "codex-subscription" and not bool(base_url)
        model.selection_seconds = round(time.monotonic() - selection_started, 3)
        return model, f"{provider} broker available"
    if provider == "anthropic":
        if not os.environ.get("ANTHROPIC_API_KEY"):
            return None, "anthropic: ANTHROPIC_API_KEY absent"
        if not importlib.util.find_spec("anthropic"):
            return None, "anthropic: package unavailable"
        from strands.models import AnthropicModel
        model = AnthropicModel()
    elif provider == "bedrock":
        aws_configured = any(key.startswith("AWS_") and value for key, value in os.environ.items()) or any(
            (Path.home() / ".aws" / name).exists() for name in ("credentials", "config"))
        if not aws_configured:
            return None, "bedrock: AWS credentials/configuration absent"
        if not importlib.util.find_spec("boto3"):
            return None, "bedrock: boto3 package unavailable"
        from strands.models import BedrockModel
        model = BedrockModel()
    elif provider == "openai-api":
        if not os.environ.get("OPENAI_API_KEY"):
            return None, "openai-api: OPENAI_API_KEY absent"
        if not importlib.util.find_spec("openai"):
            return None, "openai-api: package unavailable"
        from strands.models import OpenAIModel
        model = OpenAIModel()
    else:
        return None, f"unknown EXO_AUTHOR_PROVIDER: {provider}"
    model.provider = provider
    model.billing = "api"
    model.live = False
    return model, f"{provider} provider available"
