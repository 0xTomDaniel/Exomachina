"""Bounded, semantic Strands tool loop for one factory A2A text message.

Only the Director owns command identifiers, the authenticated principal and the
Task binding. This module never exposes those values to the model.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import time

from strands import Agent, tool
from strands.models import Model

from model_broker import ModelBroker, PiBrokerModel


LIMITS = {"max_model_calls": 4, "max_tool_calls": 4, "deadline_seconds": 90}


class DirectorBudgetExhausted(RuntimeError):
    pass


class BudgetedModel(Model):
    def __init__(self, model: Model, deadline: float, max_calls: int):
        self.model, self.deadline, self.max_calls, self.calls = model, deadline, max_calls, 0

    def update_config(self, **config):
        self.model.update_config(**config)

    def get_config(self):
        return self.model.get_config()

    async def structured_output(self, *args, **kwargs):
        raise NotImplementedError
        yield  # pragma: no cover

    async def stream(self, messages, tool_specs=None, system_prompt=None, **kwargs):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise DirectorBudgetExhausted("deadline")
        if self.calls >= self.max_calls:
            raise DirectorBudgetExhausted("model_call_limit")
        self.calls += 1
        try:
            async with asyncio.timeout(remaining):
                async for event in self.model.stream(messages, tool_specs=tool_specs,
                                                     system_prompt=system_prompt, **kwargs):
                    yield event
        except TimeoutError as error:
            raise DirectorBudgetExhausted("deadline") from error


def selected_model(config: dict, *, session_id: str) -> tuple[Model, str]:
    selection = config.get("director_model", {"provider": "fixture"})
    provider = selection.get("provider", "fixture")
    if provider not in {"synthetic-loopback", "codex-subscription"}:
        raise ValueError("text briefs require a broker-backed director_model")
    if provider == "codex-subscription":
        import os
        if "EXO_MODEL_HOME" in os.environ or "EXO_CODEX_BASE_URL" in os.environ:
            raise ValueError("live Director requires the default model home and endpoint")
    model = PiBrokerModel(ModelBroker(), model_id=selection.get("model", "gpt-6-sol"),
                          session_id=session_id)
    return model, "synthetic" if provider == "synthetic-loopback" else "live"


class DirectorTurn:
    def __init__(self, director, task_id: str, context_id: str, message_id: str,
                 model_kind: str, *, limits: dict | None = None):
        self.director = director
        self.task_id, self.context_id, self.message_id = task_id, context_id, message_id
        self.model_kind = model_kind
        self.limits = {**LIMITS, **(limits or {})}
        self.deadline = time.monotonic() + self.limits["deadline_seconds"]
        self.calls: list[dict] = []
        self.accepted: list[str] = []

    def action_id(self, operation: str) -> str:
        raw = json.dumps([self.task_id, operation, self.message_id], separators=(",", ":"))
        return "director:" + hashlib.sha256(raw.encode()).hexdigest()

    def _record(self, name: str, arguments: dict, result: dict) -> dict:
        row = {"tool": name, "arguments": arguments, "result": result,
               "accepted": bool(result.get("ok"))}
        self.calls.append(row)
        with self.director.connect() as db:
            db.execute("INSERT INTO director_tool_calls (task_id, message_id, model_kind, tool, "
                       "arguments_json, result_json, accepted, created_at) VALUES (?,?,?,?,?,?,?,?)",
                       (self.task_id, self.message_id, self.model_kind, name,
                        json.dumps(arguments, sort_keys=True), json.dumps(result, sort_keys=True),
                        int(row["accepted"]), time.time()))
        return result

    def call(self, name: str, arguments: dict) -> dict:
        """Shared model and fixture-injection path; never lets a rejection escape as success."""
        if time.monotonic() >= self.deadline:
            return self._record(name, arguments, {"ok": False, "error": {"code": "budget", "message": "deadline"}})
        if len(self.calls) >= self.limits["max_tool_calls"]:
            return self._record(name, arguments, {"ok": False, "error": {"code": "budget", "message": "tool_call_limit"}})
        schemas = {"start_research": {"question"},
                   "inspect_run": set(),
                   "decide_wait": {"action", "revision", "sha256", "rationale"}}
        if name not in schemas or not isinstance(arguments, dict) or set(arguments) != schemas[name]:
            return self._record(name, arguments, {"ok": False, "error": {"code": "invalid_arguments", "message": "tool arguments do not match the semantic contract"}})
        try:
            if name == "start_research":
                command = {"op": "start", "action_id": self.action_id("start"),
                           "inputs": {"question": arguments["question"]}}
                self.director.perform(command, self.task_id, self.context_id)
                value = {"ok": True, "accepted_command": "start"}
            elif name == "inspect_run":
                self.director.perform({"op": "inspect"}, self.task_id, self.context_id)
                value = {"ok": True, "run": self.director.inspect_bound_run(self.task_id)}
            else:
                if arguments["action"] != "abort":
                    raise ValueError("only abort is supported at the Director wait")
                command = {"op": "abort", "action_id": self.action_id("abort"),
                           "revision": arguments["revision"], "sha256": arguments["sha256"]}
                self.director.perform(command, self.task_id, self.context_id)
                value = {"ok": True, "accepted_command": "abort"}
            self.accepted.append(name)
        except Exception as error:
            # Only bounded, local rejection text is returned; provider errors are classified.
            from harness_server import Rejected
            code = "rejected" if isinstance(error, (Rejected, ValueError)) else "execution_error"
            message = str(error) if code == "rejected" else type(error).__name__
            value = {"ok": False, "error": {"code": code, "message": message}}
        return self._record(name, arguments, value)

    async def run(self, brief: str, model: Model) -> dict:
        started = time.monotonic()
        @tool
        def start_research(question: str) -> str:
            """Start verified research for this Task.

            Args:
                question: The research question from the caller's brief.
            """
            return json.dumps(self.call("start_research", {"question": question}), sort_keys=True)

        @tool
        def inspect_run() -> str:
            """Inspect this Task's bound run, including any Director wait and current revision."""
            return json.dumps(self.call("inspect_run", {}), sort_keys=True)

        @tool
        def decide_wait(action: str, revision: str, sha256: str, rationale: str) -> str:
            """Answer this Task's Director wait using the current inspected revision.

            Args:
                action: Only abort is supported.
                revision: Current revision returned by inspect_run.
                sha256: Current digest returned by inspect_run.
                rationale: Brief reason for the decision.
            """
            return json.dumps(self.call("decide_wait", {"action": action, "revision": revision,
                "sha256": sha256, "rationale": rationale}), sort_keys=True)

        budgeted = BudgetedModel(model, self.deadline, self.limits["max_model_calls"])
        agent = Agent(name="Factory Director", model=budgeted,
                      tools=[start_research, inspect_run, decide_wait],
                      system_prompt=("You are the Director for one verified-research Task. "
                                     "For a new research brief, call start_research once with the caller's "
                                     "question. At a Director wait, the pinned graph permits only abort "
                                     "at repair_exhausted. First call inspect_run, then decide using its "
                                     "current revision and sha256. "
                                     "Tools return structured errors; do not claim a rejected action succeeded."),
                      callback_handler=None)
        failure = None
        try:
            await agent.invoke_async(brief)
        except Exception as error:
            failure = "budget" if any(isinstance(e, DirectorBudgetExhausted) for e in
                (error, error.__cause__, error.__context__)) else type(error).__name__
        result = {"model_calls": budgeted.calls, "tool_calls": len(self.calls),
                  "elapsed_seconds": round(time.monotonic() - started, 3),
                  "accepted": self.accepted, "limits": self.limits, "failure": failure}
        with self.director.connect() as db:
            db.execute("INSERT INTO director_turns VALUES (?,?,?,?,?,?,?)",
                       (self.task_id, self.message_id, self.model_kind, budgeted.calls,
                        len(self.calls), json.dumps(result, sort_keys=True), time.time()))
        return result
