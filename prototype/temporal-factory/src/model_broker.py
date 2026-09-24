"""Install-wide Pi model broker client and Strands stream adapter.

This module does no work at import time. Credentials stay inside the Node broker.
"""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import threading
import time
import uuid
from typing import Any, AsyncGenerator

from strands.models import Model
from strands.types.exceptions import ModelThrottledException


SIG = "pi-reasoning/v1:"
STOP = {"toolUse": "tool_use", "stop": "end_turn", "length": "max_tokens"}
BROKER_PROGRAM = Path(__file__).resolve().parents[1] / "broker" / "exo-model.mjs"
DEFAULT_HOME = Path.home() / ".exomachina" / "model-broker"
_SPAWNED: list[subprocess.Popen] = []  # keep detached child handles until they exit


class BrokerLost(RuntimeError):
    """The broker connection ended before a terminal response."""


class SubscriptionAuthRequired(RuntimeError):
    """The subscription needs an explicit Exomachina sign-in."""


class SubscriptionQuotaExhausted(RuntimeError):
    """The subscription quota is exhausted; retrying cannot resolve it."""


_ERROR_KINDS = {"reauth_required", "rate_limit", "quota", "config", "provider", "aborted", "broker"}
_ERROR_CODE = re.compile(r"^[a-z0-9_.-]{1,64}$")
_ALLOWED_CODES = {"usage_limit_reached", "usage_not_included", "rate_limit_exceeded",
                  "invalid_request_error", "unsupported_originator", "invalid_grant",
                  "token_expired", "insufficient_quota", "other"}


def _provider_error(reply: dict) -> Exception:
    """Keep only structured, bounded broker fields at the Python boundary."""
    kind = reply.get("kind") if reply.get("kind") in _ERROR_KINDS else "broker"
    status = reply.get("status")
    code = reply.get("code")
    if type(status) is not int or not 100 <= status <= 599:
        status = None
    if code is not None and (not isinstance(code, str) or not _ERROR_CODE.fullmatch(code)
                             or code not in _ALLOWED_CODES):
        code = "other"
    message = f"model broker {kind}"
    if kind == "reauth_required":
        error = SubscriptionAuthRequired(message)
    elif kind == "quota":
        error = SubscriptionQuotaExhausted(message)
    elif kind == "rate_limit":
        error = ModelThrottledException(message)
    else:
        error = RuntimeError(message)
    error.kind = kind
    error.status = status
    error.code = code
    return error


def provider_error_record(error: Exception) -> dict:
    record = {"kind": error.kind}
    if error.status is not None:
        record["status"] = error.status
    if error.code is not None:
        record["code"] = error.code
    return record


class ModelBroker:
    def __init__(self, home: Path | None = None):
        self.home = Path(home if home is not None else os.environ.get("EXO_MODEL_HOME", DEFAULT_HOME)).expanduser().resolve()
        self.socket = self.home / "run" / "broker.sock"

    def health(self) -> dict:
        request_id = str(uuid.uuid4())
        request = {"id": request_id, "op": "health"}
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(2)
            connection.connect(str(self.socket))
            connection.sendall((json.dumps(request) + "\n").encode())
            with connection.makefile("rb") as response:
                line = response.readline(1 << 20)
        if not line:
            raise BrokerLost("model broker closed the health connection")
        reply = json.loads(line)
        if reply.get("id") != request_id or not isinstance(reply.get("health"), dict):
            raise BrokerLost("invalid model broker health response")
        return reply["health"]

    def is_running(self) -> bool:
        try:
            self.health()
            return True
        except (OSError, ValueError, BrokerLost):
            return False

    def _event(self, action: str, reason: str) -> None:
        self.home.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.home.chmod(0o700)
        entry = {"event": action, "reason": reason, "at": time.time(), "caller_pid": os.getpid()}
        flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
        fd = os.open(self.home / "broker-events.jsonl", flags, 0o600)
        with os.fdopen(fd, "a") as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(json.dumps(entry, separators=(",", ":")) + "\n")

    def ensure_started(self, *, reason: str, timeout: float = 30) -> dict:
        """Attach to a healthy broker or start its detached singleton supervisor."""
        if not reason:
            raise ValueError("broker start reason required")
        if "EXO_CODEX_BASE_URL" in os.environ and self.home.resolve() == DEFAULT_HOME.resolve():
            raise ValueError("base-URL override cannot use the default model home")
        try:
            health = self.health()
        except (OSError, ValueError, BrokerLost):
            health = None
        if health is not None:
            self._event("attach", reason)
            return health
        self.home.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.home.chmod(0o700)
        (self.home / "run").mkdir(mode=0o700, exist_ok=True)
        (self.home / "run").chmod(0o700)
        env = {**os.environ, "EXO_MODEL_HOME": str(self.home)}
        process = subprocess.Popen([os.environ.get("EXO_NODE", "node"), str(BROKER_PROGRAM), "serve"],
                                   cwd=BROKER_PROGRAM.parent.parent, env=env, start_new_session=True,
                                   stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                   stderr=subprocess.DEVNULL)
        _SPAWNED.append(process)
        self._event("start", reason)
        deadline = time.monotonic() + timeout
        ready_path = self.home / "run" / "broker-ready.json"
        while time.monotonic() < deadline:
            try:
                ready = json.loads(ready_path.read_text())
                if ready.get("socket") == str(self.socket):
                    return self.health()
            except (OSError, ValueError, BrokerLost):
                pass
            exit_code = process.poll()
            if exit_code is not None and exit_code != 0:
                raise BrokerLost("model broker exited before becoming ready")
            if exit_code == 0:
                try:
                    return self.health()
                except (OSError, ValueError, BrokerLost):
                    pass
            time.sleep(0.05)
        raise BrokerLost("model broker did not become ready")

    def stop(self) -> None:
        """Request shutdown of the ready broker process without removing runtime files."""
        ready = json.loads((self.home / "run" / "broker-ready.json").read_text())
        health = self.health()
        pid = ready.get("pid")
        if type(pid) is not int or pid < 1 or health.get("pid") != pid:
            raise BrokerLost("model broker identity changed; refusing to stop it")
        os.kill(pid, signal.SIGTERM)


class PiBrokerModel(Model):
    def __init__(self, broker: ModelBroker, *, model_id: str, session_id: str,
                 reasoning_effort: str = "low"):
        self.broker = broker
        self.session = session_id
        self.reasoning_effort = reasoning_effort
        self.config = {"model_id": model_id, "context_window_limit": 200000}
        self.provider = "codex-subscription"
        self.billing = "subscription"
        self.live = not bool(os.environ.get("EXO_CODEX_BASE_URL"))

    def update_config(self, **cfg: Any) -> None:
        self.config.update(cfg)

    def get_config(self) -> dict:
        return self.config

    async def structured_output(self, *args: Any, **kwargs: Any):
        raise NotImplementedError("broker structured output is not supported")
        yield  # pragma: no cover

    async def _ensure_started(self) -> None:
        """Keep broker startup off the event loop and abandon its wait on cancellation."""
        loop = asyncio.get_running_loop()
        finished = loop.create_future()

        def complete(error: Exception | None) -> None:
            if not finished.done():
                finished.set_exception(error) if error is not None else finished.set_result(None)

        def start() -> None:
            try:
                self.broker.ensure_started(reason="model-call")
                error = None
            except Exception as caught:
                error = caught
            try:
                loop.call_soon_threadsafe(complete, error)
            except RuntimeError:
                pass  # The timed-out session closed its event loop.

        threading.Thread(target=start, name="exo-model-broker-start", daemon=True).start()
        await finished

    @staticmethod
    def to_pi(messages: list) -> list[dict]:
        out, names = [], {}
        for message in messages:
            if message["role"] == "assistant":
                content, placed = [], []
                for block in message["content"]:
                    if "text" in block:
                        content.append({"type": "text", "text": block["text"]})
                    elif "toolUse" in block:
                        tool = block["toolUse"]
                        names[tool["toolUseId"]] = tool["name"]
                        content.append({"type": "toolCall", "id": tool["toolUseId"],
                                        "name": tool["name"], "arguments": tool["input"]})
                    elif "reasoningContent" in block:
                        signature = block["reasoningContent"].get("reasoningText", {}).get("signature", "")
                        if not signature.startswith(SIG):
                            raise ValueError("cannot replay assistant reasoning without a Pi signature envelope")
                        try:
                            envelope = json.loads(signature[len(SIG):])
                            if (type(envelope["i"]) is not int or envelope["i"] < 0 or
                                    not isinstance(envelope["text"], str) or
                                    not isinstance(envelope["sig"], str) or
                                    envelope["text"] != block["reasoningContent"]["reasoningText"]["text"]):
                                raise ValueError
                        except (KeyError, TypeError, ValueError) as error:
                            raise ValueError("cannot replay invalid Pi reasoning envelope") from error
                        placed.append((envelope["i"], {"type": "thinking", "thinking": envelope["text"],
                                                       "thinkingSignature": envelope["sig"]}))
                    else:
                        raise ValueError("cannot replay unsupported assistant content block")
                for index, block in sorted(placed, key=lambda item: item[0]):
                    if index > len(content):
                        raise ValueError("cannot replay Pi reasoning index")
                    content.insert(index, block)
                stop = "toolUse" if any(block["type"] == "toolCall" for block in content) else "stop"
                out.append({"role": "assistant", "content": content, "stopReason": stop})
            elif message["role"] == "user":
                texts = []
                def flush_texts():
                    if texts:
                        out.append({"role": "user", "content": list(texts)})
                        texts.clear()
                for block in message["content"]:
                    if "text" in block:
                        texts.append({"type": "text", "text": block["text"]})
                    elif "toolResult" in block:
                        flush_texts()
                        result = block["toolResult"]
                        items = []
                        for item in result["content"]:
                            if "text" in item:
                                items.append({"type": "text", "text": item["text"]})
                            else:
                                raise ValueError("cannot replay unsupported tool result content block")
                        out.append({"role": "toolResult", "toolCallId": result["toolUseId"],
                                    "toolName": names.get(result["toolUseId"], ""),
                                    "content": items,
                                    "isError": result.get("status") == "error"})
                    else:
                        raise ValueError("cannot replay unsupported user content block")
                flush_texts()
            else:
                raise ValueError("cannot replay unsupported message role")
        return out

    async def stream(self, messages, tool_specs=None, system_prompt=None, **kwargs) -> AsyncGenerator[dict, None]:
        await self._ensure_started()
        request_id = str(uuid.uuid4())
        request = {"id": request_id, "op": "stream", "model": self.config["model_id"],
                   "session": self.session,
                   "context": {"systemPrompt": system_prompt or "", "messages": self.to_pi(messages),
                               "tools": [{"name": tool["name"], "description": tool.get("description", ""),
                                          "parameters": tool["inputSchema"]["json"]} for tool in (tool_specs or [])]},
                   "options": {"reasoningEffort": self.reasoning_effort}}
        try:
            reader, writer = await asyncio.open_unix_connection(str(self.broker.socket), limit=1 << 24)
        except OSError as error:
            raise BrokerLost("model broker connection failed") from error
        order: list[int] = []
        blocks: dict[int, dict] = {}
        seen: set[int] = set()

        def drain() -> list[dict]:
            ready = []
            while order:
                index = order[0]
                block = blocks[index]
                ready.extend(block["events"])
                block["events"] = []
                if not block["closed"]:
                    break
                order.pop(0)
                del blocks[index]
            return ready

        try:
            writer.write((json.dumps(request) + "\n").encode())
            await writer.drain()
            yield {"messageStart": {"role": "assistant"}}
            while True:
                line = await reader.readline()
                if not line:
                    raise BrokerLost("model broker connection closed before a terminal event")
                reply = json.loads(line)
                if reply.get("id") != request_id:
                    raise BrokerLost("model broker response ID mismatch")
                if "error" in reply:
                    raise _provider_error(reply["error"])
                if "done" in reply:
                    if order:
                        raise RuntimeError("model broker ended with an unfinished content block")
                    final = reply["done"]
                    for index, content in enumerate(final["content"]):
                        if content["type"] == "thinking" and content.get("thinkingSignature"):
                            envelope = {"i": index, "text": content.get("thinking", ""),
                                        "sig": content["thinkingSignature"]}
                            yield {"contentBlockStart": {"start": {}}}
                            yield {"contentBlockDelta": {"delta": {"reasoningContent": {"text": envelope["text"]}}}}
                            yield {"contentBlockDelta": {"delta": {"reasoningContent": {"signature": SIG + json.dumps(envelope)}}}}
                            yield {"contentBlockStop": {}}
                    yield {"messageStop": {"stopReason": STOP.get(final["stopReason"], "end_turn")}}
                    usage = final.get("usage", {})
                    yield {"metadata": {"usage": {"inputTokens": usage.get("input", 0),
                                                  "outputTokens": usage.get("output", 0),
                                                  "totalTokens": usage.get("totalTokens", 0)},
                                        "metrics": {"latencyMs": 0}}}
                    return
                event = reply.get("ev")
                if not isinstance(event, dict):
                    raise RuntimeError("model broker response omitted an event")
                kind = event.get("type")
                if kind not in {"text_start", "text_delta", "text_end", "toolcall_start", "toolcall_delta", "toolcall_end"}:
                    continue
                index = event.get("contentIndex")
                if type(index) is not int or index < 0:
                    raise RuntimeError("model broker omitted a content index")
                if kind.endswith("_start"):
                    if index in seen:
                        raise RuntimeError("model broker reused a content index")
                    if seen and index <= max(seen):
                        raise RuntimeError("model broker started content indices out of order")
                    seen.add(index)
                    start = ({"toolUse": {"toolUseId": event["toolCall"]["id"],
                                          "name": event["toolCall"]["name"]}}
                             if kind == "toolcall_start" else {})
                    blocks[index] = {"kind": kind.split("_")[0],
                                     "events": [{"contentBlockStart": {"start": start}}], "closed": False}
                    order.append(index)
                else:
                    block = blocks.get(index)
                    if block is None or block["closed"] or block["kind"] != kind.split("_")[0]:
                        raise RuntimeError("model broker content index did not match an open block")
                    if kind == "text_delta":
                        block["events"].append({"contentBlockDelta": {"delta": {"text": event["delta"]}}})
                    elif kind == "toolcall_delta":
                        block["events"].append({"contentBlockDelta": {"delta": {"toolUse": {"input": event["delta"]}}}})
                    else:
                        block["events"].append({"contentBlockStop": {}})
                        block["closed"] = True
                for chunk in drain():
                    yield chunk
        except (asyncio.CancelledError, GeneratorExit):
            try:
                writer.write((json.dumps({"id": request_id, "op": "cancel"}) + "\n").encode())
                await asyncio.shield(writer.drain())
            except (OSError, RuntimeError):
                pass
            raise
        except OSError as error:
            raise BrokerLost("model broker connection lost before a terminal event") from error
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass
