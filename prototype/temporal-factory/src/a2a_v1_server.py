"""A2A v1.0 server Adapter over a2a-sdk 1.x for every Exomachina A2A service.

Each service builds its Agent Card and FastAPI app here so that all of them:

- advertise exactly one v1.0 JSON-RPC ``supportedInterfaces`` entry and no 0.3
  fields;
- serve only the v1 JSON-RPC methods (the SDK's 0.3 compatibility adapter is
  never enabled, so ``message/send`` is ``MethodNotFound``);
- require the ``A2A-Version: 1.0`` header (the SDK reads a missing header as
  0.3 and answers ``VersionNotSupportedError``);
- reject 0.3 ``kind`` discriminators and unknown roles that protobuf's lenient
  JSON parse would otherwise drop silently;
- answer ``ExtensionSupportRequiredError`` when a send does not activate every
  Agent Card extension declared ``required`` (``A2A-Extensions`` header).

``LegacyRequestHandler`` is the SDK's v1 handler that keeps the
TaskStore-authoritative event flow these services were built on (the store's
``save`` only acknowledges Tasks the service ledger already committed).
"""
from __future__ import annotations

import json
from uuid import uuid4

from google.protobuf.json_format import MessageToDict
from packaging.version import InvalidVersion, Version
from starlette.requests import Request
from starlette.responses import JSONResponse

from a2a.extensions.common import get_requested_extensions
from a2a.helpers.proto_helpers import new_data_part, new_text_part
from a2a.server.request_handlers import LegacyRequestHandler
from a2a.server.request_handlers.response_helpers import (agent_card_to_dict,
                                                          build_error_response)
from a2a.server.routes import create_agent_card_routes
from a2a.server.routes.jsonrpc_dispatcher import JsonRpcDispatcher
from a2a.server.tasks import TaskStore
from a2a.types import (AgentCard, AgentInterface, APIKeySecurityScheme,
                       ExtensionSupportRequiredError, HTTPAuthSecurityScheme,
                       InvalidParamsError, Message, Part, Role, SecurityRequirement,
                       SecurityScheme, StringList, TaskState, UnsupportedOperationError)

import a2a_v1 as wire


__all__ = ["LegacyRequestHandler", "ProjectionTaskStore", "agent_message", "bearer_security",
           "build_app", "card_dict", "card_pin_projection", "cookie_security", "data_part",
           "interfaces", "part_content", "part_data", "task_state", "text_part"]


def interfaces(url: str) -> list[AgentInterface]:
    """The only interface an Exomachina Agent Card advertises: v1.0 JSON-RPC."""
    return [AgentInterface(url=url, protocol_binding=wire.JSONRPC_BINDING,
                           protocol_version=wire.PROTOCOL_VERSION)]


def bearer_security(name: str = "fixtureBearer") -> dict:
    return {"security_schemes": {name: SecurityScheme(
                http_auth_security_scheme=HTTPAuthSecurityScheme(scheme="bearer"))},
            "security_requirements": [SecurityRequirement(schemes={name: StringList(list=[])})]}


def cookie_security(name: str, cookie: str) -> dict:
    return {"security_schemes": {name: SecurityScheme(
                api_key_security_scheme=APIKeySecurityScheme(location="cookie", name=cookie))},
            "security_requirements": [SecurityRequirement(schemes={name: StringList(list=[])})]}


def card_dict(card: AgentCard) -> dict:
    return agent_card_to_dict(card)


def card_pin_projection(card: AgentCard) -> dict:
    """Endpoint-free public Agent Card JSON used for digest pins."""
    return wire.card_without_endpoint(card_dict(card))


def task_state(name: str) -> int:
    """Adapter boundary: version-neutral state name -> v1 ``TaskState``."""
    return TaskState.Value(wire.wire_state(name))


def data_part(value) -> Part:
    return new_data_part(value, wire.JSON_MEDIA_TYPE)


def text_part(text: str, media_type: str = "text/plain") -> Part:
    return new_text_part(text, media_type)


def part_content(part: Part) -> str | None:
    return part.WhichOneof("content")


def part_data(part: Part):
    if part_content(part) != "data":
        raise ValueError("expected an A2A v1 data Part")
    return wire.normalize_numbers(MessageToDict(part.data))


def agent_message(parts: list[Part]) -> Message:
    return Message(message_id=str(uuid4()), role=Role.ROLE_AGENT, parts=parts)


class ProjectionTaskStore(TaskStore):
    """Base for ledger-backed stores: Tasks are projections, never listed or deleted."""

    async def list(self, params, context=None):
        raise UnsupportedOperationError(message="ListTasks is not offered by this agent")

    async def delete(self, task_id, context=None):
        raise UnsupportedOperationError(message="Task deletion is not offered by this agent")


def _v1_header(request: Request) -> bool:
    value = request.headers.get(wire.VERSION_HEADER)
    if not value:
        return False
    try:
        return Version(value).major == 1
    except InvalidVersion:
        return False


def _request_id(body):
    value = body.get("id") if isinstance(body, dict) else None
    return value if isinstance(value, str | int) and not isinstance(value, bool) else None


def build_app(card: AgentCard, request_handler, *, app=None):
    """Mount the v1 Agent Card and the strict v1 JSON-RPC endpoint at ``/``."""
    from fastapi import FastAPI

    app = app or FastAPI()
    dispatcher = JsonRpcDispatcher(request_handler=request_handler, enable_v0_3_compat=False)
    required = set(wire.required_extensions(card_dict(card)))

    async def jsonrpc(request: Request):
        if _v1_header(request):
            try:
                body = await request.json()
            except (ValueError, UnicodeDecodeError):
                body = None
            violation = wire.request_violation(body)
            if violation:
                return JSONResponse(build_error_response(
                    _request_id(body), InvalidParamsError(message=violation)))
            if required and isinstance(body, dict) and body.get("method") in wire.SEND_METHODS:
                activated = get_requested_extensions(
                    request.headers.getlist(wire.EXTENSIONS_HEADER))
                missing = sorted(required - activated)
                if missing:
                    return JSONResponse(build_error_response(
                        _request_id(body), ExtensionSupportRequiredError(
                            message="required A2A extension not activated: "
                            + ", ".join(missing))))
        response = await dispatcher.handle_requests(request)
        if isinstance(response, JSONResponse):
            return JSONResponse(wire.normalize_numbers(json.loads(response.body)),
                                status_code=response.status_code)
        return response

    for route in create_agent_card_routes(card):
        app.router.routes.append(route)
    app.add_route("/", jsonrpc, methods=["POST"])
    return app
