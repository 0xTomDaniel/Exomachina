"""Authenticated HTTP routes for the bounded incident Operations interface.

The Runtime owns principal resolution, factory identity, and the Operations
instance. This module delegates to that instance and never discovers factories,
opens ledgers, accepts caller identities, or executes recovery actions.
"""
from __future__ import annotations

import inspect
import json
from typing import Any, Callable

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

from operations import (
    FactoryOperations,
    OperationsConflict,
    OperationsForbidden,
    OperationsNotFound,
    OperationsPolicyError,
    OperationsStale,
)


PrincipalResolver = Callable[[Request], object]
_REPORT_FIELDS = frozenset({
    "run_id", "subject_kind", "subject_id", "failure_class", "generation", "evidence_refs"
})


async def _resolve_principal(authenticate: PrincipalResolver, request: Request) -> object | None:
    try:
        principal = authenticate(request)
        if inspect.isawaitable(principal):
            principal = await principal
        return principal
    except Exception:
        return None


def _error(error: Exception) -> JSONResponse:
    if isinstance(error, OperationsForbidden):
        return JSONResponse({"error": "not_authorized"}, status_code=403)
    if isinstance(error, OperationsNotFound):
        return JSONResponse({"error": "not_found"}, status_code=404)
    if isinstance(error, (OperationsConflict, OperationsStale)):
        return JSONResponse({"error": "conflict", "message": str(error)}, status_code=409)
    if isinstance(error, OperationsPolicyError):
        return JSONResponse({"error": "invalid_or_disallowed_request", "message": str(error)},
                            status_code=422)
    return JSONResponse({"error": "operations_unavailable"}, status_code=503)


async def _body(request: Request, expected: frozenset[str]) -> dict[str, Any]:
    try:
        value = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as error:
        raise ValueError("request body must be JSON") from error
    if not isinstance(value, dict) or set(value) != expected:
        raise ValueError("request fields do not match the Operations contract")
    return value


async def _call(operations: FactoryOperations, principal: object, method: str,
                *args: Any, **kwargs: Any) -> Any:
    callback = getattr(operations, method)
    return await run_in_threadpool(callback, principal, *args, **kwargs)


def install_operations_routes(app, operations: FactoryOperations,
                              authenticate: PrincipalResolver, *,
                              max_list_limit: int) -> None:
    """Mount incident routes using an explicit Runtime principal resolver/limit.

    ``authenticate(request)`` returns the already authenticated server-side
    principal, synchronously or asynchronously. The request body and URL are
    never consulted for identity. ``max_list_limit`` is a deployment-supplied
    bound (1..256); callers also provide an explicit ``limit`` on each list
    request. Recovery is advertised as unavailable because this Runtime has no
    authorized per-run owner action to delegate.
    """
    if not callable(authenticate):
        raise TypeError("authenticate must be the Runtime principal resolver")
    if type(max_list_limit) is not int or not 1 <= max_list_limit <= 256:
        raise ValueError("max_list_limit must be explicitly set from 1 to 256")
    if not isinstance(getattr(operations, "factory_id", None), str):
        raise ValueError("Operations must be bound to an explicit factory identity")

    async def principal_or_response(request: Request):
        principal = await _resolve_principal(authenticate, request)
        if principal is None or principal is False:
            return None, JSONResponse({"error": "authentication_required"}, status_code=401)
        return principal, None

    @app.get("/operations/capabilities")
    async def capabilities(request: Request):
        principal, rejected = await principal_or_response(request)
        if rejected is not None:
            return rejected
        try:
            await _call(operations, principal, "list_incidents", limit=1)
            return {
                "factory_id": operations.factory_id,
                "incidents": {"available": True,
                              "methods": ["report", "get", "list", "acknowledge", "claim", "escalate"]},
                "recovery": {"available": False,
                             "reason": "no_runtime_owned_per_run_recovery_action"},
            }
        except Exception as error:
            return _error(error)

    @app.post("/incidents")
    async def report_incident(request: Request):
        principal, rejected = await principal_or_response(request)
        if rejected is not None:
            return rejected
        try:
            body = await _body(request, _REPORT_FIELDS)
            if (not isinstance(body["run_id"], str)
                    or not isinstance(body["subject_kind"], str)
                    or not isinstance(body["subject_id"], str)
                    or not isinstance(body["failure_class"], str)
                    or type(body["generation"]) is not int
                    or not isinstance(body["evidence_refs"], list)):
                raise ValueError("request fields have invalid types")
            return await _call(
                operations, principal, "report_incident", run_id=body["run_id"],
                subject_kind=body["subject_kind"], subject_id=body["subject_id"],
                failure_class=body["failure_class"], generation=body["generation"],
                evidence_refs=body["evidence_refs"])
        except ValueError as error:
            return JSONResponse({"error": "invalid_request", "message": str(error)}, status_code=400)
        except Exception as error:
            return _error(error)

    @app.get("/incidents")
    async def list_incidents(request: Request):
        principal, rejected = await principal_or_response(request)
        if rejected is not None:
            return rejected
        query = request.query_params
        limit_values = query.getlist("limit")
        run_values = query.getlist("run_id")
        if (set(query.keys()) - {"run_id", "limit"}
                or len(limit_values) != 1 or len(run_values) > 1):
            return JSONResponse({"error": "invalid_request",
                                 "message": "run_id is optional and limit is required"}, status_code=400)
        raw_limit = limit_values[0]
        if (len(raw_limit) > 3 or not raw_limit.isascii() or not raw_limit.isdecimal()
                or not 1 <= int(raw_limit) <= max_list_limit):
            return JSONResponse({"error": "invalid_request", "message": "limit is outside configured bounds"},
                                status_code=400)
        run_id = run_values[0] if run_values else None
        if run_id == "":
            return JSONResponse({"error": "invalid_request", "message": "run_id cannot be empty"},
                                status_code=400)
        try:
            result = await _call(operations, principal, "list_incidents",
                                 run_id=run_id, limit=int(raw_limit))
            return {"incidents": result}
        except Exception as error:
            return _error(error)

    @app.get("/incidents/{incident_id}")
    async def get_incident(incident_id: str, request: Request):
        principal, rejected = await principal_or_response(request)
        if rejected is not None:
            return rejected
        try:
            return await _call(operations, principal, "get_incident", incident_id)
        except Exception as error:
            return _error(error)

    @app.post("/incidents/{incident_id}/acknowledge")
    async def acknowledge_incident(incident_id: str, request: Request):
        principal, rejected = await principal_or_response(request)
        if rejected is not None:
            return rejected
        try:
            body = await _body(request, frozenset({"expected_version"}))
            if type(body["expected_version"]) is not int:
                raise ValueError("expected_version must be an integer")
            return await _call(operations, principal, "acknowledge_incident", incident_id,
                               expected_version=body["expected_version"])
        except ValueError as error:
            return JSONResponse({"error": "invalid_request", "message": str(error)}, status_code=400)
        except Exception as error:
            return _error(error)

    @app.post("/incidents/{incident_id}/claim")
    async def claim_incident(incident_id: str, request: Request):
        principal, rejected = await principal_or_response(request)
        if rejected is not None:
            return rejected
        try:
            body = await _body(request, frozenset({"expected_version", "takeover"}))
            if type(body["expected_version"]) is not int or type(body["takeover"]) is not bool:
                raise ValueError("expected_version and takeover have invalid types")
            return await _call(operations, principal, "claim_incident", incident_id,
                               expected_version=body["expected_version"],
                               takeover=body["takeover"])
        except ValueError as error:
            return JSONResponse({"error": "invalid_request", "message": str(error)}, status_code=400)
        except Exception as error:
            return _error(error)

    @app.post("/incidents/{incident_id}/escalate")
    async def escalate_incident(incident_id: str, request: Request):
        principal, rejected = await principal_or_response(request)
        if rejected is not None:
            return rejected
        try:
            body = await _body(request, frozenset({"expected_version", "reason", "evidence_refs"}))
            if (type(body["expected_version"]) is not int
                    or not isinstance(body["reason"], str)
                    or not isinstance(body["evidence_refs"], list)):
                raise ValueError("escalation fields have invalid types")
            return await _call(operations, principal, "escalate_incident", incident_id,
                               expected_version=body["expected_version"],
                               reason=body["reason"], evidence_refs=body["evidence_refs"])
        except ValueError as error:
            return JSONResponse({"error": "invalid_request", "message": str(error)}, status_code=400)
        except Exception as error:
            return _error(error)
