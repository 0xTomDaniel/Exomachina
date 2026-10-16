"""FastAPI WebSocket Adapter for FactoryObservation.

Authentication is supplied by the runtime as a server-side principal resolver.
The client protocol has no bearer-token field and accepts no token in query
parameters or event data.
"""
from __future__ import annotations

import asyncio
import inspect
import json
import re
from typing import Any, Awaitable, Callable

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from observation import (CursorExpired, FactoryObservation, InvalidCursor,
                         ObservationError, ObservationForbidden, ObservationNotFound,
                         SourceContractError)


PrincipalResolver = Callable[[WebSocket], object | None | Awaitable[object | None]]
_FACTORY_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}$")
_OPAQUE_CURSOR = re.compile(r"^c1\.[A-Za-z0-9_-]{16}\.[A-Za-z0-9_-]{40,48}$")
_MAX_MESSAGE_BYTES = 16_384
_POLL_SECONDS = 0.2
_SEND_TIMEOUT_SECONDS = 3.0


class _SlowViewer(Exception):
    pass


async def _resolve_principal(authenticate: PrincipalResolver, websocket: WebSocket) -> object | None:
    try:
        principal = authenticate(websocket)
        if inspect.isawaitable(principal):
            principal = await principal
        return principal
    except Exception:
        return None


async def _receive_json(websocket: WebSocket) -> Any:
    raw = await websocket.receive_text()
    if len(raw.encode("utf-8")) > _MAX_MESSAGE_BYTES:
        raise ValueError("message too large")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("message must be an object")
    return value


def _subscribe(value: dict[str, Any]) -> tuple[str, str | None, str | None]:
    if value.get("op") != "subscribe" or set(value) - {
            "op", "factory_id", "run_id", "after_cursor"}:
        raise ValueError("invalid subscription")
    factory_id = value.get("factory_id")
    if not isinstance(factory_id, str) or not _FACTORY_ID.fullmatch(factory_id):
        raise ValueError("invalid subscription")
    run_id = value.get("run_id")
    if run_id is not None and (not isinstance(run_id, str) or not _FACTORY_ID.fullmatch(run_id)):
        raise ValueError("invalid subscription")
    cursor = value.get("after_cursor")
    if cursor is not None and (not isinstance(cursor, str) or not _OPAQUE_CURSOR.fullmatch(cursor)):
        raise ValueError("invalid subscription")
    return factory_id, run_id, cursor


def _command(value: dict[str, Any], factory_id: str) -> dict[str, Any]:
    if value.get("op") != "command" or set(value) - {
            "op", "factory_id", "command_id", "task_id", "context_id", "action",
            "expected_state", "expected_revision", "expected_sha256"}:
        raise ValueError("invalid command")
    if value.get("factory_id") != factory_id:
        raise ValueError("invalid command")
    return {key: item for key, item in value.items() if key not in {"op", "factory_id"}}


async def _send(websocket: WebSocket, message: dict[str, Any]) -> None:
    try:
        await asyncio.wait_for(websocket.send_json(message), timeout=_SEND_TIMEOUT_SECONDS)
    except asyncio.TimeoutError as error:
        # Try to tell the viewer why this connection is ending. If even this
        # bounded send cannot complete, close still makes the loss explicit.
        try:
            await asyncio.wait_for(websocket.send_json({
                "op": "resync_required", "reason": "slow_consumer",
                "message": "reconnect from a fresh snapshot",
            }), timeout=0.2)
        except Exception:
            pass
        try:
            await websocket.close(code=1013, reason="slow consumer; resync required")
        except Exception:
            pass
        raise _SlowViewer from error


async def _call_projection(fn, *args, **kwargs):
    return await asyncio.to_thread(fn, *args, **kwargs)


async def _send_snapshot(websocket: WebSocket, projection: FactoryObservation,
                         principal: object, factory_id: str, run_id: str | None) -> str:
    snapshot = await _call_projection(projection.snapshot, principal, factory_id, run_id)
    await _send(websocket, {"op": "snapshot", "snapshot": snapshot})
    return snapshot["cursor"]


async def _resync(websocket: WebSocket, projection: FactoryObservation,
                  principal: object, factory_id: str, run_id: str | None,
                  reason: str, *, minimum_cursor: str | None = None,
                  latest_cursor: str | None = None) -> str:
    frame: dict[str, Any] = {"op": "resync_required", "reason": reason}
    if minimum_cursor is not None:
        frame["minimum_cursor"] = minimum_cursor
    if latest_cursor is not None:
        frame["latest_cursor"] = latest_cursor
    await _send(websocket, frame)
    return await _send_snapshot(websocket, projection, principal, factory_id, run_id)


async def _serve_subscription(websocket: WebSocket, projection: FactoryObservation,
                              principal: object, factory_id: str, run_id: str | None,
                              after_cursor: str | None) -> None:
    catchup_pending = False
    if after_cursor is None:
        cursor = await _send_snapshot(websocket, projection, principal, factory_id, run_id)
    else:
        try:
            # This validates the cursor against retained data after refreshing
            # authoritative source records; returned events are sent below.
            page = await _call_projection(projection.events_after, principal, factory_id,
                                          after_cursor, run_id=run_id)
        except CursorExpired as error:
            cursor = await _resync(websocket, projection, principal, factory_id, run_id,
                                   "retention_expired", minimum_cursor=error.minimum_cursor,
                                   latest_cursor=error.latest_cursor)
        else:
            cursor = after_cursor
            catchup_pending = True
            await _send(websocket, {"op": "resumed", "after_cursor": after_cursor,
                                    "continuation_cursor": after_cursor})
            # The first page was read before the acknowledgement. Send it now;
            # events appended after its captured read are fetched on the next loop.
            cursor = await _send_page(websocket, page, cursor,
                                      acknowledge_catchup=catchup_pending)
            if not page["has_more"]:
                catchup_pending = False

    receive_task = asyncio.create_task(_receive_json(websocket))
    try:
        while True:
            if receive_task.done():
                try:
                    message = receive_task.result()
                except (WebSocketDisconnect, asyncio.CancelledError):
                    return
                except Exception:
                    await _send(websocket, {"op": "error", "code": "invalid_message",
                                            "message": "invalid client message"})
                    await websocket.close(code=4400, reason="invalid client message")
                    return
                if message.get("op") != "command":
                    await _send(websocket, {"op": "error", "code": "unsupported_operation",
                                            "message": "only command messages are accepted after subscribe"})
                    await websocket.close(code=4400, reason="unsupported operation")
                    return
                try:
                    command = _command(message, factory_id)
                    receipt = await _call_projection(projection.submit_command, principal,
                                                     factory_id, command)
                except (ObservationForbidden, ObservationNotFound):
                    await _send(websocket, {"op": "error", "code": "command_unavailable",
                                            "message": "command is unavailable"})
                except (ObservationError, ValueError):
                    await _send(websocket, {"op": "error", "code": "invalid_command",
                                            "message": "command was rejected"})
                except Exception:
                    await _send(websocket, {"op": "error", "code": "command_unavailable",
                                            "message": "command is unavailable"})
                else:
                    await _send(websocket, {"op": "command_ack",
                                            "command_id": receipt["command_id"],
                                            "lifecycle": "received"})
                    prior = receipt.get("prior_outcome")
                    if prior is not None:
                        try:
                            prior_event = await _call_projection(
                                projection.prior_command_outcome_event, principal, factory_id,
                                receipt["command_id"], prior["lifecycle"], cursor)
                        except CursorExpired as error:
                            cursor = await _resync(
                                websocket, projection, principal, factory_id, run_id,
                                "retention_expired", minimum_cursor=error.minimum_cursor,
                                latest_cursor=error.latest_cursor)
                        except ObservationError:
                            await _send(websocket, {
                                "op": "error", "code": "observation_unavailable",
                                "message": "prior command outcome is unavailable"})
                        else:
                            if (prior_event is not None
                                    and (run_id is None or prior_event["data"].get("run_id") == run_id)):
                                # Re-emit the original CloudEvent at the current
                                # checkpoint. Its stable source/id makes this an
                                # idempotent replay, not a new transition.
                                await _send(websocket, {
                                    "op": "event", "cursor": cursor, "event": prior_event})
                receive_task = asyncio.create_task(_receive_json(websocket))

            try:
                page = await _call_projection(projection.events_after, principal, factory_id,
                                              cursor, run_id=run_id)
            except CursorExpired as error:
                cursor = await _resync(websocket, projection, principal, factory_id, run_id,
                                       "retention_expired", minimum_cursor=error.minimum_cursor,
                                       latest_cursor=error.latest_cursor)
                continue
            cursor = await _send_page(websocket, page, cursor,
                                      acknowledge_catchup=catchup_pending)
            if catchup_pending and not page["has_more"]:
                catchup_pending = False
            if page["has_more"]:
                continue
            try:
                await asyncio.wait_for(asyncio.shield(receive_task), timeout=_POLL_SECONDS)
            except asyncio.TimeoutError:
                pass
            except WebSocketDisconnect:
                return
            except Exception:
                # Let the next loop consume the completed task and return a
                # bounded protocol error for malformed client input.
                pass
    finally:
        if receive_task.done():
            try:
                receive_task.result()
            except BaseException:
                pass
        else:
            receive_task.cancel()
            try:
                await receive_task
            except BaseException:
                pass


async def _send_page(websocket: WebSocket, page: dict[str, Any], cursor: str, *,
                     acknowledge_catchup: bool = False) -> str:
    for row in page["events"]:
        await _send(websocket, {"op": "event", "cursor": row["cursor"], "event": row["event"]})
        cursor = row["cursor"]
    continuation = page["continuation_cursor"]
    skipped_events = page.get("checkpoint_advanced") and continuation != cursor
    completed_catchup = acknowledge_catchup and not page["has_more"]
    if skipped_events or completed_catchup:
        await _send(websocket, {"op": "checkpoint", "cursor": continuation})
        cursor = continuation
    return cursor


def install_observation_transport(app: FastAPI, projection: FactoryObservation,
                                  authenticate: PrincipalResolver) -> None:
    """Mount `/observations` using a runtime-provided server-side resolver.

    `authenticate(websocket)` may be synchronous or awaitable and returns a
    trusted principal object, or `None`/false to reject. The principal is
    passed to projection authorization and never included in protocol frames.
    """

    async def observations(websocket: WebSocket) -> None:
        principal = await _resolve_principal(authenticate, websocket)
        if not principal:
            await websocket.close(code=4401, reason="authentication required")
            return
        await websocket.accept()
        factory_id = None
        run_id = None
        try:
            request = await _receive_json(websocket)
            factory_id, run_id, after_cursor = _subscribe(request)
            await _serve_subscription(websocket, projection, principal,
                                      factory_id, run_id, after_cursor)
        except WebSocketDisconnect:
            return
        except _SlowViewer:
            return
        except CursorExpired as error:
            try:
                await _resync(websocket, projection, principal, factory_id, run_id,
                              "retention_expired", minimum_cursor=error.minimum_cursor,
                              latest_cursor=error.latest_cursor)
            except Exception:
                pass
        except (ObservationForbidden, ObservationNotFound):
            await _send(websocket, {"op": "error", "code": "not_authorized",
                                    "message": "factory is unavailable"})
            await websocket.close(code=4404, reason="factory unavailable")
        except SourceContractError:
            # A malformed or conflicting durable source row is a server-side
            # observation failure, not a malformed client request.
            await _send(websocket, {"op": "error", "code": "observation_unavailable",
                                    "message": "observation source is unavailable"})
            await websocket.close(code=1011, reason="observation source is unavailable")
        except (InvalidCursor, ValueError, json.JSONDecodeError):
            await _send(websocket, {"op": "error", "code": "invalid_request",
                                    "message": "observation request is invalid"})
            await websocket.close(code=4400, reason="invalid request")
        except Exception:
            await _send(websocket, {"op": "error", "code": "observation_unavailable",
                                    "message": "observation source is unavailable"})
            await websocket.close(code=1011, reason="observation unavailable")

    app.add_api_websocket_route("/observations", observations)
