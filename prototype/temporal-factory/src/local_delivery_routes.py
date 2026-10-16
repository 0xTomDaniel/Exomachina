"""Mount local byte delivery in the existing authenticated factory application."""
from pathlib import Path
from typing import Callable

from fastapi import Request
from fastapi.responses import JSONResponse

from artifact_delivery import accepted_markdown
from local_delivery import DeliveryConflict, LocalDelivery


def install_local_delivery_routes(app, *, observation, instance_dir: Path,
                                  factory_id: str, principal: Callable,
                                  configuration: dict | None):
    """The trusted server supplies identity, destination and authentication.

    Requests select an observed run/revision/digest only. No request can supply
    a path, Task binding, acceptance verdict, receipt, or destination identity.
    """
    ledger = None
    if configuration is not None:
        if not isinstance(configuration, dict) or set(configuration) != {"destination", "identity"}:
            raise ValueError("local delivery requires explicit destination and identity")
        ledger = LocalDelivery(Path(instance_dir) / "local-delivery.sqlite3",
                               Path(configuration["destination"]), factory_id=factory_id,
                               destination_identity=configuration["identity"])

    def scope(actor):
        if actor is None:
            raise PermissionError("authenticated factory session required")
        return observation.snapshot(actor, factory_id)

    @app.post("/deliveries")
    async def deliver(request: Request):
        try:
            snapshot = scope(principal())
            if ledger is None:
                return JSONResponse({"status": "unavailable", "reason": "local destination not configured"}, status_code=503)
            body = await request.json()
            if not isinstance(body, dict) or set(body) != {"run_id", "revision", "sha256"}:
                return JSONResponse({"error": "invalid delivery request"}, status_code=400)
            if not all(isinstance(value, str) and value for value in body.values()):
                return JSONResponse({"error": "invalid delivery request"}, status_code=400)
            matches = [row for row in snapshot["state"]["runs"] if row["id"] == body["run_id"]]
            if len(matches) != 1:
                return JSONResponse({"error": "delivery run is outside the observed factory"}, status_code=404)
            run = matches[0]
            status = run["status"]
            state = status if isinstance(status, str) else status.get("state")
            phase = None if isinstance(status, str) else status.get("phase")
            if state not in {"accepted", "completed"} or (state == "completed" and phase != "accepted"):
                return JSONResponse({"error": "run has no final accepted result"}, status_code=409)
            artifacts = run["artifacts"]
            if not artifacts or (artifacts[-1].get("artifact_revision") != body["revision"] or
                                 artifacts[-1].get("artifact_sha256") != body["sha256"]):
                return JSONResponse({"error": "requested revision is not the final observed artifact"}, status_code=409)
            observed = observation.inspect_artifact(principal(), factory_id, body["run_id"],
                                                   body["revision"], body["sha256"])
            verified = accepted_markdown({"revision": body["revision"], "sha256": body["sha256"],
                                          "content": observed["content"].decode("utf-8")},
                                         revision=body["revision"], sha256=body["sha256"])
            receipt = ledger.deliver(run_id=run["id"], task_id=run["task"]["id"],
                                     context_id=run["task"]["context_id"], artifact=verified)
            return {"receipt": receipt}
        except PermissionError:
            return JSONResponse({"error": "authenticated factory scope required"}, status_code=403)
        except LookupError:
            return JSONResponse({"error": "accepted artifact unavailable"}, status_code=404)
        except (DeliveryConflict, ValueError, UnicodeError):
            return JSONResponse({"error": "delivery identity or bytes could not be verified"}, status_code=409)

    @app.get("/deliveries")
    def receipts(run_id: str | None = None):
        try:
            snapshot = scope(principal())
            if ledger is None:
                return {"receipts": [], "status": "unavailable", "reason": "local destination not configured"}
            owned = {row["id"] for row in snapshot["state"]["runs"]}
            if run_id is not None and run_id not in owned:
                return JSONResponse({"error": "delivery run is outside the observed factory"}, status_code=404)
            rows = ledger.list_receipts(run_id=run_id)
            return {"receipts": [row for row in rows if row["run_id"] in owned], "status": "available"}
        except PermissionError:
            return JSONResponse({"error": "authenticated factory scope required"}, status_code=403)
        except (DeliveryConflict, ValueError):
            return JSONResponse({"receipts": [], "status": "unavailable",
                                 "reason": "delivered bytes or owner changed"}, status_code=409)

    return ledger
