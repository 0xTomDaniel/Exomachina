"""Runtime wiring for the authenticated, incident-only Operations interface.

This adapter delegates run evidence to the public Observation projection and
authorization to a Runtime-owned capability resolver. Its optional atomic
publication-context reader returns only the two verified active closure pins.
It does not open Runtime databases, inspect Temporal history, execute recovery,
or publish Engineering candidates.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Callable, Mapping

from fastapi import Request

from observation import (
    FactoryObservation,
    ObservationForbidden,
    ObservationNotFound,
)
from operations import (
    FactoryOperations,
    OperationsForbidden,
    OperationsPolicyError,
)
from operations_routes import install_operations_routes


RequestPrincipalResolver = Callable[[Request], object | None]
CapabilityAuthorizer = Callable[[object, str, str, str], str]
PublicationContextReader = Callable[[str], Mapping[str, str]]


class RuntimeObservationOperationsAdapter:
    """Operations adapter backed only by Runtime authorization and Observation.

    ``authorize_principal`` must return the authenticated actor's stable public
    identity after checking the requested capability and resource. Its input
    principal must come from the server-side ``authenticate`` callback; route
    data is never used to choose an actor.
    """

    def __init__(self, observation: FactoryObservation, *, factory_id: str,
                 authorize_principal: CapabilityAuthorizer,
                 publication_context_reader: PublicationContextReader | None = None):
        if not isinstance(factory_id, str) or not factory_id:
            raise ValueError("factory_id must be explicit")
        if not callable(authorize_principal):
            raise TypeError("authorize_principal must be the Runtime capability resolver")
        if (publication_context_reader is not None
                and not callable(publication_context_reader)):
            raise TypeError("publication_context_reader must be a Runtime publication reader")
        self.observation = observation
        self.factory_id = factory_id
        self.authorize_principal = authorize_principal
        self.publication_context_reader = publication_context_reader

    def _factory(self, factory_id: str) -> None:
        if factory_id != self.factory_id:
            raise PermissionError("factory is outside this Operations instance")

    def authorize(self, principal: object, factory_id: str, capability: str,
                  resource_id: str) -> str:
        self._factory(factory_id)
        try:
            actor_id = self.authorize_principal(
                principal, factory_id, capability, resource_id)
        except (PermissionError, OperationsForbidden):
            raise
        if not isinstance(actor_id, str) or not actor_id:
            raise PermissionError("Runtime did not authorize this capability")
        return actor_id

    def public_run_snapshot(self, principal: object, factory_id: str,
                            run_id: str) -> Mapping[str, Any]:
        """Return the authenticated public snapshot for this exact run."""
        self._factory(factory_id)
        try:
            return self.observation.snapshot(principal, factory_id, run_id)
        except ObservationForbidden as error:
            raise PermissionError("run snapshot is not authorized") from error
        except ObservationNotFound as error:
            raise LookupError("run snapshot is unavailable") from error

    def current_publication(self, factory_id: str) -> Mapping[str, Any]:
        self._factory(factory_id)
        raise OperationsPolicyError(
            "Engineering publication reads are not enabled by the incident adapter")

    def quality_policy_digest(self, factory_id: str) -> str:
        self._factory(factory_id)
        raise OperationsPolicyError(
            "Quality policy reads are not enabled by the incident adapter")

    def publication_context(self, factory_id: str) -> Mapping[str, str]:
        """Return exactly the two verified pins from one active closure.

        Engineering operations that bind both a manifest and Quality policy
        must not compose the compatibility readers above: they can observe
        different publication revisions. The injected reader is factory-bound
        by the harness and resolves the active publication once.
        """
        self._factory(factory_id)
        reader = self.publication_context_reader
        if reader is None:
            raise OperationsPolicyError(
                "atomic Engineering publication context is unavailable")
        try:
            context = reader(factory_id)
        except (KeyError, LookupError, NotImplementedError, OSError, TypeError,
                ValueError) as error:
            raise OperationsPolicyError(
                "atomic Engineering publication context is unavailable") from error
        required = {"manifest_digest", "quality_policy_digest"}
        if not isinstance(context, Mapping) or set(context) != required:
            raise OperationsPolicyError(
                "atomic Engineering publication context is malformed")
        manifest_digest = context.get("manifest_digest")
        quality_policy_digest = context.get("quality_policy_digest")
        if (not isinstance(manifest_digest, str)
                or not re.fullmatch(r"[0-9a-f]{64}", manifest_digest)
                or not isinstance(quality_policy_digest, str)
                or not re.fullmatch(r"[0-9a-f]{64}", quality_policy_digest)):
            raise OperationsPolicyError(
                "atomic Engineering publication context is malformed")
        return {"manifest_digest": manifest_digest,
                "quality_policy_digest": quality_policy_digest}

    def verify_recovery(self, factory_id: str, incident: Mapping[str, Any],
                        action_id: str, evidence_refs: list[str]) -> bool:
        self._factory(factory_id)
        # There is no Runtime-owned recovery executor/evidence verifier in this
        # adapter. A caller can never turn a claimed success into closure here.
        return False


def install_runtime_operations(app: Any, *, observation: FactoryObservation,
                               database: Path, factory_id: str,
                               authenticate: RequestPrincipalResolver,
                               authorize_principal: CapabilityAuthorizer,
                               max_list_limit: int,
                               publication_context_reader: PublicationContextReader | None = None
                               ) -> FactoryOperations:
    """Mount the bounded incident API with explicit Runtime-owned dependencies.

    Runtime must supply a per-factory Operations database path, a request
    principal resolver backed by its authenticated session, a capability
    authorizer, and an explicit list bound (1..256). The optional publication
    reader supplies only an atomic manifest/Quality digest pair; it does not
    enable candidate surfaces, promotion, research, or recovery. Returns the
    mounted ``FactoryOperations`` instance for Runtime lifecycle management.
    """
    if not callable(authenticate):
        raise TypeError("authenticate must be the Runtime request principal resolver")
    adapter = RuntimeObservationOperationsAdapter(
        observation, factory_id=factory_id,
        authorize_principal=authorize_principal,
        publication_context_reader=publication_context_reader)
    operations = FactoryOperations(
        adapter, Path(database), factory_id=factory_id,
        allowed_recovery_actions=(), max_recovery_attempts=None,
        allowed_candidate_surfaces=())
    install_operations_routes(
        app, operations, authenticate, max_list_limit=max_list_limit)
    return operations


__all__ = [
    "CapabilityAuthorizer",
    "PublicationContextReader",
    "RequestPrincipalResolver",
    "RuntimeObservationOperationsAdapter",
    "install_runtime_operations",
]
