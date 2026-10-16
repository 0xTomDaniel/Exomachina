"""Durable, bounded maintenance and Engineering request state.

This module is a state Interface, not an executor. It records authorized
requests and results supplied by Runtime-owned public Interfaces. It never
runs shell commands, starts Temporal, invokes a model, edits active run state,
or publishes a candidate.

Runtime injects an ``OperationsAdapter`` with these exact methods:

* ``authorize(principal, factory_id, capability, resource_id) -> actor_id``
  resolves a server-side principal and returns its stable public identity.
* ``public_run_snapshot(principal, factory_id, run_id) -> Mapping`` returns
  the authenticated public Observation snapshot for exactly that run. The
  principal is the server-resolved request principal, not client token data.
* ``current_publication(factory_id) -> Mapping`` returns the active record
  with ``manifest_digest``.
* ``quality_policy_digest(factory_id) -> str`` returns the currently pinned
  Quality policy digest.
* ``publication_context(factory_id) -> Mapping`` returns exactly
  ``manifest_digest`` and ``quality_policy_digest`` from one verified active
  closure. Evaluation and enabled-campaign requests use this atomic read;
  the legacy readers remain for manifest-only callers.
* ``verify_recovery(factory_id, incident, action_id, evidence_refs) -> bool``
  checks recovery against authoritative runtime evidence. A successful command
  receipt alone is insufficient to resolve an incident.

The adapter's authority is supplied by Runtime; this module does not derive an
actor identity from client data. Candidate and research operations persist
requests for Runtime/Quality to consume separately. Their bounded local state
does not certify live qualification.

The candidate and research methods are currently unmounted. Runtime must add
their routes and trusted consumers separately; this module never publishes,
starts a trial, or invokes an evaluator.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from typing import Any, Iterable, Mapping, Protocol


_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}$")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_ENUM = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")
_MAX_TEXT = 1024
_MAX_REFS = 256
_MAX_EVENTS_PER_INCIDENT = 512
_MAX_POLICY_LIMIT = 100
_MAX_CANDIDATE_BYTES = 16_384
_MAX_CANDIDATE_CHANGES = 100
_MAX_SURFACES = 32


class OperationsError(Exception):
    """Base class for a rejected public operation."""


class OperationsForbidden(OperationsError):
    pass


class OperationsNotFound(OperationsError):
    pass


class OperationsConflict(OperationsError):
    pass


class OperationsStale(OperationsError):
    pass


class OperationsPolicyError(OperationsError):
    pass


class OperationsAdapter(Protocol):
    """Runtime's narrow, server-side authority and evidence injection seam."""

    def authorize(self, principal: object, factory_id: str, capability: str,
                  resource_id: str) -> str: ...

    def public_run_snapshot(self, principal: object, factory_id: str,
                            run_id: str) -> Mapping[str, Any]: ...

    def current_publication(self, factory_id: str) -> Mapping[str, Any]: ...

    def quality_policy_digest(self, factory_id: str) -> str: ...

    def publication_context(self, factory_id: str) -> Mapping[str, str]: ...

    def verify_recovery(self, factory_id: str, incident: Mapping[str, Any],
                        action_id: str, evidence_refs: list[str]) -> bool: ...


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False)


def _time() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _require_id(field: str, value: Any) -> str:
    if not isinstance(value, str) or not _ID.fullmatch(value):
        raise OperationsPolicyError(f"invalid {field}")
    return value


def _require_digest(field: str, value: Any) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise OperationsPolicyError(f"invalid {field}")
    return value


def _require_enum(field: str, value: Any) -> str:
    if not isinstance(value, str) or not _ENUM.fullmatch(value):
        raise OperationsPolicyError(f"invalid {field}")
    return value


def _require_text(field: str, value: Any, *, maximum: int = _MAX_TEXT) -> str:
    if (not isinstance(value, str) or not value or len(value) > maximum
            or any(ord(char) < 32 for char in value)):
        raise OperationsPolicyError(f"invalid {field}")
    return value


def _require_limit(field: str, value: Any) -> int:
    if type(value) is not int or value < 1 or value > _MAX_POLICY_LIMIT:
        raise OperationsPolicyError(f"{field} must be an explicit finite integer from 1 to {_MAX_POLICY_LIMIT}")
    return value


def _refs(values: Iterable[str], *, allow_empty: bool = False) -> list[str]:
    if isinstance(values, (str, bytes)):
        raise OperationsPolicyError("evidence references must be a list of SHA-256 digests")
    result = []
    for value in values:
        result.append(_require_digest("evidence reference", value))
        if len(result) > _MAX_REFS:
            raise OperationsPolicyError("too many evidence references")
    unique = sorted(set(result))
    if not unique and not allow_empty:
        raise OperationsPolicyError("at least one evidence reference is required")
    return unique


class FactoryOperations:
    """Persistent public operations bound to one factory identity.

    ``allowed_recovery_actions`` is a Runtime policy allowlist of symbolic
    action IDs. Requests carry no executable code or command arguments. The
    finite limits are local safety ceilings; deployments still supply their
    own smaller explicit policies.
    """

    def __init__(self, adapter: OperationsAdapter, database: Path, *, factory_id: str,
                 allowed_recovery_actions: Iterable[str], max_recovery_attempts: int | None,
                 allowed_candidate_surfaces: Iterable[str] = ()):
        self.adapter = adapter
        self.database = Path(database)
        self.factory_id = _require_id("factory_id", factory_id)
        if isinstance(allowed_recovery_actions, (str, bytes)):
            raise OperationsPolicyError("recovery actions must be an explicit finite allowlist")
        recovery_actions = []
        for action in allowed_recovery_actions:
            recovery_actions.append(_require_enum("recovery action", action))
            if len(recovery_actions) > _MAX_POLICY_LIMIT:
                raise OperationsPolicyError("recovery action allowlist exceeds its finite limit")
        self.allowed_recovery_actions = frozenset(recovery_actions)
        if not self.allowed_recovery_actions and max_recovery_attempts is None:
            self.max_recovery_attempts = 0
        else:
            if (type(max_recovery_attempts) is not int
                    or not 1 <= max_recovery_attempts <= _MAX_POLICY_LIMIT):
                raise OperationsPolicyError(
                    "configured recovery actions require an explicit finite attempt limit")
            self.max_recovery_attempts = max_recovery_attempts
        if isinstance(allowed_candidate_surfaces, (str, bytes)):
            raise OperationsPolicyError("candidate surfaces must be an explicit finite allowlist")
        policy_surfaces = []
        for surface in allowed_candidate_surfaces:
            policy_surfaces.append(_require_text("candidate surface", surface, maximum=256))
            if len(policy_surfaces) > _MAX_SURFACES:
                raise OperationsPolicyError("candidate surface policy exceeds its finite limit")
        self.allowed_candidate_surfaces = frozenset(policy_surfaces)
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.database, timeout=15, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        db.execute("PRAGMA foreign_keys=ON")
        return db

    def _initialize(self) -> None:
        with self._connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS operations_incidents (
                    factory_id TEXT NOT NULL,
                    incident_id TEXT PRIMARY KEY,
                    identity_key TEXT NOT NULL UNIQUE,
                    run_id TEXT NOT NULL,
                    subject_kind TEXT NOT NULL,
                    subject_id TEXT NOT NULL,
                    failure_class TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    acknowledged_by TEXT,
                    acknowledged_at TEXT,
                    owner_id TEXT,
                    claim_epoch INTEGER NOT NULL,
                    recovery_action_id TEXT,
                    recovery_action TEXT,
                    recovery_claim_epoch INTEGER,
                    recovery_attempts INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS operations_recovery_requests (
                    action_id TEXT PRIMARY KEY,
                    factory_id TEXT NOT NULL,
                    incident_id TEXT NOT NULL REFERENCES operations_incidents(incident_id),
                    fingerprint TEXT NOT NULL,
                    action TEXT NOT NULL,
                    precondition_digest TEXT NOT NULL,
                    claim_epoch INTEGER NOT NULL,
                    attempt INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    outcome TEXT,
                    evidence_json TEXT NOT NULL DEFAULT '[]',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS operations_incident_refs (
                    incident_id TEXT NOT NULL REFERENCES operations_incidents(incident_id),
                    ref_kind TEXT NOT NULL,
                    evidence_ref TEXT NOT NULL,
                    PRIMARY KEY (incident_id, ref_kind, evidence_ref)
                );
                CREATE TABLE IF NOT EXISTS operations_incident_events (
                    event_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_id TEXT NOT NULL REFERENCES operations_incidents(incident_id),
                    kind TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    occurred_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS operations_incident_events_by_id
                    ON operations_incident_events(incident_id, event_seq);
                CREATE TABLE IF NOT EXISTS operations_candidates (
                    candidate_id TEXT PRIMARY KEY,
                    factory_id TEXT NOT NULL,
                    campaign_id TEXT,
                    parent_manifest_digest TEXT NOT NULL,
                    hypothesis TEXT NOT NULL,
                    changes_json TEXT NOT NULL,
                    mutable_surface_json TEXT NOT NULL,
                    predicted_quality_effect TEXT NOT NULL,
                    predicted_cost_effect TEXT NOT NULL,
                    state TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    validation_passed INTEGER,
                    validation_refs_json TEXT NOT NULL DEFAULT '[]',
                    quality_policy_digest TEXT,
                    evaluation_verdict TEXT,
                    evaluation_refs_json TEXT NOT NULL DEFAULT '[]',
                    evaluation_actor TEXT,
                    promotion_request_id TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS operations_candidate_events (
                    event_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    candidate_id TEXT NOT NULL REFERENCES operations_candidates(candidate_id),
                    kind TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    occurred_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS operations_candidate_events_by_id
                    ON operations_candidate_events(candidate_id, event_seq);
                CREATE TABLE IF NOT EXISTS operations_research_campaigns (
                    campaign_id TEXT PRIMARY KEY,
                    factory_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    parent_manifest_digest TEXT,
                    quality_policy_digest TEXT,
                    allowed_surface_json TEXT NOT NULL,
                    trial_limit INTEGER,
                    promotion_limit INTEGER,
                    quality_limit INTEGER,
                    trial_used INTEGER NOT NULL DEFAULT 0,
                    promotion_used INTEGER NOT NULL DEFAULT 0,
                    quality_used INTEGER NOT NULL DEFAULT 0,
                    version INTEGER NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS operations_research_admissions (
                    admission_id TEXT PRIMARY KEY,
                    factory_id TEXT NOT NULL,
                    campaign_id TEXT NOT NULL REFERENCES operations_research_campaigns(campaign_id),
                    candidate_id TEXT NOT NULL REFERENCES operations_candidates(candidate_id),
                    state TEXT NOT NULL,
                    admitted_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(campaign_id, candidate_id)
                );
                CREATE TABLE IF NOT EXISTS operations_promotion_requests (
                    promotion_request_id TEXT PRIMARY KEY,
                    factory_id TEXT NOT NULL,
                    candidate_id TEXT NOT NULL REFERENCES operations_candidates(candidate_id),
                    parent_manifest_digest TEXT NOT NULL,
                    state TEXT NOT NULL,
                    requested_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(candidate_id)
                );
            """)

    def _actor(self, principal: object, capability: str, resource_id: str) -> str:
        try:
            actor = self.adapter.authorize(principal, self.factory_id, capability, resource_id)
        except (PermissionError, OperationsForbidden) as error:
            raise OperationsForbidden("principal is not authorized for this operation") from error
        return _require_id("authorized actor identity", actor)

    def _row(self, db: sqlite3.Connection, incident_id: str) -> sqlite3.Row:
        row = db.execute("SELECT * FROM operations_incidents WHERE incident_id=?",
                         (incident_id,)).fetchone()
        if row is None:
            raise OperationsNotFound("unknown incident")
        if row["factory_id"] != self.factory_id:
            raise OperationsNotFound("unknown incident")
        return row

    @staticmethod
    def _version(value: Any) -> None:
        if type(value) is not int or value < 1:
            raise OperationsPolicyError("expected_version must be a positive integer")

    @classmethod
    def _check_version(cls, row: sqlite3.Row, expected: int) -> None:
        cls._version(expected)
        if row["version"] != expected:
            raise OperationsStale("operation version is stale")

    @staticmethod
    def _check_claim(row: sqlite3.Row, actor: str, claim_epoch: int) -> None:
        if row["owner_id"] != actor or row["claim_epoch"] != claim_epoch:
            raise OperationsStale("incident claim is no longer current")

    @staticmethod
    def _candidate_row(db: sqlite3.Connection, candidate_id: str) -> sqlite3.Row:
        row = db.execute("SELECT * FROM operations_candidates WHERE candidate_id=?",
                         (candidate_id,)).fetchone()
        if row is None:
            raise OperationsNotFound("unknown candidate")
        return row

    @staticmethod
    def _audit(db: sqlite3.Connection, incident_id: str, kind: str, actor: str,
               payload: Mapping[str, Any]) -> None:
        count = db.execute("SELECT COUNT(*) FROM operations_incident_events WHERE incident_id=?",
                           (incident_id,)).fetchone()[0]
        if count >= _MAX_EVENTS_PER_INCIDENT:
            raise OperationsPolicyError("incident evidence history reached its finite limit")
        db.execute("INSERT INTO operations_incident_events "
                   "(incident_id,kind,actor_id,payload_json,occurred_at) VALUES (?,?,?,?,?)",
                   (incident_id, kind, actor, _canonical(dict(payload)), _time()))

    def _public_incident(self, db: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
        refs: dict[str, list[str]] = {"observed": [], "protected": [], "recovery": [], "escalation": []}
        for ref in db.execute(
                "SELECT ref_kind,evidence_ref FROM operations_incident_refs "
                "WHERE incident_id=? ORDER BY ref_kind,evidence_ref", (row["incident_id"],)):
            refs.setdefault(ref["ref_kind"], []).append(ref["evidence_ref"])
        history = []
        for event in db.execute(
                "SELECT kind,actor_id,payload_json,occurred_at FROM operations_incident_events "
                "WHERE incident_id=? ORDER BY event_seq", (row["incident_id"],)):
            history.append({"kind": event["kind"], "actor_id": event["actor_id"],
                            "payload": json.loads(event["payload_json"]),
                            "occurred_at": event["occurred_at"]})
        return {
            "id": row["incident_id"], "factory_id": self.factory_id,
            "run_id": row["run_id"], "subject_kind": row["subject_kind"],
            "subject_id": row["subject_id"], "failure_class": row["failure_class"],
            "generation": row["generation"], "state": row["state"],
            "version": row["version"], "acknowledged_by": row["acknowledged_by"],
            "acknowledged_at": row["acknowledged_at"], "owner_id": row["owner_id"],
            "claim_epoch": row["claim_epoch"],
            "recovery": (None if row["recovery_action_id"] is None else {
                "action_id": row["recovery_action_id"], "action": row["recovery_action"],
                "claim_epoch": row["recovery_claim_epoch"],
                "attempt": row["recovery_attempts"]}),
            "evidence_refs": refs["observed"], "protected_evidence_refs": refs["protected"],
            "recovery_evidence_refs": refs["recovery"], "escalation_evidence_refs": refs["escalation"],
            "history": history, "created_at": row["created_at"], "updated_at": row["updated_at"],
        }

    def _record_refs(self, db: sqlite3.Connection, incident_id: str, kind: str,
                     values: Iterable[str]) -> int:
        before = db.execute("SELECT COUNT(*) FROM operations_incident_refs WHERE incident_id=?",
                            (incident_id,)).fetchone()[0]
        for value in values:
            db.execute("INSERT OR IGNORE INTO operations_incident_refs VALUES (?,?,?)",
                       (incident_id, kind, value))
        after = db.execute("SELECT COUNT(*) FROM operations_incident_refs WHERE incident_id=?",
                           (incident_id,)).fetchone()[0]
        if after > _MAX_REFS:
            raise OperationsPolicyError("incident evidence references reached their finite limit")
        return after - before

    @staticmethod
    def _candidate_audit(db: sqlite3.Connection, candidate_id: str, kind: str,
                         actor: str, payload: Mapping[str, Any]) -> None:
        count = db.execute("SELECT COUNT(*) FROM operations_candidate_events WHERE candidate_id=?",
                           (candidate_id,)).fetchone()[0]
        if count >= _MAX_EVENTS_PER_INCIDENT:
            raise OperationsPolicyError("candidate history reached its finite limit")
        db.execute("INSERT INTO operations_candidate_events "
                   "(candidate_id,kind,actor_id,payload_json,occurred_at) VALUES (?,?,?,?,?)",
                   (candidate_id, kind, actor, _canonical(dict(payload)), _time()))

    @staticmethod
    def _public_candidate(db: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
        history = []
        for event in db.execute(
                "SELECT kind,actor_id,payload_json,occurred_at FROM operations_candidate_events "
                "WHERE candidate_id=? ORDER BY event_seq", (row["candidate_id"],)):
            history.append({"kind": event["kind"], "actor_id": event["actor_id"],
                            "payload": json.loads(event["payload_json"]),
                            "occurred_at": event["occurred_at"]})
        return {
            "id": row["candidate_id"], "factory_id": row["factory_id"],
            "campaign_id": row["campaign_id"],
            "parent_manifest_digest": row["parent_manifest_digest"],
            "hypothesis": row["hypothesis"],
            "native_changes": json.loads(row["changes_json"]),
            "allowed_mutable_surface": json.loads(row["mutable_surface_json"]),
            "predicted_quality_effect": row["predicted_quality_effect"],
            "predicted_cost_effect": row["predicted_cost_effect"],
            "state": row["state"], "version": row["version"],
            "validation": (None if row["validation_passed"] is None else {
                "passed": bool(row["validation_passed"]),
                "evidence_refs": json.loads(row["validation_refs_json"])}),
            "evaluation": (None if row["evaluation_verdict"] is None else {
                "verdict": row["evaluation_verdict"],
                "quality_policy_digest": row["quality_policy_digest"],
                "evidence_refs": json.loads(row["evaluation_refs_json"]),
                "actor_id": row["evaluation_actor"]}),
            "promotion_request_id": row["promotion_request_id"],
            "created_by": row["created_by"], "created_at": row["created_at"],
            "updated_at": row["updated_at"], "history": history,
        }

    def _active_manifest(self) -> str:
        try:
            publication = self.adapter.current_publication(self.factory_id)
        except (KeyError, LookupError) as error:
            raise OperationsPolicyError("current publication is unavailable") from error
        if not isinstance(publication, Mapping):
            raise OperationsPolicyError("current publication is unavailable")
        return _require_digest("current publication manifest_digest",
                               publication.get("manifest_digest"))

    def _publication_context(self) -> tuple[str, str]:
        """Read and validate both admission pins from one active closure."""
        reader = getattr(self.adapter, "publication_context", None)
        if not callable(reader):
            raise OperationsPolicyError("atomic publication context is unavailable")
        try:
            context = reader(self.factory_id)
        except (KeyError, LookupError, NotImplementedError) as error:
            raise OperationsPolicyError("atomic publication context is unavailable") from error
        required = {"manifest_digest", "quality_policy_digest"}
        if not isinstance(context, Mapping) or set(context) != required:
            raise OperationsPolicyError("atomic publication context is malformed")
        manifest_digest = _require_digest(
            "publication context manifest_digest", context.get("manifest_digest"))
        quality_digest = _require_digest(
            "publication context quality_policy_digest", context.get("quality_policy_digest"))
        return manifest_digest, quality_digest

    def _public_run(self, principal: object, run_id: str) -> Mapping[str, Any]:
        try:
            snapshot = self.adapter.public_run_snapshot(principal, self.factory_id, run_id)
        except PermissionError as error:
            raise OperationsForbidden("public run snapshot is not authorized") from error
        except (KeyError, LookupError) as error:
            raise OperationsNotFound("unknown run") from error
        if not isinstance(snapshot, Mapping) or snapshot.get("schema_version") != 1:
            raise OperationsPolicyError("Runtime returned an invalid public snapshot")
        state = snapshot.get("state")
        runs = state.get("runs") if isinstance(state, Mapping) else None
        factory = state.get("factory") if isinstance(state, Mapping) else None
        if not isinstance(factory, Mapping) or factory.get("id") != self.factory_id:
            raise OperationsPolicyError("Runtime public snapshot has a different factory binding")
        if not isinstance(runs, list):
            raise OperationsPolicyError("Runtime public snapshot has no run records")
        matches = [run for run in runs if isinstance(run, Mapping) and run.get("id") == run_id]
        if len(matches) != 1:
            raise OperationsNotFound("unknown run")
        return matches[0]

    @staticmethod
    def _record_data(value: Any) -> Mapping[str, Any] | None:
        if not isinstance(value, Mapping):
            return None
        data = value.get("data")
        return data if isinstance(data, Mapping) else value

    @classmethod
    def _accepted_evidence_refs(cls, run: Mapping[str, Any]) -> list[str]:
        quality = run.get("quality", [])
        if not isinstance(quality, list):
            raise OperationsPolicyError("Runtime public snapshot has malformed evidence rows")
        refs = set()
        for raw in quality:
            row = cls._record_data(raw)
            if row is None or row.get("accepted") is not True:
                continue
            _require_id("accepted artifact revision", row.get("artifact_revision"))
            refs.add(_require_digest("accepted artifact digest", row.get("artifact_sha256")))
        return sorted(refs)

    def _protected_evidence(self, principal: object, run_id: str) -> list[str]:
        return self._accepted_evidence_refs(self._public_run(principal, run_id))

    @staticmethod
    def _validate_subject(run: Mapping[str, Any], subject_kind: str, subject_id: str) -> None:
        if subject_kind == "run":
            if subject_id != run.get("id"):
                raise OperationsPolicyError("run incident subject must match run_id")
            return
        assignments = run.get("assignments", [])
        if not isinstance(assignments, list):
            raise OperationsPolicyError("Runtime public snapshot has malformed assignments")
        if subject_kind == "assignment":
            if not any(isinstance(row, Mapping) and row.get("id") == subject_id
                       for row in assignments):
                raise OperationsNotFound("assignment is not present in the public run snapshot")
            return
        dependencies = set()
        for assignment in assignments:
            if not isinstance(assignment, Mapping):
                continue
            attempts = assignment.get("attempts", [])
            if not isinstance(attempts, list):
                continue
            for raw in attempts:
                row = FactoryOperations._record_data(raw)
                if row is None:
                    continue
                for key in ("provider_identity", "capability"):
                    if isinstance(row.get(key), str):
                        dependencies.add(row[key])
        if subject_id not in dependencies:
            raise OperationsNotFound("dependency is not present in the public run snapshot")

    @staticmethod
    def _json_text(field: str, value: Any, *, maximum: int = _MAX_CANDIDATE_BYTES) -> str:
        try:
            encoded = _canonical(value)
        except (TypeError, ValueError) as error:
            raise OperationsPolicyError(f"{field} must be finite JSON data") from error
        if len(encoded.encode("utf-8")) > maximum:
            raise OperationsPolicyError(f"{field} exceeds its finite size limit")
        return encoded

    def _candidate_surfaces(self, values: Iterable[str]) -> list[str]:
        if isinstance(values, (str, bytes)):
            raise OperationsPolicyError("mutable surface must be a finite list")
        result = set()
        for value in values:
            result.add(_require_text("candidate surface", value, maximum=256))
            if len(result) > _MAX_SURFACES:
                raise OperationsPolicyError("candidate must declare no more than 32 mutable surfaces")
        surfaces = sorted(result)
        if not surfaces or len(surfaces) > _MAX_SURFACES:
            raise OperationsPolicyError("candidate must declare 1 to 32 mutable surfaces")
        if not set(surfaces) <= self.allowed_candidate_surfaces:
            raise OperationsPolicyError("candidate exceeds the injected mutable-surface policy")
        return surfaces

    def _candidate_changes(self, value: Any, surfaces: list[str]) -> list[dict[str, Any]]:
        if not isinstance(value, list) or not value or len(value) > _MAX_CANDIDATE_CHANGES:
            raise OperationsPolicyError("native_changes must contain 1 to 100 bounded operations")
        result = []
        for change in value:
            if not isinstance(change, Mapping):
                raise OperationsPolicyError("native change must be an object")
            operation = change.get("op")
            path = change.get("path")
            if (not isinstance(operation, str) or operation not in {"add", "replace", "remove"}
                    or not isinstance(path, str) or path not in surfaces):
                raise OperationsPolicyError("native change is outside the declared mutable surface")
            required = {"op", "path"} if operation == "remove" else {"op", "path", "value"}
            if set(change) != required:
                raise OperationsPolicyError("native change has undeclared fields")
            result.append(dict(change))
        self._json_text("native_changes", result)
        return result

    def report_incident(self, principal: object, *, run_id: str, subject_kind: str,
                        subject_id: str, failure_class: str, generation: int,
                        evidence_refs: Iterable[str]) -> dict[str, Any]:
        """Record/coalesce an incident; accepted evidence is copied from Runtime."""
        run_id = _require_id("run_id", run_id)
        subject_id = _require_id("subject_id", subject_id)
        if subject_kind not in {"run", "assignment", "dependency"}:
            raise OperationsPolicyError("subject_kind must be run, assignment, or dependency")
        if subject_kind == "run" and subject_id != run_id:
            raise OperationsPolicyError("run incident subject must match run_id")
        failure_class = _require_enum("failure_class", failure_class)
        if type(generation) is not int or generation < 1 or generation > 2_147_483_647:
            raise OperationsPolicyError("generation must be an explicit positive integer")
        actor = self._actor(principal, "maintenance.incident.report", run_id)
        public_run = self._public_run(principal, run_id)
        self._validate_subject(public_run, subject_kind, subject_id)
        observed_refs = _refs(evidence_refs, allow_empty=True)
        protected_refs = self._accepted_evidence_refs(public_run)
        if not observed_refs and not protected_refs:
            raise OperationsPolicyError("incident requires durable evidence references")
        identity = {"factory_id": self.factory_id, "run_id": run_id,
                    "subject_kind": subject_kind, "subject_id": subject_id,
                    "failure_class": failure_class, "generation": generation}
        identity_key = hashlib.sha256(_canonical(identity).encode()).hexdigest()
        incident_id = "inc-" + identity_key
        now = _time()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            cursor = db.execute(
                "INSERT OR IGNORE INTO operations_incidents "
                "(factory_id,incident_id,identity_key,run_id,subject_kind,subject_id,failure_class,generation," 
                "state,version,claim_epoch,recovery_attempts,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,'open',1,0,0,?,?)",
                (self.factory_id, incident_id, identity_key, run_id, subject_kind, subject_id,
                 failure_class, generation, now, now))
            created = cursor.rowcount == 1
            existing = self._row(db, incident_id)
            if not created and existing["state"] == "closed":
                raise OperationsConflict("closed incident recurrence requires a new generation")
            added_observed = self._record_refs(db, incident_id, "observed", observed_refs)
            added_protected = self._record_refs(db, incident_id, "protected", protected_refs)
            row = self._row(db, incident_id)
            if created:
                self._audit(db, incident_id, "reported", actor,
                            {"evidence_count": added_observed,
                             "protected_evidence_count": added_protected})
            elif added_observed or added_protected:
                db.execute("UPDATE operations_incidents SET version=version+1,updated_at=? "
                           "WHERE incident_id=?", (now, incident_id))
                self._audit(db, incident_id, "evidence_added", actor,
                            {"evidence_count": added_observed,
                             "protected_evidence_count": added_protected})
            result = self._public_incident(db, self._row(db, incident_id))
            db.commit()
        return result

    def get_incident(self, principal: object, incident_id: str) -> dict[str, Any]:
        incident_id = _require_id("incident_id", incident_id)
        self._actor(principal, "maintenance.incident.read", incident_id)
        with self._connect() as db:
            return self._public_incident(db, self._row(db, incident_id))

    def list_incidents(self, principal: object, *, run_id: str | None = None,
                       limit: int = 100) -> list[dict[str, Any]]:
        actor = self._actor(principal, "maintenance.incident.read", run_id or self.factory_id)
        del actor
        if run_id is not None:
            run_id = _require_id("run_id", run_id)
        if type(limit) is not int or not 1 <= limit <= 256:
            raise OperationsPolicyError("incident list limit must be from 1 to 256")
        with self._connect() as db:
            if run_id is None:
                rows = db.execute("SELECT * FROM operations_incidents WHERE factory_id=? "
                                  "ORDER BY created_at DESC LIMIT ?",
                                  (self.factory_id, limit)).fetchall()
            else:
                rows = db.execute("SELECT * FROM operations_incidents WHERE factory_id=? AND run_id=? "
                                  "ORDER BY created_at DESC LIMIT ?",
                                  (self.factory_id, run_id, limit)).fetchall()
            return [self._public_incident(db, row) for row in rows]

    def acknowledge_incident(self, principal: object, incident_id: str, *,
                             expected_version: int) -> dict[str, Any]:
        incident_id = _require_id("incident_id", incident_id)
        actor = self._actor(principal, "maintenance.incident.acknowledge", incident_id)
        self._version(expected_version)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._row(db, incident_id)
            if row["acknowledged_by"] is not None:
                result = self._public_incident(db, row)
                db.commit()
                return result
            self._check_version(row, expected_version)
            if row["state"] != "open":
                raise OperationsConflict("only an open incident can be acknowledged")
            now = _time()
            db.execute("UPDATE operations_incidents SET state='acknowledged',acknowledged_by=?,"
                       "acknowledged_at=?,version=version+1,updated_at=? WHERE incident_id=?",
                       (actor, now, now, incident_id))
            self._audit(db, incident_id, "acknowledged", actor, {})
            result = self._public_incident(db, self._row(db, incident_id))
            db.commit()
            return result

    def claim_incident(self, principal: object, incident_id: str, *, expected_version: int,
                       takeover: bool = False) -> dict[str, Any]:
        incident_id = _require_id("incident_id", incident_id)
        actor = self._actor(principal, "maintenance.incident.claim", incident_id)
        if type(takeover) is not bool:
            raise OperationsPolicyError("takeover must be boolean")
        if takeover:
            self._actor(principal, "maintenance.incident.claim.takeover", incident_id)
        self._version(expected_version)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._row(db, incident_id)
            if row["owner_id"] == actor:
                result = self._public_incident(db, row)
                db.commit()
                return result
            self._check_version(row, expected_version)
            now = _time()
            if row["owner_id"] is not None:
                if not takeover or row["state"] not in {
                        "claimed", "recovery_requested", "recovery_waiting", "recovery_unknown",
                        "recovery_unverified", "escalated"}:
                    raise OperationsConflict("incident already has a recovery owner")
                state = row["state"]
                kind = "claim_transferred"
            else:
                if row["state"] == "escalated" and takeover:
                    state = "claimed"
                    kind = "claim_resumed_after_escalation"
                elif row["state"] == "acknowledged":
                    state = "claimed"
                    kind = "claimed"
                else:
                    raise OperationsConflict("incident must be acknowledged before claim")
            epoch = row["claim_epoch"] + 1
            db.execute("UPDATE operations_incidents SET state=?,owner_id=?,claim_epoch=?,"
                       "version=version+1,updated_at=? WHERE incident_id=?",
                       (state, actor, epoch, now, incident_id))
            self._audit(db, incident_id, kind, actor, {"claim_epoch": epoch})
            result = self._public_incident(db, self._row(db, incident_id))
            db.commit()
            return result

    def release_incident_claim(self, principal: object, incident_id: str, *,
                               claim_epoch: int, expected_version: int) -> dict[str, Any]:
        incident_id = _require_id("incident_id", incident_id)
        actor = self._actor(principal, "maintenance.incident.claim.release", incident_id)
        self._version(expected_version)
        if type(claim_epoch) is not int or claim_epoch < 1:
            raise OperationsPolicyError("invalid claim_epoch")
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._row(db, incident_id)
            self._check_version(row, expected_version)
            self._check_claim(row, actor, claim_epoch)
            if row["state"] != "claimed" or row["recovery_action_id"] is not None:
                raise OperationsConflict("a recovery request must be reconciled before claim release")
            next_epoch = row["claim_epoch"] + 1
            now = _time()
            db.execute("UPDATE operations_incidents SET state='acknowledged',owner_id=NULL,"
                       "claim_epoch=?,version=version+1,updated_at=? WHERE incident_id=?",
                       (next_epoch, now, incident_id))
            self._audit(db, incident_id, "claim_released", actor, {"claim_epoch": next_epoch})
            result = self._public_incident(db, self._row(db, incident_id))
            db.commit()
            return result

    def request_recovery(self, principal: object, incident_id: str, *, action_id: str,
                         action: str, precondition_digest: str, claim_epoch: int,
                         expected_version: int) -> dict[str, Any]:
        """Persist an allowlisted symbolic request; Runtime owns execution."""
        incident_id = _require_id("incident_id", incident_id)
        action_id = _require_id("action_id", action_id)
        action = _require_enum("recovery action", action)
        precondition_digest = _require_digest("precondition_digest", precondition_digest)
        actor = self._actor(principal, "maintenance.recovery.request", incident_id)
        self._version(expected_version)
        if type(claim_epoch) is not int or claim_epoch < 1:
            raise OperationsPolicyError("invalid claim_epoch")
        if action not in self.allowed_recovery_actions:
            raise OperationsPolicyError("recovery action is not in the injected Runtime policy allowlist")
        fingerprint = hashlib.sha256(_canonical({"incident_id": incident_id,
            "action": action, "precondition_digest": precondition_digest,
            "claim_epoch": claim_epoch}).encode()).hexdigest()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._row(db, incident_id)
            prior = db.execute("SELECT fingerprint FROM operations_recovery_requests "
                               "WHERE action_id=?", (action_id,)).fetchone()
            if prior is not None:
                if prior["fingerprint"] != fingerprint:
                    raise OperationsConflict("recovery action_id was reused with different content")
                result = self._public_incident(db, row)
                db.commit()
                return result
            self._check_version(row, expected_version)
            self._check_claim(row, actor, claim_epoch)
            if row["state"] != "claimed":
                raise OperationsConflict("incident is not available for a new recovery request")
            if row["recovery_attempts"] >= self.max_recovery_attempts:
                raise OperationsPolicyError("recovery attempt limit reached; escalate the incident")
            attempt = row["recovery_attempts"] + 1
            now = _time()
            db.execute("INSERT INTO operations_recovery_requests "
                       "(action_id,factory_id,incident_id,fingerprint,action,precondition_digest,"
                       "claim_epoch,attempt,state,created_at,updated_at) "
                       "VALUES (?,?,?,?,?,?,?,?,'requested',?,?)",
                       (action_id, self.factory_id, incident_id, fingerprint, action,
                        precondition_digest, claim_epoch, attempt, now, now))
            db.execute("UPDATE operations_incidents SET state='recovery_requested',"
                       "recovery_action_id=?,recovery_action=?,recovery_claim_epoch=?,"
                       "recovery_attempts=?,version=version+1,updated_at=? WHERE incident_id=?",
                       (action_id, action, claim_epoch, attempt, now, incident_id))
            self._audit(db, incident_id, "recovery_requested", actor,
                        {"action_id": action_id, "action": action, "attempt": attempt,
                         "precondition_digest": precondition_digest,
                         "claim_epoch": claim_epoch})
            result = self._public_incident(db, self._row(db, incident_id))
            db.commit()
            return result

    def record_recovery_result(self, principal: object, incident_id: str, *, action_id: str,
                               outcome: str, evidence_refs: Iterable[str], claim_epoch: int,
                               expected_version: int) -> dict[str, Any]:
        """Record a Runtime observation; only verified evidence can close an incident."""
        incident_id = _require_id("incident_id", incident_id)
        action_id = _require_id("action_id", action_id)
        actor = self._actor(principal, "maintenance.recovery.result.record", incident_id)
        if outcome not in {"waiting", "unknown", "failed", "succeeded"}:
            raise OperationsPolicyError("invalid recovery outcome")
        self._version(expected_version)
        if type(claim_epoch) is not int or claim_epoch < 1:
            raise OperationsPolicyError("invalid claim_epoch")
        refs = _refs(evidence_refs, allow_empty=True)
        if outcome == "succeeded" and not refs:
            raise OperationsPolicyError("verified recovery requires authoritative evidence references")
        protected_refs = self._protected_evidence(
            principal, self.get_incident(principal, incident_id)["run_id"])
        now = _time()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._row(db, incident_id)
            request = db.execute("SELECT * FROM operations_recovery_requests "
                                 "WHERE action_id=? AND incident_id=?",
                                 (action_id, incident_id)).fetchone()
            if request is None:
                raise OperationsNotFound("unknown recovery action")
            encoded_refs = _canonical(refs)
            # Exact retries are read-only and idempotent, even after closure.
            if request["outcome"] == outcome and request["evidence_json"] == encoded_refs:
                result = self._public_incident(db, row)
                db.commit()
                return result
            self._check_version(row, expected_version)
            self._check_claim(row, actor, claim_epoch)
            if row["recovery_action_id"] != action_id:
                raise OperationsStale("recovery action is no longer current")
            if request["state"] not in {"requested", "waiting", "unknown", "unverified", "escalated"}:
                raise OperationsConflict("recovery action is already terminal")
            if row["state"] not in {"recovery_requested", "recovery_waiting", "recovery_unknown",
                                    "recovery_unverified", "claimed"}:
                raise OperationsConflict("incident is not accepting a recovery result")
            self._record_refs(db, incident_id, "recovery", refs)
            self._record_refs(db, incident_id, "protected", protected_refs)

            verified = False
            if outcome == "succeeded":
                verified = self.adapter.verify_recovery(
                    self.factory_id, self._public_incident(db, row), action_id, refs) is True
            action_state = outcome if outcome != "succeeded" or verified else "unverified"
            db.execute("UPDATE operations_recovery_requests SET state=?,outcome=?,evidence_json=?,"
                       "updated_at=? WHERE action_id=?",
                       (action_state, outcome, encoded_refs, now, action_id))

            next_owner = row["owner_id"]
            next_epoch = row["claim_epoch"]
            next_action_id = row["recovery_action_id"]
            next_action = row["recovery_action"]
            next_action_epoch = row["recovery_claim_epoch"]
            if outcome == "waiting":
                next_state = "recovery_waiting"
            elif outcome == "unknown":
                next_state = "recovery_unknown"
            elif outcome == "succeeded" and verified:
                next_state = "closed"
                next_owner = None
                next_epoch += 1
            elif outcome == "succeeded":
                next_state = "recovery_unverified"
            elif row["recovery_attempts"] >= self.max_recovery_attempts:
                next_state = "escalated"
                next_owner = None
                next_epoch += 1
            else:
                next_state = "claimed"
                next_action_id = None
                next_action = None
                next_action_epoch = None
            version = row["version"] + 1
            db.execute("UPDATE operations_incidents SET state=?,owner_id=?,claim_epoch=?,"
                       "recovery_action_id=?,recovery_action=?,recovery_claim_epoch=?,"
                       "version=?,updated_at=? WHERE incident_id=?",
                       (next_state, next_owner, next_epoch, next_action_id, next_action,
                        next_action_epoch, version, now, incident_id))
            history_kind = ("recovery_verified" if outcome == "succeeded" and verified else
                            "recovery_unverified" if outcome == "succeeded" else
                            "recovery_" + outcome)
            self._audit(db, incident_id, history_kind, actor,
                        {"action_id": action_id, "evidence_count": len(refs),
                         "attempt": row["recovery_attempts"]})
            result = self._public_incident(db, self._row(db, incident_id))
            db.commit()
            return result

    def escalate_incident(self, principal: object, incident_id: str, *, reason: str,
                          evidence_refs: Iterable[str], expected_version: int) -> dict[str, Any]:
        """Fence the current owner and preserve evidence for Director/operator review."""
        incident_id = _require_id("incident_id", incident_id)
        reason = _require_enum("escalation reason", reason)
        actor = self._actor(principal, "maintenance.incident.escalate", incident_id)
        self._version(expected_version)
        refs = _refs(evidence_refs, allow_empty=True)
        protected_refs = self._protected_evidence(
            principal, self.get_incident(principal, incident_id)["run_id"])
        now = _time()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._row(db, incident_id)
            if row["state"] == "escalated":
                result = self._public_incident(db, row)
                db.commit()
                return result
            self._check_version(row, expected_version)
            if row["state"] == "closed":
                raise OperationsConflict("closed incident cannot be escalated")
            self._record_refs(db, incident_id, "escalation", refs)
            self._record_refs(db, incident_id, "protected", protected_refs)
            if row["recovery_action_id"] is not None:
                db.execute("UPDATE operations_recovery_requests SET state='escalated',updated_at=? "
                           "WHERE action_id=? AND state IN ('requested','waiting','unknown','unverified')",
                           (now, row["recovery_action_id"]))
            epoch = row["claim_epoch"] + 1
            db.execute("UPDATE operations_incidents SET state='escalated',owner_id=NULL,claim_epoch=?,"
                       "version=version+1,updated_at=? WHERE incident_id=?",
                       (epoch, now, incident_id))
            self._audit(db, incident_id, "escalated", actor,
                        {"reason": reason, "evidence_count": len(refs),
                         "fenced_claim_epoch": epoch})
            result = self._public_incident(db, self._row(db, incident_id))
            db.commit()
            return result

    def create_candidate(self, principal: object, *, candidate_id: str,
                         parent_manifest_digest: str, hypothesis: str,
                         native_changes: Any, allowed_mutable_surface: Iterable[str],
                         predicted_quality_effect: str, predicted_cost_effect: str,
                         campaign_id: str | None = None) -> dict[str, Any]:
        """Persist a finite native-definition proposal; never apply its changes."""
        candidate_id = _require_id("candidate_id", candidate_id)
        parent_manifest_digest = _require_digest("parent_manifest_digest", parent_manifest_digest)
        hypothesis = _require_text("hypothesis", hypothesis)
        predicted_quality_effect = _require_text("predicted_quality_effect", predicted_quality_effect)
        predicted_cost_effect = _require_text("predicted_cost_effect", predicted_cost_effect)
        if campaign_id is not None:
            campaign_id = _require_id("campaign_id", campaign_id)
        surfaces = self._candidate_surfaces(allowed_mutable_surface)
        changes = self._candidate_changes(native_changes, surfaces)
        actor = self._actor(principal, "engineering.candidate.create", candidate_id)
        active_manifest = self._active_manifest()
        if active_manifest != parent_manifest_digest:
            raise OperationsStale("candidate parent is not the current publication")
        changes_json = self._json_text("native_changes", changes)
        surfaces_json = _canonical(surfaces)
        now = _time()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if campaign_id is not None:
                campaign = db.execute("SELECT * FROM operations_research_campaigns "
                                      "WHERE campaign_id=?", (campaign_id,)).fetchone()
                if campaign is None or campaign["factory_id"] != self.factory_id:
                    raise OperationsNotFound("unknown research campaign")
                if campaign["state"] != "enabled":
                    raise OperationsPolicyError("disabled campaign admits no research candidates")
                campaign_surfaces = set(json.loads(campaign["allowed_surface_json"]))
                if (campaign["parent_manifest_digest"] != parent_manifest_digest
                        or not set(surfaces) <= campaign_surfaces):
                    raise OperationsPolicyError("candidate exceeds its campaign binding")
            prior = db.execute("SELECT * FROM operations_candidates WHERE candidate_id=?",
                               (candidate_id,)).fetchone()
            if prior is not None:
                exact = (prior["factory_id"] == self.factory_id
                         and prior["campaign_id"] == campaign_id
                         and prior["parent_manifest_digest"] == parent_manifest_digest
                         and prior["hypothesis"] == hypothesis
                         and prior["changes_json"] == changes_json
                         and prior["mutable_surface_json"] == surfaces_json
                         and prior["predicted_quality_effect"] == predicted_quality_effect
                         and prior["predicted_cost_effect"] == predicted_cost_effect)
                if not exact:
                    raise OperationsConflict("candidate_id was reused with different content")
                result = self._public_candidate(db, prior)
                db.commit()
                return result
            db.execute("INSERT INTO operations_candidates "
                       "(candidate_id,factory_id,campaign_id,parent_manifest_digest,hypothesis,"
                       "changes_json,mutable_surface_json,predicted_quality_effect,predicted_cost_effect,"
                       "state,version,created_by,created_at,updated_at) "
                       "VALUES (?,?,?,?,?,?,?,?,?,'draft',1,?,?,?)",
                       (candidate_id, self.factory_id, campaign_id, parent_manifest_digest, hypothesis,
                        changes_json, surfaces_json, predicted_quality_effect,
                        predicted_cost_effect, actor, now, now))
            self._candidate_audit(db, candidate_id, "drafted", actor,
                                  {"parent_manifest_digest": parent_manifest_digest,
                                   "change_count": len(changes),
                                   "campaign_id": campaign_id})
            result = self._public_candidate(db, self._candidate_row(db, candidate_id))
            db.commit()
            return result

    def get_candidate(self, principal: object, candidate_id: str) -> dict[str, Any]:
        candidate_id = _require_id("candidate_id", candidate_id)
        self._actor(principal, "engineering.candidate.read", candidate_id)
        with self._connect() as db:
            row = self._candidate_row(db, candidate_id)
            if row["factory_id"] != self.factory_id:
                raise OperationsNotFound("unknown candidate")
            return self._public_candidate(db, row)

    def request_candidate_validation(self, principal: object, candidate_id: str, *,
                                     expected_version: int) -> dict[str, Any]:
        candidate_id = _require_id("candidate_id", candidate_id)
        actor = self._actor(principal, "engineering.candidate.validation.request", candidate_id)
        self._version(expected_version)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._candidate_row(db, candidate_id)
            if row["factory_id"] != self.factory_id:
                raise OperationsNotFound("unknown candidate")
            self._check_version(row, expected_version)
            if row["state"] != "draft":
                raise OperationsConflict("only a draft candidate can request validation")
            now = _time()
            db.execute("UPDATE operations_candidates SET state='validation_requested',version=version+1,"
                       "updated_at=? WHERE candidate_id=?", (now, candidate_id))
            self._candidate_audit(db, candidate_id, "validation_requested", actor, {})
            result = self._public_candidate(db, self._candidate_row(db, candidate_id))
            db.commit()
            return result

    def record_candidate_validation(self, principal: object, candidate_id: str, *,
                                    passed: bool, evidence_refs: Iterable[str],
                                    expected_version: int) -> dict[str, Any]:
        """Record Runtime structural-validation evidence; no local validator is implied."""
        candidate_id = _require_id("candidate_id", candidate_id)
        actor = self._actor(principal, "engineering.candidate.validation.record", candidate_id)
        if type(passed) is not bool:
            raise OperationsPolicyError("passed must be boolean")
        refs = _refs(evidence_refs)
        self._version(expected_version)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._candidate_row(db, candidate_id)
            if row["factory_id"] != self.factory_id:
                raise OperationsNotFound("unknown candidate")
            self._check_version(row, expected_version)
            if row["state"] != "validation_requested":
                raise OperationsConflict("candidate has no pending validation request")
            now = _time()
            next_state = "validated" if passed else "validation_rejected"
            db.execute("UPDATE operations_candidates SET state=?,validation_passed=?,"
                       "validation_refs_json=?,version=version+1,updated_at=? WHERE candidate_id=?",
                       (next_state, int(passed), _canonical(refs), now, candidate_id))
            self._candidate_audit(db, candidate_id, "validation_recorded", actor,
                                  {"passed": passed, "evidence_count": len(refs)})
            result = self._public_candidate(db, self._candidate_row(db, candidate_id))
            db.commit()
            return result

    def request_candidate_evaluation(self, principal: object, candidate_id: str, *,
                                     expected_version: int) -> dict[str, Any]:
        candidate_id = _require_id("candidate_id", candidate_id)
        actor = self._actor(principal, "engineering.candidate.evaluation.request", candidate_id)
        self._version(expected_version)
        active_manifest, policy_digest = self._publication_context()
        now = _time()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._candidate_row(db, candidate_id)
            if row["factory_id"] != self.factory_id:
                raise OperationsNotFound("unknown candidate")
            self._check_version(row, expected_version)
            if row["state"] != "validated":
                raise OperationsConflict("candidate must pass structural validation before evaluation")
            if active_manifest != row["parent_manifest_digest"]:
                raise OperationsStale("candidate evaluation parent publication changed")
            if row["campaign_id"] is not None:
                campaign = db.execute("SELECT * FROM operations_research_campaigns "
                                      "WHERE campaign_id=?", (row["campaign_id"],)).fetchone()
                if (campaign is None or campaign["state"] != "enabled"
                        or campaign["quality_policy_digest"] != policy_digest):
                    raise OperationsPolicyError("research Quality policy is not current")
                if campaign["quality_used"] >= campaign["quality_limit"]:
                    raise OperationsPolicyError("research Quality evaluation limit reached")
                db.execute("UPDATE operations_research_campaigns SET quality_used=quality_used+1,"
                           "version=version+1,updated_at=? WHERE campaign_id=?",
                           (now, row["campaign_id"]))
            db.execute("UPDATE operations_candidates SET state='evaluation_requested',"
                       "quality_policy_digest=?,version=version+1,updated_at=? WHERE candidate_id=?",
                       (policy_digest, now, candidate_id))
            self._candidate_audit(db, candidate_id, "evaluation_requested", actor,
                                  {"quality_policy_digest": policy_digest,
                                   "parent_manifest_digest": active_manifest})
            result = self._public_candidate(db, self._candidate_row(db, candidate_id))
            db.commit()
            return result

    def record_candidate_evaluation(self, principal: object, candidate_id: str, *,
                                   verdict: str, quality_policy_digest: str,
                                   evidence_refs: Iterable[str],
                                   expected_version: int) -> dict[str, Any]:
        """Record an independent Quality result against the pinned policy digest."""
        candidate_id = _require_id("candidate_id", candidate_id)
        verdict = _require_enum("evaluation verdict", verdict)
        if verdict not in {"passed", "failed", "inconclusive"}:
            raise OperationsPolicyError("evaluation verdict must be passed, failed, or inconclusive")
        quality_policy_digest = _require_digest("quality_policy_digest", quality_policy_digest)
        refs = _refs(evidence_refs)
        actor = self._actor(principal, "quality.candidate.evaluation.record", candidate_id)
        self._version(expected_version)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._candidate_row(db, candidate_id)
            if row["factory_id"] != self.factory_id:
                raise OperationsNotFound("unknown candidate")
            self._check_version(row, expected_version)
            if row["state"] != "evaluation_requested":
                raise OperationsConflict("candidate has no pending Quality evaluation")
            if row["quality_policy_digest"] != quality_policy_digest:
                raise OperationsPolicyError("evaluation used a different protected Quality policy")
            if row["campaign_id"] is not None:
                campaign = db.execute("SELECT * FROM operations_research_campaigns "
                                      "WHERE campaign_id=?", (row["campaign_id"],)).fetchone()
                if campaign is None or campaign["quality_policy_digest"] != quality_policy_digest:
                    raise OperationsPolicyError("evaluation does not match the campaign Quality policy")
            now = _time()
            next_state = {"passed": "evaluation_passed", "failed": "evaluation_failed",
                          "inconclusive": "evaluation_inconclusive"}[verdict]
            db.execute("UPDATE operations_candidates SET state=?,evaluation_verdict=?,"
                       "evaluation_refs_json=?,evaluation_actor=?,version=version+1,updated_at=? "
                       "WHERE candidate_id=?",
                       (next_state, verdict, _canonical(refs), actor, now, candidate_id))
            self._candidate_audit(db, candidate_id, "quality_evaluation_recorded", actor,
                                  {"verdict": verdict,
                                   "quality_policy_digest": quality_policy_digest,
                                   "evidence_count": len(refs)})
            result = self._public_candidate(db, self._candidate_row(db, candidate_id))
            db.commit()
            return result

    def request_candidate_promotion(self, principal: object, candidate_id: str, *,
                                    promotion_request_id: str,
                                    expected_version: int) -> dict[str, Any]:
        """Record an authorized future-publication request with its original CAS precondition."""
        candidate_id = _require_id("candidate_id", candidate_id)
        promotion_request_id = _require_id("promotion_request_id", promotion_request_id)
        actor = self._actor(principal, "director.candidate.promotion.request", candidate_id)
        self._version(expected_version)
        active_manifest = self._active_manifest()
        now = _time()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._candidate_row(db, candidate_id)
            if row["factory_id"] != self.factory_id:
                raise OperationsNotFound("unknown candidate")
            if row["promotion_request_id"] == promotion_request_id:
                result = self._public_candidate(db, row)
                db.commit()
                return result
            self._check_version(row, expected_version)
            if row["state"] != "evaluation_passed":
                raise OperationsConflict("only independently evaluated candidates may request promotion")
            if active_manifest != row["parent_manifest_digest"]:
                raise OperationsStale("promotion parent is no longer the current admission binding")
            if row["campaign_id"] is not None:
                campaign = db.execute("SELECT * FROM operations_research_campaigns "
                                      "WHERE campaign_id=?", (row["campaign_id"],)).fetchone()
                if campaign is None or campaign["state"] != "enabled":
                    raise OperationsPolicyError("research campaign is not enabled for promotion requests")
                if campaign["promotion_used"] >= campaign["promotion_limit"]:
                    raise OperationsPolicyError("research promotion limit reached")
                db.execute("UPDATE operations_research_campaigns SET promotion_used=promotion_used+1,"
                           "version=version+1,updated_at=? WHERE campaign_id=?",
                           (now, row["campaign_id"]))
            prior = db.execute("SELECT * FROM operations_promotion_requests "
                               "WHERE promotion_request_id=?", (promotion_request_id,)).fetchone()
            if prior is not None:
                if (prior["factory_id"] != self.factory_id
                        or prior["candidate_id"] != candidate_id
                        or prior["parent_manifest_digest"] != row["parent_manifest_digest"]):
                    raise OperationsConflict("promotion_request_id was reused with different content")
                result = self._public_candidate(db, row)
                db.commit()
                return result
            db.execute("INSERT INTO operations_promotion_requests "
                       "(promotion_request_id,factory_id,candidate_id,parent_manifest_digest,state,"
                       "requested_by,created_at) VALUES (?,?,?,?,'requested',?,?)",
                       (promotion_request_id, self.factory_id, candidate_id,
                        row["parent_manifest_digest"], actor, now))
            db.execute("UPDATE operations_candidates SET state='promotion_requested',"
                       "promotion_request_id=?,version=version+1,updated_at=? WHERE candidate_id=?",
                       (promotion_request_id, now, candidate_id))
            self._candidate_audit(db, candidate_id, "promotion_requested", actor,
                                  {"promotion_request_id": promotion_request_id,
                                   "parent_manifest_digest": row["parent_manifest_digest"]})
            result = self._public_candidate(db, self._candidate_row(db, candidate_id))
            db.commit()
            return result

    def get_promotion_request(self, principal: object, promotion_request_id: str) -> dict[str, Any]:
        promotion_request_id = _require_id("promotion_request_id", promotion_request_id)
        self._actor(principal, "director.candidate.promotion.read", promotion_request_id)
        with self._connect() as db:
            row = db.execute("SELECT * FROM operations_promotion_requests "
                             "WHERE promotion_request_id=? AND factory_id=?",
                             (promotion_request_id, self.factory_id)).fetchone()
            if row is None:
                raise OperationsNotFound("unknown promotion request")
            return {"id": row["promotion_request_id"], "candidate_id": row["candidate_id"],
                    "parent_manifest_digest": row["parent_manifest_digest"],
                    "state": row["state"], "requested_by": row["requested_by"],
                    "created_at": row["created_at"]}

    @staticmethod
    def _public_campaign(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["campaign_id"], "factory_id": row["factory_id"],
            "state": row["state"], "parent_manifest_digest": row["parent_manifest_digest"],
            "quality_policy_digest": row["quality_policy_digest"],
            "allowed_mutable_surface": json.loads(row["allowed_surface_json"]),
            "limits": {"trials": row["trial_limit"], "promotions": row["promotion_limit"],
                       "quality_evaluations": row["quality_limit"]},
            "used": {"trials": row["trial_used"], "promotions": row["promotion_used"],
                     "quality_evaluations": row["quality_used"]},
            "version": row["version"], "created_by": row["created_by"],
            "created_at": row["created_at"], "updated_at": row["updated_at"],
        }

    def create_research_campaign(self, principal: object, *, campaign_id: str,
                                 allowed_mutable_surface: Iterable[str],
                                 enabled: bool = False, trial_limit: int | None = None,
                                 promotion_limit: int | None = None,
                                 quality_limit: int | None = None) -> dict[str, Any]:
        campaign_id = _require_id("campaign_id", campaign_id)
        actor = self._actor(principal, "engineering.research.configure", campaign_id)
        if type(enabled) is not bool:
            raise OperationsPolicyError("enabled must be boolean")
        if enabled:
            trial_limit = _require_limit("trial_limit", trial_limit)
            promotion_limit = _require_limit("promotion_limit", promotion_limit)
            quality_limit = _require_limit("quality_limit", quality_limit)
            surfaces = self._candidate_surfaces(allowed_mutable_surface)
            parent, policy = self._publication_context()
        else:
            if any(limit is not None for limit in (trial_limit, promotion_limit, quality_limit)):
                raise OperationsPolicyError("disabled campaign must not reserve admission limits")
            if isinstance(allowed_mutable_surface, (str, bytes)):
                raise OperationsPolicyError("campaign surface must be a finite list")
            surface_set = set()
            for value in allowed_mutable_surface:
                surface_set.add(_require_text("candidate surface", value, maximum=256))
                if len(surface_set) > _MAX_SURFACES:
                    raise OperationsPolicyError("campaign mutable surface exceeds its finite limit")
            surfaces = sorted(surface_set)
            if not set(surfaces) <= self.allowed_candidate_surfaces:
                raise OperationsPolicyError("disabled campaign surface is outside policy")
            parent = None
            policy = None
        now = _time()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            prior = db.execute("SELECT * FROM operations_research_campaigns "
                               "WHERE campaign_id=?", (campaign_id,)).fetchone()
            surface_json = _canonical(surfaces)
            if prior is not None:
                exact = (prior["factory_id"] == self.factory_id
                         and prior["state"] == ("enabled" if enabled else "disabled")
                         and prior["allowed_surface_json"] == surface_json
                         and prior["trial_limit"] == trial_limit
                         and prior["promotion_limit"] == promotion_limit
                         and prior["quality_limit"] == quality_limit)
                if not exact:
                    raise OperationsConflict("campaign_id already has different policy")
                result = self._public_campaign(prior)
                db.commit()
                return result
            db.execute("INSERT INTO operations_research_campaigns "
                       "(campaign_id,factory_id,state,parent_manifest_digest,quality_policy_digest,"
                       "allowed_surface_json,trial_limit,promotion_limit,quality_limit,version,"
                       "created_by,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,1,?,?,?)",
                       (campaign_id, self.factory_id, "enabled" if enabled else "disabled", parent,
                        policy, surface_json, trial_limit, promotion_limit, quality_limit,
                        actor, now, now))
            result = self._public_campaign(db.execute(
                "SELECT * FROM operations_research_campaigns WHERE campaign_id=?",
                (campaign_id,)).fetchone())
            db.commit()
            return result

    def get_research_campaign(self, principal: object, campaign_id: str) -> dict[str, Any]:
        campaign_id = _require_id("campaign_id", campaign_id)
        self._actor(principal, "engineering.research.read", campaign_id)
        with self._connect() as db:
            row = db.execute("SELECT * FROM operations_research_campaigns "
                             "WHERE campaign_id=? AND factory_id=?",
                             (campaign_id, self.factory_id)).fetchone()
            if row is None:
                raise OperationsNotFound("unknown research campaign")
            return self._public_campaign(row)

    def research_status(self, principal: object) -> dict[str, Any]:
        self._actor(principal, "engineering.research.read", self.factory_id)
        with self._connect() as db:
            rows = db.execute("SELECT * FROM operations_research_campaigns "
                              "WHERE factory_id=? ORDER BY created_at,campaign_id",
                              (self.factory_id,)).fetchall()
            return {"state": "disabled" if not any(row["state"] == "enabled" for row in rows)
                    else "enabled", "admitted_trials": sum(row["trial_used"] for row in rows),
                    "campaigns": [self._public_campaign(row) for row in rows]}

    def enable_research_campaign(self, principal: object, campaign_id: str, *,
                                 trial_limit: int, promotion_limit: int, quality_limit: int,
                                 allowed_mutable_surface: Iterable[str],
                                 expected_version: int) -> dict[str, Any]:
        campaign_id = _require_id("campaign_id", campaign_id)
        actor = self._actor(principal, "engineering.research.configure", campaign_id)
        trial_limit = _require_limit("trial_limit", trial_limit)
        promotion_limit = _require_limit("promotion_limit", promotion_limit)
        quality_limit = _require_limit("quality_limit", quality_limit)
        surfaces = self._candidate_surfaces(allowed_mutable_surface)
        self._version(expected_version)
        parent, policy = self._publication_context()
        now = _time()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM operations_research_campaigns "
                             "WHERE campaign_id=? AND factory_id=?",
                             (campaign_id, self.factory_id)).fetchone()
            if row is None:
                raise OperationsNotFound("unknown research campaign")
            self._check_version(row, expected_version)
            if row["state"] != "disabled" or row["trial_used"] != 0:
                raise OperationsConflict("only an unused disabled campaign can be enabled")
            db.execute("UPDATE operations_research_campaigns SET state='enabled',"
                       "parent_manifest_digest=?,quality_policy_digest=?,allowed_surface_json=?,"
                       "trial_limit=?,promotion_limit=?,quality_limit=?,version=version+1,updated_at=? "
                       "WHERE campaign_id=?",
                       (parent, policy, _canonical(surfaces), trial_limit, promotion_limit,
                        quality_limit, now, campaign_id))
            result = self._public_campaign(db.execute(
                "SELECT * FROM operations_research_campaigns WHERE campaign_id=?",
                (campaign_id,)).fetchone())
            db.commit()
            return result

    def stop_research_campaign(self, principal: object, campaign_id: str, *,
                               expected_version: int) -> dict[str, Any]:
        campaign_id = _require_id("campaign_id", campaign_id)
        actor = self._actor(principal, "director.research.stop", campaign_id)
        self._version(expected_version)
        now = _time()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM operations_research_campaigns "
                             "WHERE campaign_id=? AND factory_id=?",
                             (campaign_id, self.factory_id)).fetchone()
            if row is None:
                raise OperationsNotFound("unknown research campaign")
            if row["state"] == "stopped":
                result = self._public_campaign(row)
                db.commit()
                return result
            self._check_version(row, expected_version)
            if row["state"] != "enabled":
                raise OperationsConflict("only an enabled campaign can be stopped")
            db.execute("UPDATE operations_research_campaigns SET state='stopped',version=version+1,"
                       "updated_at=? WHERE campaign_id=?", (now, campaign_id))
            result = self._public_campaign(db.execute(
                "SELECT * FROM operations_research_campaigns WHERE campaign_id=?",
                (campaign_id,)).fetchone())
            db.commit()
            return result

    def admit_research_trial(self, principal: object, campaign_id: str, candidate_id: str, *,
                             admission_id: str, expected_campaign_version: int) -> dict[str, Any]:
        campaign_id = _require_id("campaign_id", campaign_id)
        candidate_id = _require_id("candidate_id", candidate_id)
        admission_id = _require_id("admission_id", admission_id)
        actor = self._actor(principal, "engineering.research.trial.admit", campaign_id)
        self._version(expected_campaign_version)
        now = _time()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            campaign = db.execute("SELECT * FROM operations_research_campaigns "
                                  "WHERE campaign_id=? AND factory_id=?",
                                  (campaign_id, self.factory_id)).fetchone()
            if campaign is None:
                raise OperationsNotFound("unknown research campaign")
            prior = db.execute("SELECT * FROM operations_research_admissions "
                               "WHERE admission_id=?", (admission_id,)).fetchone()
            if prior is not None:
                if prior["campaign_id"] != campaign_id or prior["candidate_id"] != candidate_id:
                    raise OperationsConflict("admission_id was reused with different content")
                result = {"id": prior["admission_id"], "campaign_id": campaign_id,
                          "candidate_id": candidate_id, "state": prior["state"],
                          "created_at": prior["created_at"]}
                db.commit()
                return result
            if campaign["state"] != "enabled":
                raise OperationsPolicyError("disabled or stopped research campaign admits zero trials")
            self._check_version(campaign, expected_campaign_version)
            if campaign["trial_used"] >= campaign["trial_limit"]:
                raise OperationsPolicyError("research trial limit reached")
            candidate = self._candidate_row(db, candidate_id)
            if (candidate["factory_id"] != self.factory_id
                    or candidate["campaign_id"] != campaign_id
                    or candidate["parent_manifest_digest"] != campaign["parent_manifest_digest"]
                    or candidate["state"] != "validated"):
                raise OperationsConflict("trial requires a validated candidate bound to this campaign")
            db.execute("INSERT INTO operations_research_admissions "
                       "(admission_id,factory_id,campaign_id,candidate_id,state,admitted_by,created_at) "
                       "VALUES (?,?,?,?,'admitted',?,?)",
                       (admission_id, self.factory_id, campaign_id, candidate_id, actor, now))
            db.execute("UPDATE operations_research_campaigns SET trial_used=trial_used+1,"
                       "version=version+1,updated_at=? WHERE campaign_id=?",
                       (now, campaign_id))
            result = {"id": admission_id, "campaign_id": campaign_id,
                      "candidate_id": candidate_id, "state": "admitted", "created_at": now}
            db.commit()
            return result
