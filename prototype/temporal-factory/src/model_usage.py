"""Durable, non-financial measurements reported for individual model calls.

The journal stores only whitelisted token-count categories and safe Task/action
bindings. It never stores prompts, provider payloads, credentials, responses, or
costs. Missing provider categories remain unavailable rather than becoming zero.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping


PROVIDER_CATEGORIES = {
    "input": "input_tokens",
    "output": "output_tokens",
    "cacheRead": "cache_read_tokens",
    "cacheWrite": "cache_write_tokens",
    "totalTokens": "total_tokens",
}
PUBLIC_CATEGORIES = tuple(PROVIDER_CATEGORIES.values())
CALL_SCOPES = frozenset({"authoring_overhead", "director_call", "assignment_call"})


class MeasurementConflict(ValueError):
    """A model-call identity was replayed with different immutable facts."""


def normalize_provider_usage(usage: object) -> dict[str, dict[str, int | str | None]]:
    """Copy only known integer token categories, preserving missing vs zero."""
    values = usage if isinstance(usage, Mapping) else {}
    result: dict[str, dict[str, int | str | None]] = {}
    for source_key, category in PROVIDER_CATEGORIES.items():
        value = values.get(source_key)
        if type(value) is int and value >= 0:
            result[category] = {"value": value, "status": "reported"}
        else:
            result[category] = {"value": None, "status": "unavailable"}
    return result


def unavailable_usage() -> dict[str, dict[str, int | str | None]]:
    return normalize_provider_usage(None)


# The factory's journal of agent-reported usage, beside its outcome journal.
AGENT_USAGE_DATABASE = "agent-usage.sqlite3"
AGENT_TOKEN_CATEGORIES = {"input": "input_tokens", "output": "output_tokens",
                          "cache_read": "cache_read_tokens",
                          "cache_write": "cache_write_tokens", "total": "total_tokens"}
MEASUREMENT_SOURCES = {"provider": "provider_reported", "agent": "agent_reported"}


def normalize_agent_tokens(tokens: object) -> dict[str, dict[str, int | str | None]]:
    """Map an A2A budget-extension ``incurred.tokens`` report; absent stays unavailable."""
    values = tokens if isinstance(tokens, Mapping) else {}
    return normalize_provider_usage({source: values.get(field)
                                     for field, category in AGENT_TOKEN_CATEGORIES.items()
                                     for source, target in PROVIDER_CATEGORIES.items()
                                     if target == category})


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def _required_text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be non-empty text")
    return value


def _optional_text(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, name)


class ModelUsageJournal:
    """SQLite journal for safe, idempotent per-model-call usage projections."""

    def __init__(self, database: Path | str):
        self.database = Path(database).expanduser().resolve()
        self.database.parent.mkdir(parents=True, exist_ok=True)
        with self._connection() as db:
            self._create_table(db)
            columns = {row["name"]: row for row in
                       db.execute("PRAGMA table_info(model_usage_measurements)")}
            nullable_columns = ("service_identity", "task_id", "action_id", "message_id",
                                "run_id", "definition_digest", "assignment_id", "attempt_id")
            required_columns = {
                "model_call_id", "fingerprint", "measurement_id", "service_identity", "task_id",
                "call_scope", "assignment_id", "attempt_id", "action_id", "message_id", "run_id",
                "definition_digest", "provider", "model_id", "reasoning_effort", "usage_json",
                "measurement_source", "completeness", "evidence_status", "recorded_at",
            }
            needs_migration = (any(name not in columns for name in required_columns) or
                               any(columns.get(name) and columns[name]["notnull"]
                                   for name in nullable_columns) or
                               ("call_scope" in columns and not columns["call_scope"]["notnull"]))
            if needs_migration:
                db.execute("BEGIN IMMEDIATE")
                db.execute("ALTER TABLE model_usage_measurements RENAME TO model_usage_measurements_pre_binding")
                self._create_table(db)
                previous = {row["name"] for row in
                            db.execute("PRAGMA table_info(model_usage_measurements_pre_binding)")}
                target = ("model_call_id", "fingerprint", "measurement_id", "service_identity",
                          "task_id", "call_scope", "action_id", "message_id", "run_id",
                          "definition_digest", "assignment_id", "attempt_id", "provider",
                          "model_id", "reasoning_effort", "usage_json",
                          "measurement_source", "completeness", "evidence_status", "recorded_at")
                scope_action = "action_id" if "action_id" in previous else "NULL"
                scope_message = "message_id" if "message_id" in previous else "NULL"
                scope_task = "task_id" if "task_id" in previous else "NULL"
                scope_fallback = (f"CASE WHEN {scope_action} IS NOT NULL THEN 'assignment_call' "
                                  f"WHEN {scope_message} IS NOT NULL THEN 'director_call' "
                                  f"WHEN {scope_task} IS NULL THEN 'authoring_overhead' "
                                  "ELSE 'assignment_call' END")
                expression = []
                for name in target:
                    if name == "call_scope":
                        expression.append(f"COALESCE(call_scope, {scope_fallback})"
                                          if name in previous else scope_fallback)
                    else:
                        expression.append(name if name in previous else "NULL")
                db.execute("INSERT INTO model_usage_measurements (" + ",".join(target) + ") SELECT " +
                           ",".join(expression) + " FROM model_usage_measurements_pre_binding")
                migrated_rows = db.execute("SELECT * FROM model_usage_measurements").fetchall()
                for row in migrated_rows:
                    fields = {name: row[name] for name in (
                        "model_call_id", "service_identity", "task_id", "call_scope", "action_id",
                        "message_id", "run_id", "definition_digest", "assignment_id", "attempt_id",
                        "provider", "model_id", "reasoning_effort")}
                    fields["usage"] = self._validate_categories(json.loads(row["usage_json"]))
                    fingerprint = _digest(fields)
                    db.execute("UPDATE model_usage_measurements SET fingerprint=? WHERE model_call_id=?",
                               (fingerprint, row["model_call_id"]))
                db.execute("DROP TABLE model_usage_measurements_pre_binding")

    @staticmethod
    def _create_table(db: sqlite3.Connection) -> None:
        db.execute("""CREATE TABLE IF NOT EXISTS model_usage_measurements(
                model_call_id TEXT PRIMARY KEY,
                fingerprint TEXT NOT NULL,
                measurement_id TEXT NOT NULL UNIQUE,
                service_identity TEXT,
                task_id TEXT,
                call_scope TEXT NOT NULL CHECK(call_scope IN (
                    'authoring_overhead','director_call','assignment_call')),
                assignment_id TEXT,
                attempt_id TEXT,
                action_id TEXT,
                message_id TEXT,
                run_id TEXT,
                definition_digest TEXT,
                provider TEXT NOT NULL,
                model_id TEXT NOT NULL,
                reasoning_effort TEXT,
                usage_json TEXT NOT NULL,
                measurement_source TEXT NOT NULL,
                completeness TEXT NOT NULL,
                evidence_status TEXT NOT NULL,
                recorded_at TEXT NOT NULL
            )""")

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.database, timeout=15)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        return db

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        """Commit or roll back a connection and always release its file handle."""
        db = self._connect()
        try:
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def _validate_categories(categories: Mapping[str, object]) -> dict[str, dict[str, int | str | None]]:
        if not isinstance(categories, Mapping) or set(categories) != set(PUBLIC_CATEGORIES):
            raise ValueError("usage must contain exactly the supported token categories")
        safe: dict[str, dict[str, int | str | None]] = {}
        for name in PUBLIC_CATEGORIES:
            item = categories[name]
            if not isinstance(item, Mapping) or set(item) != {"value", "status"}:
                raise ValueError(f"invalid usage category: {name}")
            value, status = item["value"], item["status"]
            if status == "reported" and type(value) is int and value >= 0:
                safe[name] = {"value": value, "status": "reported"}
            elif status == "unavailable" and value is None:
                safe[name] = {"value": None, "status": "unavailable"}
            else:
                raise ValueError(f"invalid usage evidence: {name}")
        return safe

    def record(self, *, model_call_id: str, provider: str, model_id: str,
               reasoning_effort: str | None, usage: Mapping[str, object],
               service_identity: str | None = None, task_id: str | None = None,
               call_scope: str | None = None, assignment_id: str | None = None,
               attempt_id: str | None = None,
               action_id: str | None = None, message_id: str | None = None,
               run_id: str | None = None, definition_digest: str | None = None,
               source: str = "provider") -> dict[str, Any]:
        """Persist one safe measurement; recorded_at is always assigned locally.

        ``source="agent"`` marks a factory-recorded A2A budget-extension report
        from an agent service (``agent_reported``) rather than a report this
        process received from its own model provider.
        """
        if source not in MEASUREMENT_SOURCES:
            raise ValueError("unsupported measurement source")
        identity = _required_text(model_call_id, "model_call_id")
        safe_action_id = _optional_text(action_id, "action_id")
        safe_message_id = _optional_text(message_id, "message_id")
        safe_task_id = _optional_text(task_id, "task_id")
        safe_scope = call_scope
        if safe_scope is None:
            # Compatibility for existing in-process writers: classify from real binding fields,
            # never by parsing the action identifier. New writers should pass call_scope.
            safe_scope = ("assignment_call" if safe_action_id is not None else
                          "director_call" if safe_message_id is not None else
                          "authoring_overhead" if safe_task_id is None else None)
        if safe_scope not in CALL_SCOPES:
            raise ValueError("call_scope must be an explicit supported value")
        if safe_task_id is None and safe_scope != "authoring_overhead":
            raise ValueError("task_id may be null only for authoring overhead")
        fields = {
            "model_call_id": identity,
            "service_identity": _optional_text(service_identity, "service_identity"),
            "task_id": safe_task_id,
            "call_scope": safe_scope,
            "assignment_id": _optional_text(assignment_id, "assignment_id"),
            "attempt_id": _optional_text(attempt_id, "attempt_id"),
            "action_id": safe_action_id,
            "message_id": safe_message_id,
            "run_id": _optional_text(run_id, "run_id"),
            "definition_digest": _optional_text(definition_digest, "definition_digest"),
            "provider": _required_text(provider, "provider"),
            "model_id": _required_text(model_id, "model_id"),
            "reasoning_effort": _optional_text(reasoning_effort, "reasoning_effort"),
            "usage": self._validate_categories(usage),
        }
        if source != "provider":
            # Provider rows keep their original fingerprint shape.
            fields["source"] = source
        reported = sum(item["status"] == "reported" for item in fields["usage"].values())
        completeness = "complete" if reported == len(PUBLIC_CATEGORIES) else (
            "partial" if reported else "unknown")
        evidence_status = MEASUREMENT_SOURCES[source] if reported else "unknown"
        measurement_source = evidence_status
        fingerprint = _digest(fields)
        measurement_id = "mu-" + _digest(identity)[:28]
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT * FROM model_usage_measurements WHERE model_call_id=?",
                             (identity,)).fetchone()
            if old is not None:
                if old["fingerprint"] != fingerprint:
                    raise MeasurementConflict("model-call measurement replay conflicts with prior facts")
                return self._view(old)
            recorded_at = datetime.now(timezone.utc).isoformat(timespec="microseconds")
            db.execute("""INSERT INTO model_usage_measurements
                (model_call_id,fingerprint,measurement_id,service_identity,task_id,call_scope,
                 assignment_id,attempt_id,action_id,message_id,run_id,definition_digest,provider,
                 model_id,reasoning_effort,usage_json,measurement_source,completeness,
                 evidence_status,recorded_at)
                 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                identity, fingerprint, measurement_id, fields["service_identity"], fields["task_id"],
                fields["call_scope"], fields["assignment_id"], fields["attempt_id"],
                fields["action_id"], fields["message_id"], fields["run_id"],
                fields["definition_digest"], fields["provider"], fields["model_id"],
                fields["reasoning_effort"], _canonical(fields["usage"]),
                measurement_source, completeness, evidence_status, recorded_at))
            return self._view(db.execute(
                "SELECT * FROM model_usage_measurements WHERE model_call_id=?", (identity,)).fetchone())

    @staticmethod
    def _view(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "measurement_id": row["measurement_id"],
            "model_call_id": row["model_call_id"],
            "service_identity": row["service_identity"],
            "task_id": row["task_id"],
            "call_scope": row["call_scope"],
            "assignment_id": row["assignment_id"],
            "attempt_id": row["attempt_id"],
            "action_id": row["action_id"],
            "message_id": row["message_id"],
            "run_id": row["run_id"],
            "definition_digest": row["definition_digest"],
            "provider": row["provider"],
            "model_id": row["model_id"],
            "reasoning_effort": row["reasoning_effort"],
            "unit": "tokens",
            "measurement_source": row["measurement_source"],
            "completeness": row["completeness"],
            "evidence_status": row["evidence_status"],
            "usage": json.loads(row["usage_json"]),
            "recorded_at": row["recorded_at"],
        }

    def get(self, model_call_id: str) -> dict[str, Any] | None:
        identity = _required_text(model_call_id, "model_call_id")
        with self._connection() as db:
            row = db.execute("SELECT * FROM model_usage_measurements WHERE model_call_id=?",
                             (identity,)).fetchone()
            return self._view(row) if row is not None else None

    def list_measurements(self, *, run_id: str | None = None, task_id: str | None = None,
                          action_id: str | None = None,
                          message_id: str | None = None,
                          assignment_id: str | None = None,
                          attempt_id: str | None = None,
                          model_call_id: str | None = None,
                          call_scope: str | None = None) -> list[dict[str, Any]]:
        """Read stable, safe projections without exposing SQLite internals."""
        filters = {"run_id": _optional_text(run_id, "run_id"),
                   "task_id": _optional_text(task_id, "task_id"),
                   "action_id": _optional_text(action_id, "action_id"),
                   "message_id": _optional_text(message_id, "message_id"),
                   "assignment_id": _optional_text(assignment_id, "assignment_id"),
                   "attempt_id": _optional_text(attempt_id, "attempt_id"),
                   "model_call_id": _optional_text(model_call_id, "model_call_id"),
                   "call_scope": _optional_text(call_scope, "call_scope")}
        if filters["call_scope"] is not None and filters["call_scope"] not in CALL_SCOPES:
            raise ValueError("unsupported call_scope filter")
        where = [(key, value) for key, value in filters.items() if value is not None]
        clause = " WHERE " + " AND ".join(f"{key}=?" for key, _ in where) if where else ""
        params = tuple(value for _, value in where)
        with self._connection() as db:
            rows = db.execute("SELECT * FROM model_usage_measurements" + clause +
                              " ORDER BY recorded_at,model_call_id", params).fetchall()
            return [self._view(row) for row in rows]
