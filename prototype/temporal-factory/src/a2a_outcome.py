"""Pure bounded A2A outcome transitions plus a durable local decision journal.

The caller records dispatch intent before network I/O. After an uncertain
message/send, no transition authorizes a second submission. Participating
receivers may provide caller-action-ID lookup; opaque receivers cannot.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass, replace
from enum import StrEnum
from pathlib import Path
from typing import Any, Mapping


class ReceiverKind(StrEnum):
    PARTICIPATING = "participating"
    OPAQUE = "opaque"


class EffectKind(StrEnum):
    A2A = "a2a"
    RELEASE = "release"


class Phase(StrEnum):
    SUBMITTED = "submitted"
    CONFIRMED = "confirmed"
    UNKNOWN = "unknown"
    INCIDENT = "incident"


@dataclass(frozen=True)
class OutcomeRecord:
    action_id: str
    run_id: str
    definition_digest: str
    receiver: ReceiverKind
    effect_kind: EffectKind = EffectKind.A2A
    revision: str | None = None
    sha256: str | None = None
    phase: Phase = Phase.SUBMITTED
    lookup_count: int = 0
    lookup_limit: int = 3
    receipt: dict[str, Any] | None = None
    reason: str | None = None

    @property
    def may_submit(self) -> bool:
        # The dispatch-intent row itself makes a later replay ambiguous.
        return False


def submitted(action_id: str, run_id: str, definition_digest: str,
              receiver: ReceiverKind, *, effect_kind: EffectKind = EffectKind.A2A,
              revision: str | None = None, sha256: str | None = None,
              lookup_limit: int = 3) -> OutcomeRecord:
    if not all(isinstance(x, str) and x for x in (action_id, run_id, definition_digest)):
        raise ValueError("missing A2A action binding")
    if type(lookup_limit) is not int or lookup_limit < 1:
        raise ValueError("lookup limit must be positive")
    if effect_kind == EffectKind.RELEASE:
        if not all(isinstance(x, str) and x for x in (revision, sha256)):
            raise ValueError("release requires exact revision and digest")
    elif revision is not None or sha256 is not None:
        raise ValueError("A2A action cannot carry release binding")
    return OutcomeRecord(action_id, run_id, definition_digest, receiver,
                         effect_kind=effect_kind, revision=revision, sha256=sha256,
                         lookup_limit=lookup_limit)


def _matching(record: OutcomeRecord, receipt: Mapping[str, Any]) -> bool:
    if (receipt.get("run_id") != record.run_id
            or receipt.get("definition_digest") != record.definition_digest):
        return False
    if record.effect_kind == EffectKind.RELEASE:
        return (receipt.get("release_id") == record.action_id
                and receipt.get("revision") == record.revision
                and receipt.get("sha256") == record.sha256
                and type(receipt.get("accepted_effect_count")) is int
                and receipt["accepted_effect_count"] == 1
                and type(receipt.get("attempts")) is int
                and receipt["attempts"] >= 1)
    return (receipt.get("action_id") == record.action_id
            and isinstance(receipt.get("task_id"), str) and bool(receipt["task_id"]))


def send_completed(record: OutcomeRecord, receipt: Mapping[str, Any]) -> OutcomeRecord:
    if record.phase != Phase.SUBMITTED:
        raise ValueError("submission already resolved or uncertain")
    if not _matching(record, receipt):
        return replace(record, phase=Phase.INCIDENT, reason="reply-binding-inconsistent")
    return replace(record, phase=Phase.CONFIRMED, receipt=dict(receipt))


def send_ambiguous(record: OutcomeRecord) -> OutcomeRecord:
    if record.phase != Phase.SUBMITTED:
        raise ValueError("submission already resolved or uncertain")
    return replace(record, phase=Phase.UNKNOWN, reason="submission-outcome-unknown")


def lookup_result(record: OutcomeRecord, receipt: Mapping[str, Any] | None,
                  *, available: bool = True) -> OutcomeRecord:
    if record.phase != Phase.UNKNOWN:
        raise ValueError("lookup requires an unknown outcome")
    if record.receiver == ReceiverKind.OPAQUE:
        return replace(record, phase=Phase.INCIDENT, reason="opaque-effect-unknown")
    count = record.lookup_count + 1
    if receipt is not None:
        if not _matching(record, receipt):
            return replace(record, phase=Phase.INCIDENT, lookup_count=count,
                           reason="lookup-binding-inconsistent")
        return replace(record, phase=Phase.CONFIRMED, lookup_count=count,
                       receipt=dict(receipt), reason=None)
    if count >= record.lookup_limit:
        return replace(record, phase=Phase.INCIDENT, lookup_count=count,
                       reason="lookup-exhausted" if available else "lookup-unavailable")
    return replace(record, lookup_count=count,
                   reason="lookup-empty" if available else "lookup-unavailable")


class OutcomeJournal:
    """SQLite FULL-sync journal; one exact immutable action binding per key."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute("CREATE TABLE IF NOT EXISTS outcomes "
                                "(action_id TEXT PRIMARY KEY, value TEXT NOT NULL)")
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    def get(self, action_id: str) -> OutcomeRecord | None:
        row = self.connection.execute("SELECT value FROM outcomes WHERE action_id=?",
                                      (action_id,)).fetchone()
        if row is None:
            return None
        data = json.loads(row[0])
        data["receiver"] = ReceiverKind(data["receiver"])
        data["effect_kind"] = EffectKind(data["effect_kind"])
        data["phase"] = Phase(data["phase"])
        return OutcomeRecord(**data)

    def put(self, record: OutcomeRecord) -> None:
        with self.connection:
            self.connection.execute("INSERT INTO outcomes VALUES (?, ?) "
                                    "ON CONFLICT(action_id) DO UPDATE SET value=excluded.value",
                                    (record.action_id, json.dumps(asdict(record), sort_keys=True)))

    def begin(self, record: OutcomeRecord) -> tuple[OutcomeRecord, bool]:
        with self.connection:
            cursor = self.connection.execute("INSERT OR IGNORE INTO outcomes VALUES (?, ?)",
                (record.action_id, json.dumps(asdict(record), sort_keys=True)))
            created = cursor.rowcount == 1
        prior = self.get(record.action_id)
        assert prior is not None
        if (prior.run_id, prior.definition_digest, prior.receiver,
                prior.effect_kind, prior.revision, prior.sha256) != (
                record.run_id, record.definition_digest, record.receiver,
                record.effect_kind, record.revision, record.sha256):
            raise ValueError("action ID reused with different receiver or binding")
        return prior, created
