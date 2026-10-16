"""Durable per-factory work admission and active-run capacity queue.

This module grants execution slots only. It owns no supplier budget, monetary
reservation, inference usage, Temporal transition, or payment operation. A
caller supplies the factory capacity explicitly; there is no default.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any, Iterator


class AdmissionError(ValueError):
    """An admission request or state transition is invalid."""


class AdmissionConfigurationConflict(AdmissionError):
    """Persisted per-factory capacity differs from explicit configuration."""


class AdmissionIdempotencyConflict(AdmissionError):
    """An admission or release identity was reused with different intent."""


class SlotNotHeld(AdmissionError):
    """The request does not currently hold an admitted execution slot."""


class AdmissionState(StrEnum):
    QUEUED = "queued"
    ADMITTED = "admitted"
    RELEASED = "released"


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AdmissionError(f"{name} must be non-empty text")
    return value


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


class AdmissionQueue:
    """SQLite-backed admission queue scoped to one configured factory.

    The public lifecycle is ``enqueue`` -> (queued or admitted) -> ``release``.
    Enqueue, capacity checks, FIFO promotion, and release are serialized with
    SQLite ``BEGIN IMMEDIATE`` transactions across processes. Releasing a slot
    promotes the oldest waiting request in the same transaction.

    ``request_id`` is a caller-owned idempotency identity, normally a stable
    command or Task identity. ``task_id`` is an authoritative existing Task ID;
    the queue never manufactures a Task or run ID. ``release_id`` is a durable
    caller-owned idempotency identity for the terminal slot-release fact.
    """

    def __init__(self, database: Path | str, *, factory_id: str, capacity: int):
        self.database = Path(database).expanduser().resolve()
        self.factory_id = _text(factory_id, "factory_id")
        self.capacity = self._capacity(capacity)
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @staticmethod
    def _capacity(value: object) -> int:
        if type(value) is not int or value < 0:
            raise AdmissionError("capacity must be an explicitly configured non-negative integer")
        return value

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.database, timeout=30.0, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=30000")
        db.execute("PRAGMA synchronous=FULL")
        db.execute("PRAGMA foreign_keys=ON")
        return db

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        """Close every short-lived reader/writer connection explicitly."""
        db = self._connect()
        try:
            yield db
        finally:
            db.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                db.commit()
            except BaseException:
                db.rollback()
                raise

    def _initialize(self) -> None:
        with self._transaction() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS factory_admission_config(
                factory_id TEXT PRIMARY KEY,
                capacity INTEGER NOT NULL CHECK(capacity >= 0),
                configured_at TEXT NOT NULL
            )""")
            db.execute("""CREATE TABLE IF NOT EXISTS admission_requests(
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                factory_id TEXT NOT NULL,
                request_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                fingerprint TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('queued','admitted','released')),
                created_at TEXT NOT NULL,
                admitted_at TEXT,
                released_at TEXT,
                release_id TEXT,
                UNIQUE(factory_id, request_id),
                UNIQUE(factory_id, task_id),
                UNIQUE(factory_id, release_id),
                FOREIGN KEY(factory_id) REFERENCES factory_admission_config(factory_id)
            )""")
            db.execute("""CREATE INDEX IF NOT EXISTS admission_fifo
                ON admission_requests(factory_id,state,sequence)""")
            db.execute("""CREATE TABLE IF NOT EXISTS admission_release_receipts(
                factory_id TEXT NOT NULL,
                release_id TEXT NOT NULL,
                request_id TEXT NOT NULL,
                fingerprint TEXT NOT NULL,
                result_json TEXT NOT NULL,
                PRIMARY KEY(factory_id,release_id),
                FOREIGN KEY(factory_id) REFERENCES factory_admission_config(factory_id)
            )""")
            configured = db.execute(
                "SELECT capacity FROM factory_admission_config WHERE factory_id=?",
                (self.factory_id,)).fetchone()
            if configured is None:
                db.execute("INSERT INTO factory_admission_config VALUES (?,?,?)",
                           (self.factory_id, self.capacity, _now()))
            elif configured["capacity"] != self.capacity:
                raise AdmissionConfigurationConflict(
                    "configured capacity differs from the durable factory admission configuration")

    def _promote_available(self, db: sqlite3.Connection) -> list[sqlite3.Row]:
        active = db.execute("""SELECT COUNT(*) FROM admission_requests
            WHERE factory_id=? AND state=?""",
            (self.factory_id, AdmissionState.ADMITTED.value)).fetchone()[0]
        available = self.capacity - active
        if available <= 0:
            return []
        waiting = db.execute("""SELECT sequence FROM admission_requests
            WHERE factory_id=? AND state=? ORDER BY sequence LIMIT ?""",
            (self.factory_id, AdmissionState.QUEUED.value, available)).fetchall()
        promoted: list[sqlite3.Row] = []
        for item in waiting:
            sequence, admitted_at = item["sequence"], _now()
            db.execute("UPDATE admission_requests SET state=?, admitted_at=? WHERE sequence=?",
                       (AdmissionState.ADMITTED.value, admitted_at, sequence))
            promoted.append(db.execute("SELECT * FROM admission_requests WHERE sequence=?",
                                       (sequence,)).fetchone())
        return promoted

    def _view(self, db: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
        state = row["state"]
        position = None
        if state == AdmissionState.QUEUED.value:
            position = db.execute("""SELECT COUNT(*) FROM admission_requests
                WHERE factory_id=? AND state=? AND sequence<=?""",
                (self.factory_id, AdmissionState.QUEUED.value, row["sequence"])).fetchone()[0]
        return {
            "factory_id": row["factory_id"],
            "request_id": row["request_id"],
            "task_id": row["task_id"],
            "state": state,
            "capacity": self.capacity,
            "queue_position": position,
            "created_at": row["created_at"],
            "admitted_at": row["admitted_at"],
            "released_at": row["released_at"],
            "release_id": row["release_id"],
        }

    def _by_request(self, db: sqlite3.Connection, request_id: str) -> sqlite3.Row | None:
        return db.execute("""SELECT * FROM admission_requests
            WHERE factory_id=? AND request_id=?""", (self.factory_id, request_id)).fetchone()

    def enqueue(self, request_id: str, *, task_id: str) -> dict[str, Any]:
        """Idempotently enqueue one authoritative Task and admit if a slot exists."""
        request, task = _text(request_id, "request_id"), _text(task_id, "task_id")
        fingerprint = _digest({"factory_id": self.factory_id, "request_id": request,
                               "task_id": task})
        with self._transaction() as db:
            old = self._by_request(db, request)
            if old is not None:
                if old["fingerprint"] != fingerprint:
                    raise AdmissionIdempotencyConflict(
                        "request identity reused with different Task intent")
                self._promote_available(db)
                return self._view(db, self._by_request(db, request))
            prior_task = db.execute("""SELECT request_id FROM admission_requests
                WHERE factory_id=? AND task_id=?""", (self.factory_id, task)).fetchone()
            if prior_task is not None:
                raise AdmissionIdempotencyConflict("Task already has a different admission identity")
            db.execute("""INSERT INTO admission_requests
                (factory_id,request_id,task_id,fingerprint,state,created_at)
                VALUES (?,?,?,?,?,?)""", (self.factory_id, request, task, fingerprint,
                                            AdmissionState.QUEUED.value, _now()))
            self._promote_available(db)
            return self._view(db, self._by_request(db, request))

    def admit_waiting(self) -> list[dict[str, Any]]:
        """Atomically fill free slots from the durable FIFO queue."""
        with self._transaction() as db:
            rows = self._promote_available(db)
            return [self._view(db, row) for row in rows]

    def release(self, request_id: str, *, release_id: str) -> dict[str, Any]:
        """Release one held slot and promote waiting work atomically.

        Replaying the same release identity returns the original durable result,
        including the request promoted by that release.
        """
        request, release = _text(request_id, "request_id"), _text(release_id, "release_id")
        fingerprint = _digest({"factory_id": self.factory_id, "request_id": request,
                               "release_id": release})
        with self._transaction() as db:
            old_receipt = db.execute("""SELECT fingerprint,result_json
                FROM admission_release_receipts WHERE factory_id=? AND release_id=?""",
                (self.factory_id, release)).fetchone()
            if old_receipt is not None:
                if old_receipt["fingerprint"] != fingerprint:
                    raise AdmissionIdempotencyConflict(
                        "release identity reused for different admission work")
                return json.loads(old_receipt["result_json"])
            row = self._by_request(db, request)
            if row is None or row["state"] != AdmissionState.ADMITTED.value:
                raise SlotNotHeld("request has no active admitted slot")
            released_at = _now()
            db.execute("""UPDATE admission_requests SET state=?,released_at=?,release_id=?
                WHERE factory_id=? AND request_id=?""",
                (AdmissionState.RELEASED.value, released_at, release,
                 self.factory_id, request))
            released_row = self._by_request(db, request)
            promoted = self._promote_available(db)
            result = {"released": self._view(db, released_row),
                      "admitted": [self._view(db, item) for item in promoted]}
            db.execute("""INSERT INTO admission_release_receipts
                (factory_id,release_id,request_id,fingerprint,result_json)
                VALUES (?,?,?,?,?)""",
                (self.factory_id, release, request, fingerprint, _canonical(result)))
            return result

    def get(self, request_id: str) -> dict[str, Any] | None:
        """Read one safe admission projection."""
        request = _text(request_id, "request_id")
        with self._connection() as db:
            row = self._by_request(db, request)
            return self._view(db, row) if row is not None else None

    def list_requests(self, *, state: AdmissionState | str | None = None) -> list[dict[str, Any]]:
        """List safe projections in stable FIFO identity order."""
        state_value = state.value if isinstance(state, AdmissionState) else state
        if state_value is not None and state_value not in {item.value for item in AdmissionState}:
            raise AdmissionError("unsupported admission state filter")
        with self._connection() as db:
            if state_value is None:
                rows = db.execute("""SELECT * FROM admission_requests WHERE factory_id=?
                    ORDER BY sequence""", (self.factory_id,)).fetchall()
            else:
                rows = db.execute("""SELECT * FROM admission_requests
                    WHERE factory_id=? AND state=? ORDER BY sequence""",
                    (self.factory_id, state_value)).fetchall()
            return [self._view(db, row) for row in rows]

    def capacity_view(self) -> dict[str, Any]:
        """Return one authoritative, public capacity/count snapshot for Runtime."""
        with self._connection() as db:
            row = db.execute("""SELECT c.capacity,
                    COUNT(CASE WHEN r.state='admitted' THEN 1 END) AS admitted_count,
                    COUNT(CASE WHEN r.state='queued' THEN 1 END) AS queued_count,
                    COUNT(CASE WHEN r.state='released' THEN 1 END) AS released_count
                FROM factory_admission_config AS c
                LEFT JOIN admission_requests AS r ON r.factory_id=c.factory_id
                WHERE c.factory_id=? GROUP BY c.factory_id,c.capacity""",
                (self.factory_id,)).fetchone()
            if row is None:
                raise RuntimeError("durable factory admission configuration is missing")
            admitted = int(row["admitted_count"])
            capacity = int(row["capacity"])
            if admitted > capacity:
                raise RuntimeError("durable admitted count exceeds configured capacity")
            return {
                "factory_id": self.factory_id,
                "capacity": capacity,
                "admitted_count": admitted,
                "queued_count": int(row["queued_count"]),
                "released_count": int(row["released_count"]),
                "available_slots": capacity - admitted,
            }

    def read_snapshot(self) -> dict[str, Any]:
        """Read capacity totals and request projections from one SQLite snapshot.

        Consumers that need per-request grouping should use this method rather
        than combining ``capacity_view`` and ``list_requests`` across two reads.
        Returned request entries match the existing public list projection;
        callers must keep any external response appropriately scoped.
        """
        with self._connection() as db:
            db.execute("BEGIN")
            try:
                configured = db.execute(
                    "SELECT capacity FROM factory_admission_config WHERE factory_id=?",
                    (self.factory_id,)).fetchone()
                if configured is None:
                    raise RuntimeError("durable factory admission configuration is missing")
                rows = db.execute("""SELECT * FROM admission_requests WHERE factory_id=?
                    ORDER BY sequence""", (self.factory_id,)).fetchall()
                counts = {state.value: 0 for state in AdmissionState}
                for row in rows:
                    counts[row["state"]] += 1
                capacity = int(configured["capacity"])
                admitted = counts[AdmissionState.ADMITTED.value]
                if admitted > capacity:
                    raise RuntimeError("durable admitted count exceeds configured capacity")
                result = {
                    "factory_id": self.factory_id,
                    "capacity": capacity,
                    "admitted_count": admitted,
                    "queued_count": counts[AdmissionState.QUEUED.value],
                    "released_count": counts[AdmissionState.RELEASED.value],
                    "available_slots": capacity - admitted,
                    "requests": [self._view(db, row) for row in rows],
                }
                db.commit()
                return result
            except BaseException:
                db.rollback()
                raise
