"""Durable delivery to an explicitly configured, factory-owned local destination.

The caller must obtain the artifact through the authenticated public Observation
reader and verify its authoritative Quality acceptance before calling this API.
A receipt proves deposited bytes at this destination; it makes no claim about a
remote customer's receipt or about payment.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from artifact_delivery import DeliveredMarkdown


class DeliveryConflict(ValueError):
    pass


class LocalDelivery:
    def __init__(self, database: Path, destination: Path, *, factory_id: str,
                 destination_identity: str):
        self.factory_id = self._identity(factory_id)
        self.destination_identity = self._identity(destination_identity)
        self.database = Path(database)
        self.destination = Path(destination)
        if not self.destination.is_absolute():
            raise ValueError("delivery destination must be explicitly absolute")
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self.destination.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
            db.execute("CREATE TABLE IF NOT EXISTS local_deliveries ("
                       "receipt_id TEXT PRIMARY KEY, binding TEXT NOT NULL, "
                       "receipt TEXT NOT NULL, filename TEXT NOT NULL)")

    @staticmethod
    def _identity(value: str) -> str:
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:@/-]{0,511}", value):
            raise ValueError("invalid delivery identity")
        return value

    @contextmanager
    def _connect(self):
        db = sqlite3.connect(self.database, timeout=15, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            yield db
        finally:
            db.close()

    def _verify_file(self, filename: str, digest: str) -> None:
        path = self.destination / filename
        if path.is_symlink() or not path.is_file():
            raise DeliveryConflict("delivered file is unavailable")
        if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            raise DeliveryConflict("delivered bytes changed; no replacement was written")

    def deliver(self, *, run_id: str, task_id: str, context_id: str,
                artifact: DeliveredMarkdown) -> dict:
        """Deposit exact accepted Markdown once, preserving both distinct hashes."""
        for value in (run_id, task_id, context_id, artifact.revision):
            self._identity(value)
        for value in (artifact.accepted_sha256, artifact.accepted_content_sha256,
                      artifact.markdown_sha256):
            if not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{64}", value):
                raise ValueError("invalid delivery digest")
        if (artifact.accepted_sha256 != artifact.accepted_content_sha256 or
                hashlib.sha256(artifact.content).hexdigest() != artifact.markdown_sha256):
            raise DeliveryConflict("artifact bytes do not match verified identity")
        binding = json.dumps({"factory_id": self.factory_id, "run_id": run_id,
                              "task_id": task_id, "context_id": context_id,
                              "artifact_revision": artifact.revision,
                              "artifact_sha256": artifact.accepted_sha256,
                              "markdown_sha256": artifact.markdown_sha256,
                              "destination_identity": self.destination_identity},
                             sort_keys=True, separators=(",", ":"))
        logical = json.dumps([self.factory_id, run_id, artifact.revision,
                              artifact.accepted_sha256], separators=(",", ":"))
        receipt_id = "delivery-" + hashlib.sha256(logical.encode()).hexdigest()
        filename = receipt_id + ".md"
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                prior = db.execute("SELECT * FROM local_deliveries WHERE receipt_id=?",
                                   (receipt_id,)).fetchone()
                if prior:
                    if prior["binding"] != binding:
                        raise DeliveryConflict("delivery binding changed")
                    receipt = json.loads(prior["receipt"])
                    self._verify_file(prior["filename"], receipt["markdown_sha256"])
                    db.commit()
                    return {**receipt, "duplicate": True}
                path = self.destination / filename
                if path.exists() or path.is_symlink():
                    self._verify_file(filename, artifact.markdown_sha256)
                else:
                    temporary = None
                    try:
                        with tempfile.NamedTemporaryFile(dir=self.destination, delete=False) as output:
                            temporary = Path(output.name)
                            output.write(artifact.content)
                            output.flush()
                            os.fsync(output.fileno())
                        os.replace(temporary, path)
                        directory = os.open(self.destination, os.O_RDONLY)
                        try:
                            os.fsync(directory)
                        finally:
                            os.close(directory)
                    finally:
                        if temporary is not None:
                            temporary.unlink(missing_ok=True)
                self._verify_file(filename, artifact.markdown_sha256)
                receipt = {**json.loads(binding), "receipt_id": receipt_id,
                           "state": "delivered", "delivery_kind": "local_file",
                           "byte_length": len(artifact.content),
                           "recorded_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}
                db.execute("INSERT INTO local_deliveries VALUES (?,?,?,?)",
                           (receipt_id, binding, json.dumps(receipt, sort_keys=True), filename))
                db.commit()
                return {**receipt, "duplicate": False}
            except BaseException:
                db.rollback()
                raise

    def list_receipts(self, *, run_id: str | None = None) -> list[dict]:
        """Return verified safe receipts without destination paths or artifact text."""
        if run_id is not None:
            self._identity(run_id)
        with self._connect() as db:
            rows = db.execute("SELECT * FROM local_deliveries ORDER BY receipt_id").fetchall()
        result = []
        for row in rows:
            receipt = json.loads(row["receipt"])
            if run_id is not None and receipt["run_id"] != run_id:
                continue
            if receipt["factory_id"] != self.factory_id or receipt["destination_identity"] != self.destination_identity:
                raise DeliveryConflict("configured delivery owner changed")
            self._verify_file(row["filename"], receipt["markdown_sha256"])
            result.append(receipt)
        return result
