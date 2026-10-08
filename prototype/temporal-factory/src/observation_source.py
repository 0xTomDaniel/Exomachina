"""Runtime-owned adapter from durable harness and Temporal records.

The source journal is append-only. It imports allowlisted facts from Workflow
history, the original Task/run binding, activated publications, command outcomes,
and accepted artifacts. It never builds dashboard events from periodic Workflow
query/status results.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Protocol
import re

from observation import (ObservationForbidden, ObservationNotFound, SourceContractError,
                         SourcePage, project_source_record)
from admission_observation import project_admission_observation
from definition import declared_edges, declares_handoff_graph, node_output
from wait_observation import project_public_wait_records

_OPERATIONS_INCIDENT_LIMIT = 128
_ASSIGNMENT_LINK_CORRECTION_PREFIX = (
    "projection-correction:temporal-start-link-v1:"
)
_ASSIGNMENT_NODE_LINK_CORRECTION_PREFIX = (
    "projection-correction:assignment-node-link-v1:"
)
_RUN_NODE_LINK_CORRECTION_PREFIX = (
    "projection-correction:run-node-link-v1:"
)
_TERMINAL_WORKFLOW_EVENT_TYPES = frozenset({
    "WORKFLOW_EXECUTION_COMPLETED", "WORKFLOW_EXECUTION_FAILED",
    "WORKFLOW_EXECUTION_CANCELED", "WORKFLOW_EXECUTION_TERMINATED",
    "WORKFLOW_EXECUTION_TIMED_OUT",
})


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _commercial_evidence(value: Any) -> str | None:
    return value if value in {
        "measured", "calculated_from_measured_usage", "provider_reported",
        "estimated", "unknown", "undisclosed",
    } else None


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _event_time(value: Any) -> str:
    if isinstance(value, str):
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return value
    if isinstance(value, datetime):
        return _iso(value)
    raise SourceContractError("Temporal history event is missing its durable time")


def _temporal_attempt(value: Any) -> str | None:
    """Return a Temporal attempt only when the started event states it."""
    if type(value) is int and value > 0:
        return str(value)
    return None


def _graph_nodes(document: Any, bindings: Any = None) -> list[dict[str, Any]]:
    if not isinstance(document, Mapping) or not isinstance(document.get("nodes"), Mapping):
        return []
    bindings = bindings if isinstance(bindings, Mapping) else {}
    if declares_handoff_graph(dict(document), dict(bindings)):
        # Definitions with output modes and edge kinds (7 Oct 2026) project every
        # pinned edge, including route cases and material bypass edges, with
        # the control subset and each node's declared output. Older pinned
        # definitions keep their exact earlier projection below.
        declared = []
        for name, raw in document["nodes"].items():
            if not isinstance(raw, Mapping) or not isinstance(raw.get("type"), str):
                continue
            edges = declared_edges(dict(raw))
            item = {"id": str(name), "type": raw["type"],
                    "next": [target for target, _ in edges],
                    "control": [target for target, kind in edges if kind == "control"]}
            if isinstance(raw.get("capability"), str):
                item["capability"] = raw["capability"]
            output = node_output(dict(raw), dict(bindings))
            if output is not None:
                item["output"] = output
            declared.append(item)
        return declared
    nodes: list[dict[str, Any]] = []
    for name, raw in document["nodes"].items():
        if not isinstance(raw, Mapping) or not isinstance(raw.get("type"), str):
            continue
        edges: list[str] = []
        next_value = raw.get("next")
        if isinstance(next_value, str):
            edges.append(next_value)
        elif isinstance(next_value, list):
            edges.extend(target for target in next_value if isinstance(target, str))
        for edge_name in ("exhausted",):
            target = raw.get(edge_name)
            if isinstance(target, str):
                edges.append(target)
        branches = raw.get("branches")
        if isinstance(branches, Mapping):
            for branch in branches.values():
                if isinstance(branch, Mapping) and isinstance(branch.get("next"), str):
                    edges.append(branch["next"])
        item = {"id": str(name), "type": raw["type"], "next": list(dict.fromkeys(edges))}
        if isinstance(raw.get("capability"), str):
            item["capability"] = raw["capability"]
        nodes.append(item)
    return nodes


class CommercialLedgerReader(Protocol):
    """Narrow public read seam; Runtime never owns the ledger store."""

    def list_purchases(self, *, run_id: str | None = None) -> list[dict[str, Any]]: ...


class LocalDeliveryReader(Protocol):
    """Public receipt reader; the Observation source never opens its database."""

    def list_receipts(self, *, run_id: str | None = None) -> list[dict[str, Any]]: ...


class OperationsIncidentReader(Protocol):
    """Factory-bound authenticated reader returning projected incident facts."""

    def __call__(self, principal: object, *, factory_id: str,
                 limit: int) -> list[Mapping[str, Any]]: ...


class RuntimeObservationSource:
    """`ObservationSource` implementation bound to one factory harness.

    Commercial facts are read only through an injected reader's public
    ``list_purchases(run_id=...)`` method. This adapter never opens the
    CommercialLedger database and never creates purchases, rates, reservations,
    or settlements. With no reader, commercial reporting is ``unconfigured``;
    a successful read with no attributable facts is ``unreported``. Neither
    status means zero usage or zero cost.

    Assignment-to-ledger writing remains disabled: the runtime has no agreed
    assignment offer/authorization policy or provider-usage evidence source.
    The optional read adapter does not imply that a purchase or a cost is
    attributable to every runtime assignment.
    """

    def __init__(self, director, config: dict, database: Path, *,
                 history_reader: Callable[[], Iterable[tuple[str, list[dict[str, Any]]]]] | None = None,
                 commercial_reader: CommercialLedgerReader | None = None,
                 delivery_reader: LocalDeliveryReader | None = None,
                 operations_reader: OperationsIncidentReader | None = None,
                 refresh_interval_seconds: float = 1.0):
        if refresh_interval_seconds < 0:
            raise ValueError("refresh interval must be non-negative")
        self.director = director
        self.config = config
        self.database = Path(database)
        self.history_reader = history_reader
        self.commercial_reader = commercial_reader
        self.delivery_reader = delivery_reader
        self.operations_reader = operations_reader
        self.refresh_interval_seconds = refresh_interval_seconds
        self.admission_observation_status = (
            "unconfigured" if getattr(director, "admission_queue", None) is None
            else "pending")
        self.admission_capacity_snapshot: dict[str, Any] | None = None
        self.commercial_reporting_status = "unconfigured" if commercial_reader is None else "unreported"
        self.commercial_costs_known = False
        self._lock = threading.RLock()
        self._last_refresh_at = 0.0
        self._cached_freshness: dict[str, Any] = {"status": "unknown", "observed_at": None}
        self._history_read_ids: set[str] = set()
        self._unavailable_history_ids: set[str] = set()
        self._history_children: dict[str, set[str]] = {}
        self._known_run_bindings: dict[str, tuple[str, str]] = {}
        self.database.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS source_records (
                    position INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id TEXT NOT NULL UNIQUE,
                    record_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS command_intents (
                    command_id TEXT PRIMARY KEY,
                    fingerprint TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    context_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS history_watermarks (
                    workflow_id TEXT PRIMARY KEY,
                    event_id INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS source_backfills (
                    backfill_id TEXT PRIMARY KEY,
                    completed_at TEXT NOT NULL
                );
            """)
            db.commit()

    @property
    def commercial_ledger_status(self) -> str:
        """Compatibility name retained for the harness readiness response."""
        return self.commercial_reporting_status

    @staticmethod
    def _safe_evidence_ref(value: Any) -> list[str]:
        if isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value):
            return [value]
        return []

    @staticmethod
    def _cost_amount(money: Mapping[str, Any]) -> tuple[int | None, str, str, int]:
        """Extract a ledger money tuple without changing units or scale."""
        amount = money.get("amount_units")
        currency = money.get("currency")
        scale = money.get("atomic_scale")
        evidence = _commercial_evidence(money.get("evidence"))
        if (amount is not None and (type(amount) is not int or amount < 0)):
            raise SourceContractError("CommercialLedger returned an invalid atomic amount")
        if not isinstance(currency, str) or not currency:
            raise SourceContractError("CommercialLedger money is missing currency")
        if type(scale) is not int or scale < 0:
            raise SourceContractError("CommercialLedger money is missing atomic scale")
        if not isinstance(evidence, str):
            raise SourceContractError("CommercialLedger money is missing evidence status")
        if amount is None and evidence not in {"unknown", "undisclosed"}:
            raise SourceContractError("unknown CommercialLedger money has numeric evidence status")
        if amount is not None and evidence in {"unknown", "undisclosed"}:
            raise SourceContractError("unknown CommercialLedger money carries an amount")
        return amount, currency, evidence, scale

    def _commercial_records(self, run_bindings: Mapping[str, tuple[str, str]]) -> tuple[int, int, bool]:
        """Import durable public ledger rows for known runtime runs.

        Returns (record count, cost fact count, all cost facts known). Public
        list rows are append-only identities for usage, obligations and credits;
        a settlement's recorded_at participates in its source identity so a
        later recorded settlement outcome is retained as a separate fact.
        """
        if self.commercial_reader is None:
            self.commercial_reporting_status = "unconfigured"
            self.commercial_costs_known = False
            return 0, 0, False

        purchases_seen = 0
        cost_count = 0
        expected_cost_count = 0
        all_costs_known = True
        read_ok = True
        record_count = 0
        records: list[dict[str, Any]] = []

        try:
            for run_id, (task_id, context_id) in run_bindings.items():
                purchases = self.commercial_reader.list_purchases(run_id=run_id)
                if not isinstance(purchases, list):
                    raise SourceContractError("CommercialLedger list_purchases must return a list")
                for purchase in purchases:
                    if not isinstance(purchase, Mapping) or purchase.get("run_id") != run_id:
                        read_ok = False
                        continue
                    if (purchase.get("task_id") is not None and
                            purchase.get("task_id") != task_id):
                        read_ok = False
                        continue
                    assignment_id, attempt_id = purchase.get("assignment_id"), purchase.get("attempt_id")
                    service_identity = purchase.get("service_identity")
                    if not all(isinstance(value, str) and value for value in
                               (assignment_id, attempt_id, service_identity)):
                        read_ok = False
                        continue
                    purchases_seen += 1
                    attribution = {
                        "run_id": run_id, "task_id": task_id, "context_id": context_id,
                        "assignment_id": assignment_id, "attempt_id": attempt_id,
                    }

                    usage_rows = purchase.get("usage", [])
                    if not isinstance(usage_rows, list):
                        read_ok = False
                        usage_rows = []
                    for usage in usage_rows:
                        if not isinstance(usage, Mapping):
                            read_ok = False
                            continue
                        usage_id = usage.get("usage_id")
                        row_assignment = usage.get("assignment_id")
                        row_attempt = usage.get("attempt_id")
                        row_service = usage.get("service_identity")
                        unit, evidence, measurement = (usage.get("unit"),
                            usage.get("evidence_state"), usage.get("source"))
                        if (not all(isinstance(value, str) and value for value in
                                    (usage_id, row_assignment, row_attempt, row_service, unit, evidence)) or
                                row_assignment != assignment_id or row_attempt != attempt_id):
                            read_ok = False
                            continue
                        if not isinstance(measurement, str) or not re.fullmatch(
                                r"[a-z][a-z0-9_.-]{0,63}", measurement):
                            # Do not expose arbitrary source text. The public ledger
                            # is still the exact measurement interface used here.
                            measurement = "commercial_ledger"
                        quantity = usage.get("quantity")
                        if quantity is not None and (type(quantity) is not int or quantity < 0):
                            read_ok = False
                            continue
                        evidence_status = _commercial_evidence(evidence)
                        if (evidence_status is None or
                                (quantity is None and evidence not in {"unknown", "undisclosed"}) or
                                (quantity is not None and evidence in {"unknown", "undisclosed"})):
                            read_ok = False
                            continue
                        recorded_at = usage.get("recorded_at")
                        if not isinstance(recorded_at, str) or not recorded_at:
                            read_ok = False
                            continue
                        fields: dict[str, Any] = {
                            "usage_id": usage_id, "service_identity": row_service,
                            "unit": unit, "measurement_source": measurement,
                            "completeness": "complete" if quantity is not None else evidence,
                            "evidence_status": evidence_status,
                        }
                        if quantity is not None:
                            fields["quantity"] = str(quantity)
                        for key in ("model_call_id", "model_id", "reasoning_effort"):
                            value = usage.get(key)
                            if value is not None:
                                fields[key] = value
                        refs = self._safe_evidence_ref(usage.get("evidence_ref"))
                        if refs:
                            fields["evidence_refs"] = refs
                        records.append(self._record("commercial", f"usage:{usage_id}",
                            "com.exomachina.commercial.usage.v1",
                            recorded_at,
                            fields=fields, **(attribution | {
                                "assignment_id": row_assignment, "attempt_id": row_attempt})))

                    obligation = purchase.get("obligation")
                    obligations = purchase.get("obligations")
                    if not isinstance(obligations, list):
                        obligations = [obligation] if isinstance(obligation, Mapping) else []
                    economics = purchase.get("economics")
                    economics = economics if isinstance(economics, Mapping) else {}
                    current_obligation = purchase.get("obligation")
                    current_obligation_id = (current_obligation.get("obligation_id")
                        if isinstance(current_obligation, Mapping) else None)
                    for obligation_row in obligations:
                        if not isinstance(obligation_row, Mapping):
                            read_ok = False
                            continue
                        obligation_id = obligation_row.get("obligation_id")
                        obligation_time = obligation_row.get("created_at")
                        details = obligation_row.get("details")
                        details = details if isinstance(details, Mapping) else {}
                        if (not isinstance(obligation_id, str) or not obligation_id or
                                not isinstance(obligation_time, str) or not obligation_time):
                            read_ok = False
                            continue
                        components = dict(details)
                        if obligation_id == current_obligation_id:
                            components.update({key: value for key, value in economics.items()
                                               if key not in components})
                        cost_components = (
                            "inference_cost", "hosting_cost", "markup", "supplier_charge",
                            "payment_fees", "owner_overhead", "production_cost", "customer_price")
                        expected_cost_count += len(cost_components)
                        for component in cost_components:
                            money = components.get(component)
                            if not isinstance(money, Mapping):
                                continue
                            try:
                                amount, currency, evidence_status, scale = self._cost_amount(money)
                            except SourceContractError:
                                read_ok = False
                                continue
                            if (currency != purchase.get("currency") or
                                    scale != purchase.get("atomic_scale")):
                                read_ok = False
                                continue
                            cost_count += 1
                            if amount is None:
                                all_costs_known = False
                            obligation_component_id = f"{obligation_id}:{component}"
                            obligation_state = (obligation_row.get("state") or
                                                purchase.get("payment_state"))
                            if not isinstance(obligation_state, str) or not obligation_state:
                                read_ok = False
                                continue
                            fields = {
                                "obligation_id": obligation_component_id,
                                "offer_digest": purchase.get("offer_digest"),
                                "component": component,
                                "amount_atoms": amount,
                                "currency": currency,
                                "atomic_scale": scale,
                                "evidence_status": evidence_status,
                                "state": obligation_state,
                            }
                            for key in ("price_basis", "payment_trigger"):
                                value = purchase.get(key)
                                if value is not None:
                                    fields[key] = value
                            money_basis = money.get("basis")
                            if (isinstance(money_basis, Mapping) and
                                    type(money_basis.get("markup_bps")) is int and
                                    money_basis["markup_bps"] >= 0):
                                fields["markup_bps"] = money_basis["markup_bps"]
                            refs = self._safe_evidence_ref(money.get("evidence_ref"))
                            if refs:
                                fields["evidence_refs"] = refs
                            records.append(self._record("commercial",
                                f"obligation:{obligation_id}:{component}",
                                "com.exomachina.commercial.obligation.v1",
                                obligation_time,
                                fields=fields, **attribution))

                    settlements = purchase.get("settlements", [])
                    if not isinstance(settlements, list):
                        read_ok = False
                        settlements = []
                    for settlement in settlements:
                        if not isinstance(settlement, Mapping):
                            read_ok = False
                            continue
                        payment_id = settlement.get("settlement_id")
                        amount = settlement.get("amount_units")
                        currency = settlement.get("currency")
                        scale = settlement.get("atomic_scale")
                        evidence = _commercial_evidence(
                            settlement.get("evidence_state") or "unknown")
                        recorded_at = settlement.get("recorded_at")
                        if (not isinstance(payment_id, str) or not payment_id or
                                type(amount) is not int or amount < 0 or
                                not isinstance(currency, str) or not currency or
                                type(scale) is not int or scale < 0 or
                                not isinstance(recorded_at, str) or not recorded_at or
                                currency != purchase.get("currency") or
                                scale != purchase.get("atomic_scale")):
                            read_ok = False
                            continue
                        fields = {"payment_id": payment_id, "amount_atoms": amount,
                                  "currency": currency, "atomic_scale": scale,
                                  "state": settlement.get("state"),
                                  "evidence_status": evidence}
                        if not all(isinstance(value, str) and value for value in
                                   (fields["state"], fields["evidence_status"])):
                            read_ok = False
                            continue
                        # Preserve each recorded settlement revision. Do not
                        # expose the receipt reference, which may contain free text.
                        revision = _canonical({key: settlement.get(key) for key in (
                            "state", "evidence_state", "amount_units", "currency",
                            "atomic_scale", "recorded_at")})
                        revision_id = hashlib.sha256(revision.encode()).hexdigest()[:20]
                        records.append(self._record("commercial",
                            f"payment:{payment_id}:{revision_id}",
                            "com.exomachina.commercial.payment.v1",
                            recorded_at, fields=fields, **attribution))

                    credits = purchase.get("credits", [])
                    if not isinstance(credits, list):
                        read_ok = False
                        credits = []
                    for credit in credits:
                        if not isinstance(credit, Mapping):
                            read_ok = False
                            continue
                        credit_id, amount = credit.get("credit_id"), credit.get("amount_units")
                        currency, scale = credit.get("currency"), credit.get("atomic_scale")
                        evidence = _commercial_evidence(credit.get("evidence_state"))
                        recorded_at = credit.get("recorded_at")
                        if (not isinstance(credit_id, str) or not credit_id or
                                type(amount) is not int or amount < 0 or
                                not isinstance(currency, str) or not currency or
                                type(scale) is not int or scale < 0 or
                                currency != purchase.get("currency") or
                                scale != purchase.get("atomic_scale") or
                                not isinstance(recorded_at, str) or not recorded_at or
                                not isinstance(evidence, str) or not evidence):
                            read_ok = False
                            continue
                        # Credits are internal ledger adjustments, not adapter
                        # settlements; retain them as a credited payment fact.
                        fields = {"payment_id": credit_id, "amount_atoms": amount,
                                  "currency": currency, "atomic_scale": scale,
                                  "state": "credited", "evidence_status": evidence}
                        records.append(self._record("commercial", f"credit:{credit_id}",
                            "com.exomachina.commercial.payment.v1",
                            recorded_at, fields=fields, **attribution))
        except Exception:
            # A public ledger read must not block Temporal/task observation. The
            # status stays explicitly unreported; no empty list is interpreted
            # as a numeric zero.
            read_ok = False

        for record in records:
            self._insert(record)
        record_count = len(records)
        self.commercial_reporting_status = (
            "reported" if read_ok and record_count > 0 else "unreported")
        self.commercial_costs_known = (
            bool(cost_count) and cost_count == expected_cost_count and all_costs_known and read_ok)
        return purchases_seen, cost_count, self.commercial_costs_known

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.database, timeout=15, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        return db

    @property
    def factory_id(self) -> str:
        return self.director.identity

    def authorize(self, principal: object, factory_id: str) -> None:
        if factory_id != self.factory_id or principal not in ("fixture-operator", "fixture-observer"):
            raise ObservationForbidden("factory is unavailable")

    def discover(self, principal: object) -> Iterable[Mapping[str, Any]]:
        if principal not in ("fixture-operator", "fixture-observer"):
            raise ObservationForbidden("factory is unavailable")
        row: dict[str, Any] = {
            "factory_id": self.factory_id,
            "identity": self.factory_id,
            "name": self.config["name"],
            "capability": self.config["capability"]["id"],
        }
        try:
            active = self.director.module.publications.active()
        except (OSError, ValueError, KeyError):
            active = None
        if active:
            row.update(active_manifest_digest=active["manifest_digest"],
                       active_package_digest=active["package_digest"],
                       active_interpreter_build=active["build_id"])
        return [row]

    def _insert(self, record: Mapping[str, Any]) -> None:
        source_id = record.get("source_id")
        if not isinstance(source_id, str) or not source_id:
            raise SourceContractError("runtime source record has no durable identity")
        encoded = _canonical(record)
        with self._connect() as db:
            row = db.execute("SELECT record_json FROM source_records WHERE source_id=?",
                             (source_id,)).fetchone()
            if row:
                if row["record_json"] != encoded:
                    raise SourceContractError("durable runtime source record changed")
                return
            db.execute("INSERT INTO source_records(source_id,record_json) VALUES (?,?)",
                       (source_id, encoded))

    def _record(self, source_kind: str, source_id: str, event_type: str, time_value: Any,
                *, run_id: str | None = None, fields: Mapping[str, Any] | None = None,
                **common: Any) -> dict[str, Any]:
        value: dict[str, Any] = {
            "factory_id": self.factory_id, "source_kind": source_kind,
            "source_id": source_id, "event_type": event_type,
            "time": _event_time(time_value), "fields": dict(fields or {}),
        }
        if run_id is not None:
            value["run_id"] = run_id
        value.update({key: item for key, item in common.items() if item is not None})
        source_fields = fields or {}
        for key in ("task_id", "context_id", "assignment_id", "attempt_id",
                    "manifest_digest", "package_digest", "definition_digest",
                    "interpreter_build"):
            if key not in value and source_fields.get(key) is not None:
                value[key] = source_fields[key]
        return value

    def _add_factory_and_publication(self) -> None:
        identity_path = self.director.database.parent / "instance.json"
        source_id = "factory:" + self.factory_id
        # The file mtime can change for private harness settings such as the
        # loopback QA session flag. Factory discovery is an immutable fact, so
        # keep its first durable timestamp across source restarts and config
        # touches instead of turning metadata edits into conflicting history.
        with self._connect() as db:
            prior = db.execute("SELECT record_json FROM source_records WHERE source_id=?",
                               (source_id,)).fetchone()
        if prior:
            try:
                prior_time = json.loads(prior["record_json"]).get("time")
            except (ValueError, TypeError):
                prior_time = None
            if not isinstance(prior_time, str):
                raise SourceContractError("durable factory discovery record has no time")
            timestamp = prior_time
        else:
            timestamp = datetime.fromtimestamp(identity_path.stat().st_mtime, timezone.utc)
        self._insert(self._record("temporal", source_id,
            "com.exomachina.factory.discovered.v1", timestamp,
            fields={"identity": self.factory_id, "name": self.config["name"],
                    "capability": self.config["capability"]["id"]}))
        try:
            active = self.director.module.publications.active()
        except (OSError, ValueError, KeyError):
            return
        if not isinstance(active, Mapping):
            return
        active_path = self.director.module.catalog / "active-publication.json"
        stamp = active_path.stat().st_mtime_ns
        timestamp = datetime.fromtimestamp(stamp / 1_000_000_000, timezone.utc)
        package = self.director.module.package(active["package_digest"])
        closure = active["closure"]
        manifest = closure["manifest"]
        bindings = []
        for name, binding in package.get("bindings", {}).items():
            manifest_service = manifest.get("services", {}).get(name, {})
            bindings.append({"name": name, "role": binding["role"],
                             "identity": binding["identity"],
                             "contract_digest": manifest_service["contract_digest"]})
        self._insert(self._record("publication", f"active:{active['manifest_digest']}:{stamp}",
            "com.exomachina.publication.activated.v1", timestamp,
            fields={"publication_version": active.get("label", "active"),
                    "manifest_digest": active["manifest_digest"],
                    "package_digest": active["package_digest"],
                    "definition_digest": manifest["root_digest"],
                    "interpreter_build": active["build_id"],
                    "graph_nodes": _graph_nodes(package.get("root"), package.get("bindings")),
                    "service_bindings": bindings}))

    def _run_rows(self) -> list[dict[str, Any]]:
        with self.director.connect() as db:
            return [dict(row) for row in db.execute("SELECT * FROM runs ORDER BY rowid")]

    @staticmethod
    def _common_pins(start_input: Mapping[str, Any]) -> dict[str, Any]:
        closure = start_input.get("closure") or {}
        manifest = closure.get("manifest") or {}
        interpreter = manifest.get("interpreter") or {}
        return {key: value for key, value in {
            "manifest_digest": closure.get("manifest_digest"),
            "package_digest": start_input.get("package_digest"),
            "definition_digest": start_input.get("definition_digest"),
            "interpreter_build": interpreter.get("build_id"),
        }.items() if value is not None}

    @staticmethod
    def _phase_for_activity(name: str) -> str:
        return {"assign": "assignment", "typed_join": "join", "synthesize": "synthesis",
                "review": "quality", "release": "delivery"}.get(name, "activity")

    def _history_records(self, workflow_id: str, events: list[dict[str, Any]],
                         task_id: str | None, context_id: str | None) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        scheduled: dict[int, dict[str, Any]] = {}
        started_by_event_id: dict[int, list[dict[str, Any]]] = {}
        for candidate in events:
            if candidate.get("event_type") != "ACTIVITY_TASK_STARTED":
                continue
            candidate_id = candidate.get("event_id")
            if type(candidate_id) is not int:
                continue
            candidate_attributes = candidate.get("attributes") or {}
            started_by_event_id.setdefault(candidate_id, []).append({
                "scheduled_event_id": candidate_attributes.get("scheduled_event_id"),
                "attempt_id": _temporal_attempt(candidate_attributes.get("attempt")),
                "started_at": candidate.get("time"),
            })
        start_input: dict[str, Any] = {}
        pins: dict[str, Any] = {}
        definition: dict[str, Any] = {}
        package: dict[str, Any] = {}

        def append_assignment_node_link(event_id: Any, time_value: Any,
                                        activity_input: Mapping[str, Any],
                                        assignment_id: str,
                                        attempt_id: str | None,
                                        original_fields: Mapping[str, Any]) -> None:
            """Append a versioned correction only for an explicit pinned node."""
            node = activity_input.get("node")
            if (type(event_id) is not int or event_id <= 0 or
                    not isinstance(node, str) or
                    not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}", node) or
                    node not in {item["id"] for item in _graph_nodes(definition)}):
                return
            records.append(self._record(
                "temporal",
                f"{_ASSIGNMENT_NODE_LINK_CORRECTION_PREFIX}{workflow_id}:{event_id}:assignment",
                "com.exomachina.assignment.state_changed.v1", time_value,
                run_id=workflow_id, fields={**original_fields, "node": node}, task_id=task_id,
                assignment_id=assignment_id, attempt_id=attempt_id))

        def append_run_node_link(event_id: Any, time_value: Any,
                                 activity_input: Mapping[str, Any],
                                 original_fields: Mapping[str, Any]) -> None:
            """Append an exact pinned-graph node correction for one activity event."""
            node = activity_input.get("node")
            if (type(event_id) is not int or event_id <= 0 or
                    not isinstance(node, str) or
                    not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}", node) or
                    node not in {item["id"] for item in _graph_nodes(definition)}):
                return
            records.append(self._record(
                "temporal",
                f"{_RUN_NODE_LINK_CORRECTION_PREFIX}{workflow_id}:{event_id}:run",
                "com.exomachina.run.state_changed.v1", time_value,
                run_id=workflow_id,
                fields={**original_fields, "node": node}, task_id=task_id))

        def handoff_node(activity_input: Mapping[str, Any]) -> str | None:
            node = activity_input.get("node")
            if (isinstance(node, str) and
                    re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}", node) and
                    node in {item["id"] for item in _graph_nodes(definition)}):
                return node
            return None

        def handoff_assignment(activity_input: Mapping[str, Any], item: Mapping[str, Any],
                               scheduled_id: Any) -> str:
            # Research assignments keep the identity their assignment facts use.
            return str(activity_input.get("instance") or activity_input.get("assignment_id")
                       or item.get("activity_id") or scheduled_id)

        def append_handoff_consumed(event_id: Any, time_value: Any,
                                    activity_input: Mapping[str, Any], item: Mapping[str, Any],
                                    scheduled_id: Any, attempt_id: str | None) -> None:
            """Before dispatch: the hand-offs this Task was composed from."""
            consumes = activity_input.get("consumes")
            node = handoff_node(activity_input)
            if not consumes or not isinstance(consumes, list) or node is None or attempt_id is None:
                return
            records.append(self._record(
                "temporal", f"{workflow_id}:{event_id}:handoff-consumed",
                "com.exomachina.handoff.consumed.v1", time_value, run_id=workflow_id,
                fields={"node": node, "consumed_at": _event_time(time_value), "inputs": consumes},
                assignment_id=handoff_assignment(activity_input, item, scheduled_id),
                attempt_id=attempt_id))

        def append_handoff_produced(event_id: Any, activity_input: Mapping[str, Any],
                                    item: Mapping[str, Any], scheduled_id: Any,
                                    attempt_id: str | None, result: Any) -> None:
            """On complete: the content-free record the Activity hook returned."""
            produced = result.get("handoff") if isinstance(result, Mapping) else None
            node = handoff_node(activity_input)
            if (not isinstance(produced, Mapping) or node is None or attempt_id is None
                    or not isinstance(produced.get("handoff_id"), str)):
                return
            assignment_id = handoff_assignment(activity_input, item, scheduled_id)
            for ready in produced.get("ready") or []:
                if isinstance(ready, Mapping):
                    records.append(self._record(
                        "temporal",
                        f"{workflow_id}:{event_id}:handoff-ready:{ready.get('item_index')}",
                        "com.exomachina.handoff.item_ready.v1", ready.get("ready_at"),
                        run_id=workflow_id,
                        fields={"node": node, "handoff_id": produced["handoff_id"],
                                **{key: ready.get(key) for key in
                                   ("item_index", "part_kinds", "media_type", "ready_at")}},
                        assignment_id=assignment_id, attempt_id=attempt_id))
            records.append(self._record(
                "temporal", f"{workflow_id}:{event_id}:handoff-produced",
                "com.exomachina.handoff.produced.v1", produced.get("produced_at"),
                run_id=workflow_id,
                fields={"node": node, **{key: produced.get(key) for key in
                        ("handoff_id", "handoff_revision", "produced_at", "items")}},
                assignment_id=assignment_id, attempt_id=attempt_id))

        def completed_attempt(attributes: Mapping[str, Any], scheduled_id: Any) -> str | None:
            started_event_id = attributes.get("started_event_id")
            linked = (started_by_event_id.get(started_event_id, [])
                      if type(started_event_id) is int else [])
            if (len(linked) == 1 and linked[0].get("scheduled_event_id") == scheduled_id and
                    isinstance(linked[0].get("attempt_id"), str)):
                return linked[0]["attempt_id"]
            return None

        for event in events:
            event_id = event.get("event_id")
            event_type = event.get("event_type")
            at = event.get("time")
            attributes = event.get("attributes") or {}
            if event_type == "WORKFLOW_EXECUTION_STARTED":
                start_input = attributes.get("input") or {}
                if not isinstance(start_input, dict):
                    start_input = {}
                pins = self._common_pins(start_input)
                definition = start_input.get("document") or {}
                package = start_input.get("package") or {}
                fields = {**pins, "state": "working", "started_at": at,
                          "graph_nodes": _graph_nodes(definition, package.get("bindings"))}
                records.append(self._record("temporal", f"{workflow_id}:{event_id}",
                    "com.exomachina.run.created.v1", at, run_id=workflow_id,
                    fields=fields, task_id=task_id))
                continue
            if event_type == "ACTIVITY_TASK_SCHEDULED":
                data = attributes.get("input") or {}
                name = attributes.get("activity_type") or "activity"
                scheduled[int(event_id)] = {"name": name, "input": data,
                                             "activity_id": attributes.get("activity_id")}
                phase = self._phase_for_activity(name)
                fields = {**pins, "state": "working", "phase": phase,
                          "node": attributes.get("activity_id") or str(event_id)}
                records.append(self._record("temporal", f"{workflow_id}:{event_id}",
                    "com.exomachina.run.state_changed.v1", at, run_id=workflow_id,
                    fields=fields, task_id=task_id))
                append_run_node_link(event_id, at, data, fields)
                if name == "assign":
                    instance = data.get("instance") or attributes.get("activity_id") or str(event_id)
                    binding = data.get("binding") or {}
                    fields = {"capability": data.get("capability") or "research",
                              "provider_identity": binding.get("identity") or "unknown-provider",
                              "state": "scheduled", "queue_position": 0}
                    records.append(self._record("temporal", f"{workflow_id}:{event_id}:assignment",
                        "com.exomachina.assignment.state_changed.v1", at, run_id=workflow_id,
                        fields=fields, task_id=task_id, assignment_id=str(instance),
                        attempt_id="1"))
                    append_assignment_node_link(
                        event_id, at, data, str(instance), "1", fields)
                continue
            if event_type == "ACTIVITY_TASK_STARTED":
                scheduled_id = attributes.get("scheduled_event_id")
                item = scheduled.get(scheduled_id)
                if item:
                    activity_fields = {**pins, "state": "working",
                                       "phase": self._phase_for_activity(item["name"])}
                    append_run_node_link(event_id, at, item["input"], activity_fields)
                attempt_id = _temporal_attempt(attributes.get("attempt"))
                if item:
                    append_handoff_consumed(event_id, at, item["input"], item,
                                            scheduled_id, attempt_id)
                if item and item["name"] == "assign" and attempt_id is not None:
                    data = item["input"]
                    binding = data.get("binding") or {}
                    fields = {"capability": data.get("capability") or "research",
                              "provider_identity": binding.get("identity") or "unknown-provider",
                              "state": "running", "started_at": at}
                    records.append(self._record("temporal", f"{workflow_id}:{event_id}:assignment",
                        "com.exomachina.assignment.state_changed.v1", at, run_id=workflow_id,
                        fields=fields, task_id=task_id,
                        assignment_id=str(data.get("instance") or item["activity_id"] or scheduled_id),
                        attempt_id=attempt_id))
                    append_assignment_node_link(
                        event_id, at, data,
                        str(data.get("instance") or item["activity_id"] or scheduled_id),
                        attempt_id, fields)
                continue
            if event_type == "ACTIVITY_TASK_COMPLETED":
                scheduled_id = attributes.get("scheduled_event_id")
                item = scheduled.get(scheduled_id)
                if not item:
                    continue
                data, result = item["input"], attributes.get("result") or {}
                name = item["name"]
                if name in {"assign", "synthesize", "release"}:
                    append_handoff_produced(event_id, data, item, scheduled_id,
                                            completed_attempt(attributes, scheduled_id), result)
                if name == "assign":
                    assignment_id = str(data.get("instance") or item["activity_id"] or scheduled_id)
                    legacy_source_id = f"{workflow_id}:{event_id}:assignment"
                    completion_time = _event_time(at)
                    started_event_id = attributes.get("started_event_id")
                    linked_starts = (started_by_event_id.get(started_event_id, [])
                                     if type(started_event_id) is int else [])
                    attempt_id = None
                    linked_started_at = None
                    if len(linked_starts) == 1:
                        linked_start = linked_starts[0]
                        linked_attempt_id = linked_start.get("attempt_id")
                        if (linked_start.get("scheduled_event_id") == scheduled_id and
                                isinstance(linked_attempt_id, str)):
                            attempt_id = linked_attempt_id
                            linked_started_at = linked_start.get("started_at")

                    prior_row = None
                    if attempt_id != "1":
                        with self._connect() as db:
                            prior_row = db.execute(
                                "SELECT record_json FROM source_records WHERE source_id=?",
                                (legacy_source_id,)).fetchone()
                    prior = None
                    if prior_row is not None:
                        try:
                            prior = json.loads(prior_row["record_json"])
                        except (ValueError, TypeError) as error:
                            raise SourceContractError(
                                "persisted Temporal completion record is invalid") from error
                    prior_fields = prior.get("fields") if isinstance(prior, Mapping) else None
                    prior_is_legacy_completion = (
                        isinstance(prior, Mapping) and
                        prior.get("source_kind") == "temporal" and
                        prior.get("source_id") == legacy_source_id and
                        prior.get("event_type") ==
                        "com.exomachina.assignment.state_changed.v1" and
                        prior.get("run_id") == workflow_id and
                        prior.get("time") == completion_time and
                        prior.get("assignment_id") == assignment_id and
                        prior.get("attempt_id") == "1" and
                        isinstance(prior_fields, Mapping) and
                        prior_fields.get("state") == "completed" and
                        prior_fields.get("ended_at") == completion_time)
                    if attempt_id != "1" and prior_is_legacy_completion:
                        correction_fields = {"state": "unknown"}
                        for field in ("node", "capability", "provider_identity"):
                            value = prior_fields.get(field)
                            if isinstance(value, str):
                                correction_fields[field] = value
                        correction_id = (
                            f"{_ASSIGNMENT_LINK_CORRECTION_PREFIX}{workflow_id}:"
                            f"{event_id}:assignment:attempt-1")
                        records.append(self._record(
                            "temporal", correction_id,
                            "com.exomachina.assignment.state_changed.v1", at,
                            run_id=workflow_id, fields=correction_fields,
                            task_id=task_id, assignment_id=assignment_id,
                            attempt_id="1"))
                    if attempt_id is None:
                        continue
                    binding = data.get("binding") or {}
                    fields = {"capability": data.get("capability") or "research",
                              "provider_identity": binding.get("identity") or "unknown-provider",
                              "state": "completed", "ended_at": at}
                    completion_source_id = (
                        legacy_source_id if attempt_id == "1" else
                        f"{legacy_source_id}:attempt-{attempt_id}")
                    records.append(self._record(
                        "temporal", completion_source_id,
                        "com.exomachina.assignment.state_changed.v1", at,
                        run_id=workflow_id, fields=fields, task_id=task_id,
                        assignment_id=assignment_id, attempt_id=attempt_id))
                    node_link_fields = dict(fields)
                    if isinstance(linked_started_at, str):
                        node_link_fields["started_at"] = _event_time(linked_started_at)
                    append_assignment_node_link(
                        event_id, at, data, assignment_id, attempt_id, node_link_fields)
                elif name == "synthesize" and isinstance(result, Mapping):
                    content = result.get("content")
                    digest = result.get("sha256")
                    revision = result.get("revision")
                    if isinstance(content, str) and isinstance(digest, str) and isinstance(revision, str):
                        content_bytes = content.encode("utf-8")
                        if hashlib.sha256(content_bytes).hexdigest() == digest:
                            self._store_artifact(workflow_id, revision, digest, content_bytes,
                                                 accepted=False)
                            author = data.get("identity") or "unknown-author"
                            fields = {**pins, "artifact_revision": revision,
                                      "artifact_sha256": digest, "media_type": "application/json",
                                      "byte_length": len(content_bytes),
                                      "author_identity": author}
                            records.append(self._record("artifact", f"{workflow_id}:{event_id}:artifact",
                                "com.exomachina.artifact.revised.v1", at, run_id=workflow_id,
                                fields=fields, task_id=task_id))
                elif name == "review" and isinstance(result, Mapping):
                    candidate = data.get("candidate") or result.get("candidate") or {}
                    verdict = result.get("artifact")
                    verdict = verdict if isinstance(verdict, Mapping) else result
                    findings = verdict.get("findings")
                    binding = data.get("binding") or {}
                    fields = {"artifact_revision": candidate.get("revision"),
                              "artifact_sha256": candidate.get("sha256"),
                              "reviewer_identity": (binding.get("identity") or
                                                    data.get("identity") or "unknown-reviewer"),
                              "accepted": verdict.get("accepted"),
                              "finding_count": len(findings)}
                    if (all(fields.get(key) is not None for key in
                            ("artifact_revision", "artifact_sha256", "reviewer_identity")) and
                            type(fields["accepted"]) is bool and isinstance(findings, list)):
                        records.append(self._record("temporal", f"{workflow_id}:{event_id}:quality",
                            "com.exomachina.quality.verdict.v1", at, run_id=workflow_id,
                            fields=fields, task_id=task_id))
                elif (name == "release" and isinstance(result, Mapping)
                        and isinstance(result.get("message_id"), str)):
                    # A2A release receipt: the node's evidence, from the
                    # receiver's own receipt over the exact delivered bytes.
                    command = data.get("command") or {}
                    fields = {"receipt_id": result.get("receipt_id"),
                              "artifact_revision": result.get("revision"),
                              "artifact_sha256": result.get("sha256"),
                              "destination_id": result.get("destination_identity"),
                              "delivered_at": result.get("accepted_at"),
                              "outcome": result.get("outcome")}
                    if ("unresolved" not in result and all(
                            isinstance(value, str) and value for value in fields.values())
                            and fields["artifact_sha256"] == command.get("sha256")
                            and fields["artifact_revision"] == command.get("revision")):
                        records.append(self._record("outcome", f"{workflow_id}:{event_id}:delivery",
                            "com.exomachina.delivery.receipt.v1", fields["delivered_at"],
                            run_id=workflow_id, fields=fields, task_id=task_id))
                elif name == "release" and isinstance(result, Mapping) and "unresolved" not in result:
                    # Histories from before the A2A release agent.
                    command = data.get("command") or {}
                    receipt_id = result.get("receipt_id") or result.get("release_id")
                    if isinstance(receipt_id, str):
                        fields = {"receipt_id": receipt_id,
                                  "artifact_revision": command.get("revision"),
                                  "artifact_sha256": command.get("sha256"),
                                  "destination_id": data.get("identity") or "fixture-receiver",
                                  "delivered_at": at, "outcome": "fixture-received"}
                        records.append(self._record("outcome", f"{workflow_id}:{event_id}:delivery",
                            "com.exomachina.delivery.receipt.v1", at, run_id=workflow_id,
                            fields=fields, task_id=task_id))
                continue
            if event_type == "TIMER_STARTED":
                seconds = attributes.get("timeout_seconds")
                if isinstance(seconds, (int, float)) and seconds >= 0:
                    deadline = datetime.fromisoformat(_event_time(at).replace("Z", "+00:00")) + timedelta(seconds=seconds)
                    fields = {**pins, "state": "input-required", "phase": "awaiting-director",
                              "wait_deadline": _iso(deadline)}
                    records.append(self._record("temporal", f"{workflow_id}:{event_id}:director-wait",
                        "com.exomachina.run.state_changed.v1", at, run_id=workflow_id,
                        fields=fields, task_id=task_id))
                continue
            if event_type == "WORKFLOW_EXECUTION_COMPLETED":
                result = attributes.get("result") or {}
                state = "completed"
                phase = result.get("status", "completed") if isinstance(result, Mapping) else "completed"
                fields = {**pins, "state": state, "phase": phase, "ended_at": at}
                records.append(self._record("temporal", f"{workflow_id}:{event_id}:closed",
                    "com.exomachina.run.state_changed.v1", at, run_id=workflow_id,
                    fields=fields, task_id=task_id))
                if isinstance(result, Mapping):
                    artifact = result.get("artifact")
                    if isinstance(artifact, Mapping):
                        self._record_accepted_artifact(workflow_id, event_id, at, artifact,
                                                        task_id, context_id, pins, records)
                    receipt = result.get("receipt")
                    if isinstance(receipt, Mapping):
                        artifact = artifact or {}
                        receipt_id = receipt.get("receipt_id") or receipt.get("release_id")
                        if isinstance(receipt_id, str):
                            records.append(self._record("outcome", f"{workflow_id}:{event_id}:delivery-final",
                                "com.exomachina.delivery.receipt.v1", at, run_id=workflow_id,
                                fields={"receipt_id": receipt_id,
                                    "artifact_revision": artifact.get("revision") or receipt.get("revision"),
                                    "artifact_sha256": artifact.get("sha256") or receipt.get("sha256"),
                                    "destination_id": "fixture-receiver", "delivered_at": at,
                                    "outcome": "fixture-received"}, task_id=task_id))
                continue
            if event_type in {"WORKFLOW_EXECUTION_FAILED", "WORKFLOW_EXECUTION_TIMED_OUT",
                              "WORKFLOW_EXECUTION_CANCELED", "WORKFLOW_EXECUTION_TERMINATED"}:
                fields = {**pins, "state": "failed", "phase": "execution-closed",
                          "ended_at": at}
                records.append(self._record("temporal", f"{workflow_id}:{event_id}:closed",
                    "com.exomachina.run.state_changed.v1", at, run_id=workflow_id,
                    fields=fields, task_id=task_id))
        # Every child Workflow uses its root A2A binding. Keep only those two
        # identifiers in the projection; Task messages and Workflow inputs are
        # never copied into source facts.
        for record in records:
            if task_id is not None:
                record["task_id"] = task_id
            if context_id is not None:
                record["context_id"] = context_id
        return records

    def _record_accepted_artifact(self, run_id: str, event_id: Any, at: Any,
                                  artifact: Mapping[str, Any], task_id: str | None,
                                  context_id: str | None,
                                  pins: Mapping[str, Any], records: list[dict[str, Any]]) -> None:
        content, revision, digest = (artifact.get("content"), artifact.get("revision"),
                                     artifact.get("sha256"))
        if not (isinstance(content, str) and isinstance(revision, str) and isinstance(digest, str)):
            return
        content_bytes = content.encode("utf-8")
        if hashlib.sha256(content_bytes).hexdigest() != digest:
            raise SourceContractError("accepted report history bytes do not match its digest")
        self._store_artifact(run_id, revision, digest, content_bytes, accepted=True)
        fields = {**pins, "artifact_revision": revision, "artifact_sha256": digest,
                  "media_type": "application/json", "byte_length": len(content_bytes),
                  "author_identity": "verified-research"}
        records.append(self._record("artifact", f"{run_id}:{event_id}:accepted-artifact",
            "com.exomachina.artifact.revised.v1", at, run_id=run_id,
            fields=fields, task_id=task_id, context_id=context_id))

    def _store_artifact(self, run_id: str, revision: str, digest: str, content: bytes,
                        *, accepted: bool) -> None:
        with self._connect() as db:
            db.execute("CREATE TABLE IF NOT EXISTS runtime_artifacts ("
                       "run_id TEXT NOT NULL, revision TEXT NOT NULL, sha256 TEXT NOT NULL, "
                       "content BLOB NOT NULL, accepted INTEGER NOT NULL, "
                       "PRIMARY KEY(run_id,revision,sha256))")
            row = db.execute("SELECT content, accepted FROM runtime_artifacts "
                             "WHERE run_id=? AND revision=? AND sha256=?",
                             (run_id, revision, digest)).fetchone()
            if row and (bytes(row["content"]) != content or (accepted and not row["accepted"])):
                if bytes(row["content"]) != content:
                    raise SourceContractError("artifact history changed for a pinned digest")
                db.execute("UPDATE runtime_artifacts SET accepted=1 WHERE run_id=? AND revision=? AND sha256=?",
                           (run_id, revision, digest))
            else:
                db.execute("INSERT OR IGNORE INTO runtime_artifacts VALUES (?,?,?,?,?)",
                           (run_id, revision, digest, content, int(accepted)))

    def _local_delivery_records(self, rows: list[dict[str, Any]]) -> None:
        """Append configured local receipts only when they match accepted bytes.

        Receipts are read through LocalDelivery's public projection. The root
        Director run row supplies the original Task/context binding; a receipt
        for a child or an unrelated binding is not reinterpreted as root work.
        """
        if self.delivery_reader is None:
            return
        root_bindings = {
            row["run_id"]: (row.get("task_id"), row.get("context_id"))
            for row in rows
        }
        for run_id, (task_id, context_id) in root_bindings.items():
            if not isinstance(task_id, str) or not task_id or not isinstance(context_id, str) or not context_id:
                continue
            receipts = self.delivery_reader.list_receipts(run_id=run_id)
            if not isinstance(receipts, list):
                raise SourceContractError("local delivery reader returned invalid receipts")
            for receipt in receipts:
                if not isinstance(receipt, Mapping):
                    raise SourceContractError("local delivery reader returned an invalid receipt")
                # Scope and binding are authority checks, not values inferred
                # from receipt IDs or run/action strings.
                if (receipt.get("factory_id") != self.factory_id or
                        receipt.get("run_id") != run_id or
                        receipt.get("task_id") != task_id or
                        receipt.get("context_id") != context_id):
                    continue
                receipt_id = receipt.get("receipt_id")
                revision = receipt.get("artifact_revision")
                digest = receipt.get("artifact_sha256")
                markdown_digest = receipt.get("markdown_sha256")
                destination_identity = receipt.get("destination_identity")
                delivered_at = receipt.get("recorded_at")
                byte_length = receipt.get("byte_length")
                if (not isinstance(receipt_id, str) or not receipt_id or
                        not isinstance(revision, str) or not revision or
                        not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest) or
                        not isinstance(markdown_digest, str) or
                        not re.fullmatch(r"[a-f0-9]{64}", markdown_digest) or
                        not isinstance(destination_identity, str) or not destination_identity or
                        receipt.get("delivery_kind") != "local_file" or
                        receipt.get("state") != "delivered" or
                        type(byte_length) is not int or byte_length < 0 or
                        not isinstance(delivered_at, str)):
                    continue
                try:
                    _event_time(delivered_at)
                except (SourceContractError, ValueError):
                    continue
                with self._connect() as db:
                    table = db.execute(
                        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='runtime_artifacts'").fetchone()
                    artifact_row = (db.execute(
                        "SELECT content,accepted FROM runtime_artifacts "
                        "WHERE run_id=? AND revision=? AND sha256=?",
                        (run_id, revision, digest)).fetchone() if table else None)
                if artifact_row is None or not artifact_row["accepted"]:
                    continue
                content = bytes(artifact_row["content"])
                if hashlib.sha256(content).hexdigest() != digest or len(content) == 0:
                    continue
                try:
                    report = json.loads(content)
                except (ValueError, UnicodeDecodeError):
                    continue
                markdown = report.get("markdown") if isinstance(report, Mapping) else None
                if (not isinstance(markdown, str) or
                        hashlib.sha256(markdown.encode("utf-8")).hexdigest() != markdown_digest or
                        len(markdown.encode("utf-8")) != byte_length):
                    continue
                fields = {
                    "receipt_id": receipt_id,
                    "artifact_revision": revision,
                    "artifact_sha256": digest,
                    "markdown_sha256": markdown_digest,
                    "destination_identity": destination_identity,
                    "delivery_kind": "local_file",
                    "byte_length": byte_length,
                    "delivered_at": delivered_at,
                    "outcome": "local-file-delivered",
                }
                self._insert(self._record(
                    "outcome", f"local-delivery:{receipt_id}",
                    "com.exomachina.delivery.receipt.v1", delivered_at,
                    run_id=run_id, fields=fields, task_id=task_id, context_id=context_id))

    def _operations_records(self, principal: object,
                            run_bindings: Mapping[str, tuple[str, str]]) -> None:
        """Append projected public incident facts for exact owned run bindings.

        Runtime injects a factory-bound callable that authenticates through
        FactoryOperations and delegates to ``project_operations_incidents``.
        This source does not open Operations storage, discover incidents, or
        derive run, Task, context, or publication pins.
        """
        if self.operations_reader is None:
            return
        records = self.operations_reader(
            principal, factory_id=self.factory_id,
            limit=_OPERATIONS_INCIDENT_LIMIT)
        if not isinstance(records, list) or len(records) > _OPERATIONS_INCIDENT_LIMIT:
            raise SourceContractError("Operations reader returned invalid incident facts")

        for fact in records:
            if not isinstance(fact, Mapping):
                raise SourceContractError("Operations reader returned an invalid incident fact")
            allowed_keys = {
                "source_kind", "source_id", "factory_id", "run_id", "time",
                "event_type", "fields", "task_id", "context_id",
            }
            if set(fact) - allowed_keys:
                raise SourceContractError("Operations incident fact has undeclared source fields")
            if (fact.get("source_kind") != "incident" or
                    fact.get("event_type") != "com.exomachina.incident.state_changed.v1"):
                raise SourceContractError("Operations reader returned a non-incident fact")
            if fact.get("factory_id") != self.factory_id:
                raise SourceContractError("Operations incident belongs to a different factory")

            run_id = fact.get("run_id")
            if not isinstance(run_id, str) or run_id not in run_bindings:
                raise SourceContractError("Operations incident run is not an owned Runtime run")
            binding = run_bindings[run_id]
            if not isinstance(binding, tuple) or len(binding) != 2:
                raise SourceContractError("Runtime run has an invalid original Task binding")
            task_id, context_id = binding
            if (not isinstance(task_id, str) or not task_id or
                    not isinstance(context_id, str) or not context_id):
                raise SourceContractError("owned Runtime run has no original Task/context binding")
            for key, expected in (("task_id", task_id), ("context_id", context_id)):
                if key in fact and fact[key] != expected:
                    raise SourceContractError("Operations incident Task binding conflicts with Runtime")

            fields = fact.get("fields")
            required_fields = {"incident_id", "kind", "state", "evidence_refs"}
            allowed_fields = required_fields | {"owner_identity"}
            if (not isinstance(fields, Mapping) or
                    not required_fields <= set(fields) or set(fields) - allowed_fields):
                raise SourceContractError("Operations incident fields do not match the public projection")
            incident_id = fields.get("incident_id")
            source_id = fact.get("source_id")
            if not isinstance(incident_id, str) or not isinstance(source_id, str):
                raise SourceContractError("Operations incident identity is missing")
            source_identity = source_id.rsplit(":v", 1)
            version_text = source_identity[1] if len(source_identity) == 2 else ""
            if (len(source_identity) != 2 or source_identity[0] != incident_id or
                    not version_text or len(version_text) > 19 or
                    any(character < "0" or character > "9" for character in version_text) or
                    version_text.startswith("0") or int(version_text) > (1 << 63) - 1):
                raise SourceContractError("Operations incident source ID must preserve its row version")

            # Validate against the shared event allowlist without using its
            # normalized envelope as storage: _insert keeps the exact writer
            # timestamp and source ID returned by the Operations projector.
            persisted = dict(fact)
            persisted["task_id"] = task_id
            persisted["context_id"] = context_id
            project_source_record(persisted, self.factory_id)
            self._insert(persisted)

    def _public_wait_records(self, rows: list[dict[str, Any]],
                             binding_by_run: Mapping[str, tuple[str, str]]) -> None:
        """Append current public wait facts for exact Temporal Task bindings.

        Current artifact refs are retained only as private source-journal
        consistency metadata. They are not projected into the dashboard
        event. A later inspector result that changes a known candidate during
        the same run/phase/wait-start epoch fails closed.
        """
        for record, observed_candidate in project_public_wait_records(
                self.director, self.factory_id, rows, binding_by_run):
            source_id = record["source_id"]
            with self._connect() as db:
                prior_row = db.execute(
                    "SELECT record_json FROM source_records WHERE source_id=?",
                    (source_id,)).fetchone()
            prior_candidate: dict[str, str] = {}
            if prior_row is not None:
                try:
                    prior_record = json.loads(prior_row["record_json"])
                except (ValueError, TypeError) as error:
                    raise SourceContractError("persisted wait source record is invalid") from error
                value = prior_record.get("_wait_candidate", {})
                if (not isinstance(value, Mapping) or set(value) -
                        {"current_revision", "current_sha256"} or
                        not all(isinstance(item, str) for item in value.values())):
                    raise SourceContractError("persisted wait candidate metadata is invalid")
                prior_candidate = dict(value)

            for key, value in observed_candidate.items():
                previous = prior_candidate.get(key)
                if previous is not None and previous != value:
                    raise SourceContractError(
                        "wait candidate changed during the same writer wait epoch")
            if prior_row is None and observed_candidate:
                # Candidate metadata is captured only with the first source
                # row. Later queries can reject conflicts with known refs, but
                # cannot fill a ref that was unknown at initial observation.
                record["_wait_candidate"] = dict(observed_candidate)
            elif prior_candidate:
                record["_wait_candidate"] = prior_candidate

            project_source_record(record, self.factory_id)
            self._insert(record)

    def _durable_temporal_bindings(self) -> dict[str, tuple[str, str]]:
        """Read this writer's immutable run-created identity facts, never parse IDs."""
        with self._connect() as db:
            rows = db.execute("SELECT DISTINCT json_extract(record_json, '$.run_id') AS run_id, "
                              "json_extract(record_json, '$.task_id') AS task_id, "
                              "json_extract(record_json, '$.context_id') AS context_id "
                              "FROM source_records WHERE json_extract(record_json, '$.source_kind')='temporal' "
                              "AND json_extract(record_json, '$.event_type')='com.exomachina.run.created.v1' "
                              "LIMIT 257").fetchall()
        if len(rows) > 256:
            raise SourceContractError("durable run coverage exceeds bound")
        bindings = {}
        for row in rows:
            if not all(isinstance(row[key], str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}", row[key])
                       for key in ("run_id", "task_id", "context_id")):
                continue  # Unbound historical facts cannot establish scope authority.
            binding = (row["task_id"], row["context_id"])
            if row["run_id"] in bindings and bindings[row["run_id"]] != binding:
                raise SourceContractError("conflicting durable run identity")
            bindings[row["run_id"]] = binding
        return bindings

    async def _fetch_histories(self) -> list[tuple[str, list[dict[str, Any]]]]:
        from temporalio.api.enums.v1 import EventType
        from temporalio.service import RPCError, RPCStatusCode

        missing: set[str] = set()
        client = await self.director.client()
        pending = list(dict.fromkeys([row["run_id"] for row in self._run_rows()] + list(self._durable_temporal_bindings())))
        visited: set[str] = set()
        result: list[tuple[str, list[dict[str, Any]]]] = []
        converter = client.data_converter.payload_converter

        def decode(payloads) -> Any:
            if not payloads:
                return None
            values = converter.from_payloads(list(payloads))
            return values[0] if len(values) == 1 else values

        while pending:
            workflow_id = pending.pop(0)
            if workflow_id in visited:
                continue
            visited.add(workflow_id)
            try:
                history = await client.get_workflow_handle(workflow_id).fetch_history()
            except RPCError as error:
                if error.status != RPCStatusCode.NOT_FOUND:
                    raise
                missing.add(workflow_id)
                continue
            events: list[dict[str, Any]] = []
            for event in history.events:
                event_name = EventType.Name(event.event_type).removeprefix("EVENT_TYPE_")
                stamp = event.event_time.ToDatetime(tzinfo=timezone.utc)
                attrs: dict[str, Any] = {}
                if event_name == "WORKFLOW_EXECUTION_STARTED":
                    raw = event.workflow_execution_started_event_attributes
                    attrs["input"] = decode(raw.input.payloads) if raw.input else None
                elif event_name == "ACTIVITY_TASK_SCHEDULED":
                    raw = event.activity_task_scheduled_event_attributes
                    attrs["activity_id"] = raw.activity_id
                    attrs["activity_type"] = raw.activity_type.name
                    attrs["input"] = decode(raw.input.payloads)
                elif event_name == "ACTIVITY_TASK_STARTED":
                    raw = event.activity_task_started_event_attributes
                    attrs["scheduled_event_id"] = raw.scheduled_event_id
                    attrs["attempt"] = raw.attempt
                elif event_name == "ACTIVITY_TASK_COMPLETED":
                    raw = event.activity_task_completed_event_attributes
                    attrs["scheduled_event_id"] = raw.scheduled_event_id
                    attrs["started_event_id"] = raw.started_event_id
                    attrs["result"] = decode(raw.result.payloads) if raw.result else None
                elif event_name == "TIMER_STARTED":
                    raw = event.timer_started_event_attributes
                    duration = raw.start_to_fire_timeout
                    attrs["timeout_seconds"] = duration.seconds + duration.nanos / 1_000_000_000
                elif event_name == "WORKFLOW_EXECUTION_COMPLETED":
                    raw = event.workflow_execution_completed_event_attributes
                    attrs["result"] = decode(raw.result.payloads) if raw.result else None
                elif event_name == "START_CHILD_WORKFLOW_EXECUTION_INITIATED":
                    raw = event.start_child_workflow_execution_initiated_event_attributes
                    attrs["workflow_id"] = raw.workflow_id
                    if raw.workflow_id:
                        pending.append(raw.workflow_id)
                events.append({"event_id": event.event_id, "event_type": event_name,
                               "time": _iso(stamp), "attributes": attrs})
            result.append((workflow_id, events))
        self._unavailable_history_ids = missing
        return result

    def _local_outcome_records(self, run_rows: list[dict[str, Any]]) -> None:
        for row in run_rows:
            outcome_json = row.get("outcome_json")
            if not outcome_json:
                continue
            try:
                projection = json.loads(outcome_json)
            except (ValueError, TypeError):
                continue
            result = projection.get("result") or {}
            artifact = result.get("artifact") if isinstance(result, Mapping) else None
            if not isinstance(artifact, Mapping):
                child = result.get("child") if isinstance(result, Mapping) else None
                artifact = child.get("artifact") if isinstance(child, Mapping) else None
            if isinstance(artifact, Mapping):
                try:
                    publication = self.director.module.publications.get(row["manifest_digest"])
                except (OSError, ValueError, KeyError):
                    continue
                source_id = f"{row['run_id']}:local-outcome:accepted-artifact"
                with self._connect() as db:
                    prior = db.execute("SELECT record_json FROM source_records WHERE source_id=?",
                                       (source_id,)).fetchone()
                if prior:
                    # The local outcome is a fallback for installations whose
                    # Temporal history is temporarily unavailable. Keep its
                    # first durable observation time across harness restarts;
                    # the Director database mtime changes for unrelated writes.
                    try:
                        prior_time = json.loads(prior["record_json"]).get("time")
                    except (ValueError, TypeError):
                        prior_time = None
                    if not isinstance(prior_time, str) or not prior_time:
                        raise SourceContractError("persisted local outcome has no stable time")
                    at = prior_time
                else:
                    at = datetime.fromtimestamp(
                        self.director.database.stat().st_mtime, timezone.utc)
                pins = {"manifest_digest": row["manifest_digest"],
                        "package_digest": row["package_digest"],
                        "definition_digest": publication["closure"]["manifest"]["root_digest"],
                        "interpreter_build": row["build_id"]}
                records: list[dict[str, Any]] = []
                self._record_accepted_artifact(row["run_id"], "local-outcome", at,
                    artifact, row["task_id"], row["context_id"], pins, records)
                for record in records:
                    self._insert(record)

    def _refresh_records(self, principal: object) -> dict[str, Any]:
        self._add_factory_and_publication()
        rows = self._run_rows()
        histories: list[tuple[str, list[dict[str, Any]]]] = []
        history_read = False
        self._history_read_ids = set()
        self._history_children = {}
        # Temporal remains the source of truth even when this install's worker
        # process is stopped. Try the configured client independently.
        if self.history_reader is not None:
            histories = list(self.history_reader())
            self._unavailable_history_ids = set()
            history_read = True
        elif rows:
            try:
                histories = _run_coro(self._fetch_histories())
                history_read = True
            except Exception:
                history_read = False
        self._history_read_ids = {run_id for run_id, _ in histories} if history_read else set()
        binding_by_run = {
            row["run_id"]: (row["task_id"], row["context_id"])
            for row in rows
        }
        for run_id, binding in self._durable_temporal_bindings().items():
            if run_id in binding_by_run and binding_by_run[run_id] != binding:
                raise SourceContractError("durable run binding conflicts with Runtime authority")
            binding_by_run.setdefault(run_id, binding)
        parent_by_child = {}
        for parent_id, events in histories:
            for event in events:
                if event.get("event_type") != "START_CHILD_WORKFLOW_EXECUTION_INITIATED":
                    continue
                child_id = (event.get("attributes") or {}).get("workflow_id")
                if isinstance(child_id, str) and child_id:
                    parent_by_child[child_id] = parent_id
                    self._history_children.setdefault(parent_id, set()).add(child_id)
        # Propagate only an already-known root binding along durable Temporal
        # parent->child start records. Iterate for nested child workflows.
        for _ in range(len(parent_by_child)):
            changed = False
            for child_id, parent_id in parent_by_child.items():
                if child_id not in binding_by_run and parent_id in binding_by_run:
                    binding_by_run[child_id] = binding_by_run[parent_id]
                    changed = True
            if not changed:
                break
        self._known_run_bindings = dict(binding_by_run)
        for workflow_id, events in histories:
            highwater = max((int(event["event_id"]) for event in events
                             if type(event.get("event_id")) is int), default=0)
            with self._connect() as db:
                previous = db.execute("SELECT event_id FROM history_watermarks WHERE workflow_id=?",
                                      (workflow_id,)).fetchone()
            if previous and highwater <= previous["event_id"]:
                # The original watermark predates quality projection. Append
                # only the verified durable review facts using versioned IDs;
                # never rewrite or remove the already-persisted source rows.
                backfill_id = f"quality-v1:{workflow_id}"
                with self._connect() as db:
                    backfilled = db.execute(
                        "SELECT 1 FROM source_backfills WHERE backfill_id=?",
                        (backfill_id,)).fetchone()
                if not backfilled:
                    task_id, context_id = binding_by_run.get(workflow_id, (None, None))
                    quality_records = [record for record in self._history_records(
                        workflow_id, events, task_id, context_id)
                        if record.get("event_type") == "com.exomachina.quality.verdict.v1"]
                    for record in quality_records:
                        original_source_id = record["source_id"]
                        with self._connect() as db:
                            already_projected = db.execute(
                                "SELECT 1 FROM source_records WHERE source_id=?",
                                (original_source_id,)).fetchone()
                        if already_projected:
                            continue
                        record["source_id"] += ":backfill-v1"
                        record["context_id"] = context_id
                        self._insert(record)
                    with self._connect() as db:
                        db.execute("INSERT OR IGNORE INTO source_backfills VALUES (?,?)",
                                   (backfill_id, _iso(datetime.now(timezone.utc))))
                terminal_history = any(
                    event.get("event_type") in _TERMINAL_WORKFLOW_EVENT_TYPES
                    for event in events)
                durable_terminal = terminal_history
                latest_run_state_time: datetime | None = None
                unknown_run_state_time = False
                if not durable_terminal:
                    run_source_prefix = workflow_id + ":"
                    run_node_prefix = (
                        _RUN_NODE_LINK_CORRECTION_PREFIX + workflow_id + ":")
                    with self._connect() as db:
                        prior_run_rows = db.execute(
                            "SELECT record_json FROM source_records "
                            "WHERE substr(source_id,1,?)=? OR substr(source_id,1,?)=?",
                            (len(run_source_prefix), run_source_prefix,
                             len(run_node_prefix), run_node_prefix)).fetchall()
                    for prior_row in prior_run_rows:
                        try:
                            prior_record = json.loads(prior_row["record_json"])
                        except (ValueError, TypeError) as error:
                            raise SourceContractError(
                                "persisted Temporal run-state record is invalid") from error
                        prior_fields = prior_record.get("fields")
                        if (prior_record.get("run_id") == workflow_id and
                                prior_record.get("event_type") in {
                                    "com.exomachina.run.created.v1",
                                    "com.exomachina.run.state_changed.v1"}):
                            if not isinstance(prior_fields, Mapping):
                                unknown_run_state_time = True
                                continue
                            raw_state_time = prior_record.get("time")
                            if not isinstance(raw_state_time, str):
                                unknown_run_state_time = True
                                continue
                            try:
                                state_time = datetime.fromisoformat(
                                    raw_state_time.replace("Z", "+00:00"))
                            except (TypeError, ValueError):
                                unknown_run_state_time = True
                                continue
                            if state_time.tzinfo is None:
                                unknown_run_state_time = True
                                continue
                            state_time = state_time.astimezone(timezone.utc)
                            if (latest_run_state_time is None or
                                    state_time > latest_run_state_time):
                                latest_run_state_time = state_time
                            if (prior_record.get("event_type") ==
                                    "com.exomachina.run.state_changed.v1" and
                                    (prior_fields.get("ended_at") is not None or
                                     prior_fields.get("state") in {"completed", "failed"})):
                                durable_terminal = True
                                break
                task_id, context_id = binding_by_run.get(workflow_id, (None, None))
                for record in self._history_records(workflow_id, events, task_id, context_id):
                    fields = record.get("fields")
                    is_link_correction = record.get("source_id", "").startswith(
                        _ASSIGNMENT_LINK_CORRECTION_PREFIX)
                    is_node_link_correction = record.get("source_id", "").startswith(
                        _ASSIGNMENT_NODE_LINK_CORRECTION_PREFIX)
                    is_run_node_link_correction = record.get("source_id", "").startswith(
                        _RUN_NODE_LINK_CORRECTION_PREFIX)
                    is_linked_retry_completion = (
                        record.get("event_type") ==
                        "com.exomachina.assignment.state_changed.v1" and
                        isinstance(fields, Mapping) and fields.get("state") == "completed" and
                        record.get("attempt_id") != "1")
                    if (is_link_correction or is_node_link_correction or
                            is_linked_retry_completion):
                        self._insert(record)
                    elif is_run_node_link_correction and not durable_terminal and not unknown_run_state_time:
                        correction_time_value = record.get("time")
                        try:
                            correction_time = datetime.fromisoformat(
                                correction_time_value.replace("Z", "+00:00"))
                        except (AttributeError, TypeError, ValueError):
                            continue
                        if correction_time.tzinfo is None:
                            continue
                        correction_time = correction_time.astimezone(timezone.utc)
                        if (latest_run_state_time is None or
                                correction_time >= latest_run_state_time):
                            self._insert(record)
                continue
            task_id, context_id = binding_by_run.get(workflow_id, (None, None))
            for record in self._history_records(workflow_id, events, task_id, context_id):
                self._insert(record)
            with self._connect() as db:
                db.execute("INSERT INTO history_watermarks VALUES (?,?) "
                           "ON CONFLICT(workflow_id) DO UPDATE SET event_id=excluded.event_id",
                           (workflow_id, highwater))
        self._local_outcome_records(rows)
        self._local_delivery_records(rows)
        self._operations_records(principal, binding_by_run)
        self._public_wait_records(rows, binding_by_run)
        admission_queue = getattr(self.director, "admission_queue", None)
        if admission_queue is None:
            self.admission_observation_status = "unconfigured"
            self.admission_capacity_snapshot = None
        else:
            def original_task_binding(task_id: str) -> tuple[str, str]:
                binding = self.director.task_binding(task_id)
                if (not isinstance(binding, tuple) or len(binding) != 2 or
                        not all(isinstance(value, str) and value for value in binding)):
                    raise LookupError("Admission Task has no authoritative Runtime binding")
                return binding

            admission = project_admission_observation(
                admission_queue, self.factory_id, original_task_binding)
            for record in admission["records"]:
                # The adapter has already validated the exact queue writer time
                # and original Task/context binding. Persist those durable facts
                # without normalizing or manufacturing a transition timestamp.
                self._insert(record)
            self.admission_capacity_snapshot = dict(admission["capacity"])
            self.admission_observation_status = "current"
        self._commercial_records(binding_by_run)
        observed_at = _iso(datetime.now(timezone.utc))
        if len(self._unavailable_history_ids) > 256:
            raise SourceContractError("history warning exceeds bounded coverage")
        status = "current" if history_read and not self._unavailable_history_ids else ("disconnected" if rows else "unknown")
        return {"status": status, "observed_at": observed_at, "scope": "factory",
                "unavailable_run_ids": sorted(self._unavailable_history_ids)}

    def _import_records(self, principal: object) -> dict[str, Any]:
        with self._lock:
            now = time.monotonic()
            if now - self._last_refresh_at < self.refresh_interval_seconds:
                return dict(self._cached_freshness)
            self._cached_freshness = self._refresh_records(principal)
            self._last_refresh_at = now
            return dict(self._cached_freshness)

    def get_run_freshness(self, principal: object, factory_id: str,
                          run_id: str) -> dict[str, Any]:
        """Authenticated freshness for an exact owned run and verified child starts.

        Factory history gaps remain visible. Missing or failed reads never establish
        freshness, and caller strings cannot select a history endpoint or identity.
        """
        self.authorize(principal, factory_id)
        if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}", run_id):
            raise ObservationNotFound("unknown run")
        with self._lock:
            factory = dict(self._cached_freshness)
            if run_id not in self._known_run_bindings:
                raise ObservationNotFound("unknown run")
            included: set[str] = set()
            pending = [run_id]
            while pending:
                current = pending.pop()
                if current in included:
                    continue
                included.add(current)
                if len(included) > 256:
                    raise SourceContractError("run freshness exceeds bounded coverage")
                pending.extend(self._history_children.get(current, set()))
            current = included.issubset(self._history_read_ids)
            unavailable = self._unavailable_history_ids | (included - self._history_read_ids)
            if len(unavailable) > 256:
                raise SourceContractError("history warning exceeds bounded coverage")
            return {"status": "current" if current else "disconnected",
                    "observed_at": factory.get("observed_at"), "scope": "run",
                    "run_id": run_id, "included_run_ids": sorted(included),
                    "factory_status": "fresh" if factory["status"] == "current" else factory["status"],
                    "unavailable_run_ids": sorted(unavailable)}

    def read_page(self, principal: object, factory_id: str, after_cursor: Any,
                  *, limit: int) -> SourcePage:
        self.authorize(principal, factory_id)
        if after_cursor is None:
            position = 0
        else:
            position = self._decode_source_cursor(after_cursor)
        with self._lock:
            freshness = self._import_records(principal)
            with self._connect() as db:
                rows = db.execute("SELECT position,record_json FROM source_records "
                                  "WHERE position>? ORDER BY position LIMIT ?",
                                  (position, limit + 1)).fetchall()
            if len(self._known_run_bindings) > 256:
                raise SourceContractError("run freshness exceeds bounded map")
            run_freshness = {run_id: self.get_run_freshness(principal, factory_id, run_id)
                             for run_id in self._known_run_bindings}
        has_more = len(rows) > limit
        selected = rows[:limit]
        next_position = selected[-1]["position"] if selected else position
        records = [json.loads(row["record_json"]) for row in selected]
        return SourcePage(records=records, next_cursor=self._encode_source_cursor(next_position),
                          has_more=has_more, freshness=freshness, run_freshness=run_freshness)

    @staticmethod
    def _encode_source_cursor(position: int) -> str:
        payload = _canonical({"position": position}).encode("ascii")
        encoded = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
        return "h1." + encoded

    @staticmethod
    def _decode_source_cursor(cursor: Any) -> int:
        if (not isinstance(cursor, str) or not cursor.startswith("h1.")
                or not cursor[3:] or any(ch not in
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-"
                    for ch in cursor[3:])):
            raise SourceContractError("invalid private runtime source checkpoint")
        try:
            raw = base64.urlsafe_b64decode(cursor[3:] + "=" * (-len(cursor[3:]) % 4))
            value = json.loads(raw)
        except (ValueError, TypeError) as error:
            raise SourceContractError("invalid private runtime source checkpoint") from error
        if (not isinstance(value, dict) or set(value) != {"position"}
                or type(value["position"]) is not int or value["position"] < 0
                or RuntimeObservationSource._encode_source_cursor(value["position"]) != cursor):
            raise SourceContractError("invalid private runtime source checkpoint")
        return value["position"]

    def inspect_artifact(self, principal: object, factory_id: str, run_id: str,
                         revision: str, sha256: str) -> dict[str, Any]:
        self.authorize(principal, factory_id)
        self._import_records(principal)
        with self._connect() as db:
            row = db.execute("SELECT content,accepted FROM runtime_artifacts "
                             "WHERE run_id=? AND revision=? AND sha256=?",
                             (run_id, revision, sha256)).fetchone()
        if row is None or not row["accepted"]:
            raise ObservationNotFound("artifact not found")
        content = bytes(row["content"])
        if hashlib.sha256(content).hexdigest() != sha256:
            raise SourceContractError("accepted artifact bytes changed")
        return {"content": content, "sha256": sha256, "media_type": "application/json"}

    def submit_command(self, principal: object, factory_id: str,
                       command: Mapping[str, Any]) -> dict[str, Any]:
        self.authorize(principal, factory_id)
        command_id = command["command_id"]
        task_id, context_id = command["task_id"], command["context_id"]
        binding = self.director.task_binding(task_id)
        if binding is None or binding[1] != context_id:
            raise ObservationNotFound("original Task binding not found")
        run_id = binding[0]
        legacy_fingerprint = hashlib.sha256(_canonical(dict(command)).encode()).hexdigest()
        fingerprint = hashlib.sha256(_canonical({"command": dict(command),
            "principal": principal}).encode()).hexdigest()
        record = self.director.run_record(run_id)
        with self._connect() as db:
            prior = db.execute("SELECT fingerprint FROM command_intents WHERE command_id=?",
                               (command_id,)).fetchone()
            legacy_owner_retry = (prior is not None and
                prior["fingerprint"] == legacy_fingerprint and
                record.get("authorized_actor") == principal)
            if prior and prior["fingerprint"] != fingerprint and not legacy_owner_retry:
                self._command_event(command, run_id, "rejected", "command-id-conflict")
                return {"command_id": command_id, "lifecycle": "received"}
            terminal = self._terminal_command_outcome(db, command_id) if prior else None
            if not prior:
                db.execute("INSERT INTO command_intents VALUES (?,?,?,?,?,?,?)",
                           (command_id, fingerprint, run_id, task_id, context_id,
                            command["action"], _iso(datetime.now(timezone.utc))))
        if terminal is not None:
            # The projection transport still acknowledges receipt. Its durable
            # source stream already contains this terminal outcome for replay.
            return {"command_id": command_id, "lifecycle": "received",
                    "prior_outcome": terminal}
        self._command_event(command, run_id, "received")
        try:
            action = command["action"]
            if action not in {"abort", "escalate"}:
                raise ValueError("only permitted Director decisions are supported")
            if command["expected_state"] != "input-required":
                raise ValueError("expected state does not match the original Task")
            if not command.get("expected_revision") or not command.get("expected_sha256"):
                raise ValueError("decision requires the inspected artifact revision and digest")
            current = self.director.inspect_bound_run(task_id)
            phase = current.get("phase")
            owner = record.get("authorized_actor")
            applied = (current.get("applied_decisions") or {}).get(command_id)
            expected_outcome = f"{action}-recorded"
            if applied is not None:
                # Recover a crash after Temporal committed the Update but before
                # this local journal appended its terminal command fact. The
                # actor-bound durable intent above must match this retry.
                if applied != expected_outcome:
                    raise ValueError("command ID was applied to a different decision")
                actor_allowed = True
            elif action == "escalate":
                actor_allowed = principal == owner and phase == "awaiting-director"
            elif phase == "awaiting-director":
                actor_allowed = principal == owner
            elif phase == "awaiting-human":
                actor_allowed = principal == current.get("decision_actor")
            else:
                actor_allowed = False
            if not actor_allowed:
                raise PermissionError("principal is not authorized for the current decision wait")
            if (applied is None and action not in current.get("permitted_actions", [])):
                raise ValueError("decision action is not permitted at the current wait")
            if (current.get("current_revision") != command["expected_revision"]
                    or current.get("current_sha256") != command["expected_sha256"]):
                raise ValueError("decision wait or inspected artifact changed")
            self._command_event(command, run_id, "validated", action)
            from harness import CURRENT_ACTOR
            token = CURRENT_ACTOR.set(str(principal))
            try:
                outcome = self.director.perform({"op": action, "action_id": command_id,
                    "revision": command["expected_revision"],
                    "sha256": command["expected_sha256"]}, task_id, context_id)
            finally:
                CURRENT_ACTOR.reset(token)
            expected_outcome = f"{action}-recorded"
            if (outcome.get("lifecycle") != "applied" or
                    outcome.get("outcome") != expected_outcome):
                raise RuntimeError("Director did not return the applied decision receipt")
            self._command_event(command, run_id, "applied", expected_outcome,
                                resulting_state=expected_outcome)
        except (PermissionError, ValueError, LookupError) as error:
            self._command_event(command, run_id, "rejected", type(error).__name__)
        except Exception as error:
            from harness_server import Rejected
            lifecycle = "rejected" if isinstance(error, Rejected) else "failed"
            safe_code = getattr(error, "safe_code", None)
            from harness import DIRECTOR_DECISION_REJECTION_CODES
            if (isinstance(error, Rejected) and command.get("action") in {"abort", "escalate"}):
                outcome = (safe_code if isinstance(safe_code, str) and
                           safe_code in DIRECTOR_DECISION_REJECTION_CODES else
                           "decision-rejected-unclassified")
            else:
                outcome = type(error).__name__
            self._command_event(command, run_id, lifecycle, outcome)
        return {"command_id": command_id, "lifecycle": "received"}

    @staticmethod
    def _terminal_command_outcome(db: sqlite3.Connection,
                                  command_id: str) -> dict[str, Any] | None:
        # A changed-payload conflict has its own hashed source ID beneath the
        # same command prefix. It is a rejected attempt to reuse the ID, not a
        # replacement for the original intent's durable terminal outcome.
        terminal_ids = tuple(f"command:{command_id}:{lifecycle}"
                             for lifecycle in ("applied", "rejected", "failed"))
        rows = db.execute("SELECT record_json FROM source_records WHERE source_id IN (?,?,?) "
                          "ORDER BY position", terminal_ids).fetchall()
        terminal = None
        for row in rows:
            record = json.loads(row["record_json"])
            data = record.get("fields", {})
            if data.get("lifecycle") in {"applied", "rejected", "failed"}:
                terminal = {key: data[key] for key in
                            ("lifecycle", "outcome", "resulting_state") if key in data}
        return terminal

    def _command_event(self, command: Mapping[str, Any], run_id: str, lifecycle: str,
                       outcome: str | None = None, *, resulting_state: str | None = None) -> None:
        fields: dict[str, Any] = {"command_id": command["command_id"],
                                  "lifecycle": lifecycle}
        if outcome is not None:
            fields["outcome"] = outcome.lower().replace("_", "-")
        if command.get("expected_state"):
            fields["expected_state"] = command["expected_state"]
        if resulting_state:
            fields["resulting_state"] = resulting_state
        if command.get("expected_revision"):
            fields["artifact_revision"] = command["expected_revision"]
        if command.get("expected_sha256"):
            fields["artifact_sha256"] = command["expected_sha256"]
        now = _iso(datetime.now(timezone.utc))
        source_id = f"command:{command['command_id']}:{lifecycle}"
        if lifecycle == "rejected" and outcome == "command-id-conflict":
            source_id += ":" + hashlib.sha256(_canonical(dict(command)).encode()).hexdigest()[:12]
        with self._connect() as db:
            prior = db.execute("SELECT record_json FROM source_records WHERE source_id=?",
                               (source_id,)).fetchone()
        if prior:
            original = json.loads(prior["record_json"])
            value = self._record("command", source_id,
                "com.exomachina.command.outcome.v1", original["time"], run_id=run_id,
                fields=fields, task_id=command["task_id"], context_id=command["context_id"])
            if _canonical(value) != _canonical(original):
                raise SourceContractError("durable command lifecycle record changed")
            return
        self._insert(self._record("command", source_id,
            "com.exomachina.command.outcome.v1", now, run_id=run_id, fields=fields,
            task_id=command["task_id"], context_id=command["context_id"]))


def _run_coro(coroutine):
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(lambda: asyncio.run(coroutine)).result()
