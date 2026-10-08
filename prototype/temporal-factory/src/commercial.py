"""Local durable supplier accounting and unqualified payment adapter seams.

All monetary values are integer atomic units. This module records commercial
facts only: it performs no inference, payment-network, wallet, or AP2 trust I/O.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any, Iterator, Mapping, Protocol, Sequence


class CommercialError(ValueError):
    """A commercial record or transition is invalid."""


class IdempotencyConflict(CommercialError):
    """An idempotency identity was reused with different content."""


class BudgetExceeded(CommercialError):
    """A spend reservation would exceed the configured account budget."""


class AuthorizationRejected(CommercialError):
    """The purchase does not fit its recorded authority."""


class UnsupportedPriceBasis(CommercialError):
    """The price basis is represented but not implemented locally."""


class UnsupportedAdapter(CommercialError):
    """No configured and qualified adapter exists for this profile."""


class ReconciliationConflict(CommercialError):
    """A settlement result conflicts with the original payment intent."""


class EvidenceState(StrEnum):
    MEASURED = "measured"
    CALCULATED = "calculated_from_measured_usage"
    PROVIDER_REPORTED = "provider_reported"
    ESTIMATED = "estimated"
    UNKNOWN = "unknown"
    UNDISCLOSED = "undisclosed"


class PriceBasis(StrEnum):
    USAGE = "usage"
    FIXED_ASSIGNMENT = "fixed_assignment"
    FIXED_ATTEMPT = "fixed_attempt"
    ACCEPTED_OUTCOME = "accepted_outcome"


class PaymentTrigger(StrEnum):
    UPFRONT = "upfront"
    INCREMENTAL_USE = "incremental_use"
    ATTEMPT_COMPLETION = "attempt_completion"
    ACCEPTANCE = "acceptance"


class RoundingMode(StrEnum):
    FLOOR = "floor"
    CEILING = "ceiling"
    HALF_UP = "half_up"


class SettlementState(StrEnum):
    PENDING = "settlement_pending"
    UNRESOLVED = "unresolved"
    SETTLED = "settled"
    FAILED = "failed"


PAYMENT_ADAPTER_DECLARATIONS: Mapping[str, dict[str, Any]] = {
    "mpp-session": {
        "protocol": "MPP",
        "profile": "session",
        "version": None,
        "version_status": "unconfigured",
        "operations": ["parse_challenge", "authorize_session", "meter_usage", "settle", "reconcile"],
        "prerequisites": {
            "provider_activation": "unverified",
            "sdk_version": None,
            "method": None,
            "network": None,
            "session_limit": None,
            "credential_or_custody": None,
        },
        "configured": False,
        "qualified": False,
        "advertisable": False,
        "invokable": False,
    },
    "x402-upto": {
        "protocol": "x402",
        "profile": "upto",
        "version": "2",
        "version_status": "specification_mapping_only",
        "operations": ["authorize_maximum", "settle_actual", "reconcile"],
        "prerequisites": {
            "sdk_version": None,
            "scheme": "upto",
            "facilitator": None,
            "network": None,
            "asset": None,
            "maximum_amount": None,
            "credential_or_custody": None,
        },
        "configured": False,
        "qualified": False,
        "advertisable": False,
        "invokable": False,
    },
    "ap2-v0.2": {
        "protocol": "AP2",
        "profile": "checkout-and-payment-mandates",
        "version": "0.2",
        "version_status": "specification_mapping_only",
        "operations": ["verify_checkout_mandate", "verify_payment_mandate",
                       "bind_payment_mandate_to_checkout", "verify_checkout_receipt",
                       "verify_payment_receipt", "enforce_local_scope"],
        "prerequisites": {
            "schema_and_sdk": None,
            "trusted_surface_and_trust_roots": None,
            "signature_verifier": None,
            "merchant_checkout_jwt": None,
            "credential_provider_verification": None,
            "receipt_validation": None,
            "nested_agent_delegation": "out_of_scope_in_v0.2",
        },
        "configured": False,
        "qualified": False,
        "advertisable": False,
        "invokable": False,
    },
    "test-only-recorded": {
        "protocol": "local-test",
        "profile": "recorded-outcome",
        "version": "1",
        "version_status": "test_only",
        "operations": ["record_pending", "record_unresolved", "record_outcome"],
        "prerequisites": {"network": "none", "wallet": "none"},
        "configured": True,
        "qualified": False,
        "advertisable": False,
        "invokable": False,
        "test_only_recording": True,
    },
}


class PaymentAdapter(Protocol):
    """Future operation shape. No concrete implementation is supplied here."""

    profile: Mapping[str, Any]

    def authorize(self, purchase: Mapping[str, Any]) -> Mapping[str, Any]: ...
    def settle(self, obligation: Mapping[str, Any], *, idempotency_key: str) -> Mapping[str, Any]: ...
    def lookup(self, settlement_id: str, *, idempotency_key: str) -> Mapping[str, Any] | None: ...
    def refund(self, settlement_id: str, *, amount_units: int,
               idempotency_key: str) -> Mapping[str, Any]: ...


class PaymentAdapterCatalog:
    """Capability declarations only; unqualified profiles cannot be invoked."""

    def declarations(self) -> list[dict[str, Any]]:
        return [
            {"adapter_id": name, **json.loads(_canonical(declaration))}
            for name, declaration in PAYMENT_ADAPTER_DECLARATIONS.items()
        ]

    def get(self, profile: str) -> dict[str, Any]:
        try:
            return json.loads(_canonical(PAYMENT_ADAPTER_DECLARATIONS[profile]))
        except KeyError as error:
            raise UnsupportedAdapter("payment adapter profile is not declared") from error

    def advertised_profiles(self) -> list[str]:
        return sorted(name for name, value in PAYMENT_ADAPTER_DECLARATIONS.items()
                      if value["configured"] and value["qualified"] and value["advertisable"])

    def require_invokable(self, profile: str) -> dict[str, Any]:
        declaration = self.get(profile)
        if not (declaration["configured"] and declaration["qualified"]
                and declaration["invokable"]):
            raise UnsupportedAdapter(f"payment adapter profile is not configured and qualified: {profile}")
        return declaration


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CommercialError(f"{field} must be a non-empty string")
    return value.strip()


def _optional_text(value: Any, field: str) -> str | None:
    return None if value is None else _text(value, field)


def _int(value: Any, field: str, *, minimum: int = 0,
         allow_none: bool = False) -> int | None:
    if value is None and allow_none:
        return None
    if type(value) is not int or value < minimum or value > 9_223_372_036_854_775_807:
        raise CommercialError(f"{field} must be a SQLite-safe integer >= {minimum}")
    return value


def _reject_floats(value: Any, path: str = "record") -> None:
    if isinstance(value, float):
        raise CommercialError(f"{path} contains a float; monetary arithmetic uses exact integers")
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise CommercialError(f"{path} contains a non-string key")
            _reject_floats(item, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _reject_floats(item, f"{path}[{index}]")


def _canonical(value: Any) -> str:
    _reject_floats(value)
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True,
                          separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as error:
        raise CommercialError("record is not JSON-safe") from error


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _evidence(value: Any) -> EvidenceState:
    try:
        return EvidenceState(value)
    except (ValueError, TypeError) as error:
        raise CommercialError("unknown evidence state") from error


def _timestamp(value: Any, field: str) -> str:
    raw = _text(value, field)
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as error:
        raise CommercialError(f"{field} must be ISO-8601") from error
    if parsed.tzinfo is None:
        raise CommercialError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat()


def _money(amount: int | None, currency: str, atomic_scale: int,
           evidence: EvidenceState | str,
           basis: Mapping[str, Any] | None = None) -> dict[str, Any]:
    state = EvidenceState(evidence)
    if not isinstance(currency, str) or not currency:
        raise CommercialError("currency must be explicit")
    _int(atomic_scale, "atomic_scale")
    if amount is None:
        if state not in {EvidenceState.UNKNOWN, EvidenceState.UNDISCLOSED}:
            raise CommercialError("a missing amount must be unknown or undisclosed")
    else:
        _int(amount, "amount_units")
        if state in {EvidenceState.UNKNOWN, EvidenceState.UNDISCLOSED}:
            raise CommercialError("unknown or undisclosed money cannot have a numeric amount")
    return {"amount_units": amount, "currency": currency, "atomic_scale": atomic_scale,
            "evidence": state.value,
            "basis": dict(basis or {})}


def _round_ratio(numerator: int, denominator: int, mode: RoundingMode) -> int:
    if numerator < 0 or denominator <= 0:
        raise CommercialError("invalid rate fraction")
    q, r = divmod(numerator, denominator)
    if r == 0 or mode == RoundingMode.FLOOR:
        return q
    if mode == RoundingMode.CEILING:
        return q + 1
    return q + int(2 * r >= denominator)


def _unknown_state(states: Sequence[EvidenceState]) -> EvidenceState:
    return EvidenceState.UNDISCLOSED if EvidenceState.UNDISCLOSED in states else EvidenceState.UNKNOWN


class CommercialLedger:
    """SQLite commercial store. Supply every price, limit, and policy explicitly."""

    def __init__(self, path: str | Path, *, timeout_seconds: int = 30):
        if str(path) == ":memory:":
            raise CommercialError("durable accounting requires a shared file-backed database")
        if type(timeout_seconds) is not int or timeout_seconds <= 0:
            raise CommercialError("timeout_seconds must be a positive integer")
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.timeout_seconds = timeout_seconds
        self.adapters = PaymentAdapterCatalog()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=self.timeout_seconds, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute(f"PRAGMA busy_timeout={self.timeout_seconds * 1000}")
        db.execute("PRAGMA synchronous=FULL")
        return db

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        db = self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def _initialize(self) -> None:
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS offers(
                    digest TEXT PRIMARY KEY, supplier_id TEXT NOT NULL, offer_id TEXT NOT NULL,
                    version TEXT NOT NULL, currency TEXT, atomic_scale INTEGER NOT NULL CHECK(atomic_scale>=0),
                    terms TEXT NOT NULL, created_at TEXT NOT NULL,
                    UNIQUE(supplier_id,offer_id,version));
                CREATE TABLE IF NOT EXISTS budgets(
                    account_id TEXT NOT NULL, currency TEXT NOT NULL,
                    atomic_scale INTEGER NOT NULL CHECK(atomic_scale>=0), budget_id TEXT NOT NULL,
                    limit_units INTEGER NOT NULL CHECK(limit_units>=0),
                    PRIMARY KEY(account_id,currency,atomic_scale));
                CREATE TABLE IF NOT EXISTS authorizations(
                    authorization_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
                    principal_id TEXT NOT NULL, account_id TEXT NOT NULL, supplier_id TEXT NOT NULL,
                    offer_digest TEXT NOT NULL, run_id TEXT NOT NULL, assignment_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL, currency TEXT NOT NULL,
                    atomic_scale INTEGER NOT NULL CHECK(atomic_scale>=0),
                    ceiling_units INTEGER NOT NULL CHECK(ceiling_units>=0), expires_at TEXT NOT NULL,
                    evidence_ref TEXT, revoked INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY(offer_digest) REFERENCES offers(digest));
                CREATE TABLE IF NOT EXISTS purchases(
                    purchase_id TEXT PRIMARY KEY, idempotency_key TEXT UNIQUE NOT NULL,
                    fingerprint TEXT NOT NULL, authorization_id TEXT NOT NULL, account_id TEXT NOT NULL,
                    principal_id TEXT NOT NULL, supplier_id TEXT NOT NULL, offer_digest TEXT NOT NULL,
                    run_id TEXT NOT NULL, assignment_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
                    service_identity TEXT NOT NULL, task_id TEXT, currency TEXT NOT NULL,
                    atomic_scale INTEGER NOT NULL CHECK(atomic_scale>=0),
                    ceiling_units INTEGER NOT NULL CHECK(ceiling_units>=0),
                    usage_complete INTEGER NOT NULL DEFAULT 0, closed INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY(authorization_id) REFERENCES authorizations(authorization_id),
                    FOREIGN KEY(offer_digest) REFERENCES offers(digest));
                CREATE TABLE IF NOT EXISTS reservations(
                    purchase_id TEXT PRIMARY KEY, account_id TEXT NOT NULL, currency TEXT NOT NULL,
                    atomic_scale INTEGER NOT NULL CHECK(atomic_scale>=0),
                    amount_units INTEGER NOT NULL CHECK(amount_units>=0), state TEXT NOT NULL,
                    FOREIGN KEY(purchase_id) REFERENCES purchases(purchase_id));
                CREATE TABLE IF NOT EXISTS usage_records(
                    usage_id TEXT PRIMARY KEY, purchase_id TEXT NOT NULL, fingerprint TEXT NOT NULL,
                    unit TEXT NOT NULL, quantity INTEGER, evidence_state TEXT NOT NULL, source TEXT NOT NULL,
                    evidence_ref TEXT, service_identity TEXT NOT NULL, assignment_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL, task_id TEXT, model_call_id TEXT, late INTEGER NOT NULL,
                    recorded_at TEXT NOT NULL,
                    FOREIGN KEY(purchase_id) REFERENCES purchases(purchase_id));
                CREATE TABLE IF NOT EXISTS obligations(
                    obligation_id TEXT PRIMARY KEY, purchase_id TEXT NOT NULL, digest TEXT NOT NULL,
                    amount_units INTEGER, currency TEXT NOT NULL, atomic_scale INTEGER NOT NULL CHECK(atomic_scale>=0),
                    evidence_state TEXT NOT NULL, details TEXT NOT NULL,
                    created_at TEXT NOT NULL, UNIQUE(purchase_id,digest),
                    FOREIGN KEY(purchase_id) REFERENCES purchases(purchase_id));
                CREATE TABLE IF NOT EXISTS settlements(
                    settlement_id TEXT PRIMARY KEY, idempotency_key TEXT UNIQUE NOT NULL,
                    fingerprint TEXT NOT NULL, purchase_id TEXT NOT NULL, obligation_id TEXT NOT NULL,
                    direction TEXT NOT NULL, adapter_profile TEXT NOT NULL, amount_units INTEGER NOT NULL,
                    currency TEXT NOT NULL, atomic_scale INTEGER NOT NULL CHECK(atomic_scale>=0),
                    state TEXT NOT NULL, recorded_at TEXT NOT NULL, receipt_ref TEXT, evidence_state TEXT,
                    FOREIGN KEY(purchase_id) REFERENCES purchases(purchase_id),
                    FOREIGN KEY(obligation_id) REFERENCES obligations(obligation_id));
                CREATE TABLE IF NOT EXISTS credits(
                    credit_id TEXT PRIMARY KEY, idempotency_key TEXT UNIQUE NOT NULL,
                    fingerprint TEXT NOT NULL, purchase_id TEXT NOT NULL, amount_units INTEGER NOT NULL,
                    currency TEXT NOT NULL, atomic_scale INTEGER NOT NULL CHECK(atomic_scale>=0),
                    reason TEXT NOT NULL, evidence_state TEXT NOT NULL,
                    evidence_ref TEXT, recorded_at TEXT NOT NULL,
                    FOREIGN KEY(purchase_id) REFERENCES purchases(purchase_id));
                CREATE TABLE IF NOT EXISTS economic_facts(
                    fact_id TEXT PRIMARY KEY, idempotency_key TEXT UNIQUE NOT NULL,
                    fingerprint TEXT NOT NULL, kind TEXT NOT NULL, account_id TEXT NOT NULL,
                    run_id TEXT NOT NULL, purchase_id TEXT, assignment_id TEXT, attempt_id TEXT,
                    currency TEXT NOT NULL, atomic_scale INTEGER NOT NULL CHECK(atomic_scale>=0),
                    amount_units INTEGER, evidence_state TEXT NOT NULL,
                    source TEXT NOT NULL, basis TEXT NOT NULL,
                    FOREIGN KEY(purchase_id) REFERENCES purchases(purchase_id));
                CREATE TABLE IF NOT EXISTS completion_keys(
                    idempotency_key TEXT PRIMARY KEY, fingerprint TEXT NOT NULL);
            """)

    def pin_offer(self, terms: Mapping[str, Any]) -> dict[str, Any]:
        """Pin a versioned offer exactly; other price bases remain represented only."""
        value = dict(terms)
        _reject_floats(value)
        supplier = _text(value.get("supplier_id"), "supplier_id")
        offer = _text(value.get("offer_id"), "offer_id")
        version = _text(value.get("version"), "version")
        try:
            basis = PriceBasis(value.get("price_basis"))
            trigger = PaymentTrigger(value.get("payment_trigger"))
            rounding = RoundingMode(value.get("rounding_mode", "half_up"))
        except (ValueError, TypeError) as error:
            raise CommercialError("offer needs a recognized price basis, trigger, and rounding mode") from error
        currency = value.get("currency")
        if currency is not None:
            currency = _text(currency, "currency")
        elif value.get("currency_state", "unknown") not in {"unknown", "undisclosed"}:
            raise CommercialError("missing currency must be unknown or undisclosed")
        scale = _int(value.get("atomic_scale"), "atomic_scale")
        assert scale is not None
        units = value.get("required_usage_units", [])
        rates = value.get("rates", [])
        if (not isinstance(units, list) or any(not isinstance(u, str) or not u for u in units)
                or len(set(units)) != len(units) or not isinstance(rates, list)):
            raise CommercialError("usage units and rates must be explicit lists")
        seen: set[str] = set()
        for rate in rates:
            if not isinstance(rate, Mapping):
                raise CommercialError("rate entries must be mappings")
            unit = _text(rate.get("unit"), "rate.unit")
            component = _text(rate.get("component"), "rate.component")
            if unit in seen or component not in {"inference_cost", "hosting_cost"}:
                raise CommercialError("rate unit must be unique and use an allowed cost component")
            seen.add(unit)
            state = _evidence(rate.get("evidence_state", "unknown"))
            numerator, denominator = rate.get("numerator"), rate.get("denominator")
            if numerator is None:
                if state not in {EvidenceState.UNKNOWN, EvidenceState.UNDISCLOSED} or denominator is not None:
                    raise CommercialError("missing rate must be explicitly unknown or undisclosed")
            else:
                if currency is None:
                    raise CommercialError("a numeric offer rate requires explicit currency")
                _int(numerator, "rate.numerator")
                _int(denominator, "rate.denominator", minimum=1)
                if state in {EvidenceState.UNKNOWN, EvidenceState.UNDISCLOSED}:
                    raise CommercialError("unknown rate cannot include a numeric amount")
            if rate.get("source_ref") is not None:
                _text(rate["source_ref"], "rate.source_ref")
        markup = value.get("markup_bps")
        markup_state = _evidence(value.get("markup_evidence_state",
                                  "unknown" if markup is None else "provider_reported"))
        if markup is None:
            if markup_state not in {EvidenceState.UNKNOWN, EvidenceState.UNDISCLOSED}:
                raise CommercialError("missing markup must be unknown or undisclosed")
        else:
            _int(markup, "markup_bps")
            if markup_state in {EvidenceState.UNKNOWN, EvidenceState.UNDISCLOSED}:
                raise CommercialError("unknown markup cannot include a numeric value")
        for name in ("charge_base", "markup_base"):
            items = value.get(name)
            if items is not None and (not isinstance(items, list) or
                    any(not isinstance(item, str) or not item for item in items)):
                raise CommercialError(f"{name} must be a list or explicitly null")
            if items is not None and any(item not in {"inference_cost", "hosting_cost"} for item in items):
                raise CommercialError(f"{name} contains an unsupported cost component")
            if items is not None and len(set(items)) != len(items):
                raise CommercialError(f"{name} cannot contain duplicate components")
        if value.get("charge_base") is not None and value.get("markup_base") is not None:
            if not set(value["markup_base"]).issubset(value["charge_base"]):
                raise CommercialError("markup_base must be a subset of charge_base")
        value.update({"supplier_id": supplier, "offer_id": offer, "version": version,
                      "price_basis": basis.value, "payment_trigger": trigger.value,
                      "rounding_mode": rounding.value, "atomic_scale": scale})
        digest = _digest(value)
        with self._transaction() as db:
            old = db.execute("SELECT * FROM offers WHERE supplier_id=? AND offer_id=? AND version=?",
                             (supplier, offer, version)).fetchone()
            if old:
                if old["digest"] != digest:
                    raise IdempotencyConflict("offer version is already pinned with different terms")
                return self._offer_view(old)
            db.execute("""INSERT INTO offers
                (digest,supplier_id,offer_id,version,currency,atomic_scale,terms,created_at)
                VALUES (?,?,?,?,?,?,?,?)""",
                (digest, supplier, offer, version, currency, scale, _canonical(value), _now()))
            return self._offer_view(db.execute("SELECT * FROM offers WHERE digest=?", (digest,)).fetchone())

    @staticmethod
    def _offer_view(row: sqlite3.Row) -> dict[str, Any]:
        terms = json.loads(row["terms"])
        return {"offer_digest": row["digest"], "supplier_id": row["supplier_id"],
                "offer_id": row["offer_id"], "version": row["version"],
                "currency": row["currency"], "atomic_scale": row["atomic_scale"],
                "price_basis": terms["price_basis"], "payment_trigger": terms["payment_trigger"],
                "pricing_implementation": "local_usage_accounting" if terms["price_basis"] == "usage"
                                          else "represented_only", "terms": terms}

    def configure_budget(self, account_id: str, currency: str, atomic_scale: int,
                         limit_units: int,
                         *, budget_id: str) -> dict[str, Any]:
        account, curr, revision = _text(account_id, "account_id"), _text(currency, "currency"), _text(budget_id, "budget_id")
        scale = _int(atomic_scale, "atomic_scale")
        limit = _int(limit_units, "limit_units")
        assert scale is not None
        assert limit is not None
        with self._transaction() as db:
            old = db.execute("SELECT * FROM budgets WHERE account_id=? AND currency=? AND atomic_scale=?",
                             (account, curr, scale)).fetchone()
            if old and old["budget_id"] == revision and old["limit_units"] != limit:
                raise IdempotencyConflict("budget revision reused with a different limit")
            committed = self._account_committed(db, account, curr, scale)
            if committed > limit:
                raise BudgetExceeded("budget cannot be lowered below committed amounts")
            db.execute("""INSERT INTO budgets VALUES (?,?,?,?,?)
                ON CONFLICT(account_id,currency,atomic_scale) DO UPDATE SET budget_id=excluded.budget_id,
                limit_units=excluded.limit_units""", (account, curr, scale, revision, limit))
            return {"account_id": account, "currency": curr, "atomic_scale": scale, "budget_id": revision,
                    "limit_units": limit}

    def record_authorization(self, *, authorization_id: str, principal_id: str,
                             account_id: str, supplier_id: str, offer_digest: str,
                             run_id: str, assignment_id: str, attempt_id: str,
                             currency: str, atomic_scale: int, ceiling_units: int, expires_at: str,
                             evidence_ref: str | None = None) -> dict[str, Any]:
        args = {"authorization_id": _text(authorization_id, "authorization_id"),
                "principal_id": _text(principal_id, "principal_id"),
                "account_id": _text(account_id, "account_id"),
                "supplier_id": _text(supplier_id, "supplier_id"),
                "offer_digest": _text(offer_digest, "offer_digest"),
                "run_id": _text(run_id, "run_id"),
                "assignment_id": _text(assignment_id, "assignment_id"),
                "attempt_id": _text(attempt_id, "attempt_id"),
                "currency": _text(currency, "currency"),
                "atomic_scale": _int(atomic_scale, "atomic_scale"),
                "ceiling_units": _int(ceiling_units, "ceiling_units"),
                "expires_at": _timestamp(expires_at, "expires_at"),
                "evidence_ref": _optional_text(evidence_ref, "evidence_ref")}
        fingerprint = _digest(args)
        with self._transaction() as db:
            offer = db.execute("SELECT supplier_id,currency,atomic_scale FROM offers WHERE digest=?",
                               (args["offer_digest"],)).fetchone()
            if not offer:
                raise CommercialError("pinned offer not found")
            if offer["supplier_id"] != args["supplier_id"]:
                raise AuthorizationRejected("authorization supplier differs from pinned offer")
            if offer["currency"] and offer["currency"] != args["currency"]:
                raise AuthorizationRejected("authorization currency differs from pinned offer")
            if offer["atomic_scale"] != args["atomic_scale"]:
                raise AuthorizationRejected("authorization scale differs from pinned offer")
            old = db.execute("SELECT * FROM authorizations WHERE authorization_id=?",
                             (args["authorization_id"],)).fetchone()
            if old:
                if old["fingerprint"] != fingerprint:
                    raise IdempotencyConflict("authorization ID reused with different scope")
                return self._authorization_view(old)
            db.execute("""INSERT INTO authorizations
                (authorization_id,fingerprint,principal_id,account_id,supplier_id,offer_digest,
                 run_id,assignment_id,attempt_id,currency,atomic_scale,ceiling_units,expires_at,evidence_ref)
                 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                args["authorization_id"], fingerprint, args["principal_id"], args["account_id"],
                args["supplier_id"], args["offer_digest"], args["run_id"],
                args["assignment_id"], args["attempt_id"], args["currency"], args["atomic_scale"],
                args["ceiling_units"], args["expires_at"], args["evidence_ref"]))
            return self._authorization_view(db.execute(
                "SELECT * FROM authorizations WHERE authorization_id=?",
                (args["authorization_id"],)).fetchone())

    @staticmethod
    def _authorization_view(row: sqlite3.Row) -> dict[str, Any]:
        return {key: row[key] for key in (
            "authorization_id", "principal_id", "account_id", "supplier_id", "offer_digest",
            "run_id", "assignment_id", "attempt_id", "currency", "atomic_scale", "ceiling_units", "expires_at",
            "evidence_ref")} | {"revoked": bool(row["revoked"])}

    def reserve_purchase(self, *, purchase_id: str, idempotency_key: str,
                         authorization_id: str, offer_digest: str, run_id: str,
                         assignment_id: str, attempt_id: str, service_identity: str,
                         currency: str, atomic_scale: int, ceiling_units: int,
                         task_id: str | None = None) -> dict[str, Any]:
        args = {"purchase_id": _text(purchase_id, "purchase_id"),
                "idempotency_key": _text(idempotency_key, "idempotency_key"),
                "authorization_id": _text(authorization_id, "authorization_id"),
                "offer_digest": _text(offer_digest, "offer_digest"),
                "run_id": _text(run_id, "run_id"),
                "assignment_id": _text(assignment_id, "assignment_id"),
                "attempt_id": _text(attempt_id, "attempt_id"),
                "service_identity": _text(service_identity, "service_identity"),
                "currency": _text(currency, "currency"),
                "atomic_scale": _int(atomic_scale, "atomic_scale"),
                "ceiling_units": _int(ceiling_units, "ceiling_units"),
                "task_id": _optional_text(task_id, "task_id")}
        fingerprint = _digest(args)
        with self._transaction() as db:
            old = db.execute("SELECT * FROM purchases WHERE purchase_id=? OR idempotency_key=?",
                             (args["purchase_id"], args["idempotency_key"])).fetchone()
            if old:
                if old["fingerprint"] != fingerprint:
                    raise IdempotencyConflict("purchase identity reused with different intent")
                return self._purchase_view(db, old["purchase_id"])
            auth = db.execute("SELECT * FROM authorizations WHERE authorization_id=?",
                              (args["authorization_id"],)).fetchone()
            if not auth or auth["revoked"]:
                raise AuthorizationRejected("purchase authority is absent or revoked")
            if datetime.fromisoformat(auth["expires_at"]) <= datetime.now(timezone.utc):
                raise AuthorizationRejected("purchase authority has expired")
            offer = db.execute("SELECT * FROM offers WHERE digest=?", (args["offer_digest"],)).fetchone()
            if not offer:
                raise CommercialError("pinned offer not found")
            terms = json.loads(offer["terms"])
            if terms["price_basis"] != PriceBasis.USAGE.value:
                raise UnsupportedPriceBasis("only usage pricing is implemented locally")
            if (args["offer_digest"] != auth["offer_digest"] or
                    offer["supplier_id"] != auth["supplier_id"] or
                    args["run_id"] != auth["run_id"] or
                    args["assignment_id"] != auth["assignment_id"] or
                    args["attempt_id"] != auth["attempt_id"] or
                    args["ceiling_units"] > auth["ceiling_units"]):
                raise AuthorizationRejected("purchase exceeds its exact authorization scope")
            currency = offer["currency"]
            scale = offer["atomic_scale"]
            if (not currency or auth["currency"] != currency or
                    args["currency"] != currency or auth["atomic_scale"] != scale or
                    args["atomic_scale"] != scale):
                raise AuthorizationRejected("offer or authorization currency is unknown or inconsistent")
            budget = db.execute("SELECT * FROM budgets WHERE account_id=? AND currency=? AND atomic_scale=?",
                                (auth["account_id"], currency, scale)).fetchone()
            if not budget:
                raise AuthorizationRejected("no explicit account budget is configured")
            committed = self._account_committed(db, auth["account_id"], currency, scale)
            if committed + args["ceiling_units"] > budget["limit_units"]:
                raise BudgetExceeded("atomic reservation exceeds remaining spend budget")
            db.execute("""INSERT INTO purchases
                (purchase_id,idempotency_key,fingerprint,authorization_id,account_id,principal_id,
                 supplier_id,offer_digest,run_id,assignment_id,attempt_id,service_identity,task_id,
                 currency,atomic_scale,ceiling_units) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                args["purchase_id"], args["idempotency_key"], fingerprint, auth["authorization_id"],
                auth["account_id"], auth["principal_id"], offer["supplier_id"], offer["digest"],
                args["run_id"], args["assignment_id"], args["attempt_id"], args["service_identity"],
                args["task_id"], currency, scale, args["ceiling_units"]))
            db.execute("INSERT INTO reservations VALUES (?,?,?,?,?,?)",
                       (args["purchase_id"], auth["account_id"], currency, scale,
                        args["ceiling_units"], "held"))
            return self._purchase_view(db, args["purchase_id"])

    def bind_remote_task(self, purchase_id: str, task_id: str) -> dict[str, Any]:
        purchase, task = _text(purchase_id, "purchase_id"), _text(task_id, "task_id")
        with self._transaction() as db:
            row = self._purchase_row(db, purchase)
            if row["closed"]:
                raise CommercialError("closed purchase cannot bind a Task")
            if row["task_id"] and row["task_id"] != task:
                raise IdempotencyConflict("purchase already has a different Task binding")
            db.execute("UPDATE purchases SET task_id=? WHERE purchase_id=?", (task, purchase))
            return self._purchase_view(db, purchase)

    def record_usage(self, *, usage_id: str, purchase_id: str, unit: str,
                     quantity: int | None, evidence_state: EvidenceState | str,
                     source: str, service_identity: str, assignment_id: str,
                     attempt_id: str, task_id: str | None = None,
                     model_call_id: str | None = None, evidence_ref: str | None = None,
                     late: bool = False) -> dict[str, Any]:
        state = _evidence(evidence_state)
        if quantity is None:
            if state not in {EvidenceState.UNKNOWN, EvidenceState.UNDISCLOSED}:
                raise CommercialError("missing usage must be unknown or undisclosed")
        else:
            _int(quantity, "quantity")
            if state in {EvidenceState.UNKNOWN, EvidenceState.UNDISCLOSED}:
                raise CommercialError("unknown usage cannot carry a numeric quantity")
        if type(late) is not bool:
            raise CommercialError("late must be an explicit boolean")
        args = {"usage_id": _text(usage_id, "usage_id"),
                "purchase_id": _text(purchase_id, "purchase_id"),
                "unit": _text(unit, "unit"), "quantity": quantity,
                "evidence_state": state.value, "source": _text(source, "source"),
                "service_identity": _text(service_identity, "service_identity"),
                "assignment_id": _text(assignment_id, "assignment_id"),
                "attempt_id": _text(attempt_id, "attempt_id"),
                "task_id": _optional_text(task_id, "task_id"),
                "model_call_id": _optional_text(model_call_id, "model_call_id"),
                "evidence_ref": _optional_text(evidence_ref, "evidence_ref"), "late": late}
        fingerprint = _digest(args)
        with self._transaction() as db:
            purchase = self._purchase_row(db, args["purchase_id"])
            old = db.execute("SELECT * FROM usage_records WHERE usage_id=?",
                             (args["usage_id"],)).fetchone()
            if old:
                if old["fingerprint"] != fingerprint:
                    raise IdempotencyConflict("usage ID reused with changed attribution or amount")
                return self._purchase_view(db, args["purchase_id"])
            if purchase["closed"]:
                raise CommercialError("closed purchase cannot accept late usage")
            if (args["service_identity"] != purchase["service_identity"] or
                    args["assignment_id"] != purchase["assignment_id"] or
                    args["attempt_id"] != purchase["attempt_id"] or
                    (purchase["task_id"] and args["task_id"] != purchase["task_id"])):
                raise AuthorizationRejected("usage does not match purchase attribution")
            terms = json.loads(self._offer_row(db, purchase["offer_digest"])["terms"])
            if args["unit"] not in terms.get("required_usage_units", []):
                raise AuthorizationRejected("usage unit is absent from the pinned offer")
            if purchase["usage_complete"]:
                policy = terms.get("late_usage_policy") or {}
                if not (late and policy.get("accept_after_completion") is True):
                    raise CommercialError("late usage is not explicitly allowed by pinned terms")
                db.execute("UPDATE purchases SET usage_complete=0 WHERE purchase_id=?",
                           (args["purchase_id"],))
            elif late:
                policy = terms.get("late_usage_policy") or {}
                prior_late = db.execute(
                    "SELECT 1 FROM usage_records WHERE purchase_id=? AND late=1 LIMIT 1",
                    (args["purchase_id"],)).fetchone()
                if not (prior_late and policy.get("accept_after_completion") is True):
                    raise CommercialError("late usage requires a previously complete usage set")
            db.execute("""INSERT INTO usage_records
                (usage_id,purchase_id,fingerprint,unit,quantity,evidence_state,source,evidence_ref,
                 service_identity,assignment_id,attempt_id,task_id,model_call_id,late,recorded_at)
                 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                args["usage_id"], args["purchase_id"], fingerprint, args["unit"],
                args["quantity"], args["evidence_state"], args["source"], args["evidence_ref"],
                args["service_identity"], args["assignment_id"], args["attempt_id"],
                args["task_id"], args["model_call_id"], int(late), _now()))
            self._persist_assessment(db, args["purchase_id"])
            return self._purchase_view(db, args["purchase_id"])

    def mark_usage_complete(self, purchase_id: str, *, evidence_ref: str,
                            idempotency_key: str) -> dict[str, Any]:
        purchase, ref, key = (_text(purchase_id, "purchase_id"),
                              _text(evidence_ref, "evidence_ref"),
                              _text(idempotency_key, "idempotency_key"))
        fingerprint = _digest({"purchase_id": purchase, "evidence_ref": ref})
        with self._transaction() as db:
            row = self._purchase_row(db, purchase)
            old = db.execute("SELECT fingerprint FROM completion_keys WHERE idempotency_key=?",
                             (key,)).fetchone()
            if old and old["fingerprint"] != fingerprint:
                raise IdempotencyConflict("usage-completion key reused differently")
            if row["usage_complete"]:
                return self._purchase_view(db, purchase)
            if row["closed"]:
                raise CommercialError("closed purchase cannot change usage completeness")
            if not old:
                db.execute("INSERT INTO completion_keys VALUES (?,?)", (key, fingerprint))
            db.execute("UPDATE purchases SET usage_complete=1 WHERE purchase_id=?", (purchase,))
            self._persist_assessment(db, purchase)
            return self._purchase_view(db, purchase)

    def _purchase_row(self, db: sqlite3.Connection, purchase_id: str) -> sqlite3.Row:
        row = db.execute("SELECT * FROM purchases WHERE purchase_id=?", (purchase_id,)).fetchone()
        if row is None:
            raise CommercialError("purchase not found")
        return row

    def _offer_row(self, db: sqlite3.Connection, digest: str) -> sqlite3.Row:
        row = db.execute("SELECT * FROM offers WHERE digest=?", (digest,)).fetchone()
        if row is None:
            raise CommercialError("pinned offer not found")
        return row

    def _calculate(self, terms: Mapping[str, Any], rows: Sequence[sqlite3.Row],
                   complete: bool, currency: str, atomic_scale: int) -> dict[str, Any]:
        if terms["price_basis"] != PriceBasis.USAGE.value:
            unknown = _money(None, currency, atomic_scale, EvidenceState.UNKNOWN,
                             {"reason": "basis is represented only"})
            return {"pricing_implementation": "represented_only", "usage_complete": complete,
                    "inference_cost": unknown, "hosting_cost": unknown, "markup_base": unknown,
                    "markup": unknown, "supplier_charge": unknown, "unit_costs": {},
                    "warnings": ["price basis is not calculated locally"]}
        try:
            rounding = RoundingMode(terms["rounding_mode"])
        except (KeyError, ValueError) as error:
            raise CommercialError("pinned rounding mode is invalid") from error
        usage: dict[str, list[sqlite3.Row]] = {}
        for row in rows:
            usage.setdefault(row["unit"], []).append(row)
        rates = {rate["unit"]: rate for rate in terms.get("rates", [])}
        unit_costs: dict[str, Any] = {}
        sums = {"inference_cost": 0, "hosting_cost": 0}
        states: dict[str, list[EvidenceState]] = {"inference_cost": [], "hosting_cost": []}
        warnings: list[str] = []
        total_known = complete
        for unit in terms.get("required_usage_units", []):
            records, rate = usage.get(unit, []), rates.get(unit)
            if not records:
                unit_costs[unit] = {"quantity": None, "amount_units": None,
                                    "currency": currency, "atomic_scale": atomic_scale,
                                    "evidence": "unknown"}
                total_known = False
                warnings.append(f"required usage unit {unit} has no record")
                if rate:
                    states[rate["component"]].append(EvidenceState.UNKNOWN)
                continue
            unknown = [EvidenceState(record["evidence_state"]) for record in records
                       if record["quantity"] is None or record["evidence_state"] in
                       {EvidenceState.UNKNOWN.value, EvidenceState.UNDISCLOSED.value}]
            if unknown:
                state = _unknown_state(unknown)
                unit_costs[unit] = {"quantity": None, "amount_units": None,
                                    "currency": currency, "atomic_scale": atomic_scale,
                                    "evidence": state.value,
                                    "usage_ids": [record["usage_id"] for record in records]}
                total_known = False
                if rate:
                    states[rate["component"]].append(state)
                continue
            count = sum(record["quantity"] for record in records)
            if rate is None or rate.get("numerator") is None:
                state = EvidenceState((rate or {}).get("evidence_state", "unknown"))
                unit_costs[unit] = {"quantity": count, "amount_units": None,
                                    "currency": currency, "atomic_scale": atomic_scale,
                                    "evidence": state.value, "reason": "rate unavailable"}
                states[(rate or {}).get("component", "inference_cost")].append(state)
                total_known = False
                continue
            amount = _round_ratio(count * rate["numerator"], rate["denominator"], rounding)
            source_states = [EvidenceState(record["evidence_state"]) for record in records]
            rate_state = EvidenceState(rate.get("evidence_state", "provider_reported"))
            state = (EvidenceState.ESTIMATED if EvidenceState.ESTIMATED in source_states
                     or rate_state == EvidenceState.ESTIMATED else EvidenceState.CALCULATED)
            unit_costs[unit] = {"quantity": count, "quantity_evidence": _combine(source_states).value,
                                "amount_units": amount, "currency": currency,
                                "atomic_scale": atomic_scale, "evidence": state.value,
                                "component": rate["component"],
                                "rate": {"numerator": rate["numerator"],
                                         "denominator": rate["denominator"],
                                         "evidence_state": rate_state.value,
                                         "source_ref": rate.get("source_ref")},
                                "usage_ids": [record["usage_id"] for record in records]}
            sums[rate["component"]] += amount
            states[rate["component"]].append(state)
        if not complete:
            total_known = False
            warnings.append("usage collection is not explicitly complete")
        component_money = {}
        for component in sums:
            unknown_states = [state for state in states[component]
                              if state in {EvidenceState.UNKNOWN, EvidenceState.UNDISCLOSED}]
            if unknown_states or not states[component]:
                evidence = _unknown_state(unknown_states) if unknown_states else EvidenceState.UNKNOWN
                component_money[component] = _money(None, currency, atomic_scale, evidence)
            else:
                evidence = EvidenceState.ESTIMATED if EvidenceState.ESTIMATED in states[component] else EvidenceState.CALCULATED
                component_money[component] = _money(sums[component], currency, atomic_scale, evidence)

        charge_base, markup_base, bps = terms.get("charge_base"), terms.get("markup_base"), terms.get("markup_bps")
        markup_state = EvidenceState(terms.get("markup_evidence_state", "unknown" if bps is None else "provider_reported"))
        if charge_base is None or markup_base is None or bps is None:
            missing = markup_state if bps is None else EvidenceState.UNKNOWN
            base_money = _money(None, currency, atomic_scale, missing, {"components": markup_base})
            markup_money = _money(None, currency, atomic_scale, missing,
                                  {"basis_components": markup_base, "markup_bps": None})
            charge_money = _money(None, currency, atomic_scale, missing,
                                  {"reason": "charge terms incomplete"})
            total_known = False
        else:
            names = set(charge_base) | set(markup_base)
            unknown_components = [name for name in names if name not in component_money or
                                  component_money[name]["amount_units"] is None]
            if unknown_components:
                missing_states = [EvidenceState(component_money[name]["evidence"])
                                  for name in unknown_components if name in component_money]
                missing = _unknown_state(missing_states)
                base_money = _money(None, currency, atomic_scale, missing, {"components": markup_base})
                markup_money = _money(None, currency, atomic_scale, missing, {"basis_components": markup_base,
                                                                 "markup_bps": bps})
                charge_money = _money(None, currency, atomic_scale, missing, {"charge_base": charge_base})
                total_known = False
            else:
                base = sum(component_money[name]["amount_units"] for name in charge_base)
                markup_base_amount = sum(component_money[name]["amount_units"] for name in markup_base)
                markup_amount = _round_ratio(markup_base_amount * bps, 10_000, rounding)
                all_states = [EvidenceState(component_money[name]["evidence"]) for name in names]
                result_state = EvidenceState.ESTIMATED if EvidenceState.ESTIMATED in all_states else EvidenceState.CALCULATED
                base_money = _money(markup_base_amount, currency, atomic_scale, result_state,
                                    {"components": markup_base})
                markup_money = _money(markup_amount, currency, atomic_scale, result_state,
                                      {"basis_components": markup_base, "markup_bps": bps,
                                       "rounding_mode": rounding.value})
                charge_money = _money(base + markup_amount, currency, atomic_scale, result_state,
                                      {"charge_base": charge_base, "markup_base": markup_base,
                                       "markup_bps": bps, "rounding_mode": rounding.value})
                if not total_known:
                    charge_money = _money(None, currency, atomic_scale, EvidenceState.UNKNOWN,
                        {"reason": "usage incomplete", "partial_amount_units": base + markup_amount})
        return {"pricing_implementation": "local_usage_accounting", "usage_complete": complete,
                "inference_cost": component_money["inference_cost"],
                "hosting_cost": component_money["hosting_cost"],
                "charge_base": charge_base,
                "markup_base": base_money, "markup": markup_money,
                "supplier_charge": charge_money, "unit_costs": unit_costs,
                "warnings": warnings}

    def _persist_assessment(self, db: sqlite3.Connection, purchase_id: str) -> dict[str, Any]:
        purchase = self._purchase_row(db, purchase_id)
        terms = json.loads(self._offer_row(db, purchase["offer_digest"])["terms"])
        rows = db.execute("SELECT * FROM usage_records WHERE purchase_id=? ORDER BY usage_id",
                          (purchase_id,)).fetchall()
        details = self._calculate(terms, rows, bool(purchase["usage_complete"]),
                                  purchase["currency"], purchase["atomic_scale"])
        inputs = {"offer_digest": purchase["offer_digest"],
                  "usage_ids": [row["usage_id"] for row in rows],
                  "complete": bool(purchase["usage_complete"])}
        fingerprint = _digest(inputs)
        obligation_id = "obl-" + fingerprint[:28]
        db.execute("""INSERT OR IGNORE INTO obligations
            (obligation_id,purchase_id,digest,amount_units,currency,atomic_scale,evidence_state,details,created_at)
            VALUES (?,?,?,?,?,?,?,?,?)""", (
            obligation_id, purchase_id, fingerprint, details["supplier_charge"]["amount_units"],
            purchase["currency"], purchase["atomic_scale"],
            details["supplier_charge"]["evidence"], _canonical(details), _now()))
        return self._obligation_view(db.execute(
            "SELECT * FROM obligations WHERE obligation_id=?", (obligation_id,)).fetchone())

    @staticmethod
    def _obligation_view(row: sqlite3.Row) -> dict[str, Any]:
        return {"obligation_id": row["obligation_id"], "purchase_id": row["purchase_id"],
                "amount_units": row["amount_units"], "currency": row["currency"],
                "atomic_scale": row["atomic_scale"], "evidence_state": row["evidence_state"],
                "details": json.loads(row["details"]), "created_at": row["created_at"]}

    def begin_settlement(self, *, settlement_id: str, idempotency_key: str,
                         purchase_id: str, adapter_profile: str,
                         direction: str = "charge") -> dict[str, Any]:
        """Record an intent for later adapter work; never performs that work."""
        if adapter_profile != "test-only-recorded":
            self.adapters.require_invokable(adapter_profile)
        if direction not in {"charge", "refund"}:
            raise CommercialError("direction must be charge or refund")
        sid, key, purchase = (_text(settlement_id, "settlement_id"),
                              _text(idempotency_key, "idempotency_key"),
                              _text(purchase_id, "purchase_id"))
        with self._transaction() as db:
            row = self._purchase_row(db, purchase)
            fingerprint = _digest({"settlement_id": sid, "purchase_id": purchase,
                                   "adapter_profile": adapter_profile, "direction": direction,
                                   "currency": row["currency"],
                                   "atomic_scale": row["atomic_scale"]})
            old = db.execute("SELECT * FROM settlements WHERE settlement_id=? OR idempotency_key=?",
                             (sid, key)).fetchone()
            if old:
                if old["fingerprint"] != fingerprint or old["idempotency_key"] != key:
                    raise IdempotencyConflict("settlement identity reused with different intent")
                return self._settlement_view(old)
            if row["closed"]:
                raise CommercialError("closed purchase cannot settle")
            if db.execute("SELECT 1 FROM settlements WHERE purchase_id=? AND state IN (?,?) LIMIT 1",
                (purchase, SettlementState.PENDING.value, SettlementState.UNRESOLVED.value)).fetchone():
                raise CommercialError("reconcile the unresolved settlement before another attempt")
            obligation = self._persist_assessment(db, purchase)
            amount_due = obligation["amount_units"]
            if amount_due is None:
                raise CommercialError("unknown obligation cannot be settled")
            if direction == "charge" and amount_due > row["ceiling_units"]:
                raise AuthorizationRejected("supplier charge exceeds reserved purchase authority")
            paid = self._settled(db, purchase, "charge")
            refunded = self._settled(db, purchase, "refund")
            credited = self._credited(db, purchase)
            if direction == "charge":
                amount = max(amount_due - credited - paid + refunded, 0)
            else:
                amount = max(paid - max(amount_due - credited, 0) - refunded, 0)
            if amount <= 0:
                raise CommercialError(f"no {direction} is currently due")
            db.execute("""INSERT INTO settlements
                (settlement_id,idempotency_key,fingerprint,purchase_id,obligation_id,direction,
                 adapter_profile,amount_units,currency,atomic_scale,state,recorded_at)
                 VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", (
                sid, key, fingerprint, purchase, obligation["obligation_id"], direction,
                adapter_profile, amount, row["currency"], row["atomic_scale"],
                SettlementState.PENDING.value, _now()))
            return self._settlement_view(db.execute(
                "SELECT * FROM settlements WHERE settlement_id=?", (sid,)).fetchone())

    def mark_settlement_unresolved(self, settlement_id: str, *, reason: str,
                                   evidence_ref: str | None = None) -> dict[str, Any]:
        sid = _text(settlement_id, "settlement_id")
        reason_value = _text(reason, "reason")
        ref = _optional_text(evidence_ref, "evidence_ref")
        with self._transaction() as db:
            row = db.execute("SELECT * FROM settlements WHERE settlement_id=?", (sid,)).fetchone()
            if not row:
                raise CommercialError("settlement not found")
            if row["state"] in {SettlementState.SETTLED.value, SettlementState.FAILED.value}:
                raise CommercialError("terminal settlement cannot become unresolved")
            db.execute("UPDATE settlements SET state=?,receipt_ref=?,evidence_state=?,recorded_at=? WHERE settlement_id=?",
                       (SettlementState.UNRESOLVED.value,
                        _canonical({"reason": reason_value, "evidence_ref": ref}),
                        EvidenceState.UNKNOWN.value, _now(), sid))
            return self._settlement_view(db.execute(
                "SELECT * FROM settlements WHERE settlement_id=?", (sid,)).fetchone())

    def reconcile_settlement(self, settlement_id: str, *, result: str,
                             currency: str, atomic_scale: int,
                             receipt_ref: str | None = None,
                             amount_units: int | None = None,
                             evidence_state: EvidenceState | str = EvidenceState.PROVIDER_REPORTED) -> dict[str, Any]:
        sid, ref = _text(settlement_id, "settlement_id"), _optional_text(receipt_ref, "receipt_ref")
        curr = _text(currency, "currency")
        scale = _int(atomic_scale, "atomic_scale")
        assert scale is not None
        state = _evidence(evidence_state)
        if result not in {"settled", "failed", "unresolved"}:
            raise CommercialError("result must be settled, failed, or unresolved")
        if amount_units is not None:
            _int(amount_units, "amount_units")
        with self._transaction() as db:
            row = db.execute("SELECT * FROM settlements WHERE settlement_id=?", (sid,)).fetchone()
            if not row:
                raise CommercialError("settlement not found")
            if curr != row["currency"] or scale != row["atomic_scale"]:
                raise ReconciliationConflict("settlement receipt currency or atomic scale differs from intent")
            if row["state"] in {SettlementState.SETTLED.value, SettlementState.FAILED.value}:
                if (result == row["state"] and row["receipt_ref"] == ref
                        and row["evidence_state"] == state.value
                        and (result == "failed" or row["amount_units"] == amount_units)):
                    return self._settlement_view(row)
                raise IdempotencyConflict("terminal settlement evidence is immutable")
            if result == "settled":
                if not ref or amount_units != row["amount_units"]:
                    raise ReconciliationConflict("settlement receipt differs from requested atomic amount")
                if state in {EvidenceState.UNKNOWN, EvidenceState.UNDISCLOSED}:
                    raise ReconciliationConflict("settlement needs explicit receipt evidence")
                next_state = SettlementState.SETTLED.value
            elif result == "failed":
                if not ref or state in {EvidenceState.UNKNOWN, EvidenceState.UNDISCLOSED}:
                    raise ReconciliationConflict("failure needs explicit evidence")
                next_state = SettlementState.FAILED.value
            else:
                next_state = SettlementState.UNRESOLVED.value
                state = EvidenceState.UNKNOWN
            db.execute("UPDATE settlements SET state=?,receipt_ref=?,evidence_state=?,recorded_at=? WHERE settlement_id=?",
                       (next_state, ref, state.value, _now(), sid))
            return self._settlement_view(db.execute(
                "SELECT * FROM settlements WHERE settlement_id=?", (sid,)).fetchone())

    @staticmethod
    def _settlement_view(row: sqlite3.Row) -> dict[str, Any]:
        ref = row["receipt_ref"]
        if row["state"] == SettlementState.UNRESOLVED.value and ref:
            try:
                ref = json.loads(ref)
            except ValueError:
                pass
        return {key: row[key] for key in (
            "settlement_id", "idempotency_key", "purchase_id", "obligation_id", "direction",
            "adapter_profile", "amount_units", "currency", "atomic_scale", "state", "evidence_state",
            "recorded_at")} | {
                "receipt_ref": ref}

    def record_credit(self, *, credit_id: str, idempotency_key: str,
                      purchase_id: str, currency: str, atomic_scale: int,
                      amount_units: int, reason: str,
                      evidence_state: EvidenceState | str,
                      evidence_ref: str | None = None) -> dict[str, Any]:
        cid, key, purchase = (_text(credit_id, "credit_id"), _text(idempotency_key, "idempotency_key"),
                              _text(purchase_id, "purchase_id"))
        amount = _int(amount_units, "amount_units", minimum=1)
        curr = _text(currency, "currency")
        scale = _int(atomic_scale, "atomic_scale")
        assert scale is not None
        state, reason_value = _evidence(evidence_state), _text(reason, "reason")
        ref = _optional_text(evidence_ref, "evidence_ref")
        if state in {EvidenceState.UNKNOWN, EvidenceState.UNDISCLOSED}:
            raise CommercialError("numeric credit requires explicit evidence")
        fingerprint = _digest({"credit_id": cid, "purchase_id": purchase,
                               "currency": curr, "atomic_scale": scale,
                               "amount_units": amount, "reason": reason_value,
                               "evidence_state": state.value, "evidence_ref": ref})
        with self._transaction() as db:
            row = self._purchase_row(db, purchase)
            if curr != row["currency"] or scale != row["atomic_scale"]:
                raise AuthorizationRejected("credit currency or scale differs from purchase")
            old = db.execute("SELECT * FROM credits WHERE credit_id=? OR idempotency_key=?",
                             (cid, key)).fetchone()
            if old:
                if old["fingerprint"] != fingerprint or old["idempotency_key"] != key:
                    raise IdempotencyConflict("credit identity reused with different intent")
                return self._credit_view(old)
            obligation = self._persist_assessment(db, purchase)
            if obligation["amount_units"] is None:
                raise CommercialError("cannot credit an unknown obligation")
            if self._credited(db, purchase) + amount > obligation["amount_units"]:
                raise CommercialError("credits cannot exceed the pinned supplier charge")
            db.execute("""INSERT INTO credits
                (credit_id,idempotency_key,fingerprint,purchase_id,amount_units,currency,atomic_scale,reason,
                 evidence_state,evidence_ref,recorded_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)""", (
                cid, key, fingerprint, purchase, amount, curr, scale, reason_value,
                state.value, ref, _now()))
            return self._credit_view(db.execute("SELECT * FROM credits WHERE credit_id=?", (cid,)).fetchone())

    @staticmethod
    def _credit_view(row: sqlite3.Row) -> dict[str, Any]:
        return {key: row[key] for key in (
            "credit_id", "idempotency_key", "purchase_id", "amount_units", "currency", "atomic_scale",
            "reason", "evidence_state", "evidence_ref", "recorded_at")}

    def record_economic_fact(self, *, fact_id: str, idempotency_key: str, kind: str,
                             account_id: str, run_id: str, currency: str, atomic_scale: int,
                             amount_units: int | None, evidence_state: EvidenceState | str,
                             source: str, basis: Mapping[str, Any] | None = None,
                             purchase_id: str | None = None,
                             assignment_id: str | None = None,
                             attempt_id: str | None = None) -> dict[str, Any]:
        """Append payment fee, owner overhead, or independent caller price evidence."""
        if kind not in {"payment_fee", "owner_overhead", "customer_price"}:
            raise CommercialError("economic fact kind must be payment_fee, owner_overhead, or customer_price")
        state = _evidence(evidence_state)
        curr = _text(currency, "currency")
        scale = _int(atomic_scale, "atomic_scale")
        assert scale is not None
        if amount_units is None:
            if state not in {EvidenceState.UNKNOWN, EvidenceState.UNDISCLOSED}:
                raise CommercialError("missing fact amount must be unknown or undisclosed")
        else:
            _int(amount_units, "amount_units")
            if state in {EvidenceState.UNKNOWN, EvidenceState.UNDISCLOSED}:
                raise CommercialError("unknown fact cannot contain an amount")
        args = {"fact_id": _text(fact_id, "fact_id"),
                "kind": kind, "account_id": _text(account_id, "account_id"),
                "run_id": _text(run_id, "run_id"), "currency": curr, "atomic_scale": scale,
                "amount_units": amount_units, "evidence_state": state.value,
                "source": _text(source, "source"), "basis": dict(basis or {}),
                "purchase_id": _optional_text(purchase_id, "purchase_id"),
                "assignment_id": _optional_text(assignment_id, "assignment_id"),
                "attempt_id": _optional_text(attempt_id, "attempt_id")}
        _reject_floats(args)
        fingerprint = _digest(args)
        key = _text(idempotency_key, "idempotency_key")
        with self._transaction() as db:
            if args["purchase_id"]:
                purchase = self._purchase_row(db, args["purchase_id"])
                if (purchase["account_id"] != args["account_id"] or
                    purchase["run_id"] != args["run_id"] or purchase["currency"] != curr or
                    purchase["atomic_scale"] != scale):
                    raise AuthorizationRejected("economic fact does not match its purchase scope")
                if ((args["assignment_id"] or args["attempt_id"]) and
                    (args["assignment_id"] != purchase["assignment_id"] or
                     args["attempt_id"] != purchase["attempt_id"])):
                    raise AuthorizationRejected("economic fact attribution differs from its purchase")
            if bool(args["assignment_id"]) != bool(args["attempt_id"]):
                raise CommercialError("assignment_id and attempt_id must be supplied together")
            old = db.execute("SELECT * FROM economic_facts WHERE fact_id=? OR idempotency_key=?",
                             (args["fact_id"], key)).fetchone()
            if old:
                if old["fingerprint"] != fingerprint or old["idempotency_key"] != key:
                    raise IdempotencyConflict("economic fact identity reused with changed content")
                return self._fact_view(old)
            db.execute("""INSERT INTO economic_facts
                (fact_id,idempotency_key,fingerprint,kind,account_id,run_id,purchase_id,
                 assignment_id,attempt_id,currency,atomic_scale,amount_units,evidence_state,source,basis)
                 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                args["fact_id"], key, fingerprint, kind, args["account_id"], args["run_id"],
                args["purchase_id"], args["assignment_id"], args["attempt_id"], curr,
                scale, amount_units, state.value, args["source"], _canonical(args["basis"])))
            return self._fact_view(db.execute("SELECT * FROM economic_facts WHERE fact_id=?",
                                              (args["fact_id"],)).fetchone())

    @staticmethod
    def _fact_view(row: sqlite3.Row) -> dict[str, Any]:
        return {key: row[key] for key in (
            "fact_id", "idempotency_key", "kind", "account_id", "run_id", "purchase_id",
            "assignment_id", "attempt_id", "currency", "atomic_scale", "amount_units", "evidence_state",
            "source")} | {"basis": json.loads(row["basis"])}

    def close_purchase(self, purchase_id: str, *, evidence_ref: str) -> dict[str, Any]:
        purchase, ref = _text(purchase_id, "purchase_id"), _text(evidence_ref, "evidence_ref")
        with self._transaction() as db:
            row = self._purchase_row(db, purchase)
            if row["closed"]:
                return self._purchase_view(db, purchase)
            if not row["usage_complete"]:
                raise CommercialError("usage must be explicitly complete before close")
            obligation = self._persist_assessment(db, purchase)
            if obligation["amount_units"] is None:
                raise CommercialError("unknown obligation keeps its reservation")
            if db.execute("SELECT 1 FROM settlements WHERE purchase_id=? AND state IN (?,?)",
                (purchase, SettlementState.PENDING.value, SettlementState.UNRESOLVED.value)).fetchone():
                raise CommercialError("unresolved settlement keeps its reservation")
            gross = obligation["amount_units"]
            net = max(gross - self._credited(db, purchase), 0)
            paid = self._settled(db, purchase, "charge") - self._settled(db, purchase, "refund")
            if paid != net:
                raise CommercialError("unpaid obligation or unprocessed refund keeps its reservation")
            db.execute("UPDATE purchases SET closed=1 WHERE purchase_id=?", (purchase,))
            db.execute("UPDATE reservations SET amount_units=0,state='released' WHERE purchase_id=?", (purchase,))
            return self._purchase_view(db, purchase)

    def get_purchase(self, purchase_id: str) -> dict[str, Any]:
        with self._connect() as db:
            return self._purchase_view(db, _text(purchase_id, "purchase_id"))

    def list_purchases(self, *, run_id: str | None = None) -> list[dict[str, Any]]:
        """Return safe purchase projections in stable run and purchase order."""
        run = _optional_text(run_id, "run_id")
        with self._connect() as db:
            if run is None:
                rows = db.execute("SELECT purchase_id FROM purchases ORDER BY run_id,purchase_id").fetchall()
            else:
                rows = db.execute("SELECT purchase_id FROM purchases WHERE run_id=? ORDER BY run_id,purchase_id",
                                  (run,)).fetchall()
            return [self._purchase_view(db, row["purchase_id"]) for row in rows]

    def _purchase_view(self, db: sqlite3.Connection, purchase_id: str) -> dict[str, Any]:
        row = self._purchase_row(db, purchase_id)
        offer_row = self._offer_row(db, row["offer_digest"])
        if row["atomic_scale"] != offer_row["atomic_scale"] or row["currency"] != offer_row["currency"]:
            raise CommercialError("purchase denomination differs from its pinned offer")
        terms = json.loads(offer_row["terms"])
        usage_rows = db.execute("SELECT * FROM usage_records WHERE purchase_id=? ORDER BY usage_id",
                                (purchase_id,)).fetchall()
        obligation_row = db.execute("SELECT * FROM obligations WHERE purchase_id=? ORDER BY rowid DESC LIMIT 1",
                                    (purchase_id,)).fetchone()
        if obligation_row:
            obligation = self._obligation_view(obligation_row)
            costs = obligation["details"]
        else:
            costs = self._calculate(terms, [], bool(row["usage_complete"]),
                                    row["currency"], row["atomic_scale"])
            obligation = None
        reservation = db.execute("SELECT * FROM reservations WHERE purchase_id=?", (purchase_id,)).fetchone()
        if reservation["currency"] != row["currency"] or reservation["atomic_scale"] != row["atomic_scale"]:
            raise CommercialError("reservation denomination differs from purchase")
        settlements = [self._settlement_view(item) for item in db.execute(
            "SELECT * FROM settlements WHERE purchase_id=? ORDER BY rowid", (purchase_id,)).fetchall()]
        credits = [self._credit_view(item) for item in db.execute(
            "SELECT * FROM credits WHERE purchase_id=? ORDER BY rowid", (purchase_id,)).fetchall()]
        facts = [self._fact_view(item) for item in db.execute(
            """SELECT * FROM economic_facts WHERE account_id=? AND run_id=? AND
               (purchase_id=? OR (purchase_id IS NULL AND assignment_id=? AND attempt_id=?))""",
            (row["account_id"], row["run_id"], purchase_id,
             row["assignment_id"], row["attempt_id"])).fetchall()]
        charge = costs["supplier_charge"]
        gross, curr = charge["amount_units"], row["currency"]
        paid = self._settled(db, purchase_id, "charge")
        refunded = self._settled(db, purchase_id, "refund")
        credited = self._credited(db, purchase_id)
        if gross is None:
            outstanding = refund_due = None
            payment_state = "unknown"
        else:
            outstanding = max(gross - credited - paid + refunded, 0)
            refund_due = max(paid - max(gross - credited, 0) - refunded, 0)
            pending = next((item for item in reversed(settlements)
                            if item["state"] in {"settlement_pending", "unresolved"}), None)
            latest = settlements[-1] if settlements else None
            payment_state = (pending["state"] if pending else
                "credited" if refund_due or credited else
                "failed" if outstanding and latest and latest["state"] == "failed" else
                "accrued" if outstanding or gross == 0 else
                "refunded" if refunded else "settled" if paid else "authorized")
        costs_and_facts = self._economics(costs, facts, row, curr, row["atomic_scale"])
        return {
            "purchase_id": row["purchase_id"], "idempotency_key": row["idempotency_key"],
            "authorization_id": row["authorization_id"], "principal_id": row["principal_id"],
            "account_id": row["account_id"], "supplier_id": row["supplier_id"],
            "service_identity": row["service_identity"], "offer_digest": row["offer_digest"],
            "offer_id": offer_row["offer_id"], "offer_version": offer_row["version"],
            "price_basis": terms["price_basis"], "payment_trigger": terms["payment_trigger"],
            "run_id": row["run_id"], "assignment_id": row["assignment_id"],
            "attempt_id": row["attempt_id"], "task_id": row["task_id"],
            "currency": curr, "atomic_scale": row["atomic_scale"],
            "ceiling_units": row["ceiling_units"],
            "usage_complete": bool(row["usage_complete"]), "closed": bool(row["closed"]),
            "reservation": {"amount_units": reservation["amount_units"],
                            "currency": reservation["currency"],
                            "atomic_scale": reservation["atomic_scale"],
                            "state": reservation["state"]},
            "usage": [{key: item[key] for key in (
                "usage_id", "unit", "quantity", "evidence_state", "source", "evidence_ref",
                "service_identity", "assignment_id", "attempt_id", "task_id", "model_call_id",
                "late", "recorded_at")}
                for item in usage_rows],
            "obligation": obligation, "costs": costs, "supplier_charge": charge,
            "paid_units": paid - refunded, "credited_units": credited,
            "outstanding_units": outstanding, "refund_due_units": refund_due,
            "payment_state": payment_state, "settlements": settlements, "credits": credits,
            "economics": costs_and_facts,
        }

    def _economics(self, costs: Mapping[str, Any], facts: Sequence[Mapping[str, Any]],
                   purchase: sqlite3.Row, currency: str, atomic_scale: int) -> dict[str, Any]:
        def total(kind: str) -> dict[str, Any]:
            selected = [item for item in facts if item["kind"] == kind and
                        item["currency"] == currency and item["atomic_scale"] == atomic_scale]
            if not selected or any(item["amount_units"] is None for item in selected):
                states = [EvidenceState(item["evidence_state"]) for item in selected
                          if item["amount_units"] is None]
                state = _unknown_state(states) if states else EvidenceState.UNKNOWN
                return _money(None, currency, atomic_scale, state,
                              {"reason": "no complete evidence"})
            statuses = [EvidenceState(item["evidence_state"]) for item in selected]
            state = (EvidenceState.ESTIMATED if EvidenceState.ESTIMATED in statuses else
                     EvidenceState.PROVIDER_REPORTED if EvidenceState.PROVIDER_REPORTED in statuses else
                     EvidenceState.CALCULATED if EvidenceState.CALCULATED in statuses else
                     EvidenceState.MEASURED)
            return _money(sum(item["amount_units"] for item in selected), currency,
                          atomic_scale, state)
        fees, overhead, customer_price = (total("payment_fee"), total("owner_overhead"),
                                          total("customer_price"))
        supplier = costs["supplier_charge"]
        if any(item["amount_units"] is None for item in (supplier, fees, overhead)):
            states = [EvidenceState(item["evidence"]) for item in (supplier, fees, overhead)
                      if item["amount_units"] is None]
            production = _money(None, currency, atomic_scale, _unknown_state(states))
        else:
            production = _money(sum(item["amount_units"] for item in (supplier, fees, overhead)),
                                currency, atomic_scale, EvidenceState.CALCULATED,
                                {"components": ["supplier_charge", "payment_fees", "owner_overhead"]})
        return {"inference_cost": costs["inference_cost"], "hosting_cost": costs["hosting_cost"],
                "markup_base": costs["markup_base"], "markup": costs["markup"],
                "supplier_charge": supplier, "payment_fees": fees, "owner_overhead": overhead,
                "production_cost": production, "customer_price": customer_price,
                "customer_price_is_independent": True}

    @staticmethod
    def _settled(db: sqlite3.Connection, purchase_id: str, direction: str) -> int:
        value = db.execute("SELECT COALESCE(SUM(amount_units),0) FROM settlements WHERE purchase_id=? AND direction=? AND state=?",
            (purchase_id, direction, SettlementState.SETTLED.value)).fetchone()[0]
        return int(value)

    @staticmethod
    def _credited(db: sqlite3.Connection, purchase_id: str) -> int:
        value = db.execute("SELECT COALESCE(SUM(amount_units),0) FROM credits WHERE purchase_id=?",
                           (purchase_id,)).fetchone()[0]
        return int(value)

    def _account_committed(self, db: sqlite3.Connection, account_id: str,
                           currency: str, atomic_scale: int) -> int:
        rows = db.execute("""SELECT p.purchase_id,r.amount_units,r.state FROM purchases p
            LEFT JOIN reservations r USING(purchase_id)
            WHERE p.account_id=? AND p.currency=? AND p.atomic_scale=?""",
            (account_id, currency, atomic_scale)).fetchall()
        committed = 0
        for row in rows:
            held = row["amount_units"] if row["state"] == "held" else 0
            paid = max(self._settled(db, row["purchase_id"], "charge") -
                       self._settled(db, row["purchase_id"], "refund"), 0)
            committed += max(held or 0, paid)
        return committed

    def account_status(self, account_id: str, currency: str,
                       atomic_scale: int) -> dict[str, Any]:
        account, curr = _text(account_id, "account_id"), _text(currency, "currency")
        scale = _int(atomic_scale, "atomic_scale")
        assert scale is not None
        with self._connect() as db:
            budget = db.execute("SELECT * FROM budgets WHERE account_id=? AND currency=? AND atomic_scale=?",
                                (account, curr, scale)).fetchone()
            if not budget:
                return {"account_id": account, "currency": curr, "atomic_scale": scale,
                        "budget_id": None,
                        "limit_units": None, "committed_units": None,
                        "available_units": None, "evidence": "unknown"}
            committed = self._account_committed(db, account, curr, scale)
            return {"account_id": account, "currency": curr, "atomic_scale": scale,
                    "budget_id": budget["budget_id"],
                    "limit_units": budget["limit_units"], "committed_units": committed,
                    "available_units": budget["limit_units"] - committed, "evidence": "measured"}


def _combine(states: Sequence[EvidenceState]) -> EvidenceState:
    if EvidenceState.ESTIMATED in states:
        return EvidenceState.ESTIMATED
    if EvidenceState.PROVIDER_REPORTED in states:
        return EvidenceState.PROVIDER_REPORTED
    if EvidenceState.CALCULATED in states:
        return EvidenceState.CALCULATED
    return EvidenceState.MEASURED


__all__ = [
    "AuthorizationRejected", "BudgetExceeded", "CommercialError", "CommercialLedger",
    "EvidenceState", "IdempotencyConflict", "PAYMENT_ADAPTER_DECLARATIONS",
    "PaymentAdapter", "PaymentAdapterCatalog", "PaymentTrigger", "PriceBasis",
    "ReconciliationConflict", "SettlementState", "UnsupportedAdapter",
    "UnsupportedPriceBasis",
]
