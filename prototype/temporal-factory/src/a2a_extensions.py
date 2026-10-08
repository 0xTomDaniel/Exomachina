"""Generic A2A v1 extensions offered by Exomachina agent services to any client.

These are protocol definitions only, shared by agent services and their clients
like ``a2a_v1``. Nothing here names a factory, run, assignment, attempt, action,
definition, node or pin: an agent service offers the same extensions to every
A2A client (A2A v1 mediation decisions 7 and 8). Every extension is optional:
an agent's identity is its pinned Agent Card, and the factory needs no
Exomachina extension from an agent (decision 9).

- ``BUDGET_URI`` (optional): #2121's budget/usage field shape under an
  Exomachina-owned URI. A client may put ``budget`` in the SendMessage request
  metadata; an activated agent reports ``incurred`` in the terminal Task's
  metadata, keyed by the URI. Only what was reported appears; absence never
  means zero.
- ``TEST_STIMULUS_URI``: a test-only control declared solely by agents started
  with test controls. Production Agent Cards never declare it.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping


BUDGET_URI = "https://github.com/0xTomDaniel/Exomachina/a2a/extensions/budget/v1"
TEST_STIMULUS_URI = "urn:exomachina:a2a-test-stimulus:v1"
TOKEN_FIELDS = ("input", "output", "cache_read", "cache_write", "total")
_CURRENCY = re.compile(r"[A-Z][A-Z0-9]{2,15}")
_LABEL = re.compile(r"[a-z][a-z0-9_.-]{0,63}")
_AMOUNT = re.compile(r"(0|[1-9][0-9]{0,17})(\.[0-9]{1,18})?")


class BudgetError(ValueError):
    """A budget is malformed or clearly insufficient for this agent."""


def _parse_time(value: object, label: str) -> datetime:
    if not isinstance(value, str) or not value or len(value) > 64:
        raise BudgetError(f"{label} must be an RFC 3339 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise BudgetError(f"{label} must be an RFC 3339 timestamp") from error
    if parsed.tzinfo is None:
        raise BudgetError(f"{label} must carry a UTC offset")
    return parsed


def _amount(value: object, label: str) -> str:
    if not isinstance(value, str) or _AMOUNT.fullmatch(value) is None:
        raise BudgetError(f"{label} must be a non-negative decimal string")
    return value


def parse_budget(value: object) -> dict | None:
    """Validate a client budget: ``cost.amount``/``currency``, ``tokens.limit``, ``deadline``.

    Returns the normalized budget, or None when no budget was supplied.
    Unknown fields are refused rather than silently ignored.
    """
    if value is None:
        return None
    if not isinstance(value, Mapping) or not value:
        raise BudgetError("budget must be a non-empty object")
    unknown = set(value) - {"cost", "tokens", "deadline"}
    if unknown:
        raise BudgetError("unsupported budget fields: " + ", ".join(sorted(unknown)))
    budget: dict[str, Any] = {}
    if "cost" in value:
        cost = value["cost"]
        if not isinstance(cost, Mapping) or set(cost) != {"amount", "currency"}:
            raise BudgetError("budget.cost requires exactly amount and currency")
        if not isinstance(cost["currency"], str) or _CURRENCY.fullmatch(cost["currency"]) is None:
            raise BudgetError("budget.cost.currency must be an upper-case currency code")
        budget["cost"] = {"amount": _amount(cost["amount"], "budget.cost.amount"),
                          "currency": cost["currency"]}
    if "tokens" in value:
        tokens = value["tokens"]
        if (not isinstance(tokens, Mapping) or set(tokens) != {"limit"}
                or type(tokens["limit"]) is not int or tokens["limit"] < 0):
            raise BudgetError("budget.tokens requires a non-negative integer limit")
        budget["tokens"] = {"limit": tokens["limit"]}
    if "deadline" in value:
        _parse_time(value["deadline"], "budget.deadline")
        budget["deadline"] = value["deadline"]
    return budget


def check_budget(budget: Mapping | None, *, minimum_tokens: int, minimum_seconds: float,
                 now: datetime | None = None) -> None:
    """Reject a budget that clearly cannot cover one unit of this agent's work."""
    if not budget:
        return
    now = now or datetime.now(timezone.utc)
    cost = budget.get("cost")
    if cost is not None and Decimal(cost["amount"]) <= 0:
        raise BudgetError("budget.cost.amount is zero; this agent cannot work without cost")
    tokens = budget.get("tokens")
    if tokens is not None and tokens["limit"] < minimum_tokens:
        raise BudgetError(f"budget.tokens.limit {tokens['limit']} is below this agent's "
                          f"minimum of {minimum_tokens} tokens for one model call")
    deadline = budget.get("deadline")
    if deadline is not None:
        remaining = (_parse_time(deadline, "budget.deadline") - now).total_seconds()
        if remaining < minimum_seconds:
            raise BudgetError(f"budget.deadline leaves {max(0.0, remaining):.1f}s; this agent "
                              f"needs at least {minimum_seconds:g}s")


def deadline_epoch(budget: Mapping | None) -> float | None:
    """The budget deadline as POSIX seconds, or None."""
    if not budget or budget.get("deadline") is None:
        return None
    return _parse_time(budget["deadline"], "budget.deadline").timestamp()


def sum_reported_tokens(calls: Iterable[Mapping[str, int | None]]) -> dict[str, int]:
    """Sum each token field only when every call reported it; never default to zero."""
    calls = list(calls)
    if not calls:
        return {}
    result = {}
    for field in TOKEN_FIELDS:
        values = [call.get(field) for call in calls]
        if all(type(value) is int and value >= 0 for value in values):
            result[field] = sum(values)
    return result


def incurred_metadata(tokens: Mapping[str, int] | None = None,
                      cost: Mapping[str, str] | None = None) -> dict:
    """The Task metadata entry for an activated client: only reported fields."""
    incurred: dict[str, Any] = {}
    if cost:
        incurred["cost"] = dict(cost)
    if tokens:
        incurred["tokens"] = {key: tokens[key] for key in TOKEN_FIELDS if key in tokens}
    return {BUDGET_URI: {"incurred": incurred}}


def requested(extensions: object, uri: str) -> bool:
    try:
        return uri in set(extensions or ())
    except TypeError:
        return False


def parse_incurred(metadata: object) -> dict | None:
    """Client side: validate an agent's ``incurred`` report from Task metadata.

    Returns None when the agent sent no report. A malformed report raises
    ValueError; the caller decides whether that is an incident.
    """
    if not isinstance(metadata, Mapping) or BUDGET_URI not in metadata:
        return None
    entry = metadata[BUDGET_URI]
    if not isinstance(entry, Mapping) or set(entry) != {"incurred"}:
        raise ValueError("budget extension metadata must contain only incurred")
    incurred = entry["incurred"]
    if not isinstance(incurred, Mapping) or set(incurred) - {"cost", "tokens"}:
        raise ValueError("incurred may contain only cost and tokens")
    report: dict[str, Any] = {}
    if "tokens" in incurred:
        tokens = incurred["tokens"]
        if (not isinstance(tokens, Mapping) or not tokens or set(tokens) - set(TOKEN_FIELDS)
                or any(type(value) is not int or value < 0 for value in tokens.values())):
            raise ValueError("incurred.tokens must hold non-negative integer token fields")
        report["tokens"] = {key: tokens[key] for key in TOKEN_FIELDS if key in tokens}
    if "cost" in incurred:
        cost = incurred["cost"]
        if (not isinstance(cost, Mapping) or set(cost) != {"amount", "currency", "source"}
                or not isinstance(cost["currency"], str)
                or _CURRENCY.fullmatch(cost["currency"]) is None
                or not isinstance(cost["source"], str) or _LABEL.fullmatch(cost["source"]) is None):
            raise ValueError("incurred.cost requires amount, currency and source")
        try:
            _amount(cost["amount"], "incurred.cost.amount")
        except BudgetError as error:
            raise ValueError(str(error)) from error
        report["cost"] = dict(cost)
    return report


__all__ = ["BUDGET_URI", "TEST_STIMULUS_URI", "TOKEN_FIELDS",
           "BudgetError", "parse_budget", "check_budget", "deadline_epoch",
           "sum_reported_tokens", "incurred_metadata", "requested", "parse_incurred"]
