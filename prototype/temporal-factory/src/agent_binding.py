"""Static identity snapshot and Agent Card pin for factory-side A2A clients.

Agent Card discovery at the well-known path is the only read an A2A client
makes outside JSON-RPC. An agent's identity is its pinned Agent Card (A2A
decision 9): the pin is the digest of the endpoint-free card, and the binding
identity is derived from that digest. The factory requires no Exomachina
extension or metadata from an agent to dispatch, accept, pin or correlate.

Reconciliation follows what the card publicly promises: a skill tagged
``message-id-idempotent`` returns the original Task when a ``messageId`` is
resent, so an uncertain send may be resent; any other agent is ``opaque`` and
is dispatched, accepted and correlated normally, but never resent after an
uncertain send.
"""
from __future__ import annotations

import hashlib
import json
import urllib.error
import urllib.request
from pathlib import Path

import a2a_v1


CARD_IDENTITY_PREFIX = "a2a-card-"
IDEMPOTENT_RESEND_TAG = "message-id-idempotent"
RECONCILE_MODES = ("a2a-idempotent-resend", "opaque")


class UnavailableBinding(Exception):
    """The mapped endpoint may return; a retry must re-read the snapshot."""


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def digest(value: object) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def card_identity(card_sha256: str) -> str:
    """The binding identity derived from a pinned Agent Card digest."""
    return CARD_IDENTITY_PREFIX + card_sha256[:24]


def read_json(url: str) -> dict:
    request = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            value = json.load(response)
    except urllib.error.HTTPError as error:
        raise ValueError(f"agent card HTTP {error.code}") from error
    except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
        raise UnavailableBinding(str(error)) from error
    if not isinstance(value, dict):
        raise ValueError("expected JSON object")
    return value


def read_card(url: str) -> dict:
    """Read the A2A v1 Agent Card served for ``url``; its endpoint must be ``url``."""
    card = read_json(url.rstrip("/") + "/.well-known/agent-card.json")
    try:
        endpoint = a2a_v1.card_url(card)
    except a2a_v1.ProtocolError as error:
        raise ValueError("Agent Card is not A2A v1.0 only: " + str(error)) from error
    if endpoint.rstrip("/") != url.rstrip("/"):
        raise ValueError("Agent Card endpoint differs from snapshot")
    return card


def describe(card: dict) -> dict:
    """The factory's view of one Agent Card: pin, derived identity, reconcile, skills."""
    extensions = (card.get("capabilities") or {}).get("extensions") or []
    if not isinstance(extensions, list) or not all(isinstance(item, dict) for item in extensions):
        raise ValueError("Agent Card extensions are malformed")
    skills = card.get("skills") or []
    if not isinstance(skills, list) or not all(isinstance(skill, dict) for skill in skills):
        raise ValueError("Agent Card skills are malformed")
    tags = {tag for skill in skills for tag in (skill.get("tags") or []) if isinstance(tag, str)}
    sha = digest(a2a_v1.card_without_endpoint(card))
    return {"card_sha256": sha, "identity": card_identity(sha),
            "reconcile": RECONCILE_MODES[0] if IDEMPOTENT_RESEND_TAG in tags else RECONCILE_MODES[1],
            "skills": [skill.get("id") for skill in skills],
            "required_extensions": sorted(str(item.get("uri")) for item in extensions
                                          if item.get("required") is True)}


def card_observation(url: str) -> dict:
    """Read and describe one Agent Card; the only discovery read."""
    return {"url": url, **describe(read_card(url))}


def pin(url: str, identity: str | None = None) -> dict:
    """Pin an agent from its Agent Card alone.

    Refuses a card that requires an extension: an A2A client that does not
    activate a required extension cannot use the agent.
    """
    observed = card_observation(url)
    if observed["required_extensions"]:
        raise ValueError("Agent Card requires extensions this client does not activate")
    if identity is not None and observed["identity"] != identity:
        raise ValueError("Agent Card identity differs from the expected identity")
    return {"card_sha256": observed["card_sha256"], "identity": observed["identity"],
            "reconcile": observed["reconcile"]}


def resolve(snapshot_path: Path, identity: str, pinned: dict) -> tuple[str, dict]:
    """Resolve a pinned identity through the static snapshot and re-verify its card."""
    snapshot = json.loads(snapshot_path.read_text())
    if snapshot.get("snapshot_version") != 1 or not isinstance(snapshot.get("agents"), dict):
        raise ValueError("invalid agent snapshot")
    entry = snapshot["agents"].get(identity)
    if not isinstance(entry, dict) or set(entry) != {"url"}:
        raise ValueError("pinned identity absent from snapshot")
    url = entry["url"]
    if not isinstance(url, str) or not url.startswith("http://127.0.0.1:"):
        raise ValueError("invalid snapshot endpoint")
    observed = card_observation(url)
    if (observed["card_sha256"] != pinned.get("card_sha256")
            or observed["identity"] != identity
            or pinned.get("identity") not in (None, identity)
            # A pin may treat a resend-safe agent as opaque, never the reverse.
            or (pinned.get("reconcile") == RECONCILE_MODES[0]
                and observed["reconcile"] != RECONCILE_MODES[0])):
        raise ValueError("pinned Agent Card or identity mismatch")
    return url, observed
