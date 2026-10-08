"""Static identity snapshot and Agent Card pin for factory-side A2A clients.

Agent Card discovery at the well-known path is the only read an A2A client
makes outside JSON-RPC. The pin is the digest of the endpoint-free card plus
the agent's self-declared identity and resend rule (``a2a_extensions.AGENT_URI``).
"""
from __future__ import annotations

import hashlib
import json
import urllib.error
import urllib.request
from pathlib import Path

import a2a_extensions
import a2a_v1


EXTENSION_URI = a2a_extensions.AGENT_URI


class UnavailableBinding(Exception):
    """The mapped endpoint may return; a retry must re-read the snapshot."""


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def digest(value: object) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


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


def card_observation(url: str) -> dict:
    """Read and summarize one Agent Card; the only discovery read."""
    card = read_json(url.rstrip("/") + "/.well-known/agent-card.json")
    try:
        endpoint = a2a_v1.card_url(card)
    except a2a_v1.ProtocolError as error:
        raise ValueError("Agent Card is not A2A v1.0 only: " + str(error)) from error
    if endpoint.rstrip("/") != url.rstrip("/"):
        raise ValueError("Agent Card endpoint differs from snapshot")
    extensions = (card.get("capabilities") or {}).get("extensions") or []
    if not isinstance(extensions, list) or not all(isinstance(item, dict) for item in extensions):
        raise ValueError("Agent Card extensions are malformed")
    by_uri = {item.get("uri"): item for item in extensions}
    agent = by_uri.get(EXTENSION_URI) or {}
    params = agent.get("params") or {}
    skills = [skill.get("id") for skill in card.get("skills") or [] if isinstance(skill, dict)]
    return {"url": url, "card_sha256": digest(a2a_v1.card_without_endpoint(card)),
            "identity": params.get("identity"), "resend": params.get("resend"),
            "required_extensions": sorted(uri for uri, item in by_uri.items()
                                          if item.get("required") is True),
            "extensions": sorted(uri for uri in by_uri if isinstance(uri, str)),
            "skills": skills}


def pin(url: str, identity: str | None = None) -> dict:
    """Pin an agent from its Agent Card alone."""
    observed = card_observation(url)
    if not isinstance(observed["identity"], str) or not observed["identity"]:
        raise ValueError("Agent Card declares no agent identity")
    if identity is not None and observed["identity"] != identity:
        raise ValueError("Agent Card identity differs from the expected identity")
    if observed["required_extensions"]:
        raise ValueError("Agent Card requires extensions this client does not activate")
    resend = observed["resend"] == a2a_extensions.RESEND_RULE
    return {"card_sha256": observed["card_sha256"], "identity": observed["identity"],
            "reconcile": "a2a-idempotent-resend" if resend else "opaque"}


def resolve(snapshot_path: Path, identity: str, pinned: dict) -> tuple[str, dict]:
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
            or pinned.get("identity") not in (None, identity)):
        raise ValueError("pinned Agent Card or identity mismatch")
    return url, observed
