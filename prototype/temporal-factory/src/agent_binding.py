"""Static identity snapshot and Agent Card pin for the async A2A contract."""
from __future__ import annotations

import hashlib
import json
import urllib.error
import urllib.request
from pathlib import Path


EXTENSION_URI = "urn:exomachina:a2a-action-contract:v1"
CONTRACT = "action-idempotent-async@1"


class UnavailableBinding(Exception):
    """The mapped endpoint may return; a retry must re-read the snapshot."""


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def digest(value: object) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def read_json(url: str) -> dict:
    request = urllib.request.Request(url, headers={"Authorization": "Bearer fixture-token"})
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            value = json.load(response)
    except urllib.error.HTTPError as error:
        raise ValueError(f"agent metadata HTTP {error.code}") from error
    except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
        raise UnavailableBinding(str(error)) from error
    if not isinstance(value, dict):
        raise ValueError("expected JSON object")
    return value


def card_observation(url: str) -> dict:
    card = read_json(url.rstrip("/") + "/.well-known/agent-card.json")
    if card.get("url", "").rstrip("/") != url.rstrip("/"):
        raise ValueError("Agent Card endpoint differs from snapshot")
    extensions = (card.get("capabilities") or {}).get("extensions") or []
    if len(extensions) != 1:
        raise ValueError("Agent Card must declare exactly one extension")
    extension = extensions[0]
    params = extension.get("params") or {}
    without_url = {key: value for key, value in card.items() if key != "url"}
    contract = read_json(url.rstrip("/") + "/contract")
    return {"url": url, "card_sha256": digest(without_url),
            "extension_uri": extension.get("uri"),
            "extension_required": extension.get("required"),
            "extension_identity": params.get("identity"),
            "extension_contract": params.get("contract"),
            "extension_contract_digest": params.get("contract_digest"),
            "contract_sha256": digest(contract),
            "contract_document": contract}


def pin(url: str, identity: str) -> dict:
    observed = card_observation(url)
    if (observed["extension_uri"] != EXTENSION_URI
            or observed["extension_required"] is not True
            or observed["extension_identity"] != identity
            or observed["extension_contract"] != CONTRACT
            or observed["extension_contract_digest"] != observed["contract_sha256"]):
        raise ValueError("delayed agent contract is invalid")
    document = observed["contract_document"]
    idempotency = document.get("idempotency") or {}
    resend = (document.get("name") == CONTRACT
              and document.get("reconcile") == "a2a-idempotent-resend"
              and idempotency.get("key") == "action_id"
              and idempotency.get("same_payload") == "original_task_id"
              and idempotency.get("commit_before_response") is True)
    return {"card_sha256": observed["card_sha256"],
            "a2a_extension": {"uri": EXTENSION_URI, "contract": CONTRACT,
                              "contract_digest": observed["extension_contract_digest"]},
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
    expected = pinned.get("a2a_extension") or {}
    if (observed["card_sha256"] != pinned.get("card_sha256")
            or observed["extension_uri"] != expected.get("uri")
            or observed["extension_required"] is not True
            or observed["extension_identity"] != identity
            or observed["extension_contract"] != expected.get("contract")
            or observed["extension_contract_digest"] != expected.get("contract_digest")
            or observed["contract_sha256"] != expected.get("contract_digest")):
        raise ValueError("pinned Agent Card, identity or contract mismatch")
    return url, observed
