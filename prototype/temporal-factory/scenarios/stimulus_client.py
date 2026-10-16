"""Qualification-only client for the test-only A2A stimulus extension.

Production Agent Cards never declare this extension; ``testbed.py up
--test-controls`` starts only the synthesizer with it. A control is an
ordinary A2A SendMessage carrying one data Part with the extension activated;
the agent answers with a Message. The armed stimulus binds to the next new
contextId the agent receives, never to a caller's run.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import a2a_extensions  # noqa: E402
import a2a_v1  # noqa: E402

TOKEN = "Bearer fixture-token"


def _control(url: str, value: dict, *, timeout: float = 30) -> dict:
    body = a2a_v1.rpc(a2a_v1.SEND_MESSAGE, a2a_v1.send_params(
        a2a_v1.user_message([a2a_v1.data_part(value)])))
    request = urllib.request.Request(
        url.rstrip("/") + "/", data=json.dumps(body).encode(), method="POST",
        headers={"Content-Type": "application/json", "Authorization": TOKEN,
                 **a2a_v1.headers([a2a_extensions.TEST_STIMULUS_URI])})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        reply = json.loads(response.read())
    if "error" in reply:
        raise RuntimeError("stimulus control error: " + json.dumps(reply["error"]))
    message = (reply.get("result") or {}).get("message") or {}
    parts = message.get("parts") or []
    if len(parts) != 1 or "data" not in parts[0]:
        raise RuntimeError("stimulus control returned no data Message")
    return a2a_v1.normalize_numbers(parts[0]["data"])


def arm(url: str, stimulus: dict) -> dict:
    return _control(url, {"arm": stimulus})


def log(url: str) -> list[dict]:
    return _control(url, {"stimulus_log": True}).get("stimulus_log") or []


def factory_contexts(outcome_db: Path) -> dict[str, dict]:
    """contextId -> {run_id, identity} from the factory's own journal."""
    if not outcome_db.exists():
        return {}
    with sqlite3.connect(f"file:{outcome_db}?mode=ro", uri=True) as db:
        rows = db.execute("SELECT run_id, identity, context_id FROM a2a_contexts").fetchall()
    return {context_id: {"run_id": run_id, "identity": identity}
            for run_id, identity, context_id in rows}


def bind_to_runs(entries: list[dict], contexts: dict[str, dict], identity: str) -> list[dict]:
    """Attach the factory run that owns each logged contextId (factory-side)."""
    bound = []
    for entry in entries:
        owner = contexts.get(entry.get("context_id")) or {}
        bound.append({**entry, "run_id": owner.get("run_id")
                      if owner.get("identity") == identity else None})
    return bound
