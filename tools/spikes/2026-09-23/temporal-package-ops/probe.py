"""Independent local fixture service and Director A2A helpers for package probes."""
from __future__ import annotations

import json
import os
import subprocess
import urllib.request
from pathlib import Path
from uuid import uuid4

import runtime

HERE = Path(__file__).resolve().parent
COMMON = HERE / "common"
PRIOR_COMMON = HERE / "prior_common"
PYTHON = HERE.parent / "venv/bin/python"
SERVICE_PORTS = {"source": 43021, "counter": 43022, "quality": 43023,
                 "release": 43024, "opaque": 43025, "director": 43026}


def start_service(name: str, state: Path) -> subprocess.Popen:
    port = SERVICE_PORTS[name]
    service_state = state / name
    if name in {"source", "counter"}:
        script = HERE / "slow_harness_server.py" if name == "source" else PRIOR_COMMON / "harness_server.py"
        argv = [str(PYTHON), str(script), "--state", str(service_state),
                "--role", "capability", "--port", str(port)]
    elif name == "quality":
        argv = [str(PYTHON), str(COMMON / "quality_server.py"),
                "--state", str(service_state), "--port", str(port)]
    elif name == "director":
        argv = [str(PYTHON), str(HERE / "director_server.py"),
                "--state", str(service_state), "--catalog", str(state / "catalog"),
                "--address", f"127.0.0.1:{runtime.PORTS['frontend']}", "--port", str(port)]
    else:
        argv = [str(PYTHON), str(COMMON / "release_server.py"),
                "--state", str(service_state), "--mode",
                "opaque" if name == "opaque" else "participating", "--port", str(port)]
    env = dict(os.environ, EXO_DIRECTOR_BEARER=(state / "secrets/director-token").read_text().strip())
    with (state / f"{name}.log").open("a") as log:
        return subprocess.Popen(argv, stdout=log, stderr=log, env=env, start_new_session=True)


async def health(name: str, process: subprocess.Popen) -> dict:
    url = f"http://127.0.0.1:{SERVICE_PORTS[name]}/health"
    async def check():
        if process.poll() is not None:
            raise RuntimeError(f"{name} exited {process.returncode}")
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                return json.load(response)
        except Exception:
            return False
    return await runtime.until(check, seconds=30)


def bindings(identities: dict) -> dict:
    result = {}
    for name in ("source", "counter", "quality", "release"):
        role = "capability" if name in {"source", "counter"} else name
        result[name] = {"role": role, "url": f"http://127.0.0.1:{SERVICE_PORTS[name]}",
            "identity": identities[name]["identity"], "approved": True}
    return result


def director_rpc(method: str, params: dict) -> dict:
    rpc = {"jsonrpc": "2.0", "id": str(uuid4()), "method": method, "params": params}
    request = urllib.request.Request(f"http://127.0.0.1:{SERVICE_PORTS['director']}/",
        data=json.dumps(rpc).encode(), method="POST",
        headers={"Authorization": "Bearer " + (runtime.STATE / "secrets/director-token").read_text().strip(),
                 "Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=20) as response:
        body = json.load(response)
    if "error" in body:
        raise RuntimeError("Director A2A error: " + json.dumps(body["error"]))
    return body["result"]


def director_send(command: dict) -> dict:
    return director_rpc("message/send", {"message": {"role": "user", "messageId": str(uuid4()),
        "parts": [{"kind": "data", "data": command}]}})


def director_task(task_id: str) -> dict:
    return director_rpc("tasks/get", {"id": task_id})
