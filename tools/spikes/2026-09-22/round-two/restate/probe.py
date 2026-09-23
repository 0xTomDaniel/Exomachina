"""Run the pinned Restate no-rollout publication and recovery countertrial."""

import collections
import hashlib
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor


HERE = Path(__file__).resolve().parent
SERVER_BIN = Path(os.environ["RESTATE_SERVER_BIN"])
ROOT = Path(tempfile.mkdtemp(prefix="exomachina-round2-restate-"))
CATALOG = ROOT / "catalog"
EVENTS = ROOT / "events.jsonl"
INGRESS = "http://127.0.0.1:8080"
ADMIN = "http://127.0.0.1:9070"
HANDLER = "http://127.0.0.1:19080"


def request(method: str, url: str, value=None, timeout=30):
    data = None if value is None else json.dumps(value).encode()
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("content-type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            body = response.read().decode()
            return {"status": response.status, "body": json.loads(body) if body else None}
    except urllib.error.HTTPError as error:
        return {"status": error.code, "body": error.read().decode()}


def until_port(port: int, process: subprocess.Popen, seconds=30):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"process exited before port {port} was ready: {process.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.2)
    raise TimeoutError(f"port {port} was not ready")


def until_node(run_id: str, node: str, seconds=30):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if EVENTS.exists():
            rows = [json.loads(line) for line in EVENTS.read_text().splitlines()]
            if any(row == {"run_id": run_id, "node": node} for row in rows):
                return
        time.sleep(0.2)
    raise TimeoutError(f"{run_id} did not reach {node}")


def publish(version: int):
    definition = json.loads((HERE / "fixtures" / f"v{version}.json").read_text())
    steps = definition["steps"]
    if not set(steps) <= {"research_a", "research_b", "join", "verify", "review", "director_wait", "deliver"}:
        raise ValueError("unapproved block")
    if steps.index("review") > steps.index("director_wait") or steps.index("review") > steps.index("deliver"):
        raise ValueError("review bypass")
    raw = json.dumps(definition, sort_keys=True, separators=(",", ":")).encode()
    digest = hashlib.sha256(raw).hexdigest()
    path = CATALOG / f"{digest}.json"
    path.write_bytes(raw)
    return digest


def start_server(log):
    process = subprocess.Popen(
        [str(SERVER_BIN), "--base-dir", str(ROOT / "restate-data"), "--bind-ip", "127.0.0.1", "--listen-mode", "tcp", "--no-logo"],
        cwd=ROOT,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    until_port(9070, process, 60)
    until_port(8080, process, 60)
    return process


def start_handler(log):
    env = os.environ.copy()
    env["EXOMACHINA_RESTATE_CATALOG"] = str(CATALOG)
    env["EXOMACHINA_RESTATE_EVENTS"] = str(EVENTS)
    process = subprocess.Popen(
        [sys.executable, "-m", "hypercorn", "server:app", "--bind", "127.0.0.1:19080"],
        cwd=HERE,
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    until_port(19080, process, 30)
    return process


def stop(process, hard=False):
    if process is None or process.poll() is not None:
        return
    process.send_signal(signal.SIGKILL if hard else signal.SIGTERM)
    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=15)


def rss_kib(process):
    out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(process.pid)], text=True).strip()
    return int(out)


def disk_kib(path):
    return int(subprocess.check_output(["du", "-sk", str(path)], text=True).split()[0])


def main():
    CATALOG.mkdir()
    server_log = (ROOT / "server.log").open("w")
    handler_log = (ROOT / "handler.log").open("w")
    server = handler = None
    evidence = {"root": str(ROOT), "server_binary": str(SERVER_BIN)}
    try:
        server = start_server(server_log)
        handler = start_handler(handler_log)
        evidence["initial_pids"] = {"server": server.pid, "handler": handler.pid}
        evidence["warm_rss_kib"] = {"server": rss_kib(server), "handler": rss_kib(handler)}
        evidence["registration"] = request("POST", ADMIN + "/deployments", {"uri": HANDLER})
        if evidence["registration"]["status"] not in (200, 201, 202):
            raise RuntimeError(f"registration failed: {evidence['registration']}")

        digest1 = publish(1)
        evidence["digest_v1"] = digest1
        evidence["start_v1"] = request("POST", INGRESS + "/FactoryRun/harness-1-v1/run/send", {"definition_digest": digest1})
        until_node("harness-1-v1", "director_wait")

        digest2 = publish(2)
        evidence["digest_v2"] = digest2
        evidence["handler_unchanged_at_v2_publish"] = handler.pid == evidence["initial_pids"]["handler"]
        evidence["start_v2"] = request("POST", INGRESS + "/FactoryRun/harness-2-v2/run/send", {"definition_digest": digest2})
        until_node("harness-2-v2", "director_wait")
        evidence["paused_rss_kib"] = {"server": rss_kib(server), "handler": rss_kib(handler)}

        stop(handler, hard=True)
        stop(server, hard=True)
        server = start_server(server_log)
        handler = start_handler(handler_log)
        evidence["restart_pids"] = {"server": server.pid, "handler": handler.pid}

        def approve(run_id):
            return request("POST", INGRESS + f"/FactoryRun/{run_id}/approve", {"accepted": True})

        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = [pool.submit(approve, "harness-1-v1"), pool.submit(approve, "harness-1-v1"), pool.submit(approve, "harness-2-v2")]
            evidence["approvals"] = [f.result() for f in futures]
        evidence["result_v1"] = request("GET", INGRESS + "/restate/workflow/FactoryRun/harness-1-v1/attach")
        evidence["result_v2"] = request("GET", INGRESS + "/restate/workflow/FactoryRun/harness-2-v2/attach")
        rows = [json.loads(line) for line in EVENTS.read_text().splitlines()]
        evidence["event_counts"] = {f"{run_id}:{node}": count for (run_id, node), count in collections.Counter((row["run_id"], row["node"]) for row in rows).items()}
        evidence["state_disk_kib"] = disk_kib(ROOT / "restate-data")
        assert evidence["result_v1"]["body"]["version"] == 1
        assert evidence["result_v2"]["body"]["version"] == 2
        assert evidence["event_counts"].get("harness-1-v1:deliver") == 1
        assert evidence["event_counts"].get("harness-2-v2:deliver") == 1
        assert "harness-1-v1:verify" not in evidence["event_counts"]
        assert evidence["event_counts"].get("harness-2-v2:verify") == 1
        evidence["passed"] = True
    except Exception as error:
        evidence["passed"] = False
        evidence["error"] = repr(error)
        raise
    finally:
        stop(handler)
        stop(server)
        server_log.close()
        handler_log.close()
        (HERE / "observed.json").write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
        print(json.dumps(evidence, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
