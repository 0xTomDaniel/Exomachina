"""Reproducible local Dagu product-slice runner; all runtime state stays in /tmp."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import shlex
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request

from publisher import publish


HERE = Path(__file__).resolve().parent
SPIKES = HERE.parents[1]
COMMON = HERE.parent / "common"
S2_PYTHON = SPIKES / "s2" / ".venv" / "bin" / "python"
DAGU = Path("/tmp/exomachina-countertrials/dagu/dagu")


def port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def get(url: str, auth: bool = False) -> dict:
    request = urllib.request.Request(url, headers={"Authorization": "Bearer fixture-token"} if auth else {})
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.load(response)


def post(url: str, data: dict) -> dict:
    request = urllib.request.Request(url, data=json.dumps(data).encode(), method="POST",
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=8) as response:
        return json.load(response)


def until(check, description: str, timeout: float = 45):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except (OSError, urllib.error.URLError, AssertionError) as exc:
            last = repr(exc)
        time.sleep(.15)
    raise TimeoutError(f"{description} timed out; last={last}")


def launch(argv: list[str], env: dict[str, str], log: Path, ready: str) -> subprocess.Popen:
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("ab") as stream:
        process = subprocess.Popen(argv, env=env, stdout=stream, stderr=stream, start_new_session=True)
    until(lambda: process.poll() is None and get(ready), f"launch {' '.join(argv[:2])}", 20)
    return process


def stop(process: subprocess.Popen | None) -> None:
    if process and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def rss_kib(process: subprocess.Popen | None) -> int | None:
    if process is None or process.poll() is not None:
        return None
    output = subprocess.check_output(["ps", "-o", "rss=", "-p", str(process.pid)], text=True).strip()
    return int(output) if output else None


def status(base: str, version: str) -> dict:
    name = f"exo_decision_factory_{version}"
    run_id = f"dagu-product-{version}-001"
    return get(f"{base}/api/v1/dag-runs/{name}/{run_id}")["dagRunDetails"]


def at_node(base: str, version: str, node: str, label: str = "waiting") -> dict | None:
    data = status(base, version)
    steps = {n["step"]["id"]: n for n in data["nodes"]}
    if steps[node]["statusLabel"] == label:
        return data
    if data["statusLabel"] in {"failed", "aborted"}:
        raise AssertionError(f"{version} failed: {[(n['step']['id'], n['statusLabel'], n.get('stderr')) for n in data['nodes']]}")
    return None


def complete(base: str, version: str, node: str, accepted: bool = True) -> dict:
    name = f"exo_decision_factory_{version}"
    run_id = f"dagu-product-{version}-001"
    return post(f"{base}/api/v1/dag-runs/{name}/{run_id}/human-tasks/{node}/complete",
                {"accepted": accepted})


def rejects_false(base: str, version: str, node: str) -> int:
    try:
        complete(base, version, node, accepted=False)
    except urllib.error.HTTPError as error:
        if error.code in {400, 422}:
            return error.code
        raise
    raise AssertionError(f"{version} {node} accepted false")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("runtime", type=Path)
    parser.add_argument("--acceptance-gap-fault", action="store_true")
    args = parser.parse_args()
    rt = args.runtime.resolve()
    if rt.exists() and any(rt.iterdir()):
        raise SystemExit(f"runtime must be empty: {rt}")
    rt.mkdir(parents=True, exist_ok=True)
    if not S2_PYTHON.exists() or not DAGU.exists():
        raise SystemExit("first run `uv sync --frozen` in s2 and prepare pinned Dagu v2.17.0")
    cap_port, quality_port, dagu_port = port(), port(), port()
    bin_dir = rt / "bin"
    bin_dir.mkdir()
    adapter_command = bin_dir / "exo-dagu-adapter"
    cap_url, quality_url, base = (f"http://127.0.0.1:{item}" for item in
                                 (cap_port, quality_port, dagu_port))
    adapter_command.write_text("#!/bin/sh\n"
        + "export EXO_DAGU_RUNTIME=" + shlex.quote(str(rt)) + "\n"
        + "export EXO_CAPABILITY_URL=" + shlex.quote(cap_url) + "\n"
        + "export EXO_QUALITY_URL=" + shlex.quote(quality_url) + "\n"
        + ("export EXO_DAGU_CRASH_AFTER_ACCEPT_VERSION=v1\n" if args.acceptance_gap_fault else "")
        + "exec python3 " + shlex.quote(str(HERE / "adapter.py")) + " \"$@\"\n")
    adapter_command.chmod(0o755)
    env = {**os.environ, "PATH": str(bin_dir) + os.pathsep + os.environ["PATH"],
           "DAGU_HOME": str(rt / "home"), "DAGU_AUTH_MODE": "none",
           "DAGU_COORDINATOR_ENABLED": "false", "EXO_DAGU_SPIKE_DIR": str(HERE),
           "EXO_DAGU_RUNTIME": str(rt), "EXO_CAPABILITY_URL": cap_url,
           "EXO_QUALITY_URL": quality_url}
    manifests = {"v1": publish(HERE / "definitions", rt / "home", "v1", DAGU)}
    cap = quality = dagu = None
    observations: dict[str, object] = {"ports": {"capability": cap_port, "quality": quality_port,
                                                 "dagu": dagu_port}, "manifests": manifests}
    try:
        cap_cmd = [str(S2_PYTHON), str(COMMON / "harness_server.py"), "--state", str(rt / "capability"),
                   "--role", "capability", "--port", str(cap_port)]
        quality_cmd = [str(S2_PYTHON), str(COMMON / "harness_server.py"), "--state", str(rt / "quality"),
                       "--role", "quality", "--port", str(quality_port)]
        dagu_cmd = [str(DAGU), "start-all", "--host", "127.0.0.1", "--port", str(dagu_port)]
        cap = launch(cap_cmd, env, rt / "capability.log", cap_url + "/health")
        quality = launch(quality_cmd, env, rt / "quality.log", quality_url + "/health")
        dagu = launch(dagu_cmd, env, rt / "dagu.log", base + "/api/v1/dags")
        observations["harnesses_initial"] = {"capability": get(cap_url + "/health"),
                                              "quality": get(quality_url + "/health")}
        post(base + "/api/v1/dags/exo_decision_factory_v1.yaml/start",
             {"dagRunId": "dagu-product-v1-001"})
        until(lambda: at_node(base, "v1", "publication_gate"), "v1 publication wait")
        manifests["v2"] = publish(HERE / "definitions", rt / "home", "v2", DAGU)
        post(base + "/api/v1/dags/exo_decision_factory_v2.yaml/start",
             {"dagRunId": "dagu-product-v2-001"})
        until(lambda: at_node(base, "v2", "publication_gate"), "v2 publication wait")
        observations["false_publication_rejected_http"] = rejects_false(base, "v1", "publication_gate")
        observations["both_waiting_before_restart"] = {
            "v1": status(base, "v1")["statusLabel"], "v2": status(base, "v2")["statusLabel"]}
        observations["rss_kib_waiting"] = {"dagu": rss_kib(dagu), "capability": rss_kib(cap),
                                           "quality": rss_kib(quality)}
        stop(dagu)
        dagu = launch(dagu_cmd, env, rt / "dagu-restarted.log", base + "/api/v1/dags")
        until(lambda: at_node(base, "v1", "publication_gate"), "v1 restored wait")
        until(lambda: at_node(base, "v2", "publication_gate"), "v2 restored wait")
        complete(base, "v1", "publication_gate")
        until(lambda: cap.poll() is not None, "capability commit then lost acknowledgement", 30)
        observations["capability_drop_exit"] = cap.returncode
        cap = launch(cap_cmd, env, rt / "capability-restarted.log", cap_url + "/health")
        observations["harnesses_after_restart"] = {"capability": get(cap_url + "/health"),
                                                    "quality": get(quality_url + "/health")}
        if args.acceptance_gap_fault:
            until(lambda: status(base, "v1")["statusLabel"] == "failed", "post-acceptance Dagu failure", 40)
            with sqlite3.connect(rt / "ledger.sqlite") as db:
                db.row_factory = sqlite3.Row
                accepted = db.execute("SELECT * FROM runs WHERE run_id='dagu-product-v1-001'").fetchone()
                assert accepted["accepted_sha256"] == accepted["current_sha256"]
                assert accepted["release_count"] == 0
                observations["acceptance_before_retry"] = dict(accepted)
            assert (rt / "fault-after-accept-v1.marker").is_file()
            retried = subprocess.run([str(DAGU), "retry", "--run-id", "dagu-product-v1-001",
                "--step", "review", "--downstream", "exo_decision_factory_v1"],
                env=env, text=True, capture_output=True, timeout=30)
            observations["dagu_retry"] = {"returncode": retried.returncode,
                                           "stdout": retried.stdout, "stderr": retried.stderr}
            if retried.returncode != 0:
                raise RuntimeError("Dagu retry failed: " + retried.stderr)
        until(lambda: at_node(base, "v1", "director"), "v1 Quality then Director", 45)
        complete(base, "v2", "publication_gate")
        until(lambda: at_node(base, "v2", "director"), "v2 Quality then Director", 45)
        observations["false_director_rejected_http"] = rejects_false(base, "v1", "director")
        with ThreadPoolExecutor(max_workers=2) as pool:
            observations["director_race"] = list(pool.map(lambda _: complete(base, "v1", "director"), range(2)))
        complete(base, "v2", "director")
        until(lambda: at_node(base, "v1", "deliver", "succeeded"), "v1 delivery", 30)
        until(lambda: at_node(base, "v2", "deliver", "succeeded"), "v2 delivery", 30)
        observations["runs"] = {version: status(base, version) for version in ("v1", "v2")}
        with sqlite3.connect(rt / "ledger.sqlite") as db:
            db.row_factory = sqlite3.Row
            observations["ledger"] = [dict(row) for row in db.execute("SELECT * FROM runs ORDER BY run_id")]
            observations["assignments"] = [dict(row) for row in db.execute("SELECT * FROM assignments ORDER BY action_id")]
        observations["receipts"] = {version: get(cap_url + "/fixture/actions/" +
                                             f"dagu-product-{version}-001:capability", auth=True) for version in ("v1", "v2")}
        observations["quality_receipts"] = {version: get(quality_url + "/fixture/actions/" +
                                                     f"dagu-product-{version}-001:quality", auth=True) for version in ("v1", "v2")}
        observations["rss_kib_final"] = {"dagu": rss_kib(dagu), "capability": rss_kib(cap),
                                         "quality": rss_kib(quality)}
        assert observations["harnesses_initial"]["capability"]["identity"] == observations["harnesses_after_restart"]["capability"]["identity"]
        assert observations["harnesses_initial"]["capability"]["identity"] != observations["harnesses_initial"]["quality"]["identity"]
        assert all(row["release_count"] == 1 for row in observations["ledger"])
        assert all(row["accepted_sha256"] == row["current_sha256"] for row in observations["ledger"])
        assert all(row["accepted_reviewer"] != row["author"] for row in observations["ledger"])
        assert all(value["accepted_count"] == 1 for value in observations["receipts"].values())
        assert all(value["accepted_count"] == 1 for value in observations["quality_receipts"].values())
        if args.acceptance_gap_fault:
            assert observations["quality_receipts"]["v1"]["accepted_count"] == 1
            assert observations["quality_receipts"]["v1"]["attempts"] == 2
            assert observations["acceptance_before_retry"]["accepted_sha256"] == next(
                row["accepted_sha256"] for row in observations["ledger"] if row["run_id"] == "dagu-product-v1-001")
        assert all(node["doneCount"] == 1 for run in observations["runs"].values() for node in run["nodes"])
        (rt / "result.json").write_text(json.dumps(observations, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"result": "passed", "evidence": str(rt / "result.json"),
                          "rss_kib_final": observations["rss_kib_final"]}, sort_keys=True))
    except Exception:
        (rt / "partial-result.json").write_text(json.dumps(observations, indent=2, sort_keys=True) + "\n")
        raise
    finally:
        for process in (dagu, cap, quality):
            stop(process)


if __name__ == "__main__":
    main()
