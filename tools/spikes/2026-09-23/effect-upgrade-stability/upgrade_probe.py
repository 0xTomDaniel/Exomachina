"""Restart two paused Effect runs under changed and retained helper binaries."""
from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time

HERE = Path(__file__).resolve().parent
PRIOR = HERE.parent / "effect-parity"
OLD = HERE.parents[1] / "2026-09-22"
COMMON = OLD / "arbitration" / "common"
sys.path.insert(0, str(PRIOR))
from probe import api, free_port, until  # noqa: E402
import definition  # noqa: E402
sys.path.insert(0, str(COMMON))
import service_probe  # noqa: E402


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def snapshot(url: str, run_id: str) -> dict:
    code, parent = api(url, "/poll", {"id": run_id})
    if code != 200:
        raise RuntimeError(parent)
    child_id = parent["run"]["child_id"]
    if not child_id:
        return {"parent": parent, "child": None}
    code, child = api(url, "/poll", {"id": child_id})
    if code != 200:
        raise RuntimeError(child)
    code, ledger = api(url, "/ledger", {"id": child_id})
    if code != 200:
        raise RuntimeError(ledger)
    return {"parent": parent, "child": child, "ledger": ledger}


def event_counts(ledger: dict) -> dict[str, int]:
    counts = {}
    for event in ledger["events"]:
        counts[event["kind"]] = counts.get(event["kind"], 0) + 1
    return counts


def helper_rss(pid: int) -> int | None:
    result = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)],
                            capture_output=True, text=True)
    return int(result.stdout.strip()) * 1024 if result.returncode == 0 and result.stdout.strip() else None


def run() -> dict:
    base = Path(tempfile.mkdtemp(prefix="exo-effect-upgrade-", dir="/tmp"))
    ports = {name: free_port() for name in ("changed", "pinned")}
    urls = {name: f"http://127.0.0.1:{port}" for name, port in ports.items()}
    processes = []
    helpers = {}
    evidence = {"base": str(base), "helper_v1_sha256": sha(HERE / "helper_v1.mjs"),
                "helper_v2_sha256": sha(HERE / "helper_v2.mjs"),
                "frozen_helper_sha256": sha(PRIOR / "helper.mjs"),
                "frozen_bridge_sha256": sha(PRIOR / "bridge.py")}

    def service(name: str, script: Path, args: list[str]):
        proc, endpoint, health, _ = service_probe.launch(base, name, script, args)
        processes.append(proc)
        return endpoint, health

    def launch(name: str, variant: str):
        effect_root = base / name / "effect"
        effect_root.mkdir(parents=True, exist_ok=True)
        env = {**os.environ,
               "EFFECT_PARITY_ROOT": str(effect_root),
               "EFFECT_PARITY_APPROVED": str(base / "approved.json"),
               "EFFECT_PARITY_PORT": str(ports[name]),
               "EFFECT_PARITY_PYTHON": sys.executable}
        logfile = base / f"{name}-{variant}.log"
        with logfile.open("a") as stream:
            proc = subprocess.Popen(["node", str(HERE / f"helper_{variant}.mjs")],
                                    cwd=HERE, env=env, stdout=stream, stderr=subprocess.STDOUT)
        processes.append(proc)
        helpers[name] = proc
        until(lambda: api(urls[name], "/health")[1].get("pid") == proc.pid,
              seconds=40, label=f"{name} {variant} ready")
        return proc

    def waiting(name: str, run_id: str):
        snap = snapshot(urls[name], run_id)
        return snap if snap["child"] and snap["child"]["run"]["phase"] == "awaiting-director" else None

    def decide(name: str, snap: dict):
        current = snap["child"]["run"]
        code, body = api(urls[name], "/decide", {"id": current["id"],
            "action": "abort", "actor": "director-v1", "token": "director-token",
            "epoch": 1, "digest": current["digest"], "revision": current["revision"],
            "sha256": current["sha256"]})
        return {"status": code, "body": body}

    def complete_snap(name: str, run_id: str):
        snap = snapshot(urls[name], run_id)
        return snap if snap["parent"]["state"] == "Complete" else None

    try:
        harness = OLD / "decision-round" / "common" / "harness_server.py"
        source_url, source = service("source", harness, ["--role", "capability"])
        counter_url, counter = service("counter", harness, ["--role", "capability"])
        quality_url, quality = service("quality", COMMON / "quality_server.py", [])
        release_url, release = service("release", COMMON / "release_server.py",
                                       ["--mode", "participating"])
        approved = {
            "source": {"role": "capability", "url": source_url, "identity": source["identity"], "approved": True},
            "counter": {"role": "capability", "url": counter_url, "identity": counter["identity"], "approved": True},
            "quality": {"role": "quality", "url": quality_url, "identity": quality["identity"], "approved": True},
            "release": {"role": "release", "url": release_url, "identity": release["identity"], "approved": True},
        }
        (base / "approved.json").write_text(json.dumps(approved))
        assert evidence["helper_v1_sha256"] == evidence["frozen_helper_sha256"]
        assert sha(HERE / "bridge.py") == evidence["frozen_bridge_sha256"]
        template = json.loads((PRIOR / "definitions" / "visible-v3.json").read_text())
        child = copy.deepcopy(template["child"])
        root = copy.deepcopy(template["root"])
        child["revision"] = root["revision"] = "upgrade-v1"
        child_digest = definition.digest(child)
        root["nodes"]["invoke_child"]["child_digest"] = child_digest
        package = {"schema": 1, "root": root, "children": {child_digest: child},
                   "bindings": approved}
        evidence["child_digest"] = child_digest
        run_ids = {name: "effect-upgrade-" + name for name in urls}
        for name in urls:
            launch(name, "v1")
            code, published = api(urls[name], "/publish", package)
            assert code == 200, published
            evidence.setdefault("published", {})[name] = published
            code, started = api(urls[name], "/start", {"id": run_ids[name],
                "package_digest": published["package_digest"],
                "input": {"resolution_after_repairs": 3, "wait_seconds": 300,
                    "director": {"identity": "director-v1", "token": "director-token", "epoch": 1}}})
            assert code == 200, started
        assert evidence["published"]["changed"] == evidence["published"]["pinned"]
        before = {name: until(lambda name=name: waiting(name, run_ids[name]),
                               seconds=100, label=name + " r3 wait") for name in urls}
        evidence["before_upgrade"] = {name: {
            "parent_id": run_ids[name], "child_id": snap["child"]["run"]["id"],
            "phase": snap["child"]["run"]["phase"], "revision": snap["child"]["run"]["revision"],
            "digest": snap["child"]["run"]["digest"],
            "event_counts": event_counts(snap["ledger"])} for name, snap in before.items()}
        for name in urls:
            proc = helpers[name]
            proc.kill()
            proc.wait(timeout=10)
        launch("changed", "v2")
        launch("pinned", "v1")
        after_restart = {name: until(lambda name=name: waiting(name, run_ids[name]),
                                     seconds=40, label=name + " wait after restart") for name in urls}
        evidence["after_restart"] = {name: {
            "helper_pid": helpers[name].pid,
            "helper_rss_bytes": helper_rss(helpers[name].pid),
            "child_id": snap["child"]["run"]["id"],
            "digest": snap["child"]["run"]["digest"],
            "event_counts": event_counts(snap["ledger"])} for name, snap in after_restart.items()}
        for name in urls:
            assert evidence["after_restart"][name]["child_id"] == evidence["before_upgrade"][name]["child_id"]
            assert evidence["after_restart"][name]["digest"] == evidence["before_upgrade"][name]["digest"]
        evidence["decisions"] = {name: decide(name, after_restart[name]) for name in urls}
        for name in urls:
            assert evidence["decisions"][name]["status"] == 200, evidence["decisions"][name]
        final = {name: until(lambda name=name: complete_snap(name, run_ids[name]),
                            seconds=60, label=name + " completion") for name in urls}
        evidence["final"] = {name: {
            "parent": snap["parent"], "child": snap["child"],
            "event_counts": event_counts(snap["ledger"]),
            "acceptance": snap["ledger"]["run"]["acceptance_json"],
            "release": snap["ledger"]["run"]["receipt_json"]} for name, snap in final.items()}
        log = base / "changed" / "effect" / "v2-bridge-invocations.jsonl"
        evidence["v2_bridge_invocations"] = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
        with sqlite3.connect(base / "quality" / "harness.sqlite3") as quality_db:
            evidence["quality_actions"] = [dict(zip(("run_id", "actions", "attempts", "accepted_count"), row))
                for row in quality_db.execute("SELECT run_id,COUNT(*),SUM(attempts),SUM(accepted_count) "
                                               "FROM actions GROUP BY run_id ORDER BY run_id")]
        evidence["store_bytes"] = {name: sum(p.stat().st_size for p in (base / name / "effect").glob("*.sqlite*"))
                                   for name in urls}
        evidence["status"] = "observed"
    except Exception as error:
        evidence["status"] = "error"
        evidence["error"] = repr(error)
        raise
    finally:
        for proc in reversed(processes):
            if proc.poll() is None:
                proc.terminate()
                try: proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill(); proc.wait(timeout=5)
        evidence["source_hashes_after"] = {name: sha(HERE / name) for name in
            ("helper_v1.mjs", "helper_v2.mjs", "bridge.py", "definition.py")}
        evidence["frozen_source_hashes_after"] = {name: sha(PRIOR / name) for name in
            ("helper.mjs", "bridge.py", "definition.py")}
        (HERE / "observed.json").write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    return evidence


if __name__ == "__main__":
    result = run()
    print(json.dumps({"status": result["status"], "base": result["base"]}, sort_keys=True))
