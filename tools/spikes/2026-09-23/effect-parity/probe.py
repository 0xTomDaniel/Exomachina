"""Visible Effect parity probe against the common real Strands/A2A fixtures."""
from __future__ import annotations

import copy
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
OLD = HERE.parents[1] / "2026-09-22"
COMMON = OLD / "arbitration" / "common"
sys.path.insert(0, str(HERE))
import definition  # noqa: E402
sys.path.insert(0, str(COMMON))
import service_probe  # noqa: E402


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def api(url, route, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request(url + route, data=data, method="POST" if data else "GET",
                                     headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=40) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)


def until(predicate, seconds=60, label="condition"):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            pass
        time.sleep(.15)
    raise TimeoutError(f"{label} not reached; last={last}")


def main():
    base = Path(tempfile.mkdtemp(prefix="exo-effect-parity-"))
    processes = []
    evidence = {"base": str(base), "scope": "visible A1 plus bounded negatives/restart",
                "effect_version": json.loads((HERE / "package.json").read_text())["dependencies"]}
    helper = None
    port = free_port()
    url = f"http://127.0.0.1:{port}"

    def service(name, script, args):
        process, endpoint, health, logfile = service_probe.launch(base, name, script, args)
        processes.append(process)
        return endpoint, health

    def start_helper():
        log = (base / "effect-helper.log").open("a")
        env = os.environ.copy()
        env.update(EFFECT_PARITY_ROOT=str(base / "effect"),
                   EFFECT_PARITY_APPROVED=str(base / "approved.json"),
                   EFFECT_PARITY_PORT=str(port), EFFECT_PARITY_PYTHON=sys.executable)
        process = subprocess.Popen(["node", str(HERE / "helper.mjs")], cwd=HERE, env=env,
                                   stdout=log, stderr=subprocess.STDOUT)
        log.close()
        processes.append(process)
        until(lambda: api(url, "/health")[1].get("pid") == process.pid,
              label="Effect helper ready")
        return process

    def state(run_id):
        status, body = api(url, "/poll", {"id": run_id})
        if status != 200:
            raise RuntimeError(body)
        return body

    def child_wait(parent):
        result = state(parent)
        child_id = result["run"]["child_id"]
        if not child_id:
            return None
        child = state(child_id)
        if child["run"]["phase"] == "awaiting-director":
            return parent, child_id, child
        return None

    def complete(run_id):
        result = state(run_id)
        if result["state"] != "Complete":
            return None
        if result["exit"] != "Success":
            raise RuntimeError(f"{run_id} workflow failed: {result}")
        return result["value"]

    def decide(child_id, child_state, token="director-token", actor="director-v1", epoch=1):
        current = child_state["run"]
        return api(url, "/decide", {"id": child_id, "action": "abort", "actor": actor,
            "token": token, "epoch": epoch, "digest": current["digest"],
            "revision": current["revision"], "sha256": current["sha256"]})

    def input_for(repair):
        return {"resolution_after_repairs": repair, "wait_seconds": 300,
                "director": {"identity": "director-v1", "token": "director-token", "epoch": 1}}

    try:
        source_url, source_health = service("source", OLD / "decision-round" / "common" / "harness_server.py",
                                            ["--role", "capability"])
        counter_url, counter_health = service("counter", OLD / "decision-round" / "common" / "harness_server.py",
                                              ["--role", "capability"])
        quality_url, quality_health = service("quality", COMMON / "quality_server.py", [])
        release_url, release_health = service("release", COMMON / "release_server.py",
                                              ["--mode", "participating"])
        evidence["real_strands_services"] = True
        approved = {
            "source": {"role": "capability", "url": source_url,
                       "identity": source_health["identity"], "approved": True},
            "counter": {"role": "capability", "url": counter_url,
                        "identity": counter_health["identity"], "approved": True},
            "quality": {"role": "quality", "url": quality_url,
                        "identity": quality_health["identity"], "approved": True},
            "release": {"role": "release", "url": release_url,
                        "identity": release_health["identity"], "approved": True},
        }
        (base / "approved.json").write_text(json.dumps(approved))
        helper = start_helper()
        template = json.loads((HERE / "definitions" / "visible-v3.json").read_text())

        def package(revision):
            child = copy.deepcopy(template["child"])
            root = copy.deepcopy(template["root"])
            child["revision"] = root["revision"] = revision
            child_digest = definition.digest(child)
            root["nodes"]["invoke_child"]["child_digest"] = child_digest
            return {"schema": 1, "root": root, "children": {child_digest: child},
                    "bindings": approved}

        v2, v3 = package("v2"), package("v3")
        code, published_v2 = api(url, "/publish", v2)
        assert code == 200, published_v2
        code, started_v2 = api(url, "/start", {"id": "effect-v2-wait",
            "package_digest": published_v2["package_digest"], "input": input_for(3)})
        assert code == 200, started_v2
        old_wait = until(lambda: child_wait("effect-v2-wait"), label="old child Director wait")
        evidence["v2_wait"] = {"parent": old_wait[0], "child": old_wait[1],
                               "digest": old_wait[2]["run"]["digest"],
                               "revision": old_wait[2]["run"]["revision"],
                               "repairs": old_wait[2]["run"]["repair_count"]}
        code, published_v3 = api(url, "/publish", v3)
        assert code == 200, published_v3
        evidence["publish_v3_without_worker_restart"] = (
            helper.poll() is None and api(url, "/health")[1]["pid"] == helper.pid)
        assert evidence["publish_v3_without_worker_restart"]
        evidence["v2_still_waiting_at_v3_publish"] = child_wait("effect-v2-wait") is not None
        assert evidence["v2_still_waiting_at_v3_publish"]
        for run_id, repair in (("effect-v3-success", 1), ("effect-v3-exhausted", 3)):
            code, started = api(url, "/start", {"id": run_id,
                "package_digest": published_v3["package_digest"], "input": input_for(repair)})
            assert code == 200, started
        accepted = until(lambda: complete("effect-v3-success"), label="v3 success")
        assert accepted["status"] == "accepted" and accepted["acceptance"]["revision"] == "r2"
        exhausted = until(lambda: child_wait("effect-v3-exhausted"), label="v3 exhausted child wait")
        bad_actor = decide(exhausted[1], exhausted[2], actor="forged-director")
        stale = decide(exhausted[1], exhausted[2], epoch=0)
        assert bad_actor[0] == 403 and stale[0] == 409, (bad_actor, stale)
        good = decide(exhausted[1], exhausted[2])
        assert good[0] == 200, good
        aborted = until(lambda: complete("effect-v3-exhausted"), label="v3 abort")
        assert aborted["status"] == "aborted" and aborted["child"]["repairs"] == 2
        evidence["v3_success"] = accepted
        evidence["v3_exhaustion"] = aborted
        evidence["director_guards"] = {"forged": bad_actor[0], "stale_owner": stale[0],
                                        "valid": good[0]}
        # A killed Effect helper is restarted with the old child/parent still
        # suspended at the same definition and child run identities.
        helper.kill()
        helper.wait(timeout=10)
        helper = start_helper()
        old_again = until(lambda: child_wait("effect-v2-wait"), label="v2 wait after restart")
        assert old_again[1] == old_wait[1]
        assert old_again[2]["run"]["digest"] == old_wait[2]["run"]["digest"]
        code, _ = decide(old_again[1], old_again[2])
        assert code == 200
        old_aborted = until(lambda: complete("effect-v2-wait"), label="v2 abort after restart")
        assert old_aborted["status"] == "aborted"
        evidence["old_wait_restart_and_abort"] = {"same_child": True,
                                                     "same_digest": True, "status": "aborted"}

        child_success = accepted["child"]["run"]
        status, ledger = api(url, "/ledger", {"id": child_success})
        assert status == 200
        events = ledger["events"]
        by_kind = {}
        for entry in events:
            by_kind.setdefault(entry["kind"], []).append(entry)
        starts = {e["key"]: e["created_ms"] for e in events if e["kind"] == "assign-start"}
        ends = {e["key"]: e["created_ms"] for e in events if e["kind"] == "assign"}
        overlap = max(starts.values()) <= min(ends.values())
        evidence["branch_overlap"] = overlap
        evidence["success_counts"] = {kind: len(items) for kind, items in by_kind.items()}
        evidence["success_child_binding"] = {"digest": ledger["run"]["digest"],
            "repair_count": ledger["run"]["repair_count"],
            "accepted_revision": json.loads(ledger["run"]["acceptance_json"])["revision"]}
        assert overlap and len(by_kind["assign"]) == 2 and len(by_kind["quality"]) == 2
        assert len(by_kind["acceptance"]) == 1 and len(by_kind["release"]) == 1

        # Negative definitions must be rejected without changing the catalog.
        mutations = {
            "review_bypass": lambda p: p["children"][next(iter(p["children"]))]["nodes"]["join_evidence"].update(next="publish"),
            "wrong_join": lambda p: p["children"][next(iter(p["children"]))]["nodes"]["join_evidence"].update(branches=["source_evidence", "absent"]),
            "bad_repair_bound": lambda p: p["children"][next(iter(p["children"]))]["nodes"]["repair"].update(max_repairs=3),
            "unsafe_code": lambda p: p["children"][next(iter(p["children"]))]["nodes"]["draft"].update(shell="echo unsafe"),
        }
        evidence["publication_negatives"] = {}
        for name, mutate in mutations.items():
            invalid = copy.deepcopy(v3)
            mutate(invalid)
            child = next(iter(invalid["children"].values()))
            child_digest = definition.digest(child)
            invalid["children"] = {child_digest: child}
            invalid["root"]["nodes"]["invoke_child"]["child_digest"] = child_digest
            code, body = api(url, "/publish", invalid)
            evidence["publication_negatives"][name] = {"status": code, "error": body.get("error")}
            assert code == 400, (name, body)
        evidence["passed"] = True
    except Exception as error:
        evidence["passed"] = False
        evidence["error"] = repr(error)
        raise
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        (HERE / "observed.json").write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
        print(json.dumps(evidence, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
