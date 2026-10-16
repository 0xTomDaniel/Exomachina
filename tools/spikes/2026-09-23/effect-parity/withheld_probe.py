"""Fresh A2B four-branch composition, authored only after evaluator freeze receipt."""
from __future__ import annotations

import copy
import json
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
OLD = HERE.parents[1] / "2026-09-22"
COMMON = OLD / "arbitration" / "common"
sys.path.insert(0, str(HERE))
import definition  # noqa: E402
from probe import api, free_port, until  # noqa: E402
sys.path.insert(0, str(COMMON))
import service_probe  # noqa: E402


def freeze_verify():
    result = subprocess.check_output([sys.executable, str(COMMON / "freeze.py"),
        "verify", str(HERE / "freeze.json")], text=True)
    return json.loads(result)


def main():
    base = Path(tempfile.mkdtemp(prefix="exo-effect-a2b-"))
    processes = []
    evidence = {"base": str(base), "scope": "fresh withheld four-branch A2B",
                "freeze_before": freeze_verify()}
    port = free_port()
    url = f"http://127.0.0.1:{port}"
    product_db = base / "effect" / "product.sqlite"
    engine_db = base / "effect" / "effect.sqlite"
    helper = None
    counter_primary_process = None

    def service(name, script, args):
        process, endpoint, health, logfile = service_probe.launch(base, name, script, args)
        processes.append(process)
        return process, endpoint, health

    def start_helper():
        log = (base / "effect-helper.log").open("a")
        env = os.environ.copy()
        env.update(EFFECT_PARITY_ROOT=str(base / "effect"),
                   EFFECT_PARITY_APPROVED=str(base / "approved.json"),
                   EFFECT_PARITY_PORT=str(port), EFFECT_PARITY_PYTHON=sys.executable)
        process = subprocess.Popen(["node", str(HERE / "helper.mjs")], cwd=HERE,
                                   env=env, stdout=log, stderr=subprocess.STDOUT)
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

    def complete(run_id):
        result = state(run_id)
        if result["state"] != "Complete":
            return None
        if result["exit"] != "Success":
            raise RuntimeError(f"{run_id} failed: {result}")
        return result["value"]

    def child_wait(parent):
        child_id = state(parent)["run"]["child_id"]
        if not child_id:
            return None
        child = state(child_id)
        return (child_id, child) if child["run"]["phase"] == "awaiting-director" else None

    def input_for(resolve_after):
        return {"resolution_after_repairs": resolve_after, "wait_seconds": 300,
                "director": {"identity": "director-a2b", "token": "director-a2b-token", "epoch": 1}}

    def materialize(template_name, bindings):
        template = json.loads((HERE / "definitions" / template_name).read_text())
        child = copy.deepcopy(template["child"])
        root = copy.deepcopy(template["root"])
        child_digest = definition.digest(child)
        root["nodes"]["invoke_child"]["child_digest"] = child_digest
        pkg = {"schema": 1, "root": root, "children": {child_digest: child},
               "bindings": bindings}
        definition.validate(pkg, approved)
        (base / (template_name.removesuffix(".json") + "-native.json")).write_text(
            json.dumps(pkg, indent=2, sort_keys=True) + "\n")
        return pkg, child_digest

    def event_rows(child_id):
        with sqlite3.connect(product_db) as conn:
            return [{"rowid": rowid, "key": key, "kind": kind,
                     "value": json.loads(value), "created_ms": created}
                    for rowid, key, kind, value, created in conn.execute(
                        "SELECT rowid,event_key,kind,value_json,created_ms FROM events "
                        "WHERE run_id=? ORDER BY rowid", (child_id,))]

    def delayed_start(run_id, published, child_digest, resolve_after):
        child_id = f"{run_id}:child:{child_digest[:12]}"
        counter_primary_process.send_signal(signal.SIGSTOP)
        other_done = False
        try:
            code, started = api(url, "/start", {"id": run_id,
                "package_digest": published["package_digest"],
                "input": input_for(resolve_after)})
            assert code == 200, started
            deadline = time.monotonic() + 2.5
            while time.monotonic() < deadline:
                completed = {entry["key"] for entry in event_rows(child_id)
                             if entry["kind"] == "assign"}
                if completed >= {"gather:assign:source_alpha", "gather:assign:source_beta",
                                  "gather:assign:counter_crosscheck"}:
                    other_done = True
                    break
                time.sleep(.01)
        finally:
            counter_primary_process.send_signal(signal.SIGCONT)
        return child_id, started, other_done

    def analyze(run_id, child_id, expected_scope, expected_quality, expected_accept):
        rows = event_rows(child_id)
        assigns = [r for r in rows if r["kind"] == "assign"]
        assert len(assigns) == 4, (run_id, assigns)
        task_ids = {r["value"]["task_id"] for r in assigns}
        assert len(task_ids) == 4
        assert len({r["key"] for r in assigns}) == 4
        last_assign = max(assigns, key=lambda r: r["rowid"])
        assert last_assign["key"] == "gather:assign:counter_primary", last_assign
        joins = [r for r in rows if r["kind"] == "join"]
        assert len(joins) == 1 and joins[0]["rowid"] > last_assign["rowid"]
        joined = joins[0]["value"]
        assert len(joined["branch_artifact_sha256"]) == 4
        assert len(joined["claims"]) == 4 and len(joined["objections"]) == 4
        assert {claim["branch_instance"] for claim in joined["claims"]} == {"source_alpha", "source_beta"}
        assert {objection["branch_instance"] for objection in joined["objections"]} == {
            "counter_primary", "counter_crosscheck"}
        assert joined["requires_scope"] is expected_scope
        scope_routes = [r for r in rows if r["kind"] == "route"
                        and r["value"]["field"] == "join.requires_scope"]
        assert len(scope_routes) == 1 and scope_routes[0]["rowid"] < next(
            r["rowid"] for r in rows if r["kind"] == "synthesize")
        quality = [r for r in rows if r["kind"] == "quality"]
        assert [r["value"]["artifact"]["accepted"] for r in quality] == expected_quality
        accepted = [r for r in rows if r["kind"] == "acceptance"]
        released = [r for r in rows if r["kind"] == "release"]
        assert len(accepted) == len(released) == expected_accept
        return {"four_distinct_a2a_tasks": True, "counter_primary_last": True,
                "typed_join_after_all_four": True, "claims": len(joined["claims"]),
                "objections": len(joined["objections"]),
                "branch_sha256": joined["branch_artifact_sha256"],
                "requires_scope": joined["requires_scope"],
                "route_before_synthesis": True,
                "quality_verdicts": [r["value"]["artifact"]["accepted"] for r in quality],
                "quality_task_ids": [r["value"]["task_id"] for r in quality],
                "acceptances": len(accepted), "releases": len(released),
                "release_receipt": released[0]["value"] if released else None}

    try:
        cap_script = OLD / "decision-round" / "common" / "harness_server.py"
        a_process, a_url, a_health = service("source_alpha", cap_script, ["--role", "capability"])
        b_process, b_url, b_health = service("source_beta", cap_script, ["--role", "capability"])
        counter_primary_process, c_url, c_health = service("counter_primary", cap_script,
                                                           ["--role", "capability"])
        d_process, d_url, d_health = service("counter_crosscheck", cap_script,
                                            ["--role", "capability"])
        q_process, q_url, q_health = service("quality", COMMON / "quality_server.py", [])
        r_process, r_url, r_health = service("release", COMMON / "release_server.py",
                                            ["--mode", "participating"])
        evidence["four_distinct_capability_harness_identities"] = len({
            a_health["identity"], b_health["identity"], c_health["identity"],
            d_health["identity"]}) == 4
        assert evidence["four_distinct_capability_harness_identities"]
        def binding(role, endpoint, health):
            return {"role": role, "url": endpoint, "identity": health["identity"],
                    "approved": True}
        approved = {
            "source": binding("capability", a_url, a_health),
            "counter": binding("capability", c_url, c_health),
            "source_alpha": binding("capability", a_url, a_health),
            "source_beta": binding("capability", b_url, b_health),
            "counter_primary": binding("capability", c_url, c_health),
            "counter_crosscheck": binding("capability", d_url, d_health),
            "quality": binding("quality", q_url, q_health),
            "release": binding("release", r_url, r_health),
        }
        (base / "approved.json").write_text(json.dumps(approved))
        v2_bindings = {key: approved[key] for key in ("source", "counter", "quality", "release")}
        v4_bindings = {key: approved[key] for key in (
            "source_alpha", "source_beta", "counter_primary", "counter_crosscheck",
            "quality", "release")}
        helper = start_helper()
        v2, v2_child = materialize("visible-v3.json", v2_bindings)
        v2["root"]["revision"] = v2["children"][v2_child]["revision"] = "v2"
        v2_new_digest = definition.digest(v2["children"][v2_child])
        v2["children"] = {v2_new_digest: v2["children"][v2_child]}
        v2["root"]["nodes"]["invoke_child"]["child_digest"] = v2_new_digest
        (base / "v2-old-native.json").write_text(json.dumps(v2, indent=2, sort_keys=True) + "\n")
        code, v2_pub = api(url, "/publish", v2)
        assert code == 200, v2_pub
        code, v2_start = api(url, "/start", {"id": "a2b-old-v2",
            "package_digest": v2_pub["package_digest"], "input": input_for(3)})
        assert code == 200, v2_start
        old_id, old_wait = until(lambda: child_wait("a2b-old-v2"), label="old v2 wait")
        evidence["old_v2_wait"] = {"child_id": old_id,
            "digest": old_wait["run"]["digest"], "revision": old_wait["run"]["revision"]}

        v4a, v4a_child = materialize("withheld-v4a.json", v4_bindings)
        v4b, v4b_child = materialize("withheld-v4b.json", v4_bindings)
        code, v4a_pub = api(url, "/publish", v4a)
        assert code == 200, v4a_pub
        code, v4b_pub = api(url, "/publish", v4b)
        assert code == 200, v4b_pub
        evidence["v4a_package_digest"] = v4a_pub["package_digest"]
        evidence["v4b_package_digest"] = v4b_pub["package_digest"]
        evidence["same_helper_after_publications"] = (
            helper.poll() is None and api(url, "/health")[1]["pid"] == helper.pid)
        assert evidence["same_helper_after_publications"]
        assert child_wait("a2b-old-v2") is not None

        success_id, _, hold_success = delayed_start("a2b-v4a-success", v4a_pub, v4a_child, 1)
        success = until(lambda: complete("a2b-v4a-success"), label="v4a accepted")
        assert success["status"] == "accepted" and success["child"]["repairs"] == 1
        evidence["v4a_success"] = analyze("a2b-v4a-success", success_id, True,
                                           [False, True], 1)
        evidence["v4a_success"]["other_three_finished_before_primary_resumed"] = hold_success
        assert hold_success

        exhausted_id, _, hold_exhausted = delayed_start("a2b-v4a-exhausted", v4a_pub, v4a_child, 3)
        waited_id, waited = until(lambda: child_wait("a2b-v4a-exhausted"), label="v4a Director wait")
        assert waited_id == exhausted_id and waited["run"]["repair_count"] == 2
        evidence["v4a_exhaustion"] = analyze("a2b-v4a-exhausted", exhausted_id,
                                              True, [False, False, False], 0)
        evidence["v4a_exhaustion"]["other_three_finished_before_primary_resumed"] = hold_exhausted
        assert hold_exhausted
        current = waited["run"]
        command = {"id": exhausted_id, "action": "abort", "actor": "director-a2b",
            "token": "director-a2b-token", "epoch": 1, "digest": current["digest"],
            "revision": current["revision"], "sha256": current["sha256"]}
        forged = api(url, "/decide", {**command, "actor": "forged"})
        stale = api(url, "/decide", {**command, "revision": "r1"})
        assert forged[0] == 403 and stale[0] == 409
        valid = api(url, "/decide", command)
        assert valid[0] == 200
        aborted = until(lambda: complete("a2b-v4a-exhausted"), label="v4a abort")
        assert aborted["status"] == "aborted" and aborted["child"]["repairs"] == 2
        evidence["v4a_exhaustion"]["authorized_abort"] = True

        direct_id, _, hold_direct = delayed_start("a2b-v4b-direct", v4b_pub, v4b_child, 3)
        direct = until(lambda: complete("a2b-v4b-direct"), label="v4b direct r1")
        assert direct["status"] == "accepted" and direct["child"]["repairs"] == 0
        evidence["v4b_direct"] = analyze("a2b-v4b-direct", direct_id, False, [True], 1)
        evidence["v4b_direct"]["other_three_finished_before_primary_resumed"] = hold_direct
        assert hold_direct

        helper.kill()
        helper.wait(timeout=10)
        helper = start_helper()
        old_again_id, old_again = until(lambda: child_wait("a2b-old-v2"),
                                         label="old v2 wait after helper restart")
        assert old_again_id == old_id and old_again["run"]["digest"] == old_wait["run"]["digest"]
        evidence["old_v2_pinned_after_restart"] = True
        code, _ = api(url, "/decide", {"id": old_id, "action": "abort",
            "actor": "director-a2b", "token": "director-a2b-token", "epoch": 1,
            "digest": old_again["run"]["digest"], "revision": old_again["run"]["revision"],
            "sha256": old_again["run"]["sha256"]})
        assert code == 200
        old_done = until(lambda: complete("a2b-old-v2"), label="old v2 abort")
        assert old_done["status"] == "aborted"

        with sqlite3.connect(engine_db) as conn:
            evidence["native_effect_workflow_entities"] = conn.execute(
                "SELECT COUNT(DISTINCT entity_id) FROM cluster_messages "
                "WHERE entity_type='Workflow/EffectParityFactoryRun'").fetchone()[0]
        assert evidence["native_effect_workflow_entities"] >= 8
        evidence["freeze_after"] = freeze_verify()
        assert evidence["freeze_before"] == evidence["freeze_after"]
        evidence["passed"] = True
    except Exception as error:
        evidence["passed"] = False
        evidence["error"] = repr(error)
        raise
    finally:
        if counter_primary_process and counter_primary_process.poll() is None:
            counter_primary_process.send_signal(signal.SIGCONT)
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
                try: process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        try: evidence["freeze_after"] = freeze_verify()
        except Exception as error: evidence["freeze_after_error"] = repr(error)
        (HERE / "a2b-observed.json").write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
        print(json.dumps(evidence, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
