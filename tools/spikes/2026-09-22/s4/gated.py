"""One integrated native A2A -> product acceptance -> Kestra continuation slice."""

import json
import os
import subprocess
import sys
from pathlib import Path

import kestra_bridge as bridge

ROOT = Path(__file__).parent
probe = bridge.probe


def start(base):
    bridge.init(base)
    source = (ROOT / "a2a-gated.yaml").read_text()
    code, flow = probe.call(bridge.KESTRA, "POST", "/flows", source, "application/x-yaml")
    assert code in (200, 201), (code, flow)
    code, execution = probe.call(
        bridge.KESTRA, "POST", "/executions/exomachina.spikes/exomachina_s4_a2a_gated",
        b"--empty--\r\n", "multipart/form-data; boundary=empty",
    )
    assert code in (200, 201), (code, execution)
    execution_id = execution["id"]
    paused = probe.wait_for(bridge.KESTRA, execution_id, {"PAUSED", "FAILED"})
    assert probe.state(paused) == "PAUSED", paused.get("state")
    assert [r["taskId"] for r in paused["taskRunList"]] == ["assign", "quality_gate"]
    action_id = "remote-" + execution_id
    with bridge.db(base) as conn:
        conn.execute("INSERT INTO binding VALUES (?, ?)", (action_id, execution_id))
        conn.execute("INSERT INTO assignments VALUES (?, 'a2a', 'intent', NULL)", (action_id,))
    os._exit(41)  # Native engine persisted result; product has not observed it.


def observe(base):
    action_id, execution_id = bridge.one_binding(base)
    code, execution = probe.call(bridge.KESTRA, "GET", "/executions/" + execution_id)
    assert code == 200 and probe.state(execution) == "PAUSED"
    assign = next(r for r in execution["taskRunList"] if r["taskId"] == "assign")
    code, output = probe.call(bridge.KESTRA, "GET", f"/outputs/tasks/{execution_id}/{assign['id']}")
    assert code == 200 and output["code"] == 200
    message = json.loads(output["body"])["result"]
    assert message["kind"] == "task" and message["status"]["state"] == "completed"
    assert len(message["artifacts"]) == 1
    artifact = message["artifacts"][0]
    part = artifact["parts"][0]
    assert part["kind"] == "data" and part["data"]["sha256"] == artifact["artifactId"]
    with bridge.db(base) as conn:
        conn.execute("UPDATE assignments SET status='submitted', task_id=? WHERE action_id=?", (message["id"], action_id))
        conn.execute("INSERT INTO artifacts VALUES (?, 'capability-service', ?, NULL, NULL)", (action_id, artifact["artifactId"]))
    (base / "observed-a2a.json").write_text(json.dumps({
        "execution_id": execution_id,
        "a2a_task_id": message["id"],
        "a2a_run_id": message["metadata"]["run_id"],
        "artifact_id": artifact["artifactId"],
        "a2a_state": message["status"]["state"],
    }, indent=2) + "\n")


def accept(base):
    action_id, execution_id = bridge.one_binding(base)
    with bridge.db(base) as conn:
        author, revision, accepted = conn.execute("SELECT author,current_revision,accepted_revision FROM artifacts WHERE action_id=?", (action_id,)).fetchone()
        assert accepted is None
        def approve(candidate, reviewer):
            if candidate != revision or reviewer == author:
                return False
            conn.execute("UPDATE artifacts SET accepted_revision=?, reviewer=? WHERE action_id=?", (candidate, reviewer, action_id))
            conn.execute("INSERT INTO outbox(id,action_id,revision) VALUES (?, ?, ?)", ("resume-" + execution_id, action_id, candidate))
            return True
        assert not approve("stale-revision", "quality-1")
        assert not approve(revision, author)
        assert approve(revision, "quality-1")
    os._exit(42)  # Product decision committed; engine still waiting.


def verify(base):
    action_id, execution_id = bridge.one_binding(base)
    execution = probe.wait_for(bridge.KESTRA, execution_id, {"SUCCESS", "FAILED", "KILLED"})
    assert probe.state(execution) == "SUCCESS"
    task_runs = execution["taskRunList"]
    assert [r["taskId"] for r in task_runs] == ["assign", "quality_gate", "deliver"]
    assert all(r["state"]["current"] == "SUCCESS" for r in task_runs)
    with bridge.db(base) as conn:
        revision, accepted, reviewer = conn.execute("SELECT current_revision,accepted_revision,reviewer FROM artifacts WHERE action_id=?", (action_id,)).fetchone()
        done = conn.execute("SELECT done FROM outbox").fetchone()[0]
    assert revision == accepted and reviewer == "quality-1" and done == 1
    observed = json.loads((base / "observed-a2a.json").read_text())
    result = {"result": "PASS", "engine": "Kestra OSS 2.0.3 / PostgreSQL", "a2a_protocol": "0.3.0", "execution_id": execution_id, "a2a_task_id": observed["a2a_task_id"], "accepted_artifact_id": revision, "reviewer": reviewer, "outbox_done": done, "engine_terminal": probe.state(execution), "downstream_delivery_runs": 1, "forced_client_exits": [41, 42, 33]}
    (base / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


def test(base):
    if base.exists() and any(base.iterdir()):
        raise SystemExit(f"Refusing to overwrite {base}")
    def phase(command, expected=0):
        child = subprocess.run([sys.executable, __file__, command, str(base)], capture_output=True, text=True)
        assert child.returncode == expected, f"{command} exit={child.returncode}: {child.stdout} {child.stderr}"
        return child.stdout
    phase("start", 41)
    phase("observe")
    phase("accept", 42)
    phase("drain-drop", 33)
    phase("drain-reconcile")
    print(phase("verify"), end="")


if __name__ == "__main__":
    command = sys.argv[1]
    base = Path(sys.argv[2])
    if command == "test":
        test(base)
    elif command == "start":
        start(base)
    elif command == "observe":
        observe(base)
    elif command == "accept":
        accept(base)
    elif command == "drain-drop":
        bridge.drain(base, True)
    elif command == "drain-reconcile":
        bridge.drain(base, False)
    elif command == "verify":
        verify(base)
    else:
        raise SystemExit(f"Unknown command: {command}")
