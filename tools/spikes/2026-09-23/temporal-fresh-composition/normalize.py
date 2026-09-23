"""Map retained native/receiver evidence into the shared arbitration oracle."""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "2026-09-22" / "arbitration" / "common"))
import fixture  # noqa: E402
import oracle  # noqa: E402


def db_rows(path, query, *args):
    with sqlite3.connect(path) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute(query, args)]


def records(state, service, run):
    path = state / service / "harness.sqlite3"
    values = db_rows(path, "SELECT * FROM actions WHERE run_id=?", run)
    for value in values:
        value["artifact"] = json.loads(value["artifact"])
    return values


def identity(state, service):
    return db_rows(state / service / "harness.sqlite3", "SELECT id FROM identity")[0]["id"]


def normalize(state, run, parent_result, outcome, *, wait=None):
    source = records(state, "source", run)
    counter = records(state, "counter", run)
    assert len(source) == len(counter) == 1
    digest = source[0]["definition_digest"]
    branches = {}
    for name, row, service in (("source_evidence", source[0], "source"),
                               ("counter_evidence", counter[0], "counter")):
        branches[name] = {"action_id": row["action_id"], "run_id": run,
            "definition_digest": digest, "task_id": row["task_id"],
            "artifact": row["artifact"], "harness_identity": identity(state, service),
            "harness_role": "capability"}
    join = fixture.typed_join(branches, run_id=run, definition_digest=digest)
    events = [json.loads(line) for line in (state / "activities.jsonl").read_text().splitlines()
              if run in line]
    intervals = {}
    for name in branches:
        start = next(event["wall_time"] for event in events
                     if event["kind"] == "assign-start" and event.get("instance") == name)
        end = next(event["wall_time"] for event in events
                   if event["kind"] == "assign-complete" and event.get("instance") == name)
        intervals[name] = {"start": int(start * 1_000_000_000),
                           "end": int(end * 1_000_000_000)}
    quality_identity = identity(state, "quality")
    quality_rows = records(state, "quality", run)
    by_revision = {row["artifact"]["revision"]: row for row in quality_rows}
    expected = [False, True] if outcome == "success" else [False, False, False]
    revisions = []
    for index, accepted in enumerate(expected, 1):
        revision = f"r{index}"
        artifact = fixture.candidate_artifact(join, revision, f"factory:{run}",
                                               resolved=accepted)
        row = by_revision[revision]
        quality = {"action_id": row["action_id"], "run_id": run,
            "definition_digest": digest, "task_id": row["task_id"],
            "artifact": row["artifact"], "harness_identity": quality_identity,
            "harness_role": "quality"}
        revisions.append({"artifact": artifact, "quality": quality})
    child = parent_result["child"]
    def public(result):
        if outcome == "success":
            return {"status": result["status"],
                    "revision": result["acceptance"]["revision"],
                    "sha256": result["acceptance"]["sha256"],
                    "release_id": result["receipt"]["release_id"]}
        return {"status": result["status"], "revision": "r3"}
    acceptance = None
    receipt = None
    director_wait = None
    if outcome == "success":
        accepted = parent_result["acceptance"]
        acceptance = {"count": 1, "run_id": accepted["run"],
            "definition_digest": accepted["definition_digest"],
            "revision": accepted["revision"], "sha256": accepted["sha256"],
            "quality_task_id": accepted["quality_task_id"]}
        receipt = parent_result["receipt"]
    else:
        director_wait = {"scope": "nested_child", "current_revision": "r3",
            "persisted_after_restart": (wait["before"]["phase"] == "awaiting-director"
                                        and wait["after"]["phase"] == "awaiting-director"),
            "command": {"decision": "abort", "authorized": True,
                "revision": "r3", "accepted": wait["new_response"]["kind"] == "task"}}
    return {"run_id": run, "definition_digest": digest,
        "quality_identity": quality_identity, "branch_receipts": branches,
        "join": join, "branch_intervals_ns": intervals, "revisions": revisions,
        "repair_count": len(expected) - 1, "acceptance": acceptance,
        "release_receipt": receipt, "director_wait": director_wait,
        "public_child_result": public(child),
        "public_parent_result": public(parent_result)}


def main():
    visible = json.loads((HERE / "observed.json").read_text())
    fault = json.loads((HERE / "fault-observed.json").read_text())
    success_result = visible["checks"]["visible_success"]
    success = normalize(Path(visible["state"]), success_result["child"]["run"],
                        success_result, "success")
    wait = fault["checks"]["wait_restart_stale_owner"]
    exhausted = normalize(Path(fault["state"]), wait["before"]["run"],
                          wait["parent_result"], "exhaustion", wait=wait)
    verdicts = {"success": oracle.verify_run(success, "success"),
                "exhaustion": oracle.verify_run(exhausted, "exhaustion")}
    output = {"oracle": verdicts, "runs": {"success": success,
                                            "exhaustion": exhausted}}
    (HERE / "oracle-observed.json").write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    print(json.dumps(verdicts, sort_keys=True))


if __name__ == "__main__":
    main()
