"""Normalize distinct receiver, Quality, product and release counters."""
from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent


def rows(path, table):
    with sqlite3.connect(path) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute(f"SELECT * FROM {table}")]


def main():
    observed = json.loads((HERE / "fault-observed.json").read_text())
    state = Path(observed["state"])
    grouped = defaultdict(lambda: {"remote_submission_attempts": 0,
        "receiver_assignment_effects": 0, "quality_negative_verdicts": 0,
        "quality_positive_verdicts": 0, "quality_receiver_effects": 0,
        "authoritative_product_acceptances": 0,
        "release_submission_attempts": 0, "release_effects": 0,
        "release_receipts": 0, "a2a_tasks": []})
    for service in ("source", "counter"):
        for row in rows(state / service / "harness.sqlite3", "actions"):
            record = grouped[row["run_id"]]
            record["remote_submission_attempts"] += row["attempts"]
            record["receiver_assignment_effects"] += row["accepted_count"]
            record["a2a_tasks"].append({"service": service, "task_id": row["task_id"],
                                          "action_id": row["action_id"]})
    for row in rows(state / "quality" / "harness.sqlite3", "actions"):
        record = grouped[row["run_id"]]
        verdict = json.loads(row["artifact"])
        record["quality_positive_verdicts" if verdict["accepted"] else
               "quality_negative_verdicts"] += 1
        record["quality_receiver_effects"] += row["accepted_count"]
        record["a2a_tasks"].append({"service": "quality", "task_id": row["task_id"],
                                      "action_id": row["action_id"], "verdict": verdict})
    for row in rows(state / "release" / "release.sqlite3", "releases"):
        record = grouped[row["run_id"]]
        record["release_submission_attempts"] += row["attempts"]
        record["release_effects"] += row["accepted_effect_count"]
        record["release_receipts"] += 1
    checks = observed["checks"]
    for key in ("mid_assignment_engine_pg_kill", "quality_remote_commit_gap",
                "acceptance_before_continuation", "participating_lost_acks"):
        parent = checks[key]["result"] if "result" in checks[key] else checks[key]
        child = parent["child"]
        if child.get("acceptance"):
            grouped[child["run"]]["authoritative_product_acceptances"] += 1
    opaque = checks["opaque_durable_unknown"]
    opaque_run = opaque["after"]["run"]
    if opaque["after"]["authoritative_acceptance"]:
        grouped[opaque_run]["authoritative_product_acceptances"] += 1
    grouped[opaque_run]["opaque_receiver_effects_without_receipt"] = opaque["effects_after"][0]["effects"]
    grouped[opaque_run]["release_submission_attempts"] = len(
        rows(state / "opaque" / "release.sqlite3", "opaque_effects"))
    output = {"state": str(state), "runs": dict(sorted(grouped.items())),
        "counter_semantics": "Quality receiver accepted_count records an action even for a negative verdict; product acceptance comes from Temporal Workflow history."}
    (HERE / "counts-observed.json").write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"runs": len(grouped), "file": str(HERE / "counts-observed.json")}))


if __name__ == "__main__":
    main()
