"""Post-freeze guard probe; works on an isolated copy of observed product state."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import sqlite3

import ops


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source_runtime", type=Path)
    parser.add_argument("probe_runtime", type=Path)
    args = parser.parse_args()
    source = args.source_runtime.resolve()
    probe = args.probe_runtime.resolve()
    if probe.exists():
        raise SystemExit("probe runtime must be fresh")
    probe.mkdir(parents=True)
    shutil.copy2(source / "config.json", probe / "config.json")
    shutil.copytree(source / "home" / "manifests", probe / "home" / "manifests")
    shutil.copytree(source / "home" / "dags", probe / "home" / "dags")
    with sqlite3.connect(source / "ledger.sqlite") as original, sqlite3.connect(probe / "ledger.sqlite") as copied:
        original.backup(copied)
    os.environ["EXO_ARB_RUNTIME"] = str(probe)
    with ops.db_connect() as db:
        row = db.execute("SELECT * FROM runs WHERE state='released' AND parent_id IS NOT NULL "
                         "AND accepted_revision='r2' LIMIT 1").fetchone()
        if row is None:
            raise AssertionError("one accepted r2 child required")
        run_id = row["run_id"]
        verdict = db.execute("SELECT * FROM verdicts WHERE run_id=? AND revision='r2'",
                             (run_id,)).fetchone()
        if verdict is None:
            raise AssertionError("r2 verdict required")
        positive = json.loads(verdict["verdict_json"])
        expected_identity = ops.expected_identity("quality")
        db.execute("UPDATE runs SET accepted_revision=NULL,accepted_sha256=NULL,accepted_reviewer=NULL,"
                   "acceptance_count=0,release_count=0,receipt_json=NULL,state='active' WHERE run_id=?", (run_id,))
        db.execute("UPDATE verdicts SET accepted=0,state='intent',task_id=NULL,verdict_json=NULL "
                   "WHERE action_id=?", (verdict["action_id"],))
    fake = {"action_id": f"{run_id}:quality:r2", "run_id": run_id,
            "definition_digest": row["definition_digest"],
            "task_id": "forged-metadata-omitted-task", "artifact": positive}
    recorded = ops._record_quality(dict(row), "r2", fake, expected_identity)
    os.environ["DAG_RUN_ID"] = run_id
    accepted = ops.accept("r2")
    with ops.db_connect() as db:
        final = db.execute("SELECT acceptance_count,accepted_revision,accepted_sha256 "
                           "FROM runs WHERE run_id=?", (run_id,)).fetchone()
        saved = db.execute("SELECT task_id,reviewer,accepted,state FROM verdicts WHERE action_id=?",
                           (fake["action_id"],)).fetchone()
    output = {"schema": "exomachina.arbitration.dagu.identity-gap/1",
              "guard_result": "accepted_forged_record_without_harness_identity",
              "source_run_id": run_id, "missing_harness_identity": "harness_identity" not in fake,
              "forged_task_id": fake["task_id"], "verdict_accepted": recorded["accepted"],
              "saved_verdict": dict(saved), "product_acceptance": dict(final),
              "accept_result": accepted,
              "scope": "isolated direct guard invocation on copied product ledger; no live HTTP ingress"}
    (probe / "result.json").write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"result": "gap_demonstrated", "evidence": str(probe / "result.json")}, sort_keys=True))


if __name__ == "__main__":
    main()
