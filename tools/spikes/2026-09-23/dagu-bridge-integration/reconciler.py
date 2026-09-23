"""Automatic same-run Dagu recovery after a process or adapter gap.

This is product-owned recovery glue. It never creates a new logical run, and
only retries a failed native step through Dagu's own retry command.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sqlite3
import subprocess
import time

from ops import db_connect, engine_status, runtime


def retry_failed(dagu: Path) -> list[dict]:
    with db_connect() as db:
        db.executescript("""
            CREATE TABLE IF NOT EXISTS engine_retries (
              run_id TEXT NOT NULL, step_id TEXT NOT NULL,
              attempts INTEGER NOT NULL, last_at REAL NOT NULL,
              PRIMARY KEY(run_id,step_id));
        """)
        rows = [dict(row) for row in db.execute("SELECT run_id,dag_name,state FROM runs WHERE state NOT IN ('released','aborted')")]
    results: list[dict] = []
    for row in rows:
        try:
            status = engine_status(row["dag_name"], row["run_id"])
        except (OSError, ValueError):
            continue
        if not status or status["statusLabel"] not in {"failed", "aborted"}:
            continue
        failed = [node["step"]["id"] for node in status["nodes"] if node["statusLabel"] == "failed"]
        if len(failed) != 1:
            continue
        step = failed[0]
        with db_connect() as db:
            db.execute("BEGIN IMMEDIATE")
            prior = db.execute("SELECT attempts,last_at FROM engine_retries WHERE run_id=? AND step_id=?",
                               (row["run_id"], step)).fetchone()
            if prior and (prior["attempts"] >= 2 or time.time() - prior["last_at"] < 3):
                continue
            attempts = 1 if prior is None else prior["attempts"] + 1
            db.execute("INSERT OR REPLACE INTO engine_retries VALUES (?,?,?,?)",
                       (row["run_id"], step, attempts, time.time()))
        result = subprocess.run([str(dagu), "retry", "--run-id", row["run_id"], "--step", step,
                                 "--downstream", row["dag_name"]],
                                env={**os.environ, "DAGU_HOME": str(runtime() / "home"),
                                     "DAGU_AUTH_MODE": "none"}, text=True,
                                capture_output=True, timeout=25)
        results.append({"run_id": row["run_id"], "step": step, "attempts": attempts,
                        "returncode": result.returncode,
                        "stderr": result.stderr[-500:]})
    return results


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dagu", type=Path, required=True)
    args = parser.parse_args()
    log = runtime() / "reconciler-events.jsonl"
    while True:
        try:
            for result in retry_failed(args.dagu):
                with log.open("a") as stream:
                    stream.write(json.dumps(result, sort_keys=True) + "\n")
        except Exception as error:
            with log.open("a") as stream:
                stream.write(json.dumps({"error": repr(error), "at": time.time()}) + "\n")
        time.sleep(.5)


if __name__ == "__main__":
    main()
