"""Bounded Graph-backed Director variant of the existing S2 Strands harness.

The imported S2 harness provides the Strands Agent/tool loop, persistent
identity, authorization, A2A Task adapter and task-alias table. This variant
replaces its fixed factory command with the candidate Graph run/ledger.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from uuid import uuid4

import uvicorn

HERE = Path(__file__).resolve().parent
S2 = HERE.parents[1] / "s2"
sys.path.insert(0, str(S2))
import harness as s2_harness  # noqa: E402
import server as s2_server  # noqa: E402

from probe import binding, current_candidate, db, queue_decision, resume, runner, start_one  # noqa: E402


class GraphHarness(s2_harness.Harness):
    """The same S2 harness shell, with core Graph as its factory runtime."""

    def __init__(self, directory: Path, role: str, organization: str = "org-fixture"):
        if role != "factory":
            raise s2_harness.Rejected("Graph Director requires factory role")
        super().__init__(directory, role, organization)
        configuration = json.loads((self.directory / "config.json").read_text())
        configuration["engine"] = "strands-graph-1.57.0"
        (self.directory / "config.json").write_text(json.dumps(configuration, indent=2) + "\n")

    def _project(self, run_id: str) -> dict:
        bound = binding(self.directory, run_id)
        with db(self.directory) as connection:
            accepted = connection.execute("SELECT * FROM acceptances WHERE run_id=?", (run_id,)).fetchone()
            delivered = connection.execute("SELECT * FROM deliveries WHERE run_id=?", (run_id,)).fetchone()
        output = None
        state = "input-required"
        if delivered is not None:
            candidate = current_candidate(self.directory, run_id)
            if accepted is None or accepted["definition_digest"] != bound["digest"] or accepted["artifact_sha256"] != candidate["sha256"] or delivered["artifact_sha256"] != candidate["sha256"]:
                raise s2_harness.Rejected("Graph completion lacks exact accepted artifact")
            output = {**candidate, "reviewer": accepted["reviewer"], "definition_digest": bound["digest"]}
            state = "completed"
        return {"id": run_id, "owner": self.identity, "organization": self.organization,
                "definition": f"graph.v{bound['revision']}:{bound['digest']}",
                "state": state, "accepted_output": output, "child": None}

    def command(self, command: dict, actor: str = "director", delivery: tuple[str, str] | None = None) -> dict:
        op = command.get("op")
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._authorize(connection, actor, op not in {"inspect"})
            if command.get("expected_incarnation", self.incarnation) != self.incarnation:
                raise s2_harness.Rejected("stale Director incarnation")
        if op == "start":
            run_id = command.get("run_id") or str(uuid4())
            revision = command.get("revision", 1)
            if type(revision) is not int or revision not in (1, 2):
                raise s2_harness.Rejected("unapproved factory revision")
            path = self.directory / f"{run_id}.json"
            if path.exists():
                if binding(self.directory, run_id)["revision"] != revision:
                    raise s2_harness.Rejected("run ID reused with another revision")
            else:
                start_one(self.directory, run_id, revision)
        elif op == "decide":
            run_id = command["run_id"]
            if not (self.directory / f"{run_id}.json").exists():
                raise s2_harness.Rejected("unknown Graph run")
            queue_decision(self.directory, run_id)
            result = resume(self.directory, run_id, self.identity, 15)
            if result.get("claim") == "rejected" and self._project(run_id)["state"] != "completed":
                raise s2_harness.Rejected("another Director owns this run")
        elif op == "inspect":
            run_id = command["run_id"]
        else:
            raise s2_harness.Rejected("unsupported Graph Director command")
        projection = self._project(run_id)
        if delivery is not None:
            with self.connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                self._bind(connection, run_id, delivery)
        return projection

    def task_record(self, task_id: str):
        with self.connect() as connection:
            self._authorize(connection, "observer", False)
            alias = connection.execute("SELECT run_id,context_id FROM aliases WHERE task_id=?", (task_id,)).fetchone()
        if alias is None:
            return None
        return self._project(alias["run_id"]), alias["context_id"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    s2_server.Harness = GraphHarness
    uvicorn.run(s2_server.create_app(args.state, "factory", args.port), host="127.0.0.1", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
