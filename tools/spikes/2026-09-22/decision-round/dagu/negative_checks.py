"""Product policy negatives against an already completed trial's copied state."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import sqlite3

from adapter import accept_quality
from publisher import closure, publish


HERE = Path(__file__).resolve().parent
DAGU = Path("/tmp/exomachina-countertrials/dagu/dagu")


def expect_value_error(call, contains: str) -> str:
    try:
        call()
    except ValueError as error:
        if contains not in str(error):
            raise AssertionError(f"expected {contains!r}; got {error!r}") from error
        return str(error)
    raise AssertionError(f"expected rejection containing {contains!r}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("runtime", type=Path)
    args = parser.parse_args()
    rt = args.runtime.resolve()
    source = HERE / "definitions"
    destination = rt / "negative-checks"
    if destination.exists():
        raise SystemExit(f"negative checks already exist: {destination}")
    destination.mkdir()
    output: dict[str, object] = {}

    with sqlite3.connect(rt / "ledger.sqlite") as original, sqlite3.connect(destination / "ledger.sqlite") as clone:
        original.backup(clone)
    db_path = destination / "ledger.sqlite"
    with sqlite3.connect(db_path) as db:
        db.row_factory = sqlite3.Row
        row = db.execute("SELECT * FROM runs WHERE run_id='dagu-product-v1-001'").fetchone()
        row = dict(row)
    run_id = row["run_id"]
    good = {"accepted": True, "revision": row["current_revision"],
            "sha256": row["current_sha256"], "reviewer": row["accepted_reviewer"]}

    def attempt(decision: dict, action_id: str, definition_digest: str | None = None,
                target_run: str | None = None) -> None:
        with sqlite3.connect(db_path) as db:
            db.row_factory = sqlite3.Row
            accept_quality(db, target_run or run_id, definition_digest or row["definition_digest"],
                           decision, action_id)

    output["stale_revision"] = expect_value_error(
        lambda: attempt({**good, "revision": "r1"}, run_id + ":other-stale"),
        "stale or different")
    output["self_review"] = expect_value_error(
        lambda: attempt({**good, "reviewer": row["author"]}, run_id + ":other-self"),
        "self-review")
    output["duplicate_acceptance"] = expect_value_error(
        lambda: attempt(good, run_id + ":other-review"),
        "duplicate acceptance")
    output["wrong_definition"] = expect_value_error(
        lambda: attempt(good, run_id + ":wrong-definition", "incorrect-closure"),
        "definition binding mismatch")
    output["wrong_run"] = expect_value_error(
        lambda: attempt(good, run_id + ":wrong-run", target_run="another-run"),
        "no current artifact")
    attempt(good, row["accepted_action_id"])
    with sqlite3.connect(db_path) as db:
        db.row_factory = sqlite3.Row
        retained = db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        assert retained["accepted_action_id"] == row["accepted_action_id"]
        assert retained["release_count"] == 1
    output["same_action_retry"] = "idempotent; one accepted action and release"

    def variant(name: str) -> Path:
        folder = destination / name
        shutil.copytree(source, folder)
        return folder

    missing = variant("missing-child")
    (missing / "exo_decision_leaf_v1.yaml").unlink()
    output["missing_child"] = expect_value_error(lambda: closure(missing, "v1"), "missing child")

    cycle = variant("cycle")
    (cycle / "exo_decision_leaf_v1.yaml").write_text(
        "steps:\n  - id: assign\n    action: dag.run\n    with:\n      dag: exo_decision_capability_v1\n")
    output["child_cycle"] = expect_value_error(lambda: closure(cycle, "v1"), "cycle")

    bypass = variant("review-bypass")
    root = bypass / "exo_decision_factory_v1.yaml"
    root.write_text(root.read_text().replace("  - id: review\n    depends: capability",
                                             "  - id: review\n    depends: publication_gate"))
    output["review_bypass"] = expect_value_error(lambda: closure(bypass, "v1"), "Quality must wait")

    mixed = variant("cross-version")
    child = mixed / "exo_decision_capability_v1.yaml"
    child.write_text(child.read_text().replace("exo_decision_leaf_v1", "exo_decision_leaf_v2"))
    output["cross_version"] = expect_value_error(lambda: closure(mixed, "v1"), "cross-version")

    immutable = variant("immutable-conflict")
    home = destination / "home"
    publish(immutable, home, "v1", DAGU)
    leaf = immutable / "exo_decision_leaf_v1.yaml"
    leaf.write_text(leaf.read_text() + "\n")
    output["immutable_overwrite"] = expect_value_error(
        lambda: publish(immutable, home, "v1", DAGU), "immutable definition conflict")

    (destination / "result.json").write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"result": "passed", "evidence": str(destination / "result.json")}, sort_keys=True))


if __name__ == "__main__":
    main()
