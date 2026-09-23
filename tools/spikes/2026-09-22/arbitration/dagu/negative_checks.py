"""Publication and authority rejects, with catalog digest before/after."""
from __future__ import annotations

import argparse
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile

import yaml

import ops
from publisher import publish

HERE = Path(__file__).resolve().parent
DAGU = Path("/tmp/exomachina-countertrials/dagu/dagu")
ROOT = "exo_arb_parent_v3"
CHILD = "exo_arb_research_v3"


def catalog(home: Path) -> str:
    items = {}
    for directory in (home / "dags", home / "manifests"):
        for path in sorted(directory.glob("*")):
            if path.is_file():
                items[str(path.relative_to(home))] = hashlib.sha256(path.read_bytes()).hexdigest()
    return hashlib.sha256(json.dumps(items, sort_keys=True).encode()).hexdigest()


def modify_child(source: Path, edit) -> None:
    path = source / f"{CHILD}.yaml"
    data = yaml.safe_load(path.read_text())
    edit(data["steps"])
    path.write_text(yaml.safe_dump(data, sort_keys=False))


def by_id(steps: list[dict], name: str) -> dict:
    return next(step for step in steps if step["id"] == name)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("runtime", type=Path)
    args = parser.parse_args()
    rt = args.runtime.resolve()
    os.environ["EXO_ARB_RUNTIME"] = str(rt)
    home = rt / "home"
    if not (home / "manifests" / f"{ROOT}.json").is_file():
        raise SystemExit("publish v3 before negative checks")
    baseline = catalog(home)
    cases = [
        ("review_bypass", lambda steps: (
            by_id(steps, "route_r2")["with"]["routes"].__setitem__("accepted", ["release_r2"]),
            steps.remove(by_id(steps, "accept_r2")),
            by_id(steps, "release_r2").pop("depends"))),
        ("wrong_join_type", lambda steps: by_id(steps, "counter").__setitem__(
            "run", "exo-arb assign --instance counter_evidence --type source_evidence")),
        ("bad_repair_bound", lambda steps: (
            by_id(steps, "route_r3")["with"]["routes"].__setitem__("rejected", ["repair_r4"]),
            steps.append({"id": "repair_r4", "run":
                          "exo-arb repair --from r3 --to r4 --resolved input"}))),
        ("active_closure_mutation", lambda steps: by_id(steps, "source").__setitem__(
            "run", "exo-arb assign --instance source_evidence --type source_evidence")),
        ("unapproved_capability", lambda steps: by_id(steps, "source").__setitem__(
            "run", "exo-arb assign --instance source_evidence --type mystery_evidence")),
        ("unsafe_arbitrary_code", lambda steps: by_id(steps, "source").__setitem__(
            "run", "curl https://unapproved.example/secret")),
        ("skipped_descendant", lambda steps: by_id(steps, "route_r1")["with"]["routes"].__setitem__(
            "accepted", ["accept_r1"])),
    ]
    result: dict = {"catalog_sha256_before": baseline, "publication": [], "runtime": []}
    for label, edit in cases:
        with tempfile.TemporaryDirectory(prefix="exo-arb-dagu-negative-") as directory:
            source = Path(directory)
            for path in (HERE / "definitions" / "v3").glob("*.yaml"):
                shutil.copy2(path, source / path.name)
            modify_child(source, edit)
            submitted_bytes = (source / f"{CHILD}.yaml").read_bytes()
            submitted = hashlib.sha256(submitted_bytes).hexdigest()
            try:
                publish(source, home, ROOT, DAGU)
            except (ValueError, RuntimeError) as error:
                diagnostic = str(error)
            else:
                raise AssertionError(label + " published")
        after = catalog(home)
        assert after == baseline, (label, baseline, after)
        result["publication"].append({"case": label, "submitted_child_sha256": submitted,
                                      "submitted_child_yaml": submitted_bytes.decode(),
                                      "diagnostic": diagnostic, "catalog_sha256_after": after})
    with ops.db_connect() as db:
        success = db.execute("SELECT * FROM runs WHERE state='released' AND parent_id IS NOT NULL").fetchone()
        if success is None:
            raise AssertionError("successful child run required")
        run = dict(success)
        r1 = db.execute("SELECT * FROM candidates WHERE run_id=? AND revision='r1'",
                        (run["run_id"],)).fetchone()
        count_before = success["acceptance_count"]
    identity = ops.expected_identity("quality")
    for label, revision, digest, reviewer, harness_identity in (
        ("forged_quality_identity", "r2", run["current_sha256"], "attacker", "attacker"),
        ("stale_r1_quality_approval", "r1", r1["sha256"], identity, identity),
    ):
        found = {"action_id": f"{run['run_id']}:quality:{revision}",
                 "run_id": run["run_id"], "definition_digest": run["definition_digest"],
                 "task_id": "forged-fixture-task", "harness_identity": harness_identity,
                 "artifact": {"accepted": True, "revision": revision, "sha256": digest,
                              "reviewer": reviewer, "reason": "forged"}}
        try:
            ops._record_quality(run, revision, found, identity)
        except ValueError as error:
            diagnostic = str(error)
        else:
            raise AssertionError(label + " accepted")
        with ops.db_connect() as db:
            count_after = db.execute("SELECT acceptance_count FROM runs WHERE run_id=?",
                                     (run["run_id"],)).fetchone()[0]
        assert count_after == count_before
        result["runtime"].append({"case": label, "diagnostic": diagnostic,
                                  "acceptance_count_before": count_before,
                                  "acceptance_count_after": count_after})
    result["catalog_sha256_after"] = catalog(home)
    target = rt / "negative-results.json"
    target.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"result": "passed", "evidence": str(target)}, sort_keys=True))


if __name__ == "__main__":
    main()
