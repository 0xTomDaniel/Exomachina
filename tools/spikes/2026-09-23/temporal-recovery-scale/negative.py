"""Six separate pre-publication rejection cases against one immutable catalog."""
from __future__ import annotations

import copy
import hashlib
import json
import subprocess
import sys
import tempfile
from pathlib import Path

from author import materialize, template
from definition import digest, publish

HERE = Path(__file__).resolve().parent


def fake_bindings():
    return {name: {"role": role, "url": f"http://127.0.0.1:{25000 + index}",
                   "identity": f"approved-{name}", "approved": True}
            for index, (name, role) in enumerate((
                ("source", "capability"), ("counter", "capability"),
                ("quality", "quality"), ("release", "release")))}


def main():
    def freeze():
        return json.loads(subprocess.check_output(
            [sys.executable, str(HERE.parent / "common" / "freeze.py"),
             "verify", str(HERE / "freeze.json")], text=True))

    freeze_before = freeze()
    authority = fake_bindings()
    catalog = Path(tempfile.mkdtemp(prefix="exo-temporal-negative-", dir="/tmp"))
    good = materialize(template("visible-v3.json"), authority)
    good_digest = publish(good, catalog, authority)
    def catalog_inventory():
        return {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted(catalog.iterdir())}
    baseline = catalog_inventory()
    results = {}
    child_digest = next(iter(good["children"]))

    def child(package):
        return package["children"][child_digest]

    def attempt(name, mutate, *, rehash_child=False):
        package = copy.deepcopy(good)
        mutate(package)
        if rehash_child:
            changed = package["children"].pop(child_digest)
            new_digest = digest(changed)
            package["children"][new_digest] = changed
            package["root"]["nodes"]["invoke_child"]["child_digest"] = new_digest
        submitted = digest(package)
        before = catalog_inventory()
        try:
            publish(package, catalog, authority)
        except (ValueError, FileExistsError) as error:
            results[name] = {"submitted_digest": submitted,
                             "diagnostic": str(error), "rejected": True,
                             "child_digest_recomputed": rehash_child,
                             "catalog_before": before,
                             "catalog_after": catalog_inventory(),
                             "catalog_unchanged": before == catalog_inventory() == baseline}
        else:
            raise AssertionError(name + " was admitted")

    attempt("review_bypass", lambda p: child(p)["nodes"]["join_evidence"].update(next="publish"), rehash_child=True)
    def wrong_join(p):
        branch = child(p)["nodes"]["gather"]["branches"]["counter_evidence"]
        branch.update(result_type="source_evidence", capability="source_evidence@1",
                      scope_status=None)
    attempt("wrong_join_type", wrong_join, rehash_child=True)
    for bound in (0, -1, "2", 3):
        attempt(f"bad_repair_bound_{bound}",
                lambda p, bound=bound: child(p)["nodes"]["repair"].update(max_repairs=bound),
                rehash_child=True)
    attempt("mutable_active_closure", lambda p: p["root"]["nodes"]["invoke_child"].update(child_digest="latest"))
    attempt("unapproved_capability", lambda p: p["bindings"]["source"].update(identity="forged"))
    attempt("arbitrary_code", lambda p: child(p)["nodes"]["draft"].update(expression="__import__('os').system('id')"), rehash_child=True)
    try:
        publish(good, catalog, authority)
    except FileExistsError as error:
        results["immutable_overwrite"] = {"diagnostic": str(error), "rejected": True,
                                           "catalog_before": baseline,
                                           "catalog_after": catalog_inventory(),
                                           "catalog_unchanged": baseline == catalog_inventory()}
    else:
        raise AssertionError("immutable catalog overwrite was admitted")
    assert all(row["rejected"] and row["catalog_unchanged"] for row in results.values())
    freeze_after = freeze()
    assert freeze_before == freeze_after and freeze_after["verified"]
    output = {"status": "passed", "good_digest": good_digest,
              "freeze_before": freeze_before, "freeze_after": freeze_after,
              "catalog": str(catalog), "cases": results}
    (HERE / "negative-observed.json").write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"status": "passed", "cases": sorted(results)}))


if __name__ == "__main__":
    main()
