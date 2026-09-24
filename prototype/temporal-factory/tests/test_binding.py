from __future__ import annotations

import copy
import json
import sys
import tempfile
import unittest
from contextlib import nullcontext
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from binding import DEPLOYMENT, PublicationStore, build_id_for, digest, make_manifest, may_retire, verify_closure
from definition import digest as definition_digest


def materialized_v1(bindings):
    source = json.loads((ROOT / "definitions" / "v1-template.json").read_text())
    child = source["child"]
    child_digest = definition_digest(child)
    for node in source["root"]["nodes"].values():
        if node["type"] == "nested_factory" and node["child_digest"] == "@child":
            node["child_digest"] = child_digest
    return {"schema": source["schema"], "root": source["root"],
            "children": {child_digest: child}, "bindings": bindings,
            "run_inputs": source["run_inputs"]}


class BindingTests(unittest.TestCase):
    def setUp(self):
        bindings = {name: {"role": role, "url": f"http://127.0.0.1:{42161+i}",
                           "identity": name, "approved": True}
                    for i, (name, role) in enumerate((("source_alpha", "capability"),
                        ("source_beta", "capability"), ("counter_alpha", "capability"),
                        ("counter_beta", "capability"), ("quality", "quality"),
                        ("release", "release")))}
        self.package = materialized_v1(bindings)
        self.contracts = {name: {"revision": 1, "service": name} for name in bindings}
        self.policy = {"revision": 1, "authority": "quality"}
        self.build = build_id_for("a" * 64)
        manifest = make_manifest(self.package, self.contracts, self.policy,
            build_id=self.build, code_digest="a" * 64, python="3.12.9", temporalio="1.33.0")
        self.closure = {"manifest": manifest, "manifest_digest": digest(manifest),
                        "contracts": self.contracts, "quality_policy": self.policy}

    def verify(self, closure=None, package=None, build=None, definition=None):
        return verify_closure(closure or self.closure, package or self.package,
            build_id=build or self.build, definition_digest=definition or definition_digest(self.package["root"]))

    def test_root_and_child_verify(self):
        self.assertEqual(self.verify(), self.closure["manifest_digest"])
        self.assertEqual(self.verify(definition=next(iter(self.package["children"]))),
                         self.closure["manifest_digest"])

    def test_mutations_fail_closed(self):
        cases = []
        changed = copy.deepcopy(self.closure)
        changed["contracts"]["source_alpha"]["revision"] = 2
        cases.append((changed, self.package, self.build))
        changed = copy.deepcopy(self.closure)
        changed["quality_policy"]["revision"] = 2
        cases.append((changed, self.package, self.build))
        changed = copy.deepcopy(self.closure)
        changed["manifest"]["interpreter"]["source_digest"] = "b" * 64
        cases.append((changed, self.package, self.build))
        changed_package = copy.deepcopy(self.package)
        changed_package["bindings"]["source_alpha"]["identity"] = "other"
        cases.append((self.closure, changed_package, self.build))
        cases.append((self.closure, self.package, "b-other"))
        for closure, package, build in cases:
            with self.subTest(build=build, closure=closure is self.closure):
                with self.assertRaises(ValueError):
                    self.verify(closure, package, build)

    def test_input_document_must_match_package_and_digest(self):
        altered = copy.deepcopy(self.package["root"])
        altered["revision"] = "changed"
        with self.assertRaisesRegex(ValueError, "document differs"):
            verify_closure(self.closure, self.package, build_id=self.build,
                definition_digest=definition_digest(self.package["root"]), document=altered)

    def test_publication_pointer_and_conflict(self):
        with nullcontext(tempfile.mkdtemp(prefix="exo-proto-interpreter-", dir="/tmp")) as directory:
            store = PublicationStore(Path(directory))
            key = store.publish(self.package, self.closure, label="v1")
            self.assertEqual(key, self.closure["manifest_digest"])
            with self.assertRaises(ValueError):
                store.activate(key, registered_version=f"{DEPLOYMENT}.b-other",
                    registered_source_digest="a" * 64)
            with self.assertRaises(ValueError):
                store.activate(key, registered_version=f"{DEPLOYMENT}.{self.build}",
                    registered_source_digest="b" * 64)
            store.activate(key, registered_version=f"{DEPLOYMENT}.{self.build}",
                registered_source_digest="a" * 64)
            self.assertEqual(store.active()["build_id"], self.build)
            self.assertEqual(store.get(key)["label"], "v1")
            self.assertEqual(len(store.list()), 1)
            changed = copy.deepcopy(self.closure)
            changed["quality_policy"]["revision"] = 2
            with self.assertRaises(ValueError):
                store.publish(self.package, changed, label="v1")
            another_source = copy.deepcopy(self.closure)
            another_source["manifest"]["interpreter"]["source_digest"] = "b" * 64
            another_source["manifest_digest"] = digest(another_source["manifest"])
            with self.assertRaisesRegex(ValueError, "wrong interpreter build"):
                store.publish(self.package, another_source, label="v2")

    def test_retirement_requires_drained_and_no_open_run(self):
        view = {"versionSummaries": [{"BuildID": "b1", "drainageStatus": "drained"}]}
        self.assertFalse(may_retire("b1", "b1", [], view))
        self.assertFalse(may_retire("b1", "b2", ["b1"], view))
        self.assertFalse(may_retire("b1", "b2", [], {"versionSummaries": [
            {"BuildID": "b1", "drainageStatus": "draining"}]}))
        self.assertTrue(may_retire("b1", "b2", [], view))


if __name__ == "__main__":
    unittest.main()
