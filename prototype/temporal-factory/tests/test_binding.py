from __future__ import annotations

import copy
import tempfile
import unittest
from pathlib import Path

from author import materialize, template
from binding import DEPLOYMENT, PublicationStore, digest, make_manifest, may_retire, verify_closure
from definition import digest as definition_digest


class BindingTests(unittest.TestCase):
    def setUp(self):
        bindings = {name: {"role": role, "url": f"http://127.0.0.1:{42161+i}",
                           "identity": name, "approved": True}
                    for i, (name, role) in enumerate((("source", "capability"),
                        ("counter", "capability"), ("quality", "quality"),
                        ("release", "release")))}
        self.package = materialize(template("visible-exhausted.json"), bindings)
        self.contracts = {name: {"revision": 1, "service": name} for name in bindings}
        self.policy = {"revision": 1, "authority": "quality"}
        manifest = make_manifest(self.package, self.contracts, self.policy,
            build_id="b1", code_digest="a" * 64, python="3.12.9", temporalio="1.33.0")
        self.closure = {"manifest": manifest, "manifest_digest": digest(manifest),
                        "contracts": self.contracts, "quality_policy": self.policy}

    def verify(self, closure=None, package=None, build="b1", definition=None):
        return verify_closure(closure or self.closure, package or self.package,
            build_id=build, definition_digest=definition or definition_digest(self.package["root"]))

    def test_root_and_child_verify(self):
        self.assertEqual(self.verify(), self.closure["manifest_digest"])
        self.assertEqual(self.verify(definition=next(iter(self.package["children"]))),
                         self.closure["manifest_digest"])

    def test_mutations_fail_closed(self):
        cases = []
        changed = copy.deepcopy(self.closure)
        changed["contracts"]["source"]["revision"] = 2
        cases.append((changed, self.package, "b1"))
        changed = copy.deepcopy(self.closure)
        changed["quality_policy"]["revision"] = 2
        cases.append((changed, self.package, "b1"))
        changed = copy.deepcopy(self.closure)
        changed["manifest"]["interpreter"]["source_digest"] = "b" * 64
        cases.append((changed, self.package, "b1"))
        changed_package = copy.deepcopy(self.package)
        changed_package["bindings"]["source"]["identity"] = "other"
        cases.append((self.closure, changed_package, "b1"))
        cases.append((self.closure, self.package, "b2"))
        for closure, package, build in cases:
            with self.subTest(build=build, closure=closure is self.closure):
                with self.assertRaises(ValueError):
                    self.verify(closure, package, build)

    def test_input_document_must_match_package_and_digest(self):
        altered = copy.deepcopy(self.package["root"])
        altered["revision"] = "changed"
        with self.assertRaisesRegex(ValueError, "document differs"):
            verify_closure(self.closure, self.package, build_id="b1",
                definition_digest=definition_digest(self.package["root"]), document=altered)

    def test_publication_pointer_and_conflict(self):
        with tempfile.TemporaryDirectory(prefix="exo-tq-version-") as directory:
            store = PublicationStore(Path(directory))
            key = store.publish(self.package, self.closure)
            self.assertEqual(key, self.closure["manifest_digest"])
            with self.assertRaises(ValueError):
                store.activate(key, registered_version=f"{DEPLOYMENT}.b2",
                    registered_source_digest="a" * 64)
            with self.assertRaises(ValueError):
                store.activate(key, registered_version=f"{DEPLOYMENT}.b1",
                    registered_source_digest="b" * 64)
            store.activate(key, registered_version=f"{DEPLOYMENT}.b1",
                registered_source_digest="a" * 64)
            self.assertEqual(store.active()["build_id"], "b1")
            changed = copy.deepcopy(self.closure)
            changed["quality_policy"]["revision"] = 2
            with self.assertRaises(ValueError):
                store.publish(self.package, changed)
            another_source = copy.deepcopy(self.closure)
            another_source["manifest"]["interpreter"]["source_digest"] = "b" * 64
            another_source["manifest_digest"] = digest(another_source["manifest"])
            with self.assertRaisesRegex(ValueError, "already bound"):
                store.publish(self.package, another_source)

    def test_retirement_requires_drained_and_no_open_run(self):
        view = {"versionSummaries": [{"BuildID": "b1", "drainageStatus": "drained"}]}
        self.assertFalse(may_retire("b1", "b1", [], view))
        self.assertFalse(may_retire("b1", "b2", ["b1"], view))
        self.assertFalse(may_retire("b1", "b2", [], {"versionSummaries": [
            {"BuildID": "b1", "drainageStatus": "draining"}]}))
        self.assertTrue(may_retire("b1", "b2", [], view))


if __name__ == "__main__":
    unittest.main()
