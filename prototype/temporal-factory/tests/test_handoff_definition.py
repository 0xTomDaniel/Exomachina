"""Definition output modes and edge kinds (A2A v1 mediation decisions 3 and 5)."""
from __future__ import annotations

import copy
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "services"))
sys.path.insert(0, str(ROOT / "tests"))

from authoring import authoring_vocabulary, materialize  # noqa: E402
from definition import declared_edges, digest, node_output, validate  # noqa: E402
from observation import _safe_nodes, _snapshot_graph, SourceContractError  # noqa: E402
from observation_source import _graph_nodes  # noqa: E402
from report_fixture import bindings, packet, template  # noqa: E402
from testbed import report_bindings  # noqa: E402


def shipped_bindings() -> dict:
    health = {name: {"identity": f"unit-{name}"} for name in
              ("research_findings", "research_risks", "synthesizer", "quality", "release")}
    return report_bindings(health, 45720)


def child_of(package: dict) -> dict:
    return next(iter(package["children"].values()))


class DefinitionOutputAndEdgeKindTests(unittest.TestCase):
    def package(self, tpl=None, approved=None) -> dict:
        approved = shipped_bindings() if approved is None else approved
        return materialize(tpl or template(), approved, evidence_packet=packet())

    def test_shipped_template_and_testbed_declare_side_effect_release_and_control_routes(self):
        approved = shipped_bindings()
        # Release is bound strict: its receipt is the A2A Task's result artifact.
        # It stays a side-effect node through its control-only outgoing edge.
        self.assertEqual(approved["release"]["output"], "artifacts")
        self.assertTrue(all("output" not in approved[name] for name in approved if name != "release"))
        package = self.package(approved=approved)
        self.assertEqual(validate(package, approved), digest(package))
        child = child_of(package)
        nodes = child["nodes"]
        self.assertEqual(node_output(nodes["publish"], package["bindings"]), "artifacts")
        self.assertEqual(node_output(nodes["draft"], package["bindings"]), "artifacts")
        self.assertEqual(node_output(nodes["gather"], package["bindings"]), "artifacts")
        self.assertIsNone(node_output(nodes["route_verdict"], package["bindings"]))
        self.assertEqual(declared_edges(nodes["route_verdict"]),
                         [("publish", "control"), ("repair", "control")])
        # Repair consumes the judged draft and Quality's findings hand-off over
        # declared material edges (decision 5, amended 8 Oct 2026): from the
        # draft's producer and from Quality into repair, then on to the
        # repair synthesis at the draft station.
        self.assertEqual(declared_edges(nodes["repair"]),
                         [("draft", "material"), ("director", "control")])
        self.assertEqual(declared_edges(nodes["draft"]),
                         [("independent_quality", "material"), ("repair", "material")])
        self.assertEqual(declared_edges(nodes["director"]), [("abort", "control")])
        self.assertEqual(declared_edges(nodes["publish"]), [("done", "control")])
        # Quality forwards the draft carrier to release on a material bypass belt.
        self.assertEqual(declared_edges(nodes["independent_quality"]),
                         [("route_verdict", "control"), ("publish", "material"),
                          ("repair", "material")])
        self.assertEqual(declared_edges(nodes["join_evidence"]), [("draft", "material")])

    def test_side_effect_node_with_outgoing_material_edge_is_rejected_at_publication(self):
        tpl = template()
        del tpl["child"]["nodes"]["publish"]["edges"]
        approved = shipped_bindings()
        with self.assertRaisesRegex(ValueError, "side-effect node may not have an outgoing material edge"):
            validate(self.package(tpl, approved), approved)
        tpl = template()
        tpl["child"]["nodes"]["publish"]["edges"] = {"done": "material"}
        with self.assertRaisesRegex(ValueError, "side-effect node"):
            validate(self.package(tpl, approved), approved)

    def test_edge_kind_and_output_declarations_are_bounded(self):
        approved = shipped_bindings()
        cases = []
        tpl = template(); tpl["child"]["nodes"]["independent_quality"]["edges"]["publish"] = "control"
        cases.append((tpl, "bypass edge must be material"))
        tpl = template(); tpl["child"]["nodes"]["draft"]["edges"] = {"nowhere": "material"}
        cases.append((tpl, "missing or self target"))
        tpl = template(); tpl["child"]["nodes"]["draft"]["edges"] = {"independent_quality": "belt"}
        cases.append((tpl, "material or control"))
        tpl = template(); tpl["child"]["nodes"]["draft"]["edges"] = {}
        cases.append((tpl, "bounded target map"))
        for tpl, message in cases:
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                validate(self.package(tpl, approved), approved)
        for name, output, error in (
                ("release", "none", "release node binding output must be strict artifacts"),
                ("release", "message", "release node binding output must be strict artifacts"),
                ("quality", "stream", "binding output"),
                # A result-less agent on a node with an outgoing material edge.
                ("synthesizer", "none", "draft: side-effect node")):
            changed = copy.deepcopy(approved)
            changed[name]["output"] = output
            with self.subTest(binding=name, output=output), self.assertRaisesRegex(ValueError, error):
                validate(self.package(approved=changed), changed)
        unbound = copy.deepcopy(approved)
        unbound["release"]["role"] = "capability"
        with self.assertRaisesRegex(ValueError, "release node must bind an approved release agent"):
            validate(self.package(approved=unbound), unbound)
        message = copy.deepcopy(approved)
        message["research_risks"]["output"] = "message"
        self.assertEqual(validate(self.package(approved=message), message),
                         digest(self.package(approved=message)))

    def test_publications_without_the_new_fields_keep_their_documents_and_digests(self):
        old = template()
        for document in (old["root"], old["child"]):
            for node in document["nodes"].values():
                node.pop("edges", None)
        legacy = bindings()
        package = materialize(old, legacy, evidence_packet=packet())
        self.assertEqual(validate(package, legacy), digest(package))
        self.assertNotIn("output", legacy["release"])
        # The earlier graph projection is unchanged for such pins: no edge kinds,
        # no outputs, and no route-case edges.
        projected = _graph_nodes(child_of(package), package["bindings"])
        self.assertTrue(all(set(node) <= {"id", "type", "next", "capability"} for node in projected))
        self.assertEqual(next(n for n in projected if n["id"] == "route_verdict")["next"], [])
        graph = _snapshot_graph(_safe_nodes(projected))
        self.assertTrue(all("kind" not in edge for edge in graph["edges"]))

    def test_pinned_graph_projection_carries_output_and_edge_kind(self):
        package = self.package()
        projected = _graph_nodes(child_of(package), package["bindings"])
        by_id = {node["id"]: node for node in projected}
        self.assertEqual(by_id["publish"]["output"], "artifacts")
        self.assertEqual(by_id["publish"]["control"], ["done"])
        self.assertEqual(by_id["route_verdict"]["next"], ["publish", "repair"])
        self.assertEqual(by_id["independent_quality"]["control"], ["route_verdict"])
        self.assertNotIn("output", by_id["join_evidence"])
        graph = _snapshot_graph(_safe_nodes(projected))
        kinds = {(edge["from"], edge["to"]): edge["kind"] for edge in graph["edges"]}
        self.assertEqual(kinds[("independent_quality", "publish")], "material")
        self.assertEqual(kinds[("draft", "independent_quality")], "material")
        self.assertEqual(kinds[("route_verdict", "repair")], "control")
        for edge in (("draft", "repair"), ("independent_quality", "repair"), ("repair", "draft")):
            self.assertEqual(kinds[edge], "material", edge)
        self.assertEqual(kinds[("repair", "director")], "control")
        self.assertEqual(kinds[("publish", "done")], "control")
        self.assertEqual({n["id"]: n.get("output") for n in graph["nodes"]}["publish"], "artifacts")
        broken = [dict(node) for node in projected]
        # The generic rule: a result-less (output none) node may not feed material.
        next(node for node in broken if node["id"] == "publish").update(output="none", control=[])
        with self.assertRaises(SourceContractError):
            _snapshot_graph(_safe_nodes(broken))
        with self.assertRaises(SourceContractError):
            _safe_nodes([{"id": "a", "type": "complete", "next": [], "control": ["b"]}])

    def test_authoring_vocabulary_describes_outputs_and_edge_kinds(self):
        vocabulary = authoring_vocabulary(shipped_bindings())
        self.assertEqual(vocabulary["bindings"]["release"]["output"], "artifacts")
        self.assertEqual(vocabulary["node_outputs"], ["artifacts", "message", "none"])
        self.assertEqual(vocabulary["edge_kinds"], ["material", "control"])


if __name__ == "__main__":
    unittest.main()
