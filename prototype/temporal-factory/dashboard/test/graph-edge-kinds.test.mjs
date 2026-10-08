import test from "node:test";
import assert from "node:assert/strict";
import { DashboardContractError, makeCloudEvent, validateServerMessage } from "../contract.mjs";
import { createDashboardState, reduceDashboard } from "../reducer.mjs";

// The event-stream graph_nodes form announces edge kinds with `control`, the
// subset of `next` that only sequences work (runtime phase, 7 Oct 2026). The
// reducer's graph must equal the snapshot graph the runtime projects
// (src/observation.py _snapshot_graph), key for key.
const FACTORY = "factory-edge-kinds", T = "2026-10-07T12:00:00.000Z", hex = c => c.repeat(64);
const pins = { manifest_digest:hex("7"), package_digest:hex("8"), definition_digest:hex("9"), interpreter_build:"build-1" };
const graphNodes = [
  { id:"draft", type:"synthesize", next:["independent_quality"], control:[], output:"artifacts" },
  { id:"independent_quality", type:"quality", next:["route_verdict", "publish"], control:["route_verdict"], output:"artifacts" },
  { id:"route_verdict", type:"route", next:["publish", "repair"], control:["publish", "repair"] },
  { id:"repair", type:"repair", next:["draft"], control:["draft"] },
  { id:"publish", type:"release", next:["done"], control:["done"], output:"none" },
  { id:"done", type:"complete", next:[], control:[] },
];
// Exactly what src/observation.py _snapshot_graph emits for the nodes above.
const snapshotGraph = { nodes:graphNodes.map(n => ({ id:n.id, kind:n.type, ...(n.output ? { output:n.output } : {}) })),
  edges:graphNodes.flatMap(n => n.next.map(to => ({ from:n.id, to, kind:n.control.includes(to) ? "control" : "material" }))) };
function snapshot(graph) {
  const run = { id:"run-e", task:{ id:"task-e", context_id:"context-e" }, status:{ state:"working" }, pinned:{ ...pins }, started_at:T, ...(graph ? { graph } : {}), assignments:[], artifacts:[], quality:[], decisions:[], commands:[], delivery:[], incidents:[], admissions:[] };
  return { schema_version:1, cursor:`c1.abcdefghijklmnop.${"0".repeat(40)}`, captured_at:T, freshness:{ status:"fresh", observed_at:T }, state:{ factory:{ id:FACTORY, name:"Edge kinds", graph:{ nodes:[], edges:[] } }, runs:[run], active_publication:null, capacity:null, commercial:{ usage:[], obligations:[], payments:[] } } };
}
const created = nodes => validateServerMessage({ op:"event", cursor:`c1.abcdefghijklmnop.${"1".padStart(40, "0")}`, event:makeCloudEvent({ id:`obs-${"1".padStart(64, "0")}`, factory_id:FACTORY, type:"com.exomachina.run.created.v1", time:T, subject:"runs/run-e",
  data:{ schema_version:1, factory_id:FACTORY, run_id:"run-e", task_id:"task-e", context_id:"context-e", ...pins, state:"working", started_at:T, graph_nodes:nodes } }) });

test("event-stream graph_nodes control lists give every edge its kind, equal to the runtime snapshot graph", () => {
  const state = reduceDashboard(createDashboardState(snapshot(snapshotGraph)), created(graphNodes));
  const graph = state.runs.get("run-e").graph;
  assert.deepEqual(graph, snapshotGraph, "no pinned-graph-changed conflict with the runtime snapshot");
  const kind = (from, to) => graph.edges.find(e => e.from === from && e.to === to).kind;
  assert.equal(kind("independent_quality", "publish"), "material", "material bypass belt to release");
  assert.equal(kind("route_verdict", "repair"), "control");
  assert.equal(kind("publish", "done"), "control", "the side-effect node has no outgoing belt");
  const fresh = reduceDashboard(createDashboardState(snapshot(null)), created(graphNodes)).runs.get("run-e").graph;
  assert.deepEqual(fresh, snapshotGraph);
});

test("graph_nodes without control keep the earlier kind-free edges; malformed control is rejected", () => {
  const legacy = graphNodes.map(({ control, output, ...n }) => n);
  const graph = reduceDashboard(createDashboardState(snapshot(null)), created(legacy)).runs.get("run-e").graph;
  assert.ok(graph.edges.every(e => !("kind" in e)));
  for (const control of [["nowhere"], ["done", "done"], "done"]) {
    const nodes = graphNodes.map(n => n.id === "publish" ? { ...n, control } : n);
    assert.throws(() => created(nodes), DashboardContractError);
  }
});
