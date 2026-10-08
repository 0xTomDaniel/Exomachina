import assert from "node:assert/strict";
import test from "node:test";
import { communicationPresentation, preferredFloorRunId } from "../presentation.mjs";

const graph = { nodes: [
  { id: "intake", kind: "intake", name: "Receive work" },
  { id: "review", kind: "director_wait", name: "Director review" },
  { id: "human-review", kind: "wait", name: "Human review" },
  { id: "quality", kind: "quality", name: "Quality" },
] };

test("empty current pair exposes navigation without inventing a Director control or inbox item", () => {
  const result = communicationPresentation({
    source: "live",
    freshness: { status: "fresh" },
    graph: { nodes: graph.nodes.filter(node => node.id === "intake" || node.id === "quality") },
    waits: [],
    incidents: [],
    declaredRoutes: { entryTargets: ["intake"] },
  });

  assert.deepEqual(result.director.targets, ["intake"], "entry navigation target is included for the renderer without becoming a control route");
  assert.deepEqual(result.director.routes, [{ node_id: "intake", kind: "presentation_navigation", label: "Submission / Decisions view" }]);
  assert.equal(result.director.route, "Decisions");
  assert.match(result.director.reason, /Presentation navigation only/);
  assert.deepEqual(result.humanInbox, {
    state: "empty", items: [], targets: [], route: "Decisions",
    reason: "No human inbox facts are currently observed.",
  });
  assert.deepEqual(result.incidentChannel, {
    state: "empty", items: [], route: "Decisions",
    reason: "No incident channel facts are currently observed.",
  });
});

test("Director targets use exact director_wait nodes and human items retain exact Task and node provenance", () => {
  const result = communicationPresentation({
    source: "live",
    freshness: { status: "fresh" },
    graph,
    waits: [
      { run_id: "run-d", wait_role: "director", node: "review", permitted_actions: ["abort"] },
      {
        run_id: "run-h", task: { id: "task-h", context_id: "context-h" }, wait_role: "human",
        wait_actor_identity: "operator-1", node: "human-review", wait_started_at: "2026-10-04T12:00:00.000Z",
        wait_deadline: "2026-10-04T12:20:00.000Z", permitted_actions: ["abort"],
      },
      { run_id: "run-old", task: { id: "task-old", context_id: "context-old" }, wait_role: "human", node: "missing-node" },
    ],
    incidents: [{ incident_id: "inc-old", run_id: "prior-run", task_id: "task-prior", context_id: "context-prior", kind: "observation_unavailable", state: "escalated", owner_identity: "operator-2", node: "missing-node" }],
  });

  assert.deepEqual(result.director.targets, ["review"]);
  assert.deepEqual(result.director.routes, [{ node_id: "review", kind: "declared_control", label: "Director wait · Director review" }]);
  assert.equal(result.humanInbox.state, "pending");
  assert.deepEqual(result.humanInbox.targets, ["human-review"], "a wait for a node absent from the displayed graph has unknown origin and is not a layout target");
  assert.deepEqual(result.humanInbox.items[0], {
    run_id: "run-h", task_id: "task-h", context_id: "context-h", node_id: "human-review",
    origin: { node_id: "human-review", label: "Human review" }, actor_identity: "operator-1",
    wait_started_at: "2026-10-04T12:00:00.000Z", wait_deadline: "2026-10-04T12:20:00.000Z", permitted_actions: ["abort"],
  });
  assert.equal(result.humanInbox.items[1].origin, "unknown");
  assert.equal(result.incidentChannel.state, "pending");
  assert.deepEqual(result.incidentChannel.items[0], {
    incident_id: "inc-old", run_id: "prior-run", task_id: "task-prior", context_id: "context-prior", kind: "observation_unavailable", state: "escalated",
    owner_identity: "operator-2", node_id: "missing-node", origin: "unknown",
  });
});

test("Demo route declarations are finite and graph-bound; Live ignores Demo-only controls", () => {
  const declaredRoutes = { entryTargets: ["intake", "missing"], directorTargets: ["quality", "missing"], humanTargets: ["human-review"] };
  const demo = communicationPresentation({ source: "demo", freshness: { status: "fresh" }, graph, waits: [], incidents: [], declaredRoutes });
  assert.deepEqual(demo.director.targets, ["review", "quality", "intake"]);
  assert.deepEqual(demo.director.routes, [
    { node_id: "review", kind: "declared_control", label: "Director wait · Director review" },
    { node_id: "quality", kind: "declared_control", label: "Demo route · Quality" },
    { node_id: "intake", kind: "presentation_navigation", label: "Submission / Decisions view" },
  ]);
  assert.deepEqual(demo.humanInbox.targets, ["human-review"]);
  assert.equal(demo.humanInbox.state, "empty");
  assert.match(demo.humanInbox.reason, /isolated fixture/);

  const live = communicationPresentation({ source: "live", freshness: "fresh", graph, waits: [], incidents: [], declaredRoutes });
  assert.deepEqual(live.director.targets, ["review", "intake"]);
  assert.deepEqual(live.humanInbox.targets, []);
});

test("stale Live freshness marks inbox and incident channels unavailable while preserving observed run attribution", () => {
  const result = communicationPresentation({
    source: "live", freshness: { status: "disconnected" }, graph,
    waits: [{ run_id: "run-last", wait_role: "human", node: "human-review" }],
    incidents: [{ incident_id: "inc-last", run_id: "prior-run", kind: "worker_fault", state: "claimed", owner_identity: "operator-3" }],
    declaredRoutes: { entryTargets: ["intake"] },
  });
  assert.equal(result.humanInbox.state, "unavailable");
  assert.equal(result.humanInbox.items.length, 1, "last observed wait is retained but not presented as current");
  assert.match(result.humanInbox.reason, /Live Observation is disconnected/);
  assert.equal(result.incidentChannel.state, "unavailable");
  assert.equal(result.incidentChannel.items[0].run_id, "prior-run");
  assert.match(result.incidentChannel.reason, /Live Observation is disconnected/);
  assert.match(result.director.reason, /control availability is not current/);
});

test("fresh selected run does not imply complete factory inbox or incident coverage",()=>{
 const result=communicationPresentation({source:"live",freshness:{status:"fresh",scope:"run",factory_status:"disconnected"},graph,waits:[],incidents:[],declaredRoutes:{entryTargets:["intake"]}});
 assert.equal(result.humanInbox.state,"unavailable");assert.equal(result.incidentChannel.state,"unavailable");
 assert.match(result.humanInbox.reason,/Only the selected run/);
});

test('a declared Demo intake route replaces duplicate entry navigation',()=>{
 const result=communicationPresentation({source:'demo',freshness:{status:'fresh'},graph,waits:[],incidents:[],declaredRoutes:{entryTargets:['intake'],directorTargets:['intake']}});
 const routes=result.director.routes.filter(route=>route.node_id==='intake');
 assert.equal(routes.length,1);assert.equal(routes[0].kind,'declared_control');
 assert.equal(result.director.targets.filter(id=>id==='intake').length,1);
});

test('initial Live selection prefers recent readable detailed graph and preserves facts',()=>{
 const node={id:'work',kind:'assign'};
 const runs=[{run_id:'older',graph:{nodes:[node,node]},state:{started_at:'2026-10-03T00:00:00Z'}},{run_id:'latest',graph:{nodes:[node,node]},state:{started_at:'2026-10-04T00:00:00Z'}},{run_id:'parent',graph:{nodes:[node]},state:{started_at:'2026-10-04T00:00:00Z'}}];
 const before=JSON.stringify(runs);
 assert.equal(preferredFloorRunId(runs),'latest');
 assert.equal(preferredFloorRunId(runs,['latest']),'older');
 assert.equal(preferredFloorRunId(runs,['latest','older','parent']),null);
 assert.equal(JSON.stringify(runs),before);
 assert.equal(preferredFloorRunId([{run_id:'missing-graph'}]),null);
});
