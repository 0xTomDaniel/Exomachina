import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { runInNewContext } from "node:vm";
import { makeCloudEvent, validateServerMessage } from "../contract.mjs";
import { createDashboardState, reduceDashboard, toFloorModel } from "../reducer.mjs";

// Live/Recorded floor flow on A2A semantics (operator request, 7 Oct 2026):
// Tasks at stations, artifacts on belts, observed dwell, evidence-labelled items.
const FACTORY = "factory-flow", T = s => new Date(Date.parse("2026-10-07T10:00:00.000Z") + s * 1000).toISOString();
const S1 = "1".repeat(64), S2 = "2".repeat(64), ID_F = "identity-findings", ID_R = "identity-risks";
const pins = { manifest_digest:"a".repeat(64), package_digest:"b".repeat(64), definition_digest:"c".repeat(64), interpreter_build:"build-1" };
const graph = { nodes:[{id:"abort",kind:"abort"},{id:"compose_report",kind:"synthesize"},{id:"director",kind:"director_wait"},{id:"done",kind:"complete"},{id:"gather",kind:"parallel"},{id:"independent_quality",kind:"quality"},{id:"join_evidence",kind:"join"},{id:"publish",kind:"release"},{id:"repair",kind:"repair"},{id:"route_verdict",kind:"route"}],
  edges:[{from:"compose_report",to:"independent_quality"},{from:"director",to:"abort"},{from:"gather",to:"join_evidence"},{from:"independent_quality",to:"route_verdict"},{from:"join_evidence",to:"compose_report"},{from:"publish",to:"done"},{from:"repair",to:"compose_report"},{from:"repair",to:"director"}] };
const bindings = [{name:"research_findings",role:"capability",identity:ID_F,contract_digest:"d".repeat(64)},{name:"research_risks",role:"capability",identity:ID_R,contract_digest:"e".repeat(64)},{name:"synthesizer",role:"capability",identity:"identity-synth",contract_digest:"f".repeat(64)}];
const attempt = (assignment_id, capability, provider_identity, started, ended, run_id = "run-new") => ({ assignment_id, attempt_id:"1", run_id, task_id:"task-new", capability, provider_identity, node:"gather", state:"completed", started_at:T(started), ended_at:T(ended) });
function run(id, { started = 0, status = { state:"working", phase:"started" }, assignments = [], artifacts = [], quality = [], delivery = [], runPins = pins } = {}) {
  return { id, task:{ id:`task-${id}`, context_id:`context-${id}` }, status, pinned:{ ...runPins }, started_at:T(started), graph, assignments:assignments.map(row => ({ id:row.assignment_id, attempts:[row] })), artifacts, quality, decisions:[], commands:[], delivery, incidents:[], admissions:[] };
}
function snapshot(runs, captured = 200) {
  return { schema_version:1, cursor:`c1.abcdefghijklmnop.${"0".repeat(40)}`, captured_at:T(captured), freshness:{ status:"fresh", observed_at:T(captured) }, state:{ factory:{ id:FACTORY, name:"Report factory", graph:{ nodes:[], edges:[] }, agent_bindings:bindings }, runs, active_publication:{ publication_version:"v1", ...pins }, capacity:null, commercial:{ usage:[], obligations:[], payments:[] } } };
}
let n = 0;
function frame(type, data, at) {
  n++;
  const event = makeCloudEvent({ id:`obs-${n.toString(16).padStart(64, "0")}`, factory_id:FACTORY, type:`com.exomachina.${type}.v1`, time:T(at), subject:"runs/run-new", data:{ schema_version:1, factory_id:FACTORY, run_id:"run-new", ...data } });
  return validateServerMessage({ op:"event", cursor:`c1.abcdefghijklmnop.${String(n + 1).padStart(40, "0")}`, event });
}
function streamedRun() {
  let state = createDashboardState(snapshot([run("run-new")], 0), { source:"live" });
  const task = { task_id:"task-new", context_id:"context-new" };
  const frames = [
    frame("assignment.state_changed", { ...task, assignment_id:"research_findings", attempt_id:"1", capability:"packet_findings@1", provider_identity:ID_F, node:"gather", state:"running", started_at:T(0.1) }, 0.1),
    frame("assignment.state_changed", { ...task, assignment_id:"research_risks", attempt_id:"1", capability:"packet_risks@1", provider_identity:ID_R, node:"gather", state:"running", started_at:T(0.1) }, 0.1),
    frame("assignment.state_changed", { ...task, assignment_id:"research_findings", attempt_id:"1", capability:"packet_findings@1", provider_identity:ID_F, node:"gather", state:"completed", ended_at:T(15.7) }, 15.7),
    frame("assignment.state_changed", { ...task, assignment_id:"research_risks", attempt_id:"1", capability:"packet_risks@1", provider_identity:ID_R, node:"gather", state:"completed", ended_at:T(60.6) }, 60.6),
    frame("run.state_changed", { state:"working", phase:"synthesize", node:"compose_report" }, 60.7),
    frame("artifact.revised", { ...task, artifact_revision:"r1", artifact_sha256:S1 }, 74),
    frame("run.state_changed", { state:"working", phase:"quality", node:"independent_quality" }, 74.02),
    frame("quality.verdict", { ...task, artifact_revision:"r1", artifact_sha256:S1, reviewer_identity:"identity-quality", accepted:false, finding_count:2 }, 79),
    frame("run.state_changed", { state:"working", phase:"synthesize", node:"compose_report" }, 79.02),
    frame("artifact.revised", { ...task, artifact_revision:"r2", artifact_sha256:S2 }, 95),
    frame("run.state_changed", { state:"working", phase:"quality", node:"independent_quality" }, 95.02),
    frame("quality.verdict", { ...task, artifact_revision:"r2", artifact_sha256:S2, reviewer_identity:"identity-quality", accepted:true, finding_count:0 }, 98),
    frame("run.state_changed", { state:"working", phase:"release", node:"publish" }, 98.02),
    frame("delivery.receipt", { ...task, receipt_id:"receipt-r2", artifact_revision:"r2", artifact_sha256:S2, destination_id:"fixture-receiver", delivered_at:T(98.5), outcome:"fixture-received" }, 98.5),
    frame("run.state_changed", { state:"completed", phase:"accepted", ended_at:T(98.6) }, 98.6),
  ];
  return { state, frames };
}
const byItem = (timeline, prefix) => timeline.filter(e => e.item?.startsWith(prefix));

test("streamed Live facts move Tasks through branch pods, outputs and evidenced report revisions to the terminal", () => {
  const { state:start, frames } = streamedRun();
  let state = start;
  for (const f of frames) state = reduceDashboard(state, f);
  const before = JSON.stringify(state.runs.get("run-new").graph);
  const floor = toFloorModel(state, { runId:"run-new" });
  assert.equal(JSON.stringify(state.runs.get("run-new").graph), before, "pinned graph facts are unchanged");
  assert.deepEqual(floor.steps.map(s => s.id), graph.nodes.map(s => s.id), "pods are not pinned steps");
  assert.deepEqual(floor.edges, graph.edges);
  assert.deepEqual(floor.branchPods.map(p => [p.id, p.name, p.join, p.presentation_only, p.basis]), [
    ["pod:gather:packet_findings@1", "research_findings · packet_findings@1", "join_evidence", true, "observed_assignments"],
    ["pod:gather:packet_risks@1", "research_risks · packet_risks@1", "join_evidence", true, "observed_assignments"]]);
  assert.match(floor.flowBasis, /Tasks occupy stations.*artifacts ride belts.*dwell is the observed gap.*inferred from pinned graph order/);
  const tl = floor.runs[0].timeline;
  // A2A Tasks occupy their pods for the exact observed interval.
  const podWork = tl.filter(e => e.type === "work" && e.step.startsWith("pod:"));
  assert.deepEqual(podWork.map(e => [e.step, e.t, +e.dur.toFixed(3)]), [["pod:gather:packet_findings@1", 0.1, 15.6], ["pod:gather:packet_risks@1", 0.1, 60.5]]);
  assert.ok(tl.some(e => e.type === "move" && e.from === "gather" && e.to === "pod:gather:packet_findings@1"));
  // Task outputs appear at completion and dwell on the belt until compose_report's observed start.
  const findings = byItem(tl, "out:research_findings");
  assert.equal(findings[0].type, "spawn"); assert.equal(findings[0].t, 15.7); assert.equal(findings[0].art, "task-output");
  assert.match(findings[0].evidence, /Task output · artifact not recorded.*inferred from pinned graph order/);
  assert.deepEqual(findings.filter(e => e.type === "move").map(e => `${e.from}>${e.to}`), ["pod:gather:packet_findings@1>join_evidence", "join_evidence>compose_report"]);
  assert.equal(findings.at(-1).type, "consume"); assert.equal(findings.at(-1).at, "compose_report"); assert.equal(+findings.at(-1).t.toFixed(3), 60.7);
  // A sub-second gap gets the minimum visible hop; observed times stay on the item.
  const risks = byItem(tl, "out:research_risks");
  assert.ok(risks.filter(e => e.type === "move").every(e => e.dur === 0.8));
  assert.ok(risks.at(-1).t > 60.7, "short observed gap is shown with a minimum hop");
  assert.match(risks[0].observed, /assignment ended 2026-10-07T10:01:00\.600Z/);
  // Station occupancy from observed run node transitions ends at the node's completion fact.
  const compose = tl.filter(e => e.type === "work" && e.step === "compose_report");
  assert.deepEqual(compose.map(e => [+e.t.toFixed(3), +e.dur.toFixed(3)]), [[60.7, 13.3], [79.02, 15.98]]);
  // R1: produced at compose_report, rejected at Quality, follows the repair loop and is consumed by R2's synthesis.
  const r1 = byItem(tl, "artifact:r1:");
  assert.equal(r1[0].label, "R1"); assert.equal(r1[0].sha, S1); assert.equal(r1[0].at, "compose_report"); assert.equal(r1[0].t, 74);
  assert.match(r1[0].evidence, /Report artifact r1 · sha256 1{64} · Quality rejected \(same sha256\)/);
  assert.deepEqual(r1.filter(e => e.type === "move").map(e => `${e.from}>${e.to}${e.undeclared ? "*" : ""}`), ["compose_report>independent_quality", "independent_quality>route_verdict", "route_verdict>repair*", "repair>compose_report"]);
  assert.ok(r1.some(e => e.type === "verdict" && e.verdict === "rejected" && e.step === "independent_quality" && e.t === 79));
  assert.ok(r1.some(e => e.type === "flag" && e.flag === "rejected"));
  assert.equal(r1.at(-1).type, "consume"); assert.equal(r1.at(-1).at, "compose_report");
  // R2: accepted, delivered with a verified receipt, then released at the declared terminal.
  const r2 = byItem(tl, "artifact:r2:");
  assert.match(r2[0].evidence, /Quality accepted \(same sha256\) · delivery receipt \(same sha256\)/);
  assert.deepEqual(r2.filter(e => e.type === "move").map(e => `${e.from}>${e.to}${e.undeclared ? "*" : ""}`), ["compose_report>independent_quality", "independent_quality>route_verdict", "route_verdict>publish*", "publish>done"]);
  assert.equal(r2.at(-1).type, "release"); assert.equal(r2.at(-1).at, "done");
  for (const e of tl.filter(e => e.item)) { const spawn = tl.find(x => x.type === "spawn" && x.item === e.item); assert.ok(spawn && spawn.t <= e.t, `${e.type} ${e.item} follows its spawn`); }
});

test("the live edge keeps updating as facts stream in", () => {
  const { state:start, frames } = streamedRun();
  let state = start;
  for (const f of frames.slice(0, 5)) state = reduceDashboard(state, f);
  let tl = toFloorModel(state, { runId:"run-new" }).runs[0].timeline;
  assert.ok(tl.some(e => e.type === "work" && e.step === "compose_report" && e.dur === undefined), "current station stays open");
  assert.equal(tl.some(e => e.item?.startsWith("artifact:")), false, "no report artifact before artifact.revised");
  state = reduceDashboard(state, frames[5]);
  tl = toFloorModel(state, { runId:"run-new" }).runs[0].timeline;
  assert.ok(tl.some(e => e.type === "spawn" && e.item.startsWith("artifact:r1:")));
});

test("a retained snapshot keeps unrecorded stations unlocated and never times untimed rows", () => {
  const conflicting = [{ receipt_id:"rid", artifact_revision:"r1", artifact_sha256:S1, destination_id:"a", delivered_at:T(106.84), outcome:"fixture-received" }, { receipt_id:"rid", artifact_revision:"r1", artifact_sha256:S1, destination_id:"b", delivered_at:T(106.85), outcome:"fixture-received" }];
  const row = run("run-old", { status:{ state:"completed", phase:"accepted", started_at:T(0), ended_at:T(106.86) }, assignments:[attempt("research_findings", "packet_findings@1", ID_F, 0.1, 15.7, "run-old"), attempt("research_risks", "packet_risks@1", ID_R, 0.1, 60.6, "run-old")],
    artifacts:[{ run_id:"run-old", artifact_revision:"r1", artifact_sha256:S1 }], quality:[{ run_id:"run-old", artifact_revision:"r1", artifact_sha256:S1, accepted:true, finding_count:0, reviewer_identity:"q" }], delivery:conflicting });
  const floor = toFloorModel(createDashboardState(snapshot([row]), { source:"live" }), { runId:"run-old" });
  const tl = floor.runs[0].timeline;
  assert.equal(tl.some(e => e.type === "work" && ["compose_report", "independent_quality", "publish"].includes(e.step)), false, "no station occupancy without an observed fact");
  const r1 = byItem(tl, "artifact:r1:");
  assert.equal(r1[0].at, "publish"); assert.ok([106.84, 106.85].includes(+r1[0].t.toFixed(2)), "located at the receipt time");
  assert.match(r1[0].evidence, /verdict time not recorded.*delivery unverified \(conflicting receipts\)/);
  assert.equal(r1.some(e => e.type === "release"), false, "unverified delivery never exits as delivered");
  const outputs = tl.filter(e => e.type === "consume" && e.item.startsWith("out:"));
  assert.ok(outputs.every(e => e.at === "compose_report" && +e.t.toFixed(2) === 106.86), "outputs wait on the belt until the observed run end");
});

test("All jobs scope keeps every run on the anchor's pinned graph and nothing else", () => {
  const other = { ...pins, definition_digest:"9".repeat(64) };
  const rows = [run("run-a", { started:0, status:{ state:"completed", ended_at:T(50) }, assignments:[attempt("research_findings", "packet_findings@1", ID_F, 1, 9, "run-a"), attempt("research_risks", "packet_risks@1", ID_R, 1, 12, "run-a")] }),
    run("run-new", { started:100 }), { ...run("run-parent", { started:99, runPins:other }), graph:{ nodes:[{id:"invoke_child",kind:"nested_factory"},{id:"done",kind:"complete"}], edges:[{from:"invoke_child",to:"done"}] } }];
  const state = createDashboardState(snapshot(rows), { source:"live" });
  const floor = toFloorModel(state, { graphOf:"run-new" });
  assert.equal(floor.runs[0].id, "dashboard-all-runs");
  assert.deepEqual(floor.runs.slice(1).map(r => r.id).sort(), ["run-a", "run-new"]);
  assert.deepEqual(floor.steps.map(s => s.id), graph.nodes.map(s => s.id));
  assert.equal(floor.branchPods.length, 2, "pods derive from any run on this graph");
  assert.deepEqual(floor.runs[0].events.filter(e => e.type === "job").map(e => e.job).sort(), ["run-a", "run-new"]);
  assert.throws(() => toFloorModel(state, { graphOf:"missing" }), /selected run is unavailable/);
});

test("renderer branch pods are presentation steps on the pinned fan-out route", async () => {
  const html = await readFile(new URL("../../../../docs/design/exomachina-floor.html", import.meta.url), "utf8");
  const start = html.indexOf("function withBranchPods("), end = html.indexOf("async function refreshLiveHistory(", start);
  const expand = runInNewContext(`(${html.slice(start, end).trim()})`);
  const { state:begin, frames } = streamedRun();
  let state = begin; for (const f of frames.slice(0, 4)) state = reduceDashboard(state, f);
  const model = toFloorModel(state, { runId:"run-new" });
  const f = expand(model);
  assert.deepEqual(f.pinnedSteps.map(s => s.id), graph.nodes.map(s => s.id));
  assert.deepEqual(f.pinnedEdges, graph.edges);
  const pod = f.steps.find(s => s.id === "pod:gather:packet_findings@1");
  assert.equal(pod.presentation_only, true); assert.equal(pod.derived, "observed_assignments"); assert.equal(pod.agent, pod.id); assert.equal(pod.name, "research_findings");
  assert.equal(f.edges.some(e => e.from === "gather" && e.to === "join_evidence"), false, "the pinned route is drawn through its pods");
  assert.ok(f.edges.some(e => e.from === "gather" && e.to === pod.id && e.pinned.to === "join_evidence"));
  assert.match(f.agents[pod.id].note, /derived from observed assignments.*presentation only/);
  const agentStation = runInNewContext(html.slice(html.indexOf("  const agentStation = g =>"), html.indexOf(";", html.indexOf("  const agentStation = g =>"))).trim().replace(/^const agentStation = /, "(") + ")");
  assert.equal(agentStation({ s:pod }), true, "focus mode treats a branch pod as an agent station");
  const plain = { ...model, branchPods:[] };
  assert.equal(expand(plain), plain, "no observed fan-out capabilities, no pods");
});

test("Live defaults to all jobs on the followed graph without binding a pending submission", async () => {
  const html = await readFile(new URL("../../../../docs/design/exomachina-floor.html", import.meta.url), "utf8");
  const render = html.slice(html.indexOf("async function renderLatestFloor("), html.indexOf("function renderFrame("));
  assert.match(render, /const scopeAll=currentSource==='live'&&store\.dashboardScope!=='run'/);
  assert.match(render, /if\(!store\.dashboardSubmittedTask\)for\(const \[id,run\] of state\.runs\)runs\.set\(id,run\)/);
  assert.match(render, /anchor=store\.dashboardSubmittedTask\?null:runId/);
  assert.match(render, /toFloorModel\(floorState,\{graphOf\}\)/);
  assert.match(render, /model\.followJob=anchor/);
  assert.match(render, /window\.EXO_DASHBOARD_FLOOR\.followJob\(model\.followJob\)/);
  const submit = html.slice(html.indexOf("async function submitBrief("), html.indexOf("function appendBriefForm("));
  assert.match(submit, /store\.dashboardScope='all';liveHistory=null;/);
  assert.match(html, /if\(currentSource==='live'&&!runId\)\{store\.dashboardScope='all';/);
  assert.match(html, /if \(j\.end != null && t >= j\.end\)\{ if \(t - j\.end < 2\.4 \|\| S\.f\.actual\) out\.push\(/);
});
