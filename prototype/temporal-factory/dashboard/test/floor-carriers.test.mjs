import test from "node:test";
import assert from "node:assert/strict";
import { makeCloudEvent, validateServerMessage } from "../contract.mjs";
import { CARRIER_ITEM_FIELDS, createDashboardState, reduceDashboard, toFloorModel } from "../reducer.mjs";

// Hand-off carriers over synthetic, test-only hand-off fact streams (A2A v1 mediation
// decisions 3-6). Runtime emission of these facts is a later phase.
const FACTORY = "factory-carriers", T = s => new Date(Date.parse("2026-10-07T12:00:00.000Z") + s * 1000).toISOString();
const hex = c => c.repeat(64), S1 = hex("1"), S2 = hex("2");
const DF0 = hex("a"), DF1 = hex("b"), DR0 = hex("c"), DD1 = hex("d"), DD2 = hex("e"), DD2U = hex("f");
const ID_F = "identity-findings", ID_R = "identity-risks";
const pins = { manifest_digest:hex("7"), package_digest:hex("8"), definition_digest:hex("9"), interpreter_build:"build-1" };
// Material belts carry hand-offs; route transitions and side-effect exits are control edges.
const graph = { nodes:[{id:"gather",kind:"parallel"},{id:"join_evidence",kind:"join"},{id:"draft",kind:"synthesize"},{id:"quality",kind:"quality"},{id:"route_verdict",kind:"route"},{id:"repair",kind:"repair"},{id:"publish",kind:"release",output:"none"},{id:"done",kind:"complete"},{id:"abort",kind:"abort"}],
  edges:[{from:"gather",to:"join_evidence"},{from:"join_evidence",to:"draft"},{from:"draft",to:"quality",kind:"material"},{from:"quality",to:"route_verdict",kind:"control"},{from:"quality",to:"publish"},
    {from:"route_verdict",to:"publish",kind:"control"},{from:"route_verdict",to:"repair",kind:"control"},{from:"repair",to:"draft",kind:"control"},{from:"publish",to:"done",kind:"control"}] };
const bindings = [{name:"research_findings",role:"capability",identity:ID_F,contract_digest:hex("3")},{name:"research_risks",role:"capability",identity:ID_R,contract_digest:hex("4")}];
function snapshot(g = graph) {
  const run = { id:"run-c", task:{ id:"task-c", context_id:"context-c" }, status:{ state:"working", phase:"started" }, pinned:{ ...pins }, started_at:T(0), graph:g, assignments:[], artifacts:[], quality:[], decisions:[], commands:[], delivery:[], incidents:[], admissions:[] };
  return { schema_version:1, cursor:`c1.abcdefghijklmnop.${"0".repeat(40)}`, captured_at:T(0), freshness:{ status:"fresh", observed_at:T(0) }, state:{ factory:{ id:FACTORY, name:"Carrier factory", graph:{ nodes:[], edges:[] }, agent_bindings:bindings }, runs:[run], active_publication:{ publication_version:"v1", ...pins }, capacity:null, commercial:{ usage:[], obligations:[], payments:[] } } };
}
let n = 0;
function frame(type, data, at) {
  n++;
  const event = makeCloudEvent({ id:`obs-${n.toString(16).padStart(64, "0")}`, factory_id:FACTORY, type:`com.exomachina.${type}.v1`, time:T(at), subject:"runs/run-c", data:{ schema_version:1, factory_id:FACTORY, run_id:"run-c", ...data } });
  return validateServerMessage({ op:"event", cursor:`c1.abcdefghijklmnop.${String(n + 1).padStart(40, "0")}`, event });
}
const task = { task_id:"task-c", context_id:"context-c" };
const assign = (id, capability, identity, node, state, extra, at) => frame("assignment.state_changed", { ...task, assignment_id:id, attempt_id:"1", capability, provider_identity:identity, node, state, ...extra }, at);
const item = (index, kinds, digest, ready, extra = {}) => ({ item_index:index, source:"artifact", part_kinds:kinds, media_type:kinds[0] === "url" ? null : kinds[0] === "data" ? "application/json" : kinds[0] === "raw" ? "application/pdf" : "text/markdown", byte_length:kinds[0] === "url" ? null : 1000 + index, ready_at:T(ready), digest, ...extra });
const produced = (assignment, node, handoff, revision, at, items) => frame("handoff.produced", { assignment_id:assignment, attempt_id:"1", node, handoff_id:handoff, handoff_revision:revision, produced_at:T(at), items }, at);
const consumed = (assignment, node, at, inputs) => frame("handoff.consumed", { assignment_id:assignment, attempt_id:"1", node, consumed_at:T(at), inputs:inputs.map(([handoff_id, ...item_digests]) => ({ handoff_id, item_digests })) }, at);
const ready = (assignment, node, handoff, index, kinds, at) => frame("handoff.item_ready", { assignment_id:assignment, attempt_id:"1", node, handoff_id:handoff, item_index:index, part_kinds:kinds, media_type:"text/plain", ready_at:T(at) }, at);
const runState = (extra, at) => frame("run.state_changed", extra, at);
const build = (frames, g) => frames.reduce((s, f) => reduceDashboard(s, f), createDashboardState(snapshot(g), { source:"live" }));
const timeline = state => toFloorModel(state, { runId:"run-c" }).runs[0].timeline;
const of = (tl, item) => tl.filter(e => e.item === item);

// The full report route: streamed fan-out, fan-in merge, R1 rejected, R2 accepted and delivered.
function reportRun({ end = true } = {}) {
  return [
    assign("research_findings", "packet_findings@1", ID_F, "gather", "running", { started_at:T(0.1) }, 0.1),
    assign("research_risks", "packet_risks@1", ID_R, "gather", "running", { started_at:T(0.1) }, 0.1),
    ready("research_findings", "gather", "h-findings", 0, ["text"], 5),
    ready("research_findings", "gather", "h-findings", 1, ["data"], 8),
    produced("research_findings", "gather", "h-findings", 1, 10, [item(0, ["text"], DF0, 5), item(1, ["data"], DF1, 8)]),
    assign("research_findings", "packet_findings@1", ID_F, "gather", "completed", { ended_at:T(10) }, 10),
    produced("research_risks", "gather", "h-risks", 1, 12, [item(0, ["raw"], DR0, 12)]),
    assign("research_risks", "packet_risks@1", ID_R, "gather", "completed", { ended_at:T(12) }, 12),
    runState({ state:"working", phase:"synthesize", node:"draft" }, 13),
    consumed("synth", "draft", 13, [["h-findings", DF0, DF1], ["h-risks", DR0]]),
    produced("synth", "draft", "h-draft", 1, 20, [item(0, ["text"], DD1, 20, { artifact_revision:"r1", artifact_sha256:S1 })]),
    frame("artifact.revised", { ...task, artifact_revision:"r1", artifact_sha256:S1 }, 20),
    runState({ state:"working", phase:"quality", node:"quality" }, 20.1),
    consumed("quality", "quality", 21, [["h-draft", DD1]]),
    frame("quality.verdict", { ...task, artifact_revision:"r1", artifact_sha256:S1, reviewer_identity:"identity-quality", accepted:false, finding_count:2 }, 25),
    runState({ state:"working", phase:"synthesize", node:"draft" }, 25.1),
    consumed("synth", "draft", 25.2, [["h-draft", DD1]]),
    produced("synth", "draft", "h-draft", 2, 30, [item(0, ["text"], DD2, 30, { artifact_revision:"r2", artifact_sha256:S2 }), item(1, ["url"], DD2U, 29)]),
    frame("artifact.revised", { ...task, artifact_revision:"r2", artifact_sha256:S2 }, 30),
    runState({ state:"working", phase:"quality", node:"quality" }, 30.1),
    consumed("quality", "quality", 31, [["h-draft", DD2, DD2U]]),
    frame("quality.verdict", { ...task, artifact_revision:"r2", artifact_sha256:S2, reviewer_identity:"identity-quality", accepted:true, finding_count:0 }, 33),
    runState({ state:"working", phase:"release", node:"publish" }, 33.1),
    consumed("release", "publish", 34, [["h-draft", DD2, DD2U]]),
    // A side-effect node never produces a carrier, even if a record names it.
    produced("release", "publish", "h-publish", 1, 34.4, [item(0, ["text"], hex("9"), 34.4)]),
    frame("delivery.receipt", { ...task, receipt_id:"receipt-r2", artifact_revision:"r2", artifact_sha256:S2, destination_id:"fixture-receiver", delivered_at:T(34.5), outcome:"fixture-received" }, 34.5),
    ...(end ? [runState({ state:"completed", phase:"accepted", ended_at:T(35) }, 35)] : []),
  ];
}

test("linear flow: a recorded carrier leaves at production, rides material belts, and is released by the side-effect node", () => {
  const tl = timeline(build(reportRun()));
  const r2 = of(tl, "carrier:h-draft:2");
  assert.equal(r2[0].type, "spawn"); assert.equal(r2[0].at, "draft"); assert.equal(r2[0].art, "carrier:draft"); assert.equal(r2[0].label, "R2");
  assert.deepEqual(r2.filter(e => e.type === "move").map(e => `${e.from}>${e.to}`), ["draft>quality", "quality>publish"], "material belts only");
  assert.equal(r2.find(e => e.type === "move").t, 30, "leaves when the hand-off is produced");
  const release = r2.find(e => e.type === "release");
  assert.equal(release.at, "publish"); assert.equal(release.t, 34.5, "exits at the verified receipt");
  assert.equal(tl.some(e => e.type === "move" && ["route_verdict", "repair", "done"].includes(e.to) && e.item?.startsWith("carrier:")), false, "control edges carry no carriers");
  assert.equal(tl.some(e => e.item?.startsWith("out:") || e.item?.startsWith("artifact:")), false, "recorded hand-offs replace inferred items");
  assert.equal(tl.some(e => e.item === "carrier:h-publish:1"), false, "an output:none node produces no carrier");
  const model = toFloorModel(build(reportRun()), { runId:"run-c" });
  assert.equal(model.artifacts["carrier:draft"].shape, "square"); assert.equal(model.artifacts["carrier:gather"].shape, "capsule");
  assert.equal(model.steps.find(s => s.id === "publish").output, "none");
  assert.equal(model.edges.find(e => e.from === "publish").kind, "control");
});

test("streaming fill: a carrier waits in its station while its items become ready, then leaves at production", () => {
  const tl = timeline(build(reportRun()));
  const hf = of(tl, "carrier:h-findings:1"), spawn = hf[0];
  assert.equal(spawn.at, "pod:gather:packet_findings@1", "fills inside its branch pod");
  assert.equal(spawn.t, 5, "appears when its first item is ready");
  assert.equal(spawn.carrier.sockets, 2);
  assert.deepEqual(spawn.carrier.items.map(i => [i.gem, i.ready_t, i.evidence]), [["sapphire", 5, "recorded"], ["emerald", 8, "recorded"]]);
  assert.equal(hf.find(e => e.type === "move").t, 10, "departs when produced");
  // Live edge before production: ready items plus one empty socket still filling.
  const live = timeline(build(reportRun().slice(0, 4), graph));
  const filling = live.find(e => e.type === "spawn" && e.item === "carrier:h-findings:1:filling");
  assert.equal(filling.carrier.filling, true); assert.equal(filling.carrier.items.length, 2); assert.equal(filling.carrier.sockets, 3, "one more empty socket while it fills");
  assert.equal(live.some(e => e.item === "carrier:h-findings:1:filling" && e.type !== "spawn"), false, "it does not leave before production");
});

test("fan-in: carriers meet at the join and leave it as one carrier holding every item", () => {
  const tl = timeline(build(reportRun()));
  for (const id of ["carrier:h-findings:1", "carrier:h-risks:1"]) {
    const rows = of(tl, id);
    assert.equal(rows.filter(e => e.type === "move").at(-1).to, "join_evidence");
    assert.deepEqual(rows.filter(e => e.type === "consume").map(e => e.at), ["join_evidence"]);
  }
  const merged = tl.find(e => e.type === "spawn" && e.item.startsWith("merge:"));
  assert.equal(merged.at, "join_evidence");
  assert.deepEqual(merged.carrier.merged_from, ["h-findings", "h-risks"]);
  assert.deepEqual(merged.carrier.items.map(i => i.gem), ["sapphire", "emerald", "amethyst"]);
  const mergedRows = of(tl, merged.item);
  assert.deepEqual(mergedRows.filter(e => e.type === "move").map(e => `${e.from}>${e.to}`), ["join_evidence>draft"]);
  const done = mergedRows.find(e => e.type === "consume");
  assert.equal(done.at, "draft"); assert.ok(done.t >= 13 && done.t <= 13 + 0.8 + 0.01, "consumed at the observed consumption, adjusted only by the minimum visible hop");
});

test("gate seal: R1 is sealed rejected and retired, R2 is the same carrier sealed accepted and forwarded", () => {
  const tl = timeline(build(reportRun()));
  const verdicts = tl.filter(e => e.type === "verdict");
  assert.deepEqual(verdicts.map(v => [v.item, v.verdict, v.step, v.seal]), [["carrier:h-draft:1", "rejected", "quality", true], ["carrier:h-draft:2", "accepted", "quality", true]]);
  assert.equal(tl.filter(e => e.type === "spawn" && e.at === "quality").length, 0, "the gate mints no new carrier");
  const r1 = of(tl, "carrier:h-draft:1");
  assert.equal(r1[0].label, "R1");
  assert.ok(r1.some(e => e.type === "flag" && e.flag === "rejected"));
  const gone = r1.find(e => e.type === "consume");
  assert.equal(gone.at, "quality"); assert.equal(gone.t, 25.2, "no material route back: retired where it waits when the repair consumes it");
  assert.equal(r1.some(e => e.type === "move" && e.to !== "quality"), false);
  assert.deepEqual(r1[0].carrier.consumers.map(c => [c.node, c.digest_match]), [["quality", true], ["draft", true]]);
});

test("a side-effect node with a control edge and a material bypass belt", () => {
  const g = { nodes:[{id:"compose",kind:"synthesize"},{id:"notify",kind:"release",output:"none"},{id:"summarize",kind:"synthesize"},{id:"done",kind:"complete"}],
    edges:[{from:"compose",to:"notify"},{from:"notify",to:"summarize",kind:"control"},{from:"compose",to:"summarize",kind:"material"},{from:"summarize",to:"done",kind:"control"}] };
  const frames = [
    runState({ state:"working", phase:"compose", node:"compose" }, 0.5),
    produced("a1", "compose", "h-compose", 1, 4, [item(0, ["text"], DD1, 4)]),
    runState({ state:"working", phase:"notify", node:"notify" }, 4.2),
    consumed("a2", "notify", 5, [["h-compose", DD1]]),
    runState({ state:"working", phase:"summarize", node:"summarize" }, 6),
    consumed("a3", "summarize", 6.5, [["h-compose", DD1]]),
    runState({ state:"completed", phase:"done", ended_at:T(9) }, 9),
  ];
  const tl = timeline(build(frames, g));
  assert.deepEqual(of(tl, "carrier:h-compose:1").filter(e => e.type !== "spawn").map(e => `${e.type}:${e.from ?? e.at}>${e.to ?? ""}`), ["move:compose>notify", "consume:notify>"]);
  const bypass = of(tl, "carrier:h-compose:1~1");
  assert.equal(bypass[0].type, "spawn"); assert.equal(bypass[0].at, "compose"); assert.equal(bypass[0].carrier.leg, "summarize");
  assert.deepEqual(bypass.filter(e => e.type === "move").map(e => `${e.from}>${e.to}`), ["compose>summarize"], "the bypass belt runs from the producer");
  assert.equal(bypass.find(e => e.type === "consume").t, 6.5);
  assert.equal(tl.some(e => e.type === "move" && e.from === "notify"), false, "nothing leaves the side-effect node");
  const model = toFloorModel(build(frames, g), { runId:"run-c" });
  assert.equal(model.edges.find(e => e.from === "notify").kind, "control");
});

test("+N overflow: every item is kept on the carrier and the socket count reports them all", () => {
  const items = Array.from({ length:7 }, (_, i) => item(i, [["text", "data", "raw", "url"][i % 4]], (i + 10).toString(16).repeat(64).slice(0, 64), 3 + i * 0.1));
  const frames = [runState({ state:"working", phase:"compose", node:"draft" }, 0.5), produced("a1", "draft", "h-many", 1, 4, items)];
  const spawn = timeline(build(frames)).find(e => e.item === "carrier:h-many:1");
  assert.equal(spawn.carrier.sockets, 7); assert.equal(spawn.carrier.items.length, 7);
  assert.deepEqual(spawn.carrier.items.slice(0, 4).map(i => i.gem), ["sapphire", "emerald", "amethyst", "topaz"]);
  assert.equal(spawn.carrier.items[3].byte_length, null, "url parts have no byte length");
});

test("mixed run: recorded attempts become carriers, unrecorded ones keep inferred carriers with one cloudy socket", () => {
  const frames = reportRun().filter(f => !(f.event.type.includes("handoff") && f.event.data.assignment_id === "research_risks") && !(f.event.type.includes("handoff") && f.event.data.node !== "gather"));
  const tl = timeline(build(frames));
  assert.ok(tl.some(e => e.type === "spawn" && e.item === "carrier:h-findings:1"), "recorded attempt is a carrier");
  const risks = tl.find(e => e.type === "spawn" && e.item.startsWith("out:research_risks"));
  assert.ok(risks, "unrecorded attempt keeps its inferred Task output");
  assert.deepEqual(risks.carrier, { evidence:"inferred", sockets:1, items:[], consumers:[], note:"contents not recorded; hand-off inferred from pinned graph order" });
  assert.equal(tl.some(e => e.item?.startsWith("out:research_findings")), false);
  const report = tl.find(e => e.type === "spawn" && e.item.startsWith("artifact:r1:"));
  assert.equal(report.carrier.evidence, "inferred", "report artifacts without a hand-off record stay inferred");
  // A run with no records at all keeps today's path.
  const legacy = timeline(build(reportRun().filter(f => !f.event.type.includes("handoff"))));
  assert.equal(legacy.some(e => e.item?.startsWith("carrier:")), false);
  assert.ok(legacy.filter(e => e.type === "spawn" && (e.item.startsWith("out:") || e.item.startsWith("artifact:"))).every(e => e.carrier.evidence === "inferred"));
});

test("no-content guarantee: carrier projections carry only allowlisted fields, even if a record held more", () => {
  const state = build(reportRun());
  // Defence in depth: content smuggled past the validator into reducer state never reaches the floor.
  const run = state.runs.get("run-c");
  for (const p of run.handoffs.produced) { p.text = "SECRET-TEXT"; for (const i of p.items) Object.assign(i, { text:"SECRET-TEXT", name:"secret.md", url:"https://secret.example", metadata:{ k:"SECRET" }, filename:"secret.md", artifactId:"SECRET-ID" }); }
  for (const c of run.handoffs.consumed) c.inputs[0].description = "SECRET-TEXT";
  const model = toFloorModel(state, { runId:"run-c" });
  const json = JSON.stringify(model);
  assert.equal(/SECRET|secret\.md/.test(json), false);
  const carriers = model.runs[0].timeline.filter(e => e.carrier);
  assert.ok(carriers.length >= 5);
  const CARRIER_FIELDS = new Set(["evidence", "handoff_id", "revision", "node", "output", "produced_at", "produced_t", "sockets", "items", "consumers", "digest_match", "merged_from", "leg", "filling", "note"]);
  for (const e of carriers) {
    for (const key of Object.keys(e.carrier)) assert.ok(CARRIER_FIELDS.has(key), `carrier.${key}`);
    for (const i of e.carrier.items) {
      assert.deepEqual(Object.keys(i).filter(k => !CARRIER_ITEM_FIELDS.includes(k)), []);
      assert.ok(i.digest === null || /^[0-9a-f]{12}$/.test(i.digest), "only a short digest is shown");
    }
    for (const c of e.carrier.consumers) assert.deepEqual(Object.keys(c).sort(), ["consumed_at", "digest_match", "node"]);
  }
});

test("a consumer whose digests match no produced revision is shown as a digest mismatch", () => {
  const frames = [runState({ state:"working", phase:"draft", node:"draft" }, 0.5), produced("a1", "draft", "h-x", 1, 4, [item(0, ["text"], DD1, 4)]),
    runState({ state:"working", phase:"quality", node:"quality" }, 4.5), consumed("q", "quality", 5, [["h-x", DD2]])];
  const spawn = timeline(build(frames)).find(e => e.item === "carrier:h-x:1");
  assert.equal(spawn.carrier.digest_match, false);
  assert.deepEqual(spawn.carrier.consumers.map(c => c.digest_match), [false]);
});

test("inferred items hop a declared control route without the undeclared label", () => {
  const g = { ...graph, edges:graph.edges.filter(e => !(e.from === "quality" && e.to === "publish")) };
  const legacy = reportRun().filter(f => !f.event.type.includes("handoff"));
  const hops = timeline(build(legacy, g)).filter(e => e.type === "move" && e.from === "route_verdict");
  assert.ok(hops.length, "the report item follows the route case");
  assert.ok(hops.every(e => e.control === true && !e.undeclared));
  const undeclared = { ...g, edges:g.edges.filter(e => e.from !== "route_verdict") };
  const old = timeline(build(legacy, undeclared)).filter(e => e.type === "move" && e.from === "route_verdict");
  assert.ok(old.length && old.every(e => e.undeclared === true && !e.control), "a route case with no declared edge stays undeclared");
});
