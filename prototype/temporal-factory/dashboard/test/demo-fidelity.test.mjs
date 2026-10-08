// F02–F05 Demo fidelity: the shipped Floor scenarios must survive the full
// Demo Adapter → contract → reducer → toFloorModel path, and the illustrative
// layer that carries them must never be accepted for Live or Recorded payloads.
import test from "node:test";
import assert from "node:assert/strict";
import { DEMO_ILLUSTRATION_EVENT_TYPE, validateBundle, validateServerMessage, validateSnapshot } from "../contract.mjs";
import { createDashboardState, reduceDashboard, toFloorModel } from "../reducer.mjs";
import { createDemoAdapter } from "../adapters/demo.mjs";
import { loadFloorDemoFixtures } from "./support/floor-fixtures.mjs";

const DEMO = { source:"demo" };
const fixtures = await loadFloorDemoFixtures();
const fixture = id => fixtures.find(row => row.id === id);
// The fixtures are evaluated from the page source in a separate realm: normalize before deep comparison.
const plain = value => JSON.parse(JSON.stringify(value));
const countBy = rows => rows.reduce((acc, row) => (acc[row.type] = (acc[row.type] ?? 0) + 1, acc), {});

async function project(id, { runId = null } = {}) {
  const frames = [];
  const adapter = await createDemoAdapter({ fixtures:[fixture(id)] });
  adapter.observe(id, null, null, frame => frames.push(frame));
  let state = { source:"demo" };
  for (const frame of frames) state = reduceDashboard(state, frame);
  return { adapter, frames, state, floor:toFloorModel(state, { runId }) };
}
const verified = await project("verified-research");
const suppliers = await project("evaluate-suppliers");
const translation = await project("translate-document");
const scenario = { "verified-research":verified, "evaluate-suppliers":suppliers, "translate-document":translation };

test("F05: Demo operating metadata survives the shared path with explicit illustrative labels", () => {
  for (const [id, { floor }] of Object.entries(scenario)) {
    const source = fixture(id);
    assert.equal(floor.illustrative, true);
    assert.equal(floor.illustrativeLabel, "Illustrative Demo fixture");
    assert.match(floor.provenance, /^Demo fixture · /);
    assert.equal(floor.digest, source.digest, `${id} keeps its declared digest`);
    assert.deepEqual(floor.budget, plain(source.budget), `${id} keeps its simulated budget`);
    assert.equal(floor.mainArt, source.mainArt, `${id} keeps its profile result kind`);
    assert.deepEqual(floor.programs, plain(source.programs));
    assert.deepEqual(floor.admission, plain(source.admission));
    assert.deepEqual(floor.artifacts, plain(source.artifacts));
    for (const [key, agent] of Object.entries(source.agents)) {
      const { illustrative, ...rest } = floor.agents[key];
      assert.equal(illustrative, true);
      assert.deepEqual(rest, plain(agent), `${id} agent ${key}`);
    }
  }
  const vr = verified.floor;
  assert.equal(`${vr.version} · ${vr.digest}`, "v2 · d41f7a03");
  assert.deepEqual(vr.versions, plain(fixture("verified-research").versions));
  assert.equal(vr.sla, 240); assert.equal(vr.queueAdvisory, 30);
  assert.deepEqual(vr.agents.synthesizer.others, [[-999, 2]]);
  assert.deepEqual(vr.agents.synthesizer.sharedWith, ["Market Brief", "Supplier Due Diligence"]);
  assert.equal(suppliers.floor.agents["verified-research"].nested, true);
  assert.deepEqual(suppliers.floor.programs.map(p => p.name), ["Maintenance", "Improvement", "Research"]);
  assert.equal(suppliers.floor.mainArt, "scorecard");
  assert.equal(translation.floor.mainArt, "translation");
  const review = vr.steps.find(step => step.id === "review"), decide = vr.steps.find(step => step.id === "decide");
  assert.deepEqual(review.loop, { to:"synth", max:2 });
  assert.equal(decide.escalateTo, "You · Research lead");
  assert.deepEqual(decide.allowed, ["Abort", "One more repair", "Escalate to you"]);
  assert.equal(decide.kind, "wait", "presentation cues never replace the projected kind");
  assert.equal(suppliers.floor.steps.find(step => step.id === "dd").kind, "nested");
});

test("F02: scenario clock basis, ordering, durations and terminal states are preserved", () => {
  for (const [id, { floor }] of Object.entries(scenario)) {
    const source = fixture(id).runs[0], run = floor.runs[0];
    assert.equal(run.start, new Date(Date.parse(source.start)).toISOString(), `${id} keeps the fixture's run.start as t=0`);
    assert.equal(run.startAt, source.startAt ?? 0);
    assert.equal(run.live, source.live === true);
    if (source.live) assert.equal(run.now, source.now);
    const expected = [...source.events].map((event, i) => [event, i]).sort((a, b) => a[0].t - b[0].t || a[1] - b[1]).map(([event]) => `${event.type}@${event.t}`);
    assert.deepEqual(run.timeline.map(event => `${event.type}@${event.t}`), expected, `${id} keeps the original ordering`);
    const works = [...source.events].filter(event => event.type === "work").map(event => event.dur);
    assert.deepEqual(run.timeline.filter(event => event.type === "work").map(event => event.dur), works);
  }
  // Translation has no job-created entry: it starts at the declared scenario start and ends released.
  const tr = translation.floor.runs[0];
  assert.equal(tr.live, false);
  assert.deepEqual(tr.timeline.filter(event => event.type === "end").map(event => [event.t, event.outcome]), [[32, "released"]]);
  assert.equal([...translation.state.runs.values()][0].state.state, "released");
  assert.equal(Math.max(...tr.timeline.map(event => event.t)), 32);
  // Supplier keeps its 118.4 s script and is still waiting (not complete) at its scenario present.
  const sup = suppliers.floor.runs[0];
  assert.equal(Math.max(...sup.timeline.map(event => event.t)), 118.4);
  assert.equal([...suppliers.state.runs.values()][0].state.state, "waiting");
  // A selected Verified Research job starts at its own job entry on the same clock.
  const jobRun = [...verified.state.runs.values()].find(run => run.illustrations.some(row => row.type === "job" && row.t > 0));
  const jobEntry = jobRun.illustrations.find(row => row.type === "job");
  const selected = toFloorModel(verified.state, { runId:jobRun.run_id }).runs;
  assert.equal(selected.length, 1);
  assert.equal(Date.parse(selected[0].start), Date.parse(fixture("verified-research").runs[0].start) + Math.round(jobEntry.t * 1000));
  assert.equal(selected[0].timeline[0].type, "job"); assert.equal(selected[0].timeline[0].t, 0);
});

test("F03/F04: artifact work, movement, Director activity, alarms, recommendations and notes survive projection", () => {
  for (const [id, { floor }] of Object.entries(scenario)) {
    const source = fixture(id).runs[0].events;
    assert.deepEqual(countBy(floor.runs[0].timeline), plain(countBy(source)), `${id} keeps every scripted entry kind`);
    const spawned = floor.runs[0].timeline.filter(event => event.type === "spawn");
    const identity = event => [event.item, event.art, event.rev ?? null, event.sha ?? null];
    assert.deepEqual(new Set(spawned.map(event => JSON.stringify(identity(event)))), new Set(source.filter(event => event.type === "spawn").map(event => JSON.stringify(identity(event)))), `${id} keeps artifact identities`);
  }
  const vr = verified.floor.runs[0].timeline, now = fixture("verified-research").runs[0].now;
  const activeAt = (rows, t) => rows.filter(row => row.type === "alarm" && row.t <= t && (row.t1 == null || row.t1 > t)).length;
  const sourceRows = fixture("verified-research").runs[0].events;
  assert.equal(activeAt(vr, now), activeAt(sourceRows.map(row => ({ ...row, t1:Number.isFinite(row.t1) ? row.t1 : null })), now));
  assert.equal(activeAt(vr, now), 3, "the Verified Research present keeps its three active alarms (HEAD: same three texts)");
  assert.ok(vr.some(row => row.type === "director" && row.tools.length && row.target), "Director turns keep target/tools");
  assert.ok(vr.some(row => row.type === "escalate" && row.reason), "escalation recommendations keep their reason");
  assert.equal(vr.find(row => row.type === "publish").job, null, "factory-wide publication is not a phantom job");
  assert.ok(!verified.floor.runs.some(run => run.id === "floor"), "no job-less phantom run");
  const jobKeys = new Set([...sourceRows].filter(row => row.job != null).map(row => row.job));
  assert.deepEqual(new Set(verified.state.runs.keys()), jobKeys, "scripted job identities (for example 0435) are the run identities");
  assert.ok(vr.filter(row => row.type === "alarm").every(row => row.job === null || jobKeys.has(row.job)));
  const supplierWait = suppliers.floor.runs[0].timeline.find(row => row.type === "wait");
  assert.deepEqual(supplierWait.rec, plain(fixture("evaluate-suppliers").runs[0].events.find(row => row.type === "wait").rec));
  assert.equal(supplierWait.t1, null, "an open human wait remains open");
  const note = suppliers.floor.runs[0].timeline.find(row => row.type === "note");
  assert.deepEqual(note.parts[1], { inbox:true, label:"Open your inbox" });
  const release = translation.floor.runs[0].timeline.find(row => row.type === "release");
  assert.equal(release.item, "t1");
  assert.deepEqual(translation.floor.runs[0].timeline.filter(row => row.type === "readout").map(row => row.value), ["0", "1"]);
  // Completion alone never implies an acceptance verdict.
  assert.equal(translation.floor.runs[0].timeline.some(row => row.type === "verdict"), false);
});

test("illustrative Demo layer is rejected for Live and Recorded payloads", async () => {
  const snapshot = await verified.adapter.snapshot("verified-research");
  assert.equal(validateSnapshot(snapshot, DEMO), snapshot);
  for (const options of [{}, { source:"live" }, { source:"recorded" }]) {
    assert.throws(() => validateSnapshot(structuredClone(snapshot), options), /illustrative Demo fields are not permitted/);
  }
  assert.throws(() => createDashboardState(structuredClone(snapshot), { source:"live" }), /illustrative Demo fields are not permitted/);
  assert.throws(() => reduceDashboard(null, { op:"snapshot", snapshot:structuredClone(snapshot) }), /illustrative Demo fields are not permitted/);
  // Control: the same snapshot without the labelled layer is an ordinary public snapshot.
  const publicSnapshot = structuredClone(snapshot); delete publicSnapshot.state.demo;
  const liveState = createDashboardState(publicSnapshot, { source:"live" });
  const illustration = verified.frames.find(frame => frame.op === "event" && frame.event.type === DEMO_ILLUSTRATION_EVENT_TYPE);
  assert.throws(() => validateServerMessage(illustration), /illustrative Demo events are not permitted/);
  assert.throws(() => reduceDashboard(liveState, illustration), /illustrative Demo events are not permitted/);
  assert.throws(() => validateBundle({ schema_version:1, source:"recorded", snapshot:publicSnapshot, frames:[illustration], provenance:{ label:"copied" } }), /illustrative Demo events are not permitted/);
  assert.throws(() => validateBundle({ schema_version:1, source:"recorded", snapshot, frames:[], provenance:{ label:"copied" } }), /illustrative Demo fields are not permitted/);
  // Copying illustrative fields into a standard Live event is still rejected by the strict allowlist.
  const work = verified.frames.find(frame => frame.event?.type === "com.exomachina.assignment.state_changed.v1");
  const smuggled = structuredClone(work); smuggled.event.data.illustration = illustration.event.data.illustration;
  assert.throws(() => validateServerMessage(smuggled), /not allowlisted/);
  assert.throws(() => validateServerMessage(smuggled, DEMO), /not allowlisted/);
  // Even a hand-built Live state carrying a demo layer projects only public facts.
  const leaked = { ...liveState, demo:structuredClone(snapshot.state.demo) };
  const floor = toFloorModel(leaked);
  assert.equal(floor.budget, null); assert.equal(floor.digest, "Unknown"); assert.deepEqual(floor.versions, {});
  assert.equal(floor.illustrative, undefined); assert.ok(!Object.values(floor.agents).some(agent => agent.price || agent.illustrative));
});

test("illustrative Demo values are bounded and allowlisted even for the Demo source", async () => {
  const snapshot = await translation.adapter.snapshot("translate-document");
  const relabelled = structuredClone(snapshot); relabelled.state.demo.label = "Live";
  assert.throws(() => validateSnapshot(relabelled, DEMO), /Demo label/);
  const extra = structuredClone(snapshot); extra.state.demo.factory.access_token = "x";
  assert.throws(() => validateSnapshot(extra, DEMO), /not allowlisted|sensitive/);
  const frame = structuredClone(translation.frames.find(row => row.event?.type === DEMO_ILLUSTRATION_EVENT_TYPE));
  frame.event.data.illustration.secret_note = "x";
  assert.throws(() => validateServerMessage(frame, DEMO), /not allowlisted/);
  const kind = structuredClone(translation.frames.find(row => row.event?.type === DEMO_ILLUSTRATION_EVENT_TYPE));
  kind.event.data.illustration.type = "payment";
  assert.throws(() => validateServerMessage(kind, DEMO), /unsupported illustrative event kind/);
});

test("duplicate illustrative frames do not duplicate displayed entries", () => {
  let state = translation.state;
  for (const frame of translation.frames.filter(row => row.event?.type === DEMO_ILLUSTRATION_EVENT_TYPE)) state = reduceDashboard(state, frame);
  assert.deepEqual(countBy(toFloorModel(state).runs[0].timeline), countBy(translation.floor.runs[0].timeline));
});

test("a Demo command that ends an illustrated run closes its open waits and alarms at the observed end", async () => {
  const local = { id:"demo-abort-illustrated", name:"Illustrated abort", steps:[{ id:"intake", kind:"intake" }, { id:"gate", kind:"wait" }],
    runs:[{ id:"job", start:"2026-09-24T09:00:00", events:[
      { t:0, type:"spawn", item:"a", art:"brief", at:"intake" },
      { t:1, type:"move", item:"a", from:"intake", to:"gate", dur:1 },
      { t:2, type:"wait", t1:null, step:"gate", human:true, responder:"You", allowed:["Abort"] },
      { t:2, type:"alarm", t1:null, level:"action", text:"Decision needed" },
    ] }] };
  let state = { source:"demo" };
  const adapter = await createDemoAdapter({ fixtures:[local], onFrame:frame => { state = reduceDashboard(state, frame); } });
  const snapshot = await adapter.snapshot(local.id), run = snapshot.state.runs[0];
  adapter.observe(local.id);
  const result = await adapter.command({ op:"command", factory_id:local.id, command_id:"abort-illustrated", task_id:run.task.id, context_id:run.task.context_id, action:"abort", expected_state:"waiting" });
  assert.equal(result.lifecycle, "applied");
  const timeline = toFloorModel(state).runs[0].timeline, end = timeline.find(row => row.type === "end");
  assert.equal(end.outcome, "aborted"); assert.equal(end.illustrative_basis, "observed_demo_command");
  assert.ok(end.t >= 2);
  assert.equal(timeline.find(row => row.type === "wait").t1, end.t);
  assert.equal(timeline.find(row => row.type === "alarm").t1, end.t);
  assert.equal(toFloorModel(state).runs[0].live, false);
});
