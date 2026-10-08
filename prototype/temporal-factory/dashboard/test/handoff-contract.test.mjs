import test from "node:test";
import assert from "node:assert/strict";
import { HANDOFF_EVENT_TYPES, makeCloudEvent, validateCloudEvent, validateSnapshot } from "../contract.mjs";

// Content-free hand-off Observation facts (A2A v1 mediation decision 4). Synthetic,
// test-only facts: runtime emission is a later phase.
const FACTORY = "factory-handoff", D1 = "1".repeat(64), D2 = "2".repeat(64), SHA = "a".repeat(64);
const at = s => new Date(Date.parse("2026-10-07T10:00:00.000Z") + s * 1000).toISOString();
const base = { schema_version:1, factory_id:FACTORY, run_id:"run-1", assignment_id:"assign-1", attempt_id:"1", node:"draft" };
const item = (extra = {}) => ({ item_index:0, source:"artifact", part_kinds:["text"], media_type:"text/markdown", byte_length:1200, ready_at:at(5), digest:D1, ...extra });
const produced = (extra = {}, itemExtra = {}) => ({ ...base, handoff_id:"handoff-draft", handoff_revision:1, produced_at:at(6), items:[item(itemExtra)], ...extra });
const consumed = (extra = {}, inputExtra = {}) => ({ ...base, node:"quality", consumed_at:at(7), inputs:[{ handoff_id:"handoff-draft", item_digests:[D1], ...inputExtra }], ...extra });
const ready = (extra = {}) => ({ ...base, handoff_id:"handoff-draft", item_index:0, part_kinds:["data"], media_type:"application/json", ready_at:at(4), ...extra });
let n = 0;
const event = (suffix, data) => makeCloudEvent({ id:`obs-${(++n).toString(16).padStart(64, "0")}`, factory_id:FACTORY, type:`com.exomachina.handoff.${suffix}.v1`, time:at(8), subject:"runs/run-1", data });
const check = (suffix, data) => validateCloudEvent(event(suffix, data));

test("the three hand-off fact types validate with their exact field sets", () => {
  assert.deepEqual(HANDOFF_EVENT_TYPES, ["com.exomachina.handoff.produced.v1", "com.exomachina.handoff.consumed.v1", "com.exomachina.handoff.item_ready.v1"]);
  check("produced", produced());
  check("produced", produced({ items:[item({ artifact_revision:"r1", artifact_sha256:SHA }), item({ item_index:1, part_kinds:["url"], byte_length:null, media_type:null, digest:D2 })] }));
  check("produced", produced({ items:[item({ source:"message", part_kinds:["text", "data"] })] }));
  check("consumed", consumed());
  check("consumed", consumed({ inputs:[{ handoff_id:"a", item_digests:[D1] }, { handoff_id:"b", item_digests:[D2, D1] }] }));
  check("item_ready", ready());
});

test("content-bearing fields are rejected at every depth of every hand-off fact", () => {
  const content = { text:"secret words", data:{ a:1 }, name:"report.md", description:"the report", artifactId:"art-1", filename:"report.md", url:"https://example.test/x", metadata:{ k:"v" }, bytes:"AAAA", raw:"AAAA", parts:[] };
  for (const [key, value] of Object.entries(content)) {
    assert.throws(() => check("produced", produced({ [key]:value })), /not allowlisted/, `produced.${key}`);
    assert.throws(() => check("produced", produced({}, { [key]:value })), /not allowlisted/, `produced.items[].${key}`);
    assert.throws(() => check("consumed", consumed({ [key]:value })), /not allowlisted/, `consumed.${key}`);
    assert.throws(() => check("consumed", consumed({}, { [key]:value })), /not allowlisted/, `consumed.inputs[].${key}`);
    assert.throws(() => check("item_ready", ready({ [key]:value })), /not allowlisted/, `item_ready.${key}`);
  }
  // Fields of other event types do not leak into the hand-off allowlist, and vice versa.
  for (const key of ["task_id", "context_id", "capability", "state", "author_identity"]) assert.throws(() => check("produced", produced({ [key]:"x" })), /not allowlisted/);
  assert.throws(() => validateCloudEvent(makeCloudEvent({ id:`obs-${"f".repeat(64)}`, factory_id:FACTORY, type:"com.exomachina.artifact.revised.v1", time:at(1), subject:"runs/run-1", data:{ schema_version:1, factory_id:FACTORY, run_id:"run-1", artifact_revision:"r1", artifact_sha256:SHA, handoff_id:"h" } })), /not allowlisted/);
});

test("hand-off facts reject malformed and contract-violating shapes", () => {
  const bad = [
    ["produced", produced({ items:[] }), /non-empty item array/],
    ["produced", produced({ handoff_revision:0 }), /positive integer/],
    ["produced", produced({}, { part_kinds:[] }), /invalid part kinds/],
    ["produced", produced({}, { part_kinds:["file"] }), /invalid part kinds/],
    ["produced", produced({}, { source:"agent" }), /invalid hand-off item source/],
    ["produced", produced({}, { byte_length:null }), /only for url parts/],
    ["produced", produced({}, { digest:"short" }), /invalid digest/],
    ["produced", produced({}, { media_type:"Text Markdown" }), /invalid media type/],
    ["produced", produced({}, { artifact_revision:"r1" }), /revision and sha256/],
    ["produced", produced({ items:[item(), item()] }), /duplicate hand-off item index/],
    ["produced", produced({ items:[item({ source:"message" }), item({ item_index:1 })] }), /exactly one item/],
    ["produced", (({ produced_at, ...rest }) => rest)(produced()), /required event fact missing/],
    ["produced", (({ digest, ...rest }) => ({ ...produced(), items:[rest] }))(item()), /required hand-off item fact missing/],
    ["consumed", consumed({ inputs:[] }), /non-empty input array/],
    ["consumed", consumed({}, { item_digests:[] }), /non-empty digest array/],
    ["consumed", consumed({ inputs:[{ handoff_id:"a", item_digests:[D1] }, { handoff_id:"a", item_digests:[D2] }] }), /duplicate consumed hand-off/],
    ["item_ready", ready({ item_index:-1 }), /bounded item index/],
    ["item_ready", ready({ ready_at:"2026-10-07T10:00:00" }), /timezone-qualified/],
  ];
  for (const [suffix, data, pattern] of bad) assert.throws(() => check(suffix, data), pattern, JSON.stringify(data).slice(0, 120));
});

test("pinned graphs accept material/control edge kinds and node output modes, strictly", () => {
  const snap = graph => ({ schema_version:1, cursor:`c1.abcdefghijklmnop.${"0".repeat(40)}`, captured_at:at(0), freshness:{ status:"fresh", observed_at:at(0) }, state:{ factory:{ id:FACTORY, name:"F", graph }, runs:[], active_publication:null, capacity:null, commercial:{ usage:[], obligations:[], payments:[] } } });
  const nodes = [{ id:"draft", kind:"synthesize", output:"artifacts" }, { id:"publish", kind:"release", output:"none" }, { id:"ask", kind:"synthesize", output:"message" }, { id:"done", kind:"complete" }];
  validateSnapshot(snap({ nodes, edges:[{ from:"draft", to:"publish" }, { from:"draft", to:"ask", kind:"material" }, { from:"publish", to:"done", kind:"control" }] }));
  assert.throws(() => validateSnapshot(snap({ nodes, edges:[{ from:"publish", to:"done" }] })), /side-effect node may not have an outgoing material edge/);
  assert.throws(() => validateSnapshot(snap({ nodes, edges:[{ from:"draft", to:"done", kind:"belt" }] })), /invalid edge kind/);
  assert.throws(() => validateSnapshot(snap({ nodes:[{ id:"draft", kind:"synthesize", output:"files" }], edges:[] })), /invalid node output mode/);
  const activated = output => validateCloudEvent(makeCloudEvent({ id:`obs-${"e".repeat(64)}`, factory_id:FACTORY, type:"com.exomachina.publication.activated.v1", time:at(1), subject:"publication", data:{ schema_version:1, factory_id:FACTORY, manifest_digest:SHA, package_digest:SHA, definition_digest:SHA, interpreter_build:"b1", graph_nodes:[{ id:"publish", type:"release", output }] } }));
  activated("none");
  assert.throws(() => activated("everything"), /invalid node output mode/);
});
