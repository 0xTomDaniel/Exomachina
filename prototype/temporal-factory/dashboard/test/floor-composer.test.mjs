import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { createDemoAdapter } from "../adapters/demo.mjs";

const htmlPath = new URL("../../../../docs/design/exomachina-floor.html", import.meta.url);
const floorHtml = await readFile(htmlPath, "utf8");

test("Demo projection retains only safe, explicit graph dept values", async () => {
  const adapter = await createDemoAdapter({ fixtures: [{
    id: "dept-boundary-demo",
    name: "Department preservation fixture",
    capability: "dept-boundary-demo@1",
    steps: [
      { id: "intake", kind: "intake", name: "Intake" },
      { id: "research", kind: "agent", name: "Research", capability: "research", dept: "analysis" },
      { id: "review", kind: "gate", name: "Review", dept: "unsupported group" },
    ],
    edges: [{ from: "intake", to: "research" }, { from: "research", to: "review" }],
    runs: [],
  }] });
  try {
    const snapshot = await adapter.snapshot("dept-boundary-demo");
    const nodes = snapshot.state.factory.graph.nodes;
    assert.equal(nodes.find(node => node.id === "research").dept, "analysis");
    assert.equal(Object.hasOwn(nodes.find(node => node.id === "review"), "dept"), false);
    assert.equal(nodes.length, 3, "Demo graph projection does not add presentation or execution nodes");
  } finally {
    adapter.close();
  }
});

test("Floor boundaries attach to existing nodes and distinguish graph groups from visual groups", () => {
  assert.match(floorHtml, /wall\.dataset\.boundary\s*=\s*['"]entry['"]/);
  assert.match(floorHtml, /wall\.dataset\.boundary\s*=\s*['"]exit['"]/);
  assert.match(floorHtml, /groupLabel\.dataset\.presentationGroup\s*=/);
  assert.match(floorHtml, /Declared graph group/);
  assert.match(floorHtml, /Visual group/);
  assert.match(floorHtml, /Factory entry/);
  assert.match(floorHtml, /Factory exit/);
  assert.match(floorHtml, /f\.boundaries\?\.entry|model\.boundaries\?\.entry/);
  assert.match(floorHtml, /f\.boundaries\?\.exit|model\.boundaries\?\.exit/);
  assert.match(floorHtml, /delivery-unverified[^\n]*Delivery unverified/);
});

test("Send work opens the shared Floor composer and uses the existing brief submit path", () => {
  assert.match(floorHtml, /send\.dataset\.sendWork\s*=/);
  assert.match(floorHtml, /window\.EXO_DASHBOARD_OPEN_BRIEF/);
  assert.match(floorHtml, /brief\.id\s*=\s*['"]exo-floor-brief['"]/);
  assert.match(floorHtml, /brief\.dataset\.sendWorkComposer\s*=/);
  assert.match(floorHtml, /close\.dataset\.closeComposer\s*=/);
  assert.match(floorHtml, /appendBriefForm\(/);
  assert.match(floorHtml, /submitBrief\(input\.value,null,/);
  assert.match(floorHtml, /setAttribute\(['"]role['"],['"]status['"]\)/);
  assert.match(floorHtml, /Recorded evidence is read-only/);
  assert.match(floorHtml, /input\.readOnly=policy\.readOnly/);
  assert.match(floorHtml, /button\.disabled=policy\.submitDisabled/);
  assert.match(floorHtml, /notice\.dataset\.disabledReason\s*=/);
  assert.match(floorHtml, /notice\.textContent\s*=\s*reason/);
  assert.match(floorHtml, /Submission blocked: /);
  assert.match(floorHtml, /Source: /);
  assert.match(floorHtml, /health\?\.readiness\?\.submission_ready\s*===\s*true/);
  assert.match(floorHtml, /fetch\(['"]\/health['"]\s*,\s*\{credentials:['"]same-origin['"],cache:['"]no-store['"]/);
  assert.match(floorHtml, /if\(requireSubmission&&!submissionReadiness\.ready\)return submissionReadiness\.reason/);

  const openerStart = floorHtml.indexOf("window.EXO_DASHBOARD_OPEN_BRIEF=opener=>");
  const openerEnd = floorHtml.indexOf("document.addEventListener('keydown'", openerStart);
  assert.notEqual(openerStart, -1);
  assert.notEqual(openerEnd, -1);
  const opener = floorHtml.slice(openerStart, openerEnd);
  assert.doesNotMatch(opener, /fetch\(|submitBrief\(/, "opening the composer performs no submission or provider request");
  assert.match(opener, /renderFloorBrief\(\{focus:true\}\)/);
});


test("submission reports its lifecycle in the composer and guards a double click", async () => {
  const {runInNewContext}=await import("node:vm");
  const start=floorHtml.indexOf("async function submitBrief("), end=floorHtml.indexOf("function appendBriefForm(",start);
  const statuses=[];
  let calls=0;
  const submit=runInNewContext('('+floorHtml.slice(start,end).trim()+')',{controlReason:()=>'',sourceStatus:()=>{},currentSource:'demo',adapter:{submit:async()=>{calls++;return {lifecycle:'local_only',reason:'Local simulation only.'};}},briefOutcomeByFactory:new Map(),sourceGeneration:1,activeFactoryId:'factory-one',crypto:{randomUUID:()=> 'test-message'}});
  await submit('QA marker',null,text=>statuses.push(text));
  assert.equal(calls,1);
  assert.deepEqual(statuses,['Checking local Demo brief…','Demo only: no real work was started. Local simulation only.']);
  const blocked=[];
  const gated=runInNewContext('('+floorHtml.slice(start,end).trim()+')',{currentSource:'live',activeFactoryId:'factory-one',briefOutcomeByFactory:new Map(),controlReason:()=> 'Live Observation is not current and connected; commands are unavailable.',sourceStatus:()=>{}});
  await gated('QA marker',null,text=>blocked.push(text));
  assert.deepEqual(blocked,['Live Observation is not current and connected; commands are unavailable.']);
  assert.match(floorHtml,/form.dataset.submitting==='true'/);
  assert.match(floorHtml,/notice.textContent=text/);
});


test("plain A2A replies remain visible without inventing a new workflow", async () => {
 const {runInNewContext}=await import('node:vm');
 const start=floorHtml.indexOf('async function submitBrief('),end=floorHtml.indexOf('function appendBriefForm(',start);
 const run=async reply=>{
  const outcomes=new Map(),statuses=[],requests=[];
  const fn=runInNewContext('('+floorHtml.slice(start,end).trim()+')',{currentSource:'live',activeFactoryId:'factory-one',sourceGeneration:1,briefOutcomeByFactory:outcomes,controlReason:()=>'',sourceStatus:()=>{},adapter:{submit:async request=>{requests.push(request);return reply;}},crypto:{randomUUID:()=> 'fixture-id'},SAFE_SELECTION_ID:/^[a-z]+$/,refreshSubmissionReadiness:async()=>{}});
  await fn('Fixture input',null,value=>statuses.push(value));
  return {outcomes,statuses,requests};
 };
 // A2A v1.0: the request is SendMessage with a kind-free text Part and ROLE_USER.
 const v1=await run({result:{message:{messageId:'reply',role:'ROLE_AGENT',parts:[{text:'Fixture reply only.'}]}}});
 assert.equal(v1.statuses.at(-1),'Factory reply: Fixture reply only.');
 assert.equal(v1.outcomes.get('["live","factory-one"]'),'Factory reply: Fixture reply only.');
 assert.equal(v1.requests.length,1);
 assert.equal(v1.requests[0].method,'SendMessage');
 assert.equal(v1.requests[0].params.message.role,'ROLE_USER');
 assert.deepEqual(JSON.parse(JSON.stringify(v1.requests[0].params.message.parts)),[{text:'Fixture input',mediaType:'text/plain'}]);
 // A v1 Director error reply arrives as {message}; it is reported, not treated as a Task.
 const failed=await run({result:{message:{messageId:'reply',role:'ROLE_AGENT',parts:[{data:{error:'factory permits one active job; another original Task is busy'}}]}}});
 assert.equal(failed.statuses.at(-1),'factory permits one active job; another original Task is busy');
 // A 0.3-shaped reply (unwrapped result, kind Parts) is never shown as a factory reply.
 const legacy=await run({result:{kind:'message',parts:[{kind:'text',text:'Legacy reply.'}]}});
 assert.equal(legacy.statuses.at(-1),'Brief received by A2A; workflow outcome is pending Observation.');
 assert.match(floorHtml,/outcome.dataset.briefResult/);
});
