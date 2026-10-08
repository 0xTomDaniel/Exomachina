import test from "node:test";
import assert from "node:assert/strict";
import { createDashboardState, toFloorModel, reduceDashboard } from "../reducer.mjs";
import { makeCloudEvent } from "../contract.mjs";
import { readFile } from "node:fs/promises";

const T0="2026-10-03T16:00:00.000Z", T1="2026-10-03T16:01:00.000Z";
const pins={manifest_digest:"a".repeat(64),package_digest:"b".repeat(64),definition_digest:"c".repeat(64),interpreter_build:"build-1"};
function stateFor(graph,{pinned=false,delivery=[]}={}) {
  return createDashboardState({schema_version:1,cursor:`c1.abcdefghijklmnop.${"0".repeat(40)}`,captured_at:T1,freshness:{status:"fresh",observed_at:T1},state:{
    factory:{id:"floor-factory",name:"Floor factory",graph:pinned?{nodes:[],edges:[]}:graph},
    active_publication:{publication_version:"v1",...pins},capacity:null,commercial:{usage:[],payments:[],obligations:[]},
    runs:[{id:"run-one",task:{id:"task-one",context_id:"context-one"},started_at:T0,status:{state:"completed",phase:"accepted",ended_at:T1},pinned:pins,...(pinned?{graph}:{}),assignments:[],artifacts:[],quality:[],commands:[],decisions:[],delivery,incidents:[],admissions:[]}],
  }},{source:"live"});
}
const parent={nodes:[{id:"done",kind:"complete"},{id:"invoke_child",kind:"nested_factory"}],edges:[{from:"invoke_child",to:"done"}]};
const child={nodes:[{id:"abort",kind:"abort"},{id:"compose_report",kind:"synthesize"},{id:"director",kind:"director_wait"},{id:"done",kind:"complete"},{id:"gather",kind:"parallel"},{id:"independent_quality",kind:"quality"},{id:"join_evidence",kind:"join"},{id:"publish",kind:"release"},{id:"repair",kind:"repair"},{id:"route_verdict",kind:"route"}],edges:[{from:"compose_report",to:"independent_quality"},{from:"director",to:"abort"},{from:"gather",to:"join_evidence"},{from:"independent_quality",to:"route_verdict"},{from:"join_evidence",to:"compose_report"},{from:"publish",to:"done"},{from:"repair",to:"compose_report"},{from:"repair",to:"director"}]};

test("pinned parent gets visual boundaries and groups without changing execution facts",()=>{
  const state=stateFor(parent,{pinned:true}); const before=JSON.stringify([...state.runs]);
  const floor=toFloorModel(state,{runId:"run-one"});
  assert.deepEqual(floor.boundaries,{entry:["invoke_child"],exit:["done"],presentation_only:true,entry_basis:"visual_graph_source",exit_basis:"declared_terminal"});
  assert.deepEqual(floor.steps.map(n=>n.id),parent.nodes.map(n=>n.id));
  assert.deepEqual(floor.edges,parent.edges);
  assert.equal(floor.steps.find(n=>n.id==="done").kind,"end");
  assert.equal(floor.steps.find(n=>n.id==="done").type,"complete");
  assert.deepEqual(floor.runs[0].pinned,pins);
  assert.ok(floor.departments.every(g=>g.presentation_only&&g.source==="visual_kind"));
  assert.equal(JSON.stringify([...state.runs]),before);
});

test("child preserves omitted branch edges and separates release from terminal exits",()=>{
  const floor=toFloorModel(stateFor(child,{pinned:true}),{runId:"run-one"});
  assert.deepEqual(floor.boundaries.entry,["gather"]);
  assert.deepEqual(floor.boundaries.exit,["abort","done"]);
  assert.deepEqual(floor.edges,child.edges);
  assert.equal(floor.steps.find(n=>n.id==="publish").kind,"output");
  assert.equal(floor.steps.find(n=>n.id==="abort").type,"abort");
  assert.deepEqual(new Set(floor.departments.map(g=>g.name)),new Set(["Work · visual group","Review · visual group","Completion · visual group"]));
  assert.equal(floor.runs[0].timeline.at(-1).outcome,"completed");
});

test("declared intake and group metadata survive with collision-safe visual annotations",()=>{
  const graph={nodes:[{id:"intake",kind:"intake",dept:"production"},{id:"work",kind:"agent",dept:"visual-work"},{id:"extra",kind:"agent"},{id:"release",kind:"output",dept:"quality"}],edges:[{from:"intake",to:"work"},{from:"work",to:"release"}]};
  const floor=toFloorModel(stateFor(graph));
  assert.equal(floor.boundaries.entry_basis,"declared_intake");
  assert.deepEqual(floor.boundaries.entry,["intake"]);
  assert.equal(floor.boundaries.exit_basis,"declared_output");
  assert.equal(floor.steps.find(n=>n.id==="work").dept,"visual-work");
  assert.equal(floor.steps.find(n=>n.id==="extra").dept,"visual-work-presentation");
  assert.equal(floor.departments.find(g=>g.id==="production").source,"declared_graph");
  assert.equal(floor.departments.find(g=>g.id==="production").name,"Production · graph group");
});

test("cyclic graph uses explicitly visual fallback attachments and no new nodes",()=>{
  const graph={nodes:[{id:"a",kind:"agent"},{id:"b",kind:"agent"}],edges:[{from:"a",to:"b"},{from:"b",to:"a"}]};
  const floor=toFloorModel(stateFor(graph));
  assert.equal(floor.boundaries.entry_basis,"visual_fallback");
  assert.equal(floor.boundaries.exit_basis,"visual_fallback");
  assert.deepEqual(floor.boundaries.entry,["a"]);assert.deepEqual(floor.boundaries.exit,["b"]);
  assert.equal(floor.steps.length,2);
});

test("artifact facts alone never create spatial output or delivered claims",()=>{
  const state=stateFor(parent);
  const event=makeCloudEvent({id:`obs-${"d".repeat(64)}`,factory_id:"floor-factory",type:"com.exomachina.artifact.revised.v1",time:T1,subject:"runs/run-one",data:{schema_version:1,factory_id:"floor-factory",run_id:"run-one",task_id:"task-one",artifact_revision:"r1",artifact_sha256:"e".repeat(64)}});
  const next=reduceDashboard(state,{op:"event",cursor:`c1.abcdefghijklmnop.${"1".repeat(40)}`,event});
  const floor=toFloorModel(next);
  assert.equal(next.runs.get("run-one").artifacts.length,1);
  assert.equal(floor.runs[0].timeline.some(row=>row.type==="spawn"),false);
  assert.equal(floor.runs[0].timeline.at(-1).outcome,"completed");
});

test("conflicted receipt stays unverified while independent valid local receipt can prove delivery",()=>{
  const conflict={receipt_id:"receipt-one",delivery_kind:"local_file",outcome:"local-file-delivered",receipt_conflict:true};
  const state=stateFor(parent);state.runs.get("run-one").delivery=[conflict];
  assert.equal(toFloorModel(state).runs[0].timeline.at(-1).outcome,"delivery-unverified");
  state.runs.get("run-one").delivery.push({...conflict,receipt_id:"receipt-two",receipt_conflict:false});
  assert.equal(toFloorModel(state).runs[0].timeline.at(-1).outcome,"local-file-delivered");
});

test("absent graph remains unavailable rather than inventing boundaries",()=>{
  assert.equal(toFloorModel(stateFor({nodes:[],edges:[]})),null);
});

test("standalone release boundary survives alongside abort; entry controls stay outside canvas pan handling",async()=>{
  const floor=toFloorModel(stateFor({nodes:[{id:"start",kind:"intake"},{id:"release",kind:"output"},{id:"abort",kind:"end"}],edges:[{from:"start",to:"release"},{from:"start",to:"abort"}]}));
  assert.deepEqual(floor.boundaries.exit,["abort","release"]);
  const html=await readFile(new URL("../../../../docs/design/exomachina-floor.html",import.meta.url),"utf8");
  assert.match(html,/'boundary-send float-ui'/);
  assert.match(html,/brief\.className='float-ui'/);
  assert.match(html,/<dt>Floor groups<\/dt>/);
  const click=html.slice(html.indexOf("send.addEventListener('click'"),html.indexOf("G.exit.forEach(e =>",html.indexOf("send.addEventListener('click'")));
  assert.doesNotMatch(click,/wasDrag/);
});
