import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { makeCloudEvent, validateCloudEvent, validateSnapshot } from "../contract.mjs";
import { createDashboardState, dashboardViewModels, reduceDashboard, toFloorModel } from "../reducer.mjs";

const FACTORY="wait-factory";
const RUN="wait-run";
const TASK="wait-task";
const CONTEXT="wait-context";
const T0="2026-09-24T16:48:44.459646Z";
const TWAIT="2026-09-24T16:49:14.459646Z";
const TDEADLINE="2026-09-24T16:54:14.459646Z";
const TFRAME="2026-09-24T16:49:20.083123Z";
const pins={manifest_digest:"a".repeat(64),package_digest:"b".repeat(64),definition_digest:"c".repeat(64),interpreter_build:"build-1"};
const cursor=n=>`c1.abcdefghijklmnop.${String(n).padStart(40,"0")}`;

function snapshot(status={}) {
  const graph={nodes:[{id:"intake",kind:"intake"},{id:"approval",kind:"wait",name:"Approval"}],edges:[{from:"intake",to:"approval"}]};
  return validateSnapshot({
    schema_version:1,cursor:cursor(1),captured_at:TFRAME,freshness:{status:"fresh",observed_at:TFRAME},
    state:{
      factory:{id:FACTORY,name:"Wait parity",graph},
      runs:[{id:RUN,task:{id:TASK,context_id:CONTEXT},status:{state:"working",phase:"started",...status},started_at:T0,pinned:{...pins},assignments:[],artifacts:[],quality:[],decisions:[],commands:[],delivery:[],incidents:[],admissions:[]}],
      active_publication:{schema_version:1,factory_id:FACTORY,publication_version:"wait-publication",...pins},
      capacity:null,commercial:{usage:[],obligations:[],payments:[]},
    },
  });
}

let serial=0;
function waitEvent(overrides={}) {
  serial++;
  const role=overrides.wait_role??"human";
  const data={
    schema_version:1,factory_id:FACTORY,run_id:RUN,task_id:TASK,context_id:CONTEXT,
    state:"waiting",phase:role==="director"?"awaiting-director":"awaiting-human",node:"approval",wait_role:"human",wait_actor_identity:"operator-7",
    wait_started_at:TWAIT,wait_deadline:TDEADLINE,permitted_actions:["approve","send_back"],
    ...overrides,
  };
  for(const key of Object.keys(data))if(data[key]===undefined)delete data[key];
  return makeCloudEvent({
    id:`obs-${String(serial).padStart(64,"0")}`,factory_id:FACTORY,
    type:"com.exomachina.run.state_changed.v1",time:TFRAME,subject:`runs/${RUN}`,data,
  });
}

function reduceWait(overrides={}) {
  const initial=createDashboardState(snapshot(),{source:"live"});
  const event=waitEvent(overrides);
  validateCloudEvent(event);
  return reduceDashboard(initial,{op:"event",cursor:cursor(serial+1),event});
}

test("actual human wait projects safe task pins and a renderer wait at explicit time on an existing node",()=>{
  const state=reduceWait();
  const waits=dashboardViewModels(state).waits;
  assert.equal(waits.length,1);
  assert.deepEqual(waits[0],{
    run_id:RUN,task:{id:TASK,context_id:CONTEXT},pinned:pins,node:"approval",
    wait_role:"human",wait_actor_identity:"operator-7",responder:"operator-7",
    wait_started_at:TWAIT,wait_deadline:TDEADLINE,permitted_actions:["approve","send_back"],
    candidate_refs:null,context:null,recommendation:null,
  });
  const floor=toFloorModel(state,{runId:RUN});
  const wait=floor.runs[0].timeline.find(row=>row.type==="wait");
  assert.deepEqual({type:wait.type,step:wait.step,human:wait.human,responder:wait.responder,allowed:wait.allowed,t:wait.t},
    {type:"wait",step:"approval",human:true,responder:"operator-7",allowed:["approve","send_back"],t:30});
  assert.deepEqual(floor.steps.map(step=>step.id),["intake","approval"]);
});

test("snapshot human wait validates the same finite fields and phase pairing as events",()=>{
  const status={state:"waiting",phase:"awaiting-human",node:"approval",wait_role:"human",wait_actor_identity:"operator-7",wait_started_at:TWAIT,wait_deadline:TDEADLINE,permitted_actions:["approve","send_back"]};
  const state=createDashboardState(snapshot(status),{source:"live"});
  const wait=toFloorModel(state,{runId:RUN}).runs[0].timeline.find(row=>row.type==="wait");
  assert.equal(wait.human,true);
  assert.equal(wait.responder,"operator-7");
  assert.equal(wait.t,30);
  assert.deepEqual(wait.allowed,["approve","send_back"]);
  assert.equal(dashboardViewModels(state).waits[0].recommendation,null);

  assert.throws(()=>snapshot({...status,wait_role:"operator"}),/unsupported wait role/);
  assert.throws(()=>snapshot({...status,wait_actor_identity:"not an identity"}),/invalid identifier/);
  assert.throws(()=>snapshot({...status,permitted_actions:["approve","approve"]}),/invalid bounded action array/);
  assert.throws(()=>snapshot({...status,wait_started_at:"not-a-time"}),/timezone-qualified date-time/);
  assert.throws(()=>snapshot({...status,phase:"awaiting-director"}),/wait facts do not match observed phase/);
});

test("director wait stays non-human; missing actor is unreported and absent start or graph node creates no Floor wait",()=>{
  const director=reduceWait({wait_role:"director",wait_actor_identity:undefined});
  const directorWait=toFloorModel(director,{runId:RUN}).runs[0].timeline.find(row=>row.type==="wait");
  assert.equal(directorWait.human,false);
  assert.equal(directorWait.responder,"unreported");
  assert.equal(dashboardViewModels(director).waits[0].responder,"unreported");

  const noTime=reduceWait({wait_started_at:undefined});
  assert.equal(dashboardViewModels(noTime).waits[0].wait_started_at,null);
  assert.equal(toFloorModel(noTime,{runId:RUN}).runs[0].timeline.some(row=>row.type==="wait"),false);

  const unknownNode=reduceWait({node:"not-in-pinned-graph"});
  assert.equal(toFloorModel(unknownNode,{runId:RUN}).runs[0].timeline.some(row=>row.type==="wait"),false);
  assert.deepEqual(toFloorModel(unknownNode,{runId:RUN}).steps.map(step=>step.id),["intake","approval"]);
});

test("legacy statuses do not infer a human wait and invalid role is rejected",()=>{
  const state=reduceWait({wait_role:undefined,wait_actor_identity:undefined,wait_started_at:undefined,wait_deadline:undefined,permitted_actions:undefined});
  assert.deepEqual(dashboardViewModels(state).waits,[]);
  assert.equal(toFloorModel(state,{runId:RUN}).runs[0].timeline.some(row=>row.type==="wait"),false);
  assert.throws(()=>validateCloudEvent(waitEvent({wait_role:"operator"})),/wait facts do not match observed phase/);
  assert.throws(()=>validateCloudEvent(waitEvent({phase:"wait"})),/wait facts do not match observed phase/);
  assert.throws(()=>validateCloudEvent(waitEvent({phase:"awaiting-director"})),/wait facts do not match observed phase/);
});

test("Decisions wait actions are live-only, permission-backed, run-bound, and honestly unavailable without a recommendation",async()=>{
  const html=await readFile(new URL("../../../../docs/design/exomachina-floor.html",import.meta.url),"utf8");
  const start=html.indexOf("if(view==='Decisions') {");
  const end=html.indexOf("if(view==='Outputs') {",start);
  assert.notEqual(start,-1,"Decisions renderer exists");
  assert.notEqual(end,-1,"Decisions renderer ends before Outputs");
  const decisions=html.slice(start,end);

  assert.match(decisions,/const waits=vm\.waits\|\|\[\]/);
  assert.match(decisions,/typeof wait\.recommendation==='string'\?wait\.recommendation:'unavailable'/);
  assert.match(decisions,/currentSource==='live'&&Array\.isArray\(wait\.permitted_actions\)/);
  assert.match(decisions,/for\(const action of wait\.permitted_actions\)/);
  assert.match(decisions,/controlReason\(\{runId:wait\.run_id\}\)/);
  assert.match(decisions,/Number\.isFinite\(deadlineMs\)&&deadlineMs<=Date\.now\(\)/);
  assert.match(decisions,/button\.disabled=!!reason\|\|expired/);
  assert.match(decisions,/void store\.decide\(action,wait\.run_id\)/);
  assert.doesNotMatch(decisions,/\b(?:approve|reject|send_back)\b/);
});
