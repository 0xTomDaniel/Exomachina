import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { makeCloudEvent, validateCloudEvent, validateSnapshot } from "../contract.mjs";
import { createDashboardState, dashboardViewModels, reduceDashboard, toFloorModel } from "../reducer.mjs";

const FACTORY="a40ec20b-2787-438a-8d11-11dd9ccecc16";
const RUN="a40ec20b-2787-438a-8d11-11dd9ccecc16.d18e065039051331a98d";
const TASK="261aa778-bf8a-40f9-a9a4-a33a8098df12";
const CONTEXT="0fcab54d-d9be-4514-8a5b-29bb7335eb94";
const INCIDENT="inc-25442bdea46acfb3f28cc1261b989ae160e7461e28d47d2c4697c07e3c31ece3";
const EVIDENCE="05c2b9f45c27ff96d7e58d4e8f2840c380bb440c71ba8ed0bb3d677d000e1229";
const T0="2026-10-03T20:00:00.000Z";
const T1="2026-10-03T20:01:00.000Z";
const cursor=n=>`c1.abcdefghijklmnop.${String(n).padStart(40,"0")}`;
const digest=n=>String(n).repeat(64);

// Safe incident fields copied from actual-incident-claimed-snapshot.json.
// The enclosing minimal snapshot is a deterministic test fixture, not a second live capture.
const capturedIncident={
  schema_version:1,factory_id:FACTORY,run_id:RUN,task_id:TASK,context_id:CONTEXT,
  incident_id:INCIDENT,kind:"observation_unavailable",state:"claimed",
  owner_identity:"fixture-operator",evidence_refs:[EVIDENCE],
};

function publicRun(id,taskId,contextId,pinned,graph,incidents=[]){
  return {id,task:{id:taskId,context_id:contextId},status:{state:"working",phase:"running"},started_at:T0,pinned,graph,assignments:[],artifacts:[],quality:[],decisions:[],commands:[],delivery:[],incidents,admissions:[]};
}

function capturedIncidentSnapshot(){
  const currentGraph={nodes:[{id:"current-intake",kind:"intake"}],edges:[]};
  return validateSnapshot({schema_version:1,cursor:cursor(1),captured_at:T0,freshness:{status:"fresh",observed_at:T0},state:{
    factory:{id:FACTORY,name:"Captured incident source",graph:currentGraph},
    runs:[publicRun(RUN,TASK,CONTEXT,{},currentGraph,[capturedIncident])],
    active_publication:{publication_version:"current"},capacity:null,
    commercial:{usage:[],obligations:[],payments:[]},
  }});
}

test("captured claimed incident projects its owner and kind; acknowledged stays distinct from resolved",async()=>{
  let state=createDashboardState(capturedIncidentSnapshot(),{source:"live"});
  const claimed=dashboardViewModels(state).incidents[0];
  assert.equal(claimed.incident_id,INCIDENT);
  assert.equal(claimed.kind,"observation_unavailable");
  assert.equal(claimed.owner_identity,"fixture-operator");
  assert.equal(claimed.state,"claimed");

  // Acknowledgement here is a synthetic transition check; the supplied live capture is claimed.
  const event=validateCloudEvent(makeCloudEvent({
    id:`obs-${"a".repeat(64)}`,factory_id:FACTORY,type:"com.exomachina.incident.state_changed.v1",
    subject:`incidents/${INCIDENT}`,time:T1,
    data:{...capturedIncident,state:"acknowledged"},
  }));
  state=reduceDashboard(state,{op:"event",cursor:cursor(2),event});
  const acknowledged=dashboardViewModels(state).incidents[0];
  assert.equal(acknowledged.owner_identity,"fixture-operator");
  assert.equal(acknowledged.kind,"observation_unavailable");
  assert.equal(acknowledged.state,"acknowledged");
  assert.notEqual(acknowledged.state,"resolved");

  const html=await readFile(new URL("../../../../docs/design/exomachina-floor.html",import.meta.url),"utf8");
  const start=html.indexOf("if(view==='Decisions') {");
  const end=html.indexOf("if(view==='Outputs') {",start);
  assert.notEqual(start,-1);assert.notEqual(end,-1);
  const decisions=html.slice(start,end);
  assert.match(decisions,/Failure class \$\{row\.kind\?\?'unknown'\} · Run \$\{row\.run_id\?\?'unreported'\} · Origin \$\{row\.node\?\?'unreported'\} · Owner \$\{row\.owner_identity\?\?'unreported'\} · State \$\{row\.state\?\?'unknown'\}/);
  assert.match(decisions,/detail\.textContent=JSON\.stringify\(row,null,2\)/,"the incident inspector retains the complete safe record");
  assert.doesNotMatch(decisions,/acknowledged[^\n]{0,80}resolved|resolved[^\n]{0,80}acknowledged/);
});

test("incident facts do not change prior pinned run selection; local delivery and token controls remain mounted",async()=>{
  const priorGraph={nodes:[{id:"prior-intake",kind:"intake"}],edges:[]};
  const currentGraph={nodes:[{id:"current-intake",kind:"intake"}],edges:[]};
  const priorPins={manifest_digest:digest("a"),package_digest:digest("b"),definition_digest:digest("c"),interpreter_build:"prior-build",publication_version:"prior-publication"};
  const currentPins={manifest_digest:digest("d"),package_digest:digest("e"),definition_digest:digest("f"),interpreter_build:"current-build",publication_version:"current-publication"};
  const snapshot=validateSnapshot({schema_version:1,cursor:cursor(1),captured_at:T0,freshness:{status:"fresh",observed_at:T0},state:{
    factory:{id:"selection-factory",name:"Pinned run selection",graph:currentGraph},
    runs:[
      publicRun("prior-run","prior-task","prior-context",priorPins,priorGraph),
      publicRun("current-run","current-task","current-context",currentPins,currentGraph),
    ],
    active_publication:{schema_version:1,factory_id:"selection-factory",publication_version:"current-publication",...currentPins},
    capacity:null,commercial:{usage:[],obligations:[],payments:[]},
  }});
  const state=createDashboardState(snapshot,{source:"live"});
  const prior=toFloorModel(state,{runId:"prior-run"});
  assert.equal(prior.runs.length,1);
  assert.equal(prior.runs[0].id,"prior-run");
  assert.equal(prior.runs[0].pinned.definition_digest,priorPins.definition_digest);
  assert.deepEqual(prior.steps.map(step=>step.id),["prior-intake"]);
  assert.equal(state.runs.size,2,"building the selected floor model does not discard other runs");

  const html=await readFile(new URL("../../../../docs/design/exomachina-floor.html",import.meta.url),"utf8");
  assert.match(html,/save\.textContent='Save to local destination'/);
  assert.match(html,/saveLocalDelivery\(\{endpoint:bootstrap\.deliveryEndpoint/);
  assert.match(html,/Refresh local delivery receipts/);
  assert.match(html,/Refresh token measurements/);
  assert.match(html,/addRows\('Token measurements'/);
});
