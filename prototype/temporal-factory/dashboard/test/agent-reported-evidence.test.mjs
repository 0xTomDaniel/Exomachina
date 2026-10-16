import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { makeCloudEvent } from "../contract.mjs";
import { createDashboardState, dashboardViewModels, deliveryEvidence, reduceDashboard, toFloorModel } from "../reducer.mjs";
import { measurementGroups, validateMeasurements } from "../usage.mjs";

// Agent-reported usage and delivery evidence (operator decision, 8 Oct 2026).
const T0="2026-10-08T10:00:00.000Z", T1="2026-10-08T10:00:03.000Z";
const factory="evidence-factory", run="evidence-run";
const sha="a".repeat(64), otherSha="b".repeat(64);
const pins={manifest_digest:"c".repeat(64),package_digest:"d".repeat(64),definition_digest:"e".repeat(64),interpreter_build:"build-1"};
const graph={nodes:[{id:"publish",kind:"release",output:"artifacts"},{id:"done",kind:"complete"}],edges:[{from:"publish",to:"done",kind:"control"}]};
const cursor=n=>`c1.abcdefghijklmnop.${String(n).padStart(40,"0")}`;
const receipt={schema_version:1,factory_id:factory,run_id:run,task_id:"task-one",context_id:"context-one",
  receipt_id:"receipt-one",artifact_revision:"r1",artifact_sha256:sha,
  destination_id:"a2a-card-0123456789abcdef01234567",delivered_at:"2026-10-08T10:00:01.250Z",outcome:"delivered"};
const verdict={schema_version:1,factory_id:factory,run_id:run,task_id:"task-one",artifact_revision:"r1",artifact_sha256:sha,
  reviewer_identity:"quality-reviewer",accepted:true,finding_count:0};

function snapshot({commercial={usage:[],payments:[],obligations:[]},agent_bindings}={}){
  return {schema_version:1,cursor:cursor(1),captured_at:T1,freshness:{status:"fresh",observed_at:T1},state:{
    factory:{id:factory,name:"Evidence factory",graph,...(agent_bindings?{agent_bindings}:{})},active_publication:{publication_version:"v1",...pins},capacity:null,commercial,
    runs:[{id:run,task:{id:"task-one",context_id:"context-one"},started_at:T0,status:{state:"completed",phase:"accepted",ended_at:T1},pinned:pins,assignments:[],artifacts:[],quality:[],commands:[],decisions:[],delivery:[],incidents:[],admissions:[]}]}};
}
function stream(rows){
  let state=createDashboardState(snapshot(),{source:"live"});
  rows.forEach(([type,data],index)=>{
    const event=makeCloudEvent({id:`obs-${String(index+1).padStart(64,"0")}`,factory_id:factory,type:`com.exomachina.${type}.v1`,time:data.delivered_at??"2026-10-08T10:00:01.000Z",subject:`runs/${run}`,data});
    state=reduceDashboard(state,{op:"event",cursor:cursor(index+2),event});
  });
  return state;
}
const floorRun=state=>toFloorModel(state).runs[0];
const outcome=state=>floorRun(state).timeline.find(row=>row.type==="end").outcome;
const released=state=>floorRun(state).timeline.some(row=>row.type==="release");

test("delivery is verified only when the one receipt's sha256 equals the accepted revision's",()=>{
  const state=stream([["quality.verdict",verdict],["delivery.receipt",receipt]]);
  assert.equal(state.runs.get(run).delivery.length,1);
  assert.deepEqual(floorRun(state).deliveryEvidence,{status:"verified",receipts:1});
  assert.equal(outcome(state),"released");
  assert.equal(released(state),true);
});

test("a receipt with no accepted revision observed is never shown as verified",()=>{
  const evidence=floorRun(stream([["delivery.receipt",receipt]])).deliveryEvidence;
  assert.equal(evidence.status,"unverified");
  assert.notEqual(deliveryEvidence({delivery:[receipt],quality:[{...verdict,accepted:false}]}).status,"verified");
});

test("a receipt whose sha256 differs from the accepted revision is a delivery fault, not delivered",()=>{
  for(const change of [{artifact_sha256:otherSha},{artifact_revision:"r2",artifact_sha256:otherSha}]){
    const state=stream([["quality.verdict",verdict],["delivery.receipt",{...receipt,...change}]]);
    const evidence=floorRun(state).deliveryEvidence;
    assert.equal(evidence.status,"fault");
    assert.match(evidence.reason,/differs from the accepted revision/);
    assert.equal(outcome(state),"delivery-fault");
    assert.equal(released(state),false,"a faulted carrier never leaves the floor as delivered");
  }
});

test("more than one receipt for one delivery is a delivery fault, not delivered",()=>{
  const state=stream([["quality.verdict",verdict],["delivery.receipt",receipt],["delivery.receipt",{...receipt,receipt_id:"receipt-two",delivered_at:"2026-10-08T10:00:02.000Z"}]]);
  const evidence=floorRun(state).deliveryEvidence;
  assert.deepEqual(evidence,{status:"fault",receipts:2,reason:"more than one receipt for one delivery"});
  assert.equal(outcome(state),"delivery-fault");
  assert.equal(released(state),false);
  const html=readFileSync(new URL("../../../../docs/design/exomachina-floor.html",import.meta.url),"utf8");
  assert.match(html,/'delivery-fault':'Delivery fault'/,"the floor labels the fault instead of 'Delivered'");
});

const categories=["input_tokens","output_tokens","cache_read_tokens","cache_write_tokens","total_tokens"];
const unavailable=Object.fromEntries(categories.map(name=>[name,{status:"unavailable",value:null}]));
const agentRow={measurement_id:"agent-measurement",model_call_id:"urn:exomachina:agent-task:agent-1:task-1",recorded_at:"2026-10-08T10:00:00Z",
  call_scope:"assignment_call",provider:"a2a-agent",model_id:"unreported",reasoning_effort:null,unit:"tokens",
  measurement_source:"agent_reported",completeness:"partial",evidence_status:"agent_reported",
  usage:{...structuredClone(unavailable),input_tokens:{status:"reported",value:7},output_tokens:{status:"reported",value:5}},
  service_identity:"agent-1",task_id:"task-1",run_id:"run-1",assignment_id:"assignment-1",attempt_id:"attempt-1"};
const directorRow={...agentRow,measurement_id:"director-measurement",model_call_id:"director-call-1",call_scope:"director_call",
  provider:"codex",model_id:"gpt-6-luna",reasoning_effort:"xhigh",measurement_source:"provider_reported",evidence_status:"provider_reported",
  service_identity:null,assignment_id:null,attempt_id:null};

test("agent usage is labelled agent-reported and never shows the agent's model, provider, or model calls",()=>{
  const [agent,director]=validateMeasurements([agentRow,directorRow]);
  assert.equal(agent.usage_label,"agent-reported");
  for(const key of ["provider","model_id","model_call_id","reasoning_effort"])assert.equal(Object.hasOwn(agent,key),false,key);
  assert.doesNotMatch(JSON.stringify(agent),/a2a-agent|unreported|urn:exomachina:agent-task/);
  assert.equal(director.model_id,"gpt-6-luna","the factory's own Director calls keep their model");
  assert.equal(director.provider,"codex");
  assert.equal(director.model_call_id,"director-call-1");
  assert.equal(Object.hasOwn(director,"usage_label"),false);
  // The renderer regroups already-validated rows; the projection must survive that pass.
  const groups=measurementGroups([agent,director]);
  assert.deepEqual(groups.flatMap(group=>group.measurements),[agent,director]);
  assert.throws(()=>validateMeasurements([{...agent,model_id:"leaked-model"}]),/not permitted/);
  assert.throws(()=>validateMeasurements([{...director,usage_label:"agent-reported"}]),/usage_label/);
});

test("token categories an agent did not report stay unavailable, never zero",()=>{
  const [agent]=validateMeasurements([agentRow]);
  for(const name of ["cache_read_tokens","cache_write_tokens","total_tokens"])assert.deepEqual(agent.usage[name],{status:"unavailable",value:null});
  assert.equal(agent.usage.input_tokens.value,7);
  const html=readFileSync(new URL("../../../../docs/design/exomachina-floor.html",import.meta.url),"utf8");
  assert.match(html,/row\.usage_label\?\?row\.model_id\?\?'unknown'/,"the measurement table shows the agent-reported label in place of a model");
});

test("commercial usage stays tokens; money appears only from a reported amount or payment",()=>{
  const usage=[{run_id:run,assignment_id:"assignment-1",attempt_id:"attempt-1",usage_id:"usage-1",unit:"tokens",quantity:"12",
    measurement_source:"agent_reported",completeness:"complete",evidence_status:"measured",model_id:"unreported",model_call_id:"agent-call-1",service_identity:"agent-1"}];
  const obligations=[{run_id:run,assignment_id:"assignment-1",attempt_id:"attempt-1",obligation_id:"obligation-1",component:"supplier_charge",amount_atoms:null,currency:"USD",atomic_scale:6,evidence_status:"unknown",state:"recorded"}];
  const view=dashboardViewModels(createDashboardState(snapshot({commercial:{usage,obligations,payments:[]}}),{source:"live"}));
  const [row]=view.commercial.usage;
  assert.equal(row.usage_label,"agent-reported");
  for(const key of ["model_id","model_call_id","reasoning_effort"])assert.equal(Object.hasOwn(row,key),false,key);
  assert.equal(row.quantity_display,"12 tokens");
  assert.doesNotMatch(JSON.stringify(row),/amount|cost|USD|currency/,"a token count is never a cost");
  assert.equal(view.commercial.obligations[0].amount_known,false);
  assert.equal(view.commercial.obligations[0].amount_display,"Unknown");
  assert.deepEqual(view.commercial.payments,[]);
});

test("the live floor shows no agent-side occupancy",()=>{
  const state=createDashboardState(snapshot({agent_bindings:[{capability:"research",name:"Research agent",identity:"agent-1",contract_digest:"f".repeat(64)}]}),{source:"live"});
  for(const agent of Object.values(toFloorModel(state).agents)){
    for(const key of ["others","occupancy","active_count","queued_count"])assert.equal(Object.hasOwn(agent,key),false,key);
  }
});
