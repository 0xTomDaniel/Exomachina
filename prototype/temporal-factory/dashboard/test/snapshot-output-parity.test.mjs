import test from "node:test";
import assert from "node:assert/strict";
import { makeCloudEvent } from "../contract.mjs";
import { createDashboardState, dashboardViewModels, reduceDashboard } from "../reducer.mjs";

const factory="output-parity-factory",run="output-parity-run";
const time="2026-10-04T04:02:50.327Z";
const sha="a".repeat(64),otherSha="b".repeat(64);
const cursor=n=>`c1.abcdefghijklmnop.${String(n).padStart(40,"0")}`;
const common={schema_version:1,factory_id:factory,run_id:run,task_id:"task-parity",context_id:"context-parity"};
const artifact={...common,artifact_revision:"r1",artifact_sha256:sha,byte_length:3120,media_type:"application/json",author_identity:"unknown-author"};
const receipt={...common,artifact_revision:"r1",artifact_sha256:sha,receipt_id:"receipt-one",destination_id:"fixture-receiver",outcome:"fixture-received",delivered_at:time};
function snapshot(artifacts=[],delivery=[]){
  return {schema_version:1,cursor:cursor(1),captured_at:time,freshness:{status:"fresh",observed_at:time},state:{
    factory:{id:factory,name:"Output parity",graph:{nodes:[],edges:[]}},active_publication:null,capacity:null,commercial:{usage:[],obligations:[],payments:[]},
    runs:[{id:run,task:{id:"task-parity",context_id:"context-parity"},status:{state:"completed",phase:"accepted"},pinned:{manifest_digest:sha,package_digest:sha,definition_digest:sha,interpreter_build:"build-parity"},assignments:[],artifacts,quality:[],decisions:[],commands:[],delivery,incidents:[],admissions:[]}],
  }};
}
function stream(rows){
  let state=createDashboardState(snapshot());
  rows.forEach(([kind,data],index)=>{
    const event=makeCloudEvent({id:`obs-${String(index+1).padStart(64,"0")}`,factory_id:factory,type:`com.exomachina.${kind}.v1`,time,subject:`runs/${run}`,data});
    state=reduceDashboard(state,{op:"event",cursor:cursor(index+2),event});
  });
  return state;
}

test("snapshot and stream materialize repeated artifact and receipt identities equally",()=>{
  const revised={...artifact,author_identity:"verified-research"};
  const currentReceipt={...receipt,destination_id:"destination-pinned"};
  const wire=snapshot([artifact,revised,revised],[receipt,currentReceipt,currentReceipt]);
  const original=structuredClone(wire);
  const fromSnapshot=createDashboardState(wire);
  const fromStream=stream([["artifact.revised",artifact],["artifact.revised",revised],["artifact.revised",revised],["delivery.receipt",receipt],["delivery.receipt",currentReceipt],["delivery.receipt",currentReceipt]]);
  assert.deepEqual(dashboardViewModels(fromSnapshot).outputs,dashboardViewModels(fromStream).outputs);
  assert.equal(dashboardViewModels(fromSnapshot).outputs.length,2);
  assert.deepEqual(wire,original,"source snapshot is not rewritten");
  assert.equal(fromSnapshot.runs.get(run).artifacts[0].author_identity,"verified-research");
  assert.equal(fromSnapshot.runs.get(run).delivery[0].destination_id,"destination-pinned");
  assert.equal(fromSnapshot.runs.get(run).delivery[0].receipt_conflict,true);
  assert.deepEqual(fromSnapshot.runs.get(run).delivery[0].receipt_conflict_fields,["destination_id"]);
});

test("different artifact digests and distinct delivery receipts remain separate",()=>{
  const state=createDashboardState(snapshot([artifact,{...artifact,artifact_sha256:otherSha}],[receipt,{...receipt,receipt_id:"receipt-two"}]));
  assert.equal(state.runs.get(run).artifacts.length,2);
  assert.equal(state.runs.get(run).delivery.length,2);
  assert.equal(state.runs.get(run).delivery.some(row=>row.receipt_conflict),false);
  assert.equal(dashboardViewModels(state).outputs.length,4);
});

test("replaying a known output after snapshot does not add a displayed row",()=>{
  let state=createDashboardState(snapshot([artifact,artifact],[receipt,receipt]));
  for(const [index,kind,data] of [[0,"artifact.revised",artifact],[1,"delivery.receipt",receipt]]){
    const event=makeCloudEvent({id:`obs-${String(index+1).padStart(64,"0")}`,factory_id:factory,type:`com.exomachina.${kind}.v1`,time,subject:`runs/${run}`,data});
    state=reduceDashboard(state,{op:"event",cursor:cursor(index+2),event});
    state=reduceDashboard(state,{op:"event",cursor:cursor(index+2),event});
  }
  assert.equal(dashboardViewModels(state).outputs.length,2);
  assert.equal(state.runs.get(run).delivery[0].receipt_conflict,undefined);
});

test("conflicting receipt binding stays explicitly unverified across identical replay",()=>{
  const changed={...receipt,artifact_sha256:otherSha};
  const state=stream([["delivery.receipt",receipt],["delivery.receipt",changed],["delivery.receipt",changed]]);
  const rows=dashboardViewModels(state).outputs;
  assert.equal(rows.length,1);
  assert.equal(rows[0].receipt_conflict,true);
  assert.deepEqual(rows[0].receipt_conflict_fields,["artifact_sha256"]);
});
