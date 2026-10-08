import test from "node:test";
import assert from "node:assert/strict";
import { makeCloudEvent } from "../contract.mjs";
import { createDashboardState, reduceDashboard, toFloorModel } from "../reducer.mjs";

// The A2A release receipt fact the Runtime source emits exactly once per delivery.
const T0="2026-10-08T10:00:00.000Z", T1="2026-10-08T10:00:03.000Z";
const factory="release-factory", run="child-run";
const sha="a".repeat(64), otherSha="b".repeat(64);
const pins={manifest_digest:"c".repeat(64),package_digest:"d".repeat(64),definition_digest:"e".repeat(64),interpreter_build:"build-1"};
const graph={nodes:[{id:"publish",kind:"release",output:"artifacts"},{id:"done",kind:"complete"}],edges:[{from:"publish",to:"done",kind:"control"}]};
const receipt={schema_version:1,factory_id:factory,run_id:run,task_id:"task-one",context_id:"context-one",
  receipt_id:"4d1b8a8e-4c55-4b8e-9d0e-1f7c2a3b5c6d",artifact_revision:"r1",artifact_sha256:sha,
  destination_id:"a2a-card-0123456789abcdef01234567",delivered_at:"2026-10-08T10:00:01.250Z",outcome:"delivered"};
const cursor=n=>`c1.abcdefghijklmnop.${String(n).padStart(40,"0")}`;

function stream(rows){
  let state=createDashboardState({schema_version:1,cursor:cursor(1),captured_at:T1,freshness:{status:"fresh",observed_at:T1},state:{
    factory:{id:factory,name:"Release factory",graph},active_publication:{publication_version:"v1",...pins},capacity:null,commercial:{usage:[],payments:[],obligations:[]},
    runs:[{id:run,task:{id:"task-one",context_id:"context-one"},started_at:T0,status:{state:"completed",phase:"accepted",ended_at:T1},pinned:pins,assignments:[],artifacts:[],quality:[],commands:[],decisions:[],delivery:[],incidents:[],admissions:[]}]}},{source:"live"});
  rows.forEach((data,index)=>{
    const event=makeCloudEvent({id:`obs-${String(index+1).padStart(64,"0")}`,factory_id:factory,type:"com.exomachina.delivery.receipt.v1",time:data.delivered_at,subject:`runs/${run}`,data});
    state=reduceDashboard(state,{op:"event",cursor:cursor(index+2),event});
  });
  return state;
}
const outcome=state=>toFloorModel(state).runs[0].timeline.at(-1).outcome;

test("one A2A release receipt, replayed identically, proves delivery without a conflict",()=>{
  const state=stream([receipt,receipt]);
  const rows=state.runs.get(run).delivery;
  assert.equal(rows.length,1);
  assert.equal(rows[0].receipt_conflict,undefined);
  assert.equal(outcome(state),"released");
});

test("genuinely conflicting facts for one receipt id stay delivery-unverified",()=>{
  for(const change of [{artifact_sha256:otherSha},{destination_id:"another-destination"},{delivered_at:"2026-10-08T10:00:09.000Z"}]){
    const state=stream([receipt,{...receipt,...change}]);
    const rows=state.runs.get(run).delivery;
    assert.equal(rows.length,1);
    assert.equal(rows[0].receipt_conflict,true);
    assert.deepEqual(rows[0].receipt_conflict_fields,Object.keys(change));
    assert.equal(outcome(state),"delivery-unverified");
  }
});

test("distinct deliveries keep distinct receipts",()=>{
  const state=stream([receipt,{...receipt,receipt_id:"second-receipt",artifact_revision:"r2",artifact_sha256:otherSha}]);
  assert.equal(state.runs.get(run).delivery.length,2);
  assert.equal(state.runs.get(run).delivery.some(row=>row.receipt_conflict),false);
});
