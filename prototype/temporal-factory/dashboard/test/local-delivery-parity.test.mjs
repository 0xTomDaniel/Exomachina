import test from "node:test";
import assert from "node:assert/strict";
import { makeCloudEvent, validateCloudEvent, validateSnapshot } from "../contract.mjs";
import { createDashboardState, dashboardViewModels, reduceDashboard, toFloorModel } from "../reducer.mjs";

const FACTORY="factory-delivery-check";
const RUN="run-delivery-check";
const TASK="task-delivery-check";
const CONTEXT="context-delivery-check";
const T0="2026-09-24T16:48:44.459646Z";
const T1="2026-09-24T16:49:20.083123Z";
const T2="2026-09-24T21:48:44.459646Z";
const digest=char=>char.repeat(64);
const cursor=n=>`c1.abcdefghijklmnop.${String(n).padStart(40,"0")}`;
const pins={manifest_digest:digest("a"),package_digest:digest("b"),definition_digest:digest("c"),interpreter_build:"build-1"};

function snapshot() {
  const graph={nodes:[{id:"intake",kind:"intake"},{id:"work",kind:"agent",capability:"research"}],edges:[{from:"intake",to:"work"}]};
  return validateSnapshot({
    schema_version:1,cursor:cursor(1),captured_at:T1,freshness:{status:"fresh",observed_at:T1},
    state:{
      factory:{id:FACTORY,name:"Delivery parity",fixture_label:"synthetic recorded fixture",graph},
      runs:[{
        id:RUN,task:{id:TASK,context_id:CONTEXT},status:{state:"completed",phase:"end",started_at:T0,ended_at:T1},
        started_at:T0,pinned:{...pins,model_label:"historical fixture model",fixture_label:"HTTP fixture release"},
        assignments:[],artifacts:[],quality:[],decisions:[],commands:[],
        delivery:[{receipt_id:"fixture-receipt",outcome:"fixture-received",artifact_revision:"fixture-r1",artifact_sha256:digest("d")}],
        incidents:[],admissions:[],model_label:"historical fixture model",fixture_label:"HTTP fixture release",
      }],
      active_publication:{schema_version:1,factory_id:FACTORY,publication_version:"fixture-publication",...pins},
      capacity:null,commercial:{usage:[],obligations:[],payments:[]},
    },
  });
}

function receiptData(overrides={}) {
  return {
    schema_version:1,factory_id:FACTORY,run_id:RUN,task_id:TASK,context_id:CONTEXT,
    receipt_id:"local-receipt-1",artifact_revision:"report-r1",artifact_sha256:digest("e"),
    destination_id:"local-destination-1",destination_identity:"local-identity-1",
    delivery_kind:"local_file",outcome:"local-file-delivered",delivered_at:T1,
    markdown_sha256:digest("f"),byte_length:42,
    ...overrides,
  };
}

function event(data=receiptData(), eventTime=data.delivered_at??T1) {
  return makeCloudEvent({
    id:`obs-${"1".repeat(64)}`,factory_id:FACTORY,type:"com.exomachina.delivery.receipt.v1",
    time:eventTime,subject:`runs/${data.run_id}`,data,
  });
}

test("local delivery receipt stays distinct from fixture receipt and remote customer delivery",()=>{
  const initial=snapshot();
  let state=createDashboardState(initial,{source:"recorded"});
  const frame={op:"event",cursor:cursor(2),event:event()};
  const localEvent=validateCloudEvent(frame.event);
  state=reduceDashboard(state,frame);
  const afterFirst=state;
  state=reduceDashboard(state,frame);

  assert.strictEqual(state,afterFirst,"duplicate CloudEvent must be idempotent");
  assert.equal(state.events.length,1);
  const view=dashboardViewModels(state);
  assert.equal(view.outputs.length,2,"fixture and actual local receipt remain separate facts");
  const local=view.outputs.find(row=>row.receipt_id==="local-receipt-1");
  assert.equal(local.delivery_kind,"local_file");
  assert.equal(local.destination_identity,"local-identity-1");
  assert.equal(local.markdown_sha256,digest("f"));
  assert.equal(local.byte_length,42);
  assert.equal(view.outputs.find(row=>row.receipt_id==="fixture-receipt").outcome,"fixture-received");
  assert.equal(localEvent.data.outcome,"local-file-delivered");
  assert.equal(Object.hasOwn(local,"customer_receipt"),false);

  const floor=toFloorModel(state,{runId:RUN});
  const terminal=floor.runs[0].timeline.find(row=>row.type==="end");
  assert.equal(terminal.outcome,"local-file-delivered");
  assert.notEqual(terminal.outcome,"released");
});

test("local delivery event fails closed on unsafe identity, digest, or delivery kind",()=>{
  for(const [overrides,pattern] of [
    [{destination_identity:"Remote Customer Identity"},/destination_identity/],
    [{markdown_sha256:"not-a-sha256"},/markdown_sha256/],
    [{delivery_kind:"remote_customer"},/delivery kind/],
  ]) assert.throws(()=>validateCloudEvent(event(receiptData(overrides))),pattern);
});

test("selected root Floor carries actual receipt label data while child fixture and workflow end time stay isolated",()=>{
  const wire=snapshot();
  wire.state.runs.push({
    ...structuredClone(wire.state.runs[0]),
    id:"child-run",task:{id:"child-task",context_id:"child-context"},
    delivery:[{receipt_id:"child-fixture-receipt",outcome:"fixture-received",artifact_revision:"child-fixture-r1",artifact_sha256:digest("9")}],
  });
  const validated=validateSnapshot(wire);
  const state=createDashboardState(validated,{source:"recorded"});
  const frame={op:"event",cursor:cursor(2),event:event(receiptData({delivered_at:T2}),T2)};
  const checkedEvent=validateCloudEvent(frame.event);
  const next=reduceDashboard(state,frame);

  assert.equal(checkedEvent.data.outcome,"local-file-delivered");
  assert.equal(next.runs.get(RUN).state.ended_at,T1,"receipt arrival must not rewrite workflow end time");
  const rootFloor=toFloorModel(next,{runId:RUN}).runs[0];
  assert.ok(Array.isArray(rootFloor.delivery),"selected Floor run exposes its delivery evidence");
  const rootReceipt=rootFloor.delivery.find(row=>row.receipt_id==="local-receipt-1");
  assert.equal(rootReceipt.delivery_kind,"local_file");
  assert.equal(rootReceipt.outcome,"local-file-delivered");
  assert.equal(rootReceipt.delivered_at,T2);
  const rootEnd=rootFloor.timeline.find(row=>row.type==="end");
  assert.equal(rootEnd.outcome,"local-file-delivered");
  assert.equal(rootEnd.t,(Date.parse(T1)-Date.parse(T0))/1000);

  const childFloor=toFloorModel(next,{runId:"child-run"}).runs[0];
  assert.deepEqual(childFloor.delivery.map(row=>row.receipt_id),["child-fixture-receipt"]);
  assert.equal(childFloor.timeline.find(row=>row.type==="end").outcome,"fixture-received");
});
