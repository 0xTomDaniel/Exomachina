import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import path from "node:path";
import {
  DASHBOARD_EVENT_SCHEMA, makeCloudEvent, validateBundle, validateCloudEvent,
  validateServerMessage, validateSnapshot, verifyArtifactBytes,
} from "../contract.mjs";
import { createDashboardState, dashboardViewModels, reduceDashboard, toFloorModel } from "../reducer.mjs";
import { createDemoAdapter } from "../adapters/demo.mjs";
import { createRecordedAdapter } from "../adapters/recorded.mjs";
import { createLiveAdapter } from "../adapters/live.mjs";

const FACTORY="factory-historical";
const RUN1="run-one";
const RUN2="run-two";
const TASK1="task-one";
const CONTEXT1="context-one";
const T0="2026-09-24T16:48:44.459646Z";
const T1="2026-09-24T16:49:20.083123Z";
const D1="a".repeat(64);
const D2="b".repeat(64);
const cursor=n=>`c1.abcdefghijklmnop.${String(n).padStart(40,"0")}`;
const pins={manifest_digest:D1,package_digest:D2,definition_digest:"c".repeat(64),interpreter_build:"build-1"};

function publicRun(id=RUN1,startedAt=T0){
  const row={id,task:{id:TASK1,context_id:CONTEXT1},status:{state:"working",phase:"started"},pinned:{...pins},assignments:[],artifacts:[],quality:[],decisions:[],commands:[],delivery:[],incidents:[],admissions:[]};
  if(startedAt)row.started_at=startedAt;
  return row;
}
function snapshot({runs=[publicRun()],commercial={usage:[],obligations:[],payments:[]},graph={nodes:[{id:"intake",kind:"intake",name:"Intake"},{id:"research",kind:"agent",capability:"research"}],edges:[{from:"intake",to:"research"}]},cursorValue=cursor(1)}={}){
  return {schema_version:1,cursor:cursorValue,captured_at:T0,freshness:{status:"fresh",observed_at:T0},state:{factory:{id:FACTORY,name:"Historical factory",graph},runs,active_publication:{publication_version:"v1",...pins},capacity:null,commercial}};
}
let eventNo=0;
function cloudEvent(type,data,time=T0){
  const suffix=(++eventNo).toString(16).padStart(64,"0");
  return makeCloudEvent({id:`obs-${suffix}`,factory_id:FACTORY,type,time,subject:`runs/${data.run_id??RUN1}`,data:{schema_version:1,factory_id:FACTORY,...data}});
}
function eventFrame(event,n=eventNo){return {op:"event",cursor:cursor(n+1),event};}
function bundleFor(frames=[],snap=snapshot({runs:[]})){
  return {schema_version:1,source:"recorded",snapshot:snap,frames,provenance:{label:"Historical · gpt-6-sol · partial",model_label:"gpt-6-sol",fixture_label:"HTTP fixture release",release_label:"http-release (fixture)",defect_labels:["historical induced defect evidence"],completeness:"partial",evidence_ref:"synthetic unit-test bundle"}};
}
const eventType=suffix=>`com.exomachina.${suffix}.v1`;

test("validates the normalized public Observation snapshot and opaque scoped cursors",()=>{
  const value=snapshot();
  assert.equal(validateSnapshot(value).state.factory.id,FACTORY);
  assert.equal(value.factory_id,undefined);
  const withoutOptionalStarted=publicRun(RUN1,null);assert.equal(validateSnapshot(snapshot({runs:[withoutOptionalStarted]})).state.runs[0].started_at,undefined);
  assert.throws(()=>validateSnapshot({...value,cursor:"c1.short.cursor"}),/cursor/);
  assert.throws(()=>validateSnapshot({...value,factory_id:FACTORY}),/snapshot fields/);
  const graph=value.state.factory.graph;assert.equal(graph.nodes[0].kind,"intake");assert.equal("type" in graph.nodes[0],false);
});

test("actual public publication and artifact records retain schema and factory bindings",()=>{
  const graph_nodes=[{id:"intake",type:"intake",next:["research"]},{id:"research",type:"agent",capability:"research",next:[]}];
  const service_bindings=[{name:"Research",role:"research",identity:"service-research",capability:"research",contract_digest:D1}];
  const publication={schema_version:1,factory_id:FACTORY,publication_version:"v2",...pins,graph_nodes,service_bindings};
  const artifact={schema_version:1,factory_id:FACTORY,run_id:RUN1,task_id:TASK1,artifact_revision:"report-r1",artifact_sha256:D1,media_type:"text/markdown",byte_length:128,author_identity:"author-1"};
  const value=snapshot();value.state.active_publication=publication;value.state.runs[0].artifacts=[artifact];
  const bundle=validateBundle(bundleFor([],value));
  const initial=createDashboardState(bundle.snapshot);
  assert.equal(toFloorModel(initial).steps.length,2);
  assert.equal(toFloorModel(initial).agents.research.identity,"service-research");

  const activated=validateCloudEvent(cloudEvent(eventType("publication.activated"),publication));
  const revised=validateCloudEvent(cloudEvent(eventType("artifact.revised"),{run_id:RUN1,task_id:TASK1,artifact_revision:"report-r2",artifact_sha256:D2,previous_revision:"report-r1",previous_sha256:D1,media_type:"text/markdown",byte_length:256,author_identity:"author-1"}));
  let state=reduceDashboard(initial,eventFrame(activated,90));
  state=reduceDashboard(state,eventFrame(revised,91));
  assert.equal(state.factory.graph.nodes.length,2);
  assert.equal(state.factory.agent_bindings[0].role,"research");
  assert.equal(state.runs.get(RUN1).artifacts.at(-1).artifact_revision,"report-r2");
  assert.equal(toFloorModel(state).steps.length,2);
});

test("pinned childgraph maps quality to a gate and distinct assignments share a parallel node",()=>{
  const run=publicRun();
  run.graph={nodes:[{id:"parallel",kind:"parallel"},{id:"quality",kind:"quality"},{id:"output",kind:"output"}],edges:[{from:"parallel",to:"quality"},{from:"quality",to:"output"}]};
  const state=createDashboardState(snapshot({runs:[run],graph:{nodes:[],edges:[]}}));
  const assignments=["assignment-a","assignment-b"].map((assignment_id,index)=>cloudEvent(eventType("assignment.state_changed"),{
    run_id:RUN1,task_id:TASK1,assignment_id,attempt_id:`attempt-${index+1}`,capability:"research",node:"parallel",state:"completed",started_at:T0,ended_at:T1,
  }));
  let next=state;
  assignments.forEach((event,index)=>{validateCloudEvent(event);next=reduceDashboard(next,eventFrame(event,100+index));});
  const model=toFloorModel(next,{runId:RUN1});
  assert.deepEqual(model.steps.map(step=>step.id),["parallel","quality","output"]);
  assert.equal(model.steps.find(step=>step.id==="quality").kind,"gate");
  const work=model.runs[0].timeline.filter(event=>event.type==="work"&&event.step==="parallel");
  assert.equal(work.length,2);
  assert.equal(new Set(work.map(event=>event.item)).size,2);
  assert.ok(work.every(event=>event.job===RUN1));
});

test("accepts public agent binding roles and rejects unsafe or undeclared binding fields",()=>{
  const value=snapshot();
  const binding={name:"Research",role:"research",identity:"service-research",capability:"research",contract_digest:D1};
  value.state.factory.agent_bindings=[binding];
  assert.equal(validateSnapshot(value).state.factory.agent_bindings[0].role,"research");
  for(const role of [42,null,{name:"research"},"x".repeat(10000)]){
    value.state.factory.agent_bindings=[{...binding,role}];
    assert.throws(()=>validateSnapshot(value),/role/);
  }
  value.state.factory.agent_bindings=[{...binding,prompt:"private"}];
  assert.throws(()=>validateSnapshot(value),/prompt/);
});

test("requires exact quantity/completeness pairing and the independent evidence enum",()=>{
  const common={run_id:RUN1,task_id:TASK1,context_id:CONTEXT1,assignment_id:"assignment-1",attempt_id:"attempt-1",usage_id:"usage-1",unit:"tokens"};
  for(const evidence_status of ["measured","calculated_from_measured_usage","provider_reported","estimated","unknown","undisclosed"]){
    const e=cloudEvent(eventType("commercial.usage"),{...common,completeness:"complete",quantity:"0",evidence_status});
    assert.equal(validateCloudEvent(e).data.evidence_status,evidence_status);
  }
  assert.throws(()=>cloudEvent(eventType("commercial.usage"),{...common,completeness:"complete",quantity:null,evidence_status:"measured"}),/quantity/);
  assert.throws(()=>cloudEvent(eventType("commercial.usage"),{...common,completeness:"unknown",quantity:"1.25",evidence_status:"measured"}),/complete/);
  assert.throws(()=>cloudEvent(eventType("commercial.usage"),{...common,completeness:"complete",evidence_status:"measured"}),/omitted/);
  assert.throws(()=>cloudEvent(eventType("commercial.usage"),{...common,completeness:"complete",quantity:"1",evidence_status:"calculated"}),/enum/);
  const unknown=cloudEvent(eventType("commercial.usage"),{...common,usage_id:"usage-unknown",completeness:"undisclosed",evidence_status:"unknown"});
  assert.equal("quantity" in validateCloudEvent(unknown).data,false);
});

test("commercial snapshot and reducer preserve unknown separately from zero",()=>{
  const usageUnknown={run_id:RUN1,assignment_id:"assignment-1",attempt_id:"attempt-1",usage_id:"usage-unknown",unit:"tokens",completeness:"unknown",evidence_status:"unknown",service_identity:"svc-1",model_call_id:"call-1"};
  const obligationUnknown={run_id:RUN1,assignment_id:"assignment-1",attempt_id:"attempt-1",obligation_id:"obligation-unknown",component:"inference_cost",amount_atoms:null,currency:"USD",atomic_scale:6,evidence_status:"unknown",state:"pending"};
  const obligationZero={run_id:RUN1,assignment_id:"assignment-1",attempt_id:"attempt-1",obligation_id:"obligation-zero",component:"customer_price",amount_atoms:0,currency:"USD",atomic_scale:6,evidence_status:"provider_reported",state:"paid"};
  const snap=snapshot({commercial:{usage:[usageUnknown],obligations:[obligationUnknown,obligationZero],payments:[]}});
  validateSnapshot(snap);
  const usageEvent=cloudEvent(eventType("commercial.usage"),{schema_version:1,factory_id:FACTORY,...usageUnknown,usage_id:"usage-zero",quantity:"0",completeness:"complete",evidence_status:"calculated_from_measured_usage"});
  const unknownEvent=cloudEvent(eventType("commercial.usage"),{schema_version:1,factory_id:FACTORY,...usageUnknown,completeness:"undisclosed",evidence_status:"unknown"});
  const zeroEvent=cloudEvent(eventType("commercial.obligation"),{schema_version:1,factory_id:FACTORY,run_id:RUN1,assignment_id:"assignment-1",attempt_id:"attempt-1",obligation_id:"obligation-zero",component:"customer_price",amount_atoms:0,currency:"USD",atomic_scale:6,evidence_status:"provider_reported",state:"paid"});
  const nullEvent=cloudEvent(eventType("commercial.obligation"),{schema_version:1,factory_id:FACTORY,run_id:RUN1,assignment_id:"assignment-1",attempt_id:"attempt-1",obligation_id:"obligation-unknown",component:"inference_cost",amount_atoms:null,currency:"USD",atomic_scale:6,evidence_status:"undisclosed",state:"unavailable"});
  assert.throws(()=>cloudEvent(eventType("commercial.obligation"),{schema_version:1,factory_id:FACTORY,run_id:RUN1,assignment_id:"assignment-1",attempt_id:"attempt-1",obligation_id:"bad-null",component:"inference_cost",amount_atoms:null,currency:"USD",atomic_scale:6,evidence_status:"measured",state:"pending"}),/unknown or undisclosed/);
  assert.throws(()=>cloudEvent(eventType("commercial.obligation"),{schema_version:1,factory_id:FACTORY,run_id:RUN1,assignment_id:"assignment-1",attempt_id:"attempt-1",obligation_id:"bad-component",component:"misc",amount_atoms:1,currency:"USD",atomic_scale:6,evidence_status:"measured",state:"pending"}),/component/);
  const state=[usageEvent,unknownEvent,zeroEvent,nullEvent].reduce((s,e,i)=>reduceDashboard(s,eventFrame(e,i+2)),createDashboardState(snapshot({commercial:{usage:[],obligations:[],payments:[]}})));
  const vm=dashboardViewModels(state);
  const measured=vm.commercial.usage.find(row=>row.completeness==="complete");
  const omitted=vm.commercial.usage.find(row=>row.completeness==="undisclosed");
  assert.equal(measured.quantity,"0");assert.equal(measured.quantity_known,true);assert.equal(measured.evidence_status,"calculated_from_measured_usage");
  assert.equal("quantity" in omitted,false);assert.equal(omitted.quantity_known,false);assert.equal(omitted.quantity_display,"Undisclosed");
  const zero=vm.commercial.obligations.find(row=>row.component==="customer_price");
  const unknown=vm.commercial.obligations.find(row=>row.component==="inference_cost");
  assert.equal(zero.amount_atoms,0);assert.equal(zero.amount_known,true);assert.equal(zero.state,"paid");
  assert.equal(unknown.amount_atoms,null);assert.equal(unknown.amount_known,false);assert.equal(unknown.amount_display,"Undisclosed");
  assert.equal(vm.commercial.obligationsByComponent.inference_cost.length,1);
});

test("strict CloudEvent allowlist rejects prompt, content, and unknown fields",()=>{
  const base={schema_version:1,factory_id:FACTORY,run_id:RUN1,state:"working",phase:"started"};
  for(const key of ["prompt","content","private_annotation"]){const data={...base,[key]:"must not pass"};const e={specversion:"1.0",id:`obs-${(++eventNo).toString(16).padStart(64,"0")}`,source:`/factories/${FACTORY}`,type:eventType("run.state_changed"),time:T0,subject:`runs/${RUN1}`,datacontenttype:"application/json",dataschema:DASHBOARD_EVENT_SCHEMA,data};assert.throws(()=>validateCloudEvent(e),new RegExp(key));}
});

test("resume, checkpoint, duplicate delivery, and retention gaps preserve opaque cursors",()=>{
  const snap=snapshot();let state=createDashboardState(snap,{source:"live"});
  const resumed={op:"resumed",after_cursor:snap.cursor,continuation_cursor:cursor(9)};
  state=reduceDashboard(state,resumed);assert.equal(state.cursor,resumed.continuation_cursor);
  const checkpoint={op:"checkpoint",cursor:cursor(10)};state=reduceDashboard(state,checkpoint);assert.equal(state.cursor,checkpoint.cursor);
  const created=cloudEvent(eventType("run.created"),{run_id:RUN2,task_id:TASK1,context_id:CONTEXT1,...pins,state:"working",phase:"started"});
  const frame=eventFrame(created,11);state=reduceDashboard(state,frame);const afterFirst=state;
  state=reduceDashboard(state,frame);assert.equal(state,afterFirst);assert.equal(state.events.length,1);assert.equal(state.runs.get(RUN2).task.context_id,CONTEXT1);
  const gap=reduceDashboard(state,{op:"resync_required",reason:"retention_expired",minimum_cursor:cursor(12),latest_cursor:cursor(13),message:"retention window moved"});
  assert.equal(gap.transport.gap,true);assert.equal(gap.freshness.status,"stale");
  const recovered=reduceDashboard(gap,{op:"snapshot",snapshot:snapshot({cursorValue:cursor(14)})});
  assert.equal(recovered.transport.status,"connected");assert.equal(recovered.freshness.status,"fresh");assert.equal(recovered.transport.gap,true);
});

test("run start time survives later state changes without started_at",()=>{
  const endedAt="2026-09-24T16:50:00.000000Z";
  let state=createDashboardState(snapshot({runs:[]}),{source:"live"});
  const start=cloudEvent(eventType("run.state_changed"),{run_id:RUN1,task_id:TASK1,context_id:CONTEXT1,state:"working",phase:"started",started_at:T0},T0);
  state=reduceDashboard(state,eventFrame(start,31));
  const end=cloudEvent(eventType("run.state_changed"),{run_id:RUN1,task_id:TASK1,context_id:CONTEXT1,state:"completed",phase:"end",ended_at:endedAt},endedAt);
  state=reduceDashboard(state,eventFrame(end,32));
  const run=state.runs.get(RUN1),floorRun=toFloorModel(state).runs[0];
  assert.equal(run.started_at,T0);
  assert.equal(floorRun.start,T0);
  assert.equal(floorRun.startKnown,true);
  assert.equal(floorRun.timeline.length,1);
  assert.equal(floorRun.timeline[0].type,"end");
  assert.equal(floorRun.timeline[0].t,(Date.parse(endedAt)-Date.parse(T0))/1000);
});

test("recorded adapter run filter and callback fanout deliver each frame once",()=>{
  const e1=cloudEvent(eventType("run.state_changed"),{run_id:RUN1,state:"working",phase:"step"},T0);
  const e2=cloudEvent(eventType("run.state_changed"),{run_id:RUN2,state:"working",phase:"step"},T0);
  const e3=cloudEvent(eventType("run.state_changed"),{run_id:RUN1,state:"accepted",phase:"done"},T1);
  const bundle=validateBundle(bundleFor([eventFrame(e1,21),eventFrame(e2,22),eventFrame(e3,23)]));
  const transportFrames=[],observedFrames=[];
  const adapter=createRecordedAdapter(bundle,{onFrame:frame=>transportFrames.push(frame)});
  const sub=adapter.observe(FACTORY,null,RUN1,frame=>observedFrames.push(frame));
  sub.next();sub.play();
  assert.deepEqual(transportFrames.map(frame=>frame.op),["snapshot","event","event"]);
  assert.deepEqual(observedFrames.map(frame=>frame.op),["snapshot","event","event"]);
  assert.deepEqual(transportFrames,observedFrames);
  const resume=adapter.observe(FACTORY,eventFrame(e1,21).cursor,RUN1);assert.equal(resume.next().event.id,e3.id);
  assert.equal(resume.next(),null);
});

test("demo and recorded bundles use the same validated reducer and graph renderer model",async()=>{
  const fixture={id:"demo-factory",name:"Demo",provenance:"isolated",steps:[{id:"intake",kind:"intake"},{id:"agent",kind:"agent",agent:"research"}],edges:[{from:"intake",to:"agent"}],runs:[{id:"demo-run",events:[{t:0,type:"job"},{t:1,type:"work",step:"agent",dur:1},{t:2,type:"end",outcome:"released"}]}]};
  const frames=[],observed=[];const adapter=await createDemoAdapter({fixtures:[fixture],onFrame:frame=>frames.push(frame)});
  const [factory]=await adapter.discover();assert.equal(factory.id,"demo-factory");
  const demoSnapshot=validateSnapshot(await adapter.snapshot(factory.id),{source:"demo"});
  const demoRun=demoSnapshot.state.runs.find(row=>row.id==="demo-run");assert.ok(demoRun.task.id.startsWith("demo-task-"));assert.ok(demoRun.task.context_id.startsWith("demo-context-"));
  const subscription=adapter.observe(factory.id,null,"demo-run",frame=>observed.push(frame));subscription.play();
  let demoState={source:"demo"};for(const frame of frames){validateServerMessage(frame,{source:"demo"});demoState=reduceDashboard(demoState,frame);}
  assert.equal(toFloorModel(demoState).id,"demo-factory");assert.equal(demoState.source,"demo");
  assert.ok(frames.length>1);
  const binding={op:"command",factory_id:factory.id,command_id:"demo-command-one",task_id:demoRun.task.id,context_id:demoRun.task.context_id,action:"approve",expected_state:demoState.runs.get("demo-run").state.state};
  const first=await adapter.command(binding);assert.equal(first.lifecycle,"rejected");assert.match(first.reason,/terminal/);assert.deepEqual(first.frames.map(frame=>frame.op),["command_ack","event"]);assert.equal(first.transition,null);
  const commandData=first.outcome.event.data;assert.equal(commandData.run_id,"demo-run");assert.equal(commandData.task_id,demoRun.task.id);assert.equal(commandData.context_id,demoRun.task.context_id);
  validateServerMessage(first.received);validateServerMessage(first.outcome);
  const afterCommand=frames.length;const repeated=await adapter.command(binding);assert.equal(repeated.duplicate,true);assert.equal(repeated.outcome.event.id,first.outcome.event.id);assert.equal(frames.length,afterCommand);
  const conflict=await adapter.command({...binding,action:"abort"});assert.equal(conflict.lifecycle,"rejected");assert.equal(conflict.frames[0].op,"error");assert.equal(frames.length,afterCommand);
  const mismatch=await adapter.command({...binding,command_id:"demo-command-mismatch",expected_state:"working"});assert.equal(mismatch.lifecycle,"rejected");assert.equal(mismatch.outcome.event.data.lifecycle,"rejected");assert.equal(mismatch.outcome.event.data.run_id,"demo-run");
  demoState=reduceDashboard(demoState,first.outcome);assert.equal(dashboardViewModels(demoState).commands.find(row=>row.command_id===binding.command_id).context_id,demoRun.task.context_id);
  const secondFrames=[],secondObserved=[];const secondAdapter=await createDemoAdapter({fixtures:[fixture],onFrame:frame=>secondFrames.push(frame)});const [secondFactory]=await secondAdapter.discover();const secondSnapshot=await secondAdapter.snapshot(secondFactory.id);secondAdapter.observe(secondFactory.id,null,"demo-run",frame=>secondObserved.push(frame));
  const secondResult=await secondAdapter.command(binding);assert.equal(secondResult.lifecycle,"rejected");assert.match(secondResult.reason,/terminal/);assert.equal(secondResult.duplicate,undefined);assert.deepEqual(secondObserved.slice(-2).map(frame=>frame.op),["command_ack","event"]);assert.equal(observed.at(-1).event.id,mismatch.outcome.event.id);assert.equal(frames.length,afterCommand+2);assert.equal(secondSnapshot.state.factory.id,factory.id);
  assert.equal((await adapter.submit({text:"hello"})).lifecycle,"local_only");
});

test("repeated Demo work at one step preserves separate assignment identities",async()=>{
  const fixture={id:"demo-repeat",name:"Repeated work",steps:[{id:"worker",kind:"agent",agent:"research"}],runs:[{id:"demo-run",events:[{t:0,type:"job"},{t:1,type:"work",step:"worker",dur:1},{t:3,type:"work",step:"worker",dur:1},{t:5,type:"end",outcome:"released"}]}]};
  let state={source:"demo"};
  const adapter=await createDemoAdapter({fixtures:[fixture],onFrame:frame=>{state=reduceDashboard(state,frame);}});
  adapter.observe("demo-repeat");
  assert.equal(Object.keys(state.runs.get("demo-run").assignments).length,2);
  const spawns=toFloorModel(state).runs[0].timeline.filter(event=>event.type==="spawn");
  assert.equal(new Set(spawns.map(event=>event.item)).size,2);
  assert.deepEqual(spawns.map(event=>event.t),[1,3]);
});

test("Demo decisions and chat use no fetch or WebSocket transport",async()=>{
  const originalFetch=globalThis.fetch,originalWebSocket=Object.getOwnPropertyDescriptor(globalThis,"WebSocket");let fetchCalls=0,socketCalls=0;
  globalThis.fetch=()=>{fetchCalls++;throw new Error("network must not be used by Demo");};
  Object.defineProperty(globalThis,"WebSocket",{configurable:true,writable:true,value:class{constructor(){socketCalls++;throw new Error("WebSocket must not be used by Demo");}}});
  try{
    const adapter=await createDemoAdapter({fixtures:[{id:"isolated-demo",name:"Isolated",steps:[{id:"intake",kind:"intake"},{id:"review",kind:"wait"}],runs:[{id:"demo-local-run",events:[{t:0,type:"job"},{t:1,type:"wait",step:"review",human:true,allowed:["Hold"]}]}]}]});
    const [factory]=await adapter.discover(),snap=await adapter.snapshot(factory.id),run=snap.state.runs[0];
    adapter.observe(factory.id,null,run.id);
    const result=await adapter.command({op:"command",factory_id:factory.id,command_id:"isolated-command",task_id:run.task.id,context_id:run.task.context_id,action:"hold",expected_state:"waiting"});
    assert.equal(result.lifecycle,"applied");assert.equal((await adapter.submit({text:"local only"})).lifecycle,"local_only");
    assert.equal((await adapter.inspect_artifact(run.id,"r1",D1)).available,false);assert.equal(fetchCalls,0);assert.equal(socketCalls,0);
  }finally{
    globalThis.fetch=originalFetch;
    if(originalWebSocket)Object.defineProperty(globalThis,"WebSocket",originalWebSocket);else delete globalThis.WebSocket;
  }
});

test("live adapter stays explicitly unauthenticated without a server session",async()=>{
  const statuses=[],adapter=createLiveAdapter({observationEndpoint:"wss://example.invalid/observe",principalResolverReady:false,onStatus:(...args)=>statuses.push(args),socketFactory:()=>{throw new Error("must not create a socket");}});
  await assert.rejects(adapter.discover(),/Unauthenticated/);
  assert.throws(()=>adapter.snapshot(FACTORY),/Unauthenticated/);
  assert.ok(statuses.some(([status])=>status==="unauthenticated"));
});

test("live adapter application readiness requires a fresh snapshot and caught-up checkpoint",async()=>{
  class FakeSocket{
    constructor(){this.OPEN=1;this.readyState=0;this.listeners=new Map();this.sent=[];}
    addEventListener(type,fn){const rows=this.listeners.get(type)||[];rows.push(fn);this.listeners.set(type,rows);}
    emit(type,value={}){for(const fn of this.listeners.get(type)||[])fn(type==="message"?{data:JSON.stringify(value)}:value);}
    open(){this.readyState=this.OPEN;this.emit("open");}
    send(value){this.sent.push(value);}
    close(_code,_reason=""){if(this.readyState===3)return;this.readyState=3;this.emit("close",{reason:_reason});}
  }
  const command={op:"command",factory_id:FACTORY,command_id:"live-ready-command",task_id:TASK1,context_id:CONTEXT1,action:"hold",expected_state:"waiting"};
  const makeAdapter=({freshness="fresh",snapshotTimeoutMs=250}={})=>{
    const sockets=[],statuses=[];let submits=0,fetches=0;
    const adapter=createLiveAdapter({
      observationEndpoint:"ws://observation.invalid/v1",principalResolverReady:true,snapshotTimeoutMs,
      socketFactory:()=>{const socket=new FakeSocket();sockets.push(socket);return socket;},
      fetcher:async()=>{fetches++;return{ok:true,status:200,json:async()=>({})};},
      messageSendEndpoint:"/message/send",submit:async()=>{submits++;return{lifecycle:"accepted"};},
    });
    return{adapter,sockets,statuses,counts:()=>({submits,fetches}),freshness};
  };
  const sendSnapshot=async harness=>{
    const pending=harness.adapter.snapshot(FACTORY,RUN1),socket=harness.sockets.at(-1);socket.open();
    const value=snapshot();value.freshness.status=harness.freshness;socket.emit("message",{op:"snapshot",snapshot:value});
    return pending;
  };

  for(const freshness of ["stale","unknown"]){
    const harness=makeAdapter({freshness});await sendSnapshot(harness);
    const statuses=[];const sub=harness.adapter.observe(FACTORY,cursor(1),RUN1,()=>{},(...args)=>statuses.push(args));
    const socket=harness.sockets.at(-1);socket.open();
    socket.emit("message",{op:"resumed",after_cursor:cursor(1),continuation_cursor:cursor(1)});
    socket.emit("message",{op:"checkpoint",cursor:cursor(1)});
    assert.notEqual(statuses.at(-1)?.[0],"connected",`${freshness} baseline must not become ready at checkpoint`);
    assert.throws(()=>sub.command(command),/fresh snapshot or catch-up checkpoint/);
    assert.equal((await harness.adapter.submit({messageId:"brief-1",parts:[]})).lifecycle,"unavailable");
    assert.deepEqual(harness.counts(),{submits:0,fetches:0});
  }

  const harness=makeAdapter();await sendSnapshot(harness);
  const statuses=[];const sub=harness.adapter.observe(FACTORY,cursor(1),RUN1,()=>{},(...args)=>statuses.push(args));
  const socket=harness.sockets.at(-1);socket.open();
  assert.equal(statuses.at(-1)[0],"connecting","raw socket open is not application readiness");
  socket.emit("message",{op:"resumed",after_cursor:cursor(1),continuation_cursor:cursor(1)});
  assert.equal(statuses.at(-1)[0],"connecting","resumed alone is not caught up");
  assert.throws(()=>sub.command(command),/fresh snapshot or catch-up checkpoint/);
  assert.equal((await harness.adapter.submit({messageId:"brief-2",parts:[]})).lifecycle,"unavailable");
  assert.deepEqual(harness.counts(),{submits:0,fetches:0});
  socket.emit("message",{op:"checkpoint",cursor:cursor(1)});
  assert.equal(statuses.at(-1)[0],"connected");
  const pendingAck=sub.command(command);socket.emit("message",{op:"command_ack",command_id:command.command_id,lifecycle:"received"});
  assert.deepEqual(await pendingAck,{lifecycle:"received"});
  assert.deepEqual(await harness.adapter.submit({messageId:"brief-3",parts:[]}),{lifecycle:"accepted"});
  assert.deepEqual(harness.counts(),{submits:1,fetches:0});
  socket.emit("message",{op:"resync_required",reason:"retention_expired",minimum_cursor:cursor(2),latest_cursor:cursor(3),message:"gap"});
  assert.equal(statuses.at(-1)[0],"error");
  socket.emit("message",{op:"checkpoint",cursor:cursor(3)});
  assert.equal(statuses.at(-1)[0],"error","checkpoint cannot clear a retention gap without a fresh snapshot");
  assert.throws(()=>sub.command({...command,command_id:"after-gap-command"}),/fresh snapshot or catch-up checkpoint/);
  assert.equal((await harness.adapter.submit({messageId:"brief-4",parts:[]})).lifecycle,"unavailable");

  const timeoutHarness=makeAdapter({snapshotTimeoutMs:5});
  const timedOut=timeoutHarness.adapter.snapshot(FACTORY,RUN1);await assert.rejects(timedOut,/timed out before its snapshot/);
  const closeHarness=makeAdapter();const closed=closeHarness.adapter.snapshot(FACTORY,RUN1);closeHarness.sockets.at(-1).close(1000,"closed early");
  await assert.rejects(closed,/closed early/);
});

test("live measurements require authorized same-origin bootstrap and validate the Runtime response",async()=>{
  let calls=0;
  const inertFetch=async()=>{calls++;throw new Error("unexpected fetch");};
  await assert.rejects(createLiveAdapter({principalResolverReady:false,fetcher:inertFetch}).measurements(),/Unauthenticated/);
  await assert.rejects(createLiveAdapter({principalResolverReady:true,fetcher:inertFetch}).measurements(),/not configured/);
  await assert.rejects(createLiveAdapter({principalResolverReady:true,usageEndpoint:"https://other.invalid/usage",fetcher:inertFetch}).measurements(),/same-origin/);
  await assert.rejects(createLiveAdapter({principalResolverReady:true,usageEndpoint:"/usage",fetcher:inertFetch}).measurements({raw_prompt:"private"}),/invalid token measurement filter/);
  assert.equal(calls,0);

  const usage=Object.fromEntries(["input_tokens","output_tokens","cache_read_tokens","cache_write_tokens","total_tokens"].map((name,index)=>[name,{status:"reported",value:index===0?0:index*5}]));
  const measurement={measurement_id:"measurement-live-1",model_call_id:"call-live-1",recorded_at:T0,call_scope:"assignment_call",provider:"codex",model_id:"gpt-6-luna",unit:"tokens",measurement_source:"provider_reported",completeness:"complete",evidence_status:"provider_reported",usage,service_identity:"service-live",task_id:TASK1,run_id:RUN1,assignment_id:"assignment-live",attempt_id:"attempt-live"};
  const coverage={status:"partial",factory_id:FACTORY,scoped_run_count:1,bound_sources_status:"available",authoring_bound:{status:"available",queries_failed:0,rows_rejected:0,conflicts:0},authoring_unbound:{status:"unavailable",reason:"shared_model_home_factory_exclusivity_not_proven"},pinned_services:{status:"fixture_only",availability:"available",authentication:"fixture_bearer_only",pinned_owner_count:1,owners_responded:1,owner_or_request_failures:0,rows_rejected:0,conflicts:0},director:{status:"unavailable",reason:"no_public_list_measurements_accessor"},commercial_costs:"not_included"};
  let requested;
  const adapter=createLiveAdapter({principalResolverReady:true,usageEndpoint:"/usage/measurements",fetcher:async(url,options)=>{requested={url,options};return {ok:true,status:200,json:async()=>({measurements:[measurement],coverage})};}});
  const response=await adapter.measurements({run_id:RUN1});
  assert.equal(response.measurements[0].measurement_id,"measurement-live-1");
  assert.equal(response.measurements[0].usage.input_tokens.value,0);
  assert.equal(response.coverage.commercial_costs,"not_included");
  assert.equal(new URL(requested.url).origin,"http://localhost");
  assert.equal(new URL(requested.url).searchParams.get("run_id"),RUN1);
  assert.equal(requested.options.credentials,"same-origin");
  assert.equal(requested.options.headers.Accept,"application/json");
  assert.doesNotMatch(requested.url,/token|credential|secret/i);
});

test("historical bundle is graph-unavailable, source-ID preserving, and report bytes verify independently",async()=>{
  const recordings=fileURLToPath(new URL("../recordings/",import.meta.url));
  const bundle=JSON.parse(await readFile(path.join(recordings,"codex-subscription-3.bundle.json"),"utf8"));
  const manifest=JSON.parse(await readFile(path.join(recordings,"codex-subscription-3-artifacts.json"),"utf8"));
  validateBundle(bundle);assert.equal(bundle.provenance.model_label,"gpt-6-sol");assert.match(bundle.provenance.fixture_label,/HTTP fixture/);
  for(const ref of manifest.artifacts){assert.deepEqual(Object.keys(ref).sort(),["revision","run_id","sha256","url"]);assert.throws(()=>createRecordedAdapter(bundle,{artifactRefs:[{...ref,task_id:"must-not-be-in-manifest"}]}),/invalid recorded artifact reference/);assert.throws(()=>createRecordedAdapter(bundle,{artifactRefs:[{...ref,context_id:"must-not-be-in-manifest"}]}),/invalid recorded artifact reference/);}
  assert.deepEqual(bundle.snapshot.state.factory.graph,{nodes:[],edges:[]});
  assert.equal(toFloorModel(createDashboardState(bundle.snapshot,{source:"recorded"})),null);
  assert.equal(bundle.snapshot.state.runs.length,3);
  assert.deepEqual(bundle.snapshot.state.runs.map(row=>row.status),["accepted","accepted","aborted"]);
  assert.equal(bundle.snapshot.state.runs[2].artifacts.some(row=>row.artifact_revision.startsWith("report-")),false);
  assert.ok(bundle.frames.some(frame=>frame.event.subject.includes("/event/1/")));
  assert.ok(bundle.frames.every(frame=>!JSON.stringify(frame).includes("workflowExecutionStartedEventAttributes")));
  const adapter=createRecordedAdapter(bundle,{artifactRefs:manifest.artifacts,fetcher:async url=>{
    const fileName=decodeURIComponent(new URL(url).pathname.split("/").at(-1));const bytes=await readFile(path.join(recordings,"artifacts",fileName));
    return {ok:true,arrayBuffer:async()=>bytes.buffer.slice(bytes.byteOffset,bytes.byteOffset+bytes.byteLength)};
  }});
  for(const ref of manifest.artifacts){const result=await adapter.inspect_artifact(ref.run_id,ref.revision,ref.sha256);assert.equal(result.valid,true);assert.equal(result.sha256,ref.sha256);}
  const route1=manifest.artifacts[0];const route2=manifest.artifacts[1];assert.notEqual(route1.run_id,route2.run_id);assert.notEqual(bundle.snapshot.state.runs[0].task.id,bundle.snapshot.state.runs[1].task.id);
  const bad=await verifyArtifactBytes(new TextEncoder().encode("wrong"),route1.sha256);assert.equal(bad.valid,false);
});

test("existing floor keeps accessible views and removes live playback controls",async()=>{
  const html=await readFile(fileURLToPath(new URL("../../../../docs/design/exomachina-floor.html",import.meta.url)),"utf8");
  assert.match(html,/from '\/dashboard-assets\/reducer\.mjs'/);assert.match(html,/adapters\/demo\.mjs/);assert.match(html,/adapters\/recorded\.mjs/);assert.match(html,/adapters\/live\.mjs/);
  assert.match(html,/id='exo-source-picker';select\.setAttribute\('aria-label','Observation source'\)/);
  assert.match(html,/id='exo-source-status';status\.setAttribute\('role','status'\);status\.setAttribute\('aria-live','polite'\)/);
  assert.match(html,/id='exo-live-reconnect';reconnect\.type='button';reconnect\.textContent='Reconnect'/);
  assert.match(html,/function readSavedSelection\(\)/);assert.match(html,/function persistSelection\(/);
  assert.match(html,/discovered\.some\(row=>\(row\.id\|\|row\.factory_id\)===saved\.factory_id\)/);
  assert.match(html,/dashboardState\.runs\.has\(restoreRunId\)/);
  assert.match(html,/sessionStorage\.setItem\(SELECTION_STORAGE_KEY,JSON\.stringify\(\{schema_version:1,source:currentSource,factory_id:factoryId,run_id:runId\}\)\)/);
  const selectionBlock=html.slice(html.indexOf('const SELECTION_STORAGE_KEY'),html.indexOf('const text = '));
  assert.doesNotMatch(selectionBlock,/\b(task_id|context_id|brief|prompt|token|secret)\b/i);
  assert.match(html,/nav class="views" aria-label="Factory views"/);
  for(const view of ["Floor","Board","Decisions","Outputs","Definition","Agents"])assert.ok(html.includes(`'${view}'`),`missing ${view} view`);
  assert.match(html,/panel\.setAttribute\('aria-live','polite'\)/);
  // Live follows the present while a job is live; a finished Live job can be replayed (operator request, 7 Oct 2026).
  assert.match(html,/class="transport" x-show="\$store\.floor\.sourceMode !== 'live' \|\| \(\$store\.floor\.ready && !\$store\.floor\.live\)"/);
  assert.match(html,/x-show="\$store\.floor\.live && \$store\.floor\.sourceMode !== 'live'"/);
  assert.match(html,/x-show="i\.live && \$store\.floor\.sourceMode !== 'live'"/);
  assert.match(html,/function seek\(t\)\{ if \(store\.sourceMode === 'live' && \(!S\.R \|\| S\.R\.run\.live\)\) return;/);
  assert.match(html,/function toggle\(\)\{ if \(!S\.R \|\| \(store\.sourceMode === 'live' && S\.R\.run\.live\)\) return;/);
  assert.match(html,/function jump\(dir\)\{ if \(!S\.R \|\| \(store\.sourceMode === 'live' && S\.R\.run\.live\)\) return;/);
  assert.match(html,/if \(store\.sourceMode === 'live' && \(R\.run\.live \|\| !S\.liveReplay\)\)\{/);
  // Live run selection scopes the public snapshot; it does not seek playback.
  assert.match(html,/selectedAdapter\.snapshot\(selectedFactory,runId\)/);
  assert.match(html,/selectedAdapter\.observe\(selectedFactory,snapshot\.cursor,runId/);
  assert.match(html,/event\.code==='Space'\|\|\['ArrowRight','ArrowLeft','Home','End','PageUp','PageDown'\]/);
  assert.match(html,/server-side Observation principal resolver\/session is not selected/);
  const commandGate=html.slice(html.indexOf('function controlReason('),html.indexOf('function updateDirectorControls()'));
  assert.match(commandGate,/transport\?\.status!=='connected'\|\|dashboardState\?\.freshness\?\.status!=='fresh'/);
  assert.doesNotMatch(commandGate,/transport\?\.gap/);
  assert.match(html,/const generation=\+\+sourceGeneration;currentSource=mode/);
  assert.match(html,/if\(generation!==sourceGeneration\)return;adapter=nextAdapter/);
  assert.match(html,/if\(epoch!==renderEpoch\|\|selectedAdapter!==adapter\|\|selectedSource!==currentSource\|\|selectedFactory!==activeFactoryId\)return/);
});

test("multiple runs share a chronological floor without losing their Task bindings",()=>{
  const first=publicRun(RUN1,T0),second=publicRun(RUN2,T1);
  first.assignments=[{id:"work-one",attempts:[{assignment_id:"work-one",attempt_id:"attempt-one",capability:"research",state:"working",started_at:T0}]}];
  const state=createDashboardState(snapshot({runs:[first,second]}),{source:"live"});
  const model=toFloorModel(state),aggregate=model.runs[0];
  assert.equal(aggregate.multi,true);
  assert.equal(aggregate.events.filter(row=>row.type==="job").length,2);
  assert.deepEqual(aggregate.events.filter(row=>row.type==="admit").map(row=>row.job),[RUN1]);
  const secondStart=aggregate.events.find(row=>row.type==="job"&&row.job===RUN2);
  assert.equal(secondStart.t,(Date.parse(T1)-Date.parse(T0))/1000);
  assert.equal(state.runs.get(RUN1).task.id,TASK1);
  assert.equal(state.runs.get(RUN2).task.context_id,CONTEXT1);
});

test("completed or accepted observations do not manufacture a delivery",()=>{
  for(const value of ["accepted","completed","expired","failed"]){
    const run=publicRun();run.status={state:value,started_at:T0,ended_at:T1};
    const state=createDashboardState(snapshot({runs:[run]}));
    assert.equal(toFloorModel(state).runs[0].timeline.at(-1).outcome,value);
  }
});

test("different graph pins fail closed instead of using a new publication",()=>{
  const first=publicRun(),second=publicRun(RUN2);second.pinned.definition_digest="d".repeat(64);
  assert.throws(()=>toFloorModel(createDashboardState(snapshot({runs:[first,second]}))),/Run graph pins differ/);
});

test("selected historical run renders its own graph instead of the active publication",()=>{
  const old=publicRun();old.pinned.definition_digest="d".repeat(64);
  old.graph={nodes:[{id:"old-work",kind:"agent",capability:"old-role"}],edges:[]};
  const current=publicRun(RUN2);
  const state=createDashboardState(snapshot({runs:[old,current]}));
  const model=toFloorModel(state,{runId:RUN1});
  assert.deepEqual(model.steps.map(row=>row.id),["old-work"]);
  assert.equal(model.runs.length,1);assert.equal(model.runs[0].id,RUN1);
  assert.equal(model.runs[0].pinned.definition_digest,old.pinned.definition_digest);
  assert.equal(state.runs.size,2);
});

test("a run pin cannot be replaced by a later event",()=>{
  const state=createDashboardState(snapshot());
  const event=cloudEvent(eventType("run.state_changed"),{run_id:RUN1,state:"working",phase:"running",definition_digest:"d".repeat(64)});
  assert.throws(()=>reduceDashboard(state,eventFrame(event,81)),/run pin changed/);
  assert.equal(state.runs.get(RUN1).pinned.definition_digest,pins.definition_digest);
});

test('same-attempt completion retains observed node/start while unknown corrections clear the end',()=>{
 let state=createDashboardState(snapshot(),{source:'live'});
 const common={run_id:RUN1,task_id:TASK1,assignment_id:'assignment-real',attempt_id:'attempt-real',capability:'research',provider_identity:'research-owner'};
 state=reduceDashboard(state,eventFrame(cloudEvent(eventType('assignment.state_changed'),{...common,node:'research',state:'running',started_at:T0})));
 state=reduceDashboard(state,eventFrame(cloudEvent(eventType('assignment.state_changed'),{...common,state:'completed',ended_at:T1})));
 let row=state.runs.get(RUN1).assignments['assignment-real']['attempt-real'];
 assert.equal(row.node,'research');assert.equal(row.started_at,T0);assert.equal(row.ended_at,T1);assert.equal(row.state,'completed');
 assert.equal(toFloorModel(state).runs[0].events.find(e=>e.type==='work').dur,(Date.parse(T1)-Date.parse(T0))/1000);
 state=reduceDashboard(state,eventFrame(cloudEvent(eventType('assignment.state_changed'),{...common,attempt_id:'another-attempt',state:'scheduled'})));
 assert.equal(Object.hasOwn(state.runs.get(RUN1).assignments['assignment-real']['another-attempt'],'started_at'),false);
 state=reduceDashboard(state,eventFrame(cloudEvent(eventType('assignment.state_changed'),{...common,state:'unknown'})));
 row=state.runs.get(RUN1).assignments['assignment-real']['attempt-real'];assert.equal(Object.hasOwn(row,'ended_at'),false);assert.equal(row.started_at,T0);
});


test('phase cues never invent assignments and completed attempts do not retain raw infinite work',()=>{
 let state=createDashboardState(snapshot());
 state=reduceDashboard(state,eventFrame(cloudEvent(eventType('run.state_changed'),{run_id:RUN1,state:'working',phase:'quality',node:'research'},T0)));
 assert.equal(toFloorModel(state).runs[0].events.some(e=>e.type==='work'),false);
 assert.equal(state.runs.get(RUN1).state.node,'research');
 const common={run_id:RUN1,task_id:TASK1,context_id:CONTEXT1,assignment_id:'a-real',attempt_id:'attempt-real',node:'research',capability:'research'};
 state=reduceDashboard(state,eventFrame(cloudEvent(eventType('assignment.state_changed'),{...common,state:'running',started_at:T0},T0)));
 state=reduceDashboard(state,eventFrame(cloudEvent(eventType('assignment.state_changed'),{...common,state:'completed',ended_at:T1},T1)));
 let work=toFloorModel(state).runs[0].events.filter(e=>e.type==='work');
 assert.equal(work.length,1);assert.equal(work[0].dur,(Date.parse(T1)-Date.parse(T0))/1000);
 state=reduceDashboard(state,eventFrame(cloudEvent(eventType('assignment.state_changed'),{...common,state:'unknown'},T1)));
 assert.equal(toFloorModel(state).runs[0].events.some(e=>e.type==='work'),false);
 assert.equal(state.runs.get(RUN1).assignments['a-real']['attempt-real'].started_at,T0);
 state=reduceDashboard(state,eventFrame(cloudEvent(eventType('assignment.state_changed'),{...common,state:'running',started_at:T0},T1)));
 state=reduceDashboard(state,eventFrame(cloudEvent(eventType('run.state_changed'),{run_id:RUN1,state:'completed',phase:'accepted',ended_at:T1},T1)));
 assert.equal(toFloorModel(state).runs[0].events.some(e=>e.type==='work'),false);
 assert.equal(state.runs.get(RUN1).assignments['a-real']['attempt-real'].state,'running');
});
