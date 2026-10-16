import test from "node:test";
import assert from "node:assert/strict";
import { validateServerMessage, validateSnapshot } from "../contract.mjs";
import { createDashboardState, reduceDashboard, toFloorModel } from "../reducer.mjs";
import { createDemoAdapter } from "../adapters/demo.mjs";

const BASE=Date.parse("2026-01-01T00:00:00.000Z");
// Demo frames carry the labelled illustrative layer, accepted only for the Demo source.
const DEMO={source:"demo"};

test("multi-job Demo projects per-job bindings and preserves signed chronological events through the shared schema/reducer",async()=>{
  const fixture={
    id:"demo-multi",
    name:"Multi-job local scenario",
    steps:[{id:"intake",kind:"intake"},{id:"worker",kind:"agent",agent:"research",rate_atoms:999999}],
    agent_rates:{research:{amount_atoms:999999,currency:"USD"}},
    runs:[{id:"batch",events:[
      {t:1,type:"work",job:"job-a",step:"worker",dur:0.5},
      {t:-1,type:"job",job:"job-a"},
      {t:2,type:"jobend",job:"job-a",outcome:"accepted"},
      {t:-2,type:"job",job:"job-b"},
      {t:0,type:"work",job:"job-b",step:"worker",dur:1},
      {t:3,type:"end",job:"job-b",outcome:"failed"},
      {t:-0.5,type:"admit",job:"job-a",capacity_limit:2,queue_position:0},
      {t:-1.5,type:"admit",job:"job-b"},
    ]}],
  };
  const received=[];
  const adapter=await createDemoAdapter({fixtures:[fixture],onFrame:frame=>received.push(frame)});
  const [factory]=await adapter.discover();
  const snapshot=validateSnapshot(await adapter.snapshot(factory.id),DEMO);
  assert.equal(snapshot.state.runs.length,2);
  assert.equal(snapshot.state.capacity,null);
  assert.deepEqual(snapshot.state.commercial,{usage:[],obligations:[],payments:[]});
  const [runA,runB]=snapshot.state.runs;
  assert.equal(runA.id,"job-a");assert.equal(runB.id,"job-b");
  assert.ok(runA.task.id.startsWith("demo-task-"));assert.ok(runA.task.context_id.startsWith("demo-context-"));
  assert.notEqual(runA.task.id,runB.task.id);assert.notEqual(runA.task.context_id,runB.task.context_id);

  adapter.observe(factory.id);
  assert.equal(received[0].op,"snapshot");
  const frames=received.slice(1);
  frames.forEach(frame=>validateServerMessage(frame,DEMO));
  const times=frames.map(frame=>Date.parse(frame.event.time));
  assert.deepEqual(times,[...times].sort((a,b)=>a-b));
  assert.equal(times[0],BASE-2000);
  const events=frames.map(frame=>frame.event);
  for(const run of snapshot.state.runs){
    const own=events.filter(event=>event.data.run_id===run.id);
    assert.ok(own.length>0);
    assert.ok(own.every(event=>event.data.task_id===run.task.id&&event.data.context_id===run.task.context_id));
  }
  const startB=events.find(event=>event.data.run_id===runB.id&&event.data.started_at);
  assert.equal(Date.parse(startB.event_time??startB.time),BASE-2000);
  const endA=events.find(event=>event.data.run_id===runA.id&&event.data.phase==="ended");
  const endB=events.find(event=>event.data.run_id===runB.id&&event.data.phase==="ended");
  assert.equal(endA.data.state,"accepted");assert.equal(endB.data.state,"failed");
  const admissions=events.filter(event=>event.type==="com.exomachina.admission.state_changed.v1");
  assert.equal(admissions.length,1);
  assert.equal(admissions[0].data.run_id,runA.id);assert.equal(admissions[0].data.capacity_limit,2);

  let state={source:"demo"};
  for(const frame of received){validateServerMessage(frame,DEMO);state=reduceDashboard(state,frame);}
  assert.equal(state.runs.size,2);
  assert.equal(state.runs.get(runA.id).task.context_id,runA.task.context_id);
  assert.equal(state.runs.get(runB.id).task.context_id,runB.task.context_id);
  const terminalCommand=await adapter.command({op:"command",factory_id:factory.id,command_id:"multi-job-terminal-command",task_id:runA.task.id,context_id:runA.task.context_id,action:"abort",expected_state:"accepted"});
  assert.equal(terminalCommand.lifecycle,"rejected");
  assert.match(terminalCommand.reason,/terminal/);
  assert.equal(terminalCommand.transition,null);
});

test("Verified Research fixture facts retain job versions and run filtering while the historical v1 graph stays unavailable",async()=>{
  // This small module fixture models the existing page's Verified Research labels only.
  // The page has no reconstructible pinned v1 graph, so v1 here is not graph-parity proof.
  const fixture={
    id:"verified-research",name:"Verified Research",capability:"verified-research@1",version:"v2",digest:"d41f7a03",
    versions:{v1:{digest:"6cbb2bd8",note:"v2 moved Quality review to report.review@2. Jobs that started on v1 finish on v1."}},
    steps:[{id:"intake",kind:"intake",name:"Caller Task"},{id:"rf",kind:"assign",name:"Research findings",agent:"research-findings"}],
    edges:[{from:"intake",to:"rf"}],
    runs:[{id:"floor",multi:true,events:[
      {t:-2,type:"job",job:"research-v1",version:"v1"},
      {t:-1,type:"work",job:"research-v1",step:"rf",dur:2},
      {t:0,type:"job",job:"research-v2",version:"v2"},
      {t:0.5,type:"work",job:"research-v2",step:"rf",dur:1},
    ]}],
  };
  const adapter=await createDemoAdapter({fixtures:[fixture]});
  const [factory]=await adapter.discover();
  const snapshot=validateSnapshot(await adapter.snapshot(factory.id),DEMO);
  const [v1Run,v2Run]=snapshot.state.runs;
  assert.equal(snapshot.state.factory.id,"verified-research");
  assert.equal(snapshot.state.active_publication.version,"v2");
  assert.deepEqual(snapshot.state.factory.graph.nodes.map(node=>node.id),["intake","rf"]);
  assert.equal(v1Run.id,"research-v1");assert.equal(v2Run.id,"research-v2");
  assert.equal(v1Run.pinned.publication_version,"v1");assert.equal(v2Run.pinned.publication_version,"v2");
  assert.equal(Object.hasOwn(v1Run,"graph"),false,"the illustrated fixture does not provide a historical v1 graph");

  const allFrames=[];
  adapter.observe(factory.id,null,null,frame=>allFrames.push(frame));
  let allState={source:"demo"};
  for(const frame of allFrames){validateServerMessage(frame,DEMO);allState=reduceDashboard(allState,frame);}
  const allFloor=toFloorModel(allState);
  assert.equal(allFloor.version,"v2");
  assert.equal(allFloor.runs[0].multi,true);
  assert.deepEqual(allFloor.runs[0].timeline.filter(row=>row.type==="job").map(row=>row.version),["v1","v2"]);
  const workIntervals=allFloor.runs[0].timeline.filter(row=>row.type==="work"&&Number.isFinite(row.dur)).map(row=>[row.t,row.t+row.dur]);
  assert.ok(Math.max(workIntervals[0][0],workIntervals[1][0])<Math.min(workIntervals[0][1],workIntervals[1][1]),"the two illustrated jobs work concurrently");

  const selectedFrames=[];
  adapter.observe(factory.id,null,v1Run.id,frame=>selectedFrames.push(frame));
  assert.equal(selectedFrames[0].op,"snapshot");
  assert.equal(selectedFrames[0].snapshot.state.runs.length,2);
  const selectedEvents=selectedFrames.filter(frame=>frame.op==="event");
  assert.ok(selectedEvents.length>0);
  assert.ok(selectedEvents.every(frame=>frame.event.data.run_id===v1Run.id));

  let state={source:"demo"};
  for(const frame of selectedFrames){validateServerMessage(frame,DEMO);state=reduceDashboard(state,frame);}
  const selectedFloor=toFloorModel(state,{runId:v1Run.id});
  assert.equal(selectedFloor.version,"v2","this is the known active factory graph label, not proof of the v1 run graph");
  assert.equal(selectedFloor.runs.length,1);
  assert.equal(selectedFloor.runs[0].id,v1Run.id);
  assert.equal(selectedFloor.runs[0].pinned.publication_version,"v1");
  assert.notEqual(selectedFloor.version,selectedFloor.runs[0].pinned.publication_version);
  assert.deepEqual(selectedFloor.steps.map(step=>step.id),["intake","rf"],"the available projection is still the active v2 factory graph");
});

function decisionFixture(id,wait,tail=[]){
  return {id,name:"Local decision fixture",steps:[{id:"intake",kind:"intake"},{id:"gate",kind:"wait"}],runs:[{id:"task-run",events:[{t:0,type:"job"},{t:1,type:"wait",step:"gate",human:true,level:"action",...wait},...tail]}]};
}

async function observedDemo(fixture){
  const frames=[];let state={source:"demo"};
  const adapter=await createDemoAdapter({fixtures:[fixture],onFrame:frame=>{validateServerMessage(frame,DEMO);frames.push(frame);state=reduceDashboard(state,frame);}});
  const [factory]=await adapter.discover(),snapshot=validateSnapshot(await adapter.snapshot(factory.id),DEMO);
  adapter.observe(factory.id);
  return {adapter,factory,snapshot,get state(){return state;},frames};
}

test("Demo abort changes run state, command IDs are idempotent, and adapters remain isolated",async()=>{
  const fixture=decisionFixture("demo-abort",{allowed:["Abort"]});
  const first=await observedDemo(fixture),second=await observedDemo(fixture),run=first.snapshot.state.runs[0];
  assert.equal(first.state.runs.get(run.id).state.state,"waiting");
  const command={op:"command",factory_id:first.factory.id,command_id:"same-local-command",task_id:run.task.id,context_id:run.task.context_id,action:"abort",expected_state:"waiting"};
  const result=await first.adapter.command(command);
  assert.equal(result.lifecycle,"applied");assert.equal(result.transition.event.data.state,"aborted");
  assert.equal(first.state.runs.get(run.id).state.state,"aborted");
  assert.equal(second.state.runs.get(run.id).state.state,"waiting");
  const framesAfter=first.frames.length,repeated=await first.adapter.command(command);
  assert.equal(repeated.duplicate,true);assert.equal(first.frames.length,framesAfter);
  const independent=await second.adapter.command(command);
  assert.equal(independent.lifecycle,"applied");assert.equal(independent.duplicate,undefined);
  assert.equal(second.state.runs.get(run.id).state.state,"aborted");
  const conflict=await first.adapter.command({...command,action:"hold"});
  assert.equal(conflict.lifecycle,"rejected");assert.match(conflict.reason,/reused/);
  const terminal=await first.adapter.command({...command,command_id:"after-abort",expected_state:"aborted"});
  assert.equal(terminal.lifecycle,"rejected");assert.match(terminal.reason,/terminal/);
  assert.equal(first.state.runs.get(run.id).state.state,"aborted");
});

test("Demo hold and bounded repair change state; unbounded repair is rejected",async()=>{
  const hold=await observedDemo(decisionFixture("demo-hold",{allowed:["Hold"]}));
  const holdRun=hold.snapshot.state.runs[0];
  const held=await hold.adapter.command({op:"command",factory_id:hold.factory.id,command_id:"hold-one",task_id:holdRun.task.id,context_id:holdRun.task.context_id,action:"hold",expected_state:"waiting"});
  assert.equal(held.lifecycle,"applied");assert.equal(hold.state.runs.get(holdRun.id).state.state,"held");

  const bounded=await observedDemo(decisionFixture("demo-repair-bounded",{allowed:["One more repair"],repair_count:0,max_repairs:1}));
  const boundedRun=bounded.snapshot.state.runs[0];
  const repaired=await bounded.adapter.command({op:"command",factory_id:bounded.factory.id,command_id:"repair-one",task_id:boundedRun.task.id,context_id:boundedRun.task.context_id,action:"one_more_repair",expected_state:"waiting"});
  assert.equal(repaired.lifecycle,"applied");assert.equal(repaired.transition.event.data.state,"working");
  assert.equal(repaired.transition.event.data.repair_count,1);assert.equal(bounded.state.runs.get(boundedRun.id).state.state,"working");
  const overBound=await bounded.adapter.command({op:"command",factory_id:bounded.factory.id,command_id:"repair-two",task_id:boundedRun.task.id,context_id:boundedRun.task.context_id,action:"one_more_repair",expected_state:"working"});
  assert.equal(overBound.lifecycle,"rejected");assert.equal(bounded.state.runs.get(boundedRun.id).state.state,"working");

  const unbounded=await observedDemo(decisionFixture("demo-repair-unbounded",{allowed:["One more repair"]}));
  const unboundedRun=unbounded.snapshot.state.runs[0];
  const refused=await unbounded.adapter.command({op:"command",factory_id:unbounded.factory.id,command_id:"repair-no-bound",task_id:unboundedRun.task.id,context_id:unboundedRun.task.context_id,action:"one_more_repair",expected_state:"waiting"});
  assert.equal(refused.lifecycle,"rejected");assert.match(refused.reason,/no finite repair_count\/max_repairs bound/);
  assert.equal(unbounded.state.runs.get(unboundedRun.id).state.state,"waiting");
});

test("Demo rejects stale and unsupported approval actions without inventing delivery",async()=>{
  const demo=await observedDemo(decisionFixture("demo-approval",{allowed:["Approve"]}));
  const run=demo.snapshot.state.runs[0];
  const stale=await demo.adapter.command({op:"command",factory_id:demo.factory.id,command_id:"stale",task_id:run.task.id,context_id:run.task.context_id,action:"approve",expected_state:"working"});
  assert.equal(stale.lifecycle,"rejected");assert.match(stale.reason,/Stale command/);
  const unsupported=await demo.adapter.command({op:"command",factory_id:demo.factory.id,command_id:"approve-unsupported",task_id:run.task.id,context_id:run.task.context_id,action:"approve",expected_state:"waiting"});
  assert.equal(unsupported.lifecycle,"rejected");assert.match(unsupported.reason,/no bounded local transition/);
  assert.equal(demo.state.runs.get(run.id).state.state,"waiting");
  assert.deepEqual(demo.state.runs.get(run.id).artifacts,[]);assert.deepEqual(demo.state.runs.get(run.id).delivery,[]);
});

test("Demo rejects actions when no active fixture wait declares them",async()=>{
  const fixture={id:"demo-no-wait",name:"No wait authority",steps:[{id:"worker",kind:"agent"}],runs:[{id:"task-run",events:[{t:0,type:"job"}]}]};
  let state={source:"demo"};
  const adapter=await createDemoAdapter({fixtures:[fixture],onFrame:frame=>{state=reduceDashboard(state,frame);}});
  const [factory]=await adapter.discover(),snapshot=validateSnapshot(await adapter.snapshot(factory.id),DEMO),run=snapshot.state.runs[0];
  adapter.observe(factory.id);
  const rejected=await adapter.command({op:"command",factory_id:factory.id,command_id:"undeclared-hold",task_id:run.task.id,context_id:run.task.context_id,action:"hold",expected_state:"working"});
  assert.equal(rejected.lifecycle,"rejected");
  assert.match(rejected.reason,/actions require an active fixture wait/);
  assert.equal(rejected.transition,null);
  assert.equal(state.runs.get(run.id).state.state,"working");
});
