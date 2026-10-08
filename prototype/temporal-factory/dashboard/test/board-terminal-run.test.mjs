import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { validateSnapshot } from "../contract.mjs";
import { createDashboardState, dashboardViewModels, toFloorModel } from "../reducer.mjs";

const FACTORY="board-terminal-factory";
const T0="2026-10-03T15:53:00.187Z";
const T1="2026-10-03T15:54:11.764Z";
const T2="2026-10-03T15:56:18.732Z";
const PINS={manifest_digest:"a".repeat(64),package_digest:"b".repeat(64),definition_digest:"c".repeat(64),interpreter_build:"build-1"};

function attempt(assignment_id,attempt_id,state,started_at,ended_at) {
  return {assignment_id,attempt_id,capability:assignment_id,state,started_at,...(ended_at?{ended_at}:{})};
}

function publicSnapshot(extraTerminalAttempt={}) {
  const graph={nodes:[{id:"research",kind:"agent",capability:"research_findings"}],edges:[]};
  const baseRun={
    task:{id:"task-terminal",context_id:"context-terminal"},status:{state:"completed",phase:"accepted",started_at:T0,ended_at:T2},
    started_at:T0,pinned:{...PINS},
    assignments:[{id:"research_findings",attempts:[
      attempt("research_findings","attempt-1","completed",T1,T2),
      attempt("research_findings","attempt-2","running",T1),
      ...(extraTerminalAttempt.state?[extraTerminalAttempt]:[]),
    ]}],
    artifacts:[],quality:[],decisions:[],commands:[],delivery:[],incidents:[],admissions:[],
  };
  const activeRun={
    id:"run-active",task:{id:"task-active",context_id:"context-active"},
    status:{state:"working",phase:"started",started_at:T0},started_at:T0,pinned:{...PINS},
    assignments:[{id:"research_risks",attempts:[attempt("research_risks","attempt-active","running",T1)]}],
    artifacts:[],quality:[],decisions:[],commands:[],delivery:[],incidents:[],admissions:[],
  };
  return validateSnapshot({
    schema_version:1,cursor:`c1.abcdefghijklmnop.${"0".repeat(40)}`,captured_at:T2,
    freshness:{status:"fresh",observed_at:T2},
    state:{
      factory:{id:FACTORY,name:"Terminal assignment observation",graph},
      runs:[{id:"run-terminal",...baseRun},activeRun],
      active_publication:{schema_version:1,factory_id:FACTORY,publication_version:"v1",...PINS,
        graph_nodes:[{id:"research",type:"agent",next:[],capability:"research_findings"}],service_bindings:[]},
      capacity:null,commercial:{usage:[],obligations:[],payments:[]},
    },
  });
}

test("terminal run suppresses active projection but preserves raw running attempt and unknown outcome",()=>{
  const state=createDashboardState(publicSnapshot(),{source:"live"});
  const rows=dashboardViewModels(state).assignments;
  const terminalRows=rows.filter(row=>row.run_id==="run-terminal");
  const running=terminalRows.find(row=>row.attempt_id==="attempt-2");
  const completed=terminalRows.find(row=>row.attempt_id==="attempt-1");
  const active=rows.find(row=>row.attempt_id==="attempt-active");

  assert.equal(rows.length,3);
  assert.equal(terminalRows.length,2);
  assert.equal(completed.state,"completed");
  assert.equal(completed.active,false);
  assert.equal(completed.outcome_unknown,false);
  assert.equal(running.state,"running");
  assert.equal(running.started_at,T1);
  assert.equal(Object.hasOwn(running,"ended_at"),false);
  assert.equal(running.active,false);
  assert.equal(running.outcome_unknown,true);
  assert.equal(running.activity_label,"Historical running; outcome unreported");
  assert.equal(active.state,"running");
  assert.equal(active.active,true);
  assert.equal(active.outcome_unknown,false);

  const floor=toFloorModel(state);
  assert.equal(floor.runs.find(run=>run.id==="run-terminal").outcomeLabel,"completed");
});

test("terminal run with a nonterminal unknown assignment state stays inactive and unreported",()=>{
  const state=createDashboardState(publicSnapshot(attempt("research_findings","attempt-3","unknown",T1)),{source:"live"});
  const row=dashboardViewModels(state).assignments.find(item=>item.attempt_id==="attempt-3");
  assert.equal(row.state,"unknown");
  assert.equal(row.active,false);
  assert.equal(row.outcome_unknown,true);
  assert.equal(row.activity_label,"Historical assignment; outcome unreported");
  assert.equal(Object.hasOwn(row,"ended_at"),false);
});

test("Board shows terminal-run assignment uncertainty without counting it as active work",async()=>{
  const html=await readFile(new URL("../../../../docs/design/exomachina-floor.html",import.meta.url),"utf8");
  const start=html.indexOf('<section class="board"');
  const end=html.indexOf("</section>",start);
  assert.notEqual(start,-1,"Board section exists");
  const board=html.slice(start,end);
  assert.match(board,/x-text="b\.sub"/);
  assert.match(board,/row\.outcome_unknown/);
  assert.match(board,/assignment\.activity_label/);
  assert.match(board,/assignment\.run_id/);
  assert.match(board,/assignment\.attempt_id/);
  assert.match(board,/sourceMode === 'demo'/);
  assert.match(board,/sourceMode !== 'demo'/);
  const rendererStart=html.indexOf("function boardCols(group)");
  const rendererEnd=html.indexOf("/* ---------- inspector ---------- */",rendererStart);
  const boardRenderer=html.slice(rendererStart,rendererEnd);
  assert.match(boardRenderer,/admission capacity unreported/);
  assert.match(boardRenderer,/if \(demo && ln\.key === 'all'\)/);
  assert.match(boardRenderer,/Number\.isFinite\(reportedCap\)\s*\?\s*reportedCap\s*:\s*'\?'/);
});
