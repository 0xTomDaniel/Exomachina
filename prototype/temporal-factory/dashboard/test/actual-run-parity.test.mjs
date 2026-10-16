import test from "node:test";
import assert from "node:assert/strict";
import { validateSnapshot } from "../contract.mjs";
import { createDashboardState, dashboardViewModels, toFloorModel } from "../reducer.mjs";

const factoryId = "factory-root-child";
const pins = {
  manifest_digest:"a".repeat(64),
  package_digest:"b".repeat(64),
  definition_digest:"c".repeat(64),
  interpreter_build:"build-1",
};
const graph = {
  nodes:[
    {id:"root-intake",kind:"intake",name:"Intake"},
    {id:"root-research",kind:"agent",name:"Research",capability:"research"},
  ],
  edges:[{from:"root-intake",to:"root-research"}],
};

function run(id, taskId, assignmentId, attemptId, historicNode) {
  return {
    id,
    task:{id:taskId,context_id:`context-${id}`},
    status:{state:"working",phase:"started"},
    pinned:{...pins},
    assignments:[{
      id:assignmentId,
      attempts:[{
        assignment_id:assignmentId,
        attempt_id:attemptId,
        capability:"research",
        state:"completed",
        node:historicNode,
      }],
    }],
    artifacts:[],quality:[],decisions:[],commands:[],delivery:[],incidents:[],admissions:[],
  };
}

function rootChildSnapshot() {
  const graphNodes = [
    {id:"root-intake",type:"intake",next:["root-research"]},
    {id:"root-research",type:"agent",next:[],capability:"research"},
  ];
  return {
    schema_version:1,
    cursor:`c1.abcdefghijklmnop.${"0".repeat(40)}`,
    captured_at:"2026-09-24T16:49:20.083123Z",
    freshness:{status:"fresh",observed_at:"2026-09-24T16:49:20.083123Z"},
    state:{
      factory:{id:factoryId,name:"Root and child",graph},
      runs:[
        run("root-run","root-task","root-assignment","root-attempt","historic-root-node"),
        run("child-run","child-task","child-assignment","child-attempt","historic-child-node"),
      ],
      active_publication:{
        schema_version:1,factory_id:factoryId,publication_version:"root-publication",
        ...pins,graph_nodes:graphNodes,service_bindings:[],
      },
      capacity:null,
      commercial:{usage:[],obligations:[],payments:[]},
    },
  };
}

test("root and child assignment facts retain safe IDs without extending the pinned factory graph",()=>{
  const snapshot=validateSnapshot(rootChildSnapshot());
  const state=createDashboardState(snapshot,{source:"recorded"});
  const view=dashboardViewModels(state);

  assert.deepEqual(view.assignments.map(({run_id,assignment_id,attempt_id,node})=>({run_id,assignment_id,attempt_id,node})),[
    {run_id:"root-run",assignment_id:"root-assignment",attempt_id:"root-attempt",node:"historic-root-node"},
    {run_id:"child-run",assignment_id:"child-assignment",attempt_id:"child-attempt",node:"historic-child-node"},
  ]);
  assert.deepEqual(view.definition.graph.nodes.map(node=>node.id),["root-intake","root-research"]);

  const floor=toFloorModel(state);
  assert.deepEqual(floor.steps.map(step=>step.id),["root-intake","root-research"]);
  assert.equal(floor.steps.some(step=>step.id.startsWith("historic-")),false);
});

test("normalized incident and admission facts are available to the Decisions inspector",()=>{
  const wire=rootChildSnapshot();
  wire.state.runs[0].incidents=[{incident_id:"incident-root",kind:"operator_attention",state:"open",owner_identity:"operator-1"}];
  wire.state.runs[0].admissions=[{admission_id:"admission-root",state:"admitted",queue_position:1,capacity_limit:2}];
  const state=createDashboardState(validateSnapshot(wire),{source:"recorded"});
  const view=dashboardViewModels(state);

  assert.deepEqual(view.incidents,[{incident_id:"incident-root",kind:"operator_attention",state:"open",owner_identity:"operator-1",run_id:"root-run"}]);
  assert.deepEqual(view.admissions,[{admission_id:"admission-root",state:"admitted",queue_position:1,capacity_limit:2,run_id:"root-run"}]);
});
