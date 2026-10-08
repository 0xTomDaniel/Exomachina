import test from 'node:test';
import assert from 'node:assert/strict';
import { createDemoAdapter } from '../adapters/demo.mjs';
import { reduceDashboard, toFloorModel, dashboardViewModels } from '../reducer.mjs';

async function project(fixture) {
 let state={source:'demo'};
 const adapter=await createDemoAdapter({fixtures:[fixture],onFrame:frame=>{state=reduceDashboard(state,frame);}});
 adapter.observe(fixture.id);
 return {state,floor:toFloorModel(state)};
}
test('renderer vocabulary preserves assign and nested with original types',async()=>{
 const {floor}=await project({id:'demo-kind',steps:[{id:'a',kind:'assign'},{id:'n',kind:'nested'},{id:'s',kind:'synthesize'},{id:'nf',kind:'nested_factory'}],edges:[],runs:[{id:'job',events:[]}]});
 assert.deepEqual(floor.steps.map(n=>n.kind),['assign','nested','assign','nested']);
 assert.deepEqual(floor.steps.map(n=>n.type),['assign','nested','synthesize','nested_factory']);
});
test('single illustrative job retains seconds and terminal fact',async()=>{
 const {state,floor}=await project({id:'demo-time',steps:[{id:'translate',kind:'assign',agent:'translator'}],edges:[],runs:[{id:'job',events:[{t:6,type:'work',step:'translate',dur:20},{t:32,type:'end',outcome:'released'}]}]});
 const run=floor.runs[0];
 assert.equal(run.timeline.find(e=>e.type==='work').t,6);
 assert.equal(run.timeline.find(e=>e.type==='consume').t,26);
 assert.equal(run.timeline.find(e=>e.type==='end').t,32);
 assert.equal([...state.runs.values()][0].state.state,'released');
});
test('declared illustrative human wait projects only while current',async()=>{
 const fixture={id:'demo-human',steps:[{id:'approve',kind:'wait'}],edges:[],runs:[{id:'job',events:[{t:111.6,type:'wait',step:'approve',human:true,responder:'demo-human',allowed:['Abort']}]}]};
 const {state,floor}=await project(fixture);
 assert.equal(dashboardViewModels(state).waits[0].wait_role,'human');
 assert.equal(floor.runs[0].timeline.find(e=>e.type==='wait').t,111.6);
 fixture.runs[0].events.push({t:120,type:'end',outcome:'aborted'});
 const terminal=await project(fixture);
 assert.deepEqual(dashboardViewModels(terminal.state).waits,[]);
 assert.equal(terminal.floor.runs[0].timeline.some(e=>e.type==='wait'),false);
});
