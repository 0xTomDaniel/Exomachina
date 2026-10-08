import test from 'node:test';
import assert from 'node:assert/strict';
import { validateSnapshot, makeCloudEvent } from '../contract.mjs';
import { createDashboardState, reduceDashboard } from '../reducer.mjs';
const time='2026-10-04T05:00:00.000Z',cursor='c1.abcdefghijklmnop.'+'0'.repeat(40);
function snapshot(freshness){return {schema_version:1,cursor,captured_at:time,freshness,state:{factory:{id:'factory',name:'Factory',graph:{nodes:[],edges:[]}},active_publication:{},capacity:null,commercial:{usage:[],obligations:[],payments:[]},runs:freshness.scope==='run'?[{id:'root',task:{id:'task',context_id:'context'},status:{state:'completed'},pinned:{},assignments:[],artifacts:[],quality:[],decisions:[],commands:[],delivery:[],incidents:[],admissions:[]}]:[]}};}
test('run freshness carries finite missing-history warning without declaring factory fresh',()=>{
 const f={status:'fresh',observed_at:time,scope:'run',run_id:'root',included_run_ids:['root','child'],factory_status:'disconnected',unavailable_run_ids:['historical-root','historical-child']};
 assert.deepEqual(validateSnapshot(snapshot(f)).freshness,f);
 for(const mutation of [{included_run_ids:['child']},{included_run_ids:['root','root']},{unavailable_run_ids:['root']},{run_id:'../ unsafe'},{private_error:'secret'}, {scope:'factory'}])assert.throws(()=>validateSnapshot(snapshot({...f,...mutation})),/freshness/);
});
test('cursor catch-up and events never upgrade disconnected or stale live source freshness',()=>{
 for(const status of ['disconnected','stale','unknown']){
  let state=createDashboardState(snapshot({status,observed_at:time}),{source:'live'});
  state=reduceDashboard(state,{op:'checkpoint',cursor});assert.equal(state.freshness.status,status);
  const event=makeCloudEvent({id:'obs-'+'a'.repeat(64),factory_id:'factory',type:'com.exomachina.run.state_changed.v1',time,subject:'runs/root',data:{schema_version:1,factory_id:'factory',run_id:'root',state:'working',phase:'started'}});
  state=reduceDashboard(state,{op:'event',cursor,event});assert.equal(state.freshness.status,status);
  state=reduceDashboard(state,{op:'snapshot',snapshot:snapshot({status:'fresh',observed_at:time})});assert.equal(state.freshness.status,'fresh');
 }
});
