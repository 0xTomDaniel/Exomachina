import test from 'node:test';
import assert from 'node:assert/strict';
import {makeCloudEvent} from '../contract.mjs';
import {createDashboardState,reduceDashboard,dashboardViewModels} from '../reducer.mjs';

const cursor=n=>`c1.abcdefghijklmnop.${String(n).padStart(40,'0')}`;
const cut='2026-10-03T20:00:00Z', completion='2026-10-03T15:54:23.206Z';
function initial(){return createDashboardState({schema_version:1,cursor:cursor(1),captured_at:cut,freshness:{status:'fresh',observed_at:cut},state:{factory:{id:'factory',name:'Synthetic correction regression',graph:{nodes:[],edges:[]}},runs:[{id:'run',task:{id:'task',context_id:'context'},pinned:{manifest_digest:'a'.repeat(64),package_digest:'b'.repeat(64),definition_digest:'c'.repeat(64),interpreter_build:'build'},status:{state:'completed'},assignments:[{id:'assignment',attempts:[{assignment_id:'assignment',attempt_id:'1',state:'completed',ended_at:completion}]}],artifacts:[],quality:[],decisions:[],commands:[],delivery:[],incidents:[],admissions:[]}],active_publication:null,capacity:null,commercial:{usage:[],obligations:[],payments:[]}}});}
function frame(time){return {op:'event',cursor:cursor(2),event:makeCloudEvent({id:`obs-${'2'.repeat(64)}`,type:'com.exomachina.assignment.state_changed.v1',factory_id:'factory',subject:'assignments/assignment',time,data:{schema_version:1,factory_id:'factory',run_id:'run',task_id:'task',assignment_id:'assignment',attempt_id:'1',capability:'research',state:'unknown'}})};}
test('late attribution correction preserves the observed clock and writer fact',()=>{
 const state=initial(), update=frame(completion),next=reduceDashboard(state,update);
 assert.equal(next.captured_at,cut);assert.equal(next.freshness.observed_at,cut);
 assert.equal(next.events[0].event.time,completion);
 const row=dashboardViewModels(next).assignments[0];
 assert.equal(row.state,'unknown');assert.equal(row.active,false);assert.equal(row.outcome_unknown,true);
 assert.equal(Object.hasOwn(row,'ended_at'),false);
 assert.equal(dashboardViewModels(state).assignments[0].state,'completed');
 assert.deepEqual(reduceDashboard(next,update),next);
});
test('a later writer fact advances the observed clock',()=>{
 const later='2026-10-03T20:01:00Z',next=reduceDashboard(initial(),frame(later));
 assert.equal(next.captured_at,later);assert.equal(next.freshness.observed_at,later);
});
