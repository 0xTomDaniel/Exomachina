import test from 'node:test';
import assert from 'node:assert/strict';
import {makeCloudEvent,validateCloudEvent} from '../contract.mjs';
const data={schema_version:1,factory_id:'factory',run_id:'run',incident_id:'incident',kind:'activity-failure',state:'claimed',owner_identity:'maintenance-owner',evidence_refs:[]};
const event=value=>makeCloudEvent({type:'com.exomachina.incident.state_changed.v1',id:`obs-${'1'.repeat(64)}`,factory_id:'factory',subject:'incidents/incident',data:value});
test('claimed incident carries only a safe owner identity',()=>{
 assert.equal(validateCloudEvent(event(data)).data.owner_identity,'maintenance-owner');
 assert.throws(()=>validateCloudEvent(event({...data,owner_identity:'unsafe owner text'})));
 assert.throws(()=>validateCloudEvent(event({...data,owner_identity:{actor:'private'}})));
});

import {createDashboardState,reduceDashboard,dashboardViewModels} from '../reducer.mjs';
const pins={manifest_digest:'a'.repeat(64),package_digest:'b'.repeat(64),definition_digest:'c'.repeat(64),interpreter_build:'build'};
const cursor=n=>`c1.abcdefghijklmnop.${String(n).padStart(40,'0')}`;
function snapshot(incidents){return {schema_version:1,cursor:cursor(1),captured_at:'2026-10-03T00:00:00Z',freshness:{status:'fresh',observed_at:'2026-10-03T00:00:00Z'},state:{factory:{id:'factory',name:'Synthetic factory',graph:{nodes:[],edges:[]}},runs:[{id:'run',task:{id:'task',context_id:'context'},pinned:pins,status:{state:'completed'},assignments:[],artifacts:[],quality:[],decisions:[],commands:[],delivery:[],incidents,admissions:[]}],active_publication:null,capacity:null,commercial:{usage:[],obligations:[],payments:[]}}};}
const escalated={schema_version:1,factory_id:'factory',run_id:'run',incident_id:'incident',kind:'activity-failure',state:'escalated',evidence_refs:[]};
test('current incident replaces a prior claim without retaining its owner',()=>{
 const initial=createDashboardState(snapshot([data]));
 const update=makeCloudEvent({type:'com.exomachina.incident.state_changed.v1',id:`obs-${'2'.repeat(64)}`,factory_id:'factory',subject:'incidents/incident',data:escalated});
 const frame={op:'event',cursor:cursor(2),event:update};const next=reduceDashboard(initial,frame);
 const rows=dashboardViewModels(next).incidents;
 assert.equal(rows.length,1);assert.equal(rows[0].state,'escalated');assert.equal(Object.hasOwn(rows[0],'owner_identity'),false);
 assert.equal(dashboardViewModels(initial).incidents[0].owner_identity,'maintenance-owner');
 assert.deepEqual(reduceDashboard(next,frame),next);
});
test('legacy incident history snapshots normalize to complete current rows',()=>{
 const other={...data,incident_id:'other'};
 const rows=dashboardViewModels(createDashboardState(snapshot([data,other,escalated]))).incidents;
 assert.equal(rows.length,2);assert.equal(rows.find(x=>x.incident_id==='incident').state,'escalated');
 assert.equal(Object.hasOwn(rows.find(x=>x.incident_id==='incident'),'owner_identity'),false);
 assert.equal(rows.find(x=>x.incident_id==='other').owner_identity,'maintenance-owner');
});
