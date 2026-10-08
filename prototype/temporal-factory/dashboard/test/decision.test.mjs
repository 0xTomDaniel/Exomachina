import test from 'node:test';
import assert from 'node:assert/strict';
import {makeWaitCommand} from '../decision.mjs';
import {dashboardViewModels} from '../reducer.mjs';
const sha='a'.repeat(64);
const run={task:{id:'task',context_id:'context'},state:{state:'input-required',phase:'awaiting-director',wait_role:'director',wait_deadline:'2026-10-03T01:00:00Z',permitted_actions:['abort','escalate']},quality:[{accepted:false,artifact_revision:'r3',artifact_sha256:sha}],artifacts:[{artifact_revision:'r3',artifact_sha256:sha}]};
const options={factoryId:'factory',run,action:'escalate',commandId:'command',nowMs:Date.parse('2026-10-03T00:00:00Z'),inspected:{valid:true,sha256:sha}};
test('original Task wait command binds verified rejected revision',()=>assert.deepEqual(makeWaitCommand(options),{op:'command',factory_id:'factory',command_id:'command',task_id:'task',context_id:'context',action:'escalate',expected_state:'input-required',expected_revision:'r3',expected_sha256:sha}));
test('human resolution uses published permission and same inspected candidate',()=>{const human=structuredClone(run);human.state={...human.state,phase:'awaiting-human',wait_role:'human',permitted_actions:['abort']};assert.equal(makeWaitCommand({...options,run:human,action:'abort'}).task_id,'task');assert.throws(()=>makeWaitCommand({...options,run:human}),/not permitted/);});
test('unknown wait, accepted/stale candidate, missing bindings or unverified bytes fail closed',()=>{
 for(const alter of [r=>delete r.state.wait_role,r=>r.state.phase='completed',r=>delete r.task.context_id,r=>r.quality[0].accepted=true,r=>r.artifacts[0].artifact_sha256='b'.repeat(64)]){const r=structuredClone(run);alter(r);assert.throws(()=>makeWaitCommand({...options,run:r}));}
 assert.throws(()=>makeWaitCommand({...options,inspected:{valid:false,sha256:sha}}));assert.throws(()=>makeWaitCommand({...options,inspected:{valid:true,sha256:'b'.repeat(64)}}));
});

test('wait inspector exposes only an exact rejected candidate reference, never a recommendation',()=>{
 const state={runs:new Map([['run',{...structuredClone(run),run_id:'run',pinned:{},assignments:{},quality:run.quality,artifacts:run.artifacts}]]),events:[],commercial:{usage:[],obligations:[]}};
 const wait=dashboardViewModels(state).waits[0];
 assert.deepEqual(wait.candidate_refs,{artifact_revision:'r3',artifact_sha256:sha});
 assert.equal(wait.recommendation,null);
 state.runs.get('run').quality=[{...run.quality[0],accepted:true}];
 assert.equal(dashboardViewModels(state).waits[0].candidate_refs,null);
});

test('missing or expired deadline prevents a browser command',()=>{assert.throws(()=>makeWaitCommand({...options,nowMs:Date.parse(run.state.wait_deadline)}),/deadline/);const unknown=structuredClone(run);delete unknown.state.wait_deadline;assert.throws(()=>makeWaitCommand({...options,run:unknown}),/deadline/);});
