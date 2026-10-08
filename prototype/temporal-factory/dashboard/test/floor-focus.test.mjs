import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {runInNewContext} from 'node:vm';

const html=await readFile(new URL('../../../../docs/design/exomachina-floor.html',import.meta.url),'utf8');
const start=html.indexOf('  const agentStation = g =>'),end=html.indexOf('  function applyFocus(',start);

// The focus model is plain code inside the page script; evaluate that slice and return its focusSet.
const focusIn=S=>runInNewContext(html.slice(start,end)+'\nfocusSet',{S});

function focusFor(sel){
 const step=(id,kind,agent,type=kind)=>[id,{s:{id,kind,agent,type}}];
 const L={steps:new Map([step('intake','intake','director'),step('rf','assign','research-findings'),step('rr','assign','research-risks'),step('join','join'),step('review','gate','quality')]),
  edges:[{id:'e0',def:{from:'intake',to:'rf'}},{id:'e1',def:{from:'intake',to:'rr'}},{id:'e2',def:{from:'rf',to:'join'}},{id:'e3',def:{from:'rr',to:'join'}},{id:'e4',def:{from:'join',to:'review'}},{id:'e5',def:{from:'review',to:'rf'},loop:true}]};
 const S={L,sel};
 return focusIn(S)();
}

test('opening an agent station focuses it, its routes, and the stations at their other ends',()=>{
 const focus=focusFor({type:'step',id:'rf'});
 assert.deepEqual([...focus.steps].sort(),['intake','join','review','rf']);
 assert.deepEqual([...focus.edges].sort(),['e0','e2','e5']);
});

test('Live graph agent stations focus without an agent binding id',()=>{
 const step=(id,kind,type)=>[id,{s:{id,kind,type}}];
 const L={steps:new Map([step('gather','fanout','parallel'),step('compose_report','assign','synthesize'),step('independent_quality','gate','quality'),step('route_verdict','gate','route')]),
  edges:[{id:'e0',def:{from:'gather',to:'compose_report'}},{id:'e1',def:{from:'compose_report',to:'independent_quality'}},{id:'e2',def:{from:'independent_quality',to:'route_verdict'}}]};
 const run=sel=>focusIn({L,sel})();
 assert.deepEqual([...run({type:'step',id:'compose_report'}).steps].sort(),['compose_report','gather','independent_quality']);
 assert.deepEqual([...run({type:'step',id:'independent_quality'}).edges].sort(),['e1','e2']);
 assert.equal(run({type:'step',id:'route_verdict'}),null);
});

test('non-agent stations, the entry, and other selections do not focus the floor',()=>{
 assert.equal(focusFor({type:'step',id:'join'}),null);
 assert.equal(focusFor({type:'step',id:'intake'}),null);
 assert.equal(focusFor({type:'run',id:null}),null);
 assert.equal(focusFor({type:'step',id:'missing'}),null);
});

test('the Director booth opens its console in the panel, not the Decisions view',()=>{
 assert.ok(html.includes("bt.addEventListener('click', () => { if (!wasDrag()) select('director'); });"));
 assert.ok(html.includes("bl.addEventListener('click', () => { if (!wasDrag()) select('director'); });"));
 assert.ok(!html.includes("EXO_DASHBOARD_OPEN_DECISIONS?.('director')"));
});

test('every way of opening a station shares the focus path and Escape leaves it',()=>{
 assert.ok(html.includes("@click=\"$store.floor.select('step', a.step)\""),'Agents capacity list opens through select');
 assert.ok(html.includes("b.addEventListener('click', () => { if (!wasDrag()) select('step', id); }); b.dataset.step = id;"),'floor station button opens through select');
 assert.ok(html.includes("else if (e.key === 'Escape' && S.sel.type === 'step') back();"));
 assert.ok(html.includes('#ov.focusing > :not(.lit){opacity:var(--dim,1)}'));
});
