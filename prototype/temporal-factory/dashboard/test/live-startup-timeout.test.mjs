import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createLiveAdapter} from '../adapters/live.mjs';

test('discovery timeout aborts a stalled HTTP request with a recoverable error',async()=>{
 let aborted=false;
 const adapter=createLiveAdapter({principalResolverReady:true,discoveryEndpoint:'/discover',discoveryTimeoutMs:5,fetcher:(_url,{signal})=>new Promise((_resolve,reject)=>signal.addEventListener('abort',()=>{aborted=true;reject(new Error('aborted'));}))});
 await assert.rejects(adapter.discover(),/Factory discovery timed out/);
 assert.equal(aborted,true);
});

test('startup transport failure replaces the centered connecting message',async()=>{
 const html=await readFile(new URL('../../../../docs/design/exomachina-floor.html',import.meta.url),'utf8');
 assert.ok(html.includes("!store.ready&&['error','disconnected','unauthenticated'].includes(status)"));
 assert.ok(html.includes('store.error=`Observation unavailable · ${detail||status}`'));
});

test('ELK layout deadline releases a stuck worker for the existing fallback',async()=>{
 const {runInNewContext}=await import('node:vm');
 const html=await readFile(new URL('../../../../docs/design/exomachina-floor.html',import.meta.url),'utf8');
 const start=html.indexOf('async function elkLayout('),end=html.indexOf('  function fallbackLayout(',start);
 let terminated=false;
 const layout=runInNewContext('('+html.slice(start,end).trim()+')',{ELK:class{layout(){return new Promise(()=>{});}terminateWorker(){terminated=true;}},setTimeout:(fn)=>setTimeout(fn,5),clearTimeout,LABEL_H:10});
 await assert.rejects(layout({steps:[],edges:[]},{}),/Floor layout timed out/);
 assert.equal(terminated,true);
 assert.ok(html.includes('L = fallbackLayout(f, sizes)'));
});


test('ELK layout keeps a successful result when the bundled worker cannot terminate',async()=>{
 const {runInNewContext}=await import('node:vm');
 const html=await readFile(new URL('../../../../docs/design/exomachina-floor.html',import.meta.url),'utf8');
 const start=html.indexOf('async function elkLayout('),end=html.indexOf('  function fallbackLayout(',start);
 let terminated=false;
 const layout=runInNewContext('('+html.slice(start,end).trim()+')',{ELK:class{layout(root){return Promise.resolve({...root,children:[],edges:[]});}terminateWorker(){terminated=true;throw new TypeError('this.worker.terminate is not a function');}},setTimeout,clearTimeout,LABEL_H:10});
 const result=await layout({steps:[],edges:[]},{});
 assert.ok(result&&typeof result==='object');
 assert.equal(terminated,false);
});

test('boot illustration cannot publish HUD or escalation controls into Live',async()=>{
 const {runInNewContext}=await import('node:vm');
 const html=await readFile(new URL('../../../../docs/design/exomachina-floor.html',import.meta.url),'utf8');
 const start=html.indexOf('  function syncHUD('),end=html.indexOf('    if (force) invalidate();',start);
 const guard=html.slice(start,end)+' return "current-source"; }';
 const blocked=runInNewContext('('+guard+')',{dashboardPresentationSource:'live',S:{f:{id:'demo'}}});
 assert.equal(blocked(0,true),undefined);
 const current=runInNewContext('('+guard+')',{dashboardPresentationSource:'live',S:{f:{dashboardSource:'live'}}});
 assert.equal(current(0,true),'current-source');
 assert.ok(html.includes('window.EXO_DASHBOARD_FLOOR.resetDashboardSource(mode)'));
 assert.ok(html.includes('store.decision=null;store.insp=null;store.toasts=[]'));
});
