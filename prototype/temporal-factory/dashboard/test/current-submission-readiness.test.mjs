import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {runInNewContext} from 'node:vm';
import {validateSubmissionReadiness} from '../contract.mjs';
import {createLiveAdapter} from '../adapters/live.mjs';
const response=(status='ready',reason_code=null)=>({schema_version:1,factory_id:'factory-one',observed_at:new Date().toISOString(),status,reason_code});
class Socket {readyState=0; listeners={};addEventListener(k,fn){this.listeners[k]=fn;}send(){}close(){this.readyState=3;}open(){this.readyState=1;this.listeners.open?.();}}
test('current submission projection is strict and factory-bound',()=>{
 assert.equal(validateSubmissionReadiness(response(),'factory-one').status,'ready');
 for(const value of [{...response(),factory_id:'other'}, {...response(),credential:'canary'},response('ready','factory_busy'),response('blocked',null),response('blocked','arbitrary'),{...response(),observed_at:'bad'}])assert.throws(()=>validateSubmissionReadiness(value,'factory-one'));
});
test('current submission rechecks its own authority while old-run commands remain gated',async()=>{
 let socket,gets=0,posts=0,projection=response();
 const adapter=createLiveAdapter({principalResolverReady:true,observationEndpoint:'ws://localhost/observations',a2aMessageSendEndpoint:'/',submissionReadinessEndpoint:'/submission/readiness',socketFactory:()=>socket=new Socket(),fetcher:async(_url,options)=>{if(options.method==='POST'){posts++;assert.equal(options.headers['A2A-Version'],'1.0');return {ok:true,json:async()=>({fixture:true})};}gets++;assert.equal(options.cache,'no-store');return {ok:true,json:async()=>projection};}});
 adapter.observe('factory-one');socket.open();
 assert.equal(adapter.submissionConnectionReady(),true);
 assert.equal((await adapter.submit({params:{message:{taskId:'old-task',contextId:'old-context'}}})).lifecycle,'unavailable');assert.equal(gets,0);assert.equal(posts,0);
 assert.throws(()=>adapter.command({op:'command',factory_id:'factory-one',command_id:'test-hold',task_id:'task-one',context_id:'context-one',action:'hold',expected_state:'waiting'}),/fresh snapshot/);
 assert.deepEqual(await adapter.submit({synthetic:true}),{fixture:true});assert.equal(gets,1);assert.equal(posts,1);
 projection=response('blocked','factory_busy');assert.equal((await adapter.submit({synthetic:true})).lifecycle,'unavailable');assert.equal(posts,1);
 projection={...response(),factory_id:'wrong'};await assert.rejects(adapter.submit({synthetic:true}),/factory mismatch/);assert.equal(posts,1);
 projection={...response(),observed_at:'2020-01-01T00:00:00Z'};await assert.rejects(adapter.submit({synthetic:true}),/timestamp/);assert.equal(posts,1);
 socket.close();assert.equal((await adapter.submit({synthetic:true})).lifecycle,'unavailable');assert.equal(posts,1);
});
test('submission UI uses explicit current authority without upgrading old run freshness',async()=>{
 const html=await readFile(new URL('../../../../docs/design/exomachina-floor.html',import.meta.url),'utf8');
 const part=html.slice(html.indexOf('function controlReason('),html.indexOf('function updateDirectorControls()'));
 const context={currentSource:'live',adapter:{submissionConnectionReady:()=>true},bootstrap:{authorizedSession:true,a2aMessageSendEndpoint:'/',submissionReadinessEndpoint:'/submission/readiness'},submissionReadiness:{ready:true},dashboardState:{transport:{status:'error'},freshness:{status:'disconnected'}}};
 const gate=runInNewContext('('+part.trim()+')',context);
 assert.equal(gate({requireRun:false,requireSubmission:true}),'');
 assert.match(gate({requireRun:false}),/not current/);
 context.submissionReadiness={ready:false,reason:'factory busy'};assert.equal(gate({requireRun:false,requireSubmission:true}),'factory busy');
 context.bootstrap.authorizedSession=false;assert.match(gate({requireRun:false,requireSubmission:true}),/Unauthenticated/);
});


test('a pending submitted Task is never replaced by an unrelated older run',async()=>{
 const {preferredFloorRunId}=await import('../presentation.mjs');
 const html=await readFile(new URL('../../../../docs/design/exomachina-floor.html',import.meta.url),'utf8');
 const start=html.indexOf('function selectFloorGraph('),end=html.indexOf('let floorRenderRequested',start);
 const calls=[];
 const choose=runInNewContext('('+html.slice(start,end).trim()+')',{currentSource:'live',liveRunChoices:new Map(),preferredFloorRunId,document:{getElementById:()=>null},queueMicrotask:fn=>fn(),selectRun:id=>calls.push(id)});
 const old={run_id:'old-run',task:{id:'old-task',context_id:'old-context'},pinned:{},graph:{nodes:[{id:'old',kind:'assign'}],edges:[]}};
 const state={factoryId:'factory-one',runs:new Map([['old-run',old]]),freshness:{unavailable_run_ids:[]}};
 const store={dashboardSubmittedTask:{factoryId:'factory-one',taskId:'new-task',contextId:'new-context'},dashboardRunId:null};
 assert.equal(choose(state,store),null);assert.deepEqual(calls,[]);
 const current={...old,run_id:'new-run',task:{id:'new-task',context_id:'new-context'}};state.runs.set(current.run_id,current);
 assert.equal(choose(state,store),'new-run');assert.equal(store.dashboardSubmittedTask,null);assert.deepEqual(calls,['new-run']);
});


test('closing an adapter during the current check prevents the later POST',async()=>{
 let socket,release,posts=0;
 const adapter=createLiveAdapter({principalResolverReady:true,observationEndpoint:'ws://localhost/observations',a2aMessageSendEndpoint:'/',submissionReadinessEndpoint:'/submission/readiness',socketFactory:()=>socket=new Socket(),fetcher:(_url,options)=>{if(options.method==='POST'){posts++;throw Error('must not post');}return new Promise(resolve=>release=()=>resolve({ok:true,json:async()=>response()}));}});
 adapter.observe('factory-one');socket.open();const pending=adapter.submit({synthetic:true});adapter.close();release();
 assert.equal((await pending).lifecycle,'unavailable');assert.equal(posts,0);
});


test('reusing a rendered graph after reconnect restores display without upgrading source status',async()=>{
 const html=await readFile(new URL('../../../../docs/design/exomachina-floor.html',import.meta.url),'utf8');
 const start=html.indexOf('  async function updateDashboardFactory('),end=html.indexOf('  async function loadFactory(',start);
 const f={id:'factory-one',steps:[{id:'a'}],edges:[],signals:[],runs:[]};
 const store={ready:false,error:'Connecting',dashboardUnavailable:'Unavailable',observationStatus:'error'},noop=()=>{};
 const surface={style:{display:'none'}};
 const update=runInNewContext('('+html.slice(start,end).trim()+')',{document:{getElementById:()=>surface},S:{f},store,dashboardGraphKey:JSON.stringify([f.id,f.steps,f.edges,f.signals,null]),compile:()=>({}),buildItems:noop,buildStamps:noop,setTicks:noop,syncHUD:noop,invalidate:noop});
 await update(f);assert.equal(store.ready,true);assert.equal(store.error,'');assert.equal(store.observationStatus,'error');assert.equal(surface.style.display,'');
});

test('accepted submission reads its exact bound run before any factory-wide snapshot',async()=>{
 const html=await readFile(new URL('../../../../docs/design/exomachina-floor.html',import.meta.url),'utf8');
 const start=html.indexOf('async function selectFactoryAndRestore('),end=html.indexOf('async function reconnectLive(',start);
 const run={run_id:'new-root',task:{id:'new-task',context_id:'new-context'},graph:{nodes:[{id:'a'}]},pinned:{}};
 const store={factories:[{id:'factory-one'}],dashboardRunId:'old-run'};const reads=[];
 const context={Map,window:{Alpine:{store:()=>store}},closeFloorBrief(){},sourceGeneration:1,adapter:{},renderEpoch:0,activeFactoryId:'factory-one',currentSource:'live',briefDrafts:{get:()=>''},liveRunChoices:new Map(),dashboardState:null,preferredFloorRunId:runs=>runs[0]?.run_id||null,persistSelection(){},floorRenderRequested:0,queueMicrotask(){},renderLatestFloor(){}};
 context.selectRun=async id=>{reads.push(id);context.renderEpoch++;context.dashboardState={factoryId:'factory-one',runs:new Map([[run.run_id,run]]),freshness:{unavailable_run_ids:[]}};};
 const select=runInNewContext('('+html.slice(start,end).trim()+')',context);
 await select('factory-one',null,{runId:'new-root',taskId:'new-task',contextId:'new-context'});
 assert.deepEqual(reads,['new-root']);assert.equal(store.dashboardRunId,'new-root');
});

test('Floor summary uses actual scoped facts and keeps Quality separate from delivery',async()=>{
 const html=await readFile(new URL('../../../../docs/design/exomachina-floor.html',import.meta.url),'utf8');
 const start=html.indexOf('function observedJobSummary('),end=html.indexOf('function renderObservedJobSummary(',start);
 const summarize=runInNewContext('('+html.slice(start,end).trim()+')');
 const vm={runs:[{run_id:'current',task:{id:'task'},state:{state:'completed',phase:'accepted'}}],assignments:[{run_id:'current',state:'completed',active:false},{run_id:'other',active:true}],quality:[{run_id:'current',accepted:true}]};
 const summary=summarize(vm,'current');assert.equal(summary.state,'completed');assert.equal(summary.phase,'accepted');assert.equal(summary.activeAssignments,0);assert.equal(summary.completedAssignments,1);assert.equal(summary.quality,'accepted');assert.equal(Object.hasOwn(summary,'delivered'),false);assert.equal(summarize(vm,'missing'),null);
});

test('waiting for a new run snapshot clears the previous job before awaiting transport',async()=>{
 const html=await readFile(new URL('../../../../docs/design/exomachina-floor.html',import.meta.url),'utf8');
 const start=html.indexOf('async function selectRun('),end=html.indexOf('async function selectFactory(',start);
 const store={ready:true,dashboardViewModels:{runs:[{run_id:'old'}]},runs:[{id:'old'}],rail:[{id:'old'}]};
 const surface={style:{display:''}};let release,summaryRenders=0;
 const adapter={snapshot:()=>new Promise(resolve=>release=resolve)};
 const context={adapter,activeFactoryId:'factory-one',activeSubscription:null,renderEpoch:0,currentSource:'live',window:{Alpine:{store:()=>store}},document:{getElementById:()=>surface},renderObservedJobSummary:()=>summaryRenders++,validateSnapshot:x=>x,renderFrame(){},persistSelection(){},setTransport(){},updateDirectorControls(){}};
 adapter.observe=()=>({close(){}});
 const select=runInNewContext('('+html.slice(start,end).trim()+')',context);const pending=select('new');
 assert.equal(store.ready,false);assert.equal(store.dashboardViewModels,null);assert.equal(store.runs.length,0);assert.equal(surface.style.display,'none');assert.equal(summaryRenders,1);
 release({cursor:'cursor'});await pending;
});

test('unchanged stream checkpoints do not reset open artifact details',async()=>{
 const html=await readFile(new URL('../../../../docs/design/exomachina-floor.html',import.meta.url),'utf8');
 const start=html.indexOf('function schemaViewRevision('),end=html.indexOf('function renderSchemaView(',start);
 const context={currentSource:'live',activeFactoryId:'factory-one',dashboardState:{cursor:'one',freshness:{status:'fresh'},transport:{status:'connected'}},bootstrap:{authorizedSession:true},submissionReadiness:{ready:true,reason:''}};
 const revision=runInNewContext('('+html.slice(start,end).trim()+')',context);const store={view:'Outputs'},vm={runs:[{state:{state:'working'}}]};const first=revision(store,vm);
 context.dashboardState.cursor='two';assert.equal(revision(store,vm),first);
 vm.runs[0].state.state='completed';assert.notEqual(revision(store,vm),first);
 const complete=revision(store,vm);context.dashboardState.transport.status='disconnected';assert.notEqual(revision(store,vm),complete);
 assert.match(html,/panel\.dataset\.observationRevision===revision\)return/);
});

test('detailed submitted work is selected only through explicit original Task/context facts',async()=>{
 const {preferredFloorRunId}=await import('../presentation.mjs');
 const html=await readFile(new URL('../../../../docs/design/exomachina-floor.html',import.meta.url),'utf8');
 const start=html.indexOf('function submittedTaskDetailRun('),end=html.indexOf('async function selectFactoryAndRestore(',start);
 const choose=runInNewContext('('+html.slice(start,end).trim()+')',{preferredFloorRunId});
 const run=(id,taskId,contextId,count)=>({id,task:{id:taskId,context_id:contextId},graph:{nodes:Array.from({length:count},(_,i)=>({id:'n'+i}))},status:{},pinned:{}});
 const snapshot={state:{factory:{id:'factory-one'},runs:[run('root','task','context',2),run('detail','task','context',10),run('other','other-task','context',30)]},freshness:{unavailable_run_ids:[]}};
 assert.equal(choose(snapshot,'factory-one',{taskId:'task',contextId:'context'}),'detail');
 assert.equal(choose(snapshot,'wrong-factory',{taskId:'task',contextId:'context'}),null);
 snapshot.freshness.unavailable_run_ids=['detail'];assert.equal(choose(snapshot,'factory-one',{taskId:'task',contextId:'context'}),'root');
});

test('root is usable before optional child discovery, and child read failure returns to root',async()=>{
 const {preferredFloorRunId}=await import('../presentation.mjs');
 const html=await readFile(new URL('../../../../docs/design/exomachina-floor.html',import.meta.url),'utf8');
 const helperStart=html.indexOf('function submittedTaskDetailRun('),selectStart=html.indexOf('async function selectFactoryAndRestore('),end=html.indexOf('async function reconnectLive(',selectStart);
 for(const [failChild,delayChild] of [[false,false],[true,false],[false,true]]){
  const rows=['root','detail'].map((id,i)=>({id,run_id:id,task:{id:'task',context_id:'context'},status:{},graph:{nodes:Array.from({length:i?10:2},(_,j)=>({id:'n'+j}))},pinned:{}}));
  const store={factories:[{id:'factory-one'}],dashboardRunId:'old'},reads=[];
  const adapter={snapshot:async()=>{reads.push('overview');return {state:{factory:{id:'factory-one'},runs:delayChild&&reads.filter(x=>x==='overview').length===1?[rows[0]]:rows},freshness:{unavailable_run_ids:[]}};}};
  const context={Map,setTimeout:fn=>fn(),window:{Alpine:{store:()=>store}},closeFloorBrief(){},sourceGeneration:1,adapter,renderEpoch:0,activeFactoryId:'factory-one',currentSource:'live',briefDrafts:{get:()=>''},dashboardState:null,preferredFloorRunId,persistSelection(){},floorRenderRequested:0,queueMicrotask(){},renderLatestFloor(){},validateSnapshot:x=>x};
  context.selectRun=async id=>{reads.push(id);context.renderEpoch++;context.dashboardState=failChild&&id==='detail'?{}:{factoryId:'factory-one',runs:new Map([[id,rows.find(row=>row.id===id)]]),freshness:{unavailable_run_ids:[]}};};
  context.submittedTaskDetailRun=runInNewContext('('+html.slice(helperStart,selectStart).trim()+')',context);
  const select=runInNewContext('('+html.slice(selectStart,end).trim()+')',context);
  await select('factory-one',null,{runId:'root',taskId:'task',contextId:'context'});
  assert.deepEqual(reads,failChild?['root','overview','detail','root']:delayChild?['root','overview','overview','detail']:['root','overview','detail']);
  assert.equal(store.dashboardRunId,failChild?'root':'detail');
 }
});


test('observed progress belongs to the job inspector and never overlays the composer',async()=>{
 const html=await readFile(new URL('../../../../docs/design/exomachina-floor.html',import.meta.url),'utf8');
 const start=html.indexOf('function renderObservedJobSummary(){'),end=html.indexOf('function schemaViewRevision',start);
 const store={},summary={state:'completed',quality:'accepted'};let removed=0;
 const render=runInNewContext('('+html.slice(start,end).trim()+')',{window:{Alpine:{store:()=>store}},document:{getElementById:()=>({remove(){removed++;}})},currentSource:'live',observedJobSummary:()=>summary});
 render();assert.deepEqual(store.observedJobSummary,summary);assert.equal(removed,1);
 assert.doesNotMatch(html.slice(start,end),/createElement|append|position:absolute/);
 const inspector=html.slice(html.indexOf('<template x-if="$store.floor.insp && ($store.floor.insp.kind'),html.indexOf(`<template x-if="$store.floor.insp && $store.floor.insp.kind === 'director'`));
 assert.match(inspector,/data-observed-job-summary/);assert.match(inspector,/View outputs/);
});


test('accepted new work reveals its job card and bounded child discovery respects user selection',async()=>{
 const html=await readFile(new URL('../../../../docs/design/exomachina-floor.html',import.meta.url),'utf8');
 const submit=html.slice(html.indexOf('async function submitBrief('),html.indexOf('function appendBriefForm('));
 assert.match(submit,/dashboardRevealTask=\{taskId:acceptedTask.id,contextId:acceptedTask.contextId\}/);
 assert.match(submit,/store.setView\('Floor'\)/);
 const render=html.slice(html.indexOf('async function renderLatestFloor('),html.indexOf('function renderFrame('));
 assert.match(render,/selected\?\.task\?\.id===reveal.taskId/);assert.match(render,/store.panel=true;store.select\('run'\)/);
 const selection=html.slice(html.indexOf('async function selectFactoryAndRestore('),html.indexOf('async function reconnectLive('));
 assert.match(selection,/discovery<3/);assert.match(selection,/renderEpoch!==detailEpoch/);
});

test('observed current activity lights only pinned stations and stops on terminal runs',async()=>{
 const html=await readFile(new URL('../../../../docs/design/exomachina-floor.html',import.meta.url),'utf8');
 const start=html.indexOf('  function observedActiveNodes('),end=html.indexOf('  /* ---------- per frame ---------- */',start);
 const activity=runInNewContext('('+html.slice(start,end).trim()+')');
 const vm={runs:[{run_id:'new',state:{state:'working',node:'quality'}}],assignments:[{run_id:'new',active:true,node_id:'research'},{run_id:'old',active:true,node_id:'other'},{run_id:'new',active:true,node_id:'unknown'}]};
 const steps=['quality','research','other'].map(id=>({id}));
 assert.deepEqual([...activity(vm,'new',steps)],['quality','research']);
 vm.runs[0].state={state:'completed',node:'quality'};assert.deepEqual([...activity(vm,'new',steps)],[]);
 vm.runs[0].state={state:'working',node:'5'};vm.assignments=[];assert.deepEqual([...activity(vm,'new',steps)],[]);
 vm.runs[0].state={state:'working',node:'gather'};vm.assignments=[{run_id:'new',active:true,node:'gather',capability:'packet_risks@1'},{run_id:'new',active:true,node:'gather',capability:'unpodded@1'}];
 assert.deepEqual([...activity(vm,'new',[{id:'gather'},{id:'pod:gather:packet_risks@1'}])],['gather','pod:gather:packet_risks@1']);
 assert.match(html,/Task outputs are not recorded as artifacts, so their hand-off follows pinned graph order\. Branch agent pods are derived from observed assignments\./);
 assert.doesNotMatch(html,/Moving belt chevrons are a visual route cue/);
 assert.match(html,/observedNodes.has\(e.def.to\)&&!e.loop/);
});
