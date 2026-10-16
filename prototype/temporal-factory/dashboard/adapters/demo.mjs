import { DEMO_ILLUSTRATION_EVENT_TYPE, DEMO_ILLUSTRATION_LABEL, makeCloudEvent, sha256Hex, validateClientMessage, validateSnapshot } from "../contract.mjs";

// Fallback clock basis only for fixtures that declare no scenario start.
const BASE_TIME = Date.parse("2026-01-01T00:00:00.000Z");
const DEMO = {source:"demo"};
const dateAt = (seconds, basis=BASE_TIME) => new Date(basis + (Number.isFinite(Number(seconds))?Number(seconds):0)*1000).toISOString();
// A fixture's run.start is the scenario clock at t=0. Like the original page it is
// read as the viewer's local wall time when it carries no zone.
const scenarioBasis = run => { const value=typeof run?.start==="string"?Date.parse(run.start):NaN; return Number.isFinite(value)?value:BASE_TIME; };
const finite = value => typeof value==="number"&&Number.isFinite(value);
const idSafe = (value, fallback) => /^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}$/.test(String(value??"")) ? String(value) : fallback;
const enumSafe = value => String(value??"demo").toLowerCase().replace(/[^a-z0-9_.-]+/g,"_").replace(/^[^a-z]+/,"demo_").slice(0,64);
const digestSafe = value => typeof value === "string" && /^[0-9a-f]{64}$/.test(value) ? value : null;
const terminalStates = new Set(["completed","accepted","failed","aborted","expired","closed","cancelled","canceled","released"]);
const safeCount = value => Number.isSafeInteger(value)&&value>=0?value:null;

async function stableSuffix(value) { return (await sha256Hex(String(value))).slice(0,40); }
async function cursorFor(factoryId,value) { return `c1.${(await stableSuffix(factoryId)).slice(0,16)}.${await stableSuffix(value)}`; }
async function eventFor(factoryId, runId, type, data, seconds, key, basis=BASE_TIME) {
  const hash=await sha256Hex(`${factoryId}\0demo\0${key}`);
  return makeCloudEvent({id:`obs-${hash}`,factory_id:factoryId,type,subject:runId?`runs/${encodeURIComponent(runId)}`:"factory",data,time:dateAt(seconds,basis)},DEMO);
}

/*
 * Illustrative presentation layer. Every original fixture timeline entry and the
 * fixture's simulated operating metadata cross the shared Seam as an explicitly
 * labelled Demo layer (see contract.mjs). Standard lifecycle facts are still
 * emitted for the representable subset; the layer never becomes a Live fact.
 */
const ILLUSTRATION_TEXT=["item","art","at","from","to","step","label","rev","sha","msg","level","level2","text","by","cmd","outcome","action","stamp","verdict","finding","flag","value","target","responder","context","onExpiry","task","version","brief","reason","rationale"];
const ILLUSTRATION_NUMBER=["dur","attempt","repair","queued","findings"];
const ILLUSTRATION_BOOLEAN=["loop","limit","human","escalated","tick"];
const boundedText=(value,max=512)=>String(value).replace(/[\u0000-\u001f\u007f]/g," ").slice(0,max);
function illustrationFor(event,job,order){
  // order is the entry's position in the scenario script: the original tie-break for equal times.
  const out={type:String(event.type),t:Number.isFinite(Number(event.t))?Number(event.t):0,order};
  if(job!==undefined)out.job=job===null?null:boundedText(job);
  for(const key of ILLUSTRATION_TEXT)if(event[key]!=null&&["string","number"].includes(typeof event[key]))out[key]=boundedText(event[key]);
  for(const key of ILLUSTRATION_NUMBER)if(finite(event[key]))out[key]=event[key];
  for(const key of ILLUSTRATION_BOOLEAN)if(typeof event[key]==="boolean")out[key]=event[key];
  // Open intervals (t1 missing or Infinity) stay open: null is the renderer's "until resolved".
  for(const key of ["t1","expires"])if(Object.hasOwn(event,key))out[key]=finite(event[key])?event[key]:null;
  for(const key of ["tools","allowed"])if(Array.isArray(event[key]))out[key]=event[key].slice(0,32).map(value=>boundedText(value,256));
  if(event.rec&&typeof event.rec==="object"){out.rec={};if(finite(event.rec.t))out.rec.t=event.rec.t;for(const key of ["action","text","why"])if(event.rec[key]!=null)out.rec[key]=boundedText(event.rec[key]);}
  if(Array.isArray(event.parts))out.parts=event.parts.slice(0,16).map(part=>{
    if(part&&typeof part==="object"){const row={};if(typeof part.inbox==="boolean")row.inbox=part.inbox;for(const key of ["label","step","job","item"])if(part[key]!=null)row[key]=boundedText(part[key],256);return row;}
    return boundedText(part);
  });
  return out;
}
const STANDARD_KINDS=new Set(["job","work","wait","admit","end","jobend","decide","spawn"]);
const STEP_CUES=["short","sub","loop","responder","allowed","escalateTo","humanAllowed","expires","onExpiry","human","recommendedBy","out","outs"];
const plain=value=>value===undefined?undefined:JSON.parse(JSON.stringify(value,(key,entry)=>typeof entry==="number"&&!Number.isFinite(entry)?null:entry));
function presentationFor(fixture,runPresentations){
  const factory={};
  for(const key of ["digest","provenance","mainArt","sla","queueAdvisory"])if(fixture[key]!=null)factory[key]=plain(fixture[key]);
  for(const key of ["versions","departments","programs","admission","budget","agents","artifacts"])if(fixture[key]!=null)factory[key]=plain(fixture[key]);
  const steps={};
  for(const step of fixture.steps??[]){const cues={};for(const key of STEP_CUES)if(step[key]!=null)cues[key]=plain(step[key]);if(Object.keys(cues).length&&idSafe(step.id,null))steps[step.id]=cues;}
  factory.steps=steps;
  return {label:DEMO_ILLUSTRATION_LABEL,factory,runs:runPresentations};
}

function fixtureGraph(fixture) {
  const next=new Map();
  for (const edge of fixture.edges??[]) { const rows=next.get(edge.from)??[]; rows.push(edge.to); next.set(edge.from,rows); }
  return (fixture.steps??[]).map((step,index)=>{
    const type=idSafe(step.kind??"agent","agent");
    const node={id:idSafe(step.id,`step-${index+1}`),kind:type,name:String(step.name??step.id).slice(0,128)};
    if(typeof step.dept==="string"&&/^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}$/.test(step.dept))node.dept=step.dept;
    if (Array.isArray(next.get(step.id))&&next.get(step.id).length) node.next=next.get(step.id).map(id=>idSafe(id,"unknown"));
    if (step.agent){node.capability=idSafe(step.agent,`demo-${step.id}`);node.agent=node.capability;}
    return node;
  });
}

function emptyRun(runId) {
  return {run_id:runId,pinned:{},state:null,assignments:{},artifacts:[],quality:[],decisions:[],commands:[],delivery:[],incidents:[],admissions:[]};
}

async function fixtureBundle(fixture) {
  const factoryId=idSafe(fixture.id,"demo-factory");
  const nodes=fixtureGraph(fixture);
  const factory={id:factoryId,name:String(fixture.name??factoryId).slice(0,128),identity:factoryId,capability:idSafe(fixture.capability,"demo-factory"),graph:{nodes,edges:(fixture.edges??[]).map(e=>({from:idSafe(e.from,"step"),to:idSafe(e.to,"step"),...(e.loop?{loop:true}:{}),...(e.label?{label:String(e.label).slice(0,128)}:{})}))},fixture_label:String(fixture.provenance??fixture.name??"local scenario").slice(0,128)};
  const publication={version:String(fixture.version??"demo").slice(0,128)};
  const capacity=Number.isSafeInteger(fixture.admission?.limit)&&fixture.admission.limit>=0?{limit:fixture.admission.limit}:null;
  const snapshotRuns=[],pendingFrames=[],actionPlans=new Map(),runPresentations=[]; let sequence=0,earliestMs=Infinity;
  const fixtureRuns=fixture.runs??[];
  // A scripted job keeps its scenario identity (for example "0435") as its run id
  // unless that would be ambiguous within the fixture.
  const jobUse=new Map(),parentIds=new Set(fixtureRuns.map((run,index)=>idSafe(run.id,`${factoryId}-run-${index}`)));
  for(const run of fixtureRuns)for(const key of new Set((run.events??run.timeline??[]).filter(e=>e.job!=null&&String(e.job)!=="").map(e=>String(e.job))))jobUse.set(key,(jobUse.get(key)??0)+1);
  for (let runIndex=0;runIndex<fixtureRuns.length;runIndex++) {
    const run=fixtureRuns[runIndex];
    const parentRunId=idSafe(run.id,`${factoryId}-run-${runIndex}`);
    const basis=scenarioBasis(run);
    const src=run.events??run.timeline??[];
    let groups=new Map(),factoryWide=[];
    for(let i=0;i<src.length;i++){
      const e=src[i],jobKey=e.job==null||String(e.job)===""?parentRunId:String(e.job);
      if(!groups.has(jobKey))groups.set(jobKey,[]);
      groups.get(jobKey).push({event:e,index:i,seconds:Number.isFinite(Number(e.t))?Number(e.t):0});
    }
    if(!groups.size)groups.set(parentRunId,[]);
    const multiJob=groups.size>1||(run.multi===true&&!groups.has(parentRunId));
    // A multi-job scenario's purely illustrative job-less entries (for example a
    // publication notice) are factory-wide, not a phantom job.
    if(multiJob&&groups.has(parentRunId)&&groups.get(parentRunId).every(({event})=>!STANDARD_KINDS.has(event.type))){factoryWide=groups.get(parentRunId);groups.delete(parentRunId);}
    const runIds=[];
    for(const [jobKey,jobEvents] of groups){
      const candidate=jobUse.get(jobKey)===1&&!parentIds.has(jobKey)&&idSafe(jobKey,null)?jobKey:`${parentRunId}-${jobKey}`;
      const runId=multiJob?idSafe(candidate,`${parentRunId}-job-${await stableSuffix(`${factoryId}:${parentRunId}:${jobKey}`)}`):parentRunId;
      runIds.push(runId);
      const taskId=`demo-task-${await stableSuffix(`${factoryId}:${runId}:task`)}`;
      const contextId=`demo-context-${await stableSuffix(`${factoryId}:${runId}:context`)}`;
      const lifecycle=[...jobEvents].sort((a,b)=>a.seconds-b.seconds||a.index-b.index);
      const jobEvent=lifecycle.find(({event})=>event.type==="job");
      const jobVersion=typeof jobEvent?.event.version==="string"&&jobEvent.event.version.length?jobEvent.event.version:undefined;
      // Scenario start: the job-created entry, else (no such entry) the first entry,
      // never later than the scenario's own t=0 for a single hand-authored run.
      const startSeconds=jobEvent?jobEvent.seconds:lifecycle.length?(multiJob?lifecycle[0].seconds:Math.min(0,lifecycle[0].seconds)):0;
      earliestMs=Math.min(earliestMs,basis+startSeconds*1000);
      snapshotRuns.push({id:runId,task:{id:taskId,context_id:contextId},started_at:dateAt(startSeconds,basis),status:{state:"working",phase:"demo",started_at:dateAt(startSeconds,basis)},pinned:jobVersion?{publication_version:jobVersion}:{},assignments:[],artifacts:[],quality:[],decisions:[],commands:[],delivery:[],incidents:[],admissions:[]});
      const pendingWaits=new Map();
      for(const {event:e,index:i} of lifecycle){
        if(e.type==="wait"){
          const waitKey=e.step==null?`wait-${i}`:String(e.step);
          pendingWaits.set(waitKey,{step:e.step==null?null:String(e.step),human:e.human===true,allowed:new Set(Array.isArray(e.allowed)?e.allowed.map(enumSafe):[]),repairCount:safeCount(e.repair_count),maxRepairs:safeCount(e.max_repairs)});
        }else if(e.type==="decide"||e.type==="escalate"){
          if(e.step==null)pendingWaits.clear();else pendingWaits.delete(String(e.step));
        }else if(e.type==="end"||e.type==="jobend"||e.type==="scrap")pendingWaits.clear();
      }
      if(pendingWaits.size)actionPlans.set(runId,[...pendingWaits.values()]);
      for(const {event:e,index:i,seconds:at} of lifecycle){
        const data={schema_version:1,factory_id:factoryId,run_id:runId,task_id:taskId,context_id:contextId}; let type=null;
        if (e.type==="job") { type="com.exomachina.run.state_changed.v1"; Object.assign(data,{state:"working",phase:"started",started_at:dateAt(at,basis)}); }
        else if (e.type==="work") {
          type="com.exomachina.assignment.state_changed.v1";
          const assignment=idSafe(`${runId}-${e.step}-${i}`,`${runId}-assignment-${i}`), attempt=idSafe(`${assignment}-a${e.attempt??1}`,`${assignment}-attempt`);
          const step=(fixture.steps??[]).find(x=>x.id===e.step);
          Object.assign(data,{assignment_id:assignment,attempt_id:attempt,capability:idSafe(step?.agent,`demo-${idSafe(e.step,"step")}`),state:Number.isFinite(e.dur)?"completed":"working",started_at:dateAt(at,basis)});
          if(idSafe(e.step,null))data.node=e.step;
          if (Number.isFinite(e.dur)) data.ended_at=dateAt(at+Math.max(0,e.dur),basis);
        } else if (e.type==="wait") {
          type="com.exomachina.run.state_changed.v1"; Object.assign(data,{state:"waiting",phase:e.human===true?"awaiting-human":"awaiting-director",node:idSafe(e.step,"unknown-step"),wait_role:e.human===true?"human":"director",wait_actor_identity:idSafe(e.responder,e.human===true?"demo-human":"demo-director"),wait_started_at:dateAt(at,basis),permitted_actions:Array.isArray(e.allowed)?[...new Set(e.allowed.map(enumSafe))].slice(0,16):[]});
          const repairCount=safeCount(e.repair_count),maxRepairs=safeCount(e.max_repairs);
          if(repairCount!==null)data.repair_count=repairCount;
          if(maxRepairs!==null)data.max_repairs=maxRepairs;
        } else if (e.type==="admit") {
          const capacityLimit=Number.isSafeInteger(e.capacity_limit)&&e.capacity_limit>=0?e.capacity_limit:(Number.isSafeInteger(fixture.admission?.limit)&&fixture.admission.limit>=0?fixture.admission.limit:null);
          if(capacityLimit!==null){
            type="com.exomachina.admission.state_changed.v1";
            Object.assign(data,{admission_id:idSafe(e.admission_id,`${runId}-admission-${i}`),state:enumSafe(e.state??"admitted"),capacity_limit:capacityLimit});
            if(Number.isSafeInteger(e.queue_position)&&e.queue_position>=0)data.queue_position=e.queue_position;
          }
        } else if (e.type==="end"||e.type==="jobend") {
          type="com.exomachina.run.state_changed.v1"; Object.assign(data,{state:enumSafe(e.outcome??"unknown"),phase:"ended",ended_at:dateAt(at,basis)});
        } else if (e.type==="decide") {
          type="com.exomachina.decision.outcome.v1"; Object.assign(data,{decision_id:idSafe(`${runId}-decision-${i}`,`${runId}-decision`),action:enumSafe(e.action),outcome:"applied"});
        } else if (e.type==="spawn" && digestSafe(e.sha) && e.rev) {
          type="com.exomachina.artifact.revised.v1"; Object.assign(data,{artifact_revision:idSafe(e.rev,`${runId}-revision-${i}`),artifact_sha256:e.sha});
        }
        if (type) pendingFrames.push({seconds:at,basis,sequence:sequence++,event:await eventFor(factoryId,runId,type,data,at,`${runId}:${i}:${e.type}`,basis)});
        const illustration={schema_version:1,factory_id:factoryId,run_id:runId,task_id:taskId,context_id:contextId,illustration:illustrationFor(e,multiJob?runId:undefined,i)};
        pendingFrames.push({seconds:at,basis,sequence:sequence++,event:await eventFor(factoryId,runId,DEMO_ILLUSTRATION_EVENT_TYPE,illustration,at,`${runId}:${i}:${e.type}:illustration`,basis)});
      }
    }
    for(const {event:e,index:i,seconds:at} of factoryWide){
      const illustration={schema_version:1,factory_id:factoryId,illustration:illustrationFor(e,null,i)};
      pendingFrames.push({seconds:at,basis,sequence:sequence++,event:await eventFor(factoryId,null,DEMO_ILLUSTRATION_EVENT_TYPE,illustration,at,`${parentRunId}:${i}:${e.type}:factory-illustration`,basis)});
    }
    const presentation={id:parentRunId,run_ids:runIds,start:new Date(basis).toISOString(),multi:multiJob};
    for(const key of ["name","brief","outcomeLabel","tone"])if(typeof run[key]==="string")presentation[key]=boundedText(run[key]);
    for(const key of ["startAt","now"])if(finite(run[key]))presentation[key]=run[key];
    if(typeof run.live==="boolean")presentation.live=run.live;
    runPresentations.push(presentation);
  }
  pendingFrames.sort((a,b)=>(a.basis+a.seconds*1000)-(b.basis+b.seconds*1000)||a.sequence-b.sequence);
  const frames=[];
  for(let i=0;i<pendingFrames.length;i++)frames.push({op:"event",cursor:await cursorFor(factoryId,`${factoryId}:event:${i}`),event:pendingFrames[i].event});
  const capturedAt=new Date(Number.isFinite(earliestMs)?earliestMs:BASE_TIME).toISOString();
  const snapshot=validateSnapshot({schema_version:1,cursor:await cursorFor(factoryId,`${factoryId}:snapshot`),captured_at:capturedAt,freshness:{status:"fresh",observed_at:capturedAt},state:{factory,runs:snapshotRuns,active_publication:publication,capacity,commercial:{usage:[],obligations:[],payments:[]},demo:presentationFor(fixture,runPresentations)}},DEMO);
  return {snapshot,frames,actionPlans};
}

export async function createDemoAdapter({fixtures=[],onFrame=()=>{}}={}) {
  const bundles=new Map(); for (const fixture of fixtures) { const bundle=await fixtureBundle(fixture); bundles.set(bundle.snapshot.state.factory.id,bundle); }
  let selected=[...bundles.keys()][0]??null, eventOrdinal=100000;
  const observers=new Set(),commands=new Map(),runStates=new Map(),activeWaits=new Map(),runTimes=new Map();
  for(const [factoryId,bundle] of bundles){
    for(const run of bundle.snapshot.state.runs)runStates.set(`${factoryId}:${run.id}`,run.status.state);
    for(const [runId,waits] of bundle.actionPlans)activeWaits.set(`${factoryId}:${runId}`,structuredClone(waits));
  }
  const track=frame=>{
    if(frame.op!=="event")return;
    const event=frame.event,data=event.data,key=`${data.factory_id}:${data.run_id}`;
    if(["com.exomachina.run.created.v1","com.exomachina.run.state_changed.v1"].includes(event.type))runStates.set(key,data.state);
    const eventTime=Date.parse(event.time),previous=runTimes.get(key);
    if(Number.isFinite(eventTime)&&(!Number.isFinite(previous)||eventTime>previous))runTimes.set(key,eventTime);
  };
  const emit=(frame,factoryId=null,runId=null)=>{
    const copy=structuredClone(frame);track(copy);onFrame(copy);
    for(const observer of observers)if(observer.factoryId===factoryId&&(runId==null||observer.runId==null||observer.runId===runId))observer.callback(structuredClone(copy));
  };
  const get=id=>bundles.get(id??selected);
  return {
    source:"demo",
    async discover(){return [...bundles.values()].map(({snapshot})=>({id:snapshot.state.factory.id,name:snapshot.state.factory.name,schema_version:1,source:"demo"}));},
    async snapshot(factoryId=selected){const bundle=get(factoryId);if(!bundle)throw new Error("demo factory is unavailable");selected=factoryId;return structuredClone(bundle.snapshot);},
    observe(factoryId=selected,afterCursor=null,runId=null,onObservedFrame=()=>{}){
      const bundle=get(factoryId);if(!bundle)throw new Error("demo factory is unavailable");selected=factoryId;
      const filtered=bundle.frames.filter(f=>!runId||f.event.data.run_id===runId);let index=afterCursor?filtered.findIndex(f=>f.cursor===afterCursor)+1:0;if(index<0)index=0;
      const observer={factoryId,runId,callback:onObservedFrame};observers.add(observer);
      const publish=frame=>{const copy=structuredClone(frame);track(copy);onFrame(copy);onObservedFrame(copy);};
      publish({op:"snapshot",snapshot:structuredClone(bundle.snapshot)});
      const sub={next(){const frame=filtered[index++];if(frame)publish(frame);return frame??null;},play(){while(index<filtered.length)this.next();},close(){observers.delete(observer);}};
      sub.play();return sub;
    },
    async command(message){
      validateClientMessage(message); if(message.op!=="command")throw new Error("expected command");
      const bundle=get(message.factory_id); if(!bundle)throw new Error("demo factory is unavailable");
      const run=bundle.snapshot.state.runs.find(row=>row.task.id===message.task_id&&row.task.context_id===message.context_id);
      if(!run)return {lifecycle:"rejected",reason:"Demo Task/context binding is unknown for this factory.",frames:[{op:"error",code:"invalid_command",message:"Demo Task/context binding is unknown for this factory."}]};
      const fingerprint=JSON.stringify([message.factory_id,message.task_id,message.context_id,message.action,message.expected_state,message.expected_revision??null,message.expected_sha256??null]);
      const previous=commands.get(message.command_id);
      if(previous){
        if(previous.fingerprint===fingerprint)return {...structuredClone(previous.result),duplicate:true};
        return {lifecycle:"rejected",reason:"command_id was reused with different command data.",frames:[{op:"error",code:"invalid_command",message:"command_id was reused with different command data."}]};
      }
      const runKey=`${message.factory_id}:${run.id}`;
      const actualState=runStates.get(runKey)??run.status.state;
      const waits=activeWaits.get(runKey)??[];
      const action=enumSafe(message.action);
      let reason="",transition=null,wait=null;
      if(terminalStates.has(actualState)) reason=`Demo run is terminal (${actualState}); no further action is permitted.`;
      else if(message.expected_state!==actualState) reason=`Stale command: expected ${message.expected_state}, current state is ${actualState}.`;
      else if(actualState!=="waiting") reason=`Demo run is ${actualState}; actions require an active fixture wait.`;
      else if(waits.length!==1) reason=waits.length?"Demo fixture has multiple active waits and the command has no wait identifier.":"Demo fixture does not declare an active actionable wait.";
      else {
        wait=waits[0];
        if(!wait.human)reason="Demo fixture wait is not declared as human-actionable.";
        else if(!wait.allowed.has(action))reason=`Action ${action} is not allowed by the active fixture wait.`;
        else if(action==="abort")transition={state:"aborted",phase:"ended",ended_at:true};
        else if(action==="hold")transition={state:"held",phase:"held"};
        else if(action==="one_more_repair"||action==="repair"){
          if(wait.repairCount===null||wait.maxRepairs===null)reason="The fixture declares a repair action but provides no finite repair_count/max_repairs bound; no repair was applied.";
          else if(wait.repairCount>=wait.maxRepairs)reason=`The fixture repair bound is exhausted (${wait.repairCount}/${wait.maxRepairs}).`;
          else transition={state:"working",phase:"repair",repair_count:wait.repairCount+1,max_repairs:wait.maxRepairs};
        }else reason=`The fixture declares ${action}, but Demo has no bounded local transition for it; no approval or artifact delivery was inferred.`;
      }
      const applied=!!transition;
      const resultingState=transition?.state??actualState;
      const startedMs=Date.parse(run.started_at??"");
      const atMs=(runTimes.get(runKey)??(Number.isFinite(startedMs)?startedMs:BASE_TIME))+1;
      const seconds=(atMs-BASE_TIME)/1000;
      const received={op:"command_ack",command_id:message.command_id,lifecycle:"received"};
      const data={schema_version:1,factory_id:message.factory_id,run_id:run.id,task_id:run.task.id,context_id:run.task.context_id,command_id:message.command_id,lifecycle:applied?"applied":"rejected",outcome:applied?"applied":"rejected",expected_state:message.expected_state,resulting_state:resultingState};
      const event=await eventFor(message.factory_id,run.id,"com.exomachina.command.outcome.v1",data,seconds,`demo-command:${message.command_id}:${fingerprint}:outcome`);
      const outcome={op:"event",cursor:await cursorFor(message.factory_id,`${message.factory_id}:command:${eventOrdinal++}`),event};
      let transitionFrame=null;
      if(applied){
        const stateData={schema_version:1,factory_id:message.factory_id,run_id:run.id,task_id:run.task.id,context_id:run.task.context_id,state:transition.state,phase:transition.phase};
        if(transition.ended_at)stateData.ended_at=new Date(atMs).toISOString();
        if(transition.repair_count!==undefined)stateData.repair_count=transition.repair_count;
        if(transition.max_repairs!==undefined)stateData.max_repairs=transition.max_repairs;
        const stateEvent=await eventFor(message.factory_id,run.id,"com.exomachina.run.state_changed.v1",stateData,seconds,`demo-command:${message.command_id}:${fingerprint}:state`);
        transitionFrame={op:"event",cursor:await cursorFor(message.factory_id,`${message.factory_id}:command:${eventOrdinal++}`),event:stateEvent};
        activeWaits.delete(runKey);
      }
      const result={lifecycle:applied?"applied":"rejected",...(reason?{reason}:{}),received,outcome,transition:transitionFrame,frames:[received,outcome]};
      commands.set(message.command_id,{fingerprint,result:structuredClone(result)});
      emit(received,message.factory_id,run.id);emit(outcome,message.factory_id,run.id);if(transitionFrame)emit(transitionFrame,message.factory_id,run.id);
      return structuredClone(result);
    },
    async submit(request){return typeof request?.text==="string"&&request.text.trim()?{lifecycle:"local_only",reason:"Demo chat remains in the local floor simulation."}:{lifecycle:"rejected",reason:"Demo accepts only local chat text."};},
    async inspect_artifact(){return {available:false,reason:"Demo scenario contains no artifact bytes."};},
    close(){observers.clear();commands.clear();},
  };
}
