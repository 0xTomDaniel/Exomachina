import { validateClientMessage, validateSubmissionReadiness, validateDiscovery, validateServerMessage, verifyArtifactBytes } from "../contract.mjs";
import { validateMeasurementsResponse } from "../usage.mjs";

function listen(socket, type, fn) {
  if (socket.addEventListener) socket.addEventListener(type, fn);
  else socket[`on${type}`] = fn;
}
const ready = socket => socket.readyState === 1 || socket.readyState === socket.OPEN;

/**
 * Runtime-backed browser adapter. The bootstrap supplies the WebSocket URL and
 * optional same-origin HTTP routes; the adapter never transports credentials
 * in a URL or application message.
 */
export function createLiveAdapter({
  observationEndpoint,
  discoveryEndpoint,
  artifactEndpoint,
  a2aEndpoint,
  a2aMessageSendEndpoint,
  usageEndpoint,
  submissionReadinessEndpoint,
  principalResolverReady = false,
  snapshotTimeoutMs = 12_000,
  discoveryTimeoutMs = 12_000,
  socketFactory = url => new WebSocket(url),
  fetcher = globalThis.fetch,
  credentials = "same-origin",
  discover:discoverFn,
  inspectArtifact:inspectFn,
  submit:submitFn,
  onStatus = () => {},
} = {}) {
  let activeSocket = null;
  let activeFactory = null;
  let activeApplicationReady = false;
  const pendingAcks = new Map();
  const snapshotFreshness = new Map();
  const selectionKey=(factoryId,runId)=>`${factoryId}\u0000${runId||""}`;

  const unauthenticated = () => {
    const message="Unauthenticated: the server-side Observation principal resolver/session is not selected.";
    onStatus("unauthenticated",message);throw new Error(message);
  };
  const ensureEndpoint = () => {
    if (!principalResolverReady) unauthenticated();
    if (!observationEndpoint) throw new Error("Observation WebSocket endpoint is not configured by server bootstrap");
  };
  const asUrl = template => {
    if (typeof template === "function") return template;
    if (!template) return null;
    return ({run_id,revision,sha256}) => {
      const url=new URL(template,globalThis.location?.href ?? "http://localhost/");
      if(run_id!=null)url.searchParams.set("run_id",run_id);
      if(revision!=null)url.searchParams.set("revision",revision);
      if(sha256!=null)url.searchParams.set("sha256",sha256);
      return url.toString();
    };
  };
  const artifactUrl=asUrl(artifactEndpoint);
  const messageSendEndpoint=a2aMessageSendEndpoint??a2aEndpoint;

  return {
    source:"live",
    async discover() {
      if(!principalResolverReady)unauthenticated();
      if (discoverFn) return discoverFn();
      if (!discoveryEndpoint || typeof fetcher !== "function") throw new Error("factory discovery endpoint is not configured by server bootstrap");
      const controller=new AbortController();
      const timer=setTimeout(()=>controller.abort(),Math.min(12_000,Math.max(1,Number(discoveryTimeoutMs)||12_000)));
      try{
        const response=await fetcher(discoveryEndpoint,{credentials,signal:controller.signal,headers:{Accept:"application/json"}});
        if(!response.ok)throw new Error(`factory discovery failed (${response.status})`);
        const body=await response.json();
        return validateDiscovery(body).factories;
      }catch(error){if(controller.signal.aborted)throw new Error('Factory discovery timed out; use Reconnect or reload.');throw error;}
      finally{clearTimeout(timer);}
    },
    snapshot(factoryId,runId) {
      ensureEndpoint();
      snapshotFreshness.delete(selectionKey(factoryId,runId));
      return new Promise((resolve,reject)=>{
        const socket=socketFactory(observationEndpoint); let settled=false;
        let timer=null;
        const fail=error=>{if(!settled){settled=true;if(timer!==null)clearTimeout(timer);try{socket.close();}catch{}reject(error);}};
        const timeout=Math.min(12_000,Math.max(1,Number(snapshotTimeoutMs)||12_000));
        timer=setTimeout(()=>fail(new Error("Observation WebSocket timed out before its snapshot")),timeout);
        listen(socket,"close",event=>fail(new Error(event?.reason||"Observation WebSocket closed before its snapshot")));
        listen(socket,"error",()=>fail(new Error("Observation WebSocket failed before its snapshot")));
        listen(socket,"message",event=>{
          let frame;try{frame=validateServerMessage(JSON.parse(event.data));}catch(error){fail(error);return;}
          if(frame.op==="snapshot"){
            if(frame.snapshot.state.factory.id!==factoryId){fail(new Error("snapshot factory does not match subscription"));return;}
            snapshotFreshness.set(selectionKey(factoryId,runId),frame.snapshot.freshness.status==="fresh");
            settled=true;if(timer!==null)clearTimeout(timer);socket.close();resolve(frame.snapshot);
          }
          else if(frame.op==="error")fail(new Error(`${frame.code}: ${frame.message}`));
        });
        listen(socket,"open",()=>{
          const message={op:"subscribe",factory_id:factoryId,...(runId?{run_id:runId}:{})};
          try{validateClientMessage(message);socket.send(JSON.stringify(message));}catch(error){fail(error);}
        });
      });
    },
    observe(factoryId,afterCursor=null,runId=null,onFrame=()=>{},statusHandler=onStatus) {
      ensureEndpoint(); activeFactory=factoryId;
      const socket=socketFactory(observationEndpoint); activeSocket=socket; let closed=false,applicationReady=false;
      let baselineFresh=snapshotFreshness.get(selectionKey(factoryId,runId))===true,requiresSnapshot=!baselineFresh;
      activeApplicationReady=false;
      statusHandler("connecting");
      listen(socket,"open",()=>{
        statusHandler("connecting","WebSocket open; waiting for a current Observation frame.");
        const message={op:"subscribe",factory_id:factoryId,...(runId?{run_id:runId}:{}),...(afterCursor!=null?{after_cursor:afterCursor}:{})};
        try{validateClientMessage(message);socket.send(JSON.stringify(message));}catch(error){statusHandler("error",error.message);socket.close();}
      });
      listen(socket,"message",event=>{
        if(closed)return;
        let frame;
        try{frame=validateServerMessage(JSON.parse(event.data));}
        catch(error){applicationReady=false;requiresSnapshot=true;activeApplicationReady=false;snapshotFreshness.set(selectionKey(factoryId,runId),false);statusHandler("error",`Invalid Observation v1 frame: ${error.message}`);socket.close(1002,"invalid v1 frame");return;}
        if(frame.op==="snapshot"&&frame.snapshot.state.factory.id!==factoryId){applicationReady=false;requiresSnapshot=true;activeApplicationReady=false;snapshotFreshness.set(selectionKey(factoryId,runId),false);statusHandler("error","snapshot factory does not match subscription");socket.close(1002,"factory mismatch");return;}
        if(frame.op==="snapshot"){
          baselineFresh=frame.snapshot.freshness.status==="fresh";
          snapshotFreshness.set(selectionKey(factoryId,runId),baselineFresh);
          requiresSnapshot=!baselineFresh;applicationReady=baselineFresh;activeApplicationReady=activeSocket===socket&&baselineFresh;
        }
        else if(frame.op==="resync_required"){baselineFresh=false;applicationReady=false;requiresSnapshot=true;activeApplicationReady=false;snapshotFreshness.set(selectionKey(factoryId,runId),false);}
        else if(frame.op==="error"){baselineFresh=false;applicationReady=false;requiresSnapshot=true;activeApplicationReady=false;snapshotFreshness.set(selectionKey(factoryId,runId),false);}
        else if(frame.op==="checkpoint"&&baselineFresh&&!requiresSnapshot){applicationReady=true;activeApplicationReady=activeSocket===socket;}
        if(frame.op==="command_ack"){
          const resolve=pendingAcks.get(frame.command_id);if(resolve){resolve({lifecycle:"received"});pendingAcks.delete(frame.command_id);}
        }
        onFrame(frame);
        if(frame.op==="snapshot"&&baselineFresh)statusHandler("connected");
        else if(frame.op==="snapshot")statusHandler("error",`Observation snapshot freshness is ${frame.snapshot.freshness.status}; actions are unavailable until a fresh snapshot arrives.`);
        else if(frame.op==="checkpoint"&&baselineFresh&&!requiresSnapshot)statusHandler("connected");
        else if(frame.op==="resync_required")statusHandler("error",`Observation resynchronization required (${frame.reason}); a fresh snapshot is required.`);
        else if(frame.op==="error")statusHandler("error",`${frame.code}: ${frame.message}`);
        else if(!applicationReady)statusHandler(requiresSnapshot?"error":"connecting",requiresSnapshot?"Waiting for a fresh snapshot after resynchronization.":"Waiting for a current snapshot or catch-up checkpoint.");
      });
      listen(socket,"close",event=>{
        if(closed)return;closed=true;if(activeSocket===socket){activeSocket=null;activeApplicationReady=false;}
        statusHandler("disconnected",event?.reason||"Observation socket closed");
        for(const [id,resolve] of pendingAcks){resolve({lifecycle:"unknown",reason:"socket disconnected before acknowledgement"});pendingAcks.delete(id);}
      });
      listen(socket,"error",()=>{if(activeSocket===socket)activeApplicationReady=false;applicationReady=false;requiresSnapshot=true;snapshotFreshness.set(selectionKey(factoryId,runId),false);statusHandler("error","Observation WebSocket transport error");});
      return {
        close(){closed=true;if(activeSocket===socket){activeSocket=null;activeApplicationReady=false;}socket.close();},
        command:message=>this.command(message),
        socket,
      };
    },
    command(message) {
      validateClientMessage(message);
      if(message.op!=="command")throw new Error("expected Observation command message");
      if(message.factory_id!==activeFactory)throw new Error("command factory must match the active observation subscription");
      if(!activeSocket||!ready(activeSocket))throw new Error("Observation socket is not connected");
      if(!activeApplicationReady)throw new Error("Observation stream has not supplied a fresh snapshot or catch-up checkpoint");
      if(pendingAcks.has(message.command_id))return Promise.reject(new Error("command acknowledgement is already pending"));
      return new Promise((resolve,reject)=>{
        pendingAcks.set(message.command_id,resolve);
        try{activeSocket.send(JSON.stringify(message));}
        catch(error){pendingAcks.delete(message.command_id);reject(error);}
      });
    },
    async inspect_artifact(runId,revision,sha256) {
      if(inspectFn)return inspectFn({run_id:runId,revision,sha256});
      if(!artifactUrl||typeof fetcher!=="function")return {available:false,reason:"artifact endpoint is not configured by server bootstrap"};
      const response=await fetcher(artifactUrl({run_id:runId,revision,sha256}),{credentials,headers:{Accept:"application/octet-stream, application/json"}});
      if(!response.ok)return {available:false,reason:`artifact request failed (${response.status})`};
      const bytes=await response.arrayBuffer();const check=await verifyArtifactBytes(bytes,sha256);
      return {available:check.valid,bytes,sha256:check.actual,valid:check.valid,contentType:response.headers.get("content-type"),reason:check.valid?"":"artifact digest mismatch"};
    },
    async measurements(filters = {}) {
      if(!principalResolverReady)unauthenticated();
      if(!usageEndpoint||typeof fetcher!=="function")throw new Error("token measurement reader is not configured by server bootstrap");
      const allowed=new Set(["run_id","task_id","assignment_id","attempt_id","model_call_id","call_scope"]);
      for(const [key,value] of Object.entries(filters))if(!allowed.has(key)||typeof value!=="string"||!value.length)throw new Error("invalid token measurement filter");
      const base=globalThis.location?.href??"http://localhost/";
      const url=new URL(usageEndpoint,base);
      if(url.origin!==new URL(base).origin)throw new Error("token measurement reader must be same-origin");
      for(const [key,value]of Object.entries(filters))url.searchParams.set(key,value);
      const response=await fetcher(url.toString(),{credentials,headers:{Accept:"application/json"}});
      if(response.status===401||response.status===403)throw new Error("token measurement session is unauthorized");
      if(!response.ok&&response.status!==503)throw new Error(`token measurement request failed (${response.status})`);
      return validateMeasurementsResponse(await response.json());
    },
    submissionConnectionReady(){return !!activeSocket&&ready(activeSocket);},
    async submissionReadiness(factoryId=activeFactory) {
      if(!principalResolverReady)unauthenticated();
      if(!submissionReadinessEndpoint||!factoryId)throw new Error('Current factory submission readiness is not configured.');
      const base=globalThis.location?.href??'http://localhost/';
      const url=new URL(submissionReadinessEndpoint,base);
      if(url.origin!==new URL(base).origin)throw new Error('Submission readiness must be same-origin.');
      const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),5000);
      try{
        const response=await fetcher(url.toString(),{credentials:'same-origin',cache:'no-store',headers:{Accept:'application/json'},signal:controller.signal});
        if(!response.ok)throw new Error(`Current submission readiness unavailable (${response.status}).`);
        const result=validateSubmissionReadiness(await response.json(),factoryId);
        const age=Date.now()-Date.parse(result.observed_at);
        if(age>30000||age < -30000)throw new Error('Current factory readiness timestamp is stale or clock is inconsistent.');
        return result;
      }finally{clearTimeout(timer);}
    },
    async submit(request) {
      if(!principalResolverReady)return {lifecycle:"unavailable",reason:"Unauthenticated: the server-side Observation principal resolver is not selected."};
      const taskBound=!!request?.params?.message?.taskId;
      if(taskBound&&!request.params.message.contextId)return {lifecycle:'unavailable',reason:'An existing Task message requires its original context.'};
      if(!activeSocket||!ready(activeSocket)||((taskBound||!submissionReadinessEndpoint)&&!activeApplicationReady))return {lifecycle:"unavailable",reason:"Live Observation is not fresh and connected; A2A submission is unavailable."};
      if(submissionReadinessEndpoint&&!taskBound){
        const submittingFactory=activeFactory,submittingSocket=activeSocket;
        const current=await this.submissionReadiness(submittingFactory);
        if(activeFactory!==submittingFactory||activeSocket!==submittingSocket||!ready(submittingSocket))return {lifecycle:'unavailable',reason:'Factory selection or connection changed before submission.'};
        if(current.status!=='ready')return {lifecycle:'unavailable',reason:`Current factory cannot accept work: ${current.reason_code}.`};
      }
      if(submitFn)return submitFn(request);
      if(!messageSendEndpoint||typeof fetcher!=="function")return {lifecycle:"unavailable",reason:"A2A message/send endpoint is not configured by server bootstrap"};
      const response=await fetcher(messageSendEndpoint,{method:"POST",credentials:"same-origin",headers:{"Content-Type":"application/json",Accept:"application/json"},body:JSON.stringify(request)});
      const body=await response.json().catch(()=>({}));
      if(response.status===401||response.status===403)return {lifecycle:"unavailable",reason:"Unauthenticated: the same-origin server session is not authorized for A2A message/send."};
      if(!response.ok)throw new Error(`A2A submission failed (${response.status})`);
      return body;
    },
    close(){activeSocket?.close();activeSocket=null;activeFactory=null;},
  };
}
