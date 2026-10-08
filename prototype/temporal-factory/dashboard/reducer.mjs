import { DEMO_ILLUSTRATION_EVENT_TYPE, DashboardContractError, HANDOFF_EVENT_TYPES, validateServerMessage, validateSnapshot } from "./contract.mjs";

const clone = value => structuredClone(value);
const values = value => value && typeof value === "object" ? Object.values(value) : [];
const terminal = new Set(["completed", "accepted", "failed", "aborted", "expired", "closed", "cancelled", "canceled", "released"]);
const eventType = suffix => `com.exomachina.${suffix}.v1`;
const MAX_ILLUSTRATIONS = 8192;

function newRun(runId) {
  return { run_id:runId, started_at:null, pinned:{}, state:null, assignments:{}, artifacts:[], quality:[], decisions:[], commands:[], delivery:[], incidents:[], admissions:[], illustrations:[] };
}
// Content-free hand-off facts are kept per run, keyed by their identities so
// at-least-once replays and duplicate emissions do not double a carrier.
const HANDOFF_KEYS = {
  "com.exomachina.handoff.produced.v1": ["produced", d => `${d.handoff_id}\u0000${d.handoff_revision}`],
  "com.exomachina.handoff.consumed.v1": ["consumed", d => `${d.assignment_id}\u0000${d.attempt_id}\u0000${d.node}\u0000${d.consumed_at}`],
  "com.exomachina.handoff.item_ready.v1": ["ready", d => `${d.assignment_id}\u0000${d.attempt_id}\u0000${d.handoff_id}\u0000${d.item_index}`],
};
function addHandoff(run, type, data) {
  const [list, key] = HANDOFF_KEYS[type];
  const rows = (run.handoffs ??= { produced:[], consumed:[], ready:[] })[list];
  const id = key(data), index = rows.findIndex(row => key(row) === id);
  if (index >= 0) rows[index] = clone(data); else rows.push(clone(data));
  if (rows.length > 256) rows.splice(0, rows.length - 256);
}
// Event-stream graph nodes list every pinned edge in `next`; a node that also
// declares `control` gives each edge its kind (the subset only sequences work).
const graphNodeEdges = n => (n.next ?? []).map(to => Array.isArray(n.control) ? { from:n.id, to, kind:n.control.includes(to) ? "control" : "material" } : { from:n.id, to });
function addBounded(rows, data, key) {
  const id = key ? data[key] : null;
  const index = id ? rows.findIndex(row => row[key] === id) : -1;
  if (index >= 0) rows[index] = { ...rows[index], ...clone(data) };
  else rows.push(clone(data));
  if (rows.length > 256) rows.splice(0, rows.length - 256);
}
function addArtifact(rows,data){
  const i=rows.findIndex(row=>row.artifact_revision===data.artifact_revision&&row.artifact_sha256===data.artifact_sha256);
  if(i>=0)rows[i]={...rows[i],...clone(data)};else rows.push(clone(data));
  if(rows.length>256)rows.splice(0,rows.length-256);
}
function addDelivery(rows,data){
  const prior=rows.find(row=>row.receipt_id===data.receipt_id);
  const fields=["factory_id","run_id","task_id","context_id","artifact_revision","artifact_sha256","markdown_sha256","destination_id","destination_identity","delivery_kind","outcome","delivered_at","byte_length"];
  const conflicts=new Set(prior?.receipt_conflict_fields??[]);
  if(prior)for(const field of fields)if(prior[field]!=null&&data[field]!=null&&prior[field]!==data[field])conflicts.add(field);
  addBounded(rows,data,"receipt_id");
  if(conflicts.size){const current=rows.find(row=>row.receipt_id===data.receipt_id);current.receipt_conflict=true;current.receipt_conflict_fields=[...conflicts].sort();}
}
function replaceByKey(rows,data,keys){
  const index=rows.findIndex(row=>keys.every(key=>row[key]===data[key]));
  if(index>=0)rows[index]=clone(data);else rows.push(clone(data));
  if(rows.length>256)rows.splice(0,rows.length-256);
}
function currentIncidents(rows){
  const current=[];
  for(const row of rows??[]){
    if(row.incident_id)replaceByKey(current,row,["incident_id"]);
    else current.push(clone(row));
  }
  return current;
}
function replaceObligation(rows,data){
  const existing=rows.find(row=>row.obligation_id===data.obligation_id);
  if(existing&&existing.component!==data.component)throw new DashboardContractError("obligation_id is already assigned to another component","$event.data.obligation_id");
  replaceByKey(rows,data,["obligation_id"]);
}
function arraysFromSnapshot(snapshot) {
  const wire = snapshot.state;
  const runs = new Map(wire.runs.map(row=>{
    const assignments={};
    for(const assignment of row.assignments??[]){const attempts={};for(let i=0;i<(assignment.attempts??[]).length;i++){const attempt=assignment.attempts[i];attempts[attempt.attempt_id??attempt.id??String(i)]=clone(attempt);}assignments[assignment.id]=attempts;}
    // Snapshot journals retain source facts; materialize the same identities
    // as the stream so refresh does not duplicate displayed outputs.
    const artifacts=[],delivery=[];
    for(const artifact of row.artifacts??[])addArtifact(artifacts,artifact);
    for(const receipt of row.delivery??[])addDelivery(delivery,receipt);
    const status=typeof row.status==="string"?{state:row.status}:clone(row.status);
    const handoffRun={};
    for(const [list,type] of [["produced","com.exomachina.handoff.produced.v1"],["consumed","com.exomachina.handoff.consumed.v1"],["ready","com.exomachina.handoff.item_ready.v1"]])for(const data of row.handoffs?.[list]??[])addHandoff(handoffRun,type,data);
    const internal={run_id:row.id,...(handoffRun.handoffs?{handoffs:handoffRun.handoffs}:{}),graph:clone(row.graph??null),task:clone(row.task),state:{...status,...(row.started_at?{started_at:row.started_at}:{})},started_at:row.started_at??status.started_at??null,pinned:clone(row.pinned),assignments,artifacts,quality:clone(row.quality),decisions:clone(row.decisions),commands:clone(row.commands),delivery,incidents:currentIncidents(row.incidents),admissions:clone(row.admissions),illustrations:[],model_label:row.model_label,fixture_label:row.fixture_label};
    return [row.id,internal];
  }));
  return { factoryId:wire.factory.id, factory:clone(wire.factory), publication:clone(wire.active_publication), capacity:clone(wire.capacity), commercial:clone(wire.commercial), runs, demo:clone(wire.demo ?? null), illustrations:[] };
}
function mergeCloudEvent(state, event) {
  const d = event.data;
  if (d.factory_id !== state.factoryId) throw new DashboardContractError("event factory does not match subscription", "$event.data.factory_id");
  if (event.type === DEMO_ILLUSTRATION_EVENT_TYPE) {
    // Validation already restricted this layer to the isolated Demo source.
    const row = { ...clone(d.illustration), event_id:event.id };
    let rows;
    if (d.run_id) {
      let run = state.runs.get(d.run_id);
      if (!run) { run = newRun(d.run_id); state.runs.set(d.run_id, run); }
      if (d.task_id) run.task = { id:d.task_id, context_id:d.context_id ?? run.task?.context_id ?? null };
      rows = run.illustrations ??= [];
    } else rows = state.illustrations ??= [];
    if (rows.some(entry => entry.event_id === event.id)) return; // at-least-once replay
    rows.push(row);
    if (rows.length > MAX_ILLUSTRATIONS) rows.splice(0, rows.length - MAX_ILLUSTRATIONS);
    return;
  }
  if (event.type === eventType("factory.discovered")) state.factory = { ...(state.factory??{}), id:d.factory_id, name:d.name, identity:d.identity, capability:d.capability, graph:state.factory?.graph??{nodes:[],edges:[]} };
  else if (event.type === eventType("publication.activated")) {
    state.publication = clone(d);
    if(Array.isArray(d.graph_nodes)){
      const nodes=d.graph_nodes.map(n=>({id:n.id,name:n.name??n.id,type:n.type,kind:n.type,...(n.capability?{capability:n.capability,agent:n.capability}:{}),...(n.output?{output:n.output}:{})}));
      const edges=d.graph_nodes.flatMap(graphNodeEdges);
      state.factory={...(state.factory??{}),id:state.factoryId,name:state.factory?.name??state.factoryId,graph:{nodes,edges},agent_bindings:clone(d.service_bindings??[])};
    }
  }
  else if (event.type === eventType("capacity.state_changed")) state.capacity = clone(d);

  const runId = d.run_id;
  if (event.type.startsWith("com.exomachina.commercial.")) {
    const category = event.type === eventType("commercial.usage") ? "usage" : event.type === eventType("commercial.obligation") ? "obligations" : "payments";
    if(category==="obligations")replaceObligation(state.commercial[category],d);
    else replaceByKey(state.commercial[category], d, [category === "usage" ? "usage_id" : "payment_id"]);
  }
  if (!runId) return;
  let run = state.runs.get(runId);
  if (!run) { run = newRun(runId); run.task={id:d.task_id??null,context_id:d.context_id??null}; state.runs.set(runId, run); }
  if(d.task_id)run.task={...(run.task??{}),id:d.task_id,context_id:d.context_id??run.task?.context_id??null};
  for (const k of ["manifest_digest", "package_digest", "definition_digest", "interpreter_build"]) if (d[k]) {
    if(run.pinned[k] && run.pinned[k] !== d[k])throw new DashboardContractError("run pin changed", `$event.data.${k}`);
    run.pinned[k] = d[k];
  }
  if(event.type === eventType("run.created") && Array.isArray(d.graph_nodes)){
    const graph={nodes:d.graph_nodes.map(n=>({id:n.id,kind:n.type,...(n.capability?{capability:n.capability}: {}),...(n.output?{output:n.output}:{})})),edges:d.graph_nodes.flatMap(graphNodeEdges)};
    if(run.graph && JSON.stringify(run.graph)!==JSON.stringify(graph))throw new DashboardContractError("pinned run graph changed","$event.data.graph_nodes");
    run.graph=graph;
  }
  switch (event.type) {
    case eventType("run.created"):
    case eventType("run.state_changed"):
      if (d.started_at && (!run.started_at || Date.parse(d.started_at) < Date.parse(run.started_at))) run.started_at = d.started_at;
      run.state = clone(d); break;
    case eventType("assignment.state_changed"):
      run.assignments[d.assignment_id] ??= {};
      const previous=run.assignments[d.assignment_id][d.attempt_id]??{};
      const assignment=clone(d);
      for(const field of ["started_at","provider_identity","node"])if(!Object.hasOwn(assignment,field)&&Object.hasOwn(previous,field))assignment[field]=previous[field];
      if(["scheduled","running","working","active","unknown"].includes(d.state)&&!Object.hasOwn(d,"ended_at"))delete assignment.ended_at;
      run.assignments[d.assignment_id][d.attempt_id] = assignment; break;
    case eventType("artifact.revised"): addArtifact(run.artifacts, d); break;
    case eventType("quality.verdict"): addBounded(run.quality, d, "artifact_revision"); break;
    case eventType("decision.outcome"): addBounded(run.decisions, d, "decision_id"); break;
    case eventType("command.outcome"): addBounded(run.commands, d, "command_id"); break;
    case eventType("delivery.receipt"): addDelivery(run.delivery, d); break;
    case eventType("incident.state_changed"): replaceByKey(run.incidents, d, ["incident_id"]); break;
    case eventType("admission.state_changed"): addBounded(run.admissions, d, "admission_id"); break;
    case eventType("commercial.usage"): replaceByKey(run.commercial_usage ??= [], d, ["usage_id"]); break;
    case eventType("commercial.obligation"): replaceObligation(run.commercial_obligations ??= [], d); break;
    case eventType("commercial.payment"): replaceByKey(run.commercial_payments ??= [], d, ["payment_id"]); break;
    default: if (HANDOFF_EVENT_TYPES.includes(event.type)) addHandoff(run, event.type, d); break;
  }
}

export function createDashboardState(snapshot, { source = "live", gap = false } = {}) {
  validateSnapshot(snapshot, { source });
  return {
    schema_version:1, source, factoryId:snapshot.state.factory.id, cursor:snapshot.cursor,
    captured_at:snapshot.captured_at, freshness:clone(snapshot.freshness), ...arraysFromSnapshot(snapshot),
    events:[], seenEventIds:new Set(), commandAcks:new Map(), transport:{status:"connected", gap, notice:gap ? "Observation gap; showing a fresh snapshot." : ""},
  };
}

export function reduceDashboard(state, frame) {
  validateServerMessage(frame, { source:state?.source ?? "live" });
  if (frame.op === "snapshot") {
    return createDashboardState(frame.snapshot, { source:state?.source ?? "live", gap:!!state?.transport?.gap });
  }
  if (!state) throw new DashboardContractError("a snapshot is required before transport messages", "$state");
  if (frame.op === "resync_required") return { ...state, freshness:{ ...state.freshness, status:"stale" }, transport:{ status:"resync_required", gap:true, notice:`Observation resynchronization required (${frame.reason}).` } };
  if (frame.op === "resumed") return { ...state, cursor:frame.continuation_cursor, transport:{ status:"connected", gap:state.transport.gap, notice:state.transport.gap ? state.transport.notice : "Observation stream resumed." } };
  if (frame.op === "checkpoint") return { ...state, cursor:frame.cursor, transport:{ status:"connected", gap:state.transport.gap, notice:state.transport.gap ? state.transport.notice : "" } };
  if (frame.op === "command_ack") {
    const acks = new Map(state.commandAcks); acks.set(frame.command_id, { lifecycle:"received" });
    return { ...state, commandAcks:acks };
  }
  if (frame.op === "error") return { ...state, transport:{ ...state.transport, notice:`${frame.code}: ${frame.message}` } };
  if (frame.op !== "event") return state;
  if (state.seenEventIds.has(frame.event.id)) return state; // stable CloudEvent IDs make at-least-once delivery idempotent.
  const runs=new Map(state.runs),runId=frame.event.data.run_id;
  if(runId&&runs.has(runId))runs.set(runId,clone(runs.get(runId)));
  // Illustrative Demo entries live only in their run/factory illustration lists
  // (deduplicated there by stable event id); they are not Observation log rows.
  const illustrative=frame.event.type===DEMO_ILLUSTRATION_EVENT_TYPE;
  const next = { ...state, commercial:frame.event.type.startsWith("com.exomachina.commercial.")?clone(state.commercial):state.commercial, illustrations:illustrative&&!runId?[...(state.illustrations??[])]:state.illustrations, runs, events:illustrative?state.events:[...state.events], seenEventIds:illustrative?state.seenEventIds:new Set(state.seenEventIds), commandAcks:new Map(state.commandAcks) };
  mergeCloudEvent(next, frame.event);
  if(!illustrative){
    next.events.push({ cursor:frame.cursor, event:clone(frame.event) });
    next.seenEventIds.add(frame.event.id);
  }
  next.cursor = frame.cursor; // opaque continuation token: retain exactly as delivered.
  // Historical backfills retain their writer time without rewinding the
  // materialized view's already observed clock.
  next.captured_at = Date.parse(frame.event.time) > Date.parse(state.captured_at) ? frame.event.time : state.captured_at;
  const observedAt = state.freshness.observed_at;
  next.freshness = { ...next.freshness, observed_at:!observedAt || Date.parse(frame.event.time) > Date.parse(observedAt) ? frame.event.time : observedAt };
  next.transport = { status:"connected", gap:state.transport.gap, notice:state.transport.gap ? state.transport.notice : "" };
  return next;
}

export function reduceTransportStatus(state, status, detail = "") {
  if (!state) return state;
  const label = status === "disconnected" ? "disconnected" : status === "unauthenticated" ? "unauthenticated" : status === "connecting" ? "connecting" : status === "error" ? "error" : "connected";
  return { ...state, transport:{ ...state.transport, status:label, notice:detail || (label === "disconnected" ? "Disconnected; showing the last received state." : state.transport.notice) }, freshness:label === "disconnected" ? { ...state.freshness, status:"disconnected" } : state.freshness };
}

export function reduceMany(state, frames) { return frames.reduce((s, frame) => reduceDashboard(s, frame), state); }

const allRunRows = (run, key) => key === "assignments" ? values(run.assignments).flatMap(values) : run[key] ?? [];
export function dashboardViewModels(state) {
  if (!state) return { decisions:[], waits:[], outputs:[], definition:{ graph_nodes:[], service_bindings:[] }, agents:[], runs:[], events:[], commercial:{usage:[],obligations:[],payments:[]} };
  const runs = [...state.runs.values()];
  const waits=runs.flatMap(run=>{
    const wait=run.state??{};
    if(terminal.has(wait.state))return [];
    if(wait.wait_role!=="director"&&wait.wait_role!=="human")return [];
    const quality=run.quality?.at(-1),artifact=run.artifacts?.at(-1);
    const candidate=quality?.accepted===false&&quality.artifact_revision&&quality.artifact_sha256&&artifact?.artifact_revision===quality.artifact_revision&&artifact?.artifact_sha256===quality.artifact_sha256?{artifact_revision:quality.artifact_revision,artifact_sha256:quality.artifact_sha256}:null;
    return [{
      run_id:run.run_id,task:clone(run.task??null),pinned:clone(run.pinned??{}),node:wait.node??null,
      wait_role:wait.wait_role,wait_actor_identity:wait.wait_actor_identity??null,
      responder:wait.wait_actor_identity??"unreported",wait_started_at:wait.wait_started_at??null,
      wait_deadline:wait.wait_deadline??null,permitted_actions:Array.isArray(wait.permitted_actions)?clone(wait.permitted_actions):null,
      candidate_refs:clone(candidate),context:null,recommendation:null,
    }];
  });
  const usage=state.commercial.usage.map(row=>({
    ...clone(row), quantity_known:Object.hasOwn(row,"quantity"),
    quantity_display:Object.hasOwn(row,"quantity")?`${row.quantity} ${row.unit}`:(row.completeness==="undisclosed"?"Undisclosed":"Unknown"),
  }));
  const obligations=state.commercial.obligations.map(row=>({
    ...clone(row), amount_known:row.amount_atoms!==null,
    amount_display:row.amount_atoms===null?(row.evidence_status==="undisclosed"?"Undisclosed":"Unknown"):`${row.amount_atoms} ${row.currency} (scale ${row.atomic_scale})`,
  }));
  return {
    decisions:runs.flatMap(run=>allRunRows(run,"decisions").map(row=>({...row,run_id:run.run_id}))),
    waits,
    outputs:runs.flatMap(run=>[...allRunRows(run,"artifacts"),...allRunRows(run,"delivery")].map(row=>({...row,run_id:run.run_id}))),
    assignments:runs.flatMap(run=>{
      const runIsTerminal=terminal.has(run.state?.state);
      return allRunRows(run,"assignments").map(row=>{
        const assignmentIsTerminal=terminal.has(row.state);
        const outcome_unknown=runIsTerminal&&!assignmentIsTerminal;
        return {
          ...row,run_id:run.run_id,
          active:!runIsTerminal&&["running","working","active"].includes(row.state),
          outcome_unknown,
          ...(outcome_unknown?{activity_label:row.state==="running"?"Historical running; outcome unreported":"Historical assignment; outcome unreported"}:{}),
        };
      });
    }),
    quality:runs.flatMap(run=>allRunRows(run,"quality").map(row=>({...row,run_id:run.run_id}))),
    commands:runs.flatMap(run=>allRunRows(run,"commands").map(row=>({...row,run_id:run.run_id}))),
    incidents:runs.flatMap(run=>allRunRows(run,"incidents").map(row=>({...row,run_id:run.run_id}))),
    admissions:runs.flatMap(run=>allRunRows(run,"admissions").map(row=>({...row,run_id:run.run_id}))),
    definition:{ graph:clone(state.factory?.graph ?? {nodes:[],edges:[]}), graph_nodes:clone(state.factory?.graph?.nodes ?? state.publication?.graph_nodes ?? []), service_bindings:clone(state.factory?.agent_bindings ?? state.publication?.service_bindings ?? []), publication:clone(state.publication), factory:clone(state.factory) },
    agents:clone(state.factory?.agent_bindings ?? state.publication?.service_bindings ?? state.factory?.agents ?? []), runs:runs.map(run=>({run_id:run.run_id,task:clone(run.task),state:clone(run.state),pinned:clone(run.pinned)})),
    events:state.events.map(row=>({cursor:row.cursor,id:row.event.id,type:row.event.type,time:row.event.time,subject:row.event.subject,data:clone(row.event.data)})),
    commercial:{ ...clone(state.commercial), usage, obligations, obligationsByComponent:Object.fromEntries([...new Set(obligations.map(row=>row.component))].map(component=>[component,obligations.filter(row=>row.component===component)])) },
  };
}

function secondsBetween(a,b) { const x=Date.parse(a), y=Date.parse(b); return Number.isFinite(x)&&Number.isFinite(y) ? Math.max(0,(x-y)/1000) : null; }
const graphKind = type => ({parallel:"fanout",synthesize:"assign",quality:"gate",route:"gate",repair:"gate",director_wait:"wait",release:"output",complete:"end",abort:"end",nested_factory:"nested"}[type] ?? (["intake","assign","nested","wait","gate","join","fanout","output","end"].includes(type) ? type : "assign"));

// These annotations belong to the renderer, never the public execution graph.
// Source nodes can include disconnected conditional branches: an attachment is
// visual presentation, not proof of the workflow's authoritative entry point.
function floorPresentation(nodes, edges) {
  const kind = node => node.kind ?? node.type;
  const terminalKinds = new Set(["complete", "end", "abort"]);
  const outputKinds = new Set(["release", "output"]);
  const ordered = rows => rows.map(node => node.id).sort();
  const incoming = new Set(edges.map(edge => edge.to));
  const outgoing = new Set(edges.map(edge => edge.from));
  const intake = ordered(nodes.filter(node => kind(node) === "intake"));
  const sources = ordered(nodes.filter(node => !incoming.has(node.id) &&
    !terminalKinds.has(kind(node)) && !outputKinds.has(kind(node))));
  const fallbackEntry = ordered(nodes.filter(node => !terminalKinds.has(kind(node)) && !outputKinds.has(kind(node))));
  const terminals = ordered(nodes.filter(node => terminalKinds.has(kind(node))));
  const outputs = ordered(nodes.filter(node => outputKinds.has(kind(node))));
  // A release flowing directly to a declared terminal stays inside the floor.
  // Its terminal supplies the exit; drawing both stubs would cross that node.
  const exposedOutputs = outputs.filter(id => !edges.some(edge => edge.from===id && terminals.includes(edge.to)));
  const sinks = ordered(nodes.filter(node => !outgoing.has(node.id)));
  const boundaries = {
    entry: intake.length ? intake : [sources[0] ?? fallbackEntry[0] ?? nodes[0].id],
    exit: terminals.length ? [...new Set([...terminals,...exposedOutputs])].sort() : outputs.length ? outputs : sinks.length ? sinks : [nodes.at(-1).id],
    presentation_only: true,
    entry_basis: intake.length ? "declared_intake" : sources.length ? "visual_graph_source" : "visual_fallback",
    exit_basis: terminals.length ? "declared_terminal" : outputs.length ? "declared_output" : sinks.length ? "visual_graph_sink" : "visual_fallback",
  };
  const declared = new Set(nodes.map(node => node.dept).filter(Boolean));
  const groups = new Map(), byNode = new Map();
  const reviewKinds = new Set(["quality", "route", "repair", "director_wait", "wait", "gate"]);
  for (const node of nodes) {
    let id = node.dept, source = "declared_graph", name;
    if (id) name = `${id.replace(/[_-]/g, " ").replace(/\b\w/g, char => char.toUpperCase())} · graph group`;
    else {
      const category = terminalKinds.has(kind(node)) || outputKinds.has(kind(node)) ? "completion" : reviewKinds.has(kind(node)) ? "review" : "work";
      id = `visual-${category}`;
      while (declared.has(id)) id += "-presentation";
      source = "visual_kind";
      name = `${category[0].toUpperCase()}${category.slice(1)} · visual group`;
    }
    if (!groups.has(id)) groups.set(id, {id, name, glyph:"", presentation_only:true, source});
    byNode.set(node.id, id);
  }
  return {boundaries, departments:[...groups.values()], byNode};
}
// The current Director/human wait is an observed station cue for every source.
function waitCue(state, run) {
  const events=[];
  const graphNodes = state.factory?.graph?.nodes ?? state.publication?.graph_nodes ?? [];
  const currentWait=run.state??{};
  const waitGraphNodes=run.graph?.nodes?.length?run.graph.nodes:graphNodes;
  const waitNode=waitGraphNodes.find(node=>node.id===currentWait.node);
  const runStartedAt=run.started_at??run.state?.started_at;
  const waitStartedAt=currentWait.wait_started_at;
  if (!terminal.has(currentWait.state)&&(currentWait.wait_role==="director"||currentWait.wait_role==="human")&&waitStartedAt&&runStartedAt&&waitNode) {
    const runStartMs=Date.parse(runStartedAt),waitStartMs=Date.parse(waitStartedAt);
    if(Number.isFinite(runStartMs)&&Number.isFinite(waitStartMs)&&waitStartMs>=runStartMs){
      const waitEvent={t:(waitStartMs-runStartMs)/1000,type:"wait",job:run.run_id,step:currentWait.node,human:currentWait.wait_role==="human",responder:currentWait.wait_actor_identity??"unreported"};
      if(Array.isArray(currentWait.permitted_actions))waitEvent.allowed=clone(currentWait.permitted_actions);
      if(currentWait.wait_deadline)waitEvent.wait_deadline=currentWait.wait_deadline;
      events.push(waitEvent);
    }
  }
  return events;
}
function demoFloorEvents(state, run) {
  const graphNodes = state.factory?.graph?.nodes ?? state.publication?.graph_nodes ?? [];
  const byCapability = new Map(graphNodes.filter(n=>n.capability).map(n=>[n.capability,n.id]));
  const events = [];
  const runStart = run.started_at ?? run.state?.started_at;
  const appendAssignment = (d, time, idHint) => {
    const step = d.node && graphNodes.some(n=>n.id===d.node) ? d.node : byCapability.get(d.capability); const started = d.started_at ?? time;
    if (!step || !started) return;
    const active=["running","working","active"].includes(d.state)&&!terminal.has(run.state?.state);
    if(!d.ended_at&&!active)return; // Unknown historical outcomes are kept in Board.
    const t = runStart ? secondsBetween(started, runStart) : secondsBetween(time, state.captured_at);
    if (t == null) return;
    const item = `assignment:${d.assignment_id}:${d.attempt_id ?? idHint ?? "current"}`;
    events.push({ t:Math.max(0,t), type:"spawn", item, art:"task", at:step, label:String(d.attempt_id ?? "").slice(-3), job:run.run_id });
    const ended = d.ended_at;
    events.push({ t:Math.max(0,t), type:"work", step, item, task:d.task_id ?? "", status:d.state,
      dur:ended ? Math.max(0, secondsBetween(ended, started) ?? 0) : undefined, job:run.run_id });
    if (ended) events.push({ t:Math.max(0,secondsBetween(ended,runStart) ?? t), type:"consume", item, at:step, job:run.run_id });
  };
  for (const [assignmentId, attempts] of Object.entries(run.assignments ?? {})) for (const [attemptId,d] of Object.entries(attempts)) appendAssignment(d, d.started_at, `${assignmentId}:${attemptId}`);
  // Canonical attempts already merge stream facts with snapshots. Replaying raw
  // starts here would overwrite their finite duration with an endless work marker.
  // Run phase/node changes drive current station cues, not invented assignments.
  events.push(...waitCue(state, run));
  const unique = new Map();
  for (const e of events) unique.set(`${e.type}|${e.item}|${e.step ?? e.at}|${e.t}`,e);
  return [...unique.values()].sort((a,b)=>a.t-b.t);
}

/*
 * Live/Recorded flow on A2A semantics (operator request, 7 Oct 2026). A2A Tasks
 * are work AT a station: an observed assignment attempt occupies its station (or
 * its presentation-only branch pod) from started_at to ended_at, and an observed
 * run node transition occupies that pinned node until the run moves on or the
 * node's completion fact (artifact/verdict/receipt) is written. Artifacts ride
 * the belts: an item appears on the outgoing belt when its producing station
 * completes and stays on the belt until the next station's observed work starts,
 * so belt dwell is the observed gap. The only presentation adjustment is a
 * minimum visible hop when that gap is shorter than MIN_HOP; exact observed times
 * stay on every item and station. Evidence levels are explicit: report artifacts
 * are evidenced by revision/sha256 (artifact -> verdict -> receipt); assignment
 * outputs are not observed as artifacts, so their items say so and their hand-off
 * follows pinned graph order (inferred). Untimed snapshot rows never get a time.
 */
const MIN_HOP = 0.8, BELT_HOP = 1.4, EPS = 0.002;
const branchPodId = (node, capability) => `pod:${node}:${capability}`;
const ROUTE_TARGET_TYPES = new Set(["repair", "director_wait", "release", "abort", "complete"]);
function branchPods(state, runs, nodes, edges) {
  const kinds = new Map(nodes.map(n => [n.id, graphKind(n.kind ?? n.type)]));
  const bindings = state.factory?.agent_bindings ?? state.publication?.service_bindings ?? [];
  const pods = new Map();
  for (const run of runs) for (const row of allRunRows(run, "assignments")) {
    if (kinds.get(row.node) !== "fanout" || typeof row.capability !== "string" || !row.capability) continue;
    const next = [...new Set(edges.filter(e => e.from === row.node).map(e => e.to))];
    if (next.length !== 1) continue;
    const id = branchPodId(row.node, row.capability);
    const binding = bindings.find(b => b.identity && b.identity === row.provider_identity);
    const prior = pods.get(id);
    if (prior && prior.binding_name) continue;
    pods.set(id, { id, node:row.node, join:next[0], capability:row.capability, name:binding?.name ? `${binding.name} · ${row.capability}` : row.capability,
      binding_name:binding?.name ?? null, identity:binding?.identity ?? null, contract:binding?.contract_digest ?? null, presentation_only:true, basis:"observed_assignments" });
  }
  const perNode = new Map();
  for (const pod of pods.values()) perNode.set(pod.node, (perNode.get(pod.node) ?? 0) + 1);
  return [...pods.values()].filter(pod => perNode.get(pod.node) > 1).sort((a, b) => a.id.localeCompare(b.id));
}
// Presentation projection of one hand-off item. Only these allowlisted, content-free
// fields ever reach the floor: source, part kinds, media type, size, short keyed
// digest, ready time, gem, and evidence level.
export const CARRIER_ITEM_FIELDS = Object.freeze(["index", "source", "part_kinds", "media_type", "byte_length", "digest", "ready_at", "ready_t", "gem", "evidence"]);
const GEMS = { text:"sapphire", data:"emerald", raw:"amethyst", url:"topaz" };
export const gemFor = item => item.source === "message" ? "diamond" : GEMS[item.part_kinds?.[0]] ?? "unknown";
// Runs without hand-off records: one chipped, cloudy "contents not recorded" socket.
const INFERRED_CARRIER = () => ({ evidence:"inferred", sockets:1, items:[], consumers:[], note:"contents not recorded; hand-off inferred from pinned graph order" });
function liveFlow(state, run, nodes, edges, pods) {
  const runStart = run.started_at ?? run.state?.started_at;
  if (!runStart || !nodes.length) return [];
  const T = iso => secondsBetween(iso, runStart);
  const carrierItem = (i, readyAt, evidence) => ({ index:i.item_index, source:i.source, part_kinds:[...i.part_kinds], media_type:i.media_type ?? null,
    byte_length:i.byte_length ?? null, digest:typeof i.digest === "string" ? i.digest.slice(0, 12) : null, ready_at:readyAt, ready_t:Math.max(0, T(readyAt) ?? 0), gem:gemFor(i), evidence });
  const ids = new Set(nodes.map(n => n.id)), type = new Map(nodes.map(n => [n.id, n.type ?? n.kind])), kind = new Map(nodes.map(n => [n.id, graphKind(n.kind ?? n.type)]));
  const byCapability = new Map(nodes.filter(n => n.capability).map(n => [n.capability, n.id]));
  const podFor = new Map(pods.map(p => [`${p.node}\u0000${p.capability}`, p])), podById = new Map(pods.map(p => [p.id, p]));
  // Only material edges carry items; control edges sequence work (decision 5).
  const material = e => (e.kind ?? "material") === "material";
  const succ = id => podById.has(id) ? [podById.get(id).join] : [...new Set(edges.filter(e => e.from === id && material(e)).map(e => e.to))];
  const controlSucc = id => podById.has(id) ? [] : [...new Set(edges.filter(e => e.from === id && e.kind === "control").map(e => e.to))];
  const output = new Map(nodes.map(n => [n.id, n.output ?? "artifacts"]));
  const ended = terminal.has(run.state?.state), endT = ended && run.state?.ended_at ? T(run.state.ended_at) : null;
  const job = run.run_id, out = [], at = t => Math.max(0, t);
  // A2A Tasks observed as assignment attempts.
  const tasks = [];
  for (const [aid, attempts] of Object.entries(run.assignments ?? {})) for (const [att, d] of Object.entries(attempts)) {
    const node = ids.has(d.node) ? d.node : byCapability.get(d.capability);
    if (!node || !d.started_at) continue;
    const active = ["running", "working", "active"].includes(d.state) && !ended;
    if (!d.ended_at && !active) continue; // Unknown historical outcomes stay on the Board.
    const t0 = T(d.started_at), t1 = d.ended_at ? T(d.ended_at) : null;
    if (t0 == null) continue;
    const pod = podFor.get(`${node}\u0000${d.capability}`);
    tasks.push({ key:`${d.assignment_id ?? aid}:${d.attempt_id ?? att}`, node, station:pod?.id ?? node, pod, capability:d.capability ?? "", t0, t1, status:d.state, task:d.task_id ?? "", started_at:d.started_at, ended_at:d.ended_at ?? null });
  }
  tasks.sort((a, b) => a.t0 - b.t0 || a.key.localeCompare(b.key));
  // Assignment facts are authoritative where a node declares a capability or fans out.
  const taskNodes = new Set([...tasks.map(x => x.node), ...nodes.filter(n => n.capability || kind.get(n.id) === "fanout").map(n => n.id)]);
  // Observed run node transitions (streamed facts only; snapshots keep just the current node).
  const facts = (state.events ?? []).filter(row => row.event?.data?.run_id === job);
  const runTypes = new Set([eventType("run.state_changed"), eventType("run.created")]);
  const nodeEvents = facts.filter(row => runTypes.has(row.event.type) && ids.has(row.event.data.node) && !terminal.has(row.event.data.state) && !taskNodes.has(row.event.data.node))
    .map(row => ({ node:row.event.data.node, t:T(row.event.time), iso:row.event.time })).filter(e => e.t != null).sort((a, b) => a.t - b.t);
  const timedFacts = suffix => facts.filter(row => row.event.type === eventType(suffix)).map(row => ({ ...row.event.data, t:T(row.event.time), iso:row.event.time })).filter(e => e.t != null).sort((a, b) => a.t - b.t);
  const revised = timedFacts("artifact.revised"), verdicts = timedFacts("quality.verdict");
  const receipts = (run.delivery ?? []).filter(row => row.delivered_at && row.artifact_sha256).map(row => ({ ...row, t:T(row.delivered_at), iso:row.delivered_at })).filter(e => e.t != null).sort((a, b) => a.t - b.t);
  const completions = [...revised, ...verdicts, ...receipts].map(e => e.t).sort((a, b) => a - b);
  const visits = [];
  for (const e of nodeEvents) {
    const last = visits.at(-1);
    if (last && last.node === e.node && last.t1 == null) continue;
    if (last && last.t1 == null) last.t1 = e.t;
    visits.push({ node:e.node, t0:e.t, t1:null, started_at:e.iso, basis:"run_node_transition" });
  }
  for (const v of visits) {
    const done = completions.find(t => t > v.t0 + EPS && (v.t1 == null || t <= v.t1 + EPS));
    if (done != null) v.t1 = done; else if (v.t1 == null && endT != null) v.t1 = endT;
  }
  const current = run.state?.node;
  if (!ended && ids.has(current) && !taskNodes.has(current) && visits.at(-1)?.node !== current) visits.push({ node:current, t0:null, t1:null, basis:"current_run_node", arrival:"unobserved" });
  const lastKnown = () => Math.max(0, ...tasks.flatMap(x => [x.t0, x.t1 ?? x.t0]), ...visits.flatMap(v => v.t0 == null ? [] : [v.t0, v.t1 ?? v.t0]));
  for (const v of visits) if (v.t0 == null) v.t0 = lastKnown();
  const uniqueType = wanted => { const rows = nodes.filter(n => (n.type ?? n.kind) === wanted); return rows.length === 1 ? rows[0].id : null; };
  const locate = (t, wanted) => {
    const v = visits.find(x => x.t0 <= t + EPS && (x.t1 == null || t <= x.t1 + EPS));
    if (v) return { node:v.node, basis:"run node transition" };
    const id = uniqueType(wanted);
    return id ? { node:id, basis:`only ${wanted} node in the pinned graph` } : null;
  };
  // Pinned-graph path over material edges first. Inferred items may then follow
  // declared control edges (labelled control) and, from a route node, one hop to a
  // route target that is not declared at all (labelled undeclared).
  const search = (from, to, allow) => {
    const seen = new Map([[from, null]]), queue = [from];
    while (queue.length) {
      const id = queue.shift();
      const hops = succ(id).map(n => [n, null]);
      if (allow) {
        for (const n of controlSucc(id)) hops.push([n, "control"]);
        if (type.get(id) === "route") for (const n of nodes) if (ROUTE_TARGET_TYPES.has(n.type ?? n.kind) && n.id !== id) hops.push([n.id, "undeclared"]);
      }
      for (const [n, how] of hops) {
        if (seen.has(n)) continue;
        seen.set(n, { prev:id, how });
        if (n === to) { const path = []; let c = to; while (c !== from) { const s = seen.get(c); path.unshift({ from:s.prev, to:c, ...(s.how ? { [s.how]:true } : {}) }); c = s.prev; } return path; }
        queue.push(n);
      }
    }
    return null;
  };
  const pathTo = (from, to) => from === to ? [] : search(from, to, false) ?? search(from, to, true) ?? [{ from, to, undeclared:true }];
  // Carriers (recorded hand-offs) ride material edges only.
  const materialPath = (from, to) => from === to ? [] : search(from, to, false);
  // Emit belt hops from `start`; dwell on the belt is the observed gap up to `arrive`.
  const travel = (item, path, start, arrive) => {
    if (!path.length) return start;
    const gap = arrive == null ? Infinity : arrive - start;
    const dur = Math.max(MIN_HOP, Math.min(BELT_HOP, gap / path.length));
    path.forEach((hop, k) => out.push({ t:at(start + k * dur), type:"move", item, from:hop.from, to:hop.to, dur, job, ...(hop.undeclared ? { undeclared:true } : {}), ...(hop.control ? { control:true } : {}) }));
    return start + path.length * dur;
  };
  const stationStarts = [...tasks.map(x => ({ node:x.node, station:x.station, t0:x.t0 })), ...visits.map(v => ({ node:v.node, station:v.node, t0:v.t0 }))].sort((a, b) => a.t0 - b.t0);
  // Station occupancy (A2A Tasks and observed run node visits).
  for (const x of tasks) {
    const item = `in:${x.key}`, hop = x.pod ? MIN_HOP : 0;
    out.push({ t:at(x.t0), type:"spawn", item, art:"brief", at:x.node, label:"T", job, evidence:`A2A Task at ${x.pod ? x.pod.name : x.node} (${x.capability || "assignment"}); observed assignment ${x.started_at} → ${x.ended_at ?? "running"}`, observed:`assignment started ${x.started_at}${x.ended_at ? `, ended ${x.ended_at}` : ""}` });
    if (x.pod) out.push({ t:at(x.t0), type:"move", item, from:x.node, to:x.station, dur:MIN_HOP, job });
    if (x.t1 != null) out.push({ t:at(Math.max(x.t1, x.t0 + hop)), type:"consume", item, at:x.station, job });
    else if (endT != null) out.push({ t:at(Math.max(endT, x.t0 + hop)), type:"consume", item, at:x.station, job });
    out.push({ t:at(x.t0), type:"work", step:x.station, item, task:x.task, status:x.status, label:x.pod ? `${x.capability} · observed assignment` : undefined,
      dur:x.t1 != null ? Math.max(0, x.t1 - x.t0) : undefined, job, observed:`${x.started_at} → ${x.ended_at ?? "running"}` });
  }
  // The submitted Task enters at the first observed station and waits there until work starts.
  const first = stationStarts[0];
  if (first) {
    const item = `task:${job}`;
    out.push({ t:0, type:"spawn", item, art:"brief", at:first.node, label:"T", job, evidence:"Submitted Task; run start observed", observed:`run started ${runStart}` });
    out.push({ t:at(Math.max(first.t0, 0.9)), type:"consume", item, at:first.node, job });
  }
  // Recorded hand-offs: the belt item is the factory's carrier (decisions 4-6). A
  // carrier fills in its producing station as items become ready, leaves when the
  // hand-off is produced, rides material belts until each observed consumption, is
  // sealed (not replaced) by a gate verdict, and merges with its siblings at a join.
  // Carriers replace the inferred Task output and report items they cover.
  const handoffs = run.handoffs ?? {};
  const producedRows = (handoffs.produced ?? []).filter(p => ids.has(p.node) && output.get(p.node) !== "none" && T(p.produced_at) != null)
    .sort((a, b) => Date.parse(a.produced_at) - Date.parse(b.produced_at) || a.handoff_id.localeCompare(b.handoff_id));
  const readyRows = (handoffs.ready ?? []).filter(r => ids.has(r.node) && output.get(r.node) !== "none" && T(r.ready_at) != null);
  const consumedRows = (handoffs.consumed ?? []).filter(c => ids.has(c.node) && T(c.consumed_at) != null)
    .sort((a, b) => Date.parse(a.consumed_at) - Date.parse(b.consumed_at));
  const attemptOf = d => `${d.assignment_id}:${d.attempt_id}`;
  const taskByKey = new Map(tasks.map(x => [x.key, x]));
  const recordedAttempts = new Set([...producedRows, ...readyRows].map(attemptOf));
  const recordedShas = new Set(producedRows.flatMap(p => p.items.map(i => i.artifact_sha256).filter(Boolean)));
  const carriers = [];
  for (const p of producedRows) {
    const items = [...p.items].sort((a, b) => a.item_index - b.item_index).map(i => {
      const streamed = readyRows.find(r => r.handoff_id === p.handoff_id && attemptOf(r) === attemptOf(p) && r.item_index === i.item_index);
      return carrierItem(i, streamed && Date.parse(streamed.ready_at) < Date.parse(i.ready_at) ? streamed.ready_at : i.ready_at, "recorded");
    });
    const task = taskByKey.get(attemptOf(p)), station = task?.station ?? p.node, producedT = T(p.produced_at);
    const report = p.items.find(i => i.artifact_sha256);
    carriers.push({ id:`carrier:${p.handoff_id}:${p.handoff_revision}`, row:p, node:p.node, station, task, producedT, items,
      digests:new Set(p.items.map(i => i.digest)), shas:new Set(p.items.map(i => i.artifact_sha256).filter(Boolean)),
      label:report ? String(report.artifact_revision).toUpperCase() : `R${p.handoff_revision}`, consumers:[] });
  }
  // Consumers match by handoff_id and item digests; an input naming a hand-off whose
  // digests match no revision is attached to the latest revision then produced and
  // shown as a digest mismatch.
  const mergeRows = new Map();
  for (const c of consumedRows) {
    const t = T(c.consumed_at), matched = [];
    for (const input of c.inputs) {
      const same = carriers.filter(x => x.row.handoff_id === input.handoff_id && x.producedT <= t + EPS);
      const hit = same.filter(x => input.item_digests.some(d => x.digests.has(d))).at(-1);
      const carrier = hit ?? same.at(-1);
      if (!carrier) continue;
      const match = !!hit && input.item_digests.every(d => carrier.digests.has(d));
      carrier.consumers.push({ row:c, t, node:c.node, match });
      matched.push(carrier);
    }
    if (matched.length > 1) mergeRows.set(c, { row:c, t, inputs:matched, arrivals:[] });
  }
  const isGate = id => type.get(id) === "quality" || (kind.get(id) === "gate" && !["route", "repair"].includes(type.get(id)));
  const spawnCarrier = (c, id, t, station, items, extra = {}) => out.push({ t:at(t), type:"spawn", item:id, art:`carrier:${c.node}`, at:station, label:c.label, job,
    evidence:`Recorded hand-off ${c.row?.handoff_id ?? "merge"} · revision ${c.row?.handoff_revision ?? 1} from ${c.node} · ${items.length} item${items.length === 1 ? "" : "s"} · contents not shown`,
    observed:c.row ? `hand-off produced ${c.row.produced_at}` : extra.observed ?? "",
    carrier:{ evidence:"recorded", handoff_id:c.row?.handoff_id ?? null, revision:c.row?.handoff_revision ?? null, node:c.node, output:output.get(c.node),
      produced_at:c.row?.produced_at ?? null, produced_t:c.producedT ?? t, sockets:items.length, items:clone(items),
      consumers:c.consumers.map(k => ({ node:k.node, consumed_at:k.row.consumed_at, digest_match:k.match })),
      digest_match:c.consumers.every(k => k.match), ...extra.carrier } });
  const retireAtEnd = (id, here, ready) => {
    if (!ended || endT == null) return;
    const outcome = run.state?.state;
    out.push({ t:at(Math.max(endT, ready + MIN_HOP)), type:outcome === "aborted" || outcome === "failed" ? "scrap" : "consume", item:id, at:here, job });
  };
  const visitAt = (node, t) => visits.find(v => v.node === node && v.t0 <= t + EPS && (v.t1 == null || t <= v.t1 + EPS));
  for (const c of carriers) {
    const fillT = Math.min(c.producedT, ...c.items.map(i => i.ready_t));
    spawnCarrier(c, c.id, c.task ? Math.max(c.task.t0, fillT) : fillT, c.station, c.items);
    let here = c.station, ready = c.producedT, alive = true, branches = 0;
    for (const k of [...c.consumers].sort((a, b) => a.t - b.t)) {
      const merge = mergeRows.get(k.row);
      let item = c.id, from = here, start = ready, path = alive ? materialPath(here, k.node) : null;
      // A retired carrier reaches a later consumer only on its own belt from the producer (a material bypass).
      if (!path) {
        const bypass = materialPath(c.station, k.node);
        if (alive) { out.push({ t:at(Math.max(k.t, ready)), type:"consume", item, at:here, job }); alive = false; }
        if (!bypass || !bypass.length) continue;
        item = `${c.id}~${++branches}`; from = c.station; start = c.producedT; path = bypass;
        spawnCarrier(c, item, c.producedT, c.station, c.items, { carrier:{ leg:k.node } });
      }
      if (merge) {
        const j = path.map(h => h.to).findLastIndex(n => n !== k.node && kind.get(n) === "join");
        // The leg to the join takes its share of the observed gap; the merged carrier rides the rest.
        if (j >= 0) { merge.arrivals.push({ item, carrier:c, join:path[j].to, t:travel(item, path.slice(0, j + 1), start, start + (k.t - start) * (j + 1) / path.length) }); if (item === c.id) alive = false; continue; }
      }
      const arrived = travel(item, path, start, k.t);
      if (isGate(k.node) && item === c.id) {
        here = k.node; ready = Math.max(k.t, arrived);
        const visit = visitAt(k.node, k.t); if (visit && !visit.item) visit.item = item;
        // A retained snapshot keeps the verdict row but not its time: seal on arrival at the gate.
        const vv = verdicts.find(v => c.shas.has(v.artifact_sha256) && v.t >= k.t - EPS)
          ?? (run.quality ?? []).filter(v => c.shas.has(v.artifact_sha256) && typeof v.accepted === "boolean" && !verdicts.some(x => x.artifact_sha256 === v.artifact_sha256)).map(v => ({ ...v, t:arrived, untimed:true })).at(-1);
        if (vv) {
          out.push({ t:at(Math.max(vv.t, arrived)), type:"verdict", step:k.node, item, verdict:vv.accepted === true ? "accepted" : "rejected", rev:vv.artifact_revision, sha:vv.artifact_sha256.slice(0, 12), finding:`${vv.finding_count ?? 0} finding${vv.finding_count === 1 ? "" : "s"}`, job, seal:true, ...(vv.untimed ? { observed:"verdict time not recorded" } : {}) });
          if (vv.accepted === false) out.push({ t:at(Math.max(vv.t, arrived)), type:"flag", item, flag:"rejected", job });
          ready = Math.max(ready, vv.t);
        }
        continue;
      }
      // A side-effect release with a verified receipt for this carrier's report exits the factory there.
      const receipt = receipts.find(r => c.shas.has(r.artifact_sha256) && !r.receipt_conflict && r.t >= k.t - EPS);
      if (receipt && (output.get(k.node) === "none" || type.get(k.node) === "release")) out.push({ t:at(Math.max(receipt.t, k.t, arrived)), type:"release", item, at:k.node, job });
      else out.push({ t:at(Math.max(k.t, arrived)), type:"consume", item, at:k.node, job });
      if (item === c.id) { alive = false; here = k.node; }
    }
    if (!alive) continue;
    if (c.consumers.length) { retireAtEnd(c.id, here, ready); continue; }
    // Not consumed yet: ride the material belt to the next station and wait there.
    const nexts = succ(c.station);
    let dest = nexts.length === 1 ? nexts[0] : null;
    const path = dest ? [{ from:c.station, to:dest }] : [];
    while (dest && kind.get(dest) === "join" && !stationStarts.some(s => s.node === dest)) { const next = succ(dest); if (next.length !== 1) break; path.push({ from:dest, to:next[0] }); dest = next[0]; }
    const arrived = travel(c.id, path, c.producedT, endT);
    retireAtEnd(c.id, dest ?? c.station, arrived);
  }
  // Fan-in: input carriers meet at the join and leave it as one carrier holding every item.
  for (const merge of mergeRows.values()) {
    const byJoin = new Map();
    for (const a of merge.arrivals) (byJoin.get(a.join) ?? byJoin.set(a.join, []).get(a.join)).push(a);
    for (const [joinId, rows] of byJoin) {
      const mergeT = Math.max(...rows.map(a => a.t));
      const id = `merge:${attemptOf(merge.row)}:${merge.row.node}:${joinId}`;
      if (rows.length > 1) for (const a of rows) out.push({ t:at(mergeT), type:"consume", item:a.item, at:joinId, job });
      const carrier = rows.length > 1 ? { node:joinId, row:null, producedT:mergeT, label:"J", consumers:[{ row:merge.row, t:merge.t, node:merge.row.node, match:rows.every(a => a.carrier.consumers.find(k => k.row === merge.row)?.match) }] } : null;
      if (carrier) spawnCarrier(carrier, id, mergeT, joinId, rows.flatMap(a => a.carrier.items), { observed:`merged at ${joinId}`, carrier:{ merged_from:rows.map(a => a.carrier.row.handoff_id) } });
      const item = carrier ? id : rows[0].item, path = materialPath(joinId, merge.row.node) ?? [];
      const arrived = travel(item, path, mergeT, merge.t);
      out.push({ t:at(Math.max(merge.t, arrived)), type:"consume", item, at:merge.row.node, job });
    }
  }
  // A live carrier still filling: empty sockets wait for the items still streaming.
  const filling = new Map();
  for (const r of readyRows) {
    if (producedRows.some(p => p.handoff_id === r.handoff_id && attemptOf(p) === attemptOf(r) && Date.parse(p.produced_at) >= Date.parse(r.ready_at) - 1)) continue;
    const key = `${attemptOf(r)}\u0000${r.handoff_id}`;
    (filling.get(key) ?? filling.set(key, []).get(key)).push(r);
  }
  for (const rows of filling.values()) {
    const r0 = rows[0], task = taskByKey.get(attemptOf(r0)), station = task?.station ?? r0.node;
    const items = rows.sort((a, b) => a.item_index - b.item_index).map(r => carrierItem({ ...r, source:"artifact", byte_length:null, digest:null }, r.ready_at, "recorded"));
    const revision = Math.max(0, ...producedRows.filter(p => p.handoff_id === r0.handoff_id).map(p => p.handoff_revision)) + 1;
    const id = `carrier:${r0.handoff_id}:${revision}:filling`;
    const fillT = Math.min(...items.map(i => i.ready_t));
    out.push({ t:at(task ? Math.max(task.t0, fillT) : fillT), type:"spawn", item:id, art:`carrier:${r0.node}`, at:station, label:`R${revision}`, job,
      evidence:`Hand-off ${r0.handoff_id} filling at ${r0.node}: ${items.length} item${items.length === 1 ? "" : "s"} ready, not yet produced · contents not shown`, observed:`first item ready ${items[0].ready_at}`,
      carrier:{ evidence:"recorded", handoff_id:r0.handoff_id, revision, node:r0.node, output:output.get(r0.node), produced_at:null, produced_t:null, filling:true, sockets:items.length + 1, items, consumers:[], digest_match:true } });
    if (ended && endT != null) out.push({ t:at(Math.max(endT, fillT)), type:"consume", item:id, at:station, job });
  }
  // Assignment outputs without hand-off records: inferred carriers whose contents are
  // not recorded; their hand-off follows pinned graph order. Side-effect nodes produce none.
  for (const x of tasks) {
    if (x.t1 == null || recordedAttempts.has(x.key) || output.get(x.node) === "none") continue;
    const item = `out:${x.key}`;
    const nexts = succ(x.station);
    let dest = nexts.length === 1 ? nexts[0] : null;
    const path = [];
    if (dest) path.push({ from:x.station, to:dest, undeclared:false });
    while (dest && kind.get(dest) === "join" && !stationStarts.some(s => s.node === dest)) { const next = succ(dest); if (next.length !== 1) break; path.push({ from:dest, to:next[0], undeclared:false }); dest = next[0]; }
    out.push({ t:at(x.t1), type:"spawn", item, art:"task-output", at:x.station, label:"TO", job, evidence:`Task output · artifact not recorded. Observation records the ${x.capability || "assignment"} Task (ended ${x.ended_at}) but not its output; hand-off to ${dest ?? "the next station"} is inferred from pinned graph order.`, observed:`assignment ended ${x.ended_at}`, carrier:INFERRED_CARRIER() });
    if (!dest) { if (endT != null) out.push({ t:at(Math.max(endT, x.t1)), type:"consume", item, at:x.station, job }); continue; }
    const next = stationStarts.find(s => s.node === dest && s.t0 >= x.t1 - EPS);
    const consumeAt = next?.t0 ?? endT;
    const arrived = travel(item, path, x.t1, consumeAt);
    if (consumeAt != null) out.push({ t:at(Math.max(consumeAt, arrived)), type:"consume", item, at:dest, job });
  }
  // Report artifacts, evidenced by revision and digest.
  const revs = new Map();
  for (const e of revised) if (!revs.has(e.artifact_sha256)) revs.set(e.artifact_sha256, { rev:e.artifact_revision, sha:e.artifact_sha256, t:e.t, iso:e.iso, timed:"revised" });
  for (const r of receipts) if (!revs.has(r.artifact_sha256)) revs.set(r.artifact_sha256, { rev:r.artifact_revision, sha:r.artifact_sha256, t:r.t, iso:r.iso, timed:"receipt" });
  const ordered = [...revs.values()].sort((a, b) => a.t - b.t);
  ordered.forEach((a, index) => {
    if (recordedShas.has(a.sha)) return; // carried by a recorded hand-off
    const next = ordered[index + 1];
    const verdict = verdicts.find(v => v.artifact_sha256 === a.sha) ?? (run.quality ?? []).find(v => v.artifact_sha256 === a.sha);
    const myReceipts = receipts.filter(r => r.artifact_sha256 === a.sha), verified = myReceipts.some(r => !r.receipt_conflict);
    const item = `artifact:${a.rev}:${a.sha.slice(0, 12)}`, label = String(a.rev ?? "R").toUpperCase();
    const start = a.timed === "revised" ? locate(a.t, "synthesize") : locate(a.t, "release");
    if (!start) return;
    const evidence = [`Report artifact ${a.rev} · sha256 ${a.sha}`,
      verdict ? `Quality ${verdict.accepted === true ? "accepted" : verdict.accepted === false ? "rejected" : "verdict"} (same sha256${verdict.t == null ? "; verdict time not recorded" : ""})` : "No quality verdict observed",
      myReceipts.length ? (verified ? "delivery receipt (same sha256)" : "delivery unverified (conflicting receipts)") : "No delivery receipt observed",
      `located at ${start.node} by ${start.basis}`].join(" · ");
    out.push({ t:at(a.t), type:"spawn", item, art:"report", at:start.node, label, rev:a.rev, sha:a.sha, job, evidence, observed:`${a.timed === "revised" ? "artifact revised" : "first delivered"} ${a.iso}`, carrier:INFERRED_CARRIER() });
    let here = start.node, ready = a.t;
    const stopUntil = next ? next.t : Infinity;
    const stops = visits.filter(v => v.t0 >= a.t - EPS && v.t0 < stopUntil + EPS && !(v.node === start.node && v.t0 <= a.t + EPS));
    if (a.timed === "revised") for (const r of myReceipts) { const loc = locate(r.t, "release"); if (loc && !stops.some(v => v.node === loc.node)) stops.push({ node:loc.node, t0:r.t, t1:r.t, basis:"receipt" }); }
    stops.sort((x, y) => x.t0 - y.t0);
    let consumed = false;
    for (const v of stops) {
      const arrived = travel(item, pathTo(here, v.node), ready, v.t0);
      here = v.node; ready = Math.max(v.t1 ?? v.t0, arrived); if (!v.item && v.basis !== "receipt") v.item = item;
      const isNextProducer = next && next.timed === "revised" && v.t1 != null && Math.abs(v.t1 - next.t) < EPS;
      if (isNextProducer) { out.push({ t:at(Math.max(v.t0, arrived)), type:"consume", item, at:v.node, job }); consumed = true; break; }
      const vv = verdicts.find(x => x.artifact_sha256 === a.sha && x.t >= v.t0 - EPS && (v.t1 == null || x.t <= v.t1 + EPS));
      if (vv) {
        out.push({ t:at(vv.t), type:"verdict", step:v.node, item, verdict:vv.accepted === true ? "accepted" : "rejected", rev:a.rev, sha:a.sha.slice(0, 12), finding:`${vv.finding_count ?? 0} finding${vv.finding_count === 1 ? "" : "s"}`, job });
        if (vv.accepted === false) out.push({ t:at(vv.t), type:"flag", item, flag:"rejected", job });
      }
      if (v.t1 == null) break;
    }
    if (consumed) return;
    if (ended && endT != null) {
      const outcome = run.state?.state;
      const finish = ["completed", "accepted", "released"].includes(outcome) ? uniqueType("complete") : (["aborted"].includes(outcome) ? uniqueType("abort") : null);
      if (verified && finish && pathTo(here, finish).every(hop => !hop.undeclared)) {
        const arrived = travel(item, pathTo(here, finish), Math.max(ready, myReceipts[0]?.t ?? ready), endT);
        out.push({ t:at(Math.max(endT, arrived)), type:"release", item, at:finish, job });
      } else if (outcome === "aborted" || outcome === "failed") out.push({ t:at(Math.max(endT, ready + MIN_HOP)), type:"scrap", item, at:here, job });
      else out.push({ t:at(Math.max(endT, ready + MIN_HOP)), type:"consume", item, at:here, job });
    }
  });
  for (const v of visits) out.push({ t:at(v.t0), type:"work", step:v.node, ...(v.item ? { item:v.item } : {}), status:v.t1 == null ? "working" : "completed", label:v.arrival ? "Current run node · arrival time not recorded" : "Observed run node transition", dur:v.t1 != null ? Math.max(0, v.t1 - v.t0) : undefined, job, observed:v.started_at ? `run node ${v.node} from ${v.started_at}` : `current run node ${v.node}` });
  return out;
}
function floorEvents(state, run, nodes, edges, pods) {
  const events = state.source === "demo" ? demoFloorEvents(state, run) : [...liveFlow(state, run, nodes, edges, pods), ...waitCue(state, run)];
  if (state.source === "demo") return events;
  return events.map((e, i) => [e, i]).sort((a, b) => a[0].t - b[0].t || a[1] - b[1]).map(([e]) => e);
}

// A renderer grouping is presentation only; authoritative run/Task identities stay
// in state.runs and on every grouped event.
function deliveryOutcome(run) {
  const receipts = (run.delivery ?? []).filter(row => !row.receipt_conflict);
  if (receipts.some(row => row.delivery_kind === "local_file" && row.outcome === "local-file-delivered")) return "local-file-delivered";
  if (receipts.some(row => row.outcome === "fixture-received")) return "fixture-received";
  const delivered = receipts.some(row =>
    ["delivered", "released", "completed", "succeeded"].includes(row.state ?? row.outcome));
  if (!delivered && (run.delivery ?? []).some(row => row.receipt_conflict)) return "delivery-unverified";
  return delivered || run.state?.state === "released" ? "released" : (run.state?.state ?? "unknown");
}
function checkGraphPins(state) {
  for (const key of ["manifest_digest", "definition_digest"]) {
    const pins = new Set([...state.runs.values()].map(run => run.pinned?.[key]).filter(Boolean));
    const active = state.publication?.[key];
    if (pins.size > 1 || (active && [...pins].some(pin => pin !== active))) {
      throw new DashboardContractError("Run graph pins differ; select a run with its pinned graph before displaying this floor", `$state.runs.${key}`);
    }
  }
}
// The all-jobs floor runs on one clock; carrier item times move with their job.
const shiftCarrier = (carrier, offset) => ({ ...carrier, ...(carrier.produced_t != null ? { produced_t:carrier.produced_t + offset } : {}), items:(carrier.items ?? []).map(item => ({ ...item, ready_t:item.ready_t + offset })) });
function aggregateFloorRuns(state, runs) {
  if (runs.length < 2) return runs;
  const known = runs.filter(run => run.startKnown);
  if (!known.length) return runs;
  const baseMs = Math.min(...known.map(run => Date.parse(run.start)));
  const events = [];
  for (const run of runs) {
    if (!run.startKnown) continue;
    const offset = (Date.parse(run.start) - baseMs) / 1000;
    events.push({type:"job",t:offset,job:run.id,version:run.pinned.publication_version ?? (run.pinned.manifest_digest === state.publication?.manifest_digest ? state.publication?.publication_version : null)});
    const source = state.runs.get(run.id);
    const starts = Object.values(source.assignments ?? {}).flatMap(Object.values)
      .map(row => Date.parse(row.started_at)).filter(Number.isFinite);
    for (const {event} of state.events) {
      const d=event.data;
      if(d.run_id === run.id && event.type === eventType("admission.state_changed") && ["admitted","running","active"].includes(d.state)) starts.push(Date.parse(d.started_at ?? event.time));
    }
    for (const row of source.admissions ?? []) {
      if (["admitted","running","active"].includes(row.state) && row.started_at) starts.push(Date.parse(row.started_at));
    }
    const admissions = starts.filter(Number.isFinite);
    if (admissions.length) events.push({type:"admit",t:Math.max(offset,(Math.min(...admissions)-baseMs)/1000),job:run.id});
    for (const event of run.timeline) events.push({...event,t:event.t+offset,type:event.type === "end" ? "jobend" : event.type,job:run.id,...(event.item?{item:`${run.id}:${event.item}`}:{}),...(event.carrier?{carrier:shiftCarrier(event.carrier,offset)}:{})});
  }
  events.sort((a,b)=>a.t-b.t);
  const now=Math.max(0,(Date.parse(state.captured_at)-baseMs)/1000);
  const aggregate={id:"dashboard-all-runs",name:"All observed jobs",multi:true,brief:"",start:new Date(baseMs).toISOString(),startKnown:true,startAt:0,snapshotAt:now,now,live:state.source === "live" && runs.some(run=>run.live),events,timeline:events,delivery:runs.flatMap(run=>clone(run.delivery??[])),outcomeLabel:"Observed jobs",pinned:{}};
  return [aggregate,...runs];
}

/*
 * Isolated Demo presentation. The Demo Adapter's explicitly labelled illustrative
 * layer (validated only for source "demo") restores the original scenario
 * timeline, scenario clock and simulated operating metadata. It is never read for
 * Live or Recorded state, so no illustrative value can become a Live fact.
 */
const demoLayer = state => state?.source === "demo" && state.demo?.factory ? state.demo : null;
const DEMO_TIME_KEYS = ["t", "t1", "expires"];
function demoShift(row, offset) {
  const { event_id, ...entry } = clone(row);
  for (const key of DEMO_TIME_KEYS) if (typeof entry[key] === "number") entry[key] -= offset;
  if (entry.rec && typeof entry.rec.t === "number") entry.rec.t -= offset;
  return entry;
}
// A Demo command (for example an applied abort) can end a run after its script;
// close that run's open illustrative intervals at the observed end.
function demoTerminal(run, rows, pres, offset, multi) {
  const ended = run.state?.ended_at;
  if (!terminal.has(run.state?.state) || !ended) return rows;
  const endT = (Date.parse(ended) - Date.parse(pres.start)) / 1000 - offset;
  if (!Number.isFinite(endT) || rows.some(row => (row.type === "end" || row.type === "jobend") && row.t >= endT - 0.002)) return rows;
  for (const row of rows) if (["wait", "alarm", "flag"].includes(row.type) && (row.t1 == null || row.t1 > endT)) row.t1 = endT;
  rows.push({ type:multi ? "jobend" : "end", t:endT, outcome:run.state.state, job:run.run_id, illustrative_basis:"observed_demo_command" });
  return rows;
}
// A scenario that authors its own item lifecycle replays it; a minimal scenario
// (work entries but no spawned items) keeps the fact-derived assignment items and
// current wait, and adds only its non-item illustrative activity.
const DEMO_ITEM_KINDS = new Set(["spawn", "move", "consume", "release", "scrap", "flag"]);
const DEMO_FACT_KINDS = new Set(["work", "wait", "end", "jobend"]);
const demoItemized = rows => rows.some(row => row.type === "spawn");
const demoOrder = row => typeof row.order === "number" ? row.order : Infinity;
const demoSort = rows => rows.map((row, i) => [row, i]).sort((a, b) => a[0].t - b[0].t || demoOrder(a[0]) - demoOrder(b[0]) || a[1] - b[1]).map(([row]) => row);
function demoRuns(state, demo, baseRun) {
  const byRun = new Map(), presentations = demo.runs ?? [];
  for (const pres of presentations) for (const id of pres.run_ids ?? []) byRun.set(id, pres);
  const rows = [], aggregates = new Map();
  for (const run of state.runs.values()) {
    const pres = byRun.get(run.run_id);
    if (!pres || !run.illustrations?.length) { rows.push(baseRun(run)); continue; }
    const startMs = Date.parse(pres.start), runStart = run.started_at ?? run.state?.started_at;
    // A job's own scripted entry is its exact offset on the scenario clock (ISO times round to ms).
    const jobEntry = run.illustrations.find(row => row.type === "job");
    const offset = !pres.multi ? 0 : jobEntry ? jobEntry.t : runStart ? (Date.parse(runStart) - startMs) / 1000 : 0;
    const ended = terminal.has(run.state?.state);
    const base = baseRun(run);
    const itemized = demoItemized(run.illustrations);
    const shifted = run.illustrations.map(row => demoShift(row, offset));
    const timeline = itemized ? demoSort(demoTerminal(run, shifted, pres, offset, false))
      : demoSort([...base.timeline.map(row => ({ ...row, t:row.t + ((Date.parse(base.start) - startMs) / 1000 - offset) })), ...shifted.filter(row => !DEMO_ITEM_KINDS.has(row.type) && !DEMO_FACT_KINDS.has(row.type))]);
    if (!pres.multi) for (const row of timeline) if (row.type === "jobend") row.type = "end";
    const live = pres.live === true && !ended;
    const now = typeof pres.now === "number" ? pres.now - offset : base.now;
    rows.push({ ...base, name:pres.multi ? run.run_id : (pres.name ?? run.run_id), brief:pres.multi ? (jobEntry?.brief ?? "") : (pres.brief ?? ""),
      outcomeLabel:ended || !pres.outcomeLabel || pres.multi ? base.outcomeLabel : pres.outcomeLabel, tone:ended || pres.multi ? "normal" : (pres.tone ?? "normal"),
      start:new Date(startMs + offset * 1000).toISOString(), startKnown:true, startAt:pres.multi ? 0 : (pres.startAt ?? 0),
      snapshotAt:live ? now : base.snapshotAt, now:live ? now : base.now, live, timeline, events:timeline, illustrative:true });
    if (pres.multi) {
      if (!aggregates.has(pres.id)) aggregates.set(pres.id, { pres, events:[] });
      aggregates.get(pres.id).events.push(...(itemized ? demoTerminal(run, run.illustrations.map(row => demoShift(row, 0)), pres, 0, true)
        : [...base.timeline.map(row => ({ ...row, t:row.t + (Date.parse(base.start) - startMs) / 1000, type:row.type === "end" ? "jobend" : row.type, ...(row.item ? { item:`${run.run_id}:${row.item}` } : {}) })), ...run.illustrations.map(row => demoShift(row, 0)).filter(row => !DEMO_ITEM_KINDS.has(row.type) && !DEMO_FACT_KINDS.has(row.type))]));
    }
  }
  const all = [];
  for (const { pres, events } of aggregates.values()) {
    if (state.runs.size < 2) continue; // a selected job is shown on its own
    const timeline = demoSort([...events, ...(state.illustrations ?? []).map(row => demoShift(row, 0))]);
    all.push({ id:"dashboard-all-runs", name:pres.name ?? "All jobs", multi:true, brief:pres.brief ?? "", start:pres.start, startKnown:true,
      startAt:pres.startAt ?? 0, snapshotAt:pres.now ?? 0, now:pres.now ?? 0, live:pres.live === true, events:timeline, timeline,
      delivery:[], outcomeLabel:pres.outcomeLabel ?? "Illustrative jobs", tone:pres.tone ?? "normal", pinned:{}, illustrative:true });
  }
  return [...all.filter(row => row.multi), ...rows];
}
function demoFactory(model, demo) {
  const meta = demo.factory;
  for (const step of model.steps) Object.assign(step, clone(meta.steps?.[step.id] ?? {}));
  const agents = { ...model.agents };
  for (const [key, agent] of Object.entries(meta.agents ?? {})) agents[key] = { ...clone(agent), illustrative:true };
  return { ...model, digest:meta.digest ?? model.digest, versions:clone(meta.versions ?? {}), programs:clone(meta.programs ?? []),
    mainArt:meta.mainArt ?? model.mainArt, admission:meta.admission ? clone(meta.admission) : model.admission,
    ...(meta.sla != null ? { sla:meta.sla } : {}), ...(meta.queueAdvisory != null ? { queueAdvisory:meta.queueAdvisory } : {}),
    budget:meta.budget ? clone(meta.budget) : null, agents, artifacts:clone(meta.artifacts ?? {}), actual:false,
    illustrative:true, illustrativeLabel:demo.label };
}

const LIVE_ARTIFACTS = {brief:{name:"Task",shape:"circle"},"task-output":{name:"Task output · artifact not recorded",shape:"capsule"},report:{name:"Report artifact",shape:"square"}};
// A carrier's shape is its producing node's declared kind on the pinned graph, so it
// is constant across jobs (and matches the inferred item shape for the same node kind).
const CARRIER_SHAPES = {synthesize:"square",parallel:"capsule",fanout:"capsule",join:"hexagon",nested_factory:"pentagon",nested:"pentagon"};
const carrierArtifacts = nodes => Object.fromEntries(nodes.map(n => [`carrier:${n.id}`, {name:`Hand-off from ${n.name ?? n.id}`, shape:CARRIER_SHAPES[n.type ?? n.kind] ?? "capsule", carrier:true}]));
const FLOW_BASIS = "A2A flow: Tasks occupy stations from observed start to end; artifacts ride belts as carriers on material edges from the producing station's completion until the next station's observed start (dwell is the observed gap; hops shorter than 0.8 s are shown at 0.8 s). Recorded hand-offs show their items as gems (part kind, media type, size, short keyed digest, ready time; never content), and a gate verdict seals the same carrier. Runs without hand-off records keep inferred carriers: report artifacts are evidenced by revision and sha256, Task outputs are not recorded as artifacts, so their contents are not recorded and their hand-off is inferred from pinned graph order. Control edges carry no items. Branch pods are presentation derived from observed assignments.";
export function toFloorModel(state, {runId=null, graphOf=null} = {}) {
  if (!state) throw new DashboardContractError("dashboard state is unavailable", "$state");
  let hasPinnedGraph=false;
  // "All jobs" on one pinned graph: every run whose manifest and definition pins
  // equal the anchor run's pins shares that graph; other runs stay off this floor.
  if(!runId&&graphOf){
    const anchor=state.runs.get(graphOf);
    if(!anchor)throw new DashboardContractError("selected run is unavailable","$state.runs");
    if(anchor.graph?.nodes?.length&&anchor.pinned?.manifest_digest&&anchor.pinned?.definition_digest){
      const same=run=>run.run_id===graphOf||(run.graph?.nodes?.length&&["manifest_digest","definition_digest"].every(key=>run.pinned?.[key]===anchor.pinned[key]));
      hasPinnedGraph=true;
      state={...state,runs:new Map([...state.runs].filter(([,run])=>same(run))),factory:{...state.factory,graph:anchor.graph},publication:{...anchor.pinned,publication_version:anchor.pinned.publication_version??"Pinned run"}};
    } else runId=graphOf;
  }
  if(runId){
    const run=state.runs.get(runId);
    if(!run)throw new DashboardContractError("selected run is unavailable","$state.runs");
    hasPinnedGraph=!!run.graph?.nodes?.length;
    state={...state,runs:new Map([[runId,run]]),...(hasPinnedGraph?{factory:{...state.factory,graph:run.graph},publication:{...run.pinned,publication_version:run.pinned.publication_version??"Pinned run"}}:{})};
  }
  const wireNodes = state.factory?.graph?.nodes ?? state.publication?.graph_nodes ?? [];
  if (!wireNodes.length) return null;
  if(!hasPinnedGraph)checkGraphPins(state);
  const bindings = state.factory?.agent_bindings ?? state.publication?.service_bindings ?? state.factory?.agents ?? [];
  const agents = Object.fromEntries(bindings.map(b=>[b.capability ?? b.role,{name:b.name,contract:b.contract_digest,cap:b.capacity_limit ?? null,identity:b.identity}]));
  const edges = state.factory?.graph?.edges?.length ? clone(state.factory.graph.edges) : wireNodes.flatMap(n=>(n.next ?? []).map(to=>({from:n.id,to}))).map(edge=>({...edge,loop:false}));
  const presentation = floorPresentation(wireNodes, edges);
  const demoSource = state.source === "demo";
  const pods = demoSource ? [] : branchPods(state, [...state.runs.values()], wireNodes, edges);
  const steps = wireNodes.map(n=>({id:n.id,name:n.name ?? n.id,kind:graphKind(n.kind ?? n.type),type:n.type ?? n.kind,agent:n.agent ?? n.capability,capability:n.capability ?? n.agent,sub:n.sub ?? n.capability ?? n.agent ?? n.type ?? n.kind,dept:presentation.byNode.get(n.id),...(n.output?{output:n.output}:{})}));
  const nowMs=Date.parse(state.captured_at);
  const demo=demoLayer(state);
  const baseRun=run=>{
    const started=run.started_at ?? run.state?.started_at ?? null;
    const ended=run.state?.ended_at ?? null;
    const elapsed=started ? Math.max(0,(nowMs-Date.parse(started))/1000) : 0;
    const stateValue=run.state?.state ?? "unknown";
    const endedState=terminal.has(stateValue);
    const timeline=floorEvents(state,run,wireNodes,edges,pods);
    if (ended && started) timeline.push({t:secondsBetween(ended,started) ?? elapsed,type:"end",outcome:deliveryOutcome(run),job:run.run_id});
    return {id:run.run_id,name:run.run_id,brief:"",outcomeLabel:stateValue,tone:"normal",start:started ?? state.captured_at,startKnown:!!started,startAt:0,snapshotAt:elapsed,now:elapsed,live:state.source==="live"&&!endedState,multi:false,pinned:clone(run.pinned),model_label:run.model_label,fixture_label:run.fixture_label,timeline,events:timeline,task:clone(run.task),status:clone(run.state ?? {}),artifacts:clone(run.artifacts),assignments:allRunRows(run,"assignments"),delivery:clone(run.delivery),decisions:clone(run.decisions)};
  };
  const runs=demo?null:[...state.runs.values()].map(baseRun);
  const floorRuns=demo?demoRuns(state,demo,baseRun):aggregateFloorRuns(state,runs);
  const factory=state.factory ?? {};
  const publication=state.publication ?? {};
  const capacity=state.capacity;
  const model={id:state.factoryId,name:factory.name ?? state.factoryId,capability:factory.capability ?? "",version:publication.publication_version ?? publication.version ?? publication.label ?? "Unknown",digest:publication.manifest_digest ?? publication.digest ?? "Unknown",versions:{},provenance:factory.fixture_label?`Demo fixture · ${factory.fixture_label}`:state.source==="demo"?"Isolated deterministic demo":"Runtime observation",departments:presentation.departments,boundaries:presentation.boundaries,programs:[],mainArt:"report",admission:capacity?{limit:capacity.capacity_limit ?? capacity.limit,when:"observed"}:undefined,budget:null,agents,artifacts:demoSource?{}:{...clone(LIVE_ARTIFACTS),...carrierArtifacts(wireNodes)},steps,edges,signals:[],runs:floorRuns,defaultRun:floorRuns[0]?.id,actual:true,branchPods:pods,flowBasis:demoSource?null:FLOW_BASIS};
  return demo?demoFactory(model,demo):model;
}

export function canonicalSnapshot({factory_id,cursor,captured_at,freshness={status:"unknown",observed_at:null},state}) {
  return validateSnapshot({schema_version:1,factory_id,cursor,captured_at,freshness,state});
}
