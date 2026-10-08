const ID = /^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}$/;
const ACTION = /^[A-Za-z][A-Za-z0-9_-]{0,63}$/;
const MAX_ROWS = 256;
const MAX_TARGETS = 64;
const SOURCES = new Set(["demo", "recorded", "live"]);

const safeId = value => typeof value === "string" && ID.test(value) ? value : null;
const safeTime = value => typeof value === "string" && value.length <= 64 && Number.isFinite(Date.parse(value)) ? value : null;
const safeText = (value, fallback) => {
  if (typeof value !== "string") return fallback;
  const text = value.replace(/[\u0000-\u001f\u007f]/g, " ").trim().slice(0, 120);
  return text || fallback;
};
const unique = values => [...new Set(values)];

/** Prefer a readable pinned graph for initial Live viewing, without changing run facts. */
export function preferredFloorRunId(runs, unavailableRunIds = []) {
  const unavailable = new Set(unavailableRunIds);
  const started = run => {const value=Date.parse(run.state?.started_at ?? run.status?.started_at ?? '');return Number.isFinite(value)?value:0;};
  return [...runs].filter(run=>safeId(run.run_id)&&run.graph?.nodes?.length&&!unavailable.has(run.run_id))
    .sort((a,b)=>b.graph.nodes.length-a.graph.nodes.length||started(b)-started(a)||a.run_id.localeCompare(b.run_id))[0]?.run_id ?? null;
}

function idList(value) {
  if (!Array.isArray(value)) return [];
  return unique(value.slice(0, MAX_TARGETS).map(safeId).filter(Boolean));
}

function currentGraph(graph) {
  const nodes = Array.isArray(graph?.nodes) ? graph.nodes.slice(0, MAX_ROWS) : [];
  const byId = new Map();
  for (const node of nodes) {
    const id = safeId(node?.id);
    if (!id || byId.has(id)) continue;
    byId.set(id, {
      id,
      kind: typeof node.kind === "string" ? node.kind : "",
      label: safeText(node.name, id),
    });
  }
  return byId;
}

function declarationIds(declaredRoutes, name, graphNodes, { demoOnly = false, source } = {}) {
  if (demoOnly && source !== "demo") return [];
  return idList(declaredRoutes?.[name]).filter(id => graphNodes.has(id));
}

function waitItem(wait, graphNodes) {
  const nodeId = safeId(wait?.node);
  const node = nodeId ? graphNodes.get(nodeId) : null;
  const taskId = safeId(wait?.task?.id);
  const contextId = safeId(wait?.task?.context_id);
  const actions = Array.isArray(wait?.permitted_actions)
    ? unique(wait.permitted_actions.slice(0, 16).filter(value => typeof value === "string" && ACTION.test(value)))
    : [];
  return {
    run_id: safeId(wait?.run_id),
    task_id: taskId,
    context_id: contextId,
    node_id: nodeId,
    origin: node ? { node_id: node.id, label: node.label } : "unknown",
    actor_identity: safeId(wait?.wait_actor_identity),
    wait_started_at: safeTime(wait?.wait_started_at),
    wait_deadline: safeTime(wait?.wait_deadline),
    permitted_actions: actions,
  };
}

function incidentItem(incident, graphNodes) {
  const nodeId = safeId(incident?.node);
  const node = nodeId ? graphNodes.get(nodeId) : null;
  return {
    incident_id: safeId(incident?.incident_id),
    run_id: safeId(incident?.run_id),
    task_id: safeId(incident?.task_id ?? incident?.task?.id),
    context_id: safeId(incident?.context_id ?? incident?.task?.context_id),
    kind: typeof incident?.kind === "string" ? safeText(incident.kind, "unknown") : "unknown",
    state: typeof incident?.state === "string" ? safeText(incident.state, "unknown") : "unknown",
    owner_identity: safeId(incident?.owner_identity),
    node_id: nodeId,
    origin: node ? { node_id: node.id, label: node.label } : "unknown",
  };
}

function freshnessStatus(freshness) {
  return typeof freshness === "string" ? freshness : freshness?.status;
}

function channelState({ source, freshness, rowsPresent, itemCount, label }) {
  if (!SOURCES.has(source) || !rowsPresent) {
    return { state: "unavailable", reason: `${label} facts are unavailable from this source.` };
  }
  if(source==="live"&&freshness?.scope==="run"&&freshness.factory_status!=="fresh")return {state:"unavailable",reason:"Only the selected run is current; factory-wide inbox and incident coverage is unavailable."};
  if (source === "live" && freshnessStatus(freshness) !== "fresh") {
    return { state: "unavailable", reason: `Live Observation is ${freshnessStatus(freshness) || "unknown"}; last observed ${label.toLowerCase()} rows are retained.` };
  }
  if (itemCount > 0) {
    return { state: "pending", reason: label === "Human inbox"
      ? "Observed human wait facts are listed; supported actions still require current source authority."
      : "Observed incident records are listed with their reported states; no resolution is inferred." };
  }
  if (source === "demo") {
    return { state: "empty", reason: `No ${label.toLowerCase()} facts are declared or observed in this isolated fixture.` };
  }
  if (source === "recorded") {
    return { state: "empty", reason: `No ${label.toLowerCase()} facts are present in this historical recording.` };
  }
  return { state: "empty", reason: `No ${label.toLowerCase()} facts are currently observed.` };
}

/**
 * Build the shared, presentation-only communication/navigation model.
 * declaredRoutes accepts bounded ID arrays: entryTargets are navigation-only;
 * directorTargets and humanTargets are accepted only for isolated Demo data.
 */
export function communicationPresentation({ source, freshness, graph, waits, incidents, declaredRoutes } = {}) {
  const graphNodes = currentGraph(graph);
  const directorWaitIds = [...graphNodes.values()].filter(node => node.kind === "director_wait").map(node => node.id);
  const declaredDirectorIds = declarationIds(declaredRoutes, "directorTargets", graphNodes, { demoOnly: true, source });
  const declaredHumanIds = declarationIds(declaredRoutes, "humanTargets", graphNodes, { demoOnly: true, source });
  const entryIds = declarationIds(declaredRoutes, "entryTargets", graphNodes, { source });
  const directorIds = unique([...directorWaitIds, ...declaredDirectorIds]);
  const directorRoutes = directorIds.map(nodeId => {
    const node = graphNodes.get(nodeId);
    const demoRoute = declaredDirectorIds.includes(nodeId) && source === "demo";
    return {
      node_id: nodeId,
      kind: "declared_control",
      label: demoRoute ? `Demo route · ${node.label}` : `Director wait · ${node.label}`,
    };
  });
  for (const nodeId of entryIds.filter(id=>!directorIds.includes(id))) directorRoutes.push({
    node_id: nodeId,
    kind: "presentation_navigation",
    label: "Submission / Decisions view",
  });

  const humanWaits = Array.isArray(waits)
    ? waits.slice(0, MAX_ROWS).filter(wait => wait?.wait_role === "human")
    : [];
  const humanItems = humanWaits.map(wait => waitItem(wait, graphNodes));
  const humanTargets = unique([
    ...humanItems.map(item => item.node_id).filter(nodeId => nodeId && graphNodes.has(nodeId)),
    ...declaredHumanIds,
  ]);
  const incidentRows = Array.isArray(incidents) ? incidents.slice(0, MAX_ROWS) : [];
  const incidentItems = incidentRows.map(row => incidentItem(row, graphNodes));
  const inbox = channelState({ source, freshness, rowsPresent: Array.isArray(waits), itemCount: humanItems.length, label: "Human inbox" });
  const incidentChannel = channelState({ source, freshness, rowsPresent: Array.isArray(incidents), itemCount: incidentItems.length, label: "Incident channel" });
  const current = source !== "live" || freshnessStatus(freshness) === "fresh";
  const directorReason = !current
    ? `Live Observation is ${freshnessStatus(freshness) || "unknown"}; control availability is not current.`
    : directorIds.length
      ? "Targets are navigation facts only; supported actions come from current wait facts and server authorization."
      : entryIds.length
        ? "Presentation navigation only; no Director control target is declared."
        : "No supported Director target or entry navigation is available.";

  return {
    director: {
      targets: unique([...directorIds, ...entryIds]),
      routes: directorRoutes,
      route: "Decisions",
      label: directorIds.length ? "Director wait targets" : "Director navigation",
      reason: directorReason,
    },
    humanInbox: {
      state: inbox.state,
      items: humanItems,
      targets: humanTargets,
      route: "Decisions",
      reason: inbox.reason,
    },
    incidentChannel: {
      state: incidentChannel.state,
      items: incidentItems,
      route: "Decisions",
      reason: incidentChannel.reason,
    },
  };
}
