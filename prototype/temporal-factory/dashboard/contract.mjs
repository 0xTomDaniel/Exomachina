/** Dashboard v1 wire contract. Cursors are validated as strings and never decoded. */
export const DASHBOARD_SCHEMA_VERSION = 1;
export const CLOUD_EVENTS_VERSION = "1.0";
export const DASHBOARD_EVENT_SCHEMA = "urn:exomachina:dashboard:event:v1";

const ID = /^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}$/;
const CURSOR = /^c1\.[A-Za-z0-9_-]{16}\.[A-Za-z0-9_-]{40,48}$/;
const DIGEST = /^[0-9a-f]{64}$/;
const ENUM = /^[a-z][a-z0-9_.-]{0,63}$/;
const NAME = /^[A-Za-z0-9][A-Za-z0-9 _./-]{0,127}$/;
const CAPABILITY = /^[A-Za-z0-9][A-Za-z0-9._@+-]{0,127}$/;
const SENSITIVE = /(?:^|_)(?:token|secret|password|credential|authorization|private_key|signing_material|raw_history|run_inputs)(?:$|_)/i;
const EVENT_TYPES = [
  "com.exomachina.factory.discovered.v1", "com.exomachina.publication.activated.v1",
  "com.exomachina.run.created.v1", "com.exomachina.run.state_changed.v1",
  "com.exomachina.assignment.state_changed.v1", "com.exomachina.artifact.revised.v1",
  "com.exomachina.quality.verdict.v1", "com.exomachina.decision.outcome.v1",
  "com.exomachina.command.outcome.v1", "com.exomachina.delivery.receipt.v1",
  "com.exomachina.incident.state_changed.v1", "com.exomachina.admission.state_changed.v1",
  "com.exomachina.capacity.state_changed.v1", "com.exomachina.commercial.usage.v1",
  "com.exomachina.commercial.obligation.v1", "com.exomachina.commercial.payment.v1",
];
const COMMON = ["factory_id", "run_id", "task_id", "context_id", "assignment_id", "attempt_id", "manifest_digest", "package_digest", "definition_digest", "interpreter_build"];
const FIELDS = {
  "com.exomachina.factory.discovered.v1": ["name", "identity", "capability"],
  "com.exomachina.publication.activated.v1": ["publication_version", "graph_nodes", "service_bindings"],
  "com.exomachina.run.created.v1": ["state", "phase", "started_at", "graph_nodes"],
  "com.exomachina.run.state_changed.v1": ["state", "phase", "node", "repair_count", "max_repairs", "started_at", "ended_at", "wait_deadline", "wait_started_at", "wait_role", "wait_actor_identity", "permitted_actions"],
  "com.exomachina.assignment.state_changed.v1": ["capability", "provider_identity", "state", "node", "started_at", "ended_at", "queue_position"],
  "com.exomachina.artifact.revised.v1": ["artifact_revision", "artifact_sha256", "previous_revision", "previous_sha256", "media_type", "byte_length", "author_identity"],
  "com.exomachina.quality.verdict.v1": ["artifact_revision", "artifact_sha256", "reviewer_identity", "accepted", "finding_count", "finding_codes"],
  "com.exomachina.decision.outcome.v1": ["decision_id", "action", "outcome", "expected_state", "resulting_state", "artifact_revision", "artifact_sha256", "actor_identity"],
  "com.exomachina.command.outcome.v1": ["command_id", "lifecycle", "outcome", "expected_state", "resulting_state", "artifact_revision", "artifact_sha256"],
  "com.exomachina.delivery.receipt.v1": ["receipt_id", "artifact_revision", "artifact_sha256", "destination_id", "delivered_at", "outcome", "markdown_sha256", "destination_identity", "delivery_kind", "byte_length"],
  "com.exomachina.incident.state_changed.v1": ["incident_id", "kind", "state", "owner_identity", "evidence_refs"],
  "com.exomachina.admission.state_changed.v1": ["admission_id", "state", "queue_position", "capacity_limit"],
  "com.exomachina.capacity.state_changed.v1": ["capacity_limit", "active_count", "queued_count"],
  "com.exomachina.commercial.usage.v1": ["usage_id", "unit", "quantity", "measurement_source", "completeness", "evidence_status", "model_id", "model_call_id", "service_identity", "reasoning_effort"],
  "com.exomachina.commercial.obligation.v1": ["obligation_id", "component", "offer_digest", "amount_atoms", "currency", "atomic_scale", "evidence_status", "price_basis", "payment_trigger", "markup_bps", "state"],
  "com.exomachina.commercial.payment.v1": ["payment_id", "network", "asset", "amount_atoms", "currency", "atomic_scale", "state", "receipt_id", "evidence_status"],
};
const REQUIRED = {
  "com.exomachina.factory.discovered.v1": ["identity", "capability", "name"],
  "com.exomachina.publication.activated.v1": ["manifest_digest", "package_digest", "definition_digest", "interpreter_build"],
  "com.exomachina.run.created.v1": ["run_id", "manifest_digest", "package_digest", "definition_digest", "interpreter_build", "state"],
  "com.exomachina.run.state_changed.v1": ["run_id", "state", "phase"],
  "com.exomachina.assignment.state_changed.v1": ["run_id", "task_id", "assignment_id", "attempt_id", "capability", "state"],
  "com.exomachina.artifact.revised.v1": ["run_id", "artifact_revision", "artifact_sha256"],
  "com.exomachina.quality.verdict.v1": ["run_id", "task_id", "artifact_revision", "artifact_sha256", "reviewer_identity", "accepted", "finding_count"],
  "com.exomachina.decision.outcome.v1": ["run_id", "decision_id", "action", "outcome"],
  "com.exomachina.command.outcome.v1": ["run_id", "command_id", "lifecycle"],
  "com.exomachina.delivery.receipt.v1": ["run_id", "receipt_id", "artifact_revision", "artifact_sha256", "outcome"],
  "com.exomachina.incident.state_changed.v1": ["run_id", "incident_id", "kind", "state"],
  "com.exomachina.admission.state_changed.v1": ["run_id", "admission_id", "state", "capacity_limit"],
  "com.exomachina.capacity.state_changed.v1": ["capacity_limit", "active_count", "queued_count"],
  "com.exomachina.commercial.usage.v1": ["run_id", "assignment_id", "attempt_id", "usage_id", "unit", "completeness", "evidence_status"],
  "com.exomachina.commercial.obligation.v1": ["run_id", "assignment_id", "attempt_id", "obligation_id", "component", "amount_atoms", "currency", "atomic_scale", "evidence_status", "state"],
  "com.exomachina.commercial.payment.v1": ["run_id", "payment_id", "amount_atoms", "currency", "atomic_scale", "state", "evidence_status"],
};
const ID_FIELDS = new Set(["factory_id", "run_id", "task_id", "context_id", "assignment_id", "attempt_id", "artifact_revision", "previous_revision", "node", "identity", "provider_identity", "author_identity", "reviewer_identity", "actor_identity", "owner_identity", "wait_actor_identity", "destination_id", "destination_identity", "decision_id", "command_id", "receipt_id", "incident_id", "admission_id", "usage_id", "model_call_id", "service_identity", "obligation_id", "payment_id", "interpreter_build"]);
const DIGEST_FIELDS = new Set(["manifest_digest", "package_digest", "definition_digest", "artifact_sha256", "previous_sha256", "markdown_sha256", "offer_digest"]);
const ENUM_FIELDS = new Set(["state", "phase", "outcome", "expected_state", "resulting_state", "lifecycle", "kind", "measurement_source", "completeness", "reasoning_effort", "evidence_status", "price_basis", "payment_trigger", "component", "delivery_kind", "wait_role"]);
const TIME_FIELDS = new Set(["started_at", "ended_at", "wait_deadline", "wait_started_at", "delivered_at"]);
const INT_FIELDS = new Set(["repair_count", "max_repairs", "queue_position", "byte_length", "finding_count", "capacity_limit", "active_count", "queued_count", "amount_atoms", "atomic_scale", "markup_bps"]);
const STR_FIELDS = new Set(["name", "publication_version", "currency", "asset", "network", "media_type", "unit", "model_id", "quantity", "capability"]);
const UNAVAILABLE_COMPLETENESS = new Set(["unknown", "undisclosed"]);
const COMPLETENESS = new Set(["complete", "unknown", "undisclosed"]);
const EVIDENCE_STATUS = new Set(["measured", "calculated_from_measured_usage", "provider_reported", "estimated", "unknown", "undisclosed"]);
const OBLIGATION_COMPONENTS = new Set(["inference_cost", "hosting_cost", "markup", "supplier_charge", "payment_fees", "owner_overhead", "production_cost", "customer_price"]);

export class DashboardContractError extends Error {
  constructor(message, path = "$") { super(`${path}: ${message}`); this.name = "DashboardContractError"; this.path = path; }
}

/*
 * Illustrative Demo layer. The isolated Demo Adapter replays hand-authored local
 * scenarios whose presentation facts (scenario clock, simulated prices/budgets,
 * movement, Director turns, alarms, notes, ...) have no Live/Recorded meaning.
 * They cross the shared Seam only as an explicitly labelled layer that the
 * validators accept when, and only when, the caller declares the source "demo".
 * Live and Recorded payloads (the default) keep the strict public allowlist, so a
 * copied illustrative field or event in those payloads is rejected (S33).
 */
export const DEMO_ILLUSTRATION_EVENT_TYPE = "com.exomachina.demo.illustration.v1";
export const DEMO_ILLUSTRATION_LABEL = "Illustrative Demo fixture";
const DEMO_SOURCE = "demo";
const DEMO_KINDS = new Set(["job","admit","hold","spawn","move","consume","work","verdict","flag","readout","director","wait","decide","escalate","release","scrap","alarm","command","note","end","jobend","publish"]);
const DEMO_TEXT_KEYS = new Set(["item","art","at","from","to","step","label","rev","sha","msg","level","level2","text","by","cmd","outcome","action","stamp","verdict","finding","flag","value","target","responder","context","onExpiry","task","version","brief","reason","rationale","job"]);
const DEMO_NUMBER_KEYS = new Set(["t","order","dur","attempt","repair","queued","findings"]);
const DEMO_OPTIONAL_NUMBER_KEYS = new Set(["t1","expires"]);
const DEMO_BOOLEAN_KEYS = new Set(["loop","limit","human","escalated","tick"]);
const DEMO_KEY = /^[A-Za-z0-9][A-Za-z0-9_.:@+ -]{0,63}$/;
const demoSource = options => options?.source === DEMO_SOURCE;
const demoText = (value, path, max = 512) => { if (typeof value !== "string" || value.length > max || /[\u0000-\u001f\u007f]/.test(value)) throw new DashboardContractError("invalid illustrative text", path); };
const demoNumber = (value, path) => { if (typeof value !== "number" || !Number.isFinite(value) || Math.abs(value) > 1e9) throw new DashboardContractError("invalid illustrative number", path); };
function demoStrings(value, path, max = 32) {
  if (!Array.isArray(value) || value.length > max) throw new DashboardContractError("invalid illustrative list", path);
  value.forEach((entry, i) => demoText(entry, `${path}[${i}]`, 256));
}
/** One fixture timeline entry; every key and value shape is allowlisted. */
function validateDemoIllustration(value, path) {
  record(value, path);
  if (!DEMO_KINDS.has(value.type)) throw new DashboardContractError("unsupported illustrative event kind", `${path}.type`);
  demoNumber(value.t, `${path}.t`);
  for (const [key, entry] of Object.entries(value)) {
    const p = `${path}.${key}`;
    if (key === "type") continue;
    if (DEMO_TEXT_KEYS.has(key)) { if (key === "job" && entry === null) continue; demoText(entry, p); }
    else if (DEMO_NUMBER_KEYS.has(key)) demoNumber(entry, p);
    else if (DEMO_OPTIONAL_NUMBER_KEYS.has(key)) { if (entry !== null) demoNumber(entry, p); }
    else if (DEMO_BOOLEAN_KEYS.has(key)) { if (typeof entry !== "boolean") throw new DashboardContractError("expected boolean", p); }
    else if (key === "tools" || key === "allowed") demoStrings(entry, p);
    else if (key === "rec") {
      record(entry, p); exactKeys(entry, new Set(["t","action","text","why"]), p);
      if (entry.t != null) demoNumber(entry.t, `${p}.t`);
      for (const k of ["action","text","why"]) if (entry[k] != null) demoText(entry[k], `${p}.${k}`);
    } else if (key === "parts") {
      if (!Array.isArray(entry) || entry.length > 16) throw new DashboardContractError("invalid illustrative note", p);
      entry.forEach((part, i) => {
        const q = `${p}[${i}]`;
        if (typeof part === "string") return demoText(part, q);
        record(part, q); exactKeys(part, new Set(["inbox","label","step","job","item"]), q);
        if (part.inbox != null && typeof part.inbox !== "boolean") throw new DashboardContractError("expected boolean", `${q}.inbox`);
        for (const k of ["label","step","job","item"]) if (part[k] != null) demoText(part[k], `${q}.${k}`, 256);
      });
    } else throw new DashboardContractError("illustrative field is not allowlisted", p);
  }
}
/** Bounded generic metadata (simulated budgets, prices, bindings, step cues). */
function validateDemoValue(value, path, depth = 0) {
  if (depth > 6) throw new DashboardContractError("illustrative metadata is too deep", path);
  if (value === null || typeof value === "boolean") return;
  if (typeof value === "number") return demoNumber(value, path);
  if (typeof value === "string") return demoText(value, path);
  if (Array.isArray(value)) { if (value.length > 64) throw new DashboardContractError("illustrative list is too long", path); return value.forEach((entry, i) => validateDemoValue(entry, `${path}[${i}]`, depth + 1)); }
  record(value, path);
  const entries = Object.entries(value);
  if (entries.length > 128) throw new DashboardContractError("illustrative record is too large", path);
  for (const [key, entry] of entries) { if (!DEMO_KEY.test(key)) throw new DashboardContractError("invalid illustrative key", `${path}.${key}`); validateDemoValue(entry, `${path}.${key}`, depth + 1); }
}
const DEMO_FACTORY_KEYS = new Set(["digest","versions","provenance","departments","programs","mainArt","admission","sla","queueAdvisory","budget","agents","artifacts","steps"]);
const DEMO_RUN_KEYS = new Set(["id","run_ids","name","brief","outcomeLabel","tone","start","startAt","now","live","multi"]);
function validateDemoPresentation(value, path) {
  record(value, path); exactKeys(value, new Set(["label","factory","runs"]), path);
  if (value.label !== DEMO_ILLUSTRATION_LABEL) throw new DashboardContractError("illustrative Demo layer must carry its Demo label", `${path}.label`);
  record(value.factory, `${path}.factory`); exactKeys(value.factory, DEMO_FACTORY_KEYS, `${path}.factory`);
  validateDemoValue(value.factory, `${path}.factory`);
  if (!Array.isArray(value.runs) || value.runs.length > 64) throw new DashboardContractError("expected bounded illustrative runs", `${path}.runs`);
  value.runs.forEach((run, i) => {
    const p = `${path}.runs[${i}]`; record(run, p); exactKeys(run, DEMO_RUN_KEYS, p);
    if (typeof run.id !== "string" || !ID.test(run.id)) throw new DashboardContractError("invalid illustrative run id", `${p}.id`);
    if (!Array.isArray(run.run_ids) || run.run_ids.length > 512 || run.run_ids.some(id => typeof id !== "string" || !ID.test(id))) throw new DashboardContractError("invalid illustrative run bindings", `${p}.run_ids`);
    time(run.start, `${p}.start`);
    for (const k of ["name","brief","outcomeLabel","tone"]) if (run[k] != null) demoText(run[k], `${p}.${k}`);
    for (const k of ["startAt","now"]) if (run[k] != null) demoNumber(run[k], `${p}.${k}`);
    for (const k of ["live","multi"]) if (run[k] != null && typeof run[k] !== "boolean") throw new DashboardContractError("expected boolean", `${p}.${k}`);
  });
}
function validateDemoEvent(event, options) {
  if (!demoSource(options)) throw new DashboardContractError("illustrative Demo events are not permitted for this source", "$event.type");
  const data = event.data, p = "$event.data";
  record(data, p); exactKeys(data, new Set(["schema_version","factory_id","run_id","task_id","context_id","illustration"]), p);
  if (data.schema_version !== 1) throw new DashboardContractError("schema_version must be 1", `${p}.schema_version`);
  for (const k of ["factory_id","run_id","task_id","context_id"]) if (k === "factory_id" || data[k] != null) { string(data[k], `${p}.${k}`); if (!ID.test(data[k])) throw new DashboardContractError("invalid identifier", `${p}.${k}`); }
  validateDemoIllustration(data.illustration, `${p}.illustration`);
}
export const isRecord = value => value !== null && typeof value === "object" && !Array.isArray(value);
const record = (v, p) => { if (!isRecord(v)) throw new DashboardContractError("expected object", p); };
const string = (v, p, empty = false) => { if (typeof v !== "string" || (!empty && !v)) throw new DashboardContractError("expected string", p); };
const time = (v, p) => { string(v, p); if (!Number.isFinite(Date.parse(v)) || !/[zZ]|[+-]\d\d:\d\d$/.test(v)) throw new DashboardContractError("expected timezone-qualified date-time", p); };
function sensitiveScan(v, p = "$") {
  if (Array.isArray(v)) return v.forEach((x, i) => sensitiveScan(x, `${p}[${i}]`));
  if (!isRecord(v)) return;
  for (const [k, x] of Object.entries(v)) { if (SENSITIVE.test(k)) throw new DashboardContractError("sensitive field is forbidden", `${p}.${k}`); sensitiveScan(x, `${p}.${k}`); }
}
function validateData(data, eventType, p) {
  record(data, p);
  if (data.schema_version !== 1) throw new DashboardContractError("schema_version must be 1", `${p}.schema_version`);
  for (const k of Object.keys(data)) if (k !== "schema_version" && !COMMON.includes(k) && !Object.values(FIELDS).some(a => a.includes(k))) throw new DashboardContractError("field is not allowlisted", `${p}.${k}`);
  string(data.factory_id, `${p}.factory_id`); if (!ID.test(data.factory_id)) throw new DashboardContractError("invalid identifier", `${p}.factory_id`);
  if (eventType) {
    const allowed = new Set(["schema_version", ...COMMON, ...FIELDS[eventType]]);
    for (const k of Object.keys(data)) if (!allowed.has(k)) throw new DashboardContractError("field is not allowlisted for event type", `${p}.${k}`);
    for (const k of REQUIRED[eventType]) if (!(k in data)) throw new DashboardContractError("required event fact missing", `${p}.${k}`);
  }
  if (eventType === "com.exomachina.run.state_changed.v1" && ["wait_role","wait_actor_identity","wait_started_at","permitted_actions"].some(k=>Object.hasOwn(data,k))) {
    const expected={"awaiting-director":"director","awaiting-human":"human"}[data.phase];
    if (!expected || (data.wait_role != null && data.wait_role !== expected)) throw new DashboardContractError("wait facts do not match observed phase", `${p}.phase`);
  }
  if (data.delivery_kind === "local_file") {
    for (const key of ["run_id","task_id","context_id","receipt_id","artifact_revision","artifact_sha256","markdown_sha256","destination_identity","byte_length","delivered_at"])
      if (data[key] == null) throw new DashboardContractError("local delivery binding missing", `${p}.${key}`);
    if (data.outcome !== "local-file-delivered") throw new DashboardContractError("local delivery outcome mismatch", `${p}.outcome`);
  }
  for (const [k, v] of Object.entries(data)) {
    if (k === "schema_version") continue;
    if (k === "wait_role" && !["director","human"].includes(v)) throw new DashboardContractError("unsupported wait role", `${p}.${k}`);
    if (k === "permitted_actions") {
      if (!Array.isArray(v) || v.length > 16 || new Set(v).size !== v.length || v.some(action => typeof action !== "string" || !ENUM.test(action))) throw new DashboardContractError("invalid bounded action array", `${p}.${k}`);
      continue;
    }
    if (k === "delivery_kind" && v !== "local_file") throw new DashboardContractError("unsupported delivery kind", `${p}.${k}`);
    if (k === "quantity" && v === null) throw new DashboardContractError("quantity must be an exact decimal string or omitted", `${p}.quantity`);
    if (ID_FIELDS.has(k)) { string(v, `${p}.${k}`); if (!ID.test(v)) throw new DashboardContractError("invalid identifier", `${p}.${k}`); }
    else if (DIGEST_FIELDS.has(k)) { string(v, `${p}.${k}`); if (!DIGEST.test(v)) throw new DashboardContractError("invalid digest", `${p}.${k}`); }
    else if (ENUM_FIELDS.has(k)) { string(v, `${p}.${k}`); if (!ENUM.test(v) || (k === "evidence_status" && !EVIDENCE_STATUS.has(v))) throw new DashboardContractError("invalid enum", `${p}.${k}`); }
    else if (TIME_FIELDS.has(k)) time(v, `${p}.${k}`);
    else if (INT_FIELDS.has(k)) {
      if (k === "amount_atoms" && eventType === "com.exomachina.commercial.obligation.v1") {
        if (v === null) {
          if (!UNAVAILABLE_COMPLETENESS.has(data.evidence_status)) throw new DashboardContractError("null amount_atoms requires unknown or undisclosed evidence", `${p}.${k}`);
        } else if (!Number.isSafeInteger(v)) throw new DashboardContractError("expected integer or permitted null", `${p}.${k}`);
      } else if (!Number.isSafeInteger(v) || v < 0) throw new DashboardContractError("expected non-negative integer", `${p}.${k}`);
    }
    else if (k === "accepted") { if (typeof v !== "boolean") throw new DashboardContractError("expected boolean", `${p}.${k}`); }
    else if (STR_FIELDS.has(k)) {
      string(v, `${p}.${k}`);
      const pattern = k === "name" || k === "publication_version" ? NAME : k === "capability" ? CAPABILITY : k === "quantity" ? /^(0|[1-9][0-9]*)(\.[0-9]+)?$/ : k === "media_type" ? /^[a-z0-9.+-]+\/[a-z0-9.+-]+$/ : k === "unit" ? /^[a-z][a-z0-9._-]{0,31}$/ : k === "model_id" ? /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/ : /^[A-Z0-9][A-Z0-9._-]{0,15}$/;
      if (!pattern.test(v)) throw new DashboardContractError("invalid string format", `${p}.${k}`);
    } else if (k === "graph_nodes") {
      if (!Array.isArray(v) || v.length > 500) throw new DashboardContractError("expected graph node array", `${p}.${k}`);
      const ids = new Set();
      v.forEach((n, i) => { const q = `${p}.${k}[${i}]`; record(n, q); if (Object.keys(n).some(x => !["id", "type", "next", "capability", "output"].includes(x))) throw new DashboardContractError("unexpected graph field", q); if (n.output != null && !NODE_OUTPUTS.has(n.output)) throw new DashboardContractError("invalid node output mode", `${q}.output`); string(n.id, `${q}.id`); string(n.type, `${q}.type`); if (!ID.test(n.id) || !ID.test(n.type) || ids.has(n.id)) throw new DashboardContractError("invalid or duplicate graph node", q); ids.add(n.id); if (n.next != null && (!Array.isArray(n.next) || n.next.some(x => typeof x !== "string"))) throw new DashboardContractError("invalid next list", `${q}.next`); if (n.capability != null && !CAPABILITY.test(n.capability)) throw new DashboardContractError("invalid capability", `${q}.capability`); });
      for (const n of v) for (const to of n.next ?? []) if (!ids.has(to)) throw new DashboardContractError("graph edge target missing", `${p}.${k}`);
    } else if (k === "service_bindings") {
      if (!Array.isArray(v) || v.length > 100) throw new DashboardContractError("expected service binding array", `${p}.${k}`);
      v.forEach((b, i) => { const q = `${p}.${k}[${i}]`; record(b, q); for (const r of ["name", "role", "identity", "contract_digest"]) if (!(r in b)) throw new DashboardContractError("incomplete service binding", q); if (Object.keys(b).some(x => !["name", "role", "identity", "capability", "contract_digest"].includes(x))) throw new DashboardContractError("unexpected binding field", q); validatePublicRecord(b,q); });
    } else if (k === "finding_codes" || k === "evidence_refs") {
      if (!Array.isArray(v) || v.length > 100) throw new DashboardContractError("expected string array", `${p}.${k}`);
      v.forEach((x, i) => { if (typeof x !== "string" || !(k === "finding_codes" ? ENUM.test(x) : DIGEST.test(x))) throw new DashboardContractError("invalid array entry", `${p}.${k}[${i}]`); });
    }
  }
  if (eventType === "com.exomachina.commercial.usage.v1") {
    if (!COMPLETENESS.has(data.completeness)) throw new DashboardContractError("invalid usage completeness", `${p}.completeness`);
    if (data.quantity != null && data.completeness !== "complete") throw new DashboardContractError("reported quantity requires complete completeness", `${p}.quantity`);
    if (!("quantity" in data) && !UNAVAILABLE_COMPLETENESS.has(data.completeness)) throw new DashboardContractError("quantity may be omitted only when completeness is unknown or undisclosed", `${p}.quantity`);
  }
  if (eventType === "com.exomachina.commercial.obligation.v1") {
    if (!OBLIGATION_COMPONENTS.has(data.component)) throw new DashboardContractError("unsupported obligation component", `${p}.component`);
    if (data.amount_atoms === null && !UNAVAILABLE_COMPLETENESS.has(data.evidence_status)) throw new DashboardContractError("null amount_atoms requires unknown or undisclosed evidence", `${p}.amount_atoms`);
  }
  return data;
}

/*
 * Content-free hand-off Observation facts (A2A v1 mediation decision 4; INTERFACES.md
 * "A2A v1 baseline and hand-off records"). Each type has an exact field set, validated
 * separately from the generic allowlist so no other event type or snapshot row is
 * widened. Content-bearing fields (text, data, bytes, names, descriptions, artifactId,
 * filenames, URLs, metadata) are not allowlisted at any depth and are rejected.
 */
export const HANDOFF_EVENT_TYPES = Object.freeze(["com.exomachina.handoff.produced.v1", "com.exomachina.handoff.consumed.v1", "com.exomachina.handoff.item_ready.v1"]);
export const HANDOFF_PART_KINDS = Object.freeze(["text", "data", "raw", "url"]);
const HANDOFF_SOURCES = new Set(["artifact", "message"]);
const HANDOFF_BASE = ["schema_version", "factory_id", "run_id", "assignment_id", "attempt_id", "node"];
const HANDOFF_FIELDS = {
  "com.exomachina.handoff.produced.v1": [...HANDOFF_BASE, "handoff_id", "handoff_revision", "produced_at", "items"],
  "com.exomachina.handoff.consumed.v1": [...HANDOFF_BASE, "consumed_at", "inputs"],
  "com.exomachina.handoff.item_ready.v1": [...HANDOFF_BASE, "handoff_id", "item_index", "part_kinds", "media_type", "ready_at"],
};
const HANDOFF_ITEM_KEYS = new Set(["item_index", "source", "part_kinds", "media_type", "byte_length", "ready_at", "digest", "artifact_revision", "artifact_sha256"]);
const HANDOFF_ITEM_REQUIRED = ["item_index", "source", "part_kinds", "media_type", "byte_length", "ready_at", "digest"];
const HANDOFF_INPUT_KEYS = new Set(["handoff_id", "item_digests"]);
const MEDIA_TYPE = /^[a-z0-9.+-]+\/[a-z0-9.+-]+$/;
const handoffId = (v, p) => { string(v, p); if (!ID.test(v)) throw new DashboardContractError("invalid identifier", p); };
const handoffIndex = (v, p) => { if (!Number.isSafeInteger(v) || v < 0 || v > 255) throw new DashboardContractError("expected bounded item index", p); };
const handoffDigest = (v, p) => { string(v, p); if (!DIGEST.test(v)) throw new DashboardContractError("invalid digest", p); };
function handoffPartKinds(v, p) {
  if (!Array.isArray(v) || !v.length || v.length > 64 || v.some(kind => !HANDOFF_PART_KINDS.includes(kind))) throw new DashboardContractError("invalid part kinds", p);
}
// A2A v1 mediaType is optional on a part; null records "not declared".
function handoffMediaType(v, p) { if (v === null) return; string(v, p); if (v.length > 127 || !MEDIA_TYPE.test(v)) throw new DashboardContractError("invalid media type", p); }
function validateHandoffItem(item, p) {
  record(item, p); exactKeys(item, HANDOFF_ITEM_KEYS, p);
  for (const k of HANDOFF_ITEM_REQUIRED) if (!(k in item)) throw new DashboardContractError("required hand-off item fact missing", `${p}.${k}`);
  handoffIndex(item.item_index, `${p}.item_index`);
  if (!HANDOFF_SOURCES.has(item.source)) throw new DashboardContractError("invalid hand-off item source", `${p}.source`);
  handoffPartKinds(item.part_kinds, `${p}.part_kinds`);
  handoffMediaType(item.media_type, `${p}.media_type`);
  if (item.byte_length === null) { if (!item.part_kinds.includes("url")) throw new DashboardContractError("byte_length may be null only for url parts", `${p}.byte_length`); }
  else if (!Number.isSafeInteger(item.byte_length) || item.byte_length < 0) throw new DashboardContractError("expected non-negative integer", `${p}.byte_length`);
  time(item.ready_at, `${p}.ready_at`);
  handoffDigest(item.digest, `${p}.digest`);
  const report = ["artifact_revision", "artifact_sha256"].filter(k => k in item);
  if (report.length) {
    if (report.length !== 2 || item.source !== "artifact") throw new DashboardContractError("report artifact reference requires an artifact item with revision and sha256", p);
    handoffId(item.artifact_revision, `${p}.artifact_revision`); handoffDigest(item.artifact_sha256, `${p}.artifact_sha256`);
  }
}
function validateHandoffData(data, eventType, p) {
  record(data, p);
  const allowed = new Set(HANDOFF_FIELDS[eventType]);
  exactKeys(data, allowed, p);
  for (const k of allowed) if (!(k in data)) throw new DashboardContractError("required event fact missing", `${p}.${k}`);
  if (data.schema_version !== 1) throw new DashboardContractError("schema_version must be 1", `${p}.schema_version`);
  for (const k of ["factory_id", "run_id", "assignment_id", "attempt_id", "node"]) handoffId(data[k], `${p}.${k}`);
  if (eventType === "com.exomachina.handoff.produced.v1") {
    handoffId(data.handoff_id, `${p}.handoff_id`);
    if (!Number.isSafeInteger(data.handoff_revision) || data.handoff_revision < 1) throw new DashboardContractError("handoff_revision must be a positive integer", `${p}.handoff_revision`);
    time(data.produced_at, `${p}.produced_at`);
    // A hand-off never travels empty; a message output is exactly one message item.
    if (!Array.isArray(data.items) || !data.items.length || data.items.length > 256) throw new DashboardContractError("hand-off requires a bounded non-empty item array", `${p}.items`);
    data.items.forEach((item, i) => validateHandoffItem(item, `${p}.items[${i}]`));
    if (new Set(data.items.map(item => item.item_index)).size !== data.items.length) throw new DashboardContractError("duplicate hand-off item index", `${p}.items`);
    if (data.items.some(item => item.source === "message") && data.items.length !== 1) throw new DashboardContractError("a message hand-off holds exactly one item", `${p}.items`);
  } else if (eventType === "com.exomachina.handoff.consumed.v1") {
    time(data.consumed_at, `${p}.consumed_at`);
    if (!Array.isArray(data.inputs) || !data.inputs.length || data.inputs.length > 64) throw new DashboardContractError("consumption requires a bounded non-empty input array", `${p}.inputs`);
    data.inputs.forEach((input, i) => {
      const q = `${p}.inputs[${i}]`; record(input, q); exactKeys(input, HANDOFF_INPUT_KEYS, q);
      handoffId(input.handoff_id, `${q}.handoff_id`);
      if (!Array.isArray(input.item_digests) || !input.item_digests.length || input.item_digests.length > 256) throw new DashboardContractError("input requires a bounded non-empty digest array", `${q}.item_digests`);
      input.item_digests.forEach((digest, j) => handoffDigest(digest, `${q}.item_digests[${j}]`));
    });
    if (new Set(data.inputs.map(input => input.handoff_id)).size !== data.inputs.length) throw new DashboardContractError("duplicate consumed hand-off", `${p}.inputs`);
  } else {
    handoffId(data.handoff_id, `${p}.handoff_id`);
    handoffIndex(data.item_index, `${p}.item_index`);
    handoffPartKinds(data.part_kinds, `${p}.part_kinds`);
    handoffMediaType(data.media_type, `${p}.media_type`);
    time(data.ready_at, `${p}.ready_at`);
  }
  return data;
}

export function validateCloudEvent(event, options = {}) {
  record(event, "$event");
  const required = ["specversion", "id", "source", "type", "time", "subject", "datacontenttype", "dataschema", "data"];
  if (Object.keys(event).some(k => !required.includes(k)) || required.some(k => !(k in event))) throw new DashboardContractError("invalid CloudEvent envelope fields", "$event");
  if (event.specversion !== "1.0") throw new DashboardContractError("specversion must be 1.0", "$event.specversion");
  if (!/^obs-[0-9a-f]{64}$/.test(event.id)) throw new DashboardContractError("invalid stable event id", "$event.id");
  string(event.source, "$event.source"); if (!/^\/factories\/[A-Za-z0-9._%:/@+-]{1,768}$/.test(event.source)) throw new DashboardContractError("invalid event source", "$event.source");
  const illustrative = event.type === DEMO_ILLUSTRATION_EVENT_TYPE;
  const handoff = HANDOFF_EVENT_TYPES.includes(event.type);
  if (!illustrative && !handoff && !EVENT_TYPES.includes(event.type)) throw new DashboardContractError("unsupported CloudEvent type", "$event.type");
  time(event.time, "$event.time"); string(event.subject, "$event.subject");
  if (event.datacontenttype !== "application/json" || event.dataschema !== DASHBOARD_EVENT_SCHEMA) throw new DashboardContractError("unsupported CloudEvent data schema", "$event");
  if (illustrative) validateDemoEvent(event, options); else if (handoff) validateHandoffData(event.data, event.type, "$event.data"); else validateData(event.data, event.type, "$event.data");
  sensitiveScan(event);
  return event;
}

const SNAPSHOT_KEYS = new Set(["schema_version", "cursor", "captured_at", "freshness", "state"]);
const STATE_KEYS = new Set(["factory", "runs", "active_publication", "capacity", "commercial"]);
const exactKeys = (value, allowed, path) => { for (const key of Object.keys(value)) if (!allowed.has(key)) throw new DashboardContractError("field is not allowlisted", `${path}.${key}`); };
const checkText = (value,path,max=128) => { string(value,path); if(value.length>max||/[\u0000-\u001f]/.test(value))throw new DashboardContractError("invalid safe label",path); };
function validateSnapshotDataRecord(value, path) { if (value == null) return; validateData(value, null, path); }
const NODE_OUTPUTS=new Set(["artifacts","message","none"]),EDGE_KINDS=new Set(["material","control"]);
function validatePublicGraph(graph,path){
  record(graph,path);exactKeys(graph,new Set(["nodes","edges"]),path);
  if(!Array.isArray(graph.nodes)||!Array.isArray(graph.edges))throw new DashboardContractError("graph requires nodes and edges",path);
  const nodeIds=new Set();const nodeKeys=new Set(["id","type","kind","name","short","sub","capability","agent","dept","next","loop","responder","allowed","allowed_actions","role","output"]);
  graph.nodes.forEach((node,i)=>{const p=`${path}.nodes[${i}]`;record(node,p);exactKeys(node,nodeKeys,p);if(typeof node.id!=="string"||!ID.test(node.id))throw new DashboardContractError("invalid graph node identifier",`${p}.id`);const nodeKind=node.kind??node.type;if(typeof nodeKind!=="string"||!ID.test(nodeKind))throw new DashboardContractError("graph node requires a kind",`${p}.kind`);if(nodeIds.has(node.id))throw new DashboardContractError("duplicate graph node",`${p}.id`);nodeIds.add(node.id);for(const key of ["name","short","sub","role"])if(node[key]!=null)checkText(node[key],`${p}.${key}`);for(const key of ["kind","type","capability","agent","dept","responder"])if(node[key]!=null){string(node[key],`${p}.${key}`);if(!ID.test(node[key]))throw new DashboardContractError("invalid graph label",`${p}.${key}`);}for(const key of ["next","allowed","allowed_actions"])if(node[key]!=null&&(!Array.isArray(node[key])||node[key].some(x=>typeof x!=="string"||!ID.test(x))))throw new DashboardContractError("invalid graph reference list",`${p}.${key}`);if(node.output!=null&&!NODE_OUTPUTS.has(node.output))throw new DashboardContractError("invalid node output mode",`${p}.output`);});
  graph.edges.forEach((edge,i)=>{const p=`${path}.edges[${i}]`;record(edge,p);exactKeys(edge,new Set(["from","to","label","loop","kind"]),p);if(!nodeIds.has(edge.from)||!nodeIds.has(edge.to))throw new DashboardContractError("edge endpoint is not in graph",p);if(edge.label!=null)checkText(edge.label,`${p}.label`);if(edge.loop!=null&&typeof edge.loop!=="boolean")throw new DashboardContractError("loop must be boolean",`${p}.loop`);if(edge.kind!=null&&!EDGE_KINDS.has(edge.kind))throw new DashboardContractError("invalid edge kind",`${p}.kind`);});
  // A side-effect node (output "none") has no outgoing material edge (decision 5).
  const sideEffects=new Set(graph.nodes.filter(node=>node.output==="none").map(node=>node.id));
  graph.edges.forEach((edge,i)=>{if(sideEffects.has(edge.from)&&(edge.kind??"material")==="material")throw new DashboardContractError("side-effect node may not have an outgoing material edge",`${path}.edges[${i}]`);});
}
const RECORD_KEYS=new Set([...new Set(["id","attempts","task","context_id","status","pinned","assignments","artifacts","quality","decisions","commands","delivery","incidents","admissions","run_id","factory_id","limit","capacity_limit","active_count","queued_count","byte_length","version","publication_version","label","digest","manifest_digest","package_digest","definition_digest","interpreter_build","contract_digest","graph","nodes","edges","from","to","loop","source_label","model_label","fixture_label","role",...COMMON,...Object.values(FIELDS).flat()])]);
function validatePublicRecord(row,path){
  if(row==null)return;
  record(row,path);exactKeys(row,new Set([...RECORD_KEYS,"schema_version"]),path);
  const eventData=row.schema_version!=null||row.factory_id!=null;
  if(eventData)validateData(row,null,path);
  for(const [key,value]of Object.entries(row)){
    if(key==="id"||key.endsWith("_id")){string(value,`${path}.${key}`);if(!ID.test(value))throw new DashboardContractError("invalid identifier",`${path}.${key}`);}
    else if(key==="attempts"){if(!Array.isArray(value)||value.length>256)throw new DashboardContractError("attempts must be bounded array",`${path}.${key}`);value.forEach((x,i)=>validatePublicRecord(x,`${path}.${key}[${i}]`));}
    else if(key==="task"){record(value,`${path}.task`);exactKeys(value,new Set(["id","context_id"]),`${path}.task`);for(const k of ["id","context_id"])if(value[k]!=null&&(!ID.test(value[k])))throw new DashboardContractError("invalid task reference",`${path}.task.${k}`);}
    else if(key==="status"){if(typeof value==="string"){if(!ENUM.test(value))throw new DashboardContractError("invalid status",`${path}.status`);}else{record(value,`${path}.status`);exactKeys(value,new Set(["state","phase","node","started_at","ended_at","wait_deadline","outcome","wait_role","wait_actor_identity","wait_started_at","permitted_actions"]),`${path}.status`);for(const k of ["state","phase","outcome"])if(value[k]!=null&&!ENUM.test(value[k]))throw new DashboardContractError("invalid status enum",`${path}.status.${k}`);for(const k of ["started_at","ended_at","wait_deadline"])if(value[k]!=null)time(value[k],`${path}.status.${k}`);if(value.node!=null&&!ID.test(value.node))throw new DashboardContractError("invalid status node",`${path}.status.node`);const waitFields=Object.fromEntries(Object.entries(value).filter(([k])=>["wait_role","wait_actor_identity","wait_started_at","permitted_actions"].includes(k)));if(Object.keys(waitFields).length)validateData({schema_version:1,factory_id:"status-validation",...waitFields},null,`${path}.status`);if(Object.keys(waitFields).length){const expected={"awaiting-director":"director","awaiting-human":"human"}[value.phase];if(!expected||(value.wait_role!=null&&value.wait_role!==expected))throw new DashboardContractError("wait facts do not match observed phase",`${path}.status.phase`);}}}
    else if(key==="pinned"){record(value,`${path}.pinned`);exactKeys(value,new Set(["manifest_digest","package_digest","definition_digest","interpreter_build","publication_version","model_id","fixture_label","model_label"]),`${path}.pinned`);for(const[k,v]of Object.entries(value)){if(k.endsWith("digest")){if(!DIGEST.test(v))throw new DashboardContractError("invalid pin digest",`${path}.pinned.${k}`);}else checkText(v,`${path}.pinned.${k}`);}}
    else if(key==="permitted_actions"||key==="wait_role"){if(!eventData)validateData({schema_version:1,factory_id:"record-validation",[key]:value},null,path);}
    else if(key==="role")checkText(value,`${path}.role`);
    else if(key==="graph")validatePublicGraph(value,`${path}.graph`);
    else if(["assignments","artifacts","quality","decisions","commands","delivery","incidents","admissions"].includes(key)){if(!Array.isArray(value)||value.length>256)throw new DashboardContractError("expected bounded record array",`${path}.${key}`);value.forEach((x,i)=>validatePublicRecord(x,`${path}.${key}[${i}]`));}
    else if(key==="schema_version"||key==="factory_id"){}
    else if(["graph_nodes","service_bindings","finding_codes","evidence_refs"].includes(key)){
      if(!eventData)validateData({schema_version:1,factory_id:"record-validation",[key]:value},null,path);
    }
    else if(typeof value==="string"&&ID_FIELDS.has(key)){if(!ID.test(value))throw new DashboardContractError("invalid identifier",`${path}.${key}`);}
    else if(typeof value==="string"&&DIGEST_FIELDS.has(key)){if(!DIGEST.test(value))throw new DashboardContractError("invalid digest",`${path}.${key}`);}
    else if(typeof value==="string"&&ENUM_FIELDS.has(key)){if(!ENUM.test(value))throw new DashboardContractError("invalid enum",`${path}.${key}`);}
    else if(typeof value==="string"&&TIME_FIELDS.has(key))time(value,`${path}.${key}`);
    else if(typeof value==="string")checkText(value,`${path}.${key}`);
    else if(typeof value==="number"){if(!Number.isSafeInteger(value)||value<0)throw new DashboardContractError("expected non-negative integer",`${path}.${key}`);}
    else if(typeof value==="boolean"){}
    else if(value!=null)throw new DashboardContractError("nested value is not permitted here",`${path}.${key}`);
  }
}
function validateCommercialUsage(row,path){
  record(row,path);
  const allowed=new Set(["run_id","assignment_id","attempt_id","usage_id","unit","quantity","measurement_source","completeness","evidence_status","model_id","model_call_id","service_identity","reasoning_effort"]);
  exactKeys(row,allowed,path);
  for(const key of ["run_id","assignment_id","attempt_id","usage_id","unit","completeness","evidence_status"])if(!(key in row))throw new DashboardContractError("required usage fact missing",`${path}.${key}`);
  for(const key of ["run_id","assignment_id","attempt_id","usage_id","model_call_id","service_identity"])if(row[key]!=null&&(!ID.test(row[key])))throw new DashboardContractError("invalid usage identifier",`${path}.${key}`);
  if(!COMPLETENESS.has(row.completeness))throw new DashboardContractError("invalid usage completeness",`${path}.completeness`);
  if(!EVIDENCE_STATUS.has(row.evidence_status))throw new DashboardContractError("invalid usage evidence_status",`${path}.evidence_status`);
  for(const key of ["unit","measurement_source","reasoning_effort"])if(row[key]!=null&&!ENUM.test(row[key]))throw new DashboardContractError("invalid usage enum",`${path}.${key}`);
  if(row.model_id!=null&&!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(row.model_id))throw new DashboardContractError("invalid model id",`${path}.model_id`);
  if(!("quantity" in row)&&!UNAVAILABLE_COMPLETENESS.has(row.completeness))throw new DashboardContractError("quantity may be omitted only when completeness is unknown or undisclosed",`${path}.quantity`);
  if("quantity" in row){if(row.completeness!=="complete")throw new DashboardContractError("reported quantity requires complete completeness",`${path}.quantity`);if(typeof row.quantity!=="string"||!/^(0|[1-9][0-9]*)(\.[0-9]+)?$/.test(row.quantity))throw new DashboardContractError("invalid quantity",`${path}.quantity`);}
}
function validateCommercialObligation(row,path){
  record(row,path);
  const allowed=new Set(["run_id","assignment_id","attempt_id","obligation_id","component","offer_digest","amount_atoms","currency","atomic_scale","evidence_status","price_basis","payment_trigger","markup_bps","state"]);
  exactKeys(row,allowed,path);
  for(const key of ["run_id","assignment_id","attempt_id","obligation_id","component","amount_atoms","currency","atomic_scale","evidence_status","state"])if(!(key in row))throw new DashboardContractError("required obligation fact missing",`${path}.${key}`);
  for(const key of ["run_id","assignment_id","attempt_id","obligation_id"])if(!ID.test(row[key]))throw new DashboardContractError("invalid obligation identifier",`${path}.${key}`);
  if(!OBLIGATION_COMPONENTS.has(row.component))throw new DashboardContractError("unsupported obligation component",`${path}.component`);
  if(row.amount_atoms===null){if(!UNAVAILABLE_COMPLETENESS.has(row.evidence_status))throw new DashboardContractError("null amount_atoms requires unknown or undisclosed evidence",`${path}.amount_atoms`);}
  else if(!Number.isSafeInteger(row.amount_atoms))throw new DashboardContractError("amount_atoms must be an integer or permitted null",`${path}.amount_atoms`);
  if(typeof row.currency!=="string"||!/^[A-Z0-9][A-Z0-9._-]{0,15}$/.test(row.currency))throw new DashboardContractError("explicit currency is required",`${path}.currency`);
  if(!Number.isSafeInteger(row.atomic_scale)||row.atomic_scale<0)throw new DashboardContractError("explicit atomic_scale is required",`${path}.atomic_scale`);
  for(const key of ["state"])if(typeof row[key]!=="string"||!ENUM.test(row[key]))throw new DashboardContractError("invalid obligation status",`${path}.${key}`);
  if(!EVIDENCE_STATUS.has(row.evidence_status))throw new DashboardContractError("invalid obligation evidence_status",`${path}.evidence_status`);
  if(row.offer_digest!=null&&!DIGEST.test(row.offer_digest))throw new DashboardContractError("invalid offer digest",`${path}.offer_digest`);
  for(const key of ["price_basis","payment_trigger"])if(row[key]!=null&&!ENUM.test(row[key]))throw new DashboardContractError("invalid obligation enum",`${path}.${key}`);
  if(row.markup_bps!=null&&(!Number.isSafeInteger(row.markup_bps)||row.markup_bps<0))throw new DashboardContractError("invalid markup basis points",`${path}.markup_bps`);
}
export function validateSnapshot(snapshot, options = {}) {
  record(snapshot, "$snapshot");
  if (Object.keys(snapshot).some(k => !SNAPSHOT_KEYS.has(k)) || [...SNAPSHOT_KEYS].some(k => !(k in snapshot))) throw new DashboardContractError("invalid snapshot fields", "$snapshot");
  if (snapshot.schema_version !== 1) throw new DashboardContractError("schema_version must be 1", "$snapshot.schema_version");
  string(snapshot.cursor, "$snapshot.cursor"); if(!CURSOR.test(snapshot.cursor))throw new DashboardContractError("invalid opaque cursor format","$snapshot.cursor");
  time(snapshot.captured_at, "$snapshot.captured_at"); record(snapshot.freshness, "$snapshot.freshness");
  const freshness=snapshot.freshness, freshnessStates=["fresh","stale","disconnected","unknown"];
  if(Object.keys(freshness).some(key=>!["status","observed_at","scope","run_id","included_run_ids","factory_status","unavailable_run_ids"].includes(key))||!freshnessStates.includes(freshness.status))throw new DashboardContractError("invalid freshness","$snapshot.freshness");
  if(freshness.scope!==undefined&&!["factory","run"].includes(freshness.scope))throw new DashboardContractError("invalid freshness scope","$snapshot.freshness.scope");
  if(freshness.factory_status!==undefined&&!freshnessStates.includes(freshness.factory_status))throw new DashboardContractError("invalid factory freshness","$snapshot.freshness.factory_status");
  for(const key of ["included_run_ids","unavailable_run_ids"])if(freshness[key]!==undefined){const rows=freshness[key];if(!Array.isArray(rows)||rows.length>256||new Set(rows).size!==rows.length||rows.some(id=>typeof id!=="string"||!ID.test(id)))throw new DashboardContractError("invalid freshness run IDs",`$snapshot.freshness.${key}`);}
  if(freshness.scope==="run"){
    if(typeof freshness.run_id!=="string"||!ID.test(freshness.run_id)||!freshness.included_run_ids?.includes(freshness.run_id)||!freshnessStates.includes(freshness.factory_status))throw new DashboardContractError("invalid run freshness binding","$snapshot.freshness");
    if(freshness.status==="fresh"&&freshness.included_run_ids.some(id=>freshness.unavailable_run_ids?.includes(id)))throw new DashboardContractError("fresh scope includes unavailable run","$snapshot.freshness");
  }else if(freshness.run_id!==undefined||freshness.included_run_ids!==undefined||freshness.factory_status!==undefined)throw new DashboardContractError("run freshness fields require run scope","$snapshot.freshness");
  if (snapshot.freshness.observed_at !== null) time(snapshot.freshness.observed_at, "$snapshot.freshness.observed_at");
  record(snapshot.state, "$snapshot.state");
  if (Object.hasOwn(snapshot.state, "demo")) {
    if (!demoSource(options)) throw new DashboardContractError("illustrative Demo fields are not permitted for this source", "$snapshot.state.demo");
    validateDemoPresentation(snapshot.state.demo, "$snapshot.state.demo");
  }
  if (Object.keys(snapshot.state).some(k => !STATE_KEYS.has(k) && k !== "demo") || [...STATE_KEYS].some(k => !(k in snapshot.state))) throw new DashboardContractError("invalid projection state fields", "$snapshot.state");
  const state=snapshot.state;record(state.factory,"$snapshot.state.factory");exactKeys(state.factory,new Set(["id","name","identity","capability","graph","agent_bindings","agents","source_label","model_label","fixture_label"]),"$snapshot.state.factory");
  string(state.factory.id,"$snapshot.state.factory.id");if(!ID.test(state.factory.id))throw new DashboardContractError("invalid factory id","$snapshot.state.factory.id");checkText(state.factory.name,"$snapshot.state.factory.name");if(!state.factory.graph)throw new DashboardContractError("factory graph is required","$snapshot.state.factory.graph");validatePublicGraph(state.factory.graph,"$snapshot.state.factory.graph");
  for(const k of ["identity","capability"])if(state.factory[k]!=null&&!ID.test(state.factory[k]))throw new DashboardContractError("invalid factory identity",`$snapshot.state.factory.${k}`);
  for(const k of ["source_label","model_label","fixture_label"])if(state.factory[k]!=null)checkText(state.factory[k],`$snapshot.state.factory.${k}`);
  if(state.factory.agent_bindings!=null){if(!Array.isArray(state.factory.agent_bindings))throw new DashboardContractError("agent_bindings must be an array","$snapshot.state.factory.agent_bindings");state.factory.agent_bindings.forEach((x,i)=>validatePublicRecord(x,`$snapshot.state.factory.agent_bindings[${i}]`));}
  if(state.factory.agents!=null){if(!Array.isArray(state.factory.agents))throw new DashboardContractError("agents must be an array","$snapshot.state.factory.agents");state.factory.agents.forEach((x,i)=>validatePublicRecord(x,`$snapshot.state.factory.agents[${i}]`));}
  validatePublicRecord(state.active_publication,"$snapshot.state.active_publication");validatePublicRecord(state.capacity,"$snapshot.state.capacity");
  if(!Array.isArray(state.runs)||state.runs.length>256)throw new DashboardContractError("runs must be an array","$snapshot.state.runs");
  const runIds=new Set();state.runs.forEach((run,i)=>{const p=`$snapshot.state.runs[${i}]`;record(run,p);exactKeys(run,new Set(["id","task","status","started_at","graph","pinned","assignments","artifacts","quality","decisions","commands","delivery","incidents","admissions","model_label","fixture_label"]),p);if(!ID.test(run.id??""))throw new DashboardContractError("invalid run id",`${p}.id`);if(runIds.has(run.id))throw new DashboardContractError("duplicate run id",`${p}.id`);runIds.add(run.id);for(const k of ["task","status","pinned","assignments","artifacts","quality","decisions","commands","delivery","incidents","admissions"])if(!(k in run))throw new DashboardContractError("required run field missing",`${p}.${k}`);if(!run.task||!ID.test(run.task.id??"")||!ID.test(run.task.context_id??""))throw new DashboardContractError("task identity and context are required",`${p}.task`);if(run.status==null)throw new DashboardContractError("status is required",`${p}.status`);if(run.started_at!=null)time(run.started_at,`${p}.started_at`);if(run.graph!=null)validatePublicGraph(run.graph,`${p}.graph`);for(const k of ["task","status","pinned"])validatePublicRecord({[k]:run[k]},p);for(const k of ["assignments","artifacts","quality","decisions","commands","delivery","incidents","admissions"]){if(!Array.isArray(run[k])||run[k].length>256)throw new DashboardContractError("expected bounded run collection",`${p}.${k}`);run[k].forEach((row,j)=>validatePublicRecord(row,`${p}.${k}[${j}]`));}});
  if(freshness.scope==="run"&&(!runIds.has(freshness.run_id)||[...runIds].some(id=>!freshness.included_run_ids.includes(id))))throw new DashboardContractError("run freshness does not bind snapshot rows","$snapshot.freshness");
  record(state.commercial, "$snapshot.state.commercial");exactKeys(state.commercial,new Set(["usage","obligations","payments"]),"$snapshot.state.commercial");
  const obligationIds=new Set();
  for (const k of ["usage", "obligations", "payments"]) if (!Array.isArray(snapshot.state.commercial[k]) || snapshot.state.commercial[k].length > 256) throw new DashboardContractError("expected bounded record array", `$snapshot.state.commercial.${k}`); else snapshot.state.commercial[k].forEach((row,i)=>{
    const p=`$snapshot.state.commercial.${k}[${i}]`;
    if(k==="usage")validateCommercialUsage(row,p);else if(k==="obligations"){
      validateCommercialObligation(row,p);if(obligationIds.has(row.obligation_id))throw new DashboardContractError("obligation_id must be distinct per component",`${p}.obligation_id`);obligationIds.add(row.obligation_id);
    }else {validatePublicRecord(row,p);if(row?.evidence_status!=null&&!EVIDENCE_STATUS.has(row.evidence_status))throw new DashboardContractError("invalid payment evidence_status",`${p}.evidence_status`);}
  });
  sensitiveScan(snapshot); return snapshot;
}

export function validateDiscovery(discovery) {
  record(discovery,"$discovery");exactKeys(discovery,new Set(["schema_version","factories"]),"$discovery");
  if(discovery.schema_version!==1||!Array.isArray(discovery.factories))throw new DashboardContractError("invalid discovery response","$discovery");
  discovery.factories.forEach((factory,i)=>{const p=`$discovery.factories[${i}]`;record(factory,p);exactKeys(factory,new Set(["id","name","factory_id"]),p);for(const k of ["id","factory_id"])if(factory[k]!=null&&!ID.test(factory[k]))throw new DashboardContractError("invalid factory id",`${p}.${k}`);checkText(factory.name,`${p}.name`);});
  return discovery;
}

export function validateServerMessage(frame, options = {}) {
  record(frame, "$frame");
  switch (frame.op) {
    case "snapshot": exactKeys(frame,new Set(["op","snapshot"]),"$frame");validateSnapshot(frame.snapshot, options); break;
    case "resumed": exactKeys(frame,new Set(["op","after_cursor","continuation_cursor"]),"$frame");string(frame.after_cursor, "$frame.after_cursor"); string(frame.continuation_cursor, "$frame.continuation_cursor"); if(!CURSOR.test(frame.after_cursor)||!CURSOR.test(frame.continuation_cursor))throw new DashboardContractError("invalid opaque cursor format","$frame"); break;
    case "event": exactKeys(frame,new Set(["op","cursor","event"]),"$frame");string(frame.cursor, "$frame.cursor"); if(!CURSOR.test(frame.cursor))throw new DashboardContractError("invalid opaque cursor format","$frame.cursor"); validateCloudEvent(frame.event, options); break;
    case "checkpoint": exactKeys(frame,new Set(["op","cursor"]),"$frame");string(frame.cursor, "$frame.cursor"); if(!CURSOR.test(frame.cursor))throw new DashboardContractError("invalid opaque cursor format","$frame.cursor"); break;
    case "resync_required": exactKeys(frame,new Set(["op","reason","minimum_cursor","latest_cursor","message"]),"$frame");if (!["retention_expired", "slow_consumer"].includes(frame.reason)) throw new DashboardContractError("invalid resync reason", "$frame.reason"); for (const k of ["minimum_cursor", "latest_cursor"]) if (frame[k] != null && !CURSOR.test(frame[k])) throw new DashboardContractError("invalid opaque cursor format", `$frame.${k}`);if(frame.message!=null)checkText(frame.message,"$frame.message",128);break;
    case "command_ack": exactKeys(frame,new Set(["op","command_id","lifecycle"]),"$frame");string(frame.command_id, "$frame.command_id");if(!ID.test(frame.command_id))throw new DashboardContractError("invalid command id","$frame.command_id"); if (frame.lifecycle !== "received") throw new DashboardContractError("command ack must mean received", "$frame.lifecycle"); break;
    case "error": exactKeys(frame,new Set(["op","code","message"]),"$frame");if(!["invalid_message","unsupported_operation","command_unavailable","invalid_command","not_authorized","invalid_request","observation_unavailable"].includes(frame.code))throw new DashboardContractError("unsupported error code","$frame.code");checkText(frame.message,"$frame.message",160); break;
    default: throw new DashboardContractError("unsupported v1 server operation", "$frame.op");
  }
  return frame;
}

export function validateClientMessage(message) {
  record(message, "$message");
  if (message.op === "subscribe") {
    if (Object.keys(message).some(k => !["op", "factory_id", "run_id", "after_cursor"].includes(k))) throw new DashboardContractError("unexpected subscribe field", "$message");
    string(message.factory_id, "$message.factory_id"); if (!ID.test(message.factory_id)) throw new DashboardContractError("invalid factory id", "$message.factory_id");
    if (message.run_id != null && !ID.test(message.run_id)) throw new DashboardContractError("invalid run id", "$message.run_id");
    if (message.after_cursor != null && !CURSOR.test(message.after_cursor)) throw new DashboardContractError("invalid opaque cursor format", "$message.after_cursor");
    return message;
  }
  if (message.op === "command") {
    const allowed = ["op", "factory_id", "command_id", "task_id", "context_id", "action", "expected_state", "expected_revision", "expected_sha256"];
    if (Object.keys(message).some(k => !allowed.includes(k))) throw new DashboardContractError("unexpected command field", "$message");
    for (const k of ["factory_id", "command_id", "task_id", "context_id"]) { string(message[k], `$message.${k}`); if (!ID.test(message[k])) throw new DashboardContractError("invalid identifier", `$message.${k}`); }
    for (const k of ["action", "expected_state"]) { string(message[k], `$message.${k}`); if (!ENUM.test(message[k])) throw new DashboardContractError("invalid command enum", `$message.${k}`); }
    if (message.expected_revision != null && !ID.test(message.expected_revision)) throw new DashboardContractError("invalid revision", "$message.expected_revision");
    if (message.expected_sha256 != null && !DIGEST.test(message.expected_sha256)) throw new DashboardContractError("invalid digest", "$message.expected_sha256");
    if (!("expected_state" in message)) throw new DashboardContractError("expected_state is required", "$message.expected_state");
    return message;
  }
  throw new DashboardContractError("unsupported client operation", "$message.op");
}

export function validateBundle(bundle) {
  record(bundle, "$bundle");exactKeys(bundle,new Set(["schema_version","source","snapshot","frames","provenance"]),"$bundle"); if (bundle.schema_version !== 1 || !["recorded", "demo"].includes(bundle.source)) throw new DashboardContractError("unsupported bundle", "$bundle");
  const options = { source:bundle.source };
  validateSnapshot(bundle.snapshot, options); if (!Array.isArray(bundle.frames)) throw new DashboardContractError("frames must be an array", "$bundle.frames");
  if (bundle.source === "recorded") { record(bundle.provenance, "$bundle.provenance");exactKeys(bundle.provenance,new Set(["label","model_label","fixture_label","release_label","defect_labels","completeness","evidence_ref"]),"$bundle.provenance"); string(bundle.provenance.label, "$bundle.provenance.label"); for(const k of ["model_label","fixture_label","release_label","completeness","evidence_ref"])if(bundle.provenance[k]!=null)checkText(bundle.provenance[k],`$bundle.provenance.${k}`,256);if(bundle.provenance.defect_labels!=null&&(!Array.isArray(bundle.provenance.defect_labels)||bundle.provenance.defect_labels.some(x=>typeof x!=="string"||x.length>256)))throw new DashboardContractError("invalid defect labels","$bundle.provenance.defect_labels"); }
  for (let i=0;i<bundle.frames.length;i++) validateServerMessage(bundle.frames[i], options);
  sensitiveScan(bundle); return bundle;
}

export async function sha256Hex(bytes) {
  const input = typeof bytes === "string" ? new TextEncoder().encode(bytes) : bytes;
  if (!(input instanceof ArrayBuffer) && !ArrayBuffer.isView(input)) throw new DashboardContractError("artifact bytes must be a string or byte buffer", "$artifact.bytes");
  const buffer = input instanceof ArrayBuffer ? input : input.buffer.slice(input.byteOffset, input.byteOffset + input.byteLength);
  const digest = await globalThis.crypto.subtle.digest("SHA-256", buffer);
  return [...new Uint8Array(digest)].map(b => b.toString(16).padStart(2, "0")).join("");
}
export async function verifyArtifactBytes(bytes, expectedDigest) {
  string(expectedDigest, "$artifact.sha256"); const expected = expectedDigest.replace(/^sha256:/i, "").toLowerCase();
  if (!DIGEST.test(expected)) throw new DashboardContractError("expected a SHA-256 hex digest", "$artifact.sha256");
  const actual = await sha256Hex(bytes); return { valid: actual === expected, expected, actual };
}
export function makeCloudEvent({ id, type, subject, data, time = new Date().toISOString(), source, factory_id }, options = {}) {
  const event = { specversion:"1.0", id, source:source ?? `/factories/${encodeURIComponent(factory_id)}`, type, time, subject, datacontenttype:"application/json", dataschema:DASHBOARD_EVENT_SCHEMA, data };
  return validateCloudEvent(event, options);
}


// Current submission checks are a separate Runtime Interface from historical run freshness.
export function validateSubmissionReadiness(value, factoryId) {
  const path='$submissionReadiness';
  const keys=['schema_version','factory_id','observed_at','status','reason_code'];
  const reasons=['director_profile_unapproved','default_broker_path_overridden','subscription_status_unavailable','pinned_writable_model_owners_unavailable','pinned_worker_pollers_unavailable','factory_busy','factory_uncertain','unfinished_runs','current_state_unavailable'];
  if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).length!==keys.length||Object.keys(value).some(k=>!keys.includes(k)))throw new DashboardContractError('invalid readiness fields',path);
  if(value.schema_version!==1||!ID.test(value.factory_id)||value.factory_id!==factoryId)throw new DashboardContractError('readiness factory mismatch',path);
  time(value.observed_at,path+'.observed_at');
  if(!['ready','blocked'].includes(value.status)||(value.status==='ready'?value.reason_code!==null:!reasons.includes(value.reason_code)))throw new DashboardContractError('invalid readiness status',path);
  return structuredClone(value);
}
