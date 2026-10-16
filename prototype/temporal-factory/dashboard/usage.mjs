const ID = /^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}$/;
const DIGEST = /^[0-9a-f]{64}$/;
const SAFE_LABEL = /^[A-Za-z0-9][A-Za-z0-9._:/@_+-]{0,127}$/;
const CALL_SCOPES = new Set(["authoring_overhead", "director_call", "assignment_call"]);
const COMPLETENESS = new Set(["complete", "partial", "unknown"]);
// agent_reported: an A2A agent's budget-extension report, recorded by the factory.
const EVIDENCE = new Set(["provider_reported", "agent_reported", "unknown"]);
const CATEGORY_NAMES = ["input_tokens", "output_tokens", "cache_read_tokens", "cache_write_tokens", "total_tokens"];
const MEASUREMENT_KEYS = new Set([
  "measurement_id", "model_call_id", "recorded_at", "call_scope", "provider", "model_id",
  "reasoning_effort", "unit", "measurement_source", "completeness", "evidence_status", "usage",
  "service_identity", "task_id", "message_id", "run_id", "definition_digest", "assignment_id", "attempt_id",
]);
// The factory cannot see an agent's model, provider, or individual model calls;
// those stay visible only for the factory's own Director and authoring calls.
const AGENT_HIDDEN_KEYS = ["provider", "model_id", "model_call_id", "reasoning_effort"];
const COVERAGE_STATUSES = new Set(["partial", "unavailable"]);
const COVERAGE_MAIN_KEYS_LEGACY = ["status", "factory_id", "scoped_run_count", "bound_sources_status", "authoring_bound", "authoring_unbound", "pinned_services", "director", "commercial_costs"];
const COVERAGE_MAIN_KEYS = [...COVERAGE_MAIN_KEYS_LEGACY, "director_unbound"];
const AUTHORING_BOUND_KEYS = ["status", "queries_failed", "rows_rejected", "conflicts"];
const PINNED_SERVICE_COUNTERS = ["pinned_owner_count", "owner_resolution_failures", "queries_failed", "non_usage_service_count", "rows_rejected", "conflicts"];
const AUTHORING_UNBOUND_REASON = "shared_model_home_factory_exclusivity_not_proven";
const DIRECTOR_REASON = "no_public_list_measurements_accessor";

function fail(message, path) {
  const error = new TypeError(`${path}: ${message}`);
  error.name = "MeasurementViewError";
  error.path = path;
  throw error;
}

function plainRecord(value, path) {
  if (!value || typeof value !== "object" || Array.isArray(value) || (Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null)) fail("expected a plain object", path);
  for (const key of Reflect.ownKeys(value)) {
    const descriptor = typeof key === "string" ? Object.getOwnPropertyDescriptor(value, key) : null;
    if (!descriptor || !("value" in descriptor)) fail("symbol or accessor fields are not permitted", `${path}.${String(key)}`);
  }
  return value;
}

function exactKeys(value, allowed, path, required = []) {
  const allow = new Set(allowed);
  for (const key of Object.keys(value)) if (!allow.has(key)) fail("field is not permitted", `${path}.${key}`);
  for (const key of required) if (!Object.hasOwn(value, key)) fail("required field missing", `${path}.${key}`);
}

function safeId(value, path) {
  if (typeof value !== "string" || !ID.test(value)) fail("invalid identifier", path);
}

function boundedCount(value, path) {
  if (!Number.isSafeInteger(value) || value < 0 || value > 1_000_000) fail("expected bounded non-negative count", path);
}

function validateTime(value, path) {
  if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(value) || !Number.isFinite(Date.parse(value))) fail("invalid date-time", path);
}

function validateMeasurement(row, index) {
  const path = `$measurements[${index}]`;
  plainRecord(row, path);
  // An already-projected agent row (see measurementView) carries the label instead of model identity.
  const agentView = Object.hasOwn(row, "usage_label");
  if (agentView && (row.usage_label !== "agent-reported" || row.evidence_status !== "agent_reported")) fail("usage_label is only the agent-reported label", `${path}.usage_label`);
  const hidden = agentView ? new Set(AGENT_HIDDEN_KEYS) : new Set();
  const required = ["measurement_id", "model_call_id", "recorded_at", "call_scope", "provider", "model_id", "unit", "measurement_source", "completeness", "evidence_status", "usage"].filter(key => !hidden.has(key));
  exactKeys(row, agentView ? [...[...MEASUREMENT_KEYS].filter(key => !hidden.has(key)), "usage_label"] : MEASUREMENT_KEYS, path, required);
  for (const key of ["measurement_id", "model_call_id"].filter(key => !hidden.has(key))) safeId(row[key], `${path}.${key}`);
  validateTime(row.recorded_at, `${path}.recorded_at`);
  if (!CALL_SCOPES.has(row.call_scope)) fail("unsupported call_scope", `${path}.call_scope`);
  for (const key of ["provider", "model_id", "unit"].filter(key => !hidden.has(key))) if (typeof row[key] !== "string" || !SAFE_LABEL.test(row[key])) fail("invalid safe label", `${path}.${key}`);
  if (row.unit !== "tokens") fail("only token measurements are supported", `${path}.unit`);
  if (row.reasoning_effort != null && (typeof row.reasoning_effort !== "string" || !SAFE_LABEL.test(row.reasoning_effort))) fail("invalid reasoning_effort", `${path}.reasoning_effort`);
  for (const key of ["service_identity", "task_id", "message_id", "run_id", "assignment_id", "attempt_id"]) {
    if (Object.hasOwn(row, key) && row[key] !== null) safeId(row[key], `${path}.${key}`);
  }
  if (Object.hasOwn(row, "definition_digest") && row.definition_digest !== null && (typeof row.definition_digest !== "string" || !DIGEST.test(row.definition_digest))) fail("invalid definition_digest", `${path}.definition_digest`);
  if (!EVIDENCE.has(row.measurement_source)) fail("invalid measurement_source", `${path}.measurement_source`);
  if (!COMPLETENESS.has(row.completeness)) fail("invalid completeness", `${path}.completeness`);
  if (!EVIDENCE.has(row.evidence_status)) fail("invalid evidence_status", `${path}.evidence_status`);

  plainRecord(row.usage, `${path}.usage`);
  exactKeys(row.usage, CATEGORY_NAMES, `${path}.usage`, CATEGORY_NAMES);
  let reported = 0;
  for (const category of CATEGORY_NAMES) {
    const cellPath = `${path}.usage.${category}`;
    const cell = plainRecord(row.usage[category], cellPath);
    exactKeys(cell, ["value", "status"], cellPath, ["value", "status"]);
    if (cell.status === "reported") {
      if (!Number.isSafeInteger(cell.value) || cell.value < 0) fail("reported token value must be a non-negative integer", `${cellPath}.value`);
      reported++;
    } else if (cell.status === "unavailable") {
      if (cell.value !== null) fail("unavailable token value must remain null", `${cellPath}.value`);
    } else fail("status must be reported or unavailable", `${cellPath}.status`);
  }
  const completeness = reported === CATEGORY_NAMES.length ? "complete" : reported ? "partial" : "unknown";
  if (row.completeness !== completeness) fail("does not match reported category coverage", `${path}.completeness`);
  // Reported rows name who reported them; nothing reported is unknown, never zero.
  const evidenceOk = reported ? row.evidence_status !== "unknown" : row.evidence_status === "unknown";
  if (!evidenceOk || row.measurement_source !== row.evidence_status) fail("does not match reported category evidence", `${path}.evidence_status`);
  if (row.evidence_status === "agent_reported" && row.call_scope !== "assignment_call") fail("agent-reported usage is assignment usage only", `${path}.evidence_status`);
  return structuredClone(row);
}

function validateCounts(row, keys, path) {
  for (const key of keys) boundedCount(row[key], `${path}.${key}`);
}

function validateCoverage(coverage) {
  const path = "$response.coverage";
  plainRecord(coverage, path);
  if (coverage.status === "unavailable") {
    const current = Object.hasOwn(coverage, "director_unbound");
    const keys = current
      ? ["status", "reason", "authoring_unbound", "director", "director_unbound"]
      : ["status", "reason", "authoring_unbound", "director"];
    exactKeys(coverage, keys, path, keys);
    if (coverage.reason !== "factory observation source unavailable" || coverage.authoring_unbound !== "unavailable") fail("invalid unavailable coverage details", path);
    if (current) {
      if (coverage.director !== "unavailable_observation_scope_unavailable" || coverage.director_unbound !== "unavailable_observation_scope_unavailable") fail("invalid unavailable Director coverage details", path);
    } else if (coverage.director !== "unavailable_no_public_list_measurements_accessor") fail("invalid legacy unavailable Director coverage", `${path}.director`);
    return structuredClone(coverage);
  }
  const hasDirectorUnbound = Object.hasOwn(coverage, "director_unbound");
  const mainKeys = hasDirectorUnbound ? COVERAGE_MAIN_KEYS : COVERAGE_MAIN_KEYS_LEGACY;
  exactKeys(coverage, mainKeys, path, mainKeys);
  if (!COVERAGE_STATUSES.has(coverage.status) || coverage.status !== "partial") fail("invalid coverage status", `${path}.status`);
  safeId(coverage.factory_id, `${path}.factory_id`);
  boundedCount(coverage.scoped_run_count, `${path}.scoped_run_count`);
  if (!["available", "partial"].includes(coverage.bound_sources_status)) fail("invalid bound source status", `${path}.bound_sources_status`);

  const authoring = plainRecord(coverage.authoring_bound, `${path}.authoring_bound`);
  exactKeys(authoring, AUTHORING_BOUND_KEYS, `${path}.authoring_bound`, AUTHORING_BOUND_KEYS);
  if (!["available", "partial"].includes(authoring.status)) fail("invalid authoring status", `${path}.authoring_bound.status`);
  validateCounts(authoring, ["queries_failed", "rows_rejected", "conflicts"], `${path}.authoring_bound`);

  if (typeof coverage.authoring_unbound === "string") {
    if (coverage.authoring_unbound !== "unavailable") fail("invalid unbound authoring status", `${path}.authoring_unbound`);
  } else {
    const unbound = plainRecord(coverage.authoring_unbound, `${path}.authoring_unbound`);
    exactKeys(unbound, ["status", "reason"], `${path}.authoring_unbound`, ["status", "reason"]);
    if (unbound.status !== "unavailable" || unbound.reason !== AUTHORING_UNBOUND_REASON) fail("invalid unbound authoring details", `${path}.authoring_unbound`);
  }

  const pinned = plainRecord(coverage.pinned_services, `${path}.pinned_services`);
  // Agent services are never polled: pinned-service usage is what the factory
  // recorded from each agent Task's budget-extension report.
  exactKeys(pinned, ["status", "availability", "source", ...PINNED_SERVICE_COUNTERS], `${path}.pinned_services`, ["status", "availability", "source", ...PINNED_SERVICE_COUNTERS]);
  if (pinned.status !== "agent_reported" || !["available", "partial"].includes(pinned.availability) || pinned.source !== "factory_journal") fail("invalid pinned service status", `${path}.pinned_services`);
  validateCounts(pinned, PINNED_SERVICE_COUNTERS, `${path}.pinned_services`);

  if (typeof coverage.director === "string") {
    if (coverage.director !== "unavailable_no_public_list_measurements_accessor") fail("invalid director status", `${path}.director`);
  } else {
    const director = plainRecord(coverage.director, `${path}.director`);
    if (director.status === "available" || director.status === "partial") {
      exactKeys(director, ["status", "queries_failed", "rows_rejected", "conflicts"], `${path}.director`, ["status", "queries_failed", "rows_rejected", "conflicts"]);
      validateCounts(director, ["queries_failed", "rows_rejected", "conflicts"], `${path}.director`);
    } else {
      exactKeys(director, ["status", "reason"], `${path}.director`, ["status", "reason"]);
      if (director.status !== "unavailable" || director.reason !== DIRECTOR_REASON) fail("invalid director details", `${path}.director`);
    }
  }
  if (hasDirectorUnbound) {
    const unbound = plainRecord(coverage.director_unbound, `${path}.director_unbound`);
    exactKeys(unbound, ["status", "reason"], `${path}.director_unbound`, ["status", "reason"]);
    if (unbound.status !== "unavailable" || unbound.reason !== "pre_task_or_unbound_director_rows_have_no_authoritative_factory_run") fail("invalid unbound Director details", `${path}.director_unbound`);
  }
  if (coverage.commercial_costs !== "not_included") fail("commercial costs must remain explicitly absent", `${path}.commercial_costs`);
  return structuredClone(coverage);
}

/** Agent rows are labelled agent-reported and carry no model, provider, or model-call identity. */
function measurementView(row) {
  if (row.evidence_status !== "agent_reported") return row;
  const view = { ...row, usage_label: "agent-reported" };
  for (const key of AGENT_HIDDEN_KEYS) delete view[key];
  return view;
}

/** Validate the safe MeasurementView rows returned by Runtime /usage/measurements. */
export function validateMeasurements(rows) {
  if (!Array.isArray(rows) || rows.length > 256) fail("expected a bounded measurement array", "$measurements");
  const byId = new Map();
  rows.forEach((raw, index) => {
    const row = validateMeasurement(raw, index);
    const previous = byId.get(row.measurement_id);
    if (previous && JSON.stringify(previous) !== JSON.stringify(row)) fail("measurement_id conflicts with an earlier fact", `$measurements[${index}].measurement_id`);
    if (!previous) byId.set(row.measurement_id, row);
  });
  return [...byId.values()].map(measurementView);
}

/** Validate Runtime's envelope and its finite nested coverage metadata. */
export function validateMeasurementsResponse(body) {
  const path = "$response";
  plainRecord(body, path);
  const allowed = ["measurements", "coverage", "error"];
  exactKeys(body, allowed, path, ["measurements", "coverage"]);
  if (Object.hasOwn(body, "error") && !["measurement scope unavailable", "invalid measurement filter"].includes(body.error)) fail("invalid safe error", `${path}.error`);
  const measurements = validateMeasurements(body.measurements);
  const coverage = validateCoverage(body.coverage);
  if (coverage.status === "unavailable" && measurements.length) fail("unavailable coverage cannot contain measurements", `${path}.measurements`);
  return { ...(Object.hasOwn(body, "error") ? { error: body.error } : {}), measurements, coverage };
}

/** Group only rows with complete observed service/assignment/attempt bindings. */
export function measurementGroups(rows) {
  const validated = validateMeasurements(rows);
  const groups = new Map();
  for (const row of validated) {
    const bound = row.service_identity != null && row.assignment_id != null && row.attempt_id != null;
    const dimensions = bound
      ? ["bound", row.service_identity, row.call_scope, row.run_id ?? null, row.assignment_id, row.attempt_id]
      : ["unbound", row.measurement_id];
    const key = JSON.stringify(dimensions);
    let group = groups.get(key);
    if (!group) {
      group = {
        key,
        groupable: bound,
        service_identity: row.service_identity ?? null,
        call_scope: row.call_scope,
        run_id: row.run_id ?? null,
        assignment_id: row.assignment_id ?? null,
        attempt_id: row.attempt_id ?? null,
        measurements: [],
      };
      groups.set(key, group);
    }
    group.measurements.push(row);
  }
  return [...groups.values()];
}

export function toMeasurementView(rows) {
  return { groups: measurementGroups(rows) };
}
