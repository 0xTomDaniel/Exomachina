import test from "node:test";
import assert from "node:assert/strict";
import { measurementGroups, validateMeasurements, validateMeasurementsResponse } from "../usage.mjs";

const categories = ["input_tokens", "output_tokens", "cache_read_tokens", "cache_write_tokens", "total_tokens"];
const completeUsage = Object.fromEntries(categories.map((name, index) => [name, { status: "reported", value: index === 0 ? 0 : index * 10 }]));
const unavailableUsage = Object.fromEntries(categories.map(name => [name, { status: "unavailable", value: null }]));
const partialUsage = { ...completeUsage, cache_read_tokens: { status: "unavailable", value: null } };

function measurement(overrides = {}) {
  return {
    measurement_id: "measurement-1",
    model_call_id: "call-1",
    recorded_at: "2026-10-03T10:00:00Z",
    call_scope: "assignment_call",
    provider: "codex",
    model_id: "gpt-6-luna",
    reasoning_effort: "xhigh",
    unit: "tokens",
    measurement_source: "provider_reported",
    completeness: "complete",
    evidence_status: "provider_reported",
    usage: structuredClone(completeUsage),
    service_identity: "service-1",
    task_id: "task-1",
    message_id: "message-1",
    run_id: "run-1",
    definition_digest: "a".repeat(64),
    assignment_id: "assignment-1",
    attempt_id: "attempt-1",
    ...overrides,
  };
}

function coverage(overrides = {}) {
  return {
    status: "partial",
    factory_id: "factory-1",
    scoped_run_count: 1,
    bound_sources_status: "available",
    authoring_bound: { status: "available", queries_failed: 0, rows_rejected: 0, conflicts: 0 },
    authoring_unbound: { status: "unavailable", reason: "shared_model_home_factory_exclusivity_not_proven" },
    pinned_services: { status: "fixture_only", availability: "available", authentication: "fixture_bearer_only", pinned_owner_count: 1, owners_responded: 1, owner_or_request_failures: 0, rows_rejected: 0, conflicts: 0 },
    director: { status: "unavailable", reason: "no_public_list_measurements_accessor" },
    director_unbound: { status: "unavailable", reason: "pre_task_or_unbound_director_rows_have_no_authoritative_factory_run" },
    commercial_costs: "not_included",
    ...overrides,
  };
}

test("Runtime response validator accepts the exact MeasurementView and bounded coverage shape", () => {
  const response = validateMeasurementsResponse({ measurements: [measurement()], coverage: coverage() });
  assert.equal(response.measurements.length, 1);
  assert.equal(response.coverage.pinned_services.authentication, "fixture_bearer_only");
  assert.equal(response.coverage.authoring_unbound.reason, "shared_model_home_factory_exclusivity_not_proven");
  assert.equal(response.coverage.director_unbound.reason, "pre_task_or_unbound_director_rows_have_no_authoritative_factory_run");
  assert.equal(response.coverage.commercial_costs, "not_included");
});

test("accepts current sanitized aggregate coverage counters and rejects malformed or unknown counters", () => {
  const base=coverage();
  const actualShape={
    ...base,scoped_run_count:2,
    pinned_services:{...base.pinned_services,pinned_owner_count:8,owners_responded:8,non_usage_service_count:2,owner_resolution_failures:0,request_failures:0},
    director:{status:"available",queries_failed:0,rows_rejected:0,conflicts:0},
  };
  const response=validateMeasurementsResponse({measurements:[],coverage:actualShape});
  assert.deepEqual(response.coverage.pinned_services,actualShape.pinned_services);

  for(const key of ["non_usage_service_count","owner_resolution_failures","request_failures"]){
    for(const value of [-1,1.5,"1",1_000_001]){
      const malformed={...actualShape,pinned_services:{...actualShape.pinned_services,[key]:value}};
      assert.throws(()=>validateMeasurementsResponse({measurements:[],coverage:malformed}),new RegExp(key));
    }
  }
  const unknown={...actualShape,pinned_services:{...actualShape.pinned_services,customer_cost:0}};
  assert.throws(()=>validateMeasurementsResponse({measurements:[],coverage:unknown}),/field is not permitted/);
});

test("groups by observed service, call scope, run, assignment, and attempt", () => {
  const rows = [
    measurement(),
    measurement({ measurement_id: "measurement-2", model_call_id: "call-2" }),
    measurement({ measurement_id: "measurement-3", call_scope: "director_call" }),
    measurement({ measurement_id: "measurement-4", attempt_id: "attempt-2" }),
    measurement({ measurement_id: "measurement-5", service_identity: "service-2" }),
  ];
  const groups = measurementGroups(rows);
  assert.equal(groups.length, 4);
  const assignmentCall = groups.find(group => group.call_scope === "assignment_call" && group.service_identity === "service-1" && group.attempt_id === "attempt-1");
  assert.ok(assignmentCall);
  assert.deepEqual(assignmentCall.measurements.map(row => row.measurement_id), ["measurement-1", "measurement-2"]);
  assert.equal(assignmentCall.groupable, true);
  assert.equal(groups.some(group => group.call_scope === "director_call"), true);
  assert.equal(groups.some(group => group.attempt_id === "attempt-2"), true);
  assert.equal(groups.some(group => group.service_identity === "service-2"), true);
});

test("missing binding remains singleton; zero is reported and null unavailable values stay unknown", () => {
  const rows = [
    measurement(),
    measurement({ measurement_id: "measurement-unknown", model_call_id: "call-unknown", usage: structuredClone(unavailableUsage), completeness: "unknown", evidence_status: "unknown", measurement_source: "unknown", service_identity: null, assignment_id: null, attempt_id: null }),
    measurement({ measurement_id: "measurement-unbound-2", model_call_id: "call-unbound-2", service_identity: null, assignment_id: null, attempt_id: null }),
  ];
  const groups = measurementGroups(rows);
  const zero = groups.flatMap(group => group.measurements).find(row => row.measurement_id === "measurement-1");
  const unknown = groups.flatMap(group => group.measurements).find(row => row.measurement_id === "measurement-unknown");
  assert.equal(zero.usage.input_tokens.value, 0);
  assert.equal(zero.usage.input_tokens.status, "reported");
  assert.equal(unknown.usage.input_tokens.value, null);
  assert.equal(unknown.usage.input_tokens.status, "unavailable");
  assert.equal(groups.filter(group => !group.groupable).length, 2);
  assert.notEqual(groups.find(group => group.measurements[0].measurement_id === "measurement-unknown").key, groups.find(group => group.measurements[0].measurement_id === "measurement-unbound-2").key);
  assert.equal(groups.some(group => Object.hasOwn(group, "costs")), false);
  assert.equal(groups.flatMap(group => group.measurements).some(row => Object.hasOwn(row, "amount_atoms")), false);
});

test("coverage branches validate exact bounded nested fields and explicit cost absence", () => {
  const newUnavailable = validateMeasurementsResponse({
    measurements: [],
    coverage: { status: "unavailable", reason: "factory observation source unavailable", authoring_unbound: "unavailable", director: "unavailable_observation_scope_unavailable", director_unbound: "unavailable_observation_scope_unavailable" },
    error: "measurement scope unavailable",
  });
  assert.equal(newUnavailable.coverage.director_unbound, "unavailable_observation_scope_unavailable");
  const unavailable = validateMeasurementsResponse({
    measurements: [],
    coverage: { status: "unavailable", reason: "factory observation source unavailable", authoring_unbound: "unavailable", director: "unavailable_no_public_list_measurements_accessor" },
    error: "measurement scope unavailable",
  });
  assert.equal(unavailable.coverage.status, "unavailable");
  assert.equal(unavailable.error, "measurement scope unavailable");
  const directorCoverage = coverage({ director: { status: "available", queries_failed: 0, rows_rejected: 1, conflicts: 0 } });
  assert.equal(validateMeasurementsResponse({ measurements: [], coverage: directorCoverage }).coverage.director.rows_rejected, 1);
  assert.equal(validateMeasurementsResponse({ measurements: [], coverage: coverage({ director: { status: "partial", queries_failed: 1, rows_rejected: 0, conflicts: 0 } }) }).coverage.director.status, "partial");
  const { director_unbound: _legacyField, ...legacyCoverage } = coverage();
  assert.equal(validateMeasurementsResponse({ measurements: [], coverage: legacyCoverage }).coverage.director.status, "unavailable");
  assert.throws(() => validateMeasurementsResponse({ measurements: [], coverage: { ...coverage(), payment_network: "unconfigured" } }), /not permitted/);
  assert.throws(() => validateMeasurementsResponse({ measurements: [], coverage: { ...coverage(), pinned_services: { ...coverage().pinned_services, arbitrary: 1 } } }), /not permitted/);
  assert.throws(() => validateMeasurementsResponse({ measurements: [], coverage: coverage({ director: { status: "available", queries_failed: 0, rows_rejected: 0, conflicts: 0, arbitrary: 1 } }) }), /not permitted/);
  assert.throws(() => validateMeasurementsResponse({ measurements: [], coverage: coverage({ director_unbound: { status: "unavailable", reason: "unknown" } }) }), /invalid unbound Director/);
  assert.throws(() => validateMeasurementsResponse({ measurements: [], coverage: { ...coverage(), commercial_costs: "estimated" } }), /explicitly absent/);
});

test("measurement schema rejects sensitive and financial fields and preserves null/status semantics", () => {
  for (const key of ["prompt", "content", "raw_history", "amount_atoms", "cost", "private_annotation"]) assert.throws(() => validateMeasurements([measurement({ [key]: "must not pass" })]), /not permitted/);
  assert.throws(() => validateMeasurements([measurement({ usage: { ...completeUsage, input_tokens: { status: "reported", value: null } } })]), /reported token value/);
  assert.throws(() => validateMeasurements([measurement({ usage: { ...completeUsage, input_tokens: { status: "unavailable", value: 0 } } })]), /must remain null/);
  assert.throws(() => validateMeasurements([measurement({ usage: partialUsage })]), /does not match reported category coverage/);
  const validPartial = measurement({ usage: structuredClone(partialUsage), completeness: "partial" });
  assert.equal(validateMeasurements([validPartial])[0].usage.cache_read_tokens.status, "unavailable");
  assert.throws(() => validateMeasurementsResponse({ measurements: [], coverage: coverage(), access_token: "secret" }), /not permitted/);
});

test("identical measurement IDs are idempotent; conflicting facts are rejected", () => {
  const row = measurement();
  assert.equal(validateMeasurements([row, structuredClone(row)]).length, 1);
  assert.throws(() => validateMeasurements([row, measurement({ ...row, usage: { ...completeUsage, total_tokens: { status: "reported", value: 999 } } })]), /conflicts/);
});
