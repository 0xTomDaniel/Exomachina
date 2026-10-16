const ID = /^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}$/;
const DIGEST = /^[0-9a-f]{64}$/;
const RECEIPT_KEYS = new Set([
  "factory_id", "run_id", "task_id", "context_id", "artifact_revision",
  "artifact_sha256", "markdown_sha256", "destination_identity", "receipt_id",
  "state", "delivery_kind", "byte_length", "recorded_at",
]);
const RESPONSE_KEYS = new Set(["receipts", "status", "reason"]);
const READER_OPTIONS = new Set(["endpoint", "runId", "factoryId", "authenticated", "fetcher", "baseUrl"]);
const SAVE_OPTIONS = new Set(["endpoint", "runId", "revision", "sha256", "factoryId", "authenticated", "fetcher", "baseUrl"]);
const UNAVAILABLE_REASONS = new Set(["local destination not configured", "delivered bytes or owner changed"]);
const LOCAL_DELIVERY_WINDOW_MS = 5 * 60 * 1000;
const LOCAL_DELIVERY_BUCKET_MS = 30 * 1000;
const LOCAL_RECEIPT_BINDING_FIELDS = [
  "factory_id", "run_id", "task_id", "context_id", "assignment_id", "attempt_id",
  "receipt_id", "artifact_revision", "artifact_sha256", "markdown_sha256",
  "destination_id", "destination_identity", "delivery_kind", "outcome", "byte_length", "delivered_at",
];

function fail(message, path) {
  const error = new TypeError(`${path}: ${message}`);
  error.name = "DeliveryReceiptError";
  error.path = path;
  throw error;
}

function record(value, path) {
  if (!value || typeof value !== "object" || Array.isArray(value) || (Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null)) fail("expected a plain object", path);
  for (const key of Reflect.ownKeys(value)) {
    const descriptor = typeof key === "string" ? Object.getOwnPropertyDescriptor(value, key) : null;
    if (!descriptor || !("value" in descriptor)) fail("symbol or accessor fields are not permitted", `${path}.${String(key)}`);
  }
  return value;
}

function exactKeys(value, allowed, required, path) {
  for (const key of Object.keys(value)) if (!allowed.has(key)) fail("field is not permitted", `${path}.${key}`);
  for (const key of required) if (!Object.hasOwn(value, key)) fail("required field missing", `${path}.${key}`);
}

function identifier(value, path) {
  if (typeof value !== "string" || !ID.test(value)) fail("invalid identifier", path);
}

function digest(value, path) {
  if (typeof value !== "string" || !DIGEST.test(value)) fail("invalid SHA-256 digest", path);
}

function timestamp(value, path) {
  if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(value) || !Number.isFinite(Date.parse(value))) fail("invalid recorded_at timestamp", path);
}

export function validateDeliveryReceipt(receipt, { factoryId, runId } = {}) {
  const path = "$receipt";
  record(receipt, path);
  exactKeys(receipt, RECEIPT_KEYS, [...RECEIPT_KEYS], path);
  for (const key of ["factory_id", "run_id", "task_id", "context_id", "artifact_revision", "destination_identity", "receipt_id"]) identifier(receipt[key], `${path}.${key}`);
  if (factoryId != null && receipt.factory_id !== factoryId) fail("receipt belongs to another factory", `${path}.factory_id`);
  if (runId != null && receipt.run_id !== runId) fail("receipt does not match requested run", `${path}.run_id`);
  digest(receipt.artifact_sha256, `${path}.artifact_sha256`);
  digest(receipt.markdown_sha256, `${path}.markdown_sha256`);
  if (receipt.state !== "delivered") fail("only actual local delivery state is accepted", `${path}.state`);
  if (receipt.delivery_kind !== "local_file") fail("unsupported delivery kind", `${path}.delivery_kind`);
  if (!Number.isSafeInteger(receipt.byte_length) || receipt.byte_length < 0) fail("expected non-negative byte length", `${path}.byte_length`);
  timestamp(receipt.recorded_at, `${path}.recorded_at`);
  return structuredClone(receipt);
}

export function validateDeliveryResponse(body, { factoryId, runId } = {}) {
  const path = "$response";
  record(body, path);
  exactKeys(body, RESPONSE_KEYS, ["receipts", "status"], path);
  if (body.status !== "available" && body.status !== "unavailable") fail("invalid delivery status", `${path}.status`);
  if (!Array.isArray(body.receipts) || body.receipts.length > 256) fail("expected a bounded receipt array", `${path}.receipts`);
  if (body.status === "available" && Object.hasOwn(body, "reason")) fail("available response cannot carry an unavailable reason", `${path}.reason`);
  if (body.status === "unavailable") {
    if (!UNAVAILABLE_REASONS.has(body.reason)) fail("invalid unavailable reason", `${path}.reason`);
    if (body.receipts.length) fail("unavailable response must not claim receipts", `${path}.receipts`);
  }
  const receipts = body.receipts.map(row => validateDeliveryReceipt(row, { factoryId, runId }));
  const ids = new Set();
  for (const receipt of receipts) {
    if (ids.has(receipt.receipt_id)) fail("duplicate receipt_id", `${path}.receipts`);
    ids.add(receipt.receipt_id);
  }
  return { receipts, status: body.status, ...(Object.hasOwn(body, "reason") ? { reason: body.reason } : {}) };
}

/** Read only the authenticated same-origin GET /deliveries route. */
export async function readDeliveries(options = {}) {
  record(options, "$request");
  exactKeys(options, READER_OPTIONS, [], "$request");
  const { endpoint, runId, factoryId, authenticated = false, fetcher = globalThis.fetch, baseUrl = globalThis.location?.href ?? "http://localhost/" } = options;
  if (!authenticated) throw new Error("Unauthenticated: delivery receipts require an authorized same-origin session.");
  if (typeof endpoint !== "string" || !endpoint) throw new Error("delivery receipt endpoint is not configured by server bootstrap");
  if (typeof fetcher !== "function") throw new Error("delivery receipt reader is unavailable");
  if (runId != null) identifier(runId, "$request.run_id");
  if (factoryId != null) identifier(factoryId, "$request.factory_id");

  let base, url;
  try {
    base = new URL(baseUrl);
    url = new URL(endpoint, base);
  } catch {
    throw new Error("delivery receipt endpoint is invalid");
  }
  if (url.origin !== base.origin) throw new Error("delivery receipt reader must be same-origin");
  if (url.pathname !== "/deliveries" || url.username || url.password || url.search || url.hash) throw new Error("delivery receipt endpoint must be the unparameterized /deliveries route");
  if (runId != null) url.searchParams.set("run_id", runId);

  const response = await fetcher(url.toString(), { method: "GET", credentials: "same-origin", headers: { Accept: "application/json" } });
  if (response.status === 401 || response.status === 403) throw new Error("Unauthenticated: the delivery receipt session is unauthorized.");
  if (response.status === 404) throw new Error("delivery receipt run is outside the observed factory");
  if (!response.ok && response.status !== 409) throw new Error(`delivery receipt request failed (${response.status})`);
  let body;
  try { body = await response.json(); }
  catch { throw new Error("delivery receipt response is not valid JSON"); }
  return validateDeliveryResponse(body, { factoryId, runId });
}

/** Save one exact artifact through the authenticated same-origin local delivery route. */
export async function saveLocalDelivery(options = {}) {
  record(options, "$request");
  exactKeys(options, SAVE_OPTIONS, ["endpoint", "runId", "revision", "sha256", "authenticated"], "$request");
  const { endpoint, runId, revision, sha256, factoryId, authenticated = false, fetcher = globalThis.fetch, baseUrl = globalThis.location?.href ?? "http://localhost/" } = options;
  if (!authenticated) throw new Error("Unauthenticated: local delivery requires an authorized same-origin session.");
  if (typeof endpoint !== "string" || !endpoint) throw new Error("local delivery endpoint is not configured by server bootstrap");
  if (typeof fetcher !== "function") throw new Error("local delivery writer is unavailable");
  identifier(runId, "$request.run_id");
  identifier(revision, "$request.revision");
  digest(sha256, "$request.sha256");
  if (factoryId != null) identifier(factoryId, "$request.factory_id");

  let base, url;
  try {
    base = new URL(baseUrl);
    url = new URL(endpoint, base);
  } catch {
    throw new Error("local delivery endpoint is invalid");
  }
  if (url.origin !== base.origin) throw new Error("local delivery endpoint must be same-origin");
  if (url.pathname !== "/deliveries" || url.username || url.password || url.search || url.hash) throw new Error("local delivery endpoint must be the unparameterized /deliveries route");

  const response = await fetcher(url.toString(), {
    method:"POST",
    credentials:"same-origin",
    headers:{ Accept:"application/json", "Content-Type":"application/json" },
    body:JSON.stringify({run_id:runId,revision,sha256}),
  });
  if (response.status === 401 || response.status === 403) throw new Error("Unauthenticated: the local delivery session is unauthorized.");
  if (response.status === 404) throw new Error("local delivery run or artifact is outside the observed factory");
  if (response.status === 409) throw new Error("local delivery request conflicts with current run or artifact state");
  if (!response.ok) throw new Error(`local delivery request failed (${response.status})`);
  let body;
  try { body = await response.json(); }
  catch { throw new Error("local delivery response is not valid JSON"); }
  record(body, "$response");
  exactKeys(body, new Set(["receipt"]), ["receipt"], "$response");
  record(body.receipt, "$response.receipt");
  exactKeys(body.receipt, new Set([...RECEIPT_KEYS, "duplicate"]), [...RECEIPT_KEYS, "duplicate"], "$response.receipt");
  if (typeof body.receipt.duplicate !== "boolean") fail("duplicate must be boolean", "$response.receipt.duplicate");
  const { duplicate, ...receiptFields } = body.receipt;
  const receipt = validateDeliveryReceipt(receiptFields, { factoryId, runId });
  if (receipt.artifact_revision !== revision || receipt.artifact_sha256 !== sha256) fail("receipt does not match requested artifact", "$response.receipt");
  return { receipt, duplicate };
}

/**
 * Count validated local-file delivery receipts over the trailing five minutes.
 * Buckets are chronological, 30-second windows relative to nowMs; the first
 * includes the window start and the final bucket includes nowMs.
 */
export function recentLocalDeliveries(rows, nowMs) {
  if (!Array.isArray(rows)) fail("expected receipt rows", "$rows");
  if (!Number.isSafeInteger(nowMs)) fail("expected safe integer epoch milliseconds", "$nowMs");

  const windowStartMs = nowMs - LOCAL_DELIVERY_WINDOW_MS;
  const counts = Array(LOCAL_DELIVERY_WINDOW_MS / LOCAL_DELIVERY_BUCKET_MS).fill(0);
  const seen = new Map();
  for (let index = 0; index < rows.length; index++) {
    const path = `$rows[${index}]`;
    const row = record(rows[index], path);
    if (row.delivery_kind !== "local_file" || row.outcome !== "local-file-delivered") continue;
    identifier(row.receipt_id, `${path}.receipt_id`);
    if (typeof row.delivered_at !== "string" || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(row.delivered_at) || !Number.isFinite(Date.parse(row.delivered_at))) {
      fail("invalid delivered_at timestamp", `${path}.delivered_at`);
    }
    const deliveredMs = Date.parse(row.delivered_at);
    const binding = JSON.stringify(LOCAL_RECEIPT_BINDING_FIELDS.map(key => [key, Object.hasOwn(row, key) ? row[key] : null]));
    const previous = seen.get(row.receipt_id);
    if (previous) {
      if (previous.binding !== binding) fail("conflicting duplicate receipt binding", `${path}.receipt_id`);
      continue;
    }
    seen.set(row.receipt_id, { binding, deliveredMs });
    if (deliveredMs < windowStartMs || deliveredMs > nowMs) continue;
    const bucket = Math.min(counts.length - 1, Math.floor((deliveredMs - windowStartMs) / LOCAL_DELIVERY_BUCKET_MS));
    counts[bucket]++;
  }

  return {
    total: counts.reduce((sum, count) => sum + count, 0),
    windowStartMs,
    windowEndMs: nowMs,
    bucketMs: LOCAL_DELIVERY_BUCKET_MS,
    buckets: counts.map((count, index) => ({
      startMs: windowStartMs + index * LOCAL_DELIVERY_BUCKET_MS,
      endMs: windowStartMs + (index + 1) * LOCAL_DELIVERY_BUCKET_MS,
      count,
    })),
  };
}
