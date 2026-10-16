import test from "node:test";
import assert from "node:assert/strict";
import { readDeliveries, recentLocalDeliveries, saveLocalDelivery, validateDeliveryReceipt, validateDeliveryResponse } from "../delivery.mjs";

const digest = "a".repeat(64);
const receipt = (overrides = {}) => ({
  factory_id: "factory-1",
  run_id: "run-1",
  task_id: "task-1",
  context_id: "context-1",
  artifact_revision: "revision-1",
  artifact_sha256: digest,
  markdown_sha256: "b".repeat(64),
  destination_identity: "destination-local",
  receipt_id: `delivery-${"c".repeat(64)}`,
  state: "delivered",
  delivery_kind: "local_file",
  byte_length: 1639,
  recorded_at: "2026-10-03T12:00:00Z",
  ...overrides,
});

test("validates only the local receipt fields exposed by GET /deliveries", () => {
  const row = validateDeliveryReceipt(receipt(), { factoryId: "factory-1", runId: "run-1" });
  assert.equal(row.delivery_kind, "local_file");
  assert.equal(row.state, "delivered");
  const response = validateDeliveryResponse({ status: "available", receipts: [row] }, { factoryId: "factory-1" });
  assert.equal(response.receipts[0].byte_length, 1639);
  assert.equal(Object.hasOwn(response.receipts[0], "destination_path"), false);
});

test("rejects path, destination, content, cost, and unknown receipt data", () => {
  for (const key of ["destination_path", "destination", "content", "markdown", "amount_atoms", "cost", "payment_network", "prompt"]) {
    assert.throws(() => validateDeliveryReceipt(receipt({ [key]: "must not pass" })), /field is not permitted/);
  }
  assert.throws(() => validateDeliveryReceipt(receipt({ artifact_sha256: "bad" })), /SHA-256/);
  assert.throws(() => validateDeliveryReceipt(receipt({ state: "accepted" })), /actual local delivery state/);
  assert.throws(() => validateDeliveryReceipt(receipt({ delivery_kind: "remote_customer" })), /delivery kind/);
  assert.throws(() => validateDeliveryReceipt(receipt({ byte_length: -1 })), /byte length/);
  assert.throws(() => validateDeliveryReceipt(receipt(), { factoryId: "other-factory" }), /another factory/);
  assert.throws(() => validateDeliveryResponse({ status: "available", receipts: [receipt(), receipt()] }), /duplicate receipt_id/);
});

test("validates configured and unavailable route response variants without inventing a receipt", () => {
  assert.deepEqual(validateDeliveryResponse({ status: "unavailable", reason: "local destination not configured", receipts: [] }), {
    status: "unavailable", reason: "local destination not configured", receipts: [],
  });
  assert.deepEqual(validateDeliveryResponse({ status: "unavailable", reason: "delivered bytes or owner changed", receipts: [] }).receipts, []);
  assert.throws(() => validateDeliveryResponse({ status: "unavailable", reason: "customer delivery failed", receipts: [] }), /unavailable reason/);
  assert.throws(() => validateDeliveryResponse({ status: "unavailable", reason: "local destination not configured", receipts: [receipt()] }), /must not claim receipts/);
  assert.throws(() => validateDeliveryResponse({ status: "available", reason: "anything", receipts: [] }), /cannot carry/);
});

test("reader requires an authorized same-origin bootstrap route and rejects caller paths", async () => {
  let calls = 0;
  const fetcher = async () => { calls++; throw new Error("fetch should not run"); };
  await assert.rejects(readDeliveries({ endpoint: "/deliveries", fetcher }), /Unauthenticated/);
  await assert.rejects(readDeliveries({ authenticated: true, fetcher }), /not configured/);
  await assert.rejects(readDeliveries({ endpoint: "https://other.invalid/deliveries", authenticated: true, fetcher, baseUrl: "https://factory.example/" }), /same-origin/);
  await assert.rejects(readDeliveries({ endpoint: "/deliveries", authenticated: true, destination: "/tmp/customer", fetcher, baseUrl: "https://factory.example/" }), /field is not permitted/);
  for (const endpoint of ["/deliveries?destination=/tmp/x", "/deliveries/other", "/delivery", "https://user:secret@factory.example/deliveries"]) {
    await assert.rejects(readDeliveries({ endpoint, authenticated: true, fetcher, baseUrl: "https://factory.example/" }), /endpoint must be/);
  }
  await assert.rejects(readDeliveries({ endpoint: "/deliveries", authenticated: true, runId: "", fetcher, baseUrl: "https://factory.example/" }), /invalid identifier/);
  assert.equal(calls, 0);
});

test("reader performs one same-origin GET with an optional run filter and no client authority", async () => {
  const calls = [];
  const responseBody = { status: "available", receipts: [receipt()] };
  const result = await readDeliveries({
    endpoint: "/deliveries", authenticated: true, factoryId: "factory-1", runId: "run-1", baseUrl: "https://factory.example/app",
    fetcher: async (url, options) => { calls.push({ url, options }); return { ok: true, status: 200, json: async () => responseBody }; },
  });
  assert.equal(result.receipts[0].receipt_id, receipt().receipt_id);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].options.method, "GET");
  assert.equal(calls[0].options.credentials, "same-origin");
  assert.deepEqual(calls[0].options.headers, { Accept: "application/json" });
  const url = new URL(calls[0].url);
  assert.equal(url.origin, "https://factory.example");
  assert.equal(url.pathname, "/deliveries");
  assert.equal(url.searchParams.get("run_id"), "run-1");
  assert.deepEqual([...url.searchParams.keys()], ["run_id"]);
  assert.doesNotMatch(calls[0].url, /token|secret|destination|path/i);
});

test("reader surfaces unauthorized and foreign-run responses without trusting their bodies", async () => {
  for (const status of [401, 403]) {
    await assert.rejects(readDeliveries({ endpoint: "/deliveries", authenticated: true, baseUrl: "https://factory.example/", fetcher: async () => ({ ok: false, status, json: async () => ({ receipts: [receipt()] }) }) }), /session is unauthorized/);
  }
  await assert.rejects(readDeliveries({ endpoint: "/deliveries", authenticated: true, runId: "run-1", baseUrl: "https://factory.example/", fetcher: async () => ({ ok: false, status: 404, json: async () => ({ receipts: [receipt()] }) }) }), /outside the observed factory/);
});

const observationReceipt = (receiptId, deliveredAt, overrides = {}) => ({
  factory_id:"factory-1",run_id:"run-1",task_id:"task-1",context_id:"context-1",
  receipt_id:receiptId,artifact_revision:"revision-1",artifact_sha256:digest,
  markdown_sha256:"b".repeat(64),destination_id:"destination-1",destination_identity:"destination-local",
  delivery_kind:"local_file",outcome:"local-file-delivered",byte_length:1639,delivered_at:deliveredAt,
  ...overrides,
});

test("recent local delivery summary filters by receipt time and returns ten 30-second buckets", () => {
  const nowMs=Date.parse("2026-10-03T12:05:00Z"),start=nowMs-300_000;
  const result=recentLocalDeliveries([
    observationReceipt("receipt-window-start",new Date(start).toISOString()),
    observationReceipt("receipt-middle",new Date(start+4*30_000).toISOString()),
    observationReceipt("receipt-now",new Date(nowMs).toISOString()),
    observationReceipt("receipt-old",new Date(start-1).toISOString()),
    observationReceipt("receipt-future",new Date(nowMs+1).toISOString()),
    observationReceipt("fixture-only","not-a-time",{delivery_kind:"local_file",outcome:"fixture-received"}),
  ],nowMs);
  assert.equal(result.total,3);
  assert.equal(result.buckets.length,10);
  assert.equal(result.bucketMs,30_000);
  assert.equal(result.windowStartMs,start);
  assert.equal(result.windowEndMs,nowMs);
  assert.deepEqual(result.buckets.map(row=>row.count),[1,0,0,0,1,0,0,0,0,1]);
});

test("recent local delivery summary deduplicates identical receipts and rejects invalid or conflicting bindings", () => {
  const nowMs=Date.parse("2026-10-03T12:05:00Z");
  const row=observationReceipt("receipt-duplicate","2026-10-03T12:04:45Z");
  assert.equal(recentLocalDeliveries([row,{...row}],nowMs).total,1);
  assert.throws(()=>recentLocalDeliveries([row,{...row,artifact_sha256:"c".repeat(64)}],nowMs),/conflicting duplicate receipt binding/);
  assert.throws(()=>recentLocalDeliveries([observationReceipt("receipt-invalid-time","yesterday")],nowMs),/invalid delivered_at timestamp/);
});

const saveReceipt = (overrides = {}) => receipt({ artifact_revision:"revision-1", artifact_sha256:digest, ...overrides });
const saveOptions = (overrides = {}) => ({
  endpoint:"/deliveries",runId:"run-1",revision:"revision-1",sha256:digest,
  factoryId:"factory-1",authenticated:true,baseUrl:"https://factory.example/app",...overrides,
});

test("local delivery writer posts exact artifact binding with same-origin cookie auth and validates duplicate response", async () => {
  const calls=[];
  const body={receipt:{...saveReceipt(),duplicate:true}};
  const result=await saveLocalDelivery(saveOptions({fetcher:async(url,options)=>{calls.push({url,options});return {ok:true,status:200,json:async()=>body};}}));
  assert.equal(result.duplicate,true);
  assert.deepEqual(result.receipt,saveReceipt());
  assert.equal(calls.length,1);
  assert.equal(calls[0].url,"https://factory.example/deliveries");
  assert.equal(calls[0].options.method,"POST");
  assert.equal(calls[0].options.credentials,"same-origin");
  assert.deepEqual(calls[0].options.headers,{Accept:"application/json","Content-Type":"application/json"});
  assert.deepEqual(JSON.parse(calls[0].options.body),{run_id:"run-1",revision:"revision-1",sha256:digest});
  assert.equal(Object.hasOwn(calls[0].options.headers,"Authorization"),false);
});

test("local delivery writer rejects unauthenticated, cross-origin, caller-directed, and invalid receipt requests", async () => {
  let calls=0;
  const fetcher=async()=>{calls++;return {ok:true,status:200,json:async()=>({receipt:{...saveReceipt(),duplicate:false}})};};
  await assert.rejects(saveLocalDelivery(saveOptions({authenticated:false,fetcher})),/Unauthenticated/);
  await assert.rejects(saveLocalDelivery(saveOptions({endpoint:"https://other.invalid/deliveries",fetcher})),/same-origin/);
  await assert.rejects(saveLocalDelivery({...saveOptions(),destination:"/tmp/customer",fetcher}),/field is not permitted/);
  assert.equal(calls,0);

  for(const status of [401,403]) await assert.rejects(saveLocalDelivery(saveOptions({fetcher:async()=>({ok:false,status,json:async()=>({})})})),/session is unauthorized/);
  await assert.rejects(saveLocalDelivery(saveOptions({fetcher:async()=>({ok:true,status:200,json:async()=>({receipt:{...saveReceipt({artifact_sha256:"c".repeat(64)}),duplicate:false}})})})),/does not match requested artifact/);
  await assert.rejects(saveLocalDelivery(saveOptions({fetcher:async()=>({ok:true,status:200,json:async()=>({receipt:{...saveReceipt(),duplicate:"yes"}})})})),/duplicate must be boolean/);
  await assert.rejects(saveLocalDelivery(saveOptions({fetcher:async()=>({ok:true,status:200,json:async()=>({receipt:{...saveReceipt(),duplicate:false},duplicate:false})})})),/field is not permitted/);
  await assert.rejects(saveLocalDelivery(saveOptions({fetcher:async()=>({ok:true,status:200,json:async()=>({receipt:{...saveReceipt(),duplicate:false,remote_customer:true}})})})),/field is not permitted/);
  await assert.rejects(saveLocalDelivery(saveOptions({fetcher:async()=>({ok:false,status:409,json:async()=>({})})})),/conflicts with current/);
});
