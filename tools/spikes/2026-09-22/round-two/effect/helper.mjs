// One fixed Effect workflow interprets every approved, pinned factory document.
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import { ClusterWorkflowEngine, SingleRunner } from '@effect/cluster';
import { SqliteClient } from '@effect/sql-sqlite-node';
import { Activity, DurableDeferred, Workflow } from '@effect/workflow';
import { Effect, Layer, ManagedRuntime, Schema } from 'effect';

const root = process.env.EFFECT_ROUND2_ROOT;
if (!root) throw new Error('EFFECT_ROUND2_ROOT is required');
const port = Number(process.env.EFFECT_ROUND2_PORT ?? '19872');
const catalog = path.join(root, 'catalog');
const runs = path.join(root, 'runs');
const events = path.join(root, 'events.jsonl');
fs.mkdirSync(catalog, { recursive: true });
fs.mkdirSync(runs, { recursive: true });

const sha256 = raw => crypto.createHash('sha256').update(raw).digest('hex');
const problem = (status, message) => Object.assign(new Error(message), { status });
const requiredString = value => typeof value === 'string' && value.length > 0;

function validate(doc) {
  if (!doc || doc.name !== 'verified-research' || ![1, 2].includes(doc.version)) {
    throw problem(400, 'unsupported factory header');
  }
  const steps = doc.steps;
  const expected = doc.version === 1
    ? ['join', 'review', 'director_wait', 'deliver']
    : ['join', 'verify', 'review', 'director_wait', 'deliver'];
  if (!Array.isArray(steps) || steps.length !== expected.length + 2 ||
      new Set(steps.slice(0, 2)).size !== 2 ||
      !steps.slice(0, 2).every(step => ['research_a', 'research_b'].includes(step)) ||
      JSON.stringify(steps.slice(2)) !== JSON.stringify(expected)) {
    throw problem(400, 'unsupported steps or review bypass');
  }
  if (!doc.capabilities ||
      !requiredString(doc.capabilities.research_a) ||
      !requiredString(doc.capabilities.research_b)) {
    throw problem(400, 'missing capability references');
  }
  const keys = Object.keys(doc);
  if (keys.length !== 4 || !keys.every(key => ['name', 'version', 'capabilities', 'steps'].includes(key)) ||
      Object.keys(doc.capabilities).length !== 2) {
    throw problem(400, 'unexpected definition fields');
  }
  return doc;
}

function publish(doc) {
  validate(doc);
  const raw = JSON.stringify(doc);
  const digest = sha256(raw);
  const file = path.join(catalog, `${digest}.json`);
  try { fs.writeFileSync(file, raw, { flag: 'wx' }); }
  catch (error) {
    if (error.code !== 'EEXIST' || fs.readFileSync(file, 'utf8') !== raw) throw error;
  }
  return { digest, version: doc.version };
}

function published(digest) {
  if (!/^[a-f0-9]{64}$/.test(digest ?? '')) throw problem(400, 'invalid digest');
  let raw;
  try { raw = fs.readFileSync(path.join(catalog, `${digest}.json`), 'utf8'); }
  catch (error) {
    if (error.code === 'ENOENT') throw problem(404, 'definition not published');
    throw error;
  }
  if (sha256(raw) !== digest) throw problem(409, 'published definition changed');
  validate(JSON.parse(raw));
  return raw;
}

function runFile(id) {
  if (!/^[a-zA-Z0-9_-]{1,80}$/.test(id ?? '')) throw problem(400, 'invalid run ID');
  return path.join(runs, `${id}.json`);
}

function bindRun(id, digest, docJson) {
  const file = runFile(id);
  const binding = JSON.stringify({ id, digest, docJson });
  try { fs.writeFileSync(file, binding, { flag: 'wx' }); }
  catch (error) {
    if (error.code !== 'EEXIST') throw error;
    if (fs.readFileSync(file, 'utf8') !== binding) throw problem(409, 'run already bound to another definition');
  }
  return { id, digest, docJson };
}

function boundRun(id, digest) {
  let binding;
  try { binding = JSON.parse(fs.readFileSync(runFile(id), 'utf8')); }
  catch (error) {
    if (error.code === 'ENOENT') throw problem(404, 'run not found');
    throw error;
  }
  if (digest !== undefined && binding.digest !== digest) throw problem(409, 'run digest mismatch');
  return binding;
}

function record(id, digest, node, value, ref) {
  return Effect.sync(() => {
    fs.appendFileSync(events, JSON.stringify({ id, digest, node, value, ref, pid: process.pid }) + '\n');
    return value;
  });
}

const Approval = DurableDeferred.make('director-decision', { success: Schema.String });
const FactoryRun = Workflow.make({
  name: 'FactoryRun',
  payload: { id: Schema.String, digest: Schema.String, docJson: Schema.String },
  idempotencyKey: ({ id }) => id,
  success: Schema.String,
});
const activity = (id, digest, node, value, ref) => Activity.make({
  name: node,
  success: Schema.String,
  execute: record(id, digest, node, value, ref),
});
const FactoryLive = FactoryRun.toLayer(({ id, digest, docJson }) => Effect.gen(function* () {
  if (sha256(docJson) !== digest) throw new Error('workflow payload digest mismatch');
  const doc = validate(JSON.parse(docJson));
  const first = doc.steps.slice(0, 2);
  const produced = yield* Effect.all(first.map(node =>
    activity(id, digest, node, `${id}:${doc.capabilities[node]}`, doc.capabilities[node])
  ), { concurrency: 2 });
  const joined = yield* activity(id, digest, 'join', produced.join('|'));
  if (doc.steps.includes('verify')) yield* activity(id, digest, 'verify', `verified:${joined}`);
  yield* activity(id, digest, 'review', `reviewed:${joined}`);
  yield* activity(id, digest, 'director_wait', 'waiting');
  const decision = yield* DurableDeferred.await(Approval);
  if (decision !== 'accepted') throw new Error('unapproved decision');
  yield* activity(id, digest, 'deliver', `accepted:${joined}`);
  return JSON.stringify({ id, digest, version: doc.version, capabilities: doc.capabilities, steps: doc.steps });
}));

const Sqlite = SqliteClient.layer({ filename: path.join(root, 'state.sqlite') });
const Runner = SingleRunner.layer({ runnerStorage: 'sql' }).pipe(Layer.provideMerge(Sqlite));
const Engine = ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(Runner));
const runtime = ManagedRuntime.make(FactoryLive.pipe(Layer.provideMerge(Engine)));

async function command(route, body) {
  if (route === '/health') return { ready: true, pid: process.pid };
  if (route === '/publish') return publish(body);
  if (route === '/start') {
    const docJson = published(body.digest);
    const payload = bindRun(body.id, body.digest, docJson);
    const executionId = await runtime.runPromise(FactoryRun.execute(payload, { discard: true }));
    return { started: true, id: body.id, digest: body.digest, executionId };
  }
  if (route === '/poll') {
    const payload = boundRun(body.id);
    const executionId = await runtime.runPromise(FactoryRun.executionId(payload));
    const result = await runtime.runPromise(FactoryRun.poll(executionId));
    return result?._tag === 'Complete'
      ? { state: 'Complete', exit: result.exit._tag,
          value: result.exit._tag === 'Success' ? JSON.parse(result.exit.value) : undefined }
      : { state: result?._tag ?? 'Pending' };
  }
  if (route === '/approve') {
    const payload = boundRun(body.id, body.digest);
    const token = await runtime.runPromise(DurableDeferred.tokenFromPayload(Approval, { workflow: FactoryRun, payload }));
    await runtime.runPromise(DurableDeferred.succeed(Approval, { token, value: 'accepted' }));
    return { approved: true, id: body.id };
  }
  throw problem(404, 'unknown route');
}

await runtime.runtime();
http.createServer(async (request, response) => {
  try {
    let raw = '';
    for await (const chunk of request) {
      raw += chunk;
      if (raw.length > 65536) throw problem(413, 'request too large');
    }
    const body = raw ? JSON.parse(raw) : {};
    const result = await command(request.url, body);
    response.writeHead(200, { 'content-type': 'application/json' });
    response.end(JSON.stringify(result));
  } catch (error) {
    response.writeHead(error.status ?? 500, { 'content-type': 'application/json' });
    response.end(JSON.stringify({ error: error.message ?? String(error) }));
  }
}).listen(port, '127.0.0.1', () => {
  console.log(JSON.stringify({ event: 'ready', pid: process.pid, port }));
});
