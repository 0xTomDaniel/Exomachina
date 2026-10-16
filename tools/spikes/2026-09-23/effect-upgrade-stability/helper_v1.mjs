// Effect Workflow executes a pinned, validated graph document. Every block and
// the A2A subprocess bridge here are Exomachina-owned trial code.
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import { execFile } from 'node:child_process';
import Database from 'better-sqlite3';
import { ClusterWorkflowEngine, SingleRunner } from '@effect/cluster';
import { SqliteClient } from '@effect/sql-sqlite-node';
import { Activity, DurableDeferred, Workflow } from '@effect/workflow';
import { Effect, Layer, ManagedRuntime, Schema } from 'effect';

const root = process.env.EFFECT_PARITY_ROOT;
const approvedPath = process.env.EFFECT_PARITY_APPROVED;
if (!root || !approvedPath) throw new Error('EFFECT_PARITY_ROOT and EFFECT_PARITY_APPROVED required');
const port = Number(process.env.EFFECT_PARITY_PORT ?? '19873');
const python = process.env.EFFECT_PARITY_PYTHON ?? 'python3';
const here = path.dirname(new URL(import.meta.url).pathname);
fs.mkdirSync(root, { recursive: true });
const db = new Database(path.join(root, 'product.sqlite'));
db.pragma('journal_mode = WAL');
db.pragma('synchronous = FULL');
db.exec(`
 CREATE TABLE IF NOT EXISTS definitions (digest TEXT PRIMARY KEY, body TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS runs (
   id TEXT PRIMARY KEY, digest TEXT NOT NULL, package_digest TEXT NOT NULL,
   document_json TEXT NOT NULL, input_json TEXT NOT NULL,
   phase TEXT NOT NULL DEFAULT 'starting', node TEXT,
   repair_count INTEGER NOT NULL DEFAULT 0, revision TEXT, sha256 TEXT,
   artifact_json TEXT, verdict_json TEXT, acceptance_json TEXT,
   receipt_json TEXT, child_id TEXT, deadline_ms INTEGER,
   owner_epoch INTEGER NOT NULL DEFAULT 1, decision TEXT,
   decision_state TEXT);
 CREATE TABLE IF NOT EXISTS events (
   run_id TEXT NOT NULL, event_key TEXT NOT NULL, kind TEXT NOT NULL,
   value_json TEXT NOT NULL, created_ms INTEGER NOT NULL,
   PRIMARY KEY(run_id,event_key));
`);
const sha256 = value => crypto.createHash('sha256').update(value).digest('hex');
const canonical = value => JSON.stringify(value, (_, v) => v && typeof v === 'object' && !Array.isArray(v)
  ? Object.fromEntries(Object.entries(v).sort(([a], [b]) => a.localeCompare(b))) : v);
const problem = (status, message) => Object.assign(new Error(message), { status });
const validId = value => typeof value === 'string' && /^[A-Za-z0-9_:\-]{1,160}$/.test(value);
const validDigest = value => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value);
const approved = JSON.parse(fs.readFileSync(approvedPath, 'utf8'));

async function callBridge(op, data) {
  return await new Promise((resolve, reject) => {
    const child = execFile(python, [path.join(here, 'bridge.py'), op],
      { maxBuffer: 1024 * 1024, timeout: 90000 }, (error, stdout, stderr) => {
        if (error) reject(new Error(`${op}: ${stderr.trim() || error.message}`));
        else { try { resolve(JSON.parse(stdout)); } catch (parseError) { reject(parseError); } }
      });
    child.stdin.end(JSON.stringify(data));
  });
}

function row(id) {
  if (!validId(id)) throw problem(400, 'invalid run ID');
  const found = db.prepare('SELECT * FROM runs WHERE id=?').get(id);
  if (!found) throw problem(404, 'run not found');
  return found;
}
function bindRun(id, digest, packageDigest, document, input) {
  if (!validId(id) || !validDigest(digest) || !validDigest(packageDigest)) throw problem(400, 'invalid run binding');
  const docJson = canonical(document), inputJson = canonical(input);
  db.prepare(`INSERT OR IGNORE INTO runs
    (id,digest,package_digest,document_json,input_json,owner_epoch)
    VALUES (?,?,?,?,?,?)`).run(id, digest, packageDigest, docJson, inputJson, input.director.epoch);
  const current = row(id);
  if (current.digest !== digest || current.package_digest !== packageDigest ||
      current.document_json !== docJson || current.input_json !== inputJson)
    throw problem(409, 'run already bound to another immutable closure or input');
  return { id, digest, packageDigest, docJson, inputJson };
}
function packageAt(digest) {
  if (!validDigest(digest)) throw problem(400, 'invalid package digest');
  const item = db.prepare('SELECT body FROM definitions WHERE digest=?').get(digest);
  if (!item) throw problem(404, 'definition not published');
  if (sha256(item.body) !== digest) throw problem(409, 'definition bytes changed');
  return JSON.parse(item.body);
}
function event(id, key, kind, value) {
  db.prepare('INSERT OR IGNORE INTO events VALUES (?,?,?,?,?)')
    .run(id, key, kind, canonical(value), Date.now());
}
function phase(id, name, at, repairCount, extra = {}) {
  const set = Object.entries(extra);
  const columns = ['phase=?', 'node=?', 'repair_count=?', ...set.map(([key]) => `${key}=?`)];
  db.prepare(`UPDATE runs SET ${columns.join(',')} WHERE id=?`).run(
    name, at, repairCount, ...set.map(([, value]) => value), id);
}
const Decision = DurableDeferred.make('director-decision', { success: Schema.String });
const FactoryRun = Workflow.make({
  name: 'EffectParityFactoryRun',
  payload: { id: Schema.String, digest: Schema.String, packageDigest: Schema.String,
    docJson: Schema.String, inputJson: Schema.String },
  idempotencyKey: ({ id }) => id, success: Schema.String,
});
function stage(name, action, id, key, kind) {
  return Activity.make({ name, success: Schema.String,
    execute: Effect.promise(async () => {
      event(id, key + ':start', kind + '-start', { name });
      const value = await action();
      event(id, key, kind, value);
      return canonical(value);
    }) });
}
function parsed(effectValue) { return JSON.parse(effectValue); }

const FactoryLive = FactoryRun.toLayer(({ id, digest, packageDigest, docJson, inputJson }) => Effect.gen(function* () {
  const document = JSON.parse(docJson), input = JSON.parse(inputJson);
  if (sha256(docJson) !== digest) throw new Error('workflow document digest mismatch');
  const pkg = packageAt(packageDigest);
  const nodes = document.nodes;
  let at = document.start, branches = {}, declarations = {}, scopes = {}, joined = null;
  let candidate = null, verdict = null, acceptance = null, receipt = null, repairCount = 0, childResult = null;
  while (true) {
    const node = nodes[at], kind = node.type;
    phase(id, kind, at, repairCount);
    if (kind === 'parallel') {
      declarations = Object.fromEntries(Object.entries(node.branches).map(([key, b]) => [key, b.result_type]));
      scopes = Object.fromEntries(Object.entries(node.branches).map(([key, b]) => [key, b.scope_status]));
      const names = Object.keys(node.branches);
      const results = yield* Effect.all(names.map(instance => {
        const branch = node.branches[instance];
        return stage(`${at}:assign:${instance}`, () => callBridge('assign', {
          run: id, digest, instance, result_type: branch.result_type,
          scope_status: branch.scope_status, binding: pkg.bindings[branch.service],
        }), id, `${at}:assign:${instance}`, 'assign');
      }), { concurrency: names.length });
      branches = Object.fromEntries(names.map((instance, index) => [instance, parsed(results[index])]));
      at = node.next;
    } else if (kind === 'join') {
      joined = parsed(yield* stage(`${at}:join`, () => callBridge('join', {
        run: id, digest, branches, declarations, scopes,
      }), id, `${at}:join`, 'join'));
      at = node.next;
    } else if (kind === 'route') {
      const values = { 'join.route_status': joined?.route_status,
        'join.requires_scope': String(joined?.requires_scope),
        'verdict.accepted': String(verdict?.accepted) };
      const selected = values[node.field];
      if (!(selected in node.cases)) throw new Error('route has no validated successor');
      event(id, `${at}:${repairCount}:${selected}`, 'route', { field: node.field, selected });
      at = node.cases[selected];
    } else if (kind === 'synthesize') {
      const revision = `r${repairCount + 1}`;
      const resolved = node.resolved === true || (node.resolved === 'after_repair' &&
        repairCount >= input.resolution_after_repairs);
      const author = pkg.bindings[Object.values(nodes).find(n => n.type === 'parallel')
        .branches[Object.keys(declarations).find(name => declarations[name] === 'source_evidence')].service].identity;
      candidate = parsed(yield* stage(`${at}:synthesize:${revision}`, () => callBridge('synthesize', {
        join: joined, revision, author, resolved,
      }), id, `${at}:synthesize:${revision}`, 'synthesize'));
      const old = row(id);
      if (old.revision && old.revision === revision && old.sha256 !== candidate.sha256)
        throw new Error('immutable candidate revision changed');
      phase(id, kind, at, repairCount, { revision, sha256: candidate.sha256,
        artifact_json: canonical(candidate), verdict_json: null });
      verdict = null; acceptance = null;
      at = node.next;
    } else if (kind === 'quality') {
      const quality = Object.values(pkg.bindings).find(binding => binding.role === 'quality');
      const outcome = parsed(yield* stage(`${at}:quality:${candidate.revision}`, () => callBridge('review', {
        run: id, digest, artifact: candidate, binding: quality,
      }), id, `${at}:quality:${candidate.revision}`, 'quality'));
      verdict = outcome.artifact;
      if (verdict.revision !== candidate.revision || verdict.sha256 !== candidate.sha256 ||
          verdict.reviewer !== quality.identity || verdict.reviewer === candidate.author)
        throw new Error('stale or forged Quality verdict');
      const current = row(id);
      if (current.revision !== candidate.revision || current.sha256 !== candidate.sha256)
        throw new Error('Quality changed under current candidate');
      phase(id, kind, at, repairCount, { verdict_json: canonical({ ...outcome, artifact: verdict }) });
      if (verdict.accepted === true) {
        acceptance = { run: id, definition_digest: digest, revision: candidate.revision,
          sha256: candidate.sha256, attempt: repairCount + 1,
          quality_identity: quality.identity, quality_task_id: outcome.task_id };
        db.prepare(`UPDATE runs SET acceptance_json=? WHERE id=? AND revision=? AND sha256=?
          AND acceptance_json IS NULL`).run(canonical(acceptance), id, candidate.revision, candidate.sha256);
        if (row(id).acceptance_json !== canonical(acceptance)) throw new Error('acceptance conflict');
        event(id, `${at}:accept:${candidate.revision}`, 'acceptance', acceptance);
      }
      at = node.next;
    } else if (kind === 'repair') {
      if (verdict?.accepted !== false) throw new Error('repair without negative current verdict');
      if (repairCount < node.max_repairs) { repairCount++; at = node.next; }
      else at = node.exhausted;
    } else if (kind === 'director_wait') {
      if (verdict?.accepted !== false || repairCount < 1) throw new Error('Director wait before exhaustion');
      const current = row(id);
      const deadline = current.deadline_ms ?? Date.now() + input.wait_seconds * 1000;
      phase(id, 'awaiting-director', at, repairCount, { deadline_ms: deadline });
      const decision = yield* DurableDeferred.await(Decision);
      if (decision !== 'abort' || row(id).decision !== 'abort') throw new Error('unapproved decision');
      at = node.next;
    } else if (kind === 'abort') {
      phase(id, 'aborted', at, repairCount);
      return canonical({ status: 'aborted', run: id, definition_digest: digest,
        revision: candidate.revision, repairs: repairCount, released: false });
    } else if (kind === 'release') {
      const current = row(id);
      if (!acceptance || current.acceptance_json !== canonical(acceptance) ||
          acceptance.revision !== candidate.revision || acceptance.sha256 !== candidate.sha256)
        throw new Error('release lacks exact authoritative acceptance');
      const command = { release_id: `${id}:release:${candidate.revision}`, run_id: id,
        definition_digest: digest, revision: candidate.revision,
        sha256: candidate.sha256, content: candidate.content };
      receipt = parsed(yield* stage(`${at}:release:${candidate.revision}`, () => callBridge('release', {
        command, binding: pkg.bindings[node.service],
      }), id, `${at}:release:${candidate.revision}`, 'release'));
      phase(id, kind, at, repairCount, { receipt_json: canonical(receipt) });
      at = node.next;
    } else if (kind === 'nested_factory') {
      const child = pkg.children[node.child_digest];
      const childId = `${id}:child:${node.child_digest.slice(0, 12)}`;
      const payload = bindRun(childId, node.child_digest, packageDigest, child, input);
      phase(id, 'awaiting-child', at, repairCount, { child_id: childId });
      childResult = JSON.parse(yield* FactoryRun.execute(payload));
      if (childResult.status !== 'accepted') {
        phase(id, 'child-' + childResult.status, at, repairCount);
        return canonical({ status: childResult.status, run: id,
          definition_digest: digest, child: childResult, released: false });
      }
      acceptance = childResult.acceptance;
      receipt = childResult.receipt;
      at = node.next;
    } else if (kind === 'complete') {
      if (!acceptance || !receipt) throw new Error('completion lacks acceptance and release receipt');
      phase(id, 'accepted', at, repairCount);
      return canonical({ status: 'accepted', run: id, definition_digest: digest,
        acceptance, receipt, child: childResult, repairs: repairCount, released: true });
    } else throw new Error('unsupported node');
  }
}));

const Sqlite = SqliteClient.layer({ filename: path.join(root, 'effect.sqlite') });
const Runner = SingleRunner.layer({ runnerStorage: 'sql' }).pipe(Layer.provideMerge(Sqlite));
const Engine = ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(Runner));
const runtime = ManagedRuntime.make(FactoryLive.pipe(Layer.provideMerge(Engine)));
function payloadFor(found) { return { id: found.id, digest: found.digest,
  packageDigest: found.package_digest, docJson: found.document_json, inputJson: found.input_json }; }
async function signalDecision(found) {
  const token = await runtime.runPromise(DurableDeferred.tokenFromPayload(Decision,
    { workflow: FactoryRun, payload: payloadFor(found) }));
  await runtime.runPromise(DurableDeferred.succeed(Decision, { token, value: 'abort' }));
  db.prepare("UPDATE runs SET decision_state='signaled' WHERE id=? AND decision='abort'").run(found.id);
}
async function command(route, body) {
  if (route === '/health') return { ready: true, pid: process.pid };
  if (route === '/publish') {
    let result;
    try { result = await callBridge('validate', { package: body, approved_bindings: approved }); }
    catch (error) { throw problem(400, error.message); }
    const raw = canonical(body);
    if (sha256(raw) !== result.digest) throw problem(400, 'canonical digest mismatch');
    db.prepare('INSERT OR IGNORE INTO definitions VALUES (?,?)').run(result.digest, raw);
    if (db.prepare('SELECT body FROM definitions WHERE digest=?').get(result.digest).body !== raw)
      throw problem(409, 'published definition collision');
    return { package_digest: result.digest, root_digest: sha256(canonical(body.root)) };
  }
  if (route === '/start') {
    const pkg = packageAt(body.package_digest);
    const input = body.input;
    if (!input || !Number.isInteger(input.resolution_after_repairs) ||
        input.resolution_after_repairs < 0 || input.resolution_after_repairs > 3 ||
        !input.director || !validId(input.director.identity) ||
        typeof input.director.token !== 'string' || input.director.token.length < 8 ||
        input.director.epoch !== 1 || !Number.isInteger(input.wait_seconds) ||
        input.wait_seconds < 10 || input.wait_seconds > 600)
      throw problem(400, 'invalid bounded run input');
    const digest = sha256(canonical(pkg.root));
    const payload = bindRun(body.id, digest, body.package_digest, pkg.root, input);
    const executionId = await runtime.runPromise(FactoryRun.execute(payload, { discard: true }));
    return { started: true, executionId, id: body.id, definition_digest: digest,
      package_digest: body.package_digest };
  }
  if (route === '/poll') {
    const found = row(body.id);
    const executionId = await runtime.runPromise(FactoryRun.executionId(payloadFor(found)));
    const result = await runtime.runPromise(FactoryRun.poll(executionId));
    return { state: result?._tag ?? 'Pending', exit: result?.exit?._tag,
      value: result?.exit?._tag === 'Success' ? JSON.parse(result.exit.value) : undefined,
      run: { id: found.id, digest: found.digest, phase: found.phase,
        child_id: found.child_id, revision: found.revision, sha256: found.sha256,
        repair_count: found.repair_count, acceptance: found.acceptance_json && JSON.parse(found.acceptance_json) } };
  }
  if (route === '/decide') {
    const found = row(body.id);
    const input = JSON.parse(found.input_json);
    if (found.phase !== 'awaiting-director' || found.decision !== null)
      throw problem(409, 'no current Director wait');
    if (Date.now() >= found.deadline_ms) throw problem(409, 'Director command expired');
    if (body.action !== 'abort' || body.actor !== input.director.identity ||
        body.token !== input.director.token) throw problem(403, 'unauthorized Director');
    if (body.epoch !== found.owner_epoch || body.digest !== found.digest ||
        body.revision !== found.revision || body.sha256 !== found.sha256)
      throw problem(409, 'stale Director owner, definition, or revision');
    db.prepare("UPDATE runs SET decision='abort',decision_state='pending' WHERE id=? AND decision IS NULL")
      .run(found.id);
    await signalDecision(row(found.id));
    return { recorded: true, id: found.id };
  }
  if (route === '/ledger') {
    const found = row(body.id);
    const events = db.prepare('SELECT event_key,kind,value_json,created_ms FROM events WHERE run_id=? ORDER BY created_ms,event_key')
      .all(found.id).map(e => ({ key: e.event_key, kind: e.kind,
        value: JSON.parse(e.value_json), created_ms: e.created_ms }));
    return { run: found, events };
  }
  throw problem(404, 'unknown route');
}
await runtime.runtime();
for (const pending of db.prepare("SELECT * FROM runs WHERE decision='abort' AND decision_state='pending'").all()) {
  await signalDecision(pending);
}
http.createServer(async (request, response) => {
  try {
    let raw = '';
    for await (const chunk of request) {
      raw += chunk;
      if (raw.length > 1024 * 1024) throw problem(413, 'request too large');
    }
    const value = await command(request.url, raw ? JSON.parse(raw) : {});
    response.writeHead(200, { 'content-type': 'application/json' });
    response.end(JSON.stringify(value));
  } catch (error) {
    response.writeHead(error.status ?? 500, { 'content-type': 'application/json' });
    response.end(JSON.stringify({ error: error.message ?? String(error) }));
  }
}).listen(port, '127.0.0.1', () => console.log(JSON.stringify({ event: 'ready', pid: process.pid, port })));
