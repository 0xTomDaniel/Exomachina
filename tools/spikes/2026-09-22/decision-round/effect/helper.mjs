// One fixed Effect workflow interprets pinned, approved factory documents.
// The A2A adapter and acceptance/outbox ledger below are Exomachina-owned code.
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import Database from 'better-sqlite3';
import { ClusterWorkflowEngine, SingleRunner } from '@effect/cluster';
import { SqliteClient } from '@effect/sql-sqlite-node';
import { Activity, DurableDeferred, Workflow } from '@effect/workflow';
import { Effect, Layer, ManagedRuntime, Schema } from 'effect';

const root = process.env.EFFECT_DECISION_ROOT;
if (!root) throw new Error('EFFECT_DECISION_ROOT is required');
const port = Number(process.env.EFFECT_DECISION_PORT ?? '19872');
fs.mkdirSync(root, { recursive: true });
const db = new Database(path.join(root, 'product.sqlite'));
db.pragma('journal_mode = WAL');
db.pragma('synchronous = FULL');
db.exec(`
  CREATE TABLE IF NOT EXISTS definitions (digest TEXT PRIMARY KEY, body TEXT NOT NULL);
  CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY, digest TEXT NOT NULL, body TEXT NOT NULL,
    revision TEXT, sha256 TEXT, author TEXT, artifact TEXT, assignment_task TEXT,
    reviewer TEXT, review_task TEXT, quality_accepted INTEGER NOT NULL DEFAULT 0,
    accepted_revision TEXT, accepted_sha256 TEXT, accepted_by TEXT);
  CREATE TABLE IF NOT EXISTS outbox (
    run_id TEXT PRIMARY KEY, command_id TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK(state IN ('pending','signaled')));
  CREATE TABLE IF NOT EXISTS deliveries (run_id TEXT PRIMARY KEY, sha256 TEXT NOT NULL);
`);
const sha256 = raw => crypto.createHash('sha256').update(raw).digest('hex');
const canonical = value => JSON.stringify(value);
const problem = (status, message) => Object.assign(new Error(message), { status });
const validId = value => typeof value === 'string' && /^[a-zA-Z0-9_-]{1,80}$/.test(value);
const validDigest = value => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value);
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function validate(doc) {
  if (!doc || doc.name !== 'verified-research' || ![1, 2].includes(doc.version))
    throw problem(400, 'unsupported factory header');
  const steps = doc.version === 1
    ? ['assign', 'review', 'director_wait', 'deliver']
    : ['assign', 'verify', 'review', 'director_wait', 'deliver'];
  if (canonical(doc.steps) !== canonical(steps)) throw problem(400, 'unsupported steps or review bypass');
  if (canonical(Object.keys(doc).sort()) !== canonical(['capabilities', 'name', 'steps', 'version']))
    throw problem(400, 'unexpected definition fields');
  if (canonical(Object.keys(doc.capabilities ?? {}).sort()) !== canonical(['quality', 'worker']))
    throw problem(400, 'missing capability closure');
  for (const key of ['worker', 'quality']) {
    const cap = doc.capabilities[key];
    if (!cap || canonical(Object.keys(cap).sort()) !== canonical(['identity', 'url', 'version']) ||
        !validId(cap.identity) || !validId(cap.version) ||
        typeof cap.url !== 'string' || !/^http:\/\/127\.0\.0\.1:\d+$/.test(cap.url))
      throw problem(400, `invalid ${key} binding`);
  }
  if (doc.capabilities.worker.identity === doc.capabilities.quality.identity)
    throw problem(400, 'Quality must have independent identity');
  return doc;
}

function publish(doc) {
  validate(doc);
  const body = canonical(doc);
  const digest = sha256(body);
  db.prepare('INSERT OR IGNORE INTO definitions VALUES (?,?)').run(digest, body);
  if (db.prepare('SELECT body FROM definitions WHERE digest=?').get(digest).body !== body)
    throw problem(409, 'definition digest collision');
  return { digest, version: doc.version };
}

function definition(digest) {
  if (!validDigest(digest)) throw problem(400, 'invalid digest');
  const row = db.prepare('SELECT body FROM definitions WHERE digest=?').get(digest);
  if (!row) throw problem(404, 'definition not published');
  if (sha256(row.body) !== digest) throw problem(409, 'definition changed');
  validate(JSON.parse(row.body));
  return row.body;
}

function boundRun(id, digest) {
  if (!validId(id)) throw problem(400, 'invalid run ID');
  const row = db.prepare('SELECT * FROM runs WHERE id=?').get(id);
  if (!row) throw problem(404, 'run not found');
  if (digest !== undefined && digest !== row.digest) throw problem(409, 'run digest mismatch');
  return row;
}

function bindRun(id, digest, body) {
  if (!validId(id)) throw problem(400, 'invalid run ID');
  db.prepare('INSERT OR IGNORE INTO runs (id,digest,body) VALUES (?,?,?)').run(id,digest,body);
  const row = boundRun(id,digest);
  if (row.body !== body) throw problem(409, 'run definition changed');
  return { id, digest, docJson: body };
}

async function jsonFetch(url, options = {}) {
  const response = await fetch(url, { ...options,
    headers:{authorization:'Bearer fixture-token',...(options.headers??{})},
    signal: AbortSignal.timeout(3000) });
  if (!response.ok) throw new Error(`HTTP ${response.status} from ${url}`);
  return response.json();
}

async function actionReceipt(url, actionId) {
  try { return await jsonFetch(`${url}/fixture/actions/${encodeURIComponent(actionId)}`); }
  catch { return null; }
}

async function a2aAction(cap, command) {
  const actionId = command.action_id;
  let response = null;
  let sendError = null;
  try {
    response = await jsonFetch(`${cap.url}/`, {
      method: 'POST', headers: {'content-type': 'application/json'},
      body: canonical({ jsonrpc:'2.0', id:actionId, method:'message/send', params:{message:{
        role:'user', messageId:crypto.randomUUID(), parts:[{kind:'data',data:command}]
      }}})
    });
    if (response.error) throw new Error(canonical(response.error));
  } catch (error) { sendError = String(error); }
  // The receipt is the fixture's explicit idempotency/status query. A2A by
  // itself does not promise discovery by our stable application action ID.
  let receipt = await actionReceipt(cap.url, actionId);
  const deadline = Date.now() + 20000;
  while (!receipt && Date.now() < deadline) {
    await sleep(200);
    receipt = await actionReceipt(cap.url, actionId);
  }
  if (!receipt) throw new Error(`action ${actionId} unresolved; safe unknown: ${sendError}`);
  if (receipt.action_id !== actionId || receipt.run_id !== command.run_id ||
      receipt.definition_digest !== command.definition_digest ||
      receipt.role !== (command.op==='assign'?'capability':'quality'))
    throw new Error(`action ${actionId} receipt binding mismatch`);
  if (response?.result && response.result.status?.state !== 'completed')
    throw new Error(`action ${actionId} not completed`);
  return { receipt, responseState: response?.result?.status?.state ?? null,
           lostAck: sendError !== null, sendError };
}

function putCandidate(id, digest, action, expectedAuthor) {
  const artifact = action.receipt.artifact;
  if (!artifact || artifact.revision !== 'r2' || !validDigest(artifact.sha256) ||
      sha256(artifact.content) !== artifact.sha256 || artifact.author !== expectedAuthor)
    throw new Error('invalid worker artifact');
  const run = boundRun(id,digest);
  if (run.revision && (run.revision !== artifact.revision || run.sha256 !== artifact.sha256 ||
      run.author !== artifact.author)) throw new Error('candidate revision changed on replay');
  db.prepare(`UPDATE runs SET revision=?, sha256=?, author=?, artifact=?, assignment_task=?
              WHERE id=?`).run(artifact.revision, artifact.sha256, artifact.author,
                              canonical(artifact), action.receipt.task_id, id);
  return artifact;
}

function putReview(id, digest, action, expectedReviewer) {
  const run = boundRun(id,digest);
  const verdict = action.receipt.artifact;
  if (!verdict || verdict.accepted !== true || verdict.revision !== run.revision ||
      verdict.sha256 !== run.sha256 || verdict.reviewer !== expectedReviewer ||
      verdict.reviewer === run.author) throw new Error('Quality did not accept exact revision independently');
  if (run.reviewer && run.reviewer !== verdict.reviewer) throw new Error('Quality identity changed');
  db.prepare(`UPDATE runs SET reviewer=?, review_task=?, quality_accepted=1 WHERE id=?`)
    .run(verdict.reviewer, action.receipt.task_id, id);
  return verdict;
}

const Approval = DurableDeferred.make('director-decision', { success: Schema.String });
const FactoryRun = Workflow.make({
  name:'ProductFactoryRun', payload:{id:Schema.String,digest:Schema.String,docJson:Schema.String},
  idempotencyKey:({id})=>id, success:Schema.String
});
const stage = (name, execute) => Activity.make({
  name, success:Schema.String,
  execute:Effect.promise(async()=>canonical(await execute()))
});
const FactoryLive = FactoryRun.toLayer(({id,digest,docJson})=>Effect.gen(function*(){
  if (sha256(docJson)!==digest) throw new Error('workflow payload digest mismatch');
  const doc = validate(JSON.parse(docJson));
  const assignId = `${id}-assign`;
  const assignment = yield* stage('assign',async()=>{
    const action = await a2aAction(doc.capabilities.worker,{
      op:'assign',action_id:assignId,run_id:id,definition_digest:digest,
      brief:`verified research ${doc.version}`,
      drop_ack:id==='run-v1'
    });
    return {artifact:putCandidate(id,digest,action,doc.capabilities.worker.identity),lostAck:action.lostAck,
            acceptedCount:action.receipt.accepted_count};
  });
  const artifact = JSON.parse(assignment).artifact;
  if (doc.steps.includes('verify')) yield* stage('verify',async()=>{
    if (sha256(artifact.content)!==artifact.sha256) throw new Error('verify mismatch');
    return {verified:artifact.sha256};
  });
  const reviewId = `${id}-review`;
  yield* stage('review',async()=>{
    const action = await a2aAction(doc.capabilities.quality,{
      op:'review',action_id:reviewId,run_id:id,definition_digest:digest,artifact
    });
    return {verdict:putReview(id,digest,action,doc.capabilities.quality.identity),
            acceptedCount:action.receipt.accepted_count};
  });
  const decision = yield* DurableDeferred.await(Approval);
  if (decision!=='accepted') throw new Error('unapproved decision');
  yield* stage('deliver',async()=>{
    const run = boundRun(id,digest);
    if (run.accepted_revision!==run.revision || run.accepted_sha256!==run.sha256 ||
        run.accepted_by!==run.reviewer) throw new Error('acceptance gate changed');
    db.prepare('INSERT OR IGNORE INTO deliveries VALUES (?,?)').run(id,run.sha256);
    return {revision:run.revision,sha256:run.sha256};
  });
  return canonical({id,digest,version:doc.version,revision:artifact.revision,sha256:artifact.sha256});
}));

const Sqlite=SqliteClient.layer({filename:path.join(root,'effect.sqlite')});
const Runner=SingleRunner.layer({runnerStorage:'sql'}).pipe(Layer.provideMerge(Sqlite));
const Engine=ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(Runner));
const runtime=ManagedRuntime.make(FactoryLive.pipe(Layer.provideMerge(Engine)));

function accept(body) {
  return db.transaction(()=>{
    const run=boundRun(body.id,body.digest);
    if (!run.quality_accepted) throw problem(409,'Quality has not accepted');
    if (body.revision!==run.revision || body.sha256!==run.sha256)
      throw problem(409,'stale or mismatched artifact revision');
    if (body.actor!==run.reviewer || body.actor===run.author)
      throw problem(403,'acceptance requires independent Quality identity');
    if (run.accepted_revision) throw problem(409,'duplicate acceptance');
    const commandId=`${run.id}-continue`;
    db.prepare(`UPDATE runs SET accepted_revision=?,accepted_sha256=?,accepted_by=?
                WHERE id=? AND accepted_revision IS NULL`).run(run.revision,run.sha256,body.actor,run.id);
    db.prepare('INSERT INTO outbox VALUES (?,?,?)').run(run.id,commandId,'pending');
    return {accepted:true,id:run.id,revision:run.revision,sha256:run.sha256,commandId};
  }).immediate();
}

async function command(route,body) {
  if (route==='/health') return {ready:true,pid:process.pid};
  if (route==='/publish') return publish(body);
  if (route==='/start') {
    const docJson=definition(body.digest);
    const payload=bindRun(body.id,body.digest,docJson);
    const executionId=await runtime.runPromise(FactoryRun.execute(payload,{discard:true}));
    return {started:true,id:body.id,digest:body.digest,executionId};
  }
  if (route==='/poll') {
    const row=boundRun(body.id);
    const payload={id:row.id,digest:row.digest,docJson:row.body};
    const executionId=await runtime.runPromise(FactoryRun.executionId(payload));
    const result=await runtime.runPromise(FactoryRun.poll(executionId));
    return result?._tag==='Complete'
      ? {state:'Complete',exit:result.exit._tag,
         value:result.exit._tag==='Success'?JSON.parse(result.exit.value):undefined}
      : {state:result?._tag??'Pending'};
  }
  if (route==='/accept') return accept(body);
  if (route==='/flush') {
    const row=boundRun(body.id,body.digest);
    const out=db.prepare('SELECT * FROM outbox WHERE run_id=?').get(row.id);
    if (!out) throw problem(409,'no accepted outbox command');
    const payload={id:row.id,digest:row.digest,docJson:row.body};
    const token=await runtime.runPromise(DurableDeferred.tokenFromPayload(Approval,{workflow:FactoryRun,payload}));
    await runtime.runPromise(DurableDeferred.succeed(Approval,{token,value:'accepted'}));
    if (body.drop_ack === true) process.kill(process.pid, 'SIGKILL');
    db.prepare("UPDATE outbox SET state='signaled' WHERE run_id=?").run(row.id);
    return {signaled:true,commandId:out.command_id};
  }
  if (route==='/ledger') {
    const row=boundRun(body.id);
    return {run:row,outbox:db.prepare('SELECT * FROM outbox WHERE run_id=?').get(row.id)??null,
            delivery:db.prepare('SELECT * FROM deliveries WHERE run_id=?').get(row.id)??null};
  }
  throw problem(404,'unknown route');
}

await runtime.runtime();
http.createServer(async(request,response)=>{
  try {
    let raw='';
    for await (const chunk of request) {
      raw+=chunk;
      if (raw.length>65536) throw problem(413,'request too large');
    }
    const body=raw?JSON.parse(raw):{};
    const result=await command(request.url,body);
    response.writeHead(200,{'content-type':'application/json'});
    response.end(canonical(result));
  } catch(error) {
    response.writeHead(error.status??500,{'content-type':'application/json'});
    response.end(canonical({error:error.message??String(error)}));
  }
}).listen(port,'127.0.0.1',()=>console.log(canonical({event:'ready',pid:process.pid,port})));
