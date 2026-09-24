import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import net from 'node:net';
import http from 'node:http';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const broker = fileURLToPath(new URL('../exo-model.mjs', import.meta.url));
const preload = fileURLToPath(new URL('../testing/mock-oauth-fetch.mjs', import.meta.url));
const trial = `/tmp/exo-proto-broker-${process.pid}-${Date.now()}`;
const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const mkhome = (label, fixture = false) => {
  const home = path.join(trial, label, 'model');
  fs.mkdirSync(path.join(home, 'secrets'), { recursive: true, mode: 0o700 });
  if (fixture) fs.writeFileSync(path.join(home, 'FIXTURE_STORE'), 'synthetic fixture\n', { mode: 0o600 });
  return home;
};
const jwtPart = (value) => Buffer.from(JSON.stringify(value)).toString('base64url');
const credential = (expires = Date.now() + 3600000) => ({
  type: 'oauth',
  access: `${jwtPart({ alg: 'none' })}.${jwtPart({ 'https://api.openai.com/auth': { chatgpt_account_id: 'acct_synthetic' } })}.signature`,
  refresh: `synthetic_refresh_${process.pid}_${Date.now()}`,
  expires, accountId: 'acct_synthetic',
});
function seed(home, value = credential()) {
  const file = path.join(home, 'secrets', 'openai-codex.json');
  fs.writeFileSync(file, JSON.stringify(value), { mode: 0o600 });
  return file;
}
function run(args, env = {}) {
  return new Promise((resolve) => {
    const child = spawn(process.execPath, [broker, ...args], { env: { ...process.env, ...env }, stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '', stderr = '';
    child.stdout.on('data', (c) => { stdout += c; });
    child.stderr.on('data', (c) => { stderr += c; });
    child.on('close', (code) => resolve({ code, stdout, stderr }));
  });
}
function launch(home, extra = {}) {
  const child = spawn(process.execPath, [broker, 'serve'], {
    env: { ...process.env, EXO_MODEL_HOME: home, ...extra }, stdio: ['ignore', 'pipe', 'pipe'],
  });
  const ready = new Promise((resolve, reject) => {
    let output = '', errors = '';
    const timer = setTimeout(() => reject(new Error('serve timeout')), 6000);
    child.stdout.on('data', (c) => {
      output += c;
      const end = output.indexOf('\n');
      if (end >= 0) { clearTimeout(timer); resolve(JSON.parse(output.slice(0, end))); }
    });
    child.stderr.on('data', (c) => { errors += c; });
    child.on('close', (code) => { if (!output.includes('\n')) { clearTimeout(timer); reject(new Error(`serve exit ${code}: ${errors}`)); } });
  });
  return { child, ready };
}
async function transact(home, request, terminal = (r) => Boolean(r.health || r.done || r.error || r.refresh)) {
  return new Promise((resolve, reject) => {
    const conn = net.createConnection(path.join(home, 'run', 'broker.sock'));
    let buffer = ''; const replies = [];
    const timer = setTimeout(() => conn.destroy(new Error('socket timeout')), 6000);
    conn.on('connect', () => conn.write(JSON.stringify(request) + '\n'));
    conn.on('data', (chunk) => {
      buffer += chunk;
      for (let end; (end = buffer.indexOf('\n')) >= 0;) {
        const reply = JSON.parse(buffer.slice(0, end)); buffer = buffer.slice(end + 1);
        replies.push(reply);
        if (terminal(reply)) { clearTimeout(timer); conn.end(); resolve(replies); return; }
      }
    });
    conn.on('error', (error) => { clearTimeout(timer); reject(error); });
  });
}
async function stop(child) {
  if (child.exitCode === null) { child.kill('SIGTERM'); await new Promise((resolve) => child.once('close', resolve)); }
}

function startMock(port) {
  const requests = [];
  const server = http.createServer((req, res) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      let body;
      try { body = JSON.parse(Buffer.concat(chunks).toString()); } catch { body = {}; }
      const observed = { headers: req.headers, body, url: req.url, closed: false };
      requests.push(observed);
      res.on('close', () => { observed.closed = true; });
      if (['unauthorized', 'forbidden', 'quota', 'rate'].includes(req.headers['session-id'])) {
        const session = req.headers['session-id'];
        const status = session === 'unauthorized' ? 401 : session === 'forbidden' ? 403 : 429;
        res.writeHead(status, { 'content-type': 'application/json' });
        if (session === 'rate') { res.end('too many requests'); return; }
        res.end(JSON.stringify({ error: { code: session === 'quota' ? 'usage_limit_reached' : 'rate_limit_exceeded',
          message: session === 'quota' ? 'usage limit reached' : 'request refused' } }));
        return;
      }
      if (req.headers['session-id'] === 'hold') {
        res.writeHead(200, { 'content-type': 'text/event-stream' });
        const event = { type: 'response.created', response: { id: 'hold', object: 'response',
          model: body.model || 'gpt-6-sol', status: 'in_progress', output: [] } };
        res.write(`event: response.created\ndata: ${JSON.stringify(event)}\n\n`);
        return;
      }
      const model = body.model || 'gpt-6-sol';
      const events = [{ type: 'response.created', response: { id: 'resp_1', object: 'response', model, status: 'in_progress', output: [] } }];
      const items = [];
      if (req.headers['session-id'] === 'interleaved') {
        const calls = [
          { type: 'function_call', id: 'fc_a', call_id: 'a', name: 'alpha', arguments: '{"value":"one"}', status: 'completed' },
          { type: 'function_call', id: 'fc_b', call_id: 'b', name: 'beta', arguments: '{"value":"two"}', status: 'completed' },
        ];
        calls.forEach((call, index) => events.push({ type: 'response.output_item.added', output_index: index,
          item: { ...call, arguments: '', status: 'in_progress' } }));
        for (let part = 0; part < 3; part++) calls.forEach((call, index) => {
          const cuts = [call.arguments.slice(0, 5), call.arguments.slice(5, 10), call.arguments.slice(10)];
          events.push({ type: 'response.function_call_arguments.delta', item_id: call.id, output_index: index, delta: cuts[part] });
        });
        calls.forEach((call, index) => {
          events.push({ type: 'response.function_call_arguments.done', item_id: call.id, output_index: index, arguments: call.arguments });
          events.push({ type: 'response.output_item.done', output_index: index, item: call });
          items.push(call);
        });
      } else {
        const text = req.headers['session-id'] || 'reply';
        const msg = { type: 'message', id: 'msg_1', role: 'assistant', status: 'completed',
          content: [{ type: 'output_text', text, annotations: [] }] };
        events.push({ type: 'response.output_item.added', output_index: 0, item: { ...msg, status: 'in_progress', content: [] } });
        events.push({ type: 'response.content_part.added', item_id: 'msg_1', output_index: 0, content_index: 0,
          part: { type: 'output_text', text: '', annotations: [] } });
        events.push({ type: 'response.output_text.delta', item_id: 'msg_1', output_index: 0, content_index: 0, delta: text });
        events.push({ type: 'response.output_item.done', output_index: 0, item: msg });
        items.push(msg);
      }
      events.push({ type: 'response.completed', response: { id: 'resp_1', object: 'response', model,
        status: 'completed', output: items, usage: { input_tokens: 10, output_tokens: 5, total_tokens: 15,
          input_tokens_details: { cached_tokens: 0 }, output_tokens_details: { reasoning_tokens: 0 } } } });
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      for (const event of events) res.write(`event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`);
      res.end();
    });
  });
  return new Promise((resolve) => server.listen(port, '127.0.0.1', () => resolve({ server, requests })));
}
const context = { systemPrompt: 'test', messages: [{ role: 'user', content: [{ type: 'text', text: 'hello' }] }],
  tools: [{ name: 'alpha', description: 'a', parameters: { type: 'object', properties: {} } },
    { name: 'beta', description: 'b', parameters: { type: 'object', properties: {} } }] };

test('override guard refuses unsafe stores and hosts before HTTP', async () => {
  const mock = await startMock(46100);
  try {
    const url = 'http://127.0.0.1:46100/backend-api';
    const defaultResult = await run(['status'], { EXO_CODEX_BASE_URL: url, EXO_MODEL_HOME: '' });
    assert.equal(defaultResult.code, 1);
    assert.match(defaultResult.stderr, /"kind":"config"/);
    const unmarked = mkhome('unmarked');
    const noMarker = await run(['serve'], { EXO_MODEL_HOME: unmarked, EXO_CODEX_BASE_URL: url });
    assert.equal(noMarker.code, 1);
    const marked = mkhome('marked', true);
    const nonloopback = await run(['refresh'], { EXO_MODEL_HOME: marked, EXO_CODEX_BASE_URL: 'https://example.com/backend-api' });
    assert.equal(nonloopback.code, 1);
    const login = await run(['login'], { EXO_MODEL_HOME: marked, EXO_CODEX_BASE_URL: url });
    assert.equal(login.code, 1);
    assert.equal(mock.requests.length, 0);
  } finally { await new Promise((resolve) => mock.server.close(resolve)); }
});

test('two independent starts share one broker and retain session separation', async () => {
  const mock = await startMock(46101);
  const home = mkhome('race', true); seed(home);
  const url = 'http://127.0.0.1:46101/backend-api';
  const first = launch(home, { EXO_CODEX_BASE_URL: url });
  const second = launch(home, { EXO_CODEX_BASE_URL: url });
  try {
    const firstReady = await Promise.race([first.ready, second.ready]);
    const socket = path.join(home, 'run', 'broker.sock');
    const inode = fs.statSync(socket).ino;
    const readyPid = JSON.parse(fs.readFileSync(path.join(home, 'run', 'broker-ready.json'), 'utf8')).pid;
    const [a, b] = await Promise.all([first.ready, second.ready]);
    assert.equal(firstReady.pid, readyPid);
    assert.equal([a.serving, b.serving].filter(Boolean).length, 1);
    assert.equal([a.attached, b.attached].filter(Boolean).length, 1);
    const health = (await transact(home, { id: 'h', op: 'health' }))[0].health;
    assert.equal(a.pid, b.pid);
    assert.equal(health.pid, a.pid);
    const stored = JSON.parse(fs.readFileSync(path.join(home, 'secrets', 'openai-codex.json'), 'utf8'));
    const metadata = JSON.stringify(health) + fs.readFileSync(path.join(home, 'run', 'broker-ready.json'), 'utf8') +
      fs.readFileSync(path.join(home, 'broker-events.jsonl'), 'utf8');
    assert.ok(!metadata.includes(stored.access));
    assert.ok(!metadata.includes(stored.refresh));
    assert.equal(JSON.parse(fs.readFileSync(path.join(home, 'run', 'broker-ready.json'), 'utf8')).pid, readyPid);
    assert.equal(fs.statSync(socket).ino, inode);
    const [one, two] = await Promise.all([
      transact(home, { id: 'one', op: 'stream', model: 'gpt-6-sol', session: 'client-one', context }),
      transact(home, { id: 'two', op: 'stream', model: 'gpt-6-sol', session: 'client-two', context }),
    ]);
    assert.ok(one.at(-1).done, JSON.stringify(one.at(-1)));
    assert.ok(two.at(-1).done, JSON.stringify(two.at(-1)));
    assert.equal(fs.statSync(socket).ino, inode);
    assert.deepEqual(mock.requests.map((r) => r.headers['session-id']).sort(), ['client-one', 'client-two']);
    for (const request of mock.requests) {
      assert.equal(request.headers.originator, 'exomachina');
      assert.match(request.headers['user-agent'], /^exomachina-model-broker\/0\.1\.0 \(pi-ai\/0\.87\.1\)$/);
    }
  } finally {
    await stop(first.child); await stop(second.child);
    await new Promise((resolve) => mock.server.close(resolve));
  }
});

test('HTTP auth and usage failures use explicit broker error kinds', async () => {
  const mock = await startMock(46106);
  const home = mkhome('errors', true); seed(home);
  const service = launch(home, { EXO_CODEX_BASE_URL: 'http://127.0.0.1:46106/backend-api' });
  try {
    await service.ready;
    for (const [session, expected] of [['unauthorized', 'reauth_required'], ['forbidden', 'reauth_required'],
      ['quota', 'quota'], ['rate', 'rate_limit']]) {
      const replies = await transact(home, { id: session, op: 'stream', model: 'gpt-6-sol', session, context });
      assert.equal(replies.at(-1).error?.kind, expected, JSON.stringify(replies.at(-1)));
    }
    assert.equal(mock.requests.length, 4);
  } finally { await stop(service.child); await new Promise((resolve) => mock.server.close(resolve)); }
});

test('stale socket and dead-pid credential lock recover; stream fragments stay indexed', async () => {
  const mock = await startMock(46102);
  const home = mkhome('stale', true); seed(home);
  const runDir = path.join(home, 'run'); fs.mkdirSync(runDir, { recursive: true });
  const stale = net.createServer();
  const socket = path.join(runDir, 'broker.sock');
  await new Promise((resolve) => stale.listen(socket, resolve));
  await new Promise((resolve) => stale.close(resolve));
  fs.writeFileSync(path.join(runDir, 'broker-ready.json'), JSON.stringify({ pid: 99999999 }));
  const lock = path.join(home, 'secrets', 'openai-codex.json.lock');
  fs.mkdirSync(lock); fs.writeFileSync(path.join(lock, 'owner'), '99999999');
  const service = launch(home, { EXO_CODEX_BASE_URL: 'http://127.0.0.1:46102/backend-api' });
  try {
    await service.ready;
    const replies = await transact(home, { id: 'i', op: 'stream', model: 'gpt-6-sol', session: 'interleaved', context });
    assert.ok(replies.at(-1).done, JSON.stringify(replies.at(-1)));
    const fragments = replies.filter((r) => r.ev?.type === 'toolcall_delta').map((r) => r.ev.contentIndex);
    assert.deepEqual(fragments, [0, 1, 0, 1, 0, 1], JSON.stringify(replies.map((r) => r.ev?.type)));
    const c = await run(['status'], { EXO_MODEL_HOME: home, EXO_CODEX_BASE_URL: 'http://127.0.0.1:46102/backend-api' });
    assert.equal(c.code, 0); assert.equal(JSON.parse(c.stdout).signed_in, true);
    assert.equal(fs.statSync(home).mode & 0o777, 0o700);
    assert.equal(fs.statSync(path.join(home, 'secrets', 'openai-codex.json')).mode & 0o777, 0o600);
    const logout = await run(['logout'], { EXO_MODEL_HOME: home, EXO_CODEX_BASE_URL: 'http://127.0.0.1:46102/backend-api' });
    assert.equal(logout.code, 0);
    assert.equal(fs.existsSync(lock), false);
  } finally { await stop(service.child); await new Promise((resolve) => mock.server.close(resolve)); }
});

test('closing a client aborts its in-flight provider stream', async () => {
  const mock = await startMock(46103);
  const home = mkhome('cancel', true); seed(home);
  const service = launch(home, { EXO_CODEX_BASE_URL: 'http://127.0.0.1:46103/backend-api' });
  try {
    await service.ready;
    const conn = net.createConnection(path.join(home, 'run', 'broker.sock'));
    await new Promise((resolve, reject) => {
      conn.on('connect', () => conn.write(JSON.stringify({ id: 'hold', op: 'stream', model: 'gpt-6-sol', session: 'hold', context }) + '\n'));
      conn.on('data', () => { conn.destroy(); resolve(); });
      conn.on('error', reject);
      setTimeout(() => reject(new Error('stream did not start')), 5000);
    });
    for (let i = 0; i < 50 && !mock.requests[0]?.closed; i++) await pause(20);
    assert.equal(mock.requests[0]?.closed, true);
  } finally { await stop(service.child); await new Promise((resolve) => mock.server.close(resolve)); }
});

test('forced refresh rotates atomically through preload and leak scan finds planted copies', async () => {
  const home = mkhome('refresh', true);
  const before = credential(Date.now() - 1000);
  const file = seed(home, before);
  const record = path.join(trial, 'oauth-record.jsonl');
  const env = { EXO_CODEX_BASE_URL: 'http://127.0.0.1:46104/backend-api',
    NODE_OPTIONS: `--import=${preload}`, EXO_MOCK_OAUTH_RECORD: record };
  const service = launch(home, env);
  try {
    await service.ready;
    const firstCli = await run(['refresh'], { EXO_MODEL_HOME: home, ...env });
    assert.equal(firstCli.code, 0);
    const first = JSON.parse(firstCli.stdout);
    assert.equal(first.refreshed, true);
    const middle = JSON.parse(fs.readFileSync(file, 'utf8'));
    assert.notEqual(middle.refresh, before.refresh);
    assert.ok(middle.expires > before.expires);
    const second = (await transact(home, { id: 'refresh-2', op: 'refresh' }))[0];
    assert.equal(second.refresh.refreshed, true);
    const after = JSON.parse(fs.readFileSync(file, 'utf8'));
    assert.notEqual(after.refresh, middle.refresh);
    assert.equal(fs.readFileSync(record, 'utf8').trim().split('\n').length, 2);
    const planted = path.join(trial, 'planted-copy.json');
    const bare = path.join(trial, 'bare-token.sqlite3');
    fs.writeFileSync(planted, fs.readFileSync(file));
    fs.writeFileSync(bare, Buffer.concat([Buffer.from('SQLite format 3\0'), Buffer.from(after.access)]));
    const scan = await run(['leak-scan', home, planted, bare], { EXO_MODEL_HOME: home, ...env });
    assert.equal(scan.code, 0);
    const result = JSON.parse(scan.stdout);
    assert.deepEqual(result.excluded, [fs.realpathSync(file)]);
    assert.ok(result.hits.some((hit) => hit.path === planted && hit.kind === 'access'));
    assert.ok(result.hits.some((hit) => hit.path === bare && hit.kind === 'access'));
    assert.ok(!scan.stdout.includes(after.access));
  } finally { await stop(service.child); }
});

test('missing or api-key credential never falls back to OPENAI_API_KEY', async () => {
  const home = mkhome('no-fallback', true);
  const service = launch(home, { EXO_CODEX_BASE_URL: 'http://127.0.0.1:46105/backend-api', OPENAI_API_KEY: 'synthetic_fallback_key' });
  try {
    await service.ready;
    const missing = (await transact(home, { id: 'missing', op: 'stream', model: 'gpt-6-sol', session: 'missing', context }))[0];
    assert.equal(missing.error.kind, 'reauth_required');
    seed(home, { type: 'api_key', key: 'synthetic_invalid' });
    const invalid = (await transact(home, { id: 'invalid', op: 'stream', model: 'gpt-6-sol', session: 'invalid', context }))[0];
    assert.equal(invalid.error.kind, 'reauth_required');
  } finally { await stop(service.child); }
});
