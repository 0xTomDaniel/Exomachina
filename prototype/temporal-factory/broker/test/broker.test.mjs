import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import net from 'node:net';
import http from 'node:http';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { rewriteAuthorizeOriginator, withPidLock } from '../lib/store.mjs';

const broker = fileURLToPath(new URL('../exo-model.mjs', import.meta.url));
const preload = fileURLToPath(new URL('../testing/mock-oauth-fetch.mjs', import.meta.url));
const trial = `/tmp/exo-proto-broker-${process.pid}-${Date.now()}`;
fs.mkdirSync(trial, { recursive: true, mode: 0o700 });
const recordingPreload = path.join(trial, 'recording-oauth-fetch.mjs');
fs.writeFileSync(recordingPreload, `
import fs from 'node:fs';
const originalFetch = globalThis.fetch;
let grants = 0;
const jwtPart = (value) => Buffer.from(JSON.stringify(value)).toString('base64url');
globalThis.fetch = async (input, init = {}) => {
  const url = new URL(input instanceof Request ? input.url : input);
  if (url.hostname === 'auth.openai.com') {
    const headers = new Headers(init.headers);
    const body = String(init.body || '');
    const grant = new URLSearchParams(body).get('grant_type');
    fs.appendFileSync(process.env.EXO_MOCK_OAUTH_RECORD, JSON.stringify({ path: url.pathname,
      user_agent: headers.get('user-agent'), originator: headers.get('originator'), grant }) + '\\n');
    if (url.pathname === '/api/accounts/deviceauth/usercode')
      return new Response(JSON.stringify({ device_auth_id: 'device_synthetic', user_code: 'CODE-SYNTH', interval: 0 }), { status: 200 });
    if (url.pathname === '/api/accounts/deviceauth/token')
      return new Response(JSON.stringify({ authorization_code: 'authorization_synthetic', code_verifier: 'verifier_synthetic' }), { status: 200 });
    if (url.pathname !== '/oauth/token') throw new Error('unexpected OAuth path');
    grants++;
    const access = jwtPart({ alg: 'none' }) + '.' + jwtPart({ 'https://api.openai.com/auth': { chatgpt_account_id: 'acct_synthetic' }, grant: grants }) + '.signature';
    return new Response(JSON.stringify({ access_token: access, refresh_token: 'synthetic_refresh_' + grants,
      expires_in: 3600, token_type: 'Bearer' }), { status: 200 });
  }
  if (!['127.0.0.1', 'localhost', '::1', '[::1]'].includes(url.hostname)) throw new Error('non-loopback request blocked');
  return originalFetch(input, init);
};
`, { mode: 0o600 });
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
  let errors = '';
  child.stderr.on('data', (c) => { errors += c; });
  const ready = new Promise((resolve, reject) => {
    let output = '';
    const timer = setTimeout(() => reject(new Error('serve timeout')), 6000);
    child.stdout.on('data', (c) => {
      output += c;
      const end = output.indexOf('\n');
      if (end >= 0) { clearTimeout(timer); resolve(JSON.parse(output.slice(0, end))); }
    });
    child.on('close', (code) => { if (!output.includes('\n')) { clearTimeout(timer); reject(new Error(`serve exit ${code}: ${errors}`)); } });
  });
  return { child, ready, stderr: () => errors };
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

async function startGateOwner(lock, { mainLock = false, old = false } = {}) {
  const child = spawn(process.execPath, ['--input-type=module', '-e', `
import fs from 'node:fs';
const lock = process.env.EXO_TEST_LOCK;
if (process.env.EXO_TEST_MAIN_LOCK === '1') {
  fs.mkdirSync(lock, { mode: 0o700 });
  fs.writeFileSync(lock + '/owner', String(process.pid), { mode: 0o600 });
}
const gate = lock + '.recovery';
fs.mkdirSync(gate, { mode: 0o700 });
fs.writeFileSync(gate + '/owner', String(process.pid), { mode: 0o600, flag: 'wx' });
if (process.env.EXO_TEST_OLD_GATE === '1') {
  const old = new Date(Date.now() - 60000);
  fs.utimesSync(gate, old, old);
}
process.stdout.write('ready\\n');
setInterval(() => {}, 1000);
`], { env: { ...process.env, EXO_TEST_LOCK: lock, EXO_TEST_MAIN_LOCK: mainLock ? '1' : '0',
    EXO_TEST_OLD_GATE: old ? '1' : '0' }, stdio: ['ignore', 'pipe', 'pipe'] });
  let stderr = '';
  child.stderr.on('data', (chunk) => { stderr += chunk; });
  await new Promise((resolve, reject) => {
    let output = '';
    const timer = setTimeout(() => reject(new Error('gate owner did not start')), 3000);
    child.stdout.on('data', (chunk) => {
      output += chunk;
      if (output.includes('ready\n')) { clearTimeout(timer); resolve(); }
    });
    child.once('close', (code) => { if (!output.includes('ready\n')) { clearTimeout(timer); reject(new Error(`gate owner exit ${code}: ${stderr}`)); } });
  });
  return child;
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
      if (req.headers['session-id'] === 'token-error') {
        res.writeHead(500, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ error: { message: `provider echoed ${req.headers.authorization}` } }));
        return;
      }
      if (['structured-error', 'unknown-code'].includes(req.headers['session-id'])) {
        res.writeHead(400, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ error: {
          code: req.headers['session-id'] === 'structured-error' ? 'unsupported_originator' : 'unreviewed_secret_code',
          message: 'originator rejected; diagnostic opaqueSecret_9Qx7v2Lm5pR8',
        } }));
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
      if (req.headers['session-id'] === 'jwt-tool') {
        const call = { type: 'function_call', id: 'fc_jwt', call_id: 'jwt', name: 'alpha',
          arguments: JSON.stringify({ value: 'abcdefghijklmnop.qrstuvwxyzABCDEF.ghijklmnopqrstuv' }), status: 'completed' };
        events.push({ type: 'response.output_item.added', output_index: 0,
          item: { ...call, arguments: '', status: 'in_progress' } });
        events.push({ type: 'response.function_call_arguments.delta', item_id: call.id, output_index: 0, delta: call.arguments });
        events.push({ type: 'response.function_call_arguments.done', item_id: call.id, output_index: 0, arguments: call.arguments });
        events.push({ type: 'response.output_item.done', output_index: 0, item: call });
        items.push(call);
      } else if (req.headers['session-id'] === 'interleaved') {
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

test('symlinked home, secrets, run, credential and lock are configuration errors before requests', async () => {
  const mock = await startMock(46108);
  const url = 'http://127.0.0.1:46108/backend-api';
  const cases = [];
  const linkedHomeTarget = mkhome('linked-home-target', true);
  seed(linkedHomeTarget);
  const linkedHome = path.join(trial, 'linked-home');
  fs.symlinkSync(linkedHomeTarget, linkedHome, 'dir');
  cases.push(linkedHome);
  const linkedSecrets = mkhome('linked-secrets', true);
  fs.renameSync(path.join(linkedSecrets, 'secrets'), path.join(linkedSecrets, 'original-secrets'));
  fs.symlinkSync(path.join(linkedSecrets, 'original-secrets'), path.join(linkedSecrets, 'secrets'), 'dir');
  cases.push(linkedSecrets);
  const linkedRun = mkhome('linked-run', true);
  fs.mkdirSync(path.join(linkedRun, 'run-target'));
  fs.symlinkSync(path.join(linkedRun, 'run-target'), path.join(linkedRun, 'run'), 'dir');
  cases.push(linkedRun);
  const linkedCredential = mkhome('linked-credential', true);
  const target = path.join(linkedCredential, 'synthetic-credential.json');
  fs.writeFileSync(target, JSON.stringify(credential()), { mode: 0o600 });
  fs.symlinkSync(target, path.join(linkedCredential, 'secrets', 'openai-codex.json'));
  cases.push(linkedCredential);
  const linkedLock = mkhome('linked-lock', true);
  const lockTarget = path.join(linkedLock, 'lock-target');
  fs.mkdirSync(lockTarget, { mode: 0o700 });
  fs.symlinkSync(lockTarget, path.join(linkedLock, 'secrets', 'openai-codex.json.lock'), 'dir');
  cases.push(linkedLock);
  try {
    for (const home of cases) {
      for (const override of [undefined, url]) {
        const result = await run(['status'], { EXO_MODEL_HOME: home, EXO_CODEX_BASE_URL: override });
        assert.equal(result.code, 1, home);
        assert.match(result.stderr, /"kind":"config"/, home);
      }
    }
    assert.equal(mock.requests.length, 0);
  } finally { await new Promise((resolve) => mock.server.close(resolve)); }
});

test('hard-linked credentials are configuration errors before fixture requests', async () => {
  const mock = await startMock(46111);
  const url = 'http://127.0.0.1:46111/backend-api';
  const home = mkhome('hard-linked-credential', true);
  const file = seed(home);
  fs.linkSync(file, path.join(trial, 'credential-alias.json'));
  const syntheticUserHome = path.join(trial, 'synthetic-user-home');
  const defaultCredential = path.join(syntheticUserHome, '.exomachina', 'model-broker', 'secrets', 'openai-codex.json');
  fs.mkdirSync(path.dirname(defaultCredential), { recursive: true, mode: 0o700 });
  fs.writeFileSync(defaultCredential, JSON.stringify(credential()), { mode: 0o600 });
  const aliasHome = mkhome('default-inode-alias', true);
  fs.linkSync(defaultCredential, path.join(aliasHome, 'secrets', 'openai-codex.json'));
  try {
    for (const [modelHome, override] of [[home, undefined], [home, url], [aliasHome, url]]) {
      const result = await run(['serve'], { HOME: syntheticUserHome, EXO_MODEL_HOME: modelHome,
        EXO_CODEX_BASE_URL: override });
      assert.equal(result.code, 1, result.stdout + result.stderr);
      assert.match(result.stderr, /"kind":"config"/);
    }
    assert.equal(mock.requests.length, 0);
  } finally { await new Promise((resolve) => mock.server.close(resolve)); }
});

test('OAuth callback host is loopback and browser authorize URL changes only originator', async () => {
  const home = mkhome('callback-host');
  const record = path.join(trial, 'callback-oauth-record.jsonl');
  const result = await run(['login', '--browser'], { EXO_MODEL_HOME: home,
    PI_OAUTH_CALLBACK_HOST: '0.0.0.0', NODE_OPTIONS: `--import=${recordingPreload}`,
    EXO_MOCK_OAUTH_RECORD: record, EXO_CODEX_BASE_URL: undefined });
  assert.equal(result.code, 1);
  assert.match(result.stderr, /"kind":"config"/);
  assert.equal(fs.existsSync(record), false);
  const original = 'https://auth.openai.com/oauth/authorize?client_id=app_EMoamEEZ73f0CkXaXp7hrann&state=synthetic&originator=pi&scope=openid';
  assert.equal(rewriteAuthorizeOriginator(original), original.replace('originator=pi', 'originator=exomachina'));
  assert.equal(new URL(rewriteAuthorizeOriginator(original)).searchParams.get('originator'), 'exomachina');
});

test('device start, poll, exchange and refresh carry broker identity', async () => {
  const record = path.join(trial, 'identity-oauth-record.jsonl');
  const env = { NODE_OPTIONS: `--import=${recordingPreload}`, EXO_MOCK_OAUTH_RECORD: record, EXO_CODEX_BASE_URL: undefined };
  const loginHome = mkhome('device-login');
  const login = await run(['login'], { ...env, EXO_MODEL_HOME: loginHome });
  assert.equal(login.code, 0, login.stderr);
  assert.equal(JSON.parse(login.stdout).signed_in, true);
  assert.match(login.stderr, /CODE-SYNTH/);
  const refreshHome = mkhome('identity-refresh', true); seed(refreshHome, credential(Date.now() - 1000));
  const service = launch(refreshHome, { ...env, EXO_CODEX_BASE_URL: 'http://127.0.0.1:46109/backend-api' });
  try {
    await service.ready;
    const result = (await transact(refreshHome, { id: 'identity-refresh', op: 'refresh' }))[0];
    assert.equal(result.refresh?.refreshed, true, JSON.stringify(result));
    const requests = fs.readFileSync(record, 'utf8').trim().split('\n').map(JSON.parse);
    assert.deepEqual(requests.map((request) => request.path), [
      '/api/accounts/deviceauth/usercode', '/api/accounts/deviceauth/token', '/oauth/token', '/oauth/token']);
    assert.deepEqual(requests.filter((request) => request.path === '/oauth/token').map((request) => request.grant),
      ['authorization_code', 'refresh_token']);
    for (const request of requests) {
      assert.equal(request.originator, 'exomachina');
      assert.equal(request.user_agent, 'exomachina-model-broker/0.1.0 (pi-ai/0.87.1)');
    }
  } finally { await stop(service.child); }
});

test('JWT-shaped tool arguments survive while provider token errors stay fixed after rotation', async () => {
  const mock = await startMock(46107);
  const home = mkhome('redaction', true);
  const initial = credential(); seed(home, initial);
  const service = launch(home, { EXO_CODEX_BASE_URL: 'http://127.0.0.1:46107/backend-api' });
  try {
    await service.ready;
    const args = 'abcdefghijklmnop.qrstuvwxyzABCDEF.ghijklmnopqrstuv';
    const reply = await transact(home, { id: 'jwt-tool', op: 'stream', model: 'gpt-6-sol', session: 'jwt-tool', context });
    assert.ok(reply.at(-1).done, JSON.stringify(reply.at(-1)));
    assert.ok(JSON.stringify(reply.filter((line) => line.ev)).includes(args));
    assert.ok(JSON.stringify(reply.at(-1).done).includes(args));
    const replacement = credential();
    replacement.access = `${jwtPart({ alg: 'none' })}.${jwtPart({
      'https://api.openai.com/auth': { chatgpt_account_id: 'acct_synthetic' }, rotation: 'second',
    })}.signature`;
    const temporary = path.join(home, 'secrets', 'replacement.json');
    fs.writeFileSync(temporary, JSON.stringify(replacement), { mode: 0o600 });
    fs.renameSync(temporary, path.join(home, 'secrets', 'openai-codex.json'));
    const failure = (await transact(home, { id: 'token-error', op: 'stream', model: 'gpt-6-sol',
      session: 'token-error', context })).at(-1);
    assert.equal(failure.error?.kind, 'provider', JSON.stringify(failure));
    assert.ok(!JSON.stringify(failure).includes(replacement.access));
    assert.deepEqual(failure.error, { kind: 'provider', message: 'Codex provider request failed', status: 500, code: 'other' });
    const events = fs.readFileSync(path.join(home, 'broker-events.jsonl'), 'utf8');
    assert.ok(!events.includes(initial.access) && !events.includes(replacement.access));
  } finally { await stop(service.child); await new Promise((resolve) => mock.server.close(resolve)); }
});

test('provider error replies and logs expose only fixed text, HTTP status and allowlisted code', async () => {
  const mock = await startMock(46110);
  const home = mkhome('structured-errors', true); seed(home);
  const service = launch(home, { EXO_CODEX_BASE_URL: 'http://127.0.0.1:46110/backend-api' });
  const planted = 'opaqueSecret_9Qx7v2Lm5pR8';
  try {
    await service.ready;
    for (const [session, code] of [['structured-error', 'unsupported_originator'], ['unknown-code', 'other']]) {
      const reply = (await transact(home, { id: session, op: 'stream', model: 'gpt-6-sol', session, context })).at(-1);
      assert.deepEqual(reply.error, { kind: 'provider', message: 'Codex provider request failed', status: 400, code });
      assert.ok(!JSON.stringify(reply).includes(planted));
    }
    const events = fs.readFileSync(path.join(home, 'broker-events.jsonl'), 'utf8').trim().split('\n').map(JSON.parse);
    const errors = events.filter((event) => event.event === 'error');
    assert.equal(errors.length, 2);
    assert.deepEqual(errors.map(({ kind, status, code }) => ({ kind, status, code })), [
      { kind: 'provider', status: 400, code: 'unsupported_originator' },
      { kind: 'provider', status: 400, code: 'other' },
    ]);
    for (const event of errors) assert.deepEqual(Object.keys(event).sort(), ['at', 'code', 'event', 'kind', 'status']);
    assert.ok(!JSON.stringify(events).includes(planted));
    assert.ok(!service.stderr().includes(planted));
  } finally { await stop(service.child); await new Promise((resolve) => mock.server.close(resolve)); }
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

test('two stale-lock recoverers preserve the new live lock and serialize callbacks', async () => {
  const lock = path.join(trial, 'interleaved-dead-owner.lock');
  fs.mkdirSync(lock, { mode: 0o700 });
  fs.writeFileSync(path.join(lock, 'owner'), '99999999', { mode: 0o600 });
  let releaseSecond, releaseFirst;
  const secondMayProceed = new Promise((resolve) => { releaseSecond = resolve; });
  const firstMayExit = new Promise((resolve) => { releaseFirst = resolve; });
  let firstEntered, secondObservedRecovery;
  const inFirst = new Promise((resolve) => { firstEntered = resolve; });
  const secondRecovered = new Promise((resolve) => { secondObservedRecovery = resolve; });
  const entries = [];
  const first = withPidLock(lock, async () => {
    entries.push('first-enter');
    firstEntered();
    await firstMayExit;
    entries.push('first-exit');
  }, 2000);
  const second = withPidLock(lock, async () => { entries.push('second-enter'); }, 2000, {
    afterDeadOwnerObserved: () => secondMayProceed,
    afterRecoveryAttempt: secondObservedRecovery,
  });
  try {
    await inFirst;
    const live = fs.statSync(lock);
    assert.equal(fs.readFileSync(path.join(lock, 'owner'), 'utf8'), String(process.pid));
    releaseSecond();
    await secondRecovered;
    assert.equal(fs.statSync(lock).ino, live.ino);
    assert.deepEqual(entries, ['first-enter']);
  } finally { releaseSecond(); releaseFirst(); }
  await Promise.all([first, second]);
  assert.deepEqual(entries, ['first-enter', 'first-exit', 'second-enter']);
  assert.equal(fs.existsSync(lock), false);
});

test('a killed recovery-gate owner is reclaimed and the dead lock proceeds', async () => {
  const lock = path.join(trial, 'killed-recovery-gate.lock');
  const child = await startGateOwner(lock, { mainLock: true });
  child.kill('SIGKILL');
  await new Promise((resolve) => child.once('close', resolve));
  let entered = false;
  await withPidLock(lock, async () => { entered = true; }, 1500);
  assert.equal(entered, true);
  assert.equal(fs.existsSync(lock), false);
  assert.equal(fs.existsSync(`${lock}.recovery`), false);
});

test('an old recovery gate with a live owner is preserved', async () => {
  const lock = path.join(trial, 'live-old-recovery-gate.lock');
  const child = await startGateOwner(lock, { old: true });
  const gate = `${lock}.recovery`;
  const inode = fs.lstatSync(gate).ino;
  try {
    await assert.rejects(withPidLock(lock, async () => {}, 150), /credential lock unavailable/);
    assert.equal(fs.lstatSync(gate).ino, inode);
    assert.equal(fs.readFileSync(path.join(gate, 'owner'), 'utf8'), String(child.pid));
    assert.equal(fs.existsSync(lock), false);
  } finally {
    child.kill('SIGKILL');
    await new Promise((resolve) => child.once('close', resolve));
  }
});

test('malformed recovery-gate owner needs age before reclamation', async () => {
  const freshLock = path.join(trial, 'fresh-malformed-recovery-gate.lock');
  const freshGate = `${freshLock}.recovery`;
  fs.mkdirSync(freshGate, { mode: 0o700 });
  fs.writeFileSync(path.join(freshGate, 'owner'), 'not-a-pid', { mode: 0o600 });
  const inode = fs.lstatSync(freshGate).ino;
  await assert.rejects(withPidLock(freshLock, async () => {}, 150), /credential lock unavailable/);
  assert.equal(fs.lstatSync(freshGate).ino, inode);
  assert.equal(fs.readFileSync(path.join(freshGate, 'owner'), 'utf8'), 'not-a-pid');
  const oldLock = path.join(trial, 'old-malformed-recovery-gate.lock');
  const oldGate = `${oldLock}.recovery`;
  fs.mkdirSync(oldGate, { mode: 0o700 });
  fs.writeFileSync(path.join(oldGate, 'owner'), 'not-a-pid', { mode: 0o600 });
  const old = new Date(Date.now() - 60000);
  fs.utimesSync(oldGate, old, old);
  let entered = false;
  await withPidLock(oldLock, async () => { entered = true; }, 1500);
  assert.equal(entered, true);
  assert.equal(fs.existsSync(oldGate), false);
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
    const linkedCopy = path.join(trial, 'linked-copy.json');
    fs.symlinkSync(planted, linkedCopy);
    const linkedDirectoryTarget = path.join(trial, 'linked-directory-target');
    fs.mkdirSync(linkedDirectoryTarget, { mode: 0o700 });
    fs.writeFileSync(path.join(linkedDirectoryTarget, 'token.txt'), after.access);
    fs.symlinkSync(linkedDirectoryTarget, path.join(linkedDirectoryTarget, 'cycle'), 'dir');
    const linkedDirectory = path.join(trial, 'linked-directory');
    fs.symlinkSync(linkedDirectoryTarget, linkedDirectory, 'dir');
    const linkedCredential = path.join(trial, 'linked-canonical-credential.json');
    fs.symlinkSync(file, linkedCredential);
    const scan = await run(['leak-scan', linkedCredential, linkedCopy, linkedDirectory, home, planted, bare],
      { EXO_MODEL_HOME: home, ...env });
    assert.equal(scan.code, 0);
    const result = JSON.parse(scan.stdout);
    assert.deepEqual(result.excluded, [fs.realpathSync(file)]);
    assert.ok(result.hits.some((hit) => hit.path === linkedCopy && hit.kind === 'access'));
    assert.ok(result.hits.some((hit) => hit.path === path.join(linkedDirectory, 'token.txt') && hit.kind === 'access'));
    assert.ok(result.hits.some((hit) => hit.path === bare && hit.kind === 'access'));
    assert.ok(!result.hits.some((hit) => hit.path === planted || hit.path === linkedCredential));
    assert.ok(result.files_scanned >= 3);
    assert.ok(!scan.stdout.includes(after.access));
  } finally { await stop(service.child); }
});

test('leak scan reports a hard link created after its credential read', async () => {
  const home = mkhome('inflight-hardlink-scan');
  const file = seed(home);
  const linked = path.join(trial, 'inflight-credential-hardlink.json');
  const linkPreload = path.join(trial, 'link-after-credential-read.mjs');
  fs.writeFileSync(linkPreload, `
import fs from 'node:fs';
const originalRead = fs.readFileSync;
let linked = false;
fs.readFileSync = (...args) => {
  const data = originalRead.apply(fs, args);
  if (!linked && typeof args[0] === 'number') {
    const opened = fs.fstatSync(args[0]);
    const credential = fs.lstatSync(process.env.EXO_SCAN_CREDENTIAL);
    if (opened.dev === credential.dev && opened.ino === credential.ino) {
      fs.linkSync(process.env.EXO_SCAN_CREDENTIAL, process.env.EXO_SCAN_LINK);
      linked = true;
    }
  }
  return data;
};
`, { mode: 0o600 });
  const scan = await run(['leak-scan', linked], { EXO_MODEL_HOME: home, EXO_CODEX_BASE_URL: undefined,
    NODE_OPTIONS: `--import=${linkPreload}`, EXO_SCAN_CREDENTIAL: file, EXO_SCAN_LINK: linked });
  assert.equal(scan.code, 0, scan.stderr);
  assert.equal(fs.lstatSync(file).nlink, 2);
  const result = JSON.parse(scan.stdout);
  assert.ok(result.hits.some((hit) => hit.path === linked && hit.kind === 'access'));
  assert.deepEqual(result.excluded, []);
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
