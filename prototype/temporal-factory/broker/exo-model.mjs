#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import net from 'node:net';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { createInterface } from 'node:readline/promises';
import { FileCredentialStore, checkCredentialPath, ownerOnlyDirectory, ownerOnlyFile, pidAlive,
  rewriteAuthorizeOriginator, withPidLock } from './lib/store.mjs';

const VERSION = '0.1.0';
const PI_VERSION = '0.87.1';
const PROVIDER = 'openai-codex';
const ORIGINATOR = 'exomachina';
const defaultHome = path.resolve(path.join(os.homedir(), '.exomachina', 'model-broker'));
const home = path.resolve(process.env.EXO_MODEL_HOME || defaultHome);
const secretDir = path.join(home, 'secrets');
const runDir = path.join(home, 'run');
const credentialFile = path.join(secretDir, 'openai-codex.json');
const socketFile = path.join(runDir, 'broker.sock');
const readyFile = path.join(runDir, 'broker-ready.json');
const eventFile = path.join(home, 'broker-events.jsonl');
const baseUrl = process.env.EXO_CODEX_BASE_URL;
const commandName = process.argv[2];
function configFailure() {
  process.stderr.write(JSON.stringify({ error: { kind: 'config', message: 'invalid model broker configuration' } }) + '\n');
  process.exit(1);
}
function canonical(file) {
  try { return fs.realpathSync.native(file); }
  catch {
    const parent = path.dirname(file);
    return parent === file ? path.resolve(file) : path.join(canonical(parent), path.basename(file));
  }
}
if (Buffer.byteLength(socketFile) > 103) configFailure();
const fixtureMarker = path.join(home, 'FIXTURE_STORE');
const fixtureMarked = (() => { try { return fs.statSync(fixtureMarker).isFile(); } catch { return false; } })();
if (baseUrl !== undefined) {
  let target;
  try { target = new URL(baseUrl); } catch { configFailure(); }
  const isolated = Boolean(process.env.EXO_MODEL_HOME) && canonical(home) !== canonical(defaultHome);
  const loopback = ['127.0.0.1', '::1', '[::1]', 'localhost'].includes(target.hostname) &&
    ['http:', 'https:'].includes(target.protocol);
  const realHome = canonical(home);
  const ownSecrets = canonical(secretDir) === path.join(realHome, 'secrets');
  const ownCredential = canonical(credentialFile) === path.join(realHome, 'secrets', 'openai-codex.json');
  if (!isolated || !fixtureMarked || !loopback || !ownSecrets || !ownCredential) configFailure();
  // Compare metadata only. The default credential is never opened or read.
  try {
    const fixture = fs.lstatSync(credentialFile, { throwIfNoEntry: false });
    const installed = fs.lstatSync(path.join(defaultHome, 'secrets', 'openai-codex.json'),
      { throwIfNoEntry: false });
    if (fixture && installed && fixture.dev === installed.dev && fixture.ino === installed.ino) configFailure();
  } catch { configFailure(); }
}
if (commandName === 'login' && fixtureMarked) configFailure();
if (commandName === 'login' && process.env.PI_OAUTH_CALLBACK_HOST !== undefined &&
    !['127.0.0.1', 'localhost', '::1'].includes(process.env.PI_OAUTH_CALLBACK_HOST)) configFailure();

try {
  for (const dir of [home, secretDir, runDir]) ownerOnlyDirectory(dir);
  ownerOnlyFile(credentialFile, true);
  const credentialLock = `${credentialFile}.lock`;
  if (fs.lstatSync(credentialLock, { throwIfNoEntry: false })) {
    ownerOnlyDirectory(credentialLock);
    ownerOnlyFile(path.join(credentialLock, 'owner'));
  }
  checkCredentialPath(credentialFile);
} catch { configFailure(); }
const store = new FileCredentialStore(credentialFile);

function account(credential) {
  return typeof credential?.accountId === 'string'
    ? `sha256:${createHash('sha256').update(credential.accountId).digest('hex').slice(0, 12)}` : null;
}
async function status() {
  const c = await store.read(PROVIDER);
  const signed_in = c?.type === 'oauth';
  return { signed_in, account: signed_in ? account(c) : null,
    expires_at: signed_in ? c.expires : null,
    expired: signed_in ? Date.now() >= c.expires : null };
}
function log(event, fields = {}) {
  const line = JSON.stringify({ at: new Date().toISOString(), event, ...fields });
  fs.appendFileSync(eventFile, redact(line, true) + '\n', { mode: 0o600 });
  fs.chmodSync(eventFile, 0o600);
}
function partsOf(c) {
  const values = [];
  for (const key of ['access', 'refresh', 'id_token', 'idToken']) {
    if (typeof c?.[key] === 'string' && c[key].length) values.push([key, c[key]]);
  }
  return values;
}
const redactionTokens = new Set();
let credentialStamp;
function currentRedactionTokens() {
  checkCredentialPath(credentialFile);
  const stat = fs.lstatSync(credentialFile, { throwIfNoEntry: false });
  const stamp = stat ? `${stat.dev}:${stat.ino}:${stat.mtimeNs ?? stat.mtimeMs}` : 'missing';
  if (stamp !== credentialStamp) {
    const credential = store.readCurrent();
    for (const [, token] of partsOf(credential)) redactionTokens.add(token);
    credentialStamp = stamp;
  }
  return redactionTokens;
}
function redact(value, errorPayload = false) {
  let output = String(value);
  for (const token of currentRedactionTokens()) output = output.split(token).join('[redacted]');
  return errorPayload ? output.replace(/\b[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\b/g, '[redacted-jwt]') : output;
}
function safeLine(value) { return redact(JSON.stringify(value), Boolean(value.error)) + '\n'; }
function allowed(url) {
  const u = new URL(url);
  const loopback = ['127.0.0.1', '::1', '[::1]', 'localhost'].includes(u.hostname);
  if (loopback && ['http:', 'https:'].includes(u.protocol)) return true;
  return u.protocol === 'https:' && ['chatgpt.com', 'auth.openai.com'].includes(u.hostname);
}
const underlyingFetch = globalThis.fetch;
globalThis.fetch = (input, init = {}) => {
  const target = typeof input === 'string' || input instanceof URL ? input : input.url;
  if (!allowed(target)) throw new Error('egress host refused');
  const url = new URL(target);
  if (url.hostname === 'auth.openai.com') {
    const headers = new Headers(input instanceof Request ? input.headers : undefined);
    for (const [name, value] of new Headers(init.headers)) headers.set(name, value);
    headers.set('originator', ORIGINATOR);
    headers.set('User-Agent', `exomachina-model-broker/${VERSION} (pi-ai/${PI_VERSION})`);
    return underlyingFetch(input, { ...init, headers, redirect: 'manual' });
  }
  return underlyingFetch(input, { ...init, redirect: 'manual' });
};
function honestFetch(url, init = {}) {
  const headers = new Headers(init.headers);
  headers.set('originator', ORIGINATOR);
  headers.set('User-Agent', `exomachina-model-broker/${VERSION} (pi-ai/${PI_VERSION})`);
  return globalThis.fetch(url, { ...init, headers });
}

const { createModels, normalizeContext, ModelsError } = await import('@earendil-works/pi-ai');
const { openaiCodexProvider } = await import('@earendil-works/pi-ai/providers/openai-codex');
const { stream } = await import('@earendil-works/pi-ai/api/openai-codex-responses');
const provider = openaiCodexProvider();
const models = createModels({ credentials: store, authContext: { env: async () => undefined, fileExists: async () => false } });
models.setProvider(provider);

function classify(error, statusCode) {
  const message = String(error?.errorMessage || error?.message || error || '');
  if (error instanceof ModelsError && ['auth', 'oauth'].includes(error.code)) return 'reauth_required';
  if (statusCode === 401 || statusCode === 403 || /\b(401|403|invalid_grant|unauthori[sz]ed|forbidden)\b/i.test(message)) return 'reauth_required';
  if (/usage.limit|usage_limit|quota|usage_not_included/i.test(message)) return 'quota';
  if (statusCode === 429 || /\b429\b|rate.limit|rate_limit/i.test(message)) return 'rate_limit';
  if (error?.name === 'AbortError') return 'aborted';
  if (error instanceof ModelsError && error.code === 'provider') return 'config';
  return 'provider';
}
const publicMessages = {
  reauth_required: 'Codex subscription sign-in required', rate_limit: 'Codex rate limit reached',
  quota: 'Codex subscription usage limit reached', config: 'broker configuration error',
  provider: 'Codex provider request failed', aborted: 'model request aborted', broker: 'broker request failed',
};
const safeProviderCodes = new Set([
  'usage_limit_reached', 'usage_not_included', 'rate_limit_exceeded', 'invalid_request_error',
  'unsupported_originator', 'invalid_grant', 'token_expired', 'insufficient_quota',
]);
function boundedCode(value) {
  return typeof value === 'string' && /^[a-z0-9_.-]{1,64}$/.test(value) && safeProviderCodes.has(value)
    ? value : 'other';
}
function structuredCode(value) {
  return value?.error?.code ?? value?.error?.type ?? value?.code ?? value?.type;
}
function errReply(id, kind, status, code) {
  const error = { kind, message: publicMessages[kind] || publicMessages.broker };
  if (Number.isInteger(status) && status >= 100 && status <= 599) error.status = status;
  if (code !== undefined) error.code = boundedCode(code);
  return { id, error };
}
function toPiMessages(messages, model) {
  return messages.map((m) => m.role !== 'assistant' ? { ...m, timestamp: 0 } : {
    ...m, api: model.api, provider: model.provider, model: model.id, timestamp: 0,
    usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
  });
}
const active = new Map();
async function handleStream(req, send, owned) {
  const key = `${owned.id}:${String(req.id)}`;
  const started = Date.now();
  let httpStatus;
  let providerCode;
  const abort = new AbortController();
  active.set(key, abort);
  owned.streams.add(key);
  let terminal = false;
  const finish = async (reply, event, kind) => {
    if (terminal) return;
    terminal = true;
    await send(reply);
    if (event === 'error') {
      const { kind: errorKind, status, code } = reply.error;
      log('error', { kind: errorKind, ...(status === undefined ? {} : { status }),
        ...(code === undefined ? {} : { code }) });
    } else log(event, { id: String(req.id), model: String(req.model || ''),
      session: String(req.session || ''), duration_ms: Date.now() - started });
  };
  try {
    const credential = await store.read(PROVIDER);
    if (credential?.type !== 'oauth') return finish(errReply(req.id, 'reauth_required'), 'error', 'reauth_required');
    const auth = await models.getAuth(PROVIDER);
    if (!auth?.auth?.apiKey) return finish(errReply(req.id, 'reauth_required'), 'error', 'reauth_required');
    const catalog = provider.getModels().find((m) => m.id === req.model);
    if (!catalog) return finish(errReply(req.id, 'config'), 'error', 'config');
    const model = { ...catalog, baseUrl: baseUrl || catalog.baseUrl };
    const context = normalizeContext({ systemPrompt: req.context?.systemPrompt,
      messages: toPiMessages(req.context?.messages || [], model), tools: req.context?.tools || [] });
    const providerFetch = async (url, init) => {
      const response = await honestFetch(url, init);
      if (!response.ok) {
        const payload = await response.clone().json().catch(() => null);
        providerCode = boundedCode(structuredCode(payload));
      }
      return response;
    };
    const events = stream(model, context, { apiKey: auth.auth.apiKey, transport: 'sse', fetch: providerFetch,
      sessionId: req.session, maxRetries: 0, signal: abort.signal,
      reasoningEffort: req.options?.reasoningEffort || 'low',
      onResponse: (response) => { httpStatus = response.status; } });
    for await (const ev of events) {
      if (ev.type === 'done') { await finish({ id: req.id, done: ev.message }, 'stream'); return; }
      if (ev.type === 'error') {
        const kind = classify(ev.error, httpStatus);
        await finish(errReply(req.id, kind, httpStatus, providerCode ?? structuredCode(ev.error)), 'error', kind); return;
      }
      const e = { type: ev.type, contentIndex: ev.contentIndex };
      if (ev.delta !== undefined) e.delta = ev.delta;
      if (ev.type === 'toolcall_start') {
        const call = ev.partial?.content?.[ev.contentIndex];
        e.toolCall = { id: call?.id, name: call?.name };
      }
      await send({ id: req.id, ev: e });
    }
    await finish(errReply(req.id, abort.signal.aborted ? 'aborted' : 'broker'), 'error', abort.signal.aborted ? 'aborted' : 'broker');
  } catch (error) {
    const kind = classify(error, httpStatus);
    await finish(errReply(req.id, abort.signal.aborted ? 'aborted' : kind, httpStatus,
      providerCode ?? structuredCode(error)), 'error', abort.signal.aborted ? 'aborted' : kind);
  } finally { active.delete(key); owned.streams.delete(key); }
}
async function forcedRefresh() {
  const before = await store.read(PROVIDER);
  if (before?.type !== 'oauth') throw Object.assign(new Error('sign-in required'), { kind: 'reauth_required' });
  await store.modify(PROVIDER, async (current) => {
    if (current?.type !== 'oauth') throw Object.assign(new Error('sign-in required'), { kind: 'reauth_required' });
    return provider.auth.oauth.refresh(current, new AbortController().signal);
  });
  const after = await store.read(PROVIDER);
  log('refresh', { expires_at_before: before.expires, expires_at_after: after.expires });
  return { refreshed: true, expires_at_before: before.expires, expires_at_after: after.expires };
}
async function request(op, timeoutMs = 1500) {
  return new Promise((resolve, reject) => {
    const conn = net.createConnection(socketFile);
    let data = '';
    const timer = setTimeout(() => conn.destroy(new Error('broker timeout')), timeoutMs);
    conn.on('connect', () => conn.write(JSON.stringify({ id: 'cli', ...op }) + '\n'));
    conn.on('data', (chunk) => {
      data += chunk;
      const end = data.indexOf('\n');
      if (end >= 0) {
        clearTimeout(timer); conn.end();
        try { resolve(JSON.parse(data.slice(0, end))); } catch { reject(new Error('bad broker reply')); }
      }
    });
    conn.on('error', (error) => { clearTimeout(timer); reject(error); });
    conn.on('close', () => { clearTimeout(timer); if (!data.includes('\n')) reject(new Error('broker closed')); });
  });
}
async function healthOrNull() { try { const reply = await request({ op: 'health' }, 600); return reply.health || null; } catch { return null; } }
async function socketRefused() {
  try { await request({ op: 'health' }, 600); return false; }
  catch (error) { return error.code === 'ECONNREFUSED'; }
}
async function serve() {
  return withPidLock(path.join(runDir, 'broker-start.lock'), async () => {
    const live = await healthOrNull();
    if (live) { log('attach', { pid: live.pid }); console.log(JSON.stringify({ attached: true, ...live })); return; }
    if (fs.existsSync(socketFile)) {
      const stat = fs.lstatSync(socketFile);
      if (!stat.isSocket()) throw new Error('broker socket path is occupied');
      let prior;
      try { prior = JSON.parse(fs.readFileSync(readyFile, 'utf8')); } catch { prior = null; }
      if (prior?.pid && pidAlive(prior.pid)) throw new Error('broker socket owner is still alive');
      // ECONNREFUSED proves that no process currently owns this socket. A
      // nonresponsive live listener may time out, but is never removed.
      if (!(await socketRefused())) throw new Error('broker socket is not proven stale');
      fs.unlinkSync(socketFile);
    }
    const server = net.createServer((conn) => {
      const owned = { id: Math.random().toString(36).slice(2), streams: new Set() };
      let buffer = '';
      const send = async (value) => {
        try { if (conn.writable) conn.write(safeLine(value)); }
        catch { conn.destroy(); }
      };
      conn.on('close', () => { for (const key of owned.streams) active.get(key)?.abort(); });
      conn.on('data', (chunk) => {
        buffer += chunk;
        for (let end; (end = buffer.indexOf('\n')) >= 0;) {
          const raw = buffer.slice(0, end); buffer = buffer.slice(end + 1);
          let req;
          try { req = JSON.parse(raw); } catch { void send(errReply(null, 'broker')); continue; }
          if (req.op === 'health') void status().then((s) => send({ id: req.id, health: {
            pid: process.pid, pi_ai: PI_VERSION, provider: PROVIDER, originator: ORIGINATOR,
            signed_in: s.signed_in, expires_at: s.expires_at, account: s.account } }))
            .catch(() => send(errReply(req.id, 'broker')));
          else if (req.op === 'cancel') active.get(`${owned.id}:${String(req.id)}`)?.abort();
          else if (req.op === 'stream') void handleStream(req, send, owned);
          else if (req.op === 'refresh') void forcedRefresh().then((refresh) => send({ id: req.id, refresh }))
            .catch((error) => send(errReply(req.id, error.kind || classify(error))));
          else void send(errReply(req.id, 'config'));
        }
      });
    });
    await new Promise((resolve, reject) => {
      server.once('error', reject);
      server.listen(socketFile, () => { server.off('error', reject); resolve(); });
    });
    fs.chmodSync(socketFile, 0o600);
    const ready = { pid: process.pid, socket: socketFile, started_at: new Date().toISOString(),
      pi_ai: PI_VERSION, provider: PROVIDER, originator: ORIGINATOR };
    fs.writeFileSync(readyFile, JSON.stringify(ready), { mode: 0o600 });
    log('start', { pid: process.pid });
    console.log(JSON.stringify({ serving: true, ...ready }));
  });
}
async function ensureRunning() {
  let health = await healthOrNull();
  if (health) return health;
  const child = spawn(process.execPath, [new URL(import.meta.url).pathname, 'serve'],
    { env: process.env, detached: true, stdio: 'ignore' });
  child.unref();
  const deadline = Date.now() + 10000;
  while (Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 50));
    health = await healthOrNull();
    if (health) return health;
    if (child.exitCode !== null) break;
  }
  throw new Error('broker did not start');
}
async function login(browser) {
  const reader = createInterface({ input: process.stdin, output: process.stderr });
  try {
    const interaction = {
      prompt: async (prompt) => {
        if (prompt.type === 'select') return browser ? 'browser' : 'device_code';
        if (prompt.type === 'manual_code') return reader.question('Authorization code or redirect URL: ', { signal: prompt.signal });
        throw new Error('unexpected login prompt');
      },
      notify: (event) => {
        if (event.type === 'device_code') console.error(JSON.stringify({ verification_url: event.verificationUri, user_code: event.userCode }));
        if (event.type === 'auth_url') console.error(JSON.stringify({ authorization_url: rewriteAuthorizeOriginator(event.url) }));
      },
    };
    await models.login(PROVIDER, 'oauth', interaction);
    const s = await status();
    console.log(JSON.stringify({ signed_in: s.signed_in, account: s.account, expires_at: s.expires_at }));
  } finally { reader.close(); }
}
function scanTokens(c) {
  const needles = [];
  for (const [kind, token] of partsOf(c)) {
    needles.push([kind, Buffer.from(token)]);
    if (token.split('.').length === 3) {
      token.split('.').forEach((part, index) => { if (part.length >= 12) needles.push([`${kind}_jwt_fragment_${index}`, Buffer.from(part)]); });
    }
  }
  return needles;
}
function leakScan(paths) {
  const scanned = { files_scanned: 0, bytes_scanned: 0, excluded: [], hits: [] };
  const canonicalCredential = canonical(credentialFile);
  return store.read(PROVIDER).then((credential) => {
    const needles = scanTokens(credential);
    const visited = new Set();
    const visit = (file) => {
      let real;
      try { real = fs.realpathSync.native(file); }
      catch (error) {
        if (['ENOENT', 'ELOOP'].includes(error.code)) return;
        throw error;
      }
      if (visited.has(real)) return;
      visited.add(real);
      const stat = fs.statSync(file);
      if (stat.isDirectory()) { for (const name of fs.readdirSync(file)) visit(path.join(file, name)); return; }
      if (!stat.isFile()) return;
      if (real === canonicalCredential) {
        if (!scanned.excluded.includes(canonicalCredential)) scanned.excluded.push(canonicalCredential);
        return;
      }
      const data = fs.readFileSync(file);
      scanned.files_scanned += 1; scanned.bytes_scanned += data.length;
      for (const [kind, needle] of needles) if (data.includes(needle)) scanned.hits.push({ path: file, kind });
    };
    for (const file of paths) visit(path.resolve(file));
    return scanned;
  });
}

try {
  const [command, ...args] = process.argv.slice(2);
  if (command === 'serve') await serve();
  else if (command === 'status') console.log(JSON.stringify(await status()));
  else if (command === 'login') await login(args.includes('--browser'));
  else if (command === 'logout') { await models.logout(PROVIDER); console.log(JSON.stringify({ signed_in: false })); }
  else if (command === 'refresh') {
    await ensureRunning();
    const reply = await request({ op: 'refresh' }, 30000);
    if (reply.error) throw Object.assign(new Error(reply.error.message), { kind: reply.error.kind });
    console.log(JSON.stringify(reply.refresh));
  }
  else if (command === 'leak-scan' && args.length) console.log(JSON.stringify(await leakScan(args)));
  else throw new Error('usage: exo-model.mjs serve|login|status|refresh|logout|leak-scan');
} catch (error) {
  const kind = error.kind || classify(error);
  console.error(JSON.stringify({ error: { kind, message: publicMessages[kind] || publicMessages.broker } }));
  process.exitCode = 1;
}
