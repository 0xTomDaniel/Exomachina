import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { fileURLToPath } from 'node:url';

const mock = fileURLToPath(new URL('../testing/mock-codex.mjs', import.meta.url));
const followUp = 'The factory is waiting for a Director decision on this request. Please review the run and decide.';

function completed(stream) {
  const events = stream.split('\n\n').filter(Boolean).map((block) => {
    const data = block.split('\n').find((line) => line.startsWith('data: '));
    return JSON.parse(data.slice(6));
  });
  return events.find((event) => event.type === 'response.completed').response.output;
}

test('registered Director wait follow-up inspects and aborts the current revision', async () => {
  const port = 46460;
  const record = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'exo-sf-director-mock-')), 'requests.jsonl');
  const child = spawn(process.execPath, [mock, '--port', String(port), '--record', record, '--script', 'director'], {
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stderr = '';
  child.stderr.on('data', (chunk) => { stderr += chunk; });
  try {
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`Director mock startup timed out: ${stderr}`)), 5000);
      child.stdout.once('data', () => { clearTimeout(timer); resolve(); });
      child.once('exit', (code) => { clearTimeout(timer); reject(new Error(`Director mock exited ${code}: ${stderr}`)); });
    });

    const session = `director-wait-${process.pid}`;
    const user = { role: 'user', content: [{ type: 'input_text', text: followUp }] };
    const body = (input) => ({
      model: 'gpt-6-sol', prompt_cache_key: session, store: false, stream: true,
      include: ['reasoning.encrypted_content'],
      tools: ['start_research', 'inspect_run', 'decide_wait'].map((name) => ({ name })),
      input,
    });
    const send = async (input) => {
      const response = await fetch(`http://127.0.0.1:${port}/v1/responses`, {
        method: 'POST', signal: AbortSignal.timeout(5000),
        headers: { 'content-type': 'application/json', originator: 'exomachina',
          authorization: 'Bearer synthetic.token.signature', 'chatgpt-account-id': 'acct_synthetic',
          'session-id': session },
        body: JSON.stringify(body(input)),
      });
      const stream = await response.text();
      assert.equal(response.status, 200, stream);
      return completed(stream);
    };

    const first = await send([user]);
    const inspect = first.find((item) => item.type === 'function_call');
    assert.deepEqual(first.filter((item) => item.type === 'function_call').map((item) => item.name), ['inspect_run']);
    assert.deepEqual(JSON.parse(inspect.arguments), {});

    const revision = 'r3', sha256 = 'a'.repeat(64);
    const second = await send([
      user,
      first.find((item) => item.type === 'reasoning'),
      inspect,
      { type: 'function_call_output', call_id: inspect.call_id,
        output: JSON.stringify({ run: { current_revision: revision, current_sha256: sha256 } }) },
    ]);
    const decisions = second.filter((item) => item.type === 'function_call');
    assert.deepEqual(decisions.map((item) => item.name), ['decide_wait']);
    assert.deepEqual(JSON.parse(decisions[0].arguments), {
      action: 'abort', revision, sha256, rationale: 'repair exhausted',
    });

    const requests = fs.readFileSync(record, 'utf8').trim().split('\n').map(JSON.parse);
    assert.equal(requests.filter((entry) => entry.kind === 'request').length, 2);
    assert.deepEqual(requests.filter((entry) => entry.kind === 'director')
      .flatMap((entry) => entry.reply.calls), [inspect.call_id, decisions[0].call_id]);
    assert.ok(requests.every((entry) => !entry.reply?.calls?.some((id) => id.startsWith('call_start_'))));
  } finally {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill('SIGTERM');
      await once(child, 'exit');
    }
  }
});
