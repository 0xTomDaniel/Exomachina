import fs from 'node:fs';
import path from 'node:path';

const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export function pidAlive(pid) {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  try { process.kill(pid, 0); return true; }
  catch (error) { return error.code === 'EPERM'; }
}

export function ownerOnlyDirectory(dir) {
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  fs.chmodSync(dir, 0o700);
}

function ownerPid(lock) {
  try { return Number(fs.readFileSync(path.join(lock, 'owner'), 'utf8').trim()); }
  catch { return null; }
}

export async function withPidLock(lock, fn, timeoutMs = 10000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      fs.mkdirSync(lock, { mode: 0o700 });
      fs.writeFileSync(path.join(lock, 'owner'), String(process.pid), { mode: 0o600, flag: 'wx' });
      break;
    } catch (error) {
      if (error.code !== 'EEXIST') throw error;
      const pid = ownerPid(lock);
      // A missing or malformed owner is ambiguous during lock creation. Only a
      // recorded, dead PID proves that removing this lock is safe.
      if (pid && !pidAlive(pid)) {
        try {
          if (ownerPid(lock) === pid) {
            fs.unlinkSync(path.join(lock, 'owner'));
            fs.rmdirSync(lock);
          }
        } catch (race) {
          if (!['ENOENT', 'ENOTEMPTY', 'EEXIST'].includes(race.code)) throw race;
        }
      }
      if (Date.now() >= deadline) throw new Error('credential lock unavailable');
      await pause(25);
    }
  }
  try { return await fn(); }
  finally {
    fs.unlinkSync(path.join(lock, 'owner'));
    fs.rmdirSync(lock);
  }
}

export class FileCredentialStore {
  constructor(file) { this.file = file; this.chain = Promise.resolve(); }
  async read(id) {
    try {
      if (id !== 'openai-codex') return undefined;
      return JSON.parse(fs.readFileSync(this.file, 'utf8'));
    } catch (error) {
      if (error.code === 'ENOENT') return undefined;
      throw error;
    }
  }
  async list() {
    try {
      const value = JSON.parse(fs.readFileSync(this.file, 'utf8'));
      return [{ providerId: 'openai-codex', type: value.type }];
    } catch (error) {
      if (error.code === 'ENOENT') return [];
      throw error;
    }
  }
  async transaction(id, fn) {
    return withPidLock(`${this.file}.lock`, async () => {
      if (id !== 'openai-codex') throw new Error('unsupported credential provider');
      let current;
      try { current = JSON.parse(fs.readFileSync(this.file, 'utf8')); }
      catch (error) {
        if (error.code !== 'ENOENT') throw error;
      }
      const result = await fn(current);
      if (result.write) {
        // Logout writes an empty object atomically so the credential path and
        // its audit trail remain in place under the no-deletion policy.
        const temporary = `${this.file}.${process.pid}.${Date.now()}.tmp`;
        fs.writeFileSync(temporary, JSON.stringify(result.remove ? {} : result.value), { mode: 0o600, flag: 'wx' });
        fs.renameSync(temporary, this.file);
        fs.chmodSync(this.file, 0o600);
      }
      return result.write ? result.value : current;
    });
  }
  enqueue(id, fn) {
    const work = this.chain.then(() => this.transaction(id, fn));
    this.chain = work.catch(() => {});
    return work;
  }
  modify(id, fn) {
    return this.enqueue(id, async (current) => {
      const value = await fn(current);
      return { write: value !== undefined, value };
    });
  }
  async delete(id) {
    await this.enqueue(id, async () => ({ write: true, remove: true, value: undefined }));
  }
}
