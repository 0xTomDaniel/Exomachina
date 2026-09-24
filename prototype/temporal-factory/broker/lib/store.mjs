import fs from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';

const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const RECOVERY_GATE_STALE_MS = 30000;

export function pidAlive(pid) {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  try { process.kill(pid, 0); return true; }
  catch (error) { return error.code === 'EPERM'; }
}

function unsafePath() {
  return Object.assign(new Error('unsafe model broker filesystem path'), { kind: 'config' });
}

function checkedStat(file, type, missing = false, singleLink = false) {
  let stat;
  try { stat = fs.lstatSync(file); }
  catch (error) {
    if (missing && error.code === 'ENOENT') return null;
    throw error;
  }
  if (stat.isSymbolicLink() || stat.uid !== process.getuid() ||
      (type === 'directory' ? !stat.isDirectory() : !stat.isFile()) ||
      (stat.mode & 0o077) || (singleLink && stat.nlink !== 1)) throw unsafePath();
  return stat;
}

export function ownerOnlyDirectory(dir) {
  const existing = fs.lstatSync(dir, { throwIfNoEntry: false });
  if (existing && (existing.isSymbolicLink() || !existing.isDirectory() || existing.uid !== process.getuid())) throw unsafePath();
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  const before = fs.lstatSync(dir);
  if (before.isSymbolicLink() || !before.isDirectory() || before.uid !== process.getuid()) throw unsafePath();
  const fd = fs.openSync(dir, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY | fs.constants.O_NOFOLLOW);
  try {
    const opened = fs.fstatSync(fd);
    if (opened.ino !== before.ino || opened.dev !== before.dev) throw unsafePath();
    fs.fchmodSync(fd, 0o700);
    const after = checkedStat(dir, 'directory');
    if (after.ino !== before.ino || after.dev !== before.dev) throw unsafePath();
  } finally { fs.closeSync(fd); }
}

export function ownerOnlyFile(file, singleLink = false) {
  let before;
  try { before = fs.lstatSync(file); }
  catch (error) { if (error.code === 'ENOENT') return; throw error; }
  if (before.isSymbolicLink() || !before.isFile() || before.uid !== process.getuid() ||
      (singleLink && before.nlink !== 1)) throw unsafePath();
  const fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  try {
    const opened = fs.fstatSync(fd);
    if (opened.ino !== before.ino || opened.dev !== before.dev ||
        (singleLink && opened.nlink !== 1)) throw unsafePath();
    fs.fchmodSync(fd, 0o600);
    const after = checkedStat(file, 'file', false, singleLink);
    if (after.ino !== before.ino || after.dev !== before.dev) throw unsafePath();
  } finally { fs.closeSync(fd); }
}

function ownerPid(lock) {
  const owner = readOwnerOnlyFile(path.join(lock, 'owner'));
  return owner === undefined ? null : Number(owner.trim());
}

export function checkCredentialPath(file) {
  checkedStat(path.dirname(path.dirname(file)), 'directory');
  checkedStat(path.dirname(file), 'directory');
  checkedStat(file, 'file', true, true);
  checkedStat(`${file}.lock`, 'directory', true);
}

export function readOwnerOnlyFile(file, singleLink = false) {
  const before = checkedStat(file, 'file', true, singleLink);
  if (!before) return undefined;
  const fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  try {
    const after = fs.fstatSync(fd);
    if (!after.isFile() || after.uid !== process.getuid() || (after.mode & 0o077) ||
        after.ino !== before.ino || after.dev !== before.dev ||
        (singleLink && after.nlink !== 1)) throw unsafePath();
    return fs.readFileSync(fd, 'utf8');
  } finally { fs.closeSync(fd); }
}

export function rewriteAuthorizeOriginator(url) {
  if (!/[?&]originator=/.test(url)) throw Object.assign(new Error('authorization URL has no originator'), { kind: 'config' });
  return url.replace(/([?&]originator=)[^&#]*/, '$1exomachina');
}

function recoverAbandonedGate(gate) {
  const abandoned = (location, stat) => {
    const pid = ownerPid(location);
    if (Number.isSafeInteger(pid) && pid > 0) return !pidAlive(pid);
    // Only a missing or malformed owner uses age: mkdir may have completed
    // immediately before the process died, before it could write owner.
    return Date.now() - stat.mtimeMs >= RECOVERY_GATE_STALE_MS;
  };
  const observed = checkedStat(gate, 'directory', true);
  if (!observed || !abandoned(gate, observed)) return false;
  // The gate holder performs no awaits between mkdir and rmdir. Recheck the
  // same directory and its owner immediately before moving it.
  const before = checkedStat(gate, 'directory', true);
  if (!before || before.dev !== observed.dev || before.ino !== observed.ino ||
      !abandoned(gate, before)) return false;
  const tombstone = `${gate}.stale-${process.pid}-${randomUUID()}`;
  try { fs.renameSync(gate, tombstone); }
  catch (error) { if (error.code === 'ENOENT') return false; throw error; }
  const moved = checkedStat(tombstone, 'directory');
  if (moved.dev !== before.dev || moved.ino !== before.ino || !abandoned(tombstone, moved)) {
    // Another process replaced the path before the rename. Preserve its gate.
    if (!fs.lstatSync(gate, { throwIfNoEntry: false })) fs.renameSync(tombstone, gate);
    throw unsafePath();
  }
  if (fs.lstatSync(path.join(tombstone, 'owner'), { throwIfNoEntry: false }))
    fs.unlinkSync(path.join(tombstone, 'owner'));
  fs.rmdirSync(tombstone);
  return true;
}

// The optional hooks are used only by direct store-level tests. Product
// callers, including every CLI and socket operation, never supply them.
export async function withPidLock(lock, fn, timeoutMs = 10000, hooks = {}) {
  const deadline = Date.now() + timeoutMs;
  const recovery = `${lock}.recovery`;
  for (;;) {
    try {
      // A recoverer holds this gate while it moves the stale directory away.
      // A late acquirer may still mkdir after the move; the recoverer then
      // touches only its tombstone, never the new live lock.
      if (checkedStat(recovery, 'directory', true)) {
        if (recoverAbandonedGate(recovery)) continue;
        if (Date.now() >= deadline) throw new Error('credential lock unavailable');
        await pause(25);
        continue;
      }
      checkedStat(lock, 'directory', true);
      fs.mkdirSync(lock, { mode: 0o700 });
      fs.writeFileSync(path.join(lock, 'owner'), String(process.pid), { mode: 0o600, flag: 'wx' });
      break;
    } catch (error) {
      if (error.code !== 'EEXIST') throw error;
      // Another recoverer can move the directory between our failed mkdir
      // and this check. That is ordinary contention, not a configuration error.
      if (!checkedStat(lock, 'directory', true)) {
        if (Date.now() >= deadline) throw new Error('credential lock unavailable');
        await pause(25);
        continue;
      }
      const pid = ownerPid(lock);
      // A missing or malformed owner is ambiguous during lock creation. Only a
      // recorded, dead PID proves that recovering this lock is safe.
      if (pid && !pidAlive(pid)) {
        await hooks.afterDeadOwnerObserved?.();
        try {
          fs.mkdirSync(recovery, { mode: 0o700 });
          fs.writeFileSync(path.join(recovery, 'owner'), String(process.pid), { mode: 0o600, flag: 'wx' });
        } catch (race) {
          if (race.code !== 'EEXIST') throw race;
          if (Date.now() >= deadline) throw new Error('credential lock unavailable');
          await pause(25);
          continue;
        }
        try {
          const before = checkedStat(lock, 'directory', true);
          if (before && ownerPid(lock) === pid && !pidAlive(pid)) {
            const tombstone = `${lock}.stale-${process.pid}-${randomUUID()}`;
            fs.renameSync(lock, tombstone);
            // Only the process that moved this directory owns its tombstone.
            // A different lock at the original path is never removed here.
            const moved = checkedStat(tombstone, 'directory');
            if (moved.dev !== before.dev || moved.ino !== before.ino) throw unsafePath();
            fs.unlinkSync(path.join(tombstone, 'owner'));
            fs.rmdirSync(tombstone);
          }
        } catch (race) {
          if (!['ENOENT', 'ENOTEMPTY', 'EEXIST'].includes(race.code)) throw race;
        } finally {
          fs.unlinkSync(path.join(recovery, 'owner'));
          fs.rmdirSync(recovery);
        }
        await hooks.afterRecoveryAttempt?.();
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
  readCurrent() {
    checkCredentialPath(this.file);
    const contents = readOwnerOnlyFile(this.file, true);
    return contents === undefined ? undefined : JSON.parse(contents);
  }
  async read(id) {
    if (id !== 'openai-codex') return undefined;
    return this.readCurrent();
  }
  async list() {
    const value = this.readCurrent();
    return value === undefined ? [] : [{ providerId: 'openai-codex', type: value.type }];
  }
  async transaction(id, fn) {
    checkCredentialPath(this.file);
    return withPidLock(`${this.file}.lock`, async () => {
      if (id !== 'openai-codex') throw new Error('unsupported credential provider');
      const current = this.readCurrent();
      const result = await fn(current);
      if (result.write) {
        // Logout writes an empty object atomically so the credential path and
        // its audit trail remain in place under the no-deletion policy.
        const temporary = `${this.file}.${process.pid}.${Date.now()}.tmp`;
        fs.writeFileSync(temporary, JSON.stringify(result.remove ? {} : result.value), { mode: 0o600, flag: 'wx' });
        checkCredentialPath(this.file);
        fs.renameSync(temporary, this.file);
        checkedStat(this.file, 'file');
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
