import fs from 'node:fs';
import path from 'node:path';

const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export function pidAlive(pid) {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  try { process.kill(pid, 0); return true; }
  catch (error) { return error.code === 'EPERM'; }
}

function unsafePath() {
  return Object.assign(new Error('unsafe model broker filesystem path'), { kind: 'config' });
}

function checkedStat(file, type, missing = false) {
  let stat;
  try { stat = fs.lstatSync(file); }
  catch (error) {
    if (missing && error.code === 'ENOENT') return null;
    throw error;
  }
  if (stat.isSymbolicLink() || stat.uid !== process.getuid() ||
      (type === 'directory' ? !stat.isDirectory() : !stat.isFile()) ||
      (stat.mode & 0o077)) throw unsafePath();
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

export function ownerOnlyFile(file) {
  let before;
  try { before = fs.lstatSync(file); }
  catch (error) { if (error.code === 'ENOENT') return; throw error; }
  if (before.isSymbolicLink() || !before.isFile() || before.uid !== process.getuid()) throw unsafePath();
  const fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  try {
    const opened = fs.fstatSync(fd);
    if (opened.ino !== before.ino || opened.dev !== before.dev) throw unsafePath();
    fs.fchmodSync(fd, 0o600);
    const after = checkedStat(file, 'file');
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
  checkedStat(file, 'file', true);
  checkedStat(`${file}.lock`, 'directory', true);
}

export function readOwnerOnlyFile(file) {
  const before = checkedStat(file, 'file', true);
  if (!before) return undefined;
  const fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  try {
    const after = fs.fstatSync(fd);
    if (!after.isFile() || after.uid !== process.getuid() || (after.mode & 0o077) ||
        after.ino !== before.ino || after.dev !== before.dev) throw unsafePath();
    return fs.readFileSync(fd, 'utf8');
  } finally { fs.closeSync(fd); }
}

export function rewriteAuthorizeOriginator(url) {
  if (!/[?&]originator=/.test(url)) throw Object.assign(new Error('authorization URL has no originator'), { kind: 'config' });
  return url.replace(/([?&]originator=)[^&#]*/, '$1exomachina');
}

export async function withPidLock(lock, fn, timeoutMs = 10000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      checkedStat(lock, 'directory', true);
      fs.mkdirSync(lock, { mode: 0o700 });
      fs.writeFileSync(path.join(lock, 'owner'), String(process.pid), { mode: 0o600, flag: 'wx' });
      break;
    } catch (error) {
      if (error.code !== 'EEXIST') throw error;
      checkedStat(lock, 'directory');
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
  readCurrent() {
    checkCredentialPath(this.file);
    const contents = readOwnerOnlyFile(this.file);
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
