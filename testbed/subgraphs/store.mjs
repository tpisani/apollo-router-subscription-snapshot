import { DatabaseSync } from 'node:sqlite';
import { diff } from './patch.mjs';

export class SyncError extends Error {
  constructor(message, code) { super(message); this.code = code; }
}

// One versioned document per match: the current state and a counter. No event
// log: whoever needs to recover asks for a snapshot with a minimum version.
// Each subgraph has its own Store (scoreboard and stats do not share a database).
export class Store {
  constructor(path = ':memory:', { initial, onCommit = () => {}, clock = Date.now } = {}) {
    this.db = new DatabaseSync(path);
    this.initial = initial; this.onCommit = onCommit; this.clock = clock;
    this.db.exec(`
      PRAGMA journal_mode=WAL;
      PRAGMA busy_timeout=5000;
      CREATE TABLE IF NOT EXISTS docs (key TEXT PRIMARY KEY, version INTEGER NOT NULL, state TEXT NOT NULL);
    `);
    this.q = {
      get: this.db.prepare('SELECT version, state FROM docs WHERE key = ?'),
      put: this.db.prepare(`INSERT INTO docs(key, version, state) VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET version = excluded.version, state = excluded.state`)
    };
  }

  #check(key) {
    if (typeof key !== 'string' || !key || key.length > 100) throw new SyncError('Invalid match', 'BAD_USER_INPUT');
  }

  read(key) {
    this.#check(key);
    const row = this.q.get.get(key);
    return row ? { key, version: row.version, state: JSON.parse(row.state) } : { key, version: 0, state: this.initial(key) };
  }

  version(key) { return this.read(key).version; }

  // mutate gets a copy of the state and changes it. Version + state in the same
  // transaction; the patch (final values) is computed here and published after COMMIT.
  update(key, mutate) {
    this.#check(key);
    this.db.exec('BEGIN IMMEDIATE');
    let change;
    try {
      const current = this.read(key);
      const next = structuredClone(current.state);
      mutate(next);
      const patch = diff(current.state, next);
      if (!patch.length) { this.db.exec('COMMIT'); return null; } // nothing changed: no new version
      const version = current.version + 1;
      this.q.put.run(key, version, JSON.stringify(next));
      this.db.exec('COMMIT');
      change = { key, version, state: next, patch, committedAt: this.clock() };
    } catch (error) { this.db.exec('ROLLBACK'); throw error; }
    this.onCommit(change);
    return change;
  }

  close() { this.db.close(); }
}

// The subgraph's internal cache. Serves any snapshot with version >= minVersion and
// only reads the database when what it has is too old. Never returns version < minVersion.
export class SnapshotCache {
  constructor(store, ttlMs = 1000, clock = Date.now) {
    this.store = store; this.ttlMs = ttlMs; this.clock = clock; this.entries = new Map(); this.reads = 0;
  }
  get(key, minVersion = 0) {
    const entry = this.entries.get(key);
    if (entry && entry.expires > this.clock() && entry.value.version >= minVersion) return structuredClone(entry.value);
    this.reads++;
    const value = this.store.read(key);
    if (value.version < minVersion) throw new SyncError(`Version ${minVersion} not available yet (current ${value.version})`
, 'NOT_YET');
    this.entries.set(key, { value, expires: this.clock() + this.ttlMs });
    return structuredClone(value);
  }
}
