import { DatabaseSync } from 'node:sqlite';

export class SyncError extends Error {
  constructor(message, code) { super(message); this.code = code; }
}

export class Store {
  constructor(path = ':memory:') {
    this.db = new DatabaseSync(path);
    this.db.exec(`
      PRAGMA journal_mode=WAL;
      PRAGMA busy_timeout=5000;
      CREATE TABLE IF NOT EXISTS scopes (
        id TEXT PRIMARY KEY, head INTEGER NOT NULL DEFAULT 0, floor INTEGER NOT NULL DEFAULT 0);
      CREATE TABLE IF NOT EXISTS orders (
        scope TEXT NOT NULL, id TEXT NOT NULL, version INTEGER NOT NULL,
        status TEXT NOT NULL, deleted INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(scope,id));
      CREATE TABLE IF NOT EXISTS events (
        scope TEXT NOT NULL, seq INTEGER NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(scope,seq));
    `);
  }
  ensure(scope) {
    if (!scope || scope.length > 100) throw new SyncError('Escopo inválido', 'BAD_USER_INPUT');
    this.db.prepare('INSERT OR IGNORE INTO scopes(id) VALUES (?)').run(scope);
  }
  head(scope) { this.ensure(scope); return this.db.prepare('SELECT head FROM scopes WHERE id=?').get(scope).head; }
  transaction(fn, write = false) {
    this.db.exec(write ? 'BEGIN IMMEDIATE' : 'BEGIN');
    try { const result = fn(); this.db.exec('COMMIT'); return result; }
    catch (error) { this.db.exec('ROLLBACK'); throw error; }
  }
  change(scope, id, status, deleted = false) {
    this.ensure(scope);
    if (!id || (!deleted && !['PENDING','PAID','SHIPPED'].includes(status))) {
      throw new SyncError('Pedido/status inválido', 'BAD_USER_INPUT');
    }
    return this.transaction(() => {
      const current = this.db.prepare('SELECT * FROM orders WHERE scope=? AND id=?').get(scope,id);
      const version = (current?.version ?? 0) + 1;
      const seq = this.db.prepare('SELECT head FROM scopes WHERE id=?').get(scope).head + 1;
      const order = { id, version, status: status ?? current?.status ?? 'PENDING' };
      const event = { scopeId: scope, cursor: String(seq), operation: deleted ? 'DELETE' : 'UPSERT', order };
      this.db.prepare(`INSERT INTO orders(scope,id,version,status,deleted) VALUES (?,?,?,?,?)
        ON CONFLICT(scope,id) DO UPDATE SET version=excluded.version,status=excluded.status,deleted=excluded.deleted`)
        .run(scope,id,version,order.status,Number(deleted));
      this.db.prepare('INSERT INTO events VALUES (?,?,?)').run(scope,seq,JSON.stringify(event));
      this.db.prepare('UPDATE scopes SET head=? WHERE id=?').run(seq,scope);
      return event;
    }, true);
  }
  snapshot(scope) {
    this.ensure(scope);
    return this.transaction(() => ({
      scopeId: scope,
      cursor: String(this.db.prepare('SELECT head FROM scopes WHERE id=?').get(scope).head),
      orders: this.db.prepare('SELECT id,version,status FROM orders WHERE scope=? AND deleted=0 ORDER BY id').all(scope)
    }));
  }
  changes(scope, after, through, first = 100) {
    this.ensure(scope);
    return this.transaction(() => {
      const { head, floor } = this.db.prepare('SELECT head,floor FROM scopes WHERE id=?').get(scope);
      const a = Number(after), h = Number(through);
      if (![a,h].every(Number.isSafeInteger) || a < 0 || h < a || h > head || first < 1 || first > 500) {
        throw new SyncError('Intervalo/cursor inválido', 'BAD_USER_INPUT');
      }
      if (a < floor) throw new SyncError('Histórico expirado; obtenha um snapshot fresh', 'CURSOR_EXPIRED');
      const events = this.db.prepare('SELECT payload FROM events WHERE scope=? AND seq>? AND seq<=? ORDER BY seq LIMIT ?')
        .all(scope,a,h,first).map(row => JSON.parse(row.payload));
      const next = events.length ? Number(events.at(-1).cursor) : a;
      return { events, nextCursor: String(next), hasMore: next < h, through: String(h) };
    });
  }
  prune(scope, through) {
    this.ensure(scope);
    this.transaction(() => {
      const head = this.db.prepare('SELECT head FROM scopes WHERE id=?').get(scope).head;
      if (!Number.isSafeInteger(through) || through < 0 || through > head) throw new Error('Invalid prune');
      this.db.prepare('DELETE FROM events WHERE scope=? AND seq<=?').run(scope,through);
      this.db.prepare('UPDATE scopes SET floor=MAX(floor,?) WHERE id=?').run(through,scope);
    }, true);
  }
  close() { this.db.close(); }
}

// Cache do objeto inteiro: dados e cursor nunca são obtidos separadamente.
export class SnapshotCache {
  constructor(store, ttlMs = 120_000, clock = Date.now) {
    this.store = store; this.ttlMs = ttlMs; this.clock = clock; this.entries = new Map();
  }
  get(scope, fresh = false) {
    const entry = this.entries.get(scope);
    if (!fresh && entry && entry.expires > this.clock()) return structuredClone(entry.value);
    const value = this.store.snapshot(scope);
    this.entries.set(scope,{ value, expires: this.clock() + this.ttlMs });
    return structuredClone(value);
  }
}
