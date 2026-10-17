import { DatabaseSync } from 'node:sqlite';

export class SyncError extends Error {
  constructor(message, code) { super(message); this.code = code; }
}

const STATUSES = ['PENDING', 'PAID', 'SHIPPED'];
const plain = rows => rows.map(row => ({ ...row }));
export const MAX_PAGE = 500;

// Projeção + log na mesma transação. onCommit só roda depois do COMMIT.
//
// As leituras têm contratos que não mudam com o tempo, para que qualquer
// camada possa cachear a resposta sem torná-la falsa:
//   snapshot(scope, minCursor) -> "um estado consistente em algum cursor >= minCursor"
//   changes(scope, after, through) -> "os eventos (after, through]"; exige through <= head,
//     então o intervalo já está fechado e é imutável.
export class Store {
  constructor(path = ':memory:', { onCommit = () => {}, clock = Date.now } = {}) {
    this.db = new DatabaseSync(path);
    this.onCommit = onCommit;
    this.clock = clock;
    this.db.exec(`
      PRAGMA journal_mode=WAL;
      PRAGMA busy_timeout=5000;
      CREATE TABLE IF NOT EXISTS scopes (
        id TEXT PRIMARY KEY, head INTEGER NOT NULL DEFAULT 0, floor INTEGER NOT NULL DEFAULT 0);
      CREATE TABLE IF NOT EXISTS orders (
        scope TEXT NOT NULL, id TEXT NOT NULL, version INTEGER NOT NULL, status TEXT NOT NULL,
        deleted INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (scope, id));
      CREATE TABLE IF NOT EXISTS events (
        scope TEXT NOT NULL, seq INTEGER NOT NULL, payload TEXT NOT NULL, PRIMARY KEY (scope, seq));
    `);
    this.q = {
      ensure: this.db.prepare('INSERT OR IGNORE INTO scopes(id) VALUES (?)'),
      scope: this.db.prepare('SELECT head, floor FROM scopes WHERE id = ?'),
      order: this.db.prepare('SELECT version, status, deleted FROM orders WHERE scope = ? AND id = ?'),
      upsert: this.db.prepare(`INSERT INTO orders(scope, id, version, status, deleted) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(scope, id) DO UPDATE SET version = excluded.version, status = excluded.status, deleted = excluded.deleted`),
      event: this.db.prepare('INSERT INTO events(scope, seq, payload) VALUES (?, ?, ?)'),
      advance: this.db.prepare('UPDATE scopes SET head = ? WHERE id = ?'),
      live: this.db.prepare('SELECT id, version, status FROM orders WHERE scope = ? AND deleted = 0 ORDER BY id'),
      range: this.db.prepare('SELECT payload FROM events WHERE scope = ? AND seq > ? AND seq <= ? ORDER BY seq')
    };
  }

  #scope(scope) {
    if (typeof scope !== 'string' || !scope || scope.length > 100) throw new SyncError('Escopo inválido', 'BAD_USER_INPUT');
    this.q.ensure.run(scope);
    return this.q.scope.get(scope);
  }

  #transaction(fn, write = false) {
    this.db.exec(write ? 'BEGIN IMMEDIATE' : 'BEGIN');
    try { const result = fn(); this.db.exec('COMMIT'); return result; }
    catch (error) { this.db.exec('ROLLBACK'); throw error; }
  }

  head(scope) { return this.#transaction(() => this.#scope(scope).head); }

  change(scope, id, status, deleted = false) {
    if (typeof id !== 'string' || !id || (!deleted && !STATUSES.includes(status))) {
      throw new SyncError('Pedido/status inválido', 'BAD_USER_INPUT');
    }
    const change = this.#transaction(() => {
      const { head } = this.#scope(scope);
      const current = this.q.order.get(scope, id);
      if (deleted && (!current || current.deleted)) throw new SyncError('Pedido inexistente', 'NOT_FOUND');
      const seq = head + 1;
      const order = { id, version: (current?.version ?? 0) + 1, status: status ?? current.status };
      const change = { scopeId: scope, cursor: String(seq), operation: deleted ? 'DELETE' : 'UPSERT', order, committedAt: this.clock() };
      this.q.upsert.run(scope, id, order.version, order.status, Number(deleted));
      this.q.event.run(scope, seq, JSON.stringify(change));
      this.q.advance.run(seq, scope);
      return change;
    }, true);
    this.onCommit(change);
    return change;
  }

  snapshot(scope) {
    return this.#transaction(() => {
      const { head } = this.#scope(scope);
      return { scopeId: scope, cursor: String(head), orders: plain(this.q.live.all(scope)) };
    });
  }

  changes(scope, after, through) {
    const a = Number(after), t = Number(through);
    return this.#transaction(() => {
      const { head, floor } = this.#scope(scope);
      if (![a, t].every(Number.isSafeInteger) || a < 0 || t < a || t - a > MAX_PAGE) {
        throw new SyncError(`Intervalo inválido (máximo ${MAX_PAGE} eventos)`, 'BAD_USER_INPUT');
      }
      // Pedir além do head tornaria a resposta mutável; com réplicas defasadas isso
      // acontece de verdade, e o cliente só tenta de novo.
      if (t > head) throw new SyncError(`Cursor ${t} ainda não disponível (head ${head})`, 'NOT_YET');
      if (a < floor) throw new SyncError('Histórico expirado; use um snapshot mais novo', 'CURSOR_EXPIRED');
      const events = this.q.range.all(scope, a, t).map(row => JSON.parse(row.payload));
      return { scopeId: scope, after: String(a), through: String(t), events };
    });
  }

  prune(scope, through) {
    this.#transaction(() => {
      const { head } = this.#scope(scope);
      if (!Number.isSafeInteger(through) || through < 0 || through > head) throw new SyncError('Prune inválido', 'BAD_USER_INPUT');
      this.db.prepare('DELETE FROM events WHERE scope = ? AND seq <= ?').run(scope, through);
      this.db.prepare('UPDATE scopes SET floor = MAX(floor, ?) WHERE id = ?').run(through, scope);
    }, true);
  }

  close() { this.db.close(); }
}

// Cache interno do subgraph. Serve qualquer snapshot que satisfaça minCursor; só
// lê o banco quando o que tem é antigo demais ou expirou. Nunca devolve cursor < minCursor.
export class SnapshotCache {
  constructor(store, ttlMs = 120_000, clock = Date.now) {
    this.store = store; this.ttlMs = ttlMs; this.clock = clock; this.entries = new Map(); this.reads = 0;
  }
  get(scope, minCursor = 0) {
    const entry = this.entries.get(scope);
    if (entry && entry.expires > this.clock() && Number(entry.value.cursor) >= minCursor) return structuredClone(entry.value);
    this.reads++;
    const value = this.store.snapshot(scope);
    if (Number(value.cursor) < minCursor) {
      throw new SyncError(`Cursor ${minCursor} ainda não disponível (head ${value.cursor})`, 'NOT_YET');
    }
    this.entries.set(scope, { value, expires: this.clock() + this.ttlMs });
    return structuredClone(value);
  }
}
