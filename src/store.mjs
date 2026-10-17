import { DatabaseSync } from 'node:sqlite';

export class FeedError extends Error {
  constructor(message, code) { super(message); this.code = code; }
}

const STATUSES = ['PENDING', 'PAID', 'SHIPPED'];
const plain = rows => rows.map(row => ({ ...row }));

// Projeção + log de eventos na mesma transação. O log é a fonte de replay;
// onCommit só é chamado depois do COMMIT, então nunca publica algo que não persistiu.
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
      range: this.db.prepare('SELECT payload FROM events WHERE scope = ? AND seq > ? AND seq <= ? ORDER BY seq LIMIT ?')
    };
  }

  #scope(scope) {
    if (typeof scope !== 'string' || !scope || scope.length > 100) throw new FeedError('Escopo inválido', 'BAD_USER_INPUT');
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
      throw new FeedError('Pedido/status inválido', 'BAD_USER_INPUT');
    }
    const change = this.#transaction(() => {
      const { head } = this.#scope(scope);
      const current = this.q.order.get(scope, id);
      if (deleted && (!current || current.deleted)) throw new FeedError('Pedido inexistente', 'NOT_FOUND');
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

  // Ponto de entrada do feed: devolve o que falta para o cliente alcançar o head,
  // lido numa única transação. Replay quando o histórico cobre o cursor e a
  // diferença é pequena; caso contrário um snapshot (mais barato que milhares de eventos).
  catchUp(scope, after, maxReplay = 1000) {
    return this.#transaction(() => {
      const { head, floor } = this.#scope(scope);
      const from = after == null ? null : Number(after);
      if (from !== null && (!Number.isSafeInteger(from) || from < 0)) throw new FeedError('Cursor inválido', 'BAD_USER_INPUT');
      // Cursor acima do head: banco recriado ou cursor de outro ambiente. Reinicia o cliente.
      const replayable = from !== null && from >= floor && from <= head && head - from <= maxReplay;
      if (!replayable) {
        return { kind: 'SNAPSHOT', scopeId: scope, cursor: String(head), orders: plain(this.q.live.all(scope)), changes: [] };
      }
      const changes = this.q.range.all(scope, from, head, maxReplay).map(row => JSON.parse(row.payload));
      return { kind: 'CHANGES', scopeId: scope, cursor: String(head), orders: null, changes };
    });
  }

  prune(scope, through) {
    this.#transaction(() => {
      const { head } = this.#scope(scope);
      if (!Number.isSafeInteger(through) || through < 0 || through > head) throw new FeedError('Prune inválido', 'BAD_USER_INPUT');
      this.db.prepare('DELETE FROM events WHERE scope = ? AND seq <= ?').run(scope, through);
      this.db.prepare('UPDATE scopes SET floor = MAX(floor, ?) WHERE id = ?').run(through, scope);
    }, true);
  }

  close() { this.db.close(); }
}
