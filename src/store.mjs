import { DatabaseSync } from 'node:sqlite';
import { diff } from './patch.mjs';

export class SyncError extends Error {
  constructor(message, code) { super(message); this.code = code; }
}

// Um documento versionado por partida: o estado atual e um contador. Sem log de
// eventos: quem precisa recuperar pede um snapshot com versão mínima.
// Cada subgraph tem o seu Store (placar e estatísticas não compartilham banco).
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
    if (typeof key !== 'string' || !key || key.length > 100) throw new SyncError('Partida inválida', 'BAD_USER_INPUT');
  }

  read(key) {
    this.#check(key);
    const row = this.q.get.get(key);
    return row ? { key, version: row.version, state: JSON.parse(row.state) } : { key, version: 0, state: this.initial() };
  }

  version(key) { return this.read(key).version; }

  // mutate recebe uma cópia do estado e a altera. Versão + estado na mesma
  // transação; o patch (valores finais) é calculado aqui e publicado após o COMMIT.
  update(key, mutate) {
    this.#check(key);
    this.db.exec('BEGIN IMMEDIATE');
    let change;
    try {
      const current = this.read(key);
      const next = structuredClone(current.state);
      mutate(next);
      const patch = diff(current.state, next);
      if (!patch.length) { this.db.exec('COMMIT'); return null; } // nada mudou: sem nova versão
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

// Cache interno do subgraph. Serve qualquer snapshot com versão >= minVersion e
// só lê o banco quando o que tem é antigo demais. Nunca devolve versão < minVersion.
export class SnapshotCache {
  constructor(store, ttlMs = 1000, clock = Date.now) {
    this.store = store; this.ttlMs = ttlMs; this.clock = clock; this.entries = new Map(); this.reads = 0;
  }
  get(key, minVersion = 0) {
    const entry = this.entries.get(key);
    if (entry && entry.expires > this.clock() && entry.value.version >= minVersion) return structuredClone(entry.value);
    this.reads++;
    const value = this.store.read(key);
    if (value.version < minVersion) throw new SyncError(`Versão ${minVersion} ainda não disponível (atual ${value.version})`, 'NOT_YET');
    this.entries.set(key, { value, expires: this.clock() + this.ttlMs });
    return structuredClone(value);
  }
}
