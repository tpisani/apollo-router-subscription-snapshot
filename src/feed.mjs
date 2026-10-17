import { FeedError } from './store.mjs';

// Fan-out em memória por escopo. Um commit gera uma única chamada por assinante,
// sem leitura extra no banco. Para várias réplicas do subgraph, troque por um
// adaptador com a mesma interface (Redis Pub/Sub, NATS, Postgres LISTEN/NOTIFY):
// o feed continua correto porque o replay vem do log, não do bus.
export class Bus {
  constructor() { this.listeners = new Map(); }
  subscribe(scope, listener) {
    let set = this.listeners.get(scope);
    if (!set) this.listeners.set(scope, set = new Set());
    set.add(listener);
    return () => { set.delete(listener); if (!set.size) this.listeners.delete(scope); };
  }
  publish(change) {
    for (const listener of this.listeners.get(change.scopeId) ?? []) listener(change);
  }
  get size() { let n = 0; for (const set of this.listeners.values()) n += set.size; return n; }
}

// Um feed por assinante:
//   1. registra no bus ANTES de ler o banco (nada commitado depois disso se perde);
//   2. envia SNAPSHOT ou CHANGES até o head lido na mesma transação;
//   3. envia o que chegou pelo bus, descartando seq <= cursor já enviado.
// Iterator manual (e não async generator) para que return() libere o listener
// imediatamente, mesmo com o feed aguardando o próximo commit.
export class Feed {
  constructor({ store, bus, scopeId, after = null, maxReplay = 1000, queueLimit = 1000, onClose = () => {} }) {
    this.scopeId = scopeId; this.queueLimit = queueLimit; this.onClose = onClose;
    this.queue = []; this.failure = null; this.done = false; this.wake = null;
    this.unsubscribe = bus.subscribe(scopeId, change => {
      if (this.queue.length >= queueLimit) {
        // Consumidor lento: em vez de acumular memória sem limite, encerra o feed.
        // O cliente reconecta com seu cursor e recebe replay/snapshot.
        this.failure ??= new FeedError('Consumidor lento; reconecte com o último cursor', 'SLOW_CONSUMER');
      } else this.queue.push(change);
      this.wake?.();
    });
    try {
      this.initial = store.catchUp(scopeId, after, maxReplay);
      this.cursor = Number(this.initial.cursor);
    } catch (error) { this.#close(); throw error; }
  }

  #close() {
    if (this.done) return;
    this.done = true; this.unsubscribe(); this.wake?.(); this.onClose();
  }

  async next() {
    if (this.initial) { const value = this.initial; this.initial = null; return { done: false, value }; }
    while (!this.done) {
      if (this.failure) { const error = this.failure; this.#close(); throw error; }
      if (this.queue.length) {
        const changes = [];
        for (const change of this.queue.splice(0)) {
          const seq = Number(change.cursor);
          if (seq <= this.cursor) continue;
          if (seq !== this.cursor + 1) { this.#close(); throw new FeedError(`Lacuna no bus: ${this.cursor} -> ${seq}`, 'GAP'); }
          changes.push(change); this.cursor = seq;
        }
        // Lote natural: se o consumidor demorou, tudo o que acumulou vai num frame só.
        if (changes.length) return { done: false, value: { kind: 'CHANGES', scopeId: this.scopeId, cursor: String(this.cursor), orders: null, changes } };
        continue;
      }
      await new Promise(resolve => { this.wake = resolve; });
      this.wake = null;
    }
    return { done: true, value: undefined };
  }

  async return() { this.#close(); return { done: true, value: undefined }; }
  async throw(error) { this.#close(); throw error; }
  [Symbol.asyncIterator]() { return this; }
}
