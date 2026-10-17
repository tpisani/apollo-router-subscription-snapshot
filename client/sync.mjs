export class GapError extends Error {}

// Monta e mantém o estado de um escopo combinando:
//   - um stream compartilhado (deduplicável): CHANGE ao vivo e CHECKPOINT periódico;
//   - queries cujas respostas podem vir de qualquer cache sem ficar erradas.
//
// Invariante: orders é sempre o estado exato em `cursor`, nunca uma mistura.
// `target` é o maior cursor que o stream já provou existir. synced = cursor >= target
// depois de ao menos um frame recebido nesta conexão.
//
// Um snapshot vindo de cache pode estar antigo; isso só significa mais eventos a
// aplicar. Ele nunca é a fonte do "agora": o alvo vem sempre do stream, e os
// intervalos (cursor, alvo] são imutáveis, portanto corretos mesmo se cacheados.
export class Synchronizer {
  constructor({ scopeId, api, pageSize = 100, maxReplay = 2000 }) {
    Object.assign(this, { scopeId, api, pageSize, maxReplay });
    this.orders = new Map(); this.cursor = null; this.target = 0; this.synced = false; this.base = null;
    this.stats = { snapshots: 0, reseeds: 0, pages: 0, live: 0 };
  }

  initialize(snapshot) {
    if (snapshot.scopeId !== this.scopeId) throw new Error('Snapshot de outro escopo');
    this.orders = new Map(snapshot.orders.map(order => [order.id, { ...order }]));
    this.cursor = Number(snapshot.cursor);
    this.target = Math.max(this.target, this.cursor);
    this.stats.snapshots++;
  }

  applyChange(change) {
    if (change.scopeId !== this.scopeId) throw new Error('Evento de outro escopo');
    const seq = Number(change.cursor);
    if (seq <= this.cursor) return; // Reentrega ou sobreposição entre histórico e stream.
    if (seq !== this.cursor + 1) throw new GapError(`Lacuna: ${this.cursor} -> ${seq}`);
    if (change.operation === 'DELETE') this.orders.delete(change.order.id);
    else this.orders.set(change.order.id, { ...change.order });
    this.cursor = seq;
  }

  // Base inicial: a chave compartilhada minCursor "0", a mais provável de estar em cache.
  // Pode ser buscada em paralelo à abertura do stream (ver runSession).
  startBase() {
    this.base ??= this.api.snapshot('0').catch(error => { this.base = null; throw error; });
    return this.base;
  }

  // Snapshot com minCursor = alvo: chave nova, então nenhum cache antigo a satisfaz
  // a menos que já tenha um estado >= alvo. Usado quando o replay seria caro ou expirou.
  async reseed() {
    const min = this.target;
    const snapshot = await this.api.snapshot(String(min));
    if (Number(snapshot.cursor) < min) throw new Error(`Snapshot violou minCursor: ${snapshot.cursor} < ${min}`);
    this.initialize(snapshot);
    this.stats.reseeds++;
  }

  async catchUp() {
    while (this.cursor < this.target) {
      if (this.target - this.cursor > this.maxReplay) { await this.reseed(); continue; }
      // Páginas alinhadas: clientes em cursores diferentes pedem as mesmas chaves,
      // e páginas completas são compartilhadas no cache.
      const after = Math.floor(this.cursor / this.pageSize) * this.pageSize;
      const through = Math.min(this.target, after + this.pageSize);
      let page;
      try { page = await this.api.changes(String(after), String(through)); }
      catch (error) {
        if (error.code === 'CURSOR_EXPIRED') { await this.reseed(); continue; }
        throw error;
      }
      this.stats.pages++;
      const seqs = page.events.map(event => Number(event.cursor));
      if (page.after !== String(after) || page.through !== String(through) ||
          seqs.length !== through - after || seqs.some((seq, i) => seq !== after + i + 1)) {
        throw new GapError(`Página inconsistente (${after}, ${through}]`);
      }
      for (const event of page.events) this.applyChange(event);
    }
  }

  async accept(frame) {
    this.busy = true;
    try { await this.#accept(frame); } finally { this.busy = false; }
  }

  async #accept(frame) {
    if (frame.scopeId !== this.scopeId) throw new Error('Frame de outro escopo');
    const seq = Number(frame.cursor);
    this.target = Math.max(this.target, seq);
    if (this.cursor === null) {
      const snapshot = await this.startBase();
      if (this.cursor === null) this.initialize(snapshot);
    }
    if (frame.kind === 'CHANGE' && seq === this.cursor + 1) {
      if (frame.change?.cursor !== frame.cursor) throw new GapError('Frame inconsistente');
      this.applyChange(frame.change); this.stats.live++;
    } else if (this.cursor < this.target) {
      // Checkpoint adiantado, evento fora de ordem ou primeiro frame de um late joiner.
      await this.catchUp();
    }
    this.synced = true;
  }

  view() {
    return {
      cursor: this.cursor === null ? null : String(this.cursor), synced: this.synced,
      orders: [...this.orders.values()].sort((a, b) => a.id.localeCompare(b.id))
    };
  }
}

// Uma conexão do stream. Frames são processados em ordem; enquanto um catch-up
// roda, os próximos ficam no buffer do transporte e são aplicados depois
// (os de seq <= cursor são descartados).
export async function runSession(sync, frames, { onUpdate = () => {} } = {}) {
  sync.synced = false; // Até o primeiro frame desta conexão, pode ter perdido eventos.
  const iterator = frames[Symbol.asyncIterator]();
  if (sync.cursor === null) {
    // Renderização provisória: a base pode chegar antes do primeiro frame.
    sync.startBase().then(snapshot => {
      if (sync.cursor === null) { sync.initialize(snapshot); onUpdate(sync.view()); }
    }, () => {}); // a falha reaparece no accept do primeiro frame
  }
  try {
    while (true) {
      const next = await iterator.next();
      if (next.done) throw new Error('Stream encerrado; reconectar');
      await sync.accept(next.value);
      onUpdate(sync.view());
    }
  } finally { await iterator.return?.(); }
}

const backoff = attempt => Math.min(10_000, 200 * 2 ** attempt) * (0.5 + Math.random() / 2);

// Reconexão com backoff e watchdog: sem nenhum frame (checkpoints incluídos) em
// stallMs, a conexão é considerada morta e reaberta. O estado local é mantido;
// a retomada é o mesmo catch-up a partir do cursor.
export async function runSync({ sync, connect, signal, stallMs = 5000, onUpdate = () => {}, onError = () => {}, wait = backoff }) {
  let attempt = 0;
  while (!signal?.aborted) {
    const session = new AbortController();
    const abort = () => session.abort();
    signal?.addEventListener('abort', abort, { once: true });
    let lastFrame = Date.now();
    // Não conta o tempo gasto em catch-up (queries), só o silêncio do stream.
    const watchdog = setInterval(() => {
      if (sync.busy) lastFrame = Date.now();
      else if (Date.now() - lastFrame > stallMs) session.abort(new Error('Stream parado'));
    }, Math.min(1000, stallMs / 2));
    try {
      await runSession(sync, connect(session.signal), { onUpdate: view => { lastFrame = Date.now(); attempt = 0; onUpdate(view); } });
    } catch (error) {
      if (signal?.aborted) break;
      onError(session.signal.aborted && session.signal.reason instanceof Error ? session.signal.reason : error, attempt);
      await new Promise(resolve => setTimeout(resolve, wait(attempt++)));
    } finally {
      clearInterval(watchdog); session.abort(); signal?.removeEventListener('abort', abort);
    }
  }
}
