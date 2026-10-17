// Stream compartilhável: o mesmo conteúdo para todo assinante de um escopo, sem
// estado por cliente. Por isso o Router pode deduplicar: quem entra depois
// recebe os próximos frames e monta o passado por queries cacheáveis.
//
// Frames:
//   CHANGE     { cursor: seq, change }  — empurrado no commit, sem polling
//   CHECKPOINT { cursor: head durável } — periódico: prova que o stream está vivo,
//                                         dá o alvo de sincronização e revela perdas
export class Hub {
  constructor({ head, checkpointMs = 250 }) {
    this.head = head; this.checkpointMs = checkpointMs; this.scopes = new Map();
  }
  subscribe(scopeId, listener) {
    let scope = this.scopes.get(scopeId);
    if (!scope) {
      scope = { listeners: new Set(), timer: setInterval(() => this.#checkpoint(scopeId), this.checkpointMs) };
      this.scopes.set(scopeId, scope);
    }
    scope.listeners.add(listener);
    return () => {
      scope.listeners.delete(listener);
      if (!scope.listeners.size) { clearInterval(scope.timer); this.scopes.delete(scopeId); }
    };
  }
  checkpointFrame(scopeId) {
    return { scopeId, kind: 'CHECKPOINT', cursor: String(this.head(scopeId)), change: null };
  }
  #checkpoint(scopeId) {
    const scope = this.scopes.get(scopeId);
    if (!scope) return;
    // Lê o head durável (uma leitura por escopo ativo por intervalo, não por assinante).
    // Com um bus distribuído, o checkpoint pode chegar antes do CHANGE: o cliente busca
    // pelo histórico e descarta o CHANGE depois. Nunca o contrário.
    const frame = this.checkpointFrame(scopeId);
    for (const listener of scope.listeners) listener(frame);
  }
  publish(change) {
    const scope = this.scopes.get(change.scopeId);
    if (!scope) return;
    const frame = { scopeId: change.scopeId, kind: 'CHANGE', cursor: change.cursor, change };
    for (const listener of scope.listeners) listener(frame);
  }
  get size() { let n = 0; for (const scope of this.scopes.values()) n += scope.listeners.size; return n; }
}

// Iterator de uma assinatura (para o Router, uma por escopo após a deduplicação).
// Fila limitada: se o consumidor não acompanha, os frames pendentes viram um único
// CHECKPOINT (conflação). O stream fica com perda, mas o cliente não: ele vê o
// checkpoint acima do seu cursor e busca o intervalo pelo histórico.
// Iterator manual para que return() libere o listener na hora, mesmo em espera.
export class LiveStream {
  constructor({ hub, scopeId, queueLimit = 1000, onClose = () => {}, onConflate = () => {} }) {
    this.hub = hub; this.scopeId = scopeId; this.onClose = onClose; this.done = false; this.wake = null;
    this.queue = [hub.checkpointFrame(scopeId)]; // Primeiro frame imediato para quem abre o upstream.
    this.unsubscribe = hub.subscribe(scopeId, frame => {
      if (this.queue.length >= queueLimit) {
        this.queue = [hub.checkpointFrame(scopeId)]; onConflate();
      } else this.queue.push(frame);
      this.wake?.();
    });
  }
  async next() {
    while (!this.done) {
      if (this.queue.length) return { done: false, value: this.queue.shift() };
      await new Promise(resolve => { this.wake = resolve; });
      this.wake = null;
    }
    return { done: true, value: undefined };
  }
  async return() {
    if (!this.done) { this.done = true; this.unsubscribe(); this.wake?.(); this.onClose(); }
    return { done: true, value: undefined };
  }
  async throw(error) { await this.return(); throw error; }
  [Symbol.asyncIterator]() { return this; }
}
