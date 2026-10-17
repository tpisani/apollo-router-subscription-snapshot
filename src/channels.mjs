import { mergePatches } from './patch.mjs';

// Um canal = um documento versionado por partida + uma política de envio.
//
// mode 'state' (usado pelos dois canais: placar e estatísticas)
//   STATE { version, state }: o estado inteiro. O mais novo substitui os anteriores,
//   então perda, duplicata e late joiner se resolvem com o próximo frame, sem query,
//   e o último frame pode ser retido e reenviado por quem estiver no meio (edge/Router).
//   throttleMs = 0: um frame por commit, na hora (placar).
//   throttleMs > 0: limitador. Se nada saiu nos últimos throttleMs, o frame sai na
//   hora; senão sai no fim da janela com o estado mais recente (estatísticas: no
//   máximo 2 frames/s numa rajada, latência de ms em jogo calmo).
//   A cada periodicMs sai um STATE mesmo sem mudança: sinal de vida e estado para
//   quem entrou num upstream já aberto.
//
// mode 'delta' (só nos experimentos de comparação, scripts/experiments.mjs)
//   DELTA { from, version, patch } por janela de coalesceMs (ou STATE, se menor) e
//   CHECKPOINT { version } periódico. Ficou registrado como alternativa medida.
//
// Os frames são iguais para todos os assinantes da partida: o Router deduplica.
export class ChannelHub {
  constructor({ store, mode, periodicMs = 1000, throttleMs = 0, coalesceMs = 0, adaptive = true, now = () => Date.now(), onFrame = () => {} }) {
    Object.assign(this, { store, mode, periodicMs, throttleMs, coalesceMs, adaptive, now, onFrame });
    this.keys = new Map();
  }

  subscribe(key, listener) {
    let entry = this.keys.get(key);
    if (!entry) {
      const doc = this.store.read(key);
      entry = { listeners: new Set(), latest: doc, delivered: doc.version, pending: null, flushTimer: null, lastEmit: 0, unsentSince: null };
      entry.timer = setInterval(() => this.#emit(key, this.#checkpoint(entry, key)), this.periodicMs);
      this.keys.set(key, entry);
    }
    entry.listeners.add(listener);
    return () => {
      entry.listeners.delete(listener);
      if (!entry.listeners.size) {
        clearInterval(entry.timer); clearTimeout(entry.flushTimer);
        this.keys.delete(key);
      }
    };
  }

  initialFrame(key) { return this.#checkpoint(this.keys.get(key), key); }

  #checkpoint(entry, key) {
    if (this.mode === 'state') return { matchId: key, kind: 'STATE', version: entry.latest.version, state: entry.latest.state, committedAt: null };
    // Só o que o stream já entregou, nunca o head: um commit ainda na janela não
    // pode fazer o cliente achar que perdeu algo.
    return { matchId: key, kind: 'CHECKPOINT', version: entry.delivered, from: null, patch: null, committedAt: null };
  }

  publish(change) {
    const entry = this.keys.get(change.key);
    if (!entry) return;
    entry.latest = { version: change.version, state: change.state };
    if (this.mode === 'state') {
      entry.unsentSince ??= change.committedAt;
      const wait = this.throttleMs - (this.now() - entry.lastEmit);
      if (wait <= 0 && !entry.flushTimer) this.#flushState(change.key);                    // calmo: na hora
      else entry.flushTimer ??= setTimeout(() => this.#flushState(change.key), Math.max(0, wait)); // rajada: fim da janela
      return;
    }
    if (entry.pending) {
      entry.pending.version = change.version;
      entry.pending.patch = mergePatches(entry.pending.patch, change.patch);
    } else {
      entry.pending = { from: entry.delivered, version: change.version, patch: change.patch, committedAt: change.committedAt };
      if (this.coalesceMs > 0) entry.flushTimer = setTimeout(() => this.#flush(change.key), this.coalesceMs);
    }
    if (this.coalesceMs === 0) this.#flush(change.key);
  }

  #flushState(key) {
    const entry = this.keys.get(key);
    if (!entry) return;
    clearTimeout(entry.flushTimer); entry.flushTimer = null;
    // committedAt = o commit mais antigo ainda não enviado: mede o pior atraso do frame.
    const committedAt = entry.unsentSince; entry.unsentSince = null; entry.lastEmit = this.now();
    this.#emit(key, { matchId: key, kind: 'STATE', version: entry.latest.version, state: entry.latest.state, committedAt });
  }

  #flush(key) {
    const entry = this.keys.get(key);
    if (!entry?.pending) return;
    // committedAt = o commit mais antigo da janela: a latência medida é a do pior dado do frame.
    const { from, version, patch, committedAt } = entry.pending;
    entry.pending = null; entry.flushTimer = null; entry.delivered = version;
    // No flush o estado mais recente está exatamente em `version`.
    if (this.adaptive && JSON.stringify(entry.latest.state).length < JSON.stringify(patch).length) {
      this.#emit(key, { matchId: key, kind: 'STATE', from: null, version, state: entry.latest.state, patch: null, committedAt });
    } else this.#emit(key, { matchId: key, kind: 'DELTA', from, version, patch, committedAt });
  }

  #emit(key, frame) {
    const entry = this.keys.get(key);
    if (!entry) return;
    this.onFrame(frame);
    for (const listener of entry.listeners) listener(frame);
  }

  get size() { let n = 0; for (const entry of this.keys.values()) n += entry.listeners.size; return n; }
}

// Fila de um assinante com fusão sem perda, o que limita a memória sem desconectar:
//   STATE novo descarta tudo o que está na fila (estado inteiro supera tudo);
//   DELTA seguido de DELTA vira um DELTA (patches por valor final se fundem);
//   CHECKPOINT com algo na fila é descartado (a fila já traz informação mais nova).
// Iterator manual para que return() libere o listener na hora.
export class LiveStream {
  constructor({ hub, key, onClose = () => {}, onMerge = () => {} }) {
    this.onClose = onClose; this.onMerge = onMerge; this.done = false; this.wake = null;
    this.unsubscribe = hub.subscribe(key, frame => { this.#push(frame); this.wake?.(); });
    this.queue = [hub.initialFrame(key)];
  }
  #push(frame) {
    const last = this.queue.at(-1);
    if (frame.kind === 'STATE') {
      if (this.queue.length) this.onMerge();
      this.queue = [frame];
    } else if (frame.kind === 'DELTA' && last?.kind === 'DELTA') {
      this.queue[this.queue.length - 1] = { ...last, version: frame.version, patch: mergePatches(last.patch, frame.patch) };
      this.onMerge();
    } else if (frame.kind === 'CHECKPOINT' && this.queue.length) {
      // descartado
    } else this.queue.push(frame);
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
